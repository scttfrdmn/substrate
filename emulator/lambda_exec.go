package emulator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// LambdaExecCfg holds Docker execution settings for the Lambda engine.
//
// Docker execution is off unless [Config].Lambda.DockerEnabled is set, which
// [RegisterDefaultPlugins] reads: with it unset no executor is constructed at all, so the
// default — and every CI run — answers invocations from the stub path in [LambdaPlugin].
// Nothing on this type gates it; a zero LambdaExecCfg belongs to an executor that is
// already running.
//
// Neither execution path runs a caller's handler in the ZIP case. See
// [LambdaExecutor.startZIPContainer] for what the ZIP path does instead and why the archive
// is deliberately not extracted (#1079).
type LambdaExecCfg struct {
	// ReplayMode selects invocation behavior: "live" or "recorded".
	ReplayMode string

	// WarmPoolTTL is the maximum idle duration before a container is stopped.
	WarmPoolTTL time.Duration
}

// containerHandle tracks a running Lambda RIE container.
type containerHandle struct {
	containerID string
	port        int
	tempDir     string
	lastUsed    time.Time
}

// LambdaExecutor manages warm Lambda RIE containers and falls back to stub responses when
// the docker binary cannot be run.
//
// It does not know whether Docker execution is configured — an executor exists only because
// [RegisterDefaultPlugins] found the flag set, so the fallback it can make is the narrower
// one: [LambdaExecutor.isDockerAvailable] failing, or a container failing to start. The
// pre-#1079 wording here named a DockerEnabled field on [LambdaExecCfg], which has no such
// field and never did.
//
// All public methods are safe for concurrent use.
type LambdaExecutor struct {
	mu        sync.Mutex
	pool      map[string]*containerHandle // functionARN → handle
	cfg       LambdaExecCfg
	logger    Logger
	available bool
	availOnce sync.Once
	stopCh    chan struct{}
}

// NewLambdaExecutor creates a LambdaExecutor and starts the idle-container
// eviction goroutine. Call StopAll when the executor is no longer needed.
func NewLambdaExecutor(cfg LambdaExecCfg, logger Logger) *LambdaExecutor {
	if cfg.WarmPoolTTL == 0 {
		cfg.WarmPoolTTL = 5 * time.Minute
	}
	if cfg.ReplayMode == "" {
		cfg.ReplayMode = "live"
	}
	e := &LambdaExecutor{
		pool:   make(map[string]*containerHandle),
		cfg:    cfg,
		logger: logger,
		stopCh: make(chan struct{}),
	}
	go e.evictLoop()
	return e
}

// isDockerAvailable returns true when the Docker daemon is reachable. The
// result is cached after the first probe.
func (e *LambdaExecutor) isDockerAvailable() bool {
	e.availOnce.Do(func() {
		cmd := exec.Command("docker", "info")
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		e.available = cmd.Run() == nil
	})
	return e.available
}

// Execute invokes the Lambda function. When Docker is unavailable (or not
// configured) it returns the same stub payload the existing LambdaPlugin uses.
// When Docker is available it routes through the warm container pool.
//
// functionError is the runtime's X-Amz-Function-Error value ("Handled" or
// "Unhandled") and is empty when the handler completed normally, which is what
// lets the caller omit the header entirely on success (#393).
func (e *LambdaExecutor) Execute(ctx context.Context, fn LambdaFunction, zipBytes, payload []byte) (result []byte, functionError string, err error) {
	if !e.isDockerAvailable() {
		return []byte(`{"statusCode":200,"body":"null"}`), "", nil
	}
	h, err := e.getOrStartContainer(ctx, fn, zipBytes)
	if err != nil {
		// Graceful degradation: log and return stub.
		e.logger.Warn("lambda executor: container start failed, using stub", "function", fn.FunctionName, "err", err)
		return []byte(`{"statusCode":200,"body":"null"}`), "", nil
	}
	result, functionError, err = e.invokePOST(ctx, h, payload)
	if err != nil {
		return nil, "", fmt.Errorf("lambda executor invoke: %w", err)
	}
	e.mu.Lock()
	h.lastUsed = time.Now()
	e.mu.Unlock()
	return result, functionError, nil
}

// getOrStartContainer returns the warm container for fn, starting one if needed.
func (e *LambdaExecutor) getOrStartContainer(ctx context.Context, fn LambdaFunction, zipBytes []byte) (*containerHandle, error) {
	e.mu.Lock()
	if h, ok := e.pool[fn.FunctionArn]; ok {
		e.mu.Unlock()
		return h, nil
	}
	e.mu.Unlock()

	var h *containerHandle
	var err error
	if fn.PackageType == "Image" && fn.ImageURI != "" {
		h, err = e.startImageContainer(fn)
	} else {
		h, err = e.startZIPContainer(fn, zipBytes)
	}
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	e.pool[fn.FunctionArn] = h
	e.mu.Unlock()
	return h, nil
}

// startZIPContainer writes the ZIP to a temp dir and starts a Lambda RIE container.
//
// The archive is written but never extracted, so the container's /var/task holds one file
// named function.zip and no module tree. A handler cannot be imported from it whatever the
// ZIP contains, and that is deliberate as of #1079: extracting it would complete in-process
// execution of a caller's own code, which CLAUDE.md's scope boundary excludes by name and
// which no test in this repo could cover — it would depend on container-start latency, the
// handler's I/O and clock, and an image pull over the network. The write stays as the
// recorded intent the same boundary asks for, and it is what [lambdaZipStateKey]'s bytes are
// for.
//
// What a caller observes is the part worth knowing, because it is not a stub: docker run
// succeeds, [LambdaExecutor.waitReady] succeeds, and [LambdaExecutor.invokePOST] returns the
// RIE's own answer — HTTP 200 with an import-error body and X-Amz-Function-Error: Unhandled,
// which [invokeResponse] forwards. So Invoke reports that the caller's handler raised, for
// code that was never loaded. [LambdaExecutor.startImageContainer] is the only path that can
// run real code, and substrate does not build the image it runs.
func (e *LambdaExecutor) startZIPContainer(fn LambdaFunction, zipBytes []byte) (*containerHandle, error) {
	port, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("find free port: %w", err)
	}

	dir, err := os.MkdirTemp("", "substrate-lambda-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	// Write the ZIP file into the temp dir as "function.zip". Recorded intent, not a
	// deployment: nothing extracts it, so the mount below carries the archive rather than
	// the module tree the runtime interface expects. See the doc comment for why (#1079).
	if len(zipBytes) > 0 {
		if writeErr := os.WriteFile(dir+"/function.zip", zipBytes, 0600); writeErr != nil {
			_ = os.RemoveAll(dir)
			return nil, fmt.Errorf("write zip: %w", writeErr)
		}
	}

	image := runtimeToImage(fn.Runtime)
	handler := fn.Handler
	if handler == "" {
		handler = "index.handler"
	}

	args := []string{
		"run", "-d",
		"-p", fmt.Sprintf("%d:8080", port),
		"-v", dir + ":/var/task:ro",
		"-e", "AWS_LAMBDA_FUNCTION_NAME=" + fn.FunctionName,
		"-e", "AWS_LAMBDA_FUNCTION_HANDLER=" + handler,
	}
	for k, v := range fn.Environment {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, image, handler)

	cmd := exec.Command("docker", args...)
	out, runErr := cmd.Output()
	if runErr != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("docker run: %w", runErr)
	}

	containerID := strings.TrimSpace(string(out))
	h := &containerHandle{
		containerID: containerID,
		port:        port,
		tempDir:     dir,
		lastUsed:    time.Now(),
	}
	if waitErr := e.waitReady(h, 10*time.Second); waitErr != nil {
		_ = e.stopContainer(h)
		return nil, fmt.Errorf("container not ready: %w", waitErr)
	}
	return h, nil
}

// startImageContainer starts a container from a pre-built Lambda image.
//
// This is the only path that can run a caller's code, and only because the code is already
// inside an image substrate neither builds nor inspects — [LambdaExecutor.startZIPContainer]
// documents why the archive path deliberately does not (#1079). What that image does is
// outside substrate's observation either way: an Invoke reports whatever the runtime
// interface answers.
func (e *LambdaExecutor) startImageContainer(fn LambdaFunction) (*containerHandle, error) {
	port, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("find free port: %w", err)
	}

	args := []string{
		"run", "-d",
		"-p", fmt.Sprintf("%d:8080", port),
		"--entrypoint", "/lambda-entrypoint.sh",
	}
	for k, v := range fn.Environment {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, fn.ImageURI)

	cmd := exec.Command("docker", args...)
	out, runErr := cmd.Output()
	if runErr != nil {
		return nil, fmt.Errorf("docker run image: %w", runErr)
	}

	containerID := strings.TrimSpace(string(out))
	h := &containerHandle{
		containerID: containerID,
		port:        port,
		lastUsed:    time.Now(),
	}
	if waitErr := e.waitReady(h, 10*time.Second); waitErr != nil {
		_ = e.stopContainer(h)
		return nil, fmt.Errorf("container not ready: %w", waitErr)
	}
	return h, nil
}

// waitReady polls the RIE endpoint until it responds or the deadline is exceeded.
//
// The endpoint it polls is the *invocation* endpoint with an empty payload, so becoming ready
// means having been invoked once. On the ZIP path that costs nothing because nothing runs;
// on [LambdaExecutor.startImageContainer]'s path it is an invocation the caller never asked
// for. Recorded here rather than fixed with #1079, which is about the ZIP path: TODO(#1129):
// probe readiness without invoking the handler.
func (e *LambdaExecutor) waitReady(h *containerHandle, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://localhost:%d/2015-03-31/functions/function/invocations", h.port)
	for time.Now().Before(deadline) {
		resp, err := http.Post(url, "application/json", strings.NewReader(`{}`)) //nolint:noctx
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("container did not become ready within %s", timeout)
}

// invokePOST POSTs the payload to the container's RIE invocation endpoint.
// invokePOST posts the payload to a warm container's runtime interface and
// returns its response body along with the X-Amz-Function-Error header the
// runtime sets when the handler raised. That header is the only signal a handler
// failed — the RIE answers 200 either way — so dropping it made a failed
// invocation indistinguishable from a successful one (#393).
func (e *LambdaExecutor) invokePOST(ctx context.Context, h *containerHandle, payload []byte) (body []byte, functionError string, err error) {
	url := fmt.Sprintf("http://localhost:%d/2015-03-31/functions/function/invocations", h.port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("http post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read invoke response: %w", err)
	}
	return body, resp.Header.Get("X-Amz-Function-Error"), nil
}

// evictLoop periodically removes containers that have been idle longer than WarmPoolTTL.
func (e *LambdaExecutor) evictLoop() {
	ticker := time.NewTicker(e.cfg.WarmPoolTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.evictStale()
		case <-e.stopCh:
			return
		}
	}
}

// evictStale stops and removes containers idle longer than WarmPoolTTL.
func (e *LambdaExecutor) evictStale() {
	cutoff := time.Now().Add(-e.cfg.WarmPoolTTL)
	e.mu.Lock()
	var stale []*containerHandle
	for arn, h := range e.pool {
		if h.lastUsed.Before(cutoff) {
			stale = append(stale, h)
			delete(e.pool, arn)
		}
	}
	e.mu.Unlock()
	for _, h := range stale {
		if err := e.stopContainer(h); err != nil {
			e.logger.Warn("lambda executor: evict container", "id", h.containerID, "err", err)
		}
	}
}

// StopAll stops all running containers and cleans up temp directories, and ends the
// idle-eviction goroutine. It is safe to call multiple times, and the executor is
// not reusable afterwards — call it when the executor is being discarded, and
// [LambdaExecutor.DrainPool] when it is not.
func (e *LambdaExecutor) StopAll() {
	// Signal the evict loop to exit.
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}

	e.DrainPool()
}

// DrainPool stops every warm container and empties the pool, leaving the executor
// usable: the idle-eviction goroutine keeps running and the next invocation starts a
// fresh container.
//
// It is what a state reset calls (#903), where [LambdaExecutor.StopAll] would be
// wrong — StopAll closes the channel the eviction loop selects on, so every
// container started after a reset would stay warm however long it sat idle, until
// Shutdown.
func (e *LambdaExecutor) DrainPool() {
	e.mu.Lock()
	handles := make([]*containerHandle, 0, len(e.pool))
	for _, h := range e.pool {
		handles = append(handles, h)
	}
	e.pool = make(map[string]*containerHandle)
	e.mu.Unlock()

	for _, h := range handles {
		if err := e.stopContainer(h); err != nil {
			e.logger.Warn("lambda executor: stop container", "id", h.containerID, "err", err)
		}
	}
}

// Evict stops the warm container for one function ARN and drops its pool entry, doing
// nothing when that function has no warm container.
//
// This is the granularity [LambdaExecutor.DrainPool] is not: the pool is keyed by function
// ARN alone and [containerHandle] records no code identity at all, so the only way substrate
// can tell that a container is running code the function no longer has is for the operation
// that changed the code to say so (#1035). `UpdateFunctionCode` on one function must not
// throw away every other function's container.
//
// Dropping a warm container is always safe — the next invocation starts a fresh one — so
// this reports nothing and never fails a caller's request. A container that cannot be
// stopped is logged and its pool entry is dropped regardless, because leaving the entry
// would mean serving the stale code the eviction exists to prevent.
//
// The container is stopped outside the lock, following DrainPool: `docker stop` blocks for
// as long as the container takes to exit, and holding e.mu across it would stall every
// concurrent invoke of every other function.
func (e *LambdaExecutor) Evict(functionARN string) {
	e.mu.Lock()
	h, ok := e.pool[functionARN]
	if ok {
		delete(e.pool, functionARN)
	}
	e.mu.Unlock()
	if !ok {
		return
	}
	if err := e.stopContainer(h); err != nil {
		e.logger.Warn("lambda executor: evict container for a changed function",
			"arn", functionARN, "id", h.containerID, "err", err)
	}
}

// stopContainer stops a single container, removes it, and removes its temp directory.
//
// `docker rm` follows the `stop`, which is what #903 taught [RDSExecutor] and what this did
// not do until #1035: a stopped container still holds its name, its writable layer and its
// port reservation on the host, and it outlives the process rather than the executor. That
// matters more now than it did, because eviction is no longer only the idle TTL and
// shutdown — a caller updating one function's code in a loop would otherwise leave one dead
// container per call.
//
// The stop error is the one returned, since it is the failure a caller can act on; a remove
// that fails after a successful stop is logged. Both are attempted regardless of the other's
// outcome, because a container that refused to stop is still worth removing.
func (e *LambdaExecutor) stopContainer(h *containerHandle) error {
	stopCmd := exec.Command("docker", "stop", h.containerID)
	stopCmd.Stdout = io.Discard
	stopCmd.Stderr = io.Discard
	err := stopCmd.Run()

	rmCmd := exec.Command("docker", "rm", "-f", h.containerID)
	rmCmd.Stdout = io.Discard
	rmCmd.Stderr = io.Discard
	if rmErr := rmCmd.Run(); rmErr != nil && err == nil {
		e.logger.Warn("lambda executor: remove container", "id", h.containerID, "err", rmErr)
	}

	if h.tempDir != "" {
		_ = os.RemoveAll(h.tempDir)
	}
	return err
}

// runtimeToImage maps an AWS Lambda runtime identifier to a public ECR image.
func runtimeToImage(runtime string) string {
	switch runtime {
	case "python3.12":
		return "public.ecr.aws/lambda/python:3.12"
	case "python3.11":
		return "public.ecr.aws/lambda/python:3.11"
	case "python3.10":
		return "public.ecr.aws/lambda/python:3.10"
	case "python3.9":
		return "public.ecr.aws/lambda/python:3.9"
	case "python3.8":
		return "public.ecr.aws/lambda/python:3.8"
	case "nodejs20.x":
		return "public.ecr.aws/lambda/nodejs:20"
	case "nodejs18.x":
		return "public.ecr.aws/lambda/nodejs:18"
	case "nodejs16.x":
		return "public.ecr.aws/lambda/nodejs:16"
	case "java21":
		return "public.ecr.aws/lambda/java:21"
	case "java17":
		return "public.ecr.aws/lambda/java:17"
	case "java11":
		return "public.ecr.aws/lambda/java:11"
	case "java8.al2":
		return "public.ecr.aws/lambda/java:8.al2"
	case "dotnet8":
		return "public.ecr.aws/lambda/dotnet:8"
	case "dotnet7":
		return "public.ecr.aws/lambda/dotnet:7"
	case "ruby3.2":
		return "public.ecr.aws/lambda/ruby:3.2"
	case "ruby3.3":
		return "public.ecr.aws/lambda/ruby:3.3"
	case "go1.x", "provided.al2023", "provided.al2", "provided":
		return "public.ecr.aws/lambda/provided:al2023"
	default:
		// Fall back to a generic provided image.
		return "public.ecr.aws/lambda/provided:al2023"
	}
}

// findFreePort finds a free TCP port on localhost by binding to :0.
func findFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port, nil
}

// --- Replay cache helpers (defined here; called from LambdaPlugin methods) ---

// lambdaReplayCacheKey computes the state key for a cached Lambda invocation
// result. The key is derived from the function ARN and the payload SHA256, so
// it is stable across process restarts.
func lambdaReplayCacheKey(functionARN string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(functionARN))
	h.Write([]byte("|"))
	h.Write(payload)
	return fmt.Sprintf("replay:%x", h.Sum(nil))
}

// saveReplay persists a Lambda invocation result in the replay cache.
func (p *LambdaPlugin) saveReplay(functionARN string, payload, response []byte) {
	key := lambdaReplayCacheKey(functionARN, payload)
	data, err := json.Marshal(response)
	if err != nil {
		return
	}
	_ = p.state.Put(context.Background(), lambdaNamespace, key, data)
}

// loadReplay retrieves a previously cached Lambda invocation result.
// Returns (nil, false) when no cache entry exists.
func (p *LambdaPlugin) loadReplay(functionARN string, payload []byte) ([]byte, bool) {
	key := lambdaReplayCacheKey(functionARN, payload)
	data, err := p.state.Get(context.Background(), lambdaNamespace, key)
	if err != nil || data == nil {
		return nil, false
	}
	var result []byte
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, false
	}
	return result, true
}
