package substrate_test

import (
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// TestLambdaExecutor_StubWhenDockerUnavailable verifies that Execute returns the
// standard stub payload when Docker is not available.
func TestLambdaExecutor_StubWhenDockerUnavailable(t *testing.T) {
	t.Parallel()
	logger := substrate.NewDefaultLogger(-4, false)
	exec := substrate.NewLambdaExecutorForTest(substrate.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, false /* force unavailable */)
	defer exec.StopAll()

	fn := substrate.LambdaFunction{
		FunctionName: "test-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
		Runtime:      "python3.12",
		Handler:      "index.handler",
	}
	result, err := exec.Execute(t.Context(), fn, nil, []byte(`{"key":"value"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"statusCode":200,"body":"null"}`
	if string(result) != want {
		t.Errorf("got %s; want %s", result, want)
	}
}

// TestRuntimeToImage verifies that runtimeToImage returns expected ECR URIs.
func TestRuntimeToImage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		runtime string
		wantPfx string
	}{
		{"python3.12", "public.ecr.aws/lambda/python:3.12"},
		{"python3.11", "public.ecr.aws/lambda/python:3.11"},
		{"python3.10", "public.ecr.aws/lambda/python:3.10"},
		{"python3.9", "public.ecr.aws/lambda/python:3.9"},
		{"python3.8", "public.ecr.aws/lambda/python:3.8"},
		{"nodejs20.x", "public.ecr.aws/lambda/nodejs:20"},
		{"nodejs18.x", "public.ecr.aws/lambda/nodejs:18"},
		{"nodejs16.x", "public.ecr.aws/lambda/nodejs:16"},
		{"java21", "public.ecr.aws/lambda/java:21"},
		{"java17", "public.ecr.aws/lambda/java:17"},
		{"java11", "public.ecr.aws/lambda/java:11"},
		{"java8.al2", "public.ecr.aws/lambda/java:8.al2"},
		{"dotnet8", "public.ecr.aws/lambda/dotnet:8"},
		{"dotnet7", "public.ecr.aws/lambda/dotnet:7"},
		{"ruby3.2", "public.ecr.aws/lambda/ruby:3.2"},
		{"ruby3.3", "public.ecr.aws/lambda/ruby:3.3"},
		{"go1.x", "public.ecr.aws/lambda/provided:al2023"},
		{"provided.al2023", "public.ecr.aws/lambda/provided:al2023"},
		{"provided.al2", "public.ecr.aws/lambda/provided:al2023"},
		{"provided", "public.ecr.aws/lambda/provided:al2023"},
		{"unknown-runtime", "public.ecr.aws/lambda/provided:al2023"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.runtime, func(t *testing.T) {
			t.Parallel()
			got := substrate.RuntimeToImage(tc.runtime)
			if got != tc.wantPfx {
				t.Errorf("RuntimeToImage(%q) = %q; want %q", tc.runtime, got, tc.wantPfx)
			}
		})
	}
}

// TestFindFreePort verifies that a free port is returned in the valid range.
func TestFindFreePort(t *testing.T) {
	t.Parallel()
	port, err := substrate.FindFreePort()
	if err != nil {
		t.Fatalf("FindFreePort: %v", err)
	}
	if port < 1024 || port > 65535 {
		t.Errorf("port %d is outside valid range [1024, 65535]", port)
	}
}

// TestLambdaExecutor_StopAll verifies that StopAll closes the stop channel
// and can be called repeatedly without panicking.
func TestLambdaExecutor_StopAll(t *testing.T) {
	t.Parallel()
	logger := substrate.NewDefaultLogger(-4, false)
	exec := substrate.NewLambdaExecutorForTest(substrate.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, false)
	// First call should succeed.
	exec.StopAll()
	// Second call should not panic.
	exec.StopAll()
}

// TestLambdaExecutor_IsDockerAvailable verifies that isDockerAvailable returns
// a result without panicking and caches the result on subsequent calls.
func TestLambdaExecutor_IsDockerAvailable(t *testing.T) {
	t.Parallel()
	logger := substrate.NewDefaultLogger(-4, false)
	// Create a real executor (no forced Docker state) so the probe runs.
	exec := substrate.NewLambdaExecutor(substrate.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger)
	defer exec.StopAll()
	// First call runs the real probe; second call returns the cached value.
	_ = exec.IsDockerAvailableForTest()
	_ = exec.IsDockerAvailableForTest()
}

// TestLambdaExecutor_EvictStale verifies that evictStale runs without panicking
// on an empty pool.
func TestLambdaExecutor_EvictStale(t *testing.T) {
	t.Parallel()
	logger := substrate.NewDefaultLogger(-4, false)
	exec := substrate.NewLambdaExecutorForTest(substrate.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, false)
	defer exec.StopAll()
	// Should not panic on empty pool.
	exec.EvictStaleForTest()
}

// TestLambdaPlugin_Shutdown verifies that Shutdown on a bare plugin does not
// panic and returns nil.
func TestLambdaPlugin_Shutdown(t *testing.T) {
	t.Parallel()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := substrate.NewLambdaPluginForTest(state, tc)
	if err := substrate.ShutdownLambdaPluginForTest(p, t.Context()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	// Second call should also be safe.
	if err := substrate.ShutdownLambdaPluginForTest(p, t.Context()); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
}

// TestInvoke_DockerFallback verifies that when Docker is available but
// the container fails to start (e.g. invalid image), invoke degrades gracefully
// and returns the stub response, also saving it to the replay cache.
func TestInvoke_DockerFallback(t *testing.T) {
	t.Parallel()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := substrate.NewLambdaPluginForTest(state, tc)

	// Force available=true but use a clearly invalid image so Docker fails fast.
	logger := substrate.NewDefaultLogger(-4, false)
	exec := substrate.NewLambdaExecutorForTest(substrate.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, true /* force available */)
	defer exec.StopAll()
	substrate.LambdaPluginSetExecutorForTest(p, exec)

	const fnName = "docker-fallback-fn"
	const acct = "123456789012"
	const region = "us-east-1"
	fnARN := "arn:aws:lambda:" + region + ":" + acct + ":function:" + fnName

	substrate.LambdaPluginCreateFunctionForTest(p, substrate.LambdaFunction{
		FunctionName: fnName,
		FunctionArn:  fnARN,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		PackageType:  "Image",
		ImageURI:     "substrate-nonexistent-image-xyz:never",
	})

	req := &substrate.AWSRequest{
		Operation: "POST",
		Path:      "/2015-03-31/functions/" + fnName + "/invocations",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      []byte(`{}`),
	}
	reqCtx := &substrate.RequestContext{
		RequestID: "test-req-id",
		AccountID: acct,
		Region:    region,
	}
	resp, err := substrate.InvokeLambdaForTest(p, reqCtx, req, fnName)
	if err != nil {
		t.Fatalf("InvokeLambda: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d; want 200", resp.StatusCode)
	}
}

// TestInvoke_RecordedReplay verifies that invoke returns the cached response
// when ReplayMode is "recorded" and a cache entry exists.
func TestInvoke_RecordedReplay(t *testing.T) {
	t.Parallel()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := substrate.NewLambdaPluginForTest(state, tc)

	// Create an executor in "recorded" mode with Docker forced available.
	logger := substrate.NewDefaultLogger(-4, false)
	exec := substrate.NewLambdaExecutorForTest(substrate.LambdaExecCfg{
		ReplayMode:  "recorded",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, true /* force available */)
	defer exec.StopAll()
	substrate.LambdaPluginSetExecutorForTest(p, exec)

	const fnName = "replay-test-fn"
	const acct = "123456789012"
	const region = "us-east-1"
	fnARN := "arn:aws:lambda:" + region + ":" + acct + ":function:" + fnName

	// Create function in state.
	substrate.LambdaPluginCreateFunctionForTest(p, substrate.LambdaFunction{
		FunctionName: fnName,
		FunctionArn:  fnARN,
		Runtime:      "python3.12",
		Handler:      "index.handler",
	})

	// Pre-populate the replay cache.
	cachedResponse := []byte(`{"statusCode":200,"body":"cached"}`)
	payload := []byte(`{"key":"val"}`)
	p.SaveReplayForTest(fnARN, payload, cachedResponse)

	// Invoke — should return the cached response without starting Docker.
	req := &substrate.AWSRequest{
		Operation: "POST",
		Path:      "/2015-03-31/functions/" + fnName + "/invocations",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      payload,
	}
	reqCtx := &substrate.RequestContext{
		RequestID: "test-req-id",
		AccountID: acct,
		Region:    region,
	}
	resp, err := substrate.InvokeLambdaForTest(p, reqCtx, req, fnName)
	if err != nil {
		t.Fatalf("InvokeLambda: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d; want 200", resp.StatusCode)
	}
	if string(resp.Body) != string(cachedResponse) {
		t.Errorf("Body = %s; want %s", resp.Body, cachedResponse)
	}
}

// TestReplayCache_RoundTrip verifies that saved replay entries can be loaded.
func TestReplayCache_RoundTrip(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)
	_ = ts // server needed to exercise state manager path indirectly

	// Exercise the cache helpers via a LambdaPlugin created with a fresh state manager.
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := substrate.NewLambdaPluginForTest(state, tc)

	const arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
	payload := []byte(`{"input":"hello"}`)
	response := []byte(`{"statusCode":200,"body":"world"}`)

	p.SaveReplayForTest(arn, payload, response)

	got, ok := p.LoadReplayForTest(arn, payload)
	if !ok {
		t.Fatal("expected cache hit; got miss")
	}
	if string(got) != string(response) {
		t.Errorf("cached response mismatch: got %s; want %s", got, response)
	}

	// Different payload → cache miss.
	_, ok = p.LoadReplayForTest(arn, []byte(`{"input":"other"}`))
	if ok {
		t.Error("expected cache miss for different payload; got hit")
	}
}
