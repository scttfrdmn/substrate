package emulator_test

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A warm container outlived the code it was started from — #1035.
//
// The executor's warm pool is keyed by function ARN alone and its handle records no code
// identity at all, so nothing in the invoke path could notice that a container is running code
// the function no longer has. Only `Shutdown` and the idle TTL ever dropped an entry, and the
// TTL is by *idle* time, so a function invoked in a loop kept its stale container indefinitely:
// `GetFunction` reported the new `CodeSha256` while `Invoke` returned the previous function's
// output, and the response a caller was testing against was the stale one.
//
// **What these tests assert is the bookkeeping, not the execution, and the issue's request for a
// Docker-gated "the invoke runs the new code" test cannot be met** — not because CI has no Docker,
// but because no container substrate starts ever runs a caller's handler. `startZIPContainer`
// writes the archive to `/var/task/function.zip` and mounts the directory; nothing extracts it,
// where the Lambda runtime interface expects the module tree, so `index.handler` cannot be
// imported whichever version of the code is mounted. There is no observable difference between
// the old code and the new one to assert on. That is consistent with CLAUDE.md's scope boundary,
// which puts running a Lambda's code out of scope, and it is recorded rather than worked around
// (#1079).
//
// The half that *is* observable everywhere is whether the pool entry survived the operation, and
// that is what the fix consists of — the same split #903 made with
// [emulator.RDSActiveContainerCountForTest].
//
// The injected container IDs name no container, so the `docker stop` inside an eviction fails
// and is logged. That is deliberate rather than tolerated: the pool entry must go whether or not
// the container could be stopped, because leaving it would serve exactly the stale code the
// eviction exists to prevent. No test here skips on Docker's absence for that reason.

// lambdaWarmWorld is a served Lambda plugin with an executor attached, which the default test
// server has not got: the executor is nil unless Docker execution is configured.
//
// Requests go over HTTP through [lambdaRequest] rather than through `HandleRequest`, since the
// question is what an API caller's sequence of calls leaves behind.
type lambdaWarmWorld struct {
	srv   *emulator.Server
	exec  *emulator.LambdaExecutor
	state emulator.StateManager
}

// newLambdaWarmWorld builds one. `false` for Docker availability: the invoke path is never taken
// here, and forcing availability would have the stub path try to start a container.
func newLambdaWarmWorld(t *testing.T) *lambdaWarmWorld {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	exec := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{
		ReplayMode: "live", WarmPoolTTL: time.Hour,
	}, logger, false)
	t.Cleanup(exec.StopAll)

	plugin := &emulator.LambdaPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	emulator.LambdaPluginSetExecutorForTest(plugin, exec)
	registry.Register(plugin)

	return &lambdaWarmWorld{
		srv:   emulator.NewServer(*cfg, registry, store, state, tc, logger),
		exec:  exec,
		state: state,
	}
}

// lambdaWarmARN is the ARN the pool keys a function by, derived from account, Region and name —
// which is why a recreated function inherits a dead one's entry.
func lambdaWarmARN(name string) string {
	return "arn:aws:lambda:us-east-1:123456789012:function:" + name
}

// lambdaWarmCreate creates a function with an inline package, so `ZipStored` is set and there is
// a deployment-package record for the delete test to look for.
func (w *lambdaWarmWorld) create(t *testing.T, name string) {
	t.Helper()
	resp := lambdaRequest(t, w.srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": name,
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::123456789012:role/svc",
		"Code":         map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte("original"))},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// warm injects a pool entry for a function, standing in for a container an earlier invoke
// started.
func (w *lambdaWarmWorld) warm(name string) {
	emulator.InjectPoolEntryForTest(w.exec, lambdaWarmARN(name), "substrate-test-no-such-container")
}

// pooled returns the ARNs the executor still believes it holds a container for.
func (w *lambdaWarmWorld) pooled() []string {
	return emulator.WarmPoolARNsForTest(w.exec)
}

// TestLambdaWarmContainer_UpdateFunctionCodeEvictsOnlyThatFunction is the criterion the issue
// leads with, plus the half that makes per-ARN eviction the right granularity.
//
// `DrainPool` would also stop the stale container; it would stop every other function's too, for
// a change to one function's code. Two warm functions, one updated, is the only shape that tells
// the two apart.
func TestLambdaWarmContainer_UpdateFunctionCodeEvictsOnlyThatFunction(t *testing.T) {
	t.Parallel()
	w := newLambdaWarmWorld(t)
	w.create(t, "updated-fn")
	w.create(t, "bystander-fn")
	w.warm("updated-fn")
	w.warm("bystander-fn")
	require.Len(t, w.pooled(), 2, "both functions start warm")

	resp := lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/updated-fn/code",
		map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte("replacement"))})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, []string{lambdaWarmARN("bystander-fn")}, w.pooled(),
		"UpdateFunctionCode drops the updated function's container and no other")
}

// TestLambdaWarmContainer_UpdateFunctionConfigurationEvicts pins the decision this PR records:
// the configuration update joins the code update.
//
// `Handler`, `Runtime` and `Environment` are fixed at `docker run` time on both the ZIP and the
// image path, so a container started before one of them changed is running the old handler under
// the old image with the old environment. The eviction is unconditional rather than per-member —
// `MemorySize` and `Timeout` reach no container — so this asserts on `Timeout` alone, the member
// that would be exempt under a per-member rule, which is what makes the assertion say something.
func TestLambdaWarmContainer_UpdateFunctionConfigurationEvicts(t *testing.T) {
	t.Parallel()
	w := newLambdaWarmWorld(t)
	w.create(t, "reconfigured-fn")
	w.warm("reconfigured-fn")

	resp := lambdaRequest(t, w.srv, http.MethodPut,
		"/2015-03-31/functions/reconfigured-fn/configuration", map[string]any{"Timeout": 30})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Empty(t, w.pooled(),
		"UpdateFunctionConfiguration drops the container, whichever member changed")
}

// TestLambdaWarmContainer_DeleteFunctionEvictsAndReleasesItsPackage covers the issue's second
// consequence and the leak beside it.
//
// The pool is keyed by an ARN derived from the name, so without the eviction a function recreated
// under the same name is invoked in the deleted function's container — the name-derived-key trap
// #903 fixed for pollers, on the live path. The deployment package is asserted gone in the same
// test because both are part of what `DeleteFunction` leaves behind.
func TestLambdaWarmContainer_DeleteFunctionEvictsAndReleasesItsPackage(t *testing.T) {
	t.Parallel()
	w := newLambdaWarmWorld(t)
	w.create(t, "doomed-fn")
	w.warm("doomed-fn")

	const zipKey = "function_zip:123456789012/us-east-1/doomed-fn"
	stored, err := w.state.Get(t.Context(), "lambda", zipKey)
	require.NoError(t, err)
	require.NotNil(t, stored, "an inline package is stored, so there is something to release")

	resp := lambdaRequest(t, w.srv, http.MethodDelete, "/2015-03-31/functions/doomed-fn", nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Empty(t, w.pooled(),
		"a function recreated under this name must not inherit the deleted one's container")

	stored, err = w.state.Get(t.Context(), "lambda", zipKey)
	require.NoError(t, err)
	assert.Nil(t, stored,
		"the deployment package goes with the function; it accumulated for the life of the process")
}

// TestLambdaWarmContainer_ARefusedUpdateEvictsNothing is the direction an eviction placed before
// the write gets wrong.
//
// `UpdateFunctionCode` for a function that does not exist is refused, and refusing it must not
// disturb a container — the pool is keyed by ARN, and a name that resolves to no function today
// can be created tomorrow. The warm entry here is injected for a *different*, live function, so
// an eviction keyed off the request's name rather than off a completed write would take it.
func TestLambdaWarmContainer_ARefusedUpdateEvictsNothing(t *testing.T) {
	t.Parallel()
	w := newLambdaWarmWorld(t)
	w.create(t, "live-fn")
	w.warm("live-fn")
	w.warm("absent-fn")

	resp := lambdaRequest(t, w.srv, http.MethodPut, "/2015-03-31/functions/absent-fn/code",
		map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte("replacement"))})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, []string{lambdaWarmARN("absent-fn"), lambdaWarmARN("live-fn")}, w.pooled(),
		"a refused update stored no code, so no container is running anything stale")
}

// TestLambdaWarmContainer_AnOperationThatChangesNoContainerKeepsIt is the honesty half: the
// eviction is scoped to three operations and must not spread to the rest of the plugin.
//
// A tag write and a read change nothing a container was started from, and dropping a container
// for either would trade a correctness fix for a cold start on every call a caller makes.
func TestLambdaWarmContainer_AnOperationThatChangesNoContainerKeepsIt(t *testing.T) {
	t.Parallel()
	w := newLambdaWarmWorld(t)
	w.create(t, "untouched-fn")
	w.warm("untouched-fn")

	resp := lambdaRequest(t, w.srv, http.MethodPost,
		"/2017-03-31/tags/"+lambdaWarmARN("untouched-fn"),
		map[string]any{"Tags": map[string]string{"env": "test"}})
	// The status is asserted because it was not: this was the one site in the tree already posting
	// to the published tags date, and it passed against the 404 of #1142 — "a tag write evicts
	// nothing" is not proven by a tag write that never happened.
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = lambdaRequest(t, w.srv, http.MethodGet, "/2015-03-31/functions/untouched-fn", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, []string{lambdaWarmARN("untouched-fn")}, w.pooled(),
		"neither a tag write nor a read changes what the container is running")
}

// TestLambdaWarmContainer_NoExecutorIsSafe covers the configuration every run but a Docker one
// is in: the plugin holds no executor at all, and the three operations must not care.
func TestLambdaWarmContainer_NoExecutorIsSafe(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)

	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "no-exec-fn",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::123456789012:role/svc",
		"Code":         map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte("code"))},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	for _, step := range []struct {
		method, path string
		body         any
		want         int
	}{
		{http.MethodPut, "/2015-03-31/functions/no-exec-fn/code",
			map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte("new"))}, http.StatusOK},
		{http.MethodPut, "/2015-03-31/functions/no-exec-fn/configuration",
			map[string]any{"Timeout": 15}, http.StatusOK},
		{http.MethodDelete, "/2015-03-31/functions/no-exec-fn", nil, http.StatusNoContent},
	} {
		resp := lambdaRequest(t, srv, step.method, step.path, step.body)
		assert.Equalf(t, step.want, resp.StatusCode, "%s %s", step.method, step.path)
		require.NoError(t, resp.Body.Close())
	}
}
