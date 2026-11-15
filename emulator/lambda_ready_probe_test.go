package emulator_test

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Lambda container readiness probe does not invoke the handler (#1129). The runtime interface
// emulator routes one path, /2015-03-31/functions/function/invocations, and runs the function for any
// method on it; the probe asks a path it does not route. A local listener stands in for the
// container, so no Docker daemon is needed.

const lambdaInvocationPath = "/2015-03-31/functions/function/invocations"

func TestLambdaReadyProbe_NeverRequestsTheInvocationEndpoint(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var seen []string
	// A mux shaped like the emulator's: the invocation path and nothing else.
	mux := http.NewServeMux()
	mux.HandleFunc(lambdaInvocationPath, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, "invoked "+r.Method)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path)
		mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	require.NoError(t, emulator.RIEReadyProbeForTest(port), "a 404 from an unrouted path means the server is up")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"GET /"}, seen, "one request, to a path the runtime interface does not route")
	for _, s := range seen {
		assert.NotContains(t, s, "invoked", "the probe must not reach the function")
	}
}

func TestLambdaReadyProbe_NoListenerIsNotReady(t *testing.T) {
	t.Parallel()
	port, err := emulator.FindFreePort()
	require.NoError(t, err)
	assert.Error(t, emulator.RIEReadyProbeForTest(port), "a port nothing listens on is not ready")
}

func TestLambdaReadyProbe_WaitReadyUsesTheProbe(t *testing.T) {
	t.Parallel()
	e := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{}, emulator.NewDefaultLogger(slog.LevelError, false), false)
	t.Cleanup(e.StopAll)

	var probed []int
	require.NoError(t, e.WaitReadyForTest(4242, 0, func(port int) error {
		probed = append(probed, port)
		return nil
	}))
	assert.Equal(t, []int{4242}, probed, "readiness is the probe answering for the container's port")

	// A probe that never answers fails once the deadline has passed; a zero timeout tries exactly once.
	calls := 0
	err := e.WaitReadyForTest(4242, 0, func(int) error {
		calls++
		return errors.New("connection refused")
	})
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}
