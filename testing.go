package substrate

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestServer is a running Substrate server for use in integration tests.
// Create one with [StartTestServer]; it is automatically shut down when the
// test ends via t.Cleanup.
type TestServer struct {
	// URL is the base URL of the server, e.g. "http://127.0.0.1:54321".
	URL string
	// Port is the TCP port the server is listening on.
	Port int
	srv  *Server
}

// StartTestServer starts an in-process Substrate server on a random port,
// registers all default plugins, and schedules t.Cleanup to shut it down.
// The returned [TestServer] is ready to accept requests when this function
// returns.
func StartTestServer(t *testing.T) *TestServer {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Server.Address = "127.0.0.1:0"
	cfg.EventStore.Enabled = false
	cfg.Log.Level = "error"

	state := NewMemoryStateManager()
	tc := NewTimeController(time.Now())
	registry := NewPluginRegistry()
	logger := NewDefaultLogger(slog.LevelError, false)

	store := NewEventStore(cfg.EventStore.ToEventStoreConfig(), WithTimeController(tc))

	ctx := context.Background()
	if err := RegisterDefaultPlugins(ctx, registry, state, tc, logger, store, nil); err != nil {
		t.Fatalf("StartTestServer: register plugins: %v", err)
	}

	// Bind to a random port and keep the listener open to avoid the TOCTOU race
	// between port reservation and server bind.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartTestServer: listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	srv := NewServer(*cfg, registry, store, state, tc, logger)

	srvCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(srvCtx, ln)
	}()

	// Wait until the health endpoint responds.
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, pingErr := http.Get(baseURL + "/health") //nolint:noctx
		if pingErr == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.Stop(shutCtx)
		<-done
	})

	return &TestServer{URL: baseURL, Port: port, srv: srv}
}

// ResetState wipes all server state. Call this between test cases that share
// a single [TestServer] instance to avoid state leaking across cases.
func (ts *TestServer) ResetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/state/reset", "application/json", nil) //nolint:noctx
	if err != nil {
		t.Fatalf("ResetState: post: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ResetState: unexpected status %d", resp.StatusCode)
	}
}
