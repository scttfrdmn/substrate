package emulator_test

import (
	"context"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestNewRDSExecutor verifies that NewRDSExecutor returns a non-nil executor.
func TestNewRDSExecutor(t *testing.T) {
	t.Parallel()
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewRDSExecutor(logger)
	if exec == nil {
		t.Fatal("NewRDSExecutor returned nil")
	}
}

// TestRDSExecutorStopAll_EmptyPool verifies that StopAll on an empty executor
// returns nil without panicking.
func TestRDSExecutorStopAll_EmptyPool(t *testing.T) {
	t.Parallel()
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewRDSExecutor(logger)
	if err := exec.StopAll(context.Background()); err != nil {
		t.Errorf("StopAll on empty pool: %v", err)
	}
}

// TestRDSExecutorStopAll_WithFakeHandle verifies that StopAll iterates over
// active containers and does not panic when docker stop fails for a bogus ID.
// This test is skipped when Docker is not available.
func TestRDSExecutorStopAll_WithFakeHandle(t *testing.T) {
	t.Parallel()
	if !isDockerAvailable() {
		t.Skip("Docker not available")
	}
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewRDSExecutor(logger)
	// Inject a fake handle so StopAll has something to iterate over.
	emulator.InjectRDSHandleForTest(exec, "fake-instance", "nonexistent-container-id-rds")
	// StopAll will log an error for the fake container but must not panic.
	_ = exec.StopAll(context.Background())

	// And it empties the active set, so a second call — a shutdown after a state reset,
	// say — does not stop the same containers again (#903).
	if n := emulator.RDSActiveContainerCountForTest(exec); n != 0 {
		t.Errorf("active containers after StopAll = %d; want 0", n)
	}
}

// TestRDSStopContainer_ForgetsTheHandle asserts that stopping one container drops its
// active entry. Until #903 nothing did, so the map grew for the life of the process and
// a state reset left an entry pointing at a container it could no longer reach.
// This test is skipped when Docker is not available.
func TestRDSStopContainer_ForgetsTheHandle(t *testing.T) {
	t.Parallel()
	if !isDockerAvailable() {
		t.Skip("Docker not available")
	}
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewRDSExecutor(logger)
	emulator.InjectRDSHandleForTest(exec, "forget-instance", "nonexistent-container-id-forget")

	// The docker calls fail for a container that does not exist; the bookkeeping must
	// happen regardless, or the entry outlives every way of reaching it.
	_ = emulator.RDSStopContainerForTest(exec, context.Background(), "nonexistent-container-id-forget")

	if n := emulator.RDSActiveContainerCountForTest(exec); n != 0 {
		t.Errorf("active containers after StopContainer = %d; want 0", n)
	}
}

// TestRDSStopContainer_NonExistent verifies that StopContainer returns an error
// for a non-existent container ID (Docker returns non-zero exit).
// This test is skipped when Docker is not available.
func TestRDSStopContainer_NonExistent(t *testing.T) {
	t.Parallel()
	if !isDockerAvailable() {
		t.Skip("Docker not available")
	}
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewRDSExecutor(logger)
	err := emulator.RDSStopContainerForTest(exec, context.Background(), "nonexistent-container-id-xyz")
	if err == nil {
		t.Error("expected error stopping non-existent container; got nil")
	}
}

// TestRDSExecutor_SkipWhenDockerAbsent verifies that StartPostgres returns
// an error (gracefully) when Docker is not available.
func TestRDSExecutor_SkipWhenDockerAbsent(t *testing.T) {
	t.Parallel()
	if isDockerAvailable() {
		t.Skip("Docker is available; this test only runs when Docker is absent")
	}
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewRDSExecutor(logger)
	_, err := exec.StartPostgres(context.Background(), "test-instance", "postgres", "secret")
	if err == nil {
		t.Error("expected an error when Docker is absent; got nil")
	}
}
