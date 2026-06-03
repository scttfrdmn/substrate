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
