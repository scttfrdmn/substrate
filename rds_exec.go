package substrate

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// RDSContainerHandle tracks a running Postgres Docker container.
type RDSContainerHandle struct {
	// ContainerID is the Docker container ID returned by "docker run -d".
	ContainerID string

	// HostPort is the local TCP port mapped to the container's 5432.
	HostPort int
}

// RDSExecutor launches and stops Postgres Docker containers for RDS instances.
// It is safe for concurrent use and gracefully falls back (returning an error)
// when Docker is unavailable.
type RDSExecutor struct {
	logger Logger
	mu     sync.Mutex
	active map[string]*RDSContainerHandle // dbInstanceID → handle
}

// NewRDSExecutor creates an RDSExecutor with the given logger.
func NewRDSExecutor(logger Logger) *RDSExecutor {
	return &RDSExecutor{
		logger: logger,
		active: make(map[string]*RDSContainerHandle),
	}
}

// StartPostgres runs a postgres:latest container for the given RDS instance,
// waits up to 30 s for it to accept connections, and returns the handle.
// The caller is responsible for calling StopContainer when the instance is deleted.
func (e *RDSExecutor) StartPostgres(ctx context.Context, instanceID, user, password string) (*RDSContainerHandle, error) {
	port, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("rds executor find port: %w", err)
	}

	args := []string{
		"run", "-d",
		"-p", fmt.Sprintf("%d:5432", port),
		"--name", "substrate-rds-" + instanceID,
		"-e", "POSTGRES_USER=" + user,
		"-e", "POSTGRES_PASSWORD=" + password,
		"postgres:latest",
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, runErr := cmd.Output()
	if runErr != nil {
		return nil, fmt.Errorf("rds executor docker run: %w", runErr)
	}

	containerID := strings.TrimSpace(string(out))
	h := &RDSContainerHandle{ContainerID: containerID, HostPort: port}

	if waitErr := e.waitPostgresReady(ctx, containerID, user, 30*time.Second); waitErr != nil {
		_ = e.StopContainer(context.Background(), containerID)
		return nil, fmt.Errorf("rds executor postgres not ready: %w", waitErr)
	}

	e.mu.Lock()
	e.active[instanceID] = h
	e.mu.Unlock()
	return h, nil
}

// waitPostgresReady polls pg_isready inside the container until it succeeds
// or the deadline is exceeded.
func (e *RDSExecutor) waitPostgresReady(ctx context.Context, containerID, user string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cmd := exec.Command("docker", "exec", containerID, "pg_isready", "-U", user)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if cmd.Run() == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("postgres did not become ready within %s", timeout)
}

// StopContainer stops and removes the Docker container identified by containerID.
func (e *RDSExecutor) StopContainer(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "stop", containerID)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// StopAll stops all active Postgres containers.
func (e *RDSExecutor) StopAll(ctx context.Context) error {
	e.mu.Lock()
	handles := make(map[string]*RDSContainerHandle, len(e.active))
	for k, v := range e.active {
		handles[k] = v
	}
	e.active = make(map[string]*RDSContainerHandle)
	e.mu.Unlock()

	var lastErr error
	for instanceID, h := range handles {
		if err := e.StopContainer(ctx, h.ContainerID); err != nil {
			e.logger.Warn("rds executor: stop container", "instance", instanceID, "err", err)
			lastErr = err
		}
	}
	return lastErr
}
