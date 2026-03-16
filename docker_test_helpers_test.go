package substrate_test

import (
	"io"
	"os/exec"
)

// isDockerAvailable reports whether the Docker daemon is reachable.
// Used to skip Docker-dependent integration tests in CI environments without Docker.
func isDockerAvailable() bool {
	cmd := exec.Command("docker", "info")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}
