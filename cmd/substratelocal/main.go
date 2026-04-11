// Command substratelocal wraps a command with AWS environment variables
// pre-configured to target a running Substrate (or LocalStack) server.
//
// Usage:
//
//	substratelocal [--endpoint URL] <command> [args...]
//
// The following environment variables are injected into the child process:
//
//	AWS_ENDPOINT_URL          set to the Substrate server URL
//	LOCALSTACK_ENDPOINT       set to the Substrate server URL (Prism compat)
//	AWS_ACCESS_KEY_ID         "test"
//	AWS_SECRET_ACCESS_KEY     "test"
//	AWS_DEFAULT_REGION        "us-east-1" (unless already set)
//	AWS_REGION                "us-east-1" (unless already set)
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "substratelocal:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("substratelocal", flag.ContinueOnError)
	endpoint := fs.String("endpoint", "", "Substrate server URL (default: $SUBSTRATE_ENDPOINT or http://localhost:4566)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: substratelocal [--endpoint URL] <command> [args...]")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		fs.Usage()
		return fmt.Errorf("no command specified")
	}

	// Resolve endpoint: flag > env > default.
	ep := *endpoint
	if ep == "" {
		ep = os.Getenv("SUBSTRATE_ENDPOINT")
	}
	if ep == "" {
		ep = "http://localhost:4566"
	}

	cmd := exec.Command(remaining[0], remaining[1:]...) //nolint:gosec // nosemgrep
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = injectEnv(os.Environ(), ep)

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		return err
	}
	return nil
}

// injectEnv returns a copy of env with AWS variables set for Substrate.
// Existing AWS_DEFAULT_REGION and AWS_REGION are preserved if already set.
func injectEnv(env []string, endpoint string) []string {
	overrides := map[string]string{
		"AWS_ENDPOINT_URL":      endpoint,
		"LOCALSTACK_ENDPOINT":   endpoint,
		"AWS_ACCESS_KEY_ID":     "test",
		"AWS_SECRET_ACCESS_KEY": "test",
	}
	// Only set region vars when not already present.
	regionDefaults := map[string]string{
		"AWS_DEFAULT_REGION": "us-east-1",
		"AWS_REGION":         "us-east-1",
	}

	out := make([]string, 0, len(env)+len(overrides)+len(regionDefaults))
	regionSeen := map[string]bool{}

	for _, kv := range env {
		key := envKey(kv)
		if _, isOverride := overrides[key]; isOverride {
			continue // will be replaced below
		}
		if _, isRegion := regionDefaults[key]; isRegion {
			regionSeen[key] = true
		}
		out = append(out, kv)
	}

	for k, v := range overrides {
		out = append(out, k+"="+v)
	}
	for k, v := range regionDefaults {
		if !regionSeen[k] {
			out = append(out, k+"="+v)
		}
	}

	return out
}

// envKey returns the variable name portion of a KEY=VALUE string.
func envKey(kv string) string {
	for i := 0; i < len(kv); i++ {
		if kv[i] == '=' {
			return kv[:i]
		}
	}
	return kv
}
