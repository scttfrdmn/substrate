package emulator_test

import (
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestLambdaExecutor_GracefulDegradation_WithDocker verifies that Execute falls
// back to the stub response when Docker is available but the container fails to
// start (e.g. because the image does not exist locally and cannot be pulled).
// This test is skipped when Docker is not available.
func TestLambdaExecutor_GracefulDegradation_WithDocker(t *testing.T) {
	t.Parallel()
	if !isDockerAvailable() {
		t.Skip("Docker not available")
	}
	logger := emulator.NewDefaultLogger(-4, false)
	// Force available=true so Execute does not take the stub-path immediately.
	exec := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, true /* force available */)
	defer exec.StopAll()

	// Use an Image-type function with a clearly invalid image URI so that
	// "docker run" fails immediately without pulling anything.
	fn := emulator.LambdaFunction{
		FunctionName: "docker-fail-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:docker-fail-fn",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		PackageType:  "Image",
		ImageURI:     "substrate-test-nonexistent-image-abc123:latest",
	}

	result, err := exec.Execute(t.Context(), fn, nil, []byte(`{}`))
	// Execute must not return an error; it degrades gracefully to the stub.
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	want := `{"statusCode":200,"body":"null"}`
	if string(result) != want {
		t.Errorf("Execute = %s; want %s", result, want)
	}
}

// TestLambdaExecutor_StopAll_WithPool verifies that StopAll can stop a pool
// that contains entries (even if the container IDs are bogus — docker stop
// will return a non-zero exit but StopAll must not panic).
// This test is skipped when Docker is not available.
func TestLambdaExecutor_StopAll_WithPool(t *testing.T) {
	t.Parallel()
	if !isDockerAvailable() {
		t.Skip("Docker not available")
	}
	logger := emulator.NewDefaultLogger(-4, false)
	exec := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{
		ReplayMode:  "live",
		WarmPoolTTL: 5 * time.Minute,
	}, logger, false)
	// Inject a fake pool entry to exercise the stop-container path.
	emulator.InjectPoolEntryForTest(exec, "arn:aws:lambda:us-east-1:000000000000:function:f",
		"nonexistent-container-id-xyz")
	// StopAll logs the error but must not panic.
	exec.StopAll()
}
