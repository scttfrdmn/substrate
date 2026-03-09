package substrate_test

import (
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func TestFaultController_NoRules(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{Enabled: true}, 42)
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &substrate.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Errorf("expected no error, got %v", awsErr)
	}
	if delay != 0 {
		t.Errorf("expected zero delay, got %v", delay)
	}
}

func TestFaultController_Disabled(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{
		Enabled: false,
		Rules: []substrate.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 1.0},
		},
	}, 42)
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &substrate.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Errorf("disabled controller should not inject error, got %v", awsErr)
	}
	if delay != 0 {
		t.Errorf("disabled controller should not inject delay, got %v", delay)
	}
}

func TestFaultController_ErrorFault(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{
				Service:     "s3",
				FaultType:   "error",
				ErrorCode:   "ServiceUnavailable",
				HTTPStatus:  503,
				Probability: 1.0,
			},
		},
	}, 42)
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &substrate.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr == nil {
		t.Fatal("expected error fault, got nil")
	}
	if awsErr.Code != "ServiceUnavailable" {
		t.Errorf("expected code ServiceUnavailable, got %q", awsErr.Code)
	}
	if awsErr.HTTPStatus != 503 {
		t.Errorf("expected HTTP 503, got %d", awsErr.HTTPStatus)
	}
	if delay != 0 {
		t.Errorf("expected zero delay for error fault, got %v", delay)
	}
}

func TestFaultController_LatencyFault(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{
				FaultType:   "latency",
				LatencyMs:   200,
				Probability: 1.0,
			},
		},
	}, 42)
	req := &substrate.AWSRequest{Service: "dynamodb", Operation: "PutItem"}
	reqCtx := &substrate.RequestContext{}
	awsErr, delay := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Errorf("expected no error for latency fault, got %v", awsErr)
	}
	if delay != 200*time.Millisecond {
		t.Errorf("expected 200ms delay, got %v", delay)
	}
}

func TestFaultController_Probability_Zero(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 0.0},
		},
	}, 42)
	req := &substrate.AWSRequest{Service: "s3", Operation: "GetObject"}
	reqCtx := &substrate.RequestContext{}
	// With p=0, the roll is always >= p so it should never fire.
	// But per the implementation, p<=0 defaults to 1.0, so test p=0.001 approaches 0.
	// Actually looking at the code, p<=0 is treated as 1.0. So let's test by
	// using a very small value that won't fire with our seed.
	// Instead let's just test that 0.0 uses the 1.0 default and fires.
	awsErr, _ := fc.InjectFault(reqCtx, req)
	// p=0.0 defaults to 1.0 per spec, so it should fire
	if awsErr == nil {
		t.Error("expected fault to fire when probability defaults to 1.0")
	}
}

func TestFaultController_ServiceFilter(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{
				Service:     "s3",
				FaultType:   "error",
				ErrorCode:   "InternalError",
				Probability: 1.0,
			},
		},
	}, 42)
	// IAM request should not be affected by s3 rule
	iamReq := &substrate.AWSRequest{Service: "iam", Operation: "CreateUser"}
	reqCtx := &substrate.RequestContext{}
	awsErr, _ := fc.InjectFault(reqCtx, iamReq)
	if awsErr != nil {
		t.Errorf("s3 rule should not fire for iam request, got %v", awsErr)
	}
	// S3 request should be affected
	s3Req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	awsErr, _ = fc.InjectFault(reqCtx, s3Req)
	if awsErr == nil {
		t.Error("s3 rule should fire for s3 request")
	}
}

func TestFaultController_OperationFilter(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{
				Service:     "s3",
				Operation:   "PutObject",
				FaultType:   "error",
				ErrorCode:   "InternalError",
				Probability: 1.0,
			},
		},
	}, 42)
	reqCtx := &substrate.RequestContext{}
	// PutObject should fire
	putReq := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	awsErr, _ := fc.InjectFault(reqCtx, putReq)
	if awsErr == nil {
		t.Error("rule should fire for matching operation")
	}
	// GetObject should not fire
	getReq := &substrate.AWSRequest{Service: "s3", Operation: "GetObject"}
	awsErr, _ = fc.InjectFault(reqCtx, getReq)
	if awsErr != nil {
		t.Errorf("rule should not fire for non-matching operation, got %v", awsErr)
	}
}

func TestFaultController_UpdateConfig(t *testing.T) {
	fc := substrate.NewFaultController(substrate.FaultConfig{Enabled: false}, 42)
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject"}
	reqCtx := &substrate.RequestContext{}
	awsErr, _ := fc.InjectFault(reqCtx, req)
	if awsErr != nil {
		t.Error("disabled controller should not fire")
	}

	// Enable and add a rule
	fc.UpdateConfig(substrate.FaultConfig{
		Enabled: true,
		Rules: []substrate.FaultRule{
			{FaultType: "error", ErrorCode: "InternalError", Probability: 1.0},
		},
	})
	awsErr, _ = fc.InjectFault(reqCtx, req)
	if awsErr == nil {
		t.Error("expected fault after UpdateConfig enabled it")
	}
}
