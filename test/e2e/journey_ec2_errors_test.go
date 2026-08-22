package e2e_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_EC2ErrorCodesReachTheSDK is #591's gate, and it is here rather than in
// emulator/ because only a real SDK client can hold this property. EC2 is the sole
// service on the "ec2" protocol, whose error document wraps the error in a plural
// <Errors> element; substrate served the Query protocol's <ErrorResponse><Error>
// instead, so aws-sdk-go-v2's ec2query deserializer — which reads the code at the
// XPath "Errors>Error>Code" — found nothing and kept its "UnknownError" default.
//
// A unit test asserting bytes cannot catch that on its own, and neither could any of
// substrate's AWS-CLI coverage: botocore's EC2QueryParser._get_error_root falls back
// to the document root when <Errors> is absent, so the CLI reported the right code out
// of the wrong document the whole time. The SDK is the strict reader, so the SDK is
// the gate.
//
// Both halves are checked because both were broken. Every writeError call site shares
// one serializer, so an organic error was as unmatchable as an injected one — the
// issue reported the fault and predicted the rest.
func TestJourney_EC2ErrorCodesReachTheSDK(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	// Retries off: a retried call would mask which attempt produced the error, and
	// the point here is the code on the response the caller actually sees.
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	// An organic error: a well-formed instance ID that resolves to nothing. Before the
	// fix this arrived as UnknownError, so a consumer's
	// "if code == InvalidInstanceID.NotFound" branch was unreachable and its test
	// passed while verifying nothing.
	_, err = client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{"i-0000000000000dead"},
	})
	if err == nil {
		t.Fatal("DescribeInstances on an absent ID: expected an error, got nil")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected a smithy.APIError, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "InvalidInstanceID.NotFound" {
		t.Fatalf("organic error: ErrorCode() = %q, want InvalidInstanceID.NotFound",
			apiErr.ErrorCode())
	}

	// An injected error, which is the half #591 reported. Times is -1 so the rule is
	// not spent by a single call, and the message is asserted too: a code that arrives
	// with the wrong message would mean the document was only half read.
	const capacityMsg = "There is not enough capacity to fulfill your request."
	seedFaultRules(t, ts, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:     "ec2",
			Operation:   "RunInstances",
			FaultType:   "error",
			ErrorCode:   "InsufficientInstanceCapacity",
			HTTPStatus:  500,
			ErrorMsg:    capacityMsg,
			Probability: 1.0,
			Times:       -1,
		}},
	})

	_, err = client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(journeyImage),
		InstanceType: "t3.micro",
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	if err == nil {
		t.Fatal("expected the seeded capacity error, got nil")
	}
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected a smithy.APIError, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "InsufficientInstanceCapacity" {
		t.Fatalf("injected error: ErrorCode() = %q, want InsufficientInstanceCapacity",
			apiErr.ErrorCode())
	}
	if apiErr.ErrorMessage() != capacityMsg {
		t.Fatalf("injected error: ErrorMessage() = %q, want %q",
			apiErr.ErrorMessage(), capacityMsg)
	}

	// A rule that matched nothing produces the same passing test as a consumer's
	// error branch working, so the count is what tells those two apart.
	if fired := faultsFired(t, ts); fired != 1 {
		t.Fatalf("fired = %d, want 1", fired)
	}

	// Clearing the fault restores the nominal path, which proves the assertion above
	// was reading the seeded error rather than a launch that would have failed anyway.
	// That argument only holds if the launch is otherwise valid, which is why both calls
	// name the bundled journeyImage rather than a fabricated ID RunInstances would refuse
	// with InvalidAMIID.NotFound (#733).
	clearFaultRules(t, ts)
	if _, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(journeyImage),
		InstanceType: "t3.micro",
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	}); err != nil {
		t.Fatalf("RunInstances after clearing the fault: %v", err)
	}
}
