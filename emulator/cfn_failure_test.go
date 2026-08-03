package emulator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestCFN_RefusedResourceIsRecordedAsAFailure is the gate for the defect #516's
// own reproduction exposed without naming: a resource the plugin refused was
// reported as though it had been created.
//
// Plugins signal a client error two ways, and both are in wide use. S3 and IAM
// return a 4xx *AWSResponse with a nil error — the same shape a real endpoint puts
// on the wire — while EC2, ECS, CloudWatch Logs and SQS return an *AWSError.
// StackDeployer only ever looked at the error return, so a refused S3 or IAM
// resource recorded no error at all, and DescribeStackResources maps an empty
// error to CREATE_COMPLETE. The stack reported success for a bucket that does not
// exist.
//
// The table covers one resource per convention so that neither path can regress
// into the other.
func TestCFN_RefusedResourceIsRecordedAsAFailure(t *testing.T) {
	cases := []struct {
		name string
		// convention names how the plugin reports the refusal, because that is
		// the axis the defect lay along.
		convention  string
		tmpl        string
		logicalID   string
		wantCode    string
		wantMessage string
	}{
		{
			// #516's reproduction: a name S3 rejects (under three characters —
			// deployS3Bucket lowercases, so a case violation would not survive to
			// the plugin), refused with a 400, and the stack reported
			// CREATE_COMPLETE.
			name:        "S3InvalidBucketName",
			convention:  "4xx response, nil error",
			tmpl:        `{"Resources": {"B": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "no"}}}}`,
			logicalID:   "B",
			wantCode:    "InvalidBucketName",
			wantMessage: "not valid",
		},
		{
			// IAM is the other response-style plugin, and a trust policy that is
			// not JSON is refused with MalformedPolicyDocument.
			name:       "IAMMalformedPolicyDocument",
			convention: "4xx response, nil error",
			tmpl: `{"Resources": {"R": {"Type": "AWS::IAM::Role", "Properties": {
				"RoleName": "refused-role",
				"AssumeRolePolicyDocument": "this is not json"}}}}`,
			logicalID:   "R",
			wantCode:    "MalformedPolicyDocument",
			wantMessage: "not valid JSON",
		},
		{
			// EC2 returns an *AWSError, which the deployer already handled. The
			// case is here so the fix cannot quietly break the path that worked.
			name:       "EC2GroupNotFound",
			convention: "*AWSError",
			tmpl: `{"Resources": {"I": {"Type": "AWS::EC2::SecurityGroupIngress", "Properties": {
				"GroupId": "sg-does-not-exist",
				"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
				"CidrIp": "0.0.0.0/0"}}}}`,
			logicalID:   "I",
			wantCode:    "InvalidGroup.NotFound",
			wantMessage: "Security group not found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newFullTestDeployer(t)
			result, err := d.Deploy(context.Background(), tc.tmpl, "refused-"+tc.name, nil)

			// The deploy itself succeeds: a per-resource failure is recorded on
			// the resource, not returned as a stack-level error. That is the
			// existing behavior and this change does not alter it.
			require.NoError(t, err)
			require.Len(t, result.Resources, 1)

			dr := result.Resources[0]
			assert.Equal(t, tc.logicalID, dr.LogicalID)
			require.NotEmpty(t, dr.Error,
				"a resource refused via %s must record the failure", tc.convention)
			assert.Contains(t, dr.Error, tc.wantCode,
				"the recorded reason must name the plugin's own error code, not a bare status")
			assert.Contains(t, dr.Error, tc.wantMessage,
				"the recorded reason must carry the plugin's message")
		})
	}
}

// TestCFN_RefusedBucketDoesNotExist pins the other half of #516's reproduction:
// the resource the stack refused really is absent, so a consumer reading it back
// gets a miss. Reporting CREATE_FAILED would be no use if the bucket were somehow
// half-created.
func TestCFN_RefusedBucketDoesNotExist(t *testing.T) {
	d, state := newTestDeployerWithState(t)
	tmpl := `{"Resources": {"B": {"Type": "AWS::S3::Bucket",
	                              "Properties": {"BucketName": "no"}}}}`

	result, err := d.Deploy(context.Background(), tmpl, "refused-absent", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Contains(t, result.Resources[0].Error, "InvalidBucketName")

	v, err := state.Get(context.Background(), "s3", "bucket:no")
	require.NoError(t, err)
	assert.Nil(t, v, "the refused bucket must not be in state")
}

// TestCFN_RefusedBucketSkipsItsSubresources pins that a refused bucket's follow-up
// configuration requests are never sent. deployS3Bucket discarded the response
// entirely, so it went on to configure versioning on a bucket that does not exist.
//
// The reason to care is the event log, not the recorded error: the follow-up's own
// result is discarded, so it could not have overwritten the reason. What it did do
// was record a PUT ?versioning that was refused with NoSuchBucket — a request no
// real client would have sent, in the log substrate replays. So the assertion is on
// the stream: the refused bucket's PUT is there, and nothing follows it.
func TestCFN_RefusedBucketSkipsItsSubresources(t *testing.T) {
	d, store := newTestDeployerWithStore(t)
	tmpl := `{"Resources": {"B": {"Type": "AWS::S3::Bucket", "Properties": {
		"BucketName": "no",
		"VersioningConfiguration": {"Status": "Enabled"}}}}}`

	ctx := context.Background()
	result, err := d.Deploy(ctx, tmpl, "refused-subresources", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Contains(t, result.Resources[0].Error, "InvalidBucketName")

	events, err := store.GetStream(ctx, "refused-subresources")
	require.NoError(t, err)
	require.Len(t, events, 1,
		"only the refused PUT belongs in the log; a follow-up request against a "+
			"bucket that does not exist is one no real client would have sent")
	assert.Equal(t, "s3", events[0].Service)
	for _, ev := range events {
		assert.NotContains(t, ev.Error, "NoSuchBucket",
			"no request may be sent against the refused bucket")
	}
}

// TestCFN_SucceededResourceRecordsNoError is the negative control. The status
// check must fire on a refusal and only on a refusal: were it inverted, or were
// the threshold off by one, a 200 or a 3xx would start reporting CREATE_FAILED for
// every stack substrate deploys.
func TestCFN_SucceededResourceRecordsNoError(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{"Resources": {
		"B": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "accepted-bucket"}},
		"R": {"Type": "AWS::IAM::Role", "Properties": {
			"RoleName": "accepted-role",
			"AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}}},
		"Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "accepted-queue"}}
	}}`

	result, err := d.Deploy(context.Background(), tmpl, "accepted", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s must not report a failure", r.LogicalID)
		assert.NotEmpty(t, r.PhysicalID, "resource %s must have a physical ID", r.LogicalID)
	}
}

// TestCFN_FailedResourceDoesNotStopTheStack is the DO_NOTHING case, and it is this
// test rather than a new one because the behavior it pins did not go away — it
// stopped being unconditional.
//
// Until #520 this asserted "substrate does not roll back" as substrate's whole
// contract: a refused resource left the stack complete and every resource beside it
// in place. That is now what OnFailure=DO_NOTHING asks for, so the same assertions
// stand with the option named, and the difference between the old default and the new
// one is exactly the option.
//
// Substrate still deploys the resources *after* the failure, where real
// CloudFormation stops at the first one. That divergence is deliberate and predates
// rollback: it is what makes a single deploy report every refusal a template contains
// rather than only the first, which is the more useful observable for the
// validation-run case substrate exists to serve. The stack status is the same either
// way, and the status is what a caller keys off.
func TestCFN_FailedResourceDoesNotStopTheStack(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{"Resources": {
		"Refused": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "no"}},
		"Accepted": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "after-failure"}}
	}}`

	result, err := d.DeployWithOptions(context.Background(), tmpl, "partial-failure", nil,
		emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDoNothing})
	require.NoError(t, err, "a failed resource is a stack status, not a failed call")
	require.Len(t, result.Resources, 2)

	assert.Equal(t, "CREATE_FAILED", result.Status,
		"DO_NOTHING leaves the failure standing rather than rolling it back")
	assert.Contains(t, result.StatusReason, "InvalidBucketName",
		"the stack's reason names what failed")
	assert.Empty(t, result.ResourceDeletions, "DO_NOTHING deletes nothing")

	byLogicalID := map[string]emulator.DeployedResource{}
	for _, r := range result.Resources {
		byLogicalID[r.LogicalID] = r
	}
	assert.Contains(t, byLogicalID["Refused"].Error, "InvalidBucketName")
	assert.Empty(t, byLogicalID["Accepted"].Error,
		"a resource after the failure is still deployed")
	assert.Equal(t, "after-failure", byLogicalID["Accepted"].PhysicalID,
		"the queue should have been deployed despite the earlier failure")
}
