package emulator_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A stack-tag propagation failure is reported to the caller, not only logged (#1138).
//
// Every error cfnPropagateStackTags returns is a substrate failure (a state read, a write, or a
// marshal), never a modeled AWS refusal; #1077 decided propagation does not refuse on a quota. Until
// #1138 StackDeployer.reconcileStackTags sent each one to logger.Warn and nowhere else, so the stack
// and the resource both reported success and the resource silently lacked the stack's tags.
//
// The reading recorded on DeployResult.TagPropagationFailures: real CloudFormation writes a resource's
// tags in its own create call, so "created, but its tags could not be written" is a state it cannot
// be in. Substrate propagates in a pass after the resources deploy, so the observation is
// substrate's own. It is reported in process, which invents no wire observation, and it is not
// DeployedResource.Error, which would report CREATE_FAILED for a resource that created.

// cfnFailPropagationState fails a Put on a key containing key whose value contains needle.
//
// That is exactly the propagation write. The VPC's create and the three-key stamp write the same
// record, but neither carries the stack's own tag values. The stack record carries them under
// another key, so the deploy itself is undisturbed.
type cfnFailPropagationState struct {
	emulator.StateManager
	key    string
	needle []byte
	fired  int
}

var errCFNPropagationFault = errors.New("injected stack-tag propagation fault")

func (s *cfnFailPropagationState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if strings.Contains(key, s.key) && bytes.Contains(value, s.needle) {
		s.fired++
		return errCFNPropagationFault
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

const cfnPropagationFailureMarker = "fail-propagation-1138"

func TestCFN_AStackTagPropagationFailureIsReportedToTheCaller(t *testing.T) {
	state := &cfnFailPropagationState{
		StateManager: emulator.NewMemoryStateManager(),
		key:          "vpc:",
		needle:       []byte(cfnPropagationFailureMarker),
	}
	logger := &cfnRecordingLogger{}
	f := newCFNStampFixtureWithLogger(t, state, logger)

	result, err := f.deployer.DeployWithOptions(context.Background(), `{"Resources": {
		"Vpc":    {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.14.0.0/16"}},
		"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "propagation-failure-1138"}}
	}}`, "propagation-failure-stack", nil,
		emulator.CFNDeployOptions{Tags: map[string]string{"team": cfnPropagationFailureMarker}})
	require.NoError(t, err)
	require.Positive(t, state.fired, "the propagation write to the VPC was faulted")

	// The failure is on the result, naming the resource it struck and the error.
	require.Len(t, result.TagPropagationFailures, 1, "%+v", result.TagPropagationFailures)
	failure := result.TagPropagationFailures[0]
	assert.Equal(t, "Vpc", failure.LogicalID)
	assert.Equal(t, "AWS::EC2::VPC", failure.Type)
	assert.NotEmpty(t, failure.PhysicalID)
	assert.Contains(t, failure.Error, errCFNPropagationFault.Error())

	// The stack and the resource keep the status they earned: the VPC created, and so did the stack.
	assert.Equal(t, "CREATE_COMPLETE", result.Status)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "%s: a tag failure is not a resource failure", r.LogicalID)
	}

	// The bucket, whose propagation was not faulted, carries the tag, so the failure is per resource.
	assert.Contains(t, f.bucketTagsFor(t, "propagation-failure-1138"), "team="+cfnPropagationFailureMarker)

	// The log line remains; it is no longer the only report.
	assert.Contains(t, logger.joined(), "could not propagate")
}

// The silent skip is not a failure: a resource no arm models tags for reports nothing.
func TestCFN_AServiceThatModelsNoTagsReportsNoPropagationFailure(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	result, err := f.deployer.DeployWithOptions(context.Background(), `{"Resources": {
		"Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": "/propagate/none-1138"}}
	}}`, "untaggable-1138", nil, emulator.CFNDeployOptions{Tags: map[string]string{"team": "platform"}})
	require.NoError(t, err)
	assert.Empty(t, result.TagPropagationFailures, "a silent skip is not a propagation failure")
}
