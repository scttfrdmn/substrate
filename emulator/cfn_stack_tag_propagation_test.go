package emulator_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A stack's tags reaching the resources it creates (#764).
//
// #764's first half recorded `Tags.member.N` on the stack and reported it back; this is the half
// a policy or a cost report reads. AWS states it on `CreateStack`'s own `Tags` member —
// "CloudFormation also propagates these tags to the resources created in the stack" — and
// `UpdateStack` settles what happens when a tag leaves the stack, which is the case that needs
// the stack's *previous* tag set to answer at all.
//
// Every assertion reads the tags back through the owning service's own tag call, for the reason
// #765's suite gives: a tag written to a state key the service does not read would satisfy a
// state assertion and no caller.

// cfnExpectedTags is [cfnExpectedStamp] plus the stack tags a resource carries, in the sorted
// "key=value" form the fixture's readers return.
func cfnExpectedTags(stackName, stackID, logicalID string, stackTags map[string]string) []string {
	out := cfnExpectedStamp(stackName, stackID, logicalID)
	for k, v := range stackTags {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// tagQueueDirectly sets one tag on a queue through SQS's own TagQueue, which is how a caller
// sets a tag the stack must not clobber. TagQueue merges rather than replacing, so the stamp and
// the stack tags already on the queue survive it.
func (f *cfnStampFixture) tagQueueDirectly(t *testing.T, queueName, key, value string) {
	t.Helper()
	url := "https://sqs." + cfnStampRegion + ".amazonaws.com/" + cfnStampAccount + "/" + queueName
	resp, err := f.sqs.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "sqs",
		Operation: "TagQueue",
		Params: map[string]string{
			"Action":      "TagQueue",
			"QueueUrl":    url,
			"Tag.1.Key":   key,
			"Tag.1.Value": value,
		},
		Headers: map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// cfnTaggedStackTemplate is two named resources of two different services, so an update
// redeploys the same resources rather than minting new ones — which is what makes a tag change
// and a tag removal observable at all. A VPC would be re-created on every update, since nothing
// names it.
const cfnTaggedStackTemplate = `{
	"Resources": {
		"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "propagate-bucket"}},
		"Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "propagate-queue"}}
	},
	"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
}`

// TestCFN_AStackTagReachesEveryResourceThatCanCarryIt is #764's third criterion, across every
// service the stamp reaches.
//
// One subtest per service rather than one assertion for the stack, for #746's reason: the tags
// are written from a single place behind a type switch, so a service the switch does not claim
// is skipped silently and only a per-service assertion catches it.
func TestCFN_AStackTagReachesEveryResourceThatCanCarryIt(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "propagating-stack"
	stackTags := map[string]string{"team": "platform", "env": "test"}

	result, err := f.deployer.DeployWithOptions(context.Background(), cfnBeyondEC2Template,
		stackName, nil, emulator.CFNDeployOptions{Tags: stackTags})
	require.NoError(t, err)
	ids := physicalIDs(t, result)
	arns := arnsByLogicalID(t, result)
	stackID := result.Outputs["StackId"]
	require.NotEmpty(t, stackID)

	want := func(logicalID string) []string {
		return cfnExpectedTags(stackName, stackID, logicalID, stackTags)
	}

	t.Run("S3 bucket", func(t *testing.T) {
		assert.Equal(t, want("Bucket"), f.bucketTagsFor(t, ids["Bucket"]))
	})
	t.Run("Lambda function", func(t *testing.T) {
		assert.Equal(t, want("Fn"), f.functionTagsFor(t, arns["Fn"]))
	})
	t.Run("SQS queue", func(t *testing.T) {
		assert.Equal(t, want("Queue"), f.queueTagsFor(t, ids["Queue"]))
	})
	t.Run("DynamoDB table", func(t *testing.T) {
		assert.Equal(t, want("Table"), f.tableTagsFor(t, ids["Table"]))
	})
	t.Run("EC2 VPC", func(t *testing.T) {
		assert.Equal(t, want("Vpc"), f.tagsFor(t, ids["Vpc"]))
	})
	t.Run("ELBv2 load balancer", func(t *testing.T) {
		assert.Equal(t, want("Lb"), f.elbTagsFor(t, arns["Lb"]))
	})
	t.Run("ELBv2 target group", func(t *testing.T) {
		assert.Equal(t, want("Tg"), f.elbTagsFor(t, arns["Tg"]))
	})
}

// TestCFN_AnUpdateAddsChangesAndRemovesATagOnTheResources is #764's second criterion, all three
// verbs in one stack because they share the one mechanism: the stack's previous tag set.
//
// The removal is the half that cannot be done from the new tags alone — nothing in a request
// that no longer mentions `env` says which resources ever carried it — and it is why the stack's
// stored tags are read before the update overwrites them.
func TestCFN_AnUpdateAddsChangesAndRemovesATagOnTheResources(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "updating-stack"

	created, err := f.deployer.DeployWithOptions(context.Background(), cfnTaggedStackTemplate,
		stackName, nil, emulator.CFNDeployOptions{
			Tags: map[string]string{"team": "platform", "env": "test"},
		})
	require.NoError(t, err)
	ids := physicalIDs(t, created)
	stackID := created.Outputs["StackId"]

	require.Equal(t,
		cfnExpectedTags(stackName, stackID, "Bucket",
			map[string]string{"team": "platform", "env": "test"}),
		f.bucketTagsFor(t, ids["Bucket"]))

	// `team` changes value, `env` leaves the stack, `owner` joins it.
	updated, err := f.deployer.UpdateStack(context.Background(), cfnTaggedStackTemplate,
		stackName, nil, emulator.CFNDeployOptions{
			Tags: map[string]string{"team": "infra", "owner": "sre"},
		})
	require.NoError(t, err)
	require.Empty(t, cfnFailedLogicalIDs(updated), "the update redeployed both resources")

	after := map[string]string{"team": "infra", "owner": "sre"}
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Bucket", after),
		f.bucketTagsFor(t, ids["Bucket"]),
		"the bucket carries the new value, not the old one, and no longer carries env")
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Queue", after),
		f.queueTagsFor(t, ids["Queue"]),
		"and so does the queue, through a different tag store")
}

// TestCFN_AnUpdateThatOmitsTagsLeavesTheResourcesAlone pins the other half of AWS's update rule
// on the resources rather than on the stack: "If you don't specify this parameter,
// CloudFormation doesn't modify the stack's tags."
//
// The stack's tags being preserved is #764's first half and already pinned; what this asserts is
// that the preserved set is not then *re-propagated* into something different — an update that
// re-derived the tags would be the way a caller's own resource tag got quietly overwritten on
// every deployment.
func TestCFN_AnUpdateThatOmitsTagsLeavesTheResourcesAlone(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "omitting-stack"
	stackTags := map[string]string{"team": "platform"}

	created, err := f.deployer.DeployWithOptions(context.Background(), cfnTaggedStackTemplate,
		stackName, nil, emulator.CFNDeployOptions{Tags: stackTags})
	require.NoError(t, err)
	ids := physicalIDs(t, created)
	before := f.bucketTagsFor(t, ids["Bucket"])

	_, err = f.deployer.UpdateStack(context.Background(), cfnTaggedStackTemplate, stackName, nil,
		emulator.CFNDeployOptions{})
	require.NoError(t, err)

	assert.Equal(t, before, f.bucketTagsFor(t, ids["Bucket"]))
	assert.Equal(t, cfnExpectedTags(stackName, created.Outputs["StackId"], "Queue", stackTags),
		f.queueTagsFor(t, ids["Queue"]))
}

// TestCFN_ATagTheCallerSetDirectlyIsNotClobbered is #764's fourth criterion.
//
// The caller sets `team` on the queue through SQS's own `TagQueue` to a value the stack does not
// carry. From then on that key is theirs: an update that changes the stack's `team` does not
// overwrite it, and one that drops `team` from the stack does not delete it. What decides this
// is the value comparison against the stack's previous tag set — the queue holds `mine` where
// the stack propagated `platform`, so the tag cannot have come from the stack.
//
// The resource beside it is the control: the bucket never had its `team` touched, so it still
// tracks the stack.
func TestCFN_ATagTheCallerSetDirectlyIsNotClobbered(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "caller-tag-stack"

	created, err := f.deployer.DeployWithOptions(context.Background(), cfnTaggedStackTemplate,
		stackName, nil, emulator.CFNDeployOptions{Tags: map[string]string{"team": "platform"}})
	require.NoError(t, err)
	ids := physicalIDs(t, created)
	stackID := created.Outputs["StackId"]

	f.tagQueueDirectly(t, ids["Queue"], "team", "mine")

	_, err = f.deployer.UpdateStack(context.Background(), cfnTaggedStackTemplate, stackName, nil,
		emulator.CFNDeployOptions{Tags: map[string]string{"team": "infra"}})
	require.NoError(t, err)

	assert.Equal(t,
		cfnExpectedTags(stackName, stackID, "Queue", map[string]string{"team": "mine"}),
		f.queueTagsFor(t, ids["Queue"]),
		"the caller's value survives a stack tag of the same name changing")
	assert.Equal(t,
		cfnExpectedTags(stackName, stackID, "Bucket", map[string]string{"team": "infra"}),
		f.bucketTagsFor(t, ids["Bucket"]),
		"and the resource the caller did not touch still tracks the stack")

	// Now the stack drops the key entirely. The caller's tag is still not the stack's to remove.
	_, err = f.deployer.UpdateStack(context.Background(), cfnTaggedStackTemplate, stackName, nil,
		emulator.CFNDeployOptions{Tags: map[string]string{}})
	require.NoError(t, err)

	assert.Equal(t,
		cfnExpectedTags(stackName, stackID, "Queue", map[string]string{"team": "mine"}),
		f.queueTagsFor(t, ids["Queue"]),
		"an empty Tags clears the stack's tags and still leaves the caller's alone")
	assert.Equal(t, cfnExpectedStamp(stackName, stackID, "Bucket"),
		f.bucketTagsFor(t, ids["Bucket"]),
		"while the propagated copy goes, leaving the three aws:cloudformation:* keys")
}

// TestCFN_AStackTagOnAServiceThatModelsNoTagsIsSkippedSilently pins that propagation is subject
// to the same rule as the stamp: a resource whose service keeps no tag state is skipped without
// a log line, because a stack creates far more of those than of the kinds that can carry a tag.
//
// The assertion is on the log, since a silent skip is not observable any other way.
func TestCFN_AStackTagOnAServiceThatModelsNoTagsIsSkippedSilently(t *testing.T) {
	logger := &cfnRecordingLogger{}
	f := newCFNStampFixtureWithLogger(t, nil, logger)

	result, err := f.deployer.DeployWithOptions(context.Background(), `{"Resources": {
		"Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": "/propagate/none"}},
		"Vpc":  {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.13.0.0/16"}}
	}}`, "untaggable-tagged-stack", nil,
		emulator.CFNDeployOptions{Tags: map[string]string{"team": "platform"}})
	require.NoError(t, err)

	ids := physicalIDs(t, result)
	assert.Contains(t, f.tagsFor(t, ids["Vpc"]), "team=platform",
		"the resource that can carry the tag does")
	assert.NotContains(t, logger.joined(), "could not propagate",
		"a service that models no tags is skipped in silence")
}

// cfnFailedLogicalIDs names the resources a deployment reports an error for, so a test can say
// which failed rather than only that one did.
func cfnFailedLogicalIDs(result *emulator.DeployResult) []string {
	var out []string
	for _, r := range result.Resources {
		if r.Error != "" {
			out = append(out, r.LogicalID+": "+r.Error)
		}
	}
	return out
}
