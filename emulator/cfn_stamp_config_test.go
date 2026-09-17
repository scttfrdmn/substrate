package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// AWS Config, #819's last row and the only service whose tags are not on its record at all.
//
// Every other service the stamp reaches keeps a resource's tags *inside* the resource's own
// record, in one of four shapes [cfnRecordTags] knows. Config keeps a **side-car** record keyed by
// ARN whose whole document is the tag map, because neither AWS's `ConfigurationRecorder` shape nor
// its `ConfigRule` shape has a `Tags` member and inventing one would emit a member AWS never emits
// (#836). Three things follow, and they are what the tests below are really about:
//
//   - The record proving the resource exists is a *different key* from the one holding its tags,
//     so the stamp has to check existence against the resource and then create a record that is
//     usually absent — where every other arm merges into one that must already be there.
//   - The reconciliation cannot read the side-car through [cfnRecordTags], which would find
//     neither a `tags` nor a `Tags` member and report the resource as carrying none. That is the
//     KMS defect #819's second half fixed, in the direction that matters: an empty existing set
//     makes a caller's own tag get clobbered and a withdrawn stack tag never get removed.
//   - The ARN, not the physical ID, is the key. A rule's physical ID is its *name* while its ARN
//     names it by a hashed `ConfigRuleId`, and a recorder's ARN carries a minted `RecorderId` that
//     no API member holds — so both are read back from the service at deploy time, and an
//     unreadable one leaves the ARN empty and the stamp skips rather than guessing.
//
// The third type a template can declare, `AWS::Config::DeliveryChannel`, is asserted here as the
// negative case rather than left out: `TagResource`'s `ResourceArn` enumerates the nine resources
// Config can tag and a delivery channel is not one of them, so substrate's channel deploy records no
// ARN, nothing can key a tag under it, and it must deploy clean and be skipped in silence.
//
// As everywhere in #765's line of tests, each tag is read back through **Config's own
// `ListTagsForResource`** rather than out of state — a stamp written under an ARN spelling that
// operation never looks at would satisfy a state assertion and satisfy no caller, which is the
// failure #826, #845 and #943 found three times in three services.

// cfnConfigStampTemplate is one of each of the three AWS Config types the deployer creates.
//
// Four properties are load-bearing and none of them is decoration:
//
//   - `Recorder`'s `RoleARN` is required at the CloudFormation layer (the resource page marks it
//     Required: Yes where the API model does not), so a template without it deploys a failed
//     resource — and a failed resource is skipped by the stamp, which would hide the arm entirely.
//   - `Channel`'s `S3BucketName` is required the same way, and the bucket must really exist and
//     carry a policy admitting Config: `PutDeliveryChannel` reads S3 state. That is what
//     [cfnStampConfigDeliveryBucket] sets up.
//   - `Rule`'s `Source` is the one requirement the resource page and the API model agree on.
//   - `Rule`'s `ConfigRuleName` is pinned so the rule is addressable by name, which is what
//     [cfnStampFixture.configRuleARNFor]'s absence would otherwise force: a generated name is
//     fine for the deploy but says nothing about the ARN, and the ARN is the whole question here.
const cfnConfigStampTemplate = `{
	"Resources": {
		"Recorder": {"Type": "AWS::Config::ConfigurationRecorder", "Properties": {
			"Name": "default",
			"RoleARN": "arn:aws:iam::123456789012:role/config-role"}},
		"Channel": {"Type": "AWS::Config::DeliveryChannel", "Properties": {
			"Name": "default", "S3BucketName": "cfn-stamp-config-logs"}},
		"Rule": {"Type": "AWS::Config::ConfigRule", "Properties": {
			"ConfigRuleName": "cfn-stamp-versioning",
			"Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"}}}
	},
	"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
}`

// TestCFN_StampReachesAWSConfig is #819's last acceptance row: the stamp on a Config resource,
// read back through Config's own tag call.
//
// Both stampable types rather than one, because they resolve through *different* lookups behind one
// arm — a recorder by whole-ARN equality against the account's single recorder, a rule by walking
// the name index for the record holding that hashed ID — so an arm that reached one would leave the
// other silently untagged.
//
// The delivery channel is the third assertion and the reason the template carries it. It has no ARN
// by design, so the stamp must skip it *in silence*: the count of side-car records is what makes
// that observable, since a warning-free log alone cannot distinguish "skipped" from "wrote
// something somewhere else".
func TestCFN_StampReachesAWSConfig(t *testing.T) {
	logger := &cfnRecordingLogger{}
	f := newCFNStampFixtureWithLogger(t, nil, logger)
	const stackName = "config-stamp-stack"
	cfnStampConfigDeliveryBucket(t, f, "cfn-stamp-config-logs")

	result, err := f.deployer.Deploy(context.Background(), cfnConfigStampTemplate, stackName, nil)
	require.NoError(t, err)
	ids := physicalIDs(t, result)
	require.Len(t, ids, 3, "every resource in the template deployed")
	arns := arnsByLogicalID(t, result)

	stackID := result.Outputs["StackId"]
	require.NotEmpty(t, stackID)

	t.Run("configuration recorder", func(t *testing.T) {
		require.Contains(t, arns["Recorder"], ":configuration-recorder/default/",
			"the ARN carries the minted RecorderId, which is what the tag is keyed by")
		assert.Equal(t, cfnExpectedStamp(stackName, stackID, "Recorder"),
			f.configResourceTagsFor(t, arns["Recorder"]))
	})
	t.Run("config rule", func(t *testing.T) {
		require.NotContains(t, arns["Rule"], "cfn-stamp-versioning",
			"a rule's ARN names it by a hashed ConfigRuleId, not by the name in the template")
		assert.Equal(t, cfnExpectedStamp(stackName, stackID, "Rule"),
			f.configResourceTagsFor(t, arns["Rule"]))
	})
	t.Run("delivery channel", func(t *testing.T) {
		assert.Empty(t, arns["Channel"],
			"a delivery channel is not a taggable Config resource, so the deploy records no ARN")
		assert.Len(t, f.configTagRecordKeys(t), 2,
			"two side-cars, one per stampable type — the channel got none")
		assert.NotContains(t, logger.joined(), "could not stamp",
			"a type that cannot carry a tag is skipped in silence, not warned about")
	})
}

// TestCFN_AStackTagReachesAWSConfig is the other half of the arm: the same resolution drives
// #764's stack-tag propagation, so a stack tag reaches both types too.
//
// Worth its own test rather than folded above, because propagation *reads* the existing tags where
// the stamp only writes, and for Config that read cannot go through [cfnRecordTags] at all. A
// reader that looked for a tag member inside the side-car would find none and report the resource
// as untagged — which on a create is invisible, since there is nothing to clobber yet.
func TestCFN_AStackTagReachesAWSConfig(t *testing.T) {
	logger := &cfnRecordingLogger{}
	f := newCFNStampFixtureWithLogger(t, nil, logger)
	const stackName = "config-tagged-stack"
	cfnStampConfigDeliveryBucket(t, f, "cfn-stamp-config-logs")

	result, err := f.deployer.DeployWithOptions(context.Background(), cfnConfigStampTemplate,
		stackName, nil, emulator.CFNDeployOptions{Tags: map[string]string{"team": "platform"}})
	require.NoError(t, err)
	arns := arnsByLogicalID(t, result)
	stackID := result.Outputs["StackId"]
	require.NotEmpty(t, stackID)

	stackTags := map[string]string{"team": "platform"}
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Recorder", stackTags),
		f.configResourceTagsFor(t, arns["Recorder"]))
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Rule", stackTags),
		f.configResourceTagsFor(t, arns["Rule"]))

	assert.NotContains(t, logger.joined(), "could not propagate",
		"both side-cars are reachable, so neither is reported unreadable")
}

// TestCFN_AConfigSideCarIsReadBeforeItIsReconciled is the assertion the side-car reader exists for,
// and like its list-shaped counterpart it needs an *update* to be observable at all.
//
// A create writes the propagated tag whichever way the read went, because a resource whose tags read
// back as empty looks like one carrying none and nothing is at risk. Both failure directions are
// second-deployment cases and both hinge on the existing set having been read:
//
//   - The caller's own tag of the same name is overwritten, because an empty existing set means the
//     comparison against the stack's previous value never happens.
//   - A tag the stack stops carrying is never removed, for the same reason — the removal is
//     conditional on the stored value still matching what the stack propagated.
//
// Config is the one service in #819 where this can be asserted for **every** type it added, which is
// worth saying because ten of the previous twelve could not carry it: an `UpdateStack` re-creates
// every resource in the template, so a type whose create mints a fresh identity is never revisited
// at the record a caller could have tagged. Both Config types re-mint their ARN from the *name*
// on every Put, so the identity survives the update, and both Puts leave the tag side-car alone on
// an update — Config's own words: "Tags are added at creation and cannot be updated with this
// operation".
//
// No delivery channel here, and no bucket: `PutConfigRule` requires only that a recorder *exist*,
// not that it be recording, so the channel would add a precondition without adding an assertion.
func TestCFN_AConfigSideCarIsReadBeforeItIsReconciled(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "config-reconciled-stack"
	tmpl := `{
		"Resources": {
			"Recorder": {"Type": "AWS::Config::ConfigurationRecorder", "Properties": {
				"Name": "default",
				"RoleARN": "arn:aws:iam::123456789012:role/config-role"}},
			"Rule": {"Type": "AWS::Config::ConfigRule", "Properties": {
				"ConfigRuleName": "reconciled-versioning",
				"Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"}}}
		},
		"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
	}`

	created, err := f.deployer.DeployWithOptions(context.Background(), tmpl,
		stackName, nil, emulator.CFNDeployOptions{
			Tags: map[string]string{"team": "platform", "env": "test"},
		})
	require.NoError(t, err)
	arns := arnsByLogicalID(t, created)
	stackID := created.Outputs["StackId"]

	both := map[string]string{"team": "platform", "env": "test"}
	require.Equal(t, cfnExpectedTags(stackName, stackID, "Recorder", both),
		f.configResourceTagsFor(t, arns["Recorder"]))
	require.Equal(t, cfnExpectedTags(stackName, stackID, "Rule", both),
		f.configResourceTagsFor(t, arns["Rule"]))

	// From here `team` is the caller's on both, at a value the stack does not carry.
	f.tagConfigResourceDirectly(t, arns["Recorder"], "team", "mine")
	f.tagConfigResourceDirectly(t, arns["Rule"], "team", "mine")

	// `team` changes on the stack and `env` leaves it.
	updated, err := f.deployer.UpdateStack(context.Background(), tmpl,
		stackName, nil, emulator.CFNDeployOptions{Tags: map[string]string{"team": "infra"}})
	require.NoError(t, err)
	require.Empty(t, cfnFailedLogicalIDs(updated), "the update redeployed both resources")
	updatedARNs := arnsByLogicalID(t, updated)
	require.Equal(t, arns["Recorder"], updatedARNs["Recorder"],
		"the recorder's ARN is minted from its name, so the reconciliation revisits the "+
			"side-car just tagged")
	require.Equal(t, arns["Rule"], updatedARNs["Rule"], "and so is the rule's")

	mine := map[string]string{"team": "mine"}
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Recorder", mine),
		f.configResourceTagsFor(t, arns["Recorder"]),
		"the caller's value survives and env is gone — both need the side-car to be readable")
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Rule", mine),
		f.configResourceTagsFor(t, arns["Rule"]))
}

// TestCFN_AnUnreachableConfigResourceIsSkippedNotFailed covers the two guards each Config arm opens
// with, which are the two a template cannot reach.
//
// Both are contracts rather than dead code, and the contract is the deployer's: a `(false, nil)` is
// rendered as "skipped in silence" while an error is logged as a failure to stamp, so what is asserted
// here is that a resource the bookkeeping cannot reach never fails a stack. Neither shape arises from a
// deploy — a recorder's and a rule's ARN is read back from the service, so a resource that deployed has
// one and resolves — which is exactly why they are asserted directly, following
// [emulator.CFNDecodeRecordTagsForTest]'s reason for existing.
//
// The unresolvable case is the one with a real occasion behind it: a caller can delete a Config rule
// through Config's own `DeleteConfigRule` between two runs against one event stream, and a
// reconciliation that then failed the stack would make a stack undeployable over a resource somebody
// else removed.
func TestCFN_AnUnreachableConfigResourceIsSkippedNotFailed(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	reqCtx := cfnStampReqCtx()
	stamp := []emulator.EC2Tag{{Key: "aws:cloudformation:stack-name", Value: "s"}}
	next := map[string]string{"team": "platform"}

	for _, tc := range []struct {
		name string
		dr   emulator.DeployedResource
	}{
		{
			name: "no ARN was read back",
			dr: emulator.DeployedResource{
				Type: "AWS::Config::ConfigRule", LogicalID: "Rule", PhysicalID: "orders",
			},
		},
		{
			name: "the ARN names no such resource",
			dr: emulator.DeployedResource{
				Type: "AWS::Config::ConfigRule", LogicalID: "Rule", PhysicalID: "orders",
				ARN: "arn:aws:config:" + cfnStampRegion + ":" + cfnStampAccount +
					":config-rule/config-rule-abc123",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stamped, err := emulator.CFNStampConfigResourceForTest(state, reqCtx, tc.dr, stamp)
			require.NoError(t, err, "an unreachable resource must not fail the stack")
			assert.False(t, stamped, "and must report that nothing was stamped")

			reconciled, err := emulator.CFNPropagateConfigStackTagsForTest(
				state, reqCtx, tc.dr, nil, next,
			)
			require.NoError(t, err)
			assert.False(t, reconciled)
		})
	}

	keys, err := state.List(context.Background(), "config", "tags:")
	require.NoError(t, err)
	assert.Empty(t, keys, "neither arm wrote a side-car for a resource it could not reach")
}

// cfnConfigFailingState is a StateManager whose reads or whose writes fail once armed, so the other
// half of each Config arm's early exit is reachable: a resource that could not be *read* rather than
// one that is not there.
type cfnConfigFailingState struct {
	emulator.StateManager
	failGet bool
	failPut bool
	err     error
}

func (m *cfnConfigFailingState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.failGet {
		return nil, m.err
	}
	return m.StateManager.Get(ctx, namespace, key)
}

func (m *cfnConfigFailingState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.failPut && namespace == "config" {
		return m.err
	}
	return m.StateManager.Put(ctx, namespace, key, value)
}

// TestCFN_AConfigStoreFailureIsReportedNotSkipped is the distinction the two guards above exist
// against, and the reason neither of them may report a store problem.
//
// A resource that is not there and a resource that could not be read are two different answers, and
// both arms have to give them differently: an absent one is `(false, nil)`, which the deployer renders
// as a silent skip, while a store failure is `(true, err)`, which it logs as a failure to stamp. Were
// the second collapsed into the first, a Config resource would go untagged and nothing anywhere would
// say so — the tag would simply be missing, on a stack that deployed clean.
//
// Both directions are covered because they fail in different places: the read is the resolution, and
// the write is the side-car save that happens after a resource has been found.
func TestCFN_AConfigStoreFailureIsReportedNotSkipped(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	tmpl := `{
		"Resources": {
			"Recorder": {"Type": "AWS::Config::ConfigurationRecorder", "Properties": {
				"Name": "default",
				"RoleARN": "arn:aws:iam::123456789012:role/config-role"}}
		}
	}`
	result, err := f.deployer.Deploy(context.Background(), tmpl, "config-store-failure-stack", nil)
	require.NoError(t, err)
	arn := arnsByLogicalID(t, result)["Recorder"]
	require.NotEmpty(t, arn)

	dr := emulator.DeployedResource{
		Type: "AWS::Config::ConfigurationRecorder", LogicalID: "Recorder",
		PhysicalID: "default", ARN: arn,
	}
	stamp := []emulator.EC2Tag{{Key: "aws:cloudformation:stack-name", Value: "s"}}
	next := map[string]string{"team": "platform"}
	boom := errors.New("store unavailable")

	for _, tc := range []struct {
		name  string
		state emulator.StateManager
	}{
		{
			name:  "the resource could not be read",
			state: &cfnConfigFailingState{StateManager: f.state, failGet: true, err: boom},
		},
		{
			name:  "the side-car could not be written",
			state: &cfnConfigFailingState{StateManager: f.state, failPut: true, err: boom},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stamped, err := emulator.CFNStampConfigResourceForTest(tc.state, cfnStampReqCtx(), dr, stamp)
			require.ErrorIs(t, err, boom, "a store failure is not an absent resource")
			assert.True(t, stamped, "and is reported as reached, so the caller logs it")

			reconciled, err := emulator.CFNPropagateConfigStackTagsForTest(
				tc.state, cfnStampReqCtx(), dr, nil, next,
			)
			require.ErrorIs(t, err, boom)
			assert.True(t, reconciled)
		})
	}
}

// configResourceTagsFor reads one Config resource's tags through Config's own
// `ListTagsForResource`, which names the resource in a `ResourceArn` body member and answers a
// `Key`/`Value` list — the shape [cfnUpperTagListJSONStrings] already reads.
//
// This is the call that makes the test mean anything: it resolves the ARN through the same
// [cfgsvcFindTaggable] walk the stamp writes through, so a tag landing under any other spelling
// reads back as absent rather than as a difference the assertion could miss.
func (f *cfnStampFixture) configResourceTagsFor(t *testing.T, arn string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"ResourceArn": arn})
	require.NoError(t, err)

	resp, err := f.configservice.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "config",
		Operation: "ListTagsForResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return cfnUpperTagListJSONStrings(t, resp.Body, "ListTagsForResource")
}

// tagConfigResourceDirectly puts one tag on a Config resource through Config's own `TagResource`,
// so a test can hold a tag the *caller* owns rather than one the stack propagated.
//
// Through the service's own operation rather than by writing the side-car, for the reason #765 gives
// about the `putTest*` helpers: a tag written past the service cannot show that the reconciliation
// reads the record the service actually keeps, which is the whole question here.
func (f *cfnStampFixture) tagConfigResourceDirectly(t *testing.T, arn, key, value string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"ResourceArn": arn,
		"Tags":        []map[string]string{{"Key": key, "Value": value}},
	})
	require.NoError(t, err)

	_, err = f.configservice.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "config",
		Operation: "TagResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
	})
	require.NoError(t, err)
}

// configTagRecordKeys returns the keys of every Config tag side-car in state, which is how a
// resource that was *not* stamped is observable at all.
//
// The one state read in this file, and it is about absence rather than about a value: a type the
// stamp skips writes nothing, and "nothing" cannot be read back through an API call. Everything
// else here goes through Config's own operations.
func (f *cfnStampFixture) configTagRecordKeys(t *testing.T) []string {
	t.Helper()
	// "config" is cfgsvcTagsKey's namespace and "tags:" its prefix; both are unexported.
	keys, err := f.state.List(context.Background(), "config", "tags:")
	require.NoError(t, err)
	return keys
}

// cfnStampConfigDeliveryBucket creates the delivery bucket the template's channel needs, with a
// policy that admits Config, through S3's own wire calls.
//
// Outside the template rather than in it because substrate has no `AWS::S3::BucketPolicy` resource
// type: a template can create the bucket but cannot express the policy `PutDeliveryChannel` reads
// real S3 state for, so a stack carrying a channel needs the policy from somewhere else. That gap is
// stated in `cfn_resources_v101.go`; this is its fixture form, and it is the same one
// [cfnConfigDeliveryBucket] uses, written against the stamp fixture's own plugin rather than a
// registry it does not expose.
func cfnStampConfigDeliveryBucket(t *testing.T, f *cfnStampFixture, bucket string) {
	t.Helper()
	_, err := f.s3.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PUT",
		Path:      "/" + bucket,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[{` +
		`"Sid":"AWSConfigBucketDelivery","Effect":"Allow",` +
		`"Principal":{"Service":"config.amazonaws.com"},` +
		`"Action":"s3:PutObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`
	_, err = f.s3.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PUT",
		Path:      "/" + bucket,
		Body:      []byte(policy),
		Params:    map[string]string{"policy": "1"},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
}
