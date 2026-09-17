package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The twelve resource types #819 could not stamp until #835 (#819, second half).
//
// #765 widened the `aws:cloudformation:*` stamp past EC2 and #819's first half added nine more
// types. Twelve were held out, and by one condition rather than by a judgement about which
// service matters: [mergeResourceTags] had no arm reaching their namespace and key prefix, so
// there was nowhere for a stamp to land that the owning service would ever read. #835 gave all
// seven of those namespaces an arm, and this file is the twelve arriving.
//
// Every assertion reads the tag back through the **owning service's own tag call**, which is
// #765's criterion and the whole point: a stamp written to a key the service does not read
// satisfies a state assertion and satisfies no caller. That is the defect #826 found for SQS,
// #845 for ECS and #943 for Lambda and DynamoDB, three times in three services, so the readers
// here are deliberately the calls a caller would make and nothing cheaper.
//
// Seven of the twelve needed a bespoke resolver arm rather than a line in
// [cfnRegionalStampKinds], and each for a structural reason worth naming here because it is what
// the corresponding subtest is really testing:
//
//   - A KMS key's, a secret's and a topic's physical ID is an *ARN* where the record's key holds a
//     bare ID or name.
//   - A CloudFront distribution's key carries no Region, CloudFront being global.
//   - An SSM parameter's key carries the leading "/" `PutParameter` adds, which the deployer's
//     physical ID does not — so the un-slashed name in the template below is the case that fails
//     without the normalization.
//   - An ECS service's and task definition's key has *four* segments, one of which (the cluster,
//     and the revision) appears only in the ARN.

// cfnGroup3bTemplate is one resource of each of the twelve types #819's second half added.
//
// The names are pinned rather than left to fall back to the logical ID, because for four of the
// twelve the name *is* the identifier the record is keyed by, and a template that did not say so
// would still pass while proving less. Three properties are load-bearing beyond that:
//
//   - `Param`'s name carries **no** leading slash, which is the shape `PutParameter` normalizes
//     and the deployer's physical ID does not — the case the resolver's own normalization exists
//     for.
//   - `Svc`'s `Cluster` names a cluster other than `default`, because the cluster segment is
//     precisely what an ECS service's physical ID lacks and its ARN carries.
//   - `Replica` deploys through the same function as `Key` and only relabels its type, so the two
//     are separate records under one arm; asserting both is what shows the arm covers the label
//     it is not named after.
const cfnGroup3bTemplate = `{
	"Resources": {
		"Activity": {"Type": "AWS::StepFunctions::Activity", "Properties": {
			"Name": "group3b-activity"}},
		"Cert": {"Type": "AWS::CertificateManager::Certificate", "Properties": {
			"DomainName": "group3b.example.com"}},
		"DbCluster": {"Type": "AWS::RDS::DBCluster", "Properties": {
			"DBClusterIdentifier": "group3b-dbcluster", "Engine": "aurora-mysql",
			"MasterUsername": "admin"}},
		"SubnetGroup": {"Type": "AWS::RDS::DBSubnetGroup", "Properties": {
			"DBSubnetGroupName": "group3b-subnets",
			"DBSubnetGroupDescription": "group 3b"}},
		"Dist": {"Type": "AWS::CloudFront::Distribution", "Properties": {
			"DistributionConfig": {"Comment": "group 3b"}}},
		"Param": {"Type": "AWS::SSM::Parameter", "Properties": {
			"Name": "group3b-param", "Type": "String", "Value": "v"}},
		"Key": {"Type": "AWS::KMS::Key", "Properties": {"Description": "group 3b key"}},
		"Replica": {"Type": "AWS::KMS::ReplicaKey", "Properties": {
			"Description": "group 3b replica"}},
		"Secret": {"Type": "AWS::SecretsManager::Secret", "Properties": {
			"Name": "group3b-secret", "SecretString": "s"}},
		"Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "group3b-topic"}},
		"TaskDef": {"Type": "AWS::ECS::TaskDefinition", "Properties": {
			"Family": "group3b-family"}},
		"Svc": {"Type": "AWS::ECS::Service", "Properties": {
			"ServiceName": "group3b-svc", "Cluster": "group3b-ecs",
			"TaskDefinition": {"Ref": "TaskDef"}}}
	},
	"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
}`

// TestCFN_StampReachesGroup3B is #819's acceptance criterion for the twelve types #835 unblocked:
// each read back through that service's own tag-reading operation.
//
// One subtest per type rather than one for the stack, for the reason #746's, #765's and the first
// half's equivalents give: the stamp is written from a single resolver behind a type switch, so a
// type the resolver does not claim is skipped in *silence* and only a per-type assertion catches
// it. The pairs matter more than the singletons here — a KMS key and its replica, an ECS service
// and task definition, an RDS cluster and subnet group all share one arm or one table, so an arm
// that named the wrong prefix for one of a pair would read back empty rather than fail loudly.
func TestCFN_StampReachesGroup3B(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "group3b-stack"

	result, err := f.deployer.Deploy(context.Background(), cfnGroup3bTemplate, stackName, nil)
	require.NoError(t, err)
	ids := physicalIDs(t, result)
	require.Len(t, ids, 12, "every resource in the template deployed")

	stackID := result.Outputs["StackId"]
	require.NotEmpty(t, stackID)
	want := func(logicalID string) []string {
		return cfnExpectedStamp(stackName, stackID, logicalID)
	}
	arns := arnsByLogicalID(t, result)

	t.Run("Step Functions activity", func(t *testing.T) {
		assert.Equal(t, want("Activity"), f.activityTagsFor(t, ids["Activity"]))
	})
	t.Run("ACM certificate", func(t *testing.T) {
		// The physical ID is the certificate ARN and [acmCertKey]'s last segment is the whole ARN
		// by design, so this is the one type whose ARN-shaped physical ID needs no parsing.
		assert.Equal(t, want("Cert"), f.certificateTagsFor(t, ids["Cert"]))
	})
	t.Run("RDS DB cluster", func(t *testing.T) {
		assert.Equal(t, want("DbCluster"), f.dbClusterTagsFor(t, ids["DbCluster"]))
	})
	t.Run("RDS DB subnet group", func(t *testing.T) {
		assert.Equal(t, want("SubnetGroup"), f.dbSubnetGroupTagsFor(t, ids["SubnetGroup"]))
	})
	t.Run("CloudFront distribution", func(t *testing.T) {
		assert.Equal(t, want("Dist"), f.distributionTagsFor(t, ids["Dist"]))
	})
	t.Run("SSM parameter", func(t *testing.T) {
		// The template's name carries no leading slash. Without the resolver's normalization the
		// stamp lands at `parameter:…/group3b-param` while this call loads
		// `parameter:…//group3b-param`, so the failure is an empty tag set rather than an error.
		assert.Equal(t, want("Param"), f.parameterTagsFor(t, ids["Param"]))
	})
	t.Run("KMS key", func(t *testing.T) {
		assert.Equal(t, want("Key"), f.kmsKeyTagsFor(t, ids["Key"]))
	})
	t.Run("KMS replica key", func(t *testing.T) {
		assert.Equal(t, want("Replica"), f.kmsKeyTagsFor(t, ids["Replica"]))
	})
	t.Run("Secrets Manager secret", func(t *testing.T) {
		assert.Equal(t, want("Secret"), f.secretTagsFor(t, ids["Secret"]))
	})
	t.Run("SNS topic", func(t *testing.T) {
		assert.Equal(t, want("Topic"), f.topicTagsFor(t, ids["Topic"]))
	})
	t.Run("ECS task definition", func(t *testing.T) {
		// The revision is in the ARN and nowhere else, and here the physical ID *is* the ARN —
		// so the resolver's two attempts have to agree, which reading through ECS's own
		// `ecsTagStateKey` is what guarantees.
		assert.Equal(t, want("TaskDef"), f.ecsTaskDefTagsFor(t, ids["TaskDef"]))
	})
	t.Run("ECS service", func(t *testing.T) {
		// The cluster is in the ARN and not in the physical ID, which is `group3b-svc` alone.
		require.NotEmpty(t, arns["Svc"], "the service recorded its ARN")
		assert.Equal(t, want("Svc"), f.ecsServiceTagsFor(t, arns["Svc"]))
	})
}

// TestCFN_AStackTagReachesGroup3B is the other half of what widening the resolver does: the same
// resolver drives #764's stack-tag propagation, so a stack tag reaches the twelve as well.
//
// Worth its own test rather than folded into the stamp's, because propagation reads the resource's
// *existing* tags where the stamp only writes — through [cfnRecordTags], which has to recognize
// the shape the record keeps them in. Four of the seven namespaces #835 added keep a tag *list*
// rather than a map, and one of those four (KMS) spells its members `TagKey`/`TagValue` where
// every other service in the tree spells them `Key`/`Value`.
func TestCFN_AStackTagReachesGroup3B(t *testing.T) {
	logger := &cfnRecordingLogger{}
	f := newCFNStampFixtureWithLogger(t, nil, logger)
	const stackName = "group3b-tagged-stack"

	result, err := f.deployer.DeployWithOptions(context.Background(), cfnGroup3bTemplate,
		stackName, nil, emulator.CFNDeployOptions{Tags: map[string]string{"team": "platform"}})
	require.NoError(t, err)
	ids := physicalIDs(t, result)
	arns := arnsByLogicalID(t, result)
	stackID := result.Outputs["StackId"]
	require.NotEmpty(t, stackID)

	want := func(logicalID string) []string {
		return cfnExpectedTags(stackName, stackID, logicalID,
			map[string]string{"team": "platform"})
	}

	// One per tag-record shape rather than one per type: a map member (`acm`, `cloudfront`,
	// `rds`, `states`), a `Key`/`Value` list (`sns`, `secretsmanager`, `ssm`), a lowercase
	// `key`/`value` list (`ecs`) and KMS's `TagKey`/`TagValue` list.
	assert.Equal(t, want("Cert"), f.certificateTagsFor(t, ids["Cert"]))
	assert.Equal(t, want("Dist"), f.distributionTagsFor(t, ids["Dist"]))
	assert.Equal(t, want("DbCluster"), f.dbClusterTagsFor(t, ids["DbCluster"]))
	assert.Equal(t, want("Activity"), f.activityTagsFor(t, ids["Activity"]))
	assert.Equal(t, want("Topic"), f.topicTagsFor(t, ids["Topic"]))
	assert.Equal(t, want("Secret"), f.secretTagsFor(t, ids["Secret"]))
	assert.Equal(t, want("Param"), f.parameterTagsFor(t, ids["Param"]))
	assert.Equal(t, want("Svc"), f.ecsServiceTagsFor(t, arns["Svc"]))
	assert.Equal(t, want("Key"), f.kmsKeyTagsFor(t, ids["Key"]))

	assert.NotContains(t, logger.joined(), "could not propagate",
		"every one of the twelve records is reachable, so none is reported unreadable")
}

// TestCFN_AListShapedRecordIsReadBeforeItIsReconciled is the assertion the list decode exists for,
// and it needs an *update* rather than a create to be observable at all.
//
// A create writes the propagated tag whatever the reader saw, because a record whose tags read
// back as `nil` looks like a resource carrying none and nothing is at risk of being clobbered. The
// two directions that go wrong are both second-deployment cases, and both hinge on the *existing*
// set having been read:
//
//   - The caller's own tag of the same name is overwritten, because a `nil` existing set means the
//     comparison against the stack's previous value never happens.
//   - A tag the stack stops carrying is never removed, for the same reason — the removal is
//     conditional on the stored value still matching what the stack propagated.
//
// So each resource is tagged directly through its *own* service's `TagResource` between the two
// deployments, and both directions are asserted in one pass. Against a reader that cannot see a tag
// *list* the first assertion fails on the clobber and the second on the tag that will not leave.
//
// A topic and a parameter rather than a KMS key, which is the record whose shape actually broke.
// Three of the twelve cannot carry this assertion at all, for a reason worth stating because it is
// not about tags: an `UpdateStack` re-creates every resource in the template, so a type whose create
// mints a fresh identity (a KMS key, a certificate, a distribution) is never revisited at the record
// a caller could have tagged, and a type whose create *rebuilds* its record (an ECS service, whose
// ARN does survive) has the caller's tag wiped before the reconciliation runs. A topic and a
// parameter are the two list-shaped records among the twelve whose create is idempotent, so they are
// the whole set this can be asserted through. KMS's own `TagKey`/`TagValue` spelling — the one shape
// no wire path reaches — is covered by [TestCFNDecodeRecordTags_ReadsEveryShapeARecordKeepsTagsIn].
func TestCFN_AListShapedRecordIsReadBeforeItIsReconciled(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "group3b-reconciled-stack"
	tmpl := `{
		"Resources": {
			"Topic": {"Type": "AWS::SNS::Topic", "Properties": {
				"TopicName": "reconciled-topic"}},
			"Param": {"Type": "AWS::SSM::Parameter", "Properties": {
				"Name": "reconciled-param", "Type": "String", "Value": "v"}}
		},
		"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
	}`

	created, err := f.deployer.DeployWithOptions(context.Background(), tmpl,
		stackName, nil, emulator.CFNDeployOptions{
			Tags: map[string]string{"team": "platform", "env": "test"},
		})
	require.NoError(t, err)
	ids := physicalIDs(t, created)
	stackID := created.Outputs["StackId"]

	both := map[string]string{"team": "platform", "env": "test"}
	require.Equal(t, cfnExpectedTags(stackName, stackID, "Topic", both),
		f.topicTagsFor(t, ids["Topic"]))
	require.Equal(t, cfnExpectedTags(stackName, stackID, "Param", both),
		f.parameterTagsFor(t, ids["Param"]))

	// From here `team` is the caller's on both, at a value the stack does not carry.
	f.tagTopicDirectly(t, ids["Topic"], "team", "mine")
	f.tagParameterDirectly(t, ids["Param"], "team", "mine")

	// `team` changes on the stack and `env` leaves it.
	updated, err := f.deployer.UpdateStack(context.Background(), tmpl,
		stackName, nil, emulator.CFNDeployOptions{Tags: map[string]string{"team": "infra"}})
	require.NoError(t, err)
	require.Empty(t, cfnFailedLogicalIDs(updated), "the update redeployed both resources")
	require.Equal(t, ids["Topic"], physicalIDs(t, updated)["Topic"],
		"the topic kept its ARN, so the reconciliation revisits the record just tagged")

	mine := map[string]string{"team": "mine"}
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Topic", mine),
		f.topicTagsFor(t, ids["Topic"]),
		"the caller's value survives and env is gone — both need the stored list to be readable")
	assert.Equal(t, cfnExpectedTags(stackName, stackID, "Param", mine),
		f.parameterTagsFor(t, ids["Param"]))
}

// TestCFNDecodeRecordTags_ReadsEveryShapeARecordKeepsTagsIn covers the four shapes substrate's
// records keep tags in, one of which no wire path reaches.
//
// KMS is the reason this test is here rather than folded into a deployment: it is the only service
// in the tree spelling its tag members `TagKey`/`TagValue` — AWS's own naming for the KMS `Tag`
// shape, not substrate's invention — and its record is unreachable from the reconciliation for the
// reason [TestCFN_AListShapedRecordIsReadBeforeItIsReconciled] gives. A missed spelling here is not
// a missing read but a wrong decision: `nil` is indistinguishable from an untagged resource, so a
// caller's tag is clobbered and a removed stack tag is never removed.
//
// The unreadable case is asserted too, because it is a decision rather than an oversight: a shape
// none of the decodes reaches reads as untagged, so a record substrate cannot parse cannot fail a
// stack update.
func TestCFNDecodeRecordTags_ReadsEveryShapeARecordKeepsTagsIn(t *testing.T) {
	want := map[string]string{"team": "platform", "env": "test"}

	for name, member := range map[string]string{
		"a map, as most records keep them":       `{"team": "platform", "env": "test"}`,
		"a Key/Value list, as EFS keeps them":    `[{"Key":"team","Value":"platform"},{"Key":"env","Value":"test"}]`,
		"a key/value list, as ECS keeps them":    `[{"key":"team","value":"platform"},{"key":"env","value":"test"}]`,
		"a TagKey/TagValue list, KMS's spelling": `[{"TagKey":"team","TagValue":"platform"},{"TagKey":"env","TagValue":"test"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, want, emulator.CFNDecodeRecordTagsForTest([]byte(member)))
		})
	}

	for name, member := range map[string]string{
		"an absent member":            ``,
		"a JSON null":                 `null`,
		"an empty list":               `[]`,
		"a shape no decode reaches":   `[["team","platform"]]`,
		"a list of unnamed tag pairs": `[{"Name":"team","Val":"platform"}]`,
	} {
		t.Run(name+" reads as untagged", func(t *testing.T) {
			assert.Nil(t, emulator.CFNDecodeRecordTagsForTest([]byte(member)))
		})
	}
}

// The eleven readers below are #819's second half, each going through that service's own
// tag-reading call for the reason the file header gives.
//
// Eleven functions rather than one table because the calls genuinely differ, and the differences
// are the point: KMS names the key in a `KeyId` body member and answers a `TagKey`/`TagValue`
// list; Secrets Manager publishes no `ListTagsForResource` at all (#929) so its tags are read
// through `DescribeSecret`; ACM takes a `CertificateArn`; CloudFront is a GET with the ARN in a
// `Resource` *query parameter*; SNS is query-protocol XML; SSM takes a resource *type* and a bare
// parameter name and refuses an ARN. Three of the eleven delegate to a `*TagsForARN` helper shared
// with a reader in `cfn_stamp_beyond_ec2_test.go`, because Step Functions, ECS and RDS each answer
// for more than one kind through one operation that decides from the ARN.

// sfnTagsForARN reads whatever a Step Functions ARN names through ListTagsForResource.
func (f *cfnStampFixture) sfnTagsForARN(t *testing.T, arn string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"resourceArn": arn})
	require.NoError(t, err)

	resp, err := f.states.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "states",
		Operation: "ListTagsForResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{"x-amz-target": "AWSStepFunctions.ListTagsForResource"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Step Functions renders tags as AWS's array of {key, value} objects, unlike ECR's and
	// Glue's object form in the same tests — the shape difference #910 corrected, since this
	// operation previously answered an object no SDK could decode into its tags field.
	return cfnLowerTagListJSONStrings(t, resp.Body)
}

// activityTagsFor reads one Step Functions activity's tags through ListTagsForResource.
//
// The keyword is `activity` where a state machine's is `stateMachine`, and that single letter is
// the whole difference between the two records — which is why the activity's own entry in
// [cfnRegionalStampKinds] had to be checked against `sfnKeyIsTaggable` rather than assumed from
// the state machine's.
func (f *cfnStampFixture) activityTagsFor(t *testing.T, name string) []string {
	t.Helper()
	return f.sfnTagsForARN(t,
		"arn:aws:states:"+cfnStampRegion+":"+cfnStampAccount+":activity:"+name)
}

// ecsTagsForARN reads whatever an ECS ARN names through ECS's ListTagsForResource.
func (f *cfnStampFixture) ecsTagsForARN(t *testing.T, arn string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"resourceArn": arn})
	require.NoError(t, err)

	resp, err := f.ecs.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "ecs",
		Operation: "ListTagsForResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return cfnLowerTagListJSONStrings(t, resp.Body)
}

// ecsServiceTagsFor reads one ECS service's tags, given the ARN the deployer recorded.
//
// The ARN rather than the physical ID, and not for convenience: an ECS service's key is
// `service:<account>/<region>/<cluster>/<name>` and the physical ID is the name alone, so the
// cluster segment exists only in the ARN. AWS refuses a short service ARN here too — "if you use
// a short ARN to tag a service, you receive an InvalidParameterException" — so there is no shape
// in which the name alone would have done.
func (f *cfnStampFixture) ecsServiceTagsFor(t *testing.T, arn string) []string {
	t.Helper()
	return f.ecsTagsForARN(t, arn)
}

// ecsTaskDefTagsFor reads one task definition's tags, given the ARN its physical ID is.
//
// `taskdef:<account>/<region>/<family>/<revision>` is a four-segment key whose revision the
// template never states, so the revision the deploy response reported is the only source for it.
func (f *cfnStampFixture) ecsTaskDefTagsFor(t *testing.T, arn string) []string {
	t.Helper()
	return f.ecsTagsForARN(t, arn)
}

// cfnLowerTagListJSONStrings reads the lowercase `{"tags": [{"key", "value"}]}` list that Step
// Functions and ECS both answer with, which is the one response shape two of the eleven share.
func cfnLowerTagListJSONStrings(t *testing.T, body []byte) []string {
	t.Helper()
	var doc struct {
		Tags []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"tags"`
	}
	require.NoError(t, json.Unmarshal(body, &doc), "ListTagsForResource body: %s", body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// rdsTagsForARN reads whatever an RDS ARN names through RDS's ListTagsForResource.
func (f *cfnStampFixture) rdsTagsForARN(t *testing.T, arn string) []string {
	t.Helper()
	resp, err := f.rds.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "rds",
		Operation: "ListTagsForResource",
		Params: map[string]string{
			"Action":       "ListTagsForResource",
			"ResourceName": arn,
		},
		Headers: map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return cfnTagListXMLStrings(t, resp.Body)
}

// dbClusterTagsFor reads one Aurora cluster's tags, whose ARN keyword is `cluster` — distinct
// from `cluster-pg` and `cluster-snapshot`, which are their own resource types and their own
// records.
func (f *cfnStampFixture) dbClusterTagsFor(t *testing.T, id string) []string {
	t.Helper()
	return f.rdsTagsForARN(t,
		"arn:aws:rds:"+cfnStampRegion+":"+cfnStampAccount+":cluster:"+id)
}

// dbSubnetGroupTagsFor reads one DB subnet group's tags, whose ARN keyword is `subgrp` — the one
// RDS keyword that is not a prefix of the resource name it introduces.
func (f *cfnStampFixture) dbSubnetGroupTagsFor(t *testing.T, name string) []string {
	t.Helper()
	return f.rdsTagsForARN(t,
		"arn:aws:rds:"+cfnStampRegion+":"+cfnStampAccount+":subgrp:"+name)
}

// certificateTagsFor reads one certificate's tags through ACM's ListTagsForCertificate, which is
// what ACM calls its tag-reading operation — it publishes no `ListTagsForResource`.
func (f *cfnStampFixture) certificateTagsFor(t *testing.T, certARN string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"CertificateArn": certARN})
	require.NoError(t, err)

	resp, err := f.acm.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "acm",
		Operation: "ListTagsForCertificate",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return cfnUpperTagListJSONStrings(t, resp.Body, "ListTagsForCertificate")
}

// parameterTagsFor reads one SSM parameter's tags through ListTagsForResource, which names the
// parameter by a resource *type* and a bare name rather than by an ARN — an ARN in `ResourceId`
// is refused outright.
//
// The name is passed exactly as the template wrote it, unslashed. `ssmResolveTagTarget` adds the
// leading "/" the same way `PutParameter` does, so this reader lands on the record the stamp had
// to have written and would report an empty tag set if it had not.
func (f *cfnStampFixture) parameterTagsFor(t *testing.T, name string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"ResourceType": "Parameter",
		"ResourceId":   name,
	})
	require.NoError(t, err)

	resp, err := f.ssm.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "ssm",
		Operation: "ListTagsForResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		TagList []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"TagList"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &doc), "ListTagsForResource body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.TagList))
	for _, tag := range doc.TagList {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// kmsKeyTagsFor reads one KMS key's tags through ListResourceTags, which takes the key's ARN or
// bare ID in a `KeyId` member and answers a list of `TagKey`/`TagValue` pairs.
//
// Those two member names are AWS's own for the KMS `Tag` shape, not substrate's invention, and
// KMS is the only service in the tree that uses them — which is what made the propagation path's
// reader miss this record's tags entirely until #819's second half.
func (f *cfnStampFixture) kmsKeyTagsFor(t *testing.T, keyID string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"KeyId": keyID})
	require.NoError(t, err)

	resp, err := f.kms.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "kms",
		Operation: "ListResourceTags",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `json:"TagKey"`
			Value string `json:"TagValue"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &doc), "ListResourceTags body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// tagTopicDirectly puts one tag on a topic through SNS's own TagResource, so a test can hold a tag
// the *caller* owns rather than one the stack propagated.
//
// Through the service's own operation rather than by writing state, for the reason #765 gives about
// the `putTest*` helpers: a tag written past the service cannot show that the reconciliation reads
// the record the service actually keeps, which is the whole question here.
func (f *cfnStampFixture) tagTopicDirectly(t *testing.T, topicARN, key, value string) {
	t.Helper()
	_, err := f.sns.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "sns",
		Operation: "TagResource",
		Params: map[string]string{
			"Action":              "TagResource",
			"ResourceArn":         topicARN,
			"Tags.member.1.Key":   key,
			"Tags.member.1.Value": value,
		},
		Headers: map[string]string{},
	})
	require.NoError(t, err)
}

// tagParameterDirectly puts one tag on a parameter through SSM's AddTagsToResource, which is what
// SSM calls its tagging operation — there is no `TagResource`.
func (f *cfnStampFixture) tagParameterDirectly(t *testing.T, name, key, value string) {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"ResourceType": "Parameter",
		"ResourceId":   name,
		"Tags":         []map[string]string{{"Key": key, "Value": value}},
	})
	require.NoError(t, err)

	_, err = f.ssm.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "ssm",
		Operation: "AddTagsToResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
}

// secretTagsFor reads one secret's tags through DescribeSecret, which is the operation a caller
// reads them back through: Secrets Manager publishes no `ListTagsForResource` at all (#929), so
// there is no cheaper call to make and no second one to disagree with.
func (f *cfnStampFixture) secretTagsFor(t *testing.T, secretID string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"SecretId": secretID})
	require.NoError(t, err)

	resp, err := f.secretsmanager.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "secretsmanager",
		Operation: "DescribeSecret",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return cfnUpperTagListJSONStrings(t, resp.Body, "DescribeSecret")
}

// cfnUpperTagListJSONStrings reads a `"Tags": [{"Key", "Value"}]` member, the shape ACM's and
// Secrets Manager's readers share.
//
// An absent member reads as no tags rather than as a decode failure, because that is what both
// operations answer for an untagged resource — Secrets Manager deliberately omits `Tags` there
// rather than sending the `null` AWS does not send (#928).
func cfnUpperTagListJSONStrings(t *testing.T, body []byte, operation string) []string {
	t.Helper()
	var doc struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(body, &doc), "%s body: %s", operation, body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// topicTagsFor reads one topic's tags through SNS's ListTagsForResource, which names the topic in
// a `ResourceArn` query parameter and answers query-protocol XML.
func (f *cfnStampFixture) topicTagsFor(t *testing.T, topicARN string) []string {
	t.Helper()
	resp, err := f.sns.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "sns",
		Operation: "ListTagsForResource",
		Params: map[string]string{
			"Action":      "ListTagsForResource",
			"ResourceArn": topicARN,
		},
		Headers: map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListTagsForResourceResult>Tags>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "ListTagsForResource body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// distributionTagsFor reads one distribution's tags through CloudFront's ListTagsForResource,
// which is a `GET /2020-05-31/tagging?Resource={arn}` — the ARN is a query parameter rather than
// a body member or a path segment, and the method is what tells the three tagging operations
// apart.
//
// The ARN carries an empty Region field, CloudFront being global, which is the reason this type
// could not be a line in [cfnRegionalStampKinds]: its record's key has no Region segment either.
func (f *cfnStampFixture) distributionTagsFor(t *testing.T, distID string) []string {
	t.Helper()
	arn := "arn:aws:cloudfront::" + cfnStampAccount + ":distribution/" + distID
	resp, err := f.cloudfront.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:    "cloudfront",
		Operation:  "ListTagsForResource",
		HTTPMethod: http.MethodGet,
		Path:       "/2020-05-31/tagging",
		Params:     map[string]string{"Resource": arn},
		Headers:    map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"Items>Tag"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "ListTagsForResource body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}
