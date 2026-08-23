package emulator_test

import (
	"context"
	"encoding/xml"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFormation's own tag stamp on the resources a stack creates (#746).
//
// AWS puts three tags on every resource a stack creates — aws:cloudformation:stack-name,
// aws:cloudformation:stack-id and aws:cloudformation:logical-id — and substrate's deployer
// put none, which left AmazonECS_FullAccess's ManagedCloudformationResourcesCleanupPolicy
// statement permanently unsatisfiable: it conditions on
// ec2:ResourceTag/aws:cloudformation:stack-name, the resolution for that key works (#730),
// and there was no path by which the tag could ever exist. CreateTags refuses an aws: key as
// AWS does, so a caller could not fabricate it either.
//
// The last test here is the one that matters most, because it asserts the whole chain rather
// than the tag: deploy a stack, then have the bundled policy actually authorize a delete.

const (
	cfnStampAccount = "123456789012"
	cfnStampRegion  = "us-east-1"

	cfnStampStackNameTag = "aws:cloudformation:stack-name"
	cfnStampStackIDTag   = "aws:cloudformation:stack-id"
	cfnStampLogicalIDTag = "aws:cloudformation:logical-id"
)

// cfnStampFixture is a deployer and an EC2 plugin over one state, which is what makes the
// stamp observable the way a caller observes it: the deployer writes it and DescribeTags
// reads it back.
type cfnStampFixture struct {
	deployer *emulator.StackDeployer
	ec2      *emulator.EC2Plugin
	state    emulator.StateManager
}

// newCFNStampFixture builds the fixture over the given state, so a test that also needs an
// AuthController can share one store with it. A nil state gets a fresh one.
func newCFNStampFixture(t *testing.T, state emulator.StateManager) *cfnStampFixture {
	t.Helper()
	if state == nil {
		state = emulator.NewMemoryStateManager()
	}
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	ec2Plugin := &emulator.EC2Plugin{}
	require.NoError(t, ec2Plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(ec2Plugin)

	// S3 too, so "a physical ID that is not an EC2 ID is skipped" is asserted against a real
	// bucket rather than a fabricated one.
	s3Plugin := &emulator.S3Plugin{}
	require.NoError(t, s3Plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc, "filesystem": afero.NewMemMapFs()},
	}))
	registry.Register(s3Plugin)

	return &cfnStampFixture{
		deployer: emulator.NewStackDeployer(registry, store, state, tc, logger, costs),
		ec2:      ec2Plugin,
		state:    state,
	}
}

// tagsFor returns the tags DescribeTags reports for one resource, as "key=value" strings
// sorted so the assertion does not depend on an element order AWS says may vary.
func (f *cfnStampFixture) tagsFor(t *testing.T, resourceID string) []string {
	t.Helper()
	resp, err := f.ec2.HandleRequest(&emulator.RequestContext{
		AccountID: cfnStampAccount,
		Region:    cfnStampRegion,
		Metadata:  map[string]interface{}{},
	}, &emulator.AWSRequest{
		Service:   "ec2",
		Operation: "DescribeTags",
		Params: map[string]string{
			"Action":           "DescribeTags",
			"Filter.1.Name":    "resource-id",
			"Filter.1.Value.1": resourceID,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"tagSet>item"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "DescribeTags body: %s", resp.Body)

	out := make([]string, 0, len(doc.Tags))
	for _, tag := range doc.Tags {
		out = append(out, tag.Key+"="+tag.Value)
	}
	sort.Strings(out)
	return out
}

// physicalIDs maps each deployed resource's logical ID to its physical ID.
func physicalIDs(t *testing.T, result *emulator.DeployResult) map[string]string {
	t.Helper()
	out := make(map[string]string, len(result.Resources))
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "%s failed to deploy", r.LogicalID)
		out[r.LogicalID] = r.PhysicalID
	}
	return out
}

// cfnStampTemplate is a stack covering every EC2 resource type the deployer creates whose
// physical ID is an EC2 ID: all nine, so no arm of the resolver is left unproven.
var cfnStampTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"Vpc":     {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.7.0.0/16"}},
		"Subnet":  {"Type": "AWS::EC2::Subnet", "Properties": {
			"VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.7.1.0/24"}},
		"Sg":      {"Type": "AWS::EC2::SecurityGroup", "Properties": {
			"GroupDescription": "stamped", "VpcId": {"Ref": "Vpc"}}},
		"Igw":     {"Type": "AWS::EC2::InternetGateway", "Properties": {}},
		"Rtb":     {"Type": "AWS::EC2::RouteTable", "Properties": {"VpcId": {"Ref": "Vpc"}}},
		"Eip":     {"Type": "AWS::EC2::EIP", "Properties": {"Domain": "vpc"}},
		"Nat":     {"Type": "AWS::EC2::NatGateway", "Properties": {
			"SubnetId": {"Ref": "Subnet"}, "AllocationId": {"Ref": "Eip"}}},
		"Lt":      {"Type": "AWS::EC2::LaunchTemplate", "Properties": {
			"LaunchTemplateName": "stamped-lt",
			"LaunchTemplateData": {"InstanceType": "t3.micro"}}},
		"Instance": {"Type": "AWS::EC2::Instance", "Properties": {
			"ImageId": "` + ec2TestImage + `", "InstanceType": "t3.micro",
			"SubnetId": {"Ref": "Subnet"}}}
	},
	"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
}`

// TestCFN_StampsItsOwnTagsOnEveryEC2ResourceItCreates is the issue's own scenario, across
// every EC2 type the deployer creates.
//
// One assertion per resource rather than one for the stack, because the stamp is written from
// a single place keyed on the physical ID's prefix: a type whose ID the resolver does not
// recognize would be silently skipped, and only a per-type assertion catches that.
func TestCFN_StampsItsOwnTagsOnEveryEC2ResourceItCreates(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "stamped-stack"

	result, err := f.deployer.Deploy(context.Background(), cfnStampTemplate, stackName, nil)
	require.NoError(t, err)
	ids := physicalIDs(t, result)
	require.Len(t, ids, 9, "every resource in the template deployed")

	for logicalID, physicalID := range ids {
		t.Run(logicalID, func(t *testing.T) {
			require.NotEmpty(t, physicalID)
			tags := f.tagsFor(t, physicalID)
			assert.Contains(t, tags, cfnStampStackNameTag+"="+stackName)
			assert.Contains(t, tags, cfnStampLogicalIDTag+"="+logicalID)

			var stackID string
			for _, tag := range tags {
				if strings.HasPrefix(tag, cfnStampStackIDTag+"=") {
					stackID = strings.TrimPrefix(tag, cfnStampStackIDTag+"=")
				}
			}
			assert.Equal(t, result.Outputs["StackId"], stackID,
				"the stamped stack-id is the stack ARN AWS::StackId resolves to")
		})
	}
}

// TestCFN_StampedStackIDIsTheStackTheAPIReports pins that the tag names the same stack the
// rest of the deployment does. Two derivations of one identity is the defect #517 was, so the
// tag has to agree with AWS::StackId rather than be built again beside it.
func TestCFN_StampedStackIDIsTheStackTheAPIReports(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "identity-stack"
	tmpl := `{
		"Resources": {"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.8.0.0/16"}}},
		"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
	}`

	result, err := f.deployer.Deploy(context.Background(), tmpl, stackName, nil)
	require.NoError(t, err)
	stackID := result.Outputs["StackId"]
	require.True(t, strings.HasPrefix(stackID,
		"arn:aws:cloudformation:"+cfnStampRegion+":"+cfnStampAccount+":stack/"+stackName+"/"),
		"AWS::StackId is the stack ARN, got %q", stackID)

	assert.Equal(t, []string{
		cfnStampLogicalIDTag + "=Vpc",
		cfnStampStackIDTag + "=" + stackID,
		cfnStampStackNameTag + "=" + stackName,
	}, f.tagsFor(t, physicalIDs(t, result)["Vpc"]),
		"exactly the three tags, and nothing else")
}

// TestCFN_StampIsIdempotent pins that re-deploying a stack does not accumulate tags.
//
// The stamp is an upsert through the same writer CreateTags uses, so a second deployment
// rewrites the three values rather than appending three more. That matters because an
// UpdateStack re-creates unchanged resources here, so the second pass is the normal case
// rather than an edge one.
func TestCFN_StampIsIdempotent(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "twice-stack"
	tmpl := `{"Resources": {"Igw": {"Type": "AWS::EC2::InternetGateway", "Properties": {}}}}`

	first, err := f.deployer.Deploy(context.Background(), tmpl, stackName, nil)
	require.NoError(t, err)
	igw := physicalIDs(t, first)["Igw"]
	require.Len(t, f.tagsFor(t, igw), 3)

	// A second deployment of the same template mints a second gateway; the first one's tags
	// must be untouched and un-duplicated.
	_, err = f.deployer.Deploy(context.Background(), tmpl, stackName, nil)
	require.NoError(t, err)
	assert.Len(t, f.tagsFor(t, igw), 3, "the stamp upserts rather than appends")
}

// TestCFN_ANonEC2ResourceIsNotStamped pins the silent skip.
//
// A bucket name resolves to no EC2 state key, so nothing is written and nothing is logged —
// the deployer creates far more non-EC2 resources than EC2 ones, and a warning per resource
// would drown a real one. The stamp beyond EC2 is #765.
func TestCFN_ANonEC2ResourceIsNotStamped(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	tmpl := `{"Resources": {
		"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "stamp-test-bucket"}},
		"Vpc":    {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.9.0.0/16"}}
	}}`

	result, err := f.deployer.Deploy(context.Background(), tmpl, "mixed-stack", nil)
	require.NoError(t, err)
	ids := physicalIDs(t, result)

	assert.Empty(t, f.tagsFor(t, ids["Bucket"]), "a bucket carries no EC2 tags")
	assert.Len(t, f.tagsFor(t, ids["Vpc"]), 3, "and the EC2 resource beside it is still stamped")
}

// TestCFN_AFailedResourceIsNotStamped pins that the stamp does not claim a resource exists.
//
// A resource CloudFormation reports as CREATE_FAILED may have no physical ID at all, and
// tagging one would put a stack's bookkeeping on something the stack does not own.
func TestCFN_AFailedResourceIsNotStamped(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	tmpl := `{"Resources": {"Instance": {"Type": "AWS::EC2::Instance", "Properties": {
		"ImageId": "ami-000000000000nope", "InstanceType": "t3.micro"}}}}`

	result, err := f.deployer.Deploy(context.Background(), tmpl, "failed-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.NotEmpty(t, result.Resources[0].Error, "an unknown AMI fails the resource")
	assert.Empty(t, f.tagsFor(t, result.Resources[0].PhysicalID))
}

// TestCFN_BundledCleanupStatementBecomesSatisfiable is the end of the chain and the reason
// the issue exists.
//
// AmazonECS_FullAccess's ManagedCloudformationResourcesCleanupPolicy allows four EC2 deletes
// on any resource tagged aws:cloudformation:stack-name like "EC2ContainerService-*". Before
// the stamp, no resource could carry that tag, so the statement granted nothing at all. Here
// it authorizes a delete of a resource an EC2ContainerService-* stack created, and still
// refuses the same delete for a resource created by a stack whose name does not match — which
// is what shows the tag is being read rather than the condition ignored.
func TestCFN_BundledCleanupStatementBecomesSatisfiable(t *testing.T) {
	state := newAuthTestState(t, "ecsuser", "arn:aws:iam::aws:policy/AmazonECS_FullAccess",
		emulator.PolicyDocument{})
	f := newCFNStampFixture(t, state)
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

	tmpl := `{"Resources": {
		"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.10.0.0/16"}},
		"Sg":  {"Type": "AWS::EC2::SecurityGroup", "Properties": {
			"GroupDescription": "cluster sg", "VpcId": {"Ref": "Vpc"}}}
	}}`

	matching, err := f.deployer.Deploy(context.Background(), tmpl, "EC2ContainerService-web", nil)
	require.NoError(t, err)
	other, err := f.deployer.Deploy(context.Background(), tmpl, "unrelated-stack", nil)
	require.NoError(t, err)

	deleteSG := func(groupID string) error {
		return auth.CheckAccess(
			newAuthTestReqCtx("arn:aws:iam::"+cfnStampAccount+":user/ecsuser"),
			&emulator.AWSRequest{
				Service:   "ec2",
				Operation: "DeleteSecurityGroup",
				Path:      "/",
				Params:    map[string]string{"GroupId.1": groupID},
			})
	}

	require.NoError(t, deleteSG(physicalIDs(t, matching)["Sg"]),
		"the bundled cleanup statement grants the delete for a stack it names")

	err = deleteSG(physicalIDs(t, other)["Sg"])
	require.Error(t, err, "a security group from an unrelated stack is not covered")
	assert.True(t, strings.Contains(err.Error(), "not authorized"),
		"refused by the policy, got %v", err)
}
