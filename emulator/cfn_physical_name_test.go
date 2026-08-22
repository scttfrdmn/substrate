package emulator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// repeatableTemplate is #560's reproducer: every resource omits its physical
// name, which is the recommended practice precisely because it is what makes a
// template deployable more than once.
const repeatableTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyRole": {
			"Type": "AWS::IAM::Role",
			"Properties": {
				"AssumeRolePolicyDocument": {
					"Version": "2012-10-17",
					"Statement": [{
						"Effect": "Allow",
						"Principal": {"Service": "ec2.amazonaws.com"},
						"Action": "sts:AssumeRole"
					}]
				}
			}
		},
		"MyQueue": {"Type": "AWS::SQS::Queue", "Properties": {}},
		"MyTopic": {"Type": "AWS::SNS::Topic", "Properties": {}}
	},
	"Outputs": {
		"RoleRef":  {"Value": {"Ref": "MyRole"}},
		"RoleArn":  {"Value": {"Fn::GetAtt": ["MyRole", "Arn"]}},
		"QueueRef": {"Value": {"Ref": "MyQueue"}}
	}
}`

// resourceByLogicalID returns the deployed resource with the given logical ID.
func resourceByLogicalID(t *testing.T, r *emulator.DeployResult, id string) emulator.DeployedResource {
	t.Helper()
	for _, res := range r.Resources {
		if res.LogicalID == id {
			return res
		}
	}
	t.Fatalf("no resource %q in %+v", id, r.Resources)
	return emulator.DeployedResource{}
}

// TestCFNGeneratedName_SameTemplateDeploysTwice is #560. A logical ID is unique
// only within a stack, but an IAM role name is unique across the account, so
// using the logical ID verbatim meant the second stack from any template that
// omits its names could never be created. It failed with EntityAlreadyExists.
func TestCFNGeneratedName_SameTemplateDeploysTwice(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	first, err := d.Deploy(ctx, repeatableTemplate, "repro-a", nil)
	require.NoError(t, err)
	second, err := d.Deploy(ctx, repeatableTemplate, "repro-b", nil)
	require.NoError(t, err)

	for _, res := range append(append([]emulator.DeployedResource{}, first.Resources...),
		second.Resources...) {
		assert.Empty(t, res.Error, "%s must deploy cleanly in both stacks", res.LogicalID)
	}

	for _, id := range []string{"MyRole", "MyQueue", "MyTopic"} {
		a := resourceByLogicalID(t, first, id).PhysicalID
		b := resourceByLogicalID(t, second, id).PhysicalID
		assert.NotEqual(t, id, a, "%s must not use the logical ID as its physical name", id)
		assert.NotEqual(t, a, b, "%s must differ between the two stacks", id)
		// Contains, not HasPrefix: an SNS topic's physical ID is its ARN — Ref on
		// AWS::SNS::Topic returns the topic ARN — so the generated name is embedded
		// in it rather than leading it.
		assert.Contains(t, a, "repro-a-"+id+"-", "unexpected name %q", a)
		assert.Contains(t, b, "repro-b-"+id+"-", "unexpected name %q", b)
	}
}

// TestCFNGeneratedName_TwoStacksDoNotShareAQueue is the failure mode #560 does
// not name, and the worse of the two. CreateQueue and CreateTopic are idempotent,
// so two stacks from one template did not collide — they both reported
// CREATE_COMPLETE while pointing at a single queue. Deleting either stack then
// destroyed the other's resource, and nothing reported an error to anyone.
func TestCFNGeneratedName_TwoStacksDoNotShareAQueue(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	keep, err := d.Deploy(ctx, repeatableTemplate, "share-a", nil)
	require.NoError(t, err)
	_, err = d.Deploy(ctx, repeatableTemplate, "share-b", nil)
	require.NoError(t, err)

	require.NoError(t, d.DeleteStack(ctx, "share-b"))

	// The surviving stack's queue must still resolve. Before the fix this
	// returned QueueDoesNotExist while share-a still reported CREATE_COMPLETE.
	kept := resourceByLogicalID(t, keep, "MyQueue").PhysicalID
	resp, err := d.DispatchForTest(ctx, &emulator.AWSRequest{
		Service:   "sqs",
		Operation: "GetQueueUrl",
		Headers:   map[string]string{},
		Params:    map[string]string{"Action": "GetQueueUrl", "QueueName": kept},
	}, "share-a")
	require.NoError(t, err, "deleting share-b must not destroy share-a's queue")
	require.NotNil(t, resp)
	assert.Contains(t, string(resp.Body), kept)
}

// TestCFNGeneratedName_ExplicitNameStillWins asserts the generated name is only
// a fallback. A template that names a resource is asking for AWS's own
// un-repeatable behavior and must get exactly the name it wrote.
func TestCFNGeneratedName_ExplicitNameStillWins(t *testing.T) {
	tmpl := `{
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {"QueueName": "explicitly-named"}
			}
		}
	}`

	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "explicit", nil)
	require.NoError(t, err)
	assert.Equal(t, "explicitly-named",
		resourceByLogicalID(t, result, "MyQueue").PhysicalID)
}

// TestCFNGeneratedName_RefAndGetAttResolveToIt asserts the intrinsics follow the
// generated name. They read DeployedResource.PhysicalID and .ARN, so a template
// that wires resources together with Ref and GetAtt keeps working — a fix that
// left Ref returning the logical ID would produce a stack whose resources point
// at names that do not exist.
func TestCFNGeneratedName_RefAndGetAttResolveToIt(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), repeatableTemplate, "refs", nil)
	require.NoError(t, err)

	role := resourceByLogicalID(t, result, "MyRole")
	assert.Equal(t, role.PhysicalID, result.Outputs["RoleRef"])
	assert.Equal(t, role.ARN, result.Outputs["RoleArn"])
	assert.Contains(t, result.Outputs["RoleArn"], role.PhysicalID)
	assert.Equal(t, resourceByLogicalID(t, result, "MyQueue").PhysicalID,
		result.Outputs["QueueRef"])
}

// TestCFNGeneratedName_UpdateDoesNotMoveAPhysicalID is why the suffix is derived
// rather than random. UpdateStack in substrate is a re-Deploy of the whole
// template (clearUnchangedRedeploys exists for that reason), so a name
// regenerated per deploy would mint a new resource on every update and leak the
// one it replaced. This is the gate a crypto/rand suffix fails.
func TestCFNGeneratedName_UpdateDoesNotMoveAPhysicalID(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	before, err := d.Deploy(ctx, repeatableTemplate, "stable", nil)
	require.NoError(t, err)
	after, err := d.UpdateStack(ctx, repeatableTemplate, "stable", nil)
	require.NoError(t, err)

	for _, id := range []string{"MyRole", "MyQueue", "MyTopic"} {
		assert.Equal(t, resourceByLogicalID(t, before, id).PhysicalID,
			resourceByLogicalID(t, after, id).PhysicalID,
			"%s: an unchanged update must not move a physical ID", id)
	}
}

// TestCFNGeneratedName_Constraints covers the name arithmetic directly, because
// the shortest limit in the table (63, for a bucket) is under the 94 characters
// an untruncated {40-char stack}-{40-char logical}-{12} would need. Substrate
// validates bucket names but not IAM role-name length, so without truncation an
// over-long name would fail for some types and silently pass for others.
func TestCFNGeneratedName_Constraints(t *testing.T) {
	const (
		acct   = "123456789012"
		region = "us-east-1"
		long40 = "abcdefghijabcdefghijabcdefghijabcdefghij"
	)

	tests := []struct {
		name      string
		stackName string
		resType   string
		logicalID string
		maxLen    int
		lower     bool
	}{
		{"role fits 64", long40, "AWS::IAM::Role", long40, 64, false},
		{"bucket fits 63 and lowercases", long40, "AWS::S3::Bucket", "MyBucket", 63, true},
		{"queue fits 80", long40, "AWS::SQS::Queue", long40, 80, false},
		{"short names are not truncated", "st", "AWS::IAM::Role", "Res", 64, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emulator.CFNGeneratedNameForTest(acct, region, tt.stackName,
				tt.resType, tt.logicalID)

			assert.LessOrEqual(t, len(got), tt.maxLen, "name %q exceeds the limit", got)
			if tt.lower {
				assert.Equal(t, strings.ToLower(got), got, "name must be lowercase")
			}

			// The documented charset for a generated name: begins with a letter,
			// ASCII letters/digits/hyphens only, no trailing or doubled hyphen.
			require.NotEmpty(t, got)
			assert.Regexp(t, `^[A-Za-z]`, got)
			assert.Regexp(t, `^[A-Za-z0-9-]+$`, got)
			assert.NotContains(t, got, "--")
			assert.False(t, strings.HasSuffix(got, "-"))

			// The suffix is never sacrificed to truncation: it is the only part of
			// the name that makes it unique.
			parts := strings.Split(got, "-")
			assert.Len(t, parts[len(parts)-1], emulator.CFNGeneratedNameSuffixLenForTest)
		})
	}
}

// TestCFNGeneratedName_ScopeChangesTheName pins the hash inputs. Each is here
// for a reason: dropping the stack name reinstates #560, and dropping the
// account or Region makes two same-named stacks in different Regions collide —
// which sameScope already treats as different stacks.
func TestCFNGeneratedName_ScopeChangesTheName(t *testing.T) {
	base := emulator.CFNGeneratedNameForTest("123456789012", "us-east-1", "stk",
		"AWS::IAM::Role", "MyRole")

	tests := []struct {
		name                           string
		acct, region, stack, logicalID string
	}{
		{"a different stack", "123456789012", "us-east-1", "other", "MyRole"},
		{"a different Region", "123456789012", "eu-west-1", "stk", "MyRole"},
		{"a different account", "111111111111", "us-east-1", "stk", "MyRole"},
		{"a different logical ID", "123456789012", "us-east-1", "stk", "OtherRole"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emulator.CFNGeneratedNameForTest(tt.acct, tt.region, tt.stack,
				"AWS::IAM::Role", tt.logicalID)
			assert.NotEqual(t, base, got, "%s must produce a different name", tt.name)
		})
	}
}

// TestCFNGeneratedName_UntabledTypeKeepsItsLogicalID asserts the fix is a table
// of account-unique names rather than a blanket rewrite. An API Gateway
// resource's PathPart is a URL segment unique only within its parent, so
// generating it would change the URL a template was written to produce.
func TestCFNGeneratedName_UntabledTypeKeepsItsLogicalID(t *testing.T) {
	for _, resType := range []string{
		"AWS::ApiGateway::Resource",
		"AWS::ElastiCache::ReplicationGroup",
		"AWS::SecretsManager::Secret",
		"AWS::Unknown::Thing",
	} {
		assert.Equal(t, "Widget", emulator.CFNGeneratedNameForTest("123456789012",
			"us-east-1", "stk", resType, "Widget"),
			"%s is not in the generated-name table and must keep its logical ID", resType)
	}
}

// TestCFNGeneratedName_LongStackNamesStillDiffer is why the stack name is in the
// *hash* and not only in the visible segment. Two stack names long enough to be
// truncated to the same prefix produce the same segments, so the suffix is the
// only thing left distinguishing them — and if the suffix did not depend on the
// full stack name, two real stacks would collide exactly as they did in #560.
// Measured: dropping stackName from cfnNameSuffix passes every other test here.
func TestCFNGeneratedName_LongStackNamesStillDiffer(t *testing.T) {
	const (
		acct   = "123456789012"
		region = "us-east-1"
		prefix = "a-very-long-stack-name-that-will-be-truncated"
	)

	for _, resType := range []string{"AWS::S3::Bucket", "AWS::IAM::Role"} {
		t.Run(resType, func(t *testing.T) {
			first := emulator.CFNGeneratedNameForTest(acct, region, prefix+"-one",
				resType, "MyResource")
			second := emulator.CFNGeneratedNameForTest(acct, region, prefix+"-two",
				resType, "MyResource")

			// Not vacuous: the segments really are truncated to the same text, so the
			// suffix is carrying the whole distinction.
			cut := len(first) - emulator.CFNGeneratedNameSuffixLenForTest
			assert.Equal(t, first[:cut], second[:cut],
				"the test needs two names whose segments truncate identically")
			assert.NotEqual(t, first, second,
				"two stacks truncating to the same segments must still get different names")
		})
	}
}
