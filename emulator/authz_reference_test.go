package emulator_test

import (
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuthzReference_ClassifiesAnActionAsAWSPublishesIt pins the distinction the whole
// table exists for: an action AWS documents with no resource type is published and empty,
// which is how the reference says "no resource-level permissions", and is a different fact
// from an action AWS does not publish at all. A single-return lookup collapses the two,
// which is why the reader returns both.
func TestAuthzReference_ClassifiesAnActionAsAWSPublishesIt(t *testing.T) {
	tests := []struct {
		name      string
		service   string
		operation string
		published bool
		want      []string
	}{
		{
			name: "a describe supports no resource-level permissions", service: "ec2",
			operation: "DescribeInstances", published: true, want: []string{},
		},
		{
			name: "the operation #762 names in prose", service: "ec2",
			operation: "DescribePlacementGroups", published: true, want: []string{},
		},
		{
			name: "a terminate is scoped to the instance", service: "ec2",
			operation: "TerminateInstances", published: true, want: []string{"instance"},
		},
		{
			name: "an operation may support several types", service: "ec2",
			operation: "ModifyInstanceAttribute", published: true,
			want: []string{"instance", "security-group", "volume"},
		},
		{
			name: "an IAM read names its entity", service: "iam",
			operation: "GetUser", published: true, want: []string{"user"},
		},
		{
			name: "an IAM listing is account-wide", service: "iam",
			operation: "ListUsers", published: true, want: []string{},
		},
		{
			name: "an operation AWS does not publish", service: "ec2",
			operation: "DescribeNothingAtAll", published: false, want: nil,
		},
		{
			name: "a service the snapshot does not cover", service: "s3",
			operation: "ListBucket", published: false, want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := emulator.AuthzActionResourceTypesForTest(tt.service, tt.operation)
			assert.Equal(t, tt.published, ok, "whether AWS publishes the action")
			if tt.published {
				assert.Equal(t, tt.want, got)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

// TestAuthzReference_SupportsResourceTypeAnswersFalseForBothKindsOfAbsence asserts the
// gate #762 turns on. An action AWS scopes to `*` and an action AWS does not publish both
// answer false, because narrowing either one would invent a grant: a policy scoping the
// operation to an ARN would work here and not on AWS.
func TestAuthzReference_SupportsResourceTypeAnswersFalseForBothKindsOfAbsence(t *testing.T) {
	assert.True(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", "TerminateInstances", "instance"))
	assert.True(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", "ModifyInstanceAttribute", "security-group"),
		"an operation naming several types supports each of them")

	assert.False(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", "DescribeInstances", "instance"),
		"AWS documents no resource type for DescribeInstances, so its resource is *")
	assert.False(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", "TerminateInstances", "volume"),
		"a type the operation does not name is not supported by it")
	assert.False(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", "DescribeNothingAtAll", "instance"),
		"an unpublished operation must not be narrowed either")
}

// TestAuthzReference_PublishesTheARNFormatSubstrateBuilds keeps the ARN shapes substrate
// mints tied to AWS's published ones. The formats are prose templates, not something
// substrate fills in, so the assertion is on the string AWS documents.
func TestAuthzReference_PublishesTheARNFormatSubstrateBuilds(t *testing.T) {
	tests := []struct {
		service, resourceType, want string
	}{
		{"ec2", "instance", "arn:${Partition}:ec2:${Region}:${Account}:instance/${InstanceId}"},
		{"iam", "user", "arn:${Partition}:iam::${Account}:user/${UserNameWithPath}"},
		{"iam", "role", "arn:${Partition}:iam::${Account}:role/${RoleNameWithPath}"},
		{"iam", "group", "arn:${Partition}:iam::${Account}:group/${GroupNameWithPath}"},
		{"iam", "policy", "arn:${Partition}:iam::${Account}:policy/${PolicyNameWithPath}"},
		{
			"iam", "instance-profile",
			"arn:${Partition}:iam::${Account}:instance-profile/${InstanceProfileNameWithPath}",
		},
		{
			"iam", "assumed-role",
			"arn:${Partition}:iam::${Account}:assumed-role/${RoleName}/${RoleSessionName}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.service+":"+tt.resourceType, func(t *testing.T) {
			formats, ok := emulator.AuthzResourceARNFormatsForTest(tt.service, tt.resourceType)
			require.True(t, ok, "AWS publishes this resource type")
			assert.Contains(t, formats, tt.want)
		})
	}

	_, ok := emulator.AuthzResourceARNFormatsForTest("ec2", "not-a-resource-type")
	assert.False(t, ok, "an unpublished type has no format")
}

// TestAuthzReference_KnowsEveryEC2TypeSubstrateResolves sweeps every ID prefix
// [ec2TaggableResource] recognizes and asserts AWS publishes the resource type it names.
//
// This is the join between substrate's own vocabulary and AWS's. The #762 gate compares
// the two by string, so a type substrate spells differently from AWS — `natgateway` is
// unhyphenated where every sibling is hyphenated, and `elastic-ip` is not `eip` — would
// silently never match and quietly widen every operation naming it to `*`.
func TestAuthzReference_KnowsEveryEC2TypeSubstrateResolves(t *testing.T) {
	// One ID per arm of the resolver, in its order. A fleet ID carries internal hyphens
	// of its own, and pg-/key- resolve their type even when the resource is absent.
	ids := []string{
		"i-0123456789abcdef0", "vpc-0123456789abcdef0", "subnet-0123456789abcdef0",
		"sg-0123456789abcdef0", "igw-0123456789abcdef0", "rtb-0123456789abcdef0",
		"eipalloc-0123456789abcdef0", "nat-0123456789abcdef0", "vol-0123456789abcdef0",
		"snap-0123456789abcdef0", "ami-0123456789abcdef0", "lt-0123456789abcdef0",
		"fleet-12a34b56-7cd8-90ef-1234-567890abcdef", "pg-0123456789abcdef0",
		"key-0123456789abcdef0",
	}

	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		arnType := emulator.EC2TaggableARNTypeForTest(id)
		require.NotEmpty(t, arnType, "substrate resolves %q to a resource type", id)
		seen[arnType] = true

		_, ok := emulator.AuthzResourceARNFormatsForTest("ec2", arnType)
		assert.True(t, ok,
			"substrate resolves %q to EC2 resource type %q, which AWS does not publish — "+
				"the #762 gate compares these by string, so a mismatch widens every operation "+
				"naming it to *", id, arnType)
	}
	assert.Len(t, seen, 15, "the resolver has fifteen types; add the new one's ID above")
}

// TestAuthzReference_AgreesWithTheOperationsTheGateWillDecide guards the operations #762's
// gate changes and the ones it must leave alone, read off AWS's table rather than asserted
// from memory. The three EC2 resolvers substrate already gates by operation —
// CreateTags/DeleteTags and RunInstances — must stay resource-scoped, or the gate would
// undo #662 and #674 while fixing #762.
func TestAuthzReference_AgreesWithTheOperationsTheGateWillDecide(t *testing.T) {
	resourceScoped := map[string]string{
		"CreateTags":                "instance",
		"DeleteTags":                "instance",
		"RunInstances":              "instance",
		"StopInstances":             "instance",
		"RebootInstances":           "instance",
		"AssociateRouteTable":       "route-table",
		"AttachInternetGateway":     "internet-gateway",
		"DeleteRoute":               "route-table",
		"RevokeSecurityGroupEgress": "security-group",
	}
	for operation, resourceType := range resourceScoped {
		assert.True(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", operation, resourceType),
			"ec2:%s must stay scoped to %s", operation, resourceType)
	}

	starOnly := []string{
		"DescribeInstances", "DescribeSecurityGroups", "DescribeRouteTables",
		"DescribeInternetGateways", "DescribePlacementGroups", "DescribeTags",
		"DescribeVolumes", "DescribeLaunchTemplates",
	}
	for _, operation := range starOnly {
		types, ok := emulator.AuthzActionResourceTypesForTest("ec2", operation)
		require.True(t, ok, "AWS publishes ec2:%s", operation)
		assert.Empty(t, types, "ec2:%s supports no resource-level permissions", operation)
	}
}
