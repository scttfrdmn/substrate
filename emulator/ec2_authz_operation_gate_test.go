package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Whether an EC2 request has a resource is a property of the *operation*, not of the
// parameters the request carries (#762).
//
// #730 and #744 taught the decision to resolve every ID under `InstanceId`, `GroupId`,
// `RouteTableId` and `InternetGatewayId` — for any operation carrying one, because nothing
// consulted `req.Operation`. AWS documents `ec2:DescribeInstances` as supporting no
// resource-level permissions, so a policy scoping it to an instance ARN grants nothing
// there and granted exactly those instances here. That is the failure direction that
// matters: it grants, and the caller's test passes here while their deployment fails on
// AWS.
//
// The tests below are on both sides of the line. The line itself is not a hand-written
// operation list — it is AWS's Service Reference Information, vendored and generated (see
// emulator/authzref) — so the assertions are about the gate, and
// TestAuthzReference_AgreesWithTheOperationsTheGateWillDecide is what pins each operation's
// classification against AWS.

// TestEC2_Authz_OperationDecidesWhetherAResourceIsResolved is the headline: the same
// parameter on two operations produces two different request resources.
func TestEC2_Authz_OperationDecidesWhetherAResourceIsResolved(t *testing.T) {
	const pgName, pgID = "cluster-a", "pg-0aab11112222bbbb3"
	tests := []struct {
		name      string
		operation string
		params    map[string]string
		resource  string
		seed      func(f *ec2AuthzFixture, t *testing.T)
		why       string
	}{
		{
			name:      "DescribeInstances names instances and is still decided against *",
			operation: "DescribeInstances",
			params:    map[string]string{"InstanceId.1": ec2MultiInstanceA, "InstanceId.2": ec2MultiInstanceB},
			resource:  "*",
			why:       "AWS publishes no resource type for ec2:DescribeInstances",
		},
		{
			name:      "TerminateInstances keeps its per-ID expansion",
			operation: "TerminateInstances",
			params:    map[string]string{"InstanceId.1": ec2MultiInstanceA},
			resource:  ec2MultiARN("instance", ec2MultiInstanceA),
			why:       "AWS scopes ec2:TerminateInstances to instance",
		},
		{
			name:      "DescribeSecurityGroups names groups and is decided against *",
			operation: "DescribeSecurityGroups",
			params:    map[string]string{"GroupId.1": ec2AuthzSG},
			resource:  "*",
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putSecurityGroup(t, ec2AuthzSG, nil)
			},
			why: "AWS publishes no resource type for ec2:DescribeSecurityGroups",
		},
		{
			name:      "DeleteSecurityGroup resolves the same parameter",
			operation: "DeleteSecurityGroup",
			params:    map[string]string{"GroupId": ec2AuthzSG},
			resource:  ec2AuthzSGARN,
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putSecurityGroup(t, ec2AuthzSG, nil)
			},
			why: "AWS scopes ec2:DeleteSecurityGroup to security-group",
		},
		{
			// The over-reach docs/services.md recorded as deliberately left: GroupId is
			// overloaded, so a pg- ID was resolving to a placement-group ARN on an
			// operation AWS scopes to "*".
			name:      "DescribePlacementGroups with a pg- GroupId is decided against *",
			operation: "DescribePlacementGroups",
			params:    map[string]string{"GroupId.1": pgID},
			resource:  "*",
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.put(t, "placement_group:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+pgName,
					emulator.EC2PlacementGroup{GroupName: pgName, GroupID: pgID})
			},
			why: "AWS publishes no resource type for ec2:DescribePlacementGroups",
		},
		{
			name:      "DescribeRouteTables names a route table and is decided against *",
			operation: "DescribeRouteTables",
			params:    map[string]string{"RouteTableId.1": ec2MultiRTBB},
			resource:  "*",
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putRouteTable(t, ec2MultiRTBB, nil)
			},
			why: "AWS publishes no resource type for ec2:DescribeRouteTables",
		},
		{
			name:      "DeleteRouteTable resolves the same parameter",
			operation: "DeleteRouteTable",
			params:    map[string]string{"RouteTableId": ec2MultiRTBB},
			resource:  ec2MultiARN("route-table", ec2MultiRTBB),
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putRouteTable(t, ec2MultiRTBB, nil)
			},
			why: "AWS scopes ec2:DeleteRouteTable to route-table",
		},
		{
			name:      "DescribeInternetGateways is decided against *",
			operation: "DescribeInternetGateways",
			params:    map[string]string{"InternetGatewayId.1": ec2MultiIGWB},
			resource:  "*",
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putInternetGateway(t, ec2MultiIGWB, nil)
			},
			why: "AWS publishes no resource type for ec2:DescribeInternetGateways",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "vera", emulator.PolicyDocument{})
			if tt.seed != nil {
				tt.seed(f, t)
			}
			// An action-only Deny refuses whatever the decision resolves, so the denial
			// message reports the resource the request was actually decided against.
			f.setPolicy(t, emulator.PolicyStatement{
				Effect:   "Deny",
				Action:   emulator.StringOrSlice{"ec2:" + tt.operation},
				Resource: emulator.StringOrSlice{"*"},
			})
			err := f.call(t, tt.operation, tt.params)
			require.Error(t, err)
			assert.Equal(t, tt.resource, deniedResource(t, err), tt.why)
		})
	}
}

// TestEC2_Authz_AResourceScopedDescribeGrantsNothing is #762 stated as the consequence a
// consumer sees. A policy scoping a describe to one instance ARN allowed exactly that
// instance here and nothing at all on AWS, so a test written against substrate passed and
// the deployment behind it failed.
func TestEC2_Authz_AResourceScopedDescribeGrantsNothing(t *testing.T) {
	f := newEC2AuthzFixture(t, "wendel", emulator.PolicyDocument{})
	f.putInstance(t, ec2MultiInstanceA, nil)
	instanceARN := ec2MultiARN("instance", ec2MultiInstanceA)

	f.setPolicy(t, ec2MultiStatement("ec2:DescribeInstances", instanceARN))
	err := f.call(t, "DescribeInstances", map[string]string{"InstanceId.1": ec2MultiInstanceA})
	require.True(t, ec2AuthzDenied(t, err),
		"a policy scoping ec2:DescribeInstances to an instance ARN still granted that instance")
	assert.Equal(t, "*", deniedResource(t, err),
		"the denial names the resource AWS would evaluate the call against")

	// And the statement AWS would require does grant it.
	f.setPolicy(t, ec2MultiStatement("ec2:DescribeInstances", "*"))
	require.NoError(t, f.call(t, "DescribeInstances", map[string]string{"InstanceId.1": ec2MultiInstanceA}))
}

// TestEC2_Authz_ATagConditionOnAWildcardDescribeCannotBeEvaluated records the honest cost of
// the gate, so it is a decision rather than a surprise: with the request resource "*" there
// is no resource whose tags to read, so an `ec2:ResourceTag/…` condition on a describe no
// longer matches. That is AWS's behavior too — AWS cannot evaluate a resource tag for an
// action with no resource — and it is why the condition belongs on the mutating operations
// #730 added it for.
func TestEC2_Authz_ATagConditionOnAWildcardDescribeCannotBeEvaluated(t *testing.T) {
	f := newEC2AuthzFixture(t, "xenia", emulator.PolicyDocument{})
	f.putInstance(t, ec2MultiInstanceA, map[string]string{"Env": "dev"})
	params := map[string]string{"InstanceId.1": ec2MultiInstanceA}

	tagged := emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{"ec2:DescribeInstances", "ec2:TerminateInstances"},
		Resource: emulator.StringOrSlice{"*"},
		Condition: map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {"ec2:ResourceTag/Env": emulator.StringOrSlice{"dev"}},
		},
	}

	f.setPolicy(t, tagged)
	// The mutating operation resolves the instance, so its tags are there to compare.
	require.NoError(t, f.call(t, "TerminateInstances", params),
		"TerminateInstances resolves the instance, so ec2:ResourceTag/Env is evaluable")

	err := f.call(t, "DescribeInstances", params)
	require.True(t, ec2AuthzDenied(t, err),
		"a resource-tag condition matched a describe, which has no resource to read tags from")
}

// TestEC2_Authz_AnOperationAWSDoesNotPublishIsDecidedAgainstStar pins the other half of the
// classification: substrate is not entitled to narrow an operation AWS's reference does not
// mention. A stale snapshot, a rename on either side, or an operation substrate invents all
// land here, and all get "*" — the direction that can only ever narrow a grant.
func TestEC2_Authz_AnOperationAWSDoesNotPublishIsDecidedAgainstStar(t *testing.T) {
	require.False(t, emulator.AuthzActionSupportsResourceTypeForTest("ec2", "TerminateInstancesTypo", "instance"),
		"an unpublished operation must not be narrowed")

	f := newEC2AuthzFixture(t, "yuri", emulator.PolicyDocument{})
	f.putInstance(t, ec2MultiInstanceA, nil)
	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstancesTypo", ec2MultiARN("instance", ec2MultiInstanceA)))

	// Driven through the resolver directly: an operation substrate does not route cannot be
	// dispatched, and it is the resolver's answer that is under test.
	arns := emulator.EC2AuthzNamedResourceARNsForTest(f.state, "TerminateInstancesTypo",
		map[string]string{"InstanceId.1": ec2MultiInstanceA})
	assert.Empty(t, arns,
		"an operation AWS does not publish resolved a resource, so a stale snapshot would widen a grant")
}
