package emulator_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #730: the ec2:ResourceTag/<key> prefix, and the resolver that makes it observable.
//
// Two of the AWS-authored managed policies substrate bundles condition on
// ec2:ResourceTag/<key> rather than the global aws:ResourceTag/<key>, and both were
// inert: substrate wrote a resource's tags under the aws: prefix only, and resolved a
// single EC2 resource from InstanceId alone, so DeleteSecurityGroup, DeleteRouteTable,
// DeleteRoute and DeleteInternetGateway were all decided against the literal "*" with no
// tags in the condition context at all. A grant scoped to a tag matched nothing, and a
// Deny scoped to one was silently ignored.

// ec2AuthzRouteTable and ec2AuthzIGW are the two IDs the delete tests name.
const (
	ec2AuthzRouteTable = "rtb-0eee66667777ffff8"
	ec2AuthzIGW        = "igw-0fff88889999aaab0"
)

var (
	ec2AuthzRouteTableARN = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":route-table/" + ec2AuthzRouteTable
	ec2AuthzIGWARN        = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":internet-gateway/" + ec2AuthzIGW
)

func (f *ec2AuthzFixture) putRouteTable(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	f.put(t, "rtb:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+id, emulator.EC2RouteTable{
		RouteTableID: id,
		Tags:         ec2AuthzTags(tags),
	})
}

func (f *ec2AuthzFixture) putInternetGateway(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	f.put(t, "igw:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+id, emulator.EC2InternetGateway{
		InternetGatewayID: id,
		Tags:              ec2AuthzTags(tags),
	})
}

// ec2AuthzTagCondition builds the condition shape both bundled policies use: one
// operator, one ec2:ResourceTag key, one value.
func ec2AuthzTagCondition(operator, key, value string) map[string]map[string]emulator.StringOrSlice {
	return map[string]map[string]emulator.StringOrSlice{
		operator: {key: emulator.StringOrSlice{value}},
	}
}

// ec2AuthzConditionStatement allows one action on "*" if the condition holds, which is
// exactly the shape of ManagedCloudformationResourcesCleanupPolicy: Resource "*", scoped
// by a tag rather than by an ARN.
func ec2AuthzConditionStatement(
	action string,
	condition map[string]map[string]emulator.StringOrSlice,
) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:    "Allow",
		Action:    emulator.StringOrSlice{action},
		Resource:  emulator.StringOrSlice{"*"},
		Condition: condition,
	}
}

// TestEC2Authz_ResourceTagPrefix_GrantsOnTheServicePrefix is the bundled cleanup policy's
// own statement, run against a tagged and an untagged security group.
//
// AWS's ManagedCloudformationResourcesCleanupPolicy conditions ec2:DeleteSecurityGroup on
// StringLike ec2:ResourceTag/aws:cloudformation:stack-name — so before the second prefix
// existed, a caller carrying AWS's own policy could delete nothing.
func TestEC2Authz_ResourceTagPrefix_GrantsOnTheServicePrefix(t *testing.T) {
	t.Parallel()
	f := newEC2AuthzFixture(t, "cfn-cleanup", emulator.PolicyDocument{})
	f.setPolicy(t, ec2AuthzConditionStatement("ec2:DeleteSecurityGroup",
		ec2AuthzTagCondition("StringLike", "ec2:ResourceTag/aws:cloudformation:stack-name",
			"EC2ContainerService-*")))

	f.putSecurityGroup(t, ec2AuthzSG, map[string]string{
		"aws:cloudformation:stack-name": "EC2ContainerService-prod",
	})
	require.NoError(t, f.call(t, "DeleteSecurityGroup", map[string]string{"GroupId": ec2AuthzSG}),
		"the tag matches the pattern AWS's own policy names")

	// The same request against a group the pattern does not match is denied, which is
	// what shows the grant is the tag's doing and not the prefix merely being present.
	other := "sg-0999aaaabbbbcccc1"
	f.putSecurityGroup(t, other, map[string]string{"aws:cloudformation:stack-name": "SomethingElse"})
	err := f.call(t, "DeleteSecurityGroup", map[string]string{"GroupId": other})
	require.Error(t, err)
	assert.Contains(t, err.Error(), ec2AuthzSGARNFor(other),
		"and the refusal names the group's own ARN, not \"*\"")
}

// ec2AuthzSGARNFor is the ARN a security-group decision is made against.
func ec2AuthzSGARNFor(id string) string {
	return "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":security-group/" + id
}

// TestEC2Authz_ResourceTagPrefix_BothPrefixesReportTheSameTag pins that adding the
// service prefix did not take the global one away.
//
// AWS documents aws:ResourceTag/tag-key as global — "the tag attached to the resource that
// you specify in the policy" — so a policy written either way sees the same tag, and a
// policy written both ways must see it under both keys. #704's case folding is not
// involved: aws:ResourceTag/Env and ec2:ResourceTag/Env are two keys, not one compared
// case-insensitively.
func TestEC2Authz_ResourceTagPrefix_BothPrefixesReportTheSameTag(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"aws:ResourceTag/Env", "ec2:ResourceTag/Env"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			f := newEC2AuthzFixture(t, "tagged-delete", emulator.PolicyDocument{})
			f.setPolicy(t, ec2AuthzConditionStatement("ec2:DeleteRouteTable",
				ec2AuthzTagCondition("StringEquals", key, "dev")))

			f.putRouteTable(t, ec2AuthzRouteTable, map[string]string{"Env": "dev"})
			assert.NoError(t, f.call(t, "DeleteRouteTable",
				map[string]string{"RouteTableId": ec2AuthzRouteTable}))

			// And the tag is what decides it: retagged, the same policy denies.
			f.putRouteTable(t, ec2AuthzRouteTable, map[string]string{"Env": "prod"})
			assert.Error(t, f.call(t, "DeleteRouteTable",
				map[string]string{"RouteTableId": ec2AuthzRouteTable}))
		})
	}
}

// TestEC2Authz_ResourceTagPrefix_IsPerService asserts the prefix is not blanket.
//
// A resource's tags are reported under a service prefix only where AWS publishes one. An
// S3 bucket's tags under s3:ResourceTag/ would be a condition key AWS does not define, so
// a policy using it would be honored here and ignored on AWS — a divergence that grants,
// which is the worse direction.
func TestEC2Authz_ResourceTagPrefix_IsPerService(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		condKey string
		allowed bool
	}{
		"aws:ResourceTag/Env": {"aws:ResourceTag/Env", true},
		"s3:ResourceTag/Env":  {"s3:ResourceTag/Env", false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			policyARN := "arn:aws:iam::" + ec2AuthzAccount + ":policy/S3Prefix"
			state := newAuthTestState(t, "s3-prefix",
				policyARN, newABACPolicy("Allow", "s3:PutObject", "*", tc.condKey, "prod"))
			putS3BucketWithTags(t, state, "tagged-bucket", map[string]string{"Env": "prod"})

			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
			err := auth.CheckAccess(
				newAuthTestReqCtx("arn:aws:iam::"+ec2AuthzAccount+":user/s3-prefix"),
				&emulator.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/tagged-bucket/key"})

			if tc.allowed {
				assert.NoError(t, err, "the global key reports the bucket's tag")
			} else {
				assert.Error(t, err, "s3:ResourceTag/ is not a key AWS publishes")
			}
		})
	}
}

// TestEC2Authz_NamedResource_ResolvesTheARN covers the resolver half on its own, with no
// condition in play: an operation naming one resource by ID is decided against that
// resource's ARN rather than against "*".
//
// Each of the four parameters is here because a bundled policy's action names its target
// with it. DeleteRoute is the reason RouteTableId appears twice: the route table is the
// resource AWS authorizes a route change against.
func TestEC2Authz_NamedResource_ResolvesTheARN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		operation string
		params    map[string]string
		arn       string
	}{
		{"DeleteSecurityGroup", map[string]string{"GroupId": ec2AuthzSG}, ec2AuthzSGARN},
		{"DeleteRouteTable", map[string]string{"RouteTableId": ec2AuthzRouteTable}, ec2AuthzRouteTableARN},
		{"DeleteRoute", map[string]string{
			"RouteTableId": ec2AuthzRouteTable, "DestinationCidrBlock": "0.0.0.0/0",
		}, ec2AuthzRouteTableARN},
		{"DeleteInternetGateway", map[string]string{"InternetGatewayId": ec2AuthzIGW}, ec2AuthzIGWARN},
	}

	for _, tc := range cases {
		t.Run(tc.operation, func(t *testing.T) {
			t.Parallel()
			f := newEC2AuthzFixture(t, "arn-scoped", emulator.PolicyDocument{})
			f.putRouteTable(t, ec2AuthzRouteTable, nil)
			f.putInternetGateway(t, ec2AuthzIGW, nil)

			// An Allow naming the resource's own ARN grants the call, which it could
			// not when every such operation resolved to "*": resourceMatches passes the
			// statement's Resource to globMatch as the pattern, so a request resource of
			// "*" matched only a statement whose Resource began with "*".
			f.setPolicy(t, emulator.PolicyStatement{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"ec2:" + tc.operation},
				Resource: emulator.StringOrSlice{tc.arn},
			})
			assert.NoError(t, f.call(t, tc.operation, tc.params))

			// And an ARN-scoped Deny is no longer inert.
			f.setPolicy(t,
				emulator.PolicyStatement{
					Effect:   "Allow",
					Action:   emulator.StringOrSlice{"ec2:*"},
					Resource: emulator.StringOrSlice{"*"},
				},
				emulator.PolicyStatement{
					Effect:   "Deny",
					Action:   emulator.StringOrSlice{"ec2:" + tc.operation},
					Resource: emulator.StringOrSlice{tc.arn},
				})
			err := f.call(t, tc.operation, tc.params)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.arn)
		})
	}
}

// TestEC2Authz_NamedResource_UnresolvableFallsBackToWildcard pins the direction the
// resolver is allowed to move a decision in.
//
// ok=false sends the caller back to the "*" it used before, and that is safe in one
// direction only: "*" as the request resource matches a statement whose own Resource
// starts with "*", so replacing it with a real ARN can narrow a grant but never widen
// one. The three cases are the ones a live request produces — an ID naming no record, an
// ID whose prefix names no taggable type, and no ID parameter at all.
func TestEC2Authz_NamedResource_UnresolvableFallsBackToWildcard(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		operation string
		params    map[string]string
	}{
		"absent record":     {"DeleteRouteTable", map[string]string{"RouteTableId": "rtb-0dddeeeeffff11112"}},
		"untaggable prefix": {"DeleteVpcEndpoints", map[string]string{"GroupId": "vpce-0aaabbbbccccdddd3"}},
		"no ID at all":      {"DescribeRouteTables", map[string]string{}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newEC2AuthzFixture(t, "wildcard-fallback", emulator.PolicyDocument{})

			// A wildcard Allow still grants, so an unresolvable ID does not turn into a
			// refusal this function has no standing to make — the handler owes that
			// answer, as NotFound.
			f.setPolicy(t, emulator.PolicyStatement{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"ec2:*"},
				Resource: emulator.StringOrSlice{"*"},
			})
			assert.NoError(t, f.call(t, tc.operation, tc.params))

			// And with no tags to read, a tag condition is not satisfied — an absent
			// record must not read as an empty tag set that happens to match.
			f.setPolicy(t, ec2AuthzConditionStatement("ec2:"+tc.operation,
				ec2AuthzTagCondition("StringEquals", "ec2:ResourceTag/Env", "dev")))
			assert.Error(t, f.call(t, tc.operation, tc.params))
		})
	}
}

// TestEC2Authz_NamedResource_InstanceStillResolves is the regression guard on the one
// parameter that already worked.
//
// InstanceId was the sole case resourceTagsFor and buildResourceARN handled, each with
// its own EC2Instance decode. Both now go through the shared table, so the tags a
// condition is evaluated against and the ARN the decision is made about come from one
// place — but an instance must still resolve exactly as it did.
func TestEC2Authz_NamedResource_InstanceStillResolves(t *testing.T) {
	t.Parallel()
	const instanceID = "i-0abc11112222dddd3"
	instanceARN := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":instance/" + instanceID

	f := newEC2AuthzFixture(t, "instance-tagged", emulator.PolicyDocument{})
	f.put(t, "instance:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+instanceID, emulator.EC2Instance{
		InstanceID: instanceID,
		Tags:       ec2AuthzTags(map[string]string{"Env": "dev"}),
	})

	f.setPolicy(t, ec2AuthzConditionStatement("ec2:TerminateInstances",
		ec2AuthzTagCondition("StringEquals", "aws:ResourceTag/Env", "dev")))
	assert.NoError(t, f.call(t, "TerminateInstances", map[string]string{"InstanceId.1": instanceID}))

	f.setPolicy(t, emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{"ec2:TerminateInstances"},
		Resource: emulator.StringOrSlice{instanceARN},
	})
	assert.NoError(t, f.call(t, "TerminateInstances", map[string]string{"InstanceId.1": instanceID}))
}
