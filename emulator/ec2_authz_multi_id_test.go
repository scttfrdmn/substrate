package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An EC2 operation naming several resources of one type is authorized against every one
// of them, not the first alone (#744).
//
// #730 taught the decision to resolve a single EC2 resource from four ID parameters, but
// kept `ids[0]`. So a `TerminateInstances` naming three instances was decided against
// `InstanceId.1`'s ARN and `InstanceId.1`'s tags, and the other two were not authorized at
// all: a policy allowing instances tagged `Env=dev` and denying the rest permitted a call
// naming one dev instance and two production ones, provided the dev one came first. That is
// the same shape #674 fixed for tagging calls, and the failure direction that matters —
// it grants.
//
// One decision here is substrate's reading and is pinned below rather than left to be
// rediscovered: an ID mid-batch that resolves to no ARN is **skipped**, so a batch of one
// resolvable and one unparseable ID is authorized as a batch of one. See
// [ec2AuthzNamedResources] for why, and note the narrowness of the case — a well-formed
// `i-` ID naming no instance still gets its own ARN, because an instance's state key is
// computed from its ID rather than looked up.

// The IDs the batch tests name. Two of each type, so "the second one is evaluated too" is
// what every table row asserts.
const (
	ec2MultiInstanceA = "i-0aaa11112222bbbb3"
	ec2MultiInstanceB = "i-0bbb22223333cccc4"
	ec2MultiInstanceC = "i-0ccc33334444dddd5"
	ec2MultiSGB       = "sg-0ddd44445555eeee6"
	ec2MultiRTBB      = "rtb-0eee55556666ffff7"
	ec2MultiIGWB      = "igw-0fff66667777aaa08"
)

// ec2MultiARN builds the ARN the decision names a resource of type arnType by.
func ec2MultiARN(arnType, id string) string {
	return "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":" + arnType + "/" + id
}

// putInstance writes one instance record, which is what gives the ID tags to be
// evaluated against. An instance with no record still resolves to an ARN — its state key
// is computed from the ID — so this is about the tags, not about the ARN.
func (f *ec2AuthzFixture) putInstance(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	f.put(t, "instance:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+id,
		emulator.EC2Instance{InstanceID: id, Tags: ec2AuthzTags(tags)})
}

// ec2MultiStatement allows one action on the given resources, which is the least-privilege
// shape a batch has to satisfy for every member.
func ec2MultiStatement(action string, resources ...string) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{action},
		Resource: emulator.StringOrSlice(resources),
	}
}

// TestEC2_Authz_EveryNamedInstanceIsAuthorized is the issue's own scenario: a
// TerminateInstances naming three instances is denied unless the policy admits all three.
//
// The denial names the first instance the policy does not allow, in request order, which
// is the resource a caller can act on — remove it from the batch and the call proceeds.
func TestEC2_Authz_EveryNamedInstanceIsAuthorized(t *testing.T) {
	f := newEC2AuthzFixture(t, "nadia", emulator.PolicyDocument{})
	params := map[string]string{
		"InstanceId.1": ec2MultiInstanceA,
		"InstanceId.2": ec2MultiInstanceB,
		"InstanceId.3": ec2MultiInstanceC,
	}
	arnA := ec2MultiARN("instance", ec2MultiInstanceA)
	arnB := ec2MultiARN("instance", ec2MultiInstanceB)
	arnC := ec2MultiARN("instance", ec2MultiInstanceC)

	// The first ID alone is what the decision used to resolve, so this policy used to
	// permit the whole batch.
	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstances", arnA))
	err := f.call(t, "TerminateInstances", params)
	require.True(t, ec2AuthzDenied(t, err), "a batch was allowed by a policy naming only its first instance")
	assert.Equal(t, arnB, deniedResource(t, err), "the denial names the first instance the policy omits")

	// Two of three is still not all of them, and the denial moves on to the third.
	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstances", arnA, arnB))
	err = f.call(t, "TerminateInstances", params)
	require.True(t, ec2AuthzDenied(t, err), "a batch was allowed by a policy naming two of its three instances")
	assert.Equal(t, arnC, deniedResource(t, err))

	// All three: allowed.
	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstances", arnA, arnB, arnC))
	require.NoError(t, f.call(t, "TerminateInstances", params))
}

// TestEC2_Authz_ScopedDenyReachesEveryNamedInstance is the guardrail direction, which is
// the one a consumer writes: a broad Allow beside a Deny scoped to one instance.
//
// It was inert for every instance but the first, so a Deny fencing off a production
// instance did not stop a batch that put a dev instance in front of it.
func TestEC2_Authz_ScopedDenyReachesEveryNamedInstance(t *testing.T) {
	f := newEC2AuthzFixture(t, "omar", emulator.PolicyDocument{})
	f.setPolicy(t,
		ec2MultiStatement("ec2:StopInstances", "*"),
		emulator.PolicyStatement{
			Effect:   "Deny",
			Action:   emulator.StringOrSlice{"ec2:StopInstances"},
			Resource: emulator.StringOrSlice{ec2MultiARN("instance", ec2MultiInstanceC)},
		},
	)

	require.NoError(t, f.call(t, "StopInstances", map[string]string{"InstanceId.1": ec2MultiInstanceA}),
		"an instance the Deny does not name is still allowed")

	err := f.call(t, "StopInstances", map[string]string{
		"InstanceId.1": ec2MultiInstanceA,
		"InstanceId.2": ec2MultiInstanceC,
	})
	require.True(t, ec2AuthzDenied(t, err), "a Deny on the second instance of a batch was inert")
	assert.Equal(t, ec2MultiARN("instance", ec2MultiInstanceC), deniedResource(t, err))
}

// TestEC2_Authz_EachInstanceIsJudgedByItsOwnTags is why every resource carries its own
// tags rather than one merged map: the tag condition the issue describes.
//
// A policy allowing ec2:TerminateInstances on instances tagged Env=dev must refuse a batch
// that also names a production one. Merging the two tag sets would let the dev tag satisfy
// the condition about the production instance, which is the false allow
// orgAuthzResourceID's comment exists to prevent — so this asserts the denial names the
// production instance specifically, not merely that something was denied.
func TestEC2_Authz_EachInstanceIsJudgedByItsOwnTags(t *testing.T) {
	f := newEC2AuthzFixture(t, "priya", emulator.PolicyDocument{})
	f.putInstance(t, ec2MultiInstanceA, map[string]string{"Env": "dev"})
	f.putInstance(t, ec2MultiInstanceB, map[string]string{"Env": "prod"})

	for _, prefix := range []string{"aws:ResourceTag/", "ec2:ResourceTag/"} {
		t.Run(prefix, func(t *testing.T) {
			f.setPolicy(t, ec2AuthzConditionStatement("ec2:TerminateInstances",
				ec2AuthzTagCondition("StringEquals", prefix+"Env", "dev")))

			require.NoError(t, f.call(t, "TerminateInstances",
				map[string]string{"InstanceId.1": ec2MultiInstanceA}),
				"the dev instance alone satisfies the condition")

			err := f.call(t, "TerminateInstances", map[string]string{
				"InstanceId.1": ec2MultiInstanceA,
				"InstanceId.2": ec2MultiInstanceB,
			})
			require.True(t, ec2AuthzDenied(t, err),
				"a production instance behind a dev one was terminated under an Env=dev policy")
			assert.Equal(t, ec2MultiARN("instance", ec2MultiInstanceB), deniedResource(t, err),
				"the denial names the instance whose own tags failed the condition")
		})
	}
}

// TestEC2_Authz_AllFourIDParametersExpand pins that the expansion covers every parameter in
// ec2AuthzIDParams, not only InstanceId — each of the other three is in that list because a
// bundled AWS policy conditions a delete on the tags of the resource it names (#730), and a
// delete of several is the same batch.
//
// GroupId's placement-group row is the overload worth naming: DescribePlacementGroups reads
// pg- IDs through the same parameter, and nothing mis-resolves because ec2TaggableResource
// keys on the ID's prefix.
func TestEC2_Authz_AllFourIDParametersExpand(t *testing.T) {
	const pgName, pgID = "cluster-a", "pg-0aab11112222bbbb3"
	tests := []struct {
		name      string
		operation string
		param     string
		first     string
		second    string
		firstARN  string
		secondARN string
		seed      func(f *ec2AuthzFixture, t *testing.T)
	}{
		{
			name:      "InstanceId",
			operation: "TerminateInstances",
			param:     "InstanceId",
			first:     ec2MultiInstanceA, second: ec2MultiInstanceB,
			firstARN:  ec2MultiARN("instance", ec2MultiInstanceA),
			secondARN: ec2MultiARN("instance", ec2MultiInstanceB),
		},
		{
			name:      "GroupId",
			operation: "DeleteSecurityGroup",
			param:     "GroupId",
			first:     ec2AuthzSG, second: ec2MultiSGB,
			firstARN:  ec2AuthzSGARN,
			secondARN: ec2MultiARN("security-group", ec2MultiSGB),
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putSecurityGroup(t, ec2MultiSGB, nil)
			},
		},
		{
			name:      "RouteTableId",
			operation: "DeleteRouteTable",
			param:     "RouteTableId",
			first:     ec2AuthzRouteTable, second: ec2MultiRTBB,
			firstARN:  ec2AuthzRouteTableARN,
			secondARN: ec2MultiARN("route-table", ec2MultiRTBB),
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putRouteTable(t, ec2AuthzRouteTable, nil)
				f.putRouteTable(t, ec2MultiRTBB, nil)
			},
		},
		{
			name:      "InternetGatewayId",
			operation: "DeleteInternetGateway",
			param:     "InternetGatewayId",
			first:     ec2AuthzIGW, second: ec2MultiIGWB,
			firstARN:  ec2AuthzIGWARN,
			secondARN: ec2MultiARN("internet-gateway", ec2MultiIGWB),
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.putInternetGateway(t, ec2AuthzIGW, nil)
				f.putInternetGateway(t, ec2MultiIGWB, nil)
			},
		},
		{
			// The placement group ARN is by *name*, so this also proves the expansion
			// carries each resource's own translation rather than echoing the request.
			name:      "GroupId naming placement groups",
			operation: "DescribePlacementGroups",
			param:     "GroupId",
			first:     pgID, second: ec2MultiSGB,
			firstARN:  ec2MultiARN("placement-group", pgName),
			secondARN: ec2MultiARN("security-group", ec2MultiSGB),
			seed: func(f *ec2AuthzFixture, t *testing.T) {
				f.put(t, "placement_group:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+pgName,
					emulator.EC2PlacementGroup{GroupName: pgName, GroupID: pgID})
				f.putSecurityGroup(t, ec2MultiSGB, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "quinn", emulator.PolicyDocument{})
			if tt.seed != nil {
				tt.seed(f, t)
			}
			params := map[string]string{
				tt.param + ".1": tt.first,
				tt.param + ".2": tt.second,
			}
			action := "ec2:" + tt.operation

			f.setPolicy(t, ec2MultiStatement(action, tt.firstARN))
			err := f.call(t, tt.operation, params)
			require.True(t, ec2AuthzDenied(t, err), "the second %s was not authorized", tt.param)
			assert.Equal(t, tt.secondARN, deniedResource(t, err))

			f.setPolicy(t, ec2MultiStatement(action, tt.firstARN, tt.secondARN))
			require.NoError(t, f.call(t, tt.operation, params))
		})
	}
}

// TestEC2_Authz_UnindexedAndIndexedIDsAreTheSameRequest pins that the un-indexed spelling
// still resolves, which is what extractIndexedParams gives and what the handlers accept.
func TestEC2_Authz_UnindexedAndIndexedIDsAreTheSameRequest(t *testing.T) {
	f := newEC2AuthzFixture(t, "ravi", emulator.PolicyDocument{})
	arnA := ec2MultiARN("instance", ec2MultiInstanceA)
	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstances", arnA))

	require.NoError(t, f.call(t, "TerminateInstances", map[string]string{"InstanceId": ec2MultiInstanceA}))

	err := f.call(t, "TerminateInstances", map[string]string{"InstanceId": ec2MultiInstanceB})
	require.True(t, ec2AuthzDenied(t, err))
	assert.Equal(t, ec2MultiARN("instance", ec2MultiInstanceB), deniedResource(t, err))
}

// TestEC2_Authz_UnresolvableIDInABatchIsSkipped pins the decision the issue asks to be
// decided rather than assumed.
//
// An ID that resolves to no ARN is skipped, as [ec2AuthzTagResources] skips one, so a batch
// of one resolvable and one unparseable ID is authorized as a batch of one. The refusal the
// caller is owed is the handler's — AWS answers a malformed instance ID with
// InvalidInstanceID.Malformed, not AccessDenied — and TerminateInstances is documented
// all-or-nothing, so the bogus ID fails the whole call regardless of what the policy said.
//
// The case is narrower than it sounds, and the second half here is why: a well-formed i- ID
// naming no instance is *not* unresolvable. Its state key is computed from the ID, so it
// gets its own ARN with an empty tag set, and a least-privilege policy still has to name it.
func TestEC2_Authz_UnresolvableIDInABatchIsSkipped(t *testing.T) {
	f := newEC2AuthzFixture(t, "sona", emulator.PolicyDocument{})
	arnA := ec2MultiARN("instance", ec2MultiInstanceA)
	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstances", arnA))

	require.NoError(t, f.call(t, "TerminateInstances", map[string]string{
		"InstanceId.1": ec2MultiInstanceA,
		// No prefix any taggable type answers to, so there is no ARN to decide about.
		"InstanceId.2": "not-an-instance-id",
	}), "an unparseable ID beside an allowed one is skipped, not denied")

	// And it is not laundering the batch: an absent-but-well-formed ID is still its own
	// resource, so the same policy refuses it.
	err := f.call(t, "TerminateInstances", map[string]string{
		"InstanceId.1": ec2MultiInstanceA,
		"InstanceId.2": ec2MultiInstanceB,
	})
	require.True(t, ec2AuthzDenied(t, err), "an instance with no record was not authorized")
	assert.Equal(t, ec2MultiARN("instance", ec2MultiInstanceB), deniedResource(t, err))

	// A request naming only unresolvable IDs falls back to the wildcard it used before,
	// which can only narrow a grant: "*" as the request resource matches a statement whose
	// own Resource starts with "*".
	f.setPolicy(t, emulator.PolicyStatement{
		Effect:   "Deny",
		Action:   emulator.StringOrSlice{"ec2:TerminateInstances"},
		Resource: emulator.StringOrSlice{"*"},
	})
	err = f.call(t, "TerminateInstances", map[string]string{"InstanceId.1": "not-an-instance-id"})
	require.True(t, ec2AuthzDenied(t, err))
	assert.Equal(t, "*", deniedResource(t, err))
}

// TestEC2_Authz_BatchDenialIsReplayStable pins the ordering guarantee: the same request
// against the same state names the same resource every time.
//
// The decision walks a fixed parameter order and then the request's own index order, so
// nothing here depends on Go's map iteration — which is what makes the denial message
// replayable, and it is the message a consumer's test asserts on.
func TestEC2_Authz_BatchDenialIsReplayStable(t *testing.T) {
	f := newEC2AuthzFixture(t, "tarek", emulator.PolicyDocument{})
	params := map[string]string{
		"InstanceId.1": ec2MultiInstanceA,
		"InstanceId.2": ec2MultiInstanceB,
		"InstanceId.3": ec2MultiInstanceC,
	}
	// Nothing is allowed, so every resource in the batch would deny — which is what makes
	// this an ordering assertion rather than a matching one.
	f.setPolicy(t, ec2MultiStatement("ec2:DescribeInstances", "arn:aws:ec2:*:*:instance/none"))

	want := ec2MultiARN("instance", ec2MultiInstanceA)
	for i := 0; i < 25; i++ {
		err := f.call(t, "DescribeInstances", params)
		require.True(t, ec2AuthzDenied(t, err))
		require.Equal(t, want, deniedResource(t, err), "run %d named a different resource", i)
	}
}

// TestEC2_Authz_BatchIsCheckedAgainstThePermissionBoundary pins that the boundary sees every
// resource in a batch too. It is loaded once outside the loop, so a boundary allowing only
// the first instance must still refuse the second.
func TestEC2_Authz_BatchIsCheckedAgainstThePermissionBoundary(t *testing.T) {
	f := newEC2AuthzFixture(t, "ulla", emulator.PolicyDocument{})
	arnA := ec2MultiARN("instance", ec2MultiInstanceA)
	arnB := ec2MultiARN("instance", ec2MultiInstanceB)
	params := map[string]string{"InstanceId.1": ec2MultiInstanceA, "InstanceId.2": ec2MultiInstanceB}

	f.setPolicy(t, ec2MultiStatement("ec2:TerminateInstances", arnA, arnB))
	require.NoError(t, f.call(t, "TerminateInstances", params), "the identity policy allows both")

	f.setBoundary(t, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []emulator.PolicyStatement{ec2MultiStatement("ec2:TerminateInstances", arnA)},
	})
	err := f.call(t, "TerminateInstances", params)
	require.True(t, ec2AuthzDenied(t, err), "a boundary naming only the first instance allowed the batch")

	f.setBoundary(t, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []emulator.PolicyStatement{ec2MultiStatement("ec2:TerminateInstances", arnA, arnB)},
	})
	require.NoError(t, f.call(t, "TerminateInstances", params))
}
