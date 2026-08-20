package emulator_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The authorization decision for ec2:CreateTags and ec2:DeleteTags (#674).
//
// Both operations name their resources in ResourceId.N — up to 1000 of them, of
// mixed types — and substrate decided every one of them against a single ARN built
// from InstanceId.1, which neither operation carries. So every tagging call was
// decided against the literal string "*", with the same two-directional failure
// #662 documented for RunInstances: an ARN-scoped Deny never matched and a
// least-privilege Allow naming the ARNs denied every call.
//
// These tests use the #662 fixture, and they lean on one property of it: an
// action-only Deny refuses whatever the decision resolves, and the denial message
// names the first resource the policy does not allow — so the message is how a test
// reads back which ARNs were built and in what order.

const (
	ec2TagAuthzInstance = "i-0aaa1111bbbb2222c"
	ec2TagAuthzVolume   = "vol-0ddd3333eeee4444f"
	ec2TagAuthzVPC      = "vpc-0eee5555ffff6666a"
	ec2TagAuthzIGW      = "igw-0fff7777aaaa8888b"
	ec2TagAuthzRTB      = "rtb-0aaa9999bbbb0000c"
	ec2TagAuthzEIP      = "eipalloc-0bbb1111cccc2222d"
	ec2TagAuthzNAT      = "nat-0ccc3333dddd4444e"
)

// ec2TagARN builds the ARN a tagging call is authorized against.
//
// Written out here rather than derived from the state key on purpose: for five of
// the nine taggable prefixes substrate's state key abbreviates the resource type
// the Service Authorization Reference's ARN uses, and a test that derived one from
// the other would agree with a wrong answer.
func ec2TagARN(arnType, id string) string {
	return "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":" + arnType + "/" + id
}

// ec2TagAuthzResource is one taggable resource: the ID a caller passes as
// ResourceId.N, the ARN the decision must build for it, and the record that has to
// exist for its tags to be readable.
type ec2TagAuthzResource struct {
	id  string
	arn string
	put func(*ec2AuthzFixture, *testing.T, map[string]string)
}

// ec2TagAuthzAllTypes is every resource type substrate can tag, in the order these
// tests name them in ResourceId.N — which is the order the decision evaluates.
//
// All nine, not a sample: the ARN resource type is a hand-written string per type,
// so a wrong one is only caught by a case that names that type.
func ec2TagAuthzAllTypes() []ec2TagAuthzResource {
	return []ec2TagAuthzResource{
		{ec2TagAuthzInstance, ec2TagARN("instance", ec2TagAuthzInstance), (*ec2AuthzFixture).putTagInstance},
		{ec2AuthzSubnet, ec2TagARN("subnet", ec2AuthzSubnet), func(f *ec2AuthzFixture, t *testing.T, tags map[string]string) {
			f.putSubnet(t, ec2AuthzSubnet, tags)
		}},
		{ec2AuthzSG, ec2TagARN("security-group", ec2AuthzSG), func(f *ec2AuthzFixture, t *testing.T, tags map[string]string) {
			f.putSecurityGroup(t, ec2AuthzSG, tags)
		}},
		{ec2TagAuthzVolume, ec2TagARN("volume", ec2TagAuthzVolume), (*ec2AuthzFixture).putTagVolume},
		{ec2TagAuthzVPC, ec2TagARN("vpc", ec2TagAuthzVPC), (*ec2AuthzFixture).putTagVPC},
		{ec2TagAuthzIGW, ec2TagARN("internet-gateway", ec2TagAuthzIGW), (*ec2AuthzFixture).putTagInternetGateway},
		{ec2TagAuthzRTB, ec2TagARN("route-table", ec2TagAuthzRTB), (*ec2AuthzFixture).putTagRouteTable},
		{ec2TagAuthzEIP, ec2TagARN("elastic-ip", ec2TagAuthzEIP), (*ec2AuthzFixture).putTagElasticIP},
		{ec2TagAuthzNAT, ec2TagARN("natgateway", ec2TagAuthzNAT), (*ec2AuthzFixture).putTagNATGateway},
	}
}

// The records for the types the #662 fixture had no reason to write. Each uses the
// real state key and the real struct, so a test proves the tags the decision reads
// are the ones the resource actually stores under its "tags" member.
func (f *ec2AuthzFixture) putTagInstance(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "instance:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzInstance,
		emulator.EC2Instance{InstanceID: ec2TagAuthzInstance, Tags: ec2AuthzTags(tags)})
}

func (f *ec2AuthzFixture) putTagVolume(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "volume:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzVolume,
		emulator.EC2Volume{VolumeID: ec2TagAuthzVolume, Tags: ec2AuthzTags(tags)})
}

func (f *ec2AuthzFixture) putTagVPC(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "vpc:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzVPC,
		emulator.EC2VPC{VPCID: ec2TagAuthzVPC, Tags: ec2AuthzTags(tags)})
}

func (f *ec2AuthzFixture) putTagInternetGateway(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "igw:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzIGW,
		emulator.EC2InternetGateway{InternetGatewayID: ec2TagAuthzIGW, Tags: ec2AuthzTags(tags)})
}

func (f *ec2AuthzFixture) putTagRouteTable(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "rtb:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzRTB,
		emulator.EC2RouteTable{RouteTableID: ec2TagAuthzRTB, Tags: ec2AuthzTags(tags)})
}

func (f *ec2AuthzFixture) putTagElasticIP(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "eip:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzEIP,
		emulator.EC2ElasticIP{AllocationID: ec2TagAuthzEIP, Tags: ec2AuthzTags(tags)})
}

func (f *ec2AuthzFixture) putTagNATGateway(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "nat:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2TagAuthzNAT,
		emulator.EC2NATGateway{NatGatewayID: ec2TagAuthzNAT, Tags: ec2AuthzTags(tags)})
}

// ec2TagAuthzFixture is the #662 fixture with every taggable resource present and
// untagged, which is the state a tagging call is decided against.
func ec2TagAuthzFixture(t *testing.T, user string) *ec2AuthzFixture {
	t.Helper()
	f := newEC2AuthzFixture(t, user, emulator.PolicyDocument{})
	for _, res := range ec2TagAuthzAllTypes() {
		res.put(f, t, nil)
	}
	return f
}

// ec2TagAuthzParams names every taggable resource in one request, tagging them all
// with one key — the shape a consumer's "tag everything this stack created" step
// takes.
func ec2TagAuthzParams() map[string]string {
	params := map[string]string{"Tag.1.Key": "Env", "Tag.1.Value": "test"}
	for i, res := range ec2TagAuthzAllTypes() {
		params["ResourceId."+strconv.Itoa(i+1)] = res.id
	}
	return params
}

// ec2TagAuthzStatement builds one statement over a tagging action.
func ec2TagAuthzStatement(effect, action string, resources ...string) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:   effect,
		Action:   emulator.StringOrSlice{action},
		Resource: emulator.StringOrSlice(resources),
	}
}

// TestEC2_TagAuthz_ScopedDenyOnAnyNamedResourceBlocks is the decisive test: the
// false allow #674 reports, once per resource type.
//
// A policy allowing ec2:CreateTags broadly while denying it on one resource is the
// guardrail shape — "tag anything except the shared VPC". It had no effect at all,
// because the call resolved to the resource "*" and resourceMatches asks globMatch
// whether the *statement's* ARN matches that "*", which it does not.
//
// Each subtest denies exactly one of the nine resources the request names, so it
// fails unless the decision both reaches that resource and spells its ARN the way
// the Service Authorization Reference does. Five of the nine are the interesting
// ones — sg, igw, rtb, eip and nat abbreviate in the state key and do not in the
// ARN — and a Deny on security-group/sg-… matching nothing is exactly what a
// string-munged ARN would produce.
func TestEC2_TagAuthz_ScopedDenyOnAnyNamedResourceBlocks(t *testing.T) {
	for _, res := range ec2TagAuthzAllTypes() {
		t.Run(res.arn, func(t *testing.T) {
			f := ec2TagAuthzFixture(t, "iris")
			f.setPolicy(t,
				ec2TagAuthzStatement("Allow", "ec2:CreateTags", "*"),
				ec2TagAuthzStatement("Deny", "ec2:CreateTags", res.arn),
			)
			err := f.call(t, "CreateTags", ec2TagAuthzParams())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("a Deny naming %s did not block a CreateTags that names it", res.arn)
			}
			assert.Equal(t, res.arn, deniedResource(t, err),
				"the denial names the resource the Deny scoped to")
		})
	}
}

// TestEC2_TagAuthz_LeastPrivilegePolicyAllows is the other direction the single-ARN
// resolution broke, and the one a consumer hits while writing a policy rather than
// while testing one: a policy naming exactly the ARNs the call touches refused it.
func TestEC2_TagAuthz_LeastPrivilegePolicyAllows(t *testing.T) {
	all := ec2TagAuthzAllTypes()
	arns := make([]string, 0, len(all))
	for _, res := range all {
		arns = append(arns, res.arn)
	}

	t.Run("naming every resource allows", func(t *testing.T) {
		f := ec2TagAuthzFixture(t, "jonas")
		f.setPolicy(t, ec2TagAuthzStatement("Allow", "ec2:CreateTags", arns...))
		require.NoError(t, f.call(t, "CreateTags", ec2TagAuthzParams()))
	})

	// Dropping one ARN refuses the call and says which one, which is what makes a
	// least-privilege tagging policy writable by iteration.
	for i, omitted := range all {
		t.Run("omitting "+omitted.arn+" denies", func(t *testing.T) {
			f := ec2TagAuthzFixture(t, "jonas")
			kept := make([]string, 0, len(arns)-1)
			for j, arn := range arns {
				if j != i {
					kept = append(kept, arn)
				}
			}
			f.setPolicy(t, ec2TagAuthzStatement("Allow", "ec2:CreateTags", kept...))
			err := f.call(t, "CreateTags", ec2TagAuthzParams())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("a policy omitting %s allowed a CreateTags that names it", omitted.arn)
			}
			assert.Equal(t, omitted.arn, deniedResource(t, err))
		})
	}
}

// TestEC2_TagAuthz_DeleteTagsResolvesTheSameWay pins the second operation.
//
// DeleteTags names its resources identically and has the same (empty) set of
// required resource types, so it resolves identically. The two differ only in
// condition-key surface — DeleteTags carries no ec2:CreateAction — which substrate
// does not model either way.
func TestEC2_TagAuthz_DeleteTagsResolvesTheSameWay(t *testing.T) {
	f := ec2TagAuthzFixture(t, "kira")
	sgARN := ec2TagARN("security-group", ec2AuthzSG)
	f.setPolicy(t,
		ec2TagAuthzStatement("Allow", "ec2:DeleteTags", "*"),
		ec2TagAuthzStatement("Deny", "ec2:DeleteTags", sgARN),
	)
	// DeleteTags names keys and treats the value as optional, so this request carries
	// Tag.1.Key alone — the form that has to resolve too.
	params := ec2TagAuthzParams()
	delete(params, "Tag.1.Value")
	err := f.call(t, "DeleteTags", params)
	if !ec2AuthzDenied(t, err) {
		t.Fatalf("a Deny naming %s did not block a DeleteTags that names it", sgARN)
	}
	assert.Equal(t, sgARN, deniedResource(t, err))
}

// TestEC2_TagAuthz_UnrecognizedIDIsSkipped pins that an ID whose prefix names no
// taggable resource type is left out of the list rather than denied or widened.
//
// Denying would answer with a code AWS does not use for the condition: an
// unparseable tagging ID earns InvalidID — "The specified ID for the resource you
// are trying to tag is not valid" — and substrate's handler treats it as a no-op.
// Widening to "*" would hand a caller the resource that matches a broad Allow,
// which is the direction a privilege boundary must never fail in.
func TestEC2_TagAuthz_UnrecognizedIDIsSkipped(t *testing.T) {
	t.Run("a request naming only unknown IDs falls back unchanged", func(t *testing.T) {
		f := ec2TagAuthzFixture(t, "liam")
		params := map[string]string{
			"ResourceId.1": "tgw-0abc1111", // a transit gateway: real AWS tags it, substrate does not model it
			"ResourceId.2": "not-an-id",
			"Tag.1.Key":    "Env",
			"Tag.1.Value":  "test",
		}
		// Nothing resolved, so the single-resource path decides it exactly as before:
		// the ec2 arm of buildResourceARN with no InstanceId.1, which is "*".
		f.setPolicy(t, emulator.PolicyStatement{
			Effect:   "Deny",
			Action:   emulator.StringOrSlice{"ec2:CreateTags"},
			Resource: emulator.StringOrSlice{"*"},
		})
		err := f.call(t, "CreateTags", params)
		require.Error(t, err)
		assert.Equal(t, "*", deniedResource(t, err))
	})

	t.Run("a known ID beside an unknown one still resolves", func(t *testing.T) {
		f := ec2TagAuthzFixture(t, "liam")
		vpcARN := ec2TagARN("vpc", ec2TagAuthzVPC)
		// The policy names the VPC and nothing else. If the unknown ID widened the list
		// to "*" this would be denied; if it were skipped from the *request* rather than
		// from the list, there would be nothing left to allow.
		f.setPolicy(t, ec2TagAuthzStatement("Allow", "ec2:CreateTags", vpcARN))
		require.NoError(t, f.call(t, "CreateTags", map[string]string{
			"ResourceId.1": "tgw-0abc1111",
			"ResourceId.2": ec2TagAuthzVPC,
			"Tag.1.Key":    "Env",
			"Tag.1.Value":  "test",
		}))
	})
}

// TestEC2_TagAuthz_ResourceTagsPairWithTheirOwnResource is why each resolved
// resource carries its own tags rather than one merged map: a condition is written
// about the resource being evaluated, so a tag on the VPC must not satisfy a
// condition checked against the security group.
//
// This is the tag-based access control AWS's own tagging examples are written
// around, and it is only reachable now that the ARNs resolve — before this, the
// resource was "*" and its tags were the instance's, read from an InstanceId.1
// neither operation sends.
func TestEC2_TagAuthz_ResourceTagsPairWithTheirOwnResource(t *testing.T) {
	const good = "test"
	const bad = "prod"
	abac := func(f *ec2AuthzFixture, t *testing.T) {
		t.Helper()
		f.setPolicy(t, emulator.PolicyStatement{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"ec2:CreateTags"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"StringEquals": {"aws:ResourceTag/Env": {good}},
			},
		})
	}

	t.Run("every resource tagged is allowed", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "mira", emulator.PolicyDocument{})
		for _, res := range ec2TagAuthzAllTypes() {
			res.put(f, t, map[string]string{"Env": good})
		}
		abac(f, t)
		require.NoError(t, f.call(t, "CreateTags", ec2TagAuthzParams()))
	})

	for _, mistagged := range ec2TagAuthzAllTypes() {
		t.Run("mistagging "+mistagged.arn+" denies", func(t *testing.T) {
			f := newEC2AuthzFixture(t, "mira", emulator.PolicyDocument{})
			for _, res := range ec2TagAuthzAllTypes() {
				value := good
				if res.id == mistagged.id {
					value = bad
				}
				res.put(f, t, map[string]string{"Env": value})
			}
			abac(f, t)
			err := f.call(t, "CreateTags", ec2TagAuthzParams())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("%s tagged Env=%s satisfied a condition requiring Env=%s",
					mistagged.arn, bad, good)
			}
			assert.Equal(t, mistagged.arn, deniedResource(t, err))
		})
	}
}

// TestEC2_TagAuthz_RequestTagConditionEvaluates covers the half of #674 that had to
// land with the resource resolution rather than after it.
//
// addRequestTags read only TagSpecification.N.Tag.M.Key/Value, and a direct
// CreateTags sends Tag.N.Key/Tag.N.Value — so aws:RequestTag/* was empty for every
// tagging call. On its own that was inert, because the statement never matched the
// resource anyway. Resolving the ARNs without this would have converted the false
// allow into a false deny: AWS's documented "may only tag with these keys" policies
// would finally match the resource and then fail on the absent condition key.
func TestEC2_TagAuthz_RequestTagConditionEvaluates(t *testing.T) {
	scoped := func(f *ec2AuthzFixture, t *testing.T) {
		t.Helper()
		f.setPolicy(t, emulator.PolicyStatement{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"ec2:CreateTags"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"StringEquals": {"aws:RequestTag/Env": {"test"}},
			},
		})
	}

	t.Run("the tag the request carries satisfies the condition", func(t *testing.T) {
		f := ec2TagAuthzFixture(t, "nadia")
		scoped(f, t)
		require.NoError(t, f.call(t, "CreateTags", map[string]string{
			"ResourceId.1": ec2TagAuthzVPC,
			"Tag.1.Key":    "Env",
			"Tag.1.Value":  "test",
		}))
	})

	t.Run("a different value denies", func(t *testing.T) {
		f := ec2TagAuthzFixture(t, "nadia")
		scoped(f, t)
		err := f.call(t, "CreateTags", map[string]string{
			"ResourceId.1": ec2TagAuthzVPC,
			"Tag.1.Key":    "Env",
			"Tag.1.Value":  "prod",
		})
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a policy requiring Env=test allowed a request tagging Env=prod")
		}
	})

	t.Run("a second tag beside it is read too", func(t *testing.T) {
		// The walk stops at the first absent index, so a request whose conditioned key
		// is not first has to be read past — Tag.1 here is unrelated.
		f := ec2TagAuthzFixture(t, "nadia")
		scoped(f, t)
		require.NoError(t, f.call(t, "CreateTags", map[string]string{
			"ResourceId.1": ec2TagAuthzVPC,
			"Tag.1.Key":    "Owner",
			"Tag.1.Value":  "platform",
			"Tag.2.Key":    "Env",
			"Tag.2.Value":  "test",
		}))
	})

	t.Run("a launch's TagSpecification form still resolves", func(t *testing.T) {
		// The pre-existing walk is untouched, so the shape RunInstances sends keeps
		// populating the same condition key (#468).
		f := ec2TagAuthzFixture(t, "nadia")
		f.setPolicy(t, emulator.PolicyStatement{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"ec2:RunInstances"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"StringEquals": {"aws:RequestTag/Env": {"test"}},
			},
		})
		params := nominalLaunch()
		params["TagSpecification.1.ResourceType"] = "instance"
		params["TagSpecification.1.Tag.1.Key"] = "Env"
		params["TagSpecification.1.Tag.1.Value"] = "test"
		require.NoError(t, f.launch(t, params))
	})
}

// TestEC2_TagAuthz_PermissionBoundaryCoversEveryResource is the mutation-motivated
// test: a boundary that refuses the *first* resource refuses the request whichever
// loop the check sits in, so the boundary here allows the first resource and
// refuses the second.
func TestEC2_TagAuthz_PermissionBoundaryCoversEveryResource(t *testing.T) {
	f := ec2TagAuthzFixture(t, "opal")
	f.setPolicy(t, ec2TagAuthzStatement("Allow", "ec2:CreateTags", "*"))

	boundaryARN := "arn:aws:iam::" + ec2AuthzAccount + ":policy/TagBoundary"
	// The instance, which the request names first, and nothing else.
	boundary := emulator.IAMPolicy{
		PolicyName:       "TagBoundary",
		PolicyID:         "ANPATAGB",
		ARN:              boundaryARN,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{
				ec2TagAuthzStatement("Allow", "ec2:CreateTags", ec2TagARN("instance", ec2TagAuthzInstance)),
			},
		},
	}
	raw, err := json.Marshal(boundary)
	require.NoError(t, err)
	require.NoError(t, f.state.Put(context.Background(), "iam", "policy:"+boundaryARN, raw))

	user := emulator.IAMUser{
		UserName:            "opal",
		UserID:              "AIDATEST",
		ARN:                 "arn:aws:iam::" + ec2AuthzAccount + ":user/opal",
		Path:                "/",
		PermissionsBoundary: &emulator.IAMAttachedPolicy{PolicyARN: boundaryARN, PolicyName: "TagBoundary"},
	}
	userRaw, err := json.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, f.state.Put(context.Background(), "iam", "user:opal", userRaw))

	callErr := f.call(t, "CreateTags", ec2TagAuthzParams())
	if !ec2AuthzDenied(t, callErr) {
		t.Fatal("a boundary allowing only the first resource did not block a call naming nine")
	}
	var awsErr *emulator.AWSError
	require.ErrorAs(t, callErr, &awsErr)
	assert.Contains(t, awsErr.Message, "permission boundary")
}
