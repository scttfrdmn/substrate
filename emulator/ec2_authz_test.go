package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The authorization decision for ec2:RunInstances (#662).
//
// RunInstances names five required resource types — image*, instance*,
// network-interface*, security-group* and subnet* — and was decided against a
// single ARN built from InstanceId.1, a parameter a launch never carries. Every
// launch therefore resolved to the literal string "*".
//
// These tests pin both directions that broke, because "*" as the *request*
// resource is not a wildcard: resourceMatches passes the statement's Resource
// entry to globMatch as the pattern, so "*" matched only a statement whose own
// Resource begins with "*". An ARN-scoped Deny never matched and was silently
// inert; a least-privilege Allow naming the five ARNs never matched either and
// denied every launch.

const (
	ec2AuthzAccount = "123456789012"
	ec2AuthzRegion  = "us-east-1"
	ec2AuthzSubnet  = "subnet-0aaa11112222bbbb3"
	ec2AuthzSG      = "sg-0ccc44445555dddd6"
)

// ec2AuthzAMI is the AMI the nominal launch names. It is a bundled image rather than a
// fabricated literal so the fixture names an AMI a real RunInstances would also accept
// (#733) — these tests reach only the authorization decision, but a reader copying the
// fixture into a launch should not be handed an ID substrate refuses.
var ec2AuthzAMI = ec2TestImage

// ec2AuthzFixture is an EC2 state store plus an AuthController over the same
// store, which is what makes a resource-tag condition testable: the tag has to
// travel from the EC2 namespace into the condition context under the ARN it
// belongs to.
type ec2AuthzFixture struct {
	state emulator.StateManager
	auth  *emulator.AuthController
	user  string
}

// the five ARNs a launch naming an AMI, a subnet and one security group is
// authorized against, in the order the decision builds them.
var (
	ec2AuthzImageARN  = "arn:aws:ec2:" + ec2AuthzRegion + "::image/" + ec2AuthzAMI
	ec2AuthzSubnetARN = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":subnet/" + ec2AuthzSubnet
	ec2AuthzSGARN     = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":security-group/" + ec2AuthzSG
	ec2AuthzENIARN    = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":network-interface/*"
	ec2AuthzInstARN   = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":instance/*"

	// The two default-VPC resources a launch that names no subnet is about to create,
	// which is why they are wildcards rather than IDs (#673).
	ec2AuthzSubnetWildcardARN = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":subnet/*"
	ec2AuthzSGWildcardARN     = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":security-group/*"
)

// ec2AuthzAllFive is every ARN a launch requires, which is what a least-privilege
// RunInstances policy has to name.
func ec2AuthzAllFive() []string {
	return []string{ec2AuthzImageARN, ec2AuthzSubnetARN, ec2AuthzSGARN, ec2AuthzENIARN, ec2AuthzInstARN}
}

// newEC2AuthzFixture builds a user carrying one policy plus the AMI, subnet and
// security group a launch names, each tagged as given.
func newEC2AuthzFixture(t *testing.T, user string, doc emulator.PolicyDocument) *ec2AuthzFixture {
	t.Helper()
	policyARN := "arn:aws:iam::" + ec2AuthzAccount + ":policy/RunInstances-" + user
	state := newAuthTestState(t, user, policyARN, doc)
	f := &ec2AuthzFixture{
		state: state,
		auth:  emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false)),
		user:  user,
	}
	f.putImage(t, ec2AuthzAMI, nil)
	f.putSubnet(t, ec2AuthzSubnet, nil)
	f.putSecurityGroup(t, ec2AuthzSG, nil)
	return f
}

// put writes one EC2 record under key. Records are written directly rather than
// through the plugin: the decision reads state, and a plugin call would drag a
// launch's own authorization into the fixture for it.
func (f *ec2AuthzFixture) put(t *testing.T, key string, record any) {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	if err := f.state.Put(context.Background(), "ec2", key, raw); err != nil { //nolint:contextcheck
		t.Fatalf("store %s: %v", key, err)
	}
}

func (f *ec2AuthzFixture) putImage(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	f.put(t, "image:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+id, emulator.EC2Image{
		ImageID:   id,
		AccountID: ec2AuthzAccount,
		Region:    ec2AuthzRegion,
		Tags:      ec2AuthzTags(tags),
	})
}

func (f *ec2AuthzFixture) putSubnet(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	f.put(t, "subnet:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+id, emulator.EC2Subnet{
		SubnetID: id,
		Tags:     ec2AuthzTags(tags),
	})
}

func (f *ec2AuthzFixture) putSecurityGroup(t *testing.T, id string, tags map[string]string) {
	t.Helper()
	f.put(t, "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+id, emulator.EC2SecurityGroup{
		GroupID: id,
		Tags:    ec2AuthzTags(tags),
	})
}

// ec2AuthzTags converts a map to the []EC2Tag shape the records store, in no
// particular order — a condition context is a map, so order cannot matter.
func ec2AuthzTags(tags map[string]string) []emulator.EC2Tag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]emulator.EC2Tag, 0, len(tags))
	for k, v := range tags {
		out = append(out, emulator.EC2Tag{Key: k, Value: v})
	}
	return out
}

// launch runs one RunInstances request through the authorization decision.
//
// Params, not Body: EC2 is a query-protocol service, so every resource a launch
// names arrives as a form parameter.
func (f *ec2AuthzFixture) launch(t *testing.T, params map[string]string) error {
	t.Helper()
	return f.call(t, "RunInstances", params)
}

// call runs one EC2 request through the authorization decision.
func (f *ec2AuthzFixture) call(t *testing.T, operation string, params map[string]string) error {
	t.Helper()
	reqCtx := newAuthTestReqCtx("arn:aws:iam::" + ec2AuthzAccount + ":user/" + f.user)
	return f.auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service:   "ec2",
		Operation: operation,
		Path:      "/",
		Params:    params,
	})
}

// nominalLaunch is a launch naming an AMI, a subnet and one security group — the
// shape a consumer's RunInstances actually takes, and the one whose five ARNs the
// tests below add to and subtract from.
func nominalLaunch() map[string]string {
	return map[string]string{
		"ImageId":           ec2AuthzAMI,
		"InstanceType":      "t3.micro",
		"MinCount":          "1",
		"MaxCount":          "1",
		"SubnetId":          ec2AuthzSubnet,
		"SecurityGroupId.1": ec2AuthzSG,
	}
}

// setPolicy rewrites the user's single attached policy so its statements are
// exactly those given, which is how a test scopes a decision to a list of ARNs
// rather than to "*".
func (f *ec2AuthzFixture) setPolicy(t *testing.T, statements ...emulator.PolicyStatement) {
	t.Helper()
	arn := "arn:aws:iam::" + ec2AuthzAccount + ":policy/RunInstances-" + f.user
	pol := emulator.IAMPolicy{
		PolicyName:       "runinstances",
		PolicyID:         "ANPARUN",
		ARN:              arn,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         emulator.PolicyDocument{Version: "2012-10-17", Statement: statements},
	}
	raw, err := json.Marshal(pol)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	if err := f.state.Put(context.Background(), "iam", emulator.IAMPolicyKeyForTest(arn), raw); err != nil { //nolint:contextcheck
		t.Fatalf("store policy: %v", err)
	}
}

// ec2AuthzStatement builds one statement over the RunInstances action.
func ec2AuthzStatement(effect string, resources ...string) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:   effect,
		Action:   emulator.StringOrSlice{"ec2:RunInstances"},
		Resource: emulator.StringOrSlice(resources),
	}
}

// ec2AuthzDenied reports whether err is the denial an EC2 caller sees.
//
// EC2 speaks the query protocol with its own XML error shape, so the code is the
// bare "AccessDenied" — not the "Exception"-suffixed spelling the JSON-RPC
// services use. An SDK dispatches on that string, so it is the load-bearing half.
func ec2AuthzDenied(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	var awsErr *emulator.AWSError
	if !errors.As(err, &awsErr) {
		t.Fatalf("expected an *AWSError, got %T: %v", err, err)
	}
	if awsErr.Code != "AccessDenied" {
		t.Errorf("expected AccessDenied, got %q", awsErr.Code)
	}
	if awsErr.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected 403, got %d", awsErr.HTTPStatus)
	}
	return true
}

// TestEC2_Authz_RunInstancesScopedDenyBlocks is the decisive test: the false
// allow #662 reports.
//
// A policy that allows ec2:RunInstances on "*" while denying it on one subnet is
// the standard guardrail shape — "launch anywhere except the private subnet". It
// had no effect at all. The launch resolved to the resource "*", the Deny's
// Resource named a subnet ARN, and globMatch was asked whether the pattern
// "arn:…:subnet/subnet-private" matches the value "*" — it does not, so the Deny
// never applied and the guardrail was silently inert.
func TestEC2_Authz_RunInstancesScopedDenyBlocks(t *testing.T) {
	f := newEC2AuthzFixture(t, "ivan", emulator.PolicyDocument{})
	f.setPolicy(t,
		ec2AuthzStatement("Allow", "*"),
		ec2AuthzStatement("Deny", ec2AuthzSubnetARN),
	)

	err := f.launch(t, nominalLaunch())
	if !ec2AuthzDenied(t, err) {
		t.Fatal("a Deny scoped to the launch's subnet ARN did not block the launch")
	}
	assert.Equal(t, ec2AuthzSubnetARN, deniedResource(t, err),
		"the denial names the subnet the Deny scoped to")
}

// TestEC2_Authz_ScopedDenyOnEachResourceBlocks covers the same guardrail written
// about each of the three resources a request names by ID. All three were inert.
func TestEC2_Authz_ScopedDenyOnEachResourceBlocks(t *testing.T) {
	for _, arn := range []string{ec2AuthzImageARN, ec2AuthzSubnetARN, ec2AuthzSGARN} {
		t.Run(arn, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "judy", emulator.PolicyDocument{})
			f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", arn))

			err := f.launch(t, nominalLaunch())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("a Deny scoped to %s did not block the launch", arn)
			}
			assert.Equal(t, arn, deniedResource(t, err))
		})
	}
}

// TestEC2_Authz_LeastPrivilegePolicyAllows is the other direction the single-ARN
// resolution broke: a policy naming exactly the five ARNs AWS requires denied
// every launch, because "arn:…:image/ami-…" as a pattern does not match the value
// "*" either. Writing the policy AWS's own documentation calls for made the
// emulator refuse the call.
func TestEC2_Authz_LeastPrivilegePolicyAllows(t *testing.T) {
	f := newEC2AuthzFixture(t, "karl", emulator.PolicyDocument{})
	f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))

	require.NoError(t, f.launch(t, nominalLaunch()),
		"a policy naming every required ARN must permit the launch")
}

// TestEC2_Authz_OmittingAnyRequiredResourceDenies pins that all five must be
// allowed, and that the denial names the one that was not — the message is the
// only place the failing resource surfaces, so it is what tells a caller which
// ARN to add.
func TestEC2_Authz_OmittingAnyRequiredResourceDenies(t *testing.T) {
	all := ec2AuthzAllFive()
	for i, missing := range all {
		t.Run(missing, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "lena", emulator.PolicyDocument{})
			kept := make([]string, 0, len(all)-1)
			kept = append(kept, all[:i]...)
			kept = append(kept, all[i+1:]...)
			f.setPolicy(t, ec2AuthzStatement("Allow", kept...))

			err := f.launch(t, nominalLaunch())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("a policy omitting %s allowed the launch", missing)
			}
			assert.Equal(t, missing, deniedResource(t, err),
				"the denial names the ARN the policy omitted")
		})
	}
}

// TestEC2_Authz_ResourceTagsPairWithTheirOwnResource is why authzResource carries
// tags per ARN rather than one merged map.
//
// The condition is written about the resource being evaluated, so a tag on the
// AMI cannot satisfy a condition checked against the subnet. One merged map would
// let any tagged resource in the request satisfy the condition for all of them —
// a false allow, and the reason the decision loop builds a fresh condCtx per
// resource.
func TestEC2_Authz_ResourceTagsPairWithTheirOwnResource(t *testing.T) {
	newFixture := func(t *testing.T) *ec2AuthzFixture {
		t.Helper()
		doc := newABACPolicy("Allow", "ec2:RunInstances", "*", "aws:ResourceTag/Env", "test")
		return newEC2AuthzFixture(t, "mona", doc)
	}
	const good = "test"
	const bad = "prod"

	t.Run("every resource tagged is allowed", func(t *testing.T) {
		f := newFixture(t)
		f.putImage(t, ec2AuthzAMI, map[string]string{"Env": good})
		f.putSubnet(t, ec2AuthzSubnet, map[string]string{"Env": good})
		f.putSecurityGroup(t, ec2AuthzSG, map[string]string{"Env": good})
		// The instance and network-interface ARNs carry no tags — they do not exist
		// yet — so the condition cannot be met for them. The launch names its own
		// resources, so a tag-gated policy has to allow those two unconditionally,
		// which is what the second statement does.
		f.setPolicy(t,
			emulator.PolicyStatement{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"ec2:RunInstances"},
				Resource: emulator.StringOrSlice{ec2AuthzImageARN, ec2AuthzSubnetARN, ec2AuthzSGARN},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:ResourceTag/Env": {good}},
				},
			},
			ec2AuthzStatement("Allow", ec2AuthzENIARN, ec2AuthzInstARN),
		)
		require.NoError(t, f.launch(t, nominalLaunch()))
	})

	tagged := map[string]func(*ec2AuthzFixture, *testing.T, string){
		ec2AuthzImageARN: func(f *ec2AuthzFixture, t *testing.T, v string) {
			f.putImage(t, ec2AuthzAMI, map[string]string{"Env": v})
		},
		ec2AuthzSubnetARN: func(f *ec2AuthzFixture, t *testing.T, v string) {
			f.putSubnet(t, ec2AuthzSubnet, map[string]string{"Env": v})
		},
		ec2AuthzSGARN: func(f *ec2AuthzFixture, t *testing.T, v string) {
			f.putSecurityGroup(t, ec2AuthzSG, map[string]string{"Env": v})
		},
	}
	for arn, mistag := range tagged {
		t.Run("mistagging "+arn+" denies", func(t *testing.T) {
			f := newFixture(t)
			for other, tag := range tagged {
				if other == arn {
					continue
				}
				tag(f, t, good)
			}
			mistag(f, t, bad)
			f.setPolicy(t,
				emulator.PolicyStatement{
					Effect:   "Allow",
					Action:   emulator.StringOrSlice{"ec2:RunInstances"},
					Resource: emulator.StringOrSlice{ec2AuthzImageARN, ec2AuthzSubnetARN, ec2AuthzSGARN},
					Condition: map[string]map[string]emulator.StringOrSlice{
						"StringEquals": {"aws:ResourceTag/Env": {good}},
					},
				},
				ec2AuthzStatement("Allow", ec2AuthzENIARN, ec2AuthzInstARN),
			)
			err := f.launch(t, nominalLaunch())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("a %s tagged Env=%s satisfied a condition requiring Env=%s", arn, bad, good)
			}
			assert.Equal(t, arn, deniedResource(t, err))
		})
	}
}

// TestEC2_Authz_PermissionBoundaryCoversEveryResource pins the boundary against
// every resource rather than only the first.
//
// This is the mutation #660's testing found surviving: moving the boundary check
// out of the per-resource loop still passed every test that existed, because a
// boundary that refuses the *first* resource refuses the request either way. The
// boundary here allows the AMI and denies the subnet, so only a check that
// reaches the second resource catches it.
func TestEC2_Authz_PermissionBoundaryCoversEveryResource(t *testing.T) {
	f := newEC2AuthzFixture(t, "nina", emulator.PolicyDocument{})
	f.setPolicy(t, ec2AuthzStatement("Allow", "*"))

	boundaryARN := "arn:aws:iam::" + ec2AuthzAccount + ":policy/LaunchBoundary"
	boundary := emulator.IAMPolicy{
		PolicyName:       "LaunchBoundary",
		PolicyID:         "ANPABOUND",
		ARN:              boundaryARN,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			// Everything except the subnet, which is the second resource evaluated.
			Statement: []emulator.PolicyStatement{
				ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzSGARN, ec2AuthzENIARN, ec2AuthzInstARN),
			},
		},
	}
	raw, err := json.Marshal(boundary)
	require.NoError(t, err)
	require.NoError(t, f.state.Put(context.Background(), "iam", emulator.IAMPolicyKeyForTest(boundaryARN), raw))

	user := emulator.IAMUser{
		UserName:            "nina",
		UserID:              "AIDATEST",
		ARN:                 "arn:aws:iam::" + ec2AuthzAccount + ":user/nina",
		Path:                "/",
		PermissionsBoundary: &emulator.IAMAttachedPolicy{PolicyARN: boundaryARN, PolicyName: "LaunchBoundary"},
	}
	userRaw, err := json.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, f.state.Put(context.Background(), "iam", emulator.IAMUserKeyForTest(authzTestAccount, "nina"), userRaw))

	launchErr := f.launch(t, nominalLaunch())
	if !ec2AuthzDenied(t, launchErr) {
		t.Fatal("a boundary refusing the launch's subnet did not block it")
	}
	var awsErr *emulator.AWSError
	require.ErrorAs(t, launchErr, &awsErr)
	assert.Contains(t, awsErr.Message, "permission boundary")
}

// TestEC2_Authz_AbsentMembersAreSkipped pins that a resource the request does not
// name is left out rather than resolved to "*".
//
// "*" for an absent ID would widen the policy the caller wrote: a statement
// naming only "*" would then satisfy a launch that named no subnet, which is the
// direction a privilege boundary must never fail in.
//
// The subnet and security group are the exception, and not to this rule: a launch that
// names neither takes both from the default VPC, creating them if the account has none,
// so they are resolved rather than absent — by ID when they exist and as subnet/* and
// security-group/* when the launch is about to mint them, the same footing instance/*
// and network-interface/* are already on. That is a resource the request *creates*, not
// one it omits (#673); see TestEC2_Authz_FreshAccountResolvesToWildcards. Every other
// member below is genuinely absent, which is why the subtests here assert the two are
// named while the rest are not.
func TestEC2_Authz_AbsentMembersAreSkipped(t *testing.T) {
	t.Run("an AMI-only launch names the default-VPC wildcards too", func(t *testing.T) {
		// Before #673 this launch named three resources and the policy below sufficed.
		// A launch that omits SubnetId does not omit a subnet — it takes the default
		// VPC's, and in an empty fixture it is about to create one — so subnet/* and
		// security-group/* are part of the decision. Skip-don't-widen still governs a
		// resource the request truly leaves out; it does not cover one the launch
		// creates.
		f := newEC2AuthzFixture(t, "omar", emulator.PolicyDocument{})
		params := map[string]string{"ImageId": ec2AuthzAMI, "MinCount": "1", "MaxCount": "1"}

		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzENIARN, ec2AuthzInstARN))
		err := f.launch(t, params)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a policy omitting subnet/* allowed a launch that will create a default subnet")
		}
		assert.Equal(t, ec2AuthzSubnetWildcardARN, deniedResource(t, err))

		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN,
			ec2AuthzSubnetWildcardARN, ec2AuthzSGWildcardARN, ec2AuthzENIARN, ec2AuthzInstARN))
		require.NoError(t, f.launch(t, params),
			"naming the two wildcards the launch will create permits it")
	})

	t.Run("the AMI is still required", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "omar", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzENIARN, ec2AuthzInstARN))
		err := f.launch(t, map[string]string{"ImageId": ec2AuthzAMI, "MinCount": "1", "MaxCount": "1"})
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a policy omitting the AMI allowed a launch that named it")
		}
		assert.Equal(t, ec2AuthzImageARN, deniedResource(t, err))
	})

	t.Run("a launch naming nothing is still decided against ARNs", func(t *testing.T) {
		// Before #673 this fell back to buildResourceARN's single-resource path and
		// resolved to the literal "*". It no longer can: the launch will create a
		// default subnet, a default security group, an interface and an instance, so
		// four ARNs are named and none of them is "*". That closes the last route by
		// which a RunInstances escaped its own resource list.
		f := newEC2AuthzFixture(t, "omar", emulator.PolicyDocument{})
		params := map[string]string{"MinCount": "1", "MaxCount": "1"}

		f.setPolicy(t, ec2AuthzStatement("Allow", "*"))
		require.NoError(t, f.launch(t, params))

		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzInstARN))
		err := f.launch(t, params)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a policy naming only instance/* allowed a launch that also creates a subnet")
		}
		assert.Equal(t, ec2AuthzSubnetWildcardARN, deniedResource(t, err),
			"the denial names the default subnet the launch would create, not \"*\"")

		f.setPolicy(t, ec2AuthzStatement("Allow",
			ec2AuthzSubnetWildcardARN, ec2AuthzSGWildcardARN, ec2AuthzENIARN, ec2AuthzInstARN))
		require.NoError(t, f.launch(t, params),
			"a launch naming no AMI requires no image ARN, so these four suffice")
	})
}

// TestEC2_Authz_NestedNetworkInterfaceFormResolves covers the shape the AWS SDK
// actually sends. Whenever a public-IP preference is set the SDK moves SubnetId
// and the security groups inside NetworkInterface.1, so reading only the
// top-level parameters would authorize such a launch against neither.
func TestEC2_Authz_NestedNetworkInterfaceFormResolves(t *testing.T) {
	nested := map[string]string{
		"ImageId":                        ec2AuthzAMI,
		"MinCount":                       "1",
		"MaxCount":                       "1",
		"NetworkInterface.1.DeviceIndex": "0",
		"NetworkInterface.1.SubnetId":    ec2AuthzSubnet,
		"NetworkInterface.1.Groups.1":    ec2AuthzSG,
		"NetworkInterface.1.AssociatePublicIpAddress": "true",
	}

	t.Run("the nested subnet is required", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "pia", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", ec2AuthzSubnetARN))
		err := f.launch(t, nested)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a Deny on the subnet named inside NetworkInterface.1 did not block the launch")
		}
		assert.Equal(t, ec2AuthzSubnetARN, deniedResource(t, err))
	})

	t.Run("the five ARNs are the same as the top-level form", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "pia", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))
		require.NoError(t, f.launch(t, nested))
	})

	t.Run("an interface brought by ID is named, not wildcarded", func(t *testing.T) {
		const eniID = "eni-0eee77778888ffff9"
		byID := map[string]string{
			"ImageId":                               ec2AuthzAMI,
			"MinCount":                              "1",
			"MaxCount":                              "1",
			"NetworkInterface.1.DeviceIndex":        "0",
			"NetworkInterface.1.NetworkInterfaceId": eniID,
		}
		eniARN := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":network-interface/" + eniID

		f := newEC2AuthzFixture(t, "pia", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", eniARN))
		err := f.launch(t, byID)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a Deny on the ENI the launch attaches did not block it")
		}
		assert.Equal(t, eniARN, deniedResource(t, err))

		// And the wildcard is not also evaluated: a policy naming the specific ENI
		// suffices, so an existing interface does not require network-interface/*.
		// The two default-VPC wildcards join it, because the interface carries no
		// subnet and the launch has none of its own (#673).
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, eniARN, ec2AuthzInstARN,
			ec2AuthzSubnetWildcardARN, ec2AuthzSGWildcardARN))
		require.NoError(t, f.launch(t, byID))
	})
}

// TestEC2_Authz_LaunchTemplateResourcesAreAuthorized pins that a launch reaching
// its subnet through a template is authorized against that subnet.
//
// AWS's CreateFleet reference is the authority: "Resource-level permissions for
// this action do not include the resources specified in a launch template. To
// specify resource-level permissions for resources specified in a launch
// template, you must include the resources in the RunInstances action
// statement." So the template's resources are part of a RunInstances decision,
// and a guardrail scoped to one subnet has to fence off a template that reaches
// it — otherwise a template is a way around the policy.
func TestEC2_Authz_LaunchTemplateResourcesAreAuthorized(t *testing.T) {
	const ltID = "lt-0aaa1111bbbb2222c"
	const ltName = "launch-into-private"
	newFixture := func(t *testing.T) *ec2AuthzFixture {
		t.Helper()
		f := newEC2AuthzFixture(t, "quinn", emulator.PolicyDocument{})
		lt := emulator.EC2LaunchTemplate{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: ltName,
			DefaultVersionNum:  1,
			LatestVersionNum:   1,
			Versions: []emulator.EC2LaunchTemplateVersion{{
				VersionNumber: 1,
				Data: emulator.EC2LaunchTemplateData{
					ImageID:                ec2AuthzAMI,
					SubnetID:               ec2AuthzSubnet,
					NetworkInterfaceGroups: []string{ec2AuthzSG},
				},
			}},
		}
		f.put(t, "lt:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ltID, lt)
		if err := f.state.Put(context.Background(), "ec2", //nolint:contextcheck
			"lt_by_name:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ltName, []byte(ltID)); err != nil {
			t.Fatalf("store lt_by_name: %v", err)
		}
		return f
	}
	byName := map[string]string{
		"MinCount":                          "1",
		"MaxCount":                          "1",
		"LaunchTemplate.LaunchTemplateName": ltName,
	}

	t.Run("by name, a Deny on the template's subnet blocks the launch", func(t *testing.T) {
		f := newFixture(t)
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", ec2AuthzSubnetARN))
		err := f.launch(t, byName)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a template was a way around a subnet-scoped Deny")
		}
		assert.Equal(t, ec2AuthzSubnetARN, deniedResource(t, err))
	})

	t.Run("by ID, the same five ARNs are required", func(t *testing.T) {
		f := newFixture(t)
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))
		require.NoError(t, f.launch(t, map[string]string{
			"MinCount":                        "1",
			"MaxCount":                        "1",
			"LaunchTemplate.LaunchTemplateId": ltID,
		}))
	})

	t.Run("the request's own AMI wins over the template's", func(t *testing.T) {
		// AWS: "Any additional parameters that you specify for the new instance
		// overwrite the corresponding parameters included in the launch template." So
		// the decision has to be about the AMI the launch will actually use.
		// A second bundled image, distinct from the template's, so "the request's AMI
		// won" is falsifiable.
		ownAMI := ec2TestImageArm
		f := newFixture(t)
		f.putImage(t, ownAMI, nil)
		ownARN := "arn:aws:ec2:" + ec2AuthzRegion + "::image/" + ownAMI
		params := map[string]string{
			"MinCount":                          "1",
			"MaxCount":                          "1",
			"ImageId":                           ownAMI,
			"LaunchTemplate.LaunchTemplateName": ltName,
		}

		f.setPolicy(t, ec2AuthzStatement("Allow", ownARN, ec2AuthzSubnetARN, ec2AuthzSGARN, ec2AuthzENIARN, ec2AuthzInstARN))
		require.NoError(t, f.launch(t, params))

		// The template's AMI is not required, because the launch will not use it.
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", ec2AuthzImageARN))
		require.NoError(t, f.launch(t, params),
			"a Deny on the template's AMI must not block a launch that overrode it")
	})

	t.Run("an unresolvable template contributes nothing", func(t *testing.T) {
		// The handler answers a missing template with InvalidLaunchTemplateId.NotFound.
		// The decision must not turn that into AccessDenied, so an unreadable template
		// adds no resources rather than failing the request.
		f := newFixture(t)
		// Contributing nothing leaves the launch with no subnet of its own, so the
		// default-VPC wildcards apply as they would to any subnet-less launch (#673).
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzENIARN, ec2AuthzInstARN,
			ec2AuthzSubnetWildcardARN, ec2AuthzSGWildcardARN))
		require.NoError(t, f.launch(t, map[string]string{
			"ImageId":                           ec2AuthzAMI,
			"MinCount":                          "1",
			"MaxCount":                          "1",
			"LaunchTemplate.LaunchTemplateName": "no-such-template",
		}))
	})
}

// TestEC2_Authz_MultipleSecurityGroupsEachRequireAllowing pins that every group
// is evaluated, not just the first. A policy allowing one group and not the other
// must refuse the launch, or "may only use the approved security group" is not
// expressible.
func TestEC2_Authz_MultipleSecurityGroupsEachRequireAllowing(t *testing.T) {
	const secondSG = "sg-0999888877776666a"
	secondARN := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":security-group/" + secondSG

	f := newEC2AuthzFixture(t, "rita", emulator.PolicyDocument{})
	f.putSecurityGroup(t, secondSG, nil)
	params := nominalLaunch()
	params["SecurityGroupId.2"] = secondSG

	f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))
	err := f.launch(t, params)
	if !ec2AuthzDenied(t, err) {
		t.Fatal("a second security group the policy does not name allowed the launch")
	}
	assert.Equal(t, secondARN, deniedResource(t, err))

	f.setPolicy(t, ec2AuthzStatement("Allow", append(ec2AuthzAllFive(), secondARN)...))
	require.NoError(t, f.launch(t, params))
}

// TestEC2_Authz_ImageARNCarriesNoAccountID pins the one ARN whose account field
// is empty.
//
// The Service Authorization Reference gives the image ARN as
// arn:${Partition}:ec2:${Region}::image/${ImageId} — an AMI is shareable, so the
// account field is empty by design rather than by omission. It has to stay empty:
// arnAccountID reads an empty account as "no account" rather than as "this
// account", and a policy naming the ARN AWS documents would not match one
// carrying an account ID.
func TestEC2_Authz_ImageARNCarriesNoAccountID(t *testing.T) {
	f := newEC2AuthzFixture(t, "sami", emulator.PolicyDocument{})
	f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))
	require.NoError(t, f.launch(t, nominalLaunch()))

	assert.Equal(t, "arn:aws:ec2:us-east-1::image/"+ec2AuthzAMI, ec2AuthzImageARN,
		"the ARN the decision builds has an empty account field")
	assert.NotContains(t, strings.TrimPrefix(ec2AuthzImageARN, "arn:aws:ec2:us-east-1:"), ec2AuthzAccount)

	// The account-scoped spelling must not be what the decision uses: a policy
	// naming it does not grant the launch.
	withAccount := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":image/" + ec2AuthzAMI
	f.setPolicy(t, ec2AuthzStatement("Allow", withAccount, ec2AuthzSubnetARN, ec2AuthzSGARN, ec2AuthzENIARN, ec2AuthzInstARN))
	err := f.launch(t, nominalLaunch())
	if !ec2AuthzDenied(t, err) {
		t.Fatal("an account-scoped image ARN granted a launch, so the decision is not using AWS's format")
	}
	assert.Equal(t, ec2AuthzImageARN, deniedResource(t, err))
}

// TestEC2_Authz_SingleResourceOperationsAreUnchanged pins the floor of the resolution:
// an EC2 operation that names no resource substrate can resolve is still decided against
// the literal "*", exactly as it was. These changes are about resolving the resources a
// request names, not about tightening EC2 as a whole.
//
// Its rows name one ID each, so the two instance rows go through
// [ec2AuthzNamedResources] and come back with the single resource that parameter names —
// which is what #744 left unchanged for a batch of one. The multi-resource paths a request
// can take are RunInstances' five (#662), a tagging call's up-to-1000 (#674) and every ID
// under one of [ec2AuthzIDParams] (#730, #744); everything else is here.
func TestEC2_Authz_SingleResourceOperationsAreUnchanged(t *testing.T) {
	instanceARN := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":instance/i-0abc"
	tests := []struct {
		name      string
		operation string
		params    map[string]string
		resource  string
	}{
		{
			"DescribeInstances by ID names the instance",
			"DescribeInstances",
			map[string]string{"InstanceId.1": "i-0abc"},
			instanceARN,
		},
		{
			"TerminateInstances by ID names the instance",
			"TerminateInstances",
			map[string]string{"InstanceId.1": "i-0abc"},
			instanceARN,
		},
		{
			// CreateTags with no ResourceId at all names nothing, so it stays on the
			// single-resource path. The multi-resource form it used to take here — two
			// ResourceId.N resolving to the one ARN "*" — was the follow-up #662 scoped
			// out, and #674 closed it; see TestEC2_TagAuthz_ScopedDenyOnAnyNamedResourceBlocks.
			"a CreateTags naming no resource resolves to \"*\"",
			"CreateTags",
			map[string]string{"Tag.1.Key": "Env", "Tag.1.Value": "test"},
			"*",
		},
		{
			"DescribeVolumes resolves to \"*\"",
			"DescribeVolumes",
			map[string]string{"VolumeId.1": "vol-0abc"},
			"*",
		},
		{
			// Not RunInstances, and it names an AMI — the gate is on the operation, so
			// this must not pick up the launch resolver.
			"CreateImage resolves to \"*\"",
			"CreateImage",
			map[string]string{"ImageId": ec2AuthzAMI, "Name": "snap"},
			"*",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "toni", emulator.PolicyDocument{})
			// An action-only Deny with no Resource scoping refuses whatever the
			// decision resolves, so the message reports the single ARN it built.
			f.setPolicy(t, emulator.PolicyStatement{
				Effect:   "Deny",
				Action:   emulator.StringOrSlice{"ec2:" + tt.operation},
				Resource: emulator.StringOrSlice{"*"},
			})
			err := f.call(t, tt.operation, tt.params)
			require.Error(t, err)
			assert.Equal(t, tt.resource, deniedResource(t, err))
		})
	}
}
