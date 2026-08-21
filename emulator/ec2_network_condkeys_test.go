package emulator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The ec2:Subnet and ec2:Vpc condition keys on a launch's networking resources
// (#692).
//
// #673 made a launch that omits SubnetId authorized against the default VPC's subnet
// ARN, and docs/services.md labeled that substrate's reading rather than AWS parity —
// pointing instead at the mechanism AWS itself documents for fencing a launch into one
// subnet, a condition key on the network-interface resource. Substrate had neither key,
// so the policy AWS tells a consumer to write was worse than inert: an Allow gated on
// ec2:Subnet could not be satisfied and denied every launch, and AWS's own Deny — which
// is written with the negated ArnNotEquals — compared the permitted ARN against the
// empty string, matched, and denied every launch too, the permitted subnet included.
// Both halves of the recorded fail-before are that blanket deny.
//
// AWS's own subnet guardrail, quoted verbatim from the EC2 example policies, is the
// Deny in [TestEC2_Authz_SubnetKeyFencesALaunchToOneSubnet]: "you could create a
// policy that denies users permissions to launch an instance into any other subnet.
// The statement does this by denying permission to create a network interface, except
// where subnet subnet-12345678 is specified."
//
// Both keys take a full ARN. For the VPC AWS says so outright — "To specify a VPC for
// the ec2:Vpc condition key, you must specify the full ARN of the VPC" — and its
// machine-readable service reference declares the Type of both keys as ARN. Its two
// example policies nonetheless reach for different operator families, ArnNotEquals for
// ec2:Subnet and StringEquals for ec2:Vpc, so both are exercised below.

const (
	// ec2CondKeyVPCID is the VPC the fixture's own subnet and security group sit in.
	ec2CondKeyVPCID = "vpc-0aaa22223333bbbb4"

	// ec2CondKeyOtherSubnet and ec2CondKeyOtherVPCID are the subnet and VPC a
	// guardrail is written to keep a launch out of.
	ec2CondKeyOtherSubnet = "subnet-0bbb33334444cccc5"
	ec2CondKeyOtherVPCID  = "vpc-0ccc44445555dddd6"
)

var (
	ec2CondKeyVPCARN        = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":vpc/" + ec2CondKeyVPCID
	ec2CondKeyOtherVPCARN   = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":vpc/" + ec2CondKeyOtherVPCID
	ec2CondKeyDefaultVPCARN = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount +
		":vpc/" + ec2AuthzDefaultVPCID
)

// putSubnetInVPC re-puts a subnet with the VPC it belongs to, which the shared
// fixture's putSubnet leaves empty — the record's vpc_id is where ec2:Vpc comes from
// for a launch whose subnet the request named.
func (f *ec2AuthzFixture) putSubnetInVPC(t *testing.T, subnetID, vpcID string) {
	t.Helper()
	f.put(t, "subnet:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+subnetID, emulator.EC2Subnet{
		SubnetID:  subnetID,
		VPCID:     vpcID,
		State:     "available",
		AccountID: ec2AuthzAccount,
		Region:    ec2AuthzRegion,
	})
}

// putSGInVPC re-puts a security group with the VPC it belongs to. The reference lists
// ec2:Vpc on RunInstances' security-group resource as well, where it means the group's
// own VPC — not the launch's.
func (f *ec2AuthzFixture) putSGInVPC(t *testing.T, sgID, vpcID string) {
	t.Helper()
	f.put(t, "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+sgID, emulator.EC2SecurityGroup{
		GroupID:   sgID,
		VPCID:     vpcID,
		AccountID: ec2AuthzAccount,
		Region:    ec2AuthzRegion,
	})
}

// ec2CondKeyStatement builds one RunInstances statement with a condition block, the
// shape both of AWS's example policies take.
func ec2CondKeyStatement(effect string, resources []string,
	cond map[string]map[string]emulator.StringOrSlice) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:    effect,
		Action:    emulator.StringOrSlice{"ec2:RunInstances"},
		Resource:  emulator.StringOrSlice(resources),
		Condition: cond,
	}
}

// ec2CondKeyAllowAll is the unconditional grant the guardrail Denies below sit beside,
// so a refusal can only have come from the condition under test.
func ec2CondKeyAllowAll() emulator.PolicyStatement {
	return ec2AuthzStatement("Allow", "*")
}

// ec2CondKeyTemplateLaunch stores a launch template carrying subnetID and returns the
// params of a launch that names it, which is how a caller reaches a subnet without
// writing it in the request.
func (f *ec2AuthzFixture) ec2CondKeyTemplateLaunch(t *testing.T, subnetID string) map[string]string {
	t.Helper()
	const ltID = "lt-0eee55556666ffff7"
	const ltName = "subnet-from-template"
	f.put(t, "lt:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ltID, emulator.EC2LaunchTemplate{
		LaunchTemplateID:   ltID,
		LaunchTemplateName: ltName,
		DefaultVersionNum:  1,
		LatestVersionNum:   1,
		Versions: []emulator.EC2LaunchTemplateVersion{{
			VersionNumber: 1,
			Data: emulator.EC2LaunchTemplateData{
				ImageID:                ec2AuthzAMI,
				SubnetID:               subnetID,
				NetworkInterfaceGroups: []string{ec2AuthzSG},
			},
		}},
	})
	if err := f.state.Put(context.Background(), "ec2", //nolint:contextcheck
		"lt_by_name:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ltName, []byte(ltID)); err != nil {
		t.Fatalf("store lt_by_name: %v", err)
	}
	return map[string]string{
		"MinCount":                          "1",
		"MaxCount":                          "1",
		"LaunchTemplate.LaunchTemplateName": ltName,
	}
}

// TestEC2_Authz_SubnetKeyFencesALaunchToOneSubnet is the decisive test: AWS's own
// documented subnet guardrail, which had no effect before this change.
//
// The policy is AWS's, shape for shape — a Deny on network-interface/* with
// ArnNotEquals over ec2:Subnet, whose prose reads "denying permission to create a
// network interface, except where subnet subnet-12345678 is specified" — beside an
// unconditional Allow, so the only thing that can refuse a launch is the key.
func TestEC2_Authz_SubnetKeyFencesALaunchToOneSubnet(t *testing.T) {
	guardrail := func(t *testing.T, f *ec2AuthzFixture) {
		t.Helper()
		f.setPolicy(t,
			ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN},
				map[string]map[string]emulator.StringOrSlice{
					"ArnNotEquals": {"ec2:Subnet": {ec2AuthzSubnetARN}},
				}),
			ec2CondKeyAllowAll(),
		)
	}

	t.Run("a launch into the permitted subnet", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "amara", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		guardrail(t, f)
		require.NoError(t, f.launch(t, nominalLaunch()))
	})

	t.Run("a launch into any other subnet", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "bao", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2CondKeyOtherSubnet, ec2CondKeyOtherVPCID)
		guardrail(t, f)
		params := nominalLaunch()
		params["SubnetId"] = ec2CondKeyOtherSubnet
		err := f.launch(t, params)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("AWS's documented subnet guardrail did not fence off another subnet")
		}
		// The Deny is written against the interface, so that is the resource named —
		// which is also what tells a reader the key lives there and not on the subnet.
		assert.Equal(t, ec2AuthzENIARN, deniedResource(t, err))
	})

	t.Run("a launch that omits SubnetId and lands in the default subnet", func(t *testing.T) {
		// The gap #673 closed for resource ARNs, now closed for the key AWS's own
		// documentation recommends instead: leaving the parameter out must not be a
		// way around the guardrail.
		f := newEC2AuthzFixture(t, "cato", emulator.PolicyDocument{})
		f.putDefaultVPC(t, nil)
		guardrail(t, f)
		if !ec2AuthzDenied(t, f.launch(t, subnetlessLaunch())) {
			t.Fatal("omitting SubnetId defeated an ec2:Subnet guardrail")
		}
	})

	t.Run("a launch reaching the permitted subnet through a template", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "dilnoza", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		params := f.ec2CondKeyTemplateLaunch(t, ec2AuthzSubnet)
		guardrail(t, f)
		require.NoError(t, f.launch(t, params))
	})

	t.Run("a launch reaching another subnet through a template", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "emeka", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2CondKeyOtherSubnet, ec2CondKeyOtherVPCID)
		params := f.ec2CondKeyTemplateLaunch(t, ec2CondKeyOtherSubnet)
		guardrail(t, f)
		if !ec2AuthzDenied(t, f.launch(t, params)) {
			t.Fatal("a template was a way around an ec2:Subnet guardrail")
		}
	})
}

// TestEC2_Authz_VpcKeyScopesALaunch pins ec2:Vpc, whose value AWS documents as a full
// ARN: "To specify a VPC for the ec2:Vpc condition key, you must specify the full ARN
// of the VPC."
//
// Both operator families are exercised. AWS's own ec2:Vpc example uses StringEquals
// even though the reference declares the key's type as ARN, and its ec2:Subnet example
// uses ArnNotEquals — a consumer will have copied whichever it found, and a full ARN
// satisfies both.
func TestEC2_Authz_VpcKeyScopesALaunch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		operator string
	}{
		{"StringNotEquals, the operator AWS's own ec2:Vpc example uses", "StringNotEquals"},
		{"ArnNotEquals, the family the reference's ARN type implies", "ArnNotEquals"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("a launch inside the permitted VPC", func(t *testing.T) {
				f := newEC2AuthzFixture(t, "farida", emulator.PolicyDocument{})
				f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
				f.putSGInVPC(t, ec2AuthzSG, ec2CondKeyVPCID)
				f.setPolicy(t,
					ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN},
						map[string]map[string]emulator.StringOrSlice{
							tc.operator: {"ec2:Vpc": {ec2CondKeyVPCARN}},
						}),
					ec2CondKeyAllowAll(),
				)
				require.NoError(t, f.launch(t, nominalLaunch()))
			})

			t.Run("a launch in another VPC", func(t *testing.T) {
				f := newEC2AuthzFixture(t, "gorou", emulator.PolicyDocument{})
				f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyOtherVPCID)
				f.putSGInVPC(t, ec2AuthzSG, ec2CondKeyOtherVPCID)
				f.setPolicy(t,
					ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN},
						map[string]map[string]emulator.StringOrSlice{
							tc.operator: {"ec2:Vpc": {ec2CondKeyVPCARN}},
						}),
					ec2CondKeyAllowAll(),
				)
				if !ec2AuthzDenied(t, f.launch(t, nominalLaunch())) {
					t.Fatal("an ec2:Vpc guardrail did not fence off another VPC")
				}
			})
		})
	}
}

// TestEC2_Authz_TheKeysAreScopedToTheResourceTheyDescribe pins the reason Context
// hangs off each resource rather than off the request: the reference lists ec2:Subnet
// on RunInstances' network-interface resource only, and ec2:Vpc on the interface, the
// subnet and each security group — not on the image and not on the instance.
//
// A merged request context would make all five carry both keys, so a Deny a consumer
// scoped to instance/* would fire on a launch AWS permits.
func TestEC2_Authz_TheKeysAreScopedToTheResourceTheyDescribe(t *testing.T) {
	// Null:"false" matches when the key is *present*, which is the only way to ask
	// "does this resource carry the key at all" without also asserting its value.
	present := func(key string) map[string]map[string]emulator.StringOrSlice {
		return map[string]map[string]emulator.StringOrSlice{
			"Null": {key: {"false"}},
		}
	}

	t.Run("ec2:Subnet is not on the instance resource", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "hana", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		f.setPolicy(t,
			ec2CondKeyStatement("Deny", []string{ec2AuthzInstARN}, present("ec2:Subnet")),
			ec2CondKeyAllowAll(),
		)
		require.NoError(t, f.launch(t, nominalLaunch()),
			"the instance resource carried ec2:Subnet, which the reference does not list on it")
	})

	t.Run("ec2:Subnet is on the network-interface resource", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "ines", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		f.setPolicy(t,
			ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN}, present("ec2:Subnet")),
			ec2CondKeyAllowAll(),
		)
		if !ec2AuthzDenied(t, f.launch(t, nominalLaunch())) {
			t.Fatal("the network-interface resource did not carry ec2:Subnet")
		}
	})

	t.Run("ec2:Vpc is on the subnet resource too", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "jomo", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		f.setPolicy(t,
			ec2CondKeyStatement("Deny", []string{ec2AuthzSubnetARN},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"ec2:Vpc": {ec2CondKeyVPCARN}},
				}),
			ec2CondKeyAllowAll(),
		)
		err := f.launch(t, nominalLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("the subnet resource did not carry ec2:Vpc")
		}
		assert.Equal(t, ec2AuthzSubnetARN, deniedResource(t, err))
	})

	t.Run("a security group reports its own VPC, not the launch's", func(t *testing.T) {
		// The group is in one VPC and the subnet in another — a real misconfiguration
		// AWS refuses, and the case that shows whose VPC each resource reports.
		f := newEC2AuthzFixture(t, "kwame", emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		f.putSGInVPC(t, ec2AuthzSG, ec2CondKeyOtherVPCID)
		denyGroupsInVPC := func(t *testing.T, f *ec2AuthzFixture, vpcARN string) {
			t.Helper()
			f.setPolicy(t,
				ec2CondKeyStatement("Deny", []string{ec2AuthzSGARN},
					map[string]map[string]emulator.StringOrSlice{
						"StringEquals": {"ec2:Vpc": {vpcARN}},
					}),
				ec2CondKeyAllowAll(),
			)
		}

		denyGroupsInVPC(t, f, ec2CondKeyOtherVPCARN)
		err := f.launch(t, nominalLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("the security-group resource did not report the group's own VPC")
		}
		assert.Equal(t, ec2AuthzSGARN, deniedResource(t, err))

		// And the subnet's VPC does not match the group's resource, which is what
		// makes the two independent rather than one value copied onto both.
		g := newEC2AuthzFixture(t, "lior", emulator.PolicyDocument{})
		g.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		g.putSGInVPC(t, ec2AuthzSG, ec2CondKeyOtherVPCID)
		denyGroupsInVPC(t, g, ec2CondKeyVPCARN)
		require.NoError(t, g.launch(t, nominalLaunch()))
	})
}

// TestEC2_Authz_NetworkKeysAreOmittedWhenNothingResolved pins the absent-key rule the
// issue asks for: "The keys are absent rather than empty when no subnet resolves, so a
// condition on them denies rather than vacuously allowing."
//
// A launch with no subnet and no default VPC is about to create both, so there is no
// subnet and no VPC to report. Writing "*" as the *value* would be an ARN-shaped lie
// no caller's ArnEquals could match; omission is the honest answer, and it makes an
// Allow gated on either key refuse such a launch.
func TestEC2_Authz_NetworkKeysAreOmittedWhenNothingResolved(t *testing.T) {
	for _, key := range []string{"ec2:Subnet", "ec2:Vpc"} {
		t.Run(key+" is absent", func(t *testing.T) {
			f := newEC2AuthzFixture(t, "mira-"+key, emulator.PolicyDocument{})
			f.setPolicy(t,
				ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN},
					map[string]map[string]emulator.StringOrSlice{
						"Null": {key: {"true"}},
					}),
				ec2CondKeyAllowAll(),
			)
			if !ec2AuthzDenied(t, f.launch(t, subnetlessLaunch())) {
				t.Fatalf("%s was present on a launch that resolved neither", key)
			}
		})

		t.Run("an Allow gated on "+key+" refuses it", func(t *testing.T) {
			// Not vacuously allowed: the statement's condition is unsatisfiable, so
			// the interface has no grant and the launch is refused, naming it.
			f := newEC2AuthzFixture(t, "nadia-"+key, emulator.PolicyDocument{})
			f.setPolicy(t,
				ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzSubnetWildcardARN,
					ec2AuthzSGWildcardARN, ec2AuthzInstARN),
				ec2CondKeyStatement("Allow", []string{ec2AuthzENIARN},
					map[string]map[string]emulator.StringOrSlice{
						"StringLike": {key: {"arn:aws:ec2:*"}},
					}),
			)
			err := f.launch(t, subnetlessLaunch())
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("an Allow gated on an absent %s permitted the launch", key)
			}
			assert.Equal(t, ec2AuthzENIARN, deniedResource(t, err))
		})
	}
}

// TestEC2_Authz_DefaultVPCLaunchReportsWhereItLands pins the other half of the
// absent-key rule: a subnetless launch whose default VPC *does* exist reports both
// keys, because substrate resolved where the instance will land before deciding.
//
// This is what makes AWS's guardrail hold for the launch shape every getting-started
// example uses — the one #673 found defeating a subnet-scoped Deny by omission.
func TestEC2_Authz_DefaultVPCLaunchReportsWhereItLands(t *testing.T) {
	for _, tc := range []struct {
		name string
		cond map[string]map[string]emulator.StringOrSlice
	}{
		{"the resolved default subnet", map[string]map[string]emulator.StringOrSlice{
			"ArnEquals": {"ec2:Subnet": {ec2AuthzDefaultSubnetARN}},
		}},
		{"the default VPC", map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {"ec2:Vpc": {ec2CondKeyDefaultVPCARN}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "omar", emulator.PolicyDocument{})
			f.putDefaultVPC(t, nil)
			f.setPolicy(t,
				ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN}, tc.cond),
				ec2CondKeyAllowAll(),
			)
			if !ec2AuthzDenied(t, f.launch(t, subnetlessLaunch())) {
				t.Fatal("a subnetless launch did not report where it lands")
			}
		})
	}
}

// TestEC2_Authz_VpcIsAnswerableWithoutASubnet covers the one state a launch can be in
// where the VPC is known and the subnet is not: a default VPC that has no default
// subnet, so the launch is about to mint one.
//
// ec2:Vpc is answerable there and ec2:Subnet is not, which is why they are resolved
// separately rather than both hanging off the subnet record.
func TestEC2_Authz_VpcIsAnswerableWithoutASubnet(t *testing.T) {
	// Only the VPC and a group inside it — putDefaultVPC's third record, the default
	// subnet, is deliberately left out.
	newFixture := func(t *testing.T, user string) *ec2AuthzFixture {
		t.Helper()
		f := newEC2AuthzFixture(t, user, emulator.PolicyDocument{})
		f.put(t, "vpc:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzDefaultVPCID, emulator.EC2VPC{
			VPCID:     ec2AuthzDefaultVPCID,
			CIDRBlock: "172.31.0.0/16",
			IsDefault: true,
			State:     "available",
			AccountID: ec2AuthzAccount,
			Region:    ec2AuthzRegion,
		})
		f.putSGInVPC(t, ec2AuthzSG, ec2AuthzDefaultVPCID)
		return f
	}

	t.Run("the subnet the launch will mint carries the VPC", func(t *testing.T) {
		f := newFixture(t, "rafiq")
		f.setPolicy(t,
			ec2CondKeyStatement("Deny", []string{ec2AuthzSubnetWildcardARN},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"ec2:Vpc": {ec2CondKeyDefaultVPCARN}},
				}),
			ec2CondKeyAllowAll(),
		)
		err := f.launch(t, subnetlessLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("subnet/* did not carry the default VPC it will be minted in")
		}
		assert.Equal(t, ec2AuthzSubnetWildcardARN, deniedResource(t, err))
	})

	t.Run("the interface carries the VPC and not a subnet", func(t *testing.T) {
		f := newFixture(t, "sanne")
		f.setPolicy(t,
			ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"ec2:Vpc": {ec2CondKeyDefaultVPCARN}},
					"Null":         {"ec2:Subnet": {"true"}},
				}),
			ec2CondKeyAllowAll(),
		)
		if !ec2AuthzDenied(t, f.launch(t, subnetlessLaunch())) {
			t.Fatal("the interface reported no VPC, or reported a subnet that does not exist")
		}
	})
}

// TestEC2_Authz_NetworkKeysReachTheBoundary pins that the keys are part of the same
// per-resource context both evaluations read, so a permission boundary written as
// AWS's guardrail bounds a launch the identity policy would permit — and, in the
// other direction, does not bound the launch it names.
func TestEC2_Authz_NetworkKeysReachTheBoundary(t *testing.T) {
	bounded := func(t *testing.T, user, subnetID string) error {
		t.Helper()
		f := newEC2AuthzFixture(t, user, emulator.PolicyDocument{})
		f.putSubnetInVPC(t, ec2AuthzSubnet, ec2CondKeyVPCID)
		f.setPolicy(t, ec2CondKeyAllowAll())
		f.setBoundary(t, emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{
				ec2CondKeyAllowAll(),
				ec2CondKeyStatement("Deny", []string{ec2AuthzENIARN},
					map[string]map[string]emulator.StringOrSlice{
						"ArnNotEquals": {"ec2:Subnet": {"arn:aws:ec2:" + ec2AuthzRegion + ":" +
							ec2AuthzAccount + ":subnet/" + subnetID}},
					}),
			},
		})
		return f.launch(t, nominalLaunch())
	}

	t.Run("a boundary naming another subnet bounds the launch", func(t *testing.T) {
		err := bounded(t, "priya", ec2CondKeyOtherSubnet)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a boundary gated on ec2:Subnet did not bound the launch")
		}
		assert.Contains(t, err.Error(), "permission boundary")
	})

	t.Run("a boundary naming the launch's own subnet does not", func(t *testing.T) {
		// The direction that distinguishes a working key from an absent one: before
		// #692 the key was missing, so ArnNotEquals compared the allowed ARN against
		// "" and matched, making AWS's guardrail a blanket deny rather than inert.
		require.NoError(t, bounded(t, "quinn", ec2AuthzSubnet))
	})
}
