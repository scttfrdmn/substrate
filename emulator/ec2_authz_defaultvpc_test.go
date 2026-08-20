package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The authorization decision for a RunInstances that omits SubnetId (#673).
//
// Such a launch does not run without a subnet: substrate resolves the default VPC's
// default subnet and default security group, creating them if the account has none.
// Until #673 that happened *after* CheckAccess, so the decision named neither — a
// subnet-scoped guardrail was defeated by leaving the parameter out, which is the one
// thing such a guardrail exists to prevent.
//
// These tests pin the resolved resources both ways: an existing default subnet is
// named by its own ID, and an account that has none yet is decided against subnet/*
// and security-group/*, because those are resources the launch is about to create —
// the same reasoning instance/* and network-interface/* already rest on.

const (
	ec2AuthzDefaultVPCID    = "vpc-0ddd88889999eeee0"
	ec2AuthzDefaultSubnetID = "subnet-0fff00001111aaaa2"
)

var ec2AuthzDefaultSubnetARN = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount +
	":subnet/" + ec2AuthzDefaultSubnetID

// putDefaultVPC gives the fixture's account a default VPC, its default subnet, and a
// default security group inside it — the three records ensureDefaultVPC would create.
//
// The security group is the fixture's own ec2AuthzSG re-put with the VPC's ID rather
// than a second group, because ec2LookupDefaultSecurityGroup takes the *first*
// security-group key in the account and only if it belongs to the VPC. Adding a
// second group would make which one resolves depend on key ordering, which is not
// what these tests are about.
//
// subnetTags are the resolved subnet's own tags, which is what an aws:ResourceTag
// condition about it is evaluated against.
func (f *ec2AuthzFixture) putDefaultVPC(t *testing.T, subnetTags map[string]string) {
	t.Helper()
	f.put(t, "vpc:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzDefaultVPCID, emulator.EC2VPC{
		VPCID:     ec2AuthzDefaultVPCID,
		CIDRBlock: "172.31.0.0/16",
		IsDefault: true,
		State:     "available",
		AccountID: ec2AuthzAccount,
		Region:    ec2AuthzRegion,
	})
	f.put(t, "subnet:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzDefaultSubnetID, emulator.EC2Subnet{
		SubnetID:  ec2AuthzDefaultSubnetID,
		VPCID:     ec2AuthzDefaultVPCID,
		CIDRBlock: "172.31.0.0/20",
		IsDefault: true,
		State:     "available",
		Tags:      ec2AuthzTags(subnetTags),
		AccountID: ec2AuthzAccount,
		Region:    ec2AuthzRegion,
	})
	f.put(t, "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzSG, emulator.EC2SecurityGroup{
		GroupID: ec2AuthzSG,
		VPCID:   ec2AuthzDefaultVPCID,
	})
}

// subnetlessLaunch is a launch naming an AMI and nothing else — the shape every
// getting-started example and most CDK-generated launches take.
func subnetlessLaunch() map[string]string {
	return map[string]string{
		"ImageId":      ec2AuthzAMI,
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
	}
}

// TestEC2_Authz_DefaultSubnetDenyBlocksSubnetlessLaunch is the decisive test: the
// false allow #673 reports.
//
// "Launch anywhere except the default subnet" is the guardrail, and omitting SubnetId
// was the way around it — the launch landed in the very subnet the Deny named,
// because the decision was made before the subnet was resolved.
func TestEC2_Authz_DefaultSubnetDenyBlocksSubnetlessLaunch(t *testing.T) {
	f := newEC2AuthzFixture(t, "tariq", emulator.PolicyDocument{})
	f.putDefaultVPC(t, nil)
	f.setPolicy(t,
		ec2AuthzStatement("Allow", "*"),
		ec2AuthzStatement("Deny", ec2AuthzDefaultSubnetARN),
	)

	err := f.launch(t, subnetlessLaunch())
	if !ec2AuthzDenied(t, err) {
		t.Fatal("omitting SubnetId was a way around a Deny on the subnet the launch lands in")
	}
	assert.Equal(t, ec2AuthzDefaultSubnetARN, deniedResource(t, err),
		"the denial names the default subnet, resolved from state before the decision")
}

// TestEC2_Authz_DefaultVPCLeastPrivilegeAllows is the other direction: a policy
// naming the resolved default subnet and security group has to permit the launch, or
// the fix would make the ordinary subnet-less launch unrunnable under least
// privilege.
func TestEC2_Authz_DefaultVPCLeastPrivilegeAllows(t *testing.T) {
	f := newEC2AuthzFixture(t, "ursula", emulator.PolicyDocument{})
	f.putDefaultVPC(t, nil)
	f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzDefaultSubnetARN,
		ec2AuthzSGARN, ec2AuthzENIARN, ec2AuthzInstARN))

	require.NoError(t, f.launch(t, subnetlessLaunch()),
		"a policy naming the resolved default subnet and group must permit the launch")
}

// TestEC2_Authz_DefaultSecurityGroupIsNamed pins that the group the launch will
// attach is part of the decision too, not only the subnet.
//
// The handler attaches the default VPC's security group to a launch that named none,
// so a policy that fences off that group has to refuse the launch. Naming the subnet
// and omitting the group is exactly the mistake a least-privilege policy makes, and
// the denial has to say which ARN is missing.
func TestEC2_Authz_DefaultSecurityGroupIsNamed(t *testing.T) {
	f := newEC2AuthzFixture(t, "vera", emulator.PolicyDocument{})
	f.putDefaultVPC(t, nil)
	f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzDefaultSubnetARN,
		ec2AuthzENIARN, ec2AuthzInstARN))

	err := f.launch(t, subnetlessLaunch())
	if !ec2AuthzDenied(t, err) {
		t.Fatal("a policy omitting the default security group allowed a launch that attaches it")
	}
	assert.Equal(t, ec2AuthzSGARN, deniedResource(t, err))
}

// TestEC2_Authz_NamedGroupsSuppressTheDefaultGroup pins that a launch naming its own
// security groups is not additionally authorized against the default one.
//
// The handler attaches what the request named and nothing else, so a decision that
// also demanded the default group would require an ARN the launch never touches.
func TestEC2_Authz_NamedGroupsSuppressTheDefaultGroup(t *testing.T) {
	const ownSG = "sg-0111222233334444a"
	ownARN := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":security-group/" + ownSG

	f := newEC2AuthzFixture(t, "walid", emulator.PolicyDocument{})
	f.putDefaultVPC(t, nil)
	f.putSecurityGroup(t, ownSG, nil)
	params := subnetlessLaunch()
	params["SecurityGroupId.1"] = ownSG

	f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzDefaultSubnetARN,
		ownARN, ec2AuthzENIARN, ec2AuthzInstARN))
	require.NoError(t, f.launch(t, params),
		"the group the request named is the only one required")

	// And a Deny on the group the launch does *not* attach must not block it.
	f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", ec2AuthzSGARN))
	require.NoError(t, f.launch(t, params),
		"a Deny on the default group must not block a launch that named its own")
}

// TestEC2_Authz_FreshAccountResolvesToWildcards covers the case substrate's fixtures
// all start in: no default VPC exists, so the launch will create one.
//
// This is not the skip-don't-widen case. That rule is about a resource the request
// omits; this is a resource the request *creates*, which is why instance/* and
// network-interface/* are already wildcards. Without it the first launch in a fresh
// account escapes the guardrail every later launch is subject to.
func TestEC2_Authz_FreshAccountResolvesToWildcards(t *testing.T) {
	t.Run("a Deny on subnet/* blocks the launch", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "xena", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"),
			ec2AuthzStatement("Deny", ec2AuthzSubnetWildcardARN))

		err := f.launch(t, subnetlessLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a Deny on subnet/* did not block a launch that will create a default subnet")
		}
		assert.Equal(t, ec2AuthzSubnetWildcardARN, deniedResource(t, err))
	})

	t.Run("a Deny on security-group/* blocks the launch", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "xena", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"),
			ec2AuthzStatement("Deny", ec2AuthzSGWildcardARN))

		err := f.launch(t, subnetlessLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a Deny on security-group/* did not block a launch that will create one")
		}
		assert.Equal(t, ec2AuthzSGWildcardARN, deniedResource(t, err))
	})

	t.Run("a policy scoped to one subnet does not permit it", func(t *testing.T) {
		// The fail-before this issue turns on. A policy allowing exactly one subnet
		// permitted a subnet-less launch, because the launch resolved to "*" and "*"
		// as a request resource matches only a statement whose own Resource starts
		// with "*". Now it is decided against subnet/*, which the specific ARN does
		// not match — the honest answer, since the launch will mint a different subnet.
		f := newEC2AuthzFixture(t, "yusuf", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))

		err := f.launch(t, subnetlessLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a policy allowing only one subnet permitted a launch that omits SubnetId")
		}
		assert.Equal(t, ec2AuthzSubnetWildcardARN, deniedResource(t, err))
	})

	t.Run("a launch naming its own groups is not decided against security-group/*", func(t *testing.T) {
		// The fresh-account mirror of TestEC2_Authz_NamedGroupsSuppressTheDefaultGroup.
		// A launch that named its own groups attaches those and no others, so no default
		// group is created and the decision must not demand one.
		//
		// A least-privilege Allow is what shows this, not a Deny on security-group/*: that
		// pattern matches the named group's own ARN too, so it would refuse the launch
		// either way. Naming the group and not the wildcard distinguishes them, because a
		// specific ARN in a statement does not match a request resource of
		// security-group/*.
		const ownSG = "sg-0555666677778888b"
		ownARN := "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":security-group/" + ownSG
		f := newEC2AuthzFixture(t, "xena", emulator.PolicyDocument{})
		f.putSecurityGroup(t, ownSG, nil)
		params := subnetlessLaunch()
		params["SecurityGroupId.1"] = ownSG

		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzSubnetWildcardARN,
			ownARN, ec2AuthzENIARN, ec2AuthzInstARN))
		require.NoError(t, f.launch(t, params),
			"the group the request named is the only one required, default VPC or not")
	})

	t.Run("naming both wildcards permits it", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "yusuf", emulator.PolicyDocument{})
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzSubnetWildcardARN,
			ec2AuthzSGWildcardARN, ec2AuthzENIARN, ec2AuthzInstARN))
		require.NoError(t, f.launch(t, subnetlessLaunch()))
	})
}

// TestEC2_Authz_DefaultVPCWithoutADefaultSubnet covers the state ensureDefaultVPC can
// leave behind: the VPC exists but its default subnet does not, so the launch mints
// the subnet and keeps the VPC.
//
// The subnet is therefore the wildcard while the security group — which does exist
// alongside the VPC — is named. Resolving both to wildcards would let a Deny on the
// existing group go unmatched.
func TestEC2_Authz_DefaultVPCWithoutADefaultSubnet(t *testing.T) {
	newFixture := func(t *testing.T) *ec2AuthzFixture {
		t.Helper()
		f := newEC2AuthzFixture(t, "zora", emulator.PolicyDocument{})
		f.put(t, "vpc:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzDefaultVPCID, emulator.EC2VPC{
			VPCID:     ec2AuthzDefaultVPCID,
			IsDefault: true,
			State:     "available",
			AccountID: ec2AuthzAccount,
			Region:    ec2AuthzRegion,
		})
		f.put(t, "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzSG, emulator.EC2SecurityGroup{
			GroupID: ec2AuthzSG,
			VPCID:   ec2AuthzDefaultVPCID,
		})
		return f
	}

	t.Run("the subnet is the wildcard and the group is named", func(t *testing.T) {
		f := newFixture(t)
		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzImageARN, ec2AuthzSubnetWildcardARN,
			ec2AuthzSGARN, ec2AuthzENIARN, ec2AuthzInstARN))
		require.NoError(t, f.launch(t, subnetlessLaunch()))
	})

	t.Run("a Deny on the existing default group still matches", func(t *testing.T) {
		f := newFixture(t)
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"), ec2AuthzStatement("Deny", ec2AuthzSGARN))
		err := f.launch(t, subnetlessLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a Deny on the default VPC's security group did not block the launch")
		}
		assert.Equal(t, ec2AuthzSGARN, deniedResource(t, err))
	})
}

// TestEC2_Authz_DefaultVPCYieldsToEverySubnetSource pins the precedence: the default
// VPC applies only to a launch nothing else gave a subnet.
//
// resolveDefaultVPC runs last for exactly this reason, and the handler applies the
// same order — a decision made about the default subnet for a launch that lands in a
// template's would be a false denial in one direction and a false allow in the other.
func TestEC2_Authz_DefaultVPCYieldsToEverySubnetSource(t *testing.T) {
	// The default subnet exists, so if precedence were wrong the decision would name
	// it rather than the subnet each launch actually uses.
	t.Run("a request-named subnet wins", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "abel", emulator.PolicyDocument{})
		f.putDefaultVPC(t, nil)
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"),
			ec2AuthzStatement("Deny", ec2AuthzDefaultSubnetARN))
		require.NoError(t, f.launch(t, nominalLaunch()),
			"a Deny on the default subnet must not block a launch that named another")
	})

	t.Run("a nested interface's subnet wins", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "abel", emulator.PolicyDocument{})
		f.putDefaultVPC(t, nil)
		f.setPolicy(t, ec2AuthzStatement("Allow", "*"),
			ec2AuthzStatement("Deny", ec2AuthzDefaultSubnetARN))
		require.NoError(t, f.launch(t, map[string]string{
			"ImageId":                        ec2AuthzAMI,
			"MinCount":                       "1",
			"MaxCount":                       "1",
			"NetworkInterface.1.DeviceIndex": "0",
			"NetworkInterface.1.SubnetId":    ec2AuthzSubnet,
			"NetworkInterface.1.Groups.1":    ec2AuthzSG,
		}), "a Deny on the default subnet must not block a launch whose interface named another")
	})

	t.Run("a launch template's subnet wins", func(t *testing.T) {
		// The regression this guards against is structural: the template merge used to
		// return early, and putting the default-VPC lookup inside it would have skipped
		// the lookup for a template-less launch. Splitting the two made this case and
		// the wildcard cases above both reachable.
		const ltID = "lt-0eee3333ffff4444d"
		f := newEC2AuthzFixture(t, "abel", emulator.PolicyDocument{})
		f.putDefaultVPC(t, nil)
		f.put(t, "lt:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ltID, emulator.EC2LaunchTemplate{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: "into-a-named-subnet",
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
		})
		params := map[string]string{
			"MinCount":                        "1",
			"MaxCount":                        "1",
			"LaunchTemplate.LaunchTemplateId": ltID,
		}

		f.setPolicy(t, ec2AuthzStatement("Allow", "*"),
			ec2AuthzStatement("Deny", ec2AuthzDefaultSubnetARN))
		require.NoError(t, f.launch(t, params),
			"a Deny on the default subnet must not block a launch whose template named another")

		f.setPolicy(t, ec2AuthzStatement("Allow", ec2AuthzAllFive()...))
		require.NoError(t, f.launch(t, params),
			"the template's own subnet is the one the decision names")
	})
}

// TestEC2_Authz_DefaultSubnetTagsAreItsOwn pins that the resolved subnet arrives with
// its tags, not bare.
//
// Resolving the ARN without the tags would leave aws:ResourceTag/* absent for it, so
// an ABAC policy would deny every subnet-less launch — the false deny that is the
// mirror image of #673's false allow.
func TestEC2_Authz_DefaultSubnetTagsAreItsOwn(t *testing.T) {
	doc := newABACPolicy("Allow", "ec2:RunInstances", "*", "aws:ResourceTag/Env", "test")

	t.Run("a matching tag on the resolved subnet allows", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "bianca", doc)
		f.putDefaultVPC(t, map[string]string{"Env": "test"})
		f.putImage(t, ec2AuthzAMI, map[string]string{"Env": "test"})
		f.put(t, "sg:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ec2AuthzSG, emulator.EC2SecurityGroup{
			GroupID: ec2AuthzSG,
			VPCID:   ec2AuthzDefaultVPCID,
			Tags:    ec2AuthzTags(map[string]string{"Env": "test"}),
		})
		f.setPolicy(t,
			emulator.PolicyStatement{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"ec2:RunInstances"},
				Resource: emulator.StringOrSlice{ec2AuthzImageARN, ec2AuthzDefaultSubnetARN, ec2AuthzSGARN},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:ResourceTag/Env": {"test"}},
				},
			},
			ec2AuthzStatement("Allow", ec2AuthzENIARN, ec2AuthzInstARN),
		)
		require.NoError(t, f.launch(t, subnetlessLaunch()))
	})

	t.Run("a mismatched tag on the resolved subnet denies", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "bianca", doc)
		f.putDefaultVPC(t, map[string]string{"Env": "prod"})
		f.putImage(t, ec2AuthzAMI, map[string]string{"Env": "test"})
		f.setPolicy(t,
			emulator.PolicyStatement{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"ec2:RunInstances"},
				Resource: emulator.StringOrSlice{ec2AuthzImageARN, ec2AuthzDefaultSubnetARN, ec2AuthzSGARN},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:ResourceTag/Env": {"test"}},
				},
			},
			ec2AuthzStatement("Allow", ec2AuthzENIARN, ec2AuthzInstARN),
		)
		err := f.launch(t, subnetlessLaunch())
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a default subnet tagged Env=prod satisfied a condition requiring Env=test")
		}
		assert.Equal(t, ec2AuthzDefaultSubnetARN, deniedResource(t, err))
	})
}
