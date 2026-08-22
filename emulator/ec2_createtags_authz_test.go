package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The second authorization pass AWS performs on ec2:CreateTags when a
// resource-creating request applies tags (#691).
//
// AWS: "To enable users to tag resources on creation, they must have permissions
// to use the action that creates the resource, such as ec2:RunInstances or
// ec2:CreateVolume. If tags are specified in the resource-creating action, Amazon
// performs additional authorization on the ec2:CreateTags action to verify if
// users have permissions to create tags. Therefore, users must also have explicit
// permissions to use the ec2:CreateTags action." And the converse: "The
// ec2:CreateTags action is only evaluated if tags are applied during the
// resource-creating action."
//
// Before this, a tagged create was authorized as the creating action alone, so
// every policy AWS documents for tag-on-create — all of which scope a separate
// ec2:CreateTags statement with ec2:CreateAction — permitted more than it says: a
// caller holding only ec2:RunInstances could tag whatever the request named.

// ec2CreateTagsVolumeARN and ec2CreateTagsAnyARN are the two other ARNs the second
// pass names in these tests: the wildcard for a declared resource type that is not
// instance, and the */* form AWS's own examples write as the Resource of an
// ec2:CreateTags statement.
var (
	ec2CreateTagsVolumeARN = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":volume/*"
	ec2CreateTagsAnyARN    = "arn:aws:ec2:" + ec2AuthzRegion + ":" + ec2AuthzAccount + ":*/*"
)

// ec2CreateTagsStatement builds one statement over an arbitrary action, with an
// optional condition block — the shape AWS's tag-on-create examples are written in,
// which ec2AuthzStatement cannot express because it hardcodes ec2:RunInstances.
func ec2CreateTagsStatement(effect, action string, resources []string,
	cond map[string]map[string]emulator.StringOrSlice) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:    effect,
		Action:    emulator.StringOrSlice{action},
		Resource:  emulator.StringOrSlice(resources),
		Condition: cond,
	}
}

// ec2CreateActionAllow is AWS's documented tagging grant: ec2:CreateTags on the
// given resources, but only in the context of the named creating action.
func ec2CreateActionAllow(createAction string, resources ...string) emulator.PolicyStatement {
	return ec2CreateTagsStatement("Allow", "ec2:CreateTags", resources,
		map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {"ec2:CreateAction": {createAction}},
		})
}

// ec2CreateTagsParams merges extra params into a copy of base, so a table row can
// add a tag specification to nominalLaunch without mutating it.
func ec2CreateTagsParams(base map[string]string, extra ...map[string]string) map[string]string {
	out := make(map[string]string, len(base)+4)
	for k, v := range base {
		out[k] = v
	}
	for _, m := range extra {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// ec2TagSpecParams returns the TagSpecification.N params that scope kv pairs to
// resourceType, in the wire spelling EC2's query protocol uses.
func ec2TagSpecParams(n int, resourceType string, kv ...string) map[string]string {
	spec := "TagSpecification." + strconv.Itoa(n)
	out := map[string]string{spec + ".ResourceType": resourceType}
	for i := 0; i+1 < len(kv); i += 2 {
		tag := spec + ".Tag." + strconv.Itoa(i/2+1)
		out[tag+".Key"] = kv[i]
		out[tag+".Value"] = kv[i+1]
	}
	return out
}

// deniedActionNamed reports the action a denial names, which is what tells a caller
// the tagging pass refused rather than the create itself.
func deniedActionNamed(t *testing.T, err error) string {
	t.Helper()
	var awsErr *emulator.AWSError
	if !errors.As(err, &awsErr) {
		t.Fatalf("expected an *AWSError, got %T: %v", err, err)
	}
	const marker = "to perform: "
	idx := strings.Index(awsErr.Message, marker)
	if idx < 0 {
		t.Fatalf("denial names no action: %q", awsErr.Message)
	}
	rest := awsErr.Message[idx+len(marker):]
	if end := strings.Index(rest, " "); end >= 0 {
		return rest[:end]
	}
	return rest
}

// setBoundary attaches a permission boundary to the fixture's user, which is how a
// boundary reaches CheckAccess: it is read off the IAM entity, not from the
// attached-policy list.
func (f *ec2AuthzFixture) setBoundary(t *testing.T, doc emulator.PolicyDocument) {
	t.Helper()
	arn := "arn:aws:iam::" + ec2AuthzAccount + ":policy/Boundary-" + f.user
	pol := emulator.IAMPolicy{
		PolicyName:       "boundary",
		PolicyID:         "ANPABOUND",
		ARN:              arn,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         doc,
	}
	raw, err := json.Marshal(pol)
	if err != nil {
		t.Fatalf("marshal boundary: %v", err)
	}
	if err := f.state.Put(context.Background(), "iam", "policy:"+arn, raw); err != nil { //nolint:contextcheck
		t.Fatalf("store boundary: %v", err)
	}
	user := emulator.IAMUser{
		UserName:            f.user,
		UserID:              "AIDATEST",
		ARN:                 "arn:aws:iam::" + ec2AuthzAccount + ":user/" + f.user,
		Path:                "/",
		PermissionsBoundary: &emulator.IAMAttachedPolicy{PolicyARN: arn, PolicyName: "boundary"},
	}
	userRaw, err := json.Marshal(user)
	if err != nil {
		t.Fatalf("marshal user: %v", err)
	}
	if err := f.state.Put(context.Background(), "iam", "user:"+f.user, userRaw); err != nil { //nolint:contextcheck
		t.Fatalf("store user: %v", err)
	}
}

// TestEC2_Authz_TaggedCreateRequiresCreateTags is the decisive test: a caller
// holding the creating action alone cannot tag on create.
//
// AWS: "users must also have explicit permissions to use the ec2:CreateTags
// action." The policy here is the one a least-privilege consumer writes for a
// tagging launch and, before #691, it permitted the tagging too.
func TestEC2_Authz_TaggedCreateRequiresCreateTags(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation string
		params    map[string]string
		wantARN   string // "" means the request must be allowed
	}{
		{
			name:      "a launch tagging its instance",
			operation: "RunInstances",
			params:    ec2TagSpecParams(1, "instance", "Env", "prod"),
			wantARN:   ec2AuthzInstARN,
		},
		{
			name:      "a launch tagging its volumes",
			operation: "RunInstances",
			params:    ec2TagSpecParams(1, "volume", "Env", "prod"),
			wantARN:   ec2CreateTagsVolumeARN,
		},
		{
			name:      "a volume created with tags",
			operation: "CreateVolume",
			params:    ec2TagSpecParams(1, "volume", "Env", "prod"),
			wantARN:   ec2CreateTagsVolumeARN,
		},
		{
			// AWS: "a user that has permissions to create a resource … does not
			// require permissions to use the ec2:CreateTags action if no tags are
			// specified in the request."
			name:      "an untagged launch",
			operation: "RunInstances",
		},
		{
			name:      "an untagged volume",
			operation: "CreateVolume",
		},
		{
			// A specification that declares a type and applies no tag applies no
			// tags, so there is nothing for the tagging pass to authorize.
			name:      "a tag specification carrying no tags",
			operation: "RunInstances",
			params:    ec2TagSpecParams(1, "instance"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "tori", emulator.PolicyDocument{})
			f.setPolicy(t,
				ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
				ec2CreateTagsStatement("Allow", "ec2:CreateVolume", []string{"*"}, nil),
			)

			base := map[string]string{}
			if tc.operation == "RunInstances" {
				base = nominalLaunch()
			}
			err := f.call(t, tc.operation, ec2CreateTagsParams(base, tc.params))

			if tc.wantARN == "" {
				require.NoError(t, err, "a create applying no tags needs no ec2:CreateTags")
				return
			}
			if !ec2AuthzDenied(t, err) {
				t.Fatalf("a %s applying tags was allowed without ec2:CreateTags", tc.operation)
			}
			assert.Equal(t, "ec2:CreateTags", deniedActionNamed(t, err),
				"the denial names the action that was missing, not the create")
			assert.Equal(t, tc.wantARN, deniedResource(t, err),
				"the tagging pass is authorized against the declared resource type's wildcard")
		})
	}
}

// TestEC2_Authz_CreateActionScopesTheTaggingGrant runs AWS's own documented
// tag-on-create policy, which is the reason ec2:CreateAction exists.
//
// AWS: "The second statement uses the ec2:CreateAction condition key to allow
// users to create tags only in the context of RunInstances, and only for
// instances. Users cannot tag existing resources, and users cannot tag volumes
// using the RunInstances request".
func TestEC2_Authz_CreateActionScopesTheTaggingGrant(t *testing.T) {
	statements := []emulator.PolicyStatement{
		ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
		ec2CreateActionAllow("RunInstances", ec2AuthzInstARN),
	}

	t.Run("tagging the instance during the launch is allowed", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "ulla", emulator.PolicyDocument{})
		f.setPolicy(t, statements...)
		require.NoError(t, f.call(t, "RunInstances",
			ec2CreateTagsParams(nominalLaunch(), ec2TagSpecParams(1, "instance", "Env", "prod"))))
	})

	t.Run("tagging the volumes in the same launch is not", func(t *testing.T) {
		// The prose above is explicit, and it turns on the resource: the grant names
		// instance/*, so the volume scope of the same request has no statement.
		f := newEC2AuthzFixture(t, "ulla", emulator.PolicyDocument{})
		f.setPolicy(t, statements...)
		err := f.call(t, "RunInstances", ec2CreateTagsParams(nominalLaunch(),
			ec2TagSpecParams(1, "instance", "Env", "prod"),
			ec2TagSpecParams(2, "volume", "Env", "prod")))
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a grant scoped to instance/* permitted tagging volumes")
		}
		assert.Equal(t, ec2CreateTagsVolumeARN, deniedResource(t, err))
	})

	t.Run("tagging an existing instance is not", func(t *testing.T) {
		// The whole point of the key: a direct CreateTags carries no ec2:CreateAction,
		// so a grant conditioned on it cannot be satisfied outside a create.
		f := newEC2AuthzFixture(t, "ulla", emulator.PolicyDocument{})
		f.putTagInstance(t, nil)
		f.setPolicy(t, statements...)
		err := f.call(t, "CreateTags", map[string]string{
			"ResourceId.1": ec2TagAuthzInstance,
			"Tag.1.Key":    "Env",
			"Tag.1.Value":  "prod",
		})
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a grant conditioned on ec2:CreateAction permitted standalone tagging")
		}
	})

	t.Run("the value is the creating operation and is case-sensitive", func(t *testing.T) {
		// AWS: "the condition key is not case-sensitive and the condition value is
		// case-sensitive." A grant written for CreateVolume must not admit a launch.
		f := newEC2AuthzFixture(t, "ulla", emulator.PolicyDocument{})
		f.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateActionAllow("CreateVolume", ec2CreateTagsAnyARN),
		)
		err := f.call(t, "RunInstances",
			ec2CreateTagsParams(nominalLaunch(), ec2TagSpecParams(1, "instance", "Env", "prod")))
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a grant written for CreateVolume admitted a RunInstances")
		}
	})

	t.Run("the key's own name is not case-sensitive", func(t *testing.T) {
		// AWS: "Context key *names* are not case-sensitive. For example, including the
		// aws:SourceIP context key is equivalent to testing for AWS:SourceIp."
		//
		// This ran on the enforcement path rather than through Evaluate directly,
		// because that is where the gap was found (#691) and where it mattered: a real
		// policy naming "ec2:createaction" was silently unmatched, so AWS's own
		// tag-on-create grant was a false refusal and the same grant written as a Deny
		// was inert. #704 closed it; this subtest replaces the comment that recorded it.
		for _, spelling := range []string{"ec2:createaction", "EC2:CreateAction"} {
			t.Run(spelling, func(t *testing.T) {
				f := newEC2AuthzFixture(t, "ulla", emulator.PolicyDocument{})
				f.setPolicy(t,
					ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
					ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2AuthzInstARN},
						map[string]map[string]emulator.StringOrSlice{
							"StringEquals": {spelling: {"RunInstances"}},
						}),
				)
				require.NoError(t, f.call(t, "RunInstances",
					ec2CreateTagsParams(nominalLaunch(), ec2TagSpecParams(1, "instance", "Env", "prod"))),
					"a grant naming the key as %q must be evaluated", spelling)
			})
		}
	})
}

// TestEC2_Authz_CreateActionIsAbsentOutsideACreate pins the key's absence, which
// is the half AWS's examples depend on and which no positive test can show.
//
// A Deny gated on Null:false fires only when the key is present, so it separates
// "absent" from "present with some other value" — the distinction a StringEquals
// cannot make.
func TestEC2_Authz_CreateActionIsAbsentOutsideACreate(t *testing.T) {
	statements := []emulator.PolicyStatement{
		ec2CreateTagsStatement("Allow", "ec2:*", []string{"*"}, nil),
		ec2CreateTagsStatement("Deny", "ec2:CreateTags", []string{"*"},
			map[string]map[string]emulator.StringOrSlice{
				"Null": {"ec2:CreateAction": {"false"}},
			}),
	}

	t.Run("a direct CreateTags", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "vera", emulator.PolicyDocument{})
		f.putTagInstance(t, nil)
		f.setPolicy(t, statements...)
		require.NoError(t, f.call(t, "CreateTags", map[string]string{
			"ResourceId.1": ec2TagAuthzInstance,
			"Tag.1.Key":    "Env",
			"Tag.1.Value":  "prod",
		}), "a standalone tagging call must carry no ec2:CreateAction")
	})

	t.Run("a direct DeleteTags", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "vera", emulator.PolicyDocument{})
		f.putTagInstance(t, nil)
		f.setPolicy(t, statements...)
		require.NoError(t, f.call(t, "DeleteTags", map[string]string{
			"ResourceId.1": ec2TagAuthzInstance,
			"Tag.1.Key":    "Env",
		}))
	})

	t.Run("a tagged launch", func(t *testing.T) {
		f := newEC2AuthzFixture(t, "vera", emulator.PolicyDocument{})
		f.setPolicy(t, statements...)
		err := f.call(t, "RunInstances",
			ec2CreateTagsParams(nominalLaunch(), ec2TagSpecParams(1, "instance", "Env", "prod")))
		if !ec2AuthzDenied(t, err) {
			t.Fatal("the tagging pass of a tagged create carried no ec2:CreateAction")
		}
		assert.Equal(t, "ec2:CreateTags", deniedActionNamed(t, err))
	})
}

// TestEC2_Authz_TaggingPassSeesTheRequestTags runs AWS's "if instances are tagged
// on creation, they must be tagged with a specific tag" policy, which conditions
// the tagging grant on aws:RequestTag and aws:TagKeys rather than on the resource.
//
// AWS: "users do not have to specify tags in the request, but if they do, the tag
// must be purpose=test. No other tags are allowed".
func TestEC2_Authz_TaggingPassSeesTheRequestTags(t *testing.T) {
	statements := []emulator.PolicyStatement{
		ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
		ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2CreateTagsAnyARN},
			map[string]map[string]emulator.StringOrSlice{
				"StringEquals": {
					"aws:RequestTag/purpose": {"test"},
					"ec2:CreateAction":       {"RunInstances"},
				},
				"ForAllValues:StringEquals": {"aws:TagKeys": {"purpose"}},
			}),
	}

	for _, tc := range []struct {
		name    string
		params  map[string]string
		allowed bool
	}{
		{
			name:    "the prescribed tag",
			params:  ec2TagSpecParams(1, "instance", "purpose", "test"),
			allowed: true,
		},
		{
			name:   "the prescribed key with another value",
			params: ec2TagSpecParams(1, "instance", "purpose", "prod"),
		},
		{
			name:   "the prescribed tag plus one more",
			params: ec2TagSpecParams(1, "instance", "purpose", "test", "Env", "prod"),
		},
		{
			name:    "no tags at all",
			allowed: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "wanda", emulator.PolicyDocument{})
			f.setPolicy(t, statements...)
			err := f.call(t, "RunInstances", ec2CreateTagsParams(nominalLaunch(), tc.params))
			if tc.allowed {
				require.NoError(t, err)
				return
			}
			if !ec2AuthzDenied(t, err) {
				t.Fatal("the tagging pass ignored the tags the request applies")
			}
		})
	}
}

// TestEC2_Authz_TemplateSuppliedTagsAreAuthorized pins AWS's other half: "The
// ec2:CreateTags action is also evaluated if tags are provided in a launch
// template."
//
// The template is resolved for the decision exactly as it is for the launch, under
// the same replace-rather-than-merge precedence, so the tags authorized are the
// ones the launch will actually apply.
func TestEC2_Authz_TemplateSuppliedTagsAreAuthorized(t *testing.T) {
	const ltID = "lt-0ddd3333eeee4444f"
	const ltName = "tags-from-template"

	newFixture := func(t *testing.T, data emulator.EC2LaunchTemplateData) *ec2AuthzFixture {
		t.Helper()
		f := newEC2AuthzFixture(t, "xena", emulator.PolicyDocument{})
		data.ImageID = ec2AuthzAMI
		data.SubnetID = ec2AuthzSubnet
		data.NetworkInterfaceGroups = []string{ec2AuthzSG}
		f.put(t, "lt:"+ec2AuthzAccount+"/"+ec2AuthzRegion+"/"+ltID, emulator.EC2LaunchTemplate{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: ltName,
			DefaultVersionNum:  1,
			LatestVersionNum:   1,
			Versions: []emulator.EC2LaunchTemplateVersion{{
				VersionNumber: 1,
				Data:          data,
			}},
		})
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
	instanceTags := emulator.EC2LaunchTemplateData{
		TagSpecifications: []emulator.EC2Tag{{Key: "Env", Value: "tmpl"}},
	}

	t.Run("a template's instance tags require ec2:CreateTags", func(t *testing.T) {
		f := newFixture(t, instanceTags)
		f.setPolicy(t, ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil))
		err := f.launch(t, byName)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a template was a way to tag on create without ec2:CreateTags")
		}
		assert.Equal(t, "ec2:CreateTags", deniedActionNamed(t, err))
		assert.Equal(t, ec2AuthzInstARN, deniedResource(t, err))
	})

	t.Run("the grant AWS documents for a tagging template permits it", func(t *testing.T) {
		// AWS's launch-template example: "The second part of the statement allows
		// users to tag instances on creation—this part of the statement is necessary
		// if tags are specified for the instance in the launch template."
		f := newFixture(t, instanceTags)
		f.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateActionAllow("RunInstances", ec2AuthzInstARN),
		)
		require.NoError(t, f.launch(t, byName))
	})

	t.Run("a template's volume tags name the volume wildcard", func(t *testing.T) {
		f := newFixture(t, emulator.EC2LaunchTemplateData{
			VolumeTagSpecifications: []emulator.EC2Tag{{Key: "Env", Value: "tmpl"}},
		})
		f.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateActionAllow("RunInstances", ec2AuthzInstARN),
		)
		err := f.launch(t, byName)
		if !ec2AuthzDenied(t, err) {
			t.Fatal("a template's volume tags were authorized as instance tags")
		}
		assert.Equal(t, ec2CreateTagsVolumeARN, deniedResource(t, err))
	})

	t.Run("the request's own tags replace the template's", func(t *testing.T) {
		// The handler applies a template's tag scope only when the request named
		// none, so the decision must too: a launch naming Env=req applies Env=req,
		// and a grant that admits only that value has to permit it.
		f := newFixture(t, instanceTags)
		f.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2CreateTagsAnyARN},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:RequestTag/Env": {"req"}},
				}),
		)
		require.NoError(t, f.launch(t,
			ec2CreateTagsParams(byName, ec2TagSpecParams(1, "instance", "Env", "req"))))

		// And the same policy refuses the template's own value, which is what shows
		// the tags in the decision came from the template when the request named none.
		g := newFixture(t, instanceTags)
		g.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2CreateTagsAnyARN},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:RequestTag/Env": {"req"}},
				}),
		)
		if !ec2AuthzDenied(t, g.launch(t, byName)) {
			t.Fatal("the template's Env=tmpl was not in the tagging decision")
		}
	})

	t.Run("a template's own values are what the tagging pass evaluates", func(t *testing.T) {
		// The mirror of the case above, and the one that shows the template's tags
		// reach aws:RequestTag/* rather than merely making the pass happen: a grant
		// admitting only Env=tmpl has to permit a launch whose Env=tmpl comes from
		// the template alone.
		f := newFixture(t, instanceTags)
		f.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2CreateTagsAnyARN},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:RequestTag/Env": {"tmpl"}},
				}),
		)
		require.NoError(t, f.launch(t, byName))
	})

	t.Run("a request's volume tags keep the template's out of the decision", func(t *testing.T) {
		// Precedence is per scope, and the scope the request overrode must not
		// contribute the template's keys: AWS's own approved-keys guardrail would
		// otherwise refuse a launch over a tag the launch never applies.
		f := newFixture(t, emulator.EC2LaunchTemplateData{
			VolumeTagSpecifications: []emulator.EC2Tag{{Key: "Team", Value: "tmpl"}},
		})
		f.setPolicy(t,
			ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil),
			ec2CreateTagsStatement("Allow", "ec2:CreateTags", []string{ec2CreateTagsAnyARN},
				map[string]map[string]emulator.StringOrSlice{
					"ForAllValues:StringEquals": {"aws:TagKeys": {"Name"}},
					"Null":                      {"aws:TagKeys": {"false"}},
				}),
		)
		require.NoError(t, f.launch(t,
			ec2CreateTagsParams(byName, ec2TagSpecParams(1, "volume", "Name", "data"))))
	})

	t.Run("an untagged template needs no ec2:CreateTags", func(t *testing.T) {
		f := newFixture(t, emulator.EC2LaunchTemplateData{})
		f.setPolicy(t, ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil))
		require.NoError(t, f.launch(t, byName))
	})
}

// TestEC2_Authz_BoundaryAppliesToTheTaggingPass pins that the second pass is a
// full decision, not an identity-policy-only shortcut: a permission boundary that
// withholds ec2:CreateTags blocks a tagged create just as it blocks the create.
func TestEC2_Authz_BoundaryAppliesToTheTaggingPass(t *testing.T) {
	f := newEC2AuthzFixture(t, "yuri", emulator.PolicyDocument{})
	f.setPolicy(t, ec2CreateTagsStatement("Allow", "ec2:*", []string{"*"}, nil))
	f.setBoundary(t, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []emulator.PolicyStatement{ec2CreateTagsStatement("Allow", "ec2:RunInstances", []string{"*"}, nil)},
	})

	require.NoError(t, f.launch(t, nominalLaunch()),
		"the boundary allows the launch itself")

	err := f.call(t, "RunInstances",
		ec2CreateTagsParams(nominalLaunch(), ec2TagSpecParams(1, "instance", "Env", "prod")))
	if !ec2AuthzDenied(t, err) {
		t.Fatal("a boundary withholding ec2:CreateTags permitted a tagged launch")
	}
	assert.Contains(t, err.Error(), "permission boundary")
}
