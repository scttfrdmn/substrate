package emulator_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What an IAM entity read reports about its tags (#796).
//
// Tags were stored and never reported, so a consumer that tagged a role at create time and
// read it back saw permanent drift: the tags were in the record, `ListRoleTags` reported
// them, and `GetRole` did not. A CDK or Terraform run comparing desired state against the
// entity read therefore proposed the same tag change on every plan.
//
// AWS draws the line by *shape*, not by entity. The single-entity shapes — `User`, `Role`,
// `Policy`, `InstanceProfile` — each document a `Tags` member; the listing operations
// document the opposite, in the same words on all four of `ListUsers`, `ListRoles`,
// `ListPolicies` and `ListInstanceProfiles`:
//
//	IAM resource-listing operations return a subset of the available attributes for the
//	resource. This operation does not return the following attributes, even though they are
//	an attribute of the returned object: PermissionsBoundary, RoleLastUsed, Tags. To view
//	all of the information for a role, see GetRole.
//
// So the assertions below come in pairs: what a read reports, and what the list of the same
// entity deliberately does not.
//
// These tests drive the query protocol through [iamFormRequest], because `Tags.member.N` is
// how a real client sends tags — the same reason #639 exists.

// iamEntityTags returns the tags an entity-bearing IAM response reports for its wrapper
// element ("User", "Role", "Policy", "InstanceProfile"), and an empty map when it reports
// none. [iamFormTagNames] reads the *top-level* `Tags` of a ListUserTags shape; an entity
// read nests its tags one level deeper.
func iamEntityTags(t *testing.T, resp *http.Response, wrapper string) map[string]string {
	t.Helper()
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	entity, ok := result[wrapper].(map[string]any)
	require.True(t, ok, "response carries no <%s>: %v", wrapper, result)
	raw, ok := entity["Tags"].([]any)
	if !ok {
		return map[string]string{}
	}
	tags := make(map[string]string, len(raw))
	for _, entry := range raw {
		tag, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		key, _ := tag["Key"].(string)
		value, _ := tag["Value"].(string)
		tags[key] = value
	}
	return tags
}

// iamResponseXML returns a response's raw body, for an assertion a decoded map cannot make:
// whether an element is *absent* rather than present and empty.
func iamResponseXML(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return string(body)
}

// iamFormOK executes a query-protocol IAM request and requires a 200, returning the raw body.
func iamFormOK(t *testing.T, srv *emulator.Server, operation string, params map[string]string) string {
	t.Helper()
	resp := iamFormRequest(t, srv, operation, params)
	body := iamResponseXML(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", operation, body)
	return body
}

func TestIAMEntityTags_AUserReadReportsTagsSetAfterCreate(t *testing.T) {
	// The end-to-end drift: TagUser stores, ListUserTags reports, and GetUser did not.
	t.Parallel()
	srv := newIAMTestServer(t)

	iamFormOK(t, srv, "CreateUser", map[string]string{"UserName": "jill"})
	iamFormOK(t, srv, "TagUser", map[string]string{
		"UserName":            "jill",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
		"Tags.member.2.Key":   "team",
		"Tags.member.2.Value": "infra",
	})

	resp := iamFormRequest(t, srv, "GetUser", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"},
		iamEntityTags(t, resp, "User"),
		"GetUser reports the tags the record holds; ListUserTags already did")
}

func TestIAMEntityTags_ARoleReadReportsTagsSetAfterCreate(t *testing.T) {
	// The same for the other entity a consumer tags most: an ECS task role or a Lambda
	// execution role, tagged after creation by the CDK's tag aspect.
	t.Parallel()
	srv := newIAMTestServer(t)

	iamFormOK(t, srv, "CreateRole", map[string]string{
		"RoleName":                 "worker",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	iamFormOK(t, srv, "TagRole", map[string]string{
		"RoleName":            "worker",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})

	resp := iamFormRequest(t, srv, "GetRole", map[string]string{"RoleName": "worker"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "prod"}, iamEntityTags(t, resp, "Role"))
}

func TestIAMEntityTags_CreateTimeTagsAreOnTheResponseAndOnTheRead(t *testing.T) {
	// `Tags.member.N` at create time, for each of the four shapes AWS documents a `Tags`
	// member on. Policy and instance profile had nowhere to put one before #796 — their
	// records carried no field at all — so a `CreatePolicy --tags` dropped them silently.
	//
	// Both halves matter: the create response is what a consumer's client returns from the
	// call, and the read is what its next plan compares against.
	t.Parallel()

	for _, tc := range []struct {
		name         string
		create, read string
		createParams map[string]string
		readParams   map[string]string
		wrapper      string
	}{
		{
			name:         "a user",
			create:       "CreateUser",
			createParams: map[string]string{"UserName": "jill"},
			read:         "GetUser",
			readParams:   map[string]string{"UserName": "jill"},
			wrapper:      "User",
		},
		{
			name:   "a role",
			create: "CreateRole",
			createParams: map[string]string{
				"RoleName":                 "worker",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			read:       "GetRole",
			readParams: map[string]string{"RoleName": "worker"},
			wrapper:    "Role",
		},
		{
			name:   "a customer-managed policy",
			create: "CreatePolicy",
			createParams: map[string]string{
				"PolicyName": "reader",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
					`"Action":"s3:GetObject","Resource":"*"}]}`,
			},
			read: "GetPolicy",
			readParams: map[string]string{
				"PolicyArn": "arn:aws:iam::123456789012:policy/reader",
			},
			wrapper: "Policy",
		},
		{
			name:         "an instance profile",
			create:       "CreateInstanceProfile",
			createParams: map[string]string{"InstanceProfileName": "web"},
			read:         "GetInstanceProfile",
			readParams:   map[string]string{"InstanceProfileName": "web"},
			wrapper:      "InstanceProfile",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)

			params := map[string]string{
				"Tags.member.1.Key":   "env",
				"Tags.member.1.Value": "prod",
				"Tags.member.2.Key":   "owner",
				"Tags.member.2.Value": "platform",
			}
			for k, v := range tc.createParams {
				params[k] = v
			}
			want := map[string]string{"env": "prod", "owner": "platform"}

			resp := iamFormRequest(t, srv, tc.create, params)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, want, iamEntityTags(t, resp, tc.wrapper),
				"%s reports the tags it was given", tc.create)

			resp = iamFormRequest(t, srv, tc.read, tc.readParams)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, want, iamEntityTags(t, resp, tc.wrapper),
				"%s reports the tags the create stored", tc.read)
		})
	}
}

func TestIAMEntityTags_TheListShapesReportNone(t *testing.T) {
	// AWS's note, quoted at the top of this file, on all four listing operations. The
	// entities below are tagged and their reads report the tags; the lists must not, so a
	// consumer following the note reads the entity to see them.
	t.Parallel()
	srv := newIAMTestServer(t)

	tagged := map[string]string{"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"}
	create := func(operation string, params map[string]string) {
		merged := map[string]string{}
		for k, v := range tagged {
			merged[k] = v
		}
		for k, v := range params {
			merged[k] = v
		}
		iamFormOK(t, srv, operation, merged)
	}
	create("CreateUser", map[string]string{"UserName": "jill"})
	create("CreateRole", map[string]string{
		"RoleName":                 "worker",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	create("CreatePolicy", map[string]string{
		"PolicyName":     "reader",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	create("CreateInstanceProfile", map[string]string{"InstanceProfileName": "web"})

	for _, operation := range []string{
		"ListUsers", "ListRoles", "ListPolicies", "ListInstanceProfiles",
	} {
		t.Run(operation, func(t *testing.T) {
			body := iamFormOK(t, srv, operation, nil)
			assert.NotContains(t, body, "<Tags>",
				"%s reports a subset of the attributes, and Tags is not in it", operation)
			// Not vacuous: the entity is in the list, and it is the tagged one.
			assert.Contains(t, body, "<Arn>")
		})
	}

	// And the same entities do report them one call away, which is what the note promises.
	resp := iamFormRequest(t, srv, "GetUser", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "prod"}, iamEntityTags(t, resp, "User"))
}

func TestIAMEntityTags_AnUntaggedEntityHasNoTagsMember(t *testing.T) {
	// Asserted on raw XML, because a decoded map cannot tell an absent element from an
	// empty one — and the difference is the whole rule. `Tags` is `Required: No` on all four
	// data types and every untagged reference sample omits it;
	// CreateInstanceProfile's sample proves the contrast by rendering its *required* empty
	// list as `<Roles/>` while carrying no `<Tags>` at all.
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, tc := range []struct {
		name         string
		create, read string
		createParams map[string]string
		readParams   map[string]string
	}{
		{
			name: "a user", create: "CreateUser", read: "GetUser",
			createParams: map[string]string{"UserName": "bare"},
			readParams:   map[string]string{"UserName": "bare"},
		},
		{
			name: "a role", create: "CreateRole", read: "GetRole",
			createParams: map[string]string{
				"RoleName":                 "bare",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			readParams: map[string]string{"RoleName": "bare"},
		},
		{
			name: "a policy", create: "CreatePolicy", read: "GetPolicy",
			createParams: map[string]string{
				"PolicyName":     "bare",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			readParams: map[string]string{
				"PolicyArn": "arn:aws:iam::123456789012:policy/bare",
			},
		},
		{
			name:   "an instance profile",
			create: "CreateInstanceProfile", read: "GetInstanceProfile",
			createParams: map[string]string{"InstanceProfileName": "bare"},
			readParams:   map[string]string{"InstanceProfileName": "bare"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created := iamFormOK(t, srv, tc.create, tc.createParams)
			assert.NotContains(t, created, "<Tags", "%s: an untagged entity omits the member", tc.create)

			read := iamFormOK(t, srv, tc.read, tc.readParams)
			assert.NotContains(t, read, "<Tags", "%s: an untagged entity omits the member", tc.read)
		})
	}
}

func TestIAMEntityTags_AGroupNeverReportsTags(t *testing.T) {
	// A group is not a taggable resource, so its shape gains nothing here. The `Group` data
	// type documents no `Tags` member, the Actions index has no TagGroup/UntagGroup/
	// ListGroupTags, the vendored SAR snapshot publishes no `iam:*Group` tagging action, and
	// the User Guide says it directly: "You can tag most IAM resources, but not groups,
	// assumed roles, access reports, or hardware-based MFA devices."
	//
	// A Tags.member.N sent to CreateGroup anyway is ignored rather than stored: the parameter
	// is not AWS's, and storing it would report tags on a read where AWS reports none.
	t.Parallel()
	srv := newIAMTestServer(t)

	created := iamFormOK(t, srv, "CreateGroup", map[string]string{
		"GroupName":           "admins",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})
	assert.NotContains(t, created, "<Tags")

	read := iamFormOK(t, srv, "GetGroup", map[string]string{"GroupName": "admins"})
	assert.NotContains(t, read, "<Tags")
}

func TestIAMEntityTags_ARoleNestedInAnInstanceProfileCarriesNone(t *testing.T) {
	// The instance-profile shape embeds whole `Role` structures, and that nested list is a
	// list: the same note applies, and AWS's GetInstanceProfile sample renders no `<Tags>`
	// inside `<Roles>`. The profile's own tags are reported; the role's are not, even though
	// the role is tagged and GetRole reports them.
	t.Parallel()
	srv := newIAMTestServer(t)

	iamFormOK(t, srv, "CreateRole", map[string]string{
		"RoleName":                 "worker",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		"Tags.member.1.Key":        "role-tag",
		"Tags.member.1.Value":      "yes",
	})
	iamFormOK(t, srv, "CreateInstanceProfile", map[string]string{
		"InstanceProfileName": "web",
		"Tags.member.1.Key":   "profile-tag",
		"Tags.member.1.Value": "yes",
	})
	iamFormOK(t, srv, "AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "web",
		"RoleName":            "worker",
	})

	body := iamFormOK(t, srv, "GetInstanceProfile",
		map[string]string{"InstanceProfileName": "web"})
	assert.Equal(t, 1, strings.Count(body, "<Tags>"),
		"one <Tags>, the profile's own — the nested role carries none: %s", body)
	assert.Contains(t, body, "profile-tag")
	assert.NotContains(t, body, "role-tag")

	// Not vacuous: the role is there, and its own read reports the tag the profile omits.
	assert.Contains(t, body, "<RoleName>worker</RoleName>")
	resp := iamFormRequest(t, srv, "GetRole", map[string]string{"RoleName": "worker"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"role-tag": "yes"}, iamEntityTags(t, resp, "Role"))
}
