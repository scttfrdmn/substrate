package emulator_test

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What an entity's permissions boundary reports, and what a policy's AttachmentCount is derived
// from (#852, #846, #847).
//
// Two defects of one kind: substrate reported a value nothing on the receiving end could read,
// and a value nothing had written.
//
//   - `GetRole` and `GetUser` rendered the boundary as `<PolicyArn>` and `<PolicyName>`, and
//     neither name is on AWS's [AttachedPermissionsBoundary] — its two members are
//     `PermissionsBoundaryArn` and `PermissionsBoundaryType`. So an SDK decoded the element into
//     an empty struct: a consumer reading `role.PermissionsBoundary.PermissionsBoundaryArn` got
//     "" for a boundary substrate had stored and was reporting.
//   - Nothing writes `IAMPolicy.AttachmentCount`. `CreatePolicy` never set it, the bundled
//     catalog carries no value for it, and an attach writes only the entity's own ARN list — so
//     `GetPolicy` reported 0 for every policy in every state while `ListPolicies`, which derives
//     the count, reported the truth.
//
// The boundary assertions here are the decode side. The raw-XML side is in
// iam_shape_members_test.go, and both are needed: raw XML is the only way to tell an absent
// member from an empty one, and a decode is the only way to show that the names substrate emits
// are the names an SDK looks for.
//
// [AttachedPermissionsBoundary]: https://docs.aws.amazon.com/IAM/latest/APIReference/API_AttachedPermissionsBoundary.html

// iamBoundaryDoc is a minimal valid policy document, for a CreatePolicy that only has to
// produce a policy something else can name.
const iamBoundaryDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

// iamDecodedBoundary is AWS's AttachedPermissionsBoundary shape, spelled with AWS's member
// names, so a successful decode is itself the assertion an SDK makes.
type iamDecodedBoundary struct {
	PermissionsBoundaryArn  string `xml:"PermissionsBoundaryArn"`
	PermissionsBoundaryType string `xml:"PermissionsBoundaryType"`
}

// iamDecodeUserBoundary returns the boundary GetUser reports for name, decoded through AWS's
// member names.
func iamDecodeUserBoundary(t *testing.T, srv *emulator.Server, name string) iamDecodedBoundary {
	t.Helper()
	var decoded struct {
		Boundary iamDecodedBoundary `xml:"GetUserResult>User>PermissionsBoundary"`
	}
	body := iamFormRaw(t, srv, "GetUser", map[string]string{"UserName": name})
	require.NoError(t, xml.Unmarshal([]byte(body), &decoded), body)
	return decoded.Boundary
}

// iamDecodeRoleBoundary returns the boundary GetRole reports for name, decoded through AWS's
// member names.
func iamDecodeRoleBoundary(t *testing.T, srv *emulator.Server, name string) iamDecodedBoundary {
	t.Helper()
	var decoded struct {
		Boundary iamDecodedBoundary `xml:"GetRoleResult>Role>PermissionsBoundary"`
	}
	body := iamFormRaw(t, srv, "GetRole", map[string]string{"RoleName": name})
	require.NoError(t, xml.Unmarshal([]byte(body), &decoded), body)
	return decoded.Boundary
}

// TestIAMBoundary_AnSDKDecodesTheARNItAskedFor is the defect stated as its symptom: the boundary
// an SDK reads back is the one that was set, rather than an empty struct.
func TestIAMBoundary_AnSDKDecodesTheARNItAskedFor(t *testing.T) {
	srv := newIAMTestServer(t)
	const boundary = "arn:aws:iam::aws:policy/PowerUserAccess"

	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": boundary,
	})
	iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})
	iamFormRaw(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": boundary,
	})

	cases := []struct {
		name string
		got  iamDecodedBoundary
	}{
		{"GetUser", iamDecodeUserBoundary(t, srv, "alice")},
		{"GetRole", iamDecodeRoleBoundary(t, srv, "deploy")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, boundary, tc.got.PermissionsBoundaryArn,
				"this decoded to the empty string until #852")
			// The prose on AWS's own page says the type "can only have a value of Policy"
			// while the same page's enumeration says PermissionsBoundaryPolicy, and the CLI
			// v2 reference — generated from the service model — lists only the latter. Under
			// #671, only what the API model states, so the enum's value is the wire value.
			assert.Equal(t, "PermissionsBoundaryPolicy", tc.got.PermissionsBoundaryType)
		})
	}
}

// TestIAMBoundary_ADeletedBoundaryDecodesAsAbsent is the negative half, so the test above
// cannot pass against a renderer that emits the members unconditionally.
func TestIAMBoundary_ADeletedBoundaryDecodesAsAbsent(t *testing.T) {
	srv := newIAMTestServer(t)

	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": "arn:aws:iam::aws:policy/PowerUserAccess",
	})
	require.NotEmpty(t, iamDecodeUserBoundary(t, srv, "alice").PermissionsBoundaryArn)

	iamFormRaw(t, srv, "DeleteUserPermissionsBoundary", map[string]string{"UserName": "alice"})

	body := iamFormRaw(t, srv, "GetUser", map[string]string{"UserName": "alice"})
	assert.NotContains(t, body, "PermissionsBoundary",
		"an unset boundary omits the member rather than rendering it empty")
	assert.Empty(t, iamDecodeUserBoundary(t, srv, "alice").PermissionsBoundaryArn)
}

// TestIAMBoundary_AMalformedARNIsInvalidInput pins the code both operations publish.
//
// `arnType` is min 20 and max 2048, and `iamValidatePolicyARN` — the same function the three
// attach operations use — refuses a length outside those bounds before running the pattern, so a
// 19-character and a 2049-character ARN are refused on size and name it in the message.
func TestIAMBoundary_AMalformedARNIsInvalidInput(t *testing.T) {
	targets := []struct {
		operation string
		entity    string
		field     string
	}{
		{"PutUserPermissionsBoundary", "alice", "UserName"},
		{"PutRolePermissionsBoundary", "deploy", "RoleName"},
	}
	cases := []struct {
		name        string
		boundary    string
		wantMessage string
	}{
		{"not an ARN at all", "PowerUserAccess-but-long-enough", "is not valid"},
		{"an ARN for the wrong resource type",
			"arn:aws:iam::123456789012:role/PowerUserAccess", "is not valid"},
		{"nineteen characters, one below arnType's minimum",
			strings.Repeat("a", 19), "between 20 and 2048"},
		{"2049 characters, one above arnType's maximum",
			"arn:aws:iam::aws:policy/" + strings.Repeat("x", 2049-len("arn:aws:iam::aws:policy/")),
			"between 20 and 2048"},
	}

	for _, target := range targets {
		for _, tc := range cases {
			t.Run(target.operation+"/"+tc.name, func(t *testing.T) {
				srv := newIAMTestServer(t)
				iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
				iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})

				resp := iamFormRequest(t, srv, target.operation, map[string]string{
					target.field: target.entity, "PermissionsBoundary": tc.boundary,
				})
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				assert.Equal(t, "InvalidInput", result["__type"],
					"both operations publish InvalidInput; neither publishes ValidationError")
				message, _ := result["message"].(string)
				assert.Contains(t, message, tc.wantMessage)

				// The refusal is a refusal: no boundary reaches the entity.
				assert.Empty(t, iamDecodeUserBoundary(t, srv, "alice").PermissionsBoundaryArn)
				assert.Empty(t, iamDecodeRoleBoundary(t, srv, "deploy").PermissionsBoundaryArn)
			})
		}
	}
}

// TestIAMBoundary_AnAbsentMemberIsValidationError keeps the framework-level code where it
// belongs, so the InvalidInput assertion above is about the ARN rather than about any bad input.
//
// ValidationError is absent from both operations' error lists and present on IAM's CommonErrors
// page — "The input doesn't meet the required format or constraints. Check that all required
// parameters are included and that values are valid", HTTP 400 — which is where a missing
// required member is answered from, and what every other IAM handler answers for one.
func TestIAMBoundary_AnAbsentMemberIsValidationError(t *testing.T) {
	cases := []struct {
		name      string
		operation string
		params    map[string]string
	}{
		{"no boundary", "PutUserPermissionsBoundary", map[string]string{"UserName": "alice"}},
		{"no user", "PutUserPermissionsBoundary",
			map[string]string{"PermissionsBoundary": "arn:aws:iam::aws:policy/PowerUserAccess"}},
		{"no role", "PutRolePermissionsBoundary",
			map[string]string{"PermissionsBoundary": "arn:aws:iam::aws:policy/PowerUserAccess"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newIAMTestServer(t)
			iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
			assert.Equal(t, "ValidationError",
				iamFormErrorCode(t, srv, tc.operation, tc.params, http.StatusBadRequest))
		})
	}
}

// TestIAMBoundary_AnUnbundledManagedPolicySucceedsWithAWarning is #499's guarantee applied to a
// boundary, and it is the test that stops #846's first acceptance criterion being quietly
// reinstated.
//
// AWS says a boundary may be "an AWS managed policy or a customer managed policy". Substrate
// bundles 52 of roughly 1,200 managed policies, so refusing an ARN it cannot resolve would fail
// the ~1,148 calls that succeed against AWS. Neither operation's page states what a nonexistent
// boundary policy produces, so a NoSuchEntity refusal would be substrate's inference presented as
// AWS's behavior.
//
// The warning is separate from the attach path's, because the consequence is the reverse one: an
// unresolvable attached policy grants nothing, while an unresolvable boundary *restricts*
// nothing — loadPermissionBoundary returns nil, which is indistinguishable from no boundary at
// all, so the entity keeps every permission its attached policies grant.
func TestIAMBoundary_AnUnbundledManagedPolicySucceedsWithAWarning(t *testing.T) {
	const arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
	_, bundled := emulator.GetManagedPolicy(arn)
	require.False(t, bundled,
		"this test needs an ARN the catalog does not hold; pick another if it gets bundled")

	cases := []struct {
		operation string
		field     string
		entity    string
		wantWarn  string
	}{
		{"PutUserPermissionsBoundary", "UserName", "alice", "does not bundle"},
		{"PutRolePermissionsBoundary", "RoleName", "deploy", "does not bundle"},
	}
	for _, tc := range cases {
		t.Run(tc.operation, func(t *testing.T) {
			logs := &iamWarnLog{}
			srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)
			iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
			iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})

			resp := iamFormRequest(t, srv, tc.operation, map[string]string{
				tc.field: tc.entity, "PermissionsBoundary": arn,
			})
			require.Equal(t, http.StatusOK, resp.StatusCode,
				"AWS accepts this boundary, so substrate must")
			require.NoError(t, resp.Body.Close())

			warns := logs.warningsAbout(tc.wantWarn)
			require.Len(t, warns, 1, "an inert boundary has to be visible somewhere")
			assert.Contains(t, warns[0], "enforces nothing",
				"the boundary warning states its own consequence, not the attach path's")
			assert.Contains(t, warns[0], arn)

			// And it is readable back, which is what makes the success a success rather than
			// a no-op that happened to answer 200.
			if tc.field == "UserName" {
				assert.Equal(t, arn, iamDecodeUserBoundary(t, srv, "alice").PermissionsBoundaryArn)
				return
			}
			assert.Equal(t, arn, iamDecodeRoleBoundary(t, srv, "deploy").PermissionsBoundaryArn)
		})
	}
}

// TestIAMBoundary_ABundledPolicyBoundaryDoesNotWarn is the contrast that keeps the warning
// meaningful: a policy substrate holds resolves, so warning about it would train a consumer to
// ignore the line.
func TestIAMBoundary_ABundledPolicyBoundaryDoesNotWarn(t *testing.T) {
	logs := &iamWarnLog{}
	srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)

	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "alice", "PermissionsBoundary": "arn:aws:iam::aws:policy/PowerUserAccess",
	})

	assert.Empty(t, logs.warningsAbout("permissionsBoundary"),
		"a bundled boundary resolves and is enforced, so there is nothing to warn about")
}

// TestIAMBoundary_AServiceLinkedRoleIsUnmodifiable pins AWS's own asymmetry between the two
// operations.
//
// `PutRolePermissionsBoundary`'s prose states it outright — "You cannot set the boundary for a
// service-linked role" — and `UnmodifiableEntity` is on its error list at HTTP 400. Neither the
// sentence nor the code appears on `PutUserPermissionsBoundary`, which is AWS confirming the
// asymmetry rather than it being inferred from "there is no service-linked user".
func TestIAMBoundary_AServiceLinkedRoleIsUnmodifiable(t *testing.T) {
	srv := newIAMTestServer(t)

	resp := iamRequest(t, srv, "CreateServiceLinkedRole",
		map[string]string{"AWSServiceName": "lambda.amazonaws.com"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	const slrName = "AWSServiceRoleForLambda"

	code := iamFormErrorCode(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": slrName, "PermissionsBoundary": "arn:aws:iam::aws:policy/PowerUserAccess",
	}, http.StatusBadRequest)
	assert.Equal(t, "UnmodifiableEntity", code)

	assert.Empty(t, iamDecodeRoleBoundary(t, srv, slrName).PermissionsBoundaryArn,
		"the refusal must not have stored the boundary on the way to refusing")

	// An ordinary role at an ordinary path is unaffected, so the guard is about the reserved
	// path rather than about roles.
	iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})
	iamFormRaw(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": "arn:aws:iam::aws:policy/PowerUserAccess",
	})
	assert.NotEmpty(t, iamDecodeRoleBoundary(t, srv, "deploy").PermissionsBoundaryArn)
}

// iamAttachmentCount returns the AttachmentCount an IAM XML body reports, failing when the body
// carries none or carries more than one — a missing member must not read as zero, which is the
// defect this file exists for.
func iamAttachmentCount(t *testing.T, body string) int {
	t.Helper()
	const open, shut = "<AttachmentCount>", "</AttachmentCount>"
	start := strings.Index(body, open)
	require.GreaterOrEqual(t, start, 0, "no AttachmentCount in %s", body)
	rest := body[start+len(open):]
	end := strings.Index(rest, shut)
	require.GreaterOrEqual(t, end, 0, "unterminated AttachmentCount")
	require.NotContains(t, rest[end:], open, "more than one policy reported a count")
	count, err := strconv.Atoi(rest[:end])
	require.NoError(t, err, "AttachmentCount is an Integer on the Policy type")
	return count
}

// TestIAMAttachmentCount_GetPolicyAgreesWithListPolicies walks a customer-managed policy onto a
// user, a group and a role and back off again, asserting after every step that the two shapes
// report the same number.
//
// Both derive it from the same three "<kind>_policies:" prefixes, so a disagreement means one of
// them is reading the stored field — which is what GetPolicy did until #847, reporting 0 for
// every policy in every state.
func TestIAMAttachmentCount_GetPolicyAgreesWithListPolicies(t *testing.T) {
	srv := newIAMTestServer(t)

	created := iamFormRaw(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "deployer", "PolicyDocument": iamBoundaryDoc,
	})
	assert.Equal(t, 0, iamAttachmentCount(t, created),
		"a policy that has just been created is attached to nothing")
	arn := iamPolicyARNFrom(t, created)

	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "CreateGroup", map[string]string{"GroupName": "admins"})
	iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": "deploy"})

	steps := []struct {
		name      string
		operation string
		params    map[string]string
		want      int
	}{
		{"a user's attach counts", "AttachUserPolicy",
			map[string]string{"UserName": "alice", "PolicyArn": arn}, 1},
		{"a group's counts with it", "AttachGroupPolicy",
			map[string]string{"GroupName": "admins", "PolicyArn": arn}, 2},
		{"and a role's", "AttachRolePolicy",
			map[string]string{"RoleName": "deploy", "PolicyArn": arn}, 3},
		{"a detach is immediately visible", "DetachGroupPolicy",
			map[string]string{"GroupName": "admins", "PolicyArn": arn}, 2},
		{"detaching the rest reaches zero again", "DetachUserPolicy",
			map[string]string{"UserName": "alice", "PolicyArn": arn}, 1},
		{"the last one", "DetachRolePolicy",
			map[string]string{"RoleName": "deploy", "PolicyArn": arn}, 0},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			iamFormRaw(t, srv, step.operation, step.params)

			single := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
			// Scope=Local is the created policy alone, so the listing has exactly one member
			// and its count is unambiguous — under Scope=All the 52 bundled policies would
			// each report one too.
			listing := iamFormRaw(t, srv, "ListPolicies", map[string]string{"Scope": "Local"})
			require.Contains(t, listing, "<PolicyName>deployer</PolicyName>",
				"policy missing from listing")

			assert.Equal(t, step.want, iamAttachmentCount(t, single))
			assert.Equal(t, iamAttachmentCount(t, listing), iamAttachmentCount(t, single),
				"both shapes derive the count from the same state, so they cannot disagree")
		})
	}
}

// TestIAMAttachmentCount_ABundledPolicyCountDoesNotLeak is why the catalog arm copies the record
// before writing the count.
//
// GetManagedPolicy hands back the catalog's shared pointer, and the catalog is package-level and
// shared by every server in the process. Writing the count through that pointer would leak one
// request's number into every later read of the same bundled policy — including a read from
// another emulator entirely, which no consumer could explain.
func TestIAMAttachmentCount_ABundledPolicyCountDoesNotLeak(t *testing.T) {
	const bundled = "arn:aws:iam::aws:policy/PowerUserAccess"

	srv := newIAMTestServer(t)
	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})
	iamFormRaw(t, srv, "AttachUserPolicy", map[string]string{
		"UserName": "alice", "PolicyArn": bundled,
	})

	first := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": bundled})
	require.Equal(t, 1, iamAttachmentCount(t, first),
		"a bundled policy's count comes from state, since the catalog carries none")
	second := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": bundled})
	assert.Equal(t, 1, iamAttachmentCount(t, second),
		"a second read reports the same count, not an accumulated one")

	fresh := newIAMTestServer(t)
	unattached := iamFormRaw(t, fresh, "GetPolicy", map[string]string{"PolicyArn": bundled})
	assert.Equal(t, 0, iamAttachmentCount(t, unattached),
		"the same bundled policy in another emulator is attached to nothing")

	// And the first server still reports its own count, so the fresh read did not clear a
	// shared field either.
	assert.Equal(t, 1, iamAttachmentCount(t,
		iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": bundled})))
}

// TestIAMAttachmentCount_CountsTheEntitiesNotTheAttachCalls pins that the count is a number of
// entities rather than a number of operations: an attach of the same policy to the same user
// twice is idempotent on AWS, so it cannot count twice.
func TestIAMAttachmentCount_CountsTheEntitiesNotTheAttachCalls(t *testing.T) {
	srv := newIAMTestServer(t)

	arn := iamPolicyARNFrom(t, iamFormRaw(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "deployer", "PolicyDocument": iamBoundaryDoc,
	}))
	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": "alice"})

	for range 2 {
		iamFormRaw(t, srv, "AttachUserPolicy", map[string]string{
			"UserName": "alice", "PolicyArn": arn,
		})
	}

	body := iamFormRaw(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
	assert.Equal(t, 1, iamAttachmentCount(t, body))
}
