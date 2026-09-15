package emulator_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// DeletePolicy's three refusals (#853).
//
// The operation used to go straight from the NoSuchEntity check to state.Delete, so a policy
// still attached to a user, group or role deleted silently and left every one of them holding
// an ARN pointing at nothing. What made that worth more than a missing error code is what it
// left behind: ListAttachedUserPolicies still reported the ARN, the authorization evaluator
// loaded no document for it so the entity silently lost the permissions it granted — a test
// asserting a deny then passed for the wrong reason — and GetPolicy answered NoSuchEntity for
// the same ARN two other operations still named.
//
// Every entity here is created and attached through the API rather than written into state, per
// #765: a helper that writes an attachment directly cannot prove the refusal reads the same
// keys an attach writes, which is the whole of the issue's second acceptance criterion.
//
// The 409's message is asserted on its text rather than just its code, because
// ListEntitiesForPolicy is not implemented — so the message is the only way a substrate caller
// can learn what to detach, and an unnamed entity is an unrecoverable wedge rather than a
// cosmetic omission.

const iamDeletePolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

// iamDeletePolicyTestARN is the ARN CreatePolicy mints for "read-objects" in the test account.
const iamDeletePolicyTestARN = "arn:aws:iam::123456789012:policy/read-objects"

// iamBundledPolicyARN names a policy from the bundled catalog — one that exists for GetPolicy
// and has no state record at all, which is the pair DeletePolicy used to contradict.
const iamBundledPolicyARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

// iamDeletePolicyError executes operation and returns the decoded error body, requiring the
// status.
func iamDeletePolicyError(t *testing.T, srv *emulator.Server, operation string, body map[string]string, wantStatus int) map[string]any {
	t.Helper()
	resp := iamRequest(t, srv, operation, body)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	require.Equal(t, wantStatus, resp.StatusCode, "%s: %v", operation, out)
	return out
}

// iamDeletePolicyFixture creates the policy plus one user, one group and one role, so a test
// can attach whichever kinds it is about.
func iamDeletePolicyFixture(t *testing.T) *emulator.Server {
	t.Helper()
	srv := newIAMTestServer(t)
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "read-objects", "PolicyDocument": iamDeletePolicyDoc,
	})
	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateGroup", map[string]string{"GroupName": "engineers"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	return srv
}

// TestIAMDeletePolicy_AttachedIsRefused covers each kind on its own as well as all three
// together.
//
// One prefix scanned would pass a test that only attaches to a user, so the single-kind cases
// are the ones that pin the group and role prefixes; the all-three case is what pins the
// message naming every kind rather than the first one found.
func TestIAMDeletePolicy_AttachedIsRefused(t *testing.T) {
	t.Parallel()

	attachUser := func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		iamAuthzOK(t, srv, "AttachUserPolicy", map[string]string{
			"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN,
		})
	}
	attachGroup := func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		iamAuthzOK(t, srv, "AttachGroupPolicy", map[string]string{
			"GroupName": "engineers", "PolicyArn": iamDeletePolicyTestARN,
		})
	}
	attachRole := func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		iamAuthzOK(t, srv, "AttachRolePolicy", map[string]string{
			"RoleName": "deploy", "PolicyArn": iamDeletePolicyTestARN,
		})
	}

	tests := []struct {
		name string

		// attach applies the attachments this case is about.
		attach []func(*testing.T, *emulator.Server)

		// wantNamed are the entity names the message must carry, and wantAbsent the ones it
		// must not: a message naming a group the policy is not attached to would send the
		// caller to detach something that is already clean.
		wantNamed  []string
		wantAbsent []string
	}{
		{
			name:       "a user only",
			attach:     []func(*testing.T, *emulator.Server){attachUser},
			wantNamed:  []string{"Users: ada"},
			wantAbsent: []string{"Groups:", "Roles:"},
		},
		{
			name:       "a group only",
			attach:     []func(*testing.T, *emulator.Server){attachGroup},
			wantNamed:  []string{"Groups: engineers"},
			wantAbsent: []string{"Users:", "Roles:"},
		},
		{
			name:       "a role only",
			attach:     []func(*testing.T, *emulator.Server){attachRole},
			wantNamed:  []string{"Roles: deploy"},
			wantAbsent: []string{"Users:", "Groups:"},
		},
		{
			name:      "all three kinds",
			attach:    []func(*testing.T, *emulator.Server){attachUser, attachGroup, attachRole},
			wantNamed: []string{"Users: ada", "Groups: engineers", "Roles: deploy"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := iamDeletePolicyFixture(t)
			for _, attach := range tt.attach {
				attach(t, srv)
			}

			out := iamDeletePolicyError(t, srv, "DeletePolicy",
				map[string]string{"PolicyArn": iamDeletePolicyTestARN}, http.StatusConflict)
			assert.Equal(t, "DeleteConflict", out["__type"],
				"API_DeletePolicy publishes DeleteConflict/409 for attached subordinate entities")

			message, ok := out["message"].(string)
			require.True(t, ok, "the refusal carries a message: %v", out)
			for _, want := range tt.wantNamed {
				assert.Contains(t, message, want,
					"the reference says the error message describes these entities, and "+
						"ListEntitiesForPolicy is not implemented, so the message is the only "+
						"way a caller can learn them")
			}
			for _, absent := range tt.wantAbsent {
				assert.NotContains(t, message, absent,
					"a kind with nothing attached must not be named")
			}

			// The refusal must not have deleted anything on its way out.
			resp := iamRequest(t, srv, "GetPolicy",
				map[string]string{"PolicyArn": iamDeletePolicyTestARN})
			assert.Equal(t, http.StatusOK, resp.StatusCode,
				"a refused delete leaves the policy readable")
			require.NoError(t, resp.Body.Close())
		})
	}
}

// TestIAMDeletePolicy_SucceedsAfterDetach walks the refusal down to zero one detach at a time.
//
// The intermediate steps are what prove the refusal is derived rather than a latch: a delete
// that stayed refused after two of three detaches, or succeeded after one, would still pass a
// test that only checked the endpoints.
func TestIAMDeletePolicy_SucceedsAfterDetach(t *testing.T) {
	t.Parallel()
	srv := iamDeletePolicyFixture(t)

	for _, attach := range []struct {
		operation string
		body      map[string]string
	}{
		{"AttachUserPolicy", map[string]string{"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN}},
		{"AttachGroupPolicy", map[string]string{"GroupName": "engineers", "PolicyArn": iamDeletePolicyTestARN}},
		{"AttachRolePolicy", map[string]string{"RoleName": "deploy", "PolicyArn": iamDeletePolicyTestARN}},
	} {
		iamAuthzOK(t, srv, attach.operation, attach.body)
	}

	for _, detach := range []struct {
		name      string
		operation string
		body      map[string]string
	}{
		{"user detached", "DetachUserPolicy", map[string]string{"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN}},
		{"group detached", "DetachGroupPolicy", map[string]string{"GroupName": "engineers", "PolicyArn": iamDeletePolicyTestARN}},
	} {
		t.Run(detach.name+", still refused", func(t *testing.T) {
			iamAuthzOK(t, srv, detach.operation, detach.body)
			out := iamDeletePolicyError(t, srv, "DeletePolicy",
				map[string]string{"PolicyArn": iamDeletePolicyTestARN}, http.StatusConflict)
			assert.Equal(t, "DeleteConflict", out["__type"])
		})
	}

	t.Run("role detached, delete succeeds", func(t *testing.T) {
		iamAuthzOK(t, srv, "DetachRolePolicy",
			map[string]string{"RoleName": "deploy", "PolicyArn": iamDeletePolicyTestARN})
		iamAuthzOK(t, srv, "DeletePolicy", map[string]string{"PolicyArn": iamDeletePolicyTestARN})

		out := iamDeletePolicyError(t, srv, "GetPolicy",
			map[string]string{"PolicyArn": iamDeletePolicyTestARN}, http.StatusNotFound)
		assert.Equal(t, "NoSuchEntity", out["__type"],
			"after the delete the two operations agree the policy is gone")
	})
}

// TestIAMDeletePolicy_RecreatedARNInheritsNoAttachments asserts the invariant the refusal buys
// rather than the refusal itself.
//
// Nothing writes a back-reference onto a policy, so before #853 a delete-then-recreate under
// the same ARN inherited the previous policy's attachments — and since #847 derives
// AttachmentCount from those same lists, the stale number was reported through GetPolicy. The
// refusal makes it unreachable: a successful delete now implies nothing is attached, so there
// is nothing for a re-created policy to inherit.
func TestIAMDeletePolicy_RecreatedARNInheritsNoAttachments(t *testing.T) {
	t.Parallel()
	srv := iamDeletePolicyFixture(t)

	attachmentCount := func(t *testing.T) string {
		t.Helper()
		resp := iamRequest(t, srv, "GetPolicy", map[string]string{"PolicyArn": iamDeletePolicyTestARN})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return iamXMLSection(t, string(raw), "AttachmentCount")
	}

	iamAuthzOK(t, srv, "AttachUserPolicy",
		map[string]string{"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN})
	require.Equal(t, "1", attachmentCount(t))

	iamAuthzOK(t, srv, "DetachUserPolicy",
		map[string]string{"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN})
	iamAuthzOK(t, srv, "DeletePolicy", map[string]string{"PolicyArn": iamDeletePolicyTestARN})
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "read-objects", "PolicyDocument": iamDeletePolicyDoc,
	})

	assert.Equal(t, "0", attachmentCount(t),
		"a policy re-created under a deleted ARN starts unattached")
}

// TestIAMDeletePolicy_MalformedARNRefused pins the shape check the handler did not have.
//
// Every one of these used to reach iamPolicyKey, which composed a state key nothing could
// match, so the caller was told the policy did not exist — the wrong answer for a request that
// was never well-formed enough to name one.
func TestIAMDeletePolicy_MalformedARNRefused(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	tests := []struct {
		name string
		arn  string

		// wantMessage is a fragment the refusal must carry, so the length case and the
		// pattern case are not interchangeable.
		wantMessage string
	}{
		{
			name:        "below the minimum length",
			arn:         strings.Repeat("a", 19),
			wantMessage: "must be between 20 and 2048 characters long (got 19)",
		},
		{
			// arnType's maximum is 2048, and the length check runs before the pattern so a
			// large body is refused on its size rather than run through the regexp.
			name:        "above the maximum length",
			arn:         "arn:aws:iam::123456789012:policy/" + strings.Repeat("a", 2016),
			wantMessage: "must be between 20 and 2048 characters long (got 2049)",
		},
		{
			name:        "a bare policy name",
			arn:         "read-objects-not-an-arn",
			wantMessage: "is not valid",
		},
		{
			name:        "an ARN for a different service",
			arn:         "arn:aws:s3:::example-bucket-name",
			wantMessage: "is not valid",
		},
		{
			name:        "no policy resource type",
			arn:         "arn:aws:iam::123456789012:user/read-objects",
			wantMessage: "is not valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := iamDeletePolicyError(t, srv, "DeletePolicy",
				map[string]string{"PolicyArn": tt.arn}, http.StatusBadRequest)
			assert.Equal(t, "InvalidInput", out["__type"],
				"DeletePolicy publishes InvalidInput, which is what the attach operations answer")
			message, ok := out["message"].(string)
			require.True(t, ok, "the refusal carries a message: %v", out)
			assert.Contains(t, message, tt.wantMessage)
		})
	}
}

// TestIAMDeletePolicy_AWSManagedARNIsRefusedNotDenied is the two-operations-disagreeing case.
//
// The handler read state only, so a bundled ARN answered NoSuchEntity while GetPolicy resolved
// the same ARN from the catalog and returned the policy — one ARN, two operations, opposite
// answers about whether the thing exists. The code is substrate's reading, drawn from
// DeletePolicy's own published Errors section, and what this test pins is that the two
// operations stop contradicting each other: GetPolicy still reports the policy after the
// refusal.
func TestIAMDeletePolicy_AWSManagedARNIsRefusedNotDenied(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	out := iamDeletePolicyError(t, srv, "DeletePolicy",
		map[string]string{"PolicyArn": iamBundledPolicyARN}, http.StatusBadRequest)
	assert.Equal(t, "InvalidInput", out["__type"])
	assert.NotEqual(t, "NoSuchEntity", out["__type"],
		"the policy exists — GetPolicy returns it — so reporting it missing is the wrong answer")
	message, ok := out["message"].(string)
	require.True(t, ok, "the refusal carries a message: %v", out)
	assert.Contains(t, message, "AWS managed policy")

	resp := iamRequest(t, srv, "GetPolicy", map[string]string{"PolicyArn": iamBundledPolicyARN})
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"the refusal leaves the catalog's answer unchanged, so the two operations agree")
	require.NoError(t, resp.Body.Close())
}

// TestIAMDeletePolicy_StateFailureIsNotADelete pins the error arms of the attachment scan.
//
// A store read that fails and a policy with nothing attached are opposite signals: the first
// says "ask again", the second says "go ahead and delete". Swallowing the failure would let a
// broken store turn the refusal off, which is the one direction that loses data — the delete
// would succeed and leave exactly the dangling ARNs #853 is about.
func TestIAMDeletePolicy_StateFailureIsNotADelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arm  func(*iamFaultState)
	}{
		{
			name: "the user attachment listing fails",
			arm: func(s *iamFaultState) {
				s.listErrPrefix = emulator.IAMAttachedPoliciesPrefixForTest(authzTestAccount, "user")
			},
		},
		{
			name: "the group attachment listing fails",
			arm: func(s *iamFaultState) {
				s.listErrPrefix = emulator.IAMAttachedPoliciesPrefixForTest(authzTestAccount, "group")
			},
		},
		{
			name: "the role attachment listing fails",
			arm: func(s *iamFaultState) {
				s.listErrPrefix = emulator.IAMAttachedPoliciesPrefixForTest(authzTestAccount, "role")
			},
		},
		{
			name: "an attachment list is unreadable",
			arm:  func(s *iamFaultState) { s.getErrKeySubstr = "user_policies:" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The fault is armed only after the setup writes have landed: a store that fails
			// from the first call never gets a policy created to trip over.
			fault := &iamFaultState{StateManager: emulator.NewMemoryStateManager()}
			srv := newIAMTestServerWithState(t, fault)
			iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
				"PolicyName": "read-objects", "PolicyDocument": iamDeletePolicyDoc,
			})
			iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
			iamAuthzOK(t, srv, "AttachUserPolicy",
				map[string]string{"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN})
			tt.arm(fault)

			resp := iamRequest(t, srv, "DeletePolicy",
				map[string]string{"PolicyArn": iamDeletePolicyTestARN})
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
				"a failed read reaches the caller rather than becoming an empty attachment set")
			require.NoError(t, resp.Body.Close())
		})
	}
}

// TestIAMDeletePolicy_AttachmentsAgreeWithTheCount asserts the two derivations cannot diverge.
//
// [iamPolicyAttachments] and the AttachmentCount GetPolicy reports are separate helpers over
// the same three prefixes, which is the arrangement #853's second acceptance criterion asks
// for — and the failure mode it guards against is the one #847 was: two readings of the same
// state disagreeing, with no test putting them side by side.
func TestIAMDeletePolicy_AttachmentsAgreeWithTheCount(t *testing.T) {
	t.Parallel()
	srv := iamDeletePolicyFixture(t)

	iamAuthzOK(t, srv, "AttachUserPolicy",
		map[string]string{"UserName": "ada", "PolicyArn": iamDeletePolicyTestARN})
	iamAuthzOK(t, srv, "AttachRolePolicy",
		map[string]string{"RoleName": "deploy", "PolicyArn": iamDeletePolicyTestARN})

	resp := iamRequest(t, srv, "GetPolicy", map[string]string{"PolicyArn": iamDeletePolicyTestARN})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "2", iamXMLSection(t, string(raw), "AttachmentCount"))

	out := iamDeletePolicyError(t, srv, "DeletePolicy",
		map[string]string{"PolicyArn": iamDeletePolicyTestARN}, http.StatusConflict)
	message, ok := out["message"].(string)
	require.True(t, ok, "the refusal carries a message: %v", out)

	named := 0
	for _, entity := range []string{"ada", "deploy"} {
		if strings.Contains(message, entity) {
			named++
		}
	}
	assert.Equal(t, 2, named,
		fmt.Sprintf("the refusal names as many entities as AttachmentCount counts: %q", message))
}
