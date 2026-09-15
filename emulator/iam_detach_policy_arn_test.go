package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Detach-time PolicyArn validation (#875).
//
// #875 reported that a malformed ARN detached silently and successfully. It did not: all three
// handlers already walked the attached list and answered NoSuchEntity/404 when the ARN was not
// in it. The real gap was the code: 404 "the policy is not attached to the specified entity"
// where the attach counterpart answered InvalidInput/400 for the same string, which points a
// caller at the attachment when the mistake was the ARN it typed.
//
// So these tests assert two different things and the distinction is the point. A *malformed*
// ARN is InvalidInput/400 — the new behavior, and what API_DetachUserPolicy publishes for "an
// invalid or out-of-range value supplied for an input parameter". A *well-formed but
// unattached* ARN stays NoSuchEntity/404, which is also published, and which is asserted here
// so the fix cannot quietly turn every unattached detach into a 400.
//
// The tables come from iam_policy_arn_test.go, shared with the attach tests, because the
// criterion is that the two sides agree about what they accept.

// iamDetach runs one detach operation against target's entity and returns the response.
//
// It does not create the entity: the caller decides whether one exists, since a detach against a
// missing entity and a detach of a missing attachment are different cases.
func iamDetach(t *testing.T, srv *emulator.Server, target iamAttachTarget,
	field, name, policyARN string,
) *http.Response {
	t.Helper()
	return iamRequest(t, srv, target.detach, map[string]any{
		field:       name,
		"PolicyArn": policyARN,
	})
}

// iamAttachedARNs returns the policy ARNs target's listing reports for the named entity.
func iamAttachedARNs(t *testing.T, srv *emulator.Server, target iamAttachTarget,
	field, name string,
) []string {
	t.Helper()
	resp := iamRequest(t, srv, target.list, map[string]any{field: name})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeIAMXML(t, resp, &result)

	// An entity with no attachments renders no AttachedPolicies member at all, which is not the
	// same as an empty one — so a missing key is an empty slice here rather than a failure.
	entries, ok := result["AttachedPolicies"].([]any)
	if !ok {
		return nil
	}
	arns := make([]string, 0, len(entries))
	for _, entry := range entries {
		fields, ok := entry.(map[string]any)
		require.True(t, ok)
		arn, ok := fields["PolicyArn"].(string)
		require.True(t, ok)
		arns = append(arns, arn)
	}
	return arns
}

func TestDetachPolicy_RefusesAMalformedARN(t *testing.T) {
	// The fix. Before it, every case here answered NoSuchEntity/404 naming the attachment
	// instead of the parameter.
	for _, target := range iamAttachTargets {
		for _, tc := range iamMalformedPolicyARNs {
			t.Run(target.detach+"/"+tc.name, func(t *testing.T) {
				srv := newIAMTestServer(t)
				field, name := target.setup(t, srv)

				resp := iamDetach(t, srv, target, field, name, tc.policyARN)

				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				assert.Equal(t, "InvalidInput", result["__type"],
					"the model declares InvalidInput for all three detach operations")
				message, _ := result["message"].(string)
				assert.Contains(t, message, tc.wantMessage)
			})
		}
	}
}

func TestDetachPolicy_AttachAndDetachAgreeOnWhatIsAccepted(t *testing.T) {
	// #875's fourth criterion, and the one that stops the pair drifting apart again: for one
	// string, both directions must reach the same verdict. Before the fix the attach answered
	// InvalidInput/400 and the detach NoSuchEntity/404 for every case in the table.
	for _, target := range iamAttachTargets {
		for _, tc := range iamMalformedPolicyARNs {
			t.Run(target.operation+"/"+tc.name, func(t *testing.T) {
				srv := newIAMTestServer(t)
				field, name := target.setup(t, srv)

				attachResp := iamRequest(t, srv, target.operation, map[string]any{
					field:       name,
					"PolicyArn": tc.policyARN,
				})
				var attachResult map[string]any
				decodeIAMXML(t, attachResp, &attachResult)

				detachResp := iamDetach(t, srv, target, field, name, tc.policyARN)
				var detachResult map[string]any
				decodeIAMXML(t, detachResp, &detachResult)

				assert.Equal(t, attachResp.StatusCode, detachResp.StatusCode,
					"the two directions must not disagree about whether the ARN is acceptable")
				assert.Equal(t, attachResult["__type"], detachResult["__type"])
			})
		}
	}
}

func TestDetachPolicy_AWellFormedAttachedARNStillDetaches(t *testing.T) {
	// The check must refuse a shape without refusing the operation. Asserted through the
	// entity's own listing rather than from state, per #765: a state read cannot prove the
	// removal is observable through the API that owns it.
	const arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

	for _, target := range iamAttachTargets {
		t.Run(target.detach, func(t *testing.T) {
			srv := newIAMTestServer(t)
			field, name := target.setup(t, srv)

			resp := iamRequest(t, srv, target.operation, map[string]any{
				field:       name,
				"PolicyArn": arn,
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, []string{arn}, iamAttachedARNs(t, srv, target, field, name))

			resp = iamDetach(t, srv, target, field, name, arn)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			assert.Empty(t, iamAttachedARNs(t, srv, target, field, name),
				"the detach removed the attachment the listing reported")
		})
	}
}

func TestDetachPolicy_AWellFormedUnattachedARNAnswersNoSuchEntity(t *testing.T) {
	// Recorded, not changed. This behavior predates #875 and matches what
	// API_DetachUserPolicy publishes; the test exists so the shape check cannot turn it into a
	// 400 by accident, which would report the ARN as invalid when it is perfectly well-formed.
	//
	// It is also not in tension with #499's refusal to require the *policy* to exist. Substrate
	// holds every attachment it was told about, so absence is a fact it can report; policy
	// existence is a fact it cannot, bundling 52 of roughly 1,200 managed policies.
	const arn = "arn:aws:iam::123456789012:policy/never-attached"

	for _, target := range iamAttachTargets {
		t.Run(target.detach, func(t *testing.T) {
			srv := newIAMTestServer(t)
			field, name := target.setup(t, srv)

			resp := iamDetach(t, srv, target, field, name, arn)

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "NoSuchEntity", result["__type"])
		})
	}
}

func TestDetachPolicy_TheShapeCheckPrecedesAuthorization(t *testing.T) {
	// Same ordering the attach operations pin: a malformed ARN is refused on its shape whatever
	// the caller may do, so the refusal is a property of the request and not of the caller's
	// permissions. Swapping the two would report AccessDenied for a request that is invalid
	// regardless.
	for _, target := range iamAttachTargets {
		t.Run(target.detach, func(t *testing.T) {
			srv := newIAMTestServer(t)
			field, name := target.setup(t, srv)

			resp := iamDetach(t, srv, target, field, name, "AmazonS3ReadOnlyAccess")
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
}

func TestDetachPolicy_OverTheQueryProtocol(t *testing.T) {
	// A real client posts a form body, and the refusal has to survive that path: a check that
	// only fires for a hand-built JSON body is a check no SDK caller reaches.
	const arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	srv := newIAMTestServer(t)

	resp := iamFormRequest(t, srv, "CreateUser", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "AttachUserPolicy", map[string]string{
		"UserName":  "jill",
		"PolicyArn": arn,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "DetachUserPolicy", map[string]string{
		"UserName":  "jill",
		"PolicyArn": "AmazonS3ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "InvalidInput", result["__type"])

	resp = iamFormRequest(t, srv, "DetachUserPolicy", map[string]string{
		"UserName":  "jill",
		"PolicyArn": arn,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}
