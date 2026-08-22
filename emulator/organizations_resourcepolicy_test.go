package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// A well-formed resource policy document. The content is not parsed by
// substrate — the model's pattern is "[\s\S]*" and the operation declares no
// MalformedPolicyDocumentException — but a realistic one is used so a test
// reading this file sees what the operation is for: delegating a read to a
// member account.
const orgResourcePolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"111122223333"},"Action":"organizations:DescribeOrganization","Resource":"*"}]}`

// --- helpers --------------------------------------------------------------

// orgDescribeResourcePolicy posts DescribeResourcePolicy and returns the status,
// the policy, and the error code a refusal carries.
func orgDescribeResourcePolicy(t *testing.T, ts *httptest.Server) (int, emulator.OrgResourcePolicy, string) {
	t.Helper()
	return orgResourcePolicyCall(t, ts, "DescribeResourcePolicy", map[string]any{})
}

// orgPutResourcePolicy posts PutResourcePolicy with the given body.
func orgPutResourcePolicy(t *testing.T, ts *httptest.Server, body map[string]any) (int, emulator.OrgResourcePolicy, string) {
	t.Helper()
	return orgResourcePolicyCall(t, ts, "PutResourcePolicy", body)
}

// orgResourcePolicyCall posts one operation and decodes either the ResourcePolicy
// or the error code, so a refusal can be asserted by code rather than by status
// alone — every Organizations exception shares HTTP 400.
func orgResourcePolicyCall(t *testing.T, ts *httptest.Server, op string, body map[string]any) (int, emulator.OrgResourcePolicy, string) {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		ResourcePolicy emulator.OrgResourcePolicy `json:"ResourcePolicy"`
		Type           string                     `json:"__type"`
		Message        string                     `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", op, err)
	}
	code := out.Type
	if code != "" {
		code += "/" + out.Message
	}
	return resp.StatusCode, out.ResourcePolicy, code
}

// --- tests ----------------------------------------------------------------

// TestOrganizations_ResourcePolicy_NotFoundIsTheNormalCase is #619's repro
// verbatim, and the assertion the issue cares most about.
//
// Most organizations have no resource policy, so this refusal is the ordinary
// answer rather than an edge case. A caller checking whether management delegated
// anything to it has to tell ResourcePolicyNotFoundException apart from
// AccessDeniedException; answering an empty policy would collapse "no delegation"
// and "delegation I cannot read" into one observation.
func TestOrganizations_ResourcePolicy_NotFoundIsTheNormalCase(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	status, _, code := orgDescribeResourcePolicy(t, ts)
	if status != http.StatusBadRequest {
		t.Fatalf("DescribeResourcePolicy on a fresh organization: expected 400, got %d", status)
	}
	if !strings.HasPrefix(code, "ResourcePolicyNotFoundException") {
		t.Fatalf("expected ResourcePolicyNotFoundException, got %q", code)
	}
}

// TestOrganizations_ResourcePolicy_RoundTrip covers the lifecycle a delegation
// story needs: put, read back, delete, and read again.
func TestOrganizations_ResourcePolicy_RoundTrip(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	status, put, code := orgPutResourcePolicy(t, ts, map[string]any{"Content": orgResourcePolicyDoc})
	if status != http.StatusOK {
		t.Fatalf("PutResourcePolicy: expected 200, got %d (%s)", status, code)
	}
	id := put.ResourcePolicySummary.ID
	if !strings.HasPrefix(id, "rp-") {
		t.Fatalf("resource policy ID %q does not have the rp- prefix the model's pattern requires", id)
	}
	// The ARN embeds the organization ID, so a caller that scopes a policy statement
	// to it must see the same one DescribeOrganization reports.
	if arn := put.ResourcePolicySummary.Arn; !strings.Contains(arn, ":resourcepolicy/") ||
		!strings.HasSuffix(arn, "/"+id) {
		t.Fatalf("resource policy ARN %q is not the documented resourcepolicy/{orgId}/{rpId} shape", arn)
	}
	if put.Content != orgResourcePolicyDoc {
		t.Fatalf("PutResourcePolicy returned content %q, want the document that was sent", put.Content)
	}

	status, got, code := orgDescribeResourcePolicy(t, ts)
	if status != http.StatusOK {
		t.Fatalf("DescribeResourcePolicy after a put: expected 200, got %d (%s)", status, code)
	}
	if got != put {
		t.Fatalf("DescribeResourcePolicy returned %+v, want the policy Put reported %+v", got, put)
	}

	resp := orgsRequest(t, ts, "DeleteResourcePolicy", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteResourcePolicy: expected 200, got %d", resp.StatusCode)
	}

	status, _, code = orgDescribeResourcePolicy(t, ts)
	if status != http.StatusBadRequest || !strings.HasPrefix(code, "ResourcePolicyNotFoundException") {
		t.Fatalf("after a delete: expected 400/ResourcePolicyNotFoundException, got %d/%q", status, code)
	}
}

// TestOrganizations_ResourcePolicy_ReplacementKeepsTheID asserts the decision that
// a Put updates the single policy in place.
//
// An organization holds exactly one resource policy, so a fresh ID would name
// nothing new — it would only make a caller holding the previous ARN conclude its
// policy had been replaced by a different one.
func TestOrganizations_ResourcePolicy_ReplacementKeepsTheID(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	_, first, _ := orgPutResourcePolicy(t, ts, map[string]any{"Content": orgResourcePolicyDoc})

	const replacement = `{"Version":"2012-10-17","Statement":[]}`
	status, second, code := orgPutResourcePolicy(t, ts, map[string]any{"Content": replacement})
	if status != http.StatusOK {
		t.Fatalf("replacing the resource policy: expected 200, got %d (%s)", status, code)
	}
	if second.ResourcePolicySummary != first.ResourcePolicySummary {
		t.Fatalf("a replacement re-minted the identity: %+v then %+v",
			first.ResourcePolicySummary, second.ResourcePolicySummary)
	}
	if second.Content != replacement {
		t.Fatalf("the replacement's content = %q, want the new document", second.Content)
	}

	// The stored policy has to agree, or a caller reading after the write sees the
	// pre-replacement document.
	_, got, _ := orgDescribeResourcePolicy(t, ts)
	if got.Content != replacement || got.ResourcePolicySummary != first.ResourcePolicySummary {
		t.Fatalf("DescribeResourcePolicy after a replacement = %+v, want content %q with the original identity",
			got, replacement)
	}
}

// TestOrganizations_ResourcePolicy_DeleteTwiceIsARefusal is the re-runnability
// property: a second teardown pass gets an error it can branch on rather than an
// outcome indistinguishable from having done the work.
func TestOrganizations_ResourcePolicy_DeleteTwiceIsARefusal(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	if _, _, code := orgPutResourcePolicy(t, ts, map[string]any{"Content": orgResourcePolicyDoc}); code != "" {
		t.Fatalf("PutResourcePolicy: %s", code)
	}

	first := orgsRequest(t, ts, "DeleteResourcePolicy", map[string]any{})
	defer first.Body.Close() //nolint:errcheck
	if first.StatusCode != http.StatusOK {
		t.Fatalf("the first DeleteResourcePolicy: expected 200, got %d", first.StatusCode)
	}

	status, _, code := orgResourcePolicyCall(t, ts, "DeleteResourcePolicy", map[string]any{})
	if status != http.StatusBadRequest || !strings.HasPrefix(code, "ResourcePolicyNotFoundException") {
		t.Fatalf("the second DeleteResourcePolicy: expected 400/ResourcePolicyNotFoundException, got %d/%q",
			status, code)
	}
}

// TestOrganizations_PutResourcePolicy_ContentBounds covers ResourcePolicyContent's
// documented 1-40,000-character range at its boundaries.
//
// The maximum is the interesting one: it is 40,000 rather than the 10,240 an SCP
// gets, so a caller sizing its delegation document against the SCP limit would be
// refused for no reason, and an off-by-one here is invisible in a nominal run.
func TestOrganizations_PutResourcePolicy_ContentBounds(t *testing.T) {
	// Padding goes inside a JSON string so the oversized document is still
	// well-formed: the refusal under test is the length, and a document that also
	// failed to parse would pass this test for the wrong reason.
	doc := func(n int) string {
		const wrapper = `{"Version":"2012-10-17","Statement":"%s"}`
		fill := n - (len(wrapper) - 2)
		if fill < 0 {
			t.Fatalf("cannot build a %d-character document", n)
		}
		return strings.Replace(wrapper, "%s", strings.Repeat("x", fill), 1)
	}

	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "empty content is refused",
			content: "",
			wantErr: "InvalidInputException",
		},
		{
			// The shortest parseable document, so this pins the minimum rather than
			// the JSON check that follows it. A single "x" is within the length bound
			// and would be refused for the other reason, which would leave the
			// minimum itself unexercised.
			name:    "the documented minimum is a length, not a shape",
			content: "0",
			wantErr: "",
		},
		{
			name:    "the maximum is accepted",
			content: doc(emulator.OrgMaxResourcePolicyCharsForTest),
			wantErr: "",
		},
		{
			name:    "one character past the maximum is refused",
			content: doc(emulator.OrgMaxResourcePolicyCharsForTest + 1),
			wantErr: "InvalidInputException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newOrganizationsTestServer(t)
			status, _, code := orgPutResourcePolicy(t, ts, map[string]any{"Content": tc.content})
			if tc.wantErr == "" {
				if status != http.StatusOK {
					t.Fatalf("expected 200, got %d (%s)", status, code)
				}
				return
			}
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", status)
			}
			if !strings.HasPrefix(code, tc.wantErr) {
				t.Fatalf("expected %s, got %q", tc.wantErr, code)
			}
		})
	}
}

// TestOrganizations_PutResourcePolicy_ContentRequired distinguishes an absent
// required member from a present-but-empty one. Both are refused, but a caller
// debugging its request needs to know which mistake it made.
func TestOrganizations_PutResourcePolicy_ContentRequired(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	status, _, code := orgPutResourcePolicy(t, ts, map[string]any{})
	if status != http.StatusBadRequest {
		t.Fatalf("PutResourcePolicy with no Content: expected 400, got %d", status)
	}
	if !strings.Contains(code, "INPUT_REQUIRED") {
		t.Fatalf("expected the INPUT_REQUIRED reason for an absent Content, got %q", code)
	}
}

// TestOrganizations_PutResourcePolicy_TagsAreCreateOnly covers the model's note
// that "Calls with tags apply to the initial creation of the resource policy,
// otherwise an exception is thrown".
//
// Accepting tags on an update and silently dropping them would leave a tag-gated
// authorization decision reading a tag set the caller believes it just wrote.
func TestOrganizations_PutResourcePolicy_TagsAreCreateOnly(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	status, created, code := orgPutResourcePolicy(t, ts, map[string]any{
		"Content": orgResourcePolicyDoc,
		"Tags":    []map[string]any{{"Key": "Owner", "Value": "platform"}},
	})
	if status != http.StatusOK {
		t.Fatalf("PutResourcePolicy with create-time tags: expected 200, got %d (%s)", status, code)
	}

	// The tag has to be readable through ListTagsForResource, or nothing can gate
	// on it — a resource policy is taggable precisely because Put accepts Tags.
	resp := orgsRequest(t, ts, "ListTagsForResource",
		map[string]any{"ResourceId": created.ResourcePolicySummary.ID})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource on the resource policy: expected 200, got %d", resp.StatusCode)
	}
	var tagsOut struct {
		Tags []emulator.OrgTag `json:"Tags"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tagsOut); err != nil {
		t.Fatalf("decode ListTagsForResource: %v", err)
	}
	if len(tagsOut.Tags) != 1 || tagsOut.Tags[0].Key != "Owner" || tagsOut.Tags[0].Value != "platform" {
		t.Fatalf("the create-time tag is not readable back: %+v", tagsOut.Tags)
	}

	// A second Put carrying tags is refused rather than silently ignoring them.
	status, _, code = orgPutResourcePolicy(t, ts, map[string]any{
		"Content": `{"Version":"2012-10-17","Statement":[]}`,
		"Tags":    []map[string]any{{"Key": "Owner", "Value": "someone-else"}},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("PutResourcePolicy with tags on an update: expected 400, got %d", status)
	}
	if !strings.HasPrefix(code, "InvalidInputException") {
		t.Fatalf("expected InvalidInputException, got %q", code)
	}

	// An update without tags still succeeds, and leaves the create-time tags alone.
	if status, _, code = orgPutResourcePolicy(t, ts,
		map[string]any{"Content": `{"Version":"2012-10-17","Statement":[]}`}); status != http.StatusOK {
		t.Fatalf("PutResourcePolicy without tags on an update: expected 200, got %d (%s)", status, code)
	}
}

// TestOrganizations_PutResourcePolicy_RejectsReservedTagKeys asserts the resource
// policy's inline tags go through the same validation every other Organizations
// create does.
//
// An "aws:"-prefixed key AWS never lets a caller write could otherwise be planted
// through this operation and then read as aws:ResourceTag by a policy condition.
func TestOrganizations_PutResourcePolicy_RejectsReservedTagKeys(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	status, _, code := orgPutResourcePolicy(t, ts, map[string]any{
		"Content": orgResourcePolicyDoc,
		"Tags":    []map[string]any{{"Key": "aws:cloudformation:stack-name", "Value": "planted"}},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("an aws:-prefixed tag key: expected 400, got %d", status)
	}
	if !strings.HasPrefix(code, "InvalidInputException") {
		t.Fatalf("expected InvalidInputException, got %q", code)
	}

	// Nothing was written, so the failed Put did not leave a policy behind.
	if status, _, _ := orgDescribeResourcePolicy(t, ts); status != http.StatusBadRequest {
		t.Fatal("a refused PutResourcePolicy created the policy anyway")
	}
}

// TestOrganizations_PutResourcePolicy_RejectsUnparseableContent covers the
// INVALID_RESOURCE_POLICY_JSON member of the model's InvalidInputExceptionReason
// enum.
//
// The enum member is the evidence AWS parses the document; the shape's own pattern,
// "[\s\S]*", is not. Accepting an unparseable document would let a test go green on
// a policy real Organizations refuses — the exact false signal the emulator exists to
// prevent.
func TestOrganizations_PutResourcePolicy_RejectsUnparseableContent(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "truncated object", content: `{"Version":`},
		{name: "not JSON at all", content: "this is not a policy"},
		{name: "trailing garbage after a valid document", content: `{"Version":"2012-10-17"} oops`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newOrganizationsTestServer(t)
			status, _, code := orgPutResourcePolicy(t, ts, map[string]any{"Content": tc.content})
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", status)
			}
			// The reason has to be the JSON one, not a length complaint: a caller
			// reading MIN_LENGTH_EXCEEDED would go pad a document whose real problem
			// is its syntax.
			if !strings.Contains(code, "INVALID_RESOURCE_POLICY_JSON") {
				t.Fatalf("expected the INVALID_RESOURCE_POLICY_JSON reason, got %q", code)
			}
		})
	}
}

// TestOrganizations_PutResourcePolicy_TagsRefusalInventsNoReason pins that the
// create-only tags refusal carries no reason prefix.
//
// Every other InvalidInputException here leads with a member of the model's
// InvalidInputExceptionReason enum, and none of them describes this condition. A
// borrowed reason — INVALID_PARTY_TYPE_TARGET, say, which is about handshake
// parties — would read as documented AWS behavior while matching no branch a
// consumer could have written.
func TestOrganizations_PutResourcePolicy_TagsRefusalInventsNoReason(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	if _, _, code := orgPutResourcePolicy(t, ts, map[string]any{"Content": orgResourcePolicyDoc}); code != "" {
		t.Fatalf("PutResourcePolicy: %s", code)
	}
	_, _, code := orgPutResourcePolicy(t, ts, map[string]any{
		"Content": orgResourcePolicyDoc,
		"Tags":    []map[string]any{{"Key": "Owner", "Value": "x"}},
	})
	message, ok := strings.CutPrefix(code, "InvalidInputException/")
	if !ok {
		t.Fatalf("expected InvalidInputException, got %q", code)
	}
	// A reason prefix is SCREAMING_SNAKE_CASE followed by ": ". Nothing in the enum
	// fits this refusal, so the message must start with prose instead.
	if reason, _, found := strings.Cut(message, ": "); found && reason == strings.ToUpper(reason) {
		t.Errorf("the refusal leads with the reason %q, which is not in the model's enum", reason)
	}
}

// TestOrganizations_ResourcePolicy_DeleteRemovesTags asserts the tags go with the
// policy.
//
// Left behind, they would keep an aws:ResourceTag condition matching a resource
// that no longer exists, and a later Put that mints a fresh ID could collide with
// a stale tag set.
func TestOrganizations_ResourcePolicy_DeleteRemovesTags(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	_, created, _ := orgPutResourcePolicy(t, ts, map[string]any{
		"Content": orgResourcePolicyDoc,
		"Tags":    []map[string]any{{"Key": "Owner", "Value": "platform"}},
	})
	id := created.ResourcePolicySummary.ID

	resp := orgsRequest(t, ts, "DeleteResourcePolicy", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteResourcePolicy: expected 200, got %d", resp.StatusCode)
	}

	// The ID now names nothing, so tagging it is TargetNotFoundException — the same
	// answer any other deleted resource gets.
	status, _, code := orgResourcePolicyCall(t, ts, "ListTagsForResource", map[string]any{"ResourceId": id})
	if status != http.StatusBadRequest || !strings.HasPrefix(code, "TargetNotFoundException") {
		t.Fatalf("ListTagsForResource on the deleted policy: expected 400/TargetNotFoundException, got %d/%q",
			status, code)
	}
}

// TestOrganizations_ResourcePolicy_MalformedBodyIsRefused covers the decode guard
// on the one operation of the three that reads a body.
//
// Skipping it would decode into a zero-valued input, where Content is nil — and the
// refusal a caller then gets says its Content was missing when in fact its JSON was
// unparseable, sending it to inspect the wrong part of its request.
func TestOrganizations_ResourcePolicy_MalformedBodyIsRefused(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", newOrgBadBody())
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Organizations_20161128.PutResourcePolicy")
	req.Host = "organizations.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PutResourcePolicy: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for an unparseable body, got %d", resp.StatusCode)
	}
}

// TestOrganizations_ResourcePolicy_StoreReadFailureIsInternalFailure asserts a store
// read failure is a 500 an SDK retries rather than the 400 every
// ResourcePolicyNotFoundException carries.
//
// This lane is the one place the distinction matters most: the not-found refusal is
// the *normal* answer here, so collapsing a transient failure into it would tell a
// caller "management delegated nothing" — a terminal, wrong conclusion — over a blip.
func TestOrganizations_ResourcePolicy_StoreReadFailureIsInternalFailure(t *testing.T) {
	// Both reads every operation performs: the organization each one ensures first,
	// and the policy document itself.
	for _, prefix := range []string{"org:", "resource_policy:"} {
		for _, op := range []string{"PutResourcePolicy", "DescribeResourcePolicy", "DeleteResourcePolicy"} {
			t.Run(prefix+op, func(t *testing.T) {
				inner := emulator.NewMemoryStateManager()
				state := &errOrgState{
					inner: inner, prefix: prefix,
					err: errors.New("store unavailable"), onGet: true,
				}
				ts := newOrgServerOverState(t, state)
				if _, _, code := orgPutResourcePolicy(t, ts,
					map[string]any{"Content": orgResourcePolicyDoc}); code != "" {
					t.Fatalf("warm-up: %s", code)
				}
				state.armed = true

				resp := orgsRequest(t, ts, op, map[string]any{"Content": orgResourcePolicyDoc})
				gotStatus := resp.StatusCode
				resp.Body.Close() //nolint:errcheck
				if gotStatus != http.StatusInternalServerError {
					t.Errorf("expected 500 while %s reads were failing, got %d", prefix, gotStatus)
				}
			})
		}
	}
}

// TestOrganizations_ResourcePolicy_WriteFailureIsNotASuccess asserts a failed write
// is never reported as a completed change.
//
// A Put that answers 200 while the document never landed is the worst outcome
// available: the caller records the ARN and believes it delegated a read that no
// member account can actually make. The tags case is the same claim one level down —
// a 200 with the tag write lost leaves a tag-gated condition reading nothing.
func TestOrganizations_ResourcePolicy_WriteFailureIsNotASuccess(t *testing.T) {
	cases := []struct {
		name    string
		prefix  string
		op      string
		body    map[string]any
		warmUp  bool
		onPut   bool
		onDelet bool
	}{
		{
			name: "the policy document", prefix: "resource_policy:", op: "PutResourcePolicy",
			body: map[string]any{"Content": orgResourcePolicyDoc}, onPut: true,
		},
		{
			name: "the create-time tags", prefix: "tags:", op: "PutResourcePolicy",
			body: map[string]any{
				"Content": orgResourcePolicyDoc,
				"Tags":    []map[string]any{{"Key": "Owner", "Value": "platform"}},
			},
			onPut: true,
		},
		{
			name: "the delete", prefix: "resource_policy:", op: "DeleteResourcePolicy",
			body: map[string]any{}, warmUp: true, onDelet: true, onPut: true,
		},
		{
			name: "the delete's tag cleanup", prefix: "tags:", op: "DeleteResourcePolicy",
			body: map[string]any{}, warmUp: true, onDelet: true, onPut: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{
				inner: inner, prefix: c.prefix, err: errors.New("store unavailable"),
				onPut: c.onPut, onDelete: c.onDelet,
			}
			ts := newOrgServerOverState(t, state)
			if c.warmUp {
				if _, _, code := orgPutResourcePolicy(t, ts, map[string]any{
					"Content": orgResourcePolicyDoc,
					"Tags":    []map[string]any{{"Key": "Owner", "Value": "platform"}},
				}); code != "" {
					t.Fatalf("warm-up: %s", code)
				}
			}
			state.armed = true

			resp := orgsRequest(t, ts, c.op, c.body)
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus == http.StatusOK {
				t.Errorf("%s answered 200 while %s writes were failing", c.op, c.prefix)
			}
		})
	}
}

// TestOrganizations_ResourcePolicy_CorruptRecordIsAnError asserts an unreadable
// document is reported rather than treated as absent.
//
// Absent is the dangerous direction here: the caller's next step is to Put again,
// which would mint a fresh ID over a record that is still in the store — the ID
// stability the round-trip test pins, lost through the failure path.
func TestOrganizations_ResourcePolicy_CorruptRecordIsAnError(t *testing.T) {
	for _, op := range []string{"PutResourcePolicy", "DescribeResourcePolicy", "DeleteResourcePolicy"} {
		t.Run(op, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &corruptOrgState{StateManager: inner, prefix: "resource_policy:"}
			ts := newOrgServerOverState(t, state)
			if _, _, code := orgPutResourcePolicy(t, ts,
				map[string]any{"Content": orgResourcePolicyDoc}); code != "" {
				t.Fatalf("warm-up: %s", code)
			}
			state.armed = true

			resp := orgsRequest(t, ts, op, map[string]any{"Content": orgResourcePolicyDoc})
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("expected 500 over an unreadable resource-policy record, got %d", gotStatus)
			}
		})
	}
}

// TestOrganizations_ResourcePolicyOperationsAreClaimed pins that all three
// operations reach the new cluster.
//
// This is the failure #619 actually reported: an unclaimed operation falls through to
// the unknown-action refusal, which a caller reads as "no such API" rather than
// "substrate has not implemented it", so it hunts for a bug in its own request.
//
// The code to watch for is Organizations' own protocol's: JSON, so
// UnknownOperationException. It was InvalidAction until #716 routed every plugin's
// refusal through one place, and a sentinel still naming that code would silently stop
// catching anything.
func TestOrganizations_ResourcePolicyOperationsAreClaimed(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	for _, op := range []string{"PutResourcePolicy", "DescribeResourcePolicy", "DeleteResourcePolicy"} {
		if _, _, code := orgResourcePolicyCall(t, ts, op, map[string]any{}); strings.HasPrefix(code, "UnknownOperationException") {
			t.Errorf("%s: expected the operation to be claimed, got UnknownOperationException", op)
		}
	}
}

// TestOrganizations_ResourcePolicy_ResolvesAsATarget covers resolveOrgTarget's
// rp- arm, which is what makes the policy taggable.
//
// The organization holds at most one, so the ID has to match the stored one rather
// than index into a collection: a well-formed rp- ID that is not the current
// policy names nothing.
func TestOrganizations_ResourcePolicy_ResolvesAsATarget(t *testing.T) {
	f := newOrgTagsFixture(t)

	kind, err := f.plugin.ResolveOrgTargetForTest(t.Context(), orgTestAccount, "rp-notthere")
	if err != nil {
		t.Fatalf("resolve an absent resource policy: %v", err)
	}
	if kind != "" {
		t.Fatalf("a well-formed rp- ID with no policy set resolved to %q, want absent", kind)
	}

	_, created, code := orgPutResourcePolicy(t, f.ts, map[string]any{"Content": orgResourcePolicyDoc})
	if code != "" {
		t.Fatalf("PutResourcePolicy: %s", code)
	}

	kind, err = f.plugin.ResolveOrgTargetForTest(t.Context(), orgTestAccount, created.ResourcePolicySummary.ID)
	if err != nil {
		t.Fatalf("resolve the resource policy: %v", err)
	}
	if kind != emulator.OrgKindResourcePolicyForTest {
		t.Fatalf("the resource policy resolved to %q, want %q", kind, emulator.OrgKindResourcePolicyForTest)
	}

	// A different well-formed rp- ID still names nothing, rather than borrowing the
	// real policy's identity.
	kind, err = f.plugin.ResolveOrgTargetForTest(t.Context(), orgTestAccount, "rp-someotherpolicy")
	if err != nil {
		t.Fatalf("resolve a non-matching resource policy ID: %v", err)
	}
	if kind != "" {
		t.Fatalf("a non-matching rp- ID resolved to %q, want absent", kind)
	}
}
