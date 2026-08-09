package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// --- helpers ---

// orgPolicyDoc is a minimal well-formed SCP document, used wherever the content
// itself is not what the test is about.
const orgPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"iam:*","Resource":"*"}]}`

// orgPolicyOK posts an operation, asserts 200, and decodes the body.
func orgPolicyOK(t *testing.T, ts *httptest.Server, op string, body, out interface{}) {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		var errBody map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&errBody) //nolint:errcheck
		t.Fatalf("%s: expected 200, got %d (%v)", op, resp.StatusCode, errBody)
	}
	if out == nil {
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("%s decode: %v", op, err)
	}
}

// orgPolicyRefused posts an operation and asserts it is refused at 400 with the
// given error code, returning the message so a test can pin a reason. Every
// Organizations exception is 400, so the status is pinned as well as the code.
func orgPolicyRefused(t *testing.T, ts *httptest.Server, op string, body interface{}, wantCode string) string {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("%s: expected 400, got %d", op, resp.StatusCode)
	}
	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("%s decode: %v", op, err)
	}
	if out.Type != wantCode {
		t.Errorf("%s: expected __type=%s, got %q (%q)", op, wantCode, out.Type, out.Message)
	}
	return out.Message
}

// orgCreatePolicy creates an SCP and returns its ID.
func orgCreatePolicy(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	var out struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "CreatePolicy", map[string]interface{}{
		"Name":        name,
		"Description": "created by " + t.Name(),
		"Type":        emulator.OrgPolicyTypeSCPForTest,
		"Content":     orgPolicyDoc,
	}, &out)
	if out.Policy.PolicySummary.ID == "" {
		t.Fatalf("CreatePolicy %s: no policy ID in the response", name)
	}
	return out.Policy.PolicySummary.ID
}

// orgAttachedPolicies returns the policy IDs ListPoliciesForTarget reports for a
// target, following NextToken so a paginated answer is not mistaken for a short one.
func orgAttachedPolicies(t *testing.T, ts *httptest.Server, targetID string) []string {
	t.Helper()
	ids := []string{}
	token := ""
	for {
		body := map[string]interface{}{"TargetId": targetID, "Filter": emulator.OrgPolicyTypeSCPForTest}
		if token != "" {
			body["NextToken"] = token
		}
		var out struct {
			Policies  []emulator.OrgPolicySummary `json:"Policies"`
			NextToken string                      `json:"NextToken"`
		}
		orgPolicyOK(t, ts, "ListPoliciesForTarget", body, &out)
		for _, s := range out.Policies {
			ids = append(ids, s.ID)
		}
		if out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	slices.Sort(ids)
	return ids
}

// orgSeedFeatureSet points the plugin's control plane at a feature set. The seed
// wins over the stored value, so a CONSOLIDATED_BILLING organization is reachable
// without recreating one.
func orgSeedFeatureSet(t *testing.T, ts *httptest.Server, featureSet string) {
	t.Helper()
	body := strings.NewReader(`{"featureSet":"` + featureSet + `"}`)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/organizations/feature-set", body)
	if err != nil {
		t.Fatalf("build feature-set seed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed feature set: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed feature set: expected 200, got %d", resp.StatusCode)
	}
}

// --- the decisive tests ---

// TestOrganizations_SCPDisabledCreateSucceedsAttachRefused is issue #578's point 6,
// end to end. An all-features organization has SCPs enabled from creation, so the
// unenforced state is only reachable by disabling — and once there, CreatePolicy
// still succeeds while AttachPolicy refuses. A caller that treats create-then-attach
// as one atomic step breaks exactly here, and would otherwise record a guardrail as
// applied when nothing is evaluating it.
func TestOrganizations_SCPDisabledCreateSucceedsAttachRefused(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	var disabled struct {
		Root emulator.OrgRoot `json:"Root"`
	}
	orgPolicyOK(t, ts, "DisablePolicyType", map[string]interface{}{
		"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
	}, &disabled)
	if len(disabled.Root.PolicyTypes) != 0 {
		t.Errorf("expected no policy types on the root after disabling, got %+v", disabled.Root.PolicyTypes)
	}

	// Creating a policy is legal with the type off; only attaching is not.
	policyID := orgCreatePolicy(t, ts, "deny-iam")

	msg := orgPolicyRefused(t, ts, "AttachPolicy", map[string]interface{}{
		"PolicyId": policyID, "TargetId": rootID,
	}, "PolicyTypeNotEnabledException")
	if msg == "" {
		t.Error("expected the refusal to carry a message a caller can log")
	}

	// Nothing is attached anywhere while the type is off, including FullAWSAccess.
	if got := orgAttachedPolicies(t, ts, rootID); len(got) != 0 {
		t.Errorf("expected no attachments while the type is disabled, got %v", got)
	}

	// Re-enabling restores FullAWSAccess only. The policy created in between still
	// exists, but it is not attached: disable-then-enable is a destructive round
	// trip, not a toggle.
	var reenabled struct {
		Root emulator.OrgRoot `json:"Root"`
	}
	orgPolicyOK(t, ts, "EnablePolicyType", map[string]interface{}{
		"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
	}, &reenabled)
	if len(reenabled.Root.PolicyTypes) != 1 || reenabled.Root.PolicyTypes[0].Status != "ENABLED" {
		t.Fatalf("expected SCP ENABLED on the root after re-enabling, got %+v", reenabled.Root.PolicyTypes)
	}
	want := []string{emulator.OrgFullAWSAccessIDForTest}
	if got := orgAttachedPolicies(t, ts, rootID); !slices.Equal(got, want) {
		t.Errorf("expected re-enabling to restore only FullAWSAccess, got %v", got)
	}
	var described struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "DescribePolicy", map[string]interface{}{"PolicyId": policyID}, &described)
	if described.Policy.PolicySummary.ID != policyID {
		t.Errorf("expected the policy created while disabled to survive, got %+v", described.Policy.PolicySummary)
	}
}

// TestOrganizations_DisablePolicyTypeLosesPriorAttachments pins that the
// attachments are *lost*, not remembered. The User Guide says so, and the
// difference matters: a caller that disables the type to make a change and
// re-enables it afterwards has silently removed every guardrail it had attached, and
// only finds out if the emulator does not helpfully restore them.
func TestOrganizations_DisablePolicyTypeLosesPriorAttachments(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	policyID := orgCreatePolicy(t, ts, "deny-iam")
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}, nil)
	before := orgAttachedPolicies(t, ts, rootID)
	if !slices.Contains(before, policyID) {
		t.Fatalf("setup: expected the policy attached, got %v", before)
	}

	orgPolicyOK(t, ts, "DisablePolicyType", map[string]interface{}{
		"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
	}, nil)
	orgPolicyOK(t, ts, "EnablePolicyType", map[string]interface{}{
		"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
	}, nil)

	after := orgAttachedPolicies(t, ts, rootID)
	if slices.Contains(after, policyID) {
		t.Errorf("expected the prior attachment to be lost across a disable/enable round trip, got %v", after)
	}
	if !slices.Contains(after, emulator.OrgFullAWSAccessIDForTest) {
		t.Errorf("expected FullAWSAccess restored on re-enable, got %v", after)
	}

	// Both directions of the index agree: the policy no longer names the root as a
	// target either. A one-sided detach would have ListTargetsForPolicy contradict
	// ListPoliciesForTarget, a state no sequence of API calls can produce.
	var targets struct {
		Targets []emulator.OrgPolicyTargetSummary `json:"Targets"`
	}
	orgPolicyOK(t, ts, "ListTargetsForPolicy", map[string]interface{}{"PolicyId": policyID}, &targets)
	if len(targets.Targets) != 0 {
		t.Errorf("expected the policy to name no targets after the round trip, got %+v", targets.Targets)
	}
}

// TestOrganizations_DisablePolicyTypeClearsTheWholeSubtree asserts the clearing
// reaches OUs and accounts, not just the root. AWS detaches from every entity in
// the root; an emulator that cleared only the root would leave an account still
// carrying an SCP that the root no longer evaluates, which is a state a caller
// cannot reach through the API and so has no handling for.
func TestOrganizations_DisablePolicyTypeClearsTheWholeSubtree(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	// The management account exists from auto-creation and carries FullAWSAccess.
	var accounts struct {
		Accounts []emulator.OrgAccount `json:"Accounts"`
	}
	orgPolicyOK(t, ts, "ListAccounts", map[string]interface{}{}, &accounts)
	if len(accounts.Accounts) == 0 {
		t.Fatal("setup: expected the management account to exist")
	}
	acctID := accounts.Accounts[0].ID
	if got := orgAttachedPolicies(t, ts, acctID); !slices.Contains(got, emulator.OrgFullAWSAccessIDForTest) {
		t.Fatalf("setup: expected the account to carry FullAWSAccess, got %v", got)
	}

	orgPolicyOK(t, ts, "DisablePolicyType", map[string]interface{}{
		"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
	}, nil)

	for _, id := range []string{rootID, acctID} {
		if got := orgAttachedPolicies(t, ts, id); len(got) != 0 {
			t.Errorf("%s: expected every attachment cleared, got %v", id, got)
		}
	}

	orgPolicyOK(t, ts, "EnablePolicyType", map[string]interface{}{
		"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
	}, nil)
	for _, id := range []string{rootID, acctID} {
		want := []string{emulator.OrgFullAWSAccessIDForTest}
		if got := orgAttachedPolicies(t, ts, id); !slices.Equal(got, want) {
			t.Errorf("%s: expected FullAWSAccess restored, got %v", id, got)
		}
	}
}

// TestOrganizations_MinAttachmentFloor is the second decisive case. FullAWSAccess
// can be detached from a target that has another SCP, but the last SCP cannot be
// detached at all: in real AWS an entity with the type enabled and no SCP attached
// denies everything, so a detach that emptied the set would produce the exact
// opposite of what the caller intended, and produce it silently.
func TestOrganizations_MinAttachmentFloor(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)
	policyID := orgCreatePolicy(t, ts, "deny-iam")
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}, nil)

	// Two attached, so detaching FullAWSAccess is allowed — this is the documented
	// way to replace the allow-everything default with a real guardrail.
	orgPolicyOK(t, ts, "DetachPolicy", map[string]interface{}{
		"PolicyId": emulator.OrgFullAWSAccessIDForTest, "TargetId": rootID,
	}, nil)
	want := []string{policyID}
	if got := orgAttachedPolicies(t, ts, rootID); !slices.Equal(got, want) {
		t.Fatalf("expected only the new policy attached, got %v", got)
	}

	// One left, so the floor fires.
	msg := orgPolicyRefused(t, ts, "DetachPolicy", map[string]interface{}{
		"PolicyId": policyID, "TargetId": rootID,
	}, "ConstraintViolationException")
	if !strings.HasPrefix(msg, "MIN_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED:") {
		t.Errorf("expected MIN_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED, got %q", msg)
	}
	if got := orgAttachedPolicies(t, ts, rootID); !slices.Equal(got, want) {
		t.Errorf("expected the refused detach to change nothing, got %v", got)
	}
	if emulator.OrgMinSCPsPerTargetForTest != 1 {
		t.Errorf("expected the documented floor of 1 SCP per target, got %d", emulator.OrgMinSCPsPerTargetForTest)
	}
}

// TestOrganizations_ConsolidatedBillingRefusesEveryPolicyOperation is the third.
// Under CONSOLIDATED_BILLING the policy type does not exist at all, which is a
// different problem from an all-features organization that has it switched off: the
// first needs a migration, the second needs one API call. A caller branches on the
// distinction, so the two must not collapse into one code.
func TestOrganizations_ConsolidatedBillingRefusesEveryPolicyOperation(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)
	// A policy created before the switch, so the refusals are about the feature set
	// rather than about a policy that never existed.
	policyID := orgCreatePolicy(t, ts, "deny-iam")
	orgSeedFeatureSet(t, ts, "CONSOLIDATED_BILLING")

	scp := emulator.OrgPolicyTypeSCPForTest
	cases := []struct {
		op       string
		body     map[string]interface{}
		wantCode string
	}{
		// These two declare PolicyTypeNotAvailableForOrganizationException in the
		// model, and they are where a caller learns the feature set is the problem.
		{"CreatePolicy", map[string]interface{}{
			"Name": "another", "Description": "d", "Type": scp, "Content": orgPolicyDoc,
		}, "PolicyTypeNotAvailableForOrganizationException"},
		{"EnablePolicyType", map[string]interface{}{"RootId": rootID, "PolicyType": scp},
			"PolicyTypeNotAvailableForOrganizationException"},

		// The rest do not declare it, so they answer with the code their own error
		// list carries. Emitting an undeclared exception would hand a caller
		// something its SDK cannot catch by type.
		{"DescribePolicy", map[string]interface{}{"PolicyId": policyID}, "PolicyNotFoundException"},
		{"UpdatePolicy", map[string]interface{}{"PolicyId": policyID, "Name": "x"}, "PolicyNotFoundException"},
		{"DeletePolicy", map[string]interface{}{"PolicyId": policyID}, "PolicyNotFoundException"},
		{"AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}, "PolicyNotFoundException"},
		{"DetachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}, "PolicyNotFoundException"},
		{"ListTargetsForPolicy", map[string]interface{}{"PolicyId": policyID}, "PolicyNotFoundException"},
		{"DisablePolicyType", map[string]interface{}{"RootId": rootID, "PolicyType": scp}, "PolicyTypeNotEnabledException"},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			orgPolicyRefused(t, ts, c.op, c.body, c.wantCode)
		})
	}

	// Even FullAWSAccess is invisible: it is an SCP, and no SCP exists in this mode.
	t.Run("FullAWSAccess is invisible", func(t *testing.T) {
		orgPolicyRefused(t, ts, "DescribePolicy",
			map[string]interface{}{"PolicyId": emulator.OrgFullAWSAccessIDForTest}, "PolicyNotFoundException")
	})

	// The listings answer empty rather than refusing: the model gives them no code
	// for this, and "no policies" is a truthful answer for an organization that can
	// hold none.
	t.Run("ListPolicies is empty", func(t *testing.T) {
		var out struct {
			Policies []emulator.OrgPolicySummary `json:"Policies"`
		}
		orgPolicyOK(t, ts, "ListPolicies", map[string]interface{}{"Filter": scp}, &out)
		if len(out.Policies) != 0 {
			t.Errorf("expected no policies under CONSOLIDATED_BILLING, got %+v", out.Policies)
		}
	})
	t.Run("ListPoliciesForTarget is empty", func(t *testing.T) {
		if got := orgAttachedPolicies(t, ts, rootID); len(got) != 0 {
			t.Errorf("expected no attachments under CONSOLIDATED_BILLING, got %v", got)
		}
	})
}

// TestOrganizations_FullAWSAccessIsImmutable is the fourth. A teardown that deletes
// every policy it can list must not be able to delete the one the minimum-
// attachment rule depends on, and a caller that tries gets IMMUTABLE_POLICY rather
// than a not-found it would read as "already cleaned up".
func TestOrganizations_FullAWSAccessIsImmutable(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	full := emulator.OrgFullAWSAccessIDForTest

	for _, c := range []struct {
		op   string
		body map[string]interface{}
	}{
		{"UpdatePolicy", map[string]interface{}{"PolicyId": full, "Name": "NotFullAccess"}},
		{"UpdatePolicy", map[string]interface{}{"PolicyId": full, "Content": orgPolicyDoc}},
		{"DeletePolicy", map[string]interface{}{"PolicyId": full}},
	} {
		t.Run(c.op, func(t *testing.T) {
			msg := orgPolicyRefused(t, ts, c.op, c.body, "InvalidInputException")
			if !strings.HasPrefix(msg, "IMMUTABLE_POLICY:") {
				t.Errorf("expected IMMUTABLE_POLICY, got %q", msg)
			}
		})
	}

	// It is still there, still AWS-managed, and still owned by the "aws" account
	// rather than by the management account.
	var described struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "DescribePolicy", map[string]interface{}{"PolicyId": full}, &described)
	wantSummary := emulator.FullAWSAccessPolicyForTest()
	if described.Policy.PolicySummary != wantSummary.PolicySummary {
		t.Errorf("expected FullAWSAccess unchanged, got %+v", described.Policy.PolicySummary)
	}
	if !strings.HasPrefix(described.Policy.PolicySummary.Arn, "arn:aws:organizations::aws:policy/") {
		t.Errorf("expected the AWS-owned ARN form, got %q", described.Policy.PolicySummary.Arn)
	}

	var listed struct {
		Policies []emulator.OrgPolicySummary `json:"Policies"`
	}
	orgPolicyOK(t, ts, "ListPolicies", map[string]interface{}{"Filter": emulator.OrgPolicyTypeSCPForTest}, &listed)
	found := false
	for _, s := range listed.Policies {
		if s.ID == full {
			found = true
			if !s.AwsManaged {
				t.Error("expected FullAWSAccess to report AwsManaged=true")
			}
		}
	}
	if !found {
		t.Errorf("expected FullAWSAccess in ListPolicies after the refused writes, got %+v", listed.Policies)
	}
}

// TestOrganizations_AttachmentCapFiresOnTheEleventh is the fifth. The SCP quota is
// 10 per target, not the 5 that belongs to resource control policies, so the cap
// must fire on the 11th attachment — the 10th succeeding is as much the assertion
// as the 11th failing.
func TestOrganizations_AttachmentCapFiresOnTheEleventh(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	if emulator.OrgMaxSCPsPerTargetForTest != 10 {
		t.Fatalf("expected the documented SCP cap of 10 per target, got %d", emulator.OrgMaxSCPsPerTargetForTest)
	}

	// FullAWSAccess already occupies one slot, so nine more reach the cap.
	created := make([]string, 0, emulator.OrgMaxSCPsPerTargetForTest)
	for i := 1; i < emulator.OrgMaxSCPsPerTargetForTest; i++ {
		id := orgCreatePolicy(t, ts, "deny-"+string(rune('a'+i)))
		created = append(created, id)
		orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": id, "TargetId": rootID}, nil)
	}
	if got := orgAttachedPolicies(t, ts, rootID); len(got) != emulator.OrgMaxSCPsPerTargetForTest {
		t.Fatalf("expected exactly %d attachments at the cap, got %d (%v)",
			emulator.OrgMaxSCPsPerTargetForTest, len(got), got)
	}

	overflow := orgCreatePolicy(t, ts, "deny-overflow")
	msg := orgPolicyRefused(t, ts, "AttachPolicy", map[string]interface{}{
		"PolicyId": overflow, "TargetId": rootID,
	}, "ConstraintViolationException")
	if !strings.HasPrefix(msg, "MAX_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED:") {
		t.Errorf("expected MAX_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED, got %q", msg)
	}

	// Detaching one makes room again, so the cap is a live count rather than a
	// high-water mark a caller can never recover from.
	orgPolicyOK(t, ts, "DetachPolicy", map[string]interface{}{
		"PolicyId": created[0], "TargetId": rootID,
	}, nil)
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": overflow, "TargetId": rootID}, nil)
}

// --- refusals ---

// TestOrganizations_PolicyRefusals covers the remaining documented refusals, each
// against the code its operation's error list in the API model declares. They are
// the point of the lane: they are what a re-runnable governance script is built on,
// and each one distinguishes a case a caller must handle differently.
func TestOrganizations_PolicyRefusals(t *testing.T) {
	scp := emulator.OrgPolicyTypeSCPForTest

	t.Run("duplicate policy name", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		orgCreatePolicy(t, ts, "deny-iam")
		// A re-run has to be able to tell "I already made this" from "I made it now",
		// and the name is the only handle it has before it knows the ID.
		orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "deny-iam", "Description": "d", "Type": scp, "Content": orgPolicyDoc,
		}, "DuplicatePolicyException")
	})

	t.Run("duplicate name shadowing FullAWSAccess", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		// FullAWSAccess is a policy of the organization even though it is synthesized,
		// so its name is taken too — a caller that shadowed it would get two policies
		// answering to one name in its own bookkeeping.
		orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "FullAWSAccess", "Description": "d", "Type": scp, "Content": orgPolicyDoc,
		}, "DuplicatePolicyException")
	})

	t.Run("rename onto an existing name", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		orgCreatePolicy(t, ts, "deny-iam")
		second := orgCreatePolicy(t, ts, "deny-s3")
		orgPolicyRefused(t, ts, "UpdatePolicy", map[string]interface{}{
			"PolicyId": second, "Name": "deny-iam",
		}, "DuplicatePolicyException")
	})

	t.Run("rename to its own name", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		id := orgCreatePolicy(t, ts, "deny-iam")
		// Not a duplicate: a re-applied update that sets the same name is the shape a
		// declarative tool sends every run, and refusing it would make the tool
		// non-convergent.
		orgPolicyOK(t, ts, "UpdatePolicy", map[string]interface{}{"PolicyId": id, "Name": "deny-iam"}, nil)
	})

	t.Run("already attached", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		rootID := orgListRootsID(t, ts)
		id := orgCreatePolicy(t, ts, "deny-iam")
		orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": id, "TargetId": rootID}, nil)
		// Attaching is not idempotent, which is the whole reason a re-run needs a
		// distinguishable refusal rather than a silent success.
		orgPolicyRefused(t, ts, "AttachPolicy", map[string]interface{}{
			"PolicyId": id, "TargetId": rootID,
		}, "DuplicatePolicyAttachmentException")
	})

	t.Run("detaching a policy that is not attached", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		rootID := orgListRootsID(t, ts)
		id := orgCreatePolicy(t, ts, "deny-iam")
		// Distinct from the min-attachment floor: nothing was there to remove, so a
		// caller reconciling its own record of the attachment learns its record is
		// stale rather than that it hit a quota.
		orgPolicyRefused(t, ts, "DetachPolicy", map[string]interface{}{
			"PolicyId": id, "TargetId": rootID,
		}, "PolicyNotAttachedException")
	})

	t.Run("unknown policy", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		rootID := orgListRootsID(t, ts)
		unknown := "p-99998888"
		for _, c := range []struct {
			op   string
			body map[string]interface{}
		}{
			{"DescribePolicy", map[string]interface{}{"PolicyId": unknown}},
			{"UpdatePolicy", map[string]interface{}{"PolicyId": unknown, "Name": "x"}},
			{"DeletePolicy", map[string]interface{}{"PolicyId": unknown}},
			{"AttachPolicy", map[string]interface{}{"PolicyId": unknown, "TargetId": rootID}},
			{"DetachPolicy", map[string]interface{}{"PolicyId": unknown, "TargetId": rootID}},
			{"ListTargetsForPolicy", map[string]interface{}{"PolicyId": unknown}},
		} {
			orgPolicyRefused(t, ts, c.op, c.body, "PolicyNotFoundException")
		}
	})

	t.Run("unknown target", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		id := orgCreatePolicy(t, ts, "deny-iam")
		// The policy is checked before the target on attach and detach, so an unknown
		// target is only reachable with a real policy.
		for _, op := range []string{"AttachPolicy", "DetachPolicy"} {
			orgPolicyRefused(t, ts, op, map[string]interface{}{
				"PolicyId": id, "TargetId": "999999999999",
			}, "TargetNotFoundException")
		}
		orgPolicyRefused(t, ts, "ListPoliciesForTarget", map[string]interface{}{
			"TargetId": "999999999999", "Filter": scp,
		}, "TargetNotFoundException")
	})

	t.Run("a policy is not an attachment target", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		id := orgCreatePolicy(t, ts, "deny-iam")
		// A policy ID resolves as an entity — it is taggable — but it is not
		// attachable, and the TargetId pattern in the model does not admit one.
		orgPolicyRefused(t, ts, "AttachPolicy", map[string]interface{}{
			"PolicyId": id, "TargetId": id,
		}, "InvalidInputException")
	})

	t.Run("deleting an attached policy", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		rootID := orgListRootsID(t, ts)
		id := orgCreatePolicy(t, ts, "deny-iam")
		orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": id, "TargetId": rootID}, nil)
		// Refused rather than cascaded: a cascade would remove a guardrail from every
		// entity it was attached to, and a teardown running in the wrong order would
		// look like it worked.
		orgPolicyRefused(t, ts, "DeletePolicy", map[string]interface{}{"PolicyId": id}, "PolicyInUseException")

		// Detaching first makes the delete legal, and the policy is then gone from
		// every listing.
		orgPolicyOK(t, ts, "DetachPolicy", map[string]interface{}{"PolicyId": id, "TargetId": rootID}, nil)
		orgPolicyOK(t, ts, "DeletePolicy", map[string]interface{}{"PolicyId": id}, nil)
		orgPolicyRefused(t, ts, "DescribePolicy", map[string]interface{}{"PolicyId": id}, "PolicyNotFoundException")
		var listed struct {
			Policies []emulator.OrgPolicySummary `json:"Policies"`
		}
		orgPolicyOK(t, ts, "ListPolicies", map[string]interface{}{"Filter": scp}, &listed)
		for _, s := range listed.Policies {
			if s.ID == id {
				t.Error("expected the deleted policy gone from ListPolicies")
			}
		}
	})

	t.Run("unparseable policy content", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		// A caller that shipped an untemplated string learns the document is the
		// problem, not the request. Both create and update check it: an update that
		// accepted garbage would leave a stored policy nothing could evaluate.
		orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "broken", "Description": "d", "Type": scp, "Content": "not a policy",
		}, "MalformedPolicyDocumentException")

		id := orgCreatePolicy(t, ts, "deny-iam")
		orgPolicyRefused(t, ts, "UpdatePolicy", map[string]interface{}{
			"PolicyId": id, "Content": `{"Version":`,
		}, "MalformedPolicyDocumentException")
	})

	t.Run("content over the character limit", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		if emulator.OrgMaxSCPBytesForTest != 10240 {
			t.Fatalf("expected the documented SCP limit of 10240 characters, got %d", emulator.OrgMaxSCPBytesForTest)
		}
		// A well-formed document one character over the limit, so the refusal is about
		// the size and not about the syntax — the two need different fixes (split the
		// policy vs. correct it), so they must not share a code.
		prefix := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"iam:*","Resource":"`
		suffix := `"}]}`
		pad := emulator.OrgMaxSCPBytesForTest + 1 - len(prefix) - len(suffix)
		oversize := prefix + strings.Repeat("x", pad) + suffix
		if len(oversize) != emulator.OrgMaxSCPBytesForTest+1 {
			t.Fatalf("test bug: built a %d-character document", len(oversize))
		}
		msg := orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "big", "Description": "d", "Type": scp, "Content": oversize,
		}, "ConstraintViolationException")
		if !strings.HasPrefix(msg, "POLICY_CONTENT_LIMIT_EXCEEDED:") {
			t.Errorf("expected POLICY_CONTENT_LIMIT_EXCEEDED, got %q", msg)
		}

		// Exactly at the limit is accepted; the boundary is inclusive.
		atLimit := prefix + strings.Repeat("x", pad-1) + suffix
		orgPolicyOK(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "at-limit", "Description": "d", "Type": scp, "Content": atLimit,
		}, nil)
	})

	t.Run("policy type already enabled", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		rootID := orgListRootsID(t, ts)
		// An all-features organization has SCPs on from creation, so the first
		// EnablePolicyType a caller makes is already the second.
		orgPolicyRefused(t, ts, "EnablePolicyType", map[string]interface{}{
			"RootId": rootID, "PolicyType": scp,
		}, "PolicyTypeAlreadyEnabledException")
	})

	t.Run("policy type not enabled", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		rootID := orgListRootsID(t, ts)
		orgPolicyOK(t, ts, "DisablePolicyType", map[string]interface{}{"RootId": rootID, "PolicyType": scp}, nil)
		orgPolicyRefused(t, ts, "DisablePolicyType", map[string]interface{}{
			"RootId": rootID, "PolicyType": scp,
		}, "PolicyTypeNotEnabledException")
	})

	t.Run("unknown root", func(t *testing.T) {
		ts := newOrganizationsTestServer(t)
		// Checked before the feature set: a caller that named the wrong root has a
		// different bug from one whose organization cannot hold the type, and being
		// told about the feature set would send it to fix the wrong thing.
		for _, op := range []string{"EnablePolicyType", "DisablePolicyType"} {
			orgPolicyRefused(t, ts, op, map[string]interface{}{
				"RootId": "r-9999", "PolicyType": scp,
			}, "RootNotFoundException")
		}
	})

	t.Run("policy number limit", func(t *testing.T) {
		// Asserted against the constant and the comparison rather than by creating
		// 10,000 policies: the boundary is what an off-by-one would move, and the
		// count is not reachable in a test that has to run in milliseconds.
		if emulator.OrgMaxSCPsPerOrgForTest != 10000 {
			t.Errorf("expected the documented limit of 10000 SCPs per organization, got %d",
				emulator.OrgMaxSCPsPerOrgForTest)
		}
		for _, c := range []struct {
			count int
			want  bool
		}{
			{0, false},
			{emulator.OrgMaxSCPsPerOrgForTest - 1, false},
			{emulator.OrgMaxSCPsPerOrgForTest, true},
			{emulator.OrgMaxSCPsPerOrgForTest + 1, true},
		} {
			if got := emulator.OrgPolicyNumberLimitExceededForTest(c.count); got != c.want {
				t.Errorf("with %d policies stored: expected exceeded=%v, got %v", c.count, c.want, got)
			}
		}
	})
}

// TestOrganizations_UnsupportedPolicyTypesAreRefused pins the distinction between a
// policy type string the caller typo'd and one that is valid in the API model but
// that substrate does not model. They call for different responses from the caller —
// fix the string, versus do not expect this type here — so they get different codes.
func TestOrganizations_UnsupportedPolicyTypesAreRefused(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	t.Run("outside the enum", func(t *testing.T) {
		for _, c := range []struct {
			op   string
			body map[string]interface{}
		}{
			{"CreatePolicy", map[string]interface{}{
				"Name": "x", "Description": "d", "Type": "NOT_A_POLICY_TYPE", "Content": orgPolicyDoc,
			}},
			{"ListPolicies", map[string]interface{}{"Filter": "NOT_A_POLICY_TYPE"}},
			{"ListPoliciesForTarget", map[string]interface{}{"TargetId": rootID, "Filter": "NOT_A_POLICY_TYPE"}},
			{"EnablePolicyType", map[string]interface{}{"RootId": rootID, "PolicyType": "NOT_A_POLICY_TYPE"}},
			{"DisablePolicyType", map[string]interface{}{"RootId": rootID, "PolicyType": "NOT_A_POLICY_TYPE"}},
		} {
			msg := orgPolicyRefused(t, ts, c.op, c.body, "InvalidInputException")
			if !strings.HasPrefix(msg, "INVALID_ENUM_POLICY_TYPE:") {
				t.Errorf("%s: expected INVALID_ENUM_POLICY_TYPE, got %q", c.op, msg)
			}
		}
	})

	t.Run("valid in the enum but not modeled", func(t *testing.T) {
		// TAG_POLICY is a real AWS policy type. Substrate models only SCPs, so it is
		// unavailable for this organization rather than an invalid string.
		orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "tags", "Description": "d", "Type": "TAG_POLICY", "Content": orgPolicyDoc,
		}, "PolicyTypeNotAvailableForOrganizationException")
		orgPolicyRefused(t, ts, "EnablePolicyType", map[string]interface{}{
			"RootId": rootID, "PolicyType": "TAG_POLICY",
		}, "PolicyTypeNotAvailableForOrganizationException")
		orgPolicyRefused(t, ts, "DisablePolicyType", map[string]interface{}{
			"RootId": rootID, "PolicyType": "TAG_POLICY",
		}, "PolicyTypeNotEnabledException")
	})

	t.Run("listings filtered to an unmodeled type are empty", func(t *testing.T) {
		// An empty page rather than a refusal: an organization with no tag policies is
		// exactly what AWS reports as an empty list, and these operations declare no
		// code to refuse with.
		var out struct {
			Policies []emulator.OrgPolicySummary `json:"Policies"`
		}
		orgPolicyOK(t, ts, "ListPolicies", map[string]interface{}{"Filter": "TAG_POLICY"}, &out)
		if len(out.Policies) != 0 {
			t.Errorf("expected no TAG_POLICY policies, got %+v", out.Policies)
		}
		orgPolicyOK(t, ts, "ListPoliciesForTarget",
			map[string]interface{}{"TargetId": rootID, "Filter": "TAG_POLICY"}, &out)
		if len(out.Policies) != 0 {
			t.Errorf("expected no TAG_POLICY attachments, got %+v", out.Policies)
		}
	})
}

// TestOrganizations_PolicyInputValidation covers the required-member and
// syntax refusals. A malformed policy ID is a syntax error rather than a
// not-found: a caller that passed a policy *name* where an ID belongs would
// otherwise conclude the policy had been deleted and go on to recreate it.
func TestOrganizations_PolicyInputValidation(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)
	scp := emulator.OrgPolicyTypeSCPForTest

	t.Run("required members", func(t *testing.T) {
		cases := []struct {
			name string
			op   string
			body map[string]interface{}
		}{
			// Description is required in the model even though it reads as optional.
			{"CreatePolicy without Name", "CreatePolicy", map[string]interface{}{
				"Description": "d", "Type": scp, "Content": orgPolicyDoc,
			}},
			{"CreatePolicy without Content", "CreatePolicy", map[string]interface{}{
				"Name": "x", "Description": "d", "Type": scp,
			}},
			{"CreatePolicy without Description", "CreatePolicy", map[string]interface{}{
				"Name": "x", "Type": scp, "Content": orgPolicyDoc,
			}},
			{"CreatePolicy without Type", "CreatePolicy", map[string]interface{}{
				"Name": "x", "Description": "d", "Content": orgPolicyDoc,
			}},
			{"DescribePolicy without PolicyId", "DescribePolicy", map[string]interface{}{}},
			{"UpdatePolicy without PolicyId", "UpdatePolicy", map[string]interface{}{"Name": "x"}},
			{"DeletePolicy without PolicyId", "DeletePolicy", map[string]interface{}{}},
			{"AttachPolicy without PolicyId", "AttachPolicy", map[string]interface{}{"TargetId": rootID}},
			{"AttachPolicy without TargetId", "AttachPolicy", map[string]interface{}{"PolicyId": "p-11112222"}},
			{"DetachPolicy without TargetId", "DetachPolicy", map[string]interface{}{"PolicyId": "p-11112222"}},
			{"ListPolicies without Filter", "ListPolicies", map[string]interface{}{}},
			{"ListPoliciesForTarget without Filter", "ListPoliciesForTarget",
				map[string]interface{}{"TargetId": rootID}},
			{"ListPoliciesForTarget without TargetId", "ListPoliciesForTarget",
				map[string]interface{}{"Filter": scp}},
			{"ListTargetsForPolicy without PolicyId", "ListTargetsForPolicy", map[string]interface{}{}},
			{"EnablePolicyType without RootId", "EnablePolicyType", map[string]interface{}{"PolicyType": scp}},
			{"EnablePolicyType without PolicyType", "EnablePolicyType", map[string]interface{}{"RootId": rootID}},
			{"DisablePolicyType without RootId", "DisablePolicyType", map[string]interface{}{"PolicyType": scp}},
			{"DisablePolicyType without PolicyType", "DisablePolicyType", map[string]interface{}{"RootId": rootID}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				msg := orgPolicyRefused(t, ts, c.op, c.body, "InvalidInputException")
				if !strings.HasPrefix(msg, "INPUT_REQUIRED:") {
					t.Errorf("expected INPUT_REQUIRED, got %q", msg)
				}
			})
		}
	})

	t.Run("malformed policy ID", func(t *testing.T) {
		for _, id := range []string{"deny-iam", "p-short", "p-has!bang", "policy-11112222", "p-"} {
			msg := orgPolicyRefused(t, ts, "DescribePolicy", map[string]interface{}{"PolicyId": id}, "InvalidInputException")
			if !strings.HasPrefix(msg, "INVALID_SYNTAX_POLICY_ID:") {
				t.Errorf("%q: expected INVALID_SYNTAX_POLICY_ID, got %q", id, msg)
			}
		}
	})

	t.Run("malformed target ID", func(t *testing.T) {
		id := orgCreatePolicy(t, ts, "deny-target-syntax")
		for _, target := range []string{"root", "12345", "00000000000a", "ou-abcd", "ou-ab-11112222"} {
			msg := orgPolicyRefused(t, ts, "AttachPolicy",
				map[string]interface{}{"PolicyId": id, "TargetId": target}, "InvalidInputException")
			if !strings.HasPrefix(msg, "INVALID_PATTERN_TARGET_ID:") {
				t.Errorf("%q: expected INVALID_PATTERN_TARGET_ID, got %q", target, msg)
			}
		}
	})

	t.Run("name and description lengths", func(t *testing.T) {
		msg := orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": strings.Repeat("n", 129), "Description": "d", "Type": scp, "Content": orgPolicyDoc,
		}, "InvalidInputException")
		if !strings.HasPrefix(msg, "MAX_LENGTH_EXCEEDED:") {
			t.Errorf("expected MAX_LENGTH_EXCEEDED for a 129-character name, got %q", msg)
		}
		msg = orgPolicyRefused(t, ts, "CreatePolicy", map[string]interface{}{
			"Name": "long-description", "Description": strings.Repeat("d", 513), "Type": scp, "Content": orgPolicyDoc,
		}, "InvalidInputException")
		if !strings.HasPrefix(msg, "MAX_LENGTH_EXCEEDED:") {
			t.Errorf("expected MAX_LENGTH_EXCEEDED for a 513-character description, got %q", msg)
		}
		// An empty description is permitted; an empty *name* is not, and each is
		// reachable only through UpdatePolicy, where an empty string is a value rather
		// than an omission.
		id := orgCreatePolicy(t, ts, "deny-lengths")
		orgPolicyOK(t, ts, "UpdatePolicy", map[string]interface{}{"PolicyId": id, "Description": ""}, nil)
		msg = orgPolicyRefused(t, ts, "UpdatePolicy",
			map[string]interface{}{"PolicyId": id, "Name": ""}, "InvalidInputException")
		if !strings.HasPrefix(msg, "MIN_LENGTH_EXCEEDED:") {
			t.Errorf("expected MIN_LENGTH_EXCEEDED for an empty name, got %q", msg)
		}
		msg = orgPolicyRefused(t, ts, "UpdatePolicy",
			map[string]interface{}{"PolicyId": id, "Content": ""}, "InvalidInputException")
		if !strings.HasPrefix(msg, "MIN_LENGTH_EXCEEDED:") {
			t.Errorf("expected MIN_LENGTH_EXCEEDED for empty content, got %q", msg)
		}
	})

	t.Run("unparseable body", func(t *testing.T) {
		// Every handler decodes before it does anything else, so a body no decoder can
		// read is InvalidInputException rather than an answer computed from a
		// zero-valued input.
		for _, op := range []string{
			"CreatePolicy", "UpdatePolicy", "DeletePolicy", "DescribePolicy", "ListPolicies",
			"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "ListTargetsForPolicy",
			"EnablePolicyType", "DisablePolicyType",
		} {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", newOrgBadBody())
			if err != nil {
				t.Fatalf("%s: build request: %v", op, err)
			}
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "Organizations_20161128."+op)
			req.Host = "organizations.us-east-1.amazonaws.com"
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s: %v", op, err)
			}
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusBadRequest {
				t.Errorf("%s: expected 400 for an unparseable body, got %d", op, gotStatus)
			}
		}
	})
}

// --- nominal shapes ---

// TestOrganizations_PolicyLifecycleShapes pins the response shapes against the API
// model: a wrong member name is invisible to a test that only checks the status, and
// is exactly what breaks a typed SDK.
func TestOrganizations_PolicyLifecycleShapes(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	scp := emulator.OrgPolicyTypeSCPForTest

	var created struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "CreatePolicy", map[string]interface{}{
		"Name": "deny-iam", "Description": "blocks IAM", "Type": scp, "Content": orgPolicyDoc,
	}, &created)

	summary := created.Policy.PolicySummary
	if summary.Name != "deny-iam" || summary.Description != "blocks IAM" || summary.Type != scp {
		t.Errorf("unexpected summary: %+v", summary)
	}
	if summary.AwsManaged {
		t.Error("expected a caller-created policy to report AwsManaged=false")
	}
	if created.Policy.Content != orgPolicyDoc {
		t.Errorf("expected the content echoed verbatim, got %q", created.Policy.Content)
	}
	// The ID and the ARN each have to satisfy the model's own pattern. PolicyId
	// admits 8-128 characters after "p-" while the ARN pattern requires 10-32, so a
	// generated suffix has to satisfy both.
	if !emulator.IsOrgPolicyIDSyntaxForTest(summary.ID) {
		t.Errorf("policy ID %q does not match the model's PolicyId pattern", summary.ID)
	}
	if len(strings.TrimPrefix(summary.ID, "p-")) < 10 {
		t.Errorf("policy ID %q is too short for the ARN pattern's 10-character minimum", summary.ID)
	}
	var org struct {
		Organization emulator.Organization `json:"Organization"`
	}
	orgPolicyOK(t, ts, "DescribeOrganization", map[string]interface{}{}, &org)
	wantArn := "arn:aws:organizations::" + orgTestAccount + ":policy/" + org.Organization.ID +
		"/service_control_policy/" + summary.ID
	if summary.Arn != wantArn {
		t.Errorf("expected ARN %q, got %q", wantArn, summary.Arn)
	}

	// DescribePolicy returns the same nested Policy{PolicySummary, Content}.
	var described struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "DescribePolicy", map[string]interface{}{"PolicyId": summary.ID}, &described)
	if described.Policy != created.Policy {
		t.Errorf("expected DescribePolicy to match CreatePolicy, got %+v", described.Policy)
	}

	// UpdatePolicy is a partial update: an omitted member leaves the stored value
	// alone. Collapsing omitted with empty would blank the description on a rename.
	var updated struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "UpdatePolicy", map[string]interface{}{
		"PolicyId": summary.ID, "Name": "deny-iam-v2",
	}, &updated)
	if updated.Policy.PolicySummary.Name != "deny-iam-v2" {
		t.Errorf("expected the rename applied, got %q", updated.Policy.PolicySummary.Name)
	}
	if updated.Policy.PolicySummary.Description != "blocks IAM" {
		t.Errorf("expected an omitted Description to be left alone, got %q", updated.Policy.PolicySummary.Description)
	}
	if updated.Policy.Content != orgPolicyDoc {
		t.Errorf("expected an omitted Content to be left alone, got %q", updated.Policy.Content)
	}
	if updated.Policy.PolicySummary.ID != summary.ID || updated.Policy.PolicySummary.Arn != summary.Arn {
		t.Error("expected an update to preserve the policy's identity")
	}

	// AttachPolicy, DetachPolicy and DeletePolicy have no output shape in the model,
	// so they answer with an empty JSON object.
	rootID := orgListRootsID(t, ts)
	for _, c := range []struct {
		op   string
		body map[string]interface{}
	}{
		{"AttachPolicy", map[string]interface{}{"PolicyId": summary.ID, "TargetId": rootID}},
		{"DetachPolicy", map[string]interface{}{"PolicyId": summary.ID, "TargetId": rootID}},
		{"DeletePolicy", map[string]interface{}{"PolicyId": summary.ID}},
	} {
		resp := orgsRequest(t, ts, c.op, c.body)
		body := make([]byte, 64)
		n, _ := resp.Body.Read(body) //nolint:errcheck
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", c.op, gotStatus)
		}
		if got := string(body[:n]); got != "{}" {
			t.Errorf("%s: expected an empty JSON object, got %q", c.op, got)
		}
	}
}

// TestOrganizations_ListTargetsForPolicyShapes pins the target summaries. Each of
// the three kinds carries its own Type, ARN and Name, and a caller uses Type to
// decide whether the attachment governs one account or a whole subtree.
func TestOrganizations_ListTargetsForPolicyShapes(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	var accounts struct {
		Accounts []emulator.OrgAccount `json:"Accounts"`
	}
	orgPolicyOK(t, ts, "ListAccounts", map[string]interface{}{}, &accounts)
	if len(accounts.Accounts) == 0 {
		t.Fatal("setup: expected the management account")
	}
	acct := accounts.Accounts[0]

	policyID := orgCreatePolicy(t, ts, "deny-iam")
	for _, target := range []string{rootID, acct.ID} {
		orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": target}, nil)
	}

	var out struct {
		Targets []emulator.OrgPolicyTargetSummary `json:"Targets"`
	}
	orgPolicyOK(t, ts, "ListTargetsForPolicy", map[string]interface{}{"PolicyId": policyID}, &out)
	byID := map[string]emulator.OrgPolicyTargetSummary{}
	for _, s := range out.Targets {
		byID[s.TargetID] = s
	}
	if len(byID) != 2 {
		t.Fatalf("expected 2 targets, got %+v", out.Targets)
	}
	if got := byID[rootID]; got.Type != emulator.OrgKindRootForTest || got.Name != "Root" || got.Arn == "" {
		t.Errorf("unexpected root target summary: %+v", got)
	}
	if got := byID[acct.ID]; got.Type != emulator.OrgKindAccountForTest || got.Name != acct.Name || got.Arn != acct.Arn {
		t.Errorf("unexpected account target summary: %+v", got)
	}

	// FullAWSAccess reports its targets too, which is how a caller confirms a fresh
	// organization is governed by the allow-everything default rather than by nothing.
	orgPolicyOK(t, ts, "ListTargetsForPolicy",
		map[string]interface{}{"PolicyId": emulator.OrgFullAWSAccessIDForTest}, &out)
	if len(out.Targets) < 2 {
		t.Errorf("expected FullAWSAccess attached to at least the root and the management account, got %+v", out.Targets)
	}
}

// TestOrganizations_PolicyListPagination asserts every List* in the lane honors
// MaxResults and NextToken. A caller that ignores NextToken gets a truncated answer
// with no error, and for attachments that means concluding a policy is not attached
// when it is — so the truncation has to be observable for the caller's paging loop
// to be testable at all.
func TestOrganizations_PolicyListPagination(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	rootID := orgListRootsID(t, ts)

	ids := []string{emulator.OrgFullAWSAccessIDForTest}
	for i := range 3 {
		id := orgCreatePolicy(t, ts, "deny-"+string(rune('a'+i)))
		ids = append(ids, id)
		orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": id, "TargetId": rootID}, nil)
	}
	slices.Sort(ids)

	t.Run("ListPolicies", func(t *testing.T) {
		seen := []string{}
		token := ""
		for range len(ids) + 1 {
			body := map[string]interface{}{"Filter": emulator.OrgPolicyTypeSCPForTest, "MaxResults": 2}
			if token != "" {
				body["NextToken"] = token
			}
			var out struct {
				Policies  []emulator.OrgPolicySummary `json:"Policies"`
				NextToken string                      `json:"NextToken"`
			}
			orgPolicyOK(t, ts, "ListPolicies", body, &out)
			if len(out.Policies) > 2 {
				t.Fatalf("expected MaxResults=2 honored, got %d", len(out.Policies))
			}
			for _, s := range out.Policies {
				seen = append(seen, s.ID)
			}
			if out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		slices.Sort(seen)
		if !slices.Equal(seen, ids) {
			t.Errorf("expected paging to see every policy exactly once: want %v, got %v", ids, seen)
		}
	})

	t.Run("ListPoliciesForTarget", func(t *testing.T) {
		var out struct {
			Policies  []emulator.OrgPolicySummary `json:"Policies"`
			NextToken string                      `json:"NextToken"`
		}
		orgPolicyOK(t, ts, "ListPoliciesForTarget", map[string]interface{}{
			"TargetId": rootID, "Filter": emulator.OrgPolicyTypeSCPForTest, "MaxResults": 1,
		}, &out)
		if len(out.Policies) != 1 || out.NextToken == "" {
			t.Fatalf("expected a truncated first page with a token, got %d policies and token %q",
				len(out.Policies), out.NextToken)
		}
		// The full walk agrees with the truncated one, so the token is a cursor rather
		// than a restart.
		if got := orgAttachedPolicies(t, ts, rootID); !slices.Equal(got, ids) {
			t.Errorf("expected paging to see every attachment: want %v, got %v", ids, got)
		}
	})

	t.Run("ListTargetsForPolicy", func(t *testing.T) {
		full := emulator.OrgFullAWSAccessIDForTest
		var out struct {
			Targets   []emulator.OrgPolicyTargetSummary `json:"Targets"`
			NextToken string                            `json:"NextToken"`
		}
		orgPolicyOK(t, ts, "ListTargetsForPolicy",
			map[string]interface{}{"PolicyId": full, "MaxResults": 1}, &out)
		if len(out.Targets) != 1 || out.NextToken == "" {
			t.Fatalf("expected a truncated first page with a token, got %d targets and token %q",
				len(out.Targets), out.NextToken)
		}
	})

	t.Run("stale token", func(t *testing.T) {
		// A token naming no known ID is refused rather than silently restarting.
		// Restarting is the worst answer available to a paging loop: it never
		// terminates and there is no error to explain why.
		for _, c := range []struct {
			op   string
			body map[string]interface{}
		}{
			{"ListPolicies", map[string]interface{}{
				"Filter": emulator.OrgPolicyTypeSCPForTest, "NextToken": "bm90LWEtcmVhbC1pZA==",
			}},
			{"ListPoliciesForTarget", map[string]interface{}{
				"TargetId": rootID, "Filter": emulator.OrgPolicyTypeSCPForTest, "NextToken": "bm90LWEtcmVhbC1pZA==",
			}},
			{"ListTargetsForPolicy", map[string]interface{}{
				"PolicyId": emulator.OrgFullAWSAccessIDForTest, "NextToken": "bm90LWEtcmVhbC1pZA==",
			}},
		} {
			msg := orgPolicyRefused(t, ts, c.op, c.body, "InvalidInputException")
			if !strings.HasPrefix(msg, "INVALID_NEXT_TOKEN:") {
				t.Errorf("%s: expected INVALID_NEXT_TOKEN, got %q", c.op, msg)
			}
		}
	})
}

// TestOrganizations_ListPoliciesIncludesFullAWSAccessOnAFreshOrganization pins the
// state of an organization nobody has touched. FullAWSAccess is synthesized rather
// than stored, so a listing that read only the stored index would report no policies
// at all — and a caller that concluded a fresh organization has no SCPs would also
// conclude nothing is allowed, the reverse of the truth.
func TestOrganizations_ListPoliciesIncludesFullAWSAccessOnAFreshOrganization(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	var out struct {
		Policies  []emulator.OrgPolicySummary `json:"Policies"`
		NextToken string                      `json:"NextToken"`
	}
	orgPolicyOK(t, ts, "ListPolicies", map[string]interface{}{"Filter": emulator.OrgPolicyTypeSCPForTest}, &out)
	if len(out.Policies) != 1 {
		t.Fatalf("expected exactly FullAWSAccess on a fresh organization, got %+v", out.Policies)
	}
	if out.NextToken != "" {
		t.Errorf("expected no NextToken for a single-page listing, got %q", out.NextToken)
	}
	want := emulator.FullAWSAccessPolicyForTest().PolicySummary
	if out.Policies[0] != want {
		t.Errorf("expected %+v, got %+v", want, out.Policies[0])
	}

	// And it is attached to the root, which is what makes the min-attachment floor
	// enforceable from the first request.
	got := orgAttachedPolicies(t, ts, orgListRootsID(t, ts))
	if !slices.Equal(got, []string{emulator.OrgFullAWSAccessIDForTest}) {
		t.Errorf("expected FullAWSAccess attached to the root, got %v", got)
	}
}

// TestOrganizations_PolicyDeleteRemovesTags asserts a deleted policy's tags go with
// it. A later policy that reused the ID would otherwise inherit them, and since tags
// reach the authorization decision that would fail open on a tag-gated policy.
func TestOrganizations_PolicyDeleteRemovesTags(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newOrgPolicyServer(t, state)
	p := emulator.NewOrganizationsPluginForTest(state, emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))

	var created struct {
		Policy emulator.OrgPolicy `json:"Policy"`
	}
	orgPolicyOK(t, ts, "CreatePolicy", map[string]interface{}{
		"Name": "deny-iam", "Description": "d", "Type": emulator.OrgPolicyTypeSCPForTest,
		"Content": orgPolicyDoc,
		"Tags":    []emulator.OrgTag{{Key: "Owner", Value: "platform"}},
	}, &created)
	id := created.Policy.PolicySummary.ID

	orgPolicyOK(t, ts, "DeletePolicy", map[string]interface{}{"PolicyId": id}, nil)
	orgPolicyRefused(t, ts, "DescribePolicy", map[string]interface{}{"PolicyId": id}, "PolicyNotFoundException")

	tags, err := p.LoadTagsForTest(t.Context(), id)
	if err != nil {
		t.Fatalf("load tags: %v", err)
	}
	if len(tags) != 0 {
		t.Errorf("expected the deleted policy's tags removed, got %+v", tags)
	}
}

// TestOrganizations_PolicyAttachmentToAnOU covers the OU branch of the attachment
// path. An OU is where a governance script actually attaches an SCP — attaching per
// account is the thing OUs exist to avoid — and its summary carries a different Type
// from a root's, which is how a caller tells "this governs one account" from "this
// governs a subtree". The OU is created through the storage layer because the OU
// operations belong to a different cluster; the attachment path being exercised is
// the same one AttachPolicy takes for any target.
func TestOrganizations_PolicyAttachmentToAnOU(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newOrgPolicyServer(t, state)
	p := emulator.NewOrganizationsPluginForTest(state, emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	ctx := t.Context()

	rootID := orgListRootsID(t, ts)
	ouID := "ou-" + rootID[2:] + "-11112222"
	ou := emulator.OrgOrganizationalUnit{
		ID:   ouID,
		Arn:  "arn:aws:organizations::" + orgTestAccount + ":ou/o-test/" + ouID,
		Name: "prod",
	}
	if err := p.SaveOUForTest(ctx, orgTestAccount, ou); err != nil {
		t.Fatalf("save OU: %v", err)
	}
	if err := p.PlaceChildForTest(ctx, rootID, ouID); err != nil {
		t.Fatalf("place OU: %v", err)
	}
	if err := p.AttachFullAWSAccessForTest(ctx, orgTestAccount, ouID); err != nil {
		t.Fatalf("attach FullAWSAccess to the OU: %v", err)
	}

	policyID := orgCreatePolicy(t, ts, "deny-iam")
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": ouID}, nil)

	var targets struct {
		Targets []emulator.OrgPolicyTargetSummary `json:"Targets"`
	}
	orgPolicyOK(t, ts, "ListTargetsForPolicy", map[string]interface{}{"PolicyId": policyID}, &targets)
	if len(targets.Targets) != 1 {
		t.Fatalf("expected the OU as the only target, got %+v", targets.Targets)
	}
	got := targets.Targets[0]
	if got.TargetID != ouID || got.Type != emulator.OrgKindOUForTest || got.Name != "prod" || got.Arn != ou.Arn {
		t.Errorf("unexpected OU target summary: %+v", got)
	}

	// Disabling the type clears the OU's attachments too, and re-enabling restores
	// FullAWSAccess to it. An OU left carrying an SCP the root no longer evaluates is
	// a state no sequence of API calls can reach.
	orgPolicyOK(t, ts, "DisablePolicyType",
		map[string]interface{}{"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest}, nil)
	if attached := orgAttachedPolicies(t, ts, ouID); len(attached) != 0 {
		t.Errorf("expected the OU's attachments cleared, got %v", attached)
	}
	orgPolicyOK(t, ts, "EnablePolicyType",
		map[string]interface{}{"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest}, nil)
	want := []string{emulator.OrgFullAWSAccessIDForTest}
	if attached := orgAttachedPolicies(t, ts, ouID); !slices.Equal(attached, want) {
		t.Errorf("expected FullAWSAccess restored to the OU, got %v", attached)
	}

	// A target whose OU record has gone is skipped rather than summarized empty.
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": ouID}, nil)
	if err := state.Delete(ctx, "organizations", "ou:"+ouID); err != nil {
		t.Fatalf("delete OU record: %v", err)
	}
	orgPolicyOK(t, ts, "ListTargetsForPolicy", map[string]interface{}{"PolicyId": policyID}, &targets)
	if len(targets.Targets) != 0 {
		t.Errorf("expected the vanished OU skipped, got %+v", targets.Targets)
	}
}

// TestOrganizations_TargetSummarySkipsAVanishedRoot covers the root branch of the
// same skip. The root ID is stable for the life of the state store, so the only way
// its record disagrees with an attachment index is a store written by something
// else — and answering with a half-filled summary would put an empty Id in front of
// a caller that has no way to trace it.
func TestOrganizations_TargetSummarySkipsAVanishedRoot(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newOrgPolicyServer(t, state)
	rootID := orgListRootsID(t, ts)
	policyID := orgCreatePolicy(t, ts, "deny-iam")
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}, nil)

	// A stale root ID in the attachment index, as a store rewritten under the
	// emulator would leave it.
	stale := "r-999a"
	if err := state.Put(t.Context(), "organizations", "policy_targets:"+policyID,
		[]byte(`["`+stale+`"]`)); err != nil {
		t.Fatalf("rewrite the target index: %v", err)
	}

	var targets struct {
		Targets []emulator.OrgPolicyTargetSummary `json:"Targets"`
	}
	orgPolicyOK(t, ts, "ListTargetsForPolicy", map[string]interface{}{"PolicyId": policyID}, &targets)
	if len(targets.Targets) != 0 {
		t.Errorf("expected a root ID that is not this organization's root to be skipped, got %+v", targets.Targets)
	}
}

// --- store failures and corrupt records ---

// newOrgPolicyServer wires a plugin over the supplied state manager onto an HTTP
// server, so the policy operations can be driven while the store misbehaves.
func newOrgPolicyServer(t *testing.T, state emulator.StateManager) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// orgPolicyStoreGroup is a set of operations that all read one state-key prefix
// and all need the same warm-up. The mapping is spelled out rather than asserted
// loosely ("some error happened") because a loose assertion passes when an
// operation stops reading a store it needs — and an operation answering from a
// store it never consulted is exactly the bug this covers.
type orgPolicyStoreGroup struct {
	// name is the subtest name; two groups can share a prefix with different
	// warm-ups.
	name string

	// prefix is the state-key prefix the fault applies to.
	prefix string

	// ops are the operations expected to read it.
	ops []string

	// disableSCP turns the policy type off before the fault is armed. It is the
	// only way to reach EnablePolicyType's body: an all-features organization starts
	// enabled, so the request would otherwise stop at
	// PolicyTypeAlreadyEnabledException and the test would pass having exercised
	// nothing.
	disableSCP bool

	// useFullAWSAccess drives the operation against p-FullAWSAccess, which is the
	// only policy attached to anything after warm-up and so the only one whose
	// target summaries are built at all.
	useFullAWSAccess bool
}

// orgPolicyStoreReads is the prefix-to-operation map the store-fault and
// corrupt-record tests share.
var orgPolicyStoreReads = []orgPolicyStoreGroup{
	{name: "policy", prefix: "policy:", ops: []string{
		"CreatePolicy", "UpdatePolicy", "DeletePolicy", "DescribePolicy", "ListPolicies",
		"AttachPolicy", "DetachPolicy", "ListTargetsForPolicy",
	}},
	{name: "policy_ids", prefix: "policy_ids:", ops: []string{
		"CreatePolicy", "UpdatePolicy", "DeletePolicy", "ListPolicies",
	}},
	{name: "attachments", prefix: "attachments:", ops: []string{
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "DisablePolicyType",
	}},
	{name: "attachments on enable", prefix: "attachments:", disableSCP: true,
		ops: []string{"EnablePolicyType"}},
	{name: "policy_targets", prefix: "policy_targets:", ops: []string{
		"DeletePolicy", "ListTargetsForPolicy", "DisablePolicyType",
	}},
	{name: "policy_targets on enable", prefix: "policy_targets:", disableSCP: true,
		ops: []string{"EnablePolicyType"}},
	{name: "root", prefix: "root:", ops: []string{
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "EnablePolicyType", "DisablePolicyType",
	}},
	// The feature-set seed decides whether SCPs exist at all, so every operation in
	// the lane consults it. A failure to read it must not default to "available":
	// that would have the emulator answer as though the organization were
	// all-features when it cannot tell, and a consolidated-billing test would then
	// silently exercise the wrong path while passing.
	{name: "feature-set seed", prefix: "feature-set", ops: []string{
		"CreatePolicy", "UpdatePolicy", "DeletePolicy", "DescribePolicy", "ListPolicies",
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "ListTargetsForPolicy",
		"EnablePolicyType", "DisablePolicyType",
	}},
	// EnablePolicyType and DisablePolicyType walk every entity in the root, so they
	// depend on the OU and account indexes. A partial walk is the dangerous outcome:
	// some entities cleared and others not is a state no sequence of API calls can
	// produce, and no caller has handling for it.
	{name: "ou_ids on enable", prefix: "ou_ids:", disableSCP: true, ops: []string{"EnablePolicyType"}},
	{name: "ou_ids on disable", prefix: "ou_ids:", ops: []string{"DisablePolicyType"}},
	{name: "account_ids on enable", prefix: "account_ids:", disableSCP: true, ops: []string{"EnablePolicyType"}},
	{name: "account_ids on disable", prefix: "account_ids:", ops: []string{"DisablePolicyType"}},
	// ListTargetsForPolicy summarizes each target from its own record.
	{name: "account", prefix: "account:", useFullAWSAccess: true, ops: []string{"ListTargetsForPolicy"}},
}

// orgPolicyOpBody returns a well-formed request body for op, so a fault-injection
// test reaches the store rather than stopping at validation.
func orgPolicyOpBody(op, rootID, policyID string) map[string]interface{} {
	scp := emulator.OrgPolicyTypeSCPForTest
	switch op {
	case "CreatePolicy":
		return map[string]interface{}{"Name": "another", "Description": "d", "Type": scp, "Content": orgPolicyDoc}
	case "UpdatePolicy":
		return map[string]interface{}{"PolicyId": policyID, "Name": "renamed"}
	case "DeletePolicy", "DescribePolicy", "ListTargetsForPolicy":
		return map[string]interface{}{"PolicyId": policyID}
	case "ListPolicies":
		return map[string]interface{}{"Filter": scp}
	case "AttachPolicy", "DetachPolicy":
		return map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}
	case "ListPoliciesForTarget":
		return map[string]interface{}{"TargetId": rootID, "Filter": scp}
	case "EnablePolicyType", "DisablePolicyType":
		return map[string]interface{}{"RootId": rootID, "PolicyType": scp}
	default:
		return map[string]interface{}{}
	}
}

// orgPolicyFaultWarmUp brings an organization, a root and a policy into existence
// before a fault is armed, and returns the root and policy IDs the group's
// operations should name. Warming up first is what makes the fault reach the
// operation rather than the setup.
func orgPolicyFaultWarmUp(t *testing.T, ts *httptest.Server, group orgPolicyStoreGroup) (rootID, policyID string) {
	t.Helper()
	rootID = orgListRootsID(t, ts)
	policyID = orgCreatePolicy(t, ts, "deny-iam")
	if group.useFullAWSAccess {
		policyID = emulator.OrgFullAWSAccessIDForTest
	}
	if group.disableSCP {
		orgPolicyOK(t, ts, "DisablePolicyType", map[string]interface{}{
			"RootId": rootID, "PolicyType": emulator.OrgPolicyTypeSCPForTest,
		}, nil)
	}
	return rootID, policyID
}

// TestOrganizations_PolicyStoreFailureIsInternalFailure asserts a store failure
// reaching a policy operation is a 500 InternalFailure, which an SDK retries, rather
// than a 400 it treats as terminal. Collapsing a transient store failure into a
// refusal would send a consumer down a permanent-failure path over a blip — and for
// AttachPolicy specifically, a 400 would read as "the guardrail cannot be applied"
// when the truth is "we could not tell".
func TestOrganizations_PolicyStoreFailureIsInternalFailure(t *testing.T) {
	for _, group := range orgPolicyStoreReads {
		t.Run(group.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{inner: inner, prefix: group.prefix, err: errors.New("store unavailable"), onGet: true}
			ts := newOrgPolicyServer(t, state)
			rootID, policyID := orgPolicyFaultWarmUp(t, ts, group)
			state.armed = true

			for _, op := range group.ops {
				resp := orgsRequest(t, ts, op, orgPolicyOpBody(op, rootID, policyID))
				gotStatus := resp.StatusCode
				resp.Body.Close() //nolint:errcheck
				if gotStatus != http.StatusInternalServerError {
					t.Errorf("%s: expected 500 while %s reads were failing, got %d", op, group.prefix, gotStatus)
				}
			}
		})
	}
}

// TestOrganizations_PolicyWriteFailureIsNotASuccess asserts a failed write is never
// reported as a completed change. A CreatePolicy that answers 200 while the record
// never landed is the worst outcome available: the caller records a policy ID that
// no subsequent call can find, and a governance script believes a guardrail exists.
func TestOrganizations_PolicyWriteFailureIsNotASuccess(t *testing.T) {
	scp := emulator.OrgPolicyTypeSCPForTest
	cases := []struct {
		name   string
		prefix string
		op     string
		// disableFirst turns the policy type off during warm-up, which is the only
		// way to reach EnablePolicyType's write path — an all-features organization
		// starts enabled, so an armed store would otherwise be masked by
		// PolicyTypeAlreadyEnabledException and the test would pass having exercised
		// nothing.
		disableFirst bool
	}{
		{name: "create record", prefix: "policy:", op: "CreatePolicy"},
		{name: "create index", prefix: "policy_ids:", op: "CreatePolicy"},
		{name: "attach", prefix: "attachments:", op: "AttachPolicy"},
		{name: "attach reverse index", prefix: "policy_targets:", op: "AttachPolicy"},
		{name: "enable", prefix: "root:", op: "EnablePolicyType", disableFirst: true},
		{name: "disable", prefix: "root:", op: "DisablePolicyType"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{inner: inner, prefix: c.prefix, err: errors.New("store unavailable"), onPut: true}
			ts := newOrgPolicyServer(t, state)
			rootID := orgListRootsID(t, ts)
			policyID := orgCreatePolicy(t, ts, "deny-iam")
			if c.disableFirst {
				orgPolicyOK(t, ts, "DisablePolicyType",
					map[string]interface{}{"RootId": rootID, "PolicyType": scp}, nil)
			}
			state.armed = true

			resp := orgsRequest(t, ts, c.op, orgPolicyOpBody(c.op, rootID, policyID))
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("%s: expected 500 while %s writes were failing, got %d", c.op, c.prefix, gotStatus)
			}
		})
	}
}

// TestOrganizations_PolicyDeleteFailureIsNotASuccess covers the delete path's
// writes. A delete reported successful with the record still in the store leaves a
// policy a listing still shows, which a teardown script reads as a leak it cannot
// clean up.
func TestOrganizations_PolicyDeleteFailureIsNotASuccess(t *testing.T) {
	for _, prefix := range []string{"policy:", "policy_ids:"} {
		t.Run(prefix, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{
				inner: inner, prefix: prefix, err: errors.New("store unavailable"),
				onDelete: true, onPut: true,
			}
			ts := newOrgPolicyServer(t, state)
			policyID := orgCreatePolicy(t, ts, "deny-iam")
			state.armed = true

			resp := orgsRequest(t, ts, "DeletePolicy", map[string]interface{}{"PolicyId": policyID})
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus == http.StatusOK {
				t.Errorf("DeletePolicy answered 200 while %s writes were failing", prefix)
			}
		})
	}
}

// TestOrganizations_CorruptPolicyRecordIsAnError asserts an unreadable policy is
// reported rather than treated as absent. Absent is the dangerous direction: the
// caller's next step is to create the policy again, on top of one that is still in
// the store, and it would then hold two IDs for one guardrail.
func TestOrganizations_CorruptPolicyRecordIsAnError(t *testing.T) {
	for _, group := range orgPolicyStoreReads {
		t.Run(group.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &corruptOrgState{StateManager: inner, prefix: group.prefix}
			ts := newOrgPolicyServer(t, state)
			rootID, policyID := orgPolicyFaultWarmUp(t, ts, group)
			state.armed = true

			for _, op := range group.ops {
				resp := orgsRequest(t, ts, op, orgPolicyOpBody(op, rootID, policyID))
				gotStatus := resp.StatusCode
				resp.Body.Close() //nolint:errcheck
				if gotStatus != http.StatusInternalServerError {
					t.Errorf("%s: expected 500 over an unreadable %s record, got %d", op, group.prefix, gotStatus)
				}
			}
		})
	}
}

// TestOrganizations_ListingsSkipVanishedRecords asserts a listing tolerates an index
// entry whose record is gone rather than reporting a zero-valued member. A summary
// with an empty Id is worse than a short page: a consumer iterating it calls
// DescribePolicy with "" and gets a refusal it has nothing to trace to.
func TestOrganizations_ListingsSkipVanishedRecords(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newOrgPolicyServer(t, state)
	rootID := orgListRootsID(t, ts)
	policyID := orgCreatePolicy(t, ts, "deny-iam")
	orgPolicyOK(t, ts, "AttachPolicy", map[string]interface{}{"PolicyId": policyID, "TargetId": rootID}, nil)

	// The indexes still name the policy; its record does not exist.
	if err := state.Delete(t.Context(), "organizations", "policy:"+policyID); err != nil {
		t.Fatalf("delete policy record: %v", err)
	}

	var out struct {
		Policies []emulator.OrgPolicySummary `json:"Policies"`
	}
	orgPolicyOK(t, ts, "ListPolicies", map[string]interface{}{"Filter": emulator.OrgPolicyTypeSCPForTest}, &out)
	for _, s := range out.Policies {
		if s.ID == "" || s.ID == policyID {
			t.Errorf("expected the vanished policy skipped rather than listed empty, got %+v", out.Policies)
		}
	}
	if got := orgAttachedPolicies(t, ts, rootID); slices.Contains(got, policyID) {
		t.Errorf("expected the vanished policy skipped in the target listing, got %v", got)
	}

	// The same for a target whose entity has gone.
	var accounts struct {
		Accounts []emulator.OrgAccount `json:"Accounts"`
	}
	orgPolicyOK(t, ts, "ListAccounts", map[string]interface{}{}, &accounts)
	if len(accounts.Accounts) == 0 {
		t.Fatal("setup: expected the management account")
	}
	if err := state.Delete(t.Context(), "organizations", "account:"+accounts.Accounts[0].ID); err != nil {
		t.Fatalf("delete account record: %v", err)
	}
	var targets struct {
		Targets []emulator.OrgPolicyTargetSummary `json:"Targets"`
	}
	orgPolicyOK(t, ts, "ListTargetsForPolicy",
		map[string]interface{}{"PolicyId": emulator.OrgFullAWSAccessIDForTest}, &targets)
	for _, s := range targets.Targets {
		if s.TargetID == "" {
			t.Errorf("expected the vanished target skipped rather than summarized empty, got %+v", targets.Targets)
		}
	}
}

// TestOrganizations_PolicyIDSyntaxUnits exercises the identifier matchers directly.
// They stand in for the model's regexes, and a matcher that is too permissive turns
// a caller's typo into a not-found — a wrong diagnosis rather than a refused
// request — so the boundary lengths are worth pinning without an HTTP round trip.
func TestOrganizations_PolicyIDSyntaxUnits(t *testing.T) {
	t.Run("policy ID", func(t *testing.T) {
		cases := map[string]bool{
			"p-11112222":                       true,
			"p-Full_AWSAccess":                 true,
			emulator.OrgFullAWSAccessIDForTest: true,
			"p-" + strings.Repeat("a", 8):      true,
			"p-" + strings.Repeat("a", 128):    true,
			"p-" + strings.Repeat("a", 7):      false,
			"p-" + strings.Repeat("a", 129):    false,
			"p-with-dash":                      false,
			"p-":                               false,
			"":                                 false,
			"r-abcd":                           false,
			"11112222":                         false,
		}
		for id, want := range cases {
			if got := emulator.IsOrgPolicyIDSyntaxForTest(id); got != want {
				t.Errorf("%q: expected %v, got %v", id, want, got)
			}
		}
	})

	t.Run("target ID", func(t *testing.T) {
		cases := map[string]bool{
			"r-abcd":                             true,
			"r-" + strings.Repeat("a", 32):       true,
			"000000000000":                       true,
			"ou-abcd-11112222":                   true,
			"ou-abcd-" + strings.Repeat("a", 32): true,
			"r-abc":                              false,
			"r-ABCD":                             false,
			"00000000000":                        false,
			"0000000000000":                      false,
			"00000000000a":                       false,
			"ou-abcd":                            false,
			"ou-abc-11112222":                    false,
			"ou-abcd-1111222":                    false,
			"ou-abcd-11112222-extra":             false,
			"p-11112222":                         false,
			"":                                   false,
		}
		for id, want := range cases {
			if got := emulator.IsOrgTargetIDSyntaxForTest(id); got != want {
				t.Errorf("%q: expected %v, got %v", id, want, got)
			}
		}
	})
}

// TestOrganizations_PolicyOperationsAreClaimed pins that every operation in the lane
// is dispatched, and that an operation outside it still falls through to
// InvalidAction. A silently unclaimed operation would answer InvalidAction, which a
// caller reads as "this API does not exist" rather than "substrate has not
// implemented it".
func TestOrganizations_PolicyOperationsAreClaimed(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	for _, op := range []string{
		"CreatePolicy", "UpdatePolicy", "DeletePolicy", "DescribePolicy", "ListPolicies",
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "ListTargetsForPolicy",
		"EnablePolicyType", "DisablePolicyType",
	} {
		resp := orgsRequest(t, ts, op, map[string]interface{}{})
		var out struct {
			Type string `json:"__type"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out) //nolint:errcheck
		resp.Body.Close()                           //nolint:errcheck
		if out.Type == "InvalidAction" {
			t.Errorf("%s: expected the operation to be claimed, got InvalidAction", op)
		}
	}

	resp := orgsRequest(t, ts, "DescribeResourcePolicy", map[string]interface{}{})
	var out struct {
		Type string `json:"__type"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out) //nolint:errcheck
	resp.Body.Close()                           //nolint:errcheck
	if out.Type != "InvalidAction" {
		t.Errorf("expected an unimplemented operation to answer InvalidAction, got %q", out.Type)
	}
}
