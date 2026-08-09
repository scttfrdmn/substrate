package emulator_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestOrganizations_SaveRootRoundTrip covers the pair a policy-type change needs:
// saveRoot writes PolicyTypes verbatim and loadStoredRoot reads it back without
// the feature-set masking loadRoot applies. Reading through loadRoot instead would
// make a disable look like it worked under CONSOLIDATED_BILLING, where the mask
// hides the field either way.
func TestOrganizations_SaveRootRoundTrip(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	root, err := p.LoadStoredRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load stored root: %v", err)
	}
	if len(root.PolicyTypes) != 1 || root.PolicyTypes[0].Type != emulator.OrgPolicyTypeSCPForTest {
		t.Fatalf("expected SERVICE_CONTROL_POLICY enabled on a fresh ALL-features root, got %v", root.PolicyTypes)
	}

	// Disabling the type is a write of the stored root, so the round trip is what
	// Lane B's DisablePolicyType stands on.
	root.PolicyTypes = nil
	if err := p.SaveRootForTest(ctx, orgTestAccount, *root); err != nil {
		t.Fatalf("save root: %v", err)
	}
	reloaded, err := p.LoadStoredRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("reload stored root: %v", err)
	}
	if len(reloaded.PolicyTypes) != 0 {
		t.Errorf("expected no policy types after the write, got %v", reloaded.PolicyTypes)
	}
	if reloaded.ID != root.ID {
		t.Errorf("the write changed the root ID: %q became %q", root.ID, reloaded.ID)
	}
	enabled, err := p.SCPEnabledForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("scp enabled: %v", err)
	}
	if enabled {
		t.Error("expected SCPs reported disabled once the root carries no policy type")
	}
}

// TestOrganizations_OUIndex checks the per-organization OU index that
// ListOrganizationalUnitsForParent reads. An OU written but missing from the index
// is invisible to every listing while DescribeOrganizationalUnit still finds it —
// a split that looks like a pagination bug.
func TestOrganizations_OUIndex(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	ids, err := p.LoadOUIDsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load OU IDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no OUs in a fresh organization, got %v", ids)
	}

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	for _, suffix := range []string{"bbbbbbbb", "aaaaaaaa"} {
		ou := emulator.OrgOrganizationalUnit{
			ID:   "ou-" + root.ID[2:] + "-" + suffix,
			Arn:  "arn:aws:organizations::" + orgTestAccount + ":ou/o-test/ou-" + suffix,
			Name: suffix,
		}
		if err := p.SaveOUForTest(ctx, orgTestAccount, ou); err != nil {
			t.Fatalf("save OU %s: %v", suffix, err)
		}
	}

	ids, err = p.LoadOUIDsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("reload OU IDs: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 indexed OUs, got %v", ids)
	}
	// The index is sorted, so a paginated listing is stable across calls.
	if ids[0] >= ids[1] {
		t.Errorf("expected the OU index sorted, got %v", ids)
	}

	loaded, err := p.LoadOUForTest(ctx, ids[0])
	if err != nil {
		t.Fatalf("load OU: %v", err)
	}
	if loaded == nil || loaded.ID != ids[0] {
		t.Fatalf("expected to load %q, got %v", ids[0], loaded)
	}
	missing, err := p.LoadOUForTest(ctx, "ou-zzzz-99999999")
	if err != nil {
		t.Fatalf("load unknown OU: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for an unknown OU, got %v", missing)
	}
}

// TestOrganizations_DetachPolicy checks a detach clears both directions of the
// attachment index. Clearing only one leaves ListPoliciesForTarget and
// ListTargetsForPolicy contradicting each other, which is the shape of bug that
// makes a re-run of a governance script attach a policy it thinks is missing.
func TestOrganizations_DetachPolicy(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}

	attached, err := p.LoadAttachmentsForTest(ctx, root.ID)
	if err != nil {
		t.Fatalf("load attachments: %v", err)
	}
	if len(attached) != 1 || attached[0] != emulator.OrgFullAWSAccessIDForTest {
		t.Fatalf("expected FullAWSAccess on the root, got %v", attached)
	}

	removed, err := p.DetachPolicyFromForTest(ctx, emulator.OrgFullAWSAccessIDForTest, root.ID)
	if err != nil {
		t.Fatalf("detach: %v", err)
	}
	if !removed {
		t.Error("expected the detach to report it removed an attachment")
	}
	attached, err = p.LoadAttachmentsForTest(ctx, root.ID)
	if err != nil {
		t.Fatalf("reload attachments: %v", err)
	}
	if len(attached) != 0 {
		t.Errorf("expected no attachments on the root, got %v", attached)
	}
	// The reverse index loses the root but keeps the management account, which
	// carries its own attachment.
	targets, err := p.LoadPolicyTargetsForTest(ctx, emulator.OrgFullAWSAccessIDForTest)
	if err != nil {
		t.Fatalf("load policy targets: %v", err)
	}
	if len(targets) != 1 || targets[0] != orgTestAccount {
		t.Errorf("expected only the management account left as a target, got %v", targets)
	}

	// Detaching what is not attached reports false rather than erroring, so a
	// handler can turn it into PolicyNotAttachedException itself.
	removed, err = p.DetachPolicyFromForTest(ctx, emulator.OrgFullAWSAccessIDForTest, root.ID)
	if err != nil {
		t.Fatalf("second detach: %v", err)
	}
	if removed {
		t.Error("expected the second detach to report nothing removed")
	}
}

// TestOrganizations_Tags covers the tag store for the four taggable kinds. Tags
// are read back in key order because ListTagsForResource paginates: an unordered
// store would shuffle pages between calls and lose entries.
func TestOrganizations_Tags(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	tags, err := p.LoadTagsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load tags: %v", err)
	}
	if len(tags) != 0 {
		t.Fatalf("expected an untagged account, got %v", tags)
	}

	if err := p.SaveTagsForTest(ctx, orgTestAccount, []emulator.OrgTag{
		{Key: "Owner", Value: "platform"},
		{Key: "CostCenter", Value: "1234"},
		{Key: "Empty", Value: ""},
	}); err != nil {
		t.Fatalf("save tags: %v", err)
	}

	tags, err = p.LoadTagsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("reload tags: %v", err)
	}
	if len(tags) != 3 {
		t.Fatalf("expected 3 tags, got %v", tags)
	}
	want := []emulator.OrgTag{
		{Key: "CostCenter", Value: "1234"},
		{Key: "Empty", Value: ""},
		{Key: "Owner", Value: "platform"},
	}
	for i, w := range want {
		if tags[i] != w {
			t.Errorf("tag %d: expected %v, got %v", i, w, tags[i])
		}
	}

	// saveTags replaces rather than merges, which is what UntagResource needs.
	if err := p.SaveTagsForTest(ctx, orgTestAccount, []emulator.OrgTag{{Key: "Owner", Value: "platform"}}); err != nil {
		t.Fatalf("replace tags: %v", err)
	}
	tags, err = p.LoadTagsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("reload replaced tags: %v", err)
	}
	if len(tags) != 1 || tags[0].Key != "Owner" {
		t.Errorf("expected the tag set replaced, got %v", tags)
	}
}

// TestOrganizations_TagsAreScopedPerResource pins that two resources do not share
// a tag set. A key collision here would make an OU's tags govern an account, and
// once tags reach the authorization decision that is a privilege boundary.
func TestOrganizations_TagsAreScopedPerResource(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	if err := p.SaveTagsForTest(ctx, "ou-abcd-11112222", []emulator.OrgTag{{Key: "Owner", Value: "ou"}}); err != nil {
		t.Fatalf("save OU tags: %v", err)
	}
	if err := p.SaveTagsForTest(ctx, emulator.OrgFullAWSAccessIDForTest, []emulator.OrgTag{{Key: "Owner", Value: "policy"}}); err != nil {
		t.Fatalf("save policy tags: %v", err)
	}

	ouTags, err := p.LoadTagsForTest(ctx, "ou-abcd-11112222")
	if err != nil {
		t.Fatalf("load OU tags: %v", err)
	}
	policyTags, err := p.LoadTagsForTest(ctx, emulator.OrgFullAWSAccessIDForTest)
	if err != nil {
		t.Fatalf("load policy tags: %v", err)
	}
	if len(ouTags) != 1 || ouTags[0].Value != "ou" {
		t.Errorf("expected the OU's own tags, got %v", ouTags)
	}
	if len(policyTags) != 1 || policyTags[0].Value != "policy" {
		t.Errorf("expected the policy's own tags, got %v", policyTags)
	}
	if accountTags, tagErr := p.LoadTagsForTest(ctx, orgTestAccount); tagErr != nil || len(accountTags) != 0 {
		t.Errorf("expected the account untouched, got %v (err %v)", accountTags, tagErr)
	}
}

// TestOrganizations_CreateAccountStatusStore covers the request store an
// asynchronous CreateAccount is polled through, including the unknown-ID answer
// that becomes CreateAccountStatusNotFoundException.
func TestOrganizations_CreateAccountStatusStore(t *testing.T) {
	p := newOrganizationsStateFixture(t)
	ctx := t.Context()

	ids, err := p.LoadCreateAccountStatusIDsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load status IDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no requests in a fresh organization, got %v", ids)
	}

	requested := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	completed := requested.Add(time.Minute)
	for _, st := range []emulator.OrgCreateAccountStatus{
		{ID: "car-bbbbbbbb", AccountName: "prod", State: "IN_PROGRESS", RequestedTimestamp: requested},
		{
			ID: "car-aaaaaaaa", AccountName: "dev", State: "FAILED", RequestedTimestamp: requested,
			CompletedTimestamp: &completed, FailureReason: "EMAIL_ALREADY_EXISTS",
		},
	} {
		if err := p.SaveCreateAccountStatusForTest(ctx, orgTestAccount, st); err != nil {
			t.Fatalf("save status %s: %v", st.ID, err)
		}
	}

	ids, err = p.LoadCreateAccountStatusIDsForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("reload status IDs: %v", err)
	}
	if len(ids) != 2 || ids[0] != "car-aaaaaaaa" {
		t.Fatalf("expected 2 sorted request IDs, got %v", ids)
	}

	failed, err := p.LoadCreateAccountStatusForTest(ctx, "car-aaaaaaaa")
	if err != nil {
		t.Fatalf("load failed status: %v", err)
	}
	if failed == nil {
		t.Fatal("expected to load the failed request")
	}
	if failed.State != "FAILED" || failed.FailureReason != "EMAIL_ALREADY_EXISTS" || failed.AccountID != "" {
		t.Errorf("expected a FAILED status carrying no AccountId, got %+v", failed)
	}
	if failed.CompletedTimestamp == nil || !failed.CompletedTimestamp.Equal(completed) {
		t.Errorf("expected the completion timestamp preserved, got %v", failed.CompletedTimestamp)
	}

	inProgress, err := p.LoadCreateAccountStatusForTest(ctx, "car-bbbbbbbb")
	if err != nil {
		t.Fatalf("load in-progress status: %v", err)
	}
	if inProgress == nil || inProgress.CompletedTimestamp != nil {
		t.Errorf("expected an IN_PROGRESS status with no completion timestamp, got %+v", inProgress)
	}

	missing, err := p.LoadCreateAccountStatusForTest(ctx, "car-99999999")
	if err != nil {
		t.Fatalf("load unknown status: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for an unknown request ID, got %+v", missing)
	}
}

// TestOrganizations_ConstraintViolationShape pins the quota-refusal constructor.
// Every Organizations exception is HTTP 400, and the reason has to reach the
// caller: an SDK catch branch reads ConstraintViolationException's reason to tell
// a quota it can raise from one it cannot.
func TestOrganizations_ConstraintViolationShape(t *testing.T) {
	err := emulator.OrgConstraintViolationForTest("ACCOUNT_NUMBER_LIMIT_EXCEEDED", "You have exceeded the limit.")
	if err.Code != "ConstraintViolationException" {
		t.Errorf("expected ConstraintViolationException, got %q", err.Code)
	}
	if err.HTTPStatus != http.StatusBadRequest {
		t.Errorf("expected HTTP 400, got %d", err.HTTPStatus)
	}
	// The JSON-RPC error shape has no Reason member, so the reason rides in the
	// message; a caller matching on it needs it verbatim and first.
	const want = "ACCOUNT_NUMBER_LIMIT_EXCEEDED: You have exceeded the limit."
	if err.Message != want {
		t.Errorf("expected message %q, got %q", want, err.Message)
	}
}

// TestOrganizations_EmptyResponse pins the body of the operations the API model
// gives no output shape. An empty body rather than "{}" makes an SDK fail to
// unmarshal a call that actually succeeded.
func TestOrganizations_EmptyResponse(t *testing.T) {
	resp := emulator.OrgEmptyResponseForTest()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if string(resp.Body) != "{}" {
		t.Errorf("expected an empty JSON object, got %q", resp.Body)
	}
}
