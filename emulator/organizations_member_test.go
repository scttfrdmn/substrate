package emulator_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file covers #623 and the remainder of #619: an Organizations caller that
// is a *member* of an organization rather than its management account.
//
// It is the only Organizations test file that signs its requests. The rest drive
// a bare plugin through a hand-built X-Amz-Target header and are all account
// 123456789012 by construction, so nothing in them can observe a second caller —
// which is why #623 survived every one of them. Reaching a member identity means
// a credential registry, and a registry is also what switches SigV4 verification
// on, so every request here carries a real signature.

// orgManagementAccount is the account the built-in test credential belongs to,
// and therefore the management account of the organization these tests build.
const orgManagementAccount = "123456789012"

// orgOutsiderAccount is an account that joins no organization. It is used for the
// auto-create regression guard: #623's fix must not turn an unknown caller's
// first request into a refusal, because a fresh emulator with no
// CreateOrganization call is substrate's documented starting point.
const orgOutsiderAccount = "222233334444"

// --- helpers --------------------------------------------------------------

// orgTestNow is a fixed instant for the storage-layer test's clock. A literal
// rather than time.Now, so nothing here can depend on when it runs.
func orgTestNow() time.Time {
	return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
}

// orgsTarget is the Organizations JSON-target endpoint. The signing name and the
// host label are both the plain service name here, unlike Service Quotas.
var orgsTarget = signedRequestTarget{
	host:        "organizations.us-east-1.amazonaws.com",
	target:      "Organizations_20161128",
	signingName: "organizations",
}

// orgSignedRequest posts an Organizations operation signed as the given account.
func orgSignedRequest(t *testing.T, ts *emulator.TestServer, account, op string, body any) *http.Response {
	t.Helper()
	return signedRequest(t, ts, orgsTarget, account, op, body)
}

// orgVendMember creates an account through CreateAccount, polls the request to
// SUCCEEDED, registers a signing credential for the vended account, and returns
// its ID.
//
// The poll is what makes the account ID available at all: CreateAccount answers
// IN_PROGRESS with no AccountId, so a test that skipped the poll would have
// nothing to sign as.
func orgVendMember(t *testing.T, ts *emulator.TestServer, name, email string) string {
	t.Helper()

	var created struct {
		CreateAccountStatus emulator.OrgCreateAccountStatus `json:"CreateAccountStatus"`
	}
	status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount, "CreateAccount",
		map[string]any{"AccountName": name, "Email": email}), &created)
	if status != http.StatusOK {
		t.Fatalf("CreateAccount: %d %s", status, code)
	}
	requestID := created.CreateAccountStatus.ID

	var describe struct {
		CreateAccountStatus emulator.OrgCreateAccountStatus `json:"CreateAccountStatus"`
	}
	status, code = decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount, "DescribeCreateAccountStatus",
		map[string]any{"CreateAccountRequestId": requestID}), &describe)
	if status != http.StatusOK {
		t.Fatalf("DescribeCreateAccountStatus: %d %s", status, code)
	}
	if describe.CreateAccountStatus.State != "SUCCEEDED" {
		t.Fatalf("CreateAccount resolved to %q, want SUCCEEDED", describe.CreateAccountStatus.State)
	}
	member := describe.CreateAccountStatus.AccountID
	if member == "" {
		t.Fatal("a SUCCEEDED CreateAccount reported no AccountId, so nothing can sign as the member")
	}

	ts.RegisterAccount(t, member)
	return member
}

// --- #623: a member sees the organization it belongs to -------------------

// TestOrganizations_MemberSeesManagementsOrganization is #623's repro.
//
// Before the reverse index, every Organizations record was keyed by the account
// that signed the request and ensureOrganization auto-created a whole
// organization — root, management account, FullAWSAccess — for any account it had
// not seen. So a member account calling DescribeOrganization was handed a private
// organization of its own, with a different o- ID and a different root, and a
// consumer walking the hierarchy from a member credential saw an organization of
// exactly one account that management had never created.
func TestOrganizations_MemberSeesManagementsOrganization(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t)

	var mgmt struct {
		Organization emulator.Organization `json:"Organization"`
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"DescribeOrganization", map[string]any{}), &mgmt); status != http.StatusOK {
		t.Fatalf("DescribeOrganization as management: %d %s", status, code)
	}

	member := orgVendMember(t, ts, "member", "member@example.com")

	var got struct {
		Organization emulator.Organization `json:"Organization"`
	}
	status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, member,
		"DescribeOrganization", map[string]any{}), &got)
	if status != http.StatusOK {
		t.Fatalf("DescribeOrganization as the member: %d %s", status, code)
	}
	if got.Organization.ID != mgmt.Organization.ID {
		t.Errorf("the member sees organization %s, management sees %s — the member was given one of its own",
			got.Organization.ID, mgmt.Organization.ID)
	}
	// MasterAccountId is the field a consumer reads to find out who manages it, so
	// a member that saw itself there would conclude it was the management account.
	if got.Organization.MasterAccountID != orgManagementAccount {
		t.Errorf("the member reads MasterAccountId %q, want %s",
			got.Organization.MasterAccountID, orgManagementAccount)
	}

	// The root has to agree too. ListRoots is where a hierarchy walk starts, and
	// the pre-#623 behavior gave the member a second r- ID with nothing under it.
	var mgmtRoots, memberRoots struct {
		Roots []emulator.OrgRoot `json:"Roots"`
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"ListRoots", map[string]any{}), &mgmtRoots); status != http.StatusOK {
		t.Fatalf("ListRoots as management: %d %s", status, code)
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, member,
		"ListRoots", map[string]any{}), &memberRoots); status != http.StatusOK {
		t.Fatalf("ListRoots as the member: %d %s", status, code)
	}
	if len(mgmtRoots.Roots) != 1 || len(memberRoots.Roots) != 1 {
		t.Fatalf("ListRoots returned %d/%d roots, want 1 each",
			len(mgmtRoots.Roots), len(memberRoots.Roots))
	}
	if memberRoots.Roots[0].ID != mgmtRoots.Roots[0].ID {
		t.Errorf("the member's root is %s, management's is %s",
			memberRoots.Roots[0].ID, mgmtRoots.Roots[0].ID)
	}

	// ListAccounts from the member reports the organization's accounts, which is
	// the observation that proves the member is reading management's member list
	// rather than a fresh one holding only itself.
	var listed struct {
		Accounts []emulator.OrgAccount `json:"Accounts"`
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, member,
		"ListAccounts", map[string]any{}), &listed); status != http.StatusOK {
		t.Fatalf("ListAccounts as the member: %d %s", status, code)
	}
	ids := make(map[string]bool, len(listed.Accounts))
	for _, a := range listed.Accounts {
		ids[a.ID] = true
	}
	if !ids[orgManagementAccount] || !ids[member] {
		t.Errorf("ListAccounts as the member returned %v, want both %s and %s",
			ids, orgManagementAccount, member)
	}
}

// TestOrganizations_UnknownAccountStillAutoCreates is the regression guard on the
// no-setup path.
//
// #623's fix consults a reverse index before auto-creating, and an account the
// index does not name must still get an organization rather than a refusal: a
// fresh emulator has no CreateOrganization call in it, and every existing
// Organizations fixture depends on the first observation creating one. The
// distinction the index has to preserve is "not a member of anything" versus "a
// member of that organization", not "known" versus "unknown".
func TestOrganizations_UnknownAccountStillAutoCreates(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t, orgOutsiderAccount)

	var mgmt, outsider struct {
		Organization emulator.Organization `json:"Organization"`
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"DescribeOrganization", map[string]any{}), &mgmt); status != http.StatusOK {
		t.Fatalf("DescribeOrganization as management: %d %s", status, code)
	}
	status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgOutsiderAccount,
		"DescribeOrganization", map[string]any{}), &outsider)
	if status != http.StatusOK {
		t.Fatalf("DescribeOrganization as an account in no organization: %d %s — the auto-create path must still work",
			status, code)
	}
	if outsider.Organization.ID == mgmt.Organization.ID {
		t.Error("an account in no organization was placed in management's; only a recorded member resolves there")
	}
	if outsider.Organization.MasterAccountID != orgOutsiderAccount {
		t.Errorf("the outsider's organization reports MasterAccountId %q, want itself (%s)",
			outsider.Organization.MasterAccountID, orgOutsiderAccount)
	}
}

// TestOrganizations_MemberOwnerIndexIsWrittenBySaveAccount pins the index at the
// storage layer, below the HTTP surface.
//
// saveAccount is the single write point — every path that creates or joins an
// account goes through it — so this asserts the property the handlers rely on
// rather than one handler's answer: the management account indexes to itself, a
// member indexes to management, and an account substrate has never seen indexes
// to "". That third case is not a default; it is what tells the auto-create path
// apart from a resolution, and collapsing it would make a member of a deleted
// organization look like a management account.
func TestOrganizations_MemberOwnerIndexIsWrittenBySaveAccount(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := emulator.NewOrganizationsPluginForTest(state, emulator.NewTimeController(orgTestNow()))
	ctx := t.Context()

	if _, err := p.EnsureOrganizationForTest(ctx, orgManagementAccount); err != nil {
		t.Fatalf("ensure organization: %v", err)
	}
	owner, err := p.OrganizationOwnerForTest(ctx, orgManagementAccount)
	if err != nil {
		t.Fatalf("organizationOwner(management): %v", err)
	}
	if owner != orgManagementAccount {
		t.Errorf("the management account indexes to %q, want itself — one lookup must answer for every account substrate knows", owner)
	}

	const member = "555566667777"
	if err := p.SaveAccountForTest(ctx, orgManagementAccount, emulator.OrgAccount{
		ID: member, Name: "member", Email: "member@example.com", Status: "ACTIVE",
	}); err != nil {
		t.Fatalf("save member account: %v", err)
	}
	owner, err = p.OrganizationOwnerForTest(ctx, member)
	if err != nil {
		t.Fatalf("organizationOwner(member): %v", err)
	}
	if owner != orgManagementAccount {
		t.Errorf("the member indexes to %q, want %s", owner, orgManagementAccount)
	}

	owner, err = p.OrganizationOwnerForTest(ctx, orgOutsiderAccount)
	if err != nil {
		t.Fatalf("organizationOwner(unknown): %v", err)
	}
	if owner != "" {
		t.Errorf("an account substrate has never seen indexes to %q, want \"\"", owner)
	}

	// ensureOrganization resolves through the index, so the member gets
	// management's organization rather than one of its own.
	mgmtOrg, err := p.EnsureOrganizationForTest(ctx, orgManagementAccount)
	if err != nil {
		t.Fatalf("ensure organization (management): %v", err)
	}
	memberOrg, err := p.EnsureOrganizationForTest(ctx, member)
	if err != nil {
		t.Fatalf("ensure organization (member): %v", err)
	}
	if memberOrg.ID != mgmtOrg.ID {
		t.Errorf("ensureOrganization gave the member %s, management has %s", memberOrg.ID, mgmtOrg.ID)
	}
}

// --- #619: the resource-policy asymmetry ---------------------------------

// orgDelegationPolicy is a resource policy that names one account as a delegated
// administrator, in the form every delegation example in the Organizations User
// Guide uses: the member appears as "arn:aws:iam::<account>:root" in Principal.
func orgDelegationPolicy(account string) string {
	return `{"Version":"2012-10-17","Statement":[{"Sid":"Delegate","Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::` + account + `:root"},` +
		`"Action":["organizations:DescribeOrganization","organizations:ListAccounts"],` +
		`"Resource":"*"}]}`
}

// TestOrganizations_ResourcePolicy_MemberAsymmetry is the remainder of #619: the
// three answers DescribeResourcePolicy gives, and the fact that they are
// distinguishable.
//
// A tool checking whether management delegated anything to it must be able to
// tell "nothing is delegated" from "something is, and I cannot read it". Those
// are ResourcePolicyNotFoundException and AccessDeniedException respectively, and
// they differ in HTTP status too — 400 against 403 — because AccessDeniedException
// is a common error rather than one the Organizations model declares.
func TestOrganizations_ResourcePolicy_MemberAsymmetry(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t)

	delegated := orgVendMember(t, ts, "delegated", "delegated@example.com")
	plain := orgVendMember(t, ts, "plain", "plain@example.com")

	// Before any policy exists, management gets the absence — the ordinary answer,
	// since most organizations have no resource policy.
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"DescribeResourcePolicy", map[string]any{}), nil); status != http.StatusBadRequest ||
		code != "ResourcePolicyNotFoundException" {
		t.Fatalf("DescribeResourcePolicy with no policy set: %d %q, want 400/ResourcePolicyNotFoundException", status, code)
	}

	// A member gets denied rather than the absence, so it cannot use this
	// operation to learn whether the organization has a policy at all.
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, plain,
		"DescribeResourcePolicy", map[string]any{}), nil); status != http.StatusForbidden ||
		code != "AccessDeniedException" {
		t.Fatalf("DescribeResourcePolicy as a member with no policy set: %d %q, want 403/AccessDeniedException", status, code)
	}

	var put struct {
		ResourcePolicy emulator.OrgResourcePolicy `json:"ResourcePolicy"`
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"PutResourcePolicy", map[string]any{"Content": orgDelegationPolicy(delegated)}),
		&put); status != http.StatusOK {
		t.Fatalf("PutResourcePolicy as management: %d %s", status, code)
	}

	// The delegated member reads exactly what management wrote. Identical Content,
	// Id and Arn is the point: a delegated administrator that saw a different ARN
	// would conclude it was looking at a different policy.
	var got struct {
		ResourcePolicy emulator.OrgResourcePolicy `json:"ResourcePolicy"`
	}
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, delegated,
		"DescribeResourcePolicy", map[string]any{}), &got); status != http.StatusOK {
		t.Fatalf("DescribeResourcePolicy as the delegated member: %d %s", status, code)
	}
	if got.ResourcePolicy != put.ResourcePolicy {
		t.Errorf("the delegated member reads %+v, management wrote %+v",
			got.ResourcePolicy, put.ResourcePolicy)
	}

	// A member the policy does not name is still denied, with the policy now in
	// place. This is the case the API can actually reach: none of the three
	// operations takes an input naming an organization, so a caller can only ever
	// ask about its own and "an account in no relationship to this organization"
	// has no way to pose the question.
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, plain,
		"DescribeResourcePolicy", map[string]any{}), nil); status != http.StatusForbidden ||
		code != "AccessDeniedException" {
		t.Fatalf("DescribeResourcePolicy as an undelegated member: %d %q, want 403/AccessDeniedException", status, code)
	}

	// Management still reads it, which is what makes the member's denial an
	// asymmetry rather than the policy having gone missing.
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"DescribeResourcePolicy", map[string]any{}), &got); status != http.StatusOK {
		t.Fatalf("DescribeResourcePolicy as management after the put: %d %s", status, code)
	}
	if got.ResourcePolicy != put.ResourcePolicy {
		t.Errorf("management reads back %+v, want %+v", got.ResourcePolicy, put.ResourcePolicy)
	}
}

// TestOrganizations_ResourcePolicy_WritesAreManagementOnly covers the other half
// of the asymmetry: the writes admit no member, delegated or not.
//
// The AWS reference is explicit that PutResourcePolicy and DeleteResourcePolicy
// "can be called only from the organization's management account", while
// DescribeResourcePolicy also admits a delegated administrator. Letting a
// delegated member write would let it widen its own delegation, which is why the
// guard here does not consult the policy document at all.
func TestOrganizations_ResourcePolicy_WritesAreManagementOnly(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t)

	delegated := orgVendMember(t, ts, "delegated", "delegated@example.com")
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"PutResourcePolicy", map[string]any{"Content": orgDelegationPolicy(delegated)}),
		nil); status != http.StatusOK {
		t.Fatalf("PutResourcePolicy as management: %d %s", status, code)
	}

	for _, op := range []string{"PutResourcePolicy", "DeleteResourcePolicy"} {
		body := map[string]any{}
		if op == "PutResourcePolicy" {
			body["Content"] = orgDelegationPolicy(delegated)
		}
		status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, delegated, op, body), nil)
		if status != http.StatusForbidden || code != "AccessDeniedException" {
			t.Errorf("%s as a delegated member: %d %q, want 403/AccessDeniedException", op, status, code)
		}
	}

	// And management's own delete still works, so the refusals above are about the
	// caller rather than the operation being broken.
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount,
		"DeleteResourcePolicy", map[string]any{}), nil); status != http.StatusOK {
		t.Fatalf("DeleteResourcePolicy as management: %d %s", status, code)
	}
}
