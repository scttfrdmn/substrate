package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file covers CloseAccount (#625). The interesting properties are all about
// what a closure does *not* do: the account does not leave ListAccounts, does not
// stop counting against the account quota, and does not reach SUSPENDED before a
// caller has had one chance to observe PENDING_CLOSURE.

// orgCloseAccount posts CloseAccount and returns the HTTP status and the error
// code a refusal carries. A success has no output shape, so there is nothing else
// to read.
func orgCloseAccount(t *testing.T, ts *httptest.Server, accountID string) (int, string) {
	t.Helper()
	return orgErrorCodeOrEmpty(t, ts, "CloseAccount", map[string]interface{}{"AccountId": accountID})
}

// orgErrorCodeOrEmpty posts an operation whose success has no output shape,
// returning the status and, for a refusal, the JSON-RPC error code.
func orgErrorCodeOrEmpty(t *testing.T, ts *httptest.Server, op string, body map[string]interface{}) (int, string) {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("%s decode body: %v", op, err)
	}
	if resp.StatusCode == http.StatusOK {
		// The model gives CloseAccount no output shape, so an empty object is the
		// whole success. A body carrying members would be a shape no SDK can decode
		// into the operation's output struct.
		if len(out) != 0 {
			t.Errorf("%s answered 200 with %v, want an empty object — the model gives it no output shape", op, out)
		}
		return resp.StatusCode, ""
	}
	code, _ := out["__type"].(string)
	return resp.StatusCode, code
}

// orgAccountStatus reads one account's Status through DescribeAccount, which is
// the operation AWS documents as the way to watch a closure.
func orgAccountStatus(t *testing.T, ts *httptest.Server, accountID string) string {
	t.Helper()
	resp := orgsRequest(t, ts, "DescribeAccount", map[string]interface{}{"AccountId": accountID})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeAccount(%s): expected 200, got %d", accountID, resp.StatusCode)
	}
	var out struct {
		Account struct {
			Status string `json:"Status"`
		} `json:"Account"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("DescribeAccount decode: %v", err)
	}
	return out.Account.Status
}

// orgListAccountStatuses reads every account's status out of ListAccounts, keyed
// by ID.
func orgListAccountStatuses(t *testing.T, ts *httptest.Server) map[string]string {
	t.Helper()
	resp := orgsRequest(t, ts, "ListAccounts", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListAccounts: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Accounts []struct {
			ID     string `json:"Id"`
			Status string `json:"Status"`
		} `json:"Accounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("ListAccounts decode: %v", err)
	}
	byID := make(map[string]string, len(out.Accounts))
	for _, a := range out.Accounts {
		byID[a.ID] = a.Status
	}
	return byID
}

// TestOrganizations_CloseAccountResolvesOnObservation is the nominal journey: an
// empty 200, then PENDING_CLOSURE, then SUSPENDED, and stable after that.
//
// The two-step sequence is the point. CloseAccount has no output shape, so a poll
// is the only place PENDING_CLOSURE is observable at all — resolving on the first
// read would leave a consumer's in-flight branch unexecutable, and a status that
// moved back would make a waiter comparing successive polls loop forever.
func TestOrganizations_CloseAccountResolvesOnObservation(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	member := orgVendAccount(t, ts, "doomed", "doomed@example.com")

	if status, code := orgCloseAccount(t, ts, member); status != http.StatusOK {
		t.Fatalf("CloseAccount: %d %s", status, code)
	}

	if got := orgAccountStatus(t, ts, member); got != "PENDING_CLOSURE" {
		t.Errorf("the first observation reads %q, want PENDING_CLOSURE — CloseAccount has no output, so this is the only place the in-flight status appears", got)
	}
	for i := range 3 {
		if got := orgAccountStatus(t, ts, member); got != "SUSPENDED" {
			t.Fatalf("observation %d reads %q, want SUSPENDED", i+2, got)
		}
	}
}

// TestOrganizations_CloseAccountKeepsTheAccountInTheOrganization is the
// interaction with L-E619E033 that makes this worth modeling.
//
// The User Guide is explicit: "When an account is closed it does not stop counting
// against this quota until it is permanently closed." So a consumer whose cleanup
// path closes accounts to make room does not get room, and the account stays
// visible in every listing. Removing it would make a broken cleanup script look
// correct.
func TestOrganizations_CloseAccountKeepsTheAccountInTheOrganization(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	// Nine vended accounts plus the management account fills the default quota.
	var members []string
	for i := range 9 {
		members = append(members, orgVendAccount(t, ts,
			"member-"+string(rune('a'+i)), "member-"+string(rune('a'+i))+"@example.com"))
	}
	if status, code := orgErrorCode(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "one-too-many", "Email": "one-too-many@example.com",
	}); status != http.StatusBadRequest || code != "ConstraintViolationException" {
		t.Fatalf("the tenth vend answered %d %q, want 400/ConstraintViolationException", status, code)
	}

	if status, code := orgCloseAccount(t, ts, members[0]); status != http.StatusOK {
		t.Fatalf("CloseAccount: %d %s", status, code)
	}

	// Still listed, and listed with its in-flight status: ListAccounts reports the
	// same status DescribeAccount would, or a caller polling through the listing
	// would disagree with one polling the Describe.
	listed := orgListAccountStatuses(t, ts)
	if got, ok := listed[members[0]]; !ok {
		t.Errorf("ListAccounts dropped the closed account %s; a closed account stays in the organization until it is permanently closed", members[0])
	} else if got != "PENDING_CLOSURE" {
		t.Errorf("ListAccounts reports the closing account as %q, want PENDING_CLOSURE", got)
	}
	if len(listed) != 10 {
		t.Errorf("ListAccounts returned %d accounts after a closure, want 10", len(listed))
	}

	// And the quota still refuses, which is the observation a cleanup script's
	// author would otherwise get wrong.
	if status, code := orgErrorCode(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "after-close", "Email": "after-close@example.com",
	}); status != http.StatusBadRequest || code != "ConstraintViolationException" {
		t.Errorf("vending after a closure answered %d %q, want 400/ConstraintViolationException — a closed account keeps counting against the quota",
			status, code)
	}
}

// TestOrganizations_CloseAccountRefusals covers every refusal that is about which
// account was named, each one distinguishable from the others.
func TestOrganizations_CloseAccountRefusals(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	member := orgVendAccount(t, ts, "doomed", "doomed@example.com")

	tests := []struct {
		name      string
		accountID string
		status    int
		code      string
		// wants is a fragment the message must carry. Two of these share a code, so
		// the code alone does not identify which limit or reason was violated — the
		// JSON-RPC error document has no reason member to read.
		wants string
	}{
		{
			name: "the management account cannot be closed through this API",
			// orgTestAccount, because an unsigned request is account 000000000000 and
			// therefore the management account of the organization this fixture
			// auto-creates.
			accountID: orgTestAccount,
			status:    http.StatusBadRequest,
			code:      "ConstraintViolationException",
			wants:     "CANNOT_CLOSE_MANAGEMENT_ACCOUNT",
		},
		{
			name:      "an account of another organization is not found",
			accountID: "999988887777",
			status:    http.StatusBadRequest,
			code:      "AccountNotFoundException",
		},
		{
			name:      "a malformed ID is a pattern violation, not a missing account",
			accountID: "not-an-account",
			status:    http.StatusBadRequest,
			code:      "InvalidInputException",
			wants:     "INVALID_PATTERN",
		},
		{
			name:      "an absent AccountId is a missing required member",
			accountID: "",
			status:    http.StatusBadRequest,
			code:      "InvalidInputException",
			wants:     "INPUT_REQUIRED",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := orgsRequest(t, ts, "CloseAccount", map[string]interface{}{"AccountId": tc.accountID})
			defer resp.Body.Close() //nolint:errcheck
			var out struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.StatusCode != tc.status || out.Type != tc.code {
				t.Fatalf("CloseAccount(%q) answered %d %q, want %d %q",
					tc.accountID, resp.StatusCode, out.Type, tc.status, tc.code)
			}
			if tc.wants != "" && !strings.Contains(out.Message, tc.wants) {
				t.Errorf("the refusal message is %q; it must name %q, since the error document has no reason member",
					out.Message, tc.wants)
			}
		})
	}

	// The member itself is closable, which is what makes the refusals above about
	// the target rather than the operation being broken.
	if status, code := orgCloseAccount(t, ts, member); status != http.StatusOK {
		t.Fatalf("CloseAccount on a member: %d %s", status, code)
	}
}

// TestOrganizations_CloseAccountTwiceIsDistinguishable separates the two states a
// second close can find: a closure still in flight, and one already finished.
//
// The model declares both ConflictException and AccountAlreadyClosedException for
// this operation and does not say which applies to a PENDING_CLOSURE target.
// Substrate reads "already closed" as the terminal state, so a teardown script
// re-run can tell "this is finishing" from "this was done" — one code for both
// would collapse that.
func TestOrganizations_CloseAccountTwiceIsDistinguishable(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	member := orgVendAccount(t, ts, "doomed", "doomed@example.com")

	if status, code := orgCloseAccount(t, ts, member); status != http.StatusOK {
		t.Fatalf("CloseAccount: %d %s", status, code)
	}
	// No observation between the two closes, so the account is still
	// PENDING_CLOSURE.
	if status, code := orgCloseAccount(t, ts, member); status != http.StatusBadRequest ||
		code != "ConflictException" {
		t.Errorf("closing an in-flight closure answered %d %q, want 400/ConflictException", status, code)
	}

	// Two observations take it to SUSPENDED, and then the refusal changes.
	orgAccountStatus(t, ts, member)
	if got := orgAccountStatus(t, ts, member); got != "SUSPENDED" {
		t.Fatalf("the account is %q after two observations, want SUSPENDED", got)
	}
	if status, code := orgCloseAccount(t, ts, member); status != http.StatusBadRequest ||
		code != "AccountAlreadyClosedException" {
		t.Errorf("closing a SUSPENDED account answered %d %q, want 400/AccountAlreadyClosedException", status, code)
	}
}

// TestOrganizations_CloseAccountConcurrencyLimit pins the one closure quota
// substrate models: three closures in flight at once.
//
// It is countable from state — the number of accounts currently in
// PENDING_CLOSURE — with no wall-clock window, which is exactly why it is modeled
// and the rolling-30-day allowance is not. Counting must also not advance the
// closures it counts: if it did, closing a fourth account would converge the first
// three and the limit could never be reached.
func TestOrganizations_CloseAccountConcurrencyLimit(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	var members []string
	for i := range 4 {
		name := "member-" + string(rune('a'+i))
		members = append(members, orgVendAccount(t, ts, name, name+"@example.com"))
	}

	for i, member := range members[:3] {
		if status, code := orgCloseAccount(t, ts, member); status != http.StatusOK {
			t.Fatalf("CloseAccount %d: %d %s", i+1, status, code)
		}
	}
	status, code := orgCloseAccount(t, ts, members[3])
	if status != http.StatusBadRequest || code != "ConstraintViolationException" {
		t.Fatalf("the fourth concurrent closure answered %d %q, want 400/ConstraintViolationException", status, code)
	}

	// Observing all three to their terminal status frees the slots, so the fourth
	// then succeeds. That is the "as soon as one finishes, you can close another"
	// half of the quota, and it is what proves the count is of in-flight closures
	// rather than of closures ever made.
	for range 2 {
		for _, member := range members[:3] {
			orgAccountStatus(t, ts, member)
		}
	}
	if status, code := orgCloseAccount(t, ts, members[3]); status != http.StatusOK {
		t.Errorf("CloseAccount after the three in-flight closures finished: %d %s", status, code)
	}
}

// TestOrganizations_CloseAccountRequiresAllFeatures gates the operation on the
// feature set, per "You can close an account when all features are enabled".
//
// Under CONSOLIDATED_BILLING the console hides the button entirely and directs the
// caller to close the account as its own root user, which is not this API.
func TestOrganizations_CloseAccountRequiresAllFeatures(t *testing.T) {
	ts := newOrganizationsTestServer(t)
	member := orgVendAccount(t, ts, "doomed", "doomed@example.com")

	orgSeedFeatureSet(t, ts, "CONSOLIDATED_BILLING")
	resp := orgsRequest(t, ts, "CloseAccount", map[string]interface{}{"AccountId": member})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest || out.Type != "ConstraintViolationException" {
		t.Fatalf("CloseAccount under CONSOLIDATED_BILLING answered %d %q, want 400/ConstraintViolationException",
			resp.StatusCode, out.Type)
	}
	if !strings.Contains(out.Message, "ORGANIZATION_NOT_IN_ALL_FEATURES_MODE") {
		t.Errorf("the refusal message is %q; it must name the reason", out.Message)
	}

	// The account is untouched, so a caller that retries after enabling all
	// features is not starting from a half-applied state.
	if got := orgAccountStatus(t, ts, member); got != "ACTIVE" {
		t.Errorf("the account is %q after a refused close, want ACTIVE", got)
	}
}

// TestOrganizations_CloseAccountIsManagementOnly is the member half of the
// asymmetry, through a signed request.
//
// The User Guide reserves closing a member account to the management account:
// "When you sign in to the organization's management account, you can close member
// accounts that are part of your organization." A member that could close its
// siblings would let a compromised member account dismantle the organization.
func TestOrganizations_CloseAccountIsManagementOnly(t *testing.T) {
	ts := emulator.StartTestServerWithAccounts(t)

	victim := orgVendMember(t, ts, "victim", "victim@example.com")
	attacker := orgVendMember(t, ts, "attacker", "attacker@example.com")

	status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, attacker, "CloseAccount",
		map[string]any{"AccountId": victim}), nil)
	if status != http.StatusForbidden || code != "AccessDeniedException" {
		t.Fatalf("CloseAccount as a member answered %d %q, want 403/AccessDeniedException", status, code)
	}

	// A member cannot close itself either: the guard is on the caller, not on the
	// relationship between caller and target.
	status, code = decodeAWSResponse(t, orgSignedRequest(t, ts, attacker, "CloseAccount",
		map[string]any{"AccountId": attacker}), nil)
	if status != http.StatusForbidden || code != "AccessDeniedException" {
		t.Errorf("a member closing itself answered %d %q, want 403/AccessDeniedException", status, code)
	}

	// Management closes the same account the member could not, so the refusals are
	// about the caller.
	if status, code := decodeAWSResponse(t, orgSignedRequest(t, ts, orgManagementAccount, "CloseAccount",
		map[string]any{"AccountId": victim}), nil); status != http.StatusOK {
		t.Errorf("CloseAccount as management: %d %s", status, code)
	}
}
