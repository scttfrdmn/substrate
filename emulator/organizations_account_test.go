package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// orgCreateStatus is the CreateAccountStatus shape, named exactly as the model
// spells its members so a rename in the plugin fails the decode rather than
// silently reading back a zero value.
// The timestamps decode as numbers, not strings: the JSON 1.1 protocol carries a
// timestamp as epoch seconds, and aws-sdk-go-v2 fails the whole response on an
// RFC3339 string. Decoding them as float64 here is what makes this test able to
// catch that — a string field would accept the wire form the SDK rejects.
type orgCreateStatus struct {
	ID                 string   `json:"Id"`
	AccountName        string   `json:"AccountName"`
	State              string   `json:"State"`
	RequestedTimestamp *float64 `json:"RequestedTimestamp"`
	CompletedTimestamp *float64 `json:"CompletedTimestamp"`
	AccountID          string   `json:"AccountId"`
	FailureReason      string   `json:"FailureReason"`
}

// orgCreateAccount posts CreateAccount and returns the status and the HTTP code.
func orgCreateAccount(t *testing.T, ts *httptest.Server, name, email string) (orgCreateStatus, int) {
	t.Helper()
	resp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": name,
		"Email":       email,
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return orgCreateStatus{}, resp.StatusCode
	}
	var out struct {
		CreateAccountStatus orgCreateStatus `json:"CreateAccountStatus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("CreateAccount decode: %v", err)
	}
	return out.CreateAccountStatus, resp.StatusCode
}

// orgDescribeCreateStatus polls DescribeCreateAccountStatus once.
func orgDescribeCreateStatus(t *testing.T, ts *httptest.Server, requestID string) (orgCreateStatus, int) {
	t.Helper()
	resp := orgsRequest(t, ts, "DescribeCreateAccountStatus", map[string]interface{}{
		"CreateAccountRequestId": requestID,
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return orgCreateStatus{}, resp.StatusCode
	}
	var out struct {
		CreateAccountStatus orgCreateStatus `json:"CreateAccountStatus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("DescribeCreateAccountStatus decode: %v", err)
	}
	return out.CreateAccountStatus, resp.StatusCode
}

// orgVendAccount runs the nominal vend — create, then one poll — and returns the
// resolved account ID.
func orgVendAccount(t *testing.T, ts *httptest.Server, name, email string) string {
	t.Helper()
	created, code := orgCreateAccount(t, ts, name, email)
	if code != http.StatusOK {
		t.Fatalf("CreateAccount %s: expected 200, got %d", name, code)
	}
	resolved, code := orgDescribeCreateStatus(t, ts, created.ID)
	if code != http.StatusOK {
		t.Fatalf("DescribeCreateAccountStatus %s: expected 200, got %d", name, code)
	}
	if resolved.State != emulator.OrgCreateStateSucceededForTest {
		t.Fatalf("expected %s to vend, got State=%q reason=%q",
			name, resolved.State, resolved.FailureReason)
	}
	return resolved.AccountID
}

// orgErrorCode posts an operation expected to be refused and returns the status
// and the JSON-RPC error code.
func orgErrorCode(t *testing.T, ts *httptest.Server, op string, body map[string]interface{}) (int, string) {
	t.Helper()
	resp := orgsRequest(t, ts, op, body)
	defer resp.Body.Close() //nolint:errcheck
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("%s decode error body: %v", op, err)
	}
	code, _ := out["__type"].(string)
	return resp.StatusCode, code
}

// orgSeedCreateFailureOn posts a create-account-failure seed to the same server
// the AWS requests go to, so the seed and the operation share one state store.
func orgSeedCreateFailureOn(t *testing.T, ts *httptest.Server, accountName, reason string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"accountName": accountName, "failureReason": reason})
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/v1/organizations/create-account-failure", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build seed request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed create failure: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed create failure: expected 200, got %d", resp.StatusCode)
	}
}

// orgListAccountIDs returns every account ID ListAccounts reports, paging to the
// end so the assertion is about the organization rather than about a first page.
func orgListAccountIDs(t *testing.T, ts *httptest.Server) []string {
	t.Helper()
	var ids []string
	token := ""
	for {
		body := map[string]interface{}{}
		if token != "" {
			body["NextToken"] = token
		}
		resp := orgsRequest(t, ts, "ListAccounts", body)
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close() //nolint:errcheck
			t.Fatalf("ListAccounts: expected 200, got %d", resp.StatusCode)
		}
		var out struct {
			Accounts []struct {
				ID string `json:"Id"`
			} `json:"Accounts"`
			NextToken string `json:"NextToken"`
		}
		err := json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close() //nolint:errcheck
		if err != nil {
			t.Fatalf("ListAccounts decode: %v", err)
		}
		for _, a := range out.Accounts {
			ids = append(ids, a.ID)
		}
		if out.NextToken == "" {
			return ids
		}
		token = out.NextToken
	}
}

// orgCreateOU places an OU under the root directly through the storage layer.
// The OU operations belong to another lane, so the placement is made here rather
// than through CreateOrganizationalUnit: this file's subject is what MoveAccount
// does with an OU that exists, not how it came to exist.
func orgCreateOU(t *testing.T, p *emulator.OrganizationsPlugin, rootID, suffix string) string {
	t.Helper()
	ouID := "ou-" + rootID[len("r-"):] + "-" + suffix
	ou := emulator.OrgOrganizationalUnit{ID: ouID, Name: suffix}
	if err := p.SaveOUForTest(t.Context(), orgTestAccount, ou); err != nil {
		t.Fatalf("save OU: %v", err)
	}
	if err := p.PlaceChildForTest(t.Context(), rootID, ouID); err != nil {
		t.Fatalf("place OU: %v", err)
	}
	return ouID
}

// orgListCreateStatuses posts ListCreateAccountStatus with an optional States
// filter and returns one page of statuses.
func orgListCreateStatuses(t *testing.T, ts *httptest.Server, states []string) []orgCreateStatus {
	t.Helper()
	body := map[string]interface{}{}
	if states != nil {
		body["States"] = states
	}
	resp := orgsRequest(t, ts, "ListCreateAccountStatus", body)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListCreateAccountStatus: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		CreateAccountStatuses []orgCreateStatus `json:"CreateAccountStatuses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("ListCreateAccountStatus decode: %v", err)
	}
	return out.CreateAccountStatuses
}

// orgMatchesCreateRequestID reports whether id matches the model's
// CreateAccountRequestId pattern, ^car-[a-z0-9]{8,32}$.
func orgMatchesCreateRequestID(id string) bool {
	return regexp.MustCompile(`^car-[a-z0-9]{8,32}$`).MatchString(id)
}

// newOrgServerOverState wires a plugin and server to the supplied state manager,
// which is what lets a fault test arm a store failure mid-journey.
func newOrgServerOverState(t *testing.T, state emulator.StateManager) *httptest.Server {
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
		t.Fatalf("initialize organizations plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// newOrgVendingFixture returns a server and the plugin behind it, sharing one
// state store, so a test can drive the wire surface and then read the hierarchy
// the storage layer recorded.
func newOrgVendingFixture(t *testing.T) (*httptest.Server, *emulator.OrganizationsPlugin) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	ts := newOrgServerOverState(t, state)
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	return ts, emulator.NewOrganizationsPluginForTest(state, tc)
}

// TestOrganizations_FullVend is the journey #578 is really about: vend an
// account, poll it to SUCCEEDED, move it into an OU, and re-run the same move.
// The re-run must fail loudly — a governance script run twice that silently
// succeeds the second time is indistinguishable from one that did nothing, and
// the operator has no way to tell which happened.
func TestOrganizations_FullVend(t *testing.T) {
	ts, p := newOrgVendingFixture(t)
	ctx := t.Context()

	rootID := orgListRootsID(t, ts)
	accountID := orgVendAccount(t, ts, "dev-account", "dev@example.com")

	// A new account lands in the root, not in an OU: CreateAccount takes no
	// parent, so a tool that assumes otherwise leaves the account outside the OU
	// where the policies are attached.
	parent, err := p.LoadParentForTest(ctx, accountID)
	if err != nil {
		t.Fatalf("load parent: %v", err)
	}
	if parent != rootID {
		t.Fatalf("expected the new account in the root %q, got %q", rootID, parent)
	}

	ouID := orgCreateOU(t, p, rootID, "11112222")

	resp := orgsRequest(t, ts, "MoveAccount", map[string]interface{}{
		"AccountId":           accountID,
		"SourceParentId":      rootID,
		"DestinationParentId": ouID,
	})
	gotStatus := resp.StatusCode
	resp.Body.Close() //nolint:errcheck
	if gotStatus != http.StatusOK {
		t.Fatalf("MoveAccount: expected 200, got %d", gotStatus)
	}

	// Both directions of the hierarchy have to agree about where the account is.
	// If they did not, ListAccountsForParent and ListParents would contradict each
	// other, and the account would appear to be in two places at once.
	ouChildren, err := p.LoadChildrenForTest(ctx, ouID)
	if err != nil {
		t.Fatalf("load OU children: %v", err)
	}
	if len(ouChildren) != 1 || ouChildren[0] != accountID {
		t.Errorf("expected the OU to contain only %q, got %v", accountID, ouChildren)
	}
	rootChildren, err := p.LoadChildrenForTest(ctx, rootID)
	if err != nil {
		t.Fatalf("load root children: %v", err)
	}
	for _, id := range rootChildren {
		if id == accountID {
			t.Errorf("expected the account gone from the root, still found in %v", rootChildren)
		}
	}
	moved, err := p.LoadParentForTest(ctx, accountID)
	if err != nil {
		t.Fatalf("load parent after move: %v", err)
	}
	if moved != ouID {
		t.Errorf("expected the account's parent to be %q, got %q", ouID, moved)
	}

	// The re-run. The source is now the OU, but the script replays what it
	// recorded, so it sends the root again; either way the destination is already
	// the parent, and DuplicateAccountException is the answer that tells it so.
	code, errCode := orgErrorCode(t, ts, "MoveAccount", map[string]interface{}{
		"AccountId":           accountID,
		"SourceParentId":      rootID,
		"DestinationParentId": ouID,
	})
	if code != http.StatusBadRequest || errCode != "DuplicateAccountException" {
		t.Errorf("expected 400/DuplicateAccountException on the re-run, got %d/%s", code, errCode)
	}
}

// TestOrganizations_CreateAccountIsAsynchronous pins the response CreateAccount
// itself gives: IN_PROGRESS, a car- request ID, a RequestedTimestamp, and no
// AccountId. A synchronous SUCCEEDED lets a consumer with no poll loop pass, and
// the poll loop is the part that has to survive an interruption.
func TestOrganizations_CreateAccountIsAsynchronous(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	status, code := orgCreateAccount(t, ts, "dev-account", "dev@example.com")
	if code != http.StatusOK {
		t.Fatalf("CreateAccount: expected 200, got %d", code)
	}
	if status.State != emulator.OrgCreateStateInProgressForTest {
		t.Errorf("expected State=%s, got %q", emulator.OrgCreateStateInProgressForTest, status.State)
	}
	if status.AccountID != "" {
		t.Errorf("expected no AccountId while IN_PROGRESS, got %q", status.AccountID)
	}
	if status.CompletedTimestamp != nil {
		t.Errorf("expected no CompletedTimestamp while IN_PROGRESS, got %v", *status.CompletedTimestamp)
	}
	if status.FailureReason != "" {
		t.Errorf("expected no FailureReason while IN_PROGRESS, got %q", status.FailureReason)
	}
	if status.AccountName != "dev-account" {
		t.Errorf("expected AccountName=dev-account, got %q", status.AccountName)
	}
	if status.RequestedTimestamp == nil || *status.RequestedTimestamp <= 0 {
		t.Errorf("expected a RequestedTimestamp in epoch seconds, got %v", status.RequestedTimestamp)
	}
	// The model's CreateAccountRequestId pattern is ^car-[a-z0-9]{8,32}$.
	if !orgMatchesCreateRequestID(status.ID) {
		t.Errorf("expected an ID matching ^car-[a-z0-9]{8,32}$, got %q", status.ID)
	}

	// The account is in ListAccounts immediately, before any poll — AWS reports a
	// new account while its creation request is still in progress.
	if len(orgListAccountIDs(t, ts)) != 2 {
		t.Errorf("expected the management account and the new one, got %v", orgListAccountIDs(t, ts))
	}
}

// TestOrganizations_SeededCreateFailureStillReturns200 is the asymmetry the seed
// exists to expose: the call succeeds, and only the status reports the failure.
// A caller that checks the HTTP status and moves on never learns the account was
// not created, which is the bug this makes reachable in a test.
func TestOrganizations_SeededCreateFailureStillReturns200(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	before := orgListAccountIDs(t, ts)
	orgSeedCreateFailureOn(t, ts, "*", "EMAIL_ALREADY_EXISTS")

	status, code := orgCreateAccount(t, ts, "doomed", "doomed@example.com")
	if code != http.StatusOK {
		t.Fatalf("expected a seeded failure to still answer 200, got %d", code)
	}
	if status.State != emulator.OrgCreateStateInProgressForTest {
		t.Errorf("expected the call itself to report IN_PROGRESS, got %q", status.State)
	}

	resolved, code := orgDescribeCreateStatus(t, ts, status.ID)
	if code != http.StatusOK {
		t.Fatalf("DescribeCreateAccountStatus: expected 200, got %d", code)
	}
	if resolved.State != emulator.OrgCreateStateFailedForTest {
		t.Fatalf("expected State=%s, got %q", emulator.OrgCreateStateFailedForTest, resolved.State)
	}
	if resolved.FailureReason != "EMAIL_ALREADY_EXISTS" {
		t.Errorf("expected the seeded reason, got %q", resolved.FailureReason)
	}
	if resolved.AccountID != "" {
		t.Errorf("expected no AccountId on a FAILED request, got %q", resolved.AccountID)
	}
	if resolved.CompletedTimestamp == nil {
		t.Error("expected a CompletedTimestamp on a terminal request")
	}

	// No account was written. A FAILED request that left an account in
	// ListAccounts would be a state real AWS cannot produce, and it would make a
	// consumer's cleanup path look correct while it deleted nothing.
	after := orgListAccountIDs(t, ts)
	if len(after) != len(before) {
		t.Errorf("expected no new account for a failed request, %v became %v", before, after)
	}
}

// TestOrganizations_OnePollResolvesAndTheStatusIsStable asserts a waiter
// converges in one poll and that the terminal status never moves afterwards. A
// status that re-resolved — a new CompletedTimestamp, or a flip back to
// IN_PROGRESS — would make a waiter comparing successive polls loop forever, and
// a re-minted AccountId would have the caller record an account nothing agrees
// with.
func TestOrganizations_OnePollResolvesAndTheStatusIsStable(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	created, code := orgCreateAccount(t, ts, "dev-account", "dev@example.com")
	if code != http.StatusOK {
		t.Fatalf("CreateAccount: expected 200, got %d", code)
	}

	first, _ := orgDescribeCreateStatus(t, ts, created.ID)
	if first.State != emulator.OrgCreateStateSucceededForTest {
		t.Fatalf("expected the first poll to resolve to SUCCEEDED, got %q", first.State)
	}
	if first.AccountID == "" {
		t.Fatal("expected an AccountId on a SUCCEEDED request")
	}
	if first.CompletedTimestamp == nil {
		t.Fatal("expected a CompletedTimestamp on a SUCCEEDED request")
	}

	for i := range 3 {
		later, _ := orgDescribeCreateStatus(t, ts, created.ID)
		if later.State != first.State {
			t.Errorf("poll %d: state moved from %q to %q", i+2, first.State, later.State)
		}
		if later.AccountID != first.AccountID {
			t.Errorf("poll %d: AccountId moved from %q to %q", i+2, first.AccountID, later.AccountID)
		}
		if later.CompletedTimestamp == nil || *later.CompletedTimestamp != *first.CompletedTimestamp {
			t.Errorf("poll %d: CompletedTimestamp moved from %v to %v",
				i+2, *first.CompletedTimestamp, later.CompletedTimestamp)
		}
	}

	// The account the status names is the one DescribeAccount resolves, and it is
	// marked CREATED rather than INVITED.
	resp := orgsRequest(t, ts, "DescribeAccount", map[string]interface{}{"AccountId": first.AccountID})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeAccount: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Account struct {
			JoinedMethod string `json:"JoinedMethod"`
			Status       string `json:"Status"`
			Email        string `json:"Email"`
		} `json:"Account"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("DescribeAccount decode: %v", err)
	}
	if out.Account.JoinedMethod != "CREATED" {
		t.Errorf("expected JoinedMethod=CREATED for a vended account, got %q", out.Account.JoinedMethod)
	}
	if out.Account.Status != "ACTIVE" {
		t.Errorf("expected Status=ACTIVE, got %q", out.Account.Status)
	}
	if out.Account.Email != "dev@example.com" {
		t.Errorf("expected the requested email, got %q", out.Account.Email)
	}
}

// TestOrganizations_AccountCapFiresOnTheEleventh pins the boundary including the
// management account. An off-by-one here is invisible in any nominal run and only
// shows up as a vending tool that stops one account early or one too late.
func TestOrganizations_AccountCapFiresOnTheEleventh(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	// The management account already occupies one slot, so orgMaxAccounts-1 vends
	// fill the organization.
	for i := range emulator.OrgMaxAccountsForTest - 1 {
		name := "acct-" + string(rune('a'+i))
		orgVendAccount(t, ts, name, name+"@example.com")
	}
	if got := len(orgListAccountIDs(t, ts)); got != emulator.OrgMaxAccountsForTest {
		t.Fatalf("expected the organization full at %d accounts, got %d",
			emulator.OrgMaxAccountsForTest, got)
	}

	code, errCode := orgErrorCode(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "one-too-many",
		"Email":       "one-too-many@example.com",
	})
	if code != http.StatusBadRequest || errCode != "ConstraintViolationException" {
		t.Fatalf("expected 400/ConstraintViolationException on the 11th account, got %d/%s", code, errCode)
	}
	// The reason travels in the message, since the JSON-RPC error document
	// substrate emits has no Reason member for a caller to read.
	resp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "one-too-many", "Email": "one-too-many@example.com",
	})
	defer resp.Body.Close() //nolint:errcheck
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg, _ := body["message"].(string); !strings.Contains(msg, "ACCOUNT_NUMBER_LIMIT_EXCEEDED") {
		t.Errorf("expected ACCOUNT_NUMBER_LIMIT_EXCEEDED in the message, got %q", msg)
	}
}

// TestOrganizations_NameScopedSeedLeavesOtherVendsAlone asserts a name-scoped
// seed fails only the account it names. A seed that leaked onto its neighbors
// would fail an unrelated vend in the same run, and the test asserting on the
// seeded failure would still pass — so the leak would only show up as an
// unexplained failure somewhere else.
func TestOrganizations_NameScopedSeedLeavesOtherVendsAlone(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	orgSeedCreateFailureOn(t, ts, "doomed", "INVALID_EMAIL")

	// The named account fails, and no record of it is left behind.
	doomed, code := orgCreateAccount(t, ts, "doomed", "doomed@example.com")
	if code != http.StatusOK {
		t.Fatalf("expected 200 for a seeded failure, got %d", code)
	}
	resolved, _ := orgDescribeCreateStatus(t, ts, doomed.ID)
	if resolved.State != emulator.OrgCreateStateFailedForTest ||
		resolved.FailureReason != "INVALID_EMAIL" {
		t.Fatalf("expected FAILED/INVALID_EMAIL, got %q/%q", resolved.State, resolved.FailureReason)
	}

	// Its neighbor vends normally.
	otherID := orgVendAccount(t, ts, "healthy", "healthy@example.com")
	ids := orgListAccountIDs(t, ts)
	if len(ids) != 2 {
		t.Fatalf("expected only the management account and %q, got %v", otherID, ids)
	}
}

// TestOrganizations_ListCreateAccountStatus covers the listing and its States
// filter, which is the enum in the model. A caller that watches the listing
// rather than one request has to see the same resolution a Describe gives, or the
// two observations of one request contradict each other.
func TestOrganizations_ListCreateAccountStatus(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	orgSeedCreateFailureOn(t, ts, "doomed", "INTERNAL_FAILURE")
	good, _ := orgCreateAccount(t, ts, "good", "good@example.com")
	bad, _ := orgCreateAccount(t, ts, "doomed", "doomed@example.com")

	// No filter: both requests, and both resolved by the listing itself.
	all := orgListCreateStatuses(t, ts, nil)
	if len(all) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(all))
	}
	for _, st := range all {
		if st.State == emulator.OrgCreateStateInProgressForTest {
			t.Errorf("expected the listing to resolve %s, got IN_PROGRESS", st.ID)
		}
	}

	succeeded := orgListCreateStatuses(t, ts, []string{emulator.OrgCreateStateSucceededForTest})
	if len(succeeded) != 1 || succeeded[0].ID != good.ID {
		t.Errorf("expected only %s to have succeeded, got %+v", good.ID, succeeded)
	}
	failed := orgListCreateStatuses(t, ts, []string{emulator.OrgCreateStateFailedForTest})
	if len(failed) != 1 || failed[0].ID != bad.ID {
		t.Fatalf("expected only %s to have failed, got %+v", bad.ID, failed)
	}
	if failed[0].FailureReason != "INTERNAL_FAILURE" {
		t.Errorf("expected the seeded reason in the listing, got %q", failed[0].FailureReason)
	}
	if none := orgListCreateStatuses(t, ts, []string{emulator.OrgCreateStateInProgressForTest}); len(none) != 0 {
		t.Errorf("expected nothing still in progress after the listing resolved both, got %+v", none)
	}
}

// TestOrganizations_ListCreateAccountStatusPaginates asserts the listing honors
// MaxResults and that a full walk visits every request exactly once. A listing
// that returned everything in one page makes a caller's missing NextToken loop
// invisible.
func TestOrganizations_ListCreateAccountStatusPaginates(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	want := map[string]bool{}
	for i := range 5 {
		name := "acct-" + string(rune('a'+i))
		st, _ := orgCreateAccount(t, ts, name, name+"@example.com")
		want[st.ID] = false
	}

	seen := 0
	token := ""
	pages := 0
	for {
		body := map[string]interface{}{"MaxResults": 2}
		if token != "" {
			body["NextToken"] = token
		}
		resp := orgsRequest(t, ts, "ListCreateAccountStatus", body)
		var out struct {
			CreateAccountStatuses []orgCreateStatus `json:"CreateAccountStatuses"`
			NextToken             string            `json:"NextToken"`
		}
		err := json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close() //nolint:errcheck
		if err != nil {
			t.Fatalf("decode page: %v", err)
		}
		if len(out.CreateAccountStatuses) > 2 {
			t.Fatalf("expected MaxResults=2 honored, got a page of %d", len(out.CreateAccountStatuses))
		}
		for _, st := range out.CreateAccountStatuses {
			already, known := want[st.ID]
			if !known {
				t.Errorf("page returned an unknown request %q", st.ID)
			}
			if already {
				t.Errorf("request %q returned twice across pages", st.ID)
			}
			want[st.ID] = true
			seen++
		}
		pages++
		if out.NextToken == "" {
			break
		}
		token = out.NextToken
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if seen != 5 {
		t.Errorf("expected 5 requests across all pages, saw %d", seen)
	}
	if pages != 3 {
		t.Errorf("expected 5 requests over 3 pages at MaxResults=2, got %d pages", pages)
	}
}

// TestOrganizations_VendingRefusals walks every documented refusal on the lane's
// operations. Each is in the operation's declared errors list in the API model,
// and the code is what a consumer's catch branch is written against — a
// plausible-looking substitute would send it down the wrong path.
func TestOrganizations_VendingRefusals(t *testing.T) {
	ts, p := newOrgVendingFixture(t)
	rootID := orgListRootsID(t, ts)
	accountID := orgVendAccount(t, ts, "dev-account", "dev@example.com")
	ouID := orgCreateOU(t, p, rootID, "11112222")

	cases := []struct {
		name string
		op   string
		body map[string]interface{}
		code string
		why  string
	}{
		{
			name: "unknown create request",
			op:   "DescribeCreateAccountStatus",
			body: map[string]interface{}{"CreateAccountRequestId": "car-99999999"},
			code: "CreateAccountStatusNotFoundException",
			why:  "a resumed run holding a stale ID must learn the request is gone rather than poll forever",
		},
		{
			name: "missing create request id",
			op:   "DescribeCreateAccountStatus",
			body: map[string]interface{}{},
			code: "InvalidInputException",
			why:  "an omitted ID must not be read as a request named the empty string",
		},
		{
			name: "unknown account",
			op:   "MoveAccount",
			body: map[string]interface{}{
				"AccountId": "999999999999", "SourceParentId": rootID, "DestinationParentId": ouID,
			},
			code: "AccountNotFoundException",
			why:  "moving an account that does not exist must not create a placement for it",
		},
		{
			name: "wrong source parent",
			op:   "MoveAccount",
			body: map[string]interface{}{
				"AccountId": accountID, "SourceParentId": "ou-" + rootID[2:] + "-99999999",
				"DestinationParentId": ouID,
			},
			code: "SourceParentNotFoundException",
			why:  "SourceParentId exists so a caller that lost track of the account cannot move it anyway",
		},
		{
			name: "unknown destination",
			op:   "MoveAccount",
			body: map[string]interface{}{
				"AccountId": accountID, "SourceParentId": rootID,
				"DestinationParentId": "ou-" + rootID[2:] + "-99999999",
			},
			code: "DestinationParentNotFoundException",
			why:  "a destination that does not exist must not silently leave the account where it was",
		},
		{
			name: "cross-root move",
			op:   "MoveAccount",
			body: map[string]interface{}{
				"AccountId": accountID, "SourceParentId": rootID,
				"DestinationParentId": "ou-zzzz-11112222",
			},
			code: "InvalidInputException",
			why:  "an OU under another root is not a container this account can reach",
		},
		{
			name: "destination is already the parent",
			op:   "MoveAccount",
			body: map[string]interface{}{
				"AccountId": accountID, "SourceParentId": rootID, "DestinationParentId": rootID,
			},
			code: "DuplicateAccountException",
			why:  "a re-run must fail distinguishably rather than succeed silently",
		},
		{
			name: "malformed parent id",
			op:   "MoveAccount",
			body: map[string]interface{}{
				"AccountId": accountID, "SourceParentId": rootID, "DestinationParentId": "not-a-parent",
			},
			code: "InvalidInputException",
			why:  "a malformed parent is a bad request, not a missing container to go and create",
		},
		{
			name: "missing move arguments",
			op:   "MoveAccount",
			body: map[string]interface{}{"AccountId": accountID},
			code: "InvalidInputException",
			why:  "an omitted parent must not be read as a move to the empty container",
		},
		{
			name: "create with no name",
			op:   "CreateAccount",
			body: map[string]interface{}{"Email": "x@example.com"},
			code: "InvalidInputException",
			why:  "both AccountName and Email are required by the model",
		},
		{
			name: "create with no email",
			op:   "CreateAccount",
			body: map[string]interface{}{"AccountName": "x"},
			code: "InvalidInputException",
			why:  "an account with no email address could never be reported by DescribeAccount",
		},
		{
			name: "state filter outside the enum",
			op:   "ListCreateAccountStatus",
			body: map[string]interface{}{"States": []string{"in_progress"}},
			code: "InvalidInputException",
			why:  "a filter that matched nothing would read as 'the organization vended nothing'",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotStatus, gotCode := orgErrorCode(t, ts, c.op, c.body)
			if gotStatus != http.StatusBadRequest {
				t.Errorf("expected 400 (%s), got %d", c.why, gotStatus)
			}
			if gotCode != c.code {
				t.Errorf("expected %s (%s), got %s", c.code, c.why, gotCode)
			}
		})
	}
}

// TestOrganizations_MoveAccountAcrossOUs asserts a move between two OUs leaves
// the account in exactly one of them. Both directions of the placement index are
// rewritten, and a move that only added to the destination would make the account
// appear in two OUs — a state no sequence of API calls can produce, so nothing
// downstream is prepared for it.
func TestOrganizations_MoveAccountAcrossOUs(t *testing.T) {
	ts, p := newOrgVendingFixture(t)
	ctx := t.Context()

	rootID := orgListRootsID(t, ts)
	accountID := orgVendAccount(t, ts, "dev-account", "dev@example.com")
	first := orgCreateOU(t, p, rootID, "11112222")
	second := orgCreateOU(t, p, rootID, "33334444")

	for _, move := range []struct{ src, dst string }{
		{rootID, first},
		{first, second},
	} {
		resp := orgsRequest(t, ts, "MoveAccount", map[string]interface{}{
			"AccountId": accountID, "SourceParentId": move.src, "DestinationParentId": move.dst,
		})
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusOK {
			t.Fatalf("move %s -> %s: expected 200, got %d", move.src, move.dst, gotStatus)
		}
	}

	if children, err := p.LoadChildrenForTest(ctx, first); err != nil {
		t.Fatalf("load first OU children: %v", err)
	} else if len(children) != 0 {
		t.Errorf("expected the account gone from the first OU, got %v", children)
	}
	if children, err := p.LoadChildrenForTest(ctx, second); err != nil {
		t.Fatalf("load second OU children: %v", err)
	} else if len(children) != 1 || children[0] != accountID {
		t.Errorf("expected the account only in the second OU, got %v", children)
	}
}

// TestOrganizations_MoveAccountEmptyResponse pins the body shape. MoveAccount
// declares no output shape in the model, so it answers an empty JSON object; a
// null body would fail an SDK's response parse.
func TestOrganizations_MoveAccountEmptyResponse(t *testing.T) {
	ts, p := newOrgVendingFixture(t)
	rootID := orgListRootsID(t, ts)
	accountID := orgVendAccount(t, ts, "dev-account", "dev@example.com")
	ouID := orgCreateOU(t, p, rootID, "11112222")

	resp := orgsRequest(t, ts, "MoveAccount", map[string]interface{}{
		"AccountId": accountID, "SourceParentId": rootID, "DestinationParentId": ouID,
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("expected a decodable empty object: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected an empty object, got %v", out)
	}
}

// TestOrganizations_CreateAccountTagsAreReadable asserts tags passed to
// CreateAccount land on the account. Dropping them would answer a tag-gated
// authorization decision with an empty tag set, which fails open.
func TestOrganizations_CreateAccountTagsAreReadable(t *testing.T) {
	ts, p := newOrgVendingFixture(t)

	resp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "tagged",
		"Email":       "tagged@example.com",
		"Tags":        []map[string]string{{"Key": "Owner", "Value": "platform"}},
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateAccount: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		CreateAccountStatus orgCreateStatus `json:"CreateAccountStatus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resolved, _ := orgDescribeCreateStatus(t, ts, out.CreateAccountStatus.ID)
	if resolved.AccountID == "" {
		t.Fatalf("expected the request to vend an account, got %q", resolved.State)
	}

	tags, err := p.LoadTagsForTest(t.Context(), resolved.AccountID)
	if err != nil {
		t.Fatalf("load tags: %v", err)
	}
	if len(tags) != 1 || tags[0].Key != "Owner" || tags[0].Value != "platform" {
		t.Errorf("expected the request's tag on the account, got %+v", tags)
	}
}

// TestOrganizations_VendingMalformedBody covers the decode guard on each
// operation this lane adds. An operation that skipped it would decode into a
// zero-valued input and answer as though the caller had asked about the empty
// request ID, which is a wrong answer rather than an error.
func TestOrganizations_VendingMalformedBody(t *testing.T) {
	ts, _ := newOrgVendingFixture(t)

	for _, op := range []string{"DescribeCreateAccountStatus", "ListCreateAccountStatus", "MoveAccount"} {
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
}

// TestOrganizations_VendingStoreFailures asserts a store failure on any of the
// lane's operations is a 500 the SDK retries rather than a 400 it treats as
// terminal. The vending path is the worst place to collapse the two: a
// CreateAccount that answers 200 while nothing landed leaves the caller holding a
// request ID no poll can resolve.
func TestOrganizations_VendingStoreFailures(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		op     string
		body   map[string]interface{}
		// needsAccount builds the request body from a vended account and an OU,
		// which MoveAccount needs before a store failure is worth arming.
		needsAccount bool
		onGet        bool
		onPut        bool
	}{
		{
			name: "create cannot read the account index", prefix: "account_ids:",
			op: "CreateAccount", body: map[string]interface{}{"AccountName": "dev", "Email": "dev@example.com"},
			onGet: true,
		},
		{
			name: "create cannot write the request record", prefix: "car:",
			op: "CreateAccount", body: map[string]interface{}{"AccountName": "dev", "Email": "dev@example.com"},
			onPut: true,
		},
		{
			name: "create cannot write the pending outcome", prefix: emulator.OrgCreatePendingPrefixForTest,
			op: "CreateAccount", body: map[string]interface{}{"AccountName": "dev", "Email": "dev@example.com"},
			onPut: true,
		},
		{
			name: "create cannot write the account record", prefix: "account:",
			op: "CreateAccount", body: map[string]interface{}{"AccountName": "dev", "Email": "dev@example.com"},
			onPut: true,
		},
		{
			name: "create cannot place the account", prefix: "children:",
			op: "CreateAccount", body: map[string]interface{}{"AccountName": "dev", "Email": "dev@example.com"},
			onPut: true,
		},
		{
			name: "create cannot write the account's tags", prefix: "tags:",
			op: "CreateAccount", body: map[string]interface{}{
				"AccountName": "dev", "Email": "dev@example.com",
				"Tags": []map[string]string{{"Key": "Owner", "Value": "platform"}},
			},
			onPut: true,
		},
		{
			name: "create cannot read the failure seed", prefix: "create-account-failure:",
			op: "CreateAccount", body: map[string]interface{}{"AccountName": "dev", "Email": "dev@example.com"},
			onGet: true,
		},
		{
			name: "describe cannot read the request", prefix: "car:",
			op: "DescribeCreateAccountStatus", body: map[string]interface{}{"CreateAccountRequestId": "car-11112222"},
			onGet: true,
		},
		{
			name: "list cannot read the request index", prefix: "car_ids:",
			op: "ListCreateAccountStatus", body: map[string]interface{}{},
			onGet: true,
		},
		{
			name: "move cannot read the placement", prefix: "parent:",
			op: "MoveAccount", needsAccount: true, onGet: true,
		},
		{
			name: "move cannot write the placement", prefix: "children:",
			op: "MoveAccount", needsAccount: true, onPut: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{
				inner: inner, prefix: c.prefix, err: errors.New("store unavailable"),
				onGet: c.onGet, onPut: c.onPut,
			}
			ts := newOrgServerOverState(t, state)

			// The organization is created before the failure is armed, since
			// arming from the start would leave nothing to operate on.
			body := c.body
			if c.needsAccount {
				rootID := orgListRootsID(t, ts)
				accountID := orgVendAccount(t, ts, "dev-account", "dev@example.com")
				body = map[string]interface{}{
					"AccountId":      accountID,
					"SourceParentId": rootID,
					// Well formed and existent, so the request reaches the store
					// read rather than stopping at a shape or lookup refusal.
					"DestinationParentId": orgCreateOU(t,
						emulator.NewOrganizationsPluginForTest(state, emulator.NewTimeController(time.Now())),
						rootID, "11112222"),
				}
			} else {
				warm := orgsRequest(t, ts, "ListRoots", map[string]interface{}{})
				warm.Body.Close() //nolint:errcheck
			}
			state.armed = true

			resp := orgsRequest(t, ts, c.op, body)
			gotStatus := resp.StatusCode
			resp.Body.Close() //nolint:errcheck
			if gotStatus != http.StatusInternalServerError {
				t.Errorf("expected 500 for a store failure, got %d", gotStatus)
			}
		})
	}
}

// TestOrganizations_MissingPendingOutcomeIsAnError pins what happens when the
// request record exists but the outcome it was committed to does not. That
// pairing is unreachable through the API — createAccount writes the outcome first
// — so it means the store lost a record, and guessing SUCCEEDED would report an
// AccountId nothing else in the organization agrees with.
func TestOrganizations_MissingPendingOutcomeIsAnError(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newOrgServerOverState(t, state)

	created, code := orgCreateAccount(t, ts, "dev-account", "dev@example.com")
	if code != http.StatusOK {
		t.Fatalf("CreateAccount: expected 200, got %d", code)
	}
	if err := state.Delete(t.Context(), "organizations",
		emulator.OrgCreatePendingPrefixForTest+created.ID); err != nil {
		t.Fatalf("delete pending outcome: %v", err)
	}

	resp := orgsRequest(t, ts, "DescribeCreateAccountStatus",
		map[string]interface{}{"CreateAccountRequestId": created.ID})
	gotStatus := resp.StatusCode
	resp.Body.Close() //nolint:errcheck
	if gotStatus != http.StatusInternalServerError {
		t.Errorf("expected 500 for a request with no recorded outcome, got %d", gotStatus)
	}
}

// TestOrganizations_ListCreateStatusSkipsVanishedRecords asserts the listing
// tolerates an index entry whose record is gone rather than reporting a
// zero-valued request. A status with an empty Id and an empty State is worse than
// a short page: a waiter iterating it would never see a terminal state.
func TestOrganizations_ListCreateStatusSkipsVanishedRecords(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newOrgServerOverState(t, state)

	created, _ := orgCreateAccount(t, ts, "dev-account", "dev@example.com")
	if err := state.Delete(t.Context(), "organizations", "car:"+created.ID); err != nil {
		t.Fatalf("delete request record: %v", err)
	}

	if got := orgListCreateStatuses(t, ts, nil); len(got) != 0 {
		t.Errorf("expected the vanished record skipped rather than listed empty, got %+v", got)
	}
}

// TestOrganizations_OUNamesRoot covers the ID-shape reasoning the cross-root
// refusal rests on. An OU ID embeds its root's suffix, so getting this wrong
// would either refuse a legitimate move or let an account cross roots.
func TestOrganizations_OUNamesRoot(t *testing.T) {
	cases := []struct {
		ou, root string
		want     bool
	}{
		{"ou-abcd-11112222", "r-abcd", true},
		{"ou-abcd-11112222", "r-zzzz", false},
		{"ou-abcd-1111-2222", "r-abcd", true},
		{"r-abcd", "r-abcd", false},
		{"ou-abcd", "r-abcd", false},
		{"ou--11112222", "r-abcd", false},
		{"ou-abcd-11112222", "ou-abcd-11112222", false},
	}
	for _, c := range cases {
		if got := emulator.OrgOUNamesRootForTest(c.ou, c.root); got != c.want {
			t.Errorf("OrgOUNamesRoot(%q, %q) = %v, want %v", c.ou, c.root, got, c.want)
		}
	}

	for id, want := range map[string]bool{
		"r-abcd": true, "ou-abcd-11112222": true, "111111111111": false, "": false, "p-11112222": false,
	} {
		if got := emulator.IsOrgParentIDForTest(id); got != want {
			t.Errorf("IsOrgParentID(%q) = %v, want %v", id, got, want)
		}
	}
}
