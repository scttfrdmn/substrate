package emulator

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// CreateAccountState values, the model's CreateAccountState enum in full.
const (
	orgCreateStateInProgress = "IN_PROGRESS"
	orgCreateStateSucceeded  = "SUCCEEDED"
	orgCreateStateFailed     = "FAILED"
)

// orgCreateAccountStates is the CreateAccountState enum, used to validate the
// States filter ListCreateAccountStatus takes. A value outside the enum is
// refused rather than matching nothing: a filter that silently matches nothing
// reads to the caller as "there are no requests", which is the same answer a
// correctly spelled filter gives for an organization that has vended nothing.
var orgCreateAccountStates = []string{
	orgCreateStateInProgress,
	orgCreateStateSucceeded,
	orgCreateStateFailed,
}

// orgCreatePendingKey holds the outcome an IN_PROGRESS CreateAccount request will
// resolve to. It is kept apart from the request record so the record itself is
// always a shape AWS could return — an IN_PROGRESS status carrying the AccountId
// it is going to get is not, and substrate's state is meant to be inspectable at
// any point in history.
func orgCreatePendingKey(id string) string { return "car_pending:" + id }

// orgPendingAccountOutcome is the terminal outcome an asynchronous CreateAccount
// request is already committed to. Exactly one field is set: an account ID for a
// request that will succeed, a CreateAccountFailureReason for one that will fail.
//
// The outcome is decided when CreateAccount is called, not when the status is
// first read, so that the account record and the status can never disagree.
// Deciding it at read time would let a seed cleared between the two produce a
// SUCCEEDED status naming an account that was never written, or a FAILED status
// for an account that is in ListAccounts — states real AWS cannot reach.
type orgPendingAccountOutcome struct {
	// AccountID is the account the request will report on success.
	AccountID string `json:"accountId,omitempty"`

	// FailureReason is the CreateAccountFailureReason the request will report.
	FailureReason string `json:"failureReason,omitempty"`
}

// accountOperation claims the account vending and placement operations.
func (p *OrganizationsPlugin) accountOperation(op string) (orgHandler, bool) {
	switch op {
	case "CreateAccount":
		return p.createAccount, true
	case "DescribeCreateAccountStatus":
		return p.describeCreateAccountStatus, true
	case "ListCreateAccountStatus":
		return p.listCreateAccountStatus, true
	case "MoveAccount":
		return p.moveAccount, true
	default:
		return nil, false
	}
}

// createAccount starts an asynchronous account creation. AWS documents
// CreateAccount as "an asynchronous request that Amazon Web Services performs in
// the background", so the call answers IN_PROGRESS with a car- request ID and no
// AccountId, and the caller polls DescribeCreateAccountStatus with that ID. A
// synchronous SUCCEEDED — what substrate returned before v0.97.0 — lets a
// consumer with no poll loop pass its tests, and the poll loop is the part that
// has to survive being interrupted: the request ID is the only handle a resumed
// run has on an account that may or may not exist yet.
func (p *OrganizationsPlugin) createAccount(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		AccountName string        `json:"AccountName"`
		Email       string        `json:"Email"`
		Tags        []orgTagInput `json:"Tags"`

		// RoleName and IamUserAccessToBilling are accepted and recorded nowhere:
		// no Organizations API observation exposes either, so modeling them would
		// be modeling the inside of the new account rather than the API surface.
		RoleName               string `json:"RoleName"`
		IamUserAccessToBilling string `json:"IamUserAccessToBilling"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.AccountName == "" || input.Email == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED",
			"you must specify both AccountName and Email to create an account")
	}
	// Tags go through the same validation TagResource applies, so a key that
	// operation refuses cannot be planted through a create instead. The refusal is
	// synchronous — it is the request that is malformed, not the vend that failed,
	// so it must not arrive later as a FAILED status the caller has to poll for.
	tags, err := validateOrgCreateTags(input.Tags)
	if err != nil {
		return nil, err
	}

	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createAccount ensure org: %w", err)
	}
	root, err := p.loadRoot(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createAccount load root: %w", err)
	}

	// The account quota is a synchronous refusal, which is what CreateAccount's
	// declared ConstraintViolationException is for. The management account counts
	// against it, so an organization at the default quota has room for nine
	// vended accounts and the tenth call is refused.
	existing, err := p.loadAccountIDs(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createAccount load account ids: %w", err)
	}
	if len(existing) >= orgMaxAccounts {
		return nil, orgConstraintViolation("ACCOUNT_NUMBER_LIMIT_EXCEEDED",
			fmt.Sprintf("you have exceeded the number of accounts allowed in the organization (%d)", orgMaxAccounts))
	}

	outcome, err := p.pendingCreateOutcome(goCtx, input.AccountName)
	if err != nil {
		return nil, fmt.Errorf("createAccount resolve outcome: %w", err)
	}

	// A request headed for FAILED writes no account. A FAILED request that left
	// an account in ListAccounts would be a state real AWS cannot produce, and it
	// is the state most likely to make a consumer's cleanup path look correct
	// while it deletes nothing.
	if outcome.AccountID != "" {
		if err := p.vendAccount(goCtx, reqCtx.AccountID, org.ID, root.ID, outcome.AccountID, input.AccountName, input.Email, tags); err != nil {
			return nil, err
		}
	}

	status := OrgCreateAccountStatus{
		ID:                 "car-" + randomLowerAlphanum(8),
		AccountName:        input.AccountName,
		State:              orgCreateStateInProgress,
		RequestedTimestamp: EpochSeconds(p.tc.Now()),
	}
	if err := p.orgPutJSON(goCtx, orgCreatePendingKey(status.ID), outcome); err != nil {
		return nil, fmt.Errorf("createAccount save outcome: %w", err)
	}
	if err := p.saveCreateAccountStatus(goCtx, reqCtx.AccountID, status); err != nil {
		return nil, fmt.Errorf("createAccount save status: %w", err)
	}

	// 200 with IN_PROGRESS even for a request that is already committed to
	// failing. That asymmetry is the whole point of the seeded failure: a caller
	// that checks the HTTP status and moves on never learns the account was not
	// created, which is exactly the bug the seed exists to catch.
	return orgJSONResponse(map[string]interface{}{"CreateAccountStatus": status}, "createAccount")
}

// vendAccount writes the account record for a request that will succeed. The
// record is written now rather than at resolution, because AWS reports a new
// account from ListAccounts while its creation request is still IN_PROGRESS.
//
// The account lands in the root: CreateAccount takes no parent, so MoveAccount is
// the only way into an OU. A tool that assumes otherwise leaves accounts
// unprotected, since the OU is where policies are attached.
func (p *OrganizationsPlugin) vendAccount(ctx context.Context, masterAcct, orgID, rootID, newAcctID, name, email string, tags []OrgTag) error {
	a := OrgAccount{
		ID:           newAcctID,
		Arn:          fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", masterAcct, orgID, newAcctID),
		Name:         name,
		Email:        email,
		Status:       "ACTIVE",
		JoinedMethod: "CREATED",
		JoinedAt:     EpochSeconds(p.tc.Now()),
	}
	if err := p.saveAccount(ctx, masterAcct, a); err != nil {
		return fmt.Errorf("createAccount save account: %w", err)
	}
	if err := p.placeChild(ctx, rootID, a.ID); err != nil {
		return fmt.Errorf("createAccount place: %w", err)
	}
	if err := p.attachFullAWSAccess(ctx, masterAcct, a.ID); err != nil {
		return fmt.Errorf("createAccount attach FullAWSAccess: %w", err)
	}
	// Tags given to CreateAccount are tags on the account, so they have to be
	// readable through ListTagsForResource; dropping them would answer a tag-gated
	// authorization decision with an empty tag set.
	if len(tags) > 0 {
		if err := p.saveTags(ctx, a.ID, tags); err != nil {
			return fmt.Errorf("createAccount save tags: %w", err)
		}
	}
	return nil
}

// pendingCreateOutcome decides what an asynchronous CreateAccount will resolve
// to, at request time. A control-plane seed decides it; with no seed the request
// succeeds, which is the nominal path a new operation defaults to.
//
// The organization-wide email uniqueness AWS enforces — surfaced asynchronously
// as EMAIL_ALREADY_EXISTS, not as an error from CreateAccount — is deliberately
// not inferred from the stored accounts here. It stays reachable only through the
// seed (TODO(#578)): inferring it would make a duplicate email fail whether the
// test asked for it or not, which changes the outcome of an existing fixture
// rather than adding a path a test can opt into.
func (p *OrganizationsPlugin) pendingCreateOutcome(ctx context.Context, accountName string) (orgPendingAccountOutcome, error) {
	seed, err := p.resolveSeededCreateFailure(ctx, accountName)
	if err != nil {
		return orgPendingAccountOutcome{}, err
	}
	if seed != nil {
		return orgPendingAccountOutcome{FailureReason: seed.FailureReason}, nil
	}
	return orgPendingAccountOutcome{AccountID: generateOrganizationAccountID()}, nil
}

// describeCreateAccountStatus reports the current state of a vending request,
// resolving it on first observation.
func (p *OrganizationsPlugin) describeCreateAccountStatus(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("describeCreateAccountStatus ensure org: %w", err)
	}

	var input struct {
		CreateAccountRequestID string `json:"CreateAccountRequestId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.CreateAccountRequestID == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify a CreateAccountRequestId")
	}

	st, err := p.loadCreateAccountStatus(goCtx, input.CreateAccountRequestID)
	if err != nil {
		return nil, fmt.Errorf("describeCreateAccountStatus load: %w", err)
	}
	if st == nil {
		// An unknown request ID is a refusal rather than an empty answer: a
		// resumed run holding an ID from a discarded state store must learn the
		// request is gone, not conclude it is still in progress and poll forever.
		return nil, orgErr("CreateAccountStatusNotFoundException",
			"We can't find a create account request with the CreateAccountRequestId "+input.CreateAccountRequestID)
	}
	if err := p.resolveCreateAccountStatus(goCtx, reqCtx.AccountID, st); err != nil {
		return nil, fmt.Errorf("describeCreateAccountStatus resolve: %w", err)
	}
	return orgJSONResponse(map[string]interface{}{"CreateAccountStatus": st}, "describeCreateAccountStatus")
}

// listCreateAccountStatus lists vending requests, optionally filtered by state.
func (p *OrganizationsPlugin) listCreateAccountStatus(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listCreateAccountStatus ensure org: %w", err)
	}

	var input struct {
		States     []string `json:"States"`
		NextToken  string   `json:"NextToken"`
		MaxResults int      `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	for _, state := range input.States {
		if !slices.Contains(orgCreateAccountStates, state) {
			return nil, orgInvalidInput("INVALID_LIST_MEMBER",
				fmt.Sprintf("%q is not a CreateAccountState", state))
		}
	}

	ids, err := p.loadCreateAccountStatusIDs(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("listCreateAccountStatus load ids: %w", err)
	}

	// The listing resolves each request the same way a Describe does. If it did
	// not, a caller polling through the listing would see IN_PROGRESS forever
	// while a Describe of the same request reported SUCCEEDED — two observations
	// of one request contradicting each other, and a waiter built on the listing
	// that never terminates.
	matched := make([]string, 0, len(ids))
	byID := make(map[string]OrgCreateAccountStatus, len(ids))
	for _, id := range ids {
		st, loadErr := p.loadCreateAccountStatus(goCtx, id)
		if loadErr != nil {
			return nil, fmt.Errorf("listCreateAccountStatus load status: %w", loadErr)
		}
		if st == nil {
			continue
		}
		if err := p.resolveCreateAccountStatus(goCtx, reqCtx.AccountID, st); err != nil {
			return nil, fmt.Errorf("listCreateAccountStatus resolve: %w", err)
		}
		if len(input.States) > 0 && !slices.Contains(input.States, st.State) {
			continue
		}
		matched = append(matched, id)
		byID[id] = *st
	}

	page, next, err := orgPaginate(matched, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}
	statuses := make([]OrgCreateAccountStatus, 0, len(page))
	for _, id := range page {
		statuses = append(statuses, byID[id])
	}

	out := map[string]interface{}{"CreateAccountStatuses": statuses}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listCreateAccountStatus")
}

// resolveCreateAccountStatus turns an IN_PROGRESS request into its terminal state
// on first observation and persists the result, so every later observation
// reports the same state, the same AccountId, and the same CompletedTimestamp.
//
// Resolving on observation rather than after an interval of the simulated clock
// is deliberate: a waiter converges in one poll with no dependence on wall-clock
// or simulated time, and a status that re-resolved — moving its
// CompletedTimestamp, or flipping back to IN_PROGRESS — would make a waiter that
// compares successive polls loop forever. Clock-driven transitions are the
// subject of #514, which is still open; choosing a duration here would front-run
// that design.
func (p *OrganizationsPlugin) resolveCreateAccountStatus(ctx context.Context, acct string, st *OrgCreateAccountStatus) error {
	if st.State != orgCreateStateInProgress {
		return nil
	}
	var pending orgPendingAccountOutcome
	found, err := p.orgGetJSON(ctx, orgCreatePendingKey(st.ID), &pending)
	if err != nil {
		return err
	}
	if !found {
		// Unreachable through the API — createAccount writes the outcome before
		// the request record — so it means the store lost a record. Guessing
		// SUCCEEDED here would report an AccountId nothing else agrees with.
		return fmt.Errorf("organizations: create-account request %s has no recorded outcome", st.ID)
	}

	completed := EpochSeconds(p.tc.Now())
	st.CompletedTimestamp = &completed
	if pending.FailureReason != "" {
		st.State = orgCreateStateFailed
		st.FailureReason = pending.FailureReason
		st.AccountID = ""
	} else {
		st.State = orgCreateStateSucceeded
		st.AccountID = pending.AccountID
	}
	if err := p.saveCreateAccountStatus(ctx, acct, *st); err != nil {
		return err
	}
	return nil
}

// moveAccount moves an account between the root and OUs. Every refusal below is
// in MoveAccount's declared errors list, and each one distinguishes a case a
// re-run of a governance script hits: nothing here is idempotent, which is the
// property such a script has to be built on.
func (p *OrganizationsPlugin) moveAccount(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		AccountID           string `json:"AccountId"`
		SourceParentID      string `json:"SourceParentId"`
		DestinationParentID string `json:"DestinationParentId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.AccountID == "" || input.SourceParentID == "" || input.DestinationParentID == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED",
			"you must specify AccountId, SourceParentId and DestinationParentId")
	}
	// Shape before existence, the order AWS validates in. A parent ID that is
	// neither a root nor an OU is a malformed request, and answering
	// DestinationParentNotFoundException for one would send a caller looking for a
	// container it never named.
	for _, id := range []string{input.SourceParentID, input.DestinationParentID} {
		if !isOrgParentID(id) {
			return nil, orgInvalidInput("INVALID_PATTERN",
				"a parent must be a root or an organizational unit ID, got "+id)
		}
	}

	root, err := p.loadRoot(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("moveAccount load root: %w", err)
	}

	a, err := p.loadAccount(goCtx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("moveAccount load account: %w", err)
	}
	if a == nil {
		return nil, orgErr("AccountNotFoundException",
			"We can't find an Amazon Web Services account with the AccountId "+input.AccountID)
	}
	currentParent, err := p.loadParent(goCtx, a.ID)
	if err != nil {
		return nil, fmt.Errorf("moveAccount load parent: %w", err)
	}

	// An OU ID embeds the ID of the root that contains it — "ou-{root
	// suffix}-{suffix}" — so an OU naming another root is a move across roots on
	// its face. That is refused as a cross-root move rather than as a missing
	// container, because the two send a caller to different places: one says
	// "this move is not possible", the other says "create the OU first".
	for _, id := range []string{input.SourceParentID, input.DestinationParentID} {
		if isOrgOUID(id) && !orgOUNamesRoot(id, root.ID) {
			return nil, orgInvalidInput("MOVING_ACCOUNT_BETWEEN_DIFFERENT_ROOTS",
				"you can move an account only between entities in the same root")
		}
	}

	destKind, err := p.resolveOrgTarget(goCtx, reqCtx.AccountID, input.DestinationParentID)
	if err != nil {
		return nil, fmt.Errorf("moveAccount resolve destination: %w", err)
	}
	if destKind != orgKindRoot && destKind != orgKindOU {
		return nil, orgErr("DestinationParentNotFoundException",
			"We can't find the destination container (a root or OU) with the ParentId "+input.DestinationParentID)
	}

	// The destination is checked before the source on purpose. A governance
	// script re-run replays the move it already made — source root, destination
	// OU — and the useful answer is "that account is already present in the
	// specified destination", not "your source OU does not exist", which is what
	// validating the source first would say. This is the reading #578's point 5
	// left open: the second pass fails loudly and distinguishably rather than
	// silently succeeding.
	if currentParent == input.DestinationParentID {
		return nil, orgErr("DuplicateAccountException",
			"That account is already present in the specified destination")
	}

	// SourceParentId has to be the account's actual parent. Accepting a stale one
	// would let a caller that has lost track of where the account is move it
	// anyway, and the source parameter exists precisely so that cannot happen.
	if input.SourceParentID != currentParent {
		return nil, orgErr("SourceParentNotFoundException",
			"We can't find a source root or OU with the ParentId "+input.SourceParentID)
	}

	if err := p.placeChild(goCtx, input.DestinationParentID, a.ID); err != nil {
		return nil, fmt.Errorf("moveAccount place: %w", err)
	}
	return orgEmptyResponse(), nil
}

// isOrgParentID reports whether id has the shape of a ParentId — a root or an OU
// — which is the pattern the model puts on both of MoveAccount's parent members.
func isOrgParentID(id string) bool { return isOrgRootID(id) || isOrgOUID(id) }

// orgOUNamesRoot reports whether an OU ID's embedded root segment is rootID. An
// OU ID is "ou-" plus the containing root's suffix, a dash, and the OU's own
// suffix, so the root an OU belongs to is readable from its ID alone.
func orgOUNamesRoot(ouID, rootID string) bool {
	if !isOrgOUID(ouID) || !isOrgRootID(rootID) {
		return false
	}
	rest := ouID[len("ou-"):]
	dash := strings.Index(rest, "-")
	if dash <= 0 {
		return false
	}
	return rest[:dash] == rootID[len("r-"):]
}
