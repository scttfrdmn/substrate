package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// orgMaxConcurrentClosures is the number of member-account closures that may be
// in progress at once: "Number of member accounts you can close concurrently: 3
// — Only three account closures can be in progress at the same time. As soon as
// one finishes, you can close another account", from "Quotas and service limits
// for AWS Organizations" in the Organizations User Guide:
// https://docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html
//
// This is the only closure quota substrate models, because it is the only one
// countable from state. See the closeAccount doc comment for the two that are
// deliberately left out.
const orgMaxConcurrentClosures = 3

// closeAccount closes a member account.
//
// The model gives CloseAccount no output shape — a success is an empty 200 — and
// AWS documents it as "an asynchronous request that Amazon Web Services performs
// in the background" whose progress is read through DescribeAccount: "While the
// close account request is in progress, Account status will indicate
// PENDING_CLOSURE. When the close account request completes, the status will
// change to SUSPENDED."
//
// The account is *not* removed from the organization. It keeps its place in the
// hierarchy, stays in ListAccounts, and keeps counting against the account quota,
// which is what the User Guide means by "When an account is closed it does not
// stop counting against this quota until it is permanently closed" — the
// interaction with L-E619E033 that makes this worth modeling at all. A consumer
// whose cleanup path closes accounts to make room does not get room, and that is
// the observation substrate has to reproduce.
//
// Two of the three published closure quotas are deliberately not modeled: the
// rolling-30-day allowance (the higher of 250 or 20% of member accounts, capped
// at 1,000) and the four-day minimum age for removing a created account. Both are
// bounded by a wall-clock window, and substrate's clock is simulated and freely
// advanced — a refusal tied to such a window would fire or not depending on
// unrelated AdvanceTime calls elsewhere in a test, which is the opposite of a
// reproducible outcome. The concurrent-closure limit has no window: it is a count
// of accounts currently in PENDING_CLOSURE, so it is exact.
func (p *OrganizationsPlugin) closeAccount(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		AccountID string `json:"AccountId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.AccountID == "" {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify an AccountId")
	}
	// Shape before existence, the order AWS validates in: the member's pattern is
	// ^\d{12}$, so anything else is a malformed request rather than a missing
	// account. Answering AccountNotFoundException for "not-an-id" would send a
	// caller looking for an account it never named.
	if !isOrgAccountID(input.AccountID) {
		return nil, orgInvalidInput("INVALID_PATTERN",
			"an account ID must be 12 digits, got "+input.AccountID)
	}

	// Management-only, per the User Guide: "When you sign in to the organization's
	// management account, you can close member accounts that are part of your
	// organization." The guard is before any state read so a member cannot use the
	// refusals below to probe the organization it belongs to.
	if reqCtx.isMember() {
		return nil, orgCloseAccountAccessDenied()
	}

	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("closeAccount ensure org: %w", err)
	}
	// "You can close an account when all features are enabled." Under
	// CONSOLIDATED_BILLING the console hides the button entirely and directs the
	// caller to close the account as its own root user, which is not this API.
	featureSet, err := p.effectiveFeatureSet(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("closeAccount feature set: %w", err)
	}
	if featureSet != orgFeatureSetAll {
		return nil, orgConstraintViolation("ORGANIZATION_NOT_IN_ALL_FEATURES_MODE",
			"you can close an account only in an organization with all features enabled")
	}

	// The management account is refused on its identity rather than its state, so
	// the check does not depend on a lookup. AWS is explicit that the API cannot do
	// it at all: "You can't close the management account with this API."
	if input.AccountID == reqCtx.AccountID {
		return nil, orgConstraintViolation("CANNOT_CLOSE_MANAGEMENT_ACCOUNT",
			"you attempted to close the management account; to close it you must first remove or close all member accounts in the organization")
	}

	// An account of another organization is reported as not found rather than
	// denied, which is what the model's own message describes: "We can't find an
	// Amazon Web Services account with the AccountId that you specified, or the
	// account whose credentials you used to make this request isn't a member of an
	// organization." Distinguishing the two would tell the caller that an account
	// it cannot manage exists.
	owner, err := p.organizationOwner(goCtx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("closeAccount resolve owner: %w", err)
	}
	a, err := p.loadAccount(goCtx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("closeAccount load account: %w", err)
	}
	if a == nil || owner != reqCtx.AccountID {
		return nil, orgErr("AccountNotFoundException",
			"We can't find an Amazon Web Services account with the AccountId "+input.AccountID)
	}

	// A closure already in flight and one already finished are different refusals.
	// AWS declares both AccountAlreadyClosedException ("You attempted to close an
	// account that is already closed") and ConflictException ("The request failed
	// because it conflicts with the current state of the specified resource") for
	// this operation but does not say which applies to a PENDING_CLOSURE target;
	// substrate reads the "already closed" wording as the terminal state and
	// answers the conflict for the in-flight one. A re-run of a teardown script can
	// then tell "this is finishing" from "this was done", which one code for both
	// cases would collapse.
	switch a.Status {
	case orgAccountStatusSuspended:
		return nil, orgErr("AccountAlreadyClosedException",
			"You attempted to close an account that is already closed")
	case orgAccountStatusPendingClosure:
		return nil, orgErr("ConflictException",
			"The request failed because it conflicts with the current state of account "+a.ID)
	}

	inFlight, err := p.pendingClosureCount(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("closeAccount count closures: %w", err)
	}
	if inFlight >= orgMaxConcurrentClosures {
		return nil, orgConstraintViolation("CLOSE_ACCOUNT_REQUESTS_LIMIT_EXCEEDED",
			fmt.Sprintf("you attempted to exceed the number of accounts that you can close at a time (%d)",
				orgMaxConcurrentClosures))
	}

	a.Status = orgAccountStatusPendingClosure
	if err := p.saveAccount(goCtx, reqCtx.AccountID, *a); err != nil {
		return nil, fmt.Errorf("closeAccount save account: %w", err)
	}
	return orgEmptyResponse(), nil
}

// pendingClosureCount counts the accounts of an organization whose closure is in
// flight, which is what the concurrent-closure quota is measured against.
//
// It reads through loadAccount rather than observeAccount on purpose: counting is
// not an observation of any one account's status, and letting it advance would
// mean that closing a fourth account converged the first three — so the quota
// could never be reached, and a seeded or unobserved closure would silently
// resolve behind a caller that never polled it.
func (p *OrganizationsPlugin) pendingClosureCount(ctx context.Context, masterAcct string) (int, error) {
	ids, err := p.loadAccountIDs(ctx, masterAcct)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		a, err := p.loadAccount(ctx, id)
		if err != nil {
			return 0, err
		}
		if a != nil && a.Status == orgAccountStatusPendingClosure {
			count++
		}
	}
	return count, nil
}

// observeAccount loads an account for an operation that reports its status,
// advancing an in-flight closure as a side effect of the observation.
//
// Only the three operations that put an account's Status on the wire —
// DescribeAccount, ListAccounts, ListAccountsForParent — read through this. Every
// other loadAccount caller resolves an account as a policy target or a hierarchy
// child and reports no status, so advancing there would move a closure a caller
// never looked at.
func (p *OrganizationsPlugin) observeAccount(ctx context.Context, id string) (*OrgAccount, error) {
	a, err := p.loadAccount(ctx, id)
	if err != nil || a == nil {
		return a, err
	}
	if err := p.resolveAccountClosure(ctx, *a); err != nil {
		return nil, err
	}
	return a, nil
}

// resolveAccountClosure persists the terminal status of an in-flight closure,
// leaving the status the caller is handed at PENDING_CLOSURE.
//
// So the first observation reports PENDING_CLOSURE and every later one reports
// SUSPENDED. That is the same departure from resolveCreateAccountStatus the
// Account Management Region opts make, and for the same reason: CloseAccount has
// no output shape, so a poll is the only place PENDING_CLOSURE is ever
// observable. Resolving on the first read instead would make a consumer's
// in-flight branch unexecutable — it would see nothing but SUSPENDED, and the
// wait loop AWS's documented status sequence exists to justify would never be
// exercised.
//
// Resolving on observation rather than after an interval of the simulated clock
// is the same choice resolveCreateAccountStatus makes: a waiter converges in two
// polls with no dependence on wall-clock or simulated time, and clock-driven
// transitions are the subject of #514.
//
// The record is written through orgPutJSON rather than saveAccount because a
// status transition changes no membership: the account keeps its place in the
// management account's member list, which is exactly what makes a closed account
// still count against the account quota.
func (p *OrganizationsPlugin) resolveAccountClosure(ctx context.Context, a OrgAccount) error {
	if a.Status != orgAccountStatusPendingClosure {
		return nil
	}
	a.Status = orgAccountStatusSuspended
	if err := p.orgPutJSON(ctx, orgAccountKey(a.ID), a); err != nil {
		return fmt.Errorf("resolve closure of %s: %w", a.ID, err)
	}
	return nil
}

// orgCloseAccountAccessDenied returns AccessDeniedException for a member account
// calling CloseAccount, which the User Guide reserves to the management account.
//
// HTTP 403, matching orgResourcePolicyAccessDenied: the Organizations API
// reference gives this code 400 on each operation page, while the service's
// Common Errors page — where AccessDeniedException actually belongs, since it is
// a common error rather than one the model declares — gives it 403. Whichever
// reading is right, one service must answer one status for one code, or a
// consumer's classifier would branch differently depending on which operation
// denied it.
func orgCloseAccountAccessDenied() *AWSError {
	return &AWSError{
		Code: "AccessDeniedException",
		Message: "You don't have permissions to perform the requested operation. " +
			"CloseAccount can be called only from the management account of the organization",
		HTTPStatus: http.StatusForbidden,
	}
}
