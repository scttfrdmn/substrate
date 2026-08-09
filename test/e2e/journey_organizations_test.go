package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_OrganizationsAccountVending is #577 and #578 at the SDK level: the
// account-vending run a landing-zone tool actually performs, through the real
// Organizations client rather than a hand-built request.
//
// The unit tests cover each operation. What only this level can catch is the join
// between them — #610 was exactly that: OrganizationsPlugin was registered and
// fully unit-tested while every SDK call fell through to "service not emulated",
// because the target prefix never routed. A journey through the SDK is the cheapest
// thing that fails when the plugin is unreachable.
//
// The journey is also the property #578 is really about: a vending run must be
// *re-runnable*. The second pass has to hit refusals rather than silently
// duplicating, and that is asserted here through the SDK's typed errors, because
// errors.As on the typed exception is the branch a consumer's error handling
// actually takes.
func TestJourney_OrganizationsAccountVending(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion below is about the first response rather than
	// whatever the SDK's retry loop eventually settled on.
	orgs := organizations.NewFromConfig(cfg, func(o *organizations.Options) { o.RetryMaxAttempts = 1 })

	// --- #577: the root has one identity ---
	//
	// Two ListRoots calls, because that is the issue's repro verbatim. Everything
	// after this references the root, so an ID that moved between calls would make
	// the rest of the journey meaningless rather than failing loudly.
	first, err := orgs.ListRoots(ctx, &organizations.ListRootsInput{})
	if err != nil {
		t.Fatalf("ListRoots: %v", err)
	}
	if len(first.Roots) != 1 {
		t.Fatalf("ListRoots returned %d roots, want exactly 1", len(first.Roots))
	}
	rootID := aws.ToString(first.Roots[0].Id)
	rootARN := aws.ToString(first.Roots[0].Arn)

	second, err := orgs.ListRoots(ctx, &organizations.ListRootsInput{})
	if err != nil {
		t.Fatalf("ListRoots again: %v", err)
	}
	if got := aws.ToString(second.Roots[0].Id); got != rootID {
		t.Fatalf("the root ID moved between calls: %q then %q", rootID, got)
	}
	if got := aws.ToString(second.Roots[0].Arn); got != rootARN {
		t.Fatalf("the root ARN moved between calls: %q then %q", rootARN, got)
	}

	// --- the governed OU the account will land in ---
	ou, err := orgs.CreateOrganizationalUnit(ctx, &organizations.CreateOrganizationalUnitInput{
		ParentId: aws.String(rootID),
		Name:     aws.String("prod"),
		Tags:     []orgtypes.Tag{{Key: aws.String("Owner"), Value: aws.String("platform")}},
	})
	if err != nil {
		t.Fatalf("CreateOrganizationalUnit: %v", err)
	}
	ouID := aws.ToString(ou.OrganizationalUnit.Id)

	// The tag has to be readable back, or nothing downstream can gate on it.
	tags, err := orgs.ListTagsForResource(ctx, &organizations.ListTagsForResourceInput{
		ResourceId: aws.String(ouID),
	})
	if err != nil {
		t.Fatalf("ListTagsForResource on the OU: %v", err)
	}
	if len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "Owner" {
		t.Fatalf("expected the OU's create-time tag readable, got %+v", tags.Tags)
	}

	// A second OU of the same name under the same parent is refused. A landing-zone
	// tool's re-run does exactly this.
	var dupOU *orgtypes.DuplicateOrganizationalUnitException
	if _, err := orgs.CreateOrganizationalUnit(ctx, &organizations.CreateOrganizationalUnitInput{
		ParentId: aws.String(rootID),
		Name:     aws.String("prod"),
	}); err == nil {
		t.Fatal("a duplicate OU name: expected DuplicateOrganizationalUnitException")
	} else if !errors.As(err, &dupOU) {
		t.Fatalf("expected *DuplicateOrganizationalUnitException, got %T: %v", err, err)
	}

	// --- #578: vending is asynchronous ---
	//
	// The call returns IN_PROGRESS with a request ID, as AWS does. A caller that
	// read State without polling would see a state that is not terminal.
	created, err := orgs.CreateAccount(ctx, &organizations.CreateAccountInput{
		AccountName: aws.String("dev"),
		Email:       aws.String("dev@example.com"),
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if created.CreateAccountStatus.State != orgtypes.CreateAccountStateInProgress {
		t.Fatalf("CreateAccount State = %q, want IN_PROGRESS", created.CreateAccountStatus.State)
	}
	if aws.ToString(created.CreateAccountStatus.AccountId) != "" {
		t.Fatal("CreateAccount reported an AccountId before the request resolved")
	}
	requestID := aws.ToString(created.CreateAccountStatus.Id)

	// The poll a waiter performs. It converges in one observation, with no
	// wall-clock dependence — the property that makes this test not flake.
	status, err := orgs.DescribeCreateAccountStatus(ctx, &organizations.DescribeCreateAccountStatusInput{
		CreateAccountRequestId: aws.String(requestID),
	})
	if err != nil {
		t.Fatalf("DescribeCreateAccountStatus: %v", err)
	}
	if status.CreateAccountStatus.State != orgtypes.CreateAccountStateSucceeded {
		t.Fatalf("status State = %q, want SUCCEEDED", status.CreateAccountStatus.State)
	}
	accountID := aws.ToString(status.CreateAccountStatus.AccountId)
	if accountID == "" {
		t.Fatal("a SUCCEEDED status carried no AccountId")
	}

	// --- placement: the account starts in the root and MoveAccount is the only
	// way out of it ---
	if parents := journeyOrgParents(t, ctx, orgs, accountID); len(parents) != 1 || parents[0] != rootID {
		t.Fatalf("a new account's parents = %v, want just the root %s", parents, rootID)
	}
	if _, err := orgs.MoveAccount(ctx, &organizations.MoveAccountInput{
		AccountId:           aws.String(accountID),
		SourceParentId:      aws.String(rootID),
		DestinationParentId: aws.String(ouID),
	}); err != nil {
		t.Fatalf("MoveAccount: %v", err)
	}
	if parents := journeyOrgParents(t, ctx, orgs, accountID); len(parents) != 1 || parents[0] != ouID {
		t.Fatalf("after the move, parents = %v, want just the OU %s", parents, ouID)
	}

	// Both listings have to agree about where the account is, since a tool reads
	// placement from one and asserts on the other.
	if got := journeyOrgAccountsForParent(t, ctx, orgs, ouID); len(got) != 1 || got[0] != accountID {
		t.Fatalf("ListAccountsForParent(%s) = %v, want just %s", ouID, got, accountID)
	}
	for _, id := range journeyOrgAccountsForParent(t, ctx, orgs, rootID) {
		if id == accountID {
			t.Fatalf("the moved account is still reported under the root %s", rootID)
		}
	}

	// --- the re-run: the same move is a refusal, not a no-op ---
	//
	// This is the re-runnability property. A no-op here would let a tool's second
	// pass appear to succeed while doing nothing, and a duplicate would corrupt the
	// hierarchy; the API's answer is DuplicateAccountException, which the source
	// parent no longer matching is *not* — so both refusals are asserted.
	var dupAccount *orgtypes.DuplicateAccountException
	if _, err := orgs.MoveAccount(ctx, &organizations.MoveAccountInput{
		AccountId:           aws.String(accountID),
		SourceParentId:      aws.String(ouID),
		DestinationParentId: aws.String(ouID),
	}); err == nil {
		t.Fatal("moving to the current parent: expected DuplicateAccountException")
	} else if !errors.As(err, &dupAccount) {
		t.Fatalf("expected *DuplicateAccountException, got %T: %v", err, err)
	}

	// A source that is not the account's actual parent is its own refusal. It needs a
	// destination that is not the current parent, because the destination is checked
	// first: on a governance re-run the useful answer is "already in the destination",
	// not "your source does not exist", so the order matters and is asserted by these
	// two cases together.
	var srcNotFound *orgtypes.SourceParentNotFoundException
	if _, err := orgs.MoveAccount(ctx, &organizations.MoveAccountInput{
		AccountId:           aws.String(accountID),
		SourceParentId:      aws.String(rootID), // no longer the parent
		DestinationParentId: aws.String(rootID),
	}); err == nil {
		t.Fatal("a stale source parent: expected SourceParentNotFoundException")
	} else if !errors.As(err, &srcNotFound) {
		t.Fatalf("expected *SourceParentNotFoundException, got %T: %v", err, err)
	}

	// An unknown account is AccountNotFoundException at HTTP 400 — the model
	// declares no 404 for it, so a consumer branching on the status must not see one.
	var acctNotFound *orgtypes.AccountNotFoundException
	if _, err := orgs.DescribeAccount(ctx, &organizations.DescribeAccountInput{
		AccountId: aws.String("999999999999"),
	}); err == nil {
		t.Fatal("an unknown account: expected AccountNotFoundException")
	} else if !errors.As(err, &acctNotFound) {
		t.Fatalf("expected *AccountNotFoundException, got %T: %v", err, err)
	}
}

// TestJourney_OrganizationsSeededVendingFailure covers the case a vending tool's
// error path exists for and that no nominal run reaches: the account never gets
// created.
//
// The shape is what makes it worth a journey. CreateAccount still answers HTTP
// 200 with IN_PROGRESS — the SDK raises nothing at all — and the failure is
// observable only through DescribeCreateAccountStatus. A consumer that treats a
// successful CreateAccount as a vended account never notices, which is precisely
// the bug this seed lets a test catch.
func TestJourney_OrganizationsSeededVendingFailure(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	orgs := organizations.NewFromConfig(cfg, func(o *organizations.Options) { o.RetryMaxAttempts = 1 })

	journeySeedOrgCreateFailure(t, ts, "dup", "EMAIL_ALREADY_EXISTS")

	created, err := orgs.CreateAccount(ctx, &organizations.CreateAccountInput{
		AccountName: aws.String("dup"),
		Email:       aws.String("taken@example.com"),
	})
	if err != nil {
		t.Fatalf("CreateAccount under a seeded failure must still succeed: %v", err)
	}
	if created.CreateAccountStatus.State != orgtypes.CreateAccountStateInProgress {
		t.Fatalf("CreateAccount State = %q, want IN_PROGRESS", created.CreateAccountStatus.State)
	}

	status, err := orgs.DescribeCreateAccountStatus(ctx, &organizations.DescribeCreateAccountStatusInput{
		CreateAccountRequestId: created.CreateAccountStatus.Id,
	})
	if err != nil {
		t.Fatalf("DescribeCreateAccountStatus: %v", err)
	}
	if status.CreateAccountStatus.State != orgtypes.CreateAccountStateFailed {
		t.Fatalf("status State = %q, want FAILED", status.CreateAccountStatus.State)
	}
	if status.CreateAccountStatus.FailureReason != orgtypes.CreateAccountFailureReasonEmailAlreadyExists {
		t.Fatalf("FailureReason = %q, want EMAIL_ALREADY_EXISTS", status.CreateAccountStatus.FailureReason)
	}
	// No AccountId, because nothing was vended. A tool that read the ID off a
	// FAILED status would carry an empty string into every call after it.
	if got := aws.ToString(status.CreateAccountStatus.AccountId); got != "" {
		t.Fatalf("a FAILED status carried AccountId %q", got)
	}
}

// TestJourney_OrganizationsDisabledSCPState is #578 point 6 through the SDK: an
// all-features organization whose root has had the SCP type disabled.
//
// It is the state a governance tool is most likely to get wrong, because it does
// not look like a failure — CreatePolicy succeeds, and only the attach is refused.
// Re-enabling then restores p-FullAWSAccess alone: the earlier attachments are
// gone, so a tool that disabled and re-enabled a root has lost its governance and
// gets no error saying so.
func TestJourney_OrganizationsDisabledSCPState(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	orgs := organizations.NewFromConfig(cfg, func(o *organizations.Options) { o.RetryMaxAttempts = 1 })

	roots, err := orgs.ListRoots(ctx, &organizations.ListRootsInput{})
	if err != nil {
		t.Fatalf("ListRoots: %v", err)
	}
	rootID := aws.ToString(roots.Roots[0].Id)

	// A fresh organization already has the managed SCP attached to its root. A
	// caller asserting an empty list here would be asserting against AWS's actual
	// behaviour, which is why this is called out in the release's compatibility note.
	attached := journeyOrgPoliciesForTarget(t, ctx, orgs, rootID)
	if len(attached) != 1 || attached[0] != "p-FullAWSAccess" {
		t.Fatalf("a fresh root's policies = %v, want just p-FullAWSAccess", attached)
	}

	const scpDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"iam:*","Resource":"*"}]}`
	governing, err := orgs.CreatePolicy(ctx, &organizations.CreatePolicyInput{
		Name:        aws.String("deny-iam"),
		Description: aws.String("no IAM in prod"),
		Type:        orgtypes.PolicyTypeServiceControlPolicy,
		Content:     aws.String(scpDoc),
	})
	if err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	policyID := aws.ToString(governing.Policy.PolicySummary.Id)
	if _, err := orgs.AttachPolicy(ctx, &organizations.AttachPolicyInput{
		PolicyId: aws.String(policyID),
		TargetId: aws.String(rootID),
	}); err != nil {
		t.Fatalf("AttachPolicy: %v", err)
	}

	// The last SCP on a target cannot be detached, so p-FullAWSAccess is load-bearing
	// rather than decorative: a tool that detached it to "clean up" would be refused.
	if _, err := orgs.DetachPolicy(ctx, &organizations.DetachPolicyInput{
		PolicyId: aws.String(policyID),
		TargetId: aws.String(rootID),
	}); err != nil {
		t.Fatalf("detaching one of two SCPs: %v", err)
	}
	var constraint *orgtypes.ConstraintViolationException
	if _, err := orgs.DetachPolicy(ctx, &organizations.DetachPolicyInput{
		PolicyId: aws.String("p-FullAWSAccess"),
		TargetId: aws.String(rootID),
	}); err == nil {
		t.Fatal("detaching the last SCP: expected ConstraintViolationException")
	} else if !errors.As(err, &constraint) {
		t.Fatalf("expected *ConstraintViolationException, got %T: %v", err, err)
	}

	// Re-attach, so the disable below has an attachment to lose.
	if _, err := orgs.AttachPolicy(ctx, &organizations.AttachPolicyInput{
		PolicyId: aws.String(policyID),
		TargetId: aws.String(rootID),
	}); err != nil {
		t.Fatalf("re-AttachPolicy: %v", err)
	}

	// --- the dangerous state ---
	if _, err := orgs.DisablePolicyType(ctx, &organizations.DisablePolicyTypeInput{
		RootId:     aws.String(rootID),
		PolicyType: orgtypes.PolicyTypeServiceControlPolicy,
	}); err != nil {
		t.Fatalf("DisablePolicyType: %v", err)
	}

	// CreatePolicy still succeeds — the trap. Nothing in this response tells a
	// caller its policy can never take effect.
	quarantine, err := orgs.CreatePolicy(ctx, &organizations.CreatePolicyInput{
		Name:        aws.String("deny-all"),
		Description: aws.String("quarantine"),
		Type:        orgtypes.PolicyTypeServiceControlPolicy,
		Content:     aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`),
	})
	if err != nil {
		t.Fatalf("CreatePolicy while the type is disabled must still succeed: %v", err)
	}
	var notEnabled *orgtypes.PolicyTypeNotEnabledException
	if _, err := orgs.AttachPolicy(ctx, &organizations.AttachPolicyInput{
		PolicyId: quarantine.Policy.PolicySummary.Id,
		TargetId: aws.String(rootID),
	}); err == nil {
		t.Fatal("attaching while the type is disabled: expected PolicyTypeNotEnabledException")
	} else if !errors.As(err, &notEnabled) {
		t.Fatalf("expected *PolicyTypeNotEnabledException, got %T: %v", err, err)
	}

	// Re-enabling restores only the managed SCP. The deny-iam attachment is gone,
	// and nothing reports that it went.
	if _, err := orgs.EnablePolicyType(ctx, &organizations.EnablePolicyTypeInput{
		RootId:     aws.String(rootID),
		PolicyType: orgtypes.PolicyTypeServiceControlPolicy,
	}); err != nil {
		t.Fatalf("EnablePolicyType: %v", err)
	}
	restored := journeyOrgPoliciesForTarget(t, ctx, orgs, rootID)
	if len(restored) != 1 || restored[0] != "p-FullAWSAccess" {
		t.Fatalf("after re-enabling, the root's policies = %v, want just p-FullAWSAccess "+
			"(attachments from before a disable are lost)", restored)
	}
}

// journeyOrgParents walks every page of ListParents and returns the parent IDs.
func journeyOrgParents(t *testing.T, ctx context.Context, orgs *organizations.Client, childID string) []string {
	t.Helper()
	var ids []string
	pager := organizations.NewListParentsPaginator(orgs, &organizations.ListParentsInput{
		ChildId: aws.String(childID),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("ListParents(%s): %v", childID, err)
		}
		for _, parent := range page.Parents {
			ids = append(ids, aws.ToString(parent.Id))
		}
	}
	return ids
}

// journeyOrgAccountsForParent walks every page of ListAccountsForParent. The
// paginator is used rather than a single call because MaxResults clamps to 20:
// reading only the first page would make this assertion silently partial.
func journeyOrgAccountsForParent(t *testing.T, ctx context.Context, orgs *organizations.Client, parentID string) []string {
	t.Helper()
	var ids []string
	pager := organizations.NewListAccountsForParentPaginator(orgs, &organizations.ListAccountsForParentInput{
		ParentId: aws.String(parentID),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("ListAccountsForParent(%s): %v", parentID, err)
		}
		for _, account := range page.Accounts {
			ids = append(ids, aws.ToString(account.Id))
		}
	}
	return ids
}

// journeyOrgPoliciesForTarget walks every page of ListPoliciesForTarget for the
// SCP type and returns the policy IDs.
func journeyOrgPoliciesForTarget(t *testing.T, ctx context.Context, orgs *organizations.Client, targetID string) []string {
	t.Helper()
	var ids []string
	pager := organizations.NewListPoliciesForTargetPaginator(orgs, &organizations.ListPoliciesForTargetInput{
		TargetId: aws.String(targetID),
		Filter:   orgtypes.PolicyTypeServiceControlPolicy,
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("ListPoliciesForTarget(%s): %v", targetID, err)
		}
		for _, policy := range page.Policies {
			ids = append(ids, aws.ToString(policy.Id))
		}
	}
	return ids
}

// journeySeedOrgCreateFailure seeds the asynchronous outcome of CreateAccount for
// one account name. The seed is a plain HTTP POST rather than an SDK call: it is
// substrate's control plane, not part of the AWS API, which is what keeps the
// journey above it identical to the code a consumer runs against real AWS.
func journeySeedOrgCreateFailure(t *testing.T, ts *emulator.TestServer, accountName, reason string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"accountName":   accountName,
		"failureReason": reason,
	})
	if err != nil {
		t.Fatalf("marshal the create-account-failure seed: %v", err)
	}
	resp, err := http.Post(ts.URL+"/v1/organizations/create-account-failure", //nolint:noctx
		"application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/organizations/create-account-failure: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/organizations/create-account-failure: status %d", resp.StatusCode)
	}
}
