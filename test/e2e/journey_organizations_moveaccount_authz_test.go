package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_OrganizationsMoveAccountConfinement is #660 at the tier a consumer's
// own confinement fixture occupies: a delegated administrator holding a policy that
// names the account it may move and the OU it may move that account into, and
// nothing else.
//
// MoveAccount names three resources — the account, the source parent and the
// destination parent — and the Service Authorization Reference marks all three
// required. Substrate authorized against the account alone, so a policy that never
// named the root allowed a move into the root: the boundary read as a confinement
// and was not one.
//
// Only this tier shows what the caller sees. The refusal has to arrive as the
// typed *AccessDeniedException a consumer's error handling branches on, and its
// message has to name the resource the policy is missing — otherwise the fix is
// correct and undiagnosable.
func TestJourney_OrganizationsMoveAccountConfinement(t *testing.T) {
	ts := emulator.StartTestServer(t)

	adminCfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	admin := organizations.NewFromConfig(adminCfg,
		func(o *organizations.Options) { o.RetryMaxAttempts = 1 })
	iamClient := iam.NewFromConfig(adminCfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	// --- the hierarchy, built by the unenforced built-in caller ---
	roots, err := admin.ListRoots(ctx, &organizations.ListRootsInput{})
	if err != nil {
		t.Fatalf("ListRoots: %v", err)
	}
	if len(roots.Roots) != 1 {
		t.Fatalf("ListRoots returned %d roots, want 1", len(roots.Roots))
	}
	rootID := aws.ToString(roots.Roots[0].Id)
	rootARN := aws.ToString(roots.Roots[0].Arn)

	ou, err := admin.CreateOrganizationalUnit(ctx, &organizations.CreateOrganizationalUnitInput{
		ParentId: aws.String(rootID),
		Name:     aws.String("confined"),
	})
	if err != nil {
		t.Fatalf("CreateOrganizationalUnit: %v", err)
	}
	ouID := aws.ToString(ou.OrganizationalUnit.Id)
	ouARN := aws.ToString(ou.OrganizationalUnit.Arn)

	created, err := admin.CreateAccount(ctx, &organizations.CreateAccountInput{
		AccountName: aws.String("workload"),
		Email:       aws.String("workload@example.com"),
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	status, err := admin.DescribeCreateAccountStatus(ctx,
		&organizations.DescribeCreateAccountStatusInput{
			CreateAccountRequestId: created.CreateAccountStatus.Id,
		})
	if err != nil {
		t.Fatalf("DescribeCreateAccountStatus: %v", err)
	}
	accountID := aws.ToString(status.CreateAccountStatus.AccountId)
	if accountID == "" {
		t.Fatal("a SUCCEEDED create-account status carried no AccountId")
	}
	account, err := admin.DescribeAccount(ctx, &organizations.DescribeAccountInput{
		AccountId: aws.String(accountID),
	})
	if err != nil {
		t.Fatalf("DescribeAccount: %v", err)
	}
	accountARN := aws.ToString(account.Account.Arn)

	// --- the confined mover ---
	//
	// A real IAM user, because the built-in AKIA key resolves to no principal and an
	// unenforced caller would make every assertion below pass regardless of policy.
	// The policy names the account and the destination OU — the ARNs a consumer
	// pastes out of the Describe responses above — and deliberately not the root.
	user, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("mover")})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if _, err := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:   user.User.UserName,
		PolicyName: aws.String("confine"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{` +
			`"Effect":"Allow","Action":"organizations:MoveAccount","Resource":[` +
			`"` + accountARN + `","` + ouARN + `"]}]}`),
	}); err != nil {
		t.Fatalf("PutUserPolicy: %v", err)
	}
	key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: user.User.UserName,
	})
	if err != nil {
		t.Fatalf("CreateAccessKey: %v", err)
	}
	moverCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			aws.ToString(key.AccessKey.AccessKeyId),
			aws.ToString(key.AccessKey.SecretAccessKey), "")),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("mover config: %v", err)
	}
	mover := organizations.NewFromConfig(moverCfg,
		func(o *organizations.Options) { o.RetryMaxAttempts = 1 })

	// --- the move out of the root, which the policy does not name ---
	//
	// This is the request that was allowed. The source parent is the root, so the
	// policy has to name it and does not.
	var denied *orgtypes.AccessDeniedException
	_, err = mover.MoveAccount(ctx, &organizations.MoveAccountInput{
		AccountId:           aws.String(accountID),
		SourceParentId:      aws.String(rootID),
		DestinationParentId: aws.String(ouID),
	})
	if !errors.As(err, &denied) {
		t.Fatalf("moving out of a root the policy does not name: got %T (%v), want *AccessDeniedException", err, err)
	}
	// The message is the only place the missing resource surfaces, and it is what
	// tells the caller which ARN to add.
	if msg := err.Error(); !strings.Contains(msg, rootARN) {
		t.Errorf("the denial does not name the root ARN %s: %v", rootARN, msg)
	}
	// And the move really did not happen: a refusal that had already written state
	// would be worse than the false allow it replaced.
	if parents := journeyOrgParents(t, ctx, admin, accountID); len(parents) != 1 || parents[0] != rootID {
		t.Fatalf("after the refusal the account's parents = %v, want just the root %s", parents, rootID)
	}

	// --- widening the policy by exactly the resource the denial named ---
	if _, err := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:   user.User.UserName,
		PolicyName: aws.String("confine"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{` +
			`"Effect":"Allow","Action":"organizations:MoveAccount","Resource":[` +
			`"` + accountARN + `","` + ouARN + `","` + rootARN + `"]}]}`),
	}); err != nil {
		t.Fatalf("PutUserPolicy widening to the root: %v", err)
	}
	if _, err := mover.MoveAccount(ctx, &organizations.MoveAccountInput{
		AccountId:           aws.String(accountID),
		SourceParentId:      aws.String(rootID),
		DestinationParentId: aws.String(ouID),
	}); err != nil {
		t.Fatalf("the move must be allowed once the policy names every resource it touches: %v", err)
	}
	if parents := journeyOrgParents(t, ctx, admin, accountID); len(parents) != 1 || parents[0] != ouID {
		t.Fatalf("after the move, parents = %v, want just the OU %s", parents, ouID)
	}

	// --- and the confinement holds in the other direction ---
	//
	// Narrowing back to account + OU must stop the account from leaving the OU,
	// which is what a delegated-admin boundary is written to guarantee.
	if _, err := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:   user.User.UserName,
		PolicyName: aws.String("confine"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{` +
			`"Effect":"Allow","Action":"organizations:MoveAccount","Resource":[` +
			`"` + accountARN + `","` + ouARN + `"]}]}`),
	}); err != nil {
		t.Fatalf("PutUserPolicy narrowing back: %v", err)
	}
	_, err = mover.MoveAccount(ctx, &organizations.MoveAccountInput{
		AccountId:           aws.String(accountID),
		SourceParentId:      aws.String(ouID),
		DestinationParentId: aws.String(rootID),
	})
	if !errors.As(err, &denied) {
		t.Fatalf("moving into a root the policy does not name: got %T (%v), want *AccessDeniedException", err, err)
	}
	if parents := journeyOrgParents(t, ctx, admin, accountID); len(parents) != 1 || parents[0] != ouID {
		t.Fatalf("the confined account left the OU: parents = %v, want just %s", parents, ouID)
	}
}
