package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_AssumeRoleHonoursTheTrustPolicy is #593's gate at the SDK level.
//
// The confused-deputy defense is the whole reason sts:ExternalId exists: a role
// shared with a third party names their account and conditions on a secret only
// the real partner knows. Substrate never read AssumeRolePolicyDocument, so that
// condition was inert — a consumer's "a caller without the secret cannot assume
// this role" test passed while verifying nothing, and passed identically with the
// trust policy deleted.
//
// This journey is at the SDK level rather than in emulator/ because it is the
// shape a consumer actually writes: assume a role, get credentials, use them. The
// negative and the positive are both here, because a gate that denies
// unconditionally produces the same output as a working one for the denial half
// alone.
func TestJourney_AssumeRoleHonoursTheTrustPolicy(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	iamClient := iam.NewFromConfig(cfg)

	const (
		roleName   = "partner-broker"
		externalID = "secret-123"
	)
	// The trust policy AWS's own ExternalId guide documents: the account may
	// assume the role, but only when it presents the shared secret.
	trustPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"` + journeyAccountID + `"},"Action":"sts:AssumeRole",` +
		`"Condition":{"StringEquals":{"sts:ExternalId":"` + externalID + `"}}}]}`

	if _, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(trustPolicy),
	}); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}

	roleARN := "arn:aws:iam::" + journeyAccountID + ":role/" + roleName

	// The caller has to be a real IAM user for a trust policy to have anyone to
	// be true of. The journey's built-in AKIA key belongs to no IAM entity, so it
	// resolves to no principal and is unenforced — the documented opt-in — which
	// would make every assertion below pass for the wrong reason. Minting a key is
	// also what a consumer does when testing a cross-account integration.
	user, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("partner")})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	// sts:AssumeRole on the identity side, so a denial below can only have come
	// from the trust policy. Without this the caller fails the *identity* gate with
	// AccessDeniedException and the trust policy is never consulted.
	if _, err := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:   user.User.UserName,
		PolicyName: aws.String("assume"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"*"}]}`),
	}); err != nil {
		t.Fatalf("PutUserPolicy: %v", err)
	}
	key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: user.User.UserName,
	})
	if err != nil {
		t.Fatalf("CreateAccessKey: %v", err)
	}

	partnerCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			aws.ToString(key.AccessKey.AccessKeyId),
			aws.ToString(key.AccessKey.SecretAccessKey), "")),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("partner config: %v", err)
	}
	// Retries off: a retried call would obscure which attempt produced the denial,
	// and the assertion is about the code on the response the caller sees.
	stsClient := sts.NewFromConfig(partnerCfg, func(o *sts.Options) { o.RetryMaxAttempts = 1 })

	// Without the ExternalId the trust policy's condition cannot be satisfied. This
	// returned working ASIA credentials before the fix.
	_, err = stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("no-secret"),
	})
	if err == nil {
		t.Fatal("AssumeRole without the ExternalId: expected a denial, got credentials")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected a smithy.APIError, got %T: %v", err, err)
	}
	// AccessDenied, not AccessDeniedException: AWS's own CLI output for this
	// operation reads "An error occurred (AccessDenied) when calling the AssumeRole
	// operation". The STS API model documents no access-denied error at all, so
	// this code comes from observed behavior.
	if apiErr.ErrorCode() != "AccessDenied" {
		t.Fatalf("no ExternalId: ErrorCode() = %q, want AccessDenied", apiErr.ErrorCode())
	}

	// A wrong secret is refused the same way — the condition is compared, not
	// merely required to be present.
	if _, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("wrong-secret"),
		ExternalId:      aws.String("not-the-secret"),
	}); err == nil {
		t.Fatal("AssumeRole with a wrong ExternalId: expected a denial, got credentials")
	} else if !errors.As(err, &apiErr) {
		t.Fatalf("expected a smithy.APIError, got %T: %v", err, err)
	} else if apiErr.ErrorCode() != "AccessDenied" {
		t.Fatalf("wrong ExternalId: ErrorCode() = %q, want AccessDenied", apiErr.ErrorCode())
	}

	// The negative control, and the half that makes the assertions above mean
	// something: the real partner still gets a session.
	out, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("real-partner"),
		ExternalId:      aws.String(externalID),
	})
	if err != nil {
		t.Fatalf("AssumeRole with the correct ExternalId: %v", err)
	}
	if out.Credentials == nil || aws.ToString(out.Credentials.AccessKeyId) == "" {
		t.Fatal("expected session credentials for the admitted caller")
	}
	if got := aws.ToString(out.Credentials.AccessKeyId); got[:4] != "ASIA" {
		t.Fatalf("AccessKeyId = %q, want an ASIA session key", got)
	}
	if arn := aws.ToString(out.AssumedRoleUser.Arn); arn == "" {
		t.Fatal("expected an AssumedRoleUser ARN")
	}

	// A role with no trust policy stays unenforced: writing one is the opt-in, so
	// every test that never wrote one keeps working.
	if _, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String("legacy"),
		AssumeRolePolicyDocument: aws.String(""),
	}); err != nil {
		t.Fatalf("CreateRole legacy: %v", err)
	}
	if _, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:aws:iam::" + journeyAccountID + ":role/legacy"),
		RoleSessionName: aws.String("unenforced"),
	}); err != nil {
		t.Fatalf("a role with no trust policy must stay assumable: %v", err)
	}
}
