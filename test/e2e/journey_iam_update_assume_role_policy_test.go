package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_UpdateAssumeRolePolicyTightensARoleInPlace is #594 at the SDK level.
//
// The shape being tested is the one CDK and Terraform emit on an update: a role
// exists, and a second pass narrows its trust policy without recreating it.
// UpdateAssumeRolePolicy was absent from substrate's IAM switch, so that second
// pass had nowhere to land — and once #593 made the document load-bearing, the
// tightening a consumer's test was written to verify became unobservable.
//
// Both halves are here. The tightening must deny, and the SDK's own round trip
// through GetRole must report the new document: a caller that cannot read the
// policy back has no way to assert what it set, and substrate did not render
// AssumeRolePolicyDocument on the Role shape at all before this.
func TestJourney_UpdateAssumeRolePolicyTightensARoleInPlace(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	iamClient := iam.NewFromConfig(cfg)

	const roleName = "deploy-broker"
	// The initial policy trusts the journey's own account, so the first assumption
	// succeeds and the denial below can only be the update's doing.
	original := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"` + journeyAccountID + `"},"Action":"sts:AssumeRole"}]}`

	if _, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(original),
	}); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}

	roleARN := "arn:aws:iam::" + journeyAccountID + ":role/" + roleName

	// A real IAM user, for the same reason the #593 journey needs one: the built-in
	// AKIA key resolves to no principal, and an unenforced caller would make every
	// assertion below pass regardless of what the trust policy says.
	user, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("deployer")})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
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

	callerCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			aws.ToString(key.AccessKey.AccessKeyId),
			aws.ToString(key.AccessKey.SecretAccessKey), "")),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("caller config: %v", err)
	}
	// Retries off, so the assertion is about the code on the first response.
	stsClient := sts.NewFromConfig(callerCfg, func(o *sts.Options) { o.RetryMaxAttempts = 1 })

	out, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("before-tightening"),
	})
	if err != nil {
		t.Fatalf("AssumeRole under the original policy: %v", err)
	}
	if out.Credentials == nil || aws.ToString(out.Credentials.AccessKeyId) == "" {
		t.Fatal("expected session credentials under the original policy")
	}

	// The update: narrow the role to a partner account this caller is not in.
	const partnerAccount = "999999999999"
	tightened := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"` + partnerAccount + `"},"Action":"sts:AssumeRole"}]}`
	if _, err := iamClient.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
		RoleName:       aws.String(roleName),
		PolicyDocument: aws.String(tightened),
	}); err != nil {
		t.Fatalf("UpdateAssumeRolePolicy: %v", err)
	}

	// The SDK's read-back, which is the only way a consumer can assert what it set.
	// Substrate emits the document as plain JSON rather than URL-encoded JSON:
	// botocore unquotes then json.loads every policyDocumentType member and
	// aws-sdk-go-v2 hands back the raw string, and unquoting plain JSON is a no-op,
	// so both see valid JSON.
	//
	// Parsed with substrate's own PolicyDocument rather than an ad-hoc struct,
	// because the document is re-marshaled from the stored parsed form: a
	// Principal submitted as {"AWS":"9999…"} comes back as the bare string
	// "9999…", which is the other form AWS accepts for the same meaning. Comparing
	// semantically is therefore the assertion that holds; a byte compare against
	// the submitted text would fail on a normalization, not on a defect.
	role, err := iamClient.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(roleName)})
	if err != nil {
		t.Fatalf("GetRole: %v", err)
	}
	doc := aws.ToString(role.Role.AssumeRolePolicyDocument)
	var parsed emulator.PolicyDocument
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("GetRole trust policy %q does not parse as JSON: %v", doc, err)
	}
	if len(parsed.Statement) != 1 {
		t.Fatalf("GetRole trust policy = %q, want exactly one statement", doc)
	}
	principal := parsed.Statement[0].Principal
	if principal == nil || len(principal.AWS) != 1 || principal.AWS[0] != partnerAccount {
		t.Fatalf("trust policy = %q, want a lone AWS principal of %s (the update must displace, not merge)",
			doc, partnerAccount)
	}

	// The tightened policy is the one the gate reads on the *next* assumption.
	_, err = stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String("after-tightening"),
	})
	if err == nil {
		t.Fatal("AssumeRole after tightening: expected a denial, got credentials")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected a smithy.APIError, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "AccessDenied" {
		t.Fatalf("after tightening: ErrorCode() = %q, want AccessDenied", apiErr.ErrorCode())
	}

	// An unknown role is NoSuchEntity. Asserted through the SDK's *typed* error,
	// because that is the branch a consumer's error handling actually takes — a code
	// that only matches as a string would leave errors.As falling through.
	var noSuchEntity *iamtypes.NoSuchEntityException
	if _, err := iamClient.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
		RoleName:       aws.String("no-such-role"),
		PolicyDocument: aws.String(tightened),
	}); err == nil {
		t.Fatal("UpdateAssumeRolePolicy on an unknown role: expected NoSuchEntity")
	} else if !errors.As(err, &noSuchEntity) {
		t.Fatalf("expected *iamtypes.NoSuchEntityException, got %T: %v", err, err)
	}
}
