package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	idptypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_CognitoUserPoolShapeDecodes is #756's Cognito block at the only tier that could have
// caught it, which is TestJourney_DynamoDBTableArnDecodes' argument (#738) in a second service.
//
// Three of the defects this exercises were invisible to the unit tier by construction. Substrate
// reported the pool's identifier as `UserPoolId`, which `UserPoolType` does not publish, so a real SDK
// decoded `UserPool.Id` as nil and a consumer reading it got an empty string with no error. It reported
// the tag set as `Tags` where the shape publishes `UserPoolTags`, with the same result. And it reported
// every timestamp as an RFC3339 string where both Cognito services publish epoch seconds, which the
// SDK's decoder rejects outright — the one failure here that surfaces as an error rather than as a
// silent zero value, and the reason an SDK-driven test is worth more in this block than usual.
//
// The emulator module cannot assert any of this: a hand-written decode asserts on whatever spelling
// substrate emits, which is exactly how the unit tests stayed green across every release that shipped
// the defect. The e2e module had no Cognito client at all before this.
func TestJourney_CognitoUserPoolShapeDecodes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := cognitoidentityprovider.NewFromConfig(cfg, func(o *cognitoidentityprovider.Options) {
		o.RetryMaxAttempts = 1
	})

	create, err := client.CreateUserPool(ctx, &cognitoidentityprovider.CreateUserPoolInput{
		PoolName:         aws.String("journey-pool"),
		MfaConfiguration: idptypes.UserPoolMfaTypeOn,
		UserPoolTags:     map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	poolID := assertPoolDecodes(t, "CreateUserPool", create.UserPool)

	describe, err := client.DescribeUserPool(ctx, &cognitoidentityprovider.DescribeUserPoolInput{
		UserPoolId: aws.String(poolID),
	})
	if err != nil {
		t.Fatalf("DescribeUserPool: %v", err)
	}
	if got := assertPoolDecodes(t, "DescribeUserPool", describe.UserPool); got != poolID {
		t.Errorf("DescribeUserPool reported Id %q, want the created pool's %q", got, poolID)
	}

	// The summary shape is API_UserPoolDescriptionType rather than UserPoolType, and it is the door that
	// already spelled Id correctly — so this is the half of #1286 that asserts the two now agree.
	list, err := client.ListUserPools(ctx, &cognitoidentityprovider.ListUserPoolsInput{
		MaxResults: aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("ListUserPools: %v", err)
	}
	if len(list.UserPools) != 1 {
		t.Fatalf("ListUserPools reported %d pools, want 1", len(list.UserPools))
	}
	if aws.ToString(list.UserPools[0].Id) != poolID {
		t.Errorf("ListUserPools reported Id %q, want %q", aws.ToString(list.UserPools[0].Id), poolID)
	}
	if list.UserPools[0].CreationDate == nil {
		t.Error("ListUserPools reported no CreationDate; the summary publishes one")
	}

	// A group and a user, for the two shapes whose timestamps are named differently again. Either would
	// have failed the SDK's epoch decoder before this change, the group on CreationDate and the user on
	// UserCreateDate and UserLastModifiedDate.
	group, err := client.CreateGroup(ctx, &cognitoidentityprovider.CreateGroupInput{
		UserPoolId: aws.String(poolID),
		GroupName:  aws.String("journey-admins"),
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if group.Group == nil || group.Group.CreationDate == nil {
		t.Fatal("CreateGroup reported no Group.CreationDate")
	}

	user, err := client.AdminCreateUser(ctx, &cognitoidentityprovider.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("journey-user"),
	})
	if err != nil {
		t.Fatalf("AdminCreateUser: %v", err)
	}
	if user.User == nil {
		t.Fatal("AdminCreateUser reported no User")
	}
	if user.User.UserCreateDate == nil || user.User.UserLastModifiedDate == nil {
		t.Error("AdminCreateUser reported a user with no create or modify timestamp")
	}

	// AdminGetUser publishes the user's members at the top level rather than nested, so it is a separate
	// shape with the same two timestamps and was fixed alongside the projection.
	got, err := client.AdminGetUser(ctx, &cognitoidentityprovider.AdminGetUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("journey-user"),
	})
	if err != nil {
		t.Fatalf("AdminGetUser: %v", err)
	}
	if got.UserCreateDate == nil {
		t.Error("AdminGetUser reported no UserCreateDate")
	}
}

// assertPoolDecodes checks the members a real SDK reads off a UserPoolType and returns the pool's Id.
//
// aws.ToString turning a nil pointer into "" is how a wrong member name stays invisible, so the Id is
// checked as a pointer before its value is used, as assertTableARNs does for #738.
func assertPoolDecodes(t *testing.T, op string, pool *idptypes.UserPoolType) string {
	t.Helper()
	if pool == nil {
		t.Fatalf("%s reported no UserPool", op)
	}
	if pool.Id == nil {
		t.Fatalf("%s: UserPoolType.Id is nil — substrate reported the identifier under a member name "+
			"the shape does not publish", op)
	}
	if *pool.Id == "" {
		t.Errorf("%s: UserPoolType.Id decoded empty", op)
	}
	if pool.UserPoolTags["env"] != "prod" {
		t.Errorf("%s: UserPoolTags = %v, want the create's env=prod — a tag set reported under another "+
			"member name decodes as an empty map", op, pool.UserPoolTags)
	}
	if pool.CreationDate == nil {
		t.Errorf("%s: UserPoolType.CreationDate is nil", op)
	}
	if pool.LastModifiedDate == nil {
		t.Errorf("%s: UserPoolType.LastModifiedDate is nil", op)
	}
	return *pool.Id
}

// TestJourney_CognitoIdentityCredentialsExpirationDecodes covers the identity service's one timestamp.
//
// API_Credentials publishes Expiration in epoch seconds like every Cognito timestamp, and substrate
// reported an RFC3339 string, so the SDK's decoder was handed a string for a number. A caller deciding
// when to refresh reads this member and nothing else.
func TestJourney_CognitoIdentityCredentialsExpirationDecodes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := cognitoidentity.NewFromConfig(cfg, func(o *cognitoidentity.Options) { o.RetryMaxAttempts = 1 })

	pool, err := client.CreateIdentityPool(ctx, &cognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("journey-identities"),
		AllowUnauthenticatedIdentities: true,
	})
	if err != nil {
		t.Fatalf("CreateIdentityPool: %v", err)
	}
	if aws.ToString(pool.IdentityPoolId) == "" {
		t.Fatal("CreateIdentityPool reported no IdentityPoolId")
	}

	id, err := client.GetId(ctx, &cognitoidentity.GetIdInput{IdentityPoolId: pool.IdentityPoolId})
	if err != nil {
		t.Fatalf("GetId: %v", err)
	}

	creds, err := client.GetCredentialsForIdentity(ctx, &cognitoidentity.GetCredentialsForIdentityInput{
		IdentityId: id.IdentityId,
	})
	if err != nil {
		t.Fatalf("GetCredentialsForIdentity: %v", err)
	}
	if creds.Credentials == nil {
		t.Fatal("GetCredentialsForIdentity reported no Credentials")
	}
	if creds.Credentials.Expiration == nil {
		t.Fatal("GetCredentialsForIdentity reported no Credentials.Expiration")
	}
	// The emulator stamps the expiry an hour past its simulated clock, which a test server seeds from
	// the real one — so the bound is generous on both sides rather than an equality against wall time.
	if until := time.Until(*creds.Credentials.Expiration); until <= 0 || until > 2*time.Hour {
		t.Errorf("Expiration is %v away, want an hour-ish in the future", until)
	}
	if aws.ToString(creds.Credentials.AccessKeyId) == "" {
		t.Error("GetCredentialsForIdentity reported no AccessKeyId")
	}
}
