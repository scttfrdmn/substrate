package e2e_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_IAMNumericParameters drives IAM's integer parameters through the real
// SDK.
//
// It is the regression for #642, and it fails against the code before that fix:
// IAM speaks the query protocol, so every parameter reaches a handler as a *string*,
// and a field declared `int` made encoding/json refuse the request. `MaxItems` on
// any paginated operation and `MaxSessionDuration` on CreateRole both answered
// 400 ValidationError.
//
// It has to live at this level for the same reason #639's did. The unit suite's
// iamRequest hand-marshals a body holding a real JSON number, so it exercises the
// one shape no client sends; only an SDK call travels the query → params → JSON
// path where the value is a string. Four tag operations shipped broken through that
// gap, and this defect shipped through it too.
func TestJourney_IAMNumericParameters(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion is about the first response.
	client := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	for _, name := range []string{"ann", "bob", "cyd"} {
		if _, err := client.CreateUser(ctx, &iam.CreateUserInput{
			UserName: aws.String(name),
		}); err != nil {
			t.Fatalf("CreateUser %s: %v", name, err)
		}
	}

	// MaxItems is the member that failed. Two of three users, then the rest through
	// the marker, so the page really is a page and not a rejected request.
	first, err := client.ListUsers(ctx, &iam.ListUsersInput{MaxItems: aws.Int32(2)})
	if err != nil {
		t.Fatalf("ListUsers with MaxItems: %v", err)
	}
	if len(first.Users) != 2 {
		t.Fatalf("MaxItems=2 returned %d users, want 2", len(first.Users))
	}
	if !first.IsTruncated {
		t.Fatal("MaxItems=2 over three users reported IsTruncated=false")
	}
	if aws.ToString(first.Marker) == "" {
		t.Fatal("truncated page carried no Marker")
	}

	second, err := client.ListUsers(ctx, &iam.ListUsersInput{
		MaxItems: aws.Int32(2),
		Marker:   first.Marker,
	})
	if err != nil {
		t.Fatalf("ListUsers second page: %v", err)
	}
	if len(second.Users) != 1 {
		t.Fatalf("second page returned %d users, want 1", len(second.Users))
	}
	if second.IsTruncated {
		t.Fatal("final page reported IsTruncated=true")
	}

	// The two pages must be disjoint and cover everything — a Marker that silently
	// restarted would still produce the counts above.
	seen := map[string]bool{}
	for _, u := range append(first.Users, second.Users...) {
		name := aws.ToString(u.UserName)
		if seen[name] {
			t.Fatalf("user %s appeared on both pages", name)
		}
		seen[name] = true
	}
	if len(seen) != 3 {
		t.Fatalf("pages covered %d users, want 3", len(seen))
	}

	// MaxSessionDuration is the same defect on a non-pagination member, and unlike
	// MaxItems it is stored, so GetRole proves the value survived the round trip
	// rather than merely being accepted.
	const trust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	if _, err := client.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String("worker"),
		AssumeRolePolicyDocument: aws.String(trust),
		MaxSessionDuration:       aws.Int32(7200),
	}); err != nil {
		t.Fatalf("CreateRole with MaxSessionDuration: %v", err)
	}
	role, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String("worker")})
	if err != nil {
		t.Fatalf("GetRole: %v", err)
	}
	if got := aws.ToInt32(role.Role.MaxSessionDuration); got != 7200 {
		t.Fatalf("MaxSessionDuration round-tripped as %d, want 7200", got)
	}
}
