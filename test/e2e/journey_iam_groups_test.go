package e2e_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_IAMGroupMembership drives the group operations through the real SDK.
//
// This level catches what the unit suite structurally cannot: an operation that is
// implemented but not routed answers InvalidAction, and a unit test that calls the
// handler directly never finds out. That was #636, and every one of these
// operations is new routing.
func TestJourney_IAMGroupMembership(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion is about the first response.
	client := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	const groupName, userName = "devs", "jill"

	if _, err := client.CreateGroup(ctx, &iam.CreateGroupInput{
		GroupName: aws.String(groupName),
	}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if _, err := client.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(userName),
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if _, err := client.AddUserToGroup(ctx, &iam.AddUserToGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	}); err != nil {
		t.Fatalf("AddUserToGroup: %v", err)
	}

	// GetGroup's member list was hardcoded empty before this release, so this is the
	// assertion that would have failed on the shipped code even with membership
	// stored correctly.
	group, err := client.GetGroup(ctx, &iam.GetGroupInput{GroupName: aws.String(groupName)})
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if len(group.Users) != 1 || aws.ToString(group.Users[0].UserName) != userName {
		t.Fatalf("GetGroup listed %d users, want exactly %s", len(group.Users), userName)
	}
	if aws.ToString(group.Group.GroupName) != groupName {
		t.Errorf("GetGroup returned group %q, want %q", aws.ToString(group.Group.GroupName), groupName)
	}

	// The other direction of the same membership. Both are read by an API, so a
	// single-sided write would satisfy one of these two calls and not the other.
	forUser, err := client.ListGroupsForUser(ctx, &iam.ListGroupsForUserInput{
		UserName: aws.String(userName),
	})
	if err != nil {
		t.Fatalf("ListGroupsForUser: %v", err)
	}
	if len(forUser.Groups) != 1 || aws.ToString(forUser.Groups[0].GroupName) != groupName {
		t.Fatalf("ListGroupsForUser returned %d groups, want exactly %s", len(forUser.Groups), groupName)
	}

	// Group policies: a managed attachment and an inline document, each read back
	// through the operation that reports it.
	const policyARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	if _, err := client.AttachGroupPolicy(ctx, &iam.AttachGroupPolicyInput{
		GroupName: aws.String(groupName),
		PolicyArn: aws.String(policyARN),
	}); err != nil {
		t.Fatalf("AttachGroupPolicy: %v", err)
	}
	attached, err := client.ListAttachedGroupPolicies(ctx, &iam.ListAttachedGroupPoliciesInput{
		GroupName: aws.String(groupName),
	})
	if err != nil {
		t.Fatalf("ListAttachedGroupPolicies: %v", err)
	}
	if len(attached.AttachedPolicies) != 1 ||
		aws.ToString(attached.AttachedPolicies[0].PolicyArn) != policyARN {
		t.Fatalf("ListAttachedGroupPolicies returned %v, want %s", attached.AttachedPolicies, policyARN)
	}

	const inlineDoc = `{"Version":"2012-10-17","Statement":[{"Sid":"ListBuckets",` +
		`"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"*"}]}`
	if _, err := client.PutGroupPolicy(ctx, &iam.PutGroupPolicyInput{
		GroupName:      aws.String(groupName),
		PolicyName:     aws.String("listbuckets"),
		PolicyDocument: aws.String(inlineDoc),
	}); err != nil {
		t.Fatalf("PutGroupPolicy: %v", err)
	}
	inline, err := client.GetGroupPolicy(ctx, &iam.GetGroupPolicyInput{
		GroupName:  aws.String(groupName),
		PolicyName: aws.String("listbuckets"),
	})
	if err != nil {
		t.Fatalf("GetGroupPolicy: %v", err)
	}
	// The SDK models GroupName as required on the response, so a handler emitting
	// <UserName> — which the inline family's default arm did for every non-role type
	// — leaves this empty.
	if aws.ToString(inline.GroupName) != groupName {
		t.Errorf("GetGroupPolicy reported GroupName %q, want %q", aws.ToString(inline.GroupName), groupName)
	}
	names, err := client.ListGroupPolicies(ctx, &iam.ListGroupPoliciesInput{
		GroupName: aws.String(groupName),
	})
	if err != nil {
		t.Fatalf("ListGroupPolicies: %v", err)
	}
	if len(names.PolicyNames) != 1 || names.PolicyNames[0] != "listbuckets" {
		t.Fatalf("ListGroupPolicies returned %v, want [listbuckets]", names.PolicyNames)
	}

	// Removal clears both sides.
	if _, err := client.RemoveUserFromGroup(ctx, &iam.RemoveUserFromGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	}); err != nil {
		t.Fatalf("RemoveUserFromGroup: %v", err)
	}
	group, err = client.GetGroup(ctx, &iam.GetGroupInput{GroupName: aws.String(groupName)})
	if err != nil {
		t.Fatalf("GetGroup after removal: %v", err)
	}
	if len(group.Users) != 0 {
		t.Errorf("GetGroup still lists %d users after RemoveUserFromGroup", len(group.Users))
	}
	forUser, err = client.ListGroupsForUser(ctx, &iam.ListGroupsForUserInput{
		UserName: aws.String(userName),
	})
	if err != nil {
		t.Fatalf("ListGroupsForUser after removal: %v", err)
	}
	if len(forUser.Groups) != 0 {
		t.Errorf("ListGroupsForUser still lists %d groups after RemoveUserFromGroup", len(forUser.Groups))
	}

	if _, err := client.DetachGroupPolicy(ctx, &iam.DetachGroupPolicyInput{
		GroupName: aws.String(groupName),
		PolicyArn: aws.String(policyARN),
	}); err != nil {
		t.Fatalf("DetachGroupPolicy: %v", err)
	}
	if _, err := client.DeleteGroupPolicy(ctx, &iam.DeleteGroupPolicyInput{
		GroupName:  aws.String(groupName),
		PolicyName: aws.String("listbuckets"),
	}); err != nil {
		t.Fatalf("DeleteGroupPolicy: %v", err)
	}
	if _, err := client.DeleteGroup(ctx, &iam.DeleteGroupInput{
		GroupName: aws.String(groupName),
	}); err != nil {
		t.Fatalf("DeleteGroup after clearing members and policies: %v", err)
	}
}
