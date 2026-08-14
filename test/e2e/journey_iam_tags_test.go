package e2e_test

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_IAMTagsSurviveTheWire is #639 at the SDK level: an IAM list
// parameter arriving in the query protocol's `.member.N` encoding.
//
// This is a level the unit suite structurally cannot reach. `iamRequest`
// (emulator/iam_plugin_test.go) hand-marshals its body, so a unit test hands
// tagUser a real JSON array — while a real client sends
// `Tags.member.1.Key=env&Tags.member.1.Value=prod`, which server.go turns into a
// JSON body whose keys are *literally* "Tags.member.1.Key". Every `[]string` and
// `[]struct` field therefore unmarshaled to nil, so TagUser answered 200 and
// stored nothing. Same blind spot as #561/#610/#636: green tests over a shape no
// client can produce.
//
// The list is two-element on purpose. A decoder that reads only index 1 would
// pass a single-tag assertion.
func TestJourney_IAMTagsSurviveTheWire(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion is about the first response.
	iamClient := iam.NewFromConfig(cfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	const userName = "jill"
	if _, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(userName),
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if _, err := iamClient.TagUser(ctx, &iam.TagUserInput{
		UserName: aws.String(userName),
		Tags: []iamtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("infra")},
		},
	}); err != nil {
		t.Fatalf("TagUser: %v", err)
	}

	got := listUserTags(t, ctx, iamClient, userName)
	want := map[string]string{"env": "prod", "team": "infra"}
	if len(got) != len(want) {
		t.Fatalf("ListUserTags returned %v, want %v — the tags an SDK sent never reached the handler", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("tag %q = %q, want %q", k, got[k], v)
		}
	}

	// The removal half: UntagRole/UntagUser take TagKeys, a flat member list rather
	// than a struct one, so it exercises the other decoder. Removing one of two
	// proves the keys are read rather than the whole tag set being dropped.
	if _, err := iamClient.UntagUser(ctx, &iam.UntagUserInput{
		UserName: aws.String(userName),
		TagKeys:  []string{"env"},
	}); err != nil {
		t.Fatalf("UntagUser: %v", err)
	}
	got = listUserTags(t, ctx, iamClient, userName)
	if len(got) != 1 || got["team"] != "infra" {
		t.Fatalf("after untagging env, ListUserTags = %v, want only team=infra", got)
	}

	// Roles carry the same two handlers over the same encoding, and each was broken
	// independently — a fix applied only to the user pair would leave these nil.
	const roleName = "tagged-role"
	if _, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName: aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`),
	}); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if _, err := iamClient.TagRole(ctx, &iam.TagRoleInput{
		RoleName: aws.String(roleName),
		Tags: []iamtypes.Tag{
			{Key: aws.String("owner"), Value: aws.String("platform")},
			{Key: aws.String("tier"), Value: aws.String("prod")},
		},
	}); err != nil {
		t.Fatalf("TagRole: %v", err)
	}
	roleTags, err := iamClient.ListRoleTags(ctx, &iam.ListRoleTagsInput{
		RoleName: aws.String(roleName),
	})
	if err != nil {
		t.Fatalf("ListRoleTags: %v", err)
	}
	if len(roleTags.Tags) != 2 {
		t.Fatalf("ListRoleTags returned %d tags, want 2", len(roleTags.Tags))
	}
	if _, err := iamClient.UntagRole(ctx, &iam.UntagRoleInput{
		RoleName: aws.String(roleName),
		TagKeys:  []string{"owner", "tier"},
	}); err != nil {
		t.Fatalf("UntagRole: %v", err)
	}
	roleTags, err = iamClient.ListRoleTags(ctx, &iam.ListRoleTagsInput{
		RoleName: aws.String(roleName),
	})
	if err != nil {
		t.Fatalf("ListRoleTags after untagging: %v", err)
	}
	if len(roleTags.Tags) != 0 {
		keys := make([]string, 0, len(roleTags.Tags))
		for _, tag := range roleTags.Tags {
			keys = append(keys, aws.ToString(tag.Key))
		}
		sort.Strings(keys)
		t.Errorf("UntagRole left %v behind; both keys were named", keys)
	}
}

// listUserTags reads a user's tags back as a map, which is what the assertions
// above compare against — the API's order is the emulator's sort order, not
// something a consumer should be pinned to.
func listUserTags(t *testing.T, ctx context.Context, client *iam.Client, userName string) map[string]string {
	t.Helper()
	out, err := client.ListUserTags(ctx, &iam.ListUserTagsInput{
		UserName: aws.String(userName),
	})
	if err != nil {
		t.Fatalf("ListUserTags: %v", err)
	}
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return tags
}
