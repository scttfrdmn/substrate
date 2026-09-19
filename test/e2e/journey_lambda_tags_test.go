package e2e_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_LambdaTags is #1142 at the SDK level: Lambda dates each operation's URI at the version
// that operation was introduced, and substrate routed one date.
//
// `lambda.TagResource` against a function the emulator had just created answered:
//
//	operation error Lambda: TagResource, https response error StatusCode: 404, RequestID: ,
//	api error UnknownOperationException: The action POST /2017-03-31/tags/arn:aws:lambda:…
//	is not recognized.
//
// Everything behind the route was implemented — the dispatch arm, the handler, the parser's own
// `/tags/` arm — and the parser trimmed the literal prefix `/2015-03-31`, so no path under any other
// date could reach any of it. Substrate's Lambda tests posted to the date the parser wanted rather
// than the one the API publishes, which is why nine of them had to move with the fix and why only a
// real client could have caught it.
//
// The journey drives the whole sequence #1142's reporter could not: create, tag, read the tags back,
// untag, and the two other families the sweep found on the wrong date.
func TestJourney_LambdaTags(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion is about the first response rather than whatever the retry loop
	// settled on.
	client := lambda.NewFromConfig(cfg, func(o *lambda.Options) { o.RetryMaxAttempts = 1 })

	created, err := client.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: aws.String("journey-poller"),
		Runtime:      lambdatypes.RuntimeProvidedal2023,
		Role:         aws.String("arn:aws:iam::" + journeyAccountID + ":role/journey-lambda"),
		Handler:      aws.String("bootstrap"),
		Architectures: []lambdatypes.Architecture{
			lambdatypes.ArchitectureArm64,
		},
		Code: &lambdatypes.FunctionCode{ZipFile: []byte("not a real zip, and substrate runs nothing")},
	})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	fnARN := aws.ToString(created.FunctionArn)
	if fnARN == "" {
		t.Fatal("CreateFunction returned no FunctionArn")
	}

	// --- the defect: the tag family is published at /2017-03-31 ---

	if _, err := client.TagResource(ctx, &lambda.TagResourceInput{
		Resource: aws.String(fnARN),
		Tags:     map[string]string{"Application": "lagotto", "env": "journey"},
	}); err != nil {
		t.Fatalf("TagResource: %v — an unroutable /2017-03-31/tags/ path is #1142 exactly", err)
	}

	listed, err := client.ListTags(ctx, &lambda.ListTagsInput{Resource: aws.String(fnARN)})
	if err != nil {
		t.Fatalf("ListTags: %v", err)
	}
	if listed.Tags["Application"] != "lagotto" || listed.Tags["env"] != "journey" {
		t.Errorf("ListTags returned %v, want Application=lagotto and env=journey", listed.Tags)
	}

	if _, err := client.UntagResource(ctx, &lambda.UntagResourceInput{
		Resource: aws.String(fnARN),
		TagKeys:  []string{"env"},
	}); err != nil {
		t.Fatalf("UntagResource: %v", err)
	}

	remaining, err := client.ListTags(ctx, &lambda.ListTagsInput{Resource: aws.String(fnARN)})
	if err != nil {
		t.Fatalf("ListTags after untag: %v", err)
	}
	if len(remaining.Tags) != 1 || remaining.Tags["Application"] != "lagotto" {
		t.Errorf("after UntagResource the function carries %v, want only Application=lagotto",
			remaining.Tags)
	}

	// --- the two families the sweep found on the same wrong date ---

	if _, err := client.PutFunctionEventInvokeConfig(ctx, &lambda.PutFunctionEventInvokeConfigInput{
		FunctionName:             aws.String("journey-poller"),
		MaximumRetryAttempts:     aws.Int32(2),
		MaximumEventAgeInSeconds: aws.Int32(3600),
	}); err != nil {
		t.Errorf("PutFunctionEventInvokeConfig: %v — the SDK serializes it to /2019-09-25", err)
	}

	// InvokeAsync is deprecated and still serialized, to /2014-11-13.
	if _, err := client.InvokeAsync(ctx, &lambda.InvokeAsyncInput{ //nolint:staticcheck // the point is that a deprecated operation still routes.
		FunctionName: aws.String("journey-poller"),
		InvokeArgs:   strings.NewReader(`{"hello":"journey"}`),
	}); err != nil {
		t.Errorf("InvokeAsync: %v — deprecated, still published at /2014-11-13", err)
	}

	// The function's own operations are unaffected, which is the other half of the rule: each date
	// routes its own operations and no others.
	if _, err := client.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String("journey-poller"),
	}); err != nil {
		t.Errorf("GetFunction: %v", err)
	}
}
