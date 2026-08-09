package e2e_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_DescribeStackEvents is #501's and #562's gate at the SDK level.
//
// DescribeStackEvents returned UnsupportedOperation (400) until #501, so a
// consumer's deployment wrapper — which polls events, because that is where a
// CloudFormation failure is conventionally read from — could not run against
// substrate at all. It is a journey rather than a plugin test because the SDK is
// what turns the XML into a []types.StackEvent: an element name substrate got wrong
// would deserialize to a zero value here while a hand-written XPath assertion in
// emulator/ still passed. That is the failure mode #591 shipped for EC2 errors.
func TestJourney_DescribeStackEvents(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off: a retried call would obscure which attempt produced a result.
	cfn := cloudformation.NewFromConfig(cfg, func(o *cloudformation.Options) { o.RetryMaxAttempts = 1 })

	const template = `{"Resources":{
		"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"journey-events-bucket"}}}}`

	if _, err := cfn.CreateStack(ctx, &cloudformation.CreateStackInput{
		StackName:    aws.String("journey-events"),
		TemplateBody: aws.String(template),
	}); err != nil {
		t.Fatalf("CreateStack: %v", err)
	}

	out, err := cfn.DescribeStackEvents(ctx, &cloudformation.DescribeStackEventsInput{
		StackName: aws.String("journey-events"),
	})
	if err != nil {
		t.Fatalf("DescribeStackEvents: %v", err)
	}
	// The stack's terminal event, the resource, then the stack's opening event.
	// Newest first, as the API documents.
	if len(out.StackEvents) != 3 {
		t.Fatalf("StackEvents: got %d events, want 3: %+v", len(out.StackEvents), out.StackEvents)
	}
	wantStatuses := []cfntypes.ResourceStatus{"CREATE_COMPLETE", "CREATE_COMPLETE", "CREATE_IN_PROGRESS"}
	for i, want := range wantStatuses {
		if got := out.StackEvents[i].ResourceStatus; got != want {
			t.Errorf("event %d: ResourceStatus = %q, want %q", i, got, want)
		}
	}
	// Every required member deserialized. A missing one would be the zero value
	// here and invisible to an XPath assertion over the raw body.
	for i, e := range out.StackEvents {
		if aws.ToString(e.EventId) == "" {
			t.Errorf("event %d: EventId is empty", i)
		}
		if aws.ToString(e.StackName) != "journey-events" {
			t.Errorf("event %d: StackName = %q", i, aws.ToString(e.StackName))
		}
		if aws.ToString(e.StackId) == "" {
			t.Errorf("event %d: StackId is empty", i)
		}
		if e.Timestamp == nil {
			t.Errorf("event %d: Timestamp is nil", i)
		}
	}
	if got := aws.ToString(out.StackEvents[1].LogicalResourceId); got != "Bucket" {
		t.Errorf("the resource event's LogicalResourceId = %q, want Bucket", got)
	}
	if got := aws.ToString(out.StackEvents[1].ResourceType); got != "AWS::S3::Bucket" {
		t.Errorf("the resource event's ResourceType = %q", got)
	}
	if got := aws.ToString(out.StackEvents[0].ResourceType); got != "AWS::CloudFormation::Stack" {
		t.Errorf("the stack event's ResourceType = %q", got)
	}
	if out.NextToken != nil {
		t.Errorf("NextToken = %q, want none for a three-event stack", aws.ToString(out.NextToken))
	}
}

// TestJourney_StackEventsNameARoleDenial is #562's fifth acceptance criterion
// through the SDK: "a stack whose role is missing one permission reaches
// ROLLBACK_COMPLETE with a StackEvent reason naming the denial".
//
// The criterion blocked #562 across four releases because DescribeStackEvents was
// refused. The role is built through the IAM SDK rather than seeded into state, so
// the whole chain a consumer writes is exercised: create a role, give it too little,
// deploy under it, then read why from events.
func TestJourney_StackEventsNameARoleDenial(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	iamClient := iam.NewFromConfig(cfg)
	cfn := cloudformation.NewFromConfig(cfg, func(o *cloudformation.Options) { o.RetryMaxAttempts = 1 })

	const roleName = "cfn-narrow"
	if _, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName: aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"Service":"cloudformation.amazonaws.com"},"Action":"sts:AssumeRole"}]}`),
	}); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	// s3:ListBucket and not s3:CreateBucket: one missing permission, which is the
	// criterion's shape — a role that is plausible rather than empty.
	if _, err := iamClient.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:   aws.String(roleName),
		PolicyName: aws.String("too-narrow"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`),
	}); err != nil {
		t.Fatalf("PutRolePolicy: %v", err)
	}

	// A rolled-back stack is still a successful CreateStack: the API returns the
	// StackId before any rollback happens, and the outcome is the stack's status.
	if _, err := cfn.CreateStack(ctx, &cloudformation.CreateStackInput{
		StackName: aws.String("journey-denied"),
		RoleARN:   aws.String("arn:aws:iam::" + journeyAccountID + ":role/" + roleName),
		TemplateBody: aws.String(`{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket",` +
			`"Properties":{"BucketName":"journey-denied-bucket"}}}}`),
	}); err != nil {
		t.Fatalf("CreateStack: %v", err)
	}

	stacks, err := cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{
		StackName: aws.String("journey-denied"),
	})
	if err != nil {
		t.Fatalf("DescribeStacks: %v", err)
	}
	if len(stacks.Stacks) != 1 {
		t.Fatalf("DescribeStacks returned %d stacks", len(stacks.Stacks))
	}
	if got := stacks.Stacks[0].StackStatus; got != cfntypes.StackStatusRollbackComplete {
		t.Fatalf("StackStatus = %q, want ROLLBACK_COMPLETE", got)
	}

	out, err := cfn.DescribeStackEvents(ctx, &cloudformation.DescribeStackEventsInput{
		StackName: aws.String("journey-denied"),
	})
	if err != nil {
		t.Fatalf("DescribeStackEvents: %v", err)
	}
	var denial string
	for _, e := range out.StackEvents {
		if e.ResourceStatus == cfntypes.ResourceStatusCreateFailed {
			denial = aws.ToString(e.ResourceStatusReason)
		}
	}
	if denial == "" {
		t.Fatalf("no CREATE_FAILED event carrying a reason: %+v", out.StackEvents)
	}
	// Naming the action is what makes the event actionable: a consumer learns which
	// permission to add, rather than only that something failed.
	for _, want := range []string{"AccessDenied", "s3:CreateBucket"} {
		if !strings.Contains(denial, want) {
			t.Errorf("ResourceStatusReason = %q, want it to name %q", denial, want)
		}
	}
}
