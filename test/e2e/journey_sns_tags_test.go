package e2e_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_SNSTags is #1141 at the SDK level: the query protocol's result element, which no
// string-matching test can miss and no generated deserializer will forgive.
//
// Substrate's own SNS tests post a form-encoded body and read the XML back as a string, so every one
// of them stayed green while `TagResource` answered 200 with the tag written and the
// `<TagResourceResult>` element absent. The real client fails the whole operation:
//
//	operation error SNS: TagResource, https response error StatusCode: 200, RequestID: ,
//	deserialization failed, failed to decode response body, TagResourceResult node not found
//
// That is the worst shape a divergence can take — the write had already happened, so state was right
// and only the envelope was wrong, while every caller that checks its error treated a successful tag
// as a failure.
//
// The two halves of the journey prove different things, and only the first is exclusive to this tier:
//
//   - `TagResource`/`UntagResource` cannot be called at all through the SDK unless the element is
//     present, so these four calls are the regression test for the defect itself;
//   - the six `smithy.api#Unit` operations are driven through the SDK as well, which proves the
//     envelope change broke none of them. It does not prove the element must be *absent* there — a
//     deserializer that does not look for an element does not mind one — so that half is pinned in
//     `emulator/sns_result_envelope_test.go`, which asserts the response root's children exactly.
func TestJourney_SNSTags(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so every assertion is about the first response rather than whatever the retry loop
	// settled on.
	client := sns.NewFromConfig(cfg, func(o *sns.Options) { o.RetryMaxAttempts = 1 })

	topic, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("journey-tags")})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	topicARN := aws.ToString(topic.TopicArn)
	if topicARN == "" {
		t.Fatal("CreateTopic returned no TopicArn")
	}

	// --- the defect: an empty result element that a generated deserializer requires ---

	if _, err := client.TagResource(ctx, &sns.TagResourceInput{
		ResourceArn: aws.String(topicARN),
		Tags: []snstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("journey")},
			{Key: aws.String("team"), Value: aws.String("substrate")},
		},
	}); err != nil {
		t.Fatalf("TagResource: %v — a missing <TagResourceResult> is #1141 exactly", err)
	}

	tags, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: aws.String(topicARN),
	})
	if err != nil {
		t.Fatalf("ListTagsForResource: %v", err)
	}
	got := map[string]string{}
	for _, tag := range tags.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if len(got) != 2 || got["env"] != "journey" || got["team"] != "substrate" {
		t.Errorf("ListTagsForResource returned %v, want env=journey and team=substrate", got)
	}

	if _, err := client.UntagResource(ctx, &sns.UntagResourceInput{
		ResourceArn: aws.String(topicARN),
		TagKeys:     []string{"team"},
	}); err != nil {
		t.Fatalf("UntagResource: %v — a missing <UntagResourceResult> is the other half of #1141", err)
	}

	remaining, err := client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: aws.String(topicARN),
	})
	if err != nil {
		t.Fatalf("ListTagsForResource after untag: %v", err)
	}
	if len(remaining.Tags) != 1 || aws.ToString(remaining.Tags[0].Key) != "env" {
		t.Errorf("after UntagResource the topic carries %d tags, want only env", len(remaining.Tags))
	}

	// --- the six Unit operations, which must keep answering with no result element ---

	if _, err := client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
		TopicArn:       aws.String(topicARN),
		AttributeName:  aws.String("DisplayName"),
		AttributeValue: aws.String("journey"),
	}); err != nil {
		t.Errorf("SetTopicAttributes: %v", err)
	}

	if _, err := client.AddPermission(ctx, &sns.AddPermissionInput{
		TopicArn:     aws.String(topicARN),
		Label:        aws.String("journey"),
		AWSAccountId: []string{journeyAccountID},
		ActionName:   []string{"Publish"},
	}); err != nil {
		t.Errorf("AddPermission: %v", err)
	}
	if _, err := client.RemovePermission(ctx, &sns.RemovePermissionInput{
		TopicArn: aws.String(topicARN),
		Label:    aws.String("journey"),
	}); err != nil {
		t.Errorf("RemovePermission: %v", err)
	}

	sub, err := client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("https://sqs.us-east-1.amazonaws.com/" + journeyAccountID + "/journey-queue"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := client.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	}); err != nil {
		t.Errorf("SetSubscriptionAttributes: %v", err)
	}
	if _, err := client.Unsubscribe(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: sub.SubscriptionArn,
	}); err != nil {
		t.Errorf("Unsubscribe: %v", err)
	}

	if _, err := client.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(topicARN)}); err != nil {
		t.Errorf("DeleteTopic: %v", err)
	}
}
