package e2e_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_TerraformRefreshReads drives, through the generated SDK clients, the reads Terraform's
// aws_dynamodb_table and aws_sns_topic make on every refresh (#1608). Each failed `terraform apply`:
// DescribeContinuousBackups was an unknown operation, and GetTopicAttributes reported no Policy for
// the provider to parse. The SDK tier proves the shapes deserialize — PITR's timestamps are epoch
// numbers on the wire, which only a generated deserializer checks.
func TestJourney_TerraformRefreshReads(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()

	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.RetryMaxAttempts = 1 })
	created, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("tf-refresh"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if aws.ToString(created.TableDescription.TableId) == "" {
		t.Error("CreateTable reported no TableId")
	}
	// The provider's waitTableWarmThroughputActive polls for this status after every create.
	if wt := created.TableDescription.WarmThroughput; wt == nil || wt.Status != ddbtypes.TableStatusActive {
		t.Errorf("CreateTable reported no ACTIVE WarmThroughput: %+v", wt)
	}

	// With server_side_encryption { kms_key_arn }, the provider's SSE waiter polls SSEDescription.
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
	encrypted, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("tf-refresh-sse"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		SSESpecification: &ddbtypes.SSESpecification{
			Enabled: aws.Bool(true), SSEType: ddbtypes.SSETypeKms, KMSMasterKeyId: aws.String(keyARN),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable with SSE: %v", err)
	}
	if sse := encrypted.TableDescription.SSEDescription; sse == nil || sse.Status != ddbtypes.SSEStatusEnabled ||
		aws.ToString(sse.KMSMasterKeyArn) != keyARN {
		t.Errorf("CreateTable with SSE reported %+v", sse)
	}

	described, err := ddb.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("tf-refresh")})
	if err != nil {
		t.Fatalf("DescribeContinuousBackups: %v", err)
	}
	if got := described.ContinuousBackupsDescription.ContinuousBackupsStatus; got != ddbtypes.ContinuousBackupsStatusEnabled {
		t.Errorf("ContinuousBackupsStatus = %q, want ENABLED", got)
	}
	if got := described.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus; got != ddbtypes.PointInTimeRecoveryStatusDisabled {
		t.Errorf("PointInTimeRecoveryStatus = %q, want DISABLED", got)
	}

	updated, err := ddb.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String("tf-refresh"),
		PointInTimeRecoverySpecification: &ddbtypes.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)},
	})
	if err != nil {
		t.Fatalf("UpdateContinuousBackups: %v", err)
	}
	pitr := updated.ContinuousBackupsDescription.PointInTimeRecoveryDescription
	if pitr.PointInTimeRecoveryStatus != ddbtypes.PointInTimeRecoveryStatusEnabled {
		t.Errorf("PointInTimeRecoveryStatus = %q, want ENABLED", pitr.PointInTimeRecoveryStatus)
	}
	if pitr.EarliestRestorableDateTime == nil || pitr.LatestRestorableDateTime == nil {
		t.Fatalf("an enabled table reports its restorable window: %+v", pitr)
	}
	if pitr.LatestRestorableDateTime.Before(*pitr.EarliestRestorableDateTime) {
		t.Errorf("LatestRestorableDateTime %v is before EarliestRestorableDateTime %v",
			pitr.LatestRestorableDateTime, pitr.EarliestRestorableDateTime)
	}
	if now := ts.TimeController().Now(); pitr.EarliestRestorableDateTime.After(now.Add(time.Second)) {
		t.Errorf("EarliestRestorableDateTime %v is after the simulated clock %v", pitr.EarliestRestorableDateTime, now)
	}
	if aws.ToInt32(pitr.RecoveryPeriodInDays) != 35 {
		t.Errorf("RecoveryPeriodInDays = %d, want the default 35", aws.ToInt32(pitr.RecoveryPeriodInDays))
	}

	topics := sns.NewFromConfig(cfg, func(o *sns.Options) { o.RetryMaxAttempts = 1 })
	topic, err := topics.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("tf-refresh")})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	attrs, err := topics.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: topic.TopicArn})
	if err != nil {
		t.Fatalf("GetTopicAttributes: %v", err)
	}
	var policy struct {
		Statement []struct{ Resource string }
	}
	if err := json.Unmarshal([]byte(attrs.Attributes["Policy"]), &policy); err != nil {
		t.Fatalf("the default Policy is not a JSON document: %v (%q)", err, attrs.Attributes["Policy"])
	}
	if len(policy.Statement) != 1 || policy.Statement[0].Resource != aws.ToString(topic.TopicArn) {
		t.Errorf("the default Policy names this topic: %+v", policy)
	}
}
