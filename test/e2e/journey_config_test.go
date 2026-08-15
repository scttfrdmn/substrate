package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/configservice"
	configtypes "github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_ConfigDetectiveControls is #580 at the SDK level: the detective-controls
// baseline a consumer walks after its stacks deploy.
//
// This tier is the one that matters most for AWS Config, because Config's
// X-Amz-Target prefix is StarlingDoveService — an internal code name bearing no
// resemblance to the config endpoint prefix. Every aws-sdk-go-v2 and boto3 Config call
// takes the target path, so the plugin could be registered, fully unit-tested, and
// unreachable from every SDK. That is exactly what #561, #610 and #636 were: a unit
// test driving a hand-built request never touches the alias.
func TestJourney_ConfigDetectiveControls(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	// Retries off, so each assertion is about the first response rather than whatever
	// the retry loop settled on.
	cs := configservice.NewFromConfig(cfg, func(o *configservice.Options) { o.RetryMaxAttempts = 1 })
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
		o.UsePathStyle = true
	})

	// --- reachability. If this answers InvalidAction, nothing below matters. ---
	recorders, err := cs.DescribeConfigurationRecorders(ctx,
		&configservice.DescribeConfigurationRecordersInput{})
	if err != nil {
		t.Fatalf("DescribeConfigurationRecorders on an empty account: %v — a routing failure surfaces here and nowhere else", err)
	}
	if len(recorders.ConfigurationRecorders) != 0 {
		t.Fatalf("a fresh account reported %d recorders, want none", len(recorders.ConfigurationRecorders))
	}

	// --- the two ordering refusals, in the order the operations document them ---
	// PutDeliveryChannel checks for a recorder before it looks at the bucket, so this
	// refusal is reachable with no S3 fixture at all.
	var noRecorder *configtypes.NoAvailableConfigurationRecorderException
	_, err = cs.PutDeliveryChannel(ctx, &configservice.PutDeliveryChannelInput{
		DeliveryChannel: &configtypes.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("cfg-logs"),
		},
	})
	if !errors.As(err, &noRecorder) {
		t.Fatalf("PutDeliveryChannel with no recorder answered %v, want *NoAvailableConfigurationRecorderException", err)
	}

	// --- the recorder ---
	if _, err := cs.PutConfigurationRecorder(ctx, &configservice.PutConfigurationRecorderInput{
		ConfigurationRecorder: &configtypes.ConfigurationRecorder{
			Name:    aws.String("default"),
			RoleARN: aws.String("arn:aws:iam::123456789012:role/config"),
			RecordingGroup: &configtypes.RecordingGroup{
				AllSupported: true,
			},
		},
	}); err != nil {
		t.Fatalf("PutConfigurationRecorder: %v", err)
	}

	// The headline of the release: a recorder that exists is not a recorder that
	// records. DescribeConfigurationRecorders reports it either way, and the two states
	// are told apart only by the status operation — which is the real misconfiguration
	// an account carries silently.
	recorders, err = cs.DescribeConfigurationRecorders(ctx,
		&configservice.DescribeConfigurationRecordersInput{})
	if err != nil {
		t.Fatalf("DescribeConfigurationRecorders: %v", err)
	}
	if len(recorders.ConfigurationRecorders) != 1 {
		t.Fatalf("got %d recorders, want 1", len(recorders.ConfigurationRecorders))
	}
	// The ARN is the Service Authorization Reference's two-segment form. A one-segment
	// ARN matches no policy written against the real template, and Config now
	// authorizes against its own ARN.
	if arn := aws.ToString(recorders.ConfigurationRecorders[0].Arn); !strings.Contains(arn,
		":configuration-recorder/default/") {
		t.Errorf("the recorder ARN is %q, want configuration-recorder/<name>/<id>", arn)
	}

	status, err := cs.DescribeConfigurationRecorderStatus(ctx,
		&configservice.DescribeConfigurationRecorderStatusInput{})
	if err != nil {
		t.Fatalf("DescribeConfigurationRecorderStatus: %v", err)
	}
	if len(status.ConfigurationRecordersStatus) != 1 {
		t.Fatalf("got %d statuses, want 1", len(status.ConfigurationRecordersStatus))
	}
	if status.ConfigurationRecordersStatus[0].Recording {
		t.Error("a recorder reports recording: true immediately after the Put — it must be false until Start, which is the whole point of #580 behavior #1")
	}

	// Start is refused without a delivery channel: "You must have created a delivery
	// channel to successfully start the configuration recorder."
	var noChannel *configtypes.NoAvailableDeliveryChannelException
	_, err = cs.StartConfigurationRecorder(ctx, &configservice.StartConfigurationRecorderInput{
		ConfigurationRecorderName: aws.String("default"),
	})
	if !errors.As(err, &noChannel) {
		t.Fatalf("StartConfigurationRecorder with no channel answered %v, want *NoAvailableDeliveryChannelException", err)
	}

	// --- the delivery channel, and the S3 checks that read real bucket state ---
	var noBucket *configtypes.NoSuchBucketException
	_, err = cs.PutDeliveryChannel(ctx, &configservice.PutDeliveryChannelInput{
		DeliveryChannel: &configtypes.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("cfg-logs"),
		},
	})
	if !errors.As(err, &noBucket) {
		t.Fatalf("PutDeliveryChannel naming an absent bucket answered %v, want *NoSuchBucketException", err)
	}

	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("cfg-logs"),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// A bucket with no policy at all cannot admit Config, so the refusal is certain
	// rather than a guess about a policy's contents.
	var insufficient *configtypes.InsufficientDeliveryPolicyException
	_, err = cs.PutDeliveryChannel(ctx, &configservice.PutDeliveryChannelInput{
		DeliveryChannel: &configtypes.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("cfg-logs"),
		},
	})
	if !errors.As(err, &insufficient) {
		t.Fatalf("PutDeliveryChannel on a policy-less bucket answered %v, want *InsufficientDeliveryPolicyException", err)
	}

	if _, err := s3c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String("cfg-logs"),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[{` +
			`"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},` +
			`"Action":"s3:PutObject","Resource":"arn:aws:s3:::cfg-logs/*"}]}`),
	}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	if _, err := cs.PutDeliveryChannel(ctx, &configservice.PutDeliveryChannelInput{
		DeliveryChannel: &configtypes.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("cfg-logs"),
			S3KeyPrefix:  aws.String("config"),
		},
	}); err != nil {
		t.Fatalf("PutDeliveryChannel with a Config-admitting policy: %v — the matcher is meant to be permissive, and a wrong refusal breaks working code", err)
	}

	// --- and now the recorder starts ---
	if _, err := cs.StartConfigurationRecorder(ctx,
		&configservice.StartConfigurationRecorderInput{
			ConfigurationRecorderName: aws.String("default"),
		}); err != nil {
		t.Fatalf("StartConfigurationRecorder: %v", err)
	}
	status, err = cs.DescribeConfigurationRecorderStatus(ctx,
		&configservice.DescribeConfigurationRecorderStatusInput{})
	if err != nil {
		t.Fatalf("DescribeConfigurationRecorderStatus after Start: %v", err)
	}
	if !status.ConfigurationRecordersStatus[0].Recording {
		t.Error("the recorder is not recording after Start")
	}

	// --- rules, and compliance that is seeded rather than computed ---
	if _, err := cs.PutConfigRule(ctx, &configservice.PutConfigRuleInput{
		ConfigRule: &configtypes.ConfigRule{
			ConfigRuleName: aws.String("s3-encrypted"),
			Source: &configtypes.Source{
				Owner:            configtypes.OwnerAws,
				SourceIdentifier: aws.String("S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED"),
			},
		},
	}); err != nil {
		t.Fatalf("PutConfigRule: %v", err)
	}

	// An unevaluated rule is INSUFFICIENT_DATA, not a fabricated COMPLIANT. A default
	// of COMPLIANT would make every consumer's compliance assertion pass for free,
	// which is worse than no answer.
	compliance, err := cs.DescribeComplianceByConfigRule(ctx,
		&configservice.DescribeComplianceByConfigRuleInput{
			ConfigRuleNames: []string{"s3-encrypted"},
		})
	if err != nil {
		t.Fatalf("DescribeComplianceByConfigRule: %v", err)
	}
	if len(compliance.ComplianceByConfigRules) != 1 {
		t.Fatalf("got %d compliance results, want 1", len(compliance.ComplianceByConfigRules))
	}
	if got := compliance.ComplianceByConfigRules[0].Compliance.ComplianceType; got !=
		configtypes.ComplianceTypeInsufficientData {
		t.Errorf("an unevaluated rule reports %q, want INSUFFICIENT_DATA", got)
	}

	// The seed is what makes a NON_COMPLIANT branch testable at all: evaluating a rule
	// against resource state is workload-internal, so per CLAUDE.md's scope boundary
	// compliance is a seeded observation.
	seedConfigRuleCompliance(t, ts, "s3-encrypted", `{"complianceType":"NON_COMPLIANT"}`)

	compliance, err = cs.DescribeComplianceByConfigRule(ctx,
		&configservice.DescribeComplianceByConfigRuleInput{
			ConfigRuleNames: []string{"s3-encrypted"},
		})
	if err != nil {
		t.Fatalf("DescribeComplianceByConfigRule after the seed: %v", err)
	}
	if got := compliance.ComplianceByConfigRules[0].Compliance.ComplianceType; got !=
		configtypes.ComplianceTypeNonCompliant {
		t.Errorf("the seeded rule reports %q, want NON_COMPLIANT", got)
	}

	// --- the teardown ordering story, which is what a repeated test run performs ---
	var lastChannel *configtypes.LastDeliveryChannelDeleteFailedException
	_, err = cs.DeleteDeliveryChannel(ctx, &configservice.DeleteDeliveryChannelInput{
		DeliveryChannelName: aws.String("default"),
	})
	if !errors.As(err, &lastChannel) {
		t.Fatalf("DeleteDeliveryChannel while the recorder records answered %v, want *LastDeliveryChannelDeleteFailedException", err)
	}

	if _, err := cs.StopConfigurationRecorder(ctx,
		&configservice.StopConfigurationRecorderInput{
			ConfigurationRecorderName: aws.String("default"),
		}); err != nil {
		t.Fatalf("StopConfigurationRecorder: %v", err)
	}
	if _, err := cs.DeleteDeliveryChannel(ctx, &configservice.DeleteDeliveryChannelInput{
		DeliveryChannelName: aws.String("default"),
	}); err != nil {
		t.Fatalf("DeleteDeliveryChannel after Stop: %v", err)
	}
	if _, err := cs.DeleteConfigurationRecorder(ctx,
		&configservice.DeleteConfigurationRecorderInput{
			ConfigurationRecorderName: aws.String("default"),
		}); err != nil {
		t.Fatalf("DeleteConfigurationRecorder: %v", err)
	}

	// And the rebuild: the account is back to a state a second run of the same fixture
	// can start from, which is what the deletes are in scope for.
	if _, err := cs.PutConfigurationRecorder(ctx, &configservice.PutConfigurationRecorderInput{
		ConfigurationRecorder: &configtypes.ConfigurationRecorder{
			Name:           aws.String("default"),
			RoleARN:        aws.String("arn:aws:iam::123456789012:role/config"),
			RecordingGroup: &configtypes.RecordingGroup{AllSupported: true},
		},
	}); err != nil {
		t.Fatalf("re-creating the recorder after the teardown: %v — a fixture must be re-runnable", err)
	}
}

// TestJourney_ConfigInvalidRole covers the refusal a consumer's own template most
// often trips, through the SDK's error type.
//
// The API model does not mark roleARN required; the reference states plainly that
// "the server will reject a request without a defined roleARN", and the exception is a
// null-or-empty check rather than an assumability check. Verifying assumability would
// refuse requests AWS accepts, which is the worse failure.
func TestJourney_ConfigInvalidRole(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	cs := configservice.NewFromConfig(cfg, func(o *configservice.Options) { o.RetryMaxAttempts = 1 })

	var invalidRole *configtypes.InvalidRoleException
	_, err = cs.PutConfigurationRecorder(ctx, &configservice.PutConfigurationRecorderInput{
		ConfigurationRecorder: &configtypes.ConfigurationRecorder{
			Name:    aws.String("default"),
			RoleARN: aws.String(""),
		},
	})
	if !errors.As(err, &invalidRole) {
		t.Fatalf("PutConfigurationRecorder with an empty roleARN answered %v, want *InvalidRoleException", err)
	}

	// A rule cannot be put without a recorder either, and the refusal names the
	// operation that fixes it.
	var noRecorder *configtypes.NoAvailableConfigurationRecorderException
	_, err = cs.PutConfigRule(ctx, &configservice.PutConfigRuleInput{
		ConfigRule: &configtypes.ConfigRule{
			ConfigRuleName: aws.String("s3-encrypted"),
			Source: &configtypes.Source{
				Owner:            configtypes.OwnerAws,
				SourceIdentifier: aws.String("S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED"),
			},
		},
	})
	if !errors.As(err, &noRecorder) {
		t.Fatalf("PutConfigRule with no recorder answered %v, want *NoAvailableConfigurationRecorderException", err)
	}
}

// seedConfigRuleCompliance posts a rule-compliance seed to the control plane.
func seedConfigRuleCompliance(t *testing.T, ts *emulator.TestServer, rule, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/v1/config/rule-compliance/"+rule, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build seed request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed %s: %v", body, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed %s: %d", body, resp.StatusCode)
	}
}
