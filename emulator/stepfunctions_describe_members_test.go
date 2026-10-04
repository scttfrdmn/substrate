package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Step Functions' share of #1199: DescribeStateMachine answered 7 of its 14 published members and
// DescribeActivity 3 of 4. The assertions are on the raw document, because a typed decode cannot tell
// an absent configuration from a zero-valued one.

const sfnDescribeDefinition = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`

// sfnDescribeHarness drives the Step Functions plugin directly on the frozen wire-test clock.
type sfnDescribeHarness struct {
	t   *testing.T
	p   *emulator.StepFunctionsPlugin
	ctx *emulator.RequestContext
}

func newSFNDescribeHarness(t *testing.T) *sfnDescribeHarness {
	t.Helper()
	h := &sfnDescribeHarness{t: t, p: &emulator.StepFunctionsPlugin{}}
	h.ctx, _ = wireSetup(t, h.p, "req-sfn-describe")
	return h
}

func (h *sfnDescribeHarness) ok(op string, body map[string]any) map[string]json.RawMessage {
	h.t.Helper()
	raw := wireJSONTarget(h.t, h.p, h.ctx, "states", "AWSStepFunctions", op, body)
	var doc map[string]json.RawMessage
	require.NoError(h.t, json.Unmarshal(raw, &doc), "%s: %s", op, raw)
	return doc
}

// refused issues op and returns the published refusal it answers.
func (h *sfnDescribeHarness) refused(op string, body map[string]any) *emulator.AWSError {
	h.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(h.t, err)
	_, err = h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "states", Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "AWSStepFunctions." + op}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	require.Truef(h.t, errors.As(err, &awsErr), "%s must be refused, got %v", op, err)
	return awsErr
}

func (h *sfnDescribeHarness) createMachine(name string, extra map[string]any) string {
	h.t.Helper()
	body := map[string]any{"name": name, "definition": sfnDescribeDefinition, "roleArn": "arn:aws:iam::123456789012:role/sfn"}
	for k, v := range extra {
		body[k] = v
	}
	var arn string
	require.NoError(h.t, json.Unmarshal(h.ok("CreateStateMachine", body)["stateMachineArn"], &arn))
	return arn
}

func TestSFNDescribe_AStateMachineWithNoConfigurationAnswersThePublishedDefaults(t *testing.T) {
	t.Parallel()
	h := newSFNDescribeHarness(t)
	arn := h.createMachine("defaults", nil)
	doc := h.ok("DescribeStateMachine", map[string]any{"stateMachineArn": arn})

	require.JSONEq(t, `{"includeExecutionData":false,"level":"OFF"}`, string(doc["loggingConfiguration"]),
		"API_CreateStateMachine: by default, the level is set to OFF")
	require.JSONEq(t, `{"enabled":false}`, string(doc["tracingConfiguration"]))
	require.JSONEq(t, `{"type":"AWS_OWNED_KEY"}`, string(doc["encryptionConfiguration"]))
	var revision string
	require.NoError(t, json.Unmarshal(doc["revisionId"], &revision), "revisionId must be a string: %s", doc["revisionId"])
	require.NotEmpty(t, revision)
	for _, absent := range []string{"description", "label", "variableReferences"} {
		_, present := doc[absent]
		require.Falsef(t, present, "%s has no value for an unversioned, unqualified state machine", absent)
	}
}

func TestSFNDescribe_ConfigurationsRoundTripAndRevisionIdMovesOnUpdate(t *testing.T) {
	t.Parallel()
	h := newSFNDescribeHarness(t)
	logging := map[string]any{"level": "ERROR", "includeExecutionData": true,
		"destinations": []any{map[string]any{"cloudWatchLogsLogGroup": map[string]any{"logGroupArn": "arn:aws:logs:us-east-1:123456789012:log-group:sfn:*"}}}}
	encryption := map[string]any{"type": "CUSTOMER_MANAGED_KMS_KEY", "kmsKeyId": "alias/sfn", "kmsDataKeyReusePeriodSeconds": 300}
	arn := h.createMachine("configured", map[string]any{
		"loggingConfiguration": logging, "tracingConfiguration": map[string]any{"enabled": true}, "encryptionConfiguration": encryption,
	})

	first := h.ok("DescribeStateMachine", map[string]any{"stateMachineArn": arn})
	wantLogging, _ := json.Marshal(logging)
	wantEncryption, _ := json.Marshal(encryption)
	require.JSONEq(t, string(wantLogging), string(first["loggingConfiguration"]))
	require.JSONEq(t, `{"enabled":true}`, string(first["tracingConfiguration"]))
	require.JSONEq(t, string(wantEncryption), string(first["encryptionConfiguration"]))

	// A second describe with no update in between answers the same revisionId: it identifies a
	// configuration, not a request.
	again := h.ok("DescribeStateMachine", map[string]any{"stateMachineArn": arn})
	require.Equal(t, string(first["revisionId"]), string(again["revisionId"]))

	// An update moves it, answers the new one in its own response, and replaces only what it names.
	updated := h.ok("UpdateStateMachine", map[string]any{"stateMachineArn": arn, "tracingConfiguration": map[string]any{"enabled": false}})
	after := h.ok("DescribeStateMachine", map[string]any{"stateMachineArn": arn})
	require.NotEqual(t, string(first["revisionId"]), string(after["revisionId"]), "an update is a new revision")
	require.Equal(t, string(updated["revisionId"]), string(after["revisionId"]), "UpdateStateMachine answers the revisionId it made")
	require.JSONEq(t, `{"enabled":false}`, string(after["tracingConfiguration"]))
	require.JSONEq(t, string(wantLogging), string(after["loggingConfiguration"]), "a configuration the update omits is unchanged")
}

func TestSFNDescribe_CreateIdempotencyComparesTheConfigurations(t *testing.T) {
	t.Parallel()
	h := newSFNDescribeHarness(t)
	logging := map[string]any{"level": "ALL", "includeExecutionData": false}
	arn := h.createMachine("idem", map[string]any{"loggingConfiguration": logging})

	// The same configuration, keys reordered, is the idempotent repeat the page publishes.
	again := h.createMachine("idem", map[string]any{"loggingConfiguration": map[string]any{"includeExecutionData": false, "level": "ALL"}})
	require.Equal(t, arn, again)

	// A different one is a conflict.
	awsErr := h.refused("CreateStateMachine", map[string]any{
		"name": "idem", "definition": sfnDescribeDefinition, "roleArn": "arn:aws:iam::123456789012:role/sfn",
		"loggingConfiguration": map[string]any{"level": "OFF"},
	})
	require.Equal(t, "StateMachineAlreadyExists", awsErr.Code)
	require.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
}

func TestSFNDescribe_AConfigurationThatIsNotAnObjectIsRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		member, code string
	}{
		{"loggingConfiguration", "InvalidLoggingConfiguration"},
		{"tracingConfiguration", "InvalidTracingConfiguration"},
		{"encryptionConfiguration", "InvalidEncryptionConfiguration"},
	} {
		t.Run(tc.member, func(t *testing.T) {
			t.Parallel()
			h := newSFNDescribeHarness(t)
			awsErr := h.refused("CreateStateMachine", map[string]any{
				"name": "bad", "definition": sfnDescribeDefinition, "roleArn": "arn:aws:iam::123456789012:role/sfn",
				tc.member: "OFF",
			})
			require.Equal(t, tc.code, awsErr.Code)
			require.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
		})
	}
}

func TestSFNDescribe_DescribeActivityAnswersItsEncryptionConfiguration(t *testing.T) {
	t.Parallel()
	h := newSFNDescribeHarness(t)
	var plain, keyed string
	require.NoError(t, json.Unmarshal(h.ok("CreateActivity", map[string]any{"name": "plain"})["activityArn"], &plain))
	encryption := map[string]any{"type": "CUSTOMER_MANAGED_KMS_KEY", "kmsKeyId": "alias/act"}
	require.NoError(t, json.Unmarshal(h.ok("CreateActivity", map[string]any{"name": "keyed", "encryptionConfiguration": encryption})["activityArn"], &keyed))

	require.JSONEq(t, `{"type":"AWS_OWNED_KEY"}`, string(h.ok("DescribeActivity", map[string]any{"activityArn": plain})["encryptionConfiguration"]))
	want, _ := json.Marshal(encryption)
	require.JSONEq(t, string(want), string(h.ok("DescribeActivity", map[string]any{"activityArn": keyed})["encryptionConfiguration"]))

	// A repeat with the same configuration is idempotent; one that changes it is the published
	// ActivityAlreadyExists, which was unreachable until the member was recorded.
	h.ok("CreateActivity", map[string]any{"name": "keyed", "encryptionConfiguration": encryption})
	awsErr := h.refused("CreateActivity", map[string]any{"name": "keyed"})
	require.Equal(t, "ActivityAlreadyExists", awsErr.Code)
	require.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
}
