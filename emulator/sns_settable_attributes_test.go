package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// SetTopicAttributes wrote req.Params["AttributeName"] into the topic record with no check of any kind,
// so every name was settable: the eight API_GetTopicAttributes publishes as facts about the topic, and
// every name neither page publishes at all (#1067).
//
// The pair of assertions below is what makes that visible. Accepting all 25 published names is half of
// it — a guard that refuses a name AWS settles is worse than no guard — and refusing the eight read-only
// ones plus an invented one is the other. A test that only checked the refusals would pass against a
// handler that refused everything.
//
// snsSettableNames restates the page's list rather than reading the plugin's, deliberately: the value of
// the assertion is that it is an independent transcription of API_SetTopicAttributes, so a name dropped
// from or misspelled in snsSettableTopicAttributeNames fails here instead of agreeing with itself.

// snsSettableNames is the twenty-five AttributeName values API_SetTopicAttributes publishes, in the
// page's own order: five general, fifteen delivery-status names across five endpoint families, two under
// server-side encryption, three under FIFO topics.
var snsSettableNames = []string{ //nolint:gochecknoglobals // a transcription of one AWS page, read by two tests
	"DeliveryPolicy",
	"DisplayName",
	"MaximumMessageSize",
	"Policy",
	"TracingConfig",

	"HTTPSuccessFeedbackRoleArn",
	"HTTPSuccessFeedbackSampleRate",
	"HTTPFailureFeedbackRoleArn",

	"FirehoseSuccessFeedbackRoleArn",
	"FirehoseSuccessFeedbackSampleRate",
	"FirehoseFailureFeedbackRoleArn",

	"LambdaSuccessFeedbackRoleArn",
	"LambdaSuccessFeedbackSampleRate",
	"LambdaFailureFeedbackRoleArn",

	"ApplicationSuccessFeedbackRoleArn",
	"ApplicationSuccessFeedbackSampleRate",
	"ApplicationFailureFeedbackRoleArn",

	"SQSSuccessFeedbackRoleArn",
	"SQSSuccessFeedbackSampleRate",
	"SQSFailureFeedbackRoleArn",

	"KmsMasterKeyId",
	"SignatureVersion",

	"ArchivePolicy",
	"ContentBasedDeduplication",
	"FifoThroughputScope",
}

// snsReadOnlyAttributeNames are the eight names API_GetTopicAttributes publishes and
// API_SetTopicAttributes does not — Get's seventeen less the nine the two pages share.
//
// They are the sharp end of #1067, because each is a fact about the topic: the ARN it is keyed by, the
// account that owns it, three subscription counts, the delivery policy SNS computes from the one a
// caller set, the archive's start time, and whether the topic is FIFO. Before the allowlist a caller
// could write any of them, and getTopicAttributes' merge order was the only thing that stopped four of
// the eight being reported back.
var snsReadOnlyAttributeNames = []string{ //nolint:gochecknoglobals // a transcription of one AWS page, read by two tests
	"BeginningArchiveTime",
	"EffectiveDeliveryPolicy",
	"FifoTopic",
	"Owner",
	"SubscriptionsConfirmed",
	"SubscriptionsDeleted",
	"SubscriptionsPending",
	"TopicArn",
}

// snsSetTopicAttribute sends one SetTopicAttributes call and returns the status and the error code.
func snsSetTopicAttribute(t *testing.T, ts *emulator.TestServer, region, arn, name, value string) (int, string) {
	t.Helper()
	status, _, errCode := snsQuery(t, ts, region, map[string]string{
		"Action":         "SetTopicAttributes",
		"TopicArn":       arn,
		"AttributeName":  name,
		"AttributeValue": value,
	})
	return status, errCode
}

// TestSNSSetTopicAttributesAcceptsEveryPublishedName walks all twenty-five settable names.
//
// The value sent is the name itself, which is not a valid value for most of them — a SampleRate is a
// percentage and MaximumMessageSize is a byte count — and that is deliberate. #1067 is about the
// *name*, and substrate validates no attribute value anywhere, so sending a plausible value per name
// would hide a future value check behind a passing test rather than expose it.
func TestSNSSetTopicAttributesAcceptsEveryPublishedName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "settable-accepted")

	require.Len(t, snsSettableNames, 25, "API_SetTopicAttributes publishes twenty-five settable names")

	for _, name := range snsSettableNames {
		status, errCode := snsSetTopicAttribute(t, ts, snsEastRegion, arn, name, name)
		assert.Equal(t, http.StatusOK, status, "SetTopicAttributes %s", name)
		assert.Empty(t, errCode, "SetTopicAttributes %s is published as settable", name)
	}

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	for _, name := range snsSettableNames {
		assert.Equal(t, name, attrs[name], "a settable %s is stored, not merely accepted", name)
	}
}

// TestSNSSetTopicAttributesRefusesTheReadOnlyNames asserts each of the eight Get-only names is refused,
// and that the refusal is a refusal to *write*: the attribute set afterwards is the one a fresh topic
// has.
//
// The code is InvalidParameter/400. API_SetTopicAttributes publishes it with the gloss "Indicates that a
// request parameter does not comply with the associated constraints", and AttributeName's constraint is
// the published list — but the page's only prose about the code is about an oversized
// MaximumMessageSize, so reading it onto an unpublished name is substrate's reading rather than a
// sentence AWS wrote. Asserting the code rather than just the status is what pins that reading: a 400
// carrying ValidationError would satisfy a status-only assertion and would be a different answer.
func TestSNSSetTopicAttributesRefusesTheReadOnlyNames(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "settable-read-only")
	snsSubscribe(t, ts, snsEastRegion, arn)

	require.Len(t, snsReadOnlyAttributeNames, 8,
		"Get publishes seventeen names and shares nine with Set, so eight are read-only")

	for _, name := range snsReadOnlyAttributeNames {
		status, errCode := snsSetTopicAttribute(t, ts, snsEastRegion, arn, name, "99")
		assert.Equal(t, http.StatusBadRequest, status, "SetTopicAttributes %s", name)
		assert.Equal(t, "InvalidParameter", errCode,
			"%s is published on GetTopicAttributes and nowhere on SetTopicAttributes", name)
	}

	keys := snsAttributeEntryKeys(t, snsGetTopicAttributesBody(t, ts, snsEastRegion, arn))
	assert.Equal(t, []string{"Owner", "SubscriptionsConfirmed", "SubscriptionsPending", "TopicArn"}, keys,
		"eight refused writes store nothing: the topic still reports only its derived members")

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, arn, attrs["TopicArn"], "the refused TopicArn did not reach the record")
	assert.Equal(t, "1", attrs["SubscriptionsConfirmed"],
		"the refused count did not reach the record either, and the index still answers")
}

// TestSNSSetTopicAttributesRefusesAnUnpublishedName is the case an allowlist exists for: a name neither
// page publishes, which no subtraction of Get's list from Set's could have caught.
func TestSNSSetTopicAttributesRefusesAnUnpublishedName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "settable-unpublished")

	for _, name := range []string{
		"Banana",
		// The lowercase spelling of a published name. AWS's list is a list of literal names and nothing
		// in this plugin normalises a query parameter, so case is part of the name.
		"displayname",
		// A name from another service's attribute vocabulary, which is the realistic version of the
		// mistake: SQS publishes VisibilityTimeout and SNS does not.
		"VisibilityTimeout",
		// The plural of a published one, which is how a hand-written caller gets it wrong.
		"DeliveryPolicies",
	} {
		status, errCode := snsSetTopicAttribute(t, ts, snsEastRegion, arn, name, "x")
		assert.Equal(t, http.StatusBadRequest, status, "SetTopicAttributes %s", name)
		assert.Equal(t, "InvalidParameter", errCode, "%s appears on neither SNS page", name)
	}

	keys := snsAttributeEntryKeys(t, snsGetTopicAttributesBody(t, ts, snsEastRegion, arn))
	assert.Equal(t, []string{"Owner", "SubscriptionsConfirmed", "SubscriptionsPending", "TopicArn"}, keys,
		"an unpublished name is not stored, so GetTopicAttributes cannot report it back")
}

// TestSNSSetTopicAttributesRequiresAnAttributeName covers the member AWS marks Required: Yes.
//
// Before #1067 an absent AttributeName stored the empty string as a key, so GetTopicAttributes answered
// an <entry> with an empty <key> — a document no SDK models.
func TestSNSSetTopicAttributesRequiresAnAttributeName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "settable-absent-name")

	status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":         "SetTopicAttributes",
		"TopicArn":       arn,
		"AttributeValue": "orphaned",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameter", errCode, "AttributeName is Required: Yes")

	keys := snsAttributeEntryKeys(t, snsGetTopicAttributesBody(t, ts, snsEastRegion, arn))
	assert.NotContains(t, keys, "", "an absent AttributeName no longer stores an empty key")
	assert.Equal(t, []string{"Owner", "SubscriptionsConfirmed", "SubscriptionsPending", "TopicArn"}, keys)
}

// TestSNSSetTopicAttributesAcceptsAnEmptyValue pins the half of the signature that is *not* required.
//
// AttributeValue is Required: No, and substrate's own CloudFormation deleter depends on it: there is no
// DeleteTopicPolicy, so AWS::SNS::TopicPolicy is removed by setting Policy to the empty string
// (cfn_delete.go:87). A guard that rejected an empty value would have broken stack deletion, which is
// why the name check is the only one added.
func TestSNSSetTopicAttributesAcceptsAnEmptyValue(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "settable-empty-value")

	const policy = `{"Version":"2008-10-17","Statement":[]}`
	status, errCode := snsSetTopicAttribute(t, ts, snsEastRegion, arn, "Policy", policy)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, errCode)

	status, errCode = snsSetTopicAttribute(t, ts, snsEastRegion, arn, "Policy", "")
	assert.Equal(t, http.StatusOK, status, "clearing a policy is a set to the empty string")
	assert.Empty(t, errCode)

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, "", attrs["Policy"], "the cleared policy is stored as empty, not left at its old value")
}

// TestSNSSetTopicAttributesWriteOnlyNamesAreReadBack records a divergence rather than a fix.
//
// Sixteen of the 25 settable names are absent from API_GetTopicAttributes' seventeen: the fifteen
// delivery-status names and FifoThroughputScope. AWS accepts them and does not report them back.
// Substrate stores them and getTopicAttributes reports the whole stored map, so it does report them —
// and this test asserts that, so the divergence is stated in the tree rather than discovered by a
// consumer.
//
// Recording it is the deliberate choice over filtering the read. The value a caller set is real, and a
// GetTopicAttributes that hid it would make SetTopicAttributes look like a no-op — the failure mode
// #1067 is about, in the opposite direction. A consumer asserting on Get's published key set must not
// use substrate's answer as the authority here; docs/services.md says so.
func TestSNSSetTopicAttributesWriteOnlyNamesAreReadBack(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "settable-write-only")

	for name, value := range map[string]string{
		"LambdaSuccessFeedbackRoleArn": "arn:aws:iam::123456789012:role/sns-logs",
		"SQSSuccessFeedbackSampleRate": "100",
		"FifoThroughputScope":          "MessageGroup",
	} {
		status, errCode := snsSetTopicAttribute(t, ts, snsEastRegion, arn, name, value)
		require.Equal(t, http.StatusOK, status, "SetTopicAttributes %s", name)
		require.Empty(t, errCode)
	}

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, "arn:aws:iam::123456789012:role/sns-logs", attrs["LambdaSuccessFeedbackRoleArn"],
		"substrate reports a write-only name back; AWS does not")
	assert.Equal(t, "100", attrs["SQSSuccessFeedbackSampleRate"])
	assert.Equal(t, "MessageGroup", attrs["FifoThroughputScope"])
}
