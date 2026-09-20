package emulator_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CreateTopic decoded no Attributes map at all. It seeded the new topic's attributes from a bare
// `DisplayName` query parameter — which API_CreateTopic does not publish as a parameter; it publishes
// exactly four, `Attributes`, `DataProtectionPolicy`, `Name` and `Tags.member.N` — and read the
// published `Attributes.entry.N.key` / `.value` form nowhere (#1126).
//
// So the defect ran in both directions at once. `create_topic(Name=…, Attributes={…})`, the ordinary
// SDK call, was answered 200 with every attribute discarded, and GetTopicAttributes then reported none
// of them; while a parameter real SNS would ignore was the only one honored, so a test written
// against substrate passed where the same call against AWS creates a topic with no display name.
//
// The tests below are the create-side counterparts of sns_settable_attributes_test.go's, and follow the
// same rule: snsCreatableNames restates API_CreateTopic's list rather than reading the plugin's, so a
// name dropped from or misspelled in snsCreatableTopicAttributeNames fails here instead of agreeing
// with itself. Every attribute goes over the wire in the form an SDK sends, never through a hand-built
// Params map, because the wire form *is* what was broken.

// snsCreatableNames is the twenty-four Attributes map keys API_CreateTopic publishes, in the page's own
// order: five general, fifteen delivery-status names across five endpoint families, one under
// server-side encryption, three under FIFO topics.
//
// The one difference from snsSettableNames is the server-side-encryption group: CreateTopic lists
// `KmsMasterKeyId` alone where SetTopicAttributes lists `KmsMasterKeyId` and `SignatureVersion`.
var snsCreatableNames = []string{ //nolint:gochecknoglobals // a transcription of one AWS page, read by two tests
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

	"ArchivePolicy",
	"ContentBasedDeduplication",
	"FifoThroughputScope",
}

// snsAttributeEntries renders an attribute map as the indexed query parameters API_CreateTopic
// publishes, which is what an SDK puts on the wire for `Attributes={…}`.
//
// Keys are taken in the caller's order so a test can say which entry is at which index — the refusal
// tests below depend on it, because a refusal names the first unpublished key the decoder reaches and
// Go's map iteration order would make that arbitrary.
func snsAttributeEntries(pairs ...string) map[string]string {
	params := make(map[string]string, len(pairs))
	for i := 0; i+1 < len(pairs); i += 2 {
		prefix := "Attributes.entry." + strconv.Itoa(i/2+1) + "."
		params[prefix+"key"] = pairs[i]
		params[prefix+"value"] = pairs[i+1]
	}
	return params
}

// snsCreateTopicWithAttributes sends one CreateTopic carrying an Attributes map and returns the status
// and the error code a refusal carries.
func snsCreateTopicWithAttributes(
	t *testing.T, ts *emulator.TestServer, name string, attrs map[string]string,
) (int, string) {
	t.Helper()
	params := map[string]string{"Action": "CreateTopic", "Name": name}
	for k, v := range attrs {
		params[k] = v
	}
	status, _, errCode := snsQuery(t, ts, snsEastRegion, params)
	return status, errCode
}

// TestSNSCreateTopicStoresThePublishedAttributesMap is #1126's first criterion: three attributes sent
// in one call are all reported by GetTopicAttributes.
//
// The three are drawn from three of the page's four groups on purpose — one general, one
// delivery-status, one server-side encryption — because the groups are where a transcription slips, and
// a decoder keyed off a prefix would pass with any one of them.
func TestSNSCreateTopicStoresThePublishedAttributesMap(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	arn := snsCreateTopicIn(t, ts, snsEastRegion, "create-attrs", snsAttributeEntries(
		"DisplayName", "Orders",
		"SQSSuccessFeedbackSampleRate", "100",
		"KmsMasterKeyId", "alias/aws/sns",
	))

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, "Orders", attrs["DisplayName"],
		"the attribute a CDK-generated AWS::SNS::Topic sets, and the one the bare parameter used to carry")
	assert.Equal(t, "100", attrs["SQSSuccessFeedbackSampleRate"])
	assert.Equal(t, "alias/aws/sns", attrs["KmsMasterKeyId"])
}

// TestSNSCreateTopicAcceptsEveryPublishedName walks all twenty-four names in a single create, and
// asserts each is stored rather than merely accepted.
//
// The value sent is the name itself, for the reason
// TestSNSSetTopicAttributesAcceptsEveryPublishedName gives: #1126 is about the name, substrate
// validates no attribute value anywhere, and a plausible value per name would hide a future value
// check behind a passing test.
func TestSNSCreateTopicAcceptsEveryPublishedName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	require.Len(t, snsCreatableNames, 24, "API_CreateTopic publishes twenty-four Attributes keys")

	pairs := make([]string, 0, 2*len(snsCreatableNames))
	for _, name := range snsCreatableNames {
		pairs = append(pairs, name, name)
	}
	arn := snsCreateTopicIn(t, ts, snsEastRegion, "create-attrs-all", snsAttributeEntries(pairs...))

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	for _, name := range snsCreatableNames {
		assert.Equal(t, name, attrs[name], "a creatable %s is stored, not merely accepted", name)
	}
}

// TestSNSCreateTopicRefusesAnUnpublishedName asserts the refusal, and that the refused call created no
// topic.
//
// The second half is the point: the decode runs before the topic index is read, so a rejected create
// cannot leave a half-built record or an index entry behind. `Banana` is the name #1067 used for the
// same question on the set side, so the two refusals are visibly one rule.
func TestSNSCreateTopicRefusesAnUnpublishedName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	status, errCode := snsCreateTopicWithAttributes(t, ts, "create-attrs-refused",
		snsAttributeEntries("DisplayName", "Orders", "Banana", "yellow"))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameter", errCode,
		"the code setTopicAttributes answers for the same mistake")

	body := snsQueryOK(t, ts, snsEastRegion, map[string]string{"Action": "ListTopics"})
	assert.NotContains(t, body, "create-attrs-refused",
		"a refused create leaves no topic behind, because the decode precedes every write")
}

// TestSNSCreateTopicRefusesTheReadOnlyNames asserts each of the eight names API_GetTopicAttributes
// publishes and API_CreateTopic does not is refused on a create.
//
// Same eight as the set side, and the same argument: each is a fact about the topic — the ARN it is
// keyed by, the account that owns it, three subscription counts, the delivery policy SNS computes from
// the one a caller set, the archive's start time, and whether the topic is FIFO — so a caller who could
// write one could then read it back as though SNS had reported it.
func TestSNSCreateTopicRefusesTheReadOnlyNames(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	for _, name := range snsReadOnlyAttributeNames {
		status, errCode := snsCreateTopicWithAttributes(t, ts,
			"create-attrs-ro-"+strings.ToLower(name), snsAttributeEntries(name, "x"))
		assert.Equal(t, http.StatusBadRequest, status, "CreateTopic %s", name)
		assert.Equal(t, "InvalidParameter", errCode, "CreateTopic %s is not published as creatable", name)
	}
}

// TestSNSSignatureVersionIsSettableButNotCreatable is the one asymmetry between the two pages, and the
// reason the two allowlists are two lists.
//
// API_SetTopicAttributes lists `SignatureVersion` in its server-side-encryption group beside
// `KmsMasterKeyId`; API_CreateTopic's same group lists `KmsMasterKeyId` alone. So the name is refused on
// a create and accepted on a set, and a topic can still be given one — through the operation whose page
// publishes it. Pinned in one test so that a later reader who collapses the two lists sees exactly what
// breaks.
func TestSNSSignatureVersionIsSettableButNotCreatable(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	status, errCode := snsCreateTopicWithAttributes(t, ts, "create-attrs-sigver",
		snsAttributeEntries("SignatureVersion", "2"))
	assert.Equal(t, http.StatusBadRequest, status, "CreateTopic does not publish SignatureVersion")
	assert.Equal(t, "InvalidParameter", errCode)

	arn := snsCreateTopic(t, ts, "create-attrs-sigver")
	setStatus, setErr := snsSetTopicAttribute(t, ts, snsEastRegion, arn, "SignatureVersion", "2")
	assert.Equal(t, http.StatusOK, setStatus, "SetTopicAttributes does publish SignatureVersion")
	assert.Empty(t, setErr)

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, "2", attrs["SignatureVersion"],
		"the name CreateTopic refuses is reachable through the operation that publishes it")
}

// TestSNSCreateTopicIgnoresTheBareDisplayNameParameter pins the removal.
//
// `DisplayName` is one of the *values* API_CreateTopic's Attributes key may take, not a request
// parameter of its own — the page publishes four, and that is not among them. Substrate honored it
// anyway, so a consumer could write `create_topic(Name=…)` with a `DisplayName` parameter smuggled in,
// see it round-trip, and get a topic with no display name from AWS. This is the observable half of
// #1126, which is why the CHANGELOG records it under Changed.
func TestSNSCreateTopicIgnoresTheBareDisplayNameParameter(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	arn := snsCreateTopicIn(t, ts, snsEastRegion, "create-attrs-bare",
		map[string]string{"DisplayName": "Ignored"})

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.NotContains(t, attrs, "DisplayName",
		"a parameter API_CreateTopic does not publish is ignored, not honored")
}

// TestSNSCreateTopicAttributesEndAtTheFirstAbsentIndex pins the decoder's termination rule.
//
// The numbering is 1-based and dense, the convention every indexed query-protocol decoder in the tree
// follows, so an entry at index 3 with nothing at index 2 is not read. Asserted rather than assumed
// because the alternative — scanning every parameter for the prefix — would silently accept a sparse
// map no SDK sends and make the refusal above depend on Go's map iteration order.
func TestSNSCreateTopicAttributesEndAtTheFirstAbsentIndex(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	arn := snsCreateTopicIn(t, ts, snsEastRegion, "create-attrs-sparse", map[string]string{
		"Attributes.entry.1.key":   "DisplayName",
		"Attributes.entry.1.value": "First",
		"Attributes.entry.3.key":   "Policy",
		"Attributes.entry.3.value": "{}",
	})

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, "First", attrs["DisplayName"])
	assert.NotContains(t, attrs, "Policy", "index 2 is absent, so index 3 is not reached")
}

// TestSNSCreateTopicAttributesAcceptAnEmptyValue pins that a key present with an empty value is stored.
//
// setTopicAttributes stores an empty AttributeValue — TestSNSSetTopicAttributesAcceptsAnEmptyValue
// pins that — and the two operations must not disagree about what clearing an attribute means. It is
// the *key* that ends the map, never the value.
func TestSNSCreateTopicAttributesAcceptAnEmptyValue(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)

	arn := snsCreateTopicIn(t, ts, snsEastRegion, "create-attrs-empty", snsAttributeEntries(
		"DisplayName", "",
		"Policy", "{}",
	))

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	require.Contains(t, attrs, "DisplayName", "an empty value does not end the map")
	assert.Empty(t, attrs["DisplayName"])
	assert.Equal(t, "{}", attrs["Policy"], "the entry after the empty one is still read")
}
