package emulator_test

import (
	"encoding/xml"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// SetSubscriptionAttributes was a literal stub: it read SubscriptionArn into `_`, read neither
// AttributeName nor AttributeValue, touched no state and answered 200 (#1125). So a subscription that
// did not exist was a success, a malformed ARN was a success, and an attribute name AWS publishes
// nowhere was a success — and the value was unreadable either way, because GetSubscriptionAttributes
// answered a fixed four entries built from the record.
//
// A consumer testing "set a FilterPolicy, then confirm the subscription reports it" got a green 200 from
// the set and a response from the get with the member simply absent. These tests are the subscription
// counterparts of sns_topic_attributes_test.go's and sns_settable_attributes_test.go's, and follow the
// same rule: snsSettableSubscriptionNames and snsSubscriptionReadOnlyNames restate the two AWS pages
// rather than reading the plugin's lists, so a name dropped from or misspelled in
// snsSettableSubscriptionAttributeNames fails here instead of agreeing with itself.

// snsSettableSubscriptionNames is the six AttributeName values API_SetSubscriptionAttributes publishes,
// in the page's own order: five general, then `SubscriptionRoleArn` under a heading reading "The
// following attribute applies only to Amazon Data Firehose delivery stream subscriptions".
//
// Six, not seven. `ReplayPolicy` is on API_Subscribe — under a heading reading "The following
// attributes apply only to FIFO topics", beside a `ReplayStatus` — and is not on this page at all, so it
// is refused here. The page's `ReplayLimitExceeded`/403 is an error shared with Subscribe and says
// nothing about which names this operation takes.
var snsSettableSubscriptionNames = []string{ //nolint:gochecknoglobals // a transcription of one AWS page, read by two tests
	"DeliveryPolicy",
	"FilterPolicy",
	"FilterPolicyScope",
	"RawMessageDelivery",
	"RedrivePolicy",

	"SubscriptionRoleArn",
}

// snsSubscriptionReadOnlyNames is the six names API_GetSubscriptionAttributes publishes and
// API_SetSubscriptionAttributes does not.
//
// Each is a fact about the subscription rather than anything a caller sets: the ARN it is keyed by, the
// topic it is attached to, the account that owns it, whether it is still awaiting confirmation, whether
// that confirmation was authenticated, and the delivery policy SNS computes from the one a caller set.
// A caller able to write one could then read it back as though SNS had reported it, which is the defect
// #1067 fixed on the topic side.
//
// The two lists partition the Get page exactly: 6 settable + 6 read-only = the 12 names it publishes.
// TestSNSSubscriptionAttributeListsPartitionTheGetPage asserts that, which is what makes a name
// appearing in neither list a test failure rather than a silent omission.
var snsSubscriptionReadOnlyNames = []string{ //nolint:gochecknoglobals // a transcription of one AWS page, read by two tests
	"ConfirmationWasAuthenticated",
	"EffectiveDeliveryPolicy",
	"Owner",
	"PendingConfirmation",
	"SubscriptionArn",
	"TopicArn",
}

// snsSubscriptionAttributes returns the attribute map GetSubscriptionAttributes reports, or the error
// code it refused with.
//
// The wire response is the probe rather than the state record, per #765: a helper reading the record
// would read the very map the assertion has to trust.
func snsSubscriptionAttributes(
	t *testing.T, ts *emulator.TestServer, region, arn string,
) (map[string]string, string) {
	t.Helper()
	status, body, errCode := snsQuery(t, ts, region, map[string]string{
		"Action":          "GetSubscriptionAttributes",
		"SubscriptionArn": arn,
	})
	if errCode != "" {
		return nil, errCode
	}
	require.Equal(t, http.StatusOK, status, "GetSubscriptionAttributes %s", arn)

	var doc struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"GetSubscriptionAttributesResult>Attributes>entry"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode GetSubscriptionAttributes %s: %v (body %s)", arn, err, body)
	}
	attrs := make(map[string]string, len(doc.Entries))
	for _, e := range doc.Entries {
		attrs[e.Key] = e.Value
	}
	return attrs, ""
}

// snsSubscriptionAttributeKeys returns the entry keys GetSubscriptionAttributes reports, in the order
// the body carries them.
//
// Read off the raw body rather than a map so a duplicated entry is visible: a merge that appended a
// derived member instead of overwriting the stored one would report the same key twice, which a map
// hides.
func snsSubscriptionAttributeKeys(t *testing.T, ts *emulator.TestServer, region, arn string) []string {
	t.Helper()
	body := snsQueryOK(t, ts, region, map[string]string{
		"Action":          "GetSubscriptionAttributes",
		"SubscriptionArn": arn,
	})
	var doc struct {
		Entries []struct {
			Key string `xml:"key"`
		} `xml:"GetSubscriptionAttributesResult>Attributes>entry"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode GetSubscriptionAttributes %s: %v (body %s)", arn, err, body)
	}
	keys := make([]string, 0, len(doc.Entries))
	for _, e := range doc.Entries {
		keys = append(keys, e.Key)
	}
	return keys
}

// snsSetSubscriptionAttribute sends one SetSubscriptionAttributes and returns the status and the error
// code a refusal carries.
func snsSetSubscriptionAttribute(
	t *testing.T, ts *emulator.TestServer, region, arn, name, value string,
) (int, string) {
	t.Helper()
	status, _, errCode := snsQuery(t, ts, region, map[string]string{
		"Action":          "SetSubscriptionAttributes",
		"SubscriptionArn": arn,
		"AttributeName":   name,
		"AttributeValue":  value,
	})
	return status, errCode
}

// snsSubscriptionFixture creates a topic, subscribes an SQS endpoint to it, and returns the
// subscription ARN.
func snsSubscriptionFixture(t *testing.T, ts *emulator.TestServer, topicName string) string {
	t.Helper()
	topicARN := snsCreateTopic(t, ts, topicName)
	return snsSubscribe(t, ts, snsEastRegion, topicARN)
}

// TestSNSSubscriptionAttributeRoundTrip is #1125's central criterion: an attribute set is an attribute
// reported.
//
// Neither half existed. The set discarded the name and the value, and the get answered a fixed four
// entries, so the round trip failed twice over — which is why it is asserted through both operations on
// the wire rather than against the record.
func TestSNSSubscriptionAttributeRoundTrip(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-roundtrip")

	before, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	assert.NotContains(t, before, "FilterPolicy", "a fresh subscription has no filter policy")

	const policy = `{"color":["red","blue"]}`
	status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, subARN, "FilterPolicy", policy)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, errCode)

	after, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	assert.Equal(t, policy, after["FilterPolicy"],
		"the value the caller set, reported verbatim by the operation that publishes it")
}

// TestSNSSubscriptionAcceptsEveryPublishedName walks all six names and asserts each is stored rather
// than merely accepted.
//
// The value sent is the name itself, for the reason TestSNSSetTopicAttributesAcceptsEveryPublishedName
// gives: #1125 is about the name, substrate validates no attribute value anywhere, and a plausible
// value per name would hide a future value check behind a passing test.
func TestSNSSubscriptionAcceptsEveryPublishedName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-all")

	require.Len(t, snsSettableSubscriptionNames, 6,
		"API_SetSubscriptionAttributes publishes six AttributeName values")

	for _, name := range snsSettableSubscriptionNames {
		status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, subARN, name, name)
		assert.Equal(t, http.StatusOK, status, "SetSubscriptionAttributes %s", name)
		assert.Empty(t, errCode, "SetSubscriptionAttributes %s", name)
	}

	attrs, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	for _, name := range snsSettableSubscriptionNames {
		assert.Equal(t, name, attrs[name], "a settable %s is stored, not merely accepted", name)
	}
}

// TestSNSSubscriptionRefusesAnUnpublishedName asserts the refusal, and that the refused call stored
// nothing.
//
// `Banana` is the name #1067 used for the same question on the topic side, so the two refusals read as
// one rule. The second half matters because the old handler's failure mode was answering 200 and
// writing nothing: asserting only the 400 would not distinguish a refusal from a silent discard.
func TestSNSSubscriptionRefusesAnUnpublishedName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-refused")

	status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, subARN, "Banana", "yellow")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameter", errCode,
		"the code setTopicAttributes answers for the same mistake")

	attrs, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	assert.NotContains(t, attrs, "Banana", "a refused name is not stored")
}

// TestSNSSubscriptionRefusesTheReadOnlyNames asserts each of the six names
// API_GetSubscriptionAttributes publishes and API_SetSubscriptionAttributes does not is refused.
//
// The stub accepted all six. `Owner` and `SubscriptionArn` are the sharp ones: a caller able to set
// either could have had GetSubscriptionAttributes report a subscription owned by another account, or
// one whose ARN names a subscription that does not exist.
func TestSNSSubscriptionRefusesTheReadOnlyNames(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-readonly")

	require.Len(t, snsSubscriptionReadOnlyNames, 6,
		"six of the twelve names Get publishes are not on Set's page")

	for _, name := range snsSubscriptionReadOnlyNames {
		status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, subARN, name, "x")
		assert.Equal(t, http.StatusBadRequest, status, "SetSubscriptionAttributes %s", name)
		assert.Equal(t, "InvalidParameter", errCode,
			"%s is published by Get and not by Set, so it is not settable", name)
	}
}

// TestSNSSubscriptionRefusesReplayPolicy pins the one name whose page is not this operation's.
//
// `ReplayPolicy` and `ReplayStatus` are on API_Subscribe, under a heading reading "The following
// attributes apply only to FIFO topics". API_SetSubscriptionAttributes lists neither, so both are
// refused here — the #671 rule, only what the API model states, applied to a name a sibling operation
// does publish. Pinned in its own test because the temptation to add it from the `ReplayLimitExceeded`
// error this page *does* publish is exactly the inference #671 rules out.
func TestSNSSubscriptionRefusesReplayPolicy(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-replay")

	for _, name := range []string{"ReplayPolicy", "ReplayStatus"} {
		status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, subARN, name, "{}")
		assert.Equal(t, http.StatusBadRequest, status, "SetSubscriptionAttributes %s", name)
		assert.Equal(t, "InvalidParameter", errCode,
			"%s is on API_Subscribe's Attributes list, not on this operation's", name)
	}
}

// TestSNSSubscriptionAttributesRequireAName asserts the missing-AttributeName refusal.
//
// The page marks AttributeName "Required: Yes" and AttributeValue "Required: No", which is the
// asymmetry TestSNSSubscriptionAttributeAcceptsAnEmptyValue covers from the other side.
func TestSNSSubscriptionAttributesRequireAName(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-noname")

	status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":          "SetSubscriptionAttributes",
		"SubscriptionArn": subARN,
		"AttributeValue":  "whatever",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameter", errCode, "AttributeName is Required: Yes")
}

// TestSNSSubscriptionAttributeAcceptsAnEmptyValue pins that an absent AttributeValue stores the empty
// string rather than being refused.
//
// The page marks it "Required: No", and setTopicAttributes gives an empty AttributeValue the same
// meaning, so the two operations do not disagree about what clearing an attribute means.
func TestSNSSubscriptionAttributeAcceptsAnEmptyValue(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	subARN := snsSubscriptionFixture(t, ts, "sub-attrs-empty")

	status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":          "SetSubscriptionAttributes",
		"SubscriptionArn": subARN,
		"AttributeName":   "RawMessageDelivery",
	})
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, errCode, "AttributeValue is Required: No")

	attrs, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	require.Contains(t, attrs, "RawMessageDelivery", "an absent value still stores the name")
	assert.Empty(t, attrs["RawMessageDelivery"])
}

// TestSNSSubscriptionAttributesRefuseAnAbsentSubscription asserts the NotFound/404 the stub could not
// answer.
//
// The ARN is well-formed and names a subscription id that was never minted, which separates this
// refusal from the malformed-ARN one below: the stub answered 200 to both, and a single test could not
// tell the two published codes apart.
func TestSNSSubscriptionAttributesRefuseAnAbsentSubscription(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "sub-attrs-absent")
	ghost := topicARN + ":00000000000000000000000000000000"

	status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, ghost, "FilterPolicy", "{}")
	assert.Equal(t, http.StatusNotFound, status, "a subscription that does not exist is not a success")
	assert.Equal(t, "NotFound", errCode)

	_, getErr := snsSubscriptionAttributes(t, ts, snsEastRegion, ghost)
	assert.Equal(t, "NotFound", getErr, "the same refusal on the read side")
}

// TestSNSSubscriptionAttributesRefuseAMalformedARN asserts the InvalidParameter/400 for a string that is
// not a subscription ARN, on both operations.
//
// A topic ARN is the interesting case and the first row: it is a perfectly good SNS ARN that names no
// subscription, and before #1125 the handlers built a state key from it and reported NotFound — telling
// a caller the subscription did not exist rather than that the ARN was not one. The two codes are
// distinguishable now, which is what makes the row above meaningful.
//
// The last three rows are seven-part ARNs missing one part each, which is why they are written out rather
// than derived from topicARN: an ARN of the right arity whose Region, account or topic is empty reaches
// the refusal only by being constructed that way, and a caller that builds an ARN by formatting can
// produce exactly this by interpolating an unset variable.
func TestSNSSubscriptionAttributesRefuseAMalformedARN(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "sub-attrs-malformed")

	for _, tc := range []struct {
		name string
		arn  string
	}{
		{"a topic ARN names no subscription", topicARN},
		{"empty", ""},
		{"not an ARN", "sub-attrs-malformed"},
		{"another service", "arn:aws:sqs:us-east-1:123456789012:queue:tail"},
		{"no subscription id", topicARN + ":"},
		{"no region", "arn:aws:sns::123456789012:sub-attrs-malformed:1a2b3c4d"},
		{"no account", "arn:aws:sns:us-east-1::sub-attrs-malformed:1a2b3c4d"},
		{"no topic", "arn:aws:sns:us-east-1:123456789012::1a2b3c4d"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, tc.arn, "FilterPolicy", "{}")
			assert.Equal(t, http.StatusBadRequest, status, "SetSubscriptionAttributes %q", tc.arn)
			assert.Equal(t, "InvalidParameter", errCode, "SetSubscriptionAttributes %q", tc.arn)

			_, getErr := snsSubscriptionAttributes(t, ts, snsEastRegion, tc.arn)
			assert.Equal(t, "InvalidParameter", getErr, "GetSubscriptionAttributes %q", tc.arn)
		})
	}
}

// TestSNSSubscriptionAttributesReportsTheDerivedMembers asserts the key set of a freshly created
// subscription: the seven members substrate derives, nothing else, and no repeat.
//
// EffectiveDeliveryPolicy is asserted *absent*, and that is the deliberate omission rather than an
// oversight. The page defines it as the policy "that takes into account the topic delivery policy and
// account system defaults", and substrate models neither the defaults nor the merge — so reporting the
// subscription's own DeliveryPolicy under the name would claim a computation that did not happen.
// Omitting it claims nothing, which is the honest-empty rule (#827).
//
// Protocol and Endpoint are asserted *present* though the Get page's list omits both. That list is
// explicitly open ("Attributes in this map include the following"), both are Subscribe parameters a
// caller cannot otherwise read back for a single subscription, and ListSubscriptions already reports
// both for the same record — so dropping them would make two readers disagree about one subscription.
func TestSNSSubscriptionAttributesReportsTheDerivedMembers(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "sub-attrs-derived")
	subARN := snsSubscribe(t, ts, snsEastRegion, topicARN)

	attrs, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)

	assert.Equal(t, subARN, attrs["SubscriptionArn"])
	assert.Equal(t, topicARN, attrs["TopicArn"])
	assert.Equal(t, taggingTestAccount, attrs["Owner"], "the account that called Subscribe")
	assert.Equal(t, "sqs", attrs["Protocol"])
	assert.NotEmpty(t, attrs["Endpoint"])
	assert.Equal(t, "false", attrs["PendingConfirmation"],
		"Subscribe mints a confirmed subscription: there is no ConfirmSubscription operation")
	assert.Equal(t, "true", attrs["ConfirmationWasAuthenticated"],
		"confirmed by the authenticated Subscribe call itself, never by an out-of-band token")

	assert.NotContains(t, attrs, "EffectiveDeliveryPolicy",
		"substrate models no account system defaults to compute one from")
	assert.NotContains(t, attrs, "DeliveryPolicy", "nothing set one")

	keys := snsSubscriptionAttributeKeys(t, ts, snsEastRegion, subARN)
	assert.Equal(t, []string{
		"ConfirmationWasAuthenticated", "Endpoint", "Owner",
		"PendingConfirmation", "Protocol", "SubscriptionArn", "TopicArn",
	}, keys, "the derived members, sorted, with no entry AWS does not publish and no repeat")
}

// TestSNSStoredSubscriptionAttributeCannotShadowADerivedOne pins the merge order.
//
// getSubscriptionAttributes merges the stored map first and the derived members over the top, so a
// stored Owner cannot shadow the real one. Since setSubscriptionAttributes refuses all six read-only
// names, no request can put one in the map — the shadow is seeded through state for that reason, which
// keeps the assertion about the merge order rather than about the refusal. A record restored from an
// event log or written by an older build reaches the same merge, which is why the property is pinned at
// the layer that still has it. This is the subscription counterpart of
// TestSNSStoredAttributeCannotShadowADerivedOne.
func TestSNSStoredSubscriptionAttributeCannotShadowADerivedOne(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "sub-attrs-no-shadow")
	subARN := snsSubscribe(t, ts, snsEastRegion, topicARN)

	for name, value := range map[string]string{
		"SubscriptionArn":              "arn:aws:sns:us-east-1:999999999999:someone-else:deadbeef",
		"TopicArn":                     "arn:aws:sns:us-east-1:999999999999:someone-elses-topic",
		"Owner":                        "999999999999",
		"PendingConfirmation":          "true",
		"ConfirmationWasAuthenticated": "false",
		"Protocol":                     "email",
		"Endpoint":                     "attacker@example.com",
	} {
		require.NoError(t, emulator.SeedSNSSubscriptionAttributeForTest(t.Context(), ts.StateManager(),
			taggingTestAccount, snsEastRegion, subARN, name, value),
			"seed a stored %s the merge must not report", name)
	}

	attrs, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	assert.Equal(t, subARN, attrs["SubscriptionArn"], "a stored ARN does not shadow the real one")
	assert.Equal(t, topicARN, attrs["TopicArn"])
	assert.Equal(t, taggingTestAccount, attrs["Owner"], "a stored Owner does not shadow the real one")
	assert.Equal(t, "false", attrs["PendingConfirmation"])
	assert.Equal(t, "true", attrs["ConfirmationWasAuthenticated"])
	assert.Equal(t, "sqs", attrs["Protocol"], "the protocol the subscription was created with")
	assert.NotEqual(t, "attacker@example.com", attrs["Endpoint"])

	keys := snsSubscriptionAttributeKeys(t, ts, snsEastRegion, subARN)
	assert.Equal(t, []string{
		"ConfirmationWasAuthenticated", "Endpoint", "Owner",
		"PendingConfirmation", "Protocol", "SubscriptionArn", "TopicArn",
	}, keys, "overwriting a derived member does not duplicate its entry")
}

// TestSNSSubscriptionAttributeListsPartitionTheGetPage asserts the two transcriptions above together
// account for every name API_GetSubscriptionAttributes publishes, exactly once each.
//
// It is what stops a name from being quietly absent from both lists. The settable six and the read-only
// six are disjoint and their union is the Get page's twelve, so a name added to one list and removed
// from neither — or a name this file forgot — fails here rather than being silently untested.
func TestSNSSubscriptionAttributeListsPartitionTheGetPage(t *testing.T) {
	t.Parallel()

	// The twelve names API_GetSubscriptionAttributes' Attributes response map publishes, transcribed
	// once more so the partition is asserted against the page rather than against the two halves.
	getPublishes := []string{
		"ConfirmationWasAuthenticated",
		"DeliveryPolicy",
		"EffectiveDeliveryPolicy",
		"FilterPolicy",
		"FilterPolicyScope",
		"Owner",
		"PendingConfirmation",
		"RawMessageDelivery",
		"RedrivePolicy",
		"SubscriptionArn",
		"TopicArn",
		"SubscriptionRoleArn",
	}
	require.Len(t, getPublishes, 12, "API_GetSubscriptionAttributes publishes twelve names")

	union := make([]string, 0, len(getPublishes))
	union = append(union, snsSettableSubscriptionNames...)
	union = append(union, snsSubscriptionReadOnlyNames...)
	sort.Strings(union)
	sorted := append([]string(nil), getPublishes...)
	sort.Strings(sorted)

	assert.Equal(t, sorted, union,
		"every name Get publishes is in exactly one of the two lists, and neither list invents one")
}

// TestSNSStoredFilterPolicyDoesNotFilterDelivery pins the scope boundary #1125 deliberately stopped at.
//
// Storing a FilterPolicy is not evaluating one. Delivering only matching messages is the subscription's
// runtime behavior rather than something observable through an API call, so it stays out per CLAUDE.md's
// boundary — the same reading that keeps a Lambda's handler from being executed. The attribute is
// recorded intent: it round-trips through GetSubscriptionAttributes and changes nothing about Publish.
//
// Asserted rather than left implicit because the opposite is the tempting next commit, and because
// SNSSubscription carries a separate FilterPolicy field that publish does consult — reachable only by a
// test writing the record directly, never by an API call. Pinning the boundary here is what makes a
// later change to it deliberate.
func TestSNSStoredFilterPolicyDoesNotFilterDelivery(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "sub-attrs-filter-inert")
	subARN := snsSubscribe(t, ts, snsEastRegion, topicARN)

	status, errCode := snsSetSubscriptionAttribute(t, ts, snsEastRegion, subARN,
		"FilterPolicy", `{"color":["red"]}`)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, errCode)

	attrs, errCode := snsSubscriptionAttributes(t, ts, snsEastRegion, subARN)
	require.Empty(t, errCode)
	require.Equal(t, `{"color":["red"]}`, attrs["FilterPolicy"], "stored as recorded intent")

	// A message whose attributes the policy would exclude is still published successfully. Publish
	// answers a MessageId either way, which is the whole of what an API caller can observe.
	body := snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":                         "Publish",
		"TopicArn":                       topicARN,
		"Message":                        "not red",
		"MessageAttributes.entry.1.Name": "color",
		"MessageAttributes.entry.1.Value.DataType":    "String",
		"MessageAttributes.entry.1.Value.StringValue": "green",
	})
	assert.Contains(t, body, "<MessageId>",
		"a stored FilterPolicy is recorded intent and does not make Publish refuse or drop")
}
