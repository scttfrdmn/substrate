package emulator_test

import (
	"encoding/xml"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetTopicAttributes reported a SubscriptionsCount member API_GetTopicAttributes does not publish
// anywhere, hardcoded to "0", and reported none of the three subscription counts the page does publish
// (#993).
//
// The invented member is the visible half. The load-bearing half is that nothing was derived at all:
// every attribute except TopicArn came out of the topic record's stored map, so Owner was absent where
// AWS's own sample carries it, and the one count substrate did report was a constant no writer ever
// touched — the #847 class crossed with the #914 class in a single four-line literal.
//
// The assertions below read the raw XML rather than a decoded map. A map[string]string does distinguish
// an absent key from one carrying "0", so that is not why the constant survived — it survived because
// no test read the attribute at all. What a map cannot see is a *duplicated* <entry>, which is the
// failure mode the derive-after-merge ordering could introduce, so the key set is asserted as the
// ordered slice the document actually carries.

// snsAttributeEntryKeys returns the Attributes entry keys a GetTopicAttributes body carries, in
// document order and without collapsing a repeat.
//
// It is the raw-XML sibling of [snsTopicAttributes], which decodes into a map. Both are needed: the map
// answers "what value does this attribute have", and this answers "which attributes are there, and is
// any of them there twice".
func snsAttributeEntryKeys(t *testing.T, body string) []string {
	t.Helper()

	var doc struct {
		Entries []struct {
			Key string `xml:"key"`
		} `xml:"GetTopicAttributesResult>Attributes>entry"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode GetTopicAttributes: %v (body %s)", err, body)
	}
	keys := make([]string, 0, len(doc.Entries))
	for _, e := range doc.Entries {
		keys = append(keys, e.Key)
	}
	return keys
}

// snsGetTopicAttributesBody returns the raw GetTopicAttributes body for a topic ARN.
func snsGetTopicAttributesBody(t *testing.T, ts *emulator.TestServer, region, arn string) string {
	t.Helper()
	return snsQueryOK(t, ts, region, map[string]string{
		"Action":   "GetTopicAttributes",
		"TopicArn": arn,
	})
}

// TestSNSTopicAttributesReportsOnlyPublishedNames asserts the attribute key set of a freshly created
// topic: the four members substrate derives, nothing AWS does not publish, and no repeat.
//
// SubscriptionsDeleted is asserted *absent*, which is the deliberate omission rather than an oversight.
// Nothing in substrate tracks a deletion — Unsubscribe removes the record and filters both indexes —
// and the page says only "The number of deleted subscriptions for the topic" without saying how long a
// deleted subscription stays counted, so a monotonic counter would be an invention. Reporting nothing
// claims nothing (#827).
func TestSNSTopicAttributesReportsOnlyPublishedNames(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "attrs-published-names")

	body := snsGetTopicAttributesBody(t, ts, snsEastRegion, arn)
	keys := snsAttributeEntryKeys(t, body)

	assert.Equal(t, []string{"Owner", "SubscriptionsConfirmed", "SubscriptionsPending", "TopicArn"}, keys,
		"a topic with no stored attributes reports exactly the four derived members, once each")
	assert.NotContains(t, keys, "SubscriptionsCount",
		"SubscriptionsCount appears nowhere on API_GetTopicAttributes")
	assert.NotContains(t, keys, "SubscriptionsDeleted",
		"SubscriptionsDeleted is omitted rather than reported as a constant nothing tracks")

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode, "GetTopicAttributes succeeds")
	assert.Equal(t, arn, attrs["TopicArn"], "TopicArn is the ARN CreateTopic minted")
	assert.Equal(t, taggingTestAccount, attrs["Owner"],
		"Owner is the account segment of the ARN the record is keyed by, per the page's sample")
	assert.Equal(t, "0", attrs["SubscriptionsConfirmed"], "a new topic has no subscriptions")
	assert.Equal(t, "0", attrs["SubscriptionsPending"],
		"substrate mints no unconfirmed subscription: there is no ConfirmSubscription")
}

// TestSNSSubscriptionsConfirmedTracksTheIndex asserts the count moves with Subscribe and Unsubscribe,
// which is the whole difference between a derived value and the constant it replaced.
//
// It also asserts the property that decided the keying question: SubscriptionsConfirmed and
// ListSubscriptionsByTopic read the same per-topic index under the same key, so they agree at every
// step. A count keyed by the ARN's target rather than the caller's account and Region would have
// disagreed with the list by construction for a cross-account ARN.
func TestSNSSubscriptionsConfirmedTracksTheIndex(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "attrs-confirmed-count")

	confirmed := func() string {
		t.Helper()
		attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
		require.Empty(t, errCode, "GetTopicAttributes succeeds")
		return attrs["SubscriptionsConfirmed"]
	}
	listed := func() int {
		t.Helper()
		body := snsQueryOK(t, ts, snsEastRegion, map[string]string{
			"Action":   "ListSubscriptionsByTopic",
			"TopicArn": arn,
		})
		var doc struct {
			ARNs []string `xml:"ListSubscriptionsByTopicResult>Subscriptions>member>SubscriptionArn"`
		}
		if err := xml.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("decode ListSubscriptionsByTopic: %v (body %s)", err, body)
		}
		return len(doc.ARNs)
	}

	require.Equal(t, "0", confirmed())
	require.Equal(t, 0, listed())

	first := snsSubscribe(t, ts, snsEastRegion, arn)
	assert.Equal(t, "1", confirmed(), "one Subscribe is one confirmed subscription")
	assert.Equal(t, 1, listed(), "the count and the list read the same index")

	snsSubscribe(t, ts, snsEastRegion, arn)
	assert.Equal(t, "2", confirmed(), "the count is the index length, not a constant")
	assert.Equal(t, 2, listed())

	snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":          "Unsubscribe",
		"SubscriptionArn": first,
	})
	assert.Equal(t, "1", confirmed(), "Unsubscribe lowers the count")
	assert.Equal(t, 1, listed(), "the count and the list still agree")

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, "0", attrs["SubscriptionsPending"],
		"a deleted subscription does not become a pending one")
	assert.NotContains(t, snsAttributeEntryKeys(t, snsGetTopicAttributesBody(t, ts, snsEastRegion, arn)),
		"SubscriptionsDeleted", "Unsubscribe does not make SubscriptionsDeleted reportable")
}

// TestSNSStoredAttributeCannotShadowADerivedOne pins the merge order.
//
// setTopicAttributes used to write any AttributeName a caller sent into the topic's stored map unchecked,
// and the handler used to merge that map *after* its own literals. So a caller could set TopicArn and
// have GetTopicAttributes report it — and once the counts became derived, could have set
// SubscriptionsConfirmed to any value it liked. The derived members are written last for that reason,
// and this test is what keeps them there.
//
// The shadow is seeded through state rather than through four SetTopicAttributes calls, which is what
// this test did until #1067. All four derived names are read-only on API_GetTopicAttributes and
// API_SetTopicAttributes publishes none of them, so the handler now refuses all four and no request can
// put one in the stored map. The merge order is still the only thing standing between a stored value and
// the derived one — a record seeded by a fixture, restored from an event log, or written by a future
// handler would reach the same merge — so the property is pinned at the layer that still has it.
func TestSNSStoredAttributeCannotShadowADerivedOne(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "attrs-no-shadow")
	snsSubscribe(t, ts, snsEastRegion, arn)

	for name, value := range map[string]string{
		"TopicArn":               "arn:aws:sns:us-east-1:999999999999:someone-elses-topic",
		"Owner":                  "999999999999",
		"SubscriptionsConfirmed": "99",
		"SubscriptionsPending":   "99",
	} {
		require.NoError(t, emulator.SeedSNSTopicAttributeForTest(t.Context(), ts.StateManager(),
			taggingTestAccount, snsEastRegion, "attrs-no-shadow", name, value),
			"seed a stored %s the merge must not report", name)
	}

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode, "GetTopicAttributes succeeds")
	assert.Equal(t, arn, attrs["TopicArn"], "a stored TopicArn does not shadow the real one")
	assert.Equal(t, taggingTestAccount, attrs["Owner"], "a stored Owner does not shadow the real one")
	assert.Equal(t, "1", attrs["SubscriptionsConfirmed"],
		"the count comes from the index, not from anything a caller can write")
	assert.Equal(t, "0", attrs["SubscriptionsPending"], "a stored count does not shadow the derived one")

	keys := snsAttributeEntryKeys(t, snsGetTopicAttributesBody(t, ts, snsEastRegion, arn))
	assert.Equal(t, []string{"Owner", "SubscriptionsConfirmed", "SubscriptionsPending", "TopicArn"}, keys,
		"overwriting a derived member does not duplicate its entry")
}

// TestSNSTopicAttributesPassesStoredMembersThrough asserts the other half of the split: an attribute
// AWS publishes and substrate does not derive is reported as stored.
//
// Policy is the interesting one, and the reason it is the case here. The page publishes it and AWS's
// sample carries a default policy document, but SNS has no operation that mints one, so substrate
// reports a Policy only when something stored it — the same shape as KMS's #983, recorded as unmodelled
// in docs/services.md rather than answered with an invented document.
func TestSNSTopicAttributesPassesStoredMembersThrough(t *testing.T) {
	t.Parallel()
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "attrs-stored-passthrough")

	before, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.NotContains(t, before, "Policy", "SNS mints no default topic policy")
	assert.NotContains(t, before, "DisplayName")

	const policy = `{"Version":"2008-10-17","Statement":[]}`
	snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":         "SetTopicAttributes",
		"TopicArn":       arn,
		"AttributeName":  "Policy",
		"AttributeValue": policy,
	})
	snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":         "SetTopicAttributes",
		"TopicArn":       arn,
		"AttributeName":  "DisplayName",
		"AttributeValue": "attrs-display",
	})

	after, errCode := snsTopicAttributes(t, ts, snsEastRegion, arn)
	require.Empty(t, errCode)
	assert.Equal(t, policy, after["Policy"], "a stored Policy is reported verbatim")
	assert.Equal(t, "attrs-display", after["DisplayName"])
	assert.Equal(t, arn, after["TopicArn"], "the derived members survive a stored one arriving")
}
