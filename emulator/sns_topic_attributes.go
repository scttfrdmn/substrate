package emulator

import (
	"context"
	"slices"
	"strconv"
)

// snsDerivedTopicAttributeNames are the GetTopicAttributes members substrate derives at read time
// rather than reading out of the topic record's stored attribute map.
//
// It exists so the set is stated once and can be asserted against: [SNSPlugin.derivedTopicAttributes]
// returns exactly these keys, and a test can name the members a stored attribute must not be able to
// shadow without restating the list.
var snsDerivedTopicAttributeNames = []string{ //nolint:gochecknoglobals // read-only reference data, stated once so a test can assert it
	"Owner",
	"SubscriptionsConfirmed",
	"SubscriptionsPending",
	"TopicArn",
}

// snsSettableTopicAttributeNames are the twenty-five AttributeName values API_SetTopicAttributes
// publishes, in the order and spelling the page lists them: five general, fifteen delivery-status names
// in five endpoint families, two under server-side encryption, and three under FIFO topics.
//
// The check it backs is an allowlist and not a denylist of the names GetTopicAttributes derives,
// because the two pages do not partition one vocabulary. Set publishes 25 names, Get publishes 17, only
// 9 appear on both, and the union is 33. A denylist built from [snsDerivedTopicAttributeNames] would
// have covered 4 of the 8 names Get publishes and Set does not, so a caller could still have written —
// and then read back — an EffectiveDeliveryPolicy, a SubscriptionsDeleted, a BeginningArchiveTime or a
// FifoTopic, each of which AWS reports as a fact about the topic rather than as anything a caller sets.
// And a denylist of any length still accepts a name neither page publishes at all, which is the whole
// of #1067: setTopicAttributes wrote req.Params["AttributeName"] into the topic record with no check
// whatsoever, so `AttributeName=Banana` was stored and GetTopicAttributes reported it back.
//
// Sixteen of these 25 are absent from Get's 17 — the fifteen delivery-status names and
// FifoThroughputScope — which is a property of AWS's own pages rather than an omission in either.
// Substrate accepts all 25 and getTopicAttributes reports the whole stored map, so it reports those
// sixteen back where AWS would not; that divergence is recorded in docs/services.md rather than papered
// over by filtering the read, because the value a caller set is real and hiding it would make
// SetTopicAttributes look like a no-op.
var snsSettableTopicAttributeNames = []string{ //nolint:gochecknoglobals // read-only reference data, stated once so a test can assert it
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

// snsTopicAttributeIsSettable reports whether API_SetTopicAttributes publishes name as an attribute a
// caller may set.
//
// The comparison is case-sensitive, because AWS's list is a list of literal names and a query parameter
// is not normalised anywhere else in this plugin either.
func snsTopicAttributeIsSettable(name string) bool {
	return slices.Contains(snsSettableTopicAttributeNames, name)
}

// derivedTopicAttributes returns the four GetTopicAttributes members substrate computes from state.
//
// API_GetTopicAttributes publishes seventeen attribute names, and substrate's answer splits in two: most
// are whatever CreateTopic or SetTopicAttributes stored, and these four are facts about the topic that
// no caller sets. Until #993 the split was wrong in both directions — the handler reported a
// SubscriptionsCount member the page does not publish anywhere, hardcoded to "0", and reported none of
// the three subscription counts the page does publish.
//
// SubscriptionsConfirmed is the length of the per-topic subscription index, read under the *caller's*
// account and Region. That keying is the one decision this function makes, and it is made to match the
// six other readers and writers of that index (SNSPlugin.subscribe, unsubscribe,
// listSubscriptionsByTopic, publish and publishBatch), which all key by the caller while the topic
// record itself is keyed by the ARN's own account and Region — see the comment in subscribe, which
// records that deliberately. Keying the count by the ARN's target instead would make
// GetTopicAttributes and ListSubscriptionsByTopic disagree about the same topic by construction: the
// count would read 0 for a cross-account ARN while the list returned the subscriptions. Reading the
// same index under the same key means they cannot.
//
// SubscriptionsPending is "0" because substrate mints no unconfirmed subscription: there is no
// ConfirmSubscription operation, and Subscribe hands back a real subscription ARN rather than the
// "pending confirmation" string API_Subscribe documents for a subscription awaiting it. So the value
// is not a placeholder — it is the only count this emulator can have.
//
// SubscriptionsDeleted is deliberately absent. Unsubscribe deletes the record and filters both indexes
// rather than tombstoning, and SNSSubscription carries no status, so nothing tracks a deletion. A
// monotonic counter would be cheap, but the page publishes only "The number of deleted subscriptions
// for the topic" and says nothing about how long a deleted subscription stays counted, so a
// never-decaying counter would be substrate's invention rather than its reading. Omitting the member
// claims nothing, which is the honest-empty rule (#827).
//
// Owner is the topic record's own AccountID, which is by construction the account segment of the ARN
// the record is keyed by, because requireTopic builds the state key from that segment. It matches the
// Owner entry in the page's sample response.
//
// Policy is not derived. The page publishes it and substrate reports it only if SetTopicAttributes
// stored one, because SNS has no operation that mints a default topic policy — the same shape as KMS's
// #983, and recorded as unmodelled in docs/services.md rather than answered with an invented document.
func (p *SNSPlugin) derivedTopicAttributes(
	goCtx context.Context, ctx *RequestContext, t *SNSTopic, topicName string,
) (map[string]string, error) {
	subARNs, err := p.loadSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, topicName))
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"Owner":                  t.AccountID,
		"SubscriptionsConfirmed": strconv.Itoa(len(subARNs)),
		"SubscriptionsPending":   "0",
		"TopicArn":               t.ARN,
	}, nil
}
