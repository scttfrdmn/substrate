package emulator

import (
	"context"
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

// derivedTopicAttributes returns the four GetTopicAttributes members substrate computes from state.
//
// API_GetTopicAttributes publishes sixteen attribute names, and substrate's answer splits in two: most
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
