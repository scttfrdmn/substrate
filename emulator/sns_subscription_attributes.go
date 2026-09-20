package emulator

import (
	"context"
	"net/http"
	"slices"
	"strings"
)

// snsSettableSubscriptionAttributeNames are the six AttributeName values API_SetSubscriptionAttributes
// publishes, in the order and spelling the page lists them: five general, then one under a heading
// reading "The following attribute applies only to Amazon Data Firehose delivery stream subscriptions".
//
// Six, not seven. `ReplayPolicy` is *not* on this page — it appears on API_Subscribe, under a separate
// heading reading "The following attributes apply only to FIFO topics", beside a `ReplayStatus` that is
// not on this page either. So a replay policy is something SNS takes at subscribe time and this
// operation does not publish a way to change, and substrate refuses it here for the reason #671
// settled: only what the API model states, never a name borrowed by analogy from a sibling operation.
// The `ReplayLimitExceeded`/403 this page does publish is not evidence to the contrary — an error
// shared with Subscribe says nothing about which AttributeName values this operation accepts.
//
// The Firehose-only qualification is recorded and not enforced. `SubscriptionRoleArn` is accepted on a
// subscription of any protocol, because enforcing it would mean deciding what SNS does with a role ARN
// set on an `sqs` subscription, and the page states only that the attribute "applies only to" Firehose
// — it publishes no error for setting it elsewhere. Refusing on an unpublished reading would invent a
// refusal; accepting and storing it records the caller's intent, which is the boundary CLAUDE.md draws.
var snsSettableSubscriptionAttributeNames = []string{ //nolint:gochecknoglobals // read-only reference data, stated once so a test can assert it
	"DeliveryPolicy",
	"FilterPolicy",
	"FilterPolicyScope",
	"RawMessageDelivery",
	"RedrivePolicy",

	"SubscriptionRoleArn",
}

// snsDerivedSubscriptionAttributeNames are the seven GetSubscriptionAttributes members substrate
// projects from the subscription record rather than reading out of its stored attribute map.
//
// Five are read-only members API_GetSubscriptionAttributes publishes and
// API_SetSubscriptionAttributes does not, so no request can put one in the stored map. Two —
// `Protocol` and `Endpoint` — are not in the Get page's enumeration at all; they are kept because that
// enumeration is explicitly open ("Attributes in this map include the following"), because both are
// Subscribe request parameters a caller has no other way to read back for a single subscription, and
// because [SNSPlugin.buildSubscriptionListResponse] already reports both for the same record. Dropping
// them to match the page's list literally would make the two readers disagree about one subscription.
//
// The list is stated once so [SNSPlugin.derivedSubscriptionAttributes] can be asserted to return
// exactly these keys, and so a test can name the members a stored attribute must not shadow without
// restating them. Per-member provenance is on that function.
var snsDerivedSubscriptionAttributeNames = []string{ //nolint:gochecknoglobals // read-only reference data, stated once so a test can assert it
	"ConfirmationWasAuthenticated",
	"Endpoint",
	"Owner",
	"PendingConfirmation",
	"Protocol",
	"SubscriptionArn",
	"TopicArn",
}

// snsSubscriptionAttributeIsSettable reports whether API_SetSubscriptionAttributes publishes name as an
// attribute a caller may set.
//
// An allowlist rather than a denylist of the derived names, for the reason
// [snsSettableTopicAttributeNames] records at length for topics: the two pages do not partition one
// vocabulary, so a denylist still accepts every name neither page publishes. Here that would have left
// `EffectiveDeliveryPolicy` settable — the policy SNS *computes* from the one a caller set — and left
// `ReplayPolicy` and `ReplayStatus` settable through the operation that publishes neither.
//
// The comparison is case-sensitive, matching [snsTopicAttributeIsSettable] and for the same reason: the
// page's list is a list of literal names, and no query parameter is normalized anywhere in this plugin.
func snsSubscriptionAttributeIsSettable(name string) bool {
	return slices.Contains(snsSettableSubscriptionAttributeNames, name)
}

// snsParseSubscriptionARN checks that arn has the shape of an SNS subscription ARN, returning the
// account and Region its segments name.
//
// The mirror of [snsParseTopicARN], and split out for the same reason: the account and Region come from
// the ARN structurally rather than by each handler remembering to read them. The two parsers are
// complements — an SNS ARN's resource portion is a topic name with no separator, or that name followed
// by ":{subscription-id}" — so the colon that makes [snsParseTopicARN] refuse an ARN is the colon this
// one requires. Neither accepts what the other does.
//
// A malformed ARN is InvalidParameter/400, which API_SetSubscriptionAttributes and
// API_GetSubscriptionAttributes both publish. Before #1125 there was no site for it on either: the
// handlers built a state key from the string and a missing record answered NotFound/404, so a caller
// who sent a topic ARN — or a typo — was told the subscription did not exist rather than that the ARN
// was not one. The message text is substrate's; the reference publishes codes and not messages.
//
// The subscription *id* is deliberately not validated beyond being non-empty. It is minted by
// [generateSNSSubID] as random hex, and a record is found by the whole ARN rather than by its
// segments, so a shape rule about the id would refuse ARNs substrate itself never issued without
// making any lookup more accurate.
func snsParseSubscriptionARN(arn string) (accountID, region string, err *AWSError) {
	// arn:aws:sns:{region}:{account}:{topic-name}:{subscription-id}
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 || parts[0] != "arn" || parts[2] != "sns" {
		return "", "", snsInvalidARN(arn, "not an SNS subscription ARN")
	}
	switch {
	case parts[3] == "":
		return "", "", snsInvalidARN(arn, "names no Region")
	case parts[4] == "":
		return "", "", snsInvalidARN(arn, "names no account")
	case parts[5] == "":
		return "", "", snsInvalidARN(arn, "names no topic")
	case parts[6] == "":
		return "", "", snsInvalidARN(arn, "names no subscription")
	}
	return parts[4], parts[3], nil
}

// requireSubscription loads the subscription an ARN names, refusing an ARN that is not one and an ARN
// that names none.
//
// The two refusals are ordered shape-then-existence, so a string that is not a subscription ARN cannot
// be reported as a subscription that does not exist. That ordering is the whole reason the helper
// exists rather than the check being inlined twice.
//
// The record is loaded under the *caller's* account and Region and not the ARN's, which is what the six
// other readers and writers of the subscription indexes do — see the comment in [SNSPlugin.subscribe],
// which records that a subscription ARN's account segment is the topic's while the record and both
// indexes stay keyed by the subscriber. Keying this lookup by the ARN's segments instead would make
// GetSubscriptionAttributes miss a record ListSubscriptions reports, for a subscription to a
// cross-account topic. The parsed segments are therefore checked and discarded, which is deliberate:
// the shape is what the published InvalidParameter is about, not the routing.
func (p *SNSPlugin) requireSubscription(ctx *RequestContext, arn string) (*SNSSubscription, error) {
	if _, _, arnErr := snsParseSubscriptionARN(arn); arnErr != nil {
		return nil, arnErr
	}
	sub, err := p.loadSub(context.Background(), ctx.AccountID, ctx.Region, arn)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, &AWSError{
			Code:       "NotFound",
			Message:    "no subscription found for the ARN " + arn,
			HTTPStatus: http.StatusNotFound,
		}
	}
	return sub, nil
}

// derivedSubscriptionAttributes returns the seven GetSubscriptionAttributes members substrate projects
// from the subscription record.
//
// API_GetSubscriptionAttributes publishes twelve names and substrate's answer splits in two: six are
// whatever SetSubscriptionAttributes stored, and these are facts about the subscription that no caller
// sets. The argument for each value, since a derived member that cannot be argued for is an invention:
//
// Owner is the record's own AccountID, which [SNSPlugin.subscribe] sets from the caller — the account
// that made the subscription, which is what the page's "The AWS account ID of the subscription's owner"
// names. It is also the account the record is keyed by, so Owner and the record's location cannot
// disagree, and it is the value [SNSPlugin.buildSubscriptionListResponse] already reports as a
// subscription's Owner. For a subscription to a cross-account topic it differs from the ARN's own
// account segment, which is the topic's; that is the asymmetry subscribe's comment records, not a
// discrepancy introduced here.
//
// PendingConfirmation is "false" because substrate mints no unconfirmed subscription. There is no
// ConfirmSubscription operation, and Subscribe returns a real subscription ARN rather than the "pending
// confirmation" string API_Subscribe documents for one awaiting it. So the value is not a placeholder —
// it is the only value this emulator can have, the same reasoning that makes SubscriptionsPending "0"
// in sns_topic_attributes.go.
//
// ConfirmationWasAuthenticated is "true", and follows from the line above rather than standing on its
// own. The page defines it as "true if the subscription confirmation request was authenticated"; a
// substrate subscription is confirmed at creation by the authenticated Subscribe call itself and never
// by an out-of-band token, which is the case a signed API call produces on AWS. A subscription that
// never needed a token cannot have been confirmed by an unauthenticated one, so the two members agree
// by construction rather than by two separate decisions.
//
// SubscriptionArn and TopicArn are the record's own fields, and Protocol and Endpoint are the Subscribe
// parameters it stored — see [snsDerivedSubscriptionAttributeNames] for why the latter two are reported
// though the page's list omits them.
//
// EffectiveDeliveryPolicy is deliberately absent. The page defines it as the policy "that takes into
// account the topic delivery policy and account system defaults", and substrate models no account
// system defaults and no merge of the two policies. Reporting the subscription's own DeliveryPolicy
// under the name would claim a computation that did not happen, and synthesizing defaults would be
// substrate's invention rather than its reading, so the member is omitted — the honest-empty rule
// (#827). The same goes for ReplayStatus, which no operation substrate routes can set.
func (p *SNSPlugin) derivedSubscriptionAttributes(sub *SNSSubscription) map[string]string {
	return map[string]string{
		"ConfirmationWasAuthenticated": "true",
		"Endpoint":                     sub.Endpoint,
		"Owner":                        sub.AccountID,
		"PendingConfirmation":          "false",
		"Protocol":                     sub.Protocol,
		"SubscriptionArn":              sub.ARN,
		"TopicArn":                     sub.TopicARN,
	}
}

// subscriptionAttributes merges a subscription's stored attributes with its derived members, derived
// last.
//
// The order is the guard [SNSPlugin.getTopicAttributes] documents for topics: a derived member is a
// fact about the subscription, so it wins over anything found in the stored map. Since
// setSubscriptionAttributes refuses all five of the read-only names the Get page publishes, no request
// can put one there — this is defense in depth, and the only guard for a value that reached the map
// another way, such as a record written by an older build.
func (p *SNSPlugin) subscriptionAttributes(sub *SNSSubscription) map[string]string {
	attrs := make(map[string]string, len(sub.Attributes)+len(snsDerivedSubscriptionAttributeNames))
	for k, v := range sub.Attributes {
		attrs[k] = v
	}
	for k, v := range p.derivedSubscriptionAttributes(sub) {
		attrs[k] = v
	}
	return attrs
}
