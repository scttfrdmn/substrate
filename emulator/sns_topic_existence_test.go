package emulator_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Four SNS operations never loaded the topic they addressed (#926), and one loaded it and refused where
// AWS says it must not (#992).
//
// Subscribe, Publish, PublishBatch and ListSubscriptionsByTopic each parsed the TopicArn, derived a
// state key from it, and then read only the *subscription index* — so an absent topic was
// indistinguishable from a real topic with no subscribers, and all four answered 200. Every one of the
// four pages publishes NotFound/404. The consequence is not just a wrong code: a consumer's error path
// for a deleted or misnamed topic was unreachable, so the CreateTopic → Publish → DeleteTopic → Publish
// sequence a producer's teardown test would assert on could not be written. Subscribe is worse than a
// false success because it *writes*: an orphan subscription record and two index entries survived it,
// reported by ListSubscriptions ever after.
//
// DeleteTopic is the same audit in the other direction. It refused an absent topic where
// API_DeleteTopic's description states "this action is idempotent, so deleting a topic that does not
// exist does not result in an error" — so a teardown that deletes unconditionally was broken here and
// correct in production.
//
// Every assertion goes over the wire through [emulator.StartTestServer] and every topic is created by
// CreateTopic (#765), and each asserts the HTTP status alongside the code (#923). The status matters
// more than usual here, because the defect being fixed *was* a status: 200 where 404 belongs.

// snsAbsentTopicARN is a well-formed topic ARN in the test account and Region that names no topic. It
// is well-formed deliberately: a malformed ARN is refused by the parser before any load, so it would
// prove nothing about the existence check.
func snsAbsentTopicARN(region string) string {
	return "arn:aws:sns:" + region + ":" + taggingTestAccount + ":no-such-topic"
}

// snsTopicOperation is one operation that must refuse an absent topic, with the parameters it needs
// beyond TopicArn.
type snsTopicOperation struct {
	action string
	extra  map[string]string
}

// snsTopicOperations is every operation that resolves a TopicArn to a topic record. All six answer
// NotFound/404 for a well-formed ARN naming no topic; the two attribute operations already did, and are
// listed so a later change cannot quietly drop one of them from the set.
//
// DeleteTopic is absent on purpose — it is the operation AWS documents as idempotent, asserted by
// [TestSNSExistence_DeleteTopicIsIdempotent] in the opposite direction.
var snsTopicOperations = []snsTopicOperation{
	{action: "GetTopicAttributes"},
	{action: "SetTopicAttributes", extra: map[string]string{"AttributeName": "DisplayName", "AttributeValue": "x"}},
	{action: "Subscribe", extra: map[string]string{"Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:inbox"}},
	{action: "Publish", extra: map[string]string{"Message": "hello"}},
	{action: "PublishBatch", extra: map[string]string{
		"PublishBatchRequestEntries.member.1.Id":      "msg1",
		"PublishBatchRequestEntries.member.1.Message": "hello",
	}},
	{action: "ListSubscriptionsByTopic"},
}

// snsTopicOperationParams builds the request parameters for one operation against a topic ARN.
func snsTopicOperationParams(op snsTopicOperation, topicARN string) map[string]string {
	params := map[string]string{"Action": op.action, "TopicArn": topicARN}
	for k, v := range op.extra {
		params[k] = v
	}
	return params
}

// TestSNSExistence_EveryTopicOperationRefusesAnAbsentTopic is the whole of #926 in one table.
//
// Each of the six publishes NotFound at HTTP 404, glossed "Indicates that the requested resource does
// not exist." Four of them answered 200 with a success body: Subscribe handed back a subscription ARN,
// Publish and PublishBatch minted MessageIds, and ListSubscriptionsByTopic reported an empty list.
func TestSNSExistence_EveryTopicOperationRefusesAnAbsentTopic(t *testing.T) {
	ts := snsTagServer(t)
	absent := snsAbsentTopicARN(snsEastRegion)

	for _, op := range snsTopicOperations {
		t.Run(op.action, func(t *testing.T) {
			status, body, errCode := snsQuery(t, ts, snsEastRegion, snsTopicOperationParams(op, absent))
			assert.Equal(t, "NotFound", errCode, "%s on an absent topic (body %s)", op.action, body)
			assert.Equal(t, http.StatusNotFound, status, "SNS publishes NotFound at HTTP 404")
		})
	}
}

// TestSNSExistence_AnExistingTopicIsStillServed is the other half of the same table, and the one that
// would catch a check that refuses everything.
//
// It also pins the pair the defect collapsed: ListSubscriptionsByTopic against a real topic with no
// subscribers answers 200 with an empty list, which is now a *different* answer from the absent topic's
// 404 rather than the same one.
func TestSNSExistence_AnExistingTopicIsStillServed(t *testing.T) {
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "live-topic")

	for _, op := range snsTopicOperations {
		t.Run(op.action, func(t *testing.T) {
			status, _, errCode := snsQuery(t, ts, snsEastRegion, snsTopicOperationParams(op, topicARN))
			assert.Empty(t, errCode, "%s on a topic that exists", op.action)
			assert.Equal(t, http.StatusOK, status, "%s on a topic that exists", op.action)
		})
	}

	// A fresh topic, because the loop above ran Subscribe against the first one.
	t.Run("an empty subscription list is not a refusal", func(t *testing.T) {
		bare := snsCreateTopic(t, ts, "no-subscribers")
		body := snsQueryOK(t, ts, snsEastRegion, map[string]string{
			"Action":   "ListSubscriptionsByTopic",
			"TopicArn": bare,
		})
		var doc struct {
			Members []struct {
				ARN string `xml:"SubscriptionArn"`
			} `xml:"ListSubscriptionsByTopicResult>Subscriptions>member"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc), "decode ListSubscriptionsByTopic (body %s)", body)
		assert.Empty(t, doc.Members, "a topic with no subscribers reports an empty list, not a 404")
	})
}

// TestSNSExistence_PublishAfterDeleteTopicIsRefused is the concrete sequence #926 names, and the reason
// the defect mattered beyond the code name: a test asserting that a producer surfaces an error once its
// topic is torn down could not be written, because the second Publish succeeded and reported a
// MessageId.
func TestSNSExistence_PublishAfterDeleteTopicIsRefused(t *testing.T) {
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "transient-topic")

	firstBody := snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":   "Publish",
		"TopicArn": topicARN,
		"Message":  "before the teardown",
	})
	assert.Contains(t, firstBody, "<MessageId>", "the first Publish succeeds")

	snsQueryOK(t, ts, snsEastRegion, map[string]string{"Action": "DeleteTopic", "TopicArn": topicARN})

	status, body, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":   "Publish",
		"TopicArn": topicARN,
		"Message":  "after the teardown",
	})
	assert.Equal(t, "NotFound", errCode, "Publish after DeleteTopic")
	assert.Equal(t, http.StatusNotFound, status, "Publish after DeleteTopic")
	assert.NotContains(t, body, "<MessageId>", "the refusal reports no MessageId")
}

// TestSNSExistence_SubscribeToAnAbsentTopicWritesNothing is the orphan-write case, and the assertion
// that failed hardest before the fix.
//
// Subscribe wrote three things without ever loading the topic: the subscription record, the
// account-wide index entry and the per-topic index entry. All three survived, so ListSubscriptions
// reported a subscription to a topic that had never existed — and had a topic of that name later been
// created, it would have started receiving messages addressed to it before it existed.
func TestSNSExistence_SubscribeToAnAbsentTopicWritesNothing(t *testing.T) {
	ts := snsTagServer(t)
	absent := snsAbsentTopicARN(snsEastRegion)

	status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": absent,
		"Protocol": "sqs",
		"Endpoint": "arn:aws:sqs:" + snsEastRegion + ":" + taggingTestAccount + ":inbox",
	})
	require.Equal(t, "NotFound", errCode, "Subscribe to an absent topic")
	require.Equal(t, http.StatusNotFound, status, "Subscribe to an absent topic")

	body := snsQueryOK(t, ts, snsEastRegion, map[string]string{"Action": "ListSubscriptions"})
	assert.NotContains(t, body, "no-such-topic",
		"the refused Subscribe left no subscription record and no index entry (body %s)", body)

	// And the per-topic index is empty too, which the account-wide listing above cannot show: creating
	// the topic afterwards must not hand it a subscription it never accepted.
	created := snsCreateTopic(t, ts, "no-such-topic")
	require.Equal(t, absent, created, "CreateTopic mints the ARN the refused Subscribe named")
	byTopic := snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":   "ListSubscriptionsByTopic",
		"TopicArn": created,
	})
	assert.NotContains(t, byTopic, "<SubscriptionArn>",
		"a topic created after the refused Subscribe has no subscribers (body %s)", byTopic)
}

// TestSNSExistence_TheCheckIsKeyedByTheARNsAccountAndRegion is #926's fourth criterion: the existence
// check must not reintroduce the resolution defect #925 fixed.
//
// The load is keyed by the ARN's own account and Region, so a same-named topic in the caller's own
// Region cannot satisfy a Publish addressed elsewhere. API_Publish requires the Region half
// independently — "You can publish messages only to topics and endpoints in the same AWS Region" — and
// the account half falls out of the state key rather than needing its own guard, which is the
// arrangement that makes isolation emergent rather than remembered.
func TestSNSExistence_TheCheckIsKeyedByTheARNsAccountAndRegion(t *testing.T) {
	ts := snsTagServer(t)

	// One topic named "shared", in us-west-2 only.
	westARN := snsCreateTopicIn(t, ts, snsWestRegion, "shared", nil)
	eastARN := strings.Replace(westARN, snsWestRegion, snsEastRegion, 1)
	foreignARN := strings.Replace(westARN, taggingTestAccount, "210987654321", 1)
	require.NotEqual(t, westARN, eastARN, "the two ARNs differ in Region only")
	require.NotEqual(t, westARN, foreignARN, "the two ARNs differ in account only")

	for _, op := range snsTopicOperations {
		t.Run(op.action, func(t *testing.T) {
			// The ARN that names the real topic is served, whichever endpoint asks.
			status, _, errCode := snsQuery(t, ts, snsEastRegion, snsTopicOperationParams(op, westARN))
			assert.Empty(t, errCode, "%s on the us-west-2 topic's own ARN", op.action)
			assert.Equal(t, http.StatusOK, status, "%s on the us-west-2 topic's own ARN", op.action)

			// A same-named ARN in another Region is not that topic.
			status, _, errCode = snsQuery(t, ts, snsEastRegion, snsTopicOperationParams(op, eastARN))
			assert.Equal(t, "NotFound", errCode, "%s on a same-named us-east-1 ARN", op.action)
			assert.Equal(t, http.StatusNotFound, status, "%s on a same-named us-east-1 ARN", op.action)

			// Nor is a same-named ARN in another account.
			status, _, errCode = snsQuery(t, ts, snsWestRegion, snsTopicOperationParams(op, foreignARN))
			assert.Equal(t, "NotFound", errCode, "%s on a same-named ARN in another account", op.action)
			assert.Equal(t, http.StatusNotFound, status, "%s on a same-named ARN in another account", op.action)
		})
	}
}

// TestSNSExistence_AMalformedARNIsStillInvalidParameter guards the refusal the new check sits behind.
//
// The existence check runs after the parse, so a string that is not an ARN must still answer
// InvalidParameter/400 rather than being reported as a topic that does not exist. The two are different
// facts and #925 established the code for the first of them.
func TestSNSExistence_AMalformedARNIsStillInvalidParameter(t *testing.T) {
	ts := snsTagServer(t)

	for _, malformed := range []string{
		"not-an-arn",
		"arn:aws:sns",
		"arn:aws:sqs:" + snsEastRegion + ":" + taggingTestAccount + ":queue",
		"arn:aws:sns:" + snsEastRegion + ":" + taggingTestAccount + ":topic:sub-id",
	} {
		for _, op := range snsTopicOperations {
			t.Run(op.action+"/"+malformed, func(t *testing.T) {
				status, _, errCode := snsQuery(t, ts, snsEastRegion, snsTopicOperationParams(op, malformed))
				assert.Equal(t, "InvalidParameter", errCode, "%s on %q", op.action, malformed)
				assert.Equal(t, http.StatusBadRequest, status, "%s on %q", op.action, malformed)
			})
		}
	}
}

// TestSNSExistence_PublishBatchRefusesAtTheTopLevelRatherThanPerEntry sources the one question #926
// left open, and asserts the shape rather than only the code.
//
// API_PublishBatch has two failure shapes — a top-level error and a per-entry BatchResultErrorEntry in
// a 200 body — and the issue declined to guess. TopicArn is a request-level parameter, one per call, so
// every entry addresses the same topic and a per-entry rendering would report the identical failure on
// each of them behind a 200. NotFound appears in the operation's top-level Errors list, and the batch
// codes it publishes are about individual messages.
//
// A three-entry batch is sent so that a per-entry implementation would be visible: it would answer 200
// with three Failed members. The assertion is that the body carries no batch result at all.
func TestSNSExistence_PublishBatchRefusesAtTheTopLevelRatherThanPerEntry(t *testing.T) {
	ts := snsTagServer(t)

	params := map[string]string{
		"Action":   "PublishBatch",
		"TopicArn": snsAbsentTopicARN(snsEastRegion),
	}
	for _, id := range []string{"1", "2", "3"} {
		params["PublishBatchRequestEntries.member."+id+".Id"] = "msg" + id
		params["PublishBatchRequestEntries.member."+id+".Message"] = "body " + id
	}

	status, body, errCode := snsQuery(t, ts, snsEastRegion, params)
	assert.Equal(t, "NotFound", errCode, "PublishBatch on an absent topic")
	assert.Equal(t, http.StatusNotFound, status, "the refusal is top-level, so it carries the 404")
	assert.NotContains(t, body, "<Successful>", "no entry succeeded (body %s)", body)
	assert.NotContains(t, body, "<Failed>", "the refusal is not rendered as per-entry failures (body %s)", body)
	assert.NotContains(t, body, "<MessageId>", "no MessageId was minted (body %s)", body)
}

// TestSNSExistence_UnsubscribeRefusesAnAbsentSubscription covers the half of #926 that is about a
// subscription rather than a topic.
//
// Substrate answered 200 behind an unsourced "Idempotent — silently succeed" comment. API_Unsubscribe
// publishes NotFound/404 and states no idempotence, and SubscriptionArn is the only resource the
// operation names, so the published code can only be about the subscription. Contrast DeleteTopic,
// which AWS *does* document as idempotent (#992) — the asymmetry is AWS's, asserted in both directions
// here and in [TestSNSExistence_DeleteTopicIsIdempotent].
func TestSNSExistence_UnsubscribeRefusesAnAbsentSubscription(t *testing.T) {
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "unsub-topic")
	subARN := snsSubscribe(t, ts, snsEastRegion, topicARN)

	t.Run("an ARN naming no subscription", func(t *testing.T) {
		status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
			"Action":          "Unsubscribe",
			"SubscriptionArn": topicARN + ":00000000-0000-0000-0000-000000000000",
		})
		assert.Equal(t, "NotFound", errCode, "Unsubscribe on an absent subscription")
		assert.Equal(t, http.StatusNotFound, status, "SNS publishes NotFound at HTTP 404")
	})

	t.Run("the second Unsubscribe of one subscription", func(t *testing.T) {
		snsQueryOK(t, ts, snsEastRegion, map[string]string{"Action": "Unsubscribe", "SubscriptionArn": subARN})

		status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
			"Action":          "Unsubscribe",
			"SubscriptionArn": subARN,
		})
		assert.Equal(t, "NotFound", errCode, "the subscription is gone after the first call")
		assert.Equal(t, http.StatusNotFound, status, "the subscription is gone after the first call")
	})
}

// TestSNSExistence_DeleteTopicIsIdempotent is #992, the inverse case: the one topic operation that must
// *not* refuse an absent topic.
//
// API_DeleteTopic's description, verbatim: "This action is idempotent, so deleting a topic that does not
// exist does not result in an error." The same page's Errors list publishes NotFound/404 all the same,
// so the page contradicts itself; the sentence governs because it names this condition and this outcome
// while the error-list entry names no condition at all. Substrate answered the 404, so a teardown that
// deletes unconditionally — or a retried delete — failed here and succeeded in production.
func TestSNSExistence_DeleteTopicIsIdempotent(t *testing.T) {
	ts := snsTagServer(t)

	t.Run("a topic that never existed", func(t *testing.T) {
		status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
			"Action":   "DeleteTopic",
			"TopicArn": snsAbsentTopicARN(snsEastRegion),
		})
		assert.Empty(t, errCode, "DeleteTopic on a topic that does not exist")
		assert.Equal(t, http.StatusOK, status, "AWS documents the action as idempotent")
	})

	t.Run("the same topic twice", func(t *testing.T) {
		topicARN := snsCreateTopic(t, ts, "delete-me-twice")
		params := map[string]string{"Action": "DeleteTopic", "TopicArn": topicARN}
		snsQueryOK(t, ts, snsEastRegion, params)
		snsQueryOK(t, ts, snsEastRegion, params)
	})

	// The idempotent path must write nothing rather than rewrite the owning Region's index to itself,
	// which is the one way a no-op delete could still be observable.
	t.Run("an unrelated topic in the same Region survives", func(t *testing.T) {
		keep := snsCreateTopic(t, ts, "keep-me")
		snsQueryOK(t, ts, snsEastRegion, map[string]string{
			"Action":   "DeleteTopic",
			"TopicArn": snsAbsentTopicARN(snsEastRegion),
		})
		assert.Contains(t, snsListTopicARNs(t, ts, snsEastRegion), keep,
			"deleting an absent topic left the Region's topic index alone")
	})

	t.Run("a malformed ARN is still refused", func(t *testing.T) {
		status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
			"Action":   "DeleteTopic",
			"TopicArn": "not-an-arn",
		})
		assert.Equal(t, "InvalidParameter", errCode,
			"idempotence licenses a topic that does not exist, not a string that is not an ARN")
		assert.Equal(t, http.StatusBadRequest, status, "a malformed ARN is InvalidParameter/400")
	})
}

// TestSNSExistence_TheRefusalNamesTheARN checks the message a caller reads, since #926's whole point is
// that the caller could not tell an absent topic from an empty one.
//
// The message text is substrate's — SNS's reference publishes codes and not messages — but naming the
// ARN is what makes a misnamed topic diagnosable from the response alone, and it is the same shape
// [snsTopicNotFound] uses for the tag operations so the two read alike.
func TestSNSExistence_TheRefusalNamesTheARN(t *testing.T) {
	ts := snsTagServer(t)
	absent := snsAbsentTopicARN(snsEastRegion)

	_, body, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":   "Publish",
		"TopicArn": absent,
		"Message":  "hello",
	})
	require.Equal(t, "NotFound", errCode, "Publish on an absent topic")

	var doc struct {
		Message string `xml:"Error>Message"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "decode the error document (body %s)", body)
	assert.Contains(t, doc.Message, absent, "the refusal names the ARN that resolved to nothing")
}

// TestSNSExistence_OneHelperAnswersForEverySite is the drift guard, on the #961/#969 precedent: those
// issues each found one operation refusing nothing that its siblings refused, which is what a
// per-handler existence check invites. All six route through SNSPlugin.requireTopic, so the code, the
// status and the message text are identical across them — and a seventh operation added later that
// hand-rolls its own check shows up here as a difference.
func TestSNSExistence_OneHelperAnswersForEverySite(t *testing.T) {
	ts := snsTagServer(t)
	absent := snsAbsentTopicARN(snsEastRegion)

	messages := map[string]string{}
	for _, op := range snsTopicOperations {
		_, body, errCode := snsQuery(t, ts, snsEastRegion, snsTopicOperationParams(op, absent))
		require.Equal(t, "NotFound", errCode, "%s on an absent topic", op.action)

		var doc struct {
			Message string `xml:"Error>Message"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc), "decode %s error (body %s)", op.action, body)
		messages[op.action] = doc.Message
	}

	require.Len(t, messages, len(snsTopicOperations), "every operation answered")
	var first string
	for _, op := range snsTopicOperations {
		if first == "" {
			first = messages[op.action]
			continue
		}
		assert.Equal(t, first, messages[op.action],
			"%s answers the same refusal as its siblings, from one helper", op.action)
	}
}
