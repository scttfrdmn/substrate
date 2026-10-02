package emulator_test

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for SNS's two records (#756).
//
// SNSTopic and SNSSubscription declare AccountID and Region under wire-visible `json` tags, and the
// topic also declares EverTagged as `ever_tagged`, because the record is what MemoryStateManager
// snapshots and a replay reads back. None reaches a body. emulator/sns_types.go carries no `xml` tag;
// every snsXMLResponse site is handed a response struct declared for its operation; the two list
// responses build entry structs member by member; and Publish's delivery envelope is a map built
// member by member too. So SNS was already projected in code, as Redshift and EC2 were, and what it
// lacked was this file.
//
// # Why the attribute keys are walked as well as the element names
//
// GetTopicAttributes and GetSubscriptionAttributes answer a string map, rendered as
// `<entry><key>…</key><value>…</value></entry>`. A member leaked into that map would arrive as the
// *text* of a `<key>`, under an element named `key`, and an element walk alone would pass it. The
// attributes are built from the record field by field, which is exactly where a later edit could add
// one, so the walk reads `<key>` text too. A control run confirms it is needed.

// snsWireClock is the instant every fixture below starts the simulated clock at. No assertion
// equates a timestamp; this file only walks member names.
var snsWireClock = time.Unix(1700000000, 0).UTC()

// snsWireAccount and snsWireRegion scope every state key the plugin writes.
const (
	snsWireAccount = "123456789012"
	snsWireRegion  = "us-east-1"
)

// snsBookkeepingMembers are the members SNS's records declare and no SNS shape publishes. The fold
// in wireAssertNoMemberXML covers any capitalization; `ever_tagged` is listed in its own right
// because a fold does not reach a snake_case spelling.
var snsBookkeepingMembers = []string{"AccountID", "Region", "EverTagged", "ever_tagged"}

// setupSNSWirePlugin returns the SNS plugin, a request context and the state manager behind it.
//
// No registry is configured, so a publish delivers nowhere: delivery is not what this file asserts,
// and the plugin skips fan-out without one.
func setupSNSWirePlugin(t *testing.T) (*emulator.SNSPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.SNSPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(snsWireClock)},
	}), "emulator.SNSPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: snsWireAccount,
		Region:    snsWireRegion,
		RequestID: "req-sns-wire",
		IDs:       emulator.NewIDMint("req-sns-wire"),
	}, state
}

// snsWire issues one query-protocol action and returns the raw response body, failing the test on
// anything but 200.
func snsWire(t *testing.T, p *emulator.SNSPlugin, ctx *emulator.RequestContext, action string, params map[string]string) []byte {
	t.Helper()
	full := map[string]string{"Action": action, "Version": "2010-03-31"}
	maps.Copy(full, params)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "sns",
		Operation: action,
		Path:      "/",
		Params:    full,
	})
	require.NoError(t, err, "%s", action)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return resp.Body
}

// snsWireElement returns the text of the first element named name in body.
func snsWireElement(t *testing.T, action string, body []byte, name string) string {
	t.Helper()
	m := regexp.MustCompile(`<` + name + `>([^<]+)</` + name + `>`).FindSubmatch(body)
	require.NotNilf(t, m, "%s answered no <%s>: %s", action, name, body)
	return string(m[1])
}

// snsWireRecord returns the record at key as raw JSON, so a member with no published home can be read
// without a Go type deciding which members exist.
func snsWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "sns", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// snsWireRequireScoped requires that the stored record carries both scope members — the presence
// anchor without which an absence assertion would pass without testing anything.
func snsWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	record := snsWireRecord(t, state, key)
	require.JSONEqf(t, `"`+snsWireAccount+`"`, string(record["AccountID"]),
		"%s must persist AccountID before an absence assertion on it means anything", key)
	require.JSONEqf(t, `"`+snsWireRegion+`"`, string(record["Region"]),
		"%s must persist Region before an absence assertion on it means anything", key)
}

// snsWireCase is one action driven by one of the tests below. A non-nil held reuses a response the
// test already has rather than issuing the action a second time.
type snsWireCase struct {
	action string
	params map[string]string
	held   []byte
	anchor string
}

// snsWireRun drives each case as a subtest, in order: the presence anchor first, so a body that did
// not render what it should fails as a missing anchor rather than passing as an absence, then the
// walk.
func snsWireRun(t *testing.T, p *emulator.SNSPlugin, ctx *emulator.RequestContext, cases []snsWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = snsWire(t, p, ctx, tc.action, tc.params)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.action, tc.anchor)
			wireAssertNoMemberXML(t, tc.action, body, snsBookkeepingMembers, "key")
		})
	}
}

// snsWireRequestID is the anchor of every action whose published result is empty: the envelope's
// ResponseMetadata is all it carries.
const snsWireRequestID = "<RequestId>req-sns-wire</RequestId>"

func TestSNSWire_TopicResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSNSWirePlugin(t)

	const name = "wire-topic"
	created := snsWire(t, p, ctx, "CreateTopic", map[string]string{"Name": name})
	topicARN := snsWireElement(t, "CreateTopic", created, "TopicArn")
	key := "topic:" + snsWireAccount + "/" + snsWireRegion + "/" + name
	snsWireRequireScoped(t, state, key)

	// SNSTopic's third bookkeeping member is `ever_tagged,omitempty`, set only by a tag write and never
	// by create-with-tags (#938). Until it is set, every response is missing it for free — the vacuous
	// assertion #1304 shipped on EFS — so the topic is tagged and the flag read back before any
	// response is walked.
	tagged := snsWire(t, p, ctx, "TagResource", map[string]string{
		"ResourceArn": topicARN, "Tags.member.1.Key": "team", "Tags.member.1.Value": "wire",
	})
	require.JSONEqf(t, "true", string(snsWireRecord(t, state, key)["ever_tagged"]),
		"%s must persist ever_tagged before an absence assertion on it means anything", key)

	topic := map[string]string{"TopicArn": topicARN}
	snsWireRun(t, p, ctx, []snsWireCase{
		{action: "CreateTopic", held: created, anchor: "<TopicArn>" + topicARN + "</TopicArn>"},
		{action: "TagResource", held: tagged, anchor: snsWireRequestID},
		{action: "SetTopicAttributes", params: map[string]string{
			"TopicArn": topicARN, "AttributeName": "DisplayName", "AttributeValue": "wire",
		}, anchor: snsWireRequestID},
		// After the set, so the map renders a member the request wrote as well as the defaults.
		{action: "GetTopicAttributes", params: topic, anchor: "<value>wire</value>"},
		{action: "ListTopics", anchor: "<TopicArn>" + topicARN + "</TopicArn>"},
		{action: "ListTagsForResource", params: map[string]string{"ResourceArn": topicARN},
			anchor: "<Key>team</Key>"},
		{action: "AddPermission", params: map[string]string{
			"TopicArn": topicARN, "Label": "wire", "AWSAccountId.member.1": snsWireAccount,
			"ActionName.member.1": "Publish",
		}, anchor: snsWireRequestID},
		{action: "RemovePermission", params: map[string]string{"TopicArn": topicARN, "Label": "wire"},
			anchor: snsWireRequestID},
		{action: "Publish", params: map[string]string{"TopicArn": topicARN, "Message": "wire"},
			anchor: "<MessageId>"},
		{action: "PublishBatch", params: map[string]string{
			"TopicArn":                                    topicARN,
			"PublishBatchRequestEntries.member.1.Id":      "one",
			"PublishBatchRequestEntries.member.1.Message": "wire",
		}, anchor: "<Id>one</Id>"},
		{action: "UntagResource", params: map[string]string{"ResourceArn": topicARN, "TagKeys.member.1": "team"},
			anchor: snsWireRequestID},
		// Last: it removes the record every case above reads.
		{action: "DeleteTopic", params: topic, anchor: snsWireRequestID},
	})
}

func TestSNSWire_SubscriptionResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSNSWirePlugin(t)

	created := snsWire(t, p, ctx, "CreateTopic", map[string]string{"Name": "wire-subscribed"})
	topicARN := snsWireElement(t, "CreateTopic", created, "TopicArn")

	subscribed := snsWire(t, p, ctx, "Subscribe", map[string]string{
		"TopicArn": topicARN,
		"Protocol": "sqs",
		"Endpoint": "arn:aws:sqs:" + snsWireRegion + ":" + snsWireAccount + ":wire-queue",
	})
	subARN := snsWireElement(t, "Subscribe", subscribed, "SubscriptionArn")
	snsWireRequireScoped(t, state, "subscription:"+snsWireAccount+"/"+snsWireRegion+"/"+subARN)

	sub := map[string]string{"SubscriptionArn": subARN}
	snsWireRun(t, p, ctx, []snsWireCase{
		{action: "Subscribe", held: subscribed, anchor: "<SubscriptionArn>" + subARN + "</SubscriptionArn>"},
		{action: "SetSubscriptionAttributes", params: map[string]string{
			"SubscriptionArn": subARN, "AttributeName": "RawMessageDelivery", "AttributeValue": "true",
		}, anchor: snsWireRequestID},
		// After the set, so the map renders a member the request wrote as well as the defaults.
		{action: "GetSubscriptionAttributes", params: sub, anchor: "<key>RawMessageDelivery</key>"},
		{action: "ListSubscriptions", anchor: "<SubscriptionArn>" + subARN + "</SubscriptionArn>"},
		{action: "ListSubscriptionsByTopic", params: map[string]string{"TopicArn": topicARN},
			anchor: "<SubscriptionArn>" + subARN + "</SubscriptionArn>"},
		// Last: it removes the record every case above reads.
		{action: "Unsubscribe", params: sub, anchor: snsWireRequestID},
	})
}
