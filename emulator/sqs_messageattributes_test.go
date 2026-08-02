package emulator_test

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// receivedAttrXML is one message attribute in a ReceiveMessage XML response.
type receivedAttrXML struct {
	Name  string `xml:"Name"`
	Value struct {
		DataType    string `xml:"DataType"`
		StringValue string `xml:"StringValue"`
		BinaryValue string `xml:"BinaryValue"`
	} `xml:"Value"`
}

// receivedMessageXML is one message in a ReceiveMessage XML response, carrying the
// fields #461 is about.
type receivedMessageXML struct {
	MessageID              string            `xml:"MessageId"`
	Body                   string            `xml:"Body"`
	MD5OfBody              string            `xml:"MD5OfBody"`
	MD5OfMessageAttributes string            `xml:"MD5OfMessageAttributes"`
	MessageAttribute       []receivedAttrXML `xml:"MessageAttribute"`
}

// receivedAttr is a message attribute as a test observes it, normalized across the
// two protocols so an assertion can be written once. A binary value is held in its
// base64 wire form, which is what both protocols carry.
type receivedAttr struct {
	dataType    string
	stringValue string
	binaryValue string
}

// receivedMessage is a received message normalized across protocols.
type receivedMessage struct {
	messageID              string
	body                   string
	md5OfMessageAttributes string
	attributes             map[string]receivedAttr
}

// receiveMessages calls ReceiveMessage under either protocol, optionally naming
// MessageAttributeName selectors, and returns the messages in a protocol-independent
// shape.
//
// A nil selector list sends no MessageAttributeName at all, which is the case that
// distinguishes "return nothing" from "return everything" — the whole point of the
// selection rule.
func receiveMessages(t *testing.T, srv *emulator.Server, queueURL string,
	attrNames []string, jsonProto bool,
) []receivedMessage {
	t.Helper()
	if jsonProto {
		in := map[string]interface{}{"QueueUrl": queueURL, "MaxNumberOfMessages": 10}
		if attrNames != nil {
			in["MessageAttributeNames"] = attrNames
		}
		resp := sqsJSONRequest(t, srv, "ReceiveMessage", in)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body := readJSONBody(t, resp)

		raw, _ := body["Messages"].([]interface{})
		out := make([]receivedMessage, 0, len(raw))
		for _, item := range raw {
			m, ok := item.(map[string]interface{})
			require.True(t, ok, "message is not an object: %v", item)
			msg := receivedMessage{}
			msg.messageID, _ = m["MessageId"].(string)
			msg.body, _ = m["Body"].(string)
			msg.md5OfMessageAttributes, _ = m["MD5OfMessageAttributes"].(string)
			if attrs, present := m["MessageAttributes"].(map[string]interface{}); present {
				msg.attributes = make(map[string]receivedAttr, len(attrs))
				for name, v := range attrs {
					av, isObj := v.(map[string]interface{})
					require.True(t, isObj, "attribute %q is not an object", name)
					got := receivedAttr{}
					got.dataType, _ = av["DataType"].(string)
					got.stringValue, _ = av["StringValue"].(string)
					// A []byte field marshals to base64 in JSON, so the value
					// arrives in the same wire form the XML path emits.
					got.binaryValue, _ = av["BinaryValue"].(string)
					msg.attributes[name] = got
				}
			}
			out = append(out, msg)
		}
		return out
	}

	params := map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "10",
	}
	for i, name := range attrNames {
		params["MessageAttributeName."+strconv.Itoa(i+1)] = name
	}
	resp := sqsRequest(t, srv, params)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var parsed struct {
		Result struct {
			Message []receivedMessageXML `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(readBody(t, resp)), &parsed))

	out := make([]receivedMessage, 0, len(parsed.Result.Message))
	for _, m := range parsed.Result.Message {
		msg := receivedMessage{
			messageID:              m.MessageID,
			body:                   m.Body,
			md5OfMessageAttributes: m.MD5OfMessageAttributes,
		}
		if len(m.MessageAttribute) > 0 {
			msg.attributes = make(map[string]receivedAttr, len(m.MessageAttribute))
			for _, a := range m.MessageAttribute {
				msg.attributes[a.Name] = receivedAttr{
					dataType:    a.Value.DataType,
					stringValue: a.Value.StringValue,
					binaryValue: a.Value.BinaryValue,
				}
			}
		}
		out = append(out, msg)
	}
	return out
}

// sendMessageAttrDigest sends a message with attributes and returns the
// MD5OfMessageAttributes from the send response, plus whether the field was present
// at all. The presence flag is what distinguishes an omitted field from an empty one,
// which is the assertion the no-attribute case needs.
func sendMessageAttrDigest(t *testing.T, srv *emulator.Server, queueURL, body string,
	attrs []sqsMessageAttr, jsonProto bool,
) (string, bool) {
	t.Helper()
	if jsonProto {
		in := map[string]interface{}{"QueueUrl": queueURL, "MessageBody": body}
		if len(attrs) > 0 {
			in["MessageAttributes"] = jsonAttrMap(attrs)
		}
		resp := sqsJSONRequest(t, srv, "SendMessage", in)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		got := readJSONBody(t, resp)
		v, ok := got["MD5OfMessageAttributes"].(string)
		return v, ok
	}

	params := map[string]string{"Action": "SendMessage", "QueueUrl": queueURL, "MessageBody": body}
	for i, a := range attrs {
		base := "MessageAttribute." + strconv.Itoa(i+1) + "."
		params[base+"Name"] = a.name
		params[base+"Value.DataType"] = a.dataType
		if a.stringValue != "" {
			params[base+"Value.StringValue"] = a.stringValue
		}
		if a.binaryValue != nil {
			params[base+"Value.BinaryValue"] = base64.StdEncoding.EncodeToString(a.binaryValue)
		}
	}
	resp := sqsRequest(t, srv, params)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var parsed struct {
		Result struct {
			MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes"`
		} `xml:"SendMessageResult"`
	}
	raw := readBody(t, resp)
	require.NoError(t, xml.Unmarshal([]byte(raw), &parsed))
	// `omitempty` on the field means an absent element and an empty one are
	// indistinguishable once unmarshaled, so presence is read from the document.
	return parsed.Result.MD5OfMessageAttributes, strings.Contains(raw, "<MD5OfMessageAttributes>")
}

// jsonAttrMap renders attributes in the JSON protocol's MessageAttributes shape.
func jsonAttrMap(attrs []sqsMessageAttr) map[string]interface{} {
	m := make(map[string]interface{}, len(attrs))
	for _, a := range attrs {
		v := map[string]interface{}{"DataType": a.dataType}
		if a.stringValue != "" {
			v["StringValue"] = a.stringValue
		}
		if a.binaryValue != nil {
			v["BinaryValue"] = a.binaryValue
		}
		m[a.name] = v
	}
	return m
}

// createAttrQueue creates a default queue and returns its URL.
func createAttrQueue(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	status, code := createQueueWithAttrs(t, srv, name, nil, false)
	require.Equal(t, http.StatusOK, status, "create failed: %s", code)
	return "http://sqs.us-east-1.localhost/000000000000/" + name
}

// TestSQS_MessageAttributes_KnownGoodDigests is #461's "verified against a known-good
// digest" criterion, met without network access.
//
// The three vectors are real-AWS MD5OfMessageAttributes values captured in moto's
// test suite (tests/test_sqs/test_sqs.py, test_message_send_with_attributes), which
// asserts them against responses recorded from the live service. Each was reproduced
// from the documented algorithm — sort by name; 4-byte big-endian length plus UTF-8
// name; the same for the data type; one transport byte (1 for String and Number, 2
// for Binary); then the value's length and bytes — before any of this code was
// written, which is what makes them a check on the implementation rather than a
// transcription of it.
//
// They are not interchangeable. Vector 1 is the minimal single-attribute case.
// Vector 2 pins that a custom data-type suffix is hashed in full: it does not
// reproduce if "Number.java.lang.Long" is truncated to its "Number" base type.
// Vector 3 pins that a binary value is hashed raw — the base64 form of the same
// input digests to 5ff413c9dc7bd18abea88ca05643f902 instead, which is the mistake a
// reimplementation makes by default because base64 is what travels on the wire.
func TestSQS_MessageAttributes_KnownGoodDigests(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "digest-vectors")

	tests := []struct {
		name  string
		attrs []sqsMessageAttr
		want  string
	}{
		{
			name: "single Number attribute",
			attrs: []sqsMessageAttr{
				{name: "SOME_Valid.attribute-Name", dataType: "Number", stringValue: "1493147359900"},
			},
			want: "36655e7e9d7c0e8479fa3f3f42247ae7",
		},
		{
			name: "three attributes including a custom Number suffix",
			attrs: []sqsMessageAttr{
				{name: "id", dataType: "String", stringValue: "2018fc74-4f77-1a5a-1be0-c2d037d5052b"},
				{name: "contentType", dataType: "String", stringValue: "application/json"},
				{name: "timestamp", dataType: "Number.java.lang.Long", stringValue: "1602845432024"},
			},
			want: "b12289320bb6e494b18b645ef562b4a9",
		},
		{
			name: "binary attribute hashes raw bytes",
			attrs: []sqsMessageAttr{
				{name: "id", dataType: "String", stringValue: "453ae55e-f03b-21a6-a4b1-70c2e2e8fe71"},
				{name: "mybin", dataType: "Binary", binaryValue: []byte("kekchebukek")},
				{name: "timestamp", dataType: "Number.java.lang.Long", stringValue: "1603134247654"},
				{name: "contentType", dataType: "String", stringValue: "application/json"},
			},
			want: "049075255ebc53fb95f7f9f3cedf3c50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				got, present := sendMessageAttrDigest(t, srv, queueURL, "derp", tt.attrs, jsonProto)
				assert.True(t, present, "MD5OfMessageAttributes should be present")
				assert.Equal(t, tt.want, got)
			})
		})
	}
}

// TestSQS_MessageAttributes_BinaryIsIdentifiedByDataType pins that the digest's
// transport byte is chosen from the declared data type, not from which value field
// happens to be populated.
//
// The two rules agree on every attribute with a value, and diverge only on a Binary
// attribute whose value is empty: keyed off the data type it transports as binary
// (byte 2), keyed off the value it would fall through to the string branch (byte 1)
// and digest to something real SQS never emits.
//
// There is no real-AWS digest to compare against here: the SendMessage reference
// states that an attribute's "name, type, and value must not be empty or null", so
// real SQS rejects this request outright, and substrate not enforcing that rule yet is
// what makes the case reachable at all. So the expected value is computed from the
// published algorithm rather than captured — 4-byte length and "payload", 4-byte
// length and "Binary", transport byte 2, then a 4-byte zero length and no value bytes.
// Byte 1 in that position yields e275a101bd238d68e457c2f49e1ad0e7, which is what an
// implementation keyed off the value field produces.
func TestSQS_MessageAttributes_BinaryIsIdentifiedByDataType(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "attr-empty-binary")

	got, present := sendMessageAttrDigest(t, srv, queueURL, "body",
		[]sqsMessageAttr{{name: "payload", dataType: "Binary"}}, false)
	require.True(t, present)
	assert.Equal(t, "6f8879e6be14e9d1c45ef3e9631b86c9", got,
		"an empty Binary attribute must still transport as binary")

	// A custom binary type transports the same way, since "Binary.gzip" is a
	// documented custom type in the Binary family.
	gzipped, present := sendMessageAttrDigest(t, srv, queueURL, "body",
		[]sqsMessageAttr{{name: "payload", dataType: "Binary.gzip",
			binaryValue: []byte{0x1f, 0x8b}}}, false)
	require.True(t, present)
	assert.NotEmpty(t, gzipped)
}

// TestSQS_MessageAttributes_DigestIsOrderIndependent pins that the digest depends on
// the attribute names, not on the order they were sent in.
//
// The algorithm sorts by name, so two requests carrying the same attributes in
// different orders must produce the same digest — and a Go map's random iteration
// order means an implementation that skipped the sort would produce a *different*
// digest on different runs of the same test, which is precisely the nondeterminism
// this emulator exists to avoid.
func TestSQS_MessageAttributes_DigestIsOrderIndependent(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "digest-order")

	forward := []sqsMessageAttr{
		{name: "alpha", dataType: "String", stringValue: "1"},
		{name: "beta", dataType: "String", stringValue: "2"},
		{name: "gamma", dataType: "String", stringValue: "3"},
	}
	reversed := []sqsMessageAttr{forward[2], forward[1], forward[0]}

	first, _ := sendMessageAttrDigest(t, srv, queueURL, "body", forward, false)
	second, _ := sendMessageAttrDigest(t, srv, queueURL, "body", reversed, false)
	assert.Equal(t, first, second, "digest must not depend on send order")
	assert.NotEmpty(t, first)
}

// TestSQS_MessageAttributes_OmittedWhenNoneSent pins that MD5OfMessageAttributes is
// absent from a send with no attributes, rather than reported as the digest of zero
// bytes.
//
// Real SQS omits the field entirely — moto's suite asserts `"MD5OfMessageAttributes"
// not in msg` for a bare send, against recorded live behavior. A digest of nothing
// (d41d8cd98f00b204e9800998ecf8427e) would be a value a caller could compare against
// and "successfully" verify, so reporting it is worse than reporting nothing.
func TestSQS_MessageAttributes_OmittedWhenNoneSent(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "digest-absent")

		got, present := sendMessageAttrDigest(t, srv, queueURL, "derp", nil, jsonProto)
		assert.False(t, present, "MD5OfMessageAttributes should be omitted, got %q", got)
		assert.Empty(t, got)
	})
}

// TestSQS_MessageAttributes_RoundTrip is the core of #461: attributes were parsed for
// size measurement (#454) but never stored, so ReceiveMessage returned none of them.
// A consumer routing on an attribute — a messageType discriminator, a trace ID, a
// tenant key — saw every message fall to its default branch, with no error to explain
// why.
//
// All four value shapes round-trip: String, Number, a custom-suffixed Number, and
// Binary. The binary case is the one that can go wrong invisibly, since it must come
// back base64-encoded on the wire and decode to the exact bytes that were sent.
func TestSQS_MessageAttributes_RoundTrip(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "attr-roundtrip")

		binary := []byte{0x00, 0x01, 0xfe, 0xff, 0x42}
		attrs := []sqsMessageAttr{
			{name: "messageType", dataType: "String", stringValue: "OrderPlaced"},
			{name: "retries", dataType: "Number", stringValue: "3"},
			{name: "timestamp", dataType: "Number.java.lang.Long", stringValue: "1602845432024"},
			{name: "payload", dataType: "Binary", binaryValue: binary},
		}

		status, code := sendMessageWithAttrs(t, srv, queueURL, "order-42", attrs, jsonProto)
		require.Equal(t, http.StatusOK, status, "send failed: %s", code)

		msgs := receiveMessages(t, srv, queueURL, []string{"All"}, jsonProto)
		require.Len(t, msgs, 1)
		require.Len(t, msgs[0].attributes, 4)

		assert.Equal(t, receivedAttr{dataType: "String", stringValue: "OrderPlaced"},
			msgs[0].attributes["messageType"])
		assert.Equal(t, receivedAttr{dataType: "Number", stringValue: "3"},
			msgs[0].attributes["retries"])
		assert.Equal(t, receivedAttr{dataType: "Number.java.lang.Long", stringValue: "1602845432024"},
			msgs[0].attributes["timestamp"])

		got := msgs[0].attributes["payload"]
		assert.Equal(t, "Binary", got.dataType)
		decoded, err := base64.StdEncoding.DecodeString(got.binaryValue)
		require.NoError(t, err, "BinaryValue should be base64 on the wire")
		assert.Equal(t, binary, decoded, "binary value must survive byte-identical")
	})
}

// TestSQS_MessageAttributes_ReturnedOnlyWhenRequested pins the four documented
// selection outcomes.
//
// Returning attributes unconditionally is the trap a naive fix falls into, and it is
// an infidelity in the permissive direction: a consumer whose production caller never
// sets MessageAttributeNames would pass a test against substrate and then read no
// attributes from AWS. The selector forms come from the ReceiveMessage reference —
// "All" and ".*" for everything, a name for one, and a "prefix.*" form the guide
// documents as "all message attributes starting with a prefix, for example bar.*".
func TestSQS_MessageAttributes_ReturnedOnlyWhenRequested(t *testing.T) {
	attrs := []sqsMessageAttr{
		{name: "alpha", dataType: "String", stringValue: "a"},
		{name: "beta", dataType: "String", stringValue: "b"},
		{name: "trace.id", dataType: "String", stringValue: "t"},
		{name: "trace.span", dataType: "String", stringValue: "s"},
	}

	tests := []struct {
		name      string
		requested []string
		want      []string
	}{
		{name: "omitted returns none", requested: nil, want: nil},
		{name: "All returns every attribute", requested: []string{"All"},
			want: []string{"alpha", "beta", "trace.id", "trace.span"}},
		{name: "dot-star returns every attribute", requested: []string{".*"},
			want: []string{"alpha", "beta", "trace.id", "trace.span"}},
		{name: "a named subset returns only those", requested: []string{"alpha", "beta"},
			want: []string{"alpha", "beta"}},
		{name: "a single name returns one", requested: []string{"beta"}, want: []string{"beta"}},
		{name: "a prefix selector returns its family", requested: []string{"trace.*"},
			want: []string{"trace.id", "trace.span"}},
		{name: "an unmatched name returns none", requested: []string{"nosuchattr"}, want: nil},
		{name: "a mix of matched and unmatched returns the matched", requested: []string{"alpha", "nosuchattr"},
			want: []string{"alpha"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				srv, _ := newSQSTestServer(t)
				queueURL := createAttrQueue(t, srv, "attr-select")

				status, code := sendMessageWithAttrs(t, srv, queueURL, "body", attrs, jsonProto)
				require.Equal(t, http.StatusOK, status, "send failed: %s", code)

				msgs := receiveMessages(t, srv, queueURL, tt.requested, jsonProto)
				require.Len(t, msgs, 1)

				names := make([]string, 0, len(msgs[0].attributes))
				for name := range msgs[0].attributes {
					names = append(names, name)
				}
				sort.Strings(names)
				assert.Equal(t, tt.want, nilIfEmpty(names))
			})
		})
	}
}

// nilIfEmpty normalizes an empty slice to nil, so a test can express "no attributes"
// once rather than distinguishing a nil map from an empty one at every call site.
func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

// TestSQS_MessageAttributes_ReceiveDigestCoversTheReturnedSubset pins that
// MD5OfMessageAttributes on a receive is the digest of what is being returned, not a
// replay of the send-time value.
//
// The digest exists so a caller can checksum the attributes in hand. A request naming
// a subset that got the full send-time digest would fail that check every time — and
// fail it in the confusing direction, since the data is correct and only the checksum
// disagrees.
func TestSQS_MessageAttributes_ReceiveDigestCoversTheReturnedSubset(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, tc := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "attr-subset-digest")

		// The full set is vector 2, so the send-time digest is a known constant and
		// the subset digest is demonstrably not it.
		attrs := []sqsMessageAttr{
			{name: "id", dataType: "String", stringValue: "2018fc74-4f77-1a5a-1be0-c2d037d5052b"},
			{name: "contentType", dataType: "String", stringValue: "application/json"},
			{name: "timestamp", dataType: "Number.java.lang.Long", stringValue: "1602845432024"},
		}
		const fullDigest = "b12289320bb6e494b18b645ef562b4a9"

		sent, present := sendMessageAttrDigest(t, srv, queueURL, "derp", attrs, jsonProto)
		require.True(t, present)
		require.Equal(t, fullDigest, sent)

		// Each receive here inspects the same message, so the simulated clock is moved
		// past the 30-second visibility timeout between them. Without that the second
		// and third calls would return nothing and the assertions below would pass
		// vacuously on an empty list.
		redeliver := func() { tc.SetTime(tc.Now().Add(31 * time.Second)) }

		// Receiving everything reproduces the send-time digest.
		all := receiveMessages(t, srv, queueURL, []string{"All"}, jsonProto)
		require.Len(t, all, 1)
		assert.Equal(t, fullDigest, all[0].md5OfMessageAttributes)

		// A single-attribute subset must digest to that attribute alone. Asserting it
		// is not the full digest is the part that catches a replayed send-time value.
		redeliver()
		subset := receiveMessages(t, srv, queueURL, []string{"contentType"}, jsonProto)
		require.Len(t, subset, 1)
		require.Len(t, subset[0].attributes, 1)
		assert.NotEqual(t, fullDigest, subset[0].md5OfMessageAttributes,
			"a subset must not report the full set's digest")
		assert.NotEmpty(t, subset[0].md5OfMessageAttributes)

		// And a receive that asked for nothing carries no digest at all, since there
		// is nothing for a caller to checksum.
		redeliver()
		none := receiveMessages(t, srv, queueURL, nil, jsonProto)
		require.Len(t, none, 1)
		assert.Empty(t, none[0].md5OfMessageAttributes)
	})
}

// TestSQS_MessageAttributes_BatchRoundTrip pins that SendMessageBatch stores and
// digests per-entry attributes.
//
// The batch path parsed attributes only to measure them (#454) and constructed its
// messages without them, in both protocols. Each entry's digest must cover that
// entry's own attributes — a batch reporting one shared digest, or the first entry's,
// would break a caller checksumming per entry.
func TestSQS_MessageAttributes_BatchRoundTrip(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "attr-batch")

		entries := []sqsBatchEntry{
			{id: "e1", body: "first", attrs: []sqsMessageAttr{
				{name: "messageType", dataType: "String", stringValue: "Alpha"},
			}},
			{id: "e2", body: "second", attrs: []sqsMessageAttr{
				{name: "messageType", dataType: "String", stringValue: "Beta"},
				{name: "retries", dataType: "Number", stringValue: "7"},
			}},
			{id: "e3", body: "third"},
		}
		status, code := sendMessageBatch(t, srv, queueURL, entries, jsonProto)
		require.Equal(t, http.StatusOK, status, "batch failed: %s", code)

		msgs := receiveMessages(t, srv, queueURL, []string{"All"}, jsonProto)
		require.Len(t, msgs, 3)

		byBody := make(map[string]receivedMessage, len(msgs))
		for _, m := range msgs {
			byBody[m.body] = m
		}

		require.Contains(t, byBody, "first")
		assert.Equal(t, receivedAttr{dataType: "String", stringValue: "Alpha"},
			byBody["first"].attributes["messageType"])

		require.Contains(t, byBody, "second")
		assert.Len(t, byBody["second"].attributes, 2)
		assert.Equal(t, receivedAttr{dataType: "String", stringValue: "Beta"},
			byBody["second"].attributes["messageType"])
		assert.Equal(t, receivedAttr{dataType: "Number", stringValue: "7"},
			byBody["second"].attributes["retries"])

		// The entry that carried none has none, and no digest — the per-entry
		// omission rule, not just the per-message one.
		require.Contains(t, byBody, "third")
		assert.Empty(t, byBody["third"].attributes)
		assert.Empty(t, byBody["third"].md5OfMessageAttributes)

		// Each entry's digest differs, because each covers its own attributes.
		assert.NotEqual(t, byBody["first"].md5OfMessageAttributes,
			byBody["second"].md5OfMessageAttributes)
	})
}

// TestSQS_MessageAttributes_SurviveRedelivery pins that attributes are stored on the
// message rather than held for the first receive.
//
// A message whose visibility timeout lapses is redelivered, and a consumer's retry
// path reads the same attributes the first delivery carried. Attributes that vanished
// on redelivery would make the retry take a different branch than the original
// attempt — the kind of divergence that only shows up under failure, which is when it
// is least welcome.
func TestSQS_MessageAttributes_SurviveRedelivery(t *testing.T) {
	srv, tc := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "attr-redelivery")

	attrs := []sqsMessageAttr{
		{name: "messageType", dataType: "String", stringValue: "OrderPlaced"},
	}
	status, code := sendMessageWithAttrs(t, srv, queueURL, "order-42", attrs, false)
	require.Equal(t, http.StatusOK, status, "send failed: %s", code)

	first := receiveMessages(t, srv, queueURL, []string{"All"}, false)
	require.Len(t, first, 1)
	require.Equal(t, "OrderPlaced", first[0].attributes["messageType"].stringValue)

	// Let the default 30-second visibility timeout lapse on the simulated clock.
	tc.SetTime(tc.Now().Add(31 * time.Second))

	second := receiveMessages(t, srv, queueURL, []string{"All"}, false)
	require.Len(t, second, 1)
	assert.Equal(t, first[0].messageID, second[0].messageID)
	assert.Equal(t, "OrderPlaced", second[0].attributes["messageType"].stringValue)
	assert.Equal(t, first[0].md5OfMessageAttributes, second[0].md5OfMessageAttributes)
}

// TestSQS_MessageAttributes_TenAttributesAccepted pins AWS's documented per-message
// maximum as the boundary it is, not as a rejection.
//
// The reference states a message "can have up to 10 metadata attributes", so ten is
// legal and the eleventh is not. This test owns the legal side, and it is the guard
// #472's enforcement had to survive: an off-by-one in the count check turns a documented
// maximum into a rejection at nine.
//
// It also asserts all ten come back, which the rejection test cannot: a count check that
// dropped an attribute instead of refusing the message would pass an error-code
// assertion.
func TestSQS_MessageAttributes_TenAttributesAccepted(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "attr-ten")

		attrs := make([]sqsMessageAttr, 0, 10)
		for i := 0; i < 10; i++ {
			attrs = append(attrs, sqsMessageAttr{
				name:        "attr" + strconv.Itoa(i),
				dataType:    "String",
				stringValue: "v" + strconv.Itoa(i),
			})
		}

		status, code := sendMessageWithAttrs(t, srv, queueURL, "body", attrs, jsonProto)
		require.Equal(t, http.StatusOK, status, "send failed: %s", code)

		msgs := receiveMessages(t, srv, queueURL, []string{"All"}, jsonProto)
		require.Len(t, msgs, 1)
		assert.Len(t, msgs[0].attributes, 10)
	})
}

// TestSQS_MessageAttributes_FifoDedupReportsThisRequestsDigest pins that a
// deduplicated FIFO send reports the digest of the attributes *this* request carried,
// not the original message's.
//
// The digest is a checksum of what the caller sent, so a caller whose retry carries
// different attributes must be able to verify its own request was received intact.
// Replaying the first send's digest would fail that check for a request that
// legitimately succeeded.
func TestSQS_MessageAttributes_FifoDedupReportsThisRequestsDigest(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	status, code := createQueueWithAttrs(t, srv, "attr-dedup.fifo",
		map[string]string{"FifoQueue": "true"}, false)
	require.Equal(t, http.StatusOK, status, "create failed: %s", code)
	queueURL := "http://sqs.us-east-1.localhost/000000000000/attr-dedup.fifo"

	send := func(attrs []sqsMessageAttr) string {
		t.Helper()
		params := map[string]string{
			"Action":                 "SendMessage",
			"QueueUrl":               queueURL,
			"MessageBody":            "body",
			"MessageGroupId":         "g1",
			"MessageDeduplicationId": "dedup-1",
		}
		for i, a := range attrs {
			base := "MessageAttribute." + strconv.Itoa(i+1) + "."
			params[base+"Name"] = a.name
			params[base+"Value.DataType"] = a.dataType
			params[base+"Value.StringValue"] = a.stringValue
		}
		resp := sqsRequest(t, srv, params)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var parsed struct {
			Result struct {
				MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes"`
			} `xml:"SendMessageResult"`
		}
		require.NoError(t, xml.Unmarshal([]byte(readBody(t, resp)), &parsed))
		return parsed.Result.MD5OfMessageAttributes
	}

	firstDigest := send([]sqsMessageAttr{
		{name: "SOME_Valid.attribute-Name", dataType: "Number", stringValue: "1493147359900"},
	})
	assert.Equal(t, "36655e7e9d7c0e8479fa3f3f42247ae7", firstDigest)

	// Same deduplication ID, different attributes: the send is deduplicated, and the
	// digest describes this request rather than the stored message.
	secondDigest := send([]sqsMessageAttr{
		{name: "different", dataType: "String", stringValue: "value"},
	})
	assert.NotEqual(t, firstDigest, secondDigest,
		"a deduplicated send must digest this request's attributes")
	assert.NotEmpty(t, secondDigest)
}
