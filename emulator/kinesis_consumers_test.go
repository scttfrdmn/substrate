package emulator_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1613: enhanced fan-out consumers.

// ksConsumer is the Consumer and ConsumerDescription members a test reads.
type ksConsumer struct {
	ConsumerName              string  `json:"ConsumerName"`
	ConsumerARN               string  `json:"ConsumerARN"`
	ConsumerStatus            string  `json:"ConsumerStatus"`
	ConsumerCreationTimestamp float64 `json:"ConsumerCreationTimestamp"`
	StreamARN                 string  `json:"StreamARN"`
}

// ksRegister registers a consumer and returns it.
func ksRegister(t *testing.T, ts *emulator.TestServer, stream, name string) ksConsumer {
	t.Helper()
	out := ksOK(t, ts, "RegisterStreamConsumer", map[string]any{"StreamARN": ksTestARN(stream), "ConsumerName": name})
	var resp struct {
		Consumer ksConsumer `json:"Consumer"`
	}
	require.NoError(t, json.Unmarshal(out, &resp), "%s", out)
	return resp.Consumer
}

// ksDescribeConsumer describes a consumer by the given reference.
func ksDescribeConsumer(t *testing.T, ts *emulator.TestServer, ref map[string]any) ksConsumer {
	t.Helper()
	out := ksOK(t, ts, "DescribeStreamConsumer", ref)
	var resp struct {
		ConsumerDescription ksConsumer `json:"ConsumerDescription"`
	}
	require.NoError(t, json.Unmarshal(out, &resp), "%s", out)
	return resp.ConsumerDescription
}

// ksControl posts or deletes a control-plane seed and requires a 200.
func ksControl(t *testing.T, ts *emulator.TestServer, method, path, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, out)
}

func TestKinesisConsumers_RegisterDescribeListDeregister(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	registered := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	ts.FreezeTimeAt(registered)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "fan", "ShardCount": 1})

	c := ksRegister(t, ts, "fan", "app")
	want := ksTestARN("fan") + "/consumer/app:" + strconv.FormatInt(registered.Unix(), 10)
	assert.Equal(t, want, c.ConsumerARN, "the ARN carries the registration time in epoch seconds")
	assert.Equal(t, "ACTIVE", c.ConsumerStatus, "unseeded, a consumer is ACTIVE at once")
	assert.InDelta(t, float64(registered.Unix()), c.ConsumerCreationTimestamp, 0.001)

	// By ARN, by stream ARN and name, and by all three.
	for _, ref := range []map[string]any{
		{"ConsumerARN": want},
		{"StreamARN": ksTestARN("fan"), "ConsumerName": "app"},
		{"StreamARN": ksTestARN("fan"), "ConsumerName": "app", "ConsumerARN": want},
	} {
		got := ksDescribeConsumer(t, ts, ref)
		assert.Equal(t, want, got.ConsumerARN)
		assert.Equal(t, ksTestARN("fan"), got.StreamARN)
	}

	out := ksOK(t, ts, "DescribeStreamSummary", map[string]any{"StreamName": "fan"})
	assert.Contains(t, string(out), `"ConsumerCount":1`)
	assert.Contains(t, string(out), `"EncryptionType":"NONE"`)

	ksRefusal(t, ts, "RegisterStreamConsumer", map[string]any{"StreamARN": ksTestARN("fan"), "ConsumerName": "app"},
		"ResourceInUseException")
	ksRefusal(t, ts, "DeleteStream", map[string]any{"StreamName": "fan"}, "ResourceInUseException")

	ksOK(t, ts, "DeregisterStreamConsumer", map[string]any{"ConsumerARN": want})
	ksRefusal(t, ts, "DescribeStreamConsumer", map[string]any{"ConsumerARN": want}, "ResourceNotFoundException")

	// Registered again a minute later, the consumer has a new ARN and the old one names nothing.
	ts.FreezeTimeAt(registered.Add(time.Minute))
	again := ksRegister(t, ts, "fan", "app")
	assert.NotEqual(t, want, again.ConsumerARN)
	ksRefusal(t, ts, "DescribeStreamConsumer", map[string]any{"ConsumerARN": want}, "ResourceNotFoundException")
	ksRefusal(t, ts, "DeregisterStreamConsumer", map[string]any{"ConsumerARN": want}, "ResourceNotFoundException")

	// EnforceConsumerDeletion deletes the stream and its consumers.
	ksOK(t, ts, "DeleteStream", map[string]any{"StreamName": "fan", "EnforceConsumerDeletion": true})
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "fan", "ShardCount": 1})
	out = ksOK(t, ts, "ListStreamConsumers", map[string]any{"StreamARN": ksTestARN("fan")})
	assert.JSONEq(t, `{"Consumers":[]}`, string(out))
}

func TestKinesisConsumers_ListPagesAndLimits(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "many", "ShardCount": 1})
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "many-other", "ShardCount": 1})
	ksRegister(t, ts, "many-other", "elsewhere")
	for i := range 20 {
		ksRegister(t, ts, "many", fmt.Sprintf("c%02d", i))
	}
	ksRefusal(t, ts, "RegisterStreamConsumer", map[string]any{"StreamARN": ksTestARN("many"), "ConsumerName": "c20"},
		"LimitExceededException")

	var names []string
	token := ""
	for {
		body := map[string]any{"StreamARN": ksTestARN("many"), "MaxResults": 7}
		if token != "" {
			body["NextToken"] = token
		}
		out := ksOK(t, ts, "ListStreamConsumers", body)
		var resp struct {
			Consumers []ksConsumer `json:"Consumers"`
			NextToken string       `json:"NextToken"`
		}
		require.NoError(t, json.Unmarshal(out, &resp))
		for _, c := range resp.Consumers {
			names = append(names, c.ConsumerName)
		}
		if token = resp.NextToken; token == "" {
			break
		}
	}
	assert.Len(t, names, 20, "the other stream's consumer is not listed")
	assert.Equal(t, "c00", names[0])
	assert.Equal(t, "c19", names[19])

	_, firstToken := ksListConsumersToken(t, ts, "many")
	for name, body := range map[string]map[string]any{
		"no StreamARN":                           {},
		"a token for another stream":             {"StreamARN": ksTestARN("many-other"), "NextToken": firstToken},
		"NextToken with StreamCreationTimestamp": {"StreamARN": ksTestARN("many"), "NextToken": firstToken, "StreamCreationTimestamp": 1},
		"MaxResults 0":                           {"StreamARN": ksTestARN("many"), "MaxResults": 0},
	} {
		t.Run(name, func(t *testing.T) { ksRefusal(t, ts, "ListStreamConsumers", body, "InvalidArgumentException") })
	}
	ksRefusal(t, ts, "ListStreamConsumers", map[string]any{"StreamARN": ksTestARN("nope")}, "ResourceNotFoundException")
}

// ksListConsumersToken returns the first page's names and token at MaxResults 1.
func ksListConsumersToken(t *testing.T, ts *emulator.TestServer, stream string) ([]ksConsumer, string) {
	t.Helper()
	out := ksOK(t, ts, "ListStreamConsumers", map[string]any{"StreamARN": ksTestARN(stream), "MaxResults": 1})
	var resp struct {
		Consumers []ksConsumer `json:"Consumers"`
		NextToken string       `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	return resp.Consumers, resp.NextToken
}

func TestKinesisConsumers_SeededCreatingThenActive(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "slow", "ShardCount": 1})
	ksControl(t, ts, http.MethodPost, "/v1/kinesis/consumer-status", `{"consumerName":"app","pendingObservations":2}`)

	c := ksRegister(t, ts, "slow", "app")
	assert.Equal(t, "CREATING", c.ConsumerStatus, "the register response peeks")
	ref := map[string]any{"ConsumerARN": c.ConsumerARN}
	assert.Equal(t, "CREATING", ksDescribeConsumer(t, ts, ref).ConsumerStatus)
	consumers, _ := ksListConsumersToken(t, ts, "slow")
	require.Len(t, consumers, 1)
	assert.Equal(t, "CREATING", consumers[0].ConsumerStatus, "a listing observes too")
	assert.Equal(t, "ACTIVE", ksDescribeConsumer(t, ts, ref).ConsumerStatus)

	// Five consumers CREATING at once is the limit. Posting the wildcard restarts every countdown, so
	// "app" is CREATING again and counts towards it.
	ksControl(t, ts, http.MethodPost, "/v1/kinesis/consumer-status", `{"pendingObservations":3}`)
	for i := range 4 {
		assert.Equal(t, "CREATING", ksRegister(t, ts, "slow", fmt.Sprintf("w%d", i)).ConsumerStatus)
	}
	ksRefusal(t, ts, "RegisterStreamConsumer", map[string]any{"StreamARN": ksTestARN("slow"), "ConsumerName": "w4"},
		"LimitExceededException")

	ksControl(t, ts, http.MethodDelete, "/v1/kinesis/consumer-status", "")
	assert.Equal(t, "ACTIVE", ksRegister(t, ts, "slow", "w4").ConsumerStatus, "clearing the seeds settles every consumer")

	for _, body := range []string{`{"pendingObservations":-1}`, `{"consumerARN":"arn:aws:kinesis:us-east-1:123456789012:stream/x"}`,
		`{"consumerName":"other","consumerARN":"` + c.ConsumerARN + `"}`} {
		resp, err := http.Post(ts.URL+"/v1/kinesis/consumer-status", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		resp.Body.Close() //nolint:errcheck,gosec
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
	}
}

func TestKinesisConsumers_Refusals(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "r", "ShardCount": 1})
	c := ksRegister(t, ts, "r", "app")
	arn := ksTestARN("r")
	for name, tc := range map[string]struct {
		op   string
		body map[string]any
		code string
	}{
		"register with no StreamARN":        {"RegisterStreamConsumer", map[string]any{"ConsumerName": "x"}, "InvalidArgumentException"},
		"register with no name":             {"RegisterStreamConsumer", map[string]any{"StreamARN": arn}, "InvalidArgumentException"},
		"register with a bad name":          {"RegisterStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "a b"}, "InvalidArgumentException"},
		"register with a long name":         {"RegisterStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": strings.Repeat("a", 129)}, "InvalidArgumentException"},
		"register with an empty tag key":    {"RegisterStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "t", "Tags": map[string]string{"": "v"}}, "InvalidArgumentException"},
		"register on a missing stream":      {"RegisterStreamConsumer", map[string]any{"StreamARN": ksTestARN("gone"), "ConsumerName": "x"}, "ResourceNotFoundException"},
		"describe naming nothing":           {"DescribeStreamConsumer", map[string]any{}, "InvalidArgumentException"},
		"describe by name alone":            {"DescribeStreamConsumer", map[string]any{"ConsumerName": "app"}, "InvalidArgumentException"},
		"describe with a malformed ARN":     {"DescribeStreamConsumer", map[string]any{"ConsumerARN": arn + "/consumer/app"}, "InvalidArgumentException"},
		"describe with a stream mismatch":   {"DescribeStreamConsumer", map[string]any{"ConsumerARN": c.ConsumerARN, "StreamARN": ksTestARN("x")}, "InvalidArgumentException"},
		"describe with a name mismatch":     {"DescribeStreamConsumer", map[string]any{"ConsumerARN": c.ConsumerARN, "ConsumerName": "other"}, "InvalidArgumentException"},
		"describe a missing consumer":       {"DescribeStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "nope"}, "ResourceNotFoundException"},
		"deregister a missing consumer":     {"DeregisterStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "nope"}, "ResourceNotFoundException"},
		"describe with a non-JSON body":     {"DescribeStreamConsumer", nil, "InvalidArgumentException"},
		"subscribe is refused by name":      {"SubscribeToShard", map[string]any{"ConsumerARN": c.ConsumerARN}, "UnknownOperationException"},
		"an unknown operation still is one": {"NoSuchOperation", map[string]any{}, "UnknownOperationException"},
	} {
		t.Run(name, func(t *testing.T) {
			status, out := ksCallRaw(t, ts, tc.op, tc.body)
			assert.Contains(t, string(out), tc.code, "%s", out)
			if tc.code == "UnknownOperationException" {
				assert.Equal(t, http.StatusNotFound, status)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
	_, out := ksCallRaw(t, ts, "SubscribeToShard", map[string]any{})
	assert.Contains(t, string(out), "issues/1695", "the refusal points at the tracking issue")

	// A stream that is not ACTIVE takes no consumer.
	ksSeed(t, ts, `{"streamName":"busy","pendingObservations":1}`)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "busy", "ShardCount": 1})
	ksRefusal(t, ts, "RegisterStreamConsumer", map[string]any{"StreamARN": ksTestARN("busy"), "ConsumerName": "x"},
		"ResourceInUseException")
}

// ksCallRaw is ksCall with a nil body sent as a non-JSON payload.
func ksCallRaw(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, []byte) {
	t.Helper()
	if body == nil {
		return ksCallBytes(t, ts, op, []byte("not json"))
	}
	return ksCall(t, ts, op, body)
}

// ksCallBytes issues one Kinesis operation with a raw body.
func ksCallBytes(t *testing.T, ts *emulator.TestServer, op string, raw []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(string(raw)))
	require.NoError(t, err)
	req.Host = "kinesis.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

func TestKinesisResourceTags_StreamsAndConsumers(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "tg", "ShardCount": 1})
	out := ksOK(t, ts, "RegisterStreamConsumer", map[string]any{
		"StreamARN": ksTestARN("tg"), "ConsumerName": "app", "Tags": map[string]string{"team": "data"},
	})
	var reg struct {
		Consumer ksConsumer `json:"Consumer"`
	}
	require.NoError(t, json.Unmarshal(out, &reg))
	consumerARN := reg.Consumer.ConsumerARN

	ksOK(t, ts, "TagResource", map[string]any{"ResourceARN": consumerARN, "Tags": map[string]string{"env": "dev"}})
	ksOK(t, ts, "TagResource", map[string]any{"ResourceARN": ksTestARN("tg"), "Tags": map[string]string{"owner": "me"}})
	out = ksOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": consumerARN})
	assert.JSONEq(t, `{"Tags":[{"Key":"env","Value":"dev"},{"Key":"team","Value":"data"}]}`, string(out))

	// The stream's ARN reaches the tags AddTagsToStream writes.
	out = ksOK(t, ts, "ListTagsForStream", map[string]any{"StreamName": "tg"})
	assert.Contains(t, string(out), `"owner"`)
	ksOK(t, ts, "UntagResource", map[string]any{"ResourceARN": ksTestARN("tg"), "TagKeys": []string{"owner"}})
	out = ksOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": ksTestARN("tg")})
	assert.JSONEq(t, `{"Tags":[]}`, string(out))
	ksOK(t, ts, "UntagResource", map[string]any{"ResourceARN": consumerARN, "TagKeys": []string{"env", "team"}})
	out = ksOK(t, ts, "ListTagsForResource", map[string]any{"ResourceARN": consumerARN})
	assert.JSONEq(t, `{"Tags":[]}`, string(out))

	fifty := make(map[string]string, 50)
	for i := range 50 {
		fifty[fmt.Sprintf("k%02d", i)] = "v"
	}
	ksOK(t, ts, "TagResource", map[string]any{"ResourceARN": consumerARN, "Tags": fifty})
	for name, tc := range map[string]struct {
		op   string
		body map[string]any
		code string
	}{
		"a 51st tag":         {"TagResource", map[string]any{"ResourceARN": consumerARN, "Tags": map[string]string{"one": "more"}}, "LimitExceededException"},
		"no tags":            {"TagResource", map[string]any{"ResourceARN": consumerARN}, "InvalidArgumentException"},
		"no ARN":             {"ListTagsForResource", map[string]any{}, "InvalidArgumentException"},
		"a queue ARN":        {"ListTagsForResource", map[string]any{"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:queue/q"}, "InvalidArgumentException"},
		"a missing stream":   {"ListTagsForResource", map[string]any{"ResourceARN": ksTestARN("nope")}, "ResourceNotFoundException"},
		"a missing consumer": {"ListTagsForResource", map[string]any{"ResourceARN": ksTestARN("tg") + "/consumer/nope:1"}, "ResourceNotFoundException"},
		"no keys":            {"UntagResource", map[string]any{"ResourceARN": consumerARN, "TagKeys": []string{}}, "InvalidArgumentException"},
	} {
		t.Run(name, func(t *testing.T) { ksRefusal(t, ts, tc.op, tc.body, tc.code) })
	}
}

func TestKinesisConsumers_AnARNSeedGovernsOneConsumer(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "arnseed", "ShardCount": 1})
	a := ksRegister(t, ts, "arnseed", "a")
	b := ksRegister(t, ts, "arnseed", "b")
	seed := `{"consumerARN":"` + a.ConsumerARN + `","pendingObservations":1}`
	ksControl(t, ts, http.MethodPost, "/v1/kinesis/consumer-status", seed)
	assert.Equal(t, "CREATING", ksDescribeConsumer(t, ts, map[string]any{"ConsumerARN": a.ConsumerARN}).ConsumerStatus)
	assert.Equal(t, "ACTIVE", ksDescribeConsumer(t, ts, map[string]any{"ConsumerARN": b.ConsumerARN}).ConsumerStatus,
		"an ARN seed governs only its consumer")
	assert.Equal(t, "ACTIVE", ksDescribeConsumer(t, ts, map[string]any{"ConsumerARN": a.ConsumerARN}).ConsumerStatus)

	ksControl(t, ts, http.MethodPost, "/v1/kinesis/consumer-status", seed)
	ksControl(t, ts, http.MethodDelete, "/v1/kinesis/consumer-status?consumer="+a.ConsumerARN, "")
	assert.Equal(t, "ACTIVE", ksDescribeConsumer(t, ts, map[string]any{"ConsumerARN": a.ConsumerARN}).ConsumerStatus,
		"clearing the ARN seed settles the consumer")
}
