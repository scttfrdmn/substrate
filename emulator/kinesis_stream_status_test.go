package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1119: a stream's StreamStatus passes through CREATING after CreateStream and UPDATING after a
// reshard, for as many observations as a seed holds it, and reaches ACTIVE. The assertions read the
// raw StreamStatus member of DescribeStream and DescribeStreamSummary over HTTP, because a
// consumer's wait-for-ACTIVE loop reads nothing else.

// ksCall issues one Kinesis operation over HTTP and returns the status and body.
func ksCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
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

// ksOK issues one operation and requires a 200.
func ksOK(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) []byte {
	t.Helper()
	status, out := ksCall(t, ts, op, body)
	require.Equal(t, http.StatusOK, status, "%s: %s", op, out)
	return out
}

// ksStatus returns the raw StreamStatus one describe reports; summary picks DescribeStreamSummary.
func ksStatus(t *testing.T, ts *emulator.TestServer, name string, summary bool) string {
	t.Helper()
	op, member := "DescribeStream", "StreamDescription"
	if summary {
		op, member = "DescribeStreamSummary", "StreamDescriptionSummary"
	}
	out := ksOK(t, ts, op, map[string]any{"StreamName": name})
	var doc map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &doc), "decode %s: %s", op, out)
	var status string
	require.NoError(t, json.Unmarshal(doc[member]["StreamStatus"], &status), "%s StreamStatus: %s", op, out)
	return status
}

// ksSeed POSTs a stream-status seed and returns the status code.
func ksSeed(t *testing.T, ts *emulator.TestServer, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/kinesis/stream-status", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// ksClear DELETEs one stream-status seed, or every one when stream is empty.
func ksClear(t *testing.T, ts *emulator.TestServer, stream string) {
	t.Helper()
	u := ts.URL + "/v1/kinesis/stream-status"
	if stream != "" {
		u += "?stream=" + stream
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, u, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func ksCreate(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": name, "ShardCount": 2})
}

func ksReshard(t *testing.T, ts *emulator.TestServer, name string, target int) (int, []byte) {
	t.Helper()
	return ksCall(t, ts, "UpdateShardCount", map[string]any{
		"StreamName": name, "TargetShardCount": target, "ScalingType": "UNIFORM_SCALING",
	})
}

func TestKinesisStreamStatus_UnseededIsActiveThroughout(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksCreate(t, ts, "plain")
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "plain", false))
	status, out := ksReshard(t, ts, "plain", 4)
	require.Equal(t, http.StatusOK, status, "%s", out)
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "plain", true), "an unseeded reshard settles at once, as before #1119")
}

func TestKinesisStreamStatus_CreatingThenUpdatingThenActive(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, out := ksSeed(t, ts, `{"pendingObservations":2}`)
	require.Equal(t, http.StatusOK, status, "%s", out)
	ksCreate(t, ts, "s")

	// Both describes observe, so the countdown is shared between them.
	assert.Equal(t, "CREATING", ksStatus(t, ts, "s", true))
	assert.Equal(t, "CREATING", ksStatus(t, ts, "s", false))
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "s", true))

	status, out = ksReshard(t, ts, "s", 4)
	require.Equal(t, http.StatusOK, status, "%s", out)
	assert.Equal(t, "UPDATING", ksStatus(t, ts, "s", false))

	// A second reshard mid-transition is refused, and the refusal peeks, spending nothing.
	status, out = ksReshard(t, ts, "s", 2)
	require.Equal(t, http.StatusBadRequest, status, "%s", out)
	assert.Contains(t, string(out), "ResourceInUseException")
	assert.Equal(t, "UPDATING", ksStatus(t, ts, "s", true), "the refused reshard did not spend the second observation")
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "s", true))

	// The record settled: the reshard's shard count is what the stream now reports.
	out = ksOK(t, ts, "DescribeStreamSummary", map[string]any{"StreamName": "s"})
	assert.Contains(t, string(out), `"OpenShardCount":4`)
}

func TestKinesisStreamStatus_MergeAndSplitReportUpdatingAndRefuseMidTransition(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"MergeShards", "SplitShard"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			ksCreate(t, ts, "m")
			status, out := ksSeed(t, ts, `{"streamName":"m","pendingObservations":1}`)
			require.Equal(t, http.StatusOK, status, "%s", out)
			require.Equal(t, "CREATING", ksStatus(t, ts, "m", true))
			require.Equal(t, "ACTIVE", ksStatus(t, ts, "m", true))

			body := map[string]any{"StreamName": "m", "ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000001"}
			if op == "SplitShard" {
				body = map[string]any{"StreamName": "m", "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "10"}
			}
			ksOK(t, ts, op, body)
			status, out = ksCall(t, ts, op, body)
			require.Equal(t, http.StatusBadRequest, status, "%s mid-transition: %s", op, out)
			assert.Contains(t, string(out), "ResourceInUseException")
			assert.Equal(t, "UPDATING", ksStatus(t, ts, "m", true))
			assert.Equal(t, "ACTIVE", ksStatus(t, ts, "m", true))
		})
	}
}

// A wildcard seed is spent per stream, so describing one stream does not use up another's (#582).
func TestKinesisStreamStatus_CountsPerStream(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, out := ksSeed(t, ts, `{"streamName":"*","pendingObservations":1}`)
	require.Equal(t, http.StatusOK, status, "%s", out)
	ksCreate(t, ts, "a")
	ksCreate(t, ts, "b")
	assert.Equal(t, "CREATING", ksStatus(t, ts, "a", true))
	assert.Equal(t, "CREATING", ksStatus(t, ts, "b", true), "b's countdown is its own")
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "a", true))
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "b", true))
}

// An ARN seed beats a name seed, which beats the wildcard.
func TestKinesisStreamStatus_ARNBeatsNameBeatsWildcard(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksCreate(t, ts, "p")
	ksCreate(t, ts, "q")
	out := ksOK(t, ts, "DescribeStreamSummary", map[string]any{"StreamName": "p"})
	var doc struct {
		Summary struct {
			StreamARN string `json:"StreamARN"`
		} `json:"StreamDescriptionSummary"`
	}
	require.NoError(t, json.Unmarshal(out, &doc))
	arn := doc.Summary.StreamARN
	require.NotEmpty(t, arn)

	for _, seed := range []string{
		`{"pendingObservations":5}`,
		`{"streamName":"p","pendingObservations":3}`,
		`{"streamARN":"` + arn + `","pendingObservations":1}`,
	} {
		status, body := ksSeed(t, ts, seed)
		require.Equal(t, http.StatusOK, status, "%s", body)
	}
	assert.Equal(t, "CREATING", ksStatus(t, ts, "p", true))
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "p", true), "p is governed by its ARN seed, one observation")
	for i := range 5 {
		assert.Equal(t, "CREATING", ksStatus(t, ts, "q", true), "q is governed by the wildcard, observation %d", i+1)
	}
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "q", true))

	// Clearing the ARN seed hands p back to its name seed, restarted.
	ksClear(t, ts, arn)
	for range 3 {
		assert.Equal(t, "CREATING", ksStatus(t, ts, "p", true))
	}
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "p", true))

	// Clearing every seed makes each stream read its record at once.
	ksClear(t, ts, "")
	ksCreate(t, ts, "r")
	assert.Equal(t, "ACTIVE", ksStatus(t, ts, "r", true))
}

func TestKinesisStreamStatus_SeedValidation(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, tc := range []struct {
		name, body string
	}{
		{"negative count", `{"pendingObservations":-1}`},
		{"not a stream ARN", `{"streamARN":"arn:aws:sqs:us-east-1:123456789012:q","pendingObservations":1}`},
		{"name disagrees with ARN", `{"streamName":"x","streamARN":"arn:aws:kinesis:us-east-1:123456789012:stream/y","pendingObservations":1}`},
		{"not JSON", `{`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, out := ksSeed(t, ts, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s", out)
		})
	}
}

// The seed is a control-plane write recorded in the stream; a replay that re-applies it reproduces
// the recorded CREATING, UPDATING and ACTIVE sequence, and one that withholds it does not.
func TestKinesisStreamStatus_AReplayReproducesTheSequence(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	status, out := ksSeed(t, ts, `{"pendingObservations":2}`)
	require.Equal(t, http.StatusOK, status, "%s", out)
	ksCreate(t, ts, "rep")
	recorded := []string{ksStatus(t, ts, "rep", true), ksStatus(t, ts, "rep", false), ksStatus(t, ts, "rep", true)}
	status, out = ksReshard(t, ts, "rep", 4)
	require.Equal(t, http.StatusOK, status, "%s", out)
	recorded = append(recorded, ksStatus(t, ts, "rep", true), ksStatus(t, ts, "rep", true), ksStatus(t, ts, "rep", true))
	require.Equal(t, []string{"CREATING", "CREATING", "ACTIVE", "UPDATING", "UPDATING", "ACTIVE"}, recorded,
		"the recording has to observe the sequence, or the replay has nothing to reproduce")

	withSeed := emulator.NewReplayEngine(ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.WithControlPlaneHandler(ts.ControlPlaneHandler()))
	results, err := withSeed.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences, "the replayed describes answer the recorded statuses: %s", replayDifferenceSummary(results))

	// The control: withheld, the seed is skipped and the replayed describes answer ACTIVE where the
	// recording answered CREATING and UPDATING.
	withoutSeed := emulator.NewReplayEngine(ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false))
	results, err = withoutSeed.Replay(t.Context(), "default")
	require.NoError(t, err)
	assert.Equal(t, 1, results.SkippedEvents, "the seed is the one event withheld")
	assert.NotEmpty(t, results.Differences, "without the seed the transient statuses are not reproduced")
}

// A store fault on the progression's seed or counter keys is an error, never answered as a status.
func TestKinesisStreamStatus_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
	}{
		{"describe, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "DescribeStream"},
		{"describe, counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, "DescribeStreamSummary"},
		{"describe, corrupt counter", func(m *cfFaultStateManager) { m.corruptGet = "observed:" }, "DescribeStream"},
		{"describe, counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, "DescribeStream"},
		{"reshard, precondition read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "UpdateShardCount"},
		{"reshard, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, "UpdateShardCount"},
		{"merge, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, "MergeShards"},
		{"split, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, "SplitShard"},
		{"delete, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, "DeleteStream"},
		{"create, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, "CreateStream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.KinesisPlugin{}
			ctx, _ := wireSetup(t, p, "req-ks-fault")
			// wireSetup initialized p over its own store; re-initialize over the faulting one.
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State: fault, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
			}))
			call := func(op string, body map[string]any) error {
				raw, err := json.Marshal(body)
				require.NoError(t, err)
				_, err = p.HandleRequest(ctx, &emulator.AWSRequest{
					Service: "kinesis", Operation: op, Path: "/", Body: raw,
					Headers: map[string]string{"X-Amz-Target": "Kinesis_20131202." + op},
					Params:  map[string]string{},
				})
				return err
			}
			if tc.op != "CreateStream" {
				require.NoError(t, call("CreateStream", map[string]any{"StreamName": "f", "ShardCount": 2}))
			}
			// A describe needs a countdown still running, so its counter is read and written; every
			// other case needs one already exhausted, so the reshard precondition passes and the fault
			// lands on the counter reset that follows.
			seed := `{"pendingObservations":0}`
			if strings.HasPrefix(tc.op, "Describe") {
				seed = `{"pendingObservations":1}`
			}
			require.NoError(t, fault.Put(t.Context(), "kinesis-stream-status-ctrl", "status:*", []byte(seed)))
			tc.arm(fault)
			body := map[string]any{"StreamName": "f"}
			switch tc.op {
			case "CreateStream":
				body["ShardCount"] = 1
			case "UpdateShardCount":
				body["TargetShardCount"], body["ScalingType"] = 4, "UNIFORM_SCALING"
			case "MergeShards":
				body["ShardToMerge"], body["AdjacentShardToMerge"] = "shardId-000000000000", "shardId-000000000001"
			case "SplitShard":
				body["ShardToSplit"], body["NewStartingHashKey"] = "shardId-000000000000", "10"
			}
			err := call(tc.op, body)
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
