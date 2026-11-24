package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1613 and #1539: retention period and maximum record size.

// ksSummaryInt reads one integer member of DescribeStreamSummary.
func ksSummaryInt(t *testing.T, ts *emulator.TestServer, name, member string) int {
	t.Helper()
	out := ksOK(t, ts, "DescribeStreamSummary", map[string]any{"StreamName": name})
	var doc struct {
		Summary map[string]json.RawMessage `json:"StreamDescriptionSummary"`
	}
	require.NoError(t, json.Unmarshal(out, &doc), "%s", out)
	var n int
	require.NoError(t, json.Unmarshal(doc.Summary[member], &n), "%s: %s", member, out)
	return n
}

func TestKinesisRetention_IncreaseAndDecrease(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "ret", "ShardCount": 1})
	assert.Equal(t, 24, ksSummaryInt(t, ts, "ret", "RetentionPeriodHours"))

	ksOK(t, ts, "IncreaseStreamRetentionPeriod", map[string]any{"StreamName": "ret", "RetentionPeriodHours": 8760})
	assert.Equal(t, 8760, ksSummaryInt(t, ts, "ret", "RetentionPeriodHours"))
	ksOK(t, ts, "DecreaseStreamRetentionPeriod", map[string]any{"StreamARN": ksTestARN("ret"), "RetentionPeriodHours": 24})
	assert.Equal(t, 24, ksSummaryInt(t, ts, "ret", "RetentionPeriodHours"))

	for name, tc := range map[string]struct {
		op    string
		hours any
		code  string
	}{
		"increase to the current period":   {"IncreaseStreamRetentionPeriod", 24, "InvalidArgumentException"},
		"increase above 8760":              {"IncreaseStreamRetentionPeriod", 8761, "InvalidArgumentException"},
		"decrease to the current period":   {"DecreaseStreamRetentionPeriod", 24, "InvalidArgumentException"},
		"decrease below 24":                {"DecreaseStreamRetentionPeriod", 23, "InvalidArgumentException"},
		"increase with no period":          {"IncreaseStreamRetentionPeriod", nil, "InvalidArgumentException"},
		"increase on a missing stream":     {"IncreaseStreamRetentionPeriod", 48, "ResourceNotFoundException"},
		"decrease a period it has not got": {"DecreaseStreamRetentionPeriod", 30, "InvalidArgumentException"},
	} {
		t.Run(name, func(t *testing.T) {
			body := map[string]any{"StreamName": "ret"}
			if strings.Contains(name, "missing") {
				body["StreamName"] = "missing"
			}
			if tc.hours != nil {
				body["RetentionPeriodHours"] = tc.hours
			}
			ksRefusal(t, ts, tc.op, body, tc.code)
		})
	}

	ksSeed(t, ts, `{"streamName":"busy","pendingObservations":1}`)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "busy", "ShardCount": 1})
	ksRefusal(t, ts, "IncreaseStreamRetentionPeriod", map[string]any{"StreamName": "busy", "RetentionPeriodHours": 48},
		"ResourceInUseException")
}

func TestKinesisUpdateMaxRecordSize(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ksOK(t, ts, "CreateStream", map[string]any{"StreamName": "big", "ShardCount": 1})
	assert.Equal(t, 1024, ksSummaryInt(t, ts, "big", "MaxRecordSizeInKiB"))

	twoMiB := base64.StdEncoding.EncodeToString(make([]byte, 2<<20))
	put := map[string]any{"StreamName": "big", "PartitionKey": "pk", "Data": twoMiB}
	ksRefusal(t, ts, "PutRecord", put, "InvalidArgumentException")

	ksOK(t, ts, "UpdateMaxRecordSize", map[string]any{"StreamARN": ksTestARN("big"), "MaxRecordSizeInKiB": 10240})
	assert.Equal(t, 10240, ksSummaryInt(t, ts, "big", "MaxRecordSizeInKiB"))
	ksOK(t, ts, "PutRecord", put)

	for name, tc := range map[string]struct {
		body map[string]any
		code string
	}{
		"below 1024":       {map[string]any{"StreamARN": ksTestARN("big"), "MaxRecordSizeInKiB": 1023}, "ValidationException"},
		"above 10240":      {map[string]any{"StreamARN": ksTestARN("big"), "MaxRecordSizeInKiB": 10241}, "ValidationException"},
		"no size":          {map[string]any{"StreamARN": ksTestARN("big")}, "InvalidArgumentException"},
		"no StreamARN":     {map[string]any{"MaxRecordSizeInKiB": 2048}, "InvalidArgumentException"},
		"a bad StreamARN":  {map[string]any{"StreamARN": "arn:aws:s3:::b", "MaxRecordSizeInKiB": 2048}, "InvalidArgumentException"},
		"a missing stream": {map[string]any{"StreamARN": ksTestARN("nope"), "MaxRecordSizeInKiB": 2048}, "ResourceNotFoundException"},
	} {
		t.Run(name, func(t *testing.T) { ksRefusal(t, ts, "UpdateMaxRecordSize", tc.body, tc.code) })
	}
}
