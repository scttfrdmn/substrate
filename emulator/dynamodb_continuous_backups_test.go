package emulator_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ddbPITRStart is the fixed simulated time the continuous-backups tests start from.
var ddbPITRStart = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// newDynamoDBClockServer is newDynamoDBTestServer on a clock the test controls.
func newDynamoDBClockServer(t *testing.T) (*emulator.Server, *emulator.TimeController) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(ddbPITRStart)
	tc.SetScale(0)

	plugin := &emulator.DynamoDBPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), emulator.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)
	return emulator.NewServer(*cfg, registry, store, state, tc, logger), tc
}

type ddbContinuousBackups struct {
	ContinuousBackupsDescription struct {
		ContinuousBackupsStatus        string
		PointInTimeRecoveryDescription map[string]any
	}
}

func ddbCreatePITRTable(t *testing.T, srv *emulator.Server, name string) {
	t.Helper()
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName":            name,
		"KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
		"BillingMode":          "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func ddbContinuousBackupsCall(t *testing.T, srv *emulator.Server, op string, body map[string]any) ddbContinuousBackups {
	t.Helper()
	resp := dynamodbRequest(t, srv, op, body)
	var out ddbContinuousBackups
	decodeDynamoJSON(t, resp, &out)
	return out
}

func TestDynamoDB_ContinuousBackupsLifecycle(t *testing.T) {
	t.Parallel()
	srv, tc := newDynamoDBClockServer(t)
	ddbCreatePITRTable(t, srv, "pitr")

	// "Continuous backups are ENABLED on all tables at table creation"; PITR is off until enabled.
	got := ddbContinuousBackupsCall(t, srv, "DescribeContinuousBackups", map[string]any{"TableName": "pitr"})
	assert.Equal(t, "ENABLED", got.ContinuousBackupsDescription.ContinuousBackupsStatus)
	assert.Equal(t, map[string]any{"PointInTimeRecoveryStatus": "DISABLED"},
		got.ContinuousBackupsDescription.PointInTimeRecoveryDescription)

	enabledAt := float64(ddbPITRStart.Unix())
	got = ddbContinuousBackupsCall(t, srv, "UpdateContinuousBackups", map[string]any{
		"TableName":                        "pitr",
		"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": true},
	})
	pitr := got.ContinuousBackupsDescription.PointInTimeRecoveryDescription
	assert.Equal(t, "ENABLED", pitr["PointInTimeRecoveryStatus"])
	assert.InDelta(t, 35, pitr["RecoveryPeriodInDays"], 0, "the default recovery period")
	assert.InDelta(t, enabledAt, pitr["EarliestRestorableDateTime"], 0.001, "nothing is restorable before enabling")
	assert.InDelta(t, enabledAt, pitr["LatestRestorableDateTime"], 0.001, "the latest is never before the earliest")

	// An hour on, the latest restorable time trails the clock by five minutes.
	tc.SetTime(ddbPITRStart.Add(time.Hour))
	pitr = ddbContinuousBackupsCall(t, srv, "DescribeContinuousBackups",
		map[string]any{"TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/pitr"}).
		ContinuousBackupsDescription.PointInTimeRecoveryDescription
	assert.InDelta(t, enabledAt, pitr["EarliestRestorableDateTime"], 0.001)
	assert.InDelta(t, enabledAt+3600-300, pitr["LatestRestorableDateTime"], 0.001)

	// Forty days on, with a seven-day period, the window is the last seven days. Re-enabling keeps the
	// window rather than restarting it.
	tc.SetTime(ddbPITRStart.Add(40 * 24 * time.Hour))
	now := float64(ddbPITRStart.Add(40 * 24 * time.Hour).Unix())
	pitr = ddbContinuousBackupsCall(t, srv, "UpdateContinuousBackups", map[string]any{
		"TableName": "pitr",
		"PointInTimeRecoverySpecification": map[string]any{
			"PointInTimeRecoveryEnabled": true, "RecoveryPeriodInDays": 7,
		},
	}).ContinuousBackupsDescription.PointInTimeRecoveryDescription
	assert.InDelta(t, 7, pitr["RecoveryPeriodInDays"], 0)
	assert.InDelta(t, now-7*86400, pitr["EarliestRestorableDateTime"], 0.001)
	assert.InDelta(t, now-300, pitr["LatestRestorableDateTime"], 0.001)

	// Re-enabling without a period asks for the default, not the previous seven days: Terraform sends
	// no period when none is configured and would otherwise never converge.
	pitr = ddbContinuousBackupsCall(t, srv, "UpdateContinuousBackups", map[string]any{
		"TableName":                        "pitr",
		"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": true},
	}).ContinuousBackupsDescription.PointInTimeRecoveryDescription
	assert.InDelta(t, 35, pitr["RecoveryPeriodInDays"], 0, "an omitted period is the default")
	assert.InDelta(t, now-35*86400, pitr["EarliestRestorableDateTime"], 0.001,
		"the window is not restarted: it reaches the full default period back")

	got = ddbContinuousBackupsCall(t, srv, "UpdateContinuousBackups", map[string]any{
		"TableName":                        "pitr",
		"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": false},
	})
	assert.Equal(t, map[string]any{"PointInTimeRecoveryStatus": "DISABLED"},
		got.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
}

func TestDynamoDB_ContinuousBackupsRefusals(t *testing.T) {
	t.Parallel()
	srv, _ := newDynamoDBClockServer(t)
	ddbCreatePITRTable(t, srv, "pitr-refusals")

	cases := []struct {
		name string
		op   string
		body string
		code string
	}{
		{"describe absent table", "DescribeContinuousBackups", `{"TableName":"nope"}`, "TableNotFoundException"},
		{"describe no name", "DescribeContinuousBackups", `{}`, "ValidationException"},
		{"describe wrong type", "DescribeContinuousBackups", `{"TableName":7}`, "SerializationException"},
		{"update absent table", "UpdateContinuousBackups",
			`{"TableName":"nope","PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}`, "TableNotFoundException"},
		{"update no spec", "UpdateContinuousBackups", `{"TableName":"pitr-refusals"}`, "ValidationException"},
		{"update no enabled", "UpdateContinuousBackups",
			`{"TableName":"pitr-refusals","PointInTimeRecoverySpecification":{}}`, "ValidationException"},
		{"update period too long", "UpdateContinuousBackups",
			`{"TableName":"pitr-refusals","PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true,"RecoveryPeriodInDays":36}}`,
			"ValidationException"},
		{"update period zero", "UpdateContinuousBackups",
			`{"TableName":"pitr-refusals","PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true,"RecoveryPeriodInDays":0}}`,
			"ValidationException"},
		{"update wrong type", "UpdateContinuousBackups", `{"TableName":7}`, "SerializationException"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := dynamodbRequest(t, srv, tc.op, rawJSON(tc.body))
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", raw)
			assert.Contains(t, string(raw), tc.code)
		})
	}
}

// rawJSON is a request body that json.Marshal writes through verbatim.
type rawJSON string

// MarshalJSON returns the body as written.
func (r rawJSON) MarshalJSON() ([]byte, error) { return []byte(r), nil }

// CreateTable answered a null TableId (#1608): the member is a UUID minted from the request, and every
// table description reports it.
func TestDynamoDB_TableIDIsMintedAndReported(t *testing.T) {
	t.Parallel()
	srv := newDynamoDBTestServer(t)
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName":            "with-id",
		"KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
	})
	var created struct{ TableDescription struct{ TableId string } }
	decodeDynamoJSON(t, resp, &created)
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, created.TableDescription.TableId)

	var described struct{ Table struct{ TableId string } }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "DescribeTable", map[string]any{"TableName": "with-id"}), &described)
	assert.Equal(t, created.TableDescription.TableId, described.Table.TableId, "the id is stored, not re-minted")
}

// UpdateContinuousBackups and TagResource each load, change and save the table record. Without the
// table's stripe held across all three, one writer's save dropped the other's change.
func TestDynamoDB_TableRecordWritersDoNotLoseUpdates(t *testing.T) {
	t.Parallel()
	srv, _ := newDynamoDBClockServer(t)
	ddbCreatePITRTable(t, srv, "pitr-race")
	const arn = "arn:aws:dynamodb:us-east-1:123456789012:table/pitr-race"
	const n = 20

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(2)
		go func() {
			defer wg.Done()
			resp := dynamodbRequest(t, srv, "TagResource", map[string]any{
				"ResourceArn": arn, "Tags": []map[string]string{{"Key": fmt.Sprintf("k%02d", i), "Value": "v"}},
			})
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.NoError(t, resp.Body.Close())
		}()
		go func() {
			defer wg.Done()
			resp := dynamodbRequest(t, srv, "UpdateContinuousBackups", map[string]any{
				"TableName":                        "pitr-race",
				"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": true},
			})
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.NoError(t, resp.Body.Close())
		}()
	}
	wg.Wait()

	var tags struct{ Tags []struct{ Key string } }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "ListTagsOfResource", map[string]any{"ResourceArn": arn}), &tags)
	assert.Len(t, tags.Tags, n, "every tag survived the concurrent PITR writes")
	pitr := ddbContinuousBackupsCall(t, srv, "DescribeContinuousBackups", map[string]any{"TableName": "pitr-race"}).
		ContinuousBackupsDescription.PointInTimeRecoveryDescription
	assert.Equal(t, "ENABLED", pitr["PointInTimeRecoveryStatus"], "PITR survived the concurrent tag writes")
}
