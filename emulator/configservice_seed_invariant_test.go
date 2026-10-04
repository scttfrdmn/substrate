package emulator_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The invariant emulator/configservice_control.go's header states for every AWS Config seed: each
// "is applied at *read* time rather than written into the resource — so clearing a seed restores
// the real state instead of leaving the seeded value behind" (#1320).

// configSeedInvariantClock is the instant both servers in a comparison run at, on a frozen clock,
// so the times each operation stamps are equal and the stored records can be compared as bytes.
var configSeedInvariantClock = time.Unix(1700000000, 0).UTC()

// configStoredRecorderStatus returns the raw stored recorder-status record, failing if there is not
// exactly one.
func configStoredRecorderStatus(t *testing.T, ts *emulator.TestServer) []byte {
	t.Helper()
	keys, err := ts.StateManager().List(t.Context(), "config", "recorder_status:")
	require.NoError(t, err, "list recorder status")
	require.Len(t, keys, 1, "want one stored recorder status, have %v", keys)
	data, err := ts.StateManager().Get(t.Context(), "config", keys[0])
	require.NoError(t, err, "get %s", keys[0])
	return data
}

// configFailureSeed is a recorder-status seed in the failing form a consumer's error branch needs.
var configFailureSeed = map[string]any{
	"lastStatus": "Failure", "lastErrorCode": "InsufficientDeliveryPolicy", "lastErrorMessage": "nope",
}

// TestConfigSeeds_AreAppliedAtReadTimeNotWrittenIntoTheResource names the invariant. Every sequence
// of recorder writes is run twice, once with no seed and once with the recorder-status seed in place
// throughout, and after the seed is cleared the two stored records must be byte-identical. A writer
// that persists the seeded view, as Start and Stop did until #1320, fails here whichever operation it
// is.
func TestConfigSeeds_AreAppliedAtReadTimeNotWrittenIntoTheResource(t *testing.T) {
	t.Parallel()
	start := func(t *testing.T, ts *emulator.TestServer) { configStartRecorder(t, ts, "default") }
	stop := func(t *testing.T, ts *emulator.TestServer) { configStopRecorder(t, ts, "default") }
	for _, tc := range []struct {
		name  string
		steps []func(*testing.T, *emulator.TestServer)
	}{
		{"start", []func(*testing.T, *emulator.TestServer){start}},
		{"start, stop", []func(*testing.T, *emulator.TestServer){start, stop}},
		{"start, stop, start", []func(*testing.T, *emulator.TestServer){start, stop, start}},
		{"start, stop, start, stop", []func(*testing.T, *emulator.TestServer){start, stop, start, stop}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := func(seeded bool) *emulator.TestServer {
				ts := emulator.StartTestServer(t)
				ts.FreezeTimeAt(configSeedInvariantClock)
				configPutRecorder(t, ts, "default")
				configPutChannel(t, ts, "default", "cfg-invariant")
				if seeded {
					configSeed(t, ts, "/v1/config/recorder-status", configFailureSeed)
				}
				for _, step := range tc.steps {
					step(t, ts)
				}
				if seeded {
					configClearSeed(t, ts, "/v1/config/recorder-status")
				}
				return ts
			}
			unseeded, seeded := run(false), run(true)
			assert.JSONEq(t, string(configStoredRecorderStatus(t, unseeded)), string(configStoredRecorderStatus(t, seeded)),
				"the stored recorder status must not depend on a seed that has been cleared")
			assert.Equal(t, configDescribeRecorderStatus(t, unseeded), configDescribeRecorderStatus(t, seeded),
				"DescribeConfigurationRecorderStatus must not depend on a seed that has been cleared")
		})
	}
}

// TestConfigRecorder_ClearingTheStatusSeedRestoresTheRealStatus is #1320's reproduction: a Stop while
// the seed is in place, then a Start while it is still in place, then the clear.
func TestConfigRecorder_ClearingTheStatusSeedRestoresTheRealStatus(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "zz-logs")
	configStartRecorder(t, ts, "default")

	configSeed(t, ts, "/v1/config/recorder-status", configFailureSeed)
	configStopRecorder(t, ts, "default")
	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, "Failure", statuses[0]["lastStatus"], "while seeded, the seed answers")

	configClearSeed(t, ts, "/v1/config/recorder-status")
	statuses = configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, "Success", statuses[0]["lastStatus"], "the real status the recorder had before the seed")
	assert.Empty(t, statuses[0]["lastErrorCode"], "a Stop made while seeded must not persist the seeded code")
	assert.Empty(t, statuses[0]["lastErrorMessage"], "a Stop made while seeded must not persist the seeded message")
	assert.Equal(t, false, statuses[0]["recording"], "the Stop itself is real and stays")

	configSeed(t, ts, "/v1/config/recorder-status", configFailureSeed)
	configStartRecorder(t, ts, "default")
	configClearSeed(t, ts, "/v1/config/recorder-status")
	statuses = configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, "Success", statuses[0]["lastStatus"])
	assert.Empty(t, statuses[0]["lastErrorCode"], "a Start made while seeded must not persist the seeded code")
	assert.Equal(t, true, statuses[0]["recording"])
}

// TestConfigRecorder_StartNeverPersistsAnErrorWithoutAFailure covers the sharper half of #1320: a
// Start persisted Success alongside the seeded lastErrorCode/lastErrorMessage, the combination the
// recorder-status seed itself refuses. The assertion is on the stored record, which is where the
// combination lived while every read was masked by the seed.
func TestConfigRecorder_StartNeverPersistsAnErrorWithoutAFailure(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "zz-logs")
	configSeed(t, ts, "/v1/config/recorder-status", configFailureSeed)
	configStartRecorder(t, ts, "default")

	configRequireStoredSuccessWithoutError(t, ts)
}

// TestConfigRecorder_StartClearsAnErrorAStatusRecordedBeforeTheFixStillHolds covers a status stored
// by a substrate before #1320, which may already hold the seeded pair on a stopped recorder. The
// record is written directly, the way such a recording would replay it, and a Start must not carry
// the pair forward under Success.
func TestConfigRecorder_StartClearsAnErrorAStatusRecordedBeforeTheFixStillHolds(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "zz-logs")
	configStartRecorder(t, ts, "default")
	configStopRecorder(t, ts, "default")

	keys, err := ts.StateManager().List(t.Context(), "config", "recorder_status:")
	require.NoError(t, err, "list recorder status")
	require.Len(t, keys, 1)
	var legacy map[string]any
	require.NoError(t, json.Unmarshal(configStoredRecorderStatus(t, ts), &legacy))
	legacy["lastStatus"], legacy["lastErrorCode"], legacy["lastErrorMessage"] = "Failure", "InsufficientDeliveryPolicy", "nope"
	data, err := json.Marshal(legacy)
	require.NoError(t, err, "marshal legacy status")
	require.NoError(t, ts.StateManager().Put(t.Context(), "config", keys[0], data), "write legacy status")

	configStartRecorder(t, ts, "default")
	configRequireStoredSuccessWithoutError(t, ts)
}

// configRequireStoredSuccessWithoutError requires the stored recorder status to be Success with no
// error code or message.
func configRequireStoredSuccessWithoutError(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	var stored map[string]json.RawMessage
	raw := configStoredRecorderStatus(t, ts)
	require.NoError(t, json.Unmarshal(raw, &stored), "decode stored status: %s", raw)
	require.JSONEq(t, `"Success"`, string(stored["lastStatus"]), "the stored status: %s", raw)
	for _, member := range []string{"lastErrorCode", "lastErrorMessage"} {
		if v, ok := stored[member]; ok {
			assert.JSONEqf(t, `""`, string(v), "a stored Success must carry no %s: %s", member, raw)
		}
	}
}
