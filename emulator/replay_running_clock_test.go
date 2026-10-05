package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1396: on a running clock, a replayed date is the one the live request answered.
//
// #1217 made a replay freeze the clock at each event's recorded timestamp, so every read
// during a replayed event returns that instant exactly. That is only half of
// reproducibility. The other half is that the recorded timestamp has to be the instant
// the live handler read. It was not: the event was stamped when it was recorded, after
// the handler had run, and on a running clock every read in between had advanced. A
// handler that rendered `CreationTime` at `…480` live was replayed at the later stamp
// and rendered `…481`. Frozen-clock tests could not see it, because a frozen clock reads
// the same instant at both points.
//
// The fix holds the clock for the duration of each top-level request
// ([TimeController.Hold]). Every read the handler makes, and the event's timestamp,
// return the instant the request began, so the instant a replay pins is the instant the
// handler used.

// runningClockScale makes a running clock advance a simulated second per real
// microsecond. Any two reads of an unheld clock then differ at second resolution, and
// a fortiori at millisecond resolution, so the defect fails on every run rather than when
// two reads happen to straddle a boundary. It is a scale, not a sleep: nothing waits.
const runningClockScale = 1e6

// runningClockJSON issues one awsJson request over the wire and returns its body,
// requiring a 200.
func runningClockJSON(t *testing.T, ts *emulator.TestServer, host, target string, body any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: %s", target, out)
	return out
}

// TestReplay_OnARunningClockEveryRenderedDateReplaysByteIdentically is #1396's regression
// test. It records, on a running clock, responses from three services whose bodies render
// dates from the clock at different resolutions:
//   - S3 `ListBuckets` `CreationDate`;
//   - Kinesis `GetRecords` `ApproximateArrivalTimestamp`, epoch seconds to three decimals;
//   - ACM `DescribeCertificate` `CreatedAt` and its other dates, also epoch seconds.
//
// It then replays the stream and requires no differences. Before the fix it fails on
// every run, because at runningClockScale the stamp recorded after a handler ran is
// always seconds later than the instant the handler read.
func TestReplay_OnARunningClockEveryRenderedDateReplaysByteIdentically(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.TimeController().SetScale(runningClockScale)
	require.False(t, ts.TimeFrozen(), "the defect lives on a running clock; a frozen one cannot show it")

	// S3: a listing that renders each bucket's CreationDate.
	for _, bucket := range []string{"running-alpha", "running-beta"} {
		replayPutBucket(t, ts, bucket)
	}
	require.Contains(t, replayListBuckets(t, ts), "running-beta")

	// Kinesis: a record's ApproximateArrivalTimestamp, rendered as EpochSeconds.
	const kinesis = "kinesis.us-east-1.amazonaws.com"
	runningClockJSON(t, ts, kinesis, "Kinesis_20131202.CreateStream", map[string]any{"StreamName": "running", "ShardCount": 1})
	put := runningClockJSON(t, ts, kinesis, "Kinesis_20131202.PutRecord",
		map[string]any{"StreamName": "running", "PartitionKey": "k", "Data": "cnVubmluZw=="})
	var putOut struct {
		ShardID string `json:"ShardId"`
	}
	require.NoError(t, json.Unmarshal(put, &putOut))
	iter := runningClockJSON(t, ts, kinesis, "Kinesis_20131202.GetShardIterator",
		map[string]any{"StreamName": "running", "ShardId": putOut.ShardID, "ShardIteratorType": "TRIM_HORIZON"})
	var iterOut struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(iter, &iterOut))
	records := runningClockJSON(t, ts, kinesis, "Kinesis_20131202.GetRecords", map[string]any{"ShardIterator": iterOut.ShardIterator})
	require.Contains(t, string(records), "ApproximateArrivalTimestamp")

	// ACM: a certificate's dates, rendered as EpochSeconds.
	const acm = "acm.us-east-1.amazonaws.com"
	cert := runningClockJSON(t, ts, acm, "CertificateManager.RequestCertificate", map[string]any{"DomainName": "running.example.com"})
	var certOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(cert, &certOut))
	described := runningClockJSON(t, ts, acm, "CertificateManager.DescribeCertificate", map[string]any{"CertificateArn": certOut.CertificateArn})
	require.Contains(t, string(described), "CreatedAt")

	require.False(t, ts.TimeFrozen(), "the recording ran on a running clock throughout")

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Zero(t, results.SkippedEvents)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	assert.False(t, ts.TimeFrozen(), "the replay restores the running clock it found")
}

// TestTimeController_AHoldReadsOneInstantAndDoesNotLag is the unit-level statement of the
// hold: while held, every read returns the instant the hold took; released, the clock
// reads exactly where an unheld clock would, not behind it by the held interval.
func TestTimeController_AHoldReadsOneInstantAndDoesNotLag(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(time.Unix(1700000000, 0).UTC())
	tc.SetScale(runningClockScale)

	release := tc.Hold()
	held := tc.Now()
	for range 1000 {
		require.Equal(t, held, tc.Now(), "a held clock returns one instant")
	}
	release()

	after := tc.Now()
	assert.True(t, after.After(held), "released, it advances again")
	// The held interval is not paid back as lag: a fresh controller started at the same
	// baseline and scale would read at least as late as `after` at this point, so `after`
	// must be well past `held` given a scale where a microsecond is a simulated second.
	assert.Greater(t, after.Sub(held), time.Second,
		"the clock kept advancing underneath the hold, so release resumes where an unheld clock would be")
}

// TestTimeController_OnlyTheHoldsOwnerReleasesIt pins the rule that keeps overlapping
// requests from pinning the clock indefinitely: a hold taken while another is in force
// shares its instant, and releasing it does nothing. Only the owner's release ends the
// hold, so a stream of overlapping requests cannot extend one hold forever.
func TestTimeController_OnlyTheHoldsOwnerReleasesIt(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(time.Unix(1700000000, 0).UTC())
	tc.SetScale(runningClockScale)

	releaseOwner := tc.Hold()
	held := tc.Now()
	releaseJoiner := tc.Hold()
	assert.Equal(t, held, tc.Now(), "a hold taken during another shares its instant")

	releaseJoiner()
	assert.Equal(t, held, tc.Now(), "and releasing the joiner leaves the owner's hold in force")

	releaseOwner()
	assert.True(t, tc.Now().After(held), "the owner's release ends it")

	releaseOwner()
	releaseJoiner()
	assert.True(t, tc.Now().After(held), "and a second release of either is a no-op")
}

// TestTimeController_AHoldOnAFrozenClockChangesNothing: a frozen clock already reads one
// instant, so a hold is a no-op on it, and releasing it does not start the clock.
func TestTimeController_AHoldOnAFrozenClockChangesNothing(t *testing.T) {
	t.Parallel()
	start := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(start)
	tc.Freeze()
	tc.SetTime(start)

	release := tc.Hold()
	assert.Equal(t, start, tc.Now())
	release()
	assert.True(t, tc.Frozen(), "a hold does not unfreeze")
	assert.Equal(t, start, tc.Now())
}

// TestTimeController_FreezeDuringAHoldStopsAtTheHeldInstant: Freeze stops the clock at the
// instant Now currently reports, and while held that is the held instant.
func TestTimeController_FreezeDuringAHoldStopsAtTheHeldInstant(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(time.Unix(1700000000, 0).UTC())
	tc.SetScale(runningClockScale)

	release := tc.Hold()
	held := tc.Now()
	tc.Freeze()
	release()
	assert.Equal(t, held, tc.Now(), "frozen at the instant the held clock reported, not where it would have run to")
	tc.Unfreeze()
}
