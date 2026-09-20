package emulator_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1217: a replayed timestamp is the recorded one, not a value near it.
//
// The defect was in the gap between two sentences that read as though they said the
// same thing. replayEvent called SetTime(event.Timestamp), and SetTime sets a
// *baseline* the clock then advances from at its scale — so a handler reading Now()
// while replaying saw event.Timestamp plus the wall-clock latency of the replay
// dispatch. Rendered at second resolution, which is what RFC3339 without fractional
// seconds gives, that agreed with the recording whenever the two reads landed in the
// same second and differed by one second when they straddled a boundary.
//
// It surfaced as a flake in TestReplayBodyDiff_ARecordedListingReplaysByteIdentically
// on a documentation-only PR, which is the worst version of it: nothing in the change
// under test was anywhere near the clock.
//
// # Every other replay assertion, and whether it was exposed
//
// #1217's fourth criterion asks for the survey, so here it is. Five places assert
// something about a replay's differences:
//
//   - TestReplayBodyDiff_ARecordedListingReplaysByteIdentically asserts zero differences
//     over a ListBuckets whose CreationDate renders from the clock. **Exposed** — it is
//     the one that flaked, roughly once per (buckets × latency / 1s) runs.
//   - TestELBResponseMetadata_AReplayReproducesTheRecordedRequestID replays a classic
//     DescribeLoadBalancers, whose CreatedTime renders from the clock
//     (elb_classic.go:529), but filters its assertion to the ResponseMetadata/RequestId
//     path. **Exposed and silent**: a one-second CreatedTime divergence was tolerated
//     rather than reported, so the flake could not be seen there. The filter is still
//     right for its own reason — #856's minted DNS-name suffix — and is left alone.
//   - TestReplayPipeline's fault-rewind case asserts zero differences over PutBucket
//     recordings, whose bodies carry no rendered timestamp. **Not exposed**, and now
//     protected if one is ever added.
//   - coverage_test.go's error-match case replays an event with no plugin and no body.
//     **Not exposed.**
//   - test/e2e/journey_replay_test.go logs the difference count without asserting on it.
//     **Not exposed**, and would not have caught it.

// boundaryClock is one nanosecond before a second boundary.
//
// The value is the whole method of TestReplay_ARecordedTimestampAtASecondBoundary...
// below: with the recording pinned here, *any* forward motion at all during the replay
// crosses into the next second, so the old behavior fails on every run rather than
// once in a while. The date is the one from the CI run the defect was found on, which
// makes the failure message recognizable against the issue.
var boundaryClock = time.Date(2026, 9, 20, 0, 36, 20, int(time.Second-1), time.UTC)

// TestReplay_ARecordedTimestampAtASecondBoundaryReplaysExactly is #1217's regression
// test, and it is deterministic where the flake it stands in for was not.
//
// The recording runs on a frozen clock, so the bucket creation dates and the event
// timestamps are all exactly boundaryClock — the recording is reproducible in
// principle, which leaves the replay as the only thing under test. The clock is then
// released, so the replay has to stop it itself; that is the production path, and it is
// what did not happen before. One nanosecond of advance is enough to render
// `00:36:21Z` where the recording says `00:36:20Z`.
func TestReplay_ARecordedTimestampAtASecondBoundaryReplaysExactly(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(boundaryClock)

	for _, bucket := range []string{"boundary-alpha", "boundary-beta"} {
		replayPutBucket(t, ts, bucket)
	}
	require.Contains(t, replayListBuckets(t, ts), "boundary-beta",
		"the recording must contain the listing whose dates are compared")

	// Released before the replay: a replay that only inherited a clock somebody else
	// had already stopped would prove nothing about what replayEvent does.
	ts.UnfreezeTime()
	require.False(t, ts.TimeFrozen())

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Zero(t, results.SkippedEvents)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))

	assert.False(t, ts.TimeFrozen(),
		"the replay restores the clock it froze, so a live emulator is not stopped by having replayed")
}

// TestReplay_AReplayLeavesAnAlreadyFrozenClockFrozen is the other half of the restore:
// a caller that stopped the clock on purpose — which is the only way to make a *live*
// run's timestamps exact too — must not have it started again by a replay running over
// the same controller.
func TestReplay_AReplayLeavesAnAlreadyFrozenClockFrozen(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(boundaryClock)

	replayPutBucket(t, ts, "still-frozen")
	require.Contains(t, replayListBuckets(t, ts), "still-frozen")

	engine := pipelineReplayEngine(ts, emulator.ReplayPipeline{})
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))

	assert.True(t, ts.TimeFrozen(), "the replay restores the frozen state it found, not the unfrozen one")
	assert.Equal(t, boundaryClock, ts.TimeController().Now(),
		"and the clock is still where it was stopped")
}

// TestTimeController_FrozenNowDoesNotAdvance is the unit-level statement of the
// property, without a replay in the way.
//
// Two reads of an unfrozen Now() always differ — time.Since has nanosecond resolution
// — so equality here is a real assertion and not one that passes by accident. That is
// also why it needs no sleep, which CLAUDE.md's testing rule rules out depending on.
func TestTimeController_FrozenNowDoesNotAdvance(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(boundaryClock)

	require.False(t, tc.Frozen())
	assert.NotEqual(t, tc.Now(), tc.Now(), "an unfrozen clock advances between two reads")

	tc.Freeze()
	require.True(t, tc.Frozen())
	frozen := tc.Now()
	for range 1000 {
		require.Equal(t, frozen, tc.Now())
	}

	tc.Unfreeze()
	require.False(t, tc.Frozen())
	assert.True(t, tc.Now().After(frozen), "and resumes from where it stopped rather than jumping back")
}

// TestTimeController_FreezeThenSetTimeReadsTheSetTimeExactly is the combination
// replayEvent uses, and the one SetTime alone could not give it.
func TestTimeController_FreezeThenSetTimeReadsTheSetTimeExactly(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(time.Now())
	tc.Freeze()
	tc.SetTime(boundaryClock)

	assert.Equal(t, boundaryClock, tc.Now(), "frozen, SetTime sets what the clock reads")
	assert.Equal(t, "2026-09-20T00:36:20Z", tc.Now().Format(time.RFC3339),
		"which is the rendering the flake turned into 00:36:21Z")
	assert.True(t, tc.Frozen(), "SetTime preserves the frozen state")
}

// TestTimeController_FreezeIsIdempotentAndDoesNotJump covers the capture inside Freeze:
// freezing stops the clock where it currently reads, and freezing again is a no-op
// rather than a second capture.
func TestTimeController_FreezeIsIdempotentAndDoesNotJump(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(boundaryClock)
	tc.SetScale(1000)

	before := tc.Now()
	tc.Freeze()
	first := tc.Now()
	assert.False(t, first.Before(before), "freezing captures the time the clock reads, so it cannot go back")

	tc.Freeze()
	assert.Equal(t, first, tc.Now(), "freezing a frozen clock changes nothing")

	tc.Unfreeze()
	tc.Unfreeze()
	assert.False(t, tc.Frozen(), "and unfreezing twice is not a re-freeze")
}

// TestTimeController_SetScaleOnAFrozenClockDoesNotStartIt pins the guard in SetScale.
//
// Without it, changing the scale would advance the baseline by the wall interval since
// the freeze — undoing the freeze in the act of configuring it, which is the kind of
// interaction that made the original defect invisible.
func TestTimeController_SetScaleOnAFrozenClockDoesNotStartIt(t *testing.T) {
	t.Parallel()
	tc := emulator.NewTimeController(boundaryClock)
	tc.Freeze()
	frozen := tc.Now()

	tc.SetScale(86400)
	assert.Equal(t, frozen, tc.Now(), "a frozen clock stays where it is")
	assert.InDelta(t, 86400.0, tc.Scale(), 0, "and the new scale is recorded for when it is released")
	assert.True(t, tc.Frozen())

	tc.Unfreeze()
	assert.False(t, tc.Now().Before(frozen), "released, it resumes forward at the scale that was set")
}
