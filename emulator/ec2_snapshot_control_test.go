package emulator_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #715's surface: a snapshot's status and progress can be seeded so that a consumer's
// poll-until-completed loop has something to poll.
//
// Every assertion is at HTTP level, because the whole of the defect was observable only
// through a response: a snapshot in substrate is born completed, so `aws ec2 wait
// snapshot-completed` — and CDK's, Terraform's and every hand-written equivalent — exits on
// its first iteration and the retry, timeout and error branches it carries are never taken.
// A consumer whose polling is broken passed against substrate and failed against AWS.

// ec2SeedSnapshotStatus POSTs a snapshot-progression seed to the control plane.
func ec2SeedSnapshotStatus(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/ec2/snapshot-status", "application/json", strings.NewReader(body))
	require.NoError(t, err, "seed snapshot status")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed snapshot status = %d: %s", resp.StatusCode, got)
	}
}

// ec2ClearSnapshotStatus DELETEs a progression seed; an empty snapshotID clears every seed.
func ec2ClearSnapshotStatus(t *testing.T, ts *httptest.Server, snapshotID string) {
	t.Helper()
	u := ts.URL + "/v1/ec2/snapshot-status"
	if snapshotID != "" {
		u += "?snapshotId=" + snapshotID
	}
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	require.NoError(t, err, "build clear request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "clear snapshot status")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear snapshot status = %d: %s", resp.StatusCode, got)
	}
}

// ec2DescribeOneSnapshot describes exactly one snapshot by ID, which is what a poll loop does.
//
// Naming the ID matters and is not just narrowing: describeSnapshots advances the countdown of
// every snapshot it observes, so describing the whole region would spend an observation on
// snapshots the test was not polling.
func ec2DescribeOneSnapshot(t *testing.T, ts *httptest.Server, snapshotID string) describedSnapshot {
	t.Helper()
	got := ec2DescribeSnapshots(t, ts, map[string]string{"SnapshotId.1": snapshotID})
	require.Len(t, got, 1)
	return got[0]
}

// TestEC2_SnapshotProgression_PollsToCompleted is the operation's fail-before: without a seed
// the first observation is already "completed", so this whole sequence collapses to one line.
//
// The progress ramp is asserted value by value rather than only "not 100%", because a rising
// percentage is what a consumer that logs or asserts on progress reads — and because a
// single frozen value would satisfy a laxer assertion while being useless to such a consumer.
func TestEC2_SnapshotProgression_PollsToCompleted(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 20, nil)

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":4}`)

	// The create is born pending at 0% — and does not consume an observation, so all four
	// polls below are still available.
	created := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID})
	assert.Equal(t, "pending", created.Status,
		"a snapshot created under a seed is born in the seeded state, so create → poll is testable")
	assert.Equal(t, "0%", created.Progress)

	for i, want := range []string{"0%", "25%", "50%", "75%"} {
		got := ec2DescribeOneSnapshot(t, ts, created.SnapshotID)
		assert.Equal(t, "pending", got.State, "poll %d", i+1)
		assert.Equal(t, want, got.Progress, "poll %d", i+1)
	}

	// The fifth poll is the one the countdown was budgeted for.
	done := ec2DescribeOneSnapshot(t, ts, created.SnapshotID)
	assert.Equal(t, "completed", done.State, "the fifth poll exhausts a four-observation countdown")
	assert.Equal(t, "100%", done.Progress)

	// And it stays completed: the countdown does not restart, so a consumer that polls once
	// more after success is not told the snapshot went back to pending.
	again := ec2DescribeOneSnapshot(t, ts, created.SnapshotID)
	assert.Equal(t, "completed", again.State)
	assert.Equal(t, "100%", again.Progress)
}

// TestEC2_SnapshotProgression_LeavesTheRecordAlone pins the rule the whole design rests on: a
// seed governs what an observation reports and never rewrites the snapshot.
//
// It is what makes the seed safe to leave in place — and what makes "the default is unchanged"
// a fact about one overlay rather than a claim about every write path.
func TestEC2_SnapshotProgression_LeavesTheRecordAlone(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":10}`)
	assert.Equal(t, "pending", ec2DescribeOneSnapshot(t, ts, snapshotID).State)

	ec2ClearSnapshotStatus(t, ts, "")
	got := ec2DescribeOneSnapshot(t, ts, snapshotID)
	assert.Equal(t, "completed", got.State,
		"clearing the seed reveals the record, which was never anything but completed")
	assert.Equal(t, "100%", got.Progress)

	// Re-seeding restarts the countdown rather than resuming the six observations left over
	// from the first seed, so a test that seeds twice gets two full progressions.
	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":1}`)
	first := ec2DescribeOneSnapshot(t, ts, snapshotID)
	assert.Equal(t, "pending", first.State, "a fresh seed starts from the beginning")
	assert.Equal(t, "completed", ec2DescribeOneSnapshot(t, ts, snapshotID).State)
}

// TestEC2_SnapshotProgression_CountsPerSnapshot pins the hazard #582 recorded for the SQS
// seed, met here by keying the countdown position per snapshot even under a "*" seed.
//
// Without that split, one DescribeSnapshots over three snapshots would burn three
// observations off a single shared countdown — so a test seeding two polls would see the
// snapshot it was actually watching complete on its first one.
func TestEC2_SnapshotProgression_CountsPerSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":2}`)

	ids := make([]string, 0, 3)
	for range 3 {
		ids = append(ids, ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID)
	}

	// Two unfiltered describes observe all three snapshots twice, which is exactly the budget.
	for range 2 {
		for _, got := range ec2DescribeSnapshots(t, ts, nil) {
			assert.Equal(t, "pending", got.State, "%s is still within its own countdown", got.SnapshotID)
		}
	}
	for _, got := range ec2DescribeSnapshots(t, ts, nil) {
		assert.Equal(t, "completed", got.State,
			"%s spent its own two observations, not a third of a shared budget", got.SnapshotID)
	}
	assert.Len(t, ids, 3)
}

// TestEC2_SnapshotProgression_ObservedBeforeFiltering pins the ordering describeSnapshots
// documents: the observation is taken, and the countdown advanced, before the filters run.
//
// The CLI's own waiter polls `--filters Name=status,Values=completed`, so a snapshot the
// filter excludes has still been observed. Consuming after the filter instead would leave
// that loop polling a countdown that never advances — a hang rather than a wrong answer.
func TestEC2_SnapshotProgression_ObservedBeforeFiltering(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":2}`)

	completedOnly := map[string]string{
		"SnapshotId.1":  snapshotID,
		"Filter.1.Name": "status", "Filter.1.Value.1": "completed",
	}
	for i := range 2 {
		assert.Empty(t, ec2SnapshotIDsFor(t, ts, completedOnly),
			"poll %d sees a pending snapshot, which the filter excludes", i+1)
	}
	assert.Equal(t, []string{snapshotID}, ec2SnapshotIDsFor(t, ts, completedOnly),
		"the two excluded polls still advanced the countdown, so the waiter terminates")
}

// TestEC2_SnapshotProgression_FiltersReadTheObservation pins that status and progress compare
// against what this request reports, not against the stored record.
//
// Filtering on the record would make `status=pending` select nothing at the very moment the
// caller is being told the snapshot is pending — the one case where the two disagree, and the
// only case a poll loop cares about.
func TestEC2_SnapshotProgression_FiltersReadTheObservation(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":4}`)

	assert.Equal(t, []string{snapshotID}, ec2SnapshotIDsFor(t, ts, ec2Filter("status", "pending")),
		"status=pending selects a snapshot whose observation is pending, though its record is not")
	assert.Equal(t, []string{snapshotID}, ec2SnapshotIDsFor(t, ts, ec2Filter("progress", "25%")),
		"progress compares against the ramp this request rendered")
	assert.Empty(t, ec2SnapshotIDsFor(t, ts, ec2Filter("progress", "100%")),
		"a snapshot mid-countdown is not at 100%")
}

// TestEC2_SnapshotProgression_TerminalStates covers a seed whose final state is not completed,
// which is the outcome a consumer's error branch exists for and the one AWS's own waiter fails
// fast on (botocore's snapshot-completed defines an `error` acceptor).
//
// progress reaches 100% for every terminal state, not only completed: AWS's
// restore-snapshot-from-recycle-bin example shows "Progress": "100%" beside "State":
// "recovering", so progress measures how far the progression ran rather than whether it
// succeeded. A consumer that polls progress instead of status gets that wrong, and this is
// where such a consumer's test catches it.
func TestEC2_SnapshotProgression_TerminalStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		seed        string
		wantState   string
		wantMessage string
	}{
		{
			name:      "error immediately",
			seed:      `{"snapshotId":"*","pendingObservations":0,"finalState":"error"}`,
			wantState: "error",
		},
		{
			name: "error with a diagnostic",
			seed: `{"snapshotId":"*","pendingObservations":0,"finalState":"error",` +
				`"stateMessage":"Given key ID is not accessible"}`,
			wantState:   "error",
			wantMessage: "Given key ID is not accessible",
		},
		{
			name:      "recovering from the recycle bin",
			seed:      `{"snapshotId":"*","pendingObservations":0,"finalState":"recovering"}`,
			wantState: "recovering",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
			ec2SeedSnapshotStatus(t, ts, tc.seed)
			snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

			got := ec2DescribeOneSnapshot(t, ts, snapshotID)
			assert.Equal(t, tc.wantState, got.State,
				"a zero-observation seed is terminal from the first look")
			assert.Equal(t, "100%", got.Progress,
				"progress records how far the progression ran, not whether it succeeded")
			assert.Equal(t, tc.wantMessage, got.StatusMsg)
		})
	}
}

// TestEC2_SnapshotProgression_StatusMessageOnlyOnDescribe pins AWS's own scoping of the
// member: "this parameter is only returned by DescribeSnapshots."
func TestEC2_SnapshotProgression_StatusMessageOnlyOnDescribe(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	ec2SeedSnapshotStatus(t, ts,
		`{"snapshotId":"*","pendingObservations":0,"finalState":"error","stateMessage":"KMS key unavailable"}`)

	resp := ec2Request(t, ts, map[string]string{"Action": "CreateSnapshot", "VolumeId": volumeID})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Contains(t, string(body), "<status>error</status>", "the seed governs what the create reports")
	assert.NotContains(t, string(body), "statusMessage",
		"CreateSnapshot renders no statusMessage, because AWS returns it only from DescribeSnapshots")

	described := ec2DescribeSnapshots(t, ts, nil)
	require.Len(t, described, 1)
	assert.Equal(t, "KMS key unavailable", described[0].StatusMsg,
		"DescribeSnapshots is where the diagnostic is published")
}

// TestEC2_SnapshotProgression_ScopedSeedBeatsTheWildcard pins the resolution order every seed
// in the repo uses: the exact ID first, then "*".
func TestEC2_SnapshotProgression_ScopedSeedBeatsTheWildcard(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	scoped := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID
	other := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":0,"finalState":"error"}`)
	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"`+scoped+`","pendingObservations":1}`)

	assert.Equal(t, "pending", ec2DescribeOneSnapshot(t, ts, scoped).State,
		"the ID-scoped seed governs, not the wildcard")
	assert.Equal(t, "completed", ec2DescribeOneSnapshot(t, ts, scoped).State)
	assert.Equal(t, "error", ec2DescribeOneSnapshot(t, ts, other).State,
		"a snapshot with no seed of its own falls to the wildcard")

	// Clearing the scoped seed hands that snapshot back to the wildcard.
	ec2ClearSnapshotStatus(t, ts, scoped)
	assert.Equal(t, "error", ec2DescribeOneSnapshot(t, ts, scoped).State)
}

// TestEC2_SnapshotProgression_CreateVolumeRefusesAnUnusableSnapshot covers the rule AWS states
// in its snapshot-states table — "a snapshot can't be used while it is in the pending state",
// "a snapshot can't be used if it is in the error state" — with the IncorrectState code
// substrate already answers with for a volume in the wrong state.
func TestEC2_SnapshotProgression_CreateVolumeRefusesAnUnusableSnapshot(t *testing.T) {
	t.Parallel()

	for _, state := range []string{"pending", "error"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			volumeID := ec2CreateSizedVolume(t, ts, 20, nil)
			snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

			restore := map[string]string{
				"Action": "CreateVolume", "AvailabilityZone": "us-east-1a", "SnapshotId": snapshotID,
			}

			// Unseeded, the restore succeeds — which is what makes the refusal below a
			// consequence of the seed rather than of anything else in the request.
			var ok struct {
				VolumeID string `xml:"volumeId"`
			}
			ec2FleetXML(t, ts, restore, &ok)
			require.NotEmpty(t, ok.VolumeID)

			if state == "pending" {
				ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":5}`)
			} else {
				ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":0,"finalState":"error"}`)
			}

			status, code, message := ec2ErrorDetail(t, ts, restore)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "IncorrectState", code)
			assert.Contains(t, message, state)
			assert.Contains(t, message, snapshotID)
		})
	}
}

// TestEC2_SnapshotProgression_CreateVolumeDoesNotSpendAPoll pins that the refusal above reads
// the state without consuming an observation.
//
// Otherwise "pendingObservations: 2" would mean two polls in a test that only describes and
// one in a test that also tries a restore — and a consumer's retry loop, which alternates
// CreateVolume and DescribeSnapshots, would race its own budget.
func TestEC2_SnapshotProgression_CreateVolumeDoesNotSpendAPoll(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 20, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":1}`)

	restore := map[string]string{
		"Action": "CreateVolume", "AvailabilityZone": "us-east-1a", "SnapshotId": snapshotID,
	}
	for range 3 {
		_, code, _ := ec2ErrorDetail(t, ts, restore)
		require.Equal(t, "IncorrectState", code, "three refusals must not exhaust a one-poll budget")
	}

	assert.Equal(t, "pending", ec2DescribeOneSnapshot(t, ts, snapshotID).State,
		"the single budgeted observation is still unspent")
	assert.Equal(t, "completed", ec2DescribeOneSnapshot(t, ts, snapshotID).State)

	var ok struct {
		VolumeID string `xml:"volumeId"`
		Size     int    `xml:"size"`
	}
	ec2FleetXML(t, ts, restore, &ok)
	assert.NotEmpty(t, ok.VolumeID, "a completed snapshot restores")
	assert.Equal(t, 20, ok.Size, "and still sizes from the snapshot")
}

// TestEC2_SnapshotProgression_DeleteSnapshotIgnoresTheState pins AWS's explicit permission:
// "although you can delete a snapshot that is still in progress, the snapshot must complete
// before the deletion takes effect." There is no error code for the case, so there is no
// refusal — and the deferred-effect half is not modeled, for the reason deleteSnapshot records.
func TestEC2_SnapshotProgression_DeleteSnapshotIgnoresTheState(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":9}`)
	require.Equal(t, "pending", ec2DescribeOneSnapshot(t, ts, snapshotID).State)

	resp := ec2Request(t, ts, map[string]string{"Action": "DeleteSnapshot", "SnapshotId": snapshotID})
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"a snapshot still in progress is deletable, which AWS documents in so many words")

	assert.Empty(t, ec2DescribeSnapshots(t, ts, nil), "and it is gone at once, not after the countdown")
}

// TestEC2_SeedSnapshotStatus_Validation covers the endpoint's own refusals.
//
// An unknown state is refused rather than stored because it is one no SDK can map and no
// consumer can branch on: the seed would look accepted and produce a response the caller's own
// model rejects. Both members are checked, since a seed is as easily wrong about where a
// progression ends as about where it starts.
func TestEC2_SeedSnapshotStatus_Validation(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{"wildcard default", `{"pendingObservations":2}`, http.StatusOK},
		{"every documented state is seedable", `{"state":"recoverable","finalState":"recovering"}`, http.StatusOK},
		{"zero observations", `{"pendingObservations":0}`, http.StatusOK},
		{"negative observations", `{"pendingObservations":-1}`, http.StatusBadRequest},
		{"unknown pending state", `{"state":"creating"}`, http.StatusBadRequest},
		{"unknown final state", `{"finalState":"done"}`, http.StatusBadRequest},
		{"state casing is not folded", `{"state":"Pending"}`, http.StatusBadRequest},
		{"malformed json", `{`, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+"/v1/ec2/snapshot-status",
				"application/json", strings.NewReader(tc.body))
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}

	// An ID-scoped clear leaves the wildcard alone, and a bare clear sweeps both.
	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"snap-0123456789abcdef0","pendingObservations":1}`)
	ec2ClearSnapshotStatus(t, ts, "snap-0123456789abcdef0")
	ec2ClearSnapshotStatus(t, ts, "")
}

// TestEC2_SnapshotProgression_UnseededIsUnchanged is the criterion "default behavior
// unchanged", checked in one place rather than inferred from the rest of the suite staying
// green: with no seed ever posted, every snapshot-reporting operation says completed at 100%.
func TestEC2_SnapshotProgression_UnseededIsUnchanged(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)

	created := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID})
	assert.Equal(t, "completed", created.Status)
	assert.Equal(t, "100%", created.Progress)

	// Repeated describes cannot drift, because an exhausted countdown writes nothing.
	for range 3 {
		got := ec2DescribeOneSnapshot(t, ts, created.SnapshotID)
		assert.Equal(t, "completed", got.State)
		assert.Equal(t, "100%", got.Progress)
		assert.Empty(t, got.StatusMsg, "an unseeded snapshot has no diagnostic to publish")
	}
}
