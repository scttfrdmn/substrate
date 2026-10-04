package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The shared seeded-progression helper (emulator/progression.go), which EC2 snapshots (#715) and
// instances (#514) now share and every service with a published lifecycle builds on (#1155, #1196).
// EC2's own tests pin what each of those countdowns reports; these pin the mechanics underneath.

const progressionTestNS = "progression-test"

// progressionHarness is one progression kind over a fresh store.
type progressionHarness struct {
	t     *testing.T
	pg    *emulator.ProgressionForTest
	state emulator.StateManager
}

func newProgressionHarness(t *testing.T, state emulator.StateManager) *progressionHarness {
	t.Helper()
	if state == nil {
		state = emulator.NewMemoryStateManager()
	}
	return &progressionHarness{t: t, pg: emulator.NewProgressionForTest(progressionTestNS), state: state}
}

func (h *progressionHarness) seed(seed emulator.ProgressionSeedForTest) {
	h.t.Helper()
	_, err := h.pg.Put(context.Background(), h.state, seed)
	require.NoError(h.t, err, "seed %+v", seed)
}

// observeSeen observes id once and returns the observation's index, failing on an error or on no
// seed applying.
func (h *progressionHarness) observeSeen(id string) int {
	h.t.Helper()
	seed, seen, err := h.pg.Observe(context.Background(), h.state, id)
	require.NoError(h.t, err, "observe %s", id)
	require.NotNil(h.t, seed, "a seed must govern %s", id)
	return seen
}

func TestProgression_ObserveSpendsAndPeekDoesNot(t *testing.T) {
	t.Parallel()
	h := newProgressionHarness(t, nil)
	h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 2})

	for range 3 {
		_, seen, err := h.pg.Peek(context.Background(), h.state, "r-1")
		require.NoError(t, err)
		require.Equal(t, 0, seen, "a peek must spend no observation")
	}
	require.Equal(t, []int{0, 1, 2, 2, 2}, []int{h.observeSeen("r-1"), h.observeSeen("r-1"), h.observeSeen("r-1"),
		h.observeSeen("r-1"), h.observeSeen("r-1")}, "observations count up and stop at the seed's length")
}

func TestProgression_AnIDSeedOutranksTheWildcard(t *testing.T) {
	t.Parallel()
	h := newProgressionHarness(t, nil)
	h.seed(emulator.ProgressionSeedForTest{ID: "*", PendingObservations: 3, State: "RUNNING"})
	h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 1, State: "PENDING"})

	for id, want := range map[string]emulator.ProgressionSeedForTest{
		"r-1": {ID: "r-1", PendingObservations: 1, State: "PENDING"},
		"r-2": {ID: "*", PendingObservations: 3, State: "RUNNING"},
	} {
		seed, _, err := h.pg.Peek(context.Background(), h.state, id)
		require.NoError(t, err)
		require.NotNil(t, seed, "%s", id)
		require.Equal(t, want, *seed, "%s resolves the exact ID first, then the wildcard", id)
	}

	seed, _, err := emulator.NewProgressionForTest("progression-unseeded").Peek(context.Background(), h.state, "r-1")
	require.NoError(t, err)
	require.Nil(t, seed, "no seed applies in a namespace that holds none")
}

// One describe over several resources must not spend several observations off one wildcard
// countdown (#582): the specification is shared, the progress through it is per resource.
func TestProgression_AWildcardCountsEachResourceSeparately(t *testing.T) {
	t.Parallel()
	h := newProgressionHarness(t, nil)
	h.seed(emulator.ProgressionSeedForTest{PendingObservations: 2})

	require.Equal(t, 0, h.observeSeen("r-1"))
	require.Equal(t, 1, h.observeSeen("r-1"))
	require.Equal(t, 0, h.observeSeen("r-2"), "r-2's countdown starts at zero however far r-1's has run")
	require.Equal(t, 0, h.observeSeen("r-3"))
	require.Equal(t, 2, h.observeSeen("r-1"))
}

func TestProgression_ResetRestartsOneResource(t *testing.T) {
	t.Parallel()
	h := newProgressionHarness(t, nil)
	h.seed(emulator.ProgressionSeedForTest{PendingObservations: 3})
	h.observeSeen("r-1")
	h.observeSeen("r-1")
	h.observeSeen("r-2")

	require.NoError(t, h.pg.Reset(context.Background(), h.state, "r-1"))
	require.Equal(t, 0, h.observeSeen("r-1"), "a reset restarts the countdown")
	require.Equal(t, 1, h.observeSeen("r-2"), "and only that resource's")
}

func TestProgression_SeedingAndClearing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("a new seed restarts the countdowns it governs", func(t *testing.T) {
		t.Parallel()
		h := newProgressionHarness(t, nil)
		h.seed(emulator.ProgressionSeedForTest{PendingObservations: 3})
		h.observeSeen("r-1")
		h.observeSeen("r-2")
		h.seed(emulator.ProgressionSeedForTest{PendingObservations: 3})
		require.Equal(t, 0, h.observeSeen("r-1"), "a wildcard seed sweeps every counter")
		require.Equal(t, 0, h.observeSeen("r-2"))

		h.observeSeen("r-1")
		h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 3})
		require.Equal(t, 0, h.observeSeen("r-1"), "an ID seed restarts its own resource")
		require.Equal(t, 1, h.observeSeen("r-2"), "and no other")
	})

	t.Run("clearing one seed leaves the wildcard", func(t *testing.T) {
		t.Parallel()
		h := newProgressionHarness(t, nil)
		h.seed(emulator.ProgressionSeedForTest{PendingObservations: 5, State: "RUNNING"})
		h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 1, State: "PENDING"})
		h.observeSeen("r-1")

		require.NoError(t, h.pg.Clear(ctx, h.state, "r-1"))
		seed, seen, err := h.pg.Peek(ctx, h.state, "r-1")
		require.NoError(t, err)
		require.Equal(t, "RUNNING", seed.State, "r-1 falls back to the wildcard")
		require.Equal(t, 0, seen, "and its countdown is cleared with its seed")
	})

	t.Run("clearing everything removes every seed and counter", func(t *testing.T) {
		t.Parallel()
		h := newProgressionHarness(t, nil)
		h.seed(emulator.ProgressionSeedForTest{PendingObservations: 5})
		h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 1})
		h.observeSeen("r-1")
		h.observeSeen("r-2")

		require.NoError(t, h.pg.Clear(ctx, h.state, ""))
		for _, id := range []string{"r-1", "r-2"} {
			seed, _, err := h.pg.Peek(ctx, h.state, id)
			require.NoError(t, err)
			require.Nil(t, seed, "%s is unseeded after a clear-all", id)
		}
		keys, err := h.state.List(ctx, progressionTestNS, "")
		require.NoError(t, err)
		require.Empty(t, keys, "a clear-all leaves nothing in the namespace")
	})
}

// putCountingState counts Puts to one namespace, so a test can see that a settled resource's
// observations stop writing.
type putCountingState struct {
	emulator.StateManager
	puts int
}

func (s *putCountingState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if namespace == progressionTestNS && strings.HasPrefix(key, "observed:") {
		s.puts++
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

func TestProgression_ASettledResourceWritesNothing(t *testing.T) {
	t.Parallel()
	counting := &putCountingState{StateManager: emulator.NewMemoryStateManager()}
	h := newProgressionHarness(t, counting)
	h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 2})

	for range 6 {
		h.observeSeen("r-1")
	}
	require.Equal(t, 2, counting.puts, "the counter is written only while it can still change an answer")

	_, _, err := h.pg.Observe(context.Background(), counting, "r-unseeded-namespace-wide")
	require.NoError(t, err)
	require.Equal(t, 2, counting.puts, "an unseeded resource writes nothing at all")
}

func TestProgression_TheEndpointRefusesABadSeed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		status     int
		want       string
	}{
		{"a state outside the enumeration", `{"id":"r-1","pendingObservations":1,"state":"WAITING"}`, http.StatusBadRequest, "unknown test state"},
		{"a final state outside the enumeration", `{"pendingObservations":1,"finalState":"DONE"}`, http.StatusBadRequest, "unknown test state"},
		{"a negative count", `{"pendingObservations":-1}`, http.StatusBadRequest, "pendingObservations must be >= 0"},
		{"a body that does not parse", `{"pendingObservations":`, http.StatusBadRequest, "error"},
		{"a valid ID seed", `{"id":"r-1","pendingObservations":2,"state":"RUNNING","finalState":"FAILED"}`, http.StatusOK, `"id":"status:r-1"`},
		{"a valid wildcard seed", `{"pendingObservations":2}`, http.StatusOK, `"id":"status:*"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newProgressionHarness(t, nil)
			w := httptest.NewRecorder()
			h.pg.ServeSeed(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/test/x-status", strings.NewReader(tc.body)), h.state)
			require.Equal(t, tc.status, w.Code, "%s: %s", tc.name, w.Body.String())
			require.Contains(t, w.Body.String(), tc.want)

			keys, err := h.state.List(context.Background(), progressionTestNS, "status:")
			require.NoError(t, err)
			if tc.status == http.StatusOK {
				require.Len(t, keys, 1, "a valid seed is stored")
				var body map[string]any
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body), "the response is JSON: %s", w.Body.String())
			} else {
				require.Empty(t, keys, "a refused seed stores nothing")
			}
		})
	}
}

func TestProgression_TheClearEndpoint(t *testing.T) {
	t.Parallel()
	h := newProgressionHarness(t, nil)
	h.seed(emulator.ProgressionSeedForTest{ID: "r-1", PendingObservations: 1})
	h.seed(emulator.ProgressionSeedForTest{ID: "r-2", PendingObservations: 1})

	w := httptest.NewRecorder()
	h.pg.ServeClear(w, httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/v1/test/x-status?id=r-1", nil), h.state)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	keys, err := h.state.List(context.Background(), progressionTestNS, "status:")
	require.NoError(t, err)
	require.Equal(t, []string{"status:r-2"}, keys, "?id= removes that seed alone")

	w = httptest.NewRecorder()
	h.pg.ServeClear(w, httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/v1/test/x-status", nil), h.state)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	keys, err = h.state.List(context.Background(), progressionTestNS, "status:")
	require.NoError(t, err)
	require.Empty(t, keys, "no query removes every seed")
}

// A store fault anywhere in the countdown is an error, never an answer that looks unseeded or
// settled.
func TestProgression_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   func(h *progressionHarness) error
	}{
		{"the seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, func(h *progressionHarness) error {
			_, _, err := h.pg.Peek(context.Background(), h.state, "r-1")
			return err
		}},
		{"a corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "status:" }, func(h *progressionHarness) error {
			_, _, err := h.pg.Observe(context.Background(), h.state, "r-1")
			return err
		}},
		{"the counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, func(h *progressionHarness) error {
			_, _, err := h.pg.Observe(context.Background(), h.state, "r-1")
			return err
		}},
		{"a corrupt counter", func(m *cfFaultStateManager) { m.corruptGet = "observed:" }, func(h *progressionHarness) error {
			_, _, err := h.pg.Peek(context.Background(), h.state, "r-1")
			return err
		}},
		{"the counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, func(h *progressionHarness) error {
			_, _, err := h.pg.Observe(context.Background(), h.state, "r-1")
			return err
		}},
		{"the reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, func(h *progressionHarness) error {
			return h.pg.Reset(context.Background(), h.state, "r-1")
		}},
		{"the seed write", func(m *cfFaultStateManager) { m.failPut = "status:" }, func(h *progressionHarness) error {
			_, err := h.pg.Put(context.Background(), h.state, emulator.ProgressionSeedForTest{PendingObservations: 1})
			return err
		}},
		{"a clear's delete", func(m *cfFaultStateManager) { m.failDelete = "status:" }, func(h *progressionHarness) error {
			return h.pg.Clear(context.Background(), h.state, "")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newProgressionHarness(t, fault)
			h.seed(emulator.ProgressionSeedForTest{PendingObservations: 3})
			h.observeSeen("r-1")

			tc.arm(fault)
			err := tc.op(h)
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.False(t, errors.As(err, &awsErr), "a store fault is not a published refusal")
		})
	}
}

func TestProgression_CountdownStateFallsBackToTheDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		seen, total             int
		transient, final, state string
		terminal                bool
	}{
		{0, 2, "", "", "pending", false},
		{1, 2, "RUNNING", "", "RUNNING", false},
		{2, 2, "", "", "completed", true},
		{5, 2, "", "FAILED", "FAILED", true},
		{0, 0, "RUNNING", "", "completed", true},
	} {
		state, terminal := emulator.CountdownStateForTest(tc.seen, tc.total, tc.transient, tc.final, "pending", "completed")
		require.Equal(t, tc.state, state, "%+v", tc)
		require.Equal(t, tc.terminal, terminal, "%+v", tc)
	}
}
