package emulator

import (
	"context"
	"net/http"
	"sync"
)

// This file exports the shared progression helper (progression.go) to external tests, over a
// test-only seed type. The helper is generic and unexported because each plugin declares its own
// seed type; the tests exercise the helper itself, which every plugin's countdown is built on.

// ProgressionSeedForTest is a free-choice seed of the shape a new service's seed takes: an ID, a
// countdown length, and the transient and final states it names.
type ProgressionSeedForTest struct {
	ID                  string `json:"id"`
	PendingObservations int    `json:"pendingObservations"`
	State               string `json:"state"`
	FinalState          string `json:"finalState"`
}

// progressionTestStates is the enumeration [ProgressionSeedForTest] validates against.
var progressionTestStates = []string{"PENDING", "RUNNING", "SUCCEEDED", "FAILED"}

func (s ProgressionSeedForTest) progressionID() string        { return s.ID }
func (s ProgressionSeedForTest) progressionObservations() int { return s.PendingObservations }
func (s ProgressionSeedForTest) validateProgression() error {
	return progressionStates("test", progressionTestStates, s.State, s.FinalState)
}

// ProgressionForTest wraps one [progression] kind and the mutex a plugin would own.
type ProgressionForTest struct {
	pg progression[ProgressionSeedForTest]
	mu sync.Mutex
}

// NewProgressionForTest returns a progression kind in namespace, with "id" as its DELETE query
// parameter and "pendingObservations" as its count member.
func NewProgressionForTest(namespace string) *ProgressionForTest {
	return &ProgressionForTest{pg: newProgression[ProgressionSeedForTest](namespace, "id", "pendingObservations")}
}

// Peek wraps progression.peek.
func (t *ProgressionForTest) Peek(ctx context.Context, state StateManager, id string) (*ProgressionSeedForTest, int, error) {
	return t.pg.peek(ctx, state, id)
}

// Observe wraps progression.observe.
func (t *ProgressionForTest) Observe(ctx context.Context, state StateManager, id string) (*ProgressionSeedForTest, int, error) {
	return t.pg.observe(ctx, state, &t.mu, id)
}

// Reset wraps progression.reset.
func (t *ProgressionForTest) Reset(ctx context.Context, state StateManager, id string) error {
	return t.pg.reset(ctx, state, id)
}

// Put wraps progression.put, returning the key the seed was stored under.
func (t *ProgressionForTest) Put(ctx context.Context, state StateManager, seed ProgressionSeedForTest) (string, error) {
	return t.pg.put(ctx, state, seed)
}

// Clear wraps progression.clear; an empty id clears every seed and counter.
func (t *ProgressionForTest) Clear(ctx context.Context, state StateManager, id string) error {
	return t.pg.clear(ctx, state, id)
}

// ServeSeed wraps progression.serveSeed.
func (t *ProgressionForTest) ServeSeed(w http.ResponseWriter, r *http.Request, state StateManager) {
	t.pg.serveSeed(w, r, state, NewDefaultLogger(0, false))
}

// ServeClear wraps progression.serveClear.
func (t *ProgressionForTest) ServeClear(w http.ResponseWriter, r *http.Request, state StateManager) {
	t.pg.serveClear(w, r, state, NewDefaultLogger(0, false))
}

// CountdownStateForTest wraps countdownState.
func CountdownStateForTest(seen, total int, transient, final, defaultTransient, defaultFinal string) (string, bool) {
	return countdownState(seen, total, transient, final, defaultTransient, defaultFinal)
}
