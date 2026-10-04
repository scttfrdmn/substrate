package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
)

// A seeded status progression, shared by every resource whose published lifecycle passes through
// states a single request cannot show (#1155, #1196).
//
// Substrate completes work in the request that starts it, so a resource is usually born in its
// terminal state and a consumer's poll, wait or retry loop exits on its first iteration — never
// exercising the path it exists for. A progression makes the transition observable: a seed, written
// through the control plane, says how many *observations* of a resource report a non-terminal state
// before it reports its settled one. The model was built twice, for EC2 snapshots (#715) and
// instances (#514); this file is the one copy every service now uses.
//
// # The rules a progression keeps
//
//   - A seed governs what an observation reports; it never rewrites the resource record. Clearing
//     the seed — or POST /v1/state/reset, which clears the namespace — makes every resource read its
//     own record again, and an unseeded resource reads exactly as it does without this file.
//   - The countdown is counted in observations, not measured as a duration. [TimeController.Now]
//     advances with wall time from its baseline, so a duration seed would expire partway through a
//     test and make every "still pending" assertion depend on how long the rest of the test took.
//     A count is exactly reproducible, live and on replay.
//   - The seed is keyed by resource ID or "*", and resolved ID first. The progress through it is
//     kept per resource, so one describe over five resources does not spend five observations off a
//     shared wildcard countdown — the hazard #582 records for the SQS seed.
//   - A describe-style read observes, spending one observation; a create-time or precondition read
//     peeks, spending none. Counting a create or a precondition check against a budget the caller
//     meant to spend on polling would make "pendingObservations: 2" mean two polls in one test and
//     one in another.
//   - The counter is written only while it can still change an answer. Past the end of the
//     countdown a further write would rewrite state on every describe of a settled resource for no
//     observable difference, and every one of those writes lands in the event log a replay walks.
//   - Each resource kind has its own namespace, so a prefix-sweeping DELETE of one kind's seeds
//     cannot clear another's.
//
// # Replay
//
// A seed is a control-plane write, not an AWS request, and [ReplayEngine.Replay] resets the whole
// [StateManager] before it starts. The handlers below must therefore be mounted inside server.go's
// control-plane group, where [Server.recordControlPlaneWrites] records each write as an event and
// a replay re-issues it in position (#1140). The countdown then restarts from zero at the same
// point in the stream, so the same observations see the same states.

// progressionSeed is what a seed type tells a [progression] about itself. Each resource kind
// declares its own seed struct — the JSON members are part of its published control-plane shape —
// and implements these three methods.
type progressionSeed interface {
	// progressionID is the resource the seed targets; "" and "*" both mean every resource.
	progressionID() string
	// progressionObservations is how many observations report a non-terminal state before the
	// resource settles. Zero means it is settled from the first observation.
	progressionObservations() int
	// validateProgression refuses a seed whose states are outside the published enumeration, or
	// that is otherwise malformed. A negative count is refused by the handler before this runs.
	validateProgression() error
}

// progression is one resource kind's seeded countdown: where its seeds and counters live, and how
// its control-plane endpoint names its fields. S is the kind's seed type.
//
// A progression holds no state of its own and no lock: the plugin passes its [StateManager] to each
// call, and owns the mutex [progression.observe] serializes on. It is a value a plugin declares
// once, as a package-level constant-like var built by [newProgression]; nothing mutates it.
type progression[S progressionSeed] struct {
	// namespace is the [StateManager] namespace the kind's seeds and counters live in.
	namespace string
	// idParam is the DELETE query parameter naming one seed, and the key the POST response echoes
	// the stored seed key under — "snapshotId", "instanceId", "jobId".
	idParam string
	// countField is the seed member holding the countdown length, named in the refusal of a
	// negative one — "pendingObservations", "transientObservations".
	countField string
}

// The key prefixes every progression uses inside its own namespace.
const (
	// progressionSeedPrefix is the key prefix for every seed, used to clear all.
	progressionSeedPrefix = "status:"
	// progressionObservedPrefix is the key prefix for the per-resource observation counters.
	progressionObservedPrefix = "observed:"
)

// newProgression returns the progression for one resource kind. namespace must be unique to the
// kind; idParam and countField are the kind's control-plane member names (see [progression]).
func newProgression[S progressionSeed](namespace, idParam, countField string) progression[S] {
	return progression[S]{namespace: namespace, idParam: idParam, countField: countField}
}

// progressionSeedKey returns the state key for a seed: "status:{id}", or "status:*" for the
// wildcard, which an empty ID also names.
func progressionSeedKey(id string) string {
	if id == "" {
		id = "*"
	}
	return progressionSeedPrefix + id
}

// progressionObservedKey returns the state key holding how many observations one resource has
// already had. It is keyed by resource ID even when the governing seed is the wildcard — see the
// file comment on #582.
func progressionObservedKey(id string) string {
	return progressionObservedPrefix + id
}

// progressionObserved is a resource's countdown position.
type progressionObserved struct {
	// Observations is how many observations the resource has had since its countdown started.
	Observations int `json:"observations"`
}

// resolve returns the seed governing one resource, the exact ID first and then the "*" wildcard,
// or (nil, nil) when none applies.
func (pg progression[S]) resolve(ctx context.Context, state StateManager, id string) (*S, error) {
	for _, key := range []string{progressionSeedKey(id), progressionSeedKey("*")} {
		data, err := state.Get(ctx, pg.namespace, key)
		if err != nil {
			return nil, fmt.Errorf("%s resolve seed get: %w", pg.namespace, err)
		}
		if data == nil {
			continue
		}
		var seed S
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("%s resolve seed unmarshal: %w", pg.namespace, err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by the caller.
}

// observations returns how many observations one resource has already had, zero when none.
func (pg progression[S]) observations(ctx context.Context, state StateManager, id string) (int, error) {
	data, err := state.Get(ctx, pg.namespace, progressionObservedKey(id))
	if err != nil {
		return 0, fmt.Errorf("%s observations get: %w", pg.namespace, err)
	}
	if data == nil {
		return 0, nil
	}
	var observed progressionObserved
	if err := json.Unmarshal(data, &observed); err != nil {
		return 0, fmt.Errorf("%s observations unmarshal: %w", pg.namespace, err)
	}
	return observed.Observations, nil
}

// advance records that one resource has had seen+1 observations. Callers advance only while
// seen is below the seed's count — see the file comment on terminal-stop.
func (pg progression[S]) advance(ctx context.Context, state StateManager, id string, seen int) error {
	data, err := json.Marshal(progressionObserved{Observations: seen + 1})
	if err != nil {
		return fmt.Errorf("%s advance marshal: %w", pg.namespace, err)
	}
	if err := state.Put(ctx, pg.namespace, progressionObservedKey(id), data); err != nil {
		return fmt.Errorf("%s advance put: %w", pg.namespace, err)
	}
	return nil
}

// peek returns the seed governing one resource and how many observations it has had, without
// spending one. seed is nil when no seed applies, and the caller reports the record unchanged.
func (pg progression[S]) peek(ctx context.Context, state StateManager, id string) (seed *S, seen int, err error) {
	seed, err = pg.resolve(ctx, state, id)
	if err != nil || seed == nil {
		return seed, 0, err
	}
	seen, err = pg.observations(ctx, state, id)
	if err != nil {
		return nil, 0, err
	}
	return seed, seen, nil
}

// observe returns the seed governing one resource and the index of this observation (counting from
// zero), and spends it: the counter advances while seen is below the seed's count, and is left alone
// once the countdown is exhausted.
//
// It holds mu across the read-modify-write. [StateManager] offers no compare-and-swap, so two
// concurrent describes of one resource could both read a count of 1 and both write 2, consuming one
// observation twice and making "the third poll settles" flake. The plugin owns mu — one per plugin,
// shared by all its kinds, so there is no lock order to get wrong — and the guarantee is
// process-local, which covers substrate's single-process topology.
func (pg progression[S]) observe(ctx context.Context, state StateManager, mu *sync.Mutex, id string) (seed *S, seen int, err error) {
	mu.Lock()
	defer mu.Unlock()
	seed, seen, err = pg.peek(ctx, state, id)
	if err != nil || seed == nil {
		return seed, seen, err
	}
	if seen < (*seed).progressionObservations() {
		if err := pg.advance(ctx, state, id, seen); err != nil {
			return nil, 0, err
		}
	}
	return seed, seen, nil
}

// reset restarts one resource's countdown, so the next observation is the first again. A plugin
// calls it from every operation that starts a new transition of an existing resource — a stop after
// a start, an update after a create — which is what makes one seed cover several transitions.
func (pg progression[S]) reset(ctx context.Context, state StateManager, id string) error {
	if err := state.Delete(ctx, pg.namespace, progressionObservedKey(id)); err != nil {
		return fmt.Errorf("%s reset %s: %w", pg.namespace, id, err)
	}
	return nil
}

// clearObservations resets the countdown of every resource a seed governs: one for an ID-scoped
// seed, every resource for the wildcard, which sweeps the counter prefix because the counters are
// per resource by design.
func (pg progression[S]) clearObservations(ctx context.Context, state StateManager, id string) error {
	if id != "" && id != "*" {
		return pg.reset(ctx, state, id)
	}
	return pg.deletePrefix(ctx, state, progressionObservedPrefix)
}

// deletePrefix deletes every key in the kind's namespace that begins with prefix.
func (pg progression[S]) deletePrefix(ctx context.Context, state StateManager, prefix string) error {
	keys, err := state.List(ctx, pg.namespace, prefix)
	if err != nil {
		return fmt.Errorf("%s list %s: %w", pg.namespace, prefix, err)
	}
	for _, k := range keys {
		if err := state.Delete(ctx, pg.namespace, k); err != nil {
			return fmt.Errorf("%s delete %s: %w", pg.namespace, k, err)
		}
	}
	return nil
}

// put stores a validated seed and resets the countdown of every resource it governs, so a test
// that seeds twice gets two full progressions rather than the remainder of the first. It returns
// the key the seed was stored under.
func (pg progression[S]) put(ctx context.Context, state StateManager, seed S) (string, error) {
	data, err := json.Marshal(seed)
	if err != nil {
		return "", fmt.Errorf("%s seed marshal: %w", pg.namespace, err)
	}
	key := progressionSeedKey(seed.progressionID())
	if err := state.Put(ctx, pg.namespace, key, data); err != nil {
		return "", fmt.Errorf("%s seed put: %w", pg.namespace, err)
	}
	if err := pg.clearObservations(ctx, state, seed.progressionID()); err != nil {
		return "", err
	}
	return key, nil
}

// clear removes one seed and its resource's countdown, or — for an empty id — every seed and every
// counter in the kind's namespace.
func (pg progression[S]) clear(ctx context.Context, state StateManager, id string) error {
	if id != "" {
		if err := state.Delete(ctx, pg.namespace, progressionSeedKey(id)); err != nil {
			return fmt.Errorf("%s clear seed %s: %w", pg.namespace, id, err)
		}
		return pg.clearObservations(ctx, state, id)
	}
	for _, prefix := range []string{progressionSeedPrefix, progressionObservedPrefix} {
		if err := pg.deletePrefix(ctx, state, prefix); err != nil {
			return err
		}
	}
	return nil
}

// serveSeed handles the kind's POST /v1/{service}/{kind}-status: it decodes a seed, refuses a
// negative count or an invalid one with 400, stores it, and answers {"ok":true,idParam:key}.
//
// It must be mounted inside server.go's control-plane group so the write is recorded and replayed;
// see the file comment.
func (pg progression[S]) serveSeed(w http.ResponseWriter, r *http.Request, state StateManager, logger Logger) {
	var seed S
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.progressionObservations() < 0 {
		http.Error(w, fmt.Sprintf(`{"error":"%s must be >= 0"}`, pg.countField), http.StatusBadRequest)
		return
	}
	if err := seed.validateProgression(); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	key, err := pg.put(r.Context(), state, seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, logger, map[string]any{"ok": true, pg.idParam: key})
}

// serveClear handles the kind's DELETE /v1/{service}/{kind}-status: with ?{idParam}=… it removes
// that seed and its resource's countdown, and without it removes every seed and counter of the
// kind. Like [progression.serveSeed], it must be mounted inside the control-plane group.
func (pg progression[S]) serveClear(w http.ResponseWriter, r *http.Request, state StateManager, logger Logger) {
	if err := pg.clear(r.Context(), state, r.URL.Query().Get(pg.idParam)); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, logger, map[string]any{"ok": true})
}

// progressionStates validates seeded state names against a published enumeration, naming the
// resource kind in the refusal. Empty names are accepted, since they select the kind's default.
func progressionStates(kind string, published []string, states ...string) error {
	for _, state := range states {
		if state != "" && !slices.Contains(published, state) {
			return fmt.Errorf("unknown %s state %q, want one of %v", kind, state, published)
		}
	}
	return nil
}

// countdownState returns the state the seen-th observation reports (counting from zero) under a
// seed of total observations: transient while the countdown runs, final once it is exhausted,
// each falling back to its default when the seed leaves it empty. terminal reports which.
//
// It is the arithmetic a free-choice seed — one whose states the seed names, like a snapshot's —
// shares; a seed whose transient state is derived from the record, like an instance's, does not
// need it.
func countdownState(seen, total int, transient, final, defaultTransient, defaultFinal string) (state string, terminal bool) {
	if seen < total {
		if transient == "" {
			transient = defaultTransient
		}
		return transient, false
	}
	if final == "" {
		final = defaultFinal
	}
	return final, true
}
