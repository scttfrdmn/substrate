package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// s3CtrlNamespace is the state namespace for S3 control-plane (seed) data.
const s3CtrlNamespace = "s3-ctrl"

// s3CtrlConflictPrefix is the key prefix for every conditional-conflict seed, used
// to clear all of them.
const s3CtrlConflictPrefix = "conflict:"

// s3ConditionalConflictMessage accompanies ConditionalRequestConflict.
//
// This wording is Substrate's own. The code and the 409 status are documented —
// they appear in the If-Match and If-None-Match member documentation for
// PutObject, CopyObject and CompleteMultipartUpload, and in the "Conditional
// write behavior" section of the S3 user guide — but no message string is
// documented anywhere, and the API model carries no ConditionalRequestConflict
// shape and does not list the code in any operation's errors. Per
// docs/fidelity.md, a message with no corroborating source is Substrate's own
// and says so here rather than being written to look authoritative. Assert on
// the code and the status.
const s3ConditionalConflictMessage = "A conflicting operation occurred during this conditional write. " +
	"The request was not applied."

// s3ConflictSeed is a seeded 409 ConditionalRequestConflict on a conditional
// write. S3 returns that code when a concurrent operation — in the documented
// case, a delete — interferes with a conditional write between its evaluation and
// its completion. It is a timing accident rather than a state a request can
// assert, so it cannot be derived by inspecting the current object the way a 412
// is; seeding is what makes the branch reachable.
//
// The branch matters because the three outcomes of a conditional write select
// different recovery paths in the caller, and only two of them were reachable
// before this seed existed. A 412 means another writer won: re-read, recompute,
// retry the compare-and-swap. A 409 on PutObject means retry the PUT as-is. A 409
// on CompleteMultipartUpload means the upload ID is finished: re-do
// CreateMultipartUpload and every UploadPart. A CAS loop that answers the third
// case like the first spins until it gives up, and without this seed that loop
// looks correct (#540).
//
// Fault injection cannot substitute for this. [FaultController] rules are
// evaluated before a request reaches its plugin, so a faulted Complete writes no
// state and leaves its upload ID completable — the retry a consumer must not
// perform still succeeds, which is the defect the seed exists to expose.
//
// The budget is counted in occurrences rather than measured as a duration, for
// the reason recorded on [sqsConsistencySeed]: [TimeController.Now] advances with
// wall time from its baseline, so a duration-based window would expire partway
// through a test and make assertions wall-clock dependent, which no test here may
// be. A counter is exactly reproducible.
//
// Keyed by "bucket/key" or the "*" wildcard. The three counters are independent so
// a fixture can seed the multipart arm — the one with the expensive recovery —
// without perturbing plain writes; all three default to 0, so an unseeded key
// behaves exactly as before.
type s3ConflictSeed struct {
	// Bucket the seed applies to. Empty, together with an empty Key, seeds the
	// "*" wildcard.
	Bucket string `json:"bucket"`

	// Key the seed applies to, within Bucket.
	Key string `json:"key"`

	// PutConflicts is the number of subsequent conditional PutObject calls on
	// this key that report ConditionalRequestConflict.
	PutConflicts int `json:"putConflicts"`

	// CopyConflicts is the same count for CopyObject, evaluated against the
	// destination key.
	CopyConflicts int `json:"copyConflicts"`

	// CompleteConflicts is the same count for CompleteMultipartUpload. Consuming
	// one also invalidates the upload, so the caller's only recovery is the one
	// AWS documents; see [S3Plugin.consumeConditionalConflict].
	CompleteConflicts int `json:"completeConflicts"`
}

// s3ConflictOp names which counter on a seed a conditional write consumes.
type s3ConflictOp int

const (
	// s3ConflictPut selects the PutObject counter.
	s3ConflictPut s3ConflictOp = iota

	// s3ConflictCopy selects the CopyObject counter.
	s3ConflictCopy

	// s3ConflictComplete selects the CompleteMultipartUpload counter.
	s3ConflictComplete
)

// counter returns a pointer to the seed field this operation consumes, so
// consumeConditionalConflict decrements the right one without duplicating the
// switch.
func (op s3ConflictOp) counter(seed *s3ConflictSeed) *int {
	switch op {
	case s3ConflictCopy:
		return &seed.CopyConflicts
	case s3ConflictComplete:
		return &seed.CompleteConflicts
	default:
		return &seed.PutConflicts
	}
}

// s3ConflictKey returns the state key for a conditional-conflict seed. Key-scoped
// seeds use "conflict:{bucket}/{key}"; the wildcard uses "conflict:*".
func s3ConflictKey(bucket, key string) string {
	if bucket == "" && key == "" {
		return s3CtrlConflictPrefix + "*"
	}
	return s3CtrlConflictPrefix + bucket + "/" + key
}

// consumeConditionalConflict reports whether this conditional write should report
// a seeded 409 ConditionalRequestConflict instead of proceeding, decrementing the
// budget when it does. It matches bucket/key exactly first, then the "*" wildcard,
// and returns false when no seed applies.
//
// Callers must consult this only *after* [evaluateWritePreconditions] has passed,
// and only for a request that actually carries a precondition. Both halves matter.
// A 412 or a 404 is a determinate observation of the destination's current state
// and must not be masked by a seeded race — and consuming budget on a request that
// was going to fail anyway silently spends what the test meant to spend on the
// conflict, leaving the later assertion meaningless. That is the hazard
// [SQSPlugin.consumeQueueMiss] names for the same reason. An unconditional write
// never reports this code at all, because AWS documents it only on the two
// conditional headers.
//
// Consumption takes one dedicated mutex rather than the per-key stripe the caller
// already holds. The stripe is chosen by hashing bucket/key, but the wildcard seed
// is shared across every key, so two concurrent writes to *different* keys hold
// different stripes and could both read a budget of 1, both decrement to 0 and
// both Put — consuming one seeded conflict twice, exactly the read-modify-write
// hazard [s3KeyMutex] exists to close (there is no compare-and-swap on
// [StateManager]). Seed consumption is a harness-frequency operation, so a single
// mutex costs nothing here. The lock order is always key-stripe → seed mutex and
// never the reverse, so the two cannot deadlock.
func (p *S3Plugin) consumeConditionalConflict(ctx context.Context, bucket, key string, op s3ConflictOp) (bool, error) {
	p.seedMu.Lock()
	defer p.seedMu.Unlock()

	for _, stateKey := range []string{s3ConflictKey(bucket, key), s3ConflictKey("", "")} {
		data, err := p.state.Get(ctx, s3CtrlNamespace, stateKey)
		if err != nil {
			return false, fmt.Errorf("s3 consumeConditionalConflict get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed s3ConflictSeed
		if err := json.Unmarshal(data, &seed); err != nil {
			return false, fmt.Errorf("s3 consumeConditionalConflict unmarshal: %w", err)
		}

		counter := op.counter(&seed)
		if *counter <= 0 {
			// This seed exists but has nothing left for this operation. Fall
			// through to the wildcard: an exhausted key-scoped seed must not mask
			// a wildcard seed that still has budget.
			continue
		}

		*counter--
		updated, err := json.Marshal(seed)
		if err != nil {
			return false, fmt.Errorf("s3 consumeConditionalConflict marshal: %w", err)
		}
		if err := p.state.Put(ctx, s3CtrlNamespace, stateKey, updated); err != nil {
			return false, fmt.Errorf("s3 consumeConditionalConflict put: %w", err)
		}
		return true, nil
	}
	return false, nil
}

// s3ConditionalConflictResponse returns the 409 ConditionalRequestConflict a
// seeded conflict serves. It goes through [s3ErrorResponse] so the document is
// byte-identical to the one a genuine S3 error would produce — a bare <Error>
// rather than the wrapped <ErrorResponse> the Query protocol uses — which is what
// lets an SDK recover the code instead of falling back to the HTTP status.
func s3ConditionalConflictResponse() *AWSResponse {
	return s3ErrorResponse("ConditionalRequestConflict", s3ConditionalConflictMessage, http.StatusConflict)
}

// handleS3SeedConditionalConflict handles POST /v1/s3/conditional-conflict. It
// seeds how many subsequent conditional writes to a key report
// 409 ConditionalRequestConflict, modeling the concurrent-delete race AWS
// documents. Body:
// {"bucket","key","putConflicts","copyConflicts","completeConflicts"}; an empty
// bucket and key seed the "*" wildcard.
func (s *Server) handleS3SeedConditionalConflict(w http.ResponseWriter, r *http.Request) {
	var seed s3ConflictSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.PutConflicts < 0 || seed.CopyConflicts < 0 || seed.CompleteConflicts < 0 {
		http.Error(w, `{"error":"conflict counts must not be negative"}`, http.StatusBadRequest)
		return
	}
	// A bucket without a key (or the reverse) would be stored under a key no write
	// can ever match, so it would look armed and never fire. Refusing it is the
	// difference between a fixture that fails and one that passes vacuously.
	if (seed.Bucket == "") != (seed.Key == "") {
		http.Error(w, `{"error":"bucket and key must be given together, or both omitted for the wildcard"}`,
			http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	stateKey := s3ConflictKey(seed.Bucket, seed.Key)
	if err := s.state.Put(r.Context(), s3CtrlNamespace, stateKey, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "key": strings.TrimPrefix(stateKey, s3CtrlConflictPrefix)})
}

// handleS3ClearConditionalConflict handles DELETE /v1/s3/conditional-conflict.
// With ?bucket=&key= it removes that seed; without either it removes all,
// including the wildcard.
func (s *Server) handleS3ClearConditionalConflict(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")
	if bucket != "" || key != "" {
		if bucket == "" || key == "" {
			http.Error(w, `{"error":"bucket and key must be given together"}`, http.StatusBadRequest)
			return
		}
		if err := s.state.Delete(r.Context(), s3CtrlNamespace, s3ConflictKey(bucket, key)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), s3CtrlNamespace, s3CtrlConflictPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), s3CtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
