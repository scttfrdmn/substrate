package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
)

// sqsCtrlNamespace is the state namespace for SQS control-plane (seed) data.
const sqsCtrlNamespace = "sqs-ctrl"

// sqsQueueLockStripes is the number of mutexes in [SQSPlugin]'s per-queue stripe
// set. A power of two well above the concurrency any test drives, so distinct
// queues almost never contend.
const sqsQueueLockStripes = 64

// sqsQueueMutex is a striped mutex set serializing seed consumption per queue.
//
// It deliberately mirrors [s3KeyMutex] rather than reusing it: that type's API is
// two-part (bucket, key) and its documentation is written entirely in S3 terms, so
// sharing would mean either changing a signature used at every S3
// conditional-write call site or adding a joined-string API that reads worse at
// both. Roughly twenty lines of FNV striping is the cheaper duplication; extract a
// common type on the third caller.
//
// The hazard is the same one s3KeyMutex exists for. Consuming a seeded miss is a
// read-modify-write, and [StateManager] offers no compare-and-swap — Get/Put/
// Delete/List only, with [MemoryStateManager] last-write-wins. Two concurrent
// lookups could both read misses=1 and both write 0, consuming one miss where two
// were seeded, so a test asserting "exactly N calls fail" would flake. Holding a
// per-queue lock across read→decrement→Put closes that window.
//
// The guarantee is bounded the same way, and for the same reason: it is
// process-local. That is airtight for substrate's single-process topology and
// would not hold across two emulator processes sharing one state backend.
type sqsQueueMutex struct {
	stripes [sqsQueueLockStripes]sync.Mutex
}

// lock acquires the stripe guarding a queue name and returns its unlock function.
func (m *sqsQueueMutex) lock(queueName string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(queueName))
	mu := &m.stripes[h.Sum32()%sqsQueueLockStripes]
	mu.Lock()
	return mu.Unlock
}

// sqsConsistencySeed is a seeded create→lookup eventual-consistency window. AWS
// documents that a caller "must wait at least one second after the queue is
// created to be able to use the queue", so a real create→lookup→retry loop can
// observe QueueDoesNotExist for a queue that does exist. Substrate resolves a new
// queue instantly, which makes that retry path unreachable and a consumer's test
// of it vacuous; seeding is what makes the window observable.
//
// The window is counted in misses rather than measured as a duration on purpose.
// [TimeController.Now] advances with wall time from its baseline, so a duration
// seed would expire partway through a test and make "still missing" assertions
// wall-clock dependent — which no test here may be. A miss counter is exactly
// reproducible.
//
// Keyed by queue name or the "*" wildcard. The two counters are independent so a
// test can seed one operation without perturbing the other; both default to 0, so
// an unseeded queue behaves exactly as before.
type sqsConsistencySeed struct {
	// QueueName the seed applies to, or "*" for any queue.
	QueueName string `json:"queueName"`

	// GetURLMisses is the number of subsequent GetQueueUrl calls that report
	// QueueDoesNotExist even though the queue exists.
	GetURLMisses int `json:"getUrlMisses"`

	// GetAttributesMisses is the same count for GetQueueAttributes.
	GetAttributesMisses int `json:"getAttributesMisses"`

	// DeletedRecentlyMisses is the number of subsequent CreateQueue calls that
	// report QueueDeletedRecently rather than creating the queue (#429).
	//
	// It rides on this seed rather than on a separate endpoint because it is the
	// same kind of value: a count of API observations that deviate from the nominal
	// path for one queue, cleared by the same DELETE and the same state reset. AWS
	// documents the real condition as a 60-second wall-clock window after
	// DeleteQueue, which is a duration — and counted for exactly the reason the
	// misses above are, since a wall-clock window would make the assertion depend
	// on how long the rest of the test took.
	//
	// Unlike the lookup counters this one is *not* gated on the queue existing:
	// QueueDeletedRecently is precisely the case where the queue is absent, so
	// gating it on existence would make it unreachable.
	DeletedRecentlyMisses int `json:"deletedRecentlyMisses"`
}

// sqsCtrlKey returns the state key for a consistency seed. Queue-scoped seeds use
// "consistency:{name}"; the wildcard uses "consistency:*".
func sqsCtrlKey(queueName string) string {
	if queueName == "" {
		queueName = "*"
	}
	return "consistency:" + queueName
}

// sqsCtrlKeyPrefix is the key prefix for every consistency seed, used to clear all.
const sqsCtrlKeyPrefix = "consistency:"

// sqsQueueNameFromURL returns the queue name from a queue URL — its last path
// component. GetQueueAttributes identifies a queue by URL while seeds are keyed by
// name, so the name has to be recovered here.
func sqsQueueNameFromURL(queueURL string) string {
	trimmed := strings.TrimRight(queueURL, "/")
	if idx := strings.LastIndexByte(trimmed, '/'); idx >= 0 {
		return trimmed[idx+1:]
	}
	return trimmed
}

// sqsConsistencyOp names which counter on a seed an operation consumes.
type sqsConsistencyOp int

const (
	// sqsConsistencyGetURL selects the GetQueueUrl counter.
	sqsConsistencyGetURL sqsConsistencyOp = iota

	// sqsConsistencyGetAttributes selects the GetQueueAttributes counter.
	sqsConsistencyGetAttributes

	// sqsConsistencyDeletedRecently selects the CreateQueue QueueDeletedRecently
	// counter.
	sqsConsistencyDeletedRecently
)

// counter returns a pointer to the seed field this operation consumes, so
// consumeQueueMiss decrements the right one without duplicating the switch.
func (op sqsConsistencyOp) counter(seed *sqsConsistencySeed) *int {
	switch op {
	case sqsConsistencyGetAttributes:
		return &seed.GetAttributesMisses
	case sqsConsistencyDeletedRecently:
		return &seed.DeletedRecentlyMisses
	default:
		return &seed.GetURLMisses
	}
}

// consumeQueueMiss reports whether the current call should take a seeded deviation
// instead of its nominal path, decrementing the budget when it does. It matches the
// queue name exactly first, then the "*" wildcard, and returns false when no seed
// applies.
//
// For the two lookup counters, callers must consult this only *after* establishing
// that the queue exists. A miss consumed on a genuinely absent queue would silently
// burn budget the test meant to spend on the window, leaving the later retry
// assertion meaningless. [sqsConsistencyDeletedRecently] is the deliberate
// exception — the queue is absent by definition in that case.
func (p *SQSPlugin) consumeQueueMiss(queueName string, op sqsConsistencyOp) (bool, error) {
	unlock := p.queueMu.lock(queueName)
	defer unlock()

	goCtx := context.Background()
	for _, key := range []string{sqsCtrlKey(queueName), sqsCtrlKey("*")} {
		data, err := p.state.Get(goCtx, sqsCtrlNamespace, key)
		if err != nil {
			return false, fmt.Errorf("sqs consumeQueueMiss get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed sqsConsistencySeed
		if err := json.Unmarshal(data, &seed); err != nil {
			return false, fmt.Errorf("sqs consumeQueueMiss unmarshal: %w", err)
		}

		counter := op.counter(&seed)
		if *counter <= 0 {
			// This seed exists but has nothing left for this operation. Fall
			// through to the wildcard: an exhausted name-scoped seed must not mask
			// a wildcard seed that still has budget.
			continue
		}

		*counter--
		updated, err := json.Marshal(seed)
		if err != nil {
			return false, fmt.Errorf("sqs consumeQueueMiss marshal: %w", err)
		}
		if err := p.state.Put(goCtx, sqsCtrlNamespace, key, updated); err != nil {
			return false, fmt.Errorf("sqs consumeQueueMiss put: %w", err)
		}
		return true, nil
	}
	return false, nil
}

// handleSQSSeedConsistency handles POST /v1/sqs/consistency. It seeds how many
// subsequent lookups report QueueDoesNotExist for a queue that exists, modeling
// the create→lookup window AWS documents, and how many CreateQueue calls report
// QueueDeletedRecently. Body:
// {"queueName","getUrlMisses","getAttributesMisses","deletedRecentlyMisses"}; an
// empty queueName seeds the "*" wildcard.
func (s *Server) handleSQSSeedConsistency(w http.ResponseWriter, r *http.Request) {
	var seed sqsConsistencySeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.GetURLMisses < 0 || seed.GetAttributesMisses < 0 || seed.DeletedRecentlyMisses < 0 {
		http.Error(w, `{"error":"miss counts must not be negative"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), sqsCtrlNamespace, sqsCtrlKey(seed.QueueName), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "queueName": sqsCtrlKey(seed.QueueName)})
}

// handleSQSClearConsistency handles DELETE /v1/sqs/consistency. With
// ?queueName=... it removes that seed; without it removes all.
func (s *Server) handleSQSClearConsistency(w http.ResponseWriter, r *http.Request) {
	if name := r.URL.Query().Get("queueName"); name != "" {
		if err := s.state.Delete(r.Context(), sqsCtrlNamespace, sqsCtrlKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), sqsCtrlNamespace, sqsCtrlKeyPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), sqsCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
