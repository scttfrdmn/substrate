package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryStateManager is a thread-safe, in-memory [StateManager] backed by a
// nested map of namespace → key → value. It also implements
// [SnapshotableStateManager] so snapshots and state-driven replay work without
// an external storage dependency.
type MemoryStateManager struct {
	mu   sync.RWMutex
	data map[string]map[string][]byte
}

// NewMemoryStateManager creates an empty MemoryStateManager.
func NewMemoryStateManager() *MemoryStateManager {
	return &MemoryStateManager{
		data: make(map[string]map[string][]byte),
	}
}

// NewStateManager builds the [StateManager] named by cfg.
//
// It exists so that the server and the replay engine cannot disagree about which
// backend is in use. Before #881 there was nothing to disagree with: `state:` was
// validated and then read by nobody, and both call sites constructed a
// [MemoryStateManager] unconditionally — so a config asking for anything else got
// memory, silently.
//
// An unrecognized backend is an error rather than a fallback to memory. A caller
// who asked for persistence and got a manager that forgets everything at process
// exit has been handed a wrong answer, and the failure surfaces later as absent
// state rather than here as a refused configuration. [Validate] refuses an
// unknown backend at load time, so a Config that came through [LoadConfig] cannot
// reach this error; a Config built in process can.
//
// An empty Backend selects memory, matching [DefaultConfig]: it means the section
// was not written rather than that another backend was requested.
func NewStateManager(cfg StateCfg) (StateManager, error) {
	switch cfg.Backend {
	case "", "memory":
		return NewMemoryStateManager(), nil
	case "sqlite":
		// Accepted by Validate until #881 and implemented by nothing; #2 adds it.
		return nil, fmt.Errorf("state.backend %q is not implemented; choose memory", cfg.Backend)
	default:
		return nil, fmt.Errorf("state.backend %q is not valid; choose memory", cfg.Backend)
	}
}

// Get retrieves the value stored at namespace/key.
// Returns (nil, nil) if the key does not exist.
func (m *MemoryStateManager) Get(_ context.Context, namespace, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ns, ok := m.data[namespace]
	if !ok {
		return nil, nil
	}

	val, ok := ns[key]
	if !ok {
		return nil, nil
	}

	// Return a copy so callers cannot mutate internal state.
	out := make([]byte, len(val))
	copy(out, val)

	return out, nil
}

// Put stores value at namespace/key, creating or overwriting as needed.
func (m *MemoryStateManager) Put(_ context.Context, namespace, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data[namespace] == nil {
		m.data[namespace] = make(map[string][]byte)
	}

	cp := make([]byte, len(value))
	copy(cp, value)
	m.data[namespace][key] = cp

	return nil
}

// Delete removes namespace/key. No error is returned if the key is absent.
func (m *MemoryStateManager) Delete(_ context.Context, namespace, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ns, ok := m.data[namespace]; ok {
		delete(ns, key)
	}

	return nil
}

// List returns all keys in namespace whose names begin with prefix, sorted
// lexicographically. An empty prefix returns all keys in the namespace.
//
// The sort is the contract, not a convenience. State is held in a Go map, whose
// iteration order is randomized per process, so an unsorted return made every
// caller that renders these keys in order answer differently from one run to the
// next. Verified operation by operation, that is S3 ListBuckets,
// ListMultipartUploads and ListObjectVersions, the four ELBv2 describes that
// return a list, RDS's five describes and fifteen EC2 describes; the rest of the
// 119 call sites are single-key lookups or mutations that stop at the first match,
// where the order was never observable. Worst of the set are the ones that page
// over the keys with a cursor — RDS's Marker, ELBv2's Marker/PageSize — because a
// cursor over an unstable order can omit or repeat a resource across pages rather
// than merely reordering them.
//
// Sorting here rather than at each call site is deliberate. The same defect had
// already been fixed five separate times at five individual sites — CFN stack
// tags (#764), aws:TagKeys, CreateSnapshots' snapshotSet, DeleteSnapshot's
// image-ID tie-break, and the four tag merge helpers (#862) — each with its own
// written rationale, and each leaving every other caller exposed. The state
// hash was already deterministic only by accident, because [MemoryStateManager.Snapshot]
// marshals a map and encoding/json sorts map keys on the way out (#865).
//
// Where AWS documents an order other than lexicographic for a particular
// operation, that operation sorts into it after this call; see
// S3's listMultipartUploads and listObjectVersions.
func (m *MemoryStateManager) List(_ context.Context, namespace, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ns, ok := m.data[namespace]
	if !ok {
		return []string{}, nil
	}

	keys := make([]string, 0, len(ns))
	for k := range ns {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	return keys, nil
}

// Snapshot serializes the entire manager contents to JSON.
// Values are encoded as base64 by the standard JSON marshaller.
func (m *MemoryStateManager) Snapshot(_ context.Context) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := json.Marshal(m.data)
	if err != nil {
		return nil, fmt.Errorf("snapshot marshal: %w", err)
	}

	return data, nil
}

// Restore replaces all state with the contents previously produced by [Snapshot].
func (m *MemoryStateManager) Restore(_ context.Context, raw []byte) error {
	var incoming map[string]map[string][]byte
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return fmt.Errorf("restore unmarshal: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = incoming

	return nil
}

// Reset wipes all state, leaving the manager empty.
func (m *MemoryStateManager) Reset(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = make(map[string]map[string][]byte)

	return nil
}
