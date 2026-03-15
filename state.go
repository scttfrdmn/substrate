package substrate

import (
	"context"
	"encoding/json"
	"fmt"
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

// List returns all keys in namespace whose names begin with prefix.
// An empty prefix returns all keys in the namespace.
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
