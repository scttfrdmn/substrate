package substrate_test

import (
	"context"
	"sort"
	"testing"

	"github.com/scttfrdmn/substrate"
)

func TestMemoryStateManager_GetPutDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	// Get on missing key returns nil, nil.
	val, err := m.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get missing key: unexpected error: %v", err)
	}
	if val != nil {
		t.Fatalf("Get missing key: expected nil, got %v", val)
	}

	// Put then Get round-trips the value.
	if err := m.Put(ctx, "ns", "key", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err = m.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get after Put: %v", err)
	}
	if string(val) != "hello" {
		t.Fatalf("Get after Put: expected %q, got %q", "hello", val)
	}

	// Put overwrites.
	if err := m.Put(ctx, "ns", "key", []byte("world")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	val, _ = m.Get(ctx, "ns", "key")
	if string(val) != "world" {
		t.Fatalf("Put overwrite: expected %q, got %q", "world", val)
	}

	// Delete removes the key.
	if err := m.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	val, err = m.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if val != nil {
		t.Fatalf("Get after Delete: expected nil, got %v", val)
	}

	// Delete of missing key is a no-op.
	if err := m.Delete(ctx, "ns", "missing"); err != nil {
		t.Fatalf("Delete missing key: %v", err)
	}
}

func TestMemoryStateManager_IsolatesNamespaces(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	_ = m.Put(ctx, "nsA", "key", []byte("A"))
	_ = m.Put(ctx, "nsB", "key", []byte("B"))

	a, _ := m.Get(ctx, "nsA", "key")
	b, _ := m.Get(ctx, "nsB", "key")

	if string(a) != "A" {
		t.Fatalf("namespace A: expected A, got %s", a)
	}
	if string(b) != "B" {
		t.Fatalf("namespace B: expected B, got %s", b)
	}
}

func TestMemoryStateManager_List(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	keys := []string{"alpha", "apple", "banana", "apricot"}
	for _, k := range keys {
		_ = m.Put(ctx, "ns", k, []byte(k))
	}

	t.Run("all keys", func(t *testing.T) {
		t.Parallel()
		got, err := m.List(ctx, "ns", "")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		sort.Strings(got)
		want := []string{"alpha", "apple", "apricot", "banana"}
		if len(got) != len(want) {
			t.Fatalf("List all: expected %v, got %v", want, got)
		}
		for i, k := range want {
			if got[i] != k {
				t.Fatalf("List all[%d]: expected %q, got %q", i, k, got[i])
			}
		}
	})

	t.Run("prefix filter", func(t *testing.T) {
		t.Parallel()
		got, err := m.List(ctx, "ns", "ap")
		if err != nil {
			t.Fatalf("List prefix: %v", err)
		}
		sort.Strings(got)
		want := []string{"apple", "apricot"}
		if len(got) != len(want) {
			t.Fatalf("List prefix: expected %v, got %v", want, got)
		}
		for i, k := range want {
			if got[i] != k {
				t.Fatalf("List prefix[%d]: expected %q, got %q", i, k, got[i])
			}
		}
	})

	t.Run("missing namespace", func(t *testing.T) {
		t.Parallel()
		got, err := m.List(ctx, "noexist", "")
		if err != nil {
			t.Fatalf("List missing ns: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("List missing ns: expected empty, got %v", got)
		}
	})
}

func TestMemoryStateManager_GetReturnsCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	_ = m.Put(ctx, "ns", "k", []byte("original"))

	got, _ := m.Get(ctx, "ns", "k")
	got[0] = 'X'

	again, _ := m.Get(ctx, "ns", "k")
	if string(again) != "original" {
		t.Fatalf("Get should return a copy; internal value was mutated to %q", again)
	}
}

func TestMemoryStateManager_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	_ = m.Put(ctx, "ns1", "a", []byte("1"))
	_ = m.Put(ctx, "ns1", "b", []byte("2"))
	_ = m.Put(ctx, "ns2", "x", []byte("3"))

	snap, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("Snapshot returned empty bytes")
	}

	// Mutate state after snapshot.
	_ = m.Put(ctx, "ns1", "a", []byte("changed"))
	_ = m.Delete(ctx, "ns2", "x")

	if err := m.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Values should match pre-snapshot state.
	cases := []struct{ ns, key, want string }{
		{"ns1", "a", "1"},
		{"ns1", "b", "2"},
		{"ns2", "x", "3"},
	}
	for _, c := range cases {
		val, err := m.Get(ctx, c.ns, c.key)
		if err != nil {
			t.Fatalf("Get %s/%s after Restore: %v", c.ns, c.key, err)
		}
		if string(val) != c.want {
			t.Fatalf("Get %s/%s after Restore: expected %q, got %q", c.ns, c.key, c.want, val)
		}
	}
}

func TestMemoryStateManager_Reset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	_ = m.Put(ctx, "ns", "k", []byte("v"))

	if err := m.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	val, err := m.Get(ctx, "ns", "k")
	if err != nil {
		t.Fatalf("Get after Reset: %v", err)
	}
	if val != nil {
		t.Fatalf("Get after Reset: expected nil, got %v", val)
	}

	keys, _ := m.List(ctx, "ns", "")
	if len(keys) != 0 {
		t.Fatalf("List after Reset: expected empty, got %v", keys)
	}
}

func TestMemoryStateManager_SnapshotRestoreEmptyState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	snap, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot empty: %v", err)
	}

	_ = m.Put(ctx, "ns", "k", []byte("v"))

	if err := m.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore to empty: %v", err)
	}

	val, _ := m.Get(ctx, "ns", "k")
	if val != nil {
		t.Fatalf("after Restore to empty state: expected nil, got %v", val)
	}
}

func TestMemoryStateManager_ImplementsInterfaces(t *testing.T) {
	t.Parallel()

	// Compile-time checks embedded in runtime test.
	var _ substrate.StateManager = (*substrate.MemoryStateManager)(nil)
	var _ substrate.SnapshotableStateManager = (*substrate.MemoryStateManager)(nil)
}

func TestMemoryStateManager_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := substrate.NewMemoryStateManager()

	const goroutines = 10
	const ops = 100

	done := make(chan struct{})
	for i := range goroutines {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			key := string(rune('a' + id))
			for range ops {
				_ = m.Put(ctx, "ns", key, []byte(key))
				_, _ = m.Get(ctx, "ns", key)
				_, _ = m.List(ctx, "ns", "")
			}
		}(i)
	}

	for range goroutines {
		<-done
	}
}
