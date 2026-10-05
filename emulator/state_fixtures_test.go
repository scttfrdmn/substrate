package emulator_test

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/scttfrdmn/substrate/emulator"
)

// errStoreUnavailable is the failure [failingPutStateManager] returns from a write it refuses.
var errStoreUnavailable = errors.New("state store unavailable")

// failingPutStateManager is a StateManager whose writes can be made to fail, so an operation's
// write can be refused after its reads have succeeded. [emulator.MemoryStateManager] never fails
// a Put, which is how every discarded write #1175 and #1192 found went unnoticed: the only way to
// see a swallowed write is a store that refuses one.
//
// It was iam_tagging_test.go's iamFailingPutStateManager and cloudwatchlogs_tags_test.go's
// cwlFailingPutStateManager, two copies of one fixture, until #1192 promoted it here. Unarmed it is
// a pass-through. [failingPutStateManager.failEvery] refuses every write from then on, which is
// what those two tests armed. [failingPutStateManager.failNth] refuses only the nth write, so a
// sweep can refuse each write an operation makes in turn, and [failingPutStateManager.failKey]
// refuses only writes to a key containing a substring, so a test can aim at one record, such as
// a list index, and let the rest through.
type failingPutStateManager struct {
	inner emulator.StateManager

	mu sync.Mutex
	// mode is what an armed store does with a write: "" passes it through, "every" fails it,
	// "nth" fails only write number nth, "key" fails one whose key contains match, and "watch"
	// passes it through while recording its key.
	mode  string
	nth   int
	match string
	// written is the keys of the writes seen since the store was last armed, in order,
	// including the one refused.
	written []string
}

// newFailingPutStateManager is an unarmed store over a fresh in-memory one.
func newFailingPutStateManager() *failingPutStateManager {
	return &failingPutStateManager{inner: emulator.NewMemoryStateManager()}
}

func (m *failingPutStateManager) arm(mode string, nth int, match string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode, m.nth, m.match, m.written = mode, nth, match, nil
}

// failEvery refuses every write from now on.
func (m *failingPutStateManager) failEvery() { m.arm("every", 0, "") }

// failNth refuses the nth write from now on, counting from one, and passes the rest through.
func (m *failingPutStateManager) failNth(n int) { m.arm("nth", n, "") }

// failKey refuses every write to a key containing match from now on.
func (m *failingPutStateManager) failKey(match string) { m.arm("key", 0, match) }

// watch records the key of every write from now on without refusing any.
func (m *failingPutStateManager) watch() { m.arm("watch", 0, "") }

// writes is the keys written since the store was last armed, in order.
func (m *failingPutStateManager) writes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.written...)
}

func (m *failingPutStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	return m.inner.Get(ctx, namespace, key)
}

func (m *failingPutStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	m.mu.Lock()
	refuse := false
	if m.mode != "" {
		m.written = append(m.written, namespace+"/"+key)
		switch m.mode {
		case "every":
			refuse = true
		case "nth":
			refuse = len(m.written) == m.nth
		case "key":
			refuse = strings.Contains(key, m.match)
		}
	}
	m.mu.Unlock()
	if refuse {
		return errStoreUnavailable
	}
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *failingPutStateManager) Delete(ctx context.Context, namespace, key string) error {
	return m.inner.Delete(ctx, namespace, key)
}

func (m *failingPutStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}
