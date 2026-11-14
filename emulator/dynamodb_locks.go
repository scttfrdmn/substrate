package emulator

import (
	"hash/fnv"
	"sort"
	"sync"
)

// dynamodbLockStripes is the number of mutexes in each of [dynamodbLocks]' stripe
// sets. A power of two well above the concurrency any test drives, so distinct items
// almost never contend.
const dynamodbLockStripes = 64

// dynamodbLocks serializes DynamoDB writes per item and per table (#1449).
//
// It exists for the reason [s3KeyMutex] does: [StateManager] offers no
// compare-and-swap, so a conditional write that loaded an item, evaluated its
// ConditionExpression and then saved it left a window in which N concurrent writers
// all evaluated against the same pre-write state and all succeeded. DynamoDB
// serializes conditional writes per item — of N writers whose conditions are mutually
// exclusive, exactly one wins. Holding the item's stripe across load → condition →
// save restores that.
//
// The table stripes guard the per-table read-modify-write records every item write
// touches — the item_keys list Scan and Query walk, and the stream-record ring — which
// otherwise lose entries when two distinct items are written at once. They are a
// separate stripe set from the item stripes and are only ever taken innermost and
// alone, so holding an item stripe while taking a table stripe cannot deadlock.
//
// Like s3KeyMutex the guarantee is process-local: airtight for substrate's
// single-process topology, not across two emulator processes sharing one state backend.
type dynamodbLocks struct {
	items  [dynamodbLockStripes]sync.Mutex
	tables [dynamodbLockStripes]sync.Mutex
}

// dynamodbStripe hashes the parts of a lock name to a stripe index.
func dynamodbStripe(parts ...string) int {
	h := fnv.New32a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return int(h.Sum32() % dynamodbLockStripes)
}

// itemStripe returns the stripe guarding one item of one table.
func (l *dynamodbLocks) itemStripe(accountID, region, tableName, itemKey string) int {
	return dynamodbStripe(accountID, region, tableName, itemKey)
}

// lockItem acquires the stripe guarding one item and returns its unlock function.
func (l *dynamodbLocks) lockItem(accountID, region, tableName, itemKey string) func() {
	mu := &l.items[l.itemStripe(accountID, region, tableName, itemKey)]
	mu.Lock()
	return mu.Unlock
}

// lockItemStripes acquires every listed item stripe in ascending order, each once, and
// returns a function releasing them all. Acquiring in one global order is what keeps
// two transactions over overlapping items from deadlocking, and deduplicating keeps a
// transaction whose items share a stripe from locking it twice.
func (l *dynamodbLocks) lockItemStripes(stripes []int) func() {
	uniq := append([]int(nil), stripes...)
	sort.Ints(uniq)
	n := 0
	for i, s := range uniq {
		if i == 0 || s != uniq[n-1] {
			uniq[n] = s
			n++
		}
	}
	uniq = uniq[:n]
	for _, s := range uniq {
		l.items[s].Lock()
	}
	return func() {
		for i := len(uniq) - 1; i >= 0; i-- {
			l.items[uniq[i]].Unlock()
		}
	}
}

// lockTable acquires the stripe guarding one table's shared records and returns its
// unlock function.
func (l *dynamodbLocks) lockTable(accountID, region, tableName string) func() {
	mu := &l.tables[dynamodbStripe(accountID, region, tableName)]
	mu.Lock()
	return mu.Unlock
}
