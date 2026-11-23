package emulator

import "sort"

// ReplaySnapshotPositionsForTest returns, sorted, the positions at which the engine's
// active replay holds a restorable snapshot. Exported so a test can tell a jump that
// restored a snapshot from one that replayed from the start, which no state reading
// can: both reach the same state.
func ReplaySnapshotPositionsForTest(e *ReplayEngine) []int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.currentReplay == nil {
		return nil
	}
	var out []int
	for pos := range e.currentReplay.pluginSnapshots {
		if _, ok := e.currentReplay.StateSnapshots[pos]; ok {
			out = append(out, pos)
		}
	}
	sort.Ints(out)
	return out
}

// ReplayPositionForTest returns the engine's active replay position, or -1.
func ReplayPositionForTest(e *ReplayEngine) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.currentReplay == nil {
		return -1
	}
	return e.currentReplay.Position
}
