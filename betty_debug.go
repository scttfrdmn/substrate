package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// DebugSession supports time-travel inspection of a recorded event stream.
// Create via BettyClient.NewDebugSession; navigation loads the stream on first call.
type DebugSession struct {
	engine   *ReplayEngine
	streamID string
	ready    bool
}

// ensureLoaded initializes the replay engine with the stream on the first
// navigation call.
func (d *DebugSession) ensureLoaded(ctx context.Context) error {
	if d.ready {
		return nil
	}
	if _, ok := d.engine.stateManager.(SnapshotableStateManager); !ok {
		return fmt.Errorf("state manager does not implement SnapshotableStateManager; time-travel debugging unavailable")
	}
	if _, err := d.engine.Replay(ctx, d.streamID); err != nil {
		return fmt.Errorf("load stream %s: %w", d.streamID, err)
	}
	d.ready = true
	return nil
}

// JumpToEvent advances or rewinds the replay to event at sequence seq.
func (d *DebugSession) JumpToEvent(ctx context.Context, seq int64) error {
	if err := d.ensureLoaded(ctx); err != nil {
		return err
	}
	return d.engine.JumpToEvent(ctx, seq)
}

// StepBackward reverses one event, restoring state to the previous snapshot.
func (d *DebugSession) StepBackward(ctx context.Context) error {
	if err := d.ensureLoaded(ctx); err != nil {
		return err
	}
	_, err := d.engine.StepBackward(ctx)
	return err
}

// InspectState returns the raw JSON snapshot of the state manager at the
// current replay position. The snapshot format is map[namespace]map[key]base64value.
// If no navigation has been performed yet, it returns the state as it currently
// stands in the state manager.
func (d *DebugSession) InspectState(ctx context.Context) ([]byte, error) {
	ss, ok := d.engine.stateManager.(SnapshotableStateManager)
	if !ok {
		return nil, fmt.Errorf("state manager does not implement SnapshotableStateManager")
	}

	raw, err := ss.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot state: %w", err)
	}

	// raw is already a JSON snapshot from MemoryStateManager; parse and re-encode
	// as map[namespace]map[key]base64value for a consistent external format.
	var decoded map[string]map[string][]byte
	if jsonErr := json.Unmarshal(raw, &decoded); jsonErr == nil {
		// Already in the desired format — return as-is.
		return raw, nil
	}

	// Fallback: wrap raw bytes as a base64-encoded value.
	out := map[string]string{
		"raw": base64.StdEncoding.EncodeToString(raw),
	}
	return json.Marshal(out)
}
