package emulator

import (
	"context"
	"fmt"
)

// DebugSession supports time-travel inspection of a recorded event stream.
// Create via [Client.NewDebugSession]; navigation loads the stream on first call.
//
// The session replays into its own [ReplaySandbox], built with the default plugins, so
// navigating never resets or writes to the client's live state. A plugin registered
// only on the client's own registry is not in the sandbox.
type DebugSession struct {
	client   *Client
	streamID string
	sandbox  *ReplaySandbox
}

// ensureLoaded builds the sandbox and replays the stream into it once, on the first
// navigation call, so its differences and snapshots are recorded.
func (d *DebugSession) ensureLoaded(ctx context.Context) error {
	if d.sandbox != nil {
		return nil
	}
	// A Client dispatches through no quota, consistency or fault controller, so its
	// replay re-decides none.
	sandbox, err := NewReplaySandbox(ctx, nil, d.client.store, d.client.logger, withSandboxGatesOf(ServerOptions{}))
	if err != nil {
		return err
	}
	if _, err := sandbox.Engine().Replay(ctx, d.streamID); err != nil {
		return fmt.Errorf("load stream %s: %w", d.streamID, err)
	}
	d.sandbox = sandbox
	return nil
}

// JumpToEvent moves the session to just before the event with sequence seq (see
// [ReplayEngine.JumpToSequence]).
func (d *DebugSession) JumpToEvent(ctx context.Context, seq int64) error {
	if err := d.ensureLoaded(ctx); err != nil {
		return err
	}
	return d.sandbox.Engine().JumpToSequence(ctx, seq)
}

// StepBackward moves the session back one event (see [ReplayEngine.StepBackward]).
func (d *DebugSession) StepBackward(ctx context.Context) error {
	if err := d.ensureLoaded(ctx); err != nil {
		return err
	}
	_, err := d.sandbox.Engine().StepBackward(ctx)
	return err
}

// Results returns the differences and counts from loading the stream, or nil before
// the first navigation.
func (d *DebugSession) Results() *ReplayResults {
	if d.sandbox == nil {
		return nil
	}
	d.sandbox.engine.mu.RLock()
	defer d.sandbox.engine.mu.RUnlock()
	if d.sandbox.engine.currentReplay == nil {
		return nil
	}
	return d.sandbox.engine.currentReplay.Results
}

// InspectState returns the JSON snapshot of the state at the session's position, in the
// format map[namespace]map[key]base64value. Before the first navigation it returns the
// client's live state, read without modifying it.
func (d *DebugSession) InspectState(ctx context.Context) ([]byte, error) {
	sm := d.client.state
	if d.sandbox != nil {
		sm = d.sandbox.StateManager()
	}
	ss, ok := sm.(SnapshotableStateManager)
	if !ok {
		return nil, fmt.Errorf("state manager does not implement SnapshotableStateManager")
	}
	raw, err := ss.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot state: %w", err)
	}
	return raw, nil
}
