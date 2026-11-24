package emulator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"sync/atomic"

	"github.com/spf13/afero"
)

// replaySnapshotter is implemented by every [ResettablePlugin] whose state outside the
// [StateManager] a replay snapshot can capture and restore.
//
// A snapshot of the state manager alone is not the emulator's state: S3 keeps object
// payloads and a version counter on the plugin, SES v2 a message counter. Restoring
// only the state manager would leave those at the furthest point the replay reached.
// So [ReplayEngine] uses a snapshot only when every resettable plugin implements this
// interface, and only at a position where each of them produced one.
type replaySnapshotter interface {
	ResettablePlugin

	// snapshotForReplay returns the plugin's run state, or errReplaySnapshotUnsupported
	// when it cannot be captured.
	snapshotForReplay() (any, error)

	// restoreForReplay puts back a value snapshotForReplay returned.
	restoreForReplay(ctx context.Context, snapshot any) error
}

// errReplaySnapshotUnsupported reports plugin state a replay snapshot cannot capture,
// such as a filesystem the caller injected.
var errReplaySnapshotUnsupported = errors.New("plugin state cannot be snapshotted for replay")

// snapshotInterval returns how many events apart the engine takes snapshots, from the
// store's [EventStoreConfig.SnapshotInterval]. Zero means none.
func (r *ReplayEngine) snapshotInterval() int {
	if r.eventStore == nil {
		return 0
	}
	return r.eventStore.config.SnapshotInterval
}

// snapshotsUsable reports whether a snapshot can stand for the emulator's whole state.
//
// An armed [FaultController] rules it out: its fired counts and per-rule random streams
// advance as a run proceeds and cannot be captured, so a snapshot restored under armed
// rules would answer the requests after it with different draws than a replay from the
// start. An enabled [QuotaController] or [ConsistencyController] rules it out for the
// same reason: its buckets and windows are not in the snapshot.
func (r *ReplayEngine) snapshotsUsable() bool {
	if _, ok := r.stateManager.(SnapshotableStateManager); !ok || r.registry == nil {
		return false
	}
	if r.pipeline.Fault != nil && r.pipeline.Fault.hasArmedRules() {
		return false
	}
	// Token buckets and propagation windows are run state of the same kind.
	if r.pipeline.Quota.enabled() || r.pipeline.Consistency.enabled() {
		return false
	}
	_, ok := r.registry.replaySnapshotters()
	return ok
}

// maybeSnapshot takes a snapshot before the event at the replay's position when one is
// due, the replay has moved backward at least once (see [ActiveReplay.snapshotting]),
// and the emulator's state can be captured. A failure is logged and leaves no snapshot:
// snapshots only shorten a jump.
func (r *ReplayEngine) maybeSnapshot(ctx context.Context, replay *ActiveReplay) {
	pos := replay.Position
	interval := r.snapshotInterval()
	if !replay.snapshotting || interval <= 0 || pos == 0 || pos%interval != 0 {
		return
	}
	if _, done := replay.pluginSnapshots[pos]; done || !r.snapshotsUsable() {
		return
	}
	state, err := r.stateManager.(SnapshotableStateManager).Snapshot(ctx)
	if err != nil {
		r.logger.Debug("replay snapshot skipped", "position", pos, "error", err)
		return
	}
	plugins, err := r.registry.snapshotPluginsForReplay()
	if err != nil {
		r.logger.Debug("replay snapshot skipped", "position", pos, "error", err)
		return
	}
	replay.StateSnapshots[pos] = state
	replay.pluginSnapshots[pos] = plugins
}

// nearestSnapshot returns the latest restorable snapshot position at or before target.
func (r *ReplayEngine) nearestSnapshot(replay *ActiveReplay, target int) (int, bool) {
	if !r.snapshotsUsable() {
		return 0, false
	}
	best := -1
	for pos := range replay.pluginSnapshots {
		if _, ok := replay.StateSnapshots[pos]; ok && pos <= target && pos > best {
			best = pos
		}
	}
	return best, best >= 0
}

// restoreSnapshot puts the emulator back into the state recorded at pos.
func (r *ReplayEngine) restoreSnapshot(ctx context.Context, replay *ActiveReplay, pos int) error {
	if err := r.restoreState(ctx, replay.StateSnapshots[pos]); err != nil {
		return fmt.Errorf("restore state snapshot at position %d: %w", pos, err)
	}
	if err := r.registry.restorePluginsForReplay(ctx, replay.pluginSnapshots[pos]); err != nil {
		return fmt.Errorf("restore plugin snapshot at position %d: %w", pos, err)
	}
	return nil
}

// replaySnapshotters returns every resettable plugin by name, and false when one of them
// does not implement [replaySnapshotter].
func (r *PluginRegistry) replaySnapshotters() (map[string]replaySnapshotter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]replaySnapshotter)
	for name, p := range r.plugins {
		if _, resettable := p.(ResettablePlugin); !resettable {
			continue
		}
		rs, ok := p.(replaySnapshotter)
		if !ok {
			return nil, false
		}
		out[name] = rs
	}
	return out, true
}

// snapshotPluginsForReplay captures every resettable plugin's run state.
func (r *PluginRegistry) snapshotPluginsForReplay() (map[string]any, error) {
	plugins, ok := r.replaySnapshotters()
	if !ok {
		return nil, errReplaySnapshotUnsupported
	}
	out := make(map[string]any, len(plugins))
	for name, p := range plugins {
		snap, err := p.snapshotForReplay()
		if err != nil {
			return nil, fmt.Errorf("snapshot plugin %s: %w", name, err)
		}
		out[name] = snap
	}
	return out, nil
}

// restorePluginsForReplay restores what [PluginRegistry.snapshotPluginsForReplay]
// captured, in plugin-name order.
func (r *PluginRegistry) restorePluginsForReplay(ctx context.Context, snaps map[string]any) error {
	plugins, ok := r.replaySnapshotters()
	if !ok {
		return errReplaySnapshotUnsupported
	}
	names := make([]string, 0, len(plugins))
	for name := range plugins {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := plugins[name].restoreForReplay(ctx, snaps[name]); err != nil {
			return fmt.Errorf("restore plugin %s: %w", name, err)
		}
	}
	return nil
}

// hasArmedRules reports whether the controller is enabled with a rule. A nil controller
// has none.
func (f *FaultController) hasArmedRules() bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return (f.armed.Enabled && len(f.armed.Rules) > 0) || (f.config.Enabled && len(f.config.Rules) > 0)
}

// cloneArmed returns a new controller armed with f's rules and seed, so a replay into a
// separate emulator re-decides faults without advancing f's own counters. A nil
// controller clones to nil.
func (f *FaultController) cloneArmed() *FaultController {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return NewFaultController(f.armed, f.seed)
}

// s3ReplaySnapshot is the S3 plugin's run state: its version counter and every payload.
type s3ReplaySnapshot struct {
	versionSeq int64
	dirs       []string
	files      map[string][]byte
}

// snapshotForReplay copies the version counter and the payload filesystem. A filesystem
// the caller injected may be backed by a directory, so it is not copied.
func (p *S3Plugin) snapshotForReplay() (any, error) {
	if !p.ownsFS {
		return nil, errReplaySnapshotUnsupported
	}
	snap := s3ReplaySnapshot{
		versionSeq: atomic.LoadInt64(&p.versionSeq),
		files:      make(map[string][]byte),
	}
	err := afero.Walk(p.fs, "/", func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			snap.dirs = append(snap.dirs, name)
			return nil
		}
		data, readErr := afero.ReadFile(p.fs, name)
		if readErr != nil {
			return readErr
		}
		snap.files[name] = data
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("copy s3 payloads: %w", err)
	}
	return snap, nil
}

// restoreForReplay empties the payload filesystem and writes the snapshot back into it.
func (p *S3Plugin) restoreForReplay(_ context.Context, v any) error {
	snap, ok := v.(s3ReplaySnapshot)
	if !ok {
		return fmt.Errorf("s3 replay snapshot has type %T", v)
	}
	if err := p.emptyFilesystem(); err != nil {
		return err
	}
	for _, dir := range snap.dirs {
		if err := p.fs.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("restore s3 directory %s: %w", dir, err)
		}
	}
	for name, data := range snap.files {
		if err := p.fs.MkdirAll(path.Dir(name), 0o755); err != nil {
			return fmt.Errorf("restore s3 directory for %s: %w", name, err)
		}
		if err := afero.WriteFile(p.fs, name, data, 0o644); err != nil {
			return fmt.Errorf("restore s3 payload %s: %w", name, err)
		}
	}
	atomic.StoreInt64(&p.versionSeq, snap.versionSeq)
	return nil
}

// snapshotForReplay copies the message counter.
func (p *SESv2Plugin) snapshotForReplay() (any, error) { return p.msgSeq.Load(), nil }

// restoreForReplay puts the message counter back.
func (p *SESv2Plugin) restoreForReplay(_ context.Context, v any) error {
	seq, ok := v.(int64)
	if !ok {
		return fmt.Errorf("sesv2 replay snapshot has type %T", v)
	}
	p.msgSeq.Store(seq)
	return nil
}

// snapshotForReplay copies the set of event-source mappings [LambdaPlugin.RunDue] polls.
// Their poll cursors are in the state manager.
func (p *LambdaPlugin) snapshotForReplay() (any, error) {
	p.esmMu.Lock()
	defer p.esmMu.Unlock()
	active := make(map[string]struct{}, len(p.esmActive))
	for uuid := range p.esmActive {
		active[uuid] = struct{}{}
	}
	return active, nil
}

// restoreForReplay puts back the polled mapping set, so a mapping created before the
// snapshot keeps polling after a backward jump.
func (p *LambdaPlugin) restoreForReplay(_ context.Context, v any) error {
	snap, ok := v.(map[string]struct{})
	if !ok {
		return fmt.Errorf("lambda replay snapshot has type %T", v)
	}
	active := make(map[string]struct{}, len(snap))
	for uuid := range snap {
		active[uuid] = struct{}{}
	}
	p.esmMu.Lock()
	p.esmActive = active
	p.esmMu.Unlock()
	return nil
}
