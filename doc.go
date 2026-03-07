// Package substrate provides an event-sourced AWS emulation layer for
// deterministic testing of AI-generated infrastructure code.
//
// # Overview
//
// Substrate's three core differentiators:
//
//  1. Deterministic reproducibility: event sourcing + seeded RNG + time control
//     means the same test inputs always produce the same outputs.
//
//  2. Time-travel debugging: step backward through recorded request history
//     and inspect AWS service state at any point in time.
//
//  3. Cost visibility: real AWS pricing is tracked per operation so you know
//     projected monthly costs before the bill arrives.
//
// # Quick Start
//
// Start a recording session, run your tests against the emulator, then replay:
//
//	engine := substrate.NewReplayEngine(store, state, tc, registry, cfg, log)
//
//	session, err := engine.StartRecording(ctx, "my-integration-test")
//	if err != nil { ... }
//
//	// Run tests against AWS SDK pointed at localhost:4566
//
//	count, err := engine.StopRecording(ctx, session)
//
//	results, err := engine.Replay(ctx, session.StreamID)
//
// # Architecture
//
// All types live in a single package. The [EventStore] records every AWS
// request as an immutable [Event]. The [ReplayEngine] replays event streams
// for deterministic test validation and time-travel debugging.
//
// Service emulation is provided by [Plugin] implementations registered with
// a [PluginRegistry]. State is persisted via the [StateManager] interface.
package substrate
