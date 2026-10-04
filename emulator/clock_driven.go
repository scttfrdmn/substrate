package emulator

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
)

// Work a plugin does because simulated time passed, rather than because a request asked
// for it, and how that work stays deterministic and replayable (#1292).
//
// An event-source mapping is the case that needs it. A real mapping consumes its queue on
// its own; nothing the caller sends names the poll. Substrate used to model that with a
// goroutine on a one-second wall-clock ticker, which made an ESM's effect on state depend
// on how long a test happened to run, ignored a frozen or scaled clock, and recorded
// nothing, so a replay could not reproduce it.
//
// The model now is evaluation at observation. Before every top-level request reaches its
// plugin — on the live path in [Server.handleAWSRequest] and on replay in
// [ReplayEngine.replayEvent], at the same step of the same pipeline — each
// [ClockDrivenPlugin] runs whatever is due at the simulated clock's current reading. A
// frozen clock makes nothing due, so nothing happens; a scaled clock scales the cadence,
// because the cadence is measured in simulated time. And because a replay pins the clock
// to each recorded event's timestamp, the same work comes due at the same request in the
// stream, with request IDs derived from that request's ID, so it mints the same
// identifiers and leaves the same state.
//
// The live path also records each dispatch the work makes, so the event log shows what an
// ESM consumed and invoked. A replay skips those recorded dispatches rather than
// re-executing them: re-deriving them from the triggering request is what reproduces
// them, and executing both would apply each twice. See [isClockDrivenDispatchEvent].
//
// Internal dispatch paths — the CloudFormation deployer's and an API Gateway proxy
// integration's — do not run the work. They are nested inside a top-level request that
// already did, at the same clock reading.

// ClockDrivenPlugin is a [Plugin] that does work because simulated time passed.
type ClockDrivenPlugin interface {
	// RunDue performs the work due at the plugin's clock. trigger is the top-level
	// request about to be dispatched; the work derives its request IDs from it. dispatch
	// routes one internal request and must be used for every request the work makes, so
	// the caller can record it. The returned error reports work that failed, and does not
	// refuse the triggering request.
	RunDue(trigger *RequestContext, dispatch InternalDispatch) error
}

// InternalDispatch routes one request a [ClockDrivenPlugin] makes on its own behalf.
type InternalDispatch func(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error)

// clockDrivenRequestIDPrefix begins the request ID of every dispatch a [ClockDrivenPlugin]
// makes, which is how a replay recognizes the recorded copy of one.
const clockDrivenRequestIDPrefix = "req-clock-"

// clockDrivenRequestID derives the request ID of one dispatch from the request that
// triggered the work and a label naming the dispatch within it. It is a pure function of
// both, so a replay of the triggering request derives the same ID, and the [IDMint] seeded
// from it mints the same identifiers.
func clockDrivenRequestID(trigger, label string) string {
	sum := sha256.Sum256([]byte(trigger + "\x00" + label))
	return clockDrivenRequestIDPrefix + hex.EncodeToString(sum[:12])
}

// isClockDrivenDispatchEvent reports whether event is a recorded dispatch of clock-driven
// work, which a replay skips because replaying the request that triggered it re-derives it.
func isClockDrivenDispatchEvent(event *Event) bool {
	return event != nil && strings.HasPrefix(event.RequestID, clockDrivenRequestIDPrefix)
}

// RunClockDriven runs every registered [ClockDrivenPlugin]'s due work, in plugin-name
// order so the order is the same on every run. A plugin's failure does not stop the
// others; the errors are joined.
func (r *PluginRegistry) RunClockDriven(trigger *RequestContext, dispatch InternalDispatch) error {
	r.mu.RLock()
	names := make([]string, 0, len(r.plugins))
	for name, p := range r.plugins {
		if _, ok := p.(ClockDrivenPlugin); ok {
			names = append(names, name)
		}
	}
	r.mu.RUnlock()
	sort.Strings(names)

	var errs []error
	for _, name := range names {
		p, ok := r.Plugin(name)
		if !ok {
			continue
		}
		cd, ok := p.(ClockDrivenPlugin)
		if !ok {
			continue
		}
		if err := cd.RunDue(trigger, dispatch); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
