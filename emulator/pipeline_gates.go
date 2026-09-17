package emulator

import "time"

// prePluginStep names one of the pipeline stages a request passes through before it
// reaches a plugin. It is the refusing stage's identity, carried out of
// [prePluginGates.check] so a caller can report the refusal in whatever terms its own
// path uses — an HTTP error and a metrics counter on the live path, a comparison
// against the recorded outcome on the replay path.
type prePluginStep string

const (
	// stepAuth is cross-service IAM authorization, step 2 of the pipeline.
	stepAuth prePluginStep = "auth"

	// stepQuota is rate-limit enforcement, step 3.
	stepQuota prePluginStep = "quota"

	// stepConsistency is the eventual-consistency read gate, step 4.
	stepConsistency prePluginStep = "consistency"

	// stepFault is fault injection, step 4.5.
	stepFault prePluginStep = "fault"
)

// prePluginGates holds the four controllers a request passes through between parsing
// and plugin dispatch. Any of them may be nil, in which case that step is skipped.
//
// It exists so that the live path and the replay path cannot answer differently. Both
// halves of that contract were written and never joined: [QuotaController.CheckQuota]
// and [ConsistencyController.CheckRead] each open with an [isReplaying] guard, and
// [ReplayEngine.replayEvent] dutifully sets the flag those guards read — but a replay
// called [PluginRegistry.RouteRequest] directly, so the guards were unreachable from
// the only path that sets the flag, and a request the recording refused with a 403, a
// 429 or an injected fault was re-executed on replay and *succeeded* (#833).
//
// The two other dispatch paths, [StackDeployer.dispatch] and
// [Server.stateAtSequence], keep their own narrower subsets of the pipeline. Bringing
// those under this type as well is a larger change than #833 needs and is not
// attempted here.
type prePluginGates struct {
	auth        *AuthController
	quota       *QuotaController
	consistency *ConsistencyController
	fault       *FaultController
}

// prePluginOutcome is what the four gates decided about one request.
type prePluginOutcome struct {
	// step names the gate that refused, and is empty when none did.
	step prePluginStep

	// err is the refusal, or nil when the request may proceed to a plugin.
	err error

	// latency is the delay a latency fault rule asks for, and zero when no rule
	// matched. It is separate from err because a latency rule is not a refusal: the
	// caller waits and the request proceeds. Whether the wait is honored is the
	// caller's decision — the live server sleeps, a replay does not, since no replay
	// may consume wall-clock time.
	latency time.Duration
}

// check runs the four gates in pipeline order and reports the first refusal.
//
// The order is the one [Server.handleAWSRequest] documents and is load-bearing:
// authorization decides whether the caller may make the request at all, so it comes
// before the two controllers that decide whether substrate will serve it now, and
// fault injection comes last so that an injected failure stands in for the service's
// own answer rather than for a refusal that would have happened anyway.
//
// A latency fault is reported alongside a nil error, and — like the live path before
// this type existed — the rule's fired count is spent either way, so the caller must
// treat a zero err with a non-zero latency as a request that proceeds.
func (g prePluginGates) check(reqCtx *RequestContext, req *AWSRequest) prePluginOutcome {
	if g.auth != nil {
		if err := g.auth.CheckAccess(reqCtx, req); err != nil {
			return prePluginOutcome{step: stepAuth, err: err}
		}
	}
	if g.quota != nil {
		if err := g.quota.CheckQuota(reqCtx, req); err != nil {
			return prePluginOutcome{step: stepQuota, err: err}
		}
	}
	if g.consistency != nil {
		if err := g.consistency.CheckRead(reqCtx, req); err != nil {
			return prePluginOutcome{step: stepConsistency, err: err}
		}
	}
	if g.fault != nil {
		faultErr, delay := g.fault.InjectFault(reqCtx, req)
		out := prePluginOutcome{latency: delay}
		if faultErr != nil {
			out.step, out.err = stepFault, faultErr
		}
		return out
	}
	return prePluginOutcome{}
}

// prePluginGates returns the server's four pre-plugin controllers.
func (s *Server) prePluginGates() prePluginGates {
	return prePluginGates{
		auth:        s.opts.Auth,
		quota:       s.opts.Quota,
		consistency: s.opts.Consistency,
		fault:       s.opts.Fault,
	}
}

// gates returns the replay engine's four pre-plugin controllers, so a replayed request
// passes through the same [prePluginGates.check] a live one does.
func (p ReplayPipeline) gates() prePluginGates {
	return prePluginGates{
		auth:        p.Auth,
		quota:       p.Quota,
		consistency: p.Consistency,
		fault:       p.Fault,
	}
}
