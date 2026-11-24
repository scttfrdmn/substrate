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

	// stepValidate is request validation against the vendored service models, step 4.6.
	stepValidate prePluginStep = "validate"
)

// prePluginGates holds the four controllers a request passes through between parsing
// and plugin dispatch, and whether it is validated against its input shape. Any controller
// may be nil, in which case that step is skipped.
//
// It exists so that the live path and the replay path cannot answer differently: a
// replay called [PluginRegistry.RouteRequest] directly, so a request the recording
// refused with a 403, a 429 or an injected fault was re-executed on replay and
// *succeeded* (#833). All four gates re-decide a replayed request (quota and
// consistency since #1601), each from a controller the replay rewinds first.
//
// [StackDeployer.dispatch] keeps its own narrower subset of the pipeline (authorization
// only), the API Gateway proxy checks nothing, and a replay of either's recorded
// dispatches passes authorization only.
type prePluginGates struct {
	auth        *AuthController
	quota       *QuotaController
	consistency *ConsistencyController
	fault       *FaultController

	// validate runs [validateRequest] after the four controllers. The live server sets it from
	// wire.validate; a replay sets it per event, from [Event.ValidationModel], and never for an
	// internal dispatch.
	validate bool
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

	// validated reports that the validation step ran on the request, which a live server
	// records as [Event.ValidationModel] so a replay knows to run it again.
	validated bool
}

// check runs the gates in pipeline order and reports the first refusal.
//
// The order is the one [Server.handleAWSRequest] documents and is load-bearing:
// authorization decides whether the caller may make the request at all, so it comes
// before the two controllers that decide whether substrate will serve it now, and
// fault injection follows them so that an injected failure stands in for the service's
// own answer rather than for a refusal that would have happened anyway. Validation comes
// last: it is the first part of the service's own answer, so an injected fault replaces
// it as it replaces the rest, and a caller refused authorization learns nothing about
// what its request carried.
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
	var out prePluginOutcome
	if g.fault != nil {
		faultErr, delay := g.fault.InjectFault(reqCtx, req)
		out.latency = delay
		if faultErr != nil {
			out.step, out.err = stepFault, faultErr
			return out
		}
	}
	if g.validate && wireValidates(req) {
		out.validated = true
		if err := validateRequest(req); err != nil {
			out.step, out.err = stepValidate, err
		}
	}
	return out
}

// recordOption is the [EventRecordOption] that marks the request validated, when it was.
func (o prePluginOutcome) recordOption() EventRecordOption {
	if !o.validated {
		return func(*eventRecordOptions) {}
	}
	return WithValidationModel(wireModelVersion)
}

// prePluginGates returns the server's pre-plugin gates.
func (s *Server) prePluginGates() prePluginGates {
	return prePluginGates{
		auth:        s.opts.Auth,
		quota:       s.opts.Quota,
		consistency: s.opts.Consistency,
		fault:       s.opts.Fault,
		validate:    !s.config.Wire.SkipValidation,
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
