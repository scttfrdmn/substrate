package emulator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// controlPlaneEventService is the [Event.Service] a recorded control-plane write
// carries, and the marker [ReplayEngine.replayEvent] recognizes one by.
//
// It is not a service name substrate can derive from a real request: serviceFromRequest
// takes a service either from the parsed AWS endpoint or from an X-Amz-Target prefix,
// and no AWS service is called this — so an event bearing it can only have been
// recorded by [Server.recordControlPlaneWrites].
const controlPlaneEventService = "substrate-control"

// replayingControlPlaneKey marks the context of a control-plane request the replay
// engine synthesized, so [Server.recordControlPlaneWrites] does not record it a
// second time.
//
// A replay writes no events — that is [ReplayEngine.replayEvent]'s contract — and a
// replayed seed is re-issued through the very middleware that recorded it. Without
// this marker, replaying a stream would append a copy of every seed it contains, and
// with a file-backed store the next replay of the same stream would read a longer
// stream. The [ReplayEngine] used by the CLI is additionally given a disabled store
// for its control plane, so the property holds twice over; the marker is what makes it
// hold for a replay driven against a *live* server, whose store is enabled.
type replayingControlPlaneKey struct{}

// isControlPlaneEvent reports whether event records a control-plane write rather than
// an AWS request.
func isControlPlaneEvent(event *Event) bool {
	return event != nil && event.Service == controlPlaneEventService
}

// recordControlPlaneWrites records each successful write through a control-plane
// endpoint as an [Event], so replaying a stream recorded under a seed replays it
// under the same seed (#1140).
//
// # Why a seed has to be an event
//
// Every seedable outcome in substrate is written through a control-plane endpoint
// rather than through an AWS request, and [Server.handleAWSRequest] is the only place
// that recorded anything — so a seed never entered the event stream. A replay begins
// with [ReplayEngine.resetState], which wipes the whole [StateManager], and seeds live
// in the state manager (`ec2-snap-ctrl`, `sqs-ctrl`, and 19 other `-ctrl` namespaces).
// So a stream recorded under a seed replayed as the *unseeded* sequence: four
// `pending` snapshot observations followed by `completed` came back as five
// `completed`s, and nothing failed, because every request was re-executed and every
// one succeeded. CLAUDE.md promises "same inputs (including seeds) → same outputs";
// the parenthesis was the part that did not hold.
//
// # Why this rather than exempting the seed namespaces from the reset
//
// #1140 offered three options. Preserving the `-ctrl` namespaces across resetState is
// the smaller change and was rejected on two counts.
//
// The first is fatal to it: two namespaces spend their seed **in place**. consumeQueueMiss
// (sqs_control.go) and the three S3 conditional-conflict counters decrement the stored
// record and write it back, so the as-seeded count is retained nowhere. A preserved
// namespace would hand the replay a *partially spent* seed, which answers neither the
// recording's sequence nor the unseeded one. #1140's fourth criterion — that the
// `observed:` observation counters must reset rather than be preserved — is the same
// problem seen from the other side, and it is why the distinction would have had to be
// drawn inside a namespace rather than at its boundary.
//
// The second is that a seed written *between* two recorded observations cannot be
// reproduced by preserving state at all: position is the information, and only an event
// carries it. Recording the write gets both for free — resetState keeps wiping
// everything, so the counters restart from zero, and the seed is re-applied exactly
// where the recording applied it.
//
// # Why a middleware over a group, rather than a list of endpoints
//
// The middleware is attached with [chi.Router.Group] around the seed registrations in
// [Server.buildRouter], so the set of recorded endpoints is the set registered inside
// that block: a seed endpoint added there is recorded with no further edit, which is the
// failure mode #1140 asked for ("a list that must not drift as seeds are added"). Only
// routes in the group reach this code, so the AWS catch-all never has its body buffered
// here — which matters, because a path-prefix test could not tell `PUT /v1/{key}` against
// a bucket named `v1` from a control-plane write.
//
// Endpoints deliberately left outside the group are the ones a replay must *not*
// re-issue: `/v1/state/reset` (the replay owns the reset), `/v1/control/time` and
// `/v1/control/scale` (replayEvent owns the clock, and re-applying a recorded SetTime
// would fight it), `/v1/fault/rules` (rules live on the [FaultController], which
// survives resetState and is rewound rather than cleared, so re-arming them would double
// them), and the ones that write nothing a replayed AWS observation reads: `/v1/s3/presign`
// mints a URL, `/v1/pricing/refresh` reloads a price table, and the pricing discount and
// credit endpoints write to the [CostController], which resetState does not touch.
//
// # What is recorded
//
// The request target including its query string, the body, the headers, and the status.
// The query string is not optional: most `DELETE` seed endpoints key on one
// (`?snapshotId=`, `?roleName=`, `?accountId=&region=`), so an event carrying the path
// alone would replay as a clear-everything. Only a 2xx is recorded — a refused seed
// changed nothing, so there is nothing for a replay to re-apply.
//
// There is no filter on the method, because every registration in the group is a POST or
// a DELETE: a read belongs outside it, next to GET /v1/pricing and GET /v1/fault/rules.
// One registered here would be recorded and re-issued during a replay, which is wasted
// work rather than a wrong answer, since a read writes nothing for the replay to double.
func (s *Server) recordControlPlaneWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.store == nil || r.Context().Value(replayingControlPlaneKey{}) != nil {
			next.ServeHTTP(w, r)
			return
		}

		// Buffered and handed back, because the handler below is the one that decodes it.
		// A read error is passed through rather than answered here: the handler fails on
		// the same truncated body and owns the reply the caller gets, and an event is not
		// recorded for a request that did not take effect.
		body, readErr := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		var respBody bytes.Buffer
		ww.Tee(&respBody)

		start := time.Now()
		next.ServeHTTP(ww, r)
		duration := time.Since(start)

		// A handler that answered nothing at all reports 0 here and is not recorded,
		// alongside the refusals: it took no effect worth re-applying either.
		status := ww.Status()
		if readErr != nil || status < http.StatusOK || status >= http.StatusMultipleChoices {
			return
		}

		// RoutePattern is read after the handler ran, which is the only point chi
		// guarantees it is populated. It is the endpoint's pattern rather than the
		// request's path, so [Event.Operation] stays a bounded set of names even for the
		// endpoints that carry an identifier in the path.
		pattern := chi.RouteContext(r.Context()).RoutePattern()

		req := &AWSRequest{
			Service:    controlPlaneEventService,
			Operation:  r.Method + " " + pattern,
			HTTPMethod: r.Method,
			Path:       r.URL.RequestURI(),
			Headers:    flattenHeader(r.Header),
			Body:       body,
		}
		reqCtx := &RequestContext{
			RequestID: generateRequestID(),
			AccountID: s.config.Account.Default,
			Timestamp: s.tc.Now(),
		}
		resp := &AWSResponse{StatusCode: status, Body: respBody.Bytes()}

		if err := s.store.RecordRequest(r.Context(), reqCtx, req, resp, duration, 0, nil); err != nil {
			s.logger.Warn("failed to record control-plane write",
				"operation", req.Operation, "err", err)
		}
	})
}

// flattenHeader collapses an [http.Header] to the single-valued map an [AWSRequest]
// carries, keeping the first value of each name — the same shape and the same loss
// ParseAWSRequest accepts for an AWS request.
func flattenHeader(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for name, values := range h {
		if len(values) > 0 {
			out[name] = values[0]
		}
	}
	return out
}

// controlPlaneReplayHost is the authority the replay engine gives the request it
// rebuilds.
//
// A recorded event carries the request target and not the host it was sent to, because
// the control plane is reached over whatever address the emulator happens to be bound
// to and that address is not part of what a seed says. The handler is invoked directly
// rather than over a socket, so only the path and query are read; `.invalid` is the
// reserved TLD for a name that must never resolve (RFC 2606).
const controlPlaneReplayHost = "http://control-plane.invalid"

// replayControlPlaneEvent re-applies a recorded control-plane write by re-issuing it
// against the handler [WithControlPlaneHandler] supplied.
//
// Without a handler the event is skipped rather than failed. An engine constructed
// without one is in exactly the position every engine was in before #1140 — the seed
// is not re-applied and the following observations answer the unseeded sequence — and
// reporting that as a skip says so in [ReplayResults.SkippedEvents], which is the same
// treatment an event recorded without a body gets and for the same reason: nothing was
// verified, and a caller reading only the error count must not be told otherwise.
func (r *ReplayEngine) replayControlPlaneEvent(
	ctx context.Context,
	event *Event,
	replay *ActiveReplay,
) (bool, error) {
	if r.controlPlane == nil {
		replay.Results.SkippedEvents++
		return false, nil
	}

	body := event.Request.Body
	req, err := http.NewRequestWithContext(
		context.WithValue(ctx, replayingControlPlaneKey{}, true),
		requestMethod(event.Request),
		controlPlaneReplayHost+event.Request.Path,
		bytes.NewReader(body),
	)
	if err != nil {
		r.recordErrorDifference(replay, event, err)
		return true, fmt.Errorf("rebuild control-plane request %q: %w", event.Operation, err)
	}
	for name, value := range event.Request.Headers {
		req.Header.Set(name, value)
	}
	req.ContentLength = int64(len(body))

	rec := &controlPlaneRecorder{status: http.StatusOK}
	r.controlPlane.ServeHTTP(rec, req)

	// Only the status is compared; see [controlPlaneRecorder] for why. A recorded event
	// always carries a 2xx, so a disagreement here means the replay refused a seed the
	// recording accepted, which is reported rather than returned as an error: the event
	// was re-executed, and the divergence belongs in Differences alongside the AWS
	// divergences it is about to cause.
	if event.Response != nil && rec.status != event.Response.StatusCode {
		replay.Results.Differences = append(replay.Results.Differences, &EventDifference{
			EventID:      event.ID,
			Sequence:     event.Sequence,
			Operation:    event.Operation,
			Field:        "status_code",
			Expected:     event.Response.StatusCode,
			Actual:       rec.status,
			Significance: "critical",
		})
	}
	return true, nil
}

// controlPlaneRecorder is the [http.ResponseWriter] a replayed control-plane write is
// answered into.
//
// The body is discarded rather than compared. A control-plane response is substrate's
// own and carries no AWS contract, so a difference in one is not a divergence a
// consumer could observe; what a replay is checking is the AWS request that reads the
// seed afterwards. The status is kept, because a seed that was accepted in the
// recording and refused in the replay would otherwise leave the following observations
// diverging with nothing to point at.
type controlPlaneRecorder struct {
	header http.Header
	status int
	wrote  bool
}

// Header returns the header map the handler writes into, which is then discarded.
func (c *controlPlaneRecorder) Header() http.Header {
	if c.header == nil {
		c.header = make(http.Header)
	}
	return c.header
}

// Write discards the body and reports it written, implying a 200 as [http.ResponseWriter]
// requires of a handler that writes without setting a status.
func (c *controlPlaneRecorder) Write(p []byte) (int, error) {
	c.WriteHeader(http.StatusOK)
	return len(p), nil
}

// WriteHeader keeps the first status written, which is the one the replay compares.
func (c *controlPlaneRecorder) WriteHeader(status int) {
	if c.wrote {
		return
	}
	c.status = status
	c.wrote = true
}
