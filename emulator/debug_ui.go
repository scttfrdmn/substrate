package emulator

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

//go:embed ui.html
var debugUIHTML []byte

// handleDebugUI serves the embedded browser-based debug UI.
func (s *Server) handleDebugUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(debugUIHTML) // nosemgrep
}

// debugEventSummary is a trimmed Event representation returned by /v1/debug/events.
// Request and response bodies are omitted to keep the payload small.
//
// StatusCode and ErrorCode are both recorded whether or not bodies are, which is what
// lets a caller running the default configuration tell a refused request from an
// accepted one (#1237). StatusCode used to be read off the recorded response, so it was
// absent in exactly that configuration — and a plugin that reports a refusal as a
// response rather than as an error, which S3 does, left no trace of the refusal at all.
//
// Method and path are deliberately not members: they live on the recorded request, which
// `event_store.include_bodies` governs because a body is what makes an event large, so a
// `path` member would be empty whenever it is off. Service
// plus operation identifies an operation in every configuration.
type debugEventSummary struct {
	Sequence   int64   `json:"seq"`
	ID         string  `json:"id"`
	Timestamp  string  `json:"timestamp"`
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	StatusCode int     `json:"status_code,omitempty"`
	Cost       float64 `json:"cost,omitempty"`
	DurationMS int64   `json:"duration_ms"`
	ErrorCode  string  `json:"error_code,omitempty"`
	Error      string  `json:"error,omitempty"`
	StreamID   string  `json:"stream_id,omitempty"`
	AccountID  string  `json:"account_id,omitempty"`
	Region     string  `json:"region,omitempty"`
}

// handleDebugEvents returns a filtered list of events from the store.
//
// Query params: ?service=, ?operation= (or ?op=), ?stream=, ?limit= (default 500),
// ?after= (min sequence).
//
// **This is the request log an out-of-process consumer counts with**, which is what
// #1237 asked for and what shaped the two changes here. A consumer that runs
// `substrate server` as a separate process — objectfs mounts a bucket under a real
// `sudo mount -t objectfs`, so the mount cannot reach an in-process recorder — has no
// access to [EventStore.GetEvents] and had no way to ask this endpoint for one
// operation. Its alternative was to diff a state listing, which cannot see a PUT that
// rewrote identical bytes and cannot tell "no write was attempted" from "a write was
// attempted and refused".
//
// `operation` is honored because [EventFilter] has carried the member and the store
// has carried a `byOperation` index all along, and nothing read either from HTTP.
// `op` is accepted as an alias because that is the spelling #1237 proposed: silently
// ignoring one spelling of the filter would answer a "count my PutObjects" request
// with every event in the log, which is the over-count this endpoint exists to rule
// out. `operation` is the canonical name because the member it fills is named
// `operation` in every entry.
//
// `total` is the number of events matching the filter **before** `limit` is applied,
// and `truncated` says whether any were dropped. `count` keeps its meaning — the
// length of `events` — so the existing UI and `substrate inspect` are unaffected. The
// distinction is the whole point for a counting caller: the limit trims the *oldest*
// matching events, so before this a run that recorded more than the limit answered a
// count that was silently short, and a write early in the run became invisible — the
// same false negative the state diff gives. A caller asserting a count reads `total`;
// a caller displaying a tail reads `events`. A `limit` that is not a positive number
// is ignored in favor of the default, and `truncated` is what makes that visible.
func (s *Server) handleDebugEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	operation := q.Get("operation")
	if operation == "" {
		operation = q.Get("op")
	}

	filter := EventFilter{
		Service:   q.Get("service"),
		StreamID:  q.Get("stream"),
		Operation: operation,
	}

	if after := q.Get("after"); after != "" {
		if seq, err := strconv.ParseInt(after, 10, 64); err == nil {
			filter.MinSequence = seq + 1
		}
	}

	limit := 500
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	events, err := s.store.GetEvents(r.Context(), filter)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Counted before the trim, because the trim is what makes a count short.
	total := len(events)
	if len(events) > limit {
		events = events[len(events)-limit:]
	}

	summaries := make([]debugEventSummary, 0, len(events))
	for _, ev := range events {
		// The event's own status, not the recorded response's: a stream recorded before
		// #1237 carries neither, and one recorded without bodies carries only this.
		statusCode := ev.StatusCode
		if statusCode == 0 && ev.Response != nil {
			statusCode = ev.Response.StatusCode
		}
		summaries = append(summaries, debugEventSummary{
			Sequence:   ev.Sequence,
			ID:         ev.ID,
			Timestamp:  ev.Timestamp.UTC().Format(time.RFC3339Nano),
			Service:    ev.Service,
			Operation:  ev.Operation,
			StatusCode: statusCode,
			Cost:       ev.Cost,
			DurationMS: ev.Duration.Milliseconds(),
			ErrorCode:  ev.ErrorCode,
			Error:      ev.Error,
			StreamID:   ev.StreamID,
			AccountID:  ev.AccountID,
			Region:     ev.Region,
		})
	}

	result := map[string]interface{}{
		"events":    summaries,
		"count":     len(summaries),
		"total":     total,
		"truncated": total > len(summaries),
	}
	writeJSONDebug(w, s.logger, result)
}

// handleDebugStateAt returns the state stream ?stream= (default "default") had before
// the event with sequence {seq} ran, as a [StateAt]. Sequence 0 is the state before any
// event; a sequence past the last event is the final state.
//
// The state is rebuilt by replaying the stream into a [ReplaySandbox], with each event's
// recorded request ID, clock and principal, so it names the identifiers the live run
// minted. Where the replay diverged, the response lists it in `differences`. A stream
// with no events is a 404.
func (s *Server) handleDebugStateAt(w http.ResponseWriter, r *http.Request) {
	seq, err := strconv.ParseInt(chi.URLParam(r, "seq"), 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid sequence"}`, http.StatusBadRequest)
		return
	}
	sandbox, err := s.replaySandbox(r.Context())
	if err != nil {
		writeJSONErrorDebug(w, http.StatusInternalServerError, "%s", err.Error())
		return
	}
	at, err := s.stateAt(r.Context(), sandbox, debugStream(r), seq)
	if err != nil {
		writeJSONErrorDebug(w, stateAtErrorStatus(err), "%s", err.Error())
		return
	}
	writeJSONDebug(w, s.logger, at)
}

// handleDebugStateDiff returns the state keys that were added, removed and changed
// between two sequence points of stream ?stream= (default "default"). Query params:
// ?from=, ?to= (sequence numbers, with the meaning [Server.handleDebugStateAt] gives
// them). A key is "namespace/key"; a value is the base64 of the stored bytes. The
// differences each replay found are returned beside the diff.
func (s *Server) handleDebugStateDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	fromSeq, err := strconv.ParseInt(q.Get("from"), 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid from parameter"}`, http.StatusBadRequest)
		return
	}
	toSeq, err := strconv.ParseInt(q.Get("to"), 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid to parameter"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	stream := debugStream(r)
	sandbox, err := s.replaySandbox(ctx)
	if err != nil {
		writeJSONErrorDebug(w, http.StatusInternalServerError, "%s", err.Error())
		return
	}
	from, err := s.stateAt(ctx, sandbox, stream, fromSeq)
	if err != nil {
		writeJSONErrorDebug(w, stateAtErrorStatus(err), "%s", err.Error())
		return
	}
	to, err := s.stateAt(ctx, sandbox, stream, toSeq)
	if err != nil {
		writeJSONErrorDebug(w, stateAtErrorStatus(err), "%s", err.Error())
		return
	}

	fromState, err := flattenStateSnapshot(from.State)
	if err != nil {
		writeJSONErrorDebug(w, http.StatusInternalServerError, "%s", err.Error())
		return
	}
	toState, err := flattenStateSnapshot(to.State)
	if err != nil {
		writeJSONErrorDebug(w, http.StatusInternalServerError, "%s", err.Error())
		return
	}

	added := make(map[string]string)
	removed := make(map[string]string)
	changed := make(map[string]map[string]string)
	for k, v := range toState {
		if _, ok := fromState[k]; !ok {
			added[k] = v
		}
	}
	for k, fv := range fromState {
		tv, ok := toState[k]
		switch {
		case !ok:
			removed[k] = fv
		case tv != fv:
			changed[k] = map[string]string{"before": fv, "after": tv}
		}
	}

	differences := from.Differences
	for _, d := range to.Differences {
		if d.Sequence >= fromSeq {
			differences = append(differences, d)
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{
		"stream":      stream,
		"added":       added,
		"removed":     removed,
		"changed":     changed,
		"differences": differences,
	})
}

// debugStream returns the ?stream= parameter, or [DefaultStreamID].
func debugStream(r *http.Request) string {
	if stream := r.URL.Query().Get("stream"); stream != "" {
		return stream
	}
	return DefaultStreamID
}

// errNoStreamEvents reports a state-at request for a stream with no events.
var errNoStreamEvents = errors.New("no events in stream")

// stateAtErrorStatus maps a [Server.stateAt] error to an HTTP status.
func stateAtErrorStatus(err error) int {
	if errors.Is(err, errNoStreamEvents) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

// replaySandbox builds a sandbox for this server's configuration and armed fault rules.
func (s *Server) replaySandbox(ctx context.Context) (*ReplaySandbox, error) {
	sandbox, err := NewReplaySandbox(ctx, &s.config, s.store, s.logger, withSandboxGatesOf(s.opts))
	if err != nil {
		return nil, fmt.Errorf("build replay sandbox: %w", err)
	}
	return sandbox, nil
}

// stateAt is [ReplaySandbox.StateAt] with an empty stream reported as errNoStreamEvents.
func (s *Server) stateAt(ctx context.Context, sandbox *ReplaySandbox, stream string, seq int64) (*StateAt, error) {
	events, err := s.store.GetStream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("get stream %q: %w", stream, err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("%w %q", errNoStreamEvents, stream)
	}
	return sandbox.StateAt(ctx, stream, seq)
}

// flattenStateSnapshot turns a [MemoryStateManager] snapshot into one map keyed by
// "namespace/key", with each value base64-encoded as the snapshot stores it.
func flattenStateSnapshot(raw []byte) (map[string]string, error) {
	var nested map[string]map[string]string
	if err := json.Unmarshal(raw, &nested); err != nil {
		return nil, fmt.Errorf("decode state snapshot: %w", err)
	}
	out := make(map[string]string)
	for ns, keys := range nested {
		for k, v := range keys {
			out[ns+"/"+k] = v
		}
	}
	return out, nil
}

// handleDebugCosts returns aggregated cost information.
// Query params: ?account= (default all), ?stream= (optional stream filter).
func (s *Server) handleDebugCosts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	accountID := q.Get("account")
	streamID := q.Get("stream")

	ctx := r.Context()

	if streamID != "" {
		// Gather unique account IDs from stream events and aggregate costs.
		events, err := s.store.GetEvents(ctx, EventFilter{StreamID: streamID})
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		seen := make(map[string]bool)
		for _, ev := range events {
			seen[ev.AccountID] = true
		}
		combined := &CostSummary{
			AccountID:   streamID,
			ByService:   make(map[string]float64),
			ByOperation: make(map[string]float64),
		}
		for acct := range seen {
			summary, err := s.store.GetCostSummary(ctx, acct, time.Time{}, time.Time{})
			if err != nil {
				continue
			}
			combined.TotalCost += summary.TotalCost
			combined.RequestCount += summary.RequestCount
			for k, v := range summary.ByService {
				combined.ByService[k] += v
			}
			for k, v := range summary.ByOperation {
				combined.ByOperation[k] += v
			}
		}
		writeJSONDebug(w, s.logger, combined)
		return
	}

	summary, err := s.store.GetCostSummary(ctx, accountID, time.Time{}, time.Time{})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, summary)
}

// handleDebugExport generates and returns a Go test fixture file for the given stream.
// Query params: ?stream= (required), ?package= (default "substrate_test"), ?test= (default "TestGeneratedReplay").
func (s *Server) handleDebugExport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	streamID := q.Get("stream")
	if streamID == "" {
		http.Error(w, `{"error":"stream parameter is required"}`, http.StatusBadRequest)
		return
	}
	packageName := q.Get("package")
	if packageName == "" {
		packageName = "substrate_test"
	}
	testName := q.Get("test")
	if testName == "" {
		testName = "TestGeneratedReplay"
	}

	events, err := s.store.GetEvents(r.Context(), EventFilter{StreamID: streamID})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// A stream with an unreplayable event, or a package or test name that is not a Go
	// identifier, is refused as unprocessable rather than exported as a fixture that
	// cannot pass.
	//
	// A fixture replays into a default StartTestServer, which arms no fault rule, so a
	// stream recorded under armed rules would replay its injected failures as
	// successes. A default-VPC switch has a test-server option, and is carried.
	if s.opts.Fault.hasArmedRules() {
		writeJSONErrorDebug(w, http.StatusUnprocessableEntity,
			"the server has fault rules armed, which a fixture's test server does not reproduce; clear them with DELETE /v1/fault/rules and record again")
		return
	}
	var serverOptions []string
	if !s.config.EC2.DefaultVPC {
		serverOptions = append(serverOptions, "emulator.WithoutDefaultVPC()")
	}
	if s.config.CloudFormation.Lenient {
		serverOptions = append(serverOptions, "emulator.WithLenientCloudFormation()")
	}
	src, err := generateTestFixture(events, packageName, testName, serverOptions)
	if err != nil {
		writeJSONErrorDebug(w, http.StatusUnprocessableEntity, "%s", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(src)) // nosemgrep
}

// timeControlResponse is the JSON shape returned by /v1/control/time.
type timeControlResponse struct {
	SimulatedTime string  `json:"simulated_time"`
	Scale         float64 `json:"scale"`
	// Frozen reports whether the clock is stopped; see [TimeController.Freeze].
	Frozen bool `json:"frozen"`
}

// handleGetTime returns the current simulated time and scale factor.
func (s *Server) handleGetTime(w http.ResponseWriter, _ *http.Request) {
	if s.tc == nil {
		http.Error(w, `{"error":"time controller not configured"}`, http.StatusNotImplemented)
		return
	}
	writeJSONDebug(w, s.logger, timeControlResponse{
		SimulatedTime: s.tc.Now().UTC().Format(time.RFC3339Nano),
		Scale:         s.tc.Scale(),
		Frozen:        s.tc.Frozen(),
	})
}

// handleSetTime sets the simulated clock from the JSON body
// {"time": "<RFC3339>", "frozen": <bool>}, and responds with the same shape as
// GET /v1/control/time.
//
// Either field may be omitted, but not both. "frozen": true stops the clock — at "time"
// exactly when one is given, freezing before setting for the reason [TimeController.Freeze]
// gives — and "frozen": false resumes it, which is the HTTP form of
// [TestServer.FreezeTimeAt] and [TestServer.UnfreezeTime] (#1598). With "frozen" omitted the
// frozen state is left as it is, so a frozen clock moved by "time" alone stays frozen.
//
// "frozen": false also unpins a clock POST /v1/control/run pinned, so GET /v1/control/run
// never reports an epoch for a running clock and a later reset does not freeze it again.
// "frozen": true freezes without pinning: it names no instant for a reset to return to. Moving
// a pinned clock with "time" alone leaves it pinned, and a reset returns it to the epoch.
func (s *Server) handleSetTime(w http.ResponseWriter, r *http.Request) {
	if s.tc == nil {
		http.Error(w, `{"error":"time controller not configured"}`, http.StatusNotImplemented)
		return
	}
	var body struct {
		Time   string `json:"time"`
		Frozen *bool  `json:"frozen"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if body.Time == "" && body.Frozen == nil {
		http.Error(w, `{"error":"time or frozen is required"}`, http.StatusBadRequest)
		return
	}
	var t time.Time
	if body.Time != "" {
		var err error
		t, err = time.Parse(time.RFC3339Nano, body.Time)
		if err != nil {
			t, err = time.Parse(time.RFC3339, body.Time)
		}
		if err != nil {
			http.Error(w, `{"error":"time must be RFC3339 format"}`, http.StatusBadRequest)
			return
		}
	}
	if body.Frozen != nil && *body.Frozen {
		s.tc.Freeze()
	}
	if body.Time != "" {
		s.tc.SetTime(t)
	}
	if body.Frozen != nil && !*body.Frozen {
		s.unpinClock()
	}
	s.handleGetTime(w, r)
}

// handleSetScale sets the time acceleration factor from the JSON body.
// The request body must be {"scale": <positive float>}.  Responds with the same
// shape as GET /v1/control/time.
func (s *Server) handleSetScale(w http.ResponseWriter, r *http.Request) {
	if s.tc == nil {
		http.Error(w, `{"error":"time controller not configured"}`, http.StatusNotImplemented)
		return
	}
	var body struct {
		Scale float64 `json:"scale"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if body.Scale <= 0 {
		http.Error(w, `{"error":"scale must be > 0"}`, http.StatusBadRequest)
		return
	}
	s.tc.SetScale(body.Scale)
	s.handleGetTime(w, r)
}

// writeJSONErrorDebug writes a control-plane error body, formatting message through
// fmt.Sprintf and JSON-encoding the result.
//
// It exists because the obvious inline form — fmt.Sprintf(`{"error":"… %q …"}`, v) —
// produces invalid JSON whenever an interpolated value lands inside the already-quoted
// error string: %q adds its own quotes, closing the JSON string early. A caller that
// decodes the body to report why its seed was refused gets a parse error instead of
// the reason, which is the one moment it most needs to read the message.
func writeJSONErrorDebug(w http.ResponseWriter, status int, message string, args ...any) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	body, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		http.Error(w, `{"error":"could not encode error message"}`, http.StatusInternalServerError)
		return
	}
	http.Error(w, string(body), status)
}

// writeJSONDebug marshals v to JSON and writes it to w.
func writeJSONDebug(w http.ResponseWriter, logger Logger, v interface{}) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil { // nosemgrep
		logger.Warn("failed to write debug response", "err", err)
	}
}
