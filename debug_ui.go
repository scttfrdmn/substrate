package substrate

import (
	"context"
	_ "embed"
	"encoding/json"
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
type debugEventSummary struct {
	Sequence   int64   `json:"seq"`
	ID         string  `json:"id"`
	Timestamp  string  `json:"timestamp"`
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	StatusCode int     `json:"status_code,omitempty"`
	Cost       float64 `json:"cost,omitempty"`
	DurationMS int64   `json:"duration_ms"`
	Error      string  `json:"error,omitempty"`
	StreamID   string  `json:"stream_id,omitempty"`
	AccountID  string  `json:"account_id,omitempty"`
	Region     string  `json:"region,omitempty"`
}

// handleDebugEvents returns a filtered list of events from the store.
// Query params: ?service=, ?stream=, ?limit= (default 500), ?after= (min sequence).
func (s *Server) handleDebugEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := EventFilter{
		Service:  q.Get("service"),
		StreamID: q.Get("stream"),
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

	// Trim to limit.
	if len(events) > limit {
		events = events[len(events)-limit:]
	}

	summaries := make([]debugEventSummary, 0, len(events))
	for _, ev := range events {
		statusCode := 0
		if ev.Response != nil {
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
			Error:      ev.Error,
			StreamID:   ev.StreamID,
			AccountID:  ev.AccountID,
			Region:     ev.Region,
		})
	}

	result := map[string]interface{}{
		"events": summaries,
		"count":  len(summaries),
	}
	writeJSONDebug(w, s.logger, result)
}

// handleDebugStateAt returns the emulator state snapshot at sequence number {seq}.
func (s *Server) handleDebugStateAt(w http.ResponseWriter, r *http.Request) {
	seqStr := chi.URLParam(r, "seq")
	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid sequence"}`, http.StatusBadRequest)
		return
	}

	stateJSON, err := s.stateAtSequence(r.Context(), seq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(stateJSON) // nosemgrep
}

// handleDebugStateDiff returns a JSON diff of state between two sequence points.
// Query params: ?from=, ?to= (sequence numbers).
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

	fromJSON, err := s.stateAtSequence(r.Context(), fromSeq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	toJSON, err := s.stateAtSequence(r.Context(), toSeq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	var fromState, toState map[string]interface{}
	if err := json.Unmarshal(fromJSON, &fromState); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(toJSON, &toState); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	added := make(map[string]interface{})
	removed := make(map[string]interface{})
	changed := make(map[string]interface{})

	// Keys in toState not in fromState → added.
	for k, v := range toState {
		if _, ok := fromState[k]; !ok {
			added[k] = v
		}
	}
	// Keys in fromState not in toState → removed; differing values → changed.
	for k, fv := range fromState {
		tv, ok := toState[k]
		if !ok {
			removed[k] = fv
			continue
		}
		fJSON, _ := json.Marshal(fv)
		tJSON, _ := json.Marshal(tv)
		if string(fJSON) != string(tJSON) {
			changed[k] = map[string]interface{}{
				"before": fv,
				"after":  tv,
			}
		}
	}

	result := map[string]interface{}{
		"added":   added,
		"removed": removed,
		"changed": changed,
	}
	writeJSONDebug(w, s.logger, result)
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

	src, err := GenerateTestFixture(events, packageName, testName)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(src)) // nosemgrep
}

// stateAtSequence replays all events up to and including upToSeq into a fresh
// in-memory state and returns the resulting state snapshot as JSON.
// Events without request bodies are silently skipped.
func (s *Server) stateAtSequence(ctx context.Context, upToSeq int64) ([]byte, error) {
	freshState := NewMemoryStateManager()
	freshRegistry := NewPluginRegistry()
	freshTC := NewTimeController(time.Time{})
	if err := RegisterDefaultPlugins(ctx, freshRegistry, freshState, freshTC, s.logger, nil, nil); err != nil {
		return nil, fmt.Errorf("stateAtSequence register plugins: %w", err)
	}

	events, err := s.store.GetEvents(ctx, EventFilter{MaxSequence: upToSeq})
	if err != nil {
		return nil, fmt.Errorf("stateAtSequence get events: %w", err)
	}

	for _, ev := range events {
		if ev.Request == nil {
			continue
		}
		reqCtx := &RequestContext{AccountID: ev.AccountID, Region: ev.Region}
		_, _ = freshRegistry.RouteRequest(reqCtx, ev.Request)
	}

	return freshState.Snapshot(ctx)
}

// timeControlResponse is the JSON shape returned by /v1/control/time.
type timeControlResponse struct {
	SimulatedTime string  `json:"simulated_time"`
	Scale         float64 `json:"scale"`
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
	})
}

// handleSetTime sets the simulated clock to the time specified in the JSON body.
// The request body must be {"time": "<RFC3339>"}.  Responds with the same shape
// as GET /v1/control/time.
func (s *Server) handleSetTime(w http.ResponseWriter, r *http.Request) {
	if s.tc == nil {
		http.Error(w, `{"error":"time controller not configured"}`, http.StatusNotImplemented)
		return
	}
	var body struct {
		Time string `json:"time"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	t, err := time.Parse(time.RFC3339Nano, body.Time)
	if err != nil {
		t, err = time.Parse(time.RFC3339, body.Time)
	}
	if err != nil {
		http.Error(w, `{"error":"time must be RFC3339 format"}`, http.StatusBadRequest)
		return
	}
	s.tc.SetTime(t)
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
