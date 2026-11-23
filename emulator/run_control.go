package emulator

import (
	"encoding/json"
	"net/http"
	"time"
)

// runControlResponse is the JSON shape GET and POST /v1/control/run answer.
type runControlResponse struct {
	// Seed is the request-ID seed; 0 means unseeded.
	Seed int64 `json:"seed"`
	// Epoch is the instant the clock is pinned at, empty when it is not pinned.
	Epoch string `json:"epoch,omitempty"`
	// Drawn is how many request IDs the seeded source has assigned since it was seeded or
	// last rewound.
	Drawn uint64 `json:"drawn"`
}

// runControlRequest is the body POST /v1/control/run accepts. Both fields are optional;
// an absent one leaves that half of the run as it is.
type runControlRequest struct {
	// Seed reseeds the request IDs and rewinds their counter; 0 unseeds them.
	Seed *int64 `json:"seed"`
	// Epoch pins the clock at an RFC 3339 instant; the empty string unpins it.
	Epoch *string `json:"epoch"`
}

// handleGetRun reports the deterministic-run settings: the request-ID seed and the epoch
// the clock is pinned at, if any (#1598).
func (s *Server) handleGetRun(w http.ResponseWriter, _ *http.Request) {
	resp := runControlResponse{Seed: s.ids.Seed(), Drawn: s.ids.Drawn()}
	if epoch, ok := s.pinnedEpoch(); ok {
		resp.Epoch = epoch.UTC().Format(time.RFC3339Nano)
	}
	writeJSONDebug(w, s.logger, resp)
}

// handleSetRun changes the deterministic-run settings over HTTP, for a harness that did
// not start the server and cannot set run.seed or run.epoch in its config — the
// pytest-substrate fixtures, for one.
//
// A seed reseeds and rewinds the request IDs, so the next request is the first of the new
// sequence. An epoch freezes the clock at it, and the empty string unpins the clock and
// resumes it from where it stands. The body is validated whole before anything changes, so
// a refused request leaves the run as it was. It answers the shape GET does.
func (s *Server) handleSetRun(w http.ResponseWriter, r *http.Request) {
	var body runControlRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErrorDebug(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	var epoch time.Time
	pin := false
	if body.Epoch != nil && *body.Epoch != "" {
		parsed, ok, err := RunCfg{Epoch: *body.Epoch}.ParseEpoch()
		if err != nil {
			writeJSONErrorDebug(w, http.StatusBadRequest, "epoch must be an RFC 3339 time")
			return
		}
		epoch, pin = parsed, ok
	}
	if body.Seed != nil {
		s.ids.SetSeed(*body.Seed)
	}
	switch {
	case pin:
		s.pinClock(epoch)
	case body.Epoch != nil:
		s.unpinClock()
	}
	s.handleGetRun(w, r)
}
