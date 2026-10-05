package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// A Transfer Family server's seeded lifecycle (#1196), on the shared [progression], and the
// StartServer and StopServer operations that move it.
//
// A server used to be born ONLINE and to have no route to any other state: StartServer and
// StopServer were unrouted, so OFFLINE, STARTING, STOPPING, START_FAILED and STOP_FAILED were
// unreachable by any input and a consumer's wait-until-online loop exited on its first describe.
//
// # What the pages publish, and what is substrate's reading
//
// DescribedServer's State is OFFLINE | ONLINE | STARTING | STOPPING | START_FAILED | STOP_FAILED:
// STARTING and STOPPING are "an intermediate state", and START_FAILED and STOP_FAILED "can indicate
// an error condition". API_StopServer "changes the state … from ONLINE to OFFLINE"; API_StartServer
// "from OFFLINE to ONLINE", and "has no impact on a server that is already ONLINE". Both pages say
// the response is "an HTTP 200 response with an empty HTTP body" while their examples show
// {"ServerId": …}; substrate answers {} for the reason [TransferPlugin.deleteServer] gives the
// deletes' identical inconsistency (#1206).
//
// CreateServer's response carries no state, and no page says what a new server reports while it
// comes up. That a seeded new server reports STARTING before ONLINE is therefore substrate's
// reading, recorded as unverified; unseeded, it is ONLINE from its first describe, as before.
// No page publishes a deleting state either, so DeleteServer removes the server at once, as it
// always has.
//
// # One seed, every transition
//
// A server's record holds the state it is moving to — ONLINE after a create or a start, OFFLINE
// after a stop — and the seed's countdown restarts on each start and stop, the EC2 instance model
// (#514). While it runs a describe or a list reports STARTING when the server is moving to ONLINE
// and STOPPING when it is moving to OFFLINE (or the seed's state, when it names one). Once it is
// spent the server reports the state it moved to, or the seed's finalState when that is the failure
// of this transition: START_FAILED for a start, STOP_FAILED for a stop. A START_FAILED or
// STOP_FAILED server can be started or stopped again.

// transferServerStates is DescribedServer's published State list.
var transferServerStates = []string{"OFFLINE", "ONLINE", "STARTING", "STOPPING", "START_FAILED", "STOP_FAILED"}

// transferServerProgressions is Transfer's server-state countdown, seeded through
// POST /v1/transfer/server-status.
var transferServerProgressions = newProgression[transferServerStatusSeed]("transfer-server-ctrl", "serverId", "pendingObservations")

// transferServerStatusSeed is the body of POST /v1/transfer/server-status:
// {"serverId","pendingObservations","state","finalState"}.
type transferServerStatusSeed struct {
	// ServerID names the server the seed governs; empty or "*" governs every server.
	ServerID string `json:"serverId"`
	// PendingObservations is how many observations of a transition report its intermediate state.
	PendingObservations int `json:"pendingObservations"`
	// State overrides the intermediate state (STARTING or STOPPING by default).
	State string `json:"state,omitempty"`
	// FinalState is START_FAILED or STOP_FAILED: the outcome a start or a stop settles in when it is
	// that transition's failure. Any other transition settles in the state it moved to.
	FinalState string `json:"finalState,omitempty"`
}

func (s transferServerStatusSeed) progressionID() string        { return s.ServerID }
func (s transferServerStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a state outside DescribedServer's State, and a finalState that is not
// one of the two failures.
func (s transferServerStatusSeed) validateProgression() error {
	if err := progressionStates("server", transferServerStates, s.State, s.FinalState); err != nil {
		return err
	}
	switch s.FinalState {
	case "", "START_FAILED", "STOP_FAILED":
		return nil
	}
	return fmt.Errorf("finalState %q is not a failure: a transition settles in the state it moved to unless it is START_FAILED or STOP_FAILED", s.FinalState)
}

// serverObservation reports the state a server shows now; spend selects an observation over a peek.
func (p *TransferPlugin) serverObservation(s *TransferServer, spend bool) (string, error) {
	var (
		seed *transferServerStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = transferServerProgressions.observe(context.Background(), p.state, &p.seedMu, s.ServerID)
	} else {
		seed, seen, err = transferServerProgressions.peek(context.Background(), p.state, s.ServerID)
	}
	if err != nil {
		return "", fmt.Errorf("transfer server observation: %w", err)
	}
	if seed == nil {
		return s.State, nil
	}
	if seen < seed.PendingObservations {
		if seed.State != "" {
			return seed.State, nil
		}
		if s.State == "OFFLINE" {
			return "STOPPING", nil
		}
		return "STARTING", nil
	}
	switch {
	case seed.FinalState == "START_FAILED" && s.State == "ONLINE":
		return "START_FAILED", nil
	case seed.FinalState == "STOP_FAILED" && s.State == "OFFLINE":
		return "STOP_FAILED", nil
	}
	return s.State, nil
}

// startServer handles StartServer: OFFLINE (or a failed transition) to ONLINE, with no impact on a
// server already ONLINE.
func (p *TransferPlugin) startServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.moveServer(reqCtx, req, "ONLINE")
}

// stopServer handles StopServer: ONLINE (or a failed transition) to OFFLINE.
func (p *TransferPlugin) stopServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.moveServer(reqCtx, req, "OFFLINE")
}

// moveServer is StartServer's and StopServer's shared body: it reads ServerId, refuses an absent or
// malformed one as the other server operations do, and, unless the server already shows target,
// records target as the state the server is moving to and restarts its countdown.
func (p *TransferPlugin) moveServer(reqCtx *RequestContext, req *AWSRequest, target string) (*AWSResponse, error) {
	var input struct {
		ServerID string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, transferInvalidBody()
		}
	}
	server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerID)
	if err != nil {
		return nil, err
	}
	shown, err := p.serverObservation(server, false)
	if err != nil {
		return nil, err
	}
	if shown == target {
		return transferJSONResponse(http.StatusOK, map[string]interface{}{})
	}
	server.State = target
	data, err := json.Marshal(server)
	if err != nil {
		return nil, fmt.Errorf("transfer move server marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), transferNamespace, transferServerKey(reqCtx.AccountID, reqCtx.Region, server.ServerID), data); err != nil {
		return nil, fmt.Errorf("transfer move server put: %w", err)
	}
	if err := transferServerProgressions.reset(context.Background(), p.state, server.ServerID); err != nil {
		return nil, fmt.Errorf("transfer move server: %w", err)
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{})
}

// handleTransferSeedServerStatus handles POST /v1/transfer/server-status.
func (s *Server) handleTransferSeedServerStatus(w http.ResponseWriter, r *http.Request) {
	transferServerProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleTransferClearServerStatus handles DELETE /v1/transfer/server-status: ?serverId=… clears one
// seed, none clears all.
func (s *Server) handleTransferClearServerStatus(w http.ResponseWriter, r *http.Request) {
	transferServerProgressions.serveClear(w, r, s.state, s.logger)
}
