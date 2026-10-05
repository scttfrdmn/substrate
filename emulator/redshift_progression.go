package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
)

// A Redshift cluster's and snapshot's seeded lifecycles (#1196), on the shared [progression].
//
// A cluster used to be born available, to apply a resize in place, and to vanish the moment
// DeleteCluster answered; a snapshot was born available. So a wait-until-available loop exited on
// its first describe, a modify-then-poll loop had nothing to poll, and nineteen of the twenty
// published ClusterStatus values were unreachable by any input. The seeds below make each transition
// observable, and a failure reachable, without changing what an unseeded resource's describe reports.
//
// # What the pages publish
//
//   - API_CreateCluster's sample response answers ClusterStatus creating, so CreateCluster answers
//     creating, seeded or not. API_Snapshot states CreateClusterSnapshot "returns status as
//     'creating'", so it does too. A describe then reports the seeded countdown; unseeded, the
//     record's own status, available, on the first describe — the zero default every existing
//     fixture's describe relies on.
//   - API_DeleteCluster's sample response answers deleting, and its text says a cluster whose final
//     snapshot is requested is "final-snapshot" while the snapshot is taken and then "deleting". It
//     publishes SkipFinalClusterSnapshot (default false), FinalClusterSnapshotIdentifier ("must be
//     specified if SkipFinalClusterSnapshot is false"; "If this parameter is provided,
//     SkipFinalClusterSnapshot must be false"), FinalClusterSnapshotRetentionPeriod (-1 or 1 to
//     3,653) and the errors ClusterSnapshotAlreadyExists, InvalidClusterState and
//     InvalidRetentionPeriodFault, all 400. Until #1196 all three parameters were discarded.
//   - API_ModifyCluster publishes InvalidClusterState, and API_Cluster's PendingModifiedValues is
//     where a pending resize's new NodeType and NumberOfNodes are reported.
//
// # One seed, every transition
//
// A cluster records which transition it is in. CreateCluster leaves Transition empty, a seeded
// resizing ModifyCluster sets resizing, and a seeded DeleteCluster sets deleting or final-snapshot.
// Each restarts the countdown ([progression.reset]), so one seed covers a create, a resize and a
// delete in turn, as an EC2 instance's covers a launch and a stop (#514). While the countdown runs a
// describe reports the transition's status (or the seed's state, when it names one):
//
//   - create: creating, then the seed's finalState (default the record's available), which is how a
//     test reaches hardware-failure, incompatible-network and the rest;
//   - resize: resizing, with the cluster's previous NodeType and NumberOfNodes and the new ones under
//     PendingModifiedValues, then the new values and no PendingModifiedValues;
//   - delete: final-snapshot for the first observation when a final snapshot was requested, deleting
//     otherwise; once the countdown is spent the record is removed and that describe answers
//     ClusterNotFound.
//
// Unseeded, ModifyCluster applies in place and DeleteCluster removes the record at once, as before.
// A describe observes; CreateCluster, ModifyCluster, DeleteCluster and CreateClusterSnapshot peek.
// ModifyCluster and DeleteCluster refuse a cluster whose resize or delete is still counting down with
// InvalidClusterState, "The specified cluster is not in the available state". A create counting down
// is not refused, because a seed posted after the create restarts that countdown too and is usually
// meant for the next transition; nor is a cluster that settled in a failure status, so a test that
// seeded hardware-failure can still delete it.

// redshiftClusterStatuses is API_Cluster's published ClusterStatus list.
var redshiftClusterStatuses = []string{
	"available", "available, prep-for-resize", "available, resize-cleanup", "cancelling-resize",
	"creating", "deleting", "final-snapshot", "hardware-failure", "incompatible-hsm",
	"incompatible-network", "incompatible-parameters", "incompatible-restore", "modifying", "paused",
	"rebooting", "renaming", "resizing", "rotating-keys", "storage-full", "updating-hsm",
}

// redshiftSnapshotStatuses are the Status values API_Snapshot names: creating, available,
// "final snapshot" and failed (DeleteClusterSnapshot's deleted is not a state a describe reports).
var redshiftSnapshotStatuses = []string{"creating", "available", "final snapshot", "failed"}

// The Transition values a cluster record carries.
const (
	redshiftTransitionResizing      = "resizing"
	redshiftTransitionDeleting      = "deleting"
	redshiftTransitionFinalSnapshot = "final-snapshot"
)

// redshiftClusterProgressions is Redshift's cluster countdown, seeded through
// POST /v1/redshift/cluster-status.
var redshiftClusterProgressions = newProgression[redshiftClusterStatusSeed]("redshift-cluster-ctrl", "clusterIdentifier", "pendingObservations")

// redshiftSnapshotProgressions is Redshift's snapshot countdown, seeded through
// POST /v1/redshift/snapshot-status.
var redshiftSnapshotProgressions = newProgression[redshiftSnapshotStatusSeed]("redshift-snapshot-ctrl", "snapshotIdentifier", "pendingObservations")

// redshiftClusterStatusSeed is the body of POST /v1/redshift/cluster-status:
// {"clusterIdentifier","pendingObservations","state","finalState"}.
type redshiftClusterStatusSeed struct {
	// ClusterIdentifier names the cluster the seed governs; empty or "*" governs every cluster.
	ClusterIdentifier string `json:"clusterIdentifier"`
	// PendingObservations is how many describes of a transition report its transient status.
	PendingObservations int `json:"pendingObservations"`
	// State overrides a create's or resize's transient status (creating or resizing by default). A
	// delete's is always final-snapshot or deleting.
	State string `json:"state,omitempty"`
	// FinalState is the status a create or resize settles in. Defaults to the record's, available.
	FinalState string `json:"finalState,omitempty"`
}

func (s redshiftClusterStatusSeed) progressionID() string        { return s.ClusterIdentifier }
func (s redshiftClusterStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a status outside ClusterStatus, and a finalState a cluster cannot
// settle in: creating, deleting and final-snapshot are transitions.
func (s redshiftClusterStatusSeed) validateProgression() error {
	if err := progressionStates("cluster", redshiftClusterStatuses, s.State, s.FinalState); err != nil {
		return err
	}
	switch s.FinalState {
	case "creating", redshiftTransitionDeleting, redshiftTransitionFinalSnapshot:
		return fmt.Errorf("finalState %q is a transition, not a status a cluster settles in", s.FinalState)
	}
	return nil
}

// redshiftSnapshotStatusSeed is the body of POST /v1/redshift/snapshot-status:
// {"snapshotIdentifier","pendingObservations","state","finalState"}.
type redshiftSnapshotStatusSeed struct {
	// SnapshotIdentifier names the snapshot the seed governs; empty or "*" governs every snapshot.
	SnapshotIdentifier string `json:"snapshotIdentifier"`
	// PendingObservations is how many describes report the transient status.
	PendingObservations int `json:"pendingObservations"`
	// State is the transient status. Defaults to creating.
	State string `json:"state,omitempty"`
	// FinalState is the settled status. Defaults to the record's, available; failed reaches a failed
	// snapshot.
	FinalState string `json:"finalState,omitempty"`
}

func (s redshiftSnapshotStatusSeed) progressionID() string        { return s.SnapshotIdentifier }
func (s redshiftSnapshotStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a status outside the four API_Snapshot names.
func (s redshiftSnapshotStatusSeed) validateProgression() error {
	return progressionStates("snapshot", redshiftSnapshotStatuses, s.State, s.FinalState)
}

// redshiftResizeValues is a cluster's NodeType and NumberOfNodes, kept from before a seeded resize so
// a describe can show them while the resize counts down.
type redshiftResizeValues struct {
	NodeType      string `json:"nodeType"`
	NumberOfNodes int    `json:"numberOfNodes"`
}

// redshiftPendingModifiedValuesXML is API_Cluster's PendingModifiedValues, carrying the two members a
// resize sets.
type redshiftPendingModifiedValuesXML struct {
	NodeType      string `xml:"NodeType,omitempty"`
	NumberOfNodes int    `xml:"NumberOfNodes,omitempty"`
}

// redshiftClusterObservation is what one observation of a cluster reports.
type redshiftClusterObservation struct {
	// Status is the ClusterStatus to report.
	Status string
	// InProgress is true while a seeded resize's or delete's countdown is running. A create's countdown
	// does not set it: a seed posted after a create restarts that cluster's countdown too (a POST resets
	// every counter it governs), and substrate cannot tell such a seed from one meant for the next
	// transition, so a create counting down does not refuse the modify or delete that seed is for.
	InProgress bool
	// Gone is true once a seeded delete's countdown is spent: the record is to be removed.
	Gone bool
	// Previous is the pre-resize NodeType and NumberOfNodes to show while a resize counts down.
	Previous *redshiftResizeValues
}

// clusterObservation reports what the cluster shows now; spend selects an observation over a peek.
func (p *RedshiftPlugin) clusterObservation(c *RedshiftCluster, spend bool) (redshiftClusterObservation, error) {
	var (
		seed *redshiftClusterStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = redshiftClusterProgressions.observe(context.Background(), p.state, &p.seedMu, c.ClusterIdentifier)
	} else {
		seed, seen, err = redshiftClusterProgressions.peek(context.Background(), p.state, c.ClusterIdentifier)
	}
	if err != nil {
		return redshiftClusterObservation{}, fmt.Errorf("redshift cluster observation: %w", err)
	}
	deleting := c.Transition == redshiftTransitionDeleting || c.Transition == redshiftTransitionFinalSnapshot
	if seed == nil {
		return redshiftClusterObservation{Status: c.ClusterStatus, Gone: deleting}, nil
	}
	if seen >= seed.PendingObservations {
		if deleting {
			return redshiftClusterObservation{Gone: true}, nil
		}
		final := seed.FinalState
		if final == "" {
			final = c.ClusterStatus
		}
		return redshiftClusterObservation{Status: final}, nil
	}
	obs := redshiftClusterObservation{InProgress: c.Transition != ""}
	switch c.Transition {
	case redshiftTransitionFinalSnapshot:
		obs.Status = redshiftTransitionDeleting
		if seen == 0 {
			obs.Status = redshiftTransitionFinalSnapshot
		}
	case redshiftTransitionDeleting:
		obs.Status = redshiftTransitionDeleting
	case redshiftTransitionResizing:
		obs.Status = redshiftTransitionResizing
		obs.Previous = c.Previous
	default:
		obs.Status = "creating"
	}
	if seed.State != "" && !deleting {
		obs.Status = seed.State
	}
	return obs, nil
}

// observedClusterXML renders a cluster as one observation reports it.
func observedClusterXML(c RedshiftCluster, obs redshiftClusterObservation) redshiftClusterXML {
	out := clusterToXML(c)
	out.ClusterStatus = obs.Status
	if obs.Previous != nil {
		out.PendingModifiedValues = &redshiftPendingModifiedValuesXML{NodeType: c.NodeType, NumberOfNodes: c.NumberOfNodes}
		out.NodeType = obs.Previous.NodeType
		out.NumberOfNodes = obs.Previous.NumberOfNodes
	}
	return out
}

// removeCluster deletes a cluster's record, its index entry and its countdown.
func (p *RedshiftPlugin) removeCluster(c *RedshiftCluster) error {
	ctx := context.Background()
	if err := p.state.Delete(ctx, redshiftNamespace, redshiftClusterKey(c.AccountID, c.Region, c.ClusterIdentifier)); err != nil {
		return fmt.Errorf("redshift remove cluster: %w", err)
	}
	removeFromStringIndex(ctx, p.state, redshiftNamespace, redshiftClusterIDsKey(c.AccountID, c.Region), c.ClusterIdentifier)
	if err := redshiftClusterProgressions.reset(ctx, p.state, c.ClusterIdentifier); err != nil {
		return fmt.Errorf("redshift remove cluster: %w", err)
	}
	return nil
}

// putCluster stores a cluster record.
func (p *RedshiftPlugin) putCluster(c *RedshiftCluster) error {
	d, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("redshift marshal cluster: %w", err)
	}
	if err := p.state.Put(context.Background(), redshiftNamespace, redshiftClusterKey(c.AccountID, c.Region, c.ClusterIdentifier), d); err != nil {
		return fmt.Errorf("redshift put cluster: %w", err)
	}
	return nil
}

// redshiftInvalidClusterState is the refusal ModifyCluster and DeleteCluster publish for a cluster
// that is not available.
func redshiftInvalidClusterState(id string) *AWSError {
	return &AWSError{
		Code:       "InvalidClusterState",
		Message:    "The specified cluster " + id + " is not in the available state.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// redshiftFinalSnapshotIDPattern is FinalClusterSnapshotIdentifier's published constraint: 1 to 255
// alphanumeric characters or hyphens, a letter first, no trailing hyphen, no two consecutive.
var redshiftFinalSnapshotIDPattern = regexp.MustCompile(`^[A-Za-z](?:[A-Za-z0-9]|-(?:[A-Za-z0-9]))*$`)

// redshiftDeleteRequest is DeleteCluster's three final-snapshot parameters, checked against the page.
type redshiftDeleteRequest struct {
	skipFinalSnapshot bool
	finalSnapshotID   string
}

// parseRedshiftDelete reads and checks DeleteCluster's final-snapshot parameters.
//
// The page requires FinalClusterSnapshotIdentifier unless SkipFinalClusterSnapshot is true, and
// forbids it when SkipFinalClusterSnapshot is true. Neither condition has a code of its own on the
// page, so both are InvalidParameterCombination/400, Redshift's Common Errors code for "parameters
// that must not be used together" — substrate's reading. A malformed boolean or identifier is
// InvalidParameterValue/400, and a retention period outside -1 or 1 to 3,653 is the page's own
// InvalidRetentionPeriodFault/400.
func parseRedshiftDelete(params map[string]string) (redshiftDeleteRequest, *AWSError) {
	var out redshiftDeleteRequest
	if raw, ok := params["SkipFinalClusterSnapshot"]; ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return out, &AWSError{Code: "InvalidParameterValue", Message: "SkipFinalClusterSnapshot must be true or false.", HTTPStatus: http.StatusBadRequest}
		}
		out.skipFinalSnapshot = b
	}
	out.finalSnapshotID = params["FinalClusterSnapshotIdentifier"]
	switch {
	case !out.skipFinalSnapshot && out.finalSnapshotID == "":
		return out, &AWSError{Code: "InvalidParameterCombination", Message: "FinalClusterSnapshotIdentifier is required unless SkipFinalClusterSnapshot is specified.", HTTPStatus: http.StatusBadRequest}
	case out.skipFinalSnapshot && out.finalSnapshotID != "":
		return out, &AWSError{Code: "InvalidParameterCombination", Message: "FinalClusterSnapshotIdentifier must not be specified when SkipFinalClusterSnapshot is true.", HTTPStatus: http.StatusBadRequest}
	}
	if out.finalSnapshotID != "" && (len(out.finalSnapshotID) > 255 || !redshiftFinalSnapshotIDPattern.MatchString(out.finalSnapshotID)) {
		return out, &AWSError{Code: "InvalidParameterValue", Message: "FinalClusterSnapshotIdentifier must be 1 to 255 alphanumeric characters or hyphens, begin with a letter, and contain no two consecutive hyphens or a trailing hyphen.", HTTPStatus: http.StatusBadRequest}
	}
	if raw, ok := params["FinalClusterSnapshotRetentionPeriod"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || (n != -1 && (n < 1 || n > 3653)) {
			return out, &AWSError{Code: "InvalidRetentionPeriodFault", Message: "The retention period must be either -1 or an integer between 1 and 3,653.", HTTPStatus: http.StatusBadRequest}
		}
	}
	return out, nil
}

// snapshotObservation reports what the snapshot shows now; spend selects an observation over a peek.
func (p *RedshiftPlugin) snapshotObservation(s *RedshiftSnapshot, spend bool) (string, error) {
	var (
		seed *redshiftSnapshotStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = redshiftSnapshotProgressions.observe(context.Background(), p.state, &p.seedMu, s.SnapshotIdentifier)
	} else {
		seed, seen, err = redshiftSnapshotProgressions.peek(context.Background(), p.state, s.SnapshotIdentifier)
	}
	if err != nil {
		return "", fmt.Errorf("redshift snapshot observation: %w", err)
	}
	if seed == nil {
		return s.Status, nil
	}
	state, _ := countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "creating", s.Status)
	return state, nil
}

// handleRedshiftSeedClusterStatus handles POST /v1/redshift/cluster-status.
func (s *Server) handleRedshiftSeedClusterStatus(w http.ResponseWriter, r *http.Request) {
	redshiftClusterProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleRedshiftClearClusterStatus handles DELETE /v1/redshift/cluster-status:
// ?clusterIdentifier=… clears one seed, none clears all.
func (s *Server) handleRedshiftClearClusterStatus(w http.ResponseWriter, r *http.Request) {
	redshiftClusterProgressions.serveClear(w, r, s.state, s.logger)
}

// handleRedshiftSeedSnapshotStatus handles POST /v1/redshift/snapshot-status.
func (s *Server) handleRedshiftSeedSnapshotStatus(w http.ResponseWriter, r *http.Request) {
	redshiftSnapshotProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleRedshiftClearSnapshotStatus handles DELETE /v1/redshift/snapshot-status:
// ?snapshotIdentifier=… clears one seed, none clears all.
func (s *Server) handleRedshiftClearSnapshotStatus(w http.ResponseWriter, r *http.Request) {
	redshiftSnapshotProgressions.serveClear(w, r, s.state, s.logger)
}
