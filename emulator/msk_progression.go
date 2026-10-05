package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// An MSK cluster's seeded lifecycle (#1196), on the shared [progression] (#1155, #1196).
//
// A cluster used to be born ACTIVE and to vanish the moment DeleteCluster answered, so a consumer's
// wait-until-active loop exited on its first describe and a wait-until-deleted loop had no deleting
// cluster to poll; CREATING, DELETING and FAILED were unreachable by any input. The seed below makes
// both transitions observable, and a failure reachable, without changing what an unseeded cluster
// reports.
//
// # What the pages publish, and what is substrate's reading
//
// ClusterInfo's state is the ClusterState enumeration: ACTIVE, CREATING, UPDATING, DELETING, FAILED,
// MAINTENANCE, REBOOTING_BROKER and HEALING (clusters-clusterarn.html). stateInfo carries code and
// message "if the cluster is in an unusable state". CreateClusterResponse and DeleteClusterResponse
// each carry a state. No MSK page publishes the value a fresh create answers, so an unseeded create
// answers the record's own state, as it always has, and a seeded one answers CREATING — the seed's
// transient default. DeleteCluster's state is DELETING, as before: the response is the one
// observation of a deleting cluster an unseeded delete affords.
//
// # Two transitions, one seed
//
// A cluster records which transition it is in (Transition: empty for a create, DELETING once a
// seeded delete keeps it). A seed's countdown restarts on every transition — DeleteCluster calls
// [progression.reset] — so one seed covers a create and the delete after it, the way an EC2
// instance's seed covers a launch and a stop (#514). While a create's countdown runs the cluster
// reports the seed's state (default CREATING); once it is spent it reports the seed's finalState
// (default the record's own, ACTIVE), with the seed's stateInfo when that is FAILED. While a delete's
// countdown runs it reports DELETING; once it is spent the record is removed, and that observation
// answers NotFoundException, as a describe of a deleted cluster does.
//
// Describes and lists observe, spending one observation per cluster they report. Create, the
// precondition reads of DeleteCluster, GetBootstrapBrokers and ListNodes peek: none of them is a
// poll of the cluster's state.

// mskClusterStates is ClusterState's published enumeration (clusters-clusterarn.html).
var mskClusterStates = []string{"ACTIVE", "CREATING", "UPDATING", "DELETING", "FAILED", "MAINTENANCE", "REBOOTING_BROKER", "HEALING"}

// mskClusterTransitionDeleting is the Transition a cluster records once a seeded delete keeps it.
const mskClusterTransitionDeleting = "DELETING"

// mskClusterProgressions is MSK's cluster-state countdown, seeded through
// POST /v1/msk/cluster-status.
var mskClusterProgressions = newProgression[mskClusterStatusSeed]("msk-cluster-ctrl", "clusterArn", "pendingObservations")

// mskClusterStatusSeed is the body of POST /v1/msk/cluster-status:
// {"clusterArn","pendingObservations","state","finalState","stateInfo"}.
type mskClusterStatusSeed struct {
	// ClusterARN names the cluster the seed governs; empty or "*" governs every cluster.
	ClusterARN string `json:"clusterArn"`
	// PendingObservations is how many observations of a transition report its transient state.
	PendingObservations int `json:"pendingObservations"`
	// State is what a create's countdown reports. Defaults to CREATING; a delete's always reports
	// DELETING.
	State string `json:"state,omitempty"`
	// FinalState is what the cluster settles in after a create's countdown. Defaults to the record's
	// own state, ACTIVE. FAILED is how a test reaches a failed cluster.
	FinalState string `json:"finalState,omitempty"`
	// StateInfo is answered beside a FAILED final state, as stateInfo's code and message.
	StateInfo *mskStateInfoOut `json:"stateInfo,omitempty"`
}

func (s mskClusterStatusSeed) progressionID() string        { return s.ClusterARN }
func (s mskClusterStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression refuses a state outside ClusterState, and a finalState a cluster could not
// settle in: CREATING and DELETING are transitions, not outcomes.
func (s mskClusterStatusSeed) validateProgression() error {
	if err := progressionStates("cluster", mskClusterStates, s.State, s.FinalState); err != nil {
		return err
	}
	if s.FinalState == "CREATING" || s.FinalState == mskClusterTransitionDeleting {
		return fmt.Errorf("finalState %q is a transition, not a state a cluster settles in", s.FinalState)
	}
	return nil
}

// mskStateInfoOut is ClusterInfo's and Cluster's stateInfo.
type mskStateInfoOut struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// mskObservation is what one observation of a cluster reports: its state, its stateInfo when it
// settled FAILED under a seed, and gone when a seeded delete's countdown is spent.
type mskObservation struct {
	State     string
	StateInfo *mskStateInfoOut
	Gone      bool
}

// clusterObservation reports what the cluster shows now. spend selects an observation (a describe or
// a list) over a peek (every other read).
func (p *MSKPlugin) clusterObservation(c *MSKCluster, spend bool) (mskObservation, error) {
	var (
		seed *mskClusterStatusSeed
		seen int
		err  error
	)
	if spend {
		seed, seen, err = mskClusterProgressions.observe(context.Background(), p.state, &p.seedMu, c.ClusterARN)
	} else {
		seed, seen, err = mskClusterProgressions.peek(context.Background(), p.state, c.ClusterARN)
	}
	if err != nil {
		return mskObservation{}, fmt.Errorf("msk cluster observation: %w", err)
	}
	deleting := c.Transition == mskClusterTransitionDeleting
	if seed == nil {
		// A delete kept by a seed that has since been cleared settles at once: there is nothing left
		// to count down, and the record was only kept for the countdown.
		return mskObservation{State: c.State, Gone: deleting}, nil
	}
	if deleting {
		if seen < seed.PendingObservations {
			return mskObservation{State: mskClusterTransitionDeleting}, nil
		}
		return mskObservation{Gone: true}, nil
	}
	state, terminal := countdownState(seen, seed.PendingObservations, seed.State, seed.FinalState, "CREATING", c.State)
	obs := mskObservation{State: state}
	if terminal && state == "FAILED" {
		obs.StateInfo = seed.StateInfo
	}
	return obs, nil
}

// observedCluster loads the cluster an ARN names and reports it as this read observes it. A cluster
// whose seeded delete has run its course is removed here and answers the not-found refusal, the same
// answer a describe gives a cluster deleted unseeded.
func (p *MSKPlugin) observedCluster(clusterARN string, spend bool) (*MSKCluster, mskObservation, error) {
	cluster, err := p.loadClusterByARN(clusterARN)
	if err != nil {
		return nil, mskObservation{}, err
	}
	obs, err := p.clusterObservation(cluster, spend)
	if err != nil {
		return nil, mskObservation{}, err
	}
	if obs.Gone {
		if err := p.removeCluster(cluster); err != nil {
			return nil, mskObservation{}, err
		}
		return nil, mskObservation{}, mskNotFound("clusterArn", "Cluster not found: "+clusterARN)
	}
	return cluster, obs, nil
}

// removeCluster deletes a cluster's record, its index entry and its countdown.
func (p *MSKPlugin) removeCluster(c *MSKCluster) error {
	scope := c.AccountID + "/" + c.Region
	if err := p.state.Delete(context.Background(), mskNamespace, "cluster:"+scope+"/"+c.ClusterName); err != nil {
		return fmt.Errorf("msk remove cluster: %w", err)
	}
	removeFromStringIndex(context.Background(), p.state, mskNamespace, "cluster_ids:"+scope, c.ClusterName)
	if err := mskClusterProgressions.reset(context.Background(), p.state, c.ClusterARN); err != nil {
		return fmt.Errorf("msk remove cluster: %w", err)
	}
	return nil
}

// observedListing applies one observation to each listed cluster, dropping (and removing) those a
// seeded delete has finished with. Each returned cluster is a copy carrying the observed state.
func (p *MSKPlugin) observedListing(clusters []*MSKCluster) ([]*MSKCluster, []*mskStateInfoOut, error) {
	out := make([]*MSKCluster, 0, len(clusters))
	infos := make([]*mskStateInfoOut, 0, len(clusters))
	for _, c := range clusters {
		obs, err := p.clusterObservation(c, true)
		if err != nil {
			return nil, nil, err
		}
		if obs.Gone {
			if err := p.removeCluster(c); err != nil {
				return nil, nil, err
			}
			continue
		}
		shown := *c
		shown.State = obs.State
		out = append(out, &shown)
		infos = append(infos, obs.StateInfo)
	}
	return out, infos, nil
}

// mskCurrentVersionAlphabet is the alphabet of a minted currentVersion. The page's one example,
// KTVPDKIKX0DER, is thirteen upper-case letters and digits; MSK publishes no pattern, so the length
// and alphabet are that example's.
const mskCurrentVersionAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// mskMintCurrentVersion mints a cluster's currentVersion from the request's [IDMint] (#856), after
// the ARN's UUID, so an ARN minted before #1196 is unchanged.
func mskMintCurrentVersion(m *IDMint) string {
	return m.Chars(13, mskCurrentVersionAlphabet)
}

// checkCurrentVersion refuses a DeleteCluster whose currentVersion names a version the cluster is not
// at. The parameter is optional, so an absent one is accepted; a record from before #1196 carries no
// version, and accepts any. MSK publishes no error codes, so BadRequestException/400 — the status the
// page gives "the input is incorrect" — naming the parameter is substrate's reading (#671).
func mskCheckCurrentVersion(c *MSKCluster, given string) error {
	if given == "" || c.CurrentVersion == "" || given == c.CurrentVersion {
		return nil
	}
	return mskBadRequest("currentVersion", "The cluster's current version is "+c.CurrentVersion+", not "+given+".")
}

// handleMSKSeedClusterStatus handles POST /v1/msk/cluster-status.
func (s *Server) handleMSKSeedClusterStatus(w http.ResponseWriter, r *http.Request) {
	mskClusterProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleMSKClearClusterStatus handles DELETE /v1/msk/cluster-status: ?clusterArn=… clears one seed,
// none clears all.
func (s *Server) handleMSKClearClusterStatus(w http.ResponseWriter, r *http.Request) {
	mskClusterProgressions.serveClear(w, r, s.state, s.logger)
}
