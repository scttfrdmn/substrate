package emulator

import (
	"context"
	"net/http"
)

// A distribution's Status, seeded through the shared progression (#1381; see progression.go).
//
// API_Distribution publishes Status as a String: "When the status is Deployed, the
// distribution's information is fully propagated to all CloudFront edge locations." A real
// distribution reports InProgress after CreateDistribution and after every UpdateDistribution,
// until the change propagates, and `aws cloudfront wait distribution-deployed` and the SDK waiters
// poll GetDistribution for Deployed. Substrate completes the change in the request, so without a
// seed every distribution is Deployed from its first observation and a consumer's wait loop exits
// on its first poll. The seed makes the window observable: InProgressObservations is how many
// observations report InProgress before the distribution reports its record's own Status.
//
// The default is zero observations, so an unseeded distribution reads exactly as it did before:
// Deployed at once. A seeded wait loop is opt-in because every existing consumer and fixture reads
// Deployed straight after a create, and a default window would turn each of those into a poll.
//
// # Which reads observe
//
// GetDistribution and ListDistributions observe: they are what a waiter polls. ListDistributions
// spends one observation per distribution it reports, through the per-distribution counter, so a
// listing of five distributions under a wildcard seed advances each one once rather than one
// shared countdown five times (#582). The responses of CreateDistribution, the create with tags
// and UpdateDistribution peek — they report the first observation of the new window without
// spending it — and UpdateDistribution restarts the countdown first, because every update starts a
// new propagation; that includes the disable an UpdateDistribution makes before a delete.
// GetDistributionConfig publishes no Status and spends nothing.
//
// # Deleting during the window
//
// API_DeleteDistribution publishes DistributionNotDisabled for an enabled distribution and no
// refusal for one still InProgress, so a disabled distribution deletes whatever an observation
// would report. The CloudFormation stack delete (cfn_delete_cloudfront.go) reads the
// configuration, disables and deletes; it does not poll for Deployed between the disable and the
// delete, because nothing refuses the delete, and its GetDistributionConfig read spends nothing.
// A deleted distribution's counter is removed with it.

// cfDistCtrlNamespace holds the distribution-status seeds and their per-distribution counters.
const cfDistCtrlNamespace = "cloudfront-dist-ctrl"

// The two Status values API_Distribution's description names.
const (
	cfStatusInProgress = "InProgress"
	cfStatusDeployed   = "Deployed"
)

// cfDistributionStatusSeed is the body of POST /v1/cloudfront/distribution-status:
// {"distributionId","inProgressObservations"}. An empty or "*" distributionId governs every
// distribution.
type cfDistributionStatusSeed struct {
	// DistributionID is the distribution the seed governs, or "*" for every distribution.
	DistributionID string `json:"distributionId"`
	// InProgressObservations is how many observations after a create or an update report
	// InProgress before the distribution reports Deployed.
	InProgressObservations int `json:"inProgressObservations"`
}

func (s cfDistributionStatusSeed) progressionID() string        { return s.DistributionID }
func (s cfDistributionStatusSeed) progressionObservations() int { return s.InProgressObservations }

// validateProgression accepts every seed the handler has not already refused: the seed names no
// state, only a count, because the page names exactly two values and the countdown runs from one
// to the other.
func (cfDistributionStatusSeed) validateProgression() error { return nil }

// cfDistributionProgressions is CloudFront's distribution-status countdown.
var cfDistributionProgressions = newProgression[cfDistributionStatusSeed](cfDistCtrlNamespace, "distributionId", "inProgressObservations")

// distributionStatus returns the Status one read of a distribution reports. observe spends an
// observation (GetDistribution, ListDistributions); otherwise the read peeks (the responses of
// the create and update operations). An unseeded distribution reports its record's own Status.
func (p *CloudFrontPlugin) distributionStatus(ctx context.Context, dist CloudFrontDistribution, observe bool) (string, error) {
	var (
		seed *cfDistributionStatusSeed
		seen int
		err  error
	)
	if observe {
		seed, seen, err = cfDistributionProgressions.observe(ctx, p.state, &p.seedMu, dist.ID)
	} else {
		seed, seen, err = cfDistributionProgressions.peek(ctx, p.state, dist.ID)
	}
	if err != nil {
		return "", err
	}
	if seed == nil {
		return dist.Status, nil
	}
	status, _ := countdownState(seen, seed.InProgressObservations, "", "", cfStatusInProgress, dist.Status)
	return status, nil
}

// handleCloudFrontSeedDistributionStatus handles POST /v1/cloudfront/distribution-status. It seeds
// how many observations after a create or an update report InProgress before a distribution
// reports Deployed, for the given distribution or "*". Seeding restarts the countdown of every
// distribution the seed governs.
func (s *Server) handleCloudFrontSeedDistributionStatus(w http.ResponseWriter, r *http.Request) {
	cfDistributionProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleCloudFrontClearDistributionStatus handles DELETE /v1/cloudfront/distribution-status. With
// ?distributionId=... it removes that seed and its countdown; without it removes every seed and
// countdown, and every distribution reads Deployed again.
func (s *Server) handleCloudFrontClearDistributionStatus(w http.ResponseWriter, r *http.Request) {
	cfDistributionProgressions.serveClear(w, r, s.state, s.logger)
}
