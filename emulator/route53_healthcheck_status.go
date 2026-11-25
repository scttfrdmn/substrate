package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

// GetHealthCheckStatus, seeded (#1613).
//
// API_GetHealthCheckStatus answers "one HealthCheckObservation element for each Amazon Route 53
// health checker that is reporting a status about the health check endpoint", each with its Region and
// a StatusReport of Status text and CheckedTime. A real status is the result of requests Route 53
// sends to the endpoint, which is the workload substrate does not run, so the status is a seed and is
// never computed: POST /v1/route53/health-check-status names a health check by ID, or "*" for every
// health check, and says whether it reports healthy, and optionally the exact Status text.
//
// Unseeded, every health check reports healthy. The healthy text is the page's example, "Success:
// HTTP Status Code: 200. Resolved IP: 192.0.2.2. OK", without its "Resolved IP" clause, since
// substrate resolves nothing; the unhealthy text is substrate's own. Either can be replaced by the
// seed's statusReport. One observation is reported for each of the health check's Regions, or for
// each of the eight HealthCheckConfig publishes as the default ("If you don't specify any regions,
// Route 53 health checkers automatically performs checks from all of the regions that are listed under
// Valid Values"), with no IPAddress, because substrate runs no health checkers. CheckedTime is the
// simulated clock's now.
//
// The page says "You can't use GetHealthCheckStatus to get the status of a calculated health check",
// so a CALCULATED health check is refused with InvalidInput (400), the code the page publishes for
// "The input is not valid."; that message is substrate's own.
//
// The seed is a control-plane write mounted in server.go's recorded group, so a replay re-issues it.

// route53HealthCheckCtrlNamespace holds the health-check status seeds.
const route53HealthCheckCtrlNamespace = "route53-healthcheck-ctrl"

// r53DefaultCheckerRegions are the HealthCheckConfig Regions' Valid Values, every Region a health
// check is checked from when it names none.
var r53DefaultCheckerRegions = []string{ //nolint:gochecknoglobals // immutable table
	"us-east-1", "us-west-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "sa-east-1",
}

// The Status texts an unseeded or plainly seeded health check reports.
const (
	r53HealthyStatus   = "Success: HTTP Status Code: 200. OK"
	r53UnhealthyStatus = "Failure: the health check's status is seeded unhealthy."
)

// route53HealthCheckStatusSeed is the body of POST /v1/route53/health-check-status:
// {"healthCheckId","healthy","statusReport"}. An empty or "*" healthCheckId governs every health
// check; an absent healthy means healthy.
type route53HealthCheckStatusSeed struct {
	// HealthCheckID is the health check the seed governs, or "*".
	HealthCheckID string `json:"healthCheckId"`
	// Healthy says whether the health check reports healthy; nil means it does.
	Healthy *bool `json:"healthy,omitempty"`
	// StatusReport replaces the Status text every observation reports.
	StatusReport string `json:"statusReport,omitempty"`
}

// r53HealthCheckSeedKey is the state key of the seed for id, "*" for the wildcard.
func r53HealthCheckSeedKey(id string) string {
	if id == "" {
		id = "*"
	}
	return "status:" + id
}

// healthCheckStatusText returns the Status text health check id reports: its own seed, else the
// wildcard's, else healthy.
func (p *Route53Plugin) healthCheckStatusText(id string) (string, error) {
	for _, key := range []string{r53HealthCheckSeedKey(id), r53HealthCheckSeedKey("*")} {
		data, err := p.state.Get(context.Background(), route53HealthCheckCtrlNamespace, key)
		if err != nil {
			return "", fmt.Errorf("route53 health check status seed get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed route53HealthCheckStatusSeed
		if err := r53DecodeJSON(data, key, &seed); err != nil {
			return "", err
		}
		switch {
		case seed.StatusReport != "":
			return seed.StatusReport, nil
		case seed.Healthy != nil && !*seed.Healthy:
			return r53UnhealthyStatus, nil
		}
		return r53HealthyStatus, nil
	}
	return r53HealthyStatus, nil
}

// getHealthCheckStatus handles GetHealthCheckStatus.
func (p *Route53Plugin) getHealthCheckStatus(reqCtx *RequestContext, _ *AWSRequest, id string) (*AWSResponse, error) {
	hc, err := p.loadHealthCheck(reqCtx, id)
	if err != nil {
		return nil, err
	}
	if hc.Config.Type == "CALCULATED" {
		return nil, &AWSError{Code: "InvalidInput",
			Message:    "GetHealthCheckStatus does not report the status of a CALCULATED health check.",
			HTTPStatus: http.StatusBadRequest}
	}
	text, err := p.healthCheckStatusText(hc.ID)
	if err != nil {
		return nil, err
	}
	regions := hc.Config.Regions
	if len(regions) == 0 {
		regions = r53DefaultCheckerRegions
	}
	type xmlObservation struct {
		Region       string `xml:"Region"`
		StatusReport struct {
			Status      string `xml:"Status"`
			CheckedTime string `xml:"CheckedTime"`
		} `xml:"StatusReport"`
	}
	checked := p.now().UTC().Format(time.RFC3339)
	obs := make([]xmlObservation, 0, len(regions))
	for _, r := range regions {
		o := xmlObservation{Region: r}
		o.StatusReport.Status = text
		o.StatusReport.CheckedTime = checked
		obs = append(obs, o)
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName                 xml.Name         `xml:"GetHealthCheckStatusResponse"`
		XMLNS                   string           `xml:"xmlns,attr"`
		HealthCheckObservations []xmlObservation `xml:"HealthCheckObservations>HealthCheckObservation"`
	}{XMLNS: r53XMLNS, HealthCheckObservations: obs})
}

// handleRoute53SeedHealthCheckStatus handles POST /v1/route53/health-check-status.
func (s *Server) handleRoute53SeedHealthCheckStatus(w http.ResponseWriter, r *http.Request) {
	var seed route53HealthCheckStatusSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := r53HealthCheckSeedKey(seed.HealthCheckID)
	if err := s.state.Put(r.Context(), route53HealthCheckCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "healthCheckId": key})
}

// handleRoute53ClearHealthCheckStatus handles DELETE /v1/route53/health-check-status. With
// ?healthCheckId=... it removes that seed; without it removes every seed, and every health check
// reports healthy again.
func (s *Server) handleRoute53ClearHealthCheckStatus(w http.ResponseWriter, r *http.Request) {
	var keys []string
	if id := r.URL.Query().Get("healthCheckId"); id != "" {
		keys = []string{r53HealthCheckSeedKey(id)}
	} else {
		listed, err := s.state.List(r.Context(), route53HealthCheckCtrlNamespace, "status:")
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		keys = listed
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), route53HealthCheckCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
