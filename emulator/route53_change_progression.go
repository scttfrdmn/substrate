package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// A change's Status, seeded through the shared progression (#1613; see progression.go).
//
// API_GetChange: "PENDING indicates that the changes in this request have not propagated to all
// Amazon Route 53 DNS servers managing the hosted zone. This is the initial status of all change
// batch requests. INSYNC indicates that the changes have propagated". CreateHostedZone,
// DeleteHostedZone and ChangeResourceRecordSets each answer a ChangeInfo, and every one is stored as
// a "change:{id}" record holding its ID, SubmittedAt and Comment, so GetChange can report it.
// Terraform's aws_route53_zone and aws_route53_record, and the SDKs' resource_record_sets_changed
// waiter, poll GetChange until INSYNC.
//
// Substrate applies a change in the request, so its settled Status is INSYNC, and an unseeded
// change reports INSYNC from its first observation: every existing consumer reads INSYNC straight
// from the response. The seed makes the propagation window observable: PendingObservations is how
// many observations report PENDING before the change reports INSYNC.
//
// GetChange observes; the responses of the three operations that mint a change peek, reporting the
// first observation of the window without spending it. A change is never reset: each change is a
// new ID, so each starts its own countdown, and a wildcard seed governs every change separately.
//
// The seed's changeId may be given with or without the "/change/" prefix ChangeInfo's Id carries;
// it is stored without it.

// route53ChangeCtrlNamespace holds the change-status seeds and their per-change counters.
const route53ChangeCtrlNamespace = "route53-change-ctrl"

// The two values API_ChangeInfo publishes for Status.
const (
	r53ChangePending = "PENDING"
	r53ChangeInSync  = "INSYNC"
)

// r53ChangePathPrefix is the prefix ChangeInfo's Id carries.
const r53ChangePathPrefix = "/change/"

// route53ChangeStatusSeed is the body of POST /v1/route53/change-status:
// {"changeId","pendingObservations"}. An empty or "*" changeId governs every change.
type route53ChangeStatusSeed struct {
	// ChangeID is the change the seed governs, with or without the /change/ prefix, or "*".
	ChangeID string `json:"changeId"`
	// PendingObservations is how many observations of a change report PENDING before INSYNC.
	PendingObservations int `json:"pendingObservations"`
}

func (s route53ChangeStatusSeed) progressionID() string        { return r53ChangeSuffix(s.ChangeID) }
func (s route53ChangeStatusSeed) progressionObservations() int { return s.PendingObservations }

// validateProgression accepts every seed the handler has not already refused: the seed names no
// state, only a count, because the page publishes exactly two values and the countdown runs from one
// to the other.
func (route53ChangeStatusSeed) validateProgression() error { return nil }

// route53ChangeProgressions is Route 53's change-status countdown.
var route53ChangeProgressions = newProgression[route53ChangeStatusSeed](route53ChangeCtrlNamespace, "changeId", "pendingObservations")

// r53ChangeSuffix returns a change ID without its "/change/" prefix. A raw request may carry the
// prefix in the path (/2013-04-01/change//change/C…), and a seed may name the ID either way.
func r53ChangeSuffix(id string) string {
	id = strings.TrimPrefix(id, "/")
	return strings.TrimPrefix(id, strings.TrimPrefix(r53ChangePathPrefix, "/"))
}

// r53ChangeKey is the state key of a stored change.
func r53ChangeKey(id string) string {
	return "change:" + r53ChangeSuffix(id)
}

// r53XMLChangeInfo is a ChangeInfo as every response that carries one renders it.
type r53XMLChangeInfo struct {
	ID          string `xml:"Id"`
	Status      string `xml:"Status"`
	SubmittedAt string `xml:"SubmittedAt"`
	Comment     string `xml:"Comment,omitempty"`
}

// recordChange mints and stores a change, and returns it as the minting operation's response
// reports it: the Status its first observation would show, without spending that observation.
func (p *Route53Plugin) recordChange(reqCtx *RequestContext, comment string) (r53XMLChangeInfo, error) {
	change := Route53ChangeInfo{
		ID:          generateChangeID(reqCtx.IDs),
		Status:      r53ChangeInSync,
		SubmittedAt: p.now().UTC(),
		Comment:     comment,
	}
	data, err := json.Marshal(change)
	if err != nil {
		return r53XMLChangeInfo{}, fmt.Errorf("route53 marshal change: %w", err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, r53ChangeKey(change.ID), data); err != nil {
		return r53XMLChangeInfo{}, fmt.Errorf("route53 put change: %w", err)
	}
	return p.changeInfo(change, false)
}

// changeInfo renders a stored change with the Status one read reports. observe spends an
// observation (GetChange); otherwise the read peeks.
func (p *Route53Plugin) changeInfo(change Route53ChangeInfo, observe bool) (r53XMLChangeInfo, error) {
	ctx := context.Background()
	id := r53ChangeSuffix(change.ID)
	var (
		seed *route53ChangeStatusSeed
		seen int
		err  error
	)
	if observe {
		seed, seen, err = route53ChangeProgressions.observe(ctx, p.state, &p.seedMu, id)
	} else {
		seed, seen, err = route53ChangeProgressions.peek(ctx, p.state, id)
	}
	if err != nil {
		return r53XMLChangeInfo{}, err
	}
	status := change.Status
	if seed != nil {
		status, _ = countdownState(seen, seed.PendingObservations, "", "", r53ChangePending, change.Status)
	}
	return r53XMLChangeInfo{
		ID:          change.ID,
		Status:      status,
		SubmittedAt: change.SubmittedAt.Format(time.RFC3339),
		Comment:     change.Comment,
	}, nil
}

// getChange handles GetChange. API_GetChange publishes NoSuchChange (404), "A change with the
// specified change ID does not exist.", which is the message for an ID no stored change has.
func (p *Route53Plugin) getChange(_ *RequestContext, _ *AWSRequest, rawID string) (*AWSResponse, error) {
	id, err := url.PathUnescape(rawID)
	if err != nil {
		id = rawID
	}
	data, err := p.state.Get(context.Background(), route53Namespace, r53ChangeKey(id))
	if err != nil {
		return nil, fmt.Errorf("route53 getChange get: %w", err)
	}
	if data == nil || r53ChangeSuffix(id) == "" {
		return nil, &AWSError{Code: "NoSuchChange", Message: "A change with the specified change ID does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var change Route53ChangeInfo
	if err := json.Unmarshal(data, &change); err != nil {
		return nil, fmt.Errorf("route53 getChange unmarshal: %w", err)
	}
	info, err := p.changeInfo(change, true)
	if err != nil {
		return nil, err
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName    xml.Name         `xml:"GetChangeResponse"`
		XMLNS      string           `xml:"xmlns,attr"`
		ChangeInfo r53XMLChangeInfo `xml:"ChangeInfo"`
	}{XMLNS: r53XMLNS, ChangeInfo: info})
}

// handleRoute53SeedChangeStatus handles POST /v1/route53/change-status. It seeds how many
// observations of a change report PENDING before INSYNC, for the given change or "*". Seeding
// restarts the countdown of every change the seed governs.
func (s *Server) handleRoute53SeedChangeStatus(w http.ResponseWriter, r *http.Request) {
	route53ChangeProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleRoute53ClearChangeStatus handles DELETE /v1/route53/change-status. With ?changeId=... (with
// or without the /change/ prefix) it removes that seed and its countdown; without it removes every
// seed and countdown, and every change reads INSYNC again.
func (s *Server) handleRoute53ClearChangeStatus(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("changeId"); id != "" {
		q := r.URL.Query()
		q.Set("changeId", r53ChangeSuffix(id))
		r.URL.RawQuery = q.Encode()
	}
	route53ChangeProgressions.serveClear(w, r, s.state, s.logger)
}
