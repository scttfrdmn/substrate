package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
)

// elbAccountLimitsCtrlNamespace is the state namespace for the ELB account-limit
// control-plane (seed) data.
const elbAccountLimitsCtrlNamespace = "elb-limits-ctrl"

// elbAccountLimitsMaxPageSize is the largest PageSize DescribeAccountLimits documents.
//
// AWS: "PageSize … Type: Integer. Valid Range: Minimum value of 1. Maximum value of 400."
// The same range is published on both generations' pages.
const elbAccountLimitsMaxPageSize = 400

// elbAccountLimitsDefaultPageSize is the page size used when the request asks for none, or
// asks for one substrate cannot use.
//
// It is the documented maximum, so an unparameterized call returns every limit in one page
// and reports no NextMarker — which is what a caller reading its quotas expects, and what
// the AWS CLI reference's own example output for the operation shows.
const elbAccountLimitsDefaultPageSize = elbAccountLimitsMaxPageSize

// elbAccountLimit is one reported Elastic Load Balancing account limit.
//
// Max is a string rather than an int because that is the member's published type: the
// Limit data type gives "Max … Type: String". A caller's code will want to treat it as a
// number and has to convert, which is why substrate renders the string AWS renders rather
// than the integer it looks like.
type elbAccountLimit struct {
	// Name is the limit's name, e.g. "application-load-balancers".
	Name string

	// Max is the limit's maximum value, rendered as the API's String.
	Max string
}

// elbDefaultAccountLimits is the limit set DescribeAccountLimits reports when nothing is
// seeded, ordered by name so a paged walk is deterministic.
//
// **Provenance, which splits three ways and matters more here than usual.**
//
// The response *shape* is the API model's: `elasticloadbalancingv2-2015-12-01`'s
// API_DescribeAccountLimits and API_Limit
// (https://docs.aws.amazon.com/elasticloadbalancing/latest/APIReference/API_Limit.html).
//
// The limit **names** are not. The v2 API_Limit page enumerates none of them — it says only
// "The name of the limit." and links the three Load Balancer quota user guides. (The
// classic 2012-06-01 API_Limit *does* enumerate exactly three: classic-listeners,
// classic-load-balancers and classic-registered-instances. Those are absent here because
// this handler answers v2 shapes; see #844.) The 23 names below are taken verbatim from the
// example output published on the AWS CLI v2 reference page for the operation,
// https://docs.aws.amazon.com/cli/latest/reference/elbv2/describe-account-limits.html,
// which is the only AWS page found that renders the tokens at all. It is an illustrative
// example, not the API model, and is labeled as such here rather than presented as the
// published model.
//
// The **values** are the current defaults from the three quota user guides the API page
// itself points at, since those are AWS's normative statement of a default and the CLI
// example is an illustration AWS does not keep in step with them:
//   - https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-limits.html
//   - https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-limits.html
//   - https://docs.aws.amazon.com/elasticloadbalancing/latest/gateway/quotas-limits.html
//
// Two entries keep the CLI example's value because no guide row names them cleanly, and one
// takes the guide's where the two disagree. Each says so on its own line.
//
// **Nothing in substrate enforces any of these numbers.** No ELB operation counts a load
// balancer, a target group, a listener or a rule against a quota, seeded or defaulted, and
// none is planned to. The number's whole job is to be the value a caller's
// "am I approaching my quota" branch reads, which is why it is seedable (#885).
var elbDefaultAccountLimits = []elbAccountLimit{
	{Name: "application-load-balancers", Max: "50"},                               // ALB guide: "Application Load Balancers per Region" 50.
	{Name: "certificates-per-application-load-balancer", Max: "25"},               // ALB guide: "Certificates per Application Load Balancer (excluding default certificates)" 25.
	{Name: "certificates-per-network-load-balancer", Max: "25"},                   // NLB guide: "Certificates per Network Load Balancer" 25.
	{Name: "condition-values-per-alb-rule", Max: "5"},                             // ALB guide: "Condition Values per Rule" 5.
	{Name: "condition-wildcards-per-alb-rule", Max: "6"},                          // ALB guide: "Condition Wildcards per Rule" 6. The CLI example says 5; the guide is the newer number and wins.
	{Name: "gateway-load-balancers", Max: "100"},                                  // GWLB guide: "Gateway Load Balancers per Region" 100.
	{Name: "gateway-load-balancers-per-vpc", Max: "100"},                          // GWLB guide: "Gateway Load Balancers per VPC" 100.
	{Name: "geneve-target-groups", Max: "100"},                                    // GWLB guide: "GENEVE target groups per Region" 100.
	{Name: "listeners-per-application-load-balancer", Max: "50"},                  // ALB guide: "Listeners per Application Load Balancer" 50.
	{Name: "listeners-per-network-load-balancer", Max: "50"},                      // NLB guide: "Listeners per Network Load Balancer" 50.
	{Name: "network-load-balancer-enis-per-vpc", Max: "1200"},                     // NLB guide: "Network Load Balancer ENIs per VPC" 1,200.
	{Name: "network-load-balancers", Max: "50"},                                   // NLB guide: "Network Load Balancers per Region" 50.
	{Name: "rules-per-application-load-balancer", Max: "100"},                     // ALB guide: "Rules per Application Load Balancer (excluding default rules)" 100.
	{Name: "target-groups", Max: "3000"},                                          // ALB and NLB guides: "Target Groups per Region" 3,000, shared between the two.
	{Name: "target-groups-per-action-on-application-load-balancer", Max: "5"},     // ALB guide: "Target Groups per Action per Application Load Balancer" 5.
	{Name: "target-groups-per-action-on-network-load-balancer", Max: "1"},         // CLI example only: the NLB guide's nearest row is "Target groups per listener rule action" 5, and whether that row is this token could not be established, so the example's value stands.
	{Name: "target-groups-per-application-load-balancer", Max: "100"},             // ALB guide: "Target Groups per Application Load Balancer" 100.
	{Name: "target-id-registrations-per-application-load-balancer", Max: "1000"},  // CLI example only: no guide row names this limit.
	{Name: "targets-per-application-load-balancer", Max: "1000"},                  // ALB guide: "Targets per Application Load Balancer" 1,000.
	{Name: "targets-per-availability-zone-per-gateway-load-balancer", Max: "300"}, // GWLB guide: "Targets per Availability Zone per Gateway Load Balancer" 300.
	{Name: "targets-per-availability-zone-per-network-load-balancer", Max: "500"}, // NLB guide: "Targets per Availability Zone per Network Load Balancer" 500.
	{Name: "targets-per-network-load-balancer", Max: "3000"},                      // NLB guide: "Targets per Network Load Balancer" 3,000.
	{Name: "targets-per-target-group", Max: "1000"},                               // ALB and NLB guides: "Targets per Target Group per Region (instances or IP addresses)" 1,000.
}

// elbAccountLimitSeed is a seeded DescribeAccountLimits maximum. Substrate enforces no ELB
// quota, so a seed does not change what any operation allows — it changes only what this
// operation reports, which is what a consumer's "approaching my quota" branch reads.
//
// Keyed by limit name or the "*" wildcard, so one seed can lower every limit at once.
type elbAccountLimitSeed struct {
	// Name is the limit name the seed applies to, or "*" for every limit.
	Name string `json:"name"`

	// Max is the value reported for it. It is a string because the API member is one,
	// which keeps the seed and the wire in the same units.
	Max string `json:"max"`
}

// elbAccountLimitsCtrlKey returns the state key for a seeded limit. Name-scoped seeds use
// "limit:{name}"; the wildcard uses "limit:*".
func elbAccountLimitsCtrlKey(name string) string {
	if name == "" {
		name = "*"
	}
	return "limit:" + name
}

// resolveAccountLimitMax returns the seeded Max for one limit name, trying the exact name
// then the "*" wildcard. It returns ("", nil) when no seed applies, so the caller falls
// back to the default in [elbDefaultAccountLimits].
func (p *ELBPlugin) resolveAccountLimitMax(name string) (string, error) {
	goCtx := context.Background()
	for _, key := range []string{elbAccountLimitsCtrlKey(name), elbAccountLimitsCtrlKey("*")} {
		data, err := p.state.Get(goCtx, elbAccountLimitsCtrlNamespace, key)
		if err != nil {
			return "", fmt.Errorf("elb resolveAccountLimitMax get %s: %w", key, err)
		}
		if data == nil {
			continue
		}
		var seed elbAccountLimitSeed
		if err := json.Unmarshal(data, &seed); err != nil {
			return "", fmt.Errorf("elb resolveAccountLimitMax unmarshal %s: %w", key, err)
		}
		return seed.Max, nil
	}
	return "", nil
}

// elbAccountLimitsPageSize resolves the page size a DescribeAccountLimits request asked
// for, falling back to [elbAccountLimitsDefaultPageSize] for a value substrate cannot use:
// absent, non-numeric, or outside the documented 1–400.
//
// Falling back rather than refusing is a decision, because substrate's paginators do not
// agree with each other. The Query-protocol family this operation belongs to — RDS's and
// ElastiCache's `MaxRecords`, CloudWatch's — takes any positive integer and quietly
// substitutes its default for anything else, enforcing no maximum. EC2's `DescribeTags`
// goes the other way and refuses a `MaxResults` outside 5–1000 with
// `InvalidParameterValue`. ELBv2 had no paginated operation at all before this one, so
// there was no ELB precedent to match and one of the two had to be chosen (#885).
//
// The deciding argument is that DescribeAccountLimits publishes **no operation-specific
// error**: its Errors section is Common Errors only, on both generations' pages. Refusing
// would mean inventing a code the page does not publish, which is the thing EC2's
// DescribeTags did *not* have to do. And because the default is the documented maximum, a
// PageSize above 400 is answered indistinguishably from a clamp to 400.
func elbAccountLimitsPageSize(raw string) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > elbAccountLimitsMaxPageSize {
		return elbAccountLimitsDefaultPageSize
	}
	return n
}

// elbLimitItem is the XML representation of an ELBv2 Limit. Members are declared in the
// order API_Limit's Contents section lists them.
type elbLimitItem struct {
	Max  string `xml:"Max"`
	Name string `xml:"Name"`
}

// describeAccountLimits reports the account's Elastic Load Balancing limits.
//
// The operation was authorizable long before it was dispatchable: substrate's generated
// authorization reference already carried `elasticloadbalancing:DescribeAccountLimits`
// with an empty resource list, while the action switch had no arm for it, so a caller
// could be *granted* a permission substrate then answered `InvalidAction` for (#885).
// Nothing here calls an authorization helper because no ELB handler does — every ELB
// request is decided centrally by [AuthController.CheckAccess] before dispatch, and the
// action string is built from the request's service and operation, so the arm below is the
// whole of the wiring the operation needed.
//
// `NextMarker` is **absent** when the walk is exhausted, not empty. The two generations
// document this differently: v2 says "If there are additional results, this is the marker
// for the next set of results. Otherwise, this is null", while classic (2012-06-01) says
// "If there are no additional results, the string is empty" — a present but empty element.
// This handler answers v2 shapes, so it follows v2's null; the difference is recorded here
// for the day classic dispatch arrives (#844), because it is a published difference and not
// a paraphrase.
func (p *ELBPlugin) describeAccountLimits(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	limits := make([]elbLimitItem, 0, len(elbDefaultAccountLimits))
	for _, limit := range elbDefaultAccountLimits {
		reported := limit.Max
		seeded, err := p.resolveAccountLimitMax(limit.Name)
		if err != nil {
			return nil, err
		}
		if seeded != "" {
			reported = seeded
		}
		limits = append(limits, elbLimitItem{Max: reported, Name: limit.Name})
	}

	// The cursor is a decimal offset into the fixed order above, matching the Marker RDS
	// and ElastiCache already answer with. An unparseable Marker restarts from the
	// beginning and an offset past the end yields an empty last page, both as they do:
	// neither is an error the operation publishes a code for.
	offset := 0
	if n, err := strconv.Atoi(req.Params["Marker"]); err == nil && n > 0 {
		offset = n
	}
	if offset > len(limits) {
		offset = len(limits)
	}
	page := limits[offset:]

	var nextMarker string
	if pageSize := elbAccountLimitsPageSize(req.Params["PageSize"]); len(page) > pageSize {
		page = page[:pageSize]
		nextMarker = strconv.Itoa(offset + pageSize)
	}

	type limitsResult struct {
		Limits     []elbLimitItem `xml:"Limits>member"`
		NextMarker string         `xml:"NextMarker,omitempty"`
	}
	type response struct {
		XMLName xml.Name     `xml:"DescribeAccountLimitsResponse"`
		XMLNS   string       `xml:"xmlns,attr"`
		Result  limitsResult `xml:"DescribeAccountLimitsResult"`
	}
	return elbXMLResponse(http.StatusOK, response{
		XMLNS:  elbXMLNS,
		Result: limitsResult{Limits: page, NextMarker: nextMarker},
	})
}

// handleELBSeedAccountLimit handles POST /v1/elb/account-limits. It seeds the Max that
// DescribeAccountLimits reports for one limit name, or for every limit when the name is
// "*" or omitted. Substrate enforces no ELB quota; this sets only what the operation
// reports. Body: {"name","max"}.
func (s *Server) handleELBSeedAccountLimit(w http.ResponseWriter, r *http.Request) {
	var seed elbAccountLimitSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Max == "" {
		http.Error(w, `{"error":"max is required"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := elbAccountLimitsCtrlKey(seed.Name)
	if err := s.state.Put(r.Context(), elbAccountLimitsCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "name": key})
}

// handleELBClearAccountLimit handles DELETE /v1/elb/account-limits. With ?name=... it
// removes that seed; without it removes all.
func (s *Server) handleELBClearAccountLimit(w http.ResponseWriter, r *http.Request) {
	if name := r.URL.Query().Get("name"); name != "" {
		if err := s.state.Delete(r.Context(), elbAccountLimitsCtrlNamespace, elbAccountLimitsCtrlKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), elbAccountLimitsCtrlNamespace, "limit:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), elbAccountLimitsCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
