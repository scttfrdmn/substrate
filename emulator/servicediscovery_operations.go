package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"
)

// Cloud Map's asynchronous operations (#1614).
//
// Creating and deleting a namespace, updating a namespace or a service, and registering and
// deregistering an instance each answer an OperationId, and a caller polls GetOperation until its
// Status is SUCCESS or FAIL: "SUBMITTED This is the initial state that occurs immediately after you
// submit a request. PENDING Cloud Map is performing the operation. SUCCESS The operation succeeded.
// FAIL The operation failed" (the Operation shape). The Terraform AWS provider waits on exactly
// that for every one of these (internal/service/servicediscovery/operation.go).
//
// # When the effect happens
//
// An operation's settled status is decided when it starts, and its effect is applied then, in the
// request, or not at all. An operation that will succeed changes the namespace, service or instance
// before the request answers; one that will fail changes nothing. So a caller that follows the
// protocol — poll GetOperation to SUCCESS, then read — always reads the operation's effect, and one
// that polls to FAIL never does. The one order this does not keep is AWS's latency: a read made
// before the operation reports SUCCESS already sees the effect.
//
// # The seed
//
// Unseeded, an operation reports its settled status from its first observation, so a poller exits
// at once. POST /v1/servicediscovery/operation-status seeds, for one operation ID or "*", how many
// observations report a transient status (SUBMITTED, or PENDING when the seed says so) before the
// settled one, and whether the settled status is SUCCESS or FAIL, with the ErrorCode and
// ErrorMessage a FAIL reports. GetOperation observes, spending one; ListOperations peeks. A seed's
// status and error govern the operations that start while it is in force — an operation already
// started keeps its settled status, because its effect has already been applied or withheld — and
// its countdown governs every operation it names.

// The four OperationStatus values.
const (
	sdOpSubmitted = "SUBMITTED"
	sdOpPending   = "PENDING"
	sdOpSuccess   = "SUCCESS"
	sdOpFail      = "FAIL"
)

// serviceDiscoveryOperationCtrlNamespace holds the operation-status seeds and their counters.
const serviceDiscoveryOperationCtrlNamespace = "servicediscovery-operation-ctrl"

// sdOperationStatusSeed is the body of POST /v1/servicediscovery/operation-status:
// {"operationId","submittedObservations","transientStatus","status","errorCode","errorMessage"}.
type sdOperationStatusSeed struct {
	// OperationID is the operation the seed governs, or "*" (or empty) for every operation.
	OperationID string `json:"operationId"`
	// SubmittedObservations is how many observations report TransientStatus before the settled one.
	SubmittedObservations int `json:"submittedObservations"`
	// TransientStatus is SUBMITTED (the default) or PENDING.
	TransientStatus string `json:"transientStatus,omitempty"`
	// Status is the settled status of an operation started under the seed: SUCCESS (the default)
	// or FAIL.
	Status string `json:"status,omitempty"`
	// ErrorCode and ErrorMessage are what a FAIL reports.
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func (s sdOperationStatusSeed) progressionID() string        { return s.OperationID }
func (s sdOperationStatusSeed) progressionObservations() int { return s.SubmittedObservations }

// validateProgression refuses a status outside the Operation shape's enumeration, a transient
// status that is settled or a settled one that is transient, and an error on a seed that succeeds.
func (s sdOperationStatusSeed) validateProgression() error {
	if err := progressionStates("transient operation", []string{sdOpSubmitted, sdOpPending}, s.TransientStatus); err != nil {
		return err
	}
	if err := progressionStates("settled operation", []string{sdOpSuccess, sdOpFail}, s.Status); err != nil {
		return err
	}
	if s.Status != sdOpFail && (s.ErrorCode != "" || s.ErrorMessage != "") {
		return fmt.Errorf("errorCode and errorMessage need status %q", sdOpFail)
	}
	return nil
}

// sdOperationProgressions is Cloud Map's operation-status countdown.
var sdOperationProgressions = newProgression[sdOperationStatusSeed](serviceDiscoveryOperationCtrlNamespace, "operationId", "submittedObservations")

// newOperation mints an operation of typ on targets and decides its settled status from the seed
// in force, without storing it. The caller applies the effect only when the status is SUCCESS, and
// may fail the operation itself (see [sdOperation]) before it calls [ServiceDiscoveryPlugin.saveOperation].
func (p *ServiceDiscoveryPlugin) newOperation(ctx *RequestContext, typ string, targets map[string]string) (sdOperation, error) {
	op := sdOperation{
		ID:         ctx.IDs.Chars(32, sdIDAlphabet) + "-" + ctx.IDs.Chars(9, sdIDAlphabet),
		Type:       typ,
		Status:     sdOpSuccess,
		CreateDate: p.tc.Now().UTC(),
		Targets:    targets,
	}
	seed, err := sdOperationProgressions.resolve(context.Background(), p.state, op.ID)
	if err != nil {
		return sdOperation{}, err
	}
	if seed != nil && seed.Status == sdOpFail {
		op.Status, op.ErrorCode, op.ErrorMessage = sdOpFail, seed.ErrorCode, seed.ErrorMessage
	}
	return op, nil
}

// fail settles op as FAIL with code and message.
func (op *sdOperation) fail(code, message string) {
	op.Status, op.ErrorCode, op.ErrorMessage = sdOpFail, code, message
}

// saveOperation stores op.
func (p *ServiceDiscoveryPlugin) saveOperation(ctx *RequestContext, op sdOperation) error {
	return p.put(sdOperationKey(ctx, op.ID), op)
}

// sdOperationAnswer is the {"OperationId"} an asynchronous operation answers.
func sdOperationAnswer(op sdOperation) (*AWSResponse, error) {
	return sdJSON(struct {
		OperationID string `json:"OperationId"`
	}{op.ID})
}

// operationStatus reports the status one read of op shows. observe spends an observation.
func (p *ServiceDiscoveryPlugin) operationStatus(op sdOperation, observe bool) (string, bool, error) {
	var (
		seed *sdOperationStatusSeed
		seen int
		err  error
	)
	if observe {
		seed, seen, err = sdOperationProgressions.observe(context.Background(), p.state, &p.seedMu, op.ID)
	} else {
		seed, seen, err = sdOperationProgressions.peek(context.Background(), p.state, op.ID)
	}
	if err != nil {
		return "", false, err
	}
	if seed == nil {
		return op.Status, true, nil
	}
	status, terminal := countdownState(seen, seed.SubmittedObservations, seed.TransientStatus, "", sdOpSubmitted, op.Status)
	return status, terminal, nil
}

// getOperation handles GetOperation. An ID no operation has is OperationNotFound (404).
func (p *ServiceDiscoveryPlugin) getOperation(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		OperationID string `json:"OperationId"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.OperationID == "" {
		return nil, sdInvalid("OperationId is required.")
	}
	var op sdOperation
	found, err := p.get(sdOperationKey(ctx, in.OperationID), &op)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sdError("OperationNotFound", http.StatusNotFound, "Operation %s not found.", in.OperationID)
	}
	status, terminal, err := p.operationStatus(op, true)
	if err != nil {
		return nil, err
	}
	out := sdOperationOut{
		ID: op.ID, OwnerAccount: ctx.AccountID, Type: op.Type, Status: status,
		CreateDate: EpochSeconds(op.CreateDate), UpdateDate: EpochSeconds(op.CreateDate), Targets: op.Targets,
	}
	if terminal && status == sdOpFail {
		out.ErrorCode, out.ErrorMessage = op.ErrorCode, op.ErrorMessage
	}
	return sdJSON(struct {
		Operation sdOperationOut `json:"Operation"`
	}{out})
}

// sdFilter is the NamespaceFilter, ServiceFilter and OperationFilter structure.
type sdFilter struct {
	Name      string   `json:"Name"`
	Values    []string `json:"Values"`
	Condition string   `json:"Condition"`
}

// matches reports whether value satisfies f under its condition: EQ (the default) and IN compare
// whole values, BEGINS_WITH a prefix.
func (f sdFilter) matches(value string) bool {
	switch f.Condition {
	case "BEGINS_WITH":
		return len(f.Values) == 1 && len(value) >= len(f.Values[0]) && value[:len(f.Values[0])] == f.Values[0]
	default:
		return slices.Contains(f.Values, value)
	}
}

// sdCheckFilters validates filters against the names and conditions an operation's filter shape
// lists: allowed maps each name to its conditions, the first of which is the default. EQ and
// BEGINS_WITH take one value. The messages are substrate's own.
func sdCheckFilters(filters []sdFilter, allowed map[string][]string) *AWSError {
	for i := range filters {
		f := &filters[i]
		conds, ok := allowed[f.Name]
		if !ok {
			return sdInvalid("Unsupported filter name: %s.", f.Name)
		}
		if f.Condition == "" {
			f.Condition = conds[0]
		}
		if !slices.Contains(conds, f.Condition) {
			return sdInvalid("Filter %s does not support condition %s.", f.Name, f.Condition)
		}
		if len(f.Values) == 0 {
			return sdInvalid("Filter %s needs at least one value.", f.Name)
		}
		if (f.Condition == "EQ" || f.Condition == "BEGINS_WITH") && len(f.Values) != 1 {
			return sdInvalid("Filter %s with condition %s takes exactly one value.", f.Name, f.Condition)
		}
		if f.Condition == "BETWEEN" && len(f.Values) != 2 {
			return sdInvalid("Filter %s with condition BETWEEN takes exactly two values.", f.Name)
		}
	}
	return nil
}

// sdPageSize reads a list operation's MaxResults: 1 to max, def when absent. The message is
// substrate's own.
func sdPageSize(maxResults *int32, def, maxSize int32) (int, *AWSError) {
	if maxResults == nil {
		return int(def), nil
	}
	if *maxResults < 1 || *maxResults > maxSize {
		return 0, sdInvalid("MaxResults must be between 1 and %d.", maxSize)
	}
	return int(*maxResults), nil
}

// sdFilterValues flattens filters into a page token's filter members.
func sdFilterValues(filters []sdFilter) map[string][]string {
	out := map[string][]string{}
	for _, f := range filters {
		key := f.Name + "/" + f.Condition
		out[key] = append(out[key], f.Values...)
	}
	return out
}

// listOperations handles ListOperations, filtered on NAMESPACE_ID, SERVICE_ID, STATUS, TYPE and
// UPDATE_DATE as the OperationFilter shape lists, in creation order. A status filter compares the
// status a read would report now, without spending an observation.
func (p *ServiceDiscoveryPlugin) listOperations(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		NextToken  string     `json:"NextToken"`
		MaxResults *int32     `json:"MaxResults"`
		Filters    []sdFilter `json:"Filters"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := sdCheckFilters(in.Filters, map[string][]string{
		"NAMESPACE_ID": {"EQ"}, "SERVICE_ID": {"EQ"}, "STATUS": {"EQ", "IN"}, "TYPE": {"EQ", "IN"},
		"UPDATE_DATE": {"BETWEEN"},
	}); awsErr != nil {
		return nil, awsErr
	}
	size, awsErr := sdPageSize(in.MaxResults, 100, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	preq := pageRequest{Op: "ListOperations", Token: in.NextToken, Size: size, Filter: sdFilterValues(in.Filters)}
	if err := preq.validate(); err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	var ops []sdOperation
	if err := p.list(sdOperationKeyPrefix+sdScope(ctx), func(raw []byte) error {
		var op sdOperation
		if err := json.Unmarshal(raw, &op); err != nil {
			return fmt.Errorf("servicediscovery decode operation: %w", err)
		}
		ops = append(ops, op)
		return nil
	}); err != nil {
		return nil, err
	}
	type summary struct {
		op     sdOperation
		status string
	}
	var matched []summary
	for _, op := range ops {
		status, _, err := p.operationStatus(op, false)
		if err != nil {
			return nil, err
		}
		if sdOperationMatches(op, status, in.Filters) {
			matched = append(matched, summary{op, status})
		}
	}
	res, err := page(matched, func(s summary) cursorKey {
		return cursorKey{Sort: s.op.CreateDate.Format(time.RFC3339Nano), Tie: s.op.ID}
	}, preq)
	if err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	out := struct {
		Operations []sdOperationSummaryOut `json:"Operations"`
		NextToken  string                  `json:"NextToken,omitempty"`
	}{Operations: make([]sdOperationSummaryOut, 0, len(res.Items)), NextToken: res.Next}
	for _, s := range res.Items {
		out.Operations = append(out.Operations, sdOperationSummaryOut{ID: s.op.ID, Status: s.status})
	}
	return sdJSON(out)
}

// sdOperationMatches reports whether op, reporting status, satisfies every filter.
func sdOperationMatches(op sdOperation, status string, filters []sdFilter) bool {
	for _, f := range filters {
		var ok bool
		switch f.Name {
		case "NAMESPACE_ID":
			ok = f.matches(op.Targets["NAMESPACE"])
		case "SERVICE_ID":
			ok = f.matches(op.Targets["SERVICE"])
		case "STATUS":
			ok = f.matches(status)
		case "TYPE":
			ok = f.matches(op.Type)
		case "UPDATE_DATE":
			from, err1 := strconv.ParseFloat(f.Values[0], 64)
			to, err2 := strconv.ParseFloat(f.Values[1], 64)
			at := float64(op.CreateDate.UnixNano()) / 1e9
			ok = err1 == nil && err2 == nil && at >= from && at <= to
		}
		if !ok {
			return false
		}
	}
	return true
}

// handleServiceDiscoverySeedOperationStatus handles POST /v1/servicediscovery/operation-status.
func (s *Server) handleServiceDiscoverySeedOperationStatus(w http.ResponseWriter, r *http.Request) {
	sdOperationProgressions.serveSeed(w, r, s.state, s.logger)
}

// handleServiceDiscoveryClearOperationStatus handles DELETE /v1/servicediscovery/operation-status.
// With ?operationId=... it removes that seed and its countdown; without it removes every seed and
// countdown, and every operation reports its settled status again.
func (s *Server) handleServiceDiscoveryClearOperationStatus(w http.ResponseWriter, r *http.Request) {
	sdOperationProgressions.serveClear(w, r, s.state, s.logger)
}
