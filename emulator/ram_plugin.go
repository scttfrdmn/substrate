package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// RAMPlugin emulates the AWS Resource Access Manager (RAM) service.
// It handles resource share CRUD operations using the REST/JSON protocol.
type RAMPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "ram".
func (p *RAMPlugin) Name() string { return ramNamespace }

// Initialize sets up the RAMPlugin with the provided configuration.
func (p *RAMPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for RAMPlugin.
func (p *RAMPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a RAM REST/JSON request to the appropriate handler.
func (p *RAMPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := parseRAMOperation(requestMethod(req), req.Path)
	switch op {
	case "CreateResourceShare":
		return p.createResourceShare(reqCtx, req)
	case "GetResourceShares":
		return p.getResourceShares(reqCtx, req)
	case "UpdateResourceShare":
		return p.updateResourceShare(reqCtx, req)
	case "DeleteResourceShare":
		return p.deleteResourceShare(reqCtx, req)
	case "AssociateResourceShare":
		return p.associateResourceShare(reqCtx, req)
	case "DisassociateResourceShare":
		return p.disassociateResourceShare(reqCtx, req)
	case "ListPrincipals":
		return p.listPrincipals(reqCtx, req)
	case "ListResources":
		return p.listResources(reqCtx, req)
	default:
		// parseRAMOperation falls back to the bare HTTP method for a path it does not
		// recognize, which is not an operation name, so the refusal reports the verb and
		// path that resolved to nothing rather than that fallback.
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

func (p *RAMPlugin) createResourceShare(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name                    string   `json:"name"`
		AllowExternalPrincipals bool     `json:"allowExternalPrincipals"`
		Principals              []string `json:"principals"`
		ResourceArns            []string `json:"resourceArns"`
		Tags                    []RAMTag `json:"tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}
	if input.Name == "" {
		return nil, ramMissingParameter("name is required")
	}

	goCtx := context.Background()
	now := p.tc.Now()
	shareArn := fmt.Sprintf("arn:aws:ram:%s:%s:resource-share/%s", reqCtx.Region, reqCtx.AccountID, generateRAMShareID(reqCtx.IDs))
	share := RAMResourceShare{
		ResourceShareArn:        shareArn,
		Name:                    input.Name,
		OwningAccountID:         reqCtx.AccountID,
		AllowExternalPrincipals: input.AllowExternalPrincipals,
		Status:                  "ACTIVE",
		Principals:              input.Principals,
		ResourceArns:            input.ResourceArns,
		Tags:                    input.Tags,
		CreationTime:            now,
		LastUpdatedTime:         now,
		AccountID:               reqCtx.AccountID,
		Region:                  reqCtx.Region,
	}
	if share.Principals == nil {
		share.Principals = []string{}
	}
	if share.ResourceArns == nil {
		share.ResourceArns = []string{}
	}

	d, err := json.Marshal(share)
	if err != nil {
		return nil, fmt.Errorf("ram createResourceShare marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ramNamespace, ramShareKey(reqCtx.AccountID, reqCtx.Region, shareArn), d); err != nil {
		return nil, fmt.Errorf("ram createResourceShare put: %w", err)
	}
	if err := updateStringIndex(goCtx, p.state, ramNamespace, ramShareArnsKey(reqCtx.AccountID, reqCtx.Region), shareArn); err != nil {
		return nil, fmt.Errorf("ram createResourceShare index: %w", err)
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"resourceShare": ramResourceShareToWire(share),
	})
}

func (p *RAMPlugin) getResourceShares(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name                string   `json:"name"`
		ResourceOwner       string   `json:"resourceOwner"`
		ResourceShareArns   []string `json:"resourceShareArns"`
		ResourceShareStatus string   `json:"resourceShareStatus"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}
	// resourceOwner is required and read, and the published filters are applied; see ram_ownership.go
	// (#1171).
	self, err := ramCheckResourceOwner(input.ResourceOwner)
	if err != nil {
		return nil, err
	}
	if input.ResourceShareStatus != "" && !slices.Contains(ramShareStatuses, input.ResourceShareStatus) {
		return nil, ramInvalidParameter("resourceShareStatus must be one of " +
			strings.Join(ramShareStatuses, ", ") + ", was " + input.ResourceShareStatus)
	}

	shares := make([]ramResourceShareOut, 0)
	if self {
		all, err := p.loadShares(context.Background(), reqCtx.AccountID, reqCtx.Region)
		if err != nil {
			return nil, err
		}
		for _, share := range all {
			if !ramMatchesAny(input.ResourceShareArns, share.ResourceShareArn) {
				continue
			}
			if input.Name != "" && share.Name != input.Name {
				continue
			}
			if input.ResourceShareStatus != "" && share.Status != input.ResourceShareStatus {
				continue
			}
			shares = append(shares, ramResourceShareToWire(share))
		}
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"resourceShares": shares,
	})
}

func (p *RAMPlugin) updateResourceShare(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceShareArn        string `json:"resourceShareArn"`
		Name                    string `json:"name"`
		AllowExternalPrincipals *bool  `json:"allowExternalPrincipals"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}

	share, err := p.loadShare(reqCtx.AccountID, reqCtx.Region, input.ResourceShareArn)
	if err != nil {
		return nil, err
	}
	if share.Status == ramShareStatusDeleted {
		return nil, ramOperationNotPermitted(share)
	}

	if input.Name != "" {
		share.Name = input.Name
	}
	if input.AllowExternalPrincipals != nil {
		share.AllowExternalPrincipals = *input.AllowExternalPrincipals
	}
	share.LastUpdatedTime = p.tc.Now()

	goCtx := context.Background()
	d, err := json.Marshal(share)
	if err != nil {
		return nil, fmt.Errorf("ram updateResourceShare marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ramNamespace, ramShareKey(reqCtx.AccountID, reqCtx.Region, share.ResourceShareArn), d); err != nil {
		return nil, fmt.Errorf("ram updateResourceShare put: %w", err)
	}
	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"resourceShare": ramResourceShareToWire(*share),
	})
}

func (p *RAMPlugin) deleteResourceShare(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceShareArn string `json:"resourceShareArn"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}
	// Also check query params (DELETE requests may pass ARN as query param).
	if input.ResourceShareArn == "" {
		input.ResourceShareArn = req.Params["resourceShareArn"]
	}

	share, err := p.loadShare(reqCtx.AccountID, reqCtx.Region, input.ResourceShareArn)
	if err != nil {
		return nil, err
	}
	if share.Status == ramShareStatusDeleted {
		return nil, ramInvalidStateTransition(share)
	}

	// A soft delete: the share stays, DELETED, for the retention window ram_ownership.go records
	// (#1171). The index entry stays with it so the share is listed until the window passes.
	share.Status = ramShareStatusDeleted
	share.LastUpdatedTime = p.tc.Now()
	d, err := json.Marshal(share)
	if err != nil {
		return nil, fmt.Errorf("ram deleteResourceShare marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ramNamespace, ramShareKey(reqCtx.AccountID, reqCtx.Region, share.ResourceShareArn), d); err != nil {
		return nil, fmt.Errorf("ram deleteResourceShare put: %w", err)
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"returnValue": true,
	})
}

func (p *RAMPlugin) associateResourceShare(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceShareArn string   `json:"resourceShareArn"`
		Principals       []string `json:"principals"`
		ResourceArns     []string `json:"resourceArns"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}

	share, err := p.loadShare(reqCtx.AccountID, reqCtx.Region, input.ResourceShareArn)
	if err != nil {
		return nil, err
	}
	if share.Status == ramShareStatusDeleted {
		return nil, ramInvalidStateTransition(share)
	}

	share.Principals = appendUnique(share.Principals, input.Principals...)
	share.ResourceArns = appendUnique(share.ResourceArns, input.ResourceArns...)
	share.LastUpdatedTime = p.tc.Now()

	goCtx := context.Background()
	d, err := json.Marshal(share)
	if err != nil {
		return nil, fmt.Errorf("ram associateResourceShare marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ramNamespace, ramShareKey(reqCtx.AccountID, reqCtx.Region, share.ResourceShareArn), d); err != nil {
		return nil, fmt.Errorf("ram associateResourceShare put: %w", err)
	}

	assocs := buildRAMAssociations(share, input.Principals, input.ResourceArns, "ASSOCIATED")
	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"resourceShareAssociations": assocs,
	})
}

func (p *RAMPlugin) disassociateResourceShare(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceShareArn string   `json:"resourceShareArn"`
		Principals       []string `json:"principals"`
		ResourceArns     []string `json:"resourceArns"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}

	share, err := p.loadShare(reqCtx.AccountID, reqCtx.Region, input.ResourceShareArn)
	if err != nil {
		return nil, err
	}
	if share.Status == ramShareStatusDeleted {
		return nil, ramInvalidStateTransition(share)
	}

	share.Principals = removeStrings(share.Principals, input.Principals...)
	share.ResourceArns = removeStrings(share.ResourceArns, input.ResourceArns...)
	share.LastUpdatedTime = p.tc.Now()

	goCtx := context.Background()
	d, err := json.Marshal(share)
	if err != nil {
		return nil, fmt.Errorf("ram disassociateResourceShare marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ramNamespace, ramShareKey(reqCtx.AccountID, reqCtx.Region, share.ResourceShareArn), d); err != nil {
		return nil, fmt.Errorf("ram disassociateResourceShare put: %w", err)
	}

	assocs := buildRAMAssociations(share, input.Principals, input.ResourceArns, "DISASSOCIATED")
	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"resourceShareAssociations": assocs,
	})
}

// listPrincipals handles ListPrincipals: the principals of the caller's active shares, filtered as
// API_ListPrincipals publishes. See ram_ownership.go (#1171).
func (p *RAMPlugin) listPrincipals(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceOwner     string   `json:"resourceOwner"`
		Principals        []string `json:"principals"`
		ResourceArn       string   `json:"resourceArn"`
		ResourceShareArns []string `json:"resourceShareArns"`
		ResourceType      string   `json:"resourceType"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}
	self, err := ramCheckResourceOwner(input.ResourceOwner)
	if err != nil {
		return nil, err
	}

	principals := make([]map[string]interface{}, 0)
	if self {
		shares, err := p.loadShares(context.Background(), reqCtx.AccountID, reqCtx.Region)
		if err != nil {
			return nil, err
		}
		seen := make(map[string]bool)
		for _, share := range shares {
			if share.Status == ramShareStatusDeleted || !ramMatchesAny(input.ResourceShareArns, share.ResourceShareArn) {
				continue
			}
			if input.ResourceArn != "" && !slices.Contains(share.ResourceArns, input.ResourceArn) {
				continue
			}
			if input.ResourceType != "" && !slices.ContainsFunc(share.ResourceArns, func(arn string) bool {
				return ramMatchesType(input.ResourceType, arn)
			}) {
				continue
			}
			for _, principal := range share.Principals {
				if seen[principal] || !ramMatchesAny(input.Principals, principal) {
					continue
				}
				seen[principal] = true
				principals = append(principals, map[string]interface{}{
					"id":               principal,
					"resourceShareArn": share.ResourceShareArn,
				})
			}
		}
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"principals": principals,
	})
}

// listResources handles ListResources: the resources in the caller's active shares, filtered as
// API_ListResources publishes. See ram_ownership.go (#1171).
func (p *RAMPlugin) listResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceOwner       string   `json:"resourceOwner"`
		Principal           string   `json:"principal"`
		ResourceArns        []string `json:"resourceArns"`
		ResourceShareArns   []string `json:"resourceShareArns"`
		ResourceType        string   `json:"resourceType"`
		ResourceRegionScope string   `json:"resourceRegionScope"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}
	self, err := ramCheckResourceOwner(input.ResourceOwner)
	if err != nil {
		return nil, err
	}
	if input.ResourceRegionScope != "" && !slices.Contains(ramRegionScopes, input.ResourceRegionScope) {
		return nil, ramInvalidParameter("resourceRegionScope must be one of " +
			strings.Join(ramRegionScopes, ", ") + ", was " + input.ResourceRegionScope)
	}

	resources := make([]map[string]interface{}, 0)
	if self {
		shares, err := p.loadShares(context.Background(), reqCtx.AccountID, reqCtx.Region)
		if err != nil {
			return nil, err
		}
		seen := make(map[string]bool)
		for _, share := range shares {
			if share.Status == ramShareStatusDeleted || !ramMatchesAny(input.ResourceShareArns, share.ResourceShareArn) {
				continue
			}
			if input.Principal != "" && !slices.Contains(share.Principals, input.Principal) {
				continue
			}
			for _, resourceArn := range share.ResourceArns {
				if seen[resourceArn] || !ramMatchesAny(input.ResourceArns, resourceArn) ||
					!ramMatchesType(input.ResourceType, resourceArn) {
					continue
				}
				if scope := input.ResourceRegionScope; scope != "" && scope != "ALL" && scope != ramResourceRegionScope(resourceArn) {
					continue
				}
				seen[resourceArn] = true
				resources = append(resources, map[string]interface{}{
					"arn":              resourceArn,
					"resourceShareArn": share.ResourceShareArn,
					"status":           "AVAILABLE",
				})
			}
		}
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"resources": resources,
	})
}

// loadShare loads a RAMResourceShare from state or returns a not-found error.
//
// A DELETED share whose retention window has passed is not found, exactly as a share that never existed
// is; within the window it loads, and each caller decides what its status permits (#1171).
func (p *RAMPlugin) loadShare(acct, region, shareArn string) (*RAMResourceShare, error) {
	if shareArn == "" {
		return nil, ramMissingParameter("resourceShareArn is required")
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, ramNamespace, ramShareKey(acct, region, shareArn))
	if err != nil {
		return nil, fmt.Errorf("ram loadShare get: %w", err)
	}
	notFound := &AWSError{Code: "UnknownResourceException", Message: "ResourceShare " + shareArn + " not found.", HTTPStatus: http.StatusBadRequest}
	if data == nil {
		return nil, notFound
	}
	var share RAMResourceShare
	if err := json.Unmarshal(data, &share); err != nil {
		return nil, fmt.Errorf("ram loadShare unmarshal: %w", err)
	}
	if !ramShareVisible(&share, p.tc.Now()) {
		return nil, notFound
	}
	return &share, nil
}

// appendUnique appends values to slice, skipping duplicates.
func appendUnique(slice []string, values ...string) []string {
	set := make(map[string]bool, len(slice))
	for _, s := range slice {
		set[s] = true
	}
	for _, v := range values {
		if !set[v] {
			set[v] = true
			slice = append(slice, v)
		}
	}
	return slice
}

// removeStrings removes values from slice.
func removeStrings(slice []string, values ...string) []string {
	rm := make(map[string]bool, len(values))
	for _, v := range values {
		rm[v] = true
	}
	result := slice[:0]
	for _, s := range slice {
		if !rm[s] {
			result = append(result, s)
		}
	}
	return result
}

// buildRAMAssociations constructs association objects for principals and resources, each reporting
// status: ASSOCIATED for an associate and DISASSOCIATED for a disassociate (#1171).
func buildRAMAssociations(share *RAMResourceShare, principals, resourceArns []string, status string) []map[string]interface{} {
	assocs := make([]map[string]interface{}, 0, len(principals)+len(resourceArns))
	for _, p := range principals {
		assocs = append(assocs, map[string]interface{}{
			"resourceShareArn":  share.ResourceShareArn,
			"associatedEntity":  p,
			"associationType":   "PRINCIPAL",
			"status":            status,
			"resourceShareName": share.Name,
		})
	}
	for _, r := range resourceArns {
		assocs = append(assocs, map[string]interface{}{
			"resourceShareArn":  share.ResourceShareArn,
			"associatedEntity":  r,
			"associationType":   "RESOURCE",
			"status":            status,
			"resourceShareName": share.Name,
		})
	}
	return assocs
}

// generateRAMShareID mints the identifier inside a resource-share ARN from m, derived from the
// request id so a replayed CreateResourceShare reports the ARN the recording reported (#856).
//
// RAM addresses a share by its whole ARN rather than by this ID — `resourceShareArns` is what
// `GetResourceShares`, `AssociateResourceShare` and `DeleteResourceShare` take — so the ID is never
// sent back on its own, and a re-minted one breaks a recorded call by making the *ARN* unrecognized.
//
// Neither `API_CreateResourceShare` nor the `ResourceShare` type publishes a pattern or a length for
// `resourceShareArn`, so nothing constrains the rendering and it keeps the shape the crypto/rand form
// produced: [IDMint.HexUUID] (#671).
func generateRAMShareID(m *IDMint) string {
	return m.HexUUID()
}

// ramJSONResponse serializes v to JSON and returns an AWSResponse with Content-Type application/json.
func ramJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ram json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
