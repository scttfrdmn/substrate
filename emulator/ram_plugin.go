package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// RAMPlugin emulates the AWS Resource Access Manager (RAM) service.
// It handles resource share CRUD operations using the REST/JSON protocol.
type RAMPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// indexMu serializes read-modify-writes of the share-ARN index: a create adds to it while a read
	// may prune an expired share from it, and StateManager has no compare-and-swap (#1171).
	indexMu sync.Mutex
	// scopeMu serializes every share writer per account and Region — the clientToken lookup, check
	// and write of CreateResourceShare and DeleteResourceShare, and the load-modify-Put of Update,
	// Associate and DisassociateResourceShare (#1170). It is always taken before indexMu, never while
	// holding it.
	scopeMu ramScopeMutex
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
		ClientToken             string   `json:"clientToken"`
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
	params, err := ramCanonicalCreateParams(req.Body)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	// The token scan, the check and the write are one step; see ram_idempotency.go (#1170).
	unlock := p.scopeMu.lock(reqCtx.AccountID, reqCtx.Region)
	defer unlock()
	token := input.ClientToken
	if token != "" {
		prior, err := p.findShareByClientToken(goCtx, reqCtx.AccountID, reqCtx.Region, token)
		if err != nil {
			return nil, err
		}
		if prior != nil {
			if prior.CreateParams != params {
				return nil, ramIdempotentParameterMismatch()
			}
			return ramJSONResponse(http.StatusOK, map[string]interface{}{
				"clientToken":   token,
				"resourceShare": ramResourceShareToWire(*prior),
			})
		}
	}

	now := p.tc.Now()
	shareArn := fmt.Sprintf("arn:aws:ram:%s:%s:resource-share/%s", reqCtx.Region, reqCtx.AccountID, generateRAMShareID(reqCtx.IDs))
	if token == "" {
		// Minted after the share ID, so a recording made before #1170 replays to the same ARN.
		token = reqCtx.IDs.UUID()
	}
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
		ClientToken:             token,
		CreateParams:            params,
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
	if err := p.putTokenEntry(goCtx, ramClientTokenKey(reqCtx.AccountID, reqCtx.Region, token), shareArn); err != nil {
		return nil, err
	}
	p.indexMu.Lock()
	defer p.indexMu.Unlock()
	if err := updateStringIndex(goCtx, p.state, ramNamespace, ramShareArnsKey(reqCtx.AccountID, reqCtx.Region), shareArn); err != nil {
		return nil, fmt.Errorf("ram createResourceShare index: %w", err)
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"clientToken":   token,
		"resourceShare": ramResourceShareToWire(share),
	})
}

func (p *RAMPlugin) getResourceShares(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// permissionArn and permissionVersion are accepted and ignored: substrate attaches no managed
	// permission to a share, so there is nothing to filter on (#1542).
	var input struct {
		MaxResults          *int           `json:"maxResults"`
		Name                string         `json:"name"`
		NextToken           string         `json:"nextToken"`
		ResourceOwner       string         `json:"resourceOwner"`
		ResourceShareArns   []string       `json:"resourceShareArns"`
		ResourceShareStatus string         `json:"resourceShareStatus"`
		TagFilters          []ramTagFilter `json:"tagFilters"`
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
	if err := ramValidateTagFilters(input.TagFilters); err != nil {
		return nil, err
	}
	cursor, err := ramReadShareCursor(input.MaxResults, input.NextToken, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
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
			if !ramMatchesTagFilters(input.TagFilters, share.Tags) {
				continue
			}
			shares = append(shares, ramResourceShareToWire(share))
		}
	}
	// The cursor is a value in ARN order, so the listing is put in that order here rather than relying
	// on the index's; see ram_pagination.go (#1542).
	slices.SortFunc(shares, func(a, b ramResourceShareOut) int { return strings.Compare(a.ResourceShareArn, b.ResourceShareArn) })
	page, next := ramPageShares(shares, cursor)
	out := map[string]interface{}{"resourceShares": page}
	if next != "" {
		out["nextToken"] = next
	}
	return ramJSONResponse(http.StatusOK, out)
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

	// Every share writer holds the scope lock across its load, check and Put, so none can overwrite
	// a concurrent delete's DELETED status or token with the record it loaded before it (#1170).
	unlock := p.scopeMu.lock(reqCtx.AccountID, reqCtx.Region)
	defer unlock()

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
		ClientToken      string `json:"clientToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, ramInvalidBody()
		}
	}
	// API_DeleteResourceShare publishes both members as URI query parameters; a POST body is read too.
	if input.ResourceShareArn == "" {
		input.ResourceShareArn = req.Params["resourceShareArn"]
	}
	if input.ClientToken == "" {
		input.ClientToken = req.Params["clientToken"]
	}
	if input.ResourceShareArn == "" {
		return nil, ramMissingParameter("resourceShareArn is required")
	}

	goCtx := context.Background()
	// The token scan, the check and the write are one step; see ram_idempotency.go (#1170).
	unlock := p.scopeMu.lock(reqCtx.AccountID, reqCtx.Region)
	defer unlock()
	token := input.ClientToken
	if token != "" {
		prior, err := p.findShareByDeleteClientToken(goCtx, reqCtx.AccountID, reqCtx.Region, token)
		if err != nil {
			return nil, err
		}
		if prior != nil {
			if prior.ResourceShareArn != input.ResourceShareArn {
				return nil, ramIdempotentParameterMismatch()
			}
			return ramJSONResponse(http.StatusOK, map[string]interface{}{
				"clientToken": token,
				"returnValue": true,
			})
		}
	}

	share, err := p.loadShare(reqCtx.AccountID, reqCtx.Region, input.ResourceShareArn)
	if err != nil {
		return nil, err
	}
	if share.Status == ramShareStatusDeleted {
		return nil, ramInvalidStateTransition(share)
	}
	if token == "" {
		token = reqCtx.IDs.UUID()
	}

	// A soft delete: the share stays, DELETED, for the retention window ram_ownership.go records
	// (#1171). The index entry stays with it so the share is listed until the window passes.
	share.Status = ramShareStatusDeleted
	share.LastUpdatedTime = p.tc.Now()
	share.DeleteClientToken = token
	d, err := json.Marshal(share)
	if err != nil {
		return nil, fmt.Errorf("ram deleteResourceShare marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ramNamespace, ramShareKey(reqCtx.AccountID, reqCtx.Region, share.ResourceShareArn), d); err != nil {
		return nil, fmt.Errorf("ram deleteResourceShare put: %w", err)
	}
	if err := p.putTokenEntry(goCtx, ramDeleteTokenKey(reqCtx.AccountID, reqCtx.Region, token), share.ResourceShareArn); err != nil {
		return nil, err
	}

	return ramJSONResponse(http.StatusOK, map[string]interface{}{
		"clientToken": token,
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

	// Every share writer holds the scope lock across its load, check and Put, so none can overwrite
	// a concurrent delete's DELETED status or token with the record it loaded before it (#1170).
	unlock := p.scopeMu.lock(reqCtx.AccountID, reqCtx.Region)
	defer unlock()

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

	// Every share writer holds the scope lock across its load, check and Put, so none can overwrite
	// a concurrent delete's DELETED status or token with the record it loaded before it (#1170).
	unlock := p.scopeMu.lock(reqCtx.AccountID, reqCtx.Region)
	defer unlock()

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
		MaxResults        *int     `json:"maxResults"`
		NextToken         string   `json:"nextToken"`
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
	pr, err := ramReadPage("ListPrincipals", map[string][]string{
		"resourceOwner":     {input.ResourceOwner},
		"principals":        input.Principals,
		"resourceArn":       {input.ResourceArn},
		"resourceShareArns": input.ResourceShareArns,
		"resourceType":      {input.ResourceType},
	}, input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}

	principals := make([]map[string]interface{}, 0)
	if self {
		shares, err := p.loadShares(context.Background(), reqCtx.AccountID, reqCtx.Region)
		if err != nil {
			return nil, err
		}
		// Shares in ARN order and each share's principals in ID order, the cursor's order, so the
		// share a duplicated principal is listed under is the first in that order.
		ramSortShares(shares)
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
			for _, principal := range slices.Sorted(slices.Values(share.Principals)) {
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

	return ramPaged("principals", "id", principals, pr)
}

// listResources handles ListResources: the resources in the caller's active shares, filtered as
// API_ListResources publishes. See ram_ownership.go (#1171).
func (p *RAMPlugin) listResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults          *int     `json:"maxResults"`
		NextToken           string   `json:"nextToken"`
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
	pr, err := ramReadPage("ListResources", map[string][]string{
		"resourceOwner":       {input.ResourceOwner},
		"principal":           {input.Principal},
		"resourceArns":        input.ResourceArns,
		"resourceShareArns":   input.ResourceShareArns,
		"resourceType":        {input.ResourceType},
		"resourceRegionScope": {input.ResourceRegionScope},
	}, input.MaxResults, input.NextToken)
	if err != nil {
		return nil, err
	}

	resources := make([]map[string]interface{}, 0)
	if self {
		shares, err := p.loadShares(context.Background(), reqCtx.AccountID, reqCtx.Region)
		if err != nil {
			return nil, err
		}
		// Shares in ARN order and each share's resources in ARN order, the cursor's order.
		ramSortShares(shares)
		seen := make(map[string]bool)
		for _, share := range shares {
			if share.Status == ramShareStatusDeleted || !ramMatchesAny(input.ResourceShareArns, share.ResourceShareArn) {
				continue
			}
			if input.Principal != "" && !slices.Contains(share.Principals, input.Principal) {
				continue
			}
			for _, resourceArn := range slices.Sorted(slices.Values(share.ResourceArns)) {
				if seen[resourceArn] || !ramMatchesAny(input.ResourceArns, resourceArn) ||
					!ramMatchesType(input.ResourceType, resourceArn) {
					continue
				}
				if scope := input.ResourceRegionScope; scope != "" && scope != "ALL" && scope != ramResourceRegionScope(resourceArn) {
					continue
				}
				seen[resourceArn] = true
				resource := map[string]interface{}{
					"arn":              resourceArn,
					"resourceShareArn": share.ResourceShareArn,
					"status":           "AVAILABLE",
				}
				// API_Resource publishes `type`; it is answered when the ARN maps to a published type.
				if t := ramResourceType(resourceArn); t != "" {
					resource["type"] = t
				}
				resources = append(resources, resource)
			}
		}
	}

	return ramPaged("resources", "arn", resources, pr)
}

// ramSortShares orders shares by ARN, the leading half of ListPrincipals' and ListResources' cursor.
func ramSortShares(shares []RAMResourceShare) {
	slices.SortFunc(shares, func(a, b RAMResourceShare) int {
		return strings.Compare(a.ResourceShareArn, b.ResourceShareArn)
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
	notFound := &AWSError{Code: ramUnknownResourceCode, Message: "ResourceShare " + shareArn + " not found.", HTTPStatus: http.StatusBadRequest}
	if data == nil {
		return nil, notFound
	}
	var share RAMResourceShare
	if err := json.Unmarshal(data, &share); err != nil {
		return nil, fmt.Errorf("ram loadShare unmarshal: %w", err)
	}
	if !ramShareVisible(&share, p.tc.Now()) {
		if err := p.pruneShare(goCtx, acct, region, &share); err != nil {
			return nil, err
		}
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
