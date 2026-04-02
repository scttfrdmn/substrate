package substrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// WAFv2Plugin emulates the AWS WAFv2 (Web Application Firewall v2) service.
// It handles Web ACL and IP set CRUD operations, LockToken-based optimistic
// concurrency control, and WebACL-to-resource associations.
// Protocol: JSON-target AWSWAF_20190729.{Op} with X-Amz-Target header.
type WAFv2Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "wafv2".
func (p *WAFv2Plugin) Name() string { return wafv2Namespace }

// Initialize sets up the WAFv2Plugin with the provided configuration.
func (p *WAFv2Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for WAFv2Plugin.
func (p *WAFv2Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a WAFv2 JSON-target request to the appropriate handler.
func (p *WAFv2Plugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateWebACL":
		return p.createWebACL(reqCtx, req)
	case "GetWebACL":
		return p.getWebACL(reqCtx, req)
	case "UpdateWebACL":
		return p.updateWebACL(reqCtx, req)
	case "DeleteWebACL":
		return p.deleteWebACL(reqCtx, req)
	case "ListWebACLs":
		return p.listWebACLs(reqCtx, req)
	case "AssociateWebACL":
		return p.associateWebACL(reqCtx, req)
	case "DisassociateWebACL":
		return p.disassociateWebACL(reqCtx, req)
	case "GetWebACLForResource":
		return p.getWebACLForResource(reqCtx, req)
	case "CreateIPSet":
		return p.createIPSet(reqCtx, req)
	case "GetIPSet":
		return p.getIPSet(reqCtx, req)
	case "UpdateIPSet":
		return p.updateIPSet(reqCtx, req)
	case "DeleteIPSet":
		return p.deleteIPSet(reqCtx, req)
	case "ListIPSets":
		return p.listIPSets(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "WAFv2Plugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// State key helpers.
func wafv2WebACLKey(acct, region, scope, id string) string {
	return "webacl:" + acct + "/" + region + "/" + scope + "/" + id
}

func wafv2WebACLIDsKey(acct, region, scope string) string {
	return "webacl_ids:" + acct + "/" + region + "/" + scope
}

func wafv2IPSetKey(acct, region, scope, id string) string {
	return "ipset:" + acct + "/" + region + "/" + scope + "/" + id
}

func wafv2IPSetIDsKey(acct, region, scope string) string {
	return "ipset_ids:" + acct + "/" + region + "/" + scope
}

func wafv2AssocKey(acct, region, resourceArn string) string {
	return "assoc:" + acct + "/" + region + "/" + resourceArn
}

// generateWAFv2Token returns a new random UUID string for use as a LockToken or ID.
func generateWAFv2Token() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

func (p *WAFv2Plugin) createWebACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name             string                 `json:"Name"`
		Scope            string                 `json:"Scope"`
		Description      string                 `json:"Description"`
		Rules            []WAFv2Rule            `json:"Rules"`
		DefaultAction    map[string]interface{} `json:"DefaultAction"`
		VisibilityConfig map[string]interface{} `json:"VisibilityConfig"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	id := generateWAFv2Token()
	lockToken := generateWAFv2Token()
	arn := fmt.Sprintf("arn:aws:wafv2:%s:%s:regional/webacl/%s/%s", reqCtx.Region, reqCtx.AccountID, input.Name, id)

	acl := WAFv2WebACL{
		ID:               id,
		Name:             input.Name,
		ARN:              arn,
		Description:      input.Description,
		Scope:            input.Scope,
		LockToken:        lockToken,
		Rules:            input.Rules,
		DefaultAction:    input.DefaultAction,
		VisibilityConfig: input.VisibilityConfig,
		CreatedAt:        p.tc.Now(),
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}
	if acl.Rules == nil {
		acl.Rules = []WAFv2Rule{}
	}

	data, err := json.Marshal(acl)
	if err != nil {
		return nil, fmt.Errorf("wafv2 createWebACL marshal: %w", err)
	}

	goCtx := context.Background()
	key := wafv2WebACLKey(reqCtx.AccountID, reqCtx.Region, input.Scope, id)
	if err := p.state.Put(goCtx, wafv2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("wafv2 createWebACL put: %w", err)
	}
	updateStringIndex(goCtx, p.state, wafv2Namespace, wafv2WebACLIDsKey(reqCtx.AccountID, reqCtx.Region, input.Scope), id)

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"Summary": map[string]interface{}{
			"Id":          id,
			"Name":        input.Name,
			"ARN":         arn,
			"LockToken":   lockToken,
			"Description": input.Description,
		},
	})
}

func (p *WAFv2Plugin) getWebACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID    string `json:"Id"`
		Name  string `json:"Name"`
		Scope string `json:"Scope"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	acl, err := p.loadWebACLByID(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err != nil {
		return nil, err
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"WebACL":    acl,
		"LockToken": acl.LockToken,
	})
}

func (p *WAFv2Plugin) updateWebACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID               string                 `json:"Id"`
		Name             string                 `json:"Name"`
		Scope            string                 `json:"Scope"`
		LockToken        string                 `json:"LockToken"`
		Description      string                 `json:"Description"`
		Rules            []WAFv2Rule            `json:"Rules"`
		DefaultAction    map[string]interface{} `json:"DefaultAction"`
		VisibilityConfig map[string]interface{} `json:"VisibilityConfig"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	acl, err := p.loadWebACLByID(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err != nil {
		return nil, err
	}

	if acl.LockToken != input.LockToken {
		return nil, &AWSError{Code: "WAFOptimisticLockException", Message: "The optimistic lock token you provided is out of date.", HTTPStatus: http.StatusBadRequest}
	}

	// Apply updates.
	if input.Description != "" {
		acl.Description = input.Description
	}
	if input.Rules != nil {
		acl.Rules = input.Rules
	}
	if input.DefaultAction != nil {
		acl.DefaultAction = input.DefaultAction
	}
	if input.VisibilityConfig != nil {
		acl.VisibilityConfig = input.VisibilityConfig
	}
	// Regenerate lock token.
	newToken := generateWAFv2Token()
	acl.LockToken = newToken

	data, err := json.Marshal(acl)
	if err != nil {
		return nil, fmt.Errorf("wafv2 updateWebACL marshal: %w", err)
	}

	goCtx := context.Background()
	key := wafv2WebACLKey(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err := p.state.Put(goCtx, wafv2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("wafv2 updateWebACL put: %w", err)
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"NextLockToken": newToken,
	})
}

func (p *WAFv2Plugin) deleteWebACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID        string `json:"Id"`
		Name      string `json:"Name"`
		Scope     string `json:"Scope"`
		LockToken string `json:"LockToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	acl, err := p.loadWebACLByID(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err != nil {
		return nil, err
	}

	if acl.LockToken != input.LockToken {
		return nil, &AWSError{Code: "WAFOptimisticLockException", Message: "The optimistic lock token you provided is out of date.", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := wafv2WebACLKey(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err := p.state.Delete(goCtx, wafv2Namespace, key); err != nil {
		return nil, fmt.Errorf("wafv2 deleteWebACL delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, wafv2Namespace, wafv2WebACLIDsKey(reqCtx.AccountID, reqCtx.Region, input.Scope), input.ID)

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *WAFv2Plugin) listWebACLs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Scope string `json:"Scope"`
		Limit int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, wafv2Namespace, wafv2WebACLIDsKey(reqCtx.AccountID, reqCtx.Region, input.Scope))
	if err != nil {
		return nil, fmt.Errorf("wafv2 listWebACLs load index: %w", err)
	}

	summaries := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		acl, err := p.loadWebACLByID(reqCtx.AccountID, reqCtx.Region, input.Scope, id)
		if err != nil {
			continue
		}
		summaries = append(summaries, map[string]interface{}{
			"Id":          acl.ID,
			"Name":        acl.Name,
			"ARN":         acl.ARN,
			"LockToken":   acl.LockToken,
			"Description": acl.Description,
		})
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"WebACLs": summaries,
	})
}

func (p *WAFv2Plugin) associateWebACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		WebACLArn   string `json:"WebACLArn"`
		ResourceArn string `json:"ResourceArn"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ResourceArn == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := wafv2AssocKey(reqCtx.AccountID, reqCtx.Region, input.ResourceArn)
	data, err := json.Marshal(input.WebACLArn)
	if err != nil {
		return nil, fmt.Errorf("wafv2 associateWebACL marshal: %w", err)
	}
	if err := p.state.Put(goCtx, wafv2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("wafv2 associateWebACL put: %w", err)
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *WAFv2Plugin) disassociateWebACL(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ResourceArn == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := wafv2AssocKey(reqCtx.AccountID, reqCtx.Region, input.ResourceArn)
	if err := p.state.Delete(goCtx, wafv2Namespace, key); err != nil {
		return nil, fmt.Errorf("wafv2 disassociateWebACL delete: %w", err)
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *WAFv2Plugin) getWebACLForResource(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ResourceArn == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := wafv2AssocKey(reqCtx.AccountID, reqCtx.Region, input.ResourceArn)
	data, err := p.state.Get(goCtx, wafv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("wafv2 getWebACLForResource get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "WAFNonexistentItemException", Message: "No WebACL is associated with resource " + input.ResourceArn, HTTPStatus: http.StatusNotFound}
	}

	var webACLArn string
	if err := json.Unmarshal(data, &webACLArn); err != nil {
		return nil, fmt.Errorf("wafv2 getWebACLForResource unmarshal arn: %w", err)
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"WebACL": map[string]interface{}{
			"ARN": webACLArn,
		},
	})
}

func (p *WAFv2Plugin) createIPSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name        string   `json:"Name"`
		Scope       string   `json:"Scope"`
		Description string   `json:"Description"`
		IPVersion   string   `json:"IPVersion"`
		Addresses   []string `json:"Addresses"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}
	if input.IPVersion == "" {
		input.IPVersion = "IPV4"
	}
	if input.Addresses == nil {
		input.Addresses = []string{}
	}

	id := generateWAFv2Token()
	lockToken := generateWAFv2Token()
	arn := fmt.Sprintf("arn:aws:wafv2:%s:%s:regional/ipset/%s/%s", reqCtx.Region, reqCtx.AccountID, input.Name, id)

	ipset := WAFv2IPSet{
		ID:          id,
		Name:        input.Name,
		ARN:         arn,
		Description: input.Description,
		Scope:       input.Scope,
		LockToken:   lockToken,
		IPVersion:   input.IPVersion,
		Addresses:   input.Addresses,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}

	data, err := json.Marshal(ipset)
	if err != nil {
		return nil, fmt.Errorf("wafv2 createIPSet marshal: %w", err)
	}

	goCtx := context.Background()
	key := wafv2IPSetKey(reqCtx.AccountID, reqCtx.Region, input.Scope, id)
	if err := p.state.Put(goCtx, wafv2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("wafv2 createIPSet put: %w", err)
	}
	updateStringIndex(goCtx, p.state, wafv2Namespace, wafv2IPSetIDsKey(reqCtx.AccountID, reqCtx.Region, input.Scope), id)

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"Summary": map[string]interface{}{
			"Id":          id,
			"Name":        input.Name,
			"ARN":         arn,
			"LockToken":   lockToken,
			"Description": input.Description,
		},
	})
}

func (p *WAFv2Plugin) getIPSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID    string `json:"Id"`
		Name  string `json:"Name"`
		Scope string `json:"Scope"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	ipset, err := p.loadIPSetByID(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err != nil {
		return nil, err
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"IPSet":     ipset,
		"LockToken": ipset.LockToken,
	})
}

func (p *WAFv2Plugin) updateIPSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID        string   `json:"Id"`
		Name      string   `json:"Name"`
		Scope     string   `json:"Scope"`
		LockToken string   `json:"LockToken"`
		Addresses []string `json:"Addresses"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	ipset, err := p.loadIPSetByID(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err != nil {
		return nil, err
	}

	if ipset.LockToken != input.LockToken {
		return nil, &AWSError{Code: "WAFOptimisticLockException", Message: "The optimistic lock token you provided is out of date.", HTTPStatus: http.StatusBadRequest}
	}

	if input.Addresses != nil {
		ipset.Addresses = input.Addresses
	}
	newToken := generateWAFv2Token()
	ipset.LockToken = newToken

	data, err := json.Marshal(ipset)
	if err != nil {
		return nil, fmt.Errorf("wafv2 updateIPSet marshal: %w", err)
	}

	goCtx := context.Background()
	key := wafv2IPSetKey(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err := p.state.Put(goCtx, wafv2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("wafv2 updateIPSet put: %w", err)
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"NextLockToken": newToken,
	})
}

func (p *WAFv2Plugin) deleteIPSet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ID        string `json:"Id"`
		Name      string `json:"Name"`
		Scope     string `json:"Scope"`
		LockToken string `json:"LockToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	ipset, err := p.loadIPSetByID(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err != nil {
		return nil, err
	}

	if ipset.LockToken != input.LockToken {
		return nil, &AWSError{Code: "WAFOptimisticLockException", Message: "The optimistic lock token you provided is out of date.", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := wafv2IPSetKey(reqCtx.AccountID, reqCtx.Region, input.Scope, input.ID)
	if err := p.state.Delete(goCtx, wafv2Namespace, key); err != nil {
		return nil, fmt.Errorf("wafv2 deleteIPSet delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, wafv2Namespace, wafv2IPSetIDsKey(reqCtx.AccountID, reqCtx.Region, input.Scope), input.ID)

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *WAFv2Plugin) listIPSets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Scope string `json:"Scope"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	}

	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, wafv2Namespace, wafv2IPSetIDsKey(reqCtx.AccountID, reqCtx.Region, input.Scope))
	if err != nil {
		return nil, fmt.Errorf("wafv2 listIPSets load index: %w", err)
	}

	summaries := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		ipset, err := p.loadIPSetByID(reqCtx.AccountID, reqCtx.Region, input.Scope, id)
		if err != nil {
			continue
		}
		summaries = append(summaries, map[string]interface{}{
			"Id":          ipset.ID,
			"Name":        ipset.Name,
			"ARN":         ipset.ARN,
			"LockToken":   ipset.LockToken,
			"Description": ipset.Description,
		})
	}

	return wafv2JSONResponse(http.StatusOK, map[string]interface{}{
		"IPSets": summaries,
	})
}

// loadWebACLByID loads a WAFv2WebACL from state by ID or returns a not-found error.
func (p *WAFv2Plugin) loadWebACLByID(acct, region, scope, id string) (*WAFv2WebACL, error) {
	if id == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "Id is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := wafv2WebACLKey(acct, region, scope, id)
	data, err := p.state.Get(goCtx, wafv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("wafv2 loadWebACLByID get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "WAFNonexistentItemException", Message: "Web ACL with ID " + id + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var acl WAFv2WebACL
	if err := json.Unmarshal(data, &acl); err != nil {
		return nil, fmt.Errorf("wafv2 loadWebACLByID unmarshal: %w", err)
	}
	return &acl, nil
}

// loadIPSetByID loads a WAFv2IPSet from state by ID or returns a not-found error.
func (p *WAFv2Plugin) loadIPSetByID(acct, region, scope, id string) (*WAFv2IPSet, error) {
	if id == "" {
		return nil, &AWSError{Code: "WAFInvalidParameterException", Message: "Id is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := wafv2IPSetKey(acct, region, scope, id)
	data, err := p.state.Get(goCtx, wafv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("wafv2 loadIPSetByID get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "WAFNonexistentItemException", Message: "IP set with ID " + id + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var ipset WAFv2IPSet
	if err := json.Unmarshal(data, &ipset); err != nil {
		return nil, fmt.Errorf("wafv2 loadIPSetByID unmarshal: %w", err)
	}
	return &ipset, nil
}

// wafv2JSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.1.
func wafv2JSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("wafv2 json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
