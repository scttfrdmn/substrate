package emulator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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
		return nil, unknownActionError(p.Name(), req.Operation)
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

// wafv2ARN builds the ARN of a WAFv2 resource. It is the single builder for the
// whole service, for the reason #826 established: two builders are two answers,
// and one logical web ACL reported two different ARNs depending on whether the
// WAFv2 API or CloudFormation created it, because the plugin hardcoded the scope
// segment "regional" while the CloudFormation helper derived it.
//
// The scope segment is the lowercase of the Scope value, which is a REGIONAL
// reading extended to CLOUDFRONT rather than a documented fact, and the extension
// is substrate's. AWS publishes exactly one substituted example of a web-ACL ARN
// anywhere, on the AWS::WAFv2::WebACL Template Reference page —
//
//	arn:aws:wafv2:us-east-1:ExampleAccountNumber:regional/webacl/exampleWebACL/exampleWebACLExampleID
//
// — and it is a REGIONAL ACL rendering "regional". The Service Authorization
// Reference page returns an empty body, the machine-readable Service Reference
// gives only the unsubstituted "${Scope}", and API_AssociateWebACL lists the
// protectable resources' ARN formats but no web-ACL ARN. So whether a CLOUDFRONT
// ACL's segment reads "cloudfront" or something else is unverified. What this
// function guarantees is that every path agrees on one answer; which answer it is
// remains substrate's reading.
func wafv2ARN(region, accountID, scope, resourceType, name, id string) string {
	return fmt.Sprintf("arn:aws:wafv2:%s:%s:%s/%s/%s/%s",
		region, accountID, strings.ToLower(scope), resourceType, name, id)
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
			return nil, wafv2InvalidBody()
		}
	}
	if input.Name == "" {
		return nil, wafv2MissingMember("Name")
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
	}

	id := generateWAFv2Token()
	lockToken := generateWAFv2Token()
	arn := wafv2ARN(reqCtx.Region, reqCtx.AccountID, input.Scope, "webacl", input.Name, id)

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
			return nil, wafv2InvalidBody()
		}
	}
	// Scope is Required: No here alone, so an absent one keeps substrate's REGIONAL fallback
	// (a recorded divergence — the page publishes no default) while a present one is still
	// held to the two published values. See [wafv2ValidateScope].
	if input.Scope == "" {
		input.Scope = "REGIONAL"
	} else if err := wafv2ValidateScopeValue(input.Scope); err != nil {
		return nil, err
	}
	// API_GetWebACL marks ARN, Id, Name and Scope all Required: No — the ARN alone identifies a
	// web ACL, and the Name/Id/Scope triple is the alternative. Substrate models only the
	// triple, so a request supplying none of the four has addressed nothing, which is not a
	// complaint about one member (#1063).
	if input.ID == "" {
		return nil, wafv2ValidationError("the request identifies no web ACL; supply Name, Id and Scope")
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
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
	}
	// Id is Required: Yes on API_UpdateWebACL, which is why the check is here and not in
	// loadWebACLByID — see that function for the caller that disagrees.
	if input.ID == "" {
		return nil, wafv2MissingMember("Id")
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
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
	}
	// Id is Required: Yes on API_DeleteWebACL; see [WAFv2Plugin.loadWebACLByID].
	if input.ID == "" {
		return nil, wafv2MissingMember("Id")
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
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
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
			return nil, wafv2InvalidBody()
		}
	}
	if input.ResourceArn == "" {
		return nil, wafv2MissingMember("ResourceArn")
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
			return nil, wafv2InvalidBody()
		}
	}
	if input.ResourceArn == "" {
		return nil, wafv2MissingMember("ResourceArn")
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
			return nil, wafv2InvalidBody()
		}
	}
	if input.ResourceArn == "" {
		return nil, wafv2MissingMember("ResourceArn")
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
		Name             string   `json:"Name"`
		Scope            string   `json:"Scope"`
		Description      string   `json:"Description"`
		IPAddressVersion string   `json:"IPAddressVersion"`
		Addresses        []string `json:"Addresses"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateCreateIPSet(input.Name, input.Scope, input.IPAddressVersion, input.Addresses); err != nil {
		return nil, err
	}

	id := generateWAFv2Token()
	lockToken := generateWAFv2Token()
	arn := wafv2ARN(reqCtx.Region, reqCtx.AccountID, input.Scope, "ipset", input.Name, id)

	ipset := WAFv2IPSet{
		ID:               id,
		Name:             input.Name,
		ARN:              arn,
		Description:      input.Description,
		Scope:            input.Scope,
		LockToken:        lockToken,
		IPAddressVersion: input.IPAddressVersion,
		Addresses:        input.Addresses,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
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
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
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
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
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
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
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
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, wafv2InvalidBody()
		}
	}
	if err := wafv2ValidateScope(input.Scope); err != nil {
		return nil, err
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
//
// It carries no required-member check, and that is the one place in #1063 where correcting
// the code was not enough: its four callers do not agree about whether Id is required.
// API_UpdateWebACL and API_DeleteWebACL mark it Required: Yes, listWebACLs supplies an ID out
// of the index, and **API_GetWebACL marks ARN, Id, Name and Scope all Required: No**, because
// there the ARN alone identifies a web ACL and the Name/Id/Scope triple is the alternative.
// A single check here would have reported "Id is a required parameter" from the one operation
// whose page says it is not, so each caller states its own page's requirement and this
// function only resolves.
//
// An empty ID therefore reaches the lookup below and answers WAFNonexistentItemException.
// That is unreachable from the four handlers as written, all of which now refuse first.
func (p *WAFv2Plugin) loadWebACLByID(acct, region, scope, id string) (*WAFv2WebACL, error) {
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
//
// The required-member check stays here, unlike [WAFv2Plugin.loadWebACLByID]'s, because all
// four callers agree with each other: API_GetIPSet, API_UpdateIPSet and API_DeleteIPSet each
// mark Id Required: Yes, and listIPSets supplies an ID read out of the index, which is never
// empty. So one answer serves every caller and the check belongs at the one place that needs
// it (#1063).
func (p *WAFv2Plugin) loadIPSetByID(acct, region, scope, id string) (*WAFv2IPSet, error) {
	if id == "" {
		return nil, wafv2MissingMember("Id")
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
