package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SESv2Plugin emulates the Amazon Simple Email Service v2.
// It supports email and domain identity CRUD operations and stub email sending
// using the SES v2 REST/JSON API at /v2/email/... paths.
type SESv2Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "sesv2".
func (p *SESv2Plugin) Name() string { return sesv2Namespace }

// Initialize sets up the SESv2Plugin with the provided configuration.
func (p *SESv2Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SESv2Plugin.
func (p *SESv2Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an SES v2 REST/JSON request to the appropriate handler.
func (p *SESv2Plugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, identityName := parseSESv2Operation(req.Operation, req.Path)
	switch op {
	case "CreateEmailIdentity":
		return p.createEmailIdentity(ctx, req)
	case "ListEmailIdentities":
		return p.listEmailIdentities(ctx, req)
	case "GetEmailIdentity":
		return p.getEmailIdentity(ctx, req, identityName)
	case "DeleteEmailIdentity":
		return p.deleteEmailIdentity(ctx, req, identityName)
	case "SendEmail":
		return p.sendEmail(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "SESv2Plugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseSESv2Operation derives the operation name and optional identity name
// from the HTTP method and request path.
func parseSESv2Operation(method, path string) (op, identityName string) {
	// Normalise: strip trailing slash.
	path = strings.TrimRight(path, "/")

	switch {
	case path == "/v2/email/identities" && method == "POST":
		return "CreateEmailIdentity", ""
	case path == "/v2/email/identities" && method == "GET":
		return "ListEmailIdentities", ""
	case strings.HasPrefix(path, "/v2/email/identities/") && method == "GET":
		name := strings.TrimPrefix(path, "/v2/email/identities/")
		return "GetEmailIdentity", name
	case strings.HasPrefix(path, "/v2/email/identities/") && method == "DELETE":
		name := strings.TrimPrefix(path, "/v2/email/identities/")
		return "DeleteEmailIdentity", name
	case path == "/v2/email/outbound-emails" && method == "POST":
		return "SendEmail", ""
	}
	return "", ""
}

func (p *SESv2Plugin) createEmailIdentity(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		EmailIdentity string            `json:"EmailIdentity"`
		Tags          map[string]string `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.EmailIdentity == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "EmailIdentity is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := "identity:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.EmailIdentity
	existing, err := p.state.Get(goCtx, sesv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("sesv2 createEmailIdentity get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "AlreadyExistsException", Message: "Identity " + input.EmailIdentity + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	identityType := "EMAIL_ADDRESS"
	if !strings.Contains(input.EmailIdentity, "@") {
		identityType = "DOMAIN"
	}

	identity := SESv2Identity{
		IdentityName: input.EmailIdentity,
		IdentityType: identityType,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
		Tags:         input.Tags,
		CreatedAt:    p.tc.Now(),
	}

	data, err := json.Marshal(identity)
	if err != nil {
		return nil, fmt.Errorf("sesv2 createEmailIdentity marshal: %w", err)
	}
	if err := p.state.Put(goCtx, sesv2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("sesv2 createEmailIdentity put: %w", err)
	}
	updateStringIndex(goCtx, p.state, sesv2Namespace, "identity_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.EmailIdentity)

	return sesv2JSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SESv2Plugin) listEmailIdentities(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	pageSize := 100
	var input struct {
		PageSize  int    `json:"PageSize"`
		NextToken string `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
		if input.PageSize > 0 {
			pageSize = input.PageSize
		}
	}

	names, err := loadStringIndex(goCtx, p.state, sesv2Namespace, "identity_names:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("sesv2 listEmailIdentities load index: %w", err)
	}

	// Apply cursor pagination.
	startIdx := 0
	if input.NextToken != "" {
		for i, n := range names {
			if n == input.NextToken {
				startIdx = i
				break
			}
		}
	}
	page := names[startIdx:]
	var nextToken string
	if len(page) > pageSize {
		nextToken = page[pageSize]
		page = page[:pageSize]
	}

	prefix := "identity:" + reqCtx.AccountID + "/" + reqCtx.Region + "/"
	type identityEntry struct {
		IdentityName string `json:"IdentityName"`
		IdentityType string `json:"IdentityType"`
	}
	var entries []identityEntry
	for _, name := range page {
		data, err := p.state.Get(goCtx, sesv2Namespace, prefix+name)
		if err != nil || data == nil {
			continue
		}
		var id SESv2Identity
		if json.Unmarshal(data, &id) != nil {
			continue
		}
		entries = append(entries, identityEntry{IdentityName: id.IdentityName, IdentityType: id.IdentityType})
	}
	if entries == nil {
		entries = []identityEntry{}
	}

	result := map[string]interface{}{
		"EmailIdentities": entries,
	}
	if nextToken != "" {
		result["NextToken"] = nextToken
	}
	return sesv2JSONResponse(http.StatusOK, result)
}

func (p *SESv2Plugin) getEmailIdentity(reqCtx *RequestContext, _ *AWSRequest, identityName string) (*AWSResponse, error) {
	if identityName == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "identity name is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "identity:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + identityName
	data, err := p.state.Get(goCtx, sesv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("sesv2 getEmailIdentity get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Identity " + identityName + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var identity SESv2Identity
	if err := json.Unmarshal(data, &identity); err != nil {
		return nil, fmt.Errorf("sesv2 getEmailIdentity unmarshal: %w", err)
	}
	return sesv2JSONResponse(http.StatusOK, identity)
}

func (p *SESv2Plugin) deleteEmailIdentity(reqCtx *RequestContext, _ *AWSRequest, identityName string) (*AWSResponse, error) {
	if identityName == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "identity name is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "identity:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + identityName

	existing, err := p.state.Get(goCtx, sesv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("sesv2 deleteEmailIdentity get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Identity " + identityName + " does not exist.", HTTPStatus: http.StatusNotFound}
	}

	if err := p.state.Delete(goCtx, sesv2Namespace, key); err != nil {
		return nil, fmt.Errorf("sesv2 deleteEmailIdentity delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, sesv2Namespace, "identity_names:"+reqCtx.AccountID+"/"+reqCtx.Region, identityName)
	return sesv2JSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SESv2Plugin) sendEmail(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	messageID := fmt.Sprintf("msg-%d", p.tc.Now().UnixNano())
	return sesv2JSONResponse(http.StatusOK, map[string]string{
		"MessageId": messageID,
	})
}

// sesv2JSONResponse serializes v to JSON and returns an AWSResponse.
func sesv2JSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sesv2 json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
