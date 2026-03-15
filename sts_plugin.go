package substrate

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// stsNamespace is the state namespace used by STSPlugin.
const stsNamespace = "sts"

// stsDefaultSessionDuration is the default session duration in seconds (1 hour).
const stsDefaultSessionDuration = 3600

// STSPlugin emulates the AWS Security Token Service (STS) API.
// It implements the [Plugin] interface and handles query-protocol STS requests
// routed via the sts.amazonaws.com host.
type STSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "sts".
func (p *STSPlugin) Name() string { return "sts" }

// Initialize stores the provided configuration and optionally a TimeController
// from Options["time_controller"].
func (p *STSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"]; ok {
		if typed, ok := tc.(*TimeController); ok {
			p.tc = typed
		}
	}
	return nil
}

// Shutdown is a no-op for STSPlugin.
func (p *STSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches the STS API operation to the appropriate handler.
func (p *STSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "GetCallerIdentity":
		return p.getCallerIdentity(ctx, req)
	case "AssumeRole":
		return p.assumeRole(ctx, req)
	case "GetSessionToken":
		return p.getSessionToken(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("Could not find operation %s", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Operations ------------------------------------------------------------

func (p *STSPlugin) getCallerIdentity(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	userID := ctx.AccountID
	account := ctx.AccountID
	arn := fmt.Sprintf("arn:aws:iam::%s:root", ctx.AccountID)

	if ctx.Principal != nil {
		arn = ctx.Principal.ARN
		entityType, entityName := parsePrincipalARN(ctx.Principal.ARN)
		switch entityType {
		case "user":
			userID = entityName
		case "assumed-role":
			userID = entityName
		default:
			userID = entityName
		}
	}

	type result struct {
		UserID  string `xml:"UserId"`
		Account string `xml:"Account"`
		Arn     string `xml:"Arn"`
	}
	type response struct {
		XMLName                 xml.Name         `xml:"GetCallerIdentityResponse"`
		Xmlns                   string           `xml:"xmlns,attr"`
		GetCallerIdentityResult result           `xml:"GetCallerIdentityResult"`
		ResponseMetadata        responseMetadata `xml:"ResponseMetadata"`
	}

	resp := response{
		Xmlns: "https://sts.amazonaws.com/doc/2011-06-15/",
		GetCallerIdentityResult: result{
			UserID:  userID,
			Account: account,
			Arn:     arn,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	}

	return stsXMLResponse(http.StatusOK, resp)
}

func (p *STSPlugin) assumeRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	roleARN := req.Params["RoleArn"]
	sessionName := req.Params["RoleSessionName"]
	durationStr := req.Params["DurationSeconds"]

	if roleARN == "" {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "RoleArn is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if sessionName == "" {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "RoleSessionName is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	duration := stsDefaultSessionDuration
	if durationStr != "" {
		d, err := strconv.Atoi(durationStr)
		if err != nil {
			return nil, &AWSError{
				Code:       "ValidationError",
				Message:    "DurationSeconds must be an integer",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		duration = d
	}
	if duration < 900 || duration > 43200 {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "DurationSeconds must be between 900 and 43200",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// Look up the role in IAM state.
	_, roleName := parsePrincipalARN(roleARN)
	if roleName == "" {
		// Try parsing last segment after "/".
		if idx := strings.LastIndexByte(roleARN, '/'); idx >= 0 {
			roleName = roleARN[idx+1:]
		}
	}

	goCtx := context.Background()

	raw, err := p.state.Get(goCtx, iamNamespace, "role:"+roleName)
	if err != nil {
		return nil, fmt.Errorf("get role: %w", err)
	}
	if raw == nil {
		return nil, &AWSError{
			Code:       "NoSuchEntityException",
			Message:    fmt.Sprintf("The role %s cannot be found.", roleARN),
			HTTPStatus: http.StatusNotFound,
		}
	}

	var role IAMRole
	if err := json.Unmarshal(raw, &role); err != nil {
		return nil, fmt.Errorf("unmarshal role: %w", err)
	}

	now := p.now()
	expiry := now.Add(time.Duration(duration) * time.Second)

	creds := STSSessionCredentials{
		AccessKeyID:     generateIAMID("ASIA"),
		SecretAccessKey: stsGenerateSecret(),
		SessionToken:    stsGenerateToken(),
		Expiration:      expiry,
		PrincipalARN:    fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s/%s", ctx.AccountID, roleName, sessionName),
		AccountID:       ctx.AccountID,
	}

	credRaw, err := json.Marshal(creds)
	if err != nil {
		return nil, fmt.Errorf("marshal session credentials: %w", err)
	}
	if err := p.state.Put(goCtx, stsNamespace, "session:"+creds.AccessKeyID, credRaw); err != nil {
		return nil, fmt.Errorf("store session credentials: %w", err)
	}

	assumedRoleID := role.RoleID + ":" + sessionName
	assumedRoleARN := fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s/%s", ctx.AccountID, roleName, sessionName)

	type xmlCreds struct {
		AccessKeyID     string    `xml:"AccessKeyId"`
		SecretAccessKey string    `xml:"SecretAccessKey"`
		SessionToken    string    `xml:"SessionToken"`
		Expiration      time.Time `xml:"Expiration"`
	}
	type assumedRoleUser struct {
		AssumedRoleID string `xml:"AssumedRoleId"`
		Arn           string `xml:"Arn"`
	}
	type assumeResult struct {
		Credentials     xmlCreds        `xml:"Credentials"`
		AssumedRoleUser assumedRoleUser `xml:"AssumedRoleUser"`
	}
	type response struct {
		XMLName          xml.Name         `xml:"AssumeRoleResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		AssumeRoleResult assumeResult     `xml:"AssumeRoleResult"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}

	resp := response{
		Xmlns: "https://sts.amazonaws.com/doc/2011-06-15/",
		AssumeRoleResult: assumeResult{
			Credentials: xmlCreds{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      creds.Expiration,
			},
			AssumedRoleUser: assumedRoleUser{
				AssumedRoleID: assumedRoleID,
				Arn:           assumedRoleARN,
			},
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	}

	return stsXMLResponse(http.StatusOK, resp)
}

func (p *STSPlugin) getSessionToken(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	durationStr := req.Params["DurationSeconds"]

	duration := stsDefaultSessionDuration
	if durationStr != "" {
		d, err := strconv.Atoi(durationStr)
		if err != nil {
			return nil, &AWSError{
				Code:       "ValidationError",
				Message:    "DurationSeconds must be an integer",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		duration = d
	}
	if duration < 900 || duration > 129600 {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "DurationSeconds must be between 900 and 129600",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	now := p.now()
	expiry := now.Add(time.Duration(duration) * time.Second)

	creds := STSSessionCredentials{
		AccessKeyID:     generateIAMID("ASIA"),
		SecretAccessKey: stsGenerateSecret(),
		SessionToken:    stsGenerateToken(),
		Expiration:      expiry,
		AccountID:       ctx.AccountID,
	}
	if ctx.Principal != nil {
		creds.PrincipalARN = ctx.Principal.ARN
	}

	goCtx := context.Background()

	credRaw, err := json.Marshal(creds)
	if err != nil {
		return nil, fmt.Errorf("marshal session credentials: %w", err)
	}
	if err := p.state.Put(goCtx, stsNamespace, "session:"+creds.AccessKeyID, credRaw); err != nil {
		return nil, fmt.Errorf("store session credentials: %w", err)
	}

	type xmlCreds struct {
		AccessKeyID     string    `xml:"AccessKeyId"`
		SecretAccessKey string    `xml:"SecretAccessKey"`
		SessionToken    string    `xml:"SessionToken"`
		Expiration      time.Time `xml:"Expiration"`
	}
	type sessionResult struct {
		Credentials xmlCreds `xml:"Credentials"`
	}
	type response struct {
		XMLName               xml.Name         `xml:"GetSessionTokenResponse"`
		Xmlns                 string           `xml:"xmlns,attr"`
		GetSessionTokenResult sessionResult    `xml:"GetSessionTokenResult"`
		ResponseMetadata      responseMetadata `xml:"ResponseMetadata"`
	}

	resp := response{
		Xmlns: "https://sts.amazonaws.com/doc/2011-06-15/",
		GetSessionTokenResult: sessionResult{
			Credentials: xmlCreds{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      creds.Expiration,
			},
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	}

	return stsXMLResponse(http.StatusOK, resp)
}

// --- Internal types --------------------------------------------------------

// STSSessionCredentials stores the credential data for an assumed role or
// session token, persisted in state for subsequent request authentication.
type STSSessionCredentials struct {
	AccessKeyID     string    `json:"AccessKeyId"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	SessionToken    string    `json:"SessionToken"`
	Expiration      time.Time `json:"Expiration"`
	PrincipalARN    string    `json:"PrincipalArn"`
	AccountID       string    `json:"AccountId"`
}

// responseMetadata is the XML response metadata included in all STS responses.
type responseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// --- Helpers ---------------------------------------------------------------

// now returns the current time from the injected TimeController, or
// time.Now() if no controller is set.
func (p *STSPlugin) now() time.Time {
	if p.tc != nil {
		return p.tc.Now()
	}
	return time.Now().UTC()
}

// stsXMLResponse serializes v to XML and wraps it in an AWSResponse.
func stsXMLResponse(status int, v any) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal STS response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// stsGenerateSecret generates a 40-character secret access key.
func stsGenerateSecret() string {
	b := make([]byte, 30)
	if _, err := cryptorand.Read(b); err != nil {
		panic(fmt.Sprintf("stsGenerateSecret: crypto/rand read: %v", err))
	}
	return base64.StdEncoding.EncodeToString(b)[:40]
}

// stsGenerateToken generates a session token string.
func stsGenerateToken() string {
	b := make([]byte, 96)
	if _, err := cryptorand.Read(b); err != nil {
		panic(fmt.Sprintf("stsGenerateToken: crypto/rand read: %v", err))
	}
	return base64.StdEncoding.EncodeToString(b)
}
