package emulator

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
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- Operations ------------------------------------------------------------

func (p *STSPlugin) getCallerIdentity(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	account := ctx.AccountID
	// The account root's unique ID *is* the account ID, per AWS's identifiers reference —
	// so an unsigned request and one signed with a credential that resolves to no
	// principal both report the documented value here without any lookup.
	userID := ctx.AccountID
	arn := fmt.Sprintf("arn:aws:iam::%s:root", ctx.AccountID)

	if ctx.Principal != nil {
		arn = ctx.Principal.ARN
		userID = p.callerUniqueID(context.Background(), ctx.Principal)
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

// callerUniqueID returns the unique ID GetCallerIdentity reports for a principal.
//
// AWS's identifiers reference gives the three forms and names GetCallerIdentity as the way
// to obtain one: an IAM user's `AIDA…`, an assumed role's `AROA…:<role-session-name>`, and
// the account ID for the account root. Substrate reported the caller's *friendly name*
// instead, which is a shape AWS never produces, and disagreed with its own
// `aws:userid` — published from the same field since #771 (#805).
//
// The recorded value is preferred, on #745's record-don't-derive rule: [resolvePrincipal]
// copies it from the access key's or the session's own record, so no lookup is needed on the
// common path. The read below is the fallback for a credential minted before #771, whose
// record has the field empty; GetCallerIdentity is not a hot path, and a stale answer here
// would be worse than a Get.
func (p *STSPlugin) callerUniqueID(ctx context.Context, principal *Principal) string {
	if principal.UserID != "" {
		return principal.UserID
	}

	entity, ok := iamEntityForPrincipalARN(principal.ARN)
	if !ok {
		return stsCallerNameFallback(principal.ARN)
	}
	recorded := p.entityUniqueID(ctx, entity)
	if recorded == "" {
		return stsCallerNameFallback(principal.ARN)
	}

	// An assumed role's unique ID is the role's, joined to the session name the caller
	// chose — "{role-id}:{caller-specified-role-name}" — and the session name is the only
	// part of it the ARN carries. assumeRole composes the same string when it mints a
	// session; this rebuilds it for a session minted before it did.
	if entityType, nameWithPath := parsePrincipalARN(principal.ARN); entityType == "assumed-role" {
		if slash := strings.IndexByte(nameWithPath, '/'); slash >= 0 {
			return recorded + ":" + nameWithPath[slash+1:]
		}
	}
	return recorded
}

// entityUniqueID returns the `AIDA…` or `AROA…` recorded on an IAM entity, or "" when there
// is no such record and when the read fails.
//
// One key serves both because the two fields cannot both be set: a record is a user or a
// role, and [iamEntityKey] already keys them apart by kind.
func (p *STSPlugin) entityUniqueID(ctx context.Context, entity iamEntity) string {
	raw, err := p.state.Get(ctx, iamNamespace, iamEntityKey(entity.Account, entity.Kind, entity.Name))
	if err != nil || raw == nil {
		return ""
	}
	var record struct {
		UserID string `json:"UserId"`
		RoleID string `json:"RoleId"`
	}
	if unmarshalErr := json.Unmarshal(raw, &record); unmarshalErr != nil {
		return ""
	}
	if record.UserID != "" {
		return record.UserID
	}
	return record.RoleID
}

// stsCallerNameFallback returns the caller's friendly name, which is what GetCallerIdentity
// reports when there is no unique ID to be had.
//
// That is substrate's choice, not AWS's: on AWS every caller has a unique ID, so the case
// cannot arise there. It arises here for a principal that resolves to no IAM entity — a
// CloudFormation service-role principal built from a stack's `RoleARN` whose role was
// deleted, say. Reporting the name keeps the value substrate published before #805 rather
// than regressing to an empty member, which a consumer asserting on `UserId` could not
// distinguish from a bug.
func stsCallerNameFallback(principalARN string) string {
	entityType, nameWithPath := parsePrincipalARN(principalARN)
	if entityType == "user" {
		// The friendly name: an entity ARN's name component carries the path (#801).
		return iamFriendlyName(nameWithPath)
	}
	// An assumed-role ARN carries no path, so <role>/<session> is left whole.
	return nameWithPath
}

func (p *STSPlugin) assumeRole(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	roleARN := req.Params["RoleArn"]
	sessionName := req.Params["RoleSessionName"]
	durationStr := req.Params["DurationSeconds"]
	externalID := req.Params["ExternalId"]

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
	// ExternalId's documented bounds. An absent one is legal — it is only required
	// when a trust policy conditions on it, which the evaluation below decides.
	if externalID != "" && (len(externalID) < 2 || len(externalID) > 1224) {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    "ExternalId must be between 2 and 1224 characters",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// Look up the role in IAM state, by the name the record is keyed under: the friendly
	// name, with any path dropped. A role ARN carries its path —
	// arn:aws:iam::123:role/service-role/CfnRole is what AWS's console creates — and
	// reading the whole component as the name made every such role unassumable, a hard
	// NoSuchEntity for a perfectly valid ARN (#801).
	_, nameWithPath := parsePrincipalARN(roleARN)
	roleName := iamFriendlyName(nameWithPath)
	if roleName == "" {
		// Not an ARN this parse understands at all; fall back to its last segment.
		if idx := strings.LastIndexByte(roleARN, '/'); idx >= 0 {
			roleName = roleARN[idx+1:]
		}
	}

	goCtx := context.Background()

	raw, err := p.state.Get(goCtx, iamNamespace, iamRoleKey(arnAccountID(roleARN), roleName))
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

	// The trust policy decides *before* a credential exists. Minting first and
	// checking after would leave a usable session in state behind a 403.
	if err := p.checkTrustPolicy(ctx, role, roleARN, externalID); err != nil {
		return nil, err
	}

	now := p.now()
	expiry := now.Add(time.Duration(duration) * time.Second)

	// AWS's aws:userid for an assumed role: "{role-id}:{caller-specified-role-name}
	// where role-id is the unique id of the role and the caller-specified-role-name is
	// specified by the RoleSessionName parameter". The response publishes it as
	// AssumedRoleId below; the session record carries it so that a request signed with
	// these credentials can be authorized against it (#771).
	assumedRoleID := role.RoleID + ":" + sessionName

	creds := STSSessionCredentials{
		AccessKeyID:     generateIAMID("ASIA"),
		SecretAccessKey: stsGenerateSecret(),
		SessionToken:    stsGenerateToken(),
		Expiration:      expiry,
		PrincipalARN:    fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s/%s", ctx.AccountID, roleName, sessionName),
		AccountID:       ctx.AccountID,
		PrincipalID:     assumedRoleID,
	}

	credRaw, err := json.Marshal(creds)
	if err != nil {
		return nil, fmt.Errorf("marshal session credentials: %w", err)
	}
	if err := p.state.Put(goCtx, stsNamespace, "session:"+creds.AccessKeyID, credRaw); err != nil {
		return nil, fmt.Errorf("store session credentials: %w", err)
	}

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

// checkTrustPolicy evaluates a role's trust policy against the caller, returning
// an *AWSError when the policy refuses them and nil when it admits them or is not
// enforced.
//
// A trust policy is a *resource* policy: it answers "who may become this role",
// which is a different question from the permissions policy [AuthController.CheckAccess]
// already evaluates ("what may the role do"). Both gates apply to AssumeRole, and
// this is the second one — a caller must be allowed to call sts:AssumeRole *and*
// be admitted by the role they name. Without this, sts:ExternalId — the
// confused-deputy defense, whose whole purpose is refusing a caller who cannot
// present a shared secret — did nothing at all (#593).
//
// Enforcement is opt-in by writing a trust policy, mirroring the rule authz
// already follows for principals: a role with no statements is "not configured"
// rather than "trusts nobody". Real IAM requires a trust policy at CreateRole and
// would read an empty document as an implicit deny, but substrate permits creating
// a role without one, so denying here would refuse roles that cannot exist on AWS
// and would break every caller who never wrote a policy. A nil principal is
// skipped for the same reason CheckAccess skips one: an unauthenticated request
// resolves to no ARN, so there is nothing for a Principal element to be true of.
func (p *STSPlugin) checkTrustPolicy(ctx *RequestContext, role IAMRole, roleARN, externalID string) error {
	if len(role.AssumeRolePolicyDocument.Statement) == 0 {
		p.logger.Debug("sts: role has no trust policy, not enforced", "role", roleARN)
		return nil
	}
	if ctx.Principal == nil {
		p.logger.Debug("sts: unauthenticated caller, trust policy not enforced", "role", roleARN)
		return nil
	}

	// A condition key absent from the context is not the same as one set to "":
	// conditionMatches reads a missing key as the empty string, so a policy
	// requiring StringEquals sts:ExternalId correctly fails when none was sent.
	condCtx := make(map[string]string, 2)
	if externalID != "" {
		condCtx["sts:ExternalId"] = externalID
	}
	// The principal asking to assume the role. A trust policy is the likeliest place in
	// AWS for a condition on aws:PrincipalArn — it is how an account-wide Principal is
	// narrowed to particular callers — and with the key absent, a negated operator over it
	// was satisfied by every caller, exempting nobody and, on an Allow, admitting all
	// (#714). See [authzPrincipalContext].
	authzPrincipalContext(condCtx, ctx.Principal)

	// Resource is empty because a trust policy carries no Resource element — the
	// role it is attached to *is* the resource. resourceMatches treats an empty
	// resource as a match, so a statement stands or falls on its principal,
	// action and conditions.
	result := Evaluate([]PolicyDocument{role.AssumeRolePolicyDocument}, EvaluationRequest{
		Principal: ctx.Principal.ARN,
		Action:    "sts:AssumeRole",
		Resource:  "",
		Context:   condCtx,
	})
	if result.Decision == DecisionAllow {
		return nil
	}

	// AWS reports both refusals under the same code and distinguishes them only in
	// the message, which is the distinction substrate's two deny decisions already
	// draw. The code is AccessDenied — observed from the API, which documents no
	// access-denied error for AssumeRole at all.
	msg := fmt.Sprintf(
		"User: %s is not authorized to perform: sts:AssumeRole because no role trust policy allows the sts:AssumeRole action",
		ctx.Principal.ARN)
	if result.Decision == DecisionDeny {
		msg = fmt.Sprintf(
			"User: %s is not authorized to perform: sts:AssumeRole with an explicit deny in the role trust policy",
			ctx.Principal.ARN)
	}
	p.logger.Debug("sts: trust policy denied AssumeRole",
		"role", roleARN, "principal", ctx.Principal.ARN, "decision", result.Decision)
	return &AWSError{
		Code:       "AccessDenied",
		Message:    msg,
		HTTPStatus: http.StatusForbidden,
	}
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
		// GetSessionToken returns credentials for the *same* IAM user, so the session
		// carries that user's name forward and `aws:username` keeps its value across the
		// call. assumeRole deliberately sets no UserName (#745).
		creds.UserName = ctx.Principal.UserName
		// Same reasoning for the unique ID: the principal does not change, so
		// aws:userid stays the calling user's AIDA… rather than becoming a session
		// pairing. Empty when the caller has none, in which case the session publishes
		// no aws:userid either (#771).
		creds.PrincipalID = ctx.Principal.UserID
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

	// UserName is the IAM user name behind the session, set for GetSessionToken and
	// empty for AssumeRole.
	//
	// AWS publishes `aws:username` for a GetSessionToken session — its principal is
	// the calling IAM user, unchanged — and publishes none for an assumed role. The
	// distinction cannot be recovered from PrincipalARN, whose GetSessionToken form is
	// the user's own ARN and whose AssumeRole form is an assumed-role ARN, so it is
	// recorded when the session is minted (#745).
	UserName string `json:"UserName,omitempty"`

	// PrincipalID is the session's `aws:userid` value: `<role-id>:<session-name>` for
	// AssumeRole, and the calling user's `AIDA…` for GetSessionToken, whose principal
	// is that user unchanged.
	//
	// AssumeRole already computes the pairing for the AssumedRoleId member of its
	// response; it is recorded here because it cannot be recovered from the session
	// afterwards — PrincipalARN carries the role's *name*, and the role record it
	// would have to be read back from may since have been deleted or replaced with a
	// new ID (#771).
	PrincipalID string `json:"PrincipalId,omitempty"`
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
