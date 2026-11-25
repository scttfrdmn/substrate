package emulator

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // AWS defines NameQualifier as a SHA-1 digest; it is not used for security.
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Federated assumes: AssumeRoleWithWebIdentity (OpenID Connect) and AssumeRoleWithSAML.
//
// Substrate verifies the token's *claims* and nothing it would have to fetch. A web identity
// token's payload is decoded and its signature is not checked; a SAML assertion is parsed and its
// signature is not checked. What is checked is what a test can arrange and assert on:
//
//   - the token names a provider registered in the role's account (OIDC `iss`; SAML PrincipalArn,
//     and an assertion Issuer the provider's metadata declares);
//   - the OIDC `aud` is one of the provider's client IDs, and a SAML assertion's Audience is AWS;
//   - `nbf`/`exp`, or the assertion's NotBefore/NotOnOrAfter, bracket the simulated clock;
//   - a SAML assertion's Role attribute pairs this role with this provider;
//   - the role's trust policy admits the provider as a `Federated` principal for the action, with
//     the provider's condition keys (`<issuer>:sub`, `<issuer>:aud`, `saml:aud`, …) in context, and
//     allows sts:TagSession and sts:SetSourceIdentity when the token passes session tags or a
//     source identity;
//   - DurationSeconds is within the role's MaxSessionDuration.
//
// Everything that depends on the identity provider itself — a signature or thumbprint that does
// not verify, a claim the provider rejects, a provider that cannot be reached — is seedable through
// `POST /v1/sts/federation-outcome`, keyed by provider ARN or "*".

const (
	stsWebIdentityAction = "sts:AssumeRoleWithWebIdentity"
	stsSAMLAction        = "sts:AssumeRoleWithSAML"
)

// stsCtrlNamespace is the state namespace for STS control-plane seeds.
const stsCtrlNamespace = "sts-ctrl"

// stsCtrlFederationOutcomeKey returns the state key for a seeded federation outcome, keyed by
// identity-provider ARN or "*".
func stsCtrlFederationOutcomeKey(provider string) string {
	return "federation_outcome:" + provider
}

// stsFederationOutcomes are the errors a federation seed may name, with their HTTP statuses
// from the AssumeRoleWithWebIdentity and AssumeRoleWithSAML pages.
var stsFederationOutcomes = map[string]int{
	"InvalidIdentityToken":  http.StatusBadRequest,
	"IDPRejectedClaim":      http.StatusForbidden,
	"IDPCommunicationError": http.StatusBadRequest,
}

// STSFederationSeed is a seeded federation outcome: the error the next federated assumes through
// a provider answer, and how many of them.
type STSFederationSeed struct {
	// Provider is the identity provider's ARN, or "*".
	Provider string `json:"provider"`

	// Error is InvalidIdentityToken, IDPRejectedClaim or IDPCommunicationError.
	Error string `json:"error"`

	// Message replaces the error's default message when set.
	Message string `json:"message,omitempty"`

	// Count is how many assumes the seed answers before it clears itself; 0 holds until cleared.
	Count int `json:"count,omitempty"`
}

// stsRoleSessionNamePattern is RoleSessionName's documented pattern and length.
var stsRoleSessionNamePattern = regexp.MustCompile(`^[\w+=,.@-]{2,64}$`)

// stsSourceIdentityPattern is SourceIdentity's documented pattern and length.
var stsSourceIdentityPattern = regexp.MustCompile(`^[\w+=,.@-]{2,256}$`)

// stsFederationError is an STS error with the Query protocol's code and status.
func stsFederationError(code, message string, status int) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: status}
}

// stsNotAuthorized is the refusal a federated assume answers when the role cannot be assumed: it
// does not exist, the SAML assertion does not name it, or its trust policy does not admit the
// provider (or does not allow sts:TagSession or sts:SetSourceIdentity when they are needed). The
// wording is AWS's — documented for SAML in the IAM troubleshooting guide ("Not authorized to
// perform sts:AssumeRoleWithSAML", AccessDenied/403) and observed for web identity.
func stsNotAuthorized(action string) *AWSError {
	return stsFederationError("AccessDenied", "Not authorized to perform "+action, http.StatusForbidden)
}

// stsDurationParam reads DurationSeconds, defaulting to def, within the documented 900–43200.
func stsDurationParam(req *AWSRequest, def int) (int, *AWSError) {
	s := req.Params["DurationSeconds"]
	if s == "" {
		return def, nil
	}
	d, err := strconv.Atoi(s)
	if err != nil {
		return 0, stsFederationError("ValidationError", "DurationSeconds must be an integer", http.StatusBadRequest)
	}
	if d < 900 || d > 43200 {
		return 0, stsFederationError("ValidationError", "DurationSeconds must be between 900 and 43200",
			http.StatusBadRequest)
	}
	return d, nil
}

// stsRequire refuses a request missing any of the named parameters.
func stsRequire(req *AWSRequest, names ...string) *AWSError {
	for _, name := range names {
		if req.Params[name] == "" {
			return stsFederationError("ValidationError", name+" is required", http.StatusBadRequest)
		}
	}
	return nil
}

// stsSessionExtras is what a session carries beyond its identity: the session policies that
// narrow it, and a federated session's tags and source identity.
type stsSessionExtras struct {
	policies       []PolicyDocument
	tags           map[string]string
	transitiveKeys []string
	sourceIdentity string
}

// stsSessionPolicyMaxChars and stsSessionPolicyMaxARNs are the documented session-policy limits:
// "The plaintext that you use for both inline and managed session policies can't exceed 2,048
// characters", and "You can provide up to 10 managed policy ARNs".
const (
	stsSessionPolicyMaxChars = 2048
	stsSessionPolicyMaxARNs  = 10
)

// stsSessionPolicies reads an AssumeRole* call's session policies — the inline Policy and the
// managed policies PolicyArns names — as documents. The session they are minted into may do only
// what both the role's policies and these allow; [authzSessionPolicyCheck] applies them.
//
// A customer managed policy ARN that names no policy is refused with MalformedPolicyDocument, in the
// wording AWS returns (observed through the AWS CLI). An AWS managed policy outside substrate's
// bundled catalog is accepted and not applied. The other messages are substrate's own.
func (p *STSPlugin) stsSessionPolicies(goCtx context.Context, req *AWSRequest) ([]PolicyDocument, *AWSError, error) {
	text := req.Params["Policy"]
	var arns []string
	for _, m := range iamMemberStructs(req.Params, "PolicyArns") {
		arns = append(arns, m["arn"])
	}
	if len(text) > stsSessionPolicyMaxChars {
		return nil, stsFederationError("ValidationError",
			fmt.Sprintf("Policy must be at most %d characters.", stsSessionPolicyMaxChars), http.StatusBadRequest), nil
	}
	if len(arns) > stsSessionPolicyMaxARNs {
		return nil, stsFederationError("ValidationError",
			fmt.Sprintf("PolicyArns can name at most %d policies.", stsSessionPolicyMaxARNs), http.StatusBadRequest), nil
	}
	var docs []PolicyDocument
	if text != "" {
		var doc PolicyDocument
		if err := json.Unmarshal([]byte(text), &doc); err != nil || len(doc.Statement) == 0 {
			return nil, stsFederationError("MalformedPolicyDocument",
				"The session policy is not a valid policy document.", http.StatusBadRequest), nil
		}
		docs = append(docs, doc)
	}
	for _, arn := range arns {
		if mp, ok := GetManagedPolicy(arn); ok {
			docs = append(docs, mp.Document)
			continue
		}
		// An AWS managed policy substrate does not bundle is valid on AWS, so it is accepted —
		// and, with no document to read, it neither narrows the session nor fails the call. Only
		// the catalog bundled in iam_managed.go narrows a session.
		if strings.HasPrefix(arn, "arn:aws:iam::aws:policy/") {
			p.logger.Debug("sts: session policy names an unbundled AWS managed policy; not applied", "arn", arn)
			continue
		}
		raw, err := p.state.Get(goCtx, iamNamespace, iamPolicyKey(arn))
		if err != nil {
			return nil, nil, fmt.Errorf("get session policy: %w", err)
		}
		var pol IAMPolicy
		if raw == nil || json.Unmarshal(raw, &pol) != nil || arn == "" {
			return nil, stsFederationError("MalformedPolicyDocument",
				fmt.Sprintf("Policy %s does not exist or is not attachable.", arn), http.StatusBadRequest), nil
		}
		docs = append(docs, pol.Document)
	}
	return docs, nil, nil
}

// stsTag is one session tag as a request or token passed it.
type stsTag struct{ key, value string }

// stsSessionTagMap validates session tags and returns them as a map, or nil for none.
//
// The limits are documented: "You can pass a maximum of 50 session tags", keys up to 128 characters
// and values up to 256; that message is substrate's own and the code is refusalCode. Keys that
// repeat regardless of case are refused with InvalidParameterValue in IAM's wording, "Duplicate tag
// keys found. Please note that Tag keys are case insensitive.", because "New session tags override
// existing … session tags with the same tag key, regardless of character case" leaves two of them
// no defined winner.
func stsSessionTagMap(tags []stsTag, refusalCode string) (map[string]string, *AWSError) {
	if len(tags) == 0 {
		return nil, nil
	}
	if len(tags) > 50 {
		return nil, stsFederationError(refusalCode,
			"Session tags must number at most 50, with keys of 1 to 128 characters and values of at most 256.",
			http.StatusBadRequest)
	}
	out := make(map[string]string, len(tags))
	seen := make(map[string]bool, len(tags))
	for _, t := range tags {
		if t.key == "" || len(t.key) > 128 || len(t.value) > 256 {
			return nil, stsFederationError(refusalCode,
				"Session tags must number at most 50, with keys of 1 to 128 characters and values of at most 256.",
				http.StatusBadRequest)
		}
		folded := strings.ToLower(t.key)
		if seen[folded] {
			return nil, stsFederationError("InvalidParameterValue",
				"Duplicate tag keys found. Please note that Tag keys are case insensitive.", http.StatusBadRequest)
		}
		seen[folded] = true
		out[t.key] = t.value
	}
	return out, nil
}

// stsValidateSourceIdentity checks a source identity in the IAM troubleshooting guide's wording.
func stsValidateSourceIdentity(si string) *AWSError {
	if si == "" {
		return nil
	}
	if !stsSourceIdentityPattern.MatchString(si) || strings.HasPrefix(strings.ToLower(si), "aws:") {
		return stsFederationError("InvalidIdentityToken",
			`Source Identity must match [a-zA-Z_0-9+=,.@-]{2,256} and not begin with "aws:"`, http.StatusBadRequest)
	}
	return nil
}

// stsMergeSessionTags returns role tags with session tags over them; a session tag replaces a role
// tag of the same key "regardless of character case", per the IAM User Guide.
func stsMergeSessionTags(roleTags, sessionTags map[string]string) map[string]string {
	if len(sessionTags) == 0 {
		return roleTags
	}
	out := make(map[string]string, len(roleTags)+len(sessionTags))
	byFold := make(map[string]string, len(roleTags))
	for _, k := range sortedStringKeys(roleTags) {
		out[k] = roleTags[k]
		byFold[strings.ToLower(k)] = k
	}
	for _, k := range sortedStringKeys(sessionTags) {
		if existing, ok := byFold[strings.ToLower(k)]; ok {
			delete(out, existing)
		}
		out[k] = sessionTags[k]
	}
	return out
}

// federationSeed returns the error seeded for provider (exact match first, then "*"), or nil, and
// counts the use against a seed with a Count. defaultMessage is used when the seed carries none.
func (p *STSPlugin) federationSeed(goCtx context.Context, provider string, allowed []string,
	defaultMessage map[string]string) (*AWSError, error) {
	p.seedMu.Lock()
	defer p.seedMu.Unlock()
	for _, key := range []string{provider, "*"} {
		raw, err := p.state.Get(goCtx, stsCtrlNamespace, stsCtrlFederationOutcomeKey(key))
		if err != nil {
			return nil, fmt.Errorf("get federation seed: %w", err)
		}
		if raw == nil {
			continue
		}
		var seed STSFederationSeed
		if err := json.Unmarshal(raw, &seed); err != nil {
			return nil, fmt.Errorf("unmarshal federation seed: %w", err)
		}
		if !slices.Contains(allowed, seed.Error) {
			continue
		}
		if seed.Count > 0 {
			seed.Count--
			if seed.Count == 0 {
				if err := p.state.Delete(goCtx, stsCtrlNamespace, stsCtrlFederationOutcomeKey(key)); err != nil {
					return nil, fmt.Errorf("delete federation seed: %w", err)
				}
			} else {
				out, err := json.Marshal(seed)
				if err != nil {
					return nil, fmt.Errorf("marshal federation seed: %w", err)
				}
				if err := p.state.Put(goCtx, stsCtrlNamespace, stsCtrlFederationOutcomeKey(key), out); err != nil {
					return nil, fmt.Errorf("put federation seed: %w", err)
				}
			}
		}
		msg := seed.Message
		if msg == "" {
			msg = defaultMessage[seed.Error]
		}
		return stsFederationError(seed.Error, msg, stsFederationOutcomes[seed.Error]), nil
	}
	return nil, nil
}

// loadFederatedRole reads the role roleARN names, or nil when there is none.
func (p *STSPlugin) loadFederatedRole(goCtx context.Context, roleARN string) (*IAMRole, string, error) {
	_, nameWithPath := parsePrincipalARN(roleARN)
	roleName := iamFriendlyName(nameWithPath)
	if roleName == "" {
		return nil, "", nil
	}
	raw, err := p.state.Get(goCtx, iamNamespace, iamRoleKey(arnAccountID(roleARN), roleName))
	if err != nil {
		return nil, "", fmt.Errorf("get role: %w", err)
	}
	if raw == nil {
		return nil, roleName, nil
	}
	var role IAMRole
	if err := json.Unmarshal(raw, &role); err != nil {
		return nil, "", fmt.Errorf("unmarshal role: %w", err)
	}
	return &role, roleName, nil
}

// stsFederatedCredentialsXML is the Credentials member every assume response carries.
type stsFederatedCredentialsXML struct {
	AccessKeyID     string    `xml:"AccessKeyId"`
	SecretAccessKey string    `xml:"SecretAccessKey"`
	SessionToken    string    `xml:"SessionToken"`
	Expiration      time.Time `xml:"Expiration"`
}

// stsAssumedRoleUserXML is the AssumedRoleUser member every assume response carries.
type stsAssumedRoleUserXML struct {
	AssumedRoleID string `xml:"AssumedRoleId"`
	Arn           string `xml:"Arn"`
}

// stsFederatedAssume is one federated assume, after its token's claims have been checked.
type stsFederatedAssume struct {
	roleARN, providerARN, action, sessionName string
	role                                      *IAMRole
	roleName                                  string
	condCtx                                   map[string]string
	multi                                     map[string][]string
	tags                                      map[string]string
	transitiveKeys                            []string
	sourceIdentity                            string
	duration                                  int
	sessionCap                                *time.Time
}

// stsMaxSessionDuration is the MaxSessionDuration a role reports when its record carries none:
// "This setting can have a value from 1 hour to 12 hours", and CreateRole's default is one hour.
const stsMaxSessionDuration = 3600

// federatedSession admits a federated principal to the role through its trust policy, mints the
// session, and stamps RoleLastUsed — the half the two federated assumes share.
func (p *STSPlugin) federatedSession(goCtx context.Context, ctx *RequestContext, req *AWSRequest,
	a stsFederatedAssume) (STSSessionCredentials, *AWSError, error) {
	// The trust policy must name the provider: a role without one admits no federated caller,
	// unlike AssumeRole's opt-in, because AWS's pages require "the identity provider must be
	// specified in the role's trust policy".
	if a.role == nil || len(a.role.AssumeRolePolicyDocument.Statement) == 0 {
		return STSSessionCredentials{}, stsNotAuthorized(a.action), nil
	}

	// The session tags and source identity are part of the request every trust-policy statement
	// is evaluated against, as aws:RequestTag/<key>, aws:TagKeys, sts:TransitiveTagKeys and
	// sts:SourceIdentity.
	multi := map[string][]string{}
	for k, v := range a.multi {
		multi[k] = v
	}
	if len(a.tags) > 0 {
		keys := make([]string, 0, len(a.tags))
		for k, v := range a.tags {
			a.condCtx["aws:RequestTag/"+k] = v
			keys = append(keys, k)
		}
		sort.Strings(keys)
		multi["aws:TagKeys"] = keys
		if len(a.transitiveKeys) > 0 {
			multi["sts:TransitiveTagKeys"] = a.transitiveKeys
		}
	}
	if a.sourceIdentity != "" {
		a.condCtx["sts:SourceIdentity"] = a.sourceIdentity
	}

	// "When using session tags, trust policies for all roles connected to the identity provider
	// (IdP) passing tags must have the sts:TagSession permission", and the same for a source
	// identity and sts:SetSourceIdentity.
	actions := []string{a.action}
	if len(a.tags) > 0 {
		actions = append(actions, "sts:TagSession")
	}
	if a.sourceIdentity != "" {
		actions = append(actions, "sts:SetSourceIdentity")
	}
	for _, action := range actions {
		if stsTrustDecision(*a.role, a.providerARN, action, a.condCtx, multi).Decision != DecisionAllow {
			p.logger.Debug("sts: trust policy refused federated assume",
				"role", a.roleARN, "provider", a.providerARN, "action", action)
			return STSSessionCredentials{}, stsNotAuthorized(a.action), nil
		}
	}

	// "If you specify a value higher than this setting, the operation fails." The wording is the
	// IAM troubleshooting guide's.
	maxDuration := a.role.MaxSessionDuration
	if maxDuration == 0 {
		maxDuration = stsMaxSessionDuration
	}
	if a.duration > maxDuration {
		return STSSessionCredentials{}, stsFederationError("ValidationError",
			"Requested DurationSeconds exceeds MaxSessionDuration set for this role.", http.StatusBadRequest), nil
	}

	policies, refusal, err := p.stsSessionPolicies(goCtx, req)
	if err != nil || refusal != nil {
		return STSSessionCredentials{}, refusal, err
	}

	now := p.now()
	expiry := now.Add(time.Duration(a.duration) * time.Second)
	if a.sessionCap != nil && a.sessionCap.Before(expiry) {
		expiry = a.sessionCap.UTC()
	}
	var transitive []string
	if len(a.tags) > 0 {
		transitive = a.transitiveKeys
	}
	creds, err := p.mintRoleSession(goCtx, ctx, arnAccountID(a.roleARN), a.roleName, a.role.RoleID, a.sessionName,
		expiry, stsSessionExtras{policies: policies, tags: a.tags, transitiveKeys: transitive, sourceIdentity: a.sourceIdentity})
	if err != nil {
		return STSSessionCredentials{}, nil, err
	}
	if err := p.recordRoleLastUsed(goCtx, ctx, a.roleARN, a.roleName, a.role, now); err != nil {
		return STSSessionCredentials{}, nil, err
	}
	return creds, nil, nil
}

// --- AssumeRoleWithWebIdentity --------------------------------------------------

// stsJWTClaims are the claims of a web identity token substrate reads.
type stsJWTClaims struct {
	raw map[string]any
}

// str returns a string claim, or "".
func (c stsJWTClaims) str(name string) string {
	s, _ := c.raw[name].(string)
	return s
}

// strs returns a claim that may be a string or a list of strings, such as aud and amr.
func (c stsJWTClaims) strs(name string) []string {
	return stsStrings(c.raw[name])
}

// stsStrings reads a JSON value that may be a string or a list of strings.
func stsStrings(v any) []string {
	switch v := v.(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// stsJWTTagsClaim and its flattened forms are where a web identity token carries session tags,
// per the IAM User Guide's "Passing session tags using AssumeRoleWithWebIdentity".
const (
	stsJWTTagsClaim           = "https://aws.amazon.com/tags"
	stsJWTFlatTagPrefix       = "https://aws.amazon.com/tags/principal_tags/"
	stsJWTFlatTransitiveClaim = "https://aws.amazon.com/tags/transitive_tag_keys"
	stsJWTSourceIdentityClaim = "https://aws.amazon.com/source_identity"
)

// sessionTags returns the session tags and transitive tag keys the token passes, in the nested
// and the flattened claim formats together, in key order so a duplicate is reported the same way on
// every run. Each principal tag's value is a single string, or a list holding one in the nested
// format.
func (c stsJWTClaims) sessionTags() ([]stsTag, []string) {
	var tags []stsTag
	var transitive []string
	if nested, ok := c.raw[stsJWTTagsClaim].(map[string]any); ok {
		if principal, ok := nested["principal_tags"].(map[string]any); ok {
			for k, v := range principal {
				if values := stsStrings(v); len(values) > 0 {
					tags = append(tags, stsTag{k, values[0]})
				}
			}
		}
		transitive = append(transitive, stsStrings(nested["transitive_tag_keys"])...)
	}
	for name, v := range c.raw {
		if key, ok := strings.CutPrefix(name, stsJWTFlatTagPrefix); ok {
			if values := stsStrings(v); len(values) > 0 {
				tags = append(tags, stsTag{key, values[0]})
			}
		}
	}
	transitive = append(transitive, stsStrings(c.raw[stsJWTFlatTransitiveClaim])...)
	sort.SliceStable(tags, func(i, j int) bool { return tags[i].key < tags[j].key })
	return tags, transitive
}

// stsDecodeJWT decodes a JWT's payload without verifying its signature. A token that is not three
// base64url segments holding a JSON object is InvalidIdentityToken; the message is substrate's own.
func stsDecodeJWT(token string) (stsJWTClaims, *AWSError) {
	malformed := stsFederationError("InvalidIdentityToken",
		"The web identity token is not a well-formed JWT.", http.StatusBadRequest)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return stsJWTClaims{}, malformed
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return stsJWTClaims{}, malformed
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil || raw == nil {
		return stsJWTClaims{}, malformed
	}
	return stsJWTClaims{raw: raw}, nil
}

// stsWebIdentityDefaultMessages are the messages of the seedable web-identity errors. The
// InvalidIdentityToken one is AWS's, observed when a provider's certificate does not match its
// thumbprint; the other two are substrate's own.
var stsWebIdentityDefaultMessages = map[string]string{
	"InvalidIdentityToken":  "OpenIDConnect provider's HTTPS certificate doesn't match configured thumbprint",
	"IDPRejectedClaim":      "The identity provider rejected the claim.",
	"IDPCommunicationError": "The identity provider could not be reached.",
}

// webIdentityTimes checks the token's exp and nbf against the simulated clock. exp is required
// (OpenID Connect Core 1.0 §2 marks it REQUIRED); nbf is checked when present.
func webIdentityTimes(claims stsJWTClaims, now time.Time) *AWSError {
	exp, ok := stsNumericClaim(claims.raw["exp"])
	if !ok {
		return stsFederationError("InvalidIdentityToken", "The web identity token has no exp claim.", http.StatusBadRequest)
	}
	if now.Unix() >= exp {
		// Observed AWS wording, which renders both instants as Unix seconds.
		return stsFederationError("ExpiredTokenException",
			fmt.Sprintf("Token expired: current date/time %d must be before the expiration date/time %d", now.Unix(), exp),
			http.StatusBadRequest)
	}
	if raw, present := claims.raw["nbf"]; present {
		nbf, ok := stsNumericClaim(raw)
		if !ok || now.Unix() < nbf {
			return stsFederationError("InvalidIdentityToken",
				fmt.Sprintf("The web identity token is not valid before %v.", raw), http.StatusBadRequest)
		}
	}
	return nil
}

func (p *STSPlugin) assumeRoleWithWebIdentity(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if err := stsRequire(req, "RoleArn", "RoleSessionName", "WebIdentityToken"); err != nil {
		return nil, err
	}
	roleARN, sessionName := req.Params["RoleArn"], req.Params["RoleSessionName"]
	if !stsRoleSessionNamePattern.MatchString(sessionName) {
		return nil, stsFederationError("ValidationError",
			"RoleSessionName must be 2 to 64 characters from [\\w+=,.@-]", http.StatusBadRequest)
	}
	duration, durErr := stsDurationParam(req, stsDefaultSessionDuration)
	if durErr != nil {
		return nil, durErr
	}
	claims, tokErr := stsDecodeJWT(req.Params["WebIdentityToken"])
	if tokErr != nil {
		return nil, tokErr
	}
	iss := claims.str("iss")
	goCtx := context.Background()
	account := arnAccountID(roleARN)

	// The provider is the one whose URL is the issuer. "The URL is case-sensitive" (IAM User
	// Guide, Create an OpenID Connect identity provider), so the match is exact. AWS's wording
	// when nothing matches is observed through the AWS CLI and every GitHub Actions OIDC setup
	// that skipped creating one.
	raw, err := p.state.Get(goCtx, iamNamespace, iamOIDCProviderKey(account, iamOIDCHostPath(iss)))
	if err != nil {
		return nil, fmt.Errorf("get oidc provider: %w", err)
	}
	if iss == "" || raw == nil {
		return nil, stsFederationError("InvalidIdentityToken",
			"No OpenIDConnect provider found in your account for "+iss, http.StatusBadRequest)
	}
	var prov IAMOIDCProvider
	if err := json.Unmarshal(raw, &prov); err != nil {
		return nil, fmt.Errorf("unmarshal oidc provider: %w", err)
	}

	seeded, err := p.federationSeed(goCtx, prov.ARN,
		[]string{"InvalidIdentityToken", "IDPRejectedClaim", "IDPCommunicationError"}, stsWebIdentityDefaultMessages)
	if err != nil {
		return nil, err
	}
	if seeded != nil {
		return nil, seeded
	}

	audience := ""
	for _, aud := range claims.strs("aud") {
		if slices.Contains(prov.ClientIDList, aud) {
			audience = aud
			break
		}
	}
	if audience == "" {
		// Observed AWS wording.
		return nil, stsFederationError("InvalidIdentityToken", "Incorrect token audience", http.StatusBadRequest)
	}
	// sub is REQUIRED in an ID token (OpenID Connect Core 1.0 §2), and AWS reports it as
	// SubjectFromWebIdentityToken. The message is substrate's own.
	sub := claims.str("sub")
	if sub == "" {
		return nil, stsFederationError("InvalidIdentityToken", "The web identity token has no sub claim.",
			http.StatusBadRequest)
	}
	if refusal := webIdentityTimes(claims, p.now()); refusal != nil {
		return nil, refusal
	}

	tagList, transitive := claims.sessionTags()
	tags, refusal := stsSessionTagMap(tagList, "InvalidIdentityToken")
	if refusal != nil {
		return nil, refusal
	}
	sourceIdentity := claims.str(stsJWTSourceIdentityClaim)
	if refusal := stsValidateSourceIdentity(sourceIdentity); refusal != nil {
		return nil, refusal
	}

	role, roleName, err := p.loadFederatedRole(goCtx, roleARN)
	if err != nil {
		return nil, err
	}

	// The provider's condition keys are its URL without the scheme followed by the claim, with
	// the default mapping AWS documents for an IdP it has no special case for: aud is azp when
	// the token sets one and aud otherwise, oaud is aud, and sub, email and amr are themselves.
	// Each is published only when the token carries the claim behind it.
	prefix := prov.URL + ":"
	condCtx := map[string]string{
		prefix + "sub":        sub,
		prefix + "aud":        audience,
		prefix + "oaud":       audience,
		"sts:RoleSessionName": sessionName,
	}
	if azp := claims.str("azp"); azp != "" {
		condCtx[prefix+"aud"] = azp
	}
	if email := claims.str("email"); email != "" {
		condCtx[prefix+"email"] = email
	}
	multi := map[string][]string{}
	if amr := claims.strs("amr"); len(amr) > 0 {
		multi[prefix+"amr"] = amr
	}

	creds, refusal, err := p.federatedSession(goCtx, ctx, req, stsFederatedAssume{
		roleARN: roleARN, providerARN: prov.ARN, action: stsWebIdentityAction, sessionName: sessionName,
		role: role, roleName: roleName, condCtx: condCtx, multi: multi,
		tags: tags, transitiveKeys: transitive, sourceIdentity: sourceIdentity, duration: duration,
	})
	if err != nil {
		return nil, err
	}
	if refusal != nil {
		return nil, refusal
	}

	type result struct {
		Credentials                 stsFederatedCredentialsXML `xml:"Credentials"`
		SubjectFromWebIdentityToken string                     `xml:"SubjectFromWebIdentityToken"`
		AssumedRoleUser             stsAssumedRoleUserXML      `xml:"AssumedRoleUser"`
		Provider                    string                     `xml:"Provider"`
		Audience                    string                     `xml:"Audience"`
		SourceIdentity              string                     `xml:"SourceIdentity,omitempty"`
	}
	type response struct {
		XMLName                         xml.Name         `xml:"AssumeRoleWithWebIdentityResponse"`
		Xmlns                           string           `xml:"xmlns,attr"`
		AssumeRoleWithWebIdentityResult result           `xml:"AssumeRoleWithWebIdentityResult"`
		ResponseMetadata                responseMetadata `xml:"ResponseMetadata"`
	}
	// Provider is "the value of the iss field" for an OpenID Connect ID token.
	return stsXMLResponse(http.StatusOK, response{
		Xmlns: "https://sts.amazonaws.com/doc/2011-06-15/",
		AssumeRoleWithWebIdentityResult: result{
			Credentials:                 stsCredentialsXML(creds),
			SubjectFromWebIdentityToken: sub,
			AssumedRoleUser:             stsAssumedRoleUserXML{AssumedRoleID: creds.PrincipalID, Arn: creds.PrincipalARN},
			Provider:                    iss,
			Audience:                    audience,
			SourceIdentity:              sourceIdentity,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

// stsCredentialsXML renders a session's credentials.
func stsCredentialsXML(c STSSessionCredentials) stsFederatedCredentialsXML {
	return stsFederatedCredentialsXML{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey,
		SessionToken: c.SessionToken, Expiration: c.Expiration,
	}
}

// stsNumericClaim reads a NumericDate claim — "Timestamps in the token must be formatted as either
// an integer or a long integer" — as Unix seconds.
func stsNumericClaim(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	if i, err := n.Int64(); err == nil {
		return i, true
	}
	f, err := n.Float64()
	if err != nil {
		return 0, false
	}
	return int64(f), true
}

// --- AssumeRoleWithSAML -----------------------------------------------------------

// SAML attribute names AWS reads, from "Configure SAML assertions for the authentication
// response" and the session-tag and source-identity pages.
const (
	stsSAMLRoleAttr            = "https://aws.amazon.com/SAML/Attributes/Role"
	stsSAMLRoleSessionNameAttr = "https://aws.amazon.com/SAML/Attributes/RoleSessionName"
	stsSAMLPrincipalTagPrefix  = "https://aws.amazon.com/SAML/Attributes/PrincipalTag:"
	stsSAMLTransitiveAttr      = "https://aws.amazon.com/SAML/Attributes/TransitiveTagKeys"
	stsSAMLSourceIdentityAttr  = "https://aws.amazon.com/SAML/Attributes/SourceIdentity"
)

// stsSAMLAudiencePattern matches the audiences AWS accepts in a SAML assertion: the sign-in
// endpoint, global or regional, and the `urn:amazon:webservices` URN.
var stsSAMLAudiencePattern = regexp.MustCompile(`^(urn:amazon:webservices|https://([a-z0-9-]+\.)?signin\.aws\.amazon\.com/saml)$`)

// stsSAMLAssertion is the part of a SAML assertion substrate reads. Element names are matched by
// local name, so the saml: and saml2: prefixes both decode.
type stsSAMLAssertion struct {
	Issuer  string `xml:"Issuer"`
	Subject struct {
		NameID struct {
			Format string `xml:"Format,attr"`
			Value  string `xml:",chardata"`
		} `xml:"NameID"`
		Confirmations []struct {
			Data struct {
				NotBefore    string `xml:"NotBefore,attr"`
				NotOnOrAfter string `xml:"NotOnOrAfter,attr"`
				Recipient    string `xml:"Recipient,attr"`
			} `xml:"SubjectConfirmationData"`
		} `xml:"SubjectConfirmation"`
	} `xml:"Subject"`
	Conditions struct {
		NotBefore    string   `xml:"NotBefore,attr"`
		NotOnOrAfter string   `xml:"NotOnOrAfter,attr"`
		Audiences    []string `xml:"AudienceRestriction>Audience"`
	} `xml:"Conditions"`
	AuthnStatement struct {
		SessionNotOnOrAfter string `xml:"SessionNotOnOrAfter,attr"`
	} `xml:"AuthnStatement"`
	Attributes []struct {
		Name   string   `xml:"Name,attr"`
		Values []string `xml:"AttributeValue"`
	} `xml:"AttributeStatement>Attribute"`
}

// attribute returns the trimmed values of the named attribute.
func (a *stsSAMLAssertion) attribute(name string) []string {
	var out []string
	for _, attr := range a.Attributes {
		if attr.Name != name {
			continue
		}
		for _, v := range attr.Values {
			out = append(out, strings.TrimSpace(v))
		}
	}
	return out
}

// sessionTags returns the PrincipalTag:<key> attributes as session tags, in assertion order, and
// TransitiveTagKeys.
func (a *stsSAMLAssertion) sessionTags() ([]stsTag, []string) {
	var tags []stsTag
	for _, attr := range a.Attributes {
		if key, ok := strings.CutPrefix(attr.Name, stsSAMLPrincipalTagPrefix); ok && len(attr.Values) > 0 {
			tags = append(tags, stsTag{key, strings.TrimSpace(attr.Values[0])})
		}
	}
	return tags, a.attribute(stsSAMLTransitiveAttr)
}

// stsSAMLResponse is a SAML Response envelope around one assertion.
type stsSAMLResponse struct {
	XMLName            xml.Name
	Issuer             string            `xml:"Issuer"`
	Assertion          *stsSAMLAssertion `xml:"Assertion"`
	EncryptedAssertion *struct{}         `xml:"EncryptedAssertion"`
}

// stsParseSAMLAssertion decodes SAMLAssertion, "the base64 encoded SAML authentication response
// provided by the IdP". A response that does not decode or carries no readable assertion is
// InvalidIdentityToken; the messages are substrate's own. An encrypted assertion is refused,
// because substrate holds no private key to decrypt it with.
func stsParseSAMLAssertion(encoded string) (*stsSAMLAssertion, string, *AWSError) {
	malformed := stsFederationError("InvalidIdentityToken",
		"The SAML assertion is not a base64-encoded SAML response.", http.StatusBadRequest)
	data, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(encoded), ""))
	if err != nil {
		return nil, "", malformed
	}
	var resp stsSAMLResponse
	if err := xml.Unmarshal(data, &resp); err != nil {
		return nil, "", malformed
	}
	switch {
	case resp.XMLName.Local == "Assertion":
		var a stsSAMLAssertion
		if err := xml.Unmarshal(data, &a); err != nil {
			return nil, "", malformed
		}
		return &a, strings.TrimSpace(a.Issuer), nil
	case resp.EncryptedAssertion != nil && resp.Assertion == nil:
		return nil, "", stsFederationError("InvalidIdentityToken",
			"Substrate cannot decrypt an encrypted SAML assertion.", http.StatusBadRequest)
	case resp.Assertion == nil:
		return nil, "", malformed
	}
	issuer := strings.TrimSpace(resp.Assertion.Issuer)
	if issuer == "" {
		issuer = strings.TrimSpace(resp.Issuer)
	}
	return resp.Assertion, issuer, nil
}

// stsSAMLDefaultMessages are the messages of the seedable SAML errors. The InvalidIdentityToken one
// is the IAM troubleshooting guide's ("Response signature invalid"); IDPRejectedClaim's is
// substrate's own.
var stsSAMLDefaultMessages = map[string]string{
	"InvalidIdentityToken": "Response signature invalid",
	"IDPRejectedClaim":     "The identity provider rejected the claim.",
}

// stsSAMLSubjectType strips the `urn:oasis:names:tc:SAML:2.0:nameid-format:` prefix, as the
// SubjectType response member documents.
func stsSAMLSubjectType(format string) string {
	return strings.TrimPrefix(format, "urn:oasis:names:tc:SAML:2.0:nameid-format:")
}

// samlTimes checks every NotBefore and NotOnOrAfter in the assertion against the simulated clock.
// The messages are substrate's own.
func samlTimes(a *stsSAMLAssertion, now time.Time) *AWSError {
	notBefore := []string{a.Conditions.NotBefore}
	notOnOrAfter := []string{a.Conditions.NotOnOrAfter}
	for _, c := range a.Subject.Confirmations {
		notBefore = append(notBefore, c.Data.NotBefore)
		notOnOrAfter = append(notOnOrAfter, c.Data.NotOnOrAfter)
	}
	parse := func(s string) (time.Time, *AWSError) {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Time{}, stsFederationError("InvalidIdentityToken",
				"The SAML assertion carries an unreadable NotBefore or NotOnOrAfter.", http.StatusBadRequest)
		}
		return t, nil
	}
	for _, s := range notOnOrAfter {
		if s == "" {
			continue
		}
		t, bad := parse(s)
		if bad != nil {
			return bad
		}
		if !now.Before(t) {
			return stsFederationError("ExpiredTokenException",
				fmt.Sprintf("The SAML assertion expired at %s.", t.UTC().Format(time.RFC3339)), http.StatusBadRequest)
		}
	}
	for _, s := range notBefore {
		if s == "" {
			continue
		}
		t, bad := parse(s)
		if bad != nil {
			return bad
		}
		if now.Before(t) {
			return stsFederationError("InvalidIdentityToken",
				fmt.Sprintf("The SAML assertion is not valid before %s.", t.UTC().Format(time.RFC3339)), http.StatusBadRequest)
		}
	}
	return nil
}

func (p *STSPlugin) assumeRoleWithSAML(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if err := stsRequire(req, "RoleArn", "PrincipalArn", "SAMLAssertion"); err != nil {
		return nil, err
	}
	roleARN, providerARN := req.Params["RoleArn"], req.Params["PrincipalArn"]
	goCtx := context.Background()

	providerAccount := arnAccountID(providerARN)
	providerName := iamProviderARNName(providerARN, "saml-provider", providerAccount)
	var prov *IAMSAMLProvider
	if providerName != "" {
		raw, err := p.state.Get(goCtx, iamNamespace, iamSAMLProviderKey(providerAccount, providerName))
		if err != nil {
			return nil, fmt.Errorf("get saml provider: %w", err)
		}
		if raw != nil {
			prov = &IAMSAMLProvider{}
			if err := json.Unmarshal(raw, prov); err != nil {
				return nil, fmt.Errorf("unmarshal saml provider: %w", err)
			}
		}
	}
	if prov == nil {
		// The IAM troubleshooting guide's wording; the guide gives no code for it, so the code is
		// substrate's choice.
		return nil, stsFederationError("InvalidIdentityToken", "Specified provider doesn't exist.",
			http.StatusBadRequest)
	}

	assertion, issuer, parseErr := stsParseSAMLAssertion(req.Params["SAMLAssertion"])
	if parseErr != nil {
		return nil, parseErr
	}
	// The assertion's issuer must be one the provider's metadata declares. The wording is the IAM
	// troubleshooting guide's ("Issuer not present in specified provider"); the guide reports it
	// from the console's sign-in path, so the code here is substrate's choice.
	if !slices.Contains(prov.EntityIDs, issuer) {
		return nil, stsFederationError("InvalidIdentityToken", "Issuer not present in specified provider",
			http.StatusBadRequest)
	}
	seeded, err := p.federationSeed(goCtx, prov.ARN, []string{"InvalidIdentityToken", "IDPRejectedClaim"},
		stsSAMLDefaultMessages)
	if err != nil {
		return nil, err
	}
	if seeded != nil {
		return nil, seeded
	}
	// "Response does not contain the required audience." — the IAM troubleshooting guide.
	if !slices.ContainsFunc(assertion.Conditions.Audiences, func(a string) bool {
		return stsSAMLAudiencePattern.MatchString(strings.TrimSpace(a))
	}) {
		return nil, stsFederationError("InvalidIdentityToken", "Response does not contain the required audience.",
			http.StatusBadRequest)
	}
	if refusal := samlTimes(assertion, p.now()); refusal != nil {
		return nil, refusal
	}
	recipient := ""
	for _, c := range assertion.Subject.Confirmations {
		if recipient == "" {
			recipient = c.Data.Recipient
		}
	}

	// The Role attribute pairs a role with a provider, in either order.
	paired := false
	for _, v := range assertion.attribute(stsSAMLRoleAttr) {
		parts := strings.Split(v, ",")
		if len(parts) != 2 {
			continue
		}
		a, b := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if (a == roleARN && b == providerARN) || (a == providerARN && b == roleARN) {
			paired = true
			break
		}
	}
	subject := strings.TrimSpace(assertion.Subject.NameID.Value)
	if !paired || subject == "" {
		return nil, stsNotAuthorized(stsSAMLAction)
	}

	sessionNames := assertion.attribute(stsSAMLRoleSessionNameAttr)
	if len(sessionNames) == 0 {
		return nil, stsFederationError("InvalidIdentityToken", "RoleSessionName is required in AuthnResponse",
			http.StatusBadRequest)
	}
	sessionName := sessionNames[0]
	if !stsRoleSessionNamePattern.MatchString(sessionName) {
		return nil, stsFederationError("InvalidIdentityToken",
			"RoleSessionName in AuthnResponse must match [a-zA-Z_0-9+=,.@-]{2,64}", http.StatusBadRequest)
	}
	tagList, transitive := assertion.sessionTags()
	tags, refusal := stsSessionTagMap(tagList, "InvalidIdentityToken")
	if refusal != nil {
		return nil, refusal
	}
	sourceIdentity := ""
	if v := assertion.attribute(stsSAMLSourceIdentityAttr); len(v) > 0 {
		sourceIdentity = v[0]
	}
	if refusal := stsValidateSourceIdentity(sourceIdentity); refusal != nil {
		return nil, refusal
	}

	// The assertion's SessionDuration attribute is the console session's maximum, not the API's
	// default, so the API session lasts DurationSeconds (3600 when absent), capped by
	// SessionNotOnOrAfter.
	duration, durErr := stsDurationParam(req, stsDefaultSessionDuration)
	if durErr != nil {
		return nil, durErr
	}
	var sessionCap *time.Time
	if s := assertion.AuthnStatement.SessionNotOnOrAfter; s != "" {
		if t, perr := time.Parse(time.RFC3339, s); perr == nil {
			sessionCap = &t
		}
	}

	role, roleName, err := p.loadFederatedRole(goCtx, roleARN)
	if err != nil {
		return nil, err
	}

	// NameQualifier: "BASE64 ( SHA1 ( "https://example.com/saml" + "123456789012" + "/MySAMLIdP" ) )".
	sum := sha1.Sum([]byte(issuer + providerAccount + "/" + providerName)) //nolint:gosec // AWS's definition.
	nameQualifier := base64.StdEncoding.EncodeToString(sum[:])
	subjectType := stsSAMLSubjectType(assertion.Subject.NameID.Format)
	condCtx := map[string]string{
		"saml:aud":           recipient,
		"saml:iss":           issuer,
		"saml:sub":           subject,
		"saml:doc":           providerAccount + "/" + providerName,
		"saml:namequalifier": nameQualifier,
	}
	if subjectType != "" {
		condCtx["saml:sub_type"] = subjectType
	}

	creds, refusal, err := p.federatedSession(goCtx, ctx, req, stsFederatedAssume{
		roleARN: roleARN, providerARN: prov.ARN, action: stsSAMLAction, sessionName: sessionName,
		role: role, roleName: roleName, condCtx: condCtx,
		tags: tags, transitiveKeys: transitive, sourceIdentity: sourceIdentity,
		duration: duration, sessionCap: sessionCap,
	})
	if err != nil {
		return nil, err
	}
	if refusal != nil {
		return nil, refusal
	}

	type result struct {
		Credentials     stsFederatedCredentialsXML `xml:"Credentials"`
		AssumedRoleUser stsAssumedRoleUserXML      `xml:"AssumedRoleUser"`
		Audience        string                     `xml:"Audience"`
		Issuer          string                     `xml:"Issuer"`
		Subject         string                     `xml:"Subject"`
		SubjectType     string                     `xml:"SubjectType"`
		NameQualifier   string                     `xml:"NameQualifier"`
		SourceIdentity  string                     `xml:"SourceIdentity,omitempty"`
	}
	type response struct {
		XMLName                  xml.Name         `xml:"AssumeRoleWithSAMLResponse"`
		Xmlns                    string           `xml:"xmlns,attr"`
		AssumeRoleWithSAMLResult result           `xml:"AssumeRoleWithSAMLResult"`
		ResponseMetadata         responseMetadata `xml:"ResponseMetadata"`
	}
	return stsXMLResponse(http.StatusOK, response{
		Xmlns: "https://sts.amazonaws.com/doc/2011-06-15/",
		AssumeRoleWithSAMLResult: result{
			Credentials:     stsCredentialsXML(creds),
			AssumedRoleUser: stsAssumedRoleUserXML{AssumedRoleID: creds.PrincipalID, Arn: creds.PrincipalARN},
			Audience:        recipient,
			Issuer:          issuer,
			Subject:         subject,
			SubjectType:     subjectType,
			NameQualifier:   nameQualifier,
			SourceIdentity:  sourceIdentity,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}
