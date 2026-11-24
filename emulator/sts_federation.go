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
//   - the token names a provider registered in the role's account (OIDC `iss`, SAML PrincipalArn);
//   - the OIDC `aud` is one of the provider's client IDs;
//   - `exp`, or the assertion's NotOnOrAfter, is still ahead of the simulated clock;
//   - a SAML assertion's Role attribute pairs this role with this provider;
//   - the role's trust policy admits the provider as a `Federated` principal for the action, with
//     the provider's condition keys (`<issuer>:sub`, `<issuer>:aud`, `saml:aud`, …) in context.
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

// stsFederationError is an STS error with the Query protocol's code and status.
func stsFederationError(code, message string, status int) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: status}
}

// stsNotAuthorized is the refusal a federated assume answers when the role cannot be assumed: it
// does not exist, the SAML assertion does not name it, or its trust policy does not admit the
// provider. The wording is AWS's — documented for SAML in the IAM troubleshooting guide ("Not
// authorized to perform sts:AssumeRoleWithSAML", AccessDenied/403) and observed for web identity.
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

// federatedSession admits a federated principal to role through its trust policy, mints the
// session, and stamps RoleLastUsed — the half the two federated assumes share.
func (p *STSPlugin) federatedSession(goCtx context.Context, ctx *RequestContext, roleARN, providerARN,
	action, sessionName string, role *IAMRole, roleName string, condCtx map[string]string,
	multi map[string][]string, expiry func(now time.Time) time.Time) (STSSessionCredentials, *AWSError, error) {
	// The trust policy must name the provider: a role without one admits no federated caller,
	// unlike AssumeRole's opt-in, because AWS's pages require "the identity provider must be
	// specified in the role's trust policy".
	if role == nil || len(role.AssumeRolePolicyDocument.Statement) == 0 {
		return STSSessionCredentials{}, stsNotAuthorized(action), nil
	}
	if stsTrustDecision(*role, providerARN, action, condCtx, multi).Decision != DecisionAllow {
		p.logger.Debug("sts: trust policy refused federated assume", "role", roleARN, "provider", providerARN)
		return STSSessionCredentials{}, stsNotAuthorized(action), nil
	}
	now := p.now()
	creds, err := p.mintRoleSession(goCtx, ctx, arnAccountID(roleARN), roleName, role.RoleID, sessionName, expiry(now))
	if err != nil {
		return STSSessionCredentials{}, nil, err
	}
	if err := p.recordRoleLastUsed(goCtx, ctx, roleARN, roleName, role, now); err != nil {
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
	switch v := c.raw[name].(type) {
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

	// AWS's wording when the issuer names no provider, observed through the AWS CLI and every
	// GitHub Actions OIDC setup that skipped creating one.
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

	exp, ok := stsNumericClaim(claims.raw["exp"])
	if !ok {
		return nil, stsFederationError("InvalidIdentityToken",
			"The web identity token has no exp claim.", http.StatusBadRequest)
	}
	now := p.now()
	if now.Unix() >= exp {
		// Observed AWS wording, which renders both instants as Unix seconds.
		return nil, stsFederationError("ExpiredTokenException",
			fmt.Sprintf("Token expired: current date/time %d must be before the expiration date/time %d", now.Unix(), exp),
			http.StatusBadRequest)
	}

	role, roleName, err := p.loadFederatedRole(goCtx, roleARN)
	if err != nil {
		return nil, err
	}

	// The provider's condition keys are its URL without the scheme followed by the claim, with
	// the default mapping AWS documents for an IdP it has no special case for: aud is azp when
	// the token sets one and aud otherwise, oaud is aud, and sub, email and amr are themselves.
	prefix := prov.URL + ":"
	condCtx := map[string]string{
		prefix + "sub":        claims.str("sub"),
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
	var multi map[string][]string
	if amr := claims.strs("amr"); len(amr) > 0 {
		multi = map[string][]string{prefix + "amr": amr}
	}

	creds, refusal, err := p.federatedSession(goCtx, ctx, roleARN, prov.ARN, stsWebIdentityAction, sessionName,
		role, roleName, condCtx, multi, func(now time.Time) time.Time {
			return now.Add(time.Duration(duration) * time.Second)
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
			SubjectFromWebIdentityToken: claims.str("sub"),
			AssumedRoleUser:             stsAssumedRoleUserXML{AssumedRoleID: creds.PrincipalID, Arn: creds.PrincipalARN},
			Provider:                    iss,
			Audience:                    audience,
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

// SAML attribute names AWS reads, from "Configure SAML assertions for the authentication response".
const (
	stsSAMLRoleAttr            = "https://aws.amazon.com/SAML/Attributes/Role"
	stsSAMLRoleSessionNameAttr = "https://aws.amazon.com/SAML/Attributes/RoleSessionName"
	stsSAMLSessionDurationAttr = "https://aws.amazon.com/SAML/Attributes/SessionDuration"
)

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
				NotOnOrAfter string `xml:"NotOnOrAfter,attr"`
				Recipient    string `xml:"Recipient,attr"`
			} `xml:"SubjectConfirmationData"`
		} `xml:"SubjectConfirmation"`
	} `xml:"Subject"`
	Conditions struct {
		NotOnOrAfter string `xml:"NotOnOrAfter,attr"`
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
	seeded, err := p.federationSeed(goCtx, prov.ARN, []string{"InvalidIdentityToken", "IDPRejectedClaim"},
		stsSAMLDefaultMessages)
	if err != nil {
		return nil, err
	}
	if seeded != nil {
		return nil, seeded
	}

	now := p.now()
	recipient := ""
	notOnOrAfter := []string{assertion.Conditions.NotOnOrAfter}
	for _, c := range assertion.Subject.Confirmations {
		notOnOrAfter = append(notOnOrAfter, c.Data.NotOnOrAfter)
		if recipient == "" {
			recipient = c.Data.Recipient
		}
	}
	for _, s := range notOnOrAfter {
		if s == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, stsFederationError("InvalidIdentityToken",
				"The SAML assertion carries an unreadable NotOnOrAfter.", http.StatusBadRequest)
		}
		if !now.Before(t) {
			return nil, stsFederationError("ExpiredTokenException",
				fmt.Sprintf("The SAML assertion expired at %s.", t.UTC().Format(time.RFC3339)), http.StatusBadRequest)
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

	defaultDuration := stsDefaultSessionDuration
	if v := assertion.attribute(stsSAMLSessionDurationAttr); len(v) > 0 {
		if d, convErr := strconv.Atoi(v[0]); convErr == nil && d >= 900 && d <= 43200 {
			defaultDuration = d
		}
	}
	duration, durErr := stsDurationParam(req, defaultDuration)
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
		"saml:sub_type":      subjectType,
		"saml:doc":           providerAccount + "/" + providerName,
		"saml:namequalifier": nameQualifier,
	}

	creds, refusal, err := p.federatedSession(goCtx, ctx, roleARN, prov.ARN, stsSAMLAction, sessionName,
		role, roleName, condCtx, nil, func(now time.Time) time.Time {
			// "Your role session lasts for the duration that you specify … or until the time
			// specified in the SAML authentication response's SessionNotOnOrAfter value, whichever
			// is shorter."
			expiry := now.Add(time.Duration(duration) * time.Second)
			if sessionCap != nil && sessionCap.Before(expiry) {
				return sessionCap.UTC()
			}
			return expiry
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
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}
