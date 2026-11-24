package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

// IAM identity providers: the OpenID Connect and SAML provider records a role's trust policy
// names as a `Federated` principal, and which STS's AssumeRoleWithWebIdentity and
// AssumeRoleWithSAML read (sts_federation.go).
//
// A provider is a record, not a connection. Substrate fetches no discovery document, JWKS or
// certificate, so a provider created without a ThumbprintList stores none (AWS would retrieve
// one), and `OpenIdIdpCommunicationError` is reachable only through the
// `/v1/iam/oidc-provider-outcome` seed.

// iamOIDCMaxThumbprints and iamOIDCMaxClientIDs are the limits CreateOpenIDConnectProvider
// documents: "IAM lets you have up to five thumbprints for an OIDC provider" and "You cannot
// register more than 100 client IDs with a single IAM OIDC provider." Both are its words.
const (
	iamOIDCMaxThumbprints = 5
	iamOIDCMaxClientIDs   = 100
)

// iamSAMLMaxPrivateKeys is the private-key limit the IAM troubleshooting guide names: "You can
// save up to two private keys for each identity provider." That is its wording.
const iamSAMLMaxPrivateKeys = 2

// IAMOIDCProvider is a stored IAM OpenID Connect provider.
type IAMOIDCProvider struct {
	// ARN is `arn:aws:iam::<account>:oidc-provider/<host/path>`.
	ARN string `json:"Arn"`

	// URL is the provider URL without its `https://` scheme, which is how
	// GetOpenIDConnectProvider reports it (its sample answers `server.example.com`).
	URL string `json:"Url"`

	// ClientIDList are the registered audiences, in the order they were added.
	ClientIDList []string `json:"ClientIDList"`

	// ThumbprintList are the registered certificate thumbprints.
	ThumbprintList []string `json:"ThumbprintList"`

	// CreateDate is when the provider was created.
	CreateDate time.Time `json:"CreateDate"`

	// Tags are the provider's tags, sorted by key.
	Tags []IAMTag `json:"Tags,omitempty"`
}

// IAMSAMLPrivateKey is the metadata of one SAML decryption key. The key text is never stored.
type IAMSAMLPrivateKey struct {
	// KeyID identifies the key to UpdateSAMLProvider's RemovePrivateKey.
	KeyID string `json:"KeyId"`

	// Timestamp is when the key was added.
	Timestamp time.Time `json:"Timestamp"`
}

// IAMSAMLProvider is a stored IAM SAML provider.
type IAMSAMLProvider struct {
	// ARN is `arn:aws:iam::<account>:saml-provider/<name>`.
	ARN string `json:"Arn"`

	// Name is the provider's friendly name, the last component of its ARN.
	Name string `json:"Name"`

	// SAMLMetadataDocument is the metadata document as submitted.
	SAMLMetadataDocument string `json:"SAMLMetadataDocument"`

	// SAMLProviderUUID is the provider's unique identifier.
	SAMLProviderUUID string `json:"SAMLProviderUUID"`

	// AssertionEncryptionMode is `Required` or `Allowed`, or empty when never set.
	AssertionEncryptionMode string `json:"AssertionEncryptionMode,omitempty"`

	// CreateDate is when the provider was created.
	CreateDate time.Time `json:"CreateDate"`

	// ValidUntil is the metadata document's `validUntil`, when it declares one.
	ValidUntil *time.Time `json:"ValidUntil,omitempty"`

	// PrivateKeyList holds the identifiers of the decryption keys added to the provider.
	PrivateKeyList []IAMSAMLPrivateKey `json:"PrivateKeyList,omitempty"`

	// Tags are the provider's tags, sorted by key.
	Tags []IAMTag `json:"Tags,omitempty"`
}

// iamOIDCProviderARN returns the ARN for an OpenID Connect provider whose URL, without its
// scheme, is hostPath.
func iamOIDCProviderARN(accountID, hostPath string) string {
	return "arn:aws:iam::" + accountID + ":oidc-provider/" + hostPath
}

// iamSAMLProviderARN returns the ARN for a SAML provider.
func iamSAMLProviderARN(accountID, name string) string {
	return "arn:aws:iam::" + accountID + ":saml-provider/" + name
}

// iamOIDCHostPath returns an OIDC provider URL or token issuer without its `https://` scheme,
// which is the component a provider's ARN and state key end in.
func iamOIDCHostPath(url string) string {
	return strings.TrimPrefix(url, "https://")
}

// iamProviderARNName returns what follows `:<kind>/` in an identity-provider ARN of the request's
// account, or "" when arn is not one.
func iamProviderARNName(arn, kind, accountID string) string {
	prefix := "arn:aws:iam::" + accountID + ":" + kind + "/"
	if !strings.HasPrefix(arn, prefix) {
		return ""
	}
	return arn[len(prefix):]
}

// iamOIDCNotFound is GetOpenIDConnectProvider's NoSuchEntity, in the wording AWS returns (observed
// through the AWS CLI and Terraform; the API reference publishes none).
func iamOIDCNotFound(arn string) *AWSResponse {
	return iamErrorResponse("NoSuchEntity",
		fmt.Sprintf("OpenIDConnect Provider not found for arn %s", arn), http.StatusNotFound)
}

// iamSAMLNotFound is the NoSuchEntity a SAML provider operation answers. The message is
// substrate's own.
func iamSAMLNotFound(arn string) *AWSResponse {
	return iamErrorResponse("NoSuchEntity",
		fmt.Sprintf("SAML provider %s was not found.", arn), http.StatusNotFound)
}

// loadOIDCProvider returns the provider arn names in accountID, or nil when there is none.
func (p *IAMPlugin) loadOIDCProvider(goCtx context.Context, accountID, arn string) (*IAMOIDCProvider, error) {
	hostPath := iamProviderARNName(arn, "oidc-provider", accountID)
	if hostPath == "" {
		return nil, nil //nolint:nilnil // an ARN naming nothing in this account is the caller's NoSuchEntity.
	}
	raw, err := p.state.Get(goCtx, iamNamespace, iamOIDCProviderKey(accountID, hostPath))
	if err != nil {
		return nil, fmt.Errorf("get oidc provider: %w", err)
	}
	if raw == nil {
		return nil, nil //nolint:nilnil // absent is the caller's NoSuchEntity.
	}
	var prov IAMOIDCProvider
	if err := json.Unmarshal(raw, &prov); err != nil {
		return nil, fmt.Errorf("unmarshal oidc provider: %w", err)
	}
	return &prov, nil
}

// putOIDCProvider stores prov under its URL.
func (p *IAMPlugin) putOIDCProvider(goCtx context.Context, accountID string, prov *IAMOIDCProvider) error {
	raw, err := json.Marshal(prov)
	if err != nil {
		return fmt.Errorf("marshal oidc provider: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, iamOIDCProviderKey(accountID, prov.URL), raw); err != nil {
		return fmt.Errorf("put oidc provider: %w", err)
	}
	return nil
}

// loadSAMLProvider returns the SAML provider arn names in accountID, or nil when there is none.
func (p *IAMPlugin) loadSAMLProvider(goCtx context.Context, accountID, arn string) (*IAMSAMLProvider, error) {
	name := iamProviderARNName(arn, "saml-provider", accountID)
	if name == "" {
		return nil, nil //nolint:nilnil // an ARN naming nothing in this account is the caller's NoSuchEntity.
	}
	raw, err := p.state.Get(goCtx, iamNamespace, iamSAMLProviderKey(accountID, name))
	if err != nil {
		return nil, fmt.Errorf("get saml provider: %w", err)
	}
	if raw == nil {
		return nil, nil //nolint:nilnil // absent is the caller's NoSuchEntity.
	}
	var prov IAMSAMLProvider
	if err := json.Unmarshal(raw, &prov); err != nil {
		return nil, fmt.Errorf("unmarshal saml provider: %w", err)
	}
	return &prov, nil
}

// putSAMLProvider stores prov under its name.
func (p *IAMPlugin) putSAMLProvider(goCtx context.Context, accountID string, prov *IAMSAMLProvider) error {
	raw, err := json.Marshal(prov)
	if err != nil {
		return fmt.Errorf("marshal saml provider: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, iamSAMLProviderKey(accountID, prov.Name), raw); err != nil {
		return fmt.Errorf("put saml provider: %w", err)
	}
	return nil
}

// iamValidateThumbprints refuses a thumbprint list over the documented limit or holding a value
// that is not the documented 40 characters. Both messages are substrate's own.
func iamValidateThumbprints(thumbprints []string) *AWSResponse {
	if len(thumbprints) > iamOIDCMaxThumbprints {
		return iamErrorResponse("LimitExceeded",
			fmt.Sprintf("An OpenID Connect provider can have at most %d thumbprints.", iamOIDCMaxThumbprints),
			http.StatusConflict)
	}
	for _, t := range thumbprints {
		if len(t) != 40 {
			return iamErrorResponse("ValidationError",
				fmt.Sprintf("Thumbprint %q must be 40 characters long.", t), http.StatusBadRequest)
		}
	}
	return nil
}

// iamValidateClientID refuses a client ID outside the documented 1–255 characters. The message is
// substrate's own.
func iamValidateClientID(id string) *AWSResponse {
	if id == "" || len(id) > 255 {
		return iamErrorResponse("ValidationError",
			"ClientID must be between 1 and 255 characters long.", http.StatusBadRequest)
	}
	return nil
}

// iamUniqueStrings returns list with repeats removed, keeping the first of each.
func iamUniqueStrings(list []string) []string {
	out := make([]string, 0, len(list))
	for _, s := range list {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

// --- OpenID Connect providers ------------------------------------------------

func (p *IAMPlugin) createOpenIDConnectProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		URL            string   `json:"Url"`
		ClientIDList   []string `json:"ClientIDList"`
		ThumbprintList []string `json:"ThumbprintList"`
		Tags           []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if list := iamMemberList(req.Params, "ClientIDList"); list != nil {
		params.ClientIDList = list
	}
	if list := iamMemberList(req.Params, "ThumbprintList"); list != nil {
		params.ThumbprintList = list
	}
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}
	if params.URL == "" {
		return iamErrorResponse("ValidationError", "Url is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:CreateOpenIDConnectProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	// "The URL must begin with https:// … query parameters are not [allowed]." The message is
	// substrate's own.
	if !strings.HasPrefix(params.URL, "https://") || len(params.URL) <= len("https://") ||
		strings.Contains(params.URL, "?") || len(params.URL) > 255 {
		return iamErrorResponse("InvalidInput",
			fmt.Sprintf("The URL %s is not valid: it must begin with https:// and carry no query.", params.URL),
			http.StatusBadRequest), nil
	}
	params.ClientIDList = iamUniqueStrings(params.ClientIDList)
	for _, id := range params.ClientIDList {
		if resp := iamValidateClientID(id); resp != nil {
			return resp, nil
		}
	}
	if len(params.ClientIDList) > iamOIDCMaxClientIDs {
		return iamErrorResponse("LimitExceeded",
			fmt.Sprintf("An OpenID Connect provider can have at most %d client IDs.", iamOIDCMaxClientIDs),
			http.StatusConflict), nil
	}
	if resp := iamValidateThumbprints(params.ThumbprintList); resp != nil {
		return resp, nil
	}
	tags, tagResp := iamCreateTagSet(params.Tags, iamTagKeysCaseSensitive)
	if tagResp != nil {
		return tagResp, nil
	}

	hostPath := iamOIDCHostPath(params.URL)
	if seeded := p.oidcCreateSeed(goCtx, params.URL); seeded != nil {
		return seeded, nil
	}

	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	existing, err := p.state.Get(goCtx, iamNamespace, iamOIDCProviderKey(ctx.AccountID, hostPath))
	if err != nil {
		return nil, fmt.Errorf("get oidc provider: %w", err)
	}
	if existing != nil {
		// AWS's wording, observed through the AWS CLI and Terraform.
		return iamErrorResponse("EntityAlreadyExists",
			fmt.Sprintf("Provider with url %s already exists.", params.URL), http.StatusConflict), nil
	}

	prov := &IAMOIDCProvider{
		ARN:            iamOIDCProviderARN(ctx.AccountID, hostPath),
		URL:            hostPath,
		ClientIDList:   params.ClientIDList,
		ThumbprintList: params.ThumbprintList,
		CreateDate:     p.now().UTC(),
		Tags:           tags,
	}
	if err := p.putOIDCProvider(goCtx, ctx.AccountID, prov); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "CreateOpenIDConnectProvider",
		"<OpenIDConnectProviderArn>"+xmlEsc(prov.ARN)+"</OpenIDConnectProviderArn>"+iamEntityTagsXML(prov.Tags))
}

// oidcCreateSeed returns the OpenIdIdpCommunicationError seeded for url (or "*"), or nil. IAM
// would contact the provider to fetch a thumbprint; substrate contacts nothing, so the error is
// reachable only through the seed. The message is substrate's own.
func (p *IAMPlugin) oidcCreateSeed(goCtx context.Context, url string) *AWSResponse {
	for _, key := range []string{url, "*"} {
		raw, err := p.state.Get(goCtx, iamCtrlNamespace, iamCtrlOIDCOutcomeKey(key))
		if err != nil || raw == nil {
			continue
		}
		return iamErrorResponse("OpenIdIdpCommunicationError",
			fmt.Sprintf("Could not communicate with the OpenID Connect provider at %s.", url),
			http.StatusBadRequest)
	}
	return nil
}

// iamOIDCProviderARNParam reads the OpenIDConnectProviderArn every operation after the create
// carries, and the operation's other parameters into dst.
func iamOIDCProviderARNParam(req *AWSRequest, dst any) (string, *AWSResponse) {
	if err := parseIAMBody(req.Body, dst); err != nil {
		return "", iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest)
	}
	var arn struct {
		ARN string `json:"OpenIDConnectProviderArn"`
	}
	if err := parseIAMBody(req.Body, &arn); err != nil {
		return "", iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest)
	}
	if arn.ARN == "" {
		return "", iamErrorResponse("ValidationError", "OpenIDConnectProviderArn is required", http.StatusBadRequest)
	}
	return arn.ARN, nil
}

func (p *IAMPlugin) getOpenIDConnectProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn, errResp := iamOIDCProviderARNParam(req, &struct{}{})
	if errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:GetOpenIDConnectProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	prov, err := p.loadOIDCProvider(goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if prov == nil {
		return iamOIDCNotFound(arn), nil
	}
	body := iamStringListXML("ClientIDList", prov.ClientIDList) +
		iamStringListXML("ThumbprintList", prov.ThumbprintList) +
		"<CreateDate>" + prov.CreateDate.UTC().Format("2006-01-02T15:04:05Z") + "</CreateDate>" +
		"<Url>" + xmlEsc(prov.URL) + "</Url>" + iamEntityTagsXML(prov.Tags)
	return iamXMLResponse(http.StatusOK, "GetOpenIDConnectProvider", body)
}

func (p *IAMPlugin) listOpenIDConnectProviders(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListOpenIDConnectProviders", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	keys, err := p.state.List(goCtx, iamNamespace, iamOIDCProviderPrefix(ctx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("list oidc providers: %w", err)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("<OpenIDConnectProviderList>")
	for _, key := range keys {
		hostPath := strings.TrimPrefix(key, iamOIDCProviderPrefix(ctx.AccountID))
		b.WriteString("<member><Arn>" + xmlEsc(iamOIDCProviderARN(ctx.AccountID, hostPath)) + "</Arn></member>")
	}
	b.WriteString("</OpenIDConnectProviderList>")
	return iamXMLResponse(http.StatusOK, "ListOpenIDConnectProviders", b.String())
}

// deleteOpenIDConnectProvider deletes a provider. Its page declares NoSuchEntity and, unlike the
// client-ID operations, makes no idempotency statement, so a missing provider is refused.
func (p *IAMPlugin) deleteOpenIDConnectProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn, errResp := iamOIDCProviderARNParam(req, &struct{}{})
	if errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:DeleteOpenIDConnectProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	prov, err := p.loadOIDCProvider(goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if prov == nil {
		return iamOIDCNotFound(arn), nil
	}
	if err := p.state.Delete(goCtx, iamNamespace, iamOIDCProviderKey(ctx.AccountID, prov.URL)); err != nil {
		return nil, fmt.Errorf("delete oidc provider: %w", err)
	}
	return iamXMLEmptyResponse("DeleteOpenIDConnectProvider"), nil
}

// updateOIDCProvider loads the provider the request names, applies change under providerMu and
// stores the result. change may answer a refusal, which leaves the record unchanged.
func (p *IAMPlugin) updateOIDCProvider(ctx *RequestContext, req *AWSRequest, op string, dst any,
	change func(*IAMOIDCProvider) *AWSResponse) (*AWSResponse, error) {
	arn, errResp := iamOIDCProviderARNParam(req, dst)
	if errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:"+op, p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	prov, err := p.loadOIDCProvider(goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if prov == nil {
		return iamOIDCNotFound(arn), nil
	}
	if resp := change(prov); resp != nil {
		return resp, nil
	}
	if err := p.putOIDCProvider(goCtx, ctx.AccountID, prov); err != nil {
		return nil, err
	}
	return iamXMLEmptyResponse(op), nil
}

// updateOpenIDConnectProviderThumbprint replaces the thumbprint list: "The list that you pass with
// this operation completely replaces the existing list of thumbprints," per its page.
func (p *IAMPlugin) updateOpenIDConnectProviderThumbprint(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		ThumbprintList []string `json:"ThumbprintList"`
	}
	return p.updateOIDCProvider(ctx, req, "UpdateOpenIDConnectProviderThumbprint", &params,
		func(prov *IAMOIDCProvider) *AWSResponse {
			if list := iamMemberList(req.Params, "ThumbprintList"); list != nil {
				params.ThumbprintList = list
			}
			if resp := iamValidateThumbprints(params.ThumbprintList); resp != nil {
				return resp
			}
			prov.ThumbprintList = params.ThumbprintList
			return nil
		})
}

// addClientIDToOpenIDConnectProvider adds a client ID. "This operation is idempotent; it does not
// fail or return an error if you add an existing client ID to the provider," per its page.
func (p *IAMPlugin) addClientIDToOpenIDConnectProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		ClientID string `json:"ClientID"`
	}
	return p.updateOIDCProvider(ctx, req, "AddClientIDToOpenIDConnectProvider", &params,
		func(prov *IAMOIDCProvider) *AWSResponse {
			if resp := iamValidateClientID(params.ClientID); resp != nil {
				return resp
			}
			if slices.Contains(prov.ClientIDList, params.ClientID) {
				return nil
			}
			if len(prov.ClientIDList) >= iamOIDCMaxClientIDs {
				return iamErrorResponse("LimitExceeded",
					fmt.Sprintf("An OpenID Connect provider can have at most %d client IDs.", iamOIDCMaxClientIDs),
					http.StatusConflict)
			}
			prov.ClientIDList = append(prov.ClientIDList, params.ClientID)
			return nil
		})
}

// removeClientIDFromOpenIDConnectProvider removes a client ID. "This operation is idempotent; it
// does not fail or return an error if you try to remove a client ID that does not exist," per
// its page.
func (p *IAMPlugin) removeClientIDFromOpenIDConnectProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		ClientID string `json:"ClientID"`
	}
	return p.updateOIDCProvider(ctx, req, "RemoveClientIDFromOpenIDConnectProvider", &params,
		func(prov *IAMOIDCProvider) *AWSResponse {
			if resp := iamValidateClientID(params.ClientID); resp != nil {
				return resp
			}
			prov.ClientIDList = slices.DeleteFunc(prov.ClientIDList, func(id string) bool { return id == params.ClientID })
			return nil
		})
}

// --- SAML providers ------------------------------------------------------------

// iamSAMLMetadata is what substrate reads out of a SAML metadata document: that it is one, and its
// `validUntil`. The keys it carries are not read, because substrate verifies no signature.
type iamSAMLMetadata struct {
	XMLName    xml.Name
	ValidUntil string `xml:"validUntil,attr"`
}

// iamParseSAMLMetadata parses a metadata document, accepting it as XML or as base64-encoded XML
// (the CreateSAMLProvider sample sends the latter). A document that is neither, or whose root is
// not an EntityDescriptor, is refused with the IAM troubleshooting guide's wording, "Could not
// parse metadata.".
func iamParseSAMLMetadata(doc string) (*time.Time, *AWSResponse) {
	parse := func(text []byte) (*iamSAMLMetadata, bool) {
		var md iamSAMLMetadata
		if err := xml.Unmarshal(text, &md); err != nil {
			return nil, false
		}
		if md.XMLName.Local != "EntityDescriptor" && md.XMLName.Local != "EntitiesDescriptor" {
			return nil, false
		}
		return &md, true
	}
	md, ok := parse([]byte(doc))
	if !ok {
		if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(doc)); err == nil {
			md, ok = parse(decoded)
		}
	}
	if !ok {
		return nil, iamErrorResponse("InvalidInput", "Could not parse metadata.", http.StatusBadRequest)
	}
	if md.ValidUntil == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, md.ValidUntil)
	if err != nil {
		return nil, iamErrorResponse("InvalidInput", "Could not parse metadata.", http.StatusBadRequest)
	}
	t = t.UTC()
	return &t, nil
}

// iamSAMLEncryptionModes are the values AssertionEncryptionMode documents.
var iamSAMLEncryptionModes = []string{"Required", "Allowed"}

// iamSAMLKeyIDChars is the alphabet of a SAML private key ID, from the RemovePrivateKey pattern
// `[A-Z0-9]+`.
const iamSAMLKeyIDChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func (p *IAMPlugin) createSAMLProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		Name                    string   `json:"Name"`
		SAMLMetadataDocument    string   `json:"SAMLMetadataDocument"`
		AssertionEncryptionMode string   `json:"AssertionEncryptionMode"`
		AddPrivateKey           string   `json:"AddPrivateKey"`
		Tags                    []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}
	if params.Name == "" {
		return iamErrorResponse("ValidationError", "Name is required", http.StatusBadRequest), nil
	}
	if params.SAMLMetadataDocument == "" {
		return iamErrorResponse("ValidationError", "SAMLMetadataDocument is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:CreateSAMLProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if len(params.Name) > 128 || strings.ContainsAny(params.Name, "/: ") {
		return iamErrorResponse("ValidationError",
			"Name must be 1 to 128 characters from [\\w._-].", http.StatusBadRequest), nil
	}
	if resp := iamSAMLCheckEncryption(params.AssertionEncryptionMode, params.AddPrivateKey != "", false); resp != nil {
		return resp, nil
	}
	validUntil, errResp := iamParseSAMLMetadata(params.SAMLMetadataDocument)
	if errResp != nil {
		return errResp, nil
	}
	tags, tagResp := iamCreateTagSet(params.Tags, iamTagKeysCaseSensitive)
	if tagResp != nil {
		return tagResp, nil
	}

	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	existing, err := p.state.Get(goCtx, iamNamespace, iamSAMLProviderKey(ctx.AccountID, params.Name))
	if err != nil {
		return nil, fmt.Errorf("get saml provider: %w", err)
	}
	if existing != nil {
		return iamErrorResponse("EntityAlreadyExists",
			fmt.Sprintf("SAML provider %s already exists.", params.Name), http.StatusConflict), nil
	}
	now := p.now().UTC()
	prov := &IAMSAMLProvider{
		ARN:                     iamSAMLProviderARN(ctx.AccountID, params.Name),
		Name:                    params.Name,
		SAMLMetadataDocument:    params.SAMLMetadataDocument,
		SAMLProviderUUID:        ctx.IDs.Chars(22, iamSAMLKeyIDChars),
		AssertionEncryptionMode: params.AssertionEncryptionMode,
		CreateDate:              now,
		ValidUntil:              validUntil,
		Tags:                    tags,
	}
	if params.AddPrivateKey != "" {
		prov.PrivateKeyList = []IAMSAMLPrivateKey{{KeyID: "SAML" + ctx.IDs.Chars(18, iamSAMLKeyIDChars), Timestamp: now}}
	}
	if err := p.putSAMLProvider(goCtx, ctx.AccountID, prov); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "CreateSAMLProvider",
		"<SAMLProviderArn>"+xmlEsc(prov.ARN)+"</SAMLProviderArn>"+iamEntityTagsXML(prov.Tags))
}

// iamSAMLCheckEncryption validates AssertionEncryptionMode against the private keys the provider
// will hold, refusing Required without a key in the IAM troubleshooting guide's wording.
func iamSAMLCheckEncryption(mode string, addingKey, holdsKey bool) *AWSResponse {
	if mode == "" {
		return nil
	}
	if !slices.Contains(iamSAMLEncryptionModes, mode) {
		return iamErrorResponse("ValidationError",
			iamEnumMessage("assertionEncryptionMode", mode, iamSAMLEncryptionModes), http.StatusBadRequest)
	}
	if mode == "Required" && !addingKey && !holdsKey {
		return iamErrorResponse("InvalidInput",
			"Unable to set assertion encryption mode to Required because no private key is provided.",
			http.StatusBadRequest)
	}
	return nil
}

// iamSAMLProviderARNParam reads the SAMLProviderArn every operation after the create carries.
func iamSAMLProviderARNParam(req *AWSRequest) (string, *AWSResponse) {
	var arn struct {
		ARN string `json:"SAMLProviderArn"`
	}
	if err := parseIAMBody(req.Body, &arn); err != nil {
		return "", iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest)
	}
	if arn.ARN == "" {
		return "", iamErrorResponse("ValidationError", "SAMLProviderArn is required", http.StatusBadRequest)
	}
	return arn.ARN, nil
}

func (p *IAMPlugin) getSAMLProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn, errResp := iamSAMLProviderARNParam(req)
	if errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:GetSAMLProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	prov, err := p.loadSAMLProvider(goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if prov == nil {
		return iamSAMLNotFound(arn), nil
	}
	var b strings.Builder
	if prov.AssertionEncryptionMode != "" {
		b.WriteString("<AssertionEncryptionMode>" + xmlEsc(prov.AssertionEncryptionMode) + "</AssertionEncryptionMode>")
	}
	b.WriteString("<CreateDate>" + prov.CreateDate.UTC().Format("2006-01-02T15:04:05Z") + "</CreateDate>")
	if prov.ValidUntil != nil {
		b.WriteString("<ValidUntil>" + prov.ValidUntil.UTC().Format("2006-01-02T15:04:05Z") + "</ValidUntil>")
	}
	b.WriteString("<SAMLMetadataDocument>" + xmlEsc(prov.SAMLMetadataDocument) + "</SAMLMetadataDocument>")
	b.WriteString("<SAMLProviderUUID>" + xmlEsc(prov.SAMLProviderUUID) + "</SAMLProviderUUID>")
	if len(prov.PrivateKeyList) > 0 {
		b.WriteString("<PrivateKeyList>")
		for _, k := range prov.PrivateKeyList {
			b.WriteString("<member><KeyId>" + xmlEsc(k.KeyID) + "</KeyId><Timestamp>" +
				k.Timestamp.UTC().Format("2006-01-02T15:04:05Z") + "</Timestamp></member>")
		}
		b.WriteString("</PrivateKeyList>")
	}
	b.WriteString(iamEntityTagsXML(prov.Tags))
	return iamXMLResponse(http.StatusOK, "GetSAMLProvider", b.String())
}

func (p *IAMPlugin) listSAMLProviders(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListSAMLProviders", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	keys, err := p.state.List(goCtx, iamNamespace, iamSAMLProviderPrefix(ctx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("list saml providers: %w", err)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("<SAMLProviderList>")
	for _, key := range keys {
		raw, getErr := p.state.Get(goCtx, iamNamespace, key)
		if getErr != nil {
			return nil, fmt.Errorf("get saml provider: %w", getErr)
		}
		if raw == nil {
			continue
		}
		var prov IAMSAMLProvider
		if err := json.Unmarshal(raw, &prov); err != nil {
			return nil, fmt.Errorf("unmarshal saml provider: %w", err)
		}
		b.WriteString("<member><Arn>" + xmlEsc(prov.ARN) + "</Arn>")
		if prov.ValidUntil != nil {
			b.WriteString("<ValidUntil>" + prov.ValidUntil.UTC().Format("2006-01-02T15:04:05Z") + "</ValidUntil>")
		}
		b.WriteString("<CreateDate>" + prov.CreateDate.UTC().Format("2006-01-02T15:04:05Z") + "</CreateDate></member>")
	}
	b.WriteString("</SAMLProviderList>")
	return iamXMLResponse(http.StatusOK, "ListSAMLProviders", b.String())
}

// updateSAMLProvider applies the metadata, encryption-mode and private-key changes UpdateSAMLProvider
// documents. The refusals use the IAM troubleshooting guide's wording.
func (p *IAMPlugin) updateSAMLProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn, errResp := iamSAMLProviderARNParam(req)
	if errResp != nil {
		return errResp, nil
	}
	var params struct {
		SAMLMetadataDocument    string `json:"SAMLMetadataDocument"`
		AssertionEncryptionMode string `json:"AssertionEncryptionMode"`
		AddPrivateKey           string `json:"AddPrivateKey"`
		RemovePrivateKey        string `json:"RemovePrivateKey"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:UpdateSAMLProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if params.SAMLMetadataDocument == "" && params.AssertionEncryptionMode == "" &&
		params.AddPrivateKey == "" && params.RemovePrivateKey == "" {
		return iamErrorResponse("InvalidInput",
			"Unable to update identity provider. No updates are defined for metadata or encryption assertion.",
			http.StatusBadRequest), nil
	}
	if params.AddPrivateKey != "" && params.RemovePrivateKey != "" {
		return iamErrorResponse("InvalidInput",
			"Unable to add and remove private keys in the same request. Set a value for only one of the two parameters.",
			http.StatusBadRequest), nil
	}

	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	prov, err := p.loadSAMLProvider(goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if prov == nil {
		return iamSAMLNotFound(arn), nil
	}
	if params.SAMLMetadataDocument != "" {
		validUntil, mdResp := iamParseSAMLMetadata(params.SAMLMetadataDocument)
		if mdResp != nil {
			return mdResp, nil
		}
		prov.SAMLMetadataDocument, prov.ValidUntil = params.SAMLMetadataDocument, validUntil
	}
	if params.AddPrivateKey != "" {
		if len(prov.PrivateKeyList) >= iamSAMLMaxPrivateKeys {
			return iamErrorResponse("LimitExceeded", "Private key limit of 2 is reached.", http.StatusConflict), nil
		}
		prov.PrivateKeyList = append(prov.PrivateKeyList, IAMSAMLPrivateKey{
			KeyID: "SAML" + ctx.IDs.Chars(18, iamSAMLKeyIDChars), Timestamp: p.now().UTC()})
	}
	if params.RemovePrivateKey != "" {
		i := slices.IndexFunc(prov.PrivateKeyList, func(k IAMSAMLPrivateKey) bool { return k.KeyID == params.RemovePrivateKey })
		if i < 0 {
			return iamErrorResponse("InvalidInput",
				"Failed to remove private key because the Key ID does not match a private key.",
				http.StatusBadRequest), nil
		}
		mode := prov.AssertionEncryptionMode
		if params.AssertionEncryptionMode != "" {
			mode = params.AssertionEncryptionMode
		}
		if mode == "Required" && len(prov.PrivateKeyList) == 1 {
			return iamErrorResponse("InvalidInput", "Failed to remove private key.", http.StatusBadRequest), nil
		}
		prov.PrivateKeyList = slices.Delete(prov.PrivateKeyList, i, i+1)
	}
	if resp := iamSAMLCheckEncryption(params.AssertionEncryptionMode, false, len(prov.PrivateKeyList) > 0); resp != nil {
		return resp, nil
	}
	if params.AssertionEncryptionMode != "" {
		prov.AssertionEncryptionMode = params.AssertionEncryptionMode
	}
	if err := p.putSAMLProvider(goCtx, ctx.AccountID, prov); err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "UpdateSAMLProvider", "<SAMLProviderArn>"+xmlEsc(prov.ARN)+"</SAMLProviderArn>")
}

func (p *IAMPlugin) deleteSAMLProvider(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn, errResp := iamSAMLProviderARNParam(req)
	if errResp != nil {
		return errResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:DeleteSAMLProvider", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	prov, err := p.loadSAMLProvider(goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if prov == nil {
		return iamSAMLNotFound(arn), nil
	}
	if err := p.state.Delete(goCtx, iamNamespace, iamSAMLProviderKey(ctx.AccountID, prov.Name)); err != nil {
		return nil, fmt.Errorf("delete saml provider: %w", err)
	}
	return iamXMLEmptyResponse("DeleteSAMLProvider"), nil
}

// --- Provider tagging ------------------------------------------------------------

// loadTaggedOIDCProvider returns the OIDC provider arn names as a tag record, or nil.
func (p *IAMPlugin) loadTaggedOIDCProvider(goCtx context.Context, accountID, arn string) (*iamTagRecord, error) {
	prov, err := p.loadOIDCProvider(goCtx, accountID, arn)
	if err != nil || prov == nil {
		return nil, err
	}
	return &iamTagRecord{tags: prov.Tags, store: func(ctx context.Context, tags []IAMTag) error {
		prov.Tags = tags
		return p.putOIDCProvider(ctx, accountID, prov)
	}}, nil
}

// loadTaggedSAMLProvider returns the SAML provider arn names as a tag record, or nil.
func (p *IAMPlugin) loadTaggedSAMLProvider(goCtx context.Context, accountID, arn string) (*iamTagRecord, error) {
	prov, err := p.loadSAMLProvider(goCtx, accountID, arn)
	if err != nil || prov == nil {
		return nil, err
	}
	return &iamTagRecord{tags: prov.Tags, store: func(ctx context.Context, tags []IAMTag) error {
		prov.Tags = tags
		return p.putSAMLProvider(ctx, accountID, prov)
	}}, nil
}

// iamProviderTagKind is one identity-provider type's half of the six tagging operations: the
// parameter its ARN arrives in, how its record is loaded, and its NoSuchEntity.
type iamProviderTagKind struct {
	arnParam string
	load     func(p *IAMPlugin, goCtx context.Context, accountID, arn string) (*iamTagRecord, error)
	notFound func(arn string) *AWSResponse
}

var (
	iamOIDCTagKind = iamProviderTagKind{"OpenIDConnectProviderArn", (*IAMPlugin).loadTaggedOIDCProvider, iamOIDCNotFound}
	iamSAMLTagKind = iamProviderTagKind{"SAMLProviderArn", (*IAMPlugin).loadTaggedSAMLProvider, iamSAMLNotFound}
)

// providerARN reads the kind's ARN parameter.
func (k iamProviderTagKind) providerARN(req *AWSRequest) (string, *AWSResponse) {
	var params map[string]json.RawMessage
	if err := parseIAMBody(req.Body, &params); err != nil {
		return "", iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest)
	}
	var arn string
	if raw, ok := params[k.arnParam]; ok {
		if err := json.Unmarshal(raw, &arn); err != nil {
			return "", iamErrorResponse("ValidationError", k.arnParam+" must be a string", http.StatusBadRequest)
		}
	}
	if arn == "" {
		return "", iamErrorResponse("ValidationError", k.arnParam+" is required", http.StatusBadRequest)
	}
	return arn, nil
}

// tagProvider handles Tag{OpenIDConnect,SAML}Provider.
func (p *IAMPlugin) tagProvider(ctx *RequestContext, req *AWSRequest, kind iamProviderTagKind) (*AWSResponse, error) {
	arn, errResp := kind.providerARN(req)
	if errResp != nil {
		return errResp, nil
	}
	var params struct {
		Tags []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:"+req.Operation, p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if resp := iamValidateTagSet(params.Tags); resp != nil {
		return resp, nil
	}
	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	record, err := kind.load(p, goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return kind.notFound(arn), nil
	}
	merged := iamMergeTagSet(record.tags, params.Tags, iamTagKeysCaseSensitive)
	if resp := iamCheckTagLimit(merged); resp != nil {
		return resp, nil
	}
	if err := record.store(goCtx, merged); err != nil {
		return nil, fmt.Errorf("%s: %w", req.Operation, err)
	}
	return iamXMLEmptyResponse(req.Operation), nil
}

// untagProvider handles Untag{OpenIDConnect,SAML}Provider.
func (p *IAMPlugin) untagProvider(ctx *RequestContext, req *AWSRequest, kind iamProviderTagKind) (*AWSResponse, error) {
	arn, errResp := kind.providerARN(req)
	if errResp != nil {
		return errResp, nil
	}
	var params struct {
		TagKeys []string `json:"TagKeys"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if keys := iamMemberList(req.Params, "TagKeys"); keys != nil {
		params.TagKeys = keys
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:"+req.Operation, p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if resp := iamValidateTagKeys(params.TagKeys); resp != nil {
		return resp, nil
	}
	p.providerMu.Lock()
	defer p.providerMu.Unlock()
	record, err := kind.load(p, goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return kind.notFound(arn), nil
	}
	if err := record.store(goCtx, iamRemoveTagKeys(record.tags, params.TagKeys, iamTagKeysCaseSensitive)); err != nil {
		return nil, fmt.Errorf("%s: %w", req.Operation, err)
	}
	return iamXMLEmptyResponse(req.Operation), nil
}

// listProviderTags handles List{OpenIDConnect,SAML}ProviderTags.
func (p *IAMPlugin) listProviderTags(ctx *RequestContext, req *AWSRequest, kind iamProviderTagKind) (*AWSResponse, error) {
	arn, errResp := kind.providerARN(req)
	if errResp != nil {
		return errResp, nil
	}
	var params struct {
		Marker   string `json:"Marker"`
		MaxItems iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	pg, pgResp := iamMarker(req, params.MaxItems, params.Marker)
	if pgResp != nil {
		return pgResp, nil
	}
	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:"+req.Operation, p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	record, err := kind.load(p, goCtx, ctx.AccountID, arn)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return kind.notFound(arn), nil
	}
	return iamTagListingResponse(req.Operation, record.tags, pg)
}
