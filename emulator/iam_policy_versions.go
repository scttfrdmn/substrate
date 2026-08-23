package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// Managed-policy versions: GetPolicyVersion and ListPolicyVersions (#498).
//
// Before these, a managed policy's *document* was not observable over the wire at all.
// GetPolicy returns metadata only — PolicyId, PolicyName, Arn, Path, DefaultVersionId,
// AttachmentCount, CreateDate, UpdateDate — which matches AWS, and the document lives
// behind GetPolicyVersion. So the 52 bundled documents, several copied verbatim from
// their AWS reference pages, were readable by a Go consumer through GetManagedPolicy and
// by no consumer over HTTP.
//
// Substrate models exactly one version per policy: IAMPolicy holds a single Document and
// a DefaultVersionID, and CreatePolicyVersion/SetDefaultPolicyVersion/
// DeletePolicyVersion do not exist. That shapes both operations, and the shape is stated
// rather than papered over — see iamPolicyVersionFor.

// iamPolicyVersionIDPattern is policyVersionIdType from the API model:
// "v[1-9][0-9]*(\.[A-Za-z0-9-]*)?".
//
// Anchored, because the model's pattern constrains the whole value — an unanchored match
// would accept "xxv1yy", which no client sends and which would then be compared against
// a real version ID and reported as NoSuchEntity rather than as the malformed input it
// is.
var iamPolicyVersionIDPattern = regexp.MustCompile(`^v[1-9][0-9]*(\.[A-Za-z0-9-]*)?$`)

// getPolicyVersion returns one version of a managed policy, including its document.
func (p *IAMPlugin) getPolicyVersion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
		VersionId string `json:"VersionId"` //nolint:revive,staticcheck // matches the API member name.
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", iamParamMessage(err), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" || params.VersionId == "" {
		return iamErrorResponse("ValidationError",
			"PolicyArn and VersionId are required", http.StatusBadRequest), nil
	}
	// The pattern is checked before the policy is looked up, because a malformed version
	// ID is InvalidInput whether or not the policy exists: reporting NoSuchEntity for it
	// would tell the caller to go looking for a policy when the parameter is the problem.
	if !iamPolicyVersionIDPattern.MatchString(params.VersionId) {
		return iamErrorResponse("InvalidInput",
			fmt.Sprintf("The specified value for versionId is invalid. It must match the pattern %s.",
				iamPolicyVersionIDPattern.String()),
			http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:GetPolicyVersion", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	policy, errResp, err := p.resolveManagedPolicy(goCtx, params.PolicyArn)
	if err != nil {
		return nil, err
	}
	if errResp != nil {
		return errResp, nil
	}

	version, ok := iamPolicyVersionFor(policy, params.VersionId)
	if !ok {
		return iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Policy %s version %s does not exist or is not attachable.",
				params.PolicyArn, params.VersionId),
			http.StatusNotFound), nil
	}

	body, err := iamPolicyVersionXML(version, true)
	if err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "GetPolicyVersion", "<PolicyVersion>"+body+"</PolicyVersion>")
}

// listPolicyVersions lists a managed policy's versions, without their documents.
//
// The document is deliberately absent: the model documents PolicyVersion.Document as
// returned by GetPolicyVersion and GetAccountAuthorizationDetails and *not* by
// ListPolicyVersions or CreatePolicyVersion. Emitting it here would hand a caller a
// member AWS does not send, which is the kind of difference that makes a consumer work
// against substrate and fail against AWS.
func (p *IAMPlugin) listPolicyVersions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
		Marker    string `json:"Marker"`
		MaxItems  iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", iamParamMessage(err), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListPolicyVersions", "*"); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	policy, errResp, err := p.resolveManagedPolicy(goCtx, params.PolicyArn)
	if err != nil {
		return nil, err
	}
	if errResp != nil {
		return errResp, nil
	}

	// One version, so Marker and MaxItems cannot truncate anything. They are still
	// accepted and reported: a caller that pages this operation must see IsTruncated
	// false rather than a missing element, and MaxItems must not fail the request the
	// way it did before #642.
	//
	// The default version is taken directly rather than looked up by ID: passing
	// policy.DefaultVersionID back through iamPolicyVersionFor would find nothing for a
	// policy whose stored ID is empty, and the nil result would be encoded.
	body, err := iamPolicyVersionXML(iamPolicyDefaultVersion(policy), false)
	if err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "ListPolicyVersions",
		"<Versions><member>"+body+"</member></Versions>"+
			"<IsTruncated>"+iamBoolXML(false)+"</IsTruncated>")
}

// resolveManagedPolicy resolves a policy ARN to its entity, catalog before state — the
// same order every other IAM caller uses, so a bundled and a customer-managed policy
// behave alike.
//
// A returned errResp is an answer for the caller (NoSuchEntity); a returned error is
// substrate's own failure.
func (p *IAMPlugin) resolveManagedPolicy(goCtx context.Context, arn string) (*IAMPolicy, *AWSResponse, error) {
	if mp, ok := GetManagedPolicy(arn); ok {
		return mp, nil, nil
	}
	raw, err := p.state.Get(goCtx, iamNamespace, iamPolicyKey(arn))
	if err != nil {
		return nil, nil, fmt.Errorf("get policy: %w", err)
	}
	if raw == nil {
		return nil, iamErrorResponse("NoSuchEntity",
			fmt.Sprintf("Policy %s was not found.", arn), http.StatusNotFound), nil
	}
	var policy IAMPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return nil, nil, fmt.Errorf("unmarshal policy: %w", err)
	}
	return &policy, nil, nil
}

// iamPolicyVersion is one row of a policy's version list.
type iamPolicyVersion struct {
	// policy is the policy the version belongs to, for its document and dates.
	policy *IAMPolicy

	// versionID is the version's own ID.
	versionID string

	// isDefault reports whether this is the policy's default version.
	isDefault bool
}

// iamPolicyVersionFor returns the named version of policy, reporting whether it exists.
//
// Substrate stores one document per policy, so the only version that exists is the one
// the policy names as its default. A request for any other version is NoSuchEntity, which
// is the honest answer: claiming v1 exists when the policy reports v3 as its default
// would hand back v3's document under v1's name, and a consumer pinning a version would
// be told a document is v1 that AWS would report as something else. The bundled catalog
// makes this immediately reachable — AmazonSSMManagedInstanceCore reports v2 and
// AWSLambdaVPCAccessExecutionRole reports v3 — rather than hypothetical.
func iamPolicyVersionFor(policy *IAMPolicy, versionID string) (iamPolicyVersion, bool) {
	version := iamPolicyDefaultVersion(policy)
	if version.policy == nil || versionID != version.versionID {
		return iamPolicyVersion{}, false
	}
	return version, true
}

// iamPolicyDefaultVersion returns policy's default version — the only one substrate
// stores.
//
// An empty DefaultVersionID falls back to "v1", which is what CreatePolicy assigns, so a
// policy written by an older release still reports a version ID rather than an empty
// element.
func iamPolicyDefaultVersion(policy *IAMPolicy) iamPolicyVersion {
	if policy == nil {
		return iamPolicyVersion{}
	}
	versionID := policy.DefaultVersionID
	if versionID == "" {
		versionID = "v1"
	}
	return iamPolicyVersion{policy: policy, versionID: versionID, isDefault: true}
}

// iamPolicyVersionXML encodes a PolicyVersion, including the document only when
// includeDocument is set.
func iamPolicyVersionXML(version iamPolicyVersion, includeDocument bool) (string, error) {
	var b strings.Builder
	b.WriteString("<VersionId>")
	b.WriteString(xmlEsc(version.versionID))
	b.WriteString("</VersionId><IsDefaultVersion>")
	b.WriteString(iamBoolXML(version.isDefault))
	b.WriteString("</IsDefaultVersion><CreateDate>")
	b.WriteString(version.policy.CreateDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")

	if includeDocument {
		raw, err := json.Marshal(version.policy.Document)
		if err != nil {
			return "", fmt.Errorf("marshal policy document: %w", err)
		}
		b.WriteString("<Document>")
		b.WriteString(xmlEsc(iamEscapeRFC3986(string(raw))))
		b.WriteString("</Document>")
	}
	return b.String(), nil
}

// iamEscapeRFC3986 percent-encodes s, leaving only RFC 3986's unreserved characters
// (ALPHA / DIGIT / "-" / "." / "_" / "~") as themselves.
//
// Neither stdlib escaper is this. url.QueryEscape encodes a space as "+", which a
// URL-decoder that follows RFC 3986 reads back as a literal plus, silently corrupting a
// document containing a space. url.PathEscape leaves ":" and several sub-delimiters bare,
// so its output differs from what AWS sends. GetPolicyVersion's contract is specifically
// "URL-encoded compliant with RFC 3986", and a consumer decoding the value with a strict
// decoder is the case that has to work — most SDKs decode automatically, so a difference
// here breaks the raw HTTP client and no one else, which is exactly the sort of gap that
// only shows up in production.
func iamEscapeRFC3986(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isRFC3986Unreserved(c) {
			b.WriteByte(c)
			continue
		}
		// Byte-at-a-time, so a multi-byte UTF-8 rune is encoded as its bytes, which is
		// what RFC 3986 §2.5 requires.
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// isRFC3986Unreserved reports whether c is in RFC 3986's unreserved set.
func isRFC3986Unreserved(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '.', c == '_', c == '~':
		return true
	}
	return false
}
