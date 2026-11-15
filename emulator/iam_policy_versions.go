package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Managed-policy versions: GetPolicyVersion and ListPolicyVersions (#498), and the three
// operations that change a policy's version list — CreatePolicyVersion, DeletePolicyVersion and
// SetDefaultPolicyVersion (#1560).
//
// Before #498 a managed policy's *document* was not observable over the wire at all. GetPolicy
// returns metadata only — PolicyId, PolicyName, Arn, Path, DefaultVersionId, AttachmentCount,
// CreateDate, UpdateDate — which matches AWS, and the document lives behind GetPolicyVersion.
//
// Until #1560 substrate modeled exactly one version per policy. A customer managed policy now
// holds up to five: "A managed policy can have up to five versions. If the policy has five
// versions, you must delete an existing version using DeletePolicyVersion before you create a new
// version" (API_CreatePolicyVersion). The policy's Document, DocumentText and DefaultVersionID
// always mirror its default version, which is what keeps the evaluator — which reads Document —
// unaware that versions exist: the default version "is the version that is in effect for the IAM
// users, groups, and roles to which the policy is attached", and the others are not in effect.
//
// A bundled AWS managed policy still has exactly its one stored version. Its other published
// versions are not bundled, so a request for any of them is NoSuchEntity — see
// [iamPolicyVersionFor] — and the three write operations refuse an AWS managed ARN, for the
// reason [IAMPlugin.deletePolicy] gives.

// iamMaxPolicyVersions is the five-version limit API_CreatePolicyVersion publishes.
const iamMaxPolicyVersions = 5

// iamPolicyVersionIDPattern is policyVersionIdType from the API model:
// "v[1-9][0-9]*(\.[A-Za-z0-9-]*)?".
//
// Anchored, because the model's pattern constrains the whole value — an unanchored match
// would accept "xxv1yy", which no client sends and which would then be compared against
// a real version ID and reported as NoSuchEntity rather than as the malformed input it
// is.
var iamPolicyVersionIDPattern = regexp.MustCompile(`^v[1-9][0-9]*(\.[A-Za-z0-9-]*)?$`)

// iamVersionIDInvalid answers InvalidInput for a version ID outside the model's pattern. The
// message is substrate's own; the code is the one every version operation publishes.
func iamVersionIDInvalid() *AWSResponse {
	return iamErrorResponse("InvalidInput",
		fmt.Sprintf("The specified value for versionId is invalid. It must match the pattern %s.",
			iamPolicyVersionIDPattern.String()),
		http.StatusBadRequest)
}

// iamNoSuchPolicyVersion answers NoSuchEntity for a version the policy does not hold.
func iamNoSuchPolicyVersion(arn, versionID string) *AWSResponse {
	return iamErrorResponse("NoSuchEntity",
		fmt.Sprintf("Policy %s version %s does not exist or is not attachable.", arn, versionID),
		http.StatusNotFound)
}

// getPolicyVersion returns one version of a managed policy, including its document.
func (p *IAMPlugin) getPolicyVersion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
		VersionId string `json:"VersionId"` //nolint:revive,staticcheck // matches the API member name.
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		// parseIAMBody has already named the parameter, so the wrapped message is final.
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" || params.VersionId == "" {
		return iamErrorResponse("ValidationError",
			"PolicyArn and VersionId are required", http.StatusBadRequest), nil
	}
	// The pattern is checked before the policy is looked up, because a malformed version
	// ID is InvalidInput whether or not the policy exists: reporting NoSuchEntity for it
	// would tell the caller to go looking for a policy when the parameter is the problem.
	if !iamPolicyVersionIDPattern.MatchString(params.VersionId) {
		return iamVersionIDInvalid(), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:GetPolicyVersion", p.authzResource(ctx, req)); err != nil {
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
		return iamNoSuchPolicyVersion(params.PolicyArn, params.VersionId), nil
	}

	body, err := iamPolicyVersionXML(version, true)
	if err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "GetPolicyVersion", "<PolicyVersion>"+body+"</PolicyVersion>")
}

// listPolicyVersions lists a managed policy's versions, newest first, without their documents.
//
// The document is deliberately absent: the model documents PolicyVersion.Document as
// returned by GetPolicyVersion and GetAccountAuthorizationDetails and *not* by
// ListPolicyVersions or CreatePolicyVersion. Emitting it here would hand a caller a
// member AWS does not send, which is the kind of difference that makes a consumer work
// against substrate and fail against AWS.
//
// Newest first is the order of the page's own sample response, which lists v3, v2, v1. MaxItems
// pages the list; the Marker is the version ID the next page starts at, and a Marker naming no
// version of the policy is InvalidInput rather than page one (the message is substrate's own).
func (p *IAMPlugin) listPolicyVersions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
		Marker    string `json:"Marker"`
		MaxItems  iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		// parseIAMBody has already named the parameter, so the wrapped message is final.
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}

	if errResp := iamValidateMaxItems(req, params.MaxItems); errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:ListPolicyVersions", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	policy, errResp, err := p.resolveManagedPolicy(goCtx, params.PolicyArn)
	if err != nil {
		return nil, err
	}
	if errResp != nil {
		return errResp, nil
	}

	versions := iamPolicyVersions(policy)
	slices.Reverse(versions)
	start := 0
	if params.Marker != "" {
		start = slices.IndexFunc(versions, func(v iamPolicyVersion) bool { return v.versionID == params.Marker })
		if start < 0 {
			return iamErrorResponse("InvalidInput",
				fmt.Sprintf("The marker %s names no version of policy %s.", params.Marker, params.PolicyArn),
				http.StatusBadRequest), nil
		}
	}
	end := len(versions)
	if limit := params.MaxItems.Int(); limit > 0 && start+limit < end {
		end = start + limit
	}

	var b strings.Builder
	b.WriteString("<Versions>")
	for _, v := range versions[start:end] {
		body, err := iamPolicyVersionXML(v, false)
		if err != nil {
			return nil, err
		}
		b.WriteString("<member>" + body + "</member>")
	}
	b.WriteString("</Versions><IsTruncated>")
	b.WriteString(iamBoolXML(end < len(versions)))
	b.WriteString("</IsTruncated>")
	if end < len(versions) {
		b.WriteString("<Marker>" + xmlEsc(versions[end].versionID) + "</Marker>")
	}
	return iamXMLResponse(http.StatusOK, "ListPolicyVersions", b.String())
}

// createPolicyVersion adds a version to a customer managed policy, making it the default when
// SetAsDefault is true (#1560).
//
// The five-version limit is LimitExceeded/409, published on this operation; its message is the
// API reference's own sentence. A version's number is one more than the highest the policy has
// ever had, so a number freed by DeletePolicyVersion is not reused. UpdateDate becomes the new
// version's creation time: "When a policy has more than one version, this field contains the date
// and time when the most recent policy version was created" (the ManagedPolicy UpdateDate
// attribute).
func (p *IAMPlugin) createPolicyVersion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn      string  `json:"PolicyArn"`
		PolicyDocument string  `json:"PolicyDocument"`
		SetAsDefault   iamBool `json:"SetAsDefault"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" || params.PolicyDocument == "" {
		return iamErrorResponse("ValidationError",
			"PolicyArn and PolicyDocument are required", http.StatusBadRequest), nil
	}
	if message, ok := iamValidatePolicyARN(params.PolicyArn); !ok {
		return iamErrorResponse("InvalidInput", message, http.StatusBadRequest), nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:CreatePolicyVersion", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if resp := iamRefuseAWSManagedWrite(params.PolicyArn); resp != nil {
		return resp, nil
	}

	var doc PolicyDocument
	if err := json.Unmarshal([]byte(params.PolicyDocument), &doc); err != nil {
		return iamErrorResponse("MalformedPolicyDocument", //nolint:nilerr
			"PolicyDocument is not valid JSON.", http.StatusBadRequest), nil
	}
	if err := iamValidateConditionOperators(doc); err != nil {
		return nil, err
	}

	p.policyMu.Lock()
	defer p.policyMu.Unlock()

	policy, errResp, err := p.loadCustomerPolicy(goCtx, params.PolicyArn)
	if err != nil || errResp != nil {
		return errResp, err
	}
	versions := iamPolicyVersionRecords(policy)
	if len(versions) >= iamMaxPolicyVersions {
		// API_CreatePolicyVersion's own description of the limit.
		return iamErrorResponse("LimitExceeded",
			"A managed policy can have up to five versions. If the policy has five versions, you "+
				"must delete an existing version using DeletePolicyVersion before you create a new version.",
			http.StatusConflict), nil
	}

	now := p.now().UTC()
	number := iamPolicyLastVersionNumber(policy) + 1
	record := IAMPolicyVersionRecord{
		VersionID:    "v" + strconv.Itoa(number),
		Document:     doc,
		DocumentText: params.PolicyDocument,
		CreateDate:   now,
	}
	policy.Versions = append(versions, record)
	policy.LastVersionNumber = number
	policy.UpdateDate = now
	if params.SetAsDefault.Bool() {
		iamApplyDefaultVersion(policy, record)
	}
	if err := p.putCustomerPolicy(goCtx, policy); err != nil {
		return nil, err
	}

	body, err := iamPolicyVersionXML(iamPolicyVersion{
		versionID: record.VersionID, isDefault: params.SetAsDefault.Bool(), createDate: record.CreateDate,
	}, false)
	if err != nil {
		return nil, err
	}
	return iamXMLResponse(http.StatusOK, "CreatePolicyVersion", "<PolicyVersion>"+body+"</PolicyVersion>")
}

// deletePolicyVersion removes a non-default version of a customer managed policy (#1560).
//
// "You cannot delete the default version from a policy using this operation. To delete the
// default version from a policy, use DeletePolicy" (API_DeletePolicyVersion), refused as
// DeleteConflict/409, which the operation publishes; the message is that sentence.
func (p *IAMPlugin) deletePolicyVersion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	policyArn, versionID, errResp := iamParseVersionTarget(req)
	if errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:DeletePolicyVersion", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if resp := iamRefuseAWSManagedWrite(policyArn); resp != nil {
		return resp, nil
	}

	p.policyMu.Lock()
	defer p.policyMu.Unlock()

	policy, errResp, err := p.loadCustomerPolicy(goCtx, policyArn)
	if err != nil || errResp != nil {
		return errResp, err
	}
	versions := iamPolicyVersionRecords(policy)
	i := slices.IndexFunc(versions, func(v IAMPolicyVersionRecord) bool { return v.VersionID == versionID })
	if i < 0 {
		return iamNoSuchPolicyVersion(policyArn, versionID), nil
	}
	if versionID == iamPolicyDefaultVersionID(policy) {
		return iamErrorResponse("DeleteConflict",
			"You cannot delete the default version from a policy using this operation. "+
				"To delete the default version from a policy, use DeletePolicy.",
			http.StatusConflict), nil
	}
	policy.LastVersionNumber = iamPolicyLastVersionNumber(policy)
	policy.Versions = slices.Delete(versions, i, i+1)
	if err := p.putCustomerPolicy(goCtx, policy); err != nil {
		return nil, err
	}
	return iamXMLEmptyResponse("DeletePolicyVersion"), nil
}

// setDefaultPolicyVersion makes one of a customer managed policy's versions its default, which
// "affects all users, groups, and roles that the policy is attached to" (#1560): the next
// authorization decision reads the new default's document.
func (p *IAMPlugin) setDefaultPolicyVersion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	policyArn, versionID, errResp := iamParseVersionTarget(req)
	if errResp != nil {
		return errResp, nil
	}

	goCtx := context.Background()
	if err := p.authorize(goCtx, ctx, "iam:SetDefaultPolicyVersion", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}
	if resp := iamRefuseAWSManagedWrite(policyArn); resp != nil {
		return resp, nil
	}

	p.policyMu.Lock()
	defer p.policyMu.Unlock()

	policy, errResp, err := p.loadCustomerPolicy(goCtx, policyArn)
	if err != nil || errResp != nil {
		return errResp, err
	}
	versions := iamPolicyVersionRecords(policy)
	i := slices.IndexFunc(versions, func(v IAMPolicyVersionRecord) bool { return v.VersionID == versionID })
	if i < 0 {
		return iamNoSuchPolicyVersion(policyArn, versionID), nil
	}
	policy.LastVersionNumber = iamPolicyLastVersionNumber(policy)
	policy.Versions = versions
	iamApplyDefaultVersion(policy, versions[i])
	if err := p.putCustomerPolicy(goCtx, policy); err != nil {
		return nil, err
	}
	return iamXMLEmptyResponse("SetDefaultPolicyVersion"), nil
}

// iamParseVersionTarget reads the PolicyArn and VersionId DeletePolicyVersion and
// SetDefaultPolicyVersion both take, answering the refusal for a missing or malformed one. Both
// are shape checks, made before authorization as the attach operations make theirs.
func iamParseVersionTarget(req *AWSRequest) (policyArn, versionID string, errResp *AWSResponse) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
		VersionId string `json:"VersionId"` //nolint:revive,staticcheck // matches the API member name.
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return "", "", iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest)
	}
	if params.PolicyArn == "" || params.VersionId == "" {
		return "", "", iamErrorResponse("ValidationError",
			"PolicyArn and VersionId are required", http.StatusBadRequest)
	}
	if message, ok := iamValidatePolicyARN(params.PolicyArn); !ok {
		return "", "", iamErrorResponse("InvalidInput", message, http.StatusBadRequest)
	}
	if !iamPolicyVersionIDPattern.MatchString(params.VersionId) {
		return "", "", iamVersionIDInvalid()
	}
	return params.PolicyArn, params.VersionId, nil
}

// iamRefuseAWSManagedWrite refuses a version write against an AWS managed policy with
// InvalidInput, the code and the reading [IAMPlugin.deletePolicy] gives for deleting one: "You
// cannot change the permissions defined in AWS managed policies", and InvalidInput is the one
// published code on each of the three operations that describes a rejected input value. The
// message is substrate's own.
func iamRefuseAWSManagedWrite(arn string) *AWSResponse {
	if !iamPolicyARNIsAWSManaged(arn) {
		return nil
	}
	return iamErrorResponse("InvalidInput",
		fmt.Sprintf("Policy %s is an AWS managed policy and its versions cannot be changed. "+
			"Only the policies in your own account can be changed.", arn),
		http.StatusBadRequest)
}

// loadCustomerPolicy reads a customer managed policy from state, answering NoSuchEntity when
// there is none.
func (p *IAMPlugin) loadCustomerPolicy(goCtx context.Context, arn string) (*IAMPolicy, *AWSResponse, error) {
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

// putCustomerPolicy writes a customer managed policy back to state.
func (p *IAMPlugin) putCustomerPolicy(goCtx context.Context, policy *IAMPolicy) error {
	raw, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}
	if err := p.state.Put(goCtx, iamNamespace, iamPolicyKey(policy.ARN), raw); err != nil {
		return fmt.Errorf("put policy: %w", err)
	}
	return nil
}

// iamApplyDefaultVersion makes record the policy's default, mirroring its document onto the
// policy so the evaluator reads it.
func iamApplyDefaultVersion(policy *IAMPolicy, record IAMPolicyVersionRecord) {
	policy.DefaultVersionID = record.VersionID
	policy.Document = record.Document
	policy.DocumentText = record.DocumentText
}

// iamPolicyVersionRecords returns the policy's versions in creation order, synthesizing the one
// version of a policy that has never had a second from its Document and DocumentText. The slice
// is the caller's to change.
func iamPolicyVersionRecords(policy *IAMPolicy) []IAMPolicyVersionRecord {
	if len(policy.Versions) > 0 {
		return slices.Clone(policy.Versions)
	}
	return []IAMPolicyVersionRecord{{
		VersionID:    iamPolicyDefaultVersionID(policy),
		Document:     policy.Document,
		DocumentText: policy.DocumentText,
		CreateDate:   policy.CreateDate,
	}}
}

// iamPolicyDefaultVersionID returns the policy's default version ID. An empty DefaultVersionID
// falls back to "v1", which is what CreatePolicy assigns, so a policy written by an older release
// still reports a version ID rather than an empty element.
func iamPolicyDefaultVersionID(policy *IAMPolicy) string {
	if policy.DefaultVersionID == "" {
		return "v1"
	}
	return policy.DefaultVersionID
}

// iamPolicyLastVersionNumber returns the highest version number the policy has had: the recorded
// one, or for a record written before #1560 the largest number among its versions.
func iamPolicyLastVersionNumber(policy *IAMPolicy) int {
	highest := policy.LastVersionNumber
	for _, v := range iamPolicyVersionRecords(policy) {
		if n := iamVersionNumber(v.VersionID); n > highest {
			highest = n
		}
	}
	return highest
}

// iamVersionNumber reads the number out of a version ID such as "v3" or "v3.beta", or 0.
func iamVersionNumber(versionID string) int {
	digits := strings.TrimPrefix(versionID, "v")
	if i := strings.IndexByte(digits, '.'); i >= 0 {
		digits = digits[:i]
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return 0
	}
	return n
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
	return p.loadCustomerPolicy(goCtx, arn)
}

// iamPolicyVersion is one row of a policy's version list.
type iamPolicyVersion struct {
	// versionID is the version's own ID.
	versionID string

	// isDefault reports whether this is the policy's default version.
	isDefault bool

	// createDate is when the version was created.
	createDate time.Time

	// text and doc are the version's document as submitted and parsed; text may be empty, for
	// a bundled policy or a record written before #1452.
	text string
	doc  PolicyDocument
}

// iamPolicyVersions returns the policy's versions in creation order.
func iamPolicyVersions(policy *IAMPolicy) []iamPolicyVersion {
	if policy == nil {
		return nil
	}
	defaultID := iamPolicyDefaultVersionID(policy)
	records := iamPolicyVersionRecords(policy)
	out := make([]iamPolicyVersion, 0, len(records))
	for _, r := range records {
		out = append(out, iamPolicyVersion{
			versionID: r.VersionID, isDefault: r.VersionID == defaultID,
			createDate: r.CreateDate, text: r.DocumentText, doc: r.Document,
		})
	}
	return out
}

// iamPolicyVersionFor returns the named version of policy, reporting whether it exists.
//
// A bundled AWS managed policy holds only its current default, so a request for any of its other
// published versions is NoSuchEntity, which is the honest answer: claiming v1 exists when the
// policy reports v3 as its default would hand back v3's document under v1's name. The bundled
// catalog makes this immediately reachable — AmazonSSMManagedInstanceCore reports v2 and
// AWSLambdaVPCAccessExecutionRole reports v3 — rather than hypothetical.
func iamPolicyVersionFor(policy *IAMPolicy, versionID string) (iamPolicyVersion, bool) {
	for _, v := range iamPolicyVersions(policy) {
		if v.versionID == versionID {
			return v, true
		}
	}
	return iamPolicyVersion{}, false
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
	b.WriteString(version.createDate.UTC().Format("2006-01-02T15:04:05Z"))
	b.WriteString("</CreateDate>")

	if includeDocument {
		// The text the version was created with, not the parsed form re-marshaled (#1452).
		text, err := iamPolicyText(version.text, version.doc)
		if err != nil {
			return "", err
		}
		b.WriteString("<Document>")
		b.WriteString(iamRenderPolicyDocument(text))
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
