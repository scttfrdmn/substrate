package emulator

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// S3 request tags for authorization, and the extra permissions an S3 create needs when it also
// applies tags or an ownership setting (#1434, #1445, #1463).

// s3AuthzRequestTags returns the tag set an S3 request applies, read with the same parsers the
// handlers use so the condition and the stored set cannot disagree: CreateBucket's
// CreateBucketConfiguration Tags, and the x-amz-tagging header for every other operation.
//
// ok is false when the request names a tag set that cannot be read as one: a header that is not
// query-encoded, a CreateBucket body that does not parse, or a key named twice. Each handler
// refuses such a request, but authorization runs first, so it must not evaluate a condition on
// aws:RequestTag against a set it could not read — [AuthController.CheckAccess] denies instead.
// Before this, the reader was url.ParseQuery, which disagreed with the handler: a ";" in a value
// made it read no tags at all while the handler stored them, and a duplicate key was read as its
// first value where the handler refuses it.
func s3AuthzRequestTags(req *AWSRequest) (map[string]string, bool) {
	tags := make(map[string]string)
	add := func(set []s3Tag) bool {
		for _, t := range set {
			if _, dup := tags[t.Key]; dup {
				return false
			}
			tags[t.Key] = t.Value
		}
		return true
	}

	if _, _, op := parseS3Operation(req); op == "CreateBucket" {
		cfg, errResp := parseCreateBucketConfiguration(req)
		if errResp != nil || !add(cfg.Tags.Tags) {
			return nil, false
		}
	}
	if tagging := headerValueFold(req.Headers, "x-amz-tagging"); tagging != "" {
		set, ok := parseS3TaggingHeaderValue(tagging)
		if !ok || !add(set) {
			return nil, false
		}
	}
	return tags, true
}

// s3AuthzUnreadableTagsError is the denial for a request whose tag set [s3AuthzRequestTags] could
// not read.
func s3AuthzUnreadableTagsError(reqCtx *RequestContext, action, deniedCode string) error {
	return &AWSError{
		Code: deniedCode,
		Message: fmt.Sprintf("User: %s is not authorized to perform: %s because the request's tag set "+
			"could not be read", reqCtx.Principal.ARN, action),
		HTTPStatus: http.StatusForbidden,
	}
}

// s3AuthzCreateSideEffectActions returns the additional actions an S3 request needs because of
// what it applies beyond its own operation, each from the operation's page:
//
//   - PutObject with a non-empty x-amz-tagging: s3:PutObjectTagging — "To successfully set the
//     tag-set with your PutObject request, you must have the s3:PutObjectTagging" (API_PutObject).
//   - CreateBucket with CreateBucketConfiguration Tags: s3:TagResource — "You must have the
//     s3:TagResource permission to create a general purpose bucket with tags"
//     (API_CreateBucketConfiguration).
//   - CreateBucket with x-amz-object-ownership: s3:PutBucketOwnershipControls — "If your
//     CreateBucket request includes the x-amz-object-ownership header, then the
//     s3:PutBucketOwnershipControls permission is required" (API_CreateBucket).
//
// CreateMultipartUpload and CopyObject also apply x-amz-tagging, but neither page, nor the
// multipart-upload permissions table in the User Guide, states an extra permission for it, so none
// is invented here.
func s3AuthzCreateSideEffectActions(req *AWSRequest, requestTags map[string]string) []string {
	_, _, op := parseS3Operation(req)
	var actions []string
	switch op {
	case "PutObject":
		if headerValueFold(req.Headers, "x-amz-tagging") != "" && len(requestTags) > 0 {
			actions = append(actions, "s3:PutObjectTagging")
		}
	case "CreateBucket":
		if cfg, errResp := parseCreateBucketConfiguration(req); errResp == nil && len(cfg.Tags.Tags) > 0 {
			actions = append(actions, "s3:TagResource")
		}
		if v, ok := headerLookupFold(req.Headers, "x-amz-object-ownership"); ok && v != "" {
			actions = append(actions, "s3:PutBucketOwnershipControls")
		}
	}
	return actions
}

// s3AuthzCopySourceARN returns the ARN of the object a CopyObject or UploadPartCopy request reads,
// and false for any other request or one whose x-amz-copy-source names no object.
//
// The header is the source bucket and key separated by a slash, URL-encoded, with an optional
// leading slash and an optional `?versionId=` suffix, or the URL-encoded ARN of an object accessed
// through an access point (API_CopyObject, API_UploadPartCopy). The version suffix is not part of the
// object's ARN and is dropped. An access-point ARN is already the resource a policy names, so it is
// used as it stands. The bucket and key are split by [parseS3CopySource], the handlers' own parser,
// so authorization and the copy cannot read different sources.
func s3AuthzCopySourceARN(req *AWSRequest) (string, bool) {
	if _, _, op := parseS3Operation(req); op != "CopyObject" && op != "UploadPartCopy" {
		return "", false
	}
	raw := headerValueFold(req.Headers, "x-amz-copy-source")
	if raw == "" {
		return "", false
	}
	if i := strings.Index(raw, "?versionId="); i >= 0 {
		raw = raw[:i]
	}
	if decoded, err := url.QueryUnescape(raw); err == nil && strings.HasPrefix(decoded, "arn:") {
		return decoded, true
	}
	bucket, key, errResp := parseS3CopySource(raw)
	if errResp != nil || bucket == "" || key == "" {
		return "", false
	}
	return "arn:aws:s3:::" + bucket + "/" + key, true
}

// checkS3CopySource runs the second authorization pass a copy needs: s3:GetObject on the source
// object, in addition to s3:PutObject on the destination the primary decision already allowed.
// Both API_CopyObject and API_UploadPartCopy state it — "If the source object is in a general
// purpose bucket, you must have s3:GetObject permission to read the source object that is being
// copied" — and so does the multipart permissions table for UploadPartCopy. Until #1489 no copy
// read its source's permission at all, so a caller allowed to write anywhere could copy an object
// it could not read.
//
// The pass follows [AuthController.checkS3CreateSideEffects]: it runs after the primary decision,
// against the identity policies and the boundary, with the request's time and principal keys. It
// reports no aws:ResourceTag/* for the source, matching the primary S3 pass, which reports a
// bucket's tags only.
func (a *AuthController) checkS3CopySource(reqCtx *RequestContext, req *AWSRequest,
	docs []PolicyDocument, boundary *PolicyDocument, deniedCode string, now time.Time, haveClock bool) error {
	if req.Service != "s3" {
		return nil
	}
	source, ok := s3AuthzCopySourceARN(req)
	if !ok {
		return nil
	}
	const action = "s3:GetObject"
	condCtx := make(map[string]string, 4)
	a.authzTimeContext(condCtx, now, haveClock)
	authzPrincipalContext(condCtx, reqCtx.Principal)
	evalReq := EvaluationRequest{
		Principal: reqCtx.Principal.ARN,
		Action:    action,
		Resource:  source,
		Context:   condCtx,
	}
	if Evaluate(docs, evalReq).Decision != DecisionAllow {
		return &AWSError{
			Code: deniedCode,
			Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
				reqCtx.Principal.ARN, action, source),
			HTTPStatus: http.StatusForbidden,
		}
	}
	if boundary != nil && Evaluate([]PolicyDocument{*boundary}, evalReq).Decision != DecisionAllow {
		return &AWSError{
			Code: deniedCode,
			Message: fmt.Sprintf("User: %s is not authorized to perform: %s (blocked by permission boundary)",
				reqCtx.Principal.ARN, action),
			HTTPStatus: http.StatusForbidden,
		}
	}
	return nil
}

// checkS3CreateSideEffects runs the additional authorization pass S3 documents for a create that
// also tags, or sets ownership controls, returning nil when the request needs none or the caller
// holds every one. It follows [AuthController.checkEC2TagOnCreate]: it runs after the primary
// decision, against the same resources, with the request's tags as aws:RequestTag/* and
// aws:TagKeys, and with no aws:ResourceTag/*, because the object or bucket does not exist yet.
func (a *AuthController) checkS3CreateSideEffects(reqCtx *RequestContext, req *AWSRequest,
	docs []PolicyDocument, boundary *PolicyDocument, resources []authzResource,
	requestTags map[string]string, deniedCode string, now time.Time, haveClock bool) error {
	if req.Service != "s3" {
		return nil
	}
	actions := s3AuthzCreateSideEffectActions(req, requestTags)
	if len(actions) == 0 {
		return nil
	}

	condCtx := make(map[string]string, len(requestTags)+4)
	for k, v := range requestTags {
		condCtx[k] = v
	}
	a.authzTimeContext(condCtx, now, haveClock)
	authzPrincipalContext(condCtx, reqCtx.Principal)
	tagKeysMulti := make(map[string][]string, 1)
	if keys := requestTagKeys(condCtx); len(keys) > 0 {
		tagKeysMulti["aws:TagKeys"] = keys
	}

	for _, action := range actions {
		for _, res := range resources {
			evalReq := EvaluationRequest{
				Principal:    reqCtx.Principal.ARN,
				Action:       action,
				Resource:     res.ARN,
				Context:      condCtx,
				MultiContext: tagKeysMulti,
			}
			if Evaluate(docs, evalReq).Decision != DecisionAllow {
				return &AWSError{
					Code: deniedCode,
					Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s",
						reqCtx.Principal.ARN, action, res.ARN),
					HTTPStatus: http.StatusForbidden,
				}
			}
			if boundary != nil && Evaluate([]PolicyDocument{*boundary}, evalReq).Decision != DecisionAllow {
				return &AWSError{
					Code: deniedCode,
					Message: fmt.Sprintf("User: %s is not authorized to perform: %s (blocked by permission boundary)",
						reqCtx.Principal.ARN, action),
					HTTPStatus: http.StatusForbidden,
				}
			}
		}
	}
	return nil
}
