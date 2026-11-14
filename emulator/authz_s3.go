package emulator

import (
	"fmt"
	"net/http"
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
