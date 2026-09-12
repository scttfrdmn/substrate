package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
)

// IAM resource tagging.
//
// This file holds the six operations that were missing entirely — `TagPolicy`, `UntagPolicy`,
// `ListPolicyTags`, `TagInstanceProfile`, `UntagInstanceProfile`, `ListInstanceProfileTags`,
// each of which answered `unknownActionError` before #796 — and the semantics all twelve IAM
// tagging operations share. The user and role handlers stay in `iam_plugin.go` next to the rest
// of the entity CRUD, and call the helpers here, so there is one implementation of the
// merge-by-key, the removal and the listing rather than four or six of them.
//
// What the operations do not do is validate. AWS documents `InvalidInput`/400,
// `LimitExceeded`/409 (the 50-tag cap) and `ConcurrentModification`/409 on the tag-writing
// operations, and none of the three is emitted here; a key is also compared
// case-sensitively where AWS treats user and role tag keys case-insensitively. That is filed
// as #806 rather than folded in, because each rejection is a behavior change for a consumer
// on today's permissive path and earns its own compatibility note.
//
// There is no group equivalent, and there will not be: the `Group` data type documents no
// `Tags` member, the Actions index publishes no `TagGroup`/`UntagGroup`/`ListGroupTags`, the
// vendored service-authorization snapshot has no `iam:*Group` tagging action, and the IAM User
// Guide says it directly — "You can tag most IAM resources, but not groups, assumed roles,
// access reports, or hardware-based MFA devices." A `TagGroup` request therefore keeps
// answering with the unknown-action error, which is the honest report that substrate models no
// such operation.

// iamMergeTagSet returns existing with incoming merged over it by key, sorted by key.
//
// A repeated key takes the incoming value, because AWS says so of every tag-writing
// operation: "If a tag with the same key name already exists, then that tag is overwritten
// with the new value." Sorting is not cosmetic — the listing operations document that the
// returned list is sorted by tag key, and their marker pagination is a position in that order.
func iamMergeTagSet(existing, incoming []IAMTag) []IAMTag {
	byKey := make(map[string]string, len(existing)+len(incoming))
	for _, t := range existing {
		byKey[t.Key] = t.Value
	}
	for _, t := range incoming {
		byKey[t.Key] = t.Value
	}
	merged := make([]IAMTag, 0, len(byKey))
	for k, v := range byKey {
		merged = append(merged, IAMTag{Key: k, Value: v})
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })
	return merged
}

// iamRemoveTagKeys returns tags without the named keys, keeping the order of the rest.
//
// A key that is not present is not an error: AWS's untag operations declare no error for it,
// and a consumer removing a tag it is unsure of should not have to read first.
func iamRemoveTagKeys(tags []IAMTag, keys []string) []IAMTag {
	if len(keys) == 0 {
		return tags
	}
	remove := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		remove[k] = struct{}{}
	}
	kept := make([]IAMTag, 0, len(tags))
	for _, t := range tags {
		if _, drop := remove[t.Key]; !drop {
			kept = append(kept, t)
		}
	}
	return kept
}

// iamTagListingResponse renders one page of a tags listing for operation.
//
// The shape is AWS's, and is the same on all six listings: `Tags` is a *required* member, so
// it is rendered even when the entity has none — "If no tags are attached to the specified
// resource, the response contains an empty list", which is the opposite of the entity shapes,
// where `Tags` is `Required: No` and an untagged entity omits it (#796). `MaxItems` defaults
// to 100, and `Marker` appears in the response only when `IsTruncated` is true.
//
// The page is taken from a key-sorted copy, per "The returned list of tags is sorted by tag
// key" — a marker names a key rather than an offset, so an unsorted underlying order would
// make a second page arbitrary.
func iamTagListingResponse(operation string, tags []IAMTag, marker string, maxItems iamInt) (*AWSResponse, error) {
	sorted := make([]IAMTag, len(tags))
	copy(sorted, tags)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	limit := maxItems.Int()
	if limit <= 0 {
		limit = 100
	}

	start := 0
	if marker != "" {
		for i, t := range sorted {
			if t.Key == marker {
				start = i
				break
			}
		}
	}
	end := start + limit
	isTruncated := false
	nextMarker := ""
	if end < len(sorted) {
		isTruncated = true
		nextMarker = sorted[end].Key
	} else {
		end = len(sorted)
	}

	body := iamTagListXML(sorted[start:end]) + "<IsTruncated>" + iamBoolXML(isTruncated) + "</IsTruncated>"
	if nextMarker != "" {
		body += "<Marker>" + xmlEsc(nextMarker) + "</Marker>"
	}
	return iamXMLResponse(http.StatusOK, operation, body)
}

// iamTagRecord is a loaded IAM record whose tags a tagging operation reads or rewrites.
//
// The entity types differ only in where their tags sit inside the record and which state key
// holds it; store closes over both, so the handlers below carry no per-type read-modify-write.
type iamTagRecord struct {
	// tags are the entity's current tags, in the order the record holds them.
	tags []IAMTag

	// store writes tags back onto the record this was loaded from.
	store func(ctx context.Context, tags []IAMTag) error
}

// loadTaggedPolicy returns the customer-managed policy at arn, or nil when there is none.
//
// A bundled AWS managed policy is deliberately not resolved here, so tagging one answers
// `NoSuchEntity` from the absent record. AWS's own operations are documented for the "IAM
// customer managed policy" only, and an AWS managed policy is not one: it belongs to the `aws`
// account, and substrate's catalog is read-only for the same reason.
func (p *IAMPlugin) loadTaggedPolicy(ctx context.Context, arn string) (*iamTagRecord, error) {
	key := iamPolicyKey(arn)
	raw, err := p.state.Get(ctx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	if raw == nil {
		return nil, nil //nolint:nilnil // an absent record is the caller's NoSuchEntity, not an error.
	}
	var policy IAMPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}
	return &iamTagRecord{tags: policy.Tags, store: func(ctx context.Context, tags []IAMTag) error {
		policy.Tags = tags
		out, marshalErr := json.Marshal(&policy)
		if marshalErr != nil {
			return fmt.Errorf("marshal policy: %w", marshalErr)
		}
		if putErr := p.state.Put(ctx, iamNamespace, key, out); putErr != nil {
			return fmt.Errorf("put policy: %w", putErr)
		}
		return nil
	}}, nil
}

// loadTaggedInstanceProfile returns the named instance profile, or nil when there is none.
func (p *IAMPlugin) loadTaggedInstanceProfile(ctx context.Context, accountID, name string) (*iamTagRecord, error) {
	key := iamInstanceProfileKey(accountID, name)
	raw, err := p.state.Get(ctx, iamNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("get instance profile: %w", err)
	}
	if raw == nil {
		return nil, nil //nolint:nilnil // an absent record is the caller's NoSuchEntity, not an error.
	}
	var profile IAMInstanceProfile
	if err := json.Unmarshal(raw, &profile); err != nil {
		return nil, fmt.Errorf("unmarshal instance profile: %w", err)
	}
	return &iamTagRecord{tags: profile.Tags, store: func(ctx context.Context, tags []IAMTag) error {
		profile.Tags = tags
		out, marshalErr := json.Marshal(&profile)
		if marshalErr != nil {
			return fmt.Errorf("marshal instance profile: %w", marshalErr)
		}
		if putErr := p.state.Put(ctx, iamNamespace, key, out); putErr != nil {
			return fmt.Errorf("put instance profile: %w", putErr)
		}
		return nil
	}}, nil
}

// iamPolicyNotFound is the NoSuchEntity a policy operation answers, worded as `GetPolicy`
// words it so a consumer matching on the message matches for either.
func iamPolicyNotFound(arn string) *AWSResponse {
	return iamErrorResponse("NoSuchEntity",
		fmt.Sprintf("Policy %s was not found.", arn), http.StatusNotFound)
}

// iamInstanceProfileNotFound is the NoSuchEntity an instance-profile operation answers,
// worded as `GetInstanceProfile` words it.
func iamInstanceProfileNotFound(name string) *AWSResponse {
	return iamErrorResponse("NoSuchEntity",
		fmt.Sprintf("Instance Profile %s cannot be found.", name), http.StatusNotFound)
}

// --- Policy tagging ---------------------------------------------------------

func (p *IAMPlugin) tagPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string   `json:"PolicyArn"`
		Tags      []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}
	// Tags is a query-protocol list, so it arrives as Tags.member.N.Key/Value; see tagUser
	// and iam_query.go (#639).
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:TagPolicy", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	record, err := p.loadTaggedPolicy(goCtx, params.PolicyArn)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return iamPolicyNotFound(params.PolicyArn), nil
	}
	if err := record.store(goCtx, iamMergeTagSet(record.tags, params.Tags)); err != nil {
		return nil, fmt.Errorf("tagPolicy: %w", err)
	}
	return iamXMLEmptyResponse("TagPolicy"), nil
}

func (p *IAMPlugin) untagPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string   `json:"PolicyArn"`
		TagKeys   []string `json:"TagKeys"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}
	// TagKeys is a flat query-protocol list; see untagUser and iam_query.go (#639).
	if keys := iamMemberList(req.Params, "TagKeys"); keys != nil {
		params.TagKeys = keys
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:UntagPolicy", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	record, err := p.loadTaggedPolicy(goCtx, params.PolicyArn)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return iamPolicyNotFound(params.PolicyArn), nil
	}
	if err := record.store(goCtx, iamRemoveTagKeys(record.tags, params.TagKeys)); err != nil {
		return nil, fmt.Errorf("untagPolicy: %w", err)
	}
	return iamXMLEmptyResponse("UntagPolicy"), nil
}

func (p *IAMPlugin) listPolicyTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		PolicyArn string `json:"PolicyArn"`
		Marker    string `json:"Marker"`
		MaxItems  iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.PolicyArn == "" {
		return iamErrorResponse("ValidationError", "PolicyArn is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListPolicyTags", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	record, err := p.loadTaggedPolicy(goCtx, params.PolicyArn)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return iamPolicyNotFound(params.PolicyArn), nil
	}
	return iamTagListingResponse("ListPolicyTags", record.tags, params.Marker, params.MaxItems)
}

// --- Instance-profile tagging -----------------------------------------------

func (p *IAMPlugin) tagInstanceProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string   `json:"InstanceProfileName"`
		Tags                []IAMTag `json:"Tags"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName is required", http.StatusBadRequest), nil
	}
	if tags := iamMemberTags(req.Params); tags != nil {
		params.Tags = tags
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:TagInstanceProfile", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	record, err := p.loadTaggedInstanceProfile(goCtx, ctx.AccountID, params.InstanceProfileName)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return iamInstanceProfileNotFound(params.InstanceProfileName), nil
	}
	if err := record.store(goCtx, iamMergeTagSet(record.tags, params.Tags)); err != nil {
		return nil, fmt.Errorf("tagInstanceProfile: %w", err)
	}
	return iamXMLEmptyResponse("TagInstanceProfile"), nil
}

func (p *IAMPlugin) untagInstanceProfile(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string   `json:"InstanceProfileName"`
		TagKeys             []string `json:"TagKeys"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName is required", http.StatusBadRequest), nil
	}
	if keys := iamMemberList(req.Params, "TagKeys"); keys != nil {
		params.TagKeys = keys
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:UntagInstanceProfile", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	record, err := p.loadTaggedInstanceProfile(goCtx, ctx.AccountID, params.InstanceProfileName)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return iamInstanceProfileNotFound(params.InstanceProfileName), nil
	}
	if err := record.store(goCtx, iamRemoveTagKeys(record.tags, params.TagKeys)); err != nil {
		return nil, fmt.Errorf("untagInstanceProfile: %w", err)
	}
	return iamXMLEmptyResponse("UntagInstanceProfile"), nil
}

func (p *IAMPlugin) listInstanceProfileTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var params struct {
		InstanceProfileName string `json:"InstanceProfileName"`
		Marker              string `json:"Marker"`
		MaxItems            iamInt `json:"MaxItems"`
	}
	if err := parseIAMBody(req.Body, &params); err != nil {
		return iamErrorResponse("ValidationError", err.Error(), http.StatusBadRequest), nil
	}
	if params.InstanceProfileName == "" {
		return iamErrorResponse("ValidationError", "InstanceProfileName is required", http.StatusBadRequest), nil
	}

	goCtx := context.Background()

	if err := p.authorize(goCtx, ctx, "iam:ListInstanceProfileTags", p.authzResource(ctx, req)); err != nil {
		return iamErrorResponse(iamAccessDeniedCode, err.Error(), http.StatusForbidden), nil
	}

	record, err := p.loadTaggedInstanceProfile(goCtx, ctx.AccountID, params.InstanceProfileName)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return iamInstanceProfileNotFound(params.InstanceProfileName), nil
	}
	return iamTagListingResponse("ListInstanceProfileTags", record.tags, params.Marker, params.MaxItems)
}
