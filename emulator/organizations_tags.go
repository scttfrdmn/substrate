package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Tag shape bounds, from the Organizations API model's TagKey and TagValue
// shapes. Lengths are in characters rather than bytes, which is what the model's
// min/max mean and what a caller tagging with non-ASCII depends on.
const (
	// orgMinTagKeyChars is TagKey's minimum length. An empty key is refused
	// rather than stored, because a tag with no key can never be matched by an
	// aws:ResourceTag condition and so would silently never gate anything.
	orgMinTagKeyChars = 1

	// orgMaxTagKeyChars is TagKey's maximum length.
	orgMaxTagKeyChars = 128

	// orgMaxTagValueChars is TagValue's maximum length. TagValue's minimum is 0:
	// the empty string is a legal value, and only a null one is refused.
	orgMaxTagValueChars = 256

	// orgSystemTagPrefix marks the tag keys AWS reserves for itself. The model's
	// INVALID_SYSTEM_TAGS_PARAMETER reason exists for a caller that tries to
	// write one: a caller cannot add, edit, or delete a system tag key, because
	// those keys are reserved for AWS's own use.
	orgSystemTagPrefix = "aws:"
)

// orgTagInput is one tag as the wire sends it. Key and Value are pointers so an
// absent or null member can be told apart from an empty string: TagValue's
// minimum length is 0, so "" is a legal value the caller may have meant, while
// null is not a value at all. Collapsing the two would store an empty-valued tag
// for a request AWS refuses.
type orgTagInput struct {
	Key   *string `json:"Key"`
	Value *string `json:"Value"`
}

// tagOperation claims the resource tagging operations. Tags reach the
// authorization decision through the organizations arms of buildResourceARN,
// addResourceTags and addRequestTags in authz.go — tagging that no policy can
// read is bookkeeping, not a privilege boundary.
func (p *OrganizationsPlugin) tagOperation(op string) (orgHandler, bool) {
	switch op {
	case "TagResource":
		return p.tagResource, true
	case "UntagResource":
		return p.untagResource, true
	case "ListTagsForResource":
		return p.listTagsForResource, true
	default:
		return nil, false
	}
}

// --- operations ---

// tagResource adds or overwrites tags on an account, root, OU, or policy.
//
// Every input is validated before anything is written, which is what TagResource
// documents: "If any one of the tags is not valid or if you exceed the maximum
// allowed number of tags for a resource, then the entire request fails." A
// partially applied batch would leave the caller unable to tell which tags
// landed, and a retry of the same request would then behave differently from the
// first attempt.
func (p *OrganizationsPlugin) tagResource(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ResourceID string        `json:"ResourceId"`
		Tags       []orgTagInput `json:"Tags"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := validateOrgTaggableResourceID(input.ResourceID); err != nil {
		return nil, err
	}
	// A nil slice is an absent required member; an empty one is a request that
	// asked for no tags, which the model permits (Tags carries no minimum length)
	// and which is a no-op.
	if input.Tags == nil {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify Tags")
	}
	tags, err := validateOrgTags(input.Tags)
	if err != nil {
		return nil, err
	}

	if err := p.requireWritableOrgResource(goCtx, reqCtx.AccountID, input.ResourceID); err != nil {
		return nil, err
	}

	existing, err := p.loadTags(goCtx, input.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("tagResource load tags: %w", err)
	}
	merged := mergeOrgTags(existing, tags)
	// The limit is on the resulting tag set, not on the request: re-tagging an
	// existing key overwrites it, so a resource already at the limit can still be
	// retagged. Counting the request instead would refuse a governance script that
	// only ever rewrites the tags it owns.
	if len(merged) > orgMaxTagsPerResource {
		return nil, orgConstraintViolation("MAX_TAG_LIMIT_EXCEEDED",
			fmt.Sprintf("you have exceeded the number of tags allowed on this resource (%d)", orgMaxTagsPerResource))
	}
	if err := p.saveTags(goCtx, input.ResourceID, merged); err != nil {
		return nil, fmt.Errorf("tagResource save tags: %w", err)
	}
	return orgEmptyResponse(), nil
}

// untagResource removes the named tag keys from a resource.
//
// A key that is not on the resource is not an error: UntagResource "removes any
// tags with the specified keys", so the operation is idempotent, and a cleanup
// path that runs twice must not fail the second time.
func (p *OrganizationsPlugin) untagResource(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ResourceID string   `json:"ResourceId"`
		TagKeys    []string `json:"TagKeys"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := validateOrgTaggableResourceID(input.ResourceID); err != nil {
		return nil, err
	}
	if input.TagKeys == nil {
		return nil, orgInvalidInput("INPUT_REQUIRED", "you must specify TagKeys")
	}
	for _, key := range input.TagKeys {
		if err := validateOrgTagKey(key); err != nil {
			return nil, err
		}
	}

	if err := p.requireWritableOrgResource(goCtx, reqCtx.AccountID, input.ResourceID); err != nil {
		return nil, err
	}

	existing, err := p.loadTags(goCtx, input.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("untagResource load tags: %w", err)
	}
	remove := make(map[string]bool, len(input.TagKeys))
	for _, key := range input.TagKeys {
		remove[key] = true
	}
	kept := make([]OrgTag, 0, len(existing))
	for _, t := range existing {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	// Nothing matched, so nothing is written: an untag of keys a resource never
	// had leaves no record behind, and a resource that was never tagged stays
	// untagged rather than acquiring an empty tag set.
	if len(kept) == len(existing) {
		return orgEmptyResponse(), nil
	}
	if err := p.saveTags(goCtx, input.ResourceID, kept); err != nil {
		return nil, fmt.Errorf("untagResource save tags: %w", err)
	}
	return orgEmptyResponse(), nil
}

// listTagsForResource returns a resource's tags, one page at a time.
//
// The model gives this operation a NextToken but no MaxResults, so the page size
// is the ceiling orgPaginate applies to every Organizations listing. That is
// below the 50-tag limit, so a heavily tagged resource really does page, and a
// consumer that reads only the first page is caught here rather than against AWS.
func (p *OrganizationsPlugin) listTagsForResource(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		ResourceID string `json:"ResourceId"`
		NextToken  string `json:"NextToken"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if err := validateOrgTaggableResourceID(input.ResourceID); err != nil {
		return nil, err
	}
	// Listing is a read, so it does not care whether the resource is writable —
	// an AWS-managed policy has no tags and answers with an empty list.
	if err := p.requireOrgResource(goCtx, reqCtx.AccountID, input.ResourceID); err != nil {
		return nil, err
	}

	tags, err := p.loadTags(goCtx, input.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("listTagsForResource load tags: %w", err)
	}
	byKey := make(map[string]string, len(tags))
	keys := make([]string, 0, len(tags))
	for _, t := range tags {
		byKey[t.Key] = t.Value
		keys = append(keys, t.Key)
	}
	// MaxResults is 0 because the operation has none; orgPaginate reads that as
	// "the maximum" and still validates the token.
	page, next, err := orgPaginate(keys, input.NextToken, 0)
	if err != nil {
		return nil, err
	}

	out := map[string]interface{}{"Tags": orgTagsFromKeys(page, byKey)}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listTagsForResource")
}

// --- resource resolution ---

// requireOrgResource refuses a resource the organization does not contain.
//
// TargetNotFoundException rather than a silent success: a tag written against an
// ID that resolves to nothing would be unreadable by every later call, and a
// caller that mistyped a resource ID would discover it only when the tag-gated
// policy it wrote failed to gate anything.
func (p *OrganizationsPlugin) requireOrgResource(ctx context.Context, acct, resourceID string) error {
	if _, err := p.ensureOrganization(ctx, acct); err != nil {
		return fmt.Errorf("organizations tagging ensure org: %w", err)
	}
	kind, err := p.resolveOrgTarget(ctx, acct, resourceID)
	if err != nil {
		return fmt.Errorf("organizations tagging resolve %s: %w", resourceID, err)
	}
	if kind == "" {
		return orgErr("TargetNotFoundException",
			"We can't find a root, OU, account, or policy with the ResourceId "+resourceID)
	}
	return nil
}

// requireWritableOrgResource additionally refuses a resource whose tags cannot
// be changed.
//
// p-FullAWSAccess is owned by AWS, not by the organization — its ARN names the
// "aws" account — so a tag written against it would be visible in one
// organization and nowhere else, a state no sequence of AWS calls can produce.
// IMMUTABLE_POLICY is the model's reason for "a policy that is managed by Amazon
// Web Services and can't be modified".
func (p *OrganizationsPlugin) requireWritableOrgResource(ctx context.Context, acct, resourceID string) error {
	if err := p.requireOrgResource(ctx, acct, resourceID); err != nil {
		return err
	}
	// p-FullAWSAccess is the only AWS-managed policy substrate synthesizes; every
	// other policy ID resolves to a record the organization owns.
	if resourceID == orgFullAWSAccessID {
		return orgInvalidInput("IMMUTABLE_POLICY",
			"you specified a policy that is managed by Amazon Web Services and can't be modified: "+resourceID)
	}
	return nil
}

// --- validation ---

// validateOrgTaggableResourceID refuses a ResourceId the model's
// TaggableResourceId pattern does not admit.
//
// A malformed ID and an absent resource are different answers to different
// mistakes: "ou-oops" is a typo the caller can fix from the error, while a
// well-formed ID that names nothing means the resource is gone. Answering
// TargetNotFoundException for both would send a caller looking for a deleted OU
// that never existed.
func validateOrgTaggableResourceID(id string) error {
	if id == "" {
		return orgInvalidInput("INPUT_REQUIRED", "you must specify ResourceId")
	}
	if !isOrgTaggableResourceID(id) {
		return orgInvalidInput("INVALID_PATTERN",
			"the ResourceId "+id+" does not match the required pattern for a root, account, OU, or policy")
	}
	return nil
}

// isOrgTaggableResourceID reports whether id has one of the six forms the
// model's TaggableResourceId pattern accepts. rp- names the organization's
// resource policy, which is taggable because PutResourcePolicy accepts Tags.
// rt- (responsibility transfer) is well-formed but names a resource substrate
// does not model, so it is admitted here and refused as absent by
// resolveOrgTarget, which is the same answer AWS gives for an ID in an
// organization that has no such resource.
func isOrgTaggableResourceID(id string) bool {
	switch {
	case strings.HasPrefix(id, "rp-"):
		return orgIDBodyMatches(id[3:], 4, 128, isOrgPolicyIDRune)
	case strings.HasPrefix(id, "rt-"):
		return orgIDBodyMatches(id[3:], 8, 32, isOrgPolicyIDRune)
	case strings.HasPrefix(id, "r-"):
		return orgIDBodyMatches(id[2:], 4, 32, isOrgLowerAlphanumRune)
	case strings.HasPrefix(id, "ou-"):
		root, child, ok := strings.Cut(id[3:], "-")
		return ok && orgIDBodyMatches(root, 4, 32, isOrgLowerAlphanumRune) &&
			orgIDBodyMatches(child, 8, 32, isOrgLowerAlphanumRune)
	case strings.HasPrefix(id, "p-"):
		return orgIDBodyMatches(id[2:], 8, 128, isOrgPolicyIDRune)
	default:
		return orgIDBodyMatches(id, 12, 12, func(r rune) bool { return r >= '0' && r <= '9' })
	}
}

// orgIDBodyMatches reports whether body has between minLen and maxLen characters
// and every one of them satisfies allowed.
func orgIDBodyMatches(body string, minLen, maxLen int, allowed func(rune) bool) bool {
	n := 0
	for _, r := range body {
		if !allowed(r) {
			return false
		}
		n++
	}
	return n >= minLen && n <= maxLen
}

// isOrgLowerAlphanumRune matches the [0-9a-z] class the root and OU ID patterns
// use.
func isOrgLowerAlphanumRune(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z')
}

// isOrgPolicyIDRune matches the [0-9a-zA-Z_] class the policy ID pattern uses;
// p-FullAWSAccess is why the class is mixed case.
func isOrgPolicyIDRune(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
}

// validateOrgTags checks a whole request's tags and returns them, refusing the
// first problem it finds.
//
// The duplicate-key check is over the request rather than over the stored tags:
// two entries for one key in one call leave the result dependent on which one
// wins, so DUPLICATE_TAG_KEY refuses the ambiguity instead of silently picking.
func validateOrgTags(inputs []orgTagInput) ([]OrgTag, error) {
	tags := make([]OrgTag, 0, len(inputs))
	seen := make(map[string]bool, len(inputs))
	for _, in := range inputs {
		if in.Key == nil {
			return nil, orgInvalidInput("INPUT_REQUIRED", "every tag must specify a Key")
		}
		if in.Value == nil {
			return nil, orgInvalidInput("INPUT_REQUIRED",
				"the tag "+*in.Key+" must specify a Value; the value may be an empty string but not null")
		}
		if err := validateOrgTagKey(*in.Key); err != nil {
			return nil, err
		}
		if err := validateOrgTagValue(*in.Key, *in.Value); err != nil {
			return nil, err
		}
		if seen[*in.Key] {
			return nil, orgInvalidInput("DUPLICATE_TAG_KEY",
				"tag keys must be unique among the tags attached to the same entity: "+*in.Key)
		}
		seen[*in.Key] = true
		tags = append(tags, OrgTag{Key: *in.Key, Value: *in.Value})
	}
	return tags, nil
}

// validateOrgCreateTags checks the inline Tags of a create operation —
// CreateOrganizationalUnit, CreatePolicy, CreateAccount — and returns them.
//
// It is the same validation TagResource applies, plus the count quota, because
// the tags are the same tags: a key TagResource refuses has to be refused here
// too, or a caller can plant a tag through a create that it could never set
// afterwards, and an "aws:"-prefixed one would then be readable as
// aws:ResourceTag by a policy condition. The count is checked against the request
// alone rather than a merge, since a resource being created carries no tags yet.
//
// Every refusal fires before the resource is written: each of these operations
// documents that an invalid tag fails the whole request, and a partially created
// resource would make the caller's retry collide with something it does not
// believe it created.
func validateOrgCreateTags(inputs []orgTagInput) ([]OrgTag, error) {
	if len(inputs) > orgMaxTagsPerResource {
		return nil, orgConstraintViolation("MAX_TAG_LIMIT_EXCEEDED",
			fmt.Sprintf("you have exceeded the number of tags allowed on this resource (%d)", orgMaxTagsPerResource))
	}
	return validateOrgTags(inputs)
}

// validateOrgTagKey enforces the TagKey shape and the reservation of the "aws:"
// prefix.
func validateOrgTagKey(key string) error {
	n := utf8.RuneCountInString(key)
	switch {
	case n < orgMinTagKeyChars:
		return orgInvalidInput("MIN_LENGTH_EXCEEDED",
			fmt.Sprintf("a tag key must be at least %d character long", orgMinTagKeyChars))
	case n > orgMaxTagKeyChars:
		return orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("the tag key %q is longer than the %d-character maximum", key, orgMaxTagKeyChars))
	case !isOrgTagText(key):
		return orgInvalidInput("INVALID_PATTERN",
			fmt.Sprintf("the tag key %q contains a character tag keys do not allow", key))
	case strings.HasPrefix(key, orgSystemTagPrefix):
		return orgInvalidInput("INVALID_SYSTEM_TAGS_PARAMETER",
			fmt.Sprintf("the tag key %q is a system tag; system tag keys are reserved for Amazon Web Services use", key))
	}
	return nil
}

// validateOrgTagValue enforces the TagValue shape. The empty string passes: the
// shape's minimum length is 0, and a caller that tags with an empty value is
// recording the key's presence.
func validateOrgTagValue(key, value string) error {
	if utf8.RuneCountInString(value) > orgMaxTagValueChars {
		return orgInvalidInput("MAX_LENGTH_EXCEEDED",
			fmt.Sprintf("the value of tag %q is longer than the %d-character maximum", key, orgMaxTagValueChars))
	}
	if !isOrgTagText(value) {
		return orgInvalidInput("INVALID_PATTERN",
			fmt.Sprintf("the value of tag %q contains a character tag values do not allow", key))
	}
	return nil
}

// isOrgTagText reports whether every character of s is in the class the model's
// TagKey and TagValue patterns share: ^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$.
func isOrgTagText(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.Is(unicode.Z, r) {
			continue
		}
		switch r {
		case '_', '.', ':', '/', '=', '+', '-', '@':
			continue
		}
		return false
	}
	return true
}

// --- tag set arithmetic ---

// mergeOrgTags applies added over existing, overwriting a key rather than
// duplicating it. A resource carrying one key twice would make an
// aws:ResourceTag condition depend on read order, which is the one thing a
// deterministic emulator must not allow.
func mergeOrgTags(existing, added []OrgTag) []OrgTag {
	merged := make([]OrgTag, 0, len(existing)+len(added))
	replaced := make(map[string]string, len(added))
	for _, t := range added {
		replaced[t.Key] = t.Value
	}
	for _, t := range existing {
		if v, ok := replaced[t.Key]; ok {
			merged = append(merged, OrgTag{Key: t.Key, Value: v})
			delete(replaced, t.Key)
			continue
		}
		merged = append(merged, t)
	}
	for _, t := range added {
		if _, ok := replaced[t.Key]; ok {
			merged = append(merged, t)
			delete(replaced, t.Key)
		}
	}
	return merged
}

// orgTagsFromKeys rebuilds the tags for one page of keys.
func orgTagsFromKeys(keys []string, byKey map[string]string) []OrgTag {
	tags := make([]OrgTag, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, OrgTag{Key: k, Value: byKey[k]})
	}
	return tags
}

// --- authorization support ---
//
// These three feed the organizations arms of buildResourceARN, addResourceTags
// and addRequestTags in authz.go. They read through the raw StateManager rather
// than through the plugin because authorization runs before dispatch, so no
// plugin instance is in hand — and they run on every request, so each reads at
// most the one resource the request names.

// orgAuthzResourceID returns the ID of the Organizations resource a request
// names, or "" when it names none.
//
// One resource, not several: AttachPolicy names both a policy and a target, and
// merging both tag sets into aws:ResourceTag would let a tag on the policy
// satisfy a condition written about the target — a false allow, which is the
// failure a tag-gated boundary exists to prevent. The order below prefers the
// member that names the operation's own subject.
func orgAuthzResourceID(req *AWSRequest) string {
	var body struct {
		ResourceID           string `json:"ResourceId"`
		PolicyID             string `json:"PolicyId"`
		TargetID             string `json:"TargetId"`
		AccountID            string `json:"AccountId"`
		OrganizationalUnitID string `json:"OrganizationalUnitId"`
		RootID               string `json:"RootId"`
		ParentID             string `json:"ParentId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return ""
	}
	for _, id := range []string{
		body.ResourceID, body.PolicyID, body.TargetID, body.AccountID,
		body.OrganizationalUnitID, body.RootID, body.ParentID,
	} {
		if id != "" {
			return id
		}
	}
	return ""
}

// orgAuthzResourceARN returns the ARN of the Organizations resource a request
// names, for the Resource element of a policy statement to be matched against.
//
// It returns the ARN the API itself reports for the resource, read from the
// stored record, rather than one reassembled here: the organization ID is a
// segment of every one of them, and a synthesized ARN that disagreed with what
// DescribeOrganizationalUnit returns would make a policy written from an
// observed ARN fail to match the resource it names.
//
// "*" is the answer when the request names no resource, or names one that is not
// there. It is deliberately not "" — resourceMatches treats the empty string as
// "matches every statement", so an unreadable record would widen a
// resource-scoped policy instead of narrowing it.
func orgAuthzResourceARN(state StateManager, reqCtx *RequestContext, req *AWSRequest) string {
	id := orgAuthzResourceID(req)
	if id == "" {
		return "*"
	}
	goCtx := context.Background()
	arn := ""
	switch {
	case isOrgRootID(id):
		var root OrgRoot
		if orgAuthzGetJSON(goCtx, state, orgRootKey(reqCtx.AccountID), &root) && root.ID == id {
			arn = root.Arn
		}
	case isOrgOUID(id):
		var ou OrgOrganizationalUnit
		if orgAuthzGetJSON(goCtx, state, orgOUKey(id), &ou) {
			arn = ou.Arn
		}
	case strings.HasPrefix(id, "rp-"):
		// Keyed by the management account rather than by the ID, like the root: the
		// organization holds exactly one resource policy. The stored ID must match,
		// so a well-formed rp- ID that is not the current policy falls through to "*"
		// rather than borrowing the real policy's ARN.
		var rp OrgResourcePolicy
		if orgAuthzGetJSON(goCtx, state, orgResourcePolicyKey(reqCtx.AccountID), &rp) &&
			rp.ResourcePolicySummary.ID == id {
			arn = rp.ResourcePolicySummary.Arn
		}
	case strings.HasPrefix(id, "p-"):
		if id == orgFullAWSAccessID {
			// Synthesized rather than stored, so there is no record to read.
			return orgFullAWSAccessArn
		}
		var pol OrgPolicy
		if orgAuthzGetJSON(goCtx, state, orgPolicyKey(id), &pol) {
			arn = pol.PolicySummary.Arn
		}
	default:
		var a OrgAccount
		if orgAuthzGetJSON(goCtx, state, orgAccountKey(id), &a) {
			arn = a.Arn
		}
	}
	if arn == "" {
		return "*"
	}
	return arn
}

// orgAuthzResourceTags returns the tags on the Organizations resource a request
// names, as an aws:ResourceTag-shaped map.
func orgAuthzResourceTags(state StateManager, req *AWSRequest) map[string]string {
	id := orgAuthzResourceID(req)
	if id == "" {
		return nil
	}
	var tags []OrgTag
	if !orgAuthzGetJSON(context.Background(), state, orgTagsKey(id), &tags) {
		return nil
	}
	return orgTagsToMap(tags)
}

// orgAuthzRequestTags returns the tags a request asks to set, as an
// aws:RequestTag-shaped map. Organizations sends them in the JSON body as
// "Tags": [{"Key":…,"Value":…}], the same shape IAM uses.
func orgAuthzRequestTags(req *AWSRequest) map[string]string {
	var body struct {
		Tags []OrgTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil
	}
	return orgTagsToMap(body.Tags)
}

// orgTagsToMap converts a tag list to the key-value map the condition context
// wants.
func orgTagsToMap(tags []OrgTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

// orgAuthzGetJSON decodes the value at key, reporting false when it is absent or
// cannot be read.
//
// Failing quietly is the established behavior of every other arm of
// addResourceTags, and it is the safe direction here: an unread tag leaves the
// condition key absent, so an Allow that requires it does not match and the
// caller is denied. The opposite — assuming a tag matched — would let a storage
// fault grant access.
func orgAuthzGetJSON(ctx context.Context, state StateManager, key string, out interface{}) bool {
	raw, err := state.Get(ctx, organizationsNamespace, key)
	if err != nil || raw == nil {
		return false
	}
	return json.Unmarshal(raw, out) == nil
}
