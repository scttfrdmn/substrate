package emulator

import (
	"context"
	"encoding/json"
	"strings"
)

// AWS Config resource tagging: TagResource, UntagResource and ListTagsForResource
// (#580).
//
// Tags matter here for a reason beyond bookkeeping: an IAM policy conditioned on
// aws:ResourceTag is how a consumer expresses "only the compliance team may delete a
// Config rule", and that condition cannot be evaluated unless the tags on the named
// resource are readable at authorization time. The authz hooks at the bottom of this
// file are what make such a policy enforceable; without them a policy scoped to one
// rule would apply to every one.
//
// The three operations validate through the same cfgsvcCheckTag the Puts' creation-time
// TagsList does, so a tag AWS would refuse cannot enter through one door having been
// refused at the other.
//
// **Which resources are taggable is documented, and a delivery channel is not among
// them.** TagResource's own ResourceArn documentation enumerates the supported types,
// and the Service Authorization Reference's resource table — the authoritative,
// machine-readable copy at
// servicereference.us-east-1.amazonaws.com/v1/config/config.json — defines ten
// resource types, none of them a delivery channel. So a channel cannot be tagged, has
// no ARN member in its own shape to be named by, and is not policy-addressable. Of the
// taggable types, substrate models three: the configuration recorder, the Config rule
// and the conformance pack. An ARN naming one of the other six (an aggregator, an
// aggregation authorization, an organization rule or pack, a remediation configuration,
// or a stored query) is answered ResourceNotFoundException, which is also what AWS
// answers for a type that exists but has no such instance — the same observation a
// caller gets either way.
//
// One further note on the documentation: the developer guide's "Tagging AWS Config
// resources" page claims only three types support the aws:ResourceTag condition key.
// The Service Authorization Reference lists it on nine of the ten, and the SAR is the
// authoritative source for authorization behavior, so that is what is implemented here.

// cfgsvcMaxTagKeys is the TagKeyList bound on UntagResource, and the TagList bound on
// TagResource: both are min 1, max 50 in the API model. The per-resource ceiling is the
// same number (cfgsvcMaxTags) but a different rule — see untagResource for why the two
// are answered with different exceptions.
const cfgsvcMaxTagKeys = 50

// cfgsvcMaxTagsLimit is the ceiling ListTagsForResource's Limit accepts.
//
// **The API reference documents this two ways.** The Limit member's own prose says
// "The limit maximum is 50. You cannot specify a number greater than 50", while the
// shape it points at is {"type":"integer","max":100,"min":0} — and the contradiction is
// present in both the rendered page and the vendored model, so it is AWS's, not a
// stale copy. Which bound the real service enforces is not documented.
//
// Substrate takes 100, the model's. An SDK generated from that model will not
// client-side-reject a Limit of 60, so a request carrying one reaches the service; if
// substrate refused it, substrate would be refusing a request the SDK was built to
// send. Taking the wider bound means substrate never refuses what the model permits,
// and the narrower prose is recorded as provenance rather than enforced.
const cfgsvcMaxTagsLimit = 100

// cfgsvcTagResourceRequest is TagResourceRequest; both members are required.
type cfgsvcTagResourceRequest struct {
	// ResourceArn names the resource to tag.
	ResourceArn string `json:"ResourceArn"`

	// Tags are the tags to apply.
	Tags []ConfigTag `json:"Tags"`
}

// cfgsvcUntagResourceRequest is UntagResourceRequest; both members are required.
type cfgsvcUntagResourceRequest struct {
	// ResourceArn names the resource to untag.
	ResourceArn string `json:"ResourceArn"`

	// TagKeys are the keys of the tags to remove.
	TagKeys []string `json:"TagKeys"`
}

// cfgsvcListTagsRequest is ListTagsForResourceRequest; only ResourceArn is required.
type cfgsvcListTagsRequest struct {
	// ResourceArn names the resource whose tags to list.
	ResourceArn string `json:"ResourceArn"`

	// Limit is the page size, 0-100 — see cfgsvcMaxTagsLimit for the documented
	// contradiction in that bound.
	Limit int `json:"Limit"`

	// NextToken continues a previous page.
	NextToken string `json:"NextToken"`
}

// --- errors ---

// cfgsvcResourceNotFound is ResourceNotFoundException, carrying the model's own message
// verbatim: "You have specified a resource that does not exist".
func cfgsvcResourceNotFound() *AWSError {
	return cfgsvcErr("ResourceNotFoundException", "You have specified a resource that does not exist.")
}

// cfgsvcTooManyTags is TooManyTagsException, declared by TagResource alone of the three.
//
// The model's message points at the Service Limits page rather than naming a number,
// so the number is added here: a caller reading only the message otherwise cannot tell
// what it exceeded.
func cfgsvcTooManyTags() *AWSError {
	return cfgsvcErr("TooManyTagsException",
		"You have reached the limit of the number of tags you can use. You can use up to "+
			"50 tags per resource.")
}

// tagOperation claims the three tagging operations.
func (p *ConfigServicePlugin) tagOperation(op string) (cfgsvcHandler, bool) {
	switch op {
	case "TagResource":
		return p.tagResource, true
	case "UntagResource":
		return p.untagResource, true
	case "ListTagsForResource":
		return p.listTagsForResource, true
	}
	return nil, false
}

// tagResource adds or updates tags on a Config resource.
//
// It merges rather than replaces, per the operation's own prose: "If existing tags on a
// resource are not specified in the request parameters, they are not changed. If
// existing tags are specified, however, then their values will be updated." So a same-key
// tag overwrites and every other tag survives — a replace-everything implementation
// would silently drop tags a caller never mentioned, which is the failure a consumer
// would only notice much later.
//
// The two size rules are answered with different exceptions on purpose. A Tags list
// longer than 50 violates TagList's own model bound, which ValidationException covers
// ("missing required fields or if the input value fails the validation"). A merge that
// would leave the *resource* holding more than 50 violates the service limit, which is
// what TooManyTagsException is for and is the only one of the two a caller can hit with
// a well-formed request. Both are declared by this operation.
func (p *ConfigServicePlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcTagResourceRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if len(in.Tags) == 0 {
		return nil, cfgsvcValidation("Tags is required and must contain at least one tag.")
	}
	if len(in.Tags) > cfgsvcMaxTagKeys {
		return nil, cfgsvcValidation("Tags accepts up to 50 tags.")
	}
	added, err := cfgsvcTagsToMap(in.Tags)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	arn, err := p.cfgsvcResolveTaggable(goCtx, ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags, err := p.cfgsvcLoadTags(goCtx, arn)
	if err != nil {
		return nil, err
	}
	for key, value := range added {
		tags[key] = value
	}
	if len(tags) > cfgsvcMaxTags {
		return nil, cfgsvcTooManyTags()
	}
	if err := p.cfgsvcSaveTags(goCtx, arn, tags); err != nil {
		return nil, err
	}
	p.logger.Debug("config: tagged resource",
		"arn", arn, "added", len(added), "total", len(tags))
	// TagResource has no output shape.
	return cfgsvcEmptyResponse(), nil
}

// untagResource removes tags from a Config resource by key.
//
// **Removing a key the resource does not carry is a no-op at 200.** Neither the
// operation's prose nor its error list says what happens, and the two candidate
// exceptions it declares are for a malformed request and a missing resource — neither
// describes an absent key. The no-op is the reading that keeps a teardown idempotent:
// a fixture that untags before deleting, and runs twice, must not fail the second time
// on a tag the first run already removed. Refusing would make substrate stricter than
// anything documented, and a wrong refusal breaks working code.
//
// Note also that UntagResource does *not* declare TooManyTagsException — only
// TagResource does — so a TagKeys list over the model's 50 is ValidationException here,
// which is one of the two exceptions this operation declares.
func (p *ConfigServicePlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcUntagResourceRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if len(in.TagKeys) == 0 {
		return nil, cfgsvcValidation("TagKeys is required and must contain at least one key.")
	}
	if len(in.TagKeys) > cfgsvcMaxTagKeys {
		return nil, cfgsvcValidation("TagKeys accepts up to 50 tag keys.")
	}
	for _, key := range in.TagKeys {
		// Only the key bounds apply: TagKey is 1-128 and there is no value to check.
		// cfgsvcCheckTag is reused with an empty value, which it permits, so an untag
		// and a tag agree on what a valid key is.
		if err := cfgsvcCheckTag(key, ""); err != nil {
			return nil, err
		}
	}

	goCtx := context.Background()
	arn, err := p.cfgsvcResolveTaggable(goCtx, ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags, err := p.cfgsvcLoadTags(goCtx, arn)
	if err != nil {
		return nil, err
	}
	for _, key := range in.TagKeys {
		delete(tags, key)
	}
	if err := p.cfgsvcSaveTags(goCtx, arn, tags); err != nil {
		return nil, err
	}
	p.logger.Debug("config: untagged resource",
		"arn", arn, "removed", len(in.TagKeys), "remaining", len(tags))
	// UntagResource has no output shape.
	return cfgsvcEmptyResponse(), nil
}

// listTagsForResource reports a resource's tags, paginated.
//
// This is the only one of the three with an output shape. Its Tags member points at
// TagList, whose model bound is min 1 — which cannot hold for an untagged resource, so
// an empty list is emitted rather than an omitted member: a consumer taking len() of
// the result gets 0 instead of a nil-pointer path, and output bounds are not something
// an SDK enforces on the way in.
//
// Pagination walks the keys in sorted order so a page boundary falls in the same place
// on every replay of the same event stream. Both InvalidLimitException and
// InvalidNextTokenException are declared by this operation — unusually, since most
// paginated Config operations declare one or the other — so each complaint is answered
// with the code that describes it.
func (p *ConfigServicePlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (
	*AWSResponse, error) {
	var in cfgsvcListTagsRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if in.Limit < 0 || in.Limit > cfgsvcMaxTagsLimit {
		return nil, cfgsvcInvalidLimit()
	}

	goCtx := context.Background()
	arn, err := p.cfgsvcResolveTaggable(goCtx, ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags, err := p.cfgsvcLoadTags(goCtx, arn)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	page, next, err := cfgsvcPaginate(keys, in.NextToken, in.Limit, cfgsvcMaxTagsLimit)
	if err != nil {
		return nil, err
	}

	out := struct {
		Tags      []ConfigTag `json:"Tags"`
		NextToken string      `json:"NextToken,omitempty"`
	}{Tags: make([]ConfigTag, 0, len(page)), NextToken: next}
	for _, key := range page {
		out.Tags = append(out.Tags, ConfigTag{Key: key, Value: tags[key]})
	}
	return cfgsvcJSONResponse(out, req.Operation)
}

// --- resolving an ARN to a resource that exists ---

// cfgsvcLoadTags reads a resource's tags, returning an empty map rather than nil so a
// caller can add to it without a nil check.
func (p *ConfigServicePlugin) cfgsvcLoadTags(goCtx context.Context, arn string) (
	map[string]string, error) {
	var tags map[string]string
	if _, err := p.cfgsvcGetJSON(goCtx, cfgsvcTagsKey(arn), &tags); err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	return tags, nil
}

// cfgsvcResolveTaggable checks that an ARN names a Config resource that exists in the
// caller's own account and Region, returning the ARN the tags are keyed by.
//
// The returned ARN is the resource's *stored* ARN rather than the one the request sent.
// For a recorder and a pack those are the same string, but reading it back from the
// record is what keeps the tag key aligned with the key the Put wrote — the alternative
// is two spellings of one resource's tags, one of which no operation would ever find.
//
// An ARN from another account or Region is ResourceNotFoundException, not a validation
// complaint: Config is regional and per-account, so from the caller's position the
// resource genuinely does not exist. That is also the answer for a well-formed ARN
// naming one of the six taggable types substrate does not model.
func (p *ConfigServicePlugin) cfgsvcResolveTaggable(goCtx context.Context, ctx *RequestContext,
	arn string) (string, error) {
	if arn == "" {
		return "", cfgsvcValidation("ResourceArn is required.")
	}
	if len([]rune(arn)) > 1000 {
		// AmazonResourceName is 1-1000 in the model.
		return "", cfgsvcValidation("ResourceArn may be up to 1000 characters long.")
	}

	prefix := "arn:aws:config:" + ctx.Region + ":" + ctx.AccountID + ":"
	if !strings.HasPrefix(arn, prefix) {
		return "", cfgsvcResourceNotFound()
	}
	resource := strings.TrimPrefix(arn, prefix)
	kind, rest, ok := strings.Cut(resource, "/")
	if !ok || rest == "" {
		return "", cfgsvcResourceNotFound()
	}

	switch kind {
	case "configuration-recorder":
		return p.cfgsvcResolveRecorderARN(goCtx, ctx, arn)
	case "config-rule":
		return p.cfgsvcResolveRuleARN(goCtx, ctx, rest)
	case "conformance-pack":
		return p.cfgsvcResolvePackARN(goCtx, ctx, arn, rest)
	default:
		return "", cfgsvcResourceNotFound()
	}
}

// cfgsvcResolveRecorderARN matches an ARN against the account's recorder.
//
// The whole ARN is compared rather than just the name, because the template carries a
// name *and* an ID (configuration-recorder/${RecorderName}/${RecorderId}) and the ID is
// derived from the name — so an ARN pairing a real name with a wrong ID names no
// resource and must not tag the real one.
func (p *ConfigServicePlugin) cfgsvcResolveRecorderARN(goCtx context.Context, ctx *RequestContext,
	arn string) (string, error) {
	var recorder ConfigRecorder
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder)
	if err != nil {
		return "", err
	}
	if !found || recorder.ARN != arn {
		return "", cfgsvcResourceNotFound()
	}
	return recorder.ARN, nil
}

// cfgsvcResolveRuleARN matches a config-rule/${ConfigRuleId} ARN against the account's
// rules.
//
// A rule's ARN names it by ID and not by name, so the index is walked to find the rule
// holding that ID. The alternative — deriving the name back from the ID — is impossible
// by construction: the ID is a hash.
func (p *ConfigServicePlugin) cfgsvcResolveRuleARN(goCtx context.Context, ctx *RequestContext,
	ruleID string) (string, error) {
	names, err := p.cfgsvcRuleNames(goCtx, ctx)
	if err != nil {
		return "", err
	}
	for _, name := range names {
		var rule ConfigRule
		found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleKey(ctx.AccountID, ctx.Region, name), &rule)
		if err != nil {
			return "", err
		}
		if found && rule.ConfigRuleId == ruleID {
			return rule.ConfigRuleArn, nil
		}
	}
	return "", cfgsvcResourceNotFound()
}

// cfgsvcResolvePackARN matches a conformance-pack/${Name}/${Id} ARN against the
// account's packs, comparing the whole ARN for the reason the recorder's does.
func (p *ConfigServicePlugin) cfgsvcResolvePackARN(goCtx context.Context, ctx *RequestContext,
	arn, rest string) (string, error) {
	name, _, ok := strings.Cut(rest, "/")
	if !ok || name == "" {
		return "", cfgsvcResourceNotFound()
	}
	var pack ConfigConformancePack
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcPackKey(ctx.AccountID, ctx.Region, name), &pack)
	if err != nil {
		return "", err
	}
	if !found || pack.ConformancePackArn != arn {
		return "", cfgsvcResourceNotFound()
	}
	return pack.ConformancePackArn, nil
}

// --- authorization hooks ---
//
// These are what buildResourceARN and the two tag-injection arms in authz.go call, and
// they exist for the same reason the Organizations ones do: without a service-specific
// ARN, every Config request authorizes against "*", so a policy scoped to one rule
// applies to all of them and a test asserting a denial passes for the wrong reason.
//
// The resource is resolved from the operation rather than by scanning the body for any
// name-shaped member. **One resource, not several**: an operation naming zero resources,
// or a list of them, resolves to "*" rather than to one arbitrary member of the list.
// Merging two named resources' tags into one condition context would let a caller
// authorized for one resource act on the other — a false allow, which is the failure
// direction that matters.

// cfgsvcAuthzResourceARN builds the IAM resource ARN a Config request authorizes
// against.
//
// Identifiers are minted rather than read from state, so an operation naming a resource
// that does not exist still authorizes against the ARN it would have: authorization
// runs before the handler, and a policy denying DeleteConfigRule on one rule should
// deny it whether or not the rule is there. The mint is the same deterministic function
// the handlers use, so the two agree.
//
// The delivery-channel operations resolve to "*" because **there is no delivery-channel
// resource type**: the Service Authorization Reference gives PutDeliveryChannel,
// DescribeDeliveryChannels, DescribeDeliveryChannelStatus and DeleteDeliveryChannel an
// empty resource list, which is how it spells "this action authorizes against * only".
// Synthesizing an ARN for them would invent a resource a real policy could never name.
func cfgsvcAuthzResourceARN(reqCtx *RequestContext, req *AWSRequest) string {
	switch req.Operation {
	case "TagResource", "UntagResource", "ListTagsForResource":
		// The request names the resource outright, so no minting is needed — and using
		// the caller's own string is right even when it names nothing, because that is
		// the resource the request asked to act on.
		if arn := cfgsvcAuthzBodyString(req, "ResourceArn"); arn != "" {
			return arn
		}
	case "PutConfigurationRecorder":
		if name := cfgsvcAuthzRecorderPutName(req); name != "" {
			return cfgsvcRecorderARN(reqCtx, name)
		}
	case "StartConfigurationRecorder", "StopConfigurationRecorder", "DeleteConfigurationRecorder":
		if name := cfgsvcAuthzBodyString(req, "ConfigurationRecorderName"); name != "" {
			return cfgsvcRecorderARN(reqCtx, name)
		}
	case "PutConfigRule":
		if name := cfgsvcAuthzRulePutName(req); name != "" {
			return cfgsvcRuleARN(reqCtx, cfgsvcMintRuleID(reqCtx.AccountID, reqCtx.Region, name))
		}
	case "DeleteConfigRule", "GetComplianceDetailsByConfigRule":
		if name := cfgsvcAuthzBodyString(req, "ConfigRuleName"); name != "" {
			return cfgsvcRuleARN(reqCtx, cfgsvcMintRuleID(reqCtx.AccountID, reqCtx.Region, name))
		}
	case "PutConformancePack", "DeleteConformancePack", "DescribeConformancePackCompliance":
		if name := cfgsvcAuthzBodyString(req, "ConformancePackName"); name != "" {
			return cfgsvcPackARN(reqCtx, name, cfgsvcMintPackID(reqCtx.AccountID, reqCtx.Region, name))
		}
	}
	// Everything else names no single resource: the describes take name *lists*, the
	// delivery-channel operations have no resource type, and PutEvaluations carries no
	// rule name at all (its ResultToken is the only thing that says which rule, and
	// decoding a token to authorize would give a caller control over the resource its
	// own request is checked against).
	return "*"
}

// cfgsvcAuthzResourceTags reads the tags on the resource a Config request names, for
// aws:ResourceTag conditions.
//
// A read failure returns no tags rather than an error, deliberately: an absent
// condition key makes a policy requiring it *not* match, so an unreadable resource
// denies. Returning a partial map would be the false-allow direction.
func cfgsvcAuthzResourceTags(state StateManager, reqCtx *RequestContext, req *AWSRequest) map[string]string {
	arn := cfgsvcAuthzResourceARN(reqCtx, req)
	if arn == "" || arn == "*" {
		return nil
	}
	raw, err := state.Get(context.Background(), configServiceNamespace, cfgsvcTagsKey(arn))
	if err != nil || raw == nil {
		return nil
	}
	var tags map[string]string
	if err := json.Unmarshal(raw, &tags); err != nil {
		return nil
	}
	return tags
}

// cfgsvcAuthzRequestTags reads the tags a Config request is trying to apply, for
// aws:RequestTag conditions.
//
// All four creating operations and TagResource carry the same [{Key,Value}] list, under
// "Tags" — so one reader serves them all, and a policy can gate a tagged
// PutConfigurationRecorder on the tag being applied rather than only on tags already
// stored.
func cfgsvcAuthzRequestTags(req *AWSRequest) map[string]string {
	var body struct {
		Tags []ConfigTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || len(body.Tags) == 0 {
		return nil
	}
	tags := make(map[string]string, len(body.Tags))
	for _, tag := range body.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// cfgsvcAuthzBodyString reads one top-level string member from a request body, quietly
// reporting "" for a body that will not parse: the handler answers that with the
// service's own validation exception, and authorization is not the place to decide a
// request is malformed.
func cfgsvcAuthzBodyString(req *AWSRequest, member string) string {
	var body map[string]interface{}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return ""
	}
	value, _ := body[member].(string)
	return value
}

// cfgsvcAuthzRecorderPutName reads the recorder name out of a PutConfigurationRecorder
// body, where it is nested under ConfigurationRecorder and spelled lowerCamel ("name")
// unlike every other member — the API model's asymmetry, not substrate's.
//
// An omitted name defaults to "default", the same default the handler applies, so the
// policy and the handler are talking about the same recorder.
func cfgsvcAuthzRecorderPutName(req *AWSRequest) string {
	var body struct {
		ConfigurationRecorder struct {
			Name string `json:"name"`
		} `json:"ConfigurationRecorder"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return ""
	}
	if body.ConfigurationRecorder.Name == "" {
		return cfgsvcDefaultName
	}
	return body.ConfigurationRecorder.Name
}

// cfgsvcAuthzRulePutName reads the rule name out of a PutConfigRule body, where it is
// nested under ConfigRule.
//
// An update may name its rule by Id or Arn instead of by Name. Those resolve to "*"
// rather than being converted, because converting would require reading state to find
// which rule an ID belongs to and then authorizing against a resource the request did
// not name — and if the lookup missed, the request would authorize against a
// *different* rule's ARN. "*" is the honest answer for a request whose resource cannot
// be named without a lookup that can be wrong.
func cfgsvcAuthzRulePutName(req *AWSRequest) string {
	var body struct {
		ConfigRule struct {
			ConfigRuleName string `json:"ConfigRuleName"`
		} `json:"ConfigRule"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return ""
	}
	return body.ConfigRule.ConfigRuleName
}
