package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"
)

// ELB resource kinds, which are the ARN resource types the four taggable ELBv2
// resources use. They are the switch value everywhere a tagging call has to know
// which of the four it is looking at.
//
// The listener and rule types are the ones AWS's ARNs carry —
// `listener/app/<lb>/<lb-id>/<listener-id>` and
// `listener-rule/app/<lb>/<lb-id>/<listener-id>/<rule-id>` — which is *not* the shape
// [elbListenerARN] and [elbRuleARN] mint (they nest the suffix under the load
// balancer's own ARN). Authorization uses the AWS spelling because that is what a
// policy is written against; resolution reads substrate's, because that is what state
// holds. The malformed ARN shape is #774, filed rather than fixed here.
const (
	elbKindLoadBalancer = "loadbalancer"
	elbKindTargetGroup  = "targetgroup"
	elbKindListener     = "listener"
	elbKindRule         = "listener-rule"
)

// elbMaxTagsPerResource is the number of user tags ELB allows on one resource.
//
// From the ELB tagging documentation's restrictions: "Maximum number of tags per
// resource—50". Tags carrying [elbReservedTagPrefix] are excluded from the count, per
// the same list — "Tags with this prefix do not count against your tags per resource
// limit" — which is byte-for-byte the rule [ec2CheckTagLimit] already implements.
const elbMaxTagsPerResource = 50

// elbReservedTagPrefix is the tag-key prefix ELB reserves for AWS's own use.
//
// It is used **only** to exclude a reserved key from the count above. ELB's
// restrictions do say "You can't edit or delete tag names or values with this prefix",
// but neither the API reference's Errors sections nor any reachable page publishes a
// code for that refusal, and no real response has been observed — so substrate does not
// refuse the prefix. Inventing a code here is the #561 failure in a different costume: a
// consumer's error branch would dispatch on something AWS never sends. The gap is
// recorded in docs/services.md rather than papered over.
const elbReservedTagPrefix = "aws:"

// elbMaxTagKeyLength and elbMaxTagValueLength are the tag length limits.
//
// **AWS contradicts itself here and substrate follows the API model.** The Tag type's
// API reference gives "Length Constraints: Minimum length of 1. Maximum length of 128"
// for Key and "Maximum length of 256" for Value; the ELB user guide's restrictions list
// says "Maximum key length—127 Unicode characters" and "Maximum value length—255
// Unicode characters". The model is what an SDK validates against and what the service
// enforces, so it is the one implemented — a 128-character key is accepted here, and a
// caller who trusts the user guide's 127 is inside that.
//
// The unit is Unicode characters rather than bytes, so the checks count runes; see
// [ec2MaxTagKeyLength] for why the two are indistinguishable on ASCII.
const (
	elbMaxTagKeyLength   = 128
	elbMaxTagValueLength = 256
)

// elbDescribeTagsMaxResources is the number of resources one DescribeTags request may
// name, from the operation's "Array Members: Maximum number of 20 items" on
// ResourceArns. AddTags and RemoveTags carry no such cap.
const elbDescribeTagsMaxResources = 20

// elbMaxRemoveTagKeys is the number of keys one RemoveTags request may name, from that
// operation's "Array Members: Minimum number of 1 item. Maximum number of 128 items" on
// TagKeys. It is a per-request cap and unrelated to [elbMaxTagsPerResource]: a request may
// legally name more keys than any one resource holds, because a key that is not present is
// ignored.
const elbMaxRemoveTagKeys = 128

// elbTagCharPattern is the character set the Tag type's API reference permits in a key
// and in a value: `^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$`.
//
// The pattern admits the empty string, so it decides characters only — the minimum
// length of 1 on a key is enforced separately by [elbCheckTagRules]. The user guide
// spells the same set in prose ("letters, spaces, and numbers representable in UTF-8,
// plus the following special characters: + - = . _ : / @"), and the two agree except
// that the prose omits the comma the pattern also omits, so there is nothing to choose
// between here.
var elbTagCharPattern = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)

// elbTagValidationError returns the refusal for a tag that breaks one of the Tag type's
// documented constraints.
//
// Provenance: the *constraints* are the API model's, but the model publishes no error
// for violating one, and none of AddTags', RemoveTags' or the four creates' Errors
// sections names one. A real service answers the common `ValidationError` for a member
// outside its constraints, which is what this returns; the message text is substrate's
// own, written to name which constraint and which key, because that is the only place a
// caller learns it.
func elbTagValidationError(format string, args ...any) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    fmt.Sprintf(format, args...),
		HTTPStatus: http.StatusBadRequest,
	}
}

// elbDuplicateTagKeysError returns the error AddTags raises when one key appears twice.
//
// Both the code and the message are the API reference's own: "DuplicateTagKeys — A tag
// key was specified more than once. HTTP Status Code: 400". It is listed on **AddTags
// and on CreateLoadBalancer, and on neither CreateTargetGroup, CreateListener nor
// CreateRule** — so a duplicate key on those three creates is not refused here either;
// see [elbCheckCreateTags].
func elbDuplicateTagKeysError() *AWSError {
	return &AWSError{
		Code:       "DuplicateTagKeys",
		Message:    "A tag key was specified more than once.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// elbTooManyTagsError returns the error raised when a resource would exceed
// [elbMaxTagsPerResource].
//
// Code and message are the API reference's own — "TooManyTags — You've reached the limit
// on the number of tags for this resource. HTTP Status Code: 400" — and it is listed on
// AddTags and on all four creates, so every path that can apply a tag can raise it.
func elbTooManyTagsError() *AWSError {
	return &AWSError{
		Code:       "TooManyTags",
		Message:    "You've reached the limit on the number of tags for this resource.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// elbNotFoundCodes maps a resource kind to the code the tagging operations answer when
// an ARN of that kind names nothing.
//
// All four appear on AddTags, RemoveTags and DescribeTags alike, each at HTTP 400 — a
// *400*, not the 404 a reader expects, which is the ELB API's own choice and the reason
// this table exists rather than one shared NotFound. TrustStoreNotFound is the fifth
// code those operations list and has no row here because substrate models no trust
// store, so no ARN can ever name one.
var elbNotFoundCodes = map[string]string{
	elbKindLoadBalancer: "LoadBalancerNotFound",
	elbKindTargetGroup:  "TargetGroupNotFound",
	elbKindListener:     "ListenerNotFound",
	elbKindRule:         "RuleNotFound",
}

// elbNotFoundError returns the refusal for an ARN of a known kind that names nothing.
//
// The message names the ARN. AWS's published text for each of these is a one-line
// description of the code rather than a response body ("The specified load balancer does
// not exist."), so the wording is substrate's; the code, which is what an SDK dispatches
// on, is the model's.
func elbNotFoundError(kind, arn string) *AWSError {
	code, ok := elbNotFoundCodes[kind]
	if !ok {
		return elbTagValidationError("'%s' is not a valid Elastic Load Balancing resource ARN", arn)
	}
	return &AWSError{
		Code:       code,
		Message:    fmt.Sprintf("The specified resource does not exist: %s", arn),
		HTTPStatus: http.StatusBadRequest,
	}
}

// elbResourceKindFromARN reports which of the four taggable kinds an ARN names, or ""
// when it names none.
//
// The listener and rule tests come first and match on substrate's own nesting —
// `…:loadbalancer/app/<name>/<id>/listener/<suffix>` and that plus `/rule/<suffix>` —
// so they must be tried before the load-balancer test, which their prefix also
// satisfies. AWS's flat `…:listener/…` and `…:listener-rule/…` are recognized too, so a
// caller passing an ARN of the real shape is not told it names nothing; that is the
// forward-compatible half of #774.
func elbResourceKindFromARN(arn string) string {
	switch {
	case strings.Contains(arn, "/rule/") || strings.Contains(arn, ":listener-rule/"):
		return elbKindRule
	case strings.Contains(arn, "/listener/") || strings.Contains(arn, ":listener/"):
		return elbKindListener
	case strings.Contains(arn, ":loadbalancer/"):
		return elbKindLoadBalancer
	case strings.Contains(arn, ":targetgroup/"):
		return elbKindTargetGroup
	default:
		return ""
	}
}

// elbStateKeyPrefix returns the state-key prefix records of a kind are stored under.
func elbStateKeyPrefix(kind, scope string) string {
	switch kind {
	case elbKindLoadBalancer:
		return "lb:" + scope + "/"
	case elbKindTargetGroup:
		return "tg:" + scope + "/"
	case elbKindListener:
		return "listener:" + scope + "/"
	case elbKindRule:
		return "rule:" + scope + "/"
	default:
		return ""
	}
}

// elbTaggedResource is one ELB resource a tagging call names, resolved from state.
type elbTaggedResource struct {
	// arn is the ARN the request named, which is also the one DescribeTags reports.
	arn string

	// stateKey is the key the record lives under, so a write goes back where it came
	// from rather than to a key rebuilt from the ARN — the listener and rule keys are
	// suffix-based and not derivable from the ARN alone.
	stateKey string

	// tags are the tags currently on the resource.
	tags []ELBTag

	// withTags re-marshals the record carrying a new tag set. It closes over the
	// decoded record, which is what keeps every other member intact: rebuilding the
	// record from the ARN would drop whatever the caller set at create time.
	withTags func(tags []ELBTag) ([]byte, error)
}

// elbDecodeTaggedResource decodes one state record of the given kind, or returns nil
// when the bytes do not parse as that kind.
func elbDecodeTaggedResource(kind, stateKey string, data []byte) *elbTaggedResource {
	switch kind {
	case elbKindLoadBalancer:
		var lb ELBLoadBalancer
		if json.Unmarshal(data, &lb) != nil {
			return nil
		}
		return &elbTaggedResource{arn: lb.ARN, stateKey: stateKey, tags: lb.Tags,
			withTags: func(tags []ELBTag) ([]byte, error) {
				lb.Tags = tags
				return json.Marshal(lb)
			}}
	case elbKindTargetGroup:
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil {
			return nil
		}
		return &elbTaggedResource{arn: tg.ARN, stateKey: stateKey, tags: tg.Tags,
			withTags: func(tags []ELBTag) ([]byte, error) {
				tg.Tags = tags
				return json.Marshal(tg)
			}}
	case elbKindListener:
		var l ELBListener
		if json.Unmarshal(data, &l) != nil {
			return nil
		}
		return &elbTaggedResource{arn: l.ARN, stateKey: stateKey, tags: l.Tags,
			withTags: func(tags []ELBTag) ([]byte, error) {
				l.Tags = tags
				return json.Marshal(l)
			}}
	case elbKindRule:
		var r ELBRule
		if json.Unmarshal(data, &r) != nil {
			return nil
		}
		return &elbTaggedResource{arn: r.ARN, stateKey: stateKey, tags: r.Tags,
			withTags: func(tags []ELBTag) ([]byte, error) {
				r.Tags = tags
				return json.Marshal(r)
			}}
	default:
		return nil
	}
}

// elbResolveTaggedResource finds the record an ARN names, or the refusal for it.
//
// A read that genuinely fails is reported as an error rather than as a NotFound: a
// broken backend is not an absent resource, and answering LoadBalancerNotFound for one
// would tell a consumer's retry loop the wrong thing.
func elbResolveTaggedResource(state StateManager, scope, arn string) (*elbTaggedResource, *AWSError, error) {
	kind := elbResourceKindFromARN(arn)
	if kind == "" {
		return nil, elbTagValidationError("'%s' is not a valid Elastic Load Balancing resource ARN", arn), nil
	}
	keys, err := state.List(context.Background(), elbNamespace, elbStateKeyPrefix(kind, scope))
	if err != nil {
		return nil, nil, fmt.Errorf("elb resolve %s: %w", arn, err)
	}
	for _, k := range keys {
		data, getErr := state.Get(context.Background(), elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		res := elbDecodeTaggedResource(kind, k, data)
		if res == nil || res.arn != arn {
			continue
		}
		return res, nil, nil
	}
	return nil, elbNotFoundError(kind, arn), nil
}

// elbResolveAll resolves every ARN a tagging call names, refusing the whole request on
// the first one that names nothing.
//
// Resolving all of them before any write is what makes a partly-unknown AddTags apply
// nothing at all, the same pre-pass [EC2Plugin.terminateInstances] runs for the same
// reason: a request that is going to be refused must not have already changed half the
// resources it named. AWS documents no ordering for ELB's multi-resource tagging calls,
// so **this is substrate's reading**, chosen because the alternative is a partial write
// no caller can undo from the error alone.
func elbResolveAll(state StateManager, scope string, arns []string) ([]*elbTaggedResource, *AWSError, error) {
	out := make([]*elbTaggedResource, 0, len(arns))
	for _, arn := range arns {
		res, awsErr, err := elbResolveTaggedResource(state, scope, arn)
		if err != nil {
			return nil, nil, err
		}
		if awsErr != nil {
			return nil, awsErr, nil
		}
		out = append(out, res)
	}
	return out, nil, nil
}

// extractELBTags reads an indexed Tags.member.N list of {Key, Value} from query params.
//
// The walk ends on an absent or empty Key, which is how the query protocol terminates an
// indexed list and what [extractEC2Tags] does. A value is optional — the Tag type's
// Value has a minimum length of 0 — so an empty value is a legal tag, not a terminator.
func extractELBTags(params map[string]string, prefix string) []ELBTag {
	var tags []ELBTag
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("%s.%d.Key", prefix, i)]
		if key == "" {
			break
		}
		tags = append(tags, ELBTag{Key: key, Value: params[fmt.Sprintf("%s.%d.Value", prefix, i)]})
	}
	return tags
}

// elbCheckTagKey validates one tag key against the Tag type's constraints.
func elbCheckTagKey(key string) *AWSError {
	if n := utf8.RuneCountInString(key); n > elbMaxTagKeyLength {
		return elbTagValidationError(
			"Tag key must be no more than %d characters; the supplied key is %d",
			elbMaxTagKeyLength, n)
	}
	if !elbTagCharPattern.MatchString(key) {
		return elbTagValidationError("Tag key '%s' contains characters that are not permitted", key)
	}
	return nil
}

// elbCheckTagRules validates a tag list against every Tag constraint substrate models:
// key and value lengths, and the permitted character set.
//
// Tags are checked in slice order, so which tag a mixed request is refused on is decided
// identically on every run — the replay-stability reason [ec2CheckTagLengths] avoids a
// map. The key's minimum length of 1 is not checked because it is not expressible:
// [extractELBTags] ends its walk on an empty key.
func elbCheckTagRules(tags []ELBTag) *AWSError {
	for _, t := range tags {
		if awsErr := elbCheckTagKey(t.Key); awsErr != nil {
			return awsErr
		}
		if n := utf8.RuneCountInString(t.Value); n > elbMaxTagValueLength {
			return elbTagValidationError(
				"Tag value must be no more than %d characters; the supplied value is %d",
				elbMaxTagValueLength, n)
		}
		if !elbTagCharPattern.MatchString(t.Value) {
			return elbTagValidationError("Tag value '%s' contains characters that are not permitted", t.Value)
		}
	}
	return nil
}

// elbCheckDuplicateTagKeys returns [elbDuplicateTagKeysError] when one key appears more
// than once, or nil.
func elbCheckDuplicateTagKeys(tags []ELBTag) *AWSError {
	seen := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		if _, dup := seen[t.Key]; dup {
			return elbDuplicateTagKeysError()
		}
		seen[t.Key] = struct{}{}
	}
	return nil
}

// elbCheckTagLimit returns an error if merging incoming into existing would leave a
// resource with more than [elbMaxTagsPerResource] user tags, or nil.
//
// The count is over the post-merge key set with reserved keys excluded, which is the
// same expression [ec2CheckTagLimit] uses and gets the same two documented rules right:
// re-tagging an existing key on a resource already at the limit succeeds, and a reserved
// key neither counts nor consumes room.
func elbCheckTagLimit(existing, incoming []ELBTag) *AWSError {
	keys := make(map[string]struct{}, len(existing)+len(incoming))
	for _, t := range existing {
		if !strings.HasPrefix(t.Key, elbReservedTagPrefix) {
			keys[t.Key] = struct{}{}
		}
	}
	for _, t := range incoming {
		if !strings.HasPrefix(t.Key, elbReservedTagPrefix) {
			keys[t.Key] = struct{}{}
		}
	}
	if len(keys) > elbMaxTagsPerResource {
		return elbTooManyTagsError()
	}
	return nil
}

// elbCheckCreateTags validates the Tags.member.N a create carries.
//
// It is the create's half of the tagging rules and is deliberately narrower than
// AddTags': **no duplicate-key refusal**. DuplicateTagKeys is listed on AddTags and on
// CreateLoadBalancer, and on none of CreateTargetGroup, CreateListener or CreateRule, so
// refusing a duplicate on all four would invent a code three of them do not publish. A
// duplicate therefore resolves last-wins through [elbMergeTags], which is the only other
// thing it can do.
func elbCheckCreateTags(tags []ELBTag) *AWSError {
	if awsErr := elbCheckTagRules(tags); awsErr != nil {
		return awsErr
	}
	return elbCheckTagLimit(nil, tags)
}

// elbMergeTags returns existing with incoming applied, overwriting a key already
// present and appending one that is not.
//
// Existing order is preserved and a new key is appended, so a resource's tag list is a
// stable, replay-identical sequence rather than a map walk.
func elbMergeTags(existing, incoming []ELBTag) []ELBTag {
	merged := make([]ELBTag, len(existing))
	copy(merged, existing)
	for _, in := range incoming {
		replaced := false
		for i := range merged {
			if merged[i].Key == in.Key {
				merged[i].Value = in.Value
				replaced = true
				break
			}
		}
		if !replaced {
			merged = append(merged, in)
		}
	}
	return merged
}

// elbRemoveTagKeys returns existing without the named keys.
//
// A key that is not present is silently ignored: RemoveTags publishes no error for one,
// and its own description is "Removes the specified tags from the specified Elastic Load
// Balancing resources" with no requirement that they be there.
func elbRemoveTagKeys(existing []ELBTag, keys []string) []ELBTag {
	if len(existing) == 0 {
		return existing
	}
	remove := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		remove[k] = struct{}{}
	}
	kept := make([]ELBTag, 0, len(existing))
	for _, t := range existing {
		if _, drop := remove[t.Key]; !drop {
			kept = append(kept, t)
		}
	}
	return kept
}

// elbWriteTags persists a resolved resource's new tag set.
func (p *ELBPlugin) elbWriteTags(res *elbTaggedResource, tags []ELBTag) error {
	data, err := res.withTags(tags)
	if err != nil {
		return fmt.Errorf("elb marshal %s: %w", res.arn, err)
	}
	if err := p.state.Put(context.Background(), elbNamespace, res.stateKey, data); err != nil {
		return fmt.Errorf("elb write tags %s: %w", res.arn, err)
	}
	return nil
}

// --- Tagging operations ---

// addTags adds the given tags to the given resources.
//
// AWS: "Adds the specified tags to the specified Elastic Load Balancing resource type.
// Each tag consists of a key and an optional value. If a tag with the same key is
// already associated with the resource, AddTags updates its value." Both ResourceArns
// and Tags are required, and Tags carries a minimum of one item.
func (p *ELBPlugin) addTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arns := extractIndexedParams(req.Params, "ResourceArns.member")
	if len(arns) == 0 {
		return nil, &AWSError{Code: "ValidationError", Message: "ResourceArns is required",
			HTTPStatus: http.StatusBadRequest}
	}
	tags := extractELBTags(req.Params, "Tags.member")
	if len(tags) == 0 {
		return nil, &AWSError{Code: "ValidationError", Message: "Tags is required",
			HTTPStatus: http.StatusBadRequest}
	}
	if awsErr := elbCheckDuplicateTagKeys(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := elbCheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	resolved, awsErr, err := elbResolveAll(p.state, scope, arns)
	if err != nil {
		return nil, err
	}
	if awsErr != nil {
		return nil, awsErr
	}
	// Every resource is checked against the limit before any is written, for the same
	// reason resolution is: the request either applies to all of them or to none.
	for _, res := range resolved {
		if limitErr := elbCheckTagLimit(res.tags, tags); limitErr != nil {
			return nil, limitErr
		}
	}
	for _, res := range resolved {
		if writeErr := p.elbWriteTags(res, elbMergeTags(res.tags, tags)); writeErr != nil {
			return nil, writeErr
		}
	}

	return elbEmptyOKResponse("AddTags")
}

// removeTags removes the named tag keys from the given resources.
//
// TagKeys is required and carries 1–128 items, each under the same key constraints a
// Tag's own Key has. There is no DuplicateTagKeys here — the code is listed on AddTags
// and not on RemoveTags, and removing the same key twice removes it once.
func (p *ELBPlugin) removeTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arns := extractIndexedParams(req.Params, "ResourceArns.member")
	if len(arns) == 0 {
		return nil, &AWSError{Code: "ValidationError", Message: "ResourceArns is required",
			HTTPStatus: http.StatusBadRequest}
	}
	keys := extractIndexedParams(req.Params, "TagKeys.member")
	if len(keys) == 0 {
		return nil, &AWSError{Code: "ValidationError", Message: "TagKeys is required",
			HTTPStatus: http.StatusBadRequest}
	}
	if len(keys) > elbMaxRemoveTagKeys {
		return nil, elbTagValidationError(
			"TagKeys must name no more than %d keys; %d were supplied", elbMaxRemoveTagKeys, len(keys))
	}
	for _, k := range keys {
		if awsErr := elbCheckTagKey(k); awsErr != nil {
			return nil, awsErr
		}
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	resolved, awsErr, err := elbResolveAll(p.state, scope, arns)
	if err != nil {
		return nil, err
	}
	if awsErr != nil {
		return nil, awsErr
	}
	for _, res := range resolved {
		if writeErr := p.elbWriteTags(res, elbRemoveTagKeys(res.tags, keys)); writeErr != nil {
			return nil, writeErr
		}
	}

	return elbEmptyOKResponse("RemoveTags")
}

// describeTags reports the tags on up to [elbDescribeTagsMaxResources] resources.
//
// A resource carrying no tags is still reported, with an empty Tags list: the operation
// answers about the resources the request named, and omitting an untagged one would make
// "no tags" indistinguishable from "no such resource" — which the operation has four
// separate codes to distinguish.
func (p *ELBPlugin) describeTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arns := extractIndexedParams(req.Params, "ResourceArns.member")
	if len(arns) == 0 {
		return nil, &AWSError{Code: "ValidationError", Message: "ResourceArns is required",
			HTTPStatus: http.StatusBadRequest}
	}
	if len(arns) > elbDescribeTagsMaxResources {
		return nil, elbTagValidationError(
			"ResourceArns must name no more than %d resources; %d were supplied",
			elbDescribeTagsMaxResources, len(arns))
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	resolved, awsErr, err := elbResolveAll(p.state, scope, arns)
	if err != nil {
		return nil, err
	}
	if awsErr != nil {
		return nil, awsErr
	}

	type tagsResult struct {
		TagDescriptions []elbTagDescriptionItem `xml:"TagDescriptions>member"`
	}
	type response struct {
		XMLName xml.Name   `xml:"DescribeTagsResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Result  tagsResult `xml:"DescribeTagsResult"`
	}
	resp := response{XMLNS: elbXMLNS}
	for _, res := range resolved {
		resp.Result.TagDescriptions = append(resp.Result.TagDescriptions, elbTagDescriptionItem{
			ResourceArn: res.arn,
			Tags:        elbTagItems(res.tags),
		})
	}
	return elbXMLResponse(http.StatusOK, resp)
}

// elbTagItem is the XML representation of one ELB tag.
type elbTagItem struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// elbTagDescriptionItem is the XML representation of one resource's tags.
type elbTagDescriptionItem struct {
	ResourceArn string       `xml:"ResourceArn"`
	Tags        []elbTagItem `xml:"Tags>member"`
}

// elbTagItems renders a tag list for the wire.
func elbTagItems(tags []ELBTag) []elbTagItem {
	items := make([]elbTagItem, 0, len(tags))
	for _, t := range tags {
		items = append(items, elbTagItem(t))
	}
	return items
}
