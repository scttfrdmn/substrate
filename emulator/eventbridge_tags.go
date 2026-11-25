package emulator

// EventBridge tagging: TagResource, UntagResource, ListTagsForResource, PutRule's and
// CreateEventBus's Tags, the ARN rule that decides which resource a ResourceARN names (#1607), and
// the Resource Groups Tagging API provider for rules and buses (#1640).
//
// # What can be tagged
//
// "In EventBridge, rules and event buses can be tagged." Both carry their tags on their own record
// ([EBRule.Tags], [EBEventBus.Tags]). The default bus has a record only once something has been
// written to it, so tagging it creates one; until then it is synthesized and untagged. Any other
// resource type in an EventBridge ARN is not taggable at all and is a ValidationError, the
// common-errors fallback this plugin uses for a malformed request (see eventbridge_errors.go).
//
// # The ARN, not the caller, names the resource
//
// The account and Region come from the ARN, the rule #826 set for every resolver. A rule ARN is
// arn:{partition}:events:{region}:{account}:rule/{name} for the default bus and rule/{bus}/{name}
// for a custom one, and each form names the rule on that bus ([ebRuleRef.key]). A bus ARN is
// event-bus/{name}.
//
// # Quota and validation
//
// "You can associate as many as 50 tags with a resource." TagResource's page publishes no code for
// exceeding it; PutRule's publishes LimitExceededException, "The request failed because it attempted
// to create resource beyond the allowed service quota", and the tag ceiling is that quota, so every
// tag write answers it — for TagResource that is substrate's reading. A Tag's Key is 1 to 128
// characters and its Value 0 to 256 (API_Tag); a tag outside them is a ValidationError.
//
// Tags are stored sorted by key, so ListTagsForResource answers in an order that does not depend on
// how a merge was built, and a recorded run replays to the same body (#862). Messages in this file
// are substrate's own.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ebMaxTagsPerResource is the number of tags an EventBridge resource may carry.
const ebMaxTagsPerResource = 50

// Tag constraints API_Tag publishes.
const (
	ebMaxTagKeyLen   = 128
	ebMaxTagValueLen = 256
)

// ebRuleNotFound reports that a rule does not exist on its bus.
//
// ResourceNotFoundException at 400, the status every EventBridge page that publishes the code gives
// it, glossed "An entity that you specified does not exist." The wording is AWS's as observed in
// two reports of PutTargets naming a rule on the wrong bus: "Rule SendToApi-30d74a6 does not exist
// on EventBus default." (Stack Overflow 76683058) and pulumi/pulumi-aws#1323.
func ebRuleNotFound(ref ebRuleRef) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    fmt.Sprintf("Rule %s does not exist on EventBus %s.", ref.name, ref.bus),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ebValidateTags checks each tag against API_Tag's length constraints.
func ebValidateTags(tags []EBTag) *AWSError {
	for _, t := range tags {
		if t.Key == "" || len(t.Key) > ebMaxTagKeyLen {
			return ebValidationError(fmt.Sprintf("tag key %q must be 1 to %d characters", t.Key, ebMaxTagKeyLen))
		}
		if len(t.Value) > ebMaxTagValueLen {
			return ebValidationError(fmt.Sprintf("the value of tag %q must be at most %d characters", t.Key, ebMaxTagValueLen))
		}
	}
	return nil
}

// ebCheckTagQuota refuses a tag set larger than the 50-tag ceiling.
func ebCheckTagQuota(resource string, count int) *AWSError {
	if count <= ebMaxTagsPerResource {
		return nil
	}
	return &AWSError{
		Code: "LimitExceededException",
		Message: fmt.Sprintf("%s would have %d tags; a resource can have no more than %d",
			resource, count, ebMaxTagsPerResource),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ebMergeTags returns existing with add applied (an existing key takes the new value) and removeKeys
// removed, sorted by key.
func ebMergeTags(existing, add []EBTag, removeKeys []string) []EBTag {
	m := make(map[string]string, len(existing)+len(add))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for _, t := range add {
		m[t.Key] = t.Value
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	out := make([]EBTag, 0, len(m))
	for k, v := range m {
		out = append(out, EBTag{Key: k, Value: v})
	}
	sortTagsByKey(out, func(t EBTag) string { return t.Key })
	return out
}

// ebTagTarget is the rule or bus a tagging ARN names.
type ebTagTarget struct {
	// ref is the rule, or for a bus its account, Region and bus with no name.
	ref ebRuleRef

	// isBus reports that the ARN names the bus itself.
	isBus bool
}

// key is the state key of the record the target's tags live on.
func (t ebTagTarget) key() string {
	if t.isBus {
		return ebBusKey(t.ref.accountID, t.ref.region, t.ref.bus)
	}
	return t.ref.key()
}

// notFound is the refusal for a target with no record.
func (t ebTagTarget) notFound() *AWSError {
	if t.isBus {
		return ebBusNotFound(t.ref.bus)
	}
	return ebRuleNotFound(t.ref)
}

// inScope reports whether the target is in accountID and region, an empty region matching any.
// EventBridge is regional and substrate models no cross-account access, so a tag operation on an
// ARN naming another account's or Region's resource finds nothing, as the Resource Groups Tagging
// API states for its own writes: "You can only tag resources that are located in the specified AWS
// Region for the AWS account" (API_TagResources).
func (t ebTagTarget) inScope(accountID, region string) bool {
	return t.ref.accountID == accountID && (region == "" || t.ref.region == region)
}

// ebRequestTagTarget parses a tag operation's ResourceARN and refuses one outside the caller's
// account and Region with the target's own not-found answer.
func ebRequestTagTarget(ctx *RequestContext, arn string) (ebTagTarget, *AWSError) {
	target, arnErr := ebParseTagARN(arn)
	if arnErr != nil {
		return target, arnErr
	}
	if !target.inScope(ctx.AccountID, ctx.Region) {
		return target, target.notFound()
	}
	return target, nil
}

// ebParseTagARN resolves a ResourceARN to the rule or bus it names; see this file's preamble.
func ebParseTagARN(arn string) (ebTagTarget, *AWSError) {
	if arn == "" {
		return ebTagTarget{}, ebValidationError("ResourceARN is required")
	}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "events" || parts[3] == "" || parts[4] == "" {
		return ebTagTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q is not an EventBridge ARN", arn))
	}
	resource := parts[5]
	ref := ebRuleRef{accountID: parts[4], region: parts[3], bus: ebDefaultBus}
	switch {
	case strings.HasPrefix(resource, "rule/"):
		segs := strings.Split(strings.TrimPrefix(resource, "rule/"), "/")
		switch len(segs) {
		case 1:
			ref.name = segs[0]
		case 2:
			ref.bus, ref.name = segs[0], segs[1]
		}
		if ref.name == "" || ref.bus == "" {
			return ebTagTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q does not name a rule", arn))
		}
		return ebTagTarget{ref: ref}, nil
	case strings.HasPrefix(resource, "event-bus/"):
		ref.bus = strings.TrimPrefix(resource, "event-bus/")
		if ref.bus == "" {
			return ebTagTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q does not name an event bus", arn))
		}
		return ebTagTarget{ref: ref, isBus: true}, nil
	default:
		return ebTagTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q names neither a rule nor an event bus, the two taggable EventBridge resources", arn))
	}
}

// ebRequestBus returns the bus a request's EventBusName (or DescribeEventBus's Name) names in the
// caller's account and Region. A name is the caller's, and an absent value is the default bus. An
// event-bus ARN names the bus in the ARN's own account and Region (#826); substrate models no
// cross-account or cross-Region bus access, so an ARN naming another account or Region is a bus this
// caller cannot reach, answered ResourceNotFoundException as a bus that does not exist is.
func ebRequestBus(ctx *RequestContext, v string) (string, *AWSError) {
	if !strings.HasPrefix(v, "arn:") {
		return ebBusName(v), nil
	}
	parts := strings.SplitN(v, ":", 6)
	if len(parts) < 6 || parts[2] != "events" || !strings.HasPrefix(parts[5], "event-bus/") || parts[5] == "event-bus/" {
		return "", ebValidationError(fmt.Sprintf("%q is not an event-bus ARN", v))
	}
	if parts[3] != ctx.Region || parts[4] != ctx.AccountID {
		return "", ebBusNotFound(v)
	}
	return strings.TrimPrefix(parts[5], "event-bus/"), nil
}

// ebBusName returns the bus name an EventBusName value carries, a name or an event-bus ARN, without
// regard to the ARN's account and Region. An absent value is the default bus. Requests resolve
// through [ebRequestBus]; this serves CloudFormation, whose values are the stack's own.
func ebBusName(v string) string {
	if i := strings.LastIndex(v, ":event-bus/"); i >= 0 {
		return v[i+len(":event-bus/"):]
	}
	if v == "" {
		return ebDefaultBus
	}
	return v
}

// loadRule reads a rule record, answering nil for an absent one. The record's EventBusName is the
// bus its key names: a record written before rules were keyed per bus is stored at the default
// bus's key whatever bus its request named, and it is that default-bus rule.
func (p *EventBridgePlugin) loadRule(goCtx context.Context, ref ebRuleRef) (*EBRule, error) {
	data, err := p.state.Get(goCtx, eventbridgeNamespace, ref.key())
	if err != nil {
		return nil, fmt.Errorf("eventbridge load rule %s: %w", ref.name, err)
	}
	if data == nil {
		return nil, nil
	}
	var rule EBRule
	if err := json.Unmarshal(data, &rule); err != nil {
		return nil, fmt.Errorf("eventbridge decode rule %s: %w", ref.name, err)
	}
	rule.EventBusName = ref.bus
	if rule.ARN == "" {
		rule.ARN = ref.arn()
	}
	return &rule, nil
}

// saveRule writes a rule record back.
func (p *EventBridgePlugin) saveRule(goCtx context.Context, ref ebRuleRef, rule *EBRule) error {
	data, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("eventbridge encode rule %s: %w", rule.Name, err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, ref.key(), data); err != nil {
		return fmt.Errorf("eventbridge store rule %s: %w", rule.Name, err)
	}
	return nil
}

// ebTaggedRecord is a rule or bus record loaded for a tag read or write; exactly one is set.
type ebTaggedRecord struct {
	rule *EBRule
	bus  *EBEventBus
}

// tags returns the record's tags.
func (r ebTaggedRecord) tags() []EBTag {
	if r.rule != nil {
		return r.rule.Tags
	}
	return r.bus.Tags
}

// arn returns the record's ARN.
func (r ebTaggedRecord) arn() string {
	if r.rule != nil {
		return r.rule.ARN
	}
	return r.bus.ARN
}

// everTagged returns the record's previously-tagged flag.
func (r ebTaggedRecord) everTagged() bool {
	if r.rule != nil {
		return r.rule.EverTagged
	}
	return r.bus.EverTagged
}

// set replaces the record's tags and previously-tagged flag.
func (r ebTaggedRecord) set(tags []EBTag, ever bool) {
	if r.rule != nil {
		r.rule.Tags, r.rule.EverTagged = tags, ever
		return
	}
	r.bus.Tags, r.bus.EverTagged = tags, ever
}

// ebLoadTaggedRecord reads the rule or bus stored at key, reporting false when there is none. The
// default bus is always found ([ebLoadBusAt]).
func ebLoadTaggedRecord(ctx context.Context, state StateManager, key string) (ebTaggedRecord, bool, error) {
	if strings.HasPrefix(key, ebBusKeyPrefix) {
		bus, found, err := ebLoadBusAt(ctx, state, key)
		return ebTaggedRecord{bus: bus}, found, err
	}
	if !strings.HasPrefix(key, ebRuleKeyPrefix) {
		return ebTaggedRecord{}, false, fmt.Errorf("eventbridge: %s is not a taggable record key", key)
	}
	raw, err := state.Get(ctx, eventbridgeNamespace, key)
	if err != nil {
		return ebTaggedRecord{}, false, fmt.Errorf("eventbridge load rule %s: %w", key, err)
	}
	if raw == nil {
		return ebTaggedRecord{}, false, nil
	}
	var rule EBRule
	if err := json.Unmarshal(raw, &rule); err != nil {
		return ebTaggedRecord{}, false, fmt.Errorf("eventbridge decode rule %s: %w", key, err)
	}
	return ebTaggedRecord{rule: &rule}, true, nil
}

// ebStoreTaggedRecord writes a record loaded by [ebLoadTaggedRecord] back to key.
func ebStoreTaggedRecord(ctx context.Context, state StateManager, key string, rec ebTaggedRecord) error {
	var v any = rec.rule
	if rec.rule == nil {
		v = rec.bus
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("eventbridge encode %s: %w", key, err)
	}
	if err := state.Put(ctx, eventbridgeNamespace, key, data); err != nil {
		return fmt.Errorf("eventbridge store %s: %w", key, err)
	}
	return nil
}

// ebMergeTagsAt merges add and remove into the rule or bus stored at key and stamps its tag history.
// With quota enforced and tags to add, the merged set is held to the 50-tag ceiling, reported as a
// [taggingQuotaError] naming the resource's ARN. An absent record is [errTagResourceNotFound]. It
// takes the state rather than the plugin so the tagging plugin's direct path, used when no
// EventBridge plugin is registered, runs the same code; the plugin's callers hold
// [EventBridgePlugin.ruleMu].
func ebMergeTagsAt(
	ctx context.Context, state StateManager, key string, add []EBTag, remove []string, quota tagQuotaMode,
) error {
	rec, found, err := ebLoadTaggedRecord(ctx, state, key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: %s/%s", errTagResourceNotFound, eventbridgeNamespace, key)
	}
	before := rec.tags()
	merged := ebMergeTags(before, add, remove)
	if quota == enforceTagQuota && len(add) > 0 {
		if quotaErr := ebCheckTagQuota(rec.arn(), len(merged)); quotaErr != nil {
			return taggingQuotaRefusal(quotaErr)
		}
	}
	if len(merged) == 0 {
		merged = nil
	}
	rec.set(merged, taggingEverTagged(rec.everTagged(), len(before), len(add)))
	return ebStoreTaggedRecord(ctx, state, key, rec)
}

// ebTagMapAt is [ebMergeTagsAt] for the tagging API's map of tags to add. With quota enforced the
// tags are held to API_Tag's lengths too, as EventBridge's own TagResource holds them.
func ebTagMapAt(ctx context.Context, state StateManager, key string, add map[string]string, remove []string, quota tagQuotaMode) error {
	tags := make([]EBTag, 0, len(add))
	for _, k := range taggingSortedKeys(add) {
		tags = append(tags, EBTag{Key: k, Value: add[k]})
	}
	if quota == enforceTagQuota {
		if tagErr := ebValidateTags(tags); tagErr != nil {
			return taggingQuotaRefusal(tagErr)
		}
	}
	return ebMergeTagsAt(ctx, state, key, tags, remove, quota)
}

// ebOwnTagWrite runs a tag write for EventBridge's own TagResource or UntagResource, answering the
// target's own not-found and the quota refusal as the [AWSError] the operation publishes.
func (p *EventBridgePlugin) ebOwnTagWrite(ctx *RequestContext, arn string, add []EBTag, remove []string) error {
	target, arnErr := ebRequestTagTarget(ctx, arn)
	if arnErr != nil {
		return arnErr
	}
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	err := ebMergeTagsAt(context.Background(), p.state, target.key(), add, remove, enforceTagQuota)
	var quota *taggingQuotaError
	switch {
	case errors.Is(err, errTagResourceNotFound):
		return target.notFound()
	case errors.As(err, &quota):
		return quota.awsErr
	}
	return err
}

// tagResource handles TagResource: it merges tags onto a rule or bus.
func (p *EventBridgePlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string  `json:"ResourceARN"`
		Tags        []EBTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	if body.Tags == nil {
		return nil, ebValidationError("Tags is required")
	}
	if tagErr := ebValidateTags(body.Tags); tagErr != nil {
		return nil, tagErr
	}
	if err := p.ebOwnTagWrite(ctx, body.ResourceARN, body.Tags, nil); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// untagResource handles UntagResource: it removes the named keys from a rule or bus. A key the
// resource does not carry is not an error; the page publishes no code for one.
func (p *EventBridgePlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	if body.TagKeys == nil {
		return nil, ebValidationError("TagKeys is required")
	}
	if err := p.ebOwnTagWrite(ctx, body.ResourceARN, nil, body.TagKeys); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// listTagsForResource handles ListTagsForResource. An untagged resource answers `{"Tags":[]}`: the
// response's one member is documented without a condition, and a caller converging tags compares the
// list it reads.
func (p *EventBridgePlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string `json:"ResourceARN"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	target, arnErr := ebRequestTagTarget(ctx, body.ResourceARN)
	if arnErr != nil {
		return nil, arnErr
	}
	rec, found, err := ebLoadTaggedRecord(context.Background(), p.state, target.key())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, target.notFound()
	}
	tags := rec.tags()
	if tags == nil {
		tags = []EBTag{}
	}
	return ebJSONResponse(http.StatusOK, struct {
		Tags []EBTag `json:"Tags"`
	}{Tags: tags})
}

// ----- Tagging API provider ------------------------------------------------

// EventBridge answers the Resource Groups Tagging API for its rules and buses (#1640): GetResources
// through [EventBridgePlugin.listTaggedResources], and TagResources and UntagResources through
// [EventBridgePlugin.tagResourceByARN], under [EventBridgePlugin.ruleMu], the lock every write to a
// rule or bus holds. A refusal EventBridge's own TagResource would answer is carried out as a
// [taggingQuotaError], so the tagging API reports the owning service's own code.

// ebTagStateKey resolves a rule or bus ARN to its record's state key through [ebParseTagARN], which
// EventBridge's own three tagging operations parse through. An ARN it refuses is the unsupported-type
// answer, and one outside accountID and region ([ebTagTarget.inScope]) names no resource the caller
// can tag, [errTagResourceNotFound]. The tagging plugin's direct path knows the caller's account and
// not its Region, so it passes an empty region. resource is the ARN's resource portion, for the
// message.
func ebTagStateKey(arn, resource, accountID, region string) (string, error) {
	target, arnErr := ebParseTagARN(arn)
	if arnErr != nil {
		return "", unsupportedTagResource("EventBridge %q is not a rule or event-bus ARN: %s", resource, arnErr.Code)
	}
	if !target.inScope(accountID, region) {
		return "", fmt.Errorf("%w: %s is outside the caller's account and Region", errTagResourceNotFound, arn)
	}
	return target.key(), nil
}

// ebListTaggedResources reports the caller's EventBridge rules and buses, under the ARN each record
// carries. The default bus is reported only once a write has given it a record, which every tag
// write does, so a default bus that has never been tagged is, correctly, absent (#938).
func ebListTaggedResources(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	for _, prefix := range []string{ebRuleKeyPrefix, ebBusKeyPrefix} {
		keys, err := state.List(ctx, eventbridgeNamespace, taggingScanPrefix(prefix, reqCtx))
		if err != nil {
			return out, fmt.Errorf("list eventbridge %s records: %w", prefix, err)
		}
		for _, k := range keys {
			rec, found, err := ebLoadTaggedRecord(ctx, state, k)
			if err != nil {
				return out, err
			}
			if !found {
				continue
			}
			tags := make([]taggingTag, 0, len(rec.tags()))
			for _, t := range rec.tags() {
				tags = append(tags, taggingTag(t))
			}
			out = append(out, resourceTagMapping{ResourceARN: rec.arn(), Tags: tags, everTagged: rec.everTagged()})
		}
	}
	return out, nil
}

// listTaggedResources implements [taggableResourceLister].
func (p *EventBridgePlugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return ebListTaggedResources(ctx, p.state, reqCtx)
}

// tagResourceByARN implements [resourceTagger]. EventBridge's own TagResource publishes no rule
// about the reserved prefix, so an `aws:` key is refused with the tagging API's own answer
// ([taggingReservedKeyRefusal]).
func (p *EventBridgePlugin) tagResourceByARN(
	reqCtx *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	parts, ok := taggingSplitARN(arn)
	if !ok {
		return taggingResolveFailed(fmt.Errorf("invalid ARN: %q", arn))
	}
	key, err := ebTagStateKey(arn, parts[5], reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return taggingResolveFailed(err)
	}
	if refusal := taggingReservedKeyRefusal(add, remove); refusal != nil {
		return taggingQuotaRefusal(refusal)
	}
	return p.tagRecord(context.Background(), key, add, remove, quota)
}

// tagRecord implements [resourceTagger], through [ebTagMapAt] under the lock EventBridge's own tag
// operations hold.
func (p *EventBridgePlugin) tagRecord(
	ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	return ebTagMapAt(ctx, p.state, key, add, remove, quota)
}
