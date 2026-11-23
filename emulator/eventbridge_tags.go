package emulator

// EventBridge tagging: TagResource, UntagResource, ListTagsForResource, PutRule's Tags, and the ARN
// rule that decides which resource a ResourceARN names (#1607).
//
// All three operations were unrouted, and Terraform's aws_cloudwatch_event_rule calls
// ListTagsForResource on every refresh, so a rule substrate had created could not be read back.
//
// # What can be tagged
//
// "In EventBridge, rules and event buses can be tagged." Rules are modeled and carry their tags on
// the rule record ([EBRule.Tags]). Event buses are not modeled as records — substrate routes no
// CreateEventBus, and ListEventBuses answers the default bus from no stored state — so an event-bus
// ARN names nothing substrate can hold tags on, and is answered ResourceNotFoundException ("An entity
// that you specified does not exist."). That is exactly true of a custom bus, which cannot exist here;
// for the default bus it is a documented gap rather than AWS's answer. Any other resource type in an
// EventBridge ARN is not taggable at all and is a ValidationError, the common-errors fallback this
// plugin uses for a malformed request (see eventbridge_errors.go).
//
// # The ARN, not the caller, names the rule
//
// The account and Region come from the ARN, the rule #826 set for every resolver. A rule ARN is
// arn:{partition}:events:{region}:{account}:rule/{name} for the default bus and rule/{bus}/{name}
// for a custom one; substrate mints the first form for every rule and keys rules by name, so the
// second form resolves by name and must name the bus the rule is stored on.
//
// # Quota and validation
//
// "You can associate as many as 50 tags with a resource." TagResource's page publishes no code for
// exceeding it; PutRule's publishes LimitExceededException, "The request failed because it attempted
// to create resource beyond the allowed service quota", and the tag ceiling is that quota, so both
// operations answer it — for TagResource that is substrate's reading. A Tag's Key is 1 to 128
// characters and its Value 0 to 256 (API_Tag); a tag outside them is a ValidationError.
//
// Tags are stored sorted by key, so ListTagsForResource answers in an order that does not depend on
// how a merge was built, and a recorded run replays to the same body (#862). Messages in this file
// are substrate's own.

import (
	"context"
	"encoding/json"
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

// ebRuleNotFound reports that a rule does not exist.
//
// ResourceNotFoundException at 400, the status every EventBridge page that publishes the code gives
// it, glossed "An entity that you specified does not exist." Substrate answered 404 until #1607. The
// message is substrate's own: no page publishes AWS's text for it.
func ebRuleNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    fmt.Sprintf("no rule named %q is stored in this account and Region", name),
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

// ebRuleTarget is the rule a tagging ARN names.
type ebRuleTarget struct {
	accountID, region, bus, name string
}

// ebParseTagARN resolves a ResourceARN to the rule it names; see this file's preamble.
func ebParseTagARN(arn string) (ebRuleTarget, *AWSError) {
	if arn == "" {
		return ebRuleTarget{}, ebValidationError("ResourceARN is required")
	}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "events" || parts[3] == "" || parts[4] == "" {
		return ebRuleTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q is not an EventBridge ARN", arn))
	}
	resource := parts[5]
	switch {
	case strings.HasPrefix(resource, "rule/"):
		segs := strings.Split(strings.TrimPrefix(resource, "rule/"), "/")
		t := ebRuleTarget{accountID: parts[4], region: parts[3]}
		switch len(segs) {
		case 1:
			t.name = segs[0]
		case 2:
			t.bus, t.name = segs[0], segs[1]
		}
		if t.name == "" || (len(segs) == 2 && t.bus == "") {
			return ebRuleTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q does not name a rule", arn))
		}
		return t, nil
	case strings.HasPrefix(resource, "event-bus/"):
		return ebRuleTarget{}, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("substrate stores no record for event bus %q, so it holds no tags", strings.TrimPrefix(resource, "event-bus/")),
			HTTPStatus: http.StatusBadRequest,
		}
	default:
		return ebRuleTarget{}, ebValidationError(fmt.Sprintf("ResourceARN %q names neither a rule nor an event bus, the two taggable EventBridge resources", arn))
	}
}

// ebBusName returns the bus name an EventBusName value carries, which PutRule accepts as a name or
// an event-bus ARN.
func ebBusName(v string) string {
	if i := strings.LastIndex(v, ":event-bus/"); i >= 0 {
		return v[i+len(":event-bus/"):]
	}
	if v == "" {
		return "default"
	}
	return v
}

// loadRule reads a rule record, answering nil for an absent one.
func (p *EventBridgePlugin) loadRule(goCtx context.Context, accountID, region, name string) (*EBRule, error) {
	data, err := p.state.Get(goCtx, eventbridgeNamespace, ebRuleKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("eventbridge load rule %s: %w", name, err)
	}
	if data == nil {
		return nil, nil
	}
	var rule EBRule
	if err := json.Unmarshal(data, &rule); err != nil {
		return nil, fmt.Errorf("eventbridge decode rule %s: %w", name, err)
	}
	return &rule, nil
}

// saveRule writes a rule record back.
func (p *EventBridgePlugin) saveRule(goCtx context.Context, accountID, region string, rule *EBRule) error {
	data, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("eventbridge encode rule %s: %w", rule.Name, err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, ebRuleKey(accountID, region, rule.Name), data); err != nil {
		return fmt.Errorf("eventbridge store rule %s: %w", rule.Name, err)
	}
	return nil
}

// resolveTaggedRule loads the rule a ResourceARN names, refusing a malformed ARN and an absent rule.
func (p *EventBridgePlugin) resolveTaggedRule(arn string) (*EBRule, ebRuleTarget, error) {
	target, arnErr := ebParseTagARN(arn)
	if arnErr != nil {
		return nil, target, arnErr
	}
	rule, err := p.loadRule(context.Background(), target.accountID, target.region, target.name)
	if err != nil {
		return nil, target, err
	}
	if rule == nil || (target.bus != "" && ebBusName(rule.EventBusName) != target.bus) {
		return nil, target, ebRuleNotFound(target.name)
	}
	return rule, target, nil
}

// tagResource handles TagResource: it merges tags onto a rule.
func (p *EventBridgePlugin) tagResource(req *AWSRequest) (*AWSResponse, error) {
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

	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	rule, target, err := p.resolveTaggedRule(body.ResourceARN)
	if err != nil {
		return nil, err
	}
	merged := ebMergeTags(rule.Tags, body.Tags, nil)
	if quotaErr := ebCheckTagQuota(body.ResourceARN, len(merged)); quotaErr != nil {
		return nil, quotaErr
	}
	rule.EverTagged = taggingEverTagged(rule.EverTagged, len(rule.Tags), len(body.Tags))
	rule.Tags = merged
	if err := p.saveRule(context.Background(), target.accountID, target.region, rule); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// untagResource handles UntagResource: it removes the named keys from a rule. A key the rule does
// not carry is not an error; the page publishes no code for one.
func (p *EventBridgePlugin) untagResource(req *AWSRequest) (*AWSResponse, error) {
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

	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	rule, target, err := p.resolveTaggedRule(body.ResourceARN)
	if err != nil {
		return nil, err
	}
	rule.EverTagged = taggingEverTagged(rule.EverTagged, len(rule.Tags), 0)
	rule.Tags = ebMergeTags(rule.Tags, nil, body.TagKeys)
	if err := p.saveRule(context.Background(), target.accountID, target.region, rule); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// listTagsForResource handles ListTagsForResource. An untagged rule answers `{"Tags":[]}`: the
// response's one member is documented without a condition, and a caller converging tags compares the
// list it reads.
func (p *EventBridgePlugin) listTagsForResource(req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string `json:"ResourceARN"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	rule, _, err := p.resolveTaggedRule(body.ResourceARN)
	if err != nil {
		return nil, err
	}
	tags := rule.Tags
	if tags == nil {
		tags = []EBTag{}
	}
	return ebJSONResponse(http.StatusOK, struct {
		Tags []EBTag `json:"Tags"`
	}{Tags: tags})
}

// scanEventBridgeRules reports every EventBridge rule in the caller's account and Region, with its
// tags, for the Resource Groups Tagging API's GetResources (#1607).
//
// It lives beside the rule's tag operations rather than in tagging_plugin.go so the prefix it scans,
// [ebRuleKeyPrefix], and the record it decodes are read next to the code that writes them. The colon
// in the prefix keeps the rule-name index, "ebrule_names:", out of the scan.
func (p *TaggingPlugin) scanEventBridgeRules(goCtx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	keys, err := p.state.List(goCtx, eventbridgeNamespace, taggingScanPrefix(ebRuleKeyPrefix, reqCtx))
	if err != nil {
		return nil, fmt.Errorf("list eventbridge rules: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, eventbridgeNamespace, k)
		if err != nil {
			return out, fmt.Errorf("get eventbridge rule %s: %w", k, err)
		}
		var rule EBRule
		if raw == nil || json.Unmarshal(raw, &rule) != nil {
			continue
		}
		tags := make([]taggingTag, 0, len(rule.Tags))
		for _, t := range rule.Tags {
			tags = append(tags, taggingTag(t))
		}
		out = append(out, resourceTagMapping{ResourceARN: rule.ARN, Tags: tags, everTagged: rule.EverTagged})
	}
	return out, nil
}
