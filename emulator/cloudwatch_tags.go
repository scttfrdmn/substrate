package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"
)

// CloudWatch tagging: TagResource, UntagResource and ListTagsForResource on alarm ARNs (#1618),
// and the tags PutMetricAlarm sets when it creates an alarm. #1607 found ListTagsForResource
// unrouted from the Terraform side — the AWS provider reads an alarm's tags on every refresh — and
// #1618 from a consumer that reads its own ownership tag before it updates or deletes an alarm.
// cloudwatchlogs_tags.go is the model.
//
// # The taggable set
//
// "Currently, the only CloudWatch resources that can be tagged are alarms, dashboards, metric
// streams and Contributor Insights rules." Substrate models alarms only. An ARN naming one of the
// other three published types is well-formed and names a resource that cannot exist here, so it
// answers ResourceNotFoundException — "The named resource does not exist." — which is exactly true
// of it, rather than an ARN refusal: the ARN is not what is wrong. Anything that is not a
// CloudWatch ARN of one of the four published forms is InvalidParameterValue, "The value of an
// input parameter is bad or out-of-range", the code all three pages publish. The messages are
// substrate's own.
//
// The account and Region come from the ARN and never from the caller's request context, the rule
// #826 set for every ARN resolver: an ARN naming another account's alarm resolves that alarm or
// none.
//
// # The quota
//
// "You can associate as many as 50 tags with a CloudWatch resource." It is checked against the
// merged set, at TagResource and at the PutMetricAlarm that creates an alarm. None of the pages
// publishes a code for exceeding it, so the refusal is the common InvalidParameterValue, with
// substrate's own message.
//
// # Previously tagged
//
// GetResources reports a resource that is "currently tagged or ever had a tag" (#938). The second
// half is a side-car beside the alarm record — alarm_tagged:{account}/{region}/{name} — for the
// reason [cwlLogGroupTaggedKeyPrefix] gives for log groups: it keeps substrate's bookkeeping off
// the persisted record, so no response shape can come to carry it. DeleteAlarms removes it with
// the alarm.

// cwAlarmKeyPrefix is the colon-terminated prefix every alarm record is stored under; see
// [cwAlarmStateKey]. The colon distinguishes it from the alarm_names: and alarm_tagged: keys.
const cwAlarmKeyPrefix = "alarm:"

// cwAlarmTaggedKeyPrefix prefixes the side-car record that says an alarm has carried a tag.
const cwAlarmTaggedKeyPrefix = "alarm_tagged:"

// cwAlarmARNType is the resource-type segment of an alarm ARN.
const cwAlarmARNType = "alarm:"

// cwUnmodeledTaggableTypes are the resource-type prefixes of the three other taggable CloudWatch
// types, which substrate does not model.
var cwUnmodeledTaggableTypes = []string{"dashboard/", "insight-rule/", "metric-stream/"}

// cwAlarmTaggedKey returns the tag-history side-car key for the alarm stored at alarmKey.
func cwAlarmTaggedKey(alarmKey string) string {
	return cwAlarmTaggedKeyPrefix + strings.TrimPrefix(alarmKey, cwAlarmKeyPrefix)
}

// cwAlarmEverTagged reports whether the alarm stored at alarmKey has carried a tag.
func cwAlarmEverTagged(ctx context.Context, state StateManager, alarmKey string) (bool, error) {
	data, err := state.Get(ctx, monitoringNamespace, cwAlarmTaggedKey(alarmKey))
	if err != nil {
		return false, fmt.Errorf("read alarm tag history: %w", err)
	}
	return data != nil, nil
}

// cwStampAlarmEverTagged records that the alarm stored at alarmKey has carried a tag, when
// [taggingEverTagged] says this write makes that so. tagsBefore is counted before the merge.
func cwStampAlarmEverTagged(ctx context.Context, state StateManager, alarmKey string, tagsBefore, tagsAdded int) error {
	if !taggingEverTagged(false, tagsBefore, tagsAdded) {
		return nil
	}
	if err := state.Put(ctx, monitoringNamespace, cwAlarmTaggedKey(alarmKey), []byte("true")); err != nil {
		return fmt.Errorf("stamp alarm tag history: %w", err)
	}
	return nil
}

// cwCheckTags refuses a tag set over the 50-tag quota or holding a tag outside the Tag shape's
// published lengths: a Key of 1 to 128 characters and a Value of 0 to 256.
func cwCheckTags(tags map[string]string) *AWSError {
	if len(tags) > cwMaxTagsPerResource {
		return cwInvalidParameterValue(fmt.Sprintf(
			"A CloudWatch resource can have at most %d tags; this request would leave it with %d.",
			cwMaxTagsPerResource, len(tags)))
	}
	for k, v := range tags {
		if n := utf8.RuneCountInString(k); n < 1 || n > 128 {
			return cwInvalidParameterValue("A tag Key must be 1 to 128 characters long.")
		}
		if utf8.RuneCountInString(v) > 256 {
			return cwInvalidParameterValue("A tag Value must be at most 256 characters long.")
		}
	}
	return nil
}

// cwParseAlarmTagARN parses a ResourceARN naming an alarm into the alarm's state key.
func cwParseAlarmTagARN(resourceARN string) (string, *AWSError) {
	if resourceARN == "" {
		return "", cwMissingParameter("ResourceARN")
	}
	// arn:aws:cloudwatch:{region}:{account}:alarm:{name} — six segments, the last keeping its own
	// colons, since an alarm name may contain them.
	parts := strings.SplitN(resourceARN, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "cloudwatch" || parts[4] == "" {
		return "", cwInvalidParameterValue("The ResourceARN " + resourceARN + " is not a CloudWatch resource ARN.")
	}
	region, account, resource := parts[3], parts[4], parts[5]
	for _, prefix := range cwUnmodeledTaggableTypes {
		if strings.HasPrefix(resource, prefix) {
			return "", cwResourceNotFound(resourceARN)
		}
	}
	name, ok := strings.CutPrefix(resource, cwAlarmARNType)
	if !ok || name == "" || region == "" {
		return "", cwInvalidParameterValue("The ResourceARN " + resourceARN + " is not a CloudWatch resource ARN.")
	}
	return cwAlarmStateKey(account, region, name), nil
}

// cwResourceNotFound is ResourceNotFoundException, "The named resource does not exist", at 404.
func cwResourceNotFound(resourceARN string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The resource " + resourceARN + " does not exist.",
		HTTPStatus: http.StatusNotFound,
	}
}

// resolveTaggedAlarm loads the alarm a ResourceARN names, with the state key it is stored at.
// All three operations resolve through here, so none accepts an ARN another refuses.
func (p *CloudWatchPlugin) resolveTaggedAlarm(ctx context.Context, resourceARN string) (CWAlarm, string, error) {
	stateKey, awsErr := cwParseAlarmTagARN(resourceARN)
	if awsErr != nil {
		return CWAlarm{}, "", awsErr
	}
	alarm, found, err := p.loadAlarm(ctx, stateKey)
	if err != nil {
		return CWAlarm{}, "", err
	}
	if !found {
		return CWAlarm{}, "", cwResourceNotFound(resourceARN)
	}
	return alarm, stateKey, nil
}

// tagResource handles TagResource. "If you specify a new tag key for the alarm, this tag is
// appended to the list of tags associated with the alarm. If you specify a tag key that is
// already associated with the alarm, the new tag value that you specify replaces the previous
// value for that tag", per API_TagResource.
func (p *CloudWatchPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tags, present := cwParseTagList(req.Params, "Tags")
	if !present {
		return nil, cwMissingParameter("Tags")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	alarm, stateKey, err := p.resolveTaggedAlarm(goCtx, req.Params["ResourceARN"])
	if err != nil {
		return nil, err
	}
	merged := mergeStringMap(alarm.Tags, tags, nil)
	if awsErr := cwCheckTags(merged); awsErr != nil {
		return nil, awsErr
	}
	tagsBefore := len(alarm.Tags)
	alarm.Tags = merged
	if err := p.storeAlarm(goCtx, stateKey, alarm); err != nil {
		return nil, err
	}
	if err := cwStampAlarmEverTagged(goCtx, p.state, stateKey, tagsBefore, len(tags)); err != nil {
		return nil, fmt.Errorf("cloudwatch tagResource: %w", err)
	}
	return cwUnitResponse(req, "TagResource", ctx.RequestID)
}

// untagResource handles UntagResource. A key the alarm does not carry is not an error; none of
// the published errors describes one.
func (p *CloudWatchPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	keys := parseMemberList(req.Params, "TagKeys")
	if keys == nil {
		return nil, cwMissingParameter("TagKeys")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	alarm, stateKey, err := p.resolveTaggedAlarm(goCtx, req.Params["ResourceARN"])
	if err != nil {
		return nil, err
	}
	tagsBefore := len(alarm.Tags)
	alarm.Tags = mergeStringMap(alarm.Tags, nil, keys)
	if len(alarm.Tags) == 0 {
		alarm.Tags = nil
	}
	if err := p.storeAlarm(goCtx, stateKey, alarm); err != nil {
		return nil, err
	}
	if err := cwStampAlarmEverTagged(goCtx, p.state, stateKey, tagsBefore, 0); err != nil {
		return nil, fmt.Errorf("cloudwatch untagResource: %w", err)
	}
	return cwUnitResponse(req, "UntagResource", ctx.RequestID)
}

// listTagsForResource handles ListTagsForResource. Tags is always present, and empty for an
// alarm with none, so a caller converging tags compares a list rather than a nil. The tags are in
// key order, which is deterministic; the page publishes no order.
func (p *CloudWatchPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	alarm, _, err := p.resolveTaggedAlarm(context.Background(), req.Params["ResourceARN"])
	if err != nil {
		return nil, err
	}
	return cwRespond(req, "ListTagsForResource", ctx.RequestID, cwDoc{}.with("Tags", cwTagList(alarm.Tags)))
}

// cwTagList renders tags as a list of Tag structures in key order.
func cwTagList(tags map[string]string) cwList {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	list := make(cwList, 0, len(keys))
	for _, k := range keys {
		list = append(list, cwDoc{}.with("Key", k).with("Value", tags[k]))
	}
	return list
}

// ----- Tagging API provider ------------------------------------------------

// CloudWatch answers the Resource Groups Tagging API for its alarms (#1609): GetResources through
// [CloudWatchPlugin.listTaggedResources], and TagResources and UntagResources through
// [CloudWatchPlugin.tagResourceByARN], under [CloudWatchPlugin.mu], the lock every write to an alarm
// holds. A second writer outside that lock could interleave with a PutMetricAlarm, SetAlarmState or
// DeleteAlarms and lose a tag or resurrect a deleted alarm. A refusal CloudWatch's own TagResource
// would answer is carried out as a [taggingQuotaError], so the tagging API reports the owning
// service's own code, as it does for a quota refusal.

// cwAlarmTagStateKey resolves an alarm ARN to the alarm's state key, through [cwParseAlarmTagARN],
// which CloudWatch's own three tagging operations parse through (#1618). A dashboard, metric stream
// or Contributor Insights rule is taggable at AWS and unmodeled here, and is the unsupported-type
// answer like any ARN this cannot key. resource is the ARN's resource portion, for the message.
func cwAlarmTagStateKey(arn, resource string) (string, error) {
	key, arnErr := cwParseAlarmTagARN(arn)
	if arnErr != nil {
		return "", unsupportedTagResource("CloudWatch %q is not an alarm ARN: %s", resource, arnErr.Code)
	}
	return key, nil
}

// cwListTaggedAlarms reports the caller's CloudWatch alarms (#1618), under the ARN the alarm
// record carries — arn:aws:cloudwatch:{region}:{acct}:alarm:{name}, the form CloudWatch's own
// TagResource takes. Whether an alarm is reported follows #938's rule through the tag-history
// side-car ([cwAlarmEverTagged]). A store fault reading either is returned, which
// [TaggingPlugin.scanAllResources] logs and skips.
func cwListTaggedAlarms(_ context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := state.List(goCtx, monitoringNamespace, taggingScanPrefix(cwAlarmKeyPrefix, reqCtx))
	if err != nil {
		return nil, fmt.Errorf("list cloudwatch alarms: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := state.Get(goCtx, monitoringNamespace, k)
		if err != nil {
			return nil, fmt.Errorf("get cloudwatch alarm %s: %w", k, err)
		}
		var alarm CWAlarm
		if raw == nil || json.Unmarshal(raw, &alarm) != nil {
			continue
		}
		everTagged, err := cwAlarmEverTagged(goCtx, state, k)
		if err != nil {
			return nil, err
		}
		out = append(out, resourceTagMapping{
			ResourceARN: alarm.AlarmARN,
			Tags:        mapToTaggingTags(alarm.Tags),
			everTagged:  everTagged,
		})
	}
	return out, nil
}

// listTaggedResources implements [taggableResourceLister].
func (p *CloudWatchPlugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return cwListTaggedAlarms(ctx, p.state, reqCtx)
}

// tagResourceByARN implements [resourceTagger]. CloudWatch's own TagResource publishes no rule
// about the reserved prefix, so an `aws:` key is refused with the tagging API's own answer
// ([taggingReservedKeyRefusal]); the Tag shape's lengths are checked in [cwMergeAlarmTags].
func (p *CloudWatchPlugin) tagResourceByARN(
	_ *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	parts, ok := taggingSplitARN(arn)
	if !ok {
		return taggingResolveFailed(fmt.Errorf("invalid ARN: %q", arn))
	}
	key, err := cwAlarmTagStateKey(arn, parts[5])
	if err != nil {
		return taggingResolveFailed(err)
	}
	if refusal := taggingReservedKeyRefusal(add, remove); refusal != nil {
		return taggingQuotaRefusal(refusal)
	}
	return p.tagRecord(context.Background(), key, add, remove, quota)
}

// tagRecord implements [resourceTagger], through [cwMergeAlarmTags] under the lock CloudWatch's
// own TagResource and UntagResource hold.
func (p *CloudWatchPlugin) tagRecord(
	ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cwMergeAlarmTags(ctx, p.state, key, add, remove, quota)
}

// cwMergeAlarmTags merges add and remove into the alarm stored at key and stamps its tag history,
// with TagResource's and UntagResource's rules. With quota enforced and tags to add, the merged set
// is held to [cwCheckTags], as TagResource holds it. It takes the state rather than the plugin, so
// the tagging plugin's direct path, used when no CloudWatch plugin is registered, runs the same code
// ([mergeResourceTags]); the plugin's own caller holds [CloudWatchPlugin.mu].
//
// A write that adds and removes nothing is refused with the MissingParameter UntagResource answers
// for an absent TagKeys. That is the answer the tagging API has given for an alarm named in a
// TagResources with no tags, or an UntagResources with no keys, since #1618.
func cwMergeAlarmTags(
	ctx context.Context, state StateManager, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	if len(add) == 0 && len(remove) == 0 {
		return taggingQuotaRefusal(cwMissingParameter("TagKeys"))
	}
	raw, err := loadTagRecord(ctx, state, monitoringNamespace, key)
	if err != nil {
		return err
	}
	var alarm CWAlarm
	if err := json.Unmarshal(raw, &alarm); err != nil {
		return fmt.Errorf("tagging: CloudWatch decode alarm: %w", err)
	}
	tagsBefore := len(alarm.Tags)
	merged := mergeStringMap(alarm.Tags, add, remove)
	if quota == enforceTagQuota && len(add) > 0 {
		if awsErr := cwCheckTags(merged); awsErr != nil {
			return taggingQuotaRefusal(awsErr)
		}
	}
	alarm.Tags = merged
	updated, err := json.Marshal(alarm)
	if err != nil {
		return fmt.Errorf("tagging: CloudWatch marshal alarm: %w", err)
	}
	if err := state.Put(ctx, monitoringNamespace, key, updated); err != nil {
		return fmt.Errorf("tagging: CloudWatch store alarm: %w", err)
	}
	if err := cwStampAlarmEverTagged(ctx, state, key, tagsBefore, len(add)); err != nil {
		return fmt.Errorf("tagging: CloudWatch: %w", err)
	}
	return nil
}
