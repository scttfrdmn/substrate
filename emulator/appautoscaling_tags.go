package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Only a scalable target carries tags: "you cannot tag a scaling policy or scheduled action"
// (API_TagResource). Its own three tag operations and the Resource Groups Tagging API write through
// [aasMergeTags] under [ApplicationAutoScalingPlugin.mu].

// aasMaxTags is the per-target tag quota. The Tagging AWS Resources user guide gives 50 user tags per
// resource as the limit AWS services apply, and TooManyTagsException is the code TagResource
// publishes for exceeding it.
const aasMaxTags = 50

// aasCheckTags validates the tags a write would leave on a target: each key 1 to 128 characters and
// each value at most 256 (the TagMap shape's constraints), no key with the reserved `aws:` prefix,
// and no more than [aasMaxTags]. The messages are substrate's own.
func aasCheckTags(add map[string]string, existing map[string]string) *AWSError {
	for _, k := range taggingSortedKeys(add) {
		if k == "" || len(k) > 128 {
			return aasValidation("Tag keys must be between 1 and 128 characters long.")
		}
		if len(add[k]) > 256 {
			return aasValidation("Tag values must be no more than 256 characters long.")
		}
		if taggingReservedKey(k) {
			return aasValidation("Tag keys that begin with aws: are reserved for AWS use.")
		}
	}
	if len(mergeStringMap(existing, add, nil)) > aasMaxTags {
		return &AWSError{
			Code:       "TooManyTagsException",
			Message:    fmt.Sprintf("A scalable target can have at most %d tags.", aasMaxTags),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// aasParseTargetARN splits a scalable target ARN into its Region, account and ID, reporting false
// for anything that is not one: the ResourceARN shape's pattern is
// `^arn:.+:application-autoscaling:.+:[0-9]+:scalable-target\/[a-zA-Z0-9-]+$`.
func aasParseTargetARN(arn string) (region, account string, ok bool) {
	parts, isARN := taggingSplitARN(arn)
	if !isARN || parts[2] != appAutoScalingNamespace || parts[3] == "" || parts[4] == "" {
		return "", "", false
	}
	id, found := strings.CutPrefix(parts[5], "scalable-target/")
	if !found || id == "" {
		return "", "", false
	}
	for _, r := range id {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
			return "", "", false
		}
	}
	for _, r := range parts[4] {
		if r < '0' || r > '9' {
			return "", "", false
		}
	}
	return parts[3], parts[4], true
}

// aasTargetKeyForARN resolves a scalable target ARN to its record's state key, or "" when no target
// in the ARN's account and Region carries it.
func aasTargetKeyForARN(ctx context.Context, state StateManager, arn string) (string, error) {
	region, account, ok := aasParseTargetARN(arn)
	if !ok {
		return "", nil
	}
	targets, keys, err := aasListTargets(ctx, state, aasTargetKeyPrefix+account+"/"+region+"/")
	if err != nil {
		return "", err
	}
	for i, t := range targets {
		if t.ScalableTargetARN == arn {
			return keys[i], nil
		}
	}
	return "", nil
}

// resolveTaggedTarget resolves a tag operation's ResourceARN, answering ValidationException for one
// that is not a scalable target ARN and ResourceNotFoundException for one that names no target the
// caller can see.
func (p *ApplicationAutoScalingPlugin) resolveTaggedTarget(ctx *RequestContext, arn string) (string, *AWSError, error) {
	if arn == "" {
		return "", aasValidation("ResourceARN is required."), nil
	}
	region, account, ok := aasParseTargetARN(arn)
	if !ok {
		return "", aasValidation(fmt.Sprintf("Invalid scalable target ARN: %s.", arn)), nil
	}
	if region != ctx.Region || account != ctx.AccountID {
		return "", aasResourceNotFound(arn), nil
	}
	key, err := aasTargetKeyForARN(context.Background(), p.state, arn)
	if err != nil {
		return "", nil, err
	}
	if key == "" {
		return "", aasResourceNotFound(arn), nil
	}
	return key, nil, nil
}

// tagResource handles TagResource.
func (p *ApplicationAutoScalingPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ResourceARN string            `json:"ResourceARN"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Tags == nil {
		return nil, aasValidation("Tags is required.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, awsErr, err := p.resolveTaggedTarget(ctx, in.ResourceARN)
	if err != nil || awsErr != nil {
		return nil, aasErr(awsErr, err)
	}
	if err := aasMergeTags(context.Background(), p.state, key, in.Tags, nil, enforceTagQuota); err != nil {
		return nil, aasUnwrapQuota(err)
	}
	return aasJSON(struct{}{})
}

// untagResource handles UntagResource.
func (p *ApplicationAutoScalingPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	if in.TagKeys == nil {
		return nil, aasValidation("TagKeys is required.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, awsErr, err := p.resolveTaggedTarget(ctx, in.ResourceARN)
	if err != nil || awsErr != nil {
		return nil, aasErr(awsErr, err)
	}
	if err := aasMergeTags(context.Background(), p.state, key, nil, in.TagKeys, enforceTagQuota); err != nil {
		return nil, aasUnwrapQuota(err)
	}
	return aasJSON(struct{}{})
}

// listTagsForResource handles ListTagsForResource. Tags is always present, and empty for a target
// with none.
func (p *ApplicationAutoScalingPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ResourceARN string `json:"ResourceARN"`
	}
	if err := aasDecode(req, &in); err != nil {
		return nil, err
	}
	key, awsErr, err := p.resolveTaggedTarget(ctx, in.ResourceARN)
	if err != nil || awsErr != nil {
		return nil, aasErr(awsErr, err)
	}
	t, found, err := p.loadTarget(key)
	if err != nil {
		return nil, err
	}
	tags := map[string]string{}
	if found {
		for k, v := range t.Tags {
			tags[k] = v
		}
	}
	return aasJSON(struct {
		Tags map[string]string `json:"Tags"`
	}{tags})
}

// aasErr returns the AWS refusal when there is one and the internal error otherwise.
func aasErr(awsErr *AWSError, err error) error {
	if awsErr != nil {
		return awsErr
	}
	return err
}

// aasUnwrapQuota turns a refusal [aasMergeTags] carried out as a [taggingQuotaError] back into the
// AWS error the plugin's own operation answers.
func aasUnwrapQuota(err error) error {
	var q *taggingQuotaError
	if errors.As(err, &q) {
		return q.awsErr
	}
	return err
}

// aasMergeTags merges add and remove into the target at key and stamps its tag history. With quota
// enforced, the added tags are held to [aasCheckTags]; a refusal is carried out as a
// [taggingQuotaError], so the tagging API reports the code the service's own TagResource answers.
// The caller holds [ApplicationAutoScalingPlugin.mu].
func aasMergeTags(ctx context.Context, state StateManager, key string, add map[string]string, remove []string, quota tagQuotaMode) error {
	raw, err := loadTagRecord(ctx, state, appAutoScalingNamespace, key)
	if err != nil {
		return err
	}
	var t aasTarget
	if err := json.Unmarshal(raw, &t); err != nil {
		return fmt.Errorf("tagging: Application Auto Scaling decode target: %w", err)
	}
	if quota == enforceTagQuota {
		if awsErr := aasCheckTags(add, t.Tags); awsErr != nil {
			return taggingQuotaRefusal(awsErr)
		}
	}
	t.EverTagged = taggingEverTagged(t.EverTagged, len(t.Tags), len(add))
	t.Tags = mergeStringMap(t.Tags, add, remove)
	if len(t.Tags) == 0 {
		t.Tags = nil
	}
	return aasStoreTarget(ctx, state, key, &t)
}

// ----- Tagging API provider ------------------------------------------------------------------------

// aasListTaggedTargets reports the caller's scalable targets to the tagging API's GetResources, with
// their tags and their previously-tagged flag.
func aasListTaggedTargets(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	targets, _, err := aasListTargets(ctx, state, taggingScanPrefix(aasTargetKeyPrefix, reqCtx))
	if err != nil {
		return nil, err
	}
	out := make([]resourceTagMapping, 0, len(targets))
	for _, t := range targets {
		out = append(out, resourceTagMapping{
			ResourceARN: t.ScalableTargetARN,
			Tags:        mapToTaggingTags(t.Tags),
			everTagged:  t.EverTagged,
		})
	}
	return out, nil
}

// listTaggedResources implements [taggableResourceLister].
func (p *ApplicationAutoScalingPlugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return aasListTaggedTargets(ctx, p.state, reqCtx)
}

// tagResourceByARN implements [resourceTagger]. An ARN that is not a scalable target is the ARN's
// failure, and so is one naming another account's or Region's target: Application Auto Scaling is
// regional and substrate models no cross-account access. The ARN is resolved to its record under
// [ApplicationAutoScalingPlugin.mu], the lock a deregistration holds, so the record written is the
// one the ARN named.
func (p *ApplicationAutoScalingPlugin) tagResourceByARN(
	reqCtx *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	region, account, ok := aasParseTargetARN(arn)
	if !ok {
		return taggingResolveFailed(unsupportedTagResource("Application Auto Scaling %q is not a scalable target ARN", arn))
	}
	if region != reqCtx.Region || account != reqCtx.AccountID {
		return taggingResolveFailed(fmt.Errorf("%w: %s is outside the caller's account and Region", errTagResourceNotFound, arn))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, err := aasTargetKeyForARN(context.Background(), p.state, arn)
	if err != nil {
		return err
	}
	if key == "" {
		return taggingResolveFailed(fmt.Errorf("%w: %s", errTagResourceNotFound, arn))
	}
	return aasMergeTags(context.Background(), p.state, key, add, remove, quota)
}

// tagRecord implements [resourceTagger], under the lock the plugin's own tag operations hold.
func (p *ApplicationAutoScalingPlugin) tagRecord(
	ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return aasMergeTags(ctx, p.state, key, add, remove, quota)
}
