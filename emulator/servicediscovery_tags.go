package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// Cloud Map tags namespaces and services. Its own three tag operations and the Resource Groups
// Tagging API write through [sdMergeTags] under [ServiceDiscoveryPlugin.mu].

// sdMaxTags is the per-resource tag quota: Cloud Map's quotas page gives 50 tags per resource, and
// TooManyTagsException is the code TagResource publishes for exceeding it.
const sdMaxTags = 50

// sdTagMap converts a Tags list to a map and validates the set it would leave on a resource with
// existing tags: each key 1 to 128 characters and each value at most 256 (the Tag shape), no
// reserved `aws:` key, and at most [sdMaxTags]. The messages are substrate's own.
func sdTagMap(tags []taggingTag, existing map[string]string) (map[string]string, *AWSError) {
	add := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key == "" || len(t.Key) > 128 {
			return nil, sdInvalid("Tag keys must be between 1 and 128 characters long.")
		}
		if len(t.Value) > 256 {
			return nil, sdInvalid("Tag values must be no more than 256 characters long.")
		}
		if taggingReservedKey(t.Key) {
			return nil, sdInvalid("Tag keys that begin with aws: are reserved for AWS use.")
		}
		add[t.Key] = t.Value
	}
	if awsErr := sdCheckTagCount(add, existing); awsErr != nil {
		return nil, awsErr
	}
	if len(add) == 0 {
		return nil, nil
	}
	return add, nil
}

// sdCheckTagCount refuses a write that would leave more than [sdMaxTags] tags.
func sdCheckTagCount(add, existing map[string]string) *AWSError {
	if len(mergeStringMap(existing, add, nil)) > sdMaxTags {
		return sdError("TooManyTagsException", http.StatusBadRequest, "A resource can have at most %d tags.", sdMaxTags)
	}
	return nil
}

// sdTagList renders tags as a Tag list in key order.
func sdTagList(tags map[string]string) []taggingTag {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]taggingTag, 0, len(keys))
	for _, k := range keys {
		out = append(out, taggingTag{Key: k, Value: tags[k]})
	}
	return out
}

// sdTagKeyForARN resolves a namespace or service ARN to its record's state key, reporting ok false
// for an ARN that is neither.
func sdTagKeyForARN(arn string) (string, bool) {
	parts, isARN := taggingSplitARN(arn)
	if !isARN || parts[2] != serviceDiscoveryNamespace || parts[3] == "" || parts[4] == "" {
		return "", false
	}
	scope := parts[4] + "/" + parts[3] + "/"
	if id, ok := strings.CutPrefix(parts[5], "namespace/"); ok && id != "" {
		return sdNamespaceKeyPrefix + scope + id, true
	}
	if id, ok := strings.CutPrefix(parts[5], "service/"); ok && id != "" {
		return sdServiceKeyPrefix + scope + id, true
	}
	return "", false
}

// sdTaggedRecord is the part of a namespace or service record a tag write reads and writes; the
// rest of the record is kept as it was.
type sdTaggedRecord struct {
	raw        map[string]json.RawMessage
	tags       map[string]string
	everTagged bool
}

// sdLoadTagged reads the record at key for a tag write.
func sdLoadTagged(ctx context.Context, state StateManager, key string) (*sdTaggedRecord, error) {
	raw, err := loadTagRecord(ctx, state, serviceDiscoveryNamespace, key)
	if err != nil {
		return nil, err
	}
	rec := &sdTaggedRecord{}
	if err := json.Unmarshal(raw, &rec.raw); err != nil {
		return nil, fmt.Errorf("tagging: Cloud Map decode record: %w", err)
	}
	if v, ok := rec.raw["Tags"]; ok {
		if err := json.Unmarshal(v, &rec.tags); err != nil {
			return nil, fmt.Errorf("tagging: Cloud Map decode tags: %w", err)
		}
	}
	if v, ok := rec.raw[taggingEverTaggedJSONMember]; ok {
		if err := json.Unmarshal(v, &rec.everTagged); err != nil {
			return nil, fmt.Errorf("tagging: Cloud Map decode tag history: %w", err)
		}
	}
	return rec, nil
}

// sdMergeTags merges add and remove into the record at key and stamps its tag history. With quota
// enforced the merged set is held to [sdCheckTagCount], carried out as a [taggingQuotaError]. The
// caller holds [ServiceDiscoveryPlugin.mu].
func sdMergeTags(ctx context.Context, state StateManager, key string, add map[string]string, remove []string, quota tagQuotaMode) error {
	rec, err := sdLoadTagged(ctx, state, key)
	if err != nil {
		return err
	}
	if quota == enforceTagQuota {
		if awsErr := sdCheckTagCount(add, rec.tags); awsErr != nil {
			return taggingQuotaRefusal(awsErr)
		}
	}
	everTagged := taggingEverTagged(rec.everTagged, len(rec.tags), len(add))
	merged := mergeStringMap(rec.tags, add, remove)
	if len(merged) == 0 {
		delete(rec.raw, "Tags")
	} else {
		data, err := json.Marshal(merged)
		if err != nil {
			return fmt.Errorf("tagging: Cloud Map encode tags: %w", err)
		}
		rec.raw["Tags"] = data
	}
	if everTagged {
		rec.raw[taggingEverTaggedJSONMember] = json.RawMessage("true")
	}
	return sdPut(ctx, state, key, rec.raw)
}

// resolveTagged resolves a tag operation's ResourceARN: InvalidInput for an ARN that names no
// namespace or service, ResourceNotFoundException (404) for one the caller has no record of.
func (p *ServiceDiscoveryPlugin) resolveTagged(ctx *RequestContext, arn string) (string, error) {
	if arn == "" {
		return "", sdInvalid("ResourceARN is required.")
	}
	key, ok := sdTagKeyForARN(arn)
	if !ok {
		return "", sdInvalid("Invalid resource ARN: %s.", arn)
	}
	if !strings.Contains(key, sdScope(ctx)) {
		return "", sdError("ResourceNotFoundException", http.StatusNotFound, "Resource %s not found.", arn)
	}
	raw, err := p.state.Get(context.Background(), serviceDiscoveryNamespace, key)
	if err != nil {
		return "", fmt.Errorf("servicediscovery get %s: %w", key, err)
	}
	if raw == nil {
		return "", sdError("ResourceNotFoundException", http.StatusNotFound, "Resource %s not found.", arn)
	}
	return key, nil
}

// sdUnwrapQuota turns a refusal carried out of [sdMergeTags] back into the AWS error.
func sdUnwrapQuota(err error) error {
	var q *taggingQuotaError
	if errors.As(err, &q) {
		return q.awsErr
	}
	return err
}

// tagResource handles TagResource.
func (p *ServiceDiscoveryPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ResourceARN string       `json:"ResourceARN"`
		Tags        []taggingTag `json:"Tags"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Tags == nil {
		return nil, sdInvalid("Tags is required.")
	}
	add, awsErr := sdTagMap(in.Tags, nil)
	if awsErr != nil {
		return nil, awsErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, err := p.resolveTagged(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}
	if err := sdMergeTags(context.Background(), p.state, key, add, nil, enforceTagQuota); err != nil {
		return nil, sdUnwrapQuota(err)
	}
	return sdJSON(struct{}{})
}

// untagResource handles UntagResource.
func (p *ServiceDiscoveryPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.TagKeys == nil {
		return nil, sdInvalid("TagKeys is required.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, err := p.resolveTagged(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}
	if err := sdMergeTags(context.Background(), p.state, key, nil, in.TagKeys, enforceTagQuota); err != nil {
		return nil, sdUnwrapQuota(err)
	}
	return sdJSON(struct{}{})
}

// listTagsForResource handles ListTagsForResource. Tags is always present, in key order.
func (p *ServiceDiscoveryPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ResourceARN string `json:"ResourceARN"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	key, err := p.resolveTagged(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}
	rec, err := sdLoadTagged(context.Background(), p.state, key)
	if err != nil {
		return nil, err
	}
	return sdJSON(struct {
		Tags []taggingTag `json:"Tags"`
	}{sdTagList(rec.tags)})
}

// ----- Tagging API provider ------------------------------------------------------------------------

// sdListTaggedResources reports the caller's namespaces and services to the tagging API's
// GetResources, with their tags and their previously-tagged flag.
func sdListTaggedResources(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	for _, prefix := range []string{sdNamespaceKeyPrefix, sdServiceKeyPrefix} {
		err := sdList(ctx, state, taggingScanPrefix(prefix, reqCtx), func(_ string, raw []byte) error {
			var rec struct {
				ARN        string            `json:"Arn"`
				Tags       map[string]string `json:"Tags"`
				EverTagged bool              `json:"ever_tagged"`
			}
			if err := json.Unmarshal(raw, &rec); err != nil {
				return fmt.Errorf("servicediscovery decode tagged record: %w", err)
			}
			out = append(out, resourceTagMapping{ResourceARN: rec.ARN, Tags: mapToTaggingTags(rec.Tags), everTagged: rec.EverTagged})
			return nil
		})
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// listTaggedResources implements [taggableResourceLister].
func (p *ServiceDiscoveryPlugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return sdListTaggedResources(ctx, p.state, reqCtx)
}

// tagResourceByARN implements [resourceTagger]. An ARN that names no namespace or service is the
// ARN's failure; the reserved-prefix refusal is the tagging API's own ([taggingReservedKeyRefusal]).
func (p *ServiceDiscoveryPlugin) tagResourceByARN(
	_ *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	key, ok := sdTagKeyForARN(arn)
	if !ok {
		return taggingResolveFailed(unsupportedTagResource("Cloud Map %q is not a namespace or service ARN", arn))
	}
	if refusal := taggingReservedKeyRefusal(add, remove); refusal != nil {
		return taggingQuotaRefusal(refusal)
	}
	return p.tagRecord(context.Background(), key, add, remove, quota)
}

// tagRecord implements [resourceTagger], under the lock the plugin's own tag operations hold.
func (p *ServiceDiscoveryPlugin) tagRecord(
	ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return sdMergeTags(ctx, p.state, key, add, remove, quota)
}
