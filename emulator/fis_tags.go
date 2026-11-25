package emulator

// FIS tagging (#1619): TagResource, UntagResource and ListTagsForResource on experiment
// templates and experiments, and the Resource Groups Tagging API provider for both.
//
// An ARN names a template (experiment-template/{id}) or an experiment (experiment/{id}) in the
// account and Region it carries; one in another account or Region, or one naming nothing, is
// ResourceNotFoundException. The model publishes no error for the three operations, so that
// code is the one FIS publishes for a resource it cannot find. A resource holds at most 50 tags
// (TagMap's max), and an `aws:` key is refused, as for every service whose own key rules
// substrate does not model. GetResources reports a template or experiment that carries a tag;
// substrate keeps no record of a resource's past tags, so one whose tags were all removed is not
// reported. The messages are substrate's own.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
)

// fisKeyForARN returns the state key of the template or experiment arn names, scoped to
// accountID and region, and false for an ARN that names neither there.
func fisKeyForARN(arn, accountID, region string) (string, bool) {
	prefix := "arn:aws:fis:" + region + ":" + accountID + ":"
	rest, ok := strings.CutPrefix(arn, prefix)
	if !ok {
		return "", false
	}
	if id, ok := strings.CutPrefix(rest, "experiment-template/"); ok && id != "" && !strings.Contains(id, "/") {
		return fisTemplateKey(accountID, region, id), true
	}
	if id, ok := strings.CutPrefix(rest, "experiment/"); ok && id != "" && !strings.Contains(id, "/") {
		return fisExperimentKey(accountID, region, id), true
	}
	return "", false
}

// fisTagRecord is the tagged part of a template or experiment, and the whole record kept raw so
// a tag write rewrites nothing else.
type fisTagRecord struct {
	key  string
	tmpl *FISTemplate
	exp  *FISExperiment
}

func (r *fisTagRecord) tags() map[string]string {
	if r.tmpl != nil {
		return r.tmpl.Tags
	}
	return r.exp.Tags
}

func (r *fisTagRecord) setTags(tags map[string]string) {
	if r.tmpl != nil {
		r.tmpl.Tags = tags
		return
	}
	r.exp.Tags = tags
}

func (r *fisTagRecord) record() any {
	if r.tmpl != nil {
		return r.tmpl
	}
	return r.exp
}

// loadTagRecord reads the template or experiment at key, nil when absent.
func (p *FISPlugin) loadTagRecord(key string) (*fisTagRecord, error) {
	rec := &fisTagRecord{key: key}
	var target any
	if strings.HasPrefix(key, "template:") {
		rec.tmpl = &FISTemplate{}
		target = rec.tmpl
	} else {
		rec.exp = &FISExperiment{}
		target = rec.exp
	}
	found, err := p.loadJSON(key, target)
	if err != nil || !found {
		return nil, err
	}
	return rec, nil
}

// fisTagQuotaRefusal refuses a tag set larger than the 50-tag ceiling.
func fisTagQuotaRefusal(count int) *AWSError {
	if count <= fisMaxTags {
		return nil
	}
	return fisValidation(fmt.Sprintf("A resource can have at most %d tags; this request would leave %d.", fisMaxTags, count))
}

// mergeTags applies add and remove to the record at key under the caller's lock, refusing a
// result over the quota when quota is enforced.
func (p *FISPlugin) mergeTags(key string, add map[string]string, remove []string, quota tagQuotaMode) error {
	rec, err := p.loadTagRecord(key)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("%w: %s/%s", errTagResourceNotFound, fisNamespace, key)
	}
	merged := maps.Clone(rec.tags())
	if merged == nil {
		merged = map[string]string{}
	}
	maps.Copy(merged, add)
	for _, k := range remove {
		delete(merged, k)
	}
	if quota == enforceTagQuota {
		if refusal := fisTagQuotaRefusal(len(merged)); refusal != nil {
			return taggingQuotaRefusal(refusal)
		}
	}
	if len(merged) == 0 {
		merged = nil
	}
	rec.setTags(merged)
	return p.putJSON(key, rec.record())
}

// requireTagKey resolves a tag operation's ARN, refusing one that names nothing.
func (p *FISPlugin) requireTagKey(ctx *RequestContext, arn string) (string, *AWSError) {
	key, ok := fisKeyForARN(arn, ctx.AccountID, ctx.Region)
	if !ok {
		return "", fisNotFound("Resource " + arn + " not found.")
	}
	rec, err := p.loadTagRecord(key)
	if err != nil || rec == nil {
		return "", fisNotFound("Resource " + arn + " not found.")
	}
	return key, nil
}

// fisTagWriteError maps a merge failure onto the answer TagResource and UntagResource give.
func fisTagWriteError(err error) error {
	var quotaErr *taggingQuotaError
	if errors.As(err, &quotaErr) {
		return quotaErr.awsErr
	}
	return err
}

func (p *FISPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		Tags map[string]string `json:"tags"`
	}
	if awsErr := fisDecodeBody(req, &in); awsErr != nil {
		return nil, awsErr
	}
	if in.Tags == nil {
		return nil, fisRequiredMember("tags")
	}
	if refusal := taggingReservedKeyRefusal(in.Tags, nil); refusal != nil {
		return nil, fisValidation(refusal.Message)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, awsErr := p.requireTagKey(ctx, strings.TrimPrefix(req.Path, "/tags/"))
	if awsErr != nil {
		return nil, awsErr
	}
	if err := p.mergeTags(key, in.Tags, nil, enforceTagQuota); err != nil {
		return nil, fisTagWriteError(err)
	}
	return fisJSONResponse(struct{}{})
}

func (p *FISPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	keys := req.MultiValueParams["tagKeys"]
	if len(keys) == 0 && req.Params["tagKeys"] != "" {
		keys = []string{req.Params["tagKeys"]}
	}
	if refusal := taggingReservedKeyRefusal(nil, keys); refusal != nil {
		return nil, fisValidation(refusal.Message)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key, awsErr := p.requireTagKey(ctx, strings.TrimPrefix(req.Path, "/tags/"))
	if awsErr != nil {
		return nil, awsErr
	}
	if err := p.mergeTags(key, nil, keys, skipTagQuota); err != nil {
		return nil, fisTagWriteError(err)
	}
	return fisJSONResponse(struct{}{})
}

func (p *FISPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	key, awsErr := p.requireTagKey(ctx, strings.TrimPrefix(req.Path, "/tags/"))
	if awsErr != nil {
		return nil, awsErr
	}
	rec, err := p.loadTagRecord(key)
	if err != nil {
		return nil, err
	}
	return fisJSONResponse(map[string]any{"tags": fisNonNil(rec.tags())})
}

// listTaggedResources implements [taggableResourceLister].
func (p *FISPlugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return fisListTaggedResources(ctx, p.state, reqCtx)
}

// fisListTaggedResources reports every tagged template and experiment in the caller's account
// and Region. It reads the state manager directly, so the tagging plugin's direct path (a
// registry without the FIS plugin) and the plugin's lister run the same function.
func fisListTaggedResources(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	for _, prefix := range []string{"template:", "experiment:"} {
		keys, err := state.List(ctx, fisNamespace, taggingScanPrefix(prefix, reqCtx))
		if err != nil {
			return out, fmt.Errorf("list fis %s records: %w", prefix, err)
		}
		for _, key := range keys {
			data, err := state.Get(ctx, fisNamespace, key)
			if err != nil {
				return out, fmt.Errorf("get fis record %s: %w", key, err)
			}
			if data == nil {
				continue
			}
			var rec struct {
				ARN  string            `json:"arn"`
				Tags map[string]string `json:"tags"`
			}
			if err := json.Unmarshal(data, &rec); err != nil {
				return out, fmt.Errorf("decode fis record %s: %w", key, err)
			}
			if len(rec.Tags) == 0 {
				continue
			}
			tags := make([]taggingTag, 0, len(rec.Tags))
			for _, k := range fisSortedKeys(rec.Tags) {
				tags = append(tags, taggingTag{Key: k, Value: rec.Tags[k]})
			}
			out = append(out, resourceTagMapping{ResourceARN: rec.ARN, Tags: tags, everTagged: true})
		}
	}
	return out, nil
}

// tagResourceByARN implements [resourceTagger].
func (p *FISPlugin) tagResourceByARN(
	reqCtx *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	key, ok := fisKeyForARN(arn, reqCtx.AccountID, reqCtx.Region)
	if !ok {
		return taggingResolveFailed(fmt.Errorf("not a FIS experiment template or experiment ARN in this account and Region: %q", arn))
	}
	if refusal := taggingReservedKeyRefusal(add, remove); refusal != nil {
		return taggingQuotaRefusal(refusal)
	}
	return p.tagRecord(context.Background(), key, add, remove, quota)
}

// tagRecord implements [resourceTagger], under the lock FIS's own tag operations hold.
func (p *FISPlugin) tagRecord(_ context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mergeTags(key, add, remove, quota)
}
