package emulator

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// The plugins that own tagged resources answer the Resource Groups Tagging API for them through
// two optional interfaces (#1609). A plugin that implements them reads and writes its own records,
// under its own lock. The tagging plugin keeps a direct path to every other service's state, so a
// service that has not been converted, or a registry that lacks its plugin, answers as before.
//
// Discovery follows [PluginRegistry.ResetPlugins] and [PluginRegistry.RunClockDriven]: the
// registry is asked under its read lock which plugins assert to the interface, and the lock is
// released before any of them is called.

// taggableResourceLister is implemented by a plugin that reports its own taggable resources to
// GetResources. listTaggedResources answers every resource of every taggable type the plugin owns
// in the caller's account and Region, with its tags and its previously-tagged flag. It may return
// the resources it could read alongside an error for the ones it could not; the caller logs the
// error and still reports the readable ones. Scope and the never-tagged rule are applied by the
// caller, as for every scan ([TaggingPlugin.scanAllResources]).
type taggableResourceLister interface {
	listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error)
}

// resourceTagger is implemented by a plugin that applies tag writes to its own resources.
//
// tagResourceByARN serves TagResources and UntagResources: it resolves arn to one of the plugin's
// records and merges add and remove into it. An ARN that names nothing the plugin can tag is
// reported through [taggingResolveFailed], so the tagging API answers it as an ARN failure rather
// than a write failure.
//
// tagRecord merges into the record at key in the plugin's own namespace. It is what
// [mergeResourceTags] calls, so CloudFormation's stamps reach the same writer.
//
// Both run under the lock the plugin's own tag operations hold, so a tag write through the tagging
// API and one through the service's own tag operation are serialized with each other. The lock
// orders tag writes only: a service operation that rewrites the same record without taking it is
// not ordered against them.
//
// tagResourceByARN serves callers, so it applies the owner's key rules to what it is asked to add
// and remove, and those refuse an `aws:` key. tagRecord applies none, because its callers are
// internal and stamp `aws:cloudformation:*`. quota says whether the service's published
// per-resource tag quota is enforced ([taggingCheckTagQuota]). A record that is not there is
// [errTagResourceNotFound], and a refusal the owner would answer is a [taggingQuotaError].
type resourceTagger interface {
	tagResourceByARN(reqCtx *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode) error
	tagRecord(ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode) error
}

// taggingResolveError marks a [resourceTagger.tagResourceByARN] failure as the ARN's: the
// tagging API maps it through [TaggingPlugin.tagResolveFailure], as it maps a
// [TaggingPlugin.resolveARN] error, rather than through [TaggingPlugin.tagMergeFailure].
type taggingResolveError struct {
	err error
}

// Error implements error.
func (e *taggingResolveError) Error() string { return e.err.Error() }

// taggingResolveFailed wraps err as a [taggingResolveError].
func taggingResolveFailed(err error) error {
	return &taggingResolveError{err: err}
}

// taggingReservedTagPrefix is the prefix of tag keys AWS reserves: "System created tags that begin
// with aws: are reserved for AWS use ... You can't edit or delete a tag that begins with the aws:
// prefix" (Tagging AWS Resources, Tag naming limits and requirements). It is EC2's
// [ec2ReservedTagPrefix], whose doc records the observed refusal.
const taggingReservedTagPrefix = ec2ReservedTagPrefix

// taggingReservedKey reports whether key carries the reserved prefix. The match is case-sensitive,
// as the EC2 User Guide's Tag restrictions give it: "Tag keys and values are case-sensitive" and
// "The aws: prefix is reserved for AWS use". S3's tagging pages defer to that list ("For basic tag
// restrictions, see Tag restrictions in the Amazon EC2 User Guide", Tagging your objects), and
// observed EC2 accepts `AWS:foo` as an ordinary tag ([ec2ReservedTagPrefix]). ECS's Tag shape
// reserves "aws:, AWS:, or any upper or lowercase combination", which is ECS's own rule and not
// applied here. EC2's, S3's and the tagging API's checks all call this, so they cannot disagree.
func taggingReservedKey(key string) bool {
	return strings.HasPrefix(key, taggingReservedTagPrefix)
}

// taggingReservedKeyRefusal refuses a caller's write that adds or removes an `aws:` key, for a
// service whose own key rules substrate does not model. The tagging API reports it for the resource
// as InvalidParameterException at 400, the code its page gives for "a provided parameter value is
// out of range". The message is substrate's own.
func taggingReservedKeyRefusal(add map[string]string, remove []string) *AWSError {
	reserved := false
	for k := range add {
		reserved = reserved || taggingReservedKey(k)
	}
	for _, k := range remove {
		reserved = reserved || taggingReservedKey(k)
	}
	if !reserved {
		return nil
	}
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "Tag keys that begin with aws: are reserved for AWS use.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// taggingSortedKeys returns m's keys in order, so a check that stops at the first bad tag names the
// same one on every run.
func taggingSortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// taggingARNOwner returns the name of the plugin that owns resources whose ARN carries service as
// its service segment. They are the same for every service but CloudWatch, whose ARNs say
// "cloudwatch" and whose plugin is "monitoring".
func taggingARNOwner(service string) string {
	if service == "cloudwatch" {
		return monitoringNamespace
	}
	return service
}

// taggingSplitARN splits arn into its six segments, reporting false when it is not an ARN.
func taggingSplitARN(arn string) ([]string, bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return nil, false
	}
	return parts, true
}

// taggableResourceListers returns every registered [taggableResourceLister], by plugin name. A nil
// registry has none.
func (r *PluginRegistry) taggableResourceListers() map[string]taggableResourceLister {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	listers := make(map[string]taggableResourceLister)
	for name, p := range r.plugins {
		if l, ok := p.(taggableResourceLister); ok {
			listers[name] = l
		}
	}
	return listers
}

// resourceTagger returns the plugin registered under name when it is a [resourceTagger]. A nil
// registry has none.
func (r *PluginRegistry) resourceTagger(name string) (resourceTagger, bool) {
	if r == nil {
		return nil, false
	}
	p, ok := r.Plugin(name)
	if !ok {
		return nil, false
	}
	t, ok := p.(resourceTagger)
	return t, ok
}

// taggingProviderNames returns the sorted names of the registered plugins that implement each
// interface.
func (r *PluginRegistry) taggingProviderNames() (listers, taggers []string) {
	for name := range r.taggableResourceListers() {
		listers = append(listers, name)
	}
	sort.Strings(listers)
	if r == nil {
		return listers, nil
	}
	for _, name := range r.Names() {
		if _, ok := r.resourceTagger(name); ok {
			taggers = append(taggers, name)
		}
	}
	return listers, taggers
}

// loadTagRecord reads the record a tag write merges into, reporting [errTagResourceNotFound] when
// there is none.
func loadTagRecord(ctx context.Context, state StateManager, ns, key string) ([]byte, error) {
	raw, err := state.Get(ctx, ns, key)
	if err != nil {
		return nil, fmt.Errorf("get resource: %w", err)
	}
	if raw == nil {
		return nil, fmt.Errorf("%w: %s/%s", errTagResourceNotFound, ns, key)
	}
	return raw, nil
}

// taggingStateScan adapts a scan over a state manager to the shape [resourceDescriptor] holds, so
// the tagging plugin's direct path and the owning plugin's lister run the same function.
func (p *TaggingPlugin) taggingStateScan(
	scan func(context.Context, StateManager, *RequestContext) ([]resourceTagMapping, error),
) func(context.Context, *RequestContext) ([]resourceTagMapping, error) {
	return func(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
		return scan(ctx, p.state, reqCtx)
	}
}
