package emulator

import (
	"context"
	"encoding/json"
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
// `listener-rule/app/<lb>/<lb-id>/<listener-id>/<rule-id>` — and since #774 that is also the
// shape [elbListenerARN] and [elbRuleARN] mint, so authorization and resolution read one spelling
// rather than two. Substrate nested the child under the load balancer's own ARN before that;
// [elbResourceKindFromARN] still recognizes the nested form, because recorded state and exported
// fixtures written by an earlier version carry it.
const (
	elbKindLoadBalancer = "loadbalancer"
	elbKindTargetGroup  = "targetgroup"
	elbKindListener     = "listener"
	elbKindRule         = "listener-rule"
)

// elbMaxTagsPerResource is the number of user tags **ELBv2** allows on one resource, and
// elbClassicMaxTagsPerResource is the Classic Load Balancing API's own, lower number.
//
// The two generations publish their caps in different places, and that asymmetry is the reason
// these are two constants rather than one:
//
//   - The ELBv2 50 is **not on the API reference**. `API_AddTags` (2015-12-01) states no maximum
//     anywhere — not in its description, not as an `Array Members` constraint on `Tags` (which
//     carries only "Minimum number of 1 item"), not in its Errors section beyond naming
//     `TooManyTags`. The number comes from the ELB tagging documentation's restrictions:
//     "Maximum number of tags per resource—50". So it is a user-guide reading, and a reader
//     looking for it on the operation page will not find it.
//   - The classic 10 **is** on the API reference, in the first sentence of `API_AddTags`
//     (2012-06-01): "Adds the specified tags to the specified load balancer. Each load balancer
//     can have a maximum of 10 tags."
//
// Tags carrying [elbReservedTagPrefix] are excluded from both counts, per the same restrictions
// list — "Tags with this prefix do not count against your tags per resource limit" — which is
// byte-for-byte the rule [ec2CheckTagLimit] already implements. The classic page publishes no
// reserved-prefix rule of its own, and the restrictions list is written for the service rather
// than for one generation, so the exclusion is applied to both rather than to ELBv2 alone.
//
// Substrate held only the 50 until #1148, and #844's Tier 1a made that reachable: a classic load
// balancer became a record, and the Resource Groups Tagging API tags one. So `TagResources` on a
// classic ARN accepted an 11th tag, and a 50th, where AWS refuses the 11th.
const (
	elbMaxTagsPerResource        = 50
	elbClassicMaxTagsPerResource = 10
)

// elbTagQuota is one generation's per-resource tag cap together with the `TooManyTags` wording its
// own page publishes.
//
// The two travel as one value so they cannot drift: a cap without its refusal is how substrate came
// to enforce ELBv2's number behind classic's code path in the first place. Resolved from the
// record's own kind — never from the caller — so the Resource Groups Tagging API, ELBv2's tag doors
// and the classic tag trio #844 Tier 1b will route cannot disagree about one load balancer.
type elbTagQuota struct {
	max     int
	message string
}

// tooManyTags builds the refusal for a resource already at the quota.
//
// `TooManyTags` at HTTP 400 is published on both generations' `AddTags` pages and, for ELBv2, on all
// four creates; classic `CreateLoadBalancer` lists it too. The message differs between the pages and
// each generation answers its own wording, which is the convention every other refusal in this file
// follows.
func (q elbTagQuota) tooManyTags() *AWSError {
	return &AWSError{Code: "TooManyTags", Message: q.message, HTTPStatus: http.StatusBadRequest}
}

// elbV2TagQuota is the quota the four ELBv2 taggable kinds share.
func elbV2TagQuota() elbTagQuota {
	return elbTagQuota{
		max:     elbMaxTagsPerResource,
		message: "You've reached the limit on the number of tags for this resource.",
	}
}

// elbClassicTagQuota is the Classic Load Balancing API's own.
//
// The message is the `TooManyTags` text on the 2012-06-01 `AddTags` page — "The quota for the number
// of tags that can be assigned to a load balancer has been reached." — which words the same refusal
// differently from ELBv2's, so a consumer reading the message sees which generation refused it.
func elbClassicTagQuota() elbTagQuota {
	return elbTagQuota{
		max:     elbClassicMaxTagsPerResource,
		message: "The quota for the number of tags that can be assigned to a load balancer has been reached.",
	}
}

// elbTagQuotaForKind returns the quota that applies to a resource kind.
func elbTagQuotaForKind(kind string) elbTagQuota {
	if kind == elbKindClassicLB {
		return elbClassicTagQuota()
	}
	return elbV2TagQuota()
}

// elbTagQuotaForStateKey returns the quota that applies to the record a state key in the elb
// namespace names.
//
// Only the classic prefix is tested, because the four ELBv2 prefixes share one cap and a key
// matching none of the five has already been turned away by [elbKeyIsTaggable] at every caller.
// Keying on the stored record rather than on the request is what makes the cap the *resource's*
// and not the API door's — a classic load balancer reached through the generation-agnostic tagging
// API gets 10 because of what it is.
func elbTagQuotaForStateKey(key string) elbTagQuota {
	if strings.HasPrefix(key, elbClassicLBKeyPrefix) {
		return elbClassicTagQuota()
	}
	return elbV2TagQuota()
}

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

// elbNotFoundCodes maps a resource kind to the code the tagging operations answer when
// an ARN of that kind names nothing.
//
// All four appear on AddTags, RemoveTags and DescribeTags alike, each at HTTP 400 — a
// *400*, not the 404 a reader expects, which is the ELB API's own choice and the reason
// this table exists rather than one shared NotFound. TrustStoreNotFound is the fifth
// code those operations list and has no row here because substrate models no trust
// store, so no ARN can ever name one.
// The classic row is the same code at the same status, from the classic API's own pages: 2012-06-01
// `AddTags`, `RemoveTags` and `DescribeTags` each publish `LoadBalancerNotFound` at 400, and that is
// the code the Resource Groups Tagging API's classic arm turns into
// [errTagResourceNotFound] (#844). Nothing in ELBv2's three tag operations ever reaches it — they
// refuse a classic ARN before resolving it, see [elbResolveTaggedResource].
var elbNotFoundCodes = map[string]string{
	elbKindLoadBalancer: "LoadBalancerNotFound",
	elbKindTargetGroup:  "TargetGroupNotFound",
	elbKindListener:     "ListenerNotFound",
	elbKindRule:         "RuleNotFound",
	elbKindClassicLB:    "LoadBalancerNotFound",
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
// # Arity, not substring
//
// The classification is on how many `/`-separated segments follow the resource type, because that
// is the only thing distinguishing the two ELB generations: the vendored reference
// (`authzref/elasticloadbalancing.json`) publishes a classic load balancer as
// `…:loadbalancer/${LoadBalancerName}` — one segment — and an ELBv2 one as
// `…:loadbalancer/app/${LoadBalancerName}/${LoadBalancerId}` — three. A `strings.Contains(arn,
// ":loadbalancer/")` test, which is what this did before #863, matches both, so a classic ARN was
// classified as an ELBv2 load balancer and scanned against the `lb:` prefix where only ELBv2
// records live. When #863 made this change nothing wrote a classic record, so the substring test
// resolved to nothing rather than to the wrong record — but it answered `LoadBalancerNotFound`,
// which tells a caller a load balancer of that ARN could exist. #844 now writes classic records,
// under [elbClassicLBKeyPrefix] rather than `lb:` precisely so that the collision the substring test
// was one writer away from cannot happen: a classic `web` and an ELBv2 `web` are two resources and
// they live under two keys. This is [elbChildARN]'s `wantSegments` check applied to the read side.
//
// # This classifier stays ELBv2-only, and that is caller-scoped rather than incidental
//
// A classic ARN is *unclassified* here, and every caller of this function refuses one:
// [elbResolveTaggedResource] answers `ValidationError`, which is what an ELBv2 operation can
// honestly say about an ARN no ELBv2 resource type has. **That code is substrate's reading** —
// AddTags, RemoveTags and DescribeTags publish four `*NotFound` codes and no code for an ARN of the
// wrong generation — chosen because the alternative asserts the resource merely does not exist yet.
//
// That refusal is not a gap left by #844; it is what ELBv2 publishes. Its `AddTags` enumerates its
// taggable resources verbatim — "You can tag your Application Load Balancers, Network Load
// Balancers, Gateway Load Balancers, target groups, trust stores, listeners, and rules" — and a
// Classic Load Balancer is absent from the list. So a classic ARN handed to an ELBv2 tag operation
// is a caller mistake whichever generation substrate models.
//
// The generation-blind callers — the Resource Groups Tagging API and CloudFormation's two tag
// writers — go through [elbAnyGenerationKindFromARN] and [elbResolveAnyGenerationTaggedResource]
// instead. The split is the whole of why there are two classifiers: IAM and the tagging API are one
// surface across both generations (the vendored Service Authorization Reference lists the classic
// `loadbalancer` resource under the single `AddTags` *action*, because one IAM action spans both
// APIs), while the ELBv2 *API* is not.
//
// # Both nested and flat, and why the order is the arity
//
// The nested shapes substrate minted before #774 —
// `…:loadbalancer/app/<name>/<id>/listener/<suffix>` and that plus `/rule/<suffix>` — carry the
// `loadbalancer` resource type and five and seven segments respectively, so counting segments
// separates them from an ELBv2 load balancer's three without needing an ordered chain of
// `strings.Contains` tests to run first. The flat `…:listener/…` and `…:listener-rule/…` that #774
// mints are AWS's own and are matched on the resource type with their own arity.
//
// Keeping the nested form resolvable is deliberate: an event log recorded by an earlier version,
// or a fixture exported from one, holds listener and rule ARNs of that shape, and a replay whose
// tagging calls suddenly named nothing would be a regression in the one property the event store
// exists to provide. New ARNs are never minted in it — see [elbRuleARN].
func elbResourceKindFromARN(arn string) string {
	_, resource, ok := elbARNResource(arn)
	if !ok {
		return ""
	}
	resourceType, rest, ok := strings.Cut(resource, "/")
	if !ok {
		return ""
	}
	segments := strings.Count(rest, "/") + 1
	switch resourceType {
	case elbKindLoadBalancer:
		switch segments {
		case 3: // app/<name>/<id> — an ELBv2 load balancer. A classic one has 1 and is refused.
			return elbKindLoadBalancer
		case 5: // …/<id>/listener/<suffix> — the pre-#774 nested listener.
			return elbKindListener
		case 7: // …/listener/<suffix>/rule/<suffix> — the pre-#774 nested rule.
			return elbKindRule
		}
	case elbKindTargetGroup:
		if segments == 2 { // <name>/<id>
			return elbKindTargetGroup
		}
	case elbKindListener:
		if segments == 4 { // app/<name>/<id>/<listenerid>
			return elbKindListener
		}
	case elbKindRule:
		if segments == 5 { // app/<name>/<id>/<listenerid>/<ruleid>
			return elbKindRule
		}
	}
	return ""
}

// elbAnyGenerationKindFromARN reports which kind an ARN names across **both** ELB generations,
// adding [elbKindClassicLB] to the four [elbResourceKindFromARN] classifies.
//
// This is the classifier for the surfaces that are one surface across both APIs: the Resource Groups
// Tagging API, whose documented rule is that `ResourceTypeFilters` matches the type segment embedded
// in the ARN — which both generations spell `loadbalancer` — and CloudFormation's two tag writers,
// which stamp whatever resource a template deployed. Neither has an ELB generation; a caller holding
// a classic ARN there is not making a mistake about which API it is talking to.
//
// ELBv2's own three tag operations deliberately do not use it. [elbResourceKindFromARN] carries the
// argument for that split, and it is the reason this is a second function rather than a widening of
// the first: widening it would have moved the refusal out of ELBv2's tag doors, where AWS publishes
// it.
func elbAnyGenerationKindFromARN(arn string) string {
	if kind := elbResourceKindFromARN(arn); kind != "" {
		return kind
	}
	if elbClassicNameFromARN(arn) != "" {
		return elbKindClassicLB
	}
	return ""
}

// The state-key prefixes the four taggable ELBv2 records are stored under.
//
// They are constants rather than literals at each site because three readers outside ELB's own
// plugin now depend on them: [elbStateKeyPrefix] builds a scan prefix from one,
// [elbKeyIsTaggable] tests a key against all four, and the Resource Groups Tagging API's four ELB
// scanners narrow their `List` by one each (#863). A namespace whose prefixes are spelled in one
// place cannot have a scanner and a writer disagree about where a record lives, which is the
// failure #935 records for ECS.
//
// Each ends in a colon, which is load-bearing for the same reason it is in six other namespaces:
// this one also holds `lb_names:`, `tg_names:`, `listener_ids:` and `rule_ids:` index keys whose
// values are JSON arrays of names, and a bare `lb` or `listener` prefix would list them.
const (
	elbLBKeyPrefix       = "lb:"
	elbTGKeyPrefix       = "tg:"
	elbListenerKeyPrefix = "listener:"
	elbRuleKeyPrefix     = "rule:"
)

// elbKindKeyPrefix returns the bare state-key prefix records of a kind are stored under, or ""
// for a kind that names nothing substrate stores.
func elbKindKeyPrefix(kind string) string {
	switch kind {
	case elbKindLoadBalancer:
		return elbLBKeyPrefix
	case elbKindTargetGroup:
		return elbTGKeyPrefix
	case elbKindListener:
		return elbListenerKeyPrefix
	case elbKindRule:
		return elbRuleKeyPrefix
	case elbKindClassicLB:
		return elbClassicLBKeyPrefix
	default:
		return ""
	}
}

// elbStateKeyPrefix returns the state-key prefix records of a kind are stored under, narrowed to
// one account and Region.
func elbStateKeyPrefix(kind, scope string) string {
	prefix := elbKindKeyPrefix(kind)
	if prefix == "" {
		return ""
	}
	return prefix + scope + "/"
}

// elbTagsJSONMember is the JSON member all four taggable ELBv2 records store their tags in.
//
// [ELBLoadBalancer], [ELBTargetGroup], [ELBListener] and [ELBRule] all declare
// `json:"Tags,omitempty"` (elb_types.go), which is why one raw-JSON merge serves the whole
// namespace where rds, states and ecs each needed a per-kind decode. It is named rather than
// written inline because [mergeRecordTagListTags] writes whichever member it is given, and a
// misspelling would add a second tags member while leaving the real one untouched — a tag call that
// answers 200 and stores nothing.
const elbTagsJSONMember = "Tags"

// elbTagKeyField and elbTagValueField are the member names one element of that array uses, from
// [ELBTag]'s own `json:"Key"` and `json:"Value"`. ELB spells them the ordinary way, as SNS,
// Secrets Manager and Systems Manager do and unlike KMS's TagKey/TagValue.
const (
	elbTagKeyField   = "Key"
	elbTagValueField = "Value"
)

// elbKeyIsTaggable reports whether a state key in the elb namespace names a record that stores
// tags.
//
// It guards [mergeResourceTags]' elb arm, which edits whatever record it is handed as raw JSON.
// The namespace also holds four index keys whose values are JSON arrays of names, and a merge
// against one of those would leave an array looking like a record — the failure the same guard
// prevents for kms, sns, secretsmanager, ssm, rds and acm. Every one of the four taggable kinds
// spells its tags member the same way, so unlike those six namespaces the guard is the only
// per-kind discrimination this arm needs. [ELBClassicLoadBalancer] spells it the same way too, which
// is why #844's fifth prefix is one more entry here rather than a second guard.
func elbKeyIsTaggable(key string) bool {
	for _, prefix := range []string{
		elbLBKeyPrefix, elbTGKeyPrefix, elbListenerKeyPrefix, elbRuleKeyPrefix,
		elbClassicLBKeyPrefix,
	} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// elbARNScope returns the `{account}/{region}` scope an ELB ARN's own segments name, and reports
// whether the ARN carried the six fields an ARN has.
//
// It exists so the Resource Groups Tagging API's ELB arm reads the scope from the ARN rather than
// from the caller's request context, which is #826's rule and the one every resolver arm follows:
// an ARN naming another account's load balancer must resolve that account's record or none.
func elbARNScope(arn string) (string, bool) {
	const arnFields = 6
	parts := strings.SplitN(arn, ":", arnFields)
	if len(parts) < arnFields || parts[0] != "arn" {
		return "", false
	}
	return parts[4] + "/" + parts[3], true
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

	// everTagged is the record's previously-tagged flag as stored; see [taggingEverTagged].
	everTagged bool

	// withTags re-marshals the record carrying a new tag set and a new previously-tagged
	// flag. It closes over the decoded record, which is what keeps every other member
	// intact: rebuilding the record from the ARN would drop whatever the caller set at
	// create time. Callers go through [elbTaggedResource.encode] rather than calling this
	// directly, so the flag is computed in one place.
	withTags func(tags []ELBTag, everTagged bool) ([]byte, error)
}

// encode re-marshals the record with a new tag set, stamping the previously-tagged flag from the
// set it replaces.
//
// One method rather than the rule at each of the three writers — ELBv2's own AddTags/RemoveTags,
// the CloudFormation stamp and the stack-tag reconciliation — because [taggingEverTagged] needs the
// count from *before* the merge, and a writer holding only the merged set cannot recover it. The
// count after the merge stands in for "tags added": a removal that empties the set is already
// covered by the count before it, and no ELB path writes an empty set without having read one
// (#938, #863).
func (r *elbTaggedResource) encode(tags []ELBTag) ([]byte, error) {
	return r.withTags(tags, taggingEverTagged(r.everTagged, len(r.tags), len(tags)))
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
			everTagged: lb.EverTagged,
			withTags: func(tags []ELBTag, everTagged bool) ([]byte, error) {
				lb.Tags = tags
				lb.EverTagged = everTagged
				return json.Marshal(lb)
			}}
	case elbKindTargetGroup:
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil {
			return nil
		}
		return &elbTaggedResource{arn: tg.ARN, stateKey: stateKey, tags: tg.Tags,
			everTagged: tg.EverTagged,
			withTags: func(tags []ELBTag, everTagged bool) ([]byte, error) {
				tg.Tags = tags
				tg.EverTagged = everTagged
				return json.Marshal(tg)
			}}
	case elbKindListener:
		var l ELBListener
		if json.Unmarshal(data, &l) != nil {
			return nil
		}
		return &elbTaggedResource{arn: l.ARN, stateKey: stateKey, tags: l.Tags,
			everTagged: l.EverTagged,
			withTags: func(tags []ELBTag, everTagged bool) ([]byte, error) {
				l.Tags = tags
				l.EverTagged = everTagged
				return json.Marshal(l)
			}}
	case elbKindRule:
		var r ELBRule
		if json.Unmarshal(data, &r) != nil {
			return nil
		}
		return &elbTaggedResource{arn: r.ARN, stateKey: stateKey, tags: r.Tags,
			everTagged: r.EverTagged,
			withTags: func(tags []ELBTag, everTagged bool) ([]byte, error) {
				r.Tags = tags
				r.EverTagged = everTagged
				return json.Marshal(r)
			}}
	case elbKindClassicLB:
		return elbClassicDecodeTaggedResource(stateKey, data)
	default:
		return nil
	}
}

// elbResolveTaggedResource finds the **ELBv2** record an ARN names, or the refusal for it.
//
// This is the resolver ELBv2's own AddTags, RemoveTags and DescribeTags use, and it refuses a
// classic ARN: [elbResourceKindFromARN] carries the argument for why the refusal belongs at those
// three doors. The generation-blind callers use [elbResolveAnyGenerationTaggedResource].
func elbResolveTaggedResource(state StateManager, scope, arn string) (*elbTaggedResource, *AWSError, error) {
	return elbResolveTaggedResourceOfKind(state, elbResourceKindFromARN(arn), scope, arn)
}

// elbResolveAnyGenerationTaggedResource finds the record an ARN names in **either** ELB generation.
//
// Used by the Resource Groups Tagging API's ELB arm and by CloudFormation's two ELB tag writers, for
// the reason [elbAnyGenerationKindFromARN] gives: those three are one surface across both APIs, so a
// classic ARN reaching them is not a caller mistake. A classic ARN that names no record resolves to
// `LoadBalancerNotFound` — the code the classic tag operations publish — rather than to the
// wrong-generation `ValidationError`, because for these callers the generation was never in
// question (#844).
func elbResolveAnyGenerationTaggedResource(
	state StateManager, scope, arn string,
) (*elbTaggedResource, *AWSError, error) {
	return elbResolveTaggedResourceOfKind(state, elbAnyGenerationKindFromARN(arn), scope, arn)
}

// elbResolveTaggedResourceOfKind is the body both resolvers share, taking the kind its caller's
// classifier resolved.
//
// The kind is a parameter rather than something re-derived here so that which classifier ran is the
// caller's decision and is visible at the call site. An empty kind is the classifier's "this ARN
// names nothing I handle", and the refusal for it is the same in both directions: the ARN names no
// resource type the caller can tag.
//
// A read that genuinely fails is reported as an error rather than as a NotFound: a
// broken backend is not an absent resource, and answering LoadBalancerNotFound for one
// would tell a consumer's retry loop the wrong thing.
func elbResolveTaggedResourceOfKind(
	state StateManager, kind, scope, arn string,
) (*elbTaggedResource, *AWSError, error) {
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
// resource with more than its quota's worth of user tags, or nil.
//
// The count is over the post-merge key set with reserved keys excluded, which is the
// same expression [ec2CheckTagLimit] uses and gets the same two documented rules right:
// re-tagging an existing key on a resource already at the limit succeeds, and a reserved
// key neither counts nor consumes room.
//
// The quota is a parameter rather than a constant because the two ELB generations publish
// different numbers; it is resolved from the record, via [elbTagQuotaForKind] at a create or
// [elbTagQuotaForStateKey] at a tag call, never from which API door the request came through
// (#1148).
func elbCheckTagLimit(existing, incoming []ELBTag, quota elbTagQuota) *AWSError {
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
	if len(keys) > quota.max {
		return quota.tooManyTags()
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
func elbCheckCreateTags(tags []ELBTag, quota elbTagQuota) *AWSError {
	if awsErr := elbCheckTagRules(tags); awsErr != nil {
		return awsErr
	}
	return elbCheckTagLimit(nil, tags, quota)
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
	data, err := res.encode(tags)
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
		if limitErr := elbCheckTagLimit(res.tags, tags, elbTagQuotaForStateKey(res.stateKey)); limitErr != nil {
			return nil, limitErr
		}
	}
	for _, res := range resolved {
		if writeErr := p.elbWriteTags(res, elbMergeTags(res.tags, tags)); writeErr != nil {
			return nil, writeErr
		}
	}

	return elbEmptyOKResponse(reqCtx, "AddTags")
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

	return elbEmptyOKResponse(reqCtx, "RemoveTags")
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
	var result tagsResult
	for _, res := range resolved {
		result.TagDescriptions = append(result.TagDescriptions, elbTagDescriptionItem{
			ResourceArn: res.arn,
			Tags:        elbTagItems(res.tags),
		})
	}
	return elbOKResponse(reqCtx, "DescribeTags", elbXMLNS, result)
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
