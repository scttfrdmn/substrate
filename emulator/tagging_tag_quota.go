package emulator

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Per-resource tag quotas on the Resource Groups Tagging API path — #1000.
//
// Four services enforce a published per-resource tag quota on their own tagging operations — EC2
// ([ec2CheckTagLimit], 50), ELBv2 ([elbCheckTagLimit], 50), IAM ([iamCheckTagLimit], 50) and Kinesis
// ([kinesisCheckTagQuota], 50) — and all four are reachable through [TaggingPlugin.resolveARN], whose
// merge consulted none of them. So `TagResources` was the one way to put a resource over its own
// service's quota, after which that service's own tagging operation refused every further add: a
// resource in a state substrate's own reference says cannot exist, reached through substrate's own API.
// The shipped record said otherwise twice — `CHANGELOG.md` and `docs/services.md` both claimed Kinesis
// was the first quota of any service and that the shared merge covered sixteen — and both are corrected
// with this change. It is 23 namespace arms, and Kinesis was the fourth quota, not the first.
//
// Three readings this file makes.
//
//  1. **The refusal carries the owning service's own code, not one of FailureInfo's two enumerated
//     ones.** `FailureInfo`'s `ErrorCode` publishes "Valid Values: InternalServiceException |
//     InvalidParameterException" and, in the same member's prose, that it "can also include any valid
//     error code returned by the AWS service that hosts the resource that the ARN key represents",
//     offering `AccessDeniedException` — which is in neither enumerated value — as its example. The
//     prose is the half that fits here: a caller that put a stream over fifty tags needs to know it hit
//     a quota, and neither enumerated code says so. [TaggingPlugin.tagMergeFailure] reports only the
//     two enumerated codes for every other failure, because for those the enumeration loses nothing.
//  2. **The quota is checked on the RGTA path and on neither CloudFormation path.** All three go
//     through [mergeResourceTags], which is the point of that function, so the mode is a parameter
//     rather than a second merge. `API_TagResources` publishes "Each resource can have up to 50 tags"
//     itself, so the RGTA side is AWS-backed at the RGTA level rather than borrowed from the four
//     services by analogy. The two CloudFormation sides are skipped for two different reasons, and
//     conflating them would misstate one of them:
//
//     [cfnStampResourceTags] writes only `aws:cloudformation:*` keys, and EC2 and ELB state outright
//     that "[t]ags with the aws: prefix do not count against your tags per resource limit" — so
//     counting the stamp would refuse a deploy on a rule AWS states about something else.
//
//     [cfnPropagateRecordStackTags] writes the caller's own stack tags, which a quota could
//     legitimately refuse. It is skipped because nothing published says what CloudFormation does when
//     propagation would exceed a resource's quota: CloudFormation publishes a 50-tag limit on the
//     **stack**, and the resource-side interaction is neither in its quotas page nor in any operation's
//     Errors section. Refusing there would also turn a tag overflow into a failed deploy, which is a
//     larger decision than #1000 asks for. The gap is filed as #1077 rather than guessed.
//  3. **Each service's own checker is called rather than a shared count.** The four disagree in ways a
//     shared implementation would have to flatten: IAM answers `LimitExceeded` at **409** where the
//     other three answer 400, the codes are four different strings, and EC2 and ELB exclude `aws:`
//     keys from the count where IAM and Kinesis count them. That last divergence is recorded rather
//     than unified — it is what each service publishes — and it is unobservable on this path, since
//     the RGTA refuses a reserved key upstream and only the stamp path writes one.
//
// A removal is never checked. Both of [mergeStringMap]'s later stages only shrink the key set, so
// `UntagResources` cannot push a resource over a quota, and a resource already over one (written before
// this change) stays reportable and removable rather than becoming untouchable.

// tagQuotaMode says whether a tag merge enforces the owning service's published per-resource tag
// quota.
//
// A named type rather than a bool parameter so the two call sites that skip the check say so in the
// call. See the file comment for why the CloudFormation stamp is the side that skips.
type tagQuotaMode bool

const (
	// enforceTagQuota checks the owning service's published per-resource tag quota before merging.
	enforceTagQuota tagQuotaMode = true

	// skipTagQuota merges without a quota check, which is what both CloudFormation writers do.
	skipTagQuota tagQuotaMode = false
)

// taggingQuotaError carries an owning service's own quota refusal out of a tag merge.
//
// A wrapper type rather than the [AWSError] itself because the merge signature is `error` and three
// of its four callers want nothing to do with an API response; [TaggingPlugin.tagMergeFailure]
// errors.As-matches it and renders the three fields into the request's `FailedResourcesMap` entry.
// This is the same division of labor as [errTagResourceNotFound]: the code that detects the failure
// is not the code that decides what a caller is told about it.
type taggingQuotaError struct {
	awsErr *AWSError
}

// Error implements error.
func (e *taggingQuotaError) Error() string {
	return fmt.Sprintf("tag quota exceeded: %s: %s", e.awsErr.Code, e.awsErr.Message)
}

// taggingQuotaRefusal wraps a service's quota refusal for travel out of a merge, and returns a nil
// error for a nil refusal.
//
// The nil case is what lets every arm of [taggingCheckTagQuota] end in one line: a typed nil pointer
// assigned to an `error` is a non-nil interface value, so returning awsErr directly would make every
// passing check look like a failure.
func taggingQuotaRefusal(awsErr *AWSError) error {
	if awsErr == nil {
		return nil
	}
	return &taggingQuotaError{awsErr: awsErr}
}

// taggingCheckTagQuota reports whether merging addTags into the record stored at ns/key would leave
// it over the owning service's published per-resource tag quota.
//
// It returns nil for a namespace whose service publishes no quota substrate models, which is nineteen
// of the 23 arms — a silent pass rather than an error, because the absence of a quota is the normal
// case and the four that have one are named here explicitly.
//
// The existing tag set is read out of raw with a **minimal decode** — a struct carrying the tag member
// alone — rather than the arm's own concrete type. That keeps this function from having to know which
// of a namespace's several record shapes a key names, and it cannot truncate anything, because nothing
// here is written back. A record that will not decode into the minimal shape is passed rather than
// refused: the arm below is about to fail on the same bytes with a message naming the actual problem,
// and a quota refusal would misreport an unmarshalable record as a full one.
func taggingCheckTagQuota(ns, key string, raw []byte, addTags map[string]string, removeKeys []string) error {
	if len(addTags) == 0 {
		return nil
	}

	switch ns {
	case ec2Namespace:
		return taggingQuotaRefusal(taggingEC2QuotaRefusal(raw, addTags, removeKeys))
	case elbNamespace:
		if !elbKeyIsTaggable(key) {
			return nil
		}
		return taggingQuotaRefusal(taggingELBQuotaRefusal(raw, addTags, removeKeys))
	case iamNamespace:
		return taggingQuotaRefusal(taggingIAMQuotaRefusal(key, raw, addTags, removeKeys))
	case kinesisNamespace:
		return taggingQuotaRefusal(taggingKinesisQuotaRefusal(raw, addTags, removeKeys))
	default:
		return nil
	}
}

// taggingDecodeForQuota decodes a stored record into the minimal tag-carrying shape a quota count
// needs, reporting false when the bytes are not that shape.
//
// The decode error is deliberately dropped rather than returned, and this is the one place that
// decision is written down. The arm of [mergeResourceTags] below is about to fail on the same bytes
// with a message naming the actual problem — "unmarshal KinesisStream: …", reported as
// InternalServiceException — so turning an unreadable record into a *quota* refusal would tell a
// caller its resource is full when substrate cannot read the resource at all. Nothing here is written
// back, so a partial decode costs nothing either.
func taggingDecodeForQuota(raw []byte, into any) bool {
	return json.Unmarshal(raw, into) == nil
}

// taggingEC2QuotaRefusal applies [ec2CheckTagLimit] to a stored EC2 record.
//
// Every taggable EC2 record spells the member `tags` in lowercase and holds [EC2Tag], so one shape
// covers all fourteen of them.
func taggingEC2QuotaRefusal(raw []byte, addTags map[string]string, removeKeys []string) *AWSError {
	var record struct {
		Tags []EC2Tag `json:"tags"`
	}
	if !taggingDecodeForQuota(raw, &record) {
		return nil
	}
	return ec2CheckTagLimit(
		taggingDropRemovedTags(record.Tags, removeKeys, func(t EC2Tag) string { return t.Key }),
		taggingTagsAsEC2(addTags))
}

// taggingELBQuotaRefusal applies [elbCheckTagLimit] to a stored ELBv2 record.
//
// All four taggable ELBv2 records spell the member `Tags` and hold the same `Key`/`Value` pair, which
// is the same fact the elasticloadbalancing arm of [mergeResourceTags] relies on to merge them all
// through one call.
func taggingELBQuotaRefusal(raw []byte, addTags map[string]string, removeKeys []string) *AWSError {
	var record struct {
		Tags []ELBTag `json:"Tags"`
	}
	if !taggingDecodeForQuota(raw, &record) {
		return nil
	}
	return elbCheckTagLimit(
		taggingDropRemovedTags(record.Tags, removeKeys, func(t ELBTag) string { return t.Key }),
		taggingTagsAsELB(addTags))
}

// taggingIAMQuotaRefusal applies [iamCheckTagLimitAWSError] to a stored IAM user or role.
//
// The key prefixes mirror the iam arm of [mergeResourceTags], which stores tags on a user and a role
// and on nothing else in the namespace; any other key returns no refusal so that arm can answer with
// the message that names the actual problem.
//
// The merged set comes from [mergeIAMTags] — the same function the arm will use — rather than from a
// count, because IAM's checker takes the post-merge slice and because that function is where the
// entity type's case rule collapses two keys that differ only in case.
func taggingIAMQuotaRefusal(key string, raw []byte, addTags map[string]string, removeKeys []string) *AWSError {
	if !strings.HasPrefix(key, "user:") && !strings.HasPrefix(key, "role:") {
		return nil
	}
	var record struct {
		Tags []IAMTag `json:"Tags"`
	}
	if !taggingDecodeForQuota(raw, &record) {
		return nil
	}
	return iamCheckTagLimitAWSError(mergeIAMTags(record.Tags, addTags, removeKeys))
}

// taggingKinesisQuotaRefusal applies [kinesisCheckTagQuota] to a stored stream.
//
// The target is rebuilt from the record's own `AccountID`/`Region`/`StreamName` rather than from a
// request context, because the ARN in the refusal message must name the stream the ARN in the request
// resolved to — which for a cross-account `TagResources` is not the caller's own account (#966). The
// record is the authority on that, since [TaggingPlugin.resolveARN] built the state key from the same
// three segments.
func taggingKinesisQuotaRefusal(raw []byte, addTags map[string]string, removeKeys []string) *AWSError {
	var record struct {
		StreamName string            `json:"StreamName"`
		AccountID  string            `json:"AccountID"`
		Region     string            `json:"Region"`
		Tags       map[string]string `json:"Tags"`
	}
	if !taggingDecodeForQuota(raw, &record) {
		return nil
	}
	existing := make(map[string]string, len(record.Tags))
	for k, v := range record.Tags {
		existing[k] = v
	}
	for _, k := range removeKeys {
		delete(existing, k)
	}
	target := kinesisStreamTarget{
		AccountID: record.AccountID,
		Region:    record.Region,
		Name:      record.StreamName,
	}
	return kinesisCheckTagQuota(target, existing, addTags)
}

// taggingDropRemovedTags returns tags without the entries removeKeys names.
//
// The four checkers count a post-merge set, and [mergeStringMap] and its slice-shaped siblings apply
// removals last, so a request that both adds and removes has to be counted against what survives.
// No reachable caller sends both — `TagResources` passes only addTags and `UntagResources` only
// removeKeys — so this is the merge semantics being followed rather than a case being handled.
func taggingDropRemovedTags[T any](tags []T, removeKeys []string, keyOf func(T) string) []T {
	if len(removeKeys) == 0 {
		return tags
	}
	removed := make(map[string]struct{}, len(removeKeys))
	for _, k := range removeKeys {
		removed[k] = struct{}{}
	}
	kept := make([]T, 0, len(tags))
	for _, t := range tags {
		if _, ok := removed[keyOf(t)]; !ok {
			kept = append(kept, t)
		}
	}
	return kept
}

// taggingTagsAsEC2 renders an addTags map as the [EC2Tag] slice [ec2CheckTagLimit] counts.
//
// Order does not matter — the checker builds a key set — so no sort is needed here, unlike the merge
// helpers that write a slice back to state (#862).
func taggingTagsAsEC2(addTags map[string]string) []EC2Tag {
	out := make([]EC2Tag, 0, len(addTags))
	for k, v := range addTags {
		out = append(out, EC2Tag{Key: k, Value: v})
	}
	return out
}

// taggingTagsAsELB renders an addTags map as the [ELBTag] slice [elbCheckTagLimit] counts. See
// [taggingTagsAsEC2] for why it is unsorted.
func taggingTagsAsELB(addTags map[string]string) []ELBTag {
	out := make([]ELBTag, 0, len(addTags))
	for k, v := range addTags {
		out = append(out, ELBTag{Key: k, Value: v})
	}
	return out
}
