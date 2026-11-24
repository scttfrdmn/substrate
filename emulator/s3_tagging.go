package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Parsing and validating S3 tag sets, shared by every operation that writes one: PutObject,
// CreateMultipartUpload and CopyObject's REPLACE directive through the x-amz-tagging header (#1434),
// PutObjectTagging and PutBucketTagging through a <Tagging> body, and CreateBucket through
// CreateBucketConfiguration's <Tags> (#1463).
//
// The limits are the published ones. Object tags: "You can associate up to 10 tags with an object.
// Tags that are associated with an object must have unique tag keys. A tag key can be up to 128
// Unicode characters in length, and tag values can be up to 256 Unicode characters in length. Amazon
// S3 object tags are internally represented in UTF-16" (Tagging your objects). Bucket tags: "A tag
// set can contain as many as 50 tags ... Keys must be unique within a tag set", a key of "1 to 128
// Unicode characters" and a value of "0 to 256" (Using cost allocation S3 bucket tags). API_Tag gives
// Key a minimum length of 1.
//
// Two restrictions come from the tag-restriction pages S3's own tagging pages point to rather than
// from S3's: "The aws: prefix is reserved for AWS use" and "the allowed characters across all AWS
// services are: letters (a-z, A-Z), numbers (0-9), and spaces representable in UTF-8, and the
// following characters: + - = . _ : / @" (Amazon EC2 User Guide, Tag restrictions, which the
// Billing guide's "User-defined tag restrictions" defers to for basic restrictions). The character
// class is read as the Unicode one, [\p{L}\p{Z}\p{N}_.:/=+\-@], the pattern the services that
// publish one use (IAM and ELB's Tag shapes), so a non-ASCII letter is accepted.
//
// A set breaking any of them is refused with InvalidTag/400, the code PutObjectTagging and
// PutBucketTagging both publish for "the tag provided was not a valid tag. This error can occur if
// the tag did not pass input validation". The messages are substrate's; S3 publishes none.

const (
	// s3MaxObjectTags is the most tags an object may carry.
	s3MaxObjectTags = 10
	// s3MaxBucketTags is the most tags a general purpose bucket may carry.
	s3MaxBucketTags = 50
	// s3MaxTagKeyLength is the longest tag key, in UTF-16 code units.
	s3MaxTagKeyLength = 128
	// s3MaxTagValueLength is the longest tag value, in UTF-16 code units.
	s3MaxTagValueLength = 256
)

// s3TagCharPattern is the cross-service tag character class: letters, numbers and spaces in any
// script, and + - = . _ : / @. It matches the empty string; a key's minimum length is checked
// separately.
var s3TagCharPattern = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)

// s3InvalidTagResponse is the InvalidTag/400 refusal of a tag set.
func s3InvalidTagResponse(message string) *AWSResponse {
	return s3ErrorResponse("InvalidTag", message, http.StatusBadRequest)
}

// s3UTF16Len is the length of s in UTF-16 code units, the unit S3 measures tag lengths in: a
// character outside the Basic Multilingual Plane counts twice.
func s3UTF16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

// validateS3TagSet checks tags against the published limits for a set of at most maxTags, and
// returns the set as a map. The response is non-nil when the set is refused.
func validateS3TagSet(tags []s3Tag, maxTags int, what string) (map[string]string, *AWSResponse) {
	out, awsErr := s3CheckTagSet(tags, maxTags, what)
	if awsErr != nil {
		return nil, s3InvalidTagResponse(awsErr.Message)
	}
	return out, nil
}

// s3InvalidTag is the InvalidTag/400 refusal of a tag set, as an error.
func s3InvalidTag(message string) *AWSError {
	return &AWSError{Code: "InvalidTag", Message: message, HTTPStatus: http.StatusBadRequest}
}

// s3CheckTagSet is [validateS3TagSet] answering an [AWSError], for a caller that is not writing an
// S3 response.
func s3CheckTagSet(tags []s3Tag, maxTags int, what string) (map[string]string, *AWSError) {
	if len(tags) > maxTags {
		return nil, s3InvalidTag(what + " tags cannot be greater than " + strconv.Itoa(maxTags))
	}
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key == "" {
			return nil, s3InvalidTag("The TagKey you have provided is invalid")
		}
		if taggingReservedKey(t.Key) {
			return nil, s3InvalidTag("Your TagKey cannot be prefixed with aws:")
		}
		if !s3TagCharPattern.MatchString(t.Key) {
			return nil, s3InvalidTag("The TagKey you have provided is invalid")
		}
		if !s3TagCharPattern.MatchString(t.Value) {
			return nil, s3InvalidTag("The TagValue you have provided is invalid")
		}
		if s3UTF16Len(t.Key) > s3MaxTagKeyLength {
			return nil, s3InvalidTag("The TagKey you have provided is too long, max " + strconv.Itoa(s3MaxTagKeyLength))
		}
		if s3UTF16Len(t.Value) > s3MaxTagValueLength {
			return nil, s3InvalidTag("The TagValue you have provided is too long, max " + strconv.Itoa(s3MaxTagValueLength))
		}
		if _, dup := out[t.Key]; dup {
			return nil, s3InvalidTag("Cannot provide multiple Tags with the same key")
		}
		out[t.Key] = t.Value
	}
	return out, nil
}

// validateS3ObjectTagSet validates an object's tag set; see [validateS3TagSet].
func validateS3ObjectTagSet(tags []s3Tag) (map[string]string, *AWSResponse) {
	return validateS3TagSet(tags, s3MaxObjectTags, "Object")
}

// validateS3BucketTagSet validates a bucket's tag set; see [validateS3TagSet].
func validateS3BucketTagSet(tags []s3Tag) (map[string]string, *AWSResponse) {
	return validateS3TagSet(tags, s3MaxBucketTags, "Bucket")
}

// parseS3TaggingHeaderValue decodes an x-amz-tagging value — "the tag-set must be encoded as URL
// Query parameters. (For example, "Key1=Value1")" (API_PutObject) — into tags in the order they
// were written. A key named twice yields two tags, so the duplicate reaches validation rather than
// being collapsed by a map. ok is false when the value is not query-encoded.
func parseS3TaggingHeaderValue(tagging string) (tags []s3Tag, ok bool) {
	for tagging != "" {
		var pair string
		pair, tagging, _ = strings.Cut(tagging, "&")
		if pair == "" {
			continue
		}
		rawKey, rawValue, _ := strings.Cut(pair, "=")
		key, kErr := url.QueryUnescape(rawKey)
		value, vErr := url.QueryUnescape(rawValue)
		if kErr != nil || vErr != nil {
			return nil, false
		}
		tags = append(tags, s3Tag{Key: key, Value: value})
	}
	return tags, true
}

// resolveTaggingHeader reads x-amz-tagging from headers and returns the validated object tag set it
// names, nil when the header is absent or empty, or the refusal to serve.
//
// A value that is not query-encoded is InvalidArgument naming x-amz-tagging, the answer CopyObject's
// REPLACE directive already gave; a set breaking a limit is InvalidTag.
func resolveTaggingHeader(headers map[string]string) (map[string]string, *AWSResponse) {
	tagging := headerValueFold(headers, "x-amz-tagging")
	if tagging == "" {
		return nil, nil
	}
	tags, ok := parseS3TaggingHeaderValue(tagging)
	if !ok {
		return nil, s3ErrorResponseWith(s3Error{
			Code:    "InvalidArgument",
			Message: "The tag-set must be encoded as URL query parameters.",
			Status:  http.StatusBadRequest,
			Details: []s3ErrorDetail{{Name: "ArgumentName", Value: "x-amz-tagging"}},
		})
	}
	set, errResp := validateS3ObjectTagSet(tags)
	if errResp != nil {
		return nil, errResp
	}
	if len(set) == 0 {
		return nil, nil
	}
	return set, nil
}

// ----- Tagging API provider ------------------------------------------------

// S3 answers the Resource Groups Tagging API for its buckets (#1609): GetResources through
// [S3Plugin.listTaggedResources], and TagResources, UntagResources and CloudFormation's stamps
// through [S3Plugin.tagResourceByARN] and [S3Plugin.tagRecord]. A bucket's tag writes, these and
// PutBucketTagging's and DeleteBucketTagging's, hold [S3Plugin.lockBucketTags], which orders them
// with each other and with nothing else: another operation that rewrites the bucket record, such as
// PutBucketVersioning, does not take it. The functions below
// take the state rather than the plugin, so the tagging plugin's direct path, used when no S3 plugin
// is registered, runs the same code.

// s3BucketTagStateKey resolves the resource portion of an S3 ARN to its bucket's state key.
//
// arn:aws:s3:::bucket-name — the resource portion is the bare bucket name, with no type prefix to
// strip. An object, access point or job ARN carries a "/" and is not a bucket, which is the only
// distinction this ARN shape offers.
func s3BucketTagStateKey(resource string) (string, error) {
	if strings.Contains(resource, "/") {
		return "", unsupportedTagResource("S3 %q is not a bucket ARN", resource)
	}
	return "bucket:" + resource, nil
}

// s3ListTaggedBuckets lists the caller's buckets.
//
// S3 is the one service whose scope [taggingResourceInScope] cannot decide, because an S3 bucket ARN
// is arn:aws:s3:::{name} — it carries neither an account nor a Region segment, S3's bucket namespace
// being global. So the scope comes off the record instead: [S3Bucket] stores the Region it was
// created in, and #937 added the account that created it. The key cannot carry either, since a bucket
// name is globally unique and the key is what makes that true here as well.
//
// An empty Region or AccountID is in scope, on the same rule [taggingResourceInScope] applies to an
// absent ARN segment: a record that states no scope is not evidence of a scope to exclude it from,
// and a bucket written by a path that does not set the field would otherwise disappear from
// GetResources rather than be reported with what is known about it.
func s3ListTaggedBuckets(_ context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := state.List(goCtx, s3Namespace, "bucket:")
	if err != nil {
		return nil, fmt.Errorf("list s3 buckets: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := state.Get(goCtx, s3Namespace, k)
		if err != nil || raw == nil {
			continue
		}
		var b S3Bucket
		if err := json.Unmarshal(raw, &b); err != nil {
			continue
		}
		if b.Region != "" && b.Region != reqCtx.Region {
			continue
		}
		if b.AccountID != "" && b.AccountID != reqCtx.AccountID {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: s3BucketARN(b.Name),
			Tags:        mapToTaggingTags(b.Tags),
			everTagged:  b.EverTagged,
		})
	}
	return out, nil
}

// s3MergeBucketTags merges addTags and removeKeys into the bucket record raw, stored at key.
func s3MergeBucketTags(
	goCtx context.Context, state StateManager, key string, raw []byte,
	addTags map[string]string, removeKeys []string,
) error {
	var b S3Bucket
	if err := json.Unmarshal(raw, &b); err != nil {
		return fmt.Errorf("unmarshal S3Bucket: %w", err)
	}
	b.EverTagged = taggingEverTagged(b.EverTagged, len(b.Tags), len(addTags))
	b.Tags = mergeStringMap(b.Tags, addTags, removeKeys)
	updated, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("tagging mergeResourceTags marshal: %w", err)
	}
	return state.Put(goCtx, s3Namespace, key, updated)
}

// lockBucketTags acquires the lock every write of bucket's tag set holds, and returns its unlock
// function. It is the bucket's slot in [S3Plugin.keyLocks] under the empty object key, which no
// object can have, so it never contends with an object write except by sharing a stripe.
func (p *S3Plugin) lockBucketTags(bucket string) func() {
	return p.keyLocks.lock(bucket, "")
}

// listTaggedResources implements [taggableResourceLister].
func (p *S3Plugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return s3ListTaggedBuckets(ctx, p.state, reqCtx)
}

// tagResourceByARN implements [resourceTagger]. What it adds is held to PutBucketTagging's key and
// value rules ([s3CheckTagSet]), and a key it removes may not carry the reserved `aws:` prefix, so a
// caller can neither write nor strip one.
func (p *S3Plugin) tagResourceByARN(
	_ *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	parts, ok := taggingSplitARN(arn)
	if !ok {
		return taggingResolveFailed(fmt.Errorf("invalid ARN: %q", arn))
	}
	key, err := s3BucketTagStateKey(parts[5])
	if err != nil {
		return taggingResolveFailed(err)
	}
	if awsErr := s3TagKeyRefusal(add, remove); awsErr != nil {
		return taggingQuotaRefusal(awsErr)
	}
	return p.tagRecord(context.Background(), key, add, remove, quota)
}

// s3TagKeyRefusal holds a tagging API write to PutBucketTagging's key and value rules
// ([s3CheckTagSet]) for what it adds, and refuses a removal of a key with the reserved prefix.
// Both the S3 plugin and the tagging plugin's direct path apply it, before looking for the bucket.
func s3TagKeyRefusal(add map[string]string, remove []string) *AWSError {
	tags := make([]s3Tag, 0, len(add))
	for _, k := range taggingSortedKeys(add) {
		tags = append(tags, s3Tag{Key: k, Value: add[k]})
	}
	if _, awsErr := s3CheckTagSet(tags, s3MaxBucketTags, "Bucket"); awsErr != nil {
		return awsErr
	}
	for _, k := range remove {
		if taggingReservedKey(k) {
			return s3InvalidTag("Your TagKey cannot be prefixed with aws:")
		}
	}
	return nil
}

// tagRecord implements [resourceTagger]. With quota enforced, the merged set is held to the bucket
// limit ([s3BucketTagQuotaRefusal]).
func (p *S3Plugin) tagRecord(
	ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	defer p.lockBucketTags(strings.TrimPrefix(key, "bucket:"))()
	raw, err := loadTagRecord(ctx, p.state, s3Namespace, key)
	if err != nil {
		return err
	}
	if quota == enforceTagQuota && len(add) > 0 {
		if refusal := s3BucketTagQuotaRefusal(raw, add, remove); refusal != nil {
			return taggingQuotaRefusal(refusal)
		}
	}
	return s3MergeBucketTags(ctx, p.state, key, raw, add, remove)
}

// s3BucketTagQuotaRefusal refuses a merge that would leave the bucket record raw with more than
// [s3MaxBucketTags] user tags. An unreadable record is left for the merge to report, as
// [taggingDecodeForQuota] leaves one.
//
// One rule for every bucket writer: at most 50 tags a caller set. "A tag set can contain as many as
// 50 tags" (Using cost allocation S3 bucket tags), and "System created tags that begin with aws: are
// reserved for AWS use, and do not count against this limit" (Tagging AWS Resources, Tag naming
// limits and requirements). PutBucketTagging counts its whole set because a caller's set can hold
// no reserved key; a merge here counts all but the reserved keys, which only CloudFormation's
// stamp writes.
func s3BucketTagQuotaRefusal(raw []byte, add map[string]string, remove []string) *AWSError {
	var b S3Bucket
	if !taggingDecodeForQuota(raw, &b) {
		return nil
	}
	user := 0
	for k := range mergeStringMap(b.Tags, add, remove) {
		if !taggingReservedKey(k) {
			user++
		}
	}
	if user > s3MaxBucketTags {
		return s3InvalidTag("Bucket tags cannot be greater than " + strconv.Itoa(s3MaxBucketTags))
	}
	return nil
}
