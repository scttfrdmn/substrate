package emulator

import (
	"net/http"
	"net/url"
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
	if len(tags) > maxTags {
		return nil, s3InvalidTagResponse(what + " tags cannot be greater than " + strconv.Itoa(maxTags))
	}
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Key == "" {
			return nil, s3InvalidTagResponse("The TagKey you have provided is invalid")
		}
		if s3UTF16Len(t.Key) > s3MaxTagKeyLength {
			return nil, s3InvalidTagResponse("The TagKey you have provided is too long, max " + strconv.Itoa(s3MaxTagKeyLength))
		}
		if s3UTF16Len(t.Value) > s3MaxTagValueLength {
			return nil, s3InvalidTagResponse("The TagValue you have provided is too long, max " + strconv.Itoa(s3MaxTagValueLength))
		}
		if _, dup := out[t.Key]; dup {
			return nil, s3InvalidTagResponse("Cannot provide multiple Tags with the same key")
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
