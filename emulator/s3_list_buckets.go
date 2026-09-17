package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ListBuckets and its four request parameters (#884).
//
// The handler took its request as `_ *AWSRequest` and implemented none of them: it
// returned every bucket, unfiltered and unpaged, whatever the caller asked for. The
// sharp end of that is `max-buckets` and `continuation-token`, because a dropped
// parameter is invisible in the response. A caller asking for 10 buckets and getting
// 500 sees a well-formed answer — a short page and a complete listing are the same
// shape — and a caller looping on a token got page one again every time, so it either
// spun forever or processed the same buckets twice. `prefix` and `bucket-region`
// merely over-returned, which a consumer can at least notice.
//
// The Region half needed no new state: CreateBucket already persists
// [S3Bucket.Region] from the request context, so `bucket-region` filters on a value
// substrate has always stored. The issue flagged storing it as "the one non-trivial
// part"; it was already done.
//
// Pagination is a cursor over the lexicographic bucket-name order that
// [StateManager.List] now guarantees (#865). That ordering is *substrate's* reading —
// ListBuckets documents no order at all — and pagination is why it has to be
// guaranteed rather than merely tidy: a cursor over an unstable order omits and
// repeats members between pages.

// s3MaxBucketsLimit is the largest value `max-buckets` accepts, and
// s3MaxBucketsDefault the page size applied when the caller names none.
//
// Both are from API_ListBuckets. The range is published on the parameter — "Valid
// Range: Minimum value of 1. Maximum value of 10000" — and the default from the note
// that "If you specify the bucket-region, prefix, or continuation-token query
// parameters without using max-buckets to set the maximum number of buckets returned
// in the response, Amazon S3 applies a default page size of 10,000 and provides a
// continuation token if there are more buckets."
//
// The same 10,000 is also the general purpose bucket quota at which AWS stops
// accepting unpaginated requests, so one number serves both the paged and unpaged
// cases and substrate does not need to distinguish them.
const (
	s3MaxBucketsLimit   = 10000
	s3MaxBucketsDefault = 10000
)

// s3ContinuationTokenMaxLength is the `continuation-token` length ceiling:
// "Length Constraints: Minimum length of 0. Maximum length of 1024".
const s3ContinuationTokenMaxLength = 1024

// s3BucketEntry is one `Bucket` in a ListAllMyBucketsResult.
//
// BucketArn is deliberately absent rather than omitted-when-empty. API_Bucket says of
// it: "This parameter is only supported for S3 directory buckets", and API_ListBuckets
// opens with "This operation is not supported for directory buckets" — so the one
// operation that renders a Bucket can never be looking at a bucket the member applies
// to. A synthesized general purpose bucket ARN here would be a value real S3 does not
// return, which is worse than a missing one: a consumer reading it would build on a
// field that is empty against AWS. #884 asked for BucketArn on the strength of its
// appearing in the Bucket type; the type page is what rules it out.
//
// BucketRegion is conditional, quoting API_Bucket: "BucketRegion indicates the AWS
// region where the bucket is located. If the request contains at least one valid
// parameter, it is included in the response." The examples bear that out — the
// unpaginated Example 1 shows no BucketRegion and Examples 2-5, each of which passes a
// parameter, all show one.
type s3BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion,omitempty"`
}

// s3ListAllMyBucketsResult is the ListBuckets response body.
//
// Field order follows the published Response Syntax — Buckets, Owner,
// ContinuationToken, Prefix — because Go's xml encoder emits struct fields in
// declaration order and a consumer comparing a recorded body byte-for-byte would see
// any reordering.
//
// ContinuationToken is the *next* page's token, which is a genuine asymmetry with
// substrate's other S3 listings rather than a slip: ListObjectsV2 echoes the request
// token as ContinuationToken and advertises the next one as NextContinuationToken,
// whereas ListBuckets publishes no NextContinuationToken at all and reuses the one
// name for the forward cursor — "ContinuationToken is included in the response when
// there are more buckets that can be listed with pagination. The next ListBuckets
// request to Amazon S3 can be continued with this ContinuationToken." AWS's own
// Examples 2 and 3 show the response value being passed straight back as the
// `continuation-token` query parameter.
type s3ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets struct {
		Bucket []s3BucketEntry `xml:"Bucket"`
	} `xml:"Buckets"`
	Owner             s3BucketsOwner `xml:"Owner"`
	ContinuationToken string         `xml:"ContinuationToken,omitempty"`
	Prefix            string         `xml:"Prefix,omitempty"`
}

// s3BucketsOwner is the `Owner` of a ListAllMyBucketsResult.
//
// It exists rather than reusing [S3Owner] because the two differ in exactly the way
// that matters here: S3Owner is the ACL owner, where DisplayName is rendered
// unconditionally, and changing its tag would alter every ACL response. ListBuckets
// must be able to omit DisplayName — see [s3ListBucketsOwnerFor] for why it always
// does — and an empty `<DisplayName></DisplayName>` is not a shape AWS returns.
type s3BucketsOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

// s3ListBucketsParams is the parsed and validated query string.
type s3ListBucketsParams struct {
	prefix       string
	bucketRegion string
	afterBucket  string
	maxBuckets   int

	// filtered records whether the caller passed any of the four parameters, which
	// is what gates BucketRegion in the response per API_Bucket.
	filtered bool
}

// listBuckets handles GET / — the account's buckets, narrowed by prefix and
// bucket-region and paged by max-buckets and continuation-token.
func (p *S3Plugin) listBuckets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	params, errResp := parseS3ListBucketsParams(req)
	if errResp != nil {
		return errResp, nil
	}

	ctx := context.Background()
	keys, err := p.state.List(ctx, s3Namespace, "bucket:")
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	result := s3ListAllMyBucketsResult{
		Owner:  s3ListBucketsOwnerFor(reqCtx),
		Prefix: params.prefix,
	}

	// keys arrive in lexicographic order from [StateManager.List], and every key here
	// shares the "bucket:" prefix, so the order of the names is the order of the keys.
	// The cursor is a strict ">" against the last name of the previous page, so a
	// bucket deleted between two pages costs no others their place.
	for _, k := range keys {
		name := strings.TrimPrefix(k, "bucket:")
		if params.afterBucket != "" && name <= params.afterBucket {
			continue
		}
		if params.prefix != "" && !strings.HasPrefix(name, params.prefix) {
			continue
		}

		b, loadErr := p.loadBucket(ctx, k)
		if loadErr != nil || b == nil {
			continue
		}
		if params.bucketRegion != "" && b.Region != params.bucketRegion {
			continue
		}

		// Truncation is decided on the *next* matching bucket rather than on a count
		// reaching the limit, so a listing whose last page exactly fills max-buckets
		// reports no token. Emitting one there would cost a paging caller an extra
		// round trip to an empty page, and AWS ties the token to "there are more
		// buckets that can be listed" rather than to a full page.
		if len(result.Buckets.Bucket) >= params.maxBuckets {
			last := result.Buckets.Bucket[len(result.Buckets.Bucket)-1].Name
			result.ContinuationToken = base64.StdEncoding.EncodeToString([]byte(last))
			break
		}

		entry := s3BucketEntry{
			Name:         b.Name,
			CreationDate: b.CreationDate.UTC().Format(time.RFC3339),
		}
		if params.filtered {
			entry.BucketRegion = b.Region
		}
		result.Buckets.Bucket = append(result.Buckets.Bucket, entry)
	}

	return s3XMLResponse(http.StatusOK, result)
}

// loadBucket reads and decodes the bucket stored under key, returning nil when the
// key holds nothing.
//
// A decode failure is reported rather than swallowed: the caller skips the entry
// either way, but a listing that silently drops a bucket whose stored JSON is corrupt
// looks identical to one where the bucket does not exist.
func (p *S3Plugin) loadBucket(ctx context.Context, key string) (*S3Bucket, error) {
	data, err := p.state.Get(ctx, s3Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("get bucket %q: %w", key, err)
	}
	if data == nil {
		return nil, nil
	}
	var b S3Bucket
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("unmarshal bucket %q: %w", key, err)
	}
	return &b, nil
}

// s3ListBucketsOwnerFor reports the owner of the listed buckets.
//
// Substrate has no account-owner concept beyond the account ID, so the canonical user
// ID is the account ID. That is not what real S3 returns — a real ID is a 64-character
// hex canonical user ID unrelated to the account number — but it is the only
// account-scoped identifier substrate holds, and it keeps the value stable across a
// replay, which a synthesized one would not be.
//
// DisplayName is omitted. Owner.DisplayName is `Required: No` and carries no
// description at all on API_Owner, and every one of the five ListBuckets examples
// renders the owner as `<Owner><ID>AIDACKCEVSQ6C2EXAMPLE</ID></Owner>` with no
// DisplayName — so omitting it matches the documented shape rather than guessing a
// name for an account that has none.
func s3ListBucketsOwnerFor(reqCtx *RequestContext) s3BucketsOwner {
	accountID := defaultAccountID
	if reqCtx != nil && reqCtx.AccountID != "" {
		accountID = reqCtx.AccountID
	}
	return s3BucketsOwner{ID: accountID}
}

// parseS3ListBucketsParams validates the ListBuckets query string, returning either
// the parsed parameters or the error response to send.
//
// All four are query-string parameters, per the published Request Syntax:
// `GET /?bucket-region={{BucketRegion}}&continuation-token={{ContinuationToken}}&max-buckets={{MaxBuckets}}&prefix={{Prefix}}`.
//
// **The error code is unverified.** API_ListBuckets publishes no Errors section, and
// the S3 ErrorResponses page returns an empty body to automated fetches, so nothing
// here could be sourced for what AWS returns on an out-of-range `max-buckets` or an
// undecodable token. `InvalidArgument` with HTTP 400 is substrate's reading, chosen
// because it is what S3 uses elsewhere for a malformed query-parameter value.
//
// Refusing rather than clamping is deliberate, and it is the same argument the
// operation's own defect makes: silently substituting a value the caller did not ask
// for is invisible in a well-formed response. An undecodable continuation token is the
// worst case — restarting at page one there is precisely the infinite loop #884
// describes — so it is refused rather than treated as "no cursor".
func parseS3ListBucketsParams(req *AWSRequest) (*s3ListBucketsParams, *AWSResponse) {
	params := &s3ListBucketsParams{maxBuckets: s3MaxBucketsDefault}
	if req == nil {
		return params, nil
	}

	rawMax := req.Params["max-buckets"]
	contToken := req.Params["continuation-token"]
	params.prefix = req.Params["prefix"]
	params.bucketRegion = req.Params["bucket-region"]
	params.filtered = rawMax != "" || contToken != "" || params.prefix != "" || params.bucketRegion != ""

	if rawMax != "" {
		n, err := strconv.Atoi(rawMax)
		if err != nil || n < 1 || n > s3MaxBucketsLimit {
			return nil, s3ErrorResponse("InvalidArgument",
				fmt.Sprintf("max-buckets must be an integer between 1 and %d.", s3MaxBucketsLimit),
				http.StatusBadRequest)
		}
		params.maxBuckets = n
	}

	if contToken != "" {
		if len(contToken) > s3ContinuationTokenMaxLength {
			return nil, s3ErrorResponse("InvalidArgument",
				fmt.Sprintf("continuation-token must be at most %d characters.", s3ContinuationTokenMaxLength),
				http.StatusBadRequest)
		}
		after, refusal := s3DecodeContinuationToken(contToken)
		if refusal != nil {
			return nil, refusal
		}
		params.afterBucket = after
	}

	return params, nil
}

// s3DecodeContinuationToken decodes the cursor ListBuckets and ListObjectsV2 share, returning
// either the key or bucket name the next page starts after or the error response to send.
//
// The token is base64, which satisfies AWS's only statement about its content —
// "ContinuationToken is obfuscated and is not a real key" — and is what makes a token substrate
// never issued detectable at all. One helper is what makes the two operations refuse alike
// structurally rather than by two call sites agreeing (#915); the 1024-character ceiling stays at
// the ListBuckets call site, because that is the one of the two whose page publishes a Length
// Constraint on the parameter.
//
// **The error code is unverified**, for the reason [parseS3ListBucketsParams] gives, and it is no
// better sourced on the other side: API_ListObjectsV2 publishes exactly one error, NoSuchBucket at
// 404, and nothing on the page says what an unusable continuation-token answers.
//
// A token that decodes to something other than a real key is not refused — it simply selects the
// keys sorting after that value, which may be none. Resuming after an object that has since been
// deleted is precisely what a value-based cursor is for, the same reading [parseQueryMarker]
// records for the RDS and ElastiCache marker.
func s3DecodeContinuationToken(token string) (string, *AWSResponse) {
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", s3ErrorResponse("InvalidArgument",
			"The continuation token provided is incorrect.",
			http.StatusBadRequest)
	}
	return string(decoded), nil
}
