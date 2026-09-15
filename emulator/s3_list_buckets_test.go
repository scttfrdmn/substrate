package emulator_test

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// These are #884's gates. Every bucket here is created with a real PUT and every
// listing read back off the raw wire response, per the standing rule that a helper
// writing state directly cannot prove a value is readable through the owning
// service's own call (#765).
//
// The decode target below deliberately declares BucketArn even though substrate never
// emits it. Asserting a field is absent requires somewhere for it to land; a struct
// without the field would unmarshal an unexpected element into nothing and pass.

// listBucketsBucket mirrors one Bucket element of a ListAllMyBucketsResult.
type listBucketsBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion"`
	BucketArn    string `xml:"BucketArn"`
}

// listBucketsResult mirrors the ListBuckets response body.
type listBucketsResult struct {
	XMLName xml.Name            `xml:"ListAllMyBucketsResult"`
	Buckets []listBucketsBucket `xml:"Buckets>Bucket"`
	Owner   struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	} `xml:"Owner"`
	ContinuationToken string `xml:"ContinuationToken"`
	Prefix            string `xml:"Prefix"`
}

// s3RegionalRequest issues a path-style S3 request against a Region-bearing host, so
// the Region the plugin records comes from the wire rather than from a test reaching
// into state. "s3.<region>.amazonaws.com" is the layout extractRegionFromHost reads.
func s3RegionalRequest(t *testing.T, srv *emulator.Server, method, region, path string) *httptest.ResponseRecorder {
	t.Helper()
	host := "s3." + region + ".amazonaws.com"
	r := httptest.NewRequest(method, "http://"+host+path, nil)
	r.Host = host
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w
}

// createBucketInRegion creates a bucket through CreateBucket in the named Region.
func createBucketInRegion(t *testing.T, srv *emulator.Server, region, bucket string) {
	t.Helper()
	w := s3RegionalRequest(t, srv, http.MethodPut, region, "/"+bucket)
	require.Equal(t, http.StatusOK, w.Code, "CreateBucket %s in %s", bucket, region)
}

// listBuckets issues ListBuckets with a raw query string and decodes the body.
func listBuckets(t *testing.T, srv *emulator.Server, query string) (int, listBucketsResult) {
	t.Helper()
	path := "/"
	if query != "" {
		path += "?" + query
	}
	w := s3Request(t, srv, http.MethodGet, path, nil, nil)
	var out listBucketsResult
	if w.Code == http.StatusOK {
		require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &out), "body: %s", w.Body.String())
	}
	return w.Code, out
}

// bucketNames pulls the Name of each returned bucket, in response order.
func bucketNames(res listBucketsResult) []string {
	names := make([]string, 0, len(res.Buckets))
	for _, b := range res.Buckets {
		names = append(names, b.Name)
	}
	return names
}

// listBucketsFixture creates nine buckets across three Regions and returns the
// server. The names are chosen so that prefix groups and Region groups cut across
// each other: filtering by one must not imply the other.
//
// Lexicographic order of all nine:
//
//	alpha-one, alpha-three, alpha-two, beta-one, beta-three, beta-two,
//	gamma-one, gamma-three, gamma-two
func listBucketsFixture(t *testing.T) *emulator.Server {
	t.Helper()
	srv, _ := newS3TestServer(t)
	for _, b := range []struct{ region, name string }{
		{"us-east-1", "alpha-one"},
		{"us-west-2", "alpha-two"},
		{"eu-west-1", "alpha-three"},
		{"us-east-1", "beta-one"},
		{"us-west-2", "beta-two"},
		{"eu-west-1", "beta-three"},
		{"us-east-1", "gamma-one"},
		{"us-west-2", "gamma-two"},
		{"eu-west-1", "gamma-three"},
	} {
		createBucketInRegion(t, srv, b.region, b.name)
	}
	return srv
}

// allNineBuckets is the fixture's lexicographic order, which is the order every
// unfiltered listing must report and the order pagination walks.
var allNineBuckets = []string{
	"alpha-one", "alpha-three", "alpha-two",
	"beta-one", "beta-three", "beta-two",
	"gamma-one", "gamma-three", "gamma-two",
}

// TestS3ListBuckets_MaxBucketsBoundsTheResult is the half of #884 that a caller
// cannot otherwise detect: before the fix `max-buckets=3` returned all nine, and a
// short page and a complete listing are the same shape.
func TestS3ListBuckets_MaxBucketsBoundsTheResult(t *testing.T) {
	srv := listBucketsFixture(t)

	tests := []struct {
		name      string
		maxdd     string
		wantNames []string
		wantToken bool
	}{
		{"one", "1", allNineBuckets[:1], true},
		{"three", "3", allNineBuckets[:3], true},
		{"eight", "8", allNineBuckets[:8], true},
		// Exactly the number of buckets: full but not truncated, so no token. AWS
		// ties the token to "there are more buckets that can be listed", not to a
		// full page.
		{"exactly nine", "9", allNineBuckets, false},
		{"more than exist", "50", allNineBuckets, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, res := listBuckets(t, srv, "max-buckets="+tc.maxdd)
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, tc.wantNames, bucketNames(res))
			if tc.wantToken {
				assert.NotEmpty(t, res.ContinuationToken, "truncated result must carry a token")
			} else {
				assert.Empty(t, res.ContinuationToken, "complete result must carry no token")
			}
		})
	}
}

// TestS3ListBuckets_ContinuationTokenPagesWithoutRepeatOrOmission walks the whole
// listing three at a time. Before the fix the token was ignored, so page two was
// page one — a paging caller looped forever or double-processed every bucket.
func TestS3ListBuckets_ContinuationTokenPagesWithoutRepeatOrOmission(t *testing.T) {
	srv := listBucketsFixture(t)

	var seen []string
	token := ""
	for page := 0; page < 10; page++ {
		query := "max-buckets=3"
		if token != "" {
			query += "&continuation-token=" + token
		}
		code, res := listBuckets(t, srv, query)
		require.Equal(t, http.StatusOK, code)
		require.LessOrEqual(t, len(res.Buckets), 3, "page %d exceeded max-buckets", page)

		seen = append(seen, bucketNames(res)...)
		token = res.ContinuationToken
		if token == "" {
			break
		}
	}

	require.Empty(t, token, "pagination did not terminate")
	// Concatenating the pages reproduces the listing exactly: same members, same
	// order, nothing repeated and nothing dropped.
	assert.Equal(t, allNineBuckets, seen)
}

// TestS3ListBuckets_ContinuationTokenIsOpaqueBase64 pins the token's encoding to the
// one ListObjectsV2 already emits, which is what satisfies AWS's only statement about
// its content: "ContinuationToken is obfuscated and is not a real key".
func TestS3ListBuckets_ContinuationTokenIsOpaqueBase64(t *testing.T) {
	srv := listBucketsFixture(t)

	code, res := listBuckets(t, srv, "max-buckets=2")
	require.Equal(t, http.StatusOK, code)
	require.NotEmpty(t, res.ContinuationToken)

	// It is not a bare bucket name on the wire...
	assert.NotEqual(t, "alpha-three", res.ContinuationToken)
	// ...but it decodes to the last bucket of the page just returned.
	decoded, err := base64.StdEncoding.DecodeString(res.ContinuationToken)
	require.NoError(t, err, "token must be base64")
	assert.Equal(t, "alpha-three", string(decoded))
}

// TestS3ListBuckets_PrefixFilters covers the prefix parameter and the Prefix echo,
// which AWS specifies as "If Prefix was sent with the request, it is included in the
// response".
func TestS3ListBuckets_PrefixFilters(t *testing.T) {
	srv := listBucketsFixture(t)

	tests := []struct {
		name      string
		prefix    string
		wantNames []string
	}{
		{"alpha", "alpha", []string{"alpha-one", "alpha-three", "alpha-two"}},
		{"beta", "beta", []string{"beta-one", "beta-three", "beta-two"}},
		{"narrower than a group", "gamma-t", []string{"gamma-three", "gamma-two"}},
		{"exact bucket name", "alpha-one", []string{"alpha-one"}},
		{"matches nothing", "delta", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, res := listBuckets(t, srv, "prefix="+tc.prefix)
			require.Equal(t, http.StatusOK, code)
			if tc.wantNames == nil {
				assert.Empty(t, res.Buckets)
			} else {
				assert.Equal(t, tc.wantNames, bucketNames(res))
			}
			assert.Equal(t, tc.prefix, res.Prefix, "Prefix must be echoed when sent")
		})
	}
}

// TestS3ListBuckets_PrefixOmittedWhenNotSent is the other half of the echo rule: the
// element is conditional on the request, not unconditional.
func TestS3ListBuckets_PrefixOmittedWhenNotSent(t *testing.T) {
	srv := listBucketsFixture(t)

	w := s3Request(t, srv, http.MethodGet, "/", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, w.Body.String(), "<Prefix>")
}

// TestS3ListBuckets_BucketRegionFilters proves the Region a bucket was created in is
// both stored and selectable. The Regions cut across the prefix groups, so a filter
// that quietly ignored the Region would return three times as many buckets.
func TestS3ListBuckets_BucketRegionFilters(t *testing.T) {
	srv := listBucketsFixture(t)

	tests := []struct {
		name      string
		region    string
		wantNames []string
	}{
		{"us-east-1", "us-east-1", []string{"alpha-one", "beta-one", "gamma-one"}},
		{"us-west-2", "us-west-2", []string{"alpha-two", "beta-two", "gamma-two"}},
		{"eu-west-1", "eu-west-1", []string{"alpha-three", "beta-three", "gamma-three"}},
		{"a Region holding none", "ap-south-1", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, res := listBuckets(t, srv, "bucket-region="+tc.region)
			require.Equal(t, http.StatusOK, code)
			if tc.wantNames == nil {
				assert.Empty(t, res.Buckets)
				return
			}
			assert.Equal(t, tc.wantNames, bucketNames(res))
			for _, b := range res.Buckets {
				assert.Equal(t, tc.region, b.BucketRegion)
			}
		})
	}
}

// TestS3ListBuckets_FiltersCompose checks the parameters narrow together rather than
// the last one winning, including the paged case.
func TestS3ListBuckets_FiltersCompose(t *testing.T) {
	srv := listBucketsFixture(t)

	t.Run("prefix and region", func(t *testing.T) {
		code, res := listBuckets(t, srv, "prefix=alpha&bucket-region=us-west-2")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []string{"alpha-two"}, bucketNames(res))
	})

	t.Run("prefix paged", func(t *testing.T) {
		code, res := listBuckets(t, srv, "prefix=beta&max-buckets=2")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []string{"beta-one", "beta-three"}, bucketNames(res))
		require.NotEmpty(t, res.ContinuationToken)

		code, res = listBuckets(t, srv, "prefix=beta&max-buckets=2&continuation-token="+res.ContinuationToken)
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []string{"beta-two"}, bucketNames(res))
		assert.Empty(t, res.ContinuationToken)
	})

	t.Run("region paged", func(t *testing.T) {
		code, res := listBuckets(t, srv, "bucket-region=us-east-1&max-buckets=2")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []string{"alpha-one", "beta-one"}, bucketNames(res))
		require.NotEmpty(t, res.ContinuationToken)

		code, res = listBuckets(t, srv, "bucket-region=us-east-1&max-buckets=2&continuation-token="+res.ContinuationToken)
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []string{"gamma-one"}, bucketNames(res))
		assert.Empty(t, res.ContinuationToken)
	})
}

// TestS3ListBuckets_OwnerIsReported covers the Owner element, which the response
// omitted entirely. AWS renders it in every published example.
func TestS3ListBuckets_OwnerIsReported(t *testing.T) {
	srv := listBucketsFixture(t)

	code, res := listBuckets(t, srv, "")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "123456789012", res.Owner.ID, "Owner.ID is substrate's account ID")

	// DisplayName is omitted: it carries no description on API_Owner and no example
	// renders it, so substrate does not invent a name for an account without one.
	w := s3Request(t, srv, http.MethodGet, "/", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "<Owner>")
	assert.NotContains(t, w.Body.String(), "<DisplayName>")
}

// TestS3ListBuckets_BucketRegionIsConditional pins API_Bucket's rule for BucketRegion:
// "If the request contains at least one valid parameter, it is included in the
// response." AWS's unpaginated Example 1 shows no BucketRegion; Examples 2-5, each of
// which passes a parameter, all show one.
func TestS3ListBuckets_BucketRegionIsConditional(t *testing.T) {
	srv := listBucketsFixture(t)

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{"no parameters", "", false},
		{"max-buckets", "max-buckets=5", true},
		{"prefix", "prefix=alpha", true},
		{"bucket-region", "bucket-region=us-east-1", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, res := listBuckets(t, srv, tc.query)
			require.Equal(t, http.StatusOK, code)
			require.NotEmpty(t, res.Buckets)
			for _, b := range res.Buckets {
				if tc.want {
					assert.NotEmpty(t, b.BucketRegion, "bucket %s", b.Name)
				} else {
					assert.Empty(t, b.BucketRegion, "bucket %s", b.Name)
				}
			}
		})
	}
}

// TestS3ListBuckets_BucketArnIsNeverReported is the deliberate non-implementation of
// #884's BucketArn criterion. API_Bucket says BucketArn "is only supported for S3
// directory buckets" and API_ListBuckets "is not supported for directory buckets", so
// no ListBuckets response AWS produces carries one. A synthesized general purpose
// bucket ARN would be a value a consumer could read here and never against AWS.
func TestS3ListBuckets_BucketArnIsNeverReported(t *testing.T) {
	srv := listBucketsFixture(t)

	for _, query := range []string{"", "max-buckets=5", "prefix=alpha", "bucket-region=us-east-1"} {
		code, res := listBuckets(t, srv, query)
		require.Equal(t, http.StatusOK, code)
		require.NotEmpty(t, res.Buckets)
		for _, b := range res.Buckets {
			assert.Empty(t, b.BucketArn, "query %q, bucket %s", query, b.Name)
		}
	}

	w := s3Request(t, srv, http.MethodGet, "/?max-buckets=5", nil, nil)
	assert.NotContains(t, w.Body.String(), "BucketArn")
}

// TestS3ListBuckets_RejectsOutOfRangeMaxBuckets enforces the published range, "Valid
// Range: Minimum value of 1. Maximum value of 10000".
//
// The code is substrate's reading: API_ListBuckets publishes no Errors section and the
// S3 ErrorResponses page could not be fetched, so InvalidArgument/400 is unverified.
// Refusing rather than clamping is the point — silently substituting a bound is the
// same invisible-substitution defect this issue is about.
func TestS3ListBuckets_RejectsOutOfRangeMaxBuckets(t *testing.T) {
	srv := listBucketsFixture(t)

	tests := []struct {
		name  string
		value string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"above the ceiling", "10001"},
		{"not a number", "many"},
		{"empty-ish whitespace", "%20"},
		{"fractional", "3.5"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/?max-buckets="+tc.value, nil, nil)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>InvalidArgument</Code>")
		})
	}
}

// TestS3ListBuckets_AcceptsTheRangeBounds is the companion to the refusals: the
// documented endpoints themselves must be accepted.
func TestS3ListBuckets_AcceptsTheRangeBounds(t *testing.T) {
	srv := listBucketsFixture(t)

	for _, value := range []string{"1", "10000"} {
		code, res := listBuckets(t, srv, "max-buckets="+value)
		require.Equal(t, http.StatusOK, code, "max-buckets=%s", value)
		assert.NotEmpty(t, res.Buckets)
	}
}

// TestS3ListBuckets_RejectsMalformedContinuationToken keeps a bad cursor from
// restarting the listing, which is the infinite loop #884 describes. A token that
// silently means "no cursor" turns a paging bug into a non-terminating one.
func TestS3ListBuckets_RejectsMalformedContinuationToken(t *testing.T) {
	srv := listBucketsFixture(t)

	tests := []struct {
		name  string
		token string
	}{
		{"not base64", "!!!not-base64!!!"},
		{"truncated base64", "YWxwaGEtb25l="},
		{"over the length ceiling", strings.Repeat("A", 1025)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/?continuation-token="+tc.token, nil, nil)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>InvalidArgument</Code>")
		})
	}
}

// TestS3ListBuckets_TokenBeyondTheEndReturnsNothing covers the cursor's tail: a token
// naming the last bucket yields an empty, untruncated page rather than wrapping.
func TestS3ListBuckets_TokenBeyondTheEndReturnsNothing(t *testing.T) {
	srv := listBucketsFixture(t)

	token := base64.StdEncoding.EncodeToString([]byte("zzzz-past-the-end"))
	code, res := listBuckets(t, srv, "max-buckets=3&continuation-token="+token)
	require.Equal(t, http.StatusOK, code)
	assert.Empty(t, res.Buckets)
	assert.Empty(t, res.ContinuationToken)
}

// TestS3ListBuckets_UnfilteredListingIsUnchanged is the compatibility gate: a caller
// passing no parameters still gets every bucket, in lexicographic order, with Name and
// CreationDate as before. Owner is the only addition to that body.
func TestS3ListBuckets_UnfilteredListingIsUnchanged(t *testing.T) {
	srv := listBucketsFixture(t)

	code, res := listBuckets(t, srv, "")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, allNineBuckets, bucketNames(res))
	for _, b := range res.Buckets {
		assert.NotEmpty(t, b.CreationDate, "bucket %s", b.Name)
	}
	assert.Empty(t, res.ContinuationToken)
}

// TestS3ListBuckets_EmptyAccountStillReportsOwner checks the degenerate case: no
// buckets is a valid listing, and it still names an owner.
func TestS3ListBuckets_EmptyAccountStillReportsOwner(t *testing.T) {
	srv, _ := newS3TestServer(t)

	code, res := listBuckets(t, srv, "max-buckets=10")
	require.Equal(t, http.StatusOK, code)
	assert.Empty(t, res.Buckets)
	assert.Empty(t, res.ContinuationToken)
	assert.Equal(t, "123456789012", res.Owner.ID)
}
