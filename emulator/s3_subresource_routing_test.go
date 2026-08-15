package emulator_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// These are #656's gates: every S3 subresource marker in the shape a real client
// sends it, "?policy=" — a key with an empty value — rather than the bare "?policy"
// the AWS documentation writes and substrate's own tests had always hand-built.
//
// Seven routing tests compared the marker against parser.go's bare-key sentinel
// ("1"), which aws-sdk-go-v2 never produces, so every SDK bucket-policy and ACL call
// fell through to its arm's default: PutBucketPolicy became CreateBucket,
// GetBucketPolicy became a list, and DeleteBucketPolicy became **DeleteBucket** —
// clearing a policy destroyed the bucket. Every unit test passed throughout, because
// each set Params{"policy": "1"} itself and so asserted the one shape no client sends.
//
// The requests here go through Server.ServeHTTP so the parser produces the params,
// which is the only level at which the two shapes differ at all.

// s3SubresourceQueries is the marker shape captured from aws-sdk-go-v2
// service/s3 v1.107.0 against an httptest server: PutBucketPolicy sends
// RawQuery="policy=", GetBucketAcl sends "acl=", and so on. The bare form is kept
// alongside it because the AWS documentation writes markers bare, some clients send
// them that way, and both must route identically.
var s3SubresourceQueries = []struct {
	name  string
	query string
}{
	{"empty value, as aws-sdk-go-v2 sends it", "="},
	{"bare key, as the AWS docs write it", ""},
}

// TestS3_BucketPolicyRoundTripsThroughEverySubresourceShape asserts the whole
// policy lifecycle, and that the bucket survives the delete.
//
// The survival assertion is the point: a DeleteBucketPolicy misrouted to DeleteBucket
// answers 204 either way, so nothing in the response distinguishes the data-loss path
// from the correct one. Only a later HeadBucket does.
func TestS3_BucketPolicyRoundTripsThroughEverySubresourceShape(t *testing.T) {
	const policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"config.amazonaws.com"},"Action":"s3:PutObject",` +
		`"Resource":"arn:aws:s3:::b/*"}]}`

	for _, shape := range s3SubresourceQueries {
		t.Run(shape.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			bucket := "policy-bucket"
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)

			marker := "/" + bucket + "?policy" + shape.query

			put := s3Request(t, srv, http.MethodPut, marker, []byte(policy),
				map[string]string{"Content-Type": "application/json"})
			assert.Equal(t, http.StatusNoContent, put.Code,
				"PutBucketPolicy: a 409 BucketAlreadyExists here means the request "+
					"was routed as CreateBucket (#656): %s", put.Body.String())

			get := s3Request(t, srv, http.MethodGet, marker, nil, nil)
			require.Equal(t, http.StatusOK, get.Code, get.Body.String())
			assert.JSONEq(t, policy, get.Body.String(),
				"GetBucketPolicy must answer the stored policy, not an object listing")

			del := s3Request(t, srv, http.MethodDelete, marker, nil, nil)
			assert.Equal(t, http.StatusNoContent, del.Code, del.Body.String())

			head := s3Request(t, srv, http.MethodHead, "/"+bucket, nil, nil)
			assert.Equal(t, http.StatusOK, head.Code,
				"the bucket must survive DeleteBucketPolicy — a 404 here is #656's "+
					"data-loss path, where clearing a policy deleted the bucket")

			gone := s3Request(t, srv, http.MethodGet, marker, nil, nil)
			assert.Equal(t, http.StatusNotFound, gone.Code,
				"and the policy is really gone: NoSuchBucketPolicy")
		})
	}
}

// TestS3_BucketACLRoutesThroughEverySubresourceShape covers the other pair of
// misrouted markers. A misrouted PutBucketAcl reached CreateBucket, so it answered
// 409 rather than storing anything.
func TestS3_BucketACLRoutesThroughEverySubresourceShape(t *testing.T) {
	for _, shape := range s3SubresourceQueries {
		t.Run(shape.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			bucket := "acl-bucket"
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)

			marker := "/" + bucket + "?acl" + shape.query

			put := s3Request(t, srv, http.MethodPut, marker, nil,
				map[string]string{"X-Amz-Acl": "public-read"})
			assert.Equal(t, http.StatusOK, put.Code,
				"PutBucketAcl: a 409 here means the request was routed as CreateBucket "+
					"(#656): %s", put.Body.String())

			get := s3Request(t, srv, http.MethodGet, marker, nil, nil)
			require.Equal(t, http.StatusOK, get.Code, get.Body.String())
			assert.Contains(t, get.Body.String(), "AccessControlPolicy",
				"GetBucketAcl must answer an ACL document, not an object listing")
		})
	}
}

// TestS3_ObjectACLRoutesThroughEverySubresourceShape covers the two object-level
// markers, whose fall-throughs are quieter than the bucket ones: a misrouted
// PutObjectAcl overwrites the object with the ACL request's body, and a misrouted
// GetObjectAcl answers the object's own bytes.
func TestS3_ObjectACLRoutesThroughEverySubresourceShape(t *testing.T) {
	for _, shape := range s3SubresourceQueries {
		t.Run(shape.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			bucket, key := "objacl-bucket", "obj.txt"
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
				"/"+bucket+"/"+key, []byte("payload"), nil).Code)

			marker := "/" + bucket + "/" + key + "?acl" + shape.query

			put := s3Request(t, srv, http.MethodPut, marker, nil,
				map[string]string{"X-Amz-Acl": "public-read"})
			assert.Equal(t, http.StatusOK, put.Code, put.Body.String())

			get := s3Request(t, srv, http.MethodGet, marker, nil, nil)
			require.Equal(t, http.StatusOK, get.Code, get.Body.String())
			assert.Contains(t, get.Body.String(), "AccessControlPolicy",
				"GetObjectAcl must answer an ACL document, not the object's bytes")

			// The object itself is untouched: a misrouted PutObjectAcl is a PutObject,
			// which would have replaced the payload with the ACL request's empty body.
			body := s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key, nil, nil)
			require.Equal(t, http.StatusOK, body.Code)
			assert.Equal(t, "payload", body.Body.String(),
				"the object's bytes must survive PutObjectAcl")
		})
	}
}

// TestS3_SubresourceMarkersAreNotValueSensitive is the guard against the class
// rather than the instance: no subresource marker's *value* may change how a request
// routes, whatever a client puts there. A test that only covered "=" and the bare
// form would still pass if a future comparison were written against "" specifically.
func TestS3_SubresourceMarkersAreNotValueSensitive(t *testing.T) {
	markers := []string{"policy", "acl", "tagging", "versioning", "notification",
		"lifecycle", "publicAccessBlock", "uploads", "versions"}
	// Values a client has no reason to send, precisely so that anything reading them
	// is caught. "1" is included because it was the sentinel the seven broken tests
	// compared against, so a partially-reverted fix shows up here.
	values := []string{"", "=", "=1", "=true", "=x"}

	for _, marker := range markers {
		srv, _ := newS3TestServer(t)
		bucket := "marker-" + strings.ToLower(marker)
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)

		codes := make(map[string]int, len(values))
		for _, v := range values {
			got := s3Request(t, srv, http.MethodGet, "/"+bucket+"?"+marker+v, nil, nil)
			codes[v] = got.Code
		}
		for _, v := range values[1:] {
			assert.Equal(t, codes[values[0]], codes[v],
				"?%s%s routes differently from the bare ?%s (%d vs %d) — a marker's "+
					"value must not affect routing", marker, v, marker,
				codes[values[0]], codes[v])
		}
	}
}

// TestS3_ListTypeIsStillValueSensitive is the counter-case that keeps the rule above
// honest. list-type is not a subresource marker but a real parameter whose value
// selects the operation — "?list-type=2" is ListObjectsV2 and its absence is
// ListObjects — so it is the one query key routing may read, and the fix must not
// have flattened it.
func TestS3_ListTypeIsStillValueSensitive(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/list-bucket", nil, nil).Code)

	v2 := s3Request(t, srv, http.MethodGet, "/list-bucket?list-type=2", nil, nil)
	require.Equal(t, http.StatusOK, v2.Code)
	assert.Contains(t, v2.Body.String(), "KeyCount",
		"ListObjectsV2 carries KeyCount; ListObjects does not")

	v1 := s3Request(t, srv, http.MethodGet, "/list-bucket", nil, nil)
	require.Equal(t, http.StatusOK, v1.Code)
	assert.NotContains(t, v1.Body.String(), "KeyCount",
		"a request with no list-type is ListObjects")

	// The case that actually separates a value test from a presence test: "2" is the
	// only value the S3 API defines for list-type, so any other value is not
	// ListObjectsV2. Without this, a presence test passes both assertions above —
	// which is how a mutant flattening list-type to presence survived the first
	// version of this test.
	for _, other := range []string{"", "1", "3"} {
		got := s3Request(t, srv, http.MethodGet, "/list-bucket?list-type="+other, nil, nil)
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.NotContains(t, got.Body.String(), "KeyCount",
			"?list-type=%q is not ListObjectsV2: 2 is the only value the API defines, "+
				"so this key is read by value where a subresource marker is read by "+
				"presence", other)
	}
}

// TestS3_BucketPolicyRoutesFromAnInProcessRequest keeps the in-process path working
// too. A caller building an AWSRequest by hand — the CloudFormation deployer does,
// and so does every plugin reading another service's state — sets the marker itself,
// and both the sentinel it used to have to send and an empty value must route.
func TestS3_BucketPolicyRoutesFromAnInProcessRequest(t *testing.T) {
	for _, value := range []string{"1", ""} {
		registry := emulator.NewPluginRegistry()
		registry.Register(newS3PluginDirect(t))

		reqCtx := &emulator.RequestContext{
			RequestID: "inproc-request",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		_, err := registry.RouteRequest(reqCtx, &emulator.AWSRequest{
			Service: "s3", Operation: http.MethodPut, Path: "/inproc-bucket",
			Headers: map[string]string{}, Params: map[string]string{},
		})
		require.NoError(t, err)

		_, err = registry.RouteRequest(reqCtx, &emulator.AWSRequest{
			Service: "s3", Operation: http.MethodPut, Path: "/inproc-bucket",
			Body:    []byte(`{"Version":"2012-10-17","Statement":[]}`),
			Headers: map[string]string{"Content-Type": "application/json"},
			Params:  map[string]string{"policy": value},
		})
		require.NoError(t, err, "an in-process PutBucketPolicy with policy=%q", value)

		resp, err := registry.RouteRequest(reqCtx, &emulator.AWSRequest{
			Service: "s3", Operation: http.MethodGet, Path: "/inproc-bucket",
			Headers: map[string]string{}, Params: map[string]string{"policy": value},
		})
		require.NoError(t, err)
		assert.Contains(t, string(resp.Body), "2012-10-17",
			"the policy reads back with policy=%q", value)
	}
}
