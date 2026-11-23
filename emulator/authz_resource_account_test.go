package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestAuthz_CDKBootstrapRoleInputs pins the three authorization inputs the CDK bootstrap roles'
// policies are written against (#1606): aws:ResourceAccount and s3:ResourceAccount, an SSM
// parameter's ARN, and the ARN of a bucket-level S3 request sent with a trailing slash.
func TestAuthz_CDKBootstrapRoleInputs(t *testing.T) {
	const bucket = "cdk-assets"
	inAccount := func(key, account string) map[string]map[string]emulator.StringOrSlice {
		return map[string]map[string]emulator.StringOrSlice{"StringEquals": {key: {account}}}
	}
	allow := func(action, resource string, cond map[string]map[string]emulator.StringOrSlice) emulator.PolicyDocument {
		return emulator.PolicyDocument{Version: "2012-10-17", Statement: []emulator.PolicyStatement{{
			Effect: "Allow", Action: emulator.StringOrSlice{action}, Resource: emulator.StringOrSlice{resource},
			Condition: cond,
		}}}
	}
	ssmGet := func(name string) *emulator.AWSRequest {
		body, err := json.Marshal(map[string]string{"Name": name})
		require.NoError(t, err)
		return &emulator.AWSRequest{Service: "ssm", Operation: "GetParameter", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	}
	const versionARN = "arn:aws:ssm:us-east-1:123456789012:parameter/cdk-bootstrap/hnb659fds/version"
	tests := []struct {
		name        string
		doc         emulator.PolicyDocument
		req         *emulator.AWSRequest
		bucketOwner string // the account recorded as the bucket's creator, "" for no bucket record
		denied      bool
	}{
		{name: "the file-publishing statement on an object", bucketOwner: "123456789012",
			doc: allow("s3:PutObject", "arn:aws:s3:::"+bucket+"/*", inAccount("aws:ResourceAccount", "123456789012")),
			req: &emulator.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/" + bucket + "/k"}},
		{name: "a bucket another account created", bucketOwner: "210987654321",
			doc: allow("s3:PutObject", "arn:aws:s3:::"+bucket+"/*", inAccount("aws:ResourceAccount", "123456789012")),
			req: &emulator.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/" + bucket + "/k"}, denied: true},
		{name: "s3:ResourceAccount", bucketOwner: "123456789012",
			doc: allow("s3:GetObject", "*", inAccount("s3:ResourceAccount", "123456789012")),
			req: &emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/" + bucket + "/k"}},
		{name: "a bucket with no record is the caller's",
			doc: allow("s3:GetObject", "*", inAccount("aws:ResourceAccount", "123456789012")),
			req: &emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/" + bucket + "/k"}},
		{name: "an ARN's own account",
			doc: allow("ssm:GetParameter", "*", inAccount("aws:ResourceAccount", "999999999999")),
			req: ssmGet("arn:aws:ssm:us-east-1:999999999999:parameter/shared")},
		{name: "a listing sent with a trailing slash", bucketOwner: "123456789012",
			doc: allow("s3:ListBucket", "arn:aws:s3:::"+bucket, nil),
			req: &emulator.AWSRequest{Service: "s3", Operation: "ListObjectsV2", Path: "/" + bucket + "/",
				Params: map[string]string{"list-type": "2"}}},
		{name: "the deploy role's ReadVersion statement", doc: allow("ssm:GetParameter", versionARN, nil),
			req: ssmGet("/cdk-bootstrap/hnb659fds/version")},
		{name: "another parameter", doc: allow("ssm:GetParameter", versionARN, nil),
			req: ssmGet("/cdk-bootstrap/other/version"), denied: true},
		{name: "a name with no leading slash",
			doc: allow("ssm:GetParameter", "arn:aws:ssm:us-east-1:123456789012:parameter/flat", nil), req: ssmGet("flat")},
		{name: "a query-form Name",
			doc: allow("ssm:GetParameter", "arn:aws:ssm:us-east-1:123456789012:parameter/flat", nil),
			req: &emulator.AWSRequest{Service: "ssm", Operation: "GetParameter", Params: map[string]string{"Name": "flat"}}},
		{name: "a public parameter's ARN names no account",
			doc: allow("ssm:GetParameter", "arn:aws:ssm:*::parameter/aws/service/*", nil),
			req: ssmGet("/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64")},
		{name: "a public parameter is not the caller's",
			doc: allow("ssm:GetParameter", "*", inAccount("aws:ResourceAccount", "123456789012")),
			req: ssmGet("/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"), denied: true},
		{name: "a request naming no parameter is *",
			doc: allow("ssm:DescribeParameters", versionARN, nil),
			req: &emulator.AWSRequest{Service: "ssm", Operation: "DescribeParameters", Body: []byte(`{}`)}, denied: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const policyARN = "arn:aws:iam::123456789012:policy/cdk-policy"
			state := newAuthTestState(t, "cdk", policyARN, tc.doc)
			if tc.bucketOwner != "" {
				raw, err := json.Marshal(emulator.S3Bucket{Name: bucket, AccountID: tc.bucketOwner, Region: "us-east-1"})
				require.NoError(t, err)
				require.NoError(t, state.Put(context.Background(), "s3", "bucket:"+bucket, raw))
			}
			if tc.req.Headers == nil {
				tc.req.Headers = map[string]string{}
			}
			if tc.req.Params == nil {
				tc.req.Params = map[string]string{}
			}
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
			err := auth.CheckAccess(newAuthTestReqCtx("arn:aws:iam::123456789012:user/cdk"), tc.req)
			if tc.denied {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestS3_GetBucketLocation pins GetBucketLocation (#1606): a bucket's Region, null — an empty
// element — for us-east-1, and NoSuchBucket for a bucket that does not exist.
func TestS3_GetBucketLocation(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/located-default", nil, nil).Code)
	// A bucket is created in the Region the request was sent to.
	r := httptest.NewRequest(http.MethodPut, "http://s3.eu-west-1.amazonaws.com/located-eu", nil)
	r.Host = "s3.eu-west-1.amazonaws.com"
	created := httptest.NewRecorder()
	srv.ServeHTTP(created, r)
	require.Equal(t, http.StatusOK, created.Code, created.Body.String())

	w := s3Request(t, srv, http.MethodGet, "/located-default?location", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)

	w = s3Request(t, srv, http.MethodGet, "/located-eu?location=", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `>eu-west-1</LocationConstraint>`)

	w = s3Request(t, srv, http.MethodGet, "/located-nowhere?location", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>NoSuchBucket</Code>")
}
