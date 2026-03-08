package substrate_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestParseAWSRequest_NilRequest(t *testing.T) {
	_, _, err := substrate.ParseAWSRequest(nil)
	require.Error(t, err)
}

func TestParseAWSRequest_Service(t *testing.T) {
	tests := []struct {
		name        string
		target      string
		host        string
		path        string
		wantService string
	}{
		{
			name:        "X-Amz-Target Amazon prefix",
			target:      "AmazonDynamoDB.GetItem",
			wantService: "dynamodb",
		},
		{
			name:        "X-Amz-Target versioned namespace",
			target:      "DynamoDB_20120810.GetItem",
			wantService: "dynamodb",
		},
		{
			name:        "X-Amz-Target SQS",
			target:      "AmazonSQS.SendMessage",
			wantService: "sqs",
		},
		{
			name:        "Host regional",
			host:        "s3.us-west-2.amazonaws.com",
			wantService: "s3",
		},
		{
			name:        "Host global",
			host:        "iam.amazonaws.com",
			wantService: "iam",
		},
		{
			name:        "Host with port",
			host:        "sts.amazonaws.com:443",
			wantService: "sts",
		},
		{
			name:        "URL path prefix",
			path:        "/service/lambda/2015-03-31/functions",
			wantService: "lambda",
		},
		{
			name:        "no signal",
			wantService: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.path
			if path == "" {
				path = "/"
			}
			r := httptest.NewRequest(http.MethodPost, "http://localhost"+path, nil)
			if tt.target != "" {
				r.Header.Set("X-Amz-Target", tt.target)
			}
			if tt.host != "" {
				r.Host = tt.host
			}

			req, _, err := substrate.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantService, req.Service)
		})
	}
}

func TestParseAWSRequest_Operation(t *testing.T) {
	tests := []struct {
		name          string
		target        string
		actionParam   string
		method        string
		wantOperation string
	}{
		{
			name:          "from X-Amz-Target",
			target:        "AmazonDynamoDB.GetItem",
			method:        http.MethodPost,
			wantOperation: "GetItem",
		},
		{
			name:          "from Action query param",
			actionParam:   "DescribeInstances",
			method:        http.MethodPost,
			wantOperation: "DescribeInstances",
		},
		{
			name:          "fallback to HTTP method",
			method:        http.MethodGet,
			wantOperation: http.MethodGet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "http://localhost/"
			if tt.actionParam != "" {
				url = "http://localhost/?Action=" + tt.actionParam
			}
			method := tt.method
			if method == "" {
				method = http.MethodPost
			}
			r := httptest.NewRequest(method, url, nil)
			if tt.target != "" {
				r.Header.Set("X-Amz-Target", tt.target)
			}

			req, _, err := substrate.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantOperation, req.Operation)
		})
	}
}

func TestParseAWSRequest_Region(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		authHeader string
		wantRegion string
	}{
		{
			name:       "from Host regional",
			host:       "dynamodb.ap-southeast-1.amazonaws.com",
			wantRegion: "ap-southeast-1",
		},
		{
			name:       "from Authorization SigV4",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/eu-west-1/s3/aws4_request, SignedHeaders=host, Signature=abc",
			wantRegion: "eu-west-1",
		},
		{
			name:       "global host no region → default",
			host:       "iam.amazonaws.com",
			wantRegion: "us-east-1",
		},
		{
			name:       "no signal → default",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
			if tt.host != "" {
				r.Host = tt.host
			}
			if tt.authHeader != "" {
				r.Header.Set("Authorization", tt.authHeader)
			}

			_, reqCtx, err := substrate.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRegion, reqCtx.Region)
		})
	}
}

func TestParseAWSRequest_Account(t *testing.T) {
	tests := []struct {
		name        string
		authHeader  string
		wantAccount string
	}{
		{
			name:        "test AKIA key",
			authHeader:  "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
			wantAccount: "123456789012",
		},
		{
			name:        "no auth → fallback",
			wantAccount: "000000000000",
		},
		{
			name:        "non-AKIA key → fallback",
			authHeader:  "AWS4-HMAC-SHA256 Credential=ASIAXYZ/20130524/us-east-1/s3/aws4_request",
			wantAccount: "000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
			if tt.authHeader != "" {
				r.Header.Set("Authorization", tt.authHeader)
			}

			_, reqCtx, err := substrate.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAccount, reqCtx.AccountID)
		})
	}
}

func TestParseAWSRequest_Headers(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
	r.Header.Set("X-Amz-Target", "AmazonDynamoDB.GetItem")
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")

	req, reqCtx, err := substrate.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "application/x-amz-json-1.0", req.Headers["Content-Type"])
	assert.NotEmpty(t, reqCtx.RequestID)
	assert.False(t, reqCtx.Timestamp.IsZero())
}

func TestParseAWSRequest_Params(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "http://localhost/?Action=DescribeInstances&Version=2016-11-15", nil)

	req, _, err := substrate.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "DescribeInstances", req.Params["Action"])
	assert.Equal(t, "2016-11-15", req.Params["Version"])
	assert.Equal(t, "DescribeInstances", req.Operation)
}
