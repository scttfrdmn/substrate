package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestParseAWSRequest_NilRequest(t *testing.T) {
	_, _, err := emulator.ParseAWSRequest(nil)
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
			name:        "Budgets AmazonBudgetServiceGateway",
			target:      "AmazonBudgetServiceGateway.DescribeBudgets",
			wantService: "budgets",
		},
		{
			name:        "Budgets AWSBudgetServiceGateway",
			target:      "AWSBudgetServiceGateway.CreateBudget",
			wantService: "budgets",
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

			req, _, err := emulator.ParseAWSRequest(r)
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

			req, _, err := emulator.ParseAWSRequest(r)
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

			_, reqCtx, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRegion, reqCtx.Region)
		})
	}
}

// TestExtractRegionFromHost covers every host layout the parser recognizes, plus
// the layouts it must refuse to guess at.
//
// The refusals are the point of #403. The function used to fall through to a
// "<service>.<region>" assumption and return the second label of whatever it was
// given, so "api.pricing.us-east-1.amazonaws.com" yielded "pricing" — a service
// name presented as a region, indistinguishable to the caller from a real one.
func TestExtractRegionFromHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		host string
		want string
	}{
		// The reported bug: "api.<service>.<region>".
		{"api-prefixed pricing host", "api.pricing.us-east-1.amazonaws.com", "us-east-1"},
		{"api-prefixed pricing ap-south-1", "api.pricing.ap-south-1.amazonaws.com", "ap-south-1"},

		// Region shapes that must all be accepted. Codes taken from the region
		// table in the AWS General Reference.
		{"four-segment gov partition", "ec2.us-gov-west-1.amazonaws.com", "us-gov-west-1"},
		{"long compass word", "ec2.ap-southeast-4.amazonaws.com", "ap-southeast-4"},
		{"il geography", "ec2.il-central-1.amazonaws.com", "il-central-1"},
		{"mx geography", "ec2.mx-central-1.amazonaws.com", "mx-central-1"},
		{"cn geography", "ec2.cn-northwest-1.amazonaws.com", "cn-northwest-1"},

		// Pre-existing layouts, which must keep parsing as they did.
		{"path-style s3 regional", "s3.us-west-2.amazonaws.com", "us-west-2"},
		{"virtual-hosted s3 regional", "mybucket.s3.us-east-1.amazonaws.com", "us-east-1"},
		{"dotted bucket virtual-hosted", "my.bucket.s3.us-west-2.amazonaws.com", "us-west-2"},
		{"execute-api runtime", "abc123.execute-api.us-east-1.amazonaws.com", "us-east-1"},
		{"plain service regional", "dynamodb.ap-southeast-1.amazonaws.com", "ap-southeast-1"},
		{"regional host with port", "dynamodb.eu-west-1.amazonaws.com:443", "eu-west-1"},

		// Hosts that carry the region in the second label for reasons of their own
		// rather than by the "<service>.<region>" convention. The shape check
		// passes them through, which is the right answer.
		{"elb dns name", "my-lb-1234.us-east-1.elb.amazonaws.com", "us-east-1"},
		{"opensearch domain", "my-domain.us-east-1.es.amazonaws.com", "us-east-1"},

		// Layouts with no region to find: the function must yield "" so the
		// caller falls back rather than acting on a guess.
		{"global s3", "s3.amazonaws.com", ""},
		{"bare service host", "iam.amazonaws.com", ""},
		{"virtual-hosted s3 global", "mybucket.s3.amazonaws.com", ""},
		{"api-prefixed global host", "api.pricing.amazonaws.com", ""},
		{"not an aws host", "localhost:4566", ""},
		{"empty host", "", ""},

		// Near-misses on the region shape, each failing a different check.
		{"two segments only", "svc.us-east.amazonaws.com", ""},
		{"five segments", "svc.us-gov-iso-east-1.amazonaws.com", ""},
		{"three-letter geography", "svc.usa-east-1.amazonaws.com", ""},
		{"non-numeric ordinal", "svc.us-east-one.amazonaws.com", ""},
		{"digits in compass word", "svc.us-e4st-1.amazonaws.com", ""},
		{"uppercase region", "svc.US-EAST-1.amazonaws.com", ""},
		{"trailing hyphen", "svc.us-east-.amazonaws.com", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, emulator.ExtractRegionFromHostForTest(tt.host))
		})
	}
}

// TestExtractRegion_APIPrefixedHost asserts the fix reaches the RequestContext a
// plugin actually sees, not just the helper. A pricing client signs for
// us-east-1, so the SigV4 fallback would mask a parser that still returned
// "pricing" — this sends no Authorization header so the host is the only signal.
func TestExtractRegion_APIPrefixedHost(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
	r.Host = "api.pricing.us-east-1.amazonaws.com"

	_, reqCtx, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", reqCtx.Region)
}

// TestExtractRegion_UnparseableHostFallsBackToAuth is the payoff of failing
// closed: because the host no longer yields a bogus region, the SigV4 credential
// scope — which is authoritative — gets its turn.
func TestExtractRegion_UnparseableHostFallsBackToAuth(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
	r.Host = "api.pricing.amazonaws.com"
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/ap-south-1/pricing/aws4_request, "+
			"SignedHeaders=host, Signature=abc")

	_, reqCtx, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "ap-south-1", reqCtx.Region)
}

// TestExtractService_APIPrefixedHost covers the service half of the same host
// layout (#401). Taking the first label of "api.pricing.us-east-1" yielded a
// service literally named "api", which routes to no plugin at all — so these
// endpoints were reachable only via their X-Amz-Target namespace. No
// Authorization header is sent, because the credential scope also names the
// service and would mask a parser that still returned "api".
func TestExtractService_APIPrefixedHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		host string
		want string
	}{
		{"pricing regional", "api.pricing.us-east-1.amazonaws.com", "pricing"},
		{"pricing global", "api.pricing.amazonaws.com", "pricing"},
		// Two other services that front their endpoint with a literal "api"
		// label, both previously reachable only by X-Amz-Target.
		{"ecr regional", "api.ecr.us-west-2.amazonaws.com", "ecr"},
		{"sagemaker regional", "api.sagemaker.eu-west-1.amazonaws.com", "sagemaker"},
		// A service whose own name merely begins with "api" must not be
		// truncated: the prefix stripped is the label "api.", not the letters.
		{"apigateway is not api-prefixed", "apigateway.us-east-1.amazonaws.com", "apigateway"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
			r.Host = tt.host

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.want, req.Service)
		})
	}
}

// TestExtractService_PriceListTarget pins the X-Amz-Target namespace both
// aws-sdk-go-v2 and boto3 use for the Price List Query API. Its SigV4 signing
// name is "pricing", which is the plugin's own name, so the alias is what closes
// the gap between the two spellings.
func TestExtractService_PriceListTarget(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
	r.Host = "localhost:4566"
	r.Header.Set("X-Amz-Target", "AWSPriceListService.GetProducts")

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "pricing", req.Service)
	assert.Equal(t, "GetProducts", req.Operation)
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

			_, reqCtx, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAccount, reqCtx.AccountID)
		})
	}
}

func TestParseAWSRequest_Headers(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
	r.Header.Set("X-Amz-Target", "AmazonDynamoDB.GetItem")
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")

	req, reqCtx, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "application/x-amz-json-1.0", req.Headers["Content-Type"])
	assert.NotEmpty(t, reqCtx.RequestID)
	assert.False(t, reqCtx.Timestamp.IsZero())
}

func TestParseAWSRequest_Params(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "http://localhost/?Action=DescribeInstances&Version=2016-11-15", nil)

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "DescribeInstances", req.Params["Action"])
	assert.Equal(t, "2016-11-15", req.Params["Version"])
	assert.Equal(t, "DescribeInstances", req.Operation)
}

func TestParseAWSRequest_BareQueryKey(t *testing.T) {
	// ?uploads is a bare key with no value — must map to "1".
	r := httptest.NewRequest(http.MethodGet, "http://s3.amazonaws.com/mybucket/mykey?uploads", nil)
	r.Host = "s3.amazonaws.com"

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "1", req.Params["uploads"])
}

func TestParseAWSRequest_EmptyValueQueryParam(t *testing.T) {
	// Keys with an explicit empty value (e.g. ?prefix=) must be preserved as ""
	// and must NOT be converted to "1" (issue #200).
	r := httptest.NewRequest(http.MethodGet,
		"http://s3.amazonaws.com/mybucket?list-type=2&prefix=&delimiter=%2F", nil)
	r.Host = "s3.amazonaws.com"

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "", req.Params["prefix"], "explicit empty prefix must be preserved as empty string")
	assert.Equal(t, "/", req.Params["delimiter"])
	assert.Equal(t, "2", req.Params["list-type"])
}

func TestParseAWSRequest_SmithyRPCV2(t *testing.T) {
	// AWS SDK Go v2 cloudwatch v1.55+ sends GetMetricData via rpc-v2-cbor:
	//   POST /service/GraniteServiceVersion20100801/operation/GetMetricData
	//   Smithy-Protocol: rpc-v2-cbor
	//   Content-Type: application/cbor
	r := httptest.NewRequest(http.MethodPost,
		"http://localhost:4566/service/GraniteServiceVersion20100801/operation/GetMetricData",
		nil)
	r.Host = "localhost:4566"
	r.Header.Set("Smithy-Protocol", "rpc-v2-cbor")
	r.Header.Set("Content-Type", "application/cbor")
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20240101/us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=fake")

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "monitoring", req.Service, "Smithy service ID should resolve to 'monitoring'")
	assert.Equal(t, "GetMetricData", req.Operation, "operation should be extracted from URL path")
}

func TestParseAWSRequest_S3VirtualHosted(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		urlPath     string
		wantService string
		wantPath    string
		wantRegion  string
	}{
		{
			name:        "virtual-hosted simple",
			host:        "mybucket.s3.amazonaws.com",
			urlPath:     "/mykey.txt",
			wantService: "s3",
			wantPath:    "/mybucket/mykey.txt",
			wantRegion:  "us-east-1",
		},
		{
			name:        "virtual-hosted regional",
			host:        "mybucket.s3.us-west-2.amazonaws.com",
			urlPath:     "/data/file.json",
			wantService: "s3",
			wantPath:    "/mybucket/data/file.json",
			wantRegion:  "us-west-2",
		},
		{
			name:        "virtual-hosted bucket root",
			host:        "mybucket.s3.amazonaws.com",
			urlPath:     "/",
			wantService: "s3",
			wantPath:    "/mybucket/",
			wantRegion:  "us-east-1",
		},
		{
			name:        "path-style unchanged",
			host:        "s3.us-east-1.amazonaws.com",
			urlPath:     "/mybucket/mykey",
			wantService: "s3",
			wantPath:    "/mybucket/mykey",
			wantRegion:  "us-east-1",
		},
		{
			name:        "global path-style unchanged",
			host:        "s3.amazonaws.com",
			urlPath:     "/mybucket/mykey",
			wantService: "s3",
			wantPath:    "/mybucket/mykey",
			wantRegion:  "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "http://"+tt.host+tt.urlPath, nil)
			r.Host = tt.host

			req, reqCtx, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantService, req.Service)
			assert.Equal(t, tt.wantPath, req.Path)
			assert.Equal(t, tt.wantRegion, reqCtx.Region)
		})
	}
}

// TestParseAWSRequest_S3CustomEndpoint verifies that S3 requests to a single
// base-endpoint URL are correctly identified and that virtual-hosted style
// paths are normalised even when the host is not amazonaws.com.
// This covers the AWS SDK v2 config.WithBaseEndpoint pattern (issue #191).
func TestParseAWSRequest_S3CustomEndpoint(t *testing.T) {
	t.Parallel()
	const s3Auth = "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc"

	tests := []struct {
		name        string
		host        string
		urlPath     string
		wantService string
		wantPath    string
	}{
		{
			// AWS SDK v2 virtual-hosted style: bucket prepended to base endpoint host.
			name:        "virtual-hosted custom endpoint",
			host:        "my-bucket.localhost:4566",
			urlPath:     "/my-key.txt",
			wantService: "s3",
			wantPath:    "/my-bucket/my-key.txt",
		},
		{
			// Bucket-root request (no key).
			name:        "virtual-hosted bucket root",
			host:        "my-bucket.localhost:4566",
			urlPath:     "/",
			wantService: "s3",
			wantPath:    "/my-bucket/",
		},
		{
			// Path-style with custom endpoint: bucket in path, bare host.
			name:        "path-style custom endpoint",
			host:        "localhost:4566",
			urlPath:     "/my-bucket/my-key.txt",
			wantService: "s3",
			wantPath:    "/my-bucket/my-key.txt",
		},
		{
			// Multi-label emulator host (e.g. substrate.local:4566).
			name:        "virtual-hosted multi-label emulator host",
			host:        "my-bucket.substrate.local:4566",
			urlPath:     "/obj",
			wantService: "s3",
			wantPath:    "/my-bucket/obj",
		},
		{
			// IPv4 loopback — dots must NOT be mistaken for a virtual-hosted bucket.
			// Regression test for #213.
			name:        "path-style IPv4 loopback",
			host:        "127.0.0.1:4566",
			urlPath:     "/my-bucket/my-key.txt",
			wantService: "s3",
			wantPath:    "/my-bucket/my-key.txt",
		},
		{
			// IPv4 without port.
			name:        "path-style IPv4 no port",
			host:        "192.168.1.1",
			urlPath:     "/bucket/key",
			wantService: "s3",
			wantPath:    "/bucket/key",
		},
		{
			// IPv6 loopback in bracket notation.
			name:        "path-style IPv6 loopback",
			host:        "[::1]:4566",
			urlPath:     "/my-bucket/key",
			wantService: "s3",
			wantPath:    "/my-bucket/key",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPut, "http://"+tt.host+tt.urlPath, nil)
			r.Host = tt.host
			r.Header.Set("Authorization", s3Auth)

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantService, req.Service)
			assert.Equal(t, tt.wantPath, req.Path)
		})
	}
}

// TestParseAWSRequest_ServiceFromSigV4Auth verifies that when a client sends
// requests to a single base-endpoint URL (Host: localhost:4566) without a
// service-specific hostname, the service is still correctly identified via the
// SigV4 credential scope in the Authorization header.  This is the common
// config.WithBaseEndpoint integration-test pattern.
func TestParseAWSRequest_ServiceFromSigV4Auth(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		authScope   string // "<key>/<date>/<region>/<service>/aws4_request"
		wantService string
	}{
		{"sts", "AKIATEST12345678901/20240101/us-east-1/sts/aws4_request", "sts"},
		{"ec2", "AKIATEST12345678901/20240101/us-east-1/ec2/aws4_request", "ec2"},
		{"iam", "AKIATEST12345678901/20240101/us-east-1/iam/aws4_request", "iam"},
		{"monitoring (CloudWatch)", "AKIATEST12345678901/20240101/us-east-1/monitoring/aws4_request", "monitoring"},
		{"logs (CWLogs)", "AKIATEST12345678901/20240101/us-east-1/logs/aws4_request", "logs"},
		{"elasticloadbalancing", "AKIATEST12345678901/20240101/us-east-1/elasticloadbalancing/aws4_request", "elasticloadbalancing"},
		{"elasticfilesystem→efs", "AKIATEST12345678901/20240101/us-east-1/elasticfilesystem/aws4_request", "efs"},
		{"ses→sesv2", "AKIATEST12345678901/20240101/us-east-1/ses/aws4_request", "sesv2"},
		{"kafka→msk", "AKIATEST12345678901/20240101/us-east-1/kafka/aws4_request", "msk"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "http://localhost:4566/", nil)
			// Host is the emulator address, not a service-specific hostname.
			r.Host = "localhost:4566"
			r.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential="+tt.authScope+", SignedHeaders=content-type;host;x-amz-date, Signature=abc123")
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantService, req.Service)
		})
	}
}

func TestNormalizeS3VirtualHost(t *testing.T) {
	tests := []struct {
		host       string
		urlPath    string
		wantBucket string
		wantPath   string
		wantOK     bool
	}{
		{"mybucket.s3.amazonaws.com", "/key", "mybucket", "/mybucket/key", true},
		{"mybucket.s3.us-east-1.amazonaws.com", "/k/p", "mybucket", "/mybucket/k/p", true},
		{"my.bucket.s3.amazonaws.com", "/obj", "my.bucket", "/my.bucket/obj", true},
		{"s3.amazonaws.com", "/bucket/key", "", "", false},
		{"s3.us-east-1.amazonaws.com", "/bucket/key", "", "", false},
		{"iam.amazonaws.com", "/", "", "", false},
		{"dynamodb.us-east-1.amazonaws.com", "/", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.host+tt.urlPath, func(t *testing.T) {
			bucket, normPath, ok := emulator.NormalizeS3VirtualHostForTest(tt.host, tt.urlPath)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantPath, normPath)
		})
	}
}
