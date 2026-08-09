package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestParseAWSRequest_S3SemanticOperation covers #480 finding 1. S3 supplies none
// of extractOperation's first three signals, so every S3 request used to enter the
// pipeline named after its HTTP method and only acquired a semantic name inside the
// plugin — one step after fault injection, cost and consistency had already read it.
// A fault rule naming PutObject matched nothing while one naming PUT also took out
// UploadPart, and the two cannot be told apart from the caller's side.
func TestParseAWSRequest_S3SemanticOperation(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		url     string
		headers map[string]string
		want    string
	}{
		{"put object", http.MethodPut, "http://s3.amazonaws.com/b/k", nil, "PutObject"},
		{"get object", http.MethodGet, "http://s3.amazonaws.com/b/k", nil, "GetObject"},
		{"head object", http.MethodHead, "http://s3.amazonaws.com/b/k", nil, "HeadObject"},
		{"delete object", http.MethodDelete, "http://s3.amazonaws.com/b/k", nil, "DeleteObject"},
		{"create bucket", http.MethodPut, "http://s3.amazonaws.com/b", nil, "CreateBucket"},
		{"head bucket", http.MethodHead, "http://s3.amazonaws.com/b", nil, "HeadBucket"},
		{"list buckets", http.MethodGet, "http://s3.amazonaws.com/", nil, "ListBuckets"},
		{"list objects v2", http.MethodGet, "http://s3.amazonaws.com/b?list-type=2", nil, "ListObjectsV2"},
		// The multipart family: three sub-operations sharing a method and a path,
		// separated only by a query parameter. These are what a rule on the bare
		// method could not distinguish.
		{"create multipart upload", http.MethodPost, "http://s3.amazonaws.com/b/k?uploads", nil, "CreateMultipartUpload"},
		{"upload part", http.MethodPut, "http://s3.amazonaws.com/b/k?partNumber=1&uploadId=u", nil, "UploadPart"},
		{"complete multipart upload", http.MethodPost, "http://s3.amazonaws.com/b/k?uploadId=u", nil, "CompleteMultipartUpload"},
		{"abort multipart upload", http.MethodDelete, "http://s3.amazonaws.com/b/k?uploadId=u", nil, "AbortMultipartUpload"},
		{
			name:    "copy object",
			method:  http.MethodPut,
			url:     "http://s3.amazonaws.com/b/k",
			headers: map[string]string{"X-Amz-Copy-Source": "/src/k"},
			want:    "CopyObject",
		},
		{"put object acl", http.MethodPut, "http://s3.amazonaws.com/b/k?acl", nil, "PutObjectAcl"},
		{"get bucket acl", http.MethodGet, "http://s3.amazonaws.com/b?acl", nil, "GetBucketAcl"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.url, nil)
			r.Host = "s3.amazonaws.com"
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}
			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, "s3", req.Service)
			assert.Equal(t, tt.want, req.Operation)
		})
	}
}

// TestParseAWSRequest_S3VirtualHostSemanticOperation asserts the resolution reads
// the normalized path, not the raw one: virtual-hosted addressing puts the bucket in
// the Host, so a request whose URL path is "/k" is still a PutObject rather than a
// CreateBucket for a bucket named "k". Getting this backwards would arm a rule on
// the wrong operation for every SDK using the default addressing style.
func TestParseAWSRequest_S3VirtualHostSemanticOperation(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "http://mybucket.s3.amazonaws.com/k", nil)
	r.Host = "mybucket.s3.amazonaws.com"
	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "s3", req.Service)
	assert.Equal(t, "PutObject", req.Operation)
}

// TestParseAWSRequest_NonS3KeepsMethodFallback guards the other side: only S3 is
// resolved here, so a service without an Action or an X-Amz-Target still reports its
// HTTP method and no existing fault fixture on a bare method regresses.
func TestParseAWSRequest_NonS3KeepsMethodFallback(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost/anything", nil)
	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.NotEqual(t, "s3", req.Service)
	assert.Equal(t, http.MethodGet, req.Operation)
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

func TestParseAWSRequest_EmptyValueFormBodyParam(t *testing.T) {
	// The companion to TestParseAWSRequest_EmptyValueQueryParam (#200), for the
	// form body rather than the query string — the gap that let #412 through.
	// r.Form merges query and body, but the bare-key set can only be derived from
	// the query, so a body parameter sent explicitly empty was promoted to the
	// "1" sentinel. RunInstances then launched an instance from an AMI named "1"
	// instead of rejecting the request, which is how an empty ImageId "succeeded"
	// against substrate and failed against real AWS.
	body := "Action=RunInstances&MinCount=1&MaxCount=1&ImageId=&KeyName="
	r := httptest.NewRequest(http.MethodPost, "http://ec2.us-east-1.amazonaws.com/",
		strings.NewReader(body))
	r.Host = "ec2.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "", req.Params["ImageId"], "empty form-body param must not become the bare-key sentinel")
	assert.Equal(t, "", req.Params["KeyName"])
	assert.Equal(t, "RunInstances", req.Params["Action"])
	assert.Equal(t, "1", req.Params["MinCount"], "a value that is genuinely \"1\" must survive")
}

func TestParseAWSRequest_BareQueryKeyWithFormBody(t *testing.T) {
	// A bare key in the query string must still map to "1" when a form body is
	// also present — S3 POST operations (e.g. ?delete, ?uploads) rely on it, so
	// restricting the sentinel to query-string keys must not break them.
	r := httptest.NewRequest(http.MethodPost, "http://s3.amazonaws.com/mybucket?delete",
		strings.NewReader("<Delete></Delete>"))
	r.Host = "s3.amazonaws.com"

	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	assert.Equal(t, "1", req.Params["delete"])
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

// TestParseAWSRequest_APIGatewayVersionRouting pins the path discriminator that
// makes APIGatewayV2Plugin reachable. Both clients sign as "apigateway" and use
// the same hostname, so before #529 every v2 request landed on the v1 plugin and
// came back NotFoundException. The v2 model's URI space is entirely under "/v2/"
// and the v1 model's contains no "/v2/" path, so the split is exact — these cases
// cover both directions plus the guard that keeps an unrelated service's "/v2/"
// endpoint out of it.
func TestParseAWSRequest_APIGatewayVersionRouting(t *testing.T) {
	t.Parallel()
	const apigwScope = "AKIATEST12345678901/20240101/us-east-1/apigateway/aws4_request"
	tests := []struct {
		name        string
		authScope   string
		host        string
		path        string
		wantService string
	}{
		{"v2 CreateApi by auth scope", apigwScope, "", "/v2/apis", "apigatewayv2"},
		{"v2 GetRoutes by auth scope", apigwScope, "", "/v2/apis/abc123/routes", "apigatewayv2"},
		{"v2 by host", "", "apigateway.us-east-1.amazonaws.com", "/v2/apis", "apigatewayv2"},
		{"v1 restapis stays v1", apigwScope, "", "/restapis", "apigateway"},
		{"v1 nested path stays v1", apigwScope, "", "/restapis/abc123/resources", "apigateway"},
		{"v1 usageplans stays v1", apigwScope, "", "/usageplans", "apigateway"},
		{"v1 root stays v1", apigwScope, "", "/account", "apigateway"},
		// A v1 rest-api id is ten lowercase alphanumerics and a stage name is
		// caller-chosen, so either can contain "v2". Only the "/v2/" *prefix*
		// selects the v2 plugin.
		{"v1 id containing v2 stays v1", apigwScope, "", "/restapis/abcv2ghijk", "apigateway"},
		{"v1 stage named v2 stays v1", apigwScope, "", "/restapis/abc123defg/stages/v2", "apigateway"},
		// The refinement is scoped to apigateway: another service is free to use a
		// "/v2/" path and must not be captured.
		{"other service with /v2/ path", "AKIATEST12345678901/20240101/us-east-1/kafka/aws4_request", "", "/v2/clusters", "msk"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "http://localhost:4566"+tt.path, nil)
			r.Host = "localhost:4566"
			if tt.host != "" {
				r.Host = tt.host
			}
			if tt.authScope != "" {
				r.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential="+tt.authScope+", SignedHeaders=host, Signature=abc123")
			}

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantService, req.Service)
		})
	}
}

// TestParseAWSRequest_SSOAdminTargetPrefix pins the real sso-admin target prefix.
// "SWBExternalService" is the sso-admin model's targetPrefix and what clients
// actually send; the previously-mapped "AWSSSOAdminService" was a guess that
// matched nothing, so every call fell through to "service not emulated" (#561).
// The guessed prefix is retained deliberately, so both are asserted.
func TestParseAWSRequest_SSOAdminTargetPrefix(t *testing.T) {
	t.Parallel()
	for _, target := range []string{
		"SWBExternalService.ListInstances",
		"SWBExternalService.CreatePermissionSet",
		"AWSSSOAdminService.ListInstances",
	} {
		target := target
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "http://localhost:4566/", nil)
			r.Host = "localhost:4566"
			r.Header.Set("X-Amz-Target", target)

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, "sso", req.Service)
		})
	}
}

// TestParseAWSRequest_OrganizationsTargetPrefix pins the Organizations target
// prefix. "AWSOrganizationsV20161128" carries its API version inside the prefix
// rather than after an underscore, so the generic version-suffix strip never
// fires and the name never reduces to "organizations" on its own. Without the
// alias the plugin was registered and unit-tested but unreachable from any SDK,
// the same failure #561 found in sso-admin: every call answered
// "service not emulated: awsorganizationsv20161128".
func TestParseAWSRequest_OrganizationsTargetPrefix(t *testing.T) {
	t.Parallel()
	for _, target := range []string{
		"AWSOrganizationsV20161128.ListRoots",
		"AWSOrganizationsV20161128.CreateAccount",
		"AWSOrganizationsV20161128.MoveAccount",
	} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "http://localhost:4566/", nil)
			r.Host = "localhost:4566"
			r.Header.Set("X-Amz-Target", target)

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, "organizations", req.Service)
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
