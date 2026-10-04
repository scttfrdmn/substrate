package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// A bucket's CORS configuration: PutBucketCors, GetBucketCors and DeleteBucketCors (#1278).
//
// Substrate records the configuration a caller writes and answers it back. It does not evaluate a
// rule against a cross-origin request or answer a preflight OPTIONS: that is request-path behavior
// of the bucket, not an observation an API call makes, and the issue scoped it out.
//
// # What is stored
//
// The parsed rules, as JSON under bucket_cors:<bucket>, not the request bytes. GetBucketCors renders
// them in API_CORSRule's element names, so what a caller reads back is the published shape whatever
// whitespace, namespace or element order it sent. Every member round-trips: AllowedMethod,
// AllowedOrigin, AllowedHeader and ExposeHeader in the order sent, ID, and MaxAgeSeconds, which is
// a pointer so that an absent value and an explicit 0 stay distinct.
//
// # Absent is not empty
//
// A bucket with no configuration answers NoSuchCORSConfiguration/404 from GetBucketCors. A stored
// configuration always holds at least one rule, because PutBucketCors' CORSRule is Required: Yes, so
// "no configuration" and "a configuration" are the only two observations and neither is answered as
// the other.
//
// # What is refused
//
// MalformedXML/400, S3's code for XML that "was not well-formed or did not validate against our
// published schema", for a body that does not decode, a configuration with no CORSRule, or a rule
// missing AllowedMethod or AllowedOrigin, each of which API_CORSRule marks Required: Yes. Four
// published limits are not enforced, because no page names the code a breach answers: at most 100
// rules, a 64 KB document, an ID of at most 255 characters, and AllowedMethod's valid values (GET,
// PUT, HEAD, POST, DELETE).

// s3CORSKeyPrefix is the state-key prefix of a bucket's CORS configuration. It is listed in
// s3BucketSubresourcePrefixes, so DeleteBucket removes it.
const s3CORSKeyPrefix = "bucket_cors:"

// s3CORSRule is one CORSRule, in API_CORSRule's members.
type s3CORSRule struct {
	AllowedHeaders []string `xml:"AllowedHeader" json:"AllowedHeaders,omitempty"`
	AllowedMethods []string `xml:"AllowedMethod" json:"AllowedMethods"`
	AllowedOrigins []string `xml:"AllowedOrigin" json:"AllowedOrigins"`
	ExposeHeaders  []string `xml:"ExposeHeader" json:"ExposeHeaders,omitempty"`
	ID             string   `xml:"ID,omitempty" json:"ID,omitempty"`
	MaxAgeSeconds  *int     `xml:"MaxAgeSeconds,omitempty" json:"MaxAgeSeconds,omitempty"`
}

// s3CORSConfiguration is PutBucketCors' request body and GetBucketCors' response body.
type s3CORSConfiguration struct {
	XMLName xml.Name     `xml:"CORSConfiguration" json:"-"`
	Rules   []s3CORSRule `xml:"CORSRule" json:"CORSRules"`
}

// s3RequireBucket answers NoSuchBucket for a bucket that does not exist, and nil for one that does.
func (p *S3Plugin) s3RequireBucket(ctx context.Context, bucket string) (*AWSResponse, error) {
	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}
	return nil, nil
}

// putBucketCors handles PUT /<bucket>?cors. An existing configuration is replaced, as the page says.
func (p *S3Plugin) putBucketCors(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}

	var cfg s3CORSConfiguration
	body := decodeAWSChunked(req.Headers, req.Body)
	if err := xml.NewDecoder(bytes.NewReader(body)).Decode(&cfg); err != nil || !s3CORSValid(cfg) {
		return s3ErrorResponse("MalformedXML", s3MalformedXMLMessage, http.StatusBadRequest), nil //nolint:nilerr // the decode error is the refusal
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal cors config: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, s3CORSKeyPrefix+bucket, data); err != nil {
		return nil, fmt.Errorf("save cors config: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// s3CORSValid reports whether cfg carries what API_PutBucketCors and API_CORSRule mark Required: at
// least one rule, and in each an AllowedMethod and an AllowedOrigin.
func s3CORSValid(cfg s3CORSConfiguration) bool {
	if len(cfg.Rules) == 0 {
		return false
	}
	for _, rule := range cfg.Rules {
		if len(rule.AllowedMethods) == 0 || len(rule.AllowedOrigins) == 0 {
			return false
		}
	}
	return true
}

// getBucketCors handles GET /<bucket>?cors.
func (p *S3Plugin) getBucketCors(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}

	data, err := p.state.Get(ctx, s3Namespace, s3CORSKeyPrefix+bucket)
	if err != nil {
		return nil, fmt.Errorf("get cors config: %w", err)
	}
	if data == nil {
		return s3ErrorResponse("NoSuchCORSConfiguration", "The CORS configuration does not exist", http.StatusNotFound), nil
	}
	var cfg s3CORSConfiguration
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("decode cors config: %w", err)
	}
	return s3XMLResponse(http.StatusOK, cfg)
}

// deleteBucketCors handles DELETE /<bucket>?cors: 204, and idempotent, so removing a configuration
// that is not there still succeeds.
func (p *S3Plugin) deleteBucketCors(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	if err := p.state.Delete(ctx, s3Namespace, s3CORSKeyPrefix+bucket); err != nil {
		return nil, fmt.Errorf("delete cors config: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}
