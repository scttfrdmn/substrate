package emulator

import (
	"context"
	"encoding/xml"
	"net/http"
)

// A bucket's request-payment configuration: PutBucketRequestPayment and GetBucketRequestPayment
// (#1613, #1658).
//
// Substrate records who pays and answers it back. It does not bill anyone, and it does not refuse
// a request to a Requester Pays bucket that omits x-amz-request-payer: what a request costs is
// substrate's cost model (costs.go), not a per-bucket payer.
//
// "By default, the bucket owner pays for downloads from the bucket" (API_PutBucketRequestPayment),
// so a bucket with no record answers Payer BucketOwner. There is no Delete; a Put of BucketOwner
// returns the bucket to the default.
//
// MalformedXML/400 refuses a body that does not decode and a Payer that is absent or outside the
// published enum (Requester, BucketOwner): Payer is Required: Yes.

// s3RequestPaymentKeyPrefix is the state-key prefix of a bucket's request-payment configuration. It
// is listed in [s3BucketSubresourcePrefixes], so DeleteBucket removes it.
const s3RequestPaymentKeyPrefix = "bucket_request_payment:"

// s3DefaultPayer is the Payer of a bucket whose request payment was never configured.
const s3DefaultPayer = "BucketOwner"

// s3RequestPaymentConfiguration is PutBucketRequestPayment's request body and
// GetBucketRequestPayment's response body.
type s3RequestPaymentConfiguration struct {
	XMLName xml.Name `xml:"RequestPaymentConfiguration" json:"-"`
	Xmlns   string   `xml:"xmlns,attr,omitempty" json:"-"`
	Payer   string   `xml:"Payer" json:"Payer"`
}

// putBucketRequestPayment handles PUT /<bucket>?requestPayment.
func (p *S3Plugin) putBucketRequestPayment(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3RequestPaymentConfiguration
	if !s3DecodeConfigXML(req, &cfg) || (cfg.Payer != "Requester" && cfg.Payer != s3DefaultPayer) {
		return s3MalformedXMLResponse(), nil
	}
	if err := p.s3StoreBucketConfig(ctx, s3RequestPaymentKeyPrefix, bucket, s3RequestPaymentConfiguration{Payer: cfg.Payer}); err != nil {
		return nil, err
	}
	return s3EmptyOK(), nil
}

// getBucketRequestPayment handles GET /<bucket>?requestPayment.
func (p *S3Plugin) getBucketRequestPayment(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	cfg := s3RequestPaymentConfiguration{Payer: s3DefaultPayer}
	if _, err := p.s3LoadBucketConfig(ctx, s3RequestPaymentKeyPrefix, bucket, &cfg); err != nil {
		return nil, err
	}
	cfg.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, cfg)
}
