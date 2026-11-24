package emulator

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"
)

// A bucket's Transfer Acceleration state: PutBucketAccelerateConfiguration and
// GetBucketAccelerateConfiguration (#1613, #1658).
//
// Substrate records the state and answers it back. It does not serve the accelerate endpoint or
// change how a transfer behaves: acceleration is network routing, not an API observation.
//
// # Never set is not Suspended
//
// "A GET accelerate request does not return a state value for a bucket that has no transfer
// acceleration state. A bucket has no Transfer Acceleration state if a state has never been set on
// the bucket" (API_GetBucketAccelerateConfiguration), and the page's third example is the empty
// <AccelerateConfiguration/> it answers. So the absent record answers 200 with no Status, and a
// bucket that was once Enabled and then Suspended answers Suspended: there is no Delete.
//
// # What is refused
//
//   - MalformedXML/400 for a body that does not decode, and for a Status outside the published enum
//     (Enabled, Suspended). A configuration with no Status is accepted and leaves the state as it
//     was, since Status is Required: No.
//   - InvalidRequest/400, "Amazon S3 Transfer Acceleration is not supported for buckets with
//     periods (.) in their names.", for such a bucket: the page says "The name of the bucket used
//     for Transfer Acceleration must be DNS-compliant and must not contain periods", and the code and
//     message are API_Error's. A bucket substrate created already has a DNS-compliant name.

// s3AccelerateKeyPrefix is the state-key prefix of a bucket's Transfer Acceleration state. It is
// listed in [s3BucketSubresourcePrefixes], so DeleteBucket removes it.
const s3AccelerateKeyPrefix = "bucket_accelerate:"

// s3AccelerateConfiguration is PutBucketAccelerateConfiguration's request body and
// GetBucketAccelerateConfiguration's response body.
type s3AccelerateConfiguration struct {
	XMLName xml.Name `xml:"AccelerateConfiguration" json:"-"`
	Xmlns   string   `xml:"xmlns,attr,omitempty" json:"-"`
	Status  string   `xml:"Status,omitempty" json:"Status,omitempty"`
}

// putBucketAccelerateConfiguration handles PUT /<bucket>?accelerate.
func (p *S3Plugin) putBucketAccelerateConfiguration(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3AccelerateConfiguration
	if !s3DecodeConfigXML(req, &cfg) {
		return s3MalformedXMLResponse(), nil
	}
	switch cfg.Status {
	case "":
		return s3EmptyOK(), nil
	case "Enabled", "Suspended":
	default:
		return s3MalformedXMLResponse(), nil
	}
	if strings.Contains(bucket, ".") {
		return s3ErrorResponse("InvalidRequest",
			"Amazon S3 Transfer Acceleration is not supported for buckets with periods (.) in their names.",
			http.StatusBadRequest), nil
	}
	if err := p.s3StoreBucketConfig(ctx, s3AccelerateKeyPrefix, bucket, s3AccelerateConfiguration{Status: cfg.Status}); err != nil {
		return nil, err
	}
	return s3EmptyOK(), nil
}

// getBucketAccelerateConfiguration handles GET /<bucket>?accelerate: the recorded state, or an
// empty configuration for a bucket whose state was never set.
func (p *S3Plugin) getBucketAccelerateConfiguration(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3AccelerateConfiguration
	if _, err := p.s3LoadBucketConfig(ctx, s3AccelerateKeyPrefix, bucket, &cfg); err != nil {
		return nil, err
	}
	cfg.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, cfg)
}
