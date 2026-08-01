package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// s3PublicAccessBlockKey is the state key holding a bucket's Block Public Access
// configuration.
//
// The configuration lives under its own key rather than as a field on [S3Bucket]
// because "no configuration" and "a configuration with all four settings false"
// are distinct observations: the first is 404
// NoSuchPublicAccessBlockConfiguration, the second is a 200 carrying four
// `false` elements. A field on the bucket record could not tell them apart
// without a pointer, and a separate key makes DeletePublicAccessBlock a delete
// rather than a partial rewrite of the bucket — which is what keeps it from
// touching the bucket at all (#446).
func s3PublicAccessBlockKey(bucket string) string {
	return "bucket_public_access_block:" + bucket
}

// s3NoSuchPublicAccessBlockResponse is the 404 a bucket with no Block Public
// Access configuration returns from GetPublicAccessBlock.
//
// The code is not in the S3 (2006-03-01) API model — only the s3control model
// declares it, and the bucket-level GetPublicAccessBlock reference documents no
// Errors section at all. It is nonetheless what real bucket-level S3 returns:
// the s3control shape's own documentation describes the same condition, the AWS
// provider for Terraform handles this exact code on bucket-level
// GetPublicAccessBlock and DeletePublicAccessBlock
// (internal/service/s3/errors.go), and it is what callers report seeing from
// boto3's s3.get_public_access_block. Modeling the 404 is what makes a
// consumer's "no block configured" branch reachable; a 200 with four `false`
// values would report a configuration the bucket does not have.
func s3NoSuchPublicAccessBlockResponse() *AWSResponse {
	return s3ErrorResponse("NoSuchPublicAccessBlockConfiguration",
		"The public access block configuration was not found.", http.StatusNotFound)
}

// putPublicAccessBlock handles PUT /<bucket>?publicAccessBlock.
//
// Substrate does not apply S3's April 2023 default of enabling all four settings
// on a newly created bucket: a bucket that has never been the subject of a
// PutPublicAccessBlock has no configuration, and GetPublicAccessBlock 404s. That
// is deliberate. The default is a property of AWS-managed account and
// organization state layered over the bucket, which substrate does not model, and
// seeding every new bucket with a configuration would make the
// NoSuchPublicAccessBlockConfiguration path — the one a consumer's error branch
// exists for — unreachable through the public API. Consumers that need a
// configured bucket call PutPublicAccessBlock, which is what the SDK and
// CloudFormation both do.
func (p *S3Plugin) putPublicAccessBlock(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	if missing, err := p.bucketMissingResponse(ctx, bucket); err != nil {
		return nil, err
	} else if missing != nil {
		return missing, nil
	}

	// The four members are each `Required: No`, so a partial body is legal and
	// every member it omits is recorded as false — which is how S3 reports them
	// back. Unmarshalling into a zero value gets that for free.
	//
	// The body itself is `Required: Yes`, and an empty one lands here as
	// MalformedXML rather than as a request to clear the settings — clearing is
	// DeletePublicAccessBlock's job. No separate length check is needed: an empty
	// body fails to unmarshal.
	var cfg S3PublicAccessBlockConfiguration
	if err := xml.Unmarshal(req.Body, &cfg); err != nil {
		return s3ErrorResponse("MalformedXML", s3MalformedXMLMessage, http.StatusBadRequest), nil //nolint:nilerr // intentionally converted to an S3 XML error response
	}

	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal public access block: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, s3PublicAccessBlockKey(bucket), raw); err != nil {
		return nil, fmt.Errorf("put public access block: %w", err)
	}

	// 200 with an empty body, per the PutPublicAccessBlock reference — not the 204
	// that PutBucketPolicy returns.
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// getPublicAccessBlock handles GET /<bucket>?publicAccessBlock.
func (p *S3Plugin) getPublicAccessBlock(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	if missing, err := p.bucketMissingResponse(ctx, bucket); err != nil {
		return nil, err
	} else if missing != nil {
		return missing, nil
	}

	raw, err := p.state.Get(ctx, s3Namespace, s3PublicAccessBlockKey(bucket))
	if err != nil {
		return nil, fmt.Errorf("get public access block: %w", err)
	}
	if raw == nil {
		return s3NoSuchPublicAccessBlockResponse(), nil
	}

	var cfg S3PublicAccessBlockConfiguration
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal public access block: %w", err)
	}
	cfg.Xmlns = s3XMLNamespace

	return s3XMLResponse(http.StatusOK, cfg)
}

// deletePublicAccessBlock handles DELETE /<bucket>?publicAccessBlock.
//
// This is the operation that made #446 a data-loss defect rather than a fidelity
// gap: unrouted, it fell through to DeleteBucket and destroyed the bucket. The
// bucket is deliberately not touched here.
//
// Deleting a configuration that does not exist is a 204, not a 404. The
// DeletePublicAccessBlock reference documents no errors, and its own
// idempotence is what a consumer tearing down a bucket relies on — the AWS
// provider for Terraform treats a NoSuchPublicAccessBlockConfiguration on delete
// as success for that reason.
func (p *S3Plugin) deletePublicAccessBlock(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	if missing, err := p.bucketMissingResponse(ctx, bucket); err != nil {
		return nil, err
	} else if missing != nil {
		return missing, nil
	}

	if err := p.state.Delete(ctx, s3Namespace, s3PublicAccessBlockKey(bucket)); err != nil {
		return nil, fmt.Errorf("delete public access block: %w", err)
	}

	// 204, per the DELETE operation's declared responseCode.
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}
