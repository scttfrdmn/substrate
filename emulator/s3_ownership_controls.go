package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// A bucket's S3 Object Ownership setting: PutBucketOwnershipControls, GetBucketOwnershipControls
// and DeleteBucketOwnershipControls, and the x-amz-object-ownership header on CreateBucket (#1445).
//
// # Every new bucket has one
//
// "By default, Amazon S3 sets OwnershipControls for all newly created buckets"
// (API_GetBucketOwnershipControls), and "by default, ObjectOwnership is set to BucketOwnerEnforced"
// (API_CreateBucket). CreateBucket therefore stores a configuration — the header's value, or
// BucketOwnerEnforced — so a Get on a fresh bucket answers 200. The reference names two ways a
// bucket comes to have none: it predates the setting, or DeleteBucketOwnershipControls removed it.
// Only the second is reachable here, and it is what makes GetBucketOwnershipControls answer
// OwnershipControlsNotFoundError.
//
// That is a different shape from bucket encryption (s3_bucket_encryption.go), where an absent
// record *is* the default, because DeleteBucketEncryption resets to the default rather than
// removing it. Here a delete removes, so the default has to be a stored record for the delete to be
// observable.
//
// # What is not modeled
//
// The setting is recorded and reported; it is not enforced. Under BucketOwnerEnforced real S3
// disables ACLs — a write naming any ACL other than bucket-owner-full-control fails with
// AccessControlListNotSupported, and PutBucketOwnershipControls(BucketOwnerEnforced) on a bucket
// whose ACL grants others fails with InvalidBucketAclWithObjectOwnership. Substrate keeps accepting
// ACLs whatever the setting, so a bucket reporting BucketOwnerEnforced still honors one. That is
// tracked separately (#1474) rather than folded in here, because enforcing it by default would
// change the answer every existing ACL caller gets.

// s3OwnershipControlsKeyPrefix is the state-key prefix of a bucket's ownership controls, keyed by
// bucket name alone. It is listed in [s3BucketSubresourcePrefixes] so DeleteBucket clears it.
const s3OwnershipControlsKeyPrefix = "bucket_ownership_controls:"

// s3DefaultObjectOwnership is the ObjectOwnership S3 applies to a bucket created without the
// x-amz-object-ownership header.
const s3DefaultObjectOwnership = "BucketOwnerEnforced"

// s3ValidObjectOwnership reports whether v is in the published ObjectOwnership enum. A switch rather
// than a package-level set, so there is no mutable global to guard.
func s3ValidObjectOwnership(v string) bool {
	switch v {
	case "BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced":
		return true
	}
	return false
}

// s3OwnershipControls is the OwnershipControls document, both as the request body
// PutBucketOwnershipControls reads and as the response GetBucketOwnershipControls serves. The XML
// element name for each rule is Rule, though the API model calls the member Rules.
type s3OwnershipControls struct {
	XMLName xml.Name                  `xml:"OwnershipControls" json:"-"`
	Xmlns   string                    `xml:"xmlns,attr,omitempty" json:"-"`
	Rules   []s3OwnershipControlsRule `xml:"Rule" json:"rules"`
}

// s3OwnershipControlsRule is one OwnershipControlsRule.
type s3OwnershipControlsRule struct {
	ObjectOwnership string `xml:"ObjectOwnership" json:"object_ownership"`
}

// s3OwnershipControlsNotFoundResponse is the 404 GetBucketOwnershipControls answers for a bucket
// with no ownership controls.
//
// The code is the one S3's error-code list publishes for this condition; the operation's own
// reference documents no Errors section, as with GetPublicAccessBlock. It is also the code the AWS
// provider for Terraform tests for on this call, and the one boto3 callers report.
func s3OwnershipControlsNotFoundResponse() *AWSResponse {
	return s3ErrorResponse("OwnershipControlsNotFoundError",
		"The bucket ownership controls were not found", http.StatusNotFound)
}

// resolveCreateBucketObjectOwnership returns the ObjectOwnership a CreateBucket records: the
// x-amz-object-ownership header's value, or [s3DefaultObjectOwnership] when the header is absent.
// A value outside the enum is refused before the bucket exists.
func resolveCreateBucketObjectOwnership(headers map[string]string) (string, *AWSResponse) {
	value, ok := headerLookupFold(headers, "x-amz-object-ownership")
	if !ok || value == "" {
		return s3DefaultObjectOwnership, nil
	}
	if !s3ValidObjectOwnership(value) {
		return "", s3ErrorResponseWith(s3Error{
			Code:    "InvalidArgument",
			Message: "Invalid x-amz-object-ownership header: " + value,
			Status:  http.StatusBadRequest,
			Details: []s3ErrorDetail{
				{Name: "ArgumentName", Value: "x-amz-object-ownership"},
				{Name: "ArgumentValue", Value: value},
			},
		})
	}
	return value, nil
}

// s3StoreOwnershipControls records cfg as bucket's ownership controls.
func (p *S3Plugin) s3StoreOwnershipControls(ctx context.Context, bucket string, cfg s3OwnershipControls) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal ownership controls: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, s3OwnershipControlsKeyPrefix+bucket, data); err != nil {
		return fmt.Errorf("save ownership controls: %w", err)
	}
	return nil
}

// putBucketOwnershipControls handles PUT /<bucket>?ownershipControls: 200 with an empty body,
// replacing any configuration the bucket had.
//
// Rules is Required, so a document with none is MalformedXML, as is an ObjectOwnership outside the
// published enum: neither validates against the request schema.
func (p *S3Plugin) putBucketOwnershipControls(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}

	var in s3OwnershipControls
	body := decodeAWSChunked(req.Headers, req.Body)
	if err := xml.NewDecoder(bytes.NewReader(body)).Decode(&in); err != nil || len(in.Rules) == 0 {
		return s3ErrorResponse("MalformedXML", s3MalformedXMLMessage, http.StatusBadRequest), nil //nolint:nilerr // the decode error is the refusal
	}
	for _, r := range in.Rules {
		if !s3ValidObjectOwnership(r.ObjectOwnership) {
			return s3ErrorResponse("MalformedXML", s3MalformedXMLMessage, http.StatusBadRequest), nil
		}
	}

	if err := p.s3StoreOwnershipControls(ctx, bucket, s3OwnershipControls{Rules: in.Rules}); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// getBucketOwnershipControls handles GET /<bucket>?ownershipControls.
func (p *S3Plugin) getBucketOwnershipControls(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}

	raw, err := p.state.Get(ctx, s3Namespace, s3OwnershipControlsKeyPrefix+bucket)
	if err != nil {
		return nil, fmt.Errorf("get ownership controls: %w", err)
	}
	if raw == nil {
		return s3OwnershipControlsNotFoundResponse(), nil
	}
	var cfg s3OwnershipControls
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal ownership controls: %w", err)
	}
	cfg.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, cfg)
}

// deleteBucketOwnershipControls handles DELETE /<bucket>?ownershipControls: 204, after which the
// bucket has no ownership controls. The bucket itself is not touched — before #1445 this request
// was refused, and before #1349 it reached DeleteBucket.
//
// Deleting controls a bucket does not have is a 204: the reference documents no error for it.
func (p *S3Plugin) deleteBucketOwnershipControls(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	if err := p.state.Delete(ctx, s3Namespace, s3OwnershipControlsKeyPrefix+bucket); err != nil {
		return nil, fmt.Errorf("delete ownership controls: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}
