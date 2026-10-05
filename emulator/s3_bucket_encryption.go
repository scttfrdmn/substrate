package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// A bucket's default encryption, and how every write resolves the encryption it reports (#493).
//
// # The resolution order
//
// An object's encryption comes from the request that wrote it and, when the request names none,
// from the destination bucket's default. Never from anywhere else: a CopyObject naming no encryption
// takes the destination bucket's default, not the source object's — API_CopyObject: "if you don't
// specify encryption information in your copy request, the encryption setting of the target object
// is set to the default encryption configuration of the destination bucket." That is the asymmetry
// s3_copy_metadata.go documents for MetadataDirective in the other direction, and it is what lets an
// in-place copy for a metadata change silently move an SSE-KMS object off its customer managed key.
//
// # Every bucket has a default
//
// "By default, all buckets have a default encryption configuration that uses server-side encryption
// with Amazon S3 managed keys (SSE-S3)" (API_PutBucketEncryption, API_GetBucketEncryption), and
// DeleteBucketEncryption "resets the default encryption for the bucket as ... (SSE-S3)". So a bucket
// with nothing stored answers GetBucketEncryption with an AES256 rule, never with the
// ServerSideEncryptionConfigurationNotFoundError S3 answered before January 2023, and a write naming
// nothing resolves to AES256.
//
// That reverses #492's "an object written with no headers echoes none", deliberately, as #493
// requires: a real bucket never stores an object without encryption, so the old observation was one
// S3 can no longer make. What a test can still distinguish is the encryption it *chose* from the
// one it *inherited*: set a bucket default, write without headers, and read the default back.
//
// # What is stored
//
// PutBucketEncryption's rules, parsed, under bucket_encryption:<bucket>, which
// s3BucketSubresourcePrefixes lists so DeleteBucket clears it. Each write resolves its encryption
// once and stores the result on the object, so a later change to the bucket default does not
// rewrite objects already written, as in S3.
//
// # What is refused
//
// The request-side refusals are in [resolveWriteEncryption]. PutBucketEncryption refuses a body that
// does not decode or carries no Rule with MalformedXML, S3's code for XML that "did not validate
// against our published schema". It refuses an SSEAlgorithm outside the published Valid Values, and a
// KMSMasterKeyID beside an algorithm that is not aws:kms or aws:kms:dsse ("allowed if and only if"),
// with InvalidArgument/400. That code and its messages are substrate's reading: neither page
// publishes the code a breach answers. BlockedEncryptionTypes is recorded and answered back but not
// enforced, because SSE-C is out of scope.

// s3BucketEncryptionKeyPrefix is the state-key prefix of a bucket's default encryption configuration.
const s3BucketEncryptionKeyPrefix = "bucket_encryption:"

// s3SSEAlgorithms are the published values of x-amz-server-side-encryption and of
// ServerSideEncryptionByDefault.SSEAlgorithm.
var s3SSEAlgorithms = map[string]bool{
	"AES256": true, "aws:fsx": true, "aws:backup": true, "aws:kms": true, "aws:kms:dsse": true,
}

// s3SSEDefaultAlgorithm is the encryption every bucket applies when nothing else is configured.
const s3SSEDefaultAlgorithm = "AES256"

// s3SSEIsKMS reports whether algorithm is one of the two KMS-backed algorithms a key ID may name.
func s3SSEIsKMS(algorithm string) bool {
	return algorithm == "aws:kms" || algorithm == "aws:kms:dsse"
}

// s3SSEByDefault is ServerSideEncryptionByDefault.
//
// KMSKeyID is decoded from KMSKeyID as well as KMSMasterKeyID: the member is KMSMasterKeyID, but
// every example on API_PutBucketEncryption and API_GetBucketEncryption spells it KMSKeyID, so a
// caller who copied an example would otherwise lose its key silently. It is answered as
// KMSMasterKeyID, the member's name.
type s3SSEByDefault struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm" json:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty" json:"KMSMasterKeyID,omitempty"`
}

// s3SSEByDefaultIn is the decode-side form of [s3SSEByDefault], accepting the examples' spelling.
type s3SSEByDefaultIn struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID"`
	KMSKeyID       string `xml:"KMSKeyID"`
}

// s3SSEBlockedTypes is ServerSideEncryptionRule.BlockedEncryptionTypes.
type s3SSEBlockedTypes struct {
	EncryptionTypes []string `xml:"EncryptionType" json:"EncryptionTypes"`
}

// s3SSERule is ServerSideEncryptionRule.
type s3SSERule struct {
	ApplyServerSideEncryptionByDefault *s3SSEByDefault    `xml:"ApplyServerSideEncryptionByDefault,omitempty" json:"ApplyServerSideEncryptionByDefault,omitempty"`
	BlockedEncryptionTypes             *s3SSEBlockedTypes `xml:"BlockedEncryptionTypes,omitempty" json:"BlockedEncryptionTypes,omitempty"`
	BucketKeyEnabled                   *bool              `xml:"BucketKeyEnabled,omitempty" json:"BucketKeyEnabled,omitempty"`
}

// s3SSEConfiguration is ServerSideEncryptionConfiguration, PutBucketEncryption's request body and
// GetBucketEncryption's response body.
type s3SSEConfiguration struct {
	XMLName xml.Name    `xml:"ServerSideEncryptionConfiguration" json:"-"`
	Rules   []s3SSERule `xml:"Rule" json:"Rules"`
}

// s3SSEConfigurationIn is the decode-side form of [s3SSEConfiguration].
type s3SSEConfigurationIn struct {
	XMLName xml.Name `xml:"ServerSideEncryptionConfiguration"`
	Rules   []struct {
		ApplyServerSideEncryptionByDefault *s3SSEByDefaultIn  `xml:"ApplyServerSideEncryptionByDefault"`
		BlockedEncryptionTypes             *s3SSEBlockedTypes `xml:"BlockedEncryptionTypes"`
		BucketKeyEnabled                   *bool              `xml:"BucketKeyEnabled"`
	} `xml:"Rule"`
}

// s3SSEImplicitDefault is the configuration a bucket answers when none is stored: SSE-S3, with S3
// Bucket Keys off, as GetBucketEncryption reports for a bucket that was never configured.
func s3SSEImplicitDefault() s3SSEConfiguration {
	off := false
	return s3SSEConfiguration{Rules: []s3SSERule{{
		ApplyServerSideEncryptionByDefault: &s3SSEByDefault{SSEAlgorithm: s3SSEDefaultAlgorithm},
		BucketKeyEnabled:                   &off,
	}}}
}

// s3SSEInvalidArgument is the refusal for an encryption combination S3 rejects. The code is the one
// #493 names for all four; the messages are substrate's own (see [resolveWriteEncryption]).
func s3SSEInvalidArgument(message string) *AWSResponse {
	return s3ErrorResponse("InvalidArgument", message, http.StatusBadRequest)
}

// loadBucketEncryption returns the bucket's stored default encryption, or the implicit SSE-S3
// default when none is stored.
func (p *S3Plugin) loadBucketEncryption(ctx context.Context, bucket string) (s3SSEConfiguration, error) {
	data, err := p.state.Get(ctx, s3Namespace, s3BucketEncryptionKeyPrefix+bucket)
	if err != nil {
		return s3SSEConfiguration{}, fmt.Errorf("get bucket encryption: %w", err)
	}
	if data == nil {
		return s3SSEImplicitDefault(), nil
	}
	var cfg s3SSEConfiguration
	if err := json.Unmarshal(data, &cfg); err != nil {
		return s3SSEConfiguration{}, fmt.Errorf("decode bucket encryption: %w", err)
	}
	return cfg, nil
}

// bucketDefaultEncryption returns the encryption a write naming none resolves to in bucket: the
// first rule's ApplyServerSideEncryptionByDefault and BucketKeyEnabled, or SSE-S3.
func (p *S3Plugin) bucketDefaultEncryption(ctx context.Context, bucket string) (S3ServerSideEncryption, error) {
	cfg, err := p.loadBucketEncryption(ctx, bucket)
	if err != nil {
		return S3ServerSideEncryption{}, err
	}
	for _, rule := range cfg.Rules {
		if rule.ApplyServerSideEncryptionByDefault == nil {
			continue
		}
		def := rule.ApplyServerSideEncryptionByDefault
		out := S3ServerSideEncryption{Algorithm: def.SSEAlgorithm}
		if s3SSEIsKMS(def.SSEAlgorithm) {
			out.KMSKeyID = def.KMSMasterKeyID
			out.BucketKeyEnabled = rule.BucketKeyEnabled != nil && *rule.BucketKeyEnabled
		}
		return out, nil
	}
	return S3ServerSideEncryption{Algorithm: s3SSEDefaultAlgorithm}, nil
}

// resolveWriteEncryption resolves the encryption a write records: the request's own when it names
// an algorithm, otherwise the destination bucket's default. A non-nil response is a refusal.
//
// The request is refused, before anything is written, for the three combinations #493 lists, each
// InvalidArgument/400:
//
//   - an x-amz-server-side-encryption value outside the published Valid Values. API_CopyObject
//     says so outright: "Unrecognized or unsupported values won't write a destination object and
//     will receive a 400 Bad Request response";
//   - an x-amz-server-side-encryption-aws-kms-key-id beside an algorithm that is not aws:kms or
//     aws:kms:dsse, which PutObject's page names as the two the header applies to;
//   - x-amz-server-side-encryption-bucket-key-enabled set to true beside an algorithm that is not
//     aws:kms, the only algorithm an S3 Bucket Key serves ("to use an S3 Bucket Key for object
//     encryption with SSE-KMS").
//
// The last two are evaluated against the algorithm the write resolves to, so a request that names
// only a key ID or only the Bucket Key flag is accepted in a bucket whose default is SSE-KMS and
// refused in one whose default is SSE-S3. Code and status follow #493 and CopyObject's sentence; the
// messages are substrate's own text, since no capture corroborates S3's wording (#487).
//
// A request naming an algorithm takes nothing from the bucket default, so aws:kms with no key ID is
// the AWS managed key and reports no key ID, as the page states. A request that names the algorithm
// and not the Bucket Key flag takes the flag from the bucket default when both are SSE-KMS.
func (p *S3Plugin) resolveWriteEncryption(ctx context.Context, bucket string, headers map[string]string) (S3ServerSideEncryption, *AWSResponse, error) {
	requested := resolveServerSideEncryption(headers)
	bucketKeyHeader := headerValueFold(headers, s3SSEBucketKeyEnabledHeader)

	if requested.Algorithm != "" && !s3SSEAlgorithms[requested.Algorithm] {
		return S3ServerSideEncryption{}, s3SSEInvalidArgument(
			"The encryption method specified is not supported: " + requested.Algorithm), nil
	}

	defaults, err := p.bucketDefaultEncryption(ctx, bucket)
	if err != nil {
		return S3ServerSideEncryption{}, nil, err
	}

	resolved := requested
	if requested.Algorithm == "" {
		resolved = defaults
		if requested.KMSKeyID != "" {
			resolved.KMSKeyID = requested.KMSKeyID
		}
		if bucketKeyHeader != "" {
			resolved.BucketKeyEnabled = requested.BucketKeyEnabled
		}
	} else if bucketKeyHeader == "" && requested.Algorithm == "aws:kms" && defaults.Algorithm == "aws:kms" {
		resolved.BucketKeyEnabled = defaults.BucketKeyEnabled
	}

	if requested.KMSKeyID != "" && !s3SSEIsKMS(resolved.Algorithm) {
		return S3ServerSideEncryption{}, s3SSEInvalidArgument(
			"Specifying x-amz-server-side-encryption-aws-kms-key-id requires x-amz-server-side-encryption: aws:kms or aws:kms:dsse, not " + resolved.Algorithm), nil
	}
	if requested.BucketKeyEnabled && resolved.Algorithm != "aws:kms" {
		return S3ServerSideEncryption{}, s3SSEInvalidArgument(
			"x-amz-server-side-encryption-bucket-key-enabled applies only to x-amz-server-side-encryption: aws:kms, not " + resolved.Algorithm), nil
	}
	return resolved, nil, nil
}

// s3SSERequestHeaders reports the SSE-S3/SSE-KMS request headers present in headers.
//
// UploadPart and UploadPartCopy carry their upload's encryption and take none of their own: "you
// only need to specify the server-side encryption parameters in the initial Initiate Multipart
// request". The SSE-C headers are not in this list; they are out of scope.
func s3SSERequestHeaders(headers map[string]string) []string {
	var present []string
	for _, h := range []string{s3SSEAlgorithmHeader, s3SSEKMSKeyIDHeader, s3SSEBucketKeyEnabledHeader} {
		if headerValueFold(headers, h) != "" {
			present = append(present, h)
		}
	}
	return present
}

// s3SSEPartRefusal refuses a part upload that restates encryption, InvalidArgument/400. The code is
// #493's; the message is substrate's own.
func s3SSEPartRefusal(present []string) *AWSResponse {
	return s3SSEInvalidArgument("A part upload takes its multipart upload's encryption and cannot specify " +
		strings.Join(present, ", ") + "; specify encryption on CreateMultipartUpload")
}

// putBucketEncryption handles PUT /<bucket>?encryption. An existing configuration is replaced.
func (p *S3Plugin) putBucketEncryption(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}

	var in s3SSEConfigurationIn
	body := decodeAWSChunked(req.Headers, req.Body)
	if err := xml.NewDecoder(bytes.NewReader(body)).Decode(&in); err != nil || len(in.Rules) == 0 {
		return s3ErrorResponse("MalformedXML", s3MalformedXMLMessage, http.StatusBadRequest), nil //nolint:nilerr // the decode error is the refusal
	}

	cfg := s3SSEConfiguration{Rules: make([]s3SSERule, 0, len(in.Rules))}
	for _, r := range in.Rules {
		rule := s3SSERule{BlockedEncryptionTypes: r.BlockedEncryptionTypes, BucketKeyEnabled: r.BucketKeyEnabled}
		if d := r.ApplyServerSideEncryptionByDefault; d != nil {
			keyID := d.KMSMasterKeyID
			if keyID == "" {
				keyID = d.KMSKeyID
			}
			if !s3SSEAlgorithms[d.SSEAlgorithm] {
				return s3SSEInvalidArgument("The SSEAlgorithm is not supported: " + d.SSEAlgorithm), nil
			}
			if keyID != "" && !s3SSEIsKMS(d.SSEAlgorithm) {
				return s3SSEInvalidArgument("KMSMasterKeyID is allowed only with SSEAlgorithm aws:kms or aws:kms:dsse, not " + d.SSEAlgorithm), nil
			}
			rule.ApplyServerSideEncryptionByDefault = &s3SSEByDefault{SSEAlgorithm: d.SSEAlgorithm, KMSMasterKeyID: keyID}
		}
		cfg.Rules = append(cfg.Rules, rule)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal bucket encryption: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, s3BucketEncryptionKeyPrefix+bucket, data); err != nil {
		return nil, fmt.Errorf("save bucket encryption: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// getBucketEncryption handles GET /<bucket>?encryption, answering the stored configuration or the
// SSE-S3 default every bucket has.
func (p *S3Plugin) getBucketEncryption(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	cfg, err := p.loadBucketEncryption(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return s3XMLResponse(http.StatusOK, cfg)
}

// deleteBucketEncryption handles DELETE /<bucket>?encryption: 204, resetting the bucket to the
// SSE-S3 default. Idempotent, so resetting a bucket already at the default succeeds.
func (p *S3Plugin) deleteBucketEncryption(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	if err := p.state.Delete(ctx, s3Namespace, s3BucketEncryptionKeyPrefix+bucket); err != nil {
		return nil, fmt.Errorf("delete bucket encryption: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}
