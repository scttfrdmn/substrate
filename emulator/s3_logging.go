package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// A bucket's server-access-logging status: PutBucketLogging and GetBucketLogging (#1613, #1658).
//
// Substrate records where logs would go and answers it back. It never writes a log object: a log
// record is the bucket's own traffic, delivered "on a best-effort basis" hours later
// (userguide/ServerLogs.html), which is resource-internal and nondeterministic (CLAUDE.md, "Scope").
// A test asserting on access logs belongs in a different tier.
//
// # Absent is disabled
//
// GetBucketLogging on a bucket that has never had logging enabled answers 200 with an empty
// BucketLoggingStatus, the shape its page's "Showing a disabled logging status" example shows. A
// Put of an empty BucketLoggingStatus — "To disable logging, you use an empty BucketLoggingStatus
// request element" — removes the record, so the two are one observation. There is no
// DeleteBucketLogging operation.
//
// # What is refused
//
//   - MalformedXML/400 for a body that does not decode, or a LoggingEnabled without TargetBucket or
//     TargetPrefix, both Required: Yes in API_LoggingEnabled. An empty TargetPrefix is a value and
//     is accepted; only an absent element is refused.
//   - InvalidTargetBucketForLogging/400 for a target bucket that does not exist, with the message
//     "The target bucket for logging does not exist" that the AWS provider for Terraform reports
//     (hashicorp/terraform-provider-aws#23498).
//   - InvalidTargetBucketForLogging/400 for a target in another account, since "The destination
//     bucket must be in the same AWS Region and AWS account as the source bucket"
//     (userguide/ServerLogs.html). Its message is API_Error's description of the code: "The target
//     bucket for logging does not exist, is not owned by you, or does not have the appropriate
//     grants for the log-delivery group."
//   - CrossLocationLoggingProhibited/403 for a target in another Region, with API_Error's
//     description, "Cross-location logging not allowed. Buckets in one geographic location cannot
//     log information to a bucket in another location."
//
// TargetGrants are recorded and answered back but not checked against the target bucket's Object
// Ownership setting: the page says a bucket-owner-enforced target "can't use the Grantee request
// element" without naming the code the request answers.

// s3LoggingKeyPrefix is the state-key prefix of a bucket's logging status. It is listed in
// [s3BucketSubresourcePrefixes], so DeleteBucket removes it.
const s3LoggingKeyPrefix = "bucket_logging:"

// s3BucketLoggingStatus is PutBucketLogging's request body and GetBucketLogging's response body.
type s3BucketLoggingStatus struct {
	XMLName        xml.Name          `xml:"BucketLoggingStatus" json:"-"`
	Xmlns          string            `xml:"xmlns,attr,omitempty" json:"-"`
	LoggingEnabled *s3LoggingEnabled `xml:"LoggingEnabled,omitempty" json:"LoggingEnabled,omitempty"`
}

// s3LoggingEnabled is LoggingEnabled (API_LoggingEnabled). TargetPrefix is a pointer so an absent
// element, which is refused, stays distinct from an empty one.
type s3LoggingEnabled struct {
	TargetBucket          string                   `xml:"TargetBucket" json:"TargetBucket"`
	TargetGrants          *s3LoggingTargetGrants   `xml:"TargetGrants,omitempty" json:"TargetGrants,omitempty"`
	TargetObjectKeyFormat *s3TargetObjectKeyFormat `xml:"TargetObjectKeyFormat,omitempty" json:"TargetObjectKeyFormat,omitempty"`
	TargetPrefix          *string                  `xml:"TargetPrefix" json:"TargetPrefix"`
}

// s3LoggingTargetGrants is the TargetGrants container, a pointer so an absent one is not rendered.
type s3LoggingTargetGrants struct {
	Grants []S3Grant `xml:"Grant" json:"Grants"`
}

// s3TargetObjectKeyFormat is TargetObjectKeyFormat (API_TargetObjectKeyFormat): one of
// PartitionedPrefix or SimplePrefix, the latter an empty element.
type s3TargetObjectKeyFormat struct {
	PartitionedPrefix *s3PartitionedPrefix `xml:"PartitionedPrefix,omitempty" json:"PartitionedPrefix,omitempty"`
	SimplePrefix      *struct{}            `xml:"SimplePrefix,omitempty" json:"SimplePrefix,omitempty"`
}

// s3PartitionedPrefix is PartitionedPrefix (API_PartitionedPrefix).
type s3PartitionedPrefix struct {
	PartitionDateSource string `xml:"PartitionDateSource,omitempty" json:"PartitionDateSource,omitempty"`
}

// s3InvalidLoggingTarget is InvalidTargetBucketForLogging/400 with message.
func s3InvalidLoggingTarget(message string) *AWSResponse {
	return s3ErrorResponse("InvalidTargetBucketForLogging", message, http.StatusBadRequest)
}

// s3LoadBucketRecord reads bucket's record, reporting false when the bucket does not exist.
func (p *S3Plugin) s3LoadBucketRecord(ctx context.Context, bucket string) (S3Bucket, bool, error) {
	data, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return S3Bucket{}, false, fmt.Errorf("get bucket %s: %w", bucket, err)
	}
	if data == nil {
		return S3Bucket{}, false, nil
	}
	var rec S3Bucket
	if err := json.Unmarshal(data, &rec); err != nil {
		return S3Bucket{}, false, fmt.Errorf("decode bucket %s: %w", bucket, err)
	}
	return rec, true, nil
}

// s3LoggingTargetRefusal returns the refusal a logging target earns for the source bucket, or nil.
// An account or Region left empty on a record written before the field existed is not compared.
func (p *S3Plugin) s3LoggingTargetRefusal(ctx context.Context, source S3Bucket, target string) (*AWSResponse, error) {
	rec, found, err := p.s3LoadBucketRecord(ctx, target)
	if err != nil {
		return nil, err
	}
	if !found {
		return s3InvalidLoggingTarget("The target bucket for logging does not exist"), nil
	}
	if source.AccountID != "" && rec.AccountID != "" && source.AccountID != rec.AccountID {
		return s3InvalidLoggingTarget("The target bucket for logging does not exist, is not owned by you, " +
			"or does not have the appropriate grants for the log-delivery group."), nil
	}
	if source.Region != "" && rec.Region != "" && source.Region != rec.Region {
		return s3ErrorResponse("CrossLocationLoggingProhibited",
			"Cross-location logging not allowed. Buckets in one geographic location cannot log "+
				"information to a bucket in another location.", http.StatusForbidden), nil
	}
	return nil, nil
}

// putBucketLogging handles PUT /<bucket>?logging. An empty BucketLoggingStatus disables logging.
func (p *S3Plugin) putBucketLogging(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	source, found, err := p.s3LoadBucketRecord(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !found {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}
	var status s3BucketLoggingStatus
	if !s3DecodeConfigXML(req, &status) {
		return s3MalformedXMLResponse(), nil
	}
	enabled := status.LoggingEnabled
	if enabled == nil {
		if err := p.state.Delete(ctx, s3Namespace, s3BucketSubresourceKey(s3LoggingKeyPrefix, bucket)); err != nil {
			return nil, fmt.Errorf("disable logging: %w", err)
		}
		return s3EmptyOK(), nil
	}
	if enabled.TargetBucket == "" || enabled.TargetPrefix == nil {
		return s3MalformedXMLResponse(), nil
	}
	if refusal, err := p.s3LoggingTargetRefusal(ctx, source, enabled.TargetBucket); refusal != nil || err != nil {
		return refusal, err
	}
	if err := p.s3StoreBucketConfig(ctx, s3LoggingKeyPrefix, bucket, s3BucketLoggingStatus{LoggingEnabled: enabled}); err != nil {
		return nil, err
	}
	return s3EmptyOK(), nil
}

// getBucketLogging handles GET /<bucket>?logging: the recorded status, or an empty one.
func (p *S3Plugin) getBucketLogging(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var status s3BucketLoggingStatus
	if _, err := p.s3LoadBucketConfig(ctx, s3LoggingKeyPrefix, bucket, &status); err != nil {
		return nil, err
	}
	status.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, status)
}
