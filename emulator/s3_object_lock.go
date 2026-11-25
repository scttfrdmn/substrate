package emulator

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// Object Lock as far as a bucket's own state goes: CreateBucket's x-amz-bucket-object-lock-enabled,
// PutObjectLockConfiguration without a Rule, and GetObjectLockConfiguration (#1658).
//
// # What is modeled
//
// CreateBucket with x-amz-bucket-object-lock-enabled: true records that the bucket has Object Lock
// enabled, and enables versioning with it: "Object Lock works only with versioned buckets", and
// "After you enable Object Lock on a bucket, you can't disable Object Lock or suspend versioning
// for that bucket" (userguide/object-lock-configure.html). PutObjectLockConfiguration enables it on
// an existing versioned bucket. GetObjectLockConfiguration then answers an ObjectLockConfiguration
// with ObjectLockEnabled Enabled and no Rule, which is what such a bucket holds until it is given a
// default retention. The AWS provider for Terraform puts and then reads that on an aws_s3_bucket
// with object_lock_enabled = true, which is ForceNew, so answering "none" would plan the bucket's
// replacement.
//
// A bucket without Object Lock answers ObjectLockConfigurationNotFoundError/404, "Object Lock
// configuration does not exist for this bucket": the code and message the AWS CLI prints for
// get-object-lock-configuration on such a bucket, quoted in hashicorp/terraform-provider-aws#7550.
//
// PutBucketVersioning refuses to suspend versioning on a bucket with Object Lock; see
// [S3Plugin.s3VersioningSuspendRefusal].
//
// # What is not
//
// Everything that acts on objects stays with #1693: a default retention Rule (a
// PutObjectLockConfiguration carrying one answers NotImplemented), the retention and legal-hold
// operations (NotImplemented), the x-amz-object-lock-* headers on PutObject (ignored), and the
// refusal of a delete of a locked version (not made).

// s3ObjectLockKeyPrefix is the state-key prefix of a bucket's Object Lock configuration. It is
// listed in [s3BucketSubresourcePrefixes], so DeleteBucket removes it.
const s3ObjectLockKeyPrefix = "bucket_object_lock:"

// s3ObjectLockConfiguration is GetObjectLockConfiguration's response body. Rule is not modeled
// (#1693), so it is never present.
type s3ObjectLockConfiguration struct {
	XMLName           xml.Name `xml:"ObjectLockConfiguration" json:"-"`
	Xmlns             string   `xml:"xmlns,attr,omitempty" json:"-"`
	ObjectLockEnabled string   `xml:"ObjectLockEnabled,omitempty" json:"ObjectLockEnabled"`
}

// s3CreateBucketObjectLockRequested reports whether a CreateBucket asks for Object Lock. The header
// is a boolean; any value other than true, in any case, leaves Object Lock off.
func s3CreateBucketObjectLockRequested(headers map[string]string) bool {
	value, ok := headerLookupFold(headers, "x-amz-bucket-object-lock-enabled")
	return ok && strings.EqualFold(strings.TrimSpace(value), "true")
}

// s3EnableObjectLockAtCreate records Object Lock and enables versioning on a bucket CreateBucket has
// just stored. Both writes follow the bucket's, for the reason its ACL's does.
func (p *S3Plugin) s3EnableObjectLockAtCreate(ctx context.Context, bucket string) error {
	if err := p.s3StoreBucketConfig(ctx, s3ObjectLockKeyPrefix, bucket,
		s3ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}); err != nil {
		return err
	}
	if err := p.state.Put(ctx, s3Namespace, "bucket_versioning:"+bucket, []byte("Enabled")); err != nil {
		return fmt.Errorf("enable versioning for object lock: %w", err)
	}
	return nil
}

// s3BucketHasObjectLock reports whether bucket was created with Object Lock enabled.
func (p *S3Plugin) s3BucketHasObjectLock(ctx context.Context, bucket string) (bool, error) {
	var cfg s3ObjectLockConfiguration
	found, err := p.s3LoadBucketConfig(ctx, s3ObjectLockKeyPrefix, bucket, &cfg)
	return found && cfg.ObjectLockEnabled == "Enabled", err
}

// getObjectLockConfiguration handles GET /<bucket>?object-lock.
func (p *S3Plugin) getObjectLockConfiguration(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var cfg s3ObjectLockConfiguration
	found, err := p.s3LoadBucketConfig(ctx, s3ObjectLockKeyPrefix, bucket, &cfg)
	if err != nil {
		return nil, err
	}
	if !found {
		return s3ErrorResponse("ObjectLockConfigurationNotFoundError",
			"Object Lock configuration does not exist for this bucket", http.StatusNotFound), nil
	}
	cfg.Xmlns = s3XMLNamespace
	return s3XMLResponse(http.StatusOK, cfg)
}

// s3ObjectLockPutBody is PutObjectLockConfiguration's request body. Rule is captured only to tell
// whether one was sent.
type s3ObjectLockPutBody struct {
	XMLName           xml.Name  `xml:"ObjectLockConfiguration"`
	ObjectLockEnabled string    `xml:"ObjectLockEnabled"`
	Rule              *struct{} `xml:"Rule"`
}

// putObjectLockConfiguration handles PUT /<bucket>?object-lock for the one form substrate models:
// ObjectLockEnabled Enabled with no Rule, which enables Object Lock ("You can enable Object Lock for
// new or existing buckets", API_PutObjectLockConfiguration) or, on a bucket that has it, changes
// nothing. The AWS provider for Terraform sends exactly that after creating a bucket with
// object_lock_enabled = true.
//
//   - A Rule is a default retention, which is #1693: NotImplemented/501, the code S3's error-code
//     list gives for "functionality that is not implemented".
//   - A body that does not decode, or whose ObjectLockEnabled is not Enabled (its only valid
//     value), is MalformedXML. An absent ObjectLockEnabled is refused the same way, which is
//     substrate's choice: the member is Required: No, and no source says what S3 does without it.
//   - A bucket whose versioning is not Enabled is InvalidBucketState/409, "Versioning must be
//     'Enabled' on the bucket to apply a Object Lock configuration", S3's answer as
//     dev.classmethod.jp/articles/amazon-s3-enabling-object-lock-buckets reports it.
//
// x-amz-bucket-object-lock-token is not read.
func (p *S3Plugin) putObjectLockConfiguration(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if resp, err := p.s3RequireBucket(ctx, bucket); resp != nil || err != nil {
		return resp, err
	}
	var body s3ObjectLockPutBody
	if !s3DecodeConfigXML(req, &body) || body.ObjectLockEnabled != "Enabled" {
		return s3MalformedXMLResponse(), nil
	}
	if body.Rule != nil {
		return nil, &AWSError{
			Code:       "NotImplemented",
			Message:    "S3 operation not yet implemented: PutObjectLockConfiguration with a default retention Rule",
			HTTPStatus: http.StatusNotImplemented,
		}
	}
	locked, err := p.s3BucketHasObjectLock(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if locked {
		return s3EmptyOK(), nil
	}
	if p.getBucketVersioningStatus(ctx, bucket) != "Enabled" {
		return s3ErrorResponse("InvalidBucketState",
			"Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration", http.StatusConflict), nil
	}
	if err := p.s3StoreBucketConfig(ctx, s3ObjectLockKeyPrefix, bucket,
		s3ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}); err != nil {
		return nil, err
	}
	return s3EmptyOK(), nil
}

// s3VersioningSuspendRefusal returns the refusal PutBucketVersioning answers for suspending
// versioning on bucket, or nil when the bucket's state allows it.
//
//   - With Object Lock: InvalidBucketState/409, "An Object Lock configuration is present on this
//     bucket, so the versioning state cannot be changed." That is S3's answer as the AWS provider
//     for Terraform reports it (terraform-aws-modules/terraform-aws-s3-bucket#90, status 409).
//   - With a replication configuration: "If you attempt to disable versioning on the source
//     bucket, Amazon S3 returns an error. You must remove the replication configuration before you
//     can disable versioning on the source bucket" (userguide/replication-requirements.html). The
//     page names no code. InvalidBucketState/409 is API_Error's code for "The request is not valid
//     with the current state of the bucket.", and that description is the message, since no source
//     publishes S3's own text for this case.
func (p *S3Plugin) s3VersioningSuspendRefusal(ctx context.Context, bucket string) (*AWSResponse, error) {
	locked, err := p.s3BucketHasObjectLock(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if locked {
		return s3ErrorResponse("InvalidBucketState",
			"An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.",
			http.StatusConflict), nil
	}
	replication, err := p.state.Get(ctx, s3Namespace, s3BucketSubresourceKey(s3ReplicationKeyPrefix, bucket))
	if err != nil {
		return nil, fmt.Errorf("check replication configuration: %w", err)
	}
	if replication != nil {
		return s3ErrorResponse("InvalidBucketState",
			"The request is not valid with the current state of the bucket.", http.StatusConflict), nil
	}
	return nil, nil
}
