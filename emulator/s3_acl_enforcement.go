package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// What an ACL write is answered when the bucket's Object Ownership disables ACLs (#1474), and when
// it names an email-address grantee (#507).
//
// # Object Ownership
//
// "After you apply the Bucket owner enforced setting for Object Ownership, ACLs are disabled.
// Requests to set ACLs or update ACLs fail with a 400 error and return the
// AccessControlListNotSupported error code. Requests to read ACLs are still supported. Requests to
// read ACLs always return a response that shows full control for the bucket owner. In your PUT
// operations, you must either specify bucket owner full control ACLs or not specify an ACL"
// (S3 User Guide, Object Ownership troubleshooting). The forms that count as "bucket owner full
// control ACLs" are "the bucket-owner-full-control canned ACL or equivalent forms of this ACL
// expressed in XML" (S3 User Guide, Controlling ownership of objects and disabling ACLs).
//
// So under BucketOwnerEnforced every ACL write — PutBucketAcl, PutObjectAcl, and the ACL headers on
// PutObject, CopyObject and CreateMultipartUpload — is refused unless it is one of those forms, and
// a write naming no ACL is unaffected. The refusal is checked before Block Public Access and before
// the email-grantee refusal, because a bucket that does not allow ACLs refuses every ACL whatever it
// grants. Reads report the owner-full-control ACL, and a stored ACL is kept rather than deleted:
// "you can re-enable ACLs at any time after you disable them, and your preexisting bucket and
// object ACLs are restored".
//
// CreateBucket is the one ACL write answered with a different code. The same page: "if your
// CreateBucket request sets Bucket owner enforced and specifies a bucket ACL that provides access to
// an external AWS account, your request fails with a 400 error and returns the
// InvalidBucketAclWithObjectOwnership error code", and PutBucketOwnershipControls setting
// BucketOwnerEnforced on a bucket whose ACL "grants permissions to others" fails the same way. A
// CreateBucket ACL that grants only the owner (private, for instance) is accepted, since the page's
// condition is an ACL giving access to someone else.
//
// BucketOwnerPreferred and ObjectWriter keep ACLs enabled, and so does a bucket with no ownership
// controls at all — the state DeleteBucketOwnershipControls leaves, and the state every bucket was in
// before Object Ownership existed, when the object writer owned what it wrote.
//
// # Email-address grantees
//
// Every ACL-bearing operation's API reference carries the same notice: "As of October 1, 2025,
// Amazon S3 has discontinued support for Email Grantee Access Control Lists (ACLs). If you attempt to
// use an Email Grantee ACL in a request after October 1, 2025, the request will receive an HTTP 405
// (Method Not Allowed) error. This change affects the following AWS Regions: US East (N. Virginia),
// US West (N. California), US West (Oregon), Asia Pacific (Singapore), Asia Pacific (Sydney), Asia
// Pacific (Tokyo), Europe (Ireland), and South America (São Paulo)." Substrate's clock is past that
// date, so in those eight Regions an email grantee — `emailAddress=` in an x-amz-grant-* header, or
// an `AmazonCustomerByEmail` grantee in an XML body — is refused with the 405. The notice publishes a
// status and nothing else; the code is MethodNotAllowed, the code S3's error list gives HTTP 405,
// and the message is substrate's own.
//
// Outside the eight Regions email grantees were never supported, so the notice does not apply and
// publishes nothing for them. Substrate refuses there too, with 400 InvalidArgument and its own
// message, rather than silently dropping the grantee as it did before #507; that code is substrate's
// reading, not a capture.

// s3OwnershipEnforced is the ObjectOwnership value that disables ACLs.
const s3OwnershipEnforced = "BucketOwnerEnforced"

// s3CannedBucketOwnerFullControl is the one canned ACL a bucket with ACLs disabled accepts.
const s3CannedBucketOwnerFullControl = "bucket-owner-full-control"

// s3EmailGranteeXMLType is the xsi:type of an email-address grantee in an XML ACL body.
const s3EmailGranteeXMLType = "AmazonCustomerByEmail"

// s3BucketObjectOwnership returns the bucket's ObjectOwnership, or "" when it has no ownership
// controls.
func (p *S3Plugin) s3BucketObjectOwnership(ctx context.Context, bucket string) (string, error) {
	raw, err := p.state.Get(ctx, s3Namespace, s3OwnershipControlsKeyPrefix+bucket)
	if err != nil {
		return "", fmt.Errorf("get ownership controls: %w", err)
	}
	if raw == nil {
		return "", nil
	}
	var cfg s3OwnershipControls
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", fmt.Errorf("unmarshal ownership controls: %w", err)
	}
	if len(cfg.Rules) == 0 {
		return "", nil
	}
	return cfg.Rules[0].ObjectOwnership, nil
}

// s3ACLsDisabled reports whether the bucket's Object Ownership disables ACLs.
func (p *S3Plugin) s3ACLsDisabled(ctx context.Context, bucket string) (bool, error) {
	ownership, err := p.s3BucketObjectOwnership(ctx, bucket)
	if err != nil {
		return false, err
	}
	return ownership == s3OwnershipEnforced, nil
}

// s3ACLNotSupportedResponse is the 400 an ACL write on a bucket with ACLs disabled answers. The
// message is the one the Object Ownership troubleshooting page shows for it.
func s3ACLNotSupportedResponse() *AWSResponse {
	return s3ErrorResponse("AccessControlListNotSupported", "The bucket does not allow ACLs",
		http.StatusBadRequest)
}

// s3InvalidBucketACLWithOwnershipResponse is the 400 a CreateBucket or PutBucketOwnershipControls
// answers when it would leave BucketOwnerEnforced on a bucket whose ACL grants someone other than
// the owner. The message is the one the Object Ownership troubleshooting page shows for it.
func s3InvalidBucketACLWithOwnershipResponse() *AWSResponse {
	return s3ErrorResponse("InvalidBucketAclWithObjectOwnership",
		"Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting",
		http.StatusBadRequest)
}

// s3ACLOwnerID is the canonical user ID substrate reports as a bucket's owner, derived here and
// nowhere else: [s3DefaultACL] and the Object Ownership checks both call it.
func s3ACLOwnerID(bucket string) string { return bucket + "-owner" }

// s3ACLGrantsOnlyOwner reports whether every grant in acl names the bucket owner.
func s3ACLGrantsOnlyOwner(acl S3AccessControlList, bucket string) bool {
	for _, g := range acl.Grants {
		if g.Grantee.URI != "" || g.Grantee.ID != s3ACLOwnerID(bucket) || s3GranteeIsEmail(g.Grantee) {
			return false
		}
	}
	return true
}

// s3ACLIsOwnerFullControl reports whether acl is a bucket-owner-full-control ACL expressed as
// grants: at least one grant, every one of them FULL_CONTROL to the bucket owner.
func s3ACLIsOwnerFullControl(acl S3AccessControlList, bucket string) bool {
	if len(acl.Grants) == 0 || !s3ACLGrantsOnlyOwner(acl, bucket) {
		return false
	}
	for _, g := range acl.Grants {
		if g.Permission != s3PermissionFull {
			return false
		}
	}
	return true
}

// s3ACLAcceptedWhenDisabled reports whether an ACL write is one of the forms a bucket with ACLs
// disabled accepts: the bucket-owner-full-control canned ACL, or grants — in an XML body or in
// x-amz-grant-* headers — of FULL_CONTROL to the bucket owner and nobody else.
//
// acl is the ACL the request resolved to; fromBody says it came from an XML body rather than from
// headers. Any other canned ACL is refused, private included: it is not a bucket owner full control
// ACL, and the guide names no exception for it.
func s3ACLAcceptedWhenDisabled(headers map[string]string, acl S3AccessControlList, fromBody bool, bucket string) bool {
	if fromBody {
		return s3ACLIsOwnerFullControl(acl, bucket)
	}
	if s3HeadersNameEmailGrantee(headers) {
		return false
	}
	if grants := s3GrantsFromHeaders(headers); len(grants) > 0 {
		return s3ACLIsOwnerFullControl(S3AccessControlList{Grants: grants}, bucket)
	}
	return headerValueFold(headers, s3CannedACLHeader) == s3CannedBucketOwnerFullControl
}

// s3EmailGranteeRegion reports whether region is one of the eight the end-of-support notice names.
func s3EmailGranteeRegion(region string) bool {
	switch region {
	case "us-east-1", "us-west-1", "us-west-2", "ap-southeast-1", "ap-southeast-2",
		"ap-northeast-1", "eu-west-1", "sa-east-1":
		return true
	}
	return false
}

// s3EmailGranteeRefusal is the answer to a request naming an email-address grantee in region: the
// notice's 405 in the eight Regions it names, and a 400 elsewhere. Both messages are substrate's
// own; see this file's comment for the provenance of each code.
func s3EmailGranteeRefusal(region string) *AWSResponse {
	if region == "" || s3EmailGranteeRegion(region) {
		return s3ErrorResponse("MethodNotAllowed",
			"Email grantee ACLs are no longer supported", http.StatusMethodNotAllowed)
	}
	return s3ErrorResponse("InvalidArgument",
		"Email grantee ACLs are not supported in this Region", http.StatusBadRequest)
}

// s3GranteeIsEmail reports whether an XML grantee names an account by email address.
func s3GranteeIsEmail(g S3Grantee) bool {
	return strings.EqualFold(g.Type, s3EmailGranteeXMLType) || g.EmailAddress != ""
}

// s3ACLNamesEmailGrantee reports whether any grant in acl names an email-address grantee.
func s3ACLNamesEmailGrantee(acl S3AccessControlList) bool {
	for _, g := range acl.Grants {
		if s3GranteeIsEmail(g.Grantee) {
			return true
		}
	}
	return false
}

// s3HeadersNameEmailGrantee reports whether any x-amz-grant-* header names an emailAddress grantee,
// read through [s3ParseGrantees] so the refusal and the ACL resolver split the header one way.
func s3HeadersNameEmailGrantee(headers map[string]string) bool {
	return s3ACLNamesEmailGrantee(S3AccessControlList{Grants: s3GrantsFromHeaders(headers)})
}

// s3BucketRegion returns the Region in a bucket's stored record. record is the record the caller
// already read, or nil to have it read here; a bucket with no readable record has Region "".
func (p *S3Plugin) s3BucketRegion(ctx context.Context, bucket string, record []byte) (string, error) {
	if record == nil {
		raw, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
		if err != nil {
			return "", fmt.Errorf("get bucket: %w", err)
		}
		if raw == nil {
			return "", nil
		}
		record = raw
	}
	var b S3Bucket
	if err := json.Unmarshal(record, &b); err != nil {
		return "", fmt.Errorf("unmarshal bucket: %w", err)
	}
	return b.Region, nil
}

// s3ACLWriteRefusal returns the answer to an ACL write on bucket that may not proceed, or nil when
// it may, checking in order: Object Ownership (400 AccessControlListNotSupported), an email-address
// grantee (405, or 400 outside the notice's Regions), and Block Public Access (403).
//
// bucketRecord is the bucket's stored record when the caller has already read it, which every
// caller but PutObjectAcl has; it is decoded only for an email grantee's Region, and read here only
// if the caller passed nil and the Region is needed. acl is the ACL the request resolved to, or nil
// for a create that named none, which proceeds without consulting the bucket at all — there is
// nothing for any of the three checks to find. fromBody says the ACL came from an XML body. The
// error return is reserved for a state-store failure.
func (p *S3Plugin) s3ACLWriteRefusal(ctx context.Context, bucket string, bucketRecord []byte,
	headers map[string]string, acl *S3AccessControlList, fromBody bool) (*AWSResponse, error) {
	if acl == nil {
		return nil, nil
	}
	disabled, err := p.s3ACLsDisabled(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if disabled && !s3ACLAcceptedWhenDisabled(headers, *acl, fromBody, bucket) {
		return s3ACLNotSupportedResponse(), nil
	}
	if s3HeadersNameEmailGrantee(headers) || s3ACLNamesEmailGrantee(*acl) {
		region, err := p.s3BucketRegion(ctx, bucket, bucketRecord)
		if err != nil {
			return nil, err
		}
		return s3EmailGranteeRefusal(region), nil
	}
	return p.s3PublicACLDenied(ctx, bucket, *acl, headers)
}

// s3CreateBucketACLRefusal returns the answer to a CreateBucket whose ACL may not be applied, or nil
// when it may: InvalidBucketAclWithObjectOwnership when the bucket is being created with
// BucketOwnerEnforced and its ACL grants someone other than the owner, then the email-grantee
// refusal. Block Public Access is not consulted; see [S3Plugin.s3CreateBucketACL].
func s3CreateBucketACLRefusal(headers map[string]string, bucket, ownership, region string) *AWSResponse {
	acl := s3RequestACL(headers, bucket, s3ACLBucket)
	if acl == nil {
		return nil
	}
	email := s3HeadersNameEmailGrantee(headers)
	if ownership == s3OwnershipEnforced && (email || !s3ACLGrantsOnlyOwner(*acl, bucket)) {
		return s3InvalidBucketACLWithOwnershipResponse()
	}
	if email {
		return s3EmailGranteeRefusal(region)
	}
	return nil
}

// s3StoredBucketACLGrantsOthers reports whether the ACL stored for bucket grants anyone but the
// owner, which is what keeps PutBucketOwnershipControls from setting BucketOwnerEnforced.
func (p *S3Plugin) s3StoredBucketACLGrantsOthers(ctx context.Context, bucket string) (bool, error) {
	raw, err := p.state.Get(ctx, s3Namespace, "bucket_acl:"+bucket)
	if err != nil {
		return false, fmt.Errorf("get bucket acl: %w", err)
	}
	if raw == nil {
		return false, nil
	}
	var acl S3AccessControlList
	if err := json.Unmarshal(raw, &acl); err != nil {
		return false, fmt.Errorf("unmarshal bucket acl: %w", err)
	}
	return !s3ACLGrantsOnlyOwner(acl, bucket), nil
}
