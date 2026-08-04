package emulator

import (
	"encoding/xml"
	"time"
)

// S3Bucket holds metadata for an emulated S3 bucket.
type S3Bucket struct {
	// Name is the globally unique bucket name.
	Name string `json:"name"`

	// Region is the AWS region in which the bucket was created.
	Region string `json:"region"`

	// CreationDate is the time at which the bucket was created.
	CreationDate time.Time `json:"creation_date"`

	// Tags holds optional user-defined key-value tags on the bucket.
	Tags map[string]string `json:"tags"`
}

// S3Object holds metadata for an emulated S3 object. The object body is
// stored separately on the afero filesystem.
type S3Object struct {
	// Bucket is the name of the containing bucket.
	Bucket string `json:"bucket"`

	// Key is the object key within the bucket.
	Key string `json:"key"`

	// ETag is the entity tag, computed as the MD5 hex digest of the body
	// wrapped in double-quotes (e.g. `"d41d8cd98f00b204e9800998ecf8427e"`).
	ETag string `json:"etag"`

	// ContentType is the MIME type of the object body.
	ContentType string `json:"content_type"`

	// ContentEncoding is the encoding of the object body (e.g. "gzip"), set via
	// the Content-Encoding header on PutObject or on CreateMultipartUpload —
	// Complete accepts no metadata headers, so a multipart object's encoding is
	// carried on the upload record from creation. The aws-chunked transfer
	// encoding is never recorded here; see [s3PersistedContentEncoding].
	ContentEncoding string `json:"content_encoding,omitempty"`

	// S3SystemMetadata carries Cache-Control, Content-Disposition,
	// Content-Language and Expires. Embedded so it is declared once and shared
	// with [S3MultipartUpload], which is what keeps the two write paths from
	// drifting on the family.
	S3SystemMetadata

	// S3ServerSideEncryption carries the x-amz-server-side-encryption family
	// recorded on write and echoed on every read. Embedded and shared with
	// [S3MultipartUpload] for the same reason S3SystemMetadata is; no cryptography
	// is performed, and the type's own doc says why that is the right boundary.
	S3ServerSideEncryption

	// Size is the byte length of the object body.
	Size int64 `json:"size"`

	// StorageClass is the S3 storage class of the object, set via the
	// x-amz-storage-class header on write. Empty is equivalent to STANDARD.
	StorageClass string `json:"storage_class,omitempty"`

	// Checksum is the additional checksum recorded for the object, from the
	// x-amz-checksum-* family. Zero when the object was written without one;
	// returned on GetObject/HeadObject only under x-amz-checksum-mode: ENABLED.
	Checksum s3Checksum `json:"checksum,omitzero"`

	// LastModified is the time of the most recent write.
	LastModified time.Time `json:"last_modified"`

	// UserMetadata holds key-value pairs set via X-Amz-Meta-* request headers.
	// Keys are stored in lowercase without the x-amz-meta- prefix.
	UserMetadata map[string]string `json:"user_metadata"`

	// Tags holds optional user-defined key-value tags on the object.
	Tags map[string]string `json:"tags,omitempty"`

	// VersionID is the version identifier when bucket versioning is enabled.
	// Empty for unversioned objects.
	VersionID string `json:"version_id,omitempty"`

	// IsDeleteMarker indicates this is a versioning delete marker rather than
	// an actual object.
	IsDeleteMarker bool `json:"is_delete_marker,omitempty"`
}

// S3VersioningConfiguration holds the versioning state for a bucket.
type S3VersioningConfiguration struct {
	// Status is "Enabled", "Suspended", or "" (never enabled).
	Status string `xml:"Status" json:"Status"`
}

// S3ObjectVersion holds the metadata for one version of an object in a
// ListObjectVersions response.
type S3ObjectVersion struct {
	// Key is the object key.
	Key string `xml:"Key"`

	// VersionID is the version identifier.
	VersionID string `xml:"VersionId"`

	// IsLatest is true when this is the current version.
	IsLatest bool `xml:"IsLatest"`

	// LastModified is the ISO-8601 timestamp of the version.
	LastModified string `xml:"LastModified"`

	// ETag is the entity tag.
	ETag string `xml:"ETag"`

	// Size is the byte length of the object body.
	Size int64 `xml:"Size"`

	// StorageClass is the S3 storage class of this version.
	StorageClass string `xml:"StorageClass"`
}

// S3DeleteMarker holds metadata for one delete marker in a ListObjectVersions
// response.
type S3DeleteMarker struct {
	// Key is the object key.
	Key string `xml:"Key"`

	// VersionID is the version identifier of the delete marker.
	VersionID string `xml:"VersionId"`

	// IsLatest is true when this delete marker is the current version.
	IsLatest bool `xml:"IsLatest"`

	// LastModified is the ISO-8601 timestamp of the delete marker.
	LastModified string `xml:"LastModified"`
}

// ListObjectVersionsResult is the XML response body for ListObjectVersions.
type ListObjectVersionsResult struct {
	XMLName       xml.Name          `xml:"ListVersionsResult"`
	Xmlns         string            `xml:"xmlns,attr"`
	Name          string            `xml:"Name"`
	Prefix        string            `xml:"Prefix"`
	MaxKeys       int               `xml:"MaxKeys"`
	IsTruncated   bool              `xml:"IsTruncated"`
	Versions      []S3ObjectVersion `xml:"Version"`
	DeleteMarkers []S3DeleteMarker  `xml:"DeleteMarker"`
}

// S3MultipartUpload holds state for an in-progress multipart upload.
type S3MultipartUpload struct {
	// UploadID is the unique identifier for this multipart upload.
	UploadID string `json:"upload_id"`

	// Bucket is the destination bucket.
	Bucket string `json:"bucket"`

	// Key is the destination object key.
	Key string `json:"key"`

	// ContentType is the MIME type supplied at upload creation.
	ContentType string `json:"content_type"`

	// ContentEncoding is the Content-Encoding supplied at upload creation (e.g.
	// "gzip"), applied to the object CompleteMultipartUpload assembles. Create is
	// the only place it can be supplied: Complete accepts no object-metadata
	// headers, so an encoding not carried here is lost for good. The aws-chunked
	// transfer encoding is never recorded here; see [s3PersistedContentEncoding].
	ContentEncoding string `json:"content_encoding,omitempty"`

	// S3SystemMetadata carries the Cache-Control, Content-Disposition,
	// Content-Language and Expires headers supplied at upload creation, applied to
	// the object CompleteMultipartUpload assembles. Create is the only place they
	// can be supplied, for the reason ContentEncoding is. Embedded so the field set
	// is literally the same declaration [S3Object] uses and Complete can carry the
	// family across in one assignment.
	S3SystemMetadata

	// S3ServerSideEncryption carries the encryption family supplied at upload
	// creation, applied to the object CompleteMultipartUpload assembles. Create is
	// the only place it can be supplied — Complete's request accepts only the SSE-C
	// headers, so encryption is fixed for the whole upload at creation.
	//
	// Embedded here as well as on [S3Object] so the family is one declaration and
	// Complete carries it across in a single assignment. Resolving a bucket default
	// onto an upload is #493's; recording what the create named is this release's.
	S3ServerSideEncryption

	// StorageClass is the storage class supplied at upload creation, applied to
	// the object CompleteMultipartUpload assembles. Empty means STANDARD.
	StorageClass string `json:"storage_class,omitempty"`

	// ACL is the access control list the x-amz-acl or x-amz-grant-* headers named at
	// upload creation, applied to the object CompleteMultipartUpload assembles. Nil
	// when the create named none, which is what makes the assembled object report the
	// default owner-only ACL rather than a stored copy of it.
	//
	// Create is the only place an ACL can be supplied — Complete's request accepts no
	// ACL header, exactly as it accepts no encryption header — so a multipart object's
	// ACL is fixed for the whole upload at creation. A pointer rather than a value
	// because "named none" and "named private" are different observations here in the
	// same way they are on [S3Plugin.s3StoreObjectACL].
	ACL *S3AccessControlList `json:"acl,omitempty"`

	// ChecksumAlgorithm is the algorithm named in x-amz-checksum-algorithm at
	// upload creation, applied to every part and to the assembled object. Empty
	// when the upload was created without one.
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`

	// ChecksumType is COMPOSITE or FULL_OBJECT, deciding how the part checksums
	// combine into the object's. Empty when ChecksumAlgorithm is empty.
	ChecksumType string `json:"checksum_type,omitempty"`

	// Initiated is the time the multipart upload was created.
	Initiated time.Time `json:"initiated"`

	// UserMetadata holds x-amz-meta-* headers supplied at upload creation.
	UserMetadata map[string]string `json:"user_metadata,omitempty"`
}

// S3Part holds metadata for one part within a multipart upload. The part body
// is stored separately on the afero filesystem.
type S3Part struct {
	// PartNumber is the 1-based index of this part within the upload.
	PartNumber int `json:"part_number"`

	// ETag is the MD5 entity tag of this part's body.
	ETag string `json:"etag"`

	// Size is the byte length of the part body.
	Size int64 `json:"size"`

	// Checksum is the additional checksum of this part's body, under the upload's
	// algorithm. A COMPOSITE object checksum is derived from these.
	Checksum s3Checksum `json:"checksum,omitzero"`

	// LastModified is the time this part was uploaded.
	LastModified time.Time `json:"last_modified"`
}

// S3BucketPolicy stores the raw JSON policy document for an S3 bucket.
type S3BucketPolicy struct {
	// Policy is the bucket policy as a raw JSON string.
	Policy string `json:"Policy"`
}

// S3PublicAccessBlockConfiguration is a bucket's Block Public Access settings,
// the body of PutPublicAccessBlock and GetPublicAccessBlock (#446).
//
// The four members are each `Required: No` on PutPublicAccessBlock, so a caller
// may name a subset. They are plain bools rather than pointers because S3 reports
// an omitted member as `false` rather than omitting it from the GET response —
// "absent" and "explicitly false" are the same observation, so a third state
// would be one substrate could produce but real S3 never returns.
//
// Substrate enforces the two request-time settings: BlockPublicAcls refuses a
// public ACL on PutBucketAcl/PutObjectAcl and BlockPublicPolicy refuses a public
// bucket policy on PutBucketPolicy, both with 403 AccessDenied and without storing
// anything (#458). See s3_publicaccess.go for the definitions of "public", which
// are not the obvious ones.
//
// IgnorePublicAcls and RestrictPublicBuckets stay recorded-only. Both govern how an
// incoming request is evaluated against an ACL or policy already in place rather
// than which write is refused, and substrate has no unauthenticated or
// cross-account request path to deny — every request it serves is already the
// bucket owner's.
type S3PublicAccessBlockConfiguration struct {
	XMLName xml.Name `xml:"PublicAccessBlockConfiguration" json:"-"`

	// Xmlns is echoed on the GET response to match S3's wire shape. It is
	// ignored on input.
	Xmlns string `xml:"xmlns,attr,omitempty" json:"-"`

	// BlockPublicAcls rejects PutBucketAcl/PutObjectAcl calls carrying a public ACL.
	BlockPublicAcls bool `xml:"BlockPublicAcls" json:"block_public_acls"`

	// IgnorePublicAcls causes existing public ACLs to be disregarded.
	IgnorePublicAcls bool `xml:"IgnorePublicAcls" json:"ignore_public_acls"`

	// BlockPublicPolicy rejects a PutBucketPolicy that would allow public access.
	BlockPublicPolicy bool `xml:"BlockPublicPolicy" json:"block_public_policy"`

	// RestrictPublicBuckets limits a publicly-policied bucket to AWS service
	// principals and authorized users in the owning account.
	RestrictPublicBuckets bool `xml:"RestrictPublicBuckets" json:"restrict_public_buckets"`
}

// S3AccessControlList is the S3 access control list XML structure.
type S3AccessControlList struct {
	XMLName xml.Name  `xml:"AccessControlPolicy" json:"-"`
	Owner   S3Owner   `xml:"Owner" json:"Owner"`
	Grants  []S3Grant `xml:"AccessControlList>Grant" json:"Grants"`
}

// S3Owner represents the owner element in an S3 ACL.
type S3Owner struct {
	ID          string `xml:"ID" json:"ID"`
	DisplayName string `xml:"DisplayName" json:"DisplayName"`
}

// S3Grant represents a single grant in an S3 ACL.
type S3Grant struct {
	Grantee    S3Grantee `xml:"Grantee" json:"Grantee"`
	Permission string    `xml:"Permission" json:"Permission"`
}

// S3Grantee represents the grantee element in an S3 ACL grant.
type S3Grantee struct {
	Type        string `xml:"type,attr" json:"Type"`
	ID          string `xml:"ID,omitempty" json:"ID,omitempty"`
	URI         string `xml:"URI,omitempty" json:"URI,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty" json:"DisplayName,omitempty"`
}

// S3NotificationConfiguration holds event notification configurations for an S3
// bucket. This is the *state* encoding: it is what S3Plugin persists under
// "notification:<bucket>" and what fireNotifications reads back to decide where
// an event goes.
//
// It deliberately carries no xml tags. The wire encoding is a different shape —
// S3 names each configuration element in the singular (TopicConfiguration, not
// TopicConfigurations), names the destination after the service rather than the
// field (Topic, Queue, CloudFunction rather than TopicArn, QueueArn,
// LambdaFunctionArn) and repeats a bare Event element per event — so it lives in
// the s3Notification*Wire types below with projections both ways, following the
// remedy #528 established for CloudWatch Logs.
//
// Retagging this struct instead would have re-created the conflation that made
// #542 invisible: xml.Unmarshal falls back to matching Go field *names*, so a
// real-S3 body matched nothing here, returned no error, and persisted an empty
// configuration. A round-trip test could not see it, because a struct
// marshaled and unmarshaled by its own definition agrees with itself.
type S3NotificationConfiguration struct {
	// LambdaFunctionConfigurations lists Lambda invocation notification configs.
	LambdaFunctionConfigurations []S3LambdaFunctionConfiguration `json:"LambdaFunctionConfigurations,omitempty"`

	// QueueConfigurations lists SQS queue notification configs.
	QueueConfigurations []S3QueueConfiguration `json:"QueueConfigurations,omitempty"`

	// TopicConfigurations lists SNS topic notification configs.
	TopicConfigurations []S3TopicConfiguration `json:"TopicConfigurations,omitempty"`

	// EventBridgeEnabled records that the bucket asked for delivery to
	// EventBridge. Substrate stores and reports the choice so a caller can read
	// back what it configured, but does not dispatch to EventBridge; see
	// docs/services.md.
	EventBridgeEnabled bool `json:"EventBridgeEnabled,omitempty"`
}

// S3LambdaFunctionConfiguration configures event notifications to a Lambda function.
type S3LambdaFunctionConfiguration struct {
	// ID is the optional unique identifier for this configuration.
	ID string `json:"Id,omitempty"`

	// LambdaFunctionArn is the ARN of the Lambda function to invoke.
	LambdaFunctionArn string `json:"LambdaFunctionArn"`

	// Events is the list of S3 event types that trigger this notification.
	Events []string `json:"Events"`

	// Filter holds optional object key name filter rules.
	Filter *S3NotificationFilter `json:"Filter,omitempty"`
}

// S3QueueConfiguration configures event notifications to an SQS queue.
type S3QueueConfiguration struct {
	// ID is the optional unique identifier for this configuration.
	ID string `json:"Id,omitempty"`

	// QueueArn is the ARN of the SQS queue to send messages to.
	QueueArn string `json:"QueueArn"`

	// Events is the list of S3 event types that trigger this notification.
	Events []string `json:"Events"`

	// Filter holds optional object key name filter rules.
	Filter *S3NotificationFilter `json:"Filter,omitempty"`
}

// S3TopicConfiguration configures event notifications to an SNS topic.
type S3TopicConfiguration struct {
	// ID is the optional unique identifier for this configuration.
	ID string `json:"Id,omitempty"`

	// TopicArn is the ARN of the SNS topic.
	TopicArn string `json:"TopicArn"`

	// Events is the list of S3 event types that trigger this notification.
	Events []string `json:"Events"`

	// Filter holds optional object key name filter rules.
	Filter *S3NotificationFilter `json:"Filter,omitempty"`
}

// S3NotificationFilter holds filter rules for S3 event notification configurations.
type S3NotificationFilter struct {
	// Key contains filter rules on the object key name.
	Key S3KeyFilter `json:"Key"`
}

// S3KeyFilter holds filter rules based on object key name patterns.
type S3KeyFilter struct {
	// FilterRules is the list of filter rules applied to the key name.
	FilterRules []S3FilterRule `json:"FilterRules"`
}

// S3FilterRule defines a single prefix or suffix filter for S3 notifications.
type S3FilterRule struct {
	// Name is either "prefix" or "suffix".
	Name string `json:"Name"`

	// Value is the prefix or suffix string to match against.
	Value string `json:"Value"`
}

// The wire encoding of a bucket notification configuration, per the
// Put/GetBucketNotificationConfiguration references. These types exist only to
// be marshaled and unmarshaled; the persisted shape is
// [S3NotificationConfiguration], and the projections below are the one place the
// two are allowed to meet.
//
// Three differences from the state shape are what #542 turned on, and each is a
// silent failure rather than a parse error:
//
//   - Every configuration element is singular. A body carrying
//     <QueueConfiguration> does not populate a field named QueueConfigurations.
//   - The destination element is named for the service — <Topic>, <Queue>,
//     <CloudFunction> — not for the ARN it holds.
//   - Events repeat as bare <Event> elements rather than nesting under an
//     <Events> parent.
//
// xml.Unmarshal reports no error for a body none of whose elements it
// recognizes, so all three read as "an empty configuration was submitted".

// s3NotificationConfigurationWire is the wire form of a bucket's notification
// configuration.
type s3NotificationConfigurationWire struct {
	XMLName xml.Name `xml:"NotificationConfiguration"`

	// Xmlns is the S3 namespace, emitted on responses. It is omitted when empty
	// so an unmarshaled request does not have to carry it.
	Xmlns string `xml:"xmlns,attr,omitempty"`

	TopicConfigurations  []s3TopicConfigurationWire  `xml:"TopicConfiguration"`
	QueueConfigurations  []s3QueueConfigurationWire  `xml:"QueueConfiguration"`
	LambdaConfigurations []s3LambdaConfigurationWire `xml:"CloudFunctionConfiguration"`

	// EventBridge is present-but-empty when the bucket enables EventBridge
	// delivery, so a pointer distinguishes "the element was there" from "it was
	// not" — there is no field inside it to test.
	EventBridge *s3EventBridgeConfigurationWire `xml:"EventBridgeConfiguration"`
}

// s3TopicConfigurationWire is the wire form of an SNS topic notification config.
type s3TopicConfigurationWire struct {
	ID     string                    `xml:"Id,omitempty"`
	Topic  string                    `xml:"Topic"`
	Events []string                  `xml:"Event"`
	Filter *s3NotificationFilterWire `xml:"Filter,omitempty"`
}

// s3QueueConfigurationWire is the wire form of an SQS queue notification config.
type s3QueueConfigurationWire struct {
	ID     string                    `xml:"Id,omitempty"`
	Queue  string                    `xml:"Queue"`
	Events []string                  `xml:"Event"`
	Filter *s3NotificationFilterWire `xml:"Filter,omitempty"`
}

// s3LambdaConfigurationWire is the wire form of a Lambda notification config.
// Its element is CloudFunctionConfiguration and its destination CloudFunction:
// the SDKs call the member LambdaFunctionConfigurations, but the XML kept the
// original names.
type s3LambdaConfigurationWire struct {
	ID            string                    `xml:"Id,omitempty"`
	CloudFunction string                    `xml:"CloudFunction"`
	Events        []string                  `xml:"Event"`
	Filter        *s3NotificationFilterWire `xml:"Filter,omitempty"`
}

// s3EventBridgeConfigurationWire is the wire form of the EventBridge element,
// which the reference documents as carrying no members.
type s3EventBridgeConfigurationWire struct{}

// s3NotificationFilterWire is the wire form of a notification's key filter. The
// state shape names the inner element Key; the wire names it S3Key.
type s3NotificationFilterWire struct {
	Key s3KeyFilterWire `xml:"S3Key"`
}

// s3KeyFilterWire is the wire form of the key filter's rule list, which is a
// repeated FilterRule rather than a FilterRules parent.
type s3KeyFilterWire struct {
	FilterRules []s3FilterRuleWire `xml:"FilterRule"`
}

// s3FilterRuleWire is the wire form of one prefix or suffix rule.
type s3FilterRuleWire struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// isEmpty reports whether the wire configuration named no destination at all.
// An empty NotificationConfiguration is how the API documents "disable
// notifications", so it is a legitimate body — but it is also what a body whose
// elements were all unrecognized parses to, which is why the handler needs to
// tell the two apart by looking at whether the body was empty to begin with.
func (w s3NotificationConfigurationWire) isEmpty() bool {
	return len(w.TopicConfigurations) == 0 && len(w.QueueConfigurations) == 0 &&
		len(w.LambdaConfigurations) == 0 && w.EventBridge == nil
}

// s3NotificationState projects a wire configuration onto the persisted shape.
func s3NotificationState(w s3NotificationConfigurationWire) S3NotificationConfiguration {
	cfg := S3NotificationConfiguration{EventBridgeEnabled: w.EventBridge != nil}
	for _, t := range w.TopicConfigurations {
		cfg.TopicConfigurations = append(cfg.TopicConfigurations, S3TopicConfiguration{
			ID:       t.ID,
			TopicArn: t.Topic,
			Events:   t.Events,
			Filter:   s3NotificationFilterState(t.Filter),
		})
	}
	for _, q := range w.QueueConfigurations {
		cfg.QueueConfigurations = append(cfg.QueueConfigurations, S3QueueConfiguration{
			ID:       q.ID,
			QueueArn: q.Queue,
			Events:   q.Events,
			Filter:   s3NotificationFilterState(q.Filter),
		})
	}
	for _, l := range w.LambdaConfigurations {
		cfg.LambdaFunctionConfigurations = append(cfg.LambdaFunctionConfigurations, S3LambdaFunctionConfiguration{
			ID:                l.ID,
			LambdaFunctionArn: l.CloudFunction,
			Events:            l.Events,
			Filter:            s3NotificationFilterState(l.Filter),
		})
	}
	return cfg
}

// s3NotificationConfigurationWireOf projects a persisted configuration onto its
// wire form.
func s3NotificationConfigurationWireOf(cfg S3NotificationConfiguration) s3NotificationConfigurationWire {
	w := s3NotificationConfigurationWire{Xmlns: s3XMLNamespace}
	if cfg.EventBridgeEnabled {
		w.EventBridge = &s3EventBridgeConfigurationWire{}
	}
	for _, t := range cfg.TopicConfigurations {
		w.TopicConfigurations = append(w.TopicConfigurations, s3TopicConfigurationWire{
			ID:     t.ID,
			Topic:  t.TopicArn,
			Events: t.Events,
			Filter: s3NotificationFilterWireOf(t.Filter),
		})
	}
	for _, q := range cfg.QueueConfigurations {
		w.QueueConfigurations = append(w.QueueConfigurations, s3QueueConfigurationWire{
			ID:     q.ID,
			Queue:  q.QueueArn,
			Events: q.Events,
			Filter: s3NotificationFilterWireOf(q.Filter),
		})
	}
	for _, l := range cfg.LambdaFunctionConfigurations {
		w.LambdaConfigurations = append(w.LambdaConfigurations, s3LambdaConfigurationWire{
			ID:            l.ID,
			CloudFunction: l.LambdaFunctionArn,
			Events:        l.Events,
			Filter:        s3NotificationFilterWireOf(l.Filter),
		})
	}
	return w
}

// s3NotificationFilterState projects a wire key filter onto the persisted shape.
// A nil filter stays nil: "no filter" and "a filter with no rules" mean the same
// thing to s3KeyFilterMatches, and preserving the distinction would only invite
// an empty <Filter/> onto the response.
func s3NotificationFilterState(f *s3NotificationFilterWire) *S3NotificationFilter {
	if f == nil || len(f.Key.FilterRules) == 0 {
		return nil
	}
	out := &S3NotificationFilter{}
	for _, r := range f.Key.FilterRules {
		out.Key.FilterRules = append(out.Key.FilterRules, S3FilterRule(r))
	}
	return out
}

// s3NotificationFilterWireOf projects a persisted key filter onto its wire form.
func s3NotificationFilterWireOf(f *S3NotificationFilter) *s3NotificationFilterWire {
	if f == nil || len(f.Key.FilterRules) == 0 {
		return nil
	}
	out := &s3NotificationFilterWire{}
	for _, r := range f.Key.FilterRules {
		out.Key.FilterRules = append(out.Key.FilterRules, s3FilterRuleWire(r))
	}
	return out
}
