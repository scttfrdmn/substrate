package substrate

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

	// ContentEncoding is the encoding of the object body (e.g. "gzip"),
	// set via the Content-Encoding header on PutObject.
	ContentEncoding string `json:"content_encoding,omitempty"`

	// Size is the byte length of the object body.
	Size int64 `json:"size"`

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

	// LastModified is the time this part was uploaded.
	LastModified time.Time `json:"last_modified"`
}

// S3BucketPolicy stores the raw JSON policy document for an S3 bucket.
type S3BucketPolicy struct {
	// Policy is the bucket policy as a raw JSON string.
	Policy string `json:"Policy"`
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

// S3NotificationConfiguration holds event notification configurations for an S3 bucket.
type S3NotificationConfiguration struct {
	// LambdaFunctionConfigurations lists Lambda invocation notification configs.
	LambdaFunctionConfigurations []S3LambdaFunctionConfiguration `json:"LambdaFunctionConfigurations,omitempty"`

	// QueueConfigurations lists SQS queue notification configs.
	QueueConfigurations []S3QueueConfiguration `json:"QueueConfigurations,omitempty"`

	// TopicConfigurations lists SNS topic notification configs (stored but not dispatched).
	TopicConfigurations []S3TopicConfiguration `json:"TopicConfigurations,omitempty"`
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
// The topic is stored but notifications are not dispatched in this emulator.
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
