package substrate

import "time"

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

	// Size is the byte length of the object body.
	Size int64 `json:"size"`

	// LastModified is the time of the most recent write.
	LastModified time.Time `json:"last_modified"`

	// UserMetadata holds key-value pairs set via X-Amz-Meta-* request headers.
	// Keys are stored in lowercase without the x-amz-meta- prefix.
	UserMetadata map[string]string `json:"user_metadata"`
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
