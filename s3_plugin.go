package substrate

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag is defined as MD5; not used for security.
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/afero"
)

// s3Namespace is the state namespace used by S3Plugin.
const s3Namespace = "s3"

// S3Plugin emulates the AWS Simple Storage Service (S3) REST API.
// It handles CreateBucket, HeadBucket, DeleteBucket, ListBuckets,
// PutObject, GetObject, HeadObject, DeleteObject, CopyObject,
// ListObjects, ListObjectsV2, CreateMultipartUpload, UploadPart,
// CompleteMultipartUpload, AbortMultipartUpload, and ListMultipartUploads.
// Object bodies are stored in an afero.Fs; metadata is stored via StateManager.
type S3Plugin struct {
	state      StateManager
	logger     Logger
	tc         *TimeController
	fs         afero.Fs
	registry   *PluginRegistry // nil = notifications disabled
	versionSeq int64           // monotonic counter for unique version IDs
}

// Name returns the service name "s3".
func (p *S3Plugin) Name() string { return "s3" }

// Initialize sets up the S3Plugin with state, logger, optional TimeController
// (Options["time_controller"]) and optional afero.Fs (Options["filesystem"]).
// Defaults to a real-time clock and an in-memory filesystem when not provided.
func (p *S3Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger

	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}

	if fs, ok := cfg.Options["filesystem"].(afero.Fs); ok {
		p.fs = fs
	} else {
		p.fs = afero.NewMemMapFs()
	}

	p.registry, _ = cfg.Options["registry"].(*PluginRegistry)

	return nil
}

// Shutdown is a no-op for S3Plugin.
func (p *S3Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches the S3 REST operation to the appropriate handler.
// It derives the semantic operation from the HTTP method, URL path, and query
// parameters, then mutates req.Operation so the server pipeline's cost and
// consistency tracking see the canonical name.
func (p *S3Plugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	bucket, key, op := parseS3Operation(req)
	req.Operation = op // mutate so server pipeline sees semantic name

	switch op {
	case "ListBuckets":
		return p.listBuckets(ctx, req)
	case "CreateBucket":
		return p.createBucket(ctx, req, bucket)
	case "HeadBucket":
		return p.headBucket(ctx, req, bucket)
	case "DeleteBucket":
		return p.deleteBucket(ctx, req, bucket)
	case "ListObjects":
		return p.listObjects(ctx, req, bucket)
	case "ListObjectsV2":
		return p.listObjectsV2(ctx, req, bucket)
	case "ListMultipartUploads":
		return p.listMultipartUploads(ctx, req, bucket)
	case "PutObject":
		return p.putObject(ctx, req, bucket, key)
	case "CopyObject":
		return p.copyObject(ctx, req, bucket, key)
	case "GetObject":
		return p.getObject(ctx, req, bucket, key)
	case "HeadObject":
		return p.headObject(ctx, req, bucket, key)
	case "DeleteObject":
		return p.deleteObject(ctx, req, bucket, key)
	case "DeleteObjects":
		return p.deleteObjects(ctx, req, bucket)
	case "CreateMultipartUpload":
		return p.createMultipartUpload(ctx, req, bucket, key)
	case "UploadPart":
		return p.uploadPart(ctx, req, bucket, key)
	case "CompleteMultipartUpload":
		return p.completeMultipartUpload(ctx, req, bucket, key)
	case "AbortMultipartUpload":
		return p.abortMultipartUpload(ctx, req, bucket, key)
	case "GetBucketPolicy":
		return p.getBucketPolicy(ctx, req, bucket)
	case "PutBucketPolicy":
		return p.putBucketPolicy(ctx, req, bucket)
	case "DeleteBucketPolicy":
		return p.deleteBucketPolicy(ctx, req, bucket)
	case "GetBucketAcl":
		return p.getBucketACL(ctx, req, bucket)
	case "PutBucketAcl":
		return p.putBucketACL(ctx, req, bucket)
	case "GetObjectAcl":
		return p.getObjectACL(ctx, req, bucket, key)
	case "PutObjectAcl":
		return p.putObjectACL(ctx, req, bucket, key)
	case "GetBucketNotificationConfiguration":
		return p.getBucketNotificationConfiguration(ctx, req)
	case "PutBucketNotificationConfiguration":
		return p.putBucketNotificationConfiguration(ctx, req)
	case "PutBucketTagging":
		return p.putBucketTagging(ctx, req, bucket)
	case "GetBucketTagging":
		return p.getBucketTagging(ctx, req, bucket)
	case "DeleteBucketTagging":
		return p.deleteBucketTagging(ctx, req, bucket)
	case "PutObjectTagging":
		return p.putObjectTagging(ctx, req, bucket, key)
	case "GetObjectTagging":
		return p.getObjectTagging(ctx, req, bucket, key)
	case "DeleteObjectTagging":
		return p.deleteObjectTagging(ctx, req, bucket, key)
	case "PutBucketVersioning":
		return p.putBucketVersioning(ctx, req, bucket)
	case "GetBucketVersioning":
		return p.getBucketVersioning(ctx, req, bucket)
	case "ListObjectVersions":
		return p.listObjectVersions(ctx, req, bucket)
	case "PutBucketLifecycleConfiguration":
		return p.putBucketLifecycleConfiguration(ctx, req, bucket)
	case "GetBucketLifecycleConfiguration":
		return p.getBucketLifecycleConfiguration(ctx, req, bucket)
	case "DeleteBucketLifecycle":
		return p.deleteBucketLifecycle(ctx, req, bucket)
	case "SelectObjectContent":
		return p.selectObjectContent(ctx, req, bucket, key)
	default:
		return nil, &AWSError{
			Code:       "NotImplemented",
			Message:    "S3 operation not yet implemented: " + op,
			HTTPStatus: http.StatusNotImplemented,
		}
	}
}

// parseS3Operation derives the semantic S3 operation name, bucket, and key
// from an AWSRequest whose Operation field still holds the raw HTTP method.
func parseS3Operation(req *AWSRequest) (bucket, key, op string) {
	// Strip leading slash and split on the first "/" to get bucket and key.
	path := strings.TrimPrefix(req.Path, "/")
	if path == "" || path == "/" {
		return "", "", "ListBuckets"
	}

	slashIdx := strings.IndexByte(path, '/')
	if slashIdx < 0 {
		bucket = path
		key = ""
	} else {
		bucket = path[:slashIdx]
		key = path[slashIdx+1:]
	}
	// A key that is exactly "/" arises from double-slash URLs like "/bucket//"
	// and should be treated as a bucket-level operation with no key.
	// Any other trailing slash is a legitimate directory-marker suffix and must
	// be preserved (e.g. "newdir/" is a valid S3 directory-marker key).
	if key == "/" {
		key = ""
	}

	method := req.Operation // still the HTTP verb at this point

	if key == "" {
		// Bucket-level operations.
		switch method {
		case "PUT":
			if req.Params["policy"] == "1" {
				return bucket, "", "PutBucketPolicy"
			}
			if req.Params["acl"] == "1" {
				return bucket, "", "PutBucketAcl"
			}
			if _, ok := req.Params["notification"]; ok {
				return bucket, "", "PutBucketNotificationConfiguration"
			}
			if _, ok := req.Params["tagging"]; ok {
				return bucket, "", "PutBucketTagging"
			}
			if _, ok := req.Params["versioning"]; ok {
				return bucket, "", "PutBucketVersioning"
			}
			if _, ok := req.Params["lifecycle"]; ok {
				return bucket, "", "PutBucketLifecycleConfiguration"
			}
			return bucket, "", "CreateBucket"
		case "HEAD":
			return bucket, "", "HeadBucket"
		case "DELETE":
			if req.Params["policy"] == "1" {
				return bucket, "", "DeleteBucketPolicy"
			}
			if _, ok := req.Params["tagging"]; ok {
				return bucket, "", "DeleteBucketTagging"
			}
			if _, ok := req.Params["lifecycle"]; ok {
				return bucket, "", "DeleteBucketLifecycle"
			}
			return bucket, "", "DeleteBucket"
		case "GET":
			if req.Params["policy"] == "1" {
				return bucket, "", "GetBucketPolicy"
			}
			if req.Params["acl"] == "1" {
				return bucket, "", "GetBucketAcl"
			}
			if _, ok := req.Params["notification"]; ok {
				return bucket, "", "GetBucketNotificationConfiguration"
			}
			if _, ok := req.Params["tagging"]; ok {
				return bucket, "", "GetBucketTagging"
			}
			if _, ok := req.Params["uploads"]; ok {
				return bucket, "", "ListMultipartUploads"
			}
			if _, ok := req.Params["versioning"]; ok {
				return bucket, "", "GetBucketVersioning"
			}
			if _, ok := req.Params["versions"]; ok {
				return bucket, "", "ListObjectVersions"
			}
			if _, ok := req.Params["lifecycle"]; ok {
				return bucket, "", "GetBucketLifecycleConfiguration"
			}
			if req.Params["list-type"] == "2" {
				return bucket, "", "ListObjectsV2"
			}
			return bucket, "", "ListObjects"
		case "POST":
			if _, ok := req.Params["delete"]; ok {
				return bucket, "", "DeleteObjects"
			}
		}
	} else {
		// Object-level operations.
		switch method {
		case "PUT":
			if req.Params["acl"] == "1" {
				return bucket, key, "PutObjectAcl"
			}
			if _, ok := req.Params["tagging"]; ok {
				return bucket, key, "PutObjectTagging"
			}
			if req.Headers["X-Amz-Copy-Source"] != "" {
				return bucket, key, "CopyObject"
			}
			if req.Params["partNumber"] != "" && req.Params["uploadId"] != "" {
				return bucket, key, "UploadPart"
			}
			return bucket, key, "PutObject"
		case "GET":
			if req.Params["acl"] == "1" {
				return bucket, key, "GetObjectAcl"
			}
			if _, ok := req.Params["tagging"]; ok {
				return bucket, key, "GetObjectTagging"
			}
			return bucket, key, "GetObject"
		case "HEAD":
			return bucket, key, "HeadObject"
		case "DELETE":
			if req.Params["uploadId"] != "" {
				return bucket, key, "AbortMultipartUpload"
			}
			if _, ok := req.Params["tagging"]; ok {
				return bucket, key, "DeleteObjectTagging"
			}
			return bucket, key, "DeleteObject"
		case "POST":
			if _, ok := req.Params["uploads"]; ok {
				return bucket, key, "CreateMultipartUpload"
			}
			if req.Params["uploadId"] != "" {
				return bucket, key, "CompleteMultipartUpload"
			}
			if _, ok := req.Params["select"]; ok {
				return bucket, key, "SelectObjectContent"
			}
		}
	}

	return bucket, key, method // fallback: leave as HTTP verb
}

// listBuckets handles GET / — returns all buckets owned by the account.
func (p *S3Plugin) listBuckets(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	ctx := context.Background()
	keys, err := p.state.List(ctx, s3Namespace, "bucket:")
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	type bucketEntry struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	type listAllMyBucketsResult struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Buckets struct {
			Bucket []bucketEntry `xml:"Bucket"`
		} `xml:"Buckets"`
	}

	var result listAllMyBucketsResult
	for _, k := range keys {
		data, getErr := p.state.Get(ctx, s3Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var b S3Bucket
		if unmarshalErr := json.Unmarshal(data, &b); unmarshalErr != nil {
			continue
		}
		result.Buckets.Bucket = append(result.Buckets.Bucket, bucketEntry{
			Name:         b.Name,
			CreationDate: b.CreationDate.UTC().Format(time.RFC3339),
		})
	}

	return s3XMLResponse(http.StatusOK, result)
}

// createBucket handles PUT /<bucket>.
func (p *S3Plugin) createBucket(reqCtx *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	if !validateBucketName(bucket) {
		return s3ErrorResponse("InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest), nil
	}

	ctx := context.Background()
	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket existence: %w", err)
	}
	if existing != nil {
		return s3ErrorResponse("BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict), nil
	}

	region := "us-east-1"
	if reqCtx != nil && reqCtx.Region != "" {
		region = reqCtx.Region
	}

	b := S3Bucket{
		Name:         bucket,
		Region:       region,
		CreationDate: p.tc.Now(),
		Tags:         make(map[string]string),
	}

	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("marshal bucket: %w", err)
	}

	if err := p.state.Put(ctx, s3Namespace, "bucket:"+bucket, data); err != nil {
		return nil, fmt.Errorf("save bucket: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Location": "/" + bucket},
	}, nil
}

// headBucket handles HEAD /<bucket>.
func (p *S3Plugin) headBucket(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// deleteBucket handles DELETE /<bucket>.
func (p *S3Plugin) deleteBucket(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	objKeys, listErr := p.state.List(ctx, s3Namespace, "object:"+bucket+"/")
	if listErr != nil {
		return nil, fmt.Errorf("list objects: %w", listErr)
	}
	if len(objKeys) > 0 {
		return s3ErrorResponse("BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict), nil
	}

	if err := p.state.Delete(ctx, s3Namespace, "bucket:"+bucket); err != nil {
		return nil, fmt.Errorf("delete bucket: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// putObject handles PUT /<bucket>/<key>.
func (p *S3Plugin) putObject(reqCtx *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	body := req.Body
	hash := md5.Sum(body) //nolint:gosec
	etag := fmt.Sprintf(`"%x"`, hash)

	// Directory-marker objects (key ends with "/") must not be written to the
	// afero filesystem: filepath.Clean inside MemMapFs would strip the trailing
	// slash, corrupting the path and conflicting with real sub-key directories.
	// State metadata is sufficient for directory markers (body is always empty).
	if !strings.HasSuffix(key, "/") {
		filePath := "/" + bucket + "/" + key
		if mkdirErr := p.fs.MkdirAll(filepath.Dir(filePath), 0o755); mkdirErr != nil {
			return nil, fmt.Errorf("mkdir: %w", mkdirErr)
		}
		if writeErr := afero.WriteFile(p.fs, filePath, body, 0o644); writeErr != nil {
			return nil, fmt.Errorf("write object body: %w", writeErr)
		}
	}

	userMeta := extractUserMetadata(req.Headers)

	contentType := req.Headers["Content-Type"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj := S3Object{
		Bucket:          bucket,
		Key:             key,
		ETag:            etag,
		ContentType:     contentType,
		ContentEncoding: req.Headers["Content-Encoding"],
		Size:            int64(len(body)),
		LastModified:    p.tc.Now(),
		UserMetadata:    userMeta,
	}

	respHeaders := map[string]string{"ETag": etag}

	// If versioning is enabled, generate a version ID and store the versioned copy.
	versioningStatus := p.getBucketVersioningStatus(ctx, bucket)
	if versioningStatus == "Enabled" {
		versionID := fmt.Sprintf("v%d-%d", p.tc.Now().UnixNano(), atomic.AddInt64(&p.versionSeq, 1))
		obj.VersionID = versionID
		// Write body to version-specific filesystem path.
		// Skip filesystem for directory-marker keys (same reason as above).
		if !strings.HasSuffix(key, "/") {
			vfPath := "/" + bucket + "/.versions/" + key + "/" + versionID
			if mkErr := p.fs.MkdirAll(filepath.Dir(vfPath), 0o755); mkErr != nil {
				return nil, fmt.Errorf("mkdir versioned path: %w", mkErr)
			}
			if wErr := afero.WriteFile(p.fs, vfPath, body, 0o644); wErr != nil {
				return nil, fmt.Errorf("write versioned body: %w", wErr)
			}
		}
		versionedKey := "object_version:" + bucket + "/" + key + "/" + versionID
		versionedData, marshalErr := json.Marshal(obj)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal versioned object: %w", marshalErr)
		}
		if putErr := p.state.Put(ctx, s3Namespace, versionedKey, versionedData); putErr != nil {
			return nil, fmt.Errorf("save versioned object: %w", putErr)
		}
		// Prepend version ID to the version list.
		vids := p.loadVersionIDs(ctx, bucket, key)
		vids = append([]string{versionID}, vids...)
		p.saveVersionIDs(ctx, bucket, key, vids)
		respHeaders["x-amz-version-id"] = versionID
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal object metadata: %w", err)
	}

	if err := p.state.Put(ctx, s3Namespace, "object:"+bucket+"/"+key, data); err != nil {
		return nil, fmt.Errorf("save object metadata: %w", err)
	}

	p.fireNotifications(reqCtx, bucket, key, "s3:ObjectCreated:Put", obj.Size, etag)

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    respHeaders,
	}, nil
}

// getObject handles GET /<bucket>/<key>.
func (p *S3Plugin) getObject(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	// If versionId query param is present, load from versioned storage.
	versionID := req.Params["versionId"]

	var stateKey string
	var fsPath string
	if versionID != "" {
		stateKey = "object_version:" + bucket + "/" + key + "/" + versionID
		fsPath = "/" + bucket + "/.versions/" + key + "/" + versionID
	} else {
		stateKey = "object:" + bucket + "/" + key
		fsPath = "/" + bucket + "/" + key
	}

	data, err := p.state.Get(ctx, s3Namespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("get object metadata: %w", err)
	}
	if data == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}

	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal object metadata: %w", err)
	}

	if obj.IsDeleteMarker {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}

	// Directory-marker objects (key ends with "/") are never written to the
	// afero filesystem; their body is always empty.
	var body []byte
	if !strings.HasSuffix(key, "/") {
		// Try versioned fs path first, then fallback to main path.
		var readErr error
		body, readErr = afero.ReadFile(p.fs, fsPath)
		if readErr != nil {
			// Fall back to main path for objects written before versioning was enabled.
			body, readErr = afero.ReadFile(p.fs, "/"+bucket+"/"+key)
			if readErr != nil {
				return nil, fmt.Errorf("read object body: %w", readErr)
			}
		}
	}

	headers := map[string]string{
		"Content-Type":   obj.ContentType,
		"ETag":           obj.ETag,
		"Last-Modified":  obj.LastModified.UTC().Format(http.TimeFormat),
		"Content-Length": strconv.FormatInt(obj.Size, 10),
	}
	if obj.ContentEncoding != "" {
		headers["Content-Encoding"] = obj.ContentEncoding
	}
	if obj.VersionID != "" {
		headers["x-amz-version-id"] = obj.VersionID
	}
	for k, v := range obj.UserMetadata {
		headers["X-Amz-Meta-"+k] = v
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: headers, Body: body}, nil
}

// headObject handles HEAD /<bucket>/<key>.
func (p *S3Plugin) headObject(_ *RequestContext, _ *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	data, err := p.state.Get(ctx, s3Namespace, "object:"+bucket+"/"+key)
	if err != nil {
		return nil, fmt.Errorf("get object metadata: %w", err)
	}
	if data == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}

	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal object metadata: %w", err)
	}

	headers := map[string]string{
		"Content-Type":   obj.ContentType,
		"ETag":           obj.ETag,
		"Last-Modified":  obj.LastModified.UTC().Format(http.TimeFormat),
		"Content-Length": strconv.FormatInt(obj.Size, 10),
	}
	if obj.ContentEncoding != "" {
		headers["Content-Encoding"] = obj.ContentEncoding
	}
	for k, v := range obj.UserMetadata {
		headers["X-Amz-Meta-"+k] = v
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: headers}, nil
}

// deleteObject handles DELETE /<bucket>/<key>.
// S3 DELETE is idempotent: no error is returned when the key is absent.
func (p *S3Plugin) deleteObject(reqCtx *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	versioningStatus := p.getBucketVersioningStatus(ctx, bucket)
	versionID := req.Params["versionId"]

	if versionID != "" {
		// Permanently remove a specific version.
		versionedKey := "object_version:" + bucket + "/" + key + "/" + versionID
		_ = p.state.Delete(ctx, s3Namespace, versionedKey)
		_ = p.fs.Remove("/" + bucket + "/.versions/" + key + "/" + versionID)
		// Remove from version list.
		vids := p.loadVersionIDs(ctx, bucket, key)
		filtered := vids[:0]
		for _, vid := range vids {
			if vid != versionID {
				filtered = append(filtered, vid)
			}
		}
		p.saveVersionIDs(ctx, bucket, key, filtered)
		return &AWSResponse{
			StatusCode: http.StatusNoContent,
			Headers:    map[string]string{"x-amz-version-id": versionID},
		}, nil
	}

	if versioningStatus == "Enabled" {
		// Insert a delete marker.
		markerVersionID := fmt.Sprintf("dm%d-%d", p.tc.Now().UnixNano(), atomic.AddInt64(&p.versionSeq, 1))
		marker := S3Object{
			Bucket:         bucket,
			Key:            key,
			LastModified:   p.tc.Now(),
			VersionID:      markerVersionID,
			IsDeleteMarker: true,
		}
		markerData, marshalErr := json.Marshal(marker)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal delete marker: %w", marshalErr)
		}
		versionedKey := "object_version:" + bucket + "/" + key + "/" + markerVersionID
		if putErr := p.state.Put(ctx, s3Namespace, versionedKey, markerData); putErr != nil {
			return nil, fmt.Errorf("save delete marker: %w", putErr)
		}
		// Update the current object to the delete marker.
		if putErr := p.state.Put(ctx, s3Namespace, "object:"+bucket+"/"+key, markerData); putErr != nil {
			return nil, fmt.Errorf("update current to delete marker: %w", putErr)
		}
		vids := p.loadVersionIDs(ctx, bucket, key)
		vids = append([]string{markerVersionID}, vids...)
		p.saveVersionIDs(ctx, bucket, key, vids)
		p.fireNotifications(reqCtx, bucket, key, "s3:ObjectRemoved:DeleteMarkerCreated", 0, "")
		return &AWSResponse{
			StatusCode: http.StatusNoContent,
			Headers: map[string]string{
				"x-amz-version-id":    markerVersionID,
				"x-amz-delete-marker": "true",
			},
		}, nil
	}

	_ = p.state.Delete(ctx, s3Namespace, "object:"+bucket+"/"+key)
	_ = p.fs.Remove("/" + bucket + "/" + key)

	p.fireNotifications(reqCtx, bucket, key, "s3:ObjectRemoved:Delete", 0, "")

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// deleteObjects handles POST /<bucket>?delete — multi-object delete.
// The request body is XML: <Delete><Object><Key>…</Key></Object>…</Delete>.
// The response lists successfully deleted keys and any errors.
func (p *S3Plugin) deleteObjects(reqCtx *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	// Parse the XML request body.
	type deleteObject struct {
		Key       string `xml:"Key"`
		VersionId string `xml:"VersionId"` //nolint:revive // AWS XML element name
	}
	type deleteRequest struct {
		XMLName xml.Name       `xml:"Delete"`
		Objects []deleteObject `xml:"Object"`
		Quiet   bool           `xml:"Quiet"`
	}
	var deleteReq deleteRequest
	if len(req.Body) > 0 {
		if xmlErr := xml.Unmarshal(req.Body, &deleteReq); xmlErr != nil {
			return s3ErrorResponse("MalformedXML", "The XML you provided was not well-formed.", http.StatusBadRequest), nil //nolint:nilerr
		}
	}

	type deletedItem struct {
		Key       string `xml:"Key"`
		VersionId string `xml:"VersionId,omitempty"` //nolint:revive // AWS XML element name
	}
	type errorItem struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	type deleteResult struct {
		XMLName xml.Name      `xml:"DeleteResult"`
		XMLNS   string        `xml:"xmlns,attr"`
		Deleted []deletedItem `xml:"Deleted"`
		Errors  []errorItem   `xml:"Error"`
	}

	result := deleteResult{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/"}

	for _, obj := range deleteReq.Objects {
		if obj.Key == "" {
			continue
		}
		// Delegate to the single-delete path by synthesizing a minimal AWSRequest.
		synReq := &AWSRequest{
			Operation: "DELETE",
			Path:      req.Path,
			Params:    map[string]string{},
			Headers:   req.Headers,
		}
		if obj.VersionId != "" {
			synReq.Params["versionId"] = obj.VersionId
		}
		// Build a temporary request context scoped to this key.
		_, delErr := p.deleteObject(reqCtx, synReq, bucket, obj.Key)
		if delErr != nil {
			result.Errors = append(result.Errors, errorItem{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: delErr.Error(),
			})
			continue
		}
		if !deleteReq.Quiet {
			item := deletedItem{Key: obj.Key}
			if obj.VersionId != "" {
				item.VersionId = obj.VersionId
			}
			result.Deleted = append(result.Deleted, item)
		}
	}

	return s3XMLResponse(http.StatusOK, result)
}

// copyObject handles PUT /<bucket>/<key> with X-Amz-Copy-Source header.
func (p *S3Plugin) copyObject(_ *RequestContext, req *AWSRequest, dstBucket, dstKey string) (*AWSResponse, error) {
	ctx := context.Background()

	copySource := req.Headers["X-Amz-Copy-Source"]
	if copySource == "" {
		return s3ErrorResponse("InvalidArgument", "Copy Source must be specified.", http.StatusBadRequest), nil
	}

	// URL-decode the source path.
	srcPath, decodeErr := url.QueryUnescape(copySource)
	if decodeErr != nil {
		srcPath = copySource
	}
	srcPath = strings.TrimPrefix(srcPath, "/")

	slashIdx := strings.IndexByte(srcPath, '/')
	if slashIdx < 0 {
		return s3ErrorResponse("InvalidArgument", "Copy source must include an object key.", http.StatusBadRequest), nil
	}
	srcBucket := srcPath[:slashIdx]
	srcKey := srcPath[slashIdx+1:]

	srcMeta, err := p.state.Get(ctx, s3Namespace, "object:"+srcBucket+"/"+srcKey)
	if err != nil {
		return nil, fmt.Errorf("get source metadata: %w", err)
	}
	if srcMeta == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}

	var srcObj S3Object
	if err := json.Unmarshal(srcMeta, &srcObj); err != nil {
		return nil, fmt.Errorf("unmarshal source metadata: %w", err)
	}

	dstBucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+dstBucket)
	if err != nil {
		return nil, fmt.Errorf("check dest bucket: %w", err)
	}
	if dstBucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	srcBody, readErr := afero.ReadFile(p.fs, "/"+srcBucket+"/"+srcKey)
	if readErr != nil {
		return nil, fmt.Errorf("read source body: %w", readErr)
	}

	dstFilePath := "/" + dstBucket + "/" + dstKey
	if mkdirErr := p.fs.MkdirAll(filepath.Dir(dstFilePath), 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("mkdir dest: %w", mkdirErr)
	}
	if writeErr := afero.WriteFile(p.fs, dstFilePath, srcBody, 0o644); writeErr != nil {
		return nil, fmt.Errorf("write dest body: %w", writeErr)
	}

	now := p.tc.Now()
	hash := md5.Sum(srcBody) //nolint:gosec
	newETag := fmt.Sprintf(`"%x"`, hash)

	dstObj := S3Object{
		Bucket:       dstBucket,
		Key:          dstKey,
		ETag:         newETag,
		ContentType:  srcObj.ContentType,
		Size:         srcObj.Size,
		LastModified: now,
		UserMetadata: srcObj.UserMetadata,
	}

	dstMeta, err := json.Marshal(dstObj)
	if err != nil {
		return nil, fmt.Errorf("marshal dest metadata: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "object:"+dstBucket+"/"+dstKey, dstMeta); err != nil {
		return nil, fmt.Errorf("save dest metadata: %w", err)
	}

	type copyObjectResult struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}
	return s3XMLResponse(http.StatusOK, copyObjectResult{
		ETag:         newETag,
		LastModified: now.UTC().Format(time.RFC3339),
	})
}

// listObjects handles GET /<bucket> (ListObjects v1).
func (p *S3Plugin) listObjects(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	prefix := req.Params["prefix"]
	delimiter := req.Params["delimiter"]
	marker := req.Params["marker"]
	maxKeys := 1000
	if mk := req.Params["max-keys"]; mk != "" {
		if n, convErr := strconv.Atoi(mk); convErr == nil && n > 0 {
			maxKeys = n
		}
	}

	objectKeys, err := p.listSortedObjectKeys(ctx, bucket)
	if err != nil {
		return nil, err
	}

	type listBucketResult struct {
		XMLName        xml.Name          `xml:"ListBucketResult"`
		Name           string            `xml:"Name"`
		Prefix         string            `xml:"Prefix"`
		Delimiter      string            `xml:"Delimiter,omitempty"`
		Marker         string            `xml:"Marker"`
		MaxKeys        int               `xml:"MaxKeys"`
		IsTruncated    bool              `xml:"IsTruncated"`
		Contents       []objectEntryItem `xml:"Contents"`
		CommonPrefixes []struct {
			Prefix string `xml:"Prefix"`
		} `xml:"CommonPrefixes"`
	}

	result := listBucketResult{
		Name:      bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		Marker:    marker,
		MaxKeys:   maxKeys,
	}

	seenPrefixes := make(map[string]bool)
	count := 0
	pastMarker := marker == ""

	for _, objKey := range objectKeys {
		if !pastMarker {
			if objKey > marker {
				pastMarker = true
			} else {
				continue
			}
		}
		if prefix != "" && !strings.HasPrefix(objKey, prefix) {
			continue
		}
		if delimiter != "" {
			rest := strings.TrimPrefix(objKey, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					result.CommonPrefixes = append(result.CommonPrefixes, struct {
						Prefix string `xml:"Prefix"`
					}{Prefix: cp})
				}
				continue
			}
		}
		if count >= maxKeys {
			result.IsTruncated = true
			break
		}
		entry, loadErr := p.loadObjectEntry(ctx, bucket, objKey)
		if loadErr != nil || entry == nil {
			continue
		}
		result.Contents = append(result.Contents, *entry)
		count++
	}

	return s3XMLResponse(http.StatusOK, result)
}

// listObjectsV2 handles GET /<bucket>?list-type=2.
func (p *S3Plugin) listObjectsV2(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	prefix := req.Params["prefix"]
	delimiter := req.Params["delimiter"]
	startAfter := req.Params["start-after"]
	contToken := req.Params["continuation-token"]
	maxKeys := 1000
	if mk := req.Params["max-keys"]; mk != "" {
		if n, convErr := strconv.Atoi(mk); convErr == nil && n > 0 {
			maxKeys = n
		}
	}

	// Continuation token is a base64-encoded "last seen key".
	afterKey := startAfter
	if contToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(contToken); decErr == nil {
			afterKey = string(decoded)
		}
	}

	objectKeys, err := p.listSortedObjectKeys(ctx, bucket)
	if err != nil {
		return nil, err
	}

	type listBucketV2Result struct {
		XMLName               xml.Name          `xml:"ListBucketResult"`
		Name                  string            `xml:"Name"`
		Prefix                string            `xml:"Prefix"`
		Delimiter             string            `xml:"Delimiter,omitempty"`
		MaxKeys               int               `xml:"MaxKeys"`
		KeyCount              int               `xml:"KeyCount"`
		IsTruncated           bool              `xml:"IsTruncated"`
		ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
		StartAfter            string            `xml:"StartAfter,omitempty"`
		Contents              []objectEntryItem `xml:"Contents"`
		CommonPrefixes        []struct {
			Prefix string `xml:"Prefix"`
		} `xml:"CommonPrefixes"`
	}

	result := listBucketV2Result{
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		ContinuationToken: contToken,
		StartAfter:        startAfter,
	}

	seenPrefixes := make(map[string]bool)
	count := 0
	var lastKey string

	for _, objKey := range objectKeys {
		if afterKey != "" && objKey <= afterKey {
			continue
		}
		if prefix != "" && !strings.HasPrefix(objKey, prefix) {
			continue
		}
		if delimiter != "" {
			rest := strings.TrimPrefix(objKey, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					result.CommonPrefixes = append(result.CommonPrefixes, struct {
						Prefix string `xml:"Prefix"`
					}{Prefix: cp})
				}
				continue
			}
		}
		if count >= maxKeys {
			result.IsTruncated = true
			result.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(lastKey))
			break
		}
		entry, loadErr := p.loadObjectEntry(ctx, bucket, objKey)
		if loadErr != nil || entry == nil {
			continue
		}
		result.Contents = append(result.Contents, *entry)
		lastKey = objKey
		count++
	}

	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)
	return s3XMLResponse(http.StatusOK, result)
}

// createMultipartUpload handles POST /<bucket>/<key>?uploads.
func (p *S3Plugin) createMultipartUpload(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	uploadID := generateUploadID()

	contentType := req.Headers["Content-Type"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	upload := S3MultipartUpload{
		UploadID:     uploadID,
		Bucket:       bucket,
		Key:          key,
		ContentType:  contentType,
		Initiated:    p.tc.Now(),
		UserMetadata: extractUserMetadata(req.Headers),
	}

	data, err := json.Marshal(upload)
	if err != nil {
		return nil, fmt.Errorf("marshal upload metadata: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "multipart:"+uploadID, data); err != nil {
		return nil, fmt.Errorf("save upload metadata: %w", err)
	}

	type initiateMultipartUploadResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadId string   `xml:"UploadId"` //nolint:revive // matches AWS XML field name
	}
	return s3XMLResponse(http.StatusOK, initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	})
}

// uploadPart handles PUT /<bucket>/<key>?partNumber=N&uploadId=ID.
func (p *S3Plugin) uploadPart(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	uploadID := req.Params["uploadId"]
	partNumStr := req.Params["partNumber"]

	partNum, _ := strconv.Atoi(partNumStr) // non-numeric → 0, fails range check below
	if partNum < 1 || partNum > 10000 {
		return s3ErrorResponse("InvalidPart", "The part number must be an integer between 1 and 10000.", http.StatusBadRequest), nil
	}

	uploadData, err := p.state.Get(ctx, s3Namespace, "multipart:"+uploadID)
	if err != nil {
		return nil, fmt.Errorf("get upload metadata: %w", err)
	}
	if uploadData == nil {
		return s3ErrorResponse("NoSuchUpload", "The specified upload does not exist.", http.StatusNotFound), nil
	}

	var upload S3MultipartUpload
	if err := json.Unmarshal(uploadData, &upload); err != nil {
		return nil, fmt.Errorf("unmarshal upload metadata: %w", err)
	}
	if upload.Bucket != bucket || upload.Key != key {
		return s3ErrorResponse("NoSuchUpload", "The specified upload does not exist.", http.StatusNotFound), nil
	}

	body := req.Body
	hash := md5.Sum(body) //nolint:gosec
	etag := fmt.Sprintf(`"%x"`, hash)

	partPath := fmt.Sprintf("/.multipart/%s/%d", uploadID, partNum)
	if mkdirErr := p.fs.MkdirAll(filepath.Dir(partPath), 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("mkdir parts dir: %w", mkdirErr)
	}
	if writeErr := afero.WriteFile(p.fs, partPath, body, 0o644); writeErr != nil {
		return nil, fmt.Errorf("write part body: %w", writeErr)
	}

	part := S3Part{
		PartNumber:   partNum,
		ETag:         etag,
		Size:         int64(len(body)),
		LastModified: p.tc.Now(),
	}
	partData, err := json.Marshal(part)
	if err != nil {
		return nil, fmt.Errorf("marshal part metadata: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, fmt.Sprintf("part:%s/%d", uploadID, partNum), partData); err != nil {
		return nil, fmt.Errorf("save part metadata: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"ETag": etag},
	}, nil
}

// completeMultipartUpload handles POST /<bucket>/<key>?uploadId=ID.
func (p *S3Plugin) completeMultipartUpload(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	uploadID := req.Params["uploadId"]

	uploadData, err := p.state.Get(ctx, s3Namespace, "multipart:"+uploadID)
	if err != nil {
		return nil, fmt.Errorf("get upload metadata: %w", err)
	}
	if uploadData == nil {
		return s3ErrorResponse("NoSuchUpload", "The specified upload does not exist.", http.StatusNotFound), nil
	}

	var upload S3MultipartUpload
	if err := json.Unmarshal(uploadData, &upload); err != nil {
		return nil, fmt.Errorf("unmarshal upload metadata: %w", err)
	}

	type partRef struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}
	type completeReq struct {
		Parts []partRef `xml:"Part"`
	}

	var cReq completeReq
	xmlErr := xml.Unmarshal(req.Body, &cReq)
	if xmlErr != nil {
		return s3ErrorResponse("MalformedXML", "The XML you provided was not well-formed.", http.StatusBadRequest), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	if len(cReq.Parts) == 0 {
		return s3ErrorResponse("InvalidPart", "One or more of the specified parts could not be found.", http.StatusBadRequest), nil
	}

	for i := 1; i < len(cReq.Parts); i++ {
		if cReq.Parts[i].PartNumber <= cReq.Parts[i-1].PartNumber {
			return s3ErrorResponse("InvalidPartOrder", "The list of parts was not in ascending order.", http.StatusBadRequest), nil
		}
	}

	// Concatenate parts and compute multi-part ETag.
	var combined []byte
	var partMD5s []byte

	for _, pr := range cReq.Parts {
		partKey := fmt.Sprintf("part:%s/%d", uploadID, pr.PartNumber)
		partData, getErr := p.state.Get(ctx, s3Namespace, partKey)
		if getErr != nil {
			return nil, fmt.Errorf("get part %d metadata: %w", pr.PartNumber, getErr)
		}
		if partData == nil {
			return s3ErrorResponse("InvalidPart", fmt.Sprintf("Part %d not found.", pr.PartNumber), http.StatusBadRequest), nil
		}

		partPath := fmt.Sprintf("/.multipart/%s/%d", uploadID, pr.PartNumber)
		partBody, readErr := afero.ReadFile(p.fs, partPath)
		if readErr != nil {
			return nil, fmt.Errorf("read part %d body: %w", pr.PartNumber, readErr)
		}
		combined = append(combined, partBody...)
		h := md5.Sum(partBody) //nolint:gosec
		partMD5s = append(partMD5s, h[:]...)
	}

	numParts := len(cReq.Parts)
	combinedHash := md5.Sum(partMD5s) //nolint:gosec
	etag := fmt.Sprintf(`"%x-%d"`, combinedHash, numParts)

	filePath := "/" + bucket + "/" + key
	if mkdirErr := p.fs.MkdirAll(filepath.Dir(filePath), 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("mkdir: %w", mkdirErr)
	}
	if writeErr := afero.WriteFile(p.fs, filePath, combined, 0o644); writeErr != nil {
		return nil, fmt.Errorf("write assembled object: %w", writeErr)
	}

	obj := S3Object{
		Bucket:       bucket,
		Key:          key,
		ETag:         etag,
		ContentType:  upload.ContentType,
		Size:         int64(len(combined)),
		LastModified: p.tc.Now(),
		UserMetadata: upload.UserMetadata,
	}
	objData, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal assembled object metadata: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "object:"+bucket+"/"+key, objData); err != nil {
		return nil, fmt.Errorf("save assembled object metadata: %w", err)
	}

	// Clean up multipart state.
	_ = p.state.Delete(ctx, s3Namespace, "multipart:"+uploadID)
	for _, pr := range cReq.Parts {
		_ = p.state.Delete(ctx, s3Namespace, fmt.Sprintf("part:%s/%d", uploadID, pr.PartNumber))
		_ = p.fs.Remove(fmt.Sprintf("/.multipart/%s/%d", uploadID, pr.PartNumber))
	}

	type completeMultipartUploadResult struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}
	return s3XMLResponse(http.StatusOK, completeMultipartUploadResult{
		Location: "https://s3.amazonaws.com/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	})
}

// abortMultipartUpload handles DELETE /<bucket>/<key>?uploadId=ID.
func (p *S3Plugin) abortMultipartUpload(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	uploadID := req.Params["uploadId"]

	uploadData, err := p.state.Get(ctx, s3Namespace, "multipart:"+uploadID)
	if err != nil {
		return nil, fmt.Errorf("get upload metadata: %w", err)
	}
	if uploadData == nil {
		return s3ErrorResponse("NoSuchUpload", "The specified upload does not exist.", http.StatusNotFound), nil
	}

	var upload S3MultipartUpload
	if err := json.Unmarshal(uploadData, &upload); err != nil {
		return nil, fmt.Errorf("unmarshal upload metadata: %w", err)
	}
	if upload.Bucket != bucket || upload.Key != key {
		return s3ErrorResponse("NoSuchUpload", "The specified upload does not exist.", http.StatusNotFound), nil
	}

	_ = p.state.Delete(ctx, s3Namespace, "multipart:"+uploadID)

	// Delete all stored parts.
	partKeys, listErr := p.state.List(ctx, s3Namespace, "part:"+uploadID+"/")
	if listErr == nil {
		for _, k := range partKeys {
			_ = p.state.Delete(ctx, s3Namespace, k)
		}
	}
	_ = p.fs.RemoveAll(fmt.Sprintf("/.multipart/%s", uploadID))

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// listMultipartUploads handles GET /<bucket>?uploads.
func (p *S3Plugin) listMultipartUploads(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	allKeys, err := p.state.List(ctx, s3Namespace, "multipart:")
	if err != nil {
		return nil, fmt.Errorf("list uploads: %w", err)
	}

	type uploadEntry struct {
		Key       string `xml:"Key"`
		UploadId  string `xml:"UploadId"` //nolint:revive // matches AWS XML field name
		Initiated string `xml:"Initiated"`
	}
	type listMultipartUploadsResult struct {
		XMLName xml.Name      `xml:"ListMultipartUploadsResult"`
		Bucket  string        `xml:"Bucket"`
		Uploads []uploadEntry `xml:"Upload"`
	}

	result := listMultipartUploadsResult{Bucket: bucket}

	for _, k := range allKeys {
		data, getErr := p.state.Get(ctx, s3Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var upload S3MultipartUpload
		if unmarshalErr := json.Unmarshal(data, &upload); unmarshalErr != nil {
			continue
		}
		if upload.Bucket != bucket {
			continue
		}
		result.Uploads = append(result.Uploads, uploadEntry{
			Key:       upload.Key,
			UploadId:  upload.UploadID,
			Initiated: upload.Initiated.UTC().Format(time.RFC3339),
		})
	}

	return s3XMLResponse(http.StatusOK, result)
}

// --- helpers ---

// listSortedObjectKeys returns the object keys (without namespace prefix) in
// the given bucket, sorted lexicographically.
func (p *S3Plugin) listSortedObjectKeys(ctx context.Context, bucket string) ([]string, error) {
	allKeys, err := p.state.List(ctx, s3Namespace, "object:"+bucket+"/")
	if err != nil {
		return nil, fmt.Errorf("list object keys: %w", err)
	}
	prefix := "object:" + bucket + "/"
	prefixLen := len(prefix)
	out := make([]string, 0, len(allKeys))
	for _, k := range allKeys {
		if len(k) > prefixLen {
			out = append(out, k[prefixLen:])
		}
	}
	sort.Strings(out)
	return out, nil
}

// objectEntryItem is a reusable struct for list result entries.
type objectEntryItem struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

// loadObjectEntry loads and returns the XML entry for a single object.
// Returns nil (no error) when the object metadata is absent or unreadable.
func (p *S3Plugin) loadObjectEntry(ctx context.Context, bucket, key string) (*objectEntryItem, error) {
	data, err := p.state.Get(ctx, s3Namespace, "object:"+bucket+"/"+key)
	if err != nil {
		return nil, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	if data == nil {
		return nil, nil
	}
	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal object %s/%s: %w", bucket, key, err)
	}
	return &objectEntryItem{
		Key:          key,
		LastModified: obj.LastModified.UTC().Format(time.RFC3339),
		ETag:         obj.ETag,
		Size:         obj.Size,
	}, nil
}

// extractUserMetadata extracts X-Amz-Meta-* headers into a map keyed by the
// lowercased suffix (without the x-amz-meta- prefix).
func extractUserMetadata(headers map[string]string) map[string]string {
	meta := make(map[string]string)
	for k, v := range headers {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") {
			meta[lower[len("x-amz-meta-"):]] = v
		}
	}
	return meta
}

// validateBucketName returns true when name conforms to S3 bucket naming rules:
// 3–63 characters, lowercase letters/digits/hyphens/dots, start and end with a
// lowercase letter or digit.
func validateBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	isLetterOrDigit := func(c byte) bool {
		return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
	}
	if !isLetterOrDigit(name[0]) || !isLetterOrDigit(name[len(name)-1]) {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !isLetterOrDigit(c) && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

// generateUploadID produces a unique multipart upload identifier.
func generateUploadID() string {
	return fmt.Sprintf("mpu-%d", time.Now().UnixNano())
}

// s3XMLResponse serializes v as XML and wraps it in an [AWSResponse] with the
// standard S3 content type. When v is nil only the XML declaration is emitted.
func s3XMLResponse(status int, v any) (*AWSResponse, error) {
	if v == nil {
		return &AWSResponse{
			StatusCode: status,
			Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
			Body:       []byte(xml.Header),
		}, nil
	}
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal S3 XML response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// s3ErrorResponse builds an S3-style XML error [AWSResponse].
// extras is currently unused and reserved for future extension.
func s3ErrorResponse(code, message string, status int, extras ...string) *AWSResponse {
	_ = extras // reserved
	type errorXML struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		RequestID string   `xml:"RequestId"`
	}
	e := errorXML{Code: code, Message: message, RequestID: "SUBSTRATE"}
	body, _ := xml.Marshal(e)
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}
}

// --- Bucket policy operations ----------------------------------------------

// getBucketPolicy handles GET /<bucket>?policy.
func (p *S3Plugin) getBucketPolicy(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	raw, err := p.state.Get(ctx, s3Namespace, "bucket_policy:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("get bucket policy: %w", err)
	}
	if raw == nil {
		return s3ErrorResponse("NoSuchBucketPolicy",
			"The bucket policy does not exist.", http.StatusNotFound), nil
	}

	var pol S3BucketPolicy
	if err := json.Unmarshal(raw, &pol); err != nil {
		return nil, fmt.Errorf("unmarshal bucket policy: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       []byte(pol.Policy),
	}, nil
}

// putBucketPolicy handles PUT /<bucket>?policy.
func (p *S3Plugin) putBucketPolicy(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	// Verify bucket exists.
	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket",
			"The specified bucket does not exist.", http.StatusNotFound), nil
	}

	// Validate the policy document is JSON.
	policyJSON := req.Body
	if len(policyJSON) == 0 {
		return s3ErrorResponse("MalformedPolicy",
			"Request body must not be empty.", http.StatusBadRequest), nil
	}
	var rawCheck map[string]json.RawMessage
	if err := json.Unmarshal(policyJSON, &rawCheck); err != nil {
		return s3ErrorResponse("MalformedPolicy", //nolint:nilerr
			"Bucket policy must be valid JSON.", http.StatusBadRequest), nil
	}

	pol := S3BucketPolicy{Policy: string(policyJSON)}
	raw, err := json.Marshal(pol)
	if err != nil {
		return nil, fmt.Errorf("marshal bucket policy: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "bucket_policy:"+bucket, raw); err != nil {
		return nil, fmt.Errorf("put bucket policy: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusNoContent,
		Headers:    map[string]string{},
	}, nil
}

// deleteBucketPolicy handles DELETE /<bucket>?policy.
func (p *S3Plugin) deleteBucketPolicy(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	if err := p.state.Delete(ctx, s3Namespace, "bucket_policy:"+bucket); err != nil {
		return nil, fmt.Errorf("delete bucket policy: %w", err)
	}
	return &AWSResponse{
		StatusCode: http.StatusNoContent,
		Headers:    map[string]string{},
	}, nil
}

// --- Bucket and object ACL operations -------------------------------------

// getBucketACL handles GET /<bucket>?acl.
func (p *S3Plugin) getBucketACL(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	// Verify bucket exists.
	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket",
			"The specified bucket does not exist.", http.StatusNotFound), nil
	}

	raw, err := p.state.Get(ctx, s3Namespace, "bucket_acl:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("get bucket acl: %w", err)
	}

	var acl S3AccessControlList
	if raw != nil {
		if err := json.Unmarshal(raw, &acl); err != nil {
			return nil, fmt.Errorf("unmarshal bucket acl: %w", err)
		}
	} else {
		// Return a default owner-full-control ACL.
		acl = s3DefaultACL(bucket)
	}

	return s3XMLResponse(http.StatusOK, acl)
}

// putBucketACL handles PUT /<bucket>?acl.
func (p *S3Plugin) putBucketACL(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	// Verify bucket exists.
	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket",
			"The specified bucket does not exist.", http.StatusNotFound), nil
	}

	var acl S3AccessControlList
	if len(req.Body) > 0 {
		if err := xml.Unmarshal(req.Body, &acl); err != nil {
			return s3ErrorResponse("MalformedACLError", //nolint:nilerr
				"The XML you provided was not well-formed.", http.StatusBadRequest), nil
		}
	} else {
		// Honor the x-amz-acl canned ACL header.
		acl = s3CannedACL(req.Headers["X-Amz-Acl"], bucket)
	}

	raw, err := json.Marshal(acl)
	if err != nil {
		return nil, fmt.Errorf("marshal bucket acl: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "bucket_acl:"+bucket, raw); err != nil {
		return nil, fmt.Errorf("put bucket acl: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{},
	}, nil
}

// getObjectACL handles GET /<bucket>/<key>?acl.
func (p *S3Plugin) getObjectACL(_ *RequestContext, _ *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	// Verify object exists.
	objKey := "object:" + bucket + "/" + key
	existing, err := p.state.Get(ctx, s3Namespace, objKey)
	if err != nil {
		return nil, fmt.Errorf("check object: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchKey",
			"The specified key does not exist.", http.StatusNotFound), nil
	}

	raw, err := p.state.Get(ctx, s3Namespace, "object_acl:"+bucket+"/"+key)
	if err != nil {
		return nil, fmt.Errorf("get object acl: %w", err)
	}

	var acl S3AccessControlList
	if raw != nil {
		if err := json.Unmarshal(raw, &acl); err != nil {
			return nil, fmt.Errorf("unmarshal object acl: %w", err)
		}
	} else {
		acl = s3DefaultACL(bucket)
	}

	return s3XMLResponse(http.StatusOK, acl)
}

// putObjectACL handles PUT /<bucket>/<key>?acl.
func (p *S3Plugin) putObjectACL(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	// Verify object exists.
	objKey := "object:" + bucket + "/" + key
	existing, err := p.state.Get(ctx, s3Namespace, objKey)
	if err != nil {
		return nil, fmt.Errorf("check object: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchKey",
			"The specified key does not exist.", http.StatusNotFound), nil
	}

	var acl S3AccessControlList
	if len(req.Body) > 0 {
		if err := xml.Unmarshal(req.Body, &acl); err != nil {
			return s3ErrorResponse("MalformedACLError", //nolint:nilerr
				"The XML you provided was not well-formed.", http.StatusBadRequest), nil
		}
	} else {
		acl = s3CannedACL(req.Headers["X-Amz-Acl"], bucket)
	}

	raw, err := json.Marshal(acl)
	if err != nil {
		return nil, fmt.Errorf("marshal object acl: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "object_acl:"+bucket+"/"+key, raw); err != nil {
		return nil, fmt.Errorf("put object acl: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{},
	}, nil
}

// s3DefaultACL returns an owner-full-control ACL for the given resource.
func s3DefaultACL(resource string) S3AccessControlList {
	return S3AccessControlList{
		Owner: S3Owner{ID: resource + "-owner", DisplayName: resource},
		Grants: []S3Grant{{
			Grantee:    S3Grantee{Type: "CanonicalUser", ID: resource + "-owner"},
			Permission: "FULL_CONTROL",
		}},
	}
}

// getBucketNotificationConfiguration handles GET /<bucket>?notification.
func (p *S3Plugin) getBucketNotificationConfiguration(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	parts := strings.SplitN(strings.TrimPrefix(req.Path, "/"), "/", 2)
	bucket := parts[0]

	data, err := p.state.Get(context.Background(), s3Namespace, "notification:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("s3 getBucketNotificationConfiguration: %w", err)
	}
	if data == nil {
		// Return empty configuration.
		empty := S3NotificationConfiguration{}
		body, marshalErr := xml.Marshal(empty)
		if marshalErr != nil {
			return nil, fmt.Errorf("s3 getBucketNotificationConfiguration marshal: %w", marshalErr)
		}
		return &AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "application/xml"},
			Body:       body,
		}, nil
	}

	var cfg S3NotificationConfiguration
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("s3 getBucketNotificationConfiguration unmarshal: %w", err)
	}

	body, marshalErr := xml.Marshal(cfg)
	if marshalErr != nil {
		return nil, fmt.Errorf("s3 getBucketNotificationConfiguration xml marshal: %w", marshalErr)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/xml"},
		Body:       body,
	}, nil
}

// putBucketNotificationConfiguration handles PUT /<bucket>?notification.
func (p *S3Plugin) putBucketNotificationConfiguration(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	parts := strings.SplitN(strings.TrimPrefix(req.Path, "/"), "/", 2)
	bucket := parts[0]

	var cfg S3NotificationConfiguration
	if err := xml.Unmarshal(req.Body, &cfg); err != nil {
		// Try JSON fallback.
		if jsonErr := json.Unmarshal(req.Body, &cfg); jsonErr != nil {
			return nil, &AWSError{Code: "MalformedXML", Message: "invalid notification configuration", HTTPStatus: http.StatusBadRequest}
		}
	}

	data, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		return nil, fmt.Errorf("s3 putBucketNotificationConfiguration marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(context.Background(), s3Namespace, "notification:"+bucket, data); putErr != nil {
		return nil, fmt.Errorf("s3 putBucketNotificationConfiguration state.Put: %w", putErr)
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}, Body: nil}, nil
}

// fireNotifications dispatches S3 event notifications to configured Lambda
// functions and SQS queues. It is a best-effort operation: errors are logged
// but never returned to the caller.
func (p *S3Plugin) fireNotifications(ctx *RequestContext, bucket, key, eventName string, size int64, eTag string) {
	if p.registry == nil {
		return
	}

	data, err := p.state.Get(context.Background(), s3Namespace, "notification:"+bucket)
	if err != nil || data == nil {
		return
	}

	var notifCfg S3NotificationConfiguration
	if err := json.Unmarshal(data, &notifCfg); err != nil {
		p.logger.Warn("s3 fireNotifications: unmarshal error", "err", err)
		return
	}

	// Build S3 event payload.
	payload := p.buildS3EventPayload(ctx, bucket, key, eventName, size, eTag)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		p.logger.Warn("s3 fireNotifications: marshal payload error", "err", err)
		return
	}

	// Dispatch to Lambda functions.
	for _, lfCfg := range notifCfg.LambdaFunctionConfigurations {
		if !s3EventMatches(eventName, lfCfg.Events) {
			continue
		}
		if !s3KeyFilterMatches(key, lfCfg.Filter) {
			continue
		}
		// Extract function name from ARN.
		fnName := s3ARNLastComponent(lfCfg.LambdaFunctionArn)
		invokeReq := &AWSRequest{
			Service:   "lambda",
			Operation: "POST",
			Path:      "/2015-03-31/functions/" + fnName + "/invocations",
			Body:      payloadBytes,
			Headers:   map[string]string{},
			Params:    map[string]string{},
		}
		_, invokeErr := p.registry.RouteRequest(ctx, invokeReq)
		if invokeErr != nil {
			p.logger.Warn("s3 fireNotifications: lambda invoke error", "function", fnName, "err", invokeErr)
		}
	}

	// Dispatch to SQS queues.
	for _, qCfg := range notifCfg.QueueConfigurations {
		if !s3EventMatches(eventName, qCfg.Events) {
			continue
		}
		if !s3KeyFilterMatches(key, qCfg.Filter) {
			continue
		}
		queueURL := s3ARNToQueueURL(qCfg.QueueArn, ctx.Region, ctx.AccountID)
		sendReq := &AWSRequest{
			Service:   "sqs",
			Operation: "SendMessage",
			Body:      payloadBytes,
			Headers:   map[string]string{},
			Params: map[string]string{
				"Action":      "SendMessage",
				"QueueUrl":    queueURL,
				"MessageBody": string(payloadBytes),
			},
		}
		_, sendErr := p.registry.RouteRequest(ctx, sendReq)
		if sendErr != nil {
			p.logger.Warn("s3 fireNotifications: sqs send error", "queue", queueURL, "err", sendErr)
		}
	}

	// Dispatch to SNS topics.
	for _, tc2 := range notifCfg.TopicConfigurations {
		if !s3EventMatches(eventName, tc2.Events) {
			continue
		}
		if !s3KeyFilterMatches(key, tc2.Filter) {
			continue
		}
		_, pubErr := p.registry.RouteRequest(ctx, &AWSRequest{
			Service:   "sns",
			Operation: "Publish",
			Headers:   map[string]string{},
			Params: map[string]string{
				"Action":   "Publish",
				"TopicArn": tc2.TopicArn,
				"Message":  string(payloadBytes),
				"Subject":  "Amazon S3 Notification",
			},
		})
		if pubErr != nil {
			p.logger.Warn("s3 fireNotifications: sns publish error", "topicArn", tc2.TopicArn, "err", pubErr)
		}
	}
}

// buildS3EventPayload constructs the S3 event notification payload for the
// given bucket, key, event name, size, and ETag.
func (p *S3Plugin) buildS3EventPayload(ctx *RequestContext, bucket, key, eventName string, size int64, eTag string) map[string]interface{} {
	return map[string]interface{}{
		"Records": []map[string]interface{}{
			{
				"eventVersion": "2.1",
				"eventSource":  "aws:s3",
				"awsRegion":    ctx.Region,
				"eventTime":    p.tc.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
				"eventName":    eventName,
				"s3": map[string]interface{}{
					"s3SchemaVersion": "1.0",
					"bucket": map[string]interface{}{
						"name": bucket,
						"arn":  "arn:aws:s3:::" + bucket,
					},
					"object": map[string]interface{}{
						"key":  key,
						"size": size,
						"eTag": eTag,
					},
				},
			},
		},
	}
}

// s3EventMatches reports whether eventName matches any of the given patterns.
// Patterns may use a trailing ":*" wildcard, e.g. "s3:ObjectCreated:*".
func s3EventMatches(eventName string, patterns []string) bool {
	for _, p := range patterns {
		if p == eventName {
			return true
		}
		// Wildcard matching: "s3:ObjectCreated:*" matches "s3:ObjectCreated:Put"
		if strings.HasSuffix(p, ":*") {
			prefix := strings.TrimSuffix(p, "*")
			if strings.HasPrefix(eventName, prefix) {
				return true
			}
		}
	}
	return false
}

// s3KeyFilterMatches reports whether key satisfies all prefix/suffix rules in
// the given filter. A nil filter always matches.
func s3KeyFilterMatches(key string, filter *S3NotificationFilter) bool {
	if filter == nil {
		return true
	}
	for _, rule := range filter.Key.FilterRules {
		switch strings.ToLower(rule.Name) {
		case "prefix":
			if !strings.HasPrefix(key, rule.Value) {
				return false
			}
		case "suffix":
			if !strings.HasSuffix(key, rule.Value) {
				return false
			}
		}
	}
	return true
}

// s3ARNLastComponent extracts the last colon-separated component of an ARN,
// which for Lambda ARNs is the function name.
func s3ARNLastComponent(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return arn
}

// s3ARNToQueueURL converts an SQS queue ARN to a local queue URL.
// ARN format: arn:aws:sqs:{region}:{accountID}:{queueName}.
func s3ARNToQueueURL(arn, region, accountID string) string {
	parts := strings.Split(arn, ":")
	if len(parts) >= 6 {
		r := parts[3]
		a := parts[4]
		name := parts[5]
		if r == "" {
			r = region
		}
		if a == "" {
			a = accountID
		}
		return "http://sqs." + r + ".localhost/" + a + "/" + name
	}
	return arn
}

// --- Tagging operations ----------------------------------------------------

// s3Tag is the XML representation of a single S3 tag.
type s3Tag struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key"`
	Value   string   `xml:"Value"`
}

// s3Tagging is the XML representation of an S3 tagging document.
type s3Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  struct {
		Tags []s3Tag `xml:"Tag"`
	} `xml:"TagSet"`
}

func (p *S3Plugin) putBucketTagging(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	var tagging s3Tagging
	if err := xml.Unmarshal(req.Body, &tagging); err != nil {
		return s3ErrorResponse("MalformedXML", "The XML you provided was not well-formed.", http.StatusBadRequest), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	ctx := context.Background()
	data, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil || data == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	var b S3Bucket
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("putBucketTagging unmarshal: %w", err)
	}
	b.Tags = make(map[string]string)
	for _, tag := range tagging.TagSet.Tags {
		b.Tags[tag.Key] = tag.Value
	}
	newData, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("putBucketTagging marshal: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, "bucket:"+bucket, newData); err != nil {
		return nil, fmt.Errorf("putBucketTagging state.Put: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/xml"}, Body: nil}, nil
}

func (p *S3Plugin) getBucketTagging(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	data, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil || data == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	var b S3Bucket
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("getBucketTagging unmarshal: %w", err)
	}
	var result s3Tagging
	for k, v := range b.Tags {
		result.TagSet.Tags = append(result.TagSet.Tags, s3Tag{Key: k, Value: v})
	}
	return s3XMLResponse(http.StatusOK, result)
}

func (p *S3Plugin) deleteBucketTagging(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()
	data, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil || data == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	var b S3Bucket
	if unmarshalErr := json.Unmarshal(data, &b); unmarshalErr != nil {
		return nil, fmt.Errorf("deleteBucketTagging unmarshal: %w", unmarshalErr)
	}
	b.Tags = make(map[string]string)
	newData, marshalErr := json.Marshal(b)
	if marshalErr != nil {
		return nil, fmt.Errorf("deleteBucketTagging marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(ctx, s3Namespace, "bucket:"+bucket, newData); putErr != nil {
		return nil, fmt.Errorf("deleteBucketTagging state.Put: %w", putErr)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/xml"}, Body: nil}, nil
}

func (p *S3Plugin) putObjectTagging(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
	var tagging s3Tagging
	if err := xml.Unmarshal(req.Body, &tagging); err != nil {
		return s3ErrorResponse("MalformedXML", "The XML you provided was not well-formed.", http.StatusBadRequest), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	ctx := context.Background()
	stateKey := "object:" + bucket + "/" + key
	data, err := p.state.Get(ctx, s3Namespace, stateKey)
	if err != nil || data == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("putObjectTagging unmarshal: %w", err)
	}
	obj.Tags = make(map[string]string)
	for _, tag := range tagging.TagSet.Tags {
		obj.Tags[tag.Key] = tag.Value
	}
	newData, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("putObjectTagging marshal: %w", err)
	}
	if err := p.state.Put(ctx, s3Namespace, stateKey, newData); err != nil {
		return nil, fmt.Errorf("putObjectTagging state.Put: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/xml"}, Body: nil}, nil
}

func (p *S3Plugin) getObjectTagging(_ *RequestContext, _ *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()
	stateKey := "object:" + bucket + "/" + key
	data, err := p.state.Get(ctx, s3Namespace, stateKey)
	if err != nil || data == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("getObjectTagging unmarshal: %w", err)
	}
	var result s3Tagging
	for k, v := range obj.Tags {
		result.TagSet.Tags = append(result.TagSet.Tags, s3Tag{Key: k, Value: v})
	}
	return s3XMLResponse(http.StatusOK, result)
}

func (p *S3Plugin) deleteObjectTagging(_ *RequestContext, _ *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()
	stateKey := "object:" + bucket + "/" + key
	data, err := p.state.Get(ctx, s3Namespace, stateKey)
	if err != nil || data == nil {
		return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil //nolint:nilerr // intentionally converted to S3 XML error response
	}
	var obj S3Object
	if unmarshalErr := json.Unmarshal(data, &obj); unmarshalErr != nil {
		return nil, fmt.Errorf("deleteObjectTagging unmarshal: %w", unmarshalErr)
	}
	obj.Tags = nil
	newData, marshalErr := json.Marshal(obj)
	if marshalErr != nil {
		return nil, fmt.Errorf("deleteObjectTagging marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(ctx, s3Namespace, stateKey, newData); putErr != nil {
		return nil, fmt.Errorf("deleteObjectTagging state.Put: %w", putErr)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/xml"}, Body: nil}, nil
}

// s3CannedACL maps a canned ACL name (x-amz-acl header) to an S3AccessControlList.
// Unsupported or empty canned ACL values fall back to the private (owner-only) ACL.
func s3CannedACL(cannedACL, resource string) S3AccessControlList {
	owner := S3Owner{ID: resource + "-owner", DisplayName: resource}
	fullControlGrant := S3Grant{
		Grantee:    S3Grantee{Type: "CanonicalUser", ID: resource + "-owner"},
		Permission: "FULL_CONTROL",
	}
	publicReadGrant := S3Grant{
		Grantee:    S3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/global/AllUsers"},
		Permission: "READ",
	}

	switch cannedACL {
	case "public-read":
		return S3AccessControlList{Owner: owner, Grants: []S3Grant{fullControlGrant, publicReadGrant}}
	case "public-read-write":
		return S3AccessControlList{Owner: owner, Grants: []S3Grant{
			fullControlGrant,
			publicReadGrant,
			{Grantee: S3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/global/AllUsers"}, Permission: "WRITE"},
		}}
	default:
		// "private" and all other values → owner-only.
		return S3AccessControlList{Owner: owner, Grants: []S3Grant{fullControlGrant}}
	}
}

// --- Versioning helpers ------------------------------------------------------

// getBucketVersioningStatus returns "Enabled", "Suspended", or "" for the bucket.
func (p *S3Plugin) getBucketVersioningStatus(ctx context.Context, bucket string) string {
	data, err := p.state.Get(ctx, s3Namespace, "bucket_versioning:"+bucket)
	if err != nil || data == nil {
		return ""
	}
	return string(data)
}

// loadVersionIDs returns the newest-first list of version IDs for bucket/key.
func (p *S3Plugin) loadVersionIDs(ctx context.Context, bucket, key string) []string {
	data, err := p.state.Get(ctx, s3Namespace, "object_versions:"+bucket+"/"+key)
	if err != nil || data == nil {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil
	}
	return ids
}

// saveVersionIDs persists the version ID list for bucket/key.
func (p *S3Plugin) saveVersionIDs(ctx context.Context, bucket, key string, ids []string) {
	data, err := json.Marshal(ids)
	if err != nil {
		return
	}
	_ = p.state.Put(ctx, s3Namespace, "object_versions:"+bucket+"/"+key, data)
}

// --- Versioning operations ---------------------------------------------------

// putBucketVersioning handles PUT /<bucket>?versioning.
func (p *S3Plugin) putBucketVersioning(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	var cfg S3VersioningConfiguration
	if len(req.Body) > 0 {
		if parseErr := xml.Unmarshal(req.Body, &cfg); parseErr != nil {
			return nil, fmt.Errorf("parse versioning config: %w", parseErr)
		}
	}

	if cfg.Status == "" {
		cfg.Status = "Enabled"
	}
	if cfg.Status != "Enabled" && cfg.Status != "Suspended" {
		return s3ErrorResponse("IllegalVersioningConfigurationException",
			"The versioning configuration specified is not valid.", http.StatusBadRequest), nil
	}

	if err := p.state.Put(ctx, s3Namespace, "bucket_versioning:"+bucket, []byte(cfg.Status)); err != nil {
		return nil, fmt.Errorf("save versioning config: %w", err)
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// getBucketVersioning handles GET /<bucket>?versioning.
func (p *S3Plugin) getBucketVersioning(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	status := p.getBucketVersioningStatus(ctx, bucket)

	type versioningResp struct {
		XMLName xml.Name `xml:"VersioningConfiguration"`
		Xmlns   string   `xml:"xmlns,attr"`
		Status  string   `xml:"Status,omitempty"`
	}

	return s3XMLResponse(http.StatusOK, versioningResp{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Status: status,
	})
}

// listObjectVersions handles GET /<bucket>?versions.
func (p *S3Plugin) listObjectVersions(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	prefix := req.Params["prefix"]

	// Enumerate all object keys in the bucket.
	objKeys, err := p.state.List(ctx, s3Namespace, "object:"+bucket+"/")
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	result := ListObjectVersionsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:    bucket,
		MaxKeys: 1000,
	}

	seen := make(map[string]bool)
	for _, stateKey := range objKeys {
		// stateKey looks like "object:{bucket}/{key}" — extract the key part.
		key := strings.TrimPrefix(stateKey, "object:"+bucket+"/")
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if seen[key] {
			continue
		}
		seen[key] = true

		vids := p.loadVersionIDs(ctx, bucket, key)
		if len(vids) == 0 {
			// Object without versioning — return as a single version.
			objData, getErr := p.state.Get(ctx, s3Namespace, "object:"+bucket+"/"+key)
			if getErr != nil || objData == nil {
				continue
			}
			var obj S3Object
			if unmarshalErr := json.Unmarshal(objData, &obj); unmarshalErr != nil {
				continue
			}
			result.Versions = append(result.Versions, S3ObjectVersion{
				Key:          key,
				VersionID:    "null",
				IsLatest:     true,
				LastModified: obj.LastModified.UTC().Format(time.RFC3339Nano),
				ETag:         obj.ETag,
				Size:         obj.Size,
			})
			continue
		}

		for i, vid := range vids {
			versionedData, getErr := p.state.Get(ctx, s3Namespace, "object_version:"+bucket+"/"+key+"/"+vid)
			if getErr != nil || versionedData == nil {
				continue
			}
			var obj S3Object
			if unmarshalErr := json.Unmarshal(versionedData, &obj); unmarshalErr != nil {
				continue
			}
			isLatest := i == 0
			if obj.IsDeleteMarker {
				result.DeleteMarkers = append(result.DeleteMarkers, S3DeleteMarker{
					Key:          key,
					VersionID:    vid,
					IsLatest:     isLatest,
					LastModified: obj.LastModified.UTC().Format(time.RFC3339Nano),
				})
			} else {
				result.Versions = append(result.Versions, S3ObjectVersion{
					Key:          key,
					VersionID:    vid,
					IsLatest:     isLatest,
					LastModified: obj.LastModified.UTC().Format(time.RFC3339Nano),
					ETag:         obj.ETag,
					Size:         obj.Size,
				})
			}
		}
	}

	return s3XMLResponse(http.StatusOK, result)
}

// --- Lifecycle operations ----------------------------------------------------

// putBucketLifecycleConfiguration handles PUT /<bucket>?lifecycle.
// The configuration is stored as-is (config round-trip; no expiration logic).
func (p *S3Plugin) putBucketLifecycleConfiguration(_ *RequestContext, req *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	body := req.Body
	if len(body) == 0 {
		body = []byte("<LifecycleConfiguration/>")
	}
	if err := p.state.Put(ctx, s3Namespace, "bucket_lifecycle:"+bucket, body); err != nil {
		return nil, fmt.Errorf("save lifecycle config: %w", err)
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{}}, nil
}

// getBucketLifecycleConfiguration handles GET /<bucket>?lifecycle.
func (p *S3Plugin) getBucketLifecycleConfiguration(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	data, err := p.state.Get(ctx, s3Namespace, "bucket_lifecycle:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("get lifecycle config: %w", err)
	}
	if data == nil {
		return s3ErrorResponse("NoSuchLifecycleConfiguration",
			"The lifecycle configuration does not exist.", http.StatusNotFound), nil
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/xml"},
		Body:       data,
	}, nil
}

// deleteBucketLifecycle handles DELETE /<bucket>?lifecycle.
func (p *S3Plugin) deleteBucketLifecycle(_ *RequestContext, _ *AWSRequest, bucket string) (*AWSResponse, error) {
	ctx := context.Background()

	existing, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if existing == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	_ = p.state.Delete(ctx, s3Namespace, "bucket_lifecycle:"+bucket)

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}
