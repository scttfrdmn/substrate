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
	state  StateManager
	logger Logger
	tc     *TimeController
	fs     afero.Fs
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
	case "CreateMultipartUpload":
		return p.createMultipartUpload(ctx, req, bucket, key)
	case "UploadPart":
		return p.uploadPart(ctx, req, bucket, key)
	case "CompleteMultipartUpload":
		return p.completeMultipartUpload(ctx, req, bucket, key)
	case "AbortMultipartUpload":
		return p.abortMultipartUpload(ctx, req, bucket, key)
	default:
		// TODO(#22): event notifications via Lambda/SQS not yet implemented.
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
	// Trim trailing slash produced by virtual-hosted bucket-root requests.
	key = strings.TrimSuffix(key, "/")

	method := req.Operation // still the HTTP verb at this point

	if key == "" {
		// Bucket-level operations.
		switch method {
		case "PUT":
			return bucket, "", "CreateBucket"
		case "HEAD":
			return bucket, "", "HeadBucket"
		case "DELETE":
			return bucket, "", "DeleteBucket"
		case "GET":
			if req.Params["uploads"] == "1" {
				return bucket, "", "ListMultipartUploads"
			}
			if req.Params["list-type"] == "2" {
				return bucket, "", "ListObjectsV2"
			}
			return bucket, "", "ListObjects"
		}
	} else {
		// Object-level operations.
		switch method {
		case "PUT":
			if req.Headers["X-Amz-Copy-Source"] != "" {
				return bucket, key, "CopyObject"
			}
			if req.Params["partNumber"] != "" && req.Params["uploadId"] != "" {
				return bucket, key, "UploadPart"
			}
			return bucket, key, "PutObject"
		case "GET":
			return bucket, key, "GetObject"
		case "HEAD":
			return bucket, key, "HeadObject"
		case "DELETE":
			if req.Params["uploadId"] != "" {
				return bucket, key, "AbortMultipartUpload"
			}
			return bucket, key, "DeleteObject"
		case "POST":
			if req.Params["uploads"] == "1" {
				return bucket, key, "CreateMultipartUpload"
			}
			if req.Params["uploadId"] != "" {
				return bucket, key, "CompleteMultipartUpload"
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
func (p *S3Plugin) putObject(_ *RequestContext, req *AWSRequest, bucket, key string) (*AWSResponse, error) {
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

	filePath := "/" + bucket + "/" + key
	if mkdirErr := p.fs.MkdirAll(filepath.Dir(filePath), 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("mkdir: %w", mkdirErr)
	}
	if writeErr := afero.WriteFile(p.fs, filePath, body, 0o644); writeErr != nil {
		return nil, fmt.Errorf("write object body: %w", writeErr)
	}

	userMeta := extractUserMetadata(req.Headers)

	contentType := req.Headers["Content-Type"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj := S3Object{
		Bucket:       bucket,
		Key:          key,
		ETag:         etag,
		ContentType:  contentType,
		Size:         int64(len(body)),
		LastModified: p.tc.Now(),
		UserMetadata: userMeta,
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal object metadata: %w", err)
	}

	if err := p.state.Put(ctx, s3Namespace, "object:"+bucket+"/"+key, data); err != nil {
		return nil, fmt.Errorf("save object metadata: %w", err)
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"ETag": etag},
	}, nil
}

// getObject handles GET /<bucket>/<key>.
func (p *S3Plugin) getObject(_ *RequestContext, _ *AWSRequest, bucket, key string) (*AWSResponse, error) {
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

	body, readErr := afero.ReadFile(p.fs, "/"+bucket+"/"+key)
	if readErr != nil {
		return nil, fmt.Errorf("read object body: %w", readErr)
	}

	headers := map[string]string{
		"Content-Type":   obj.ContentType,
		"ETag":           obj.ETag,
		"Last-Modified":  obj.LastModified.UTC().Format(http.TimeFormat),
		"Content-Length": strconv.FormatInt(obj.Size, 10),
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
	for k, v := range obj.UserMetadata {
		headers["X-Amz-Meta-"+k] = v
	}

	return &AWSResponse{StatusCode: http.StatusOK, Headers: headers}, nil
}

// deleteObject handles DELETE /<bucket>/<key>.
// S3 DELETE is idempotent: no error is returned when the key is absent.
func (p *S3Plugin) deleteObject(_ *RequestContext, _ *AWSRequest, bucket, key string) (*AWSResponse, error) {
	ctx := context.Background()

	bucketData, err := p.state.Get(ctx, s3Namespace, "bucket:"+bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if bucketData == nil {
		return s3ErrorResponse("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound), nil
	}

	_ = p.state.Delete(ctx, s3Namespace, "object:"+bucket+"/"+key)
	_ = p.fs.Remove("/" + bucket + "/" + key)

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
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

	result.KeyCount = count
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
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		Initiated:   p.tc.Now(),
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
		UserMetadata: make(map[string]string),
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

// s3XMLResponse serialises v as XML and wraps it in an [AWSResponse] with the
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
