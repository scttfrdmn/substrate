package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/afero"
)

// The x-amz-copy-source header, read one way by everything that reads it (#532, #1489).
//
// CopyObject and UploadPartCopy document the same header: "specify the name of the source bucket and
// the key of the source object, separated by a slash (/) … The value must be URL-encoded", and "To
// copy a different version, use the versionId query parameter. Specifically, append
// ?versionId=<version-id> to the value" (API_CopyObject, API_UploadPartCopy). Because the value is
// URL-encoded, a "?" that is part of the key arrives as %3F, and the first unencoded "?" is where the
// version suffix begins. The split is therefore made on the raw header, before decoding; decoding
// first would let a key spelled "a%3FversionId%3Db" be read as key "a" at version "b".
//
// Authorization and both handlers call [parseS3CopySource], so the object a policy is evaluated
// against is the object the copy reads. Until #1489's review they disagreed: authorization cut the
// decoded header at "?versionId=" while the handlers kept it in the key, so a caller denied
// s3:GetObject on "a" could be authorized for "a?versionId=b" and the handler would read a key of that
// name.
//
// An access-point or Outposts ARN is the header's other published form. Substrate models no access
// points, so both are refused, in authorization and in the handlers alike, with NotImplemented/501 —
// the code S3's error list gives "A header you provided implies functionality that is not
// implemented". The message is substrate's own.

// s3CopySource is a parsed x-amz-copy-source: the source bucket, its URL-decoded key, and the version
// the request named, if any.
type s3CopySource struct {
	Bucket    string
	Key       string
	VersionID string
}

// ARN returns the ARN of the source object, the resource s3:GetObject is evaluated against.
func (s s3CopySource) ARN() string { return s3ResourceARN(s.Bucket + "/" + s.Key) }

// parseS3CopySource parses an x-amz-copy-source header. A non-nil *AWSResponse is the S3 error to
// return.
func parseS3CopySource(header string) (s3CopySource, *AWSResponse) {
	if header == "" {
		return s3CopySource{}, s3ErrorResponse("InvalidArgument", "Copy Source must be specified.", http.StatusBadRequest)
	}

	rawPath, rawQuery, _ := strings.Cut(header, "?")
	srcPath, decodeErr := url.QueryUnescape(rawPath)
	if decodeErr != nil {
		srcPath = rawPath
	}
	if strings.HasPrefix(srcPath, "arn:") {
		return s3CopySource{}, s3ErrorResponse("NotImplemented",
			"Copying from an access point or Outposts ARN is not supported by substrate.",
			http.StatusNotImplemented)
	}
	srcPath = strings.TrimPrefix(srcPath, "/")

	bucket, key, found := strings.Cut(srcPath, "/")
	if !found || bucket == "" || key == "" {
		return s3CopySource{}, s3ErrorResponse("InvalidArgument", "Copy source must include an object key.",
			http.StatusBadRequest)
	}

	var versionID string
	if rawQuery != "" {
		// A suffix that does not parse as a query names no version, the same reading as a suffix
		// that is not versionId at all: no error is published for either.
		if query, qErr := url.ParseQuery(rawQuery); qErr == nil {
			versionID = query.Get("versionId")
		}
	}
	return s3CopySource{Bucket: bucket, Key: key, VersionID: versionID}, nil
}

// loadS3CopySourceObject reads the metadata of the object a copy names: the current version, or the
// version the copy source names.
//
// A missing object or version is NoSuchKey/404, as GetObject answers. A current version that is a
// delete marker is the same 404: "If the current version is a delete marker, Amazon S3 behaves as if
// the object was deleted" (API_CopyObject). A version ID naming a delete marker is a 400 — "If you
// specify versionId in the x-amz-copy-source and the versionId is a delete marker, Amazon S3 returns
// an HTTP 400 Bad Request error, because you are not allowed to specify a delete marker as a version"
// (API_UploadPartCopy). The page gives no code; InvalidRequest is substrate's reading and the message
// is its own.
//
// An absent current version may be answered by a seeded object, as GetObject's is; see
// [S3Plugin.resolveSeededObject]. Its body is returned too, because a seed's bytes are not in the
// filesystem [S3Plugin.readS3CopySourceBody] reads. It is nil for every other source.
func (p *S3Plugin) loadS3CopySourceObject(ctx context.Context, src s3CopySource) (S3Object, []byte, *AWSResponse, error) {
	stateKey := "object:" + src.Bucket + "/" + src.Key
	if src.VersionID != "" {
		stateKey = "object_version:" + src.Bucket + "/" + src.Key + "/" + src.VersionID
	}
	data, err := p.state.Get(ctx, s3Namespace, stateKey)
	if err != nil {
		return S3Object{}, nil, nil, fmt.Errorf("get copy source metadata: %w", err)
	}
	if data == nil {
		// A missing source bucket is NoSuchBucket, as GetObject answers it, and is decided
		// before a seed is consulted: a "*" seed would otherwise answer for a bucket that
		// does not exist.
		if missing, bErr := p.bucketMissingResponse(ctx, src.Bucket); bErr != nil || missing != nil {
			return S3Object{}, nil, missing, bErr
		}
		if src.VersionID == "" {
			obj, body, handled, seedErr := p.resolveSeededObject(ctx, src.Bucket, src.Key)
			if seedErr != nil {
				return S3Object{}, nil, nil, seedErr
			}
			if handled {
				return obj, body, nil, nil
			}
		}
		return S3Object{}, nil, s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}
	var obj S3Object
	if err := json.Unmarshal(data, &obj); err != nil {
		return S3Object{}, nil, nil, fmt.Errorf("unmarshal copy source metadata: %w", err)
	}
	if obj.IsDeleteMarker {
		if src.VersionID != "" {
			return S3Object{}, nil, s3ErrorResponse("InvalidRequest",
				"The copy source version is a delete marker.", http.StatusBadRequest), nil
		}
		return S3Object{}, nil, s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), nil
	}
	return obj, nil, nil, nil
}

// readS3CopySourceBody reads the bytes of the object a copy names, or nil for a directory-marker key,
// which has none. A named version is read from its versioned path, falling back to the key's main
// path for an object written before versioning was enabled, as GetObject does. seeded is the body
// [S3Plugin.loadS3CopySourceObject] returned for a seeded source, which is used as it is.
func (p *S3Plugin) readS3CopySourceBody(src s3CopySource, seeded []byte) ([]byte, error) {
	if seeded != nil {
		return seeded, nil
	}
	if !s3ObjectHasBody(src.Key) {
		return nil, nil
	}
	mainPath := "/" + src.Bucket + "/" + src.Key
	if src.VersionID != "" {
		if body, err := afero.ReadFile(p.fs, "/"+src.Bucket+"/.versions/"+src.Key+"/"+src.VersionID); err == nil {
			return body, nil
		}
	}
	body, err := afero.ReadFile(p.fs, mainPath)
	if err != nil {
		return nil, fmt.Errorf("read copy source body: %w", err)
	}
	return body, nil
}
