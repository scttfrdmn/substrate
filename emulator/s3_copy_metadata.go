package emulator

import (
	"net/http"
	"net/url"
	"strings"
)

// s3DirectiveCopy and s3DirectiveReplace are the two values S3 accepts in
// x-amz-metadata-directive and x-amz-tagging-directive. COPY is the default for
// both when the header is absent.
const (
	s3DirectiveCopy    = "COPY"
	s3DirectiveReplace = "REPLACE"
)

// s3CopyMetadata is the metadata a CopyObject should record on its destination,
// already resolved against the request's x-amz-metadata-directive.
type s3CopyMetadata struct {
	ContentType     string
	ContentEncoding string
	UserMetadata    map[string]string

	// System is the Cache-Control/Content-Disposition/Content-Language/Expires
	// family, resolved under the same directive as the fields above. Embedding
	// [S3SystemMetadata] here rather than restating its members means a header added
	// to the family is carried by CopyObject without editing this file (#430).
	System S3SystemMetadata
}

// resolveDirective returns the COPY/REPLACE value of a directive header, or the
// error response to serve when the value is neither.
//
// An absent header means COPY: "if this header isn't specified, COPY is the default
// behavior". An unrecognized value is rejected rather than silently treated as the
// default, because a typo'd directive that quietly preserved metadata is exactly the
// kind of false success this emulator exists to catch. The code is S3's generic
// 400 for a bad header value; the message wording is substrate's.
func resolveDirective(headers map[string]string, name string) (string, *AWSResponse) {
	value := headerValueFold(headers, name)
	switch {
	case value == "":
		return s3DirectiveCopy, nil
	case strings.EqualFold(value, s3DirectiveCopy):
		return s3DirectiveCopy, nil
	case strings.EqualFold(value, s3DirectiveReplace):
		return s3DirectiveReplace, nil
	}
	return "", s3ErrorResponseWith(s3Error{
		Code:    "InvalidArgument",
		Message: "Unknown " + name + ". The directive must be COPY or REPLACE.",
		Status:  http.StatusBadRequest,
		Details: []s3ErrorDetail{
			{Name: "ArgumentName", Value: name},
			{Name: "ArgumentValue", Value: value},
		},
	})
}

// resolveCopyMetadata returns the metadata a CopyObject records on its destination,
// or the error response to serve when x-amz-metadata-directive is malformed.
//
// Under COPY — the default — the source's user-controlled system metadata carries
// over along with its user-defined metadata: "when you copy an object,
// user-controlled system metadata and user-defined metadata are also copied", and
// Content-Type, Content-Encoding, Content-Disposition and Cache-Control are all
// user-controlled. Only x-amz-website-redirect-location is documented as not copied,
// and substrate does not model it.
//
// Under REPLACE nothing carries over. The request must restate everything it wants
// kept: "you must explicitly specify all of the user-configurable metadata present
// on the source object in your request, even if you are changing only one of the
// metadata values". This is the asymmetry a consumer's in-place CopyObject tier
// transition trips over — a REPLACE that omits Content-Encoding loses it.
//
// Cache-Control, Content-Disposition, Content-Language and Expires follow exactly
// the same rule, and deliberately share the one directive rather than getting
// per-header treatment (#430). S3 documents a single x-amz-metadata-directive
// governing "the metadata", with no per-header variant, and all four are
// user-controlled system metadata by the same definition Content-Type and
// Content-Encoding are. So a REPLACE that restates only Content-Type drops the
// download name a consumer set — which is the failure this models, not an
// implementation shortcut.
func resolveCopyMetadata(headers map[string]string, src *S3Object) (s3CopyMetadata, *AWSResponse) {
	directive, errResp := resolveDirective(headers, "x-amz-metadata-directive")
	if errResp != nil {
		return s3CopyMetadata{}, errResp
	}

	if directive == s3DirectiveCopy {
		return s3CopyMetadata{
			ContentType:     src.ContentType,
			ContentEncoding: src.ContentEncoding,
			UserMetadata:    copyStringMap(src.UserMetadata),
			System:          src.S3SystemMetadata,
		}, nil
	}

	contentType := headerValueFold(headers, "Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return s3CopyMetadata{
		ContentType:     contentType,
		ContentEncoding: headerValueFold(headers, "Content-Encoding"),
		UserMetadata:    extractUserMetadata(headers),
		System:          resolveSystemMetadata(headers),
	}, nil
}

// resolveCopyTags returns the tag-set a CopyObject records on its destination, or
// the error response to serve when x-amz-tagging-directive is malformed.
//
// COPY is the default here too, so an ordinary copy carries the source's tags:
// "if you choose COPY for the x-amz-tagging-directive, you don't need to set the
// x-amz-tagging header, because the tag-set will be copied from the source object
// directly". Under REPLACE the tag-set comes from x-amz-tagging, which S3 documents
// as URL query-parameter encoded and defaulting to empty.
func resolveCopyTags(headers map[string]string, src *S3Object) (map[string]string, *AWSResponse) {
	directive, errResp := resolveDirective(headers, "x-amz-tagging-directive")
	if errResp != nil {
		return nil, errResp
	}

	if directive == s3DirectiveCopy {
		return copyStringMap(src.Tags), nil
	}

	tagging := headerValueFold(headers, "x-amz-tagging")
	if tagging == "" {
		return nil, nil
	}
	values, parseErr := url.ParseQuery(tagging)
	if parseErr != nil {
		return nil, s3ErrorResponseWith(s3Error{
			Code:    "InvalidArgument",
			Message: "The tag-set must be encoded as URL query parameters.",
			Status:  http.StatusBadRequest,
			Details: []s3ErrorDetail{{Name: "ArgumentName", Value: "x-amz-tagging"}},
		})
	}
	tags := make(map[string]string, len(values))
	for k := range values {
		tags[k] = values.Get(k)
	}
	return tags, nil
}

// copyStringMap returns an independent copy of m, or nil when m is empty. The copy
// keeps a destination object's metadata from aliasing the source's, so a later
// PutObjectTagging on one does not mutate the other.
func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
