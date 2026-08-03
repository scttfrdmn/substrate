package emulator

// S3SystemMetadata holds the user-controlled system-metadata headers S3 stores on
// an object and returns on every read: Cache-Control, Content-Disposition,
// Content-Language and Expires (#430).
//
// It is embedded in both [S3Object] and [S3MultipartUpload] rather than declared
// twice, so the two write paths cannot disagree about which headers survive a
// write. That divergence is exactly what #406 was — Content-Encoding was carried
// through PutObject and dropped by the multipart path — and one shared declaration
// makes the repeat structurally impossible: CompleteMultipartUpload copies the
// whole struct in one assignment, so a header added here is carried by both paths
// or by neither.
//
// The four are grouped because substrate treats them identically: recorded verbatim
// from the request, echoed verbatim on a read, never interpreted. Content-Type,
// Content-Encoding and the storage class are user-controlled system metadata too
// but are deliberately *not* in this struct — each has resolution rules of its own
// (a default of application/octet-stream, aws-chunked filtering, and a
// STANDARD-means-absent read rule respectively), so folding them in would mean
// special cases in every helper below.
//
// Values are stored as strings, Expires included. The Go SDK's own GetObject output
// deprecates its time.Time Expires in favor of ExpiresString, described as "the
// unparsed value of the Expires field from the service response" — S3 stores and
// returns whatever the caller sent, so parsing it here would be lower fidelity, not
// higher. A malformed date round-trips unchanged, which is what real S3 does and
// what lets a consumer's parse-failure branch be tested at all.
type S3SystemMetadata struct {
	// CacheControl is the Cache-Control header recorded on write, echoed on read.
	CacheControl string `json:"cache_control,omitempty"`

	// ContentDisposition is the Content-Disposition header recorded on write,
	// echoed on read — the "attachment; filename=…" download name.
	ContentDisposition string `json:"content_disposition,omitempty"`

	// ContentLanguage is the Content-Language header recorded on write, echoed on
	// read.
	ContentLanguage string `json:"content_language,omitempty"`

	// Expires is the Expires header recorded on write, echoed on read, stored
	// verbatim and never parsed as a date.
	Expires string `json:"expires,omitempty"`
}

// s3MetadataField pairs a metadata header's wire name with the field holding it, so
// capture, emission and copying all walk one list instead of repeating the family
// member by member.
type s3MetadataField struct {
	header string
	value  *string
}

// fields returns the header-to-field pairing in a fixed order. Adding a header to
// this family means one new struct field and one new entry here; nothing else in the
// emulator enumerates them.
func (m *S3SystemMetadata) fields() []s3MetadataField {
	return []s3MetadataField{
		{header: "Cache-Control", value: &m.CacheControl},
		{header: "Content-Disposition", value: &m.ContentDisposition},
		{header: "Content-Language", value: &m.ContentLanguage},
		{header: "Expires", value: &m.Expires},
	}
}

// resolveSystemMetadata reads the metadata family from request headers.
//
// Headers are matched case-insensitively via [headerValueFold] rather than by direct
// map access: an [AWSRequest] reaching a plugin has not always been through
// net/http's header canonicalization, since substrate builds requests in-process too
// (see cfn_deployer.go), and RFC 9110 header names are case-insensitive regardless.
//
// An absent header yields the empty string, which [S3SystemMetadata.emit] omits from
// the response — S3 returns no header for metadata that was never set, and an empty
// Cache-Control on a read is a different observation from no Cache-Control at all.
func resolveSystemMetadata(headers map[string]string) S3SystemMetadata {
	var meta S3SystemMetadata
	for _, f := range meta.fields() {
		*f.value = headerValueFold(headers, f.header)
	}
	return meta
}

// emit adds every recorded metadata header to a response header map, skipping the
// ones the object does not carry.
func (m *S3SystemMetadata) emit(dst map[string]string) {
	for _, f := range m.fields() {
		if *f.value != "" {
			dst[f.header] = *f.value
		}
	}
}
