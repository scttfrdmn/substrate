package emulator

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// byteRangeSpec is the resolved outcome of parsing a Range request header
// against a known object size. Three outcomes must be distinguishable, because
// they produce three different responses:
//
//   - the zero value: no usable range. Either the header was absent, malformed,
//     used a non-bytes unit, or asked for multiple ranges. S3 ignores all of
//     these and serves the whole object with 200.
//   - Satisfiable: serve Start..End inclusive with 206.
//   - Unsatisfiable: the range lies wholly outside the object. 416 InvalidRange.
type byteRangeSpec struct {
	// Satisfiable reports that Start and End describe a range to serve.
	Satisfiable bool
	// Unsatisfiable reports that the range cannot be met and 416 is required.
	Unsatisfiable bool
	// Start and End are inclusive byte offsets, valid only when Satisfiable.
	Start int64
	End   int64
}

// parseByteRange resolves a Range request header against an object of total
// bytes, following RFC 9110 section 14 as the S3 GetObject reference specifies.
//
// A range that runs past the end of the object is clamped rather than rejected,
// and a malformed or multi-range header is ignored rather than rejected — both
// match S3, and both mean a caller cannot rely on a 416 to detect a bad request.
// Only a range starting at or beyond the object's end is unsatisfiable. Every
// range against a zero-byte object is unsatisfiable.
func parseByteRange(header string, total int64) byteRangeSpec {
	spec, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(header)), "bytes=")
	if !ok {
		// Absent, or a unit other than bytes: ignore.
		return byteRangeSpec{}
	}
	if strings.Contains(spec, ",") {
		// "Amazon S3 doesn't support retrieving multiple ranges of data per
		// GET request" — the header is ignored, not an error.
		return byteRangeSpec{}
	}

	first, last, ok := strings.Cut(spec, "-")
	if !ok {
		return byteRangeSpec{}
	}
	first, last = strings.TrimSpace(first), strings.TrimSpace(last)

	switch {
	case first == "":
		// Suffix range: the final N bytes. A zero suffix-length is
		// unsatisfiable per RFC 9110, as is any suffix of an empty object.
		suffix, err := strconv.ParseInt(last, 10, 64)
		if err != nil || suffix < 0 {
			return byteRangeSpec{}
		}
		if suffix == 0 || total == 0 {
			return byteRangeSpec{Unsatisfiable: true}
		}
		start := total - suffix
		if start < 0 {
			start = 0
		}
		return byteRangeSpec{Satisfiable: true, Start: start, End: total - 1}

	case last == "":
		// Open-ended range: from Start to the end of the object.
		start, err := strconv.ParseInt(first, 10, 64)
		if err != nil || start < 0 {
			return byteRangeSpec{}
		}
		if start >= total {
			return byteRangeSpec{Unsatisfiable: true}
		}
		return byteRangeSpec{Satisfiable: true, Start: start, End: total - 1}

	default:
		start, serr := strconv.ParseInt(first, 10, 64)
		end, eerr := strconv.ParseInt(last, 10, 64)
		if serr != nil || eerr != nil || start < 0 || end < 0 || end < start {
			// A last-byte-pos below first-byte-pos makes the spec invalid,
			// which RFC 9110 says to ignore rather than reject.
			return byteRangeSpec{}
		}
		if start >= total {
			return byteRangeSpec{Unsatisfiable: true}
		}
		if end >= total {
			end = total - 1 // Clamp past EOF; not an error.
		}
		return byteRangeSpec{Satisfiable: true, Start: start, End: end}
	}
}

// applyByteRange sets the range response headers on a satisfiable spec and
// returns the status code to serve: 206 for a range, 200 otherwise.
//
// Content-Length is overwritten with the length of the range, which is required
// rather than cosmetic: [Server.writeResponse] only defaults Content-Length when
// a plugin supplied none, and [objectResponseHeaders] always supplies the full
// object size. Leaving it would desynchronize every SDK client.
func applyByteRange(spec byteRangeSpec, total int64, headers map[string]string) int {
	if !spec.Satisfiable {
		return http.StatusOK
	}
	length := spec.End - spec.Start + 1
	headers["Content-Range"] = fmt.Sprintf("bytes %d-%d/%d", spec.Start, spec.End, total)
	headers["Content-Length"] = strconv.FormatInt(length, 10)
	return http.StatusPartialContent
}

// s3InvalidRangeResponse builds the 416 InvalidRange response for a range that
// cannot be satisfied, carrying the object's actual size both as the
// Content-Range header RFC 9110 requires and as the <ActualObjectSize> element
// S3 includes, so a caller can correct the request without a second round trip.
func s3InvalidRangeResponse(rangeHeader string, total int64) *AWSResponse {
	return s3ErrorResponseWith(s3Error{
		Code:    "InvalidRange",
		Message: "The requested range cannot be satisfied.",
		Status:  http.StatusRequestedRangeNotSatisfiable,
		Details: []s3ErrorDetail{
			{Name: "RangeRequested", Value: rangeHeader},
			{Name: "ActualObjectSize", Value: strconv.FormatInt(total, 10)},
		},
		Headers: map[string]string{
			"Content-Range": fmt.Sprintf("bytes */%d", total),
		},
	})
}
