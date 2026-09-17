package emulator

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
)

// Marker pagination for the RDS and ElastiCache describes (#887, #916, #913).
//
// These helpers are the whole pagination contract for nine operations. #887 fixed the
// three that had a cursor — DescribeDBInstances, DescribeDBClusters and
// DescribeCacheClusters — and #916 brought the six that published Marker and MaxRecords
// and implemented neither: DescribeDBSnapshots, DescribeDBSubnetGroups,
// DescribeDBParameterGroups, DescribeReplicationGroups, DescribeCacheSubnetGroups and
// DescribeCacheParameterGroups. Those six answered the entire listing, emitted no Marker,
// and ignored a MaxRecords the caller sent, so a consumer's paging loop was dead code
// that first executed against real AWS and a Marker obtained elsewhere restarted the
// listing rather than being refused.
//
// Both families publish the same contract for their pagination token, and it is a
// contract about *records*, not about positions in a response array. Every one of the
// nine pages says of Marker: "If this parameter is specified, the response includes only
// records beyond the marker, up to the value specified by MaxRecords."
//
// Substrate read "beyond the marker" as an array index: the three paginated describes
// each did `offset, _ := strconv.Atoi(marker)` and then sliced `items[offset:]`. Two
// things follow from that, both observable:
//
//   - A record created or deleted between two pages shifts every later page. Delete the
//     first of five instances after reading page one, and page two — offset 2 into a
//     now-four-record listing — skips the record that moved into position 2 and never
//     reports it. Create one instead and a record is reported twice. This is the same
//     failure #865 fixed for [StateManager.List]'s ordering, one layer up: a cursor over
//     a shifting basis omits and repeats members, and a paging consumer cannot tell.
//   - strconv.Atoi's error was discarded, so a marker substrate never issued — a token
//     from another operation, a truncated copy, a hand-written string — was silently
//     treated as offset 0 and served page one again. A caller looping until the marker
//     is empty either spins or processes the same records twice, which is exactly the
//     shape of defect #884 described for ListBuckets.
//
// The fix is the cursor S3's ListBuckets already uses (see s3_list_buckets.go): the
// marker names the last record of the previous page, and the next page is the records
// whose identifier sorts strictly after it. Neither a creation nor a deletion elsewhere
// in the listing can then cost a record its place, because the cursor is a value in the
// record space rather than a count of what happened to precede it. The identifier comes
// from the state key's trailing segment, which [StateManager.List] returns in
// lexicographic order (#865) — so the order the cursor compares against and the order
// the records are rendered in are the same order by construction, not by agreement
// between two pieces of code.
//
// AWS documents nothing about the order these describes return records in — not one of
// the nine pages contains an ordering statement — so lexicographic remains *substrate's*
// reading, as it is for ListBuckets. Pagination is why that reading has to be a guarantee
// rather than a tidiness: the cursor is only well defined over a stable order.

// queryMaxRecordsDefault is the page size an RDS or ElastiCache describe applies when
// the caller names no MaxRecords, and queryMaxRecordsMin and queryMaxRecordsMax are the
// range a named one must fall in.
//
// All three are published on the parameter by every one of the nine pages, in the same
// words up to capitalisation and punctuation: "Default: 100" with "Constraints: Minimum
// 20, maximum 100." on the RDS pages (API_DescribeDBInstances, API_DescribeDBClusters,
// API_DescribeDBSnapshots, API_DescribeDBSubnetGroups, API_DescribeDBParameterGroups) and
// "Constraints: minimum 20; maximum 100." on the ElastiCache ones
// (API_DescribeCacheClusters, API_DescribeReplicationGroups,
// API_DescribeCacheSubnetGroups, API_DescribeCacheParameterGroups). The numbers are
// identical everywhere, which is what makes one range serve all nine.
//
// The default applies only to an *absent* MaxRecords. A value outside the range is
// refused by [queryMaxRecords] rather than defaulted or clamped, which is #913.
const (
	queryMaxRecordsDefault = 100
	queryMaxRecordsMin     = 20
	queryMaxRecordsMax     = 100
)

// queryMarkerCursor is a decoded RDS or ElastiCache Marker: the record identifier that
// the next page starts after.
//
// The zero value is the start of the listing, which is what an absent Marker means.
type queryMarkerCursor struct {
	after string
}

// parseQueryMarker decodes a Marker request parameter, reporting the error response to
// send when it is not a marker substrate could have issued.
//
// The marker is base64, matching the cursor ListBuckets emits, because AWS publishes
// nothing about the token's content beyond its being a token — the response element is
// described only as "a pagination token that can be used in a later request". Encoding
// it is what makes a marker substrate never issued detectable at all: a bare identifier
// or an offset left over from an older recording fails to decode and is refused rather
// than silently restarting the listing.
//
// **The error code is published for three of the nine operations and is substrate's
// reading for the other six, and the split is not the family boundary.** Three
// ElastiCache pages publish InvalidParameterValue with HTTP 400 —
// API_DescribeCacheClusters, API_DescribeReplicationGroups and
// API_DescribeCacheParameterGroups — so those three are sourced. The remaining six
// publish only their NotFound faults and say nothing about what an unusable Marker
// answers: the five RDS pages, and API_DescribeCacheSubnetGroups, which is ElastiCache
// yet publishes CacheSubnetGroupNotFoundFault/400 alone (#916 corrected its own body on
// this point). InvalidParameterValue/400 is used for all nine so one helper serves them,
// and it is already the code substrate's RDS handlers answer for a malformed parameter
// value.
//
// A base64 string that happens to decode to something other than a real identifier is
// not refused — it simply selects the records sorting after that value, which may be
// none. Distinguishing a well-formed marker from a marker of a record that has since
// been deleted is not possible, and not desirable: resuming after a deleted record is
// precisely what the value-based cursor is for.
func parseQueryMarker(raw string) (queryMarkerCursor, *AWSError) {
	if raw == "" {
		return queryMarkerCursor{}, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return queryMarkerCursor{}, &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "The marker provided is not valid.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return queryMarkerCursor{after: string(decoded)}, nil
}

// skip reports whether the record identified by id belongs to a page already returned.
//
// The comparison is a strict ">" against the marker, so the record the marker names is
// not repeated and no record is required to still exist for the ones after it to be
// reachable.
func (c queryMarkerCursor) skip(id string) bool {
	return c.after != "" && id <= c.after
}

// encodeQueryMarker renders the Marker that resumes a listing after the record
// identified by id.
func encodeQueryMarker(id string) string {
	return base64.StdEncoding.EncodeToString([]byte(id))
}

// queryMaxRecords reads a MaxRecords request parameter, reporting the error response to
// send when the caller named a page size the operation does not accept.
//
// An absent MaxRecords is [queryMaxRecordsDefault], which is what both families publish.
// Anything else must be an integer within [queryMaxRecordsMin] and [queryMaxRecordsMax];
// a value outside the range, and a value that is not an integer at all, is refused (#913).
//
// Refusing rather than honoring or coercing is the point. Substrate honored
// MaxRecords=5, which real RDS refuses, so a consumer paging five records at a time
// worked here and failed against AWS — the direction of divergence that matters — and it
// rewrote MaxRecords=0, -1 and abc to 100, a substitution invisible in a well-formed
// response: a caller asking for a small page and receiving a hundred records sees the
// same shape as a caller whose listing is short. This is the argument the IAM MaxItems
// coercion was corrected on, and the message names the range as parseSimulateRequest and
// parseS3ListBucketsParams already do.
//
// **The code's provenance is the same three-of-nine split [parseQueryMarker] records, and
// for the same reason.** API_DescribeCacheClusters, API_DescribeReplicationGroups and
// API_DescribeCacheParameterGroups publish InvalidParameterValue with HTTP 400 ("The
// value for a parameter is invalid."); the five RDS pages and
// API_DescribeCacheSubnetGroups publish only their NotFound faults, so nothing on them
// says what an out-of-range MaxRecords answers. One code serves all nine, matching
// [parseQueryMarker], so the two parameters of one cursor cannot be refused under
// different codes.
func queryMaxRecords(raw string) (int, *AWSError) {
	if raw == "" {
		return queryMaxRecordsDefault, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < queryMaxRecordsMin || n > queryMaxRecordsMax {
		return 0, &AWSError{
			Code: "InvalidParameterValue",
			Message: "MaxRecords must be an integer between " +
				strconv.Itoa(queryMaxRecordsMin) + " and " + strconv.Itoa(queryMaxRecordsMax) + ".",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return n, nil
}

// queryMarkerPage collects one page of records from keys, resuming after cursor and
// reporting the Marker for the page after it.
//
// record is called with each candidate key and the identifier taken from the key's
// trailing segment after prefix; it returns the rendered record, or ok=false for a key
// that holds nothing, fails to decode, or is filtered out by the request. Every caller
// therefore agrees on where the identifier comes from and on when the listing
// truncates, which is the property the three offset cursors did not have: they each
// computed their own next marker.
//
// Truncation is decided on the *next* matching record rather than on the page filling
// up, so a listing whose last page exactly fills MaxRecords reports no Marker. Emitting
// one there would cost a paging caller a round trip to an empty page, and both families
// tie the marker to "if more records exist than the specified MaxRecords value" rather
// than to a full page.
func queryMarkerPage[T any](keys []string, prefix string, cursor queryMarkerCursor, maxRecords int, record func(key, id string) (T, bool)) ([]T, string) {
	var (
		page   []T
		lastID string
	)
	for _, k := range keys {
		id := strings.TrimPrefix(k, prefix)
		if cursor.skip(id) {
			continue
		}
		item, ok := record(k, id)
		if !ok {
			continue
		}
		if len(page) >= maxRecords {
			return page, encodeQueryMarker(lastID)
		}
		page = append(page, item)
		lastID = id
	}
	return page, ""
}
