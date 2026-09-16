package emulator

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
)

// Marker pagination for the RDS and ElastiCache describes (#887).
//
// Both families publish the same contract for their pagination token, and it is a
// contract about *records*, not about positions in a response array.
// API_DescribeDBInstances, API_DescribeDBClusters, API_DescribeCacheClusters,
// API_DescribeReplicationGroups and API_DescribeCacheSubnetGroups all say of Marker:
// "If this parameter is specified, the response includes only records beyond the
// marker, up to the value specified by MaxRecords."
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
// AWS documents nothing about the order these describes return records in — none of the
// five pages above contains an ordering statement — so lexicographic remains
// *substrate's* reading, as it is for ListBuckets. Pagination is why that reading has to
// be a guarantee rather than a tidiness: the cursor is only well defined over a stable
// order.

// queryMaxRecordsDefault is the page size an RDS or ElastiCache describe applies when
// the caller names no MaxRecords.
//
// It is the default both families publish on the parameter: "Default: 100". The
// documented range — "Constraints: Minimum 20, maximum 100" — is deliberately not
// enforced here; substrate honors an out-of-range MaxRecords today, and refusing it is
// a separate defect from the cursor basis (#913).
const queryMaxRecordsDefault = 100

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
// **The error code is substrate's reading for RDS.** API_DescribeCacheClusters and
// API_DescribeReplicationGroups both publish InvalidParameterValue with HTTP 400, so
// ElastiCache is sourced; API_DescribeDBInstances and API_DescribeDBClusters publish
// only their NotFound faults, and nothing on either page says what an unusable Marker
// answers. InvalidParameterValue/400 is used for both families so one helper serves
// them, and it is already the code substrate's RDS handlers answer for a malformed
// parameter value.
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

// queryMaxRecords reads a MaxRecords request parameter, falling back to
// [queryMaxRecordsDefault] when it is absent or unusable.
//
// Coercion rather than refusal is what substrate does today at all three call sites;
// see [queryMaxRecordsDefault] for why correcting it is filed separately rather than
// folded in here.
func queryMaxRecords(raw string) int {
	if raw == "" {
		return queryMaxRecordsDefault
	}
	if n, err := strconv.Atoi(raw); err == nil && n > 0 {
		return n
	}
	return queryMaxRecordsDefault
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
