package emulator

import (
	"encoding/base64"
	"strconv"
)

// Offset pagination tokens, and what makes one a token substrate never issued (#915).
//
// Three operations page by encoding an array offset — CloudWatch DescribeAlarms and
// Systems Manager DescribeParameters and GetParametersByPath — and all three decoded the
// token like this:
//
//	if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
//		if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
//			offset = n
//		}
//	}
//
// Both errors are discarded, so a token substrate could not have issued — a token from
// another operation, a truncated copy, a hand-written string, an offset left over from an
// older recording — left `offset` at its zero value and the operation answered **page
// one**. That is the failure #884 named and #887 fixed for the RDS and ElastiCache
// markers: a caller looping until the token comes back empty is handed the first page
// again and loops forever, or processes the same records twice, and nothing in the
// response says so. A well-formed page is the one answer a caller cannot detect as wrong.
//
// The refusal is per-caller rather than in this file, because the three do not share a
// code: CloudWatch publishes InvalidNextToken with "The next token specified is invalid."
// and Systems Manager publishes InvalidNextToken with "The specified token isn't valid."
// This file decides only what counts as issuable, so the three cannot disagree about that
// while each keeps its own published shape.
//
// A decoded offset past the end of the listing is **not** refused. That token is one
// substrate did issue, over a listing that has since shrunk, and clamping it to a final
// empty page is the honest answer; refusing it would break a legitimate walk whose
// records were deleted mid-loop. This is the same distinction parseQueryMarker records for
// the value-based cursor: a marker naming a deleted record resumes after it rather than
// failing.
//
// What this does not fix, recorded so it is not read as fixed: an offset cursor over a
// listing that changes between pages skips or repeats records, which is the *other* half
// of #884 and the reason #887 converted the RDS and ElastiCache describes to a
// value-based cursor (see query_marker_pagination.go). These three still page by offset.
// Refusing an unissued token and choosing a stable cursor basis are independent defects,
// and only the first is #915.

// decodeOffsetPaginationToken decodes a base64-encoded array offset, reporting whether the
// token is one substrate could have issued.
//
// An empty token is the start of the listing, which is what an absent token means, so it
// decodes to offset 0 and is accepted. Anything else must be base64 whose decoded text is
// exactly what [encodeOffsetPaginationToken] would have produced for the offset it names:
// the round-trip comparison is the whole rule, and it is written that way rather than as a
// list of rejections so that the encoder defines what is issuable. It refuses a token that
// is not base64, one whose text is not an integer, a negative offset, and the forms
// strconv.Atoi accepts but the encoder never emits — "+5", "05", "-0".
//
// A decoded "0" round-trips and is therefore accepted, even though the encoder emits it
// for no page: it names the position page one starts at, so refusing it would answer an
// error where substrate can answer correctly.
func decodeOffsetPaginationToken(raw string) (int, bool) {
	if raw == "" {
		return 0, true
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return 0, false
	}
	text := string(decoded)
	offset, err := strconv.Atoi(text)
	if err != nil || offset < 0 || strconv.Itoa(offset) != text {
		return 0, false
	}
	return offset, true
}

// encodeOffsetPaginationToken renders the token that resumes a listing at the given
// offset.
//
// It is what the three operations already emitted, hoisted here so that the encoding and
// the rule for what is issuable are stated in one place rather than agreeing by
// coincidence at six sites.
func encodeOffsetPaginationToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// pageByOffsetToken returns the items at offset, at most pageSize of them, and the token
// that resumes the listing after them.
//
// The token is emitted only when a further item exists, so the last page carries none and a
// listing that is an exact multiple of the page size costs no round trip to an empty page.
// Every AWS page that publishes this style of cursor says the same thing about it — Lambda's
// NextMarker is "returned when the response doesn't contain all event source mappings" — so a
// token on a full final page would describe a page that does not exist. An offset past the
// end clamps to an empty page rather than erroring, for the reason
// [decodeOffsetPaginationToken] gives.
//
// A pageSize of zero or less means no limit: the whole remainder is returned with no token.
// No caller passes one today, since both callers apply a published default, but the case is
// defined rather than left to produce an empty page.
//
// The offset is only meaningful because the items are in a stable order, which each caller
// establishes before cutting — [StateManager.List]'s lexicographic guarantee (#865), or an
// explicit sort where the slice comes from an index rather than from a key scan. Nothing here
// can check that, so it is the caller's obligation and is stated at each call site.
//
// EC2's [ec2Page] is the same cut over a **decimal** token, kept separate because that
// service's two original paginators already issued that wire shape (#917); this one is for
// the operations whose token is the base64 form above.
func pageByOffsetToken[T any](items []T, offset, pageSize int) ([]T, string) {
	if offset > len(items) {
		offset = len(items)
	}
	page := items[offset:]
	if pageSize > 0 && len(page) > pageSize {
		return page[:pageSize], encodeOffsetPaginationToken(offset + pageSize)
	}
	return page, ""
}
