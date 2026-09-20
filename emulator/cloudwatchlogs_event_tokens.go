package emulator

import "strings"

// GetLogEvents' pair of directional tokens, and the termination rule that needs both (#1223).
//
// GetLogEvents is the one Logs paginator whose published cursor is a **pair**. Its overview states the
// termination rule in terms of that pair:
//
//	As long as the nextBackwardToken or nextForwardToken returned is NOT equal to the nextToken that
//	you passed into the API call, there might be more log events available. The token that you use
//	depends on the direction you want to move in along the log stream. The returned tokens are never
//	null.
//
// and the response elements repeat it individually — nextBackwardToken is *"not null"* and *"If you
// have reached the end of the stream, it returns the same token you passed in"*, nextForwardToken the
// same about its own direction. Both carry Length Constraints of minimum 1, so neither has an empty
// form to mean anything by.
//
// Substrate emitted nextForwardToken only when a further page existed and never emitted
// nextBackwardToken at all, which made the published rule unusable: a loop written as "stop when the
// returned token equals the one I sent" was handed no token to compare, so it either compared the
// empty string against its own empty first token and stopped before reading anything, or spun. The
// only rule that worked was the empty-token one the *other three* Logs paginators publish and this one
// does not — so a caller written from this page could not walk this operation, and one written against
// substrate would not walk AWS.
//
// # What a token is
//
// A token names a **direction** and a **position**, because the pair is only meaningful if the two are
// distinguishable: a backward token silently read as a forward offset is the same class of wrong answer
// #1086 refused a foreign token for — a well-formed page that is not the page the caller asked for.
// The direction is a prefix, which is the shape AWS's own examples publish:
//
//	"nextBackwardToken": "b/31132629274945519779805322857203735586714454643391594505",
//	"nextForwardToken":  "f/31132629323784151764587387538205132201699397759403884544"
//
// Only the prefix is a read; what follows it in AWS's tokens is opaque, and substrate keeps the offset
// encoding the other three Logs paginators use ([encodeOffsetPaginationToken]) behind it. The prefix is
// also what keeps #1086's refusal working in both directions: a bare offset token from
// DescribeLogGroups or FilterLogEvents no longer decodes here, and a token from here no longer decodes
// there, where before all four shared one wire shape and indexed into each other's listings.
//
// A forward token names the position **after** the page it was returned with; a backward token names
// the position **before** it. That is what makes the published termination rule fall out rather than
// being special-cased: a forward token at the end of the stream cuts an empty page, whose own forward
// token names the same position — so the token comes back equal to the one that was passed in, exactly
// as the page says. The same holds for a backward token at the head.
//
// This is why GetLogEvents does not use [pageByOffsetToken]. That helper omits the token on a final
// page, which is right for a cursor whose absence means "done" and wrong for one whose *equality* means
// it: under AWS's rule a full final page still carries a forward token, and presenting it costs one
// round trip to an empty page. AWS says as much in the overview — *"Partially full or empty pages don't
// necessarily mean that pagination is finished."*
//
// # What is not modeled, so that this is not read as having modeled it
//
//   - **The 24-hour expiry.** Both response elements publish *"The token expires after 24 hours."* and
//     no Logs page publishes a code for presenting an expired token. Modeling it would need the token
//     to carry its issue time and the simulated clock to judge it — which is in scope for substrate,
//     the clock being simulated — but the refusal would have to be attributed to
//     InvalidParameterException/400, the only code the page publishes for a bad parameter, and that
//     attribution is a reading rather than a read. Declined here and recorded in docs/services.md
//     rather than decided silently; it was already recorded as out of scope for #1086.
//   - **The "must specify true" coupling.** startFromHead publishes *"If you are using a previous
//     nextForwardToken value as the nextToken in this operation, you must specify true for
//     startFromHead."* Substrate takes the direction from the token itself, so a forward token resumes
//     forward whether or not startFromHead was sent, and the requirement is not enforced. No page
//     publishes a code for violating it, and inventing one would refuse a request AWS does not
//     document refusing (#671). startFromHead therefore decides only where a walk that presents **no**
//     token begins.
const (
	// cwLogsForwardTokenPrefix marks a token that resumes after the page it came with.
	cwLogsForwardTokenPrefix = "f/"

	// cwLogsBackwardTokenPrefix marks a token that resumes before the page it came with.
	cwLogsBackwardTokenPrefix = "b/"
)

// cwLogsEventCursor is the position and direction a GetLogEvents token names.
type cwLogsEventCursor struct {
	// Offset is the index into the events this request can see: the first event of the page for a
	// forward cursor, and one past the last event of the page for a backward one.
	Offset int

	// Backward reports that the cursor reads away from the head rather than toward the tail.
	Backward bool
}

// cwLogsEncodeEventToken renders the token that resumes a stream at the cursor.
func cwLogsEncodeEventToken(cursor cwLogsEventCursor) string {
	prefix := cwLogsForwardTokenPrefix
	if cursor.Backward {
		prefix = cwLogsBackwardTokenPrefix
	}
	return prefix + encodeOffsetPaginationToken(cursor.Offset)
}

// cwLogsDecodeEventToken decodes a GetLogEvents nextToken, reporting whether one was supplied and
// whether it is one substrate could have issued.
//
// An absent token is reported as not present rather than as offset zero, because the two are different
// requests: an absent token means "start where startFromHead says", and a forward token naming zero
// means "start at the head" whatever startFromHead says. Collapsing them would make the first page of a
// default (tail-first) walk depend on whether the caller spelled its first call with an explicit
// head token.
//
// Anything else must carry one of the two direction prefixes and, after it, exactly the text
// [encodeOffsetPaginationToken] would have produced — so every form [decodeOffsetPaginationToken]
// refuses is refused here too, and a bare offset token from one of the other three Logs paginators is
// refused for having no prefix.
func cwLogsDecodeEventToken(raw string) (cursor cwLogsEventCursor, present, ok bool) {
	if raw == "" {
		return cwLogsEventCursor{}, false, true
	}
	backward := false
	body := ""
	switch {
	case strings.HasPrefix(raw, cwLogsForwardTokenPrefix):
		body = strings.TrimPrefix(raw, cwLogsForwardTokenPrefix)
	case strings.HasPrefix(raw, cwLogsBackwardTokenPrefix):
		backward = true
		body = strings.TrimPrefix(raw, cwLogsBackwardTokenPrefix)
	default:
		return cwLogsEventCursor{}, true, false
	}
	// An empty body is refused rather than read as offset zero: the prefix alone is not a token
	// substrate issues, and the published minimum length of 1 is about the whole member.
	if body == "" {
		return cwLogsEventCursor{}, true, false
	}
	offset, offsetOK := decodeOffsetPaginationToken(body)
	if !offsetOK {
		return cwLogsEventCursor{}, true, false
	}
	return cwLogsEventCursor{Offset: offset, Backward: backward}, true, true
}

// cwLogsEventPageBounds returns the half-open range of events one GetLogEvents call reports.
//
// The bounds are clamped to the event count rather than refused past it, for the reason
// [decodeOffsetPaginationToken] gives: a token past the end is one substrate issued over a stream that
// has since been trimmed, and an empty final page is the honest answer where an error would break a
// legitimate walk.
//
// startFromHead decides only the first page of a walk that presents no token. Its published default is
// false — *"If the value is true, the earliest log events are returned first. If the value is false, the
// latest log events are returned first."* — so a caller that sends neither a token nor startFromHead
// reads the **tail** of the stream, and reads it backward from there. Substrate previously always
// started at the head, which is the default's opposite.
func cwLogsEventPageBounds(cursor cwLogsEventCursor, present, startFromHead bool, count, limit int) (start, end int) {
	clamp := func(n int) int {
		if n < 0 {
			return 0
		}
		if n > count {
			return count
		}
		return n
	}
	switch {
	case !present && startFromHead:
		start = 0
		end = clamp(limit)
	case !present:
		end = count
		start = clamp(count - limit)
	case cursor.Backward:
		end = clamp(cursor.Offset)
		start = clamp(end - limit)
	default:
		start = clamp(cursor.Offset)
		end = clamp(start + limit)
	}
	return start, end
}
