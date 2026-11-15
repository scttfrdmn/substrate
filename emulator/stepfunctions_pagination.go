package emulator

import (
	"encoding/base64"
	"net/http"
	"strings"
)

// The nextToken cursor ListStateMachines, ListExecutions and ListActivities issue (#1540).
//
// All three pages describe the token the same way: "The value of nextToken is a unique pagination
// token for each page. Make the call again using the returned token to retrieve the next page. Keep
// all other arguments unchanged. Each pagination token expires after 24 hours. Using an expired
// pagination token will return an HTTP 400 InvalidToken error." Each publishes InvalidToken/400, "The
// provided token is not valid."
//
// Until #1540 the token was the bare name of the next page's first item, resolved by scanning for it,
// so a token that named nothing left the offset at 0 and served page one again: the #915 / #1086
// shape, where a paging loop handed a stale or foreign token spins or processes items twice. The
// token is now the cursor query_marker_pagination.go uses — opaque base64, the last item the previous
// page returned — tagged with the listing it belongs to, so the shape is checked before any state is
// read and a token no call to that listing could have issued is InvalidToken.
//
// The cursor is a value rather than a position. State machines and activities are listed in name
// order (saveSMNames and saveActivityNames sort the index), so the next page is the names sorting
// strictly after the cursor, and deleting a state machine between two pages — including the one the
// cursor names — costs no other state machine its place. Executions are listed in start order, which
// is not a value order, so their cursor is the last execution's name resumed by position; an
// execution is never removed from its state machine's index, so a cursor naming one absent from it
// was not issued for that state machine and is refused. Their tag carries the state machine ARN,
// so a token from one state machine's listing is refused by another's ("Keep all other arguments
// unchanged").
//
// The 24-hour expiry is not modeled: a token substrate issued stays valid. Tokens from recordings made
// before #1540 are bare names, which do not decode, and are now refused.

// sfnTokenKind names the listing a token belongs to.
const (
	sfnTokenStateMachines = "statemachines"
	sfnTokenActivities    = "activities"
	sfnTokenExecutions    = "executions"
)

// sfnInvalidToken is the InvalidToken/400 all three pages publish, in their wording.
func sfnInvalidToken() *AWSError {
	return &AWSError{
		Code:       "InvalidToken",
		Message:    "The provided token is not valid.", // API_ListStateMachines, API_ListExecutions, API_ListActivities.
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnParseToken decodes a nextToken for the listing tagged tag, answering the item the previous page
// ended at, or "" for an absent token. A token that is not base64, or that another listing issued, is
// InvalidToken; it reads no state, so it can run before any.
func sfnParseToken(tag, raw string) (string, *AWSError) {
	if raw == "" {
		return "", nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", sfnInvalidToken()
	}
	after, ok := strings.CutPrefix(string(decoded), tag+"\x00")
	if !ok || after == "" {
		return "", sfnInvalidToken()
	}
	return after, nil
}

// sfnEncodeToken renders the token that resumes the listing tagged tag after item.
func sfnEncodeToken(tag, item string) string {
	return base64.StdEncoding.EncodeToString([]byte(tag + "\x00" + item))
}

// sfnPageSorted cuts one page of a name-sorted listing: the items sorting strictly after after, at
// most size of them, and the token for the next page when more remain. The item the cursor names
// need not still exist.
func sfnPageSorted(tag string, items []string, after string, size int) ([]string, string) {
	start := 0
	if after != "" {
		start = len(items)
		for i, item := range items {
			if item > after {
				start = i
				break
			}
		}
	}
	return sfnCut(tag, items[start:], size)
}

// sfnPageOrdered cuts one page of a listing in insertion order, resuming after the item after names.
// ok is false when after names no item in the listing, which a caller answers with InvalidToken.
func sfnPageOrdered(tag string, items []string, after string, size int) (page []string, next string, ok bool) {
	start := 0
	if after != "" {
		start = -1
		for i, item := range items {
			if item == after {
				start = i + 1
				break
			}
		}
		if start < 0 {
			return nil, "", false
		}
	}
	page, next = sfnCut(tag, items[start:], size)
	return page, next, true
}

// sfnCut takes the first size items of rest, with the token resuming after the last of them when
// any remain.
func sfnCut(tag string, rest []string, size int) ([]string, string) {
	if len(rest) > size {
		return rest[:size], sfnEncodeToken(tag, rest[size-1])
	}
	return rest, ""
}
