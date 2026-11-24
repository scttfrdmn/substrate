package emulator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
)

// Cursor pagination: one primitive for listings that change between pages (#1612).
//
// An offset cursor counts positions, so a record created or deleted ahead of the cursor between two
// pages shifts every later record by one place and a walk skips or repeats a record. A value cursor
// names the last record returned, and the next page is every record that sorts strictly after it, so
// a create or delete anywhere in the listing leaves the walk's surviving records each returned exactly
// once. A cursor naming a record that has since been deleted still resumes in the right place, because
// the comparison is against the value, not against a record that must still exist.
//
// [page] is that rule for any item type: the caller supplies each item's [cursorKey], the helper sorts
// by it, resumes strictly after the decoded cursor, and reports the listing truncated only when a
// further item exists, so the last page carries no token. [pageAfter] is the same cut without the
// token, for the operations whose wire cursor is a raw identifier rather than an opaque token
// (CloudFront's NextMarker, IAM's Marker).
//
// # The token
//
// The opaque token is base64 (standard alphabet, padded) of the JSON object {"o","h","s","t"}:
//
//   - o: the operation that issued it;
//   - h: the fnv-64a hash, as sixteen hex digits, of the request's canonical filter members;
//   - s and t: the [cursorKey] of the last item returned.
//
// A token is accepted only when it round-trips exactly: its base64 re-encodes to the same text, its
// JSON re-marshals to the same bytes, and nothing else is present. A token from another operation, or
// one reused with different filter members, is refused, because its position means nothing in the
// listing it was handed to. Every refusal wraps [errPageToken], which each service maps to the code
// its own page publishes; this file chooses no code.

// errPageToken is the error every refused pagination token wraps.
var errPageToken = errors.New("pagination token was not issued for this request")

// cursorKey is an item's position in a cursor-paged listing: items order by Sort, then by Tie.
//
// The pair must be unique within a listing; Tie breaks a Sort shared by several items. The zero key
// is a valid position: it sorts first, and a token naming it resumes at the first non-zero key.
type cursorKey struct {
	// Sort is the primary ordering value.
	Sort string
	// Tie orders items whose Sort is equal.
	Tie string
}

// compare orders two keys, Sort first.
func (k cursorKey) compare(o cursorKey) int {
	if c := strings.Compare(k.Sort, o.Sort); c != 0 {
		return c
	}
	return strings.Compare(k.Tie, o.Tie)
}

// pageRequest is one cursor-paged listing request.
type pageRequest struct {
	// Op is the operation name; a token carries it and is refused by any other operation.
	Op string
	// Filter is the request's members that select the listing, by wire name. A token carries their
	// hash and is refused when it does not match. Values are compared as a set, and an empty value or
	// list is the same as an absent member.
	Filter map[string][]string
	// Token is the token the request carried; empty is the first page.
	Token string
	// Size is the page size; zero or less returns the whole remainder.
	Size int
	// Descending reverses the order: the next page is the items that sort strictly before the cursor.
	Descending bool
}

// pageResult is one page of a cursor-paged listing.
type pageResult[T any] struct {
	// Items is the page, in listing order.
	Items []T
	// Truncated reports whether a further item exists after the page.
	Truncated bool
	// Next is the token that resumes after the page, empty unless Truncated.
	Next string
	// Last is the key of the page's last item, the zero key when the page is empty.
	Last cursorKey
}

// pageToken is the decoded form of an opaque token. The field order is the wire order.
type pageToken struct {
	O string `json:"o"`
	H string `json:"h"`
	S string `json:"s"`
	T string `json:"t"`
}

// validate reports whether the request's token is one this request could have been issued, without
// reading any items. Callers run it before reading state, so a refused request reads nothing.
func (r pageRequest) validate() error {
	_, err := r.cursor()
	return err
}

// cursor decodes the request's token, returning nil for the first page.
func (r pageRequest) cursor() (*cursorKey, error) {
	if r.Token == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(r.Token)
	if err != nil || base64.StdEncoding.EncodeToString(raw) != r.Token {
		return nil, fmt.Errorf("%w: not base64", errPageToken)
	}
	var tok pageToken
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&tok); err != nil {
		return nil, fmt.Errorf("%w: not a token", errPageToken)
	}
	canonical, err := json.Marshal(tok)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, fmt.Errorf("%w: not a canonical token", errPageToken)
	}
	if tok.O != r.Op {
		return nil, fmt.Errorf("%w: issued by another operation", errPageToken)
	}
	if tok.H != pageFilterHash(r.Filter) {
		return nil, fmt.Errorf("%w: issued for different filter members", errPageToken)
	}
	return &cursorKey{Sort: tok.S, Tie: tok.T}, nil
}

// encode renders the token that resumes this request's listing after last.
func (r pageRequest) encode(last cursorKey) string {
	b, err := json.Marshal(pageToken{O: r.Op, H: pageFilterHash(r.Filter), S: last.Sort, T: last.Tie})
	if err != nil {
		// A struct of four strings always marshals.
		panic(fmt.Sprintf("pagination: marshal token: %v", err))
	}
	return base64.StdEncoding.EncodeToString(b)
}

// pageFilterHash is the fnv-64a hash of filter in canonical form: members in name order, each
// member's values sorted and deduplicated (a set), every string length-prefixed so that no two
// filters share an encoding. An empty value is dropped, and a member left with no values is absent.
func pageFilterHash(filter map[string][]string) string {
	present := make(map[string][]string, len(filter))
	names := make([]string, 0, len(filter))
	for name, values := range filter {
		values = slices.DeleteFunc(slices.Clone(values), func(v string) bool { return v == "" })
		if len(values) > 0 {
			slices.Sort(values)
			present[name] = slices.Compact(values)
			names = append(names, name)
		}
	}
	slices.Sort(names)
	var buf []byte
	write := func(s string) {
		buf = strconv.AppendInt(buf, int64(len(s)), 10)
		buf = append(buf, ':')
		buf = append(buf, s...)
	}
	for _, name := range names {
		values := present[name]
		write(name)
		buf = strconv.AppendInt(buf, int64(len(values)), 10)
		buf = append(buf, ';')
		for _, v := range values {
			write(v)
		}
	}
	h := fnv.New64a()
	// Errors are impossible: hash.Hash.Write never returns one.
	_, _ = h.Write(buf)
	return fmt.Sprintf("%016x", h.Sum64())
}

// page cuts one page out of items for req: items sort by key, the page starts strictly after the
// token's cursor, and Next resumes after the page when a further item exists. items is not modified.
//
// The only error is a refused token, which wraps [errPageToken].
func page[T any](items []T, key func(T) cursorKey, req pageRequest) (pageResult[T], error) {
	after, err := req.cursor()
	if err != nil {
		return pageResult[T]{}, err
	}
	res := pageAfter(items, key, after, req.Size, req.Descending)
	if res.Truncated {
		res.Next = req.encode(res.Last)
	}
	return res, nil
}

// pageAfter is [page]'s cut with the cursor already decoded: after is nil for the first page. It
// sets no Next, which is the caller's to render from Last.
func pageAfter[T any](items []T, key func(T) cursorKey, after *cursorKey, size int, descending bool) pageResult[T] {
	order := func(a, b T) int {
		if descending {
			return key(b).compare(key(a))
		}
		return key(a).compare(key(b))
	}
	sorted := slices.Clone(items)
	slices.SortStableFunc(sorted, order)

	start := 0
	if after != nil {
		start = len(sorted)
		for i, item := range sorted {
			c := key(item).compare(*after)
			if (!descending && c > 0) || (descending && c < 0) {
				start = i
				break
			}
		}
	}
	rest := sorted[start:]
	res := pageResult[T]{Items: rest}
	if size > 0 && len(rest) > size {
		res.Items = rest[:size]
		res.Truncated = true
	}
	if len(res.Items) > 0 {
		res.Last = key(res.Items[len(res.Items)-1])
	}
	return res
}
