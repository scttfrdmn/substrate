package emulator

import "errors"

// PageItemForTest is one item of a cursor-paged listing in pagination_test.go: its key is its own
// two fields.
type PageItemForTest struct {
	// Sort and Tie are the item's [cursorKey].
	Sort, Tie string
}

// PageRequestForTest mirrors [pageRequest] for an external test package.
type PageRequestForTest struct {
	// Op, Filter, Token, Size and Descending are [pageRequest]'s fields of the same names.
	Op         string
	Filter     map[string][]string
	Token      string
	Size       int
	Descending bool
}

// PageForTest wraps [page] over items keyed by their own fields. Exported because the primitive's
// edges (ties, descending order, forged tokens) are reachable through the wire only one service at
// a time.
func PageForTest(items []PageItemForTest, req PageRequestForTest) (got []PageItemForTest, next string, truncated bool, err error) {
	res, err := page(items, func(it PageItemForTest) cursorKey { return cursorKey(it) },
		pageRequest(req))
	return res.Items, res.Next, res.Truncated, err
}

// PageValidateForTest wraps [pageRequest.validate].
func PageValidateForTest(req PageRequestForTest) error {
	return pageRequest(req).validate()
}

// IsPageTokenErrorForTest reports whether err wraps [errPageToken].
func IsPageTokenErrorForTest(err error) bool {
	return errors.Is(err, errPageToken)
}

// RestXMLNextMarkerForTest wraps [restXMLPage.nextMarker] for a page ending on the ID last.
func RestXMLNextMarkerForTest(truncated bool, last string) (string, error) {
	return restXMLPage{}.nextMarker(truncated, cursorKey{Sort: last})
}
