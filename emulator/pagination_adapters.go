package emulator

import (
	"encoding/base64"
	"net/http"
	"strconv"
)

// Adapters from each protocol family's paging members to [pageRequest] (#1612).
//
// Each adapter reads one family's request members, refuses an out-of-range page size or an unissued
// cursor with the calling service's code, and does both before the caller reads any state, so a
// refused request is not distinguishable by what it would have listed:
//
//   - [jsonNextToken]: JSON `maxResults`/`nextToken` (Go's decoder matches `MaxResults` and
//     `NextToken` to the same struct tags), with the opaque token of pagination.go;
//   - [elbMarker]: ELB's `PageSize`/`Marker`, rendered back as `NextMarker`, with the opaque token;
//   - [iamMarker]: IAM's `MaxItems`/`Marker`, whose cursor is base64 of the last item's key;
//   - [restXMLMarker]: CloudFront's `Marker`/`MaxItems`, whose cursor is the raw ID of the last item.

// jsonPaging is one JSON-protocol list operation's paging contract.
type jsonPaging struct {
	// op is the operation name its tokens carry.
	op string
	// size is the published `maxResults` range.
	size pageSizeRange
	// invalidSize renders the service's refusal of an out-of-range `maxResults`.
	invalidSize func(message string) *AWSError
	// invalidToken renders the service's refusal of an unissued `nextToken`.
	invalidToken func() *AWSError
}

// jsonNextToken builds the page request of a JSON list operation from its decoded members.
func jsonNextToken(p jsonPaging, filter map[string][]string, nextToken string, maxResults *int) (pageRequest, *AWSError) {
	size, awsErr := p.size.check(maxResults, p.invalidSize)
	if awsErr != nil {
		return pageRequest{}, awsErr
	}
	req := pageRequest{Op: p.op, Filter: filter, Token: nextToken, Size: size}
	if req.validate() != nil {
		return pageRequest{}, p.invalidToken()
	}
	return req, nil
}

// elbMarker builds the page request of an ELB describe, of either generation, from `PageSize` and
// `Marker`.
//
// `PageSize` is [elbPageSize]. A `Marker` that is not a token this operation issued for these
// filter members is `ValidationError`/400: no ELB describe publishes a code for a bad marker, and
// the Query Common Errors page each links as its own publishes `ValidationError` ("The input fails to
// satisfy the constraints specified by an AWS service."). The message is substrate's own.
func elbMarker(req *AWSRequest, op string, filter map[string][]string) (pageRequest, *AWSError) {
	size, awsErr := elbPageSize(req.Params["PageSize"])
	if awsErr != nil {
		return pageRequest{}, awsErr
	}
	pr := pageRequest{Op: op, Filter: filter, Token: req.Params["Marker"], Size: size}
	if pr.validate() != nil {
		return pageRequest{}, elbValidationError("Marker '%s' is not a marker this service issued", pr.Token)
	}
	return pr, nil
}

// iamPage is one IAM listing's decoded `MaxItems` and `Marker`.
type iamPage struct {
	// after is the key the page starts strictly after, nil for the first page.
	after *cursorKey
	// size is the page size: the request's MaxItems, or the published default of 100.
	size int
}

// iamMarker validates an IAM listing's `MaxItems` ([iamValidateMaxItems]) and `Marker`.
//
// The marker IAM listings issue is base64 of the last item's key, and the next page starts strictly
// after that key, so a key deleted between pages still resumes in place. A marker that is not
// canonical base64 of a non-empty key is `ValidationError`/400, IAM CommonErrors' code for input that
// "doesn't meet the required format or constraints"; no IAM list operation publishes a code of its
// own for it. The message is substrate's own.
func iamMarker(req *AWSRequest, maxItems iamInt, marker string) (iamPage, *AWSResponse) {
	if errResp := iamValidateMaxItems(req, maxItems); errResp != nil {
		return iamPage{}, errResp
	}
	pg := iamPage{size: maxItems.Int()}
	if pg.size <= 0 {
		pg.size = iamMaxItemsDefault
	}
	if marker == "" {
		return pg, nil
	}
	key, ok := iamDecodeMarker(marker)
	if !ok {
		return iamPage{}, iamErrorResponse("ValidationError",
			"Marker '"+marker+"' is not a marker this service issued", http.StatusBadRequest)
	}
	pg.after = &cursorKey{Sort: key}
	return pg, nil
}

// iamEncodeMarker renders the Marker that resumes after key.
func iamEncodeMarker(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key))
}

// iamDecodeMarker returns the key a Marker names, reporting false for one [iamEncodeMarker] could
// not have produced.
func iamDecodeMarker(marker string) (string, bool) {
	raw, err := base64.StdEncoding.DecodeString(marker)
	if err != nil || len(raw) == 0 || iamEncodeMarker(string(raw)) != marker {
		return "", false
	}
	return string(raw), true
}

// restXMLPage is one CloudFront listing's decoded `Marker` and `MaxItems`.
type restXMLPage struct {
	// marker is the request's Marker, echoed in the response; the page starts strictly after it.
	marker string
	// maxItems is the page size, echoed in the response.
	maxItems int
}

// after returns the cursor the page starts strictly after, nil for the first page.
func (p restXMLPage) after() *cursorKey {
	if p.marker == "" {
		return nil
	}
	return &cursorKey{Sort: p.marker}
}

// restXMLMarker reads a CloudFront listing's `Marker` and `MaxItems` query parameters.
//
// The Marker is the raw ID of the previous page's last item, which CloudFront publishes rather than
// an opaque token ("the ID of the last distribution on that page", API_ListDistributions). Any
// Marker is accepted and the page starts after the first ID that sorts above it, so a Marker naming
// a deleted item resumes where that item was. A `MaxItems` that is not a positive integer is refused
// through invalid; an absent one is def.
func restXMLMarker(req *AWSRequest, def int, invalid func(message string) *AWSError) (restXMLPage, *AWSError) {
	pg := restXMLPage{marker: req.Params["Marker"], maxItems: def}
	raw, sent := req.Params["MaxItems"]
	if !sent {
		return pg, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return restXMLPage{}, invalid("MaxItems must be a positive integer, was '" + raw + "'")
	}
	pg.maxItems = n
	return pg, nil
}
