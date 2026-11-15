package emulator

import (
	"net/url"
)

// apigwV2RawQueryString builds the 2.0 proxy event's `rawQueryString`.
//
// The HTTP API developer guide's 2.0 payload example shows the member as the query string the
// client sent, `parameter1=value1&parameter1=value2&parameter2=value`, with a repeated key's values
// in the order sent. Substrate's parsed request keeps no raw query string — only Params, a map, and
// MultiValueParams for a key that repeats — so the order the client sent its keys in is not
// recoverable. Before #1201 the string was concatenated while ranging over that map, so it differed
// between two runs of one test, and values went in unencoded, so a value holding `&` or `=` changed
// the string's shape.
//
// It is now built by [url.Values.Encode]: keys in sorted order, each repeated key's values in the
// order sent, and every key and value percent-encoded (a space as `+`). The same request therefore
// always yields the same string, and the string always parses back to the parameters it was built
// from. What it does not reproduce is a client's own key order or its own choice of encoding.
func apigwV2RawQueryString(req *AWSRequest) string {
	if len(req.Params) == 0 {
		return ""
	}
	vals := make(url.Values, len(req.Params))
	for k, v := range req.Params {
		if mv, ok := req.MultiValueParams[k]; ok && len(mv) > 0 {
			vals[k] = append([]string(nil), mv...)
			continue
		}
		vals[k] = []string{v}
	}
	return vals.Encode()
}

// executeAPIRESTCostKey and executeAPIHTTPCostKey are the cost-table keys a stage request is charged
// under, by the type of API that served it. They name no operation — execute-api routes none — and
// are reached only through [chargeOnBehalf].
const (
	executeAPIRESTCostKey = "execute-api/RESTAPIRequest"
	executeAPIHTTPCostKey = "execute-api/HTTPAPIRequest"
)

// executeAPIHTTPMeteringBytes is the size of one metered HTTP API request. The API Gateway pricing
// page's HTTP API example divides each upload by 512 KB to count requests, so a larger body is
// several billable requests.
const executeAPIHTTPMeteringBytes = 512 * 1024

// executeAPICostRequest is the request a stage invocation is priced as.
func executeAPICostRequest(isV2 bool) *AWSRequest {
	op := "RESTAPIRequest"
	if isV2 {
		op = "HTTPAPIRequest"
	}
	return &AWSRequest{Service: "execute-api", Operation: op}
}

// executeAPICostUnits is how many billable requests one stage invocation is: one for a REST API, and
// one per started 512 KB of body for an HTTP API.
func executeAPICostUnits(isV2 bool, bodyLen int) int {
	if !isV2 || bodyLen <= executeAPIHTTPMeteringBytes {
		return 1
	}
	return (bodyLen + executeAPIHTTPMeteringBytes - 1) / executeAPIHTTPMeteringBytes
}
