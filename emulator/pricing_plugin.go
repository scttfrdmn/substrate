package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// pricingNamespace is the service name used by PriceListPlugin. It is the SigV4
// signing name of the Price List Query API, so a request routed by credential
// scope lands here without an alias.
const pricingNamespace = "pricing"

// pricingCtrlNamespace is the state namespace holding seeded Price List
// failures. It is distinct from the pricingNamespace the plugin serves from so
// that clearing seeds can never touch the offer corpus.
const pricingCtrlNamespace = "pricing-ctrl"

// pricingEndpointRegions are the regions in which AWS hosts a Price List Query
// API endpoint. Every other region has no such endpoint at all — there is no
// api.pricing.eu-west-1.amazonaws.com to resolve.
//
// Substrate rejects a request signed for any other region with
// [pricingErrCodeInvalidRegion]. That is a deliberate divergence from AWS, which
// fails DNS resolution at the transport layer long before a request is sent:
// substrate serves every region from one endpoint and so cannot reproduce a name
// that does not resolve. Returning a legible API error is the closest available
// signal, and is far better than pricing a request substrate was never asked to
// price. See docs/services.md.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingEndpointRegions = map[string]bool{
	"us-east-1":    true,
	"ap-south-1":   true,
	"eu-central-1": true,
}

// Price List error codes. Every one below is from the AWS Price List Query API
// reference; all map to HTTP 400 except InternalErrorException, which is 500.
const (
	pricingErrAccessDenied     = "AccessDeniedException"
	pricingErrExpiredNextToken = "ExpiredNextTokenException"
	pricingErrInvalidNextToken = "InvalidNextTokenException"
	pricingErrInvalidParameter = "InvalidParameterException"
	pricingErrNotFound         = "NotFoundException"
	pricingErrThrottling       = "ThrottlingException"
	pricingErrInternalError    = "InternalErrorException"

	// pricingErrCodeInvalidRegion is substrate's own code for a request signed
	// for a region with no Price List endpoint. It is not an AWS code, and is
	// named so that it cannot be mistaken for one.
	pricingErrCodeInvalidRegion = "SubstrateInvalidPricingEndpoint"
)

// pricingSeedableErrorCodes is the set of error codes a Price List failure may
// be seeded with.
//
// The seed endpoint rejects anything else. A typo'd code would otherwise produce
// an error whose Code no consumer's catch branch matches, so their fallback path
// would go untested while the seed appeared to work — the same silent-green
// failure this plugin exists to make impossible.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingSeedableErrorCodes = map[string]bool{
	pricingErrAccessDenied:     true,
	pricingErrExpiredNextToken: true,
	pricingErrInvalidNextToken: true,
	pricingErrInvalidParameter: true,
	pricingErrNotFound:         true,
	pricingErrThrottling:       true,
	pricingErrInternalError:    true,
}

// pricingSeedableCodeList returns the sorted seedable error codes, for use in
// the seed endpoint's rejection message.
func pricingSeedableCodeList() []string {
	codes := make([]string, 0, len(pricingSeedableErrorCodes))
	for c := range pricingSeedableErrorCodes {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	return codes
}

// Documented MaxResults bounds. GetProducts and DescribeServices cap at 100;
// GetAttributeValues at 10000. The maximum doubles as the default so that an
// omitted MaxResults returns as much as one page can hold.
const (
	pricingMaxResultsProducts   = 100
	pricingMaxResultsServices   = 100
	pricingMaxResultsAttributes = 10000
)

// pricingMaxFilters is the documented cap on GetProducts Filters.
const pricingMaxFilters = 50

// pricingMaxFilterFieldLen is the documented length cap on a filter's Field and
// Value, and on FormatVersion's own cap of 32.
const (
	pricingMaxFilterFieldLen = 1024
	pricingMaxFormatVerLen   = 32
)

// Filter types the Price List API defines. TERM_MATCH is the only one most
// callers use, but a filter naming any other value is valid and must not be
// treated as an error.
const (
	pricingFilterTermMatch = "TERM_MATCH"
	pricingFilterEquals    = "EQUALS"
	pricingFilterContains  = "CONTAINS"
	pricingFilterAnyOf     = "ANY_OF"
	pricingFilterNoneOf    = "NONE_OF"
)

// pricingFilter is one GetProducts filter. All three members are required.
type pricingFilter struct {
	Type  string `json:"Type"`
	Field string `json:"Field"`
	Value string `json:"Value"`
}

// PriceListPlugin emulates the AWS Price List Query API — GetProducts,
// DescribeServices and GetAttributeValues — over a small offer corpus copied
// verbatim from the live AWS S3 offer file (see pricing_offer_fixture.go).
//
// It is the inverse of [AWSPricingProvider], which consumes the public offer
// index to cost substrate's own simulated usage. This plugin is the server side:
// it lets a consumer that queries pricing at runtime test that path without
// network access.
//
// Failures are seedable via POST /v1/pricing/query-failures, so a consumer can
// prove the property they actually care about — that pricing degrading does not
// take the I/O path down with it.
type PriceListPlugin struct {
	state  StateManager
	logger Logger
}

// Name returns the service name "pricing".
func (p *PriceListPlugin) Name() string { return pricingNamespace }

// Initialize configures the PriceListPlugin with the provided configuration.
func (p *PriceListPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	return nil
}

// Shutdown is a no-op for PriceListPlugin.
func (p *PriceListPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Price List JSON-target request to the appropriate
// handler. A seeded failure for the operation (or the "*" wildcard) preempts the
// handler, and a request signed for a region with no Price List endpoint is
// rejected before either.
func (p *PriceListPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if err := p.checkEndpointRegion(ctx); err != nil {
		return nil, err
	}
	if err := p.seededFailure(req.Operation); err != nil {
		return nil, err
	}
	switch req.Operation {
	case "GetProducts":
		return p.getProducts(req)
	case "DescribeServices":
		return p.describeServices(req)
	case "GetAttributeValues":
		return p.getAttributeValues(req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// checkEndpointRegion rejects a request signed for a region in which AWS hosts
// no Price List endpoint. See [pricingEndpointRegions] for why this diverges
// from AWS deliberately.
func (p *PriceListPlugin) checkEndpointRegion(ctx *RequestContext) error {
	if ctx == nil || ctx.Region == "" || pricingEndpointRegions[ctx.Region] {
		return nil
	}
	valid := make([]string, 0, len(pricingEndpointRegions))
	for r := range pricingEndpointRegions {
		valid = append(valid, r)
	}
	sort.Strings(valid)
	return &AWSError{
		Code: pricingErrCodeInvalidRegion,
		Message: fmt.Sprintf(
			"the Price List Query API has no endpoint in %s; sign for one of %s. "+
				"Real AWS fails to resolve api.pricing.%s.amazonaws.com; substrate "+
				"reports it as this error instead",
			ctx.Region, strings.Join(valid, ", "), ctx.Region),
		HTTPStatus: http.StatusBadRequest,
	}
}

// pricingSeededFailureKey is the state key holding the seeded failure for an
// operation, or for the "*" wildcard.
func pricingSeededFailureKey(operation string) string { return "failure:" + operation }

// pricingSeededFailure is the stored shape of a seeded failure.
type pricingSeededFailure struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	StatusCode int    `json:"statusCode"`
}

// seededFailure returns the seeded error for operation, preferring an
// operation-specific seed over the "*" wildcard, or nil when none is seeded.
func (p *PriceListPlugin) seededFailure(operation string) error {
	if p.state == nil {
		return nil
	}
	for _, key := range []string{pricingSeededFailureKey(operation), pricingSeededFailureKey("*")} {
		data, err := p.state.Get(context.Background(), pricingCtrlNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var seed pricingSeededFailure
		if json.Unmarshal(data, &seed) != nil {
			continue
		}
		status := seed.StatusCode
		if status == 0 {
			status = pricingErrorStatus(seed.Code)
		}
		return &AWSError{Code: seed.Code, Message: seed.Message, HTTPStatus: status}
	}
	return nil
}

// pricingErrorStatus maps a Price List error code to its HTTP status. Every
// documented code is 400 except InternalErrorException.
func pricingErrorStatus(code string) int {
	if code == pricingErrInternalError {
		return http.StatusInternalServerError
	}
	return http.StatusBadRequest
}

// pricingError builds an AWSError with the HTTP status the Price List API
// documents for code.
func pricingError(code, message string) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: pricingErrorStatus(code)}
}

// pricingJSONResponse marshals v as JSON with the Content-Type the Price List
// API uses.
func pricingJSONResponse(v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("pricing: marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// getProducts serves GetProducts. ServiceCode is required; Filters select over
// productFamily and the product's attributes.
func (p *PriceListPlugin) getProducts(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode   string          `json:"ServiceCode"`
		Filters       []pricingFilter `json:"Filters"`
		FormatVersion string          `json:"FormatVersion"`
		NextToken     string          `json:"NextToken"`
		MaxResults    *int            `json:"MaxResults"`
	}
	if err := pricingDecode(req.Body, &input); err != nil {
		return nil, err
	}
	if input.ServiceCode == "" {
		return nil, pricingError(pricingErrInvalidParameter, "ServiceCode is required")
	}
	if err := pricingValidateFormatVersion(input.FormatVersion); err != nil {
		return nil, err
	}
	if err := pricingValidateFilters(input.Filters); err != nil {
		return nil, err
	}
	limit, err := pricingLimit(input.MaxResults, pricingMaxResultsProducts)
	if err != nil {
		return nil, err
	}
	entries, err := pricingEntriesFor(input.ServiceCode)
	if err != nil {
		return nil, err
	}

	matched := make([]pricingCorpusEntry, 0, len(entries))
	for _, e := range entries {
		if e.matchesAll(input.Filters) {
			matched = append(matched, e)
		}
	}

	page, next, err := pricingPage(len(matched), input.NextToken, limit)
	if err != nil {
		return nil, err
	}

	// PriceList elements are JSON documents encoded as strings, not objects.
	// Marshaling each document and storing the result as a string is what
	// reproduces that; json.Marshal of the outer []string escapes it.
	priceList := make([]string, 0, page.end-page.start)
	for _, e := range matched[page.start:page.end] {
		doc, marshalErr := json.Marshal(e.offerDoc())
		if marshalErr != nil {
			return nil, fmt.Errorf("pricing: marshal offer document for %s: %w", e.sku, marshalErr)
		}
		priceList = append(priceList, string(doc))
	}

	out := map[string]interface{}{
		"FormatVersion": pricingFormatVersion,
		"PriceList":     priceList,
	}
	if next != "" {
		out["NextToken"] = next
	}
	return pricingJSONResponse(out)
}

// describeServices serves DescribeServices. Every field is optional; with no
// ServiceCode it lists every service in the corpus.
func (p *PriceListPlugin) describeServices(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode   string `json:"ServiceCode"`
		FormatVersion string `json:"FormatVersion"`
		NextToken     string `json:"NextToken"`
		MaxResults    *int   `json:"MaxResults"`
	}
	if err := pricingDecode(req.Body, &input); err != nil {
		return nil, err
	}
	if err := pricingValidateFormatVersion(input.FormatVersion); err != nil {
		return nil, err
	}
	limit, err := pricingLimit(input.MaxResults, pricingMaxResultsServices)
	if err != nil {
		return nil, err
	}

	codes := pricingServiceCodes()
	if input.ServiceCode != "" {
		if _, ok := pricingServiceAttributes[input.ServiceCode]; !ok {
			return nil, pricingError(pricingErrInvalidParameter,
				"unknown ServiceCode "+input.ServiceCode)
		}
		codes = []string{input.ServiceCode}
	}

	page, next, err := pricingPage(len(codes), input.NextToken, limit)
	if err != nil {
		return nil, err
	}

	services := make([]map[string]interface{}, 0, page.end-page.start)
	for _, code := range codes[page.start:page.end] {
		services = append(services, map[string]interface{}{
			"ServiceCode":    code,
			"AttributeNames": pricingServiceAttributes[code],
		})
	}

	out := map[string]interface{}{
		"FormatVersion": pricingFormatVersion,
		"Services":      services,
	}
	if next != "" {
		out["NextToken"] = next
	}
	return pricingJSONResponse(out)
}

// getAttributeValues serves GetAttributeValues. Both AttributeName and
// ServiceCode are required, and the values returned are exactly those the corpus
// carries — so a value from here always matches at least one product.
func (p *PriceListPlugin) getAttributeValues(req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServiceCode   string `json:"ServiceCode"`
		AttributeName string `json:"AttributeName"`
		NextToken     string `json:"NextToken"`
		MaxResults    *int   `json:"MaxResults"`
	}
	if err := pricingDecode(req.Body, &input); err != nil {
		return nil, err
	}
	if input.ServiceCode == "" {
		return nil, pricingError(pricingErrInvalidParameter, "ServiceCode is required")
	}
	if input.AttributeName == "" {
		return nil, pricingError(pricingErrInvalidParameter, "AttributeName is required")
	}
	limit, err := pricingLimit(input.MaxResults, pricingMaxResultsAttributes)
	if err != nil {
		return nil, err
	}
	entries, err := pricingEntriesFor(input.ServiceCode)
	if err != nil {
		return nil, err
	}
	if !pricingHasAttribute(input.ServiceCode, input.AttributeName) {
		return nil, pricingError(pricingErrInvalidParameter,
			"unknown AttributeName "+input.AttributeName+" for service "+input.ServiceCode)
	}

	seen := make(map[string]bool)
	var values []string
	for _, e := range entries {
		v, ok := e.attribute(input.AttributeName)
		// An attribute that is present but empty — "operation" is "" on every S3
		// product in the offer file — is reported as the empty value rather than
		// skipped, so that every value returned here matches at least one product
		// when fed back to a GetProducts filter. Absent is different from empty
		// and is skipped.
		if !ok || seen[v] {
			continue
		}
		seen[v] = true
		values = append(values, v)
	}
	sort.Strings(values)

	page, next, err := pricingPage(len(values), input.NextToken, limit)
	if err != nil {
		return nil, err
	}

	out := make([]map[string]string, 0, page.end-page.start)
	for _, v := range values[page.start:page.end] {
		out = append(out, map[string]string{"Value": v})
	}

	resp := map[string]interface{}{"AttributeValues": out}
	if next != "" {
		resp["NextToken"] = next
	}
	return pricingJSONResponse(resp)
}

// pricingDecode unmarshals a Price List request body. An empty body is treated
// as an empty object, since DescribeServices takes no required members.
func pricingDecode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, v); err != nil {
		return pricingError(pricingErrInvalidParameter, "malformed request body: "+err.Error())
	}
	return nil
}

// pricingValidateFormatVersion rejects a FormatVersion other than "aws_v1". An
// empty value means "unspecified" and is accepted.
func pricingValidateFormatVersion(v string) error {
	if v == "" || v == pricingFormatVersion {
		return nil
	}
	if len(v) > pricingMaxFormatVerLen {
		return pricingError(pricingErrInvalidParameter,
			fmt.Sprintf("FormatVersion must be at most %d characters", pricingMaxFormatVerLen))
	}
	return pricingError(pricingErrInvalidParameter,
		"unsupported FormatVersion "+v+"; the only valid value is "+pricingFormatVersion)
}

// pricingValidateFilters enforces the documented Filters constraints: at most 50
// filters, each with all three members set, Field and Value at most 1024
// characters, and Type one of the five documented values.
func pricingValidateFilters(filters []pricingFilter) error {
	if len(filters) > pricingMaxFilters {
		return pricingError(pricingErrInvalidParameter,
			fmt.Sprintf("at most %d filters are allowed, got %d", pricingMaxFilters, len(filters)))
	}
	for i, f := range filters {
		switch f.Type {
		case pricingFilterTermMatch, pricingFilterEquals, pricingFilterContains,
			pricingFilterAnyOf, pricingFilterNoneOf:
		case "":
			return pricingError(pricingErrInvalidParameter,
				fmt.Sprintf("Filters[%d].Type is required", i))
		default:
			return pricingError(pricingErrInvalidParameter,
				fmt.Sprintf("Filters[%d].Type %q is not a valid filter type", i, f.Type))
		}
		if f.Field == "" {
			return pricingError(pricingErrInvalidParameter,
				fmt.Sprintf("Filters[%d].Field is required", i))
		}
		if f.Value == "" {
			return pricingError(pricingErrInvalidParameter,
				fmt.Sprintf("Filters[%d].Value is required", i))
		}
		if len(f.Field) > pricingMaxFilterFieldLen || len(f.Value) > pricingMaxFilterFieldLen {
			return pricingError(pricingErrInvalidParameter,
				fmt.Sprintf("Filters[%d].Field and Value must be at most %d characters",
					i, pricingMaxFilterFieldLen))
		}
	}
	return nil
}

// pricingLimit resolves MaxResults against its documented bound. A nil pointer
// means the caller omitted it, which defaults to the maximum; an explicit value
// outside 1..max is an InvalidParameterException rather than a silent clamp,
// because clamping would hide a caller's mistake.
func pricingLimit(maxResults *int, max int) (int, error) {
	if maxResults == nil {
		return max, nil
	}
	if *maxResults < 1 || *maxResults > max {
		return 0, pricingError(pricingErrInvalidParameter,
			fmt.Sprintf("MaxResults must be between 1 and %d, got %d", max, *maxResults))
	}
	return *maxResults, nil
}

// pricingEntriesFor returns the corpus entries for a service code.
//
// An unknown service code is a NotFoundException. Substrate's corpus is far
// smaller than AWS's catalog, so a caller asking for a real service substrate
// does not carry gets a loud error rather than an empty PriceList that reads as
// "AWS has no such price". A false alarm is visible; a false empty is not.
func pricingEntriesFor(serviceCode string) ([]pricingCorpusEntry, error) {
	if _, ok := pricingServiceAttributes[serviceCode]; !ok {
		return nil, pricingError(pricingErrNotFound,
			"no offer data for ServiceCode "+serviceCode+
				"; substrate's corpus covers "+strings.Join(pricingServiceCodes(), ", "))
	}
	out := make([]pricingCorpusEntry, 0, len(pricingCorpus))
	for _, e := range pricingCorpus {
		if e.serviceCode == serviceCode {
			out = append(out, e)
		}
	}
	return out, nil
}

// pricingServiceCodes returns the sorted service codes the corpus covers.
func pricingServiceCodes() []string {
	codes := make([]string, 0, len(pricingServiceAttributes))
	for code := range pricingServiceAttributes {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// pricingHasAttribute reports whether serviceCode declares name as one of its
// attributes.
func pricingHasAttribute(serviceCode, name string) bool {
	for _, a := range pricingServiceAttributes[serviceCode] {
		if a == name {
			return true
		}
	}
	return false
}

// pricingProductFamilyField is the one filterable field that is a sibling of the
// attribute map rather than a member of it.
const pricingProductFamilyField = "productFamily"

// attribute returns the value of the named field, which may be the product's
// productFamily or any of its attributes. Matching is exact: the offer file's
// own names are what a caller must use, and accepting near-misses would let a
// filter that AWS rejects appear to work here.
func (e pricingCorpusEntry) attribute(field string) (string, bool) {
	if field == pricingProductFamilyField {
		// A product with no family reports absent, not empty — which is what
		// makes a productFamily filter miss most of the corpus, exactly as it
		// does against the real offer file.
		return e.productFamily, e.productFamily != ""
	}
	v, ok := e.attributes[field]
	return v, ok
}

// matchesAll reports whether the entry satisfies every filter. Filters are
// conjunctive, and a filter naming a field the product does not carry never
// matches — including NONE_OF, since AWS filters over products that have the
// field rather than over those that lack it.
func (e pricingCorpusEntry) matchesAll(filters []pricingFilter) bool {
	for _, f := range filters {
		v, ok := e.attribute(f.Field)
		if !ok {
			return false
		}
		if !pricingFilterMatches(f, v) {
			return false
		}
	}
	return true
}

// pricingFilterMatches applies one filter to one attribute value. ANY_OF and
// NONE_OF take a comma-separated Value.
func pricingFilterMatches(f pricingFilter, value string) bool {
	switch f.Type {
	case pricingFilterTermMatch, pricingFilterEquals:
		return value == f.Value
	case pricingFilterContains:
		return strings.Contains(value, f.Value)
	case pricingFilterAnyOf:
		return pricingInCommaList(f.Value, value)
	case pricingFilterNoneOf:
		return !pricingInCommaList(f.Value, value)
	default:
		// Unreachable: pricingValidateFilters rejects any other type.
		return false
	}
}

// pricingInCommaList reports whether value equals one of the comma-separated
// entries in list. Entries are trimmed, since AWS accepts "a, b" as two values.
func pricingInCommaList(list, value string) bool {
	for _, part := range strings.Split(list, ",") {
		if strings.TrimSpace(part) == value {
			return true
		}
	}
	return false
}

// pricingPageBounds is the half-open slice range one page covers.
type pricingPageBounds struct {
	start int
	end   int
}

// pricingPage resolves a NextToken and page size into slice bounds plus the
// token for the following page, which is "" on the last page.
//
// The token is an opaque base64 offset. A token that does not decode, or that
// names an offset past the end of the result set, is an InvalidNextTokenException
// — the error the API documents for exactly that case, and one a caller cannot
// otherwise reach.
func pricingPage(total int, token string, limit int) (pricingPageBounds, string, error) {
	start := 0
	if token != "" {
		off, err := pricingDecodeToken(token)
		if err != nil {
			return pricingPageBounds{}, "", err
		}
		if off > total {
			return pricingPageBounds{}, "", pricingError(pricingErrInvalidNextToken,
				"NextToken refers to an offset past the end of the result set")
		}
		start = off
	}
	end := start + limit
	if end > total {
		end = total
	}
	next := ""
	if end < total {
		next = pricingEncodeToken(end)
	}
	return pricingPageBounds{start: start, end: end}, next, nil
}

// pricingTokenPrefix marks a substrate pagination token. It is part of the
// encoded payload so that a token minted elsewhere fails to decode rather than
// being read as an offset.
const pricingTokenPrefix = "substrate-pricing:"

// pricingEncodeToken encodes a result offset as an opaque token.
func pricingEncodeToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(pricingTokenPrefix + strconv.Itoa(offset)))
}

// pricingDecodeToken recovers the offset from a token minted by
// pricingEncodeToken.
func pricingDecodeToken(token string) (int, error) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, pricingError(pricingErrInvalidNextToken, "NextToken is not a valid token")
	}
	s, ok := strings.CutPrefix(string(raw), pricingTokenPrefix)
	if !ok {
		return 0, pricingError(pricingErrInvalidNextToken, "NextToken is not a valid token")
	}
	offset, convErr := strconv.Atoi(s)
	if convErr != nil || offset < 0 {
		return 0, pricingError(pricingErrInvalidNextToken, "NextToken is not a valid token")
	}
	return offset, nil
}
