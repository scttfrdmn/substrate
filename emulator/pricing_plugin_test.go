package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Price List Query API is how a consumer that prices its own usage at
// runtime discovers rates. #401 was filed because substrate did not emulate it at
// all, so such a consumer could only be tested against a hand-maintained static
// table — and the reporter's table was already 10x wrong on Requests-Tier1 and
// 21x wrong on Deep Archive. These tests assert the shapes that made those errors
// possible, not just that a request succeeds.

// newPricingTestServer builds a server with PriceListPlugin registered.
func newPricingTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.PriceListPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
	}); err != nil {
		t.Fatalf("initialize pricing plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// pricingCall issues a Price List request against host
// api.pricing.<region>.amazonaws.com — the real endpoint layout, so these tests
// also exercise the routing added for #401/#403 rather than a shortcut path.
func pricingCall(t *testing.T, ts *httptest.Server, region, op string, in any) *http.Response {
	t.Helper()
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal %s input: %v", op, err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build %s request: %v", op, err)
	}
	req.Host = "api.pricing." + region + ".amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSPriceListService."+op)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s request: %v", op, err)
	}
	return resp
}

// pricingJSON reads a response body into a generic map.
func pricingJSON(t *testing.T, r *http.Response) map[string]any {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read pricing body: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal pricing body %q: %v", raw, err)
	}
	return out
}

// pricingGetProducts is the common happy-path call: GetProducts for AmazonS3
// with the given filters, asserting 200.
func pricingGetProducts(t *testing.T, ts *httptest.Server, filters []map[string]string) map[string]any {
	t.Helper()
	in := map[string]any{"ServiceCode": "AmazonS3", "FormatVersion": "aws_v1"}
	if filters != nil {
		in["Filters"] = filters
	}
	resp := pricingCall(t, ts, "us-east-1", "GetProducts", in)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetProducts status = %d, want 200 (body %s)", resp.StatusCode,
			mustReadAll(t, resp))
	}
	return pricingJSON(t, resp)
}

func mustReadAll(t *testing.T, r *http.Response) string {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// priceListDocs decodes the PriceList field, asserting each element is a JSON
// document encoded as a *string* rather than an object. That encoding is the
// single most common thing callers get wrong, so it is asserted structurally
// here rather than assumed.
func priceListDocs(t *testing.T, out map[string]any) []map[string]any {
	t.Helper()
	raw, ok := out["PriceList"]
	if !ok {
		t.Fatalf("response has no PriceList: %v", out)
	}
	list, ok := raw.([]any)
	if !ok {
		t.Fatalf("PriceList is %T, want []any", raw)
	}
	docs := make([]map[string]any, 0, len(list))
	for i, el := range list {
		s, isStr := el.(string)
		if !isStr {
			t.Fatalf("PriceList[%d] is %T, want string: AWS encodes each offer "+
				"document as a JSON string, not an object", i, el)
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(s), &doc); err != nil {
			t.Fatalf("PriceList[%d] is not valid JSON: %v", i, err)
		}
		docs = append(docs, doc)
	}
	return docs
}

// docBySKU indexes decoded offer documents by SKU.
func docBySKU(t *testing.T, docs []map[string]any) map[string]map[string]any {
	t.Helper()
	byS := make(map[string]map[string]any, len(docs))
	for _, d := range docs {
		prod, ok := d["product"].(map[string]any)
		if !ok {
			t.Fatalf("offer document has no product object: %v", d)
		}
		sku, ok := prod["sku"].(string)
		if !ok {
			t.Fatalf("product has no sku: %v", prod)
		}
		byS[sku] = d
	}
	return byS
}

// dimensions returns the OnDemand priceDimensions of an offer document.
func dimensions(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	terms, ok := doc["terms"].(map[string]any)
	if !ok {
		t.Fatalf("offer document has no terms: %v", doc)
	}
	onDemand, ok := terms["OnDemand"].(map[string]any)
	if !ok {
		t.Fatalf("terms has no OnDemand: %v", terms)
	}
	for _, term := range onDemand {
		tm, isMap := term.(map[string]any)
		if !isMap {
			continue
		}
		dims, isDims := tm["priceDimensions"].(map[string]any)
		if isDims {
			return dims
		}
	}
	t.Fatalf("OnDemand term has no priceDimensions: %v", onDemand)
	return nil
}

func TestPricingGetProducts_ReturnsWholeCorpus(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	out := pricingGetProducts(t, ts, nil)
	if got := out["FormatVersion"]; got != "aws_v1" {
		t.Errorf("FormatVersion = %v, want aws_v1", got)
	}
	docs := priceListDocs(t, out)
	if len(docs) != 7 {
		t.Fatalf("got %d products, want the whole 7-SKU corpus", len(docs))
	}
	// A single page covers the corpus, so no NextToken should be offered.
	if _, ok := out["NextToken"]; ok {
		t.Errorf("unpaginated response carries a NextToken: %v", out["NextToken"])
	}
}

// TestPricingGetProducts_PriceStringsAreStrings pins the encoding that makes a
// naive float unmarshal fail: pricePerUnit values are strings with trailing
// zeros, and so is every range bound.
func TestPricingGetProducts_PriceStringsAreStrings(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := priceListDocs(t, pricingGetProducts(t, ts, nil))
	for _, doc := range docs {
		for rateCode, raw := range dimensions(t, doc) {
			dim, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("%s: dimension is %T, want object", rateCode, raw)
			}
			ppu, ok := dim["pricePerUnit"].(map[string]any)
			if !ok {
				t.Fatalf("%s: pricePerUnit is %T, want object", rateCode, dim["pricePerUnit"])
			}
			usd, ok := ppu["USD"]
			if !ok {
				t.Fatalf("%s: pricePerUnit has no USD", rateCode)
			}
			if _, isStr := usd.(string); !isStr {
				t.Errorf("%s: pricePerUnit.USD is %T, want string — AWS never "+
					"emits a bare number here", rateCode, usd)
			}
			for _, field := range []string{"beginRange", "endRange"} {
				if _, isStr := dim[field].(string); !isStr {
					t.Errorf("%s: %s is %T, want string", rateCode, field, dim[field])
				}
			}
		}
	}
}

// TestPricingGetProducts_RequestsTier1Rate is the rate the reporter's static
// table had 10x low. $0.005 per 1,000 requests is 0.000005 per request, and the
// unit is "Requests" — so a caller must not divide by 1000 again.
func TestPricingGetProducts_RequestsTier1Rate(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	out := pricingGetProducts(t, ts, []map[string]string{
		{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "Requests-Tier1"},
	})
	docs := priceListDocs(t, out)
	if len(docs) != 1 {
		t.Fatalf("got %d products for usagetype=Requests-Tier1, want 1", len(docs))
	}
	dims := dimensions(t, docs[0])
	if len(dims) != 1 {
		t.Fatalf("got %d price dimensions, want 1", len(dims))
	}
	for _, raw := range dims {
		dim, _ := raw.(map[string]any)
		ppu, _ := dim["pricePerUnit"].(map[string]any)
		if got := ppu["USD"]; got != "0.0000050000" {
			t.Errorf("Requests-Tier1 USD = %v, want 0.0000050000 ($0.005 per 1,000)", got)
		}
		if got := dim["unit"]; got != "Requests" {
			t.Errorf("unit = %v, want Requests (per-request, not per-thousand)", got)
		}
	}
}

// TestPricingGetProducts_TieredStorageHasEveryDimension guards the trap where a
// parser takes the first priceDimension and reports the 50 TB tier rate as the
// only rate. All three tiers must be present, and the last endRange must be the
// literal string "Inf".
func TestPricingGetProducts_TieredStorageHasEveryDimension(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	out := pricingGetProducts(t, ts, []map[string]string{
		{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "TimedStorage-ByteHrs"},
	})
	docs := priceListDocs(t, out)
	if len(docs) != 1 {
		t.Fatalf("got %d products for TimedStorage-ByteHrs, want 1", len(docs))
	}
	dims := dimensions(t, docs[0])
	if len(dims) != 3 {
		t.Fatalf("got %d price dimensions, want 3 (50 TB / 450 TB / beyond)", len(dims))
	}

	// Index by beginRange so the assertion does not depend on map ordering.
	byBegin := make(map[string]map[string]any, len(dims))
	for _, raw := range dims {
		dim, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("dimension is %T, want object", raw)
		}
		begin, _ := dim["beginRange"].(string)
		byBegin[begin] = dim
	}

	for _, want := range []struct {
		begin, end, usd string
	}{
		{"0", "51200", "0.0230000000"},
		{"51200", "512000", "0.0220000000"},
		{"512000", "Inf", "0.0210000000"},
	} {
		dim, ok := byBegin[want.begin]
		if !ok {
			t.Errorf("no dimension with beginRange %s", want.begin)
			continue
		}
		if got := dim["endRange"]; got != want.end {
			t.Errorf("beginRange %s: endRange = %v, want %s", want.begin, got, want.end)
		}
		ppu, _ := dim["pricePerUnit"].(map[string]any)
		if got := ppu["USD"]; got != want.usd {
			t.Errorf("beginRange %s: USD = %v, want %s", want.begin, got, want.usd)
		}
	}
}

// TestPricingGetProducts_DeepArchiveFilterReturnsOnlyStaging encodes the trap
// verified against the live offer file: filtering productFamily=Storage with
// volumeType="Glacier Deep Archive" returns *only* the staging SKU at $0.021 —
// 21x the $0.00099 archive rate a caller is looking for. There is no
// TimedStorage-GDA-ByteHrs SKU in the S3 offer file to return instead.
func TestPricingGetProducts_DeepArchiveFilterReturnsOnlyStaging(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	out := pricingGetProducts(t, ts, []map[string]string{
		{"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"},
		{"Type": "TERM_MATCH", "Field": "volumeType", "Value": "Glacier Deep Archive"},
	})
	docs := priceListDocs(t, out)
	if len(docs) != 1 {
		t.Fatalf("got %d products, want exactly 1 (the staging decoy)", len(docs))
	}
	bySKU := docBySKU(t, docs)
	doc, ok := bySKU["EXB3YJ6YV5CRH4JN"]
	if !ok {
		t.Fatalf("want SKU EXB3YJ6YV5CRH4JN (TimedStorage-GDA-Staging), got %v", bySKU)
	}
	for _, raw := range dimensions(t, doc) {
		dim, _ := raw.(map[string]any)
		ppu, _ := dim["pricePerUnit"].(map[string]any)
		if got := ppu["USD"]; got != "0.0210000000" {
			t.Errorf("GDA-Staging USD = %v, want 0.0210000000 — the staging rate, "+
				"not the archive rate", got)
		}
	}

	// The $0.00099 rate exists, but under Intelligent-Tiering, so a caller cannot
	// reach it by filtering for Deep Archive.
	daa := priceListDocs(t, pricingGetProducts(t, ts, []map[string]string{
		{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "TimedStorage-INT-DAA-ByteHrs"},
	}))
	if len(daa) != 1 {
		t.Fatalf("got %d products for TimedStorage-INT-DAA-ByteHrs, want 1", len(daa))
	}
	for _, raw := range dimensions(t, daa[0]) {
		dim, _ := raw.(map[string]any)
		ppu, _ := dim["pricePerUnit"].(map[string]any)
		if got := ppu["USD"]; got != "0.0009900000" {
			t.Errorf("INT-DAA USD = %v, want 0.0009900000", got)
		}
	}
}

// TestPricingGetProducts_ProductFamilyOftenAbsent pins the majority case: most
// products carry no productFamily at all, so filtering on it misses them, while
// usagetype reaches every one.
func TestPricingGetProducts_ProductFamilyOftenAbsent(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := priceListDocs(t, pricingGetProducts(t, ts, nil))
	withFamily := 0
	for _, doc := range docs {
		prod, _ := doc["product"].(map[string]any)
		if _, ok := prod["productFamily"]; ok {
			withFamily++
		}
		if _, ok := prod["attributes"].(map[string]any)["usagetype"]; !ok {
			sku := prod["sku"]
			t.Errorf("product %v has no usagetype; usagetype must be present on "+
				"every product since it is the only reliable key", sku)
		}
	}
	if withFamily == len(docs) {
		t.Error("every product carries a productFamily; the corpus must include " +
			"family-less products, which are the majority in the real offer file")
	}

	// The concrete consequence: a productFamily filter cannot see the family-less
	// SKU, but a usagetype filter can.
	byFamily := priceListDocs(t, pricingGetProducts(t, ts, []map[string]string{
		{"Type": "TERM_MATCH", "Field": "productFamily", "Value": "API Request"},
	}))
	byUsage := priceListDocs(t, pricingGetProducts(t, ts, []map[string]string{
		{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "Requests-GDA-Tier2"},
	}))
	if len(byUsage) != 1 {
		t.Fatalf("usagetype=Requests-GDA-Tier2 returned %d products, want 1", len(byUsage))
	}
	for _, doc := range byFamily {
		prod, _ := doc["product"].(map[string]any)
		if prod["sku"] == "WDAC2WRXVNRSNQS6" {
			t.Error("the family-less SKU was returned by a productFamily filter; " +
				"an absent productFamily must not match")
		}
	}
}

func TestPricingGetProducts_FilterTypes(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	tests := []struct {
		name    string
		filters []map[string]string
		wantN   int
	}{
		{
			name: "EQUALS behaves as TERM_MATCH",
			filters: []map[string]string{
				{"Type": "EQUALS", "Field": "usagetype", "Value": "Requests-Tier1"},
			},
			wantN: 1,
		},
		{
			name: "CONTAINS matches a substring",
			filters: []map[string]string{
				{"Type": "CONTAINS", "Field": "usagetype", "Value": "TimedStorage"},
			},
			wantN: 4,
		},
		{
			name: "ANY_OF takes a comma-separated list",
			filters: []map[string]string{
				{"Type": "ANY_OF", "Field": "usagetype", "Value": "Requests-Tier1,Requests-Tier2"},
			},
			wantN: 2,
		},
		{
			name: "NONE_OF excludes listed values",
			filters: []map[string]string{
				{"Type": "NONE_OF", "Field": "storageClass", "Value": "General Purpose,Archive"},
			},
			// Only the four products carrying storageClass are candidates at all;
			// NONE_OF filters within them, it does not select products lacking the
			// field.
			wantN: 2,
		},
		{
			name: "conjunctive filters narrow",
			filters: []map[string]string{
				{"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"},
				{"Type": "TERM_MATCH", "Field": "storageClass", "Value": "Infrequent Access"},
			},
			wantN: 1,
		},
		{
			name: "unknown field matches nothing",
			filters: []map[string]string{
				{"Type": "TERM_MATCH", "Field": "instanceType", "Value": "m5.large"},
			},
			wantN: 0,
		},
		{
			name: "unmatched value matches nothing",
			filters: []map[string]string{
				{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "NoSuchUsageType"},
			},
			wantN: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			docs := priceListDocs(t, pricingGetProducts(t, ts, tt.filters))
			if len(docs) != tt.wantN {
				skus := make([]string, 0, len(docs))
				for s := range docBySKU(t, docs) {
					skus = append(skus, s)
				}
				t.Errorf("got %d products, want %d (SKUs %v)", len(docs), tt.wantN, skus)
			}
		})
	}
}

// TestPricingGetProducts_Pagination walks the corpus a page at a time and
// asserts the pages partition it exactly — no duplicates, no gaps, and no
// NextToken on the final page.
func TestPricingGetProducts_Pagination(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	seen := make(map[string]int)
	token := ""
	pages := 0
	for {
		in := map[string]any{"ServiceCode": "AmazonS3", "MaxResults": 3}
		if token != "" {
			in["NextToken"] = token
		}
		resp := pricingCall(t, ts, "us-east-1", "GetProducts", in)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d status = %d, want 200 (body %s)", pages,
				resp.StatusCode, mustReadAll(t, resp))
		}
		out := pricingJSON(t, resp)
		for sku := range docBySKU(t, priceListDocs(t, out)) {
			seen[sku]++
		}
		pages++
		next, ok := out["NextToken"].(string)
		if !ok || next == "" {
			break
		}
		token = next
		if pages > 10 {
			t.Fatal("pagination did not terminate within 10 pages")
		}
	}
	if pages != 3 {
		t.Errorf("walked %d pages of 3 over a 7-SKU corpus, want 3", pages)
	}
	if len(seen) != 7 {
		t.Errorf("saw %d distinct SKUs across all pages, want 7", len(seen))
	}
	for sku, n := range seen {
		if n != 1 {
			t.Errorf("SKU %s appeared on %d pages, want 1", sku, n)
		}
	}
}

func TestPricingGetProducts_InvalidNextToken(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	tests := []struct {
		name  string
		token string
	}{
		{"not base64", "!!!not-base64!!!"},
		{"base64 but not a substrate token", "aGVsbG8gd29ybGQ="},
		{"offset past the end", "c3Vic3RyYXRlLXByaWNpbmc6OTk5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
				"ServiceCode": "AmazonS3",
				"NextToken":   tt.token,
			})
			assertPricingError(t, resp, http.StatusBadRequest, "InvalidNextTokenException")
		})
	}
}

// assertPricingError checks an error response's status and Code. The Code is
// what an SDK matches on to pick an except/catch branch, so a wrong code makes
// the branch unreachable even when the status is right.
func assertPricingError(t *testing.T, resp *http.Response, wantStatus int, wantCode string) {
	t.Helper()
	body := mustReadAll(t, resp)
	if resp.StatusCode != wantStatus {
		t.Errorf("status = %d, want %d (body %s)", resp.StatusCode, wantStatus, body)
	}
	var out struct {
		Code    string `json:"Code"`
		Message string `json:"Message"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("error body is not JSON: %v (body %s)", err, body)
	}
	if out.Code != wantCode {
		t.Errorf("Code = %q, want %q (message %q)", out.Code, wantCode, out.Message)
	}
	if out.Message == "" {
		t.Error("error carries an empty Message")
	}
}

func TestPricingGetProducts_ValidationErrors(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	manyFilters := make([]map[string]string, 51)
	for i := range manyFilters {
		manyFilters[i] = map[string]string{
			"Type": "TERM_MATCH", "Field": "usagetype", "Value": "Requests-Tier1",
		}
	}

	tests := []struct {
		name  string
		input map[string]any
		code  string
	}{
		{
			name:  "ServiceCode required",
			input: map[string]any{},
			code:  "InvalidParameterException",
		},
		{
			name:  "unknown ServiceCode is NotFound",
			input: map[string]any{"ServiceCode": "AmazonNoSuchService"},
			code:  "NotFoundException",
		},
		{
			name:  "FormatVersion must be aws_v1",
			input: map[string]any{"ServiceCode": "AmazonS3", "FormatVersion": "aws_v2"},
			code:  "InvalidParameterException",
		},
		{
			name:  "MaxResults below 1",
			input: map[string]any{"ServiceCode": "AmazonS3", "MaxResults": 0},
			code:  "InvalidParameterException",
		},
		{
			name:  "MaxResults above 100",
			input: map[string]any{"ServiceCode": "AmazonS3", "MaxResults": 101},
			code:  "InvalidParameterException",
		},
		{
			name:  "more than 50 filters",
			input: map[string]any{"ServiceCode": "AmazonS3", "Filters": manyFilters},
			code:  "InvalidParameterException",
		},
		{
			name: "filter Type is required",
			input: map[string]any{"ServiceCode": "AmazonS3", "Filters": []map[string]string{
				{"Field": "usagetype", "Value": "Requests-Tier1"},
			}},
			code: "InvalidParameterException",
		},
		{
			name: "filter Type must be a known value",
			input: map[string]any{"ServiceCode": "AmazonS3", "Filters": []map[string]string{
				{"Type": "REGEX_MATCH", "Field": "usagetype", "Value": "Requests.*"},
			}},
			code: "InvalidParameterException",
		},
		{
			name: "filter Field is required",
			input: map[string]any{"ServiceCode": "AmazonS3", "Filters": []map[string]string{
				{"Type": "TERM_MATCH", "Value": "Requests-Tier1"},
			}},
			code: "InvalidParameterException",
		},
		{
			name: "filter Value is required",
			input: map[string]any{"ServiceCode": "AmazonS3", "Filters": []map[string]string{
				{"Type": "TERM_MATCH", "Field": "usagetype"},
			}},
			code: "InvalidParameterException",
		},
		{
			name: "filter Field over 1024 characters",
			input: map[string]any{"ServiceCode": "AmazonS3", "Filters": []map[string]string{
				{"Type": "TERM_MATCH", "Field": strings.Repeat("a", 1025), "Value": "x"},
			}},
			code: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := pricingCall(t, ts, "us-east-1", "GetProducts", tt.input)
			assertPricingError(t, resp, http.StatusBadRequest, tt.code)
		})
	}
}

// TestPricingGetProducts_MalformedBody: a body that is not JSON must be an
// InvalidParameterException, not a 500.
func TestPricingGetProducts_MalformedBody(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "api.pricing.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSPriceListService.GetProducts")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	assertPricingError(t, resp, http.StatusBadRequest, "InvalidParameterException")
}

func TestPricingDescribeServices(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "DescribeServices", map[string]any{})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	out := pricingJSON(t, resp)
	if got := out["FormatVersion"]; got != "aws_v1" {
		t.Errorf("FormatVersion = %v, want aws_v1", got)
	}
	services, ok := out["Services"].([]any)
	if !ok || len(services) == 0 {
		t.Fatalf("Services = %v, want a non-empty list", out["Services"])
	}
	svc, ok := services[0].(map[string]any)
	if !ok {
		t.Fatalf("Services[0] is %T, want object", services[0])
	}
	if got := svc["ServiceCode"]; got != "AmazonS3" {
		t.Errorf("ServiceCode = %v, want AmazonS3", got)
	}
	names, ok := svc["AttributeNames"].([]any)
	if !ok || len(names) == 0 {
		t.Fatalf("AttributeNames = %v, want a non-empty list", svc["AttributeNames"])
	}
	// Every attribute DescribeServices advertises must be usable as a filter
	// field, otherwise a caller following the documented discovery path
	// (DescribeServices → GetAttributeValues → GetProducts) hits a dead end.
	for _, raw := range names {
		name, isStr := raw.(string)
		if !isStr {
			t.Fatalf("AttributeNames element is %T, want string", raw)
		}
		vals := pricingCall(t, ts, "us-east-1", "GetAttributeValues", map[string]any{
			"ServiceCode": "AmazonS3", "AttributeName": name,
		})
		if vals.StatusCode != http.StatusOK {
			t.Errorf("GetAttributeValues(%s) status = %d, want 200 (body %s)",
				name, vals.StatusCode, mustReadAll(t, vals))
			continue
		}
		_ = mustReadAll(t, vals)
	}
}

// TestPricingDescribeServices_SingleServiceCode: naming a ServiceCode narrows the
// list to that one service rather than being ignored.
func TestPricingDescribeServices_SingleServiceCode(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "DescribeServices", map[string]any{
		"ServiceCode": "AmazonS3",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	out := pricingJSON(t, resp)
	services, ok := out["Services"].([]any)
	if !ok || len(services) != 1 {
		t.Fatalf("Services = %v, want exactly one entry", out["Services"])
	}
}

// TestPricingPaginationAcrossOperations: DescribeServices and
// GetAttributeValues paginate on the same token scheme as GetProducts, and each
// stops offering a token on its final page.
func TestPricingPaginationAcrossOperations(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	t.Run("DescribeServices single page has no token", func(t *testing.T) {
		t.Parallel()
		resp := pricingCall(t, ts, "us-east-1", "DescribeServices", map[string]any{
			"MaxResults": 100,
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
		}
		out := pricingJSON(t, resp)
		if _, ok := out["NextToken"]; ok {
			t.Errorf("single-page DescribeServices offered a NextToken: %v", out["NextToken"])
		}
	})

	t.Run("GetAttributeValues paginates", func(t *testing.T) {
		t.Parallel()
		seen := make(map[string]int)
		token := ""
		pages := 0
		for {
			in := map[string]any{
				"ServiceCode": "AmazonS3", "AttributeName": "usagetype", "MaxResults": 3,
			}
			if token != "" {
				in["NextToken"] = token
			}
			resp := pricingCall(t, ts, "us-east-1", "GetAttributeValues", in)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("page %d status = %d, want 200 (body %s)", pages,
					resp.StatusCode, mustReadAll(t, resp))
			}
			out := pricingJSON(t, resp)
			values, ok := out["AttributeValues"].([]any)
			if !ok {
				t.Fatalf("AttributeValues = %v, want a list", out["AttributeValues"])
			}
			for _, raw := range values {
				el, _ := raw.(map[string]any)
				v, _ := el["Value"].(string)
				seen[v]++
			}
			pages++
			next, hasNext := out["NextToken"].(string)
			if !hasNext || next == "" {
				break
			}
			token = next
			if pages > 10 {
				t.Fatal("pagination did not terminate within 10 pages")
			}
		}
		if len(seen) != 7 {
			t.Errorf("saw %d distinct usagetype values, want 7", len(seen))
		}
		for v, n := range seen {
			if n != 1 {
				t.Errorf("value %q appeared on %d pages, want 1", v, n)
			}
		}
	})

	t.Run("bad token is rejected by every operation", func(t *testing.T) {
		t.Parallel()
		for _, op := range []string{"DescribeServices", "GetAttributeValues"} {
			resp := pricingCall(t, ts, "us-east-1", op, map[string]any{
				"ServiceCode": "AmazonS3", "AttributeName": "usagetype",
				"NextToken": "!!!not-base64!!!",
			})
			assertPricingError(t, resp, http.StatusBadRequest, "InvalidNextTokenException")
		}
	})
}

// TestPricingFormatVersionTooLong covers the documented 32-character cap, which
// is a distinct rejection from "not aws_v1".
func TestPricingFormatVersionTooLong(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
		"ServiceCode":   "AmazonS3",
		"FormatVersion": strings.Repeat("v", 33),
	})
	assertPricingError(t, resp, http.StatusBadRequest, "InvalidParameterException")
}

// TestPricingShutdown: the plugin holds no resources, so Shutdown must be a
// clean no-op rather than an error a caller has to special-case.
func TestPricingShutdown(t *testing.T) {
	t.Parallel()
	p := &emulator.PriceListPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:  emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(0, false),
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := p.Shutdown(t.Context()); err != nil { //nolint:contextcheck
		t.Errorf("Shutdown: %v", err)
	}
}

func TestPricingDescribeServices_UnknownServiceCode(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "DescribeServices", map[string]any{
		"ServiceCode": "AmazonNoSuchService",
	})
	assertPricingError(t, resp, http.StatusBadRequest, "InvalidParameterException")
}

func TestPricingGetAttributeValues(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "GetAttributeValues", map[string]any{
		"ServiceCode": "AmazonS3", "AttributeName": "usagetype",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	out := pricingJSON(t, resp)
	values, ok := out["AttributeValues"].([]any)
	if !ok {
		t.Fatalf("AttributeValues = %v, want a list", out["AttributeValues"])
	}
	if len(values) != 7 {
		t.Fatalf("got %d usagetype values, want 7 (one per SKU)", len(values))
	}
	// Every returned value must select at least one product. A value that
	// matches nothing would send a caller looking for a bug in their filter.
	for _, raw := range values {
		el, isMap := raw.(map[string]any)
		if !isMap {
			t.Fatalf("AttributeValues element is %T, want object", raw)
		}
		v, isStr := el["Value"].(string)
		if !isStr {
			t.Fatalf("AttributeValues element has no string Value: %v", el)
		}
		docs := priceListDocs(t, pricingGetProducts(t, ts, []map[string]string{
			{"Type": "TERM_MATCH", "Field": "usagetype", "Value": v},
		}))
		if len(docs) == 0 {
			t.Errorf("usagetype %q was advertised but matches no product", v)
		}
	}
}

func TestPricingGetAttributeValues_Errors(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	tests := []struct {
		name  string
		input map[string]any
		code  string
	}{
		{
			name:  "ServiceCode required",
			input: map[string]any{"AttributeName": "usagetype"},
			code:  "InvalidParameterException",
		},
		{
			name:  "AttributeName required",
			input: map[string]any{"ServiceCode": "AmazonS3"},
			code:  "InvalidParameterException",
		},
		{
			name:  "unknown ServiceCode is NotFound",
			input: map[string]any{"ServiceCode": "AmazonNope", "AttributeName": "usagetype"},
			code:  "NotFoundException",
		},
		{
			name:  "unknown AttributeName",
			input: map[string]any{"ServiceCode": "AmazonS3", "AttributeName": "instanceType"},
			code:  "InvalidParameterException",
		},
		{
			name: "MaxResults above 10000",
			input: map[string]any{
				"ServiceCode": "AmazonS3", "AttributeName": "usagetype", "MaxResults": 10001,
			},
			code: "InvalidParameterException",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := pricingCall(t, ts, "us-east-1", "GetAttributeValues", tt.input)
			assertPricingError(t, resp, http.StatusBadRequest, tt.code)
		})
	}
}

// TestPricingGetAttributeValues_MaxResultsIs10000 pins the bound that differs
// from the other two operations: GetAttributeValues caps at 10000, not 100, so a
// caller passing 500 must not be rejected.
func TestPricingGetAttributeValues_MaxResultsIs10000(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "GetAttributeValues", map[string]any{
		"ServiceCode": "AmazonS3", "AttributeName": "usagetype", "MaxResults": 500,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MaxResults=500 status = %d, want 200 (body %s)",
			resp.StatusCode, mustReadAll(t, resp))
	}
	_ = mustReadAll(t, resp)
}

// TestPricingEndpointRegions covers the deliberate divergence: AWS hosts the
// Price List API in exactly three regions, and substrate rejects a request
// signed for any other rather than answering it.
func TestPricingEndpointRegions(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	for _, region := range []string{"us-east-1", "ap-south-1", "eu-central-1"} {
		t.Run("endpoint region "+region, func(t *testing.T) {
			t.Parallel()
			resp := pricingCall(t, ts, region, "GetProducts", map[string]any{
				"ServiceCode": "AmazonS3",
			})
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s status = %d, want 200 (body %s)", region,
					resp.StatusCode, mustReadAll(t, resp))
				return
			}
			_ = mustReadAll(t, resp)
		})
	}

	for _, region := range []string{"us-west-2", "eu-west-1"} {
		t.Run("non-endpoint region "+region, func(t *testing.T) {
			t.Parallel()
			resp := pricingCall(t, ts, region, "GetProducts", map[string]any{
				"ServiceCode": "AmazonS3",
			})
			assertPricingError(t, resp, http.StatusBadRequest,
				"SubstrateInvalidPricingEndpoint")
		})
	}
}

// TestPricingUnsupportedOperation pins the default arm. It answered
// InvalidParameterException — the code for a bad *parameter*, not a bad operation —
// until #716 routed every plugin's unknown-action arm through one place. Pricing is
// JSON-RPC, so the answer is now the UnknownOperationException at 404 AWS publishes
// for that protocol.
func TestPricingUnsupportedOperation(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "GetPriceListFileUrl", map[string]any{})
	assertPricingError(t, resp, http.StatusNotFound, "UnknownOperationException")
}

// TestPricingSeededFailure is the point of the seed endpoint: it lets a consumer
// prove that pricing degrading does not take their I/O path down. Without it,
// the fallback branch is unreachable and a green suite says nothing about it.
func TestPricingSeededFailure(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	// Nominal first, so the failure below is demonstrably caused by the seed.
	if docs := priceListDocs(t, pricingGetProducts(t, ts, nil)); len(docs) != 7 {
		t.Fatalf("pre-seed GetProducts returned %d products, want 7", len(docs))
	}

	seedPricingFailure(t, ts, map[string]any{
		"operation": "GetProducts",
		"code":      "ThrottlingException",
		"message":   "Rate exceeded",
	})

	resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
		"ServiceCode": "AmazonS3",
	})
	assertPricingError(t, resp, http.StatusBadRequest, "ThrottlingException")

	// The seed is scoped to GetProducts, so the other operations still work —
	// which is what lets a test isolate one failing call.
	other := pricingCall(t, ts, "us-east-1", "DescribeServices", map[string]any{})
	if other.StatusCode != http.StatusOK {
		t.Errorf("DescribeServices status = %d under a GetProducts-scoped seed, want 200",
			other.StatusCode)
	}
	_ = mustReadAll(t, other)

	clearPricingFailures(t, ts, "")
	if docs := priceListDocs(t, pricingGetProducts(t, ts, nil)); len(docs) != 7 {
		t.Error("GetProducts did not recover after the seed was cleared")
	}
}

func TestPricingSeededFailure_Wildcard(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	seedPricingFailure(t, ts, map[string]any{"code": "InternalErrorException"})

	// InternalErrorException is the one documented code that is 500, not 400.
	for _, op := range []string{"GetProducts", "DescribeServices", "GetAttributeValues"} {
		resp := pricingCall(t, ts, "us-east-1", op, map[string]any{
			"ServiceCode": "AmazonS3", "AttributeName": "usagetype",
		})
		assertPricingError(t, resp, http.StatusInternalServerError, "InternalErrorException")
	}

	// A per-operation seed takes precedence over the wildcard.
	seedPricingFailure(t, ts, map[string]any{
		"operation": "GetProducts", "code": "AccessDeniedException",
	})
	resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
		"ServiceCode": "AmazonS3",
	})
	assertPricingError(t, resp, http.StatusBadRequest, "AccessDeniedException")

	// Clearing just that operation leaves the wildcard in force.
	clearPricingFailures(t, ts, "GetProducts")
	resp = pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
		"ServiceCode": "AmazonS3",
	})
	assertPricingError(t, resp, http.StatusInternalServerError, "InternalErrorException")
}

// TestPricingSeedFailure_RejectsBadCode: the seed endpoint validates the code
// against the documented set. A typo'd code would otherwise seed an error no
// consumer's catch branch matches, so their fallback path would go untested while
// the seed itself appeared to work.
func TestPricingSeedFailure_RejectsBadCode(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	tests := []struct {
		name string
		seed map[string]any
	}{
		{"code omitted", map[string]any{"operation": "GetProducts"}},
		{"typo in a real code", map[string]any{"code": "ThrotlingException"}},
		{"not a Price List code", map[string]any{"code": "NoSuchBucket"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			body, err := json.Marshal(tt.seed)
			if err != nil {
				t.Fatalf("marshal seed: %v", err)
			}
			resp, err := http.Post(ts.URL+"/v1/pricing/query-failures", //nolint:noctx
				"application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("post seed: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (body %s)", resp.StatusCode,
					mustReadAll(t, resp))
				return
			}
			_ = mustReadAll(t, resp)
		})
	}
}

// TestPricingSeedFailure_AcceptsEveryDocumentedCode makes sure the validation
// above did not narrow the seedable set: every code the Price List API documents
// must still be seedable, with the status AWS gives it.
func TestPricingSeedFailure_AcceptsEveryDocumentedCode(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		code   string
		status int
	}{
		{"AccessDeniedException", http.StatusBadRequest},
		{"ExpiredNextTokenException", http.StatusBadRequest},
		{"InvalidNextTokenException", http.StatusBadRequest},
		{"InvalidParameterException", http.StatusBadRequest},
		{"NotFoundException", http.StatusBadRequest},
		{"ThrottlingException", http.StatusBadRequest},
		{"InternalErrorException", http.StatusInternalServerError},
	} {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()
			ts := newPricingTestServer(t)
			seedPricingFailure(t, ts, map[string]any{"code": tt.code})
			resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
				"ServiceCode": "AmazonS3",
			})
			assertPricingError(t, resp, tt.status, tt.code)
		})
	}
}

// TestPricingSeedFailure_StatusCodeOverride: an explicit statusCode wins over the
// documented default, so a consumer can reproduce a gateway or proxy returning an
// unexpected status for a known code.
func TestPricingSeedFailure_StatusCodeOverride(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	seedPricingFailure(t, ts, map[string]any{
		"code": "ThrottlingException", "statusCode": http.StatusTooManyRequests,
	})
	resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
		"ServiceCode": "AmazonS3",
	})
	assertPricingError(t, resp, http.StatusTooManyRequests, "ThrottlingException")
}

func seedPricingFailure(t *testing.T, ts *httptest.Server, seed map[string]any) {
	t.Helper()
	body, err := json.Marshal(seed)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	resp, err := http.Post(ts.URL+"/v1/pricing/query-failures", //nolint:noctx
		"application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post seed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	_ = mustReadAll(t, resp)
}

func clearPricingFailures(t *testing.T, ts *httptest.Server, operation string) {
	t.Helper()
	url := ts.URL + "/v1/pricing/query-failures"
	if operation != "" {
		url += "?operation=" + operation
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatalf("build clear request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do clear request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clear status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	_ = mustReadAll(t, resp)
}

// TestPricingRoutesFromHostAndTarget proves both routing signals reach the
// plugin independently: the X-Amz-Target namespace AWSPriceListService, and the
// api.pricing.<region> host layout. A request carrying only one of them must
// still land here (#401).
func TestPricingRoutesFromHostAndTarget(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	tests := []struct {
		name   string
		host   string
		target string
	}{
		{"target only", "localhost", "AWSPriceListService.DescribeServices"},
		{"host only", "api.pricing.us-east-1.amazonaws.com", ""},
		{"both", "api.pricing.us-east-1.amazonaws.com", "AWSPriceListService.DescribeServices"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader("{}"))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			req.Host = tt.host
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			} else {
				// Without a target the operation comes from the Action parameter,
				// which is how a host-routed request names its operation.
				req.URL.RawQuery = "Action=DescribeServices"
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			body := mustReadAll(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 — the request did not route to the "+
					"pricing plugin (body %s)", resp.StatusCode, body)
			}
			if !strings.Contains(body, "AmazonS3") {
				t.Errorf("DescribeServices body does not mention AmazonS3: %s", body)
			}
		})
	}
}
