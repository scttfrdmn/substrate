package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// The bundled corpus answers what AWS charges; a seed answers what a consumer's
// code does when the rate is X (#1033). These tests assert the second question is
// answerable through the same three operations that serve the first — a seed that
// GetProducts served but DescribeServices denied would dead-end the documented
// discovery path — and that a seed cannot store a shape the real API never emits,
// which is the failure the whole subsystem exists to catch.

// pricingSeedDocument builds one offer document in GetProducts' own shape, as a
// generic map so that a test can break exactly one member of it.
func pricingSeedDocument(sku, serviceCode, usagetype, price string) map[string]any {
	termKey := sku + ".JRTCKXETXF"
	rateCode := termKey + ".6YS6EN2CT7"
	return map[string]any{
		"product": map[string]any{
			"sku":           sku,
			"productFamily": "Storage",
			"attributes": map[string]any{
				"location":    "US East (N. Virginia)",
				"regionCode":  "us-east-1",
				"servicecode": serviceCode,
				"usagetype":   usagetype,
			},
		},
		"serviceCode": serviceCode,
		"terms": map[string]any{
			"OnDemand": map[string]any{
				termKey: map[string]any{
					"sku":           sku,
					"offerTermCode": "JRTCKXETXF",
					"effectiveDate": "2026-09-01T00:00:00Z",
					"priceDimensions": map[string]any{
						rateCode: map[string]any{
							"rateCode":     rateCode,
							"description":  "hypothetical seeded rate",
							"beginRange":   "0",
							"endRange":     "Inf",
							"unit":         "GB-Mo",
							"pricePerUnit": map[string]any{"USD": price},
							"appliesTo":    []string{},
						},
					},
					"termAttributes": map[string]any{},
				},
			},
		},
	}
}

// pricingSeedOffer posts an offer document and returns the response, so that a
// test can assert on a refusal as well as on a success.
func pricingSeedOffer(t *testing.T, ts *httptest.Server, query string, doc any) *http.Response {
	t.Helper()
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal offer: %v", err)
	}
	resp, err := http.Post(ts.URL+"/v1/pricing/offers"+query, //nolint:noctx
		"application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post offer: %v", err)
	}
	return resp
}

// pricingSeedOfferOK posts an offer document and requires it to be accepted.
func pricingSeedOfferOK(t *testing.T, ts *httptest.Server, query string, doc any) {
	t.Helper()
	resp := pricingSeedOffer(t, ts, query, doc)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed offer status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	_ = mustReadAll(t, resp)
}

// pricingClearOffers deletes one seeded offer, or every one when sku is empty, and
// requires the clear to succeed.
func pricingClearOffers(t *testing.T, ts *httptest.Server, sku string) {
	t.Helper()
	query := ""
	if sku != "" {
		query = "?sku=" + sku
	}
	resp := pricingDeleteOffers(t, ts, query)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete offers status = %d, want 200 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	_ = mustReadAll(t, resp)
}

// pricingPriceList decodes a GetProducts response's PriceList, whose elements are
// JSON documents encoded as strings rather than objects.
func pricingPriceList(t *testing.T, out map[string]any) []map[string]any {
	t.Helper()
	raw, ok := out["PriceList"].([]any)
	if !ok {
		t.Fatalf("PriceList missing or not a list in %v", out)
	}
	docs := make([]map[string]any, 0, len(raw))
	for i, element := range raw {
		s, isString := element.(string)
		if !isString {
			t.Fatalf("PriceList[%d] is %T, want a string", i, element)
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(s), &doc); err != nil {
			t.Fatalf("unmarshal PriceList[%d] %q: %v", i, s, err)
		}
		docs = append(docs, doc)
	}
	return docs
}

// pricingDocSKU reads a decoded offer document's SKU.
func pricingDocSKU(t *testing.T, doc map[string]any) string {
	t.Helper()
	product, ok := doc["product"].(map[string]any)
	if !ok {
		t.Fatalf("product missing in %v", doc)
	}
	sku, ok := product["sku"].(string)
	if !ok {
		t.Fatalf("product.sku missing in %v", product)
	}
	return sku
}

// TestPricingOfferSeed_ASeededServiceIsDiscoverableAndQueryable is the criterion
// the issue leads with, plus the one its open questions turned on.
//
// A service code substrate does not bundle is a NotFoundException before the seed
// and after the clear, and in between it is reachable by all three operations: the
// discovery path AWS documents is DescribeServices → GetAttributeValues → a
// GetProducts filter, and a seed that only reached the last of those would leave a
// caller querying a SKU that the first two deny exists.
func TestPricingOfferSeed_ASeededServiceIsDiscoverableAndQueryable(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingCall(t, ts, "us-east-1", "GetProducts",
		map[string]any{"ServiceCode": "AmazonRDS"})
	assertPricingError(t, resp, http.StatusBadRequest, "NotFoundException")

	pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
		"SEEDEDRDSSKU0001", "AmazonRDS", "InstanceUsage:db.t4g.micro", "0.0160000000"))

	products := pricingPriceList(t, mustPricingCall(t, ts, "GetProducts", map[string]any{
		"ServiceCode": "AmazonRDS",
		"Filters": []map[string]string{
			{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "InstanceUsage:db.t4g.micro"},
		},
	}))
	if len(products) != 1 {
		t.Fatalf("PriceList has %d documents, want 1", len(products))
	}
	if got := pricingDocSKU(t, products[0]); got != "SEEDEDRDSSKU0001" {
		t.Errorf("PriceList[0] sku = %q, want the seeded SKU", got)
	}

	services := mustPricingCall(t, ts, "DescribeServices", map[string]any{"ServiceCode": "AmazonRDS"})
	names := pricingAttributeNames(t, services, "AmazonRDS")
	// Computed from the seeded product, not declared: productFamily is a sibling of
	// the attribute map and is reported because the seeded document carries one.
	want := []string{"location", "productFamily", "regionCode", "servicecode", "usagetype"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("AttributeNames = %v, want %v", names, want)
	}

	values := mustPricingCall(t, ts, "GetAttributeValues", map[string]any{
		"ServiceCode": "AmazonRDS", "AttributeName": "usagetype",
	})
	if got := pricingAttributeValues(t, values); strings.Join(got, ",") != "InstanceUsage:db.t4g.micro" {
		t.Errorf("AttributeValues = %v, want the seeded usagetype alone", got)
	}

	pricingClearOffers(t, ts, "SEEDEDRDSSKU0001")
	resp = pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{"ServiceCode": "AmazonRDS"})
	assertPricingError(t, resp, http.StatusBadRequest, "NotFoundException")
}

// TestPricingOfferSeed_ABundledSKUNeedsReplace records the decision this PR makes
// on the issue's second open question.
//
// Overriding a measured rate is the useful behavior; overriding it silently is the
// one thing the corpus exists to prevent, because a caller asserting against an
// invented number has no way to notice. The flag does not prevent the override, it
// puts it in the request that caused it.
func TestPricingOfferSeed_ABundledSKUNeedsReplace(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)
	const bundled = "WP9ANXZGBYYSGJEA" // S3 TimedStorage-ByteHrs, measured.

	resp := pricingSeedOffer(t, ts, "", pricingSeedDocument(
		bundled, "AmazonS3", "TimedStorage-ByteHrs", "9.9900000000"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("seed over a bundled SKU status = %d, want 400 (body %s)",
			resp.StatusCode, mustReadAll(t, resp))
	}
	if body := mustReadAll(t, resp); !strings.Contains(body, "replace=true") {
		t.Errorf("refusal %q does not say how to override deliberately", body)
	}

	before := pricingPriceList(t, pricingGetProducts(t, ts, nil))

	pricingSeedOfferOK(t, ts, "?replace=true", pricingSeedDocument(
		bundled, "AmazonS3", "TimedStorage-ByteHrs", "9.9900000000"))

	after := pricingPriceList(t, pricingGetProducts(t, ts, nil))
	if len(after) != len(before) {
		t.Errorf("PriceList has %d documents after the replacement, want %d — a replacement "+
			"shadows the bundled SKU rather than joining it", len(after), len(before))
	}
	found := false
	for _, doc := range after {
		if pricingDocSKU(t, doc) != bundled {
			continue
		}
		found = true
		if !strings.Contains(mustMarshal(t, doc), "9.9900000000") {
			t.Errorf("bundled SKU still reports its measured rate after ?replace=true")
		}
	}
	if !found {
		t.Errorf("the replaced SKU is absent from the PriceList entirely")
	}
}

// TestPricingOfferSeed_RefusesShapesTheRealAPINeverEmits is where most of the work
// is, and the reason the endpoint is worth having at all.
//
// A seed that emits a shape AWS does not would let a consumer's parser pass here
// and fail against the real API — which is precisely the class of bug the bundled
// corpus was assembled to expose. Every refusal below names the member.
func TestPricingOfferSeed_RefusesShapesTheRealAPINeverEmits(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	for _, tt := range []struct {
		name    string
		mutate  func(t *testing.T, doc map[string]any)
		wantHas string
	}{
		{
			name: "a numeric pricePerUnit",
			mutate: func(t *testing.T, doc map[string]any) {
				pricingSeedDimension(t, doc)["pricePerUnit"] = map[string]any{"USD": 0.023}
			},
			wantHas: "pricePerUnit",
		},
		{
			name: "a pricePerUnit that is not a number at all",
			mutate: func(t *testing.T, doc map[string]any) {
				pricingSeedDimension(t, doc)["pricePerUnit"] = map[string]any{"USD": "free"}
			},
			wantHas: "decimal string",
		},
		{
			name: "no usagetype",
			mutate: func(t *testing.T, doc map[string]any) {
				attrs, ok := doc["product"].(map[string]any)["attributes"].(map[string]any)
				if !ok {
					t.Fatalf("product.attributes missing in %v", doc)
				}
				delete(attrs, "usagetype")
			},
			wantHas: "usagetype",
		},
		{
			name: "no sku",
			mutate: func(t *testing.T, doc map[string]any) {
				product, ok := doc["product"].(map[string]any)
				if !ok {
					t.Fatalf("product missing in %v", doc)
				}
				delete(product, "sku")
			},
			wantHas: "product.sku",
		},
		{
			name: "no serviceCode",
			mutate: func(t *testing.T, doc map[string]any) {
				delete(doc, "serviceCode")
			},
			wantHas: "serviceCode",
		},
		{
			name: "a Reserved term substrate cannot render",
			mutate: func(t *testing.T, doc map[string]any) {
				terms, ok := doc["terms"].(map[string]any)
				if !ok {
					t.Fatalf("terms missing in %v", doc)
				}
				terms["Reserved"] = map[string]any{}
			},
			wantHas: "not modeled",
		},
		{
			name: "a term key that is not <sku>.<offerTermCode>",
			mutate: func(t *testing.T, doc map[string]any) {
				onDemand, ok := doc["terms"].(map[string]any)["OnDemand"].(map[string]any)
				if !ok {
					t.Fatalf("terms.OnDemand missing in %v", doc)
				}
				for key, term := range onDemand {
					delete(onDemand, key)
					onDemand["JRTCKXETXF"] = term
				}
			},
			wantHas: "offerTermCode",
		},
		{
			name: "a rateCode that disagrees with its key",
			mutate: func(t *testing.T, doc map[string]any) {
				pricingSeedDimension(t, doc)["rateCode"] = "SOMETHINGELSE"
			},
			wantHas: "rateCode",
		},
		{
			name: "no unit",
			mutate: func(t *testing.T, doc map[string]any) {
				delete(pricingSeedDimension(t, doc), "unit")
			},
			wantHas: "unit",
		},
		{
			name: "no effectiveDate",
			mutate: func(t *testing.T, doc map[string]any) {
				delete(pricingSeedTerm(t, doc), "effectiveDate")
			},
			wantHas: "effectiveDate",
		},
		{
			name: "no price dimensions",
			mutate: func(t *testing.T, doc map[string]any) {
				pricingSeedTerm(t, doc)["priceDimensions"] = map[string]any{}
			},
			wantHas: "priceDimensions",
		},
		{
			// A term carries its own sku, and AWS always repeats the product's. A seed
			// that disagrees would serve a document whose two halves name different
			// products, which no offer file does.
			name: "a term sku that disagrees with the product's",
			mutate: func(t *testing.T, doc map[string]any) {
				pricingSeedTerm(t, doc)["sku"] = "SEEDEDOTHERSKU01"
			},
			wantHas: "not the product's sku",
		},
		{
			name: "two OnDemand terms for one SKU",
			mutate: func(t *testing.T, doc map[string]any) {
				onDemand, ok := doc["terms"].(map[string]any)["OnDemand"].(map[string]any)
				if !ok {
					t.Fatalf("terms.OnDemand missing in %v", doc)
				}
				onDemand["SEEDEDBADSKU0001.MZU6U2429S"] = map[string]any{}
			},
			wantHas: "exactly one term",
		},
		{
			name: "a priceDimensions key that is not <sku>.<offerTermCode>.<dimensionCode>",
			mutate: func(t *testing.T, doc map[string]any) {
				dims, ok := pricingSeedTerm(t, doc)["priceDimensions"].(map[string]any)
				if !ok {
					t.Fatalf("priceDimensions missing in %v", doc)
				}
				for key, dim := range dims {
					delete(dims, key)
					d, isMap := dim.(map[string]any)
					if !isMap {
						t.Fatalf("priceDimension %q is %T, want a map", key, dim)
					}
					d["rateCode"] = "6YS6EN2CT7"
					dims["6YS6EN2CT7"] = d
				}
			},
			wantHas: "dimensionCode",
		},
		{
			name: "an empty pricePerUnit",
			mutate: func(t *testing.T, doc map[string]any) {
				pricingSeedDimension(t, doc)["pricePerUnit"] = map[string]any{}
			},
			wantHas: "pricePerUnit is required",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			doc := pricingSeedDocument("SEEDEDBADSKU0001", "AmazonRDS", "InstanceUsage", "0.0160000000")
			tt.mutate(t, doc)
			resp := pricingSeedOffer(t, ts, "", doc)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, mustReadAll(t, resp))
			}
			if body := mustReadAll(t, resp); !strings.Contains(body, tt.wantHas) {
				t.Errorf("refusal %q does not name %q", body, tt.wantHas)
			}
		})
	}
}

// TestPricingOfferSeed_ARevisionIsCarriedOrObviouslySynthetic answers the issue's
// third open question.
//
// Every real offer file reports a 14-digit version and an RFC3339 publication date.
// A seeded product for a service substrate does not bundle has no revision to
// report, and minting a realistic-looking one would let a caller believe it was
// reading a published file — so the default pair is deliberately not plausible.
func TestPricingOfferSeed_ARevisionIsCarriedOrObviouslySynthetic(t *testing.T) {
	t.Parallel()

	t.Run("no revision in the seed", func(t *testing.T) {
		t.Parallel()
		ts := newPricingTestServer(t)
		pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
			"SEEDEDNOREV00001", "AmazonRDS", "InstanceUsage", "0.0160000000"))
		doc := pricingSeedOnlyDocument(t, ts, "AmazonRDS")
		if doc["version"] != "substrate-seeded" {
			t.Errorf("version = %v, want the obviously-synthetic default", doc["version"])
		}
		if doc["publicationDate"] != "1970-01-01T00:00:00Z" {
			t.Errorf("publicationDate = %v, want the synthetic default", doc["publicationDate"])
		}
	})

	t.Run("a revision the seed carries", func(t *testing.T) {
		t.Parallel()
		ts := newPricingTestServer(t)
		seed := pricingSeedDocument("SEEDEDWITHREV001", "AmazonRDS", "InstanceUsage", "0.0160000000")
		seed["version"] = "20260901120000"
		seed["publicationDate"] = "2026-09-01T12:00:00Z"
		pricingSeedOfferOK(t, ts, "", seed)
		doc := pricingSeedOnlyDocument(t, ts, "AmazonRDS")
		if doc["version"] != "20260901120000" {
			t.Errorf("version = %v, want the seeded one", doc["version"])
		}
		if doc["publicationDate"] != "2026-09-01T12:00:00Z" {
			t.Errorf("publicationDate = %v, want the seeded one", doc["publicationDate"])
		}
	})

	t.Run("a seeded SKU does not inherit a bundled service's revision", func(t *testing.T) {
		t.Parallel()
		ts := newPricingTestServer(t)
		pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
			"SEEDEDS3SKU00001", "AmazonS3", "TimedStorage-Seeded", "0.0010000000"))
		docs := pricingPriceList(t, pricingGetProducts(t, ts, []map[string]string{
			{"Type": "TERM_MATCH", "Field": "usagetype", "Value": "TimedStorage-Seeded"},
		}))
		if len(docs) != 1 {
			t.Fatalf("PriceList has %d documents, want 1", len(docs))
		}
		// AmazonS3's measured revision is 20260728131000; claiming it for a product
		// that file does not contain would be the lie this default exists to avoid.
		if docs[0]["version"] != "substrate-seeded" {
			t.Errorf("version = %v, want the synthetic default rather than S3's measured revision",
				docs[0]["version"])
		}
	})
}

// TestPricingOfferSeed_SeedsFollowTheBundledEntriesInAStableOrder pins the
// property that makes NextToken paging survive a seed.
//
// pricingCorpus is ordered deliberately so that PriceList order, and therefore
// every page boundary, is stable. A map-ordered overlay would undo that for every
// seeded query; seeds therefore follow the bundled entries, sorted by SKU.
func TestPricingOfferSeed_SeedsFollowTheBundledEntriesInAStableOrder(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	before := pricingPriceList(t, pricingGetProducts(t, ts, nil))
	pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
		"SEEDEDS3SKUZZZZZ", "AmazonS3", "TimedStorage-SeededZ", "0.0020000000"))
	pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
		"SEEDEDS3SKUAAAAA", "AmazonS3", "TimedStorage-SeededA", "0.0010000000"))

	after := pricingPriceList(t, pricingGetProducts(t, ts, nil))
	if len(after) != len(before)+2 {
		t.Fatalf("PriceList has %d documents, want %d", len(after), len(before)+2)
	}
	for i, doc := range before {
		if pricingDocSKU(t, after[i]) != pricingDocSKU(t, doc) {
			t.Fatalf("bundled document %d moved: %q, was %q",
				i, pricingDocSKU(t, after[i]), pricingDocSKU(t, doc))
		}
	}
	if got := pricingDocSKU(t, after[len(before)]); got != "SEEDEDS3SKUAAAAA" {
		t.Errorf("first seeded document is %q, want the lower SKU", got)
	}
	if got := pricingDocSKU(t, after[len(before)+1]); got != "SEEDEDS3SKUZZZZZ" {
		t.Errorf("second seeded document is %q, want the higher SKU", got)
	}

	// Clearing every seed leaves the bundled corpus in the only state it ever has.
	pricingClearOffers(t, ts, "")
	restored := pricingPriceList(t, pricingGetProducts(t, ts, nil))
	if len(restored) != len(before) {
		t.Errorf("PriceList has %d documents after a full clear, want the bundled %d",
			len(restored), len(before))
	}
}

// TestPricingOfferSeed_ClearingFailuresLeavesOffersAlone is the reason offers have
// their own key prefix.
//
// A bare DELETE /v1/pricing/query-failures lists the control namespace by the
// "failure:" prefix and deletes everything it finds, so an offer stored under that
// prefix would be swept away by clearing an unrelated seeded error.
func TestPricingOfferSeed_ClearingFailuresLeavesOffersAlone(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
		"SEEDEDRDSSKU0002", "AmazonRDS", "InstanceUsage", "0.0160000000"))
	seedPricingFailure(t, ts, map[string]any{"code": "ThrottlingException"})
	clearPricingFailures(t, ts, "")

	docs := pricingPriceList(t, mustPricingCall(t, ts, "GetProducts",
		map[string]any{"ServiceCode": "AmazonRDS"}))
	if len(docs) != 1 {
		t.Fatalf("PriceList has %d documents after a failure clear, want the seeded 1", len(docs))
	}
}

// TestPricingOfferSeed_ATieredSeedKeepsEveryDimension is the shape the bundled
// corpus leads with, seeded rather than measured.
//
// The S3 corpus exists largely because TimedStorage-ByteHrs carries three
// dimensions and a parser that reads the first reports the 50 TB rate for a 600 TB
// bucket. A seed that could not express a second tier would make that class of bug
// untestable at a rate of the caller's choosing, which is the point of seeding.
func TestPricingOfferSeed_ATieredSeedKeepsEveryDimension(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	const sku = "SEEDEDTIERED0001"
	doc := pricingSeedDocument(sku, "AmazonRDS", "TieredSeededStorage", "0.0230000000")
	dims, ok := pricingSeedTerm(t, doc)["priceDimensions"].(map[string]any)
	if !ok {
		t.Fatalf("priceDimensions missing in %v", doc)
	}
	second := sku + ".JRTCKXETXF.D42MF2PVJS"
	dims[second] = map[string]any{
		"rateCode":     second,
		"description":  "hypothetical seeded rate over 50 TB",
		"beginRange":   "51200",
		"endRange":     "Inf",
		"unit":         "GB-Mo",
		"pricePerUnit": map[string]any{"USD": "0.0210000000"},
		"appliesTo":    []string{},
	}
	pricingSeedOfferOK(t, ts, "", doc)

	served := pricingSeedOnlyDocument(t, ts, "AmazonRDS")
	terms, ok := served["terms"].(map[string]any)
	if !ok {
		t.Fatalf("terms missing in %v", served)
	}
	onDemand, ok := terms["OnDemand"].(map[string]any)
	if !ok {
		t.Fatalf("terms.OnDemand missing in %v", terms)
	}
	term, ok := onDemand[sku+".JRTCKXETXF"].(map[string]any)
	if !ok {
		t.Fatalf("term %q missing in %v", sku+".JRTCKXETXF", onDemand)
	}
	servedDims, ok := term["priceDimensions"].(map[string]any)
	if !ok {
		t.Fatalf("priceDimensions missing in %v", term)
	}
	if len(servedDims) != 2 {
		t.Fatalf("served term has %d priceDimensions, want both", len(servedDims))
	}
	last, ok := servedDims[second].(map[string]any)
	if !ok {
		t.Fatalf("dimension %q missing in %v", second, servedDims)
	}
	if last["endRange"] != "Inf" {
		t.Errorf("final dimension endRange = %v, want the literal string \"Inf\"", last["endRange"])
	}
}

// TestPricingOfferSeed_ABundledServiceGainsTheSeedsAttributes is the union half of
// the computed attribute list.
//
// A service substrate bundles already has a measured attribute list, and a seed
// under that service code adds to it rather than replacing it: dropping a bundled
// name would break the documented discovery path for products the seed never
// touched, and omitting the seed's own name would leave a filter that works in
// GetProducts undiscoverable in DescribeServices.
func TestPricingOfferSeed_ABundledServiceGainsTheSeedsAttributes(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	doc := pricingSeedDocument("SEEDEDS3UNION001", "AmazonS3", "TimedStorage-Union", "0.0010000000")
	attrs, ok := doc["product"].(map[string]any)["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("product.attributes missing in %v", doc)
	}
	// An attribute no S3 product carries, so the union is observable.
	attrs["engineCode"] = "seeded"
	pricingSeedOfferOK(t, ts, "", doc)

	names := pricingAttributeNames(t, mustPricingCall(t, ts, "DescribeServices",
		map[string]any{"ServiceCode": "AmazonS3"}), "AmazonS3")
	for _, want := range []string{"engineCode", "volumeType", "usagetype"} {
		if !slices.Contains(names, want) {
			t.Errorf("AttributeNames %v omits %q", names, want)
		}
	}
	if !slices.IsSorted(names) {
		t.Errorf("AttributeNames %v is not sorted", names)
	}
}

// TestPricingOfferSeed_RefusesAMalformedReplaceFlag pins the flag's own parse.
//
// replace is the one control the endpoint has over the measured corpus, so a
// misspelled value must not read as false and silently refuse an override the
// caller believes it asked for.
func TestPricingOfferSeed_RefusesAMalformedReplaceFlag(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	resp := pricingSeedOffer(t, ts, "?replace=yes-please", pricingSeedDocument(
		"WP9ANXZGBYYSGJEA", "AmazonS3", "TimedStorage-ByteHrs", "0.0010000000"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, mustReadAll(t, resp))
	}
	if body := mustReadAll(t, resp); !strings.Contains(body, "replace must be a boolean") {
		t.Errorf("refusal %q does not name the flag", body)
	}
}

// TestPricingOfferSeed_APlantedRecordIsSkippedNotFatal covers the only path by
// which an unservable seed record can exist.
//
// Everything the endpoint stores has been validated, so a record it cannot read
// was written by something else. Failing the request would take out every pricing
// call in the process — including the bundled corpus, which is fine — for a seed the
// caller may not know exists, so the record is skipped and logged instead.
func TestPricingOfferSeed_APlantedRecordIsSkippedNotFatal(t *testing.T) {
	t.Parallel()
	ts, state := newPricingTestServerWithState(t)

	// Not JSON at all, and JSON that decodes but validates as unservable: the two
	// ways a planted record fails, at the two places seededOffers can give up.
	plant := map[string]string{
		"SEEDEDGARBAGE001": "not an offer document",
		"SEEDEDNOUSAGE001": `{"product":{"sku":"SEEDEDNOUSAGE001","attributes":{}},` +
			`"serviceCode":"AmazonRDS","terms":{}}`,
	}
	for sku, raw := range plant {
		if err := state.Put(t.Context(), emulator.PricingCtrlNamespaceForTest,
			emulator.PricingSeededOfferKeyForTest(sku), []byte(raw)); err != nil {
			t.Fatalf("plant %s: %v", sku, err)
		}
	}

	// The bundled corpus answers as it always does.
	docs := pricingPriceList(t, mustPricingCall(t, ts, "GetProducts",
		map[string]any{"ServiceCode": "AmazonS3"}))
	if len(docs) == 0 {
		t.Fatal("GetProducts served no bundled documents alongside two unreadable records")
	}
	for _, doc := range docs {
		if sku := pricingDocSKU(t, doc); plant[sku] != "" {
			t.Errorf("PriceList carries planted SKU %q", sku)
		}
	}

	// And a seed posted afterwards is served, so the skip is per record.
	pricingSeedOfferOK(t, ts, "", pricingSeedDocument(
		"SEEDEDAFTERPLANT", "AmazonRDS", "InstanceUsage", "0.0160000000"))
	if got := pricingDocSKU(t, pricingSeedOnlyDocument(t, ts, "AmazonRDS")); got != "SEEDEDAFTERPLANT" {
		t.Errorf("served SKU = %q, want the one valid seed", got)
	}
}

// pricingOfferFaultState wraps a working StateManager and injects a store failure
// on one operation within the Price List control namespace, following
// elbLimitFaultState. Scoping it to that namespace keeps the failure to the store
// the endpoint under test writes, so the rest of the server still works.
type pricingOfferFaultState struct {
	emulator.StateManager
	putErr    error
	deleteErr error
	listErr   error
}

func (m *pricingOfferFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if namespace == emulator.PricingCtrlNamespaceForTest && m.putErr != nil {
		return m.putErr
	}
	return m.StateManager.Put(ctx, namespace, key, value)
}

func (m *pricingOfferFaultState) Delete(ctx context.Context, namespace, key string) error {
	if namespace == emulator.PricingCtrlNamespaceForTest && m.deleteErr != nil {
		return m.deleteErr
	}
	return m.StateManager.Delete(ctx, namespace, key)
}

func (m *pricingOfferFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if namespace == emulator.PricingCtrlNamespaceForTest && m.listErr != nil {
		return nil, m.listErr
	}
	return m.StateManager.List(ctx, namespace, prefix)
}

// TestPricingOfferSeed_AStoreFailureIsReportedNotSwallowed pins the control
// endpoints' own failure mode.
//
// A seed that answered 200 while storing nothing would leave a test asserting
// against the bundled rate and reporting a pass, which is the one outcome worse
// than a refusal. The store's reason is carried through, because the only caller is
// a developer or a test harness.
func TestPricingOfferSeed_AStoreFailureIsReportedNotSwallowed(t *testing.T) {
	t.Parallel()

	boom := errors.New("state store unavailable")
	for _, tt := range []struct {
		name  string
		state func() *pricingOfferFaultState
		call  func(t *testing.T, ts *httptest.Server) *http.Response
	}{
		{
			name:  "a seed whose put fails",
			state: func() *pricingOfferFaultState { return &pricingOfferFaultState{putErr: boom} },
			call: func(t *testing.T, ts *httptest.Server) *http.Response {
				return pricingSeedOffer(t, ts, "", pricingSeedDocument(
					"SEEDEDPUTFAILS01", "AmazonRDS", "InstanceUsage", "0.0160000000"))
			},
		},
		{
			name:  "a single clear whose delete fails",
			state: func() *pricingOfferFaultState { return &pricingOfferFaultState{deleteErr: boom} },
			call: func(t *testing.T, ts *httptest.Server) *http.Response {
				return pricingDeleteOffers(t, ts, "?sku=SEEDEDPUTFAILS01")
			},
		},
		{
			name:  "a full clear whose list fails",
			state: func() *pricingOfferFaultState { return &pricingOfferFaultState{listErr: boom} },
			call: func(t *testing.T, ts *httptest.Server) *http.Response {
				return pricingDeleteOffers(t, ts, "")
			},
		},
		{
			// The delete inside the sweep, which is a different site from the
			// single-SKU delete above: one seed is stored first, so the loop runs.
			name: "a full clear whose delete fails",
			state: func() *pricingOfferFaultState {
				inner := emulator.NewMemoryStateManager()
				if err := inner.Put(context.Background(), emulator.PricingCtrlNamespaceForTest,
					emulator.PricingSeededOfferKeyForTest("SEEDEDSWEEPME001"),
					[]byte(`{"product":{"sku":"SEEDEDSWEEPME001"}}`)); err != nil {
					t.Fatalf("seed the record to sweep: %v", err)
				}
				return &pricingOfferFaultState{StateManager: inner, deleteErr: boom}
			},
			call: func(t *testing.T, ts *httptest.Server) *http.Response {
				return pricingDeleteOffers(t, ts, "")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := tt.state()
			if state.StateManager == nil {
				state.StateManager = emulator.NewMemoryStateManager()
			}
			resp := tt.call(t, newPricingTestServerOn(t, state))
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (body %s)", resp.StatusCode, mustReadAll(t, resp))
			}
			if body := mustReadAll(t, resp); !strings.Contains(body, boom.Error()) {
				t.Errorf("response %q does not carry the store's reason", body)
			}
		})
	}
}

// TestPricingOfferSeed_AnUnlistableSeedStoreStillServesTheCorpus is the other side
// of the same store failure.
//
// A control endpoint reports it, because its caller asked for a write. A query
// operation does not, because the bundled corpus is measured data that needs no
// store at all: turning an unreadable seed store into a 500 would take out every
// rate a test was already relying on, to report a seed the test may never have set.
func TestPricingOfferSeed_AnUnlistableSeedStoreStillServesTheCorpus(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServerOn(t, &pricingOfferFaultState{
		StateManager: emulator.NewMemoryStateManager(),
		listErr:      errors.New("state store unavailable"),
	})

	docs := pricingPriceList(t, mustPricingCall(t, ts, "GetProducts",
		map[string]any{"ServiceCode": "AmazonS3"}))
	if len(docs) == 0 {
		t.Error("GetProducts served nothing when the seed store could not be listed")
	}
}

// pricingDeleteOffers issues DELETE /v1/pricing/offers and returns the response,
// so that a test can assert on a failure as well as on a clear.
func pricingDeleteOffers(t *testing.T, ts *httptest.Server, query string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/pricing/offers"+query, nil) //nolint:noctx
	if err != nil {
		t.Fatalf("build delete: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete offers: %v", err)
	}
	return resp
}

// mustPricingCall issues a Price List call and requires a 200.
func mustPricingCall(t *testing.T, ts *httptest.Server, op string, in any) map[string]any {
	t.Helper()
	resp := pricingCall(t, ts, "us-east-1", op, in)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status = %d, want 200 (body %s)", op, resp.StatusCode, mustReadAll(t, resp))
	}
	return pricingJSON(t, resp)
}

// pricingSeedOnlyDocument returns the single document GetProducts serves for a
// service that carries exactly one seeded SKU and nothing bundled.
func pricingSeedOnlyDocument(t *testing.T, ts *httptest.Server, serviceCode string) map[string]any {
	t.Helper()
	docs := pricingPriceList(t, mustPricingCall(t, ts, "GetProducts",
		map[string]any{"ServiceCode": serviceCode}))
	if len(docs) != 1 {
		t.Fatalf("PriceList has %d documents for %s, want 1", len(docs), serviceCode)
	}
	return docs[0]
}

// pricingAttributeNames reads one service's AttributeNames out of a
// DescribeServices response.
func pricingAttributeNames(t *testing.T, out map[string]any, serviceCode string) []string {
	t.Helper()
	services, ok := out["Services"].([]any)
	if !ok || len(services) != 1 {
		t.Fatalf("Services is not a single-element list in %v", out)
	}
	service, ok := services[0].(map[string]any)
	if !ok {
		t.Fatalf("Services[0] is %T, want an object", services[0])
	}
	if service["ServiceCode"] != serviceCode {
		t.Fatalf("ServiceCode = %v, want %q", service["ServiceCode"], serviceCode)
	}
	raw, ok := service["AttributeNames"].([]any)
	if !ok {
		t.Fatalf("AttributeNames missing in %v", service)
	}
	names := make([]string, 0, len(raw))
	for _, n := range raw {
		s, isString := n.(string)
		if !isString {
			t.Fatalf("AttributeNames element is %T, want a string", n)
		}
		names = append(names, s)
	}
	return names
}

// pricingAttributeValues reads the values out of a GetAttributeValues response.
func pricingAttributeValues(t *testing.T, out map[string]any) []string {
	t.Helper()
	raw, ok := out["AttributeValues"].([]any)
	if !ok {
		t.Fatalf("AttributeValues missing in %v", out)
	}
	values := make([]string, 0, len(raw))
	for _, v := range raw {
		entry, isObject := v.(map[string]any)
		if !isObject {
			t.Fatalf("AttributeValues element is %T, want an object", v)
		}
		s, isString := entry["Value"].(string)
		if !isString {
			t.Fatalf("AttributeValues element has no string Value: %v", entry)
		}
		values = append(values, s)
	}
	return values
}

// pricingSeedTerm returns the one on-demand term of a seed document under
// construction.
func pricingSeedTerm(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	onDemand, ok := doc["terms"].(map[string]any)["OnDemand"].(map[string]any)
	if !ok {
		t.Fatalf("terms.OnDemand missing in %v", doc)
	}
	for _, term := range onDemand {
		entry, isObject := term.(map[string]any)
		if !isObject {
			t.Fatalf("term is %T, want an object", term)
		}
		return entry
	}
	t.Fatalf("terms.OnDemand is empty in %v", doc)
	return nil
}

// pricingSeedDimension returns the one price dimension of a seed document under
// construction.
func pricingSeedDimension(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	dims, ok := pricingSeedTerm(t, doc)["priceDimensions"].(map[string]any)
	if !ok {
		t.Fatalf("priceDimensions missing in %v", doc)
	}
	for _, d := range dims {
		entry, isObject := d.(map[string]any)
		if !isObject {
			t.Fatalf("price dimension is %T, want an object", d)
		}
		return entry
	}
	t.Fatalf("priceDimensions is empty in %v", doc)
	return nil
}

// mustMarshal renders a decoded document back to JSON for a substring assertion.
func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}
