package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
)

// These tests cover the AmazonEC2 offer corpus (#894). They assert the shapes a
// consumer pricing its own EC2 usage gets wrong, each one measured in the live
// offer files rather than reasoned about: the documented seven-filter recipe
// returns three rates for Windows, one instance type publishes two rates that only
// marketoption separates, a Region can publish no rate at all for a type, and the
// free-tier pseudo-product falsifies four invariants a parser is likely to assume.
//
// Everything goes through a real wire call to GetProducts, because the point is
// what a caller observes, not what the fixture holds.

// ec2Products calls GetProducts for AmazonEC2 with the given filters and returns
// the decoded offer documents.
func ec2Products(t *testing.T, ts *httptest.Server, filters []map[string]string) []map[string]any {
	t.Helper()
	in := map[string]any{"ServiceCode": "AmazonEC2", "FormatVersion": "aws_v1"}
	if filters != nil {
		in["Filters"] = filters
	}
	resp := pricingCall(t, ts, "us-east-1", "GetProducts", in)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetProducts status = %d, want 200 (body %s)", resp.StatusCode,
			mustReadAll(t, resp))
	}
	return priceListDocs(t, pricingJSON(t, resp))
}

// ec2Filters builds TERM_MATCH filters from key/value pairs.
func ec2Filters(pairs ...string) []map[string]string {
	filters := make([]map[string]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		filters = append(filters, map[string]string{
			"Type": "TERM_MATCH", "Field": pairs[i], "Value": pairs[i+1],
		})
	}
	return filters
}

// onDemandRecipe is the seven filters AWS's own guidance says isolate a single
// on-demand rate for an instance type. Three of the tests below show it does not.
func onDemandRecipe(region, instanceType, os string) []map[string]string {
	return ec2Filters(
		"regionCode", region,
		"instanceType", instanceType,
		"operatingSystem", os,
		"tenancy", "Shared",
		"preInstalledSw", "NA",
		"capacitystatus", "Used",
		"marketoption", "OnDemand",
	)
}

// usdRate returns the single dimension's USD rate, failing if the document has
// more than one dimension.
func usdRate(t *testing.T, doc map[string]any) string {
	t.Helper()
	dims := dimensions(t, doc)
	if len(dims) != 1 {
		t.Fatalf("offer document has %d priceDimensions, want 1", len(dims))
	}
	for _, raw := range dims {
		dim, _ := raw.(map[string]any)
		ppu, _ := dim["pricePerUnit"].(map[string]any)
		usd, ok := ppu["USD"].(string)
		if !ok {
			t.Fatalf("pricePerUnit.USD is %T, want string", ppu["USD"])
		}
		return usd
	}
	return ""
}

// TestPricingEC2LinuxRateIsUnique: for Linux the documented recipe does isolate
// one SKU, which is what makes its failure for Windows surprising.
func TestPricingEC2LinuxRateIsUnique(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := ec2Products(t, ts, onDemandRecipe("us-east-1", "g6.xlarge", "Linux"))
	if len(docs) != 1 {
		t.Fatalf("got %d products for the Linux g6.xlarge recipe, want 1", len(docs))
	}
	bySKU := docBySKU(t, docs)
	doc, ok := bySKU["TPKUC6XYSUQ3VFK2"]
	if !ok {
		t.Fatalf("want SKU TPKUC6XYSUQ3VFK2, got %v", bySKU)
	}
	if got := usdRate(t, doc); got != "0.8048000000" {
		t.Errorf("g6.xlarge us-east-1 USD = %v, want 0.8048000000", got)
	}
}

// TestPricingEC2WindowsRecipeReturnsThreeRates: the seven-filter recipe is not
// sufficient. Three us-east-1 m5.xlarge Windows SKUs satisfy all seven and share
// one usagetype, differing only in licenseModel and operation — so a caller taking
// PriceList[0] picks one of $0.376, $0.192 and $0.192 arbitrarily.
func TestPricingEC2WindowsRecipeReturnsThreeRates(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := ec2Products(t, ts, onDemandRecipe("us-east-1", "m5.xlarge", "Windows"))
	if len(docs) != 3 {
		t.Fatalf("got %d products for the Windows m5.xlarge recipe, want 3 — the "+
			"seven documented filters do not isolate one rate", len(docs))
	}
	bySKU := docBySKU(t, docs)
	wantRates := map[string]string{
		"HXGQ45BFM9J2FJ3F": "0.3760000000",
		"AWJMJ73C9RDQFD5N": "0.1920000000",
		"8AW2DYRV3M9N5RRZ": "0.1920000000",
	}
	licenseModels := make(map[string]bool, 3)
	for sku, wantUSD := range wantRates {
		doc, ok := bySKU[sku]
		if !ok {
			t.Fatalf("want SKU %s in the result, got %v", sku, bySKU)
		}
		if got := usdRate(t, doc); got != wantUSD {
			t.Errorf("%s USD = %v, want %v", sku, got, wantUSD)
		}
		prod, _ := doc["product"].(map[string]any)
		attrs, _ := prod["attributes"].(map[string]any)
		if got := attrs["usagetype"]; got != "BoxUsage:m5.xlarge" {
			t.Errorf("%s usagetype = %v, want BoxUsage:m5.xlarge — usagetype is not "+
				"1:1 with a SKU in the EC2 offer file", sku, got)
		}
		lm, _ := attrs["licenseModel"].(string)
		licenseModels[lm] = true
	}
	if len(licenseModels) != 3 {
		t.Errorf("the three SKUs carry %d distinct licenseModel values, want 3 — "+
			"licenseModel is the eighth discriminator: %v", len(licenseModels), licenseModels)
	}

	// Adding licenseModel as an eighth filter does isolate one rate.
	narrowed := ec2Products(t, ts, append(onDemandRecipe("us-east-1", "m5.xlarge", "Windows"),
		ec2Filters("licenseModel", "Bring your own license")...))
	if len(narrowed) != 1 {
		t.Fatalf("got %d products with licenseModel added, want 1", len(narrowed))
	}
	if got := usdRate(t, narrowed[0]); got != "0.1920000000" {
		t.Errorf("BYOL USD = %v, want 0.1920000000", got)
	}
}

// TestPricingEC2CapacityBlockNeedsMarketOption: p5.48xlarge publishes an on-demand
// rate of $55.04 and a Capacity Block rate of $0.00, and marketoption is the only
// filter separating them — so a query that omits it can report a free p5.
func TestPricingEC2CapacityBlockNeedsMarketOption(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := ec2Products(t, ts, ec2Filters(
		"regionCode", "us-east-1",
		"instanceType", "p5.48xlarge",
		"operatingSystem", "Linux",
		"tenancy", "Shared",
		"preInstalledSw", "NA",
		"capacitystatus", "Used",
	))
	if len(docs) != 2 {
		t.Fatalf("got %d products for p5.48xlarge without marketoption, want 2", len(docs))
	}
	rates := []string{}
	for _, doc := range docs {
		rates = append(rates, usdRate(t, doc))
	}
	sort.Strings(rates)
	if rates[0] != "0.0000000000" || rates[1] != "55.0400000000" {
		t.Errorf("rates = %v, want [0.0000000000 55.0400000000] — the $0.00 row is "+
			"the Capacity Block product", rates)
	}

	// With marketoption the on-demand rate is unambiguous.
	onDemand := ec2Products(t, ts, onDemandRecipe("us-east-1", "p5.48xlarge", "Linux"))
	if len(onDemand) != 1 {
		t.Fatalf("got %d products with marketoption=OnDemand, want 1", len(onDemand))
	}
	if got := usdRate(t, onDemand[0]); got != "55.0400000000" {
		t.Errorf("p5.48xlarge on-demand USD = %v, want 55.0400000000", got)
	}
}

// TestPricingEC2RegionPublishesNoRate: eu-west-1 publishes no g6, p5 or trn1
// compute product at all, so an empty PriceList is a real observation about the
// Region rather than a gap in substrate's corpus.
func TestPricingEC2RegionPublishesNoRate(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	for _, instanceType := range []string{"g6.xlarge", "p5.48xlarge", "trn1.32xlarge"} {
		docs := ec2Products(t, ts, onDemandRecipe("eu-west-1", instanceType, "Linux"))
		if len(docs) != 0 {
			t.Errorf("eu-west-1 %s returned %d products, want none", instanceType, len(docs))
		}
	}
	// The same query in us-east-1 does return a rate, so the absence is the
	// Region's and not the filter's.
	if docs := ec2Products(t, ts, onDemandRecipe("us-east-1", "g6.xlarge", "Linux")); len(docs) != 1 {
		t.Errorf("us-east-1 g6.xlarge returned %d products, want 1", len(docs))
	}
	// eu-west-1 does publish the six types it has, at its own per-family premium.
	euM5 := ec2Products(t, ts, onDemandRecipe("eu-west-1", "m5.xlarge", "Linux"))
	if len(euM5) != 1 {
		t.Fatalf("eu-west-1 m5.xlarge returned %d products, want 1", len(euM5))
	}
	if got := usdRate(t, euM5[0]); got != "0.2140000000" {
		t.Errorf("eu-west-1 m5.xlarge USD = %v, want 0.2140000000", got)
	}
}

// TestPricingEC2UsageTypePrefixIsNotDerivable: the usagetype Region prefix cannot
// be computed from the Region code — us-east-1 has none, us-west-2 uses USW2- and
// eu-west-1 uses the legacy EU- rather than EUW1-.
func TestPricingEC2UsageTypePrefixIsNotDerivable(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	want := map[string]string{
		"us-east-1": "BoxUsage:m5.xlarge",
		"us-west-2": "USW2-BoxUsage:m5.xlarge",
		"eu-west-1": "EU-BoxUsage:m5.xlarge",
	}
	for region, wantUsage := range want {
		docs := ec2Products(t, ts, onDemandRecipe(region, "m5.xlarge", "Linux"))
		if len(docs) != 1 {
			t.Fatalf("%s m5.xlarge returned %d products, want 1", region, len(docs))
		}
		prod, _ := docs[0]["product"].(map[string]any)
		attrs, _ := prod["attributes"].(map[string]any)
		if got := attrs["usagetype"]; got != wantUsage {
			t.Errorf("%s usagetype = %v, want %v", region, got, wantUsage)
		}
	}
}

// TestPricingEC2TwoRegionsDisagreeAtTheTenthDecimal: us-west-2's rates are
// byte-identical to us-east-1's for every type in the corpus except p4d.24xlarge,
// so a consumer that assumes two Regions agreeing on nine rates agree on the tenth
// is wrong — at the seventh decimal place.
func TestPricingEC2TwoRegionsDisagreeAtTheTenthDecimal(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	rates := map[string]string{}
	for _, region := range []string{"us-east-1", "us-west-2"} {
		docs := ec2Products(t, ts, onDemandRecipe(region, "p4d.24xlarge", "Linux"))
		if len(docs) != 1 {
			t.Fatalf("%s p4d.24xlarge returned %d products, want 1", region, len(docs))
		}
		rates[region] = usdRate(t, docs[0])
	}
	if rates["us-east-1"] != "21.9576420000" {
		t.Errorf("us-east-1 p4d.24xlarge USD = %v, want 21.9576420000", rates["us-east-1"])
	}
	if rates["us-west-2"] != "21.9576400000" {
		t.Errorf("us-west-2 p4d.24xlarge USD = %v, want 21.9576400000", rates["us-west-2"])
	}
	if rates["us-east-1"] == rates["us-west-2"] {
		t.Error("the two Regions report one p4d.24xlarge rate; the offer files disagree")
	}
	// m5.xlarge is the counter-case in the same pair of Regions.
	same := map[string]string{}
	for _, region := range []string{"us-east-1", "us-west-2"} {
		docs := ec2Products(t, ts, onDemandRecipe(region, "m5.xlarge", "Linux"))
		same[region] = usdRate(t, docs[0])
	}
	if same["us-east-1"] != same["us-west-2"] {
		t.Errorf("m5.xlarge rates = %v, want the two Regions to agree", same)
	}
}

// TestPricingEC2TenancyAndCapacityChangeUsageType: Dedicated tenancy and an unused
// Capacity Reservation change the usagetype token rather than only the price, so a
// caller keyed on "BoxUsage:" misses both.
func TestPricingEC2TenancyAndCapacityChangeUsageType(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	tests := []struct {
		name          string
		filters       []map[string]string
		wantSKU       string
		wantUsageType string
		wantUSD       string
	}{
		{
			name: "dedicated tenancy",
			filters: ec2Filters("regionCode", "us-east-1", "instanceType", "m5.xlarge",
				"operatingSystem", "Linux", "tenancy", "Dedicated", "preInstalledSw", "NA",
				"capacitystatus", "Used", "marketoption", "OnDemand"),
			wantSKU: "AN9HBCS6XCP64Z83", wantUsageType: "DedicatedUsage:m5.xlarge",
			wantUSD: "0.2040000000",
		},
		{
			name: "unused capacity reservation",
			filters: ec2Filters("regionCode", "us-east-1", "instanceType", "m5.xlarge",
				"operatingSystem", "Linux", "tenancy", "Shared", "preInstalledSw", "NA",
				"capacitystatus", "UnusedCapacityReservation", "marketoption", "OnDemand"),
			wantSKU: "E9RWRGDUV5X76FUB", wantUsageType: "UnusedBox:m5.xlarge",
			wantUSD: "0.1920000000",
		},
		{
			name: "pre-installed SQL Server Standard",
			filters: ec2Filters("regionCode", "us-east-1", "instanceType", "m5.xlarge",
				"operatingSystem", "Linux", "tenancy", "Shared", "preInstalledSw", "SQL Std",
				"capacitystatus", "Used", "marketoption", "OnDemand"),
			wantSKU: "EHFDZFP2EH9BD4DX", wantUsageType: "BoxUsage:m5.xlarge",
			wantUSD: "0.6720000000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			docs := ec2Products(t, ts, tt.filters)
			if len(docs) != 1 {
				t.Fatalf("got %d products, want 1", len(docs))
			}
			prod, _ := docs[0]["product"].(map[string]any)
			if got := prod["sku"]; got != tt.wantSKU {
				t.Errorf("sku = %v, want %v", got, tt.wantSKU)
			}
			attrs, _ := prod["attributes"].(map[string]any)
			if got := attrs["usagetype"]; got != tt.wantUsageType {
				t.Errorf("usagetype = %v, want %v", got, tt.wantUsageType)
			}
			if got := usdRate(t, docs[0]); got != tt.wantUSD {
				t.Errorf("USD = %v, want %v", got, tt.wantUSD)
			}
		})
	}
}

// TestPricingEC2UnusedReservationCarriesInstanceSKU: the
// UnusedCapacityReservation product cross-references the Used SKU it shadows
// through an instancesku attribute — the one cross-reference in the corpus.
func TestPricingEC2UnusedReservationCarriesInstanceSKU(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := ec2Products(t, ts, ec2Filters("usagetype", "UnusedBox:m5.xlarge"))
	if len(docs) != 1 {
		t.Fatalf("got %d products for UnusedBox:m5.xlarge, want 1", len(docs))
	}
	prod, _ := docs[0]["product"].(map[string]any)
	attrs, _ := prod["attributes"].(map[string]any)
	if got := attrs["instancesku"]; got != "5G4TA8Z4MUKE6MJB" {
		t.Errorf("instancesku = %v, want 5G4TA8Z4MUKE6MJB", got)
	}
	// The referenced SKU is the nominal Used product, and it resolves.
	used := ec2Products(t, ts, ec2Filters("usagetype", "BoxUsage:m5.xlarge",
		"operatingSystem", "Linux", "preInstalledSw", "NA"))
	if len(used) != 1 {
		t.Fatalf("got %d products for the referenced Used SKU, want 1", len(used))
	}
	usedProd, _ := used[0]["product"].(map[string]any)
	if got := usedProd["sku"]; got != "5G4TA8Z4MUKE6MJB" {
		t.Errorf("referenced sku = %v, want 5G4TA8Z4MUKE6MJB", got)
	}
}

// TestPricingEC2FreeTierProductFalsifiesFourInvariants: the free-tier
// pseudo-product carries no instanceType, an endRange of "750" rather than "Inf",
// an offer-term code that is not the global on-demand one, a non-empty
// termAttributes and a non-empty appliesTo. Every one of those is something a
// parser written against the nominal rows would assume away.
func TestPricingEC2FreeTierProductFalsifiesFourInvariants(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	docs := ec2Products(t, ts, ec2Filters("usagetype", "Global-BoxUsage:freetrial"))
	if len(docs) != 1 {
		t.Fatalf("got %d products for the free-tier usagetype, want 1", len(docs))
	}
	doc := docs[0]
	prod, _ := doc["product"].(map[string]any)
	if got := prod["productFamily"]; got != "Compute Instance" {
		t.Errorf("productFamily = %v, want Compute Instance", got)
	}
	attrs, _ := prod["attributes"].(map[string]any)
	if _, ok := attrs["instanceType"]; ok {
		t.Errorf("free-tier product carries instanceType = %v; a Compute Instance "+
			"product need not have one", attrs["instanceType"])
	}
	if got := attrs["location"]; got != "Any" {
		t.Errorf("location = %v, want Any", got)
	}
	if got := attrs["regionCode"]; got != "" {
		t.Errorf("regionCode = %q, want the empty string", got)
	}

	terms, _ := doc["terms"].(map[string]any)
	onDemand, _ := terms["OnDemand"].(map[string]any)
	term, ok := onDemand["R3WPVNCD34N9W7UV.A429C66SYZ"].(map[string]any)
	if !ok {
		t.Fatalf("OnDemand has no R3WPVNCD34N9W7UV.A429C66SYZ term: %v", onDemand)
	}
	if got := term["offerTermCode"]; got != "A429C66SYZ" {
		t.Errorf("offerTermCode = %v, want A429C66SYZ, not the global on-demand code", got)
	}
	termAttrs, _ := term["termAttributes"].(map[string]any)
	if got := termAttrs["Restriction"]; got != "Limited SKU Usage" {
		t.Errorf("termAttributes.Restriction = %v, want Limited SKU Usage", got)
	}
	dims := dimensions(t, doc)
	if len(dims) != 1 {
		t.Fatalf("got %d priceDimensions, want 1", len(dims))
	}
	for _, raw := range dims {
		dim, _ := raw.(map[string]any)
		if got := dim["endRange"]; got != "750" {
			t.Errorf("endRange = %v, want 750 — not every final tier ends at Inf", got)
		}
		applies, _ := dim["appliesTo"].([]any)
		if len(applies) == 0 {
			t.Error("appliesTo is empty; the free-tier dimension lists the SKUs it applies to")
		}
	}
}

// TestPricingEC2RegionCodeFilterNeverSelectsFreeTier: the free-tier product's
// regionCode is the empty string, so a Region-scoped query cannot return it — the
// reason a caller filtering by Region never sees a $0.00 rate for a real type.
func TestPricingEC2RegionCodeFilterNeverSelectsFreeTier(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	for _, region := range []string{"us-east-1", "us-west-2", "eu-west-1"} {
		for _, doc := range ec2Products(t, ts, ec2Filters("regionCode", region)) {
			prod, _ := doc["product"].(map[string]any)
			if prod["sku"] == "R3WPVNCD34N9W7UV" {
				t.Errorf("regionCode=%s selected the free-tier SKU", region)
			}
		}
	}
}

// TestPricingOfferRevisionsCoverCorpus: every service's documents carry that
// service's own offer-file revision, and EC2's differs from S3's — because the two
// services publish on their own schedules and the real API's version strings
// differ accordingly.
func TestPricingOfferRevisionsCoverCorpus(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	revisions := map[string][2]string{}
	for _, code := range []string{"AmazonEC2", "AmazonS3"} {
		resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
			"ServiceCode": code, "MaxResults": 100,
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s GetProducts status = %d, want 200 (body %s)", code,
				resp.StatusCode, mustReadAll(t, resp))
		}
		docs := priceListDocs(t, pricingJSON(t, resp))
		if len(docs) == 0 {
			t.Fatalf("%s returned no products", code)
		}
		for _, doc := range docs {
			version, _ := doc["version"].(string)
			published, _ := doc["publicationDate"].(string)
			if version == "" || published == "" {
				t.Fatalf("%s document has version %q, publicationDate %q; both must "+
					"be populated", code, version, published)
			}
			if got, ok := revisions[code]; ok && got != [2]string{version, published} {
				t.Errorf("%s reports two revisions, %v and [%s %s]", code, got,
					version, published)
			}
			revisions[code] = [2]string{version, published}
		}
	}
	if revisions["AmazonEC2"] == revisions["AmazonS3"] {
		t.Errorf("both services report revision %v; the offer files are published "+
			"separately and their versions differ", revisions["AmazonEC2"])
	}
	if got := revisions["AmazonEC2"][0]; got != "20260910195514" {
		t.Errorf("AmazonEC2 version = %v, want 20260910195514", got)
	}
	if got := revisions["AmazonS3"][0]; got != "20260728131000" {
		t.Errorf("AmazonS3 version = %v, want 20260728131000", got)
	}
}

// TestPricingEC2ServiceCodeIsScoped: a GetProducts call for one service returns
// only that service's products, so adding the EC2 slice cannot leak into an S3
// query — the regression the shared corpus makes possible.
func TestPricingEC2ServiceCodeIsScoped(t *testing.T) {
	t.Parallel()
	ts := newPricingTestServer(t)

	for _, code := range []string{"AmazonEC2", "AmazonS3"} {
		resp := pricingCall(t, ts, "us-east-1", "GetProducts", map[string]any{
			"ServiceCode": code, "MaxResults": 100,
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d, want 200 (body %s)", code, resp.StatusCode,
				mustReadAll(t, resp))
		}
		docs := priceListDocs(t, pricingJSON(t, resp))
		for _, doc := range docs {
			if got := doc["serviceCode"]; got != code {
				t.Errorf("%s query returned a %v product", code, got)
			}
		}
	}
}
