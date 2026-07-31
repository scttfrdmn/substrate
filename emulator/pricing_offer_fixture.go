package emulator

// This file holds the offer corpus the Price List Query API serves (#401).
//
// Every SKU, attribute value, rate code, offer-term code and price string below
// is copied from the live AWS offer file
// https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json
// (version 20260728131000, publicationDate 2026-07-28T13:10:00Z). Nothing here is
// invented, because the point of the corpus is to reproduce the shapes that
// surprise a caller — and a tidied-up fixture would let a consumer's code pass
// its tests and then misbehave against the real API. The specific traps, each
// verified in that file:
//
//   - PriceList elements are JSON *documents encoded as strings*, not objects.
//   - pricePerUnit values are strings with trailing zeros ("0.0230000000"), not
//     numbers.
//   - productFamily is absent from most products — 315 of the 381 in the real S3
//     offer file omit it — so a caller that filters on it will miss most SKUs.
//     usagetype is the attribute that is reliably present and 1:1.
//   - TimedStorage-ByteHrs carries three priceDimensions, the last with
//     "endRange": "Inf". A parser that takes the first dimension silently reports
//     the 50 TB tier rate as if it were the only one.
//   - Filtering productFamily=Storage with volumeType="Glacier Deep Archive"
//     returns *only* TimedStorage-GDA-Staging at $0.021/GB-Mo — the staging rate,
//     21x the $0.00099 archive rate. There is no GDA-ByteHrs SKU in the S3 offer
//     file at all, so this filter cannot return the rate a caller expects; the
//     nearest $0.00099 SKU is Intelligent-Tiering's TimedStorage-INT-DAA-ByteHrs.
//
// See docs/services.md for the served operations and the seed endpoints.

// pricingFormatVersion is the only FormatVersion the Price List API defines.
const pricingFormatVersion = "aws_v1"

// pricingOfferVersion and pricingOfferPublicationDate identify the offer file
// revision the corpus below was taken from. They are embedded in each PriceList
// document exactly as the real API emits them.
const (
	pricingOfferVersion         = "20260728131000"
	pricingOfferPublicationDate = "2026-07-28T13:10:00Z"
)

// pricingOfferTermCode is the on-demand offer-term code. AWS uses the same
// literal for every on-demand term across every service.
const pricingOfferTermCode = "JRTCKXETXF"

// pricingPriceDimension is one rate within an offer term. A tiered SKU has
// several, distinguished by BeginRange/EndRange.
type pricingPriceDimension struct {
	// RateCode is "<sku>.<offerTermCode>.<dimensionCode>".
	RateCode string `json:"rateCode"`

	// Description is AWS's own human-readable rate text, kept verbatim because
	// some callers parse the dollar figure out of it.
	Description string `json:"description"`

	// BeginRange and EndRange bound the tier. EndRange is the literal string
	// "Inf" on the final tier, not a number and not null.
	BeginRange string `json:"beginRange"`
	EndRange   string `json:"endRange"`

	// Unit is the billing unit, e.g. "GB-Mo" or "Requests".
	Unit string `json:"unit"`

	// PricePerUnit maps a currency code to the rate as a *string*, e.g.
	// "0.0230000000". AWS never emits it as a number.
	PricePerUnit map[string]string `json:"pricePerUnit"`

	// AppliesTo is present but empty on every on-demand dimension AWS emits.
	AppliesTo []string `json:"appliesTo"`
}

// pricingTerm is one offer term for a SKU.
type pricingTerm struct {
	SKU             string                           `json:"sku"`
	OfferTermCode   string                           `json:"offerTermCode"`
	EffectiveDate   string                           `json:"effectiveDate"`
	PriceDimensions map[string]pricingPriceDimension `json:"priceDimensions"`
	TermAttributes  map[string]string                `json:"termAttributes"`
}

// pricingProduct is the product half of a PriceList document.
type pricingProduct struct {
	SKU string `json:"sku"`

	// ProductFamily is omitted when the product has none, which is the common
	// case rather than the exception. omitempty is what reproduces that.
	ProductFamily string `json:"productFamily,omitempty"`

	Attributes map[string]string `json:"attributes"`
}

// pricingOfferDoc is one element of a GetProducts PriceList — the structure that
// gets marshaled and then embedded in the response as a *string*.
type pricingOfferDoc struct {
	Product         pricingProduct                    `json:"product"`
	ServiceCode     string                            `json:"serviceCode"`
	Terms           map[string]map[string]pricingTerm `json:"terms"`
	Version         string                            `json:"version"`
	PublicationDate string                            `json:"publicationDate"`
}

// pricingCorpusEntry is a SKU in substrate's offer corpus, before assembly into
// a pricingOfferDoc.
type pricingCorpusEntry struct {
	sku           string
	serviceCode   string
	productFamily string
	attributes    map[string]string
	effectiveDate string
	dimensions    []pricingPriceDimension
}

// pricingServiceCodeS3 is the Price List service code for Amazon S3. It is not
// the same string as substrate's own "s3" service name.
const pricingServiceCodeS3 = "AmazonS3"

// pricingCorpus is the seven-SKU offer corpus substrate serves. It is
// deliberately small: each entry exists to exhibit a specific shape a caller
// must handle, and the set is ordered so that iteration is stable.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingCorpus = []pricingCorpusEntry{
	{
		// Tiered storage: three dimensions, final EndRange "Inf". A caller that
		// reads only the first reports the 50 TB rate for a 600 TB bucket.
		sku:           "WP9ANXZGBYYSGJEA",
		serviceCode:   pricingServiceCodeS3,
		productFamily: "Storage",
		attributes: map[string]string{
			"availability": "99.99%", "durability": "99.999999999%",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"storageClass": "General Purpose", "usagetype": "TimedStorage-ByteHrs",
			"volumeType": "Standard",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{
			{
				RateCode:    "WP9ANXZGBYYSGJEA.JRTCKXETXF.PGHJ3S3EYE",
				Description: "$0.023 per GB - first 50 TB / month of storage used",
				BeginRange:  "0", EndRange: "51200", Unit: "GB-Mo",
				PricePerUnit: map[string]string{"USD": "0.0230000000"}, AppliesTo: []string{},
			},
			{
				RateCode:    "WP9ANXZGBYYSGJEA.JRTCKXETXF.D42MF2PVJS",
				Description: "$0.022 per GB - next 450 TB / month of storage used",
				BeginRange:  "51200", EndRange: "512000", Unit: "GB-Mo",
				PricePerUnit: map[string]string{"USD": "0.0220000000"}, AppliesTo: []string{},
			},
			{
				RateCode:    "WP9ANXZGBYYSGJEA.JRTCKXETXF.PXJDJ3YRG3",
				Description: "$0.021 per GB - storage used / month over 500 TB",
				BeginRange:  "512000", EndRange: "Inf", Unit: "GB-Mo",
				PricePerUnit: map[string]string{"USD": "0.0210000000"}, AppliesTo: []string{},
			},
		},
	},
	{
		// The rate the reporter's static table had 10x low: $0.005 per 1,000 is
		// $0.000005 per request, and the unit is "Requests" — per-request, not
		// per-thousand. Reading the figure out of the description instead of the
		// pricePerUnit is how a 1000x error gets made.
		sku:           "E9YHNFENF4XQBZR6",
		serviceCode:   pricingServiceCodeS3,
		productFamily: "API Request",
		attributes: map[string]string{
			"group": "S3-API-Tier1", "groupDescription": "PUT/COPY/POST or LIST requests",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"usagetype": "Requests-Tier1",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "E9YHNFENF4XQBZR6.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.005 per 1,000 PUT, COPY, POST, or LIST requests",
			BeginRange:  "0", EndRange: "Inf", Unit: "Requests",
			PricePerUnit: map[string]string{"USD": "0.0000050000"}, AppliesTo: []string{},
		}},
	},
	{
		// GET requests are quoted per 10,000, not per 1,000 — the denominator
		// changes between Tier1 and Tier2 while the unit stays "Requests".
		sku:           "ZWQ6Q48CRJXX4FXE",
		serviceCode:   pricingServiceCodeS3,
		productFamily: "API Request",
		attributes: map[string]string{
			"group": "S3-API-Tier2", "groupDescription": "GET and all other requests",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"usagetype": "Requests-Tier2",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "ZWQ6Q48CRJXX4FXE.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.004 per 10,000 GET and all other requests",
			BeginRange:  "0", EndRange: "Inf", Unit: "Requests",
			PricePerUnit: map[string]string{"USD": "0.0000004000"}, AppliesTo: []string{},
		}},
	},
	{
		// The Deep Archive decoy. This is the *only* SKU in the real S3 offer
		// file matching productFamily=Storage + volumeType="Glacier Deep
		// Archive", and it prices staging storage at $0.021 — 21x the $0.00099 a
		// caller filtering that way is looking for.
		sku:           "EXB3YJ6YV5CRH4JN",
		serviceCode:   pricingServiceCodeS3,
		productFamily: "Storage",
		attributes: map[string]string{
			"availability": "99.99%", "durability": "99.999999999%",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"storageClass": "Archive", "usagetype": "TimedStorage-GDA-Staging",
			"volumeType": "Glacier Deep Archive",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "EXB3YJ6YV5CRH4JN.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.021 per GB-Month of storage used in GlacierStagingStorage",
			BeginRange:  "0", EndRange: "Inf", Unit: "GB-Mo",
			PricePerUnit: map[string]string{"USD": "0.0210000000"}, AppliesTo: []string{},
		}},
	},
	{
		// The $0.00099 rate, which lives under Intelligent-Tiering rather than
		// under any Glacier Deep Archive SKU. Included so a filter aiming for
		// $0.00099 can be tested for reaching the right SKU, not just the right
		// number.
		sku:           "82TFRVR9729PGTNP",
		serviceCode:   pricingServiceCodeS3,
		productFamily: "Storage",
		attributes: map[string]string{
			"availability": "N/A", "durability": "N/A",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"storageClass": "Intelligent-Tiering", "usagetype": "TimedStorage-INT-DAA-ByteHrs",
			"volumeType": "IntelligentTieringDeepArchiveAccess",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode: "82TFRVR9729PGTNP.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.00099 per Gigabyte Month for TimedStorage-INT-DAA-ByteHrs:" +
				"IntelligentTieringDAAStorage in US East (N. Virginia)",
			BeginRange: "0", EndRange: "Inf", Unit: "GB-Mo",
			PricePerUnit: map[string]string{"USD": "0.0009900000"}, AppliesTo: []string{},
		}},
	},
	{
		// Standard-IA, so that a storageClass filter has more than one candidate
		// and a test can prove it selected rather than took the only match.
		sku:           "379GYJUQ5WFBUYGM",
		serviceCode:   pricingServiceCodeS3,
		productFamily: "Storage",
		attributes: map[string]string{
			"availability": "99.9%", "durability": "99.999999999%",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"storageClass": "Infrequent Access", "usagetype": "TimedStorage-SIA-ByteHrs",
			"volumeType": "Standard - Infrequent Access",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "379GYJUQ5WFBUYGM.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.0125 per GB-Month of storage used in Standard-Infrequent Access",
			BeginRange:  "0", EndRange: "Inf", Unit: "GB-Mo",
			PricePerUnit: map[string]string{"USD": "0.0125000000"}, AppliesTo: []string{},
		}},
	},
	{
		// productFamily absent, which is the majority case in the real file. Note
		// it still has a usagetype, so a caller keying on usagetype finds it and
		// a caller filtering on productFamily does not.
		sku:         "WDAC2WRXVNRSNQS6",
		serviceCode: pricingServiceCodeS3,
		attributes: map[string]string{
			"group": "S3-API-GDA-Tier2", "groupDescription": "GET and all other requests to GDA",
			"location": "US East (N. Virginia)", "locationType": "AWS Region",
			"operation": "", "regionCode": "us-east-1",
			"servicecode": pricingServiceCodeS3, "servicename": "Amazon Simple Storage Service",
			"usagetype": "Requests-GDA-Tier2",
		},
		effectiveDate: "2026-07-01T00:00:00Z",
		dimensions: []pricingPriceDimension{{
			RateCode:    "WDAC2WRXVNRSNQS6.JRTCKXETXF.6YS6EN2CT7",
			Description: "$0.004 per 10,000 GET and all other requests from Glacier",
			BeginRange:  "0", EndRange: "Inf", Unit: "Requests",
			PricePerUnit: map[string]string{"USD": "0.0000004000"}, AppliesTo: []string{},
		}},
	},
}

// offerDoc assembles the entry into the document shape GetProducts serializes.
func (e pricingCorpusEntry) offerDoc() pricingOfferDoc {
	termKey := e.sku + "." + pricingOfferTermCode
	dims := make(map[string]pricingPriceDimension, len(e.dimensions))
	for _, d := range e.dimensions {
		dims[d.RateCode] = d
	}
	return pricingOfferDoc{
		Product: pricingProduct{
			SKU:           e.sku,
			ProductFamily: e.productFamily,
			Attributes:    e.attributes,
		},
		ServiceCode: e.serviceCode,
		Terms: map[string]map[string]pricingTerm{
			"OnDemand": {termKey: {
				SKU:             e.sku,
				OfferTermCode:   pricingOfferTermCode,
				EffectiveDate:   e.effectiveDate,
				PriceDimensions: dims,
				TermAttributes:  map[string]string{},
			}},
		},
		Version:         pricingOfferVersion,
		PublicationDate: pricingOfferPublicationDate,
	}
}

// pricingServiceAttributes lists the attribute names DescribeServices reports
// for a service code. Every name is one that actually appears in the offer
// corpus, so a caller can move from DescribeServices to GetAttributeValues to a
// GetProducts filter without hitting an attribute that yields nothing.
//
//nolint:gochecknoglobals // Immutable reference data, read-only after init.
var pricingServiceAttributes = map[string][]string{
	pricingServiceCodeS3: {
		"availability",
		"durability",
		"group",
		"groupDescription",
		"location",
		"locationType",
		"operation",
		"regionCode",
		"servicecode",
		"servicename",
		"storageClass",
		"usagetype",
		"volumeType",
	},
}
