package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// This file holds the seedable half of the Price List corpus (#1033).
//
// The bundled corpus answers "what does AWS charge": every SKU in it is copied
// from a real offer file, and it can never be grown to answer the other question a
// consumer has, "what does my code do when the rate is X". A fixture carrying an
// invented rate is worse than no rate at all, because a caller computing a cost
// from it is wrong with no way to notice. So an invented rate arrives the one way a
// deterministic emulator is allowed to produce a different answer: as a seed the
// caller wrote itself, over the control plane, in the request that caused it.
//
// Three properties the seed holds, each of which is a decision rather than an
// implementation detail:
//
//   - A seeded SKU is not a second code path. It becomes a [pricingCorpusEntry]
//     like any other, so GetProducts filtering, DescribeServices discovery and
//     GetAttributeValues all see it through the same functions that serve the
//     bundled corpus. An overlay that only reached GetProducts would let a caller
//     query a SKU that DescribeServices denies exists.
//   - A seed overlays; it never mutates. pricingCorpus, pricingServiceAttributes
//     and pricingOfferRevisions are read-only reference data by contract, and
//     CLAUDE.md forbids global mutable state independently of that.
//   - A seed that would emit a shape the real API never emits is refused. The
//     subsystem exists to reproduce the shapes that surprise a caller, so a seed
//     with a numeric pricePerUnit or a rate code that does not match its key would
//     let a consumer's parser pass here and fail against AWS — the exact failure
//     the corpus was built to catch.
const pricingSeededOfferPrefix = "offer:"

// pricingSeededOfferKey is the state key holding one seeded offer document.
//
// The prefix is not "failure:" for a mechanical reason: a bare
// DELETE /v1/pricing/query-failures lists that prefix and deletes everything it
// finds, so a seeded offer stored under it would be swept away by clearing an
// unrelated seeded error.
func pricingSeededOfferKey(sku string) string { return pricingSeededOfferPrefix + sku }

// pricingSeedSyntheticVersion and pricingSeedSyntheticPublicationDate are the
// revision a seeded document reports when it carries none of its own.
//
// They are deliberately not plausible. Every real offer file reports a 14-digit
// version and an RFC3339 publication date, and a seeded product for a service
// substrate does not bundle has no revision to report; minting a realistic-looking
// one would let a caller believe it was reading a published offer file. A seeded
// SKU for a service that *is* bundled does not inherit that service's real
// revision either, for the same reason — the measured offer file does not contain
// the seeded product.
const (
	pricingSeedSyntheticVersion         = "substrate-seeded"
	pricingSeedSyntheticPublicationDate = "1970-01-01T00:00:00Z"
)

// pricingSeedTermsKey is the only terms key a seed may carry.
//
// A pricingCorpusEntry holds exactly one term, so a Reserved term could not be
// rendered even if it were stored; refusing it names the limitation where a caller
// will see it rather than dropping the term and serving a document that is missing
// half of what was seeded.
const pricingSeedTermsKey = "OnDemand"

// pricingValidateSeededOffer checks a seeded offer document and returns the corpus
// entry it becomes, or a message naming what was wrong.
//
// The message is the whole point of the validation: a seed is a test's own input,
// and a refusal a caller can act on is worth more than a stored document that
// serves a shape AWS never emits.
func pricingValidateSeededOffer(doc pricingOfferDoc) (pricingCorpusEntry, string) {
	sku := doc.Product.SKU
	switch {
	case sku == "":
		return pricingCorpusEntry{}, "product.sku is required"
	case doc.ServiceCode == "":
		return pricingCorpusEntry{}, "serviceCode is required"
	case doc.Product.Attributes["usagetype"] == "":
		// usagetype is the one attribute present on every product in both offer
		// files substrate bundles, and it is what a consumer's filter reaches for.
		return pricingCorpusEntry{}, "product.attributes.usagetype is required"
	}

	term, termKey, msg := pricingSeededOfferTerm(doc, sku)
	if msg != "" {
		return pricingCorpusEntry{}, msg
	}
	termCode := strings.TrimPrefix(termKey, sku+".")
	if term.SKU != sku {
		return pricingCorpusEntry{}, fmt.Sprintf(
			"terms.OnDemand[%q].sku is %q, which is not the product's sku %q",
			termKey, term.SKU, sku)
	}
	if term.EffectiveDate == "" {
		return pricingCorpusEntry{}, fmt.Sprintf("terms.OnDemand[%q].effectiveDate is required", termKey)
	}

	dims, msg := pricingSeededOfferDimensions(term, termKey, sku, termCode)
	if msg != "" {
		return pricingCorpusEntry{}, msg
	}

	return pricingCorpusEntry{
		sku:             sku,
		serviceCode:     doc.ServiceCode,
		productFamily:   doc.Product.ProductFamily,
		attributes:      doc.Product.Attributes,
		effectiveDate:   term.EffectiveDate,
		offerTermCode:   termCode,
		termAttributes:  term.TermAttributes,
		dimensions:      dims,
		version:         doc.Version,
		publicationDate: doc.PublicationDate,
	}, ""
}

// pricingSeededOfferTerm picks the single on-demand term out of a seeded document,
// checking that its key is the "<sku>.<offerTermCode>" AWS emits.
func pricingSeededOfferTerm(doc pricingOfferDoc, sku string) (pricingTerm, string, string) {
	for key := range doc.Terms {
		if key != pricingSeedTermsKey {
			return pricingTerm{}, "", fmt.Sprintf(
				"terms.%s is not modeled; substrate serves one %s term per SKU",
				key, pricingSeedTermsKey)
		}
	}
	onDemand := doc.Terms[pricingSeedTermsKey]
	if len(onDemand) != 1 {
		return pricingTerm{}, "", fmt.Sprintf(
			"terms.%s must carry exactly one term, got %d", pricingSeedTermsKey, len(onDemand))
	}
	for key, term := range onDemand {
		code := strings.TrimPrefix(key, sku+".")
		if code == key || code == "" {
			return pricingTerm{}, "", fmt.Sprintf(
				"terms.%s key %q is not %q; AWS keys a term by \"<sku>.<offerTermCode>\"",
				pricingSeedTermsKey, key, sku+".<offerTermCode>")
		}
		return term, key, ""
	}
	// Unreachable: the length check above admits exactly one iteration.
	return pricingTerm{}, "", "terms." + pricingSeedTermsKey + " must carry exactly one term"
}

// pricingSeededOfferDimensions checks a term's price dimensions and returns them
// sorted by rate code, so that a seed stored from a map renders in a stable order.
func pricingSeededOfferDimensions(
	term pricingTerm, termKey, sku, termCode string,
) ([]pricingPriceDimension, string) {
	if len(term.PriceDimensions) == 0 {
		return nil, fmt.Sprintf("terms.OnDemand[%q].priceDimensions is required", termKey)
	}
	dims := make([]pricingPriceDimension, 0, len(term.PriceDimensions))
	for key, d := range term.PriceDimensions {
		want := sku + "." + termCode + "."
		if !strings.HasPrefix(key, want) || key == want {
			return nil, fmt.Sprintf(
				"priceDimensions key %q is not %q; AWS keys a dimension by "+
					"\"<sku>.<offerTermCode>.<dimensionCode>\"", key, want+"<dimensionCode>")
		}
		if d.RateCode != key {
			return nil, fmt.Sprintf(
				"priceDimensions[%q].rateCode is %q; AWS repeats the key as the rate code",
				key, d.RateCode)
		}
		if d.Unit == "" {
			return nil, fmt.Sprintf("priceDimensions[%q].unit is required", key)
		}
		if len(d.PricePerUnit) == 0 {
			return nil, fmt.Sprintf("priceDimensions[%q].pricePerUnit is required", key)
		}
		for currency, price := range d.PricePerUnit {
			if _, err := strconv.ParseFloat(price, 64); err != nil {
				return nil, fmt.Sprintf(
					"priceDimensions[%q].pricePerUnit[%q] is %q, which is not a decimal string",
					key, currency, price)
			}
		}
		dims = append(dims, d)
	}
	sort.Slice(dims, func(i, j int) bool { return dims[i].RateCode < dims[j].RateCode })
	return dims, ""
}

// pricingBundledSKU reports whether the bundled corpus carries a SKU, which is
// what decides whether a seed needs ?replace=true.
func pricingBundledSKU(sku string) bool {
	for _, e := range pricingCorpus {
		if e.sku == sku {
			return true
		}
	}
	return false
}

// seededOffers returns the seeded corpus entries, sorted by SKU.
//
// A record that cannot be decoded is skipped and logged rather than failing the
// request. Everything the seed endpoint stores has already been validated, so an
// unreadable record was not written by that endpoint; turning it into a 500 would
// break every pricing call in the process for a seed the caller may not know
// exists, where skipping it leaves the bundled corpus answering as it always does.
func (p *PriceListPlugin) seededOffers() []pricingCorpusEntry {
	if p.state == nil {
		return nil
	}
	keys, err := p.state.List(context.Background(), pricingCtrlNamespace, pricingSeededOfferPrefix)
	if err != nil {
		p.logger.Warn("pricing: list seeded offers", "err", err)
		return nil
	}
	sort.Strings(keys)
	entries := make([]pricingCorpusEntry, 0, len(keys))
	for _, key := range keys {
		raw, getErr := p.state.Get(context.Background(), pricingCtrlNamespace, key)
		if getErr != nil || raw == nil {
			continue
		}
		var doc pricingOfferDoc
		if unmarshalErr := json.Unmarshal(raw, &doc); unmarshalErr != nil {
			p.logger.Warn("pricing: decode seeded offer", "key", key, "err", unmarshalErr)
			continue
		}
		entry, msg := pricingValidateSeededOffer(doc)
		if msg != "" {
			p.logger.Warn("pricing: seeded offer is not servable", "key", key, "reason", msg)
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

// entriesFor returns the corpus entries for a service code, seeds included.
//
// A service code neither bundled nor seeded is a NotFoundException. Substrate's
// corpus is far smaller than AWS's catalog, so a caller asking for a real service
// substrate does not carry gets a loud error rather than an empty PriceList that
// reads as "AWS has no such price". A false alarm is visible; a false empty is not.
//
// A seeded SKU shadows a bundled one of the same SKU, which is only reachable
// because the seed endpoint required ?replace=true to store it. Seeds follow the
// bundled entries and are sorted among themselves by SKU: pricingCorpus is
// ordered deliberately so that PriceList order and NextToken paging are stable,
// and a map-ordered overlay would undo that for every seeded query.
func (p *PriceListPlugin) entriesFor(serviceCode string) ([]pricingCorpusEntry, error) {
	seeds := p.seededOffers()
	shadowed := make(map[string]bool, len(seeds))
	mine := make([]pricingCorpusEntry, 0, len(seeds))
	seededCode := false
	for _, e := range seeds {
		shadowed[e.sku] = true
		if e.serviceCode == serviceCode {
			seededCode = true
			mine = append(mine, e)
		}
	}
	_, bundledCode := pricingServiceAttributes[serviceCode]
	if !bundledCode && !seededCode {
		return nil, pricingError(pricingErrNotFound,
			"no offer data for ServiceCode "+serviceCode+
				"; substrate's corpus covers "+strings.Join(p.serviceCodes(), ", "))
	}
	out := make([]pricingCorpusEntry, 0, len(pricingCorpus)+len(mine))
	for _, e := range pricingCorpus {
		if e.serviceCode == serviceCode && !shadowed[e.sku] {
			out = append(out, e)
		}
	}
	return append(out, mine...), nil
}

// serviceAttributes returns the attribute names DescribeServices reports, seeded
// services included.
//
// A seeded service's names are computed rather than declared — the union of its
// seeded products' attribute keys — because the property the bundled lists hold is
// that every name they report filters to at least one product, and a declared list
// could not hold it for a document substrate has not seen. A service that is both
// bundled and seeded reports the union, sorted; a service with no seeds keeps its
// bundled list untouched, order included.
func (p *PriceListPlugin) serviceAttributes() map[string][]string {
	seeds := p.seededOffers()
	if len(seeds) == 0 {
		return pricingServiceAttributes
	}
	added := make(map[string]map[string]bool)
	for _, e := range seeds {
		if added[e.serviceCode] == nil {
			added[e.serviceCode] = make(map[string]bool)
		}
		for name := range e.attributes {
			added[e.serviceCode][name] = true
		}
		if e.productFamily != "" {
			added[e.serviceCode][pricingProductFamilyField] = true
		}
	}
	out := make(map[string][]string, len(pricingServiceAttributes)+len(added))
	for code, names := range pricingServiceAttributes {
		out[code] = names
	}
	for code, names := range added {
		union := make(map[string]bool, len(names)+len(out[code]))
		for _, name := range out[code] {
			union[name] = true
		}
		for name := range names {
			union[name] = true
		}
		merged := make([]string, 0, len(union))
		for name := range union {
			merged = append(merged, name)
		}
		sort.Strings(merged)
		out[code] = merged
	}
	return out
}

// serviceCodes returns the sorted service codes substrate answers for, seeded
// services included.
func (p *PriceListPlugin) serviceCodes() []string {
	attrs := p.serviceAttributes()
	codes := make([]string, 0, len(attrs))
	for code := range attrs {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// hasAttribute reports whether a service declares an attribute name, reading the
// same overlay DescribeServices reports from — so GetAttributeValues cannot refuse
// a name a seeded product carries.
func (p *PriceListPlugin) hasAttribute(serviceCode, name string) bool {
	for _, a := range p.serviceAttributes()[serviceCode] {
		if a == name {
			return true
		}
	}
	return false
}
