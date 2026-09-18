package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// handlePricingSeedOffer handles POST /v1/pricing/offers.
//
// The body is one offer document in the shape GetProducts serves it — `product`,
// `serviceCode`, `terms`, and optionally `version` and `publicationDate` — so a
// seed can be pasted straight out of a real offer file or out of a recorded
// PriceList element. The replace flag is a query parameter rather than a body
// member for that reason: a body member would make the document no longer a
// document.
//
//	curl -X POST http://localhost:4566/v1/pricing/offers -d @offer.json
//	curl -X POST 'http://localhost:4566/v1/pricing/offers?replace=true' -d @offer.json
//
// A SKU the bundled corpus carries is refused unless replace=true. The corpus
// exists so that a rate a consumer computes against is a measured one, and a
// measured rate that vanishes under a seed nobody remembers writing is how a
// caller ends up asserting against an invented number. Requiring the flag does not
// prevent the override — it puts it in the request that caused it. A seed may
// always replace an earlier seed, which is what makes the endpoint re-runnable.
func (s *Server) handlePricingSeedOffer(w http.ResponseWriter, r *http.Request) {
	replace := false
	if raw := r.URL.Query().Get("replace"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			writeJSONErrorDebug(w, http.StatusBadRequest,
				"replace must be a boolean, got %q", raw)
			return
		}
		replace = parsed
	}

	var doc pricingOfferDoc
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		// A numeric pricePerUnit lands here rather than in the validator, because
		// the field is typed map[string]string — and that is the single most likely
		// way a hand-written seed diverges from the real API, which emits every
		// rate as a string. The decode error names the member, so it is reported
		// rather than replaced with a generic refusal.
		writeJSONErrorDebug(w, http.StatusBadRequest,
			"malformed offer document: %s", err.Error())
		return
	}

	entry, msg := pricingValidateSeededOffer(doc)
	if msg != "" {
		writeJSONErrorDebug(w, http.StatusBadRequest, "invalid offer document: %s", msg)
		return
	}
	if pricingBundledSKU(entry.sku) && !replace {
		writeJSONErrorDebug(w, http.StatusBadRequest,
			"SKU %s is in substrate's bundled corpus, whose rates are measured from real offer "+
				"files; pass ?replace=true to override it deliberately", entry.sku)
		return
	}

	if doc.Version == "" {
		doc.Version = pricingSeedSyntheticVersion
	}
	if doc.PublicationDate == "" {
		doc.PublicationDate = pricingSeedSyntheticPublicationDate
	}
	stored, err := json.Marshal(doc)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), pricingCtrlNamespace,
		pricingSeededOfferKey(entry.sku), stored); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{
		"ok":          true,
		"sku":         entry.sku,
		"serviceCode": entry.serviceCode,
		"version":     doc.Version,
	})
}

// handlePricingClearOffers handles DELETE /v1/pricing/offers.
// With ?sku=... it removes that one seeded offer; without a query param it removes
// every seeded offer and leaves the bundled corpus as it was, which is the only
// state the corpus ever has.
func (s *Server) handlePricingClearOffers(w http.ResponseWriter, r *http.Request) {
	if sku := r.URL.Query().Get("sku"); sku != "" {
		if err := s.state.Delete(r.Context(), pricingCtrlNamespace,
			pricingSeededOfferKey(sku)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "sku": sku})
		return
	}
	keys, err := s.state.List(r.Context(), pricingCtrlNamespace, pricingSeededOfferPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), pricingCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "cleared": len(keys)})
}
