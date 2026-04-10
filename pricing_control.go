package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handlePricingRefresh handles POST /v1/pricing/refresh.
// It triggers a pricing data refresh from the configured provider.
func (s *Server) handlePricingRefresh(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	provider := s.opts.Costs.GetPricingProvider()
	if err := provider.Refresh(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{
		"status":   "ok",
		"source":   provider.Source(),
		"cacheAge": provider.CacheAge().String(),
	})
}

// handlePricingGet handles GET /v1/pricing.
// It returns the current pricing source, cache age, and configuration.
func (s *Server) handlePricingGet(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	provider := s.opts.Costs.GetPricingProvider()
	writeJSONDebug(w, s.logger, map[string]interface{}{
		"source":   provider.Source(),
		"cacheAge": provider.CacheAge().String(),
	})
}

// handlePricingLookup handles GET /v1/pricing/lookup?service=s3&operation=PutObject.
// It returns the base price for the specified service/operation.
func (s *Server) handlePricingLookup(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	service := r.URL.Query().Get("service")
	operation := r.URL.Query().Get("operation")
	if service == "" {
		http.Error(w, `{"error":"service parameter required"}`, http.StatusBadRequest)
		return
	}
	price := s.opts.Costs.PriceFor(service, operation)
	writeJSONDebug(w, s.logger, map[string]interface{}{
		"service":   service,
		"operation": operation,
		"price":     price,
	})
}

// handlePricingSetDiscounts handles POST /v1/pricing/discounts.
// It replaces the current discount configuration.
func (s *Server) handlePricingSetDiscounts(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	var cfg DiscountConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	s.opts.Costs.SetDiscounts(cfg)
	writeJSONDebug(w, s.logger, map[string]interface{}{"status": "ok"})
}

// handlePricingGetDiscounts handles GET /v1/pricing/discounts.
// It returns the current discount configuration.
func (s *Server) handlePricingGetDiscounts(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	cfg := s.opts.Costs.GetDiscounts()
	writeJSONDebug(w, s.logger, cfg)
}

// handlePricingClearDiscounts handles DELETE /v1/pricing/discounts.
// It clears all discount configuration.
func (s *Server) handlePricingClearDiscounts(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	s.opts.Costs.SetDiscounts(DiscountConfig{})
	writeJSONDebug(w, s.logger, map[string]interface{}{"status": "ok"})
}

// handlePricingAddCredit handles POST /v1/pricing/credits.
// It adds a new credit. Body: {"description":"...", "amount":100.0, "services":["s3"], "expiresAt":"..."}.
func (s *Server) handlePricingAddCredit(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	var cr Credit
	if err := json.NewDecoder(r.Body).Decode(&cr); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if cr.Amount <= 0 {
		http.Error(w, `{"error":"amount must be positive"}`, http.StatusBadRequest)
		return
	}
	id := s.opts.Costs.AddCredit(cr)
	writeJSONDebug(w, s.logger, map[string]interface{}{"status": "ok", "id": id})
}

// handlePricingListCredits handles GET /v1/pricing/credits.
// It returns all credits with their remaining balances.
func (s *Server) handlePricingListCredits(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	credits := s.opts.Costs.ListCredits()
	writeJSONDebug(w, s.logger, map[string]interface{}{"credits": credits})
}

// handlePricingRemoveCredit handles DELETE /v1/pricing/credits/{id}.
// It removes a credit by ID.
func (s *Server) handlePricingRemoveCredit(w http.ResponseWriter, r *http.Request) {
	if s.opts.Costs == nil {
		http.Error(w, `{"error":"cost tracking not enabled"}`, http.StatusNotImplemented)
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, `{"error":"credit id required"}`, http.StatusBadRequest)
		return
	}
	if !s.opts.Costs.RemoveCredit(id) {
		http.Error(w, `{"error":"credit not found"}`, http.StatusNotFound)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"status": "ok"})
}
