package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// orgCtrlNamespace is the state namespace for Organizations control-plane (seed)
// data. It is separate from the organizations namespace so a seed is not
// mistaken for organization state during replay or a state dump.
const orgCtrlNamespace = "organizations-ctrl"

// orgCtrlFeatureSetKey holds the seeded organization feature set.
const orgCtrlFeatureSetKey = "feature-set"

// orgCtrlCreateFailureKey returns the state key for a seeded CreateAccount
// failure. Name-scoped seeds use "create-account-failure:{name}"; the wildcard
// uses "create-account-failure:*".
func orgCtrlCreateFailureKey(accountName string) string {
	if accountName == "" {
		accountName = "*"
	}
	return "create-account-failure:" + accountName
}

// orgCreateAccountFailureReasons is the CreateAccountFailureReason enum from the
// Organizations API model. A seed is checked against it because a typo'd reason
// would produce a FAILED status carrying a value no SDK catch branch matches,
// so the caller's fallback path would go untested while the test still passed.
var orgCreateAccountFailureReasons = []string{
	"ACCOUNT_LIMIT_EXCEEDED",
	"EMAIL_ALREADY_EXISTS",
	"INVALID_ADDRESS",
	"INVALID_EMAIL",
	"CONCURRENT_ACCOUNT_MODIFICATION",
	"INTERNAL_FAILURE",
	"GOVCLOUD_ACCOUNT_ALREADY_EXISTS",
	"MISSING_BUSINESS_VALIDATION",
	"FAILED_BUSINESS_VALIDATION",
	"PENDING_BUSINESS_VALIDATION",
	"INVALID_IDENTITY_FOR_BUSINESS_VALIDATION",
	"UNKNOWN_BUSINESS_VALIDATION",
	"MISSING_PAYMENT_INSTRUMENT",
	"INVALID_PAYMENT_INSTRUMENT",
	"UPDATE_EXISTING_RESOURCE_POLICY_WITH_TAGS_NOT_SUPPORTED",
}

// orgSeededFeatureSet is a seeded organization feature set.
type orgSeededFeatureSet struct {
	// FeatureSet is "ALL" or "CONSOLIDATED_BILLING".
	FeatureSet string `json:"featureSet"`
}

// orgSeededCreateFailure is a seeded asynchronous CreateAccount outcome.
// Substrate does not create anything; the seed sets the terminal state that
// DescribeCreateAccountStatus reports, which is the only place a real
// CreateAccount failure is observable — the call itself still returns 200.
type orgSeededCreateFailure struct {
	// AccountName the seed applies to, or "*" for any account name.
	AccountName string `json:"accountName"`

	// FailureReason is the CreateAccountFailureReason the status reports.
	FailureReason string `json:"failureReason"`
}

// effectiveFeatureSet returns the organization's feature set: the seeded value
// when one is set, the stored value for an existing organization, and "ALL"
// otherwise. The seed wins over the stored value so a test can flip an already
// observed organization into CONSOLIDATED_BILLING without recreating it.
func (p *OrganizationsPlugin) effectiveFeatureSet(ctx context.Context, acct string) (string, error) {
	var seed orgSeededFeatureSet
	found, err := p.orgCtrlGetJSON(ctx, orgCtrlFeatureSetKey, &seed)
	if err != nil {
		return "", err
	}
	if found && seed.FeatureSet != "" {
		return seed.FeatureSet, nil
	}

	var org Organization
	found, err = p.orgGetJSON(ctx, orgKey(acct), &org)
	if err != nil {
		return "", err
	}
	if found && org.FeatureSet != "" {
		return org.FeatureSet, nil
	}
	return orgFeatureSetAll, nil
}

// resolveSeededCreateFailure returns the seeded failure for an account name, if
// any, matching the exact name first then the "*" wildcard. It returns
// (nil, nil) when no seed applies, so the caller falls back to the nominal
// SUCCEEDED outcome.
func (p *OrganizationsPlugin) resolveSeededCreateFailure(ctx context.Context, accountName string) (*orgSeededCreateFailure, error) {
	for _, key := range []string{orgCtrlCreateFailureKey(accountName), orgCtrlCreateFailureKey("*")} {
		var seed orgSeededCreateFailure
		found, err := p.orgCtrlGetJSON(ctx, key, &seed)
		if err != nil {
			return nil, err
		}
		if found {
			return &seed, nil
		}
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// orgCtrlGetJSON loads and decodes a control-plane value, reporting found=false
// when the key is absent.
func (p *OrganizationsPlugin) orgCtrlGetJSON(ctx context.Context, key string, out interface{}) (bool, error) {
	data, err := p.state.Get(ctx, orgCtrlNamespace, key)
	if err != nil {
		return false, fmt.Errorf("get %s: %w", key, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return true, nil
}

// handleOrganizationsSeedFeatureSet handles POST /v1/organizations/feature-set.
// It sets the feature set DescribeOrganization reports, which decides whether
// service control policies exist at all: under CONSOLIDATED_BILLING the root
// has no policy types and every policy operation is refused with
// PolicyTypeNotAvailableForOrganizationException.
// Body: {"featureSet":"ALL"|"CONSOLIDATED_BILLING"}.
func (s *Server) handleOrganizationsSeedFeatureSet(w http.ResponseWriter, r *http.Request) {
	var seed orgSeededFeatureSet
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.FeatureSet != orgFeatureSetAll && seed.FeatureSet != orgFeatureSetConsolidatedBilling {
		http.Error(w, `{"error":"featureSet must be ALL or CONSOLIDATED_BILLING"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), orgCtrlNamespace, orgCtrlFeatureSetKey, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "featureSet": seed.FeatureSet})
}

// handleOrganizationsClearFeatureSet handles DELETE /v1/organizations/feature-set,
// restoring the organization's stored feature set.
func (s *Server) handleOrganizationsClearFeatureSet(w http.ResponseWriter, r *http.Request) {
	if err := s.state.Delete(r.Context(), orgCtrlNamespace, orgCtrlFeatureSetKey); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}

// handleOrganizationsSeedCreateAccountFailure handles
// POST /v1/organizations/create-account-failure. It seeds the terminal state of
// the asynchronous CreateAccount request for a given account name (default "*"):
// the call still returns 200 with IN_PROGRESS, and DescribeCreateAccountStatus
// then reports FAILED with the seeded reason and no AccountId.
// Body: {"accountName","failureReason"}.
func (s *Server) handleOrganizationsSeedCreateAccountFailure(w http.ResponseWriter, r *http.Request) {
	var seed orgSeededCreateFailure
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.FailureReason == "" {
		http.Error(w, `{"error":"failureReason is required"}`, http.StatusBadRequest)
		return
	}
	if !slices.Contains(orgCreateAccountFailureReasons, seed.FailureReason) {
		writeJSONErrorDebug(w, http.StatusBadRequest, "failureReason %q is not a CreateAccountFailureReason",
			seed.FailureReason)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), orgCtrlNamespace, orgCtrlCreateFailureKey(seed.AccountName), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "accountName": orgCtrlCreateFailureKey(seed.AccountName)})
}

// handleOrganizationsClearCreateAccountFailure handles
// DELETE /v1/organizations/create-account-failure. With ?accountName=... it
// removes that seed; without it removes all.
func (s *Server) handleOrganizationsClearCreateAccountFailure(w http.ResponseWriter, r *http.Request) {
	if name := r.URL.Query().Get("accountName"); name != "" {
		if err := s.state.Delete(r.Context(), orgCtrlNamespace, orgCtrlCreateFailureKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), orgCtrlNamespace, "create-account-failure:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), orgCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
