package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/go-chi/chi/v5"
)

// AWS Config control-plane seeds (#580).
//
// Substrate delivers nothing to S3 and evaluates no rules, so the outcomes a
// consumer's error handling exists for — a recorder reporting Failure, a delivery
// stream that cannot write, a bucket policy substrate cannot judge — cannot be
// produced by any sequence of API calls. Seeding is how a deterministic emulator
// reaches them: the value is pinned here, read at request time, and the same on
// every replay.
//
// The seeds:
//
//	POST/DELETE /v1/config/recorder-status          lastStatus + error code/message
//	POST/DELETE /v1/config/delivery-policy          force or suppress the bucket-policy refusal
//	POST/DELETE /v1/config/delivery-status          the DeliveryStatus a channel reports
//	POST/DELETE /v1/config/rule-compliance/{name}   a Config rule's compliance verdict
//
// Seeds live in their own namespace so a seeded status is never mistaken for a real
// one in a state dump or during replay, and each is applied at *read* time rather
// than written into the resource — so clearing a seed restores the real state
// instead of leaving the seeded value behind.

// configServiceCtrlNamespace is the state namespace for AWS Config control-plane
// (seed) data, separate from configServiceNamespace so a seed is not mistaken for
// real recorder or channel state.
const configServiceCtrlNamespace = "config-ctrl"

// Delivery statuses, the DeliveryStatus enum. Note the spelling of the third:
// Not_Applicable, with an underscore and a capital A, which is how the model spells
// this member and not how it spells the RecorderStatus member NotApplicable.
const (
	// cfgsvcDeliverySuccess is a stream whose last delivery succeeded.
	cfgsvcDeliverySuccess = "Success"

	// cfgsvcDeliveryFailure is a stream whose last delivery failed.
	cfgsvcDeliveryFailure = "Failure"

	// cfgsvcDeliveryNotApplicable is a stream that has never delivered.
	cfgsvcDeliveryNotApplicable = "Not_Applicable"
)

// cfgsvcDeliveryStatuses is the DeliveryStatus enum, used to validate a seed.
var cfgsvcDeliveryStatuses = []string{
	cfgsvcDeliverySuccess,
	cfgsvcDeliveryFailure,
	cfgsvcDeliveryNotApplicable,
}

// Delivery-policy seed outcomes.
const (
	// cfgsvcDeliveryOutcomeOK suppresses the bucket-policy refusal.
	cfgsvcDeliveryOutcomeOK = "ok"

	// cfgsvcDeliveryOutcomeInsufficient forces InsufficientDeliveryPolicyException.
	cfgsvcDeliveryOutcomeInsufficient = "insufficient"
)

// cfgsvcDeliveryOutcomes is the set of values the delivery-policy seed accepts.
var cfgsvcDeliveryOutcomes = []string{cfgsvcDeliveryOutcomeOK, cfgsvcDeliveryOutcomeInsufficient}

// cfgsvcCtrlRecorderStatusKey returns the state key for a seeded recorder status,
// scoped by account and Region like the recorder it describes. The wildcard forms
// are "*" in either position, so a single-account fixture can seed without knowing
// its own account ID.
func cfgsvcCtrlRecorderStatusKey(accountID, region string) string {
	return "recorder_status:" + cfgsvcCtrlScope(accountID, region)
}

// cfgsvcCtrlDeliveryStatusKey returns the state key for a seeded delivery status.
func cfgsvcCtrlDeliveryStatusKey(accountID, region string) string {
	return "delivery_status:" + cfgsvcCtrlScope(accountID, region)
}

// cfgsvcCtrlDeliveryPolicyKey returns the state key for a seeded delivery-policy
// outcome, keyed by bucket name because that is what the check is about — a fixture
// with two buckets needs to seed them separately.
func cfgsvcCtrlDeliveryPolicyKey(bucket string) string {
	if bucket == "" {
		bucket = "*"
	}
	return "delivery_policy:" + bucket
}

// cfgsvcCtrlRuleComplianceKey returns the state key for a seeded Config-rule
// verdict, keyed by rule name within an account and Region.
//
// The rule name takes the "*" wildcard like the account and Region do, so a fixture
// can pin one default for every rule — which is how a consumer asserts "nothing is
// compliant yet" over a stack whose rule names it does not enumerate.
func cfgsvcCtrlRuleComplianceKey(accountID, region, rule string) string {
	if rule == "" {
		rule = "*"
	}
	return "rule_compliance:" + cfgsvcCtrlScope(accountID, region) + "/" + rule
}

// cfgsvcCtrlScope builds the account/region component of a seed key, defaulting
// either half to the "*" wildcard.
func cfgsvcCtrlScope(accountID, region string) string {
	if accountID == "" {
		accountID = "*"
	}
	if region == "" {
		region = "*"
	}
	return accountID + "/" + region
}

// cfgsvcSeededRecorderStatus is a pinned configuration-recorder status.
//
// Substrate delivers nothing, so a recorder it started reports Success and can never
// report Failure on its own. A consumer whose code branches on lastStatus — the
// branch that pages someone — needs this to reach that branch at all.
type cfgsvcSeededRecorderStatus struct {
	// AccountID the seed applies to, or "*" for any account.
	AccountID string `json:"accountId"`

	// Region the seed applies to, or "*" for any Region.
	Region string `json:"region"`

	// LastStatus is the RecorderStatus every observation reports.
	LastStatus string `json:"lastStatus"`

	// LastErrorCode is reported alongside it, for a Failure.
	LastErrorCode string `json:"lastErrorCode,omitempty"`

	// LastErrorMessage is reported alongside it, for a Failure.
	LastErrorMessage string `json:"lastErrorMessage,omitempty"`
}

// cfgsvcSeededDeliveryStatus is a pinned delivery-channel status, for the same
// reason: nothing here can fail a delivery.
type cfgsvcSeededDeliveryStatus struct {
	// AccountID the seed applies to, or "*" for any account.
	AccountID string `json:"accountId"`

	// Region the seed applies to, or "*" for any Region.
	Region string `json:"region"`

	// Status is the DeliveryStatus every stream reports.
	Status string `json:"status"`

	// LastErrorCode is reported alongside it, for a Failure.
	LastErrorCode string `json:"lastErrorCode,omitempty"`

	// LastErrorMessage is reported alongside it, for a Failure.
	LastErrorMessage string `json:"lastErrorMessage,omitempty"`
}

// cfgsvcSeededDeliveryPolicy forces or suppresses the bucket-policy refusal.
//
// It exists in both directions on purpose. A consumer with no S3 fixture needs
// "insufficient" without creating a bucket at all; a consumer whose policy is valid
// in a form substrate's permissive matcher still cannot read needs "ok" without
// having to argue with the matcher.
type cfgsvcSeededDeliveryPolicy struct {
	// Bucket the seed applies to, or "*" for any bucket.
	Bucket string `json:"bucket"`

	// Outcome is "ok" or "insufficient".
	Outcome string `json:"outcome"`
}

// cfgsvcSeededRuleCompliance is a pinned Config-rule verdict.
//
// Evaluating a rule is workload-internal — running a Guard policy or a Lambda
// against resource state — so substrate never computes a verdict and this is the
// only way one is ever anything other than INSUFFICIENT_DATA. See the cluster
// comment in configservice_rules.go for why that default is the right one.
type cfgsvcSeededRuleCompliance struct {
	// AccountID the seed applies to, or "*" for any account.
	AccountID string `json:"accountId"`

	// Region the seed applies to, or "*" for any Region.
	Region string `json:"region"`

	// ConfigRuleName the seed applies to, or "*" for any rule.
	ConfigRuleName string `json:"configRuleName"`

	// ComplianceType is the verdict the rule reports.
	ComplianceType string `json:"complianceType"`

	// Annotation is carried onto each synthesized evaluation result.
	Annotation string `json:"annotation,omitempty"`

	// Resources are the resources the verdict is attributed to, and may be empty.
	//
	// With none, the seed answers DescribeComplianceByConfigRule and
	// GetComplianceDetailsByConfigRule reports an empty list — a rule-level verdict
	// with no per-resource results, which is what AWS returns for a rule whose
	// evaluation produced none. Naming resources is what makes the details operation
	// worth calling.
	Resources []cfgsvcSeededResource `json:"resources,omitempty"`
}

// cfgsvcSeededResource is one resource a seeded verdict is attributed to.
type cfgsvcSeededResource struct {
	// ResourceType is the resource's AWS type, e.g. AWS::S3::Bucket.
	ResourceType string `json:"resourceType"`

	// ResourceID is the resource's identifier.
	ResourceID string `json:"resourceId"`
}

// seededRuleCompliance returns the pinned verdict for a rule, trying the rule's own
// name before the "*" rule wildcard and, within each, the account/Region candidates
// most specific first.
//
// Rule name is the outer loop because it is the more specific axis: a fixture that
// seeds one rule NON_COMPLIANT and every other rule COMPLIANT means the named seed to
// win, whatever account scope either carries.
func (p *ConfigServicePlugin) seededRuleCompliance(ctx context.Context, accountID, region, rule string) (
	seed cfgsvcSeededRuleCompliance, found bool, err error) {
	for _, name := range []string{rule, "*"} {
		build := func(a, r string) string { return cfgsvcCtrlRuleComplianceKey(a, r, name) }
		for _, key := range cfgsvcCtrlKeyCandidates(build, accountID, region) {
			data, getErr := p.state.Get(ctx, configServiceCtrlNamespace, key)
			if getErr != nil {
				return seed, false, fmt.Errorf("config seededRuleCompliance %s: %w", key, getErr)
			}
			if data == nil {
				continue
			}
			if err := json.Unmarshal(data, &seed); err != nil {
				return seed, false, fmt.Errorf("config seededRuleCompliance %s unmarshal: %w", key, err)
			}
			if seed.ComplianceType != "" {
				return seed, true, nil
			}
		}
		if rule == "*" {
			break
		}
	}
	return cfgsvcSeededRuleCompliance{}, false, nil
}

// cfgsvcClearRuleCompliance removes every seed naming one rule, which
// DeleteConfigRule does so a rebuilt rule of the same name starts at
// INSUFFICIENT_DATA rather than inheriting its predecessor's verdict.
//
// The "*" seed is deliberately left alone: it is a fixture-wide default rather than
// this rule's state, and deleting a rule should not silently change what every other
// rule reports.
func (p *ConfigServicePlugin) cfgsvcClearRuleCompliance(ctx context.Context, reqCtx *RequestContext,
	rule string) error {
	build := func(a, r string) string { return cfgsvcCtrlRuleComplianceKey(a, r, rule) }
	for _, key := range cfgsvcCtrlKeyCandidates(build, reqCtx.AccountID, reqCtx.Region) {
		if err := p.state.Delete(ctx, configServiceCtrlNamespace, key); err != nil {
			return fmt.Errorf("config clear rule compliance %s: %w", key, err)
		}
	}
	return nil
}

// seededRecorderStatus returns the pinned recorder status, matching the exact
// account and Region first, then the wildcards. found=false leaves the caller on the
// stored record.
func (p *ConfigServicePlugin) seededRecorderStatus(ctx context.Context, accountID, region string) (
	seed cfgsvcSeededRecorderStatus, found bool, err error) {
	for _, key := range cfgsvcCtrlKeyCandidates(cfgsvcCtrlRecorderStatusKey, accountID, region) {
		data, getErr := p.state.Get(ctx, configServiceCtrlNamespace, key)
		if getErr != nil {
			return seed, false, fmt.Errorf("config seededRecorderStatus %s: %w", key, getErr)
		}
		if data == nil {
			continue
		}
		if err := json.Unmarshal(data, &seed); err != nil {
			return seed, false, fmt.Errorf("config seededRecorderStatus %s unmarshal: %w", key, err)
		}
		if seed.LastStatus != "" {
			return seed, true, nil
		}
	}
	return cfgsvcSeededRecorderStatus{}, false, nil
}

// seededDeliveryStatus returns the pinned delivery status, resolved the same way.
func (p *ConfigServicePlugin) seededDeliveryStatus(ctx context.Context, accountID, region string) (
	seed cfgsvcSeededDeliveryStatus, found bool, err error) {
	for _, key := range cfgsvcCtrlKeyCandidates(cfgsvcCtrlDeliveryStatusKey, accountID, region) {
		data, getErr := p.state.Get(ctx, configServiceCtrlNamespace, key)
		if getErr != nil {
			return seed, false, fmt.Errorf("config seededDeliveryStatus %s: %w", key, getErr)
		}
		if data == nil {
			continue
		}
		if err := json.Unmarshal(data, &seed); err != nil {
			return seed, false, fmt.Errorf("config seededDeliveryStatus %s unmarshal: %w", key, err)
		}
		if seed.Status != "" {
			return seed, true, nil
		}
	}
	return cfgsvcSeededDeliveryStatus{}, false, nil
}

// seededDeliveryPolicy returns the pinned delivery-policy outcome for a bucket.
func (p *ConfigServicePlugin) seededDeliveryPolicy(ctx context.Context, bucket string) (
	outcome string, found bool, err error) {
	for _, key := range []string{cfgsvcCtrlDeliveryPolicyKey(bucket), cfgsvcCtrlDeliveryPolicyKey("*")} {
		data, getErr := p.state.Get(ctx, configServiceCtrlNamespace, key)
		if getErr != nil {
			return "", false, fmt.Errorf("config seededDeliveryPolicy %s: %w", key, getErr)
		}
		if data == nil {
			continue
		}
		var seed cfgsvcSeededDeliveryPolicy
		if err := json.Unmarshal(data, &seed); err != nil {
			return "", false, fmt.Errorf("config seededDeliveryPolicy %s unmarshal: %w", key, err)
		}
		if seed.Outcome != "" {
			return seed.Outcome, true, nil
		}
	}
	return "", false, nil
}

// cfgsvcCtrlKeyCandidates returns the four keys an account+region seed lookup tries,
// most specific first: the exact pair, either half wildcarded, then both.
//
// All four are tried rather than only exact-then-both, so a fixture can seed "every
// account in eu-west-1" or "this account in every Region" — the two scopes a
// multi-Region test actually needs.
func cfgsvcCtrlKeyCandidates(build func(string, string) string, accountID, region string) []string {
	return []string{
		build(accountID, region),
		build(accountID, "*"),
		build("*", region),
		build("*", "*"),
	}
}

// handleConfigSeedRecorderStatus handles POST /v1/config/recorder-status. It pins
// the lastStatus every DescribeConfigurationRecorderStatus reports, which is the
// only way to reach a recorder reporting Failure: substrate delivers nothing, so a
// recorder it started always succeeds.
// Body: {"accountId":"123456789012","region":"us-east-1","lastStatus":"Failure",
// "lastErrorCode":"NoAvailableDeliveryChannel","lastErrorMessage":"..."}.
func (s *Server) handleConfigSeedRecorderStatus(w http.ResponseWriter, r *http.Request) {
	var seed cfgsvcSeededRecorderStatus
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.LastStatus == "" {
		http.Error(w, `{"error":"lastStatus is required"}`, http.StatusBadRequest)
		return
	}
	// A status outside the enum would be reported as a string no SDK enum member
	// matches, so the caller's switch would fall through to its default and the test
	// would pass while asserting nothing.
	if !slices.Contains(cfgsvcRecorderStatuses, seed.LastStatus) {
		writeJSONErrorDebug(w, http.StatusBadRequest, "lastStatus %q is not a RecorderStatus", seed.LastStatus)
		return
	}
	// An error code or message on a non-Failure status would be silently dropped by
	// every consumer that reads them only on failure, so the seed is refused rather
	// than half-applied.
	if seed.LastStatus != cfgsvcRecorderStatusFailure && (seed.LastErrorCode != "" || seed.LastErrorMessage != "") {
		http.Error(w, `{"error":"lastErrorCode and lastErrorMessage apply only to lastStatus Failure"}`,
			http.StatusBadRequest)
		return
	}
	s.configSeedPut(w, r, cfgsvcCtrlRecorderStatusKey(seed.AccountID, seed.Region), seed,
		map[string]any{"lastStatus": seed.LastStatus})
}

// handleConfigClearRecorderStatus handles DELETE /v1/config/recorder-status. With
// ?accountId= and/or ?region= it removes that seed; without either it removes all of
// them, restoring each recorder to its stored status.
func (s *Server) handleConfigClearRecorderStatus(w http.ResponseWriter, r *http.Request) {
	s.configSeedClear(w, r, "recorder_status:", func() (string, bool) {
		accountID := r.URL.Query().Get("accountId")
		region := r.URL.Query().Get("region")
		if accountID == "" && region == "" {
			return "", false
		}
		return cfgsvcCtrlRecorderStatusKey(accountID, region), true
	})
}

// handleConfigSeedDeliveryStatus handles POST /v1/config/delivery-status. It pins
// the DeliveryStatus every stream of DescribeDeliveryChannelStatus reports, which is
// how a consumer reaches its delivery-failure branch — nothing substrate does can
// fail a delivery.
// Body: {"region":"us-east-1","status":"Failure","lastErrorCode":"AccessDenied"}.
func (s *Server) handleConfigSeedDeliveryStatus(w http.ResponseWriter, r *http.Request) {
	var seed cfgsvcSeededDeliveryStatus
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if seed.Status == "" {
		http.Error(w, `{"error":"status is required"}`, http.StatusBadRequest)
		return
	}
	if !slices.Contains(cfgsvcDeliveryStatuses, seed.Status) {
		writeJSONErrorDebug(w, http.StatusBadRequest,
			"status %q is not a DeliveryStatus (note Not_Applicable carries an underscore)", seed.Status)
		return
	}
	if seed.Status != cfgsvcDeliveryFailure && (seed.LastErrorCode != "" || seed.LastErrorMessage != "") {
		http.Error(w, `{"error":"lastErrorCode and lastErrorMessage apply only to status Failure"}`,
			http.StatusBadRequest)
		return
	}
	s.configSeedPut(w, r, cfgsvcCtrlDeliveryStatusKey(seed.AccountID, seed.Region), seed,
		map[string]any{"status": seed.Status})
}

// handleConfigClearDeliveryStatus handles DELETE /v1/config/delivery-status.
func (s *Server) handleConfigClearDeliveryStatus(w http.ResponseWriter, r *http.Request) {
	s.configSeedClear(w, r, "delivery_status:", func() (string, bool) {
		accountID := r.URL.Query().Get("accountId")
		region := r.URL.Query().Get("region")
		if accountID == "" && region == "" {
			return "", false
		}
		return cfgsvcCtrlDeliveryStatusKey(accountID, region), true
	})
}

// handleConfigSeedDeliveryPolicy handles POST /v1/config/delivery-policy. It forces
// or suppresses InsufficientDeliveryPolicyException for a bucket regardless of that
// bucket's real S3 state — "insufficient" for a consumer with no S3 fixture, "ok"
// for one whose valid policy substrate's permissive matcher still cannot read.
// Body: {"bucket":"cfg-logs","outcome":"insufficient"}.
func (s *Server) handleConfigSeedDeliveryPolicy(w http.ResponseWriter, r *http.Request) {
	var seed cfgsvcSeededDeliveryPolicy
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	seed.Outcome = strings.ToLower(strings.TrimSpace(seed.Outcome))
	if seed.Outcome == "" {
		http.Error(w, `{"error":"outcome is required"}`, http.StatusBadRequest)
		return
	}
	if !slices.Contains(cfgsvcDeliveryOutcomes, seed.Outcome) {
		writeJSONErrorDebug(w, http.StatusBadRequest, `outcome %q is not one of "ok" or "insufficient"`,
			seed.Outcome)
		return
	}
	s.configSeedPut(w, r, cfgsvcCtrlDeliveryPolicyKey(seed.Bucket), seed,
		map[string]any{"outcome": seed.Outcome})
}

// handleConfigClearDeliveryPolicy handles DELETE /v1/config/delivery-policy. With
// ?bucket= it removes that seed; without it removes all of them, restoring the
// computed check.
func (s *Server) handleConfigClearDeliveryPolicy(w http.ResponseWriter, r *http.Request) {
	s.configSeedClear(w, r, "delivery_policy:", func() (string, bool) {
		bucket := r.URL.Query().Get("bucket")
		if bucket == "" {
			return "", false
		}
		return cfgsvcCtrlDeliveryPolicyKey(bucket), true
	})
}

// handleConfigSeedRuleCompliance handles POST /v1/config/rule-compliance/{name}. It
// pins the verdict a Config rule reports, which is the only way one is ever anything
// but INSUFFICIENT_DATA: substrate evaluates no rules.
// Body: {"complianceType":"NON_COMPLIANT","annotation":"…",
// "resources":[{"resourceType":"AWS::S3::Bucket","resourceId":"b1"}]}.
// A {name} of "*" seeds every rule.
func (s *Server) handleConfigSeedRuleCompliance(w http.ResponseWriter, r *http.Request) {
	var seed cfgsvcSeededRuleCompliance
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	// The path names the rule, so a body naming a different one is a contradiction
	// rather than an override: honoring either would leave the caller unable to tell
	// which rule it seeded.
	if name := chi.URLParam(r, "name"); name != "" {
		if seed.ConfigRuleName != "" && seed.ConfigRuleName != name {
			writeJSONErrorDebug(w, http.StatusBadRequest,
				"configRuleName %q in the body contradicts %q in the path", seed.ConfigRuleName, name)
			return
		}
		seed.ConfigRuleName = name
	}
	if seed.ComplianceType == "" {
		http.Error(w, `{"error":"complianceType is required"}`, http.StatusBadRequest)
		return
	}
	// NOT_APPLICABLE is in the ComplianceType enum but not in the Compliance shape's
	// subset: "Config does not support the NOT_APPLICABLE value for the Compliance data
	// type." Storing it would make DescribeComplianceByConfigRule report a value the
	// shape cannot carry, which no SDK enum member matches.
	if !slices.Contains(cfgsvcRuleComplianceTypes, seed.ComplianceType) {
		writeJSONErrorDebug(w, http.StatusBadRequest, "complianceType %q is not valid for a rule; the "+
			"Compliance shape supports only COMPLIANT, NON_COMPLIANT and INSUFFICIENT_DATA (Config "+
			"does not support NOT_APPLICABLE for this data type)", seed.ComplianceType)
		return
	}
	// INSUFFICIENT_DATA cannot appear in an EvaluationResult — "Config does not support
	// the INSUFFICIENT_DATA value for the EvaluationResult data type" — so naming
	// resources alongside it asks for per-resource results that cannot be emitted. The
	// seed is refused rather than half-applied.
	if seed.ComplianceType == cfgsvcInsufficientData && len(seed.Resources) > 0 {
		http.Error(w, `{"error":"resources cannot be given with complianceType INSUFFICIENT_DATA, `+
			`which the EvaluationResult shape does not support"}`, http.StatusBadRequest)
		return
	}
	for _, res := range seed.Resources {
		if res.ResourceType == "" || res.ResourceID == "" {
			http.Error(w, `{"error":"each resource requires a resourceType and a resourceId"}`,
				http.StatusBadRequest)
			return
		}
	}
	s.configSeedPut(w, r, cfgsvcCtrlRuleComplianceKey(seed.AccountID, seed.Region, seed.ConfigRuleName),
		seed, map[string]any{
			"complianceType": seed.ComplianceType,
			"description":    cfgsvcRuleComplianceDescription(seed),
		})
}

// handleConfigClearRuleCompliance handles DELETE /v1/config/rule-compliance/{name}.
// A {name} of "*" clears the every-rule seed; omitting the segment clears all of
// them, returning every rule to INSUFFICIENT_DATA.
func (s *Server) handleConfigClearRuleCompliance(w http.ResponseWriter, r *http.Request) {
	s.configSeedClear(w, r, "rule_compliance:", func() (string, bool) {
		name := chi.URLParam(r, "name")
		if name == "" {
			return "", false
		}
		return cfgsvcCtrlRuleComplianceKey(r.URL.Query().Get("accountId"), r.URL.Query().Get("region"),
			name), true
	})
}

// configSeedPut stores one Config seed and reports what it stored.
//
// The stored key is echoed back because the wildcard defaulting is invisible from
// the request: a caller that omitted the Region gets "*" and should be able to see
// that it did.
func (s *Server) configSeedPut(w http.ResponseWriter, r *http.Request, key string, seed any, extra map[string]any) {
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), configServiceCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	out := map[string]any{"ok": true, "key": key}
	for k, v := range extra {
		out[k] = v
	}
	writeJSONDebug(w, s.logger, out)
}

// configSeedClear removes one Config seed or, when the request names none, every
// seed under prefix.
func (s *Server) configSeedClear(w http.ResponseWriter, r *http.Request, prefix string, specific func() (string, bool)) {
	if key, ok := specific(); ok {
		if err := s.state.Delete(r.Context(), configServiceCtrlNamespace, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), configServiceCtrlNamespace, prefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), configServiceCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
