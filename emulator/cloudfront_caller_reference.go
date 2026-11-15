package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// A distribution's CallerReference is the create's replay guard (#1116).
//
// API_DistributionConfig marks CallerReference Required: Yes and describes a behavior rather than a
// label: "A unique value (for example, a date-time stamp) that ensures that the request can't be
// replayed. If the value of CallerReference is new (regardless of the content of the
// DistributionConfig object), CloudFront creates a new distribution. If CallerReference is a value
// that you already sent in a previous request to create a distribution, CloudFront returns a
// DistributionAlreadyExists error." API_CreateDistribution repeats the sentence on its request member
// and publishes DistributionAlreadyExists at 409. Until #1116 substrate never read the member on a
// create, so a create without one was answered 201 and a replayed create minted a second distribution.
//
// # A reused reference is refused whatever the configuration says
//
// The page states the refusal for any reference already sent, "regardless of the content of the
// DistributionConfig object", and the API model's own documentation of the member says the same.
// Two sibling members do publish an "identical request returns the original" case —
// API_InvalidationBatch ("if the rest of the request is the same … returns information about the
// invalidation request that you previously created", modeled by createInvalidation) and
// API_CloudFrontOriginAccessIdentityConfig ("identical to the original request (ignoring white
// space)") — and DistributionConfig's text conspicuously does not, so substrate does not borrow it.
// A caller retrying a create it lost the answer to is told DistributionAlreadyExists and should list
// to find the distribution.
//
// # Scope and lifetime
//
// A reference belongs to the account that created the distribution: the index entry is keyed by
// account and reference ([cfCallerRefKey]) and names the distribution holding it. **Deleting the
// distribution releases the reference**, so a later create may reuse it. AWS publishes nothing on the
// point either way; that reading is substrate's, taken because a reference held by a distribution
// that no longer exists has nothing left to protect from replay and would otherwise make a
// deleted-and-recreated fixture unrepeatable within one run.
//
// # Why an index entry, and why it is checked against the record
//
// One read per create rather than a scan of every distribution, which under the create mutex made
// a create cost O(n) and a run of n creates O(n²). The entry is a pointer, not the truth: the
// reference itself still lives in the distribution's stored configuration, and an entry is honored
// only when the distribution it names exists and still holds the reference. So an entry a
// half-finished delete left behind releases itself, and a distribution record that cannot be read
// is logged and treated as not holding it — one bad record does not refuse every create in the
// account. A record written before #1116 is not in the index, so a create reusing the reference
// such a record's configuration happens to carry is not refused. That reaches only state restored
// from an earlier substrate's snapshot, and the alternative is the scan this index replaced.

// cfCallerReferenceRequired refuses a create whose configuration carries no CallerReference.
//
// InvalidArgument/400 is the code API_CreateDistribution (and API_CreateDistributionWithTags)
// publishes for "An argument is invalid"; neither page publishes a code specific to a missing member.
// The message is substrate's own, in the wording CreateInvalidation already answers for its own
// missing CallerReference.
func cfCallerReferenceRequired() *AWSError {
	return &AWSError{Code: "InvalidArgument", Message: "CallerReference is required", HTTPStatus: http.StatusBadRequest}
}

// cfDistributionAlreadyExists is the refusal of a create reusing a CallerReference. The message is
// the error's description on API_CreateDistribution's Errors list, at the status that list publishes.
func cfDistributionAlreadyExists() *AWSError {
	return &AWSError{
		Code:       "DistributionAlreadyExists",
		Message:    "The caller reference you attempted to create the distribution with is associated with another distribution.",
		HTTPStatus: http.StatusConflict,
	}
}

// cfCallerReferenceHeldOnUpdate refuses an UpdateDistribution that would give a distribution a
// CallerReference another of the account's distributions holds. It is reachable only for a record
// written before #1116, which holds no reference and so may adopt one: every later record's
// reference cannot change (cfCheckUpdatedConfig). DistributionAlreadyExists is not on
// API_UpdateDistribution's Errors list, so the code is IllegalUpdate/400, the code that page
// publishes for a change it does not allow; the message is substrate's own.
func cfCallerReferenceHeldOnUpdate() *AWSError {
	return cfConfigError("IllegalUpdate", "The CallerReference is associated with another distribution.")
}

// cfCallerRefKey is the state key of a CallerReference's index entry, whose value is the ID of the
// distribution holding it.
func cfCallerRefKey(accountID, ref string) string {
	return cfCallerRefPrefix + accountID + "/" + ref
}

// callerReferenceHolder returns the ID of the account's distribution holding ref, or "" when none
// does. See the file comment for why an entry is checked against the record it names.
//
// The caller holds createMu. A store fault is an error: it says nothing about whether the reference
// is held, and treating it as "not held" would let a replayed create through on a fault.
func (p *CloudFrontPlugin) callerReferenceHolder(goCtx context.Context, accountID, ref string) (string, error) {
	data, err := p.state.Get(goCtx, cloudfrontNamespace, cfCallerRefKey(accountID, ref))
	if err != nil {
		return "", fmt.Errorf("cloudfront caller reference index: %w", err)
	}
	if data == nil {
		return "", nil
	}
	var id string
	if err := json.Unmarshal(data, &id); err != nil {
		p.warn("cloudfront: caller reference index entry does not decode; treating the reference as free",
			"reference", ref, "err", err)
		return "", nil
	}
	data, err = p.state.Get(goCtx, cloudfrontNamespace, cfDistKey(accountID, id))
	if err != nil {
		return "", fmt.Errorf("cloudfront caller reference holder %s: %w", id, err)
	}
	if data == nil {
		return "", nil // the distribution is gone; a half-finished delete left the entry
	}
	var dist CloudFrontDistribution
	if err := json.Unmarshal(data, &dist); err != nil {
		p.warn("cloudfront: distribution record does not decode; treating its reference as free",
			"distribution", id, "err", err)
		return "", nil
	}
	cfg, err := cfStoredConfig(dist)
	if err != nil {
		p.warn("cloudfront: stored configuration does not parse; treating its reference as free",
			"distribution", id, "err", err)
		return "", nil
	}
	if cfConfigCallerReference(&cfg) != ref {
		return "", nil
	}
	return id, nil
}

// putCallerReference records that distID holds ref.
func (p *CloudFrontPlugin) putCallerReference(goCtx context.Context, accountID, ref, distID string) error {
	data, err := json.Marshal(distID)
	if err != nil {
		return fmt.Errorf("cloudfront caller reference index marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cloudfrontNamespace, cfCallerRefKey(accountID, ref), data); err != nil {
		return fmt.Errorf("cloudfront caller reference index put: %w", err)
	}
	return nil
}

// releaseCallerReference removes ref's index entry when it names distID. An entry naming another
// distribution is left alone: it is that distribution's.
func (p *CloudFrontPlugin) releaseCallerReference(goCtx context.Context, accountID, ref, distID string) error {
	if ref == "" {
		return nil
	}
	key := cfCallerRefKey(accountID, ref)
	data, err := p.state.Get(goCtx, cloudfrontNamespace, key)
	if err != nil {
		return fmt.Errorf("cloudfront caller reference index read: %w", err)
	}
	if data == nil {
		return nil
	}
	var holder string
	if err := json.Unmarshal(data, &holder); err != nil {
		// An entry that does not decode names no distribution, this one included, and
		// callerReferenceHolder already treats it as free; it is not this delete's to remove.
		return nil //nolint:nilerr // see above.
	}
	if holder != distID {
		return nil
	}
	if err := p.state.Delete(goCtx, cloudfrontNamespace, key); err != nil {
		return fmt.Errorf("cloudfront caller reference index delete: %w", err)
	}
	return nil
}

// warn logs through the plugin's logger, which a hand-built plugin may not have.
func (p *CloudFrontPlugin) warn(msg string, args ...any) {
	if p.logger != nil {
		p.logger.Warn(msg, args...)
	}
}
