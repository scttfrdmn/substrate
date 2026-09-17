package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// DeleteSecret's recovery window, and the operations that observe it — #953.
//
// AWS's DeleteSecret does not remove a secret. It "attaches a DeletionDate stamp to the secret that
// specifies the end of the recovery window", and only "at the end of the recovery window" does Secrets
// Manager "delete the secret permanently"; the minimum window is 7 days and the default is 30, and
// "at any time before recovery window ends, you can use RestoreSecret to remove the DeletionDate and
// cancel the deletion of the secret". Substrate removed the record, the version payload and the index
// entry on every call — the one behavior AWS reserves for ForceDeleteWithoutRecovery: true — and
// decoded neither of the two parameters that decide between them. So the destructive variant was the
// only variant, RestoreSecret had nothing to restore, and a scheduled secret and a secret that never
// existed were the same observation.
//
// A recovery window is in scope under the boundary CLAUDE.md draws: it is a state transition
// observable through an API call, stamped off the simulated clock, so a consumer's
// schedule-then-restore path is assertable with no dependence on wall-clock time.
//
// **The permanent deletion at the end of the window is deliberately not modeled**, and that is a
// decision rather than an omission: AWS publishes no guarantee to model — "There is no guarantee of a
// specific time after the recovery window for the permanent delete to occur" — so a secret whose
// DeletionDate has passed is still reported here. A test asserting it had vanished at some simulated
// instant would assert something AWS does not promise. The observable contract is the stamp and the
// refusals it causes, and both of those are modeled.

const (
	// smMinRecoveryWindowDays is the shortest recovery window DeleteSecret accepts: "The number of days
	// from 7 to 30 that Secrets Manager waits before permanently deleting the secret".
	smMinRecoveryWindowDays = 7

	// smMaxRecoveryWindowDays is the longest, from the same sentence.
	smMaxRecoveryWindowDays = 30

	// smDefaultRecoveryWindowDays is the window a call that names neither parameter gets: "If you don't
	// use either, then by default Secrets Manager uses a 30 day recovery window".
	smDefaultRecoveryWindowDays = 30
)

// smSecretScheduledForDeletion reports that an operation cannot run against a secret whose deletion is
// already scheduled.
//
// InvalidRequestException/400 is published identically on API_DeleteSecret, API_GetSecretValue,
// API_RestoreSecret and API_RotateSecret, and the first of its three "Possible causes" is "The secret
// is scheduled for deletion." The message names which cause applies, because one code covers three
// unrelated conditions and a caller reading only the code cannot tell them apart.
//
// Being distinguishable from [smSecretNotFound] is the point. A scheduled secret still exists and is
// restorable, and substrate answered ResourceNotFoundException for it — the same answer as for a secret
// that never existed, which sent a consumer's error handling down the wrong branch.
func smSecretScheduledForDeletion(id string, deletionDate time.Time) *AWSError {
	return &AWSError{
		Code: "InvalidRequestException",
		Message: fmt.Sprintf(
			"the secret %q is scheduled for deletion on %s; cancel the deletion with RestoreSecret first",
			id, deletionDate.UTC().Format(time.RFC3339)),
		HTTPStatus: http.StatusBadRequest,
	}
}

func (p *SecretsManagerPlugin) deleteSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Both optional parameters are pointers, because AWS's mutual exclusion is stated over their
	// *presence* and a zero value is otherwise indistinguishable from an omission.
	var input struct {
		SecretID                   string `json:"SecretId"`
		RecoveryWindowInDays       *int64 `json:"RecoveryWindowInDays"`
		ForceDeleteWithoutRecovery *bool  `json:"ForceDeleteWithoutRecovery"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	if input.RecoveryWindowInDays != nil && input.ForceDeleteWithoutRecovery != nil {
		// AWS states the exclusion twice, once under each parameter, and states it over *use* rather
		// than over values: "You can't use both this parameter and ForceDeleteWithoutRecovery in the
		// same call." Refusing on presence — so that ForceDeleteWithoutRecovery: false beside a window
		// is refused too — is substrate's reading of "use both", taken because the alternative makes an
		// explicit false mean the same as an omission and silently schedules a deletion the caller
		// asked to be immediate.
		return nil, &AWSError{
			Code:       "InvalidParameterException",
			Message:    "RecoveryWindowInDays and ForceDeleteWithoutRecovery cannot both be used in the same call",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	window := int64(smDefaultRecoveryWindowDays)
	if input.RecoveryWindowInDays != nil {
		window = *input.RecoveryWindowInDays
		if window < smMinRecoveryWindowDays || window > smMaxRecoveryWindowDays {
			return nil, &AWSError{
				Code: "InvalidParameterException",
				Message: fmt.Sprintf("RecoveryWindowInDays must be from %d to %d, not %d",
					smMinRecoveryWindowDays, smMaxRecoveryWindowDays, window),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	force := input.ForceDeleteWithoutRecovery != nil && *input.ForceDeleteWithoutRecovery

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}

	now := p.tc.Now()
	if secret == nil {
		if !force {
			return nil, smSecretNotFound(input.SecretID)
		}
		// "If you forcibly delete an already deleted or nonexistent secret, the operation does not
		// return ResourceNotFoundException." That sentence is the one place AWS suspends the code #930
		// corrected the status of, and it publishes the suspension without publishing the body answered
		// instead — so reporting the ARN the identifier names is substrate's reading, there being no
		// record to read one from.
		return smJSONResponse(http.StatusOK, map[string]interface{}{
			"ARN":          generateSecretARN(target.Region, target.AccountID, target.Name),
			"Name":         target.Name,
			"DeletionDate": now.Unix(),
		})
	}

	if !force {
		if !secret.DeletionDate.IsZero() {
			return nil, smSecretScheduledForDeletion(input.SecretID, secret.DeletionDate)
		}
		// "This value is the date and time of the delete request plus the number of days in
		// RecoveryWindowInDays." The request time is the simulated clock's, so the stamp a recorded run
		// answered is the stamp its replay answers.
		secret.DeletionDate = now.AddDate(0, 0, int(window))
		if err := p.saveSecret(goCtx, secret); err != nil {
			return nil, fmt.Errorf("sm deleteSecret saveSecret: %w", err)
		}
		return smJSONResponse(http.StatusOK, map[string]interface{}{
			"ARN":          secret.ARN,
			"Name":         secret.Name,
			"DeletionDate": secret.DeletionDate.Unix(),
		})
	}

	if err := p.purgeSecret(goCtx, target, secret); err != nil {
		return nil, err
	}
	// A forced delete has no window, so the same "delete request plus RecoveryWindowInDays" derivation
	// puts the DeletionDate at the request itself. Reporting one at all is substrate's reading: AWS
	// publishes the member unconditionally and publishes no separate forced-delete response.
	return smJSONResponse(http.StatusOK, map[string]interface{}{
		"ARN":          secret.ARN,
		"Name":         secret.Name,
		"DeletionDate": now.Unix(),
	})
}

// purgeSecret removes a secret's record, its current version payload and its index entry — the whole of
// what a forced delete destroys.
//
// The index entry removed is the one in the account and Region that owns the secret, which is what the
// identifier named, not the caller's: deleting the caller's left the owning account still listing a
// secret whose record had just been removed (#928). The three Delete errors are wrapped rather than
// discarded, which the previous inline form did not do.
func (p *SecretsManagerPlugin) purgeSecret(goCtx context.Context, target smSecretTarget, secret *SecretState) error {
	if err := p.state.Delete(goCtx, secretsManagerNamespace, smSecretStateKey(target.AccountID, target.Region, target.Name)); err != nil {
		return fmt.Errorf("sm purgeSecret delete record: %w", err)
	}
	if err := p.state.Delete(goCtx, secretsManagerNamespace, smSecretVersionStateKey(target.AccountID, target.Region, target.Name, secret.CurrentVersionID)); err != nil {
		return fmt.Errorf("sm purgeSecret delete version: %w", err)
	}

	names, err := p.loadSecretNames(goCtx, target.AccountID, target.Region)
	if err != nil {
		return err
	}
	remaining := make([]string, 0, len(names))
	for _, n := range names {
		if n != target.Name {
			remaining = append(remaining, n)
		}
	}
	if err := p.saveSecretNames(goCtx, target.AccountID, target.Region, remaining); err != nil {
		return fmt.Errorf("sm purgeSecret saveSecretNames: %w", err)
	}
	return nil
}

// restoreSecret cancels a scheduled deletion, which AWS describes as removing the stamp rather than as
// reviving anything: "Cancels the scheduled deletion of a secret by removing the DeletedDate time
// stamp. You can access a secret again after it has been restored."
//
// Its response is two members, ARN and Name — AWS publishes no DeletedDate on it, which is consistent
// with the stamp being gone.
//
// Restoring a secret that is not scheduled succeeds, which is **substrate's reading**: API_RestoreSecret
// publishes InvalidRequestException but its cause list names only the three conditions shared across
// the service, none of which is "not scheduled", so there is no published code to answer with. The
// alternative — inventing a refusal — would make a consumer's idempotent restore fail against substrate
// and succeed against AWS.
func (p *SecretsManagerPlugin) restoreSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
	}

	secret.DeletionDate = time.Time{}
	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm restoreSecret saveSecret: %w", err)
	}

	return smJSONResponse(http.StatusOK, map[string]interface{}{
		"ARN":  secret.ARN,
		"Name": secret.Name,
	})
}
