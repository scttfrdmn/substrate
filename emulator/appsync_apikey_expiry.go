package emulator

import "net/http"

// An AppSync api key's expiry, as API_CreateApiKey publishes it (#1122).
//
// createAPIKey decoded only `description` and set the expiry itself, a year out. Three divergences
// in one handler:
//
//   - The published default is **7 days**, not 365: "The default value for this parameter is 7 days
//     from creation time." A consumer asserting that a fresh key expires within the week — which is
//     what AWS's default makes true — saw a year.
//   - `expires` is a published request member and was decoded nowhere, so a caller asking for a
//     30-day key was given a 365-day one and told it succeeded. Its absence is fine (Required: No);
//     ignoring a value the caller sent is the silent-wrong-answer class.
//   - `ApiKeyValidityOutOfBoundsException`/400 therefore had no site. The page publishes it with the
//     constraint spelled out — "The API key expiration must be set to a value between 1 and 365 days
//     from creation (for CreateApiKey) or from update (for UpdateApiKey)" — and a refusal cannot fire
//     against a value that is never read.
//
// UpdateApiKey is deliberately not covered: it is unrouted, because #1065 gated the `apikeys` tail so
// that a POST under it keeps answering UnknownOperationException/404 rather than silently minting a
// second credential. The bound applies "from update" there, so that half arrives with the operation.

const (
	// appsyncAPIKeyHour is the granularity API_ApiKey publishes for both of its timestamps: "The date
	// is represented as seconds since the epoch, rounded down to the nearest hour".
	appsyncAPIKeyHour = int64(3600)

	// appsyncAPIKeyDay is one day in seconds, the unit the published bound is stated in.
	appsyncAPIKeyDay = int64(24 * 3600)

	// appsyncAPIKeyDefaultDays is the default API_CreateApiKey publishes for an omitted `expires`.
	appsyncAPIKeyDefaultDays = int64(7)

	// appsyncAPIKeyMinDays and appsyncAPIKeyMaxDays are the bound
	// ApiKeyValidityOutOfBoundsException's own message states.
	appsyncAPIKeyMinDays = int64(1)
	appsyncAPIKeyMaxDays = int64(365)

	// appsyncAPIKeyDeleteDays is how long past expiry API_ApiKey says a key survives: "Expired API
	// keys are kept for 60 days after the expiration time".
	appsyncAPIKeyDeleteDays = int64(60)
)

// appsyncAPIKeyValidityOutOfBounds reports an `expires` outside the published 1-to-365-day window.
//
// The message is the page's own sentence, because the bound is what a caller has to be told and the
// page states it in exactly those terms.
func appsyncAPIKeyValidityOutOfBounds() *AWSError {
	return &AWSError{
		Code: "ApiKeyValidityOutOfBoundsException",
		Message: "The API key expiration must be set to a value between 1 and 365 days from creation " +
			"(for CreateApiKey) or from update (for UpdateApiKey).",
		HTTPStatus: http.StatusBadRequest,
	}
}

// appsyncAPIKeyExpiry resolves the expiry of a key created at createdAt from the `expires` the caller
// sent, or refuses it.
//
// A zero `expires` is an omitted one — the member is Required: No and Long, so there is no other
// spelling of "not supplied" available — and takes the published 7-day default.
//
// The bound is checked against the value the caller sent and the rounding applied after, rather than
// the other way round: rounding first would refuse an `expires` of exactly one day out, which the
// page's "between 1 and 365 days" admits, because the rounding is how the timestamp is *represented*
// and not part of the constraint. AWS publishes no order for the two, so this is the reading that
// refuses nothing the sentence allows.
func appsyncAPIKeyExpiry(createdAt, requested int64) (int64, *AWSError) {
	if requested == 0 {
		return appsyncAPIKeyRoundToHour(createdAt + appsyncAPIKeyDefaultDays*appsyncAPIKeyDay), nil
	}

	validity := requested - createdAt
	if validity < appsyncAPIKeyMinDays*appsyncAPIKeyDay || validity > appsyncAPIKeyMaxDays*appsyncAPIKeyDay {
		return 0, appsyncAPIKeyValidityOutOfBounds()
	}
	return appsyncAPIKeyRoundToHour(requested), nil
}

// appsyncAPIKeyRoundToHour truncates a timestamp to the hour, the representation API_ApiKey
// publishes for `expires` and `deletes`.
//
// Go's % keeps the sign of the dividend, so a negative epoch would round towards zero rather than
// down; no AppSync key can have one, and the arithmetic is written for the epoch the simulated clock
// produces.
func appsyncAPIKeyRoundToHour(ts int64) int64 {
	return ts - ts%appsyncAPIKeyHour
}

// appsyncAPIKeyOut is the apiKey element of CreateApiKey and — under the name apiKeys — ListApiKeys.
//
// Member names follow API_ApiKey, whose four members are all Required: No. `deletes` is derived here
// rather than persisted, which is what keeps a key recorded before #1122 answering the published
// value instead of a zero: the page defines it as a fixed offset from `expires` ("Expired API keys
// are kept for 60 days after the expiration time"), so it is a projection of the record and not
// state of its own. See appsync_wire.go for why a projection and not a retagged field.
type appsyncAPIKeyOut struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	Expires     int64  `json:"expires"`
	Deletes     int64  `json:"deletes"`
}

// appsyncAPIKeyToWire projects a persisted api key onto the published shape.
func appsyncAPIKeyToWire(key AppSyncAPIKey) appsyncAPIKeyOut {
	return appsyncAPIKeyOut{
		ID:          key.ID,
		Description: key.Description,
		Expires:     key.Expires,
		Deletes:     appsyncAPIKeyRoundToHour(key.Expires + appsyncAPIKeyDeleteDays*appsyncAPIKeyDay),
	}
}

// appsyncAPIKeysToWire projects a slice of persisted api keys, for ListApiKeys.
func appsyncAPIKeysToWire(keys []AppSyncAPIKey) []appsyncAPIKeyOut {
	out := make([]appsyncAPIKeyOut, 0, len(keys))
	for _, key := range keys {
		out = append(out, appsyncAPIKeyToWire(key))
	}
	return out
}
