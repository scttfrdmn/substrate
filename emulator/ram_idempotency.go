package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
)

// CreateResourceShare and DeleteResourceShare honor their clientToken (#1170).
//
// Both pages publish the member in the same words: "Specifies a unique, case-sensitive identifier
// that you provide to ensure the idempotency of the request. This lets you safely retry the request
// without accidentally performing the same operation a second time. Passing the same value to a later
// call to an operation requires that you also pass the same value for all other parameters. … If you
// don't provide this value, then AWS generates a random one for you. If you retry the operation with
// the same ClientToken, but with different parameters, the retry fails with an
// IdempotentParameterMismatch error." Both responses publish `clientToken` — "The idempotency
// identifier associated with this request. If you want to repeat the same operation in an idempotent
// manner then you must include this value in the clientToken request parameter of that later call."
// Substrate read the member on neither and echoed it on neither, so a retried create minted a second
// share and a mismatched retry succeeded.
//
// # Where the token lives
//
// On the share, found by a scan of the caller's shares, as AWS Backup's CreatorRequestId is
// (backup_idempotency.go) — no new state key. [RAMResourceShare.ClientToken] is the create's token
// and [RAMResourceShare.CreateParams] the canonical form of every other create parameter;
// [RAMResourceShare.DeleteClientToken] is the delete's. The two operations' tokens are separate
// because the page scopes a token to "a previous call to the operation".
//
// # What a token matches
//
//   - A create whose token names a share created under it with the **same** parameters answers that
//     share and the token, creating nothing. The parameters are compared in canonical form (the
//     request body decoded and re-encoded with sorted keys, less clientToken), so the comparison
//     covers members substrate does not model — permissionArns, sources,
//     resourceShareConfiguration — as well as those it does, and is indifferent to key order and
//     whitespace but not to an omitted default.
//   - The same token with **different** parameters is IdempotentParameterMismatchException/400.
//   - A create token does not match a DELETED share. Retrying a create to bring back a share that has
//     since been deleted would answer a share whose status is DELETED as the outcome of a create,
//     which is not what any caller retrying a create is asking.
//   - A delete whose token names a share deleted under it answers `returnValue: true` and the token
//     again, rather than the InvalidStateTransitionException a second delete answers without one
//     (#1171): "repeat the same operation in an idempotent manner" is the page's own phrase for it.
//     The same delete token on a **different** share is IdempotentParameterMismatchException. Once
//     the deleted share's two-hour window passes it is gone, and a retry finds nothing to match and
//     answers UnknownResourceException, as any call naming it does.
//
// # A token the request omits
//
// Is minted through `reqCtx.IDs` ([IDMint.UUID], the shape the page recommends), so a replayed
// request mints the token the recording answered. On a create it is minted **after** the share ID, so
// a recording made before this change replays to the same share ARN. The minted token is stored like
// a caller's, so a caller that retries with the token it was handed gets the idempotent answer.
//
// # Concurrency
//
// The scan, the check and the write happen under one lock per account and Region ([ramScopeMutex]),
// so two concurrent creates under one token cannot both find nothing and both create. StateManager
// has no compare-and-swap; this is the pattern of [s3KeyMutex], and process-local as that is.
//
// # Not modeled
//
// InvalidClientTokenException is published by both pages and has no site: neither publishes a length
// or a pattern for the token, so nothing says what an invalid one is.

// ramScopeLockStripes is the number of mutexes in [ramScopeMutex].
const ramScopeLockStripes = 64

// ramScopeMutex serializes the token scan, check and write of CreateResourceShare and
// DeleteResourceShare per account and Region.
type ramScopeMutex struct {
	stripes [ramScopeLockStripes]sync.Mutex
}

// lock acquires the stripe guarding one account and Region and returns its unlock function.
func (m *ramScopeMutex) lock(acct, region string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(acct + "/" + region))
	mu := &m.stripes[h.Sum32()%ramScopeLockStripes]
	mu.Lock()
	return mu.Unlock
}

// ramIdempotentParameterMismatch is the refusal for a token reused with different parameters, in the
// words API_CreateResourceShare and API_DeleteResourceShare publish for the code.
func ramIdempotentParameterMismatch() *AWSError {
	return &AWSError{
		Code: "IdempotentParameterMismatchException",
		// API_CreateResourceShare / API_DeleteResourceShare, Errors.
		Message: "The operation failed because the client token input parameter matched one that was used " +
			"with a previous call to the operation, but at least one of the other input parameters is " +
			"different from the previous call.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramCanonicalCreateParams renders a CreateResourceShare body, less clientToken, in a canonical form:
// decoded and re-encoded, which sorts object keys and normalizes whitespace.
func ramCanonicalCreateParams(body []byte) (string, error) {
	params := map[string]any{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return "", ramInvalidBody()
		}
	}
	delete(params, "clientToken")
	out, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("ram canonical create params: %w", err)
	}
	return string(out), nil
}

// findShareByClientToken returns the caller's live share created under token, or nil when none was.
// A DELETED share does not match; see this file's comment.
func (p *RAMPlugin) findShareByClientToken(goCtx context.Context, acct, region, token string) (*RAMResourceShare, error) {
	shares, err := p.loadShares(goCtx, acct, region)
	if err != nil {
		return nil, err
	}
	for i := range shares {
		if shares[i].ClientToken == token && shares[i].Status != ramShareStatusDeleted {
			return &shares[i], nil
		}
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no share was created under this token".
}

// findShareByDeleteClientToken returns the caller's share deleted under token and still within its
// retention window, or nil when none is.
func (p *RAMPlugin) findShareByDeleteClientToken(goCtx context.Context, acct, region, token string) (*RAMResourceShare, error) {
	shares, err := p.loadShares(goCtx, acct, region)
	if err != nil {
		return nil, err
	}
	for i := range shares {
		if shares[i].DeleteClientToken == token {
			return &shares[i], nil
		}
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no share was deleted under this token".
}
