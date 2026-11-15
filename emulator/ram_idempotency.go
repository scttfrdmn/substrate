package emulator

import (
	"context"
	"encoding/json"
	"errors"
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
// On the share: [RAMResourceShare.ClientToken] is the create's token and
// [RAMResourceShare.CreateParams] the canonical form of every other create parameter;
// [RAMResourceShare.DeleteClientToken] is the delete's. The two operations' tokens are separate
// because the page scopes a token to "a previous call to the operation".
//
// A share is found from its token through one index entry per token —
// `share_client_token:{acct}/{region}/{token}` and `share_delete_token:{acct}/{region}/{token}`, each
// holding the share's ARN — rather than by scanning every share, as AWS Backup's CreatorRequestId is
// found (backup_idempotency.go). The lookup runs under the scope lock, and RAM's default quota is
// thousands of shares per account and Region, so a scan would hold every writer in the scope for a
// read of each one. The entry is a pointer, not the truth: the share it names is loaded and must still
// carry the token, so an entry that outlived or lost its share matches nothing. Pruning an expired
// share removes the entries that still name it, and only those — a create token reused after its
// share was deleted points at the new share.
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
// The lookup, the check and the write happen under one lock per account and Region ([ramScopeMutex]),
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

// ramUnknownResourceCode is the code a lookup of an absent or expired share answers.
const ramUnknownResourceCode = "UnknownResourceException"

// ramClientTokenKey is the index entry naming the share a create token made.
func ramClientTokenKey(acct, region, token string) string {
	return "share_client_token:" + acct + "/" + region + "/" + token
}

// ramDeleteTokenKey is the index entry naming the share a delete token deleted.
func ramDeleteTokenKey(acct, region, token string) string {
	return "share_delete_token:" + acct + "/" + region + "/" + token
}

// putTokenEntry records that token names the share at arn.
func (p *RAMPlugin) putTokenEntry(goCtx context.Context, key, arn string) error {
	if err := p.state.Put(goCtx, ramNamespace, key, []byte(arn)); err != nil {
		return fmt.Errorf("ram put token entry %s: %w", key, err)
	}
	return nil
}

// shareByTokenEntry loads the visible share a token entry names, or nil when there is no entry or the
// share it names is gone or has expired.
func (p *RAMPlugin) shareByTokenEntry(goCtx context.Context, acct, region, key string) (*RAMResourceShare, error) {
	data, err := p.state.Get(goCtx, ramNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("ram get token entry %s: %w", key, err)
	}
	if len(data) == 0 {
		return nil, nil //nolint:nilnil // (nil, nil) = "no share is recorded under this token".
	}
	share, err := p.loadShare(acct, region, string(data))
	if err != nil {
		var awsErr *AWSError
		if errors.As(err, &awsErr) && awsErr.Code == ramUnknownResourceCode {
			return nil, nil //nolint:nilnil // an entry that outlived its share names nothing.
		}
		return nil, err
	}
	return share, nil
}

// findShareByClientToken returns the caller's live share created under token, or nil when none was.
// A DELETED share does not match; see this file's comment.
func (p *RAMPlugin) findShareByClientToken(goCtx context.Context, acct, region, token string) (*RAMResourceShare, error) {
	share, err := p.shareByTokenEntry(goCtx, acct, region, ramClientTokenKey(acct, region, token))
	if err != nil || share == nil {
		return nil, err
	}
	if share.ClientToken != token || share.Status == ramShareStatusDeleted {
		return nil, nil //nolint:nilnil // (nil, nil) = "no live share was created under this token".
	}
	return share, nil
}

// findShareByDeleteClientToken returns the caller's share deleted under token and still within its
// retention window, or nil when none is.
func (p *RAMPlugin) findShareByDeleteClientToken(goCtx context.Context, acct, region, token string) (*RAMResourceShare, error) {
	share, err := p.shareByTokenEntry(goCtx, acct, region, ramDeleteTokenKey(acct, region, token))
	if err != nil || share == nil {
		return nil, err
	}
	if share.DeleteClientToken != token {
		return nil, nil //nolint:nilnil // (nil, nil) = "no share was deleted under this token".
	}
	return share, nil
}

// dropTokenEntries removes the token entries that still name share, as its record is pruned. An entry
// a later share has taken over is left alone.
func (p *RAMPlugin) dropTokenEntries(goCtx context.Context, acct, region string, share *RAMResourceShare) error {
	keys := make([]string, 0, 2)
	if share.ClientToken != "" {
		keys = append(keys, ramClientTokenKey(acct, region, share.ClientToken))
	}
	if share.DeleteClientToken != "" {
		keys = append(keys, ramDeleteTokenKey(acct, region, share.DeleteClientToken))
	}
	for _, key := range keys {
		data, err := p.state.Get(goCtx, ramNamespace, key)
		if err != nil {
			return fmt.Errorf("ram drop token entry get %s: %w", key, err)
		}
		if string(data) != share.ResourceShareArn {
			continue
		}
		if err := p.state.Delete(goCtx, ramNamespace, key); err != nil {
			return fmt.Errorf("ram drop token entry delete %s: %w", key, err)
		}
	}
	return nil
}
