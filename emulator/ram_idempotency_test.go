package emulator_test

// CreateResourceShare and DeleteResourceShare honor and echo clientToken (#1170). Every assertion is
// over the wire.

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ramCreated is the published CreateResourceShare response, read off the wire.
type ramCreated struct {
	ClientToken   string `json:"clientToken"`
	ResourceShare struct {
		ResourceShareArn string `json:"resourceShareArn"`
		Name             string `json:"name"`
		Status           string `json:"status"`
	} `json:"resourceShare"`
}

// ramCreate posts one CreateResourceShare and decodes a 200.
func ramCreate(t *testing.T, ts *emulator.TestServer, body map[string]any) ramCreated {
	t.Helper()
	status, raw := ramOwnershipCall(t, ts, "/createresourceshare", body)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var out ramCreated
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	return out
}

// ramDeleteQuery sends DeleteResourceShare the way the page publishes it: DELETE with both members in
// the query string and no body.
func ramDeleteQuery(t *testing.T, ts *emulator.TestServer, arn, token string) (int, []byte) {
	t.Helper()
	q := url.Values{"resourceShareArn": {arn}}
	if token != "" {
		q.Set("clientToken", token)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/deleteresourceshare?"+q.Encode(), nil)
	require.NoError(t, err)
	req.Host = "ram.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, out
}

// ramShareCount is how many shares GetResourceShares lists for the caller.
func ramShareCount(t *testing.T, ts *emulator.TestServer) int {
	t.Helper()
	return len(ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF"}).ResourceShares)
}

func TestRAMIdempotency_CreateHonorsClientToken(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	params := map[string]any{
		"name": "idem", "clientToken": "token-1",
		"principals":   []string{"111111111111"},
		"resourceArns": []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"},
	}
	first := ramCreate(t, ts, params)
	require.NotEmpty(t, first.ResourceShare.ResourceShareArn)
	assert.Equal(t, "token-1", first.ClientToken, "the caller's token is echoed")

	// The same token and the same parameters, with the keys in another order: the original share.
	again := ramCreate(t, ts, map[string]any{
		"resourceArns": []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"},
		"principals":   []string{"111111111111"},
		"clientToken":  "token-1", "name": "idem",
	})
	assert.Equal(t, first.ResourceShare.ResourceShareArn, again.ResourceShare.ResourceShareArn, "a retry answers the original share")
	assert.Equal(t, "token-1", again.ClientToken)
	assert.Equal(t, 1, ramShareCount(t, ts), "and creates nothing")

	for _, tc := range []struct {
		name   string
		change map[string]any
	}{
		{"a different name", map[string]any{"name": "other"}},
		{"different principals", map[string]any{"principals": []string{"222222222222"}}},
		{"an extra member substrate does not model", map[string]any{"permissionArns": []string{"arn:aws:ram::aws:permission/X"}}},
		{"an omitted default spelled out", map[string]any{"allowExternalPrincipals": true}},
	} {
		body := map[string]any{}
		for k, v := range params {
			body[k] = v
		}
		for k, v := range tc.change {
			body[k] = v
		}
		status, code := ramOwnershipErrorCode(t, ts, "/createresourceshare", body)
		assert.Equal(t, http.StatusBadRequest, status, tc.name)
		assert.Equal(t, "IdempotentParameterMismatchException", code, tc.name)
	}
	assert.Equal(t, 1, ramShareCount(t, ts), "a mismatched retry creates nothing")

	// A different token is a different request.
	other := ramCreate(t, ts, map[string]any{"name": "idem", "clientToken": "token-2"})
	assert.NotEqual(t, first.ResourceShare.ResourceShareArn, other.ResourceShare.ResourceShareArn)
}

func TestRAMIdempotency_AnAbsentTokenIsMintedAndUsable(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	first := ramCreate(t, ts, map[string]any{"name": "minted"})
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, first.ClientToken,
		"a token the request omits is generated, in the UUID shape the page recommends")

	// Two token-less creates are two requests, as they are against AWS.
	second := ramCreate(t, ts, map[string]any{"name": "minted"})
	assert.NotEqual(t, first.ClientToken, second.ClientToken)
	assert.NotEqual(t, first.ResourceShare.ResourceShareArn, second.ResourceShare.ResourceShareArn)

	// The minted token is the share's, so a retry under it is idempotent.
	retry := ramCreate(t, ts, map[string]any{"name": "minted", "clientToken": first.ClientToken})
	assert.Equal(t, first.ResourceShare.ResourceShareArn, retry.ResourceShare.ResourceShareArn)
	assert.Equal(t, 2, ramShareCount(t, ts))
}

func TestRAMIdempotency_ACreateTokenDoesNotMatchADeletedShare(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	first := ramCreate(t, ts, map[string]any{"name": "phoenix", "clientToken": "t"})
	status, raw := ramDeleteQuery(t, ts, first.ResourceShare.ResourceShareArn, "")
	require.Equal(t, http.StatusOK, status, "%s", raw)

	again := ramCreate(t, ts, map[string]any{"name": "phoenix", "clientToken": "t"})
	assert.NotEqual(t, first.ResourceShare.ResourceShareArn, again.ResourceShare.ResourceShareArn,
		"a retry does not answer a DELETED share as the outcome of a create")
	assert.Equal(t, "ACTIVE", again.ResourceShare.Status)
}

func TestRAMIdempotency_DeleteHonorsClientToken(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	a := ramCreate(t, ts, map[string]any{"name": "a"}).ResourceShare.ResourceShareArn
	b := ramCreate(t, ts, map[string]any{"name": "b"}).ResourceShare.ResourceShareArn

	type deleted struct {
		ClientToken string `json:"clientToken"`
		ReturnValue bool   `json:"returnValue"`
	}
	decode := func(raw []byte) deleted {
		var out deleted
		require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
		return out
	}

	status, raw := ramDeleteQuery(t, ts, a, "del-1")
	require.Equal(t, http.StatusOK, status, "%s", raw)
	assert.Equal(t, deleted{ClientToken: "del-1", ReturnValue: true}, decode(raw), "the caller's token is echoed")

	// The retry under the same token succeeds again rather than being a second delete.
	status, raw = ramDeleteQuery(t, ts, a, "del-1")
	require.Equal(t, http.StatusOK, status, "a retried delete is idempotent: %s", raw)
	assert.Equal(t, deleted{ClientToken: "del-1", ReturnValue: true}, decode(raw))

	// Without that token, a second delete is still #1171's invalid state transition.
	status, code := ramOwnershipErrorCode(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": a, "clientToken": "del-2"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidStateTransitionException", code)

	// The same token on another share is a mismatch, and deletes nothing.
	status, code = ramOwnershipErrorCode(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": b, "clientToken": "del-1"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "IdempotentParameterMismatchException", code)
	assert.Len(t, ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF", "resourceShareStatus": "ACTIVE"}).ResourceShares, 1)

	// An absent token is minted and echoed.
	status, raw = ramDeleteQuery(t, ts, b, "")
	require.Equal(t, http.StatusOK, status, "%s", raw)
	minted := decode(raw)
	assert.True(t, minted.ReturnValue)
	assert.NotEmpty(t, minted.ClientToken)
}

// TestRAMIdempotency_ConcurrentCreatesUnderOneTokenCreateOneShare pins the lock: without one held
// across the scan and the write, every concurrent create finds no prior share and each creates one.
func TestRAMIdempotency_ConcurrentCreatesUnderOneTokenCreateOneShare(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	const n = 8
	arns := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			status, raw := ramOwnershipCall(t, ts, "/createresourceshare", map[string]any{"name": "race", "clientToken": "same"})
			if status == http.StatusOK {
				var out ramCreated
				if json.Unmarshal(raw, &out) == nil {
					arns[i] = out.ResourceShare.ResourceShareArn
				}
			}
		})
	}
	wg.Wait()
	for _, arn := range arns {
		assert.Equal(t, arns[0], arn, "every concurrent create answers the one share")
	}
	assert.Equal(t, 1, ramShareCount(t, ts))
}

// TestRAMIdempotency_AReplayReproducesTheMintedToken pins that a token substrate mints comes from the
// request's ID mint, so a replayed CreateResourceShare and DeleteResourceShare answer the bodies the
// recording answered.
func TestRAMIdempotency_AReplayReproducesTheMintedToken(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	arn := ramCreate(t, ts, map[string]any{"name": "replayed"}).ResourceShare.ResourceShareArn
	status, raw := ramDeleteQuery(t, ts, arn, "")
	require.Equal(t, http.StatusOK, status, "%s", raw)

	engine := emulator.NewReplayEngine(
		ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
	)
	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Equal(t, 2, results.SuccessEvents)
	for _, diff := range results.Differences {
		assert.False(t, strings.Contains(diff.Field, "clientToken"),
			"%s replayed a different clientToken: recorded %v, replayed %v", diff.Operation, diff.Expected, diff.Actual)
	}
}
