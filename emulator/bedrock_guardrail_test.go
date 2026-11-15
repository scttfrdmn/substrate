package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A guardrail's seeded blocklist, applied by ApplyGuardrail and InvokeModel alike (#1183).

// brSeedBlocklist posts one guardrail blocklist seed.
func brSeedBlocklist(t *testing.T, ts *httptest.Server, seed map[string]any) {
	t.Helper()
	brControl(t, ts, http.MethodPost, "/v1/bedrock/guardrail-blocklist", seed)
}

// brControl issues one control-plane request and requires a 200.
func brControl(t *testing.T, ts *httptest.Server, method, path string, body map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, raw)
}

// brInvoke sends InvokeModel with the given headers and answers the status and the body.
func brInvoke(t *testing.T, ts *httptest.Server, headers map[string]string, body any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/model/anthropic.claude-test-v1:0/invoke", bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = "bedrock-runtime.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, raw
}

// brMessages is a Claude Messages request body carrying text.
func brMessages(text string) map[string]any {
	return map[string]any{
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens":        64,
		"messages": []map[string]any{{
			"role": "user", "content": []map[string]any{{"type": "text", "text": text}},
		}},
	}
}

// brGuardrailHeaders names guardrail gr1, version 1.
var brGuardrailHeaders = map[string]string{
	"X-Amzn-Bedrock-GuardrailIdentifier": "gr1",
	"X-Amzn-Bedrock-GuardrailVersion":    "1",
}

func TestBedrockGuardrail_InvokeModelAppliesTheSeededDecision(t *testing.T) {
	t.Parallel()
	setup := newBedrockRuntimeTestServer(t)
	brSeedBlocklist(t, setup.server, map[string]any{"guardrailIdentifier": "gr1", "terms": []string{"forbidden"}})

	_, plain := brInvoke(t, setup.server, nil, brMessages("say hello"))
	cases := []struct {
		name        string
		headers     map[string]string
		text        string
		intervened  bool
		sameAsPlain bool
	}{
		{"a blocked term under the guardrail", brGuardrailHeaders, "this is forbidden", true, false},
		{"the guardrail named by ARN", map[string]string{
			"X-Amzn-Bedrock-GuardrailIdentifier": "arn:aws:bedrock:us-east-1:123456789012:guardrail/gr1",
			"X-Amzn-Bedrock-GuardrailVersion":    "DRAFT",
		}, "this is forbidden", true, false},
		{"clean text under the guardrail", brGuardrailHeaders, "say hello", false, true},
		{"a blocked term with no guardrail", nil, "this is forbidden", false, false},
		{"a blocked term under another guardrail", map[string]string{
			"X-Amzn-Bedrock-GuardrailIdentifier": "gr2", "X-Amzn-Bedrock-GuardrailVersion": "1",
		}, "this is forbidden", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := brInvoke(t, setup.server, tc.headers, brMessages(tc.text))
			require.Equal(t, http.StatusOK, status, "%s", raw)
			if tc.intervened {
				assert.JSONEq(t, `{"amazon-bedrock-guardrailAction":"INTERVENED"}`, string(raw))
				return
			}
			assert.NotContains(t, string(raw), "amazon-bedrock-guardrailAction",
				"a non-intervening invocation answers the inference unchanged")
			if tc.sameAsPlain {
				assert.Equal(t, string(plain), string(raw), "byte for byte")
			}
		})
	}
}

func TestBedrockGuardrail_ASeededResponseWinsOverTheGuardrail(t *testing.T) {
	t.Parallel()
	setup := newBedrockRuntimeTestServer(t)
	brSeedBlocklist(t, setup.server, map[string]any{"terms": []string{"forbidden"}})
	brControl(t, setup.server, http.MethodPost, "/v1/bedrock-runtime/responses",
		map[string]any{"body": map[string]string{"completion": "seeded"}})

	status, raw := brInvoke(t, setup.server, brGuardrailHeaders, brMessages("this is forbidden"))
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"completion":"seeded"}`, string(raw))
}

func TestBedrockGuardrail_ThePublishedRequestErrorsAreValidationException(t *testing.T) {
	t.Parallel()
	setup := newBedrockRuntimeTestServer(t)
	withConfig := brMessages("hi")
	withConfig["amazon-bedrock-guardrailConfig"] = map[string]any{"tagSuffix": "x"}
	cases := []struct {
		name    string
		headers map[string]string
		body    any
	}{
		{"guardrailConfig in the body with no identifier", nil, withConfig},
		{"an identifier with no version", map[string]string{"X-Amzn-Bedrock-GuardrailIdentifier": "gr1"}, brMessages("hi")},
		{"a guardrail with a content type other than application/json",
			map[string]string{"X-Amzn-Bedrock-GuardrailIdentifier": "gr1", "X-Amzn-Bedrock-GuardrailVersion": "1",
				"Content-Type": "text/plain"}, brMessages("hi")},
		{"a version outside its pattern",
			map[string]string{"X-Amzn-Bedrock-GuardrailIdentifier": "gr1", "X-Amzn-Bedrock-GuardrailVersion": "v1"}, brMessages("hi")},
		{"an identifier outside its pattern",
			map[string]string{"X-Amzn-Bedrock-GuardrailIdentifier": "Not_An_ID", "X-Amzn-Bedrock-GuardrailVersion": "1"}, brMessages("hi")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := brInvoke(t, setup.server, tc.headers, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s", raw)
			assert.Contains(t, string(raw), "ValidationException")
		})
	}

	// A version alone applies no guardrail, and the page lists no error for it.
	status, raw := brInvoke(t, setup.server, map[string]string{"X-Amzn-Bedrock-GuardrailVersion": "1"}, brMessages("hi"))
	assert.Equal(t, http.StatusOK, status, "%s", raw)
}

func TestBedrockGuardrail_SeedsAreScopedAndTheMostSpecificWins(t *testing.T) {
	t.Parallel()
	setup := newBedrockRuntimeTestServer(t)
	apply := func(id, version, text string) string {
		resp := brRequest(t, setup.server, id, version, brPayload("INPUT", text))
		var out struct {
			Action string `json:"action"`
		}
		require.NoError(t, json.Unmarshal(brBody(t, resp), &out))
		return out.Action
	}

	// A seed for another Region or account never applies.
	brSeedBlocklist(t, setup.server, map[string]any{"region": "eu-west-1", "terms": []string{"alpha"}})
	brSeedBlocklist(t, setup.server, map[string]any{"accountId": "210987654321", "terms": []string{"alpha"}})
	assert.Equal(t, "NONE", apply("g", "1", "alpha"))

	// The guardrail's own version seed wins over its every-version seed, which wins over "*".
	brSeedBlocklist(t, setup.server, map[string]any{"terms": []string{"beta"}})
	brSeedBlocklist(t, setup.server, map[string]any{"guardrailIdentifier": "g", "terms": []string{"gamma"}})
	brSeedBlocklist(t, setup.server, map[string]any{"guardrailIdentifier": "g", "guardrailVersion": "2", "terms": []string{"delta"}})
	assert.Equal(t, "GUARDRAIL_INTERVENED", apply("g", "2", "delta"))
	assert.Equal(t, "NONE", apply("g", "2", "gamma"), "version 2's own seed replaces the every-version one")
	assert.Equal(t, "GUARDRAIL_INTERVENED", apply("g", "1", "gamma"))
	assert.Equal(t, "NONE", apply("g", "1", "beta"))
	assert.Equal(t, "GUARDRAIL_INTERVENED", apply("other", "1", "beta"))

	// Clearing one seed by its key leaves the rest; clearing with no query removes them all.
	brControl(t, setup.server, http.MethodDelete, "/v1/bedrock/guardrail-blocklist?guardrailIdentifier=g&guardrailVersion=2", nil)
	assert.Equal(t, "GUARDRAIL_INTERVENED", apply("g", "2", "gamma"))
	brControl(t, setup.server, http.MethodDelete, "/v1/bedrock/guardrail-blocklist", nil)
	assert.Equal(t, "NONE", apply("other", "1", "beta"))
}

func TestBedrockGuardrail_ApplyGuardrailWritesNoState(t *testing.T) {
	t.Parallel()
	setup := newBedrockRuntimeTestServer(t)
	resp := brRequest(t, setup.server, "fresh", "1", brPayload("INPUT", "anything"))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	brBody(t, resp)

	keys, err := setup.state.List(t.Context(), "bedrock-runtime", "")
	require.NoError(t, err)
	assert.Empty(t, keys, "an ApplyGuardrail is a read and stores nothing")
}

func TestBedrockGuardrail_AnUndecodableSeedIsAnError(t *testing.T) {
	t.Parallel()
	setup := newBedrockRuntimeTestServer(t)
	require.NoError(t, setup.state.Put(t.Context(), "bedrock-runtime-ctrl", "guardrail-blocklist:*/*/*/*", []byte("{not json")))
	resp := brRequest(t, setup.server, "g", "1", brPayload("INPUT", "anything"))
	brBody(t, resp)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode, "a seed that does not decode is an error, not an empty blocklist")
}
