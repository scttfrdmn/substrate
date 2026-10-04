package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A secret version's identity is its ClientRequestToken (#1285).
//
// API_PutSecretValue and API_CreateSecret both say the token "becomes the VersionId of the new
// version", publish it as 32–64 characters, and build an idempotency contract on it: a resubmitted
// token with the same value "succeeds but does nothing", and with a different value fails. Every
// assertion here is on the raw response bytes or the refusal's code and status.

// smVersionToken is a 36-character UUID-shaped token, the shape AWS's own samples and the SDKs use.
const smVersionToken = "12345678-90ab-4def-8123-4567890abcde"

// smVersionHarness is a Secrets Manager plugin over a store a test can fault.
type smVersionHarness struct {
	t     *testing.T
	p     *emulator.SecretsManagerPlugin
	ctx   *emulator.RequestContext
	fault *cfFaultStateManager
}

func newSMVersionHarness(t *testing.T) *smVersionHarness {
	t.Helper()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	p := &emulator.SecretsManagerPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   fault,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.SecretsManagerPlugin.Initialize")
	return &smVersionHarness{t: t, p: p, fault: fault, ctx: &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-sm-version", IDs: emulator.NewIDMint("req-sm-version"),
	}}
}

// call issues one operation and returns the status and body, or the refusal's status and code. A
// non-AWS error is returned as err.
func (h *smVersionHarness) call(op string, body map[string]any) (status int, out string, err error) {
	h.t.Helper()
	raw, mErr := json.Marshal(body)
	require.NoError(h.t, mErr, "marshal %s", op)
	resp, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "secretsmanager", Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "secretsmanager." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code, nil
	}
	if err != nil {
		return 0, "", err
	}
	return resp.StatusCode, string(resp.Body), nil
}

// ok issues one operation that must answer 200 and returns its body.
func (h *smVersionHarness) ok(op string, body map[string]any) string {
	h.t.Helper()
	status, out, err := h.call(op, body)
	require.NoError(h.t, err, "%s", op)
	require.Equal(h.t, http.StatusOK, status, "%s: %s", op, out)
	return out
}

// versionID decodes VersionId from a response body.
func smVersionIDOf(t *testing.T, body string) string {
	t.Helper()
	var out struct {
		VersionID string `json:"VersionId"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out), "decode: %s", body)
	return out.VersionID
}

func TestSMVersion_AMintedVersionIDIsInsideThePublishedWidth(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	created := smVersionIDOf(t, h.ok("CreateSecret", map[string]any{"Name": "minted", "SecretString": "v1"}))
	put := smVersionIDOf(t, h.ok("PutSecretValue", map[string]any{"SecretId": "minted", "SecretString": "v2"}))
	for _, id := range []string{created, put} {
		require.GreaterOrEqual(t, len(id), 32, "VersionId %q is below the published minimum of 32", id)
		require.LessOrEqual(t, len(id), 64, "VersionId %q is above the published maximum of 64", id)
		require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id, "a minted VersionId is a version-4 UUID")
	}
	require.NotEqual(t, created, put, "two versions are two draws")
}

func TestSMVersion_TheClientRequestTokenBecomesTheVersionID(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	const createToken = "EXAMPLE1-90ab-cdef-fedc-ba987SECRET1"

	created := h.ok("CreateSecret", map[string]any{"Name": "tokened", "SecretString": "first", "ClientRequestToken": createToken})
	require.Contains(t, created, `"VersionId":"`+createToken+`"`, "CreateSecret: %s", created)
	put := h.ok("PutSecretValue", map[string]any{"SecretId": "tokened", "SecretString": "second", "ClientRequestToken": smVersionToken})
	require.Contains(t, put, `"VersionId":"`+smVersionToken+`"`, "PutSecretValue: %s", put)

	// Both versions are reachable by the token they were created under.
	for token, value := range map[string]string{createToken: "first", smVersionToken: "second"} {
		got := h.ok("GetSecretValue", map[string]any{"SecretId": "tokened", "VersionId": token})
		require.Contains(t, got, `"SecretString":"`+value+`"`, "GetSecretValue by %s: %s", token, got)
		require.Contains(t, got, `"VersionId":"`+token+`"`, "GetSecretValue by %s: %s", token, got)
	}
	listed := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "tokened"})
	require.Contains(t, listed, `"VersionId":"`+smVersionToken+`"`, "ListSecretVersionIds reaches the token-named current version: %s", listed)
}

func TestSMVersion_AResubmittedTokenIsIdempotentOrRefused(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "retried", "SecretString": "v1"})
	put := map[string]any{"SecretId": "retried", "SecretString": "v2", "ClientRequestToken": smVersionToken}
	h.ok("PutSecretValue", put)
	h.ok("PutSecretValue", map[string]any{"SecretId": "retried", "SecretString": "v3"})

	// The same token and value: succeeds, reports the existing version, and moves no pointer.
	again := h.ok("PutSecretValue", put)
	require.Contains(t, again, `"VersionId":"`+smVersionToken+`"`, "a resubmitted PutSecretValue: %s", again)
	current := h.ok("GetSecretValue", map[string]any{"SecretId": "retried"})
	require.Contains(t, current, `"SecretString":"v3"`, "an ignored retry must not move AWSCURRENT back: %s", current)

	// The same token, a different value: an existing version cannot be modified.
	status, code, err := h.call("PutSecretValue", map[string]any{"SecretId": "retried", "SecretString": "changed", "ClientRequestToken": smVersionToken})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceExistsException", code)
	kept := h.ok("GetSecretValue", map[string]any{"SecretId": "retried", "VersionId": smVersionToken})
	require.Contains(t, kept, `"SecretString":"v2"`, "a refused retry must leave the version's value alone: %s", kept)

	// CreateSecret: a retried create with the same token and value is ignored; a different value is
	// refused as the version collision; a new token is the name collision it always was.
	create := map[string]any{"Name": "created-twice", "SecretString": "c1", "ClientRequestToken": smVersionToken}
	first := h.ok("CreateSecret", create)
	second := h.ok("CreateSecret", create)
	require.JSONEq(t, first, second, "a resubmitted CreateSecret answers the first create")
	for _, tc := range []struct {
		body map[string]any
		code string
	}{
		{map[string]any{"Name": "created-twice", "SecretString": "other", "ClientRequestToken": smVersionToken}, "ResourceExistsException"},
		{map[string]any{"Name": "created-twice", "SecretString": "c1", "ClientRequestToken": strings.Repeat("b", 36)}, "ResourceExistsException"},
		{map[string]any{"Name": "created-twice", "SecretString": "c1"}, "ResourceExistsException"},
	} {
		status, code, err := h.call("CreateSecret", tc.body)
		require.NoError(t, err)
		require.Equal(t, tc.code, code, "%v", tc.body)
		require.NotEqual(t, http.StatusOK, status, "%v", tc.body)
	}
}

func TestSMVersion_ATokenOutsideThePublishedWidthIsRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		token string
		ok    bool
	}{
		{"31 characters", strings.Repeat("a", 31), false},
		{"32 characters", strings.Repeat("a", 32), true},
		{"64 characters", strings.Repeat("a", 64), true},
		{"65 characters", strings.Repeat("a", 65), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newSMVersionHarness(t)
			h.ok("CreateSecret", map[string]any{"Name": "base", "SecretString": "v"})
			for _, call := range []struct {
				op   string
				body map[string]any
			}{
				{"CreateSecret", map[string]any{"Name": "fresh", "SecretString": "v", "ClientRequestToken": tc.token}},
				{"PutSecretValue", map[string]any{"SecretId": "base", "SecretString": "v2", "ClientRequestToken": tc.token}},
				{"RotateSecret", map[string]any{"SecretId": "base", "ClientRequestToken": tc.token,
					"RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:rotate"}},
			} {
				status, out, err := h.call(call.op, call.body)
				require.NoError(t, err, "%s", call.op)
				if tc.ok {
					require.Equal(t, http.StatusOK, status, "%s with a %s token: %s", call.op, tc.name, out)
					require.Contains(t, out, `"VersionId":"`+tc.token+`"`, "%s: %s", call.op, out)
					continue
				}
				require.Equal(t, http.StatusBadRequest, status, "%s with a %s token: %s", call.op, tc.name, out)
				require.Equal(t, "InvalidParameterException", out, "%s with a %s token", call.op, tc.name)
			}
		})
	}
}

// A store fault while looking up a resubmitted token's version is an error, never answered as a new
// version or as an idempotent success.
func TestSMVersion_AStoreFaultOnTheVersionLookupIsAnError(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"PutSecretValue", "CreateSecret"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			h := newSMVersionHarness(t)
			h.ok("CreateSecret", map[string]any{"Name": "faulted", "SecretString": "v"})
			h.fault.failGet = "secret_version:"
			body := map[string]any{"SecretId": "faulted", "SecretString": "v2", "ClientRequestToken": smVersionToken}
			if op == "CreateSecret" {
				body = map[string]any{"Name": "faulted", "SecretString": "v2", "ClientRequestToken": smVersionToken}
			}
			_, _, err := h.call(op, body)
			require.Error(t, err, "%s must fail on a store fault", op)
		})
	}
}
