package emulator_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// StartRun as API_StartRun publishes it (#1166): three required request members, eight response
// members, and requestId as the idempotency token. Until #1166 an empty body answered 201 with one
// member.

// omicsStartRequest is a StartRun body carrying the three required members, with overrides applied.
func omicsStartRequest(requestID string, overrides map[string]any) map[string]any {
	body := map[string]any{
		"workflowId": "1234567",
		"name":       "start-" + requestID,
		"requestId":  requestID,
		"roleArn":    "arn:aws:iam::123456789012:role/omics",
		"outputUri":  "s3://omics-out/runs/",
	}
	for k, v := range overrides {
		if v == nil {
			delete(body, k)
			continue
		}
		body[k] = v
	}
	return body
}

// omicsListedRunIDs answers the IDs ListRuns reports.
func omicsListedRunIDs(t *testing.T, baseURL string) []string {
	t.Helper()
	code, body := omicsWire(t, baseURL, http.MethodGet, "/run", nil)
	require.Equal(t, http.StatusOK, code, "ListRuns: %s", body)
	var out struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	ids := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestOmicsStartRun_RefusesWhatThePageRefuses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body any
		want string
	}{
		{"an empty object", map[string]any{}, "outputUri is required"},
		{"no outputUri", omicsStartRequest("r", map[string]any{"outputUri": nil}), "outputUri is required"},
		{"no requestId", omicsStartRequest("r", map[string]any{"requestId": nil}), "requestId is required"},
		{"no roleArn", omicsStartRequest("r", map[string]any{"roleArn": nil}), "roleArn is required"},
		{"a roleArn that is not an ARN", omicsStartRequest("r", map[string]any{"roleArn": "OmicsRole"}), "roleArn must match the pattern arn:.+"},
		{"an unpublished networkingMode", omicsStartRequest("r", map[string]any{"networkingMode": "PUBLIC"}), "networkingMode must be one of RESTRICTED, VPC"},
		{"an unpublished outputUriPrefixMode", omicsStartRequest("r", map[string]any{"outputUriPrefixMode": "NONE"}),
			"outputUriPrefixMode must be one of SERVICE_DEFINED, NONE_WITH_OVERWRITE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			code, body := omicsWire(t, ts.URL, http.MethodPost, "/run", tc.body)
			assert.Equal(t, http.StatusBadRequest, code)
			var e struct {
				Code    string `json:"Code"`
				Message string `json:"Message"`
			}
			require.NoError(t, json.Unmarshal(body, &e), "%s", body)
			assert.Equal(t, "ValidationException", e.Code)
			assert.Equal(t, tc.want, e.Message)
			assert.Empty(t, omicsListedRunIDs(t, ts.URL), "a refused start starts nothing")
		})
	}
}

var omicsUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestOmicsStartRun_AnswersThePublishedMembers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		overrides     map[string]any
		wantKeys      []string
		wantMode      string
		wantOutputFmt func(id string) string
	}{
		{
			name:          "defaults",
			wantKeys:      []string{"arn", "id", "networkingMode", "runOutputUri", "status", "uuid"},
			wantMode:      "RESTRICTED",
			wantOutputFmt: func(id string) string { return "s3://omics-out/runs/" + id },
		},
		{
			name: "tags, VPC and no service prefix",
			overrides: map[string]any{
				"tags": map[string]string{"team": "genomics"}, "networkingMode": "VPC",
				"outputUriPrefixMode": "NONE_WITH_OVERWRITE",
			},
			wantKeys:      []string{"arn", "id", "networkingMode", "runOutputUri", "status", "tags", "uuid"},
			wantMode:      "VPC",
			wantOutputFmt: func(string) string { return "s3://omics-out/runs/" },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			code, body := omicsWire(t, ts.URL, http.MethodPost, "/run", omicsStartRequest("members", tc.overrides))
			require.Equal(t, http.StatusCreated, code, "%s", body)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &raw))
			assert.Equal(t, tc.wantKeys, slices.Sorted(maps.Keys(raw)),
				"configuration is absent: substrate models no configuration to resolve it from")

			var out struct {
				Arn            string            `json:"arn"`
				ID             string            `json:"id"`
				NetworkingMode string            `json:"networkingMode"`
				RunOutputURI   string            `json:"runOutputUri"`
				Status         string            `json:"status"`
				Tags           map[string]string `json:"tags"`
				UUID           string            `json:"uuid"`
			}
			require.NoError(t, json.Unmarshal(body, &out))
			assert.Equal(t, "arn:aws:omics:us-east-1:123456789012:run/"+out.ID, out.Arn)
			assert.Regexp(t, `^[1-9][0-9]{9}$`, out.ID)
			assert.Regexp(t, omicsUUIDPattern, out.UUID)
			assert.Equal(t, tc.wantMode, out.NetworkingMode)
			assert.Equal(t, tc.wantOutputFmt(out.ID), out.RunOutputURI)
			assert.Equal(t, "COMPLETED", out.Status)

			// GetRun answers the same identity.
			code, got := omicsWire(t, ts.URL, http.MethodGet, "/run/"+out.ID, nil)
			require.Equal(t, http.StatusOK, code, "%s", got)
			var run struct {
				Arn  string `json:"arn"`
				UUID string `json:"uuid"`
			}
			require.NoError(t, json.Unmarshal(got, &run))
			assert.Equal(t, out.Arn, run.Arn)
			assert.Equal(t, out.UUID, run.UUID)
		})
	}
}

func TestOmicsStartRun_ARepeatedRequestIDStartsTheRunOnce(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	code, first := omicsWire(t, ts.URL, http.MethodPost, "/run", omicsStartRequest("once", nil))
	require.Equal(t, http.StatusCreated, code, "%s", first)

	// A retry, even one whose body differs, answers the first run: the page describes the duplicate
	// as starting "the run only once", not as a conflict.
	code, again := omicsWire(t, ts.URL, http.MethodPost, "/run", omicsStartRequest("once", map[string]any{"name": "renamed"}))
	require.Equal(t, http.StatusCreated, code, "%s", again)
	assert.JSONEq(t, string(first), string(again), "the retry answers the run the first request started")
	require.Len(t, omicsListedRunIDs(t, ts.URL), 1, "the retry started nothing")

	code, other := omicsWire(t, ts.URL, http.MethodPost, "/run", omicsStartRequest("twice", nil))
	require.Equal(t, http.StatusCreated, code, "%s", other)
	assert.Len(t, omicsListedRunIDs(t, ts.URL), 2, "a new requestId starts a new run")

	// The token lives as long as its run: once the run is deleted, the requestId starts a new one.
	var started struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(first, &started))
	code, body := omicsWire(t, ts.URL, http.MethodDelete, "/run/"+started.ID, nil)
	require.Equal(t, http.StatusAccepted, code, "DeleteRun: %s", body)
	code, reborn := omicsWire(t, ts.URL, http.MethodPost, "/run", omicsStartRequest("once", nil))
	require.Equal(t, http.StatusCreated, code, "%s", reborn)
	var rebornRun struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(reborn, &rebornRun))
	assert.NotEqual(t, started.ID, rebornRun.ID, "a deleted run releases its requestId")
}

// TestOmicsStartRun_IdentifiersComeFromTheRequestMint: two plugins, each with its own state as two
// processes would have, handed the same request id mint the same run ID and uuid — which a per-process
// seeded PRNG, the source until #1166, could not.
func TestOmicsStartRun_IdentifiersComeFromTheRequestMint(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(omicsStartRequest("mint", nil))
	require.NoError(t, err)

	var minted []string
	for range 2 {
		p := &emulator.OmicsPlugin{}
		ctx, _ := wireSetup(t, p, "req-omics-mint")
		got := omicsDirect(t, p, ctx, http.MethodPost, "/run", string(body))
		var out struct {
			ID   string `json:"id"`
			UUID string `json:"uuid"`
		}
		require.NoError(t, json.Unmarshal(got, &out))
		minted = append(minted, out.ID+"/"+out.UUID)
	}
	assert.Equal(t, minted[0], minted[1])
}

// TestOmicsStartRun_TheRequestIDIndexIsCheckedAgainstTheRun: the requestId index entry is a pointer,
// honored only when the run it names exists and carries the token, so a stale or unreadable entry
// never refuses — or wrongly deduplicates — a start; and DeleteRun removes the entry.
func TestOmicsStartRun_TheRequestIDIndexIsCheckedAgainstTheRun(t *testing.T) {
	t.Parallel()
	const entryKey = "run_reqid:123456789012/us-east-1/indexed"
	body, err := json.Marshal(omicsStartRequest("indexed", nil))
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		entry string
	}{
		{"an entry naming a run that is gone", `"1999999999"`},
		{"an entry that does not decode", `{`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := &emulator.OmicsPlugin{}
			ctx, state := wireSetup(t, p, "req-omics-index")
			require.NoError(t, state.Put(t.Context(), "omics", entryKey, []byte(tc.entry)))
			got := omicsDirect(t, p, ctx, http.MethodPost, "/run", string(body))
			var out struct {
				ID string `json:"id"`
			}
			require.NoError(t, json.Unmarshal(got, &out))
			assert.NotEqual(t, "1999999999", out.ID, "a stale entry does not answer a run that is gone")

			entry, err := state.Get(t.Context(), "omics", entryKey)
			require.NoError(t, err)
			assert.JSONEq(t, `"`+out.ID+`"`, string(entry), "the start records the run it started")

			omicsDirect(t, p, ctx, http.MethodDelete, "/run/"+out.ID, "")
			entry, err = state.Get(t.Context(), "omics", entryKey)
			require.NoError(t, err)
			assert.Nil(t, entry, "DeleteRun removes the entry with the run")
		})
	}
}
