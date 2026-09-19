package emulator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A repository response carries the nine members API_Repository publishes and nothing else (#1090).
//
// ECRRepository is a persisted state record and it was marshaled straight onto the wire at all three
// sites that answer a repository. Six of its fields are substrate's own bookkeeping, so all three
// answered members no ECR page publishes — AccountID and Region unconditionally, since neither
// carried omitempty, and Tags, LifecyclePolicy, RepositoryPolicy and ever_tagged from whichever of
// them had a value. The two policies are the worst of the six, because DescribeRepositories publishes
// no policy member at all.
//
// Every assertion here reads the **raw** response body rather than decoding into a struct, following
// [TestIAM_PolicyShapeMembers]. That is the only form that can catch this class: a decode into a type
// that names the nine published members ignores a tenth silently, which is exactly how six
// unpublished fields survived on the wire — ecr_plugin_test.go decodes, and asserts no member at all.
// Each absence assertion is paired with a presence anchor so it cannot pass vacuously.

// newECRTestServer builds a server with only the ECR plugin registered.
func newECRTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.ECRPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize ecr plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// ecrCall sends an ECR JSON-protocol request and returns the status and the raw body.
//
// X-Amz-Target is how ECR names an operation, and the server derives the operation from it, so this
// is the request shape AWS's own sample request carries.
func ecrCall(t *testing.T, ts *httptest.Server, op string, body map[string]any) (int, string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/",
		strings.NewReader(string(data)))
	require.NoError(t, err)
	req.Host = "api.ecr.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// ecrCallOK sends a request that must succeed and returns its raw body.
func ecrCallOK(t *testing.T, ts *httptest.Server, op string, body map[string]any) string {
	t.Helper()
	status, raw := ecrCall(t, ts, op, body)
	require.Equal(t, http.StatusOK, status, "%s: %s", op, raw)
	return raw
}

// ecrErrorCode reports the Code an ECR refusal carries. ECR speaks application/x-amz-json-1.1, so a
// refusal is a JSON document naming the code in __type, Code and message.
func ecrErrorCode(t *testing.T, raw string) string {
	t.Helper()
	var out struct {
		Code string `json:"Code"`
	}
	if json.Unmarshal([]byte(raw), &out) != nil {
		return ""
	}
	return out.Code
}

// ecrBookkeepingMembers is the set of field names ECRRepository carries that no ECR page publishes.
//
// Spelled as the json tags the state record uses, because those are the names that reached the wire.
var ecrBookkeepingMembers = []string{
	"AccountID", "Region", "Tags", "LifecyclePolicy", "RepositoryPolicy", "ever_tagged",
}

// TestECR_RepositoryResponsesCarryNoBookkeepingMember is #1090's first criterion, asserted at all
// three sites that answer a repository.
//
// The repository is created *with* a tag and then given both a lifecycle policy and a repository
// policy, so every one of the six fields has a value to leak: four of them are omitempty, and an
// assertion against an empty record would pass whether or not the projection exists.
func TestECR_RepositoryResponsesCarryNoBookkeepingMember(t *testing.T) {
	ts := newECRTestServer(t)

	created := ecrCallOK(t, ts, "CreateRepository", map[string]any{
		"repositoryName": "leaky",
		"tags":           []map[string]string{{"Key": "Team", "Value": "platform"}},
	})
	ecrCallOK(t, ts, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "leaky",
		"lifecyclePolicyText": `{"rules":[]}`,
	})
	ecrCallOK(t, ts, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "leaky",
		"policyText":     `{"Version":"2012-10-17","Statement":[]}`,
	})
	described := ecrCallOK(t, ts, "DescribeRepositories", map[string]any{})

	// The tag and both policies are readable where AWS puts them, which is the reason none of the
	// three belongs on the repository shape.
	assert.Contains(t, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/leaky",
	}), "platform", "the tag is still readable through the operation that publishes it")
	assert.Contains(t, ecrCallOK(t, ts, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "leaky",
	}), "lifecyclePolicyText", "and so is the lifecycle policy")

	deleted := ecrCallOK(t, ts, "DeleteRepository", map[string]any{"repositoryName": "leaky"})

	for _, tc := range []struct {
		operation string
		body      string
	}{
		{"CreateRepository", created},
		{"DescribeRepositories", described},
		{"DeleteRepository", deleted},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			require.Contains(t, tc.body, `"repositoryName":"leaky"`,
				"presence anchor: the repository must be in the body for an absence to mean anything")
			for _, member := range ecrBookkeepingMembers {
				assert.NotContains(t, tc.body, member,
					"%s answered %q, which no ECR page publishes", tc.operation, member)
			}
		})
	}
}

// TestECR_RepositoryReportsImageTagMutability pins the published member substrate never decoded and
// never reported, and its published default.
//
// API_CreateRepository publishes the default in as many words — "If this parameter is omitted, the
// default setting of MUTABLE will be used" — and AWS's own sample response for a request carrying
// only repositoryName renders "imageTagMutability":"MUTABLE", so the default is observable rather
// than inferred.
func TestECR_RepositoryReportsImageTagMutability(t *testing.T) {
	ts := newECRTestServer(t)

	created := ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "defaulted"})
	assert.Contains(t, created, `"imageTagMutability":"MUTABLE"`,
		"an omitted setting takes the default the page publishes")

	immutable := ecrCallOK(t, ts, "CreateRepository", map[string]any{
		"repositoryName":     "pinned",
		"imageTagMutability": "IMMUTABLE",
	})
	assert.Contains(t, immutable, `"imageTagMutability":"IMMUTABLE"`,
		"a supplied setting is answered, rather than the request member being discarded")

	// And it survives to the two sites that read the record back, which is where a decoded-then-
	// dropped member would show up as the default again.
	described := ecrCallOK(t, ts, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"pinned"},
	})
	assert.Contains(t, described, `"imageTagMutability":"IMMUTABLE"`)
	deleted := ecrCallOK(t, ts, "DeleteRepository", map[string]any{"repositoryName": "pinned"})
	assert.Contains(t, deleted, `"imageTagMutability":"IMMUTABLE"`)

	// imageTagMutabilityExclusionFilters is the one published member substrate does not model, and it
	// is absent rather than present and empty — AWS's own sample response omits it too, so its
	// absence is a shape the page publishes (#1013).
	assert.NotContains(t, created, "imageTagMutabilityExclusionFilters")
}

// TestECR_ImageTagMutabilityOutsideTheEnumIsRefused is why the member is checked rather than merely
// stored: it is reported on all three repository responses, so an unchecked value would put a string
// the page's Valid Values exclude onto the wire under a published name.
func TestECR_ImageTagMutabilityOutsideTheEnumIsRefused(t *testing.T) {
	ts := newECRTestServer(t)

	status, body := ecrCall(t, ts, "CreateRepository", map[string]any{
		"repositoryName":     "bad-setting",
		"imageTagMutability": "SOMETIMES",
	})
	assert.Equal(t, http.StatusBadRequest, status, "body was %s", body)
	assert.Equal(t, "InvalidParameterException", ecrErrorCode(t, body),
		"the code CreateRepository publishes for an invalid parameter")

	// The two _WITH_EXCLUSION forms are accepted even though the filters they accompany are
	// unmodelled, because those filters are Required: No — refusing the value would be substrate
	// inventing a bound the page does not publish.
	for _, value := range []string{"IMMUTABLE_WITH_EXCLUSION", "MUTABLE_WITH_EXCLUSION"} {
		body := ecrCallOK(t, ts, "CreateRepository", map[string]any{
			"repositoryName":     strings.ToLower(strings.ReplaceAll(value, "_", "-")),
			"imageTagMutability": value,
		})
		assert.Contains(t, body, `"imageTagMutability":"`+value+`"`)
	}
}

// ecrCreatedAt matches the createdAt member with a JSON number for its value.
var ecrCreatedAt = regexp.MustCompile(`"createdAt":-?[0-9]`)

// TestECR_CreatedAtIsEpochSeconds pins the type of the one timestamp member, which the state record's
// time.Time rendered as an RFC3339 string.
//
// ECR speaks application/x-amz-json-1.1, whose timestamps are epoch seconds: AWS's own sample
// response answers 1.563223656E9, and the SDK v2 decoder calls ParseEpochSeconds on a timestamp
// member, which a quoted string does not satisfy. So a caller using the SDK could not decode the
// response at all — a stricter failure than a leaked member.
func TestECR_CreatedAtIsEpochSeconds(t *testing.T) {
	ts := newECRTestServer(t)

	created := ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "timed"})
	assert.Regexp(t, ecrCreatedAt, created, "createdAt must be a number, not a quoted string")
	assert.NotContains(t, created, `"createdAt":"`,
		"an RFC3339 string is what the persisted time.Time rendered as")

	described := ecrCallOK(t, ts, "DescribeRepositories", map[string]any{})
	assert.Regexp(t, ecrCreatedAt, described)
}
