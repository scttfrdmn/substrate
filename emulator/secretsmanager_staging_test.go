package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A secret's versions and staging labels, UpdateSecret's token, and the string/binary distinction
// (#1376). Assertions are on raw response bytes or the refusal's code and status, through #1285's
// smVersionHarness on a frozen clock.

// Three more 36-character tokens, distinct from smVersionToken.
const (
	smTokenA = "aaaaaaaa-90ab-4def-8123-4567890abcde"
	smTokenB = "bbbbbbbb-90ab-4def-8123-4567890abcde"
	smTokenC = "cccccccc-90ab-4def-8123-4567890abcde"
)

// smListedVersions decodes ListSecretVersionIds' Versions, each entry as raw members so an absent
// VersionStages is distinguishable from an empty one.
func smListedVersions(t *testing.T, body string) []map[string]json.RawMessage {
	t.Helper()
	var out struct {
		Versions []map[string]json.RawMessage `json:"Versions"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out), "decode: %s", body)
	return out.Versions
}

// smStagesByID maps each listed version to its VersionStages, "" when the member is absent.
func smStagesByID(t *testing.T, body string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, v := range smListedVersions(t, body) {
		var id string
		require.NoError(t, json.Unmarshal(v["VersionId"], &id))
		got[id] = string(v["VersionStages"])
	}
	return got
}

func TestSMStaging_CreatingAnExistingNameIs400(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "taken", "SecretString": "v1"})
	status, code, err := h.call("CreateSecret", map[string]any{"Name": "taken", "SecretString": "v2"})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, status, "API_CreateSecret publishes ResourceExistsException at 400")
	require.Equal(t, "ResourceExistsException", code)
}

func TestSMStaging_LabelsMoveAndAThirdValueDeprecatesTheFirst(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "staged", "SecretString": "v1", "ClientRequestToken": smTokenA})
	put := h.ok("PutSecretValue", map[string]any{"SecretId": "staged", "SecretString": "v2", "ClientRequestToken": smTokenB})
	require.Contains(t, put, `"VersionStages":["AWSCURRENT"]`, "PutSecretValue answers the new version's labels: %s", put)

	afterTwo := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "staged"})
	require.Equal(t, map[string]string{smTokenA: `["AWSPREVIOUS"]`, smTokenB: `["AWSCURRENT"]`}, smStagesByID(t, afterTwo),
		"AWSCURRENT moves to the new version and AWSPREVIOUS to the one it left: %s", afterTwo)

	h.ok("UpdateSecret", map[string]any{"SecretId": "staged", "SecretString": "v3", "ClientRequestToken": smTokenC})
	listed := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "staged"})
	require.Equal(t, map[string]string{smTokenB: `["AWSPREVIOUS"]`, smTokenC: `["AWSCURRENT"]`}, smStagesByID(t, listed),
		"a version left with no label is deprecated and listed only on request: %s", listed)

	all := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "staged", "IncludeDeprecated": true})
	require.Equal(t, map[string]string{smTokenA: "", smTokenB: `["AWSPREVIOUS"]`, smTokenC: `["AWSCURRENT"]`}, smStagesByID(t, all),
		"IncludeDeprecated lists the deprecated version with no VersionStages member: %s", all)
	require.Contains(t, all, `"CreatedDate":1700000000`, "each version reports its CreatedDate: %s", all)

	// The previous value is reachable by label, and the deprecated one by its ID.
	prev := h.ok("GetSecretValue", map[string]any{"SecretId": "staged", "VersionStage": "AWSPREVIOUS"})
	require.Contains(t, prev, `"SecretString":"v2"`, "%s", prev)
	require.Contains(t, prev, `"VersionStages":["AWSPREVIOUS"]`, "%s", prev)
	old := h.ok("GetSecretValue", map[string]any{"SecretId": "staged", "VersionId": smTokenA})
	require.Contains(t, old, `"SecretString":"v1"`, "%s", old)
	require.NotContains(t, old, `"VersionStages"`, "a deprecated version carries no labels: %s", old)
	cur := h.ok("GetSecretValue", map[string]any{"SecretId": "staged"})
	require.Contains(t, cur, `"SecretString":"v3"`, "%s", cur)
}

func TestSMStaging_ListSecretVersionIdsPages(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "paged", "SecretString": "v1", "ClientRequestToken": smTokenA})
	h.ok("PutSecretValue", map[string]any{"SecretId": "paged", "SecretString": "v2", "ClientRequestToken": smTokenB})

	first := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "paged", "MaxResults": 1})
	require.Len(t, smListedVersions(t, first), 1, "%s", first)
	var page struct {
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal([]byte(first), &page))
	require.NotEmpty(t, page.NextToken, "a page with more to come answers NextToken: %s", first)
	second := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "paged", "MaxResults": 1, "NextToken": page.NextToken})
	require.Len(t, smListedVersions(t, second), 1, "%s", second)
	require.NotContains(t, second, `"NextToken"`, "the last page omits NextToken: %s", second)
	require.NotEqual(t, smStagesByID(t, first), smStagesByID(t, second), "the two pages hold different versions")

	for _, tc := range []struct {
		name string
		body map[string]any
		code string
	}{
		{"MaxResults below the range", map[string]any{"SecretId": "paged", "MaxResults": 0}, "InvalidParameterException"},
		{"MaxResults above the range", map[string]any{"SecretId": "paged", "MaxResults": 101}, "InvalidParameterException"},
		{"an unissued NextToken", map[string]any{"SecretId": "paged", "NextToken": "not-a-token"}, "InvalidNextTokenException"},
	} {
		status, code, err := h.call("ListSecretVersionIds", tc.body)
		require.NoError(t, err, tc.name)
		require.Equal(t, http.StatusBadRequest, status, tc.name)
		require.Equal(t, tc.code, code, tc.name)
	}
}

func TestSMStaging_UpdateSecretTakesTheTokenAndRefusesOneInUse(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "updated", "SecretString": "v1", "ClientRequestToken": smTokenA})

	out := h.ok("UpdateSecret", map[string]any{"SecretId": "updated", "SecretString": "v2", "ClientRequestToken": smTokenB})
	require.Contains(t, out, `"VersionId":"`+smTokenB+`"`, "the token becomes the new version's VersionId: %s", out)
	got := h.ok("GetSecretValue", map[string]any{"SecretId": "updated"})
	require.Contains(t, got, `"VersionId":"`+smTokenB+`"`, "%s", got)
	require.Contains(t, got, `"SecretString":"v2"`, "%s", got)

	// "If you call this operation with a ClientRequestToken that matches an existing version's
	// VersionId, the operation results in an error" — with the same value as well as a different one.
	for _, value := range []string{"v2", "v3"} {
		status, code, err := h.call("UpdateSecret", map[string]any{"SecretId": "updated", "SecretString": value, "ClientRequestToken": smTokenB})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, status, "value %q", value)
		require.Equal(t, "ResourceExistsException", code, "value %q", value)
	}
	still := h.ok("GetSecretValue", map[string]any{"SecretId": "updated"})
	require.Contains(t, still, `"SecretString":"v2"`, "a refused update changes nothing: %s", still)

	status, code, err := h.call("UpdateSecret", map[string]any{"SecretId": "updated", "SecretString": "v4", "ClientRequestToken": "short"})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", code, "a token outside 32-64 characters")

	meta := h.ok("UpdateSecret", map[string]any{"SecretId": "updated", "Description": "only metadata", "ClientRequestToken": smTokenC})
	require.NotContains(t, meta, `"VersionId"`, "an update that creates no version answers no VersionId: %s", meta)
	listed := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "updated"})
	require.NotContains(t, listed, smTokenC, "a metadata-only update's token names no version: %s", listed)
}

func TestSMStaging_StringAndBinaryAreDifferentValues(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	const encoded = "dmFsdWU=" // the same text, sent once as SecretBinary and once as SecretString
	h.ok("CreateSecret", map[string]any{"Name": "kinds", "SecretString": "v1"})
	h.ok("PutSecretValue", map[string]any{"SecretId": "kinds", "SecretBinary": encoded, "ClientRequestToken": smVersionToken})

	got := h.ok("GetSecretValue", map[string]any{"SecretId": "kinds"})
	require.Contains(t, got, `"SecretBinary":"`+encoded+`"`, "a binary version answers SecretBinary: %s", got)
	require.NotContains(t, got, `"SecretString"`, "and omits SecretString, as API_GetSecretValue states: %s", got)

	again := h.ok("PutSecretValue", map[string]any{"SecretId": "kinds", "SecretBinary": encoded, "ClientRequestToken": smVersionToken})
	require.Contains(t, again, `"VersionId":"`+smVersionToken+`"`, "the same binary value is an idempotent retry: %s", again)
	status, code, err := h.call("PutSecretValue", map[string]any{"SecretId": "kinds", "SecretString": encoded, "ClientRequestToken": smVersionToken})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceExistsException", code, "the same text as a SecretString is a different value")

	h.ok("CreateSecret", map[string]any{"Name": "kinds-create", "SecretBinary": encoded, "ClientRequestToken": smTokenA})
	status, code, err = h.call("CreateSecret", map[string]any{"Name": "kinds-create", "SecretString": encoded, "ClientRequestToken": smTokenA})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceExistsException", code, "a retried create compares kind as well as bytes")
}

func TestSMStaging_PublishedValueAndStageRefusals(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "rules", "SecretString": "v1", "ClientRequestToken": smTokenA})
	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
		code string
	}{
		{"CreateSecret with both values", "CreateSecret", map[string]any{"Name": "both", "SecretString": "s", "SecretBinary": "Yg=="}, "InvalidParameterException"},
		{"PutSecretValue with both values", "PutSecretValue", map[string]any{"SecretId": "rules", "SecretString": "s", "SecretBinary": "Yg=="}, "InvalidParameterException"},
		{"UpdateSecret with both values", "UpdateSecret", map[string]any{"SecretId": "rules", "SecretString": "s", "SecretBinary": "Yg=="}, "InvalidParameterException"},
		{"PutSecretValue with neither value", "PutSecretValue", map[string]any{"SecretId": "rules"}, "InvalidParameterException"},
		{"PutSecretValue with an empty VersionStages", "PutSecretValue", map[string]any{"SecretId": "rules", "SecretString": "v2", "VersionStages": []string{}}, "InvalidParameterException"},
		{"PutSecretValue with 21 VersionStages", "PutSecretValue", map[string]any{"SecretId": "rules", "SecretString": "v2", "VersionStages": strings.Split(strings.Repeat("L,", 20)+"L", ",")}, "InvalidParameterException"},
		{"GetSecretValue for a stage no version carries", "GetSecretValue", map[string]any{"SecretId": "rules", "VersionStage": "AWSPENDING"}, "ResourceNotFoundException"},
		{"GetSecretValue with a VersionId and a VersionStage that disagree", "GetSecretValue", map[string]any{"SecretId": "rules", "VersionId": smTokenB, "VersionStage": "AWSCURRENT"}, "InvalidParameterException"},
	} {
		status, code, err := h.call(tc.op, tc.body)
		require.NoError(t, err, tc.name)
		require.Equal(t, http.StatusBadRequest, status, tc.name)
		require.Equal(t, tc.code, code, tc.name)
	}
}

func TestSMStaging_PutSecretValueVersionStagesMovesOnlyTheNamedLabels(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	h.ok("CreateSecret", map[string]any{"Name": "pending", "SecretString": "v1", "ClientRequestToken": smTokenA})
	put := h.ok("PutSecretValue", map[string]any{"SecretId": "pending", "SecretString": "v2", "ClientRequestToken": smTokenB, "VersionStages": []string{"AWSPENDING"}})
	require.Contains(t, put, `"VersionStages":["AWSPENDING"]`, "%s", put)

	cur := h.ok("GetSecretValue", map[string]any{"SecretId": "pending"})
	require.Contains(t, cur, `"SecretString":"v1"`, "a version staged without AWSCURRENT does not become current: %s", cur)
	pending := h.ok("GetSecretValue", map[string]any{"SecretId": "pending", "VersionStage": "AWSPENDING"})
	require.Contains(t, pending, `"SecretString":"v2"`, "%s", pending)
	require.Equal(t, map[string]string{smTokenA: `["AWSCURRENT"]`, smTokenB: `["AWSPENDING"]`},
		smStagesByID(t, h.ok("ListSecretVersionIds", map[string]any{"SecretId": "pending"})))
}

// A secret stored before #1376 carries no version list. Its one version is read from
// CurrentVersionID, labeled AWSCURRENT, and the first write moves the label on as usual.
func TestSMStaging_ARecordWrittenBeforeTheVersionListReadsItsCurrentVersion(t *testing.T) {
	t.Parallel()
	h := newSMVersionHarness(t)
	created := time.Unix(1690000000, 0).UTC()
	legacy, err := json.Marshal(emulator.SecretState{
		ARN: "arn:aws:secretsmanager:us-east-1:123456789012:secret:legacy", Name: "legacy",
		CurrentVersionID: smTokenA, AccountID: "123456789012", Region: "us-east-1",
		CreatedDate: created, LastChangedDate: created,
	})
	require.NoError(t, err)
	require.NoError(t, h.fault.inner.Put(t.Context(), "secretsmanager", "secret:123456789012/us-east-1/legacy", legacy))
	require.NoError(t, h.fault.inner.Put(t.Context(), "secretsmanager", "secret_version:123456789012/us-east-1/legacy/"+smTokenA, []byte("old")))
	names, err := json.Marshal([]string{"legacy"})
	require.NoError(t, err)
	require.NoError(t, h.fault.inner.Put(t.Context(), "secretsmanager", "secret_names:123456789012/us-east-1", names))

	listed := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "legacy"})
	require.Equal(t, map[string]string{smTokenA: `["AWSCURRENT"]`}, smStagesByID(t, listed), "%s", listed)
	h.ok("PutSecretValue", map[string]any{"SecretId": "legacy", "SecretString": "new", "ClientRequestToken": smTokenB})
	moved := h.ok("ListSecretVersionIds", map[string]any{"SecretId": "legacy"})
	require.Equal(t, map[string]string{smTokenA: `["AWSPREVIOUS"]`, smTokenB: `["AWSCURRENT"]`}, smStagesByID(t, moved), "%s", moved)
}

// A store fault on a version read or delete the new paths make is an error, never answered as a
// listing, an absence or a success.
func TestSMStaging_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body map[string]any
	}{
		{"UpdateSecret's token lookup", func(m *cfFaultStateManager) { m.failGet = "secret_version:" }, "UpdateSecret",
			map[string]any{"SecretId": "faulty", "SecretString": "v2", "ClientRequestToken": smTokenB}},
		{"a forced delete's version payloads", func(m *cfFaultStateManager) { m.failDelete = "secret_version:" }, "DeleteSecret",
			map[string]any{"SecretId": "faulty", "ForceDeleteWithoutRecovery": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newSMVersionHarness(t)
			h.ok("CreateSecret", map[string]any{"Name": "faulty", "SecretString": "v1", "ClientRequestToken": smTokenA})
			tc.arm(h.fault)
			_, _, err := h.call(tc.op, tc.body)
			require.Error(t, err, "%s must fail on a store fault", tc.name)
		})
	}

	// A legacy record's one version is read from its payload; a fault there is an error too.
	t.Run("a legacy record's version read", func(t *testing.T) {
		t.Parallel()
		h := newSMVersionHarness(t)
		legacy, err := json.Marshal(emulator.SecretState{Name: "old", CurrentVersionID: smTokenA, AccountID: "123456789012", Region: "us-east-1"})
		require.NoError(t, err)
		require.NoError(t, h.fault.inner.Put(t.Context(), "secretsmanager", "secret:123456789012/us-east-1/old", legacy))
		h.fault.failGet = "secret_version:"
		_, _, callErr := h.call("ListSecretVersionIds", map[string]any{"SecretId": "old"})
		require.Error(t, callErr)
	})
}
