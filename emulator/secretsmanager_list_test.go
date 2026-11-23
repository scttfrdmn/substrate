package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListSecrets' SecretListEntry, Filters, ordering and IncludePlannedDeletion (#1622). Every call goes
// over the wire, and every secret is created through CreateSecret, so the entries asserted on are the
// ones a consumer's SDK would decode.

// smListEntries posts ListSecrets with body and returns each entry's raw members.
func smListEntries(t *testing.T, ts *emulator.TestServer, body map[string]any) ([]map[string]json.RawMessage, string) {
	t.Helper()
	status, raw, code := smRawCall(t, ts, smTarget, "ListSecrets", body)
	require.Empty(t, code, "ListSecrets %v: %s", body, raw)
	require.Equal(t, http.StatusOK, status)
	var out struct {
		SecretList []map[string]json.RawMessage `json:"SecretList"`
		NextToken  string                       `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	return out.SecretList, out.NextToken
}

// smListNames returns the names one ListSecrets call with body reports, in order.
func smListNames(t *testing.T, ts *emulator.TestServer, body map[string]any) []string {
	t.Helper()
	entries, _ := smListEntries(t, ts, body)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		var name string
		require.NoError(t, json.Unmarshal(e["Name"], &name))
		names = append(names, name)
	}
	return names
}

func TestSMList_AnEntryCarriesTheSecretsMetadata(t *testing.T) {
	ts := smTagServer(t)
	status, raw, code := smRawCall(t, ts, smTarget, "CreateSecret", map[string]any{
		"Name": "listed", "SecretString": "v", "Description": "Listed secret",
		"KmsKeyId": "alias/custom", "Tags": []map[string]string{{"Key": "owner", "Value": "aegc"}},
	})
	require.Empty(t, code, raw)
	require.Equal(t, http.StatusOK, status)
	var created struct {
		ARN       string `json:"ARN"`
		VersionID string `json:"VersionId"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &created))

	entries, _ := smListEntries(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	entry := entries[0]

	assert.JSONEq(t, `"`+created.ARN+`"`, string(entry["ARN"]))
	assert.JSONEq(t, `"Listed secret"`, string(entry["Description"]))
	assert.JSONEq(t, `"alias/custom"`, string(entry["KmsKeyId"]))
	assert.JSONEq(t, `[{"Key":"owner","Value":"aegc"}]`, string(entry["Tags"]),
		"the tags a consumer reads ownership from (#1622)")
	assert.Contains(t, entry, "CreatedDate")
	assert.Contains(t, entry, "LastChangedDate")
	assert.JSONEq(t, `{"`+created.VersionID+`":["AWSCURRENT"]}`, string(entry["SecretVersionsToStages"]),
		"the list shape's version map, under its own name")
	assert.NotContains(t, entry, "VersionIdsToStages", "DescribeSecret's name for the map is not the list shape's")
	assert.NotContains(t, entry, "RotationEnabled",
		"a never-rotated entry carries no RotationEnabled, as the page's sample entries show")
	assert.NotContains(t, entry, "DeletedDate", "a live secret has no DeletedDate")
	for _, member := range smBookkeepingMembers {
		assert.NotContains(t, entry, member, "no bookkeeping member reaches the wire")
	}
}

func TestSMList_ARotatedSecretReportsRotationEnabled(t *testing.T) {
	ts := smTagServer(t)
	smCreateSecret(t, ts, "rotated", nil)
	status, _, code := smRotate(t, ts, map[string]any{
		"SecretId": "rotated", "ClientRequestToken": smRotationToken, "RotationLambdaARN": smRotationLambda,
	})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	entries, _ := smListEntries(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.JSONEq(t, "true", string(entries[0]["RotationEnabled"]))
}

func TestSMList_ASecretWithNoValueHasNoVersionMap(t *testing.T) {
	ts := smTagServer(t)
	status, _, code := smRawCall(t, ts, smTarget, "CreateSecret", map[string]any{"Name": "empty"})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	entries, _ := smListEntries(t, ts, map[string]any{})
	require.Len(t, entries, 1)
	assert.NotContains(t, entries[0], "SecretVersionsToStages", "no version, so no map")
}

// smFilterFixture creates the secrets the filter table runs against.
func smFilterFixture(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	for _, s := range []struct {
		name, description string
		tags              map[string]string
	}{
		{"prod/db", "Primary database credentials", map[string]string{"Owner": "platform", "env": "prod"}},
		{"prod/api", "API key", map[string]string{"env": "prod"}},
		{"dev/db", "database for dev", map[string]string{"env": "dev"}},
	} {
		smCreateSecretIn(t, ts, smTarget, s.name, "v", s.tags)
		status, _, code := smRawCall(t, ts, smTarget, "UpdateSecret", map[string]any{"SecretId": s.name, "Description": s.description})
		require.Empty(t, code)
		require.Equal(t, http.StatusOK, status)
	}
}

func TestSMList_FiltersFollowTheirPublishedMatchRules(t *testing.T) {
	ts := smTagServer(t)
	smFilterFixture(t, ts)

	f := func(key string, values ...string) map[string]any { return map[string]any{"Key": key, "Values": values} }
	for _, tc := range []struct {
		name    string
		filters []map[string]any
		want    []string
	}{
		{"name is a prefix match", []map[string]any{f("name", "prod/")}, []string{"prod/api", "prod/db"}},
		{"name is case-sensitive", []map[string]any{f("name", "Prod")}, nil},
		{"a leading ! negates", []map[string]any{f("name", "!prod")}, []string{"dev/db"}},
		{"description ignores case", []map[string]any{f("description", "DATABASE")}, []string{"dev/db", "prod/db"}},
		{"description matches word by word", []map[string]any{f("description", "data cred")}, []string{"prod/db"}},
		{"tag-key is a case-sensitive prefix", []map[string]any{f("tag-key", "Own")}, []string{"prod/db"}},
		{"tag-key does not fold case", []map[string]any{f("tag-key", "own")}, nil},
		{"tag-value is a prefix match", []map[string]any{f("tag-value", "pro")}, []string{"prod/api", "prod/db"}},
		{"a negated tag-value excludes any matching tag", []map[string]any{f("tag-value", "!prod")}, []string{"dev/db"}},
		{"one filter's values are alternatives", []map[string]any{f("name", "dev/", "prod/api")}, []string{"dev/db", "prod/api"}},
		{"separate filters are all required", []map[string]any{f("tag-value", "prod"), f("description", "API")}, []string{"prod/api"}},
		{"all searches tags without case", []map[string]any{f("all", "PLATFORM")}, []string{"prod/db"}},
		{"all splits the value into words", []map[string]any{f("all", "my_KEY")}, []string{"prod/api"}},
		{"no secret has a primary Region", []map[string]any{f("primary-region", "us")}, nil},
		{"so its negation admits every secret", []map[string]any{f("owning-service", "!x")}, []string{"dev/db", "prod/api", "prod/db"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := smListNames(t, ts, map[string]any{"Filters": tc.filters, "SortBy": "name"})
			if tc.want == nil {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSMList_UnpublishedParametersAreRefused(t *testing.T) {
	ts := smTagServer(t)
	many := make([]map[string]any, 11)
	for i := range many {
		many[i] = map[string]any{"Key": "name", "Values": []string{"x"}}
	}
	values := make([]string, 11)
	for i := range values {
		values[i] = "x"
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"an unknown key", map[string]any{"Filters": []map[string]any{{"Key": "color-depth", "Values": []string{"x"}}}}},
		{"more than ten filters", map[string]any{"Filters": many}},
		{"a filter with no values", map[string]any{"Filters": []map[string]any{{"Key": "name", "Values": []string{}}}}},
		{"more than ten values", map[string]any{"Filters": []map[string]any{{"Key": "name", "Values": values}}}},
		{"a value outside the pattern", map[string]any{"Filters": []map[string]any{{"Key": "name", "Values": []string{"a*"}}}}},
		{"a value over 512 characters", map[string]any{"Filters": []map[string]any{{"Key": "name", "Values": []string{strings.Repeat("a", 513)}}}}},
		{"an unknown SortOrder", map[string]any{"SortOrder": "up"}},
		{"an unknown SortBy", map[string]any{"SortBy": "size"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, code := smRawCall(t, ts, smTarget, "ListSecrets", tc.body)
			assert.Equal(t, "InvalidParameterException", code)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
}

func TestSMList_SecretsAreListedByCreatedDate(t *testing.T) {
	ts := smTagServer(t)
	ts.SetTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		smCreateSecret(t, ts, name, nil)
		ts.AdvanceTime(time.Minute)
	}

	for _, tc := range []struct {
		name string
		body map[string]any
		want []string
	}{
		{"the default is CreatedDate ascending", map[string]any{}, []string{"charlie", "alpha", "bravo"}},
		{"desc reverses it", map[string]any{"SortOrder": "desc"}, []string{"bravo", "alpha", "charlie"}},
		{"SortBy name", map[string]any{"SortBy": "name"}, []string{"alpha", "bravo", "charlie"}},
		{"SortBy name desc", map[string]any{"SortBy": "name", "SortOrder": "desc"}, []string{"charlie", "bravo", "alpha"}},
		{"an unmodeled LastAccessedDate ties, and the name decides", map[string]any{"SortBy": "last-accessed-date"}, []string{"alpha", "bravo", "charlie"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, smListNames(t, ts, tc.body))
		})
	}

	// LastChangedDate moves when a secret changes, so the most recently updated sorts last.
	status, _, code := smRawCall(t, ts, smTarget, "PutSecretValue", map[string]any{"SecretId": "charlie", "SecretString": "v2"})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, smListNames(t, ts, map[string]any{"SortBy": "last-changed-date"}))
}

func TestSMList_ASecretPendingDeletionIsListedOnlyWhenAskedFor(t *testing.T) {
	ts := smTagServer(t)
	smCreateSecret(t, ts, "live", nil)
	arn := smCreateSecret(t, ts, "pending", nil)
	_, _, code := smDeleteSecret(t, ts, map[string]any{"SecretId": arn})
	require.Empty(t, code)

	assert.Equal(t, []string{"live"}, smListNames(t, ts, map[string]any{}),
		"By default, secrets scheduled for deletion aren't included")

	entries, _ := smListEntries(t, ts, map[string]any{"IncludePlannedDeletion": true, "SortBy": "name"})
	require.Len(t, entries, 2)
	assert.JSONEq(t, `"pending"`, string(entries[1]["Name"]))
	assert.Contains(t, entries[1], "DeletedDate", "an included pending secret carries its DeletedDate")
	assert.NotContains(t, entries[0], "DeletedDate")
}

func TestSMList_AFilteredWalkPagesOverTheFilteredList(t *testing.T) {
	ts := smTagServer(t)
	smFilterFixture(t, ts)

	body := map[string]any{
		"Filters":    []map[string]any{{"Key": "tag-value", "Values": []string{"prod"}}},
		"SortBy":     "name",
		"MaxResults": 1,
	}
	first, token := smListEntries(t, ts, body)
	require.Len(t, first, 1, "a page is cut after the filter, so it is full")
	require.NotEmpty(t, token)
	body["NextToken"] = token
	second, token := smListEntries(t, ts, body)
	require.Len(t, second, 1)
	assert.Empty(t, token, "two prod secrets, two pages")
	assert.JSONEq(t, `"prod/api"`, string(first[0]["Name"]))
	assert.JSONEq(t, `"prod/db"`, string(second[0]["Name"]))
}

// A NextToken is a cursor on the last entry, not an offset, so a loop that deletes each page's
// secrets as it walks still reaches every secret (#1641).
func TestSMList_ADeleteMidWalkSkipsNothing(t *testing.T) {
	ts := smTagServer(t)
	ts.SetTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	for _, name := range []string{"s1", "s2", "s3", "s4", "s5"} {
		smCreateSecret(t, ts, name, nil)
		ts.AdvanceTime(time.Minute)
	}

	var seen []string
	body := map[string]any{"MaxResults": 2}
	for range 5 {
		entries, token := smListEntries(t, ts, body)
		for _, e := range entries {
			var name string
			require.NoError(t, json.Unmarshal(e["Name"], &name))
			seen = append(seen, name)
			_, _, code := smDeleteSecret(t, ts, map[string]any{"SecretId": name})
			require.Empty(t, code)
		}
		if token == "" {
			break
		}
		body["NextToken"] = token
	}
	assert.Equal(t, []string{"s1", "s2", "s3", "s4", "s5"}, seen,
		"every secret is reached although each page's secrets were deleted before the next was fetched")
}

func TestSMList_ACreateMidWalkRepeatsNothing(t *testing.T) {
	ts := smTagServer(t)
	for _, name := range []string{"b", "d", "f"} {
		smCreateSecret(t, ts, name, nil)
	}
	body := map[string]any{"MaxResults": 2, "SortBy": "name", "SortOrder": "desc"}
	first, token := smListEntries(t, ts, body)
	require.Len(t, first, 2)
	require.NotEmpty(t, token)
	smCreateSecret(t, ts, "e", nil) // before the cursor in a descending walk
	smCreateSecret(t, ts, "a", nil) // after it
	body["NextToken"] = token
	assert.Equal(t, []string{"b", "a"}, smListNames(t, ts, body),
		"the walk resumes strictly after d: nothing it already served comes back, and a later name is reached")
}
