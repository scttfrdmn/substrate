package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// tsScopeQuery runs a Timestream Query as the account accessKey belongs to (the default account when
// empty) and returns the status and raw body.
func tsScopeQuery(t *testing.T, ts *emulator.TestServer, accessKey, qs string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"QueryString": qs})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(string(body)))
	require.NoError(t, err)
	req.Host = "timestream.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "Timestream_20181101.Query")
	if accessKey != "" {
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+
			"/20250101/us-east-1/timestream/aws4_request, SignedHeaders=host, Signature=unverified")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, string(raw)
}

// tsScopeControl issues a control-plane request against the Timestream seed endpoint.
func tsScopeControl(t *testing.T, ts *emulator.TestServer, method, query, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+"/v1/timestream-query/results"+query, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, string(raw)
}

// tsScopeResult is a one-row seeded result whose single value names the seed it came from.
func tsScopeResult(marker string) string {
	return `{"Rows":[{"Data":[{"ScalarValue":"` + marker + `"}]}],"ColumnInfo":[{"Name":"v","Type":{"ScalarType":"VARCHAR"}}]}`
}

// TestTimestream_SeedIsScopedByAccountAndRegion asserts a seed installed for one account does not
// answer another account's Query in the same run (#1200), which it did while the seed key carried no
// scope, and that the scope's wildcards and precedence are the ones documented.
func TestTimestream_SeedIsScopedByAccountAndRegion(t *testing.T) {
	t.Parallel()
	const accountA = "111122223333"
	ts := emulator.StartTestServer(t, emulator.WithAccounts(accountA))
	credA, ok := ts.CredentialsFor(accountA)
	require.True(t, ok)
	const qs = "SELECT v FROM db.t"

	status, body := tsScopeControl(t, ts, http.MethodPost, "",
		`{"queryString":"`+qs+`","accountId":"`+accountA+`","region":"us-east-1","result":`+tsScopeResult("A")+`}`)
	require.Equal(t, http.StatusOK, status, body)

	status, body = tsScopeQuery(t, ts, credA.AccessKeyID, qs)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"ScalarValue":"A"`, "account A's Query reads account A's seed")

	status, body = tsScopeQuery(t, ts, "", qs)
	assert.Equal(t, http.StatusBadRequest, status, "account B's Query is unseeded: %s", body)
	assert.NotContains(t, body, `"ScalarValue":"A"`, "account A's seed does not answer account B")

	// A wildcard query in any scope is weaker than the exact query in a narrower one, and an
	// account-wide seed answers that account in any Region.
	status, body = tsScopeControl(t, ts, http.MethodPost, "", `{"queryString":"*","result":`+tsScopeResult("any")+`}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeQuery(t, ts, "", qs)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"ScalarValue":"any"`)
	status, body = tsScopeQuery(t, ts, credA.AccessKeyID, qs)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"ScalarValue":"A"`, "the exact query outranks the wildcard")

	// A clear naming a Region no seed was installed in removes nothing.
	status, body = tsScopeControl(t, ts, http.MethodDelete, "?accountId="+accountA+"&region=eu-west-1", "")
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeQuery(t, ts, credA.AccessKeyID, qs)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"ScalarValue":"A"`, "the seed was installed for us-east-1")

	// A clear naming only the account removes that account's seeds in every Region, and leaves the
	// seed installed for any account.
	status, body = tsScopeControl(t, ts, http.MethodDelete, "?accountId="+accountA, "")
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeQuery(t, ts, credA.AccessKeyID, qs)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"ScalarValue":"any"`, "with A's seed cleared, the wildcard answers")

	// A clear naming only the Region removes every account's seeds there, the wildcard's included.
	status, body = tsScopeControl(t, ts, http.MethodPost, "",
		`{"queryString":"`+qs+`","accountId":"`+accountA+`","region":"us-east-1","result":`+tsScopeResult("A")+`}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeControl(t, ts, http.MethodPost, "",
		`{"queryString":"`+qs+`","region":"us-east-1","result":`+tsScopeResult("east")+`}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeControl(t, ts, http.MethodDelete, "?region=us-east-1", "")
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeQuery(t, ts, credA.AccessKeyID, qs)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"ScalarValue":"any"`, "only the any-Region wildcard survives a us-east-1 clear")

	// A query-string clear with no scope removes that query's seeds in every scope.
	status, body = tsScopeControl(t, ts, http.MethodDelete, "?queryString=*", "")
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsScopeQuery(t, ts, "", qs)
	assert.Equal(t, http.StatusBadRequest, status, "every seed is gone: %s", body)
}

// TestTimestream_SeedRequiresAQueryString asserts an omitted queryString is refused rather than
// silently installed as the "*" wildcard that answers every query (#1200).
func TestTimestream_SeedRequiresAQueryString(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	for _, body := range []string{
		`{"result":` + tsScopeResult("x") + `}`,
		`{"queryString":"","result":` + tsScopeResult("x") + `}`,
	} {
		status, raw := tsScopeControl(t, ts, http.MethodPost, "", body)
		assert.Equal(t, http.StatusBadRequest, status, raw)
		assert.Contains(t, raw, "queryString is required")
	}
	status, body := tsScopeQuery(t, ts, "", "SELECT 1")
	assert.Equal(t, http.StatusBadRequest, status, "no wildcard was installed: %s", body)
}
