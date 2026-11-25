package emulator_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// lambdaTwoVersions creates a function with versions 1 and 2, which differ in their timeout.
func lambdaTwoVersions(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	created := lambdaCreateZipFunction(t, srv, name, "print(1)", map[string]any{"Publish": true})
	status, _, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/"+name+"/configuration",
		map[string]any{"Timeout": 20})
	require.Equal(t, http.StatusOK, status, raw)
	require.Equal(t, "2", lambdaPublish(t, srv, name))
	return strings.TrimSuffix(created["FunctionArn"].(string), ":1")
}

// TestLambdaAliasCRUD pins the five alias operations and their refusals (#1613).
func TestLambdaAliasCRUD(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	arn := lambdaTwoVersions(t, srv, "alias-fn")
	base := "/2015-03-31/functions/alias-fn/aliases"

	status, live, raw := lambdaDo(t, srv, http.MethodPost, base,
		map[string]any{"Name": "live", "FunctionVersion": "1", "Description": "production"})
	require.Equal(t, http.StatusCreated, status, raw)
	assert.Equal(t, arn+":live", live["AliasArn"])
	assert.Equal(t, "1", live["FunctionVersion"])
	assert.Equal(t, "production", live["Description"])
	assert.NotEmpty(t, live["RevisionId"])
	assert.NotContains(t, live, "RoutingConfig", "an alias routing to one version reports no RoutingConfig")

	status, _, raw = lambdaDo(t, srv, http.MethodPost, base,
		map[string]any{"Name": "edge", "FunctionVersion": "$LATEST"})
	require.Equal(t, http.StatusCreated, status, raw)

	refusals := []struct {
		name     string
		body     map[string]any
		status   int
		wantCode string
	}{
		{name: "an alias that exists", body: map[string]any{"Name": "live", "FunctionVersion": "2"},
			status: http.StatusConflict, wantCode: "ResourceConflictException"},
		{name: "an all-digit name", body: map[string]any{"Name": "12", "FunctionVersion": "1"},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a name outside the pattern", body: map[string]any{"Name": "a.b", "FunctionVersion": "1"},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "no FunctionVersion", body: map[string]any{"Name": "x"},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a FunctionVersion outside the pattern", body: map[string]any{"Name": "x", "FunctionVersion": "v1"},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a version never published", body: map[string]any{"Name": "x", "FunctionVersion": "5"},
			status: http.StatusNotFound, wantCode: "ResourceNotFoundException"},
		{name: "a weight above 1", body: map[string]any{"Name": "x", "FunctionVersion": "1",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 1.5}}},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "the alias's own version as the additional one", body: map[string]any{"Name": "x", "FunctionVersion": "1",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"1": 0.1}}},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "two additional versions", body: map[string]any{"Name": "x", "FunctionVersion": "1",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.1, "3": 0.1}}},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a weight key that is not a version", body: map[string]any{"Name": "x", "FunctionVersion": "1",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"live": 0.1}}},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a weighted alias on $LATEST", body: map[string]any{"Name": "x", "FunctionVersion": "$LATEST",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.1}}},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "an additional version never published", body: map[string]any{"Name": "x", "FunctionVersion": "1",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"8": 0.1}}},
			status: http.StatusNotFound, wantCode: "ResourceNotFoundException"},
		{name: "a description over 256 characters", body: map[string]any{"Name": "x", "FunctionVersion": "1",
			"Description": strings.Repeat("d", 257)},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, http.MethodPost, base, tc.body)
			assert.Equal(t, tc.status, status, raw)
			assert.Contains(t, raw, tc.wantCode)
		})
	}
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/missing/aliases",
		map[string]any{"Name": "live", "FunctionVersion": "1"})
	assert.Equal(t, http.StatusNotFound, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodPost, base, "not an object")
	assert.Equal(t, http.StatusBadRequest, status, raw)

	status, got, raw := lambdaDo(t, srv, http.MethodGet, base+"/live", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, live, got)
	status, _, raw = lambdaDo(t, srv, http.MethodGet, base+"/nope", nil)
	assert.Equal(t, http.StatusNotFound, status, raw)
	assert.Contains(t, raw, "ResourceNotFoundException")

	t.Run("update", func(t *testing.T) {
		status, _, raw := lambdaDo(t, srv, http.MethodPut, base+"/live", map[string]any{"RevisionId": "stale"})
		assert.Equal(t, http.StatusPreconditionFailed, status, raw)
		assert.Contains(t, raw, "PreconditionFailedException")

		status, updated, raw := lambdaDo(t, srv, http.MethodPut, base+"/live", map[string]any{
			"RevisionId":    live["RevisionId"],
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.25}},
		})
		require.Equal(t, http.StatusOK, status, raw)
		assert.Equal(t, "1", updated["FunctionVersion"], "an absent FunctionVersion is kept")
		assert.Equal(t, "production", updated["Description"], "an absent Description is kept")
		assert.Equal(t, map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.25}}, updated["RoutingConfig"])
		assert.NotEqual(t, live["RevisionId"], updated["RevisionId"], "an update changes the RevisionId")

		status, updated, raw = lambdaDo(t, srv, http.MethodPut, base+"/live", map[string]any{
			"FunctionVersion": "2", "Description": "",
			"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{}},
		})
		require.Equal(t, http.StatusOK, status, raw)
		assert.Equal(t, "2", updated["FunctionVersion"])
		assert.Equal(t, "", updated["Description"])
		assert.NotContains(t, updated, "RoutingConfig", "an empty AdditionalVersionWeights clears the routing")

		status, _, raw = lambdaDo(t, srv, http.MethodPut, base+"/live", map[string]any{"FunctionVersion": "6"})
		assert.Equal(t, http.StatusNotFound, status, raw)
		status, _, raw = lambdaDo(t, srv, http.MethodPut, base+"/nope", map[string]any{})
		assert.Equal(t, http.StatusNotFound, status, raw)
		status, _, raw = lambdaDo(t, srv, http.MethodPut, base+"/live",
			map[string]any{"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"1": -0.1}}})
		assert.Equal(t, http.StatusBadRequest, status, raw)
	})

	t.Run("list", func(t *testing.T) {
		names := func(path string) []string {
			status, out, raw := lambdaDo(t, srv, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, status, raw)
			var got []string
			for _, a := range out["Aliases"].([]any) {
				got = append(got, a.(map[string]any)["Name"].(string))
			}
			return got
		}
		assert.Equal(t, []string{"edge", "live"}, names(base), "in name order")
		assert.Equal(t, []string{"live"}, names(base+"?FunctionVersion=2"))
		assert.Equal(t, []string{"edge"}, names(base+"?FunctionVersion=$LATEST"))
		assert.Empty(t, names(base+"?FunctionVersion=1"))

		status, out, raw := lambdaDo(t, srv, http.MethodGet, base+"?MaxItems=1", nil)
		require.Equal(t, http.StatusOK, status, raw)
		require.Len(t, out["Aliases"], 1)
		next := out["NextMarker"].(string)
		status, out, raw = lambdaDo(t, srv, http.MethodGet, base+"?MaxItems=1&Marker="+url.QueryEscape(next), nil)
		require.Equal(t, http.StatusOK, status, raw)
		require.Len(t, out["Aliases"], 1)
		assert.Equal(t, "live", out["Aliases"].([]any)[0].(map[string]any)["Name"])
		assert.NotContains(t, out, "NextMarker", "the last page carries no NextMarker")

		for _, bad := range []string{"?MaxItems=0", "?Marker=abc", "?FunctionVersion=v1",
			"?FunctionVersion=2&Marker=" + url.QueryEscape(next)} {
			status, _, raw := lambdaDo(t, srv, http.MethodGet, base+bad, nil)
			assert.Equal(t, http.StatusBadRequest, status, "%s: %s", bad, raw)
		}
		status, _, _ = lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/missing/aliases", nil)
		assert.Equal(t, http.StatusNotFound, status)
	})

	status, _, raw = lambdaDo(t, srv, http.MethodDelete, base+"/edge", nil)
	assert.Equal(t, http.StatusNoContent, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, base+"/edge", nil)
	assert.Equal(t, http.StatusNoContent, status, "DeleteAlias publishes no not-found error: %s", raw)
	status, _, _ = lambdaDo(t, srv, http.MethodGet, base+"/edge", nil)
	assert.Equal(t, http.StatusNotFound, status)
	status, _, _ = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/missing/aliases/edge", nil)
	assert.Equal(t, http.StatusNotFound, status)
}

// TestLambdaWeightedAliasRequiresOneRole pins the weighted-alias rule that both versions have the
// same execution role.
func TestLambdaWeightedAliasRequiresOneRole(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "roles-fn", "print(1)", map[string]any{"Publish": true})
	status, _, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/roles-fn/configuration",
		map[string]any{"Role": "arn:aws:iam::123456789012:role/other-role"})
	require.Equal(t, http.StatusOK, status, raw)
	require.Equal(t, "2", lambdaPublish(t, srv, "roles-fn"))
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/roles-fn/aliases", map[string]any{
		"Name": "split", "FunctionVersion": "1",
		"RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.5}},
	})
	assert.Equal(t, http.StatusBadRequest, status, raw)
	assert.Contains(t, raw, "same execution role")
}

// lambdaInvokeVersion invokes path and returns the X-Amz-Executed-Version it answers.
func lambdaInvokeVersion(t *testing.T, srv *emulator.Server, path string) string {
	t.Helper()
	resp := lambdaRequestWithHeaders(t, srv, http.MethodPost, path, map[string]any{}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp.Header.Get("X-Amz-Executed-Version")
}

// TestLambdaInvokeQualified pins that Invoke runs the version a qualifier names, an alias's version,
// and for a weighted alias a version drawn from the request: weight 1 always routes to the
// additional version, weight 0 never does, and an even split reaches both.
func TestLambdaInvokeQualified(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaTwoVersions(t, srv, "inv-fn")
	fnPath := "/2015-03-31/functions/inv-fn"
	for _, alias := range []map[string]any{
		{"Name": "plain", "FunctionVersion": "1"},
		{"Name": "all", "FunctionVersion": "1", "RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 1.0}}},
		{"Name": "none", "FunctionVersion": "1", "RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.0}}},
		{"Name": "half", "FunctionVersion": "1", "RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]any{"2": 0.5}}},
	} {
		status, _, raw := lambdaDo(t, srv, http.MethodPost, fnPath+"/aliases", alias)
		require.Equal(t, http.StatusCreated, status, raw)
	}

	assert.Equal(t, "$LATEST", lambdaInvokeVersion(t, srv, fnPath+"/invocations"))
	assert.Equal(t, "2", lambdaInvokeVersion(t, srv, fnPath+"/invocations?Qualifier=2"))
	assert.Equal(t, "1", lambdaInvokeVersion(t, srv, "/2015-03-31/functions/inv-fn:1/invocations"))
	assert.Equal(t, "1", lambdaInvokeVersion(t, srv, fnPath+"/invocations?Qualifier=plain"))
	seen := map[string]int{}
	for range 64 {
		assert.Equal(t, "2", lambdaInvokeVersion(t, srv, fnPath+"/invocations?Qualifier=all"))
		assert.Equal(t, "1", lambdaInvokeVersion(t, srv, fnPath+"/invocations?Qualifier=none"))
		seen[lambdaInvokeVersion(t, srv, fnPath+"/invocations?Qualifier=half")]++
	}
	assert.Positive(t, seen["1"], "an even split reaches the primary version")
	assert.Positive(t, seen["2"], "and the additional one")
	assert.Equal(t, 64, seen["1"]+seen["2"])

	resp := lambdaRequestWithHeaders(t, srv, http.MethodPost, fnPath+"/invocations?Qualifier=1",
		map[string]any{}, map[string]string{"X-Amz-Log-Type": "Tail"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	tail, err := base64.StdEncoding.DecodeString(resp.Header.Get("X-Amz-Log-Result"))
	require.NoError(t, err)
	assert.Contains(t, string(tail), "Version: 1\n", "the log names the version that ran")

	resp = lambdaRequestWithHeaders(t, srv, http.MethodPost, fnPath+"/invocations?Qualifier=missing", map[string]any{}, nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestLambdaRouteAliasIsReproducible pins the determinism of weighted routing: the version chosen
// is a function of the request id, so the same request routes the same way every time.
func TestLambdaRouteAliasIsReproducible(t *testing.T) {
	t.Parallel()
	for _, seed := range []string{"req-a", "req-b", "req-c", "req-d"} {
		first := emulator.LambdaRouteAliasForTest(seed, "1", "2", 0.5)
		for range 5 {
			assert.Equal(t, first, emulator.LambdaRouteAliasForTest(seed, "1", "2", 0.5), "seed %s", seed)
		}
		assert.Equal(t, "2", emulator.LambdaRouteAliasForTest(seed, "1", "2", 1))
		assert.Equal(t, "1", emulator.LambdaRouteAliasForTest(seed, "1", "2", 0))
	}
}

// TestLambdaQualifiedPermissions pins that a version's or alias's resource policy is its own: a
// statement added with a Qualifier is read back with it and absent from the function's policy, and
// a Qualifier naming nothing is refused.
func TestLambdaQualifiedPermissions(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	arn := lambdaTwoVersions(t, srv, "perm-fn")
	status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/perm-fn/aliases",
		map[string]any{"Name": "live", "FunctionVersion": "2"})
	require.Equal(t, http.StatusCreated, status, raw)

	stmt := map[string]any{"StatementId": "s3", "Action": "lambda:InvokeFunction", "Principal": "s3.amazonaws.com"}
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/perm-fn/policy?Qualifier=live", stmt)
	require.Equal(t, http.StatusCreated, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/perm-fn:1/policy", stmt)
	require.Equal(t, http.StatusCreated, status, "the same statement id in another qualifier's policy: %s", raw)
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/perm-fn/policy?Qualifier=nope", stmt)
	assert.Equal(t, http.StatusNotFound, status, raw)

	status, out, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/perm-fn/policy?Qualifier=live", nil)
	require.Equal(t, http.StatusOK, status, raw)
	var doc struct {
		Statement []struct{ Sid, Resource string }
	}
	require.NoError(t, json.Unmarshal([]byte(out["Policy"].(string)), &doc))
	require.Len(t, doc.Statement, 1)
	assert.Equal(t, arn+":live", doc.Statement[0].Resource)

	status, _, raw = lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/perm-fn/policy", nil)
	assert.Equal(t, http.StatusNotFound, status, "the function's own policy is separate: %s", raw)

	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/perm-fn/policy/s3?Qualifier=live", nil)
	assert.Equal(t, http.StatusNoContent, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/perm-fn/policy/s3?Qualifier=live", nil)
	assert.Equal(t, http.StatusNotFound, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/perm-fn/policy/s3?Qualifier=1", nil)
	assert.Equal(t, http.StatusNoContent, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/perm-fn:1/policy/s3?Qualifier=2", nil)
	assert.Equal(t, http.StatusBadRequest, status, raw)
}

// TestLambdaVersionsAndAliasesSurfaceStoreFaults pins that a store fault under a version, an alias
// or a qualified policy fails the request with a server error rather than being read as absent.
func TestLambdaVersionsAndAliasesSurfaceStoreFaults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                         string
		failGet, corruptGet, failPut string
		failDelete                   string
		method, path                 string
		body                         any
	}{
		{name: "a corrupt version on a qualified read", corruptGet: "function_version:",
			method: http.MethodGet, path: "/configuration?Qualifier=1"},
		{name: "a corrupt version on a listing", corruptGet: "function_version:",
			method: http.MethodGet, path: "/versions"},
		{name: "a corrupt version on a publish", corruptGet: "function_version:",
			method: http.MethodPost, path: "/versions", body: map[string]any{}},
		{name: "an unreadable counter", failGet: "function_version_seq:",
			method: http.MethodPut, path: "/code", body: map[string]any{"ZipFile": "cHJpbnQoMyk=", "Publish": true}},
		{name: "a corrupt counter", corruptGet: "function_version_seq:",
			method: http.MethodPut, path: "/code", body: map[string]any{"ZipFile": "cHJpbnQoMyk=", "Publish": true}},
		{name: "an unwritable counter", failPut: "function_version_seq:",
			method: http.MethodPut, path: "/code", body: map[string]any{"ZipFile": "cHJpbnQoMyk=", "Publish": true}},
		{name: "an unwritable version", failPut: "function_version:",
			method: http.MethodPut, path: "/code", body: map[string]any{"ZipFile": "cHJpbnQoMyk=", "Publish": true}},
		{name: "an unreadable version on a delete", failGet: "function_version:",
			method: http.MethodDelete, path: "?Qualifier=2"},
		{name: "an undeletable version", failDelete: "function_version:",
			method: http.MethodDelete, path: "?Qualifier=2"},
		{name: "an undeletable function's versions", failDelete: "function_version:",
			method: http.MethodDelete, path: ""},
		{name: "a corrupt alias on a read", corruptGet: "function_alias:",
			method: http.MethodGet, path: "/aliases/live"},
		{name: "a corrupt alias on a listing", corruptGet: "function_alias:",
			method: http.MethodGet, path: "/aliases"},
		{name: "a corrupt alias on an invoke", corruptGet: "function_alias:",
			method: http.MethodPost, path: "/invocations?Qualifier=live", body: map[string]any{}},
		{name: "a corrupt alias on a version delete", corruptGet: "function_alias:",
			method: http.MethodDelete, path: "?Qualifier=2"},
		{name: "a corrupt alias on a create", corruptGet: "function_alias:",
			method: http.MethodPost, path: "/aliases", body: map[string]any{"Name": "live", "FunctionVersion": "1"}},
		{name: "an unwritable alias", failPut: "function_alias:",
			method: http.MethodPost, path: "/aliases", body: map[string]any{"Name": "new", "FunctionVersion": "1"}},
		{name: "an unwritable alias update", failPut: "function_alias:",
			method: http.MethodPut, path: "/aliases/live", body: map[string]any{"FunctionVersion": "2"}},
		{name: "an unreadable alias on an update", failGet: "function_alias:",
			method: http.MethodPut, path: "/aliases/live", body: map[string]any{"FunctionVersion": "2"}},
		{name: "an undeletable alias", failDelete: "function_alias:",
			method: http.MethodDelete, path: "/aliases/live"},
		{name: "an undeletable alias policy", failDelete: "function_policy:",
			method: http.MethodDelete, path: "/aliases/live"},
		{name: "an undeletable function's aliases", failDelete: "function_alias:",
			method: http.MethodDelete, path: ""},
		{name: "an unreadable qualified policy", failGet: "function_policy:",
			method: http.MethodGet, path: "/policy?Qualifier=live"},
		{name: "an unreadable policy on a remove", failGet: "function_policy:",
			method: http.MethodDelete, path: "/policy/s?Qualifier=live"},
		{name: "an unwritable policy on a remove", failPut: "function_policy:",
			method: http.MethodDelete, path: "/policy/s?Qualifier=live"},
		{name: "an unreadable version for a weighted alias", failGet: "function_version:",
			method: http.MethodPost, path: "/aliases", body: map[string]any{"Name": "w", "FunctionVersion": "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newLambdaTestServerWithState(t, store)
			lambdaTwoVersions(t, srv, "fault-fn")
			status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/fault-fn/aliases",
				map[string]any{"Name": "live", "FunctionVersion": "1"})
			require.Equal(t, http.StatusCreated, status, raw)
			status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/fault-fn/policy?Qualifier=live",
				map[string]any{"StatementId": "s", "Action": "lambda:InvokeFunction", "Principal": "*"})
			require.Equal(t, http.StatusCreated, status, raw)

			store.failGet, store.corruptGet, store.failPut, store.failDelete = tc.failGet, tc.corruptGet, tc.failPut, tc.failDelete
			status, _, raw = lambdaDo(t, srv, tc.method, "/2015-03-31/functions/fault-fn"+tc.path, tc.body)
			assert.Equal(t, http.StatusInternalServerError, status, raw)
		})
	}
}

// TestLambdaRemovePermissionReadsAQualifiedStatementInTheFunctionPolicy pins the read of a record
// written before versions and aliases had policies of their own: a qualified statement stored in the
// function's policy is still removed by a qualified RemovePermission.
func TestLambdaRemovePermissionReadsAQualifiedStatementInTheFunctionPolicy(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newLambdaTestServerWithState(t, state)
	arn := lambdaCreateZipFunction(t, srv, "old-fn", "print(1)", nil)["FunctionArn"].(string)
	legacy, err := json.Marshal(emulator.LambdaResourcePolicy{Version: "2012-10-17", Statement: []emulator.LambdaPermissionStatement{
		{Sid: "keep", Effect: "Allow", Action: "lambda:InvokeFunction", Resource: arn, Principal: "*"},
		{Sid: "aliased", Effect: "Allow", Action: "lambda:InvokeFunction", Resource: arn + ":live", Principal: "*"},
	}})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "lambda", "function_policy:123456789012/us-east-1/old-fn", legacy))

	status, _, raw := lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/old-fn/policy/aliased?Qualifier=live", nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	status, out, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/old-fn/policy", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Contains(t, out["Policy"], `"keep"`)
	assert.NotContains(t, out["Policy"], `"aliased"`)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/old-fn/policy/keep?Qualifier=live", nil)
	assert.Equal(t, http.StatusNotFound, status, "an unqualified statement is not removed by a qualified call: %s", raw)
	assert.Contains(t, raw, "Statement not found",
		"the function's policy exists, so the refusal names the statement rather than a missing policy")
}
