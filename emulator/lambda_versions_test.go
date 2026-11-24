package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestLambdaListVersionsByFunction covers the read the Terraform AWS provider makes on every
// refresh of an aws_lambda_function (#1519): a function that has published nothing lists $LATEST
// alone.
func TestLambdaListVersionsByFunction(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	created := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "versions-fn",
		"Runtime":      "nodejs22.x",
		"Role":         "arn:aws:iam::123456789012:role/lambda-role",
		"Handler":      "index.handler",
	})
	require.Equal(t, http.StatusCreated, created.StatusCode)
	var fn map[string]any
	decodeLambdaJSON(t, created, &fn)
	assert.Equal(t, "$LATEST", fn["Version"], "CreateFunction answers the unpublished version")

	tests := []struct {
		name     string
		path     string
		status   int
		wantCode string
	}{
		{name: "lists $LATEST", path: "/2015-03-31/functions/versions-fn/versions", status: http.StatusOK},
		{name: "MaxItems in range", path: "/2015-03-31/functions/versions-fn/versions?MaxItems=1", status: http.StatusOK},
		{name: "MaxItems zero", path: "/2015-03-31/functions/versions-fn/versions?MaxItems=0",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "MaxItems over the range", path: "/2015-03-31/functions/versions-fn/versions?MaxItems=10001",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "MaxItems not a number", path: "/2015-03-31/functions/versions-fn/versions?MaxItems=x",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a Marker substrate never issued", path: "/2015-03-31/functions/versions-fn/versions?Marker=abc",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "an absent function", path: "/2015-03-31/functions/missing/versions",
			status: http.StatusNotFound, wantCode: "ResourceNotFoundException"},
		{name: "an absent function with a bad Marker is refused for the Marker",
			path:   "/2015-03-31/functions/missing/versions?Marker=abc",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := lambdaRequest(t, srv, http.MethodGet, tc.path, nil)
			if tc.wantCode != "" {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				assert.Equal(t, tc.status, resp.StatusCode, "%s", body)
				assert.Contains(t, string(body), tc.wantCode)
				return
			}
			require.Equal(t, tc.status, resp.StatusCode)
			var out struct {
				Versions   []map[string]any `json:"Versions"`
				NextMarker *string          `json:"NextMarker"`
			}
			decodeLambdaJSON(t, resp, &out)
			require.Len(t, out.Versions, 1)
			assert.Equal(t, "$LATEST", out.Versions[0]["Version"])
			assert.Equal(t, fn["FunctionArn"].(string)+":$LATEST", out.Versions[0]["FunctionArn"],
				"the version's ARN is qualified, as the reference's example shows")
			assert.Equal(t, "nodejs22.x", out.Versions[0]["Runtime"])
			assert.Nil(t, out.NextMarker, "one page, so no NextMarker")
		})
	}
}

// TestLambdaTracingTagsAndCodeSigning covers the other function reads Terraform's refresh makes
// (#1519): TracingConfig stored and updated, Tags at the top of GetFunction, and the code-signing
// configuration of a function that has none.
func TestLambdaTracingTagsAndCodeSigning(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	create := func(name string, extra map[string]any) *http.Response {
		body := map[string]any{
			"FunctionName": name, "Runtime": "nodejs22.x", "Handler": "index.handler",
			"Role": "arn:aws:iam::123456789012:role/lambda-role",
		}
		for k, v := range extra {
			body[k] = v
		}
		return lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", body)
	}
	tracingMode := func(t *testing.T, name string) string {
		t.Helper()
		resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/"+name, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var out struct {
			Configuration struct {
				TracingConfig struct {
					Mode string `json:"Mode"`
				} `json:"TracingConfig"`
			} `json:"Configuration"`
			Tags map[string]string `json:"Tags"`
		}
		decodeLambdaJSON(t, resp, &out)
		return out.Configuration.TracingConfig.Mode
	}

	require.Equal(t, http.StatusCreated, create("traced", map[string]any{
		"TracingConfig": map[string]any{"Mode": "Active"},
		"Tags":          map[string]string{"team": "iac"},
	}).StatusCode)
	require.Equal(t, http.StatusCreated, create("untraced", nil).StatusCode)

	assert.Equal(t, "Active", tracingMode(t, "traced"))
	assert.Equal(t, "PassThrough", tracingMode(t, "untraced"), "the mode a function created without one has")

	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/traced", nil)
	var got struct {
		Tags map[string]string `json:"Tags"`
	}
	decodeLambdaJSON(t, resp, &got)
	assert.Equal(t, map[string]string{"team": "iac"}, got.Tags, "GetFunction reports Tags beside Configuration")

	resp = lambdaRequest(t, srv, http.MethodPut, "/2015-03-31/functions/traced/configuration",
		map[string]any{"TracingConfig": map[string]any{"Mode": "PassThrough"}})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "PassThrough", tracingMode(t, "traced"))

	refusals := []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{"CreateFunction with an unknown mode", http.MethodPost, "/2015-03-31/functions", map[string]any{
			"FunctionName": "bad", "Runtime": "nodejs22.x", "Handler": "index.handler",
			"Role": "arn:aws:iam::123456789012:role/lambda-role", "TracingConfig": map[string]any{"Mode": "Sometimes"},
		}},
		{"UpdateFunctionConfiguration with an unknown mode", http.MethodPut, "/2015-03-31/functions/traced/configuration",
			map[string]any{"TracingConfig": map[string]any{"Mode": "Sometimes"}}},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			resp := lambdaRequest(t, srv, tc.method, tc.path, tc.body)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", body)
			assert.Contains(t, string(body), "InvalidParameterValueException")
		})
	}

	t.Run("code signing", func(t *testing.T) {
		resp := lambdaRequest(t, srv, http.MethodGet, "/2020-06-30/functions/traced/code-signing-config", nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var out map[string]any
		decodeLambdaJSON(t, resp, &out)
		assert.Equal(t, map[string]any{"CodeSigningConfigArn": "", "FunctionName": "traced"}, out)

		resp = lambdaRequest(t, srv, http.MethodGet, "/2020-06-30/functions/missing/code-signing-config", nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		resp = lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/traced/code-signing-config", nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "only the published date routes")
	})
}

// lambdaDo sends one Lambda request and returns its status, its decoded JSON body (nil when the
// body is empty) and the raw body for failure messages.
func lambdaDo(t *testing.T, srv *emulator.Server, method, path string, body any) (int, map[string]any, string) {
	t.Helper()
	resp := lambdaRequest(t, srv, method, path, body)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	}
	return resp.StatusCode, out, string(raw)
}

// lambdaCreateZipFunction creates a function from an inline package holding code.
func lambdaCreateZipFunction(t *testing.T, srv *emulator.Server, name, code string, extra map[string]any) map[string]any {
	t.Helper()
	body := map[string]any{
		"FunctionName": name, "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/lambda-role",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
	}
	for k, v := range extra {
		body[k] = v
	}
	status, out, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, status, raw)
	return out
}

// lambdaPublish publishes a version and returns its number.
func lambdaPublish(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	status, out, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/"+name+"/versions", map[string]any{})
	require.Equal(t, http.StatusCreated, status, raw)
	return out["Version"].(string)
}

// lambdaListVersionNames walks ListVersionsByFunction to the end, pageSize at a time, and returns
// every version name in listing order.
func lambdaListVersionNames(t *testing.T, srv *emulator.Server, name string, pageSize int) []string {
	t.Helper()
	var names []string
	marker := ""
	for range 100 {
		path := fmt.Sprintf("/2015-03-31/functions/%s/versions?MaxItems=%d", name, pageSize)
		if marker != "" {
			path += "&Marker=" + url.QueryEscape(marker)
		}
		status, out, raw := lambdaDo(t, srv, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, status, raw)
		versions, _ := out["Versions"].([]any)
		assert.LessOrEqual(t, len(versions), pageSize)
		for _, v := range versions {
			names = append(names, v.(map[string]any)["Version"].(string))
		}
		next, _ := out["NextMarker"].(string)
		if next == "" {
			return names
		}
		marker = next
	}
	t.Fatal("ListVersionsByFunction never stopped paging")
	return nil
}

// TestLambdaPublishVersion pins PublishVersion's snapshot (#1613): a version keeps the
// configuration it was published with, an unchanged function answers the last version again, and
// RevisionId and CodeSha256 guard a publish.
func TestLambdaPublishVersion(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	created := lambdaCreateZipFunction(t, srv, "pub-fn", "print(1)", nil)
	arn := created["FunctionArn"].(string)

	status, v1, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/pub-fn/versions",
		map[string]any{"Description": "first"})
	require.Equal(t, http.StatusCreated, status, raw)
	assert.Equal(t, "1", v1["Version"])
	assert.Equal(t, arn+":1", v1["FunctionArn"])
	assert.Equal(t, "first", v1["Description"], "Description overrides the function's on the version")
	assert.Equal(t, created["CodeSha256"], v1["CodeSha256"])

	assert.Equal(t, "1", lambdaPublish(t, srv, "pub-fn"), "nothing changed, so the last version is answered")

	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/pub-fn/configuration",
		map[string]any{"Timeout": 30})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "2", lambdaPublish(t, srv, "pub-fn"))

	configs := []struct {
		name        string
		path        string
		wantStatus  int
		wantVersion string
		wantTimeout float64
		wantArn     string
		wantCode    string
	}{
		{name: "unqualified is $LATEST", path: "/2015-03-31/functions/pub-fn/configuration",
			wantStatus: 200, wantVersion: "$LATEST", wantTimeout: 30, wantArn: arn},
		{name: "version 1 keeps its configuration", path: "/2015-03-31/functions/pub-fn/configuration?Qualifier=1",
			wantStatus: 200, wantVersion: "1", wantTimeout: 3, wantArn: arn + ":1"},
		{name: "a qualified FunctionName", path: "/2015-03-31/functions/pub-fn:2/configuration",
			wantStatus: 200, wantVersion: "2", wantTimeout: 30, wantArn: arn + ":2"},
		{name: "a FunctionName given as an ARN", path: "/2015-03-31/functions/" + arn + ":1/configuration",
			wantStatus: 200, wantVersion: "1", wantTimeout: 3, wantArn: arn + ":1"},
		{name: "an explicit $LATEST", path: "/2015-03-31/functions/pub-fn/configuration?Qualifier=$LATEST",
			wantStatus: 200, wantVersion: "$LATEST", wantTimeout: 30, wantArn: arn + ":$LATEST"},
		{name: "a version never published", path: "/2015-03-31/functions/pub-fn/configuration?Qualifier=9",
			wantStatus: 404, wantCode: "ResourceNotFoundException"},
		{name: "an alias that does not exist", path: "/2015-03-31/functions/pub-fn/configuration?Qualifier=live",
			wantStatus: 404, wantCode: "ResourceNotFoundException"},
		{name: "qualifiers that disagree", path: "/2015-03-31/functions/pub-fn:1/configuration?Qualifier=2",
			wantStatus: 400, wantCode: "InvalidParameterValueException"},
		{name: "an absent function", path: "/2015-03-31/functions/missing/configuration",
			wantStatus: 404, wantCode: "ResourceNotFoundException"},
	}
	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			status, out, raw := lambdaDo(t, srv, http.MethodGet, tc.path, nil)
			require.Equal(t, tc.wantStatus, status, raw)
			if tc.wantCode != "" {
				assert.Contains(t, raw, tc.wantCode)
				return
			}
			assert.Equal(t, tc.wantVersion, out["Version"])
			assert.Equal(t, tc.wantTimeout, out["Timeout"])
			assert.Equal(t, tc.wantArn, out["FunctionArn"])
		})
	}

	status, fn, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/pub-fn?Qualifier=1", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "1", fn["Configuration"].(map[string]any)["Version"], "GetFunction takes a Qualifier too")
	assert.Equal(t, "Successful", fn["Configuration"].(map[string]any)["LastUpdateStatus"],
		"the status the Terraform provider waits for after an update")

	guards := []struct {
		name     string
		body     map[string]any
		status   int
		wantCode string
	}{
		{name: "a stale RevisionId", body: map[string]any{"RevisionId": "stale"},
			status: http.StatusPreconditionFailed, wantCode: "PreconditionFailedException"},
		{name: "a stale CodeSha256", body: map[string]any{"CodeSha256": "c3RhbGU="},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "an unknown PublishTo", body: map[string]any{"PublishTo": "SOMEWHERE"},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a description over 256 characters", body: map[string]any{"Description": strings.Repeat("d", 257)},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
	}
	for _, tc := range guards {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/pub-fn/versions", tc.body)
			assert.Equal(t, tc.status, status, raw)
			assert.Contains(t, raw, tc.wantCode)
		})
	}

	_, latest, _ := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/pub-fn/configuration", nil)
	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/pub-fn/versions",
		map[string]any{"RevisionId": latest["RevisionId"], "CodeSha256": latest["CodeSha256"]})
	assert.Equal(t, http.StatusCreated, status, "the current RevisionId and CodeSha256 publish: %s", raw)

	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/missing/versions", map[string]any{})
	assert.Equal(t, http.StatusNotFound, status, raw)
}

// TestLambdaListVersionsByFunctionPages pins the listing's order and paging: $LATEST, then the
// versions in numeric order — "10" after "9", which the Terraform provider relies on when it takes
// the last entry as the latest version — cut into pages of at most MaxItems, the final page with no
// NextMarker.
func TestLambdaListVersionsByFunctionPages(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "many-fn", "print(1)", nil)
	lambdaCreateZipFunction(t, srv, "other-fn", "print(1)", nil)
	want := []string{"$LATEST"}
	for i := 1; i <= 11; i++ {
		status, _, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/many-fn/configuration",
			map[string]any{"Description": fmt.Sprintf("revision %d", i)})
		require.Equal(t, http.StatusOK, status, raw)
		require.Equal(t, strconv.Itoa(i), lambdaPublish(t, srv, "many-fn"))
		want = append(want, strconv.Itoa(i))
	}

	for _, size := range []int{1, 2, 5, 50, 10000} {
		assert.Equal(t, want, lambdaListVersionNames(t, srv, "many-fn", size), "MaxItems=%d", size)
	}

	status, out, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/many-fn/versions?MaxItems=3", nil)
	require.Equal(t, http.StatusOK, status, raw)
	marker := out["NextMarker"].(string)
	status, _, raw = lambdaDo(t, srv, http.MethodGet,
		"/2015-03-31/functions/other-fn/versions?Marker="+url.QueryEscape(marker), nil)
	assert.Equal(t, http.StatusBadRequest, status, "a Marker issued for another function is refused: %s", raw)
	assert.Contains(t, raw, "InvalidParameterValueException")
}

// TestLambdaPublishFlagAndImmutability pins Publish=true on CreateFunction and UpdateFunctionCode,
// and that UpdateFunctionConfiguration changes $LATEST and no published version.
func TestLambdaPublishFlagAndImmutability(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	created := lambdaCreateZipFunction(t, srv, "flag-fn", "print(1)", map[string]any{"Publish": true})
	assert.Equal(t, "1", created["Version"], "CreateFunction with Publish answers the version it published")
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:flag-fn:1", created["FunctionArn"])

	status, out, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/flag-fn/code", map[string]any{
		"ZipFile": base64.StdEncoding.EncodeToString([]byte("print(2)")), "Publish": true,
	})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "2", out["Version"])

	status, out, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/flag-fn/code", map[string]any{
		"ZipFile": base64.StdEncoding.EncodeToString([]byte("print(3)")),
	})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "$LATEST", out["Version"], "without Publish nothing is published")

	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/flag-fn/configuration",
		map[string]any{"MemorySize": 1024, "Handler": "other.handler"})
	require.Equal(t, http.StatusOK, status, raw)
	_, v1, _ := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/flag-fn/configuration?Qualifier=1", nil)
	assert.Equal(t, float64(128), v1["MemorySize"], "a published version keeps its configuration")
	assert.Equal(t, "index.handler", v1["Handler"])
	_, v2, _ := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/flag-fn/configuration?Qualifier=2", nil)
	assert.NotEqual(t, v1["CodeSha256"], v2["CodeSha256"], "each version keeps the code it was published from")
	assert.Equal(t, []string{"$LATEST", "1", "2"}, lambdaListVersionNames(t, srv, "flag-fn", 50))
}

// TestLambdaDeleteVersions pins DeleteFunction's Qualifier: a version an alias references is
// refused, a deleted version's number is never reused, and deleting the function deletes its
// versions and aliases, so a function recreated under the name starts again at 1.
func TestLambdaDeleteVersions(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "del-fn", "print(1)", map[string]any{"Publish": true})
	status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/del-fn/aliases",
		map[string]any{"Name": "live", "FunctionVersion": "1"})
	require.Equal(t, http.StatusCreated, status, raw)

	refusals := []struct {
		name, path, wantCode string
		status               int
	}{
		{name: "a version an alias references", path: "/2015-03-31/functions/del-fn?Qualifier=1",
			status: http.StatusConflict, wantCode: "ResourceConflictException"},
		{name: "an alias is not a version", path: "/2015-03-31/functions/del-fn?Qualifier=live",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "$LATEST on its own", path: "/2015-03-31/functions/del-fn?Qualifier=$LATEST",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "a version never published", path: "/2015-03-31/functions/del-fn?Qualifier=7",
			status: http.StatusNotFound, wantCode: "ResourceNotFoundException"},
		{name: "qualifiers that disagree", path: "/2015-03-31/functions/del-fn:1?Qualifier=2",
			status: http.StatusBadRequest, wantCode: "InvalidParameterValueException"},
		{name: "an absent function's version", path: "/2015-03-31/functions/missing?Qualifier=1",
			status: http.StatusNotFound, wantCode: "ResourceNotFoundException"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, http.MethodDelete, tc.path, nil)
			assert.Equal(t, tc.status, status, raw)
			assert.Contains(t, raw, tc.wantCode)
		})
	}

	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/del-fn/aliases/live", nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/del-fn:1", nil)
	require.Equal(t, http.StatusNoContent, status, "a qualified FunctionName deletes the version: %s", raw)
	status, _, _ = lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/del-fn/configuration?Qualifier=1", nil)
	assert.Equal(t, http.StatusNotFound, status)
	status, _, _ = lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/del-fn", nil)
	assert.Equal(t, http.StatusOK, status, "deleting a version leaves the function")

	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/del-fn/configuration",
		map[string]any{"Timeout": 9})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "2", lambdaPublish(t, srv, "del-fn"), "a deleted version's number is not reused")

	status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/del-fn/aliases",
		map[string]any{"Name": "live", "FunctionVersion": "2"})
	require.Equal(t, http.StatusCreated, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/del-fn", nil)
	require.Equal(t, http.StatusNoContent, status, raw)

	lambdaCreateZipFunction(t, srv, "del-fn", "print(1)", nil)
	assert.Equal(t, []string{"$LATEST"}, lambdaListVersionNames(t, srv, "del-fn", 50), "the function's versions went with it")
	status, out, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/del-fn/aliases", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Empty(t, out["Aliases"], "and its aliases")
	assert.Equal(t, "1", lambdaPublish(t, srv, "del-fn"), "a recreated function numbers from 1")
}

// TestLambdaFunctionNameOfAnotherAccountIsRefused pins that a FunctionName naming another account's
// function, as a full or a partial ARN, is refused rather than resolved: substrate does not evaluate
// that function's resource policy for the caller. The caller's own function in another Region is
// still reached through its ARN.
func TestLambdaFunctionNameOfAnotherAccountIsRefused(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "mine", "print(1)", map[string]any{"Publish": true})
	const theirs = "arn:aws:lambda:us-east-1:210987654321:function:mine"
	refusals := []struct {
		name, method, path string
		body               any
	}{
		{name: "DeleteFunction by full ARN", method: http.MethodDelete, path: "/2015-03-31/functions/" + theirs},
		{name: "DeleteFunction of a version", method: http.MethodDelete, path: "/2015-03-31/functions/" + theirs + "?Qualifier=1"},
		{name: "AddPermission by full ARN", method: http.MethodPost, path: "/2015-03-31/functions/" + theirs + "/policy",
			body: map[string]any{"StatementId": "s", "Action": "lambda:InvokeFunction", "Principal": "*"}},
		{name: "AddPermission by partial ARN", method: http.MethodPost, path: "/2015-03-31/functions/210987654321:function:mine/policy",
			body: map[string]any{"StatementId": "s", "Action": "lambda:InvokeFunction", "Principal": "*"}},
		{name: "Invoke", method: http.MethodPost, path: "/2015-03-31/functions/" + theirs + ":1/invocations", body: map[string]any{}},
		{name: "RemovePermission", method: http.MethodDelete, path: "/2015-03-31/functions/" + theirs + "/policy/s"},
		{name: "PublishVersion", method: http.MethodPost, path: "/2015-03-31/functions/" + theirs + "/versions", body: map[string]any{}},
		{name: "CreateAlias", method: http.MethodPost, path: "/2015-03-31/functions/" + theirs + "/aliases",
			body: map[string]any{"Name": "live", "FunctionVersion": "1"}},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, tc.method, tc.path, tc.body)
			assert.Equal(t, http.StatusForbidden, status, raw)
			assert.Contains(t, raw, "AccessDeniedException")
			assert.Contains(t, raw, "because no resource-based policy allows the lambda:")
		})
	}
	status, _, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/mine", nil)
	assert.Equal(t, http.StatusOK, status, "the caller's own function is untouched: %s", raw)

	status, _, raw = lambdaDo(t, srv, http.MethodGet,
		"/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:mine/configuration", nil)
	assert.Equal(t, http.StatusOK, status, "the caller's own ARN resolves: %s", raw)
	for _, path := range []string{
		"/2015-03-31/functions/arn:aws:lambda:us-west-2:123456789012:function:mine/configuration",
		"/2015-03-31/functions/arn:aws:lambda:us-west-2:123456789012:function:mine:1/invocations",
		"/2017-03-31/tags/arn:aws:lambda:us-west-2:123456789012:function:mine",
	} {
		method := http.MethodGet
		if strings.HasSuffix(path, "/invocations") {
			method = http.MethodPost
		}
		status, _, raw = lambdaDo(t, srv, method, path, map[string]any{})
		assert.Equal(t, http.StatusNotFound, status, "%s: %s", path, raw)
		assert.Contains(t, raw, "Functions from 'us-west-2' are not reachable in this region ('us-east-1')", path)
	}
}

// TestLambdaTagsAndMappingsOfAnotherAccountAreRefused pins the same refusal on the tag operations,
// which name their function by ARN, and on event source mappings, which invoke the function they
// name: neither reaches another account's function, and a refused TagResource writes nothing.
func TestLambdaTagsAndMappingsOfAnotherAccountAreRefused(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "mine", "print(1)", map[string]any{"Publish": true})
	lambdaCreateZipFunction(t, srv, "other", "print(1)", nil)
	const theirs = "arn:aws:lambda:us-east-1:210987654321:function:mine"

	for _, tc := range []struct {
		name, method, path string
		body               any
	}{
		{name: "TagResource", method: http.MethodPost, path: "/2017-03-31/tags/" + theirs,
			body: map[string]any{"Tags": map[string]string{"team": "x"}}},
		{name: "UntagResource", method: http.MethodDelete, path: "/2017-03-31/tags/" + theirs + "?tagKeys=team"},
		{name: "ListTags", method: http.MethodGet, path: "/2017-03-31/tags/" + theirs},
		{name: "CreateEventSourceMapping", method: http.MethodPost, path: "/2015-03-31/event-source-mappings",
			body: map[string]any{"FunctionName": theirs, "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:q"}},
		{name: "CreateEventSourceMapping by partial ARN", method: http.MethodPost, path: "/2015-03-31/event-source-mappings",
			body: map[string]any{"FunctionName": "210987654321:function:mine", "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:q"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, tc.method, tc.path, tc.body)
			assert.Equal(t, http.StatusForbidden, status, raw)
			assert.Contains(t, raw, "AccessDeniedException")
		})
	}
	status, fn, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/functions/mine", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Nil(t, fn["Tags"], "the caller's same-named function is not tagged")

	status, created, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings",
		map[string]any{"FunctionName": "mine:1", "EventSourceArn": "arn:aws:sns:us-east-1:123456789012:t"})
	require.Equal(t, http.StatusCreated, status, raw)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:mine:1", created["FunctionArn"])
	uuid := created["UUID"].(string)

	status, _, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
		map[string]any{"FunctionName": theirs})
	assert.Equal(t, http.StatusForbidden, status, "an update cannot re-point a mapping at another account: %s", raw)
	status, updated, raw := lambdaDo(t, srv, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
		map[string]any{"FunctionName": "other"})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:other", updated["FunctionArn"])
	for fnName, want := range map[string]int{"mine:1": 0, "other": 1} {
		status, list, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings?FunctionName="+fnName, nil)
		require.Equal(t, http.StatusOK, status, raw)
		assert.Len(t, list["EventSourceMappings"], want, "%s's mappings after the re-point", fnName)
	}
}

// TestLambdaUnqualifiedOperationsRefuseAQualifiedName pins that the operations whose FunctionName
// names the function itself refuse one carrying a version or alias.
func TestLambdaUnqualifiedOperationsRefuseAQualifiedName(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "plain", "print(1)", map[string]any{"Publish": true})
	cases := []struct {
		name, method, path string
		body               any
	}{
		{name: "PublishVersion", method: http.MethodPost, path: "/2015-03-31/functions/plain:1/versions", body: map[string]any{}},
		{name: "ListVersionsByFunction", method: http.MethodGet, path: "/2015-03-31/functions/plain:1/versions"},
		{name: "CreateAlias", method: http.MethodPost, path: "/2015-03-31/functions/plain:1/aliases",
			body: map[string]any{"Name": "live", "FunctionVersion": "1"}},
		{name: "ListAliases", method: http.MethodGet, path: "/2015-03-31/functions/plain:live/aliases"},
		{name: "GetAlias", method: http.MethodGet, path: "/2015-03-31/functions/plain:1/aliases/live"},
		{name: "UpdateAlias", method: http.MethodPut, path: "/2015-03-31/functions/plain:1/aliases/live", body: map[string]any{}},
		{name: "DeleteAlias", method: http.MethodDelete, path: "/2015-03-31/functions/plain:1/aliases/live"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, tc.method, tc.path, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, "InvalidParameterValueException")
		})
	}
}

// TestLambdaPublishSeesAnS3PackageMove pins that moving a function to another S3 object publishes a
// new version even when substrate's S3 holds neither object, so neither has a CodeSha256.
func TestLambdaPublishSeesAnS3PackageMove(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	status, out, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "s3-fn", "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/lambda-role", "Publish": true,
		"Code": map[string]any{"S3Bucket": "artifacts", "S3Key": "fn-v1.zip"},
	})
	require.Equal(t, http.StatusCreated, status, raw)
	require.Equal(t, "1", out["Version"])
	require.Empty(t, out["CodeSha256"], "the object is not in substrate's S3, so there is no digest")

	status, out, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/s3-fn/code",
		map[string]any{"S3Bucket": "artifacts", "S3Key": "fn-v1.zip", "Publish": true})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "1", out["Version"], "the same object is the same code")

	status, out, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/s3-fn/code",
		map[string]any{"S3Bucket": "artifacts", "S3Key": "fn-v2.zip", "Publish": true})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "2", out["Version"], "another object publishes a new version")

	status, out, raw = lambdaDo(t, srv, http.MethodPut, "/2015-03-31/functions/s3-fn/code",
		map[string]any{"S3Bucket": "artifacts", "S3Key": "fn-v2.zip", "S3ObjectVersion": "abc", "Publish": true})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "3", out["Version"], "so does another object version")
}
