package emulator_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLambdaListVersionsByFunction covers the read the Terraform AWS provider makes on every
// refresh of an aws_lambda_function (#1519): a function's only version is $LATEST.
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
