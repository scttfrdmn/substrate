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
