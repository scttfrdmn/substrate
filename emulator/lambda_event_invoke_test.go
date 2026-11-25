package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lambdaEventInvokePath = "/2019-09-25/functions/eic/event-invoke-config"

// TestLambdaEventInvokeConfig_PutEchoesAndGetReadsBack is #1685's report: Put stores and echoes
// DestinationConfig, MaximumRetryAttempts and MaximumEventAgeInSeconds, with the qualified FunctionArn
// and LastModified, and Get answers the same.
func TestLambdaEventInvokeConfig_PutEchoesAndGetReadsBack(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)

	status, put, raw := lambdaDo(t, srv, http.MethodPut, lambdaEventInvokePath, map[string]any{
		"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 3600,
		"DestinationConfig": map[string]any{
			"OnFailure": map[string]any{"Destination": "arn:aws:sqs:us-east-1:123456789012:dlq"},
		},
	})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:eic:$LATEST", put["FunctionArn"])
	assert.Equal(t, float64(1), put["MaximumRetryAttempts"])
	assert.Equal(t, float64(3600), put["MaximumEventAgeInSeconds"])
	assert.Equal(t, map[string]any{
		"OnSuccess": map[string]any{},
		"OnFailure": map[string]any{"Destination": "arn:aws:sqs:us-east-1:123456789012:dlq"},
	}, put["DestinationConfig"])
	assert.IsType(t, float64(0), put["LastModified"], "LastModified is Unix seconds")

	status, got, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath, nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, put, got)
}

// TestLambdaEventInvokeConfig_PutReplacesAndUpdateMerges pins the two write semantics.
func TestLambdaEventInvokeConfig_PutReplacesAndUpdateMerges(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)

	status, _, raw := lambdaDo(t, srv, http.MethodPost, lambdaEventInvokePath, map[string]any{"MaximumRetryAttempts": 0})
	assert.Equal(t, http.StatusNotFound, status, "Update of no configuration: %s", raw)
	assert.Contains(t, raw, "doesn't have an EventInvokeConfig")

	status, _, raw = lambdaDo(t, srv, http.MethodPut, lambdaEventInvokePath, map[string]any{
		"MaximumRetryAttempts": 2, "DestinationConfig": map[string]any{
			"OnSuccess": map[string]any{"Destination": "arn:aws:sns:us-east-1:123456789012:ok"},
		},
	})
	require.Equal(t, http.StatusOK, status, raw)

	status, got, raw := lambdaDo(t, srv, http.MethodPost, lambdaEventInvokePath, map[string]any{"MaximumEventAgeInSeconds": 60})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, float64(2), got["MaximumRetryAttempts"], "Update keeps what it does not name")
	assert.Equal(t, float64(60), got["MaximumEventAgeInSeconds"])
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:ok",
		got["DestinationConfig"].(map[string]any)["OnSuccess"].(map[string]any)["Destination"])

	status, got, raw = lambdaDo(t, srv, http.MethodPut, lambdaEventInvokePath, map[string]any{"MaximumEventAgeInSeconds": 120})
	require.Equal(t, http.StatusOK, status, raw)
	assert.NotContains(t, got, "MaximumRetryAttempts", "Put removes what it does not name")
	assert.Equal(t, map[string]any{"OnSuccess": map[string]any{}, "OnFailure": map[string]any{}}, got["DestinationConfig"])
}

// TestLambdaEventInvokeConfig_QualifiersHoldTheirOwn pins that a version and an alias each hold a
// configuration of their own, named by Qualifier or by a qualified FunctionName, that List answers
// all of them, and that deleting the alias, the version or the function deletes theirs.
func TestLambdaEventInvokeConfig_QualifiersHoldTheirOwn(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)
	version := lambdaPublish(t, srv, "eic")
	status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/eic/aliases",
		map[string]any{"Name": "live", "FunctionVersion": version})
	require.Equal(t, http.StatusCreated, status, raw)

	for path, retries := range map[string]int{
		lambdaEventInvokePath:                                           0,
		lambdaEventInvokePath + "?Qualifier=" + version:                 1,
		"/2019-09-25/functions/eic:live/event-invoke-config":            2,
		lambdaEventInvokePath + "?Qualifier=" + "%24LATEST":             0,
		"/2019-09-25/functions/eic:" + version + "/event-invoke-config": 1,
	} {
		status, _, raw := lambdaDo(t, srv, http.MethodPut, path, map[string]any{"MaximumRetryAttempts": retries})
		require.Equal(t, http.StatusOK, status, "%s: %s", path, raw)
	}

	status, got, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"?Qualifier=live", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:eic:live", got["FunctionArn"])
	assert.Equal(t, float64(2), got["MaximumRetryAttempts"])

	listArns := func() []string {
		t.Helper()
		status, got, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"/list", nil)
		require.Equal(t, http.StatusOK, status, raw)
		var arns []string
		for _, c := range got["FunctionEventInvokeConfigs"].([]any) {
			arns = append(arns, c.(map[string]any)["FunctionArn"].(string))
		}
		return arns
	}
	const fn = "arn:aws:lambda:us-east-1:123456789012:function:eic"
	assert.Equal(t, []string{fn + ":$LATEST", fn + ":" + version, fn + ":live"}, listArns())

	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/eic/aliases/live", nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/eic?Qualifier="+version, nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	assert.Equal(t, []string{fn + ":$LATEST"}, listArns(), "a deleted version or alias takes its configuration with it")

	status, _, raw = lambdaDo(t, srv, http.MethodDelete, lambdaEventInvokePath, nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	status, _, raw = lambdaDo(t, srv, http.MethodDelete, lambdaEventInvokePath, nil)
	assert.Equal(t, http.StatusNotFound, status, "nothing left to delete: %s", raw)
	status, _, raw = lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath, nil)
	assert.Equal(t, http.StatusNotFound, status, raw)
	assert.Contains(t, raw, "The function "+fn+":$LATEST doesn't have an EventInvokeConfig")
}

// TestLambdaEventInvokeConfig_DeletingTheFunctionDeletesEveryConfiguration pins that a function
// recreated under the same name starts with no configuration.
func TestLambdaEventInvokeConfig_DeletingTheFunctionDeletesEveryConfiguration(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)
	version := lambdaPublish(t, srv, "eic")
	for _, q := range []string{"", "?Qualifier=" + version} {
		status, _, raw := lambdaDo(t, srv, http.MethodPut, lambdaEventInvokePath+q, map[string]any{"MaximumRetryAttempts": 1})
		require.Equal(t, http.StatusOK, status, raw)
	}
	status, _, raw := lambdaDo(t, srv, http.MethodDelete, "/2015-03-31/functions/eic", nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)
	lambdaPublish(t, srv, "eic")

	status, got, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"/list", nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Empty(t, got["FunctionEventInvokeConfigs"])
	assert.NotContains(t, got, "NextMarker")
}

// TestLambdaEventInvokeConfig_ListPages pins ListFunctionEventInvokeConfigs' MaxItems and Marker.
func TestLambdaEventInvokeConfig_ListPages(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)
	status, _, raw := lambdaDo(t, srv, http.MethodPut, lambdaEventInvokePath, map[string]any{})
	require.Equal(t, http.StatusOK, status, raw)
	lambdaPublish(t, srv, "eic")
	status, _, raw = lambdaDo(t, srv, http.MethodPut, lambdaEventInvokePath+"?Qualifier=1", map[string]any{})
	require.Equal(t, http.StatusOK, status, raw)

	status, first, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"/list?MaxItems=1", nil)
	require.Equal(t, http.StatusOK, status, raw)
	require.Len(t, first["FunctionEventInvokeConfigs"], 1)
	marker, _ := first["NextMarker"].(string)
	require.NotEmpty(t, marker)
	status, second, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"/list?MaxItems=1&Marker="+marker, nil)
	require.Equal(t, http.StatusOK, status, raw)
	require.Len(t, second["FunctionEventInvokeConfigs"], 1)
	assert.NotContains(t, second, "NextMarker")
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:eic:1",
		second["FunctionEventInvokeConfigs"].([]any)[0].(map[string]any)["FunctionArn"])

	for _, q := range []string{"?MaxItems=0", "?MaxItems=51", "?MaxItems=x", "?Marker=not-a-marker"} {
		status, _, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"/list"+q, nil)
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", q, raw)
	}
	status, _, raw = lambdaDo(t, srv, http.MethodGet, "/2019-09-25/functions/eic:1/event-invoke-config/list", nil)
	assert.Equal(t, http.StatusBadRequest, status, "List takes the function, not a version: %s", raw)
}

// TestLambdaEventInvokeConfig_Refusals covers the validation and the lookups each operation makes.
func TestLambdaEventInvokeConfig_Refusals(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)
	cases := []struct {
		name, method, path string
		body               any
		status             int
		message            string
	}{
		{"retries above 2", http.MethodPut, lambdaEventInvokePath, map[string]any{"MaximumRetryAttempts": 3}, http.StatusBadRequest, "between 0 and 2"},
		{"negative retries", http.MethodPost, lambdaEventInvokePath, map[string]any{"MaximumRetryAttempts": -1}, http.StatusBadRequest, "between 0 and 2"},
		{"event age below 60", http.MethodPut, lambdaEventInvokePath, map[string]any{"MaximumEventAgeInSeconds": 59}, http.StatusBadRequest, "between 60 and 21600"},
		{"event age above 21600", http.MethodPut, lambdaEventInvokePath, map[string]any{"MaximumEventAgeInSeconds": 21601}, http.StatusBadRequest, "between 60 and 21600"},
		{"an S3 bucket on success", http.MethodPut, lambdaEventInvokePath, map[string]any{
			"DestinationConfig": map[string]any{"OnSuccess": map[string]any{"Destination": "arn:aws:s3:::bucket"}},
		}, http.StatusBadRequest, "OnFailure"},
		{"an unparseable body", http.MethodPut, lambdaEventInvokePath, "not json", http.StatusBadRequest, "InvalidParameterValueException"},
		{"an absent function", http.MethodGet, "/2019-09-25/functions/absent/event-invoke-config", nil, http.StatusNotFound, "Function not found"},
		{"an absent version", http.MethodPut, lambdaEventInvokePath + "?Qualifier=9", map[string]any{}, http.StatusNotFound, "Function not found"},
		{"an absent alias", http.MethodDelete, lambdaEventInvokePath + "?Qualifier=nope", nil, http.StatusNotFound, "Function not found"},
		{"list of an absent function", http.MethodGet, "/2019-09-25/functions/absent/event-invoke-config/list", nil, http.StatusNotFound, "Function not found"},
		{"another account's function", http.MethodGet,
			"/2019-09-25/functions/arn:aws:lambda:us-east-1:210987654321:function:eic/event-invoke-config", nil, http.StatusForbidden, "AccessDeniedException"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := lambdaDo(t, srv, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.status, status, raw)
			assert.Contains(t, raw, tc.message)
		})
	}
	status, _, raw := lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath, nil)
	assert.Equal(t, http.StatusNotFound, status, "a refused Put stores nothing: %s", raw)
}
