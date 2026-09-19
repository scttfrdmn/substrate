package emulator_test

import (
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for #1142: a Lambda operation is routed under the API version date its own page publishes.
//
// Three families of these tests, because the defect had three consequences. The routing is asserted
// over the wire, since that is where a caller met it. The resolved operation *name* is asserted
// through the exported parser, because over the wire an undocumented API version and an operation
// substrate does not route are the same 404 — and the name is what authorization, metering and fault
// injection are keyed on. The authorization resource ARN is asserted the same way, because a server
// with no principal authorizes everything and reports nothing about what it evaluated.

// lambdaVersionsARN is the ARN of the function the routing tests create.
const lambdaVersionsARN = "arn:aws:lambda:us-east-1:123456789012:function:versions-fn"

// newLambdaVersionsWorld starts a Lambda server holding one function.
func newLambdaVersionsWorld(t *testing.T) *emulator.Server {
	t.Helper()
	srv := newLambdaTestServer(t)
	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "versions-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	return srv
}

// TestLambdaTagOperationsRouteUnderTheirPublishedDate is #1142's own report: the three tag
// operations are implemented, and the only thing between a caller and them was the date.
func TestLambdaTagOperationsRouteUnderTheirPublishedDate(t *testing.T) {
	tests := []struct {
		op         string
		method     string
		body       any
		wantStatus int
	}{
		{op: "TagResource", method: http.MethodPost,
			body:       map[string]any{"Tags": map[string]string{"Application": "lagotto"}},
			wantStatus: http.StatusNoContent},
		{op: "ListTags", method: http.MethodGet, wantStatus: http.StatusOK},
		{op: "UntagResource", method: http.MethodDelete, wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			srv := newLambdaVersionsWorld(t)

			resp := lambdaRequest(t, srv, tt.method, "/2017-03-31/tags/"+lambdaVersionsARN, tt.body)
			assert.Equal(t, tt.wantStatus, resp.StatusCode,
				"%s is published at /2017-03-31/tags/{Resource}", tt.op)
			require.NoError(t, resp.Body.Close())

			// The same call under the functions date is the 404 every SDK saw, and stays one: a URI
			// AWS does not serve is not one substrate accepts.
			resp = lambdaRequest(t, srv, tt.method, "/2015-03-31/tags/"+lambdaVersionsARN, tt.body)
			assert.Equal(t, http.StatusNotFound, resp.StatusCode,
				"no Lambda API version but 2017-03-31 publishes a /tags/ path")
			require.NoError(t, resp.Body.Close())
		})
	}
}

// TestLambdaTagsRoundTripThroughThePublishedDate is the write the 404 was hiding: a tag set survives
// the call and comes back on the read, which is the step #1142's reporter could not exercise.
func TestLambdaTagsRoundTripThroughThePublishedDate(t *testing.T) {
	srv := newLambdaVersionsWorld(t)

	resp := lambdaRequest(t, srv, http.MethodPost, "/2017-03-31/tags/"+lambdaVersionsARN,
		map[string]any{"Tags": map[string]string{"Application": "lagotto", "env": "prod"}})
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = lambdaRequest(t, srv, http.MethodGet, "/2017-03-31/tags/"+lambdaVersionsARN, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var listed struct {
		Tags map[string]string `json:"Tags"`
	}
	decodeLambdaJSON(t, resp, &listed)
	assert.Equal(t, map[string]string{"Application": "lagotto", "env": "prod"}, listed.Tags)

	resp = lambdaRequest(t, srv, http.MethodDelete,
		"/2017-03-31/tags/"+lambdaVersionsARN+"?tagKeys=env", nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = lambdaRequest(t, srv, http.MethodGet, "/2017-03-31/tags/"+lambdaVersionsARN, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// A fresh destination, because decoding into the one above would merge into the map it already
	// holds rather than replace it, and a removed key would still look present.
	var remaining struct {
		Tags map[string]string `json:"Tags"`
	}
	decodeLambdaJSON(t, resp, &remaining)
	assert.Equal(t, map[string]string{"Application": "lagotto"}, remaining.Tags)
}

// TestLambdaDatedSubResourcesRouteUnderTheirOwnDate covers the two families the issue's sweep asked
// about and found: `invoke-async` is published at 2014-11-13 and `event-invoke-config` at
// 2019-09-25, and both were routed only under the functions date.
func TestLambdaDatedSubResourcesRouteUnderTheirOwnDate(t *testing.T) {
	tests := []struct {
		op         string
		method     string
		suffix     string
		published  string
		body       any
		wantStatus int
	}{
		{op: "InvokeAsync", method: http.MethodPost, suffix: "invoke-async",
			published: "2014-11-13", wantStatus: http.StatusAccepted},
		{op: "PutFunctionEventInvokeConfig", method: http.MethodPut, suffix: "event-invoke-config",
			published:  "2019-09-25",
			body:       map[string]any{"MaximumRetryAttempts": 2},
			wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			srv := newLambdaVersionsWorld(t)

			resp := lambdaRequest(t, srv, tt.method,
				"/"+tt.published+"/functions/versions-fn/"+tt.suffix, tt.body)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			resp = lambdaRequest(t, srv, tt.method,
				"/2015-03-31/functions/versions-fn/"+tt.suffix, tt.body)
			assert.Equal(t, http.StatusNotFound, resp.StatusCode,
				"%s is published at /%s, not under the functions date", tt.op, tt.published)
			require.NoError(t, resp.Body.Close())
		})
	}
}

// TestLambdaFunctionPathsKeepTheirOwnDate is the other direction of the same rule: the operations
// that were already right must not start accepting the dates the tag and sub-resource families use.
func TestLambdaFunctionPathsKeepTheirOwnDate(t *testing.T) {
	srv := newLambdaVersionsWorld(t)

	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/versions-fn", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	for _, version := range []string{"2014-11-13", "2017-03-31", "2019-09-25", "2018-10-31"} {
		resp = lambdaRequest(t, srv, http.MethodGet, "/"+version+"/functions/versions-fn", nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode,
			"GetFunction is published at /2015-03-31, so /%s must not route it", version)
		require.NoError(t, resp.Body.Close())

		resp = lambdaRequest(t, srv, http.MethodGet, "/"+version+"/functions", nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode,
			"ListFunctions is published at /2015-03-31, so /%s must not route it", version)
		require.NoError(t, resp.Body.Close())
	}
}

// TestLambdaOperationNameIsResolvedFromTheAPIVersion asserts the resolved name rather than the
// status, because that is what authorization, metering and fault injection read — a tag call
// resolving to "Unknown" could be neither allowed nor denied by a policy naming
// `lambda:TagResource`, and a fault seeded on the operation could not fire.
func TestLambdaOperationNameIsResolvedFromTheAPIVersion(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		path     string
		wantOp   string
		wantName string
		wantSub  string
	}{
		{name: "tag under its published date", method: http.MethodPost,
			path: "/2017-03-31/tags/" + lambdaVersionsARN, wantOp: "TagResource", wantName: lambdaVersionsARN},
		{name: "list tags under its published date", method: http.MethodGet,
			path: "/2017-03-31/tags/" + lambdaVersionsARN, wantOp: "ListTags", wantName: lambdaVersionsARN},
		{name: "untag under its published date", method: http.MethodDelete,
			path: "/2017-03-31/tags/" + lambdaVersionsARN, wantOp: "UntagResource", wantName: lambdaVersionsARN},
		{name: "tag under the functions date", method: http.MethodPost,
			path: "/2015-03-31/tags/" + lambdaVersionsARN, wantOp: "Unknown"},
		{name: "invoke-async under its published date", method: http.MethodPost,
			path: "/2014-11-13/functions/fn/invoke-async", wantOp: "InvokeAsync", wantName: "fn"},
		{name: "invoke-async under the functions date", method: http.MethodPost,
			path: "/2015-03-31/functions/fn/invoke-async", wantOp: "Unknown"},
		{name: "event-invoke-config under its published date", method: http.MethodPut,
			path:   "/2019-09-25/functions/fn/event-invoke-config",
			wantOp: "PutFunctionEventInvokeConfig", wantName: "fn"},
		{name: "event-invoke-config under the functions date", method: http.MethodPut,
			path: "/2015-03-31/functions/fn/event-invoke-config", wantOp: "Unknown"},
		{name: "a statement id is still a sub-resource", method: http.MethodDelete,
			path: "/2015-03-31/functions/fn/policy/allow-s3", wantOp: "RemovePermission",
			wantName: "fn", wantSub: "allow-s3"},
		{name: "an event source mapping uuid", method: http.MethodGet,
			path: "/2015-03-31/event-source-mappings/uuid-1", wantOp: "GetEventSourceMapping",
			wantName: "uuid-1"},
		{name: "event source mappings under the tags date", method: http.MethodGet,
			path: "/2017-03-31/event-source-mappings", wantOp: "Unknown"},
		// A path carrying no API version segment at all, which no SDK emits: every Lambda URI in
		// the service model begins with one.
		{name: "no version segment", method: http.MethodPost, path: "/functions", wantOp: "Unknown"},
		{name: "version segment only", method: http.MethodGet, path: "/2015-03-31", wantOp: "Unknown"},
		{name: "no leading slash", method: http.MethodGet, path: "2015-03-31/functions", wantOp: "Unknown"},
		{name: "an unrouted published path", method: http.MethodPost,
			path: "/2018-10-31/layers/my-layer/versions", wantOp: "Unknown"},
		// A method no page publishes for the path, which is how a hand-built request gets a URI right
		// and the verb wrong. Each of these reached a real handler under some other method.
		{name: "a method the tags path does not publish", method: http.MethodPut,
			path: "/2017-03-31/tags/" + lambdaVersionsARN, wantOp: "Unknown"},
		{name: "a method the mappings collection does not publish", method: http.MethodPut,
			path: "/2015-03-31/event-source-mappings", wantOp: "Unknown"},
		{name: "a method one mapping does not publish", method: http.MethodPost,
			path: "/2015-03-31/event-source-mappings/uuid-1", wantOp: "Unknown"},
		{name: "a sub-resource no page publishes", method: http.MethodGet,
			path: "/2015-03-31/functions/fn/aliases", wantOp: "Unknown"},
		// A path that only begins like /functions, which the prefix match must not claim.
		{name: "a segment merely starting with functions", method: http.MethodGet,
			path: "/2015-03-31/functionsets", wantOp: "Unknown"},
		// A function sub-resource under one of the other three dates: the date gates the whole family,
		// not just the two sub-resources published elsewhere.
		{name: "a function sub-resource under the event-invoke date", method: http.MethodPut,
			path: "/2019-09-25/functions/fn/code", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, name, sub := emulator.ParseLambdaOperationForTest(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantName, name)
			assert.Equal(t, tt.wantSub, sub)
		})
	}
}

// TestLambdaAuthzResourceARNNamesTheResourceTheRequestNames pins the resource half of the same
// resolution: a policy scoped to one function ARN has to match a request that names that function,
// whichever API version the operation is published under.
func TestLambdaAuthzResourceARNNamesTheResourceTheRequestNames(t *testing.T) {
	const (
		region = "us-east-1"
		acct   = "123456789012"
		fnARN  = "arn:aws:lambda:us-east-1:123456789012:function:versions-fn"
	)
	tests := []struct {
		name string
		path string
		want string
	}{
		// A tags request names a whole ARN, and that ARN is the answer: reassembling one from the
		// caller's own account and Region would retarget a cross-account ARN at the caller's own
		// function of that name.
		{name: "tags names an ARN", path: "/2017-03-31/tags/" + fnARN, want: fnARN},
		{name: "a cross-account tags ARN is not retargeted",
			path: "/2017-03-31/tags/arn:aws:lambda:eu-west-1:210987654321:function:theirs",
			want: "arn:aws:lambda:eu-west-1:210987654321:function:theirs"},
		{name: "a function path names a function", path: "/2015-03-31/functions/versions-fn", want: fnARN},
		{name: "a sub-resource names its function",
			path: "/2015-03-31/functions/versions-fn/configuration", want: fnARN},
		// Both were "*" before #1142, because the resolver read the functions date literally.
		{name: "invoke-async names its function",
			path: "/2014-11-13/functions/versions-fn/invoke-async", want: fnARN},
		{name: "event-invoke-config names its function",
			path: "/2019-09-25/functions/versions-fn/event-invoke-config", want: fnARN},
		{name: "an operation naming no resource", path: "/2015-03-31/functions", want: "*"},
		{name: "an unrouted date", path: "/2018-10-31/layers/my-layer/versions", want: "*"},
		{name: "an empty tags resource", path: "/2017-03-31/tags/", want: "*"},
		{name: "no version segment", path: "/functions/versions-fn", want: "*"},
		// A path with no second segment at all, so there is nothing to read a version from.
		{name: "a single segment", path: "/functions", want: "*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, emulator.LambdaAuthzResourceARNForTest(tt.path, region, acct))
		})
	}
}
