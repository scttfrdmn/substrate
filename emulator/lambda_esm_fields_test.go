package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// esmFieldsDo sends one request to host on ts and decodes the JSON answer.
func esmFieldsDo(t *testing.T, ts *emulator.TestServer, host, method, path string, body any, headers map[string]string) (int, map[string]any, string) {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var decoded map[string]any
	if len(out) > 0 && out[0] == '{' {
		require.NoError(t, json.Unmarshal(out, &decoded), "%s", out)
	}
	return resp.StatusCode, decoded, string(out)
}

// esmLambda is [esmFieldsDo] against Lambda.
func esmLambda(t *testing.T, ts *emulator.TestServer, method, path string, body any) (int, map[string]any, string) {
	t.Helper()
	return esmFieldsDo(t, ts, "lambda.us-east-1.amazonaws.com", method, path, body, nil)
}

// esmFunction creates a function with the given timeout.
func esmFunction(t *testing.T, ts *emulator.TestServer, name string, timeout int) {
	t.Helper()
	status, _, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": name, "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/r", "Timeout": timeout,
	})
	require.Equal(t, http.StatusCreated, status, raw)
}

// esmQueue creates an SQS queue, with a VisibilityTimeout when visibility is not empty.
func esmQueue(t *testing.T, ts *emulator.TestServer, name, visibility string) string {
	t.Helper()
	body := "Action=CreateQueue&QueueName=" + name + "&Version=2012-11-05"
	if visibility != "" {
		body += "&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=" + visibility
	}
	resp := esmDoRequest(t, ts, http.MethodPost, "/", "sqs.us-east-1.amazonaws.com",
		"application/x-www-form-urlencoded", strings.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return "arn:aws:sqs:us-east-1:123456789012:" + name
}

// TestLambdaESMFields_DefaultsBySource pins BatchSize's published per-source default — 10 for SQS,
// 100 for every other source — and that an SQS mapping reports no StartingPosition (#1659).
func TestLambdaESMFields_DefaultsBySource(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	esmFunction(t, ts, "fn", 3)
	cases := []struct {
		name, source, position string
		batch                  float64
	}{
		{"SQS", esmQueue(t, ts, "q", ""), "", 10},
		{"Kinesis", "arn:aws:kinesis:us-east-1:123456789012:stream/s", "TRIM_HORIZON", 100},
		{"DynamoDB Streams", "arn:aws:dynamodb:us-east-1:123456789012:table/t/stream/2026-01-01T00:00:00.000", "TRIM_HORIZON", 100},
		{"MSK", "arn:aws:kafka:us-east-1:123456789012:cluster/c/abc", "TRIM_HORIZON", 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, got, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings",
				map[string]any{"FunctionName": "fn", "EventSourceArn": tc.source})
			require.Equal(t, http.StatusCreated, status, raw)
			assert.Equal(t, tc.batch, got["BatchSize"])
			if tc.position == "" {
				assert.NotContains(t, got, "StartingPosition")
			} else {
				assert.Equal(t, tc.position, got["StartingPosition"])
			}
			uuid := got["UUID"].(string)
			assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:event-source-mapping:"+uuid, got["EventSourceMappingArn"])
		})
	}
}

// TestLambdaESMFields_SQSRefusals covers the refusals an SQS source adds: a StartingPosition, and a
// queue whose visibility timeout is lower than the function's timeout, each in the wording Lambda is
// observed to answer.
func TestLambdaESMFields_SQSRefusals(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	esmFunction(t, ts, "slow", 60)
	esmFunction(t, ts, "quick", 30)
	short := esmQueue(t, ts, "short", "")
	long := esmQueue(t, ts, "long", "120")

	cases := []struct {
		name    string
		body    map[string]any
		message string
	}{
		{"StartingPosition", map[string]any{"FunctionName": "quick", "EventSourceArn": short, "StartingPosition": "LATEST"},
			"StartingPosition is not valid for SQS event sources."},
		{"visibility below the timeout", map[string]any{"FunctionName": "slow", "EventSourceArn": short},
			"Queue visibility timeout: 30 seconds is less than Function timeout: 60 seconds"},
		{"the function by ARN", map[string]any{"FunctionName": "arn:aws:lambda:us-east-1:123456789012:function:slow", "EventSourceArn": short},
			"Queue visibility timeout: 30 seconds is less than Function timeout: 60 seconds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings", tc.body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, "InvalidParameterValueException")
			assert.Contains(t, raw, tc.message)
		})
	}

	status, created, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings",
		map[string]any{"FunctionName": "quick", "EventSourceArn": short})
	require.Equal(t, http.StatusCreated, status, raw)
	uuid := created["UUID"].(string)
	status, _, raw = esmLambda(t, ts, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid, map[string]any{"FunctionName": "slow"})
	assert.Equal(t, http.StatusBadRequest, status, "re-pointing at a slower function is checked too: %s", raw)
	assert.Contains(t, raw, "Queue visibility timeout: 30 seconds is less than Function timeout: 60 seconds")
	status, got, raw := esmLambda(t, ts, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:quick", got["FunctionArn"], "a refused update changes nothing")

	for _, tc := range []struct{ name, fn, source string }{
		{"a timeout equal to the visibility", "quick", short},
		{"a visibility above the timeout", "slow", long},
		{"a function that does not exist", "absent", short},
		{"a queue that does not exist", "slow", "arn:aws:sqs:us-east-1:123456789012:absent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings",
				map[string]any{"FunctionName": tc.fn, "EventSourceArn": tc.source})
			assert.Equal(t, http.StatusCreated, status, raw)
		})
	}
}

// TestLambdaESMFields_VisibilityIsCheckedAgainstTheInvokedVersion pins that the timeout compared is
// the version's the mapping invokes, not $LATEST's.
func TestLambdaESMFields_VisibilityIsCheckedAgainstTheInvokedVersion(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	esmFunction(t, ts, "ver", 10)
	queue := esmQueue(t, ts, "vq", "")
	status, _, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/functions/ver/versions", map[string]any{})
	require.Equal(t, http.StatusCreated, status, raw)
	status, _, raw = esmLambda(t, ts, http.MethodPut, "/2015-03-31/functions/ver/configuration", map[string]any{"Timeout": 60})
	require.Equal(t, http.StatusOK, status, raw)

	status, _, raw = esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings",
		map[string]any{"FunctionName": "ver:1", "EventSourceArn": queue})
	assert.Equal(t, http.StatusCreated, status, "version 1 times out in 10s: %s", raw)
	status, _, raw = esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings",
		map[string]any{"FunctionName": "ver", "EventSourceArn": queue})
	assert.Equal(t, http.StatusBadRequest, status, "$LATEST times out in 60s: %s", raw)
}

// TestLambdaESMFields_ResponseTypesAndScaling records FunctionResponseTypes and ScalingConfig, answers
// them from every mapping operation, validates them, and clears them on an empty update.
func TestLambdaESMFields_ResponseTypesAndScaling(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	esmFunction(t, ts, "fn", 3)
	queue := esmQueue(t, ts, "q", "")

	status, created, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings", map[string]any{
		"FunctionName": "fn", "EventSourceArn": queue,
		"FunctionResponseTypes": []string{"ReportBatchItemFailures"},
		"ScalingConfig":         map[string]any{"MaximumConcurrency": 10},
	})
	require.Equal(t, http.StatusCreated, status, raw)
	uuid := created["UUID"].(string)
	types := []any{"ReportBatchItemFailures"}
	scaling := map[string]any{"MaximumConcurrency": float64(10)}
	assert.Equal(t, types, created["FunctionResponseTypes"])
	assert.Equal(t, scaling, created["ScalingConfig"])

	status, got, raw := esmLambda(t, ts, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, types, got["FunctionResponseTypes"])
	assert.Equal(t, scaling, got["ScalingConfig"])

	status, got, raw = esmLambda(t, ts, http.MethodGet, "/2015-03-31/event-source-mappings?FunctionName=fn", nil)
	require.Equal(t, http.StatusOK, status, raw)
	listed := got["EventSourceMappings"].([]any)[0].(map[string]any)
	assert.Equal(t, scaling, listed["ScalingConfig"])

	status, got, raw = esmLambda(t, ts, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
		map[string]any{"ScalingConfig": map[string]any{"MaximumConcurrency": 500}})
	require.Equal(t, http.StatusOK, status, raw)
	assert.Equal(t, map[string]any{"MaximumConcurrency": float64(500)}, got["ScalingConfig"])
	assert.Equal(t, types, got["FunctionResponseTypes"], "an update naming no response types keeps them")

	status, got, raw = esmLambda(t, ts, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
		map[string]any{"ScalingConfig": map[string]any{}, "FunctionResponseTypes": []string{}})
	require.Equal(t, http.StatusOK, status, raw)
	assert.NotContains(t, got, "ScalingConfig", "an empty ScalingConfig removes it")
	assert.NotContains(t, got, "FunctionResponseTypes", "an empty list clears them")

	for _, tc := range []struct {
		name, method, path string
		body               map[string]any
		message            string
	}{
		{"concurrency below 2", http.MethodPost, "/2015-03-31/event-source-mappings",
			map[string]any{"FunctionName": "fn", "EventSourceArn": queue, "ScalingConfig": map[string]any{"MaximumConcurrency": 1}}, "between 2 and 1000"},
		{"concurrency above 1000", http.MethodPut, "/2015-03-31/event-source-mappings/" + uuid,
			map[string]any{"ScalingConfig": map[string]any{"MaximumConcurrency": 1001}}, "between 2 and 1000"},
		{"scaling on a stream", http.MethodPost, "/2015-03-31/event-source-mappings",
			map[string]any{"FunctionName": "fn", "EventSourceArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
				"ScalingConfig": map[string]any{"MaximumConcurrency": 5}}, "only for Amazon SQS"},
		{"an unknown response type", http.MethodPost, "/2015-03-31/event-source-mappings",
			map[string]any{"FunctionName": "fn", "EventSourceArn": queue, "FunctionResponseTypes": []string{"Everything"}}, "ReportBatchItemFailures"},
		{"two response types", http.MethodPut, "/2015-03-31/event-source-mappings/" + uuid,
			map[string]any{"FunctionResponseTypes": []string{"ReportBatchItemFailures", "ReportBatchItemFailures"}}, "at most one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := esmLambda(t, ts, tc.method, tc.path, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, tc.message)
		})
	}
}

// TestLambdaESMFields_Tags covers the tag operations on an event-source-mapping ARN (#1660): tags
// from the create, TagResource, UntagResource and ListTags; none of them in GetEventSourceMapping; and
// the refusals for another account's, another Region's and an unknown mapping.
func TestLambdaESMFields_Tags(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	esmFunction(t, ts, "fn", 3)
	queue := esmQueue(t, ts, "q", "")
	status, created, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings", map[string]any{
		"FunctionName": "fn", "EventSourceArn": queue, "Tags": map[string]string{"team": "a"},
	})
	require.Equal(t, http.StatusCreated, status, raw)
	assert.NotContains(t, created, "Tags")
	uuid := created["UUID"].(string)
	arn := created["EventSourceMappingArn"].(string)
	tagsPath := "/2017-03-31/tags/" + arn

	listTags := func() map[string]any {
		t.Helper()
		status, got, raw := esmLambda(t, ts, http.MethodGet, tagsPath, nil)
		require.Equal(t, http.StatusOK, status, raw)
		return got["Tags"].(map[string]any)
	}
	assert.Equal(t, map[string]any{"team": "a"}, listTags())

	status, _, raw = esmLambda(t, ts, http.MethodPost, tagsPath, map[string]any{"Tags": map[string]string{"env": "dev"}})
	require.Equal(t, http.StatusNoContent, status, raw)
	assert.Equal(t, map[string]any{"team": "a", "env": "dev"}, listTags())

	status, _, raw = esmLambda(t, ts, http.MethodDelete, tagsPath+"?tagKeys=team", nil)
	require.Equal(t, http.StatusNoContent, status, raw)
	assert.Equal(t, map[string]any{"env": "dev"}, listTags())

	status, got, raw := esmLambda(t, ts, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.NotContains(t, got, "Tags", "EventSourceMappingConfiguration has no Tags member")

	for _, tc := range []struct {
		name, arn string
		status    int
	}{
		{"another account's mapping", "arn:aws:lambda:us-east-1:210987654321:event-source-mapping:" + uuid, http.StatusForbidden},
		{"another Region's mapping", "arn:aws:lambda:us-west-2:123456789012:event-source-mapping:" + uuid, http.StatusNotFound},
		{"an unknown mapping", "arn:aws:lambda:us-east-1:123456789012:event-source-mapping:00000000-0000-0000-0000-000000000000", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []struct {
				method string
				body   any
			}{{http.MethodGet, nil}, {http.MethodPost, map[string]any{"Tags": map[string]string{"x": "y"}}}, {http.MethodDelete, nil}} {
				status, _, raw := esmLambda(t, ts, op.method, "/2017-03-31/tags/"+tc.arn+"?tagKeys=env", op.body)
				assert.Equal(t, tc.status, status, "%s: %s", op.method, raw)
			}
		})
	}
	assert.Equal(t, map[string]any{"env": "dev"}, listTags(), "a refused write changes nothing")
}

// TestLambdaESMFields_GetResourcesListsTaggedMappings pins that Resource Groups Tagging GetResources
// reports a tagged mapping as lambda:event-source-mapping, and a never-tagged one not at all (#1660).
func TestLambdaESMFields_GetResourcesListsTaggedMappings(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	esmFunction(t, ts, "fn", 3)
	queue := esmQueue(t, ts, "q", "")
	status, tagged, raw := esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings", map[string]any{
		"FunctionName": "fn", "EventSourceArn": queue, "Tags": map[string]string{"team": "a"},
	})
	require.Equal(t, http.StatusCreated, status, raw)
	status, _, raw = esmLambda(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings", map[string]any{
		"FunctionName": "fn", "EventSourceArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
	})
	require.Equal(t, http.StatusCreated, status, raw)

	status, got, raw := esmFieldsDo(t, ts, "tagging.us-east-1.amazonaws.com", http.MethodPost, "/",
		map[string]any{"ResourceTypeFilters": []string{"lambda:event-source-mapping"}},
		map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126.GetResources", "Content-Type": "application/x-amz-json-1.1"})
	require.Equal(t, http.StatusOK, status, raw)
	list := got["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1, raw)
	entry := list[0].(map[string]any)
	assert.Equal(t, tagged["EventSourceMappingArn"], entry["ResourceARN"])
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "a"}}, entry["Tags"])
}

// TestLambdaESMFields_TaggingScanKeepsToTheCallersScope pins the scan GetResources runs over the
// mapping records, whose keys carry no scope: another account's or Region's mapping is left out, and
// an unreadable record is reported as an error beside the mappings that could be read.
func TestLambdaESMFields_TaggingScanKeepsToTheCallersScope(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	put := func(uuid, value string) {
		t.Helper()
		require.NoError(t, state.Put(t.Context(), "lambda", "esm:"+uuid, []byte(value)))
	}
	mapping := func(uuid, functionARN string) string {
		return `{"UUID":"` + uuid + `","FunctionArn":"` + functionARN + `","Tags":{"k":"v"}}`
	}
	put("11111111-1111-1111-1111-111111111111", mapping("11111111-1111-1111-1111-111111111111",
		"arn:aws:lambda:us-east-1:123456789012:function:mine"))
	put("22222222-2222-2222-2222-222222222222", mapping("22222222-2222-2222-2222-222222222222",
		"arn:aws:lambda:us-east-1:210987654321:function:theirs"))
	put("33333333-3333-3333-3333-333333333333", mapping("33333333-3333-3333-3333-333333333333",
		"arn:aws:lambda:us-west-2:123456789012:function:west"))
	put("44444444-4444-4444-4444-444444444444", "{not json")

	arns, err := emulator.LambdaTaggedEventSourceMappingsForTest(state, "123456789012", "us-east-1")
	require.Error(t, err, "the unreadable record is reported")
	assert.Equal(t, []string{"arn:aws:lambda:us-east-1:123456789012:event-source-mapping:11111111-1111-1111-1111-111111111111"}, arns)
}
