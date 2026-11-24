package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// lambdaInvokeRecorder wraps the registered Lambda plugin and records the X-Amz-Executed-Version
// of every Invoke it answers, including the ones another service sends through the registry, which
// the event log does not record.
type lambdaInvokeRecorder struct {
	emulator.Plugin
	mu       sync.Mutex
	versions []string
	refused  []string
}

// HandleRequest forwards to the wrapped plugin and records the version an Invoke ran, or the code
// a refused request answered.
func (r *lambdaInvokeRecorder) HandleRequest(ctx *emulator.RequestContext, req *emulator.AWSRequest) (*emulator.AWSResponse, error) {
	resp, err := r.Plugin.HandleRequest(ctx, req)
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		r.mu.Lock()
		r.refused = append(r.refused, awsErr.Code)
		r.mu.Unlock()
	}
	if resp != nil {
		if v := resp.Headers["X-Amz-Executed-Version"]; v != "" {
			r.mu.Lock()
			r.versions = append(r.versions, v)
			r.mu.Unlock()
		}
	}
	return resp, err
}

// refusals returns the error codes recorded so far.
func (r *lambdaInvokeRecorder) refusals() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.refused...)
}

// executed returns the versions recorded so far.
func (r *lambdaInvokeRecorder) executed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.versions...)
}

// internalInvokeFixture is a server holding function "fan-fn" with versions 1 and 2 and alias "live"
// on version 2, with a recorder in front of the Lambda plugin.
type internalInvokeFixture struct {
	ts       *emulator.TestServer
	recorder *lambdaInvokeRecorder
}

// aliasARN is the ARN of the fixture's alias.
const internalInvokeAliasARN = "arn:aws:lambda:us-east-1:123456789012:function:fan-fn:live"

func newInternalInvokeFixture(t *testing.T) internalInvokeFixture {
	t.Helper()
	ts := emulator.StartTestServer(t)
	call := func(method, path string, body any) {
		t.Helper()
		code, raw := fanCall(t, ts, method, path, "lambda.us-east-1.amazonaws.com", "application/json", nil, body)
		require.Less(t, code, 300, "%s %s: %s", method, path, raw)
	}
	call(http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "fan-fn", "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/r", "Code": map[string]any{"ZipFile": "cHJpbnQoMSk="}, "Publish": true,
	})
	call(http.MethodPut, "/2015-03-31/functions/fan-fn/configuration", map[string]any{"Timeout": 9})
	call(http.MethodPost, "/2015-03-31/functions/fan-fn/versions", map[string]any{})
	call(http.MethodPost, "/2015-03-31/functions/fan-fn/aliases", map[string]any{"Name": "live", "FunctionVersion": "2"})

	inner, ok := ts.Registry().Plugin("lambda")
	require.True(t, ok)
	rec := &lambdaInvokeRecorder{Plugin: inner}
	ts.Registry().Register(rec)
	return internalInvokeFixture{ts: ts, recorder: rec}
}

// fanCall sends one request to the test server and returns its status and body. A non-nil body is
// JSON-encoded unless it is already a []byte.
func fanCall(t *testing.T, ts *emulator.TestServer, method, path, host, contentType string,
	headers map[string]string, body any,
) (int, string) {
	t.Helper()
	var payload []byte
	switch b := body.(type) {
	case nil:
	case []byte:
		payload = b
	default:
		var err error
		payload, err = json.Marshal(b)
		require.NoError(t, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// TestLambdaInternalInvokesHonourTheQualifier pins that every service that invokes a function it
// names by a qualified ARN invokes that version or alias (#1613). Each path names the alias, so the
// invocation must run version 2: not $LATEST, and not a function named "live".
func TestLambdaInternalInvokesHonourTheQualifier(t *testing.T) {
	t.Parallel()
	paths := []struct {
		name    string
		trigger func(t *testing.T, ts *emulator.TestServer)
	}{
		{name: "SNS", trigger: func(t *testing.T, ts *emulator.TestServer) {
			code, body := singleAccountQuery(t, ts, "sns", "", map[string]string{"Action": "CreateTopic", "Name": "fan"})
			require.Equal(t, http.StatusOK, code, body)
			topic := "arn:aws:sns:us-east-1:123456789012:fan"
			code, body = singleAccountQuery(t, ts, "sns", "", map[string]string{"Action": "Subscribe",
				"TopicArn": topic, "Protocol": "lambda", "Endpoint": internalInvokeAliasARN})
			require.Equal(t, http.StatusOK, code, body)
			code, body = singleAccountQuery(t, ts, "sns", "", map[string]string{"Action": "Publish",
				"TopicArn": topic, "Message": "hello"})
			require.Equal(t, http.StatusOK, code, body)
		}},
		{name: "S3 notifications", trigger: func(t *testing.T, ts *emulator.TestServer) {
			const host = "s3.amazonaws.com"
			code, body := fanCall(t, ts, http.MethodPut, "/fan-bucket", host, "", nil, nil)
			require.Equal(t, http.StatusOK, code, body)
			code, body = fanCall(t, ts, http.MethodPut, "/fan-bucket?notification", host, "application/xml", nil,
				[]byte(`<NotificationConfiguration><CloudFunctionConfiguration><Id>l</Id>`+
					`<CloudFunction>`+internalInvokeAliasARN+`</CloudFunction><Event>s3:ObjectCreated:*</Event>`+
					`</CloudFunctionConfiguration></NotificationConfiguration>`))
			require.Equal(t, http.StatusOK, code, body)
			code, body = fanCall(t, ts, http.MethodPut, "/fan-bucket/k.txt", host, "text/plain", nil, []byte("hi"))
			require.Equal(t, http.StatusOK, code, body)
		}},
		{name: "API Gateway", trigger: func(t *testing.T, ts *emulator.TestServer) {
			apiID := proxyCreateRESTAPI(t, ts)
			resourceID := proxyGetRootResourceID(t, ts, apiID)
			proxyPutMethod(t, ts, apiID, resourceID, "GET")
			proxyCreateIntegration(t, ts, apiID, resourceID, "GET",
				"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"+internalInvokeAliasARN+"/invocations")
			code, body := fanCall(t, ts, http.MethodGet, "/prod/", apiID+".execute-api.us-east-1.amazonaws.com", "", nil, nil)
			require.Equal(t, http.StatusOK, code, body)
		}},
		{name: "Step Functions", trigger: func(t *testing.T, ts *emulator.TestServer) {
			const host = "states.us-east-1.amazonaws.com"
			sfn := func(op string, body any) string {
				code, raw := fanCall(t, ts, http.MethodPost, "/", host, "application/x-amz-json-1.0",
					map[string]string{"X-Amz-Target": "AWSStepFunctions." + op}, body)
				require.Equal(t, http.StatusOK, code, raw)
				return raw
			}
			raw := sfn("CreateStateMachine", map[string]any{"name": "fan-sm", "roleArn": "arn:aws:iam::123456789012:role/r",
				"definition": `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + internalInvokeAliasARN + `","End":true}}}`})
			var created struct {
				StateMachineArn string `json:"stateMachineArn"`
			}
			require.NoError(t, json.Unmarshal([]byte(raw), &created))
			sfn("StartExecution", map[string]any{"stateMachineArn": created.StateMachineArn, "input": "{}"})
		}},
	}
	for _, tc := range paths {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newInternalInvokeFixture(t)
			tc.trigger(t, f.ts)
			assert.Equal(t, []string{"2"}, f.recorder.executed(), "the alias's version ran, once")
		})
	}
}

// TestLambdaESMInvokesItsAlias pins the event source mapping path: a mapping created on `name:alias`
// polls and invokes the alias, so the recorded invoke ran the alias's version.
func TestLambdaESMInvokesItsAlias(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(esmClockInstant)
	const name = "esm-alias"
	esmCreateSQSQueue(t, ts, name)
	esmSendSQSMessage(t, ts, fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", name), "one")
	esmCreateLambdaFunction(t, ts, name+"-fn")
	for _, step := range []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/2015-03-31/functions/" + name + "-fn/versions", map[string]any{}},
		{http.MethodPost, "/2015-03-31/functions/" + name + "-fn/aliases", map[string]any{"Name": "live", "FunctionVersion": "1"}},
	} {
		code, raw := fanCall(t, ts, step.method, step.path, "lambda.us-east-1.amazonaws.com", "application/json", nil, step.body)
		require.Less(t, code, 300, raw)
	}
	esmCreateESM(t, ts, name+"-fn:live", "arn:aws:sqs:us-east-1:123456789012:"+name)
	ts.AdvanceTime(time.Second)
	// Due work runs before the next top-level request.
	esmPeekCount(t, ts, fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", name))

	events, err := ts.Store().GetStream(t.Context(), "default")
	require.NoError(t, err)
	var executed []string
	for _, e := range events {
		if e.Service == "lambda" && e.Operation == "Invoke" && e.Response != nil {
			executed = append(executed, e.Response.Headers["X-Amz-Executed-Version"])
			assert.Equal(t, "live", e.Request.Params["Qualifier"], "the poll invokes the alias")
			assert.False(t, strings.Contains(e.Request.Path, ":live"), "the alias is the Qualifier, not the name")
		}
	}
	assert.Equal(t, []string{"1"}, executed)
}

// TestLambdaInternalInvokeOfAnotherAccountsFunctionIsRefused pins that a service invoking a function
// ARN in another account reaches neither that function nor the caller's own function of the same
// name: the invoke carries the whole ARN, and Lambda refuses it.
func TestLambdaInternalInvokeOfAnotherAccountsFunctionIsRefused(t *testing.T) {
	t.Parallel()
	f := newInternalInvokeFixture(t)
	const theirs = "arn:aws:lambda:us-east-1:210987654321:function:fan-fn:live"
	code, body := singleAccountQuery(t, f.ts, "sns", "", map[string]string{"Action": "CreateTopic", "Name": "far"})
	require.Equal(t, http.StatusOK, code, body)
	topic := "arn:aws:sns:us-east-1:123456789012:far"
	code, body = singleAccountQuery(t, f.ts, "sns", "", map[string]string{"Action": "Subscribe",
		"TopicArn": topic, "Protocol": "lambda", "Endpoint": theirs})
	require.Equal(t, http.StatusOK, code, body)
	code, body = singleAccountQuery(t, f.ts, "sns", "", map[string]string{"Action": "Publish",
		"TopicArn": topic, "Message": "hello"})
	require.Equal(t, http.StatusOK, code, body)

	assert.Empty(t, f.recorder.executed(), "the caller's same-named fan-fn did not run")
	assert.Equal(t, []string{"AccessDeniedException"}, f.recorder.refusals())
}
