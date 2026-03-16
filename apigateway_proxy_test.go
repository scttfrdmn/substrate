package substrate_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// TestBuildV1ProxyEvent verifies that all required v1 proxy event fields are present.
func TestBuildV1ProxyEvent(t *testing.T) {
	t.Parallel()
	req := &substrate.AWSRequest{
		Operation: "GET",
		Path:      "/prod/users",
		Headers: map[string]string{
			"Host":         "abc123.execute-api.us-east-1.amazonaws.com",
			"Content-Type": "application/json",
		},
		Params: map[string]string{"page": "1"},
		Body:   nil,
	}
	data, err := substrate.BuildV1ProxyEventForTest(req, "abc123", "prod", "/users")
	if err != nil {
		t.Fatalf("BuildV1ProxyEvent: %v", err)
	}
	var event map[string]interface{}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	requiredFields := []string{
		"version", "httpMethod", "path", "resource",
		"headers", "stageVariables", "requestContext",
		"isBase64Encoded",
	}
	for _, f := range requiredFields {
		if _, ok := event[f]; !ok {
			t.Errorf("missing field %q", f)
		}
	}
	if event["version"] != "1.0" {
		t.Errorf("version = %v; want 1.0", event["version"])
	}
	if event["httpMethod"] != "GET" {
		t.Errorf("httpMethod = %v; want GET", event["httpMethod"])
	}
	qs, _ := event["queryStringParameters"].(map[string]interface{})
	if qs["page"] != "1" {
		t.Errorf("queryStringParameters[page] = %v; want 1", qs["page"])
	}
}

// TestParseV1ProxyResponse verifies that a proxy response with statusCode 201 is
// converted to an AWSResponse with HTTP 201.
func TestParseV1ProxyResponse(t *testing.T) {
	t.Parallel()
	respBody, _ := json.Marshal(map[string]interface{}{
		"statusCode": 201,
		"headers":    map[string]string{"Content-Type": "application/json"},
		"body":       `{"id":"123"}`,
	})
	got, err := substrate.ParseProxyResponseForTest(respBody)
	if err != nil {
		t.Fatalf("ParseProxyResponse: %v", err)
	}
	if got.StatusCode != 201 {
		t.Errorf("StatusCode = %d; want 201", got.StatusCode)
	}
	if string(got.Body) != `{"id":"123"}` {
		t.Errorf("Body = %s; want {\"id\":\"123\"}", got.Body)
	}
}

// TestBuildV2ProxyEvent verifies the version 2.0 payload format.
func TestBuildV2ProxyEvent(t *testing.T) {
	t.Parallel()
	req := &substrate.AWSRequest{
		Operation: "POST",
		Path:      "/prod/orders",
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Params: map[string]string{},
		Body:   []byte(`{"qty":3}`),
	}
	data, err := substrate.BuildV2ProxyEventForTest(req, "xyz789", "prod", "/orders")
	if err != nil {
		t.Fatalf("BuildV2ProxyEvent: %v", err)
	}
	var event map[string]interface{}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if event["version"] != "2.0" {
		t.Errorf("version = %v; want 2.0", event["version"])
	}
	if event["body"] != `{"qty":3}` {
		t.Errorf("body = %v; want {\"qty\":3}", event["body"])
	}
	rc, _ := event["requestContext"].(map[string]interface{})
	if rc["stage"] != "prod" {
		t.Errorf("requestContext.stage = %v; want prod", rc["stage"])
	}
}

// TestExtractLambdaARNFromURI verifies that Lambda ARNs are parsed correctly
// from integration URIs.
func TestExtractLambdaARNFromURI(t *testing.T) {
	t.Parallel()
	cases := []struct {
		uri  string
		want string
	}{
		{
			"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:my-fn/invocations",
			"arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		},
		{
			"arn:aws:lambda:us-east-1:123456789012:function:my-fn",
			"arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		},
		{
			"https://example.com/no-functions-here",
			"",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.uri[:min(len(tc.uri), 40)], func(t *testing.T) {
			t.Parallel()
			got := substrate.ExtractLambdaARNFromURIForTest(tc.uri)
			if got != tc.want {
				t.Errorf("got %q; want %q", got, tc.want)
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestParseProxyResponse_InvalidJSON verifies that a non-JSON response body
// is passed through as-is with HTTP 200.
func TestParseProxyResponse_InvalidJSON(t *testing.T) {
	t.Parallel()
	raw := []byte("plain text body")
	got, err := substrate.ParseProxyResponseForTest(raw)
	if err != nil {
		t.Fatalf("ParseProxyResponse: %v", err)
	}
	if got.StatusCode != 200 {
		t.Errorf("StatusCode = %d; want 200", got.StatusCode)
	}
	if string(got.Body) != "plain text body" {
		t.Errorf("Body = %q; want plain text body", got.Body)
	}
}

// TestParseProxyResponse_ZeroStatus verifies that a zero statusCode is treated
// as 200.
func TestParseProxyResponse_ZeroStatus(t *testing.T) {
	t.Parallel()
	respBody, _ := json.Marshal(map[string]interface{}{
		"statusCode": 0,
		"body":       "hello",
	})
	got, err := substrate.ParseProxyResponseForTest(respBody)
	if err != nil {
		t.Fatalf("ParseProxyResponse: %v", err)
	}
	if got.StatusCode != 200 {
		t.Errorf("StatusCode = %d; want 200", got.StatusCode)
	}
}

// TestParseProxyResponse_NilHeaders verifies that a nil headers map in the
// Lambda response is replaced with a default Content-Type header.
func TestParseProxyResponse_NilHeaders(t *testing.T) {
	t.Parallel()
	respBody, _ := json.Marshal(map[string]interface{}{
		"statusCode": 200,
		"body":       "ok",
	})
	got, err := substrate.ParseProxyResponseForTest(respBody)
	if err != nil {
		t.Fatalf("ParseProxyResponse: %v", err)
	}
	if got.Headers["Content-Type"] != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", got.Headers["Content-Type"])
	}
}

// TestParseProxyResponse_Base64 verifies that base64-encoded response bodies
// are decoded correctly.
func TestParseProxyResponse_Base64(t *testing.T) {
	t.Parallel()
	import64 := "aGVsbG8gd29ybGQ=" // "hello world"
	respBody, _ := json.Marshal(map[string]interface{}{
		"statusCode":      200,
		"isBase64Encoded": true,
		"body":            import64,
	})
	got, err := substrate.ParseProxyResponseForTest(respBody)
	if err != nil {
		t.Fatalf("ParseProxyResponse: %v", err)
	}
	if string(got.Body) != "hello world" {
		t.Errorf("Body = %q; want %q", got.Body, "hello world")
	}
}

// TestAPIGatewayProxy_LambdaNotFound verifies that an unknown Lambda URI
// causes the proxy to return a non-200 response.
func TestAPIGatewayProxy_LambdaNotFound(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)

	// Create a REST API.
	apiID := proxyCreateRESTAPI(t, ts)
	resourceID := proxyGetRootResourceID(t, ts, apiID)

	// Put a method on the root resource.
	proxyPutMethod(t, ts, apiID, resourceID, "GET")

	// Put integration pointing to a non-existent Lambda function.
	ghostARN := "arn:aws:lambda:us-east-1:000000000000:function:ghost-fn"
	uri := fmt.Sprintf("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/%s/invocations", ghostARN)
	proxyCreateIntegration(t, ts, apiID, resourceID, "GET", uri)

	// Hit the stage endpoint via execute-api Host header.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/prod/", nil)
	req.Host = fmt.Sprintf("%s.execute-api.us-east-1.amazonaws.com", apiID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Ghost function → Lambda returns 404 → proxy returns non-2xx (e.g. 404 or 502).
	if resp.StatusCode == http.StatusOK {
		t.Errorf("expected non-200 for missing function; got 200")
	}
}

// TestAPIGatewayProxy_EndToEnd creates a REST API with an AWS_PROXY integration
// pointing to a real Lambda function (stub mode) and verifies the proxy returns
// the Lambda's stub response as an HTTP response.
func TestAPIGatewayProxy_EndToEnd(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)

	// Create a Lambda function (stub; Docker not required).
	fnName := fmt.Sprintf("proxy-e2e-fn-%d", time.Now().UnixNano())
	proxyCreateLambdaFunction(t, ts, fnName)

	// Create a REST API.
	apiID := proxyCreateRESTAPI(t, ts)
	resourceID := proxyGetRootResourceID(t, ts, apiID)

	// Put GET method + AWS_PROXY integration pointing at the Lambda function.
	proxyPutMethod(t, ts, apiID, resourceID, "GET")
	acct := "000000000000"
	lambdaARN := fmt.Sprintf("arn:aws:lambda:us-east-1:%s:function:%s", acct, fnName)
	uri := fmt.Sprintf("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/%s/invocations", lambdaARN)
	proxyCreateIntegration(t, ts, apiID, resourceID, "GET", uri)

	// Hit the stage endpoint.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/prod/", nil)
	req.Host = fmt.Sprintf("%s.execute-api.us-east-1.amazonaws.com", apiID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	// Lambda stub returns {"statusCode":200,"body":"null"} which the proxy
	// should translate to HTTP 200.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200; got %d — body: %s", resp.StatusCode, body)
	}
}

// TestAPIGatewayProxy_NoAPIFound verifies a 502 is returned when the API ID
// is unknown (no management entry in state).
func TestAPIGatewayProxy_NoAPIFound(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/prod/", nil)
	req.Host = "unknownapiid.execute-api.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		t.Errorf("expected non-200 for unknown API; got 200")
	}
}

// TestAPIGatewayProxy_V2EndToEnd creates an HTTP API (v2) with a $default
// AWS_PROXY route pointing to a Lambda function and verifies the proxy
// dispatches the request and returns the Lambda stub response.
func TestAPIGatewayProxy_V2EndToEnd(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)

	// Create a Lambda function (stub; Docker not required).
	fnName := fmt.Sprintf("proxy-v2-fn-%d", time.Now().UnixNano())
	proxyCreateLambdaFunction(t, ts, fnName)

	acct := "000000000000"
	lambdaARN := fmt.Sprintf("arn:aws:lambda:us-east-1:%s:function:%s", acct, fnName)
	lambdaURI := fmt.Sprintf("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/%s/invocations", lambdaARN)

	// Create a v2 (HTTP) API.
	apiPayload, _ := json.Marshal(map[string]interface{}{
		"Name":         "proxy-v2-test",
		"ProtocolType": "HTTP",
	})
	apiResp := proxyDoRequest(t, ts, http.MethodPost, "/v2/apis",
		"apigatewayv2.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(apiPayload)))
	apiBody, _ := io.ReadAll(apiResp.Body)
	_ = apiResp.Body.Close()
	if apiResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateApi status %d: %s", apiResp.StatusCode, apiBody)
	}
	var apiResult map[string]interface{}
	if err := json.Unmarshal(apiBody, &apiResult); err != nil {
		t.Fatalf("decode v2 api: %v", err)
	}
	apiID, _ := apiResult["ApiId"].(string)
	if apiID == "" {
		t.Fatalf("no ApiId in response: %s", apiBody)
	}

	// Create integration.
	intPayload, _ := json.Marshal(map[string]interface{}{
		"IntegrationType": "AWS_PROXY",
		"IntegrationUri":  lambdaURI,
	})
	intResp := proxyDoRequest(t, ts, http.MethodPost,
		fmt.Sprintf("/v2/apis/%s/integrations", apiID),
		"apigatewayv2.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(intPayload)))
	intBody, _ := io.ReadAll(intResp.Body)
	_ = intResp.Body.Close()
	if intResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateIntegration status %d: %s", intResp.StatusCode, intBody)
	}
	var intResult map[string]interface{}
	if err := json.Unmarshal(intBody, &intResult); err != nil {
		t.Fatalf("decode integration: %v", err)
	}
	intID, _ := intResult["IntegrationId"].(string)

	// Create a $default route.
	routePayload, _ := json.Marshal(map[string]interface{}{
		"RouteKey": "$default",
		"Target":   "integrations/" + intID,
	})
	routeResp := proxyDoRequest(t, ts, http.MethodPost,
		fmt.Sprintf("/v2/apis/%s/routes", apiID),
		"apigatewayv2.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(routePayload)))
	routeBody, _ := io.ReadAll(routeResp.Body)
	_ = routeResp.Body.Close()
	if routeResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateRoute status %d: %s", routeResp.StatusCode, routeBody)
	}

	// Hit the proxy endpoint via execute-api host.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/prod/foo", nil)
	req.Host = fmt.Sprintf("%s.execute-api.us-east-1.amazonaws.com", apiID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200; got %d — body: %s", resp.StatusCode, body)
	}
}

// --- helpers -----------------------------------------------------------------

func proxyDoRequest(t *testing.T, ts *substrate.TestServer, method, path, host, contentType string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func proxyCreateLambdaFunction(t *testing.T, ts *substrate.TestServer, name string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]interface{}{
		"FunctionName": name,
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::000000000000:role/test",
		"Handler":      "index.handler",
	})
	resp := proxyDoRequest(t, ts, http.MethodPost, "/2015-03-31/functions",
		"lambda.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(payload)))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("CreateFunction status %d: %s", resp.StatusCode, b)
	}
}

func proxyCreateRESTAPI(t *testing.T, ts *substrate.TestServer) string {
	t.Helper()
	payload, _ := json.Marshal(map[string]interface{}{
		"name": fmt.Sprintf("proxy-test-%d", time.Now().UnixNano()),
	})
	resp := proxyDoRequest(t, ts, http.MethodPost, "/restapis",
		"apigateway.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(payload)))
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateRestApi status %d: %s", resp.StatusCode, b)
	}
	_ = resp.Body.Close()
	var result map[string]interface{}
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatalf("decode restapi: %v — body: %s", err, b)
	}
	// RestAPIState uses json tag "Id" (capital I).
	id, _ := result["Id"].(string)
	return id
}

func proxyGetRootResourceID(t *testing.T, ts *substrate.TestServer, apiID string) string {
	t.Helper()
	resp := proxyDoRequest(t, ts, http.MethodGet,
		fmt.Sprintf("/restapis/%s/resources", apiID),
		"apigateway.us-east-1.amazonaws.com", "", nil)
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		t.Fatalf("GetResources status %d: %s", resp.StatusCode, b)
	}
	var result struct {
		Items []struct {
			ID   string `json:"id"`
			Path string `json:"path"`
		} `json:"items"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatalf("decode resources (status %d): %v — body: %s", resp.StatusCode, err, b)
	}
	for _, item := range result.Items {
		if item.Path == "/" {
			return item.ID
		}
	}
	t.Fatal("root resource not found")
	return ""
}

func proxyPutMethod(t *testing.T, ts *substrate.TestServer, apiID, resourceID, httpMethod string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]interface{}{
		"authorizationType": "NONE",
	})
	resp := proxyDoRequest(t, ts, http.MethodPut,
		fmt.Sprintf("/restapis/%s/resources/%s/methods/%s", apiID, resourceID, httpMethod),
		"apigateway.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(payload)))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("PutMethod status %d: %s", resp.StatusCode, b)
	}
}

func proxyCreateIntegration(t *testing.T, ts *substrate.TestServer, apiID, resourceID, httpMethod, uri string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]interface{}{
		"type":       "AWS_PROXY",
		"httpMethod": "POST",
		"uri":        uri,
	})
	resp := proxyDoRequest(t, ts, http.MethodPut,
		fmt.Sprintf("/restapis/%s/resources/%s/methods/%s/integration", apiID, resourceID, httpMethod),
		"apigateway.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(payload)))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("PutIntegration status %d: %s", resp.StatusCode, b)
	}
}
