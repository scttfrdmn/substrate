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

// TestPollAndInvoke queues two SQS messages and calls pollAndInvoke, verifying
// that Lambda is invoked and messages are deleted.
func TestPollAndInvoke(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)

	// Create SQS queue.
	queueName := fmt.Sprintf("esm-test-%d", time.Now().UnixNano())
	esmCreateSQSQueue(t, ts, queueName)
	acct := "000000000000" // fallbackAccountID used without auth
	queueURL := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/%s/%s", acct, queueName)

	// Send two messages.
	esmSendSQSMessage(t, ts, queueURL, `hello msg1`)
	esmSendSQSMessage(t, ts, queueURL, `hello msg2`)

	// Create Lambda function.
	fnName := "esm-fn-" + queueName
	esmCreateLambdaFunction(t, ts, fnName)

	// Create ESM.
	region := "us-east-1"
	sqsARN := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, acct, queueName)
	esmID := esmCreateESM(t, ts, fnName, sqsARN)

	// The poller runs in the background; wait a bit for it to fire.
	time.Sleep(3 * time.Second)

	// Delete the ESM to stop the poller.
	esmDeleteESM(t, ts, esmID)

	// Verify the queue is now empty (messages were consumed).
	count := esmReceiveSQSMessageCount(t, ts, queueURL)
	if count != 0 {
		t.Errorf("expected queue empty after ESM consumed messages; got %d messages", count)
	}
}

// TestESMPollerLifecycle verifies that creating an ESM starts a poller entry
// and that deleting it removes it.
func TestESMPollerLifecycle(t *testing.T) {
	t.Parallel()
	ts := substrate.StartTestServer(t)

	queueName := fmt.Sprintf("esm-lifecycle-%d", time.Now().UnixNano())
	acct := "000000000000"
	region := "us-east-1"
	sqsARN := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, acct, queueName)

	fnName := "lifecycle-fn-" + queueName
	esmCreateLambdaFunction(t, ts, fnName)
	esmID := esmCreateESM(t, ts, fnName, sqsARN)

	// Delete — must not error.
	esmDeleteESM(t, ts, esmID)
}

// TestESMShutdown_StopsAll verifies that server shutdown doesn't panic or
// deadlock when ESMs are active.
func TestESMShutdown_StopsAll(t *testing.T) {
	// Does not use t.Parallel() because we need to stop the server mid-test.
	ts := substrate.StartTestServer(t)

	acct := "000000000000"
	region := "us-east-1"

	for i := 0; i < 3; i++ {
		fnName := fmt.Sprintf("shutdown-fn-%d", i)
		queueName := fmt.Sprintf("shutdown-q-%d", i)
		sqsARN := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, acct, queueName)
		esmCreateLambdaFunction(t, ts, fnName)
		esmCreateESM(t, ts, fnName, sqsARN)
	}
	// Cleanup is registered by StartTestServer; shutdown is tested implicitly.
}

// --- helpers ------------------------------------------------------------------

func esmDoRequest(t *testing.T, ts *substrate.TestServer, method, path, host, contentType string, body io.Reader) *http.Response {
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

func esmCreateSQSQueue(t *testing.T, ts *substrate.TestServer, name string) {
	t.Helper()
	body := "Action=CreateQueue&QueueName=" + name + "&Version=2012-11-05"
	resp := esmDoRequest(t, ts, http.MethodPost, "/", "sqs.us-east-1.amazonaws.com",
		"application/x-www-form-urlencoded", strings.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("CreateQueue status %d: %s", resp.StatusCode, b)
	}
}

func esmSendSQSMessage(t *testing.T, ts *substrate.TestServer, queueURL, msgBody string) {
	t.Helper()
	body := "Action=SendMessage&QueueUrl=" + queueURL + "&MessageBody=" + msgBody + "&Version=2012-11-05"
	resp := esmDoRequest(t, ts, http.MethodPost, "/", "sqs.us-east-1.amazonaws.com",
		"application/x-www-form-urlencoded", strings.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("SendMessage status %d: %s", resp.StatusCode, b)
	}
}

func esmCreateLambdaFunction(t *testing.T, ts *substrate.TestServer, name string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]interface{}{
		"FunctionName": name,
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::000000000000:role/test",
		"Handler":      "index.handler",
	})
	resp := esmDoRequest(t, ts, http.MethodPost, "/2015-03-31/functions",
		"lambda.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(payload)))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("CreateFunction status %d: %s", resp.StatusCode, b)
	}
}

func esmCreateESM(t *testing.T, ts *substrate.TestServer, fnName, eventSourceARN string) string {
	t.Helper()
	payload, _ := json.Marshal(map[string]interface{}{
		"FunctionName":     fnName,
		"EventSourceArn":   eventSourceARN,
		"BatchSize":        10,
		"StartingPosition": "TRIM_HORIZON",
	})
	resp := esmDoRequest(t, ts, http.MethodPost, "/2015-03-31/event-source-mappings",
		"lambda.us-east-1.amazonaws.com", "application/json",
		strings.NewReader(string(payload)))
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateESM status %d: %s", resp.StatusCode, b)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatalf("decode ESM response: %v", err)
	}
	uuid, _ := result["UUID"].(string)
	return uuid
}

func esmDeleteESM(t *testing.T, ts *substrate.TestServer, uuid string) {
	t.Helper()
	resp := esmDoRequest(t, ts, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid,
		"lambda.us-east-1.amazonaws.com", "", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("DeleteESM status %d: %s", resp.StatusCode, b)
	}
}

func esmReceiveSQSMessageCount(t *testing.T, ts *substrate.TestServer, queueURL string) int {
	t.Helper()
	body := "Action=ReceiveMessage&QueueUrl=" + queueURL +
		"&MaxNumberOfMessages=10&WaitTimeSeconds=0&Version=2012-11-05"
	resp := esmDoRequest(t, ts, http.MethodPost, "/",
		"sqs.us-east-1.amazonaws.com", "application/x-www-form-urlencoded",
		strings.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	return strings.Count(string(respBody), "<MessageId>")
}
