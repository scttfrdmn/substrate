package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestSQSQueryCompatibleErrorHeader pins the x-amzn-query-error header SQS answers a
// query-mode JSON client with (#1519): the model's Query code, which for a missing queue is
// AWS.SimpleQueueService.NonExistentQueue rather than the shape name in __type.
func TestSQSQueryCompatibleErrorHeader(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	tests := []struct {
		name       string
		queryMode  bool
		target     string
		body       string
		wantHeader string
	}{
		{name: "a missing queue answers the legacy code", queryMode: true,
			target: "AmazonSQS.GetQueueAttributes", body: `{"QueueUrl":"http://sqs.us-east-1.localhost/123456789012/absent"}`,
			wantHeader: "AWS.SimpleQueueService.NonExistentQueue;Sender"},
		{name: "a code whose Query spelling is its name passes through", queryMode: true,
			target: "AmazonSQS.GetQueueUrl", body: `{}`, wantHeader: ""},
		{name: "no header without query mode", queryMode: false,
			target: "AmazonSQS.GetQueueAttributes", body: `{"QueueUrl":"http://sqs.us-east-1.localhost/123456789012/absent"}`,
			wantHeader: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(tc.body))
			require.NoError(t, err)
			req.Host = "sqs.us-east-1.amazonaws.com"
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			req.Header.Set("X-Amz-Target", tc.target)
			if tc.queryMode {
				req.Header.Set("X-Amzn-Query-Mode", "true")
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.GreaterOrEqual(t, resp.StatusCode, 400)
			got := resp.Header.Get("x-amzn-query-error")
			if tc.wantHeader == "" {
				if tc.queryMode {
					// The code passes through unchanged: the header names the same code as
					// the error type.
					assert.Equal(t, resp.Header.Get("x-amzn-ErrorType")+";Sender", got)
				} else {
					assert.Empty(t, got)
				}
				return
			}
			assert.Equal(t, tc.wantHeader, got)
			assert.Equal(t, "QueueDoesNotExist", resp.Header.Get("x-amzn-ErrorType"),
				"the shape name stays in the error type, for the SDK to type the error by")
		})
	}
}
