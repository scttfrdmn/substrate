package substrate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// handleS3Presign handles POST /v1/s3/presign.
// It generates a presigned URL that points at the running Substrate server and
// is accepted by the S3 plugin for GET or PUT requests.  The URL embeds
// X-Amz-Algorithm, X-Amz-Date, and X-Amz-Expires so that the emulator's
// expiry check (Step 1.65 in handleAWSRequest) can validate it.
//
// Request body:
//
//	{"bucket": "my-bucket", "key": "path/to/obj", "method": "GET", "expires": 3600}
//
// The method defaults to "GET" and expires defaults to 3600 seconds.
// Response: {"url": "http://...", "method": "GET", "expires": 3600}.
func (s *Server) handleS3Presign(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Bucket  string `json:"bucket"`
		Key     string `json:"key"`
		Method  string `json:"method"`
		Expires int    `json:"expires"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Bucket == "" || body.Key == "" {
		http.Error(w, `{"error":"bucket and key are required"}`, http.StatusBadRequest)
		return
	}

	method := strings.ToUpper(body.Method)
	if method == "" {
		method = "GET"
	}
	expires := body.Expires
	if expires <= 0 {
		expires = 3600
	}

	now := s.tc.Now().UTC()
	dateStr := now.Format("20060102T150405Z")

	host := r.Host
	if host == "" {
		host = "localhost"
	}

	rawURL := fmt.Sprintf(
		"http://%s/%s/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-SignedHeaders=host&X-Amz-Signature=substrate",
		host, body.Bucket, body.Key, dateStr, expires,
	)

	writeJSONDebug(w, s.logger, map[string]interface{}{
		"url":     rawURL,
		"method":  method,
		"expires": expires,
	})
}
