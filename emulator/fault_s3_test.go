package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// newS3FaultServer builds an S3 server with a FaultController attached, returning
// both so a test can arm rules in process and observe the wire response.
//
// It mirrors newS3TestServerWithFS rather than calling it because the controller has
// to be present when the server is constructed: ServerOptions is read once, and a
// fault evaluated at pipeline step 4.5 cannot be added afterwards.
func newS3FaultServer(t *testing.T, cfg emulator.FaultConfig) (*emulator.Server, *emulator.FaultController) {
	t.Helper()
	conf := emulator.DefaultConfig()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	s3p := &emulator.S3Plugin{}
	state := emulator.NewMemoryStateManager()
	require.NoError(t, s3p.Initialize(t.Context(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))

	registry := emulator.NewPluginRegistry()
	registry.Register(s3p)

	fc := emulator.NewFaultController(cfg, 42)
	srv := emulator.NewServer(*conf, registry,
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"}),
		state, tc, logger, emulator.ServerOptions{Fault: fc})

	return srv, fc
}

// newFaultOnlyServer builds a server with a fault controller and no plugins at all.
// Faults are evaluated at pipeline step 4.5, before routing, so a rule fires without
// any plugin being registered — which is exactly why the error's wire shape is the
// pipeline's responsibility rather than a plugin's.
func newFaultOnlyServer(t *testing.T, cfg emulator.FaultConfig) *httptest.Server {
	t.Helper()
	conf := emulator.DefaultConfig()
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	srv := emulator.NewServer(*conf, emulator.NewPluginRegistry(),
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"}),
		state, tc, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.ServerOptions{Fault: emulator.NewFaultController(cfg, 42)})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// queryRequest posts a form-encoded query-protocol request to a fault-only server,
// addressed to the named service. Faults are evaluated before routing, so no plugin
// need be registered and the service name — carried by the Host header, which is
// what the parser reads — is the only thing that varies.
func queryRequest(t *testing.T, ts *httptest.Server, service string, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = service + ".us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// TestFault_OtherProtocolsAreUnchanged is the regression guard on #480's error-shape
// fix: only S3's document was wrong, so every other service must serialize an injected
// error exactly as it did before. CloudFormation is the Query protocol and SQS is JSON
// RPC, which are the two shapes the S3 arm was carved out of and beside.
//
// EC2 was this test's Query vehicle until #591 found EC2 is not a Query service at
// all — it is the sole member of the "ec2" protocol — so the Query case moved to
// CloudFormation and EC2 got its own subtest asserting its own document.
func TestFault_OtherProtocolsAreUnchanged(t *testing.T) {
	t.Parallel()

	t.Run("cloudformation stays query xml", func(t *testing.T) {
		t.Parallel()
		ts := newFaultOnlyServer(t, emulator.FaultConfig{
			Enabled: true,
			Rules: []emulator.FaultRule{{
				Service:    "cloudformation",
				FaultType:  "error",
				ErrorCode:  "Throttling",
				HTTPStatus: http.StatusServiceUnavailable,
				ErrorMsg:   "Rate exceeded.",
				Times:      -1,
			}},
		})

		resp := queryRequest(t, ts, "cloudformation", map[string]string{"Action": "DescribeStacks"})
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		assert.Equal(t, "text/xml; charset=UTF-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var doc struct {
			XMLName xml.Name `xml:"ErrorResponse"`
			Error   struct {
				Type string `xml:"Type"`
				Code string `xml:"Code"`
			} `xml:"Error"`
		}
		require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
		assert.Equal(t, "Throttling", doc.Error.Code)
		assert.Equal(t, "Sender", doc.Error.Type)
	})

	// An injected EC2 error must carry the code where the SDK reads it, which is the
	// #591 half of the property #480 established for S3: a fault the client cannot
	// match on is a fault the consumer's error branch never sees.
	t.Run("ec2 uses the ec2 document", func(t *testing.T) {
		t.Parallel()
		ts := newFaultOnlyServer(t, emulator.FaultConfig{
			Enabled: true,
			Rules: []emulator.FaultRule{{
				Service:    "ec2",
				FaultType:  "error",
				ErrorCode:  "RequestLimitExceeded",
				HTTPStatus: http.StatusServiceUnavailable,
				ErrorMsg:   "Request limit exceeded.",
				Times:      -1,
			}},
		})

		resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		assert.Equal(t, "text/xml; charset=UTF-8", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var doc struct {
			XMLName   xml.Name `xml:"Response"`
			Code      string   `xml:"Errors>Error>Code"`
			Message   string   `xml:"Errors>Error>Message"`
			RequestID string   `xml:"RequestID"`
		}
		require.NoError(t, xml.Unmarshal(body, &doc), "body was %s", body)
		assert.Equal(t, "RequestLimitExceeded", doc.Code)
		assert.Equal(t, "Request limit exceeded.", doc.Message)
		assert.Equal(t, "SUBSTRATE", doc.RequestID)
	})

	t.Run("sqs stays json rpc", func(t *testing.T) {
		t.Parallel()
		ts := newFaultOnlyServer(t, emulator.FaultConfig{
			Enabled: true,
			Rules: []emulator.FaultRule{{
				Service:    "sqs",
				FaultType:  "error",
				ErrorCode:  "RequestThrottled",
				HTTPStatus: http.StatusServiceUnavailable,
				ErrorMsg:   "Request throttled.",
				Times:      -1,
			}},
		})

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(`{}`))
		require.NoError(t, err)
		req.Host = "sqs.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var out map[string]string
		require.NoError(t, json.Unmarshal(body, &out), "body was %s", body)
		// "__type" is the member botocore's JSON parser reads; the header is what a
		// REST-JSON parser would read, and SQS sends both today.
		assert.Equal(t, "RequestThrottled", out["__type"])
		assert.Equal(t, "RequestThrottled", resp.Header.Get("x-amzn-ErrorType"))
		assert.NotContains(t, string(body), "<Error")
	})
}

// TestS3Fault_ErrorCodeReachesTheClient covers #480 findings 3 and 4 together, over
// the wire. Finding 4 causes finding 3: the pipeline serialized an injected error as
// the wrapped <ErrorResponse> document, S3's parser recovers no code from that shape,
// and so a requested SlowDown arrived as ServiceUnavailable — the HTTP status the SDK
// falls back to. A consumer matching on SlowDown never saw their own fault.
func TestS3Fault_ErrorCodeReachesTheClient(t *testing.T) {
	t.Parallel()
	srv, _ := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "PutObject",
			FaultType:  "error",
			ErrorCode:  "SlowDown",
			HTTPStatus: http.StatusServiceUnavailable,
			ErrorMsg:   "Please reduce your request rate.",
			Times:      -1,
		}},
	})

	w := s3Request(t, srv, http.MethodPut, "/faulty/k", []byte("v"), nil)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	body := parseS3Error(t, w.Body.Bytes())
	assert.Equal(t, "SlowDown", body.Code)
	assert.Equal(t, "Please reduce your request rate.", body.Message)
	assert.NotEqual(t, "ServiceUnavailable", body.Code,
		"the SDK falls back to the status when the document shape is wrong")
}

// TestS3Fault_ErrorIsByteIdenticalToAGenuineOne is the assertion that proves
// indistinguishability, which is the one property a fault injector must not lack: a
// caller able to tell an injected NoSuchKey from a real one can tell a fault fixture
// from production. Comparing the documents byte for byte is the only way to assert it.
func TestS3Fault_ErrorIsByteIdenticalToAGenuineOne(t *testing.T) {
	t.Parallel()

	// A genuine NoSuchKey, raised by the plugin from a bucket that exists.
	plain, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, plain, http.MethodPut, "/real", nil, nil).Code)
	genuine := s3Request(t, plain, http.MethodGet, "/real/absent", nil, nil)
	require.Equal(t, http.StatusNotFound, genuine.Code)

	// The same code injected by the pipeline, which never reaches the plugin at all.
	srv, _ := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "GetObject",
			FaultType:  "error",
			ErrorCode:  "NoSuchKey",
			HTTPStatus: http.StatusNotFound,
			ErrorMsg:   "The specified key does not exist.",
			Times:      -1,
		}},
	})
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/real", nil, nil).Code)
	injected := s3Request(t, srv, http.MethodGet, "/real/absent", nil, nil)

	assert.Equal(t, genuine.Code, injected.Code)
	assert.Equal(t, genuine.Body.String(), injected.Body.String())
	assert.Equal(t, genuine.Header().Get("Content-Type"), injected.Header().Get("Content-Type"))

	// And the shape itself: a bare <Error> with a <RequestId>, not the
	// <ErrorResponse> wrapper the Query protocol uses.
	var doc struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		RequestID string   `xml:"RequestId"`
	}
	require.NoError(t, xml.Unmarshal(injected.Body.Bytes(), &doc), "body was %s", injected.Body.String())
	assert.Equal(t, "NoSuchKey", doc.Code)
	assert.NotEmpty(t, doc.RequestID)
	assert.NotContains(t, injected.Body.String(), "<ErrorResponse")
}

// TestS3Fault_OperationRuleDoesNotHitUploadPart is the #480 finding-1 gate on the
// wire. PutObject and UploadPart are both a PUT to an object path, so a rule that
// could only name the HTTP method took out both — and a fixture arming a PutObject
// fault would fail a part upload instead, somewhere the test was not looking.
func TestS3Fault_OperationRuleDoesNotHitUploadPart(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "PutObject",
			FaultType:  "error",
			ErrorCode:  "SlowDown",
			HTTPStatus: http.StatusServiceUnavailable,
			Times:      -1,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/multipart-scope", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/multipart-scope/big.bin?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code, "CreateMultipartUpload must not match a PutObject rule")
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	// UploadPart is a PUT to the same path and must not match.
	pw := s3Request(t, srv, http.MethodPut,
		"/multipart-scope/big.bin?partNumber=1&uploadId="+ir.UploadID, []byte("data"), nil)
	assert.Equal(t, http.StatusOK, pw.Code, "UploadPart must not match a PutObject rule")
	assert.Equal(t, 0, fc.FaultsFired(), "no fault should have fired yet")

	// A plain PutObject does match.
	ow := s3Request(t, srv, http.MethodPut, "/multipart-scope/plain.txt", []byte("v"), nil)
	assert.Equal(t, http.StatusServiceUnavailable, ow.Code)
	assert.Equal(t, "SlowDown", parseS3Error(t, ow.Body.Bytes()).Code)
	assert.Equal(t, 1, fc.FaultsFired())
}

// TestS3Fault_CompleteFailsLeavingNoOrphanedUpload is the test #480's reporter could
// not write. Failing CompleteMultipartUpload after every part has uploaded needs a
// rule that hits exactly that request: it shares POST and the object path with
// CreateMultipartUpload and differs only in its query parameter, so neither an
// operation-less rule nor one naming the HTTP method could reach it without also
// killing the create. QueryKey plus the semantic name select it precisely.
//
// The assertion is that a failed Complete leaves the upload open — which is real S3's
// behavior, and the reason a consumer's cleanup path exists: the parts are still
// billable until something aborts them.
func TestS3Fault_CompleteFailsLeavingNoOrphanedUpload(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "CompleteMultipartUpload",
			QueryKey:   "uploadId",
			FaultType:  "error",
			ErrorCode:  "InternalError",
			HTTPStatus: http.StatusInternalServerError,
			Times:      1,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/orphan", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/orphan/big.bin?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, "orphan", "big.bin", ir.UploadID, 1, []byte("only part"))

	// Complete fails once.
	cw := s3Request(t, srv, http.MethodPost,
		"/orphan/big.bin?uploadId="+ir.UploadID, completeBody(etag), nil)
	require.Equal(t, http.StatusInternalServerError, cw.Code)
	assert.Equal(t, "InternalError", parseS3Error(t, cw.Body.Bytes()).Code)
	assert.Equal(t, 1, fc.FaultsFired(), "the rule must have fired, not merely matched nothing")

	// The upload is still open, and the object was never assembled.
	assert.Equal(t, []string{ir.UploadID}, openUploadIDs(t, srv, "orphan"),
		"a failed Complete leaves the upload for the caller to abort")
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/orphan/big.bin", nil, nil).Code)

	// With the rule spent (Times: 1), a retry succeeds — which is what makes the
	// consumer's retry path assertable rather than merely armed.
	retry := s3Request(t, srv, http.MethodPost,
		"/orphan/big.bin?uploadId="+ir.UploadID, completeBody(etag), nil)
	require.Equal(t, http.StatusOK, retry.Code)
	assert.Empty(t, openUploadIDs(t, srv, "orphan"))
	assert.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodGet, "/orphan/big.bin", nil, nil).Code)
}

// TestS3Fault_QueryKeySelectsTheCreate is the other half of the multipart split: a
// rule naming the create's sub-resource parameter must not reach the parts, which
// share the method and path with it under a different parameter.
func TestS3Fault_QueryKeySelectsTheCreate(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			QueryKey:   "uploads",
			FaultType:  "error",
			ErrorCode:  "SlowDown",
			HTTPStatus: http.StatusServiceUnavailable,
			Times:      1,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/querykey-scope", nil, nil).Code)

	blocked := s3Request(t, srv, http.MethodPost, "/querykey-scope/big.bin?uploads", nil, nil)
	require.Equal(t, http.StatusServiceUnavailable, blocked.Code)
	assert.Equal(t, 1, fc.FaultsFired())

	// The rule is spent; the same request now succeeds and the parts are untouched.
	iw := s3Request(t, srv, http.MethodPost, "/querykey-scope/big.bin?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))
	uploadPart(t, srv, "querykey-scope", "big.bin", ir.UploadID, 1, []byte("data"))
	assert.Equal(t, 1, fc.FaultsFired(), "UploadPart carries no ?uploads and must not match")
}

// TestS3Fault_PathSuffixSelectsOneKey pins the matcher a caller reaches for when the
// operation name is right but the target is not: one key, or one file extension,
// rather than every write to the bucket.
func TestS3Fault_PathSuffixSelectsOneKey(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "PutObject",
			PathSuffix: ".parquet",
			FaultType:  "error",
			ErrorCode:  "SlowDown",
			HTTPStatus: http.StatusServiceUnavailable,
			Times:      -1,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/pathsuffix-scope", nil, nil).Code)
	assert.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/pathsuffix-scope/notes.txt", []byte("v"), nil).Code)
	assert.Equal(t, http.StatusServiceUnavailable,
		s3Request(t, srv, http.MethodPut, "/pathsuffix-scope/data.parquet", []byte("v"), nil).Code)
	assert.Equal(t, 1, fc.FaultsFired())
}

// TestS3Fault_HeaderPrefixSelectsARequestFamily covers the third wire matcher, on the
// case it exists for: a header family is a property of the request that no operation
// name carries, so a rule targeting encrypted writes alone needs it.
func TestS3Fault_HeaderPrefixSelectsARequestFamily(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:      "s3",
			Operation:    "PutObject",
			HeaderPrefix: "x-amz-server-side-encryption",
			FaultType:    "error",
			ErrorCode:    "KMS.KeyUnavailableException",
			HTTPStatus:   http.StatusServiceUnavailable,
			Times:        -1,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/headerprefix-scope", nil, nil).Code)
	assert.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/headerprefix-scope/plain.txt", []byte("v"), nil).Code)

	w := s3Request(t, srv, http.MethodPut, "/headerprefix-scope/secret.txt", []byte("v"),
		map[string]string{"x-amz-server-side-encryption": "aws:kms"})
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "KMS.KeyUnavailableException", parseS3Error(t, w.Body.Bytes()).Code)
	assert.Equal(t, 1, fc.FaultsFired())
}

// TestS3Fault_TimesTwoThenSuccess is the retry outcome the bound exists to produce,
// asserted end to end: two failures, then the write lands. Before Times existed a rule
// could only fail forever, so the one thing a retry test needs to observe — recovery —
// was unobservable.
func TestS3Fault_TimesTwoThenSuccess(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "PutObject",
			FaultType:  "error",
			ErrorCode:  "SlowDown",
			HTTPStatus: http.StatusServiceUnavailable,
			Times:      2,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/retry", nil, nil).Code)

	codes := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		codes = append(codes, s3Request(t, srv, http.MethodPut, "/retry/k", []byte("v"), nil).Code)
	}
	assert.Equal(t, []int{
		http.StatusServiceUnavailable,
		http.StatusServiceUnavailable,
		http.StatusOK,
	}, codes)
	assert.Equal(t, 2, fc.FaultsFired())

	// The third write actually stored the object; a spent rule must let the request
	// through to the plugin rather than merely stop reporting an error.
	gw := s3Request(t, srv, http.MethodGet, "/retry/k", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, "v", gw.Body.String())
}

// TestS3Fault_DefaultTimesFiresExactlyOnce asserts the zero-means-one default through
// the wire, deliberately: it is a choice, not an inevitability, and reading zero as
// unlimited would turn a mistyped field into a fixture that consumes the whole retry
// budget.
func TestS3Fault_DefaultTimesFiresExactlyOnce(t *testing.T) {
	t.Parallel()
	srv, fc := newS3FaultServer(t, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:    "s3",
			Operation:  "PutObject",
			FaultType:  "error",
			ErrorCode:  "SlowDown",
			HTTPStatus: http.StatusServiceUnavailable,
		}},
	})

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/once", nil, nil).Code)
	assert.Equal(t, http.StatusServiceUnavailable,
		s3Request(t, srv, http.MethodPut, "/once/k", []byte("v"), nil).Code)
	assert.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/once/k", []byte("v"), nil).Code)
	assert.Equal(t, 1, fc.FaultsFired())
}
