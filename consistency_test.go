package substrate_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newConsistencyCtrl is a test helper that creates a ConsistencyController
// with the given delay and affected services, panicking on error.
func newConsistencyCtrl(t *testing.T, enabled bool, delay time.Duration, services []string, tc *substrate.TimeController) *substrate.ConsistencyController {
	t.Helper()
	cfg := substrate.ConsistencyConfig{
		Enabled:          enabled,
		PropagationDelay: delay,
		AffectedServices: services,
	}
	ctrl, err := substrate.NewConsistencyController(cfg, tc)
	require.NoError(t, err)
	return ctrl
}

// iamReqCtx returns a minimal RequestContext for consistency tests.
func iamReqCtx() *substrate.RequestContext {
	return &substrate.RequestContext{
		RequestID: "test-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{},
	}
}

func TestConsistencyController_Disabled_NoRejection(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	ctrl := newConsistencyCtrl(t, false, 0, nil, tc)

	writeReq := &substrate.AWSRequest{
		Service: "iam", Operation: "CreateUser",
		Params: map[string]string{"UserName": "alice"},
	}
	readReq := &substrate.AWSRequest{
		Service: "iam", Operation: "GetUser",
		Params: map[string]string{"UserName": "alice"},
	}
	reqCtx := iamReqCtx()

	ctrl.RecordWrite(reqCtx, writeReq)
	require.NoError(t, ctrl.CheckRead(reqCtx, readReq))
}

func TestConsistencyController_WithinDelay_Rejects(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	ctrl := newConsistencyCtrl(t, true, 2*time.Second, []string{"iam"}, tc)

	writeReq := &substrate.AWSRequest{
		Service: "iam", Operation: "CreateUser",
		Params: map[string]string{"UserName": "alice"},
	}
	readReq := &substrate.AWSRequest{
		Service: "iam", Operation: "GetUser",
		Params: map[string]string{"UserName": "alice"},
	}
	reqCtx := iamReqCtx()

	ctrl.RecordWrite(reqCtx, writeReq)

	// Immediately after write — should be rejected.
	err := ctrl.CheckRead(reqCtx, readReq)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "InconsistentStateException", awsErr.Code)
	assert.Equal(t, http.StatusConflict, awsErr.HTTPStatus)
}

func TestConsistencyController_AfterDelay_Allows(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	ctrl := newConsistencyCtrl(t, true, 2*time.Second, []string{"iam"}, tc)

	writeReq := &substrate.AWSRequest{
		Service: "iam", Operation: "CreateUser",
		Params: map[string]string{"UserName": "alice"},
	}
	readReq := &substrate.AWSRequest{
		Service: "iam", Operation: "GetUser",
		Params: map[string]string{"UserName": "alice"},
	}
	reqCtx := iamReqCtx()

	ctrl.RecordWrite(reqCtx, writeReq)

	// Advance past the propagation window.
	tc.SetTime(start.Add(3 * time.Second))

	require.NoError(t, ctrl.CheckRead(reqCtx, readReq))
}

func TestConsistencyController_UnaffectedService_Allowed(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	// Only "iam" is affected.
	ctrl := newConsistencyCtrl(t, true, 2*time.Second, []string{"iam"}, tc)

	writeReq := &substrate.AWSRequest{
		Service: "s3", Operation: "PutObject",
		Params: map[string]string{"BucketName": "my-bucket"},
	}
	readReq := &substrate.AWSRequest{
		Service: "s3", Operation: "GetObject",
		Params: map[string]string{"BucketName": "my-bucket"},
	}
	reqCtx := iamReqCtx()

	ctrl.RecordWrite(reqCtx, writeReq)
	// s3 is not in affected list — should pass.
	require.NoError(t, ctrl.CheckRead(reqCtx, readReq))
}

func TestConsistencyController_ResourceKeyIsolation(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	ctrl := newConsistencyCtrl(t, true, 2*time.Second, []string{"iam"}, tc)

	writeAlice := &substrate.AWSRequest{
		Service: "iam", Operation: "CreateUser",
		Params: map[string]string{"UserName": "alice"},
	}
	readBob := &substrate.AWSRequest{
		Service: "iam", Operation: "GetUser",
		Params: map[string]string{"UserName": "bob"},
	}
	reqCtx := iamReqCtx()

	ctrl.RecordWrite(reqCtx, writeAlice)

	// bob was not written — should not be rejected.
	require.NoError(t, ctrl.CheckRead(reqCtx, readBob))
}

func TestConsistencyController_ReplayingContext_NoOp(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	ctrl := newConsistencyCtrl(t, true, 2*time.Second, []string{"iam"}, tc)

	replayCtx := &substrate.RequestContext{
		RequestID: "replay-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{"replaying": true},
	}

	writeReq := &substrate.AWSRequest{
		Service: "iam", Operation: "CreateUser",
		Params: map[string]string{"UserName": "alice"},
	}
	readReq := &substrate.AWSRequest{
		Service: "iam", Operation: "GetUser",
		Params: map[string]string{"UserName": "alice"},
	}

	// RecordWrite during replay should be a no-op.
	ctrl.RecordWrite(replayCtx, writeReq)

	// CheckRead during replay should always pass.
	require.NoError(t, ctrl.CheckRead(replayCtx, readReq))
}

func TestConsistencyController_InvalidConfig_Error(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.ConsistencyConfig{
		Enabled:          true,
		PropagationDelay: 0, // invalid when enabled
		AffectedServices: []string{"iam"},
	}
	_, err := substrate.NewConsistencyController(cfg, tc)
	require.Error(t, err)
}

func TestConsistencyController_Integration_Returns409(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)

	plug := &serverPlugin{
		serviceName: "iam",
		resp: &substrate.AWSResponse{
			StatusCode: http.StatusOK,
			Body:       []byte(`<Response/>`),
		},
	}

	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	registry.Register(plug)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)

	consistencyCtrl, err := substrate.NewConsistencyController(substrate.ConsistencyConfig{
		Enabled:          true,
		PropagationDelay: 2 * time.Second,
		AffectedServices: []string{"iam"},
	}, tc)
	require.NoError(t, err)

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
		substrate.ServerOptions{Consistency: consistencyCtrl})

	// Send a mutating request (CreateUser) to record the write.
	sendCreate := func() *http.Response {
		r := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Action=CreateUser&UserName=alice&Version=2010-05-08"))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Result()
	}

	// Send a read request (GetUser) that should be rejected.
	sendGet := func() *http.Response {
		r := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Action=GetUser&UserName=alice&Version=2010-05-08"))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Result()
	}

	resp := sendCreate()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Immediate read should be rejected.
	resp = sendGet()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	// After advancing time past the delay, read should succeed.
	tc.SetTime(start.Add(3 * time.Second))
	resp = sendGet()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestConsistencyController_UpdateConfig(t *testing.T) {
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.ConsistencyConfig{
		Enabled:          true,
		PropagationDelay: 2 * time.Second,
		AffectedServices: []string{"iam"},
	}
	cc, err := substrate.NewConsistencyController(cfg, tc)
	require.NoError(t, err)

	// Disable consistency.
	newCfg := substrate.ConsistencyConfig{
		Enabled:          false,
		PropagationDelay: 0,
		AffectedServices: []string{},
	}
	cc.UpdateConfig(newCfg)
	// After disabling, CheckRead should be a no-op (no error).
	req := &substrate.AWSRequest{Service: "iam", Operation: "GetUser"}
	checkErr := cc.CheckRead(nil, req)
	if checkErr != nil {
		t.Errorf("unexpected error after UpdateConfig(disabled): %v", checkErr)
	}
}
