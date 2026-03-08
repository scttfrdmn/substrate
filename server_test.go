package substrate_test

import (
	"context"
	"encoding/xml"
	"io"
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

// serverPlugin is a configurable [substrate.Plugin] used only by server tests.
type serverPlugin struct {
	serviceName string
	resp        *substrate.AWSResponse
	err         error
}

func (p *serverPlugin) Name() string { return p.serviceName }

func (p *serverPlugin) Initialize(_ context.Context, _ substrate.PluginConfig) error { return nil }

func (p *serverPlugin) HandleRequest(_ *substrate.RequestContext, _ *substrate.AWSRequest) (*substrate.AWSResponse, error) {
	return p.resp, p.err
}

func (p *serverPlugin) Shutdown(_ context.Context) error { return nil }

// newTestServer builds a Server with an in-memory EventStore and optional plugins.
func newTestServer(t *testing.T, plugins ...substrate.Plugin) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	for _, plug := range plugins {
		registry.Register(plug)
	}
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

func TestServer_UnknownService_Returns501(t *testing.T) {
	srv := newTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	r.Header.Set("X-Amz-Target", "AmazonUnknown.DoSomething")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "ServiceNotAvailable")
}

func TestServer_RegisteredPlugin_ReturnsPluginResponse(t *testing.T) {
	plug := &serverPlugin{
		serviceName: "dynamodb",
		resp: &substrate.AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
			Body:       []byte(`{"Item":{}}`),
		},
	}
	srv := newTestServer(t, plug)

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"TableName":"test"}`))
	r.Header.Set("X-Amz-Target", "AmazonDynamoDB.GetItem")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.JSONEq(t, `{"Item":{}}`, string(body))
}

func TestServer_PluginError_ReturnsAWSError(t *testing.T) {
	plug := &serverPlugin{
		serviceName: "s3",
		err: &substrate.AWSError{
			Code:       "NoSuchBucket",
			Message:    "the specified bucket does not exist",
			HTTPStatus: http.StatusNotFound,
		},
	}
	srv := newTestServer(t, plug)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Host = "s3.us-east-1.amazonaws.com"
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &errResp))
	assert.Equal(t, "NoSuchBucket", errResp.Error.Code)
}

func TestServer_EventsAreRecorded(t *testing.T) {
	cfg := substrate.DefaultConfig()
	cfg.EventStore.Enabled = true
	registry := substrate.NewPluginRegistry()
	plug := &serverPlugin{
		serviceName: "sqs",
		resp:        &substrate.AWSResponse{StatusCode: http.StatusOK, Body: []byte(`{}`)},
	}
	registry.Register(plug)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	r.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	events, err := store.GetStream(context.Background(), "default")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "sqs", events[0].Service)
	assert.Equal(t, "SendMessage", events[0].Operation)
}

func TestServer_ResponseHeaders(t *testing.T) {
	plug := &serverPlugin{
		serviceName: "iam",
		resp: &substrate.AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"X-Custom-Header": "value"},
			Body:       []byte(`<Response/>`),
		},
	}
	srv := newTestServer(t, plug)

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Host = "iam.amazonaws.com"
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)
	assert.Equal(t, "value", w.Header().Get("X-Custom-Header"))
}
