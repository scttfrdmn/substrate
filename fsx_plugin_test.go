package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
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

// newFSxTestServer creates a test server with only the FSxPlugin registered.
func newFSxTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	plugin := &substrate.FSxPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// fsxRequest sends an FSx JSON-protocol request.
func fsxRequest(t *testing.T, ts *httptest.Server, operation, body string) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != "" {
		reqBody = bytes.NewBufferString(body)
	} else {
		reqBody = bytes.NewBufferString("{}")
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", reqBody)
	require.NoError(t, err)
	req.Host = "fsx.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonFSx."+operation)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/fsx/aws4_request, SignedHeaders=host, Signature=fake")

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

// readFSxBody reads the full response body.
func readFSxBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return body
}

func TestFSx_CreateDescribeDelete(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Create a LUSTRE file system.
	createBody := `{
		"FileSystemType": "LUSTRE",
		"StorageCapacity": 1200,
		"StorageType": "SSD",
		"SubnetIds": ["subnet-abc123"]
	}`
	resp := fsxRequest(t, ts, "CreateFileSystem", createBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readFSxBody(t, resp)

	var createResp struct {
		FileSystem struct {
			FileSystemId    string  `json:"FileSystemId"`
			FileSystemType  string  `json:"FileSystemType"`
			StorageCapacity int32   `json:"StorageCapacity"`
			StorageType     string  `json:"StorageType"`
			Lifecycle       string  `json:"Lifecycle"`
			DNSName         string  `json:"DNSName"`
			ResourceARN     string  `json:"ResourceARN"`
			CreationTime    float64 `json:"CreationTime"`
		} `json:"FileSystem"`
	}
	require.NoError(t, json.Unmarshal(body, &createResp))
	fsID := createResp.FileSystem.FileSystemId
	assert.True(t, strings.HasPrefix(fsID, "fs-"), "expected fs- prefix, got %s", fsID)
	assert.Equal(t, "LUSTRE", createResp.FileSystem.FileSystemType)
	assert.Equal(t, int32(1200), createResp.FileSystem.StorageCapacity)
	assert.Equal(t, "SSD", createResp.FileSystem.StorageType)
	assert.Equal(t, "AVAILABLE", createResp.FileSystem.Lifecycle)
	assert.Contains(t, createResp.FileSystem.DNSName, fsID)
	assert.Contains(t, createResp.FileSystem.ResourceARN, fsID)
	assert.Greater(t, createResp.FileSystem.CreationTime, float64(0))

	// Describe — list all.
	resp = fsxRequest(t, ts, "DescribeFileSystems", "{}")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = readFSxBody(t, resp)
	var descResp struct {
		FileSystems []struct {
			FileSystemId string `json:"FileSystemId"`
		} `json:"FileSystems"`
	}
	require.NoError(t, json.Unmarshal(body, &descResp))
	require.Len(t, descResp.FileSystems, 1)
	assert.Equal(t, fsID, descResp.FileSystems[0].FileSystemId)

	// Describe by ID.
	descByIDBody, err := json.Marshal(map[string]interface{}{
		"FileSystemIds": []string{fsID},
	})
	require.NoError(t, err)
	resp = fsxRequest(t, ts, "DescribeFileSystems", string(descByIDBody))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = readFSxBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &descResp))
	require.Len(t, descResp.FileSystems, 1)

	// Delete.
	deleteBody, err := json.Marshal(map[string]string{"FileSystemId": fsID})
	require.NoError(t, err)
	resp = fsxRequest(t, ts, "DeleteFileSystem", string(deleteBody))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = readFSxBody(t, resp)
	var delResp struct {
		FileSystem struct {
			Lifecycle string `json:"Lifecycle"`
		} `json:"FileSystem"`
	}
	require.NoError(t, json.Unmarshal(body, &delResp))
	assert.Equal(t, "DELETED", delResp.FileSystem.Lifecycle)

	// Describe after delete — should return empty list.
	resp = fsxRequest(t, ts, "DescribeFileSystems", "{}")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = readFSxBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &descResp))
	assert.Empty(t, descResp.FileSystems)
}

func TestFSx_DescribeNotFound(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	body, err := json.Marshal(map[string]interface{}{
		"FileSystemIds": []string{"fs-nonexistent"},
	})
	require.NoError(t, err)
	resp := fsxRequest(t, ts, "DescribeFileSystems", string(body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestFSx_DeleteNotFound(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	body, err := json.Marshal(map[string]string{"FileSystemId": "fs-nonexistent"})
	require.NoError(t, err)
	resp := fsxRequest(t, ts, "DeleteFileSystem", string(body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestFSx_MultipleFileSystems(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	types := []string{"LUSTRE", "WINDOWS", "ONTAP"}

	for _, fsType := range types {
		createBody, err := json.Marshal(map[string]interface{}{
			"FileSystemType":  fsType,
			"StorageCapacity": 1200,
			"SubnetIds":       []string{"subnet-test"},
		})
		require.NoError(t, err)
		resp := fsxRequest(t, ts, "CreateFileSystem", string(createBody))
		require.Equal(t, http.StatusOK, resp.StatusCode, "create %s", fsType)
	}

	// All three should be listed.
	resp := fsxRequest(t, ts, "DescribeFileSystems", "{}")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readFSxBody(t, resp)
	var descResp struct {
		FileSystems []struct {
			FileSystemId string `json:"FileSystemId"`
		} `json:"FileSystems"`
	}
	require.NoError(t, json.Unmarshal(body, &descResp))
	assert.Len(t, descResp.FileSystems, 3)
}
