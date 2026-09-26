package emulator_test

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

	"github.com/scttfrdmn/substrate/emulator"
)

// newFSxTestServer creates a test server with only the FSxPlugin registered.
func newFSxTestServer(t *testing.T) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Now())

	plugin := &emulator.FSxPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
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

// assertFSxError asserts the refusal's code and status together.
//
// Every FSx refusal below asserted only the status before #1063, which is how deleteFileSystem came to
// answer InvalidRequest — an Amazon S3 code that appears on no FSx page — for four releases without a
// test noticing. A status alone cannot tell BadRequest from InvalidRequest; both are 400.
func assertFSxError(t *testing.T, r *http.Response, wantCode string) {
	t.Helper()
	assert.Equal(t, http.StatusBadRequest, r.StatusCode)
	var errShape struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(readFSxBody(t, r), &errShape))
	code := errShape.Type
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}
	assert.Equal(t, wantCode, code, "message: %s", errShape.Message)
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

	// Describe all after delete — should return empty list (DELETED filtered out).
	resp = fsxRequest(t, ts, "DescribeFileSystems", "{}")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = readFSxBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &descResp))
	assert.Empty(t, descResp.FileSystems)

	// Describe by ID after delete — should return FileSystemNotFound (satisfies SDK delete waiter).
	descByIDBody2, err2 := json.Marshal(map[string]interface{}{
		"FileSystemIds": []string{fsID},
	})
	require.NoError(t, err2)
	resp = fsxRequest(t, ts, "DescribeFileSystems", string(descByIDBody2))
	assertFSxError(t, resp, "FileSystemNotFound")
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
	assertFSxError(t, resp, "FileSystemNotFound")
}

func TestFSx_DeleteNotFound(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	body, err := json.Marshal(map[string]string{"FileSystemId": "fs-nonexistent"})
	require.NoError(t, err)
	resp := fsxRequest(t, ts, "DeleteFileSystem", string(body))
	assertFSxError(t, resp, "FileSystemNotFound")
}

// TestFSx_DeleteRequiresFileSystemID asserts the refusal #1063 corrected.
//
// API_DeleteFileSystem marks FileSystemId Required: Yes and publishes five errors — BadRequest,
// FileSystemNotFound, IncompatibleParameterError, InternalServerError and ServiceLimitExceeded. The
// handler answered InvalidRequest, which is on none of them, and #1063 attributed the site to
// describeFileSystems, where FileSystemIds is Required: No and an absent list means "describe them all"
// — the behavior TestFSx_CreateDescribeDelete above already relies on. So the guard was in the right
// place and only the code was wrong.
func TestFSx_DeleteRequiresFileSystemID(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	assertFSxError(t, fsxRequest(t, ts, "DeleteFileSystem", "{}"), "BadRequest")
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

// TestFSx_LustreConfiguration verifies that CreateFileSystem for a LUSTRE file
// system includes LustreConfiguration.MountName and DeploymentType in responses
// so SDK consumers do not encounter nil-pointer panics (#233).
func TestFSx_LustreConfiguration(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Create without specifying DeploymentType — should default to SCRATCH_2.
	resp := fsxRequest(t, ts, "CreateFileSystem", `{
		"FileSystemType": "LUSTRE",
		"StorageCapacity": 1200,
		"SubnetIds": ["subnet-abc"]
	}`)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readFSxBody(t, resp)

	var out struct {
		FileSystem struct {
			FileSystemId        string `json:"FileSystemId"`
			LustreConfiguration struct {
				MountName      string `json:"MountName"`
				DeploymentType string `json:"DeploymentType"`
			} `json:"LustreConfiguration"`
		} `json:"FileSystem"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	assert.NotEmpty(t, out.FileSystem.LustreConfiguration.MountName, "MountName must be set")
	assert.Equal(t, "fsx", out.FileSystem.LustreConfiguration.MountName)
	assert.Equal(t, "SCRATCH_2", out.FileSystem.LustreConfiguration.DeploymentType)

	// Describe should also include LustreConfiguration.
	descBody, _ := json.Marshal(map[string]interface{}{
		"FileSystemIds": []string{out.FileSystem.FileSystemId},
	})
	resp = fsxRequest(t, ts, "DescribeFileSystems", string(descBody))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readFSxBody(t, resp)

	var descOut struct {
		FileSystems []struct {
			LustreConfiguration struct {
				MountName      string `json:"MountName"`
				DeploymentType string `json:"DeploymentType"`
			} `json:"LustreConfiguration"`
		} `json:"FileSystems"`
	}
	require.NoError(t, json.Unmarshal(body, &descOut))
	require.Len(t, descOut.FileSystems, 1)
	assert.Equal(t, "fsx", descOut.FileSystems[0].LustreConfiguration.MountName)
	assert.Equal(t, "SCRATCH_2", descOut.FileSystems[0].LustreConfiguration.DeploymentType)
}

// TestFSx_SDKTargetRouting verifies that requests using the real AWS SDK v2
// X-Amz-Target header (AWSSimbaAPIService_v20180301.<Op>) are routed to the
// FSx plugin correctly (#232).
func TestFSx_SDKTargetRouting(t *testing.T) {
	srv := newFSxTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Use the exact target the AWS SDK v2 FSx client sends.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewBufferString(`{
		"FileSystemType": "LUSTRE",
		"StorageCapacity": 1200,
		"SubnetIds": ["subnet-abc"]
	}`))
	require.NoError(t, err)
	req.Host = "fsx.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSSimbaAPIService_v20180301.CreateFileSystem")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/fsx/aws4_request, SignedHeaders=host, Signature=fake")

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "SDK-style target should route to FSx plugin")

	body := readFSxBody(t, resp)
	var out struct {
		FileSystem struct {
			FileSystemId string `json:"FileSystemId"`
		} `json:"FileSystem"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	assert.True(t, strings.HasPrefix(out.FileSystem.FileSystemId, "fs-"))
}

// TestFSx_LustreMountNameIsMintedForANonScratchDeployment covers the other half of the
// MountName branch: SCRATCH_2 always reports "fsx", and every other Lustre deployment type
// reports a minted value instead.
//
// Since #856 that value derives from the request's own ID rather than from crypto/rand, so a
// replayed CreateFileSystem reports the mount name its recording reported. The assertion is on
// the shape and on its difference from the SCRATCH_2 constant; the derivation itself is
// asserted end-to-end in ids_test.go.
func TestFSx_LustreMountNameIsMintedForANonScratchDeployment(t *testing.T) {
	ts := httptest.NewServer(newFSxTestServer(t))
	t.Cleanup(ts.Close)

	mountNameFor := func(deploymentType string) string {
		resp := fsxRequest(t, ts, "CreateFileSystem", `{
			"FileSystemType": "LUSTRE",
			"StorageCapacity": 1200,
			"SubnetIds": ["subnet-12345678"],
			"LustreConfiguration": {"DeploymentType": "`+deploymentType+`"}
		}`)
		body := readFSxBody(t, resp)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
		var created struct {
			FileSystem struct {
				LustreConfiguration struct {
					MountName string `json:"MountName"`
				} `json:"LustreConfiguration"`
			} `json:"FileSystem"`
		}
		require.NoError(t, json.Unmarshal(body, &created))
		return created.FileSystem.LustreConfiguration.MountName
	}

	assert.Equal(t, "fsx", mountNameFor("SCRATCH_2"),
		"SCRATCH_2's mount name is the documented constant, not a minted value")
	assert.Regexp(t, `^[0-9a-f]{16}$`, mountNameFor("PERSISTENT_1"),
		"a persistent deployment reports a minted mount name")
}
