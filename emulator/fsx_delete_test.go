package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// FSx's DeleteFileSystem answers its published flat shape, a file system's CreationTime keeps its
// fraction, and ClientRequestToken makes both create and delete idempotent (#1210, #1373).
//
// The assertions are on raw bytes. A test decoding the delete into substrate's own struct could not
// see the {"FileSystem": …} wrapper the response used to carry, and one decoding CreationTime into a
// float could not tell 1700000000 from 1700000000.123.

// fsxDeleteClock is a sub-second instant, so a dropped fraction is visible.
var fsxDeleteClock = time.Unix(1700000000, 123000000).UTC()

// fsxHarness drives the FSx plugin directly, over a state manager a test may fault.
type fsxHarness struct {
	t     *testing.T
	p     *emulator.FSxPlugin
	ctx   *emulator.RequestContext
	state *cfFaultStateManager
}

func newFSxHarness(t *testing.T) *fsxHarness {
	t.Helper()
	tc := emulator.NewTimeController(fsxDeleteClock)
	tc.Freeze()
	tc.SetTime(fsxDeleteClock)
	h := &fsxHarness{t: t, p: &emulator.FSxPlugin{}, state: &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}}
	require.NoError(t, h.p.Initialize(t.Context(), emulator.PluginConfig{
		State:   h.state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	h.ctx = &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-fsx-delete", IDs: emulator.NewIDMint("req-fsx-delete"),
	}
	return h
}

// call issues one operation and returns the body, or the refusal's code, or the non-AWS error.
func (h *fsxHarness) call(op string, body map[string]any) ([]byte, string, error) {
	h.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(h.t, err, "marshal %s", op)
	resp, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "fsx", Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "AWSSimbaAPIService_v20180301." + op}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return nil, awsErr.Code, nil
	}
	if err != nil {
		return nil, "", err
	}
	return resp.Body, "", nil
}

// ok issues one operation and requires it to succeed.
func (h *fsxHarness) ok(op string, body map[string]any) []byte {
	h.t.Helper()
	out, code, err := h.call(op, body)
	require.NoError(h.t, err, "%s", op)
	require.Empty(h.t, code, "%s was refused", op)
	return out
}

// create makes a file system of the given type and returns its ID.
func (h *fsxHarness) create(fsType string, extra map[string]any) string {
	h.t.Helper()
	body := map[string]any{"FileSystemType": fsType, "StorageCapacity": 1200, "SubnetIds": []string{"subnet-0123456789abcdef0"}}
	maps.Copy(body, extra)
	var out struct {
		FileSystem struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystem"`
	}
	created := h.ok("CreateFileSystem", body)
	require.NoError(h.t, json.Unmarshal(created, &out), "decode CreateFileSystem: %s", created)
	return out.FileSystem.FileSystemID
}

// fsxTopKeys returns the sorted top-level members of a JSON object.
func fsxTopKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc), "decode: %s", body)
	return slices.Sorted(maps.Keys(doc))
}

func TestFSxDelete_AnswersThePublishedFlatShape(t *testing.T) {
	t.Parallel()
	tags := []map[string]string{{"Key": "final", "Value": "yes"}}
	for _, tc := range []struct {
		name  string
		fsTyp string
		extra map[string]any
		keys  []string
		has   string
	}{
		{"a Lustre file system, no configuration", "LUSTRE", nil, []string{"FileSystemId", "Lifecycle"}, ""},
		{"a Lustre file system with final-backup tags", "LUSTRE", map[string]any{"LustreConfiguration": map[string]any{"FinalBackupTags": tags}},
			[]string{"FileSystemId", "Lifecycle", "LustreResponse"}, `"LustreResponse":{"FinalBackupTags":[{"Key":"final","Value":"yes"}]}`},
		{"a Windows file system, which takes a final backup by default", "WINDOWS", nil,
			[]string{"FileSystemId", "Lifecycle", "WindowsResponse"}, `"WindowsResponse":{}`},
		{"an OpenZFS file system with final-backup tags", "OPENZFS", map[string]any{"OpenZFSConfiguration": map[string]any{"FinalBackupTags": tags}},
			[]string{"FileSystemId", "Lifecycle", "OpenZFSResponse"}, `"OpenZFSResponse":{"FinalBackupTags":[{"Key":"final","Value":"yes"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newFSxHarness(t)
			id := h.create(tc.fsTyp, nil)
			body := map[string]any{"FileSystemId": id}
			maps.Copy(body, tc.extra)
			deleted := h.ok("DeleteFileSystem", body)

			require.Equal(t, tc.keys, fsxTopKeys(t, deleted), "API_DeleteFileSystem's response is flat: %s", deleted)
			require.Contains(t, string(deleted), `"FileSystemId":"`+id+`"`, "%s", deleted)
			require.Contains(t, string(deleted), `"Lifecycle":"DELETING"`, "a successful delete reports DELETING: %s", deleted)
			require.NotContains(t, string(deleted), "DELETED", "DELETED is not a published lifecycle value: %s", deleted)
			if tc.has != "" {
				require.Contains(t, string(deleted), tc.has, "%s", deleted)
			}
		})
	}
}

// The page: on a deleted file system's ID, DescribeFileSystems "returns a FileSystemNotFound error".
func TestFSxDelete_ADeletedFileSystemIsNotFound(t *testing.T) {
	t.Parallel()
	h := newFSxHarness(t)
	id := h.create("LUSTRE", nil)
	h.ok("DeleteFileSystem", map[string]any{"FileSystemId": id})

	for _, op := range []string{"DescribeFileSystems", "DeleteFileSystem"} {
		body := map[string]any{"FileSystemId": id}
		if op == "DescribeFileSystems" {
			body = map[string]any{"FileSystemIds": []string{id}}
		}
		_, code, err := h.call(op, body)
		require.NoError(t, err, "%s", op)
		require.Equal(t, "FileSystemNotFound", code, "%s on a deleted file system", op)
	}
	listed := h.ok("DescribeFileSystems", map[string]any{})
	require.JSONEq(t, `{"FileSystems":[]}`, string(listed), "a deleted file system is not listed")
}

func TestFSx_CreationTimeKeepsItsFraction(t *testing.T) {
	t.Parallel()
	h := newFSxHarness(t)
	created := h.ok("CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE", "StorageCapacity": 1200})
	var out struct {
		FileSystem struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystem"`
	}
	require.NoError(t, json.Unmarshal(created, &out))
	described := h.ok("DescribeFileSystems", map[string]any{"FileSystemIds": []string{out.FileSystem.FileSystemID}})
	for op, body := range map[string][]byte{"CreateFileSystem": created, "DescribeFileSystems": described} {
		require.Containsf(t, string(body), `"CreationTime":1700000000.123`,
			"%s must answer CreationTime as epoch seconds with its fraction: %s", op, body)
	}
}

// A record written before #1373 holds whole seconds in the same field, and one written before #1210
// may be soft-deleted with DELETED. Both still read back.
func TestFSx_ARecordWrittenBeforeTheFixStillReadsBack(t *testing.T) {
	t.Parallel()
	h := newFSxHarness(t)
	put := func(id, lifecycle string) {
		t.Helper()
		rec := `{"file_system_id":"` + id + `","file_system_type":"LUSTRE","storage_capacity":1200,"storage_type":"SSD",` +
			`"vpc_id":"","subnet_ids":null,"dns_name":"","resource_arn":"","lifecycle":"` + lifecycle + `",` +
			`"creation_time":1700000000,"account_id":"123456789012","region":"us-east-1"}`
		require.NoError(t, h.state.Put(t.Context(), "fsx", "fs:123456789012/us-east-1/"+id, []byte(rec)))
	}
	put("fs-0000000000000001", "AVAILABLE")
	put("fs-0000000000000002", "DELETED")

	described := h.ok("DescribeFileSystems", map[string]any{"FileSystemIds": []string{"fs-0000000000000001"}})
	require.Contains(t, string(described), `"CreationTime":1700000000.000`, "a whole-second record renders three decimals: %s", described)
	_, code, err := h.call("DescribeFileSystems", map[string]any{"FileSystemIds": []string{"fs-0000000000000002"}})
	require.NoError(t, err)
	require.Equal(t, "FileSystemNotFound", code, "a record soft-deleted before #1210 is absent")
}

// API_CreateFileSystem: a repeated token with the same parameters "returns the description of the
// existing file system"; with different parameters, IncompatibleParameterError.
func TestFSx_CreateClientRequestTokenIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newFSxHarness(t)
	first := h.create("LUSTRE", map[string]any{"ClientRequestToken": "create-token-1"})
	second := h.create("LUSTRE", map[string]any{"ClientRequestToken": "create-token-1"})
	require.Equal(t, first, second, "a retried create must answer the file system the first created")

	var listed struct {
		FileSystems []json.RawMessage `json:"FileSystems"`
	}
	require.NoError(t, json.Unmarshal(h.ok("DescribeFileSystems", map[string]any{}), &listed))
	require.Len(t, listed.FileSystems, 1, "a doubled create with one token creates one file system")

	_, code, err := h.call("CreateFileSystem", map[string]any{
		"ClientRequestToken": "create-token-1", "FileSystemType": "LUSTRE", "StorageCapacity": 2400, "SubnetIds": []string{"subnet-0123456789abcdef0"},
	})
	require.NoError(t, err)
	require.Equal(t, "IncompatibleParameterError", code, "the token reused with a different StorageCapacity")

	h.ok("DeleteFileSystem", map[string]any{"FileSystemId": first})
	_, code, err = h.call("CreateFileSystem", map[string]any{
		"ClientRequestToken": "create-token-1", "FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": []string{"subnet-0123456789abcdef0"},
	})
	require.NoError(t, err)
	require.Equal(t, "FileSystemNotFound", code, "a retried create whose file system was deleted since")
}

// API_DeleteFileSystem: the token "ensure[s] idempotent deletion".
func TestFSx_DeleteClientRequestTokenIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newFSxHarness(t)
	id := h.create("WINDOWS", nil)
	body := map[string]any{"FileSystemId": id, "ClientRequestToken": "delete-token-1"}
	first := h.ok("DeleteFileSystem", body)
	second := h.ok("DeleteFileSystem", body)
	require.JSONEq(t, string(first), string(second), "a retried delete must answer what the first did")

	_, code, err := h.call("DeleteFileSystem", map[string]any{
		"FileSystemId": id, "ClientRequestToken": "delete-token-1", "WindowsConfiguration": map[string]any{"SkipFinalBackup": true},
	})
	require.NoError(t, err)
	require.Equal(t, "IncompatibleParameterError", code, "the token reused with a different configuration")
}

func TestFSx_AClientRequestTokenOutsideItsPatternIsRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		op    string
		token string
	}{
		{"create, 64 characters", "CreateFileSystem", strings.Repeat("a", 64)},
		{"create, a space", "CreateFileSystem", "has space"},
		{"delete, 64 characters", "DeleteFileSystem", strings.Repeat("b", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newFSxHarness(t)
			body := map[string]any{"ClientRequestToken": tc.token, "FileSystemType": "LUSTRE"}
			if tc.op == "DeleteFileSystem" {
				body = map[string]any{"ClientRequestToken": tc.token, "FileSystemId": h.create("LUSTRE", nil)}
			}
			_, code, err := h.call(tc.op, body)
			require.NoError(t, err)
			require.Equal(t, "BadRequest", code, "%s", tc.name)
		})
	}
}

// A store fault at any read or write the token and delete paths make is an error, never answered as
// a refusal or a success.
func TestFSx_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		// token is sent when set; prior issues one successful call with it first.
		token string
		prior bool
	}{
		{"create, token read", func(m *cfFaultStateManager) { m.failGet = "fs_token:" }, "CreateFileSystem", "t1", false},
		{"create, token corrupt", func(m *cfFaultStateManager) { m.corruptGet = "fs_token:" }, "CreateFileSystem", "t1", true},
		{"create, token write", func(m *cfFaultStateManager) { m.failPut = "fs_token:" }, "CreateFileSystem", "t1", false},
		{"create retry, record read", func(m *cfFaultStateManager) { m.failGet = "fs:" }, "CreateFileSystem", "t1", true},
		{"delete, record read", func(m *cfFaultStateManager) { m.failGet = "fs:" }, "DeleteFileSystem", "", false},
		{"delete, record corrupt", func(m *cfFaultStateManager) { m.corruptGet = "fs:" }, "DeleteFileSystem", "", false},
		{"delete, record delete", func(m *cfFaultStateManager) { m.failDelete = "fs:" }, "DeleteFileSystem", "", false},
		{"delete, token write", func(m *cfFaultStateManager) { m.failPut = "fs_token:" }, "DeleteFileSystem", "t2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newFSxHarness(t)
			body := map[string]any{"FileSystemType": "LUSTRE", "StorageCapacity": 1200}
			if tc.op == "DeleteFileSystem" {
				body = map[string]any{"FileSystemId": h.create("LUSTRE", nil)}
			}
			if tc.token != "" {
				body["ClientRequestToken"] = tc.token
			}
			if tc.prior {
				h.ok(tc.op, body)
			}
			tc.arm(h.state)
			_, code, err := h.call(tc.op, body)
			require.Errorf(t, err, "%s must fail on a store fault (answered code %q)", tc.name, code)
		})
	}
}
