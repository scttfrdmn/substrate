package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// FSx's share of the batch audits: DescribeFileSystems' pagination (#1195) and CreateFileSystem's
// Required: Yes members (#1197). The assertions are on raw bytes, because a typed decode of a
// response cannot tell a NextToken that is absent from one that is the empty string.

// fsxSubnet is a subnet ID in the published `^(subnet-[0-9a-f]{8,})$` shape.
var fsxSubnet = []string{"subnet-0123456789abcdef0"}

// fsxDescribe issues DescribeFileSystems and returns the raw document's FileSystems IDs and its
// NextToken member, reporting whether the member was present at all.
func fsxDescribe(t *testing.T, h *fsxHarness, body map[string]any) (ids []string, token string, present bool) {
	t.Helper()
	raw := h.ok("DescribeFileSystems", body)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc), "decode DescribeFileSystems: %s", raw)
	var list []struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(doc["FileSystems"], &list), "decode FileSystems: %s", raw)
	for _, fs := range list {
		ids = append(ids, fs.FileSystemID)
	}
	if rawToken, ok := doc["NextToken"]; ok {
		require.NoError(t, json.Unmarshal(rawToken, &token), "NextToken must be a string: %s", raw)
		return ids, token, true
	}
	return ids, "", false
}

func TestFSxAudit_DescribeFileSystemsPagesAndOmitsTheFinalToken(t *testing.T) {
	t.Parallel()
	h := newFSxHarness(t)
	created := make([]string, 0, 5)
	for range 5 {
		created = append(created, h.create("LUSTRE", nil))
	}

	// Two per page: 2, 2, then 1 with no token. A walk that follows the token sees every file system
	// exactly once, in creation order.
	var seen []string
	body := map[string]any{"MaxResults": 2}
	pages := 0
	for {
		ids, token, present := fsxDescribe(t, h, body)
		pages++
		seen = append(seen, ids...)
		if !present {
			break
		}
		require.NotEmpty(t, token, "a NextToken that is present must not be empty: the page publishes Minimum length of 1")
		require.LessOrEqual(t, len(ids), 2, "MaxResults bounds the page")
		body = map[string]any{"MaxResults": 2, "NextToken": token}
		require.Less(t, pages, 10, "the walk must terminate")
	}
	require.Equal(t, 3, pages, "five file systems at two per page is three pages")
	// The page calls the order "unspecified"; substrate's is the account index, sorted by ID, which is
	// what makes an offset cursor stable. What matters is every file system exactly once.
	slices.Sort(created)
	require.Equal(t, created, seen, "the walk answers every file system once, in the index's stable order")

	// With no MaxResults the page size is the published 50-item maximum, so five fit in one page and
	// the response carries no NextToken member — not an empty one.
	ids, _, present := fsxDescribe(t, h, map[string]any{})
	require.Len(t, ids, 5)
	require.False(t, present, "the last page omits NextToken rather than answering it empty")
}

func TestFSxAudit_DescribeFileSystemsRefusesWhatThePageRefuses(t *testing.T) {
	t.Parallel()
	tooMany := make([]string, 51)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("fs-%016x", i)
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"MaxResults of zero", map[string]any{"MaxResults": 0}},
		{"a negative MaxResults", map[string]any{"MaxResults": -1}},
		{"a token that is not base64", map[string]any{"NextToken": "not a token"}},
		{"a base64 token substrate never issued", map[string]any{"NextToken": base64.StdEncoding.EncodeToString([]byte("page-two"))}},
		{"a token from a padded offset", map[string]any{"NextToken": base64.StdEncoding.EncodeToString([]byte("05"))}},
		{"fifty-one FileSystemIds", map[string]any{"FileSystemIds": tooMany}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newFSxHarness(t)
			h.create("LUSTRE", nil)
			_, code, err := h.call("DescribeFileSystems", tc.body)
			require.NoError(t, err)
			require.Equal(t, "BadRequest", code, "%s is refused with the page's client-error code", tc.name)
		})
	}
}

func TestFSxAudit_CreateFileSystemRefusesAnAbsentRequiredMember(t *testing.T) {
	t.Parallel()
	manySubnets := make([]string, 51)
	for i := range manySubnets {
		manySubnets[i] = fmt.Sprintf("subnet-%016x", i)
	}
	for _, tc := range []struct {
		name    string
		body    map[string]any
		message string
	}{
		{"no FileSystemType", map[string]any{"SubnetIds": fsxSubnet}, "FileSystemType is required"},
		{"a FileSystemType outside the enum", map[string]any{"FileSystemType": "NFS", "SubnetIds": fsxSubnet}, "is not one of"},
		{"a lower-case FileSystemType", map[string]any{"FileSystemType": "lustre", "SubnetIds": fsxSubnet}, "is not one of"},
		{"a StorageType outside the enum", map[string]any{"FileSystemType": "LUSTRE", "StorageType": "NVME", "SubnetIds": fsxSubnet}, "StorageType"},
		{"no SubnetIds", map[string]any{"FileSystemType": "LUSTRE"}, "SubnetIds is required"},
		{"an empty SubnetIds", map[string]any{"FileSystemType": "LUSTRE", "SubnetIds": []string{}}, "SubnetIds is required"},
		{"fifty-one SubnetIds", map[string]any{"FileSystemType": "LUSTRE", "SubnetIds": manySubnets}, "at most 50"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newFSxHarness(t)
			resp, err := h.p.HandleRequest(h.ctx, fsxPluginRequest(t, "CreateFileSystem", tc.body))
			require.Nil(t, resp, "%s must not create a file system", tc.name)
			awsErr := requireFSxRefusal(t, err)
			require.Equal(t, "BadRequest", awsErr.Code)
			require.Equal(t, 400, awsErr.HTTPStatus)
			require.Contains(t, awsErr.Message, tc.message)

			ids, _, _ := fsxDescribe(t, h, map[string]any{})
			require.Empty(t, ids, "a refused create stores nothing")
		})
	}

	// The members that satisfy the page still create, so a fixture that sends them is unaffected.
	t.Run("every required member present", func(t *testing.T) {
		t.Parallel()
		h := newFSxHarness(t)
		out := h.ok("CreateFileSystem", map[string]any{"FileSystemType": "OPENZFS", "StorageType": "INTELLIGENT_TIERING", "SubnetIds": fsxSubnet})
		require.Contains(t, string(out), `"FileSystemType":"OPENZFS"`)
	})
}

// A store fault on the paths #1195 and #1197 added is an error, never a shorter listing or a file
// system created with no VPC.
func TestFSxAudit_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body map[string]any
	}{
		{"describe, record read", func(m *cfFaultStateManager) { m.failGet = "fs:" }, "DescribeFileSystems", map[string]any{}},
		{"describe, record corrupt", func(m *cfFaultStateManager) { m.corruptGet = "fs:" }, "DescribeFileSystems", map[string]any{}},
		{"create, subnet lookup", func(m *cfFaultStateManager) { m.failGet = "subnet:" }, "CreateFileSystem",
			map[string]any{"FileSystemType": "LUSTRE", "SubnetIds": fsxSubnet}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newFSxHarness(t)
			h.create("LUSTRE", nil)
			tc.arm(h.state)
			_, code, err := h.call(tc.op, tc.body)
			require.Errorf(t, err, "%s must fail on a store fault (answered code %q)", tc.name, code)
			require.False(t, strings.Contains(err.Error(), "BadRequest"), "a store fault is not a published refusal: %v", err)
		})
	}
}

// fsxPluginRequest builds one FSx JSON-target request.
func fsxPluginRequest(t *testing.T, op string, body map[string]any) *emulator.AWSRequest {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	return &emulator.AWSRequest{
		Service: "fsx", Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "AWSSimbaAPIService_v20180301." + op}, Params: map[string]string{},
	}
}

// requireFSxRefusal requires err to be a published refusal and returns it, so a test can assert its
// code, status and message together.
func requireFSxRefusal(t *testing.T, err error) *emulator.AWSError {
	t.Helper()
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "want a published refusal, got %v", err)
	return awsErr
}
