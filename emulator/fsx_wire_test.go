package emulator_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestFSxWire_FileSystemResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for FSxFileSystem (#756).
//
// FSxFileSystem declares its account as `account_id` and its Region as `region`, neither under
// omitempty, and neither reaches a body: every file-system response goes through fsxToWire. The
// snake_case spelling is listed in its own right because a fold does not reach it. All three routed
// operations are driven.
func TestFSxWire_FileSystemResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.FSxPlugin{}
	ctx, state := wireSetup(t, p, "req-fsx-wire")
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "fsx", "AmazonFSx", op, body)
	}

	created := call("CreateFileSystem", map[string]any{
		"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "StorageType": "SSD", "SubnetIds": []string{"subnet-abc123"},
	})
	var out struct {
		FileSystem struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystem"`
	}
	require.NoError(t, json.Unmarshal(created, &out), "decode CreateFileSystem: %s", created)
	id := out.FileSystem.FileSystemID
	require.NotEmpty(t, id, "CreateFileSystem must report an id")
	wireRequireHeld(t, state, "fsx", "fs:123456789012/us-east-1/"+id, "account_id", "region")

	wireRunJSON(t, []string{"AccountID", "account_id", "Region"}, []wireCase{
		{op: "CreateFileSystem", held: created, anchor: id},
		{op: "DescribeFileSystems", call: func() []byte { return call("DescribeFileSystems", map[string]any{"FileSystemIds": []string{id}}) }, anchor: id},
		{op: "DeleteFileSystem", call: func() []byte { return call("DeleteFileSystem", map[string]any{"FileSystemId": id}) }, anchor: id},
	})
}
