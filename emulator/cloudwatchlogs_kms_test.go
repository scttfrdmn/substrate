package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cwLogsGroupKey reads back the kmsKeyId DescribeLogGroups reports for name, "" when the group
// reports none.
func cwLogsGroupKey(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{"logGroupNamePrefix": name})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		LogGroups []map[string]any `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &out))
	require.Len(t, out.LogGroups, 1)
	key, present := out.LogGroups[0]["kmsKeyId"]
	if !present {
		return ""
	}
	s, ok := key.(string)
	require.True(t, ok, "kmsKeyId is a string")
	return s
}

// TestCWLogs_LogGroupKmsKey covers a log group's KMS key (#1519): CreateLogGroup records it,
// DescribeLogGroups reports it, and AssociateKmsKey and DisassociateKmsKey change it.
func TestCWLogs_LogGroupKmsKey(t *testing.T) {
	t.Parallel()
	srv := newCWLogsTestServer(t)
	const keyA = "arn:aws:kms:us-east-1:123456789012:key/aaaaaaaa-0000-0000-0000-000000000001"
	const keyB = "arn:aws:kms:us-east-1:123456789012:key/bbbbbbbb-0000-0000-0000-000000000002"

	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": "/kms/created", "kmsKeyId": keyA})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": "/plain/group"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, keyA, cwLogsGroupKey(t, srv, "/kms/created"), "CreateLogGroup's key is reported")
	assert.Empty(t, cwLogsGroupKey(t, srv, "/plain/group"), "a group with no key reports no member")

	steps := []struct {
		name string
		op   string
		body map[string]any
		want string
	}{
		{"AssociateKmsKey replaces the key", "AssociateKmsKey", map[string]any{"logGroupName": "/kms/created", "kmsKeyId": keyB}, keyB},
		{"DisassociateKmsKey clears it", "DisassociateKmsKey", map[string]any{"logGroupName": "/kms/created"}, ""},
		{"AssociateKmsKey sets one on a group that had none", "AssociateKmsKey", map[string]any{"logGroupName": "/kms/created", "kmsKeyId": keyA}, keyA},
	}
	for _, s := range steps {
		resp := cwLogsRequest(t, srv, s.op, s.body)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", s.name, cwLogsReadBody(t, resp))
		assert.Equal(t, s.want, cwLogsGroupKey(t, srv, "/kms/created"), s.name)
	}

	refusals := []struct {
		name   string
		op     string
		body   map[string]any
		status int
		code   string
	}{
		{"associate with an absent group", "AssociateKmsKey", map[string]any{"logGroupName": "/absent", "kmsKeyId": keyA},
			http.StatusBadRequest, "ResourceNotFoundException"},
		{"disassociate from an absent group", "DisassociateKmsKey", map[string]any{"logGroupName": "/absent"},
			http.StatusBadRequest, "ResourceNotFoundException"},
		{"associate without a key", "AssociateKmsKey", map[string]any{"logGroupName": "/kms/created"},
			http.StatusBadRequest, "InvalidParameterException"},
		{"associate without a group", "AssociateKmsKey", map[string]any{"kmsKeyId": keyA},
			http.StatusBadRequest, "InvalidParameterException"},
		{"disassociate without a group", "DisassociateKmsKey", map[string]any{},
			http.StatusBadRequest, "InvalidParameterException"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, tc.op, tc.body)
			body := cwLogsReadBody(t, resp)
			assert.Equal(t, tc.status, resp.StatusCode, "%s", body)
			assert.Contains(t, string(body), tc.code)
		})
	}
}
