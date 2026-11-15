package emulator_test

// RAM reads resourceOwner and the published filters, a disassociation reports DISASSOCIATED, and a
// delete leaves the share DELETED for the documented window (#1171). Every assertion is over the wire.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ramOwnershipCall posts one RAM REST operation and returns the status and raw body.
func ramOwnershipCall(t *testing.T, ts *emulator.TestServer, path string, body any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = "ram.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, out
}

// ramOwnershipErrorCode reads a refusal's code from the header or the body.
func ramOwnershipErrorCode(t *testing.T, ts *emulator.TestServer, path string, body any) (int, string) {
	t.Helper()
	status, raw := ramOwnershipCall(t, ts, path, body)
	require.NotEqual(t, http.StatusOK, status, "%s: %s", path, raw)
	return status, cicdCode(t, raw)
}

// ramOwnershipShare creates a share and returns its ARN.
func ramOwnershipShare(t *testing.T, ts *emulator.TestServer, name string, principals, resources []string) string {
	t.Helper()
	status, raw := ramOwnershipCall(t, ts, "/createresourceshare", map[string]any{
		"name": name, "principals": principals, "resourceArns": resources,
	})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var out struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out.ResourceShare.ResourceShareArn
}

type ramOwnershipShares struct {
	ResourceShares []struct {
		ResourceShareArn string `json:"resourceShareArn"`
		Status           string `json:"status"`
	} `json:"resourceShares"`
}

func ramOwnershipGet(t *testing.T, ts *emulator.TestServer, body map[string]any) ramOwnershipShares {
	t.Helper()
	status, raw := ramOwnershipCall(t, ts, "/getresourceshares", body)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var out ramOwnershipShares
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func TestRAMOwnership_ResourceOwnerIsRequiredAndRead(t *testing.T) {
	ts := emulator.StartTestServer(t)
	ramOwnershipShare(t, ts, "owned", []string{"111111111111"}, []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"})

	for _, path := range []string{"/getresourceshares", "/listprincipals", "/listresources"} {
		t.Run(path, func(t *testing.T) {
			status, code := ramOwnershipErrorCode(t, ts, path, map[string]any{})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationError", code, "an absent required member")

			status, code = ramOwnershipErrorCode(t, ts, path, map[string]any{"resourceOwner": "EVERYONE"})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterException", code, "a value outside SELF | OTHER-ACCOUNTS")

			status, self := ramOwnershipCall(t, ts, path, map[string]any{"resourceOwner": "SELF"})
			require.Equal(t, http.StatusOK, status, "%s", self)
			status, other := ramOwnershipCall(t, ts, path, map[string]any{"resourceOwner": "OTHER-ACCOUNTS"})
			require.Equal(t, http.StatusOK, status, "%s", other)
			var selfDoc, otherDoc map[string][]any
			require.NoError(t, json.Unmarshal(self, &selfDoc))
			require.NoError(t, json.Unmarshal(other, &otherDoc))
			for member, items := range selfDoc {
				assert.Len(t, items, 1, "SELF lists the caller's own %s", member)
				assert.Empty(t, otherDoc[member], "nothing is shared across accounts, so OTHER-ACCOUNTS is empty")
			}
		})
	}
}

func TestRAMOwnership_ListFiltersApply(t *testing.T) {
	ts := emulator.StartTestServer(t)
	subnet := "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"
	license := "arn:aws:license-manager:us-east-1:123456789012:license-configuration:lic-1"
	a := ramOwnershipShare(t, ts, "a", []string{"111111111111"}, []string{subnet})
	ramOwnershipShare(t, ts, "b", []string{"222222222222"}, []string{license})

	ids := func(path, member, field string, body map[string]any) []string {
		body["resourceOwner"] = "SELF"
		status, raw := ramOwnershipCall(t, ts, path, body)
		require.Equal(t, http.StatusOK, status, "%s", raw)
		var doc map[string][]map[string]any
		require.NoError(t, json.Unmarshal(raw, &doc))
		var out []string
		for _, item := range doc[member] {
			out = append(out, item[field].(string))
			_, hasStatus := item["status"]
			if member == "principals" {
				assert.False(t, hasStatus, "API_Principal publishes no status")
			}
		}
		return out
	}

	assert.Equal(t, []string{"111111111111"}, ids("/listprincipals", "principals", "id", map[string]any{"resourceShareArns": []string{a}}))
	assert.Equal(t, []string{"222222222222"}, ids("/listprincipals", "principals", "id", map[string]any{"principals": []string{"222222222222"}}))
	assert.Equal(t, []string{"111111111111"}, ids("/listprincipals", "principals", "id", map[string]any{"resourceArn": subnet}))
	assert.Equal(t, []string{"111111111111"}, ids("/listprincipals", "principals", "id", map[string]any{"resourceType": "EC2:Subnet"}))

	assert.Equal(t, []string{subnet}, ids("/listresources", "resources", "arn", map[string]any{"resourceShareArns": []string{a}}))
	assert.Equal(t, []string{license}, ids("/listresources", "resources", "arn", map[string]any{"principal": "222222222222"}))
	assert.Equal(t, []string{license}, ids("/listresources", "resources", "arn", map[string]any{"resourceArns": []string{license}}))
	assert.Equal(t, []string{subnet}, ids("/listresources", "resources", "arn", map[string]any{"resourceType": "ec2:subnet"}))
	assert.Len(t, ids("/listresources", "resources", "arn", map[string]any{"resourceRegionScope": "REGIONAL"}), 2)
	assert.Empty(t, ids("/listresources", "resources", "arn", map[string]any{"resourceRegionScope": "GLOBAL"}))

	status, code := ramOwnershipErrorCode(t, ts, "/listresources", map[string]any{"resourceOwner": "SELF", "resourceRegionScope": "EVERYWHERE"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", code)
	status, code = ramOwnershipErrorCode(t, ts, "/getresourceshares", map[string]any{"resourceOwner": "SELF", "resourceShareStatus": "GONE"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", code)
}

func TestRAMOwnership_DisassociationReportsDisassociated(t *testing.T) {
	ts := emulator.StartTestServer(t)
	arn := ramOwnershipShare(t, ts, "d", []string{"111111111111"}, []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"})
	status, raw := ramOwnershipCall(t, ts, "/disassociateresourceshare", map[string]any{
		"resourceShareArn": arn, "principals": []string{"111111111111"},
	})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var out struct {
		Associations []struct {
			Status string `json:"status"`
		} `json:"resourceShareAssociations"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Len(t, out.Associations, 1)
	assert.Equal(t, "DISASSOCIATED", out.Associations[0].Status)
}

func TestRAMOwnership_DeleteIsSoftForTheDocumentedWindow(t *testing.T) {
	ts := emulator.StartTestServer(t)
	arn := ramOwnershipShare(t, ts, "gone", []string{"111111111111"}, []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"})
	status, raw := ramOwnershipCall(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": arn})
	require.Equal(t, http.StatusOK, status, "%s", raw)

	all := ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF"})
	require.Len(t, all.ResourceShares, 1, "a deleted share stays readable")
	assert.Equal(t, "DELETED", all.ResourceShares[0].Status)
	assert.Len(t, ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF", "resourceShareStatus": "DELETED"}).ResourceShares, 1)
	assert.Empty(t, ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF", "resourceShareStatus": "ACTIVE"}).ResourceShares)

	// It shares nothing.
	for _, path := range []string{"/listprincipals", "/listresources"} {
		status, raw := ramOwnershipCall(t, ts, path, map[string]any{"resourceOwner": "SELF"})
		require.Equal(t, http.StatusOK, status)
		var doc map[string][]any
		require.NoError(t, json.Unmarshal(raw, &doc))
		for member, items := range doc {
			assert.Empty(t, items, "%s of a deleted share", member)
		}
	}

	// Associate, disassociate and a second delete are invalid state transitions; update is not permitted.
	for _, tc := range []struct{ path, code string }{
		{"/associateresourceshare", "InvalidStateTransitionException"},
		{"/disassociateresourceshare", "InvalidStateTransitionException"},
		{"/deleteresourceshare", "InvalidStateTransitionException"},
		{"/updateresourceshare", "OperationNotPermittedException"},
	} {
		status, code := ramOwnershipErrorCode(t, ts, tc.path, map[string]any{
			"resourceShareArn": arn, "principals": []string{"333333333333"}, "name": "renamed",
		})
		assert.Equal(t, http.StatusBadRequest, status, tc.path)
		assert.Equal(t, tc.code, code, tc.path)
	}

	// After the User Guide's two hours the share is gone: neither listed nor addressable.
	ts.AdvanceTime(2*time.Hour + time.Second)
	assert.Empty(t, ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF"}).ResourceShares)
	status, code := ramOwnershipErrorCode(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": arn})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UnknownResourceException", code)
}
