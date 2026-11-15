package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CreatePolicyVersion, DeletePolicyVersion and SetDefaultPolicyVersion (#1560).

const iamVersionedPolicyARN = "arn:aws:iam::123456789012:policy/versioned"

// iamDocAllowing is a one-statement policy document allowing action.
func iamDocAllowing(action string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"` + action + `","Resource":"*"}]}`
}

// iamVersionedPolicyServer creates the policy every test here works on, at v1 allowing s3:GetObject.
func iamVersionedPolicyServer(t *testing.T) *emulator.Server {
	t.Helper()
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
		"PolicyName": "versioned", "PolicyDocument": iamDocAllowing("s3:GetObject"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	return srv
}

// iamCreateVersion adds a version and returns the decoded PolicyVersion.
func iamCreateVersion(t *testing.T, srv *emulator.Server, action string, setDefault bool) map[string]any {
	t.Helper()
	resp := iamRequest(t, srv, "CreatePolicyVersion", map[string]any{
		"PolicyArn": iamVersionedPolicyARN, "PolicyDocument": iamDocAllowing(action), "SetAsDefault": setDefault,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	version, ok := out["PolicyVersion"].(map[string]any)
	require.True(t, ok, "CreatePolicyVersion must return a PolicyVersion: %v", out)
	return version
}

// iamListedVersionIDs lists the policy's versions, in the order ListPolicyVersions answers.
func iamListedVersionIDs(t *testing.T, srv *emulator.Server) (ids []string, defaultID string) {
	t.Helper()
	got := iamDecodePolicyVersion(t, iamRequest(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": iamVersionedPolicyARN,
	}))
	for _, v := range got.Versions {
		ids = append(ids, v.VersionID)
		if v.IsDefaultVersion {
			defaultID = v.VersionID
		}
		assert.Nil(t, v.Document, "ListPolicyVersions must not carry a document")
	}
	return ids, defaultID
}

// iamExpectError posts a request and returns its error code, requiring the status.
func iamExpectError(t *testing.T, srv *emulator.Server, op string, body map[string]any, status int) string {
	t.Helper()
	resp := iamRequest(t, srv, op, body)
	require.Equal(t, status, resp.StatusCode)
	var out map[string]any
	decodeIAMXML(t, resp, &out)
	code, _ := out["__type"].(string)
	return code
}

func TestCreatePolicyVersion_AddsAndServesEveryVersion(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)

	v2 := iamCreateVersion(t, srv, "s3:PutObject", false)
	assert.Equal(t, "v2", v2["VersionId"])
	assert.Equal(t, false, v2["IsDefaultVersion"])
	assert.NotContains(t, v2, "Document", "CreatePolicyVersion does not return the document")

	ids, def := iamListedVersionIDs(t, srv)
	assert.Equal(t, []string{"v2", "v1"}, ids, "newest first, as the page's sample lists them")
	assert.Equal(t, "v1", def, "a version created without SetAsDefault is not the default")

	assert.JSONEq(t, iamDocAllowing("s3:GetObject"), iamPolicyDocumentFor(t, srv, iamVersionedPolicyARN, "v1"))
	assert.JSONEq(t, iamDocAllowing("s3:PutObject"), iamPolicyDocumentFor(t, srv, iamVersionedPolicyARN, "v2"))

	v3 := iamCreateVersion(t, srv, "s3:DeleteObject", true)
	assert.Equal(t, "v3", v3["VersionId"])
	assert.Equal(t, true, v3["IsDefaultVersion"])
	_, def = iamListedVersionIDs(t, srv)
	assert.Equal(t, "v3", def)

	var policy map[string]any
	decodeIAMXML(t, iamRequest(t, srv, "GetPolicy", map[string]any{"PolicyArn": iamVersionedPolicyARN}), &policy)
	got, _ := policy["Policy"].(map[string]any)
	assert.Equal(t, "v3", got["DefaultVersionId"], "GetPolicy reports the new default")
}

func TestCreatePolicyVersion_FiveVersionLimitAndNumbering(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)
	for _, action := range []string{"a:B", "c:D", "e:F", "g:H"} {
		iamCreateVersion(t, srv, action, false)
	}
	code := iamExpectError(t, srv, "CreatePolicyVersion", map[string]any{
		"PolicyArn": iamVersionedPolicyARN, "PolicyDocument": iamDocAllowing("i:J"),
	}, http.StatusConflict)
	assert.Equal(t, "LimitExceeded", code, "a sixth version exceeds the published limit of five")

	resp := iamRequest(t, srv, "DeletePolicyVersion", map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v5"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	next := iamCreateVersion(t, srv, "i:J", false)
	assert.Equal(t, "v6", next["VersionId"], "a deleted version's number is not reused")
	ids, _ := iamListedVersionIDs(t, srv)
	assert.Equal(t, []string{"v6", "v4", "v3", "v2", "v1"}, ids)
}

func TestPolicyVersionWrites_Refusals(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)
	iamCreateVersion(t, srv, "s3:PutObject", false)

	tests := []struct {
		name   string
		op     string
		body   map[string]any
		status int
		code   string
	}{
		{"delete the default version", "DeletePolicyVersion",
			map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v1"}, http.StatusConflict, "DeleteConflict"},
		{"delete an absent version", "DeletePolicyVersion",
			map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v9"}, http.StatusNotFound, "NoSuchEntity"},
		{"set an absent version as default", "SetDefaultPolicyVersion",
			map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v9"}, http.StatusNotFound, "NoSuchEntity"},
		{"a malformed version ID", "SetDefaultPolicyVersion",
			map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "version-2"}, http.StatusBadRequest, "InvalidInput"},
		{"a policy that does not exist", "CreatePolicyVersion",
			map[string]any{"PolicyArn": "arn:aws:iam::123456789012:policy/absent", "PolicyDocument": iamDocAllowing("a:B")},
			http.StatusNotFound, "NoSuchEntity"},
		{"an AWS managed policy", "CreatePolicyVersion",
			map[string]any{"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess", "PolicyDocument": iamDocAllowing("a:B")},
			http.StatusBadRequest, "InvalidInput"},
		{"a document that is not JSON", "CreatePolicyVersion",
			map[string]any{"PolicyArn": iamVersionedPolicyARN, "PolicyDocument": "{not json"},
			http.StatusBadRequest, "MalformedPolicyDocument"},
		{"a missing document", "CreatePolicyVersion",
			map[string]any{"PolicyArn": iamVersionedPolicyARN}, http.StatusBadRequest, "ValidationError"},
		{"delete a policy with a non-default version", "DeletePolicy",
			map[string]any{"PolicyArn": iamVersionedPolicyARN}, http.StatusConflict, "DeleteConflict"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.code, iamExpectError(t, srv, tt.op, tt.body, tt.status))
		})
	}
}

func TestSetDefaultPolicyVersion_SwitchesTheDocumentAndUnblocksDelete(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)
	iamCreateVersion(t, srv, "s3:PutObject", false)

	resp := iamRequest(t, srv, "SetDefaultPolicyVersion", map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v2"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	_, def := iamListedVersionIDs(t, srv)
	assert.Equal(t, "v2", def)

	// The former default is an ordinary version now, so it can be deleted, and then the policy.
	for _, step := range []struct {
		op   string
		body map[string]any
	}{
		{"DeletePolicyVersion", map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v1"}},
		{"DeletePolicy", map[string]any{"PolicyArn": iamVersionedPolicyARN}},
	} {
		resp := iamRequest(t, srv, step.op, step.body)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", step.op, raw)
	}
}

// TestSetDefaultPolicyVersion_IsWhatTheEvaluatorReads pins that the default version is the one in
// effect: "The default version is the version that is in effect for the IAM users, groups, and
// roles to which the policy is attached" (API_CreatePolicyVersion).
func TestSetDefaultPolicyVersion_IsWhatTheEvaluatorReads(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)
	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "versioned-user"})
	iamRequireOK(t, srv, "AttachUserPolicy", map[string]any{"UserName": "versioned-user", "PolicyArn": iamVersionedPolicyARN})
	decide := func() map[string]string {
		return iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
			"PolicySourceArn": "arn:aws:iam::123456789012:user/versioned-user",
			"ActionNames":     []string{"s3:GetObject", "s3:PutObject"},
		}).decisions()
	}
	assert.Equal(t, map[string]string{"s3:GetObject": "allowed", "s3:PutObject": "implicitDeny"}, decide())

	iamCreateVersion(t, srv, "s3:PutObject", false)
	assert.Equal(t, "implicitDeny", decide()["s3:PutObject"], "a non-default version is not in effect")

	iamRequireOK(t, srv, "SetDefaultPolicyVersion", map[string]any{"PolicyArn": iamVersionedPolicyARN, "VersionId": "v2"})
	assert.Equal(t, map[string]string{"s3:GetObject": "implicitDeny", "s3:PutObject": "allowed"}, decide())
}

func TestListPolicyVersions_Pages(t *testing.T) {
	t.Parallel()
	srv := iamVersionedPolicyServer(t)
	iamCreateVersion(t, srv, "a:B", false)
	iamCreateVersion(t, srv, "c:D", false)

	first := iamDecodePolicyVersion(t, iamRequest(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": iamVersionedPolicyARN, "MaxItems": 2,
	}))
	require.Len(t, first.Versions, 2)
	assert.True(t, first.Truncated)

	var marker struct {
		Marker string `xml:"ListPolicyVersionsResult>Marker"`
	}
	raw := iamResponseXML(t, iamRequest(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": iamVersionedPolicyARN, "MaxItems": 2,
	}))
	require.NoError(t, xml.Unmarshal([]byte(raw), &marker))
	require.Equal(t, "v1", marker.Marker)

	rest := iamDecodePolicyVersion(t, iamRequest(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": iamVersionedPolicyARN, "Marker": marker.Marker,
	}))
	require.Len(t, rest.Versions, 1)
	assert.Equal(t, "v1", rest.Versions[0].VersionID)
	assert.False(t, rest.Truncated)

	assert.Equal(t, "InvalidInput", iamExpectError(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": iamVersionedPolicyARN, "Marker": "v99",
	}, http.StatusBadRequest), "a marker naming no version is refused rather than read as page one")
}
