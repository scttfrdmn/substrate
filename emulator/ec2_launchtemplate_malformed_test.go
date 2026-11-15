package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Launch-template identifiers of the wrong shape (#1507).

const (
	ltMalformedIDCode   = "InvalidLaunchTemplateId.Malformed"
	ltMalformedNameCode = "InvalidLaunchTemplateName.MalformedException"
	ltNameRuleMessage   = "The specified launch template name is invalid. A launch template name must be between 3 and " +
		"128 characters, and may contain letters, numbers, and the following characters: '-', '_', '.', '/', '(', and ')'."
)

// TestEC2_LaunchTemplate_MalformedIdentifiers pins both Malformed codes on every operation that
// names one template, and that a well-formed identifier naming nothing is still NotFound:
// syntax is checked before existence.
func TestEC2_LaunchTemplate_MalformedIdentifiers(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ops := []struct {
		name   string
		params map[string]string
		// prefix is the parameter prefix the identifier is sent under.
		prefix string
	}{
		{"DescribeLaunchTemplateVersions", map[string]string{"Action": "DescribeLaunchTemplateVersions"}, ""},
		{"DeleteLaunchTemplate", map[string]string{"Action": "DeleteLaunchTemplate"}, ""},
		{"CreateLaunchTemplateVersion", map[string]string{
			"Action": "CreateLaunchTemplateVersion", "LaunchTemplateData.InstanceType": "t3.micro",
		}, ""},
		{"ModifyLaunchTemplate", map[string]string{"Action": "ModifyLaunchTemplate"}, ""},
		{"RunInstances", map[string]string{
			"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		}, "LaunchTemplate."},
	}
	idents := []struct {
		name     string
		param    string
		value    string
		wantCode string
		wantMsg  string
	}{
		{"an ID that is not hex", "LaunchTemplateId", "lt-doesnotexist", ltMalformedIDCode,
			"The specified launch template ID 'lt-doesnotexist' is malformed."},
		{"an ID with the wrong prefix", "LaunchTemplateId", "ami-0123456789abcdef0", ltMalformedIDCode,
			"The specified launch template ID 'ami-0123456789abcdef0' is malformed."},
		{"an ID with no suffix", "LaunchTemplateId", "lt-", ltMalformedIDCode,
			"The specified launch template ID 'lt-' is malformed."},
		{"a name too short", "LaunchTemplateName", "ab", ltMalformedNameCode, ltNameRuleMessage},
		{"a name with a space", "LaunchTemplateName", "my template", ltMalformedNameCode, ltNameRuleMessage},
		{"a name too long", "LaunchTemplateName", strings.Repeat("a", 129), ltMalformedNameCode, ltNameRuleMessage},
		{"a well-formed ID naming nothing", "LaunchTemplateId", "lt-0123456789abcdef0",
			"InvalidLaunchTemplateId.NotFound", ""},
		{"a well-formed name naming nothing", "LaunchTemplateName", "no-such-template",
			"InvalidLaunchTemplateName.NotFoundException", ""},
	}
	for _, op := range ops {
		for _, id := range idents {
			t.Run(op.name+"/"+id.name, func(t *testing.T) {
				params := map[string]string{}
				for k, v := range op.params {
					params[k] = v
				}
				params[op.prefix+id.param] = id.value
				status, code, msg := ec2ErrorDetail(t, ts, params)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, id.wantCode, code)
				if id.wantMsg != "" {
					assert.Equal(t, id.wantMsg, msg)
				}
			})
		}
	}
}

// TestEC2_DescribeLaunchTemplates_MalformedIdentifiers covers the list form: every identifier's
// shape is checked before any lookup, so a malformed one later in the list wins over a missing
// one earlier in it.
func TestEC2_DescribeLaunchTemplates_MalformedIdentifiers(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	tests := []struct {
		name     string
		params   map[string]string
		wantCode string
	}{
		{"a malformed ID", map[string]string{"LaunchTemplateId.1": "lt-xyz"}, ltMalformedIDCode},
		{"a malformed ID after a missing one", map[string]string{
			"LaunchTemplateId.1": "lt-0123456789abcdef0", "LaunchTemplateId.2": "lt-xyz",
		}, ltMalformedIDCode},
		{"a malformed name", map[string]string{"LaunchTemplateName.1": "a*b"}, ltMalformedNameCode},
		{"a malformed name after a missing ID", map[string]string{
			"LaunchTemplateId.1": "lt-0123456789abcdef0", "LaunchTemplateName.1": "x",
		}, ltMalformedNameCode},
		{"a missing well-formed ID", map[string]string{"LaunchTemplateId.1": "lt-0123456789abcdef0"},
			"InvalidLaunchTemplateId.NotFound"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"Action": "DescribeLaunchTemplates"}
			for k, v := range tc.params {
				params[k] = v
			}
			status, code, _ := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestEC2_CreateLaunchTemplate_NameRule pins the published name rule at creation: 3 to 128
// characters of letters, digits and - _ . / ( ), the boundaries included.
func TestEC2_CreateLaunchTemplate_NameRule(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	tests := []struct {
		name string
		ok   bool
	}{
		{"abc", true},
		{strings.Repeat("n", 128), true},
		{"a-b_c.d/e(f)G9", true},
		{"ab", false},
		{strings.Repeat("n", 129), false},
		{"has space", false},
		{"colon:name", false},
		{"star*", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
				"Action": "CreateLaunchTemplate", "LaunchTemplateName": tc.name,
				"LaunchTemplateData.ImageId": ec2TestImage,
			})
			if tc.ok {
				assert.Equal(t, http.StatusOK, status, "%s: %s", code, msg)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, ltMalformedNameCode, code)
			assert.Equal(t, ltNameRuleMessage, msg)
		})
	}
}

// TestEC2_LaunchTemplate_MintedIDIsWellFormed pins that substrate's own lt- IDs, 16 hex
// characters where AWS mints 17, pass the shape check every operation now applies.
func TestEC2_LaunchTemplate_MintedIDIsWellFormed(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "minted-shape", map[string]string{"LaunchTemplateData.ImageId": ec2TestImage})
	require.True(t, strings.HasPrefix(ltID, "lt-"))
	versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
	assert.Len(t, versions, 1)
}

// TestEC2_DeleteLaunchTemplateVersions_MalformedIsPerItem pins that this operation reports a
// malformed identifier through ResponseError.code's own enum, per version, at HTTP 200.
func TestEC2_DeleteLaunchTemplateVersions_MalformedIsPerItem(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	tests := []struct {
		name     string
		param    string
		value    string
		wantCode string
		wantMsg  string
	}{
		{"by ID", "LaunchTemplateId", "lt-nothex", "launchTemplateIdMalformed",
			"The specified launch template ID 'lt-nothex' is malformed."},
		{"by name", "LaunchTemplateName", "x", "launchTemplateNameMalformed", ltNameRuleMessage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{
				"Action": "DeleteLaunchTemplateVersions", tc.param: tc.value,
				"LaunchTemplateVersion.1": "2", "LaunchTemplateVersion.2": "$Latest",
			})
			defer resp.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
			var doc struct {
				Deleted []struct{} `xml:"successfullyDeletedLaunchTemplateVersionSet>item"`
				Failed  []struct {
					ID      string `xml:"launchTemplateId"`
					Name    string `xml:"launchTemplateName"`
					Version int64  `xml:"versionNumber"`
					Code    string `xml:"responseError>code"`
					Message string `xml:"responseError>message"`
				} `xml:"unsuccessfullyDeletedLaunchTemplateVersionSet>item"`
			}
			require.NoError(t, xml.Unmarshal(body, &doc))
			assert.Empty(t, doc.Deleted)
			require.Len(t, doc.Failed, 2, "one failure per requested version")
			for _, f := range doc.Failed {
				assert.Equal(t, tc.wantCode, f.Code)
				assert.Equal(t, tc.wantMsg, f.Message)
			}
			assert.Equal(t, int64(2), doc.Failed[0].Version, "a numeric version is echoed")
			assert.Equal(t, int64(0), doc.Failed[1].Version, "an alias has no number to echo")
		})
	}
}

// TestEC2_LaunchTemplate_StoredMalformedNameStaysReachable pins that the shape rule does not lock
// out a template already in state under a name the rule refuses — one created before the rule,
// or written by a replayed event log. Every operation finds it by that name; only a new name is
// held to the rule, at CreateLaunchTemplate.
func TestEC2_LaunchTemplate_StoredMalformedNameStaysReachable(t *testing.T) {
	t.Parallel()
	const acct, region, name = "123456789012", "us-east-1", "ab"
	seed := func(t *testing.T) (*httptest.Server, string) {
		t.Helper()
		state := emulator.NewMemoryStateManager()
		ts := newEC2TestServerWithState(t, state)
		ltID := createLaunchTemplate(t, ts, "seeded-under-ab", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImage, "LaunchTemplateData.InstanceType": "t3.small",
		})
		ctx := t.Context()
		key := "lt:" + acct + "/" + region + "/" + ltID
		raw, err := state.Get(ctx, "ec2", key)
		require.NoError(t, err)
		var rec map[string]any
		require.NoError(t, json.Unmarshal(raw, &rec))
		rec["launchTemplateName"] = name
		raw, err = json.Marshal(rec)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "ec2", key, raw))
		require.NoError(t, state.Delete(ctx, "ec2", "lt_by_name:"+acct+"/"+region+"/seeded-under-ab"))
		require.NoError(t, state.Put(ctx, "ec2", "lt_by_name:"+acct+"/"+region+"/"+name, []byte(ltID)))
		return ts, ltID
	}

	ok := func(t *testing.T, ts *httptest.Server, params map[string]string) {
		t.Helper()
		status, code, msg := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusOK, status, "%s: %s", code, msg)
	}
	t.Run("described, versioned, modified and launched by name", func(t *testing.T) {
		t.Parallel()
		ts, _ := seed(t)
		ok(t, ts, map[string]string{"Action": "DescribeLaunchTemplates", "LaunchTemplateName.1": name})
		ok(t, ts, map[string]string{"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateName": name})
		ok(t, ts, map[string]string{"Action": "RunInstances", "LaunchTemplate.LaunchTemplateName": name,
			"MinCount": "1", "MaxCount": "1"})
		ok(t, ts, map[string]string{"Action": "CreateLaunchTemplateVersion", "LaunchTemplateName": name,
			"LaunchTemplateData.InstanceType": "t3.medium"})
		ok(t, ts, map[string]string{"Action": "ModifyLaunchTemplate", "LaunchTemplateName": name, "SetDefaultVersion": "2"})
	})
	t.Run("its versions are deleted by name", func(t *testing.T) {
		t.Parallel()
		ts, _ := seed(t)
		ok(t, ts, map[string]string{"Action": "CreateLaunchTemplateVersion", "LaunchTemplateName": name,
			"LaunchTemplateData.InstanceType": "t3.medium"})
		resp := ec2Request(t, ts, map[string]string{
			"Action": "DeleteLaunchTemplateVersions", "LaunchTemplateName": name, "LaunchTemplateVersion.1": "2",
		})
		defer resp.Body.Close() //nolint:errcheck
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, string(body), "<successfullyDeletedLaunchTemplateVersionSet><item>")
		assert.NotContains(t, string(body), "launchTemplateNameMalformed")
	})
	t.Run("it is deleted by name", func(t *testing.T) {
		t.Parallel()
		ts, _ := seed(t)
		ok(t, ts, map[string]string{"Action": "DeleteLaunchTemplate", "LaunchTemplateName": name})
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{"Action": "DeleteLaunchTemplate", "LaunchTemplateName": name})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, ltMalformedNameCode, code, "once gone, the name is judged by its shape")
	})
	t.Run("a new template under the name is still refused", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "CreateLaunchTemplate", "LaunchTemplateName": name, "LaunchTemplateData.ImageId": ec2TestImage,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, ltMalformedNameCode, code)
	})
}
