package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
