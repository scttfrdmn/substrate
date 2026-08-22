package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ltVersionItem is one launchTemplateVersion element, shared by
// CreateLaunchTemplateVersion and DescribeLaunchTemplateVersions.
type ltVersionItem struct {
	VersionNumber      int64  `xml:"versionNumber"`
	VersionDescription string `xml:"versionDescription"`
	DefaultVersion     bool   `xml:"defaultVersion"`
	CreatedBy          string `xml:"createdBy"`
	CreateTime         string `xml:"createTime"`
	Data               struct {
		ImageID          string   `xml:"imageId"`
		InstanceType     string   `xml:"instanceType"`
		KeyName          string   `xml:"keyName"`
		UserData         string   `xml:"userData"`
		SecurityGroupIDs []string `xml:"securityGroupIdSet>item"`
		Interfaces       []struct {
			SubnetID                 string   `xml:"subnetId"`
			AssociatePublicIPAddress string   `xml:"associatePublicIpAddress"`
			Groups                   []string `xml:"groupSet>item"`
		} `xml:"networkInterfaceSet>item"`
		IamInstanceProfile struct {
			ARN  string `xml:"arn"`
			Name string `xml:"name"`
		} `xml:"iamInstanceProfile"`
		// Note the name: the response member is tagSpecificationSet while the request
		// member is TagSpecification.N, so a round-trip has to spell it both ways.
		TagSpecifications []struct {
			ResourceType string `xml:"resourceType"`
			Tags         []struct {
				Key   string `xml:"key"`
				Value string `xml:"value"`
			} `xml:"tagSet>item"`
		} `xml:"tagSpecificationSet>item"`
	} `xml:"launchTemplateData"`
}

// createLTVersion sends CreateLaunchTemplateVersion and returns the new version.
func createLTVersion(t *testing.T, ts *httptest.Server, params map[string]string) ltVersionItem {
	t.Helper()
	full := map[string]string{"Action": "CreateLaunchTemplateVersion"}
	for k, v := range params {
		full[k] = v
	}
	resp := ec2Request(t, ts, full)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateLaunchTemplateVersion")
	var out struct {
		Version ltVersionItem `xml:"launchTemplateVersion"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	return out.Version
}

// describeLTVersions sends DescribeLaunchTemplateVersions and returns the items
// plus the pagination token.
func describeLTVersions(t *testing.T, ts *httptest.Server, params map[string]string) ([]ltVersionItem, string) {
	t.Helper()
	full := map[string]string{"Action": "DescribeLaunchTemplateVersions"}
	for k, v := range params {
		full[k] = v
	}
	resp := ec2Request(t, ts, full)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "DescribeLaunchTemplateVersions")
	var out struct {
		Versions  []ltVersionItem `xml:"launchTemplateVersionSet>item"`
		NextToken string          `xml:"nextToken"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	return out.Versions, out.NextToken
}

// ltSummary is the launchTemplate summary element, which carries no template data.
type ltSummary struct {
	LaunchTemplateID   string `xml:"launchTemplateId"`
	LaunchTemplateName string `xml:"launchTemplateName"`
	DefaultVersionNum  int64  `xml:"defaultVersionNumber"`
	LatestVersionNum   int64  `xml:"latestVersionNumber"`
}

// describeLT returns one launch template's summary.
func describeLT(t *testing.T, ts *httptest.Server, ltID string) ltSummary {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":             "DescribeLaunchTemplates",
		"LaunchTemplateId.1": ltID,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Templates []ltSummary `xml:"launchTemplates>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Templates, 1)
	return out.Templates[0]
}

// TestEC2_CreateLaunchTemplate_MakesVersionOne pins that creating a template
// creates its version 1, and that version 1 is both the default and the latest.
func TestEC2_CreateLaunchTemplate_MakesVersionOne(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "v1-only", map[string]string{
		"LaunchTemplateData.ImageId":      ec2TestImage,
		"LaunchTemplateData.InstanceType": "t3.small",
		"VersionDescription":              "initial",
	})

	summary := describeLT(t, ts, ltID)
	assert.Equal(t, int64(1), summary.DefaultVersionNum)
	assert.Equal(t, int64(1), summary.LatestVersionNum)

	versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
	require.Len(t, versions, 1)
	assert.Equal(t, int64(1), versions[0].VersionNumber)
	assert.True(t, versions[0].DefaultVersion)
	assert.Equal(t, "initial", versions[0].VersionDescription)
	assert.Equal(t, ec2TestImage, versions[0].Data.ImageID)
	assert.Equal(t, "t3.small", versions[0].Data.InstanceType)
}

// TestEC2_CreateLaunchTemplateVersion_AppendsWithoutChangingDefault covers the
// half of #456 that made the operation worth registering: a new version becomes the
// latest but *not* the default, so launches keep using version 1 until
// ModifyLaunchTemplate says otherwise.
func TestEC2_CreateLaunchTemplateVersion_AppendsWithoutChangingDefault(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "appends", map[string]string{
		"LaunchTemplateData.ImageId":      ec2TestImage,
		"LaunchTemplateData.InstanceType": "t3.small",
	})

	second := createLTVersion(t, ts, map[string]string{
		"LaunchTemplateId":           ltID,
		"LaunchTemplateData.ImageId": ec2TestImageArm,
	})
	assert.Equal(t, int64(2), second.VersionNumber)
	assert.False(t, second.DefaultVersion, "a new version must not become the default")

	third := createLTVersion(t, ts, map[string]string{
		"LaunchTemplateId":           ltID,
		"LaunchTemplateData.ImageId": ec2TestImageMinimal,
	})
	assert.Equal(t, int64(3), third.VersionNumber)

	summary := describeLT(t, ts, ltID)
	assert.Equal(t, int64(1), summary.DefaultVersionNum, "default must still be version 1")
	assert.Equal(t, int64(3), summary.LatestVersionNum)
}

// TestEC2_CreateLaunchTemplateVersion_SourceVersion covers the asymmetry the API
// reference documents and a consumer routinely gets backwards: with SourceVersion
// the new version inherits the source's parameters and the request's values
// overwrite them; *without* it the new version holds only what the request names.
//
// Getting this wrong in either direction is silent — an unintended inherit carries
// forward a parameter the caller meant to drop, and a missing inherit launches
// instances with no instance type or key pair.
func TestEC2_CreateLaunchTemplateVersion_SourceVersion(t *testing.T) {
	t.Run("SourceVersion inherits, request overwrites", func(t *testing.T) {
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "inherits", map[string]string{
			"LaunchTemplateData.ImageId":      ec2TestImage,
			"LaunchTemplateData.InstanceType": "t3.small",
			"LaunchTemplateData.KeyName":      "inherited-key",
		})

		v2 := createLTVersion(t, ts, map[string]string{
			"LaunchTemplateId":           ltID,
			"SourceVersion":              "1",
			"LaunchTemplateData.ImageId": ec2TestImageArm,
		})
		assert.Equal(t, ec2TestImageArm, v2.Data.ImageID, "the request's AMI wins")
		assert.Equal(t, "t3.small", v2.Data.InstanceType, "inherited from version 1")
		assert.Equal(t, "inherited-key", v2.Data.KeyName, "inherited from version 1")
	})

	t.Run("no SourceVersion inherits nothing", func(t *testing.T) {
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "no-inherit", map[string]string{
			"LaunchTemplateData.ImageId":      ec2TestImage,
			"LaunchTemplateData.InstanceType": "t3.small",
			"LaunchTemplateData.KeyName":      "dropped-key",
		})

		v2 := createLTVersion(t, ts, map[string]string{
			"LaunchTemplateId":           ltID,
			"LaunchTemplateData.ImageId": ec2TestImageArm,
		})
		assert.Equal(t, ec2TestImageArm, v2.Data.ImageID)
		assert.Empty(t, v2.Data.InstanceType, "an omitted SourceVersion inherits nothing")
		assert.Empty(t, v2.Data.KeyName, "an omitted SourceVersion inherits nothing")
	})

	t.Run("$Latest as the source", func(t *testing.T) {
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "latest-source", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImage,
		})
		createLTVersion(t, ts, map[string]string{
			"LaunchTemplateId":                ltID,
			"LaunchTemplateData.ImageId":      ec2TestImageArm,
			"LaunchTemplateData.InstanceType": "t3.large",
		})
		v3 := createLTVersion(t, ts, map[string]string{
			"LaunchTemplateId":           ltID,
			"SourceVersion":              "$Latest",
			"LaunchTemplateData.KeyName": "k",
		})
		assert.Equal(t, ec2TestImageArm, v3.Data.ImageID, "inherited from version 2")
		assert.Equal(t, "t3.large", v3.Data.InstanceType)
	})

	t.Run("a nonexistent SourceVersion is an error", func(t *testing.T) {
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "bad-source", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImage,
		})
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action":                     "CreateLaunchTemplateVersion",
			"LaunchTemplateId":           ltID,
			"SourceVersion":              "7",
			"LaunchTemplateData.ImageId": ec2TestImageArm,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidLaunchTemplateId.VersionNotFound", code)
	})
}

// TestEC2_RunInstances_AbsentVersionMeansDefault is the correctness gate on the
// whole PR. LaunchTemplateSpecification.Version documents "Default: The default
// version of the launch template" — not the latest — so a launch naming no version
// after ModifyLaunchTemplate must use the pinned default even though newer versions
// exist.
//
// Inverting this resolution is invisible in every other test: with the default at
// version 1 and no newer versions, latest and default are the same number. Here
// they deliberately differ in both directions.
func TestEC2_RunInstances_AbsentVersionMeansDefault(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "default-vs-latest", map[string]string{
		"LaunchTemplateData.ImageId": ec2TestImage,
	})
	createLTVersion(t, ts, map[string]string{
		"LaunchTemplateId":           ltID,
		"LaunchTemplateData.ImageId": ec2TestImageArm,
	})
	createLTVersion(t, ts, map[string]string{
		"LaunchTemplateId":           ltID,
		"LaunchTemplateData.ImageId": ec2TestImageMinimal,
	})

	// Before any Modify, the default is version 1 while the latest is 3.
	instID := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
	assert.Equal(t, ec2TestImage, describeInstance(t, ts, instID).ImageID,
		"an absent version must resolve to the default (1), not the latest (3)")

	// Pin the default to 2; the latest is still 3.
	resp := ec2Request(t, ts, map[string]string{
		"Action":            "ModifyLaunchTemplate",
		"LaunchTemplateId":  ltID,
		"SetDefaultVersion": "2",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var modified struct {
		LaunchTemplate ltSummary `xml:"launchTemplate"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&modified))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, int64(2), modified.LaunchTemplate.DefaultVersionNum)
	assert.Equal(t, int64(3), modified.LaunchTemplate.LatestVersionNum)

	instID = runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
	assert.Equal(t, ec2TestImageArm, describeInstance(t, ts, instID).ImageID,
		"an absent version must follow the new default (2), not the latest (3)")
}

// TestEC2_RunInstances_VersionSpecifiers walks every way a launch can name a
// version, with the default and the latest deliberately different numbers so no two
// rows can pass for the same reason.
func TestEC2_RunInstances_VersionSpecifiers(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantAMI string
	}{
		{name: "absent means the default", version: "", wantAMI: ec2TestImageArm},
		{name: "$Default", version: "$Default", wantAMI: ec2TestImageArm},
		{name: "$Latest", version: "$Latest", wantAMI: ec2TestImageMinimal},
		{name: "lowercase $latest", version: "$latest", wantAMI: ec2TestImageMinimal},
		{name: "an explicit number", version: "1", wantAMI: ec2TestImage},
		{name: "the default's own number", version: "2", wantAMI: ec2TestImageArm},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			ltID := createLaunchTemplate(t, ts, "specifiers", map[string]string{
				"LaunchTemplateData.ImageId": ec2TestImage,
			})
			for _, ami := range []string{ec2TestImageArm, ec2TestImageMinimal} {
				createLTVersion(t, ts, map[string]string{
					"LaunchTemplateId":           ltID,
					"LaunchTemplateData.ImageId": ami,
				})
			}
			resp := ec2Request(t, ts, map[string]string{
				"Action":            "ModifyLaunchTemplate",
				"LaunchTemplateId":  ltID,
				"SetDefaultVersion": "2",
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			params := map[string]string{"LaunchTemplate.LaunchTemplateId": ltID}
			if tt.version != "" {
				params["LaunchTemplate.Version"] = tt.version
			}
			instID := runInstance(t, ts, params)
			assert.Equal(t, tt.wantAMI, describeInstance(t, ts, instID).ImageID)
		})
	}
}

// TestEC2_RunInstances_NonexistentVersionIsRejected pins that a launch naming a
// version that does not exist fails rather than silently falling back. A fallback
// would launch an instance from parameters the caller never asked for, which is the
// defect shape #456 exists to close.
func TestEC2_RunInstances_NonexistentVersionIsRejected(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "bad-version", map[string]string{
		"LaunchTemplateData.ImageId": ec2TestImage,
	})

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"LaunchTemplate.LaunchTemplateId": ltID,
		"LaunchTemplate.Version":          "9",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidLaunchTemplateId.VersionNotFound", code)
	assert.Contains(t, message, "9")

	// And nothing launched.
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Instances []launchedInstance `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out.Instances, "a rejected launch must not create an instance")
}

// TestEC2_ModifyLaunchTemplate_RejectsUnknownVersion pins that a default cannot be
// pinned to a version that does not exist — otherwise every later launch resolves
// to nothing.
func TestEC2_ModifyLaunchTemplate_RejectsUnknownVersion(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "modify-bad", map[string]string{
		"LaunchTemplateData.ImageId": ec2TestImage,
	})

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":            "ModifyLaunchTemplate",
		"LaunchTemplateId":  ltID,
		"SetDefaultVersion": "4",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidLaunchTemplateId.VersionNotFound", code)
	assert.Equal(t, int64(1), describeLT(t, ts, ltID).DefaultVersionNum, "the default must be unchanged")
}

// TestEC2_DescribeLaunchTemplateVersions_Selection covers version selection: named
// versions in the order requested, the two aliases, and the Min/Max range form.
func TestEC2_DescribeLaunchTemplateVersions_Selection(t *testing.T) {
	newTemplate := func(t *testing.T) (*httptest.Server, string) {
		t.Helper()
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "selection", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImage,
		})
		for _, ami := range []string{
			ec2TestImageArm, ec2TestImageMinimal, ec2TestImageMinimalArm,
		} {
			createLTVersion(t, ts, map[string]string{
				"LaunchTemplateId":           ltID,
				"LaunchTemplateData.ImageId": ami,
			})
		}
		return ts, ltID
	}

	t.Run("no version named returns all, ascending", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		versions, token := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
		require.Len(t, versions, 4)
		assert.Empty(t, token)
		for i, v := range versions {
			assert.Equal(t, int64(i+1), v.VersionNumber)
		}
	})

	t.Run("named versions, in the order requested", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		versions, _ := describeLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "3",
			"LaunchTemplateVersion.2": "1",
		})
		require.Len(t, versions, 2)
		assert.Equal(t, int64(3), versions[0].VersionNumber)
		assert.Equal(t, int64(1), versions[1].VersionNumber)
	})

	t.Run("the aliases", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		versions, _ := describeLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "$Latest",
			"LaunchTemplateVersion.2": "$Default",
		})
		require.Len(t, versions, 2)
		assert.Equal(t, int64(4), versions[0].VersionNumber)
		assert.Equal(t, int64(1), versions[1].VersionNumber)
		assert.False(t, versions[0].DefaultVersion)
		assert.True(t, versions[1].DefaultVersion)
	})

	t.Run("MinVersion and MaxVersion bound the range inclusively", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		versions, _ := describeLTVersions(t, ts, map[string]string{
			"LaunchTemplateId": ltID,
			"MinVersion":       "2",
			"MaxVersion":       "3",
		})
		require.Len(t, versions, 2)
		assert.Equal(t, int64(2), versions[0].VersionNumber)
		assert.Equal(t, int64(3), versions[1].VersionNumber)
	})

	t.Run("MaxResults pages, and the token resumes", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		first, token := describeLTVersions(t, ts, map[string]string{
			"LaunchTemplateId": ltID,
			"MaxResults":       "2",
		})
		require.Len(t, first, 2)
		require.NotEmpty(t, token, "a truncated page must carry a token")

		rest, nextToken := describeLTVersions(t, ts, map[string]string{
			"LaunchTemplateId": ltID,
			"MaxResults":       "2",
			"NextToken":        token,
		})
		require.Len(t, rest, 2)
		assert.Empty(t, nextToken, "the final page must not carry a token")
		assert.Equal(t, int64(3), rest[0].VersionNumber)
		assert.Equal(t, int64(4), rest[1].VersionNumber)
	})

	t.Run("a nonexistent version is an error", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action":                  "DescribeLaunchTemplateVersions",
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "9",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidLaunchTemplateId.VersionNotFound", code)
	})
}

// TestEC2_DescribeLaunchTemplateVersions_AccountWide covers the form that names no
// template: it lists every template's $Latest and/or $Default. AWS restricts it to
// those two aliases, because a version number means nothing across templates.
func TestEC2_DescribeLaunchTemplateVersions_AccountWide(t *testing.T) {
	setup := func(t *testing.T) *httptest.Server {
		t.Helper()
		ts := newEC2TestServer(t)
		// One AMI per template per version, all four distinct, so no assertion below
		// can pass because two templates happened to hold the same image.
		images := [][2]string{
			{ec2TestImage, ec2TestImageArm},
			{ec2TestImageMinimal, ec2TestImageMinimalArm},
		}
		for i, name := range []string{"acct-a", "acct-b"} {
			ltID := createLaunchTemplate(t, ts, name, map[string]string{
				"LaunchTemplateData.ImageId": images[i][0],
			})
			createLTVersion(t, ts, map[string]string{
				"LaunchTemplateId":           ltID,
				"LaunchTemplateData.ImageId": images[i][1],
			})
		}
		return ts
	}

	t.Run("$Latest across every template", func(t *testing.T) {
		ts := setup(t)
		versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateVersion.1": "$Latest"})
		require.Len(t, versions, 2)
		for _, v := range versions {
			assert.Equal(t, int64(2), v.VersionNumber)
		}
	})

	t.Run("both aliases yield two items per template", func(t *testing.T) {
		ts := setup(t)
		versions, _ := describeLTVersions(t, ts, map[string]string{
			"LaunchTemplateVersion.1": "$Latest",
			"LaunchTemplateVersion.2": "$Default",
		})
		assert.Len(t, versions, 4)
	})

	t.Run("a version number is rejected without a template", func(t *testing.T) {
		ts := setup(t)
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action":                  "DescribeLaunchTemplateVersions",
			"LaunchTemplateVersion.1": "1",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})

	t.Run("naming neither a template nor an alias is rejected", func(t *testing.T) {
		ts := setup(t)
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "DescribeLaunchTemplateVersions",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "MissingParameter", code)
	})
}

// ltDeleteResult is DeleteLaunchTemplateVersions' two-set response.
type ltDeleteResult struct {
	Deleted []struct {
		VersionNumber int64 `xml:"versionNumber"`
	} `xml:"successfullyDeletedLaunchTemplateVersionSet>item"`
	Failed []struct {
		VersionNumber int64 `xml:"versionNumber"`
		ResponseError struct {
			Code    string `xml:"code"`
			Message string `xml:"message"`
		} `xml:"responseError"`
	} `xml:"unsuccessfullyDeletedLaunchTemplateVersionSet>item"`
}

// deleteLTVersions sends DeleteLaunchTemplateVersions and returns both sets along
// with the HTTP status, which is 200 even for a wholly-failing request.
func deleteLTVersions(t *testing.T, ts *httptest.Server, params map[string]string) (int, ltDeleteResult) {
	t.Helper()
	full := map[string]string{"Action": "DeleteLaunchTemplateVersions"}
	for k, v := range params {
		full[k] = v
	}
	resp := ec2Request(t, ts, full)
	defer resp.Body.Close() //nolint:errcheck
	var out ltDeleteResult
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	}
	return resp.StatusCode, out
}

// TestEC2_DeleteLaunchTemplateVersions covers the operation's per-item failure
// model. "You can't delete the default version of a launch template" is reported as
// an entry in unsuccessfullyDeletedLaunchTemplateVersionSet at HTTP 200, not as a
// request-level error — so a caller checking only the status code believes the
// delete succeeded. That is the same trap SendMessageBatch sets.
func TestEC2_DeleteLaunchTemplateVersions(t *testing.T) {
	newTemplate := func(t *testing.T) (*httptest.Server, string) {
		t.Helper()
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "deletes", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImage,
		})
		for _, ami := range []string{ec2TestImageArm, ec2TestImageMinimal} {
			createLTVersion(t, ts, map[string]string{
				"LaunchTemplateId":           ltID,
				"LaunchTemplateData.ImageId": ami,
			})
		}
		return ts, ltID
	}

	t.Run("a non-default version deletes", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, result := deleteLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "2",
		})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, result.Deleted, 1)
		assert.Equal(t, int64(2), result.Deleted[0].VersionNumber)
		assert.Empty(t, result.Failed)

		versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
		require.Len(t, versions, 2)
		assert.Equal(t, int64(1), versions[0].VersionNumber)
		assert.Equal(t, int64(3), versions[1].VersionNumber)
	})

	t.Run("the default version fails as an item, at 200", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, result := deleteLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "1",
		})
		require.Equal(t, http.StatusOK, status, "a per-item failure is still HTTP 200")
		assert.Empty(t, result.Deleted)
		require.Len(t, result.Failed, 1)
		assert.Equal(t, int64(1), result.Failed[0].VersionNumber)
		assert.Contains(t, result.Failed[0].ResponseError.Message, "default version")

		// And the version survives.
		versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
		assert.Len(t, versions, 3)
	})

	t.Run("$Default resolves to the default and fails too", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, result := deleteLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "$Default",
		})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, result.Failed, 1)
		assert.Empty(t, result.Deleted)
	})

	t.Run("one request, one entry in each set", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, result := deleteLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "1",
			"LaunchTemplateVersion.2": "3",
		})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, result.Deleted, 1, "the non-default version deletes")
		assert.Equal(t, int64(3), result.Deleted[0].VersionNumber)
		require.Len(t, result.Failed, 1, "the default version fails")
		assert.Equal(t, int64(1), result.Failed[0].VersionNumber)
	})

	t.Run("a nonexistent version fails as an item", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, result := deleteLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "9",
		})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, result.Failed, 1)
		// The one failure mode ResponseError.code has a documented value for.
		assert.Equal(t, "launchTemplateVersionDoesNotExist", result.Failed[0].ResponseError.Code)
	})

	t.Run("deleting the newest version does not free its number", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, _ := deleteLTVersions(t, ts, map[string]string{
			"LaunchTemplateId":        ltID,
			"LaunchTemplateVersion.1": "3",
		})
		require.Equal(t, http.StatusOK, status)

		// "You cannot replace the version number after you delete it."
		next := createLTVersion(t, ts, map[string]string{
			"LaunchTemplateId":           ltID,
			"LaunchTemplateData.ImageId": ec2TestImageMinimalArm,
		})
		assert.Equal(t, int64(4), next.VersionNumber, "a deleted version number must not be reused")
	})

	t.Run("naming no version is a request-level error", func(t *testing.T) {
		ts, ltID := newTemplate(t)
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action":           "DeleteLaunchTemplateVersions",
			"LaunchTemplateId": ltID,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "MissingParameter", code)
	})
}

// TestEC2_LaunchTemplate_PreVersioningStateStillWorks is the migration gate.
//
// A template written to state before #456 has no versions array — only the single
// latestData field. Replaying such an event log must still launch instances and must
// read back as a one-version template, which it does because that template's
// latestData *is* its version 1. The stored JSON is written by hand rather than
// produced by CreateLaunchTemplate, because CreateLaunchTemplate can no longer
// produce it: that is precisely what makes this a migration rather than a round-trip.
func TestEC2_LaunchTemplate_PreVersioningStateStillWorks(t *testing.T) {
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.EC2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	ts := httptest.NewServer(emulator.NewServer(*emulator.DefaultConfig(), registry, store, state, tc, logger))
	t.Cleanup(ts.Close)

	// Exactly the shape createLaunchTemplate wrote before #456: no "versions" key.
	const acct, region, ltID = "123456789012", "us-east-1", "lt-0legacy00000001"
	legacy := map[string]any{
		"launchTemplateId":     ltID,
		"launchTemplateName":   "legacy",
		"defaultVersionNumber": 1,
		"latestVersionNumber":  1,
		"createdBy":            acct,
		"createTime":           "2026-01-01T00:00:00Z",
		"latestData": map[string]any{
			"imageId":      ec2TestImage,
			"instanceType": "t3.small",
			"keyName":      "legacy-key",
		},
		"accountID": acct,
		"region":    region,
	}
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, state.Put(ctx, "ec2", "lt:"+acct+"/"+region+"/"+ltID, raw))
	require.NoError(t, state.Put(ctx, "ec2", "lt_by_name:"+acct+"/"+region+"/legacy", []byte(ltID)))
	idsRaw, err := json.Marshal([]string{ltID})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "ec2", "lt_ids:"+acct+"/"+region, idsRaw))

	t.Run("it still launches", func(t *testing.T) {
		instID := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
		inst := describeInstance(t, ts, instID)
		assert.Equal(t, ec2TestImage, inst.ImageID)
		assert.Equal(t, "t3.small", inst.InstanceType)
		assert.Equal(t, "legacy-key", inst.KeyName)
	})

	t.Run("it reads back as version 1, default", func(t *testing.T) {
		versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
		require.Len(t, versions, 1)
		assert.Equal(t, int64(1), versions[0].VersionNumber)
		assert.True(t, versions[0].DefaultVersion)
		assert.Equal(t, ec2TestImage, versions[0].Data.ImageID)
	})

	t.Run("$Latest and $Default both resolve to it", func(t *testing.T) {
		for _, spec := range []string{"$Latest", "$Default", "1"} {
			instID := runInstance(t, ts, map[string]string{
				"LaunchTemplate.LaunchTemplateId": ltID,
				"LaunchTemplate.Version":          spec,
			})
			assert.Equal(t, ec2TestImage, describeInstance(t, ts, instID).ImageID, "version %q", spec)
		}
	})

	t.Run("a new version appends as version 2", func(t *testing.T) {
		v2 := createLTVersion(t, ts, map[string]string{
			"LaunchTemplateId":           ltID,
			"SourceVersion":              "1",
			"LaunchTemplateData.ImageId": ec2TestImageArm,
		})
		assert.Equal(t, int64(2), v2.VersionNumber)
		assert.Equal(t, "t3.small", v2.Data.InstanceType, "inherited from the synthesized version 1")
	})
}

// TestEC2_DescribeLaunchTemplateVersions_RoundTripsNetworking pins that a
// template's network interface survives the round trip. ResponseLaunchTemplateData
// has no top-level subnetId, so the subnet and public-IP preference must come back
// nested inside networkInterfaceSet — the same asymmetry #444 hinged on.
func TestEC2_DescribeLaunchTemplateVersions_RoundTripsNetworking(t *testing.T) {
	ts := newEC2TestServer(t)
	net := newLTNetwork(t, ts, "10.9.0.0/16", "lt-versions-net")
	ltID := createLaunchTemplate(t, ts, "networking", map[string]string{
		"LaunchTemplateData.ImageId":                                     ec2TestImage,
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":              "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":                 net.subnetID,
		"LaunchTemplateData.NetworkInterface.1.Groups.1":                 net.sgID,
		"LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress": "true",
	})

	versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
	require.Len(t, versions, 1)
	require.Len(t, versions[0].Data.Interfaces, 1)
	ifc := versions[0].Data.Interfaces[0]
	assert.Equal(t, net.subnetID, ifc.SubnetID)
	assert.Equal(t, "true", ifc.AssociatePublicIPAddress)
	assert.Equal(t, []string{net.sgID}, ifc.Groups)
}

// TestEC2_Fleet_HonorsLaunchTemplateVersion covers the fleet half of #456.
// fleetPools has always parsed a config's Version; launchFleetPool dropped it, so a
// fleet pinned to version 1 launched whatever the template held latest. Nothing
// errored — the fleet just launched the wrong AMI.
func TestEC2_Fleet_HonorsLaunchTemplateVersion(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "fleet-versions", map[string]string{
		"LaunchTemplateData.ImageId":      ec2TestImage,
		"LaunchTemplateData.InstanceType": "t3.small",
	})
	createLTVersion(t, ts, map[string]string{
		"LaunchTemplateId":                ltID,
		"LaunchTemplateData.ImageId":      ec2TestImageArm,
		"LaunchTemplateData.InstanceType": "t3.small",
	})

	tests := []struct {
		name    string
		version string
		wantAMI string
	}{
		{name: "pinned to version 1", version: "1", wantAMI: ec2TestImage},
		{name: "pinned to version 2", version: "2", wantAMI: ec2TestImageArm},
		{name: "$Latest", version: "$Latest", wantAMI: ec2TestImageArm},
		// An absent version follows the default, which is still 1 even though
		// version 2 exists.
		{name: "absent means the default", version: "", wantAMI: ec2TestImage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]string{
				"Action": "CreateFleet",
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
				"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
				"TargetCapacitySpecification.DefaultTargetCapacityType":                "on-demand",
				"Type": "instant",
			}
			if tt.version != "" {
				params["LaunchTemplateConfigs.1.LaunchTemplateSpecification.Version"] = tt.version
			}
			resp := ec2Request(t, ts, params)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var out struct {
				Instances []struct {
					InstanceIDs []string `xml:"instanceIds>item"`
				} `xml:"fleetInstanceSet>item"`
			}
			require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
			require.NoError(t, resp.Body.Close())
			require.Len(t, out.Instances, 1)
			require.Len(t, out.Instances[0].InstanceIDs, 1)

			inst := describeInstance(t, ts, out.Instances[0].InstanceIDs[0])
			assert.Equal(t, tt.wantAMI, inst.ImageID)
		})
	}
}
