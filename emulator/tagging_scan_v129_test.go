package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetResources reports security groups, launch templates, volumes and instance profiles (#1523).

// taggingV129IAM posts one IAM operation to the tagging test server and requires success.
func taggingV129IAM(t *testing.T, ts *httptest.Server, op string, body map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Host = "iam.amazonaws.com"
	req.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equalf(t, http.StatusOK, resp.StatusCode, "IAM %s: %s", op, raw)
}

// taggingV129Mappings runs GetResources with body and returns ARN to tag map.
func taggingV129Mappings(t *testing.T, ts *httptest.Server, body map[string]any) map[string]map[string]string {
	t.Helper()
	resp := taggingRequest(t, ts, "GetResources", body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		ResourceTagMappingList []struct {
			ResourceARN string
			Tags        []struct{ Key, Value string }
		}
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.NoError(t, resp.Body.Close())
	out := map[string]map[string]string{}
	for _, m := range got.ResourceTagMappingList {
		tags := map[string]string{}
		for _, tag := range m.Tags {
			tags[tag.Key] = tag.Value
		}
		out[m.ResourceARN] = tags
	}
	return out
}

func TestTagging_GetResourcesReportsTheIssue1523Types(t *testing.T) {
	t.Parallel()
	ts, _ := newTaggingTestServer(t)
	const deployment = "aegc:deployment"

	var sg struct {
		ID string `xml:"groupId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "tagged-sg", "GroupDescription": "tagged",
		"TagSpecification.1.ResourceType": "security-group",
		"TagSpecification.1.Tag.1.Key":    deployment, "TagSpecification.1.Tag.1.Value": "dev",
	}, &sg)
	require.NotEmpty(t, sg.ID)

	var lt struct {
		ID string `xml:"launchTemplate>launchTemplateId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateLaunchTemplate", "LaunchTemplateName": "tagged-lt",
		"LaunchTemplateData.ImageId":      "ami-12345678",
		"TagSpecification.1.ResourceType": "launch-template",
		"TagSpecification.1.Tag.1.Key":    deployment, "TagSpecification.1.Tag.1.Value": "dev",
	}, &lt)
	require.NotEmpty(t, lt.ID)

	instances := bdmRunInstances(t, ts, map[string]string{
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    deployment, "TagSpecification.1.Tag.1.Value": "dev",
	})
	require.Len(t, instances, 1)

	taggingV129IAM(t, ts, "CreateInstanceProfile", map[string]any{
		"InstanceProfileName": "tagged-profile", "Path": "/aegc/",
	})
	taggingV129IAM(t, ts, "TagInstanceProfile", map[string]any{
		"InstanceProfileName": "tagged-profile", "Tags": []map[string]string{{"Key": deployment, "Value": "dev"}},
	})

	got := taggingV129Mappings(t, ts, map[string]any{
		"TagFilters": []map[string]any{{"Key": deployment, "Values": []string{"dev"}}},
	})
	var arns []string
	for arn := range got {
		arns = append(arns, arn)
	}
	sort.Strings(arns)
	require.Len(t, arns, 4, "%v", arns)
	assert.Contains(t, arns, "arn:aws:ec2:us-east-1:123456789012:security-group/"+sg.ID)
	assert.Contains(t, arns, "arn:aws:ec2:us-east-1:123456789012:launch-template/"+lt.ID)
	assert.Contains(t, arns, "arn:aws:iam::123456789012:instance-profile/aegc/tagged-profile")
	var volume string
	for _, a := range arns {
		if strings.Contains(a, ":volume/vol-") {
			volume = a
		}
	}
	require.NotEmpty(t, volume, "the launch's tagged volume is reported: %v", arns)

	// The type filters name each type by its ARN segment.
	for filter, want := range map[string]int{
		"ec2:security-group": 1, "ec2:launch-template": 1, "ec2:volume": 1, "iam:instance-profile": 1,
	} {
		assert.Len(t, taggingV129Mappings(t, ts, map[string]any{"ResourceTypeFilters": []string{filter}}), want, filter)
	}

	// TagResources and UntagResources take the same ARNs, and a resource whose tags are all
	// removed is still reported, with none.
	for _, arn := range arns {
		resp := taggingRequest(t, ts, "TagResources", map[string]any{
			"ResourceARNList": []string{arn}, "Tags": map[string]string{"extra": "yes"},
		})
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotContains(t, string(raw), "ErrorCode", "TagResources %s: %s", arn, raw)
	}
	for arn, tags := range taggingV129Mappings(t, ts, map[string]any{}) {
		if strings.Contains(arn, "security-group/") || strings.Contains(arn, "instance-profile/") {
			assert.Equal(t, "yes", tags["extra"], arn)
		}
	}
	resp := taggingRequest(t, ts, "UntagResources", map[string]any{
		"ResourceARNList": arns, "TagKeys": []string{deployment, "extra"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	all := taggingV129Mappings(t, ts, map[string]any{})
	for _, arn := range arns {
		tags, reported := all[arn]
		assert.True(t, reported, "%s was tagged once, so it is still reported", arn)
		assert.Empty(t, tags, arn)
	}

	// A rule change rewrites the group through its typed record, which must keep the flag.
	ec2FleetXML(t, ts, map[string]string{
		"Action": "AuthorizeSecurityGroupIngress", "GroupId": sg.ID,
		"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "443", "IpPermissions.1.ToPort": "443",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
	}, &struct{}{})
	_, reported := taggingV129Mappings(t, ts, map[string]any{"ResourceTypeFilters": []string{"ec2:security-group"}})[arns[slicesIndex(arns, "security-group/")]]
	assert.True(t, reported, "an authorize keeps the group's previously-tagged flag")
}

// slicesIndex returns the index of the first member of list containing sub.
func slicesIndex(list []string, sub string) int {
	for i, s := range list {
		if strings.Contains(s, sub) {
			return i
		}
	}
	return -1
}

// TestTagging_AnUnreadableRecordHidesNoOtherResource is review finding 6: one record that does not
// decode is reported as the scan's failure, and every other resource of its type is still listed.
func TestTagging_AnUnreadableRecordHidesNoOtherResource(t *testing.T) {
	t.Parallel()
	ts, state := newTaggingTestServer(t)
	var sg struct {
		ID string `xml:"groupId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "readable", "GroupDescription": "readable",
		"TagSpecification.1.ResourceType": "security-group",
		"TagSpecification.1.Tag.1.Key":    "team", "TagSpecification.1.Tag.1.Value": "core",
	}, &sg)
	require.NoError(t, state.Put(t.Context(), "ec2", "sg:123456789012/us-east-1/sg-0000000000unreadable", []byte("{not json")))

	got := taggingV129Mappings(t, ts, map[string]any{"ResourceTypeFilters": []string{"ec2:security-group"}})
	assert.Contains(t, got, "arn:aws:ec2:us-east-1:123456789012:security-group/"+sg.ID,
		"the readable group is still reported beside the unreadable one")
}
