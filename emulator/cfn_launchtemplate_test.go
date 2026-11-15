package emulator_test

import (
	"context"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1508: an AWS::EC2::LaunchTemplate forwards MetadataOptions and
// DisableApiTermination, and records the version numbers Fn::GetAtt reads.

// cfnLaunchTemplate is a template whose LaunchTemplateData carries both members, with the hop limit
// as the given JSON value and the token requirement from a parameter, so intrinsics are exercised.
func cfnLaunchTemplate(hopLimit, instanceType string) string {
	return `{
		"Parameters":{"Tokens":{"Type":"String","Default":"required"}},
		"Resources":{"LT":{"Type":"AWS::EC2::LaunchTemplate","Properties":{
			"LaunchTemplateName":"imds-nodes",
			"LaunchTemplateData":{
				"ImageId":"` + ec2TestImage + `","InstanceType":"` + instanceType + `",
				"DisableApiTermination":true,
				"MetadataOptions":{"HttpTokens":{"Ref":"Tokens"},"HttpPutResponseHopLimit":` + hopLimit + `,
					"HttpEndpoint":"enabled","HttpProtocolIpv6":"disabled","InstanceMetadataTags":"enabled"}}}}},
		"Outputs":{
			"Id":{"Value":{"Ref":"LT"}},
			"Latest":{"Value":{"Fn::GetAtt":["LT","LatestVersionNumber"]}},
			"Default":{"Value":{"Fn::GetAtt":["LT","DefaultVersionNumber"]}},
			"AttrId":{"Value":{"Fn::GetAtt":["LT","LaunchTemplateId"]}}}}`
}

// TestCFN_LaunchTemplateForwardsMetadataOptions is #1508's acceptance test: both members reach
// DescribeLaunchTemplateVersions with intrinsics resolved and the hop limit an integer, an instance
// launched from the template reports them, and the version attributes resolve.
func TestCFN_LaunchTemplateForwardsMetadataOptions(t *testing.T) {
	for _, hop := range []struct{ name, value string }{{"a number", "2"}, {"a string", `"2"`}} {
		t.Run(hop.name, func(t *testing.T) {
			d, registry := newEC2IAMTestDeployer(t)
			result, err := d.Deploy(context.Background(), cfnLaunchTemplate(hop.value, "t3.micro"), "imds-stack", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, result)
			id := result.Outputs["Id"]
			assert.Equal(t, id, result.Outputs["AttrId"])
			assert.Equal(t, "1", result.Outputs["Latest"], "LatestVersionNumber resolves rather than being empty")
			assert.Equal(t, "1", result.Outputs["Default"])

			body := routeQuery(t, registry, "ec2", map[string]string{
				"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": id, "LaunchTemplateVersion.1": "1"})
			var doc struct {
				Data struct {
					DisableAPITermination string         `xml:"disableApiTermination"`
					Metadata              *ltMetadataXML `xml:"metadataOptions"`
				} `xml:"launchTemplateVersionSet>item>launchTemplateData"`
			}
			require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
			assert.Equal(t, "true", doc.Data.DisableAPITermination)
			assert.Equal(t, &ltMetadataXML{
				HTTPTokens: strp("required"), HTTPPutResponseHopLimit: intp(2), HTTPEndpoint: strp("enabled"),
				HTTPProtocolIPv6: strp("disabled"), InstanceMetadataTags: strp("enabled"),
			}, doc.Data.Metadata)

			run := routeQuery(t, registry, "ec2", map[string]string{
				"Action": "RunInstances", "MinCount": "1", "MaxCount": "1",
				"LaunchTemplate.LaunchTemplateId": id})
			var launched struct {
				ID       string         `xml:"instancesSet>item>instanceId"`
				Metadata *ltMetadataXML `xml:"instancesSet>item>metadataOptions"`
			}
			require.NoError(t, xml.Unmarshal([]byte(run), &launched), run)
			require.NotNil(t, launched.Metadata, run)
			assert.Equal(t, "required", *launched.Metadata.HTTPTokens, "the instance requires IMDSv2")
			assert.Equal(t, 2, *launched.Metadata.HTTPPutResponseHopLimit)

			attr := routeQuery(t, registry, "ec2", map[string]string{
				"Action": "DescribeInstanceAttribute", "InstanceId": launched.ID, "Attribute": "disableApiTermination"})
			var termination struct {
				Value string `xml:"disableApiTermination>value"`
			}
			require.NoError(t, xml.Unmarshal([]byte(attr), &termination), attr)
			assert.Equal(t, "true", termination.Value)
		})
	}
}

// TestCFN_LaunchTemplateRefusesWhatEC2Refuses pins that a value EC2 refuses fails the resource with
// EC2's own code, rather than being dropped on the way.
func TestCFN_LaunchTemplateRefusesWhatEC2Refuses(t *testing.T) {
	tests := []struct{ name, hop, params string }{
		{"a hop limit out of range", "65", ``},
		{"a hop limit that is not an integer", `"two"`, ``},
		{"an HttpTokens value outside the enum", "2", `{"Tokens":"sometimes"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := newEC2IAMTestDeployer(t)
			params := map[string]string{}
			if tc.params != "" {
				params["Tokens"] = "sometimes"
			}
			result, err := d.DeployWithOptions(context.Background(), cfnLaunchTemplate(tc.hop, "t3.micro"),
				"imds-refused", params, emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDoNothing})
			require.NoError(t, err)
			assert.Equal(t, "CREATE_FAILED", result.Status)
			assert.Contains(t, findResource(t, result, "LT").Error, "InvalidParameterValue")
		})
	}
}

// TestCFN_LaunchTemplateUpdateAddsAVersion pins the update path: a changed LaunchTemplateData is a
// new version of the same template, LatestVersionNumber follows it, and DefaultVersionNumber — which
// "cannot be specified in AWS CloudFormation" — stays.
func TestCFN_LaunchTemplateUpdateAddsAVersion(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	ctx := context.Background()
	created, err := d.Deploy(ctx, cfnLaunchTemplate("2", "t3.micro"), "imds-update", nil)
	require.NoError(t, err)
	id := created.Outputs["Id"]

	updated, err := d.UpdateStack(ctx, cfnLaunchTemplate("2", "t3.large"), "imds-update", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, id, updated.Outputs["Id"], "the template keeps its ID")
	assert.Equal(t, "2", updated.Outputs["Latest"])
	assert.Equal(t, "1", updated.Outputs["Default"])

	body := routeQuery(t, registry, "ec2", map[string]string{
		"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": id, "LaunchTemplateVersion.1": "2"})
	var doc struct {
		InstanceType string `xml:"launchTemplateVersionSet>item>launchTemplateData>instanceType"`
		Tokens       string `xml:"launchTemplateVersionSet>item>launchTemplateData>metadataOptions>httpTokens"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	assert.Equal(t, "t3.large", doc.InstanceType)
	assert.Equal(t, "required", doc.Tokens, "the new version carries the whole data, not only the change")

	unchanged, err := d.UpdateStack(ctx, cfnLaunchTemplate("2", "t3.large"), "imds-update", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "2", unchanged.Outputs["Latest"], "an unchanged update adds no version")
}
