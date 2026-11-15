package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An ImageId of resolve:ssm:<parameter>, in RunInstances and in a launch template (#1506).

// ssmPut stores a parameter through SSM's own PutParameter on the environment's server.
func (e *asgEnv) ssmPut(name, typ, value string, overwrite bool) {
	e.t.Helper()
	body, err := json.Marshal(map[string]any{"Name": name, "Type": typ, "Value": value, "Overwrite": overwrite})
	require.NoError(e.t, err)
	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodPost, e.ts.URL+"/", bytes.NewReader(body))
	require.NoError(e.t, err)
	req.Host = "ssm.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonSSM.PutParameter")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	require.Equal(e.t, http.StatusOK, resp.StatusCode, "PutParameter %s: %s", name, out)
}

// ec2Err sends an EC2 request that must fail, and returns status, code and message.
func (e *asgEnv) ec2Err(action string, params map[string]string) (int, string, string) {
	e.t.Helper()
	status, body := e.call("ec2.us-east-1.amazonaws.com", action, params)
	require.NotEqual(e.t, http.StatusOK, status, "%s unexpectedly succeeded: %s", action, body)
	var doc struct {
		Code    string `xml:"Errors>Error>Code"`
		Message string `xml:"Errors>Error>Message"`
	}
	require.NoError(e.t, xml.Unmarshal(body, &doc), "%s", body)
	return status, doc.Code, doc.Message
}

// runImage launches one instance and returns the AMI and type it reports.
func (e *asgEnv) runImage(params map[string]string) (imageID, instanceType string) {
	e.t.Helper()
	full := map[string]string{"MinCount": "1", "MaxCount": "1", "SubnetId": e.subnetA}
	for k, v := range params {
		full[k] = v
	}
	var run struct {
		ImageID string `xml:"instancesSet>item>imageId"`
		Type    string `xml:"instancesSet>item>instanceType"`
	}
	e.ec2XML("RunInstances", full, &run)
	return run.ImageID, run.Type
}

// TestEC2_ResolveSSMImageID_Launches pins every published reference form that resolves, from
// the request and from a template: a bundled public parameter, a user String parameter by name
// with and without a leading slash, by ARN, and with its current version as the selector.
func TestEC2_ResolveSSMImageID_Launches(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.ssmPut("golden-ami", "String", ec2TestImageUbuntu, false)
	// Version 2, so a selector naming the current version is not the trivial 1.
	e.ssmPut("golden-ami", "String", ec2TestImageMinimal, true)

	const public = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
	tests := []struct {
		name, ref, want string
	}{
		{"a public parameter", "resolve:ssm:" + public, emulator.BundledImageID("us-east-1", public)},
		{"a public parameter by ARN", "resolve:ssm:arn:aws:ssm:us-east-1::parameter" + public,
			emulator.BundledImageID("us-east-1", public)},
		{"a public parameter at version 1", "resolve:ssm:" + public + ":1", emulator.BundledImageID("us-east-1", public)},
		{"a user parameter by name", "resolve:ssm:golden-ami", ec2TestImageMinimal},
		{"a user parameter with its slash", "resolve:ssm:/golden-ami", ec2TestImageMinimal},
		{"a user parameter at its current version", "resolve:ssm:golden-ami:2", ec2TestImageMinimal},
		{"a user parameter by ARN", "resolve:ssm:arn:aws:ssm:us-east-1:123456789012:parameter/golden-ami", ec2TestImageMinimal},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := e.runImage(map[string]string{"ImageId": tc.ref})
			assert.Equal(t, tc.want, got, "RunInstances reports the AMI, not the reference")

			lt := "ssm-lt-" + string(rune('a'+i))
			var created struct {
				ID string `xml:"launchTemplate>launchTemplateId"`
			}
			e.ec2XML("CreateLaunchTemplate", map[string]string{
				"LaunchTemplateName": lt, "LaunchTemplateData.ImageId": tc.ref,
			}, &created)
			got, _ = e.runImage(map[string]string{"LaunchTemplate.LaunchTemplateId": created.ID})
			assert.Equal(t, tc.want, got, "a template's reference resolves at launch")
		})
	}
}

// TestEC2_ResolveSSMImageID_FollowsTheParameter is the user guide's point: the template is
// unchanged and a launch after the parameter moves gets the new AMI.
func TestEC2_ResolveSSMImageID_FollowsTheParameter(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.ssmPut("/team/ami", "String", ec2TestImage, false)
	var created struct {
		ID string `xml:"launchTemplate>launchTemplateId"`
	}
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName": "follows", "LaunchTemplateData.ImageId": "resolve:ssm:/team/ami",
	}, &created)

	got, _ := e.runImage(map[string]string{"LaunchTemplate.LaunchTemplateId": created.ID})
	assert.Equal(t, ec2TestImage, got)
	e.ssmPut("/team/ami", "String", ec2TestImageUbuntu, true)
	got, _ = e.runImage(map[string]string{"LaunchTemplate.LaunchTemplateId": created.ID})
	assert.Equal(t, ec2TestImageUbuntu, got)
}

// TestEC2_ResolveSSMImageID_Refusals pins what does not resolve. Each is InvalidParameterValue,
// whose message is substrate's own; a resolved value that is not an AMI is the launch's own
// AMI refusal.
func TestEC2_ResolveSSMImageID_Refusals(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.ssmPut("list-param", "StringList", ec2TestImage+","+ec2TestImageUbuntu, false)
	e.ssmPut("not-an-ami", "String", "hello", false)
	e.ssmPut("golden", "String", ec2TestImage, false)

	tests := []struct {
		name, ref, wantCode string
	}{
		{"a parameter that does not exist", "resolve:ssm:/no/such/param", "InvalidParameterValue"},
		{"a StringList parameter", "resolve:ssm:list-param", "InvalidParameterValue"},
		{"a version substrate does not hold", "resolve:ssm:golden:7", "InvalidParameterValue"},
		{"a label", "resolve:ssm:golden:prod", "InvalidParameterValue"},
		{"another account's parameter", "resolve:ssm:arn:aws:ssm:us-east-1:210987654321:parameter/golden", "InvalidParameterValue"},
		{"another Region's parameter", "resolve:ssm:arn:aws:ssm:eu-west-1:123456789012:parameter/golden", "InvalidParameterValue"},
		{"an empty reference", "resolve:ssm:", "InvalidParameterValue"},
		{"a trailing separator with no selector", "resolve:ssm:golden:", "InvalidParameterValue"},
		{"an ARN with a trailing separator", "resolve:ssm:arn:aws:ssm:us-east-1:123456789012:parameter/golden:", "InvalidParameterValue"},
		{"an ARN whose resource is not parameter/", "resolve:ssm:arn:aws:ssm:us-east-1:123456789012:parametergolden", "InvalidParameterValue"},
		{"an ARN of another service", "resolve:ssm:arn:aws:s3:us-east-1:123456789012:parameter/golden", "InvalidParameterValue"},
		{"a value that is not an AMI", "resolve:ssm:not-an-ami", "InvalidAMIID.Malformed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := e.ec2Err("RunInstances", map[string]string{
				"ImageId": tc.ref, "MinCount": "1", "MaxCount": "1", "SubnetId": e.subnetA,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code, "%s", msg)
			if tc.wantCode == "InvalidParameterValue" {
				assert.Contains(t, msg, tc.ref)
			}
		})
	}
}

// TestEC2_DescribeLaunchTemplateVersions_ResolveAlias pins the reference's ResolveAlias: the
// stored reference reads back by default, and the AMI it resolves to with ResolveAlias=true.
func TestEC2_DescribeLaunchTemplateVersions_ResolveAlias(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.ssmPut("golden-ami", "String", ec2TestImageUbuntu, false)
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName": "alias-lt", "LaunchTemplateData.ImageId": "resolve:ssm:golden-ami",
	}, nil)
	tests := []struct {
		name, resolveAlias, want string
	}{
		{"absent", "", "resolve:ssm:golden-ami"},
		{"false", "false", "resolve:ssm:golden-ami"},
		{"true", "true", ec2TestImageUbuntu},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"LaunchTemplateName": "alias-lt"}
			if tc.resolveAlias != "" {
				params["ResolveAlias"] = tc.resolveAlias
			}
			var doc struct {
				ImageIDs []string `xml:"launchTemplateVersionSet>item>launchTemplateData>imageId"`
			}
			e.ec2XML("DescribeLaunchTemplateVersions", params, &doc)
			assert.Equal(t, []string{tc.want}, doc.ImageIDs)
		})
	}
}

// TestAutoScaling_ResolveSSMImage pins the Auto Scaling half: a plain template group launches
// from the resolved AMI, attribute-based selection narrows by the resolved AMI's architecture
// when the reference arrives through a later template version, and creating an attribute-based
// group on such a template is refused, as the Auto Scaling user guide states.
func TestAutoScaling_ResolveSSMImage(t *testing.T) {
	t.Parallel()

	t.Run("a template group launches the resolved AMI", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.ssmPut("/asg/ami", "String", ec2TestImageUbuntu, false)
		e.ec2XML("CreateLaunchTemplate", map[string]string{
			"LaunchTemplateName": "ssm-group-lt", "LaunchTemplateData.ImageId": "resolve:ssm:/asg/ami",
			"LaunchTemplateData.InstanceType": "t3.micro",
		}, nil)
		e.mustASG("CreateAutoScalingGroup", map[string]string{
			"AutoScalingGroupName": "ssm-group", "MinSize": "1", "MaxSize": "1", "DesiredCapacity": "1",
			"VPCZoneIdentifier": e.subnetA, "LaunchTemplate.LaunchTemplateName": "ssm-group-lt",
		})
		insts := e.ec2GroupInstances("ssm-group")
		require.Len(t, insts, 1)
		var doc struct {
			ImageID string `xml:"reservationSet>item>instancesSet>item>imageId"`
		}
		e.ec2XML("DescribeInstances", map[string]string{"InstanceId.1": insts[0].ID}, &doc)
		assert.Equal(t, ec2TestImageUbuntu, doc.ImageID)
	})

	t.Run("an attribute-based group on a resolve:ssm: template is refused", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.ssmPut("/asg/arm-ami", "String", ec2TestImageArm, false)
		e.ec2XML("CreateLaunchTemplate", map[string]string{
			"LaunchTemplateName": "ssm-abs-lt", "LaunchTemplateData.ImageId": "resolve:ssm:/asg/arm-ami",
		}, nil)
		status, code, msg := e.asgErr("CreateAutoScalingGroup", absGroupParams(e, "abs", "ssm-abs-lt"))
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "ValidationError", code)
		assert.Equal(t, "A mixed instances policy that uses attribute-based instance type selection cannot use a "+
			"Systems Manager parameter as its AMI: launch template ssm-abs-lt version 1 has ImageId "+
			"resolve:ssm:/asg/arm-ami. Specify an AMI ID in the launch template.", msg)
	})

	t.Run("an override's own resolve:ssm: ImageId is refused and named", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.ssmPut("/asg/arm-ami", "String", ec2TestImageArm, false)
		e.ec2XML("CreateLaunchTemplate", map[string]string{
			"LaunchTemplateName": "plain-abs-lt", "LaunchTemplateData.ImageId": ec2TestImageArm,
		}, nil)
		params := absGroupParams(e, "abs-ov", "plain-abs-lt")
		params["MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.ImageId"] = "resolve:ssm:/asg/arm-ami"
		status, code, msg := e.asgErr("CreateAutoScalingGroup", params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "ValidationError", code)
		assert.Equal(t, "A mixed instances policy that uses attribute-based instance type selection cannot use a "+
			"Systems Manager parameter as its AMI: the override's ImageId is resolve:ssm:/asg/arm-ami. Specify an AMI ID.", msg)
	})

	t.Run("attribute-based selection narrows by the resolved AMI's architecture", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.ssmPut("/asg/arm-ami", "String", ec2TestImageArm, false)
		var created struct {
			ID string `xml:"launchTemplate>launchTemplateId"`
		}
		e.ec2XML("CreateLaunchTemplate", map[string]string{
			"LaunchTemplateName": "ssm-later-lt", "LaunchTemplateData.ImageId": ec2TestImageArm,
		}, &created)
		params := absGroupParams(e, "later", "ssm-later-lt")
		params["MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.Version"] = "$Latest"
		e.mustASG("CreateAutoScalingGroup", params)
		// The reference arrives through a version the group's $Latest picks up later, which no
		// create or update validated — the path the resolved lookup exists for.
		e.ec2XML("CreateLaunchTemplateVersion", map[string]string{
			"LaunchTemplateId": created.ID, "LaunchTemplateData.ImageId": "resolve:ssm:/asg/arm-ami",
		}, nil)
		e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "later", "DesiredCapacity": "1"})
		insts := e.ec2GroupInstances("later")
		require.Len(t, insts, 1)
		assert.Equal(t, "c6g.large", insts[0].Type, "an arm64 type, chosen for the resolved arm64 AMI")
	})
}

// TestEC2_ResolveSSMImageID_OpenSpotRequest pins that a Spot request a seeded shortfall leaves
// open reports the AMI its resolve:ssm: ImageId named, and that AMI's product description, as a
// fulfilled request does from its instance.
func TestEC2_ResolveSSMImageID_OpenSpotRequest(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	const windows = "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base"
	resp, err := http.Post(e.ts.URL+"/v1/ec2/spot-request-outcomes", "application/json",
		bytes.NewReader([]byte(`{"statusCode":"capacity-not-available"}`)))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var created struct {
		IDs []string `xml:"spotInstanceRequestSet>item>spotInstanceRequestId"`
	}
	e.ec2XML("RequestSpotInstances", map[string]string{
		"LaunchSpecification.ImageId": "resolve:ssm:" + windows, "LaunchSpecification.InstanceType": "m5.large",
		"LaunchSpecification.SubnetId": e.subnetA,
	}, &created)
	require.Len(t, created.IDs, 1)

	var doc struct {
		Items []ec2DescribedSpotRequest `xml:"spotInstanceRequestSet>item"`
	}
	e.ec2XML("DescribeSpotInstanceRequests", map[string]string{"SpotInstanceRequestId.1": created.IDs[0]}, &doc)
	require.Len(t, doc.Items, 1)
	assert.Equal(t, "open", doc.Items[0].State)
	assert.Equal(t, emulator.BundledImageID("us-east-1", windows), doc.Items[0].ImageID, "the AMI, not the reference")
	assert.Equal(t, "Windows", doc.Items[0].Product, "the resolved AMI's platform")
}

// TestEC2_ResolveSSMImageID_StoreFailureIsAnError pins that a parameter whose stored record does
// not decode is an error, not an absent parameter and not a fall back to the bundled catalog.
func TestEC2_ResolveSSMImageID_StoreFailureIsAnError(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	// A public parameter's name, so a fallback to the bundled catalog would launch.
	const name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
	require.NoError(t, state.Put(t.Context(), "ssm", "parameter:123456789012/us-east-1/"+name, []byte("{not json")))

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "resolve:ssm:" + name, "MinCount": "1", "MaxCount": "1",
	})
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.NotEqual(t, "InvalidParameterValue", code, "a store failure is not an unresolvable reference")
}

// absGroupParams is a mixed-instances group selecting 2-vCPU, 4 GiB types by attributes.
func absGroupParams(e *asgEnv, group, lt string) map[string]string {
	const (
		mip = "MixedInstancesPolicy."
		ir  = mip + "LaunchTemplate.Overrides.member.1.InstanceRequirements."
	)
	return map[string]string{
		"AutoScalingGroupName": group, "MinSize": "0", "MaxSize": "2", "DesiredCapacity": "0",
		"VPCZoneIdentifier": e.subnetA,
		mip + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName": lt,
		ir + "VCpuCount.Min": "2", ir + "VCpuCount.Max": "2",
		ir + "MemoryMiB.Min": "4096", ir + "MemoryMiB.Max": "4096",
	}
}
