package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An AMI whose architecture the instance type cannot run is refused (#1505).

// ec2ArchMismatchMessage is the observed refusal, with the type's architectures and the AMI's.
func ec2ArchMismatchMessage(typeArch, imageArch string) string {
	return "The architecture '" + typeArch + "' of the specified instance type does not match the architecture '" +
		imageArch + "' of the specified AMI. Specify an instance type and an AMI that have matching architectures, " +
		"and try again. You can use 'describe-instance-types' or 'describe-images' to discover the architecture " +
		"of the instance type or AMI."
}

// TestEC2_RunInstances_ArchitectureMismatch is table-driven over x86_64 and arm64 types and
// AMIs, in both directions, plus the cases the check deliberately lets through.
func TestEC2_RunInstances_ArchitectureMismatch(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var i386 struct {
		ID string `xml:"imageId"`
	}
	// RegisterImage with no Architecture reports the published default, i386.
	ec2DescribeXML(t, ts, map[string]string{"Action": "RegisterImage", "Name": "legacy", "RootDeviceName": "/dev/sda1"}, &i386)
	require.NotEmpty(t, i386.ID)

	tests := []struct {
		name         string
		image        string
		instanceType string
		wantMessage  string // empty: the launch succeeds
	}{
		{"x86_64 AMI on an x86_64 type", ec2TestImage, "m5.large", ""},
		{"arm64 AMI on an arm64 type", ec2TestImageArm, "m7g.large", ""},
		{"arm64 AMI on a gd variant", ec2TestImageArm, "c7gd.large", ""},
		{"arm64 AMI on the arm64 accelerated family", ec2TestImageArm, "g5g.xlarge", ""},
		{"arm64 AMI on an x86_64 type", ec2TestImageArm, "t3.micro", ec2ArchMismatchMessage("x86_64", "arm64")},
		{"arm64 AMI on the default type", ec2TestImageArm, "", ec2ArchMismatchMessage("x86_64", "arm64")},
		{"arm64 AMI on an x86_64 GPU type", ec2TestImageArm, "g5.xlarge", ec2ArchMismatchMessage("x86_64", "arm64")},
		{"x86_64 AMI on an arm64 type", ec2TestImage, "c7g.large", ec2ArchMismatchMessage("arm64", "x86_64")},
		{"x86_64 AMI on g5g", ec2TestImage, "g5g.xlarge", ec2ArchMismatchMessage("arm64", "x86_64")},
		{"a type the catalog does not carry is not refused", ec2TestImageArm, "x2iedn.xlarge", ""},
		{"an i386 AMI is not refused", i386.ID, "c7g.large", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"Action": "RunInstances", "ImageId": tc.image, "MinCount": "1", "MaxCount": "1"}
			if tc.instanceType != "" {
				params["InstanceType"] = tc.instanceType
			}
			status, code, msg := ec2ErrorDetail(t, ts, params)
			if tc.wantMessage == "" {
				assert.Equal(t, http.StatusOK, status, "%s: %s", code, msg)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, tc.wantMessage, msg)
		})
	}
}

// TestEC2_RunInstances_ArchitectureMismatchWritesNothing pins that the refusal comes before the
// first write: no instance is left behind.
func TestEC2_RunInstances_ArchitectureMismatchWritesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	status, _, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImageArm, "InstanceType": "m5.large", "MinCount": "1", "MaxCount": "1",
	})
	require.Equal(t, http.StatusBadRequest, status)
	var doc struct {
		IDs []string `xml:"reservationSet>item>instancesSet>item>instanceId"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &doc)
	assert.Empty(t, doc.IDs)
}

// TestEC2_CreateFleet_ArchitectureMismatch pins that a fleet, which launches through the same
// path, refuses an override type its template's AMI cannot run.
func TestEC2_CreateFleet_ArchitectureMismatch(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "arm-fleet", map[string]string{"LaunchTemplateData.ImageId": ec2TestImageArm})
	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "CreateFleet", "Type": "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.Version":          "$Latest",
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "c5.large",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		"TargetCapacitySpecification.DefaultTargetCapacityType":                "on-demand",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, ec2ArchMismatchMessage("x86_64", "arm64"), msg)
}

// TestAutoScaling_ArchitectureMismatch pins both sides for a group. Attribute-based selection
// already narrows by the AMI's architecture, so it does not newly fail; an explicit override
// type the AMI cannot run fails the launch with EC2's refusal in the activity, as the Auto
// Scaling troubleshooting guide quotes it.
func TestAutoScaling_ArchitectureMismatch(t *testing.T) {
	t.Parallel()

	t.Run("attribute-based selection on an arm64 AMI launches", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.ec2XML("CreateLaunchTemplate", map[string]string{
			"LaunchTemplateName": "abs-arm", "LaunchTemplateData.ImageId": ec2TestImageArm,
		}, nil)
		e.mustASG("CreateAutoScalingGroup", absGroupParams(e, "abs-arm", "abs-arm"))
		e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "abs-arm", "DesiredCapacity": "1"})
		insts := e.ec2GroupInstances("abs-arm")
		require.Len(t, insts, 1)
		assert.Equal(t, "c6g.large", insts[0].Type)
	})

	t.Run("an explicit x86_64 override on an arm64 AMI fails the launch", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.ec2XML("CreateLaunchTemplate", map[string]string{
			"LaunchTemplateName": "typed-arm", "LaunchTemplateData.ImageId": ec2TestImageArm,
		}, nil)
		const mip = "MixedInstancesPolicy."
		e.mustASG("CreateAutoScalingGroup", map[string]string{
			"AutoScalingGroupName": "typed-arm", "MinSize": "0", "MaxSize": "1", "DesiredCapacity": "0",
			"VPCZoneIdentifier": e.subnetA,
			mip + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName": "typed-arm",
			mip + "LaunchTemplate.Overrides.member.1.InstanceType":                "m5.large",
		})
		e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "typed-arm", "DesiredCapacity": "1"})
		assert.Empty(t, e.group("typed-arm").Instances)
		acts := e.activities(map[string]string{"AutoScalingGroupName": "typed-arm"})
		require.NotEmpty(t, acts)
		assert.Equal(t, "Failed", acts[0].StatusCode)
		assert.True(t, strings.Contains(acts[0].StatusMessage, ec2ArchMismatchMessage("x86_64", "arm64")),
			"the activity carries EC2's refusal: %s", acts[0].StatusMessage)
	})
}
