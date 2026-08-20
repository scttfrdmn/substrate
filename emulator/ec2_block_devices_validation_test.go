package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #671's surface: a block device mapping real EC2 refuses is refused here too, before
// anything is written.
//
// Every case below produced a volume through v0.104.0 — the mapping was parsed,
// materialized and reported by DescribeVolumes, so a consumer whose IaC carried a
// mapping AWS would reject got a green test and a failure on real AWS. That is the
// same class of defect #412 closed for an empty ImageId.
//
// The error code is documented: EC2's client-error table lists
// InvalidBlockDeviceMapping as "A block device mapping parameter is not valid. The
// returned message indicates the incorrect value." Every message is substrate's own,
// since AWS publishes no wording, and the 400 is a class-level inference — the table
// says only that client errors are "accompanied by a 400-series HTTP response code".

// ec2InvalidBDM sends a RunInstances carrying one or more mappings and returns the
// error it is refused with.
func ec2InvalidBDM(t *testing.T, ts *httptest.Server, extra map[string]string) (int, string, string) {
	t.Helper()
	params := map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-0abcdef1234567890",
		"MinCount": "1",
		"MaxCount": "1",
	}
	for k, v := range extra {
		params[k] = v
	}
	return ec2ErrorDetail(t, ts, params)
}

func TestEC2_BlockDeviceMapping_Invalid(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params map[string]string
		msg    string
	}{
		{
			// Documented verbatim on EbsBlockDevice.VolumeSize: "You must specify
			// either a snapshot ID or a volume size."
			name: "an Ebs structure naming neither a size nor a snapshot",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
			},
			msg: "Invalid block device mapping for /dev/sdf: you must specify either a snapshot ID or a volume size",
		},
		{
			name: "a duplicate device name",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.2.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.2.Ebs.VolumeSize": "30",
			},
			msg: "Invalid block device mapping: device /dev/sdf is named by more than one mapping",
		},
		{
			name: "a virtual name beside an Ebs member",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdb",
				"BlockDeviceMapping.1.VirtualName":    "ephemeral0",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
			},
			msg: "Invalid block device mapping for /dev/sdb: virtualName names an instance store device and cannot be combined with an Ebs member",
		},
		{
			// Documented verbatim on EbsBlockDevice.Throughput: "This parameter is
			// valid only for gp3 volumes."
			name: "throughput on a volume type that is not gp3",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.1.Ebs.VolumeType": "gp2",
				"BlockDeviceMapping.1.Ebs.Throughput": "250",
			},
			msg: "Invalid block device mapping for /dev/sdf: Ebs.Throughput is valid only for gp3 volumes, not gp2",
		},
		{
			name: "iops on a volume type that has none",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "500",
				"BlockDeviceMapping.1.Ebs.VolumeType": "st1",
				"BlockDeviceMapping.1.Ebs.Iops":       "3000",
			},
			msg: "Invalid block device mapping for /dev/sdf: Ebs.Iops is not supported for st1 volumes",
		},
		{
			// The refusal #666 made necessary. Ebs.VolumeSize=60GB parsed as absent
			// and silently produced an 8 GiB volume, which is the value a consumer
			// then asserted against.
			name: "an unparseable size",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "60GB",
			},
			msg: `Invalid block device mapping for /dev/sdf: Ebs.VolumeSize value "60GB" is not an integer`,
		},
		{
			name: "an unparseable iops",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.1.Ebs.Iops":       "lots",
			},
			msg: `Invalid block device mapping for /dev/sdf: Ebs.Iops value "lots" is not an integer`,
		},
		{
			name: "an unparseable throughput",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
				"BlockDeviceMapping.1.Ebs.Throughput": "125MiB",
			},
			msg: `Invalid block device mapping for /dev/sdf: Ebs.Throughput value "125MiB" is not an integer`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			status, code, msg := ec2InvalidBDM(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidBlockDeviceMapping", code)
			assert.Equal(t, tc.msg, msg)
		})
	}
}

// TestEC2_BlockDeviceMapping_ValidIsAccepted is the other half, and it is the half that
// matters more: the settled scope for this change is "only what the API model states",
// because a rule substrate invents is a false deny, and a false deny is worse than the
// silence it replaces — the consumer cannot work around it.
func TestEC2_BlockDeviceMapping_ValidIsAccepted(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{
			// AWS's sentence is on EbsBlockDevice.VolumeSize, a member of the Ebs
			// structure. A mapping with no Ebs structure at all names no EBS block
			// device for the requirement to apply to.
			name:   "a device name with no Ebs structure",
			params: map[string]string{"BlockDeviceMapping.1.DeviceName": "/dev/sdf"},
		},
		{
			name: "a snapshot with no size",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.SnapshotId": "snap-0123456789abcdef0",
			},
		},
		{
			// Iops and Throughput with no VolumeType. Substrate resolves an absent
			// type to gp2, but on real EC2 it comes from the AMI's own mapping, which
			// is commonly gp3 — so keying either rule off the *resolved* type would
			// refuse a request real EC2 accepts.
			name: "iops and throughput with no volume type named",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.1.Ebs.Iops":       "3000",
				"BlockDeviceMapping.1.Ebs.Throughput": "125",
			},
		},
		{
			// The "only" sentence on the launch-template sibling shape excludes gp2,
			// but the same member's own paragraph describes what Iops means for a gp2
			// volume. Where AWS contradicts itself, substrate takes the permissive
			// reading.
			name: "iops on gp2",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.1.Ebs.VolumeType": "gp2",
				"BlockDeviceMapping.1.Ebs.Iops":       "100",
			},
		},
		{
			// Two documented-legal forms that rule 1 would refuse if it were not
			// scoped to a mapping that asks for an EBS volume.
			name: "a suppressed device and an instance store device",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":  "/dev/sdb",
				"BlockDeviceMapping.1.NoDevice":    "",
				"BlockDeviceMapping.2.DeviceName":  "/dev/sdc",
				"BlockDeviceMapping.2.VirtualName": "ephemeral0",
			},
		},
		{
			// NoDevice and VirtualName are not device names for the duplicate rule:
			// neither materializes a volume, so neither can collide with one.
			name: "a suppressed device sharing a name with a volume",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "20",
				"BlockDeviceMapping.2.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.2.NoDevice":       "",
			},
		},
		{
			// A size in the documented range for no type in particular. Per-type size
			// and IOPS ranges are deliberately not encoded: they change, and a stale
			// range is a false deny.
			name: "a size outside the documented range for the resolved type",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "99999",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			params := map[string]string{
				"Action":   "RunInstances",
				"ImageId":  "ami-0abcdef1234567890",
				"MinCount": "1",
				"MaxCount": "1",
			}
			for k, v := range tc.params {
				params[k] = v
			}
			resp := ec2Request(t, ts, params)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.NotContains(t, string(body), "InvalidBlockDeviceMapping")
		})
	}
}

// TestEC2_BlockDeviceMapping_RefusalWritesNothing is #671's ordering criterion, and the
// reason the validator sits where it does.
//
// runInstancesWithTags was validate-then-write in its error *returns* but not in its
// writes: ensureDefaultVPC committed a VPC, subnet, security group, internet gateway,
// route table and four index mutations before the security-group validation below it.
// A refusal that leaves a VPC behind is worse than no validation, because the next
// request in the same test sees state the refused one created.
func TestEC2_BlockDeviceMapping_RefusalWritesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	_, code, _ := ec2InvalidBDM(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
	})
	require.Equal(t, "InvalidBlockDeviceMapping", code)

	assert.Empty(t, bdmDescribeVolumes(t, ts, nil), "a refused launch materializes no volume")

	for _, action := range []string{"DescribeInstances", "DescribeVpcs", "DescribeSubnets"} {
		resp := ec2Request(t, ts, map[string]string{"Action": action})
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

		var counted struct {
			VPCs      []struct{} `xml:"vpcSet>item"`
			Subnets   []struct{} `xml:"subnetSet>item"`
			Instances []struct{} `xml:"reservationSet>item>instancesSet>item"`
		}
		require.NoError(t, xml.Unmarshal(body, &counted), string(body))
		assert.Empty(t, counted.VPCs, "%s: a refused launch creates no default VPC", action)
		assert.Empty(t, counted.Subnets, "%s: no default subnet either", action)
		assert.Empty(t, counted.Instances, "%s: and no instance", action)
	}
}

// TestEC2_BlockDeviceMapping_TemplateMappingRefusedAtLaunch pins that the validator sits
// after the launch-template merge, so a mapping that reaches a launch through a template
// is refused at RunInstances time rather than materialized.
//
// CreateLaunchTemplate deliberately does not refuse it. Its response carries a
// documented `warning` member of type ValidationWarning, which exists precisely for
// "parameters or parameter combinations that are not valid", and its Errors section
// lists none — so a 400 there would be substrate's invention. That an invalid *block
// device mapping* lands in the warning rather than in an error is substrate's reading,
// and rendering the element is a follow-up.
func TestEC2_BlockDeviceMapping_TemplateMappingRefusedAtLaunch(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "bad-mapping",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body),
		"CreateLaunchTemplate does not refuse; the warning member is where AWS puts this")

	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                            "RunInstances",
		"MinCount":                          "1",
		"MaxCount":                          "1",
		"LaunchTemplate.LaunchTemplateName": "bad-mapping",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidBlockDeviceMapping", code)
	assert.Equal(t,
		"Invalid block device mapping for /dev/sdf: you must specify either a snapshot ID or a volume size",
		msg, "a template-carried mapping is refused at launch, where it materializes")
}
