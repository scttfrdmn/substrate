package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The launch-time block device mapping behavior #666 reports, asserted through HTTP
// so what a caller actually receives is what is pinned.
//
// These assertions unmarshal the response rather than searching the body for "vol-".
// The existing volume tests use strings.Index for that, which cannot tell two
// volumes apart — and a launch now routinely produces two.

// bdmVolume is one volumeSet>item of a DescribeVolumes response.
type bdmVolume struct {
	VolumeID         string `xml:"volumeId"`
	Size             int    `xml:"size"`
	VolumeType       string `xml:"volumeType"`
	AvailabilityZone string `xml:"availabilityZone"`
	Status           string `xml:"status"`
	Encrypted        bool   `xml:"encrypted"`
	SnapshotID       string `xml:"snapshotId"`
	IOPS             int    `xml:"iops"`
	Throughput       int    `xml:"throughput"`
	Attachments      []struct {
		VolumeID            string `xml:"volumeId"`
		InstanceID          string `xml:"instanceId"`
		Device              string `xml:"device"`
		Status              string `xml:"status"`
		AttachTime          string `xml:"attachTime"`
		DeleteOnTermination bool   `xml:"deleteOnTermination"`
	} `xml:"attachmentSet>item"`
}

// bdmDevice returns the volume's first attachment device, or "" when it has none.
func (v bdmVolume) bdmDevice() string {
	if len(v.Attachments) == 0 {
		return ""
	}
	return v.Attachments[0].Device
}

// bdmDescribeVolumes calls DescribeVolumes with the given extra parameters and
// returns the volumes it reports, sorted by attachment device so an assertion does
// not depend on state-listing order.
func bdmDescribeVolumes(t *testing.T, ts *httptest.Server, params map[string]string) []bdmVolume {
	t.Helper()
	all := map[string]string{"Action": "DescribeVolumes"}
	for k, v := range params {
		all[k] = v
	}
	resp := ec2Request(t, ts, all)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		Items []bdmVolume `xml:"volumeSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	sort.Slice(parsed.Items, func(i, j int) bool {
		return parsed.Items[i].bdmDevice() < parsed.Items[j].bdmDevice()
	})
	return parsed.Items
}

// bdmRunInstances launches instances and returns their IDs in response order.
func bdmRunInstances(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	all := map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-0abcdef1234567890",
		"MinCount": "1",
		"MaxCount": "1",
	}
	for k, v := range params {
		all[k] = v
	}
	resp := ec2Request(t, ts, all)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		Items []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	ids := make([]string, 0, len(parsed.Items))
	for _, item := range parsed.Items {
		require.NotEmpty(t, item.InstanceID)
		ids = append(ids, item.InstanceID)
	}
	return ids
}

// TestEC2_BlockDeviceMapping_ReportedRepro is #666's reproduction, verbatim: a
// launch naming a 60 GiB /dev/xvda, then DescribeVolumes filtered on the instance.
//
// Before the fix this returned an empty volumeSet — the mapping was parsed nowhere,
// so the size the caller asked for was not merely hidden but unreachable, AWS's
// EbsInstanceBlockDevice carrying no size member of its own.
//
// One volume, not two. /dev/xvda is a root device name, so the mapping configures
// the root rather than adding a device beside a synthesized one.
func TestEC2_BlockDeviceMapping_ReportedRepro(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "60",
	})
	require.Len(t, ids, 1)

	vols := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
	})
	require.Len(t, vols, 1)
	assert.Equal(t, 60, vols[0].Size)
	assert.Equal(t, "in-use", vols[0].Status)
	assert.Equal(t, "/dev/xvda", vols[0].bdmDevice())
	assert.Equal(t, ids[0], vols[0].Attachments[0].InstanceID)
	// /dev/xvda is a root device, and both AWS pages agree the root volume is deleted
	// on termination. Only the data-volume default was ever in doubt (#675), and it
	// is pinned by TestEC2_BlockDeviceMapping_DataVolumeDefaultPreserves.
	assert.True(t, vols[0].Attachments[0].DeleteOnTermination,
		"the root volume a launch creates is deleted on termination")
	assert.NotEmpty(t, vols[0].Attachments[0].AttachTime)
}

// TestEC2_BlockDeviceMapping_ImplicitRootVolume pins that a launch naming no mapping
// still produces a volume. A real instance always has a root volume, and
// DescribeImages already reports an 8 GiB /dev/sda1 mapping for every AMI substrate
// serves, so producing none left the two disagreeing about one instance.
func TestEC2_BlockDeviceMapping_ImplicitRootVolume(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, nil)

	vols := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
	})
	require.Len(t, vols, 1)
	assert.Equal(t, 8, vols[0].Size)
	assert.Equal(t, "gp2", vols[0].VolumeType)
	assert.Equal(t, "/dev/sda1", vols[0].bdmDevice())
	// The volume shares the instance's zone, since an attachment across zones is a
	// state real EC2 cannot reach.
	assert.Equal(t, "us-east-1a", vols[0].AvailabilityZone)
}

// TestEC2_BlockDeviceMapping_DataVolumeAlongsideRoot covers a data volume and the
// performance members that were stored and never rendered.
func TestEC2_BlockDeviceMapping_DataVolumeAlongsideRoot(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "100",
		"BlockDeviceMapping.1.Ebs.VolumeType": "gp3",
		"BlockDeviceMapping.1.Ebs.Iops":       "4000",
		"BlockDeviceMapping.1.Ebs.Throughput": "250",
		"BlockDeviceMapping.1.Ebs.Encrypted":  "true",
	})

	vols := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
	})
	require.Len(t, vols, 2, "the data volume plus a synthesized root")

	// Sorted by device: /dev/sda1 before /dev/sdf.
	root, data := vols[0], vols[1]
	assert.Equal(t, "/dev/sda1", root.bdmDevice())
	assert.Equal(t, 8, root.Size)

	assert.Equal(t, "/dev/sdf", data.bdmDevice())
	assert.Equal(t, 100, data.Size)
	assert.Equal(t, "gp3", data.VolumeType)
	assert.Equal(t, 4000, data.IOPS, "iops was stored and never rendered")
	assert.Equal(t, 250, data.Throughput, "throughput had nowhere to be stored at all")
	assert.True(t, data.Encrypted)
	assert.NotEqual(t, root.VolumeID, data.VolumeID)
}

// TestEC2_BlockDeviceMapping_TerminationHonorsDeleteOnTermination is the half that
// makes the parsing safe to land.
//
// Without it a volume would report deleteOnTermination=true while sitting in-use on
// a terminated instance forever — a state real EC2 never reaches, and a new
// inaccuracy in place of the old silence.
func TestEC2_BlockDeviceMapping_TerminationHonorsDeleteOnTermination(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		// The root deletes by default; the data volume preserves explicitly, which
		// since #675 is also its default — this case is about honoring the explicit
		// value, so it keeps stating it.
		"BlockDeviceMapping.1.DeviceName":              "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.VolumeSize":          "30",
		"BlockDeviceMapping.2.DeviceName":              "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize":          "40",
		"BlockDeviceMapping.2.Ebs.DeleteOnTermination": "false",
	})

	before := bdmDescribeVolumes(t, ts, nil)
	require.Len(t, before, 2)
	assert.True(t, before[0].Attachments[0].DeleteOnTermination, "/dev/sda1")
	assert.False(t, before[1].Attachments[0].DeleteOnTermination, "/dev/sdf")
	preservedID := before[1].VolumeID

	termResp := ec2Request(t, ts, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": ids[0],
	})
	require.Equal(t, http.StatusOK, termResp.StatusCode)
	require.NoError(t, termResp.Body.Close())

	// An unfiltered DescribeVolumes must not report the deleted one: its record left
	// the namespace DescribeVolumes lists, not merely its attachment.
	after := bdmDescribeVolumes(t, ts, nil)
	require.Len(t, after, 1)
	assert.Equal(t, preservedID, after[0].VolumeID)
	// A preserved volume is available with no attachment, which is what a volume
	// whose instance is gone looks like.
	assert.Equal(t, "available", after[0].Status)
	assert.Empty(t, after[0].Attachments)
	assert.Equal(t, 40, after[0].Size)
}

// TestEC2_BlockDeviceMapping_DataVolumeDefaultPreserves pins the default #675 is
// about, at the tier a consumer meets it: a launch-declared *data* volume that names
// no DeleteOnTermination survives its instance, while the root volume beside it does
// not.
//
// The default was untested through HTTP before this — every existing case either
// names a root device or sets the value explicitly — so the behavior #675 disputed
// was reachable only through the unit tier. It is the destructive direction that
// needs the assertion: a volume wrongly deleted is gone, and the caller finds out by
// its absence rather than by an error.
func TestEC2_BlockDeviceMapping_DataVolumeDefaultPreserves(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		// No DeleteOnTermination on either mapping: both take the default, and the
		// two defaults differ.
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "30",
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize": "40",
	})
	require.Len(t, ids, 1)

	before := bdmDescribeVolumes(t, ts, nil)
	require.Len(t, before, 2)
	assert.True(t, before[0].Attachments[0].DeleteOnTermination,
		"the root volume is deleted on termination — both AWS pages agree")
	assert.False(t, before[1].Attachments[0].DeleteOnTermination,
		"a data volume with no explicit value is preserved (#675)")
	dataVolumeID := before[1].VolumeID

	termResp := ec2Request(t, ts, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": ids[0],
	})
	require.Equal(t, http.StatusOK, termResp.StatusCode)
	require.NoError(t, termResp.Body.Close())

	// The rendered flag and what termination actually does have to agree; a volume
	// reporting false and then vanishing would be worse than either answer.
	after := bdmDescribeVolumes(t, ts, nil)
	require.Len(t, after, 1, "the root is deleted and the data volume is not")
	assert.Equal(t, dataVolumeID, after[0].VolumeID)
	assert.Equal(t, "available", after[0].Status)
	assert.Empty(t, after[0].Attachments)
	assert.Equal(t, 40, after[0].Size)
}

// TestEC2_BlockDeviceMapping_NoDeviceAndInstanceStore pins that neither a suppressed
// device nor an instance store device becomes an EBS volume — while the root is still
// synthesized, since neither entry was it.
func TestEC2_BlockDeviceMapping_NoDeviceAndInstanceStore(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":  "/dev/sdb",
		"BlockDeviceMapping.1.NoDevice":    "",
		"BlockDeviceMapping.2.DeviceName":  "/dev/sdc",
		"BlockDeviceMapping.2.VirtualName": "ephemeral0",
	})

	vols := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
	})
	require.Len(t, vols, 1)
	assert.Equal(t, "/dev/sda1", vols[0].bdmDevice())
}

// TestEC2_BlockDeviceMapping_PerInstanceIsolation covers a multi-count launch. Two
// instances cannot share one EBS volume, and terminating one must not touch the
// other's.
func TestEC2_BlockDeviceMapping_PerInstanceIsolation(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"MaxCount":                            "2",
		"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "50",
	})
	require.Len(t, ids, 2)

	first := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
	})
	second := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[1],
	})
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	assert.NotEqual(t, first[0].VolumeID, second[0].VolumeID)
	assert.Equal(t, 50, first[0].Size)
	assert.Equal(t, 50, second[0].Size)

	termResp := ec2Request(t, ts, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": ids[0],
	})
	require.Equal(t, http.StatusOK, termResp.StatusCode)
	require.NoError(t, termResp.Body.Close())

	remaining := bdmDescribeVolumes(t, ts, nil)
	require.Len(t, remaining, 1)
	assert.Equal(t, second[0].VolumeID, remaining[0].VolumeID)
}

// TestEC2_BlockDeviceMapping_FromLaunchTemplate covers the template path, which is
// the only route by which mappings reach a fleet launch: CreateFleet forwards the
// template reference rather than the caller's own mappings.
func TestEC2_BlockDeviceMapping_FromLaunchTemplate(t *testing.T) {
	ts := newEC2TestServer(t)
	ltResp := ec2Request(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "with-storage",
		"LaunchTemplateData.ImageId": "ami-0abcdef1234567890",
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize": "40",
	})
	require.Equal(t, http.StatusOK, ltResp.StatusCode)
	require.NoError(t, ltResp.Body.Close())

	t.Run("the template supplies the mappings", func(t *testing.T) {
		ids := bdmRunInstances(t, ts, map[string]string{
			"LaunchTemplate.LaunchTemplateName": "with-storage",
		})
		vols := bdmDescribeVolumes(t, ts, map[string]string{
			"Filter.1.Name":    "attachment.instance-id",
			"Filter.1.Value.1": ids[0],
		})
		require.Len(t, vols, 1)
		assert.Equal(t, 40, vols[0].Size)
		assert.Equal(t, "/dev/xvda", vols[0].bdmDevice())
	})

	t.Run("the request overrides the template wholesale", func(t *testing.T) {
		// All-or-nothing, the rule every other template field follows: a request that
		// names one mapping must not silently inherit a second.
		ids := bdmRunInstances(t, ts, map[string]string{
			"LaunchTemplate.LaunchTemplateName":   "with-storage",
			"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
			"BlockDeviceMapping.1.Ebs.VolumeSize": "90",
		})
		vols := bdmDescribeVolumes(t, ts, map[string]string{
			"Filter.1.Name":    "attachment.instance-id",
			"Filter.1.Value.1": ids[0],
		})
		require.Len(t, vols, 1)
		assert.Equal(t, 90, vols[0].Size)
	})
}

// TestEC2_BlockDeviceMapping_AttachVolumePreserves pins the other half of AWS's
// termination rule: a volume attached *after* launch is preserved, because deleting
// one the caller brought would destroy something the launch did not make.
func TestEC2_BlockDeviceMapping_AttachVolumePreserves(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, nil)

	cvResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             "15",
	})
	require.Equal(t, http.StatusOK, cvResp.StatusCode)
	cvBody, err := io.ReadAll(cvResp.Body)
	require.NoError(t, err)
	require.NoError(t, cvResp.Body.Close())
	var created struct {
		VolumeID string `xml:"volumeId"`
	}
	require.NoError(t, xml.Unmarshal(cvBody, &created))
	require.NotEmpty(t, created.VolumeID)

	attResp := ec2Request(t, ts, map[string]string{
		"Action":     "AttachVolume",
		"VolumeId":   created.VolumeID,
		"InstanceId": ids[0],
		"Device":     "/dev/sdh",
	})
	require.Equal(t, http.StatusOK, attResp.StatusCode)
	attBody, err := io.ReadAll(attResp.Body)
	require.NoError(t, err)
	require.NoError(t, attResp.Body.Close())
	var attached struct {
		DeleteOnTermination bool `xml:"deleteOnTermination"`
	}
	require.NoError(t, xml.Unmarshal(attBody, &attached))
	assert.False(t, attached.DeleteOnTermination,
		"AWS preserves a volume attached after launch")

	termResp := ec2Request(t, ts, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": ids[0],
	})
	require.Equal(t, http.StatusOK, termResp.StatusCode)
	require.NoError(t, termResp.Body.Close())

	// The brought volume survives; the launch's own root does not.
	after := bdmDescribeVolumes(t, ts, nil)
	require.Len(t, after, 1)
	assert.Equal(t, created.VolumeID, after[0].VolumeID)
	assert.Equal(t, "available", after[0].Status)
}

// TestEC2_BlockDeviceMapping_DescribeVolumesFilters covers each filter the new state
// makes worth asking about, selecting and excluding.
func TestEC2_BlockDeviceMapping_DescribeVolumesFilters(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":              "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize":          "30",
		"BlockDeviceMapping.2.DeviceName":              "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize":          "40",
		"BlockDeviceMapping.2.Ebs.VolumeType":          "gp3",
		"BlockDeviceMapping.2.Ebs.DeleteOnTermination": "false",
	})
	require.Len(t, ids, 1)

	tests := []struct {
		name       string
		filter     string
		value      string
		wantDevice []string
	}{
		{"attachment.device selects", "attachment.device", "/dev/sdf", []string{"/dev/sdf"}},
		{"attachment.device excludes", "attachment.device", "/dev/sdz", nil},
		{
			"delete-on-termination selects the preserved one",
			"attachment.delete-on-termination", "false", []string{"/dev/sdf"},
		},
		{
			"delete-on-termination selects the deleted one",
			"attachment.delete-on-termination", "true", []string{"/dev/xvda"},
		},
		{"volume-type selects", "volume-type", "gp3", []string{"/dev/sdf"}},
		{"volume-type selects the default", "volume-type", "gp2", []string{"/dev/xvda"}},
		{"volume-type excludes", "volume-type", "io2", nil},
		{"size selects", "size", "30", []string{"/dev/xvda"}},
		{"size excludes", "size", "999", nil},
		{
			"availability-zone selects both",
			"availability-zone", "us-east-1a", []string{"/dev/sdf", "/dev/xvda"},
		},
		{"availability-zone excludes", "availability-zone", "eu-west-1a", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vols := bdmDescribeVolumes(t, ts, map[string]string{
				"Filter.1.Name":    tt.filter,
				"Filter.1.Value.1": tt.value,
			})
			devices := make([]string, 0, len(vols))
			for _, v := range vols {
				devices = append(devices, v.bdmDevice())
			}
			if tt.wantDevice == nil {
				assert.Empty(t, devices)
				return
			}
			assert.Equal(t, tt.wantDevice, devices)
		})
	}
}
