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

// #669's surfaces: the instance's own view of its block devices, on the three
// operations that render one.
//
// The set is derived from the volume records rather than from the recorded request,
// which is what makes a detached volume stop appearing and an attached one start —
// and it is also why there is nothing to fabricate: every member AWS's
// EbsInstanceBlockDevice shows is already a field on the attachment.

// instanceBDM is one blockDeviceMapping>item as an instance renders it.
type instanceBDM struct {
	DeviceName string `xml:"deviceName"`
	Ebs        struct {
		VolumeID            string `xml:"volumeId"`
		Status              string `xml:"status"`
		AttachTime          string `xml:"attachTime"`
		DeleteOnTermination bool   `xml:"deleteOnTermination"`
	} `xml:"ebs"`
}

// instanceBDMDescribeInstances returns the mappings DescribeInstances reports for one
// instance, in response order.
func instanceBDMDescribeInstances(t *testing.T, ts *httptest.Server, instanceID string) []instanceBDM {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instanceID,
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		Instances []struct {
			InstanceID string        `xml:"instanceId"`
			Mappings   []instanceBDM `xml:"blockDeviceMapping>item"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	require.Len(t, parsed.Instances, 1)
	require.Equal(t, instanceID, parsed.Instances[0].InstanceID)
	return parsed.Instances[0].Mappings
}

// instanceBDMDevices reduces a mapping set to its device names, for the assertions
// that are about which devices appear rather than about their contents.
func instanceBDMDevices(mappings []instanceBDM) []string {
	devices := make([]string, 0, len(mappings))
	for _, m := range mappings {
		devices = append(devices, m.DeviceName)
	}
	return devices
}

// TestEC2_InstanceBlockDeviceMapping_DescribeInstances is #669's first surface. A
// caller had to go to DescribeVolumes and filter on attachment.instance-id to learn a
// device's volume ID; real EC2 answers it from the instance.
func TestEC2_InstanceBlockDeviceMapping_DescribeInstances(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "30",
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize": "40",
	})
	require.Len(t, ids, 1)

	mappings := instanceBDMDescribeInstances(t, ts, ids[0])
	require.Len(t, mappings, 2)
	assert.Equal(t, []string{"/dev/sdf", "/dev/xvda"}, instanceBDMDevices(mappings),
		"/dev/xvda configures the root rather than adding a device beside a synthesized "+
			"one, and it keeps the name the request gave it")

	// The volume IDs must be the ones DescribeVolumes reports for the same instance:
	// two surfaces disagreeing about which volume is on a device would be worse than
	// one of them being silent.
	vols := bdmDescribeVolumes(t, ts, map[string]string{
		"Filter.1.Name":    "attachment.instance-id",
		"Filter.1.Value.1": ids[0],
	})
	require.Len(t, vols, 2)
	byDevice := map[string]string{}
	for _, v := range vols {
		byDevice[v.bdmDevice()] = v.VolumeID
	}
	for _, m := range mappings {
		assert.Equal(t, byDevice[m.DeviceName], m.Ebs.VolumeID,
			"device %s", m.DeviceName)
		assert.Equal(t, "attached", m.Ebs.Status,
			"EbsInstanceBlockDevice.status uses the four-value AttachmentStatus enum")
		assert.NotEmpty(t, m.Ebs.AttachTime)
	}

	// The two DeleteOnTermination defaults differ (#675), and this surface is where a
	// caller most naturally reads them.
	assert.False(t, mappings[0].Ebs.DeleteOnTermination, "/dev/sdf is a data volume")
	assert.True(t, mappings[1].Ebs.DeleteOnTermination, "/dev/xvda is the root")
}

// TestEC2_InstanceBlockDeviceMapping_RunInstances covers the same set on the launch
// response.
//
// This one is a deliberate divergence from AWS's only RunInstances sample response,
// which emits <blockDeviceMapping /> empty on a pending instance whose request
// declared no mappings at all. Substrate's instances are running by the time
// RunInstances answers and their volumes already exist — the same argument
// networkInterfaceSet is rendered on.
func TestEC2_InstanceBlockDeviceMapping_RunInstances(t *testing.T) {
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{
		"Action":                              "RunInstances",
		"ImageId":                             ec2TestImage,
		"MinCount":                            "1",
		"MaxCount":                            "1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "40",
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		Instances []struct {
			InstanceID string        `xml:"instanceId"`
			Mappings   []instanceBDM `xml:"blockDeviceMapping>item"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	require.Len(t, parsed.Instances, 1)

	mappings := parsed.Instances[0].Mappings
	require.Len(t, mappings, 2, "the data volume plus the synthesized root")
	assert.Equal(t, []string{"/dev/sda1", "/dev/sdf"}, instanceBDMDevices(mappings))
	for _, m := range mappings {
		assert.NotEmpty(t, m.Ebs.VolumeID, "device %s", m.DeviceName)
	}

	// And the launch response agrees with the describe that follows it, which is the
	// drift #444 was about.
	described := instanceBDMDescribeInstances(t, ts, parsed.Instances[0].InstanceID)
	assert.Equal(t, mappings, described,
		"RunInstances and DescribeInstances must report one instance the same way")
}

// TestEC2_InstanceBlockDeviceMapping_Attribute covers the third surface, which used to
// refuse the question outright.
func TestEC2_InstanceBlockDeviceMapping_Attribute(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "40",
	})

	resp := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeInstanceAttribute",
		"InstanceId": ids[0],
		"Attribute":  "blockDeviceMapping",
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var parsed struct {
		InstanceID string        `xml:"instanceId"`
		Mappings   []instanceBDM `xml:"blockDeviceMapping>item"`
		// The other attributes must be absent: AWS returns one attribute per call.
		UserData     *struct{} `xml:"userData"`
		InstanceType *struct{} `xml:"instanceType"`
		Groups       *struct{} `xml:"groupSet"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), string(body))
	assert.Equal(t, ids[0], parsed.InstanceID)
	require.Len(t, parsed.Mappings, 2)
	assert.Equal(t, []string{"/dev/sda1", "/dev/sdf"}, instanceBDMDevices(parsed.Mappings))
	assert.Nil(t, parsed.UserData, "userData must not appear in a blockDeviceMapping response")
	assert.Nil(t, parsed.InstanceType)
	assert.Nil(t, parsed.Groups, "groupSet's element must stay absent")

	// A blockDeviceMapping response does not carry a <value> wrapper: it is one of the
	// InstanceAttribute members that is not an AttributeValue.
	assert.NotContains(t, string(body), "<value>")
}

// TestEC2_InstanceBlockDeviceMapping_InstanceStoreAndNoDevice pins that neither a
// suppressed device nor an instance store device appears, matching what #666 decided
// for DescribeVolumes. Deriving the set from the volumes is what makes that automatic:
// neither ever became a volume.
func TestEC2_InstanceBlockDeviceMapping_InstanceStoreAndNoDevice(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":  "/dev/sdb",
		"BlockDeviceMapping.1.NoDevice":    "",
		"BlockDeviceMapping.2.DeviceName":  "/dev/sdc",
		"BlockDeviceMapping.2.VirtualName": "ephemeral0",
	})

	mappings := instanceBDMDescribeInstances(t, ts, ids[0])
	assert.Equal(t, []string{"/dev/sda1"}, instanceBDMDevices(mappings),
		"only the synthesized root is an EBS volume")
}

// TestEC2_InstanceBlockDeviceMapping_TracksAttachments is the criterion that decides
// the whole design: the set comes from the volumes, not from the launch request. So a
// volume attached after launch appears and one detached stops appearing, neither of
// which the recorded mappings could ever express.
func TestEC2_InstanceBlockDeviceMapping_TracksAttachments(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, nil)

	cvResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             "15",
	})
	cvBody, err := io.ReadAll(cvResp.Body)
	require.NoError(t, err)
	require.NoError(t, cvResp.Body.Close())
	require.Equal(t, http.StatusOK, cvResp.StatusCode, string(cvBody))
	var created struct {
		VolumeID string `xml:"volumeId"`
	}
	require.NoError(t, xml.Unmarshal(cvBody, &created))
	require.NotEmpty(t, created.VolumeID)

	assert.Equal(t, []string{"/dev/sda1"},
		instanceBDMDevices(instanceBDMDescribeInstances(t, ts, ids[0])),
		"a volume that exists but is not attached is not the instance's device")

	attResp := ec2Request(t, ts, map[string]string{
		"Action":     "AttachVolume",
		"VolumeId":   created.VolumeID,
		"InstanceId": ids[0],
		"Device":     "/dev/sdh",
	})
	require.Equal(t, http.StatusOK, attResp.StatusCode)
	require.NoError(t, attResp.Body.Close())

	afterAttach := instanceBDMDescribeInstances(t, ts, ids[0])
	require.Len(t, afterAttach, 2)
	assert.Equal(t, []string{"/dev/sda1", "/dev/sdh"}, instanceBDMDevices(afterAttach))
	var attachedItem instanceBDM
	for _, m := range afterAttach {
		if m.DeviceName == "/dev/sdh" {
			attachedItem = m
		}
	}
	assert.Equal(t, created.VolumeID, attachedItem.Ebs.VolumeID)
	assert.False(t, attachedItem.Ebs.DeleteOnTermination,
		"a volume attached after launch is preserved, and the instance reports that")

	detResp := ec2Request(t, ts, map[string]string{
		"Action":   "DetachVolume",
		"VolumeId": created.VolumeID,
	})
	require.Equal(t, http.StatusOK, detResp.StatusCode)
	require.NoError(t, detResp.Body.Close())

	assert.Equal(t, []string{"/dev/sda1"},
		instanceBDMDevices(instanceBDMDescribeInstances(t, ts, ids[0])),
		"a detached volume stops appearing")
}

// TestEC2_InstanceBlockDeviceMapping_PerInstance pins that a multi-count launch does
// not let one instance report another's devices — the bucketing bug a per-instance
// filter would hide, since each instance's volumes are read out of one list.
func TestEC2_InstanceBlockDeviceMapping_PerInstance(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"MaxCount":                            "2",
		"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "50",
	})
	require.Len(t, ids, 2)

	seen := map[string]bool{}
	for _, id := range ids {
		mappings := instanceBDMDescribeInstances(t, ts, id)
		require.Len(t, mappings, 1, "instance %s", id)
		assert.Equal(t, "/dev/xvda", mappings[0].DeviceName)
		assert.False(t, seen[mappings[0].Ebs.VolumeID],
			"two instances cannot report the same volume")
		seen[mappings[0].Ebs.VolumeID] = true
	}
}

// TestEC2_InstanceBlockDeviceMapping_EmptySetIsAbsent covers the shape of an instance
// with no EBS volume at all. Every launch produces a root volume, so the only way to
// reach it is to terminate — and a terminated instance's mappings are gone with its
// volumes.
func TestEC2_InstanceBlockDeviceMapping_EmptySetIsAbsent(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, nil)

	termResp := ec2Request(t, ts, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": ids[0],
	})
	require.Equal(t, http.StatusOK, termResp.StatusCode)
	require.NoError(t, termResp.Body.Close())

	assert.Empty(t, instanceBDMDescribeInstances(t, ts, ids[0]),
		"the root volume was deleted with the instance, so it reports no devices")

	// The attribute surface answers the same instance with a present-but-empty
	// element, which is the shape groupSet and userData already use for "nothing to
	// report": an SDK maps a present-but-empty element to an empty slice and an
	// omitted one to nil.
	resp := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeInstanceAttribute",
		"InstanceId": ids[0],
		"Attribute":  "blockDeviceMapping",
	})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Contains(t, string(body), "<blockDeviceMapping>",
		"the element is present even with no devices to list")
}

// TestEC2_InstanceBlockDeviceMapping_DeterministicOrder pins that the set comes back
// in a stable order.
//
// AWS states outright that its order may vary, so no fidelity claim rests on this —
// but a deterministic emulator answering one request two ways would break the
// guarantee the project exists for, and a consumer indexing the set would flake.
func TestEC2_InstanceBlockDeviceMapping_DeterministicOrder(t *testing.T) {
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdz",
		"BlockDeviceMapping.1.Ebs.VolumeSize": "10",
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize": "20",
		"BlockDeviceMapping.3.DeviceName":     "/dev/xvda",
		"BlockDeviceMapping.3.Ebs.VolumeSize": "30",
	})

	first := instanceBDMDevices(instanceBDMDescribeInstances(t, ts, ids[0]))
	require.Len(t, first, 3)
	sorted := append([]string(nil), first...)
	sort.Strings(sorted)
	assert.Equal(t, sorted, first, "the set is ordered by device name")

	for range 3 {
		assert.Equal(t, first,
			instanceBDMDevices(instanceBDMDescribeInstances(t, ts, ids[0])),
			"the same request must answer the same way")
	}
}
