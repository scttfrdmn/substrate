package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #666: RunInstances accepted BlockDeviceMapping.N.* and parsed none of it, so a
// launch naming a 60 GiB root volume succeeded and DescribeVolumes reported nothing
// for the instance. Since AWS's EbsInstanceBlockDevice carries no size member,
// DescribeVolumes is the only API surface where a launch-specified size is
// observable at all — the value was not merely hidden, it was unreachable.
//
// These tests cover the parser and the materializer in isolation; the launch,
// termination and filter behavior they feed is covered through HTTP in
// ec2_plugin_test.go.

func TestEC2ParseBlockDeviceMappings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		prefix string
		params map[string]string
		want   []emulator.EC2BlockDeviceMapping
	}{
		{
			name:   "no mappings",
			params: map[string]string{"ImageId": "ami-1"},
			want:   nil,
		},
		{
			// The issue's own repro.
			name: "the reported repro",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/xvda",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "60",
			},
			want: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/xvda", VolumeSize: 60},
			},
		},
		{
			name: "every ebs member",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":              "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.SnapshotId":          "snap-abc",
				"BlockDeviceMapping.1.Ebs.VolumeSize":          "100",
				"BlockDeviceMapping.1.Ebs.VolumeType":          "gp3",
				"BlockDeviceMapping.1.Ebs.Iops":                "4000",
				"BlockDeviceMapping.1.Ebs.Throughput":          "250",
				"BlockDeviceMapping.1.Ebs.Encrypted":           "true",
				"BlockDeviceMapping.1.Ebs.DeleteOnTermination": "false",
			},
			want: []emulator.EC2BlockDeviceMapping{{
				DeviceName:          "/dev/sdf",
				SnapshotID:          "snap-abc",
				VolumeSize:          100,
				VolumeType:          "gp3",
				IOPS:                4000,
				Throughput:          250,
				Encrypted:           true,
				DeleteOnTermination: "false",
			}},
		},
		{
			// Every index is read, not just the first — [ec2ParseNetworkInterfaces]'
			// own contiguous-from-1 rule, and the bug #455 fixed for interfaces.
			name: "three contiguous mappings",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "30",
				"BlockDeviceMapping.2.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.2.Ebs.VolumeSize": "40",
				"BlockDeviceMapping.3.DeviceName":     "/dev/sdg",
				"BlockDeviceMapping.3.Ebs.VolumeSize": "50",
			},
			want: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sda1", VolumeSize: 30},
				{DeviceName: "/dev/sdf", VolumeSize: 40},
				{DeviceName: "/dev/sdg", VolumeSize: 50},
			},
		},
		{
			// A gap terminates the scan, which is the convention every other indexed
			// reader here follows.
			name: "a gap stops the scan",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName": "/dev/sdf",
				"BlockDeviceMapping.3.DeviceName": "/dev/sdh",
			},
			want: []emulator.EC2BlockDeviceMapping{{DeviceName: "/dev/sdf"}},
		},
		{
			// An index naming only an Ebs member and no device name is still present:
			// requiring a chosen member would drop a mapping for spelling reasons.
			name: "size alone is present",
			params: map[string]string{
				"BlockDeviceMapping.1.Ebs.VolumeSize": "25",
			},
			want: []emulator.EC2BlockDeviceMapping{{VolumeSize: 25}},
		},
		{
			// NoDevice is an empty-string member on the wire, so its presence is the
			// signal. An index naming only NoDevice must still be seen.
			name: "no device with an empty value",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName": "/dev/sdb",
				"BlockDeviceMapping.1.NoDevice":   "",
			},
			want: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sdb", NoDevice: true},
			},
		},
		{
			name: "instance store",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":  "/dev/sdb",
				"BlockDeviceMapping.1.VirtualName": "ephemeral0",
			},
			want: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sdb", VirtualName: "ephemeral0"},
			},
		},
		{
			// The launch-template prefix, which is the whole reason the parser takes
			// one: a template and a request parse the same shape by the same rules.
			name:   "launch template prefix",
			prefix: "LaunchTemplateData.",
			params: map[string]string{
				"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
				"LaunchTemplateData.BlockDeviceMapping.1.Ebs.VolumeSize": "40",
				// A same-named parameter without the prefix must not leak in.
				"BlockDeviceMapping.1.DeviceName": "/dev/sdz",
			},
			want: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sda1", VolumeSize: 40},
			},
		},
		{
			// An unparseable number reads as zero rather than failing the launch, the
			// same tolerance ec2ParseNetworkInterface gives DeviceIndex. The index is
			// still present, so the mapping is not dropped.
			name: "unparseable size",
			params: map[string]string{
				"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
				"BlockDeviceMapping.1.Ebs.VolumeSize": "not-a-number",
			},
			want: []emulator.EC2BlockDeviceMapping{{DeviceName: "/dev/sdf"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := emulator.ParseBlockDeviceMappingsForTest(tt.params, tt.prefix)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestEC2LaunchVolumes_AlwaysHasARoot pins the decision that every launch
// materializes at least one volume.
//
// A real instance always has a root volume whether or not the request mentions one,
// and DescribeImages already reports an 8 GiB /dev/sda1 mapping for every AMI
// substrate serves. A launch that produced no volume at all would leave the two
// disagreeing about the same instance.
func TestEC2LaunchVolumes_AlwaysHasARoot(t *testing.T) {
	t.Parallel()
	vols := emulator.LaunchVolumesForTest("i-1", "us-west-2b", nil)
	require.Len(t, vols, 1)
	assert.Equal(t, 8, vols[0].Size)
	assert.Equal(t, "gp2", vols[0].VolumeType)
	assert.Equal(t, "in-use", vols[0].State)
	// The zone is the instance's, not the region's first: a volume must be in the
	// same zone as the instance it is attached to.
	assert.Equal(t, "us-west-2b", vols[0].AvailabilityZone)
	require.Len(t, vols[0].Attachments, 1)
	assert.Equal(t, "/dev/sda1", vols[0].Attachments[0].Device)
	assert.Equal(t, "i-1", vols[0].Attachments[0].InstanceID)
	assert.True(t, vols[0].Attachments[0].DeleteOnTermination)
}

func TestEC2LaunchVolumes(t *testing.T) {
	t.Parallel()
	type wantVol struct {
		device              string
		size                int
		volumeType          string
		deleteOnTermination bool
	}
	tests := []struct {
		name     string
		mappings []emulator.EC2BlockDeviceMapping
		want     []wantVol
	}{
		{
			// Both root device names configure the root rather than adding a device.
			// AWS's device-naming reference gives the HVM root as "Differs by AMI —
			// /dev/sda1 or /dev/xvda", and the issue's repro uses /dev/xvda: a rule
			// that recognized only /dev/sda1 would answer that repro with two volumes,
			// a 60 GiB /dev/xvda alongside a spurious 8 GiB root.
			name:     "xvda configures the root",
			mappings: []emulator.EC2BlockDeviceMapping{{DeviceName: "/dev/xvda", VolumeSize: 60}},
			want:     []wantVol{{"/dev/xvda", 60, "gp2", true}},
		},
		{
			name:     "sda1 configures the root",
			mappings: []emulator.EC2BlockDeviceMapping{{DeviceName: "/dev/sda1", VolumeSize: 30}},
			want:     []wantVol{{"/dev/sda1", 30, "gp2", true}},
		},
		{
			// A data volume does not suppress the root, so a launch naming only
			// /dev/sdf gets two volumes.
			name:     "a data volume gets a root alongside it",
			mappings: []emulator.EC2BlockDeviceMapping{{DeviceName: "/dev/sdf", VolumeSize: 100, VolumeType: "gp3"}},
			want: []wantVol{
				{"/dev/sdf", 100, "gp3", true},
				{"/dev/sda1", 8, "gp2", true},
			},
		},
		{
			// DeleteOnTermination defaults to true for a *data* volume too. Counter-
			// intuitive, and AWS's own behavior: its termination table splits on how
			// the volume was attached, not on what it is — a data volume attached at
			// launch through the console preserves, but through the API it is deleted,
			// and substrate has no console.
			name: "an explicit false preserves",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sdf", VolumeSize: 20, DeleteOnTermination: "false"},
			},
			want: []wantVol{
				{"/dev/sdf", 20, "gp2", false},
				{"/dev/sda1", 8, "gp2", true},
			},
		},
		{
			name: "an explicit true is honored",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sda1", VolumeSize: 20, DeleteOnTermination: "true"},
			},
			want: []wantVol{{"/dev/sda1", 20, "gp2", true}},
		},
		{
			// NoDevice suppresses the device outright, so nothing is created for it —
			// and the root is still synthesized, since the suppressed entry was not it.
			name: "no device creates nothing",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sdb", NoDevice: true},
			},
			want: []wantVol{{"/dev/sda1", 8, "gp2", true}},
		},
		{
			// An instance store device is not an EBS volume and has no presence in
			// DescribeVolumes. Manufacturing one would be an observation nothing backs.
			name: "instance store creates nothing",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sdb", VirtualName: "ephemeral0"},
			},
			want: []wantVol{{"/dev/sda1", 8, "gp2", true}},
		},
		{
			// A VirtualName alongside real Ebs members is a contradictory mapping AWS
			// would refuse; substrate honors the EBS half rather than silently dropping
			// a volume the caller sized.
			name: "virtual name with ebs members still creates",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sdb", VirtualName: "ephemeral0", VolumeSize: 12},
			},
			want: []wantVol{
				{"/dev/sdb", 12, "gp2", true},
				{"/dev/sda1", 8, "gp2", true},
			},
		},
		{
			// NoDevice wins over a size: it means "there is no such device".
			name: "no device wins over a size",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/sda1", VolumeSize: 50, NoDevice: true},
			},
			want: []wantVol{{"/dev/sda1", 8, "gp2", true}},
		},
		{
			name: "a root and two data volumes",
			mappings: []emulator.EC2BlockDeviceMapping{
				{DeviceName: "/dev/xvda", VolumeSize: 60},
				{DeviceName: "/dev/sdf", VolumeSize: 100, VolumeType: "io2"},
				{DeviceName: "/dev/sdg", VolumeSize: 200, DeleteOnTermination: "false"},
			},
			want: []wantVol{
				{"/dev/xvda", 60, "gp2", true},
				{"/dev/sdf", 100, "io2", true},
				{"/dev/sdg", 200, "gp2", false},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vols := emulator.LaunchVolumesForTest("i-abc", "us-east-1a", tt.mappings)
			require.Len(t, vols, len(tt.want))
			ids := map[string]bool{}
			for i, want := range tt.want {
				require.Len(t, vols[i].Attachments, 1, "volume %d", i)
				assert.Equal(t, want.device, vols[i].Attachments[0].Device)
				assert.Equal(t, want.size, vols[i].Size)
				assert.Equal(t, want.volumeType, vols[i].VolumeType)
				assert.Equal(t, want.deleteOnTermination, vols[i].Attachments[0].DeleteOnTermination)
				// Every volume gets its own ID; two devices cannot be one volume.
				assert.False(t, ids[vols[i].VolumeID], "duplicate volume ID")
				ids[vols[i].VolumeID] = true
			}
		})
	}
}

// TestEC2LaunchVolumes_CarriesPerformanceMembers pins that iops, throughput,
// encryption and the snapshot reach the volume, which is the only place they are
// observable — the attachment shape AWS renders on an instance carries none of them.
func TestEC2LaunchVolumes_CarriesPerformanceMembers(t *testing.T) {
	t.Parallel()
	vols := emulator.LaunchVolumesForTest("i-1", "us-east-1a",
		[]emulator.EC2BlockDeviceMapping{{
			DeviceName: "/dev/sda1",
			VolumeSize: 100,
			VolumeType: "gp3",
			IOPS:       4000,
			Throughput: 250,
			Encrypted:  true,
			SnapshotID: "snap-abc",
		}})
	require.Len(t, vols, 1)
	assert.Equal(t, 4000, vols[0].IOPS)
	assert.Equal(t, 250, vols[0].Throughput)
	assert.True(t, vols[0].Encrypted)
	assert.Equal(t, "snap-abc", vols[0].SnapshotID)
}
