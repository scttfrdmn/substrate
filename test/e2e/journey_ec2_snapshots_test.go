package e2e_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_EC2SnapshotRestore is #689's journey: create a volume, snapshot it, restore
// from the snapshot both ways a caller can, and read the size back.
//
// It belongs at this tier because the size is the whole point and the size is what a real
// SDK client reads. Snapshot.VolumeSize was the literal 8 at substrate's only producer, so
// every route a consumer had to "how big is this snapshot?" answered with a constant — and
// a restore from it produced an 8 GiB volume whatever the snapshot said. The HTTP-level
// tests pin the wire; this pins that a client's own types carry the value through.
func TestJourney_EC2SnapshotRestore(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	vol, err := client.CreateVolume(ctx, &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(40),
		Encrypted:        aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	volumeID := aws.ToString(vol.VolumeId)

	// TagSpecifications is a structured input the SDK serializes itself, so this proves
	// substrate reads the spelling the SDK sends rather than one chosen by hand.
	snap, err := client.CreateSnapshot(ctx, &ec2.CreateSnapshotInput{
		VolumeId:    aws.String(volumeID),
		Description: aws.String("nightly"),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeSnapshot,
			Tags:         []ec2types.Tag{{Key: aws.String("Scope"), Value: aws.String("nightly")}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snapshotID := aws.ToString(snap.SnapshotId)
	if snapshotID == "" {
		t.Fatal("CreateSnapshot returned an empty snapshot ID")
	}
	if got := aws.ToInt32(snap.VolumeSize); got != 40 {
		t.Errorf("CreateSnapshot VolumeSize = %d, want the source volume's 40", got)
	}
	if got := aws.ToString(snap.VolumeId); got != volumeID {
		t.Errorf("CreateSnapshot VolumeId = %q, want %q", got, volumeID)
	}
	// State is the SDK's name for the wire's `status` member. completed at once is
	// deliberate: substrate advances no snapshot asynchronously, so a waiter succeeds on
	// its first poll instead of depending on wall-clock time.
	if snap.State != ec2types.SnapshotStateCompleted {
		t.Errorf("CreateSnapshot State = %q, want completed", snap.State)
	}
	if !aws.ToBool(snap.Encrypted) {
		t.Error("a snapshot of an encrypted volume should be encrypted")
	}

	// DescribeSnapshots agrees, and the volume-size filter selects on the real value.
	described, err := client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
		Filters: []ec2types.Filter{{Name: aws.String("volume-size"), Values: []string{"40"}}},
	})
	if err != nil {
		t.Fatalf("DescribeSnapshots: %v", err)
	}
	if len(described.Snapshots) != 1 || aws.ToString(described.Snapshots[0].SnapshotId) != snapshotID {
		t.Fatalf("volume-size=40 selected %d snapshots, want just %s", len(described.Snapshots), snapshotID)
	}
	if got := aws.ToString(described.Snapshots[0].Description); got != "nightly" {
		t.Errorf("Description = %q, want %q", got, "nightly")
	}

	// Route one: CreateVolume with no Size takes the snapshot's.
	restored, err := client.CreateVolume(ctx, &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		SnapshotId:       aws.String(snapshotID),
	})
	if err != nil {
		t.Fatalf("CreateVolume from snapshot: %v", err)
	}
	if got := aws.ToInt32(restored.Size); got != 40 {
		t.Errorf("restored volume Size = %d, want the snapshot's 40", got)
	}

	// Route two: a launch's block device mapping with no size takes it too. The value is
	// observable only through DescribeVolumes — AWS's EbsInstanceBlockDevice, the shape
	// DescribeInstances renders per device, carries no size member.
	run, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/sdf"),
			Ebs:        &ec2types.EbsBlockDevice{SnapshotId: aws.String(snapshotID)},
		}},
	})
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	instanceID := aws.ToString(run.Instances[0].InstanceId)

	launched, err := client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("attachment.instance-id"), Values: []string{instanceID}},
			{Name: aws.String("attachment.device"), Values: []string{"/dev/sdf"}},
		},
	})
	if err != nil {
		t.Fatalf("DescribeVolumes: %v", err)
	}
	if len(launched.Volumes) != 1 {
		t.Fatalf("DescribeVolumes returned %d volumes for /dev/sdf, want 1", len(launched.Volumes))
	}
	if got := aws.ToInt32(launched.Volumes[0].Size); got != 40 {
		t.Errorf("launch-created volume Size = %d, want the snapshot's 40", got)
	}
}

// TestJourney_EC2SnapshotRefusals is the other half: the two mistakes a consumer's retry
// loop has to tell apart, each read through the SDK's own error shape.
//
// A snapshot that does not exist is an InvalidSnapshot.NotFound about the ID — which is
// what naming a snapshot from a previous run looks like — while a size below the
// snapshot's is an InvalidBlockDeviceMapping about the mapping. Before #689 both
// succeeded and produced an 8 GiB volume.
func TestJourney_EC2SnapshotRefusals(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	vol, err := client.CreateVolume(ctx, &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(30),
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	snap, err := client.CreateSnapshot(ctx, &ec2.CreateSnapshotInput{VolumeId: vol.VolumeId})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	launch := func(bdm ec2types.BlockDeviceMapping) error {
		_, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
			ImageId:             aws.String("ami-0abcdef1234567890"),
			InstanceType:        ec2types.InstanceTypeT3Micro,
			MinCount:            aws.Int32(1),
			MaxCount:            aws.Int32(1),
			BlockDeviceMappings: []ec2types.BlockDeviceMapping{bdm},
		})
		return err
	}

	for _, tc := range []struct {
		name string
		bdm  ec2types.BlockDeviceMapping
		code string
	}{
		{
			name: "a snapshot no account holds",
			bdm: ec2types.BlockDeviceMapping{
				DeviceName: aws.String("/dev/sdf"),
				Ebs:        &ec2types.EbsBlockDevice{SnapshotId: aws.String("snap-0123456789abcdef0")},
			},
			code: "InvalidSnapshot.NotFound",
		},
		{
			name: "a size below the snapshot's",
			bdm: ec2types.BlockDeviceMapping{
				DeviceName: aws.String("/dev/sdf"),
				Ebs: &ec2types.EbsBlockDevice{
					SnapshotId: snap.SnapshotId,
					VolumeSize: aws.Int32(10),
				},
			},
			code: "InvalidBlockDeviceMapping",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := launch(tc.bdm)
			if err == nil {
				t.Fatalf("RunInstances succeeded, want %s", tc.code)
			}
			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("error is not an APIError: %v", err)
			}
			if apiErr.ErrorCode() != tc.code {
				t.Errorf("code = %q, want %q (message %q)",
					apiErr.ErrorCode(), tc.code, apiErr.ErrorMessage())
			}
		})
	}

	// And CreateSnapshot itself refuses a volume it cannot find, so a snapshot cannot
	// exist for a volume that does not.
	_, err = client.CreateSnapshot(ctx, &ec2.CreateSnapshotInput{
		VolumeId: aws.String("vol-0123456789abcdef0"),
	})
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("CreateSnapshot on an unknown volume: error is not an APIError: %v", err)
	}
	if apiErr.ErrorCode() != "InvalidVolume.NotFound" {
		t.Errorf("code = %q, want InvalidVolume.NotFound", apiErr.ErrorCode())
	}
}
