package e2e_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_EC2LaunchBlockDeviceMappings is #666's journey, and it belongs at this
// tier because this tier is where the issue was reported from: a consumer launching an
// instance with a 60 GiB root volume, asking DescribeVolumes about it, and getting
// nothing back. test/e2e had no EBS coverage at all, which is part of why.
//
// The SDK is the right reader for it. BlockDeviceMappings is a structured input the
// SDK serializes into BlockDeviceMapping.1.Ebs.VolumeSize itself, so a hand-built form
// body proves only that substrate parses the spelling this test's author chose. And
// Volume.Size is where the value has to land: AWS's EbsInstanceBlockDevice — the shape
// DescribeInstances renders per device — carries no size member, so DescribeVolumes is
// the sole API surface on which a launch-specified size is observable.
func TestJourney_EC2LaunchBlockDeviceMappings(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	run, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/xvda"),
			Ebs:        &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(60)},
		}},
	})
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	if len(run.Instances) != 1 {
		t.Fatalf("RunInstances returned %d instances, want 1", len(run.Instances))
	}
	instanceID := aws.ToString(run.Instances[0].InstanceId)
	if instanceID == "" {
		t.Fatal("RunInstances returned an empty instance ID")
	}

	// Filtered on the instance rather than listing everything, so the assertion holds
	// whatever else the emulator happens to hold.
	byInstance := &ec2.DescribeVolumesInput{
		Filters: []ec2types.Filter{{
			Name:   aws.String("attachment.instance-id"),
			Values: []string{instanceID},
		}},
	}
	described, err := client.DescribeVolumes(ctx, byInstance)
	if err != nil {
		t.Fatalf("DescribeVolumes: %v", err)
	}
	// One volume, not two: /dev/xvda is a root device name, so the mapping configures
	// the root volume rather than adding a device beside a synthesized one.
	if len(described.Volumes) != 1 {
		t.Fatalf("DescribeVolumes returned %d volumes, want 1", len(described.Volumes))
	}
	vol := described.Volumes[0]
	if got := aws.ToInt32(vol.Size); got != 60 {
		t.Fatalf("Volume.Size = %d, want 60 — the size the launch asked for", got)
	}
	if vol.State != ec2types.VolumeStateInUse {
		t.Fatalf("Volume.State = %q, want in-use", vol.State)
	}
	if len(vol.Attachments) != 1 {
		t.Fatalf("Volume has %d attachments, want 1", len(vol.Attachments))
	}
	att := vol.Attachments[0]
	if got := aws.ToString(att.Device); got != "/dev/xvda" {
		t.Fatalf("attachment Device = %q, want /dev/xvda", got)
	}
	if got := aws.ToString(att.InstanceId); got != instanceID {
		t.Fatalf("attachment InstanceId = %q, want %q", got, instanceID)
	}
	// True is AWS's launch default for a volume created through the API — its
	// termination table splits on how the volume was attached, and the console path a
	// consumer might expect Preserve from does not exist here.
	if !aws.ToBool(att.DeleteOnTermination) {
		t.Fatal("attachment DeleteOnTermination = false, want true for a launch-created volume")
	}

	if _, err := client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	}); err != nil {
		t.Fatalf("TerminateInstances: %v", err)
	}

	// Parsing the mapping without honoring termination would leave this volume
	// reporting in-use on a terminated instance forever, which is a state real EC2
	// never reaches.
	after, err := client.DescribeVolumes(ctx, byInstance)
	if err != nil {
		t.Fatalf("DescribeVolumes after terminate: %v", err)
	}
	if len(after.Volumes) != 0 {
		t.Fatalf("DescribeVolumes after terminate returned %d volumes, want 0",
			len(after.Volumes))
	}
}
