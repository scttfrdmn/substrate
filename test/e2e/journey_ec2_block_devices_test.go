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
	// True for the *root* volume, which is what /dev/xvda is. Both AWS pages that
	// document a launch default agree about the root; they disagree about a data
	// volume, and substrate preserves that one (#675).
	if !aws.ToBool(att.DeleteOnTermination) {
		t.Fatal("attachment DeleteOnTermination = false, want true for a launch-created root volume")
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

// TestJourney_EC2LaunchDataVolumeSurvivesTermination is #675 at the tier that made it
// worth deciding: a consumer whose IaC attaches a data volume at launch and expects to
// still have it afterwards.
//
// Through v0.104.0 substrate deleted it, following the one AWS page whose console-vs-CLI
// table says so. The page that describes the mapping shape a consumer is actually
// writing says the opposite, both pages agree the real default comes from an AMI mapping
// substrate does not carry, and a volume wrongly deleted is gone — so the split default
// wins, and this is the direction that needs an SDK-tier assertion.
func TestJourney_EC2LaunchDataVolumeSurvivesTermination(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	// Neither mapping names DeleteOnTermination, so both take a default and the two
	// defaults differ.
	run, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{
			{
				DeviceName: aws.String("/dev/xvda"),
				Ebs:        &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(30)},
			},
			{
				DeviceName: aws.String("/dev/sdf"),
				Ebs:        &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(40)},
			},
		},
	})
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	if len(run.Instances) != 1 {
		t.Fatalf("RunInstances returned %d instances, want 1", len(run.Instances))
	}
	instanceID := aws.ToString(run.Instances[0].InstanceId)

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
	if len(described.Volumes) != 2 {
		t.Fatalf("DescribeVolumes returned %d volumes, want 2", len(described.Volumes))
	}
	var dataVolumeID string
	for _, vol := range described.Volumes {
		if len(vol.Attachments) != 1 {
			t.Fatalf("volume %s has %d attachments, want 1",
				aws.ToString(vol.VolumeId), len(vol.Attachments))
		}
		att := vol.Attachments[0]
		device := aws.ToString(att.Device)
		deleteOnTermination := aws.ToBool(att.DeleteOnTermination)
		switch device {
		case "/dev/xvda":
			if !deleteOnTermination {
				t.Error("the root volume reports DeleteOnTermination=false, want true")
			}
		case "/dev/sdf":
			if deleteOnTermination {
				t.Error("the data volume reports DeleteOnTermination=true, want false (#675)")
			}
			dataVolumeID = aws.ToString(vol.VolumeId)
		default:
			t.Errorf("unexpected device %q", device)
		}
	}
	if dataVolumeID == "" {
		t.Fatal("the launch produced no /dev/sdf volume")
	}

	if _, err := client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	}); err != nil {
		t.Fatalf("TerminateInstances: %v", err)
	}

	// What the flag reports and what termination does have to agree. Listing
	// everything rather than filtering on the instance: a preserved volume has no
	// attachment left to filter on, which is itself the observation.
	all, err := client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{})
	if err != nil {
		t.Fatalf("DescribeVolumes after terminate: %v", err)
	}
	if len(all.Volumes) != 1 {
		t.Fatalf("after terminate there are %d volumes, want 1 — the preserved data volume",
			len(all.Volumes))
	}
	if got := aws.ToString(all.Volumes[0].VolumeId); got != dataVolumeID {
		t.Fatalf("the surviving volume is %s, want the data volume %s", got, dataVolumeID)
	}
	if all.Volumes[0].State != ec2types.VolumeStateAvailable {
		t.Fatalf("the preserved volume is %q, want available", all.Volumes[0].State)
	}
	if len(all.Volumes[0].Attachments) != 0 {
		t.Fatalf("the preserved volume still reports %d attachments, want 0",
			len(all.Volumes[0].Attachments))
	}
}

// TestJourney_EC2InvalidBlockDeviceMappingRefused is #671 at the SDK tier, which is
// where it matters: the point of refusing is that a consumer's IaC fails here for the
// reason it would fail on real AWS, rather than passing here and failing there.
//
// The mapping names Ebs.VolumeType and no size, which AWS's EbsBlockDevice.VolumeSize
// documents as invalid — "You must specify either a snapshot ID or a volume size."
// Through v0.104.0 substrate accepted it and materialized an 8 GiB gp3 volume, so the
// consumer's assertion passed against a size their request never asked for.
func TestJourney_EC2InvalidBlockDeviceMappingRefused(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	_, err = client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/sdf"),
			Ebs:        &ec2types.EbsBlockDevice{VolumeType: ec2types.VolumeTypeGp3},
		}},
	})
	if err == nil {
		t.Fatal("RunInstances succeeded; want InvalidBlockDeviceMapping")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("RunInstances error is not an APIError: %v", err)
	}
	if apiErr.ErrorCode() != "InvalidBlockDeviceMapping" {
		t.Fatalf("error code = %q, want InvalidBlockDeviceMapping (message %q)",
			apiErr.ErrorCode(), apiErr.ErrorMessage())
	}

	// The refusal writes nothing. Without the validator's placement ahead of
	// ensureDefaultVPC this launch left a VPC, subnet, security group, internet
	// gateway and route table behind, which the next request in the same test would
	// have seen.
	vpcs, err := client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	if err != nil {
		t.Fatalf("DescribeVpcs: %v", err)
	}
	if len(vpcs.Vpcs) != 0 {
		t.Fatalf("a refused launch left %d VPCs behind, want 0", len(vpcs.Vpcs))
	}
	vols, err := client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{})
	if err != nil {
		t.Fatalf("DescribeVolumes: %v", err)
	}
	if len(vols.Volumes) != 0 {
		t.Fatalf("a refused launch materialized %d volumes, want 0", len(vols.Volumes))
	}
}
