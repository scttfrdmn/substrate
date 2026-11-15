package e2e_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_EC2LaunchTemplateMetadataOptions is #1471 and #1469 through the typed SDK, which
// is the reader that tells an absent list from an empty one: a template carrying
// MetadataOptions reads it back, reads back no NetworkInterfaces it was not sent, an instance
// launched from it reports the options, ModifyInstanceMetadataOptions decodes, and an unknown
// template name is the typed error code.
func TestJourney_EC2LaunchTemplateMetadataOptions(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	ctx := context.Background()

	ami := emulator.BundledImageID("us-east-1", "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id")
	if _, err := client.CreateLaunchTemplate(ctx, &ec2.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("imds"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String(ami),
			InstanceType: types.InstanceTypeC7gLarge,
			MetadataOptions: &types.LaunchTemplateInstanceMetadataOptionsRequest{
				HttpTokens:              types.LaunchTemplateHttpTokensStateRequired,
				HttpPutResponseHopLimit: aws.Int32(2),
				HttpEndpoint:            types.LaunchTemplateInstanceMetadataEndpointStateEnabled,
				InstanceMetadataTags:    types.LaunchTemplateInstanceMetadataTagsStateEnabled,
			},
		},
	}); err != nil {
		t.Fatalf("CreateLaunchTemplate: %v", err)
	}

	versions, err := client.DescribeLaunchTemplateVersions(ctx, &ec2.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateName: aws.String("imds"), Versions: []string{"$Default"},
	})
	if err != nil {
		t.Fatalf("DescribeLaunchTemplateVersions: %v", err)
	}
	if len(versions.LaunchTemplateVersions) != 1 {
		t.Fatalf("got %d versions, want 1", len(versions.LaunchTemplateVersions))
	}
	data := versions.LaunchTemplateVersions[0].LaunchTemplateData
	if data.MetadataOptions == nil {
		t.Fatal("MetadataOptions did not round-trip")
	}
	if data.MetadataOptions.HttpTokens != types.LaunchTemplateHttpTokensStateRequired ||
		aws.ToInt32(data.MetadataOptions.HttpPutResponseHopLimit) != 2 ||
		data.MetadataOptions.InstanceMetadataTags != types.LaunchTemplateInstanceMetadataTagsStateEnabled {
		t.Fatalf("MetadataOptions = %+v", *data.MetadataOptions)
	}
	if data.NetworkInterfaces != nil || data.SecurityGroupIds != nil || data.BlockDeviceMappings != nil || data.TagSpecifications != nil {
		t.Fatalf("lists the request never carried read back: interfaces %v, groups %v, mappings %v, tags %v",
			data.NetworkInterfaces, data.SecurityGroupIds, data.BlockDeviceMappings, data.TagSpecifications)
	}

	run, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		LaunchTemplate: &types.LaunchTemplateSpecification{LaunchTemplateName: aws.String("imds")},
	})
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	id := aws.ToString(run.Instances[0].InstanceId)
	if got := run.Instances[0].MetadataOptions; got == nil || got.HttpTokens != types.HttpTokensStateRequired ||
		got.State != types.InstanceMetadataOptionsStatePending {
		t.Fatalf("the launched instance does not require IMDSv2, pending: %+v", got)
	}

	mod, err := client.ModifyInstanceMetadataOptions(ctx, &ec2.ModifyInstanceMetadataOptionsInput{
		InstanceId: aws.String(id), HttpPutResponseHopLimit: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("ModifyInstanceMetadataOptions: %v", err)
	}
	if mod.InstanceMetadataOptions == nil || mod.InstanceMetadataOptions.State != types.InstanceMetadataOptionsStatePending ||
		aws.ToInt32(mod.InstanceMetadataOptions.HttpPutResponseHopLimit) != 1 ||
		mod.InstanceMetadataOptions.HttpTokens != types.HttpTokensStateRequired {
		t.Fatalf("ModifyInstanceMetadataOptions answered %+v", mod.InstanceMetadataOptions)
	}

	_, err = client.DescribeLaunchTemplates(ctx, &ec2.DescribeLaunchTemplatesInput{LaunchTemplateNames: []string{"nope"}})
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidLaunchTemplateName.NotFoundException" {
		t.Fatalf("DescribeLaunchTemplates(nope) = %v, want InvalidLaunchTemplateName.NotFoundException", err)
	}
}
