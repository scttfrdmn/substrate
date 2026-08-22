package e2e_test

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_EC2DescribeTags is #688 at the tier that proves the operation is reachable:
// through a typed SDK client, whose DescribeTagsPaginator is how a consumer actually walks
// a region's tags.
//
// The unit tests build the form parameters by hand. This one cannot — the SDK decides how
// Filters and MaxResults serialize and how the tagSet decodes into []types.TagDescription,
// so it is the only tier that shows a real client's shape agreeing with substrate's. Before
// #688 every call below failed with InvalidAction, which the SDK surfaces as an API error
// rather than an empty result: a consumer could not have written this loop at all.
func TestJourney_EC2DescribeTags(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })

	// Two resources of different types, so resourceType has something to distinguish.
	run, err := client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(journeyImage),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	instanceID := aws.ToString(run.Instances[0].InstanceId)

	vpc, err := client.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.42.0.0/16")})
	if err != nil {
		t.Fatalf("CreateVpc: %v", err)
	}
	vpcID := aws.ToString(vpc.Vpc.VpcId)

	tag := func(id, key, value string) {
		t.Helper()
		if _, err := client.CreateTags(ctx, &ec2.CreateTagsInput{
			Resources: []string{id},
			Tags:      []ec2types.Tag{{Key: aws.String(key), Value: aws.String(value)}},
		}); err != nil {
			t.Fatalf("CreateTags %s %s: %v", id, key, err)
		}
	}
	tag(instanceID, "Name", "webserver")
	tag(instanceID, "Env", "prod")
	tag(vpcID, "Env", "staging")

	// --- the whole region, through the paginator ---
	//
	// MaxResults 5 with three tags is one page; the point is that the SDK's own loop
	// terminates, which it does only because the last page carries no NextToken.
	var described []ec2types.TagDescription
	pages := ec2.NewDescribeTagsPaginator(client, &ec2.DescribeTagsInput{MaxResults: aws.Int32(5)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Fatalf("DescribeTags page: %v", err)
		}
		described = append(described, page.Tags...)
	}
	if len(described) != 3 {
		t.Fatalf("DescribeTags returned %d tags, want 3: %+v", len(described), described)
	}

	got := make([]string, 0, len(described))
	for _, d := range described {
		got = append(got, aws.ToString(d.ResourceId)+" "+string(d.ResourceType)+" "+
			aws.ToString(d.Key)+"="+aws.ToString(d.Value))
	}
	sort.Strings(got)
	want := []string{
		instanceID + " instance Env=prod",
		instanceID + " instance Name=webserver",
		vpcID + " vpc Env=staging",
	}
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tag %d = %q, want %q", i, got[i], want[i])
		}
	}

	// ResourceType decodes into the SDK's own enum, so a spelling substrate invented
	// would arrive as an unknown value rather than failing loudly. Assert against the
	// enum constants to catch that.
	for _, d := range described {
		switch d.ResourceType {
		case ec2types.ResourceTypeInstance, ec2types.ResourceTypeVpc:
		default:
			t.Fatalf("resourceType %q is not one of the SDK's own values", d.ResourceType)
		}
	}

	// --- a filter the SDK serializes, and a wildcard AWS documents for this operation ---
	byValue, err := client.DescribeTags(ctx, &ec2.DescribeTagsInput{
		Filters: []ec2types.Filter{{Name: aws.String("value"), Values: []string{"?ebserver"}}},
	})
	if err != nil {
		t.Fatalf("DescribeTags filtered: %v", err)
	}
	if len(byValue.Tags) != 1 || aws.ToString(byValue.Tags[0].Key) != "Name" {
		t.Fatalf("value=?ebserver returned %+v, want the one Name tag", byValue.Tags)
	}

	// Two filters AND, so a combination no single tag satisfies is an empty answer and
	// not the union — the shape a consumer's "is this resource tagged X" check turns on.
	none, err := client.DescribeTags(ctx, &ec2.DescribeTagsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("resource-id"), Values: []string{vpcID}},
			{Name: aws.String("value"), Values: []string{"prod"}},
		},
	})
	if err != nil {
		t.Fatalf("DescribeTags ANDed: %v", err)
	}
	if len(none.Tags) != 0 {
		t.Fatalf("two ANDed filters returned %+v, want none", none.Tags)
	}
}
