package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// journeyDefaultVPCRegion has to be the region [journeyImage] resolves in: these journeys
// launch from that AMI and also name it in a policy ARN, and an AMI ID is valid in exactly
// one region (#733).
const (
	journeyDefaultVPCRegion  = "us-east-1"
	journeyDefaultVPCAccount = "123456789012"
)

// journeyGuardedEC2 creates an IAM user with an access key and returns an EC2 client
// signing as it, plus a function that rewrites its inline policy.
//
// A real IAM user is what makes any of this meaningful: the built-in AKIA key resolves
// to no principal, and an unenforced caller would make every assertion below pass
// regardless of policy.
func journeyGuardedEC2(
	t *testing.T,
	ctx context.Context,
	ts *emulator.TestServer,
	iamClient *iam.Client,
	userName string,
) (*ec2.Client, func(document string)) {
	t.Helper()
	user, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(userName)})
	if err != nil {
		t.Fatalf("CreateUser %s: %v", userName, err)
	}
	putPolicy := func(document string) {
		t.Helper()
		if _, policyErr := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
			UserName:       user.User.UserName,
			PolicyName:     aws.String("guardrail"),
			PolicyDocument: aws.String(document),
		}); policyErr != nil {
			t.Fatalf("PutUserPolicy: %v", policyErr)
		}
	}
	key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: user.User.UserName})
	if err != nil {
		t.Fatalf("CreateAccessKey: %v", err)
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(journeyDefaultVPCRegion),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			aws.ToString(key.AccessKey.AccessKeyId),
			aws.ToString(key.AccessKey.SecretAccessKey), "")),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("%s config: %v", userName, err)
	}
	return ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 }), putPolicy
}

// journeyDeniedResource returns the ARN an EC2 AccessDenied names, failing the test if
// err is not one.
//
// EC2 models no exception shapes, so the refusal arrives as a generic smithy.APIError
// whose ErrorCode a consumer branches on. The message is the only place the refused
// resource surfaces, which is what makes a least-privilege policy writable by
// iteration rather than by guesswork.
func journeyDeniedResource(t *testing.T, err error) string {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("got %T (%v), want an API error", err, err)
	}
	if apiErr.ErrorCode() != "AccessDenied" {
		t.Fatalf("ErrorCode() = %q, want AccessDenied", apiErr.ErrorCode())
	}
	msg := apiErr.ErrorMessage()
	const marker = "on resource: "
	i := strings.Index(msg, marker)
	if i < 0 {
		t.Fatalf("the denial does not name a resource: %s", msg)
	}
	return strings.TrimSpace(msg[i+len(marker):])
}

// TestJourney_EC2DefaultVPCSubnetGuardrail is #673 at the tier the guardrail is
// written at: a launch that omits SubnetId.
//
// Substrate resolves such a launch's subnet and security group from the default VPC,
// creating them if the account has none — and until #673 that happened *after* the
// authorization decision, so the decision named neither. A policy scoped to one subnet
// therefore permitted a launch into a subnet it never mentioned, which is precisely
// what a subnet guardrail exists to prevent. Every getting-started example and most
// CDK-generated launches take exactly this shape.
//
// AWS's own Service Authorization Reference points the other way — its RunInstances
// scenario rows require subnet* only in the -Subnet scenarios, and its recommended
// subnet guardrail is the ec2:Subnet condition key on network-interface/* — so this is
// substrate's reading, made because a guardrail a caller defeats by omitting a
// parameter is useless for the purpose substrate exists for.
func TestJourney_EC2DefaultVPCSubnetGuardrail(t *testing.T) {
	ts := emulator.StartTestServer(t)

	adminCfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	admin := ec2.NewFromConfig(adminCfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	iamClient := iam.NewFromConfig(adminCfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	// A subnet of the caller's own, in a VPC CreateVpc does not mark as default. No
	// security group is created: once the default VPC exists, its own group is then the
	// only one in the account, which is what makes the default-group lookup below
	// resolve to a known ID rather than to whichever group sorts first.
	vpc, err := admin.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	if err != nil {
		t.Fatalf("CreateVpc: %v", err)
	}
	approved, err := admin.CreateSubnet(ctx, &ec2.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.0.1.0/24"),
	})
	if err != nil {
		t.Fatalf("CreateSubnet: %v", err)
	}
	approvedSubnet := aws.ToString(approved.Subnet.SubnetId)

	arn := func(format string) string {
		return "arn:aws:ec2:" + journeyDefaultVPCRegion + ":" + journeyDefaultVPCAccount + ":" + format
	}
	imageARN := "arn:aws:ec2:" + journeyDefaultVPCRegion + "::image/" + journeyImage
	allow := func(resources ...string) string {
		return `{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Allow","Action":"ec2:RunInstances","Resource":["` +
			strings.Join(resources, `","`) + `"]}]}`
	}

	launcher, putPolicy := journeyGuardedEC2(t, ctx, ts, iamClient, "subnetless-launcher")
	launch := func(client *ec2.Client) (*ec2.RunInstancesOutput, error) {
		return client.RunInstances(ctx, &ec2.RunInstancesInput{
			ImageId:      aws.String(journeyImage),
			InstanceType: ec2types.InstanceTypeT3Micro,
			MinCount:     aws.Int32(1),
			MaxCount:     aws.Int32(1),
		})
	}

	// --- the fail-before: a policy allowing exactly one subnet ---
	//
	// Generous about security groups and strict about subnets, so the only thing the
	// policy withholds is a subnet other than the approved one. This launch used to
	// succeed: it resolved to the literal "*", which matches only a statement whose own
	// Resource starts with "*", so no ARN in this policy was ever compared against it.
	putPolicy(allow(imageARN, arn("subnet/"+approvedSubnet), arn("security-group/*"),
		arn("network-interface/*"), arn("instance/*")))

	_, err = launch(launcher)
	if got := journeyDeniedResource(t, err); got != arn("subnet/*") {
		t.Errorf("the denial names %q, want the default subnet the launch would create, %q",
			got, arn("subnet/*"))
	}
	if got := journeyEC2InstanceCount(t, ctx, admin); got != 0 {
		t.Fatalf("after the refusal there are %d instances, want 0", got)
	}
	// And no default VPC was created on the way to the refusal, which authorization
	// running before the handler is what guarantees.
	vpcs, err := admin.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	if err != nil {
		t.Fatalf("DescribeVpcs: %v", err)
	}
	if len(vpcs.Vpcs) != 1 {
		t.Fatalf("after the refusal there are %d VPCs, want only the caller's own", len(vpcs.Vpcs))
	}

	// --- naming the two wildcards permits it, and it creates the default VPC ---
	//
	// subnet/* and security-group/* are resources this launch will *create*, which is
	// why they are wildcards on the same reasoning instance/* and network-interface/*
	// already are.
	putPolicy(allow(imageARN, arn("subnet/*"), arn("security-group/*"),
		arn("network-interface/*"), arn("instance/*")))

	if _, err = launch(launcher); err != nil {
		t.Fatalf("a policy naming the wildcards the launch will create must permit it: %v", err)
	}

	// The default subnet and group the launch resolved, read back through the API
	// rather than assumed — these are the ARNs a consumer would have to write.
	described, err := admin.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})
	if err != nil {
		t.Fatalf("DescribeInstances: %v", err)
	}
	if len(described.Reservations) != 1 || len(described.Reservations[0].Instances) != 1 {
		t.Fatalf("want exactly one instance, got %+v", described.Reservations)
	}
	instance := described.Reservations[0].Instances[0]
	defaultSubnet := aws.ToString(instance.SubnetId)
	if defaultSubnet == "" || defaultSubnet == approvedSubnet {
		t.Fatalf("the launch landed in %q, want a freshly created default subnet", defaultSubnet)
	}
	if len(instance.SecurityGroups) != 1 {
		t.Fatalf("the instance carries %d security groups, want the default VPC's one",
			len(instance.SecurityGroups))
	}
	defaultSG := aws.ToString(instance.SecurityGroups[0].GroupId)

	// --- the guardrail: a Deny on the resolved default subnet ---
	//
	// "Launch anywhere except the default subnet", which omitting SubnetId used to be
	// the way around.
	defaultSubnetARN := arn("subnet/" + defaultSubnet)
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"ec2:RunInstances","Resource":"` + defaultSubnetARN + `"}]}`)

	_, err = launch(launcher)
	if got := journeyDeniedResource(t, err); got != defaultSubnetARN {
		t.Errorf("the denial names %q, want the default subnet %q", got, defaultSubnetARN)
	}
	if got := journeyEC2InstanceCount(t, ctx, admin); got != 1 {
		t.Fatalf("the refused launch changed the instance count to %d, want 1", got)
	}

	// --- and a least-privilege policy naming what the launch resolves to works ---
	//
	// The other direction the fix has to hold in: making the ordinary subnet-less
	// launch unrunnable under least privilege would be its own defect.
	putPolicy(allow(imageARN, defaultSubnetARN, arn("security-group/"+defaultSG),
		arn("network-interface/*"), arn("instance/*")))

	run, err := launch(launcher)
	if err != nil {
		t.Fatalf("a policy naming the resolved default subnet and group must permit the launch: %v", err)
	}
	if got := aws.ToString(run.Instances[0].SubnetId); got != defaultSubnet {
		t.Errorf("the instance landed in %q, want the default subnet %q", got, defaultSubnet)
	}

	// Dropping the security group from that policy refuses the launch and says so,
	// which is what tells a caller the default group is part of the decision at all.
	putPolicy(allow(imageARN, defaultSubnetARN, arn("network-interface/*"), arn("instance/*")))
	_, err = launch(launcher)
	if got := journeyDeniedResource(t, err); got != arn("security-group/"+defaultSG) {
		t.Errorf("the denial names %q, want the default security group %q",
			got, arn("security-group/"+defaultSG))
	}
}

// TestJourney_EC2CreateFleetAuthorizesItsLaunches pins #673's second gap: the fleet
// path.
//
// CreateFleet launches its instances internally, through runInstances rather than
// through the API, so the server's authorization pipeline decided the CreateFleet
// request and never the launches it produced. A caller denied ec2:RunInstances on a
// subnet reached it by asking for a fleet instead. Real AWS does not answer that way —
// its CreateFleet reference requires permission for the launch's own resources — so
// the fleet path is authorized rather than exempted, using the same request builder the
// launch itself uses so the decision cannot be about a different request.
func TestJourney_EC2CreateFleetAuthorizesItsLaunches(t *testing.T) {
	ts := emulator.StartTestServer(t)

	adminCfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	admin := ec2.NewFromConfig(adminCfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	iamClient := iam.NewFromConfig(adminCfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	vpc, err := admin.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	if err != nil {
		t.Fatalf("CreateVpc: %v", err)
	}
	subnet, err := admin.CreateSubnet(ctx, &ec2.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.0.1.0/24"),
	})
	if err != nil {
		t.Fatalf("CreateSubnet: %v", err)
	}
	subnetID := aws.ToString(subnet.Subnet.SubnetId)

	lt, err := admin.CreateLaunchTemplate(ctx, &ec2.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("fleet-pool"),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{
			ImageId: aws.String(journeyImage),
		},
	})
	if err != nil {
		t.Fatalf("CreateLaunchTemplate: %v", err)
	}
	ltID := aws.ToString(lt.LaunchTemplate.LaunchTemplateId)

	arn := func(format string) string {
		return "arn:aws:ec2:" + journeyDefaultVPCRegion + ":" + journeyDefaultVPCAccount + ":" + format
	}
	imageARN := "arn:aws:ec2:" + journeyDefaultVPCRegion + "::image/" + journeyImage

	fleeter, putPolicy := journeyGuardedEC2(t, ctx, ts, iamClient, "fleeter")
	createFleet := func() (*ec2.CreateFleetOutput, error) {
		return fleeter.CreateFleet(ctx, &ec2.CreateFleetInput{
			Type: ec2types.FleetTypeInstant,
			LaunchTemplateConfigs: []ec2types.FleetLaunchTemplateConfigRequest{{
				LaunchTemplateSpecification: &ec2types.FleetLaunchTemplateSpecificationRequest{
					LaunchTemplateId: aws.String(ltID),
					Version:          aws.String("1"),
				},
				Overrides: []ec2types.FleetLaunchTemplateOverridesRequest{{
					InstanceType: ec2types.InstanceTypeT3Micro,
					SubnetId:     aws.String(subnetID),
				}},
			}},
			TargetCapacitySpecification: &ec2types.TargetCapacitySpecificationRequest{
				TotalTargetCapacity:       aws.Int32(2),
				DefaultTargetCapacityType: ec2types.DefaultTargetCapacityTypeOnDemand,
			},
		})
	}

	// --- ec2:CreateFleet alone is not enough ---
	//
	// This is the request that used to launch two instances. The denial names the
	// launch's own first resource, the launch template's AMI, because that is what the
	// synthesized RunInstances is decided against.
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:CreateFleet","Resource":"*"}]}`)

	_, err = createFleet()
	if got := journeyDeniedResource(t, err); got != imageARN {
		t.Errorf("the denial names %q, want the launch's AMI %q", got, imageARN)
	}
	if got := journeyEC2InstanceCount(t, ctx, admin); got != 0 {
		t.Fatalf("a refused fleet launched %d instances, want 0", got)
	}
	// And no fleet record either: every pool is decided before the first launch, so a
	// refusal writes nothing at all.
	fleets, err := admin.DescribeFleets(ctx, &ec2.DescribeFleetsInput{})
	if err != nil {
		t.Fatalf("DescribeFleets: %v", err)
	}
	if len(fleets.Fleets) != 0 {
		t.Errorf("a refused fleet left %d fleet records behind, want 0", len(fleets.Fleets))
	}

	// --- a Deny scoped to the pool's subnet blocks the fleet ---
	//
	// The guardrail a consumer actually writes, now reachable through the fleet path.
	subnetARN := arn("subnet/" + subnetID)
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"ec2:RunInstances","Resource":"` + subnetARN + `"}]}`)

	_, err = createFleet()
	if got := journeyDeniedResource(t, err); got != subnetARN {
		t.Errorf("the denial names %q, want the pool's subnet %q", got, subnetARN)
	}
	if got := journeyEC2InstanceCount(t, ctx, admin); got != 0 {
		t.Fatalf("a refused fleet launched %d instances, want 0", got)
	}

	// --- permitting ec2:RunInstances on the launch's resources lets the fleet run ---
	//
	// Which is what real AWS requires, and the reason authorizing the path rather than
	// exempting it is the right answer: the policy a consumer writes against AWS is the
	// policy that works here.
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:CreateFleet","Resource":"*"},` +
		`{"Effect":"Allow","Action":"ec2:RunInstances","Resource":["` +
		strings.Join([]string{imageARN, subnetARN, arn("network-interface/*"), arn("instance/*")},
			`","`) + `"]}]}`)

	out, err := createFleet()
	if err != nil {
		t.Fatalf("a policy permitting the launch's resources must permit the fleet: %v", err)
	}
	launched := 0
	for _, group := range out.Instances {
		launched += len(group.InstanceIds)
	}
	if launched != 2 {
		t.Errorf("the fleet launched %d instances, want 2", launched)
	}
	if got := journeyEC2InstanceCount(t, ctx, admin); got != 2 {
		t.Errorf("DescribeInstances reports %d instances, want 2", got)
	}
}
