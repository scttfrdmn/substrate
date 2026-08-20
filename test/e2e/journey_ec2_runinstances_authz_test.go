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

// TestJourney_EC2RunInstancesSubnetGuardrail is #662 at the tier the guardrail is
// actually written at: a policy that allows ec2:RunInstances everywhere except one
// subnet, which is how a consumer keeps workloads out of a private or shared
// subnet.
//
// RunInstances names five required resource types — image*, instance*,
// network-interface*, security-group* and subnet* — and substrate authorized it
// against a single ARN built from InstanceId.1, a parameter a launch never
// carries. Every launch was therefore decided against the literal string "*", and
// because resourceMatches passes the *statement's* Resource entry to globMatch as
// the pattern, an ARN-scoped Deny never matched anything. The guardrail read as a
// boundary and was not one.
//
// Only this tier shows what the caller sees. EC2 models no exception shapes, so
// the refusal arrives as a generic smithy.APIError whose ErrorCode a consumer
// branches on, and its message has to name the ARN the policy is missing —
// otherwise the fix is correct and undiagnosable.
func TestJourney_EC2RunInstancesSubnetGuardrail(t *testing.T) {
	ts := emulator.StartTestServer(t)

	adminCfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	admin := ec2.NewFromConfig(adminCfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	iamClient := iam.NewFromConfig(adminCfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	// --- the network, built by the unenforced built-in caller ---
	vpc, err := admin.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	if err != nil {
		t.Fatalf("CreateVpc: %v", err)
	}
	vpcID := aws.ToString(vpc.Vpc.VpcId)

	newSubnet := func(cidr string) string {
		t.Helper()
		out, subnetErr := admin.CreateSubnet(ctx, &ec2.CreateSubnetInput{
			VpcId:     aws.String(vpcID),
			CidrBlock: aws.String(cidr),
		})
		if subnetErr != nil {
			t.Fatalf("CreateSubnet %s: %v", cidr, subnetErr)
		}
		return aws.ToString(out.Subnet.SubnetId)
	}
	privateSubnet := newSubnet("10.0.1.0/24")
	publicSubnet := newSubnet("10.0.2.0/24")

	sg, err := admin.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String("workload"),
		Description: aws.String("journey"),
		VpcId:       aws.String(vpcID),
	})
	if err != nil {
		t.Fatalf("CreateSecurityGroup: %v", err)
	}
	sgID := aws.ToString(sg.GroupId)

	// The ARNs a consumer writes into the policy. EC2's Describe responses carry no
	// ARN member for a subnet, so these are built from the documented format — which
	// is exactly what the caller has to do, and why the format has to match.
	const region = "us-east-1"
	const account = "123456789012"
	privateSubnetARN := "arn:aws:ec2:" + region + ":" + account + ":subnet/" + privateSubnet

	// --- the guarded launcher ---
	//
	// A real IAM user, because the built-in AKIA key resolves to no principal and an
	// unenforced caller would make every assertion below pass regardless of policy.
	user, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("launcher")})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	putPolicy := func(document string) {
		t.Helper()
		if _, policyErr := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
			UserName:       user.User.UserName,
			PolicyName:     aws.String("launch-guardrail"),
			PolicyDocument: aws.String(document),
		}); policyErr != nil {
			t.Fatalf("PutUserPolicy: %v", policyErr)
		}
	}
	// "Launch anywhere except the private subnet" — the whole point of the issue.
	guardrail := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"ec2:RunInstances","Resource":"` + privateSubnetARN + `"}]}`
	putPolicy(guardrail)

	key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: user.User.UserName,
	})
	if err != nil {
		t.Fatalf("CreateAccessKey: %v", err)
	}
	launcherCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			aws.ToString(key.AccessKey.AccessKeyId),
			aws.ToString(key.AccessKey.SecretAccessKey), "")),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("launcher config: %v", err)
	}
	launcher := ec2.NewFromConfig(launcherCfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })

	launch := func(client *ec2.Client, subnetID string) (*ec2.RunInstancesOutput, error) {
		return client.RunInstances(ctx, &ec2.RunInstancesInput{
			ImageId:          aws.String("ami-0abcdef1234567890"),
			InstanceType:     ec2types.InstanceTypeT3Micro,
			MinCount:         aws.Int32(1),
			MaxCount:         aws.Int32(1),
			SubnetId:         aws.String(subnetID),
			SecurityGroupIds: []string{sgID},
		})
	}

	// --- the launch into the fenced-off subnet, which the Deny names ---
	//
	// This is the request that was allowed. EC2 models no exception shapes, so the
	// SDK surfaces a generic APIError rather than a typed one.
	_, err = launch(launcher, privateSubnet)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("launching into a subnet the policy denies: got %T (%v), want an API error", err, err)
	}
	if apiErr.ErrorCode() != "AccessDenied" {
		t.Errorf("ErrorCode() = %q, want AccessDenied — EC2's XML protocol has no Exception suffix",
			apiErr.ErrorCode())
	}
	// The message is the only place the refused resource surfaces, and it is what
	// tells the caller which ARN the decision tripped on.
	if msg := err.Error(); !strings.Contains(msg, privateSubnetARN) {
		t.Errorf("the denial does not name the subnet ARN %s: %v", privateSubnetARN, msg)
	}
	// And the refusal really launched nothing: a denial that had already written
	// state would be worse than the false allow it replaced.
	if got := journeyEC2InstanceCount(t, ctx, admin); got != 0 {
		t.Fatalf("after the refusal there are %d instances, want 0", got)
	}

	// --- the same launch into a subnet the Deny does not name ---
	run, err := launch(launcher, publicSubnet)
	if err != nil {
		t.Fatalf("a launch into a subnet the guardrail permits must succeed: %v", err)
	}
	if len(run.Instances) != 1 {
		t.Fatalf("RunInstances returned %d instances, want 1", len(run.Instances))
	}
	if got := aws.ToString(run.Instances[0].SubnetId); got != publicSubnet {
		t.Errorf("the instance landed in subnet %q, want %s", got, publicSubnet)
	}

	// --- and a least-privilege policy naming every required ARN works ---
	//
	// The other direction the single-ARN resolution broke: a policy naming exactly
	// the five ARNs AWS documents as required refused every launch, because
	// "arn:…:image/ami-…" as a pattern does not match the value "*" either. Note the
	// image ARN's empty account field, which is the format the Service Authorization
	// Reference gives — an AMI is shareable.
	leastPrivilege := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:RunInstances","Resource":[` +
		`"arn:aws:ec2:` + region + `::image/ami-0abcdef1234567890",` +
		`"arn:aws:ec2:` + region + `:` + account + `:subnet/` + publicSubnet + `",` +
		`"arn:aws:ec2:` + region + `:` + account + `:security-group/` + sgID + `",` +
		`"arn:aws:ec2:` + region + `:` + account + `:network-interface/*",` +
		`"arn:aws:ec2:` + region + `:` + account + `:instance/*"]}]}`
	putPolicy(leastPrivilege)

	if _, err := launch(launcher, publicSubnet); err != nil {
		t.Fatalf("a policy naming every required ARN must permit the launch: %v", err)
	}

	// Dropping one ARN from that policy refuses the launch and says which one, which
	// is what makes a least-privilege policy writable by iteration rather than by
	// guesswork.
	withoutSubnet := strings.Replace(leastPrivilege,
		`"arn:aws:ec2:`+region+`:`+account+`:subnet/`+publicSubnet+`",`, "", 1)
	putPolicy(withoutSubnet)

	_, err = launch(launcher, publicSubnet)
	if !errors.As(err, &apiErr) {
		t.Fatalf("a policy omitting the subnet: got %T (%v), want an API error", err, err)
	}
	wantARN := "arn:aws:ec2:" + region + ":" + account + ":subnet/" + publicSubnet
	if msg := err.Error(); !strings.Contains(msg, wantARN) {
		t.Errorf("the denial does not name the omitted subnet ARN %s: %v", wantARN, msg)
	}
}

// journeyEC2InstanceCount reports how many instances exist, read by the
// unenforced admin so the count is not itself subject to the policy under test.
func journeyEC2InstanceCount(t *testing.T, ctx context.Context, client *ec2.Client) int {
	t.Helper()
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})
	if err != nil {
		t.Fatalf("DescribeInstances: %v", err)
	}
	n := 0
	for _, r := range out.Reservations {
		n += len(r.Instances)
	}
	return n
}
