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

// TestJourney_EC2CreateTagsGuardrail is #674 at the tier the guardrail is written
// at: a policy that allows tagging everywhere except one resource, which is how a
// consumer keeps a shared or production resource out of reach of a pipeline that
// re-tags everything it can see.
//
// CreateTags and DeleteTags name their resources in ResourceId.N, and substrate
// decided them against a single ARN built from InstanceId.1 — a parameter neither
// operation carries — so every tagging call was decided against the literal string
// "*". Because resourceMatches passes the statement's own Resource to globMatch as
// the pattern, an ARN-scoped Deny never matched: the guardrail read as a boundary
// and was not one, and a least-privilege Allow naming the ARNs denied every call.
//
// This tier is where the two halves of the fix meet. The SDK serializes Tags as
// Tag.1.Key/Tag.1.Value, which is the form addRequestTags did not read, so an
// aws:RequestTag condition is only testable through a real client — and it has to
// be tested, because resolving the ARNs without that fix would have turned this
// policy shape from a false allow into a false deny.
func TestJourney_EC2CreateTagsGuardrail(t *testing.T) {
	ts := emulator.StartTestServer(t)

	adminCfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	admin := ec2.NewFromConfig(adminCfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	iamClient := iam.NewFromConfig(adminCfg, func(o *iam.Options) { o.RetryMaxAttempts = 1 })

	// --- the resources, created by the unenforced built-in caller ---
	//
	// An instance and a volume, because those are the two taggable types whose tags
	// are readable back through a Describe: this journey has to see that a denial
	// wrote nothing, not just that it answered 403.
	run, err := admin.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(journeyImage),
		InstanceType: ec2types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	instanceID := aws.ToString(run.Instances[0].InstanceId)

	vol, err := admin.CreateVolume(ctx, &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(20),
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	volumeID := aws.ToString(vol.VolumeId)

	// The ARNs a consumer writes into the policy, built from the documented formats —
	// no EC2 response carries an ARN member for either type, so this is what the
	// caller has to do, and it is why the resource-type spelling has to match. The
	// volume's would read "vol/vol-…" if it were derived from substrate's own state
	// key, and would then match nothing.
	const region = "us-east-1"
	const account = "123456789012"
	instanceARN := "arn:aws:ec2:" + region + ":" + account + ":instance/" + instanceID
	volumeARN := "arn:aws:ec2:" + region + ":" + account + ":volume/" + volumeID

	// --- the guarded tagger ---
	//
	// A real IAM user: the built-in AKIA key resolves to no principal, and an
	// unenforced caller would make every assertion below pass regardless of policy.
	user, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("tagger")})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	putPolicy := func(document string) {
		t.Helper()
		if _, policyErr := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
			UserName:       user.User.UserName,
			PolicyName:     aws.String("tag-guardrail"),
			PolicyDocument: aws.String(document),
		}); policyErr != nil {
			t.Fatalf("PutUserPolicy: %v", policyErr)
		}
	}
	// "Tag anything except that volume" — the whole point of the issue.
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"ec2:CreateTags","Resource":"` + volumeARN + `"}]}`)

	key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: user.User.UserName})
	if err != nil {
		t.Fatalf("CreateAccessKey: %v", err)
	}
	taggerCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			aws.ToString(key.AccessKey.AccessKeyId),
			aws.ToString(key.AccessKey.SecretAccessKey), "")),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("tagger config: %v", err)
	}
	tagger := ec2.NewFromConfig(taggerCfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })

	tag := func(client *ec2.Client, value string, resources ...string) error {
		_, tagErr := client.CreateTags(ctx, &ec2.CreateTagsInput{
			Resources: resources,
			Tags:      []ec2types.Tag{{Key: aws.String("Env"), Value: aws.String(value)}},
		})
		return tagErr
	}

	// --- the call that names the fenced-off volume alongside a resource it may tag ---
	//
	// This is the request that was allowed. The instance comes first, so a decision
	// that stopped at the first resource would also allow it.
	err = tag(tagger, "test", instanceID, volumeID)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("tagging a volume the policy denies: got %T (%v), want an API error", err, err)
	}
	if apiErr.ErrorCode() != "AccessDenied" {
		t.Errorf("ErrorCode() = %q, want AccessDenied — EC2's XML protocol has no Exception suffix",
			apiErr.ErrorCode())
	}
	if msg := err.Error(); !strings.Contains(msg, volumeARN) {
		t.Errorf("the denial does not name the volume ARN %s: %v", volumeARN, msg)
	}
	// And the refusal tagged nothing at all — not even the instance the policy
	// permits. Authorization runs before the handler, so a mixed request is refused
	// whole, which is the same all-or-nothing shape CreateTags' own reserved-key and
	// tag-limit checks already have.
	if got := journeyInstanceTags(t, ctx, admin, instanceID)["Env"]; got != "" {
		t.Errorf("a refused CreateTags tagged the instance anyway: Env=%q", got)
	}

	// --- the same call without the resource the Deny names ---
	if err := tag(tagger, "test", instanceID); err != nil {
		t.Fatalf("tagging a resource the guardrail permits must succeed: %v", err)
	}
	if got := journeyInstanceTags(t, ctx, admin, instanceID)["Env"]; got != "test" {
		t.Errorf("the instance carries Env=%q, want test", got)
	}

	// --- a least-privilege policy naming both ARNs works ---
	//
	// The other direction the single-ARN resolution broke, and the one a consumer hits
	// while writing the policy rather than while testing it.
	leastPrivilege := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:CreateTags","Resource":[` +
		`"` + instanceARN + `","` + volumeARN + `"]}]}`
	putPolicy(leastPrivilege)
	if err := tag(tagger, "prod", instanceID, volumeID); err != nil {
		t.Fatalf("a policy naming both ARNs must permit the call: %v", err)
	}
	if got := journeyVolumeTags(t, ctx, admin, volumeID)["Env"]; got != "prod" {
		t.Errorf("the volume carries Env=%q, want prod", got)
	}

	// Dropping one ARN refuses the call and says which one, which is what makes a
	// least-privilege tagging policy writable by iteration rather than by guesswork.
	putPolicy(strings.Replace(leastPrivilege, `,"`+volumeARN+`"`, "", 1))
	err = tag(tagger, "test", instanceID, volumeID)
	if !errors.As(err, &apiErr) {
		t.Fatalf("a policy omitting the volume: got %T (%v), want an API error", err, err)
	}
	if msg := err.Error(); !strings.Contains(msg, volumeARN) {
		t.Errorf("the denial does not name the omitted volume ARN %s: %v", volumeARN, msg)
	}

	// --- the tag the request carries decides the call ---
	//
	// AWS's own tagging guardrails are written this way: tag with the keys and values
	// we prescribe, or not at all. aws:RequestTag was empty for every direct tagging
	// call, so this policy could not be satisfied — and once the ARNs resolved, it
	// would have denied every call instead of allowing none.
	putPolicy(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"ec2:CreateTags","Resource":"*",` +
		`"Condition":{"StringEquals":{"aws:RequestTag/Env":"test"}}}]}`)
	if err := tag(tagger, "test", instanceID); err != nil {
		t.Fatalf("a request tagging Env=test must satisfy a policy requiring it: %v", err)
	}
	if err := tag(tagger, "staging", instanceID); !errors.As(err, &apiErr) {
		t.Fatalf("a request tagging Env=staging: got %T (%v), want an API error", err, err)
	}

	// --- DeleteTags resolves the same way ---
	//
	// Same resources, same ARNs, and the value is optional: this request names the key
	// alone, which is the form a consumer's cleanup step sends.
	putPolicy(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"ec2:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"ec2:DeleteTags","Resource":"` + volumeARN + `"}]}`)
	_, err = tagger.DeleteTags(ctx, &ec2.DeleteTagsInput{
		Resources: []string{volumeID},
		Tags:      []ec2types.Tag{{Key: aws.String("Env")}},
	})
	if !errors.As(err, &apiErr) {
		t.Fatalf("deleting a tag the policy denies: got %T (%v), want an API error", err, err)
	}
	if msg := err.Error(); !strings.Contains(msg, volumeARN) {
		t.Errorf("the denial does not name the volume ARN %s: %v", volumeARN, msg)
	}
	if got := journeyVolumeTags(t, ctx, admin, volumeID)["Env"]; got != "prod" {
		t.Errorf("a refused DeleteTags removed the tag anyway: Env=%q, want prod", got)
	}
}

// journeyInstanceTags reads one instance's tags through the unenforced admin, so the
// verification is not itself subject to the policy under test.
func journeyInstanceTags(t *testing.T, ctx context.Context, client *ec2.Client, id string) map[string]string {
	t.Helper()
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{id}})
	if err != nil {
		t.Fatalf("DescribeInstances %s: %v", id, err)
	}
	if len(out.Reservations) != 1 || len(out.Reservations[0].Instances) != 1 {
		t.Fatalf("DescribeInstances %s returned no instance", id)
	}
	return journeyTagMap(out.Reservations[0].Instances[0].Tags)
}

// journeyVolumeTags reads one volume's tags through the unenforced admin.
func journeyVolumeTags(t *testing.T, ctx context.Context, client *ec2.Client, id string) map[string]string {
	t.Helper()
	out, err := client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{id}})
	if err != nil {
		t.Fatalf("DescribeVolumes %s: %v", id, err)
	}
	if len(out.Volumes) != 1 {
		t.Fatalf("DescribeVolumes %s returned %d volumes, want 1", id, len(out.Volumes))
	}
	return journeyTagMap(out.Volumes[0].Tags)
}
