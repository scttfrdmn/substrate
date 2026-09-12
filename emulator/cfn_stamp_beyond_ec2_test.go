package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"log/slog"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The aws:cloudformation:* stamp beyond EC2 (#765).
//
// #746 gave the deployer a stamp, and it reached EC2 only: the resolver behind it switched on
// an EC2 id prefix, so an S3 bucket, a DynamoDB table, a Lambda function, an SQS queue and
// every ELBv2 resource a stack created carried none of the three keys. A policy or a
// cost-allocation assertion keyed on the stack name saw nothing on them.
//
// Every assertion here reads the tag back through the **owning service's own tag call** rather
// than out of state, which is what #765's first criterion asks for and what makes the test
// meaningful: a stamp written to a state key the service does not read would satisfy a state
// assertion and satisfy no caller. That is not hypothetical — the key SQS reads is
// `queue:<account>/<name>` while the Resource Groups Tagging API writes `queue:<name>` (#826),
// so a reader that agreed with the wrong one would pass while the tag stayed invisible.

// cfnExpectedStamp is the tag set every stamped resource carries, in the sorted "key=value"
// form the fixture's readers return.
func cfnExpectedStamp(stackName, stackID, logicalID string) []string {
	return []string{
		cfnStampLogicalIDTag + "=" + logicalID,
		cfnStampStackIDTag + "=" + stackID,
		cfnStampStackNameTag + "=" + stackName,
	}
}

// cfnSortedTagStrings renders a key/value pair list as sorted "key=value" strings, so an
// assertion does not depend on an element order AWS says may vary.
func cfnSortedTagStrings(pairs map[string]string) []string {
	out := make([]string, 0, len(pairs))
	for k, v := range pairs {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// cfnStampReqCtx is the request context the fixture's readers use, matching the account and
// region the deployer stamps under.
func cfnStampReqCtx() *emulator.RequestContext {
	return &emulator.RequestContext{
		RequestID: "req-stamp",
		AccountID: cfnStampAccount,
		Region:    cfnStampRegion,
		Metadata:  map[string]interface{}{},
	}
}

// bucketTagsFor reads one bucket's tags through S3's GetBucketTagging.
func (f *cfnStampFixture) bucketTagsFor(t *testing.T, bucket string) []string {
	t.Helper()
	resp, err := f.s3.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "s3",
		Operation: "GET",
		Path:      "/" + bucket,
		Params:    map[string]string{"tagging": ""},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"TagSet>Tag"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "GetBucketTagging body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// functionTagsFor reads one function's tags through Lambda's ListTags.
func (f *cfnStampFixture) functionTagsFor(t *testing.T, arn string) []string {
	t.Helper()
	resp, err := f.lambda.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "lambda",
		Operation: "GET",
		Path:      "/2015-03-31/tags/" + arn,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &doc), "ListTags body: %s", resp.Body)
	return cfnSortedTagStrings(doc.Tags)
}

// queueTagsFor reads one queue's tags through SQS's ListQueueTags.
//
// The URL is built rather than read off the create response because the fixture drives the
// plugin directly; `sqsURLKey` keys a queue by the last two components, so this resolves to the
// same record a caller's URL would.
func (f *cfnStampFixture) queueTagsFor(t *testing.T, queueName string) []string {
	t.Helper()
	url := "https://sqs." + cfnStampRegion + ".amazonaws.com/" + cfnStampAccount + "/" + queueName
	resp, err := f.sqs.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "sqs",
		Operation: "ListQueueTags",
		Params:    map[string]string{"Action": "ListQueueTags", "QueueUrl": url},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListQueueTagsResult>Tag"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "ListQueueTags body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// tableTagsFor reads one table's tags through DynamoDB's ListTagsOfResource.
func (f *cfnStampFixture) tableTagsFor(t *testing.T, tableName string) []string {
	t.Helper()
	arn := "arn:aws:dynamodb:" + cfnStampRegion + ":" + cfnStampAccount + ":table/" + tableName
	body, err := json.Marshal(map[string]string{"ResourceArn": arn})
	require.NoError(t, err)

	resp, err := f.dynamodb.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "dynamodb",
		Operation: "ListTagsOfResource",
		Body:      body,
		Params:    map[string]string{},
		Headers:   map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &doc), "ListTagsOfResource body: %s", resp.Body)

	pairs := make(map[string]string, len(doc.Tags))
	for _, tag := range doc.Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// elbTagsFor reads one ELBv2 resource's tags through DescribeTags.
func (f *cfnStampFixture) elbTagsFor(t *testing.T, arn string) []string {
	t.Helper()
	require.NotEmpty(t, arn, "an ELB resource with no ARN cannot be read back")
	resp, err := f.elb.HandleRequest(cfnStampReqCtx(), &emulator.AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "DescribeTags",
		Params: map[string]string{
			"Action":                "DescribeTags",
			"ResourceArns.member.1": arn,
		},
		Headers: map[string]string{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var doc struct {
		Descriptions []struct {
			ResourceArn string `xml:"ResourceArn"`
			Tags        []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tags>member"`
		} `xml:"DescribeTagsResult>TagDescriptions>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "DescribeTags body: %s", resp.Body)
	require.Len(t, doc.Descriptions, 1)

	pairs := make(map[string]string, len(doc.Descriptions[0].Tags))
	for _, tag := range doc.Descriptions[0].Tags {
		pairs[tag.Key] = tag.Value
	}
	return cfnSortedTagStrings(pairs)
}

// arnsByLogicalID maps each deployed resource's logical ID to its ARN, requiring every
// resource to have deployed.
func arnsByLogicalID(t *testing.T, result *emulator.DeployResult) map[string]string {
	t.Helper()
	out := make(map[string]string, len(result.Resources))
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "%s failed to deploy", r.LogicalID)
		out[r.LogicalID] = r.ARN
	}
	return out
}

// cfnBeyondEC2Template covers one resource of each of the four services
// [cfnResolveStampTarget] resolves, plus the EC2 resource the other resolver claims and the
// two ELBv2 kinds whose properties a template can express — a listener and a rule need their
// parent's ARN, which `Ref` does not yet answer with (#827), so they are deployed separately.
const cfnBeyondEC2Template = `{
	"Resources": {
		"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "beyond-stamp-bucket"}},
		"Fn": {"Type": "AWS::Lambda::Function", "Properties": {
			"FunctionName": "beyond-stamp-fn",
			"Runtime": "python3.12",
			"Handler": "index.handler",
			"Role": "arn:aws:iam::123456789012:role/lambda-exec",
			"Code": {"ZipFile": "def handler(event, context):\n    return None\n"}}},
		"Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "beyond-stamp-queue"}},
		"Table": {"Type": "AWS::DynamoDB::Table", "Properties": {
			"TableName": "beyond-stamp-table",
			"BillingMode": "PAY_PER_REQUEST",
			"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
			"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]}},
		"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.11.0.0/16"}},
		"Lb": {"Type": "AWS::ElasticLoadBalancingV2::LoadBalancer", "Properties": {
			"Name": "beyond-lb", "Type": "application"}},
		"Tg": {"Type": "AWS::ElasticLoadBalancingV2::TargetGroup", "Properties": {
			"Name": "beyond-tg", "Port": "80", "Protocol": "HTTP", "VpcId": {"Ref": "Vpc"}}}
	},
	"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
}`

// TestCFN_StampReachesEveryServiceThatModelsTags is #765's own scenario, one subtest per
// service and each read back through that service's own tag call.
//
// One assertion per service rather than one for the stack, for the reason #746's equivalent
// gives: the stamp is written from a single place behind a type switch, so a type the resolver
// does not claim is skipped silently and only a per-type assertion catches it.
func TestCFN_StampReachesEveryServiceThatModelsTags(t *testing.T) {
	f := newCFNStampFixture(t, nil)
	const stackName = "beyond-ec2-stack"

	result, err := f.deployer.Deploy(context.Background(), cfnBeyondEC2Template, stackName, nil)
	require.NoError(t, err)
	ids := physicalIDs(t, result)
	arns := arnsByLogicalID(t, result)
	require.Len(t, ids, 7, "every resource in the template deployed")

	stackID := result.Outputs["StackId"]
	require.NotEmpty(t, stackID)
	want := func(logicalID string) []string {
		return cfnExpectedStamp(stackName, stackID, logicalID)
	}

	t.Run("S3 bucket", func(t *testing.T) {
		assert.Equal(t, want("Bucket"), f.bucketTagsFor(t, ids["Bucket"]))
	})
	t.Run("Lambda function", func(t *testing.T) {
		assert.Equal(t, want("Fn"), f.functionTagsFor(t, arns["Fn"]))
	})
	t.Run("SQS queue", func(t *testing.T) {
		// The one whose state key the tagging API gets wrong (#826): read through
		// ListQueueTags, so a stamp written to `queue:<name>` would report nothing here.
		assert.Equal(t, want("Queue"), f.queueTagsFor(t, ids["Queue"]))
	})
	t.Run("DynamoDB table", func(t *testing.T) {
		assert.Equal(t, want("Table"), f.tableTagsFor(t, ids["Table"]))
	})
	t.Run("ELBv2 load balancer", func(t *testing.T) {
		assert.Equal(t, want("Lb"), f.elbTagsFor(t, arns["Lb"]))
	})
	t.Run("ELBv2 target group", func(t *testing.T) {
		assert.Equal(t, want("Tg"), f.elbTagsFor(t, arns["Tg"]))
	})
	t.Run("and the EC2 resource beside them still goes through EC2's resolver", func(t *testing.T) {
		assert.Equal(t, want("Vpc"), f.tagsFor(t, ids["Vpc"]))
	})
}

// TestCFN_StampReachesAListenerAndARule covers the two ELBv2 kinds whose parent ARN a template
// cannot yet reference.
//
// A listener is created against a load balancer ARN and a rule against a listener ARN, and
// `Ref` on an ELBv2 load balancer answers with its *name* here where AWS answers with its ARN
// (#827) — so the load balancer's ARN is taken from a first deployment and written into the
// second template literally. A rule can use `Ref` on its listener, because the deployer sets a
// listener's physical ID to its ARN.
//
// These two are worth their own test rather than a state assertion, because they are the pair
// whose physical ID *is* an ARN — the case where a resolver keyed on the physical ID's shape
// would have gone wrong.
func TestCFN_StampReachesAListenerAndARule(t *testing.T) {
	f := newCFNStampFixture(t, nil)

	parent, err := f.deployer.Deploy(context.Background(), `{
		"Resources": {
			"Lb": {"Type": "AWS::ElasticLoadBalancingV2::LoadBalancer", "Properties": {
				"Name": "listener-parent-lb", "Type": "application"}}
		},
		"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
	}`, "listener-parent-stack", nil)
	require.NoError(t, err)
	lbARN := arnsByLogicalID(t, parent)["Lb"]
	require.NotEmpty(t, lbARN)

	const stackName = "listener-stack"
	tmpl := `{
		"Resources": {
			"Listener": {"Type": "AWS::ElasticLoadBalancingV2::Listener", "Properties": {
				"LoadBalancerArn": "` + lbARN + `", "Port": "80", "Protocol": "HTTP"}},
			"Rule": {"Type": "AWS::ElasticLoadBalancingV2::ListenerRule", "Properties": {
				"ListenerArn": {"Ref": "Listener"}, "Priority": "10"}}
		},
		"Outputs": {"StackId": {"Value": {"Ref": "AWS::StackId"}}}
	}`

	result, err := f.deployer.Deploy(context.Background(), tmpl, stackName, nil)
	require.NoError(t, err)
	arns := arnsByLogicalID(t, result)
	stackID := result.Outputs["StackId"]

	assert.Equal(t, cfnExpectedStamp(stackName, stackID, "Listener"), f.elbTagsFor(t, arns["Listener"]))
	assert.Equal(t, cfnExpectedStamp(stackName, stackID, "Rule"), f.elbTagsFor(t, arns["Rule"]))
	// The listener's ARN is its parent's with one more segment, and the resolver behind the
	// stamp finds an ELB resource by scanning for its ARN — so a prefix match rather than an
	// exact one would land the listener's stamp on the load balancer too. The parent still
	// reports its *own* stack and logical ID, which is what rules that out.
	assert.Equal(t,
		cfnExpectedStamp("listener-parent-stack", parent.Outputs["StackId"], "Lb"),
		f.elbTagsFor(t, lbARN),
		"the listener's stamp did not land on its parent")
}

// TestCFN_AServiceThatModelsNoTagsDeploysCleanAndSilently pins #765's third criterion.
//
// Substrate models no tags for a CloudWatch Logs log group or an EventBridge rule, and AWS
// itself declines to publish an exhaustive propagation list — "The propagation of stack-level
// tags to resources, including tags with the `aws:` prefix, varies by resource type" — so a
// type neither resolver claims is skipped rather than warned about. The deployer creates far
// more of those than of the kinds it can stamp, and a line per resource would drown a real
// warning.
//
// The assertion is on the log: a `Warn` from the stamp is the failure this test exists to
// catch, and a silent skip cannot be observed any other way.
func TestCFN_AServiceThatModelsNoTagsDeploysCleanAndSilently(t *testing.T) {
	logger := &cfnRecordingLogger{}
	f := newCFNStampFixtureWithLogger(t, nil, logger)

	result, err := f.deployer.Deploy(context.Background(), `{"Resources": {
		"Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": "/beyond/none"}},
		"Vpc":  {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.12.0.0/16"}}
	}}`, "untaggable-stack", nil)
	require.NoError(t, err)

	ids := physicalIDs(t, result)
	assert.Len(t, f.tagsFor(t, ids["Vpc"]), 3, "the resource that can be stamped still is")
	assert.NotContains(t, logger.joined(), "could not stamp",
		"a type substrate models no tags for is skipped in silence")
}

// cfnRecordingLogger keeps every message logged at or above Warn, which is the level the stamp
// reports a failure at. Debug and Info are dropped: the deployer is chatty at both, and the
// assertion is about a warning existing at all rather than about the whole log.
type cfnRecordingLogger struct {
	messages []string
}

// Debug discards the message.
func (l *cfnRecordingLogger) Debug(_ string, _ ...any) {}

// Info discards the message.
func (l *cfnRecordingLogger) Info(_ string, _ ...any) {}

// Warn records the message.
func (l *cfnRecordingLogger) Warn(msg string, _ ...any) {
	l.messages = append(l.messages, msg)
}

// Error records the message.
func (l *cfnRecordingLogger) Error(msg string, _ ...any) {
	l.messages = append(l.messages, msg)
}

// joined returns everything recorded, so an assertion can be made against one string.
func (l *cfnRecordingLogger) joined() string { return strings.Join(l.messages, "\n") }

// TestCFN_APolicyOnTheStackNameMatchesANonEC2Resource is #765's fourth criterion, and the end
// of the chain the stamp exists for.
//
// #746 proved this for EC2 with a bundled AWS policy; the equivalent for a bucket has to be
// written here, because no bundled policy conditions on a stack tag for S3. What it proves is
// the same thing: the tag is not merely stored but resolvable as `aws:ResourceTag/...` at
// decision time, which goes through a different reader (`AuthController.resourceTagsFor`) than
// the one the assertions above use.
//
// The negative half is what shows the condition is being evaluated rather than ignored: the
// same delete against a bucket from a stack whose name does not match is refused.
func TestCFN_APolicyOnTheStackNameMatchesANonEC2Resource(t *testing.T) {
	state := newAuthTestState(t, "deployer", "arn:aws:iam::123456789012:policy/stack-cleanup",
		emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   "Allow",
				Action:   []string{"s3:DeleteBucket"},
				Resource: []string{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {
						"aws:ResourceTag/" + cfnStampStackNameTag: {"cleanup-me"},
					},
				},
			}},
		})
	f := newCFNStampFixture(t, state)
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

	tmpl := `{"Resources": {"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {
		"BucketName": "%s"}}}}`
	matching, err := f.deployer.Deploy(context.Background(),
		strings.Replace(tmpl, "%s", "cleanup-me-bucket", 1), "cleanup-me", nil)
	require.NoError(t, err)
	other, err := f.deployer.Deploy(context.Background(),
		strings.Replace(tmpl, "%s", "leave-me-bucket", 1), "leave-me", nil)
	require.NoError(t, err)

	deleteBucket := func(bucket string) error {
		return auth.CheckAccess(
			newAuthTestReqCtx("arn:aws:iam::"+cfnStampAccount+":user/deployer"),
			&emulator.AWSRequest{
				Service:   "s3",
				Operation: "DeleteBucket",
				Path:      "/" + bucket,
				Params:    map[string]string{},
			})
	}

	require.NoError(t, deleteBucket(physicalIDs(t, matching)["Bucket"]),
		"the stamp on a bucket satisfies a condition on aws:ResourceTag/"+cfnStampStackNameTag)

	err = deleteBucket(physicalIDs(t, other)["Bucket"])
	require.Error(t, err, "a bucket from a stack the policy does not name is not covered")
	assert.Contains(t, err.Error(), "not authorized", "refused by the policy, got %v", err)
}
