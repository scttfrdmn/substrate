package emulator_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A write to state that fails is reported, not swallowed (#1175, #1192).
//
// Each case below is an operation in one of the files #1175 and #1192 touched. The sweep runs it
// once against a store that records its writes, then once per write against a fresh server whose
// store refuses that write alone, and asserts every one of those runs answers 500 — the status
// updateStringIndex's doc comment states for a store failure. Refusing each write in turn, rather
// than only the one a fix touched, is what makes the test a statement about the operation: a write
// added later that discards its error fails the sweep without anyone having to know it is there.

// wireCall is one request, in whatever shape its service's protocol takes.
type wireCall struct {
	method string
	host   string
	path   string
	// target is the X-Amz-Target header for a JSON-protocol service; empty otherwise.
	target string
	// contentType defaults to the JSON protocol's when target is set and to form encoding when
	// body starts with "Action=".
	contentType string
	body        string
}

// bind substitutes {{Name}} in path and body with the value a setup response reported under
// Name, {{Name:path}} with it path-escaped, and {{Name:query}} with it query-escaped.
func (c wireCall) bind(vars map[string]string) wireCall {
	for k, v := range vars {
		for _, form := range []struct{ suffix, value string }{
			{"", v}, {":path", url.PathEscape(v)}, {":query", url.QueryEscape(v)},
		} {
			c.path = strings.ReplaceAll(c.path, "{{"+k+form.suffix+"}}", form.value)
			c.body = strings.ReplaceAll(c.body, "{{"+k+form.suffix+"}}", form.value)
		}
	}
	return c
}

// wfMember finds `"Name":"value"` in a JSON response and `<Name>value</Name>` in an XML one.
var wfMember = regexp.MustCompile(`"(\w+)":"([^"]*)"|<(\w+)>([^<]+)</\w+>`)

// capture records each string member of a setup response, first occurrence winning, so a later
// call can name the ID the setup minted.
func capture(vars map[string]string, body string) {
	for _, m := range wfMember.FindAllStringSubmatch(body, -1) {
		k, v := m[1], m[2]
		if k == "" {
			k, v = m[3], m[4]
		}
		if _, seen := vars[k]; !seen {
			vars[k] = v
		}
	}
}

func (c wireCall) do(t *testing.T, srv *emulator.Server) (int, string) {
	t.Helper()
	method := c.method
	if method == "" {
		method = http.MethodPost
	}
	path := c.path
	if path == "" {
		path = "/"
	}
	r := httptest.NewRequest(method, path, strings.NewReader(c.body))
	r.Host = c.host
	ct := c.contentType
	switch {
	case ct != "":
	case c.target != "":
		ct = "application/x-amz-json-1.1"
	case strings.HasPrefix(c.body, "Action="):
		ct = "application/x-www-form-urlencoded"
	case strings.HasPrefix(c.body, "<"):
		ct = "application/xml"
	case c.body != "":
		ct = "application/json"
	}
	if ct != "" {
		r.Header.Set("Content-Type", ct)
	}
	if c.target != "" {
		r.Header.Set("X-Amz-Target", c.target)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

// newFullServerOn is a server with every default plugin registered over state, on a fixed
// clock so nothing the sweep compares depends on the wall clock.
func newFullServerOn(t *testing.T, state emulator.StateManager) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	tc := emulator.NewTimeController(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig(), emulator.WithTimeController(tc))
	require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc, logger, store, nil))
	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// writeFailureCase is an operation and the requests that put the state it needs in place.
type writeFailureCase struct {
	// file is the source file whose discarded write the case covers, and op the operation
	// that reaches it.
	file, op string
	setup    []wireCall
	call     wireCall
}

// label is op, or when op is empty the operation the call names: its X-Amz-Target's, its
// Action's, or its method and path.
func (tc writeFailureCase) label() string {
	switch c := tc.call; {
	case tc.op != "":
		return tc.op
	case c.target != "":
		return c.target[strings.LastIndexByte(c.target, '.')+1:]
	case strings.HasPrefix(c.body, "Action="):
		action, _, _ := strings.Cut(strings.TrimPrefix(c.body, "Action="), "&")
		return action
	default:
		return c.method + " " + c.path
	}
}

// prepare starts a server, runs the setup, and returns the call bound to what the setup minted.
func (tc writeFailureCase) prepare(t *testing.T) (*failingPutStateManager, *emulator.Server, wireCall) {
	t.Helper()
	state := newFailingPutStateManager()
	srv := newFullServerOn(t, state)
	vars := map[string]string{}
	for i, c := range tc.setup {
		code, body := c.bind(vars).do(t, srv)
		require.Less(t, code, 300, "setup call %d: %s", i, body)
		capture(vars, body)
	}
	return state, srv, tc.call.bind(vars)
}

func runWriteFailureSweep(t *testing.T, tc writeFailureCase) {
	t.Helper()
	state, srv, call := tc.prepare(t)
	state.watch()
	code, body := call.do(t, srv)
	require.Less(t, code, 300, "the operation succeeds over a store that refuses nothing: %s", body)
	written := state.writes()
	require.NotEmpty(t, written, "the operation writes to state, or the sweep proves nothing")
	t.Logf("%d writes: %v", len(written), written)

	for n, key := range written {
		state, srv, call := tc.prepare(t)
		state.failNth(n + 1)
		code, body := call.do(t, srv)
		assert.Equal(t, http.StatusInternalServerError, code,
			"refusing write %d of %d (%s) must be reported: %s", n+1, len(written), key, body)
	}
}

func TestStateWriteFailure_IsReportedNotSwallowed(t *testing.T) {
	t.Parallel()
	for _, tc := range writeFailureCases() {
		t.Run(tc.file+"/"+tc.label(), func(t *testing.T) {
			t.Parallel()
			runWriteFailureSweep(t, tc)
		})
	}
}

func jsonCall(host, target, body string) wireCall {
	return wireCall{host: host, target: target, body: body}
}

func restCall(method, host, path, body string) wireCall {
	return wireCall{method: method, host: host, path: path, body: body}
}

func queryCall(host, body string) wireCall {
	return wireCall{host: host, body: body}
}

const (
	wfRegion = ".us-east-1.amazonaws.com"
	wfRole   = "arn:aws:iam::123456789012:role/r"
)

func writeFailureCases() []writeFailureCase {
	return []writeFailureCase{
		{file: "acm_plugin.go", call: jsonCall("acm"+wfRegion, "CertificateManager.RequestCertificate",
			`{"DomainName":"example.com","ValidationMethod":"DNS"}`)},
		{file: "apigateway_plugin.go", call: restCall("POST", "apigateway"+wfRegion, "/restapis", `{"name":"api"}`)},
		{file: "apigatewayv2_plugin.go", call: restCall("POST", "apigateway"+wfRegion, "/v2/apis", `{"name":"api","protocolType":"HTTP"}`)},
		{file: "appsync_plugin.go", call: restCall("POST", "appsync"+wfRegion, "/v1/apis", `{"name":"api","authenticationType":"API_KEY"}`)},
		{file: "athena_plugin.go", call: jsonCall("athena"+wfRegion, "AmazonAthena.CreateWorkGroup", `{"Name":"wg"}`)},
		{file: "backup_plugin.go", call: restCall("PUT", "backup"+wfRegion, "/backup-vaults/vault", `{}`)},
		{file: "bedrock_runtime_plugin.go", call: restCall("POST", "bedrock"+wfRegion, "/model-invocation-job",
			`{"jobName":"job","modelId":"anthropic.claude-v2","roleArn":"`+wfRole+`","inputDataConfig":{"s3InputDataConfig":{"s3Uri":"s3://b/in"}},"outputDataConfig":{"s3OutputDataConfig":{"s3Uri":"s3://b/out"}}}`)},
		{file: "cloudtrail_plugin.go", call: jsonCall("cloudtrail"+wfRegion, "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.CreateTrail",
			`{"Name":"trail","S3BucketName":"bucket"}`)},
		{file: "cloudwatch_plugin.go", call: queryCall("monitoring"+wfRegion,
			"Action=PutMetricAlarm&Version=2010-08-01&AlarmName=a&MetricName=m&Namespace=n&Statistic=Sum&Period=60&EvaluationPeriods=1&Threshold=1&ComparisonOperator=GreaterThanThreshold")},
		{file: "cloudwatchlogs_plugin.go", call: jsonCall("logs"+wfRegion, "Logs_20140328.CreateLogGroup", `{"logGroupName":"g1"}`)},
		{file: "codebuild_plugin.go", call: jsonCall("codebuild"+wfRegion, "CodeBuild_20161006.CreateProject",
			`{"name":"p","serviceRole":"`+wfRole+`","source":{"type":"NO_SOURCE","buildspec":"version: 0.2"},"artifacts":{"type":"NO_ARTIFACTS"},"environment":{"type":"LINUX_CONTAINER","image":"aws/codebuild/standard:7.0","computeType":"BUILD_GENERAL1_SMALL"}}`)},
		{file: "codedeploy_plugin.go", call: jsonCall("codedeploy"+wfRegion, "CodeDeploy_20141006.CreateApplication", `{"applicationName":"app"}`)},
		{file: "codepipeline_plugin.go", call: jsonCall("codepipeline"+wfRegion, "CodePipeline_20150709.CreatePipeline",
			`{"pipeline":{"name":"p","roleArn":"`+wfRole+`","stages":[{"name":"Source","actions":[]},{"name":"Build","actions":[]}]}}`)},
		{file: "cognito_idp_plugin.go", call: jsonCall("cognito-idp"+wfRegion, "AWSCognitoIdentityProviderService.CreateUserPool", `{"PoolName":"pool"}`)},
		{file: "cognito_identity_plugin.go", call: jsonCall("cognito-identity"+wfRegion, "AWSCognitoIdentityService.CreateIdentityPool",
			`{"IdentityPoolName":"pool","AllowUnauthenticatedIdentities":true}`)},
		{file: "ecr_plugin.go", setup: []wireCall{
			jsonCall("api.ecr"+wfRegion, "AmazonEC2ContainerRegistry_V20150921.CreateRepository", `{"repositoryName":"repo"}`),
		}, call: jsonCall("api.ecr"+wfRegion, "AmazonEC2ContainerRegistry_V20150921.PutImage",
			`{"repositoryName":"repo","imageTag":"v1","imageManifest":"{\"schemaVersion\":2}"}`)},
		{file: "ecs_plugin.go", call: jsonCall("ecs"+wfRegion, "AmazonEC2ContainerServiceV20141113.CreateCluster", `{"clusterName":"c"}`)},
		{file: "efs_plugin.go", call: restCall("POST", "elasticfilesystem"+wfRegion, "/2015-02-01/file-systems", `{"CreationToken":"tok"}`)},
		{file: "emrserverless_plugin.go", call: restCall("POST", "emr-serverless"+wfRegion, "/applications",
			`{"name":"app","releaseLabel":"emr-6.9.0","type":"SPARK","clientToken":"tok"}`)},
		{file: "eventbridge_plugin.go", call: jsonCall("events"+wfRegion, "AWSEvents.PutRule", `{"Name":"r","ScheduleExpression":"rate(5 minutes)"}`)},
		{file: "firehose_plugin.go", call: jsonCall("firehose"+wfRegion, "Firehose_20150804.CreateDeliveryStream",
			`{"DeliveryStreamName":"s","DeliveryStreamType":"DirectPut"}`)},
		{file: "fsx_plugin.go", call: jsonCall("fsx"+wfRegion, "AWSSimbaAPIService_v20180301.CreateFileSystem",
			`{"FileSystemType":"LUSTRE","StorageCapacity":1200,"SubnetIds":["subnet-1"]}`)},
		{file: "glue_plugin.go", call: jsonCall("glue"+wfRegion, "AWSGlue.CreateDatabase", `{"DatabaseInput":{"Name":"db"}}`)},
		{file: "kinesis_plugin.go", call: jsonCall("kinesis"+wfRegion, "Kinesis_20131202.CreateStream", `{"StreamName":"s","ShardCount":1}`)},
		{file: "msk_plugin.go", call: restCall("POST", "kafka"+wfRegion, "/v1/clusters",
			`{"clusterName":"c","kafkaVersion":"3.5.1","numberOfBrokerNodes":3,"brokerNodeGroupInfo":{"instanceType":"kafka.m5.large","clientSubnets":["subnet-1","subnet-2","subnet-3"]}}`)},
		{file: "omics_plugin.go", call: restCall("POST", "omics"+wfRegion, "/run",
			`{"workflowId":"1234567","roleArn":"`+wfRole+`","outputUri":"s3://b/out","requestId":"req-1"}`)},
		{file: "ram_plugin.go", call: restCall("POST", "ram"+wfRegion, "/createresourceshare", `{"name":"share"}`)},
		{file: "redshift_plugin.go", call: queryCall("redshift"+wfRegion,
			"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=c1&NodeType=dc2.large&MasterUsername=admin&MasterUserPassword=Passw0rd1")},
		{file: "sagemaker_plugin.go", call: jsonCall("api.sagemaker"+wfRegion, "SageMaker.CreateTrainingJob", `{"TrainingJobName":"job"}`)},
		{file: "scheduler_plugin.go", call: restCall("POST", "scheduler"+wfRegion, "/schedules/s",
			`{"ScheduleExpression":"rate(5 minutes)","FlexibleTimeWindow":{"Mode":"OFF"},"Target":{"Arn":"arn:aws:sqs:us-east-1:123456789012:q","RoleArn":"`+wfRole+`"}}`)},
		{file: "sesv2_plugin.go", call: restCall("POST", "email"+wfRegion, "/v2/email/identities", `{"EmailIdentity":"a@example.com"}`)},
		{file: "ssm_plugin.go", call: jsonCall("ssm"+wfRegion, "AmazonSSM.SendCommand",
			`{"DocumentName":"AWS-RunShellScript","InstanceIds":["i-0123456789abcdef0"],"Parameters":{"commands":["true"]}}`)},
		{file: "timestream_plugin.go", call: jsonCall("ingest-cell1.timestream"+wfRegion, "Timestream_20181101.CreateDatabase", `{"DatabaseName":"db"}`)},
		{file: "transfer_plugin.go", call: jsonCall("transfer"+wfRegion, "TransferService.CreateServer", `{}`)},
		{file: "batch_plugin.go", call: restCall("POST", "batch"+wfRegion, "/v1/submitjob",
			`{"jobName":"j","jobQueue":"q","jobDefinition":"d"}`)},
		{file: "cfn_resources_v32.go", op: "CreateStack (stubStore, deployGenericStub)", call: queryCall("cloudformation"+wfRegion,
			"Action=CreateStack&Version=2010-05-15&StackName=s&TemplateBody="+url.QueryEscape(`{"Resources":{`+
				`"StubbedACL":{"Type":"AWS::WAFv2::WebACL","Properties":{"Name":"acl","Scope":"REGIONAL"}},`+
				`"Generic":{"Type":"AWS::IoT::Thing","Properties":{"ThingName":"x"}}}}`))},
		// The one resource fails on its own, so the stack rolls back and every write the sweep
		// refuses is the deployer's record of that rollback. A refused write to a resource's own
		// record is not in this case: CloudFormation reports a failed resource through the stack's
		// status, not CreateStack's (see createStack), so it answers 200 and rolls back.
		{file: "cfn_rollback.go", op: "CreateStack (rolled back)", call: queryCall("cloudformation"+wfRegion,
			"Action=CreateStack&Version=2010-05-15&StackName=s&TemplateBody="+url.QueryEscape(`{"Resources":{`+
				`"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"Not_A_Valid_Bucket"}}}}`))},
		{file: "cloudfront_oac.go", call: restCall("POST", "cloudfront.amazonaws.com", "/2020-05-31/origin-access-control",
			`<OriginAccessControlConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Name>oac</Name><SigningProtocol>sigv4</SigningProtocol><SigningBehavior>always</SigningBehavior><OriginAccessControlOriginType>s3</OriginAccessControlOriginType></OriginAccessControlConfig>`)},
		{file: "cloudfront_plugin.go", call: restCall("POST", "cloudfront.amazonaws.com", "/2020-05-31/distribution",
			`<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><CallerReference>ref</CallerReference><Comment>c</Comment><Enabled>true</Enabled></DistributionConfig>`)},
		{file: "dynamodb_plugin.go", setup: []wireCall{
			jsonCall("dynamodb"+wfRegion, "DynamoDB_20120810.CreateTable",
				`{"TableName":"t","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}`),
		}, call: jsonCall("dynamodb"+wfRegion, "DynamoDB_20120810.PutItem", `{"TableName":"t","Item":{"pk":{"S":"a"}}}`)},
		{file: "ec2_plugin.go", call: queryCall("ec2"+wfRegion,
			"Action=CreateLaunchTemplate&Version=2016-11-15&LaunchTemplateName=lt&LaunchTemplateData.ImageId=ami-12345678")},
		{file: "ec2_plugin.go", op: "RunInstances (default VPC)", call: queryCall("ec2"+wfRegion,
			"Action=RunInstances&Version=2016-11-15&ImageId="+ec2TestImage+"&InstanceType=t3.micro&MinCount=1&MaxCount=1")},
		{file: "ec2_block_devices.go", setup: []wireCall{
			queryCall("ec2"+wfRegion, "Action=RunInstances&Version=2016-11-15&ImageId="+ec2TestImage+"&InstanceType=t3.micro&MinCount=1&MaxCount=1"),
		}, call: queryCall("ec2"+wfRegion, "Action=TerminateInstances&Version=2016-11-15&InstanceId.1={{instanceId}}")},
		{file: "elasticache_plugin.go", setup: []wireCall{
			queryCall("elasticache"+wfRegion, "Action=CreateCacheCluster&Version=2015-02-02&CacheClusterId=c1&Engine=redis&CacheNodeType=cache.t3.micro&NumCacheNodes=1"),
		}, call: queryCall("elasticache"+wfRegion, "Action=DeleteCacheCluster&Version=2015-02-02&CacheClusterId=c1")},
		{file: "elb_plugin.go", setup: []wireCall{
			queryCall("elasticloadbalancing"+wfRegion, "Action=CreateLoadBalancer&Version=2015-12-01&Name=lb&Subnets.member.1=subnet-1&Subnets.member.2=subnet-2"),
		}, call: queryCall("elasticloadbalancing"+wfRegion, "Action=DeleteLoadBalancer&Version=2015-12-01&LoadBalancerArn={{LoadBalancerArn:query}}")},
		{file: "elb_classic.go", setup: []wireCall{
			queryCall("elasticloadbalancing"+wfRegion, "Action=CreateLoadBalancer&Version=2012-06-01&LoadBalancerName=clb&Listeners.member.1.Protocol=HTTP&Listeners.member.1.LoadBalancerPort=80&Listeners.member.1.InstancePort=80&AvailabilityZones.member.1=us-east-1a"),
		}, call: queryCall("elasticloadbalancing"+wfRegion, "Action=DeleteLoadBalancer&Version=2012-06-01&LoadBalancerName=clb")},
		{file: "fsx_progression.go", setup: []wireCall{
			jsonCall("fsx"+wfRegion, "AWSSimbaAPIService_v20180301.CreateFileSystem", `{"FileSystemType":"LUSTRE","StorageCapacity":1200,"SubnetIds":["subnet-1"]}`),
		}, call: jsonCall("fsx"+wfRegion, "AWSSimbaAPIService_v20180301.DeleteFileSystem", `{"FileSystemId":"{{FileSystemId}}"}`)},
		{file: "lambda_plugin.go", call: restCall("POST", "lambda"+wfRegion, "/2015-03-31/functions",
			`{"FunctionName":"f","Runtime":"python3.12","Role":"`+wfRole+`","Handler":"index.handler","Code":{"ZipFile":"UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="}}`)},
		{file: "msk_progression.go", setup: []wireCall{
			restCall("POST", "kafka"+wfRegion, "/v1/clusters",
				`{"clusterName":"c","kafkaVersion":"3.5.1","numberOfBrokerNodes":3,"brokerNodeGroupInfo":{"instanceType":"kafka.m5.large","clientSubnets":["subnet-1","subnet-2","subnet-3"]}}`),
		}, call: restCall("DELETE", "kafka"+wfRegion, "/v1/clusters/{{clusterArn:path}}", "")},
		{file: "opensearch_plugin.go", call: restCall("PUT", "my-domain.us-east-1.es.amazonaws.com", "/idx/_doc/1", `{"a":1}`)},
		{file: "rds_plugin.go", call: queryCall("rds"+wfRegion,
			"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db1&DBInstanceClass=db.t3.micro&Engine=postgres&MasterUsername=admin&MasterUserPassword=Passw0rd1&AllocatedStorage=20")},
		{file: "rds_plugin.go", setup: []wireCall{
			queryCall("rds"+wfRegion, "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db1&DBInstanceClass=db.t3.micro&Engine=postgres&MasterUsername=admin&MasterUserPassword=Passw0rd1&AllocatedStorage=20"),
		}, call: queryCall("rds"+wfRegion, "Action=DeleteDBInstance&Version=2014-10-31&DBInstanceIdentifier=db1&SkipFinalSnapshot=true")},
		{file: "redshift_progression.go", setup: []wireCall{
			queryCall("redshift"+wfRegion, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=c1&NodeType=dc2.large&MasterUsername=admin&MasterUserPassword=Passw0rd1"),
		}, call: queryCall("redshift"+wfRegion, "Action=DeleteCluster&Version=2012-12-01&ClusterIdentifier=c1&SkipFinalClusterSnapshot=true")},
		{file: "route53_plugin.go", setup: []wireCall{
			restCall("POST", "route53.amazonaws.com", "/2013-04-01/hostedzone",
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>example.com</Name><CallerReference>r1</CallerReference></CreateHostedZoneRequest>`),
		}, call: restCall("DELETE", "route53.amazonaws.com", "/2013-04-01{{Id}}", "")},
		{file: "s3_plugin.go", setup: []wireCall{
			restCall("PUT", "s3.amazonaws.com", "/bucket", ""),
			restCall("PUT", "s3.amazonaws.com", "/bucket?versioning", `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
		}, call: restCall("PUT", "s3.amazonaws.com", "/bucket/key", "x")},
		{file: "sqs_plugin.go", setup: []wireCall{
			jsonCall("sqs"+wfRegion, "AmazonSQS.CreateQueue", `{"QueueName":"q.fifo","Attributes":{"FifoQueue":"true"}}`),
		}, call: jsonCall("sqs"+wfRegion, "AmazonSQS.SendMessage",
			`{"QueueUrl":"{{QueueUrl}}","MessageBody":"m","MessageGroupId":"g","MessageDeduplicationId":"d"}`)},
		{file: "sso_plugin.go", setup: []wireCall{
			jsonCall("sso"+wfRegion, "SWBExternalService.ListInstances", `{}`),
		}, call: jsonCall("sso"+wfRegion, "SWBExternalService.CreatePermissionSet", `{"InstanceArn":"{{InstanceArn}}","Name":"ps"}`)},
		{file: "timestream_progression.go", setup: []wireCall{
			jsonCall("ingest-cell1.timestream"+wfRegion, "Timestream_20181101.CreateDatabase", `{"DatabaseName":"db"}`),
			jsonCall("ingest-cell1.timestream"+wfRegion, "Timestream_20181101.CreateTable", `{"DatabaseName":"db","TableName":"t"}`),
		}, call: jsonCall("ingest-cell1.timestream"+wfRegion, "Timestream_20181101.DeleteTable", `{"DatabaseName":"db","TableName":"t"}`)},
		{file: "wafv2_plugin.go", call: jsonCall("wafv2"+wfRegion, "AWSWAF_20190729.CreateIPSet",
			`{"Name":"s","Scope":"REGIONAL","IPAddressVersion":"IPV4","Addresses":["10.0.0.0/8"]}`)},
	}
}

// TestStateWriteFailure_AnIndexWriteRefusedFailsTheCreate is #1175's case aimed at the index key
// alone: the log group's record is written and only its entry in the name index is refused.
// The create answers InternalFailure at 500, and the partial state is the one updateStringIndex's
// doc comment documents — the record without its index entry, so the group is absent from its
// own listing and a retry of the create is refused as already existing.
func TestStateWriteFailure_AnIndexWriteRefusedFailsTheCreate(t *testing.T) {
	t.Parallel()
	const host = "logs" + wfRegion
	state := newFailingPutStateManager()
	srv := newFullServerOn(t, state)

	state.failKey("loggroup_names:")
	code, body := jsonCall(host, "Logs_20140328.CreateLogGroup", `{"logGroupName":"g1"}`).do(t, srv)
	assert.Equal(t, http.StatusInternalServerError, code, body)
	assert.Contains(t, body, "InternalFailure")
	assert.Contains(t, body, "loggroup_names:", "the error names the index it could not write")

	state.watch()
	_, listed := jsonCall(host, "Logs_20140328.DescribeLogGroups", `{}`).do(t, srv)
	assert.NotContains(t, listed, `"g1"`, "a group whose index write failed is not listed")
	code, body = jsonCall(host, "Logs_20140328.CreateLogGroup", `{"logGroupName":"g1"}`).do(t, srv)
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Contains(t, body, "ResourceAlreadyExistsException")
}

// TestStateWriteFailure_AnIndexRemovalRefusedFailsTheDelete is the removal half: a delete whose
// index entry cannot be dropped is reported rather than answered 200 over a group that would
// otherwise stay listed after it was deleted.
func TestStateWriteFailure_AnIndexRemovalRefusedFailsTheDelete(t *testing.T) {
	t.Parallel()
	const host = "logs" + wfRegion
	state := newFailingPutStateManager()
	srv := newFullServerOn(t, state)
	code, body := jsonCall(host, "Logs_20140328.CreateLogGroup", `{"logGroupName":"g1"}`).do(t, srv)
	require.Equal(t, http.StatusOK, code, body)

	state.failKey("loggroup_names:")
	code, body = jsonCall(host, "Logs_20140328.DeleteLogGroup", `{"logGroupName":"g1"}`).do(t, srv)
	assert.Equal(t, http.StatusInternalServerError, code, body)
	assert.Contains(t, body, "InternalFailure")
}

// TestStateWriteFailure_AReplayThatCannotBeCachedIsReported covers lambda_exec.go's write, which
// the sweep cannot reach: Invoke only caches a result after running the function in a container,
// which a test without Docker never does. The cache write is exercised directly instead.
func TestStateWriteFailure_AReplayThatCannotBeCachedIsReported(t *testing.T) {
	t.Parallel()
	state := newFailingPutStateManager()
	p := emulator.NewLambdaPluginForTest(state, emulator.NewTimeController(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))

	state.failEvery()
	err := p.SaveReplayForTest("arn:aws:lambda:us-east-1:123456789012:function:f", []byte(`{}`), []byte(`{}`))
	require.ErrorIs(t, err, errStoreUnavailable)
}

// fatalRecorder is a testing.TB whose Fatalf records the failure instead of ending the test, so a
// test can assert that a seed helper failed its caller.
type fatalRecorder struct {
	testing.TB
	fatals []string
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}

// TestStateWriteFailure_ASeedThatCannotBeWrittenFailsTheTest covers testing.go's three writes.
// The seed helpers return nothing, and #1192 kept their exported signatures: a returned error
// would be one more value every caller could drop. They fail the test that started the server
// instead, so a test never runs against a seed that was not stored.
func TestStateWriteFailure_ASeedThatCannotBeWrittenFailsTheTest(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		match string
		seed  func(ts *emulator.TestServer)
	}{
		{"SeedSSMParameter record", "parameter:", func(ts *emulator.TestServer) { ts.SeedSSMParameter("/a/b", "v") }},
		{"SeedSSMParameter paths index", "parameter_paths:", func(ts *emulator.TestServer) { ts.SeedSSMParameter("/a/b", "v") }},
		{"SeedEC2Image", "image:", func(ts *emulator.TestServer) { ts.SeedEC2Image("ami-0123456789abcdef0", "img") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			state := newFailingPutStateManager()
			rec := &fatalRecorder{TB: t}
			ts.RedirectSeedsForTest(rec, state)

			state.failKey(tc.match)
			tc.seed(ts)
			require.Len(t, rec.fatals, 1, "the refused write fails the test exactly once")
			assert.Contains(t, rec.fatals[0], errStoreUnavailable.Error())
		})
	}
}
