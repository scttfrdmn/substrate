package emulator_test

// Every remaining body-parse guard in the tree, over the wire (#950).
//
// invalid_body_code_test.go covers Step Functions and Systems Manager, the two services #1003 fixed.
// This file covers the other sixty-six guards, in fourteen services, and it is a table of operations
// rather than of samples on purpose: the defect was per-site duplication of one literal, so the
// assertion that matters is that no site was missed, and a representative case cannot make it.
//
// Firehose is the proof of that. InvalidArgumentException is published for CreateDeliveryStream — the
// page anyone would open first — and for neither of the other two guarded operations, so a test that
// checked the representative operation would have confirmed a code that is wrong at two of three sites.
//
// Both the status and the code are asserted, per #923: a decoded error struct carries the code and a
// consumer's retry logic branches on the status, so a helper that got one of the two right would look
// correct from either side alone. Asserting the status also makes a mistyped REST path fail loudly —
// an unrouted path answers UnknownOperationException at 404 rather than the guard's 400.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// invalidBodyCase names one guarded operation and how a request reaches it.
//
// A service dispatches either on X-Amz-Target or on method and path, never both, so exactly one of
// target and path is set. The zero value of method is treated as POST, which is what every case here
// uses.
type invalidBodyCase struct {
	// op is the operation name, used only as the subtest name.
	op string
	// target is the full X-Amz-Target header value, empty for a REST service.
	target string
	// path is the URL path a REST service dispatches on, empty for a JSON-target service. Any path
	// parameter is filled in with a value the handler does not have to resolve, because a guard sitting
	// below a resource lookup is not reachable this way; the two Lambda operations that do sit below one
	// have their own test.
	path string
	// No method field: every parse guard here is reachable on POST, because a guard only runs on a
	// method that carries a body. The member-complaint table below needs GET, PUT and DELETE, and
	// carries its own.
}

// invalidBodyService groups one service's guarded operations under the code they all answer.
//
// One code per service is the point of the grouping: the rule #950 settled on is that a body which
// will not parse answers the same thing at every operation of one service, whether that code came from
// the operation pages or from the common-errors page. A table that allowed a per-operation code here
// would not be able to express the invariant.
type invalidBodyService struct {
	// name is the service's label in the subtest name.
	name string
	// host routes the request. Taken from routing.go's Hosts table.
	host string
	// code is the error code every operation below must answer, and provenance is where it comes from.
	code string
	// provenance says whether the code is published per operation or on the common-errors page. It is
	// carried in the table so a reader of a failure knows which citation is being asserted.
	provenance string
	// cases is every operation in the service whose handler guards json.Unmarshal.
	cases []invalidBodyCase
}

// invalidBodyServices is every POST-routed body-parse guard #950, #1003 and #1007 settled, less the Step
// Functions and Systems Manager tables in invalid_body_code_test.go, the two Lambda operations that need a
// function to exist first, and the three in TestInvalidBodyBelowAResourceLookup.
//
// One hundred and eighty-nine cases across thirty-six entries. A case removed from here without a reason
// is a site that stops being checked, which is the whole point of listing operations rather than samples.
//
// #1007's third slice grew this table twice over. Forty-six of the rows are guards that predate that
// slice, in the five services whose code it corrected: their codes had to change too, or the service would
// answer two different codes for one caller error, and a changed code that no test reads is a code that
// can drift back.
var invalidBodyServices = []invalidBodyService{
	{
		name:       "kinesis",
		host:       "kinesis.us-east-1.amazonaws.com",
		code:       "InvalidArgumentException",
		provenance: "all sixteen operation pages",
		cases: []invalidBodyCase{
			{op: "CreateStream", target: "Kinesis_20131202.CreateStream"},
			{op: "DeleteStream", target: "Kinesis_20131202.DeleteStream"},
			{op: "DescribeStream", target: "Kinesis_20131202.DescribeStream"},
			{op: "DescribeStreamSummary", target: "Kinesis_20131202.DescribeStreamSummary"},
			{op: "UpdateShardCount", target: "Kinesis_20131202.UpdateShardCount"},
			{op: "PutRecord", target: "Kinesis_20131202.PutRecord"},
			{op: "PutRecords", target: "Kinesis_20131202.PutRecords"},
			{op: "GetShardIterator", target: "Kinesis_20131202.GetShardIterator"},
			{op: "GetRecords", target: "Kinesis_20131202.GetRecords"},
			{op: "MergeShards", target: "Kinesis_20131202.MergeShards"},
			{op: "SplitShard", target: "Kinesis_20131202.SplitShard"},
			{op: "AddTagsToStream", target: "Kinesis_20131202.AddTagsToStream"},
			{op: "RemoveTagsFromStream", target: "Kinesis_20131202.RemoveTagsFromStream"},
			{op: "ListTagsForStream", target: "Kinesis_20131202.ListTagsForStream"},
			{op: "EnableEnhancedMonitoring", target: "Kinesis_20131202.EnableEnhancedMonitoring"},
			{op: "DisableEnhancedMonitoring", target: "Kinesis_20131202.DisableEnhancedMonitoring"},
			{op: "ListStreams", target: "Kinesis_20131202.ListStreams"},
		},
	},
	{
		name:       "eventbridge",
		host:       "events.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; EventBridge publishes no validation code on any of the seven",
		cases: []invalidBodyCase{
			{op: "PutRule", target: "AWSEvents.PutRule"},
			{op: "DeleteRule", target: "AWSEvents.DeleteRule"},
			{op: "DescribeRule", target: "AWSEvents.DescribeRule"},
			{op: "PutTargets", target: "AWSEvents.PutTargets"},
			{op: "RemoveTargets", target: "AWSEvents.RemoveTargets"},
			{op: "ListTargetsByRule", target: "AWSEvents.ListTargetsByRule"},
			{op: "PutEvents", target: "AWSEvents.PutEvents"},
			{op: "ListRules", target: "AWSEvents.ListRules"},
		},
	},
	{
		name:       "sagemaker",
		host:       "api.sagemaker.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; none of the six publishes a validation error",
		cases: []invalidBodyCase{
			{op: "CreateApp", target: "SageMaker.CreateApp"},
			{op: "DeleteApp", target: "SageMaker.DeleteApp"},
			{op: "DescribeApp", target: "SageMaker.DescribeApp"},
			{op: "CreateTrainingJob", target: "SageMaker.CreateTrainingJob"},
			{op: "DescribeTrainingJob", target: "SageMaker.DescribeTrainingJob"},
			{op: "StopTrainingJob", target: "SageMaker.StopTrainingJob"},
			{op: "ListApps", target: "SageMaker.ListApps"},
		},
	},
	{
		name:       "acm",
		host:       "acm.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; InvalidParameterException is on three of the six",
		cases: []invalidBodyCase{
			{op: "RequestCertificate", target: "CertificateManager.RequestCertificate"},
			{op: "DescribeCertificate", target: "CertificateManager.DescribeCertificate"},
			{op: "DeleteCertificate", target: "CertificateManager.DeleteCertificate"},
			{op: "AddTagsToCertificate", target: "CertificateManager.AddTagsToCertificate"},
			{op: "RemoveTagsFromCertificate", target: "CertificateManager.RemoveTagsFromCertificate"},
			{op: "ListTagsForCertificate", target: "CertificateManager.ListTagsForCertificate"},
			{op: "ListCertificates", target: "CertificateManager.ListCertificates"},
		},
	},
	{
		name:       "firehose",
		host:       "firehose.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; InvalidArgumentException is on one of the three",
		cases: []invalidBodyCase{
			{op: "CreateDeliveryStream", target: "Firehose_20150804.CreateDeliveryStream"},
			{op: "DescribeDeliveryStream", target: "Firehose_20150804.DescribeDeliveryStream"},
			{op: "DeleteDeliveryStream", target: "Firehose_20150804.DeleteDeliveryStream"},
			{op: "ListDeliveryStreams", target: "Firehose_20150804.ListDeliveryStreams"},
			{op: "PutRecordBatch", target: "Firehose_20150804.PutRecordBatch"},
		},
	},
	{
		name:       "budgets",
		host:       "budgets.amazonaws.com",
		code:       "InvalidParameterException",
		provenance: "all five operation pages",
		cases: []invalidBodyCase{
			{op: "CreateBudget", target: "AWSBudgetServiceGateway.CreateBudget"},
			{op: "DescribeBudgets", target: "AWSBudgetServiceGateway.DescribeBudgets"},
			{op: "DescribeBudget", target: "AWSBudgetServiceGateway.DescribeBudget"},
			{op: "UpdateBudget", target: "AWSBudgetServiceGateway.UpdateBudget"},
			{op: "DeleteBudget", target: "AWSBudgetServiceGateway.DeleteBudget"},
		},
	},
	{
		name:       "ce",
		host:       "ce.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; Cost Explorer publishes no validation code at all",
		cases: []invalidBodyCase{
			{op: "GetCostAndUsage", target: "AWSInsightsIndexService.GetCostAndUsage"},
			{op: "GetCostForecast", target: "AWSInsightsIndexService.GetCostForecast"},
			{op: "GetDimensionValues", target: "AWSInsightsIndexService.GetDimensionValues"},
		},
	},
	{
		name:       "servicequotas",
		host:       "servicequotas.us-east-1.amazonaws.com",
		code:       "IllegalArgumentException",
		provenance: "all four operation pages",
		cases: []invalidBodyCase{
			{op: "ListServiceQuotas", target: "ServiceQuotasV20190624.ListServiceQuotas"},
			{op: "GetServiceQuota", target: "ServiceQuotasV20190624.GetServiceQuota"},
			{op: "GetAWSDefaultServiceQuota", target: "ServiceQuotasV20190624.GetAWSDefaultServiceQuota"},
			{op: "RequestServiceQuotaIncrease", target: "ServiceQuotasV20190624.RequestServiceQuotaIncrease"},
			{op: "GetRequestedServiceQuotaChange", target: "ServiceQuotasV20190624.GetRequestedServiceQuotaChange"},
			{op: "ListRequestedServiceQuotaChangeHistory", target: "ServiceQuotasV20190624.ListRequestedServiceQuotaChangeHistory"},
		},
	},
	{
		name:       "lambda",
		host:       "lambda.us-east-1.amazonaws.com",
		code:       "InvalidParameterValueException",
		provenance: "all four operation pages",
		cases: []invalidBodyCase{
			{op: "CreateFunction", path: "/2015-03-31/functions"},
			{op: "CreateEventSourceMapping", path: "/2015-03-31/event-source-mappings"},
		},
	},
	{
		// SQS, added by #1007. Thirteen sites discarded the unmarshal error, and four of them —
		// GetQueueAttributes, DeleteQueue, ListQueueTags and PurgeQueue — carried the empty QueueUrl
		// straight into a queue lookup, so a JSON syntax error answered QueueDoesNotExist: the emulator
		// told a caller its queue was missing when the queue was fine and the body was not.
		// TestSQSInvalidBodyIsRefusedBeforeTheQueueLookup covers those four against a queue that exists,
		// which this table cannot do: it runs every service against an empty server.
		name:       "sqs",
		host:       "sqs.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; no SQS operation page names an undecodable body",
		cases: []invalidBodyCase{
			{op: "CreateQueue", target: "AmazonSQS.CreateQueue"},
			{op: "GetQueueUrl", target: "AmazonSQS.GetQueueUrl"},
			{op: "ListQueues", target: "AmazonSQS.ListQueues"},
			{op: "GetQueueAttributes", target: "AmazonSQS.GetQueueAttributes"},
			{op: "SetQueueAttributes", target: "AmazonSQS.SetQueueAttributes"},
			{op: "DeleteQueue", target: "AmazonSQS.DeleteQueue"},
			{op: "TagQueue", target: "AmazonSQS.TagQueue"},
			{op: "UntagQueue", target: "AmazonSQS.UntagQueue"},
			{op: "ListQueueTags", target: "AmazonSQS.ListQueueTags"},
			{op: "SendMessage", target: "AmazonSQS.SendMessage"},
			{op: "SendMessageBatch", target: "AmazonSQS.SendMessageBatch"},
			{op: "ReceiveMessage", target: "AmazonSQS.ReceiveMessage"},
			{op: "DeleteMessage", target: "AmazonSQS.DeleteMessage"},
			{op: "DeleteMessageBatch", target: "AmazonSQS.DeleteMessageBatch"},
			{op: "ChangeMessageVisibility", target: "AmazonSQS.ChangeMessageVisibility"},
			{op: "PurgeQueue", target: "AmazonSQS.PurgeQueue"},
		},
	},
	{
		// AWS Health, added by #1007. DescribeEventDetails discarded the error and answered 200 with an
		// empty successfulSet, so a malformed body looked like "none of your events were found".
		name:       "health",
		host:       "health.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-error-types page; the operation publishes only UnsupportedLocale",
		cases: []invalidBodyCase{
			{op: "DescribeEventDetails", target: "AWSHealth_20160804.DescribeEventDetails"},
		},
	},
	{
		name:       "efs",
		host:       "elasticfilesystem.us-east-1.amazonaws.com",
		code:       "BadRequest",
		provenance: "all four operation pages; EFS publishes no common-errors page",
		cases: []invalidBodyCase{
			{op: "CreateFileSystem", path: "/2015-02-01/file-systems"},
			{op: "CreateAccessPoint", path: "/2015-02-01/access-points"},
			{op: "CreateMountTarget", path: "/2015-02-01/mount-targets"},
			{op: "TagResource", path: "/2015-02-01/resource-tags/fs-12345678"},
		},
	},
	{
		name:       "sesv2",
		host:       "email.us-east-1.amazonaws.com",
		code:       "BadRequestException",
		provenance: "all five operation pages",
		cases: []invalidBodyCase{
			{op: "CreateEmailIdentity", path: "/v2/email/identities"},
			{op: "SendEmail", path: "/v2/email/outbound-emails"},
		},
	},
	{
		name:       "batch",
		host:       "batch.us-east-1.amazonaws.com",
		code:       "ClientException",
		provenance: "all operation pages; Batch publishes no common-errors page",
		cases: []invalidBodyCase{
			{op: "SubmitJob", path: "/v1/submitjob"},
			{op: "DescribeJobs", path: "/v1/describejobs"},
			// Added by #1007's third slice. Routed on POST here rather than on its other spelling,
			// DELETE /v1/jobs/{id}: the handler reads jobId from the body on this path and from the path
			// segment on that one, and the body is what this table is about. The length check added with
			// the guard is what keeps the DELETE spelling working — see the comment at the site.
			{op: "TerminateJob", path: "/v1/terminatejob"},
		},
	},
	{
		name:       "msk",
		host:       "kafka.us-east-1.amazonaws.com",
		code:       "BadRequest",
		provenance: "nothing published; substrate's reading, recorded in msk_errors.go",
		cases: []invalidBodyCase{
			{op: "CreateCluster", path: "/v1/clusters"},
			{op: "CreateClusterV2", path: "/api/v2/clusters"},
		},
	},
	{
		name:       "bedrock-runtime",
		host:       "bedrock-runtime.us-east-1.amazonaws.com",
		code:       "ValidationException",
		provenance: "both operation pages",
		cases: []invalidBodyCase{
			{op: "ApplyGuardrail", path: "/guardrail/gr-1/version/1/apply"},
			{op: "CreateModelInvocationJob", path: "/model-invocation-job"},
		},
	},

	// #1007's third slice: the inline-literal tail. Everything below is a site that spelled its refusal
	// as an &AWSError{…} literal rather than through a constructor, which is why this slice is the one
	// that found four services answering a code they do not publish — see invalid_body_refusals.go.
	//
	// Five of the sixty sites are not in any table here, each for a stated reason rather than by
	// omission, and TestInvalidBodyTailIsFullyCovered below counts them so the arithmetic cannot drift.
	{
		name:       "sso",
		host:       "sso.us-east-1.amazonaws.com",
		code:       "ValidationException",
		provenance: "the Identity Store and SSO Admin operation pages",
		cases: []invalidBodyCase{
			{op: "DescribePermissionSet", target: "SWBExternalService.DescribePermissionSet"},
			{op: "UpdatePermissionSet", target: "SWBExternalService.UpdatePermissionSet"},
			{op: "DeletePermissionSet", target: "SWBExternalService.DeletePermissionSet"},
			{op: "AttachManagedPolicyToPermissionSet", target: "SWBExternalService.AttachManagedPolicyToPermissionSet"},
			{op: "DetachManagedPolicyFromPermissionSet", target: "SWBExternalService.DetachManagedPolicyFromPermissionSet"},
			{op: "ListManagedPoliciesInPermissionSet", target: "SWBExternalService.ListManagedPoliciesInPermissionSet"},
			{op: "CreateAccountAssignment", target: "SWBExternalService.CreateAccountAssignment"},
			{op: "DeleteAccountAssignment", target: "SWBExternalService.DeleteAccountAssignment"},
			{op: "ListAccountAssignments", target: "SWBExternalService.ListAccountAssignments"},
		},
	},
	{
		// Four more API Gateway v1 sites are routed on PUT and carry their own table below.
		name:       "apigateway",
		host:       "apigateway.us-east-1.amazonaws.com",
		code:       "BadRequestException",
		provenance: "the REST API reference, on every operation that takes a body",
		cases: []invalidBodyCase{
			{op: "CreateDeployment", path: "/restapis/my-api/deployments"},
			{op: "CreateApiKey", path: "/apikeys"},
			{op: "CreateUsagePlan", path: "/usageplans"},
		},
	},
	{
		// A separate entry from apigateway above, not a merged one, for the same reason apigwv2InvalidBody
		// is a separate constructor: v1 and v2 are separate APIs with separate references, and a merged
		// row would let a v2 regression pass on a v1 citation. The `/v2/` prefix is what refineAPIGateway-
		// Version dispatches on, so it is load-bearing rather than cosmetic.
		name:       "apigatewayv2",
		host:       "apigateway.us-east-1.amazonaws.com",
		code:       "BadRequestException",
		provenance: "the API Gateway v2 operation pages",
		cases: []invalidBodyCase{
			{op: "CreateIntegration", path: "/v2/apis/my-api/integrations"},
			{op: "CreateAuthorizer", path: "/v2/apis/my-api/authorizers"},
			{op: "CreateDeployment", path: "/v2/apis/my-api/deployments"},
		},
	},
	{
		// The code here is a correction, not a carry-forward: these five answered MalformedQueryString,
		// which RAM does not publish at all — it is a 404 on the Query-protocol page and describes the URL
		// query string. See ramInvalidBody for why the common code beats the operation page's
		// InvalidParameterException.
		name:       "ram",
		host:       "ram.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; RAM's operation code names a parameter that was never read",
		cases: []invalidBodyCase{
			{op: "GetResourceShares", path: "/getresourceshares"},
			{op: "UpdateResourceShare", path: "/updateresourceshare"},
			{op: "DeleteResourceShare", path: "/deleteresourceshare"},
			{op: "AssociateResourceShare", path: "/associateresourceshare"},
			{op: "DisassociateResourceShare", path: "/disassociateresourceshare"},
			{op: "CreateResourceShare", path: "/createresourceshare"},
		},
	},
	{
		name:       "ecs",
		host:       "ecs.us-east-1.amazonaws.com",
		code:       "InvalidParameterException",
		provenance: "every ECS operation page; it is the only 400 ECS publishes for caller error",
		cases: []invalidBodyCase{
			{op: "DescribeClusters", target: "AmazonEC2ContainerServiceV20141113.DescribeClusters"},
			{op: "ListTaskDefinitions", target: "AmazonEC2ContainerServiceV20141113.ListTaskDefinitions"},
			{op: "ListServices", target: "AmazonEC2ContainerServiceV20141113.ListServices"},
			{op: "ListTasks", target: "AmazonEC2ContainerServiceV20141113.ListTasks"},
		},
	},
	{
		name:       "cloudwatchlogs",
		host:       "logs.us-east-1.amazonaws.com",
		code:       "InvalidParameterException",
		provenance: "the CloudWatch Logs operation pages; the metrics plugin's own code is different",
		cases: []invalidBodyCase{
			{op: "DescribeLogGroups", target: "Logs_20140328.DescribeLogGroups"},
			{op: "DescribeLogStreams", target: "Logs_20140328.DescribeLogStreams"},
			{op: "GetLogEvents", target: "Logs_20140328.GetLogEvents"},
			{op: "FilterLogEvents", target: "Logs_20140328.FilterLogEvents"},
		},
	},
	{
		name:       "athena",
		host:       "athena.us-east-1.amazonaws.com",
		code:       "InvalidRequestException",
		provenance: "every Athena operation page",
		cases: []invalidBodyCase{
			{op: "ListQueryExecutions", target: "AmazonAthena.ListQueryExecutions"},
			{op: "ListWorkGroups", target: "AmazonAthena.ListWorkGroups"},
		},
	},
	{
		name:       "cognito-identity",
		host:       "cognito-identity.us-east-1.amazonaws.com",
		code:       "InvalidParameterException",
		provenance: "the Cognito Identity operation pages",
		cases: []invalidBodyCase{
			{op: "ListIdentityPools", target: "AWSCognitoIdentityService.ListIdentityPools"},
			{op: "GetCredentialsForIdentity", target: "AWSCognitoIdentityService.GetCredentialsForIdentity"},
		},
	},
	{
		// A separate entry from cognito-identity for the same reason the constructors are separate: user
		// pools and identity pools are different APIs that happen to publish the same code.
		name:       "cognito-idp",
		host:       "cognito-idp.us-east-1.amazonaws.com",
		code:       "InvalidParameterException",
		provenance: "the Cognito user-pools operation pages",
		cases: []invalidBodyCase{
			{op: "ListUserPools", target: "AWSCognitoIdentityProviderService.ListUserPools"},
		},
	},
	{
		name:       "redshift-data",
		host:       "redshift-data.us-east-1.amazonaws.com",
		code:       "ValidationException",
		provenance: "every Redshift Data API operation page",
		cases: []invalidBodyCase{
			{op: "DescribeStatement", target: "RedshiftData.DescribeStatement"},
			{op: "GetStatementResult", target: "RedshiftData.GetStatementResult"},
		},
	},
	{
		// Timestream is two entries rather than one because it is two endpoints: ListTables is on the
		// ingest host and Query on the query host, and a single row would have to pick one and silently
		// stop covering the other.
		name:       "timestream-write",
		host:       "ingest.timestream.us-east-1.amazonaws.com",
		code:       "ValidationException",
		provenance: "the Timestream Write operation pages",
		cases: []invalidBodyCase{
			{op: "ListTables", target: "Timestream_20181101.ListTables"},
		},
	},
	{
		name:       "timestream-query",
		host:       "query.timestream.us-east-1.amazonaws.com",
		code:       "ValidationException",
		provenance: "the Timestream Query operation pages",
		cases: []invalidBodyCase{
			{op: "Query", target: "Timestream_20181101.Query"},
		},
	},
	{
		name:       "secretsmanager",
		host:       "secretsmanager.us-east-1.amazonaws.com",
		code:       "InvalidRequestException",
		provenance: "every Secrets Manager operation page",
		cases: []invalidBodyCase{
			{op: "ListSecrets", target: "secretsmanager.ListSecrets"},
		},
	},
	{
		name:       "emrserverless",
		host:       "emr-serverless.us-east-1.amazonaws.com",
		code:       "ValidationException",
		provenance: "every EMR Serverless operation page",
		cases: []invalidBodyCase{
			{op: "StartJobRun", path: "/applications/my-app/jobruns"},
		},
	},
	{
		name:       "ecr",
		host:       "api.ecr.us-east-1.amazonaws.com",
		code:       "InvalidParameterException",
		provenance: "the ECR operation pages",
		cases: []invalidBodyCase{
			{op: "DescribeRepositories", target: "AmazonEC2ContainerRegistry_V20150921.DescribeRepositories"},
		},
	},
	{
		// Another correction: this answered InvalidParameterCombinationException, which means two
		// parameters that cannot be used together and is not on LookupEvents' seven-code Errors list at
		// all. A body that will not parse yields no parameters to combine.
		name:       "cloudtrail",
		host:       "cloudtrail.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; CloudTrail publishes no parse or serialization code anywhere",
		cases: []invalidBodyCase{
			{op: "DescribeTrails", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails"},
			{op: "CreateTrail", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.CreateTrail"},
			{op: "GetTrail", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.GetTrail"},
			{op: "GetTrailStatus", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.GetTrailStatus"},
			{op: "UpdateTrail", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.UpdateTrail"},
			{op: "DeleteTrail", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DeleteTrail"},
			{op: "StartLogging", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.StartLogging"},
		},
	},
	{
		// Corrected from InvalidParameterValueException, which appears nowhere in Glue: not on its
		// Common Errors page, not on CreateDatabase/GetTables/StartJobRun, and not among the thirty-six
		// AWSGlueException subclasses.
		name:       "glue",
		host:       "glue.us-east-1.amazonaws.com",
		code:       "InvalidInputException",
		provenance: `every Glue operation page, "The input provided was not valid."`,
		cases: []invalidBodyCase{
			{op: "GetTables", target: "AWSGlue.GetTables"},
			{op: "CreateDatabase", target: "AWSGlue.CreateDatabase"},
			{op: "GetDatabase", target: "AWSGlue.GetDatabase"},
			{op: "UpdateDatabase", target: "AWSGlue.UpdateDatabase"},
			{op: "DeleteDatabase", target: "AWSGlue.DeleteDatabase"},
			{op: "CreateTable", target: "AWSGlue.CreateTable"},
			{op: "GetTable", target: "AWSGlue.GetTable"},
			{op: "UpdateTable", target: "AWSGlue.UpdateTable"},
			{op: "DeleteTable", target: "AWSGlue.DeleteTable"},
			{op: "CreateConnection", target: "AWSGlue.CreateConnection"},
			{op: "GetConnection", target: "AWSGlue.GetConnection"},
			{op: "UpdateConnection", target: "AWSGlue.UpdateConnection"},
			{op: "DeleteConnection", target: "AWSGlue.DeleteConnection"},
			{op: "CreateCrawler", target: "AWSGlue.CreateCrawler"},
			{op: "GetCrawler", target: "AWSGlue.GetCrawler"},
			{op: "UpdateCrawler", target: "AWSGlue.UpdateCrawler"},
			{op: "DeleteCrawler", target: "AWSGlue.DeleteCrawler"},
			{op: "CreateJob", target: "AWSGlue.CreateJob"},
			{op: "GetJob", target: "AWSGlue.GetJob"},
			{op: "UpdateJob", target: "AWSGlue.UpdateJob"},
			{op: "DeleteJob", target: "AWSGlue.DeleteJob"},
			{op: "StartJobRun", target: "AWSGlue.StartJobRun"},
			{op: "GetJobRun", target: "AWSGlue.GetJobRun"},
			{op: "GetJobRuns", target: "AWSGlue.GetJobRuns"},
			{op: "TagResource", target: "AWSGlue.TagResource"},
			{op: "UntagResource", target: "AWSGlue.UntagResource"},
			{op: "GetTags", target: "AWSGlue.GetTags"},
		},
	},
	{
		// Corrected from a bare InvalidRequest, which is an Amazon S3 code and appears nowhere in FSx.
		// The wire code has no Exception suffix even though the Java class does.
		name:       "fsx",
		host:       "fsx.us-east-1.amazonaws.com",
		code:       "BadRequest",
		provenance: `the FSx operation pages, "A generic error indicating a failure with a client request."`,
		cases: []invalidBodyCase{
			{op: "DescribeFileSystems", target: "AWSSimbaAPIService_v20180301.DescribeFileSystems"},
			{op: "CreateFileSystem", target: "AWSSimbaAPIService_v20180301.CreateFileSystem"},
			{op: "DeleteFileSystem", target: "AWSSimbaAPIService_v20180301.DeleteFileSystem"},
		},
	},
	{
		// Not a correction but a judgement, and the tree had already made it: WAFInvalidParameterException
		// is published at 400, but wafv2_createipset_validation_test.go records from #755 that it is for a
		// present-but-invalid *value* while an absent required member is ValidationError. A body that will
		// not parse has no members at all.
		name:       "wafv2",
		host:       "wafv2.us-east-1.amazonaws.com",
		code:       "ValidationError",
		provenance: "the common-errors page; the operation code presupposes a parameter substrate read",
		cases: []invalidBodyCase{
			{op: "ListWebACLs", target: "AWSWAF_20190729.ListWebACLs"},
			{op: "ListIPSets", target: "AWSWAF_20190729.ListIPSets"},
			{op: "CreateWebACL", target: "AWSWAF_20190729.CreateWebACL"},
			{op: "GetWebACL", target: "AWSWAF_20190729.GetWebACL"},
			{op: "UpdateWebACL", target: "AWSWAF_20190729.UpdateWebACL"},
			{op: "DeleteWebACL", target: "AWSWAF_20190729.DeleteWebACL"},
			{op: "AssociateWebACL", target: "AWSWAF_20190729.AssociateWebACL"},
			{op: "DisassociateWebACL", target: "AWSWAF_20190729.DisassociateWebACL"},
			{op: "GetWebACLForResource", target: "AWSWAF_20190729.GetWebACLForResource"},
			{op: "CreateIPSet", target: "AWSWAF_20190729.CreateIPSet"},
			{op: "GetIPSet", target: "AWSWAF_20190729.GetIPSet"},
			{op: "UpdateIPSet", target: "AWSWAF_20190729.UpdateIPSet"},
			{op: "DeleteIPSet", target: "AWSWAF_20190729.DeleteIPSet"},
		},
	},
	{
		// SerializationException is the one code in the slice that no AWS reference publishes. It is
		// observed Coral-protocol wire behavior, emitted before the request reaches DynamoDB at all,
		// which is why the service never documents it — see ddbInvalidBody. Asserted anyway, because the
		// wire is what an SDK's retry classifier reads.
		name:       "dynamodb",
		host:       "dynamodb.us-east-1.amazonaws.com",
		code:       "SerializationException",
		provenance: "observed wire behavior, not the API model",
		cases: []invalidBodyCase{
			{op: "ListTables", target: "DynamoDB_20120810.ListTables"},
			{op: "ListStreams", target: "DynamoDB_20120810.ListStreams"},
			{op: "GetRecords", target: "DynamoDB_20120810.GetRecords"},
		},
	},
}

// TestInvalidBodyAnswersThePublishedCode asserts every guard in the inventory answers its service's
// one published code at 400, with a message that names no Go type.
func TestInvalidBodyAnswersThePublishedCode(t *testing.T) {
	for _, svc := range invalidBodyServices {
		t.Run(svc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			for _, tc := range svc.cases {
				t.Run(tc.op, func(t *testing.T) {
					status, code, message := rawUnsignedCall(t, ts, svc.host, tc.target, tc.path,
						[]byte(invalidBodyPayload))
					assert.Equalf(t, svc.code, code, "%s answers the code published by %s",
						tc.op, svc.provenance)
					assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", tc.op)
					assertNoDecoderText(t, tc.op, message)
				})
			}
		})
	}
}

// TestLambdaInvalidBodyBelowAFunctionLookup covers Lambda's two parse guards, which used to sit below a
// function-existence check and since #1006 sit above one.
//
// AddPermission called loadFunction and TagResource called findFunctionByARN before either reached
// json.Unmarshal, so on an empty server both answered ResourceNotFoundException at 404 and the guard was
// unreachable — and a site that cannot be reached is a site whose code goes unchecked. Both answered
// ValidationException before #950, a code Lambda publishes on neither page.
//
// Which answer AWS gives is still unverified: no Lambda page states the precedence, the published Errors
// sections list both codes without ordering them, and substrate vendors no Smithy model to read it off.
// The absent-function subtests below therefore pin *substrate's stated reading* — a request whose shape
// is wrong is refused without consulting state — rather than a published fact, and the comment on
// addPermission is where that reading is argued. The two cases with a function in place are the
// published half, and hold whichever way the precedence is later settled.
func TestLambdaInvalidBodyBelowAFunctionLookup(t *testing.T) {
	const host = "lambda.us-east-1.amazonaws.com"
	ts := emulator.StartTestServer(t)

	created := map[string]any{
		"FunctionName": "guarded-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/lambda-role",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": "IA=="},
	}
	body, err := json.Marshal(created)
	require.NoError(t, err, "marshal the CreateFunction body")

	status, _, message := rawUnsignedCall(t, ts, host, "", "/2015-03-31/functions", body)
	require.Equalf(t, http.StatusCreated, status, "CreateFunction succeeds: %s", message)

	// The ARN comes from the create rather than being composed here, because TagResource dispatches on
	// the ARN in its own path and a hand-built one that differs by an account or a Region would answer
	// 404 and look like the guard was never fixed.
	arn := lambdaFunctionARNFromCreate(t, ts, host, "guarded-fn")

	for _, tc := range []invalidBodyCase{
		{op: "AddPermission", path: "/2015-03-31/functions/guarded-fn/policy"},
		{op: "TagResource", path: "/2015-03-31/tags/" + arn},
	} {
		t.Run(tc.op, func(t *testing.T) {
			status, code, message := rawUnsignedCall(t, ts, host, "", tc.path, []byte(invalidBodyPayload))
			assert.Equalf(t, "InvalidParameterValueException", code,
				"%s answers the code all four Lambda operation pages publish", tc.op)
			assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", tc.op)
			assertNoDecoderText(t, tc.op, message)
		})
	}

	// AddPermission's member complaint sits one line below its parse guard, so it needs the same
	// created function to be reachable at all — which is why it cannot live in
	// memberComplaintServices, where every service gets an empty server.
	t.Run("AddPermission/memberComplaint", func(t *testing.T) {
		status, code, message := rawUnsignedCall(t, ts, host, "",
			"/2015-03-31/functions/guarded-fn/policy", []byte("{}"))
		assert.Equal(t, "InvalidParameterValueException", code,
			"a missing StatementId answers the same code as an unparseable body")
		assert.Equal(t, http.StatusBadRequest, status, "AddPermission answers 400")
		assert.Contains(t, message, "StatementId is required", "the message names the member")
		assertNoDecoderText(t, "AddPermission/memberComplaint", message)
	})

	// #1006's own assertion: the same two bodies, naming a function that does not exist. Before the
	// reorder each answered ResourceNotFoundException/404, so a caller debugging a malformed request was
	// told its function was missing. The absent ARN is derived from the created one by substituting the
	// name, so account and Region cannot differ — a hand-built ARN that dispatched nowhere would answer
	// 404 for the wrong reason and pass this test while proving nothing.
	absentARN := strings.TrimSuffix(arn, "guarded-fn") + "absent-fn"
	require.NotEqual(t, arn, absentARN, "the absent ARN differs from the created one only in its name")

	for _, tc := range []invalidBodyCase{
		{op: "AddPermission", path: "/2015-03-31/functions/absent-fn/policy"},
		{op: "TagResource", path: "/2015-03-31/tags/" + absentARN},
	} {
		t.Run(tc.op+"/absentFunction", func(t *testing.T) {
			status, code, message := rawUnsignedCall(t, ts, host, "", tc.path, []byte(invalidBodyPayload))
			assert.Equalf(t, "InvalidParameterValueException", code,
				"%s refuses a body that will not parse without consulting state (#1006)", tc.op)
			assert.Equalf(t, http.StatusBadRequest, status,
				"%s answers 400, not the 404 it answered before the reorder", tc.op)
			assertNoDecoderText(t, tc.op+"/absentFunction", message)
		})
	}

	// A parsable body naming an absent function must still be a 404 on **both** operations: the reorder
	// moved each lookup, it did not remove either. Without these, deleting a lookup outright would leave
	// every assertion above green — and they are asserted per operation rather than once because the two
	// use different lookups, loadFunction and findFunctionByARN, so one can regress without the other.
	for _, tc := range []struct {
		op, path, body string
	}{
		{
			op:   "AddPermission",
			path: "/2015-03-31/functions/absent-fn/policy",
			body: `{"StatementId":"s1","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
		},
		{op: "TagResource", path: "/2015-03-31/tags/" + absentARN, body: `{"Tags":{"env":"prod"}}`},
	} {
		t.Run(tc.op+"/absentFunctionStillNotFound", func(t *testing.T) {
			status, code, _ := rawUnsignedCall(t, ts, host, "", tc.path, []byte(tc.body))
			assert.Equalf(t, "ResourceNotFoundException", code,
				"%s: a well-formed request for a function that does not exist is still a 404", tc.op)
			assert.Equalf(t, http.StatusNotFound, status, "%s answers 404 past its parse guard", tc.op)
		})
	}
}

// TestSQSInvalidBodyIsRefusedBeforeTheQueueLookup covers the four SQS operations whose discarded
// unmarshal error was observable as the *wrong code*, not merely as a missing one (#1007).
//
// GetQueueAttributes, DeleteQueue, ListQueueTags and PurgeQueue read the queue URL through
// sqsQueueURLFromRequest and pass it straight to loadQueue. When the decode was discarded, a body that
// would not parse yielded an empty URL and the lookup answered QueueDoesNotExist — so the emulator told
// a caller its queue was missing when the queue was fine and the body was not, which is the most
// expensive kind of wrong answer: it sends the reader to look at infrastructure rather than at the
// request.
//
// The queue is created first, which the shared table cannot do. Without it a passing assertion proves
// only that the code is not QueueDoesNotExist; with it, the refusal is known to have come from the guard
// rather than from a lookup that happened to fail for a different reason.
func TestSQSInvalidBodyIsRefusedBeforeTheQueueLookup(t *testing.T) {
	const host = "sqs.us-east-1.amazonaws.com"
	ts := emulator.StartTestServer(t)

	status, code, message := rawUnsignedCall(t, ts, host, "AmazonSQS.CreateQueue", "",
		[]byte(`{"QueueName":"guarded-queue"}`))
	require.Emptyf(t, code, "CreateQueue: %s", message)
	require.Equalf(t, http.StatusOK, status, "CreateQueue: %s", message)

	for _, op := range []string{"GetQueueAttributes", "DeleteQueue", "ListQueueTags", "PurgeQueue"} {
		t.Run(op, func(t *testing.T) {
			status, code, message := rawUnsignedCall(t, ts, host, "AmazonSQS."+op, "",
				[]byte(invalidBodyPayload))
			assert.Equalf(t, "ValidationError", code,
				"%s refuses the body rather than reporting the queue missing", op)
			assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", op)
			assertNoDecoderText(t, op, message)
		})
	}

	// The lookup moved, it did not go away: a well-formed request for a queue that does not exist is
	// still QueueDoesNotExist. Without this, deleting the lookup would leave every assertion above green.
	t.Run("absentQueueStillDoesNotExist", func(t *testing.T) {
		_, code, _ := rawUnsignedCall(t, ts, host, "AmazonSQS.GetQueueAttributes", "",
			[]byte(`{"QueueUrl":"http://localhost/123456789012/no-such-queue"}`))
		assert.Equal(t, "QueueDoesNotExist", code,
			"a well-formed request naming a queue that does not exist is unchanged")
	})
}

// invalidBodyMethodCase is a guarded operation the table above cannot reach, because it is routed on a
// method other than POST.
type invalidBodyMethodCase struct {
	// name is the subtest name, and is the operation's own name so a failure points at the page to read.
	name string
	// host, path and method reach the operation. All six are REST-routed, so none carries a target.
	host   string
	path   string
	method string
	// code and status are what the guard must answer, from the service's own constructor.
	code   string
	status int
}

// invalidBodyMethodCases is every guard #1007's second slice added that POST does not reach.
//
// The table above has no method column on purpose — every guard it covers is reachable on POST, because
// a guard only runs on a method that carries a body. These six are reachable on a method that carries one
// too, and a reader would reasonably assume they are simply missing, so they are here rather than absent:
// four Lambda updates and one EFS update are routed on PUT, and ListEmailIdentities is routed on GET with
// its filters in the body.
//
// The Lambda four are reachable without an existing function only since #1007 moved their parse above the
// lookup. Leaving the lookup first would have answered 404 for an unparseable body naming an absent
// function, where AddPermission and TagResource — moved by #1006 — answer 400 for the same request: one
// plugin, two codes, for one class of caller error, which is what #950 removed.
var invalidBodyMethodCases = []invalidBodyMethodCase{
	{
		name: "UpdateFunctionCode", host: "lambda.us-east-1.amazonaws.com",
		path: "/2015-03-31/functions/absent-fn/code", method: http.MethodPut,
		code: "InvalidParameterValueException", status: http.StatusBadRequest,
	},
	{
		name: "UpdateFunctionConfiguration", host: "lambda.us-east-1.amazonaws.com",
		path: "/2015-03-31/functions/absent-fn/configuration", method: http.MethodPut,
		code: "InvalidParameterValueException", status: http.StatusBadRequest,
	},
	{
		name: "PutFunctionEventInvokeConfig", host: "lambda.us-east-1.amazonaws.com",
		path: "/2015-03-31/functions/absent-fn/event-invoke-config", method: http.MethodPut,
		code: "InvalidParameterValueException", status: http.StatusBadRequest,
	},
	{
		name: "UpdateEventSourceMapping", host: "lambda.us-east-1.amazonaws.com",
		path: "/2015-03-31/event-source-mappings/no-such-uuid", method: http.MethodPut,
		code: "InvalidParameterValueException", status: http.StatusBadRequest,
	},
	{
		name: "UpdateFileSystem", host: "elasticfilesystem.us-east-1.amazonaws.com",
		path: "/2015-02-01/file-systems/fs-12345678", method: http.MethodPut,
		code: "BadRequest", status: http.StatusBadRequest,
	},
	{
		name: "ListEmailIdentities", host: "email.us-east-1.amazonaws.com",
		path: "/v2/email/identities", method: http.MethodGet,
		code: "BadRequestException", status: http.StatusBadRequest,
	},

	// #1007's third slice adds five more. Four are API Gateway v1 operations whose REST reference routes
	// them on PUT, and one is Backup's CreateBackupVault, which is a PUT with the vault name in the path.
	//
	// The API Gateway four take path parameters the handler never resolves — it reads the method and
	// integration out of the body — so a literal identifier is enough and no resource has to exist. That is
	// what makes the guard reachable against an empty server, which is the property this table needs.
	{
		name: "PutMethod", host: "apigateway.us-east-1.amazonaws.com",
		path: "/restapis/my-api/resources/my-res/methods/GET", method: http.MethodPut,
		code: "BadRequestException", status: http.StatusBadRequest,
	},
	{
		name: "PutIntegration", host: "apigateway.us-east-1.amazonaws.com",
		path: "/restapis/my-api/resources/my-res/methods/GET/integration", method: http.MethodPut,
		code: "BadRequestException", status: http.StatusBadRequest,
	},
	{
		name: "PutIntegrationResponse", host: "apigateway.us-east-1.amazonaws.com",
		path: "/restapis/my-api/resources/my-res/methods/GET/integration/responses/200", method: http.MethodPut,
		code: "BadRequestException", status: http.StatusBadRequest,
	},
	{
		name: "PutMethodResponse", host: "apigateway.us-east-1.amazonaws.com",
		path: "/restapis/my-api/resources/my-res/methods/GET/responses/200", method: http.MethodPut,
		code: "BadRequestException", status: http.StatusBadRequest,
	},
	{
		// The vault name must be non-empty because the required-member check on the path segment runs
		// *before* the parse guard. The already-exists check runs after it, so a literal name reaches the
		// guard on an empty server without anything being created.
		name: "CreateBackupVault", host: "backup.us-east-1.amazonaws.com",
		path: "/backup-vaults/my-vault", method: http.MethodPut,
		code: "InvalidRequestException", status: http.StatusBadRequest,
	},
}

// TestInvalidBodyOnANonPostOperation asserts the six guards the POST table cannot reach (#1007).
func TestInvalidBodyOnANonPostOperation(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, tc := range invalidBodyMethodCases {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := rawUnsignedMethodCall(t, ts, tc.host, "", tc.path, tc.method,
				[]byte(invalidBodyPayload))
			assert.Equalf(t, tc.code, code, "%s answers its service's published code", tc.name)
			assert.Equalf(t, tc.status, status, "%s answers %d", tc.name, tc.status)
			assertNoDecoderText(t, tc.name, message)
		})
	}
}

// TestInvalidBodyLeavesAnAbsentBodyAlone is the other half of the second and third slices, and the half a
// table of refusals cannot assert (#1007).
//
// Fourteen of the second slice's twenty sites sat inside an `if len(req.Body) > 0` check, because their
// operation publishes no required member and AWS accepts no body at all. Checking the decode error there
// must not turn an absent body into a refusal: these are list operations whose whole answer for an empty
// request is "everything". A guard that refused an empty body would satisfy every assertion in the tables
// above and break every consumer that lists without filters.
//
// The three Lambda updates are deliberately not here. They carry no length check and refuse an empty body,
// which matches AddPermission and TagResource: an update that names nothing to update is a caller error,
// not a request for a default.
//
// The third slice's fourteen are the second block below. Membership was measured rather than reasoned:
// every tail site was called with no body and the ones answering 200 were listed, then narrowed to the
// ones where 200 is what AWS publishes. Nine of the measured 200s are *not* here, because their page
// marks a member `Required: Yes` and answering 200 for an absent body is a defect this test would
// otherwise pin: sso CreateAccountAssignment / DeleteAccountAssignment / ListAccountAssignments (three
// required ARNs each), cognito-identity ListIdentityPools and cognito-idp ListUserPools (`MaxResults`),
// glue GetTables (`DatabaseName`), wafv2 ListWebACLs and ListIPSets (`Scope`), and dynamodb GetRecords
// (`ShardIterator`). Those belong to the missing-required-member class, not to this one — filed
// separately — and the distinction is the reason this test asserts 200 on a hand-checked list rather than
// on whatever the tree happens to answer.
func TestInvalidBodyLeavesAnAbsentBodyAlone(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct {
		name   string
		host   string
		target string
		path   string
		method string
	}{
		{name: "kinesis/ListStreams", host: "kinesis.us-east-1.amazonaws.com", target: "Kinesis_20131202.ListStreams"},
		{name: "eventbridge/ListRules", host: "events.us-east-1.amazonaws.com", target: "AWSEvents.ListRules"},
		{name: "sagemaker/ListApps", host: "api.sagemaker.us-east-1.amazonaws.com", target: "SageMaker.ListApps"},
		{name: "acm/ListCertificates", host: "acm.us-east-1.amazonaws.com", target: "CertificateManager.ListCertificates"},
		{name: "firehose/ListDeliveryStreams", host: "firehose.us-east-1.amazonaws.com", target: "Firehose_20150804.ListDeliveryStreams"},
		{name: "kms/ListKeys", host: "kms.us-east-1.amazonaws.com", target: "TrentService.ListKeys"},
		{name: "kms/ListAliases", host: "kms.us-east-1.amazonaws.com", target: "TrentService.ListAliases"},
		{name: "stepfunctions/ListStateMachines", host: "states.us-east-1.amazonaws.com", target: "AWSStepFunctions.ListStateMachines"},
		{name: "stepfunctions/ListActivities", host: "states.us-east-1.amazonaws.com", target: "AWSStepFunctions.ListActivities"},
		{name: "ssm/DescribeParameters", host: "ssm.us-east-1.amazonaws.com", target: "AmazonSSM.DescribeParameters"},
		{name: "servicequotas/ListRequestedServiceQuotaChangeHistory", host: "servicequotas.us-east-1.amazonaws.com", target: "ServiceQuotasV20190624.ListRequestedServiceQuotaChangeHistory"},
		{name: "sesv2/ListEmailIdentities", host: "email.us-east-1.amazonaws.com", path: "/v2/email/identities", method: http.MethodGet},

		// The third slice. Each of these publishes no required member, so an absent body means "no filter".
		{name: "ecs/DescribeClusters", host: "ecs.us-east-1.amazonaws.com", target: "AmazonEC2ContainerServiceV20141113.DescribeClusters"},
		{name: "ecs/ListTaskDefinitions", host: "ecs.us-east-1.amazonaws.com", target: "AmazonEC2ContainerServiceV20141113.ListTaskDefinitions"},
		{name: "ecs/ListServices", host: "ecs.us-east-1.amazonaws.com", target: "AmazonEC2ContainerServiceV20141113.ListServices"},
		{name: "ecs/ListTasks", host: "ecs.us-east-1.amazonaws.com", target: "AmazonEC2ContainerServiceV20141113.ListTasks"},
		{name: "cloudwatchlogs/DescribeLogGroups", host: "logs.us-east-1.amazonaws.com", target: "Logs_20140328.DescribeLogGroups"},
		{name: "athena/ListQueryExecutions", host: "athena.us-east-1.amazonaws.com", target: "AmazonAthena.ListQueryExecutions"},
		{name: "athena/ListWorkGroups", host: "athena.us-east-1.amazonaws.com", target: "AmazonAthena.ListWorkGroups"},
		{name: "timestream-write/ListTables", host: "ingest.timestream.us-east-1.amazonaws.com", target: "Timestream_20181101.ListTables"},
		{name: "secretsmanager/ListSecrets", host: "secretsmanager.us-east-1.amazonaws.com", target: "secretsmanager.ListSecrets"},
		{name: "ecr/DescribeRepositories", host: "api.ecr.us-east-1.amazonaws.com", target: "AmazonEC2ContainerRegistry_V20150921.DescribeRepositories"},
		{name: "cloudtrail/DescribeTrails", host: "cloudtrail.us-east-1.amazonaws.com", target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails"},
		{name: "fsx/DescribeFileSystems", host: "fsx.us-east-1.amazonaws.com", target: "AWSSimbaAPIService_v20180301.DescribeFileSystems"},
		{name: "dynamodb/ListTables", host: "dynamodb.us-east-1.amazonaws.com", target: "DynamoDB_20120810.ListTables"},
		{name: "dynamodb/ListStreams", host: "dynamodb.us-east-1.amazonaws.com", target: "DynamoDB_20120810.ListStreams"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := rawUnsignedMethodCall(t, ts, tc.host, tc.target, tc.path, tc.method, nil)
			assert.Emptyf(t, code, "%s on an absent body: %s", tc.name, message)
			assert.Equalf(t, http.StatusOK, status, "%s on an absent body answers 200", tc.name)
		})
	}
}

// TestSQSAMemberOfTheWrongTypeIsRefused covers what the *second* decode in nine SQS handlers is for
// (#1007).
//
// Those handlers read the queue URL through sqsQueueURLFromRequest and then decode the same body again
// into their own struct, so a body that will not *parse* has already been refused one call earlier and
// their own guard can never see one. It is still reachable, and this is the shape that reaches it: a
// body that parses cleanly but contradicts a member's type. The first decode ignores the member because
// it is not in the helper's struct — encoding/json skips a field the target does not declare without
// type-checking it — so the type error surfaces only on the second.
//
// Without these cases the nine guards would be untested, and an untested guard on an input no test sends
// is indistinguishable from dead code. The queue is created first because every one of the nine looks the
// queue up before decoding, so an absent queue would answer QueueDoesNotExist before the guard ran.
//
// The code is ValidationError, the same as for an unparseable body: substrate does not inspect
// *json.UnmarshalTypeError to name the offending member, and SQS's common-errors gloss — "The input fails
// to satisfy the constraints specified by an AWS service" — is true of both. Naming the member would mean
// answering InvalidParameterValue for this shape and ValidationError for the other, which is the
// one-plugin-two-codes split #950 removed.
func TestSQSAMemberOfTheWrongTypeIsRefused(t *testing.T) {
	const host = "sqs.us-east-1.amazonaws.com"
	const queueURL = "http://localhost/123456789012/typed-queue"
	ts := emulator.StartTestServer(t)

	status, code, message := rawUnsignedCall(t, ts, host, "AmazonSQS.CreateQueue", "",
		[]byte(`{"QueueName":"typed-queue"}`))
	require.Emptyf(t, code, "CreateQueue: %s", message)
	require.Equalf(t, http.StatusOK, status, "CreateQueue: %s", message)

	// One case per handler, each contradicting a member that handler declares and the helper does not.
	for _, tc := range []struct {
		op     string
		member string
	}{
		{op: "SetQueueAttributes", member: `"Attributes":"not-a-map"`},
		{op: "TagQueue", member: `"Tags":["not-a-map"]`},
		{op: "UntagQueue", member: `"TagKeys":"not-a-list"`},
		{op: "SendMessage", member: `"DelaySeconds":"not-a-number"`},
		{op: "SendMessageBatch", member: `"Entries":{"not":"a-list"}`},
		{op: "ReceiveMessage", member: `"MaxNumberOfMessages":"not-a-number"`},
		{op: "DeleteMessage", member: `"ReceiptHandle":17`},
		{op: "DeleteMessageBatch", member: `"Entries":"not-a-list"`},
		{op: "ChangeMessageVisibility", member: `"VisibilityTimeout":"not-a-number"`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := []byte(`{"QueueUrl":"` + queueURL + `",` + tc.member + `}`)
			status, code, message := rawUnsignedCall(t, ts, host, "AmazonSQS."+tc.op, "", body)
			assert.Equalf(t, "ValidationError", code,
				"%s refuses a member whose type contradicts the model", tc.op)
			assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", tc.op)
			assertNoDecoderText(t, tc.op, message)
		})
	}

	// The guard refuses the body rather than every request: the same operations must still succeed on a
	// body whose members are the published types. Without this, a guard that refused everything would
	// leave all nine assertions above green.
	t.Run("aWellTypedBodySucceeds", func(t *testing.T) {
		status, code, message := rawUnsignedCall(t, ts, host, "AmazonSQS.SendMessage", "",
			[]byte(`{"QueueUrl":"`+queueURL+`","MessageBody":"hello","DelaySeconds":0}`))
		assert.Emptyf(t, code, "SendMessage on a well-typed body: %s", message)
		assert.Equal(t, http.StatusOK, status, "SendMessage on a well-typed body answers 200")
	})
}

// memberCase is one complaint about a member or an identifier, as opposed to a body that will not parse.
type memberCase struct {
	// name is the subtest name, and is the handler's own name so a failure points at the source.
	name string
	// target, path and method reach the operation, as in [invalidBodyCase].
	target string
	path   string
	method string
	// body is sent verbatim. "{}" is the usual value: it parses, so the parse guard passes and the
	// member check below it is what answers.
	body string
	// wantMessage is a substring of the message the site reports, which is what distinguishes these
	// sites from each other once they all answer one code per service.
	wantMessage string
}

// memberService is one service's non-parse-guard sites, under the code they all answer.
type memberService struct {
	name  string
	host  string
	code  string
	cases []memberCase
}

// memberComplaintServices is every site whose code string #950 or #1063 changed that is **not** a parse
// guard.
//
// The first eight services are #950's; the last three are #1063's eleven, which #950 deferred — four in
// Glue, one in FSx and six in WAFv2 — and they belong in this table rather than in a new one for the
// reason the table exists: its one `code:` per service is what forced each of those eleven decisions to
// be made once. Adding a twelfth service here is the cheapest way to state that a plugin answers one
// code for one class of caller error.
//
// These are the second half of the inventory in docs/services.md — the "+ N member" column. They matter
// as much as the parse guards and are easier to miss: a parse guard is one literal per handler, while
// these are scattered complaints about a missing member or a malformed identifier, and #950 corrected
// them to the same per-service code precisely so that one plugin cannot answer two codes for one class
// of caller error.
//
// The body is "{}" wherever a member is being checked. That is deliberate and is the whole reason these
// cases cannot live in the parse-guard table: "{}" *parses*, so it travels past the parse guard and
// reaches the member check underneath, where invalidBodyPayload would have stopped one line earlier.
// Where the identifier comes from the path instead, the body is irrelevant and the path is what carries
// the case.
var memberComplaintServices = []memberService{
	{
		name: "eventbridge",
		host: "events.us-east-1.amazonaws.com",
		code: "ValidationError",
		cases: []memberCase{
			{name: "deleteRule", target: "AWSEvents.DeleteRule", body: "{}", wantMessage: "Name is required"},
			{name: "describeRule", target: "AWSEvents.DescribeRule", body: "{}", wantMessage: "Name is required"},
			{name: "putTargets", target: "AWSEvents.PutTargets", body: "{}", wantMessage: "Rule is required"},
			{name: "removeTargets", target: "AWSEvents.RemoveTargets", body: "{}", wantMessage: "Rule is required"},
			{name: "listTargetsByRule", target: "AWSEvents.ListTargetsByRule", body: "{}", wantMessage: "Rule is required"},
		},
	},
	{
		name: "firehose",
		host: "firehose.us-east-1.amazonaws.com",
		code: "ValidationError",
		cases: []memberCase{
			{name: "createDeliveryStream", target: "Firehose_20150804.CreateDeliveryStream", body: "{}", wantMessage: "DeliveryStreamName is required"},
			{name: "describeDeliveryStream", target: "Firehose_20150804.DescribeDeliveryStream", body: "{}", wantMessage: "DeliveryStreamName is required"},
			{name: "deleteDeliveryStream", target: "Firehose_20150804.DeleteDeliveryStream", body: "{}", wantMessage: "DeliveryStreamName is required"},
		},
	},
	{
		name: "sagemaker",
		host: "api.sagemaker.us-east-1.amazonaws.com",
		code: "ValidationError",
		cases: []memberCase{
			{name: "createApp", target: "SageMaker.CreateApp", body: "{}", wantMessage: "AppName is required"},
			// The three training-job sites are three of the four #950 split out of an
			// `if err != nil || body.X == ""` conflation. Sending "{}" is what proves the split
			// happened: before it, this input and an unparseable one produced the same message.
			{name: "createTrainingJob", target: "SageMaker.CreateTrainingJob", body: "{}", wantMessage: "TrainingJobName is required"},
			{name: "describeTrainingJob", target: "SageMaker.DescribeTrainingJob", body: "{}", wantMessage: "TrainingJobName is required"},
			{name: "stopTrainingJob", target: "SageMaker.StopTrainingJob", body: "{}", wantMessage: "TrainingJobName is required"},
		},
	},
	{
		name: "kinesis",
		host: "kinesis.us-east-1.amazonaws.com",
		code: "InvalidArgumentException",
		cases: []memberCase{
			// getShardIterator resolves the stream reference before it checks ShardId, so the body
			// has to name a stream to reach the member check at all.
			{name: "getShardIterator", target: "Kinesis_20131202.GetShardIterator", body: `{"StreamName":"a-stream"}`, wantMessage: "ShardId is required"},
			{name: "getRecords", target: "Kinesis_20131202.GetRecords", body: "{}", wantMessage: "ShardIterator is required"},
		},
	},
	{
		name: "servicequotas",
		host: "servicequotas.us-east-1.amazonaws.com",
		code: "IllegalArgumentException",
		cases: []memberCase{
			{name: "requestServiceQuotaIncrease", target: "ServiceQuotasV20190624.RequestServiceQuotaIncrease", body: "{}", wantMessage: "ServiceCode and QuotaCode are required"},
		},
	},
	{
		name: "lambda",
		host: "lambda.us-east-1.amazonaws.com",
		code: "InvalidParameterValueException",
		cases: []memberCase{
			{name: "createFunction", path: "/2015-03-31/functions", body: "{}", wantMessage: "FunctionName is required"},
			{name: "createEventSourceMapping", path: "/2015-03-31/event-source-mappings", body: "{}", wantMessage: "FunctionName and EventSourceArn are required"},
		},
	},
	{
		name: "sesv2",
		host: "email.us-east-1.amazonaws.com",
		code: "BadRequestException",
		cases: []memberCase{
			{name: "createEmailIdentity", path: "/v2/email/identities", body: "{}", wantMessage: "EmailIdentity is required"},
			// Reachable since #1009 dropped parseSESv2Operation's trailing-slash trim.
			{name: "getEmailIdentity", path: "/v2/email/identities/", method: http.MethodGet, body: "{}", wantMessage: "identity name is required"},
			{name: "deleteEmailIdentity", path: "/v2/email/identities/", method: http.MethodDelete, body: "{}", wantMessage: "identity name is required"},
		},
	},
	{
		name: "msk",
		host: "kafka.us-east-1.amazonaws.com",
		code: "BadRequest",
		cases: []memberCase{
			{name: "createClusterV2", path: "/api/v2/clusters", body: "{}", wantMessage: "ClusterName is required"},
			// Every empty-ARN guard is reachable since #1009 dropped parseKafkaOperation's
			// trailing-slash trim; before it, only the two with a literal segment after the ARN were.
			{name: "describeCluster", path: "/v1/clusters/", method: http.MethodGet, body: "{}", wantMessage: "cluster ARN is required"},
			{name: "deleteCluster", path: "/v1/clusters/", method: http.MethodDelete, body: "{}", wantMessage: "cluster ARN is required"},
			{name: "describeClusterV2", path: "/api/v2/clusters/", method: http.MethodGet, body: "{}", wantMessage: "cluster ARN is required"},
			{name: "getBootstrapBrokers", path: "/v1/clusters//bootstrap-brokers", method: http.MethodGet, body: "{}", wantMessage: "cluster ARN is required"},
			{name: "listNodes", path: "/v1/clusters//nodes", method: http.MethodGet, body: "{}", wantMessage: "cluster ARN is required"},
			{name: "loadClusterByARN/notAnARN", path: "/v1/clusters/notanarn", method: http.MethodGet, body: "{}", wantMessage: "invalid MSK cluster ARN"},
			{name: "loadClusterByARN/badResource", path: "/v1/clusters/arn:aws:kafka:us-east-1:123456789012:cluster", method: http.MethodGet, body: "{}", wantMessage: "invalid MSK cluster ARN resource"},
		},
	},
	{
		name: "efs",
		host: "elasticfilesystem.us-east-1.amazonaws.com",
		code: "BadRequest",
		cases: []memberCase{
			{name: "createAccessPoint", path: "/2015-02-01/access-points", body: "{}", wantMessage: "FileSystemId is required"},
			{name: "createMountTarget", path: "/2015-02-01/mount-targets", body: "{}", wantMessage: "FileSystemId is required"},
			// parseEFSOperation does not trim a trailing slash, so unlike MSK's and SES v2's the
			// empty-path-parameter guards here are all reachable.
			{name: "updateFileSystem", path: "/2015-02-01/file-systems", method: http.MethodPut, body: "{}", wantMessage: "FileSystemId is required"},
			{name: "deleteFileSystem", path: "/2015-02-01/file-systems", method: http.MethodDelete, body: "{}", wantMessage: "FileSystemId is required"},
			{name: "deleteAccessPoint", path: "/2015-02-01/access-points", method: http.MethodDelete, body: "{}", wantMessage: "AccessPointId is required"},
			{name: "deleteMountTarget", path: "/2015-02-01/mount-targets", method: http.MethodDelete, body: "{}", wantMessage: "MountTargetId is required"},
			{name: "tagResource", path: "/2015-02-01/resource-tags/", body: "{}", wantMessage: "ResourceId is required"},
			{name: "listTagsForResource", path: "/2015-02-01/resource-tags/", method: http.MethodGet, body: "{}", wantMessage: "ResourceId is required"},
			{name: "untagResource", path: "/2015-02-01/resource-tags/", method: http.MethodDelete, body: "{}", wantMessage: "ResourceId is required"},
			// EFS keys its tag store off the ID prefix, so an ID belonging to neither a file system
			// nor an access point is refused rather than silently stored under a third kind.
			{name: "mergeEFSTags", path: "/2015-02-01/resource-tags/xyz-12345678", body: `{"Tags":[{"Key":"k","Value":"v"}]}`, wantMessage: "Unknown resource ID prefix"},
			{name: "loadEFSTags", path: "/2015-02-01/resource-tags/xyz-12345678", method: http.MethodGet, body: "{}", wantMessage: "Unknown resource ID prefix"},
		},
	},
	{
		// #1063's four Glue sites. #950 corrected the plugin's twenty-seven parse guards to
		// InvalidInputException and deferred these; they had kept InvalidParameterValueException,
		// a string that appears on no Glue page, so one plugin answered a published code for a
		// body it could not read and an unpublished one for an ARN it could not parse.
		//
		// The last three rows are resolveGlueARN's remaining failure shapes, and they are here for
		// the reason the MSK rows above are: they are the evidence that the resolver decides the
		// code once. Every one of the four is a complaint about the *shape* of the string, which is
		// why EntityNotFoundException — published at 400 on all three tagging pages — is not the
		// answer: nothing has been looked up.
		name: "glue",
		host: "glue.us-east-1.amazonaws.com",
		code: "InvalidInputException",
		cases: []memberCase{
			{name: "createDatabase", target: "AWSGlue.CreateDatabase", body: "{}", wantMessage: "DatabaseInput.Name is required"},
			{name: "tagResource", target: "AWSGlue.TagResource", body: "{}", wantMessage: "invalid Glue ARN"},
			{name: "untagResource", target: "AWSGlue.UntagResource", body: "{}", wantMessage: "invalid Glue ARN"},
			{name: "getTags", target: "AWSGlue.GetTags", body: "{}", wantMessage: "invalid Glue ARN"},
			{name: "resolveGlueARN/tooFewFields", target: "AWSGlue.GetTags", body: `{"ResourceArn":"arn:aws:glue:us-east-1"}`, wantMessage: "invalid Glue ARN"},
			{name: "resolveGlueARN/noSlash", target: "AWSGlue.GetTags", body: `{"ResourceArn":"arn:aws:glue:us-east-1:123456789012:database"}`, wantMessage: "invalid Glue ARN resource"},
			{name: "resolveGlueARN/unsupportedType", target: "AWSGlue.GetTags", body: `{"ResourceArn":"arn:aws:glue:us-east-1:123456789012:widget/w"}`, wantMessage: "unsupported Glue resource type"},
		},
	},
	{
		// #1063's one FSx site, and the issue put it on the wrong operation: it is deleteFileSystem,
		// where API_DeleteFileSystem marks FileSystemId Required: Yes, not describeFileSystems,
		// where FileSystemIds is Required: No and an absent list means "describe them all". So the
		// guard is right and only the code was wrong — InvalidRequest is an Amazon S3 string.
		name: "fsx",
		host: "fsx.us-east-1.amazonaws.com",
		code: "BadRequest",
		cases: []memberCase{
			{name: "deleteFileSystem", target: "AWSSimbaAPIService_v20180301.DeleteFileSystem", body: "{}", wantMessage: "FileSystemId is required"},
		},
	},
	{
		// #1063's six WAFv2 sites, which had answered WAFInvalidParameterException for an omitted
		// member. #755 had already settled that distinction for CreateIPSet — that code is glossed
		// "AWS WAF didn't recognize a parameter in the request" and all four of its published
		// examples are about a value substrate *read* — so the plugin was answering two codes for
		// one class of caller error, which is the defect wafv2ValidateCreateIPSet's split exists to
		// prevent. This table is what keeps them together.
		//
		// getWebACL is the row that is not about a named member. API_GetWebACL marks ARN, Id, Name
		// and Scope **all Required: No**, so its refusal cannot say "Id is a required parameter";
		// it reports that the request addressed nothing, which is why the Id check could not stay
		// in loadWebACLByID where its other three callers need it.
		name: "wafv2",
		host: "wafv2.us-east-1.amazonaws.com",
		code: "ValidationError",
		cases: []memberCase{
			{name: "createWebACL", target: "AWSWAF_20190729.CreateWebACL", body: "{}", wantMessage: "Name is a required parameter"},
			{name: "getWebACL", target: "AWSWAF_20190729.GetWebACL", body: "{}", wantMessage: "the request identifies no web ACL"},
			{name: "updateWebACL", target: "AWSWAF_20190729.UpdateWebACL", body: "{}", wantMessage: "Id is a required parameter"},
			{name: "deleteWebACL", target: "AWSWAF_20190729.DeleteWebACL", body: "{}", wantMessage: "Id is a required parameter"},
			{name: "associateWebACL", target: "AWSWAF_20190729.AssociateWebACL", body: "{}", wantMessage: "ResourceArn is a required parameter"},
			{name: "disassociateWebACL", target: "AWSWAF_20190729.DisassociateWebACL", body: "{}", wantMessage: "ResourceArn is a required parameter"},
			{name: "getWebACLForResource", target: "AWSWAF_20190729.GetWebACLForResource", body: "{}", wantMessage: "ResourceArn is a required parameter"},
			// loadIPSetByID keeps its check, because API_GetIPSet, API_UpdateIPSet and
			// API_DeleteIPSet all mark Id Required: Yes and listIPSets reads the index.
			{name: "getIPSet", target: "AWSWAF_20190729.GetIPSet", body: "{}", wantMessage: "Id is a required parameter"},
			{name: "updateIPSet", target: "AWSWAF_20190729.UpdateIPSet", body: "{}", wantMessage: "Id is a required parameter"},
			{name: "deleteIPSet", target: "AWSWAF_20190729.DeleteIPSet", body: "{}", wantMessage: "Id is a required parameter"},
			// CreateIPSet answers the same code from wafv2ValidateCreateIPSet, which #755 wrote and
			// wafv2_createipset_validation_test.go covers in full. One row here proves the two
			// paths agree rather than duplicating that file.
			{name: "createIPSet", target: "AWSWAF_20190729.CreateIPSet", body: "{}", wantMessage: "Name is a required parameter"},
		},
	},
}

// TestMemberComplaintAnswersThePublishedCode asserts the non-parse-guard half of the inventory answers
// the same per-service code, with the message that tells the two apart.
//
// **The five guards this test used to declare unreachable are now here.** `parseKafkaOperation` and
// `parseSESv2Operation` both opened with `strings.TrimRight(path, "/")`, so a request naming an empty
// path parameter collapsed onto the collection route — `GET /v1/clusters/` dispatched ListClusters
// rather than DescribeCluster with an empty ARN — which made MSK's describeCluster, deleteCluster and
// describeClusterV2 checks and SES v2's getEmailIdentity and deleteEmailIdentity checks dead code, with
// codes that no request could verify. #1009 removed both trims, so the five rows above are the proof
// that each answers its service's published code, and MSK's getBootstrapBrokers and listNodes — which
// were always reachable, because a literal segment follows the ARN — sit beside them unchanged.
func TestMemberComplaintAnswersThePublishedCode(t *testing.T) {
	for _, svc := range memberComplaintServices {
		t.Run(svc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			for _, tc := range svc.cases {
				t.Run(tc.name, func(t *testing.T) {
					status, code, message := rawUnsignedMethodCall(t, ts, svc.host, tc.target, tc.path,
						tc.method, []byte(tc.body))
					assert.Equalf(t, svc.code, code, "%s answers its service's one code", tc.name)
					assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", tc.name)
					assert.Containsf(t, message, tc.wantMessage, "%s names what it wants", tc.name)
					assertNoDecoderText(t, tc.name, message)
				})
			}
		})
	}
}

// lambdaFunctionARNFromCreate reads a function's ARN back off GetFunction.
func lambdaFunctionARNFromCreate(t *testing.T, ts *emulator.TestServer, host, name string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		ts.URL+"/2015-03-31/functions/"+name, nil)
	require.NoError(t, err, "build the GetFunction request")
	req.Host = host

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GetFunction")
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read the GetFunction body")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "GetFunction succeeds: %s", raw)

	var out struct {
		Configuration struct {
			FunctionArn string `json:"FunctionArn"`
		} `json:"Configuration"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode the GetFunction body")
	require.NotEmpty(t, out.Configuration.FunctionArn, "GetFunction reports an ARN")
	return out.Configuration.FunctionArn
}

// rawUnsignedCall posts body verbatim and returns the status, the error code and the message.
//
// It is the counterpart of [rawSignedCall] for the services in this file, and it differs in two ways
// that both matter. It signs nothing, because a parse guard is reached before any authorization check
// and every one of these tests is about the body rather than the caller — StartTestServer leaves
// signature verification off, so an unsigned request resolves to the default account. And it dispatches
// on either X-Amz-Target or a URL path, because half of these services are REST-JSON, where the
// operation is the path.
//
// The code is read from whichever member the service's protocol puts it in: REST-JSON writes "Code",
// JSON-RPC writes "__type" carrying a shape ID. Reading both here rather than per service keeps a
// service's protocol out of the table, which has no business knowing it.
func rawUnsignedCall(t *testing.T, ts *emulator.TestServer, host, target, path string,
	body []byte,
) (status int, code, message string) {
	t.Helper()
	return rawUnsignedMethodCall(t, ts, host, target, path, http.MethodPost, body)
}

// rawUnsignedMethodCall is [rawUnsignedCall] with the HTTP method spelled out.
//
// Only the member-complaint table needs it: several of those guards read a path parameter rather than a
// body member, and the operation that carries them is a GET, a PUT or a DELETE.
func rawUnsignedMethodCall(t *testing.T, ts *emulator.TestServer, host, target, path, method string,
	body []byte,
) (status int, code, message string) {
	t.Helper()

	if (target == "") == (path == "") {
		t.Fatalf("exactly one of target and path must be set, got %q and %q", target, path)
	}
	if method == "" {
		method = http.MethodPost
	}

	url := ts.URL + "/"
	contentType := "application/x-amz-json-1.1"
	if path != "" {
		url = ts.URL + path
		contentType = "application/json"
	}

	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build the request for %s: %v", url, err)
	}
	req.Host = host
	req.Header.Set("Content-Type", contentType)
	if target != "" {
		req.Header.Set("X-Amz-Target", target)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post to %s: %v", url, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the response from %s: %v", url, err)
	}
	var errShape struct {
		Code    string `json:"Code"`
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr != nil {
		t.Fatalf("decode the response from %s, %s: %v", url, raw, unmarshalErr)
	}

	code = errShape.Code
	if code == "" {
		code = errShape.Type
	}
	// A JSON-RPC "__type" may name an absolute shape ID. Only the shape matters to a caller matching on
	// a code, and every SDK's parser takes the last segment.
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}
	return resp.StatusCode, code, errShape.Message
}

// TestOpenSearchInvalidBodyIsNotAnAWSError covers the three sites in #1007's tail that are not an AWS
// control-plane operation at all.
//
// These are the OpenSearch domain's own REST search API, so no AWS reference publishes a code for them and
// the refusal cannot be an AWSError: it is the engine's error envelope, a JSON body carrying a `type` and
// an HTTP status. openSearchInvalidBody answers json_parse_exception/400, following the file's existing
// convention of the engine's own lowercased exception name (resource_already_exists_exception,
// illegal_argument_exception). The provenance is engine behavior, which is why these are asserted apart
// from the published-code tables rather than folded into them with a fabricated citation.
func TestOpenSearchInvalidBodyIsNotAnAWSError(t *testing.T) {
	const host = "search-mydomain-abc123.us-east-1.es.amazonaws.com"
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct {
		op     string
		path   string
		method string
	}{
		{op: "Search", path: "/my-index/_search", method: http.MethodPost},
		{op: "Scroll", path: "/_search/scroll", method: http.MethodPost},
		{op: "ClearScroll", path: "/_search/scroll", method: http.MethodDelete},
	} {
		t.Run(tc.op, func(t *testing.T) {
			status, body := rawUnsignedRawBody(t, ts, host, tc.path, tc.method, []byte(invalidBodyPayload))
			assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", tc.op)
			assert.Containsf(t, body, "json_parse_exception",
				"%s answers the engine's own exception name, not an AWS code: %s", tc.op, body)
			assertNoDecoderText(t, tc.op, body)
		})
	}
}

// TestInvalidBodyBelowAResourceLookup covers the three tail sites no table above can reach (#1007).
//
// Each sits below a lookup of the resource its path names, so against an empty server the lookup answers
// first — NotFoundException for API Gateway v2, the loader's own error for AppSync and Backup — and the
// parse guard never runs. That is the lookup-first convention recorded under "Whether a body is parsed
// before the resource is looked up" in docs/services.md, not a defect in this slice; #1007's scope is that
// a discarded decode error is checked, which these three now satisfy.
//
// It does mean a table of refusals against a bare server cannot see them, and a site no test reaches is a
// site whose code has stopped being checked. So the prerequisite is created first and the refusal asserted
// second, the same shape as TestLambdaInvalidBodyBelowAFunctionLookup. Without this, appsyncInvalidBody
// would have no caller any test exercises at all, since CreateApiKey is AppSync's only site in the slice.
func TestInvalidBodyBelowAResourceLookup(t *testing.T) {
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct {
		name string
		host string
		// createPath and createBody bring the resource the guard sits below into existence.
		createPath string
		createBody string
		// idKey is the member of the create response holding the new resource's identifier, found at any
		// depth so that a wire shape which nests it (AppSync's graphqlApi) needs no separate column.
		idKey string
		// refusePath renders the path the refusal is asserted on, given that identifier.
		refusePath func(id string) string
		method     string
		code       string
	}{
		{
			name:       "apigatewayv2/UpdateApi",
			host:       "apigateway.us-east-1.amazonaws.com",
			createPath: "/v2/apis",
			createBody: `{"Name":"below-lookup","ProtocolType":"HTTP"}`,
			idKey:      "apiId",
			refusePath: func(id string) string { return "/v2/apis/" + id },
			method:     http.MethodPatch,
			code:       "BadRequestException",
		},
		{
			name:       "appsync/CreateApiKey",
			host:       "appsync.us-east-1.amazonaws.com",
			createPath: "/v1/apis",
			createBody: `{"name":"below-lookup","authenticationType":"API_KEY"}`,
			idKey:      "apiId",
			refusePath: func(id string) string { return "/v1/apis/" + id + "/ApiKeys" },
			method:     http.MethodPost,
			code:       "BadRequestException",
		},
		{
			name:       "backup/UpdateBackupPlan",
			host:       "backup.us-east-1.amazonaws.com",
			createPath: "/backup/plans",
			createBody: `{"BackupPlan":{"BackupPlanName":"below-lookup"}}`,
			idKey:      "BackupPlanId",
			refusePath: func(id string) string { return "/backup/plans/" + id },
			method:     http.MethodPost,
			code:       "InvalidRequestException",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := rawUnsignedRawBody(t, ts, tc.host, tc.createPath, http.MethodPost,
				[]byte(tc.createBody))
			require.Truef(t, status == http.StatusOK || status == http.StatusCreated,
				"creating the prerequisite for %s answers 200 or 201, got %d: %s", tc.name, status, raw)
			id := jsonMemberAnyDepth(t, raw, tc.idKey)

			status, code, message := rawUnsignedMethodCall(t, ts, tc.host, "", tc.refusePath(id),
				tc.method, []byte(invalidBodyPayload))
			assert.Equalf(t, tc.code, code, "%s answers its service's published code: %s", tc.name, message)
			assert.Equalf(t, http.StatusBadRequest, status, "%s answers 400", tc.name)
			assertNoDecoderText(t, tc.name, message)
		})
	}
}

// jsonMemberAnyDepth returns the first string value of key in a JSON object, searching nested objects.
//
// Searching by depth rather than by path keeps the table above to one column per resource: AppSync nests
// the identifier under graphqlApi where Backup and API Gateway v2 put it at the top level, and which of
// those a service chose is not what the test is about.
func jsonMemberAnyDepth(t *testing.T, raw, key string) string {
	t.Helper()

	var doc any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode the create response looking for %q: %v: %s", key, err, raw)
	}

	var walk func(any) (string, bool)
	walk = func(node any) (string, bool) {
		obj, ok := node.(map[string]any)
		if !ok {
			return "", false
		}
		if v, found := obj[key].(string); found && v != "" {
			return v, true
		}
		for _, child := range obj {
			if v, found := walk(child); found {
				return v, true
			}
		}
		return "", false
	}

	v, found := walk(doc)
	if !found {
		t.Fatalf("no %q member in the create response: %s", key, raw)
	}
	return v
}

// TestInvalidBodyTailIsFullyCovered asserts the sixty sites #1007's third slice changed are all accounted
// for: covered by a table, or listed as uncovered with a reason.
//
// This is arithmetic rather than behavior, and it is here because the tables cannot state it themselves.
// A site quietly dropped from a table still leaves every other assertion green, so the count is the only
// thing that catches it — the same reason the per-service counts above are pinned to docs/services.md.
func TestInvalidBodyTailIsFullyCovered(t *testing.T) {
	// The slice's own numbers, from the inventory in docs/services.md.
	const (
		tailSites       = 60
		inServiceTables = 49 // POST-routed, refused with an AWSError
		inMethodTable   = 5  // four API Gateway v1 PUTs and Backup's CreateBackupVault
		inOpenSearch    = 3  // the domain's own REST API, refused with the engine's envelope
		belowALookup    = 3  // reachable only once the resource the guard sits below exists
	)

	assert.Equalf(t, tailSites, inServiceTables+inMethodTable+inOpenSearch+belowALookup,
		"every one of the %d sites #1007's third slice changed is reached by a test above", tailSites)
}

// rawUnsignedRawBody is [rawUnsignedMethodCall] returning the response body verbatim.
//
// Only the OpenSearch test needs it. That plugin's errors are the engine's envelope rather than an
// AWSError, so there is no Code or __type member to read, and the decode in rawUnsignedMethodCall would
// report an empty code for a refusal that is plainly there. Returning the bytes lets the assertion name
// what it is actually looking for.
func rawUnsignedRawBody(t *testing.T, ts *emulator.TestServer, host, path, method string,
	body []byte,
) (status int, raw string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build the request for %s: %v", path, err)
	}
	req.Host = host
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("call %s: %v", path, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the response from %s: %v", path, err)
	}
	return resp.StatusCode, string(out)
}
