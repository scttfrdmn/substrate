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

// invalidBodyServices is every service and operation #950 corrected, less the two Step Functions and
// Systems Manager tables and the two Lambda operations that need a function to exist first.
//
// Sixty-four cases. The counts per service are the counts in docs/services.md's inventory table, and a
// case removed from here without a reason is a site that stops being checked.
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

// memberComplaintServices is every site whose code string #950 changed that is **not** a parse guard.
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
