package emulator_test

// Json-typed CloudFormation properties resolve their intrinsics at every depth (#1153).
//
// Five deployers marshaled a policy document or event pattern straight from the template, so an
// Fn::GetAtt inside one was stored as `{"Fn::GetAtt":[…]}` — valid JSON that nothing refused, so the
// stack reported CREATE_COMPLETE while the policy named a resource that is a literal intrinsic. These
// tests read each document back through the owning service's own read operation, which is where the
// divergence was observable.

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfnDocRoute sends one request through the registry and returns the success body.
func cfnDocRoute(t *testing.T, registry *emulator.PluginRegistry, req *emulator.AWSRequest) []byte {
	t.Helper()
	if req.Params == nil {
		req.Params = map[string]string{}
	}
	resp, err := registry.RouteRequest(
		&emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: req.Operation},
		req)
	require.NoErrorf(t, err, "%s", req.Operation)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: %s", req.Operation, resp.Body)
	return resp.Body
}

// requireNoIntrinsic fails when any key in a decoded JSON document names an intrinsic function.
func requireNoIntrinsic(t *testing.T, doc string) {
	t.Helper()
	assert.NotContains(t, doc, `"Fn::`, "an intrinsic was stored as data")
	assert.NotContains(t, doc, `"Ref"`, "an intrinsic was stored as data")
}

// TestCFN_IAMPolicyDocumentResolvesIntrinsics resolves the Resource through Fn::Sub over the
// pseudo-parameters rather than Fn::GetAtt.
//
// AWS::IAM::Policy carries typePriority 0, so it deploys before every other resource and an
// Fn::GetAtt from it finds nothing deployed yet; that ordering is its own defect, not this walk's.
// The Fn::GetAtt form is asserted on AWS::SNS::TopicPolicy below, which deploys after its topic.
func TestCFN_IAMPolicyDocumentResolvesIntrinsics(t *testing.T) {
	const tmpl = `{"Resources":{
		"ReadData":{"Type":"AWS::IAM::Policy","Properties":{
			"PolicyName":"read-the-data",
			"PolicyDocument":{"Version":"2012-10-17","Statement":[{
				"Effect":"Allow","Action":"sqs:ReceiveMessage",
				"Resource":{"Fn::Sub":"arn:${AWS::Partition}:sqs:${AWS::Region}:${AWS::AccountId}:cfn-doc-data"}}]}}}}}`
	deployer, registry := sfnDefinitionDeployer(t)
	result, err := deployer.Deploy(context.Background(), tmpl, "cfn-doc-policy", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	body := cfnDocRoute(t, registry, &emulator.AWSRequest{
		Service: "iam", Operation: "GetPolicyVersion",
		Body: []byte(`{"PolicyArn":"arn:aws:iam::123456789012:policy/read-the-data","VersionId":"v1"}`),
	})
	var out struct {
		Document string `xml:"GetPolicyVersionResult>PolicyVersion>Document"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), string(body))
	doc, err := url.QueryUnescape(out.Document)
	require.NoError(t, err)

	var parsed struct {
		Statement []struct {
			Resource string `json:"Resource"`
		} `json:"Statement"`
	}
	require.NoErrorf(t, json.Unmarshal([]byte(doc), &parsed), "document does not parse: %s", doc)
	require.Len(t, parsed.Statement, 1, doc)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123456789012:cfn-doc-data", parsed.Statement[0].Resource,
		"the statement names the queue the Fn::Sub built")
	requireNoIntrinsic(t, doc)
}

func TestCFN_SNSTopicPolicyDocumentResolvesIntrinsics(t *testing.T) {
	const tmpl = `{"Resources":{
		"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"cfn-doc-topic"}},
		"Policy":{"Type":"AWS::SNS::TopicPolicy","Properties":{
			"Topics":[{"Ref":"Topic"}],
			"PolicyDocument":{"Version":"2012-10-17","Statement":[{
				"Effect":"Allow","Principal":{"Service":"events.amazonaws.com"},
				"Action":"sns:Publish","Resource":{"Ref":"Topic"},
				"Condition":{"StringEquals":{"aws:SourceAccount":{"Ref":"AWS::AccountId"}}}}]}}}}}`
	deployer, registry := sfnDefinitionDeployer(t)
	result, err := deployer.Deploy(context.Background(), tmpl, "cfn-doc-topic-policy", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	const topicARN = "arn:aws:sns:us-east-1:123456789012:cfn-doc-topic"
	form := url.Values{"Action": {"GetTopicAttributes"}, "TopicArn": {topicARN}}
	body := cfnDocRoute(t, registry, &emulator.AWSRequest{
		Service: "sns", Operation: "GetTopicAttributes",
		Body:    []byte(form.Encode()),
		Params:  map[string]string{"Action": "GetTopicAttributes", "TopicArn": topicARN},
		Headers: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	})
	var out struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"GetTopicAttributesResult>Attributes>entry"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), string(body))
	var policy string
	for _, e := range out.Entries {
		if e.Key == "Policy" {
			policy = e.Value
		}
	}
	require.NotEmptyf(t, policy, "no Policy attribute in %s", body)
	assert.Contains(t, policy, `"Resource":"`+topicARN+`"`, "the statement names the topic")
	assert.Contains(t, policy, `"123456789012"`, "the pseudo-parameter in the Condition resolved")
	requireNoIntrinsic(t, policy)
}

func TestCFN_EventsRuleEventPatternResolvesIntrinsics(t *testing.T) {
	tests := []struct {
		name, pattern, want string
	}{
		{
			name:    "an Fn::GetAtt at depth",
			pattern: `{"source":["aws.sqs"],"resources":[{"Fn::GetAtt":["Data","Arn"]}]}`,
			want:    "arn:aws:sqs:us-east-1:123456789012:cfn-doc-rule-data",
		},
		{
			name:    "an Fn::Sub at depth",
			pattern: `{"source":["aws.sqs"],"detail":{"queue":[{"Fn::Sub":"${AWS::StackName}-q"}]}}`,
			want:    "cfn-doc-rule-stack-q",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := `{"Resources":{
				"Data":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-doc-rule-data"}},
				"Rule":{"Type":"AWS::Events::Rule","Properties":{
					"Name":"cfn-doc-rule","EventPattern":` + tc.pattern + `}}}}`
			deployer, registry := sfnDefinitionDeployer(t)
			result, err := deployer.Deploy(context.Background(), tmpl, "cfn-doc-rule-stack", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, result)

			body := cfnDocRoute(t, registry, &emulator.AWSRequest{
				Service: "eventbridge", Operation: "DescribeRule",
				Body:    []byte(`{"Name":"cfn-doc-rule"}`),
				Headers: map[string]string{"x-amz-target": "AWSEvents.DescribeRule"},
			})
			var out struct {
				EventPattern string `json:"EventPattern"`
			}
			require.NoError(t, json.Unmarshal(body, &out), string(body))
			require.Truef(t, json.Valid([]byte(out.EventPattern)), "pattern is not JSON: %s", out.EventPattern)
			assert.Contains(t, out.EventPattern, `"`+tc.want+`"`)
			requireNoIntrinsic(t, out.EventPattern)
		})
	}
}

// TestCFN_JSONDocumentStringFormIsNotQuotedTwice pins the string form of a Json property.
//
// CloudFormation takes a Json-typed property as an object or as a string holding the JSON, and
// AWS::ECR::Repository's LifecyclePolicyText is published as a String outright. Marshaling the
// string form quoted it a second time, so IAM refused the role as MalformedPolicyDocument; the
// string is already the document.
func TestCFN_JSONDocumentStringFormIsNotQuotedTwice(t *testing.T) {
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	encoded, err := json.Marshal(trust)
	require.NoError(t, err)
	tmpl := `{"Resources":{"Role":{"Type":"AWS::IAM::Role","Properties":{
		"RoleName":"cfn-doc-string-role","AssumeRolePolicyDocument":` + string(encoded) + `}}}}`

	deployer, registry := sfnDefinitionDeployer(t)
	result, err := deployer.Deploy(context.Background(), tmpl, "cfn-doc-string", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	body := cfnDocRoute(t, registry, &emulator.AWSRequest{
		Service: "iam", Operation: "GetRole",
		Body: []byte(`{"RoleName":"cfn-doc-string-role"}`),
	})
	assert.True(t, strings.Contains(string(body), "lambda.amazonaws.com"),
		"the trust policy's principal is in the stored role: %s", body)
}
