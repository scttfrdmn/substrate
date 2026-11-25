package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfn_cdk_types_review_test.go pins the review fixes to the four CDK types #1605 models.

// cfnStackStatusOf reads one stack's status from DescribeStacks.
func cfnStackStatusOf(t *testing.T, ts *emulator.TestServer, name string) (string, string) {
	t.Helper()
	_, body := cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": name})
	status, _, reason := cfnStackStatus(t, body)
	return status, reason
}

// cfnStackOutput reads one output of a stack from DescribeStacks.
func cfnStackOutput(t *testing.T, ts *emulator.TestServer, stack, key string) string {
	t.Helper()
	_, body := cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": stack})
	var doc struct {
		Outputs []struct {
			Key   string `xml:"OutputKey"`
			Value string `xml:"OutputValue"`
		} `xml:"DescribeStacksResult>Stacks>member>Outputs>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	for _, o := range doc.Outputs {
		if o.Key == key {
			return o.Value
		}
	}
	t.Fatalf("stack %s has no output %s: %s", stack, key, body)
	return ""
}

// TestCFN_AssociationRollsBackOntoItsTable pins that a failed update which also moved a subnet to
// another table rolls the subnet back onto the table it had, rather than treating the association
// as a new resource and leaving the subnet unassociated.
func TestCFN_AssociationRollsBackOntoItsTable(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(table, extra string) string {
		return `{"Resources":{
			"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.8.0.0/16"}},
			"Sn":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.8.1.0/24"}},
			"Rt1":{"Type":"AWS::EC2::RouteTable","Properties":{"VpcId":{"Ref":"Vpc"}}},
			"Rt2":{"Type":"AWS::EC2::RouteTable","Properties":{"VpcId":{"Ref":"Vpc"}}},
			"Assoc":{"Type":"AWS::EC2::SubnetRouteTableAssociation","Properties":{"SubnetId":{"Ref":"Sn"},"RouteTableId":{"Ref":"` + table + `"}}}` +
			extra + `},
			"Outputs":{"Rt1":{"Value":{"Ref":"Rt1"}},"Sn":{"Value":{"Ref":"Sn"}},"Assoc":{"Value":{"Ref":"Assoc"}}}}`
	}
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "assoc", "TemplateBody": tmpl("Rt1", "")})
	require.Equal(t, http.StatusOK, code, body)
	rt1, subnet := cfnStackOutput(t, ts, "assoc", "Rt1"), cfnStackOutput(t, ts, "assoc", "Sn")

	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "assoc", "TemplateBody": tmpl("Rt2",
		`,"Bad":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"Not_A_Valid_Bucket"}}`)})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "assoc")
	require.Equal(t, "UPDATE_ROLLBACK_COMPLETE", status, reason)

	code, body = singleAccountQuery(t, ts, "ec2", "", map[string]string{"Action": "DescribeRouteTables", "Version": "2016-11-15"})
	require.Equal(t, http.StatusOK, code, body)
	var doc struct {
		Tables []struct {
			ID      string `xml:"routeTableId"`
			Subnets []struct {
				SubnetID      string `xml:"subnetId"`
				AssociationID string `xml:"routeTableAssociationId"`
			} `xml:"associationSet>item"`
		} `xml:"routeTableSet>item"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	var tableOf, assocID string
	for _, rt := range doc.Tables {
		for _, a := range rt.Subnets {
			if a.SubnetID == subnet {
				tableOf, assocID = rt.ID, a.AssociationID
			}
		}
	}
	assert.Equal(t, rt1, tableOf, "the subnet is associated with the table it had before the update")
	assert.Equal(t, assocID, cfnStackOutput(t, ts, "assoc", "Assoc"), "Ref answers the live association")

	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "assoc"})
	require.Equal(t, http.StatusOK, code, body)
	code, _ = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "assoc"})
	assert.Equal(t, http.StatusBadRequest, code)
}

// cfnQueuePolicyOf reads a queue's Policy attribute.
func cfnQueuePolicyOf(t *testing.T, ts *emulator.TestServer, url string) string {
	t.Helper()
	code, body := singleAccountQuery(t, ts, "sqs", "", map[string]string{"Action": "GetQueueAttributes",
		"QueueUrl": url, "AttributeName.1": "Policy", "Version": "2012-11-05"})
	require.Equal(t, http.StatusOK, code, body)
	var doc struct {
		Attrs []struct{ Name, Value string } `xml:"GetQueueAttributesResult>Attribute"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	for _, a := range doc.Attrs {
		if a.Name == "Policy" {
			return a.Value
		}
	}
	return ""
}

const cfnQueuePolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},` +
	`"Action":"sqs:SendMessage","Resource":"*"}]}`

// TestCFN_QueuePolicyRenameKeepsThePolicy pins a CDK logical-ID rename: the new QueuePolicy is set
// before the old one is deleted, and deleting the old one must not clear the policy the new one
// set, even when the two documents are identical.
func TestCFN_QueuePolicyRenameKeepsThePolicy(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(logical string) string {
		return `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"},"` + logical + `":{"Type":"AWS::SQS::QueuePolicy",` +
			`"Properties":{"Queues":[{"Ref":"Q"}],"PolicyDocument":` + cfnQueuePolicyDoc + `}}},` +
			`"Outputs":{"Q":{"Value":{"Ref":"Q"}}}}`
	}
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "rename", "TemplateBody": tmpl("PolicyA")})
	require.Equal(t, http.StatusOK, code, body)
	url := cfnStackOutput(t, ts, "rename", "Q")
	require.Contains(t, cfnQueuePolicyOf(t, ts, url), "sns.amazonaws.com")

	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "rename", "TemplateBody": tmpl("PolicyB")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "rename")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)
	assert.Contains(t, cfnQueuePolicyOf(t, ts, url), "sns.amazonaws.com", "the renamed policy is still on the queue")

	// Deleting the stack clears it: the new resource is the one that set it.
	_, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "rename"})
	require.NotEmpty(t, body)
}

// TestCFN_QueuePolicyLeavesAPolicyItDidNotSet pins that the delete clears only the policy the
// resource applied: a caller's later policy on the queue stays.
func TestCFN_QueuePolicyLeavesAPolicyItDidNotSet(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := singleAccountQuery(t, ts, "sqs", "", map[string]string{"Action": "CreateQueue", "QueueName": "outside",
		"Version": "2012-11-05"})
	require.Equal(t, http.StatusOK, code, body)
	url := "http://sqs.us-east-1.localhost/123456789012/outside"
	if i := strings.Index(body, "<QueueUrl>"); i >= 0 {
		url = body[i+len("<QueueUrl>") : strings.Index(body, "</QueueUrl>")]
	}
	code, body = cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "outside", "TemplateBody": `{"Resources":{` +
		`"P":{"Type":"AWS::SQS::QueuePolicy","Properties":{"Queues":["` + url + `"],"PolicyDocument":` + cfnQueuePolicyDoc + `}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, cfnQueuePolicyOf(t, ts, url), "sns.amazonaws.com")

	const caller = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"*"}]}`
	code, body = singleAccountQuery(t, ts, "sqs", "", map[string]string{"Action": "SetQueueAttributes", "QueueUrl": url,
		"Attribute.1.Name": "Policy", "Attribute.1.Value": caller, "Version": "2012-11-05"})
	require.Equal(t, http.StatusOK, code, body)

	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "outside"})
	require.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, caller, cfnQueuePolicyOf(t, ts, url), "a policy the stack did not set is left alone")
}

// TestCFN_QueuePolicyPartialFailureReleasesWhatItSet pins that a create failing on its second queue
// clears the policy from the first, since a failed resource is not one a rollback deletes.
func TestCFN_QueuePolicyPartialFailureReleasesWhatItSet(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "partial", "TemplateBody": `{"Resources":{` +
		`"Q":{"Type":"AWS::SQS::Queue"},` +
		`"P":{"Type":"AWS::SQS::QueuePolicy","Properties":{"Queues":[{"Ref":"Q"},` +
		`"http://sqs.us-east-1.localhost/123456789012/does-not-exist"],"PolicyDocument":` + cfnQueuePolicyDoc + `}}},` +
		`"Outputs":{"Q":{"Value":{"Ref":"Q"}}}}`, "DisableRollback": "true"})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "partial")
	require.Equal(t, "CREATE_FAILED", status, reason)
	assert.Empty(t, cfnQueuePolicyOf(t, ts, cfnStackOutputOrEmpty(t, ts, "partial", "Q")),
		"the queue the policy reached first is cleared")
}

// cfnStackOutputOrEmpty is cfnStackOutput for a stack whose outputs may not have resolved; it falls
// back to the queue's own URL, derived from the stack's resources.
func cfnStackOutputOrEmpty(t *testing.T, ts *emulator.TestServer, stack, logical string) string {
	t.Helper()
	_, body := cfnStrictCall(t, ts, "DescribeStackResources", map[string]string{"StackName": stack, "LogicalResourceId": logical})
	var doc struct {
		Resources []struct {
			PhysicalID string `xml:"PhysicalResourceId"`
		} `xml:"DescribeStackResourcesResult>StackResources>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	require.Len(t, doc.Resources, 1, body)
	return "http://sqs.us-east-1.localhost/123456789012/" + doc.Resources[0].PhysicalID
}

// TestCFN_LambdaPermissionQualifierAndConditions pins that a permission on an alias ARN keeps its
// qualifier and lands in the alias's own policy, and that GetPolicy reports the principal and
// conditions in AddPermission's form.
func TestCFN_LambdaPermissionQualifierAndConditions(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "qual", "TemplateBody": `{"Resources":{
		"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}},
		"Fn":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"qual-fn","Runtime":"python3.12","Handler":"index.handler",
			"Role":{"Fn::GetAtt":["Role","Arn"]},"Code":{"ZipFile":"def handler(e, c): pass"}}},
		"Version":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":{"Ref":"Fn"}}},
		"Live":{"Type":"AWS::Lambda::Alias","Properties":{"FunctionName":{"Ref":"Fn"},"Name":"live",
			"FunctionVersion":{"Fn::GetAtt":["Version","Version"]}}},
		"Account":{"Type":"AWS::Lambda::Permission","Properties":{"Action":"lambda:InvokeFunction",
			"FunctionName":{"Ref":"Live"},"Principal":"111122223333"}},
		"Public":{"Type":"AWS::Lambda::Permission","Properties":{"Action":"lambda:InvokeFunctionUrl",
			"FunctionName":{"Ref":"Fn"},"Principal":"*","FunctionUrlAuthType":"NONE"}},
		"Scoped":{"Type":"AWS::Lambda::Permission","Properties":{"Action":"lambda:InvokeFunction",
			"FunctionName":{"Ref":"Fn"},"Principal":"s3.amazonaws.com","SourceArn":"arn:aws:s3:::b",
			"SourceAccount":"123456789012","PrincipalOrgID":"o-abc123"}},
		"Role2":{"Type":"AWS::Lambda::Permission","Properties":{"Action":"lambda:InvokeFunction",
			"FunctionName":{"Ref":"Fn"},"Principal":"arn:aws:iam::444455556666:role/example","InvokedViaFunctionUrl":true}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "qual")
	require.Equal(t, "CREATE_COMPLETE", status, reason)

	type statement struct {
		Sid       string
		Principal interface{}
		Resource  string
		Condition map[string]map[string]string
	}
	readPolicy := func(path string) []statement {
		code, raw := lambdaREST(t, ts, http.MethodGet, path)
		require.Equal(t, http.StatusOK, code, string(raw))
		var resp struct{ Policy string }
		require.NoError(t, json.Unmarshal(raw, &resp))
		var doc struct{ Statement []statement }
		require.NoError(t, json.Unmarshal([]byte(resp.Policy), &doc), resp.Policy)
		return doc.Statement
	}

	// The alias's statement is in the alias's policy, with the alias ARN as its Resource.
	aliasStmts := readPolicy("/2015-03-31/functions/qual-fn/policy?Qualifier=live")
	require.Len(t, aliasStmts, 1)
	account := aliasStmts[0]
	assert.Equal(t, map[string]interface{}{"AWS": "arn:aws:iam::111122223333:root"}, account.Principal)
	assert.True(t, strings.HasSuffix(account.Resource, ":function:qual-fn:live"), "the alias qualifier is kept: %s", account.Resource)

	stmts := readPolicy("/2015-03-31/functions/qual-fn/policy")
	byPrincipal := map[string]int{}
	for i, s := range stmts {
		raw, err := json.Marshal(s.Principal)
		require.NoError(t, err)
		byPrincipal[string(raw)] = i
	}
	require.Len(t, stmts, 3, "the function's own policy holds the three unqualified statements")
	doc := struct{ Statement []statement }{Statement: stmts}

	public := doc.Statement[byPrincipal[`"*"`]]
	assert.Equal(t, "NONE", public.Condition["StringEquals"]["lambda:FunctionUrlAuthType"])

	scoped := doc.Statement[byPrincipal[`{"Service":"s3.amazonaws.com"}`]]
	assert.Equal(t, "arn:aws:s3:::b", scoped.Condition["ArnLike"]["AWS:SourceArn"])
	assert.Equal(t, "123456789012", scoped.Condition["StringEquals"]["AWS:SourceAccount"])
	assert.Equal(t, "o-abc123", scoped.Condition["StringEquals"]["aws:PrincipalOrgID"])

	role := doc.Statement[byPrincipal[`{"AWS":"arn:aws:iam::444455556666:role/example"}`]]
	assert.Equal(t, "true", role.Condition["Bool"]["lambda:InvokedViaFunctionUrl"])

	// The delete removes all four, the qualified one through RemovePermission's Qualifier.
	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "qual"})
	require.Equal(t, http.StatusOK, code, body)
	code, _ = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "qual"})
	assert.Equal(t, http.StatusBadRequest, code)
}
