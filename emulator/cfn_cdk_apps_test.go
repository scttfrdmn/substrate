package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfn_cdk_apps_test.go pins that strict mode, the default since #1605, does not refuse ordinary
// CDK apps. The templates under testdata/cfn/cdk are `cdk synth` output from aws-cdk-lib with CDK
// 2.1149.0, with the feature flags `cdk init` sets:
//
//   - VpcApp: a default ec2.Vpc;
//   - LambdaApp: a function with an SQS event source;
//   - ApiApp: a LambdaRestApi;
//   - SnsApp: an SNS topic with an SQS subscription;
//   - EcsApp: a Fargate service in its own VPC;
//   - DdbApp: a provisioned table with read and write autoscaling;
//   - UrlApp: a function with a function URL and an S3 notification.
//
// The last two use types substrate cannot deploy, and are pinned as refused, naming them.

func cfnCDKApp(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "cfn", "cdk", name+".template.json"))
	require.NoError(t, err)
	return string(body)
}

// TestCFN_OrdinaryCDKAppsDeployInStrictMode deploys and deletes each supported app over the wire.
func TestCFN_OrdinaryCDKAppsDeployInStrictMode(t *testing.T) {
	for _, app := range []string{"VpcApp", "LambdaApp", "ApiApp", "SnsApp", "EcsApp"} {
		t.Run(app, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			ssmPutBootstrapVersion(t, ts)
			code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{
				"StackName": app, "TemplateBody": cfnCDKApp(t, app), "Capabilities.member.1": "CAPABILITY_IAM",
			})
			require.Equal(t, http.StatusOK, code, body)
			_, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": app})
			status, _, reason := cfnStackStatus(t, body)
			require.Equal(t, "CREATE_COMPLETE", status, reason)

			code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": app})
			require.Equal(t, http.StatusOK, code, body)
			code, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": app})
			assert.Equal(t, http.StatusBadRequest, code, "the stack deleted cleanly: %s", body)
		})
	}
}

// ssmPutBootstrapVersion writes the parameter every CDK template's BootstrapVersion parameter
// reads, as `cdk bootstrap` does.
func ssmPutBootstrapVersion(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	code, body := sjJSON(t, ts, "ssm.us-east-1.amazonaws.com", "AmazonSSM.PutParameter",
		map[string]any{"Name": "/cdk-bootstrap/hnb659fds/version", "Value": "21", "Type": "String"})
	require.Equal(t, http.StatusOK, code, "%s", body)
}

// TestCFN_CDKAppsWithUnsupportedTypesAreRefusedByName pins the two apps strict mode refuses, and
// which types it names.
func TestCFN_CDKAppsWithUnsupportedTypesAreRefusedByName(t *testing.T) {
	for app, types := range map[string][]string{
		"DdbApp": {"AWS::ApplicationAutoScaling::ScalableTarget", "AWS::ApplicationAutoScaling::ScalingPolicy"},
		"UrlApp": {"AWS::Lambda::Url", "Custom::S3BucketNotifications"},
	} {
		_, err := newStrictDeployer(t).Deploy(context.Background(), cfnCDKApp(t, app), app, map[string]string{"BootstrapVersion": "21"})
		require.Error(t, err, app)
		assert.ErrorIs(t, err, emulator.ErrCFNUnemulatedType)
		for _, typ := range types {
			assert.Contains(t, err.Error(), typ, app)
		}
	}
}

// TestCFN_RoutesAndAssociations pins AWS::EC2::Route and AWS::EC2::SubnetRouteTableAssociation:
// the create, an in-place target change and table move, a replacement, Fn::GetAtt, and the delete.
func TestCFN_RoutesAndAssociations(t *testing.T) {
	d := newStrictDeployer(t)
	ctx := context.Background()
	tmpl := func(routeTarget, assocTable, dest string) string {
		return `{"Resources":{
			"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.9.0.0/16"}},
			"Igw":{"Type":"AWS::EC2::InternetGateway"},
			"Igw2":{"Type":"AWS::EC2::InternetGateway"},
			"Attach":{"Type":"AWS::EC2::VPCGatewayAttachment","Properties":{"VpcId":{"Ref":"Vpc"},"InternetGatewayId":{"Ref":"Igw"}}},
			"Sn":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.9.1.0/24"}},
			"Rt":{"Type":"AWS::EC2::RouteTable","Properties":{"VpcId":{"Ref":"Vpc"}}},
			"Rt2":{"Type":"AWS::EC2::RouteTable","Properties":{"VpcId":{"Ref":"Vpc"}}},
			"Default":{"Type":"AWS::EC2::Route","DependsOn":"Attach","Properties":{"RouteTableId":{"Ref":"Rt"},
				"DestinationCidrBlock":"` + dest + `","GatewayId":{"Ref":"` + routeTarget + `"}}},
			"Assoc":{"Type":"AWS::EC2::SubnetRouteTableAssociation","Properties":{"SubnetId":{"Ref":"Sn"},"RouteTableId":{"Ref":"` + assocTable + `"}}}},
			"Outputs":{"Cidr":{"Value":{"Fn::GetAtt":["Default","CidrBlock"]}},"AssocId":{"Value":{"Fn::GetAtt":["Assoc","Id"]}}}}`
	}
	result, err := d.Deploy(ctx, tmpl("Igw", "Rt", "0.0.0.0/0"), "routes", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	route := findResource(t, result, "Default")
	assert.Equal(t, findResource(t, result, "Rt").PhysicalID+"|0.0.0.0/0", route.PhysicalID)
	assert.Equal(t, "0.0.0.0/0", result.Outputs["Cidr"])
	assoc := findResource(t, result, "Assoc").PhysicalID
	assert.True(t, strings.HasPrefix(assoc, "rtbassoc-"), assoc)
	assert.Equal(t, assoc, result.Outputs["AssocId"])

	// A new target is ReplaceRoute, and a new table for the association is
	// ReplaceRouteTableAssociation, which answers a new association ID.
	result, err = d.UpdateStack(ctx, tmpl("Igw2", "Rt2", "0.0.0.0/0"), "routes", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", result.Status, result.StatusReason)
	assert.Equal(t, route.PhysicalID, findResource(t, result, "Default").PhysicalID, "a target change is in place")
	moved := findResource(t, result, "Assoc").PhysicalID
	assert.NotEqual(t, assoc, moved)
	assert.True(t, strings.HasPrefix(moved, "rtbassoc-"), moved)

	// A new destination replaces the route.
	result, err = d.UpdateStack(ctx, tmpl("Igw2", "Rt2", "10.50.0.0/16"), "routes", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", result.Status, result.StatusReason)
	assert.True(t, strings.HasSuffix(findResource(t, result, "Default").PhysicalID, "|10.50.0.0/16"))

	for _, del := range d.DeleteStackResourcesForTest(ctx, "routes") {
		assert.Equal(t, "DELETE_COMPLETE", del.Status, "%s: %s", del.LogicalID, del.Reason)
	}
}

// TestCFN_LambdaPermissionAndQueuePolicy pins AWS::Lambda::Permission and AWS::SQS::QueuePolicy
// against what the services report.
func TestCFN_LambdaPermissionAndQueuePolicy(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(principal string, queues string) string {
		return `{"Resources":{
			"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}},
			"Fn":{"Type":"AWS::Lambda::Function","Properties":{"Runtime":"python3.12","Handler":"index.handler",
				"Role":{"Fn::GetAtt":["Role","Arn"]},"Code":{"ZipFile":"def handler(e, c): pass"}}},
			"Allow":{"Type":"AWS::Lambda::Permission","Properties":{"Action":"lambda:InvokeFunction",
				"FunctionName":{"Fn::GetAtt":["Fn","Arn"]},"Principal":"` + principal + `"}},
			"A":{"Type":"AWS::SQS::Queue"},"B":{"Type":"AWS::SQS::Queue"},
			"Policy":{"Type":"AWS::SQS::QueuePolicy","Properties":{"Queues":[` + queues + `],
				"PolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},
					"Action":"sqs:SendMessage","Resource":"*"}]}}}},
			"Outputs":{"Sid":{"Value":{"Fn::GetAtt":["Allow","Id"]}},"PolicyId":{"Value":{"Ref":"Policy"}},
				"Fn":{"Value":{"Ref":"Fn"}},"A":{"Value":{"Ref":"A"}},"B":{"Value":{"Ref":"B"}}}}`
	}
	outputs := func() map[string]string {
		_, body := cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "perm"})
		out := map[string]string{}
		for _, k := range []string{"Sid", "PolicyId", "Fn", "A", "B"} {
			i := strings.Index(body, "<OutputKey>"+k+"</OutputKey>")
			require.GreaterOrEqual(t, i, 0, body)
			rest := body[i:]
			v := rest[strings.Index(rest, "<OutputValue>")+len("<OutputValue>"):]
			out[k] = v[:strings.Index(v, "</OutputValue>")]
		}
		return out
	}
	statements := func(fn string) []string {
		code, raw := lambdaREST(t, ts, http.MethodGet, "/2015-03-31/functions/"+fn+"/policy")
		if code == http.StatusNotFound {
			return nil
		}
		require.Equal(t, http.StatusOK, code, raw)
		var resp struct{ Policy string }
		require.NoError(t, json.Unmarshal(raw, &resp))
		var doc struct {
			Statement []struct{ Sid, Principal interface{} }
		}
		require.NoError(t, json.Unmarshal([]byte(resp.Policy), &doc))
		var sids []string
		for _, s := range doc.Statement {
			sids = append(sids, s.Sid.(string))
		}
		return sids
	}
	queuePolicy := func(url string) string {
		code, body := singleAccountQuery(t, ts, "sqs", "", map[string]string{"Action": "GetQueueAttributes",
			"QueueUrl": url, "AttributeName.1": "Policy", "Version": "2012-11-05"})
		require.Equal(t, http.StatusOK, code, body)
		return body
	}

	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "perm",
		"TemplateBody": tmpl("apigateway.amazonaws.com", `{"Ref":"A"},{"Ref":"B"}`)})
	require.Equal(t, http.StatusOK, code, body)
	_, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "perm"})
	status, _, reason := cfnStackStatus(t, body)
	require.Equal(t, "CREATE_COMPLETE", status, reason)
	first := outputs()
	assert.True(t, strings.HasPrefix(first["Sid"], "perm-Allow-"), first["Sid"])
	assert.Equal(t, []string{first["Sid"]}, statements(first["Fn"]))
	assert.Contains(t, queuePolicy(first["A"]), "sns.amazonaws.com")
	assert.Contains(t, queuePolicy(first["B"]), "sns.amazonaws.com")

	// A changed Principal replaces the permission with a new statement, and the old one goes; a
	// queue dropped from the policy has its policy cleared.
	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "perm",
		"TemplateBody": tmpl("s3.amazonaws.com", `{"Ref":"A"}`)})
	require.Equal(t, http.StatusOK, code, body)
	second := outputs()
	assert.NotEqual(t, first["Sid"], second["Sid"])
	assert.Equal(t, []string{second["Sid"]}, statements(second["Fn"]))
	assert.Equal(t, first["PolicyId"], second["PolicyId"], "a queue policy updates in place")
	assert.Contains(t, queuePolicy(second["A"]), "sns.amazonaws.com")
	assert.NotContains(t, queuePolicy(second["B"]), "sns.amazonaws.com")

	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "perm"})
	require.Equal(t, http.StatusOK, code, body)
	code, _ = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "perm"})
	assert.Equal(t, http.StatusBadRequest, code)
}

// lambdaREST issues one Lambda REST request and returns its status and body.
func lambdaREST(t *testing.T, ts *emulator.TestServer, method, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, nil)
	require.NoError(t, err)
	req.Host = "lambda.us-east-1.amazonaws.com"
	return sjDo(t, req)
}
