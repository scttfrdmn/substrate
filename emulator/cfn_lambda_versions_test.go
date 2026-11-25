package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfnLambdaVersionTemplate is a function with a version and an alias on it, the shape the CDK emits
// for a function's currentVersion and an alias. code is the inline source, and alias the alias's
// Properties beyond FunctionName and Name.
func cfnLambdaVersionTemplate(code, versions, alias string) string {
	return `{"Resources":{
		"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}},
		"Fn":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"cfn-ver-fn","Runtime":"python3.12",
			"Handler":"index.handler","Role":{"Fn::GetAtt":["Role","Arn"]},"Code":{"ZipFile":"` + code + `"}}},
		` + versions + `,
		"Live":{"Type":"AWS::Lambda::Alias","Properties":{"FunctionName":{"Ref":"Fn"},"Name":"live",` + alias + `}}},
	"Outputs":{
		"VersionRef":{"Value":{"Ref":"V1"}},
		"VersionNumber":{"Value":{"Fn::GetAtt":["V1","Version"]}},
		"VersionArn":{"Value":{"Fn::GetAtt":["V1","FunctionArn"]}},
		"AliasRef":{"Value":{"Ref":"Live"}},
		"AliasArn":{"Value":{"Fn::GetAtt":["Live","AliasArn"]}}}}`
}

// TestCFN_LambdaVersionAndAlias pins AWS::Lambda::Version and AWS::Lambda::Alias (#1613): their Ref
// and Fn::GetAtt values, an alias updated in place onto a new version with weighted routing, and a
// stack delete that removes both.
func TestCFN_LambdaVersionAndAlias(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const fnArn = "arn:aws:lambda:us-east-1:123456789012:function:cfn-ver-fn"
	v1 := `"V1":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":{"Ref":"Fn"},"Description":"first"}}`

	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "ver",
		"TemplateBody": cfnLambdaVersionTemplate("def handler(e, c): return 1", v1,
			`"FunctionVersion":{"Fn::GetAtt":["V1","Version"]},"Description":"blue"`)})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "ver")
	require.Equal(t, "CREATE_COMPLETE", status, reason)

	assert.Equal(t, fnArn+":1", cfnStackOutput(t, ts, "ver", "VersionRef"), "Ref is the version ARN")
	assert.Equal(t, "1", cfnStackOutput(t, ts, "ver", "VersionNumber"))
	assert.Equal(t, fnArn+":1", cfnStackOutput(t, ts, "ver", "VersionArn"))
	assert.Equal(t, fnArn+":live", cfnStackOutput(t, ts, "ver", "AliasRef"), "Ref is the alias ARN")
	assert.Equal(t, fnArn+":live", cfnStackOutput(t, ts, "ver", "AliasArn"))

	code, raw := lambdaREST(t, ts, http.MethodGet, "/2015-03-31/functions/cfn-ver-fn/configuration?Qualifier=1")
	require.Equal(t, http.StatusOK, code, string(raw))
	assert.Contains(t, string(raw), `"Description":"first"`)

	// New code and a second version, with the alias moved onto it and a tenth of its traffic left on
	// the first: the alias updates in place, and the first version stays.
	v2 := v1 + `,"V2":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":{"Ref":"Fn"}}}`
	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "ver",
		"TemplateBody": cfnLambdaVersionTemplate("def handler(e, c): return 2", v2,
			`"FunctionVersion":{"Fn::GetAtt":["V2","Version"]},`+
				`"RoutingConfig":{"AdditionalVersionWeights":[{"FunctionVersion":{"Fn::GetAtt":["V1","Version"]},"FunctionWeight":0.1}]}`)})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "ver")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)

	code, raw = lambdaREST(t, ts, http.MethodGet, "/2015-03-31/functions/cfn-ver-fn/aliases/live")
	require.Equal(t, http.StatusOK, code, string(raw))
	var alias map[string]any
	require.NoError(t, json.Unmarshal(raw, &alias))
	assert.Equal(t, "2", alias["FunctionVersion"])
	assert.Equal(t, "", alias["Description"], "a removed Description is cleared")
	assert.Equal(t, map[string]any{"AdditionalVersionWeights": map[string]any{"1": 0.1}}, alias["RoutingConfig"])
	assert.Equal(t, fnArn+":live", cfnStackOutput(t, ts, "ver", "AliasArn"), "an in-place update keeps the alias")

	code, raw = lambdaREST(t, ts, http.MethodGet, "/2015-03-31/functions/cfn-ver-fn/versions")
	require.Equal(t, http.StatusOK, code, string(raw))
	assert.Equal(t, 3, strings.Count(string(raw), `"Version":`), "$LATEST and two versions: %s", raw)

	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "ver"})
	require.Equal(t, http.StatusOK, code, body)
	code, _ = lambdaREST(t, ts, http.MethodGet, "/2015-03-31/functions/cfn-ver-fn")
	assert.Equal(t, http.StatusNotFound, code)
	code, _ = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "ver"})
	assert.Equal(t, http.StatusBadRequest, code, "the stack deleted cleanly")
}

// TestCFN_LambdaAliasRefusedReportsTheResource pins that an alias Lambda refuses fails its resource
// rather than the deploy reporting it created.
func TestCFN_LambdaAliasRefusedReportsTheResource(t *testing.T) {
	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "badalias",
		"TemplateBody": `{"Resources":{
			"Fn":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"bad-alias-fn","Runtime":"python3.12",
				"Handler":"index.handler","Role":"arn:aws:iam::123456789012:role/r","Code":{"ZipFile":"x"}}},
			"Bad":{"Type":"AWS::Lambda::Alias","Properties":{"FunctionName":{"Ref":"Fn"},"Name":"live","FunctionVersion":"7"}},
			"BadVersion":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":"no-such-fn"}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "badalias")
	assert.Equal(t, "ROLLBACK_COMPLETE", status, reason)
}

// TestCFN_LambdaVersionOfAnUnchangedFunctionFails pins that an AWS::Lambda::Version cannot adopt a
// version that already exists. Replacing a Version while the function is unchanged fails the
// replacement as CloudFormation does, and the update rolls back with the existing version intact
// rather than the replacement adopting it and the old resource's cleanup deleting it.
func TestCFN_LambdaVersionOfAnUnchangedFunctionFails(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(description string) string {
		return `{"Resources":{
			"Fn":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"same-fn","Runtime":"python3.12",
				"Handler":"index.handler","Role":"arn:aws:iam::123456789012:role/r","Code":{"ZipFile":"x"}}},
			"V":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":{"Ref":"Fn"},"Description":"` + description + `"}}}}`
	}
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "same", "TemplateBody": tmpl("one")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "same")
	require.Equal(t, "CREATE_COMPLETE", status, reason)

	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "same", "TemplateBody": tmpl("two")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "same")
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", status, reason)
	_, events := cfnStrictCall(t, ts, "DescribeStackEvents", map[string]string{"StackName": "same"})
	assert.Contains(t, events, "A version for this Lambda function exists ( 1 ). Modify the function to create a new version.")

	code, raw := lambdaREST(t, ts, http.MethodGet, "/2015-03-31/functions/same-fn/configuration?Qualifier=1")
	assert.Equal(t, http.StatusOK, code, "the existing version survives the failed replacement: %s", raw)

	// Two versions of one unchanged function in one template: the second cannot be created.
	code, body = cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "twice", "TemplateBody": `{"Resources":{
		"Fn":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"twice-fn","Runtime":"python3.12",
			"Handler":"index.handler","Role":"arn:aws:iam::123456789012:role/r","Code":{"ZipFile":"x"}}},
		"A":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":{"Ref":"Fn"}}},
		"B":{"Type":"AWS::Lambda::Version","Properties":{"FunctionName":{"Ref":"Fn"}},"DependsOn":"A"}}}`})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "twice")
	assert.Equal(t, "ROLLBACK_COMPLETE", status, reason)
}
