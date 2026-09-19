package emulator_test

// A CloudFormation state machine's definition, and the three published properties it can come from
// (#1074).
//
// Every property of AWS::StepFunctions::StateMachine was resolved through the intrinsic context
// except the one carrying the definition — which is close to the only place a real template *has* to
// put an intrinsic, because an ASL Task state's Resource is a Lambda function ARN and a template that
// creates the function cannot know the ARN at authoring time.
//
// Unresolved, an Fn::Sub arrived at the deployer as a map and was stored as `{"Fn::Sub":"…"}`: a
// document that parses, describes an object, and has neither StartAt nor States. #1073 refuses that
// now, so the deploy fails loudly instead of succeeding and failing two operations later. Resolving it
// is what makes the deploy correct rather than merely loud — which is why these tests read the stored
// definition back through DescribeStateMachine and assert the ARN is in it.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfnSMFunctionARN is the ARN the template's Lambda function is deployed at, and therefore the value
// every intrinsic in these tests has to resolve to.
const cfnSMFunctionARN = "arn:aws:lambda:us-east-1:123456789012:function:cfn-sm-hello"

// cfnSMTemplate wraps one AWS::StepFunctions::StateMachine's properties in a template that also
// deploys the Lambda function those properties point at.
//
// The function comes first because the deployer resolves a reference against resources it has already
// deployed, which is also why this is the honest shape for the test: a template that could name the
// ARN literally would not exercise anything.
func cfnSMTemplate(smProps string) string {
	return `{"Resources":{` +
		`"Hello":{"Type":"AWS::Lambda::Function","Properties":{` +
		`"FunctionName":"cfn-sm-hello","Runtime":"python3.12","Handler":"index.handler",` +
		`"Role":"arn:aws:iam::123456789012:role/svc"}},` +
		`"SM":{"Type":"AWS::StepFunctions::StateMachine","Properties":{` + smProps + `}}}}`
}

// cfnSMDeployedDefinition deploys one template and returns the definition the state machine was
// actually stored with, read back through the owning service's own operation.
func cfnSMDeployedDefinition(t *testing.T, smProps, stackName, smName string) string {
	t.Helper()
	deployer, registry := sfnDefinitionDeployer(t)

	result, err := deployer.Deploy(context.Background(), cfnSMTemplate(smProps), stackName, nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	described := sfnDefinitionRoute(t, registry, "DescribeStateMachine", map[string]any{
		"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:" + smName,
	})
	definition, ok := described["definition"].(string)
	require.Truef(t, ok, "no definition in %v", described)
	return definition
}

// cfnSMTaskResource returns the Task state's Resource from a stored definition, so an assertion can
// name the member that had to resolve rather than matching a substring of the whole document.
func cfnSMTaskResource(t *testing.T, definition string) string {
	t.Helper()
	var doc struct {
		StartAt string `json:"StartAt"`
		States  map[string]struct {
			Type     string `json:"Type"`
			Resource string `json:"Resource"`
			End      bool   `json:"End"`
		} `json:"States"`
	}
	require.NoErrorf(t, json.Unmarshal([]byte(definition), &doc), "definition does not parse: %s",
		definition)
	require.Equal(t, "Invoke", doc.StartAt, definition)
	state, ok := doc.States["Invoke"]
	require.Truef(t, ok, "no Invoke state in %s", definition)
	return state.Resource
}

// TestCFNStateMachine_ADefinitionStringResolvesItsIntrinsics is the headline.
//
// Both spellings a template can use to build a document out of a deployed ARN are covered, and
// neither is the one the issue's example uses: Fn::Sub's `${LogicalId.Attribute}` form is
// unimplemented in the shared resolver — resolveRef has no dot handling, so `${Hello.Arn}` resolves to
// the literal string "Hello.Arn" — and that is a defect in every resource type's property, filed
// separately rather than fixed inside a Step Functions change. The Fn::Sub case here uses the
// two-element form, whose variable map AWS documents and which resolves an Fn::GetAtt per variable.
func TestCFNStateMachine_ADefinitionStringResolvesItsIntrinsics(t *testing.T) {
	for label, tc := range map[string]struct{ props, stack, name string }{
		"Fn::Join over an Fn::GetAtt": {
			props: `"StateMachineName":"cfn-sm-join","RoleArn":"arn:aws:iam::123456789012:role/svc",` +
				`"DefinitionString":{"Fn::Join":["",[` +
				`"{\"StartAt\":\"Invoke\",\"States\":{\"Invoke\":{\"Type\":\"Task\",\"Resource\":\"",` +
				`{"Fn::GetAtt":["Hello","Arn"]},` +
				`"\",\"End\":true}}}"]]}`,
			stack: "cfn-sm-join-stack",
			name:  "cfn-sm-join",
		},
		"Fn::Sub with a variable map": {
			props: `"StateMachineName":"cfn-sm-sub","RoleArn":"arn:aws:iam::123456789012:role/svc",` +
				`"DefinitionString":{"Fn::Sub":[` +
				`"{\"StartAt\":\"Invoke\",\"States\":{\"Invoke\":{\"Type\":\"Task\",` +
				`\"Resource\":\"${FnArn}\",\"End\":true}}}",` +
				`{"FnArn":{"Fn::GetAtt":["Hello","Arn"]}}]}`,
			stack: "cfn-sm-sub-stack",
			name:  "cfn-sm-sub",
		},
	} {
		t.Run(label, func(t *testing.T) {
			definition := cfnSMDeployedDefinition(t, tc.props, tc.stack, tc.name)

			assert.Equal(t, cfnSMFunctionARN, cfnSMTaskResource(t, definition),
				"the Task state's Resource is the deployed function's ARN")
			assert.NotContains(t, definition, "Fn::",
				"an unresolved intrinsic reached the stored definition")
		})
	}
}

// TestCFNStateMachine_TheObjectDefinitionIsRead covers the property substrate read nowhere, so a
// template using it silently deployed the stub.
//
// The stub is what makes this worth its own test rather than a row in the table above: a template
// whose whole workflow was ignored still deployed, still answered 200, and still had a *runnable*
// state machine at the end of it, so nothing a consumer could observe said the definition had been
// dropped.
func TestCFNStateMachine_TheObjectDefinitionIsRead(t *testing.T) {
	props := `"StateMachineName":"cfn-sm-object","RoleArn":"arn:aws:iam::123456789012:role/svc",` +
		`"Definition":{"StartAt":"Invoke","States":{"Invoke":{"Type":"Task",` +
		`"Resource":{"Fn::GetAtt":["Hello","Arn"]},"TimeoutSeconds":5,"End":true}}}`

	definition := cfnSMDeployedDefinition(t, props, "cfn-sm-object-stack", "cfn-sm-object")

	assert.Equal(t, cfnSMFunctionARN, cfnSMTaskResource(t, definition),
		"the object form's nested intrinsic resolved")
	assert.NotContains(t, definition, `"Comment":"stub"`, "the object definition was dropped for the stub")
	// Marshaled once, like the string form: a second encoding would store a JSON string literal
	// rather than an object, which is the #996 defect in the property next door.
	assert.True(t, strings.HasPrefix(definition, "{"), "the definition was double-encoded: %s", definition)
	// A literal number stays a number. resolveNested turns a resolved *intrinsic* into a string, and
	// a walk that stringified everything would rewrite every numeric ASL member on the way through.
	assert.Contains(t, definition, `"TimeoutSeconds":5`, "a literal number was stringified")
}

// TestCFNStateMachine_DefinitionSubstitutionsInjectADeployedARN covers AWS's own documented mechanism
// for the problem #1074 is about, which the issue does not mention.
//
// The undeclared placeholder is the assertion that matters as much as the substituted one. Reusing
// substituteTemplate would have resolved `${NotASubstitution}` through resolveRef, which returns the
// bare name for an unknown one — so the document would have lost the braces rather than keeping a
// string the template meant literally.
func TestCFNStateMachine_DefinitionSubstitutionsInjectADeployedARN(t *testing.T) {
	props := `"StateMachineName":"cfn-sm-subs","RoleArn":"arn:aws:iam::123456789012:role/svc",` +
		`"DefinitionString":"{\"StartAt\":\"Invoke\",\"States\":{\"Invoke\":{\"Type\":\"Task\",` +
		`\"Resource\":\"${HelloFunction}\",\"Comment\":\"${NotASubstitution}\",\"End\":true}}}",` +
		`"DefinitionSubstitutions":{"HelloFunction":{"Fn::GetAtt":["Hello","Arn"]}}`

	definition := cfnSMDeployedDefinition(t, props, "cfn-sm-subs-stack", "cfn-sm-subs")

	assert.Equal(t, cfnSMFunctionARN, cfnSMTaskResource(t, definition),
		"the declared substitution injected the deployed ARN")
	assert.Contains(t, definition, `${NotASubstitution}`,
		"an undeclared placeholder must be left exactly as written")
}

// TestCFNStateMachine_DefinitionStringWinsOverDefinition pins substrate's reading of a case AWS
// publishes no answer for.
//
// Both properties are "Required: No" and no sentence on the page covers supplying both. The order is
// chosen because it is the only one that changes nothing for a template that already deployed — every
// in-tree fixture and every CDK synth writes DefinitionString — so it is recorded here rather than
// left to whichever branch happens to run first.
func TestCFNStateMachine_DefinitionStringWinsOverDefinition(t *testing.T) {
	props := `"StateMachineName":"cfn-sm-both","RoleArn":"arn:aws:iam::123456789012:role/svc",` +
		`"DefinitionString":"{\"StartAt\":\"Invoke\",\"States\":{\"Invoke\":{\"Type\":\"Task\",` +
		`\"Resource\":\"` + cfnSMFunctionARN + `\",\"End\":true}}}",` +
		`"Definition":{"StartAt":"Ignored","States":{"Ignored":{"Type":"Pass","End":true}}}`

	definition := cfnSMDeployedDefinition(t, props, "cfn-sm-both-stack", "cfn-sm-both")

	assert.Equal(t, cfnSMFunctionARN, cfnSMTaskResource(t, definition))
	assert.NotContains(t, definition, "Ignored", "the object form won over the string form")
}

// TestCFNStateMachine_NoDefinitionPropertyStillGetsARunnableStub covers the two properties substrate
// declines and the case where a template supplies nothing at all.
//
// DefinitionS3Location is declined deliberately: it names an S3 object holding the document, and
// fetching it would make a deploy depend on a bucket's contents. So it lands in the same place as an
// absent definition — and the assertion that the stub *executes* is what makes the stub a stub rather
// than a deferred failure, now that #1073 refuses a definition that cannot run.
func TestCFNStateMachine_NoDefinitionPropertyStillGetsARunnableStub(t *testing.T) {
	for label, tc := range map[string]struct{ props, stack, name string }{
		"neither property": {
			props: `"StateMachineName":"cfn-sm-stub","RoleArn":"arn:aws:iam::123456789012:role/svc"`,
			stack: "cfn-sm-stub-stack",
			name:  "cfn-sm-stub",
		},
		"DefinitionS3Location, which substrate declines": {
			props: `"StateMachineName":"cfn-sm-s3","RoleArn":"arn:aws:iam::123456789012:role/svc",` +
				`"DefinitionS3Location":{"Bucket":"example","Key":"hello_world.json"}`,
			stack: "cfn-sm-s3-stack",
			name:  "cfn-sm-s3",
		},
	} {
		t.Run(label, func(t *testing.T) {
			deployer, registry := sfnDefinitionDeployer(t)

			result, err := deployer.Deploy(context.Background(), cfnSMTemplate(tc.props), tc.stack, nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, result)

			smARN := "arn:aws:states:us-east-1:123456789012:stateMachine:" + tc.name
			described := sfnDefinitionRoute(t, registry, "DescribeStateMachine",
				map[string]any{"stateMachineArn": smARN})
			assert.Contains(t, described["definition"], `"Comment":"stub"`)

			started := sfnDefinitionRoute(t, registry, "StartExecution",
				map[string]any{"stateMachineArn": smARN, "name": "stub-run"})
			execArn, ok := started["executionArn"].(string)
			require.Truef(t, ok, "no executionArn in %v", started)
			execution := sfnDefinitionRoute(t, registry, "DescribeExecution",
				map[string]any{"executionArn": execArn})
			assert.Equal(t, "SUCCEEDED", execution["status"], "the stub definition does not run")
		})
	}
}
