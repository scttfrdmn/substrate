package emulator

// cfn_resources_v20.go holds the StackDeployer.deployResource helpers for
// Step Functions.
// The name records the substrate release that added them (v0.20.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

import (
	"context"
	"encoding/json"
	"strings"
)

// ----- v0.20.0 — Step Functions --------------------------------------------

// deployStepFunctionsStateMachine creates a Step Functions state machine.
func (d *StackDeployer) deployStepFunctionsStateMachine(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "StateMachineName", logicalID, cctx)

	body := map[string]interface{}{
		"name":       name,
		"definition": cfnStateMachineDefinition(props, cctx),
		"roleArn":    resolveStringProp(props, "RoleArn", "", cctx),
		"type":       resolveStringProp(props, "StateMachineType", "STANDARD", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "states",
		Operation: "CreateStateMachine",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonStates.CreateStateMachine"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::StepFunctions::StateMachine",
		PhysicalID: name,
		Metadata:   map[string]interface{}{"Name": name},
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			StateMachineArn string `json:"stateMachineArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.StateMachineArn != "" {
			dr.ARN = result.StateMachineArn
		}
	}
	return dr, cost, nil
}

// cfnStateMachineStubDefinition is the definition a template that supplies none gets.
//
// It is a runnable state machine rather than a placeholder string, which is what lets
// the stub survive the structural validation #1073 added: a deploy that says nothing
// about the definition still produces a state machine an execution can run, so the
// stub is a stub and not a deferred failure.
const cfnStateMachineStubDefinition = `{"Comment":"stub","StartAt":"Start",` +
	`"States":{"Start":{"Type":"Pass","End":true}}}`

// cfnStateMachineDefinition resolves an AWS::StepFunctions::StateMachine's definition
// from whichever of the three published properties the template used.
//
// Every other property of this resource type was resolved through the intrinsic
// context and this one was not (#1074), which matters because the definition is close
// to the *only* place a real template has to put an intrinsic: an ASL Task state's
// Resource is a Lambda function ARN, and a template that creates the function cannot
// know the ARN at authoring time. Unresolved, an Fn::Sub arrived here as a
// map[string]interface{} and was stored as `{"Fn::Sub":"…"}` — a document that parses,
// describes an object, and has neither StartAt nor States. #1073 now refuses that at
// CreateStateMachine, so the deploy reports a resource error instead of succeeding and
// failing two operations later; resolving it is what makes the deploy correct rather
// than merely loud.
//
// Three properties, in the order this reads them:
//
//   - DefinitionString (String) goes through [resolveStringProp], which is a drop-in
//     here: an intrinsic resolves, a literal string passes through untouched, and its
//     empty-means-fallback behavior is exactly the stub rule. Note it must *not* go
//     through marshalToJSON — a template that supplies a string has already done the
//     encoding, and marshaling it a second time quoted and escaped the document into a
//     JSON string literal substrate could not read back (#996).
//   - Definition (Json) is the object form, read nowhere before #1074, so a template
//     using it silently deployed the stub. [resolveNested] resolves intrinsics at every
//     depth — it is the walk #526 added for exactly this shape — and the result is
//     marshaled once.
//   - DefinitionSubstitutions (Object) is applied last, to whichever of the two
//     supplied the document.
//
// DefinitionString wins when both it and Definition are present. AWS publishes no
// precedence — both are "Required: No" and no sentence covers supplying both — so this
// is substrate's reading, chosen because it is the one order that changes nothing for a
// template that already deployed: every in-tree fixture and every CDK synth uses
// DefinitionString.
//
// DefinitionS3Location is declined rather than modeled: it names an S3 object holding
// the document, and fetching it would make a deploy depend on a bucket's contents.
// A template using it gets the stub, and docs/services.md says so.
func cfnStateMachineDefinition(props map[string]interface{}, cctx *cfnContext) string {
	def := resolveStringProp(props, "DefinitionString", "", cctx)
	if def == "" {
		if raw, ok := props["Definition"]; ok {
			if marshaled := marshalToJSON(resolveNested(raw, cctx)); marshaled != "" &&
				marshaled != "null" {
				def = marshaled
			}
		}
	}
	if def == "" {
		return cfnStateMachineStubDefinition
	}
	return cfnApplyDefinitionSubstitutions(def, props, cctx)
}

// cfnApplyDefinitionSubstitutions replaces each ${key} in a definition with the value
// DefinitionSubstitutions maps that key to.
//
// This is AWS's own documented mechanism for the problem #1074 is about, and the issue
// does not mention it: "A map (string to string) that specifies the mappings for
// placeholder variables in the state machine definition. This enables the customer to
// inject values obtained at runtime, for example from intrinsic functions, in the state
// machine definition." The example on the resource page substitutes a Lambda ARN into a
// Task state's Resource — the same end the issue reaches for with Fn::Sub, and the one
// AWS actually writes down. Each value is resolved through the intrinsic context first,
// so `HelloFunction: !GetAtt Hello.Arn` injects the deployed ARN.
//
// Two deliberate limits. An undeclared ${key} is left **verbatim**, which is why this
// does not reuse [substituteTemplate]: that resolver falls back to [resolveRef] for an
// unknown name, and resolveRef returns the bare name, so an unrelated `${}` in the
// document would lose its braces rather than being left alone. And AWS's second
// published form, `${variable_1,variable_2,…}`, is not handled — it addresses a
// key-value map variable rather than naming a substitution key, and no key in the map
// can match it, so it falls through the undeclared case and is left as written.
//
// Substitution is textual, as AWS describes it ("${HelloFunction} will be replaced
// by …"), so a value carrying a quote produces invalid JSON. That is now a refusal at
// CreateStateMachine (#996) rather than a stored document, which is the right place for
// it: substrate cannot know whether the template meant the quote.
func cfnApplyDefinitionSubstitutions(
	def string,
	props map[string]interface{},
	cctx *cfnContext,
) string {
	raw, ok := props["DefinitionSubstitutions"].(map[string]interface{})
	if !ok || len(raw) == 0 {
		return def
	}
	subs := make(map[string]string, len(raw))
	for key, value := range raw {
		subs[key] = resolveValue(value, cctx)
	}

	var out strings.Builder
	for i := 0; i < len(def); {
		if def[i] == '$' && i+1 < len(def) && def[i+1] == '{' {
			if end := strings.Index(def[i+2:], "}"); end >= 0 {
				if value, declared := subs[def[i+2:i+2+end]]; declared {
					out.WriteString(value)
					i += 2 + end + 1
					continue
				}
			}
		}
		out.WriteByte(def[i])
		i++
	}
	return out.String()
}

// deployStepFunctionsActivity creates a Step Functions activity.
func (d *StackDeployer) deployStepFunctionsActivity(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{"name": name}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "states",
		Operation: "CreateActivity",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonStates.CreateActivity"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::StepFunctions::Activity", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ActivityArn string `json:"activityArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ActivityArn != "" {
			dr.ARN = result.ActivityArn
		}
	}
	return dr, cost, nil
}
