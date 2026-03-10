package substrate

import (
	"context"
	"encoding/json"
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
	def := marshalToJSON(props["DefinitionString"])
	if def == "" {
		def = `{"Comment":"stub","StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`
	}

	body := map[string]interface{}{
		"name":       name,
		"definition": def,
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
