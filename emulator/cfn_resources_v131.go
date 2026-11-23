package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// cfn_resources_v131.go models the four resource types every ordinary CDK app emits that had no
// deploy helper, so strict mode — the default since #1605 — would have refused the app:
//
//   - AWS::EC2::Route and AWS::EC2::SubnetRouteTableAssociation, which every ec2.Vpc emits for
//     each subnet's routing;
//   - AWS::Lambda::Permission, which a Lambda function behind API Gateway, an S3 notification or a
//     function URL emits;
//   - AWS::SQS::QueuePolicy, which an SNS-to-SQS subscription emits.
//
// Each is a call the plugins already route, so each is real: the create, an in-place update where
// the type's page says "No interruption", replacement where it says "Replacement"
// (cfnRecreationProperties), and the delete.

// ----- AWS::EC2::Route ---------------------------------------------------------------------------

// cfnRouteTargetProperties are AWS::EC2::Route's target properties, each the CreateRoute
// parameter of the same name. CoreNetworkArn and OdbNetworkArn are not among them: EC2 routes no
// target of either kind, so a template naming one is refused by CreateRoute as having no target.
var cfnRouteTargetProperties = []string{
	"CarrierGatewayId", "EgressOnlyInternetGatewayId", "GatewayId", "InstanceId", "LocalGatewayId",
	"NatGatewayId", "NetworkInterfaceId", "TransitGatewayId", "VpcEndpointId", "VpcPeeringConnectionId",
}

// cfnRouteDestinationProperties are the three ways a route names its destination.
var cfnRouteDestinationProperties = []string{"DestinationCidrBlock", "DestinationIpv6CidrBlock", "DestinationPrefixListId"}

// cfnRouteParams builds a CreateRoute or ReplaceRoute request's parameters from a route's
// properties: its table, its destination and its target.
func cfnRouteParams(action string, props map[string]interface{}, cctx *cfnContext) map[string]string {
	params := map[string]string{"Action": action, "RouteTableId": resolveStringProp(props, "RouteTableId", "", cctx)}
	for _, name := range append(append([]string{}, cfnRouteDestinationProperties...), cfnRouteTargetProperties...) {
		if v := resolveStringProp(props, name, "", cctx); v != "" {
			params[name] = v
		}
	}
	return params
}

// cfnRouteDestination returns the destination a route's properties name.
func cfnRouteDestination(params map[string]string) string {
	for _, name := range cfnRouteDestinationProperties {
		if v := params[name]; v != "" {
			return v
		}
	}
	return ""
}

// deployEC2Route adds a route through CreateRoute.
//
// The physical ID is "{RouteTableId}|{destination}", the pair that identifies a route — the
// resource's registry schema names RouteTableId and CidrBlock as its primary identifier — and that
// DeleteRoute needs back. AWS publishes no Ref format for the type, so the separator is substrate's
// reading. Fn::GetAtt CidrBlock answers the destination.
func (d *StackDeployer) deployEC2Route(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	params := cfnRouteParams("CreateRoute", props, cctx)
	dest := cfnRouteDestination(params)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::Route", PhysicalID: params["RouteTableId"] + "|" + dest}
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "ec2", Operation: "CreateRoute",
		Params: params, Headers: map[string]string{}}, streamID, &cost); err != nil {
		dr.Error = err.Error()
	} else {
		cfnSetMetadata(&dr, "CidrBlock", dest)
	}
	return dr, cost, nil
}

// updateEC2Route changes a route's target in place through ReplaceRoute — every target property is
// "Update requires: No interruption". A changed RouteTableId or destination is Replacement.
func (d *StackDeployer) updateEC2Route(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "ec2", Operation: "ReplaceRoute",
		Params: cfnRouteParams("ReplaceRoute", props, cctx), Headers: map[string]string{}}, streamID, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// cfnEC2RouteDeleter removes the route its physical ID names.
func cfnEC2RouteDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	table, dest, ok := strings.Cut(dr.PhysicalID, "|")
	if !ok || table == "" || dest == "" {
		return nil
	}
	param := "DestinationCidrBlock"
	switch {
	case strings.HasPrefix(dest, "pl-"):
		param = "DestinationPrefixListId"
	case strings.Contains(dest, ":"):
		param = "DestinationIpv6CidrBlock"
	}
	return &AWSRequest{Service: "ec2", Operation: "DeleteRoute", Headers: map[string]string{},
		Params: map[string]string{"Action": "DeleteRoute", "RouteTableId": table, param: dest}}
}

// ----- AWS::EC2::SubnetRouteTableAssociation -----------------------------------------------------

// deployEC2SubnetRouteTableAssociation associates a subnet with a route table through
// AssociateRouteTable. Ref and Fn::GetAtt Id are the association ID AssociateRouteTable answers.
func (d *StackDeployer) deployEC2SubnetRouteTableAssociation(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::SubnetRouteTableAssociation"}
	resp, cost, err := d.dispatch(ctx, &AWSRequest{Service: "ec2", Operation: "AssociateRouteTable",
		Headers: map[string]string{}, Params: map[string]string{
			"Action":       "AssociateRouteTable",
			"RouteTableId": resolveStringProp(props, "RouteTableId", "", cctx),
			"SubnetId":     resolveStringProp(props, "SubnetId", "", cctx),
		}}, streamID)
	if err != nil {
		dr.Error = err.Error()
	} else {
		dr.PhysicalID = extractXMLField(resp.Body, "associationId")
		cfnSetMetadata(&dr, "Id", dr.PhysicalID)
	}
	return dr, cost, nil
}

// updateEC2SubnetRouteTableAssociation moves the subnet to the new route table through
// ReplaceRouteTableAssociation — RouteTableId is "Update requires: No interruption" — and records
// the new association ID it answers. A changed SubnetId is Replacement.
func (d *StackDeployer) updateEC2SubnetRouteTableAssociation(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	resp, cost, err := d.dispatch(ctx, &AWSRequest{Service: "ec2", Operation: "ReplaceRouteTableAssociation",
		Headers: map[string]string{}, Params: map[string]string{
			"Action":        "ReplaceRouteTableAssociation",
			"AssociationId": prior.PhysicalID,
			"RouteTableId":  resolveStringProp(props, "RouteTableId", "", cctx),
		}}, streamID)
	if err != nil {
		dr.Error = err.Error()
	} else if id := extractXMLField(resp.Body, "newAssociationId"); id != "" {
		dr.PhysicalID = id
		dr.Metadata = cfnCopyMetadata(dr.Metadata)
		dr.Metadata["Id"] = id
	}
	return dr, cost, nil
}

// ----- AWS::Lambda::Permission -------------------------------------------------------------------

// cfnLambdaFunctionName reduces a FunctionName property — "the name of the Lambda function, version,
// or alias", which may be a name, a function ARN or a partial ARN — to the name the API path takes.
// A template usually passes the function's Arn attribute, which is how the CDK writes it.
func cfnLambdaFunctionName(v string) string {
	if i := strings.Index(v, ":function:"); i >= 0 {
		v = v[i+len(":function:"):]
	}
	name, _, _ := strings.Cut(v, ":")
	return name
}

// deployLambdaPermission adds a statement to the function's resource policy through AddPermission.
//
// CloudFormation names the statement, the type declaring no StatementId, so substrate generates one
// per stack as it does a name ({stack}-{logical}-{suffix}), and a replacement draws a new one — every
// property is "Update requires: Replacement", and the replacement is added before the old statement
// is removed, so the two must not collide. The physical ID and Fn::GetAtt Id are the statement ID;
// the function is recorded too, because RemovePermission needs both.
func (d *StackDeployer) deployLambdaPermission(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	sid := cfnGeneratedName(cctx, "AWS::Lambda::Permission", logicalID)
	function := cfnLambdaFunctionName(resolveStringProp(props, "FunctionName", "", cctx))
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Lambda::Permission", PhysicalID: sid,
		Metadata: map[string]interface{}{"Id": sid, "FunctionName": function}}
	body := map[string]interface{}{"StatementId": sid}
	for _, name := range []string{"Action", "Principal", "SourceArn", "SourceAccount", "PrincipalOrgID",
		"EventSourceToken", "FunctionUrlAuthType"} {
		if v := resolveStringProp(props, name, "", cctx); v != "" {
			body[name] = v
		}
	}
	data, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployLambdaPermission marshal: %w", err)
	}
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "lambda", Operation: http.MethodPost,
		Path: "/2015-03-31/functions/" + function + "/policy", Body: data,
		Headers: map[string]string{}, Params: map[string]string{}}, streamID, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// cfnLambdaPermissionDeleter removes the statement from the function's policy.
func cfnLambdaPermissionDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	function := cfnRefMetadata(dr, "FunctionName")
	if function == "" || dr.PhysicalID == "" {
		return nil
	}
	return &AWSRequest{Service: "lambda", Operation: http.MethodDelete,
		Path:    "/2015-03-31/functions/" + function + "/policy/" + dr.PhysicalID,
		Headers: map[string]string{}, Params: map[string]string{}}
}

// ----- AWS::SQS::QueuePolicy ---------------------------------------------------------------------

// cfnQueuePolicyQueues returns the queue URLs a QueuePolicy's Queues property names — "The URLs of
// the queues to which you want to add the policy", which a template usually writes as Refs to the
// queues, whose Ref is the URL.
func cfnQueuePolicyQueues(props map[string]interface{}, cctx *cfnContext) []string {
	return resolveValueList(props["Queues"], cctx)
}

// setSQSQueuePolicy sets (or, with an empty policy, clears) one queue's Policy attribute.
func (d *StackDeployer) setSQSQueuePolicy(ctx context.Context, queueURL, policy, streamID string, cost *float64) error {
	return d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sqs", Operation: "SetQueueAttributes",
		Headers: map[string]string{}, Params: map[string]string{
			"Action": "SetQueueAttributes", "QueueUrl": queueURL,
			"Attribute.1.Name": "Policy", "Attribute.1.Value": policy,
		}}, streamID, cost)
}

// deployQueuePolicy applies the policy to each queue through SetQueueAttributes. The queues are
// recorded, so an update can clear the ones it drops and the delete can clear them all.
//
// The physical ID and Fn::GetAtt Id are generated per stack, as CloudFormation generates the
// policy's ID; AWS publishes no format for it.
func (d *StackDeployer) deployQueuePolicy(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	id := cfnGeneratedName(cctx, "AWS::SQS::QueuePolicy", logicalID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SQS::QueuePolicy", PhysicalID: id,
		Metadata: map[string]interface{}{"Id": id}}
	return d.applyQueuePolicy(ctx, dr, nil, props, streamID, cctx)
}

// updateQueuePolicy re-applies a changed policy in place — both properties are "Update requires:
// No interruption" — and clears the policy from any queue the update drops.
func (d *StackDeployer) updateQueuePolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	previous, _ := cfnMetadataList(prior, "Queues")
	return d.applyQueuePolicy(ctx, dr, previous, props, streamID, cctx)
}

// applyQueuePolicy sets the policy on every queue props names, clears it on each queue in previous
// that props no longer names, and records the queues on dr.
func (d *StackDeployer) applyQueuePolicy(
	ctx context.Context, dr DeployedResource, previous []string, props map[string]interface{},
	streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	queues := cfnQueuePolicyQueues(props, cctx)
	policy := cfnJSONDocument(props["PolicyDocument"], cctx)
	var cost float64
	if failure := d.setQueuePolicies(ctx, queues, previous, policy, streamID, &cost); failure != "" {
		dr.Error = failure
		return dr, cost, nil
	}
	dr.Metadata = cfnCopyMetadata(dr.Metadata)
	dr.Metadata["Queues"] = append([]string{}, queues...)
	return dr, cost, nil
}

// setQueuePolicies sets policy on every queue in queues and clears it on each queue in previous
// that queues no longer names, answering the first refusal's text, or "".
func (d *StackDeployer) setQueuePolicies(
	ctx context.Context, queues, previous []string, policy, streamID string, cost *float64,
) string {
	kept := map[string]bool{}
	for _, q := range queues {
		kept[q] = true
		if err := d.setSQSQueuePolicy(ctx, q, policy, streamID, cost); err != nil {
			return err.Error()
		}
	}
	for _, q := range previous {
		if kept[q] {
			continue
		}
		if err := d.setSQSQueuePolicy(ctx, q, "", streamID, cost); err != nil && !cfnDeleteIsAbsent(err) {
			return err.Error()
		}
	}
	return ""
}

// cfnDeleteQueuePolicy clears the policy from every queue the resource recorded. A queue already
// deleted — the stack's own queue, swept first — is skipped, as an absent resource is everywhere
// in the sweep.
func cfnDeleteQueuePolicy(ctx context.Context, d *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext, streamID string) *CFNResourceDeletion {
	queues, _ := cfnMetadataList(dr, "Queues")
	var cost float64
	for _, q := range queues {
		if err := d.setSQSQueuePolicy(ctx, q, "", streamID, &cost); err != nil && !cfnDeleteIsAbsent(err) {
			return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
		}
	}
	return nil
}
