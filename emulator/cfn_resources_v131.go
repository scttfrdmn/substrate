package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
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
// parameter of the same name — every target ec2RouteTargetParams reads, which is every target the
// type declares.
var cfnRouteTargetProperties = []string{
	"CarrierGatewayId", "CoreNetworkArn", "EgressOnlyInternetGatewayId", "GatewayId", "InstanceId",
	"LocalGatewayId", "NatGatewayId", "NetworkInterfaceId", "OdbNetworkArn", "TransitGatewayId",
	"VpcEndpointId", "VpcPeeringConnectionId",
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
// AssociateRouteTable.
//
// The physical ID is the association ID the create answers, and it stays the resource's physical ID
// for life: an in-place move to another table (updateEC2SubnetRouteTableAssociation) answers a new
// association ID, which is recorded as AssociationId and is what Ref and Fn::GetAtt Id answer.
// Keeping the physical ID stable is what keeps an in-place update in place — a changed physical ID
// is how the stack tells a replacement, and a failed update would roll it back as one, leaving the
// subnet without the association it had.
func (d *StackDeployer) deployEC2SubnetRouteTableAssociation(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	subnet := resolveStringProp(props, "SubnetId", "", cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::SubnetRouteTableAssociation"}
	resp, cost, err := d.dispatch(ctx, &AWSRequest{Service: "ec2", Operation: "AssociateRouteTable",
		Headers: map[string]string{}, Params: map[string]string{
			"Action":       "AssociateRouteTable",
			"RouteTableId": resolveStringProp(props, "RouteTableId", "", cctx),
			"SubnetId":     subnet,
		}}, streamID)
	if err != nil {
		dr.Error = err.Error()
	} else {
		dr.PhysicalID = extractXMLField(resp.Body, "associationId")
		dr.Metadata = map[string]interface{}{"Id": dr.PhysicalID, "AssociationId": dr.PhysicalID, "SubnetId": subnet}
	}
	return dr, cost, nil
}

// updateEC2SubnetRouteTableAssociation moves the subnet to the new route table through
// ReplaceRouteTableAssociation — RouteTableId is "Update requires: No interruption". A changed
// SubnetId is Replacement.
//
// The association it replaces is the subnet's live one, read from EC2's record, rather than the one
// the stack recorded: the rollback of a failed update moves the subnet back from the association
// that update made, which the record the rollback starts from does not hold.
func (d *StackDeployer) updateEC2SubnetRouteTableAssociation(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	subnet := resolveStringProp(props, "SubnetId", "", cctx)
	current := d.cfnSubnetAssociation(ctx, subnet)
	if current == "" {
		current = cfnRefMetadata(prior, "AssociationId")
	}
	if current == "" {
		current = prior.PhysicalID
	}
	resp, cost, err := d.dispatch(ctx, &AWSRequest{Service: "ec2", Operation: "ReplaceRouteTableAssociation",
		Headers: map[string]string{}, Params: map[string]string{
			"Action":        "ReplaceRouteTableAssociation",
			"AssociationId": current,
			"RouteTableId":  resolveStringProp(props, "RouteTableId", "", cctx),
		}}, streamID)
	if err != nil {
		dr.Error = err.Error()
	} else if id := extractXMLField(resp.Body, "newAssociationId"); id != "" {
		dr.Metadata = cfnCopyMetadata(dr.Metadata)
		dr.Metadata["Id"] = id
		dr.Metadata["AssociationId"] = id
		dr.Metadata["SubnetId"] = subnet
	}
	return dr, cost, nil
}

// cfnSubnetAssociation returns the ID of the association that currently ties subnet to a route
// table, read from EC2's records, or "" when there is none or the records cannot be read.
func (d *StackDeployer) cfnSubnetAssociation(ctx context.Context, subnet string) string {
	if d.state == nil || subnet == "" {
		return ""
	}
	keys, err := d.state.List(ctx, ec2Namespace, "rtb:"+d.identity.accountID+"/"+d.identity.region+"/")
	if err != nil {
		return ""
	}
	for _, key := range keys {
		var rtb EC2RouteTable
		if !d.readEC2Record(ctx, key, &rtb) {
			continue
		}
		for _, a := range rtb.Associations {
			if a.SubnetID == subnet {
				return a.AssociationID
			}
		}
	}
	return ""
}

// cfnSubnetRouteTableAssociationDeleter disassociates the live association the resource recorded.
func cfnSubnetRouteTableAssociationDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	id := cfnRefMetadata(dr, "AssociationId")
	if id == "" {
		id = dr.PhysicalID
	}
	if id == "" {
		return nil
	}
	return &AWSRequest{Service: "ec2", Operation: "DisassociateRouteTable", Headers: map[string]string{},
		Params: map[string]string{"Action": "DisassociateRouteTable", "AssociationId": id}}
}

// ----- AWS::Lambda::Permission -------------------------------------------------------------------

// cfnLambdaFunctionName splits a FunctionName property — "the name of the Lambda function, version,
// or alias", which may be a name, a function ARN or a partial ARN, any of them with a version or
// alias suffix — into the name the API path takes and the qualifier AddPermission and
// RemovePermission take as their Qualifier parameter. A template usually passes the function's Arn
// attribute, which is how the CDK writes it, or an alias's.
func cfnLambdaFunctionName(v string) (name, qualifier string) {
	if i := strings.Index(v, ":function:"); i >= 0 {
		v = v[i+len(":function:"):]
	}
	name, qualifier, _ = strings.Cut(v, ":")
	return name, qualifier
}

// cfnLambdaQualifier is the query a permission request carries for a qualified function.
func cfnLambdaQualifier(qualifier string) map[string]string {
	if qualifier == "" {
		return map[string]string{}
	}
	return map[string]string{"Qualifier": qualifier}
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
	function, qualifier := cfnLambdaFunctionName(resolveStringProp(props, "FunctionName", "", cctx))
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Lambda::Permission", PhysicalID: sid,
		Metadata: map[string]interface{}{"Id": sid, "FunctionName": function}}
	if qualifier != "" {
		dr.Metadata["Qualifier"] = qualifier
	}
	body := map[string]interface{}{"StatementId": sid}
	for _, name := range []string{"Action", "Principal", "SourceArn", "SourceAccount", "PrincipalOrgID",
		"EventSourceToken", "FunctionUrlAuthType"} {
		if v := resolveStringProp(props, name, "", cctx); v != "" {
			body[name] = v
		}
	}
	if b, ok := cfgsvcCFNBool(props, "InvokedViaFunctionUrl"); ok && b {
		body["InvokedViaFunctionUrl"] = true
	}
	data, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployLambdaPermission marshal: %w", err)
	}
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "lambda", Operation: http.MethodPost,
		Path: "/2015-03-31/functions/" + function + "/policy", Body: data,
		Headers: map[string]string{}, Params: cfnLambdaQualifier(qualifier)}, streamID, &cost); err != nil {
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
		Headers: map[string]string{}, Params: cfnLambdaQualifier(cfnRefMetadata(dr, "Qualifier"))}
}

// ----- AWS::SQS::QueuePolicy ---------------------------------------------------------------------

// cfnQueuePolicyQueues returns the queue URLs a QueuePolicy's Queues property names — "The URLs of
// the queues to which you want to add the policy", which a template usually writes as Refs to the
// queues, whose Ref is the URL.
func cfnQueuePolicyQueues(props map[string]interface{}, cctx *cfnContext) []string {
	return resolveValueList(props["Queues"], cctx)
}

// cfnQueuePolicyOwnerKey is the cloudformation-namespace key recording which QueuePolicy last
// applied a queue's policy. A queue's policy is one attribute, so two QueuePolicy resources naming
// one queue — the old and new logical IDs of a renamed CDK construct, which deploys the new one
// before it deletes the old — contend for it, and only the one that set it last may clear it.
func (d *StackDeployer) cfnQueuePolicyOwnerKey(queueURL string) string {
	return "queuepolicy_owner:" + d.identity.accountID + "/" + d.identity.region + "|" + queueURL
}

// setSQSQueuePolicy sets (or, with an empty policy, clears) one queue's Policy attribute.
func (d *StackDeployer) setSQSQueuePolicy(ctx context.Context, queueURL, policy, streamID string, cost *float64) error {
	return d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "sqs", Operation: "SetQueueAttributes",
		Headers: map[string]string{}, Params: map[string]string{
			"Action": "SetQueueAttributes", "QueueUrl": queueURL,
			"Attribute.1.Name": "Policy", "Attribute.1.Value": policy,
		}}, streamID, cost)
}

// claimQueuePolicy sets the policy on a queue and records owner as the resource that set it.
func (d *StackDeployer) claimQueuePolicy(ctx context.Context, queueURL, policy, owner, streamID string, cost *float64) error {
	if err := d.setSQSQueuePolicy(ctx, queueURL, policy, streamID, cost); err != nil {
		return err
	}
	if d.state == nil {
		return nil
	}
	if err := d.state.Put(ctx, cfnNamespace, d.cfnQueuePolicyOwnerKey(queueURL), []byte(owner)); err != nil {
		return fmt.Errorf("cfn record queue policy owner: %w", err)
	}
	return nil
}

// releaseQueuePolicy clears the policy owner applied to a queue — only when owner is still the
// resource that last set it and the queue's live policy is still the one owner applied, so a policy
// another resource or a caller has since put on the queue is left alone. A queue already deleted is
// done.
func (d *StackDeployer) releaseQueuePolicy(ctx context.Context, queueURL, applied, owner, streamID string, cost *float64) error {
	if d.state != nil {
		recorded, err := d.state.Get(ctx, cfnNamespace, d.cfnQueuePolicyOwnerKey(queueURL))
		if err != nil {
			return fmt.Errorf("cfn read queue policy owner: %w", err)
		}
		if recorded != nil && string(recorded) != owner {
			return nil
		}
	}
	live, err := d.liveQueuePolicy(ctx, queueURL, streamID, cost)
	if err != nil {
		if cfnDeleteIsAbsent(err) {
			return nil
		}
		return err
	}
	if applied != "" && live != applied {
		return nil
	}
	if err := d.setSQSQueuePolicy(ctx, queueURL, "", streamID, cost); err != nil && !cfnDeleteIsAbsent(err) {
		return err
	}
	if d.state != nil {
		if err := d.state.Delete(ctx, cfnNamespace, d.cfnQueuePolicyOwnerKey(queueURL)); err != nil {
			return fmt.Errorf("cfn clear queue policy owner: %w", err)
		}
	}
	return nil
}

// liveQueuePolicy reads a queue's Policy attribute through GetQueueAttributes.
func (d *StackDeployer) liveQueuePolicy(ctx context.Context, queueURL, streamID string, cost *float64) (string, error) {
	resp, c, err := d.dispatch(ctx, &AWSRequest{Service: "sqs", Operation: "GetQueueAttributes",
		Headers: map[string]string{}, Params: map[string]string{
			"Action": "GetQueueAttributes", "QueueUrl": queueURL, "AttributeName.1": "Policy",
		}}, streamID)
	*cost += c
	if err != nil {
		return "", err
	}
	var out struct {
		Attributes []struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		} `xml:"GetQueueAttributesResult>Attribute"`
	}
	if err := xml.Unmarshal(resp.Body, &out); err != nil {
		return "", fmt.Errorf("cfn read queue policy: %w", err)
	}
	for _, a := range out.Attributes {
		if a.Name == "Policy" {
			return a.Value, nil
		}
	}
	return "", nil
}

// deployQueuePolicy applies the policy to each queue through SetQueueAttributes. The queues and
// the policy are recorded, so an update can release the queues it drops and the delete can release
// them all.
//
// A create that fails partway releases the queues it already set, since a failed resource is not
// one a rollback deletes. The physical ID and Fn::GetAtt Id are generated per stack, as
// CloudFormation generates the policy's ID; AWS publishes no format for it.
func (d *StackDeployer) deployQueuePolicy(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	id := cfnGeneratedName(cctx, "AWS::SQS::QueuePolicy", logicalID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SQS::QueuePolicy", PhysicalID: id,
		Metadata: map[string]interface{}{"Id": id}}
	policy := cfnJSONDocument(props["PolicyDocument"], cctx)
	var cost float64
	set, failure := d.setQueuePolicies(ctx, cfnQueuePolicyQueues(props, cctx), policy, id, streamID, &cost)
	if failure != "" {
		dr.Error = failure
		for _, q := range set {
			if err := d.releaseQueuePolicy(ctx, q, policy, id, streamID, &cost); err != nil {
				dr.Error += " (and the policy could not be cleared from " + q + ": " + err.Error() + ")"
			}
		}
		return dr, cost, nil
	}
	dr.Metadata["Queues"] = set
	dr.Metadata["Policy"] = policy
	return dr, cost, nil
}

// updateQueuePolicy re-applies a changed policy in place — both properties are "Update requires:
// No interruption" — and releases any queue the update drops. A failure partway records every queue
// the resource has touched, so the rollback and the delete still reach each one.
func (d *StackDeployer) updateQueuePolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	dr.Metadata = cfnCopyMetadata(dr.Metadata)
	previous, _ := cfnMetadataList(prior, "Queues")
	policy := cfnJSONDocument(props["PolicyDocument"], cctx)
	var cost float64
	set, failure := d.setQueuePolicies(ctx, cfnQueuePolicyQueues(props, cctx), policy,
		prior.PhysicalID, streamID, &cost)
	if failure != "" {
		dr.Error = failure
		dr.Metadata["Queues"] = cfnUnion(previous, set)
		return dr, cost, nil
	}
	if failure := d.releaseDroppedQueues(ctx, previous, set, cfnRefMetadata(prior, "Policy"), prior.PhysicalID,
		streamID, &cost); failure != "" {
		dr.Error = failure
		dr.Metadata["Queues"] = cfnUnion(previous, set)
		return dr, cost, nil
	}
	dr.Metadata["Queues"] = set
	dr.Metadata["Policy"] = policy
	return dr, cost, nil
}

// releaseDroppedQueues releases each queue in previous that kept does not name, answering the first
// refusal's text, or "".
func (d *StackDeployer) releaseDroppedQueues(
	ctx context.Context, previous, kept []string, applied, owner, streamID string, cost *float64,
) string {
	for _, q := range previous {
		if slices.Contains(kept, q) {
			continue
		}
		if err := d.releaseQueuePolicy(ctx, q, applied, owner, streamID, cost); err != nil {
			return err.Error()
		}
	}
	return ""
}

// setQueuePolicies sets policy on every queue in queues, as owner, answering the queues it set and
// the first refusal's text, or "".
func (d *StackDeployer) setQueuePolicies(
	ctx context.Context, queues []string, policy, owner, streamID string, cost *float64,
) ([]string, string) {
	set := make([]string, 0, len(queues))
	for _, q := range queues {
		if err := d.claimQueuePolicy(ctx, q, policy, owner, streamID, cost); err != nil {
			return set, err.Error()
		}
		set = append(set, q)
	}
	return set, ""
}

// cfnUnion returns a's members followed by those of b not in a.
func cfnUnion(a, b []string) []string {
	out := append([]string{}, a...)
	for _, v := range b {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}

// cfnDeleteQueuePolicy releases the policy from every queue the resource recorded. The policy
// depends on its queues, so the sweep deletes it before any queue the stack also owns; a queue that
// is already gone — one this stack does not own, deleted elsewhere — is skipped, as an absent
// resource is everywhere in the sweep.
func cfnDeleteQueuePolicy(ctx context.Context, d *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext, streamID string) *CFNResourceDeletion {
	queues, _ := cfnMetadataList(dr, "Queues")
	var cost float64
	for _, q := range queues {
		if err := d.releaseQueuePolicy(ctx, q, cfnRefMetadata(dr, "Policy"), dr.PhysicalID, streamID, &cost); err != nil {
			return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
		}
	}
	return nil
}
