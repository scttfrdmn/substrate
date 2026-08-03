package emulator

// cfn_resources_v77.go holds the StackDeployer.deployResource helpers for
// EC2 (launch templates, security-group rules) and IAM instance profiles.
// The name records the substrate release that added them (v0.77.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// This file wires CloudFormation resource types whose EC2/IAM API handlers
// already existed but which fell through to deployGenericStub, so a stack that
// declared them deployed "successfully" while the resource was never created
// (#388). Each handler dispatches the real API action, so the deployed resource
// is observable through the corresponding Describe call.

// deployEC2LaunchTemplate creates an EC2 launch template for the given CFN
// resource. Its Ref is the launch template ID, which is what
// AWS::AutoScaling::AutoScalingGroup and CreateFleet consume — a stub's
// synthesized ARN is not a usable launch template reference.
func (d *StackDeployer) deployEC2LaunchTemplate(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "LaunchTemplateName", logicalID, cctx)

	params := map[string]string{
		"Action":             "CreateLaunchTemplate",
		"LaunchTemplateName": name,
	}
	if data, ok := props["LaunchTemplateData"].(map[string]interface{}); ok {
		for cfnKey, param := range map[string]string{
			"ImageId":      "LaunchTemplateData.ImageId",
			"InstanceType": "LaunchTemplateData.InstanceType",
			"KeyName":      "LaunchTemplateData.KeyName",
			"UserData":     "LaunchTemplateData.UserData",
		} {
			if v := resolveStringProp(data, cfnKey, "", cctx); v != "" {
				params[param] = v
			}
		}
		for i, sg := range resolveStringList(data["SecurityGroupIds"], cctx) {
			params[fmt.Sprintf("LaunchTemplateData.SecurityGroupId.%d", i+1)] = sg
		}
	}

	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service:   "ec2",
		Operation: "CreateLaunchTemplate",
		Params:    params,
		Headers:   map[string]string{},
	}, streamID)

	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::LaunchTemplate"}
	if routeErr != nil {
		// Recorded on the resource, not returned — a returned error aborts the stack.
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}
	if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "launchTemplateId")
		dr.ARN = fmt.Sprintf("arn:aws:ec2:%s:%s:launch-template/%s", cctx.region, cctx.accountID, dr.PhysicalID)
	}
	return dr, cost, nil
}

// deployIAMInstanceProfile creates an IAM instance profile for the given CFN
// resource and attaches the roles listed in Roles. An EC2 instance that
// references a stubbed profile is launched without one, which silently changes
// its permissions.
func (d *StackDeployer) deployIAMInstanceProfile(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "InstanceProfileName", logicalID, cctx)
	path := resolveStringProp(props, "Path", "/", cctx)

	bodyBytes, err := json.Marshal(map[string]string{
		"InstanceProfileName": name,
		"Path":                path,
	})
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal instance profile body: %w", err)
	}

	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service:   "iam",
		Operation: "CreateInstanceProfile",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}, streamID)

	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::IAM::InstanceProfile", PhysicalID: name}
	if routeErr != nil {
		// Recorded on the resource, not returned — a returned error aborts the stack.
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}
	if resp != nil {
		var result struct {
			ARN string `xml:"CreateInstanceProfileResult>InstanceProfile>Arn"`
		}
		if xmlErr := xmlUnmarshalIAM(resp.Body, &result); xmlErr == nil {
			dr.ARN = result.ARN
		}
	}

	// Attach each role. CFN allows only one, but the list form is what templates use.
	totalCost := cost
	for _, roleName := range resolveStringList(props["Roles"], cctx) {
		roleBody, marshalErr := json.Marshal(map[string]string{
			"InstanceProfileName": name,
			"RoleName":            roleName,
		})
		if marshalErr != nil {
			return dr, totalCost, fmt.Errorf("marshal add-role body: %w", marshalErr)
		}
		_, addCost, addErr := d.dispatch(ctx, &AWSRequest{
			Service:   "iam",
			Operation: "AddRoleToInstanceProfile",
			Body:      roleBody,
			Headers:   map[string]string{},
			Params:    map[string]string{},
		}, streamID)
		totalCost += addCost
		if addErr != nil && dr.Error == "" {
			dr.Error = addErr.Error()
		}
	}
	return dr, totalCost, nil
}

// deployEC2SecurityGroupIngress authorizes a standalone
// AWS::EC2::SecurityGroupIngress rule. The standalone form exists precisely for
// the rules the inline form cannot express — a self-referencing rule, or a pair
// of groups referencing each other — so stubbing it silently drops exactly the
// rules a template author had to reach for it to write.
func (d *StackDeployer) deployEC2SecurityGroupIngress(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	return d.deploySGRule(ctx, logicalID, "AWS::EC2::SecurityGroupIngress",
		"AuthorizeSecurityGroupIngress", props, streamID, cctx)
}

// deployEC2SecurityGroupEgress authorizes a standalone
// AWS::EC2::SecurityGroupEgress rule.
func (d *StackDeployer) deployEC2SecurityGroupEgress(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	return d.deploySGRule(ctx, logicalID, "AWS::EC2::SecurityGroupEgress",
		"AuthorizeSecurityGroupEgress", props, streamID, cctx)
}

// deploySGRule authorizes one standalone security-group rule via action.
func (d *StackDeployer) deploySGRule(
	ctx context.Context,
	logicalID, resourceType, action string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	groupID := resolveStringProp(props, "GroupId", "", cctx)
	if groupID == "" {
		// GroupName is accepted for default-VPC groups.
		groupID = resolveStringProp(props, "GroupName", "", cctx)
	}

	params := map[string]string{"Action": action, "GroupId": groupID}
	for k, v := range sgRuleParams(props, "IpPermissions.1.", cctx) {
		params[k] = v
	}

	_, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service:   "ec2",
		Operation: action,
		Params:    params,
		Headers:   map[string]string{},
	}, streamID)

	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      resourceType,
		// AWS assigns an opaque sgr- physical ID; the group is the useful handle.
		PhysicalID: groupID,
		ARN:        groupID,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// sgRuleParams translates one CFN security-group rule's properties into EC2
// query parameters under prefix. It handles both CIDR sources and
// security-group sources (SourceSecurityGroupId / DestinationSecurityGroupId),
// the latter being what a self-referencing rule needs.
func sgRuleParams(props map[string]interface{}, prefix string, cctx *cfnContext) map[string]string {
	params := map[string]string{}
	if proto := resolveStringProp(props, "IpProtocol", "", cctx); proto != "" {
		params[prefix+"IpProtocol"] = proto
	}
	if from := resolveStringProp(props, "FromPort", "", cctx); from != "" {
		params[prefix+"FromPort"] = from
	}
	if to := resolveStringProp(props, "ToPort", "", cctx); to != "" {
		params[prefix+"ToPort"] = to
	}
	if cidr := resolveStringProp(props, "CidrIp", "", cctx); cidr != "" {
		params[prefix+"IpRanges.1.CidrIp"] = cidr
	}
	// CidrIpv6 has no IPv6-specific storage yet; record it as a range so the rule
	// is not silently dropped.
	if cidr6 := resolveStringProp(props, "CidrIpv6", "", cctx); cidr6 != "" {
		params[prefix+"IpRanges.1.CidrIp"] = cidr6
	}

	// A source/destination security group. GroupId takes precedence over
	// GroupName, matching AWS.
	srcID := resolveStringProp(props, "SourceSecurityGroupId", "", cctx)
	if srcID == "" {
		srcID = resolveStringProp(props, "DestinationSecurityGroupId", "", cctx)
	}
	srcName := resolveStringProp(props, "SourceSecurityGroupName", "", cctx)
	if srcID != "" || srcName != "" {
		if srcID != "" {
			params[prefix+"Groups.1.GroupId"] = srcID
		} else {
			params[prefix+"Groups.1.GroupName"] = srcName
		}
		if owner := resolveStringProp(props, "SourceSecurityGroupOwnerId", "", cctx); owner != "" {
			params[prefix+"Groups.1.UserId"] = owner
		}
	}
	if desc := resolveStringProp(props, "Description", "", cctx); desc != "" && (srcID != "" || srcName != "") {
		params[prefix+"Groups.1.Description"] = desc
	}
	return params
}

// authorizeInlineSGRules authorizes the SecurityGroupIngress and
// SecurityGroupEgress rules declared inline on an AWS::EC2::SecurityGroup. These
// were previously parsed as part of the resource's properties but never
// applied, so DescribeSecurityGroups reported a group with no rules (#388).
func (d *StackDeployer) authorizeInlineSGRules(
	ctx context.Context,
	groupID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (float64, string) {
	var totalCost float64
	var firstErr string

	for _, spec := range []struct {
		propKey string
		action  string
	}{
		{"SecurityGroupIngress", "AuthorizeSecurityGroupIngress"},
		{"SecurityGroupEgress", "AuthorizeSecurityGroupEgress"},
	} {
		rules, ok := props[spec.propKey].([]interface{})
		if !ok {
			continue
		}
		params := map[string]string{"Action": spec.action, "GroupId": groupID}
		count := 0
		for _, raw := range rules {
			rule, isMap := raw.(map[string]interface{})
			if !isMap {
				continue
			}
			count++
			prefix := "IpPermissions." + strconv.Itoa(count) + "."
			for k, v := range sgRuleParams(rule, prefix, cctx) {
				params[k] = v
			}
		}
		if count == 0 {
			continue
		}
		_, cost, routeErr := d.dispatch(ctx, &AWSRequest{
			Service:   "ec2",
			Operation: spec.action,
			Params:    params,
			Headers:   map[string]string{},
		}, streamID)
		totalCost += cost
		if routeErr != nil && firstErr == "" {
			firstErr = routeErr.Error()
		}
	}
	return totalCost, firstErr
}

// cfnTypeServiceOverrides maps CFN service segments whose lower-cased form is
// not the substrate plugin name.
var cfnTypeServiceOverrides = map[string]string{
	"elasticloadbalancingv2": "elbv2",
	"certificatemanager":     "acm",
	"opensearchservice":      "opensearch",
	"servicediscovery":       "servicediscovery",
	"elasticache":            "elasticache",
	"applicationautoscaling": "application-autoscaling",
	"secretsmanager":         "secretsmanager",
	"stepfunctions":          "states",
	"serverless":             "lambda",
}

// servicePluginLoaded reports whether a plugin for the CFN resource type's
// service is registered. A generic stub for a type whose plugin *is* loaded
// usually means the type needs wiring rather than that the service is
// unsupported, which is the distinction the stub warning should surface (#388).
func (d *StackDeployer) servicePluginLoaded(resType string) bool {
	parts := strings.SplitN(resType, "::", 3)
	if len(parts) < 2 {
		return false
	}
	service := strings.ToLower(parts[1])
	if override, ok := cfnTypeServiceOverrides[service]; ok {
		service = override
	}
	for _, name := range d.registry.Names() {
		if name == service {
			return true
		}
	}
	return false
}

// resolveStringList resolves a CFN list-valued property to a string slice for a
// caller that indexes the result into an AWS query parameter (Member.1,
// Member.2, …).
//
// It delegates the resolution to resolveValueList and drops empty members. The
// numbering is what makes that necessary: an empty member would still occupy an
// index, so a query API would receive `SecurityGroupId.2=` and reject it. A
// caller that needs Fn::Split's documented empty elements preserved uses
// resolveValueList directly.
func resolveStringList(v interface{}, cctx *cfnContext) []string {
	resolved := resolveValueList(v, cctx)
	out := make([]string, 0, len(resolved))
	for _, s := range resolved {
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
