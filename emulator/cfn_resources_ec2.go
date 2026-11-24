package emulator

// StackDeployer helpers for CloudFormation's EC2 resource types: Elastic IPs, NAT gateways, launch
// templates, security groups and their rules, VPC gateway attachments, routes and subnet
// route-table associations. The rules every in-place updater follows are stated with
// cfnUpdateUnmodelledProperties.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ----- Elastic IPs and NAT Gateways -------------------------------------------

// deployEC2EIP allocates an Elastic IP address for the given CFN resource.
func (d *StackDeployer) deployEC2EIP(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	domain := resolveStringProp(props, "Domain", "vpc", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "AllocateAddress",
		Params: map[string]string{
			"Action": "AllocateAddress",
			"Domain": domain,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::EIP"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		allocationID := extractXMLField(resp.Body, "allocationId")
		publicIP := extractXMLField(resp.Body, "publicIp")
		dr.PhysicalID = allocationID
		dr.ARN = allocationID
		dr.Metadata = map[string]interface{}{
			"AllocationId": allocationID,
			"PublicIp":     publicIP,
		}
	}
	return dr, cost, nil
}

// deployEC2NatGateway creates a NAT gateway for the given CFN resource.
func (d *StackDeployer) deployEC2NatGateway(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	subnetID := resolveStringProp(props, "SubnetId", "", cctx)
	allocationID := resolveStringProp(props, "AllocationId", "", cctx)
	connectivityType := resolveStringProp(props, "ConnectivityType", "public", cctx)
	if subnetID == "" {
		dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::NatGateway", Error: "SubnetId is required"}
		return dr, 0, fmt.Errorf("deployEC2NatGateway: SubnetId is required")
	}
	params := map[string]string{
		"Action":           "CreateNatGateway",
		"SubnetId":         subnetID,
		"ConnectivityType": connectivityType,
	}
	if allocationID != "" {
		params["AllocationId"] = allocationID
	}
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateNatGateway",
		Params:    params,
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::NatGateway"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		natID := extractXMLField(resp.Body, "natGatewayId")
		dr.PhysicalID = natID
		dr.ARN = natID
	}
	return dr, cost, nil
}

// The launch-template, security-group-rule and VPC-gateway-attachment helpers below, and
// deployIAMInstanceProfile, wire CloudFormation resource types whose EC2/IAM API handlers
// already existed but which fell through to deployGenericStub, so a stack that
// declared them deployed "successfully" while the resource was never created
// (#388). Each handler dispatches the real API action, so the deployed resource
// is observable through the corresponding Describe call.

// cfnLaunchTemplateMetadataOptions are the five MetadataOptions members of an
// AWS::EC2::LaunchTemplate's LaunchTemplateData, spelled as both the template and
// RequestLaunchTemplateData spell them. HttpPutResponseHopLimit is the integer one and is
// rendered separately.
var cfnLaunchTemplateMetadataOptions = []string{"HttpEndpoint", "HttpProtocolIpv6", "HttpTokens", "InstanceMetadataTags"}

// cfnLaunchTemplateDataParams renders a template's LaunchTemplateData as the
// LaunchTemplateData.* query parameters CreateLaunchTemplate and CreateLaunchTemplateVersion
// both take.
//
// MetadataOptions and DisableApiTermination are forwarded since #1508. EC2 has stored and
// validated both since #1471, so an instance launched from the template receives them, but the
// deployer forwarded only the five members below and dropped both: a CDK template with
// requireImdsv2 — which emits MetadataOptions.HttpTokens: required — launched instances that did
// not require IMDSv2. The values are not checked here. EC2's own check runs on the request, and a
// value it refuses fails the resource with EC2's code and message, which is what a CloudFormation
// deploy reports for a property the service rejects.
//
// HttpPutResponseHopLimit is an Integer on the template's MetadataOptions page, and a template
// may write it as a number or as a string (a Ref to a Number parameter resolves to one). A whole
// number is rendered as one, so 2.0 out of the JSON decoder is sent as "2" rather than in a float
// form EC2 would refuse; anything else is sent as it resolved, for EC2 to refuse.
func cfnLaunchTemplateDataParams(props map[string]interface{}, cctx *cfnContext) map[string]string {
	params := map[string]string{}
	data, ok := props["LaunchTemplateData"].(map[string]interface{})
	if !ok {
		return params
	}
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
	if v, ok := data["DisableApiTermination"]; ok && !isCFNNoValue(v, cctx) {
		if s := resolveValue(v, cctx); s != "" {
			params["LaunchTemplateData.DisableApiTermination"] = s
		}
	}
	meta, ok := resolveNested(data["MetadataOptions"], cctx).(map[string]interface{})
	if !ok {
		return params
	}
	for _, key := range cfnLaunchTemplateMetadataOptions {
		if v := resolveValue(meta[key], cctx); v != "" {
			params["LaunchTemplateData.MetadataOptions."+key] = v
		}
	}
	if raw, present := meta["HttpPutResponseHopLimit"]; present && raw != nil {
		if n, isInt := cfnIntProp(meta, "HttpPutResponseHopLimit", cctx); isInt {
			params["LaunchTemplateData.MetadataOptions.HttpPutResponseHopLimit"] = strconv.Itoa(n)
		} else if s := resolveValue(raw, cctx); s != "" {
			params["LaunchTemplateData.MetadataOptions.HttpPutResponseHopLimit"] = s
		}
	}
	return params
}

// deployEC2LaunchTemplate creates an EC2 launch template for the given CFN
// resource. Its Ref is the launch template ID, which is what
// AWS::AutoScaling::AutoScalingGroup and CreateFleet consume — a stub's
// synthesized ARN is not a usable launch template reference.
//
// DefaultVersionNumber and LatestVersionNumber are recorded from CreateLaunchTemplate's own
// response (#1508), so Fn::GetAtt on either answers the template's version rather than the empty
// string. An update that changes LaunchTemplateData adds a version: see updateEC2LaunchTemplate.
func (d *StackDeployer) deployEC2LaunchTemplate(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// "If you do not specify a name, CloudFormation creates a name for you" (AWS::EC2::LaunchTemplate),
	// so an omitted name is generated rather than the logical ID, which can fail the name rule (#1507).
	name := resolveStringProp(props, "LaunchTemplateName", cfnGeneratedName(cctx, "AWS::EC2::LaunchTemplate", logicalID), cctx)

	params := cfnLaunchTemplateDataParams(props, cctx)
	params["Action"] = "CreateLaunchTemplate"
	params["LaunchTemplateName"] = name

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
		dr.Metadata = map[string]interface{}{
			"LaunchTemplateId":     dr.PhysicalID,
			"DefaultVersionNumber": extractXMLField(resp.Body, "defaultVersionNumber"),
			"LatestVersionNumber":  extractXMLField(resp.Body, "latestVersionNumber"),
		}
	}
	return dr, cost, nil
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

// deployEC2VPCGatewayAttachment attaches an internet gateway to a VPC for an
// AWS::EC2::VPCGatewayAttachment, which "Attaches an internet gateway, or a virtual private
// gateway to a VPC".
//
// Until #1492 the type fell through to [StackDeployer.deployGenericStub], so a stack's gateway
// was never attached; now that DeleteVpc and DeleteInternetGateway refuse an attached gateway
// with DependencyViolation, the attachment is real and the sweep detaches it (see
// [cfnResourceDeleters]) before either delete runs. A virtual private gateway attachment is still
// a stub, because substrate models no virtual private gateways.
//
// The physical ID stays the logical ID the stub recorded, so a Ref to the attachment — "the ID of
// the VPC gateway attachment", whose format AWS does not publish — resolves as it did.
func (d *StackDeployer) deployEC2VPCGatewayAttachment(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	igwID := resolveStringProp(props, "InternetGatewayId", "", cctx)
	if igwID == "" {
		return d.deployGenericStub(ctx, logicalID, "AWS::EC2::VPCGatewayAttachment", props, cctx)
	}
	_, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service:   "ec2",
		Operation: "AttachInternetGateway",
		Params: map[string]string{
			"Action":            "AttachInternetGateway",
			"InternetGatewayId": igwID,
			"VpcId":             resolveStringProp(props, "VpcId", "", cctx),
		},
		Headers: map[string]string{},
	}, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::EC2::VPCGatewayAttachment",
		PhysicalID: logicalID,
		ARN:        logicalID,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
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
	// CidrIpv6 is its own source list since #1453 stored IPv6 ranges, so it no longer
	// overwrites CidrIp: a rule declaring both carries both, and a revoke built from the
	// same template names the same two sources.
	if cidr6 := resolveStringProp(props, "CidrIpv6", "", cctx); cidr6 != "" {
		params[prefix+"Ipv6Ranges.1.CidrIpv6"] = cidr6
	}
	// A prefix-list source (ingress) or destination (egress).
	prefixList := resolveStringProp(props, "SourcePrefixListId", "", cctx)
	if prefixList == "" {
		prefixList = resolveStringProp(props, "DestinationPrefixListId", "", cctx)
	}
	if prefixList != "" {
		params[prefix+"PrefixListIds.1.PrefixListId"] = prefixList
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
		// AWS::EC2::SecurityGroup: "When you create a security group, if you do not add egress
		// rules, we add egress rules that allow all outbound IPv4 and IPv6 traffic. Otherwise, we
		// do not add them." CreateSecurityGroup always adds the IPv4 one, so a template declaring
		// egress has it revoked first — without that, a declared `-1` to 0.0.0.0/0 duplicates it
		// and is refused with InvalidPermission.Duplicate (#1491), and any other declared egress
		// leaves the group allowing everything.
		if spec.propKey == "SecurityGroupEgress" {
			_, revokeCost, revokeErr := d.dispatch(ctx, &AWSRequest{
				Service:   "ec2",
				Operation: "RevokeSecurityGroupEgress",
				Params: map[string]string{
					"Action": "RevokeSecurityGroupEgress", "GroupId": groupID,
					"IpPermissions.1.IpProtocol": "-1", "IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
				},
				Headers: map[string]string{},
			}, streamID)
			totalCost += revokeCost
			if revokeErr != nil && firstErr == "" {
				firstErr = revokeErr.Error()
			}
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

// updateEC2LaunchTemplate applies a changed AWS::EC2::LaunchTemplate's LaunchTemplateData by
// adding a version with CreateLaunchTemplateVersion, keeping the template's ID (#1508).
// LaunchTemplateData is Update requires: No interruption; LaunchTemplateName is Replacement, so a
// renamed template never reaches here.
//
// LatestVersionNumber is the new version's number, from the response, so Fn::GetAtt on it answers
// the version just created — the value an Auto Scaling group's LaunchTemplate.Version is written
// against. DefaultVersionNumber is left as it was: the page says "the default version of a launch
// template cannot be specified in AWS CloudFormation", and it is ModifyLaunchTemplate that sets it.
//
// A change to TagSpecifications or VersionDescription alone makes no call. VersionDescription is
// "a description for the first version", which already exists; the template's own tags are a
// CreateTags call, and are not re-applied here.
func (d *StackDeployer) updateEC2LaunchTemplate(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !slices.Contains(changed, "LaunchTemplateData") {
		return dr, 0, nil
	}
	params := cfnLaunchTemplateDataParams(props, cctx)
	params["Action"] = "CreateLaunchTemplateVersion"
	params["LaunchTemplateId"] = prior.PhysicalID
	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "ec2", Operation: "CreateLaunchTemplateVersion",
		Params: params, Headers: map[string]string{},
	}, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
	}
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	if resp != nil {
		if v := extractXMLField(resp.Body, "versionNumber"); v != "" {
			dr.Metadata["LatestVersionNumber"] = v
		}
	}
	return dr, cost, nil
}

// ----- AWS::EC2::SecurityGroup (#1550) --------------------------------------------------

// cfnSGDefaultEgress is the egress rule CreateSecurityGroup adds to a group declared without any:
// all traffic to 0.0.0.0/0, the IPv4 rule substrate's create adds and authorizeInlineSGRules revokes
// when a template declares egress.
var cfnSGDefaultEgress = []EC2IPPermission{{IPProtocol: "-1", IPRanges: []string{"0.0.0.0/0"}}}

// updateEC2SecurityGroup applies a changed AWS::EC2::SecurityGroup's inline SecurityGroupIngress
// and SecurityGroupEgress to the existing group, keeping its ID (#1550). Both are "Some
// interruptions"; GroupName, GroupDescription and VpcId are Replacement and never reach here.
//
// The rules are diffed **declaration against declaration**, as CloudFormation does: for each
// direction whose property changed, the rules the previous declaration held and the new one does
// not are revoked, then the rules the new one adds are authorized — the order the page gives: "If
// you modify a rule, CloudFormation removes the existing rule and then adds a new rule." A rule the
// group holds for another reason — an AWS::EC2::SecurityGroupIngress or SecurityGroupEgress
// resource, another stack, an out-of-band call — is in neither declaration and is left alone. The
// group's live rules are read only to keep each call legal: a rule to revoke that the group no
// longer holds is skipped, which a revoke in a non-default VPC would refuse, and a rule to add that
// it already holds is skipped, which an authorize would refuse as a duplicate.
//
// In a rollback the two declarations are the failed update's and the one being restored
// ([cfnBaseline.force]), so the same diff undoes the failed update's changes.
//
// Egress has one asymmetry, from the page. An undeclared SecurityGroupEgress stands for the default
// all-traffic rule when it is the previous declaration — "When you create a security group, if you
// do not add egress rules, we add egress rules that allow all outbound IPv4 and IPv6 traffic" — so
// declaring egress revokes the default, as the create does. As the new declaration it stands for no
// rule: "After the security group is created, if you remove all egress rules that you added, we do
// not add egress rules, so no outbound traffic is allowed." A rollback restoring a declaration with
// no egress restores the default rule, which is what the group had unless an earlier update had
// removed its declared egress; substrate keeps no record that tells the two apart.
//
// A rule whose description alone changed makes no call: substrate does not route
// UpdateSecurityGroupRuleDescriptions*. Tags make no call, as the file comment says.
func (d *StackDeployer) updateEC2SecurityGroup(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !cfnChangedAny(changed, "SecurityGroupIngress", "SecurityGroupEgress") {
		return dr, 0, nil
	}
	group, err := d.loadCFNSecurityGroup(ctx, prior.PhysicalID)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	if group == nil {
		dr.Error = fmt.Sprintf("InvalidGroup.NotFound: the security group %s does not exist", prior.PhysicalID)
		return dr, 0, nil
	}
	var cost float64
	for _, dir := range []struct {
		prop, revoke, authorize string
		live                    []EC2IPPermission
		egress                  bool
	}{
		{"SecurityGroupIngress", "RevokeSecurityGroupIngress", "AuthorizeSecurityGroupIngress", group.IngressRules, false},
		{"SecurityGroupEgress", "RevokeSecurityGroupEgress", "AuthorizeSecurityGroupEgress", group.EgressRules, true},
	} {
		if !slices.Contains(changed, dir.prop) {
			continue
		}
		before, declaredBefore := cfnSGDeclaredPermissions(cctx.priorProps[dir.prop], cctx)
		after, declaredAfter := cfnSGDeclaredPermissions(props[dir.prop], cctx)
		if dir.egress && !declaredBefore {
			before = cfnSGDefaultEgress
		}
		if dir.egress && !declaredAfter && cctx.restoring {
			after = cfnSGDefaultEgress
		}
		revoke, authorize := cfnSGRuleDiff(before, after, dir.live)
		for _, step := range []struct {
			action string
			perms  []EC2IPPermission
		}{{dir.revoke, revoke}, {dir.authorize, authorize}} {
			if len(step.perms) == 0 {
				continue
			}
			params := cfnSGPermissionParams(step.perms)
			params["Action"] = step.action
			params["GroupId"] = prior.PhysicalID
			if routeErr := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "ec2", Operation: step.action,
				Params: params, Headers: map[string]string{}}, streamID, &cost); routeErr != nil {
				dr.Error = routeErr.Error()
				return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
			}
		}
	}
	return dr, cost, nil
}

// loadCFNSecurityGroup reads a security group's record in the deployer's account and Region, or
// nil when there is none.
func (d *StackDeployer) loadCFNSecurityGroup(ctx context.Context, groupID string) (*EC2SecurityGroup, error) {
	if d.state == nil {
		return nil, nil
	}
	data, err := d.state.Get(ctx, ec2Namespace,
		"sg:"+d.identity.accountID+"/"+d.identity.region+"/"+groupID)
	if err != nil {
		return nil, fmt.Errorf("cfn load security group %s: %w", groupID, err)
	}
	if data == nil {
		return nil, nil
	}
	var sg EC2SecurityGroup
	if err := json.Unmarshal(data, &sg); err != nil {
		return nil, fmt.Errorf("cfn unmarshal security group %s: %w", groupID, err)
	}
	return &sg, nil
}

// cfnSGDeclaredPermissions parses a SecurityGroupIngress or SecurityGroupEgress list into the
// permissions an Authorize request carrying it would store, through the same two halves the
// create uses: [sgRuleParams] renders the request, and EC2's own parseSGPermissions reads it back.
// The list is resolved structurally first, so an element an Fn::If selects is a rule and one that
// is AWS::NoValue is none. The second result reports whether the property is declared at all.
func cfnSGDeclaredPermissions(v interface{}, cctx *cfnContext) ([]EC2IPPermission, bool) {
	rules, declared := cfnResolveStructured(v, cctx).([]interface{})
	if !declared {
		return nil, false
	}
	params := map[string]string{}
	n := 0
	for _, raw := range rules {
		rule, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		n++
		for k, val := range sgRuleParams(rule, "IpPermissions."+strconv.Itoa(n)+".", cctx) {
			params[k] = val
		}
	}
	if n == 0 {
		return nil, true
	}
	return parseSGPermissions(params, cctx.accountID), true
}

// cfnSGRuleKey identifies one rule as EC2 compares it — one source under one protocol and port
// range ([ec2CanonicalPortRange]) — with a CIDR in EC2's canonical form ([ec2CanonicalRuleCIDR]),
// so `10.0.0.1/16` and `10.0.0.0/16` are one rule.
type cfnSGRuleKey struct {
	port   ec2RulePortRange
	kind   string
	source string
}

// cfnSGRule is one source's rule, kept with the spelling it was stored or declared with, which is
// what a request names it by.
type cfnSGRule struct {
	perm EC2IPPermission
	keys []cfnSGRuleKey
}

// cfnSGRules splits permissions into one rule per source, the unit EC2 stores a rule at, each with
// the keys it is known by. A referenced group is known by its ID and by its name, since a
// declaration may name it either way and a stored pair carries both once EC2 has resolved it. A
// permission naming no source is one rule.
func cfnSGRules(perms []EC2IPPermission) []cfnSGRule {
	var out []cfnSGRule
	for _, perm := range perms {
		port := EC2IPPermission{IPProtocol: perm.IPProtocol, FromPort: perm.FromPort, ToPort: perm.ToPort}
		pr := ec2CanonicalPortRange(perm)
		one := func(p EC2IPPermission, keys ...cfnSGRuleKey) {
			out = append(out, cfnSGRule{perm: p, keys: keys})
		}
		if !ec2PermHasSource(perm) {
			one(port, cfnSGRuleKey{port: pr, kind: "none"})
			continue
		}
		for _, c := range perm.IPRanges {
			p := port
			p.IPRanges = []string{c}
			one(p, cfnSGRuleKey{port: pr, kind: "cidr", source: cfnSGCanonicalCIDR(c, false)})
		}
		for _, c := range perm.IPv6Ranges {
			p := port
			p.IPv6Ranges = []string{c}
			one(p, cfnSGRuleKey{port: pr, kind: "cidr6", source: cfnSGCanonicalCIDR(c, true)})
		}
		for _, id := range perm.PrefixListIDs {
			p := port
			p.PrefixListIDs = []string{id}
			one(p, cfnSGRuleKey{port: pr, kind: "pl", source: id})
		}
		for _, pair := range perm.UserIDGroupPairs {
			p := port
			p.UserIDGroupPairs = []EC2UserIDGroupPair{pair}
			var keys []cfnSGRuleKey
			if pair.GroupID != "" {
				keys = append(keys, cfnSGRuleKey{port: pr, kind: "sg", source: pair.GroupID})
			}
			if pair.GroupName != "" {
				keys = append(keys, cfnSGRuleKey{port: pr, kind: "sgname", source: pair.GroupName})
			}
			one(p, keys...)
		}
	}
	return out
}

// cfnSGKeySet indexes rules by every key each is known by.
func cfnSGKeySet(rules []cfnSGRule) map[cfnSGRuleKey]bool {
	set := make(map[cfnSGRuleKey]bool, len(rules))
	for _, r := range rules {
		for _, k := range r.keys {
			set[k] = true
		}
	}
	return set
}

// cfnSGHeld reports whether set holds rule under any of its keys.
func cfnSGHeld(set map[cfnSGRuleKey]bool, rule cfnSGRule) bool {
	for _, k := range rule.keys {
		if set[k] {
			return true
		}
	}
	return false
}

// cfnSGCanonicalCIDR is a rule CIDR in EC2's canonical form ([ec2CanonicalRuleCIDR]), or the text
// unchanged when EC2 would refuse it, so an unparseable value still compares against itself and
// the refusal comes from the request that carries it.
func cfnSGCanonicalCIDR(cidr string, v6 bool) string {
	canonical, err := ec2CanonicalRuleCIDR(cidr, v6)
	if err != nil {
		return cidr
	}
	return canonical
}

// cfnSGRuleDiff returns the rules to revoke — in before, not in after, and still held by the group
// — and the rules to authorize — in after, not in before, and not already held — each as one
// permission per source.
//
// The two declarations are each indexed once, so their diff is linear in the rule counts. Whether
// the group holds a rule is EC2's own test, [ec2AuthorizeDuplicates] — the check an authorize is
// refused on — so the updater and the plugin cannot disagree about what one rule is; it is asked
// only for the rules the declaration diff moves, with the candidate's CIDRs canonical as an
// authorize stores them.
func cfnSGRuleDiff(before, after, live []EC2IPPermission) (revoke, authorize []EC2IPPermission) {
	beforeRules, afterRules := cfnSGRules(before), cfnSGRules(after)
	beforeSet, afterSet := cfnSGKeySet(beforeRules), cfnSGKeySet(afterRules)
	held := func(perm EC2IPPermission) bool {
		probe := perm
		probe.IPRanges = slices.Clone(perm.IPRanges)
		probe.IPv6Ranges = slices.Clone(perm.IPv6Ranges)
		for i, c := range probe.IPRanges {
			probe.IPRanges[i] = cfnSGCanonicalCIDR(c, false)
		}
		for i, c := range probe.IPv6Ranges {
			probe.IPv6Ranges[i] = cfnSGCanonicalCIDR(c, true)
		}
		return ec2AuthorizeDuplicates(live, []EC2IPPermission{probe})
	}
	for _, r := range beforeRules {
		if !cfnSGHeld(afterSet, r) && held(r.perm) {
			revoke = append(revoke, r.perm)
		}
	}
	for _, r := range afterRules {
		if !cfnSGHeld(beforeSet, r) && !held(r.perm) {
			authorize = append(authorize, r.perm)
		}
	}
	return revoke, authorize
}

// cfnSGPermissionParams renders one-source permissions as the IpPermissions.N.* members an
// Authorize or Revoke request takes.
func cfnSGPermissionParams(perms []EC2IPPermission) map[string]string {
	params := map[string]string{}
	for i, perm := range perms {
		prefix := "IpPermissions." + strconv.Itoa(i+1) + "."
		params[prefix+"IpProtocol"] = perm.IPProtocol
		params[prefix+"FromPort"] = strconv.Itoa(perm.FromPort)
		params[prefix+"ToPort"] = strconv.Itoa(perm.ToPort)
		for j, c := range perm.IPRanges {
			params[fmt.Sprintf("%sIpRanges.%d.CidrIp", prefix, j+1)] = c
		}
		for j, c := range perm.IPv6Ranges {
			params[fmt.Sprintf("%sIpv6Ranges.%d.CidrIpv6", prefix, j+1)] = c
		}
		for j, id := range perm.PrefixListIDs {
			params[fmt.Sprintf("%sPrefixListIds.%d.PrefixListId", prefix, j+1)] = id
		}
		for j, pair := range perm.UserIDGroupPairs {
			gp := fmt.Sprintf("%sGroups.%d.", prefix, j+1)
			if pair.GroupID != "" {
				params[gp+"GroupId"] = pair.GroupID
			} else if pair.GroupName != "" {
				params[gp+"GroupName"] = pair.GroupName
			}
			if pair.UserID != "" {
				params[gp+"UserId"] = pair.UserID
			}
			if pair.Description != "" {
				params[gp+"Description"] = pair.Description
			}
		}
	}
	return params
}

// The route helpers below, deployLambdaPermission and deployQueuePolicy model the four resource
// types every ordinary CDK app emits that had no deploy helper, so strict mode — the default
// since #1605 — would have refused the app:
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
