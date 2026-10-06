package emulator

// cfn_resources_v32.go holds the StackDeployer.deployResource helpers for
// Athena, Backup, CloudTrail, the CodeSuite, OpenSearch, Transfer and WAFv2.
// The name records the substrate release that added them (v0.32.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ----- v0.32.0 — Extended CFN stubs -------------------------------------------

// stubStore persists resource properties into cfnStubNamespace.
func (d *StackDeployer) stubStore(ctx context.Context, acct, region, logicalID string, props map[string]interface{}) error {
	if d.state == nil || props == nil {
		return nil
	}
	data, err := json.Marshal(props)
	if err != nil {
		return fmt.Errorf("cfn stubStore %s marshal: %w", logicalID, err)
	}
	key := fmt.Sprintf("%s/%s/%s", acct, region, logicalID)
	if err := d.state.Put(ctx, cfnStubNamespace, key, data); err != nil {
		return fmt.Errorf("cfn stubStore %s state.Put: %w", logicalID, err)
	}
	return nil
}

// deployOpenSearchDomain deploys an AWS::OpenSearchService::Domain or an AWS::Elasticsearch::Domain,
// whose Ref is the domain name for both types.
//
// It stays a stub in cfnStubNamespace, by decision rather than by omission: the OpenSearch plugin
// has no control plane (no CreateDomain or DescribeDomain is routed, #1212), so the deployer has
// no create handler to call. The plugin's data plane answers every *.es.amazonaws.com host, and it
// holds one cluster rather than one per domain. So the DomainEndpoint recorded here is a host that
// data plane serves, and the documents a consumer writes to it are visible to every other domain.
// When the control plane is routed, this deploy should dispatch CreateDomain instead.
//
// AWS::Elasticsearch::Domain is deployed here too, rather than declined. AWS still documents it
// ("While the legacy Elasticsearch resource and options are still supported"), and its Ref and its
// three attributes (Arn, DomainArn, DomainEndpoint) are a subset of the OpenSearch type's.
//
// The attributes are recorded so Fn::GetAtt answers them, because a stub recorded none and
// DomainEndpoint resolved to an empty string (#1203):
//   - Arn and DomainArn are the domain ARN, resolved by the ARN-suffix rule.
//   - DomainEndpoint is search-{name}-{suffix}.{region}.es.amazonaws.com, the form of the page's
//     example. The suffix is derived from the account, Region, stack and logical ID, so an
//     UpdateStack keeps the endpoint a consumer already wired up. The width is substrate's
//     reading: AWS's example has a 26-character suffix, and this one is 12.
//   - Id, published for the OpenSearch type only, is {account}/{name}, the form of the page's
//     example 123456789012/my-domain.
//
// DomainEndpointV2 is "provisioned" only when IPAddressType is dualstack, and substrate does not
// model the dual-stack endpoint, so Fn::GetAtt refuses it, as it does the two nested attributes
// (see cfnGetAttUnmodelled).
func (d *StackDeployer) deployOpenSearchDomain(
	ctx context.Context,
	resType string,
	logicalID string,
	props map[string]interface{},
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "DomainName", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:es:%s:%s:domain/%s", cctx.region, cctx.accountID, name)
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployOpenSearchDomain stubStore: %w", err)
	}
	suffix := cfnNameSuffix(cctx.accountID, cctx.region, cctx.stackName, logicalID)
	meta := map[string]interface{}{
		"DomainEndpoint": fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", strings.ToLower(name), suffix, cctx.region),
	}
	if resType == "AWS::OpenSearchService::Domain" {
		// Id is published for the OpenSearch type only.
		meta["Id"] = cctx.accountID + "/" + name
	}
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       resType,
		PhysicalID: name,
		ARN:        arn,
		Metadata:   meta,
	}, 0, nil
}

// deployWAFv2WebACL creates a WAFv2 WebACL stub.
//
// Ref returns "name|id|scope", so the name and the scope are recorded in Metadata: the ARN
// carries the name and the ID but not the scope, and the scope is not derivable from the ARN
// once it is in the segment (#827).
//
// Scope is a required property whose allowed values are CLOUDFRONT and REGIONAL, and it
// selects the ARN's scope segment — "cloudfront" or "regional". That segment used to be
// hardcoded to "regional", so a CLOUDFRONT web ACL reported an ARN naming a scope it does not
// have. It is corrected here rather than left wrong beside a Metadata entry recording the real
// scope two lines away.
//
// The ARN comes from wafv2ARN, which the WAFv2 plugin also uses. Fixing the segment here alone
// left the plugin still hardcoding "regional", so the same logical web ACL reported two
// different ARNs depending on which path created it; one builder is what stops that recurring.
func (d *StackDeployer) deployWAFv2WebACL(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	scope := resolveStringProp(props, "Scope", "REGIONAL", cctx)
	arn := wafv2ARN(cctx.region, cctx.accountID, scope, "webacl", name, logicalID)
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployWAFv2WebACL stubStore: %w", err)
	}
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::WAFv2::WebACL",
		PhysicalID: logicalID,
		ARN:        arn,
		Metadata: map[string]interface{}{
			"Name":  name,
			"Scope": scope,
		},
	}, 0, nil
}

// deployBackupBackupPlan deploys an AWS::Backup::BackupPlan as a stub in cfnStubNamespace.
//
// AWS publishes "Ref returns BackupPlanId" and three Fn::GetAtt attributes, BackupPlanArn,
// BackupPlanId and VersionId (#1182). This deploy used to answer the logical ID for Ref and record
// no attribute, under a comment claiming the Ref was the plan ID. Now:
//   - The plan ID is a UUID derived from the account, Region, stack and logical ID, so an
//     UpdateStack keeps the ID a sibling AWS::Backup::BackupSelection already holds. It is the
//     physical ID, so Ref answers it.
//   - VersionId is derived from the same scope plus the declared properties, so it changes when
//     the plan's declaration changes and stays the same on an unchanged redeploy.
//   - BackupPlanArn is the ARN, resolved by the ARN-suffix rule. Its resource segment is
//     backup-plan, the one the Backup plugin also mints; #1181 tracks AWS publishing plan.
//
// BackupPlan is Required: Yes, so a template without it fails the resource rather than deploying
// a plan with no rules.
//
// It stays a stub, so GetBackupPlan and ListBackupPlans do not see a deployed plan. That is a
// decision, recorded here because #1182 asked for one: CreateBackupPlan's route is being moved
// from POST to PUT in the same release (#1172), and AWS::Backup::BackupSelection, the one consumer
// of this Ref, is not deployed either. Dispatching CreateBackupPlan belongs with deploying the
// selection type, after the route settles.
func (d *StackDeployer) deployBackupBackupPlan(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Backup::BackupPlan"}
	if props["BackupPlan"] == nil {
		dr.Error = "BackupPlan is a required property of AWS::Backup::BackupPlan"
		return dr, 0, nil
	}
	declared, err := json.Marshal(props)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployBackupBackupPlan marshal: %w", err)
	}
	scope := cctx.accountID + "/" + cctx.region + "/" + cctx.stackName + "/" + logicalID
	planID := cfnDerivedUUID(scope)
	versionID := cfnDerivedUUID(scope + "/" + string(declared))
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployBackupBackupPlan stubStore: %w", err)
	}
	dr.PhysicalID = planID
	dr.ARN = fmt.Sprintf("arn:aws:backup:%s:%s:backup-plan:%s", cctx.region, cctx.accountID, planID)
	dr.Metadata = map[string]interface{}{
		"BackupPlanId": planID,
		"VersionId":    versionID,
	}
	return dr, 0, nil
}

// cfnDerivedUUID renders a UUID-shaped identifier (8-4-4-4-12 lowercase hex, the shape
// [IDMint.HexUUID] renders) derived from seed by SHA-256.
//
// For an identifier a stub deploy invents and must keep across an UpdateStack, which re-deploys
// every resource: a minted one would change on each update.
func cfnDerivedUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	h := hex.EncodeToString(sum[:16])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// deployCodeBuildProject creates a CodeBuild project stub.
// The Ref value is the project name.
func (d *StackDeployer) deployCodeBuildProject(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:codebuild:%s:%s:project/%s", cctx.region, cctx.accountID, name)
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCodeBuildProject stubStore: %w", err)
	}
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CodeBuild::Project",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployCodePipelinePipeline creates a CodePipeline pipeline stub.
// The Ref value is the pipeline name.
func (d *StackDeployer) deployCodePipelinePipeline(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// Pipeline name may be nested inside a PipelineConfig or as a top-level Name.
	name := resolveStringProp(props, "Name", logicalID, cctx)
	// CodePipeline ARN format has no resource-type prefix.
	arn := fmt.Sprintf("arn:aws:codepipeline:%s:%s:%s", cctx.region, cctx.accountID, name)
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCodePipelinePipeline stubStore: %w", err)
	}
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CodePipeline::Pipeline",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployCodeDeployApplication deploys an AWS::CodeDeploy::Application through the CodeDeploy
// plugin's CreateApplication. Ref is the application name.
//
// It was not handled at all, so it fell through to the generic stub, and the plugin never saw it.
// That mattered once the deployment group deploys for real (#1203): CreateDeploymentGroup refuses
// an application that does not exist, so a template declaring both would fail. ApplicationName is
// Required: No and falls back to the logical ID, as the other named types here do. Tags are not
// sent, because the plugin's CreateApplication does not read them.
func (d *StackDeployer) deployCodeDeployApplication(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "ApplicationName", logicalID, cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::CodeDeploy::Application"}
	body := map[string]interface{}{"applicationName": name}
	if cp := resolveStringProp(props, "ComputePlatform", "", cctx); cp != "" {
		body["computePlatform"] = cp
	}
	resp, cost, err := d.dispatchCodeDeploy(ctx, "CreateApplication", body, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, 0, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	if resp == nil {
		return dr, cost, nil
	}
	dr.PhysicalID = name
	dr.ARN = fmt.Sprintf("arn:aws:codedeploy:%s:%s:application:%s", cctx.region, cctx.accountID, name)
	return dr, cost, nil
}

// deployCodeDeployDeploymentGroup deploys an AWS::CodeDeploy::DeploymentGroup through the CodeDeploy
// plugin's CreateDeploymentGroup. Ref is the deployment group name.
//
// It was a stub in cfnStubNamespace, so GetDeploymentGroup on the name a stack exported answered
// DeploymentGroupDoesNotExistException (#1203). Now the plugin owns the group.
//
// A template's property names are PascalCase and the API's members are lowerCamel, nested members
// included, so each property is renamed by cfnCodeDeployKeys before it is sent. Two properties
// differ in shape as well as in case:
//   - Ec2TagSet's Ec2TagSetList is a list of {Ec2TagGroup: [filter…]} objects, where the API's
//     ec2TagSetList is a list of filter lists.
//   - OnPremisesTagSet's OnPremisesTagSetList is the same, with OnPremisesTagGroup.
//
// Two properties are not sent:
//   - Deployment asks CloudFormation to run a deployment once the group exists. That would be a
//     CreateDeployment, and substrate does not issue it from a stack.
//   - Tags is not read by the plugin's CreateDeploymentGroup.
//
// The namespace's third type, AWS::CodeDeploy::DeploymentConfig, is declined: the plugin routes no
// CreateDeploymentConfig, so the type falls through to the generic stub, and its Ref is the logical
// ID. The group accepts that as a deploymentConfigName, because the plugin does not look configs up.
// AWS::CodeDeploy::Application is deployed, by deployCodeDeployApplication.
func (d *StackDeployer) deployCodeDeployDeploymentGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	appName := resolveStringProp(props, "ApplicationName", "", cctx)
	name := resolveStringProp(props, "DeploymentGroupName", logicalID, cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::CodeDeploy::DeploymentGroup"}

	body := map[string]interface{}{
		"applicationName":     appName,
		"deploymentGroupName": name,
	}
	for key, v := range props {
		switch key {
		case "ApplicationName", "DeploymentGroupName", "Deployment", "Tags":
			continue
		case "Ec2TagSet":
			body["ec2TagSet"] = cfnCodeDeployTagSet(resolveNested(v, cctx), "Ec2TagSetList", "Ec2TagGroup")
		case "OnPremisesTagSet":
			body["onPremisesTagSet"] = cfnCodeDeployTagSet(resolveNested(v, cctx), "OnPremisesTagSetList", "OnPremisesTagGroup")
		default:
			body[cfnLowerCamel(key)] = cfnCodeDeployKeys(resolveNested(v, cctx))
		}
	}
	resp, cost, err := d.dispatchCodeDeploy(ctx, "CreateDeploymentGroup", body, streamID)
	if err != nil {
		dr.Error = err.Error()
		return dr, 0, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	if resp == nil {
		return dr, cost, nil
	}
	dr.PhysicalID = name
	dr.ARN = fmt.Sprintf("arn:aws:codedeploy:%s:%s:deploymentgroup:%s/%s", cctx.region, cctx.accountID, appName, name)
	return dr, cost, nil
}

// dispatchCodeDeploy sends one CodeDeploy JSON 1.1 operation through the registry.
func (d *StackDeployer) dispatchCodeDeploy(
	ctx context.Context, operation string, body map[string]interface{}, streamID string,
) (*AWSResponse, float64, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("cfn codedeploy %s marshal: %w", operation, err)
	}
	req := &AWSRequest{
		Service:   "codedeploy",
		Operation: operation,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
		Body:      data,
	}
	return d.dispatch(ctx, req, streamID)
}

// cfnCodeDeployKeys renames every map key in v from a template's PascalCase to the CodeDeploy API's
// lowerCamel, at every depth. Values are not changed.
func cfnCodeDeployKeys(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, item := range val {
			out[cfnLowerCamel(k)] = cfnCodeDeployKeys(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			out[i] = cfnCodeDeployKeys(item)
		}
		return out
	}
	return v
}

// cfnCodeDeployTagSet converts a template's tag set, {listKey: [{groupKey: [filter…]}…]}, to the
// API's EC2TagSet or OnPremisesTagSet, {lowerCamel(listKey): [[filter…]…]}.
func cfnCodeDeployTagSet(v interface{}, listKey, groupKey string) interface{} {
	m, ok := v.(map[string]interface{})
	if !ok {
		return cfnCodeDeployKeys(v)
	}
	groups, _ := m[listKey].([]interface{})
	lists := make([]interface{}, 0, len(groups))
	for _, g := range groups {
		gm, ok := g.(map[string]interface{})
		if !ok {
			continue
		}
		filters, _ := gm[groupKey].([]interface{})
		lists = append(lists, cfnCodeDeployKeys(filters))
	}
	return map[string]interface{}{cfnLowerCamel(listKey): lists}
}

// cfnLowerCamel lowercases a PascalCase name's leading capital, or its leading acronym:
// ApplicationName becomes applicationName, ECSServices ecsServices, and Ec2TagFilters
// ec2TagFilters. In a run of capitals followed by a lowercase letter, the last capital begins the
// next word and is kept.
func cfnLowerCamel(s string) string {
	n := 0
	for n < len(s) && s[n] >= 'A' && s[n] <= 'Z' {
		n++
	}
	switch {
	case n == 0:
		return s
	case n == len(s):
		return strings.ToLower(s)
	case n > 1:
		n--
	}
	return strings.ToLower(s[:n]) + s[n:]
}

// deployCloudTrailTrail creates a CloudTrail trail stub.
//
// Ref returns the trail's resource name, not its ARN, so the name is recorded rather than cut
// back out of the ARN at resolve time (#827).
func (d *StackDeployer) deployCloudTrailTrail(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "TrailName", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:cloudtrail:%s:%s:trail/%s", cctx.region, cctx.accountID, name)
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCloudTrailTrail stubStore: %w", err)
	}
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudTrail::Trail",
		PhysicalID: arn,
		ARN:        arn,
		Metadata:   map[string]interface{}{"TrailName": name},
	}, 0, nil
}

// The AWS::Config::ConfigRule and AWS::Config::ConfigurationRecorder stubs that
// shipped here in v0.32.0 are gone: they now dispatch real Config operations from
// cfn_resources_v101.go, which became possible once the service's handlers existed
// (#580).

// deployTransferServer deploys an AWS::Transfer::Server through the Transfer plugin's CreateServer.
//
// The Ref value is the server **ARN**, not the server ID (#1234 corrected this comment, which had
// said the ID and was the opposite of both the page and the tree). AWS::Transfer::Server's Return
// values section publishes "Ref returns the server ARN, such as
// arn:aws:transfer:us-east-1:123456789012:server/s-01234567890abcdef", and offers the ID only as the
// ServerId Fn::GetAtt attribute. cfn_intrinsics.go resolves Ref from the ARN.
//
// It was a stub whose server ID was "s-" plus the lowercased logical ID, a value neither the
// published s-([0-9a-f]{17}) pattern nor DescribeServer accepts (#1203). Now the plugin mints the
// ID, and the template's properties are sent as CreateServer's members, which have the same names.
// The physical ID is the server ID, which is what DeleteServer takes.
//
// Fn::GetAtt answers:
//   - Arn, by the ARN-suffix rule;
//   - ServerId, recorded here;
//   - State, recorded as ONLINE. That is the state CreateServer's record moves to, and CloudFormation
//     reports a server created once it is available. A seeded start progression is observed through
//     DescribeServer, not through an attribute resolved once at deploy time. This is substrate's
//     reading.
//
// As2ServiceManagedEgressIpAddresses is refused (see cfnGetAttUnmodelled): the plugin assigns no
// AS2 egress addresses.
func (d *StackDeployer) deployTransferServer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Transfer::Server"}
	body := map[string]interface{}{}
	for key, v := range props {
		body[key] = resolveNested(v, cctx)
	}
	data, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployTransferServer marshal: %w", err)
	}
	req := &AWSRequest{
		Service:   "transfer",
		Operation: "CreateServer",
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
		Body:      data,
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, 0, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	if resp == nil {
		return dr, cost, nil
	}
	var out struct {
		ServerID string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployTransferServer unmarshal: %w", err)
	}
	dr.PhysicalID = out.ServerID
	dr.ARN = fmt.Sprintf("arn:aws:transfer:%s:%s:server/%s", cctx.region, cctx.accountID, out.ServerID)
	dr.Metadata = map[string]interface{}{
		"ServerId": out.ServerID,
		"State":    "ONLINE",
	}
	return dr, cost, nil
}

// deployAthenaWorkGroup creates an Athena WorkGroup stub.
// The Ref value is the workgroup name.
func (d *StackDeployer) deployAthenaWorkGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", cctx.region, cctx.accountID, name)
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployAthenaWorkGroup stubStore: %w", err)
	}
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Athena::WorkGroup",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// cfnGetAttUnmodelled maps a resource type to the published Fn::GetAtt attributes substrate holds
// no value for, each with the reason.
//
// An attribute listed here fails the resource that reads it, rather than resolving to an empty
// string (#1203). An empty string for a published attribute is the failure mode to remove: a
// template that exports it hands a consumer an empty host or ID, and the error surfaces two layers
// away from its cause. A refusal names the attribute instead.
//
// The refusal is through [cfnContext.fail], so it reaches a resource property. An Output that reads
// one of these still answers an empty string, because an Output's resolution failures are not
// attributable to a resource and are dropped (see deployResource).
var cfnGetAttUnmodelled = map[string]map[string]string{
	"AWS::Transfer::Server": {
		"As2ServiceManagedEgressIpAddresses": "the Transfer plugin assigns no AS2 egress addresses",
	},
	"AWS::OpenSearchService::Domain": {
		"DomainEndpointV2": "the dual-stack endpoint is not modeled",
		"AdvancedSecurityOptions.AnonymousAuthDisableDate":   "fine-grained access control is not modeled",
		"IdentityCenterOptions.IdentityCenterApplicationARN": "IAM Identity Center integration is not modeled",
		"IdentityCenterOptions.IdentityStoreId":              "IAM Identity Center integration is not modeled",
	},
	"AWS::FSx::FileSystem": {
		"RootVolumeId": "OpenZFS volumes are not modeled",
	},
}

// cfnGetAttRefuseUnmodelled records a resolution failure when attr is one of dr's published
// attributes listed in cfnGetAttUnmodelled. A resource whose create failed is not refused again.
func cfnGetAttRefuseUnmodelled(dr DeployedResource, attr string, cctx *cfnContext) {
	if cctx == nil || dr.Error != "" {
		return
	}
	if reason, ok := cfnGetAttUnmodelled[dr.Type][attr]; ok {
		cctx.fail("Fn::GetAtt: %s.%s (%s) has no value in substrate: %s", dr.LogicalID, attr, dr.Type, reason)
	}
}
