package emulator

// cfn_resources_v128.go holds the StackDeployer helpers v0.128.0 added: AWS Backup's two
// resource types, which now dispatch the Backup API (#1428), and the in-place updates
// cfnResourceUpdaters names (#1427, #1508). The name records the release rather than the
// services, as the other cfn_resources_v*.go files do.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// ----- AWS Backup (#1428) ---------------------------------------------------------

// cfnBackupPlanBody renders an AWS::Backup::BackupPlan's BackupPlan property as the BackupPlan
// member CreateBackupPlan and UpdateBackupPlan both take, with intrinsics resolved.
//
// The template's BackupPlanResourceType and the API's BackupPlanInput spell two members
// differently, and only those two are rewritten: the rule list is BackupPlanRule in the template
// and Rules in the API, and a rule's vault is TargetBackupVault in the template and
// TargetBackupVaultName in the API (the BackupRuleResourceType and BackupRuleInput pages). Every
// other member — RuleName, ScheduleExpression, the window minutes, Lifecycle, CopyActions,
// RecoveryPointTags and the rest — has the same name on both pages and is passed as declared.
func cfnBackupPlanBody(props map[string]interface{}, cctx *cfnContext) (map[string]interface{}, bool) {
	plan, ok := resolveNested(props["BackupPlan"], cctx).(map[string]interface{})
	if !ok {
		return nil, false
	}
	out := make(map[string]interface{}, len(plan))
	for k, v := range plan {
		if k == "BackupPlanRule" {
			continue
		}
		out[k] = v
	}
	rules, _ := plan["BackupPlanRule"].([]interface{})
	apiRules := make([]interface{}, 0, len(rules))
	for _, item := range rules {
		rule, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		apiRule := make(map[string]interface{}, len(rule))
		for k, v := range rule {
			if k == "TargetBackupVault" {
				apiRule["TargetBackupVaultName"] = v
				continue
			}
			apiRule[k] = v
		}
		apiRules = append(apiRules, apiRule)
	}
	out["Rules"] = apiRules
	return out, true
}

// deployBackupBackupPlan deploys an AWS::Backup::BackupPlan through CreateBackupPlan, so
// GetBackupPlan and ListBackupPlans report the stack's plan (#1428).
//
// It was a stub in cfnStubNamespace whose plan ID was derived from the stack and logical ID, which
// was what kept the ID stable across an UpdateStack when every update re-created every resource.
// The plan ID is now the Backup plugin's own, minted through the request's IDMint like every other
// identifier the plugin hands out, and it survives an update because the update no longer creates
// the plan again: an unchanged plan is kept, and a changed one goes through UpdateBackupPlan (see
// updateBackupBackupPlan), whose plan ID is the one in the path.
//
// #1182's shapes are kept. "Ref returns BackupPlanId", so the physical ID is the plan ID; BackupPlanId
// and VersionId are recorded from CreateBackupPlan's response, and BackupPlanArn is the ARN the
// response carries, resolved by the ARN-suffix rule. BackupPlan is Required: Yes, so a template
// without it fails the resource rather than creating a plan with no rules.
//
// BackupPlanTags is sent as CreateBackupPlan's member of the same name. The plugin stores no plan
// tags, so it records nothing beyond the request.
func (d *StackDeployer) deployBackupBackupPlan(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Backup::BackupPlan"}
	plan, ok := cfnBackupPlanBody(props, cctx)
	if !ok {
		dr.Error = "BackupPlan is a required property of AWS::Backup::BackupPlan"
		return dr, 0, nil
	}
	body := map[string]interface{}{"BackupPlan": plan}
	if tags, ok := resolveNested(props["BackupPlanTags"], cctx).(map[string]interface{}); ok && len(tags) > 0 {
		body["BackupPlanTags"] = tags
	}
	data, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployBackupBackupPlan marshal: %w", err)
	}
	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "backup", Operation: "PUT", Path: "/backup/plans/", Body: data,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	}, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	if resp == nil {
		return dr, cost, nil
	}
	var out struct {
		BackupPlanID  string `json:"BackupPlanId"`
		BackupPlanArn string `json:"BackupPlanArn"`
		VersionID     string `json:"VersionId"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployBackupBackupPlan unmarshal: %w", err)
	}
	dr.PhysicalID = out.BackupPlanID
	dr.ARN = out.BackupPlanArn
	dr.Metadata = map[string]interface{}{
		"BackupPlanId": out.BackupPlanID,
		"VersionId":    out.VersionID,
	}
	return dr, cost, nil
}

// updateBackupBackupPlan applies a changed AWS::Backup::BackupPlan through UpdateBackupPlan, whose
// path names the plan, so the plan keeps its ID and every selection naming it stays attached.
// BackupPlan's "Update requires" is No interruption.
//
// UpdateBackupPlan answers a new VersionId, which is recorded: VersionId is the attribute that
// changes when a plan does. A change to BackupPlanTags alone makes no call — tagging a plan is
// TagResource, which substrate does not route for Backup — and the plan keeps its record.
func (d *StackDeployer) updateBackupBackupPlan(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	if !slices.Contains(changed, "BackupPlan") {
		return dr, 0, nil
	}
	plan, ok := cfnBackupPlanBody(props, cctx)
	if !ok {
		dr.Error = "BackupPlan is a required property of AWS::Backup::BackupPlan"
		return dr, 0, nil
	}
	data, err := json.Marshal(map[string]interface{}{"BackupPlan": plan})
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn updateBackupBackupPlan marshal: %w", err)
	}
	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "backup", Operation: "POST", Path: "/backup/plans/" + prior.PhysicalID, Body: data,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	}, streamID)
	if routeErr != nil {
		if cfnDeleteIsAbsent(routeErr) {
			// A stack recorded before #1428 holds a plan ID its stub deploy derived, which no
			// Backup plan carries. The plan is created instead, under a new ID, and the update
			// deletes the record it replaced — which the plugin reports absent.
			return d.deployBackupBackupPlan(ctx, logicalID, props, streamID, cctx)
		}
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
	}
	if resp == nil {
		return dr, cost, nil
	}
	var out struct {
		VersionID string `json:"VersionId"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn updateBackupBackupPlan unmarshal: %w", err)
	}
	dr.Metadata = cfnCopyMetadata(prior.Metadata)
	dr.Metadata["VersionId"] = out.VersionID
	return dr, cost, nil
}

// deployBackupBackupSelection deploys an AWS::Backup::BackupSelection through CreateBackupSelection
// (#1428), which had no deploy at all: a stack's selection fell through to the generic stub and
// assigned nothing to the plan.
//
// The template's BackupSelectionResourceType and the API's BackupSelection have the same members —
// SelectionName, IamRoleArn, Resources, NotResources, ListOfTags and Conditions — so the property
// is sent as declared, with intrinsics resolved. The plan is BackupPlanId, conventionally a Ref to
// the plan, which is also what orders the selection after its plan and deletes it first.
//
// "Ref returns BackupSelectionId", which is the SelectionId CreateBackupSelection answers, so that
// is the physical ID. Of the three published attributes, BackupPlanId and SelectionId are the
// response's own members. Id, "uniquely identifies the backup selection", is recorded as the same
// selection ID: the API has no second identifier for a selection to give it, so the value is
// substrate's reading of the page. Both properties are Update requires: Replacement, so a change
// creates a new selection and the old one is deleted once the update succeeds.
func (d *StackDeployer) deployBackupBackupSelection(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Backup::BackupSelection"}
	planID := resolveStringProp(props, "BackupPlanId", "", cctx)
	if planID == "" {
		dr.Error = "BackupPlanId is a required property of AWS::Backup::BackupSelection"
		return dr, 0, nil
	}
	selection, ok := resolveNested(props["BackupSelection"], cctx).(map[string]interface{})
	if !ok {
		dr.Error = "BackupSelection is a required property of AWS::Backup::BackupSelection"
		return dr, 0, nil
	}
	data, err := json.Marshal(map[string]interface{}{"BackupSelection": selection})
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployBackupBackupSelection marshal: %w", err)
	}
	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "backup", Operation: "PUT", Path: "/backup/plans/" + planID + "/selections/", Body: data,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	}, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	if resp == nil {
		return dr, cost, nil
	}
	var out struct {
		SelectionID  string `json:"SelectionId"`
		BackupPlanID string `json:"BackupPlanId"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployBackupBackupSelection unmarshal: %w", err)
	}
	dr.PhysicalID = out.SelectionID
	dr.Metadata = map[string]interface{}{
		"BackupPlanId": out.BackupPlanID,
		"SelectionId":  out.SelectionID,
		"Id":           out.SelectionID,
	}
	return dr, cost, nil
}

// backupSelectionDeleter builds DeleteBackupSelection, whose path names the plan as well as the
// selection. The plan is the one CreateBackupSelection answered, recorded on the resource, so the
// delete does not depend on re-resolving the template.
func backupSelectionDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	planID := cfnRefMetadata(dr, "BackupPlanId")
	if planID == "" || dr.PhysicalID == "" {
		return nil
	}
	return &AWSRequest{Service: "backup", Operation: "DELETE",
		Path:    "/backup/plans/" + planID + "/selections/" + dr.PhysicalID,
		Headers: map[string]string{}, Params: map[string]string{}}
}

// ----- In-place updates (#1427, #1508) ----------------------------------------------

// cfnTransferUpdateExcluded are the AWS::Transfer::Server properties UpdateServer does not take.
// Domain is Update requires: Replacement, so a change to it never reaches the update. Tags is a
// TagResource call rather than an UpdateServer member, and TagResource is not routed for Transfer.
// CreateWithHostKey "can be set only when you create the server".
var cfnTransferUpdateExcluded = map[string]bool{"Domain": true, "Tags": true, "CreateWithHostKey": true}

// updateTransferServer applies a changed AWS::Transfer::Server through UpdateServer, keeping the
// server's ID (#1427). Every property but Domain is updatable in place by the resource's page, and
// UpdateServer's members carry the same names as the template's properties, so the declared
// properties are sent as they are for the create, minus the three UpdateServer does not take.
//
// A property removed from the template is not reset: UpdateServer leaves an omitted member as it is,
// and substrate has no record of the value the server was created without it. A change to Tags alone
// makes no call, for the reason [cfnTransferUpdateExcluded] gives.
func (d *StackDeployer) updateTransferServer(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	applies := false
	for _, prop := range changed {
		if !cfnTransferUpdateExcluded[prop] {
			applies = true
		}
	}
	if !applies {
		return dr, 0, nil
	}
	body := map[string]interface{}{"ServerId": prior.PhysicalID}
	for key, v := range props {
		if !cfnTransferUpdateExcluded[key] {
			body[key] = resolveNested(v, cctx)
		}
	}
	data, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn updateTransferServer marshal: %w", err)
	}
	_, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "transfer", Operation: "UpdateServer",
		Headers: map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{}, Body: data,
	}, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
	}
	return dr, cost, nil
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

// cfnCopyMetadata returns a copy of m that an update can change without altering the record it
// was read from, which the rollback still compares against.
func cfnCopyMetadata(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}
