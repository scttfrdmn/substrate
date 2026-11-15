package emulator_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1428: a stack's AWS::Backup::BackupPlan is the Backup plugin's, and its
// AWS::Backup::BackupSelection is deployed through CreateBackupSelection.

// cfnBackupTemplate is a plan and a selection naming it by Ref, with the plan's rule and the
// selection's resource list as the two values a test varies.
func cfnBackupTemplate(schedule, resource string) string {
	return `{"Resources":{
		"Plan":{"Type":"AWS::Backup::BackupPlan","Properties":{"BackupPlan":{
			"BackupPlanName":"nightly","BackupPlanRule":[{"RuleName":"nightly-rule",
				"TargetBackupVault":"Default","ScheduleExpression":"` + schedule + `",
				"Lifecycle":{"DeleteAfterDays":30}}]}}},
		"Selection":{"Type":"AWS::Backup::BackupSelection","Properties":{
			"BackupPlanId":{"Ref":"Plan"},
			"BackupSelection":{"SelectionName":"tables",
				"IamRoleArn":"arn:aws:iam::123456789012:role/backup",
				"Resources":["` + resource + `"]}}}},
		"Outputs":{
			"PlanRef":{"Value":{"Ref":"Plan"}},
			"VersionId":{"Value":{"Fn::GetAtt":["Plan","VersionId"]}},
			"PlanArn":{"Value":{"Fn::GetAtt":["Plan","BackupPlanArn"]}},
			"SelectionRef":{"Value":{"Ref":"Selection"}},
			"SelectionId":{"Value":{"Fn::GetAtt":["Selection","SelectionId"]}},
			"SelectionPlan":{"Value":{"Fn::GetAtt":["Selection","BackupPlanId"]}},
			"SelectionAttrId":{"Value":{"Fn::GetAtt":["Selection","Id"]}}}}`
}

// backupGet sends a GET to the Backup plugin and decodes the answer, or returns the refusal.
func backupGet(t *testing.T, d *emulator.StackDeployer, path string) (map[string]any, error) {
	t.Helper()
	resp, err := cfnRawDispatch(d, &emulator.AWSRequest{Service: "backup", Operation: "GET", Path: path})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return out, nil
}

// TestCFN_BackupPlanAndSelectionReachTheBackupPlugin pins both acceptance criteria end to end: the
// plan and the selection are visible to GetBackupPlan and ListBackupSelections with the template's
// values, #1182's Ref and attribute shapes hold, both keep their IDs across an unchanged update, a
// changed plan goes through UpdateBackupPlan under the same ID, and a stack delete removes the
// selection before the plan.
func TestCFN_BackupPlanAndSelectionReachTheBackupPlugin(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()
	const stack = "backup-real"

	created, err := d.Deploy(ctx, cfnBackupTemplate("cron(0 5 ? * * *)", "arn:aws:dynamodb:us-east-1:123456789012:table/a"), stack, nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	planID := created.Outputs["PlanRef"]
	selectionID := created.Outputs["SelectionRef"]
	require.NotEmpty(t, planID)
	require.NotEmpty(t, selectionID)

	// #1182's shapes, now from the plugin's own response.
	assert.Equal(t, "arn:aws:backup:us-east-1:123456789012:backup-plan:"+planID, created.Outputs["PlanArn"])
	assert.NotEmpty(t, created.Outputs["VersionId"])
	assert.Equal(t, selectionID, created.Outputs["SelectionId"], "Ref returns BackupSelectionId")
	assert.Equal(t, selectionID, created.Outputs["SelectionAttrId"])
	assert.Equal(t, planID, created.Outputs["SelectionPlan"])

	plan, err := backupGet(t, d, "/backup/plans/"+planID)
	require.NoError(t, err, "GetBackupPlan sees the stack's plan")
	body, _ := plan["BackupPlan"].(map[string]any)
	assert.Equal(t, "nightly", body["BackupPlanName"])
	rules, _ := body["Rules"].([]any)
	require.Len(t, rules, 1)
	rule, _ := rules[0].(map[string]any)
	assert.Equal(t, "Default", rule["TargetBackupVaultName"], "TargetBackupVault is the API's TargetBackupVaultName")
	assert.Equal(t, created.Outputs["VersionId"], plan["VersionId"])

	selection, err := backupGet(t, d, "/backup/plans/"+planID+"/selections/"+selectionID)
	require.NoError(t, err, "GetBackupSelection sees the stack's selection")
	sel, _ := selection["BackupSelection"].(map[string]any)
	assert.Equal(t, "tables", sel["SelectionName"])
	assert.Equal(t, []any{"arn:aws:dynamodb:us-east-1:123456789012:table/a"}, sel["Resources"])

	// Unchanged: both IDs and the version stand, and nothing is created.
	before := len(sweepOperations(t, store, stack))
	same, err := d.UpdateStack(ctx, cfnBackupTemplate("cron(0 5 ? * * *)", "arn:aws:dynamodb:us-east-1:123456789012:table/a"),
		stack, nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", same.Status)
	assert.Equal(t, planID, same.Outputs["PlanRef"])
	assert.Equal(t, selectionID, same.Outputs["SelectionRef"])
	assert.Equal(t, created.Outputs["VersionId"], same.Outputs["VersionId"])
	assert.Len(t, sweepOperations(t, store, stack), before, "an unchanged update makes no call")

	// The plan changes: UpdateBackupPlan, the same plan ID, a new version, and the selection —
	// whose Ref to the plan resolves to the same ID — is left alone.
	changed, err := d.UpdateStack(ctx, cfnBackupTemplate("cron(0 9 ? * * *)", "arn:aws:dynamodb:us-east-1:123456789012:table/a"),
		stack, nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", changed.Status, changed.StatusReason)
	assert.Equal(t, planID, changed.Outputs["PlanRef"], "the plan ID survives a changed plan")
	assert.NotEqual(t, created.Outputs["VersionId"], changed.Outputs["VersionId"])
	assert.Equal(t, selectionID, changed.Outputs["SelectionRef"])
	ops := sweepOperations(t, store, stack)[before:]
	assert.Equal(t, []string{"UpdateBackupPlan"}, ops)
	plan, err = backupGet(t, d, "/backup/plans/"+planID)
	require.NoError(t, err)
	assert.Equal(t, changed.Outputs["VersionId"], plan["VersionId"])

	// The selection changes, and BackupSelection is Update requires: Replacement.
	replaced, err := d.UpdateStack(ctx, cfnBackupTemplate("cron(0 9 ? * * *)", "arn:aws:dynamodb:us-east-1:123456789012:table/b"),
		stack, nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", replaced.Status, replaced.StatusReason)
	newSelection := replaced.Outputs["SelectionRef"]
	assert.NotEqual(t, selectionID, newSelection)
	_, err = backupGet(t, d, "/backup/plans/"+planID+"/selections/"+selectionID)
	require.Error(t, err, "the replaced selection is deleted")
	list, err := backupGet(t, d, "/backup/plans/"+planID+"/selections/")
	require.NoError(t, err)
	members, _ := list["BackupSelectionsList"].([]any)
	assert.Len(t, members, 1)

	// Delete: the selection goes first, or DeleteBackupPlan would refuse (#1178).
	before = len(sweepOperations(t, store, stack))
	require.NoError(t, d.DeleteStack(ctx, stack))
	assert.Equal(t, []string{"DeleteBackupSelection", "DeleteBackupPlan"}, sweepOperations(t, store, stack)[before:])
	_, err = backupGet(t, d, "/backup/plans/"+planID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}
