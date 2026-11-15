package emulator_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A backup plan's ARN uses the published plan segment (#1181): API_CreateBackupPlan's example is
// arn:aws:backup:us-east-1:123456789012:plan:8F81F553-…, where substrate spelled it backup-plan. A plan
// or a stack record stored with the old segment reports the published one, because the ARN is derived
// on read.
func TestBackupPlanARN_StoredRecordsReportThePublishedSegment(t *testing.T) {
	d, state, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const stack = "backup-arn"
	template := cfnBackupTemplate("cron(0 5 ? * * *)", "arn:aws:dynamodb:us-east-1:123456789012:table/a")
	created, err := d.Deploy(ctx, template, stack, nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	planID := created.Outputs["PlanRef"]
	want := "arn:aws:backup:us-east-1:123456789012:plan:" + planID
	require.Equal(t, want, created.Outputs["PlanArn"], "a deployed plan reports the published ARN")

	// Rewrite the stored plan and the stack record into the pre-#1181 shape, as a state store written
	// by an older substrate holds them.
	legacy := "arn:aws:backup:us-east-1:123456789012:backup-plan:" + planID
	for _, ns := range []string{"backup", "cfn"} {
		keys, err := state.List(ctx, ns, "")
		require.NoError(t, err)
		for _, k := range keys {
			data, err := state.Get(ctx, ns, k)
			require.NoError(t, err)
			if bytes.Contains(data, []byte(want)) {
				require.NoError(t, state.Put(ctx, ns, k, bytes.ReplaceAll(data, []byte(want), []byte(legacy))))
			}
		}
	}

	plan, err := backupGet(t, d, "/backup/plans/"+planID)
	require.NoError(t, err)
	assert.Equal(t, want, plan["BackupPlanArn"], "GetBackupPlan derives the ARN rather than reading it back")
	list, err := backupGet(t, d, "/backup/plans")
	require.NoError(t, err)
	assert.Contains(t, list["BackupPlansList"].([]any)[0].(map[string]any)["BackupPlanArn"], ":plan:")

	// The stored stack record reports the published ARN on its plan resource and in the output that
	// resolved Fn::GetAtt BackupPlanArn, without being redeployed.
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	assert.Equal(t, want, stacks[0].Outputs["PlanArn"], "a recorded output is rewritten too")
	for _, r := range stacks[0].Resources {
		if r.Type == "AWS::Backup::BackupPlan" {
			assert.Equal(t, want, r.ARN)
		}
	}

	// An unchanged update carries the recorded resource forward, so its attribute comes from the stack
	// record.
	updated, err := d.Deploy(ctx, template, stack, nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, updated)
	assert.Equal(t, want, updated.Outputs["PlanArn"], "the stack record reports the published ARN")
	assert.False(t, strings.Contains(updated.Outputs["PlanArn"], "backup-plan"))
}
