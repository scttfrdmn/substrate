package emulator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// cfnGetDashboard reads a dashboard through the deployer's registry over the query protocol.
func cfnGetDashboard(t *testing.T, d *emulator.StackDeployer, name string) (string, error) {
	t.Helper()
	return v129Query(t, d, "monitoring", map[string]string{"Action": "GetDashboard", "DashboardName": name})
}

// TestCFNCloudWatchDashboard_DeployUpdateDelete covers AWS::CloudWatch::Dashboard's create, its
// in-place body update, a generated name, and the stack delete.
func TestCFNCloudWatchDashboard_DeployUpdateDelete(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const before = `{"Resources":{
		"Named":{"Type":"AWS::CloudWatch::Dashboard","Properties":{"DashboardName":"cfn-dash","DashboardBody":"{\"widgets\":[]}"}},
		"Unnamed":{"Type":"AWS::CloudWatch::Dashboard","Properties":{"DashboardBody":"{}"}}}}`
	const after = `{"Resources":{
		"Named":{"Type":"AWS::CloudWatch::Dashboard","Properties":{"DashboardName":"cfn-dash","DashboardBody":"{\"widgets\":[{\"type\":\"text\"}]}"}},
		"Unnamed":{"Type":"AWS::CloudWatch::Dashboard","Properties":{"DashboardBody":"{}"}}}}`

	created, updated := v129Update(t, d, "dash", before, after)
	named := resourceByLogicalID(t, updated, "Named")
	assert.Equal(t, "cfn-dash", named.PhysicalID)
	assert.Equal(t, resourceByLogicalID(t, created, "Named").PhysicalID, named.PhysicalID, "a body change is applied in place")
	assert.True(t, strings.HasSuffix(named.ARN, ":dashboard/cfn-dash"), named.ARN)

	body, err := cfnGetDashboard(t, d, "cfn-dash")
	require.NoError(t, err)
	assert.Contains(t, body, `{&#34;type&#34;:&#34;text&#34;}`, "the updated body is what GetDashboard answers: %s", body)

	unnamed := resourceByLogicalID(t, updated, "Unnamed").PhysicalID
	assert.NotEqual(t, "Unnamed", unnamed, "a dashboard without a name gets a generated one")
	_, err = cfnGetDashboard(t, d, unnamed)
	require.NoError(t, err)

	// A bad body fails the resource with CloudWatch's own refusal.
	bad, err := d.Deploy(ctx, `{"Resources":{"Bad":{"Type":"AWS::CloudWatch::Dashboard","Properties":{
		"DashboardName":"cfn-bad","DashboardBody":"not json"}}}}`, "dash-bad", nil)
	require.NoError(t, err)
	assert.Contains(t, resourceByLogicalID(t, bad, "Bad").Error, "InvalidParameterInput")

	for _, del := range d.DeleteStackResourcesForTest(ctx, "dash") {
		assert.Empty(t, del.Reason, "%s: %s", del.LogicalID, del.Status)
	}
	_, err = cfnGetDashboard(t, d, "cfn-dash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DashboardNotFoundError")
}
