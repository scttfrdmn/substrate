package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A CloudFormation stack's distribution is deleted the way API_DeleteDistribution requires: read,
// disable under the ETag read, delete under the ETag the disable answered (#1271). Before #1271
// DeleteDistribution checked neither the version nor the state, so the sweep's one bare DELETE was
// enough; now it is refused, and a stack holding a distribution would wedge in DELETE_FAILED.

// cfnSweepDistributionTemplate deploys one enabled distribution.
const cfnSweepDistributionTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"Dist": {"Type": "AWS::CloudFront::Distribution", "Properties": {
			"DistributionConfig": {"Comment": "sweep", "Enabled": true}
		}}
	}
}`

// cfnDeployedDistributionID deploys cfnSweepDistributionTemplate as stack and returns the
// distribution's ID.
func cfnDeployedDistributionID(t *testing.T, d *emulator.StackDeployer, stack string) string {
	t.Helper()
	result, err := d.Deploy(context.Background(), cfnSweepDistributionTemplate, stack, nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)
	return result.Resources[0].PhysicalID
}

// cfnDistributionStatus reports the status a GetDistribution of id answers.
func cfnDistributionStatus(t *testing.T, d *emulator.StackDeployer, id string) int {
	t.Helper()
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "cloudfront", Operation: "GET", Path: "/2020-05-31/distribution/" + id,
		Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	if resp != nil {
		return resp.StatusCode
	}
	if awsErr, ok := err.(*emulator.AWSError); ok { //nolint:errorlint // the dispatch returns the plugin's error as is
		return awsErr.HTTPStatus
	}
	require.NoError(t, err, "GetDistribution %s", id)
	return 0
}

func TestCFN_DeleteStackDisablesThenDeletesItsDistribution(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	id := cfnDeployedDistributionID(t, d, "cf-sweep")
	require.Equal(t, http.StatusOK, cfnDistributionStatus(t, d, id), "the deploy must have created the distribution")

	require.NoError(t, d.DeleteStack(context.Background(), "cf-sweep"))

	assert.Equal(t, http.StatusNotFound, cfnDistributionStatus(t, d, id), "the distribution dies with its stack")
	ops := sweepOperations(t, store, "cf-sweep")
	require.GreaterOrEqual(t, len(ops), 3, "%v", ops)
	assert.Equal(t, []string{"GetDistributionConfig", "UpdateDistribution", "DeleteDistribution"}, ops[len(ops)-3:],
		"the sweep reads, disables, then deletes, in that order")
}

// A step that fails for a reason other than absence fails the delete, and the distribution and the
// stack both stay: reporting DELETE_COMPLETE over a distribution that still exists is what #518
// exists to prevent.
func TestCFN_DeleteStackFailsWhenTheDistributionCannotBeRead(t *testing.T) {
	d, state, _, _ := newSweepDeployer(t)
	id := cfnDeployedDistributionID(t, d, "cf-unreadable")

	key := "cfdist:123456789012/" + id
	data, err := state.Get(context.Background(), "cloudfront", key)
	require.NoError(t, err)
	require.NotNil(t, data, "the distribution record must be where the test corrupts it")
	var record map[string]any
	require.NoError(t, json.Unmarshal(data, &record))
	record["Config"] = "<DistributionConfig><Comment>"
	data, err = json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "cloudfront", key, data))

	require.Error(t, d.DeleteStack(context.Background(), "cf-unreadable"), "a failed read must fail the delete")
	stacks, err := d.ListStacks(context.Background())
	require.NoError(t, err)
	assert.Len(t, stacks, 1, "a stack whose delete failed is kept")
	stored, err := state.Get(context.Background(), "cloudfront", key)
	require.NoError(t, err)
	assert.NotNil(t, stored, "the distribution was not deleted")
}

func TestCFN_DeleteStackCompletesForADistributionAlreadyGone(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	id := cfnDeployedDistributionID(t, d, "cf-gone")

	// Delete it out of band, by the same published dance.
	dispatch := func(method, path, ifMatch, body string) *emulator.AWSResponse {
		t.Helper()
		headers := map[string]string{}
		if ifMatch != "" {
			headers["If-Match"] = ifMatch
		}
		resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
			Service: "cloudfront", Operation: method, Path: path, Body: []byte(body),
			Headers: headers, Params: map[string]string{},
		}, "probe")
		require.NoError(t, err, "%s %s", method, path)
		return resp
	}
	path := "/2020-05-31/distribution/" + id
	read := dispatch("GET", path+"/config", "", "")
	disabled := dispatch("PUT", path+"/config", read.Headers["ETag"],
		strings.Replace(string(read.Body), "<Enabled>true</Enabled>", "<Enabled>false</Enabled>", 1))
	dispatch("DELETE", path, disabled.Headers["ETag"], "")

	require.NoError(t, d.DeleteStack(context.Background(), "cf-gone"), "an absent distribution is a completed delete")
	stacks, err := d.ListStacks(context.Background())
	require.NoError(t, err)
	assert.Empty(t, stacks)
}
