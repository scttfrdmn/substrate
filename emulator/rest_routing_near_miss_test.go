package emulator_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Near-miss paths for the three REST plugins in #1205's batch that routed on a substring (#1205).
//
// A test written against a plugin's own behavior uses well-formed paths, so it cannot see a router
// that also accepts malformed ones; the near-misses are the test. Each must reach the plugin's
// unknown-operation refusal, UnknownOperationException/404, which every REST-JSON service's Common
// Errors page publishes, rather than another operation's success or a 400 for "malformed input".

// nearMissCase is one request and the refusal it must answer.
type nearMissCase struct {
	method, path string
	code         string
	status       int
}

// requireRefusal issues each case to p and requires the published refusal.
func requireRefusal(t *testing.T, p emulator.Plugin, ctx *emulator.RequestContext, service string, cases []nearMissCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: service, HTTPMethod: tc.method, Path: tc.path,
				Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
			})
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%s %s must be refused, answered %v %v", tc.method, tc.path, resp, err)
			require.Equal(t, tc.code, awsErr.Code, "%s %s: %s", tc.method, tc.path, awsErr.Message)
			require.Equal(t, tc.status, awsErr.HTTPStatus, "%s %s", tc.method, tc.path)
		})
	}
}

const (
	unknownOp     = "UnknownOperationException"
	unknownStatus = http.StatusNotFound
)

func TestEMRServerlessRouting_ANearMissPathIsUnknown(t *testing.T) {
	t.Parallel()
	p := &emulator.EMRServerlessPlugin{}
	ctx, _ := wireSetup(t, p, "req-emr-nearmiss")
	appID := mintedIDMember(t, wireREST(t, p, ctx, "emr-serverless", http.MethodPost, "/applications",
		map[string]any{"name": "nm", "type": "SPARK", "releaseLabel": "emr-6.9.0", "clientToken": "nm"}), "applicationId")

	requireRefusal(t, p, ctx, "emr-serverless", []nearMissCase{
		// jobruns is a whole segment: these used to route as ListJobRuns and GetJobRun.
		{http.MethodGet, "/applications/" + appID + "/jobrunsbad", unknownOp, unknownStatus},
		{http.MethodGet, "/applications/" + appID + "/jobrunsbad/x", unknownOp, unknownStatus},
		{http.MethodPost, "/applications/" + appID + "/jobrunsbad", unknownOp, unknownStatus},
		// The application ID is one segment: this used to read it as "ab/cd".
		{http.MethodGet, "/applications/ab/" + appID + "/jobruns", unknownOp, unknownStatus},
		// An empty ID segment: this used to route ListJobRuns with application "".
		{http.MethodGet, "/applications//jobruns", unknownOp, unknownStatus},
		{http.MethodGet, "/applications/", unknownOp, unknownStatus},
		// A literal that is not a published segment at all.
		{http.MethodGet, "/applicationsx", unknownOp, unknownStatus},
		{http.MethodGet, "/applications/" + appID + "/jobruns/run/cancel", unknownOp, unknownStatus},
		// A published path with a verb it does not publish.
		{http.MethodPost, "/applications/" + appID + "/jobruns/00abc", unknownOp, unknownStatus},
		{http.MethodPut, "/applications/" + appID, unknownOp, unknownStatus},
		// Not a near miss but a well-formed GetApplication: "jobruns" is a valid application ID, so
		// /applications/jobruns is what the published URI template makes of it, and answers the
		// operation's own not-found. It used to route as ListJobRuns for application "".
		{http.MethodGet, "/applications/jobruns", "ResourceNotFoundException", http.StatusNotFound},
	})
}

func TestMSKRouting_ANearMissPathIsUnknown(t *testing.T) {
	t.Parallel()
	p := &emulator.MSKPlugin{}
	ctx, _ := wireSetup(t, p, "req-msk-nearmiss")
	arn := mintedIDMember(t, wireREST(t, p, ctx, "kafka", http.MethodPost, "/v1/clusters",
		mskMinimalCluster("nm")), "clusterArn")

	requireRefusal(t, p, ctx, "kafka", []nearMissCase{
		// The two sub-resource arms tested only the suffix, so these were ListNodes and
		// GetBootstrapBrokers refused 400 for a malformed ARN.
		{http.MethodGet, "/anything/at/all/nodes", unknownOp, unknownStatus},
		{http.MethodGet, "/anything/bootstrap-brokers", unknownOp, unknownStatus},
		{http.MethodGet, "/nodes", unknownOp, unknownStatus},
		// MSK publishes /nodes and /bootstrap-brokers under /v1 only.
		{http.MethodGet, "/v2/clusters/" + arn + "/nodes", unknownOp, unknownStatus},
		{http.MethodGet, "/api/v2/clusters/" + arn + "/nodes", unknownOp, unknownStatus},
		{http.MethodGet, "/api/v2/clusters/" + arn + "/bootstrap-brokers", unknownOp, unknownStatus},
		// A published path with a verb it does not publish.
		{http.MethodPost, "/v1/clusters/" + arn + "/nodes", unknownOp, unknownStatus},
		{http.MethodDelete, "/v1/clusters/" + arn + "/bootstrap-brokers", unknownOp, unknownStatus},
		{http.MethodPut, "/v1/clusters/" + arn, unknownOp, unknownStatus},
		{http.MethodGet, "/v1/clustersx", unknownOp, unknownStatus},
	})

	// The anchored arms still answer the published paths.
	wireREST(t, p, ctx, "kafka", http.MethodGet, "/v1/clusters/"+arn+"/nodes", nil)
	wireREST(t, p, ctx, "kafka", http.MethodGet, "/v1/clusters/"+arn+"/bootstrap-brokers", nil)
}

func TestBackupRouting_ANearMissPathIsUnknown(t *testing.T) {
	t.Parallel()
	p := &emulator.BackupPlugin{}
	ctx, _ := wireSetup(t, p, "req-backup-nearmiss")
	planID := mintedIDMember(t, wireREST(t, p, ctx, "backup", http.MethodPost, "/backup/plans",
		map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "nm", "Rules": []any{}}}), "BackupPlanId")

	requireRefusal(t, p, ctx, "backup", []nearMissCase{
		// Prefix tests, so these were vault and plan operations.
		{http.MethodGet, "/backup-vaultsx", unknownOp, unknownStatus},
		{http.MethodGet, "/backup/plansx", unknownOp, unknownStatus},
		{http.MethodGet, "/backup/plansx/" + planID, unknownOp, unknownStatus},
		// A substring search for "/selections", so these read the plan ID as "a/<id>" or routed
		// a segment that is not "selections".
		{http.MethodGet, "/backup/plans/a/" + planID + "/selections", unknownOp, unknownStatus},
		{http.MethodGet, "/backup/plans/" + planID + "/selectionsx", unknownOp, unknownStatus},
		{http.MethodGet, "/backup/plans/" + planID + "/selections/s/extra", unknownOp, unknownStatus},
		// A vault name is one segment.
		{http.MethodGet, "/backup-vaults/a/b", unknownOp, unknownStatus},
		{http.MethodGet, "/backup", unknownOp, unknownStatus},
	})

	// GetBackupPlan's published URI is `GET /backup/plans/{backupPlanId}/?versionId=…`, and the old
	// router kept the trailing slash inside the plan ID, so the published form found no plan (#1176).
	// The query string reaches the plugin as Params, as the parser splits it.
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "backup", HTTPMethod: http.MethodGet, Path: "/backup/plans/" + planID + "/",
		Headers: map[string]string{}, Params: map[string]string{"versionId": "v1"},
	})
	require.NoError(t, err, "GET /backup/plans/{id}/?versionId=")
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", resp.Body)
	require.Equal(t, planID, mintedIDMember(t, resp.Body, "BackupPlanId"), "the published GetBackupPlan form reaches the plan: %s", resp.Body)
	// Without the slash it still works, and an empty remainder is still ListBackupPlans.
	got := wireREST(t, p, ctx, "backup", http.MethodGet, "/backup/plans/"+planID, nil)
	require.Equal(t, planID, mintedIDMember(t, got, "BackupPlanId"), "%s", got)
	listed := wireREST(t, p, ctx, "backup", http.MethodGet, "/backup/plans/", nil)
	require.Contains(t, string(listed), `"BackupPlansList"`, "GET /backup/plans/ is ListBackupPlans: %s", listed)
	require.Contains(t, string(listed), planID, "%s", listed)
}
