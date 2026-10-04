package emulator_test

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Every identifier the nine services of #1204's batch mint, asserted against the pattern its page
// publishes (#1204).
//
// Each value is read from the response that publishes it, not from the minting function, so the
// test also catches a rendering applied between the mint and the wire. A test per site, because a
// mint change that silently violates a published pattern fails in the consumer's own validation or
// a typed SDK's URI builder, outside substrate, where nothing here would see it.
//
// Sites with no published pattern are not here, and the audit in docs/services.md records them as
// such: AWS Backup's plan, version and selection IDs, CodeDeploy's application, deployment-group and
// deployment IDs, and the UUID in an MSK cluster ARN. Redshift mints none: every identifier it
// reports is the caller's. MSK's cluster ARN is asserted in the form every MSK page uses, which is
// substrate's reading rather than a published pattern.

// mintedIDCase is one minted identifier: the service and site it comes from, a function that mints
// one and returns it, and the pattern it must match.
type mintedIDCase struct {
	site    string
	pattern string
	mint    func(t *testing.T) string
}

// mintedIDMember decodes body and returns the string at the dotted path, failing if it is absent.
func mintedIDMember(t *testing.T, body []byte, path string) string {
	t.Helper()
	var doc any
	require.NoError(t, json.Unmarshal(body, &doc), "decode: %s", body)
	for _, key := range strings.Split(path, ".") {
		obj, ok := doc.(map[string]any)
		require.Truef(t, ok, "%s: %s is not an object in %s", path, key, body)
		doc = obj[key]
	}
	s, ok := doc.(string)
	require.Truef(t, ok, "%s is not a string in %s", path, body)
	require.NotEmptyf(t, s, "%s is empty in %s", path, body)
	return s
}

func TestMintedIDs_SatisfyTheirPublishedPatterns(t *testing.T) {
	t.Parallel()

	emr := func(t *testing.T) (*emulator.EMRServerlessPlugin, *emulator.RequestContext, string, []byte) {
		t.Helper()
		p := &emulator.EMRServerlessPlugin{}
		ctx, _ := wireSetup(t, p, "req-minted-emr")
		app := wireREST(t, p, ctx, "emr-serverless", http.MethodPost, "/applications",
			map[string]any{"name": "minted", "type": "SPARK", "releaseLabel": "emr-6.9.0"})
		appID := mintedIDMember(t, app, "applicationId")
		run := wireREST(t, p, ctx, "emr-serverless", http.MethodPost, "/applications/"+appID+"/jobruns",
			map[string]any{"name": "minted-run"})
		return p, ctx, appID, run
	}
	jsonTarget := func(t *testing.T, p emulator.Plugin, service, target, op string, body map[string]any) []byte {
		t.Helper()
		ctx, _ := wireSetup(t, p, "req-minted-"+service)
		return wireJSONTarget(t, p, ctx, service, target, op, body)
	}

	for _, tc := range []mintedIDCase{
		{
			// API_StartJobRun / API_ListJobRuns: applicationId `[0-9a-z]+`, 1–64.
			site: "EMR Serverless applicationId", pattern: `^[0-9a-z]{1,64}$`,
			mint: func(t *testing.T) string { _, _, appID, _ := emr(t); return appID },
		},
		{
			// API_JobRun / API_StartJobRun / API_GetJobRun / API_CancelJobRun: jobRunId `[0-9a-z]+`, 1–64.
			site: "EMR Serverless jobRunId", pattern: `^[0-9a-z]{1,64}$`,
			mint: func(t *testing.T) string { _, _, _, run := emr(t); return mintedIDMember(t, run, "jobRunId") },
		},
		{
			// API_StartJobRun: arn, length 60–1024, the published pattern verbatim.
			site:    "EMR Serverless job run arn",
			pattern: `^arn:(aws[a-zA-Z0-9-]*):emr-serverless:.+:(\d{12}):\/applications\/[0-9a-zA-Z]+\/jobruns\/[0-9a-zA-Z]+$`,
			mint: func(t *testing.T) string {
				_, _, _, run := emr(t)
				arn := mintedIDMember(t, run, "arn")
				require.GreaterOrEqual(t, len(arn), 60, "API_StartJobRun publishes a minimum ARN length of 60: %s", arn)
				return arn
			},
		},
		{
			// API_ListJobRuns: JobRunSummary publishes the ID as `id`, under the same pattern.
			site: "EMR Serverless ListJobRuns id", pattern: `^[0-9a-z]{1,64}$`,
			mint: func(t *testing.T) string {
				p, ctx, appID, run := emr(t)
				listed := wireREST(t, p, ctx, "emr-serverless", http.MethodGet, "/applications/"+appID+"/jobruns", nil)
				var out struct {
					JobRuns []map[string]any `json:"jobRuns"`
				}
				require.NoError(t, json.Unmarshal(listed, &out), "decode ListJobRuns: %s", listed)
				require.Len(t, out.JobRuns, 1, "%s", listed)
				id, _ := out.JobRuns[0]["id"].(string)
				require.Equal(t, mintedIDMember(t, run, "jobRunId"), id, "ListJobRuns' id is the started run's: %s", listed)
				require.NotContains(t, out.JobRuns[0], "jobRunId", "JobRunSummary publishes no jobRunId: %s", listed)
				return id
			},
		},
		{
			// MSK publishes no pattern. Every ARN on its pages is `…:cluster/{name}/{uuid}-{n}`.
			site:    "MSK cluster ARN",
			pattern: `^arn:aws:kafka:[a-z0-9-]+:\d{12}:cluster/minted/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}-[1-9]$`,
			mint: func(t *testing.T) string {
				p := &emulator.MSKPlugin{}
				ctx, _ := wireSetup(t, p, "req-minted-msk")
				body := wireREST(t, p, ctx, "kafka", http.MethodPost, "/v1/clusters", map[string]any{"ClusterName": "minted"})
				return mintedIDMember(t, body, "clusterArn")
			},
		},
		{
			// API_FileSystem: FileSystemId `^(fs-[0-9a-f]{8,})$`, 11–21.
			site: "FSx FileSystemId", pattern: `^fs-[0-9a-f]{8,18}$`,
			mint: func(t *testing.T) string {
				body := jsonTarget(t, &emulator.FSxPlugin{}, "fsx", "AWSSimbaAPIService_v20180301", "CreateFileSystem",
					map[string]any{"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": []string{"subnet-12345678"}})
				return mintedIDMember(t, body, "FileSystem.FileSystemId")
			},
		},
		{
			// API_LustreFileSystemConfiguration: MountName `^([A-Za-z0-9_-]{1,8})$`, for every
			// deployment type but SCRATCH_1, whose mount name is the constant "fsx".
			site: "FSx Lustre MountName", pattern: `^([A-Za-z0-9_-]{1,8})$`,
			mint: func(t *testing.T) string {
				body := jsonTarget(t, &emulator.FSxPlugin{}, "fsx", "AWSSimbaAPIService_v20180301", "CreateFileSystem",
					map[string]any{
						"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": []string{"subnet-12345678"},
						"LustreConfiguration": map[string]any{"DeploymentType": "PERSISTENT_2"},
					})
				return mintedIDMember(t, body, "FileSystem.LustreConfiguration.MountName")
			},
		},
		{
			// API_DescribedServer: ServerId `s-([0-9a-f]{17})`, fixed length 19.
			site: "Transfer ServerId", pattern: `^s-[0-9a-f]{17}$`,
			mint: func(t *testing.T) string {
				body := jsonTarget(t, &emulator.TransferPlugin{}, "transfer", "TransferService", "CreateServer", map[string]any{})
				return mintedIDMember(t, body, "ServerId")
			},
		},
		{
			// API_query_Query: QueryId `[a-zA-Z0-9]+`, 1–64.
			site: "Timestream QueryId", pattern: `^[a-zA-Z0-9]{1,64}$`,
			mint: func(t *testing.T) string {
				// An unseeded query other than SELECT * of a real table is refused (#1209), so the query
				// runs against one.
				p := &emulator.TimestreamPlugin{}
				ctx, _ := wireSetup(t, p, "req-minted-timestream")
				target := func(op string, body map[string]any) []byte {
					return wireJSONTarget(t, p, ctx, "timestream", "Timestream_20181101", op, body)
				}
				target("CreateDatabase", map[string]any{"DatabaseName": "minted"})
				target("CreateTable", map[string]any{"DatabaseName": "minted", "TableName": "ids"})
				body := target("Query", map[string]any{"QueryString": "SELECT * FROM minted.ids"})
				return mintedIDMember(t, body, "QueryId")
			},
		},
		{
			// API_StartExecution: an execution name, minted when the caller sends none, is 1–80
			// characters; the page publishes no pattern beyond its forbidden characters, none of which a
			// minted name uses.
			site: "Step Functions execution name", pattern: `^[A-Za-z0-9-]{1,80}$`,
			mint: func(t *testing.T) string {
				p := &emulator.StepFunctionsPlugin{}
				ctx, _ := wireSetup(t, p, "req-minted-sfn")
				target := func(op string, body map[string]any) []byte {
					return wireJSONTarget(t, p, ctx, "states", "AWSStepFunctions", op, body)
				}
				sm := target("CreateStateMachine", map[string]any{
					"name": "minted", "roleArn": "arn:aws:iam::123456789012:role/minted",
					"definition": `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
				})
				exec := target("StartExecution", map[string]any{"stateMachineArn": mintedIDMember(t, sm, "stateMachineArn")})
				arn := mintedIDMember(t, exec, "executionArn")
				return arn[strings.LastIndex(arn, ":")+1:]
			},
		},
	} {
		t.Run(tc.site, func(t *testing.T) {
			t.Parallel()
			got := tc.mint(t)
			require.Regexpf(t, regexp.MustCompile(tc.pattern), got, "%s %q violates its published pattern %s", tc.site, got, tc.pattern)
		})
	}
}
