package emulator_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CodeDeploy's share of the batch audits: pagination (#1195), required members (#1197), published
// codes (#1198) and published members (#1199). Every assertion is on the raw response, or on the
// refusal's code and status together, because a typed decoder cannot see an extra or missing member
// and a `!= 200` check cannot see a wrong code.

// codedeployAuditRole is a well-formed service role ARN.
const codedeployAuditRole = "arn:aws:iam::123456789012:role/codedeploy-audit"

// codedeployAuditHarness is a CodeDeploy plugin on a frozen clock over a given store.
type codedeployAuditHarness struct {
	t   *testing.T
	p   *emulator.CodeDeployPlugin
	ctx *emulator.RequestContext
}

func newCodeDeployAuditHarness(t *testing.T, state emulator.StateManager) *codedeployAuditHarness {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	p := &emulator.CodeDeployPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	return &codedeployAuditHarness{t: t, p: p, ctx: &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1",
		RequestID: "req-codedeploy-audit", IDs: emulator.NewIDMint("req-codedeploy-audit"),
	}}
}

// call issues one operation and returns its raw body or its error.
func (h *codedeployAuditHarness) call(op string, body map[string]any) ([]byte, error) {
	h.t.Helper()
	resp, err := h.p.HandleRequest(h.ctx, codedeployRequest(h.t, op, body))
	if err != nil {
		return nil, err
	}
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body, nil
}

// ok issues one operation that must succeed.
func (h *codedeployAuditHarness) ok(op string, body map[string]any) []byte {
	h.t.Helper()
	out, err := h.call(op, body)
	require.NoError(h.t, err, "%s", op)
	return out
}

// requireRefused asserts err is the CodeDeploy refusal code at 400.
func requireCodeDeployRefused(t *testing.T, err error, code, context string) {
	t.Helper()
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "%s: want refusal %s, got %v", context, code, err)
	require.Equalf(t, code, awsErr.Code, "%s answers the code its page publishes", context)
	require.Equalf(t, http.StatusBadRequest, awsErr.HTTPStatus, "%s answers 400", context)
}

func TestCodeDeployAudit_EachMissingOrMalformedMemberAnswersItsPublishedCode(t *testing.T) {
	t.Parallel()
	h := newCodeDeployAuditHarness(t, emulator.NewMemoryStateManager())
	h.ok("CreateApplication", map[string]any{"applicationName": "app"})
	h.ok("CreateDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp", "serviceRoleArn": codedeployAuditRole})

	group := func(extra map[string]any) map[string]any {
		body := map[string]any{"applicationName": "app", "deploymentGroupName": "new-grp", "serviceRoleArn": codedeployAuditRole}
		for k, v := range extra {
			body[k] = v
		}
		return body
	}
	tooLong := strings.Repeat("a", 101)
	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
		code string
	}{
		{"CreateApplication/no name", "CreateApplication", map[string]any{}, "ApplicationNameRequiredException"},
		{"CreateApplication/bad name", "CreateApplication", map[string]any{"applicationName": "bad name!"}, "InvalidApplicationNameException"},
		{"CreateApplication/long name", "CreateApplication", map[string]any{"applicationName": tooLong}, "InvalidApplicationNameException"},
		{"CreateApplication/bad platform", "CreateApplication", map[string]any{"applicationName": "x", "computePlatform": "Windows"}, "InvalidComputePlatformException"},
		{"GetApplication/no name", "GetApplication", map[string]any{}, "ApplicationNameRequiredException"},
		{"DeleteApplication/no name", "DeleteApplication", map[string]any{}, "ApplicationNameRequiredException"},
		{"DeleteApplication/bad name", "DeleteApplication", map[string]any{"applicationName": "bad/name"}, "InvalidApplicationNameException"},
		{"CreateDeploymentGroup/no application", "CreateDeploymentGroup", map[string]any{"deploymentGroupName": "g", "serviceRoleArn": codedeployAuditRole}, "ApplicationNameRequiredException"},
		{"CreateDeploymentGroup/no group", "CreateDeploymentGroup", map[string]any{"applicationName": "app", "serviceRoleArn": codedeployAuditRole}, "DeploymentGroupNameRequiredException"},
		{"CreateDeploymentGroup/bad group", "CreateDeploymentGroup", group(map[string]any{"deploymentGroupName": "bad group"}), "InvalidDeploymentGroupNameException"},
		{"CreateDeploymentGroup/no role", "CreateDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "g"}, "RoleRequiredException"},
		{"CreateDeploymentGroup/bad role", "CreateDeploymentGroup", group(map[string]any{"serviceRoleArn": "my-role"}), "InvalidRoleException"},
		{"CreateDeploymentGroup/bad config", "CreateDeploymentGroup", group(map[string]any{"deploymentConfigName": "bad config"}), "InvalidDeploymentConfigNameException"},
		{"CreateDeploymentGroup/both EC2 tag forms", "CreateDeploymentGroup", group(map[string]any{
			"ec2TagFilters": []map[string]string{{"Key": "a", "Type": "KEY_ONLY"}}, "ec2TagSet": map[string]any{"ec2TagSetList": []any{}},
		}), "InvalidEC2TagCombinationException"},
		{"CreateDeploymentGroup/both on-premises tag forms", "CreateDeploymentGroup", group(map[string]any{
			"onPremisesInstanceTagFilters": []map[string]string{{"Key": "a", "Type": "KEY_ONLY"}}, "onPremisesTagSet": map[string]any{"onPremisesTagSetList": []any{}},
		}), "InvalidOnPremisesTagCombinationException"},
		{"CreateDeploymentGroup/bad strategy", "CreateDeploymentGroup", group(map[string]any{"outdatedInstancesStrategy": "SOMETIMES"}), "InvalidInputException"},
		{"CreateDeploymentGroup/missing application", "CreateDeploymentGroup", map[string]any{"applicationName": "absent", "deploymentGroupName": "g", "serviceRoleArn": codedeployAuditRole}, "ApplicationDoesNotExistException"},
		{"GetDeploymentGroup/no group", "GetDeploymentGroup", map[string]any{"applicationName": "app"}, "DeploymentGroupNameRequiredException"},
		{"GetDeploymentGroup/missing application", "GetDeploymentGroup", map[string]any{"applicationName": "absent", "deploymentGroupName": "grp"}, "ApplicationDoesNotExistException"},
		{"GetDeploymentGroup/missing group", "GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "absent"}, "DeploymentGroupDoesNotExistException"},
		{"DeleteDeploymentGroup/no application", "DeleteDeploymentGroup", map[string]any{"deploymentGroupName": "grp"}, "ApplicationNameRequiredException"},
		{"DeleteDeploymentGroup/no group", "DeleteDeploymentGroup", map[string]any{"applicationName": "app"}, "DeploymentGroupNameRequiredException"},
		{"CreateDeployment/no application", "CreateDeployment", map[string]any{}, "ApplicationNameRequiredException"},
		{"CreateDeployment/bad group", "CreateDeployment", map[string]any{"applicationName": "app", "deploymentGroupName": "bad group"}, "InvalidDeploymentGroupNameException"},
		{"CreateDeployment/bad file behavior", "CreateDeployment", map[string]any{"applicationName": "app", "fileExistsBehavior": "MERGE"}, "InvalidFileExistsBehaviorException"},
		{"CreateDeployment/bad mode", "CreateDeployment", map[string]any{"applicationName": "app", "deploymentMode": "PARTIAL"}, "InvalidInputException"},
		{"GetDeployment/no id", "GetDeployment", map[string]any{}, "DeploymentIdRequiredException"},
		{"GetDeployment/missing", "GetDeployment", map[string]any{"deploymentId": "d-ABCDEFGHI"}, "DeploymentDoesNotExistException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.call(tc.op, tc.body)
			requireCodeDeployRefused(t, err, tc.code, tc.name)
		})
	}
}

// API_DeleteApplication and API_DeleteDeploymentGroup publish no not-found code, so a delete of
// something absent succeeds (#1198).
func TestCodeDeployAudit_DeletesOfSomethingAbsentSucceed(t *testing.T) {
	t.Parallel()
	h := newCodeDeployAuditHarness(t, emulator.NewMemoryStateManager())

	body := h.ok("DeleteApplication", map[string]any{"applicationName": "never-made"})
	require.Empty(t, body, "DeleteApplication answers 200 with an empty body, as published")
	body = h.ok("DeleteDeploymentGroup", map[string]any{"applicationName": "never-made", "deploymentGroupName": "nor-this"})
	require.JSONEq(t, `{"hooksNotCleanedUp":[]}`, string(body), "DeleteDeploymentGroup answers its published body")

	// Deleting an application takes its deployment groups with it: re-creating the application does
	// not resurrect the old group.
	h.ok("CreateApplication", map[string]any{"applicationName": "app"})
	h.ok("CreateDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp", "serviceRoleArn": codedeployAuditRole})
	h.ok("DeleteApplication", map[string]any{"applicationName": "app"})
	h.ok("CreateApplication", map[string]any{"applicationName": "app"})
	_, err := h.call("GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"})
	requireCodeDeployRefused(t, err, "DeploymentGroupDoesNotExistException", "a group of a deleted application")
}

func TestCodeDeployAudit_ListApplicationsPagesAndRefusesAnUnissuedToken(t *testing.T) {
	t.Parallel()
	h := newCodeDeployAuditHarness(t, emulator.NewMemoryStateManager())
	const total = 101
	for i := range total {
		h.ok("CreateApplication", map[string]any{"applicationName": fmt.Sprintf("app-%03d", i)})
	}

	first := h.ok("ListApplications", map[string]any{})
	var page struct {
		Applications []string `json:"applications"`
		NextToken    *string  `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal(first, &page), "decode page one: %s", first)
	require.Len(t, page.Applications, 100, "page one is full")
	require.NotNil(t, page.NextToken, "page one carries a nextToken: %s", first)

	second := h.ok("ListApplications", map[string]any{"nextToken": *page.NextToken})
	var last map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(second, &last), "decode page two: %s", second)
	require.JSONEq(t, `["app-100"]`, string(last["applications"]), "page two is the remainder")
	require.NotContains(t, last, "nextToken", "the last page omits nextToken rather than answering it empty: %s", second)

	for _, bad := range []string{"not-a-token", "LTE=", "+1"} {
		_, err := h.call("ListApplications", map[string]any{"nextToken": bad})
		requireCodeDeployRefused(t, err, "InvalidNextTokenException", "an unissued token "+bad)
	}
}

func TestCodeDeployAudit_RecordsAnswerTheirPublishedMembers(t *testing.T) {
	t.Parallel()
	h := newCodeDeployAuditHarness(t, emulator.NewMemoryStateManager())
	h.ok("CreateApplication", map[string]any{"applicationName": "app"})

	app := h.ok("GetApplication", map[string]any{"applicationName": "app"})
	require.Contains(t, string(app), `"linkedToGitHub":false`, "API_ApplicationInfo's linkedToGitHub is answered: %s", app)
	require.Contains(t, string(app), `"computePlatform":"Server"`, "%s", app)

	tagFilters := `[{"Key":"Name","Type":"KEY_AND_VALUE","Value":"web"}]`
	var filters any
	require.NoError(t, json.Unmarshal([]byte(tagFilters), &filters))
	h.ok("CreateDeploymentGroup", map[string]any{
		"applicationName": "app", "deploymentGroupName": "grp", "serviceRoleArn": codedeployAuditRole,
		"ec2TagFilters": filters, "autoScalingGroups": []string{"asg-1"},
		"deploymentStyle": map[string]string{"deploymentType": "IN_PLACE", "deploymentOption": "WITHOUT_TRAFFIC_CONTROL"},
	})
	group := string(h.ok("GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"}))
	for _, want := range []string{
		`"computePlatform":"Server"`,
		`"deploymentConfigName":"CodeDeployDefault.OneAtATime"`,
		`"ec2TagFilters":` + tagFilters,
		`"autoScalingGroups":[{"name":"asg-1"}]`,
		`"deploymentType":"IN_PLACE"`,
	} {
		require.Containsf(t, group, want, "GetDeploymentGroup answers %s: %s", want, group)
	}
	require.NotContains(t, group, "lastSuccessfulDeployment", "no deployment has run yet: %s", group)

	revision := map[string]any{"revisionType": "S3", "s3Location": map[string]string{"bucket": "b", "key": "k.zip", "bundleType": "zip"}}
	created := h.ok("CreateDeployment", map[string]any{
		"applicationName": "app", "deploymentGroupName": "grp", "revision": revision,
		"description": "wire", "fileExistsBehavior": "OVERWRITE", "deploymentMode": "STANDARD",
	})
	var id struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(created, &id))

	deployment := string(h.ok("GetDeployment", map[string]any{"deploymentId": id.DeploymentID}))
	for _, want := range []string{
		`"creator":"user"`,
		`"computePlatform":"Server"`,
		`"deploymentConfigName":"CodeDeployDefault.OneAtATime"`,
		`"startTime":1700000000.000`,
		`"description":"wire"`,
		`"fileExistsBehavior":"OVERWRITE"`,
		`"bucket":"b"`,
		`"deploymentType":"IN_PLACE"`,
	} {
		require.Containsf(t, deployment, want, "GetDeployment answers %s: %s", want, deployment)
	}
	require.NotContains(t, deployment, "deploymentMode", "a STANDARD deployment answers no deploymentMode, as published: %s", deployment)

	group = string(h.ok("GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"}))
	for _, want := range []string{
		`"lastSuccessfulDeployment":{"createTime":1700000000.000,"deploymentId":"` + id.DeploymentID + `"`,
		`"lastAttemptedDeployment":{"createTime":1700000000.000,"deploymentId":"` + id.DeploymentID + `"`,
		`"targetRevision":{`,
	} {
		require.Containsf(t, group, want, "after a deployment, GetDeploymentGroup answers %s: %s", want, group)
	}

	restart := h.ok("CreateDeployment", map[string]any{"applicationName": "app", "deploymentGroupName": "grp", "deploymentMode": "RESTART"})
	require.NoError(t, json.Unmarshal(restart, &id))
	require.Contains(t, string(h.ok("GetDeployment", map[string]any{"deploymentId": id.DeploymentID})), `"deploymentMode":"RESTART"`)

	// A Lambda application's group names no default configuration: OneAtATime is EC2/on-premises only.
	h.ok("CreateApplication", map[string]any{"applicationName": "fn-app", "computePlatform": "Lambda"})
	h.ok("CreateDeploymentGroup", map[string]any{"applicationName": "fn-app", "deploymentGroupName": "fn-grp", "serviceRoleArn": codedeployAuditRole})
	fnGroup := string(h.ok("GetDeploymentGroup", map[string]any{"applicationName": "fn-app", "deploymentGroupName": "fn-grp"}))
	require.Contains(t, fnGroup, `"computePlatform":"Lambda"`, "%s", fnGroup)
	require.NotContains(t, fnGroup, "deploymentConfigName", "%s", fnGroup)
}

func TestCodeDeployAudit_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body map[string]any
	}{
		{"DeleteApplication, group index read", func(m *cfFaultStateManager) { m.failGet = "group_names:" }, "DeleteApplication", map[string]any{"applicationName": "app"}},
		{"DeleteApplication, group delete", func(m *cfFaultStateManager) { m.failDelete = "group:" }, "DeleteApplication", map[string]any{"applicationName": "app"}},
		{"DeleteApplication, group index delete", func(m *cfFaultStateManager) { m.failDelete = "group_names:" }, "DeleteApplication", map[string]any{"applicationName": "app"}},
		{"CreateDeploymentGroup, write", func(m *cfFaultStateManager) { m.failPut = "group:" }, "CreateDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "new", "serviceRoleArn": codedeployAuditRole}},
		{"CreateDeployment, group write", func(m *cfFaultStateManager) { m.failPut = "group:" }, "CreateDeployment", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"}},
		{"ListApplications, index read", func(m *cfFaultStateManager) { m.failGet = "app_names:" }, "ListApplications", map[string]any{}},
		{"GetDeploymentGroup, application read", func(m *cfFaultStateManager) { m.failGet = "app:" }, "GetDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newCodeDeployAuditHarness(t, fault)
			h.ok("CreateApplication", map[string]any{"applicationName": "app"})
			h.ok("CreateDeploymentGroup", map[string]any{"applicationName": "app", "deploymentGroupName": "grp", "serviceRoleArn": codedeployAuditRole})

			tc.arm(fault)
			_, err := h.call(tc.op, tc.body)
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
