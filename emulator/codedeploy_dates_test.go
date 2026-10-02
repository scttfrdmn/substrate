package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CodeDeploy publishes every Timestamp as epoch seconds, and these assertions are on the raw
// response bytes because that is the only place the difference is visible (#1207).
//
// A typed decoder cannot tell 1446229001.211 from "2015-10-30T16:56:41.211Z": both land in a
// time.Time holding the same instant, and emulator.EpochSeconds accepts either on the way in. That
// is how two sites agreed on the wrong form unnoticed — emulator/codedeploy_plugin_test.go decodes
// each get response into a struct of published members and reads no date at all, so nothing it
// asserts could distinguish them. So the test reads the bytes, as emulator/iam_shape_members_test.go
// does.
//
// This divergence was the hardest one in CodeDeploy, which is why it is pinned twice over. An
// awsJson1_1 timestamp deserializer expects a number, so an RFC3339 string did not give a consumer a
// wrong value — it denied the whole response: GetApplication and GetDeployment failed in the SDK
// before any assertion of the consumer's own ran.
//
// The instant is AWS's own worked example and the clock is frozen, so the expectation is an exact
// string rather than a tolerance. Freezing is what makes that assertable: TimeController.Now
// otherwise advances from its baseline by the wall time elapsed since it was set, and no test may
// read the wall clock.

// codedeployDatesClock is the instant AWS's own API_GetApplication sample response carries,
// 1446229001.211.
var codedeployDatesClock = time.Unix(1446229001, 211000000).UTC()

// codedeployDatesRendered is the rendering every published CodeDeploy date must take.
//
// emulator.EpochSeconds renders exactly three decimals, which is the precision AWS's samples carry —
// and the fractional part is load-bearing rather than cosmetic: a consumer ordering two events
// inside the same second needs it.
const codedeployDatesRendered = "1446229001.211"

// setupCodeDeployDatesPlugin returns the CodeDeploy plugin on a frozen clock set to
// codedeployDatesClock.
//
// Its own harness rather than setupCodeDeployPlugin, which seeds the clock from time.Now and leaves
// it running, and which mints no IDs.
func setupCodeDeployDatesPlugin(t *testing.T) (*emulator.CodeDeployPlugin, *emulator.RequestContext) {
	t.Helper()
	tc := emulator.NewTimeController(codedeployDatesClock)
	// Freeze then SetTime, in that order, which is the ordering TimeController.Freeze documents:
	// Freeze alone stops the clock where it currently *reads*, which is the baseline plus the tens of
	// microseconds since NewTimeController, and that drift is visible in a rendered date.
	tc.Freeze()
	tc.SetTime(codedeployDatesClock)
	p := &emulator.CodeDeployPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   emulator.NewMemoryStateManager(),
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.CodeDeployPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-codedeploy-dates",
		IDs:       emulator.NewIDMint("req-codedeploy-dates"),
	}
}

// codedeployDatesRaw issues one operation and returns the raw response body, failing the test on
// anything but 200.
func codedeployDatesRaw(t *testing.T, p *emulator.CodeDeployPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, codedeployRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// codedeployDatesRequireEpoch fails unless body renders member as the published epoch value and
// nowhere renders it as a string.
//
// Both halves are needed. The first pins the value and the precision; the second is what catches a
// second date left behind, since GetDeployment answers two and a sibling at the same depth would
// otherwise satisfy the first on its own.
func codedeployDatesRequireEpoch(t *testing.T, op, member string, body []byte) {
	t.Helper()
	require.Containsf(t, string(body), `"`+member+`":`+codedeployDatesRendered,
		"%s must answer %s as the published epoch value: %s", op, member, body)
	require.NotContainsf(t, string(body), `"`+member+`":"`,
		"%s answered %s as a string, and awsJson1_1 publishes a Timestamp as a number: %s", op, member, body)
}

// codedeployDatesSeed creates one application, one deployment group and one deployment, and returns
// the names the get operations address them by.
//
// CreateDeployment needs both of the others to exist, so the three are always built together.
func codedeployDatesSeed(t *testing.T, p *emulator.CodeDeployPlugin, ctx *emulator.RequestContext) (app, group, deploymentID string) {
	t.Helper()
	app, group = "dates-app", "dates-group"

	codedeployDatesRaw(t, p, ctx, "CreateApplication", map[string]any{
		"applicationName": app,
		"computePlatform": "Server",
	})
	codedeployDatesRaw(t, p, ctx, "CreateDeploymentGroup", map[string]any{
		"applicationName":     app,
		"deploymentGroupName": group,
		"serviceRoleArn":      "arn:aws:iam::123456789012:role/codedeploy-dates",
	})
	created := codedeployDatesRaw(t, p, ctx, "CreateDeployment", map[string]any{
		"applicationName":     app,
		"deploymentGroupName": group,
	})

	var deployment struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(created, &deployment), "decode CreateDeployment: %s", created)
	require.NotEmpty(t, deployment.DeploymentID, "CreateDeployment must report a deployment id")
	return app, group, deployment.DeploymentID
}

func TestCodeDeployDates_PublishedDatesAreEpochSeconds(t *testing.T) {
	t.Parallel()
	p, ctx := setupCodeDeployDatesPlugin(t)
	app, _, deploymentID := codedeployDatesSeed(t, p, ctx)

	getApplication := codedeployDatesRaw(t, p, ctx, "GetApplication", map[string]any{"applicationName": app})
	getDeployment := codedeployDatesRaw(t, p, ctx, "GetDeployment", map[string]any{"deploymentId": deploymentID})

	// Every site that answers a published date, and every date it answers. There are three, which is
	// the whole set: GetDeploymentGroup is absent because API_GetDeploymentGroup publishes no
	// top-level timestamp at all — its only dates are nested in lastAttemptedDeployment and
	// lastSuccessfulDeployment, which substrate does not model — and the three creates, the two
	// deletes and ListApplications each answer one or two published non-date members.
	for _, tc := range []struct {
		op     string
		member string
		body   []byte
	}{
		{"GetApplication", "createTime", getApplication},
		{"GetDeployment", "createTime", getDeployment},
		{"GetDeployment", "completeTime", getDeployment},
	} {
		t.Run(tc.op+"/"+tc.member, func(t *testing.T) {
			codedeployDatesRequireEpoch(t, tc.op, tc.member, tc.body)
		})
	}

	// GetDeployment's two dates are equal, and that is a separate defect rather than an artifact of
	// the frozen clock: createDeployment writes completeTime from the same clock read as createTime,
	// so a consumer computing a deployment's duration gets zero against a running clock too. #1196
	// owns it — a deployment is Succeeded before CreateDeployment returns — and this records that the
	// equality is known, so a later reader does not take it for what this test is asserting.
	require.Contains(t, string(getDeployment),
		`"createTime":`+codedeployDatesRendered+`,"completeTime":`+codedeployDatesRendered,
		"GetDeployment answers completeTime equal to createTime (#1196): %s", getDeployment)
}

func TestCodeDeployDates_NoResponseRendersAnRFC3339Date(t *testing.T) {
	t.Parallel()
	p, ctx := setupCodeDeployDatesPlugin(t)

	// The same instant written the way a time.Time marshals it. A substring search for this is the
	// cheap catch-all: it fails on any member, nested at any depth and under any name, that was left
	// a time.Time — which is the whole class of defect #1207 reported. Unlike Backup's counterpart
	// this one needs no exclusion, because CodeDeploy answers no unpublished date anywhere.
	rfc3339 := codedeployDatesClock.Format(time.RFC3339Nano)

	app, group, deploymentID := codedeployDatesSeed(t, p, ctx)

	// All nine routed operations, so a date reintroduced at a site that answers none today still
	// fails here. The order matters: the two deletes come last because they remove what the gets read.
	bodies := []struct {
		op   string
		body map[string]any
	}{
		{"CreateApplication", map[string]any{"applicationName": "dates-app-2"}},
		{"GetApplication", map[string]any{"applicationName": app}},
		{"ListApplications", nil},
		{"CreateDeploymentGroup", map[string]any{
			"applicationName":     app,
			"deploymentGroupName": "dates-group-2",
			"serviceRoleArn":      "arn:aws:iam::123456789012:role/codedeploy-dates",
		}},
		{"GetDeploymentGroup", map[string]any{"applicationName": app, "deploymentGroupName": group}},
		{"CreateDeployment", map[string]any{"applicationName": app, "deploymentGroupName": group}},
		{"GetDeployment", map[string]any{"deploymentId": deploymentID}},
		{"DeleteDeploymentGroup", map[string]any{"applicationName": app, "deploymentGroupName": group}},
		{"DeleteApplication", map[string]any{"applicationName": app}},
	}

	for _, tc := range bodies {
		t.Run(tc.op, func(t *testing.T) {
			body := codedeployDatesRaw(t, p, ctx, tc.op, tc.body)
			require.NotContainsf(t, string(body), rfc3339,
				"%s rendered a date as RFC3339; CodeDeploy publishes every Timestamp as a number: %s", tc.op, body)
		})
	}
}
