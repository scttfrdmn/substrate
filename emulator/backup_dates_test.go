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

// Every date AWS Backup publishes is a Unix timestamp, and these assertions are on the raw
// response bytes because that is the only place the difference is visible (#1324).
//
// A typed decoder cannot tell 1516925490.087 from "2018-01-26T00:11:30.087Z": both land in a
// time.Time holding the same instant, and emulator.EpochSeconds accepts either on the way in. That
// is how nine sites agreed on the wrong form unnoticed — emulator/backup_plugin_test.go decoded
// ListBackupVaults into []emulator.BackupVault, the record type, so the form it asserted was the
// record's and not the API's. So the test reads the bytes, as emulator/iam_shape_members_test.go
// does.
//
// The instant is AWS's own worked example, and the clock is frozen, so the expectation is an exact
// string rather than a tolerance: API_DescribeBackupVault and the seven other pages below each
// gloss their date as "in Unix format and Coordinated Universal Time (UTC) … accurate to
// milliseconds. For example, the value 1516925490.087 represents Friday, January 26, 2018
// 12:11:30.087 AM", and emulator.EpochSeconds renders exactly three decimals. Freezing is what
// makes that assertable: TimeController.Now otherwise advances from its baseline by the wall time
// elapsed since it was set, and no test may read the wall clock.

// backupDatesClock is AWS's documented example instant, 1516925490.087.
var backupDatesClock = time.Unix(1516925490, 87000000).UTC()

// backupDatesRendered is the rendering every published Backup date must take.
const backupDatesRendered = "1516925490.087"

// setupBackupDatesPlugin returns the Backup plugin on a frozen clock set to backupDatesClock.
//
// Its own harness rather than setupBackupPlugin, which seeds the clock from time.Now and leaves it
// running, and which mints no IDs — a plan ID is needed in a URL path by five of the operations
// below.
func setupBackupDatesPlugin(t *testing.T) (*emulator.BackupPlugin, *emulator.RequestContext) {
	t.Helper()
	tc := emulator.NewTimeController(backupDatesClock)
	// Freeze then SetTime, in that order, which is the ordering TimeController.Freeze documents:
	// Freeze alone stops the clock where it currently *reads*, which is the baseline plus the tens
	// of microseconds since NewTimeController, and that drift is visible in a rendered date.
	tc.Freeze()
	tc.SetTime(backupDatesClock)
	p := &emulator.BackupPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   emulator.NewMemoryStateManager(),
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.BackupPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-backup-dates",
		IDs:       emulator.NewIDMint("req-backup-dates"),
	}
}

// backupDatesRaw issues one request and returns the raw response body, failing on anything but 200.
func backupDatesRaw(t *testing.T, p *emulator.BackupPlugin, ctx *emulator.RequestContext, site, method, path string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, backupRequest(t, method, path, body))
	require.NoError(t, err, "%s", site)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", site, resp.Body)
	return resp.Body
}

// backupDatesRequireUnix fails unless body renders member as the published Unix value and nowhere
// renders it as a string.
//
// Both halves are needed. The first pins the value and the precision; the second is what catches a
// *nested* date left behind, since a list element or a nested BackupPlan member would otherwise
// satisfy the first through its sibling at the top level.
func backupDatesRequireUnix(t *testing.T, site, member string, body []byte) {
	t.Helper()
	require.Containsf(t, string(body), `"`+member+`":`+backupDatesRendered,
		"%s must answer %s as the published Unix value: %s", site, member, body)
	require.NotContainsf(t, string(body), `"`+member+`":"`,
		"%s answered %s as a string, and every Backup page publishes a Unix number: %s", site, member, body)
}

func TestBackupDates_PublishedDatesAreUnixTimestamps(t *testing.T) {
	t.Parallel()
	p, ctx := setupBackupDatesPlugin(t)

	const vault = "dates-vault"
	createdVault := backupDatesRaw(t, p, ctx, "CreateBackupVault", "PUT", "/backup-vaults/"+vault, map[string]any{
		"EncryptionKeyArn": "arn:aws:kms:us-east-1:123456789012:key/dates",
	})

	createdPlan := backupDatesRaw(t, p, ctx, "CreateBackupPlan", "POST", "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "dates-plan"},
	})
	var plan struct {
		BackupPlanID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(createdPlan, &plan), "decode CreateBackupPlan: %s", createdPlan)
	require.NotEmpty(t, plan.BackupPlanID, "CreateBackupPlan must report a plan id")

	createdSelection := backupDatesRaw(t, p, ctx, "CreateBackupSelection", "POST",
		"/backup/plans/"+plan.BackupPlanID+"/selections", map[string]any{
			"BackupSelection": map[string]any{
				"SelectionName": "dates-selection",
				"IamRoleArn":    "arn:aws:iam::123456789012:role/backup-dates",
				"Resources":     []string{"arn:aws:ec2:us-east-1:123456789012:volume/vol-1"},
			},
		})
	var selection struct {
		SelectionID string `json:"SelectionId"`
	}
	require.NoError(t, json.Unmarshal(createdSelection, &selection), "decode CreateBackupSelection: %s", createdSelection)
	require.NotEmpty(t, selection.SelectionID, "CreateBackupSelection must report a selection id")

	// A nil body reuses the create response the test already holds rather than issuing the create
	// a second time. Every one of the eight sites that answers a published date is here; the
	// member is named because a list answers it inside an element.
	for _, tc := range []struct {
		site   string
		member string
		method string
		path   string
		body   []byte
	}{
		{"CreateBackupVault", "CreationDate", "", "", createdVault},
		{"DescribeBackupVault", "CreationDate", "GET", "/backup-vaults/" + vault, nil},
		{"ListBackupVaults", "CreationDate", "GET", "/backup-vaults", nil},
		{"CreateBackupPlan", "CreationDate", "", "", createdPlan},
		{"GetBackupPlan", "CreationDate", "GET", "/backup/plans/" + plan.BackupPlanID, nil},
		{"ListBackupPlans", "CreationDate", "GET", "/backup/plans", nil},
		{"CreateBackupSelection", "CreationDate", "", "", createdSelection},
		{"GetBackupSelection", "CreationDate", "GET",
			"/backup/plans/" + plan.BackupPlanID + "/selections/" + selection.SelectionID, nil},
		// #1177: UpdateBackupPlan answers the plan's published CreationDate, where it answered an
		// UpdatedAt no AWS Backup page publishes.
		{"UpdateBackupPlan", "CreationDate", "", "", backupDatesRaw(t, p, ctx, "UpdateBackupPlan", "POST",
			"/backup/plans/"+plan.BackupPlanID, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "dates-plan-renamed"}})},
	} {
		t.Run(tc.site, func(t *testing.T) {
			body := tc.body
			if body == nil {
				body = backupDatesRaw(t, p, ctx, tc.site, tc.method, tc.path, nil)
			}
			backupDatesRequireUnix(t, tc.site, tc.member, body)
		})
	}
}

func TestBackupDates_NoResponseRendersAnRFC3339Date(t *testing.T) {
	t.Parallel()
	p, ctx := setupBackupDatesPlugin(t)

	// The same instant written the way a time.Time marshals it. A substring search for this is the
	// cheap catch-all: it fails on any member, nested at any depth and under any name, that was
	// left a time.Time — which is the whole class of defect #1324 reported.
	rfc3339 := backupDatesClock.Format(time.RFC3339Nano)

	const vault = "dates-vault"
	bodies := map[string][]byte{
		"CreateBackupVault": backupDatesRaw(t, p, ctx, "CreateBackupVault", "PUT", "/backup-vaults/"+vault, nil),
		"CreateBackupPlan": backupDatesRaw(t, p, ctx, "CreateBackupPlan", "POST", "/backup/plans", map[string]any{
			"BackupPlan": map[string]any{"BackupPlanName": "dates-plan"},
		}),
	}
	var plan struct {
		BackupPlanID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(bodies["CreateBackupPlan"], &plan), "decode CreateBackupPlan")

	bodies["CreateBackupSelection"] = backupDatesRaw(t, p, ctx, "CreateBackupSelection", "POST",
		"/backup/plans/"+plan.BackupPlanID+"/selections", map[string]any{
			"BackupSelection": map[string]any{"SelectionName": "dates-selection"},
		})
	var selection struct {
		SelectionID string `json:"SelectionId"`
	}
	require.NoError(t, json.Unmarshal(bodies["CreateBackupSelection"], &selection), "decode CreateBackupSelection")

	bodies["DescribeBackupVault"] = backupDatesRaw(t, p, ctx, "DescribeBackupVault", "GET", "/backup-vaults/"+vault, nil)
	bodies["ListBackupVaults"] = backupDatesRaw(t, p, ctx, "ListBackupVaults", "GET", "/backup-vaults", nil)
	bodies["GetBackupPlan"] = backupDatesRaw(t, p, ctx, "GetBackupPlan", "GET", "/backup/plans/"+plan.BackupPlanID, nil)
	bodies["ListBackupPlans"] = backupDatesRaw(t, p, ctx, "ListBackupPlans", "GET", "/backup/plans", nil)
	bodies["GetBackupSelection"] = backupDatesRaw(t, p, ctx, "GetBackupSelection", "GET",
		"/backup/plans/"+plan.BackupPlanID+"/selections/"+selection.SelectionID, nil)

	for site, body := range bodies {
		require.NotContainsf(t, string(body), rfc3339,
			"%s rendered a date as RFC3339; every Backup page publishes Unix: %s", site, body)
	}

	// UpdateBackupPlan answers the published CreationDate now (#1177), so it is swept like the rest
	// and must not carry the unpublished UpdatedAt.
	updated := backupDatesRaw(t, p, ctx, "UpdateBackupPlan", "POST", "/backup/plans/"+plan.BackupPlanID, nil)
	require.NotContainsf(t, string(updated), rfc3339, "UpdateBackupPlan rendered a date as RFC3339: %s", updated)
	require.NotContainsf(t, string(updated), `"UpdatedAt"`, "UpdateBackupPlan answered the unpublished UpdatedAt: %s", updated)
}
