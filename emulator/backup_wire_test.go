package emulator_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for Backup's three records
// (#756).
//
// Six baseline lines, two per record: BackupVault, BackupPlan and BackupSelection each declare
// `json:"AccountID"` and `json:"Region"`. Neither carries `,omitempty` on any of the three, so no
// record can lose a member by holding its zero value — which is why there is no counterpart here to
// the ever_tagged setup emulator/stepfunctions_wire_test.go needs. Backup routes no tag operation at
// all and no record declares EverTagged, so #1304's vacuity trap does not arise.
//
// Two of the twelve operations needed a projection written before this file could say anything.
// describeBackupVault and listBackupVaults handed the persisted BackupVault to the marshaller;
// emulator/backup_wire.go's backupVaultOut is what they answer now, and #1325 shipped it with #1324,
// whose date conversion could not reach either site while the record was being marshaled whole. The
// plan and selection handlers were already building their responses member by member, so for those
// two records the map *is* the projection and what was missing was only this citation.
//
// # Why this asserts absence rather than exact membership
//
// emulator/rds_wire_test.go, the strongest form in the suite, walks into the element that holds a
// record and requires its members to be *exactly* the published ones. That form is not taken here:
// DescribeBackupVault answers five of API_DescribeBackupVault's seventeen members and
// ListBackupVaults five of API_BackupVaultListMember's thirteen, so an exact-membership expectation
// would pin that gap and have to be rewritten by the PR that closes it (#1199). The absence walk is
// indifferent to it, states the one thing the projected inventory needs, and is the form
// emulator/configservice_wire_test.go, emulator/redshift_wire_test.go and
// emulator/stepfunctions_wire_test.go already use.
//
// # Why the three deletes are driven anyway
//
// DeleteBackupVault and DeleteBackupSelection each answer `{}`, as their pages publish, and read
// nothing off the record they address, so there is nothing a projection could leak and no record
// member to anchor on. DeleteBackupPlan answers its four published members since #1206's survey
// (#1177), built member by member from the plan it deletes, and anchors on BackupPlanId. All three
// are driven because "every response that answers the record" is what the projected file
// asks for, and a reader should not have to work out whether the list of twelve is complete or a
// sample; their anchor is the empty document itself. A control run confirms the walk would catch a
// leak at each of the three if one were added, so the subtests are regression guards rather than
// decoration.

// backupWireClock is the instant every fixture below starts the simulated clock at.
//
// A seeded baseline rather than setupBackupPlugin's time.Now(), so nothing here reads the wall clock.
// No assertion below equates a timestamp: TimeController.Now advances from its baseline by the wall
// time elapsed since it was set, and pinning the *rendering* of a Backup date is
// emulator/backup_dates_test.go's job (#1324), which freezes the clock for exactly that reason. This
// file only walks member names.
var backupWireClock = time.Unix(1700000000, 0).UTC()

// backupWireAccount and backupWireRegion scope every state key the plugin writes, and are the two
// segments every backup ARN below is built from.
const (
	backupWireAccount = "123456789012"
	backupWireRegion  = "us-east-1"
)

// backupBookkeepingMembers are the members the three Backup records declare and no Backup shape
// publishes, each listed once in the spelling the stored record uses.
//
// Compared case-insensitively by backupWireAssertNoBookkeepingMember, because a leak could arrive
// under either spelling: the records declare `json:"AccountID"` and `json:"Region"` — the Go
// identifier, capitalized — while every published member of every Backup shape is UpperCamelCase, so
// a fold is what catches a projection that spelled either one differently. A fold is an equality
// rather than a substring test, so the account and the Region appearing *inside* an ARN value is not
// a collision; that is also why each assertion is on the member name and never on the body as a
// whole.
var backupBookkeepingMembers = []string{"AccountID", "Region"}

// setupBackupWirePlugin returns the Backup plugin, a request context and the state manager behind it.
//
// The state manager is handed back because half of what each test asserts is that the record keeps
// the members its responses drop — and a record is the only place either can be read from, neither
// having a published home to read it back through.
//
// Its own harness rather than setupBackupPlugin, which seeds the clock from time.Now(), returns no
// state handle and mints no IDs. The ID mint is set because CreateBackupPlan draws twice and
// CreateBackupSelection once; the plan ID is then needed in the URL path of five operations below.
func setupBackupWirePlugin(t *testing.T) (*emulator.BackupPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.BackupPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(backupWireClock)},
	}), "emulator.BackupPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: backupWireAccount,
		Region:    backupWireRegion,
		RequestID: "req-backup-wire",
		IDs:       emulator.NewIDMint("req-backup-wire"),
	}, state
}

// backupWireRaw issues one operation and returns the raw response body, failing the test on anything
// but 200.
//
// The raw bytes are the point of this file. Every other Backup test decodes into a Go struct, which
// is exactly the step that hides a member the caller never asked about — emulator/backup_plugin_test.go
// decoded ListBackupVaults into []emulator.BackupVault, the record type, for as long as the record
// was what the site answered.
func backupWireRaw(t *testing.T, p *emulator.BackupPlugin, ctx *emulator.RequestContext, site, method, path string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, backupRequest(t, method, path, body))
	require.NoError(t, err, "%s", site)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", site, resp.Body)
	return resp.Body
}

// backupWireAssertNoBookkeepingMember fails if any member in the response document is named for a
// bookkeeping member, at any depth, reporting the path so a failure names the member rather than the
// service.
//
// The whole document rather than a record's subtree, which is also strictly stronger: a member
// rendered on the envelope rather than on the record would fail here and pass a subtree walk. That
// matters for Backup specifically, because GetBackupPlan and GetBackupSelection nest the record's
// published members under BackupPlan and BackupSelection while answering others flat, so "the
// record's subtree" is not one place.
func backupWireAssertNoBookkeepingMember(t *testing.T, site string, body []byte) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal(body, &doc), "%s answered undecodable JSON: %s", site, body)
	backupWireWalkMembers(t, site, "$", doc)
}

// backupWireWalkMembers recurses through a decoded JSON document asserting on every member name it
// meets.
func backupWireWalkMembers(t *testing.T, site, path string, node any) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			child := path + "." + key
			for _, member := range backupBookkeepingMembers {
				assert.Falsef(t, strings.EqualFold(key, member),
					"%s answered %s: %s is substrate's bookkeeping and no Backup shape publishes it",
					site, child, member)
			}
			backupWireWalkMembers(t, site, child, value)
		}
	case []any:
		for i, value := range typed {
			backupWireWalkMembers(t, site, fmt.Sprintf("%s[%d]", path, i), value)
		}
	}
}

// backupWireRequireScoped requires that the stored record at key carries both scope members.
//
// This is the presence anchor the absence assertions need on the record side: an assertion that a
// response omits a member its record never held would pass without testing anything. The record is
// read as raw JSON rather than through its Go type, so a member with no published home can be
// observed without a struct deciding which members exist. Neither member carries `,omitempty` on any
// of the three records, so a record found at all has both.
func backupWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "backup", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+backupWireAccount+`"`, string(record["AccountID"]),
		"%s must persist AccountID before an absence assertion on it means anything", key)
	require.JSONEq(t, `"`+backupWireRegion+`"`, string(record["Region"]),
		"%s must persist Region before an absence assertion on it means anything", key)
}

// backupWireARN builds a backup ARN of the given resource type, which is also where the account and
// the Region legitimately appear in every body below.
func backupWireARN(resourceType, name string) string {
	return "arn:aws:backup:" + backupWireRegion + ":" + backupWireAccount + ":" + resourceType + ":" + name
}

// backupWireCase is one operation driven by one of the three tests below.
//
// An empty method reuses a response the test already holds, rather than issuing the create a second
// time; site is spelled out rather than derived, because two cases in the plan test drive the same
// path down different verbs and a Go subtest name has to distinguish them.
type backupWireCase struct {
	site   string
	method string
	path   string
	body   map[string]any
	held   []byte
	anchor string
}

// backupWireRunCase drives one case as a subtest: the presence anchor first, so a body that did not
// render the record fails as a missing anchor rather than passing as an absence, then the walk.
func backupWireRunCase(t *testing.T, p *emulator.BackupPlugin, ctx *emulator.RequestContext, tc backupWireCase) {
	t.Helper()
	t.Run(tc.site, func(t *testing.T) {
		body := tc.held
		if tc.method != "" {
			body = backupWireRaw(t, p, ctx, tc.site, tc.method, tc.path, tc.body)
		}
		require.Containsf(t, string(body), tc.anchor,
			"presence anchor: %s has to render %s for an absence to mean anything", tc.site, tc.anchor)
		backupWireAssertNoBookkeepingMember(t, tc.site, body)
	})
}

func TestBackupWire_VaultResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupBackupWirePlugin(t)

	const name = "wire-vault"
	arn := backupWireARN("backup-vault", name)

	created := backupWireRaw(t, p, ctx, "CreateBackupVault", "PUT", "/backup-vaults/"+name, map[string]any{
		"EncryptionKeyArn": "arn:aws:kms:" + backupWireRegion + ":" + backupWireAccount + ":key/wire",
	})
	require.Contains(t, string(created), arn,
		"CreateBackupVault must report the ARN, so the absence assertion is about the member name")
	backupWireRequireScoped(t, state, "vault:"+backupWireAccount+"/"+backupWireRegion+"/"+name)

	for _, tc := range []backupWireCase{
		{site: "CreateBackupVault", held: created, anchor: arn},
		{site: "DescribeBackupVault", method: "GET", path: "/backup-vaults/" + name,
			anchor: `"BackupVaultName":"` + name + `"`},
		{site: "ListBackupVaults", method: "GET", path: "/backup-vaults",
			anchor: `"BackupVaultArn":"` + arn + `"`},
		// Last, and answers an empty object: it removes the record every case above reads.
		{site: "DeleteBackupVault", method: "DELETE", path: "/backup-vaults/" + name, anchor: "{}"},
	} {
		backupWireRunCase(t, p, ctx, tc)
	}
}

func TestBackupWire_PlanResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupBackupWirePlugin(t)

	const name = "wire-plan"
	created := backupWireRaw(t, p, ctx, "CreateBackupPlan", "PUT", "/backup/plans", map[string]any{
		// Rules are stored verbatim as recorded intent and answered back under BackupPlan, so one is
		// sent: it is the only part of a plan response that is caller-supplied, and the walk has to
		// see it to be walking the whole document.
		"BackupPlan": map[string]any{
			"BackupPlanName": name,
			"Rules": []map[string]any{{
				"RuleName":           "wire-rule",
				"TargetBackupVault":  "wire-vault",
				"ScheduleExpression": "cron(0 5 ? * * *)",
			}},
		},
	})

	var plan struct {
		BackupPlanID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(created, &plan), "decode CreateBackupPlan: %s", created)
	require.NotEmpty(t, plan.BackupPlanID, "CreateBackupPlan must report a plan id")

	arn := backupWireARN("backup-plan", plan.BackupPlanID)
	require.Contains(t, string(created), arn,
		"CreateBackupPlan must report the ARN, so the absence assertion is about the member name")
	backupWireRequireScoped(t, state, "plan:"+backupWireAccount+"/"+backupWireRegion+"/"+plan.BackupPlanID)

	for _, tc := range []backupWireCase{
		{site: "CreateBackupPlan", held: created, anchor: arn},
		{site: "GetBackupPlan", method: "GET", path: "/backup/plans/" + plan.BackupPlanID,
			anchor: `"BackupPlanName":"` + name + `"`},
		{site: "ListBackupPlans", method: "GET", path: "/backup/plans",
			anchor: `"BackupPlanName":"` + name + `"`},
		// Same path as GetBackupPlan, different verb — which is why the case carries its own site
		// name. It reads BackupPlanId and BackupPlanArn off the record, so it has a record member to
		// anchor on; the rename is after the two reads above, which assert the created name.
		{site: "UpdateBackupPlan", method: "POST", path: "/backup/plans/" + plan.BackupPlanID,
			body:   map[string]any{"BackupPlan": map[string]any{"BackupPlanName": name + "-renamed"}},
			anchor: `"BackupPlanArn":"` + arn + `"`},
		// Last, and answers an empty object: it removes the record every case above reads.
		{site: "DeleteBackupPlan", method: "DELETE", path: "/backup/plans/" + plan.BackupPlanID, anchor: `"BackupPlanId":"` + plan.BackupPlanID + `"`},
	} {
		backupWireRunCase(t, p, ctx, tc)
	}
}

func TestBackupWire_SelectionResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupBackupWirePlugin(t)

	// CreateBackupSelection refuses an unknown plan, so the plan comes first. Its own responses are
	// the plan test's subject; here it is setup.
	createdPlan := backupWireRaw(t, p, ctx, "CreateBackupPlan", "PUT", "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "wire-selection-plan"},
	})
	var plan struct {
		BackupPlanID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(createdPlan, &plan), "decode CreateBackupPlan: %s", createdPlan)

	const name = "wire-selection"
	selections := "/backup/plans/" + plan.BackupPlanID + "/selections"
	created := backupWireRaw(t, p, ctx, "CreateBackupSelection", "PUT", selections, map[string]any{
		"BackupSelection": map[string]any{
			"SelectionName": name,
			"IamRoleArn":    "arn:aws:iam::" + backupWireAccount + ":role/backup-wire",
			"Resources":     []string{"arn:aws:ec2:" + backupWireRegion + ":" + backupWireAccount + ":volume/vol-wire"},
		},
	})

	var selection struct {
		SelectionID string `json:"SelectionId"`
	}
	require.NoError(t, json.Unmarshal(created, &selection), "decode CreateBackupSelection: %s", created)
	require.NotEmpty(t, selection.SelectionID, "CreateBackupSelection must report a selection id")

	backupWireRequireScoped(t, state,
		"selection:"+backupWireAccount+"/"+backupWireRegion+"/"+plan.BackupPlanID+"/"+selection.SelectionID)

	one := selections + "/" + selection.SelectionID
	for _, tc := range []backupWireCase{
		// A selection has no ARN of its own, so the anchor is the plan it was filed under: a member
		// read off the record, which is what an anchor has to be.
		{site: "CreateBackupSelection", held: created, anchor: `"BackupPlanId":"` + plan.BackupPlanID + `"`},
		{site: "GetBackupSelection", method: "GET", path: one, anchor: `"SelectionName":"` + name + `"`},
		// Last, and answers an empty object: it removes the record every case above reads.
		{site: "DeleteBackupSelection", method: "DELETE", path: one, anchor: "{}"},
	} {
		backupWireRunCase(t, p, ctx, tc)
	}
}
