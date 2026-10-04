package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFormation's stack, change-set and drift-detection keys are scoped by account and Region
// (#1366). A stack name is unique per account per Region, and before the scope two accounts, or two
// Regions of one account, that each created `app` addressed one record.

// cfnScopeTemplate is a resourceless template whose Description names the scope that created it, so
// a read can tell which stack it was handed.
func cfnScopeTemplate(scope string) string {
	return `{"Description":"` + scope + `","Resources":{}}`
}

func TestCFNScope_OneStackNameExistsOncePerAccountAndRegion(t *testing.T) {
	ts := newCFNIdentityTestServer(t)
	scopes := []struct{ account, region string }{
		{"", "us-east-1"},
		{cfnOtherAccount, "us-east-1"},
		{"", "eu-west-1"},
		{cfnOtherAccount, "eu-west-1"},
	}
	call := func(account, region string, params map[string]string) (int, string) {
		t.Helper()
		params["Version"] = "2010-05-15"
		return cfnIdentityRequest(t, ts, "cloudformation", region, account, params)
	}

	for _, sc := range scopes {
		label := sc.account + "@" + sc.region
		code, body := call(sc.account, sc.region, map[string]string{
			"Action": "CreateStack", "StackName": "app", "TemplateBody": cfnScopeTemplate(label),
		})
		require.Equalf(t, http.StatusOK, code, "CreateStack app in %s must not collide with another scope's: %s", label, body)
		code, body = call(sc.account, sc.region, map[string]string{
			"Action": "CreateChangeSet", "StackName": "app", "ChangeSetName": "cs", "ChangeSetType": "UPDATE",
			"TemplateBody": cfnScopeTemplate(label + "-next"),
		})
		require.Equalf(t, http.StatusOK, code, "CreateChangeSet in %s: %s", label, body)
	}

	for _, sc := range scopes {
		label := sc.account + "@" + sc.region
		t.Run(label, func(t *testing.T) {
			code, body := call(sc.account, sc.region, map[string]string{"Action": "GetTemplate", "StackName": "app"})
			require.Equal(t, http.StatusOK, code, "GetTemplate: %s", body)
			assert.Contains(t, body, label, "GetTemplate must answer this scope's stack: %s", body)

			code, body = call(sc.account, sc.region, map[string]string{"Action": "ListStacks"})
			require.Equal(t, http.StatusOK, code, "ListStacks: %s", body)
			assert.Equal(t, 1, strings.Count(body, "<StackName>app</StackName>"), "ListStacks must list this scope's one stack: %s", body)

			code, body = call(sc.account, sc.region, map[string]string{"Action": "DescribeChangeSet", "StackName": "app", "ChangeSetName": "cs"})
			require.Equal(t, http.StatusOK, code, "DescribeChangeSet: %s", body)
			account := sc.account
			if account == "" {
				account = "123456789012"
			}
			assert.Contains(t, body, "<ChangeSetId>arn:aws:cloudformation:"+sc.region+":"+account+":changeSet/cs/",
				"DescribeChangeSet must answer this scope's change set: %s", body)

			code, body = call(sc.account, sc.region, map[string]string{"Action": "ListChangeSets", "StackName": "app"})
			require.Equal(t, http.StatusOK, code, "ListChangeSets: %s", body)
			assert.Equal(t, 1, strings.Count(body, "<ChangeSetName>cs</ChangeSetName>"), "ListChangeSets: %s", body)
		})
	}

	// Deleting one scope's stack leaves the other three.
	code, body := call(cfnOtherAccount, "eu-west-1", map[string]string{"Action": "DeleteStack", "StackName": "app"})
	require.Equal(t, http.StatusOK, code, "DeleteStack: %s", body)
	for _, sc := range scopes[:3] {
		code, body := call(sc.account, sc.region, map[string]string{"Action": "DescribeStacks", "StackName": "app"})
		require.Equalf(t, http.StatusOK, code, "%s@%s lost its stack when another scope deleted its own: %s", sc.account, sc.region, body)
	}
	code, _ = call(cfnOtherAccount, "eu-west-1", map[string]string{"Action": "DescribeStacks", "StackName": "app"})
	assert.Equal(t, http.StatusBadRequest, code, "the deleted scope's stack is gone")
}

// newCFNScopeDeployer builds a deployer over state in the given scope, with no plugins registered,
// which is enough for resourceless templates.
func newCFNScopeDeployer(state emulator.StateManager, account, region string) *emulator.StackDeployer {
	return emulator.NewStackDeployer(emulator.NewPluginRegistry(), nil, state,
		emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
		emulator.NewDefaultLogger(slog.LevelError, false), nil,
		emulator.WithDeployerIdentity(account, region))
}

// cfnScopeSeedLegacy writes the unscoped records substrate wrote before #1366: a stack in
// 123456789012/us-east-1, its names index, one change set and one drift detection.
func cfnScopeSeedLegacy(t *testing.T, state emulator.StateManager) {
	t.Helper()
	ctx := context.Background()
	stack, err := json.Marshal(map[string]any{
		"StackName": "legacy", "Status": "CREATE_COMPLETE", "TemplateBody": `{"Resources":{}}`,
		"AccountID": "123456789012", "Region": "us-east-1",
	})
	require.NoError(t, err)
	cs, err := json.Marshal(map[string]any{
		"ChangeSetName": "old-cs", "StackName": "legacy", "Status": "CREATE_COMPLETE", "TemplateBody": `{"Resources":{}}`,
	})
	require.NoError(t, err)
	drift, err := json.Marshal(map[string]any{
		"StackDriftDetectionId": "0f0f0f0f-0000-4000-8000-000000000000", "StackId": "legacy",
		"DetectionStatus": "DETECTION_COMPLETE",
	})
	require.NoError(t, err)
	for key, value := range map[string][]byte{
		"stack:legacy":                    stack,
		"stack_names":                     []byte(`["legacy"]`),
		"changeset:legacy/old-cs":         cs,
		"changeset_names:legacy":          []byte(`["old-cs"]`),
		"drift_detection:" + cfnLegacyDID: drift,
	} {
		require.NoError(t, state.Put(ctx, "cfn", key, value), "seed %s", key)
	}
}

const cfnLegacyDID = "0f0f0f0f-0000-4000-8000-000000000000"

func TestCFNScope_ARecordWrittenBeforeTheScopeIsReadInItsOwnScopeOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := emulator.NewMemoryStateManager()
	cfnScopeSeedLegacy(t, state)

	own := newCFNScopeDeployer(state, "123456789012", "us-east-1")
	stacks, err := own.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1, "the legacy stack is listed in its own scope")
	sets, err := own.ListChangeSets(ctx, "legacy")
	require.NoError(t, err)
	require.Len(t, sets, 1, "its change set is listed")
	_, err = own.DescribeChangeSet(ctx, "legacy", "old-cs")
	require.NoError(t, err, "its change set is described")
	_, err = own.DescribeStackDriftDetectionStatus(ctx, cfnLegacyDID)
	require.NoError(t, err, "its drift detection is described")

	for _, other := range []struct{ account, region string }{{"555566667777", "us-east-1"}, {"123456789012", "eu-west-1"}} {
		d := newCFNScopeDeployer(state, other.account, other.region)
		stacks, err := d.ListStacks(ctx)
		require.NoError(t, err)
		assert.Empty(t, stacks, "%s/%s must not see another scope's legacy stack", other.account, other.region)
		_, err = d.DescribeChangeSet(ctx, "legacy", "old-cs")
		assert.Error(t, err, "%s/%s must not see another scope's legacy change set", other.account, other.region)
		_, err = d.DescribeStackDriftDetectionStatus(ctx, cfnLegacyDID)
		assert.Error(t, err, "%s/%s must not see another scope's legacy drift detection", other.account, other.region)
	}
}

func TestCFNScope_TheFirstWriteMigratesALegacyStack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := emulator.NewMemoryStateManager()
	cfnScopeSeedLegacy(t, state)
	d := newCFNScopeDeployer(state, "123456789012", "us-east-1")

	_, err := d.CreateChangeSet(ctx, "legacy", "new-cs", `{"Resources":{}}`, nil, nil)
	require.NoError(t, err, "a write to a legacy stack")

	const scope = "123456789012/us-east-1/"
	for _, key := range []string{"stack:" + scope + "legacy", "changeset:" + scope + "legacy/old-cs",
		"changeset:" + scope + "legacy/new-cs", "drift_detection:" + scope + cfnLegacyDID} {
		data, err := state.Get(ctx, "cfn", key)
		require.NoError(t, err)
		assert.NotNil(t, data, "migrated to %s", key)
	}
	for _, key := range []string{"stack:legacy", "changeset:legacy/old-cs", "changeset_names:legacy",
		"drift_detection:" + cfnLegacyDID} {
		data, err := state.Get(ctx, "cfn", key)
		require.NoError(t, err)
		assert.Nil(t, data, "the legacy %s is removed by the migration", key)
	}
	names, err := state.Get(ctx, "cfn", "stack_names")
	require.NoError(t, err)
	assert.JSONEq(t, `[]`, string(names), "the legacy index no longer names the stack")

	sets, err := d.ListChangeSets(ctx, "legacy")
	require.NoError(t, err)
	assert.Len(t, sets, 2, "both change sets survive the migration")
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Len(t, stacks, 1, "the stack is listed once, not under both keys")

	require.NoError(t, d.DeleteStack(ctx, "legacy"))
	stacks, err = d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks, "a migrated stack deletes like any other")
}

// A store fault on any read or write the scoped keys add is an error, never a stack that is absent or
// a write that succeeded.
func TestCFNScope_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		run  func(context.Context, *emulator.StackDeployer) error
	}{
		{"ListStacks, legacy stack read", func(m *cfFaultStateManager) { m.failGet = "stack:legacy" },
			func(ctx context.Context, d *emulator.StackDeployer) error { _, err := d.ListStacks(ctx); return err }},
		{"ListStacks, legacy index corrupt", func(m *cfFaultStateManager) { m.corruptGet = "stack_names" },
			func(ctx context.Context, d *emulator.StackDeployer) error { _, err := d.ListStacks(ctx); return err }},
		{"DescribeChangeSet, legacy change set read", func(m *cfFaultStateManager) { m.failGet = "changeset:legacy/" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				_, err := d.DescribeChangeSet(ctx, "legacy", "old-cs")
				return err
			}},
		{"ListChangeSets, legacy index read", func(m *cfFaultStateManager) { m.failGet = "changeset_names:legacy" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				_, err := d.ListChangeSets(ctx, "legacy")
				return err
			}},
		{"DescribeStackDriftDetectionStatus, legacy read", func(m *cfFaultStateManager) { m.failGet = "drift_detection:" + cfnLegacyDID },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				_, err := d.DescribeStackDriftDetectionStatus(ctx, cfnLegacyDID)
				return err
			}},
		{"migration, stack write", func(m *cfFaultStateManager) { m.failPut = "stack:123456789012/" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				return d.DeleteChangeSet(ctx, "legacy", "old-cs")
			}},
		{"migration, change set write", func(m *cfFaultStateManager) { m.failPut = "changeset:123456789012/" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				return d.DeleteChangeSet(ctx, "legacy", "old-cs")
			}},
		{"migration, legacy change set delete", func(m *cfFaultStateManager) { m.failDelete = "changeset:legacy/" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				_, err := d.CreateChangeSet(ctx, "legacy", "new-cs", `{"Resources":{}}`, nil, nil)
				return err
			}},
		{"migration, drift detection write", func(m *cfFaultStateManager) { m.failPut = "drift_detection:123456789012/" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				return d.DeleteChangeSet(ctx, "legacy", "old-cs")
			}},
		{"migration, legacy stack delete", func(m *cfFaultStateManager) { m.failDelete = "stack:legacy" },
			func(ctx context.Context, d *emulator.StackDeployer) error { return d.DeleteStack(ctx, "legacy") }},
		{"migration, legacy index write", func(m *cfFaultStateManager) { m.failPut = "stack_names" },
			func(ctx context.Context, d *emulator.StackDeployer) error {
				return d.DeleteChangeSet(ctx, "legacy", "old-cs")
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			cfnScopeSeedLegacy(t, fault)
			tc.arm(fault)
			err := tc.run(context.Background(), newCFNScopeDeployer(fault, "123456789012", "us-east-1"))
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			for _, notFound := range []error{emulator.ErrCFNStackNotFound, emulator.ErrCFNChangeSetNotFound, emulator.ErrCFNDriftDetectionNotFound} {
				assert.Falsef(t, errors.Is(err, notFound), "%s answered a store fault as an absence: %v", tc.name, err)
			}
		})
	}
}
