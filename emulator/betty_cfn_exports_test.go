package emulator_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The two stacks every cross-stack test below deploys: a network stack exporting
// a bucket name under a stack-namespaced export name, and an app stack importing
// it. The export name is built with Fn::Sub over AWS::StackName because that is
// the documented idiom — an export name has to be unique per account and Region,
// so a template that hard-codes one cannot be deployed twice.
const (
	cfnExportingTemplate = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-export-bucket"}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"Data"},` +
		`"Export":{"Name":{"Fn::Sub":"${AWS::StackName}-BucketName"}}}}}`

	cfnImportingTemplate = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue",` +
		`"Properties":{"QueueName":{"Fn::ImportValue":"net-BucketName"}}}}}`
)

// TestCFN_ImportValueResolvesAnotherStacksExport is #522's first gate: an
// Fn::ImportValue of another stack's export resolves.
//
// Before this it fell through resolveValue's JSON-encoding fallback, so the queue
// was named with the literal `{"Fn::ImportValue":"net-BucketName"}` and the stack
// still reported CREATE_COMPLETE — the silent-success shape the issue is about.
func TestCFN_ImportValueResolvesAnotherStacksExport(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)

	result, err := d.Deploy(ctx, cfnImportingTemplate, "app", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error,
		"the import must resolve, not fail the resource")
	assert.Equal(t, "cfn-export-bucket", result.Resources[0].PhysicalID,
		"the queue is named with the imported value, not the intrinsic's JSON")
}

// TestCFN_ImportValueOfAnUnknownExportFailsTheResource pins the other half: an
// import naming an export nothing published fails the resource with a reason,
// rather than resolving to the empty string or to the intrinsic's JSON.
//
// Both of those alternatives are values that look resolved and are not, which is
// exactly what #522 filed. A CREATE_FAILED with the export name in the reason is
// the observable a caller can act on.
func TestCFN_ImportValueOfAnUnknownExportFailsTheResource(t *testing.T) {
	d, _ := newTestDeployerWithState(t)

	result, err := d.Deploy(context.Background(), cfnImportingTemplate, "app", nil)
	require.NoError(t, err, "the resource fails; the stack operation does not")
	require.Len(t, result.Resources, 1)
	assert.Contains(t, result.Resources[0].Error, `no exported output named "net-BucketName"`)
}

// TestCFN_ExportsAreScopedToAccountAndRegion pins the restriction that makes an
// import a reference rather than a global lookup: "when using Export and
// Fn::ImportValue, cross-stack references are limited to the same account and
// Region".
//
// An emulator that ignored the scope would resolve an import a real deployment
// rejects, which is worse than not resolving it: the template passes under test
// and fails in AWS.
func TestCFN_ExportsAreScopedToAccountAndRegion(t *testing.T) {
	_, state := newTestDeployerWithState(t)
	ctx := context.Background()

	east := newTestDeployerWithIdentity(t, state, "123456789012", "us-east-1")
	_, err := east.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)

	exports, err := east.Exports(ctx)
	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.Equal(t, "net-BucketName", exports[0].Name)

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "another region in the same account", accountID: "123456789012", region: "eu-west-1"},
		{name: "another account in the same region", accountID: "999988887777", region: "us-east-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			other := newTestDeployerWithIdentity(t, state, tc.accountID, tc.region)

			otherExports, err := other.Exports(ctx)
			require.NoError(t, err)
			assert.Empty(t, otherExports, "the export is not visible outside its scope")

			result, err := other.Deploy(ctx, cfnImportingTemplate, "app-"+tc.region+tc.accountID, nil)
			require.NoError(t, err)
			require.Len(t, result.Resources, 1)
			assert.Contains(t, result.Resources[0].Error, "no exported output named",
				"an out-of-scope export must not satisfy the import")
		})
	}
}

// TestCFN_DeletingAnExportingStackIsRefused is #522's second gate, and the reason
// exports are modeled rather than faked: "after another stack imports an output
// value, you can't delete the stack that is exporting the output value … all the
// imports must be removed before you can delete the exporting stack".
//
// A registry with no enforcement behind it is a lookup table, not a reference. The
// refusal is what a consumer's teardown ordering is tested against.
func TestCFN_DeletingAnExportingStackIsRefused(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)
	_, err = d.Deploy(ctx, cfnImportingTemplate, "app", nil)
	require.NoError(t, err)

	err = d.DeleteStack(ctx, "net")
	require.Error(t, err)
	assert.ErrorIs(t, err, emulator.ErrCFNExportInUse)
	assert.Contains(t, err.Error(), `export "net-BucketName" is imported by app`,
		"the refusal must name the importer: the caller's only remedy is to remove it")

	// And the stack is still there — a refused delete must not half-delete.
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	names := make([]string, 0, len(stacks))
	for _, s := range stacks {
		names = append(names, s.StackName)
	}
	assert.Contains(t, names, "net")

	// Removing the importer releases it, in that order: the importing stack
	// deletes first, and only then does the exporter.
	require.NoError(t, d.DeleteStack(ctx, "app"))
	require.NoError(t, d.DeleteStack(ctx, "net"),
		"with no importers left the exporting stack deletes")
}

// TestCFN_DeletingAStackThatExportsNothingImportedSucceeds is the negative that
// makes the refusal meaningful: an export nobody imports does not pin its stack.
//
// Without this a refusal keyed merely on "declares an Export" would pass the test
// above and make every exporting stack permanently undeletable.
func TestCFN_DeletingAStackThatExportsNothingImportedSucceeds(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)

	require.NoError(t, d.DeleteStack(ctx, "net"))

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks)
}

// TestCFN_ExportNamesAreUniquePerAccountAndRegion pins "for each AWS account,
// Export names must be unique within a Region".
//
// The same template deployed under two stack names is the common case and must
// work, because the documented idiom namespaces the export by AWS::StackName. The
// conflict is a second stack claiming a name that is already taken.
func TestCFN_ExportNamesAreUniquePerAccountAndRegion(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	const fixedName = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-fixed-export"}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"Data"},"Export":{"Name":"Shared-Value"}}}}`

	_, err := d.Deploy(ctx, fixedName, "first", nil)
	require.NoError(t, err)

	_, err = d.Deploy(ctx, fixedName, "second", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emulator.ErrCFNExportNameConflict)
	assert.Contains(t, err.Error(), `export name "Shared-Value" is already exported by stack "first"`)

	// Redeploying the *owning* stack is not a conflict with itself.
	_, err = d.Deploy(ctx, fixedName, "first", nil)
	require.NoError(t, err, "a stack may keep exporting the name it already owns")

	// And the stack-namespaced idiom deploys twice without collision.
	_, err = d.Deploy(ctx, cfnExportingTemplate, "net-a", nil)
	require.NoError(t, err)
	_, err = d.Deploy(ctx, cfnExportingTemplate, "net-b", nil)
	require.NoError(t, err,
		"Fn::Sub over AWS::StackName is the documented way to keep export names unique")
}

// TestCFN_ChangingAnImportedExportIsRefused pins the other half of the same
// sentence as the delete refusal: "you can't delete the stack that is exporting the
// output value **or modify the exported output value**".
//
// Substrate had no way to express this before exports existed, and it is the half a
// consumer hits by accident: an update that changes an exported value silently
// leaves every importing stack holding a value the exporter no longer publishes.
func TestCFN_ChangingAnImportedExportIsRefused(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)
	_, err = d.Deploy(ctx, cfnImportingTemplate, "app", nil)
	require.NoError(t, err)

	tests := []struct {
		name     string
		template string
		wantWhat string
	}{
		{
			name: "the exported value changes",
			template: `{"Resources":{` +
				`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-other-bucket"}}},` +
				`"Outputs":{"BucketName":{"Value":{"Ref":"Data"},` +
				`"Export":{"Name":{"Fn::Sub":"${AWS::StackName}-BucketName"}}}}}`,
			wantWhat: "changed",
		},
		{
			name: "the export is dropped altogether",
			template: `{"Resources":{` +
				`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-export-bucket"}}},` +
				`"Outputs":{"BucketName":{"Value":{"Ref":"Data"}}}}`,
			wantWhat: "removed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := d.UpdateStack(ctx, tc.template, "net", nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, emulator.ErrCFNExportInUse)
			assert.Contains(t, err.Error(),
				`export "net-BucketName" cannot be `+tc.wantWhat+" while it is imported by app")
		})
	}

	// Redeploying the same value is not a change, so it is allowed — otherwise an
	// idempotent redeploy of an exporting stack would be impossible.
	_, err = d.UpdateStack(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)
}

// TestCFN_ImportIsRecordedOnlyWhenItResolves pins what counts as an import, which
// is what the delete refusal is keyed on.
//
// An Fn::ImportValue inside a false Fn::If branch is never evaluated, so the stack
// does not import it and must not pin the exporter; an import that failed to
// resolve has no exporter to pin. Deriving imports from the template text — the
// obvious alternative — gets both wrong.
func TestCFN_ImportIsRecordedOnlyWhenItResolves(t *testing.T) {
	exports := map[string]string{"net-BucketName": "cfn-export-bucket"}

	tests := []struct {
		name        string
		value       interface{}
		wantValue   string
		wantImports []string
		wantFailure string
	}{
		{
			name:        "a resolved import is recorded",
			value:       map[string]interface{}{"Fn::ImportValue": "net-BucketName"},
			wantValue:   "cfn-export-bucket",
			wantImports: []string{"net-BucketName"},
		},
		{
			name:        "an unresolved import is not",
			value:       map[string]interface{}{"Fn::ImportValue": "no-such-export"},
			wantFailure: `no exported output named "no-such-export"`,
		},
		{
			name: "an import in the branch not taken is not",
			value: map[string]interface{}{"Fn::If": []interface{}{
				"Never",
				map[string]interface{}{"Fn::ImportValue": "net-BucketName"},
				"literal",
			}},
			wantValue: "literal",
		},
		{
			name: "an import name built by Fn::Sub resolves",
			value: map[string]interface{}{"Fn::ImportValue": map[string]interface{}{
				"Fn::Sub": "${NetStack}-BucketName",
			}},
			wantValue:   "cfn-export-bucket",
			wantImports: []string{"net-BucketName"},
		},
		{
			name:        "an empty name is a failure, not an import of \"\"",
			value:       map[string]interface{}{"Fn::ImportValue": ""},
			wantFailure: "Fn::ImportValue requires an export name",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, imports, failures := emulator.CFNResolveImportForTest(
				tc.value, exports, map[string]string{"NetStack": "net"})
			assert.Equal(t, tc.wantValue, value)
			assert.Equal(t, tc.wantImports, imports)
			if tc.wantFailure == "" {
				assert.Empty(t, failures)
				return
			}
			require.Len(t, failures, 1)
			assert.Contains(t, failures[0], tc.wantFailure)
		})
	}
}

// TestCFN_ImportValueResolvesInsideAStructuredProperty pins the reason
// Fn::ImportValue was withheld from cfnIntrinsicNames until it resolved.
//
// resolveNested consults that table to decide what is an intrinsic, so admitting an
// unresolvable Fn::ImportValue would have resolved every nested import to "" —
// strictly worse than the JSON-literal fallback, which at least left the intrinsic
// visible in the request the plugin refused. Now that it resolves, admission is
// correct and this asserts the depth.
func TestCFN_ImportValueResolvesInsideAStructuredProperty(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)

	// KeySchema is one of the properties forwarded whole to the plugin, so the
	// import here is resolved by the nested walk rather than by a scalar
	// resolution at the top level of a property.
	const tmpl = `{"Resources":{"T":{"Type":"AWS::DynamoDB::Table","Properties":{` +
		`"TableName":"cfn-import-nested",` +
		`"AttributeDefinitions":[{"AttributeName":{"Fn::ImportValue":"net-BucketName"},` +
		`"AttributeType":"S"}],` +
		`"KeySchema":[{"AttributeName":{"Fn::ImportValue":"net-BucketName"},` +
		`"KeyType":"HASH"}]}}}}`

	result, err := d.Deploy(ctx, tmpl, "nested", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error,
		"a nested import must resolve; an unresolved one reaches DynamoDB as JSON and is refused")

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	var nested *emulator.CFNStackState
	for i := range stacks {
		if stacks[i].StackName == "nested" {
			nested = &stacks[i]
		}
	}
	require.NotNil(t, nested)
	assert.Equal(t, []string{"net-BucketName"}, nested.Imports,
		"an import found by the nested walk pins the exporter just as a scalar one does")
}

// TestCFN_StackExportsJoinNamesAgainstOutputs pins the persisted shape, because it
// is what a restored snapshot is read back through.
//
// ExportNames is keyed by output key, not by export name, so one map serves both
// DescribeStacks (which reports ExportName beside its output) and the registry
// (which needs name → value). An export name whose output is gone is dropped rather
// than exported as "", since an import resolving to "" is the silent-literal
// failure the whole model exists to prevent.
func TestCFN_StackExportsJoinNamesAgainstOutputs(t *testing.T) {
	got := emulator.CFNStackExportsForTest(emulator.CFNStackState{
		Outputs: map[string]string{
			"BucketName": "cfn-export-bucket",
			"Local":      "not-exported",
		},
		ExportNames: map[string]string{
			"BucketName": "net-BucketName",
			"Vanished":   "net-Vanished",
		},
	})
	assert.Equal(t, map[string]string{"net-BucketName": "cfn-export-bucket"}, got)
}

// TestCFN_AStackDoesNotImportItsOwnExport pins that the stack being deployed is
// excluded from the registry its own imports resolve against.
//
// Without the exclusion a redeploy would resolve an import against the value the
// same deployment is in the middle of replacing — a stack reading its own previous
// output, which no CloudFormation deployment can do. It stays a failed import on
// every deploy, first and subsequent alike, so the observable does not depend on
// whether the stack has been deployed before.
func TestCFN_AStackDoesNotImportItsOwnExport(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	// The stack exports Ouroboros-Name and imports it in the same template.
	const tmpl = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-self-import"}},` +
		`"Q":{"Type":"AWS::SQS::Queue","Properties":{` +
		`"QueueName":{"Fn::ImportValue":"Ouroboros-Name"}}}},` +
		`"Outputs":{"N":{"Value":{"Ref":"Data"},"Export":{"Name":"Ouroboros-Name"}}}}`

	for _, pass := range []string{"first deploy", "redeploy"} {
		result, err := d.Deploy(ctx, tmpl, "ouroboros", nil)
		require.NoError(t, err, pass)

		var queue *emulator.DeployedResource
		for i := range result.Resources {
			if result.Resources[i].LogicalID == "Q" {
				queue = &result.Resources[i]
			}
		}
		require.NotNil(t, queue, pass)
		assert.Contains(t, queue.Error, `no exported output named "Ouroboros-Name"`,
			"%s: a stack cannot import the name it is itself exporting", pass)
	}

	// The export is published for *other* stacks all the same.
	exports, err := d.Exports(ctx)
	require.NoError(t, err)
	require.Len(t, exports, 1)
	assert.Equal(t, "Ouroboros-Name", exports[0].Name)
}

// TestCFN_AStacksOwnImportDoesNotPinIt pins the invariant both export checks rest
// on: a stack is never held open by its own recorded import.
//
// The resolver cannot currently produce such a record — a stack's own exports are
// excluded from what its imports resolve against, per the test above — so this
// asserts the guard directly against a persisted record rather than through a
// deploy. It is worth a test because the guard is what makes the two refusals
// statements about *other* stacks: without it a record like this one, from a
// restored snapshot or a later substrate that permits self-import, would make the
// stack permanently undeletable with no remedy the caller could apply.
func TestCFN_AStacksOwnImportDoesNotPinIt(t *testing.T) {
	d, state := newTestDeployerWithState(t)
	ctx := context.Background()

	record, err := json.Marshal(emulator.CFNStackState{
		StackName:   "selfish",
		Outputs:     map[string]string{"N": "cfn-selfish"},
		ExportNames: map[string]string{"N": "Selfish-Name"},
		Imports:     []string{"Selfish-Name"},
		AccountID:   "123456789012",
		Region:      "us-east-1",
		Status:      "CREATE_COMPLETE",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "cfn", "stack:selfish", record))
	require.NoError(t, state.Put(ctx, "cfn", "stack_names", []byte(`["selfish"]`)))

	importers, err := d.Imports(ctx, "Selfish-Name")
	require.NoError(t, err)
	assert.Equal(t, []string{"selfish"}, importers,
		"the record says so, and Imports reports what is recorded")

	assert.NoError(t, d.DeleteStack(ctx, "selfish"),
		"the only importer is the stack itself, so nothing is holding it open")
}

// TestCFN_LegacySnapshotStacksStayInScope pins the back-compatibility decision in
// sameScope: a stack persisted before exports existed has no AccountID or Region
// and is treated as in scope.
//
// Excluding it would make a restored snapshot's stacks vanish from ListExports
// rather than simply exporting nothing, and Snapshot/Restore round-trips these
// records — so a v0.87 snapshot restored into this release must still list its
// stacks.
func TestCFN_LegacySnapshotStacksStayInScope(t *testing.T) {
	d, state := newTestDeployerWithState(t)
	ctx := context.Background()

	// Written as a raw object so AccountID and Region are genuinely absent rather
	// than empty, which is what an older substrate wrote.
	require.NoError(t, state.Put(ctx, "cfn", "stack:legacy", []byte(
		`{"StackName":"legacy","Outputs":{"N":"legacy-bucket"},`+
			`"ExportNames":{"N":"Legacy-Name"},"Status":"CREATE_COMPLETE"}`)))
	require.NoError(t, state.Put(ctx, "cfn", "stack_names", []byte(`["legacy"]`)))

	exports, err := d.Exports(ctx)
	require.NoError(t, err)
	require.Len(t, exports, 1, "an unpartitioned stack is in every scope, not none")
	assert.Equal(t, "Legacy-Name", exports[0].Name)
	assert.Equal(t, "legacy-bucket", exports[0].Value)
}

// TestCFN_TheReportedExportConflictIsDeterministic pins the sort behind the
// uniqueness check.
//
// A template declaring two export names that are both already taken has two valid
// conflicts to report, and iterating the map directly would report whichever Go's
// randomized order reached first. Nondeterminism is the one outcome an emulator
// built on deterministic replay must never produce, so the answer is the
// alphabetically first name on every run — asserted over repeated deploys, since a
// single run agrees with a coin flip half the time.
func TestCFN_TheReportedExportConflictIsDeterministic(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	for _, s := range []struct{ stack, export string }{
		{"holder-a", "Aaa-Name"},
		{"holder-z", "Zzz-Name"},
	} {
		_, err := d.Deploy(ctx, `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",`+
			`"Properties":{"BucketName":"cfn-`+s.stack+`"}}},`+
			`"Outputs":{"N":{"Value":{"Ref":"Data"},"Export":{"Name":"`+s.export+`"}}}}`,
			s.stack, nil)
		require.NoError(t, err)
	}

	// Claims both taken names.
	const claimant = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"cfn-claimant"}}},` +
		`"Outputs":{` +
		`"A":{"Value":{"Ref":"Data"},"Export":{"Name":"Aaa-Name"}},` +
		`"Z":{"Value":{"Ref":"Data"},"Export":{"Name":"Zzz-Name"}}}}`

	for i := range 20 {
		_, err := d.Deploy(ctx, claimant, "claimant", nil)
		require.Error(t, err, "attempt %d", i)
		assert.ErrorIs(t, err, emulator.ErrCFNExportNameConflict)
		assert.Contains(t, err.Error(), `export name "Aaa-Name" is already exported by stack "holder-a"`,
			"attempt %d: the same conflict must be reported every time", i)
	}
}

// TestCFN_ImportsListsTheImportingStacks pins the read the API points a caller at
// when a delete is refused: "to delete or change exported output values, you must
// first find out which stacks are importing them".
func TestCFN_ImportsListsTheImportingStacks(t *testing.T) {
	d, _ := newTestDeployerWithState(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnExportingTemplate, "net", nil)
	require.NoError(t, err)
	for _, name := range []string{"app-b", "app-a"} {
		_, err = d.Deploy(ctx, `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{`+
			`"QueueName":{"Fn::Sub":["${n}-`+name+`",{"n":{"Fn::ImportValue":"net-BucketName"}}]}}}}}`,
			name, nil)
		require.NoError(t, err)
	}

	importers, err := d.Imports(ctx, "net-BucketName")
	require.NoError(t, err)
	assert.Equal(t, []string{"app-a", "app-b"}, importers, "sorted, so the answer is stable")

	unknown, err := d.Imports(ctx, "no-such-export")
	require.NoError(t, err,
		"ListImports documents no service-specific error: an unknown export is an empty list")
	assert.Empty(t, unknown)
}
