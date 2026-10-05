package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1107: DeleteAlias answers NotFoundException for an alias that names nothing.
//
// API_DeleteAlias publishes NotFoundException/400 and KMSInvalidStateException/400, and the key-state
// table permits DeleteAlias in every key state, so the not-found refusal is the only one the operation
// can give. It used to answer 200 whatever the name. CloudFormation's teardown stays idempotent because
// cfnDeleteAbsentCodes already treats NotFoundException as a resource already gone; the last test here
// proves that end to end.

func TestKMSDeleteAlias_AnAliasNamingNothingIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alias string
	}{
		// The page's pattern, ^[a-zA-Z0-9:/_-]+$, does not require the prefix, so a bare name reaches the
		// same alias deleteAlias resolves and gets the same refusal.
		{"with the prefix", "alias/never-created"},
		{"without the prefix", "never-created"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)
			status, code := kmsCall(t, ts, "DeleteAlias", map[string]any{"AliasName": tc.alias})
			assert.Equal(t, "NotFoundException", code, "DeleteAlias of %q", tc.alias)
			assert.Equal(t, http.StatusBadRequest, status, "DeleteAlias of %q", tc.alias)
		})
	}
}

// A second delete of the same alias is refused: the first removed it, so the second names nothing.
func TestKMSDeleteAlias_ASecondDeleteIsNotFound(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)
	require.Empty(t, mustCreateAlias(t, ts, "alias/twice", keyID))

	status, code := kmsCall(t, ts, "DeleteAlias", map[string]any{"AliasName": "alias/twice"})
	require.Empty(t, code, "the first delete")
	require.Equal(t, http.StatusOK, status, "the first delete")

	status, code = kmsCall(t, ts, "DeleteAlias", map[string]any{"AliasName": "alias/twice"})
	assert.Equal(t, "NotFoundException", code, "the second delete")
	assert.Equal(t, http.StatusBadRequest, status, "the second delete")
	assert.Empty(t, kmsAliasNames(t, ts), "ListAliases after both deletes")
}

// A stack whose alias was removed out of band still deletes: the teardown's DeleteAlias now answers
// NotFoundException, which cfnDeleteAbsentCodes reads as "already gone" rather than DELETE_FAILED.
func TestCFN_AStackWhoseAliasWasDeletedOutOfBandStillDeletes(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const tmpl = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
		`"Key":{"Type":"AWS::KMS::Key","Properties":{}},` +
		`"Alias":{"Type":"AWS::KMS::Alias","Properties":{"AliasName":"alias/out-of-band",` +
		`"TargetKeyId":{"Fn::GetAtt":["Key","Arn"]}}}}}`

	result, err := d.Deploy(ctx, tmpl, "kms-alias-gone", nil)
	require.NoError(t, err)
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "deploy of %s", r.LogicalID)
	}

	body, err := json.Marshal(map[string]any{"AliasName": "alias/out-of-band"})
	require.NoError(t, err)
	resp, err := d.DispatchForTest(ctx, &emulator.AWSRequest{
		Service: "kms", Operation: "DeleteAlias", Body: body,
		Headers: map[string]string{}, Params: map[string]string{},
	}, "kms-alias-gone")
	require.NoError(t, err, "the out-of-band delete")
	require.Equal(t, http.StatusOK, resp.StatusCode, "the out-of-band delete")

	assert.NoError(t, d.DeleteStack(ctx, "kms-alias-gone"),
		"an alias already gone is not a failed delete")
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks, "the stack record goes")
}

// A store fault while looking the alias up, or while deleting it, is an error, never answered as the
// published NotFoundException or as a 200 over an alias that was not removed.
func TestKMSDeleteAlias_AStoreFaultIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"alias lookup", func(m *cfFaultStateManager) { m.failGet = "alias:" }},
		{"corrupt alias record", func(m *cfFaultStateManager) { m.corruptGet = "alias_names:" }},
		{"alias delete", func(m *cfFaultStateManager) { m.failDelete = "alias:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.KMSPlugin{}
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State:   fault,
				Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
			}))
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1",
				RequestID: "req-kms-fault", IDs: emulator.NewIDMint("req-kms-fault")}
			created := wireJSONTarget(t, p, ctx, "kms", "TrentService", "CreateKey", map[string]any{})
			var key struct {
				KeyMetadata struct {
					KeyID string `json:"KeyId"`
				} `json:"KeyMetadata"`
			}
			require.NoError(t, json.Unmarshal(created, &key))
			wireJSONTarget(t, p, ctx, "kms", "TrentService", "CreateAlias",
				map[string]any{"AliasName": "alias/fault", "TargetKeyId": key.KeyMetadata.KeyID})

			tc.arm(fault)
			raw, err := json.Marshal(map[string]any{"AliasName": "alias/fault"})
			require.NoError(t, err)
			_, err = p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "kms", Operation: "DeleteAlias", Path: "/", Body: raw,
				Headers: map[string]string{"X-Amz-Target": "TrentService.DeleteAlias"}, Params: map[string]string{},
			})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			assert.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as %v", tc.name, awsErr)
		})
	}
}
