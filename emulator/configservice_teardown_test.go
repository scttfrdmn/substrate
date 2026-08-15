package emulator_test

import (
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AWS Config teardown ordering (#580).
//
// The four deletes live in their own clusters' files, but their *combined* behavior is
// what justifies including them in scope, so it is asserted here as a sequence rather
// than as four isolated refusals. The sequence is the one a repeated test run performs:
// tear the account's Config setup down and build it again.
//
// The load-bearing fact is an **asymmetry between the two deletes**, and it is easy to
// get wrong in the tidy direction:
//
//   - DeleteDeliveryChannel refuses while the recorder is recording, with
//     LastDeliveryChannelDeleteFailedException. Its prose says so outright: "Before you
//     can delete the delivery channel, you must stop the customer managed configuration
//     recorder."
//   - DeleteConfigurationRecorder has **no documented precondition whatsoever**. It
//     declares two exceptions — NoSuchConfigurationRecorderException and
//     UnmodifiableEntityException — and neither could express "stop it first" or "delete
//     the channel first". So substrate permits both.
//
// An emulator that "helpfully" required the recorder to be stopped before deleting it
// would refuse a sequence AWS accepts, and a consumer whose teardown works against AWS
// would fail here. That is the worse failure of the two directions, so the permissive
// reading is asserted deliberately below rather than left to chance.

func TestConfigTeardown_TheOrderingSequence(t *testing.T) {
	// The whole story in one test, in order, because the ordering is the behavior — the
	// refusal only means something in the context of the steps around it.
	ts := emulator.StartTestServer(t)

	// --- build ---
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")
	require.Equal(t, true, configDescribeRecorderStatus(t, ts)[0]["recording"],
		"the recorder is recording before the teardown starts")

	// --- the refusal: the channel cannot go while the recorder runs ---
	status, code, message := configDeleteChannel(t, ts, "default")
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "LastDeliveryChannelDeleteFailedException", code)
	assert.Contains(t, message, "configuration recorder is running",
		"the message names the reason, which is what a consumer's log will show")

	// The refusal changed nothing: the channel is still there. A refusal that had
	// deleted anyway would leave a consumer's retry acting on state it cannot see.
	require.Len(t, configDescribeChannels(t, ts), 1, "the refused delete removed nothing")

	// --- stop, then the same delete succeeds ---
	configStopRecorder(t, ts, "default")
	status, code, message = configDeleteChannel(t, ts, "default")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, configDescribeChannels(t, ts))

	// --- the recorder goes next, and the recorder's delete does not touch a channel ---
	status, code, message = configDeleteRecorder(t, ts, "default")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, configDescribeRecorders(t, ts))
	assert.Empty(t, configDescribeRecorderStatus(t, ts),
		"the status goes with the recorder, so a rebuilt one does not inherit recording:true")

	// --- rebuild: the same sequence runs clean a second time ---
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	require.Equal(t, false, configDescribeRecorderStatus(t, ts)[0]["recording"],
		"a rebuilt recorder starts not recording, whatever its predecessor was doing")
	configStartRecorder(t, ts, "default")
	assert.Equal(t, true, configDescribeRecorderStatus(t, ts)[0]["recording"])
}

func TestConfigTeardown_TheRecorderCanBeDeletedWhileRecording(t *testing.T) {
	// The asymmetry, asserted on its own so it cannot be lost in the sequence above.
	//
	// DeleteConfigurationRecorder declares only NoSuchConfigurationRecorderException and
	// UnmodifiableEntityException. There is no exception it could answer for "the
	// recorder is running" or "the channel still exists", and no prose requiring either,
	// so both succeed. Requiring a Stop first would be substrate inventing a rule.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	status, code, message := configDeleteRecorder(t, ts, "default")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, configDescribeRecorders(t, ts))

	// And the channel outlived it — deleting a recorder does not cascade. A fixture that
	// tears down must delete both, which is exactly why the sequence above deletes the
	// channel first.
	assert.Len(t, configDescribeChannels(t, ts), 1,
		"the channel is not deleted with the recorder")
}

func TestConfigTeardown_AnOrphanedChannelIsDeletableWithNoRecorder(t *testing.T) {
	// The state the previous case leaves behind: a channel whose recorder is gone. The
	// delete's guard reads the recorder's *status*, and with no recorder there is no
	// status, so the guard must treat an absent status as "not recording" rather than
	// failing to read it. Otherwise a teardown that deleted the recorder first would
	// wedge, with no documented way out.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	status, code, message := configDeleteRecorder(t, ts, "default")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	status, code, message = configDeleteChannel(t, ts, "default")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, configDescribeChannels(t, ts))
}

func TestConfigTeardown_TheStoppedRecorderRefusalIsNotResurrectedByARestart(t *testing.T) {
	// Stop then Start then Delete-channel must refuse again. A guard that read a
	// one-shot "has been stopped" flag rather than the current recording state would
	// permit the delete after the restart, which is the mutation this case exists to
	// catch.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")
	configStopRecorder(t, ts, "default")
	configStartRecorder(t, ts, "default")

	status, code, _ := configDeleteChannel(t, ts, "default")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "LastDeliveryChannelDeleteFailedException", code)
}

func TestConfigTeardown_ADeleteOfSomethingAbsentIsRefusedNotIgnored(t *testing.T) {
	// Each delete refuses an entity that is not there, with the exception its own model
	// declares. A delete that silently succeeded would make a teardown-then-rebuild
	// fixture pass while asserting nothing about the teardown having happened.
	ts := emulator.StartTestServer(t)

	status, code, _ := configDeleteRecorder(t, ts, "default")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchConfigurationRecorderException", code)

	status, code, _ = configDeleteChannel(t, ts, "default")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchDeliveryChannelException", code)

	// And a delete naming something other than the one that exists is refused too: the
	// account holds one recorder and one channel, so a wrong name names nothing.
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")

	status, code, _ = configDeleteRecorder(t, ts, "other")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchConfigurationRecorderException", code)

	status, code, _ = configDeleteChannel(t, ts, "other")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchDeliveryChannelException", code)
}

func TestConfigTeardown_ARebuildDoesNotInheritTheOldRulesOrPacks(t *testing.T) {
	// The full teardown a repeated run performs: rules and packs go too, and a rebuilt
	// account starts from the documented defaults rather than from its predecessor's
	// verdicts. Both identifiers are deterministic, so a rebuilt rule and pack have the
	// same ARNs — which is what makes an inherited verdict observable at all.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutRule(t, ts, "s3-encrypted")
	configSeedRuleCompliance(t, ts, "s3-encrypted", map[string]any{"complianceType": "NON_COMPLIANT"})
	packARN := configPutPack(t, ts, "ops")
	configSeedPackCompliance(t, ts, "ops", []map[string]any{
		{"configRuleName": "s3-encrypted", "complianceType": "NON_COMPLIANT"},
	})
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])

	status, code, message := decodeConfigResponse(t,
		configRequest(t, ts, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"}), nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	status, code, message = configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	status, code, message = configDeleteRecorder(t, ts, "default")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	// Rebuild.
	configPutRecorder(t, ts, "default")
	configPutRule(t, ts, "s3-encrypted")
	require.Equal(t, packARN, configPutPack(t, ts, "ops"),
		"the rebuilt pack's ARN is the same, so a stale verdict would be visible")

	// The rebuilt pack is CREATE_IN_PROGRESS, not the CREATE_COMPLETE its predecessor
	// reached. That cannot be asserted by reading the state — the first observation is
	// what *advances* it — so it is asserted through the refusal only an unresolved pack
	// gives: a delete arriving before anything has observed the pack.
	status, code, _ = configDeletePack(t, ts, "ops")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceInUseException", code,
		"the rebuilt pack starts unobserved rather than inheriting its predecessor's completion")
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"],
		"...and one observation settles it, as it did the first time")

	compliance := configDescribeCompliance(t, ts, map[string]any{})
	require.Len(t, compliance, 1)
	assert.Equal(t, "INSUFFICIENT_DATA", configComplianceTypeOf(t, compliance[0]),
		"the rebuilt rule starts at the documented default, not its predecessor's verdict")

	assert.Empty(t, configPackCompliance(t, ts, map[string]any{"ConformancePackName": "ops"}),
		"the rebuilt pack carries no rule verdicts")
}

// configDeleteChannel sends a DeleteDeliveryChannel and returns the status and error
// code, since every case here is about which of those two it is.
func configDeleteChannel(t *testing.T, ts *emulator.TestServer, name string) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "DeleteDeliveryChannel", map[string]any{"DeliveryChannelName": name})
	return decodeConfigResponse(t, resp, nil)
}

// configDeleteRecorder sends a DeleteConfigurationRecorder and returns the status and
// error code.
func configDeleteRecorder(t *testing.T, ts *emulator.TestServer, name string) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "DeleteConfigurationRecorder",
		map[string]any{"ConfigurationRecorderName": name})
	return decodeConfigResponse(t, resp, nil)
}

// configStopRecorder stops the named recorder, requiring success.
func configStopRecorder(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	resp := configRequest(t, ts, "StopConfigurationRecorder",
		map[string]any{"ConfigurationRecorderName": name})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}
