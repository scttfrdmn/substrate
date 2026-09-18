package emulator_test

// Step Functions ARN resolution, over the wire (#912).
//
// Eleven operations took the resource *name* from the last colon-separated segment of the ARN they
// were handed and the account and Region from the caller's own request context. Three separate things
// followed from that, and this file asserts all three are gone:
//
//  1. An ARN naming another account's or another Region's resource reached the caller's own same-named
//     one. DescribeStateMachine disclosed it, UpdateStateMachine rewrote it, DeleteStateMachine and
//     DeleteActivity removed it, StopExecution aborted it — every one answering 200.
//  2. The resource-type segment was never read, so an activity ARN at a state-machine operation, an
//     execution ARN at either, and a non-states ARN were told apart only by accident.
//  3. An execution ARN's two names were reconstructed by stripping one segment, which is right only
//     for an ARN of exactly that arity.
//
// Every resource is created through its own operation and every ARN is either taken out of a create
// response or composed *deliberately* to name something that does not exist — the two cases are kept
// visibly separate, because an ARN composed by the test and then found by the code proves nothing
// about what the create operation reports. No putTest* helper is used, per #765's rule that a helper
// writing state directly cannot prove a value is readable through the owning service's own call.
//
// The Region travels in the Host and the credential scope together, so the requests here go through
// scanScopeSignedTarget rather than signedRequest: signedRequest signs every request for us-east-1,
// and a cross-Region assertion needs a caller that genuinely is in another Region rather than one
// whose Host and scope disagree.
//
// Statuses are asserted as well as codes. Every error on all eleven AWS pages is HTTP 400, including
// the three *DoesNotExist codes, all of which substrate answered at 404 before this change — so a
// consumer branching on the status rather than the code saw something no Step Functions endpoint
// returns.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

const (
	// sfnArnOtherAccount is the second account, so "another account's ARN" is a real ARN with a real
	// credential behind it rather than one nothing could have created.
	sfnArnOtherAccount = "210987654321"

	sfnArnEastRegion = "us-east-1"
	sfnArnWestRegion = "us-west-2"

	sfnArnTargetPrefix = "AWSStepFunctions"
	sfnArnSigningName  = "states"

	// sfnArnPassDefinition is the smallest valid ASL document: one Pass state that ends. The
	// definition is a member the tests read back, so each state machine gets a distinguishable one
	// built from this plus a comment.
	sfnArnPassDefinition = `{"StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`
)

// sfnArnDefinition returns a valid ASL document carrying comment, so two state machines created with
// the same *name* in different accounts or Regions can be told apart by what Describe reports.
func sfnArnDefinition(comment string) string {
	return `{"Comment":"` + comment + `","StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`
}

// sfnArnServer starts a server with both test accounts registered.
func sfnArnServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount, sfnArnOtherAccount))
}

// sfnArnCall posts one Step Functions operation to a Region's endpoint as an account and returns the
// status, the error code a refusal carries, and the raw body.
//
// The raw body is returned because a member's *absence* is part of what several of these tests
// assert, and a decoded struct cannot tell an absent member from an empty one.
func sfnArnCall(t *testing.T, ts *emulator.TestServer, account, region, op string,
	body map[string]any,
) (status int, errCode, raw string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoErrorf(t, err, "marshal %s", op)

	rawBytes, code := scanScopeSignedTarget(t, ts, account, "states."+region+".amazonaws.com",
		sfnArnSigningName, region, sfnArnTargetPrefix, op, data)

	var errShape struct {
		Type string `json:"__type"`
	}
	// A success body has no __type, so a failed unmarshal and an absent member are the same answer.
	_ = json.Unmarshal(rawBytes, &errShape) //nolint:errcheck // absent __type means success
	return code, errShape.Type, string(rawBytes)
}

// sfnArnOK posts one operation and fails the test unless it succeeds, returning the raw body.
func sfnArnOK(t *testing.T, ts *emulator.TestServer, account, region, op string,
	body map[string]any,
) string {
	t.Helper()
	status, errCode, raw := sfnArnCall(t, ts, account, region, op, body)
	require.Emptyf(t, errCode, "%s in %s: %s", op, region, raw)
	require.Equalf(t, http.StatusOK, status, "%s in %s: %s", op, region, raw)
	return raw
}

// sfnArnMember decodes one string member out of a response body.
func sfnArnMember(t *testing.T, raw, member string) string {
	t.Helper()
	var out map[string]any
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &out), "decode %s", raw)
	value, ok := out[member].(string)
	require.Truef(t, ok, "no %q member in %s", member, raw)
	return value
}

// sfnArnCreateSM creates one state machine and returns the ARN CreateStateMachine reports.
func sfnArnCreateSM(t *testing.T, ts *emulator.TestServer, account, region, name, smType, definition string) string {
	t.Helper()
	raw := sfnArnOK(t, ts, account, region, "CreateStateMachine", map[string]any{
		"name":       name,
		"definition": definition,
		"roleArn":    "arn:aws:iam::" + account + ":role/StepFunctionsRole",
		"type":       smType,
	})
	arn := sfnArnMember(t, raw, "stateMachineArn")
	require.Containsf(t, arn, ":"+region+":"+account+":stateMachine:"+name, "CreateStateMachine ARN %s", arn)
	return arn
}

// sfnArnCreateActivity creates one activity and returns the ARN CreateActivity reports.
func sfnArnCreateActivity(t *testing.T, ts *emulator.TestServer, account, region, name string) string {
	t.Helper()
	raw := sfnArnOK(t, ts, account, region, "CreateActivity", map[string]any{"name": name})
	arn := sfnArnMember(t, raw, "activityArn")
	require.Containsf(t, arn, ":"+region+":"+account+":activity:"+name, "CreateActivity ARN %s", arn)
	return arn
}

// sfnArnStart starts one execution and returns the ARN StartExecution reports.
func sfnArnStart(t *testing.T, ts *emulator.TestServer, account, region, smARN, execName string) string {
	t.Helper()
	raw := sfnArnOK(t, ts, account, region, "StartExecution", map[string]any{
		"stateMachineArn": smARN,
		"name":            execName,
	})
	return sfnArnMember(t, raw, "executionArn")
}

// sfnArnOpCall is one operation in an ARN-kind's inventory, with the members it needs to get past body
// decoding.
//
// absentIsIdempotent marks the operations whose page does *not* publish a *DoesNotExist code, which
// after #995 is the two deletes. It is a column rather than a skip list in the two absent-resource
// tests, because the whole inventory has to stay in one place: the deletes must still answer InvalidArn
// for every malformed, wrong-type and qualified ARN — those three tests iterate the same tables — and
// removing them to express one difference would have lost three kinds of coverage to express it.
type sfnArnOpCall struct {
	op                 string
	body               map[string]any
	absentIsIdempotent bool
}

// sfnArnStateMachineOps names every operation that takes a stateMachineArn, with the rest of the
// members each needs to get past body decoding, so one table drives the refusal tests.
//
// ListExecutions is here because its stateMachineArn, although "Required: No" at AWS, resolves the
// same way when it is supplied — and it was the one operation in the set with no existence check at
// all, so an ARN naming nothing answered 200 with an empty list.
func sfnArnStateMachineOps(smARN string) []sfnArnOpCall {
	return []sfnArnOpCall{
		{op: "DescribeStateMachine", body: map[string]any{"stateMachineArn": smARN}},
		{op: "UpdateStateMachine", body: map[string]any{"stateMachineArn": smARN, "definition": sfnArnPassDefinition}},
		// API_DeleteStateMachine publishes InvalidArn and ValidationException and nothing else.
		{op: "DeleteStateMachine", body: map[string]any{"stateMachineArn": smARN}, absentIsIdempotent: true},
		{op: "StartExecution", body: map[string]any{"stateMachineArn": smARN, "name": "e1"}},
		{op: "StartSyncExecution", body: map[string]any{"stateMachineArn": smARN, "name": "e1"}},
		{op: "ListExecutions", body: map[string]any{"stateMachineArn": smARN}},
	}
}

// sfnArnActivityOps names every operation that takes an activityArn.
func sfnArnActivityOps(activityARN string) []sfnArnOpCall {
	return []sfnArnOpCall{
		{op: "DescribeActivity", body: map[string]any{"activityArn": activityARN}},
		// API_DeleteActivity publishes InvalidArn and nothing else at all.
		{op: "DeleteActivity", body: map[string]any{"activityArn": activityARN}, absentIsIdempotent: true},
	}
}

// sfnArnExecutionOps names every operation that takes an executionArn.
func sfnArnExecutionOps(execARN string) []sfnArnOpCall {
	return []sfnArnOpCall{
		{op: "DescribeExecution", body: map[string]any{"executionArn": execARN}},
		{op: "StopExecution", body: map[string]any{"executionArn": execARN}},
		{op: "GetExecutionHistory", body: map[string]any{"executionArn": execARN}},
	}
}

// TestSFNARN_ACrossRegionARNAddressesTheResourceItNames is the headline. Two state machines share a
// name in two Regions of one account; each ARN must reach its own, whichever endpoint the call
// arrives at.
func TestSFNARN_ACrossRegionARNAddressesTheResourceItNames(t *testing.T) {
	ts := sfnArnServer(t)

	eastARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("east"))
	westARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnWestRegion, "orders", "STANDARD",
		sfnArnDefinition("west"))
	require.NotEqual(t, eastARN, westARN, "two Regions must not report one ARN")

	// The caller is in us-east-1 throughout; only the ARN changes.
	for _, tc := range []struct {
		arn  string
		want string
	}{
		{eastARN, "east"},
		{westARN, "west"},
	} {
		raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
			map[string]any{"stateMachineArn": tc.arn})
		assert.Equalf(t, tc.arn, sfnArnMember(t, raw, "stateMachineArn"),
			"DescribeStateMachine echoed a different ARN from the one asked about")
		assert.Containsf(t, sfnArnMember(t, raw, "definition"), `"Comment":"`+tc.want+`"`,
			"DescribeStateMachine %s reported the wrong Region's state machine", tc.arn)
	}
}

// TestSFNARN_ACrossAccountARNAddressesTheResourceItNames is the same assertion across the account
// segment, which is the direction #826 established the rule for.
func TestSFNARN_ACrossAccountARNAddressesTheResourceItNames(t *testing.T) {
	ts := sfnArnServer(t)

	mineARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("mine"))
	theirsARN := sfnArnCreateSM(t, ts, sfnArnOtherAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("theirs"))
	require.NotEqual(t, mineARN, theirsARN)

	// Signed as taggingTestAccount, asking about the other account's ARN. Substrate models no
	// cross-account authorization for Step Functions — no handler in the plugin calls p.authorize —
	// so what is asserted here is resolution, not permission: the ARN reaches the record it names.
	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": theirsARN})
	assert.Contains(t, sfnArnMember(t, raw, "definition"), `"Comment":"theirs"`)
	assert.Equal(t, theirsARN, sfnArnMember(t, raw, "stateMachineArn"))
}

// TestSFNARN_AForeignARNNamingNothingIsARefusalNotTheCallersOwn is the defect stated as an
// assertion: the caller has a state machine called "orders" and the ARN it presents names one in
// another Region that does not exist. Before the fix, every one of these answered 200 about the
// caller's own.
func TestSFNARN_AForeignARNNamingNothingIsARefusalNotTheCallersOwn(t *testing.T) {
	ts := sfnArnServer(t)

	ownARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("own"))
	// Same name, same account, a Region with nothing in it. Composed on purpose: the point is that it
	// names nothing.
	absentARN := "arn:aws:states:" + sfnArnWestRegion + ":" + taggingTestAccount + ":stateMachine:orders"

	for _, call := range sfnArnStateMachineOps(absentARN) {
		t.Run(call.op, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
			if call.absentIsIdempotent {
				// DeleteStateMachine. API_DeleteStateMachine publishes InvalidArn and
				// ValidationException and nothing else, so an ARN naming nothing is a 200 with an empty
				// body rather than a borrowed StateMachineDoesNotExist (#995).
				assert.Emptyf(t, errCode, "%s: %s", call.op, raw)
				assert.Equalf(t, http.StatusOK, status, "%s: %s", call.op, raw)
				return
			}
			assert.Equalf(t, "StateMachineDoesNotExist", errCode, "%s: %s", call.op, raw)
			assert.Equalf(t, http.StatusBadRequest, status, "%s: every Step Functions error is 400", call.op)
		})
	}

	// And the caller's own is untouched: DeleteStateMachine above must not have removed it, and
	// UpdateStateMachine must not have rewritten it. This assertion carries more weight since #995 made
	// the delete idempotent — a 200 is now the expected answer for the foreign ARN, so nothing about the
	// status says the delete declined to act, and this is the only thing that does.
	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": ownARN})
	assert.Contains(t, sfnArnMember(t, raw, "definition"), `"Comment":"own"`,
		"the caller's own state machine was rewritten by an operation on a foreign ARN")
}

// TestSFNARN_DeleteRemovesTheARNsResourceAndOnlyThat pins the damaging direction: a delete that
// resolved to the wrong record removed one resource *and* left the named one in its index.
func TestSFNARN_DeleteRemovesTheARNsResourceAndOnlyThat(t *testing.T) {
	ts := sfnArnServer(t)

	eastARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("east"))
	westARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnWestRegion, "orders", "STANDARD",
		sfnArnDefinition("west"))

	// Delete the us-west-2 one from the us-east-1 endpoint.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DeleteStateMachine",
		map[string]any{"stateMachineArn": westARN})

	// The us-east-1 one survives, by Describe and by ListStateMachines — the index has to agree with
	// the record, which is the half a wrongly-scoped delete used to get wrong.
	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": eastARN})
	assert.Contains(t, sfnArnMember(t, raw, "definition"), `"Comment":"east"`)

	listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListStateMachines", map[string]any{})
	assert.Contains(t, listed, eastARN, "ListStateMachines dropped a state machine that was not deleted")

	// And the deleted one is gone from its own Region's listing as well as its record.
	westList := sfnArnOK(t, ts, taggingTestAccount, sfnArnWestRegion, "ListStateMachines", map[string]any{})
	assert.NotContains(t, westList, westARN, "a deleted state machine is still in its Region's index")

	status, errCode, _ := sfnArnCall(t, ts, taggingTestAccount, sfnArnWestRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": westARN})
	assert.Equal(t, "StateMachineDoesNotExist", errCode)
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestSFNARN_AnActivityARNAddressesTheActivityItNames covers the two activity operations across both
// segments, including the delete.
func TestSFNARN_AnActivityARNAddressesTheActivityItNames(t *testing.T) {
	ts := sfnArnServer(t)

	eastARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")
	westARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnWestRegion, "reviewer")
	theirsARN := sfnArnCreateActivity(t, ts, sfnArnOtherAccount, sfnArnEastRegion, "reviewer")
	require.NotEqual(t, eastARN, westARN)
	require.NotEqual(t, eastARN, theirsARN)

	// Every one is describable from one endpoint, and each reports its own ARN.
	for _, arn := range []string{eastARN, westARN, theirsARN} {
		raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
			map[string]any{"activityArn": arn})
		assert.Equalf(t, arn, sfnArnMember(t, raw, "activityArn"), "DescribeActivity echoed a different ARN")
		assert.Equal(t, "reviewer", sfnArnMember(t, raw, "name"))
	}

	// Deleting the other account's does not touch either of the caller's.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DeleteActivity",
		map[string]any{"activityArn": theirsARN})
	for _, arn := range []string{eastARN, westARN} {
		sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
			map[string]any{"activityArn": arn})
	}
	status, errCode, _ := sfnArnCall(t, ts, sfnArnOtherAccount, sfnArnEastRegion, "DescribeActivity",
		map[string]any{"activityArn": theirsARN})
	assert.Equal(t, "ActivityDoesNotExist", errCode)
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestSFNARN_AnAbsentActivityAnswersActivityDoesNotExistAt400 covers both activity operations, with
// the caller holding a same-named activity in its own Region.
func TestSFNARN_AnAbsentActivityAnswersActivityDoesNotExistAt400(t *testing.T) {
	ts := sfnArnServer(t)
	ownARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")
	absentARN := "arn:aws:states:" + sfnArnWestRegion + ":" + taggingTestAccount + ":activity:reviewer"

	for _, call := range sfnArnActivityOps(absentARN) {
		t.Run(call.op, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
			if call.absentIsIdempotent {
				// DeleteActivity. API_DeleteActivity publishes InvalidArn and nothing else at all, so an
				// ARN naming nothing is a 200 with an empty body (#995).
				assert.Emptyf(t, errCode, "%s: %s", call.op, raw)
				assert.Equalf(t, http.StatusOK, status, "%s: %s", call.op, raw)
				return
			}
			assert.Equalf(t, "ActivityDoesNotExist", errCode, "%s: %s", call.op, raw)
			assert.Equalf(t, http.StatusBadRequest, status, "%s", call.op)
		})
	}

	// The caller's own survives, which since #995 is the only thing saying the idempotent DeleteActivity
	// above declined to act: its 200 no longer distinguishes "did nothing" from "deleted something".
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
		map[string]any{"activityArn": ownARN})
}

// TestSFNARN_AnExecutionARNIsMintedInTheStateMachinesRegion is consequence 3 and the minting half
// together. StartExecution is called from us-east-1 against a us-west-2 state machine: the execution
// belongs to the state machine, so its ARN and its record are the state machine's, not the caller's.
func TestSFNARN_AnExecutionARNIsMintedInTheStateMachinesRegion(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, sfnArnOtherAccount, sfnArnWestRegion, "orders", "STANDARD",
		sfnArnDefinition("west"))
	execARN := sfnArnStart(t, ts, taggingTestAccount, sfnArnEastRegion, smARN, "run-1")

	assert.Equal(t,
		"arn:aws:states:"+sfnArnWestRegion+":"+sfnArnOtherAccount+":execution:orders:run-1",
		execARN,
		"the execution ARN carries the caller's account or Region rather than the state machine's")

	// Every execution operation resolves that ARN from either endpoint, which is what proves the
	// record was written at the key the ARN addresses.
	for _, region := range []string{sfnArnEastRegion, sfnArnWestRegion} {
		raw := sfnArnOK(t, ts, taggingTestAccount, region, "DescribeExecution",
			map[string]any{"executionArn": execARN})
		assert.Equalf(t, execARN, sfnArnMember(t, raw, "executionArn"), "DescribeExecution from %s", region)
		assert.Equalf(t, smARN, sfnArnMember(t, raw, "stateMachineArn"), "DescribeExecution from %s", region)

		sfnArnOK(t, ts, taggingTestAccount, region, "GetExecutionHistory",
			map[string]any{"executionArn": execARN})
	}

	// And ListExecutions finds it under the state machine's own account and Region, which is where
	// the execution_ids index entry had to be written for the two to agree.
	listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
		map[string]any{"stateMachineArn": smARN})
	assert.Contains(t, listed, execARN, "ListExecutions did not report an execution DescribeExecution resolves")
}

// TestSFNARN_StopExecutionAbortsTheExecutionTheARNNames pins the operation that used to abort the
// caller's own execution when handed a foreign ARN.
func TestSFNARN_StopExecutionAbortsTheExecutionTheARNNames(t *testing.T) {
	ts := sfnArnServer(t)

	// A Wait state keeps an execution from completing in one call, so there is something to abort.
	waiting := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	eastSM := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD", waiting)
	westSM := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnWestRegion, "orders", "STANDARD", waiting)

	eastExec := sfnArnStart(t, ts, taggingTestAccount, sfnArnEastRegion, eastSM, "run-1")
	westExec := sfnArnStart(t, ts, taggingTestAccount, sfnArnWestRegion, westSM, "run-1")
	require.NotEqual(t, eastExec, westExec, "two Regions must not mint one execution ARN")

	before := sfnArnMember(t,
		sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
			map[string]any{"executionArn": eastExec}), "status")

	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StopExecution",
		map[string]any{"executionArn": westExec})

	assert.Equal(t, "ABORTED", sfnArnMember(t,
		sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
			map[string]any{"executionArn": westExec}), "status"),
		"StopExecution did not abort the execution its ARN named")
	assert.Equal(t, before, sfnArnMember(t,
		sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
			map[string]any{"executionArn": eastExec}), "status"),
		"StopExecution aborted the caller's own execution instead")
}

// TestSFNARN_AnAbsentExecutionAnswersExecutionDoesNotExistAt400 covers the three execution
// operations, with the caller holding a same-named execution of a same-named state machine.
func TestSFNARN_AnAbsentExecutionAnswersExecutionDoesNotExistAt400(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)
	ownExec := sfnArnStart(t, ts, taggingTestAccount, sfnArnEastRegion, smARN, "run-1")

	for _, absent := range []string{
		// Another Region, same names.
		"arn:aws:states:" + sfnArnWestRegion + ":" + taggingTestAccount + ":execution:orders:run-1",
		// Another account, same names.
		"arn:aws:states:" + sfnArnEastRegion + ":" + sfnArnOtherAccount + ":execution:orders:run-1",
		// The caller's own state machine, an execution name it never started.
		"arn:aws:states:" + sfnArnEastRegion + ":" + taggingTestAccount + ":execution:orders:run-2",
		// A state machine that does not exist, an execution name that does. Before the fix the two
		// names were reconstructed by stripping one segment, so this pair mattered.
		"arn:aws:states:" + sfnArnEastRegion + ":" + taggingTestAccount + ":execution:invoices:run-1",
	} {
		for _, call := range sfnArnExecutionOps(absent) {
			t.Run(call.op+"/"+absent, func(t *testing.T) {
				status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
				assert.Equalf(t, "ExecutionDoesNotExist", errCode, "%s %s: %s", call.op, absent, raw)
				assert.Equalf(t, http.StatusBadRequest, status, "%s %s", call.op, absent)
			})
		}
	}

	// The caller's own is still there and still not ABORTED, so none of the refusals above reached it.
	assert.NotEqual(t, "ABORTED", sfnArnMember(t,
		sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
			map[string]any{"executionArn": ownExec}), "status"))
}

// TestSFNARN_AMalformedARNIsInvalidArnAt400 runs every malformed shape past all eleven operations.
// The old parser refused none of them: it took the last segment of whatever it was handed and
// reported the resource absent, which is the wrong error to give a caller and the wrong one to read
// in a log.
func TestSFNARN_AMalformedARNIsInvalidArnAt400(t *testing.T) {
	ts := sfnArnServer(t)
	// Created so that a "not found" answer would be a real possibility rather than a foregone one.
	sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD", sfnArnPassDefinition)
	sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")

	malformed := map[string]string{
		"empty":                  "",
		"a bare name":            "orders",
		"no arn prefix":          "aws:states:us-east-1:" + taggingTestAccount + ":stateMachine:orders",
		"another service":        "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":stateMachine:orders",
		"no resource portion":    "arn:aws:states:us-east-1:" + taggingTestAccount + ":stateMachine",
		"empty resource":         "arn:aws:states:us-east-1:" + taggingTestAccount + ":stateMachine:",
		"empty region":           "arn:aws:states::" + taggingTestAccount + ":stateMachine:orders",
		"empty account":          "arn:aws:states:us-east-1::stateMachine:orders",
		"unknown type":           "arn:aws:states:us-east-1:" + taggingTestAccount + ":workflow:orders",
		"lowercase statemachine": "arn:aws:states:us-east-1:" + taggingTestAccount + ":statemachine:orders",
	}

	for label, arn := range malformed {
		calls := sfnArnStateMachineOps(arn)
		calls = append(calls, sfnArnActivityOps(arn)...)
		calls = append(calls, sfnArnExecutionOps(arn)...)
		for _, call := range calls {
			// ListExecutions is the one operation whose stateMachineArn is "Required: No", so an empty
			// one is not a malformed ARN — it is the "neither ARN supplied" case, which
			// TestSFNARN_ListExecutionsRequiresExactlyOneOfItsTwoARNs asserts answers
			// ValidationException. Every other shape below is malformed at every operation including
			// this one.
			if arn == "" && call.op == "ListExecutions" {
				continue
			}
			t.Run(label+"/"+call.op, func(t *testing.T) {
				status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
				assert.Equalf(t, "InvalidArn", errCode, "%s %s: %s", call.op, arn, raw)
				assert.Equalf(t, http.StatusBadRequest, status, "%s %s", call.op, arn)
			})
		}
	}
}

// TestSFNARN_AWellFormedARNOfTheWrongTypeIsInvalidArn is consequence 2. The resource may well exist;
// it is the ARN that does not belong at this operation, which is the decision #910 recorded for an
// execution ARN at a tagging operation.
func TestSFNARN_AWellFormedARNOfTheWrongTypeIsInvalidArn(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)
	activityARN := sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, "reviewer")
	execARN := sfnArnStart(t, ts, taggingTestAccount, sfnArnEastRegion, smARN, "run-1")

	// A state-machine operation handed an activity or an execution ARN. Note the old parser resolved
	// an activity ARN's last segment — "reviewer" — as a state machine name, and an execution ARN's
	// last segment as one too, so DescribeStateMachine on an execution ARN looked for a state machine
	// named after the *execution*.
	for _, call := range sfnArnStateMachineOps(activityARN) {
		t.Run("activity ARN at "+call.op, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
			assert.Equalf(t, "InvalidArn", errCode, "%s: %s", call.op, raw)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
	for _, call := range sfnArnStateMachineOps(execARN) {
		t.Run("execution ARN at "+call.op, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
			assert.Equalf(t, "InvalidArn", errCode, "%s: %s", call.op, raw)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
	// An activity operation handed a state-machine or an execution ARN.
	for _, arn := range []string{smARN, execARN} {
		for _, call := range sfnArnActivityOps(arn) {
			t.Run("non-activity at "+call.op, func(t *testing.T) {
				status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
				assert.Equalf(t, "InvalidArn", errCode, "%s %s: %s", call.op, arn, raw)
				assert.Equal(t, http.StatusBadRequest, status)
			})
		}
	}
	// An execution operation handed a state-machine or an activity ARN.
	for _, arn := range []string{smARN, activityARN} {
		for _, call := range sfnArnExecutionOps(arn) {
			t.Run("non-execution at "+call.op, func(t *testing.T) {
				status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
				assert.Equalf(t, "InvalidArn", errCode, "%s %s: %s", call.op, arn, raw)
				assert.Equal(t, http.StatusBadRequest, status)
			})
		}
	}

	// Every resource named above is still exactly where it was: a refusal must not be a write.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeActivity",
		map[string]any{"activityArn": activityARN})
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
		map[string]any{"executionArn": execARN})
}

// TestSFNARN_AVersionOrAliasARNIsRefused records a deliberate boundary. Both shapes are well-formed
// at AWS and both name a resource substrate has no record of, so resolving either to the unqualified
// state machine would hand a caller a different resource from the one it asked for — the defect this
// work exists to end.
func TestSFNARN_AVersionOrAliasARNIsRefused(t *testing.T) {
	ts := sfnArnServer(t)
	sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD", sfnArnPassDefinition)

	base := "arn:aws:states:" + sfnArnEastRegion + ":" + taggingTestAccount + ":stateMachine:orders"
	for _, qualified := range []string{base + ":1", base + ":live"} {
		for _, call := range sfnArnStateMachineOps(qualified) {
			t.Run(qualified+"/"+call.op, func(t *testing.T) {
				status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
				assert.Equalf(t, "InvalidArn", errCode, "%s %s: %s", call.op, qualified, raw)
				assert.Equal(t, http.StatusBadRequest, status)
			})
		}
	}

	// The unqualified ARN still works, so the refusal is about the qualifier and not the name.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": base})
}

// TestSFNARN_AnExpressExecutionARNResolvesToNothing records the other boundary. StartSyncExecution
// mints an "express:" ARN and stores no record for it, so such an ARN answers ExecutionDoesNotExist
// rather than being refused for its shape — refusing it would claim AWS rejects a shape it mints.
func TestSFNARN_AnExpressExecutionARNResolvesToNothing(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "fast", "EXPRESS",
		sfnArnPassDefinition)
	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartSyncExecution",
		map[string]any{"stateMachineArn": smARN, "name": "sync-1"})
	execARN := sfnArnMember(t, raw, "executionArn")
	require.Contains(t, execARN, ":express:fast:sync-1")

	// The ARN StartSyncExecution just reported, and an express ARN carrying the trailing identifier
	// AWS's own express ARNs may carry, both resolve to nothing rather than to InvalidArn.
	for _, arn := range []string{execARN, execARN + ":a1b2c3d4"} {
		for _, call := range sfnArnExecutionOps(arn) {
			t.Run(call.op, func(t *testing.T) {
				status, errCode, body := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, call.op, call.body)
				assert.Equalf(t, "ExecutionDoesNotExist", errCode, "%s %s: %s", call.op, arn, body)
				assert.Equal(t, http.StatusBadRequest, status)
			})
		}
	}
}

// TestSFNARN_StartSyncExecutionMintsInTheStateMachinesRegion covers the second minting site.
func TestSFNARN_StartSyncExecutionMintsInTheStateMachinesRegion(t *testing.T) {
	ts := sfnArnServer(t)

	smARN := sfnArnCreateSM(t, ts, sfnArnOtherAccount, sfnArnWestRegion, "fast", "EXPRESS",
		sfnArnPassDefinition)
	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartSyncExecution",
		map[string]any{"stateMachineArn": smARN, "name": "sync-1"})

	assert.Equal(t,
		"arn:aws:states:"+sfnArnWestRegion+":"+sfnArnOtherAccount+":express:fast:sync-1",
		sfnArnMember(t, raw, "executionArn"),
		"the express execution ARN carries the caller's account or Region rather than the state machine's")
}

// TestSFNARN_ListExecutionsRequiresExactlyOneOfItsTwoARNs covers the one operation whose
// stateMachineArn is "Required: No". Both halves of AWS's sentence are answered, because an empty
// executions list is the one answer a caller cannot tell a refusal from.
func TestSFNARN_ListExecutionsRequiresExactlyOneOfItsTwoARNs(t *testing.T) {
	ts := sfnArnServer(t)
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnPassDefinition)
	mapRunARN := "arn:aws:states:" + sfnArnEastRegion + ":" + taggingTestAccount +
		":mapRun:orders/run-1:e3f4a5b6-0000-0000-0000-000000000000"

	t.Run("neither is a ValidationException", func(t *testing.T) {
		status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
			map[string]any{})
		assert.Equalf(t, "ValidationException", errCode, "%s", raw)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("both is a ValidationException", func(t *testing.T) {
		status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
			map[string]any{"stateMachineArn": smARN, "mapRunArn": mapRunARN})
		assert.Equalf(t, "ValidationException", errCode, "%s", raw)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("a mapRunArn alone is ResourceNotFound", func(t *testing.T) {
		// A Map Run is not modeled: nothing in the plugin mints one, so the ARN names a resource
		// substrate has no record of. ResourceNotFound is published on this page.
		status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
			map[string]any{"mapRunArn": mapRunARN})
		assert.Equalf(t, "ResourceNotFound", errCode, "%s", raw)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("a stateMachineArn alone succeeds", func(t *testing.T) {
		sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
			map[string]any{"stateMachineArn": smARN})
	})
}

// TestSFNARN_ListExecutionsRefusesAnAbsentStateMachineRatherThanReportingNone is the check
// ListExecutions had none of. An empty list and a state machine that is not there were the same
// answer, which is the one pair a caller polling for executions cannot distinguish.
func TestSFNARN_ListExecutionsRefusesAnAbsentStateMachineRatherThanReportingNone(t *testing.T) {
	ts := sfnArnServer(t)

	// An existing state machine with no executions must still answer 200 with an empty list — the
	// refusal below has to be about existence, not about emptiness.
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "quiet", "STANDARD",
		sfnArnPassDefinition)
	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
		map[string]any{"stateMachineArn": smARN})
	var listed struct {
		Executions []map[string]any `json:"executions"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &listed))
	assert.Empty(t, listed.Executions, "a state machine with no executions must report none, not refuse")

	absent := "arn:aws:states:" + sfnArnEastRegion + ":" + taggingTestAccount + ":stateMachine:never-created"
	status, errCode, body := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "ListExecutions",
		map[string]any{"stateMachineArn": absent})
	assert.Equalf(t, "StateMachineDoesNotExist", errCode, "%s", body)
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestSFNARN_UpdateStateMachineWritesToTheARNsRecord pins the write side of the state-machine ARN.
func TestSFNARN_UpdateStateMachineWritesToTheARNsRecord(t *testing.T) {
	ts := sfnArnServer(t)

	eastARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("east"))
	westARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnWestRegion, "orders", "STANDARD",
		sfnArnDefinition("west"))

	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "UpdateStateMachine", map[string]any{
		"stateMachineArn": westARN,
		"definition":      sfnArnDefinition("rewritten"),
		"roleArn":         "arn:aws:iam::" + taggingTestAccount + ":role/Rewritten",
	})

	west := sfnArnOK(t, ts, taggingTestAccount, sfnArnWestRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": westARN})
	assert.Contains(t, sfnArnMember(t, west, "definition"), `"Comment":"rewritten"`)
	assert.Equal(t, "arn:aws:iam::"+taggingTestAccount+":role/Rewritten", sfnArnMember(t, west, "roleArn"))

	east := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": eastARN})
	assert.Contains(t, sfnArnMember(t, east, "definition"), `"Comment":"east"`,
		"UpdateStateMachine rewrote the caller's own state machine instead of the one its ARN named")
}

// TestSFNARN_TheTaggingOperationsAndTheRestResolveOneARN is #765's cross-readability criterion for
// this change: the tagging operations were fixed in #910 and the other eleven here, and one parser
// now serves both, so a tag written through an ARN has to be readable back through it.
func TestSFNARN_TheTaggingOperationsAndTheRestResolveOneARN(t *testing.T) {
	ts := sfnArnServer(t)

	eastARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "orders", "STANDARD",
		sfnArnDefinition("east"))
	westARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnWestRegion, "orders", "STANDARD",
		sfnArnDefinition("west"))

	// Tag the us-west-2 one from the us-east-1 endpoint.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "TagResource", map[string]any{
		"resourceArn": westARN,
		"tags":        []map[string]string{{"key": "env", "value": "prod"}},
	})

	// It reads back through the same ARN, from its own Region's endpoint.
	tagged := sfnArnOK(t, ts, taggingTestAccount, sfnArnWestRegion, "ListTagsForResource",
		map[string]any{"resourceArn": westARN})
	assert.Contains(t, tagged, `"key":"env"`)
	assert.Contains(t, tagged, `"value":"prod"`)

	// And the same-named state machine in the caller's own Region has none.
	untagged := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListTagsForResource",
		map[string]any{"resourceArn": eastARN})
	assert.NotContains(t, untagged, `"key":"env"`,
		"a tag written through one Region's ARN reached another Region's state machine")

	// Deleting the tagged state machine leaves the untagged one, tags and all.
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DeleteStateMachine",
		map[string]any{"stateMachineArn": westARN})
	sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListTagsForResource",
		map[string]any{"resourceArn": eastARN})
}

// TestSFNARN_AQualifiedLambdaARNInvokesTheFunctionNotTheAlias covers the one ARN in this work that
// is not a Step Functions ARN.
//
// A Task state's Resource is Lambda's ARN, and the executor read its last segment through the same
// extractSMNameFromARN the eleven operations used — so
// arn:aws:lambda:…:function:score:PROD invoked a function named "PROD". The ASL executor reaches
// Lambda through the shared plugin registry, which StartTestServer wires, so this runs over the wire
// like the rest of the file.
func TestSFNARN_AQualifiedLambdaARNInvokesTheFunctionNotTheAlias(t *testing.T) {
	ts := sfnArnServer(t)

	// Create the function through Lambda's own REST endpoint.
	createFn := `{"FunctionName":"score","Runtime":"python3.12",` +
		`"Role":"arn:aws:iam::` + taggingTestAccount + `:role/lambda",` +
		`"Handler":"index.handler","Code":{"ZipFile":""}}`
	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodPost,
		"lambda."+sfnArnEastRegion+".amazonaws.com", "lambda", sfnArnEastRegion,
		"/2015-03-31/functions", []byte(createFn), "application/json")
	require.Truef(t, status == http.StatusCreated || status == http.StatusOK,
		"CreateFunction: status %d, %s", status, raw)

	qualified := "arn:aws:lambda:" + sfnArnEastRegion + ":" + taggingTestAccount + ":function:score:PROD"
	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + qualified + `","End":true}}}`
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "scorer", "STANDARD", def)

	execARN := sfnArnStart(t, ts, taggingTestAccount, sfnArnEastRegion, smARN, "run-1")
	described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
		map[string]any{"executionArn": execARN})

	// Before the fix the function name was the ARN's last segment, "PROD", which no function answers
	// to — Invoke refused, the Task failed and the execution reported FAILED. So SUCCEEDED alone
	// distinguishes the fix from the defect.
	assert.Equalf(t, "SUCCEEDED", sfnArnMember(t, described, "status"),
		"a qualified Lambda ARN did not resolve to the function: %s", described)
	assert.NotContains(t, described, "PROD", "the alias was used as the function name")

	// And the Task's output is Lambda's own stub payload rather than aslInvokeResource's empty object,
	// which is what proves the invoke reached the Lambda plugin instead of falling through to the
	// not-a-Lambda-ARN path — the other way this assertion could pass without the dispatch working.
	assert.Containsf(t, sfnArnMember(t, described, "output"), `"statusCode":200`,
		"the Task did not invoke Lambda at all: %s", described)
}

// TestSFNARN_AnOptimizedIntegrationResourceIsNotALambdaFunction covers the other half of the same
// extraction. arn:aws:states:::lambda:invoke contains ":lambda:" and so passed the old dispatch test,
// which then invoked a function named "invoke".
func TestSFNARN_AnOptimizedIntegrationResourceIsNotALambdaFunction(t *testing.T) {
	ts := sfnArnServer(t)

	def := `{"StartAt":"T","States":{"T":{"Type":"Task",` +
		`"Resource":"arn:aws:states:::lambda:invoke","End":true}}}`
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "optimized", "STANDARD", def)

	execARN := sfnArnStart(t, ts, taggingTestAccount, sfnArnEastRegion, smARN, "run-1")
	described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeExecution",
		map[string]any{"executionArn": execARN})

	// The optimized integration is not modeled, so the Task returns aslInvokeResource's empty object
	// and the execution succeeds. Before the fix the resource contained ":lambda:" and so passed the
	// dispatch test, Lambda was asked for a function named "invoke", and the execution reported FAILED.
	assert.Equalf(t, "SUCCEEDED", sfnArnMember(t, described, "status"),
		"an optimized-integration resource was dispatched as a Lambda function: %s", described)
	assert.NotContainsf(t, sfnArnMember(t, described, "output"), `"statusCode"`,
		"the optimized integration reached Lambda: %s", described)
	assert.NotContains(t, strings.ToLower(described), "invoke\":")
}
