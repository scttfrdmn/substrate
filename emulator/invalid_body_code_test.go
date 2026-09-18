package emulator_test

// Every body-parse guard in Step Functions and Systems Manager, over the wire (#950).
//
// The guards existed before this file and nothing reached them. A test that builds its request from a
// Go value cannot: json.Marshal produces valid JSON by construction, so the only way into a
// json.Unmarshal failure is to hand the server bytes that are not JSON, which is what
// [rawSignedCall] is for. That is why a green suite held InvalidRequest at fifteen Step Functions
// sites and twelve Systems Manager ones — a code neither service publishes anywhere.
//
// Each table below names every operation that carries a guard, rather than a representative sample.
// The defect was per-site duplication of one literal, so the assertion that matters is that no site
// was missed, and a sample cannot make it.
//
// Both the status and the code are asserted. #923 established why: a decoded error struct carries the
// code, and a consumer's retry logic branches on the status, so a helper that got one of the two right
// would look correct from either side alone.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scttfrdmn/substrate/emulator"
)

// invalidBodyPayload is the body every case in this file sends.
//
// It is unbalanced rather than merely empty: an empty body decodes as the zero value of the handler's
// input struct, which is a successful parse, and "null" does too. This is the shortest thing
// encoding/json refuses outright.
const invalidBodyPayload = `{"`

// sfnGuardedOperations is every Step Functions operation whose handler guards json.Unmarshal.
//
// Fifteen are in stepfunctions_plugin.go and three in stepfunctions_tags.go. All eighteen route through
// one host and target in substrate, StartSyncExecution included, so one table covers both files.
//
// The three list operations joined the table with #1007: they treat the body as optional and had been
// discarding the error from a body that was present, which is a different decision from accepting an
// absent one. TestInvalidBodyLeavesAnAbsentBodyAlone asserts that the second still holds.
var sfnGuardedOperations = []string{
	"CreateStateMachine",
	"DescribeStateMachine",
	"UpdateStateMachine",
	"DeleteStateMachine",
	"StartExecution",
	"StartSyncExecution",
	"StopExecution",
	"DescribeExecution",
	"GetExecutionHistory",
	"CreateActivity",
	"DescribeActivity",
	"DeleteActivity",
	"TagResource",
	"UntagResource",
	"ListTagsForResource",
	"ListStateMachines",
	"ListExecutions",
	"ListActivities",
}

// ssmGuardedOperations is every Systems Manager operation whose handler guards json.Unmarshal.
//
// Ten answered InvalidRequest and the last two, SendCommand and GetCommandInvocation, answered
// SerializationException — the same defect under a second code neither more published than the first,
// and the reason this list is twelve where the issue counted ten. DescribeParameters is the thirteenth,
// added by #1007: it treats the body as optional and was discarding the error from a body that was
// present, which is a different decision from accepting an absent one.
var ssmGuardedOperations = []string{
	"PutParameter",
	"GetParameter",
	"GetParameters",
	"DeleteParameter",
	"DeleteParameters",
	"GetParametersByPath",
	"GetParameterHistory",
	"AddTagsToResource",
	"RemoveTagsFromResource",
	"ListTagsForResource",
	"SendCommand",
	"GetCommandInvocation",
	"DescribeParameters",
}

// TestStepFunctionsInvalidBodyAnswersValidationError asserts every Step Functions guard answers the
// published common error.
//
// ValidationError at 400 is what CommonErrors.html publishes, and it is the whole of what is
// available: the failure belongs to no single operation, and ValidationException — which Step
// Functions does publish at 400 — is in the Errors section of only five of these fifteen operations,
// so answering it would leave the other ten reporting a code their own page does not carry.
func TestStepFunctionsInvalidBodyAnswersValidationError(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))

	for _, op := range sfnGuardedOperations {
		t.Run(op, func(t *testing.T) {
			status, code, message := rawSignedCall(t, ts, statesTarget, taggingTestAccount, op,
				[]byte(invalidBodyPayload))
			assert.Equal(t, "ValidationError", code, "%s answers the published common error", op)
			assert.Equal(t, http.StatusBadRequest, status, "%s answers 400", op)
			assertNoDecoderText(t, op, message)
		})
	}
}

// TestSystemsManagerInvalidBodyAnswersValidationError asserts every Systems Manager guard answers the
// published common error.
//
// All twelve operation pages were read and not one publishes an error for a request that could not be
// read at all — each lists only narrow resource-specific 400s plus InternalServerError — so
// CommonErrors.html is not a convenience here but the only place the answer can come from.
func TestSystemsManagerInvalidBodyAnswersValidationError(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))

	for _, op := range ssmGuardedOperations {
		t.Run(op, func(t *testing.T) {
			status, code, message := rawSignedCall(t, ts, ssmTagTarget, taggingTestAccount, op,
				[]byte(invalidBodyPayload))
			assert.Equal(t, "ValidationError", code, "%s answers the published common error", op)
			assert.Equal(t, http.StatusBadRequest, status, "%s answers 400", op)
			assertNoDecoderText(t, op, message)
		})
	}
}

// assertNoDecoderText asserts a refusal describes the request rather than the emulator's decoder.
//
// SendCommand and GetCommandInvocation passed encoding/json's error text through as the message, so a
// caller was told which Go struct field failed to unmarshal by an endpoint that is meant to look like
// AWS. The three fragments are what that text is built from — "json: cannot unmarshal … into Go value
// of type …" and "unexpected end of JSON input" — and none of them can occur in a message substrate
// wrote itself.
func assertNoDecoderText(t *testing.T, op, message string) {
	t.Helper()
	for _, fragment := range []string{"json:", "Go value", "JSON input"} {
		assert.NotContainsf(t, message, fragment,
			"%s reports the request rather than the emulator's decoder", op)
	}
	assert.NotEmptyf(t, strings.TrimSpace(message), "%s carries a message", op)
}
