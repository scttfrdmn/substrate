package emulator_test

// Eight JSON list handlers rewrote an out-of-range page size silently (#1131). Each now refuses a
// value outside its operation's published Valid Range with that service's validation code, takes the
// published default (or substrate's own, where none is published) for an absent member, and accepts
// both ends of the range. Step Functions' three are the exception at the low end, because their pages
// publish "A value of 0 uses the default": 0 is accepted there and only a negative value is refused.
//
// Every assertion is over the wire, on the code and the status.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// pageSizeOp is one list operation and the request it needs besides its page size.
type pageSizeOp struct {
	name, host, target, op, member string
	// base returns the request's other members, creating whatever the operation needs to address.
	base func(t *testing.T, ts *emulator.TestServer) map[string]any
	// refuseCode is the code an out-of-range value answers.
	refuseCode string
	// accepted and refused are values on and just past each end of the range.
	accepted, refused []int
}

func pageSizeOps() []pageSizeOp {
	empty := func(*testing.T, *emulator.TestServer) map[string]any { return map[string]any{} }
	return []pageSizeOp{
		{
			name: "secretsmanager/ListSecrets", host: "secretsmanager.us-east-1.amazonaws.com",
			target: "secretsmanager", op: "ListSecrets", member: "MaxResults", base: empty,
			refuseCode: "InvalidParameterException", accepted: []int{1, 100}, refused: []int{-1, 0, 101},
		},
		{
			name: "stepfunctions/ListStateMachines", host: "states.us-east-1.amazonaws.com",
			target: "AWSStepFunctions", op: "ListStateMachines", member: "maxResults", base: empty,
			refuseCode: "ValidationError", accepted: []int{0, 1, 1000}, refused: []int{-1, 1001},
		},
		{
			name: "stepfunctions/ListActivities", host: "states.us-east-1.amazonaws.com",
			target: "AWSStepFunctions", op: "ListActivities", member: "maxResults", base: empty,
			refuseCode: "ValidationError", accepted: []int{0, 1, 1000}, refused: []int{-1, 1001},
		},
		{
			name: "stepfunctions/ListExecutions", host: "states.us-east-1.amazonaws.com",
			target: "AWSStepFunctions", op: "ListExecutions", member: "maxResults",
			base: func(t *testing.T, ts *emulator.TestServer) map[string]any {
				return map[string]any{"stateMachineArn": pageSizeStateMachine(t, ts)}
			},
			refuseCode: "ValidationException", accepted: []int{0, 1, 1000}, refused: []int{-1, 1001},
		},
		{
			name: "ssm/GetParametersByPath", host: "ssm.us-east-1.amazonaws.com",
			target: "AmazonSSM", op: "GetParametersByPath", member: "MaxResults",
			base: func(*testing.T, *emulator.TestServer) map[string]any {
				return map[string]any{"Path": "/app"}
			},
			refuseCode: "ValidationError", accepted: []int{1, 10}, refused: []int{-1, 0, 11},
		},
		{
			name: "ssm/DescribeParameters", host: "ssm.us-east-1.amazonaws.com",
			target: "AmazonSSM", op: "DescribeParameters", member: "MaxResults", base: empty,
			refuseCode: "ValidationError", accepted: []int{1, 50}, refused: []int{-1, 0, 51},
		},
		{
			name: "kms/ListKeys", host: "kms.us-east-1.amazonaws.com",
			target: "TrentService", op: "ListKeys", member: "Limit", base: empty,
			refuseCode: "ValidationError", accepted: []int{1, 1000}, refused: []int{-1, 0, 1001},
		},
		{
			name: "kms/ListAliases", host: "kms.us-east-1.amazonaws.com",
			target: "TrentService", op: "ListAliases", member: "Limit", base: empty,
			refuseCode: "ValidationError", accepted: []int{1, 100}, refused: []int{-1, 0, 101},
		},
	}
}

// pageSizeStateMachine creates a state machine for ListExecutions to address and returns its ARN.
func pageSizeStateMachine(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	status, raw := cicdCall(t, ts, "states.us-east-1.amazonaws.com", "AWSStepFunctions", "CreateStateMachine",
		map[string]any{
			"name":       "page-size",
			"definition": `{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}}`,
			"roleArn":    "arn:aws:iam::123456789012:role/StepFunctionsRole",
		})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var out struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out.StateMachineArn
}

// TestPageSizeRange_OutOfRangeIsRefused pins each operation's published range at both ends.
func TestPageSizeRange_OutOfRangeIsRefused(t *testing.T) {
	for _, tc := range pageSizeOps() {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			base := tc.base(t, ts)
			call := func(value *int) (int, []byte) {
				body := map[string]any{}
				for k, v := range base {
					body[k] = v
				}
				if value != nil {
					body[tc.member] = *value
				}
				return cicdCall(t, ts, tc.host, tc.target, tc.op, body)
			}

			status, raw := call(nil)
			assert.Equal(t, http.StatusOK, status, "an absent %s takes the default: %s", tc.member, raw)
			for _, v := range tc.accepted {
				status, raw := call(&v)
				assert.Equal(t, http.StatusOK, status, "%s=%d is inside the published range: %s", tc.member, v, raw)
			}
			for _, v := range tc.refused {
				status, raw := call(&v)
				assert.Equal(t, http.StatusBadRequest, status, "%s=%d: %s", tc.member, v, raw)
				assert.Equal(t, tc.refuseCode, cicdCode(t, raw), "%s=%d: %s", tc.member, v, raw)
				assert.Contains(t, string(raw), fmt.Sprint(v), "the message names the value sent")
			}
		})
	}
}

// TestPageSizeRange_KMSListAliasesDefaultsToFifty asserts the published default by its observable
// consequence: sixty aliases and no Limit are two pages, not one.
func TestPageSizeRange_KMSListAliasesDefaultsToFifty(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const host, target = "kms.us-east-1.amazonaws.com", "TrentService"

	status, raw := cicdCall(t, ts, host, target, "CreateKey", map[string]any{})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var key struct {
		KeyMetadata struct {
			KeyID string `json:"KeyId"`
		} `json:"KeyMetadata"`
	}
	require.NoError(t, json.Unmarshal(raw, &key))
	for i := range 60 {
		status, raw := cicdCall(t, ts, host, target, "CreateAlias", map[string]any{
			"AliasName":   fmt.Sprintf("alias/page-%02d", i),
			"TargetKeyId": key.KeyMetadata.KeyID,
		})
		require.Equal(t, http.StatusOK, status, "%s", raw)
	}

	type page struct {
		Aliases    []map[string]any `json:"Aliases"`
		NextMarker string           `json:"NextMarker"`
		Truncated  bool             `json:"Truncated"`
	}
	list := func(body map[string]any) page {
		status, raw := cicdCall(t, ts, host, target, "ListAliases", body)
		require.Equal(t, http.StatusOK, status, "%s", raw)
		var p page
		require.NoError(t, json.Unmarshal(raw, &p))
		return p
	}

	first := list(map[string]any{})
	assert.Len(t, first.Aliases, 50, "API_ListAliases: \"If you do not include a value, it defaults to 50.\"")
	assert.True(t, first.Truncated)
	require.NotEmpty(t, first.NextMarker)

	second := list(map[string]any{"Marker": first.NextMarker})
	assert.Len(t, second.Aliases, 10)
	assert.False(t, second.Truncated)
	assert.Empty(t, second.NextMarker)
}
