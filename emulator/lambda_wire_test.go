package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestLambdaWire_FunctionResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for LambdaFunction (#756).
//
// LambdaFunction's one bookkeeping member is EverTagged, as `ever_tagged,omitempty`, set only by a tag
// write (#938). Until it is set every response is missing it for free — #1304's vacuous assertion — so
// the function is tagged and the flag read back before any response is walked. Every operation that
// answers the function is driven, except TagResource, which answers 204 with no body. The
// event-source-mapping and permission operations answer records of their own.
func TestLambdaWire_FunctionResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.LambdaPlugin{}
	ctx, state := wireSetup(t, p, "req-lambda-wire")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "lambda", method, path, body)
	}

	created := call(http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "wire-fn", "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/wire",
	})
	var fn struct {
		FunctionArn string `json:"FunctionArn"`
	}
	require.NoError(t, json.Unmarshal(created, &fn), "decode CreateFunction: %s", created)

	tagsPath := "/2017-03-31/tags/" + fn.FunctionArn
	call(http.MethodPost, tagsPath, map[string]any{"Tags": map[string]string{"team": "wire"}})
	record := wireRequireHeld(t, state, "lambda", "function:123456789012/us-east-1/wire-fn")
	require.JSONEq(t, "true", string(record["ever_tagged"]), "the function must persist ever_tagged before an absence assertion on it means anything")

	fnPath := "/2015-03-31/functions/wire-fn"
	wireRunJSON(t, []string{"EverTagged", "ever_tagged"}, []wireCase{
		{op: "CreateFunction", held: created, anchor: `"FunctionName":"wire-fn"`},
		{op: "GetFunction", call: func() []byte { return call(http.MethodGet, fnPath, nil) }, anchor: `"FunctionName":"wire-fn"`},
		{op: "ListFunctions", call: func() []byte { return call(http.MethodGet, "/2015-03-31/functions", nil) }, anchor: `"FunctionName":"wire-fn"`},
		{op: "ListTags", call: func() []byte { return call(http.MethodGet, tagsPath, nil) }, anchor: `"team"`},
		{op: "UpdateFunctionConfiguration", call: func() []byte {
			return call(http.MethodPut, fnPath+"/configuration", map[string]any{"Description": "wire"})
		}, anchor: `"FunctionName":"wire-fn"`},
		{op: "UpdateFunctionCode", call: func() []byte {
			return call(http.MethodPut, fnPath+"/code", map[string]any{"ZipFile": "UEsDBA=="})
		}, anchor: `"FunctionName":"wire-fn"`},
	})
}
