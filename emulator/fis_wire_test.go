package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestFISWire_ResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for FISTemplate and FISExperiment (#1619).
//
// Both records declare their owner as `account_id` and `region`. Neither reaches a body: every
// response renders through fisTemplateWire, fisExperimentWire or a summary. Every operation that
// answers a template or an experiment is driven.
func TestFISWire_ResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.FISPlugin{}
	ctx, state := wireSetup(t, p, "req-fis-wire")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "fis", method, path, body)
	}
	created := call(http.MethodPost, "/experimentTemplates", map[string]any{
		"description": "wire", "roleArn": "arn:aws:iam::123456789012:role/wire",
		"stopConditions": []any{map[string]any{"source": "none"}},
		"actions":        map[string]any{"w": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"}}},
		"tags":           map[string]string{"k": "v"},
	})
	var tmpl struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	require.NoError(t, json.Unmarshal(created, &tmpl))
	id := tmpl.ExperimentTemplate.ID
	wireRequireHeld(t, state, "fis", "template:123456789012/us-east-1/"+id, "account_id", "region")

	started := call(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": id})
	var exp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}
	require.NoError(t, json.Unmarshal(started, &exp))
	wireRequireHeld(t, state, "fis", "experiment:123456789012/us-east-1/"+exp.Experiment.ID, "account_id", "region")

	members := []string{"AccountID", "account_id", "Region", "client_token", "due_at", "resolved"}
	wireRunJSON(t, members, []wireCase{
		{op: "CreateExperimentTemplate", held: created, anchor: `"experimentTemplate"`},
		{op: "GetExperimentTemplate", call: func() []byte { return call(http.MethodGet, "/experimentTemplates/"+id, nil) }, anchor: id},
		{op: "ListExperimentTemplates", call: func() []byte { return call(http.MethodGet, "/experimentTemplates", nil) }, anchor: id},
		{op: "UpdateExperimentTemplate", call: func() []byte {
			return call(http.MethodPatch, "/experimentTemplates/"+id, map[string]any{"description": "wire2"})
		}, anchor: "wire2"},
		{op: "StartExperiment", held: started, anchor: `"experiment"`},
		{op: "GetExperiment", call: func() []byte { return call(http.MethodGet, "/experiments/"+exp.Experiment.ID, nil) }, anchor: exp.Experiment.ID},
		{op: "ListExperiments", call: func() []byte { return call(http.MethodGet, "/experiments", nil) }, anchor: exp.Experiment.ID},
		{op: "StopExperiment", call: func() []byte { return call(http.MethodDelete, "/experiments/"+exp.Experiment.ID, nil) }, anchor: "stopping"},
		{op: "DeleteExperimentTemplate", call: func() []byte { return call(http.MethodDelete, "/experimentTemplates/"+id, nil) }, anchor: id},
	})
}
