package emulator_test

// Amazon States Language structural validation (#1073).
//
// #996 made CreateStateMachine and UpdateStateMachine refuse a definition substrate could not read
// back. This is the rest: a definition that reads back perfectly and still cannot run — no states, a
// StartAt naming nothing, a Parallel branch transitioning into its sibling — created a state machine
// and answered 200.
//
// Every refusal below cites a sentence AWS publishes, in AWS's own words. The issue quoted three
// capitalised-"MUST" sentences from states-language.net, which AWS links but does not host; none of
// the six docs.aws.amazon.com/step-functions/ pages checked uses capitalised MUST at all. Where AWS
// publishes nothing, there is no rule — TestSFNASL_AcceptsWhatAWSDoesNotRefuse is the half of this
// file that keeps the validator from inventing one.
//
// The tests go over the wire through CreateStateMachine rather than calling the validator, because
// the observable fact is the status and the code, and because the state machine must not be stored.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sfnASLPass is the minimal valid document, used as the surrounding state machine wherever a test is
// about one nested fault rather than the top level.
const sfnASLPass = `{"StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`

// sfnASLRefusals is every structural fault, the published rule it breaks, and the fragment of the
// message that names it.
//
// A fragment rather than the whole message, because the scope prefix ("state %q branch 0 state %q")
// is a formatting detail while the named state and the named field are the contract: the acceptance
// criterion #1073 was most specific about is that a caller fixing a generated document is told which
// state is wrong.
//
//nolint:gochecknoglobals // table shared by the create and update tests
var sfnASLRefusals = map[string]struct{ definition, wantFault string }{
	// "States (Required) An object containing a comma-delimited set of states."
	//
	// Absent and empty are different documents and answer different messages, because
	// States is a map and encoding/json leaves it nil for an absent member — the same
	// absent-versus-empty distinction #1062 needed for WAFv2's Addresses.
	"an empty States object": {
		`{"StartAt":"Start","States":{}}`,
		"the definition has an empty States object",
	},

	// "All states must have a Type field", and the eight published values are the eight
	// ASLState.Type's doc comment has always enumerated.
	"a state with no Type": {
		`{"StartAt":"Start","States":{"Start":{"End":true}}}`,
		`state "Start" has no Type field`,
	},
	"a Type AWS does not publish": {
		`{"StartAt":"Start","States":{"Start":{"Type":"Loop","End":true}}}`,
		`state "Start" has Type "Loop", which is not one of`,
	},

	// "Only one of Next or End can be used in a state."
	"both Next and End": {
		`{"StartAt":"A","States":{` +
			`"A":{"Type":"Pass","Next":"B","End":true},` +
			`"B":{"Type":"Pass","End":true}}}`,
		`state "A" has both Next and End`,
	},
	"neither Next nor End": {
		`{"StartAt":"Start","States":{"Start":{"Type":"Pass"}}}`,
		`state "Start" has neither Next nor End`,
	},
	"a Next naming no state": {
		`{"StartAt":"Start","States":{"Start":{"Type":"Pass","Next":"Nowhere"}}}`,
		`state "Start" has Next "Nowhere", which is not the name of a state in the same States object`,
	},

	// "Some state types, such as Choice, or terminal states, such as Succeed workflow state and Fail
	// workflow state, don't support or use the End field."
	"End on a Choice state": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","End":true,"Choices":[{"Variable":"$.x","StringEquals":"y","Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`state "Pick" is a Choice state, which does not support the End field`,
	},
	"End on a Succeed state": {
		`{"StartAt":"Done","States":{"Done":{"Type":"Succeed","End":true}}}`,
		`state "Done" is a Succeed state, which does not support the End field`,
	},
	"End on a Fail state": {
		`{"StartAt":"Nope","States":{"Nope":{"Type":"Fail","End":true}}}`,
		`state "Nope" is a Fail state, which does not support the End field`,
	},

	// "Choice states do not support the End field. In addition, they use Next only inside their
	// Choices field." The second sentence is why a Choice state's own Next is a fault rather than an
	// alternative to End.
	"a Next on the Choice state itself": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Next":"Done","Choices":[{"Variable":"$.x","StringEquals":"y","Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`state "Pick" is a Choice state, which uses Next only inside its Choices field`,
	},

	// "Choices (Required) ... You must define at least one rule in the Choice state."
	"a Choice with no Choices": {
		`{"StartAt":"Pick","States":{"Pick":{"Type":"Choice","Default":"Pick"}}}`,
		`state "Pick" is a Choice state with no Choices`,
	},
	"a Choice with an empty Choices array": {
		`{"StartAt":"Pick","States":{"Pick":{"Type":"Choice","Choices":[]}}}`,
		`state "Pick" is a Choice state with no Choices`,
	},

	// "A Next field – The value of this field must match a state name in the state machine."
	"a Choice rule with no Next": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"Variable":"$.x","StringEquals":"y"}],"Default":"Done"},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`state "Pick" has a Choices[0] with no Next`,
	},
	"a Choice rule Next naming no state": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"Variable":"$.x","StringEquals":"y","Next":"Nowhere"}]}}}`,
		`state "Pick" has Choices[0].Next "Nowhere"`,
	},

	// Default is a state reference the issue's own Next inventory omitted: "Default (Optional,
	// Recommended) The name of the state to transition to if no Choice Rule evaluates to true."
	"a Choice Default naming no state": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"Variable":"$.x","StringEquals":"y","Next":"Pick"}],` +
			`"Default":"Nowhere"}}}`,
		`state "Pick" has Default "Nowhere"`,
	},

	// "The values of the And and Or operators must be non-empty arrays of Choice Rules that must not
	// themselves contain Next fields. Likewise, the value of a Not operator must be a single Choice
	// Rule that must not contain Next fields." / "the Next field can appear only in a top-level
	// Choice Rule."
	//
	// The target exists in all three, which is the point: a nested Next is refused for *where it is*,
	// not for what it names. A validator that resolved it against States would accept these.
	"a Next inside And": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"And":[` +
			`{"Variable":"$.x","IsPresent":true,"Next":"Done"},` +
			`{"Variable":"$.x","StringEquals":"y"}],"Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		"the Next field can appear only in a top-level Choice Rule",
	},
	"a Next inside Or": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"Or":[` +
			`{"Variable":"$.x","StringEquals":"y","Next":"Done"}],"Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`has a Next in Choices[0].Or[0]`,
	},
	"a Next inside Not": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"Not":` +
			`{"Variable":"$.x","StringEquals":"y","Next":"Done"},"Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`has a Next in Choices[0].Not`,
	},
	"an empty And array": {
		`{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"And":[],"Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`has an empty And in Choices[0]`,
	},

	// Catch's Next is a transition like any other. A Catcher with no Next has nowhere to send the
	// error it caught, so an empty one is a fault rather than something to skip.
	"a Catch with no Next": {
		`{"StartAt":"Work","States":{` +
			`"Work":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","End":true,` +
			`"Catch":[{"ErrorEquals":["States.ALL"]}]}}}`,
		`state "Work" has a Catch[0].Next with no Next`,
	},
	"a Catch Next naming no state": {
		`{"StartAt":"Work","States":{` +
			`"Work":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","End":true,` +
			`"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Nowhere"}]}}}`,
		`state "Work" has Catch[0].Next "Nowhere"`,
	},

	// "Branches (Required) An array of objects that specify state machines to execute in parallel.
	// Each such state machine object must have fields named States and StartAt, whose meanings are
	// exactly like those in the top level of a state machine." The last clause is why the nested
	// document is held to the same rules rather than a weaker set.
	"a Parallel with no Branches": {
		`{"StartAt":"Fan","States":{"Fan":{"Type":"Parallel","End":true}}}`,
		`state "Fan" is a Parallel state with no Branches`,
	},
	"a branch with no StartAt": {
		`{"StartAt":"Fan","States":{"Fan":{"Type":"Parallel","End":true,"Branches":[` +
			`{"States":{"One":{"Type":"Pass","End":true}}}]}}}`,
		`state "Fan" branch 0 has no StartAt field`,
	},

	// "Each branch must be self-contained. A state in one branch of a Parallel state must not have a
	// Next field that targets a field outside of that branch, nor can any other state outside the
	// branch transition into that branch."
	//
	// "Done" exists at the top level, so this is the case that proves each Next resolves against its
	// *own* States map rather than the outermost one.
	"a branch transitioning out of itself": {
		`{"StartAt":"Fan","States":{` +
			`"Fan":{"Type":"Parallel","Next":"Done","Branches":[` +
			`{"StartAt":"One","States":{"One":{"Type":"Pass","Next":"Done"}}}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`state "Fan" branch 0 state "One" has Next "Done"`,
	},

	// "ItemProcessor (Required)" on the Inline Map page, with Iterator accepted under a heading
	// titled "Deprecated fields". Either satisfies the requirement; neither does not.
	"a Map with no sub-state-machine": {
		`{"StartAt":"Each","States":{"Each":{"Type":"Map","End":true}}}`,
		`state "Each" is a Map state with no ItemProcessor`,
	},
	"a Map with both spellings": {
		`{"StartAt":"Each","States":{"Each":{"Type":"Map","End":true,` +
			`"Iterator":` + sfnASLPass + `,"ItemProcessor":` + sfnASLPass + `}}}`,
		`state "Each" is a Map state with both ItemProcessor and Iterator`,
	},

	// "States within the ItemProcessor field can only transition to each other. No state outside the
	// ItemProcessor field can transition to a state within it." Same shape as the Parallel case: the
	// target exists at the top level and is still refused.
	"an ItemProcessor transitioning out of itself": {
		`{"StartAt":"Each","States":{` +
			`"Each":{"Type":"Map","Next":"Done","ItemProcessor":` +
			`{"StartAt":"One","States":{"One":{"Type":"Pass","Next":"Done"}}}},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		`state "Each" ItemProcessor state "One" has Next "Done"`,
	},
	"an Iterator with an empty States object": {
		`{"StartAt":"Each","States":{"Each":{"Type":"Map","End":true,` +
			`"Iterator":{"StartAt":"One","States":{}}}}}`,
		`state "Each" Iterator has an empty States object`,
	},
}

// TestSFNASL_CreateRefusesAStructurallyInvalidDefinition checks the guard where a definition first
// arrives, and that the refusal left nothing behind.
func TestSFNASL_CreateRefusesAStructurallyInvalidDefinition(t *testing.T) {
	ts := sfnArnServer(t)

	for label, tc := range sfnASLRefusals {
		t.Run(label, func(t *testing.T) {
			name := "asl-" + strings.ReplaceAll(label, " ", "-")
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion,
				"CreateStateMachine", map[string]any{
					"name":       name,
					"definition": tc.definition,
					"roleArn":    "arn:aws:iam::" + taggingTestAccount + ":role/StepFunctionsRole",
				})
			assert.Equal(t, "InvalidDefinition", errCode, raw)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, raw, "The provided Amazon States Language definition is not valid",
				"AWS's own message for this code")
			// Against the decoded message rather than the raw body: the body escapes the quotes
			// around a state's name, and the quoting is what makes these messages readable.
			assert.Contains(t, sfnArnMember(t, raw, "Message"), tc.wantFault,
				"the message must name the offending state or field")

			listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListStateMachines",
				map[string]any{})
			assert.NotContains(t, listed, `"name":"`+name+`"`, "a refused definition was stored anyway")
		})
	}
}

// TestSFNASL_UpdateRefusesAStructurallyInvalidDefinition checks the other writer, and that the stored
// definition did not move.
//
// This is the worse of the two if it is missing: the caller's own 200 would be the last observation
// before a working state machine stopped being executable.
func TestSFNASL_UpdateRefusesAStructurallyInvalidDefinition(t *testing.T) {
	ts := sfnArnServer(t)

	original := sfnArnDefinition("original")
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "asl-update", "STANDARD",
		original)

	for label, tc := range sfnASLRefusals {
		t.Run(label, func(t *testing.T) {
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion,
				"UpdateStateMachine", map[string]any{
					"stateMachineArn": smARN,
					"definition":      tc.definition,
				})
			assert.Equal(t, "InvalidDefinition", errCode, raw)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Contains(t, sfnArnMember(t, raw, "Message"), tc.wantFault, raw)

			described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
				map[string]any{"stateMachineArn": smARN})
			assert.Equal(t, original, sfnArnMember(t, described, "definition"),
				"the stored definition moved")
		})
	}
}

// TestSFNASL_AcceptsWhatAWSDoesNotRefuse is the half that keeps the validator from inventing rules.
//
// Each of these is a document AWS accepts, and several would be refused by a plausible reading of the
// rules above: a Succeed state has neither Next nor End; a nested And carries no Next but its sibling
// comparison does; a Map uses the deprecated Iterator spelling AWS still accepts and still recommends
// to Step Functions Local; a Next on a Succeed state is pointless but nothing published refuses it.
//
// The last case is the one to read twice. `{"Type":"Succeed","Next":"X"}` can never transition,
// because Succeed is terminal — but AWS publishes only that Succeed "doesn't support or use the End
// field", and under #671 a refusal needs a published sentence, not an inference. So substrate accepts
// it and docs/services.md records it as unvalidated.
func TestSFNASL_AcceptsWhatAWSDoesNotRefuse(t *testing.T) {
	ts := sfnArnServer(t)

	for label, definition := range map[string]string{
		"a Succeed state with neither Next nor End": `{"StartAt":"Done","States":{` +
			`"Done":{"Type":"Succeed"}}}`,
		"a Fail state with neither Next nor End": `{"StartAt":"Nope","States":{` +
			`"Nope":{"Type":"Fail","Error":"Boom","Cause":"because"}}}`,
		"a Next on a Succeed state": `{"StartAt":"Done","States":{` +
			`"Done":{"Type":"Succeed","Next":"Done"}}}`,
		"a Choice with a Default and no End": `{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"Variable":"$.x","StringEquals":"y","Next":"Done"}],` +
			`"Default":"Done"},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		"nested And and Not carrying no Next": `{"StartAt":"Pick","States":{` +
			`"Pick":{"Type":"Choice","Choices":[{"And":[` +
			`{"Variable":"$.x","IsPresent":true},` +
			`{"Not":{"Variable":"$.x","StringEquals":"z"}}],"Next":"Done"}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		"a Parallel whose branches are self-contained": `{"StartAt":"Fan","States":{` +
			`"Fan":{"Type":"Parallel","Next":"Done","Branches":[` +
			`{"StartAt":"One","States":{"One":{"Type":"Pass","End":true}}},` +
			`{"StartAt":"Two","States":{"Two":{"Type":"Pass","End":true}}}]},` +
			`"Done":{"Type":"Pass","End":true}}}`,
		"a Map using the deprecated Iterator spelling": `{"StartAt":"Each","States":{` +
			`"Each":{"Type":"Map","End":true,"Iterator":` + sfnASLPass + `}}}`,
		"a Map using ItemProcessor": `{"StartAt":"Each","States":{` +
			`"Each":{"Type":"Map","End":true,"ItemProcessor":` + sfnASLPass + `}}}`,
		"an ItemProcessor carrying a ProcessorConfig substrate does not model": `{"StartAt":"Each",` +
			`"States":{"Each":{"Type":"Map","End":true,"ItemProcessor":{` +
			`"ProcessorConfig":{"Mode":"INLINE"},` +
			`"StartAt":"One","States":{"One":{"Type":"Pass","End":true}}}}}}`,
		"a Task with a Retry and a resolvable Catch": `{"StartAt":"Work","States":{` +
			`"Work":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Next":"Done",` +
			`"Retry":[{"ErrorEquals":["States.TaskFailed"],"MaxAttempts":2}],` +
			`"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Nope"}]},` +
			`"Done":{"Type":"Pass","End":true},` +
			`"Nope":{"Type":"Fail"}}}`,
		"a Wait state": `{"StartAt":"Hold","States":{` +
			`"Hold":{"Type":"Wait","Seconds":1,"Next":"Done"},` +
			`"Done":{"Type":"Pass","End":true}}}`,
	} {
		t.Run(label, func(t *testing.T) {
			arn := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion,
				"ok-"+strings.ReplaceAll(label, " ", "-"), "STANDARD", definition)
			described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
				map[string]any{"stateMachineArn": arn})
			assert.Equal(t, definition, sfnArnMember(t, described, "definition"),
				"the definition must round-trip unchanged")
		})
	}
}

// TestSFNASL_TwoFaultsReportTheSameOneEveryTime pins the property that makes the refusal replayable.
//
// States is a map, so ranging it would pick a fault at random and the same request would answer two
// different messages across runs — which breaks the event log's premise, not merely a test.
// sfnValidateASLStructure sorts the names, so the alphabetically first faulty state wins. Here that
// is "Alpha", although "Zulu" is broken too.
func TestSFNASL_TwoFaultsReportTheSameOneEveryTime(t *testing.T) {
	ts := sfnArnServer(t)

	const twoFaults = `{"StartAt":"Alpha","States":{` +
		`"Alpha":{"Type":"Pass","Next":"Nowhere"},` +
		`"Zulu":{"Type":"Pass","Next":"AlsoNowhere"}}}`

	var first string
	for i := range 12 {
		_, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion,
			"CreateStateMachine", map[string]any{
				"name":       "asl-stable",
				"definition": twoFaults,
				"roleArn":    "arn:aws:iam::" + taggingTestAccount + ":role/StepFunctionsRole",
			})
		require.Equal(t, "InvalidDefinition", errCode, raw)
		if i == 0 {
			first = raw
			assert.Contains(t, sfnArnMember(t, raw, "Message"), `state "Alpha" has Next "Nowhere"`,
				"the alphabetically first faulty state is the one reported")
			assert.NotContains(t, raw, "Zulu", "only one fault is reported")
			continue
		}
		assert.Equal(t, first, raw, "the same definition answered a different fault on attempt %d", i+1)
	}
}

// TestSFNASL_AMapWrittenWithItemProcessorIterates is why ASLState gained the member rather than the
// validator accepting a spelling the executor ignores.
//
// aslRunMap read Iterator alone, so a Map written with ItemProcessor — the spelling the Inline Map
// page marks Required — ran zero iterations and returned an empty array. Accepting the document and
// then quietly not running it is the shape of defect this release is about, so validation and
// execution now read the same member through ASLState.mapWorkflow.
//
// The assertion is on the output of an EXPRESS workflow, because StartSyncExecution reports the
// result in its own response and needs no poll.
func TestSFNASL_AMapWrittenWithItemProcessorIterates(t *testing.T) {
	ts := sfnArnServer(t)

	// Each iteration is a Pass state with a literal Result, so the output is one element per input
	// item and depends on nothing but whether the sub-state-machine ran at all.
	const mapped = `{"StartAt":"Each","States":{"Each":{"Type":"Map","End":true,` +
		`"ItemProcessor":{"StartAt":"One","States":{` +
		`"One":{"Type":"Pass","Result":"seen","End":true}}}}}}`

	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "asl-map", "EXPRESS", mapped)

	raw := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "StartSyncExecution",
		map[string]any{"stateMachineArn": smARN, "input": `[1,2,3]`})

	assert.Equal(t, "SUCCEEDED", sfnArnMember(t, raw, "status"), raw)
	assert.Equal(t, `["seen","seen","seen"]`, sfnArnMember(t, raw, "output"),
		"an ItemProcessor Map returned an empty array before #1073")
}
