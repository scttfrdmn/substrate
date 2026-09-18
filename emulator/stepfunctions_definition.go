package emulator

import (
	"bytes"
	"encoding/json"
)

// sfnValidateDefinition refuses a state-machine definition substrate would not be able to read back.
//
// Neither createStateMachine nor updateStateMachine checked the definition at all before #996: both
// stored the string as given. So substrate would accept a definition, answer 200, and then have no way
// to execute it — and the two execution handlers reported that as a caller error, one of them under a
// code its own page does not publish. Validating on the way in is what makes those paths unreachable,
// which is the resolution API_StartSyncExecution's own Note points at: "Error codes are reserved for
// errors that prevent your execution from running, such as permissions errors, limit errors, or issues
// with your state machine code and configuration." AWS has no code for a stored definition it cannot
// read because AWS would never have stored one.
//
// What is checked, and the boundary — this is deliberately narrower than AWS:
//
//   - The definition is not empty. API_CreateStateMachine publishes definition as Required: Yes with a
//     minimum length of 1.
//   - It parses as JSON.
//   - It describes a JSON object. A definition that parses as a string, a number or an array is the
//     shape the CloudFormation deployer used to produce by marshaling a DefinitionString that was
//     already a string, so this check is the one that catches a real in-tree producer rather than a
//     hypothetical caller.
//
// What is **not** checked is Amazon States Language conformance. "{}" is accepted here although AWS
// refuses it, because the ASL specification's requirements — a state machine MUST have a string field
// named StartAt and an object field named States, and StartAt MUST name one of them — are a separate
// body of rules with their own citation trail, and validating them is a larger job than this issue.
// docs/services.md states the boundary so a consumer does not read a 200 here as ASL approval.
//
// The parsed definition is returned because every caller needs it, and parsing it twice would let the
// two copies disagree.
func sfnValidateDefinition(definition string) (*StateMachineDefinition, *AWSError) {
	if definition == "" {
		return nil, sfnInvalidDefinition("the definition is empty")
	}

	// The three refusals are separated so the message names the actual fault. Unmarshaling straight
	// into the struct would collapse them: a definition that parses as a string and one that is not JSON
	// at all both come back as the same "cannot unmarshal" error, and the first is the case the
	// CloudFormation deployer produced.
	trimmed := bytes.TrimSpace([]byte(definition))
	if !json.Valid(trimmed) {
		return nil, sfnInvalidDefinition("the definition is not valid JSON")
	}
	if trimmed[0] != '{' {
		return nil, sfnInvalidDefinition("the definition is not a JSON object")
	}

	var def StateMachineDefinition
	if err := json.Unmarshal(trimmed, &def); err != nil {
		return nil, sfnInvalidDefinition("a member of the definition has the wrong type")
	}
	return &def, nil
}
