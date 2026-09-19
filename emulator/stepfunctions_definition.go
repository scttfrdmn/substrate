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
// Then the structural rules, added by #1073, which #996 deliberately left out: "{}" used to be
// accepted here although AWS refuses it, and so did a StartAt naming no state and a Next pointing out
// of its own Parallel branch. Each rule and the AWS sentence it comes from is in
// stepfunctions_asl_structure.go, along with the boundary against validating semantics — what a rule
// can assert is that the document is malformed regardless of any input, never how an execution would
// go. docs/services.md lists what is still unchecked, so a consumer does not read a 200 here as full
// ASL approval.
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

	// #996 stopped here, and `{}` created a state machine. Everything above asks
	// whether substrate can read the document back; the structural rules ask whether
	// it names a runnable state machine at all — see stepfunctions_asl_structure.go
	// for each rule's published sentence and for where the line is drawn against
	// validating semantics.
	if awsErr := sfnValidateASLStructure(&def, ""); awsErr != nil {
		return nil, awsErr
	}
	return &def, nil
}
