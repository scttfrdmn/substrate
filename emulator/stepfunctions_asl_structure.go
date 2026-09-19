package emulator

import (
	"fmt"
	"sort"
)

// This file holds the Amazon States Language structural rules that
// [sfnValidateDefinition] applies after a definition has been read back.
//
// #996 made CreateStateMachine and UpdateStateMachine refuse a definition substrate
// could not read: non-empty, valid JSON, a JSON object, and no member of the wrong
// type. #1073 is the rest — a definition that reads back perfectly and still cannot
// run, because `{}` has no states, or StartAt names a state that is not there, or a
// Next in one Parallel branch points into another. All of those created a state
// machine and answered 200.
//
// Two boundaries this file holds to deliberately.
//
// **Structure, not semantics.** A rule is here only when a *published* AWS sentence
// makes the document malformed regardless of any input. Whether a Choice rule's
// comparison can ever be true, whether a Task's Resource ARN exists, whether a
// JSONPath resolves — all of those depend on the execution, which is the
// workload-internal side of CLAUDE.md's boundary. They stay unvalidated and
// docs/services.md lists them.
//
// **Only what AWS itself publishes.** The issue quoted three "MUST" sentences from
// states-language.net, which AWS links but does not host; none of the six
// docs.aws.amazon.com/step-functions/ pages checked uses capitalised MUST at all.
// Every rule below cites an AWS page in AWS's own words, under a `(Required)` marker
// or an Important note, per #671. Where AWS publishes nothing the rule does not
// exist: a Next on a Succeed or Fail state is the clearest case — both are terminal,
// so it can never be taken, but the published sentence covers only End, so
// substrate does not refuse it.
//
// One shape this file cannot check at all: AWS rejects a *field* a state type does
// not support, and [ASLState] is one flat struct carrying every type's members at
// once, so an InputPath on a Succeed state is invisible here. The three cases the
// pages name outright — End on Choice, Succeed and Fail — are checked, because those
// are the ones a generator actually emits.

// sfnPublishedStateTypes is the set of Type values AWS publishes, which is also the
// set [ASLState.Type]'s doc comment has always enumerated.
var sfnPublishedStateTypes = map[string]bool{
	"Pass":     true,
	"Task":     true,
	"Choice":   true,
	"Wait":     true,
	"Succeed":  true,
	"Fail":     true,
	"Parallel": true,
	"Map":      true,
}

// sfnEndUnsupportedTypes is the set of state types AWS publishes as not supporting End.
//
// From amazon-states-language-state-machine-structure.html: "Only one of Next or End
// can be used in a state. Some state types, such as Choice, or terminal states, such
// as Succeed workflow state and Fail workflow state, don't support or use the End
// field." The Choice page repeats it as an Important note in stronger terms — "Choice
// states do not support the End field. In addition, they use Next only inside their
// Choices field."
//
// These three are also the types exempt from the exactly-one-of-Next-or-End rule,
// because a terminal state needs neither and a Choice transitions from inside Choices.
var sfnEndUnsupportedTypes = map[string]bool{
	"Choice":  true,
	"Succeed": true,
	"Fail":    true,
}

// sfnValidateASLStructure refuses a definition that reads back but is not a
// structurally valid state machine, and returns nil for one AWS would accept.
//
// scope names where in the document the fault is, because Branches and ItemProcessor
// recurse and "StartAt names no state" is unactionable without knowing which
// StartAt. The top level passes the empty string and the message says "the
// definition"; a nested call passes something like `state "Fan" branch 0`.
//
// Every message names the offending state or field, which is the acceptance criterion
// #1073 was most specific about: a caller fixing a generated document cannot find the
// fault from "the definition is not valid".
func sfnValidateASLStructure(def *StateMachineDefinition, scope string) *AWSError {
	where := "the definition"
	if scope != "" {
		where = scope
	}

	// Absent and empty are distinct and answer different messages. States is
	// map[string]*ASLState, so it is nil when the member is missing and non-nil when
	// the document says "States":{} — the same three-way distinction #1062 needed for
	// WAFv2's Addresses. AWS publishes "States (Required) An object containing a
	// comma-delimited set of states", and a set with no members satisfies neither the
	// requirement nor StartAt, which has to name one of them.
	if def.States == nil {
		return sfnInvalidDefinition(where + " has no States field")
	}
	if len(def.States) == 0 {
		return sfnInvalidDefinition(where + " has an empty States object")
	}

	// "StartAt (Required) A string that must exactly match (is case sensitive) the
	// name of one of the state objects." One sentence, two rules.
	if def.StartAt == "" {
		return sfnInvalidDefinition(where + " has no StartAt field")
	}
	if _, ok := def.States[def.StartAt]; !ok {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s has StartAt %q, which is not the name of a state in its States object", where, def.StartAt))
	}

	// Sorted, because a definition with two faults must report the same one on every
	// run — a map range would make the refusal a coin flip and the event log
	// unreplayable, which is the whole premise of the emulator.
	names := make([]string, 0, len(def.States))
	for name := range def.States {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		stateScope := fmt.Sprintf("state %q", name)
		if scope != "" {
			stateScope = fmt.Sprintf("%s state %q", scope, name)
		}
		if err := sfnValidateASLState(def.States[name], stateScope, def.States); err != nil {
			return err
		}
	}
	return nil
}

// sfnValidateASLState refuses one state of a States object.
//
// siblings is the States map this state's own transitions resolve against, and it is
// deliberately the enclosing map rather than the top-level one. AWS publishes the
// containment rule at both recursion points: "Each branch must be self-contained. A
// state in one branch of a Parallel state must not have a Next field that targets a
// field outside of that branch, nor can any other state outside the branch transition
// into that branch." (Parallel), and "States within the ItemProcessor field can only
// transition to each other. No state outside the ItemProcessor field can transition
// to a state within it." (Inline Map). So a nested Next naming a top-level state is a
// refusal, not a pass.
func sfnValidateASLState(state *ASLState, scope string, siblings map[string]*ASLState) *AWSError {
	if state == nil {
		return sfnInvalidDefinition(scope + " is null")
	}
	if state.Type == "" {
		return sfnInvalidDefinition(scope + " has no Type field")
	}
	if !sfnPublishedStateTypes[state.Type] {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s has Type %q, which is not one of Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map",
			scope, state.Type))
	}

	if state.End && sfnEndUnsupportedTypes[state.Type] {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s is a %s state, which does not support the End field", scope, state.Type))
	}

	// The transition rule, and the reason Choice is not simply exempt from it: AWS
	// says a Choice state uses "Next only inside their Choices field", so a Next on
	// the state itself is wrong rather than redundant. Succeed and Fail are terminal
	// and AWS publishes nothing about a Next on them, so one is left alone — see the
	// file comment.
	switch state.Type {
	case "Choice":
		if state.Next != "" {
			return sfnInvalidDefinition(fmt.Sprintf(
				"%s is a Choice state, which uses Next only inside its Choices field", scope))
		}
	case "Succeed", "Fail":
		// Terminal. Neither Next nor End is required, and neither is refused.
	default:
		if state.Next != "" && state.End {
			return sfnInvalidDefinition(fmt.Sprintf(
				"%s has both Next and End; only one of them can be used in a state", scope))
		}
		if state.Next == "" && !state.End {
			return sfnInvalidDefinition(fmt.Sprintf(
				"%s has neither Next nor End; a %s state needs one of them", scope, state.Type))
		}
		// Only a state that transitions has a target to resolve. Reaching here with
		// End set and no Next is the terminal case the two checks above have already
		// accepted.
		if state.Next != "" {
			if err := sfnResolveTransition(state.Next, scope, "Next", siblings); err != nil {
				return err
			}
		}
	}

	// Catch's Next is a transition like any other and resolves in the same scope. A
	// Catcher with no Next has nowhere to send the error, so an empty one is refused
	// rather than ignored.
	for i := range state.Catch {
		field := fmt.Sprintf("Catch[%d].Next", i)
		if state.Catch[i].Next == "" {
			return sfnInvalidDefinition(fmt.Sprintf("%s has a %s with no Next", scope, field))
		}
		if err := sfnResolveTransition(state.Catch[i].Next, scope, field, siblings); err != nil {
			return err
		}
	}

	switch state.Type {
	case "Choice":
		return sfnValidateChoiceState(state, scope, siblings)
	case "Parallel":
		return sfnValidateParallelState(state, scope)
	case "Map":
		return sfnValidateMapState(state, scope)
	}
	return nil
}

// sfnValidateChoiceState applies the Choice page's two required-field rules and the
// one rule that makes a Next a fault rather than a reference.
//
// "Choices (Required) An array of Choice Rules that determines which state the state
// machine transitions to next. You must define at least one rule in the Choice
// state." — so an absent or empty Choices is refused, and the message says which.
//
// Default is a state reference the issue's own list omitted: "Default (Optional,
// Recommended) The name of the state to transition to if no Choice Rule evaluates to
// true." Optional, so an absent one is fine; present, it must name a state, because
// [StepFunctionsPlugin.aslRunChoice] transitions to it.
func sfnValidateChoiceState(state *ASLState, scope string, siblings map[string]*ASLState) *AWSError {
	if len(state.Choices) == 0 {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s is a Choice state with no Choices; at least one Choice Rule is required", scope))
	}
	for i := range state.Choices {
		if err := sfnValidateChoiceRule(&state.Choices[i], scope, fmt.Sprintf("Choices[%d]", i),
			true, siblings); err != nil {
			return err
		}
	}
	if state.Default != "" {
		return sfnResolveTransition(state.Default, scope, "Default", siblings)
	}
	return nil
}

// sfnValidateChoiceRule refuses a Choice Rule whose Next is missing, unresolvable, or
// in a place AWS does not allow it.
//
// topLevel says whether this rule is a member of the state's own Choices array or is
// nested inside an And, Or or Not, and it is the whole point of the function. AWS
// publishes the distinction twice on the Choice page:
//
//	"The values of the And and Or operators must be non-empty arrays of Choice Rules
//	that must not themselves contain Next fields. Likewise, the value of a Not
//	operator must be a single Choice Rule that must not contain Next fields."
//
//	"You can create complex, nested Choice Rules using And, Not, and Or. However, the
//	Next field can appear only in a top-level Choice Rule."
//
// So a nested Next is *misplaced* — refused for being where it is, not resolved
// against States and accepted when the target happens to exist. [ChoiceRule] models
// And, Or and Not with a Next on every nested rule, so substrate could express the
// document AWS refuses and did accept it.
func sfnValidateChoiceRule(
	rule *ChoiceRule,
	scope, field string,
	topLevel bool,
	siblings map[string]*ASLState,
) *AWSError {
	if topLevel {
		if rule.Next == "" {
			return sfnInvalidDefinition(fmt.Sprintf("%s has a %s with no Next", scope, field))
		}
		if err := sfnResolveTransition(rule.Next, scope, field+".Next", siblings); err != nil {
			return err
		}
	} else if rule.Next != "" {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s has a Next in %s; the Next field can appear only in a top-level Choice Rule",
			scope, field))
	}

	// The non-empty requirement is on the operators themselves — "must be non-empty
	// arrays of Choice Rules" — so And:[] and Or:[] are refused, while an absent
	// operator is simply not a boolean rule.
	for _, op := range []struct {
		name  string
		rules []ChoiceRule
	}{
		{"And", rule.And},
		{"Or", rule.Or},
	} {
		if rule.hasOperator(op.name) && len(op.rules) == 0 {
			return sfnInvalidDefinition(fmt.Sprintf(
				"%s has an empty %s in %s; the value of %s must be a non-empty array of Choice Rules",
				scope, op.name, field, op.name))
		}
		for i := range op.rules {
			nested := fmt.Sprintf("%s.%s[%d]", field, op.name, i)
			if err := sfnValidateChoiceRule(&op.rules[i], scope, nested, false, siblings); err != nil {
				return err
			}
		}
	}
	if rule.Not != nil {
		return sfnValidateChoiceRule(rule.Not, scope, field+".Not", false, siblings)
	}
	return nil
}

// hasOperator reports whether the rule carries the named boolean operator at all, as
// opposed to carrying it with an empty array.
//
// encoding/json leaves a slice nil for an absent member and gives a non-nil empty
// slice for `[]`, so the two are distinguishable and AWS treats them differently: an
// absent And is not a boolean rule, while `"And": []` is one AWS refuses. This is the
// same absent-versus-empty distinction [sfnValidateASLStructure] draws for States.
func (r *ChoiceRule) hasOperator(name string) bool {
	switch name {
	case "And":
		return r.And != nil
	case "Or":
		return r.Or != nil
	}
	return false
}

// sfnValidateParallelState applies the Parallel page's Branches rules.
//
// "Branches (Required) An array of objects that specify state machines to execute in
// parallel. Each such state machine object must have fields named States and StartAt,
// whose meanings are exactly like those in the top level of a state machine." The
// last clause is the citation for recursing with the same rules rather than a weaker
// nested set.
//
// Branches is a value slice where [ASLState.Iterator] is a pointer, so this arm takes
// the address of each element and the Map arm does not.
func sfnValidateParallelState(state *ASLState, scope string) *AWSError {
	if len(state.Branches) == 0 {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s is a Parallel state with no Branches; Branches is required", scope))
	}
	for i := range state.Branches {
		if err := sfnValidateASLStructure(&state.Branches[i],
			fmt.Sprintf("%s branch %d", scope, i)); err != nil {
			return err
		}
	}
	return nil
}

// sfnValidateMapState requires a Map state to carry exactly one sub-state-machine,
// under either of the two names AWS accepts.
//
// ItemProcessor is the current spelling and the Inline Map page marks it
// "(Required)"; Iterator appears on the same page under a heading titled "Deprecated
// fields". But AWS accepts both, and says so: "The ItemProcessor field replaces the
// now deprecated Iterator field. Although you can continue to include Map states that
// use the Iterator field, we highly recommend that you replace this field with
// ItemProcessor."
//
// For an emulator the next sentence on that page decides it outright: "Step Functions
// Local doesn't currently support the ItemProcessor field. We recommend that you use
// the Iterator field with Step Functions Local." So refusing a Map for a missing
// Iterator would reject the spelling AWS marks Required, and refusing one for a
// missing ItemProcessor would reject the spelling AWS recommends to exactly the class
// of tool substrate is. Either satisfies the requirement; both together is refused,
// because the two name one thing and nothing publishes a precedence between them.
//
// [ASLState.ItemProcessor] was added by #1073 for this rule — it is the one member the
// issue's rules needed that the type did not already model, so the issue's claim that
// no schema widening is needed is not quite right. [ASLState.mapWorkflow] is what
// keeps the executor reading the same member this refuses the absence of, rather than
// validating one spelling and running the other.
func sfnValidateMapState(state *ASLState, scope string) *AWSError {
	if state.ItemProcessor != nil && state.Iterator != nil {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s is a Map state with both ItemProcessor and Iterator; they name one field and only one can be used",
			scope))
	}
	workflow, field := state.mapWorkflow()
	if workflow == nil {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s is a Map state with no ItemProcessor; ItemProcessor is required "+
				"(its deprecated spelling Iterator is also accepted)", scope))
	}
	return sfnValidateASLStructure(workflow, scope+" "+field)
}

// mapWorkflow returns a Map state's sub-state-machine and the name of the member it
// came from, or nil and "" when the state carries neither.
//
// It exists so that [sfnValidateMapState] and [StepFunctionsPlugin.aslRunMap] cannot
// disagree about which spelling counts. Before #1073 the executor read Iterator alone,
// so a Map written with ItemProcessor — the spelling AWS marks Required — ran zero
// iterations and returned an empty array, which is the kind of silently-wrong answer a
// validation guard is supposed to make impossible rather than merely legal.
//
// ItemProcessor wins when both are present; nothing publishes a precedence, but
// [sfnValidateMapState] refuses that document before either writer stores it, so the
// order here is reached only by the executor replaying a definition stored before
// #1073.
func (s *ASLState) mapWorkflow() (*StateMachineDefinition, string) {
	if s.ItemProcessor != nil {
		return s.ItemProcessor, "ItemProcessor"
	}
	if s.Iterator != nil {
		return s.Iterator, "Iterator"
	}
	return nil, ""
}

// sfnResolveTransition refuses a transition naming a state that is not in the map it
// resolves against.
//
// The map is the enclosing States object rather than the top-level one — see
// [sfnValidateASLState] for the two published containment sentences — so this is also
// what refuses a Parallel branch that transitions out of itself.
func sfnResolveTransition(target, scope, field string, siblings map[string]*ASLState) *AWSError {
	if _, ok := siblings[target]; !ok {
		return sfnInvalidDefinition(fmt.Sprintf(
			"%s has %s %q, which is not the name of a state in the same States object", scope, field, target))
	}
	return nil
}
