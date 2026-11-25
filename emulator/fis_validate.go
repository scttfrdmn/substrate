package emulator

// Experiment-template validation (#1619). CreateExperimentTemplate and UpdateExperimentTemplate
// both validate the template they would store, so an update cannot reach a state a create
// refuses. Each rule is published:
//
//   - An action's ID must be a known action, its parameters the action's own, each required one
//     present, and each duration an ISO 8601 duration inside the published range (FIS actions
//     reference). Its target key must be the action's, and the target it names must exist and
//     be of the action's resource type: "When you specify a target for an action, the target
//     must be the resource type supported by the action" (Targets page).
//   - startAfter names "the action that must be completed before the current action starts"
//     (CreateExperimentTemplateActionInput), so it must name another action, and the ordering
//     must not be circular.
//   - A target needs a resource type, ARNs or tags, and a selection mode: "You must specify at
//     least one Amazon Resource Name (ARN) or at least one resource tag. You cannot specify both
//     ARNs and tags" (CreateExperimentTemplateTargetInput), and "You can't specify both a
//     resource ID and a resource filter for the same target" (Targets page). resourceArns holds
//     at most 5.
//   - selectionMode is ALL, COUNT(n) or PERCENT(n).
//   - A stop condition's source is aws:cloudwatch:alarm, whose value "is required if the source
//     is a CloudWatch alarm", or none (create-experiment-template).
//   - roleArn is 20 to 2048 characters and the description at most 512 (the model).
//
// The messages are substrate's own.

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// fisMaxTags is the tag ceiling TagMap publishes (max 50).
const fisMaxTags = 50

// fisSelectionModePattern matches ALL, COUNT(n) and PERCENT(n).
var fisSelectionModePattern = regexp.MustCompile(`^(?:ALL|COUNT\((\d+)\)|PERCENT\((\d+)\))$`)

// fisSelection is a parsed selection mode: all, a count or a percentage.
type fisSelection struct {
	all     bool
	count   int
	percent int
}

// parseFISSelectionMode parses a selection mode, reporting false for one that is not ALL,
// COUNT(n) with n at least 1, or PERCENT(n) with n from 1 to 100.
func parseFISSelectionMode(mode string) (fisSelection, bool) {
	m := fisSelectionModePattern.FindStringSubmatch(mode)
	switch {
	case m == nil:
		return fisSelection{}, false
	case mode == "ALL":
		return fisSelection{all: true}, true
	case m[1] != "":
		n, err := strconv.Atoi(m[1])
		return fisSelection{count: n}, err == nil && n >= 1
	default:
		n, err := strconv.Atoi(m[2])
		return fisSelection{percent: n}, err == nil && n >= 1 && n <= 100
	}
}

// pick applies the selection to the identified resources, which the caller has sorted. COUNT
// takes the first n and PERCENT the first floor(len*p/100) — "If you have an odd number of
// resources and specify 50%, AWS FIS rounds down" (Targets page). AWS chooses "from the
// identified targets at random"; substrate takes the first of the sorted list instead, so the
// same state selects the same resources on every run and on replay.
func (s fisSelection) pick(ids []string) []string {
	switch {
	case s.all:
		return ids
	case s.count > 0:
		return ids[:min(s.count, len(ids))]
	default:
		return ids[:len(ids)*s.percent/100]
	}
}

// fisDurationPattern matches the ISO 8601 durations FIS parameters take, e.g. PT2M or PT1H30M.
var fisDurationPattern = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$`)

// parseFISDuration parses an ISO 8601 duration of days, hours, minutes and seconds.
func parseFISDuration(s string) (time.Duration, bool) {
	m := fisDurationPattern.FindStringSubmatch(s)
	if m == nil || s == "P" || s == "PT" {
		return 0, false
	}
	units := []time.Duration{24 * time.Hour, time.Hour, time.Minute, time.Second}
	var d time.Duration
	for i, u := range units {
		if m[i+1] == "" {
			continue
		}
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return 0, false
		}
		d += time.Duration(n) * u
	}
	return d, true
}

// fisSortedKeys returns a map's keys in order, so validation reports the same first problem on
// every run.
func fisSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// fisValidateTemplate validates a template a create or update would store.
func fisValidateTemplate(t *FISTemplate) *AWSError {
	if len(t.Description) > 512 {
		return fisValidation("The description must be at most 512 characters.")
	}
	if len(t.RoleArn) < 20 || len(t.RoleArn) > 2048 {
		return fisValidation("The roleArn must be an IAM role ARN of 20 to 2048 characters.")
	}
	if len(t.Tags) > fisMaxTags {
		return fisValidation(fmt.Sprintf("An experiment template can have at most %d tags.", fisMaxTags))
	}
	if awsErr := fisValidateStopConditions(t.StopConditions, t.AccountID, t.Region); awsErr != nil {
		return awsErr
	}
	switch t.ExperimentOptions.AccountTargeting {
	case "", "single-account", "multi-account":
	default:
		return fisValidation("accountTargeting must be single-account or multi-account.")
	}
	switch t.ExperimentOptions.EmptyTargetResolutionMode {
	case "", "fail", "skip":
	default:
		return fisValidation("emptyTargetResolutionMode must be fail or skip.")
	}
	for _, name := range fisSortedKeys(t.Targets) {
		if awsErr := fisValidateTarget(name, t.Targets[name]); awsErr != nil {
			return awsErr
		}
	}
	known := fisKnownActions()
	for _, name := range fisSortedKeys(t.Actions) {
		if awsErr := fisValidateAction(name, t.Actions[name], t, known); awsErr != nil {
			return awsErr
		}
	}
	return fisValidateStartAfterOrder(t.Actions)
}

// fisAlarmARNPattern matches a CloudWatch alarm ARN: a partition, a well-formed Region, a
// 12-digit account and a non-empty alarm name.
var fisAlarmARNPattern = regexp.MustCompile(`^arn:aws[a-z-]*:cloudwatch:([a-z]{2}(?:-[a-z]+)+-\d+):(\d{12}):alarm:(.+)$`)

// fisAlarmRef is the alarm a stop condition names.
type fisAlarmRef struct {
	region, accountID, name string
}

// parseFISAlarmARN parses a stop condition's alarm ARN, reporting false for anything else.
func parseFISAlarmARN(arn string) (fisAlarmRef, bool) {
	m := fisAlarmARNPattern.FindStringSubmatch(arn)
	if m == nil {
		return fisAlarmRef{}, false
	}
	return fisAlarmRef{region: m[1], accountID: m[2], name: m[3]}, true
}

// fisValidateStopConditions validates the stop conditions of a template in accountID and
// region. An alarm in another account or Region is refused: substrate models no cross-account
// alarm read, so such a stop condition could never fire. The refusal is substrate's reading.
func fisValidateStopConditions(conds []FISStopCondition, accountID, region string) *AWSError {
	for _, c := range conds {
		switch c.Source {
		case "none":
		case "aws:cloudwatch:alarm":
			ref, ok := parseFISAlarmARN(c.Value)
			if !ok {
				return fisValidation("The stop condition value " + strconv.Quote(c.Value) +
					" must be the ARN of a CloudWatch alarm.")
			}
			if ref.accountID != accountID || ref.region != region {
				return fisValidation("The stop condition alarm " + strconv.Quote(c.Value) +
					" must be in the experiment template's account and Region.")
			}
		default:
			return fisValidation("The stop condition source " + strconv.Quote(c.Source) +
				" is not supported; specify aws:cloudwatch:alarm or none.")
		}
	}
	return nil
}

// fisValidateTarget validates one target.
func fisValidateTarget(name string, tg FISTarget) *AWSError {
	if name == "" || len(name) > 64 {
		return fisValidation("A target name must be 1 to 64 characters.")
	}
	if _, ok := fisResourceTypeDescriptions[tg.ResourceType]; !ok {
		return fisValidation(fmt.Sprintf("Target %s: the resource type %q is not supported.", name, tg.ResourceType))
	}
	if _, ok := parseFISSelectionMode(tg.SelectionMode); !ok {
		return fisValidation(fmt.Sprintf("Target %s: the selection mode %q is not valid; specify ALL, COUNT(n) or PERCENT(n).",
			name, tg.SelectionMode))
	}
	if len(tg.ResourceArns) > 5 {
		return fisValidation(fmt.Sprintf("Target %s: resourceArns can name at most 5 resources.", name))
	}
	if len(tg.ResourceArns) > 0 && (len(tg.ResourceTags) > 0 || len(tg.Filters) > 0) {
		return fisValidation(fmt.Sprintf("Target %s: you cannot specify resourceArns together with resourceTags or filters.", name))
	}
	if len(tg.ResourceArns) == 0 && len(tg.ResourceTags) == 0 {
		return fisValidation(fmt.Sprintf("Target %s: specify at least one resource ARN or at least one resource tag.", name))
	}
	if len(tg.ResourceTags) > fisMaxTags {
		return fisValidation(fmt.Sprintf("Target %s: resourceTags can name at most %d tags.", name, fisMaxTags))
	}
	for _, f := range tg.Filters {
		if f.Path == "" || len(f.Values) == 0 {
			return fisValidation(fmt.Sprintf("Target %s: a filter needs a path and at least one value.", name))
		}
	}
	return nil
}

// fisValidateAction validates one action against the known action list and the template's
// targets.
func fisValidateAction(name string, a FISAction, t *FISTemplate, known map[string]fisActionSpec) *AWSError {
	if name == "" || len(name) > 64 {
		return fisValidation("An action name must be 1 to 64 characters.")
	}
	spec, ok := known[a.ActionID]
	if !ok {
		return fisValidation(fmt.Sprintf("Action %s: the action ID %q is not supported.", name, a.ActionID))
	}
	for _, param := range fisSortedKeys(a.Parameters) {
		ps, ok := spec.params[param]
		if !ok {
			return fisValidation(fmt.Sprintf("Action %s: %s does not take the parameter %q.", name, a.ActionID, param))
		}
		if ps.maxDuration == 0 {
			continue
		}
		d, ok := parseFISDuration(a.Parameters[param])
		if !ok || d < ps.minDuration || d > ps.maxDuration {
			return fisValidation(fmt.Sprintf("Action %s: the parameter %s must be an ISO 8601 duration from %s to %s.",
				name, param, ps.minDuration, ps.maxDuration))
		}
	}
	for _, param := range fisSortedKeys(spec.params) {
		if _, present := a.Parameters[param]; spec.params[param].required && !present {
			return fisValidation(fmt.Sprintf("Action %s: %s requires the parameter %s.", name, a.ActionID, param))
		}
	}
	for _, key := range fisSortedKeys(a.Targets) {
		if key != spec.targetKey {
			return fisValidation(fmt.Sprintf("Action %s: %s does not take the target %q.", name, a.ActionID, key))
		}
		tg, ok := t.Targets[a.Targets[key]]
		if !ok {
			return fisValidation(fmt.Sprintf("Action %s: the target %q is not defined in the template.", name, a.Targets[key]))
		}
		if tg.ResourceType != spec.resourceType {
			return fisValidation(fmt.Sprintf("Action %s: the target %s has resource type %s; %s takes %s.",
				name, a.Targets[key], tg.ResourceType, a.ActionID, spec.resourceType))
		}
	}
	if spec.targetKey != "" && a.Targets[spec.targetKey] == "" {
		return fisValidation(fmt.Sprintf("Action %s: %s requires the target %s.", name, a.ActionID, spec.targetKey))
	}
	for _, after := range a.StartAfter {
		if after == name {
			return fisValidation(fmt.Sprintf("Action %s cannot start after itself.", name))
		}
		if _, ok := t.Actions[after]; !ok {
			return fisValidation(fmt.Sprintf("Action %s: startAfter names %q, which is not an action in the template.", name, after))
		}
	}
	return nil
}

// fisValidateStartAfterOrder refuses a circular startAfter ordering, which no action could
// ever start in.
func fisValidateStartAfterOrder(actions map[string]FISAction) *AWSError {
	const (
		unvisited = iota
		visiting
		done
	)
	marks := make(map[string]int, len(actions))
	var visit func(name string) bool
	visit = func(name string) bool {
		switch marks[name] {
		case visiting:
			return false
		case done:
			return true
		}
		marks[name] = visiting
		for _, after := range actions[name].StartAfter {
			if !visit(after) {
				return false
			}
		}
		marks[name] = done
		return true
	}
	for _, name := range fisSortedKeys(actions) {
		if !visit(name) {
			return fisValidation(fmt.Sprintf("The startAfter ordering of action %s is circular.", name))
		}
	}
	return nil
}
