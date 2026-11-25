package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// maxEBEventRingBuffer is the maximum number of PutEvents events retained in state.
const maxEBEventRingBuffer = 100

// EventBridgePlugin emulates the Amazon EventBridge JSON-protocol API: rules and their targets on
// the default and custom event buses, the buses themselves with their resource policies, PutEvents,
// and tagging of rules and buses.
type EventBridgePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController

	// ruleMu serializes every read-modify-write of a rule or bus record: PutRule's update, which
	// keeps the stored tags; the target writes; Enable/DisableRule; the tag writes; the bus writes;
	// and the deletes. Two concurrent writers cannot each read a record before the other stores it
	// and lose one write, resurrect a deleted record, or create a rule on a bus being deleted.
	ruleMu sync.Mutex
}

// Name returns the service name "eventbridge".
func (p *EventBridgePlugin) Name() string { return "eventbridge" }

// Initialize sets up the EventBridgePlugin with the provided configuration.
func (p *EventBridgePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for EventBridgePlugin.
func (p *EventBridgePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EventBridge JSON-protocol request to the
// appropriate handler.
func (p *EventBridgePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "PutRule":
		return p.putRule(ctx, req)
	case "DeleteRule":
		return p.deleteRule(ctx, req)
	case "ListRules":
		return p.listRules(ctx, req)
	case "DescribeRule":
		return p.describeRule(ctx, req)
	case "EnableRule":
		return p.setRuleState(ctx, req, "ENABLED")
	case "DisableRule":
		return p.setRuleState(ctx, req, "DISABLED")
	case "PutTargets":
		return p.putTargets(ctx, req)
	case "RemoveTargets":
		return p.removeTargets(ctx, req)
	case "ListTargetsByRule":
		return p.listTargetsByRule(ctx, req)
	case "ListRuleNamesByTarget":
		return p.listRuleNamesByTarget(ctx, req)
	case "PutEvents":
		return p.putEvents(ctx, req)
	case "CreateEventBus":
		return p.createEventBus(ctx, req)
	case "DescribeEventBus":
		return p.describeEventBus(ctx, req)
	case "UpdateEventBus":
		return p.updateEventBus(ctx, req)
	case "DeleteEventBus":
		return p.deleteEventBus(ctx, req)
	case "ListEventBuses":
		return p.listEventBuses(ctx, req)
	case "PutPermission":
		return p.putPermission(ctx, req)
	case "RemovePermission":
		return p.removePermission(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- Rule addressing ---------------------------------------------------------

// ebRuleNamePattern is the Name pattern and length every rule operation publishes:
// `[\.\-_A-Za-z0-9]+`, 1 to 64 characters. It admits no "/", which is what keeps a default-bus
// rule key and a custom-bus rule key apart; see [ebRuleRef.key].
var ebRuleNamePattern = regexp.MustCompile(`^[.\-_A-Za-z0-9]{1,64}$`)

// ebRuleRef names one rule: the account, Region and bus it lives on, and its name.
type ebRuleRef struct {
	accountID, region, bus, name string
}

// ebRequestRule addresses the rule a request names on the bus its EventBusName names, in the
// caller's account and Region, refusing a name outside the published pattern. field is the wire
// member that carries the name ("Name" or "Rule").
func ebRequestRule(ctx *RequestContext, busField, name, field string) (ebRuleRef, *AWSError) {
	if name == "" {
		return ebRuleRef{}, ebValidationError(field + " is required")
	}
	if !ebRuleNamePattern.MatchString(name) {
		return ebRuleRef{}, ebValidationError(fmt.Sprintf(
			"%s %q must be 1 to 64 characters from [.-_A-Za-z0-9]", field, name))
	}
	bus, busErr := ebRequestBus(ctx, busField)
	if busErr != nil {
		return ebRuleRef{}, busErr
	}
	return ebRuleRef{accountID: ctx.AccountID, region: ctx.Region, bus: bus, name: name}, nil
}

// requireBus refuses a bus that does not exist in the caller's account and Region.
func (p *EventBridgePlugin) requireBus(goCtx context.Context, ctx *RequestContext, bus string) error {
	_, found, err := p.loadBus(goCtx, ctx.AccountID, ctx.Region, bus)
	if err != nil {
		return err
	}
	if !found {
		return ebBusNotFound(bus)
	}
	return nil
}

// ruleNames returns the names in the rule-name index of bus, refusing a bus that does not exist.
//
// A name outside [ebRuleNamePattern] is left out. PutRule refuses one, so only a default-bus rule
// stored by a release that did not check the pattern can carry it, and one containing "/" is stored
// at a key that reads as a custom-bus rule's ([ebRuleRef.key]). No rule operation can address such a
// rule, so it is not listed either; the logged warning names it.
func (p *EventBridgePlugin) ruleNames(goCtx context.Context, ctx *RequestContext, bus string) ([]string, error) {
	if err := p.requireBus(goCtx, ctx, bus); err != nil {
		return nil, err
	}
	names, err := loadStringIndex(goCtx, p.state, eventbridgeNamespace, ebRuleNamesKey(ctx.AccountID, ctx.Region, bus))
	if err != nil {
		return nil, fmt.Errorf("eventbridge load rule names of %s: %w", bus, err)
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if !ebRuleNamePattern.MatchString(n) {
			p.logger.Warn("eventbridge: skipping a stored rule whose name no rule operation accepts",
				"bus", bus, "rule", n)
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// key is the rule record's state key.
//
// A default-bus rule is stored at ebrule:{account}/{region}/{name}, the key every rule had before
// rules were keyed per bus, so a record written by an earlier release is read, listed, updated and
// deleted as the default-bus rule it always was, with no migration step. A custom-bus rule inserts
// the bus: ebrule:{account}/{region}/{bus}/{name}. A rule name cannot contain "/"
// ([ebRuleNamePattern]), so neither form can be read as the other.
func (r ebRuleRef) key() string {
	return ebRuleKeyPrefix + r.scope() + r.name
}

// targetsKey is the state key of the rule's target list, scoped the way [ebRuleRef.key] is.
func (r ebRuleRef) targetsKey() string {
	return "ebtargets:" + r.scope() + r.name
}

// namesKey is the state key of the rule-name index of the rule's bus.
func (r ebRuleRef) namesKey() string {
	return ebRuleNamesKey(r.accountID, r.region, r.bus)
}

// arn is the rule's ARN.
func (r ebRuleRef) arn() string {
	return ebRuleARN(r.region, r.accountID, r.bus, r.name)
}

// scope is the account, Region and (for a custom bus) bus segments of the rule's keys, ending in "/".
func (r ebRuleRef) scope() string {
	s := r.accountID + "/" + r.region + "/"
	if r.bus != ebDefaultBus {
		s += r.bus + "/"
	}
	return s
}

// ebRuleNamesKey is the state key of the rule-name index of bus. The default bus keeps the
// unscoped key every index had before rules were keyed per bus.
func ebRuleNamesKey(accountID, region, bus string) string {
	k := "ebrule_names:" + accountID + "/" + region
	if bus != "" && bus != ebDefaultBus {
		k += "/" + bus
	}
	return k
}

// ebEventsKey is the state key of the PutEvents ring buffer.
func ebEventsKey(accountID, region string) string {
	return "ebevents:" + accountID + "/" + region
}

// ebMaxRulesPerBus is "Number of rules: Maximum number of rules an account can have per event bus"
// on the EventBridge quotas page: 300, and 100 in the two Regions it names.
func ebMaxRulesPerBus(region string) int {
	switch region {
	case "af-south-1", "eu-south-1":
		return 100
	default:
		return 300
	}
}

// --- Rule operations --------------------------------------------------------

func (p *EventBridgePlugin) putRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name               string  `json:"Name"`
		EventPattern       string  `json:"EventPattern"`
		ScheduleExpression string  `json:"ScheduleExpression"`
		State              string  `json:"State"`
		Description        string  `json:"Description"`
		EventBusName       string  `json:"EventBusName"`
		RoleArn            string  `json:"RoleArn"`
		Tags               []EBTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Name, "Name")
	if refErr != nil {
		return nil, refErr
	}
	if tagErr := ebValidateTags(body.Tags); tagErr != nil {
		return nil, tagErr
	}

	state := body.State
	if state == "" {
		state = "ENABLED"
	}

	goCtx := context.Background()
	rule := EBRule{
		Name:               ref.name,
		ARN:                ref.arn(),
		EventPattern:       body.EventPattern,
		ScheduleExpression: body.ScheduleExpression,
		State:              state,
		Description:        body.Description,
		EventBusName:       ref.bus,
		RoleARN:            body.RoleArn,
		CreatedBy:          ctx.AccountID,
	}

	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	if err := p.requireBus(goCtx, ctx, ref.bus); err != nil {
		return nil, err
	}
	existing, err := p.loadRule(goCtx, ref)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// "If you are updating an existing rule, any tags you specify in the PutRule operation are
		// ignored", so an update keeps the tags, their history and the creating account. Every other
		// member is replaced: "If you omit arguments in PutRule, the old values for those arguments are
		// not kept."
		rule.Tags = existing.Tags
		rule.EverTagged = existing.EverTagged
		if existing.CreatedBy != "" {
			rule.CreatedBy = existing.CreatedBy
		}
	} else {
		names, idxErr := loadStringIndex(goCtx, p.state, eventbridgeNamespace, ref.namesKey())
		if idxErr != nil {
			return nil, fmt.Errorf("eventbridge putRule index: %w", idxErr)
		}
		if limit := ebMaxRulesPerBus(ref.region); len(names) >= limit {
			return nil, &AWSError{
				Code:       "LimitExceededException",
				Message:    fmt.Sprintf("event bus %s already holds %d rules, the most a bus can have", ref.bus, limit),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		if len(body.Tags) > 0 {
			merged := ebMergeTags(nil, body.Tags, nil)
			if quotaErr := ebCheckTagQuota(ref.name, len(merged)); quotaErr != nil {
				return nil, quotaErr
			}
			rule.Tags = merged
		}
	}
	if err := p.saveRule(goCtx, ref, &rule); err != nil {
		return nil, err
	}
	if err := updateStringIndex(goCtx, p.state, eventbridgeNamespace, ref.namesKey(), ref.name); err != nil {
		return nil, fmt.Errorf("eventbridge putRule index: %w", err)
	}

	type response struct {
		RuleArn string `json:"RuleArn"`
	}
	return ebJSONResponse(http.StatusOK, response{RuleArn: rule.ARN})
}

func (p *EventBridgePlugin) deleteRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name         string `json:"Name"`
		EventBusName string `json:"EventBusName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Name, "Name")
	if refErr != nil {
		return nil, refErr
	}

	goCtx := context.Background()
	// Held so a TagResource that loaded the rule cannot store it back after this deletes it.
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	existing, err := p.state.Get(goCtx, eventbridgeNamespace, ref.key())
	if err != nil {
		return nil, fmt.Errorf("eventbridge deleteRule state.Get: %w", err)
	}
	if existing == nil {
		return nil, ebRuleNotFound(ref)
	}

	targets, err := p.loadTargets(goCtx, ref)
	if err != nil {
		return nil, err
	}
	if len(targets) > 0 {
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "Rule " + ref.name + " still has targets. Remove targets before deleting the rule.",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	if err := p.state.Delete(goCtx, eventbridgeNamespace, ref.key()); err != nil {
		return nil, fmt.Errorf("eventbridge deleteRule state.Delete: %w", err)
	}
	if err := p.state.Delete(goCtx, eventbridgeNamespace, ref.targetsKey()); err != nil {
		return nil, fmt.Errorf("eventbridge deleteRule delete targets: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, eventbridgeNamespace, ref.namesKey(), ref.name); err != nil {
		return nil, fmt.Errorf("eventbridge deleteRule index: %w", err)
	}

	return ebJSONResponse(http.StatusOK, struct{}{})
}

func (p *EventBridgePlugin) listRules(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		NamePrefix   string `json:"NamePrefix"`
		EventBusName string `json:"EventBusName"`
		NextToken    string `json:"NextToken"`
		Limit        int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ebInvalidBody()
		}
	}

	// A token substrate did not issue is refused rather than answered with page one; ListRules
	// publishes InvalidToken for it in the NextToken member's own description (#1086). Refused
	// before the rule-name index is read, per #887: the refusal depends on nothing the store holds,
	// and reading first would report a store failure as a 500 for a request already refusable.
	offset, ok := decodeOffsetPaginationToken(body.NextToken)
	if !ok {
		return nil, ebInvalidToken()
	}

	goCtx := context.Background()
	bus, busErr := ebRequestBus(ctx, body.EventBusName)
	if busErr != nil {
		return nil, busErr
	}
	allNames, err := p.ruleNames(goCtx, ctx, bus)
	if err != nil {
		return nil, err
	}

	names := allNames
	if body.NamePrefix != "" {
		filtered := make([]string, 0, len(allNames))
		for _, n := range allNames {
			if strings.HasPrefix(n, body.NamePrefix) {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	limit := body.Limit
	if limit <= 0 {
		limit = 100
	}
	// The offset indexes into the rule-name index, which is appended to in one order and read back
	// in it, so a position is stable across the calls of one walk — and the prefix filter above
	// preserves that order rather than rebuilding it.
	page, nextToken := pageByOffsetToken(names, offset, limit)

	rules := make([]ebRuleWire, 0, len(page))
	for _, name := range page {
		rule, loadErr := p.loadRule(goCtx, ebRuleRef{accountID: ctx.AccountID, region: ctx.Region, bus: bus, name: name})
		if loadErr != nil {
			return nil, loadErr
		}
		if rule == nil {
			continue
		}
		rules = append(rules, ebRuleToWire(*rule))
	}

	type response struct {
		Rules     []ebRuleWire `json:"Rules"`
		NextToken string       `json:"NextToken,omitempty"`
	}
	return ebJSONResponse(http.StatusOK, response{Rules: rules, NextToken: nextToken})
}

func (p *EventBridgePlugin) describeRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name         string `json:"Name"`
		EventBusName string `json:"EventBusName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Name, "Name")
	if refErr != nil {
		return nil, refErr
	}

	rule, err := p.loadRule(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ebRuleNotFound(ref)
	}
	return ebJSONResponse(http.StatusOK, ebDescribeRuleWire{ebRuleWire: ebRuleToWire(*rule), CreatedBy: rule.CreatedBy})
}

// setRuleState handles EnableRule and DisableRule, which set a rule's State and answer an empty
// body. "If the rule does not exist, the operation fails" with ResourceNotFoundException.
func (p *EventBridgePlugin) setRuleState(ctx *RequestContext, req *AWSRequest, state string) (*AWSResponse, error) {
	var body struct {
		Name         string `json:"Name"`
		EventBusName string `json:"EventBusName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Name, "Name")
	if refErr != nil {
		return nil, refErr
	}

	goCtx := context.Background()
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	rule, err := p.loadRule(goCtx, ref)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ebRuleNotFound(ref)
	}
	rule.State = state
	if err := p.saveRule(goCtx, ref, rule); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// --- Target operations ------------------------------------------------------

func (p *EventBridgePlugin) putTargets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Rule         string     `json:"Rule"`
		EventBusName string     `json:"EventBusName"`
		Targets      []EBTarget `json:"Targets"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Rule, "Rule")
	if refErr != nil {
		return nil, refErr
	}

	goCtx := context.Background()

	// Held across the existence check and the write, so a concurrent DeleteRule cannot remove the
	// rule between them and leave this write behind as an orphan target list.
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	existing, err := p.loadRuleTargets(goCtx, ref)
	if err != nil {
		return nil, err
	}

	// Merge targets: overwrite by ID.
	idMap := make(map[string]int, len(existing))
	for i, t := range existing {
		idMap[t.Id] = i
	}
	for _, t := range body.Targets {
		if idx, ok := idMap[t.Id]; ok {
			existing[idx] = t
		} else {
			existing = append(existing, t)
			idMap[t.Id] = len(existing) - 1
		}
	}

	if err := p.saveTargets(goCtx, ref, existing); err != nil {
		return nil, err
	}

	type response struct {
		FailedEntryCount int           `json:"FailedEntryCount"`
		FailedEntries    []interface{} `json:"FailedEntries"`
	}
	return ebJSONResponse(http.StatusOK, response{FailedEntryCount: 0, FailedEntries: []interface{}{}})
}

func (p *EventBridgePlugin) removeTargets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Rule         string   `json:"Rule"`
		EventBusName string   `json:"EventBusName"`
		IDs          []string `json:"Ids"` //nolint:revive
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Rule, "Rule")
	if refErr != nil {
		return nil, refErr
	}

	goCtx := context.Background()
	// RemoveTargets publishes ResourceNotFoundException for an absent rule. Locked for the reason
	// putTargets is.
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	existing, err := p.loadRuleTargets(goCtx, ref)
	if err != nil {
		return nil, err
	}

	removeIDs := make(map[string]bool, len(body.IDs))
	for _, id := range body.IDs {
		removeIDs[id] = true
	}

	remaining := make([]EBTarget, 0, len(existing))
	for _, t := range existing {
		if !removeIDs[t.Id] {
			remaining = append(remaining, t)
		}
	}

	if err := p.saveTargets(goCtx, ref, remaining); err != nil {
		return nil, err
	}

	type response struct {
		FailedEntryCount int           `json:"FailedEntryCount"`
		FailedEntries    []interface{} `json:"FailedEntries"`
	}
	return ebJSONResponse(http.StatusOK, response{FailedEntryCount: 0, FailedEntries: []interface{}{}})
}

// loadRuleTargets refuses an absent rule and returns its stored targets.
func (p *EventBridgePlugin) loadRuleTargets(goCtx context.Context, ref ebRuleRef) ([]EBTarget, error) {
	ruleData, err := p.state.Get(goCtx, eventbridgeNamespace, ref.key())
	if err != nil {
		return nil, fmt.Errorf("eventbridge load rule %s: %w", ref.name, err)
	}
	if ruleData == nil {
		return nil, ebRuleNotFound(ref)
	}
	return p.loadTargets(goCtx, ref)
}

// loadTargets returns the target list stored for a rule, nil when there is none.
func (p *EventBridgePlugin) loadTargets(goCtx context.Context, ref ebRuleRef) ([]EBTarget, error) {
	data, err := p.state.Get(goCtx, eventbridgeNamespace, ref.targetsKey())
	if err != nil {
		return nil, fmt.Errorf("eventbridge load targets of %s: %w", ref.name, err)
	}
	var targets []EBTarget
	if data != nil {
		if err := json.Unmarshal(data, &targets); err != nil {
			return nil, fmt.Errorf("eventbridge decode targets of %s: %w", ref.name, err)
		}
	}
	return targets, nil
}

// saveTargets stores a rule's target list.
func (p *EventBridgePlugin) saveTargets(goCtx context.Context, ref ebRuleRef, targets []EBTarget) error {
	data, err := json.Marshal(targets)
	if err != nil {
		return fmt.Errorf("eventbridge encode targets of %s: %w", ref.name, err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, ref.targetsKey(), data); err != nil {
		return fmt.Errorf("eventbridge store targets of %s: %w", ref.name, err)
	}
	return nil
}

func (p *EventBridgePlugin) listTargetsByRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Rule         string `json:"Rule"`
		EventBusName string `json:"EventBusName"`
		NextToken    string `json:"NextToken"`
		Limit        int    `json:"Limit"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	ref, refErr := ebRequestRule(ctx, body.EventBusName, body.Rule, "Rule")
	if refErr != nil {
		return nil, refErr
	}

	targets, err := p.loadRuleTargets(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	if targets == nil {
		targets = []EBTarget{}
	}

	type response struct {
		Targets   []EBTarget `json:"Targets"`
		NextToken string     `json:"NextToken,omitempty"`
	}
	return ebJSONResponse(http.StatusOK, response{Targets: targets})
}

// listRuleNamesByTarget handles ListRuleNamesByTarget: the names of the rules on one bus that have
// a target whose Arn is TargetArn, in name order. It pages through [page], whose tokens resume
// after the last name returned and are refused for another TargetArn or bus.
func (p *EventBridgePlugin) listRuleNamesByTarget(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TargetArn    string `json:"TargetArn"`
		EventBusName string `json:"EventBusName"`
		NextToken    string `json:"NextToken"`
		Limit        int    `json:"Limit"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	if body.TargetArn == "" {
		return nil, ebValidationError("TargetArn is required")
	}
	if body.Limit < 0 || body.Limit > 100 {
		return nil, ebValidationError("Limit must be between 1 and 100")
	}
	bus, busErr := ebRequestBus(ctx, body.EventBusName)
	if busErr != nil {
		return nil, busErr
	}
	pr := pageRequest{
		Op:     "ListRuleNamesByTarget",
		Filter: map[string][]string{"TargetArn": {body.TargetArn}, "EventBusName": {bus}},
		Token:  body.NextToken,
		Size:   body.Limit,
	}
	if pr.validate() != nil {
		return nil, ebInvalidToken()
	}

	goCtx := context.Background()
	names, err := p.ruleNames(goCtx, ctx, bus)
	if err != nil {
		return nil, err
	}
	var matched []string
	for _, name := range names {
		targets, err := p.loadTargets(goCtx, ebRuleRef{accountID: ctx.AccountID, region: ctx.Region, bus: bus, name: name})
		if err != nil {
			return nil, err
		}
		for _, t := range targets {
			if t.ARN == body.TargetArn {
				matched = append(matched, name)
				break
			}
		}
	}
	res, err := page(matched, func(n string) cursorKey { return cursorKey{Sort: n} }, pr)
	if err != nil {
		return nil, ebInvalidToken()
	}
	out := struct {
		RuleNames []string `json:"RuleNames"`
		NextToken string   `json:"NextToken,omitempty"`
	}{RuleNames: res.Items, NextToken: res.Next}
	if out.RuleNames == nil {
		out.RuleNames = []string{}
	}
	return ebJSONResponse(http.StatusOK, out)
}

// --- Event operations -------------------------------------------------------

func (p *EventBridgePlugin) putEvents(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Entries []struct {
			Source       string `json:"Source"`
			DetailType   string `json:"DetailType"`
			Detail       string `json:"Detail"`
			EventBusName string `json:"EventBusName"`
		} `json:"Entries"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}

	goCtx := context.Background()
	eventsKey := ebEventsKey(ctx.AccountID, ctx.Region)

	var existing []EBEvent
	existingData, _ := p.state.Get(goCtx, eventbridgeNamespace, eventsKey)
	if existingData != nil {
		_ = json.Unmarshal(existingData, &existing)
	}

	now := p.tc.Now().UnixMilli()
	type resultEntry struct {
		EventID      string `json:"EventId,omitempty"`
		ErrorCode    string `json:"ErrorCode,omitempty"`
		ErrorMessage string `json:"ErrorMessage,omitempty"`
	}
	results := make([]resultEntry, 0, len(body.Entries))

	for _, entry := range body.Entries {
		if entry.Source == "" || entry.DetailType == "" || entry.Detail == "" {
			results = append(results, resultEntry{
				ErrorCode:    "InvalidEventPatternException",
				ErrorMessage: "Source, DetailType, and Detail are required",
			})
			continue
		}
		// An entry naming a bus that does not exist, or another account's or Region's bus by ARN,
		// fails on its own: PutEvents reports failures per entry. PutEventsResultEntry's list of
		// codes names none for an absent bus; ResourceNotFoundException, with the wording
		// DescribeEventBus was observed answering, is substrate's reading, and moto's.
		bus, busErr := ebRequestBus(ctx, entry.EventBusName)
		if busErr == nil {
			if err := p.requireBus(goCtx, ctx, bus); err != nil {
				if !errors.As(err, &busErr) {
					return nil, err
				}
			}
		}
		if busErr != nil {
			results = append(results, resultEntry{ErrorCode: busErr.Code, ErrorMessage: busErr.Message})
			continue
		}
		ev := EBEvent{
			Source:       entry.Source,
			DetailType:   entry.DetailType,
			Detail:       entry.Detail,
			EventBusName: bus,
			Time:         now,
			EventID:      ctx.IDs.HexUUID(),
		}
		existing = append(existing, ev)
		results = append(results, resultEntry{EventID: ev.EventID})
	}

	// Keep only the last maxEBEventRingBuffer events.
	if len(existing) > maxEBEventRingBuffer {
		existing = existing[len(existing)-maxEBEventRingBuffer:]
	}

	data, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("eventbridge putEvents marshal: %w", err)
	}
	if err := p.state.Put(goCtx, eventbridgeNamespace, eventsKey, data); err != nil {
		return nil, fmt.Errorf("eventbridge putEvents state.Put: %w", err)
	}

	failedCount := 0
	for _, r := range results {
		if r.ErrorCode != "" {
			failedCount++
		}
	}

	type response struct {
		FailedEntryCount int           `json:"FailedEntryCount"`
		Entries          []resultEntry `json:"Entries"`
	}
	return ebJSONResponse(http.StatusOK, response{FailedEntryCount: failedCount, Entries: results})
}

// --- Response helper ---------------------------------------------------------

func ebJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ebJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
