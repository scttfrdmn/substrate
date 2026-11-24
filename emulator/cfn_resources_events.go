package emulator

// StackDeployer helpers for CloudFormation's EventBridge resource types.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// cfnEventsTags renders a template's Tags property as EventBridge's Tag list, nil when it has none.
func cfnEventsTags(props map[string]interface{}, cctx *cfnContext) []EBTag {
	tags := cfnTagMap(props["Tags"], cctx)
	if len(tags) == 0 {
		return nil
	}
	out := make([]EBTag, 0, len(tags))
	for _, k := range taggingSortedKeys(tags) {
		out = append(out, EBTag{Key: k, Value: tags[k]})
	}
	return out
}

// deployEventsRule creates an EventBridge rule for the given CFN resource, on the bus EventBusName
// names (a name or an ARN; the default bus when absent).
func (d *StackDeployer) deployEventsRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	ruleName := resolveStringProp(props, "Name", logicalID, cctx)
	bus := ebBusName(resolveStringProp(props, "EventBusName", "", cctx))
	body := map[string]interface{}{
		"Name":  ruleName,
		"State": resolveStringProp(props, "State", "ENABLED", cctx),
	}
	if bus != ebDefaultBus {
		body["EventBusName"] = bus
	}
	if ep, ok := props["EventPattern"]; ok {
		body["EventPattern"] = cfnJSONDocument(ep, cctx)
	}
	if se, ok := props["ScheduleExpression"]; ok {
		body["ScheduleExpression"] = resolveValue(se, cctx)
	}
	if desc, ok := props["Description"]; ok {
		body["Description"] = resolveValue(desc, cctx)
	}
	if role := resolveStringProp(props, "RoleArn", "", cctx); role != "" {
		body["RoleArn"] = role
	}
	if tags := cfnEventsTags(props, cctx); tags != nil {
		body["Tags"] = tags
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal events rule body: %w", err)
	}
	req := &AWSRequest{
		Service:   "eventbridge",
		Operation: "PutRule",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Events::Rule",
		PhysicalID: ruleName,
		ARN:        ebRuleARN(cctx.region, cctx.accountID, bus, ruleName),
		Metadata:   map[string]interface{}{"RuleName": ruleName},
	}
	switch {
	case routeErr != nil:
		dr.Error = routeErr.Error()
	default:
		if resp != nil {
			var result struct {
				RuleArn string `json:"RuleArn"`
			}
			if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.RuleArn != "" {
				dr.ARN = result.RuleArn
			}
		}
		if targetErr := d.putEventsRuleTargets(ctx, ruleName, bus, cfnEventsTargets(props, cctx), streamID, &cost); targetErr != nil {
			dr.Error = targetErr.Error()
		}
	}
	return dr, cost, nil
}

// putEventsRuleTargets puts targets on rule, sending nothing when there are none.
func (d *StackDeployer) putEventsRuleTargets(
	ctx context.Context, rule, bus string, targets []EBTarget, streamID string, cost *float64,
) error {
	if len(targets) == 0 {
		return nil
	}
	req, err := cfnEventsTargetsRequest("PutTargets", rule, bus, "Targets", targets)
	if err != nil {
		return err
	}
	return d.cfnUpdateDispatch(ctx, req, streamID, cost)
}

// cfnEventsTargets renders a template's Targets property as PutTargets' entries. Each keeps the
// Id, Arn and RoleArn substrate stores for a target; the other Target members are not modeled.
func cfnEventsTargets(props map[string]interface{}, cctx *cfnContext) []EBTarget {
	list, _ := props["Targets"].([]interface{})
	out := make([]EBTarget, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		out = append(out, EBTarget{
			Id:      resolveStringProp(m, "Id", "", cctx),
			ARN:     resolveStringProp(m, "Arn", "", cctx),
			RoleARN: resolveStringProp(m, "RoleArn", "", cctx),
		})
	}
	return out
}

// cfnEventsTargetIDs returns the Ids of targets.
func cfnEventsTargetIDs(targets []EBTarget) []string {
	ids := make([]string, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, t.Id)
	}
	return ids
}

// cfnEventsTargetsRequest builds a PutTargets or RemoveTargets request on rule's bus, with value
// (targets or Ids) under member.
func cfnEventsTargetsRequest(operation, rule, bus, member string, value any) (*AWSRequest, error) {
	body := map[string]interface{}{"Rule": rule, member: value}
	if bus != ebDefaultBus {
		body["EventBusName"] = bus
	}
	return cfnJSONRequest("eventbridge", operation, body)
}

// cfnEventsBusProp returns the bus a declaration's EventBusName names.
func cfnEventsBusProp(props map[string]interface{}, cctx *cfnContext) string {
	return ebBusName(resolveStringProp(props, "EventBusName", "", cctx))
}

// cfnEventsRuleBus returns the bus a deployed rule lives on: the bus its recorded ARN names, which
// is where the rule was created whatever a later declaration names, or, for a record with no ARN,
// the bus declared names. declared is the declaration the record was deployed from.
func cfnEventsRuleBus(dr DeployedResource, declared map[string]interface{}, cctx *cfnContext) string {
	if t, err := ebParseTagARN(dr.ARN); err == nil && !t.isBus {
		return t.ref.bus
	}
	return cfnEventsBusProp(declared, cctx)
}

// cfnEventsRuleRequest builds a request naming the rule dr records on its bus.
func cfnEventsRuleRequest(operation string, dr DeployedResource, bus string) (*AWSRequest, error) {
	body := map[string]interface{}{"Name": dr.PhysicalID}
	if bus != ebDefaultBus {
		body["EventBusName"] = bus
	}
	return cfnJSONRequest("eventbridge", operation, body)
}

// cfnDeleteEventsRule deletes a rule on the bus it was created on, which DeleteRule needs: a rule
// on a custom bus is not found on the default one. Its declared targets are removed first by
// [cfnDeleteEventsRuleTargets]. A request that cannot be built is logged and skipped, as the sweep
// reports a nil request.
func cfnDeleteEventsRule(d *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
	req, err := cfnEventsRuleRequest("DeleteRule", dr, cfnEventsRuleBus(dr, props, cctx))
	if err != nil {
		d.logEventsRuleBuildFailure(dr, err)
		return nil
	}
	return req
}

// cfnDeleteEventsRuleTargets is the delete pre-step that removes the targets a rule's declaration
// names, which DeleteRule refuses to delete the rule with. Targets are a property of
// AWS::Events::Rule, not resources of their own, so these are the targets the stack manages; one
// added outside the stack still blocks the delete, as DeleteRule answers.
func cfnDeleteEventsRuleTargets(d *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) []*AWSRequest {
	ids := cfnEventsTargetIDs(cfnEventsTargets(props, cctx))
	if len(ids) == 0 {
		return nil
	}
	req, err := cfnEventsTargetsRequest("RemoveTargets", dr.PhysicalID, cfnEventsRuleBus(dr, props, cctx), "Ids", ids)
	if err != nil {
		d.logEventsRuleBuildFailure(dr, err)
		return nil
	}
	return []*AWSRequest{req}
}

// logEventsRuleBuildFailure logs a rule request the deployer could not build.
func (d *StackDeployer) logEventsRuleBuildFailure(dr DeployedResource, err error) {
	if d != nil && d.logger != nil {
		d.logger.Warn("cfn: cannot build an EventBridge rule request", "rule", dr.PhysicalID, "error", err)
	}
}

// cfnEventsListTargets reads the targets of rule on bus, reporting false when the rule is not there.
func (d *StackDeployer) cfnEventsListTargets(
	ctx context.Context, rule, bus, streamID string, cost *float64,
) ([]EBTarget, bool, error) {
	req, err := cfnEventsTargetsRequest("ListTargetsByRule", rule, bus, "Limit", 100)
	if err != nil {
		return nil, false, err
	}
	resp, c, err := d.dispatch(ctx, req, streamID)
	*cost += c
	var awsErr *AWSError
	if errors.As(err, &awsErr) && awsErr.Code == "ResourceNotFoundException" {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var out struct {
		Targets []EBTarget `json:"Targets"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return nil, false, fmt.Errorf("cfn decode targets of rule %s: %w", rule, err)
	}
	return out.Targets, true, nil
}

// updateEventsRule applies a changed AWS::Events::Rule in place. Name is Update requires:
// Replacement ([cfnRecreationProperties]); every other property, EventBusName included, goes
// through PutRule, which replaces every member but the tags, and the declared Targets through
// PutTargets.
//
// A changed EventBusName moves the rule ([StackDeployer.moveEventsRule]). Where the rule lives
// before the update is the bus the previous declaration names, cctx.priorProps: on a rollback or a
// forced re-apply that is the failed update's declaration, which may already have moved it, while
// the record's ARN is the deployment the update started from. Each is checked, and the rule is
// moved from whichever holds it.
//
// On the same bus, a target the previous declaration named and this one does not is removed, and
// changed Tags are synced through TagResource and UntagResource, since PutRule ignores Tags when it
// updates a rule.
func (d *StackDeployer) updateEventsRule(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	candidates := []string{cfnEventsBusProp(cctx.priorProps, cctx), cfnEventsRuleBus(prior, cctx.priorProps, cctx)}
	declaredBefore := cfnEventsTargetIDs(cfnEventsTargets(cctx.priorProps, cctx))
	next, cost, err := d.deployEventsRule(ctx, logicalID, props, streamID, cctx)
	if err != nil || next.Error != "" {
		return next, cost, err
	}
	declaredNow := cfnEventsTargetIDs(cfnEventsTargets(props, cctx))
	if reconcileErr := d.reconcileEventsRule(ctx, prior.PhysicalID, candidates, cfnEventsRuleBus(next, props, cctx),
		declaredBefore, declaredNow, slices.Contains(changed, "Tags"), next.ARN, cfnEventsTags(props, cctx),
		streamID, &cost); reconcileErr != nil {
		next.Error = reconcileErr.Error()
	}
	return next, cost, nil
}

// reconcileEventsRule finishes an update [StackDeployer.updateEventsRule] has applied through
// PutRule on newBus: it moves the rule from the first of candidates (other than newBus) that holds
// it, or, when none does, removes the targets the declaration dropped and syncs changed tags.
func (d *StackDeployer) reconcileEventsRule(
	ctx context.Context, rule string, candidates []string, newBus string, declaredBefore, declaredNow []string,
	tagsChanged bool, arn string, tags []EBTag, streamID string, cost *float64,
) error {
	for i, oldBus := range candidates {
		if oldBus == newBus || slices.Contains(candidates[:i], oldBus) {
			continue
		}
		live, found, err := d.cfnEventsListTargets(ctx, rule, oldBus, streamID, cost)
		if err != nil {
			return err
		}
		if found {
			return d.moveEventsRule(ctx, rule, oldBus, newBus, live,
				slices.Concat(declaredBefore, declaredNow), declaredNow, streamID, cost)
		}
	}
	var dropped []string
	for _, id := range declaredBefore {
		if !slices.Contains(declaredNow, id) {
			dropped = append(dropped, id)
		}
	}
	if len(dropped) > 0 {
		req, err := cfnEventsTargetsRequest("RemoveTargets", rule, newBus, "Ids", dropped)
		if err != nil {
			return err
		}
		if err := d.cfnUpdateDispatch(ctx, req, streamID, cost); err != nil {
			return err
		}
	}
	if tagsChanged {
		return d.syncEventsTags(ctx, arn, tags, streamID, cost)
	}
	return nil
}

// moveEventsRule finishes moving rule from oldBus to newBus, where PutRule has created it with its
// declared targets (declaredNow). A target live on the old rule that no declaration names (declared)
// was added outside the stack and is copied to the new rule; then the old rule's targets are
// removed and the old rule deleted.
//
// The move is all or nothing. If a step fails, what it changed is put back: the old rule's targets
// are restored, and the copy on newBus loses its targets and is deleted. The step's error is
// returned; a failure while putting things back is joined to it.
func (d *StackDeployer) moveEventsRule(
	ctx context.Context, rule, oldBus, newBus string, live []EBTarget, declared, declaredNow []string,
	streamID string, cost *float64,
) error {
	var copied []EBTarget
	for _, t := range live {
		if !slices.Contains(declared, t.Id) {
			copied = append(copied, t)
		}
	}
	type step struct {
		op, bus, member string
		value           any
	}
	steps := []step{
		{"PutTargets", newBus, "Targets", copied},
		{"RemoveTargets", oldBus, "Ids", cfnEventsTargetIDs(live)},
		{"DeleteRule", oldBus, "", nil},
	}
	undo := []step{
		{"PutTargets", oldBus, "Targets", live},
		{"RemoveTargets", newBus, "Ids", append(cfnEventsTargetIDs(copied), declaredNow...)},
		{"DeleteRule", newBus, "", nil},
	}
	send := func(s step) error {
		var req *AWSRequest
		var err error
		switch {
		case s.op == "DeleteRule":
			req, err = cfnEventsRuleRequest("DeleteRule", DeployedResource{PhysicalID: rule}, s.bus)
		case emptyEventsList(s.value):
			return nil
		default:
			req, err = cfnEventsTargetsRequest(s.op, rule, s.bus, s.member, s.value)
		}
		if err != nil {
			return err
		}
		return d.cfnUpdateDispatch(ctx, req, streamID, cost)
	}
	for i, s := range steps {
		if err := send(s); err != nil {
			// The old rule's targets are restored only once they were removed (step 1 succeeded).
			first := 1
			if i >= 2 {
				first = 0
			}
			var undoErrs []error
			for _, u := range undo[first:] {
				if undoErr := send(u); undoErr != nil {
					undoErrs = append(undoErrs, undoErr)
				}
			}
			return errors.Join(append([]error{fmt.Errorf("move rule %s from bus %s to %s: %w", rule, oldBus, newBus, err)}, undoErrs...)...)
		}
	}
	return nil
}

// emptyEventsList reports whether v is an empty target or Id list, for which no request is sent.
func emptyEventsList(v any) bool {
	switch l := v.(type) {
	case []EBTarget:
		return len(l) == 0
	case []string:
		return len(l) == 0
	}
	return false
}

// deployEventsEventBus creates an EventBridge event bus for the given CFN resource
// (AWS::Events::EventBus), then sets its Policy through PutPermission when the template declares
// one. Ref is the bus name and Fn::GetAtt answers Arn and Name.
func (d *StackDeployer) deployEventsEventBus(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	body := map[string]interface{}{"Name": name}
	for _, k := range []string{"Description", "EventSourceName", "KmsKeyIdentifier"} {
		if v := resolveStringProp(props, k, "", cctx); v != "" {
			body[k] = v
		}
	}
	if dlq, ok := props["DeadLetterConfig"].(map[string]interface{}); ok {
		if arn := resolveStringProp(dlq, "Arn", "", cctx); arn != "" {
			body["DeadLetterConfig"] = EBDeadLetterConfig{Arn: arn}
		}
	}
	if lc, ok := props["LogConfig"].(map[string]interface{}); ok {
		body["LogConfig"] = EBLogConfig{
			IncludeDetail: resolveStringProp(lc, "IncludeDetail", "", cctx),
			Level:         resolveStringProp(lc, "Level", "", cctx),
		}
	}
	if tags := cfnEventsTags(props, cctx); tags != nil {
		body["Tags"] = tags
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal event bus body: %w", err)
	}
	_, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "eventbridge", Operation: "CreateEventBus", Body: bodyBytes,
		Headers: map[string]string{}, Params: map[string]string{},
	}, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Events::EventBus",
		PhysicalID: name,
		ARN:        ebBusARN(cctx.region, cctx.accountID, name),
		Metadata:   map[string]interface{}{"Name": name},
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if policy := cfnJSONDocument(props["Policy"], cctx); policy != "" {
		raw, err := json.Marshal(map[string]string{"EventBusName": name, "Policy": policy})
		if err != nil {
			return DeployedResource{}, 0, fmt.Errorf("marshal event bus policy body: %w", err)
		}
		_, policyCost, policyErr := d.dispatch(ctx, &AWSRequest{
			Service: "eventbridge", Operation: "PutPermission", Body: raw,
			Headers: map[string]string{}, Params: map[string]string{},
		}, streamID)
		cost += policyCost
		if policyErr != nil {
			dr.Error = policyErr.Error()
		}
	}
	return dr, cost, nil
}

// updateEventsEventBus applies a changed AWS::Events::EventBus in place. Name is Update requires:
// Replacement ([cfnRecreationProperties]), and so is EventSourceName here. The configuration goes
// through UpdateEventBus, sending an empty Description, KmsKeyIdentifier, DeadLetterConfig or
// LogConfig for an absent one, which is how UpdateEventBus clears it. Policy goes through
// PutPermission, or RemovePermission with RemoveAllPermissions when the template drops it. Tags are
// synced by tagging the declared set and untagging every key the template no longer declares.
func (d *StackDeployer) updateEventsEventBus(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	name := prior.PhysicalID
	var cost float64
	var reqs []*AWSRequest
	if slices.ContainsFunc(changed, func(p string) bool {
		return slices.Contains([]string{"Description", "KmsKeyIdentifier", "DeadLetterConfig", "LogConfig"}, p)
	}) {
		body := map[string]interface{}{
			"Name":             name,
			"Description":      resolveStringProp(props, "Description", "", cctx),
			"KmsKeyIdentifier": resolveStringProp(props, "KmsKeyIdentifier", "", cctx),
			"DeadLetterConfig": EBDeadLetterConfig{},
			"LogConfig":        EBLogConfig{},
		}
		if dlq, ok := props["DeadLetterConfig"].(map[string]interface{}); ok {
			body["DeadLetterConfig"] = EBDeadLetterConfig{Arn: resolveStringProp(dlq, "Arn", "", cctx)}
		}
		if lc, ok := props["LogConfig"].(map[string]interface{}); ok {
			body["LogConfig"] = EBLogConfig{
				IncludeDetail: resolveStringProp(lc, "IncludeDetail", "", cctx),
				Level:         resolveStringProp(lc, "Level", "", cctx),
			}
		}
		req, err := cfnJSONRequest("eventbridge", "UpdateEventBus", body)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		reqs = append(reqs, req)
	}
	if slices.Contains(changed, "Policy") {
		op, body := "RemovePermission", map[string]interface{}{"EventBusName": name, "RemoveAllPermissions": true}
		if policy := cfnJSONDocument(props["Policy"], cctx); policy != "" {
			op, body = "PutPermission", map[string]interface{}{"EventBusName": name, "Policy": policy}
		}
		req, err := cfnJSONRequest("eventbridge", op, body)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		reqs = append(reqs, req)
	}
	for _, req := range reqs {
		if updateErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); updateErr != nil {
			dr.Error = updateErr.Error()
			break
		}
	}
	if dr.Error == "" && slices.Contains(changed, "Tags") {
		if tagErr := d.syncEventsTags(ctx, dr.ARN, cfnEventsTags(props, cctx), streamID, &cost); tagErr != nil {
			dr.Error = tagErr.Error()
		}
	}
	return dr, cost, nil
}

// syncEventsTags makes the tags of the EventBridge resource at arn exactly want.
func (d *StackDeployer) syncEventsTags(ctx context.Context, arn string, want []EBTag, streamID string, cost *float64) error {
	listReq, err := cfnJSONRequest("eventbridge", "ListTagsForResource", map[string]string{"ResourceARN": arn})
	if err != nil {
		return err
	}
	resp, c, err := d.dispatch(ctx, listReq, streamID)
	*cost += c
	if err != nil {
		return err
	}
	var current struct {
		Tags []EBTag `json:"Tags"`
	}
	if err := json.Unmarshal(resp.Body, &current); err != nil {
		return fmt.Errorf("cfn decode event bus tags: %w", err)
	}
	keep := make(map[string]bool, len(want))
	for _, t := range want {
		keep[t.Key] = true
	}
	var drop []string
	for _, t := range current.Tags {
		if !keep[t.Key] {
			drop = append(drop, t.Key)
		}
	}
	if len(drop) > 0 {
		req, err := cfnJSONRequest("eventbridge", "UntagResource", map[string]interface{}{"ResourceARN": arn, "TagKeys": drop})
		if err != nil {
			return err
		}
		if err := d.cfnUpdateDispatch(ctx, req, streamID, cost); err != nil {
			return err
		}
	}
	if len(want) > 0 {
		req, err := cfnJSONRequest("eventbridge", "TagResource", map[string]interface{}{"ResourceARN": arn, "Tags": want})
		if err != nil {
			return err
		}
		if err := d.cfnUpdateDispatch(ctx, req, streamID, cost); err != nil {
			return err
		}
	}
	return nil
}
