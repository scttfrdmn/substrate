package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CloudFormation stack statuses, from the Stack data type's enumeration. Only the
// ones substrate can reach are named: it deploys synchronously, so no caller ever
// observes an IN_PROGRESS status except the one a rollback persists before it
// sweeps, which is what makes a wedged rollback inspectable.
const (
	cfnStackCreateComplete = "CREATE_COMPLETE"

	// cfnStackCreateFailed is a create whose failure was not rolled back, which is
	// what OnFailure DO_NOTHING and DisableRollback ask for.
	cfnStackCreateFailed = "CREATE_FAILED"

	// cfnStackRollbackInProgress is persisted before the rollback sweep starts, so
	// a sweep that cannot finish leaves a stack saying so rather than a stack that
	// still claims to be creating.
	cfnStackRollbackInProgress = "ROLLBACK_IN_PROGRESS"

	// cfnStackRollbackComplete is a create that failed and whose resources were
	// removed. The stack itself remains: a ROLLBACK_COMPLETE stack is still
	// returned by DescribeStacks and still blocks a create of the same name until
	// it is deleted.
	cfnStackRollbackComplete = "ROLLBACK_COMPLETE"

	// cfnStackRollbackFailed is a rollback whose own sweep could not remove a
	// resource. Claiming ROLLBACK_COMPLETE there would assert a cleanliness that
	// does not hold.
	cfnStackRollbackFailed = "ROLLBACK_FAILED"

	cfnStackUpdateComplete = "UPDATE_COMPLETE"

	// cfnStackUpdateFailed is an update whose failure was not rolled back.
	cfnStackUpdateFailed = "UPDATE_FAILED"

	// cfnStackUpdateRollbackComplete is an update that failed and was converged
	// back onto the previously deployed template.
	cfnStackUpdateRollbackComplete = "UPDATE_ROLLBACK_COMPLETE"

	// cfnStackUpdateRollbackFailed is an update rollback whose re-deploy of the
	// previous template itself failed, leaving the stack in neither the old shape
	// nor the new one.
	cfnStackUpdateRollbackFailed = "UPDATE_ROLLBACK_FAILED"

	// cfnStackDeleteComplete is what OnFailure DELETE leaves behind: the record is
	// gone, so this is reported on the DeployResult rather than persisted.
	cfnStackDeleteComplete = "DELETE_COMPLETE"
)

// CloudFormation CreateStack OnFailure values: "this must be one of: DO_NOTHING,
// ROLLBACK, or DELETE".
const (
	// CFNOnFailureDoNothing leaves a failed stack and its resources in place. This
	// is what DisableRollback=true asks for by another name.
	CFNOnFailureDoNothing = "DO_NOTHING"

	// CFNOnFailureRollback deletes the resources the failed create had made and
	// leaves the stack reporting ROLLBACK_COMPLETE. "Although the default setting
	// is ROLLBACK", so this is what an unspecified option resolves to.
	CFNOnFailureRollback = "ROLLBACK"

	// CFNOnFailureDelete deletes the resources and the stack, leaving nothing.
	CFNOnFailureDelete = "DELETE"
)

// cfnValidOnFailure is the set CreateStack accepts. A value outside it is rejected
// rather than defaulted, because every alternative reading of an unrecognized
// value is a wrong one: treating it as ROLLBACK deletes resources a caller may have
// meant to keep, and treating it as DO_NOTHING keeps resources it may have meant to
// remove.
var cfnValidOnFailure = map[string]bool{
	CFNOnFailureDoNothing: true,
	CFNOnFailureRollback:  true,
	CFNOnFailureDelete:    true,
}

// CFNDeployOptions carries the stack-level failure options a create or update was
// requested with.
//
// The zero value is what the API's defaults resolve to — an unspecified OnFailure is
// ROLLBACK — so a caller that does not care passes nothing and gets CloudFormation's
// behavior rather than substrate's.
type CFNDeployOptions struct {
	// OnFailure is "DO_NOTHING", "ROLLBACK", "DELETE", or empty for the default.
	//
	// DisableRollback is deliberately absent: it is the same knob under another
	// name ("set to true to disable rollback of the stack if stack creation
	// failed"), and the two are mutually exclusive on the wire, so the wire layer
	// resolves whichever was given into this one field. Carrying both would invite
	// a caller to set them inconsistently and force every reader to decide which
	// wins.
	OnFailure string

	// Tags are the stack-level tags the operation supplies, nil when it supplies
	// none.
	//
	// Nil and empty mean different things on an update and the difference is the
	// API's: an omitted Tags leaves the stack's tags alone, an empty one removes
	// them all. A create sees no difference between the two — there is nothing to
	// preserve — so the zero value is still CloudFormation's own behavior, which is
	// what the type's contract above promises.
	Tags map[string]string
}

// resolve returns the OnFailure value in force, applying the API's default.
func (o CFNDeployOptions) resolve() string {
	if o.OnFailure == "" {
		return CFNOnFailureRollback
	}
	return o.OnFailure
}

// validate reports whether the options name an action CreateStack accepts.
func (o CFNDeployOptions) validate() error {
	if o.OnFailure != "" && !cfnValidOnFailure[o.OnFailure] {
		return cfnErrf(ErrCFNInvalidOnFailure,
			"OnFailure must be one of DO_NOTHING, ROLLBACK or DELETE, got %q", o.OnFailure)
	}
	// Validated here rather than at the wire layer so an in-process [Client] deploy is
	// held to the same limits as a request that arrived over HTTP: both funnel through
	// [StackDeployer.DeployWithOptions], and a rule enforced on one path only is one a
	// consumer discovers by switching harnesses.
	return cfnValidateStackTags(o.Tags)
}

// cfnFailedResources names the resources that failed to deploy, in the order they
// were attempted.
func cfnFailedResources(resources []DeployedResource) []string {
	var failed []string
	for _, r := range resources {
		if r.Error != "" {
			failed = append(failed, fmt.Sprintf("%s (%s): %s", r.LogicalID, r.Type, r.Error))
		}
	}
	return failed
}

// cfnCreateExistsCodes is the set of error codes that mean "a resource of this name
// already exists", as the plugins reachable from Deploy actually spell it.
//
// It exists because [StackDeployer.UpdateStack] re-creates a changed resource whose type has no
// update path in substrate, and its plugin refuses the duplicate. An unchanged resource is kept
// without a call (#1427), so the set matters only for a resource whose declaration is unchanged
// while its inputs are not, or one a rollback re-applies (see [cfnResourcePlan.clearsRefusal]).
// Real CloudFormation issues no create call in either case, so there is no real refusal being
// modeled here — the refusal is an artifact of substrate's re-create.
//
// Every entry was harvested by deploying a template twice against the default
// registry and recording the code that came back, the same way
// [cfnDeleteAbsentCodes] was built. Reading them off the AWS references would have
// missed that substrate's DynamoDB, Kinesis and Firehose all answer the shared
// ResourceInUseException while Step Functions has a code per resource
// (StateMachineAlreadyExists, ActivityAlreadyExists).
//
// Codes are absent when they are not unambiguously a name collision. ConflictException
// and AlreadyExistsException are each used by several services for conditions that are
// not a duplicate create, and clearing one of those would hide a real failure — the
// opposite and worse error. Missing an entry only leaves the pre-existing behavior of
// reporting the refusal, so the conservative side is the safe one.
//
// The two Step Functions entries changed meaning in #1072 and are kept deliberately.
// Both create operations now model the idempotency AWS publishes, so a re-create of an
// unchanged state machine or activity answers the existing ARN and reaches no refusal
// for this map to clear. ActivityAlreadyExists fires only for the one condition its page
// publishes, a changed EncryptionConfiguration (#1199), which a re-create of an unchanged
// declaration does not send — and StateMachineAlreadyExists now fires only for a genuinely
// different definition or type, the case the paragraph above argues an entry should be
// absent for. Neither is removed: the entries cost nothing while the code is unreachable,
// and dropping the live one would surface a refusal on the update path, which is
// #1077's subject rather than this map's.
var cfnCreateExistsCodes = map[string]bool{
	"ActivityAlreadyExists":                            true,
	"BucketAlreadyExists":                              true,
	"EntityAlreadyExists":                              true,
	"InvalidLaunchTemplateName.AlreadyExistsException": true,
	"RepositoryAlreadyExistsException":                 true,
	"ResourceAlreadyExistsException":                   true,
	"ResourceConflictException":                        true,
	"ResourceExistsException":                          true,
	"ResourceInUseException":                           true,
	"StateMachineAlreadyExists":                        true,
}

// cfnCreateIsExists reports whether a recorded resource error is only a duplicate-name
// refusal. The code is the text before the first ": ", matching how a dispatch error
// is rendered.
func cfnCreateIsExists(recorded string) bool {
	code := recorded
	if i := strings.Index(recorded, ": "); i >= 0 {
		code = recorded[:i]
	}
	return cfnCreateExistsCodes[code]
}

// previousStack returns the record this deploy is replacing, or nil when there is
// none. A stack outside this deployer's account and Region is not it: the record
// belongs to another scope and its resources are not the ones being redeployed.
func (d *StackDeployer) previousStack(ctx context.Context, stackName string) (*CFNStackState, error) {
	if d.state == nil {
		return nil, nil
	}
	prev, err := d.loadStack(ctx, stackName)
	if err != nil || prev == nil {
		return nil, err
	}
	if !d.sameScope(*prev) {
		return nil, nil
	}
	return prev, nil
}

// cfnSameDeclaration reports whether two template declarations of the same logical ID
// describe the same resource.
//
// Compared as marshaled JSON because Properties is an arbitrarily nested
// map[string]interface{}; Go's encoder sorts map keys, so the encoding is canonical
// and two declarations differing only in key order compare equal. A marshal error
// yields false, which keeps the refusal — the conservative direction.
func cfnSameDeclaration(before, after cfnResource) bool {
	if before.Type == "" || before.Type != after.Type {
		return false
	}
	beforeJSON, err := json.Marshal(before.Properties)
	if err != nil {
		return false
	}
	afterJSON, err := json.Marshal(after.Properties)
	if err != nil {
		return false
	}
	return string(beforeJSON) == string(afterJSON)
}

// cfnCreatedResources returns the resources a failed create did create, which are
// the ones a rollback has to remove.
//
// A resource carrying an error is excluded: the plugin refused the request, so there
// is nothing on the other side to delete, and dispatching its delete anyway would
// send a request built from the same rejected identifier — a bucket the plugin
// refused as invalidly named yields an equally invalid DeleteBucket, whose refusal
// is not a not-found and would turn a clean rollback into ROLLBACK_FAILED.
func cfnCreatedResources(resources []DeployedResource) []DeployedResource {
	created := make([]DeployedResource, 0, len(resources))
	for _, r := range resources {
		if r.Error == "" {
			created = append(created, r)
		}
	}
	return created
}

// cfnFailedCreate is everything the failure path needs about the create that failed.
// A struct rather than nine parameters, and unexported because it is an internal
// hand-off rather than API.
type cfnFailedCreate struct {
	stackName    string
	templateBody string
	params       map[string]string
	tags         map[string]string
	resources    []DeployedResource
	failures     []string
	streamID     string
	totalCost    float64
	start        time.Time
	createdAt    time.Time // the stack's creation time, carried from its previous record
	reviewedAt   time.Time // its REVIEW_IN_PROGRESS start, carried the same way
	onFailure    string
	tagFailures  []CFNTagPropagationFailure
}

// handleFailedCreate applies the stack's OnFailure option to a create that had at
// least one failed resource.
//
// Substrate deploys synchronously, so unlike the API this decision is reached within
// the call. The outcome is still reported the way the API reports it — as the stack's
// status rather than as an error — because that is what a caller polling
// DescribeStacks reads, and because a create whose stack rolled back is not a failed
// API call: real CreateStack has already returned its StackId by the time the
// rollback happens.
func (d *StackDeployer) handleFailedCreate(ctx context.Context, fc cfnFailedCreate) (*DeployResult, error) {
	result := &DeployResult{
		StackName:              fc.stackName,
		Resources:              fc.resources,
		StreamID:               fc.streamID,
		TotalCost:              fc.totalCost,
		Duration:               d.tc.Now().Sub(fc.start),
		Outputs:                map[string]string{},
		TagPropagationFailures: fc.tagFailures,
	}

	// A failed stack publishes no outputs, and therefore no exports. Resolving them
	// would let another stack import a value whose resource either failed to deploy
	// or is about to be rolled back — an import that resolves against nothing is
	// the silent-literal failure the export model exists to prevent.
	state := CFNStackState{
		StackName:    fc.stackName,
		TemplateBody: fc.templateBody,
		Parameters:   fc.params,
		Tags:         fc.tags,
		Resources:    fc.resources,
		Outputs:      map[string]string{},
		AccountID:    d.identity.accountID,
		Region:       d.identity.region,
		OnFailure:    fc.onFailure,
		CreatedAt:    fc.createdAt,
		ReviewedAt:   fc.reviewedAt,
		UpdatedAt:    d.tc.Now(),
	}
	if state.CreatedAt.IsZero() {
		state.CreatedAt = fc.start
	}
	// A stack that failed to create still records who created it. Its rollback's
	// deletes are dispatched by this same deployer, so they already run as the right
	// principal; persisting matters for what comes after — a caller that inspects a
	// ROLLBACK_FAILED stack and then deletes it gets a sweep attributed to the same
	// identity as the one that could not finish.
	state.setAttribution(d.attribution)

	if fc.onFailure == CFNOnFailureDoNothing {
		// The resources stay, including the ones deployed after the failure.
		// Substrate deploys the whole template rather than stopping at the first
		// failure, and that is what makes DO_NOTHING different from the API's: real
		// CloudFormation stops. The status is the same, and it is the status a
		// caller keys off.
		state.Status = cfnStackCreateFailed
		state.StatusReason = strings.Join(fc.failures, "; ")
		result.Status = cfnStackCreateFailed
		result.StatusReason = state.StatusReason
		if d.state != nil {
			if err := d.persistStack(ctx, state); err != nil {
				return nil, err
			}
		}
		d.logger.Warn("cfn: stack create failed and rollback is disabled",
			"stack", fc.stackName, "failures", len(fc.failures))
		return result, nil
	}

	// Persisted before the sweep, not after: the sweep dispatches real deletes, and
	// a stack that cannot finish rolling back must be discoverable as a rollback
	// that did not finish rather than as a stack that never started one.
	state.Status = cfnStackRollbackInProgress
	state.StatusReason = strings.Join(fc.failures, "; ")
	if d.state != nil {
		if err := d.persistStack(ctx, state); err != nil {
			return nil, err
		}
	}

	// Only the resources that were created are swept, and with cfnCreateRollbackOp:
	// this is "the rollback of the stack operation that initially created the
	// resource", so RetainExceptOnCreate deletes here where a DeleteStack sweep
	// would have kept it.
	sweep := state
	sweep.Resources = cfnCreatedResources(fc.resources)
	deletions := d.deleteStackResources(ctx, &sweep, fc.streamID, cfnCreateRollbackOp)
	state.ResourceDeletions = deletions
	state.UpdatedAt = d.tc.Now()
	result.ResourceDeletions = deletions

	if failed := cfnFailedDeletions(deletions); len(failed) > 0 {
		state.Status = cfnStackRollbackFailed
		state.StatusReason = "rollback could not delete: " + strings.Join(failed, "; ")
		result.Status = cfnStackRollbackFailed
		result.StatusReason = state.StatusReason
		if d.state != nil {
			if err := d.persistStack(ctx, state); err != nil {
				return nil, err
			}
		}
		d.logger.Warn("cfn: stack rollback failed",
			"stack", fc.stackName, "undeleted", len(failed))
		return result, nil
	}

	if fc.onFailure == CFNOnFailureDelete {
		// Nothing is left to report a status on, so the record goes too. The
		// DeployResult still carries DELETE_COMPLETE, which is the only place a
		// caller can learn what happened.
		result.Status = cfnStackDeleteComplete
		result.StatusReason = strings.Join(fc.failures, "; ")
		if d.state != nil {
			if err := d.removeStackRecord(ctx, fc.stackName); err != nil {
				return result, err
			}
		}
		d.logger.Warn("cfn: stack create failed and the stack was deleted",
			"stack", fc.stackName, "failures", len(fc.failures))
		return result, nil
	}

	// The stack stays. A ROLLBACK_COMPLETE stack is still reported by
	// DescribeStacks and still blocks a create of the same name until it is
	// deleted, which is the one thing that makes the failure impossible to miss.
	state.Status = cfnStackRollbackComplete
	state.StatusReason = strings.Join(fc.failures, "; ")
	result.Status = cfnStackRollbackComplete
	result.StatusReason = state.StatusReason
	if d.state != nil {
		if err := d.persistStack(ctx, state); err != nil {
			return nil, err
		}
	}
	d.logger.Warn("cfn: stack create failed and was rolled back",
		"stack", fc.stackName, "failures", len(fc.failures))
	return result, nil
}

// rollbackFailedUpdate converges a failed update back onto the record it was applied
// to, and reports UPDATE_ROLLBACK_COMPLETE.
//
// The update compared each resource with prev, so what it did to each is known
// (#1427), and the rollback undoes exactly that:
//
//   - A resource the update kept is untouched, and stays.
//   - A resource the update changed in place keeps its physical ID; the rollback applies
//     prev's declaration to it again, through the same update path.
//   - A resource the update created — an addition, or the replacement for a resource
//     whose change required one — is deleted. The resource it replaced was never deleted,
//     because a replaced resource is deleted only once an update succeeds, so prev's
//     record of it is the stack's again with its original physical ID.
//
// That is CloudFormation's rollback in outline: "CloudFormation creates the replacement
// resource first", and a rollback returns the stack to its previous resources. What
// remains approximate is a property prev never declared: re-applying prev's declaration
// leaves whatever the failed update set it to, since substrate has no record of the
// value it had before.
func (d *StackDeployer) rollbackFailedUpdate(
	ctx context.Context, prev CFNStackState, failed *DeployResult, outcome *cfnDeployOutcome,
	failures []string, streamID string,
) (*DeployResult, error) {
	d.logger.Warn("cfn: stack update failed; rolling back to the previous deployment",
		"stack", prev.StackName, "failures", len(failures))

	previous := make(map[string]DeployedResource, len(prev.Resources))
	for _, r := range prev.Resources {
		if r.Error == "" {
			previous[r.LogicalID] = r
		}
	}
	failedDecls := d.cfnFailedDeclarations(ctx, prev.StackName, outcome, failed.Resources)
	force := map[string]map[string]interface{}{}
	var created []DeployedResource
	for _, r := range failed.Resources {
		if outcome == nil || !outcome.dispatched[r.LogicalID] {
			continue
		}
		old, had := previous[r.LogicalID]
		inPlace := had && old.Type == r.Type && old.PhysicalID == r.PhysicalID && r.PhysicalID != ""
		if r.Error != "" {
			// An in-place update that was refused part-way — a security group whose revoke went
			// through and whose authorize did not — still changed the resource, so it is
			// restored as well. A refused create made nothing, and is neither restored nor swept.
			if _, updatable := cfnResourceUpdaters[r.Type]; inPlace && updatable {
				force[r.LogicalID] = failedDecls[r.LogicalID]
			}
			continue
		}
		if inPlace {
			force[r.LogicalID] = failedDecls[r.LogicalID]
			continue
		}
		created = append(created, r)
	}

	// DO_NOTHING so the re-deploy reports its own failures instead of rolling back:
	// a create rollback here would delete the resources the previous template
	// declares, which is the opposite of converging on it.
	// The previous template's tags go back with it. A rollback converges on the stack
	// as it was, and the tags are part of that: leaving the failed update's tags in
	// place would report a stack tagged by an operation that did not survive.
	result, reDeployed, err := d.deploy(ctx, prev.TemplateBody, streamID, prev.Parameters,
		CFNDeployOptions{OnFailure: CFNOnFailureDoNothing, Tags: prev.Tags}, &prev, force)
	if err != nil {
		return nil, fmt.Errorf("roll back update of stack %s: %w", prev.StackName, err)
	}

	// The failed update's own resources go last, once nothing restored refers to them,
	// with the declarations they were created from. RetainExceptOnCreate deletes here:
	// this is the rollback of the operation that created them.
	if len(created) > 0 && outcome != nil {
		sweep := CFNStackState{StackName: prev.StackName, TemplateBody: outcome.templateBody,
			Parameters: outcome.params, Resources: created}
		// Merged with whatever the re-deploy itself deleted, rather than replacing it, so the
		// record says what happened to every resource the rollback touched.
		result.ResourceDeletions = append(result.ResourceDeletions,
			d.sweepResources(ctx, &sweep, failed.Resources, streamID, cfnCreateRollbackOp)...)
	}

	reason := strings.Join(failures, "; ")
	status := cfnStackUpdateRollbackComplete
	reDeployFailures := cfnFailedResources(result.Resources)
	if reDeployed != nil {
		// An Output of the previous template that no longer resolves fails the rollback too (#1429).
		reDeployFailures = reDeployed.failures
	}
	if len(reDeployFailures) > 0 {
		// The stack is now in neither shape. Saying so is the whole value of the
		// status: the previous template no longer describes what is deployed.
		status = cfnStackUpdateRollbackFailed
		reason += " (rollback also failed: " + strings.Join(reDeployFailures, "; ") + ")"
	}
	if undeleted := cfnFailedDeletions(result.ResourceDeletions); len(undeleted) > 0 {
		reason += " (the rollback could not delete: " + strings.Join(undeleted, "; ") + ")"
	}
	result.Status = status
	result.StatusReason = reason
	if err := d.setStackOutcome(ctx, prev.StackName, status, reason, result.ResourceDeletions); err != nil {
		return nil, err
	}
	return result, nil
}

// cfnFailedDeclarations resolves each resource of a failed update's template against what that
// update deployed, in the structured form an updater reads, so a rollback can apply the difference
// between it and the previous declaration (see [cfnBaseline.force]). A template that no longer
// parses yields none, and the rollback then re-applies every declared property, as it did before.
func (d *StackDeployer) cfnFailedDeclarations(
	ctx context.Context, stackName string, outcome *cfnDeployOutcome, deployed []DeployedResource,
) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	if outcome == nil {
		return out
	}
	tmpl, err := d.parseCFNTemplate(outcome.templateBody)
	if err != nil {
		return out
	}
	fctx := buildCFNContext(tmpl, outcome.params, d.identity.region, d.identity.accountID, stackName)
	evaluateConditions(tmpl, fctx)
	fctx.exports = d.loadExports(ctx, stackName)
	for _, r := range deployed {
		fctx.resources[r.LogicalID] = r
	}
	for id, res := range tmpl.Resources {
		props, _ := cfnResolveStructured(res.Properties, fctx).(map[string]interface{})
		out[id] = props
	}
	fctx.takeFailures()
	return out
}

// setStackOutcome is [StackDeployer.setStackStatus] that also records the sweep's per-resource
// outcomes on the stack, so DescribeStackResources and the next update read the same deletions
// the result reported.
func (d *StackDeployer) setStackOutcome(
	ctx context.Context, stackName, status, reason string, deletions []CFNResourceDeletion,
) error {
	if d.state == nil {
		return nil
	}
	stack, err := d.loadStack(ctx, stackName)
	if err != nil || stack == nil {
		d.logger.Warn("cfn: cannot set stack status; stack not readable",
			"stack", stackName, "status", status)
		return nil //nolint:nilerr // an unreadable stack is logged and skipped, as setStackStatus does.
	}
	stack.Status = status
	stack.StatusReason = reason
	stack.ResourceDeletions = deletions
	stack.UpdatedAt = d.tc.Now()
	return d.persistStack(ctx, *stack)
}

// setStackStatus overwrites a persisted stack's status and reason, leaving the rest
// of the record as the deploy wrote it. A stack that cannot be read is logged and skipped; a
// write that fails is returned (#1192).
func (d *StackDeployer) setStackStatus(ctx context.Context, stackName, status, reason string) error {
	if d.state == nil {
		return nil
	}
	stack, err := d.loadStack(ctx, stackName)
	if err != nil || stack == nil {
		d.logger.Warn("cfn: cannot set stack status; stack not readable",
			"stack", stackName, "status", status)
		return nil //nolint:nilerr // an unreadable stack is logged and skipped (#1192).
	}
	stack.Status = status
	stack.StatusReason = reason
	stack.UpdatedAt = d.tc.Now()
	return d.persistStack(ctx, *stack)
}

// removeStackRecord deletes a stack's record and its entry in the names index.
//
// Extracted from DeleteStack so the OnFailure=DELETE path removes a stack exactly
// the way a delete does. Two ways to unpersist a stack would eventually disagree,
// and the disagreement would be a stack that is gone from one listing and present in
// the other.
func (d *StackDeployer) removeStackRecord(ctx context.Context, stackName string) error {
	// A stack recorded before #1366 moves to its scoped keys first, so one delete removes it.
	if err := d.migrateLegacyStack(ctx, stackName); err != nil {
		return fmt.Errorf("delete stack %s: %w", stackName, err)
	}
	if err := d.state.Delete(ctx, cfnNamespace, d.stackKey(stackName)); err != nil {
		return fmt.Errorf("delete stack %s: %w", stackName, err)
	}
	names, err := d.loadStackNames(ctx)
	if err != nil {
		return err
	}
	remaining := make([]string, 0, len(names))
	for _, n := range names {
		if n != stackName {
			remaining = append(remaining, n)
		}
	}
	return d.saveStackNames(ctx, remaining)
}

// cfnResolveFailureOptions turns CreateStack's two mutually exclusive failure
// parameters into the single action they describe.
//
// "You can specify either DisableRollback or OnFailure, but not both", so specifying
// both is a ValidationError rather than a precedence rule — and the test is
// *presence*, not value: the CLI's --no-disable-rollback sends DisableRollback=false
// explicitly, so a rule that ignored a false would accept a combination the API
// rejects. This was measured against the CLI rather than assumed.
//
// present tells this apart from absent for DisableRollback, which is why the wire
// layer passes the raw parameter map rather than a bool.
func cfnResolveFailureOptions(onFailure, disableRollback string, disableRollbackPresent bool) (CFNDeployOptions, error) {
	if onFailure != "" && disableRollbackPresent {
		return CFNDeployOptions{}, cfnErrf(ErrCFNInvalidOnFailure,
			"You can specify either DisableRollback or OnFailure, but not both")
	}
	if disableRollbackPresent {
		switch strings.ToLower(disableRollback) {
		case "true":
			return CFNDeployOptions{OnFailure: CFNOnFailureDoNothing}, nil
		case "false":
			return CFNDeployOptions{OnFailure: CFNOnFailureRollback}, nil
		default:
			return CFNDeployOptions{}, cfnErrf(ErrCFNInvalidOnFailure,
				"DisableRollback must be true or false, got %q", disableRollback)
		}
	}
	opts := CFNDeployOptions{OnFailure: onFailure}
	if err := opts.validate(); err != nil {
		return CFNDeployOptions{}, err
	}
	return opts, nil
}

// cfnDeletionFor finds a sweep's record for one resource, or nil when the sweep did
// not touch it — which for a rollback means the resource was never created.
//
// The match is on the logical ID and, when the record carries one, the physical ID: a
// deletion of the resource an update replaced, or of the replacement a failed update
// created, shares its logical ID with the resource the stack now holds, and is not a
// deletion of it. A record written before #1427 carries no physical ID and matches on
// the logical ID alone, which is all its sweeps could mean.
func cfnDeletionFor(deletions []CFNResourceDeletion, r DeployedResource) *CFNResourceDeletion {
	for i := range deletions {
		d := &deletions[i]
		if d.LogicalID == r.LogicalID && (d.PhysicalID == "" || d.PhysicalID == r.PhysicalID) {
			return d
		}
	}
	return nil
}

// cfnStackDisablesRollback reports what DescribeStacks should say for a stack's
// DisableRollback member.
//
// It is derived from OnFailure rather than stored beside it because they are one
// knob: DisableRollback=true is DO_NOTHING. Storing both would let a record say
// DO_NOTHING and DisableRollback=false at once, and nothing could resolve which the
// caller had asked for.
func cfnStackDisablesRollback(onFailure string) bool {
	return onFailure == CFNOnFailureDoNothing
}

// cfnErrText renders an error for a log field, or "none" when there is no error. A
// bare err.Error() would panic on nil, and a nil-valued field logs as "<nil>", which
// reads as a failure that did not happen.
func cfnErrText(err error) string {
	if err == nil {
		return "none"
	}
	return err.Error()
}
