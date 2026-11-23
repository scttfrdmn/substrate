package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"
)

// cfn_changeset_lifecycle.go models a change set's type and lifecycle (#1606): CREATE change sets,
// the REVIEW_IN_PROGRESS stack one creates, the empty change set, and what executing one leaves
// behind. Until #1606 substrate modeled only UPDATE change sets on an existing stack, so
// `cdk bootstrap` — and every `cdk deploy` of a new stack — failed at its first CreateChangeSet.
//
// The behavior, from API_CreateChangeSet, API_ExecuteChangeSet and API_DescribeChangeSet:
//
//   - **CREATE.** "If you create a change set for a new stack, CloudFormation creates a stack with a
//     unique stack ID, but no template or resources. The stack will be in the REVIEW_IN_PROGRESS
//     state until you execute the change set." The stack is a real record: DescribeStacks and
//     ListStacks report it, DescribeStackEvents reports its REVIEW_IN_PROGRESS event, and
//     DeleteStack removes it. A CREATE change set on a stack already in REVIEW_IN_PROGRESS is
//     accepted — the CDK creates one there after a deploy that never executed — and one on any
//     other existing stack is refused: "You can't use … the CREATE type to create a change set for
//     an existing stack". The change set is the diff against an empty template, so every change is
//     an Add.
//   - **UPDATE** is unchanged, and remains the default: "By default, CloudFormation specifies
//     UPDATE".
//   - **IMPORT** is refused. Importing brings resources that exist outside any stack under one,
//     which needs a resource-identifier model substrate does not have.
//   - **An UPDATE change set with no changes is FAILED**, with ExecutionStatus UNAVAILABLE and
//     StatusReason "The submitted information didn't contain changes. Submit different information
//     to create a change set." That text is AWS's: the AWS Cloud Operations Blog ("AWS
//     Organizations, moving an organization member account to another organization: Part 3")
//     quotes it as the reason a change set with nothing to do reports. The CDK reads exactly this
//     prefix (toolkit-lib changeSetHasNoChanges) as "no changes", deletes the change set and
//     reports the deploy as a no-op, which is how a second `cdk bootstrap` or an unchanged
//     `cdk deploy` succeeds.
//   - **Execution.** A change set whose ExecutionStatus is not AVAILABLE cannot be executed
//     (InvalidChangeSetStatus). Executing one leaves it in place reporting EXECUTE_COMPLETE, or
//     EXECUTE_FAILED when the stack ended in a failed or rolled-back state, and deletes every
//     other change set of the stack: "When you execute a change set, CloudFormation deletes all
//     other change sets associated with the stack because they aren't valid for the updated stack."
//     Before #1606 the executed change set was deleted too, which is why a DescribeChangeSet after
//     an execute answered ChangeSetNotFound.
//
// Substrate creates and executes synchronously, so a change set is never observed in
// CREATE_PENDING, CREATE_IN_PROGRESS or EXECUTE_IN_PROGRESS: each request answers with the change
// set, or the stack, already settled — the rule the rest of the deployer follows.

// Change-set and stack statuses this file writes.
const (
	// cfnStackReviewInProgress is the status of a stack a CREATE change set created and that has not
	// been executed yet.
	cfnStackReviewInProgress = "REVIEW_IN_PROGRESS"

	cfnChangeSetCreateComplete = "CREATE_COMPLETE"
	cfnChangeSetFailed         = "FAILED"

	cfnChangeSetAvailable       = "AVAILABLE"
	cfnChangeSetUnavailable     = "UNAVAILABLE"
	cfnChangeSetExecuteComplete = "EXECUTE_COMPLETE"
	cfnChangeSetExecuteFailed   = "EXECUTE_FAILED"

	// cfnEmptyChangeSetReason is AWS's StatusReason for a change set with nothing to change; see the
	// file comment for its source.
	cfnEmptyChangeSetReason = "The submitted information didn't contain changes. " +
		"Submit different information to create a change set."

	// cfnUserInitiated is the reason real CloudFormation puts on a caller-initiated stack event.
	cfnUserInitiated = "User Initiated"
)

// The ChangeSetType values: "Valid Values: CREATE | UPDATE | IMPORT".
const (
	// CFNChangeSetTypeCreate creates a change set for a stack that does not exist yet.
	CFNChangeSetTypeCreate = "CREATE"

	// CFNChangeSetTypeUpdate creates a change set for an existing stack, and is the default.
	CFNChangeSetTypeUpdate = "UPDATE"

	// CFNChangeSetTypeImport imports existing resources into a stack. Substrate refuses it.
	CFNChangeSetTypeImport = "IMPORT"
)

// CFNChangeSetOptions is what [StackDeployer.CreateChangeSetWithOptions] takes beyond a change
// set's template and parameters. The zero value is an UPDATE change set with no tags parameter,
// which is what [StackDeployer.CreateChangeSet] has always created.
type CFNChangeSetOptions struct {
	// Type is the ChangeSetType: CFNChangeSetTypeCreate or CFNChangeSetTypeUpdate. Empty is UPDATE.
	Type string

	// Tags are the stack tags executing the change set applies, with the meaning
	// [StackDeployer.CreateChangeSet] gives them: nil leaves the stack's tags as they are.
	Tags map[string]string

	// RoleARN is the service role the change set's execution, and every later operation on the
	// stack, runs as. The deployer records it; the wire plugin attributes the execution to it.
	RoleARN string

	// Capabilities are the acknowledged capabilities, recorded so DescribeChangeSet reports them.
	// Substrate does not enforce InsufficientCapabilities.
	Capabilities []string

	// Description is the caller's description of the change set.
	Description string

	// OnStackFailure is what a failed execution of a CREATE change set does: DO_NOTHING, ROLLBACK
	// or DELETE. Empty defers to the execution's DisableRollback.
	OnStackFailure string
}

// changeSetType returns the ChangeSetType the options ask for, UPDATE when unset.
func (o CFNChangeSetOptions) changeSetType() string {
	if o.Type == "" {
		return CFNChangeSetTypeUpdate
	}
	return o.Type
}

// validate refuses a ChangeSetType substrate does not model and an OnStackFailure CreateChangeSet
// does not accept.
func (o CFNChangeSetOptions) validate() error {
	switch o.changeSetType() {
	case CFNChangeSetTypeCreate, CFNChangeSetTypeUpdate:
	case CFNChangeSetTypeImport:
		return cfnErrf(ErrCFNChangeSetTypeUnsupported,
			"ChangeSetType IMPORT is not emulated: substrate does not import existing resources into a stack")
	default:
		// Substrate's own message, written in the form of the service's model-validation errors
		// (the GetTemplate TemplateStage refusal is the same form); AWS publishes no text for it.
		return cfnErrf(ErrCFNChangeSetTypeUnsupported,
			"1 validation error detected: Value '%s' at 'changeSetType' failed to satisfy constraint: "+
				"Member must satisfy enum value set: [CREATE, UPDATE, IMPORT]", o.Type)
	}
	if o.OnStackFailure == "" {
		return nil
	}
	if _, err := cfnResolveFailureOptions(o.OnStackFailure, "", false); err != nil {
		return err
	}
	// "DELETE - Deletes the change set if the stack creation fails. This is only valid when the
	// ChangeSetType parameter is set to CREATE." The message is substrate's own.
	if o.OnStackFailure == CFNOnFailureDelete && o.changeSetType() != CFNChangeSetTypeCreate {
		return cfnErrf(ErrCFNInvalidOnFailure, "OnStackFailure DELETE is only valid for a CREATE change set")
	}
	return nil
}

// CFNExecuteOptions is what [StackDeployer.ExecuteChangeSetWithOptions] takes beyond the change set.
type CFNExecuteOptions struct {
	// DisableRollback is ExecuteChangeSet's DisableRollback, nil when the request omitted it. For a
	// CREATE change set true is DO_NOTHING and false is ROLLBACK, and it may not be combined with
	// the change set's OnStackFailure. An omitted one rolls a failed create back, which is
	// CloudFormation's create default and what the CDK expects when it sends none. For an UPDATE
	// change set it is not modeled: an update that fails rolls back, as [StackDeployer.UpdateStack]
	// documents.
	DisableRollback *bool
}

// changeSetBase loads the stack a change set is compared against and that stack's template.
//
// For an UPDATE the stack must exist and admit an update. For a CREATE it must not exist, or be in
// REVIEW_IN_PROGRESS; the template it is compared against is empty either way, since a
// REVIEW_IN_PROGRESS stack has "no template or resources". A stack that does not exist yet is
// returned as a record carrying only its name, which [StackDeployer.ensureReviewStack] persists once
// the change set is known to be valid.
func (d *StackDeployer) changeSetBase(ctx context.Context, stackName, changeSetName string,
	opts CFNChangeSetOptions,
) (*CFNStackState, *cfnTemplate, error) {
	stack, err := d.loadStack(ctx, stackName)
	if err != nil {
		return nil, nil, fmt.Errorf("cfn CreateChangeSet: %w", err)
	}
	if opts.changeSetType() == CFNChangeSetTypeCreate {
		if stack != nil && stack.Status != cfnStackReviewInProgress {
			return nil, nil, cfnErrf(ErrCFNStackAlreadyExists,
				"Stack [%s] already exists and cannot be created again with the changeSet [%s].",
				stackName, changeSetName)
		}
		if stack == nil {
			stack = &CFNStackState{StackName: stackName}
		}
		return stack, &cfnTemplate{}, nil
	}
	if stack == nil {
		return nil, nil, cfnErrf(ErrCFNStackNotFound, "cfn CreateChangeSet: stack %q not found", stackName)
	}
	if err := d.checkUpdatable(stack); err != nil {
		return nil, nil, err
	}
	oldTmpl, err := d.parseCFNTemplate(stack.TemplateBody)
	if err != nil {
		return nil, nil, cfnErrf(ErrCFNTemplateInvalid, "cfn CreateChangeSet: parse old template: %w", err)
	}
	return stack, oldTmpl, nil
}

// cfnSettleChangeSet sets a new change set's Status, StatusReason and ExecutionStatus.
//
// An UPDATE change set is empty when it changes no resource, no Output and no stack tag. The
// resource diff is [diffCFNResources]'s; Outputs are compared as declared, because a change set
// whose only difference is an Output is one CloudFormation executes (the stack's outputs change);
// and a Tags parameter that differs from the stack's own changes every resource the tags
// propagate to. A parameter whose new value reaches no resource is not a change, as AWS's own
// answers on re:Post describe ("not just changes to parameter default values").
func cfnSettleChangeSet(cs *CFNChangeSet, stack *CFNStackState, oldTmpl, newTmpl *cfnTemplate) {
	cs.ExecutionStatus = cfnChangeSetAvailable
	if cs.Type == CFNChangeSetTypeCreate || len(cs.Changes) > 0 {
		return
	}
	if !cfnSameJSON(oldTmpl.Outputs, newTmpl.Outputs) {
		return
	}
	if cs.Tags != nil && !maps.Equal(cs.Tags, stack.Tags) {
		return
	}
	cs.Status = cfnChangeSetFailed
	cs.StatusReason = cfnEmptyChangeSetReason
	cs.ExecutionStatus = cfnChangeSetUnavailable
}

// cfnReconcileActivity makes a change set's changes agree with which resources each template's
// conditions make active, since only an active resource is deployed. [diffCFNResources] compares
// declarations and so cannot see a Condition turning on or off.
//
//   - An Add of a resource whose Condition is false is dropped: executing the change set does not
//     create it. The CDK bootstrap template is the case that showed it — its customer-managed KMS
//     key and permissions boundary are conditional, and a change set listing them told the CDK to
//     expect fifteen resources where twelve deploy.
//   - A Remove or Modify of a resource that was not active is dropped, and so is a Modify of one that
//     is no longer active: neither describes a deployed resource.
//   - A resource declared in both templates whose activity flips is an Add (now active) or a Remove
//     (no longer active, with its physical ID), which is what executing the change set does to it.
//     Without this, an update whose only effect was a Condition flipping read as empty and FAILED.
func cfnReconcileActivity(changes []CFNResourceChange, oldTmpl, newTmpl *cfnTemplate,
	oldActive, newActive map[string]cfnResource, physical map[string]string,
) []CFNResourceChange {
	out := make([]CFNResourceChange, 0, len(changes))
	listed := map[string]bool{}
	for _, c := range changes {
		_, wasActive := oldActive[c.LogicalID]
		_, isActive := newActive[c.LogicalID]
		switch c.Action {
		case "Add":
			if !isActive {
				continue
			}
		case "Remove":
			if !wasActive {
				continue
			}
		default:
			if !wasActive || !isActive {
				continue
			}
		}
		listed[c.Action+"/"+c.LogicalID] = true
		out = append(out, c)
	}
	ids := make([]string, 0, len(newTmpl.Resources))
	for id := range newTmpl.Resources {
		if _, both := oldTmpl.Resources[id]; both {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		_, wasActive := oldActive[id]
		_, isActive := newActive[id]
		switch {
		case isActive && !wasActive && !listed["Add/"+id]:
			out = append(out, CFNResourceChange{Action: "Add", LogicalID: id, ResourceType: newTmpl.Resources[id].Type})
		case wasActive && !isActive && !listed["Remove/"+id]:
			out = append(out, CFNResourceChange{Action: "Remove", LogicalID: id,
				ResourceType: oldTmpl.Resources[id].Type, PhysicalID: physical[id]})
		}
	}
	return out
}

// cfnSameJSON reports whether two values marshal identically. Go's encoder sorts map keys, so two
// declarations differing only in key order compare equal; a marshal failure compares unequal.
func cfnSameJSON(a, b interface{}) bool {
	aj, aErr := json.Marshal(a)
	bj, bErr := json.Marshal(b)
	return aErr == nil && bErr == nil && string(aj) == string(bj)
}

// ensureReviewStack persists the REVIEW_IN_PROGRESS stack a CREATE change set creates, when the
// stack does not exist yet. It is written after the change set's template has been validated, so a
// refused CreateChangeSet leaves no stack behind.
func (d *StackDeployer) ensureReviewStack(ctx context.Context, stack *CFNStackState, cs *CFNChangeSet) error {
	if cs.Type != CFNChangeSetTypeCreate || stack.Status != "" {
		return nil
	}
	now := d.tc.Now()
	stack.AccountID = d.identity.accountID
	stack.Region = d.identity.region
	stack.Status = cfnStackReviewInProgress
	stack.CreatedAt = now
	stack.ReviewedAt = now
	stack.setAttribution(d.attribution)
	if err := d.persistStack(ctx, *stack); err != nil {
		return fmt.Errorf("cfn CreateChangeSet: %w", err)
	}
	return nil
}

// cfnChangeSetExecutionStatus returns a change set's ExecutionStatus. A record written before
// #1606 carries none, and was AVAILABLE if it had been created, which every such record had.
func cfnChangeSetExecutionStatus(cs *CFNChangeSet) string {
	if cs.ExecutionStatus != "" {
		return cs.ExecutionStatus
	}
	if cs.Status == cfnChangeSetCreateComplete {
		return cfnChangeSetAvailable
	}
	return cfnChangeSetUnavailable
}

// ExecuteChangeSetWithOptions is [StackDeployer.ExecuteChangeSet] with ExecuteChangeSet's
// DisableRollback.
//
// A CREATE change set is executed by [StackDeployer.DeployWithOptions] against its
// REVIEW_IN_PROGRESS stack, so a failed create rolls back, does nothing or deletes the stack as
// OnStackFailure or DisableRollback say, exactly as a CreateStack does. An UPDATE change set is
// executed by [StackDeployer.UpdateStack]. Each applies the change set's own tags (#824): the
// warrant is DescribeChangeSet's description of the member — "if you execute the change set, the
// tags that will be associated with the stack".
func (d *StackDeployer) ExecuteChangeSetWithOptions(ctx context.Context, stackName, changeSetName string,
	opts CFNExecuteOptions,
) (*DeployResult, error) {
	cs, err := d.DescribeChangeSet(ctx, stackName, changeSetName)
	if err != nil {
		return nil, err
	}
	if status := cfnChangeSetExecutionStatus(cs); status != cfnChangeSetAvailable {
		return nil, cfnErrf(ErrCFNChangeSetNotExecutable,
			"ChangeSet [%s] cannot be executed in its current execution status of [%s]", changeSetName, status)
	}

	var result *DeployResult
	if cs.Type == CFNChangeSetTypeCreate {
		result, err = d.executeCreateChangeSet(ctx, cs, opts)
	} else {
		result, err = d.UpdateStack(ctx, cs.TemplateBody, stackName, cs.Parameters,
			CFNDeployOptions{Tags: cs.Tags})
	}
	if err != nil {
		return nil, err
	}
	if err := d.settleExecution(ctx, cs, result); err != nil {
		return nil, err
	}
	return result, nil
}

// executeCreateChangeSet deploys a CREATE change set's template into its REVIEW_IN_PROGRESS stack.
func (d *StackDeployer) executeCreateChangeSet(ctx context.Context, cs *CFNChangeSet,
	opts CFNExecuteOptions,
) (*DeployResult, error) {
	review, err := d.loadStack(ctx, cs.StackName)
	if err != nil {
		return nil, err
	}
	if review == nil || review.Status != cfnStackReviewInProgress {
		// Unreachable through the wire, since every operation that moves a stack out of
		// REVIEW_IN_PROGRESS also removes or settles its change sets; an in-process caller holding a
		// stale record could still ask. The message is substrate's own.
		return nil, cfnErrf(ErrCFNChangeSetNotExecutable,
			"ChangeSet [%s] cannot be executed: stack [%s] is not in REVIEW_IN_PROGRESS", cs.ChangeSetName, cs.StackName)
	}
	disableRollback, present := "", opts.DisableRollback != nil
	if present {
		disableRollback = strconv.FormatBool(*opts.DisableRollback)
	}
	deployOpts, err := cfnResolveFailureOptions(cs.OnStackFailure, disableRollback, present)
	if err != nil {
		return nil, err
	}
	deployOpts.Tags = cs.Tags
	// The deploy carries the stack's REVIEW_IN_PROGRESS start onto the record it writes
	// ([cfnCarriedCreation]), so its CreationTime and its first event stay where the change set
	// put them, on success and on a failed create alike.
	return d.DeployWithOptions(ctx, cs.TemplateBody, cs.StackName, cs.Parameters, deployOpts)
}

// settleExecution records how an executed change set ended and deletes the stack's other change
// sets. A stack whose failed create was deleted (OnStackFailure DELETE) keeps none: "Deletes the
// change set if the stack creation fails".
func (d *StackDeployer) settleExecution(ctx context.Context, cs *CFNChangeSet, result *DeployResult) error {
	stack, err := d.loadStack(ctx, cs.StackName)
	if err != nil {
		return err
	}
	if stack == nil {
		return d.deleteStackChangeSets(ctx, cs.StackName, "")
	}
	cs.ExecutionStatus = cfnChangeSetExecuteFailed
	if result != nil && (result.Status == cfnStackCreateComplete || result.Status == cfnStackUpdateComplete) {
		cs.ExecutionStatus = cfnChangeSetExecuteComplete
	}
	data, err := json.Marshal(cs)
	if err != nil {
		return fmt.Errorf("cfn ExecuteChangeSet: marshal: %w", err)
	}
	if err := d.state.Put(ctx, cfnNamespace, d.changeSetKey(cs.StackName, cs.ChangeSetName), data); err != nil {
		return fmt.Errorf("cfn ExecuteChangeSet: put: %w", err)
	}
	return d.deleteStackChangeSets(ctx, cs.StackName, cs.ChangeSetName)
}

// deleteStackChangeSets deletes every change set of a stack except keep, which may be empty.
func (d *StackDeployer) deleteStackChangeSets(ctx context.Context, stackName, keep string) error {
	names, err := d.loadChangeSetNames(ctx, stackName)
	if err != nil {
		return fmt.Errorf("cfn delete change sets of %s: %w", stackName, err)
	}
	for _, name := range names {
		if name == keep {
			continue
		}
		if err := d.DeleteChangeSet(ctx, stackName, name); err != nil {
			return err
		}
	}
	return nil
}

// cfnCarriedCreation returns the CreatedAt and ReviewedAt a deployment's record carries: the
// previous record's, so an update does not move them, except that a stack still in
// REVIEW_IN_PROGRESS is created by this deployment, at start.
func cfnCarriedCreation(prev *CFNStackState, start time.Time) (createdAt, reviewedAt time.Time) {
	if prev == nil {
		return start, time.Time{}
	}
	createdAt = prev.CreatedAt
	if prev.Status == cfnStackReviewInProgress || createdAt.IsZero() {
		createdAt = start
	}
	return createdAt, prev.ReviewedAt
}

// creationTime is the stack's CreationTime: when a CREATE change set created it in
// REVIEW_IN_PROGRESS, or else when it was first deployed.
func (s CFNStackState) creationTime() time.Time {
	if !s.ReviewedAt.IsZero() {
		return s.ReviewedAt
	}
	return s.CreatedAt
}
