package emulator

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// cfn_update.go decides what an UpdateStack does with each resource (#1427).
//
// An update used to re-deploy the whole template: every resource's create was issued again,
// whatever had changed. A type whose create mints its identifier — a Transfer server, an FSx file
// system, a KMS key, a certificate, an internet gateway — came out of every update as a second
// resource with a new ID, and the first one stayed behind, no longer named by the stack. A type
// whose create refuses a duplicate name answered an already-exists refusal, which the deployer
// cleared when the declaration was unchanged (clearUnchangedRedeploys, retired here).
//
// CloudFormation's documented behavior is per resource, and each case below is the published one:
//
//   - **Unchanged.** "When you initiate a stack update, CloudFormation updates resources based on
//     differences between what you submit and the stack's current template and parameters" (the
//     UpdateReplacePolicy attribute page). A resource whose declaration and inputs are unchanged
//     keeps its physical resource, and no call is made for it.
//   - **Changed, updatable in place.** The resource's own "Update requires" lines say which
//     properties are No interruption or Some interruptions. Such a change goes through the
//     service's update operation and the physical ID stands.
//   - **Changed, Update requires: Replacement.** "Recreating the resource generates a new physical
//     ID. CloudFormation creates the replacement resource first, and then changes references from
//     other dependent resources to point to the replacement resource. By default, CloudFormation
//     then deletes the old resource." The troubleshooting guide says when: "if a resource is
//     replaced, CloudFormation creates new resource before it deletes the old one", and the old one
//     is deleted only once the update has succeeded — a failed update rolls back onto it.
//
// **"Unchanged" is decided on resolved values, not on the template text.** Both declarations are
// resolved — the previous one against the previous deployment's parameters and resources, the new
// one against this deployment's — and compared property by property. That is what makes the rule
// generic: a resource whose text is unchanged but which Refs a parameter whose value changed, or a
// sibling that was replaced and so has a new ID, resolves differently and is updated; a resource
// whose dependencies all kept their identities resolves identically and is left alone. The deploy
// order is the dependency order (#1420), so every dependency has been decided by the time a
// resource is compared. Comparing the text, as clearUnchangedRedeploys did, could not tell those
// cases apart, which is why it had to tolerate a refusal rather than avoid the call.
//
// **What substrate does with a changed resource depends on what it models.** A type with an entry
// in [cfnResourceUpdaters] is updated through its service's own update operation, unless a changed
// property requires replacement by [cfnRecreationProperties]. A type in [cfnUpdateUnmodelled] has
// updatable properties whose update operation substrate does not route, so a change to one fails
// the resource with a reason saying so rather than replacing a resource AWS would have kept. Every
// other type is re-created, which is the pre-#1427 behavior, and when the re-create yields a new
// physical ID the old resource is now deleted after the update succeeds, as a replacement is.

// cfnUpdateFunc updates one resource in place: prior is what the stack recorded for it, and the
// returned DeployedResource replaces that record. A refusal is reported on the resource, exactly as
// a create's is.
type cfnUpdateFunc func(
	d *StackDeployer, ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error)

// cfnResourceUpdaters are the types whose changed-but-updatable declaration is applied through the
// service's own update operation.
//
// The set is the types this change needed, not every type with an update operation in substrate:
// a type absent here keeps the re-create behavior, which is listed as a follow-up rather than
// changed silently.
var cfnResourceUpdaters = map[string]cfnUpdateFunc{
	"AWS::Transfer::Server":    (*StackDeployer).updateTransferServer,
	"AWS::Backup::BackupPlan":  (*StackDeployer).updateBackupBackupPlan,
	"AWS::EC2::LaunchTemplate": (*StackDeployer).updateEC2LaunchTemplate,
}

// cfnUpdateUnmodelled maps a type to why a change to one of its updatable properties cannot be
// applied: AWS updates the property in place, and substrate does not route the operation that would.
//
// Failing the resource is deliberate. Re-creating it would answer a new physical ID for a change
// AWS makes in place — a plausible-looking wrong answer — and an MSK cluster's re-create is refused
// anyway, because its name is taken by the cluster being updated. A change to a property whose
// "Update requires" is Replacement still replaces the resource.
var cfnUpdateUnmodelled = map[string]string{
	"AWS::FSx::FileSystem": "FSx updates this property in place through UpdateFileSystem, which " +
		"substrate does not route",
	"AWS::MSK::Cluster": "MSK updates this property in place through its Update* cluster " +
		"operations, which substrate does not route",
}

// cfnBaseline is the deployment an update is compared against: the stack's previous record, the
// template it was deployed from, and a resolution context seeded with what it deployed.
type cfnBaseline struct {
	record    *CFNStackState
	tmpl      *cfnTemplate
	cctx      *cfnContext
	resources map[string]DeployedResource

	// force names resources to apply even though their resolved declaration is unchanged. A rollback
	// sets it for the resources the failed update changed in place: the rollback's template is the
	// baseline's own, so nothing compares as changed, and yet the live resource carries the failed
	// update's values until it is applied again.
	force map[string]bool
}

// newCFNBaseline builds the baseline for record, or nil when there is nothing to compare against —
// no record, no template, or a template that no longer parses. A nil baseline makes every resource
// a create, which is what a first deployment is.
func (d *StackDeployer) newCFNBaseline(record *CFNStackState, exports map[string]string, force map[string]bool) *cfnBaseline {
	if record == nil || record.TemplateBody == "" {
		return nil
	}
	tmpl, err := d.parseCFNTemplate(record.TemplateBody)
	if err != nil {
		d.logger.Warn("cfn: previous template does not parse; every resource is redeployed",
			"stack", record.StackName, "error", err.Error())
		return nil
	}
	cctx := buildCFNContext(tmpl, record.Parameters, d.identity.region, d.identity.accountID, record.StackName)
	evaluateConditions(tmpl, cctx)
	cctx.exports = exports
	b := &cfnBaseline{record: record, tmpl: tmpl, cctx: cctx,
		resources: make(map[string]DeployedResource, len(record.Resources)), force: force}
	for _, r := range record.Resources {
		cctx.resources[r.LogicalID] = r
		if r.Error == "" {
			b.resources[r.LogicalID] = r
		}
	}
	return b
}

// cfnResourcePlan is what a deployment does with one resource.
type cfnResourcePlan struct {
	// keep is set when the resource is unchanged: prior is reused and no call is made.
	keep bool

	// prior is the previous deployment's record of the resource, valid when hasPrior is set.
	prior     DeployedResource
	priorDecl cfnResource
	hasPrior  bool

	// changed names the top-level properties whose resolved values differ, sorted.
	changed []string

	// update is the in-place update to apply, or nil to create.
	update cfnUpdateFunc

	// refusal fails the resource without a call, for a change substrate cannot apply.
	refusal string
}

// plan decides what this deployment does with one resource.
func (b *cfnBaseline) plan(logicalID string, res cfnResource, cctx *cfnContext) cfnResourcePlan {
	if b == nil {
		return cfnResourcePlan{}
	}
	prior, ok := b.resources[logicalID]
	if !ok {
		return cfnResourcePlan{}
	}
	priorDecl, ok := b.tmpl.Resources[logicalID]
	if !ok {
		return cfnResourcePlan{}
	}
	p := cfnResourcePlan{prior: prior, priorDecl: priorDecl, hasPrior: true}
	if priorDecl.Type != res.Type {
		// A changed Type is a different resource: the new one is created and the old one
		// replaced, which is how a change set reports it (Remove and Add).
		return p
	}
	changed, resolved := b.changedProperties(priorDecl, res, cctx)
	if !resolved {
		// The new declaration does not resolve. Dispatching is what reports why.
		return p
	}
	if len(changed) == 0 && !b.force[logicalID] {
		p.keep = true
		return p
	}
	if len(changed) == 0 {
		// Forced: the declaration matches the baseline, and the live resource does not, so
		// every declared property is applied again. Replacement is still decided on them, but
		// none of them differs from the baseline, so a forced resource is never replaced.
		for k := range res.Properties {
			changed = append(changed, k)
		}
		sort.Strings(changed)
		p.changed = changed
		if up, ok := cfnResourceUpdaters[res.Type]; ok {
			p.update = up
		}
		return p
	}
	p.changed = changed
	if cfnRequiresReplacement(res.Type, changed) {
		return p
	}
	if up, ok := cfnResourceUpdaters[res.Type]; ok {
		p.update = up
		return p
	}
	if reason, ok := cfnUpdateUnmodelled[res.Type]; ok {
		p.refusal = fmt.Sprintf("updating %s of %s is not modeled: %s",
			strings.Join(changed, ", "), res.Type, reason)
	}
	return p
}

// changedProperties resolves both declarations and returns the top-level properties whose values
// differ, sorted. The second result is false when the new declaration recorded a resolution
// failure; the failure is drained, since the deploy that follows resolves it again and reports it.
func (b *cfnBaseline) changedProperties(before, after cfnResource, cctx *cfnContext) ([]string, bool) {
	old, _ := resolveNested(before.Properties, b.cctx).(map[string]interface{})
	b.cctx.takeFailures()
	cctx.takeFailures()
	updated, _ := resolveNested(after.Properties, cctx).(map[string]interface{})
	if len(cctx.takeFailures()) > 0 {
		return nil, false
	}
	keys := map[string]bool{}
	for k := range old {
		keys[k] = true
	}
	for k := range updated {
		keys[k] = true
	}
	var changed []string
	for k := range keys {
		if !cfnTemplateValuesEqual(old[k], updated[k]) {
			changed = append(changed, k)
		}
	}
	sort.Strings(changed)
	return changed, true
}

// replacedBy reports whether dr, just deployed under this plan, replaced the previous resource:
// the previous one deployed, the new one did too, and they are not the same resource — a changed
// type, or a different physical ID.
//
// A type whose only trace is a cfnStubNamespace record keyed by logical ID is never reported:
// deleting "the old one" would delete the record the new one just wrote.
func (p cfnResourcePlan) replacedBy(dr DeployedResource) bool {
	if !p.hasPrior || dr.Error != "" || !cfnHasRealDelete(p.prior.Type) {
		return false
	}
	if p.prior.Type != dr.Type {
		return true
	}
	return dr.PhysicalID != "" && p.prior.PhysicalID != "" && dr.PhysicalID != p.prior.PhysicalID
}

// cfnHasRealDelete reports whether a sweep deletes resType through a request, rather than by
// removing a stub record or by doing nothing.
func cfnHasRealDelete(resType string) bool {
	if cfnStubDeleteTypes[resType] {
		return false
	}
	if _, ok := cfnSequencedDeleters[resType]; ok {
		return true
	}
	_, ok := cfnResourceDeleters[resType]
	return ok
}

// cfnRequiresReplacement reports whether any changed property's "Update requires" is Replacement,
// by [cfnRecreationProperties]. A Conditionally entry does not count: the condition is the
// service's, and substrate treats the change as updatable.
func cfnRequiresReplacement(resType string, changed []string) bool {
	for _, prop := range changed {
		if cfnRequiresRecreation(resType, "Properties", prop) == "Always" {
			return true
		}
	}
	return false
}

// clearsRefusal reports whether a create's refusal only says the resource already exists, for a
// resource the previous deployment created from a declaration this template still spells the same
// way — in which case the refusal is an artifact of the re-create rather than a failure, and the
// previous record stands.
//
// This is what is left of clearUnchangedRedeploys. An unchanged resource is no longer re-created,
// so the refusal it cleared no longer happens for it; it can still happen for a resource whose
// text is unchanged but whose inputs changed (a sibling it references was replaced) and whose type
// has no update path, and for a resource a rollback re-applies. The three conditions it states are
// unchanged and each is still load-bearing: a previous record, a successful previous deployment of
// the slot, and an identical declaration — compared as text, so that a template edited to point
// the slot at a name another stack owns still fails.
func (p cfnResourcePlan) clearsRefusal(res cfnResource, dr DeployedResource) bool {
	return p.hasPrior && dr.Error != "" && cfnCreateIsExists(dr.Error) && cfnSameDeclaration(p.priorDecl, res)
}

// deleteReplaced deletes the resources an update replaced, once the update has succeeded, and
// reports what happened to each.
//
// Each is deleted with the declaration it was deployed from, in base's template, so a delete that
// needs its properties — an IAM policy's roles, an API Gateway child's parent — finds the values the
// old resource was created with; references resolve against base's resources. Its UpdateReplacePolicy
// governs: "By default, CloudFormation then deletes the old resource. Using the UpdateReplacePolicy,
// you can specify that CloudFormation retain" it.
//
// A delete that fails does not fail the update. The troubleshooting guide's "Resource removed from
// stack but not deleted": "CloudFormation removes the old resource from the stack and continues
// updating the stack. When the stack update is complete, CloudFormation issues an UPDATE_COMPLETE
// stack event, but includes a StatusReason that states that one or more resources couldn't be
// deleted." So the reason is reported on the result and the stack still completes.
func (d *StackDeployer) deleteReplaced(
	ctx context.Context, base *CFNStackState, replaced []DeployedResource, streamID string,
) ([]CFNResourceDeletion, string) {
	if base == nil || len(replaced) == 0 {
		return nil, ""
	}
	sweep := CFNStackState{StackName: base.StackName, TemplateBody: base.TemplateBody,
		Parameters: base.Parameters, Resources: replaced}
	deletions := d.sweepResources(ctx, &sweep, base.Resources, streamID, cfnUpdateReplaceOp)
	failed := cfnFailedDeletions(deletions)
	if len(failed) == 0 {
		return deletions, ""
	}
	d.logger.Warn("cfn: an update could not delete the resources it replaced",
		"stack", base.StackName, "undeleted", len(failed))
	return deletions, "the update could not delete the resources it replaced: " + strings.Join(failed, "; ")
}
