package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
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
//
// **A resource the template no longer deploys is deleted** once the update succeeds, under its
// DeletionPolicy, in the same sweep as the replaced ones (#1547; see [StackDeployer.deleteSuperseded]).

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
	// The types CDK templates most often change in place (#1545, #1550); see
	// cfnUpdateUnmodelledProperties.
	"AWS::Lambda::Function":      (*StackDeployer).updateLambdaFunction,
	"AWS::DynamoDB::Table":       (*StackDeployer).updateDynamoDBTable,
	"AWS::SQS::Queue":            (*StackDeployer).updateSQSQueue,
	"AWS::SNS::Topic":            (*StackDeployer).updateSNSTopic,
	"AWS::S3::Bucket":            (*StackDeployer).updateS3Bucket,
	"AWS::S3::BucketPolicy":      (*StackDeployer).updateS3BucketPolicy,
	"AWS::CloudWatch::Dashboard": (*StackDeployer).updateCloudWatchDashboard,
	"AWS::KMS::Key":              (*StackDeployer).updateKMSKey,
	"AWS::EC2::SecurityGroup":    (*StackDeployer).updateEC2SecurityGroup,
	"AWS::ECS::Service":          (*StackDeployer).updateECSService,
	// The IAM types (#1545, #1560, #1562); see cfnIAMRecord.
	"AWS::IAM::Role":          (*StackDeployer).updateIAMRole,
	"AWS::IAM::User":          (*StackDeployer).updateIAMUser,
	"AWS::IAM::Group":         (*StackDeployer).updateIAMGroup,
	"AWS::IAM::Policy":        (*StackDeployer).updateIAMPolicy,
	"AWS::IAM::ManagedPolicy": (*StackDeployer).updateIAMManagedPolicy,
	// The CDK's routing and queue-policy types (#1605).
	"AWS::EC2::Route":                       (*StackDeployer).updateEC2Route,
	"AWS::EC2::SubnetRouteTableAssociation": (*StackDeployer).updateEC2SubnetRouteTableAssociation,
	"AWS::SQS::QueuePolicy":                 (*StackDeployer).updateQueuePolicy,
	// #1613.
	"AWS::Events::EventBus": (*StackDeployer).updateEventsEventBus,
	"AWS::Events::Rule":     (*StackDeployer).updateEventsRule,
	// An alias the CDK points at a new currentVersion on each code change (#1613).
	"AWS::Lambda::Alias": (*StackDeployer).updateLambdaAlias,
	// The identity providers (#1613); their Url and Name are Replacement.
	"AWS::IAM::OIDCProvider": (*StackDeployer).updateIAMOIDCProvider,
	"AWS::IAM::SAMLProvider": (*StackDeployer).updateIAMSAMLProvider,
	// Cloud Map (#1614): Description and a DNS namespace's Properties, and a service's Description,
	// DnsConfig and HealthCheckConfig, are No interruption on their pages.
	"AWS::ServiceDiscovery::HttpNamespace":       cfnUpdateServiceDiscoveryNamespace("AWS::ServiceDiscovery::HttpNamespace"),
	"AWS::ServiceDiscovery::PrivateDnsNamespace": cfnUpdateServiceDiscoveryNamespace("AWS::ServiceDiscovery::PrivateDnsNamespace"),
	"AWS::ServiceDiscovery::PublicDnsNamespace":  cfnUpdateServiceDiscoveryNamespace("AWS::ServiceDiscovery::PublicDnsNamespace"),
	"AWS::ServiceDiscovery::Service":             (*StackDeployer).updateServiceDiscoveryService,
}

// cfnUpdateUnmodelled maps a type to why a change to one of its updatable properties cannot be
// applied: AWS updates the property in place, and substrate does not route the operation that would.
// It covers every updatable property of a type with no updater; cfnUpdateUnmodelledProperties
// names the individual properties an updater's type cannot apply.
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

// cfnCreateOnlyProperties are properties a resource page says an update cannot change at all —
// neither in place nor by replacement — keyed by type, with the page's reason. A change to one fails
// the resource without a call and the update rolls back, which leaves the resource as it was.
//
// AWS::Transfer::Server's CreateWithHostKey is the one: "You can set this property only when you
// create the server. If you change its value in a later stack update, the update fails and the
// server is left unchanged." The message is substrate's own; the page publishes the behavior, not
// the text.
//
// AWS::KMS::Key's KeySpec, KeyUsage and MultiRegion are Update requires: Replacement on the page,
// and the page then says the replacement never happens: "If you change the value of the KeySpec
// property on an existing KMS key, the update request fails, regardless of the value of the
// UpdateReplacePolicy attribute. This prevents you from accidentally deleting a KMS key by changing
// an immutable property value", and the same of the other two. Origin is No interruption, and "You
// cannot change the origin after you create the KMS key" is its page's sentence.
var cfnCreateOnlyProperties = map[string]map[string]string{
	"AWS::Transfer::Server": {
		"CreateWithHostKey": "it can be set only when the server is created",
	},
	"AWS::KMS::Key": {
		"KeySpec":     "a key's key spec cannot change, and CloudFormation does not replace a key for it",
		"KeyUsage":    "a key's key usage cannot change, and CloudFormation does not replace a key for it",
		"MultiRegion": "a key's multi-Region setting cannot change, and CloudFormation does not replace a key for it",
		"Origin":      "a key's origin cannot change after the key is created",
	},
}

// cfnBaseline is the deployment an update is compared against: the stack's previous record, the
// template it was deployed from, and a resolution context seeded with what it deployed.
type cfnBaseline struct {
	record    *CFNStackState
	tmpl      *cfnTemplate
	cctx      *cfnContext
	resources map[string]DeployedResource

	// force names resources to apply even though their resolved declaration is unchanged, each with
	// the failed update's declaration of it, resolved. A rollback sets it for the resources the
	// failed update changed in place: the rollback's template is the baseline's own, so nothing
	// compares as changed against the baseline, and yet the live resource carries the failed
	// update's values until it is applied again. What is applied is the difference between the two
	// declarations, so a property the failed update newly declared is reset too.
	force map[string]map[string]interface{}
}

// newCFNBaseline builds the baseline for record, or nil when there is nothing to compare against —
// no record, no template, or a template that no longer parses. A nil baseline makes every resource
// a create, which is what a first deployment is.
func (d *StackDeployer) newCFNBaseline(
	record *CFNStackState, exports map[string]string, force map[string]map[string]interface{},
) *cfnBaseline {
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
		if cfnRecordHoldsResource(record, r) {
			b.resources[r.LogicalID] = r
		}
	}
	return b
}

// cfnRecordHoldsResource reports whether a stack record's resource still exists as the stack's:
// it deployed, and no sweep since has deleted it or put it out of the stack's scope.
//
// A record keeps its resource list after a sweep — a ROLLBACK_COMPLETE or DELETE_FAILED stack
// still names everything it created, so DescribeStackResources can report what happened — and
// reading that list as live would make the next deployment "keep" resources that are gone, under
// phantom IDs. So:
//
//   - A ROLLBACK_COMPLETE record holds nothing: its rollback deleted every resource the create
//     made, or retained it outside the stack's scope ("Any resources that were created during
//     the create stack operation are deleted").
//   - Otherwise a resource the record's ResourceDeletions shows as deleted or skipped is gone
//     from the stack: DELETE_COMPLETE removed it, and DELETE_SKIPPED either never had it or
//     retained it, which "removes it from the stack's scope". One whose delete failed is still
//     there. The deletion is matched by physical ID as well as logical ID (see
//     [cfnDeletionFor]), so the deleted predecessor of a replaced resource does not hide the
//     resource that replaced it.
func cfnRecordHoldsResource(record *CFNStackState, r DeployedResource) bool {
	if r.Error != "" || record.Status == cfnStackRollbackComplete {
		return false
	}
	if del := cfnDeletionFor(record.ResourceDeletions, r); del != nil && del.Status != cfnDeleteFailed {
		return false
	}
	return true
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

	// resolvedSame is set when the declaration resolves to exactly the values the previous
	// deployment's did, which for a resource that is nonetheless dispatched means a rollback is
	// applying it again.
	resolvedSame bool

	// replacement is set when a changed property's "Update requires" is Replacement, so the
	// create that follows makes a new resource beside the old one and the old one is deleted once
	// the update succeeds. Such a resource is given a fresh generated name (#1545).
	replacement bool

	// priorResolved is the previous declaration's properties resolved against the previous
	// deployment, which an updater reads through [cfnContext.priorProps].
	priorResolved map[string]interface{}
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
	// The previous declaration in the structured form an updater reads, where an Fn::If choosing
	// an object or AWS::NoValue has been taken — resolveNested would render such a branch as a
	// string.
	p.priorResolved, _ = cfnResolveStructured(priorDecl.Properties, b.cctx).(map[string]interface{})
	b.cctx.takeFailures()
	if !resolved {
		// The new declaration does not resolve. Dispatching is what reports why.
		return p
	}
	failed, forced := b.force[logicalID]
	if len(changed) == 0 && !forced {
		p.keep = true
		return p
	}
	if len(changed) == 0 {
		// Forced: the declaration matches the baseline, and the live resource does not. What
		// differs is the failed update's declaration from this one — in both directions, so a
		// property the failed update added is in the set and, absent here, is reset by the
		// updater's rule for a removed property. The failed declaration is the "previous" one
		// the updater diffs against. None of it differs from the baseline, so a forced resource
		// is never replaced; and when nothing differs, the failed update left it as it was.
		p.resolvedSame = true
		p.priorResolved = failed
		updated, _ := cfnResolveStructured(res.Properties, cctx).(map[string]interface{})
		cctx.takeFailures()
		p.changed = cfnDiffProperties(failed, updated)
		if len(p.changed) == 0 {
			p.keep = true
			return p
		}
		if up, ok := cfnResourceUpdaters[res.Type]; ok {
			p.update = up
		}
		return p
	}
	p.changed = changed
	for _, prop := range changed {
		if reason, ok := cfnCreateOnlyProperties[res.Type][prop]; ok {
			p.refusal = fmt.Sprintf("%s of %s cannot be updated: %s", prop, res.Type, reason)
			return p
		}
	}
	if cfnRequiresReplacement(res.Type, changed) {
		p.replacement = true
		return p
	}
	var unmodelled []string
	for _, prop := range changed {
		if reason, ok := cfnUpdateUnmodelledProperties[res.Type][prop]; ok {
			unmodelled = append(unmodelled, prop+" ("+reason+")")
		}
	}
	if len(unmodelled) > 0 {
		p.refusal = fmt.Sprintf("updating %s of %s is not modeled: %s",
			strings.Join(changed, ", "), res.Type, strings.Join(unmodelled, "; "))
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
	return cfnDiffProperties(old, updated), true
}

// cfnDiffProperties returns, sorted, the top-level properties whose values differ between two
// resolved declarations, a property present in only one of them included.
func cfnDiffProperties(old, updated map[string]interface{}) []string {
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
	return changed
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
// resource the previous deployment created from a declaration that resolves exactly as this one
// does — in which case the refusal is an artifact of the re-create rather than a failure, and the
// previous record stands.
//
// This is what is left of clearUnchangedRedeploys. An unchanged resource is no longer re-created,
// so the only resource that reaches here with identical resolved inputs is one a rollback applies
// again (see [cfnBaseline.force]). A resource whose text is unchanged but whose *resolved* inputs
// changed — it references a sibling that was replaced — is not cleared: keeping the previous record
// would leave it naming the sibling the update is about to delete, so the refusal fails the
// resource and the update rolls back instead. The text comparison stays as well, so a template
// edited to point the slot at a name another stack owns still fails.
func (p cfnResourcePlan) clearsRefusal(res cfnResource, dr DeployedResource) bool {
	return p.hasPrior && p.resolvedSame && dr.Error != "" && cfnCreateIsExists(dr.Error) &&
		cfnSameDeclaration(p.priorDecl, res)
}

// deleteSuperseded deletes the resources a successful update no longer holds — the ones it replaced
// and the ones its template no longer declares — and reports what happened to each.
//
// Both go in one sweep, in the reverse of the previous template's dependency order, because a
// removed resource may depend on a replaced one or the other way round, and two sweeps would order
// each set only against itself (#1547).
//
// Each is deleted with the declaration it was deployed from, in base's template, so a delete that
// needs its properties — an IAM policy's roles, an API Gateway child's parent — finds the values the
// old resource was created with; references resolve against base's resources. The policy differs
// by why the resource is going:
//
//   - **Replaced**: its UpdateReplacePolicy. "By default, CloudFormation then deletes the old
//     resource. Using the UpdateReplacePolicy, you can specify that CloudFormation retain" it.
//   - **Removed from the template**, or declared under a Condition that is now false: its
//     DeletionPolicy. The UpdateReplacePolicy page draws the line — DeletionPolicy is what applies
//     "when the resource definition itself is deleted from the template as part of a stack update"
//     — and the troubleshooting guide's "Resource removed from stack but not deleted" describes the
//     same path: "You have removed the resource from the stack template, so CloudFormation attempts
//     to delete the resource from the stack". The op is the DeleteStack one, so
//     RetainExceptOnCreate retains: the operation removing the resource is not the one that
//     created it.
//
// A delete that fails does not fail the update. The same troubleshooting entry: "CloudFormation
// removes the old resource from the stack and continues updating the stack. When the stack update
// is complete, CloudFormation issues an UPDATE_COMPLETE stack event, but includes a StatusReason
// that states that one or more resources couldn't be deleted." So the reason is reported on the
// result and the stack still completes.
//
// A failed update reaches none of this: it rolls back onto the previous record, which still holds
// every resource this would have deleted.
func (d *StackDeployer) deleteSuperseded(
	ctx context.Context, base *CFNStackState, replaced, removed []DeployedResource, streamID string,
) ([]CFNResourceDeletion, string) {
	if base == nil || len(replaced)+len(removed) == 0 {
		return nil, ""
	}
	removedIDs := make(map[string]bool, len(removed))
	for _, r := range removed {
		removedIDs[r.LogicalID] = true
	}
	all := make([]DeployedResource, 0, len(replaced)+len(removed))
	all = append(all, replaced...)
	all = append(all, removed...)
	sweep := CFNStackState{StackName: base.StackName, TemplateBody: base.TemplateBody,
		Parameters: base.Parameters, Resources: all}
	deletions := d.sweepResourcesByOp(ctx, &sweep, base.Resources, streamID,
		func(dr DeployedResource) cfnDeletionOp {
			if removedIDs[dr.LogicalID] {
				return cfnDeleteStackOp
			}
			return cfnUpdateReplaceOp
		})
	var failedReplaced, failedRemoved []CFNResourceDeletion
	for _, del := range deletions {
		if del.Status != cfnDeleteFailed {
			continue
		}
		if removedIDs[del.LogicalID] {
			failedRemoved = append(failedRemoved, del)
		} else {
			failedReplaced = append(failedReplaced, del)
		}
	}
	var reasons []string
	if failed := cfnFailedDeletions(failedReplaced); len(failed) > 0 {
		reasons = append(reasons, "the update could not delete the resources it replaced: "+
			strings.Join(failed, "; "))
	}
	if failed := cfnFailedDeletions(failedRemoved); len(failed) > 0 {
		reasons = append(reasons, "the update could not delete the resources removed from the template: "+
			strings.Join(failed, "; "))
	}
	if len(reasons) == 0 {
		return deletions, ""
	}
	d.logger.Warn("cfn: an update could not delete the resources it no longer holds",
		"stack", base.StackName, "undeleted", len(failedReplaced)+len(failedRemoved))
	return deletions, strings.Join(reasons, "; ")
}

// cfnRemovedResources returns the resources base holds that tmpl no longer deploys: a logical ID
// the template does not declare, or declares under a Condition that is now false (#1547). They are
// in base's order, so the sweep's tie-break is stable.
//
// A resource whose logical ID is still active is never here, whatever its type: a changed type is
// a replacement, which [cfnResourcePlan.replacedBy] reports.
func cfnRemovedResources(baseline *cfnBaseline, tmpl *cfnTemplate, cctx *cfnContext) []DeployedResource {
	if baseline == nil {
		return nil
	}
	active := cfnActiveResources(tmpl, cctx)
	var removed []DeployedResource
	for _, r := range baseline.record.Resources {
		held, ok := baseline.resources[r.LogicalID]
		if !ok || held.PhysicalID != r.PhysicalID || held.Type != r.Type {
			continue
		}
		if _, still := active[r.LogicalID]; still {
			continue
		}
		removed = append(removed, r)
	}
	return removed
}

// The in-place updates cfnResourceUpdaters names for AWS::Lambda::Function, AWS::DynamoDB::Table,
// AWS::SQS::Queue, AWS::SNS::Topic, AWS::S3::Bucket, AWS::KMS::Key, AWS::EC2::SecurityGroup and
// AWS::ECS::Service (#1545, #1550) live with each type's deploy helper, beside the create-side
// configuration each shares with its update.
//
// Three rules hold for every one of those updaters, so each doc comment states only its exceptions:
//
//   - **What the update applies is what the create applies.** An updater sends the properties its
//     type's deploy helper forwards, through the service's own update operation; a property the
//     create does not forward either (most types' Tags, a Lambda function's VpcConfig, a queue's
//     RedrivePolicy) is left alone and makes no call, which is what re-creating the resource used
//     to do with it too. A property the create forwards and substrate has no routed operation to
//     update is in cfnUpdateUnmodelledProperties instead, and a change to it fails the resource.
//   - **A property removed from the template resets** where the resource page publishes the value
//     an undeclared property has — SQS's attribute defaults, a Lambda function's Timeout and
//     MemorySize — or where the create itself sends a value for an undeclared property, and the
//     service's API can express it. Otherwise the removal is left as it is and the updater's doc
//     comment says so.
//   - **The physical ID stands**: an updater returns the prior record, so a Ref to the resource
//     resolves as it did, and a refusal is reported on the resource exactly as a create's is.
//
// Template-level Tags are not applied by any of these creates, and so not by their updates. The
// stack's own tags and CloudFormation's three aws:cloudformation:* tags are written on the same
// records, so applying a template's Tags through a replace-the-set call (S3's PutBucketTagging)
// would drop them; that reconciliation is its own change.

// cfnUpdateUnmodelledProperties maps a type that has an updater to the properties its create
// forwards and its update cannot apply, each with the reason. A change to one fails the resource
// with the reason and the update rolls back, rather than leaving a resource whose live
// configuration no longer matches its template. The reasons are substrate's own.
var cfnUpdateUnmodelledProperties = map[string]map[string]string{
	// The DynamoDB page gives KeySchema "Some interruptions" and the index and stream properties
	// "No interruption"; substrate's UpdateTable takes BillingMode and ProvisionedThroughput only
	// (#1574).
	"AWS::DynamoDB::Table": {
		"KeySchema":              "UpdateTable takes no KeySchema",
		"AttributeDefinitions":   "substrate's UpdateTable takes no AttributeDefinitions",
		"GlobalSecondaryIndexes": "substrate's UpdateTable takes no GlobalSecondaryIndexUpdates",
		"LocalSecondaryIndexes":  "UpdateTable cannot change a table's local secondary indexes",
		"StreamSpecification":    "substrate's UpdateTable takes no StreamSpecification",
	},
	// LaunchType is "Some interruptions" on the ECS::Service page; UpdateService has no launch type
	// member, and how CloudFormation applies the change is not published.
	"AWS::ECS::Service": {
		"LaunchType": "UpdateService takes no launch type",
	},
}

// cfnChooseIf follows Fn::If to the branch its condition selects, as many levels as it nests, and
// reports false when the value is, or selects, AWS::NoValue.
func cfnChooseIf(v interface{}, cctx *cfnContext) (interface{}, bool) {
	for {
		if isCFNNoValue(v, cctx) {
			return nil, false
		}
		m, ok := v.(map[string]interface{})
		if !ok || len(m) != 1 {
			return v, true
		}
		args, isIf := m["Fn::If"].([]interface{})
		if !isIf || len(args) < 3 {
			return v, true
		}
		name, _ := args[0].(string)
		if cctx.condition(name) {
			v = args[1]
		} else {
			v = args[2]
		}
	}
}

// cfnResolveStructured resolves a structured property value — an object, or a list of objects —
// keeping its shape. It is [resolveNested] with one difference that matters for a list of rules or
// a map of variables: an Fn::If choosing an object or a list is followed into that branch and
// resolved in turn, and one choosing AWS::NoValue drops the member or element. resolveNested
// resolves such an Fn::If as a scalar, which renders the chosen object as a string.
//
// Any other intrinsic is resolved by resolveNested, so scalars become strings and list-valued
// intrinsics splice into the list they stand in. An absent value, or one that is AWS::NoValue,
// resolves to nil.
func cfnResolveStructured(v interface{}, cctx *cfnContext) interface{} {
	v, ok := cfnChooseIf(v, cctx)
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case map[string]interface{}:
		if isCFNIntrinsic(val) {
			return resolveNested(val, cctx)
		}
		out := make(map[string]interface{}, len(val))
		for k, item := range val {
			if chosen, present := cfnChooseIf(item, cctx); present {
				out[k] = cfnResolveStructured(chosen, cctx)
			}
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, item := range val {
			chosen, present := cfnChooseIf(item, cctx)
			if !present {
				continue
			}
			if m, isMap := chosen.(map[string]interface{}); isMap && isCFNIntrinsic(m) {
				resolved := resolveNested(m, cctx)
				if list, isList := resolved.([]interface{}); isList {
					out = append(out, list...)
				} else {
					out = append(out, resolved)
				}
				continue
			}
			out = append(out, cfnResolveStructured(chosen, cctx))
		}
		return out
	}
	return v
}

// cfnChangedAny reports whether changed names any of props.
func cfnChangedAny(changed []string, props ...string) bool {
	for _, p := range props {
		if slices.Contains(changed, p) {
			return true
		}
	}
	return false
}

// cfnUpdateDispatch sends one update request and folds its cost and refusal into the running
// totals, so an updater making several calls stops at the first refusal and reports it.
func (d *StackDeployer) cfnUpdateDispatch(
	ctx context.Context, req *AWSRequest, streamID string, cost *float64,
) error {
	_, c, err := d.dispatch(ctx, req, streamID)
	*cost += c
	return err
}

// cfnJSONRequest builds a JSON-protocol request to service with body marshaled.
func cfnJSONRequest(service, operation string, body interface{}) (*AWSRequest, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("cfn %s %s marshal: %w", service, operation, err)
	}
	return &AWSRequest{
		Service: service, Operation: operation, Body: data,
		Headers: map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	}, nil
}
