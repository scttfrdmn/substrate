package emulator

import (
	"sort"
	"strings"
)

// cfn_dependency_order.go orders a stack's resources by what they depend on (#1420).
//
// The deployer used to sort resources by [typePriority] and then by logical ID, and nothing
// else. References played no part and DependsOn was parsed and never read, so a resource could
// deploy before the resource it names. AWS::IAM::Policy, the one type at priority 0, deployed
// before everything: an Fn::GetAtt in its PolicyDocument to a queue, a bucket or a table — the
// grant pattern CDK emits — found nothing deployed and resolved to the bare "<LogicalId>.Arn".
//
// The DependsOn attribute page states the rule this file implements: "Dependent stacks also have
// implicit dependencies in the form of target properties !Ref, !GetAtt, and !Sub. For example, if
// the properties of resource A use a !Ref to resource B, the following rules apply: Resource B is
// created before resource A. Resource A is deleted before resource B. Resource B is updated before
// resource A." DependsOn adds an explicit edge of the same kind, and "can take a single string or
// list of strings".
//
// **typePriority is now a tie-break.** Among the resources whose dependencies have all deployed,
// the one with the lowest priority goes first, and the logical ID breaks a tie. That choice keeps
// every template that already deployed correctly deploying in exactly the order it did: when the
// old (priority, logical ID) order respected every edge, picking the minimum ready resource at each
// step reproduces it, because the next resource in the old order is always ready and is always the
// minimum of what remains. Only a template whose old order contradicted one of its own references
// deploys differently, and that difference is the fix.
//
// Two refusals are request-level, as CloudFormation makes them: the template is rejected with a
// ValidationError before any resource deploys. See [ErrCFNInvalidDependency] for the wording and
// where it comes from.

// cfnImpliedDependencies are the edges a service's own ordering rule requires and a template need
// not declare, keyed by the dependent type.
//
// AWS Config is the case. Its pages state the order, and real templates rely on CloudFormation
// knowing it rather than declaring DependsOn: "Before you can create a delivery channel, you must
// create a configuration recorder", and a rule is put once the recorder and its channel exist
// (PutConfigRule refuses without a recorder, and the channel's deploy is what starts the recorder;
// see deployConfigDeliveryChannel). typePriority carried this as recorder 2, channel 3, rule 4,
// which only holds while nothing else delays the recorder; an edge holds regardless.
//
// Only declared resources of the named types are depended on, and only ones that deploy: a
// recorder skipped by a false Condition adds no edge.
var cfnImpliedDependencies = map[string][]string{
	"AWS::Config::DeliveryChannel": {"AWS::Config::ConfigurationRecorder"},
	"AWS::Config::ConfigRule":      {"AWS::Config::ConfigurationRecorder", "AWS::Config::DeliveryChannel"},
}

// cfnDependsOnNames returns a resource's DependsOn attribute as a list of logical IDs: "the
// DependsOn attribute can take a single string or list of strings". A member that is not a
// string is dropped; it names nothing.
func cfnDependsOnNames(v interface{}) []string {
	switch t := v.(type) {
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	}
	return nil
}

// cfnResourceDependencies returns the logical IDs a resource depends on among active, sorted, and
// the DependsOn targets the template does not declare at all.
//
// References are collected with [cfnCollectReferences], the same walk a change set uses, so the
// two cannot disagree about what a property references: a Ref, an Fn::GetAtt in either form, and
// both forms of Fn::Sub, including a `${Logical.Attribute}` inside the template string. A
// reference to a resource skipped by a false Condition adds no edge, since that resource does not
// deploy; nor does a DependsOn naming one, which the template does declare.
func cfnResourceDependencies(
	logicalID string, res cfnResource, tmpl *cfnTemplate, active map[string]cfnResource,
) (deps []string, unresolved []string) {
	set := map[string]bool{}
	add := func(name string) {
		if _, ok := active[name]; ok {
			set[name] = true
		}
	}
	refs := cfnCollectReferences(res.Properties, tmpl.Parameters, tmpl.Resources)
	for name := range refs.resources {
		add(name)
	}
	for attr := range refs.attrs {
		name, _, _ := strings.Cut(attr, ".")
		add(name)
	}
	for _, name := range cfnDependsOnNames(res.DependsOn) {
		if _, declared := tmpl.Resources[name]; !declared {
			unresolved = append(unresolved, name)
			continue
		}
		add(name)
	}
	if implied := cfnImpliedDependencies[res.Type]; len(implied) > 0 {
		for name, other := range active {
			if name == logicalID {
				continue
			}
			for _, t := range implied {
				if other.Type == t {
					set[name] = true
				}
			}
		}
	}
	deps = make([]string, 0, len(set))
	for name := range set {
		deps = append(deps, name)
	}
	sort.Strings(deps)
	return deps, unresolved
}

// cfnOrderResources returns the logical IDs of active in an order where every resource follows
// everything it depends on, with typePriority and then the logical ID breaking ties between
// resources that are ready at the same time.
//
// strict is the request path. It refuses an undeclared DependsOn target and a cycle with the
// [ErrCFNInvalidDependency] messages. Non-strict is the delete path, which orders a stack that
// already exists: an undeclared target is ignored, and resources caught in a cycle follow the rest
// in tie-break order rather than failing a sweep that has to proceed regardless.
func cfnOrderResources(tmpl *cfnTemplate, active map[string]cfnResource, strict bool) ([]string, error) {
	deps := make(map[string][]string, len(active))
	dependents := make(map[string][]string, len(active))
	indegree := make(map[string]int, len(active))
	unresolvedSet := map[string]bool{}
	for id, res := range active {
		d, unresolved := cfnResourceDependencies(id, res, tmpl, active)
		deps[id] = d
		indegree[id] = len(d)
		for _, dep := range d {
			dependents[dep] = append(dependents[dep], id)
		}
		for _, name := range unresolved {
			unresolvedSet[name] = true
		}
	}
	if strict && len(unresolvedSet) > 0 {
		return nil, cfnErrf(ErrCFNInvalidDependency,
			"Template format error: Unresolved resource dependencies [%s] in the Resources block of the template",
			strings.Join(cfnSortedNames(unresolvedSet), ", "))
	}

	less := func(a, b string) bool {
		pa, pb := cfnTypePriority(active[a].Type), cfnTypePriority(active[b].Type)
		if pa != pb {
			return pa < pb
		}
		return a < b
	}
	var ready []string
	for id := range active {
		if indegree[id] == 0 {
			ready = append(ready, id)
		}
	}
	order := make([]string, 0, len(active))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
		next := ready[0]
		ready = ready[1:]
		order = append(order, next)
		for _, dependent := range dependents[next] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if len(order) == len(active) {
		return order, nil
	}

	// What is left is every resource on a cycle and every resource that depends on one, which is
	// the set CloudFormation's own message lists — the reports quoted on ErrCFNInvalidDependency
	// name resources downstream of the cycle as well as on it.
	placed := make(map[string]bool, len(order))
	for _, id := range order {
		placed[id] = true
	}
	stuck := make([]string, 0, len(active)-len(order))
	for id := range active {
		if !placed[id] {
			stuck = append(stuck, id)
		}
	}
	sort.Slice(stuck, func(i, j int) bool { return less(stuck[i], stuck[j]) })
	if strict {
		names := append([]string(nil), stuck...)
		sort.Strings(names)
		return nil, cfnErrf(ErrCFNInvalidDependency, "Circular dependency between resources: [%s]",
			strings.Join(names, ", "))
	}
	return append(order, stuck...), nil
}

// cfnSortedNames returns a set's members in ascending order.
func cfnSortedNames(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// cfnDeleteOrder returns resources in the order a sweep deletes them: the reverse of the order
// they deploy in, so a resource goes before anything it depends on — "Resource A is deleted
// before resource B" (#1420).
//
// The order is recomputed from the template the stack recorded rather than read off the record,
// so a stack recorded before #1420 sweeps in dependency order too. A resource the template does
// not declare, or a template that no longer parses, falls back to the tie-break order alone,
// reversed — the only order that is still derivable — and such resources are deleted first,
// since nothing in the template can depend on them.
func cfnDeleteOrder(tmpl *cfnTemplate, resources []DeployedResource) []DeployedResource {
	byID := make(map[string][]DeployedResource, len(resources))
	active := map[string]cfnResource{}
	var orphans []DeployedResource
	for _, dr := range resources {
		if tmpl != nil {
			if res, ok := tmpl.Resources[dr.LogicalID]; ok {
				active[dr.LogicalID] = res
				byID[dr.LogicalID] = append(byID[dr.LogicalID], dr)
				continue
			}
		}
		orphans = append(orphans, dr)
	}
	sort.SliceStable(orphans, func(i, j int) bool {
		pi, pj := cfnTypePriority(orphans[i].Type), cfnTypePriority(orphans[j].Type)
		if pi != pj {
			return pi > pj
		}
		return orphans[i].LogicalID > orphans[j].LogicalID
	})
	out := make([]DeployedResource, 0, len(resources))
	out = append(out, orphans...)
	if len(active) == 0 {
		return out
	}
	order, err := cfnOrderResources(tmpl, active, false)
	if err != nil {
		// Non-strict ordering does not fail; this is unreachable, and the safe answer is the
		// record's own order reversed.
		for i := len(resources) - 1; i >= 0; i-- {
			if _, ok := active[resources[i].LogicalID]; ok {
				out = append(out, resources[i])
			}
		}
		return out
	}
	for i := len(order) - 1; i >= 0; i-- {
		out = append(out, byID[order[i]]...)
	}
	return out
}

// cfnValidateDependencies refuses a template whose deployable resources cannot be ordered, the
// check CreateStack, UpdateStack and CreateChangeSet make before anything else. It answers the
// deploy order for a caller that wants it.
func cfnValidateDependencies(tmpl *cfnTemplate, cctx *cfnContext) ([]string, error) {
	return cfnOrderResources(tmpl, cfnActiveResources(tmpl, cctx), true)
}

// cfnActiveResources returns the template's resources that deploy: every one whose Condition is
// absent or true.
func cfnActiveResources(tmpl *cfnTemplate, cctx *cfnContext) map[string]cfnResource {
	active := make(map[string]cfnResource, len(tmpl.Resources))
	for id, res := range tmpl.Resources {
		if res.Condition != "" {
			if val, ok := cctx.conditions[res.Condition]; ok && !val {
				continue
			}
		}
		active[id] = res
	}
	return active
}
