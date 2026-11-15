package emulator

import (
	"container/heap"
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
// two cannot disagree about what a property references: a Ref, an Fn::GetAtt in either form —
// including one whose attribute name is itself an intrinsic — and both forms of Fn::Sub, including
// a `${Logical.Attribute}` inside the template string. A reference to a resource skipped by a
// false Condition adds no edge, since that resource does not deploy.
//
// A DependsOn naming a resource that does not deploy is unresolved, whether the template does not
// declare it at all or declares it under a false Condition. The second is observed behavior, not
// a published rule: user reports of the "Unresolved resource dependencies" refusal trace it to a
// DependsOn naming a conditioned-out resource, while a Ref to one inside the untaken branch of an
// Fn::If is accepted — which is why only DependsOn is held to it here.
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
	for name := range refs.attrTargets {
		add(name)
	}
	for _, name := range cfnDependsOnNames(res.DependsOn) {
		if _, deploys := active[name]; !deploys {
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

// cfnOrdering is the outcome of ordering a set of resources.
type cfnOrdering struct {
	// order lists every resource that could be ordered, each after everything it depends on.
	order []string

	// stuck lists, in tie-break order, every resource on a cycle and every resource depending on
	// one — the set CloudFormation's own message lists, since the reports quoted on
	// ErrCFNInvalidDependency name resources downstream of the cycle as well as on it.
	stuck []string

	// unresolved names, sorted, the DependsOn targets that do not deploy.
	unresolved []string
}

// cfnReadyQueue is the set of resources whose dependencies have all been placed, as a min-heap on
// (typePriority, logical ID) — the tie-break that decides which ready resource goes next.
type cfnReadyQueue struct {
	ids      []string
	priority func(string) int
}

// Len implements sort.Interface.
func (q *cfnReadyQueue) Len() int { return len(q.ids) }

// Less implements sort.Interface: lower priority first, then the lower logical ID.
func (q *cfnReadyQueue) Less(i, j int) bool {
	pi, pj := q.priority(q.ids[i]), q.priority(q.ids[j])
	if pi != pj {
		return pi < pj
	}
	return q.ids[i] < q.ids[j]
}

// Swap implements sort.Interface.
func (q *cfnReadyQueue) Swap(i, j int) { q.ids[i], q.ids[j] = q.ids[j], q.ids[i] }

// Push implements heap.Interface.
func (q *cfnReadyQueue) Push(x any) {
	if id, ok := x.(string); ok {
		q.ids = append(q.ids, id)
	}
}

// Pop implements heap.Interface.
func (q *cfnReadyQueue) Pop() any {
	last := q.ids[len(q.ids)-1]
	q.ids = q.ids[:len(q.ids)-1]
	return last
}

// cfnOrderResources orders active so every resource follows everything it depends on, with
// typePriority and then the logical ID breaking ties between resources that are ready at the same
// time. It never fails: what it cannot order is reported, and the caller decides whether that
// refuses the request ([cfnValidateDependencies]) or is tolerated ([cfnDeleteOrder], a rollback).
func cfnOrderResources(tmpl *cfnTemplate, active map[string]cfnResource) cfnOrdering {
	dependents := make(map[string][]string, len(active))
	indegree := make(map[string]int, len(active))
	unresolved := map[string]bool{}
	for id, res := range active {
		deps, missing := cfnResourceDependencies(id, res, tmpl, active)
		indegree[id] = len(deps)
		for _, dep := range deps {
			dependents[dep] = append(dependents[dep], id)
		}
		for _, name := range missing {
			unresolved[name] = true
		}
	}

	ready := &cfnReadyQueue{priority: func(id string) int { return cfnTypePriority(active[id].Type) }}
	for id := range active {
		if indegree[id] == 0 {
			ready.ids = append(ready.ids, id)
		}
	}
	heap.Init(ready)
	out := cfnOrdering{order: make([]string, 0, len(active)), unresolved: cfnSortedNames(unresolved)}
	for ready.Len() > 0 {
		next, _ := heap.Pop(ready).(string)
		out.order = append(out.order, next)
		for _, dependent := range dependents[next] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				heap.Push(ready, dependent)
			}
		}
	}
	if len(out.order) == len(active) {
		return out
	}
	stuck := &cfnReadyQueue{priority: ready.priority}
	for id := range active {
		if indegree[id] > 0 {
			stuck.ids = append(stuck.ids, id)
		}
	}
	sort.Sort(stuck)
	out.stuck = stuck.ids
	return out
}

// all is the whole order, with any resource that could not be ordered placed last.
func (o cfnOrdering) all() []string {
	return append(append([]string(nil), o.order...), o.stuck...)
}

// err is the request-level refusal for an ordering, or nil: an unresolved DependsOn first, as the
// template format error it is, and then a cycle.
func (o cfnOrdering) err() error {
	if len(o.unresolved) > 0 {
		return cfnErrf(ErrCFNInvalidDependency,
			"Template format error: Unresolved resource dependencies [%s] in the Resources block of the template",
			strings.Join(o.unresolved, ", "))
	}
	if len(o.stuck) > 0 {
		names := append([]string(nil), o.stuck...)
		sort.Strings(names)
		return cfnErrf(ErrCFNInvalidDependency, "Circular dependency between resources: [%s]",
			strings.Join(names, ", "))
	}
	return nil
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
// so a stack recorded before #1420 sweeps in dependency order too. It is never refused: a stack
// that exists has to be deletable, so a DependsOn naming a resource the record does not hold is
// ignored, and resources on a cycle are deleted first, in reverse tie-break order. A resource the
// template does not declare, or every resource when the template no longer parses, falls back to
// the tie-break order alone, reversed — the only order still derivable — and goes first, since
// nothing in the template can depend on it.
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
	order := cfnOrderResources(tmpl, active).all()
	for i := len(order) - 1; i >= 0; i-- {
		out = append(out, byID[order[i]]...)
	}
	return out
}

// cfnValidateDependencies refuses a template whose deployable resources cannot be ordered, the
// check CreateStack, UpdateStack and CreateChangeSet make before anything else, and otherwise
// answers the deploy order.
func cfnValidateDependencies(tmpl *cfnTemplate, cctx *cfnContext) ([]string, error) {
	ordering := cfnOrderResources(tmpl, cfnActiveResources(tmpl, cctx))
	if err := ordering.err(); err != nil {
		return nil, err
	}
	return ordering.order, nil
}

// cfnTolerantDeployOrder is the deploy order without the refusals, for a deploy that must proceed
// whatever the template says: the rollback of a failed update re-deploys the stack's previous
// template, which may predate the validation (#1420) and still has to converge.
func cfnTolerantDeployOrder(tmpl *cfnTemplate, cctx *cfnContext) []string {
	return cfnOrderResources(tmpl, cfnActiveResources(tmpl, cctx)).all()
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
