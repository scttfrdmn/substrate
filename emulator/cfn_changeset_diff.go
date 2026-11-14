package emulator

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// A change set's per-resource detail (#1446).
//
// DescribeChangeSet's ResourceChange carried Action, LogicalResourceId, ResourceType and
// Replacement, and the diff behind it compared only Type and Properties — so a template that
// changed nothing but a DeletionPolicy produced no change at all, and a consumer that reads a
// change set to prove what it will do had nothing to read. This file derives the rest of the
// published shape from the two templates and the two parameter sets: Scope, Details (Target,
// Evaluation, ChangeSource, CausingEntity) and PhysicalResourceId, and Replacement from the
// details rather than as a constant.
//
// The rules, each from the CloudFormation API reference or its "Example change sets" page:
//
//   - **A property whose template value changed** is one Static DirectModification detail.
//   - **A property whose template value is unchanged but which Refs a parameter whose value
//     changed** is two details: a Dynamic DirectModification and a Static ParameterReference
//     whose CausingEntity is the parameter. "Resources that reference a changed parameter value
//     (using the Ref intrinsic function) always result in two changes: one with a Dynamic
//     evaluation and another with a Static evaluation."
//   - **A property named Tags** targets the Tags attribute, with no Name and RequiresRecreation
//     Never, as every Tags sample on that page shows; any other property targets Properties with
//     its Name.
//   - **A changed DeletionPolicy, UpdateReplacePolicy, Metadata, CreationPolicy or
//     UpdatePolicy** is one Static DirectModification detail targeting that attribute, with
//     RequiresRecreation Never. These attributes change CloudFormation's handling of the
//     resource, not the resource, so none recreates it; RequiresRecreation is published only
//     "if the Attribute value is Properties", and Never is what the page's Tags samples report
//     for a non-Properties attribute.
//   - **A dependent of a modified resource**: a property that `Fn::GetAtt`s a resource whose
//     Properties or Tags change (or that is replaced) is a Dynamic detail with ChangeSource
//     ResourceAttribute and CausingEntity `Resource.Attribute`, because the attribute's value is not
//     known until the update runs. A policy- or Metadata-only change does not propagate: it touches
//     nothing an attribute can read, so a DeletionPolicy-only change set stays confined to the
//     resources whose policy changed; a
//     property that `Ref`s a resource that will be replaced (Replacement True or Conditional, or
//     a type change) is a Dynamic ResourceReference, because "if the resource is recreated, it
//     will have a new physical ID, so all references to that resource will also be updated". A
//     Ref to a resource modified in place is not a change — its physical ID stands. Applied to a
//     fixpoint, so a chain of dependents is followed.
//   - **Replacement** is the most intrusive detail: RequiresRecreation Always with a Static
//     evaluation is True, Always with only Dynamic evaluations is Conditional, Conditionally is
//     Conditional, and Never is False — "a RequiresRecreation value of Always has the most impact,
//     followed by Conditional, and then Never."
//
// **What substrate does not know, and so does not claim.** RequiresRecreation for a property is
// the property's "Update requires" behavior in the resource-type reference, and substrate does
// not carry that catalog. [cfnRecreationProperties] lists the properties whose reference page
// says "Update requires: Replacement" for the resource types the deployer models natively, each
// read off its page; a property outside it reports Never. So Replacement False is a proof of an
// in-place update only for a property the table covers — which is the conservative direction for
// the case the issue names, a DeletionPolicy-only change set, whose every detail is Never by the
// rule above regardless of the table.
//
// Not derived: the property-level fields (Path, BeforeValue, AfterValue, AttributeChangeType),
// which AWS reports only for a change set created with IncludePropertyValues; PolicyAction,
// BeforeContext and AfterContext, which belong to the same feature; and ChangeSource Automatic,
// which needs nested stacks.

// The ResourceTargetDefinition Attribute values, in the order the API reference lists them —
// which is also the order Scope is rendered in.
var cfnChangeAttributes = []string{
	"Properties", "Metadata", "CreationPolicy", "UpdatePolicy", "DeletionPolicy", "UpdateReplacePolicy", "Tags",
}

// cfnRecreationProperties is the per-type set of properties whose "Update requires" is
// Replacement (RequiresRecreation Always) or conditional replacement (Conditionally), each read
// off the type's page in the CloudFormation Template Reference. A property not listed reports
// Never; see the file comment for why that is the limit of the claim.
var cfnRecreationProperties = map[string]map[string]string{
	"AWS::S3::Bucket":       {"BucketName": "Always", "BucketNamePrefix": "Always", "BucketNamespace": "Always"},
	"AWS::SQS::Queue":       {"FifoQueue": "Always", "QueueName": "Always"},
	"AWS::SNS::Topic":       {"FifoTopic": "Always", "TopicName": "Always"},
	"AWS::DynamoDB::Table":  {"ImportSourceSpecification": "Always", "TableName": "Always"},
	"AWS::IAM::Role":        {"Path": "Always", "RoleName": "Always"},
	"AWS::Lambda::Function": {"DurableConfig": "Always", "FunctionName": "Always", "PackageType": "Always", "TenancyConfig": "Always"},
	// From the "Example change sets" page's own sample, which reports these two.
	"AWS::EC2::Instance": {"KeyName": "Always", "InstanceType": "Conditionally"},
}

// cfnRequiresRecreation reports a Target's RequiresRecreation.
func cfnRequiresRecreation(resourceType, attribute, property string) string {
	if attribute != "Properties" {
		return "Never"
	}
	if v, ok := cfnRecreationProperties[resourceType][property]; ok {
		return v
	}
	return "Never"
}

// cfnTemplateValuesEqual compares two template values after [cfnCanonicalValue], so a number the
// JSON decoder produced (float64) and the same number the YAML decoder produced (int) are equal, as
// are a YAML map with interface keys and the JSON object it spells.
func cfnTemplateValuesEqual(a, b interface{}) bool {
	return reflect.DeepEqual(cfnCanonicalValue(a), cfnCanonicalValue(b))
}

// cfnCanonicalValue rewrites a decoded template value into one form: every integer and float as a
// float64, the form encoding/json decodes a number into, and every map keyed by string. A template
// can arrive as JSON or YAML and a stack's two templates need not share a syntax, so comparing the
// decoders' raw output reported `3600` and `3600` as a change.
func cfnCanonicalValue(v interface{}) interface{} {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	case float32:
		return float64(t)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, child := range t {
			out[k] = cfnCanonicalValue(child)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, child := range t {
			out[fmt.Sprint(k)] = cfnCanonicalValue(child)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, child := range t {
			out[i] = cfnCanonicalValue(child)
		}
		return out
	default:
		return v
	}
}

// cfnEffectiveParameters is the value each declared parameter resolves to: the supplied value,
// else the template's Default.
func cfnEffectiveParameters(declared map[string]cfnParam, supplied map[string]string) map[string]string {
	out := make(map[string]string, len(declared))
	for name, p := range declared {
		if v, ok := supplied[name]; ok {
			out[name] = v
		} else if p.Default != nil {
			out[name] = *p.Default
		}
	}
	return out
}

// cfnReferences is what one property value references: template parameters by Ref, resources by
// Ref, and resource attributes by Fn::GetAtt — including through Fn::Sub's `${…}` forms.
type cfnReferences struct {
	params    map[string]bool
	resources map[string]bool
	attrs     map[string]bool // "Logical.Attribute"
}

// cfnCollectReferences walks v and records every Ref, Fn::GetAtt and Fn::Sub reference, sorting
// each name into params or resources by what the template declares.
func cfnCollectReferences(v interface{}, params map[string]cfnParam, resources map[string]cfnResource) cfnReferences {
	refs := cfnReferences{params: map[string]bool{}, resources: map[string]bool{}, attrs: map[string]bool{}}
	addRef := func(name string) {
		if _, ok := params[name]; ok {
			refs.params[name] = true
		} else if _, ok := resources[name]; ok {
			refs.resources[name] = true
		}
	}
	addAttr := func(logical, attr string) {
		if _, ok := resources[logical]; ok && attr != "" {
			refs.attrs[logical+"."+attr] = true
		}
	}
	subRefs := func(s string) {
		for {
			start := strings.Index(s, "${")
			if start < 0 {
				return
			}
			end := strings.Index(s[start:], "}")
			if end < 0 {
				return
			}
			expr := s[start+2 : start+end]
			s = s[start+end+1:]
			if strings.HasPrefix(expr, "!") {
				continue // "${!Literal}" is an escaped literal, not a reference.
			}
			if logical, attr, dotted := strings.Cut(expr, "."); dotted {
				addAttr(logical, attr)
			} else {
				addRef(expr)
			}
		}
	}
	var walk func(interface{})
	walk = func(v interface{}) {
		switch t := v.(type) {
		case map[string]interface{}:
			if len(t) == 1 {
				if name, ok := t["Ref"].(string); ok {
					addRef(name)
					return
				}
				if ga, ok := t["Fn::GetAtt"]; ok {
					switch g := ga.(type) {
					case []interface{}:
						if len(g) == 2 {
							logical, _ := g[0].(string)
							attr, _ := g[1].(string)
							addAttr(logical, attr)
						}
					case string:
						logical, attr, _ := strings.Cut(g, ".")
						addAttr(logical, attr)
					}
					return
				}
				if sub, ok := t["Fn::Sub"]; ok {
					switch s := sub.(type) {
					case string:
						subRefs(s)
					case []interface{}:
						if len(s) > 0 {
							if str, ok := s[0].(string); ok {
								subRefs(str)
							}
						}
						if len(s) > 1 {
							walk(s[1])
						}
					}
					return
				}
			}
			for _, child := range t {
				walk(child)
			}
		case []interface{}:
			for _, child := range t {
				walk(child)
			}
		}
	}
	walk(v)
	return refs
}

// cfnChangeBuilder accumulates one resource's details, deduplicated.
type cfnChangeBuilder struct {
	details []CFNResourceChangeDetail
	seen    map[CFNResourceChangeDetail]bool
}

func (b *cfnChangeBuilder) add(d CFNResourceChangeDetail) bool {
	if b.seen == nil {
		b.seen = map[CFNResourceChangeDetail]bool{}
	}
	if b.seen[d] {
		return false
	}
	b.seen[d] = true
	b.details = append(b.details, d)
	return true
}

// touchesResource reports whether any detail changes the resource itself — a Properties or Tags
// target. A DeletionPolicy, UpdateReplacePolicy, Metadata, CreationPolicy or UpdatePolicy change
// alters how CloudFormation handles the resource and nothing an Fn::GetAtt can read, so it does not
// make a dependent's attribute reference Dynamic.
func (b *cfnChangeBuilder) touchesResource() bool {
	for _, d := range b.details {
		if d.Target.Attribute == "Properties" || d.Target.Attribute == "Tags" {
			return true
		}
	}
	return false
}

// cfnPropertyTarget is the Target a top-level property's change reports.
func cfnPropertyTarget(resourceType, property string) CFNResourceTargetDefinition {
	if property == "Tags" {
		return CFNResourceTargetDefinition{Attribute: "Tags", RequiresRecreation: "Never"}
	}
	return CFNResourceTargetDefinition{
		Attribute:          "Properties",
		Name:               property,
		RequiresRecreation: cfnRequiresRecreation(resourceType, "Properties", property),
	}
}

// cfnReplacement derives a Modify's Replacement from its details, per the ResourceChange page.
func cfnReplacement(details []CFNResourceChangeDetail) string {
	rank := 0 // 0 False, 1 Conditional, 2 True
	for _, d := range details {
		switch d.Target.RequiresRecreation {
		case "Always":
			if d.Evaluation == "Static" {
				rank = max(rank, 2)
			} else {
				rank = max(rank, 1)
			}
		case "Conditionally":
			rank = max(rank, 1)
		}
	}
	return [...]string{"False", "Conditional", "True"}[rank]
}

// cfnScope is the distinct Target.Attribute values of details, in [cfnChangeAttributes] order.
func cfnScope(details []CFNResourceChangeDetail) []string {
	present := map[string]bool{}
	for _, d := range details {
		present[d.Target.Attribute] = true
	}
	var scope []string
	for _, a := range cfnChangeAttributes {
		if present[a] {
			scope = append(scope, a)
		}
	}
	return scope
}

// cfnSortDetails orders details deterministically: Dynamic before Static, as the "Example change
// sets" page lists them, then by attribute, property name, change source and causing entity.
func cfnSortDetails(details []CFNResourceChangeDetail) {
	attrRank := map[string]int{}
	for i, a := range cfnChangeAttributes {
		attrRank[a] = i
	}
	sort.SliceStable(details, func(i, j int) bool {
		a, b := details[i], details[j]
		if a.Evaluation != b.Evaluation {
			return a.Evaluation == "Dynamic"
		}
		if a.Target.Attribute != b.Target.Attribute {
			return attrRank[a.Target.Attribute] < attrRank[b.Target.Attribute]
		}
		if a.Target.Name != b.Target.Name {
			return a.Target.Name < b.Target.Name
		}
		if a.ChangeSource != b.ChangeSource {
			return a.ChangeSource < b.ChangeSource
		}
		return a.CausingEntity < b.CausingEntity
	})
}

// diffCFNResources compares the stack's template with a change set's and returns the change set's
// resource changes, with Scope, Details, PhysicalResourceId and Replacement per the rules in this
// file's comment. oldParams and newParams are the values each template's parameters were supplied
// with; physical maps a deployed logical ID to its physical ID.
func diffCFNResources(oldTmpl, newTmpl *cfnTemplate, oldParams, newParams, physical map[string]string) []CFNResourceChange {
	oldRes, newRes := oldTmpl.Resources, newTmpl.Resources

	oldEff := cfnEffectiveParameters(oldTmpl.Parameters, oldParams)
	newEff := cfnEffectiveParameters(newTmpl.Parameters, newParams)
	changedParams := map[string]bool{}
	for name := range newTmpl.Parameters {
		if oldEff[name] != newEff[name] {
			changedParams[name] = true
		}
	}

	var changes []CFNResourceChange
	builders := map[string]*cfnChangeBuilder{}
	replaced := map[string]bool{} // removed-and-re-added by a type change

	ids := make([]string, 0, len(newRes))
	for id := range newRes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, logicalID := range ids {
		newR := newRes[logicalID]
		oldR, exists := oldRes[logicalID]
		if !exists {
			changes = append(changes, CFNResourceChange{Action: "Add", LogicalID: logicalID, ResourceType: newR.Type})
			continue
		}
		if oldR.Type != newR.Type {
			// Type changed → replacement (remove old + add new).
			replaced[logicalID] = true
			changes = append(changes,
				CFNResourceChange{Action: "Remove", LogicalID: logicalID, ResourceType: oldR.Type, PhysicalID: physical[logicalID]},
				CFNResourceChange{Action: "Add", LogicalID: logicalID, ResourceType: newR.Type},
			)
			continue
		}

		b := &cfnChangeBuilder{}
		props := map[string]bool{}
		for k := range oldR.Properties {
			props[k] = true
		}
		for k := range newR.Properties {
			props[k] = true
		}
		for prop := range props {
			target := cfnPropertyTarget(newR.Type, prop)
			oldV, newV := oldR.Properties[prop], newR.Properties[prop]
			if !cfnTemplateValuesEqual(oldV, newV) {
				b.add(CFNResourceChangeDetail{Target: target, Evaluation: "Static", ChangeSource: "DirectModification"})
				continue
			}
			refs := cfnCollectReferences(newV, newTmpl.Parameters, newRes)
			for param := range refs.params {
				if !changedParams[param] {
					continue
				}
				b.add(CFNResourceChangeDetail{Target: target, Evaluation: "Dynamic", ChangeSource: "DirectModification"})
				b.add(CFNResourceChangeDetail{Target: target, Evaluation: "Static", ChangeSource: "ParameterReference", CausingEntity: param})
			}
		}
		for _, attr := range []struct {
			name     string
			old, new interface{}
		}{
			{"Metadata", oldR.Metadata, newR.Metadata},
			{"CreationPolicy", oldR.CreationPolicy, newR.CreationPolicy},
			{"UpdatePolicy", oldR.UpdatePolicy, newR.UpdatePolicy},
			{"DeletionPolicy", oldR.DeletionPolicy, newR.DeletionPolicy},
			{"UpdateReplacePolicy", oldR.UpdateReplacePolicy, newR.UpdateReplacePolicy},
		} {
			if !cfnTemplateValuesEqual(attr.old, attr.new) {
				b.add(CFNResourceChangeDetail{
					Target:       CFNResourceTargetDefinition{Attribute: attr.name, RequiresRecreation: "Never"},
					Evaluation:   "Static",
					ChangeSource: "DirectModification",
				})
			}
		}
		builders[logicalID] = b
	}

	// Dependents of modified or replaced resources, to a fixpoint. Each pass can only add details,
	// and the set of possible details is finite, so the loop terminates.
	for {
		grew := false
		for _, logicalID := range ids {
			b, ok := builders[logicalID]
			if !ok {
				continue // added, or removed-and-re-added
			}
			newR := newRes[logicalID]
			for prop, v := range newR.Properties {
				refs := cfnCollectReferences(v, newTmpl.Parameters, newRes)
				target := cfnPropertyTarget(newR.Type, prop)
				for attr := range refs.attrs {
					dep, _, _ := strings.Cut(attr, ".")
					if db, ok := builders[dep]; replaced[dep] || (ok && db.touchesResource()) {
						if b.add(CFNResourceChangeDetail{Target: target, Evaluation: "Dynamic", ChangeSource: "ResourceAttribute", CausingEntity: attr}) {
							grew = true
						}
					}
				}
				for dep := range refs.resources {
					db, ok := builders[dep]
					if replaced[dep] || (ok && cfnReplacement(db.details) != "False") {
						if b.add(CFNResourceChangeDetail{Target: target, Evaluation: "Dynamic", ChangeSource: "ResourceReference", CausingEntity: dep}) {
							grew = true
						}
					}
				}
			}
		}
		if !grew {
			break
		}
	}

	for _, logicalID := range ids {
		b, ok := builders[logicalID]
		if !ok || len(b.details) == 0 {
			continue
		}
		cfnSortDetails(b.details)
		changes = append(changes, CFNResourceChange{
			Action:       "Modify",
			LogicalID:    logicalID,
			ResourceType: newRes[logicalID].Type,
			PhysicalID:   physical[logicalID],
			Replacement:  cfnReplacement(b.details),
			Scope:        cfnScope(b.details),
			Details:      b.details,
		})
	}

	// Removed.
	for logicalID, oldR := range oldRes {
		if _, exists := newRes[logicalID]; !exists {
			changes = append(changes, CFNResourceChange{
				Action:       "Remove",
				LogicalID:    logicalID,
				ResourceType: oldR.Type,
				PhysicalID:   physical[logicalID],
			})
		}
	}

	// Sort for deterministic output.
	sort.SliceStable(changes, func(i, j int) bool {
		if changes[i].Action != changes[j].Action {
			return changes[i].Action < changes[j].Action
		}
		return changes[i].LogicalID < changes[j].LogicalID
	})

	return changes
}
