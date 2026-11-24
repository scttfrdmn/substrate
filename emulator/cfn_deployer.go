package emulator

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"
)

// cfnTemplate is the top-level CloudFormation template structure.
type cfnTemplate struct {
	AWSTemplateFormatVersion string                 `json:"AWSTemplateFormatVersion" yaml:"AWSTemplateFormatVersion"`
	Description              string                 `json:"Description,omitempty"    yaml:"Description,omitempty"`
	Parameters               map[string]cfnParam    `json:"Parameters,omitempty"     yaml:"Parameters,omitempty"`
	Mappings                 cfnMappings            `json:"Mappings,omitempty"       yaml:"Mappings,omitempty"`
	Conditions               map[string]interface{} `json:"Conditions,omitempty"     yaml:"Conditions,omitempty"`
	Resources                map[string]cfnResource `json:"Resources"                yaml:"Resources"`
	Outputs                  map[string]cfnOutput   `json:"Outputs,omitempty"        yaml:"Outputs,omitempty"`

	// Transform is the template's top-level Transform section — a macro name such as
	// "AWS::Serverless-2016-10-31", or a list of them. Substrate runs no macro; the section is
	// decoded only so GetTemplate can tell a template whose Processed stage would differ from
	// its Original one (#1446).
	Transform interface{} `json:"Transform,omitempty" yaml:"Transform,omitempty"`
}

// cfnMappings is the template's Mappings section: a mapping name, a top-level
// key, a second-level key, and a value.
//
// Exactly three levels of map, because that is what CloudFormation defines —
// "within the mapping, each map is a key followed by another mapping", and
// Fn::FindInMap takes exactly a map name and two keys. The leaf is interface{}
// because "the values can be of type String or List", which is why Fn::FindInMap
// is one of the intrinsics that can resolve to a list.
//
// The first three levels being typed is also a check: "the keys in mappings must
// be literal strings" and "you can't include parameters, pseudo parameters, or
// intrinsic functions in the Mappings section", so a template that puts an
// intrinsic where a key belongs fails to decode here rather than resolving to
// something CloudFormation would have rejected.
type cfnMappings map[string]map[string]map[string]interface{}

// cfnParam is a CloudFormation template parameter declaration.
type cfnParam struct {
	// Type is the CloudFormation parameter type (e.g., "String").
	Type string `json:"Type" yaml:"Type"`

	// Default is the default value for the parameter, or nil when the template
	// declares none. The pointer distinguishes `Default: ''` — the conventional
	// way to declare an optional parameter, which every `Fn::Not [Fn::Equals
	// [Ref X, '']]` condition tests for — from a parameter with no default at
	// all. Treating the two alike left the parameter undeclared, and Ref then
	// echoed the parameter's own name back, inverting the condition.
	Default *string `json:"Default" yaml:"Default"`

	// Description and NoEcho are reported by ValidateTemplate's TemplateParameter (#1605).
	// NoEcho is decoded loosely because templates write it both as a boolean and as the
	// string "true"; see [cfnParam.noEcho].
	Description string      `json:"Description,omitempty" yaml:"Description,omitempty"`
	NoEcho      interface{} `json:"NoEcho,omitempty"      yaml:"NoEcho,omitempty"`
}

// noEcho reports whether the parameter is declared NoEcho, as a boolean or as the string
// "true" in any case.
func (p cfnParam) noEcho() bool {
	switch v := p.NoEcho.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// cfnOutput is a CloudFormation template output declaration.
type cfnOutput struct {
	// Value is the output value expression (may be an intrinsic function).
	Value interface{} `json:"Value" yaml:"Value"`

	// Description is a human-readable description of the output.
	Description string `json:"Description" yaml:"Description"`

	// Export names the output for cross-stack import, or is nil when the output
	// is local to its stack. Only an exported output is importable: an output
	// without an Export is readable through DescribeStacks and nowhere else.
	Export *cfnExport `json:"Export,omitempty" yaml:"Export,omitempty"`

	// Condition names the template condition the output is created under, empty for an
	// output that always is.
	Condition string `json:"Condition,omitempty" yaml:"Condition,omitempty"`
}

// cfnOutputActive reports whether an output is created: it names no Condition, or one that is
// true. A condition the context does not know is treated as true, as a resource's is.
func cfnOutputActive(out cfnOutput, cctx *cfnContext) bool {
	if out.Condition == "" {
		return true
	}
	val, ok := cctx.conditions[out.Condition]
	return !ok || val
}

// cfnExport is an output's Export block, which names the value for import by
// another stack's Fn::ImportValue.
//
// Name is an expression rather than a string because the conventional form is
// `Export: {Name: !Sub '${AWS::StackName}-SubnetID'}` — the export name is
// namespaced by the exporting stack so two deployments of the same template do
// not collide. It resolves before any resource deploys, which the API permits:
// "the value of the Name property of an Export can't use Ref or GetAtt functions
// that depend on a resource", so an export name is knowable from the template and
// its parameters alone.
type cfnExport struct {
	Name interface{} `json:"Name" yaml:"Name"`
}

// cfnResource is a single CloudFormation resource declaration.
//
// DeletionPolicy and UpdateReplacePolicy are plain string resource attributes, so
// one field with both tags serves the JSON and YAML template paths alike — both
// unmarshal into this struct.
type cfnResource struct {
	Type       string                 `json:"Type"                yaml:"Type"`
	Properties map[string]interface{} `json:"Properties"          yaml:"Properties"`
	// DependsOn names the resources this one is created after, as a string or a list of
	// strings. It is an edge in the deploy order alongside the Ref, Fn::GetAtt and Fn::Sub
	// references in Properties (#1420; see cfnResourceDependencies), and a name the template
	// does not declare refuses the request.
	DependsOn interface{} `json:"DependsOn,omitempty" yaml:"DependsOn,omitempty"`
	Condition string      `json:"Condition,omitempty" yaml:"Condition,omitempty"`

	// DeletionPolicy is what to do with the resource when it leaves the stack:
	// "Delete", "Retain", "RetainExceptOnCreate" or "Snapshot". Empty means the
	// template declared none, which is not the same as "Delete" — the default is
	// Snapshot for two RDS types, so the default is resolved rather than assumed
	// (see cfnDeletionPolicyFor).
	DeletionPolicy string `json:"DeletionPolicy,omitempty" yaml:"DeletionPolicy,omitempty"`

	// UpdateReplacePolicy is the same set of values for the resource CloudFormation
	// leaves behind when an update replaces it. Since #1427 an update replaces a
	// resource the way CloudFormation does — the new one first, the old one deleted
	// once the update succeeds — and this is the policy that delete applies (see
	// cfnPolicyForOp); Retain leaves the old resource in place.
	UpdateReplacePolicy string `json:"UpdateReplacePolicy,omitempty" yaml:"UpdateReplacePolicy,omitempty"`

	// Metadata, CreationPolicy and UpdatePolicy are the three remaining resource
	// attributes a change set's Scope publishes. Substrate deploys none of them; they
	// are decoded so a change to one is reported by CreateChangeSet rather than being
	// invisible to the diff (#1446).
	Metadata       interface{} `json:"Metadata,omitempty"       yaml:"Metadata,omitempty"`
	CreationPolicy interface{} `json:"CreationPolicy,omitempty" yaml:"CreationPolicy,omitempty"`
	UpdatePolicy   interface{} `json:"UpdatePolicy,omitempty"   yaml:"UpdatePolicy,omitempty"`
}

// CloudFormation deletion policies, as the DeletionPolicy attribute reference
// defines them.
const (
	// cfnPolicyDelete deletes the resource and its content.
	cfnPolicyDelete = "Delete"

	// cfnPolicyRetain keeps the resource, removing it from the stack's scope only.
	cfnPolicyRetain = "Retain"

	// cfnPolicyRetainExceptOnCreate behaves like Retain "except for the stack
	// operation that initially created the resource": if the operation that created
	// the resource is rolled back, CloudFormation deletes it.
	cfnPolicyRetainExceptOnCreate = "RetainExceptOnCreate"

	// cfnPolicySnapshot snapshots the resource before deleting it.
	cfnPolicySnapshot = "Snapshot"
)

// cfnDeletionOp is the stack operation a deletion policy is being resolved for.
// RetainExceptOnCreate is the only policy whose meaning depends on it, and it
// depends on it entirely: the same declaration retains on a delete and deletes on
// a create rollback. Passing the operation in is what keeps that rule in one place
// instead of special-cased at each caller.
type cfnDeletionOp int

const (
	// cfnDeleteStackOp is a DeleteStack sweep.
	cfnDeleteStackOp cfnDeletionOp = iota

	// cfnCreateRollbackOp is the rollback of the operation that created the
	// resource, where RetainExceptOnCreate deletes rather than retains.
	cfnCreateRollbackOp

	// cfnUpdateReplaceOp is the delete of a resource an update replaced (#1427). The
	// UpdateReplacePolicy page says that attribute, not DeletionPolicy, governs it: it
	// "only applies to resources replaced during stack updates".
	cfnUpdateReplaceOp
)

// cfnPolicyForOp is the policy a sweep applies to res for op: UpdateReplacePolicy for a
// replaced resource, whose default is Delete for every type ("By default, if you don't
// specify an UpdateReplacePolicy, CloudFormation deletes your resources"), and the
// DeletionPolicy with its per-type default otherwise.
func cfnPolicyForOp(res cfnResource, op cfnDeletionOp) string {
	if op == cfnUpdateReplaceOp {
		if res.UpdateReplacePolicy != "" {
			return res.UpdateReplacePolicy
		}
		return cfnPolicyDelete
	}
	return cfnDeletionPolicyFor(res)
}

// cfnNotUpdatableStatuses are the stack statuses an update is refused in; see
// [ErrCFNStackNotUpdatable] for the source of each.
var cfnNotUpdatableStatuses = map[string]bool{
	cfnStackRollbackComplete: true,
	cfnStackRollbackFailed:   true,
	cfnStackDeleteFailed:     true,
	// A stack a CREATE change set created has no template to update until that change set is
	// executed: "The stack will be in the REVIEW_IN_PROGRESS state until you execute the change set"
	// (API_CreateChangeSet), and an UPDATE change set "can't" be used "for a new stack" (#1606).
	cfnStackReviewInProgress: true,
}

// checkUpdatable refuses an update of a stack whose status admits none.
func (d *StackDeployer) checkUpdatable(stack *CFNStackState) error {
	if stack == nil || !cfnNotUpdatableStatuses[stack.Status] {
		return nil
	}
	return cfnErrf(ErrCFNStackNotUpdatable, "Stack:%s is in %s state and can not be updated.",
		cfnStackARN(d.identity.region, d.identity.accountID, stack.StackName), stack.Status)
}

// cfnValidUpdateReplacePolicies is the set the UpdateReplacePolicy attribute page publishes:
// Delete, Retain and Snapshot. RetainExceptOnCreate is a DeletionPolicy value only — the page
// lists no such option — so it is not accepted here.
var cfnValidUpdateReplacePolicies = map[string]bool{
	cfnPolicyDelete:   true,
	cfnPolicyRetain:   true,
	cfnPolicySnapshot: true,
}

// cfnValidPolicyForOp reports whether policy is one op's attribute accepts.
func cfnValidPolicyForOp(policy string, op cfnDeletionOp) bool {
	if op == cfnUpdateReplaceOp {
		return cfnValidUpdateReplacePolicies[policy]
	}
	return cfnValidDeletionPolicies[policy]
}

// cfnValidateUpdateReplacePolicies refuses a template declaring an UpdateReplacePolicy outside the
// published set, before anything deploys.
//
// Refused at the request because the value is only read when an update replaces the resource,
// which may be long after the template was accepted; a typo'd "Retian" found then would delete the
// old resource the attribute exists to keep. The code is the template-format ValidationError every
// other malformed template answers; **the message is substrate's own**, since no AWS source
// publishes the wording for this refusal.
func cfnValidateUpdateReplacePolicies(tmpl *cfnTemplate) error {
	ids := make([]string, 0, len(tmpl.Resources))
	for id := range tmpl.Resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if p := tmpl.Resources[id].UpdateReplacePolicy; p != "" && !cfnValidUpdateReplacePolicies[p] {
			return cfnErrf(ErrCFNTemplateInvalid,
				"Invalid UpdateReplacePolicy %q for resource %s: the value must be Delete, Retain or Snapshot",
				p, id)
		}
	}
	return nil
}

// cfnValidDeletionPolicies is the set a template may declare. A value outside it
// is a template error rather than a silent fall back to the default: a typo'd
// "Retian" that deletes the resource is exactly what the attribute exists to
// prevent, and CloudFormation itself rejects the template.
var cfnValidDeletionPolicies = map[string]bool{
	cfnPolicyDelete:               true,
	cfnPolicyRetain:               true,
	cfnPolicyRetainExceptOnCreate: true,
	cfnPolicySnapshot:             true,
}

// cfnSnapshotDefaultTypes are the types whose default deletion policy is Snapshot
// rather than Delete. "The default policy is Snapshot for AWS::RDS::DBCluster
// resources and for AWS::RDS::DBInstance resources that don't specify the
// DBClusterIdentifier property" — the DBInstance condition is a property test, so
// it lives in cfnDeletionPolicyFor rather than in this set.
var cfnSnapshotDefaultTypes = map[string]bool{
	"AWS::RDS::DBCluster":  true,
	"AWS::RDS::DBInstance": true,
}

// cfnSnapshotCapableTypes are the eight types the reference lists as supporting
// Snapshot. Substrate models a delete for four of them; the rest are recorded here
// so a Snapshot declaration on a type that genuinely supports it is distinguished
// from one on a type where it means nothing.
var cfnSnapshotCapableTypes = map[string]bool{
	"AWS::DocDB::DBCluster":              true,
	"AWS::EC2::Volume":                   true,
	"AWS::ElastiCache::CacheCluster":     true,
	"AWS::ElastiCache::ReplicationGroup": true,
	"AWS::Neptune::DBCluster":            true,
	"AWS::RDS::DBCluster":                true,
	"AWS::RDS::DBInstance":               true,
	"AWS::Redshift::Cluster":             true,
}

// cfnDeletionPolicyFor resolves the policy in force for a resource, applying the
// default the reference specifies for its type.
//
// The default is resolved here rather than at the call site because it is not
// uniformly "Delete": an RDS cluster, and a standalone RDS instance, default to
// Snapshot. A sweep that assumed Delete would destroy a database the template
// asked to be snapshotted, which is the one failure mode this attribute exists to
// prevent.
func cfnDeletionPolicyFor(res cfnResource) string {
	if res.DeletionPolicy != "" {
		return res.DeletionPolicy
	}
	if cfnSnapshotDefaultTypes[res.Type] {
		// "for AWS::RDS::DBInstance resources that don't specify the
		// DBClusterIdentifier property" — an instance in a cluster follows the
		// cluster's fate, so its own default is Delete.
		if res.Type == "AWS::RDS::DBInstance" {
			if v, ok := res.Properties["DBClusterIdentifier"]; ok && v != nil {
				return cfnPolicyDelete
			}
		}
		return cfnPolicySnapshot
	}
	return cfnPolicyDelete
}

// cfnPolicyRetainsResource reports whether policy leaves the resource in place for
// the given operation.
//
// Snapshot does not retain: it snapshots and then deletes. Only Retain always
// retains, and RetainExceptOnCreate retains for every operation except the
// rollback of the create that made the resource.
func cfnPolicyRetainsResource(policy string, op cfnDeletionOp) bool {
	switch policy {
	case cfnPolicyRetain:
		return true
	case cfnPolicyRetainExceptOnCreate:
		return op != cfnCreateRollbackOp
	default:
		return false
	}
}

// CFNStackState holds persisted state for a deployed CloudFormation stack.
type CFNStackState struct {
	// StackName is the name of the CloudFormation stack.
	StackName string `json:"StackName"`

	// TemplateBody is the raw template body.
	TemplateBody string `json:"TemplateBody"`

	// Parameters holds the resolved parameter values used during deployment.
	Parameters map[string]string `json:"Parameters"`

	// Tags holds the stack-level tags the create or update supplied, empty for a
	// stack created without any.
	//
	// A map rather than an ordered list because CloudFormation's own model is
	// key-value with unique keys, and because the distinction an update turns on is
	// *absence*: "if you don't specify this parameter, CloudFormation doesn't modify
	// the stack's tags. If you specify an empty value, CloudFormation removes all
	// associated tags." A nil map is the omitted parameter and an empty non-nil one
	// is the empty list, which no slice-of-pairs encoding expresses as cheaply.
	//
	// Sorted at the point it is rendered rather than stored ordered, following
	// [cfnParametersXML], so a DescribeStacks response is byte-identical across
	// runs.
	Tags map[string]string `json:"Tags,omitempty"`

	// Resources lists the deployed resources.
	Resources []DeployedResource `json:"Resources"`

	// Outputs holds resolved output values.
	Outputs map[string]string `json:"Outputs"`

	// ExportNames maps an output key to the export name the template declared for
	// it, for the outputs that declare one. An output without an Export
	// contributes nothing: it is readable through DescribeStacks and importable
	// nowhere.
	//
	// Keyed by output key rather than by export name so it is one map serving two
	// readers without either deriving the other's answer: DescribeStacks reports
	// the ExportName beside its output, and the export registry joins these keys
	// against Outputs to get name → value.
	ExportNames map[string]string `json:"ExportNames,omitempty"`

	// Imports lists the export names this stack resolved, sorted. This is the
	// record that makes the exporting stack undeletable, and it is written from
	// what the resolver actually walked rather than from the template text — an
	// Fn::ImportValue in a false Fn::If branch or in a skipped resource is not an
	// import.
	Imports []string `json:"Imports,omitempty"`

	// AccountID and Region scope the two above. Exports and imports are matched
	// within one account and Region — "for each AWS account, Export names must be
	// unique within a Region" — so a stack that exports MyApp-SubnetID in
	// us-east-1 must not satisfy an import of the same name in eu-west-1.
	AccountID string `json:"AccountID,omitempty"`
	Region    string `json:"Region,omitempty"`

	// Status is the stack status (e.g., "CREATE_COMPLETE").
	Status string `json:"Status"`

	// StatusReason is the "success/failure message associated with the stack
	// status", which for substrate is always a failure message: the reasons the
	// resources that failed gave, or what a rollback could not delete. It is what a
	// caller reads to learn *why* a stack is not CREATE_COMPLETE without walking
	// every resource.
	StatusReason string `json:"StatusReason,omitempty"`

	// OnFailure is the failure action the create was requested with, empty for a
	// stack created before the option was modeled. It is persisted because
	// DescribeStacks reports DisableRollback, which is this value in another form —
	// see cfnStackDisablesRollback.
	OnFailure string `json:"OnFailure,omitempty"`

	// CreatedAt is the time the stack was first deployed.
	CreatedAt time.Time `json:"CreatedAt"`

	// ReviewedAt is when a CREATE change set created the stack in REVIEW_IN_PROGRESS, zero for a
	// stack CreateStack created (#1606). It is the stack's CreationTime — the stack exists from
	// that moment, "with a unique stack ID, but no template or resources" — and the time of its
	// REVIEW_IN_PROGRESS event, which precedes the CREATE_IN_PROGRESS of its execution.
	ReviewedAt time.Time `json:"ReviewedAt,omitzero"`

	// UpdatedAt is the time the stack was last updated.
	UpdatedAt time.Time `json:"UpdatedAt"`

	// ResourceDeletions records what a delete or rollback sweep did with each
	// resource, and is what tells a caller which resource is holding a stack.
	//
	// Written whenever a sweep ran and the record survived it, which is two cases: a
	// DeleteStack sweep that failed (a successful one removes the whole record), and
	// a create rollback, where the stack remains in ROLLBACK_COMPLETE or
	// ROLLBACK_FAILED with the resources gone. So its presence does not by itself
	// mean failure — the Status says which.
	ResourceDeletions []CFNResourceDeletion `json:"ResourceDeletions,omitempty"`

	// RoleARN is the IAM service role CloudFormation assumes to make the stack's
	// resource calls, empty when the stack has none.
	//
	// Persisted because it outlives the operation that set it: CloudFormation
	// "always uses this role for all future operations on the stack", and an
	// UpdateStack that omits RoleARN "uses the role that was previously associated
	// with the stack" rather than reverting to the caller's own permissions. A
	// DeleteStack's RoleARN is the exception and is deliberately not written here —
	// it applies to that operation only.
	RoleARN string `json:"RoleARN,omitempty"`

	// CreatorPrincipalARN is the principal that created the stack, empty when the
	// creating call had none.
	//
	// This is the principal a resource call runs as when the stack has no service
	// role, which is CloudFormation's own model: absent a role it uses "a temporary
	// session that's generated from your user credentials". Recording it is what
	// makes a rollback's deletes run as the identity that created the stack rather
	// than unauthorized — the alternative, an exempt service principal, would
	// reintroduce exactly the blind spot #411 exists to remove, since a rollback
	// would then be able to delete what the creator could not.
	CreatorPrincipalARN string `json:"CreatorPrincipalARN,omitempty"`

	// CreatorUserName is the creating principal's IAM user name, empty when it had
	// none.
	//
	// Persisted beside the ARN rather than derived from it, for the reason
	// [Principal.UserName] gives: the ARN may have been synthesized from an access key
	// ID, so parsing it back would publish a credential ID as `aws:username`. Without
	// this, a rollback's deletes ran with the key absent while the create that made the
	// resources ran with it present — so a policy naming ${aws:username} authorized the
	// create and refused to clean up after it (#745).
	CreatorUserName string `json:"CreatorUserName,omitempty"`
}

// attribution returns who the stack's resource calls are made as, from what was
// persisted about the stack.
//
// This is the reader that makes a later operation on a stack — an update that omits
// RoleARN, a delete, a rollback's sweep — run as whatever the stack was created
// with rather than as whoever happens to be asking now.
func (s CFNStackState) attribution() cfnAttribution {
	att := cfnAttribution{roleARN: s.RoleARN}
	if s.CreatorPrincipalARN != "" {
		att.creator = &Principal{
			ARN:      s.CreatorPrincipalARN,
			Type:     cfnPrincipalType(s.CreatorPrincipalARN),
			UserName: s.CreatorUserName,
		}
	}
	return att
}

// setAttribution records the attribution a deployment ran under, so a later
// operation on the stack resolves the same one.
//
// The inverse of [CFNStackState.attribution], and paired with it so the two fields
// are written in one place: a stack whose RoleARN was persisted but whose creator
// was not would silently fall back to unauthorized the moment its role was removed.
func (s *CFNStackState) setAttribution(att cfnAttribution) {
	s.RoleARN = att.roleARN
	if att.creator != nil {
		s.CreatorPrincipalARN = att.creator.ARN
		s.CreatorUserName = att.creator.UserName
	}
}

// cfnPrincipalType labels an ARN with the Principal.Type it describes.
//
// Nothing evaluates Type — authorization resolves the ARN — so this exists to keep
// a recorded event readable rather than to decide anything. Deriving it beats
// storing it: the type is a property of the ARN, and a stored one could disagree
// with the ARN it sits beside after a replay of a run recorded by an older build.
func cfnPrincipalType(arn string) string {
	entityType, _ := parsePrincipalARN(arn)
	switch entityType {
	case "role":
		return "Role"
	case "assumed-role":
		return "AssumedRole"
	case "user":
		return "User"
	default:
		return "Service"
	}
}

// exports returns the stack's export name → resolved value map, joining
// ExportNames against Outputs.
//
// An export name whose output has since disappeared from Outputs is dropped
// rather than exported as the empty string: an import that resolved to "" is the
// silent-literal failure the export model exists to prevent.
func (s CFNStackState) exports() map[string]string {
	if len(s.ExportNames) == 0 {
		return nil
	}
	out := make(map[string]string, len(s.ExportNames))
	for outKey, name := range s.ExportNames {
		if value, ok := s.Outputs[outKey]; ok {
			out[name] = value
		}
	}
	return out
}

// stackTags returns the tags the stack carried, nil-safe so a first deployment — which has no
// previous record at all — reads as the empty set rather than needing a check at every caller.
func (s *CFNStackState) stackTags() map[string]string {
	if s == nil {
		return nil
	}
	return s.Tags
}

// deployedResource returns what the stack recorded for one logical ID, nil-safe for the same
// reason as [CFNStackState.stackTags].
//
// Only a successfully deployed slot is answered: a previously failed resource describes nothing
// this deployment can act on.
func (s *CFNStackState) deployedResource(logicalID string) (DeployedResource, bool) {
	if s == nil {
		return DeployedResource{}, false
	}
	for _, r := range s.Resources {
		if r.LogicalID == logicalID && r.Error == "" {
			return r, true
		}
	}
	return DeployedResource{}, false
}

// cfnContext holds per-deployment resolution context for intrinsic functions.
type cfnContext struct {
	// skipPropertyValidation is set for the rollback of a failed update, which re-deploys a
	// template that may predate property validation and must converge on it regardless (#1605).
	skipPropertyValidation bool

	params     map[string]string           // caller-supplied + defaults
	conditions map[string]bool             // evaluated condition results
	resources  map[string]DeployedResource // logicalID → result for Ref/GetAtt
	region     string
	accountID  string
	stackName  string

	// listParams records which parameters were declared with a list type
	// (CommaDelimitedList, List<Number>, List<AWS::EC2::Subnet::Id>, …). A Ref
	// to one is list-valued, and only the declaration says so: the value is a
	// comma-separated string either way, and a String parameter that happens to
	// contain a comma is a single value.
	listParams map[string]bool

	// conditionExprs holds the template's unevaluated Conditions, so a condition
	// that references another by name can evaluate its referent on demand rather
	// than depending on the order the conditions happened to be walked in.
	conditionExprs map[string]interface{}

	// evaluating tracks the conditions currently being evaluated, so a reference
	// cycle terminates instead of recursing forever.
	evaluating map[string]bool

	// mappings holds the template's Mappings section, which Fn::FindInMap
	// resolves against. Nothing else reads it: a mapping cannot be referenced
	// any other way.
	mappings cfnMappings

	// exports holds the export names visible to this deployment — every value
	// exported by a stack in the same account and Region — which is what
	// Fn::ImportValue resolves against. Loaded once before the first resource
	// deploys, because "cross-stack references are limited to the same account
	// and Region" and nothing a deployment does can add to the set: a stack
	// cannot import a value it exports itself in the same operation.
	exports map[string]string

	// imports records the export names this deployment resolved, which is what
	// makes the exporting stack undeletable — "all the imports must be removed
	// before you can delete the exporting stack". Recorded at resolution time
	// rather than derived from the template afterwards, because an Fn::ImportValue
	// inside a false Fn::If branch or a skipped resource is not an import, and
	// only the resolver knows which of the two it walked.
	imports map[string]bool

	// failures records the resolution errors encountered while deploying the
	// current resource — an Fn::FindInMap naming a key no mapping holds, say.
	//
	// A resolver returns a string, and there is no shape in which it can report
	// "there is no answer": returning the empty string is indistinguishable from
	// a property that legitimately resolved to one, and returning the JSON
	// encoding of the intrinsic is the silent-literal defect (#522). So the
	// failure is collected here and read by deployResource, which records it on
	// the resource — that surfaces as CREATE_FAILED with the reason, the same
	// observable a plugin's own refusal already produces (#519), rather than
	// failing the whole Deploy call and needing #502's typed errors first.
	failures []string

	// priorPhysicalID is the physical ID the previous deployment recorded for the
	// resource being deployed, or "" on its first create. deployResource sets it for
	// each resource, so a create helper can tell a replacement from a first create
	// without drawing from the deployment's mint — a CloudFront distribution's
	// CallerReference is derived from it (#1116; see cfnDistributionCallerReference).
	priorPhysicalID string

	// priorProps is the previous deployment's declaration of the resource being updated in
	// place, resolved against the previous deployment, or nil for a create. An updater reads it
	// for what a removed property used to be — the TTL attribute an UpdateTimeToLive disabling it
	// must name (#1545).
	priorProps map[string]interface{}

	// restoring is set while a rollback re-applies a resource it changed in place: priorProps is
	// then the failed update's declaration, and the declaration being applied is the one the
	// resource had before it.
	restoring bool

	// nameSuffixes holds, by logical ID, the suffix minted for a replacement's generated name;
	// a resource absent from it gets the derived suffix. See [cfnContext.mintReplacementName].
	nameSuffixes map[string]string
}

// mintReplacementName gives logicalID's generated name a fresh suffix for the rest of this
// deployment, drawn from mint so a replay of the request draws the same one (#1545).
//
// A replacement is a new resource: "Recreating the resource generates a new physical ID", and
// CloudFormation names a resource it generates the name for afresh. The derived suffix is the
// resource being replaced, which still exists until the update succeeds, so a replacement under
// the derived name would ask for a name that is taken — an already-exists refusal for a name-keyed
// type, and a failed update.
func (c *cfnContext) mintReplacementName(logicalID string, mint *IDMint) {
	if c.nameSuffixes == nil {
		c.nameSuffixes = map[string]string{}
	}
	c.nameSuffixes[logicalID] = mint.Chars(cfnGeneratedNameSuffixLen, cfnNameSuffixAlphabet)
}

// fail records a resolution failure against the resource being deployed.
func (c *cfnContext) fail(format string, args ...interface{}) {
	c.failures = append(c.failures, fmt.Sprintf(format, args...))
}

// takeFailures returns the failures recorded since the last call and clears
// them, so each resource reports only its own.
func (c *cfnContext) takeFailures() []string {
	out := c.failures
	c.failures = nil
	return out
}

// The sentinel errors a [StackDeployer] operation wraps, so a caller can
// classify a failure with [errors.Is] rather than by matching the message.
//
// The wire plugin has to turn a deployer failure into an AWS error code and an
// HTTP status, and until these existed it did so with strings.Contains over the
// message (#502). That worked and was fragile in two directions. Rewording
// "stack %q not found" — an ordinary copy-edit — silently turned a
// ValidationError at 400 into an InternalFailure at 500 for every consumer, with
// no compiler error and no failing test outside the plugin's own suite. And it
// was lossy the other way: a resource-level deploy failure whose wrapped cause
// happened to contain "not found" (an instance whose AMI does not resolve, say)
// was reported as though the *request* had named something absent.
//
// Each site wraps the sentinel with %w and keeps its message text byte for byte,
// so anything reading a log is unaffected; only the classification changes.
var (
	// ErrCFNStackNotFound is returned when an operation names a stack that does
	// not exist.
	ErrCFNStackNotFound = errors.New("stack not found")

	// ErrCFNChangeSetNotFound is returned when an operation names a change set
	// that does not exist.
	ErrCFNChangeSetNotFound = errors.New("change set not found")

	// ErrCFNDriftDetectionNotFound is returned when a drift-detection ID does
	// not resolve.
	ErrCFNDriftDetectionNotFound = errors.New("drift detection not found")

	// ErrCFNTemplateInvalid is returned when a template body cannot be parsed.
	ErrCFNTemplateInvalid = errors.New("invalid template")

	// ErrCFNInvalidDependency is returned when a template's resources cannot be put in a
	// deploy order: a DependsOn naming a resource the template does not declare, or resources
	// that depend on each other in a cycle (#1420).
	//
	// Both are refusals of the request, made before any resource deploys and before any stack
	// record is written, so a CreateStack leaves no stack and an UpdateStack leaves the stack as
	// it was. The wire maps this to ValidationError at 400 and answers the message verbatim.
	// The two messages are AWS's, each from a published source:
	//
	//   - "Template format error: Unresolved resource dependencies [X] in the Resources block of
	//     the template" is quoted by the AWS re:Post Knowledge Center article on template
	//     validation errors, which gives it as the ValidationError a ValidateTemplate call
	//     answers for a name the template does not declare.
	//   - "Circular dependency between resources: [A, B, ...]" is quoted by the AWS CDK
	//     reference (the aws-cognito module's README) as the error a deployment reports, and
	//     CLI reports show it as the ValidationError CreateStack and CreateChangeSet answer. It
	//     carries no "Template format error:" prefix. Which resources it lists is substrate's
	//     reading of those reports: every resource that cannot be ordered, sorted.
	ErrCFNInvalidDependency = errors.New("invalid resource dependency")

	// ErrCFNStackNotUpdatable is returned when UpdateStack or CreateChangeSet names a stack
	// whose status admits no update. The status codes page publishes the rule for each:
	// ROLLBACK_COMPLETE — "When in this state, only a delete operation can be performed";
	// DELETE_FAILED — "you can't work with or update the stack"; ROLLBACK_FAILED — "Delete the
	// stack". The wire maps it to ValidationError at 400 with the message verbatim, and the
	// message — "Stack:{arn} is in {status} state and can not be updated." — is observed
	// behavior: no AWS page publishes it, and it is the text CLI users report from
	// CreateChangeSet and UpdateStack.
	ErrCFNStackNotUpdatable = errors.New("stack cannot be updated")

	// ErrCFNUnsupportedAttribute is returned when an Output reads, through Fn::GetAtt, an
	// attribute substrate holds no value for (#1429) — one listed in cfnGetAttUnmodelled. It is a
	// refusal of the request, made before any resource deploys, by CreateStack, UpdateStack and
	// CreateChangeSet alike. The wire maps it to ValidationError at 400 and answers the message
	// verbatim: "Template error: resource {LogicalId} does not support attribute type {Attribute}
	// in Fn::GetAtt". The wording is observed behavior — no AWS page publishes it — and it is the
	// ValidationError text user reports quote from CreateStack, in the AWS-org
	// cloudformation-coverage-roadmap issue #1879 and moto issue #3387. AWS answers it for an
	// attribute the type does not publish; substrate answers it for a published attribute it
	// cannot answer, which is the nearest refusal AWS has.
	ErrCFNUnsupportedAttribute = errors.New("unsupported Fn::GetAtt attribute")

	// ErrCFNResourceDeployFailed is returned when a template resource could not
	// be deployed.
	//
	// This is deliberately distinct from ErrCFNTemplateInvalid: the template
	// parsed, so the caller's request was well-formed and the failure is
	// substrate's, which is a 500 rather than a 400.
	ErrCFNResourceDeployFailed = errors.New("resource deploy failed")

	// ErrCFNStateRequired is returned when an operation needs a state manager and
	// the deployer was built without one.
	ErrCFNStateRequired = errors.New("state manager required")

	// ErrCFNExportInUse is returned when a stack cannot be deleted because
	// another stack imports one of its exported output values.
	//
	// The caller's request is what is invalid — "all the imports must be removed
	// before you can delete the exporting stack" — so this is a 400, unlike the
	// deploy failures above.
	ErrCFNExportInUse = errors.New("export in use")

	// ErrCFNExportNameConflict is returned when a stack's Export name is already
	// exported by a different stack in the same account and Region.
	//
	// The API's rule: for each AWS account, Export names must be unique within a
	// Region.
	ErrCFNExportNameConflict = errors.New("export name already exists")

	// ErrCFNDeleteFailed is returned when a stack's resource sweep could not
	// delete one of its resources.
	//
	// The stack survives in DELETE_FAILED with the per-resource reasons recorded,
	// so this is not the end of the caller's options: fix what is holding the
	// resource and delete again. It is substrate's own failure rather than an
	// invalid request, hence a 500 like the deploy failures.
	ErrCFNDeleteFailed = errors.New("stack delete failed")

	// ErrCFNInvalidOnFailure is returned when a create's failure options are not
	// ones CreateStack accepts: an OnFailure outside DO_NOTHING/ROLLBACK/DELETE, a
	// DisableRollback that is not a boolean, or both parameters at once — "you can
	// specify either DisableRollback or OnFailure, but not both".
	//
	// The caller's request is malformed rather than substrate failing, so this maps
	// to a ValidationError at 400.
	ErrCFNInvalidOnFailure = errors.New("invalid stack failure option")

	// ErrCFNInvalidTag is returned when a stack tag breaks one of the limits the
	// `Tag` data type documents — more than 50 entries, a key outside 1-128
	// characters, a value outside 1-256 — or uses the reserved `aws:` prefix.
	//
	// This maps to a ValidationError at 400, and **that code is substrate's choice
	// rather than AWS's**: `CreateStack`'s and `UpdateStack`'s Errors lists name only
	// AlreadyExists, InsufficientCapabilities, LimitExceeded, TokenAlreadyExists and
	// (for update) InvalidChangeSetStatus, none of which describes a malformed tag,
	// and the two precedents already in substrate disagree with each other — EC2
	// invents `InvalidParameterValue` for a reserved prefix while ELBv2 accepts one
	// silently. ValidationError is the code this plugin already answers for every
	// other malformed parameter, so a caller sees one code for one class of mistake.
	ErrCFNInvalidTag = errors.New("invalid stack tag")

	// ErrCFNChangeSetTypeUnsupported is returned when CreateChangeSet asks for a ChangeSetType
	// substrate does not model: IMPORT, which brings existing resources under a stack, or a value
	// outside the documented three (#1606). It maps to ValidationError at 400; the message is
	// substrate's own.
	ErrCFNChangeSetTypeUnsupported = errors.New("change set type not supported")

	// ErrCFNStackAlreadyExists is returned when a CREATE change set names a stack that exists and is
	// not in REVIEW_IN_PROGRESS: "You can't use … the CREATE type to create a change set for an
	// existing stack" (API_CreateChangeSet). It maps to ValidationError at 400 with the message
	// verbatim — "Stack [{name}] already exists and cannot be created again with the changeSet
	// [{changeSet}]." — which is observed behavior: no AWS page publishes it, and it is the
	// ValidationError text SAM CLI and Copilot users report from CreateChangeSet
	// (aws/copilot-cli#1376).
	ErrCFNStackAlreadyExists = errors.New("stack already exists")

	// ErrCFNChangeSetNotExecutable is returned when ExecuteChangeSet names a change set whose
	// ExecutionStatus is not AVAILABLE — one that FAILED, or that has already executed. It maps to
	// InvalidChangeSetStatus at 400, ExecuteChangeSet's documented code for "the specified change set
	// can't be used to update the stack"; the message is substrate's own.
	ErrCFNChangeSetNotExecutable = errors.New("change set not executable")
)

// Stack statuses substrate reports, for the ones written from more than one place.
const (
	// cfnStackDeleteFailed is the status a stack carries after a sweep failed to
	// delete one of its resources. The stack remains in DescribeStacks.
	cfnStackDeleteFailed = "DELETE_FAILED"
)

// cfnClassifiedError carries a [StackDeployer] failure's classification
// alongside its message, so the two can be independent.
//
// Wrapping the sentinel into the message with a second %w would have worked for
// errors.Is and changed the text every consumer's logs already carry — and
// "message unchanged" is the property that makes this refactor safe to land on
// its own. Holding the class in a field instead means a message may be reworded
// freely without moving the wire code, which was the whole complaint.
type cfnClassifiedError struct {
	class error
	msg   string
	cause error
}

// Error implements the error interface.
func (e *cfnClassifiedError) Error() string { return e.msg }

// Unwrap returns the wrapped cause, or the classification when there is none, so
// errors.Is finds the class either way.
func (e *cfnClassifiedError) Unwrap() error {
	if e.cause != nil {
		return e.cause
	}
	return e.class
}

// Is reports whether the error carries the given classification.
func (e *cfnClassifiedError) Is(target error) bool { return target == e.class }

// cfnErrf builds a classified deployer error whose message is exactly format
// applied to args. A %w in format is unwrapped as usual, so a parse failure's
// cause survives underneath the classification.
func cfnErrf(class error, format string, args ...interface{}) error {
	wrapped := fmt.Errorf(format, args...)
	return &cfnClassifiedError{
		class: class,
		msg:   wrapped.Error(),
		cause: errors.Unwrap(wrapped),
	}
}

// CFNResourceDeployError reports a template resource that could not be deployed,
// naming the resource so a caller need not re-parse the message to find it.
//
// DescribeStackResources reads the failure off the resource record rather than
// this error, but a deploy that fails outright returns before any record is
// written — so the logical ID has to travel with the error or it is lost.
type CFNResourceDeployError struct {
	// LogicalID is the template logical ID of the resource that failed.
	LogicalID string

	// Err is the underlying failure.
	Err error
}

// Error implements the error interface.
func (e *CFNResourceDeployError) Error() string {
	return fmt.Sprintf("deploy resource %s: %v", e.LogicalID, e.Err)
}

// Unwrap returns the underlying failure.
func (e *CFNResourceDeployError) Unwrap() error { return e.Err }

// Is reports that a CFNResourceDeployError matches ErrCFNResourceDeployFailed,
// so a caller can classify it without also matching whatever the wrapped cause
// happens to be.
func (e *CFNResourceDeployError) Is(target error) bool { return target == ErrCFNResourceDeployFailed }

// cfnNamespace is the state namespace for CloudFormation stack state.
const cfnNamespace = "cfn"

// cfnStubNamespace is the state namespace for generic CFN stub resource props.
const cfnStubNamespace = "cfn_stub"

// CFNChangeSet describes a pending set of changes to a CloudFormation stack.
type CFNChangeSet struct {
	// ChangeSetName is the name of the change set.
	ChangeSetName string `json:"ChangeSetName"`

	// StackName is the target stack.
	StackName string `json:"StackName"`

	// Status is the change set status (e.g., "CREATE_COMPLETE").
	Status string `json:"Status"`

	// TemplateBody is the proposed template.
	TemplateBody string `json:"TemplateBody"`

	// Parameters holds the proposed parameter values.
	Parameters map[string]string `json:"Parameters"`

	// Changes lists the resource-level changes.
	Changes []CFNResourceChange `json:"Changes"`

	// Tags holds the stack-level tags the change set was created with, nil when it was
	// created without the parameter (#824).
	//
	// `CreateChangeSet` documents the member exactly as `CreateStack` does — "key-value
	// pairs to associate with this stack. CloudFormation also propagates these tags to
	// resources in the stack" — and `DescribeChangeSet` says what executing it means:
	// "if you execute the change set, the tags that will be associated with the stack".
	// So the set is recorded here and applied by [StackDeployer.ExecuteChangeSet].
	//
	// **Not `omitempty`**, unlike [CFNStackState.Tags]. The three-way distinction the
	// decoder draws — absent, empty, populated — has to survive the round trip through
	// state, because it is read back at execution and handed to `UpdateStack`, where nil
	// preserves the stack's tags and an empty non-nil map clears them. `omitempty` drops
	// an empty map, so a change set created with `Tags=` would read back as one that
	// never mentioned tags and preserve where the caller asked to clear. A record
	// written before #824 carries no member at all and decodes to nil, which is the
	// omitted case and the behavior that shipped.
	Tags map[string]string `json:"Tags"`

	// CreatedAt is the creation timestamp.
	CreatedAt time.Time `json:"CreatedAt"`

	// The members below record what CreateChangeSet was asked for beyond the template, its
	// parameters and its tags, and where the change set is in its lifecycle (#1606). Each is
	// omitted from a record written before #1606, which decodes as an UPDATE change set that is
	// CREATE_COMPLETE and AVAILABLE — what every such record was. See cfn_changeset_lifecycle.go.

	// Type is the ChangeSetType it was created with, CREATE or UPDATE; empty is UPDATE, the
	// parameter's documented default.
	Type string `json:"ChangeSetType,omitempty"`

	// ExecutionStatus is the change set's ExecutionStatus; empty is AVAILABLE for a
	// CREATE_COMPLETE change set and UNAVAILABLE otherwise.
	ExecutionStatus string `json:"ExecutionStatus,omitempty"`

	// StatusReason is "a description of the change set's status", set for a FAILED one.
	StatusReason string `json:"StatusReason,omitempty"`

	// Description is the caller's own description of the change set.
	Description string `json:"Description,omitempty"`

	// RoleARN is the service role executing the change set runs as, empty for none: "CloudFormation
	// uses this role for all future operations on the stack".
	RoleARN string `json:"RoleARN,omitempty"`

	// Capabilities are the capabilities the caller acknowledged, as DescribeChangeSet reports them.
	Capabilities []string `json:"Capabilities,omitempty"`

	// OnStackFailure is what a failed execution of a CREATE change set does: DO_NOTHING, ROLLBACK or
	// DELETE. Empty leaves it to ExecuteChangeSet's DisableRollback.
	OnStackFailure string `json:"OnStackFailure,omitempty"`
}

// CFNResourceChange describes a single resource-level change in a change set.
type CFNResourceChange struct {
	// Action is "Add", "Modify", or "Remove".
	Action string `json:"Action"`

	// LogicalID is the CloudFormation logical resource ID.
	LogicalID string `json:"LogicalResourceId"`

	// ResourceType is the CloudFormation resource type.
	ResourceType string `json:"ResourceType"`

	// Replacement is "True", "False", or "Conditional" for Modify actions.
	Replacement string `json:"Replacement,omitempty"`

	// PhysicalID is the resource's physical ID, for a Modify or Remove of a resource the
	// stack has deployed. An Add has none: "Resources that you are adding don't have
	// physical IDs because they haven't been created." A record written before #1446
	// carries none either, and renders without the member.
	PhysicalID string `json:"PhysicalResourceId,omitempty"`

	// Scope is "which resource attribute is triggering this update", for a Modify: the
	// distinct Target.Attribute values of Details. Empty for Add and Remove.
	Scope []string `json:"Scope,omitempty"`

	// Details is the list of ResourceChangeDetail structures for a Modify (#1446).
	Details []CFNResourceChangeDetail `json:"Details,omitempty"`
}

// CFNResourceChangeDetail is one ResourceChangeDetail of a Modify: the field that will
// change, whether CloudFormation can determine its value now, and what caused it.
type CFNResourceChangeDetail struct {
	// Target is the field that will change.
	Target CFNResourceTargetDefinition `json:"Target"`

	// Evaluation is "Static" when the new value is known before execution and "Dynamic"
	// when it depends on an intrinsic resolved during the update.
	Evaluation string `json:"Evaluation"`

	// ChangeSource is the group CausingEntity belongs to: "DirectModification",
	// "ParameterReference", "ResourceReference" or "ResourceAttribute" — the four the
	// emulator derives.
	ChangeSource string `json:"ChangeSource"`

	// CausingEntity names the parameter, resource or resource attribute that caused the
	// change; empty for a DirectModification, as published.
	CausingEntity string `json:"CausingEntity,omitempty"`
}

// CFNResourceTargetDefinition is a ResourceTargetDefinition: which attribute of the
// resource changes, the property name when the attribute is Properties, and whether
// the change recreates the resource.
type CFNResourceTargetDefinition struct {
	// Attribute is one of Properties, Metadata, CreationPolicy, UpdatePolicy,
	// DeletionPolicy, UpdateReplacePolicy or Tags.
	Attribute string `json:"Attribute"`

	// Name is the property name when Attribute is Properties, and empty otherwise.
	Name string `json:"Name,omitempty"`

	// RequiresRecreation is "Never", "Conditionally" or "Always".
	RequiresRecreation string `json:"RequiresRecreation,omitempty"`
}

// CFNDriftResult holds the result of a stack drift detection operation.
type CFNDriftResult struct {
	// StackName is the name of the stack.
	StackName string `json:"StackName"`

	// DriftStatus is "IN_SYNC", "DRIFTED", or "NOT_CHECKED".
	DriftStatus string `json:"StackDriftStatus"`

	// DriftedCount is the number of drifted resources.
	DriftedCount int `json:"DriftedResourceCount"`

	// ResourceDrifts lists per-resource drift entries.
	ResourceDrifts []CFNResourceDriftEntry `json:"ResourceDrifts"`

	// DetectedAt is the time drift was detected.
	DetectedAt time.Time `json:"DetectedAt"`
}

// CFNResourceDriftEntry describes the drift status of a single stack resource.
type CFNResourceDriftEntry struct {
	// LogicalID is the CloudFormation logical resource ID.
	LogicalID string `json:"LogicalResourceId"`

	// ResourceType is the CloudFormation resource type.
	ResourceType string `json:"ResourceType"`

	// PhysicalID is the actual resource identifier.
	PhysicalID string `json:"PhysicalResourceId"`

	// DriftStatus is "IN_SYNC", "MODIFIED", "DELETED", or "NOT_CHECKED".
	DriftStatus string `json:"StackResourceDriftStatus"`

	// PropertyDifferences lists per-property differences when DriftStatus is
	// "MODIFIED". It is empty for all other statuses.
	PropertyDifferences []CFNPropertyDiff `json:"PropertyDifferences,omitempty"`
}

// CFNPropertyDiff describes a single property-level difference between the
// expected (template) and actual (live) value of a drifted resource.
type CFNPropertyDiff struct {
	// PropertyPath is the JSON-path-like path to the differing property.
	PropertyPath string `json:"PropertyPath"`

	// ExpectedValue is the value defined by the template.
	ExpectedValue string `json:"ExpectedValue"`

	// ActualValue is the value found in live service state.
	ActualValue string `json:"ActualValue"`

	// DifferenceType is "ADD", "REMOVE", or "NOT_EQUAL".
	DifferenceType string `json:"DifferenceType"`
}

// CFNDriftDetectionStatus holds the status of an asynchronous drift-detection
// operation started by [StackDeployer.StartStackDriftDetection].
type CFNDriftDetectionStatus struct {
	// StackDriftDetectionID is the ID returned by StartStackDriftDetection.
	StackDriftDetectionID string `json:"StackDriftDetectionId"`

	// StackName identifies the stack the detection ran against.
	StackName string `json:"StackId"`

	// DetectionStatus is "DETECTION_IN_PROGRESS", "DETECTION_COMPLETE", or
	// "DETECTION_FAILED".
	DetectionStatus string `json:"DetectionStatus"`

	// StackDriftStatus is "DRIFTED", "IN_SYNC", or "NOT_CHECKED" once detection
	// completes.
	StackDriftStatus string `json:"StackDriftStatus"`

	// DriftedStackResourceCount is the number of drifted resources.
	DriftedStackResourceCount int `json:"DriftedStackResourceCount"`

	// Timestamp is when the detection record was last updated.
	Timestamp time.Time `json:"Timestamp"`
}

// typePriority breaks ties in the deploy order between resources that do not depend on
// each other. Lower numbers deploy first, and an unlisted type is 99.
//
// It was the whole of the order until #1420, which is why its values read as a
// dependency ranking. The order is now the dependency order of cfn_dependency_order.go,
// and a priority decides only which of the resources whose dependencies have all
// deployed goes next — so a template whose references the priorities already agreed
// with deploys exactly as it did, and one whose references they contradicted follows
// the references.
var typePriority = map[string]int{
	// The two policy types follow the principals they are embedded in or attached to, so a
	// template naming a stack's role, user or group by a literal name rather than a Ref still
	// finds it (#1562, #1560). AWS::IAM::Policy was 0 while it deployed as an unattached
	// managed policy, when it named nothing.
	"AWS::IAM::Group":           1,
	"AWS::IAM::User":            2,
	"AWS::IAM::Policy":          3,
	"AWS::IAM::ManagedPolicy":   3,
	"AWS::IAM::Role":            1,
	"AWS::EC2::VPC":             1,
	"AWS::Route53::HostedZone":  1,
	"AWS::KMS::Key":             1,
	"AWS::DynamoDB::Table":      2,
	"AWS::S3::Bucket":           2,
	"AWS::S3::BucketPolicy":     3,
	"AWS::EC2::Subnet":          2,
	"AWS::EC2::SecurityGroup":   2,
	"AWS::EC2::InternetGateway": 2,
	"AWS::IAM::InstanceProfile": 2,
	"AWS::EC2::LaunchTemplate":  3,
	// Standalone security-group rules must follow every group they reference, so
	// a self-referencing or mutually-referencing pair resolves (#388).
	"AWS::EC2::SecurityGroupIngress":              3,
	"AWS::EC2::SecurityGroupEgress":               3,
	"AWS::KMS::Alias":                             2,
	"AWS::KMS::ReplicaKey":                        2,
	"AWS::SecretsManager::Secret":                 2,
	"AWS::SSM::Parameter":                         2,
	"AWS::Lambda::Function":                       3,
	"AWS::SQS::Queue":                             3,
	"AWS::EC2::RouteTable":                        3,
	"AWS::EC2::Instance":                          3,
	"AWS::ElasticLoadBalancingV2::TargetGroup":    3,
	"AWS::ElasticLoadBalancingV2::LoadBalancer":   3,
	"AWS::ElasticLoadBalancing::LoadBalancer":     3,
	"AWS::SNS::Topic":                             3,
	"AWS::ElasticLoadBalancingV2::Listener":       4,
	"AWS::ElasticLoadBalancingV2::ListenerRule":   5,
	"AWS::Route53::RecordSet":                     4,
	"AWS::Route53::RecordSetGroup":                4,
	"AWS::SNS::Subscription":                      4,
	"AWS::SNS::TopicPolicy":                       4,
	"AWS::SecretsManager::RotationSchedule":       5,
	"AWS::SecretsManager::SecretTargetAttachment": 5,
	"AWS::SSM::Association":                       5,
	"AWS::Logs::LogGroup":                         2,
	"AWS::Logs::LogStream":                        3,
	"AWS::Events::Rule":                           4,
	"AWS::CloudWatch::Alarm":                      4,
	// v0.19.0 — API Gateway and ACM.
	"AWS::CertificateManager::Certificate": 1,
	"AWS::ApiGateway::RestApi":             2,
	"AWS::ApiGateway::Authorizer":          3,
	"AWS::ApiGateway::Resource":            3,
	"AWS::ApiGateway::ApiKey":              3,
	"AWS::ApiGateway::Method":              4,
	"AWS::ApiGateway::Deployment":          4,
	"AWS::ApiGateway::UsagePlan":           4,
	"AWS::ApiGateway::Stage":               5,
	"AWS::ApiGateway::UsagePlanKey":        5,
	"AWS::ApiGatewayV2::Api":               2,
	"AWS::ApiGatewayV2::Authorizer":        3,
	"AWS::ApiGatewayV2::Integration":       3,
	"AWS::ApiGatewayV2::Route":             3,
	"AWS::ApiGatewayV2::Stage":             4,
	// v0.20.0 — Step Functions.
	"AWS::StepFunctions::Activity":     3,
	"AWS::StepFunctions::StateMachine": 4,
	// v0.21.0 — ECS and ECR.
	"AWS::ECR::Repository":       2,
	"AWS::ECS::Cluster":          2,
	"AWS::ECS::CapacityProvider": 3,
	"AWS::ECS::TaskDefinition":   3,
	"AWS::ECS::Service":          5,
	// v0.22.0 — Cognito.
	"AWS::Cognito::UserPool":                   2,
	"AWS::Cognito::IdentityPool":               2,
	"AWS::Cognito::UserPoolClient":             3,
	"AWS::Cognito::UserPoolGroup":              3,
	"AWS::Cognito::UserPoolDomain":             4,
	"AWS::Cognito::IdentityPoolRoleAttachment": 4,
	// v0.23.0 — Kinesis and CloudFront.
	"AWS::Kinesis::Stream":                            2,
	"AWS::CloudFront::CloudFrontOriginAccessIdentity": 2,
	"AWS::CloudFront::Distribution":                   3,
	// v0.25.0 — RDS and ElastiCache.
	"AWS::RDS::DBSubnetGroup":            2,
	"AWS::RDS::DBParameterGroup":         2,
	"AWS::RDS::DBCluster":                3,
	"AWS::RDS::DBInstance":               3,
	"AWS::ElastiCache::SubnetGroup":      2,
	"AWS::ElastiCache::ParameterGroup":   2,
	"AWS::ElastiCache::CacheCluster":     3,
	"AWS::ElastiCache::ReplicationGroup": 3,
	// v0.26.0 — EFS and Glue.
	"AWS::EFS::FileSystem":  2,
	"AWS::EFS::AccessPoint": 3,
	"AWS::EFS::MountTarget": 4,
	"AWS::Glue::Database":   2,
	"AWS::Glue::Connection": 2,
	"AWS::Glue::Table":      3,
	"AWS::Glue::Crawler":    3,
	"AWS::Glue::Job":        3,
	// v0.27.0 — Budgets.
	"AWS::Budgets::Budget": 3,
	// v0.28.0 — SES v2 and Firehose.
	"AWS::SES::EmailIdentity":              2,
	"AWS::KinesisFirehose::DeliveryStream": 3,
	// v0.41.0 — Elastic IPs and NAT Gateways.
	"AWS::EC2::EIP":        2,
	"AWS::EC2::NatGateway": 4,
	// v0.43.0 — FSx.
	"AWS::FSx::FileSystem": 3,
	// v0.30.0 — Lambda ESM.
	"AWS::Lambda::EventSourceMapping": 5,
	// v0.31.0 — AppSync.
	"AWS::AppSync::GraphQLApi":            2,
	"AWS::AppSync::DataSource":            3,
	"AWS::AppSync::Resolver":              4,
	"AWS::AppSync::FunctionConfiguration": 4,
	// v0.34.0 — RDS Aurora cluster and MSK.
	"AWS::MSK::Cluster": 3,
	// v0.32.0 — extended CFN stubs.
	"AWS::OpenSearchService::Domain":   2,
	"AWS::Elasticsearch::Domain":       2,
	"AWS::CodeDeploy::Application":     2,
	"AWS::WAFv2::WebACL":               2,
	"AWS::Backup::BackupPlan":          2,
	"AWS::Backup::BackupSelection":     3,
	"AWS::CodeBuild::Project":          2,
	"AWS::CodePipeline::Pipeline":      3,
	"AWS::CodeDeploy::DeploymentGroup": 3,
	"AWS::CloudTrail::Trail":           2,
	// AWS Config's three types carry an ordering the service enforces, and a
	// template need not declare it with DependsOn. "Before you can create a delivery
	// channel, you must create a configuration recorder", so the channel follows the
	// recorder; PutConfigRule refuses without a recorder, and the rule's own page says
	// the recorder must be running first, so the rule is last of the three. Since #1420
	// the order is carried by cfnImpliedDependencies as dependency edges, which hold
	// whatever else delays the recorder; these priorities are the tie-break that agrees
	// with them.
	"AWS::Config::ConfigurationRecorder": 2,
	"AWS::Config::DeliveryChannel":       3,
	"AWS::Config::ConfigRule":            4,
	"AWS::Transfer::Server":              2,
	"AWS::Athena::WorkGroup":             2,
}

// cfnIdentity is the AWS account and region a StackDeployer deploys into.
//
// Two strings rather than a whole *RequestContext: a deployer that carried one
// would carry a single request's ID and timestamp across every resource it
// deploys, which is wrong — each dispatch generates its own. The separate
// decision this comment used to defer — how a stack-deployed request is
// authorized — is now [WithDeployerPrincipal], which is deliberately its own
// option: identity says which partition a resource is written into, the principal
// says whose permissions creating it required, and a stack can change the second
// per operation (a service role) without touching the first.
type cfnIdentity struct {
	accountID string
	region    string
}

// StackDeployer parses and deploys a CloudFormation template using in-process
// plugin dispatch.
type StackDeployer struct {
	registry *PluginRegistry
	store    *EventStore
	state    StateManager
	tc       *TimeController
	logger   Logger
	costs    *CostController

	// identity is the account and region every request this deployer dispatches
	// is made under, and which the AWS::AccountId and AWS::Region
	// pseudo-parameters resolve to. It defaults to substrate's own defaults; see
	// WithDeployerIdentity for why a caller would set it.
	identity cfnIdentity

	// attribution is who every request this deployer dispatches is made as. Zero
	// means unauthorized, which is what every in-process caller was before it
	// existed. See WithDeployerAttribution.
	attribution cfnAttribution

	// auth evaluates each dispatched request against principal's policies, or is
	// nil to authorize nothing. Taken from PluginConfig.Options["auth_controller"]
	// the same way event_store and cost_controller are.
	auth *AuthController

	// mint derives the request id of every request this deployment dispatches, and
	// so — transitively — every identifier of every resource in the stack. Nil
	// means the deployment's internal request ids are drawn from the wall clock,
	// which is what they all were before #856's tier 8. See WithDeployerMint.
	mint *IDMint

	// lenient deploys a resource type substrate does not emulate as a stub instead of
	// refusing the template. False — strict — is the default (#1605). See
	// WithDeployerLenient.
	lenient bool
}

// StackDeployerOption configures optional behavior of a [StackDeployer].
type StackDeployerOption func(*StackDeployer)

// WithDeployerIdentity sets the AWS account and region the deployer deploys
// into.
//
// Without it a deployer uses substrate's default account and region, which is
// right for an in-process caller that never signs a request but wrong for one
// reached over the wire: a request signed for another account created a stack
// whose ARN named that account while every resource in it was written into
// substrate's default partition. Most state keys embed the account and region
// (EC2 instances, ECS clusters, log groups), so those resources were invisible
// to the very caller that had just created them — DescribeInstances correctly
// reported nothing.
//
// Passing the caller's identity is what keeps the stack ARN and the resources
// inside it in one partition.
func WithDeployerIdentity(accountID, region string) StackDeployerOption {
	return func(d *StackDeployer) {
		d.identity = cfnIdentity{accountID: accountID, region: region}
	}
}

// WithDeployerMint derives this deployment's internal request ids from m, which is
// the mint of the request that asked for the deployment.
//
// A stack's resources are created by requests substrate builds in this package and
// dispatches through the registry, and each of those requests mints its resource's
// identifier from its own request id. Those ids came from generateRequestID, which
// is the wall clock, so **every identifier in every CloudFormation stack was
// undirected randomness** — which is the part of #856 the draw-site count never
// showed, because the deployer contains no `rand.Read` call of its own.
//
// Deriving them fixes two different failures at once. A replayed *internal* event
// reproduces its recording because the recorded request id is the seed, which is
// true of any recorded event (see replayEvent). And a replayed outer `CreateStack`
// — which re-runs the whole deployment, since the create is itself a recorded
// event — reproduces it too, because the re-run derives the same sequence of
// internal ids from the same outer request. The second is why the option exists:
// without it the two halves of a replayed CloudFormation stream disagreed with each
// other, not merely with the recording.
//
// The sequence is reproducible because [StackDeployer.DeployWithOptions] sorts its
// resources by type priority and then by logical id before dispatching any of them.
// A deployment is single-goroutine, so the mint's ordinal advances in that order.
func WithDeployerMint(m *IDMint) StackDeployerOption {
	return func(d *StackDeployer) {
		d.mint = m
	}
}

// cfnAttribution is who a stack's resource calls are made as: a service role if
// the stack has one, otherwise the identity that created it.
//
// Both are carried rather than one resolved principal because they are not
// interchangeable and the difference is observable. A roleARN is the answer for
// every future operation on the stack whoever asks for it, while creator is only
// the answer while the stack has no role — so an update that attaches a role must
// change what the stack's calls are attributed to, and one that omits it must not.
// Resolving in one place ([cfnAttribution.resolve]) is what keeps that rule from
// being restated at each call site.
type cfnAttribution struct {
	roleARN string
	creator *Principal
}

// resolve returns the principal to dispatch as, or nil for unauthorized.
//
// The service role wins, because that is what a service role is: CloudFormation
// "always uses this role for all future operations on the stack". A roleARN is used
// directly as a role principal — the evaluator's role path resolves it — rather
// than run through AssumeRole, since the observation being modeled is that a call
// was denied, not role-assumption machinery.
func (a cfnAttribution) resolve() *Principal {
	if a.roleARN != "" {
		return &Principal{ARN: a.roleARN, Type: cfnPrincipalType(a.roleARN)}
	}
	return a.creator
}

// WithDeployerAttribution sets who the deployer's resource calls are made as, and
// therefore whose policies they are authorized against.
//
// CloudFormation does not create resources as itself: absent a service role it uses
// "a temporary session that's generated from your user credentials", and with one it
// "always uses this role for all future operations on the stack". So a stack's
// resource calls have an answerable principal, and modeling it is what makes a
// template asking for something the deploying identity cannot do fail the way it
// fails on AWS — the failure class ("forgot a permission / policy too narrow") that
// a green composition test could not catch, because a stack's calls carried no
// principal at all and PluginRegistry.RouteRequest never authorizes.
//
// Both zero, the default, dispatches unauthorized, which is what every in-process
// caller did before this option existed. That default is deliberate rather than
// conservative: authorization only means something once a caller has said which
// identity a deployment is on behalf of, and a deployer a test constructs directly
// has not.
func WithDeployerAttribution(roleARN string, creator *Principal) StackDeployerOption {
	return func(d *StackDeployer) {
		d.attribution = cfnAttribution{roleARN: roleARN, creator: creator}
	}
}

// WithDeployerAuth sets the controller that authorizes each dispatched request.
//
// Separate from [WithDeployerAttribution] because the two arrive from different
// places: attribution is a property of one stack operation, while the controller is
// a server-lifetime dependency the CloudFormation plugin receives in its
// PluginConfig. Nil authorizes nothing, so a deployer given an attribution and no
// controller records who deployed a stack without enforcing it.
func WithDeployerAuth(auth *AuthController) StackDeployerOption {
	return func(d *StackDeployer) {
		d.auth = auth
	}
}

// WithDeployerLenient turns strict mode off: a template declaring a resource type substrate
// does not emulate, or a custom resource, deploys that resource as a stub that records its
// properties and creates nothing, instead of being refused with [ErrCFNUnemulatedType].
//
// Strict is the default because a stub reports CREATE_COMPLETE for a resource nothing
// created, so a test passes against infrastructure that does not exist (#1605). The two
// refusals CloudFormation itself makes — an unrecognized type, an extraneous property —
// apply in both modes.
func WithDeployerLenient(lenient bool) StackDeployerOption {
	return func(d *StackDeployer) {
		d.lenient = lenient
	}
}

// NewStackDeployer creates a StackDeployer wired to the provided dependencies.
//
// The deployer deploys into substrate's default account and region unless
// [WithDeployerIdentity] says otherwise, and dispatches unauthorized unless both
// [WithDeployerPrincipal] and [WithDeployerAuth] say otherwise.
func NewStackDeployer(registry *PluginRegistry, store *EventStore, state StateManager, tc *TimeController, logger Logger, costs *CostController, opts ...StackDeployerOption) *StackDeployer {
	d := &StackDeployer{
		registry: registry,
		store:    store,
		state:    state,
		tc:       tc,
		logger:   logger,
		costs:    costs,
		identity: cfnIdentity{accountID: defaultAccountID, region: defaultRegion},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Deploy parses cfn and deploys all resources, returning a DeployResult.
// Resources are deployed in type-priority order. Unknown resource types are
// skipped with a warning. The optional params map overrides template parameter
// defaults.
//
// A resource that fails rolls the stack back, which is CreateStack's default: "although
// the default setting is ROLLBACK". Use [StackDeployer.DeployWithOptions] to ask for
// DO_NOTHING or DELETE instead.
func (d *StackDeployer) Deploy(ctx context.Context, cfn, streamID string, params map[string]string) (*DeployResult, error) {
	return d.DeployWithOptions(ctx, cfn, streamID, params, CFNDeployOptions{})
}

// DeployWithOptions is [StackDeployer.Deploy] with the stack-level failure options a
// create was requested with.
//
// Deploy remains the whole of the API for the succeeding case; this exists because
// what happens to a *failed* create is the caller's decision, not substrate's, and
// the zero CFNDeployOptions is CloudFormation's own default.
func (d *StackDeployer) DeployWithOptions(
	ctx context.Context, cfn, streamID string, params map[string]string, opts CFNDeployOptions,
) (*DeployResult, error) {
	result, _, err := d.deploy(ctx, cfn, streamID, params, opts, nil, nil)
	return result, err
}

// cfnDeployOutcome is what a deploy did beyond what its [DeployResult] reports, for the
// update and rollback paths that act on it afterwards.
type cfnDeployOutcome struct {
	// templateBody and params are what the deploy ran, so a rollback can delete what it
	// created with the declarations it created them from.
	templateBody string
	params       map[string]string

	// dispatched names every resource the deploy made a call for. A resource absent from it
	// was kept from the baseline unchanged.
	dispatched map[string]bool

	// failures is why the deploy failed, empty when it succeeded: each resource that failed,
	// then each Output that did not resolve (#1429). An Output failure leaves every resource
	// deployed, so the resources alone cannot say the deploy failed.
	failures []string
}

// deploy runs a template against the stack's previous deployment, or against base when
// the caller supplies one, and applies force as [cfnBaseline.force].
//
// base exists for the rollback of a failed update, which must converge on the record the
// update started from rather than on the failed update's own record — see
// [StackDeployer.rollbackFailedUpdate].
func (d *StackDeployer) deploy(
	ctx context.Context, cfn, streamID string, params map[string]string, opts CFNDeployOptions,
	base *CFNStackState, force map[string]map[string]interface{},
) (*DeployResult, *cfnDeployOutcome, error) {
	if err := opts.validate(); err != nil {
		return nil, nil, err
	}
	tmpl, err := d.parseCFNTemplate(cfn)
	if err != nil {
		return nil, nil, cfnErrf(ErrCFNTemplateInvalid, "parse template: %w", err)
	}

	stackName := streamID
	start := d.tc.Now()

	// Build resolution context.
	cctx := buildCFNContext(tmpl, params, d.identity.region, d.identity.accountID, stackName)
	evaluateConditions(tmpl, cctx)

	// Load the exports Fn::ImportValue may resolve against, before the first
	// resource deploys. The stack being redeployed is excluded: its own exports
	// are not importable by itself, and leaving them in would let a template
	// import a name it is in the middle of redefining.
	cctx.exports = d.loadExports(ctx, stackName)

	// The deploy order is the dependency order (#1420), and a template whose resources
	// cannot be ordered is refused here, before any resource deploys and before any record
	// is written — the request fails, as CloudFormation's does.
	//
	// A rollback (base set) is the exception: it re-deploys the previous template, which may
	// predate that validation, and must converge on it regardless, so it orders tolerantly,
	// as the delete path does.
	//
	// rollback is read before base is defaulted below: a rollback re-deploys the template the
	// stack had, so nothing it does is a removal, and a resource the failed update dropped is
	// still the stack's (#1547).
	rollback := base != nil
	// The previous template may predate property validation (#1605), and a rollback has to
	// converge on it, so its resources are not held to it.
	cctx.skipPropertyValidation = rollback
	var order []string
	if base == nil {
		if order, err = d.cfnValidateTemplate(tmpl, cctx); err != nil {
			return nil, nil, err
		}
	} else {
		order = cfnTolerantDeployOrder(tmpl, cctx)
	}
	for logicalID, res := range tmpl.Resources {
		if res.Condition != "" {
			if val, ok := cctx.conditions[res.Condition]; ok && !val {
				d.logger.Info("cfn: skipping resource due to false condition",
					"logical_id", logicalID, "condition", res.Condition)
			}
		}
	}

	// The previous record is what an update is compared against (#1427), and what tells a
	// tag the stack propagated from one the caller set directly. A rollback compares against
	// the record the failed update started from instead, but its tags are reconciled against
	// what is persisted now — the failed update's — because those are what it propagated.
	prev, prevErr := d.previousStack(ctx, stackName)
	if prevErr != nil {
		prev = nil
	}
	if base == nil {
		base = prev
	}
	// The stack's creation outlives every later deployment of it (#1606): its CreationTime is when
	// it was first created, and a stack a CREATE change set made keeps the REVIEW_IN_PROGRESS start
	// its first event reports. A stack still in review has not been deployed, so the deploy that
	// executes its change set is its creation.
	createdAt, reviewedAt := cfnCarriedCreation(prev, start)
	baseline := d.newCFNBaseline(base, cctx.exports, force)
	outcome := &cfnDeployOutcome{templateBody: cfn, params: cctx.params,
		dispatched: map[string]bool{}}

	resources := make([]DeployedResource, 0, len(order))
	var (
		totalCost float64
		replaced  []DeployedResource
	)
	for _, logicalID := range order {
		res := tmpl.Resources[logicalID]
		plan := baseline.plan(logicalID, res, cctx)
		if plan.keep {
			// Unchanged, and so is everything it depends on: the previous resource stands and
			// no call is made for it.
			cctx.resources[logicalID] = plan.prior
			resources = append(resources, plan.prior)
			continue
		}
		outcome.dispatched[logicalID] = true
		if plan.replacement && cfnUsesGeneratedName(res.Type, res.Properties, cctx) {
			// The replacement is a new resource and gets a new generated name, as AWS's does;
			// the derived one belongs to the resource being replaced (#1545). Only a resource
			// substrate names draws from the mint, so a replacement of a template-named or
			// logical-ID-named resource leaves every later identifier where it was.
			cctx.mintReplacementName(logicalID, d.mint)
		}
		dr, cost, deployErr := d.deployResource(ctx, logicalID, res, streamID, cctx, plan)
		if deployErr != nil {
			return nil, nil, &CFNResourceDeployError{LogicalID: logicalID, Err: deployErr}
		}
		if plan.clearsRefusal(res, dr) {
			// The re-create was refused only because the resource it would have created is the
			// one this stack already holds. Not a failure of this deployment, and the previous
			// record is still the resource.
			d.logger.Debug("cfn: resource already exists and its declaration is unchanged; "+
				"not a failure of this deployment", "logical_id", logicalID, "type", res.Type)
			dr = plan.prior
		}
		if plan.replacedBy(dr) {
			replaced = append(replaced, plan.prior)
		}
		totalCost += cost
		cctx.resources[logicalID] = dr
		resources = append(resources, dr)
	}

	// The stack's own tags reach the resources here, before the stack's fate is decided.
	// Before, because a resource that deployed carries the stack's tags whatever becomes of
	// the stack — which is what the three-key stamp already does, and a rollback is about to
	// delete the records either way. The previous tag set is what tells a tag the stack
	// propagated from one the caller set directly; on a first create there is none, and every
	// key is new.
	tagFailures := d.reconcileStackTags(resources, prev, opts.Tags)

	// Resolve outputs, and with them the export names this stack publishes.
	outputs := make(map[string]string)
	exportNames := make(map[string]string)
	outKeys := make([]string, 0, len(tmpl.Outputs))
	for outKey := range tmpl.Outputs {
		outKeys = append(outKeys, outKey)
	}
	// Sorted so that a template declaring two outputs with the same export name
	// reports the same one as the conflict every time.
	sort.Strings(outKeys)

	// An Output that does not resolve fails the stack, as a resource that does not resolve
	// fails itself (#1429): an Fn::ImportValue of no export, an Fn::FindInMap of no entry, an
	// Fn::Cidr out of range or a refused Fn::GetAtt would otherwise publish "" — and export it,
	// handing an importer an empty ID. An Fn::GetAtt substrate knows it cannot answer is refused
	// at the request instead, by cfnValidateOutputAttributes, so what reaches here is a failure
	// that depends on what deployed. The reason names the Output; the text is substrate's own.
	cctx.takeFailures()
	var outputFailures []string
	for _, outKey := range outKeys {
		outVal := tmpl.Outputs[outKey]
		if !cfnOutputActive(outVal, cctx) {
			// "To associate a condition with an output, define the condition in the Conditions
			// section of the template": an Output whose condition is false is not created, so
			// it is neither resolved nor published.
			continue
		}
		outputs[outKey] = resolveValue(outVal.Value, cctx)
		if outVal.Export != nil {
			if name := resolveValue(outVal.Export.Name, cctx); name != "" {
				exportNames[outKey] = name
			}
		}
		for _, f := range cctx.takeFailures() {
			outputFailures = append(outputFailures, fmt.Sprintf("Output %s: %s", outKey, f))
		}
	}
	failures := cfnFailedResources(resources)
	failures = append(failures, outputFailures...)
	outcome.failures = failures
	pending := CFNStackState{Outputs: outputs, ExportNames: exportNames}
	if len(outputFailures) == 0 {
		// An Output that did not resolve fails the stack, which publishes no exports; checking a
		// name built from an unresolved value would report a conflict over nothing.
		if err := d.checkExports(ctx, stackName, pending.exports()); err != nil {
			return nil, nil, err
		}
	}

	// Only now is the stack's fate decided. The export checks run first because they
	// are refusals of the *request* — substrate answers them as errors rather than as
	// a rolled-back stack (see [StackDeployer.checkExports]) — and a request substrate
	// refuses outright never became a stack whose resources could roll back.
	//
	// The outputs resolved above are then deliberately discarded: a failed stack
	// publishes none, and therefore exports none. Publishing them would let another
	// stack import a value whose resource either never deployed or is about to be
	// swept, which is the resolves-against-nothing failure the export model exists to
	// prevent.
	//
	// The resources this deploy replaced are not deleted on this path. A failed update rolls
	// back onto them — "CloudFormation creates the replacement resource first", and deletes
	// the old one only once the update has gone through — so they are still the stack's.
	if len(failures) > 0 {
		failed, failErr := d.handleFailedCreate(ctx, cfnFailedCreate{
			stackName:    stackName,
			templateBody: cfn,
			params:       cctx.params,
			tags:         opts.Tags,
			resources:    resources,
			failures:     failures,
			streamID:     streamID,
			totalCost:    totalCost,
			start:        start,
			createdAt:    createdAt,
			reviewedAt:   reviewedAt,
			onFailure:    opts.resolve(),
			tagFailures:  tagFailures,
		})
		return failed, outcome, failErr
	}

	duration := d.tc.Now().Sub(start)

	result := &DeployResult{
		StackName:              stackName,
		Resources:              resources,
		StreamID:               streamID,
		TotalCost:              totalCost,
		Duration:               duration,
		Outputs:                outputs,
		Status:                 cfnStackCreateComplete,
		TagPropagationFailures: tagFailures,
	}

	// The deployment has gone through, so the resources it replaced are deleted now, after
	// their replacements and the references to them (#1427), and so are the ones its template
	// no longer deploys (#1547). A rollback removes nothing: it re-deploys the template the
	// stack had.
	var removed []DeployedResource
	if !rollback {
		removed = cfnRemovedResources(baseline, tmpl, cctx)
	}
	if deletions, reason := d.deleteSuperseded(ctx, base, replaced, removed, streamID); len(deletions) > 0 {
		result.ResourceDeletions = deletions
		result.StatusReason = reason
	}

	// Persist stack state if state manager is available.
	if d.state != nil {
		state := CFNStackState{
			StackName:    stackName,
			TemplateBody: cfn,
			Parameters:   cctx.params,
			Tags:         opts.Tags,
			Resources:    resources,
			Outputs:      outputs,
			ExportNames:  exportNames,
			Imports:      cctx.importedNames(),
			AccountID:    d.identity.accountID,
			Region:       d.identity.region,
			Status:       cfnStackCreateComplete,
			OnFailure:    opts.resolve(),
			CreatedAt:    createdAt,
			ReviewedAt:   reviewedAt,
			UpdatedAt:    d.tc.Now(),
		}
		state.ResourceDeletions = result.ResourceDeletions
		state.StatusReason = result.StatusReason
		state.setAttribution(d.attribution)
		if err := d.persistStack(ctx, state); err != nil {
			return nil, nil, err
		}
	}

	return result, outcome, nil
}

// UpdateStack applies a new template or new parameters to a previously deployed stack.
//
// Each resource is compared with its previous deployment (#1427): an unchanged one keeps
// its physical resource and no call is made for it; a changed one is updated in place
// where substrate models the type's update operation, and otherwise re-created, with the
// resource it replaced deleted once the update succeeds. See cfn_update.go.
//
// A resource that fails during the update rolls the stack back onto the record it was
// updated from and reports UPDATE_ROLLBACK_COMPLETE: the previous resources stand, the
// ones the failed update changed in place are applied again, and the ones it created are
// deleted — see [StackDeployer.rollbackFailedUpdate]. An update whose rollback is not
// wanted has no option to disable it: DisableRollback and OnFailure are CreateStack
// parameters, and UpdateStack models neither.
//
// Only opts.Tags is read, for that same reason: the options type is shared with
// [StackDeployer.DeployWithOptions] so a caller has one struct to build rather than two
// near-identical ones, and an OnFailure set here is **ignored** — UpdateStack forces
// DO_NOTHING below because an update's failure path is a rollback onto a template, not a
// sweep of the resources.
func (d *StackDeployer) UpdateStack(
	ctx context.Context, cfn, stackName string, params map[string]string, opts CFNDeployOptions,
) (*DeployResult, error) {
	// Read before the update, because the update overwrites it: this is the only
	// description of the pre-update state a rollback has to converge on, and the only
	// record of the tags an update that omits Tags has to preserve.
	prev, prevErr := d.loadStack(ctx, stackName)
	if prevErr == nil {
		if err := d.checkUpdatable(prev); err != nil {
			return nil, err
		}
	}

	// "If you don't specify this parameter, CloudFormation doesn't modify the stack's
	// tags. If you specify an empty value, CloudFormation removes all associated tags."
	// A nil map is the omitted parameter, so it resolves to what the stack already
	// carries; an empty non-nil one is the empty list and clears them. An unreadable
	// previous record leaves the update's own tags in force rather than failing — the
	// update is about to overwrite that record anyway.
	tags := opts.Tags
	if tags == nil && prevErr == nil && prev != nil {
		tags = prev.Tags
	}

	// DO_NOTHING, so a failed update does not take the create-rollback path and
	// delete the resources: an update failure rolls *back to a template*, and
	// deleting what the previous template declares is the opposite of that.
	result, outcome, err := d.deploy(ctx, cfn, stackName, params,
		CFNDeployOptions{OnFailure: CFNOnFailureDoNothing, Tags: tags}, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("update stack %s: %w", stackName, err)
	}

	failures := cfnFailedResources(result.Resources)
	if outcome != nil {
		failures = outcome.failures
	}
	if len(failures) > 0 {
		// With no readable previous template there is nothing to converge on, so
		// the update stops at UPDATE_FAILED rather than claiming a rollback it
		// could not perform.
		if prevErr != nil || prev == nil || prev.TemplateBody == "" {
			reason := strings.Join(failures, "; ")
			result.Status = cfnStackUpdateFailed
			result.StatusReason = reason
			if err := d.setStackStatus(ctx, stackName, cfnStackUpdateFailed, reason); err != nil {
				return nil, err
			}
			// prevErr is logged rather than returned, and this is the one place that
			// choice is not obvious. Failing to read the *previous* state is not a
			// failure of this update: the update itself already ran and its resources
			// already deployed, so returning an error here would report a call that
			// did not happen and lose the UPDATE_FAILED status the caller needs. The
			// unreadable record is why no rollback was attempted, which is what the
			// log line says.
			d.logger.Warn("cfn: stack update failed and no previous template is readable; "+
				"not rolling back", "stack", stackName, "failures", len(failures),
				"previous_state_error", cfnErrText(prevErr))
			return result, nil //nolint:nilerr
		}
		return d.rollbackFailedUpdate(ctx, *prev, result, outcome, failures, stackName)
	}

	// Overwrite the persisted status.
	if d.state != nil {
		data, getErr := d.stackData(ctx, stackName)
		if getErr == nil && data != nil {
			var s CFNStackState
			if unmarshalErr := json.Unmarshal(data, &s); unmarshalErr == nil {
				s.Status = cfnStackUpdateComplete
				// Empty unless the update could not delete a resource it replaced, which
				// "includes a StatusReason that states that one or more resources couldn't
				// be deleted" on an UPDATE_COMPLETE stack.
				s.StatusReason = result.StatusReason
				s.UpdatedAt = d.tc.Now()
				if err := d.persistStack(ctx, s); err != nil {
					return nil, err
				}
			}
		}
	}
	result.Status = cfnStackUpdateComplete
	return result, nil
}

// DeleteStack removes a deployed stack from state.
//
// A stack whose exported output another stack imports is refused with
// [ErrCFNExportInUse]: "after another stack imports an output value, you can't
// delete the stack that is exporting the output value … all the imports must be
// removed before you can delete the exporting stack". That refusal is the whole
// reason exports are modeled rather than faked — an import that resolves against
// nothing enforceable is a lookup, not a reference.
func (d *StackDeployer) DeleteStack(ctx context.Context, stackName string) error {
	if d.state == nil {
		return nil
	}
	if err := d.checkExportsNotImported(ctx, stackName); err != nil {
		return err
	}

	// Sweep the stack's resources before dropping its record. The record is what
	// names them, so removing it first would strand every resource with no way
	// left to find it — which is exactly the leak this fixes (#518).
	//
	// A stack whose record is already gone sweeps nothing and succeeds: DeleteStack
	// documents no not-found error, and deleting an absent stack is a success.
	stack, err := d.loadStack(ctx, stackName)
	if err != nil {
		return err
	}
	if stack != nil {
		deletions := d.deleteStackResources(ctx, stack, stackName, cfnDeleteStackOp)
		if failed := cfnFailedDeletions(deletions); len(failed) > 0 {
			// The stack stays visible in DescribeStacks reporting DELETE_FAILED,
			// with its resource list intact. A stack that reports a failed delete
			// and then vanishes is a worse lie than the leak: a caller has no way
			// to retry, and no way to learn which resource is holding it.
			stack.Status = cfnStackDeleteFailed
			stack.ResourceDeletions = deletions
			stack.UpdatedAt = d.tc.Now()
			if err := d.persistStack(ctx, *stack); err != nil {
				return fmt.Errorf("delete stack %s: %w", stackName, err)
			}
			return cfnErrf(ErrCFNDeleteFailed, "delete stack %s: %s", stackName,
				strings.Join(failed, "; "))
		}
	}

	if err := d.removeStackRecord(ctx, stackName); err != nil {
		return err
	}
	// A deleted stack's change sets go with it, so a stack recreated under the same name — the
	// CDK's retry after deleting a REVIEW_IN_PROGRESS stack it could not execute — starts with none.
	return d.deleteStackChangeSets(ctx, stackName, "")
}

// loadStack reads a persisted stack by name, returning (nil, nil) when no stack of
// that name exists and (nil, nil) when its record does not unmarshal.
//
// A corrupt record reads as absent rather than as an error because that is what
// ListStacks already does with one, and a delete that cannot be completed because
// the record is unreadable would leave a stack nothing can remove.
func (d *StackDeployer) loadStack(ctx context.Context, stackName string) (*CFNStackState, error) {
	data, err := d.stackData(ctx, stackName)
	if err != nil {
		return nil, fmt.Errorf("load stack %s: %w", stackName, err)
	}
	if data == nil {
		return nil, nil
	}
	var s CFNStackState
	if unmarshalErr := json.Unmarshal(data, &s); unmarshalErr != nil {
		d.logger.Warn("cfn: stack record does not unmarshal", "stack", stackName,
			"error", unmarshalErr.Error())
		return nil, nil
	}
	return &s, nil
}

// cfnFailedDeletions returns one "LogicalID (Type): reason" description per
// resource the sweep failed to delete.
func cfnFailedDeletions(deletions []CFNResourceDeletion) []string {
	var failed []string
	for _, del := range deletions {
		if del.Status == cfnDeleteFailed {
			failed = append(failed,
				fmt.Sprintf("%s (%s): %s", del.LogicalID, del.Type, del.Reason))
		}
	}
	return failed
}

// ListStacks returns all persisted stack states.
func (d *StackDeployer) ListStacks(ctx context.Context) ([]CFNStackState, error) {
	if d.state == nil {
		return nil, nil
	}
	names, err := d.stackNames(ctx)
	if err != nil {
		return nil, err
	}
	stacks := make([]CFNStackState, 0, len(names))
	for _, name := range names {
		data, getErr := d.stackData(ctx, name)
		if getErr != nil || data == nil {
			continue
		}
		var s CFNStackState
		if unmarshalErr := json.Unmarshal(data, &s); unmarshalErr != nil {
			continue
		}
		stacks = append(stacks, s)
	}
	return stacks, nil
}

// persistStack writes a stack's record and its entry in the names index. Every failure is
// returned: it was logged and dropped until #1192, so a stack whose record never landed answered
// CreateStack 200 and then DescribeStacks did not know it.
func (d *StackDeployer) persistStack(ctx context.Context, s CFNStackState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("cfn persistStack %s marshal: %w", s.StackName, err)
	}
	// A stack recorded before #1366 moves to its scoped keys before it is written (cfn_scope.go).
	if err := d.migrateLegacyStack(ctx, s.StackName); err != nil {
		return fmt.Errorf("cfn persistStack %s migrate legacy record: %w", s.StackName, err)
	}
	if err := d.state.Put(ctx, cfnNamespace, d.stackKey(s.StackName), data); err != nil {
		return fmt.Errorf("cfn persistStack %s state.Put: %w", s.StackName, err)
	}
	if err := d.addStackName(ctx, s.StackName); err != nil {
		return fmt.Errorf("cfn persistStack %s index: %w", s.StackName, err)
	}
	return nil
}

func (d *StackDeployer) loadStackNames(ctx context.Context) ([]string, error) {
	data, err := d.state.Get(ctx, cfnNamespace, d.stackNamesKey())
	if err != nil {
		return nil, fmt.Errorf("cfn loadStackNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("cfn loadStackNames unmarshal: %w", err)
	}
	return names, nil
}

func (d *StackDeployer) saveStackNames(ctx context.Context, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("cfn saveStackNames marshal: %w", err)
	}
	return d.state.Put(ctx, cfnNamespace, d.stackNamesKey(), data)
}

// CFNExport is one exported output value as [StackDeployer.Exports] reports it.
type CFNExport struct {
	// Name is the export name, unique per account and Region.
	Name string `json:"Name"`

	// Value is the resolved output value.
	Value string `json:"Value"`

	// ExportingStackName names the stack that declared the export. The API
	// reports an ExportingStackId; the stack ARN is built at the wire boundary,
	// which is the only place that knows the requesting caller's partition.
	ExportingStackName string `json:"ExportingStackName"`
}

// Exports returns every exported output value visible in the deployer's account
// and Region, sorted by export name.
//
// Visibility is scoped rather than global because "for each AWS account, Export
// names must be unique within a Region" and "cross-stack references are limited to
// the same account and Region" — an emulator that ignored the scope would resolve
// an import a real deployment would reject, which is worse than not resolving it.
func (d *StackDeployer) Exports(ctx context.Context) ([]CFNExport, error) {
	stacks, err := d.ListStacks(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]CFNExport, 0, len(stacks))
	for _, s := range stacks {
		if !d.sameScope(s) {
			continue
		}
		for name, value := range s.exports() {
			out = append(out, CFNExport{Name: name, Value: value, ExportingStackName: s.StackName})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Imports returns the names of the stacks importing an export, sorted.
//
// An unknown export name yields an empty list rather than an error: ListImports
// documents no service-specific error, and "no stack imports this" and "no such
// export" are the same answer to the question the API asks.
func (d *StackDeployer) Imports(ctx context.Context, exportName string) ([]string, error) {
	stacks, err := d.ListStacks(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, s := range stacks {
		if !d.sameScope(s) {
			continue
		}
		if containsStr(s.Imports, exportName) {
			out = append(out, s.StackName)
		}
	}
	sort.Strings(out)
	return out, nil
}

// sameScope reports whether a persisted stack shares the deployer's account and
// Region, and so participates in its export namespace.
//
// An empty AccountID or Region is treated as in scope. Those fields arrived with
// exports, so a stack persisted by an earlier substrate has neither, and excluding
// it would make a restored snapshot's stacks vanish from ListExports rather than
// simply exporting nothing — a stack with no Exports map contributes nothing here
// either way.
func (d *StackDeployer) sameScope(s CFNStackState) bool {
	if s.AccountID != "" && s.AccountID != d.identity.accountID {
		return false
	}
	return s.Region == "" || s.Region == d.identity.region
}

// loadExports builds the export-name → value map a deployment resolves
// Fn::ImportValue against, excluding the stack being deployed.
//
// A failure to read state yields an empty map rather than an error: an import that
// finds no export records a resolution failure on the resource, which is the same
// observable and reaches the caller as CREATE_FAILED with a reason naming the
// export. Aborting the whole deployment would be a worse answer to a state read
// that a redeploy may well satisfy.
func (d *StackDeployer) loadExports(ctx context.Context, deployingStack string) map[string]string {
	if d.state == nil {
		return nil
	}
	stacks, err := d.ListStacks(ctx)
	if err != nil {
		d.logger.Warn("cfn: could not read exports; Fn::ImportValue will not resolve", "err", err)
		return nil
	}
	out := make(map[string]string)
	for _, s := range stacks {
		if s.StackName == deployingStack || !d.sameScope(s) {
			continue
		}
		for name, value := range s.exports() {
			out[name] = value
		}
	}
	return out
}

// checkExports refuses a deployment whose exports would break the two rules the
// export namespace enforces: a name may be exported by only one stack, and an
// exported value another stack imports may not change.
//
// Both run after the resources deploy, because an export name or value may be
// built from a resource attribute and so is not knowable earlier. The deployment is
// refused rather than rolled back: this is a refusal of the *request*, reported as
// an error, and it is checked before a failed resource is allowed to roll the stack
// back so that a duplicate export name is answered the same way whether or not some
// resource beside it also failed. The resources exist and the caller is told the
// stack record was not written, which is the honest observable rather than a
// fabricated rollback.
func (d *StackDeployer) checkExports(ctx context.Context, stackName string, exports map[string]string) error {
	if d.state == nil {
		return nil
	}
	existing, err := d.Exports(ctx)
	if err != nil {
		return err
	}

	// "For each AWS account, Export names must be unique within a Region."
	owner := make(map[string]string, len(existing))
	for _, e := range existing {
		if e.ExportingStackName != stackName {
			owner[e.Name] = e.ExportingStackName
		}
	}
	for _, name := range sortedStringKeys(exports) {
		if other, taken := owner[name]; taken {
			return cfnErrf(ErrCFNExportNameConflict,
				"cfn Deploy: export name %q is already exported by stack %q", name, other)
		}
	}

	// "You can't … modify the exported output value" while it is imported. An
	// export this stack previously published and now drops is the same
	// modification from the importer's side — the value it resolved stops
	// existing — so a removal is refused on the same terms as a change.
	for _, e := range existing {
		if e.ExportingStackName != stackName {
			continue
		}
		newValue, still := exports[e.Name]
		if still && newValue == e.Value {
			continue
		}
		importers, importErr := d.Imports(ctx, e.Name)
		if importErr != nil {
			return importErr
		}
		if importers = removeStr(importers, stackName); len(importers) == 0 {
			continue
		}
		what := "changed"
		if !still {
			what = "removed"
		}
		return cfnErrf(ErrCFNExportInUse,
			"cfn Deploy: export %q cannot be %s while it is imported by %s; "+
				"all imports must be removed first",
			e.Name, what, strings.Join(importers, ", "))
	}
	return nil
}

// sortedStringKeys returns a string-keyed map's keys in sorted order, so a loop
// over them reports the same element first every time.
func sortedStringKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkExportsNotImported refuses to delete a stack whose export another stack
// imports, naming the export and the importing stacks.
//
// The message names them because that is the only way a caller can act on the
// refusal: the API's own remedy is "first find out which stacks are importing
// them", and a refusal that withheld the answer would send them to ListImports for
// something substrate already knows.
func (d *StackDeployer) checkExportsNotImported(ctx context.Context, stackName string) error {
	data, err := d.stackData(ctx, stackName)
	if err != nil {
		return fmt.Errorf("cfn DeleteStack: read stack %q: %w", stackName, err)
	}
	if data == nil {
		// An absent stack is not an error — DeleteStack documents no not-found
		// error — and a stack that is not there exports nothing.
		return nil
	}
	var stack CFNStackState
	if err := json.Unmarshal(data, &stack); err != nil {
		// A record substrate wrote and cannot read back is corrupt state, not a
		// caller error. Reporting it beats deleting a stack whose exports could
		// not be checked, which is how an importer loses its reference silently.
		return fmt.Errorf("cfn DeleteStack: unmarshal stack %q: %w", stackName, err)
	}
	for _, name := range sortedStringKeys(stack.exports()) {
		importers, err := d.Imports(ctx, name)
		if err != nil {
			return err
		}
		importers = removeStr(importers, stackName)
		if len(importers) > 0 {
			return cfnErrf(ErrCFNExportInUse,
				"cfn DeleteStack: export %q is imported by %s; all imports must be removed first",
				name, strings.Join(importers, ", "))
		}
	}
	return nil
}

// removeStr returns list without any element equal to drop.
func removeStr(list []string, drop string) []string {
	out := make([]string, 0, len(list))
	for _, s := range list {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}

// CreateChangeSet compares a proposed template against the current stack state
// and returns a change set describing the differences. The change set is persisted
// in state and can be executed later via [StackDeployer.ExecuteChangeSet].
//
// tags are the stack-level tags the change set records and executing it applies, with the
// same three-way meaning the rest of the family gives them: nil is the omitted parameter,
// an empty non-nil map is the empty list, and a populated one replaces the stack's tags
// (#824). They are validated here rather than at the wire layer, for the reason
// [CFNDeployOptions.validate] states — an in-process caller is held to the same limits as
// a request that arrived over HTTP — and validated at *creation* rather than deferred to
// execution, because that is where AWS refuses them: `CreateChangeSet` publishes the same
// `Tags` constraints `CreateStack` does, so a change set that could never execute is not
// worth recording.
func (d *StackDeployer) CreateChangeSet(ctx context.Context, stackName, changeSetName, templateBody string, params, tags map[string]string) (*CFNChangeSet, error) {
	return d.CreateChangeSetWithOptions(ctx, stackName, changeSetName, templateBody, params,
		CFNChangeSetOptions{Tags: tags})
}

// CreateChangeSetWithOptions is [StackDeployer.CreateChangeSet] with the rest of what
// CreateChangeSet takes: the ChangeSetType, and the role, capabilities, description and failure
// action executing the change set applies (#1606). A CREATE change set creates the stack in
// REVIEW_IN_PROGRESS — see cfn_changeset_lifecycle.go.
func (d *StackDeployer) CreateChangeSetWithOptions(ctx context.Context, stackName, changeSetName, templateBody string,
	params map[string]string, opts CFNChangeSetOptions,
) (*CFNChangeSet, error) {
	if d.state == nil {
		return nil, cfnErrf(ErrCFNStateRequired, "cfn CreateChangeSet: state manager required")
	}
	tags := opts.Tags
	if err := cfnValidateStackTags(tags); err != nil {
		return nil, err
	}
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// The stack the change set is compared against, and its template: the existing stack's for an
	// UPDATE, and for a CREATE an empty one, so every resource is an Add.
	stack, oldTmpl, err := d.changeSetBase(ctx, stackName, changeSetName, opts)
	if err != nil {
		return nil, err
	}
	newTmpl, err := d.parseCFNTemplate(templateBody)
	if err != nil {
		return nil, cfnErrf(ErrCFNTemplateInvalid, "cfn CreateChangeSet: parse new template: %w", err)
	}
	// A template whose resources cannot be ordered is refused here too, as CreateChangeSet
	// refuses it, rather than recorded as a change set that could never execute (#1420).
	newCtx := buildCFNContext(newTmpl, params, d.identity.region, d.identity.accountID, stackName)
	evaluateConditions(newTmpl, newCtx)
	if _, err := d.cfnValidateTemplate(newTmpl, newCtx); err != nil {
		return nil, err
	}

	// Diff resources: the two templates, the parameter values each resolves against, and
	// the physical IDs the stack's resources were deployed under.
	physical := make(map[string]string, len(stack.Resources))
	for _, r := range stack.Resources {
		physical[r.LogicalID] = r.PhysicalID
	}
	oldCtx := buildCFNContext(oldTmpl, stack.Parameters, d.identity.region, d.identity.accountID, stackName)
	evaluateConditions(oldTmpl, oldCtx)
	changes := cfnReconcileActivity(diffCFNResources(oldTmpl, newTmpl, stack.Parameters, params, physical),
		oldTmpl, newTmpl, cfnActiveResources(oldTmpl, oldCtx), cfnActiveResources(newTmpl, newCtx), physical)

	cs := &CFNChangeSet{
		ChangeSetName:  changeSetName,
		StackName:      stackName,
		Status:         cfnChangeSetCreateComplete,
		TemplateBody:   templateBody,
		Parameters:     params,
		Changes:        changes,
		Tags:           tags,
		CreatedAt:      d.tc.Now(),
		Type:           opts.changeSetType(),
		Description:    opts.Description,
		RoleARN:        opts.RoleARN,
		Capabilities:   opts.Capabilities,
		OnStackFailure: opts.OnStackFailure,
	}
	cfnSettleChangeSet(cs, stack, oldTmpl, newTmpl)
	if err := d.ensureReviewStack(ctx, stack, cs); err != nil {
		return nil, err
	}

	// Persist.
	csData, err := json.Marshal(cs)
	if err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: marshal: %w", err)
	}
	if err := d.migrateLegacyStack(ctx, stackName); err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: %w", err)
	}
	if err := d.state.Put(ctx, cfnNamespace, d.changeSetKey(stackName, changeSetName), csData); err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: put: %w", err)
	}

	// Update index.
	names, err := d.loadChangeSetNames(ctx, stackName)
	if err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: %w", err)
	}
	names = append(names, changeSetName)
	if err := d.saveChangeSetNames(ctx, stackName, names); err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: %w", err)
	}

	return cs, nil
}

// DescribeChangeSet returns a previously created change set.
func (d *StackDeployer) DescribeChangeSet(ctx context.Context, stackName, changeSetName string) (*CFNChangeSet, error) {
	if d.state == nil {
		return nil, cfnErrf(ErrCFNStateRequired, "cfn DescribeChangeSet: state manager required")
	}
	data, err := d.changeSetData(ctx, stackName, changeSetName)
	if err != nil {
		return nil, fmt.Errorf("cfn DescribeChangeSet: %w", err)
	}
	if data == nil {
		return nil, cfnErrf(ErrCFNChangeSetNotFound, "cfn DescribeChangeSet: change set %q not found", changeSetName)
	}
	var cs CFNChangeSet
	if err := json.Unmarshal(data, &cs); err != nil {
		return nil, fmt.Errorf("cfn DescribeChangeSet: unmarshal: %w", err)
	}
	return &cs, nil
}

// ExecuteChangeSet applies a change set: a CREATE change set creates its REVIEW_IN_PROGRESS stack
// through [StackDeployer.DeployWithOptions], and an UPDATE one updates the stack through
// [StackDeployer.UpdateStack], each with the change set's template, parameters and tags.
//
// The executed change set remains, reporting EXECUTE_COMPLETE or EXECUTE_FAILED, and every other
// change set of the stack is deleted: "When you execute a change set, CloudFormation deletes all
// other change sets associated with the stack because they aren't valid for the updated stack"
// (API_ExecuteChangeSet). See [StackDeployer.ExecuteChangeSetWithOptions].
func (d *StackDeployer) ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) (*DeployResult, error) {
	return d.ExecuteChangeSetWithOptions(ctx, stackName, changeSetName, CFNExecuteOptions{})
}

// ListChangeSets returns all change sets for a stack.
func (d *StackDeployer) ListChangeSets(ctx context.Context, stackName string) ([]CFNChangeSet, error) {
	if d.state == nil {
		return nil, nil
	}
	names, err := d.loadChangeSetNames(ctx, stackName)
	if err != nil {
		return nil, err
	}
	sets := make([]CFNChangeSet, 0, len(names))
	for _, name := range names {
		data, getErr := d.changeSetData(ctx, stackName, name)
		if getErr != nil || data == nil {
			continue
		}
		var cs CFNChangeSet
		if unmarshalErr := json.Unmarshal(data, &cs); unmarshalErr != nil {
			continue
		}
		sets = append(sets, cs)
	}
	return sets, nil
}

// DeleteChangeSet removes a change set without executing it.
func (d *StackDeployer) DeleteChangeSet(ctx context.Context, stackName, changeSetName string) error {
	if d.state == nil {
		return nil
	}
	if err := d.migrateLegacyStack(ctx, stackName); err != nil {
		return fmt.Errorf("cfn DeleteChangeSet: %w", err)
	}
	if err := d.state.Delete(ctx, cfnNamespace, d.changeSetKey(stackName, changeSetName)); err != nil {
		return fmt.Errorf("cfn DeleteChangeSet: %w", err)
	}
	names, err := d.loadChangeSetNames(ctx, stackName)
	if err != nil {
		return fmt.Errorf("cfn DeleteChangeSet: %w", err)
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != changeSetName {
			newNames = append(newNames, n)
		}
	}
	return d.saveChangeSetNames(ctx, stackName, newNames)
}

// DetectStackDrift checks whether stack resources still exist in their
// respective service state. Returns IN_SYNC if all resources exist, DRIFTED if
// any resource has been deleted outside of CloudFormation.
//
// logicalIDs, when given, restricts detection to those resources — DetectStackDrift's
// LogicalResourceIds filter (#1446) — so the result, its count and its status cover only
// them. A name the stack does not declare matches nothing here; the wire handler refuses
// one before calling. With no logicalIDs every resource is checked, as before.
func (d *StackDeployer) DetectStackDrift(ctx context.Context, stackName string, logicalIDs ...string) (*CFNDriftResult, error) {
	if d.state == nil {
		return nil, cfnErrf(ErrCFNStateRequired, "cfn DetectStackDrift: state manager required")
	}
	data, err := d.stackData(ctx, stackName)
	if err != nil {
		return nil, fmt.Errorf("cfn DetectStackDrift: %w", err)
	}
	if data == nil {
		return nil, cfnErrf(ErrCFNStackNotFound, "cfn DetectStackDrift: stack %q not found", stackName)
	}
	var stack CFNStackState
	if err := json.Unmarshal(data, &stack); err != nil {
		return nil, fmt.Errorf("cfn DetectStackDrift: unmarshal: %w", err)
	}

	result := &CFNDriftResult{
		StackName:   stackName,
		DriftStatus: "IN_SYNC",
		DetectedAt:  d.tc.Now(),
	}

	var only map[string]bool
	if len(logicalIDs) > 0 {
		only = make(map[string]bool, len(logicalIDs))
		for _, id := range logicalIDs {
			only[id] = true
		}
	}

	for _, dr := range stack.Resources {
		if only != nil && !only[dr.LogicalID] {
			continue
		}
		entry := CFNResourceDriftEntry{
			LogicalID:    dr.LogicalID,
			ResourceType: dr.Type,
			PhysicalID:   dr.PhysicalID,
			DriftStatus:  "NOT_CHECKED",
		}

		checker, ok := cfnDriftCheckers[dr.Type]
		if ok {
			exists := checker(d, ctx, d.identity.accountID, d.identity.region, dr.PhysicalID)
			switch {
			case !exists:
				entry.DriftStatus = "DELETED"
				result.DriftedCount++
			default:
				entry.DriftStatus = "IN_SYNC"
				// For resource types with a property comparator, check whether
				// the live resource has drifted from its template definition.
				if cmp, hasCmp := cfnDriftComparators[dr.Type]; hasCmp {
					if diffs := cmp(ctx, d, &stack, dr); len(diffs) > 0 {
						entry.DriftStatus = "MODIFIED"
						entry.PropertyDifferences = diffs
						result.DriftedCount++
					}
				}
			}
		}

		result.ResourceDrifts = append(result.ResourceDrifts, entry)
	}

	if result.DriftedCount > 0 {
		result.DriftStatus = "DRIFTED"
	}

	return result, nil
}

// DescribeStackResourceDrifts returns the per-resource drift entries for a
// stack, optionally filtered to the given drift statuses (e.g. {"MODIFIED"}).
// An empty statusFilters returns all entries. Drift is recomputed on each call.
func (d *StackDeployer) DescribeStackResourceDrifts(ctx context.Context, stackName string, statusFilters []string) ([]CFNResourceDriftEntry, error) {
	result, err := d.DetectStackDrift(ctx, stackName)
	if err != nil {
		return nil, err
	}
	if len(statusFilters) == 0 {
		return result.ResourceDrifts, nil
	}
	filtered := make([]CFNResourceDriftEntry, 0, len(result.ResourceDrifts))
	for _, e := range result.ResourceDrifts {
		for _, s := range statusFilters {
			if e.DriftStatus == s {
				filtered = append(filtered, e)
				break
			}
		}
	}
	return filtered, nil
}

// StartStackDriftDetection begins a drift-detection operation and returns its
// detection ID. The operation runs synchronously, but the IN_PROGRESS record is
// persisted before detection runs so a concurrent
// [StackDeployer.DescribeStackDriftDetectionStatus] read can observe it.
//
// logicalIDs is passed to [StackDeployer.DetectStackDrift], so the recorded status and
// drifted-resource count cover only those resources when any are given (#1446).
func (d *StackDeployer) StartStackDriftDetection(ctx context.Context, stackName string, logicalIDs ...string) (string, error) {
	if d.state == nil {
		return "", cfnErrf(ErrCFNStateRequired, "cfn StartStackDriftDetection: state manager required")
	}
	data, err := d.stackData(ctx, stackName)
	if err != nil {
		return "", fmt.Errorf("cfn StartStackDriftDetection: %w", err)
	}
	if data == nil {
		return "", cfnErrf(ErrCFNStackNotFound, "cfn StartStackDriftDetection: stack %q not found", stackName)
	}

	// Minted from the deployment's mint, so a replayed DetectStackDrift returns the id
	// the recording returned and the recorded DescribeStackDriftDetectionStatus that
	// names it is answered rather than refused (#856). It was generateRequestID, which
	// is the wall clock — and which also put substrate's internal `req-…` request-id
	// format into an AWS response element.
	//
	// `API_DetectStackDrift` publishes `StackDriftDetectionId` as a String with a
	// **maximum length of 36** and no pattern, and its sample response is
	// `2f2b2d60-df86-11e7-bea1-500c2example` — a UUID shape. 36 is exactly what
	// [IDMint.HexUUID] renders, so the published bound is met at its limit and the value
	// looks like one CloudFormation would issue. Reading AWS's example is the provenance
	// #671 allows; inventing a bound from a sibling operation is not.
	detectionID := d.mint.HexUUID()
	// Persist the in-progress record before running detection.
	inProgress := &CFNDriftDetectionStatus{
		StackDriftDetectionID: detectionID,
		StackName:             stackName,
		DetectionStatus:       "DETECTION_IN_PROGRESS",
		Timestamp:             d.tc.Now(),
	}
	if err := d.saveDriftDetection(ctx, inProgress); err != nil {
		return "", err
	}

	result, err := d.DetectStackDrift(ctx, stackName, logicalIDs...)
	if err != nil {
		failed := &CFNDriftDetectionStatus{
			StackDriftDetectionID: detectionID,
			StackName:             stackName,
			DetectionStatus:       "DETECTION_FAILED",
			Timestamp:             d.tc.Now(),
		}
		_ = d.saveDriftDetection(ctx, failed)
		return detectionID, err
	}

	complete := &CFNDriftDetectionStatus{
		StackDriftDetectionID:     detectionID,
		StackName:                 stackName,
		DetectionStatus:           "DETECTION_COMPLETE",
		StackDriftStatus:          result.DriftStatus,
		DriftedStackResourceCount: result.DriftedCount,
		Timestamp:                 d.tc.Now(),
	}
	if err := d.saveDriftDetection(ctx, complete); err != nil {
		return "", err
	}
	return detectionID, nil
}

// DescribeStackDriftDetectionStatus returns the status of a previously started
// drift-detection operation.
func (d *StackDeployer) DescribeStackDriftDetectionStatus(ctx context.Context, detectionID string) (*CFNDriftDetectionStatus, error) {
	data, err := d.driftDetectionData(ctx, detectionID)
	if err != nil {
		return nil, fmt.Errorf("cfn DescribeStackDriftDetectionStatus: %w", err)
	}
	if data == nil {
		return nil, cfnErrf(ErrCFNDriftDetectionNotFound, "cfn DescribeStackDriftDetectionStatus: detection %q not found", detectionID)
	}
	var status CFNDriftDetectionStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("cfn DescribeStackDriftDetectionStatus: unmarshal: %w", err)
	}
	return &status, nil
}

// saveDriftDetection persists a drift-detection status record.
func (d *StackDeployer) saveDriftDetection(ctx context.Context, status *CFNDriftDetectionStatus) error {
	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("cfn saveDriftDetection: marshal: %w", err)
	}
	if err := d.migrateLegacyStack(ctx, status.StackName); err != nil {
		return fmt.Errorf("cfn saveDriftDetection: %w", err)
	}
	if err := d.state.Put(ctx, cfnNamespace, d.driftDetectionKey(status.StackDriftDetectionID), data); err != nil {
		return fmt.Errorf("cfn saveDriftDetection: put: %w", err)
	}
	return nil
}

// loadChangeSetNames returns a stack's change-set index: the scoped one, or an unmigrated in-scope
// legacy stack's.
func (d *StackDeployer) loadChangeSetNames(ctx context.Context, stackName string) ([]string, error) {
	data, err := d.state.Get(ctx, cfnNamespace, d.changeSetNamesKey(stackName))
	if err != nil {
		return nil, fmt.Errorf("cfn loadChangeSetNames: %w", err)
	}
	if data == nil {
		_, found, legacyErr := d.legacyStack(ctx, stackName)
		if legacyErr != nil || !found {
			return nil, legacyErr
		}
		return d.loadNameList(ctx, cfnLegacyChangeSetNamesKey(stackName))
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("cfn loadChangeSetNames: %w", err)
	}
	return names, nil
}

func (d *StackDeployer) saveChangeSetNames(ctx context.Context, stackName string, names []string) error {
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("cfn saveChangeSetNames: %w", err)
	}
	return d.state.Put(ctx, cfnNamespace, d.changeSetNamesKey(stackName), data)
}

// cfnDriftCheckers maps CFN resource types to functions that check whether the
// resource still exists in plugin state. Returns true if the resource exists.
var cfnDriftCheckers = map[string]func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool{
	"AWS::S3::Bucket": func(d *StackDeployer, ctx context.Context, _, _, physicalID string) bool {
		data, _ := d.state.Get(ctx, "s3", "bucket:"+physicalID)
		return data != nil
	},
	"AWS::DynamoDB::Table": func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool {
		data, _ := d.state.Get(ctx, "dynamodb", "table:"+acct+"/"+region+"/"+physicalID)
		return data != nil
	},
	"AWS::SQS::Queue": func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool {
		// A queue name is unique per account per Region, and since #1088 the key says so, for the
		// reason [sqsQueueStateKey] records — the same reason the Lambda arm below reads its own
		// Region. The PhysicalID is the queue name ([StackDeployer.deploySQSQueue]).
		data, _ := d.state.Get(ctx, "sqs", sqsQueueStateKey(acct, region, physicalID))
		return data != nil
	},
	"AWS::SNS::Topic": func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool {
		// PhysicalID is the topic ARN; the SNS state key is name-based.
		data, _ := d.state.Get(ctx, "sns", "topic:"+acct+"/"+region+"/"+snsTopicNameFromPhysicalID(physicalID))
		return data != nil
	},
	"AWS::Lambda::Function": func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool {
		// A function name is unique per account per region, and since #943 the state key says
		// so — a stack's drift check has to read its own region's record, not another's.
		data, _ := d.state.Get(ctx, "lambda", "function:"+acct+"/"+region+"/"+physicalID)
		return data != nil
	},
	"AWS::IAM::Role": func(d *StackDeployer, ctx context.Context, acct, _, physicalID string) bool {
		data, _ := d.state.Get(ctx, iamNamespace, iamRoleKey(acct, physicalID))
		return data != nil
	},
}

// cfnDriftComparators holds optional property-level drift comparators, keyed by
// CloudFormation resource type. A comparator runs only after the existence
// check passes; it returns the property differences between the template
// (expected) and live service state (actual), or nil/empty when in sync.
//
// Property-level drift is only as faithful as the properties a service plugin
// actually persists in its own state. Today the sole comparator covers
// S3 VersioningConfiguration, which is both pushed by the deploy path and
// independently readable from S3 state; other drift-checkable resource types
// (DynamoDB/SQS/SNS/Lambda/IAM) remain existence-only because their deploy
// paths do not store template-comparable properties. New comparators can be
// added here as the underlying plugins persist more properties.
var cfnDriftComparators = map[string]func(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff{
	"AWS::S3::Bucket":       compareS3BucketDrift,
	"AWS::DynamoDB::Table":  compareDynamoDBTableDrift,
	"AWS::Lambda::Function": compareLambdaFunctionDrift,
	"AWS::IAM::Role":        compareIAMRoleDrift,
	"AWS::SQS::Queue":       compareSQSQueueDrift,
	"AWS::SNS::Topic":       compareSNSTopicDrift,
}

// cfnDriftResourceProps re-parses the stack template and returns the declared
// Properties map plus a resolution context for the given deployed resource.
// It returns (nil, nil, false) when the template cannot be parsed or the
// resource is absent, so callers can simply skip drift comparison.
func (d *StackDeployer) cfnDriftResourceProps(stack *CFNStackState, dr DeployedResource) (map[string]interface{}, *cfnContext, bool) {
	tmpl, err := d.parseCFNTemplate(stack.TemplateBody)
	if err != nil {
		return nil, nil, false
	}
	res, ok := tmpl.Resources[dr.LogicalID]
	if !ok {
		return nil, nil, false
	}
	cctx := buildCFNContext(tmpl, stack.Parameters, d.identity.region, d.identity.accountID, stack.StackName)
	evaluateConditions(tmpl, cctx)
	return res.Properties, cctx, true
}

// driftDiff builds a NOT_EQUAL property difference for a property the template
// explicitly declares whose live value diverges from the expected value.
func driftDiff(path, expected, actual string) CFNPropertyDiff {
	return CFNPropertyDiff{
		PropertyPath:   path,
		ExpectedValue:  expected,
		ActualValue:    actual,
		DifferenceType: "NOT_EQUAL",
	}
}

// compareS3BucketDrift compares a deployed S3 bucket's VersioningConfiguration
// against its live state, reporting a property difference if they diverge.
func compareS3BucketDrift(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff {
	props, cctx, ok := d.cfnDriftResourceProps(stack, dr)
	if !ok {
		return nil
	}
	// Template-declared-only: compare versioning solely when the template
	// declares VersioningConfiguration.
	vc, ok := props["VersioningConfiguration"].(map[string]interface{})
	if !ok {
		return nil
	}
	expected := resolveValue(vc["Status"], cctx)

	// Actual versioning status is stored verbatim under bucket_versioning:{name}.
	actual := ""
	if data, _ := d.state.Get(ctx, "s3", "bucket_versioning:"+dr.PhysicalID); data != nil {
		actual = string(data)
	}
	if expected == actual {
		return nil
	}
	return []CFNPropertyDiff{driftDiff("/VersioningConfiguration/Status", expected, actual)}
}

// compareDynamoDBTableDrift compares a deployed DynamoDB table's declared
// BillingMode and provisioned throughput against live state.
func compareDynamoDBTableDrift(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff {
	props, cctx, ok := d.cfnDriftResourceProps(stack, dr)
	if !ok {
		return nil
	}
	data, _ := d.state.Get(ctx, "dynamodb",
		"table:"+d.identity.accountID+"/"+d.identity.region+"/"+dr.PhysicalID)
	if data == nil {
		return nil
	}
	var tbl DynamoDBTable
	if json.Unmarshal(data, &tbl) != nil {
		return nil
	}

	var diffs []CFNPropertyDiff
	if _, declared := props["BillingMode"]; declared {
		expected := resolveStringProp(props, "BillingMode", "", cctx)
		if expected != "" && expected != tbl.BillingModeSummary.BillingMode {
			diffs = append(diffs, driftDiff("/BillingMode", expected, tbl.BillingModeSummary.BillingMode))
		}
	}
	if pt, declared := props["ProvisionedThroughput"].(map[string]interface{}); declared {
		if expRead := resolveValue(pt["ReadCapacityUnits"], cctx); expRead != "" {
			actRead := strconv.FormatInt(tbl.ProvisionedThroughput.ReadCapacityUnits, 10)
			if expRead != actRead {
				diffs = append(diffs, driftDiff("/ProvisionedThroughput/ReadCapacityUnits", expRead, actRead))
			}
		}
		if expWrite := resolveValue(pt["WriteCapacityUnits"], cctx); expWrite != "" {
			actWrite := strconv.FormatInt(tbl.ProvisionedThroughput.WriteCapacityUnits, 10)
			if expWrite != actWrite {
				diffs = append(diffs, driftDiff("/ProvisionedThroughput/WriteCapacityUnits", expWrite, actWrite))
			}
		}
	}
	return diffs
}

// compareLambdaFunctionDrift compares a deployed Lambda function's declared
// configuration properties against live state.
func compareLambdaFunctionDrift(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff {
	props, cctx, ok := d.cfnDriftResourceProps(stack, dr)
	if !ok {
		return nil
	}
	data, _ := d.state.Get(ctx, "lambda",
		"function:"+d.identity.accountID+"/"+d.identity.region+"/"+dr.PhysicalID)
	if data == nil {
		return nil
	}
	var fn LambdaFunction
	if json.Unmarshal(data, &fn) != nil {
		return nil
	}

	var diffs []CFNPropertyDiff
	if _, declared := props["Runtime"]; declared {
		if exp := resolveStringProp(props, "Runtime", "", cctx); exp != "" && exp != fn.Runtime {
			diffs = append(diffs, driftDiff("/Runtime", exp, fn.Runtime))
		}
	}
	if _, declared := props["Handler"]; declared {
		if exp := resolveStringProp(props, "Handler", "", cctx); exp != "" && exp != fn.Handler {
			diffs = append(diffs, driftDiff("/Handler", exp, fn.Handler))
		}
	}
	if _, declared := props["Timeout"]; declared {
		if exp := resolveValue(props["Timeout"], cctx); exp != "" {
			if act := strconv.Itoa(fn.Timeout); exp != act {
				diffs = append(diffs, driftDiff("/Timeout", exp, act))
			}
		}
	}
	if _, declared := props["MemorySize"]; declared {
		if exp := resolveValue(props["MemorySize"], cctx); exp != "" {
			if act := strconv.Itoa(fn.MemorySize); exp != act {
				diffs = append(diffs, driftDiff("/MemorySize", exp, act))
			}
		}
	}
	return diffs
}

// compareIAMRoleDrift compares a deployed IAM role's declared Description, Path,
// and AssumeRolePolicyDocument against live state. The trust policy is compared
// order-independently via policyDocumentsEqual.
func compareIAMRoleDrift(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff {
	props, cctx, ok := d.cfnDriftResourceProps(stack, dr)
	if !ok {
		return nil
	}
	data, _ := d.state.Get(ctx, iamNamespace, iamRoleKey(stack.AccountID, dr.PhysicalID))
	if data == nil {
		return nil
	}
	var role IAMRole
	if json.Unmarshal(data, &role) != nil {
		return nil
	}

	var diffs []CFNPropertyDiff
	if _, declared := props["Description"]; declared {
		if exp := resolveStringProp(props, "Description", "", cctx); exp != role.Description {
			diffs = append(diffs, driftDiff("/Description", exp, role.Description))
		}
	}
	if _, declared := props["Path"]; declared {
		if exp := resolveStringProp(props, "Path", "", cctx); exp != "" && exp != role.Path {
			diffs = append(diffs, driftDiff("/Path", exp, role.Path))
		}
	}
	if raw, declared := props["AssumeRolePolicyDocument"]; declared {
		var expected PolicyDocument
		// The template value is a JSON object (or string); render it the way
		// deployIAMRole sent it — intrinsics resolved — and unmarshal it the same
		// way the IAM CreateRole handler stores the live document. Comparing the
		// unresolved template would report drift on every trust policy that names
		// a resource through Fn::GetAtt or Fn::Sub (#1153).
		if err := json.Unmarshal([]byte(cfnJSONDocument(raw, cctx)), &expected); err == nil {
			if !policyDocumentsEqual(expected, role.AssumeRolePolicyDocument) {
				diffs = append(diffs, driftDiff(
					"/AssumeRolePolicyDocument",
					marshalToJSON(expected),
					marshalToJSON(role.AssumeRolePolicyDocument),
				))
			}
		}
	}
	return diffs
}

// sqsDriftAttributes are the SQS queue CloudFormation properties whose names map
// 1:1 to SQS queue attributes; deploySQSQueue forwards declared ones and
// compareSQSQueueDrift checks them.
var sqsDriftAttributes = []string{
	"VisibilityTimeout",
	"MessageRetentionPeriod",
	"DelaySeconds",
	"ReceiveMessageWaitTimeSeconds",
	"MaximumMessageSize",
}

// compareSQSQueueDrift compares a deployed SQS queue's declared attribute
// properties against the live queue attributes.
func compareSQSQueueDrift(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff {
	props, cctx, ok := d.cfnDriftResourceProps(stack, dr)
	if !ok {
		return nil
	}
	// The deployer's own Region, as [compareLambdaFunctionDrift] reads for the same reason: a stack
	// lives in one Region and since #1088 so does a queue record.
	data, _ := d.state.Get(ctx, "sqs",
		sqsQueueStateKey(d.identity.accountID, d.identity.region, dr.PhysicalID))
	if data == nil {
		return nil
	}
	var q SQSQueue
	if json.Unmarshal(data, &q) != nil {
		return nil
	}

	var diffs []CFNPropertyDiff
	for _, name := range sqsDriftAttributes {
		if _, declared := props[name]; !declared {
			continue
		}
		exp := resolveValue(props[name], cctx)
		if exp == "" {
			continue
		}
		if act := q.Attributes[name]; exp != act {
			diffs = append(diffs, driftDiff("/"+name, exp, act))
		}
	}
	return diffs
}

// snsTopicNameFromPhysicalID returns the topic name from an SNS topic physical
// ID, which is the topic ARN (arn:aws:sns:{region}:{acct}:{name}). A bare name
// is returned unchanged.
func snsTopicNameFromPhysicalID(physicalID string) string {
	if idx := strings.LastIndexByte(physicalID, ':'); idx >= 0 {
		return physicalID[idx+1:]
	}
	return physicalID
}

// compareSNSTopicDrift compares a deployed SNS topic's declared DisplayName
// against live state.
func compareSNSTopicDrift(ctx context.Context, d *StackDeployer, stack *CFNStackState, dr DeployedResource) []CFNPropertyDiff {
	props, cctx, ok := d.cfnDriftResourceProps(stack, dr)
	if !ok {
		return nil
	}
	if _, declared := props["DisplayName"]; !declared {
		return nil
	}
	name := snsTopicNameFromPhysicalID(dr.PhysicalID)
	data, _ := d.state.Get(ctx, "sns", "topic:"+d.identity.accountID+"/"+d.identity.region+"/"+name)
	if data == nil {
		return nil
	}
	var topic SNSTopic
	if json.Unmarshal(data, &topic) != nil {
		return nil
	}

	exp := resolveValue(props["DisplayName"], cctx)
	act := topic.Attributes["DisplayName"]
	if exp == act {
		return nil
	}
	return []CFNPropertyDiff{driftDiff("/DisplayName", exp, act)}
}

// deployResource dispatches a single CFN resource to the correct deploy helper
// and reports any intrinsic that could not be resolved.
//
// A resolver cannot report a failure through its return value — a string has no
// shape for "there is no answer", which is why an unresolvable Fn::FindInMap used
// to reach the API as a JSON literal (#522). So resolution failures accumulate on
// the context and are drained here, after the helper has run: the resource is
// marked CREATE_FAILED with the reason, exactly as a plugin's own refusal is
// (#519), and the rest of the stack still deploys.
//
// The failure is recorded rather than returned because Deploy's error return
// aborts the whole stack, and CloudFormation fails the *resource*. It also keeps
// the reason a per-resource observable, which is what DescribeStackResources
// reports; #502's typed errors are about the request-level codes and are not
// needed for this.
func (d *StackDeployer) deployResource(
	ctx context.Context,
	logicalID string,
	res cfnResource,
	streamID string,
	cctx *cfnContext,
	plan cfnResourcePlan,
) (DeployedResource, float64, error) {
	// Drain anything a previous resource left behind, so this resource cannot
	// inherit a reason that is not its own. Nothing should have: Deploy drains
	// after every resource, and Outputs, resolved after the last resource, collect
	// their own failures and fail the stack with them (#1429).
	cctx.takeFailures()
	cctx.priorPhysicalID = ""
	if plan.hasPrior {
		cctx.priorPhysicalID = plan.prior.PhysicalID
	}
	cctx.priorProps = plan.priorResolved
	cctx.restoring = plan.resolvedSame

	var (
		dr   DeployedResource
		cost float64
		err  error
	)
	extraneous := ""
	if !cctx.skipPropertyValidation {
		extraneous = cfnExtraneousProperties(logicalID, res, cctx)
	}
	switch {
	case plan.refusal != "":
		// A change substrate cannot apply in place, refused without a call (#1427).
		dr = DeployedResource{LogicalID: logicalID, Type: res.Type, Error: plan.refusal}
	case extraneous != "":
		// A property the type does not declare fails the resource at provisioning, without a
		// call, as CloudFormation's property validation does (#1605).
		dr = DeployedResource{LogicalID: logicalID, Type: res.Type, Error: extraneous}
	case plan.update != nil:
		dr, cost, err = plan.update(d, ctx, logicalID, plan.prior, res.Properties, plan.changed, streamID, cctx)
	default:
		dr, cost, err = d.dispatchResource(ctx, logicalID, res, streamID, cctx)
	}
	if err == nil && dr.Error == "" {
		// The attributes only the service's record holds, read once it exists, so Fn::GetAtt
		// on a later resource or an Output answers them.
		d.recordEC2Attributes(ctx, &dr)
		d.recordELBAttributes(ctx, &dr)
	}
	failures := cctx.takeFailures()
	if err != nil || len(failures) == 0 {
		// Stamped after the helper has run and before the result is reported, so
		// the tags are on the resource by the time any Describe can observe it.
		d.stampCFNResourceTags(dr, cctx)
		return dr, cost, err
	}
	// A resolution failure takes precedence over a dispatch error: the request
	// the plugin refused was built from a value that never resolved, so the
	// resolver's reason is the cause and the plugin's is the symptom.
	reason := strings.Join(failures, "; ")
	if dr.Error != "" {
		reason += " (request also refused: " + dr.Error + ")"
	}
	dr.Error = reason
	d.logger.Warn("cfn: resource failed to resolve an intrinsic",
		"logical_id", logicalID, "type", res.Type, "reason", reason)
	return dr, cost, nil
}

// cfnStackResourceTagKeys are the three tags CloudFormation puts on every resource it
// creates, in the order [cfnStackResourceTags] renders them.
//
// Named as constants so the state writer, the docs and the tests cannot drift: a policy
// conditioning on `ec2:ResourceTag/aws:cloudformation:stack-name` — which AWS's own
// `ManagedCloudformationResourcesCleanupPolicy` does — reads the key literally, and a typo
// here would make it silently unsatisfiable rather than wrong.
const (
	cfnTagStackName = "aws:cloudformation:stack-name"
	cfnTagStackID   = "aws:cloudformation:stack-id"
	cfnTagLogicalID = "aws:cloudformation:logical-id"
)

// cfnStackResourceTags is the stamp CloudFormation applies to the resources a stack
// creates, for the one resource named by logicalID.
//
// Provenance: **all three keys are documented**, on the Template Reference's resource-tag
// page — "In addition to any tags you define, CloudFormation automatically creates the
// following stack-level tags with the `aws:` prefix: `aws:cloudformation:{{logical-id}}`,
// `aws:cloudformation:{{stack-id}}`, `aws:cloudformation:{{stack-name}}`". This corrects the
// note that stood here through #746, which said only `stack-name` appeared on a reachable page
// and labeled the triple as observed behavior: every user-guide page checked at the time
// returned an empty body, and the Template Reference page was not among them. The values are
// AWS's: the stack's name, the stack's ARN — which is what `AWS::StackId` resolves to and what
// `CreateStack` reports — and the resource's logical ID.
//
// The same page is the authority for how far the stamp reaches, and it declines to enumerate:
// "The propagation of stack-level tags to resources, including tags with the `aws:` prefix,
// varies by resource type." So substrate states its own rule instead — stamped where substrate
// models tags for the service, skipped silently otherwise — rather than claiming a coverage
// AWS does not publish.
//
// Ordering is fixed and alphabetical by key so a test can assert the rendered tag list
// without depending on map iteration. AWS publishes no order, and a real caller sorts.
func cfnStackResourceTags(stackName, stackID, logicalID string) []EC2Tag {
	return []EC2Tag{
		{Key: cfnTagLogicalID, Value: logicalID},
		{Key: cfnTagStackID, Value: stackID},
		{Key: cfnTagStackName, Value: stackName},
	}
}

// stampCFNResourceTags writes CloudFormation's own three tags onto a resource the deployer
// just created, for the resource types substrate can tag.
//
// Called from the single place that has everything it needs: [StackDeployer.deployResource]
// is the sole caller of [StackDeployer.dispatchResource] and already holds the physical ID,
// the logical ID and the stack's name, account and region. So none of the deploy helpers is
// touched, and a helper added later is stamped without being changed.
//
// **A state write, not a synthesized `CreateTags`.** Two reasons, both fatal to the request
// route: [StackDeployer.dispatch] runs `CheckAccess` on every request it builds, so a
// `CreateTags` would newly require every already-authorized deployment to hold
// `ec2:CreateTags`; and `CreateTags` refuses a key beginning with `aws:`, as AWS does, so the
// stamp would have to punch a hole in the check that exists to keep callers out of AWS's own
// namespace. CloudFormation on AWS is likewise not observed issuing a tagging call — the
// tags are simply on the resource — so the state write is also the closer model.
//
// **Two resolvers, tried in order** (#765). [ec2TaggableResource] goes first: it maps a
// physical ID to a state key by prefix, so it covers all nine EC2 types the deployer creates at
// once plus any tenth that arrives, and EC2 is what the bundled cleanup policy conditions on.
// [cfnStampResourceTags] then covers the services whose tags live in a differently shaped
// store — S3, DynamoDB, Lambda, SQS and ELBv2's four kinds — keyed on the CFN resource type
// rather than the ID, because outside EC2 a physical ID is a bare name with nothing to switch
// on.
//
// EC2 is tried first because its resolver keys on the ID and the type-keyed one cannot reach an
// EC2 type, so the order cannot matter for a resource either could claim. It is stated because
// the reverse order would matter if a future arm overlapped.
//
// A resource neither resolver claims is **skipped silently** — a stack creates far more
// resources than the ~22 services substrate models tags for, so a warning per resource would
// bury a real one, and `docs/services.md` names what is skipped instead. A physical ID that
// merely *looks* like an EC2 ID (a bucket named "i-something") resolves to a state key holding
// no record, which [ec2ApplyTagsToResource] treats as the no-op it treats every absent resource
// as.
//
// A failed resource is not stamped: there may be nothing to stamp, and a tag on a resource
// CloudFormation reports as CREATE_FAILED would claim a resource exists.
func (d *StackDeployer) stampCFNResourceTags(dr DeployedResource, cctx *cfnContext) {
	if dr.Error != "" || dr.PhysicalID == "" || dr.LogicalID == "" {
		return
	}
	reqCtx := &RequestContext{AccountID: cctx.accountID, Region: cctx.region}
	tags := cfnStackResourceTags(
		cctx.stackName,
		cfnStackARN(cctx.region, cctx.accountID, cctx.stackName),
		dr.LogicalID,
	)

	if target, ok := ec2TaggableResource(d.state, reqCtx, dr.PhysicalID); ok && target.resolved() {
		if err := ec2ApplyTagsToResource(d.state, reqCtx, dr.PhysicalID, tags, false); err != nil {
			d.warnStampFailed(dr, err)
		}
		return
	}

	if _, err := cfnStampResourceTags(d.state, d.registry, reqCtx, dr, tags); err != nil {
		d.warnStampFailed(dr, err)
	}
}

// reconcileStackTags propagates the stack's own tags to the resources it created, and removes
// the ones the caller has taken off the stack (#764).
//
// AWS states the propagation on `CreateStack`'s `Tags` member — "CloudFormation also propagates
// these tags to the resources created in the stack" — and it is the half of a stack tag that a
// policy or a cost report actually reads: a stack whose tags only its own `DescribeStacks`
// reports cannot be the subject of an `aws:ResourceTag` condition on anything it built.
//
// **Run over the whole result rather than per resource**, unlike the three-key stamp, and for a
// reason the update path forces: an update keeps an unchanged resource without dispatching it
// (#1427), so the per-resource stamp never runs for it, and a tag added to an existing stack has
// to reach exactly those resources.
//
// A result record with no physical ID is filled from the previous record. Since #1427 a cleared
// already-exists refusal is replaced by the previous record as it happens (see
// [cfnResourcePlan.clearsRefusal]), so this fallback serves only a record written before that,
// and it is sound for the same reason: the previous record shows this stack deployed that
// logical ID from an identical declaration, so it is the same resource.
//
// A failure does not fail the resource or the stack, for [StackDeployer.warnStampFailed]'s
// reason: the resource deployed, and a stack does not fail because a tag could not be written
// onto it. It is logged, and since #1138 it is also returned, so the caller records it on
// [DeployResult.TagPropagationFailures]; before that the log line was its only report. A
// resource no arm models tags for is skipped silently, as before: that is a (false, nil)
// return, not a failure.
func (d *StackDeployer) reconcileStackTags(
	resources []DeployedResource, prev *CFNStackState, next map[string]string,
) []CFNTagPropagationFailure {
	if d.state == nil || (len(prev.stackTags()) == 0 && len(next) == 0) {
		return nil
	}
	var failures []CFNTagPropagationFailure
	reqCtx := &RequestContext{AccountID: d.identity.accountID, Region: d.identity.region}
	previous := prev.stackTags()

	for _, dr := range resources {
		if dr.Error != "" || dr.LogicalID == "" {
			continue
		}
		if dr.PhysicalID == "" && dr.ARN == "" {
			if earlier, ok := prev.deployedResource(dr.LogicalID); ok {
				dr = earlier
			}
		}
		if dr.PhysicalID == "" && dr.ARN == "" {
			continue
		}
		if _, err := cfnPropagateStackTags(d.state, d.registry, reqCtx, dr, previous, next); err != nil {
			d.logger.Warn("cfn: could not propagate the stack's tags",
				"logical_id", dr.LogicalID, "physical_id", dr.PhysicalID, "error", err)
			failures = append(failures, CFNTagPropagationFailure{
				LogicalID:  dr.LogicalID,
				PhysicalID: dr.PhysicalID,
				Type:       dr.Type,
				Error:      err.Error(),
			})
		}
	}
	return failures
}

// warnStampFailed records a stamp that could not be written.
//
// Recorded, not returned: the resource itself deployed, and a stack does not fail because
// CloudFormation could not write its own bookkeeping tag. Logged only when a resolver *found*
// somewhere to write and the write failed — a resource no resolver claims is not a failure and
// says nothing.
func (d *StackDeployer) warnStampFailed(dr DeployedResource, err error) {
	d.logger.Warn("cfn: could not stamp aws:cloudformation tags",
		"logical_id", dr.LogicalID, "physical_id", dr.PhysicalID, "error", err)
}

// dispatchResource routes a CFN resource type to its deploy helper in [cfnResourceDeployers].
//
// A type with no helper reaches the generic stub only in lenient mode, or on the rollback of a
// stack deployed before strict mode existed: strict mode refuses such a template at the request
// ([cfnUnemulatedTypes], #1605).
func (d *StackDeployer) dispatchResource(
	ctx context.Context,
	logicalID string,
	res cfnResource,
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	if deploy, ok := cfnResourceDeployers[res.Type]; ok {
		return deploy(d, ctx, logicalID, res.Properties, streamID, cctx)
	}
	d.logger.Warn("unknown CloudFormation resource type; using generic stub",
		"logical_id", logicalID,
		"type", res.Type,
		// A loaded plugin means the API handler may exist and the type merely
		// needs wiring, which is a different problem from an unsupported
		// service (#388).
		"service_plugin_loaded", d.servicePluginLoaded(res.Type),
	)
	return d.deployGenericStub(ctx, logicalID, res.Type, res.Properties, cctx)
}

// deployS3Bucket creates an S3 bucket for the given CFN resource.
func (d *StackDeployer) deployS3Bucket(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	bucketName := strings.ToLower(resolveStringProp(props, "BucketName",
		cfnGeneratedName(cctx, "AWS::S3::Bucket", logicalID), cctx))

	req := &AWSRequest{
		Service:   "s3",
		Operation: "PUT",
		Path:      "/" + bucketName,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::S3::Bucket",
		PhysicalID: bucketName,
		// CreateBucket returns no body to read an ARN out of, and an S3 bucket ARN carries
		// no region or account, so it is a pure function of the name — built by the same
		// s3BucketARN the tagging API reports and an event notification embeds, so
		// `!GetAtt Bucket.Arn` cannot disagree with either (#827).
		ARN: s3BucketARN(bucketName),
	}
	if routeErr != nil {
		// Recorded on the resource, not returned — a returned error aborts the
		// stack. The early return skips the follow-up configuration requests: a
		// bucket that was refused has nothing to configure, and each request would
		// be refused in turn with NoSuchBucket and recorded in the event log,
		// leaving a replay to explain requests no real client would have sent.
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}

	// The bucket's declared configuration, through the same helper its update uses (#1545):
	// versioning, which this applied before, and default encryption and CORS, which it did not.
	// A refusal fails the resource, as an update's does.
	var declared []string
	for _, p := range cfnS3BucketConfigProperties {
		if _, ok := props[p]; ok {
			declared = append(declared, p)
		}
	}
	configCost, configErr := d.configureS3Bucket(ctx, bucketName, props, declared, true, streamID, cctx)
	cost += configCost
	if configErr != nil {
		dr.Error = configErr.Error()
	}

	return dr, cost, nil
}

// deployIAMRole creates an IAM role for the given CFN resource.
//
// CreateRole carries the name, path, trust policy, description, MaxSessionDuration and Tags.
// PermissionsBoundary, ManagedPolicyArns and Policies are applied after it, through
// PutRolePermissionsBoundary, AttachRolePolicy and PutRolePolicy (see applyIAMRoleExtras) — until
// v0.129.0 the last two were ignored, so a role a template granted a managed or inline policy was
// created with neither (#1545). RoleId is CreateRole's own response member.
func (d *StackDeployer) deployIAMRole(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	roleName := resolveStringProp(props, "RoleName",
		cfnGeneratedName(cctx, "AWS::IAM::Role", logicalID), cctx)

	body := map[string]string{
		"RoleName":                 roleName,
		"Path":                     resolveStringProp(props, "Path", "/", cctx),
		"AssumeRolePolicyDocument": cfnJSONDocument(props["AssumeRolePolicyDocument"], cctx),
		"Description":              resolveStringProp(props, "Description", "", cctx),
	}
	if msd := resolveStringProp(props, "MaxSessionDuration", "", cctx); msd != "" {
		body["MaxSessionDuration"] = msd
	}
	tags := cfnTagMap(props["Tags"], cctx)
	resp, cost, routeErr := d.iamCall(ctx, "CreateRole", body, cfnIAMTagParams(tags), streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Role",
		PhysicalID: roleName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused create is reported on the resource.
	}
	if resp != nil {
		var result struct {
			ARN    string `xml:"CreateRoleResult>Role>Arn"`
			RoleID string `xml:"CreateRoleResult>Role>RoleId"`
		}
		if xmlErr := xmlUnmarshalIAM(resp.Body, &result); xmlErr == nil {
			dr.ARN = result.ARN
			cfnSetMetadata(&dr, "RoleId", result.RoleID)
		}
	}

	rec := cfnIAMRecord{TagKeys: sortedStringKeys(tags)}
	cost += d.applyIAMRoleExtras(ctx, &dr, props, &rec, streamID, cctx)
	if dr.Error == "" {
		if err := setCFNIAMRecord(&dr, rec); err != nil {
			return DeployedResource{}, cost, err
		}
	}
	return dr, cost, nil
}

// deployLambdaFunction creates a Lambda function for the given CFN resource.
func (d *StackDeployer) deployLambdaFunction(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	fnName := resolveStringProp(props, "FunctionName",
		cfnGeneratedName(cctx, "AWS::Lambda::Function", logicalID), cctx)

	// The configuration members are rendered by the same function UpdateFunctionConfiguration's
	// body is (#1545), so a create and an update of one declaration cannot disagree. Environment
	// joined them there: it was not forwarded before, so a function's variables never reached it.
	body := cfnLambdaConfiguration(props, cctx)
	body["FunctionName"] = fnName
	// Code is what the function is. Omitting it — as this deployer did — meant every
	// CloudFormation-deployed function reported CodeSize 0 and an empty CodeSha256
	// however its template declared its code, and a container-image function lost
	// both its image and its package type. Called after Runtime is set, because an
	// inline package's file name depends on it.
	cfnLambdaCode(body, props, cctx)

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal lambda body: %w", err)
	}

	req := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/functions",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Lambda::Function",
		PhysicalID: fnName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			FunctionArn string `json:"FunctionArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.FunctionArn
		}
	}

	return dr, cost, nil
}

// cfnLambdaCode carries an AWS::Lambda::Function resource's Code property into a
// CreateFunction request body.
//
// The resource type and CreateFunction spell the S3 and image forms identically, so
// S3Bucket/S3Key/S3ObjectVersion and ImageUri are forwarded as they are. An ImageUri
// also implies PackageType: Image, which the resource type declares separately but a
// template need not state — real CloudFormation rejects an ImageUri alongside
// PackageType: Zip, so inferring it cannot contradict a valid template.
//
// ZipFile is the one form where the two spellings differ and the difference matters.
// The resource type's ZipFile is "the source code of your Lambda function", plain
// text: "CloudFormation places it in a file named index and zips it to create a
// deployment package". CreateFunction's ZipFile is "the base64-encoded contents of
// the deployment package". Forwarding the template's string verbatim would therefore
// hand Lambda something that is not a package and usually not even valid base64,
// which is why this builds the archive CloudFormation would have built — one entry
// named index plus the runtime's extension — and sends that. The consequence is
// visible: CodeSize is the archive's byte count and CodeSha256 a digest of real
// bytes, where an unzipped forward would have left both empty.
//
// A resource with no Code at all is left alone rather than sent an empty Code
// object: substrate's CreateFunction treats an absent Code as a function with no
// stored package, which is the same outcome and one fewer thing to explain.
func cfnLambdaCode(body, props map[string]interface{}, cctx *cfnContext) {
	code, ok := props["Code"].(map[string]interface{})
	if !ok {
		return
	}
	out := map[string]interface{}{}
	for _, key := range []string{"S3Bucket", "S3Key", "S3ObjectVersion"} {
		if v := resolveStringProp(code, key, "", cctx); v != "" {
			out[key] = v
		}
	}
	if src := resolveStringProp(code, "ZipFile", "", cctx); src != "" {
		runtime, _ := body["Runtime"].(string)
		pkg, err := cfnInlineZipPackage(src, runtime)
		if err == nil {
			out["ZipFile"] = base64.StdEncoding.EncodeToString(pkg)
		}
	}
	if uri := resolveStringProp(code, "ImageUri", "", cctx); uri != "" {
		out["ImageUri"] = uri
		body["PackageType"] = "Image"
	}
	if len(out) > 0 {
		body["Code"] = out
	}
	// PackageType may also be declared outright, for a template that says Image
	// without a Code.ImageUri or states Zip explicitly.
	if pt := resolveStringProp(props, "PackageType", "", cctx); pt != "" {
		body["PackageType"] = pt
	}
}

// cfnInlineZipPackage builds the deployment package CloudFormation builds for an
// inline Code.ZipFile: a ZIP archive holding the source as a single file named
// index, with the extension the runtime reads.
//
// The reference names the extension for one runtime family only — "when you specify
// source code inline for a Node.js function, the index file that CloudFormation
// creates uses the extension .js" — and inline code is documented as "(Node.js and
// Python)", so Python's .py is the only other case. An unrecognized runtime gets a
// bare index rather than a guess; the archive's size and digest, which is what this
// exists to make right, do not depend on the name being executable, and substrate
// never runs the package.
func cfnInlineZipPackage(source, runtime string) ([]byte, error) {
	name := "index"
	switch {
	case strings.HasPrefix(runtime, "nodejs"):
		name = "index.js"
	case strings.HasPrefix(runtime, "python"):
		name = "index.py"
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// A zero timestamp keeps the archive deterministic: the same template deployed
	// twice has to produce the same CodeSize and CodeSha256, which is the whole
	// point of reporting a digest rather than a random value.
	w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate})
	if err != nil {
		return nil, fmt.Errorf("create inline lambda package entry: %w", err)
	}
	if _, err := w.Write([]byte(source)); err != nil {
		return nil, fmt.Errorf("write inline lambda package entry: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("close inline lambda package: %w", err)
	}
	return buf.Bytes(), nil
}

// deploySQSQueue creates an SQS queue for the given CFN resource.
func (d *StackDeployer) deploySQSQueue(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	queueName := resolveStringProp(props, "QueueName",
		cfnGeneratedName(cctx, "AWS::SQS::Queue", logicalID), cctx)

	params := map[string]string{
		"Action":    "CreateQueue",
		"QueueName": queueName,
	}
	// Forward declared queue attributes so they round-trip into SQSQueue.Attributes
	// (CreateQueue persists them via parseSQSAttributes) and become drift-checkable.
	attrIdx := 0
	for _, name := range sqsDriftAttributes {
		if _, declared := props[name]; !declared {
			continue
		}
		if v := resolveValue(props[name], cctx); v != "" {
			attrIdx++
			params[fmt.Sprintf("Attribute.%d.Name", attrIdx)] = name
			params[fmt.Sprintf("Attribute.%d.Value", attrIdx)] = v
		}
	}

	req := &AWSRequest{
		Service:   "sqs",
		Operation: "CreateQueue",
		Body:      nil,
		Headers:   map[string]string{},
		Params:    params,
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SQS::Queue",
		PhysicalID: queueName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else {
		dr.ARN = sqsQueueARN(cctx.region, cctx.accountID, queueName)
	}
	_ = resp

	return dr, cost, nil
}

// deployDynamoDBTable creates a DynamoDB table for the given CFN resource.
func (d *StackDeployer) deployDynamoDBTable(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	tableName := resolveStringProp(props, "TableName",
		cfnGeneratedName(cctx, "AWS::DynamoDB::Table", logicalID), cctx)

	// Build the CreateTable body from CFN properties.
	body := map[string]interface{}{
		"TableName": tableName,
	}

	// The DynamoDB API is natively PascalCase, so these properties are forwarded
	// under CloudFormation's own member names — only the intrinsics inside them
	// need resolving (#526).
	// A literal passes through resolveNested untouched, so walking these costs a
	// numeric member nothing: `{"ReadCapacityUnits": 5}` stays the integer 5. An
	// intrinsic *inside* a numeric member is the one case still unresolvable,
	// since a resolved intrinsic is a string — but that member fails to
	// unmarshal today either way, so the walk does not make it worse.
	for _, key := range []string{
		"KeySchema",
		"AttributeDefinitions",
		"ProvisionedThroughput",
		"GlobalSecondaryIndexes",
		"LocalSecondaryIndexes",
		"StreamSpecification",
	} {
		if v, ok := props[key]; ok {
			body[key] = resolveNested(v, cctx)
		}
	}
	if billingMode, ok := props["BillingMode"]; ok {
		body["BillingMode"] = resolveValue(billingMode, cctx)
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal dynamodb body: %w", err)
	}

	req := &AWSRequest{
		Service:   "dynamodb",
		Operation: "CreateTable",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::DynamoDB::Table",
		PhysicalID: tableName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			TableDescription struct {
				// TableArn is DynamoDB's published member name, and must track the
				// tag on DynamoDBTable.TableARN or this decode silently reads empty.
				TableARN string `json:"TableArn"`
			} `json:"TableDescription"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.TableDescription.TableARN
		}
	}

	// Handle TimeToLiveSpecification if present.
	if ttlSpec, ok := props["TimeToLiveSpecification"]; ok && routeErr == nil {
		ttlBody := map[string]interface{}{
			"TableName":               tableName,
			"TimeToLiveSpecification": ttlSpec,
		}
		ttlBytes, marshalErr := json.Marshal(ttlBody)
		if marshalErr == nil {
			ttlReq := &AWSRequest{
				Service:   "dynamodb",
				Operation: "UpdateTimeToLive",
				Body:      ttlBytes,
				Headers:   map[string]string{},
				Params:    map[string]string{},
			}
			_, _, _ = d.dispatch(ctx, ttlReq, streamID)
		}
	}

	return dr, cost, nil
}

// deployEC2VPC creates an EC2 VPC for the given CFN resource.
func (d *StackDeployer) deployEC2VPC(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	cidr := resolveStringProp(props, "CidrBlock", "10.0.0.0/16", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateVpc",
		Params:    map[string]string{"Action": "CreateVpc", "CidrBlock": cidr},
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::VPC"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Extract vpcId from XML response body.
		dr.PhysicalID = extractXMLField(resp.Body, "vpcId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2Subnet creates an EC2 subnet for the given CFN resource.
func (d *StackDeployer) deployEC2Subnet(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	cidr := resolveStringProp(props, "CidrBlock", "10.0.0.0/24", cctx)
	az := resolveStringProp(props, "AvailabilityZone", cctx.region+"a", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateSubnet",
		Params: map[string]string{
			"Action":           "CreateSubnet",
			"VpcId":            vpcID,
			"CidrBlock":        cidr,
			"AvailabilityZone": az,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::Subnet"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "subnetId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2SecurityGroup creates an EC2 security group for the given CFN resource.
func (d *StackDeployer) deployEC2SecurityGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	groupName := resolveStringProp(props, "GroupName", cfnGeneratedName(cctx, "AWS::EC2::SecurityGroup", logicalID), cctx)
	description := resolveStringProp(props, "GroupDescription", groupName, cctx)
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateSecurityGroup",
		Params: map[string]string{
			"Action":    "CreateSecurityGroup",
			"GroupName": groupName,
			// API_CreateSecurityGroup's parameter is GroupDescription; this sent Description,
			// which the handler never read, so every stack's group had an empty description.
			"GroupDescription": description,
			"VpcId":            vpcID,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::SecurityGroup"}
	if routeErr != nil {
		// A failed resource is recorded on the DeployedResource, not returned — a
		// returned error aborts the whole stack.
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr
	}
	if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "groupId")
		dr.ARN = dr.PhysicalID
	}
	// Apply the rules declared inline on the group. Without this the group is
	// created with no rules at all (#388).
	if dr.PhysicalID != "" {
		ruleCost, ruleErr := d.authorizeInlineSGRules(ctx, dr.PhysicalID, props, streamID, cctx)
		cost += ruleCost
		if ruleErr != "" {
			dr.Error = ruleErr
		}
	}
	return dr, cost, nil
}

// deployEC2InternetGateway creates an EC2 internet gateway for the given CFN resource.
func (d *StackDeployer) deployEC2InternetGateway(
	ctx context.Context,
	logicalID string,
	_ map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateInternetGateway",
		Params:    map[string]string{"Action": "CreateInternetGateway"},
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::InternetGateway"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "internetGatewayId")
		dr.ARN = dr.PhysicalID
	}
	_ = cctx
	return dr, cost, nil
}

// deployEC2RouteTable creates an EC2 route table for the given CFN resource.
func (d *StackDeployer) deployEC2RouteTable(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateRouteTable",
		Params:    map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID},
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::RouteTable"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "routeTableId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2Instance launches EC2 instances for the given CFN resource.
func (d *StackDeployer) deployEC2Instance(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// The default is a bundled AMI for the stack's Region rather than the literal
	// "ami-00000000" this used through v0.107.0. That literal existed only because
	// nothing checked an AMI's existence; now that RunInstances refuses one that names
	// nothing (#733), a template omitting ImageId would have failed to deploy on
	// substrate's own default. Falling back to the image a caller would have discovered
	// through /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64
	// keeps that deployment working and keeps the AMI one substrate can resolve.
	imageID := resolveStringProp(props, "ImageId", BundledImageID(cctx.region, bundledImageDefaultParameter), cctx)
	instanceType := resolveStringProp(props, "InstanceType", "t3.micro", cctx)
	subnetID := resolveStringProp(props, "SubnetId", "", cctx)
	params := map[string]string{
		"Action":       "RunInstances",
		"ImageId":      imageID,
		"InstanceType": instanceType,
		"MinCount":     "1",
		"MaxCount":     "1",
		"SubnetId":     subnetID,
	}
	// An instance profile reference is only resolvable now that
	// AWS::IAM::InstanceProfile creates a real profile (#388).
	if profile := resolveStringProp(props, "IamInstanceProfile", "", cctx); profile != "" {
		params["IamInstanceProfile.Name"] = profile
	}
	if keyName := resolveStringProp(props, "KeyName", "", cctx); keyName != "" {
		params["KeyName"] = keyName
	}
	for i, sg := range resolveStringList(props["SecurityGroupIds"], cctx) {
		params[fmt.Sprintf("SecurityGroupId.%d", i+1)] = sg
	}
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "RunInstances",
		Params:    params,
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::Instance"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "instanceId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployELBTargetGroup creates an ELBv2 target group for the given CFN resource.
func (d *StackDeployer) deployELBTargetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateTargetGroup",
		Params: map[string]string{
			"Action":     "CreateTargetGroup",
			"Name":       name,
			"Protocol":   resolveStringProp(props, "Protocol", "HTTP", cctx),
			"Port":       resolveStringProp(props, "Port", "80", cctx),
			"VpcId":      resolveStringProp(props, "VpcId", "", cctx),
			"TargetType": resolveStringProp(props, "TargetType", "instance", cctx),
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::TargetGroup", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "TargetGroupArn")
	}
	return dr, cost, nil
}

// deployELBLoadBalancer creates an ELBv2 load balancer for the given CFN resource.
func (d *StackDeployer) deployELBLoadBalancer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateLoadBalancer",
		Params: map[string]string{
			"Action": "CreateLoadBalancer",
			"Name":   name,
			"Type":   resolveStringProp(props, "Type", "application", cctx),
			"Scheme": resolveStringProp(props, "Scheme", "internet-facing", cctx),
		},
		Headers: map[string]string{},
	}
	// The load balancer's own subnets and security groups, which the template declared and the
	// request used to drop: SecurityGroups is also what Fn::GetAtt SecurityGroups answers (#1591).
	for _, list := range []string{"Subnets", "SecurityGroups"} {
		for i, v := range resolveValueList(props[list], cctx) {
			req.Params[fmt.Sprintf("%s.member.%d", list, i+1)] = v
		}
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::LoadBalancer", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "LoadBalancerArn")
		// GetAtt DNSName. Read out of the response the plugin already renders it in rather
		// than rebuilt here, so a template's DNS name is the one describe-load-balancers
		// reports for the same balancer.
		cfnSetMetadata(&dr, "DNSName", extractXMLField(resp.Body, "DNSName"))
	}
	return dr, cost, nil
}

// deployELBClassicLoadBalancer creates a Classic Load Balancer for the given CFN resource.
//
// This is #844's Tier 2. `AWS::ElasticLoadBalancing::LoadBalancer` — no `V2` — is the classic type,
// and before this it had no arm in [StackDeployer.dispatchResource] at all: a template declaring one
// reached the generic stub, so the stack reported `CREATE_COMPLETE` for a load balancer that no
// `DescribeLoadBalancers` of either generation could find.
//
// **`Version` is the whole of the routing.** [elbClassicRequest] discriminates on the Query
// `Version` member, and `CreateLoadBalancer` is one of the six action names both generations
// publish — so the same params without `Version` reach the ELBv2 handler and quietly create an
// Application Load Balancer instead. Nothing else here distinguishes the two, which is why
// cfn_elb_classic_test.go asserts the result through the **classic** describe.
//
// **`Ref` needs no arm in [cfnRefValue].** AWS publishes it as "the name of the load balancer"
// (`aws-properties-ec2-elb.html`), which is the physical ID set below, so the default answer is
// already the right one. `docs/services.md` recorded the opposite — the DNS name — and that entry
// is deleted rather than implemented. The DNS name is `Fn::GetAtt DNSName`, which is the whole of
// this operation's published Response Elements, and it is recorded in the resource's metadata where
// step 2 of [cfnGetAttValue] finds it.
//
// `SourceSecurityGroup.GroupName` and `SourceSecurityGroup.OwnerAlias` are read back from the
// classic `DescribeLoadBalancers` after the create and recorded under those names, where step 2
// of [cfnGetAttValue] finds them (#1421); the create derives them from EC2, as
// elb_classic_source_sg.go describes. `CanonicalHostedZoneName` and `CanonicalHostedZoneNameID`
// remain #1013 absences: substrate mints no hosted zone for a classic load balancer, so each
// answers empty. Inventing a hosted-zone ID would be #561 in another costume.
//
// **The ARN is built rather than read.** No classic response carries one — the create's body is
// `DNSName` alone — so it comes from [elbClassicLoadBalancerARN], the builder the create itself
// used, which cannot disagree with the record. It is what lets the stack stamp and the Resource
// Groups Tagging API reach this record at all; see [cfnELBStampableTypes].
func (d *StackDeployer) deployELBClassicLoadBalancer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "LoadBalancerName", logicalID, cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateLoadBalancer",
		Params: map[string]string{
			"Action":           "CreateLoadBalancer",
			"Version":          elbClassicAPIVersion,
			"LoadBalancerName": name,
			"Scheme":           resolveStringProp(props, "Scheme", "internet-facing", cctx),
		},
		Headers: map[string]string{},
	}
	for k, v := range cfnELBClassicListenerParams(props["Listeners"], cctx) {
		req.Params[k] = v
	}
	// `AvailabilityZones` and `Subnets` are both `Conditional` on the CFN type and the classic
	// create takes either, so neither is defaulted: a template that supplies no placement is
	// forwarded as-is and refused by the API rather than by a guess made here.
	for _, list := range []string{"AvailabilityZones", "Subnets", "SecurityGroups"} {
		for i, v := range resolveStringList(props[list], cctx) {
			req.Params[fmt.Sprintf("%s.member.%d", list, i+1)] = v
		}
	}
	for k, v := range cfnELBClassicTagParams(props["Tags"], cctx) {
		req.Params[k] = v
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ElasticLoadBalancing::LoadBalancer",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = elbClassicLoadBalancerARN(cctx.region, cctx.accountID, name)
		cfnSetMetadata(&dr, "DNSName", extractXMLField(resp.Body, "DNSName"))
		cost += d.recordClassicSourceSecurityGroup(ctx, &dr, streamID)
	}
	return dr, cost, nil
}

// recordClassicSourceSecurityGroup reads a classic load balancer's SourceSecurityGroup back through
// the classic DescribeLoadBalancers and records its two members as the
// `SourceSecurityGroup.GroupName` and `SourceSecurityGroup.OwnerAlias` attributes (#1421). A load
// balancer with no source security group, or a read that fails, records neither, and both resolve
// to empty.
func (d *StackDeployer) recordClassicSourceSecurityGroup(ctx context.Context, dr *DeployedResource, streamID string) float64 {
	resp, cost, err := d.dispatch(ctx, &AWSRequest{
		Service: "elasticloadbalancing", Operation: "DescribeLoadBalancers", Headers: map[string]string{},
		Params: map[string]string{
			"Action": "DescribeLoadBalancers", "Version": elbClassicAPIVersion,
			"LoadBalancerNames.member.1": dr.PhysicalID,
		},
	}, streamID)
	if err != nil || resp == nil {
		return cost
	}
	var out struct {
		Source struct {
			GroupName  string `xml:"GroupName"`
			OwnerAlias string `xml:"OwnerAlias"`
		} `xml:"DescribeLoadBalancersResult>LoadBalancerDescriptions>member>SourceSecurityGroup"`
	}
	if xml.Unmarshal(resp.Body, &out) != nil {
		return cost
	}
	cfnSetMetadata(dr, "SourceSecurityGroup.GroupName", out.Source.GroupName)
	cfnSetMetadata(dr, "SourceSecurityGroup.OwnerAlias", out.Source.OwnerAlias)
	return cost
}

// cfnELBClassicListenerParams flattens a CloudFormation `Listeners` list into the
// `Listeners.member.N.*` query parameters the 2012-06-01 `CreateLoadBalancer` takes.
//
// `Listeners` is the CFN type's one `Required: Yes` property and the classic create's one required
// list, so the two agree about it: a template omitting it produces no parameters here and the API
// refuses the request for `Listeners.member.1 is required`, which is the refusal CloudFormation's
// own validator would have raised.
//
// All five members [elbClassicParseListeners] reads are forwarded. `PolicyNames` is the sixth CFN
// member and is deliberately dropped: substrate models no classic load-balancer policies, so
// forwarding it would name a policy no `DescribeLoadBalancerPolicies` could resolve.
//
// The stop-at-the-first-member-with-no-Protocol rule is [cfnELBActionParams]', for its reason: the
// wire format indexes from 1 contiguously, so an entry carrying no `Protocol` ends the list rather
// than leaving a gap the plugin's own walk would stop at anyway.
func cfnELBClassicListenerParams(prop interface{}, cctx *cfnContext) map[string]string {
	listeners, isList := prop.([]interface{})
	if !isList {
		return nil
	}
	params := make(map[string]string, len(listeners)*4)
	for i, l := range listeners {
		listener, isMap := l.(map[string]interface{})
		if !isMap {
			continue
		}
		protocol := resolveStringProp(listener, "Protocol", "", cctx)
		if protocol == "" {
			break
		}
		prefix := fmt.Sprintf("Listeners.member.%d.", i+1)
		params[prefix+"Protocol"] = protocol
		for _, member := range []string{
			"LoadBalancerPort", "InstancePort", "InstanceProtocol", "SSLCertificateId",
		} {
			if v := resolveStringProp(listener, member, "", cctx); v != "" {
				params[prefix+member] = v
			}
		}
	}
	return params
}

// cfnELBClassicTagParams flattens a CloudFormation `Tags` list into the `Tags.member.N.{Key,Value}`
// parameters the 2012-06-01 `CreateLoadBalancer` takes.
//
// Forwarded on the create rather than stamped afterwards, so the tags a template declares are
// validated by the operation that publishes `DuplicateTagKeys` and `TooManyTags` for them — a
// template declaring eleven tags fails its create, as it does on AWS, instead of producing a load
// balancer over its own published quota.
//
// [resolveValue] rather than a string assertion for both members, for #526's reason: a tag value is
// very often `{"Fn::Sub": "${AWS::StackName}-web"}`, and asserting on `string` dropped it silently.
// An entry with no key is skipped rather than ending the walk, because the key is what the query
// list is indexed by and a keyless entry carries nothing the API could store.
func cfnELBClassicTagParams(prop interface{}, cctx *cfnContext) map[string]string {
	tags, isList := prop.([]interface{})
	if !isList {
		return nil
	}
	params := make(map[string]string, len(tags)*2)
	index := 0
	for _, t := range tags {
		tag, isMap := t.(map[string]interface{})
		if !isMap {
			continue
		}
		key := resolveValue(tag["Key"], cctx)
		if key == "" {
			continue
		}
		index++
		prefix := fmt.Sprintf("Tags.member.%d.", index)
		params[prefix+"Key"] = key
		params[prefix+"Value"] = resolveValue(tag["Value"], cctx)
	}
	return params
}

// cfnELBActionParams flattens a CloudFormation listener action list into the
// `<member>.member.N.*` query parameters the ELBv2 API takes.
//
// The member name is a parameter because the two callers differ: CreateListener takes
// DefaultActions and CreateRule takes Actions.
//
// Only Type and TargetGroupArn are forwarded, which are the two members the ELB plugin reads.
// TargetGroupArn is why this exists: it is where a `!Ref` on a target group lands, and until #827
// the actions were dropped entirely, so a listener deployed from a template routed to nothing and
// DescribeListeners reported it with no default action at all.
func cfnELBActionParams(prop interface{}, member string, cctx *cfnContext) map[string]string {
	actions, isList := prop.([]interface{})
	if !isList {
		return nil
	}
	params := make(map[string]string, len(actions)*2)
	for i, a := range actions {
		action, isMap := a.(map[string]interface{})
		if !isMap {
			continue
		}
		// The plugin stops at the first member with no Type, matching the wire format's
		// 1-based contiguous indexing, so an action carrying no Type ends the list rather
		// than leaving a gap the reader would silently stop at anyway.
		actionType := resolveStringProp(action, "Type", "", cctx)
		if actionType == "" {
			break
		}
		prefix := fmt.Sprintf("%s.member.%d.", member, i+1)
		params[prefix+"Type"] = actionType
		if tg := resolveStringProp(action, "TargetGroupArn", "", cctx); tg != "" {
			params[prefix+"TargetGroupArn"] = tg
		}
	}
	return params
}

// deployELBListener creates an ELBv2 listener for the given CFN resource.
func (d *StackDeployer) deployELBListener(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	lbARN := resolveStringProp(props, "LoadBalancerArn", "", cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateListener",
		Params: map[string]string{
			"Action":          "CreateListener",
			"LoadBalancerArn": lbARN,
			"Protocol":        resolveStringProp(props, "Protocol", "HTTP", cctx),
			"Port":            resolveStringProp(props, "Port", "80", cctx),
		},
		Headers: map[string]string{},
	}
	for k, v := range cfnELBActionParams(props["DefaultActions"], "DefaultActions", cctx) {
		req.Params[k] = v
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::Listener"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "ListenerArn")
		dr.PhysicalID = dr.ARN
	}
	return dr, cost, nil
}

// deployELBListenerRule creates an ELBv2 listener rule for the given CFN resource.
func (d *StackDeployer) deployELBListenerRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	listenerARN := resolveStringProp(props, "ListenerArn", "", cctx)
	priority := resolveStringProp(props, "Priority", "1", cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateRule",
		Params: map[string]string{
			"Action":      "CreateRule",
			"ListenerArn": listenerARN,
			"Priority":    priority,
		},
		Headers: map[string]string{},
	}
	for k, v := range cfnELBActionParams(props["Actions"], "Actions", cctx) {
		req.Params[k] = v
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::ListenerRule"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "RuleArn")
		dr.PhysicalID = dr.ARN
	}
	return dr, cost, nil
}

// deployRoute53HostedZone creates a Route 53 hosted zone for the given CFN resource.
func (d *StackDeployer) deployRoute53HostedZone(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	body := `<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>` + name +
		`</Name><CallerReference>cfn-` + logicalID + `</CallerReference></CreateHostedZoneRequest>`
	req := &AWSRequest{
		Service:   "route53",
		Operation: "POST",
		Path:      "/2013-04-01/hostedzone",
		Body:      []byte(body),
		Headers:   map[string]string{"Content-Type": "application/xml"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Route53::HostedZone", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// CreateHostedZone answers its Id member in the API's path form, /hostedzone/Z23ABC4XYZL05B.
		// The CloudFormation page publishes Ref (and Fn::GetAtt Id) as "the hosted zone ID, such as
		// Z23ABC4XYZL05B" — the bare ID — so the prefix comes off here. Before #1256 the path form was
		// both the physical ID and the ARN, so a RecordSet naming its zone by Ref built
		// /2013-04-01/hostedzone//hostedzone/Z…/rrset and was refused, and the stack delete sent the
		// same doubled path. The ARN is the form the same page quotes for aws:SourceArn,
		// arn:aws:route53:::hostedzone/<id>.
		zoneID := strings.TrimPrefix(extractXMLField(resp.Body, "Id"), route53HostedZonePathPrefix)
		dr.PhysicalID = zoneID
		dr.ARN = "arn:aws:route53:::hostedzone/" + zoneID
		dr.Metadata = map[string]interface{}{"Id": zoneID}
	}
	return dr, cost, nil
}

// route53HostedZonePathPrefix is the prefix Route 53's API puts on a hosted zone's Id member.
const route53HostedZonePathPrefix = "/hostedzone/"

// deployRoute53RecordSet creates a Route 53 record set for the given CFN resource.
func (d *StackDeployer) deployRoute53RecordSet(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	zoneID := resolveStringProp(props, "HostedZoneId", "", cctx)
	name := resolveStringProp(props, "Name", "", cctx)
	rtype := resolveStringProp(props, "Type", "A", cctx)
	ttl := resolveStringProp(props, "TTL", "300", cctx)
	value := resolveStringProp(props, "ResourceRecords", "", cctx)
	body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>UPSERT</Action><ResourceRecordSet>` +
		`<Name>` + name + `</Name><Type>` + rtype + `</Type><TTL>` + ttl + `</TTL>` +
		`<ResourceRecords><ResourceRecord><Value>` + value + `</Value></ResourceRecord></ResourceRecords>` +
		`</ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	req := &AWSRequest{
		Service:   "route53",
		Operation: "POST",
		Path:      "/2013-04-01/hostedzone/" + zoneID + "/rrset",
		Body:      []byte(body),
		Headers:   map[string]string{"Content-Type": "application/xml"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Route53::RecordSet", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// cfnRecordSetInGroup returns one of a RecordSetGroup's record sets with the group's zone filled in.
//
// AWS::Route53::RecordSetGroup declares HostedZoneId and HostedZoneName on the group, for every
// record set in it; a RecordSet element inside the group does not repeat it. Until #1256 each
// element was deployed with its own properties alone, so it named no zone at all and built
// /2013-04-01/hostedzone//rrset. A zone the element does name is kept. The template's map is not
// modified.
func cfnRecordSetInGroup(group, rs map[string]interface{}) map[string]interface{} {
	_, hasID := rs["HostedZoneId"]
	_, hasName := rs["HostedZoneName"]
	if hasID || hasName {
		return rs
	}
	out := make(map[string]interface{}, len(rs)+2)
	for k, v := range rs {
		out[k] = v
	}
	for _, k := range []string{"HostedZoneId", "HostedZoneName"} {
		if v, ok := group[k]; ok {
			out[k] = v
		}
	}
	return out
}

// deployRoute53RecordSetGroup creates multiple Route 53 record sets from a CFN
// RecordSetGroup resource by iterating over its RecordSets list.
func (d *StackDeployer) deployRoute53RecordSetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	var totalCost float64
	if rsList, ok := props["RecordSets"].([]interface{}); ok {
		for i, rsRaw := range rsList {
			rsProps, ok := rsRaw.(map[string]interface{})
			if !ok {
				continue
			}
			childID := fmt.Sprintf("%s-RecordSet%d", logicalID, i)
			child, cost, err := d.deployRoute53RecordSet(ctx, childID, cfnRecordSetInGroup(props, rsProps), streamID, cctx)
			if err != nil {
				return DeployedResource{}, totalCost, fmt.Errorf("deployRoute53RecordSetGroup item %d: %w", i, err)
			}
			totalCost += cost
			// A refused record set reports through its DeployedResource, never a Go error, and
			// until #1256 that resource was discarded here — so a group whose every record set
			// was refused reported a clean resource. The group is one CloudFormation resource,
			// so its first refused record set fails it, and the rest are not attempted.
			if child.Error != "" {
				return DeployedResource{
					LogicalID:  logicalID,
					Type:       "AWS::Route53::RecordSetGroup",
					PhysicalID: logicalID,
					Error:      fmt.Sprintf("record set %d: %s", i, child.Error),
				}, totalCost, nil
			}
		}
	}
	// The physical ID is the group's logical name, which is also what Ref returns for this type
	// (#827). It used to be left unset, so a !Ref on a record set group resolved to the empty
	// string and DescribeStackResources reported a resource with no physical ID at all.
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Route53::RecordSetGroup",
		PhysicalID: logicalID,
	}, totalCost, nil
}

// deployKMSKey creates a KMS key for the given CFN resource.
//
// The two defaults below are CloudFormation's own, not substrate's: AWS::KMS::Key documents KeySpec's
// default as SYMMETRIC_DEFAULT and KeyUsage's as ENCRYPT_DECRYPT, so sending both unconditionally is
// what the resource type does. #977 made that visible rather than changing it — CreateKey now refuses a
// key spec that does not admit the usage it is paired with, so a template naming an asymmetric or HMAC
// KeySpec without a KeyUsage is refused here where it previously created a key AWS would not have.
//
// That refusal is correct for the same reason: KeyUsage "is required for asymmetric KMS keys and HMAC
// KMS keys", so such a template is invalid at AWS too and fails its own stack. Defaulting is therefore
// kept — the resource type's default is a fact about the resource type, and a template that relies on it
// for a symmetric key is unaffected.
func (d *StackDeployer) deployKMSKey(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	body := map[string]interface{}{
		"Description": resolveStringProp(props, "Description", "", cctx),
		"KeyUsage":    resolveStringProp(props, "KeyUsage", "ENCRYPT_DECRYPT", cctx),
		"KeySpec":     resolveStringProp(props, "KeySpec", "SYMMETRIC_DEFAULT", cctx),
	}
	// The key policy is CreateKey's Policy member. EnableKeyRotation used to be sent here too, and
	// CreateKey has no such member, so a template asking for rotation got a key without it; it is
	// applied after the create now, with Enabled, by configureKMSKey (#1545).
	if policy := cfnJSONDocument(props["KeyPolicy"], cctx); policy != "" {
		body["Policy"] = policy
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal kms key body: %w", err)
	}
	req := &AWSRequest{
		Service:   "kms",
		Operation: "CreateKey",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::KMS::Key"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			KeyMetadata struct {
				KeyID string `json:"KeyId"`
				ARN   string `json:"Arn"`
			} `json:"KeyMetadata"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.KeyMetadata.ARN
			dr.ARN = result.KeyMetadata.ARN
			// Ref returns the key ID, not the ARN, so the ID is recorded rather than
			// re-derived from the ARN's last segment at resolve time (#827). It was already
			// being decoded and discarded.
			dr.Metadata = map[string]interface{}{"KeyId": result.KeyMetadata.KeyID}
		}
	}
	if dr.Error == "" && dr.PhysicalID != "" {
		var declared []string
		for _, p := range []string{"Enabled", "EnableKeyRotation", "RotationPeriodInDays"} {
			if _, ok := props[p]; ok {
				declared = append(declared, p)
			}
		}
		configCost, configErr := d.configureKMSKey(ctx, dr.PhysicalID, props, declared, true, streamID, cctx)
		cost += configCost
		if configErr != nil {
			dr.Error = configErr.Error()
		}
	}
	return dr, cost, nil
}

// deployKMSAlias creates a KMS alias for the given CFN resource.
func (d *StackDeployer) deployKMSAlias(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	aliasName := resolveStringProp(props, "AliasName", "alias/"+logicalID, cctx)
	targetKeyID := resolveStringProp(props, "TargetKeyId", "", cctx)
	body := map[string]string{
		"AliasName":   aliasName,
		"TargetKeyId": targetKeyID,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal kms alias body: %w", err)
	}
	req := &AWSRequest{
		Service:   "kms",
		Operation: "CreateAlias",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::KMS::Alias", PhysicalID: aliasName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployKMSReplicaKey creates a KMS replica key (stub) for the given CFN resource.
func (d *StackDeployer) deployKMSReplicaKey(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// Stub: treat as a standard symmetric key creation.
	dr, cost, err := d.deployKMSKey(ctx, logicalID, props, streamID, cctx)
	// The delegation left dr.Type as "AWS::KMS::Key", so DescribeStackResources reported the
	// wrong type for a replica key and anything switching on the type — Ref resolution now
	// among them (#827) — could not tell the two apart. Both types Ref to the key ID, so this
	// corrects the report rather than changing a resolved value.
	dr.Type = "AWS::KMS::ReplicaKey"
	return dr, cost, err
}

// deploySecret creates a Secrets Manager secret for the given CFN resource.
func (d *StackDeployer) deploySecret(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	body := map[string]interface{}{
		"Name":        name,
		"Description": resolveStringProp(props, "Description", "", cctx),
	}
	if sv, ok := props["SecretString"]; ok {
		body["SecretString"] = resolveValue(sv, cctx)
	}
	if kmsID, ok := props["KmsKeyId"]; ok {
		body["KmsKeyId"] = resolveValue(kmsID, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal secret body: %w", err)
	}
	req := &AWSRequest{
		Service:   "secretsmanager",
		Operation: "CreateSecret",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SecretsManager::Secret", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ARN string `json:"ARN"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ARN != "" {
			dr.ARN = result.ARN
			dr.PhysicalID = result.ARN
		}
	}
	return dr, cost, nil
}

// deploySecretRotationSchedule configures rotation on a Secrets Manager secret, passing through the
// template's rotation function and schedule.
//
// It sent SecretId alone, discarding every other property the resource type publishes, which stopped
// being merely thin once RotateSecret began refusing a secret with no rotation function (#952): a
// template naming a RotationLambdaARN would have been refused for not naming one. So the pass-through
// is a consequence of that refusal rather than an unrelated improvement.
//
// The ClientRequestToken is derived from (account, Region, stack, logical ID) via
// [cfnDeterministicUUID], because RotateSecret requires one and a deploy has no caller to supply it —
// and the same stack redeployed must send the same token rather than a fresh one, since the token
// becomes the response's VersionId.
func (d *StackDeployer) deploySecretRotationSchedule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	secretID := resolveStringProp(props, "SecretId", "", cctx)
	body := map[string]interface{}{
		"SecretId":           secretID,
		"ClientRequestToken": cfnDeterministicUUID(cctx.accountID, cctx.region, cctx.stackName, logicalID),
	}
	if arn := cfnRotationLambdaARN(props, cctx); arn != "" {
		body["RotationLambdaARN"] = arn
	}
	if rules := cfnRotationRules(props, cctx); len(rules) > 0 {
		body["RotationRules"] = rules
	}
	// "RotateImmediatelyOnUpdate: … Default: true" — the same default RotateSecret's own
	// RotateImmediately carries, so an unset property is left out rather than sent as true.
	if immediate, set := cfgsvcCFNBool(props, "RotateImmediatelyOnUpdate"); set {
		body["RotateImmediately"] = immediate
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal rotation schedule body: %w", err)
	}
	req := &AWSRequest{
		Service:   "secretsmanager",
		Operation: "RotateSecret",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SecretsManager::RotationSchedule", PhysicalID: secretID, ARN: secretID}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// cfnRotationLambdaARN resolves the rotation function a RotationSchedule names, by either of the two
// routes AWS publishes for it.
//
// "Choose one of the following options for the rotation function: Create a new rotation function using
// HostedRotationLambda … Use an existing rotation function by specifying its ARN with
// RotationLambdaARN." The first route names a function rather than an ARN, so the ARN is composed from
// the deploying account and Region — **substrate's reading**, and the narrowest one available: a hosted
// rotation lambda is a function CloudFormation would create through a nested stack, which substrate
// does not do, so nothing here creates the function and no Lambda record exists at that ARN. What the
// pass-through buys is that the secret records the function the template chose instead of being refused
// for naming none, which is the difference between a deployable template and a failed resource.
func cfnRotationLambdaARN(props map[string]interface{}, cctx *cfnContext) string {
	if arn := resolveStringProp(props, "RotationLambdaARN", "", cctx); arn != "" {
		return arn
	}
	if name := resolveNestedStringProp(props, "HostedRotationLambda", "RotationLambdaName", "", cctx); name != "" {
		return lambdaFunctionARN(cctx.region, cctx.accountID, name)
	}
	return ""
}

// cfnRotationRules resolves a RotationSchedule's RotationRules into RotateSecret's parameter shape,
// omitting the whole member when the template sets none of the three.
//
// AutomaticallyAfterDays is sent as a number rather than as the string resolveValue produces, because
// AWS types it Long and RotateSecret decodes it as one; a value the template writes in a form that is
// not an integer is dropped rather than passed through as a string, which would be refused as a
// malformed body and report the wrong cause. cfgsvcCFNBool's reason for accepting two spellings applies
// here too — a JSON template's 30 arrives as float64 and a YAML template's or an Fn::If's as a string,
// and resolveValue normalizes both.
func cfnRotationRules(props map[string]interface{}, cctx *cfnContext) map[string]interface{} {
	rules := map[string]interface{}{}
	if days := resolveNestedStringProp(props, "RotationRules", "AutomaticallyAfterDays", "", cctx); days != "" {
		if parsed, err := strconv.ParseInt(days, 10, 64); err == nil {
			rules["AutomaticallyAfterDays"] = parsed
		}
	}
	if duration := resolveNestedStringProp(props, "RotationRules", "Duration", "", cctx); duration != "" {
		rules["Duration"] = duration
	}
	if expr := resolveNestedStringProp(props, "RotationRules", "ScheduleExpression", "", cctx); expr != "" {
		rules["ScheduleExpression"] = expr
	}
	return rules
}

// deploySecretTargetAttachment is a stub for SecretsManager::SecretTargetAttachment.
func (d *StackDeployer) deploySecretTargetAttachment(
	_ context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	secretID := resolveStringProp(props, "SecretId", logicalID, cctx)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SecretsManager::SecretTargetAttachment",
		PhysicalID: secretID,
		ARN:        secretID,
	}, 0, nil
}

// deploySSMParameter creates an SSM parameter for the given CFN resource.
func (d *StackDeployer) deploySSMParameter(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", "/"+logicalID, cctx)
	value := d.resolveDynamicRef(ctx, resolveStringProp(props, "Value", "", cctx), cctx)
	body := map[string]interface{}{
		"Name":      name,
		"Value":     value,
		"Type":      resolveStringProp(props, "Type", "String", cctx),
		"Overwrite": true,
	}
	if desc, ok := props["Description"]; ok {
		body["Description"] = resolveValue(desc, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal ssm parameter body: %w", err)
	}
	req := &AWSRequest{
		Service:   "ssm",
		Operation: "PutParameter",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SSM::Parameter", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else {
		// PutParameter's response carries no ARN, so it is built here by ssmParameterARN — the
		// same function the SSM plugin stores on the parameter record, applied to the same
		// leading-slash normalization PutParameter applies before storing it, so the ARN a
		// template resolves is the one GetParameter answers with (#827).
		slashed := name
		if !strings.HasPrefix(slashed, "/") {
			slashed = "/" + slashed
		}
		dr.ARN = ssmParameterARN(cctx.region, cctx.accountID, slashed)
		// GetAtt Type and Value. "Value — Returns the value of the parameter": the resolver
		// used to answer this attribute with the physical ID, which is the parameter's *name*
		// (#827). Recorded from what was sent rather than read back, because PutParameter's
		// response carries only a version.
		cfnSetMetadata(&dr, "Type", fmt.Sprintf("%v", body["Type"]))
		cfnSetMetadata(&dr, "Value", value)
	}
	return dr, cost, nil
}

// deploySSMAssociation is a stub for SSM::Association resources.
//
// The association id is minted from the deployment's own mint rather than dispatched
// for, because there is no request to dispatch: SSM's plugin models Run Command and
// Parameter Store, not State Manager, so this resource has no service behind it and
// the physical id is the whole of what a template can observe. It was the deployer's
// last caller of the shared crypto/rand helper (#856).
func (d *StackDeployer) deploySSMAssociation(
	_ context.Context,
	logicalID string,
	_ map[string]interface{},
	_ string,
	_ *cfnContext,
) (DeployedResource, float64, error) {
	assocID := d.mint.Hex(16)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SSM::Association",
		PhysicalID: assocID,
	}, 0, nil
}

// deploySNSTopic creates an SNS topic for the given CFN resource.
func (d *StackDeployer) deploySNSTopic(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	topicName := resolveStringProp(props, "TopicName",
		cfnGeneratedName(cctx, "AWS::SNS::Topic", logicalID), cctx)
	params := map[string]string{
		"Action": "CreateTopic",
		"Name":   topicName,
	}
	// Forward DisplayName when declared so it round-trips and is drift-checkable, through the
	// Attributes map API_CreateTopic publishes — `Attributes.entry.1.key` / `.value`. It used to be
	// sent as a bare `DisplayName` parameter, which is not a parameter that page publishes and which
	// the SNS plugin stopped honoring with #1126; the property itself is unchanged, only the wire
	// form it travels in, which is now the one a real CreateTopic call would carry.
	if _, declared := props["DisplayName"]; declared {
		if dn := resolveValue(props["DisplayName"], cctx); dn != "" {
			params["Attributes.entry.1.key"] = "DisplayName"
			params["Attributes.entry.1.value"] = dn
		}
	}
	req := &AWSRequest{
		Service:   "sns",
		Operation: "CreateTopic",
		Body:      nil,
		Headers:   map[string]string{},
		Params:    params,
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	// PhysicalID is the topic ARN (CloudFormation Ref on an SNS topic returns its
	// ARN); the drift checker/comparator derive the topic name from it.
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SNS::Topic", PhysicalID: topicName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		arn := extractXMLField(resp.Body, "TopicArn")
		if arn != "" {
			dr.ARN = arn
			dr.PhysicalID = arn
		}
	}
	return dr, cost, nil
}

// deploySNSSubscription creates an SNS subscription for the given CFN resource.
func (d *StackDeployer) deploySNSSubscription(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	topicARN := resolveStringProp(props, "TopicArn", "", cctx)
	protocol := resolveStringProp(props, "Protocol", "sqs", cctx)
	endpoint := resolveStringProp(props, "Endpoint", "", cctx)
	req := &AWSRequest{
		Service:   "sns",
		Operation: "Subscribe",
		Body:      nil,
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action":   "Subscribe",
			"TopicArn": topicARN,
			"Protocol": protocol,
			"Endpoint": endpoint,
		},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SNS::Subscription"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		subARN := extractXMLField(resp.Body, "SubscriptionArn")
		if subARN != "" {
			dr.PhysicalID = subARN
			dr.ARN = subARN
		}
	}
	return dr, cost, nil
}

// deploySNSTopicPolicy sets a topic policy via SetTopicAttributes for the given CFN resource.
func (d *StackDeployer) deploySNSTopicPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	topicARN := resolveStringProp(props, "Topics", "", cctx)
	// Topics is a list; try to get the first entry.
	if topicsList, ok := props["Topics"].([]interface{}); ok && len(topicsList) > 0 {
		topicARN = resolveValue(topicsList[0], cctx)
	}
	policy := cfnJSONDocument(props["PolicyDocument"], cctx)
	req := &AWSRequest{
		Service:   "sns",
		Operation: "SetTopicAttributes",
		Body:      nil,
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action":         "SetTopicAttributes",
			"TopicArn":       topicARN,
			"AttributeName":  "Policy",
			"AttributeValue": policy,
		},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SNS::TopicPolicy", PhysicalID: topicARN, ARN: topicARN}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployLogsLogGroup creates a CloudWatch Logs log group for the given CFN resource.
func (d *StackDeployer) deployLogsLogGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	lgName := resolveStringProp(props, "LogGroupName",
		cfnGeneratedName(cctx, "AWS::Logs::LogGroup", logicalID), cctx)
	body := map[string]interface{}{
		"logGroupName": lgName,
	}
	if retain, ok := props["RetentionInDays"]; ok {
		body["retentionInDays"] = retain
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal loggroup body: %w", err)
	}
	req := &AWSRequest{
		Service:   "logs",
		Operation: "CreateLogGroup",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Logs::LogGroup",
		PhysicalID: lgName,
		ARN:        cwLogGroupARN(cctx.region, cctx.accountID, lgName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployLogsLogStream creates a CloudWatch Logs log stream for the given CFN resource.
func (d *StackDeployer) deployLogsLogStream(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	groupName := resolveStringProp(props, "LogGroupName", "", cctx)
	streamName := resolveStringProp(props, "LogStreamName", logicalID, cctx)
	body := map[string]string{
		"logGroupName":  groupName,
		"logStreamName": streamName,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal logstream body: %w", err)
	}
	req := &AWSRequest{
		Service:   "logs",
		Operation: "CreateLogStream",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Logs::LogStream",
		PhysicalID: streamName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployEventsRule creates an EventBridge rule for the given CFN resource.
func (d *StackDeployer) deployEventsRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	ruleName := resolveStringProp(props, "Name", logicalID, cctx)
	body := map[string]interface{}{
		"Name":  ruleName,
		"State": resolveStringProp(props, "State", "ENABLED", cctx),
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
		ARN:        ebRuleARN(cctx.region, cctx.accountID, ruleName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			RuleArn string `json:"RuleArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.RuleArn != "" {
			dr.ARN = result.RuleArn
		}
	}
	return dr, cost, nil
}

// deployCloudWatchAlarm creates a CloudWatch alarm for the given CFN resource.
func (d *StackDeployer) deployCloudWatchAlarm(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	alarmName := resolveStringProp(props, "AlarmName", logicalID, cctx)
	params := map[string]string{
		"Action":             "PutMetricAlarm",
		"AlarmName":          alarmName,
		"MetricName":         resolveStringProp(props, "MetricName", "", cctx),
		"Namespace":          resolveStringProp(props, "Namespace", "", cctx),
		"ComparisonOperator": resolveStringProp(props, "ComparisonOperator", "", cctx),
		"Threshold":          resolveStringProp(props, "Threshold", "0", cctx),
		"EvaluationPeriods":  resolveStringProp(props, "EvaluationPeriods", "1", cctx),
		"Period":             resolveStringProp(props, "Period", "60", cctx),
	}
	if desc := resolveStringProp(props, "AlarmDescription", "", cctx); desc != "" {
		params["AlarmDescription"] = desc
	}
	if stat := resolveStringProp(props, "Statistic", "", cctx); stat != "" {
		params["Statistic"] = stat
	}
	req := &AWSRequest{
		Service:   "monitoring",
		Operation: "PutMetricAlarm",
		Body:      nil,
		Headers:   map[string]string{},
		Params:    params,
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudWatch::Alarm",
		PhysicalID: alarmName,
		ARN:        cwAlarmARN(cctx.region, cctx.accountID, alarmName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// extractXMLField extracts the text content of the first occurrence of an XML
// element with the given name from b.
func extractXMLField(b []byte, name string) string {
	openTag := "<" + name + ">"
	closeTag := "</" + name + ">"
	s := string(b)
	start := strings.Index(s, openTag)
	if start < 0 {
		return ""
	}
	start += len(openTag)
	end := strings.Index(s[start:], closeTag)
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}

// internalRequestID returns the request id for the next request this deployment
// dispatches.
//
// Derived from the deployment's mint when it has one, so the sequence reproduces on
// a re-run of the same template (see [WithDeployerMint]); drawn from the wall clock
// otherwise, which is what every in-process dispatch did before. The `req-` prefix
// and the width match generateRequestID, because this value reaches the event log
// and `/v1/debug/events` beside ids that came from there.
func (d *StackDeployer) internalRequestID() string {
	if !d.mint.Derived() {
		return generateRequestID()
	}
	return cfnInternalRequestIDPrefix + d.mint.Hex(12)
}

// dispatch performs in-process request routing, records the event, and returns
// the response, the estimated cost, and any routing error.
func (d *StackDeployer) dispatch(
	ctx context.Context,
	req *AWSRequest,
	streamID string,
) (*AWSResponse, float64, error) {
	requestID := d.internalRequestID()
	reqCtx := &RequestContext{
		RequestID: requestID,
		AccountID: d.identity.accountID,
		Region:    d.identity.region,
		Timestamp: d.tc.Now(),
		// Seeded from the request id above, which is what makes the resource this
		// request creates carry the identifier it carried in the recording (#856).
		// Before this the deployer dispatched with a nil mint, so every plugin it
		// reached fell back to crypto/rand — a replayed stack re-created every one
		// of its resources under a new identifier, and ValidateState reported a
		// state_hash mismatch for the create that was the right answer.
		IDs:       NewIDMint(requestID),
		Principal: d.attribution.resolve(),
		// The parent names the request this deployment runs for, so a replay that also
		// re-runs that request knows it reproduces this dispatch (#1601).
		Metadata: map[string]interface{}{"stream_id": streamID, parentRequestIDKey: d.mint.seedRequestID()},
	}

	// A template's resource calls are built in this package rather than parsed off
	// the wire, and 40 of those construction sites name a bare HTTP method ("PUT"
	// for a bucket, "POST" for a function). ParseAWSRequest resolves the semantic
	// name for everything arriving over HTTP (#480, #572), but these never pass
	// through it, so they would be metered, recorded and authorized as "s3:PUT" —
	// which no policy granting s3:CreateBucket matches. Resolving here reaches
	// every dispatch site at once, and is a no-op for a request that already
	// carries a real operation name.
	//
	// This must precede the authorization below: the action a policy is evaluated
	// against is derived from req.Operation, so authorizing first would ask whether
	// the stack's role may perform "s3:PUT" — a question no correct policy answers
	// yes to.
	resolveOperationName(req)

	// The "before" state hash, taken here for the same reason
	// [Server.handleAWSRequest] takes it before its first refusal point: a refused
	// call changed nothing, and recording the same hash on both sides says so
	// rather than leaving the field empty and having the replay skip the check.
	// Empty, and free, unless the store records hashes (#833).
	stateBefore := d.recordedStateHash(ctx)

	// Authorize here rather than in RouteRequest, which no caller's requests pass
	// an authorization check through. Doing it at the deployer covers create,
	// update, rollback and delete at once: every one of those sweeps reaches a
	// resource through this function, so there is no second path to keep in step.
	//
	// A denial is returned as the dispatch error, which is exactly what a refused
	// call from the resource's own service would have produced. The existing
	// machinery then reports it with no new model: the resource records the error,
	// DescribeStackResources renders it as CREATE_FAILED with the denial as
	// ResourceStatusReason, and the stack rolls back.
	if d.auth != nil && reqCtx.Principal != nil {
		if err := d.auth.CheckAccess(reqCtx, req); err != nil {
			// Record the refusal before returning it, so a denied call is in the
			// event log beside the ones that succeeded and a replay shows where the
			// deployment stopped and why.
			_ = d.store.RecordRequest(ctx, reqCtx, req, nil, 0, 0, err,
				WithStateHashes(stateBefore, d.recordedStateHash(ctx)))
			return nil, 0, err
		}
	}

	// Wall time, as [Server.handleAWSRequest] measures Event.Duration: a latency is how long
	// the dispatch took to run. This subtracted a simulated start from the wall clock, so on
	// a pinned or moved clock it recorded the distance between the two clocks instead.
	wallStart := time.Now()
	resp, routeErr := d.registry.RouteRequest(reqCtx, req)
	duration := time.Since(wallStart)
	cost := d.costs.CostForRequest(req)

	_ = d.store.RecordRequest(ctx, reqCtx, req, resp, duration, cost, routeErr,
		WithStateHashes(stateBefore, d.recordedStateHash(ctx)))

	return resp, cost, cfnDispatchError(resp, routeErr)
}

// recordedStateHash returns the hash to record on a dispatched event, or "" when
// the store is not recording state hashes.
//
// The deployer needs its own because it is one of the tree's four dispatch paths
// and does not go through [Server.handleAWSRequest]; without it a stack's resource
// calls would be the only recorded events with no hash, and a replay would
// silently skip validating exactly the requests CloudFormation made.
func (d *StackDeployer) recordedStateHash(ctx context.Context) string {
	if d.store == nil || !d.store.RecordsStateHashes() {
		return ""
	}
	return stateSnapshotHash(ctx, d.state)
}

// cfnDispatchError reports the failure a dispatched request represents, or nil if
// it succeeded.
//
// Plugins signal a client error one of two ways, and both are in wide use: EC2,
// ECS, CloudWatch Logs and SQS return an *AWSError, while S3 and IAM return a 4xx
// *AWSResponse with a nil error — the same shape a real endpoint puts on the wire.
// PluginRegistry.RouteRequest passes both through unchanged, so a deployer that
// only checked the error return recorded no failure at all for an S3 or IAM
// resource that had been refused, and the stack reported CREATE_COMPLETE for a
// resource that does not exist.
//
// The error code is lifted out of the response body so the recorded reason names
// what went wrong ("InvalidBucketName: The specified bucket is not valid.")
// rather than a bare status number. Both conventions put it in a <Code> element;
// a body in any other shape falls back to the status alone.
func cfnDispatchError(resp *AWSResponse, routeErr error) error {
	if routeErr != nil {
		return routeErr
	}
	if resp == nil || resp.StatusCode < 400 {
		return nil
	}
	code := extractXMLField(resp.Body, "Code")
	message := extractXMLField(resp.Body, "Message")
	switch {
	case code != "" && message != "":
		return fmt.Errorf("%s: %s", code, message)
	case code != "":
		return fmt.Errorf("%s (HTTP %d)", code, resp.StatusCode)
	default:
		return fmt.Errorf("request failed with HTTP %d", resp.StatusCode)
	}
}

// buildCFNContext constructs a cfnContext from template parameters and caller-supplied values.
func buildCFNContext(tmpl *cfnTemplate, callerParams map[string]string, region, accountID, stackName string) *cfnContext {
	params := make(map[string]string)
	listParams := make(map[string]bool)
	// Start with template defaults. A declared default is recorded even when it
	// is the empty string: that is how an optional parameter is spelled, and
	// leaving it out would make Ref resolve to the parameter's own name.
	for name, p := range tmpl.Parameters {
		if p.Default != nil {
			params[name] = *p.Default
		}
		if cfnListParameterType(p.Type) {
			listParams[name] = true
		}
	}
	// Overlay caller-supplied params.
	for k, v := range callerParams {
		params[k] = v
	}
	return &cfnContext{
		params:     params,
		listParams: listParams,
		conditions: make(map[string]bool),
		resources:  make(map[string]DeployedResource),
		region:     region,
		accountID:  accountID,
		stackName:  stackName,
		evaluating: make(map[string]bool),
		mappings:   tmpl.Mappings,
		imports:    make(map[string]bool),
	}
}

// evaluateConditions evaluates all Conditions in the template into cctx.conditions.
//
// A condition may reference another by name ({"Condition": "Other"}), so the
// evaluation order matters. Iterating tmpl.Conditions directly made the result
// depend on Go's map iteration order: a condition evaluated before the one it
// references read the referent's zero value and the whole template resolved
// differently from one run to the next — the one outcome an emulator built on
// deterministic replay must never produce.
//
// Each condition is therefore resolved on demand through cctx.condition, which
// evaluates a referent the first time it is needed regardless of declaration
// order. Names are walked in sorted order so that a template containing a
// reference cycle reports the same result every time.
func evaluateConditions(tmpl *cfnTemplate, cctx *cfnContext) {
	cctx.conditionExprs = tmpl.Conditions

	names := make([]string, 0, len(tmpl.Conditions))
	for name := range tmpl.Conditions {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cctx.condition(name)
	}
}

// condition returns the value of the named condition, evaluating it — and any
// condition it references — on first use.
//
// A cycle resolves to false rather than recursing forever: real CloudFormation
// rejects a circular condition at validation time, and returning false is the same
// answer an undeclared condition already gets, so a malformed template degrades
// instead of hanging the deployment.
func (c *cfnContext) condition(name string) bool {
	// Memoized, and not only to save work: "CloudFormation evaluates conditions
	// when creating or updating a stack", once, and a condition "can't reference
	// resource logical IDs or their attributes". Caching the first answer is what
	// keeps a condition's value fixed for the whole deployment, so a template that
	// does reach a resource anyway cannot see one value before that resource
	// deploys and another after.
	if v, ok := c.conditions[name]; ok {
		return v
	}
	expr, ok := c.conditionExprs[name]
	if !ok {
		return false
	}
	if c.evaluating[name] {
		return false
	}
	c.evaluating[name] = true
	v := evalConditionExpr(expr, c)
	delete(c.evaluating, name)
	c.conditions[name] = v
	return v
}

// evalConditionExpr evaluates a single condition expression.
func evalConditionExpr(expr interface{}, cctx *cfnContext) bool {
	m, ok := expr.(map[string]interface{})
	if !ok {
		return false
	}
	for fn, args := range m {
		switch fn {
		case "Fn::Equals":
			arr, ok := args.([]interface{})
			if !ok || len(arr) != 2 {
				return false
			}
			return resolveValue(arr[0], cctx) == resolveValue(arr[1], cctx)
		case "Fn::Not":
			arr, ok := args.([]interface{})
			if !ok || len(arr) != 1 {
				return true
			}
			return !evalConditionExpr(arr[0], cctx)
		case "Fn::And":
			arr, ok := args.([]interface{})
			if !ok {
				return false
			}
			for _, a := range arr {
				if !evalConditionExpr(a, cctx) {
					return false
				}
			}
			return true
		case "Fn::Or":
			arr, ok := args.([]interface{})
			if !ok {
				return false
			}
			for _, a := range arr {
				if evalConditionExpr(a, cctx) {
					return true
				}
			}
			return false
		case "Condition":
			name, ok := args.(string)
			if !ok {
				return false
			}
			return cctx.condition(name)
		}
	}
	return false
}

// resolveValue resolves a CloudFormation value (literal, Ref, or intrinsic function).
func resolveValue(v interface{}, cctx *cfnContext) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case map[string]interface{}:
		// A single key is what makes a map an intrinsic; every intrinsic
		// CloudFormation defines is a one-member object. A map carrying other
		// keys alongside is user data — a container definition or a policy
		// document may hold a member named "Ref" — and walking it returned
		// whichever recognized key Go's map iteration reached first, so the same
		// template resolved differently from one run to the next. That is the
		// one outcome an emulator built on deterministic replay must never
		// produce; such a map falls through to the JSON encoding below.
		if len(val) != 1 {
			break
		}
		for fn, args := range val {
			switch fn {
			case "Ref":
				ref, ok := args.(string)
				if !ok {
					return ""
				}
				return resolveRef(ref, cctx)
			case "Fn::Sub":
				return resolveFnSub(args, cctx)
			case "Fn::Join":
				return resolveFnJoin(args, cctx)
			case "Fn::Select":
				return resolveFnSelect(args, cctx)
			case "Fn::Split":
				// Fn::Split returns a list and this context has nowhere to put
				// one, so the elements are rejoined on the delimiter. See
				// resolveFnSplitJoined for why that beats truncating, and
				// resolveValueList for the context that keeps the list.
				return resolveFnSplitJoined(args, cctx)
			case "Fn::Base64":
				return resolveFnBase64(args, cctx)
			case "Fn::GetAtt":
				return resolveFnGetAtt(args, cctx)
			case "Fn::If":
				return resolveFnIf(args, cctx)
			case "Fn::FindInMap":
				return resolveFnFindInMap(args, cctx)
			case "Fn::ImportValue":
				return resolveFnImportValue(args, cctx)
			case "Fn::GetAZs", "Fn::Cidr":
				// Both return an array, and this context has nowhere to put
				// one, so the elements are rejoined the way Fn::Split's are —
				// see resolveFnSplitJoined. resolveValueList is the context
				// that keeps the list.
				return strings.Join(resolveValueList(val, cctx), ",")
			}
		}
	}
	// Fallback: JSON-encode. This can fail: a YAML template mapping with a non-string key
	// decodes to map[interface{}]interface{}, which encoding/json refuses. The result was
	// then silently "" (#1365). resolveValue has nowhere to return an error, and its 58
	// callers put the string straight into a request, so it falls back to the value's Go
	// rendering instead: the value stays visible in what reaches the plugin.
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// resolveValueList resolves a CloudFormation value in a list-valued context.
//
// resolveValue returns a string, so every list-valued intrinsic — Fn::Split, a
// Ref to a CommaDelimitedList parameter, and (once modeled) Fn::GetAZs and
// Fn::Cidr — had nowhere to put its list and lost everything but one element
// (#521). This is that missing return shape; the two live side by side because
// CloudFormation itself distinguishes the contexts: the same intrinsic in a
// scalar property is an error there and a rejoined string here.
//
// Conventions, which resolveStringList and resolveFnSelect both depend on:
//
//   - A scalar resolves to a one-element list, so a property that accepts either
//     a single value or a list needs no special case at the call site.
//   - Ref AWS::NoValue contributes *no* element, matching CloudFormation's
//     removal semantics — a template writes
//     `!If [HasCommand, !Split [...], !Ref 'AWS::NoValue']` precisely to say
//     "and otherwise nothing".
//   - An element that is itself list-valued splices rather than nesting, since
//     a list of lists is not a shape any AWS API member has.
//   - Empty elements are preserved: `!Split ['|', 'a||c|']` is documented to
//     return ["a", "", "c", ""]. A caller that cannot use an empty member
//     filters them itself; resolveStringList does.
//   - Only a *single-key* map is an intrinsic. A map that also carries other
//     keys is user data — an ECS container definition naming a member "Ref", say
//     — and resolving it would both corrupt that data and make the result depend
//     on Go's map iteration order, since the walk would return whichever
//     recognized key it reached first.
func resolveValueList(v interface{}, cctx *cfnContext) []string {
	switch val := v.(type) {
	case nil:
		return nil
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			out = append(out, resolveValueList(item, cctx)...)
		}
		return out
	case map[string]interface{}:
		if len(val) == 1 {
			for fn, args := range val {
				switch fn {
				case "Ref":
					ref, ok := args.(string)
					if !ok {
						return nil
					}
					return resolveRefList(ref, cctx)
				case "Fn::Split":
					arr, ok := args.([]interface{})
					if !ok || len(arr) < 2 {
						return nil
					}
					sep, ok := arr[0].(string)
					if !ok {
						return nil
					}
					return splitFnSplit(sep, resolveValue(arr[1], cctx))
				case "Fn::If":
					arr, ok := args.([]interface{})
					if !ok || len(arr) < 3 {
						return nil
					}
					condName, ok := arr[0].(string)
					if !ok {
						return nil
					}
					if cctx.condition(condName) {
						return resolveValueList(arr[1], cctx)
					}
					return resolveValueList(arr[2], cctx)
				case "Fn::GetAZs":
					return resolveFnGetAZs(args, cctx)
				case "Fn::GetAtt":
					return resolveFnGetAttList(args, cctx)
				case "Fn::Cidr":
					return resolveFnCidr(args, cctx)
				case "Fn::FindInMap":
					// A mapping's values "can be of type String or List", so a
					// lookup whose leaf is a list contributes its members here
					// and is rejoined in a scalar context. resolveFnFindInMap
					// returns the leaf untouched for exactly this reason.
					leaf, ok := resolveFnFindInMapValue(args, cctx)
					if !ok {
						return nil
					}
					return resolveValueList(leaf, cctx)
				}
			}
		}
	}
	return []string{resolveValue(v, cctx)}
}

// resolveRefList resolves a Ref in a list-valued context.
//
// A Ref is list-valued exactly when it names a parameter whose declared type is
// a list type, so the parameter declarations have to be consulted rather than
// the value guessed at: a String parameter that happens to hold a comma is one
// value, not several.
func resolveRefList(ref string, cctx *cfnContext) []string {
	if ref == "AWS::NoValue" {
		return nil
	}
	if ref == "AWS::NotificationARNs" {
		// "Unlike other pseudo parameters, AWS::NotificationARNs returns a list
		// of ARNs", so it belongs here as well as in resolveRef. The list is
		// always empty: substrate has no notification model — CreateStack's
		// NotificationARNs parameter is not recorded and no stack event is
		// published — so an empty list is the accurate answer rather than a
		// placeholder. A template's `!Select [0, !Ref 'AWS::NotificationARNs']`
		// therefore resolves to the empty string, which is what a stack created
		// without notification ARNs would give.
		return nil
	}
	if cctx.listParams[ref] {
		if v, ok := cctx.params[ref]; ok {
			return splitParameterList(v)
		}
	}
	return []string{resolveRef(ref, cctx)}
}

// splitParameterList splits a list-typed parameter's value on commas.
//
// "The total number of strings should be one more than the total number of
// commas. Also, each member string is space trimmed." An empty value is an empty
// list rather than a list holding one empty string, which is how a template
// spells "no members" with `Default: ”`.
func splitParameterList(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

// cfnListParameterType reports whether a declared parameter type holds a list.
//
// CommaDelimitedList is the literal-string form; every AWS-specific and Number
// list type is spelled List<…> (List<Number>, List<AWS::EC2::Subnet::Id>, …).
func cfnListParameterType(t string) bool {
	return t == "CommaDelimitedList" || strings.HasPrefix(t, "List<")
}

// resolveNested resolves intrinsics at any depth inside a structured property,
// returning a structurally identical value.
//
// resolveValue resolves a value that *is* an intrinsic; nothing walked into a
// map or a list to resolve one nested within (#526). So a deploy path that
// forwards a structured property whole — ContainerDefinitions, KeySchema,
// AttributeDefinitions — handed the plugin `{"Ref": "PK"}` as a literal object,
// which a typed plugin rejects and an untyped one stores. Real CloudFormation
// resolves intrinsics at any depth, and which of a consumer's properties
// resolved here depended on how each deploy path happened to have been written.
//
// Four rules that a naive walk gets wrong:
//
//   - Only a *single-key* map is an intrinsic, even when one of several keys is
//     "Ref". A multi-key map is user data — and resolving one made the result
//     depend on Go's map iteration order, which #521 fixed in resolveValue for
//     the same reason.
//   - A recognized intrinsic in a *list* position resolves through
//     resolveValueList and splices, so a nested Fn::Split contributes its
//     elements rather than one rejoined string.
//   - Fn::If yielding AWS::NoValue *removes* the property, as CloudFormation
//     does, rather than leaving an empty string behind. That is what
//     `!If [HasCommand, !Split [...], !Ref 'AWS::NoValue']` is written to say.
//   - Keys are never rewritten. Mapping a property's member names is a
//     per-service concern (#527) and conflating it with resolution is how
//     logConfiguration.options — whose keys are user data — would get mangled.
//
// A resolved intrinsic becomes a string, or a list of strings when the intrinsic
// is list-valued — this walk returns interface{}, so unlike resolveValue it can
// carry the list rather than rejoining it (#521). A resolved scalar is always a
// *string*, though: a template that writes `"Cpu": {"Ref": "Cpu"}` gets "256"
// where a literal `256` would have stayed a number, so a plugin needing a typed
// member keeps resolving that member itself.
func resolveNested(v interface{}, cctx *cfnContext) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		if branch, ok := cfnStructuredIfBranch(val, cctx); ok {
			// An Fn::If choosing a structure is that structure, resolved in turn — the bootstrap
			// template's trust policies choose a whole Condition block this way. Resolving it as
			// a scalar marshaled the block into a JSON string, which IAM refused (#1606).
			return resolveNested(branch, cctx)
		}
		if isCFNIntrinsic(val) {
			return resolveIntrinsicPreservingShape(val, cctx)
		}
		out := make(map[string]interface{}, len(val))
		for k, item := range val {
			// An Fn::If choosing AWS::NoValue removes the member, so the walk
			// has to distinguish "resolved to nothing" from "resolved to an
			// empty string" before writing the key back.
			if isCFNNoValue(item, cctx) {
				continue
			}
			out[k] = resolveNested(item, cctx)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, item := range val {
			m, isMap := item.(map[string]interface{})
			if branch, ok := cfnStructuredIfBranch(m, cctx); isMap && ok {
				// A list element an Fn::If chooses as a structure — a policy statement — is one
				// element, not a list to splice.
				out = append(out, resolveNested(branch, cctx))
				continue
			}
			if isMap && isCFNIntrinsic(m) {
				// A list position is where a list-valued intrinsic belongs, so
				// its elements splice in rather than nesting.
				for _, s := range resolveValueList(m, cctx) {
					out = append(out, s)
				}
				continue
			}
			out = append(out, resolveNested(item, cctx))
		}
		return out
	}
	return v
}

// cfnStructuredIfBranch returns the branch an Fn::If chooses when that branch is a structure — a
// map that is not itself an intrinsic function — and reports false for every other value, which
// the scalar and list resolvers already answer.
func cfnStructuredIfBranch(m map[string]interface{}, cctx *cfnContext) (map[string]interface{}, bool) {
	if len(m) != 1 {
		return nil, false
	}
	args, ok := m["Fn::If"].([]interface{})
	if !ok || len(args) != 3 {
		return nil, false
	}
	name, _ := args[0].(string)
	branch := args[2]
	if cctx.condition(name) {
		branch = args[1]
	}
	chosen, isMap := branch.(map[string]interface{})
	if !isMap || isCFNIntrinsic(chosen) {
		return nil, false
	}
	return chosen, true
}

// resolveIntrinsicPreservingShape resolves an intrinsic to a string or, when the
// intrinsic is list-valued, to a list of strings.
//
// A structured property's member can hold either shape — ECS's `command` is a
// list of strings where its `image` is one string — and JSON can express both, so
// resolveValue's rejoin (#521), which exists only because it must return a
// string, would be a loss here. The list-valued forms are exactly the ones
// resolveValueList recognizes: Fn::Split, a Ref to a list-typed parameter, and an
// Fn::If whose chosen branch is one of those.
func resolveIntrinsicPreservingShape(m map[string]interface{}, cctx *cfnContext) interface{} {
	if !cfnListValuedIntrinsic(m, cctx) {
		return resolveValue(m, cctx)
	}
	list := resolveValueList(m, cctx)
	// []string would marshal correctly, but []interface{} keeps the walk's
	// result comparable to the literal lists alongside it, which arrive from
	// encoding/json as []interface{}.
	out := make([]interface{}, 0, len(list))
	for _, s := range list {
		out = append(out, s)
	}
	return out
}

// cfnListValuedIntrinsic reports whether an intrinsic resolves to a list.
//
// Fn::If is transparent: it is list-valued exactly when the branch its condition
// selects is, which is why the condition has to be evaluated here rather than the
// two branches inspected structurally.
func cfnListValuedIntrinsic(m map[string]interface{}, cctx *cfnContext) bool {
	if len(m) != 1 {
		return false
	}
	for fn, args := range m {
		switch fn {
		case "Fn::Split", "Fn::GetAZs", "Fn::Cidr":
			return true
		case "Fn::FindInMap":
			// List-valued exactly when the leaf it finds is a list, since "the
			// values can be of type String or List". A failed lookup is not
			// list-valued, and asking here does not double-report the failure:
			// resolveIntrinsicPreservingShape resolves the same intrinsic
			// immediately after, and a resource carrying the same reason twice
			// would be noise.
			leaf, found, _ := cfnFindInMapLeaf(args, cctx)
			if !found {
				return false
			}
			_, isList := leaf.([]interface{})
			return isList
		case "Ref":
			name, ok := args.(string)
			return ok && (cctx.listParams[name] || name == "AWS::NotificationARNs")
		case "Fn::If":
			arr, ok := args.([]interface{})
			if !ok || len(arr) < 3 {
				return false
			}
			condName, ok := arr[0].(string)
			if !ok {
				return false
			}
			branch := arr[2]
			if cctx.condition(condName) {
				branch = arr[1]
			}
			// A literal list branch is list-valued too:
			// `!If [C, ['a','b'], !Ref 'AWS::NoValue']`.
			if _, isList := branch.([]interface{}); isList {
				return true
			}
			inner, isMap := branch.(map[string]interface{})
			return isMap && cfnListValuedIntrinsic(inner, cctx)
		}
	}
	return false
}

// cfnIntrinsicNames are the intrinsic function keys resolveValue recognizes,
// plus Ref. A map is an intrinsic only when it has exactly one key and that key
// is one of these.
var cfnIntrinsicNames = map[string]bool{
	"Ref":           true,
	"Fn::Sub":       true,
	"Fn::Join":      true,
	"Fn::Select":    true,
	"Fn::Split":     true,
	"Fn::Base64":    true,
	"Fn::GetAtt":    true,
	"Fn::If":        true,
	"Fn::FindInMap": true,
	"Fn::GetAZs":    true,
	"Fn::Cidr":      true,

	// Fn::ImportValue was deliberately withheld from this table until it
	// resolved: resolveNested consults it to decide what is an intrinsic, and
	// admitting an unresolvable one would have resolved every import to "" —
	// worse than the JSON-literal fallback, which at least left the intrinsic
	// visible in the request the plugin refused.
	"Fn::ImportValue": true,
}

// isCFNIntrinsic reports whether a map is an intrinsic function invocation.
//
// Every intrinsic CloudFormation defines is a one-member object, so a map with
// any other size is user data regardless of what its keys are named.
func isCFNIntrinsic(m map[string]interface{}) bool {
	if len(m) != 1 {
		return false
	}
	for k := range m {
		return cfnIntrinsicNames[k]
	}
	return false
}

// isCFNNoValue reports whether a value resolves to AWS::NoValue, directly or
// through an Fn::If branch, and so should remove the member holding it.
//
// The recursion follows Fn::If only: that is the one intrinsic whose result can
// *be* a Ref to AWS::NoValue, and it is the form a template uses to say "this
// property, or nothing".
func isCFNNoValue(v interface{}, cctx *cfnContext) bool {
	m, ok := v.(map[string]interface{})
	if !ok || len(m) != 1 {
		return false
	}
	if ref, has := m["Ref"]; has {
		name, isStr := ref.(string)
		return isStr && name == "AWS::NoValue"
	}
	args, has := m["Fn::If"]
	if !has {
		return false
	}
	arr, isArr := args.([]interface{})
	if !isArr || len(arr) < 3 {
		return false
	}
	name, isStr := arr[0].(string)
	if !isStr {
		return false
	}
	if cctx.condition(name) {
		return isCFNNoValue(arr[1], cctx)
	}
	return isCFNNoValue(arr[2], cctx)
}

func resolveRef(ref string, cctx *cfnContext) string {
	// Pseudo-parameters.
	switch ref {
	case "AWS::Region":
		return cctx.region
	case "AWS::AccountId":
		return cctx.accountID
	case "AWS::StackName":
		return cctx.stackName
	case "AWS::NoValue":
		return ""
	case "AWS::Partition":
		return cfnPartition(cctx.region)
	case "AWS::URLSuffix":
		return cfnURLSuffix(cctx.region)
	case "AWS::StackId":
		return cfnStackARN(cctx.region, cctx.accountID, cctx.stackName)
	case "AWS::NotificationARNs":
		// A list-valued pseudo-parameter in a scalar context. Empty rather than
		// the reference string, since the accurate answer is "this stack has no
		// notification ARNs" — see resolveRefList, which is where a template
		// using it in a list position lands.
		return ""
	}
	// Parameter reference.
	if v, ok := cctx.params[ref]; ok {
		return v
	}
	// Deployed resource Ref. The value AWS documents is per resource type — an ARN for some,
	// a name for others, a service-assigned ID or a URL for others still — so cfnRefValue is
	// consulted first and the physical ID is only the fallback for the majority of types whose
	// documented Ref value it already is (#827).
	if dr, ok := cctx.resources[ref]; ok {
		if v, perType := cfnRefValue(dr, cctx); perType {
			return v
		}
		return dr.PhysicalID
	}
	return ref
}

// cfnPartition returns the ARN partition a region belongs to.
//
// "For standard AWS Regions, the partition is aws. For resources in other
// partitions, the partition is aws-{partitionname}. For example, the partition
// for resources in the China (Beijing and Ningxia) Regions is aws-cn and the
// partition for resources in the AWS GovCloud (US-West) Region is aws-us-gov."
//
// Derived from the region prefix rather than fixed at "aws", so a template that
// builds an ARN with `!Sub arn:${AWS::Partition}:s3:::bucket` gets the value real
// CloudFormation would give it. Note the rest of substrate mints ARNs in the aws
// partition unconditionally, so in a cn- or us-gov- region this value and a
// deployed resource's ARN disagree; that is pre-existing and not something this
// resolver can decide on its own.
func cfnPartition(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	default:
		return "aws"
	}
}

// cfnURLSuffix returns the AWS domain suffix for a region.
//
// "The suffix is typically amazonaws.com, but for the China (Beijing) Region, the
// suffix is amazonaws.com.cn." Both China regions take the .cn suffix, so the
// test is on the partition rather than on the one region the sentence names.
func cfnURLSuffix(region string) string {
	if cfnPartition(region) == "aws-cn" {
		return "amazonaws.com.cn"
	}
	return "amazonaws.com"
}

// cfnStackARN builds a stack's ARN — the value of AWS::StackId, and what
// CreateStack reports as its StackId.
//
// One function so the two cannot disagree: a template that writes its own
// AWS::StackId into a resource property and a caller reading StackId off
// CreateStack are describing the same stack, and #517 is what happens when two
// places derive an identity separately.
func cfnStackARN(region, accountID, stackName string) string {
	return fmt.Sprintf("arn:%s:cloudformation:%s:%s:stack/%s/%s",
		cfnPartition(region), region, accountID, stackName,
		cfnDeterministicUUID(region, accountID, stackName))
}

func resolveFnSub(args interface{}, cctx *cfnContext) string {
	switch v := args.(type) {
	case string:
		return substituteTemplate(v, cctx, nil)
	case []interface{}:
		if len(v) < 1 {
			return ""
		}
		tmplStr, ok := v[0].(string)
		if !ok {
			return ""
		}
		var extra map[string]string
		if len(v) >= 2 {
			if m, ok := v[1].(map[string]interface{}); ok {
				extra = make(map[string]string, len(m))
				for k, val := range m {
					extra[k] = resolveValue(val, cctx)
				}
			}
		}
		return substituteTemplate(tmplStr, cctx, extra)
	}
	return ""
}

func substituteTemplate(s string, cctx *cfnContext, extra map[string]string) string {
	var result strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '$' && i+1 < len(s) && s[i+1] == '{' {
			end := strings.Index(s[i+2:], "}")
			if end >= 0 {
				varName := s[i+2 : i+2+end]
				// "${!Literal}" is Fn::Sub's documented escape for a literal
				// "${Literal}": the exclamation mark is dropped and the rest is
				// emitted verbatim, with no substitution.
				if strings.HasPrefix(varName, "!") {
					result.WriteString("${" + varName[1:] + "}")
					i = i + 2 + end + 1
					continue
				}
				if v, ok := extra[varName]; ok {
					result.WriteString(v)
				} else if logicalID, attr, dotted := strings.Cut(varName, "."); dotted {
					// "${MyInstance.PublicIp}" is Fn::Sub's documented resource-attribute
					// form, answered "as if you used the Fn::GetAtt intrinsic function"
					// (#1103). The split is on the first dot only, because an attribute
					// name may itself contain one (Fn::GetAtt's published
					// "SourceSecurityGroup.OwnerAlias"). Neither a parameter name nor a
					// logical ID can contain a dot, so nothing resolveRef answers is lost.
					result.WriteString(resolveFnGetAtt([]interface{}{logicalID, attr}, cctx))
				} else {
					result.WriteString(resolveRef(varName, cctx))
				}
				i = i + 2 + end + 1
				continue
			}
		}
		result.WriteByte(s[i])
		i++
	}
	return result.String()
}

func resolveFnJoin(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	sep, ok := arr[0].(string)
	if !ok {
		sep = ""
	}
	items, ok := arr[1].([]interface{})
	if !ok {
		// An intrinsic yielding the list — a list-valued Fn::GetAtt (#1591), a list parameter's
		// Ref, Fn::GetAZs, Fn::Split — is resolved in a list context, which is what the Fn::Join
		// page permits there. It used to resolve to the empty string.
		return strings.Join(resolveValueList(arr[1], cctx), sep)
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, resolveValue(item, cctx))
	}
	return strings.Join(parts, sep)
}

// resolveFnSelect resolves Fn::Select by index.
//
// The list argument goes through resolveValueList rather than being required to
// be a literal []interface{}: the Fn::Select reference permits Fn::FindInMap,
// Fn::GetAtt, Fn::GetAZs, Fn::If, Fn::Split and Ref there, and
// `!Select ['2', !Split [':', arn]]` is the example the Fn::Split reference
// itself leads with. Requiring a literal made every one of those resolve to the
// empty string with nothing reporting a problem (#521).
//
// An out-of-range index yields the empty string. Real CloudFormation fails the
// stack — "Fn::Select cannot select nonexistent value at index N" — which is
// #502's typed-error work; until then the resolver keeps its existing shape
// rather than acquiring a second, inconsistent failure mode.
func resolveFnSelect(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	idxStr := resolveValue(arr[0], cctx)
	var idx int
	_, _ = fmt.Sscanf(idxStr, "%d", &idx)
	items := resolveValueList(arr[1], cctx)
	if idx < 0 || idx >= len(items) {
		return ""
	}
	return items[idx]
}

// resolveFnSplitJoined resolves Fn::Split in a scalar context by rejoining the
// elements on the delimiter, which reproduces the source string.
//
// Fn::Split returns a list, so a scalar property is a context CloudFormation
// itself would reject. Substrate resolves rather than rejects — a template that
// deploys against AWS must deploy here — and of the two ways to spell a list as
// one string, rejoining loses nothing while truncating to the first element
// silently dropped everything after the first delimiter (#521). A list-valued
// property gets the list itself, through resolveValueList.
func resolveFnSplitJoined(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	sep, ok := arr[0].(string)
	if !ok {
		// "For the Fn::Split delimiter, you can't use any functions. You must
		// specify a string value."
		return ""
	}
	return strings.Join(splitFnSplit(sep, resolveValue(arr[1], cctx)), sep)
}

// splitFnSplit applies Fn::Split's documented split semantics: every delimiter
// divides, so consecutive delimiters and a trailing delimiter each produce an
// empty element — `!Split ['|', 'a||c|']` is ["a", "", "c", ""].
//
// An empty delimiter has no documented behavior; it yields the source string as
// a single element rather than one element per byte, which is what strings.Split
// would give.
func splitFnSplit(sep, s string) []string {
	if sep == "" {
		return []string{s}
	}
	return strings.Split(s, sep)
}

// resolveFnFindInMap resolves Fn::FindInMap in a scalar context.
//
// A lookup whose leaf is a list is rejoined on commas, for the same reason
// Fn::Split's is (see resolveFnSplitJoined): the context has nowhere to put a
// list, and a comma is the separator every list-valued property in this codebase
// is split on. A failed lookup returns the empty string *and* records a failure,
// which is what stops it being the silent literal #522 reported.
func resolveFnFindInMap(args interface{}, cctx *cfnContext) string {
	leaf, ok := resolveFnFindInMapValue(args, cctx)
	if !ok {
		return ""
	}
	if _, isList := leaf.([]interface{}); isList {
		return strings.Join(resolveValueList(leaf, cctx), ",")
	}
	return resolveValue(leaf, cctx)
}

// resolveFnImportValue resolves Fn::ImportValue against the exports another stack
// in the same account and Region published, recording the import so the exporting
// stack cannot then be deleted.
//
// The argument goes through resolveValue because the documented form is an
// expression — `Fn::ImportValue: !Sub '${NetworkStack}-SubnetID'` — and the
// functions the API permits inside it (Fn::Base64, Fn::FindInMap, Fn::If,
// Fn::Join, Fn::Select, Fn::Sub, Ref) are all ones resolveValue already handles,
// so no special case is needed. The documented restriction is that "the value of
// these functions can't depend on a resource"; substrate does not enforce it,
// because by the time an import resolves the resources it could name have already
// deployed, so honoring such a reference is strictly more permissive than
// CloudFormation rather than differently behaved.
//
// A name that no export supplies is a resolution failure, not the empty string:
// the whole of #522's silent-literal defect is a value that looks resolved and is
// not, and an import naming an export that does not exist is a template a real
// deployment would reject.
func resolveFnImportValue(args interface{}, cctx *cfnContext) string {
	name := resolveValue(args, cctx)
	if name == "" {
		cctx.fail("Fn::ImportValue requires an export name")
		return ""
	}
	value, found := cctx.exports[name]
	if !found {
		cctx.fail("Fn::ImportValue: no exported output named %q in this account and region", name)
		return ""
	}
	// Recorded only on a successful resolution. A failed import leaves the
	// exporting stack — there is none — deletable, and recording the name would
	// make a typo'd import block a delete that has nothing to do with it.
	if cctx.imports == nil {
		cctx.imports = make(map[string]bool)
	}
	cctx.imports[name] = true
	return value
}

// importedNames returns the export names this deployment resolved, sorted.
func (c *cfnContext) importedNames() []string {
	if len(c.imports) == 0 {
		return nil
	}
	out := make([]string, 0, len(c.imports))
	for name := range c.imports {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// resolveFnFindInMapValue looks a value up in the template's Mappings section,
// returning the leaf *unresolved* so a list-valued leaf keeps its shape. ok is
// false when the lookup failed, in which case the reason has been recorded on
// cctx and the resource will report CREATE_FAILED.
func resolveFnFindInMapValue(args interface{}, cctx *cfnContext) (interface{}, bool) {
	leaf, ok, reason := cfnFindInMapLeaf(args, cctx)
	if !ok {
		cctx.fail("%s", reason)
	}
	return leaf, ok
}

// cfnFindInMapLeaf performs the lookup without recording anything, returning the
// reason a caller should report if it fails. Two callers need the answer without
// the side effect — cfnListValuedIntrinsic asks only about the leaf's shape, and
// would otherwise record the same failure twice for one intrinsic.
//
// Both keys go through resolveValue: the documented form is
// `!FindInMap [RegionMap, !Ref 'AWS::Region', InstanceType]`, and the two
// functions permitted inside the arguments without the AWS::LanguageExtensions
// transform are Fn::FindInMap and Ref — both of which resolveValue handles, so
// the nested-key form (`!FindInMap [Arch2AMI, !Ref 'AWS::Region', !FindInMap
// [Type2Arch, !Ref InstanceType, Arch]]`) resolves without a special case. The
// map *name* is resolved too, which costs nothing: a literal string resolves to
// itself, and the documented restriction is on what a template may write, not on
// what this may read.
//
// A missing key is a failure rather than a fallback, unless the optional fourth
// argument supplies one: "if omitted, Fn::FindInMap raises an error when a key is
// not found", and "the fourth parameter must be a map with the key DefaultValue".
// A present top-level key holding no such second-level key takes the same path,
// since the documented DefaultValue covers "either the TopLevelKey or
// SecondLevelKey is not found".
func cfnFindInMapLeaf(args interface{}, cctx *cfnContext) (leaf interface{}, ok bool, reason string) {
	arr, isArr := args.([]interface{})
	if !isArr || len(arr) < 3 {
		return nil, false, "Fn::FindInMap requires a map name and two keys"
	}
	mapName := resolveValue(arr[0], cctx)
	topKey := resolveValue(arr[1], cctx)
	secondKey := resolveValue(arr[2], cctx)

	defaultValue, hasDefault := cfnFindInMapDefault(arr)

	top, found := cctx.mappings[mapName]
	if !found {
		if hasDefault {
			return defaultValue, true, ""
		}
		return nil, false, fmt.Sprintf(
			"Fn::FindInMap: no mapping named %q in the template's Mappings section", mapName)
	}
	second, found := top[topKey]
	if !found {
		if hasDefault {
			return defaultValue, true, ""
		}
		return nil, false, fmt.Sprintf(
			"Fn::FindInMap: mapping %q has no top-level key %q", mapName, topKey)
	}
	value, found := second[secondKey]
	if !found {
		if hasDefault {
			return defaultValue, true, ""
		}
		return nil, false, fmt.Sprintf(
			"Fn::FindInMap: mapping %q key %q has no second-level key %q", mapName, topKey, secondKey)
	}
	return value, true, ""
}

// cfnFindInMapDefault reads Fn::FindInMap's optional fourth argument.
//
// The argument is a map with exactly the key DefaultValue; anything else is not
// the documented form and is ignored rather than guessed at, so a template that
// misspells it gets the lookup failure it would get from CloudFormation instead
// of a silently different value.
func cfnFindInMapDefault(arr []interface{}) (interface{}, bool) {
	if len(arr) < 4 {
		return nil, false
	}
	m, ok := arr[3].(map[string]interface{})
	if !ok {
		return nil, false
	}
	v, has := m["DefaultValue"]
	return v, has
}

// resolveFnGetAZs resolves Fn::GetAZs to the Availability Zones of a region.
//
// The zones come from ec2SeededAZSuffixes, the same list EC2's
// DescribeAvailabilityZones, DescribeInstanceTypeOfferings and
// DescribeSpotPriceHistory derive their zone names from, so a template that picks
// a zone with !Select over !GetAZs names a zone a caller can then query. Two
// independent lists would be the defect worth avoiding here: a subnet in a zone
// DescribeAvailabilityZones does not report is not observable-consistent.
//
// "Specifying an empty string is equivalent to specifying AWS::Region", and the
// one function permitted inside is Ref — which is how the documented
// `{"Fn::GetAZs": {"Ref": "AWS::Region"}}` form arrives — so the argument goes
// through resolveValue and an empty result falls back to the caller's region.
//
// Substrate reports every zone for every region, where real CloudFormation
// "returns only Availability Zones that have a default subnet" and warns the
// order "isn't guaranteed". Substrate's order is fixed, which is what a
// deterministic emulator owes a caller indexing into the list.
func resolveFnGetAZs(args interface{}, cctx *cfnContext) []string {
	region := resolveValue(args, cctx)
	if region == "" {
		region = cctx.region
	}
	out := make([]string, 0, len(ec2SeededAZSuffixes))
	for _, suffix := range ec2SeededAZSuffixes {
		out = append(out, region+suffix)
	}
	return out
}

// resolveFnCidr resolves Fn::Cidr to a list of CIDR blocks.
//
// The arguments are `[ipBlock, count, cidrBits]`: "count — the number of CIDRs to
// generate. Valid range is between 1 and 256", and "cidrBits — the number of
// subnet bits for the CIDR. For example, specifying a value "8" for this
// parameter will create a CIDR with a mask of "/24"". So the generated mask is
// (address bits − cidrBits): the documented example
// `{"Fn::Cidr": ["192.168.0.0/24", "6", "5"]}` yields six /27 blocks, since
// 32 − 5 = 27.
//
// Both IPv4 and IPv6 are handled, because the documented IPv6 example asks for
// cidrBits 64 against a /56 — the address width has to come from the parsed
// block rather than being assumed to be 32.
//
// A malformed or impossible request records a failure rather than returning a
// short list: a caller doing `!Select [3, !Cidr [...]]` would otherwise read an
// empty string out of a list that was silently too short.
func resolveFnCidr(args interface{}, cctx *cfnContext) []string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 3 {
		cctx.fail("Fn::Cidr requires an ipBlock, a count and a cidrBits value")
		return nil
	}
	ipBlock := resolveValue(arr[0], cctx)
	count, err := strconv.Atoi(resolveValue(arr[1], cctx))
	if err != nil || count < 1 || count > 256 {
		cctx.fail("Fn::Cidr: count %q is not in the valid range 1-256", resolveValue(arr[1], cctx))
		return nil
	}
	cidrBits, err := strconv.Atoi(resolveValue(arr[2], cctx))
	if err != nil || cidrBits < 1 {
		cctx.fail("Fn::Cidr: cidrBits %q is not a positive integer", resolveValue(arr[2], cctx))
		return nil
	}
	blocks, err := cfnCidrBlocks(ipBlock, count, cidrBits)
	if err != nil {
		cctx.fail("Fn::Cidr: %s", err)
		return nil
	}
	return blocks
}

// cfnCidrBlocks splits ipBlock into count subnets whose host part is cidrBits
// wide, in ascending order.
//
// net/netip rather than net: netip.Prefix carries the address family, which is
// what decides whether the new mask is 32 − cidrBits or 128 − cidrBits, and
// netip.Addr's bytes can be incremented without the allocation-per-address that
// net.IP arithmetic needs.
func cfnCidrBlocks(ipBlock string, count, cidrBits int) ([]string, error) {
	prefix, err := netip.ParsePrefix(ipBlock)
	if err != nil {
		return nil, fmt.Errorf("ipBlock %q is not a CIDR block: %w", ipBlock, err)
	}
	prefix = prefix.Masked()
	addrBits := prefix.Addr().BitLen()
	newMask := addrBits - cidrBits
	if newMask < prefix.Bits() {
		return nil, fmt.Errorf("cidrBits %d would widen %s to /%d, which is larger than the block itself",
			cidrBits, ipBlock, newMask)
	}
	// Each subnet is 2^cidrBits addresses, so the number that fit is
	// 2^(newMask − prefix.Bits()). Computed as a shift over the exponent rather
	// than over the count itself, since a /0 with cidrBits 1 would overflow.
	if exp := newMask - prefix.Bits(); exp < 9 && count > 1<<exp {
		return nil, fmt.Errorf("%s holds only %d /%d blocks, not %d", ipBlock, 1<<exp, newMask, count)
	}
	out := make([]string, 0, count)
	addr := prefix.Addr()
	for i := 0; i < count; i++ {
		out = append(out, netip.PrefixFrom(addr, newMask).String())
		if i == count-1 {
			// Not advanced past the last block, so a request for the single
			// block that fills the whole address space succeeds rather than
			// failing on an increment nobody needed.
			break
		}
		addr, err = cfnCidrAdvance(addr, cidrBits)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// cfnCidrAdvance returns the address 2^hostBits above addr, which is the first
// address of the next subnet of that size.
func cfnCidrAdvance(addr netip.Addr, hostBits int) (netip.Addr, error) {
	b := addr.AsSlice()
	// The increment lands on the bit hostBits from the right, so it is applied to
	// byte len(b)-1-hostBits/8 at bit hostBits%8, and any carry propagates left.
	idx := len(b) - 1 - hostBits/8
	if idx < 0 {
		return netip.Addr{}, fmt.Errorf("cidrBits %d exceeds the address width", hostBits)
	}
	carry := uint16(1) << (hostBits % 8)
	for ; idx >= 0 && carry != 0; idx-- {
		sum := uint16(b[idx]) + carry
		b[idx] = byte(sum)
		carry = sum >> 8
	}
	next, ok := netip.AddrFromSlice(b)
	if !ok {
		return netip.Addr{}, fmt.Errorf("advancing %s by 2^%d produced an invalid address", addr, hostBits)
	}
	return next, nil
}

func resolveFnBase64(args interface{}, cctx *cfnContext) string {
	s := resolveValue(args, cctx)
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func resolveFnGetAtt(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	logicalID, ok := arr[0].(string)
	if !ok {
		return ""
	}
	attr, ok := arr[1].(string)
	if !ok {
		return ""
	}
	if dr, ok := cctx.resources[logicalID]; ok {
		// Per-type resolution lives in cfn_intrinsics.go next to Ref's, because the two
		// answer the same question — what value does *this* type report — and had drifted
		// into two unrelated shapes: a switch on the type for one, a switch on the
		// attribute name for the other (#827).
		return cfnGetAttValue(dr, attr, cctx)
	}
	// An attribute of a resource the template does not declare. Left as "<logicalID>.<attr>"
	// rather than resolved to empty: it names the mistake in the template, and it is the
	// form the short-form !GetAtt tests assert both spellings agree on.
	return logicalID + "." + attr
}

// resolveDynamicRef resolves {{resolve:ssm:/path}} and
// {{resolve:ssm-secure:/path}} dynamic references found inside CFN property
// strings. Other reference types are returned unchanged.
func (d *StackDeployer) resolveDynamicRef(ctx context.Context, s string, cctx *cfnContext) string {
	const prefix = "{{resolve:"
	const suffix = "}}"
	if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, suffix) {
		return s
	}
	inner := s[len(prefix) : len(s)-len(suffix)]
	// inner is like "ssm:/my/param" or "ssm-secure:/my/param" or "ssm:/my/param:3"
	colonIdx := strings.Index(inner, ":")
	if colonIdx < 0 {
		return s
	}
	service := inner[:colonIdx]
	rest := inner[colonIdx+1:]
	switch service {
	case "ssm", "ssm-secure":
		// rest may be "/path" or "/path:version" — ignore version for now.
		paramName := rest
		if idx := strings.LastIndex(rest, ":"); idx > 0 {
			// Only strip version if the part after the last colon is a number.
			maybeSuffix := rest[idx+1:]
			isNum := len(maybeSuffix) > 0
			for _, ch := range maybeSuffix {
				if ch < '0' || ch > '9' {
					isNum = false
					break
				}
			}
			if isNum {
				paramName = rest[:idx]
			}
		}
		body := map[string]string{"Name": paramName}
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return s
		}
		req := &AWSRequest{
			Service:   "ssm",
			Operation: "GetParameter",
			Body:      bodyBytes,
			Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
			Params:    map[string]string{},
		}
		resp, _, routeErr := d.dispatch(ctx, req, cctx.stackName)
		if routeErr != nil || resp == nil {
			return s
		}
		var result struct {
			Parameter struct {
				Value string `json:"Value"`
			} `json:"Parameter"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr != nil {
			return s
		}
		return result.Parameter.Value
	default:
		return s
	}
}

func resolveFnIf(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 3 {
		return ""
	}
	condName, ok := arr[0].(string)
	if !ok {
		return ""
	}
	// Through the accessor rather than the map so that the two read sites cannot
	// disagree. evaluateConditions has already resolved every declared condition by
	// the time any Fn::If is resolved, so a direct map read would behave the same
	// for a valid template; it would differ only for a name the template never
	// declared, which both spellings answer false.
	if cctx.condition(condName) {
		return resolveValue(arr[1], cctx)
	}
	return resolveValue(arr[2], cctx)
}

// resolveStringProp resolves a property value from props using the cfnContext.
//
// key is a single top-level property name. It is looked up by one flat map index, so a key
// spelled with a dot matches nothing and the fallback is returned unconditionally — which is
// how five call sites came to be silently dead (#877). Use [resolveNestedStringProp] for a
// property inside a nested object and [resolveIndexedStringProp] for one inside a list;
// TestCFNProps_NoDottedResolveStringPropKey is the tripwire that keeps a sixth from appearing.
func resolveStringProp(props map[string]interface{}, key, fallback string, cctx *cfnContext) string {
	if props == nil {
		return fallback
	}
	v, ok := props[key]
	if !ok {
		return fallback
	}
	result := resolveValue(v, cctx)
	if result == "" {
		return fallback
	}
	return result
}

// resolveNestedStringProp resolves props[outer][inner] to a string, returning fallback when
// either level is absent or the value resolves empty.
//
// This exists rather than teaching [resolveStringProp] to split a dotted key, and the reason is
// the failure mode #877 is about rather than a preference. A shared dotted-path walk would turn
// ECS's launch type and Glue's command name from effective constants into template-controlled
// values at all five dotted call sites at once, silently, as a side effect of a helper change —
// a behavior change to four unrelated resource types delivered as a refactor. A separate
// function changes nothing until a call site is edited to use it.
//
// The value goes through resolveValue, so an intrinsic in the nested position still resolves:
// a Command.ScriptLocation written as !Sub "s3://${Bucket}/etl.py" is the common template shape.
func resolveNestedStringProp(props map[string]interface{}, outer, inner, fallback string, cctx *cfnContext) string {
	if props == nil {
		return fallback
	}
	// A YAML or JSON template decodes a nested object to map[string]interface{}; nothing else
	// can carry a named member, so a non-map value here means the template put a scalar or a
	// list where an object belongs and there is no inner property to read.
	nested, ok := props[outer].(map[string]interface{})
	if !ok {
		return fallback
	}
	v, ok := nested[inner]
	if !ok {
		return fallback
	}
	result := resolveValue(v, cctx)
	if result == "" {
		return fallback
	}
	return result
}

// resolveIndexedStringProp resolves element index of the list at props[key] to a string,
// returning fallback when the property is absent, is not a list, is shorter than index+1, or
// resolves empty.
//
// Separate from [resolveNestedStringProp] because a map walk cannot reach a list element at all
// — which is why RequiresCompatibilities.0 was the highest-impact of #877's five sites and the
// one a dotted-path walk would have left broken while appearing to cover everything.
func resolveIndexedStringProp(props map[string]interface{}, key string, index int, fallback string, cctx *cfnContext) string {
	if props == nil || index < 0 {
		return fallback
	}
	list, ok := props[key].([]interface{})
	if !ok || index >= len(list) {
		return fallback
	}
	result := resolveValue(list[index], cctx)
	if result == "" {
		return fallback
	}
	return result
}

// deployGenericStub handles unknown CloudFormation resource types by generating a
// synthetic ARN and persisting the resource properties in cfnStubNamespace.
func (d *StackDeployer) deployGenericStub(
	ctx context.Context,
	logicalID string,
	resType string,
	props map[string]interface{},
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// Build a deterministic ARN from the resource type and logical ID.
	// Format: arn:aws:{service}:{region}:{acct}:{typeSlug}/{logicalID}
	parts := strings.SplitN(resType, "::", 3) // ["AWS", "Service", "ResourceType"]
	service := ""
	rtype := logicalID
	if len(parts) == 3 {
		service = strings.ToLower(parts[1])
		rtype = strings.ToLower(parts[2])
	}
	arn := fmt.Sprintf("arn:aws:%s:%s:%s:%s/%s", service, cctx.region, cctx.accountID, rtype, logicalID)

	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployGenericStub %s: %w", resType, err)
	}

	return DeployedResource{
		LogicalID:  logicalID,
		Type:       resType,
		PhysicalID: logicalID,
		ARN:        arn,
	}, 0, nil
}

// parseCFNTemplate attempts JSON then YAML unmarshalling of a CloudFormation
// template. The YAML path expands short-form intrinsic tags (!Sub, !Ref, !If, …)
// into their long forms first, so the two syntaxes resolve identically; see
// cfnExpandYAMLTags. JSON templates cannot carry tags, so the JSON path is
// untouched.
func (d *StackDeployer) parseCFNTemplate(cfn string) (*cfnTemplate, error) {
	var tmpl cfnTemplate
	if err := json.Unmarshal([]byte(cfn), &tmpl); err == nil {
		if len(tmpl.Resources) > 0 {
			return &tmpl, nil
		}
	}
	if unknown, err := cfnUnmarshalYAMLTemplate(cfn, &tmpl); err == nil {
		for _, tag := range unknown {
			d.logger.Warn("cfn: unrecognized YAML tag left unexpanded; its value is used verbatim",
				"tag", tag,
			)
		}
		if len(tmpl.Resources) > 0 {
			return &tmpl, nil
		}
	}
	// Try once more with JSON for better error messages on empty templates.
	if err := json.Unmarshal([]byte(cfn), &tmpl); err != nil {
		return nil, fmt.Errorf("invalid CloudFormation template (JSON: %w)", err)
	}
	return &tmpl, nil
}

// templateDeclaresTransform reports whether a template body declares a top-level Transform
// section. A body that does not parse declares none: GetTemplate answers whatever was stored, and a
// stored body has already been through CreateStack's parse.
func (d *StackDeployer) templateDeclaresTransform(body string) bool {
	tmpl, err := d.parseCFNTemplate(body)
	if err != nil {
		return false
	}
	switch t := tmpl.Transform.(type) {
	case nil:
		return false
	case string:
		return t != ""
	case []interface{}:
		return len(t) > 0
	default:
		return true
	}
}

// xmlUnmarshalIAM strips the IAM namespace from body and unmarshals into dst.
// Used to parse XML responses from IAMPlugin (which uses the iam.amazonaws.com namespace).
func xmlUnmarshalIAM(body []byte, dst interface{}) error {
	body = bytes.ReplaceAll(body, []byte(` xmlns="`+iamXMLNS+`"`), nil)
	return xml.Unmarshal(body, dst)
}

// marshalToJSON marshals v to a JSON string. Returns "" on nil or error.
//
// It resolves no intrinsics, because it has no *cfnContext: a caller marshaling a
// template property owns resolution, and should pass the property through
// resolveNested first — or call cfnJSONDocument, which does that and also accepts
// the property's string form (#1153). Marshaling a raw property stores
// {"Fn::GetAtt":[…]} as data.
func marshalToJSON(v interface{}) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// cfnJSONDocument renders a Json-typed template property — a policy document, an
// event pattern — as the JSON text the service API takes, with intrinsics resolved
// at every depth (#1153).
//
// CloudFormation accepts such a property either as an object or as a string holding
// the JSON. A string, or an intrinsic that resolves to one (an Fn::Sub over the
// document text), is already the document and is returned as-is; marshaling it
// would quote it a second time. Anything else is resolved by resolveNested and
// marshaled. A nil property answers "".
func cfnJSONDocument(raw interface{}, cctx *cfnContext) string {
	if raw == nil {
		return ""
	}
	resolved := resolveNested(raw, cctx)
	if s, ok := resolved.(string); ok {
		return s
	}
	return marshalToJSON(resolved)
}

// deployLambdaEventSourceMapping creates a Lambda event source mapping for the
// given CFN resource (AWS::Lambda::EventSourceMapping).
func (d *StackDeployer) deployLambdaEventSourceMapping(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	functionName := resolveStringProp(props, "FunctionName", "", cctx)
	eventSourceArn := resolveStringProp(props, "EventSourceArn", "", cctx)
	startingPosition := resolveStringProp(props, "StartingPosition", "TRIM_HORIZON", cctx)

	bodyMap := map[string]interface{}{
		"FunctionName":     functionName,
		"EventSourceArn":   eventSourceArn,
		"StartingPosition": startingPosition,
	}
	if batchSize, ok := props["BatchSize"]; ok {
		bodyMap["BatchSize"] = batchSize
	}
	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal esm body: %w", err)
	}

	req := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/event-source-mappings/",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
		Body:      bodyBytes,
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::Lambda::EventSourceMapping",
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result ESMConfig
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.UUID
		}
	}

	return dr, cost, nil
}
