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
	"reflect"
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
	DependsOn  interface{}            `json:"DependsOn,omitempty" yaml:"DependsOn,omitempty"`
	Condition  string                 `json:"Condition,omitempty" yaml:"Condition,omitempty"`

	// DeletionPolicy is what to do with the resource when it leaves the stack:
	// "Delete", "Retain", "RetainExceptOnCreate" or "Snapshot". Empty means the
	// template declared none, which is not the same as "Delete" — the default is
	// Snapshot for two RDS types, so the default is resolved rather than assumed
	// (see cfnDeletionPolicyFor).
	DeletionPolicy string `json:"DeletionPolicy,omitempty" yaml:"DeletionPolicy,omitempty"`

	// UpdateReplacePolicy is the same set of values for the resource CloudFormation
	// leaves behind when an update replaces it. Substrate's UpdateStack is a
	// re-deploy rather than a per-resource replace, so nothing consults this yet;
	// it is parsed so a template that declares it round-trips rather than being
	// silently dropped, and so DescribeStackResources can report it.
	UpdateReplacePolicy string `json:"UpdateReplacePolicy,omitempty" yaml:"UpdateReplacePolicy,omitempty"`
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
)

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

// cfnContext holds per-deployment resolution context for intrinsic functions.
type cfnContext struct {
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

	// CreatedAt is the creation timestamp.
	CreatedAt time.Time `json:"CreatedAt"`
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

// typePriority determines deployment order for CloudFormation resources.
// Lower numbers deploy first.
var typePriority = map[string]int{
	"AWS::IAM::Policy":          0,
	"AWS::IAM::Role":            1,
	"AWS::EC2::VPC":             1,
	"AWS::Route53::HostedZone":  1,
	"AWS::KMS::Key":             1,
	"AWS::DynamoDB::Table":      2,
	"AWS::S3::Bucket":           2,
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
	"AWS::ECR::LifecyclePolicy":  3,
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
	"AWS::OpenSearchService::Domain":     2,
	"AWS::WAFv2::WebACL":                 2,
	"AWS::Backup::BackupPlan":            2,
	"AWS::CodeBuild::Project":            2,
	"AWS::CodePipeline::Pipeline":        3,
	"AWS::CodeDeploy::DeploymentGroup":   3,
	"AWS::CloudTrail::Trail":             2,
	"AWS::Config::ConfigRule":            3,
	"AWS::Config::ConfigurationRecorder": 2,
	"AWS::Transfer::Server":              2,
	"AWS::Athena::WorkGroup":             2,
}

// cfnIdentity is the AWS account and region a StackDeployer deploys into.
//
// Two strings rather than a whole *RequestContext: a deployer that carried one
// would carry a single request's ID and timestamp across every resource it
// deploys, which is wrong — each dispatch generates its own — and would invite a
// future reader to propagate Principal, which is a separate decision about how
// stack-deployed requests are authorized.
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

// NewStackDeployer creates a StackDeployer wired to the provided dependencies.
//
// The deployer deploys into substrate's default account and region unless
// [WithDeployerIdentity] says otherwise.
func NewStackDeployer(registry *PluginRegistry, store *EventStore, state StateManager, tc *TimeController, logger Logger, costs *CostController, opts ...StackDeployerOption) *StackDeployer {
	d := &StackDeployer{
		registry: registry,
		store:    store,
		state:    state,
		tc:       tc,
		logger:   logger,
		costs:    costs,
		identity: cfnIdentity{accountID: testAccountID, region: defaultRegion},
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
	if err := opts.validate(); err != nil {
		return nil, err
	}
	tmpl, err := d.parseCFNTemplate(cfn)
	if err != nil {
		return nil, cfnErrf(ErrCFNTemplateInvalid, "parse template: %w", err)
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

	// Sort logical IDs by type priority, then alphabetically for stability.
	type entry struct {
		logicalID string
		resource  cfnResource
		priority  int
	}
	entries := make([]entry, 0, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		// Skip resources with a false condition.
		if res.Condition != "" {
			if val, ok := cctx.conditions[res.Condition]; ok && !val {
				d.logger.Info("cfn: skipping resource due to false condition",
					"logical_id", logicalID, "condition", res.Condition)
				continue
			}
		}
		p, ok := typePriority[res.Type]
		if !ok {
			p = 99
		}
		entries = append(entries, entry{logicalID: logicalID, resource: res, priority: p})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].priority != entries[j].priority {
			return entries[i].priority < entries[j].priority
		}
		return entries[i].logicalID < entries[j].logicalID
	})

	resources := make([]DeployedResource, 0, len(entries))
	var totalCost float64

	for _, e := range entries {
		dr, cost, deployErr := d.deployResource(ctx, e.logicalID, e.resource, streamID, cctx)
		if deployErr != nil {
			return nil, &CFNResourceDeployError{LogicalID: e.logicalID, Err: deployErr}
		}
		totalCost += cost
		cctx.resources[e.logicalID] = dr
		resources = append(resources, dr)
	}

	// A refusal that only says "this already exists" is not a failure of this
	// deployment, because a real deployment would not have made the call. Cleared
	// before the stack's fate is decided, or every redeploy of an unchanged template
	// would look like a failed create and roll back the resources it was asked to
	// keep.
	if prev, prevErr := d.previousStack(ctx, stackName); prevErr == nil {
		d.clearUnchangedRedeploys(prev, tmpl.Resources, resources)
	}

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
	for _, outKey := range outKeys {
		outVal := tmpl.Outputs[outKey]
		outputs[outKey] = resolveValue(outVal.Value, cctx)
		if outVal.Export == nil {
			continue
		}
		if name := resolveValue(outVal.Export.Name, cctx); name != "" {
			exportNames[outKey] = name
		}
	}
	pending := CFNStackState{Outputs: outputs, ExportNames: exportNames}
	if err := d.checkExports(ctx, stackName, pending.exports()); err != nil {
		return nil, err
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
	if failures := cfnFailedResources(resources); len(failures) > 0 {
		return d.handleFailedCreate(ctx, cfnFailedCreate{
			stackName:    stackName,
			templateBody: cfn,
			params:       cctx.params,
			resources:    resources,
			failures:     failures,
			streamID:     streamID,
			totalCost:    totalCost,
			start:        start,
			onFailure:    opts.resolve(),
		})
	}

	duration := d.tc.Now().Sub(start)

	result := &DeployResult{
		StackName: stackName,
		Resources: resources,
		StreamID:  streamID,
		TotalCost: totalCost,
		Duration:  duration,
		Outputs:   outputs,
		Status:    cfnStackCreateComplete,
	}

	// Persist stack state if state manager is available.
	if d.state != nil {
		state := CFNStackState{
			StackName:    stackName,
			TemplateBody: cfn,
			Parameters:   cctx.params,
			Resources:    resources,
			Outputs:      outputs,
			ExportNames:  exportNames,
			Imports:      cctx.importedNames(),
			AccountID:    d.identity.accountID,
			Region:       d.identity.region,
			Status:       cfnStackCreateComplete,
			OnFailure:    opts.resolve(),
			CreatedAt:    start,
			UpdatedAt:    d.tc.Now(),
		}
		d.persistStack(ctx, state)
	}

	return result, nil
}

// UpdateStack re-deploys a previously deployed stack with new template or parameters.
//
// A resource that fails during the update rolls the stack back onto the template that
// was deployed before it and reports UPDATE_ROLLBACK_COMPLETE. That is a *re-deploy of
// the previous template*, not CloudFormation's per-resource restore — see
// [StackDeployer.rollbackFailedUpdate] for what the difference costs. An update whose
// rollback is not wanted has no option to disable it: DisableRollback and OnFailure are
// CreateStack parameters, and UpdateStack models neither.
func (d *StackDeployer) UpdateStack(ctx context.Context, cfn, stackName string, params map[string]string) (*DeployResult, error) {
	// Read before the update, because the update overwrites it: this is the only
	// description of the pre-update state a rollback has to converge on.
	prev, prevErr := d.loadStack(ctx, stackName)

	// DO_NOTHING, so a failed update does not take the create-rollback path and
	// delete the resources: an update failure rolls *back to a template*, and
	// deleting what the previous template declares is the opposite of that.
	result, err := d.DeployWithOptions(ctx, cfn, stackName, params,
		CFNDeployOptions{OnFailure: CFNOnFailureDoNothing})
	if err != nil {
		return nil, fmt.Errorf("update stack %s: %w", stackName, err)
	}

	if failures := cfnFailedResources(result.Resources); len(failures) > 0 {
		// With no readable previous template there is nothing to converge on, so
		// the update stops at UPDATE_FAILED rather than claiming a rollback it
		// could not perform.
		if prevErr != nil || prev == nil || prev.TemplateBody == "" {
			reason := strings.Join(failures, "; ")
			result.Status = cfnStackUpdateFailed
			result.StatusReason = reason
			d.setStackStatus(ctx, stackName, cfnStackUpdateFailed, reason)
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
		return d.rollbackFailedUpdate(ctx, *prev, failures, stackName)
	}

	// Overwrite the persisted status.
	if d.state != nil {
		data, getErr := d.state.Get(ctx, cfnNamespace, "stack:"+stackName)
		if getErr == nil && data != nil {
			var s CFNStackState
			if unmarshalErr := json.Unmarshal(data, &s); unmarshalErr == nil {
				s.Status = cfnStackUpdateComplete
				s.StatusReason = ""
				s.UpdatedAt = d.tc.Now()
				d.persistStack(ctx, s)
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
			d.persistStack(ctx, *stack)
			return cfnErrf(ErrCFNDeleteFailed, "delete stack %s: %s", stackName,
				strings.Join(failed, "; "))
		}
	}

	return d.removeStackRecord(ctx, stackName)
}

// loadStack reads a persisted stack by name, returning (nil, nil) when no stack of
// that name exists and (nil, nil) when its record does not unmarshal.
//
// A corrupt record reads as absent rather than as an error because that is what
// ListStacks already does with one, and a delete that cannot be completed because
// the record is unreadable would leave a stack nothing can remove.
func (d *StackDeployer) loadStack(ctx context.Context, stackName string) (*CFNStackState, error) {
	data, err := d.state.Get(ctx, cfnNamespace, "stack:"+stackName)
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
	names, err := d.loadStackNames(ctx)
	if err != nil {
		return nil, err
	}
	stacks := make([]CFNStackState, 0, len(names))
	for _, name := range names {
		data, getErr := d.state.Get(ctx, cfnNamespace, "stack:"+name)
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

func (d *StackDeployer) persistStack(ctx context.Context, s CFNStackState) {
	data, err := json.Marshal(s)
	if err != nil {
		d.logger.Warn("cfn: failed to marshal stack state", "err", err)
		return
	}
	if err := d.state.Put(ctx, cfnNamespace, "stack:"+s.StackName, data); err != nil {
		d.logger.Warn("cfn: failed to persist stack state", "err", err)
		return
	}
	names, _ := d.loadStackNames(ctx)
	for _, n := range names {
		if n == s.StackName {
			return
		}
	}
	names = append(names, s.StackName)
	_ = d.saveStackNames(ctx, names)
}

func (d *StackDeployer) loadStackNames(ctx context.Context) ([]string, error) {
	data, err := d.state.Get(ctx, cfnNamespace, "stack_names")
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
	return d.state.Put(ctx, cfnNamespace, "stack_names", data)
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
	data, err := d.state.Get(ctx, cfnNamespace, "stack:"+stackName)
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
func (d *StackDeployer) CreateChangeSet(ctx context.Context, stackName, changeSetName, templateBody string, params map[string]string) (*CFNChangeSet, error) {
	if d.state == nil {
		return nil, cfnErrf(ErrCFNStateRequired, "cfn CreateChangeSet: state manager required")
	}

	// Load existing stack.
	data, err := d.state.Get(ctx, cfnNamespace, "stack:"+stackName)
	if err != nil || data == nil {
		return nil, cfnErrf(ErrCFNStackNotFound, "cfn CreateChangeSet: stack %q not found", stackName)
	}
	var stack CFNStackState
	if err := json.Unmarshal(data, &stack); err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: unmarshal stack: %w", err)
	}

	// Parse old and new templates.
	oldTmpl, err := d.parseCFNTemplate(stack.TemplateBody)
	if err != nil {
		return nil, cfnErrf(ErrCFNTemplateInvalid, "cfn CreateChangeSet: parse old template: %w", err)
	}
	newTmpl, err := d.parseCFNTemplate(templateBody)
	if err != nil {
		return nil, cfnErrf(ErrCFNTemplateInvalid, "cfn CreateChangeSet: parse new template: %w", err)
	}

	// Diff resources.
	changes := diffCFNResources(oldTmpl.Resources, newTmpl.Resources)

	cs := &CFNChangeSet{
		ChangeSetName: changeSetName,
		StackName:     stackName,
		Status:        "CREATE_COMPLETE",
		TemplateBody:  templateBody,
		Parameters:    params,
		Changes:       changes,
		CreatedAt:     d.tc.Now(),
	}

	// Persist.
	csData, err := json.Marshal(cs)
	if err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: marshal: %w", err)
	}
	csKey := "changeset:" + stackName + "/" + changeSetName
	if err := d.state.Put(ctx, cfnNamespace, csKey, csData); err != nil {
		return nil, fmt.Errorf("cfn CreateChangeSet: put: %w", err)
	}

	// Update index.
	names, _ := d.loadChangeSetNames(ctx, stackName)
	names = append(names, changeSetName)
	_ = d.saveChangeSetNames(ctx, stackName, names)

	return cs, nil
}

// DescribeChangeSet returns a previously created change set.
func (d *StackDeployer) DescribeChangeSet(ctx context.Context, stackName, changeSetName string) (*CFNChangeSet, error) {
	if d.state == nil {
		return nil, cfnErrf(ErrCFNStateRequired, "cfn DescribeChangeSet: state manager required")
	}
	data, err := d.state.Get(ctx, cfnNamespace, "changeset:"+stackName+"/"+changeSetName)
	if err != nil || data == nil {
		return nil, cfnErrf(ErrCFNChangeSetNotFound, "cfn DescribeChangeSet: change set %q not found", changeSetName)
	}
	var cs CFNChangeSet
	if err := json.Unmarshal(data, &cs); err != nil {
		return nil, fmt.Errorf("cfn DescribeChangeSet: unmarshal: %w", err)
	}
	return &cs, nil
}

// ExecuteChangeSet applies a change set by calling UpdateStack with the change
// set's template and parameters, then deletes the consumed change set.
func (d *StackDeployer) ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) (*DeployResult, error) {
	cs, err := d.DescribeChangeSet(ctx, stackName, changeSetName)
	if err != nil {
		return nil, err
	}
	result, err := d.UpdateStack(ctx, cs.TemplateBody, stackName, cs.Parameters)
	if err != nil {
		return nil, err
	}
	_ = d.DeleteChangeSet(ctx, stackName, changeSetName)
	return result, nil
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
		data, getErr := d.state.Get(ctx, cfnNamespace, "changeset:"+stackName+"/"+name)
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
	if err := d.state.Delete(ctx, cfnNamespace, "changeset:"+stackName+"/"+changeSetName); err != nil {
		return fmt.Errorf("cfn DeleteChangeSet: %w", err)
	}
	names, _ := d.loadChangeSetNames(ctx, stackName)
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
func (d *StackDeployer) DetectStackDrift(ctx context.Context, stackName string) (*CFNDriftResult, error) {
	if d.state == nil {
		return nil, cfnErrf(ErrCFNStateRequired, "cfn DetectStackDrift: state manager required")
	}
	data, err := d.state.Get(ctx, cfnNamespace, "stack:"+stackName)
	if err != nil || data == nil {
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

	for _, dr := range stack.Resources {
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
func (d *StackDeployer) StartStackDriftDetection(ctx context.Context, stackName string) (string, error) {
	if d.state == nil {
		return "", cfnErrf(ErrCFNStateRequired, "cfn StartStackDriftDetection: state manager required")
	}
	if data, err := d.state.Get(ctx, cfnNamespace, "stack:"+stackName); err != nil || data == nil {
		return "", cfnErrf(ErrCFNStackNotFound, "cfn StartStackDriftDetection: stack %q not found", stackName)
	}

	detectionID := generateRequestID()
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

	result, err := d.DetectStackDrift(ctx, stackName)
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
	data, err := d.state.Get(ctx, cfnNamespace, "drift_detection:"+detectionID)
	if err != nil || data == nil {
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
	if err := d.state.Put(ctx, cfnNamespace, "drift_detection:"+status.StackDriftDetectionID, data); err != nil {
		return fmt.Errorf("cfn saveDriftDetection: put: %w", err)
	}
	return nil
}

func (d *StackDeployer) loadChangeSetNames(ctx context.Context, stackName string) ([]string, error) {
	data, err := d.state.Get(ctx, cfnNamespace, "changeset_names:"+stackName)
	if err != nil || data == nil {
		return nil, nil //nolint:nilerr
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
	return d.state.Put(ctx, cfnNamespace, "changeset_names:"+stackName, data)
}

// diffCFNResources compares old and new template resource maps and returns
// the list of changes (Add, Modify, Remove).
func diffCFNResources(oldRes, newRes map[string]cfnResource) []CFNResourceChange {
	var changes []CFNResourceChange

	// Added and Modified.
	for logicalID, newR := range newRes {
		oldR, exists := oldRes[logicalID]
		if !exists {
			changes = append(changes, CFNResourceChange{
				Action:       "Add",
				LogicalID:    logicalID,
				ResourceType: newR.Type,
			})
			continue
		}
		if oldR.Type != newR.Type {
			// Type changed → replacement (remove old + add new).
			changes = append(changes, CFNResourceChange{
				Action:       "Remove",
				LogicalID:    logicalID,
				ResourceType: oldR.Type,
			})
			changes = append(changes, CFNResourceChange{
				Action:       "Add",
				LogicalID:    logicalID,
				ResourceType: newR.Type,
			})
			continue
		}
		if !reflect.DeepEqual(oldR.Properties, newR.Properties) {
			changes = append(changes, CFNResourceChange{
				Action:       "Modify",
				LogicalID:    logicalID,
				ResourceType: newR.Type,
				Replacement:  "False",
			})
		}
	}

	// Removed.
	for logicalID, oldR := range oldRes {
		if _, exists := newRes[logicalID]; !exists {
			changes = append(changes, CFNResourceChange{
				Action:       "Remove",
				LogicalID:    logicalID,
				ResourceType: oldR.Type,
			})
		}
	}

	// Sort for deterministic output.
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Action != changes[j].Action {
			return changes[i].Action < changes[j].Action
		}
		return changes[i].LogicalID < changes[j].LogicalID
	})

	return changes
}

// cfnDriftCheckers maps CFN resource types to functions that check whether the
// resource still exists in plugin state. Returns true if the resource exists.
var cfnDriftCheckers = map[string]func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool{
	"AWS::S3::Bucket": func(d *StackDeployer, ctx context.Context, _, _, physicalID string) bool {
		data, _ := d.state.Get(ctx, "s3", "bucket:"+physicalID)
		return data != nil
	},
	"AWS::DynamoDB::Table": func(d *StackDeployer, ctx context.Context, acct, _, physicalID string) bool {
		data, _ := d.state.Get(ctx, "dynamodb", "table:"+acct+"/"+physicalID)
		return data != nil
	},
	"AWS::SQS::Queue": func(d *StackDeployer, ctx context.Context, acct, _, physicalID string) bool {
		// SQS state key is account/queue-name (no region component).
		data, _ := d.state.Get(ctx, "sqs", "queue:"+acct+"/"+physicalID)
		return data != nil
	},
	"AWS::SNS::Topic": func(d *StackDeployer, ctx context.Context, acct, region, physicalID string) bool {
		// PhysicalID is the topic ARN; the SNS state key is name-based.
		data, _ := d.state.Get(ctx, "sns", "topic:"+acct+"/"+region+"/"+snsTopicNameFromPhysicalID(physicalID))
		return data != nil
	},
	"AWS::Lambda::Function": func(d *StackDeployer, ctx context.Context, _, _, physicalID string) bool {
		// Lambda state key is the bare function name (no account/region).
		data, _ := d.state.Get(ctx, "lambda", "function:"+physicalID)
		return data != nil
	},
	"AWS::IAM::Role": func(d *StackDeployer, ctx context.Context, _, _, physicalID string) bool {
		data, _ := d.state.Get(ctx, "iam", "role:"+physicalID)
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
	data, _ := d.state.Get(ctx, "dynamodb", "table:"+d.identity.accountID+"/"+dr.PhysicalID)
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
	data, _ := d.state.Get(ctx, "lambda", "function:"+dr.PhysicalID)
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
	data, _ := d.state.Get(ctx, "iam", "role:"+dr.PhysicalID)
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
		// The template value is a JSON object (or string); unmarshal it the same
		// way the IAM CreateRole handler stores the live document.
		if err := json.Unmarshal([]byte(marshalToJSON(raw)), &expected); err == nil {
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
	data, _ := d.state.Get(ctx, "sqs", "queue:"+d.identity.accountID+"/"+dr.PhysicalID)
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
) (DeployedResource, float64, error) {
	// Drain anything a previous resource left behind, so this resource cannot
	// inherit a reason that is not its own. Nothing should have: Deploy drains
	// after every resource. Outputs are resolved after the last resource and are
	// not attributable to one, so their failures are dropped here deliberately.
	cctx.takeFailures()

	dr, cost, err := d.dispatchResource(ctx, logicalID, res, streamID, cctx)
	failures := cctx.takeFailures()
	if err != nil || len(failures) == 0 {
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

// dispatchResource routes a CFN resource type to its deploy helper.
func (d *StackDeployer) dispatchResource(
	ctx context.Context,
	logicalID string,
	res cfnResource,
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	switch res.Type {
	case "AWS::IAM::Policy":
		return d.deployIAMPolicy(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::IAM::Role":
		return d.deployIAMRole(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::S3::Bucket":
		return d.deployS3Bucket(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Lambda::Function":
		return d.deployLambdaFunction(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SQS::Queue":
		return d.deploySQSQueue(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::DynamoDB::Table":
		return d.deployDynamoDBTable(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::VPC":
		return d.deployEC2VPC(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::Subnet":
		return d.deployEC2Subnet(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::SecurityGroup":
		return d.deployEC2SecurityGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::InternetGateway":
		return d.deployEC2InternetGateway(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::RouteTable":
		return d.deployEC2RouteTable(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::Instance":
		return d.deployEC2Instance(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::TargetGroup":
		return d.deployELBTargetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::LoadBalancer":
		return d.deployELBLoadBalancer(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::Listener":
		return d.deployELBListener(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::ListenerRule":
		return d.deployELBListenerRule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Route53::HostedZone":
		return d.deployRoute53HostedZone(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Route53::RecordSet":
		return d.deployRoute53RecordSet(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Route53::RecordSetGroup":
		return d.deployRoute53RecordSetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KMS::Key":
		return d.deployKMSKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KMS::Alias":
		return d.deployKMSAlias(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KMS::ReplicaKey":
		return d.deployKMSReplicaKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SecretsManager::Secret":
		return d.deploySecret(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SecretsManager::RotationSchedule":
		return d.deploySecretRotationSchedule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SecretsManager::SecretTargetAttachment":
		return d.deploySecretTargetAttachment(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SSM::Parameter":
		return d.deploySSMParameter(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SSM::Association":
		return d.deploySSMAssociation(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SNS::Topic":
		return d.deploySNSTopic(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SNS::Subscription":
		return d.deploySNSSubscription(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SNS::TopicPolicy":
		return d.deploySNSTopicPolicy(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Logs::LogGroup":
		return d.deployLogsLogGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Logs::LogStream":
		return d.deployLogsLogStream(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Events::Rule":
		return d.deployEventsRule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudWatch::Alarm":
		return d.deployCloudWatchAlarm(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.19.0 — API Gateway and ACM.
	case "AWS::CertificateManager::Certificate":
		return d.deployACMCertificate(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::RestApi":
		return d.deployAPIGatewayRestAPI(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Authorizer":
		return d.deployAPIGatewayAuthorizer(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Resource":
		return d.deployAPIGatewayResource(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Method":
		return d.deployAPIGatewayMethod(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Deployment":
		return d.deployAPIGatewayDeployment(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Stage":
		return d.deployAPIGatewayStage(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::ApiKey":
		return d.deployAPIGatewayAPIKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::UsagePlan":
		return d.deployAPIGatewayUsagePlan(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::UsagePlanKey":
		return d.deployAPIGatewayUsagePlanKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Api":
		return d.deployAPIGatewayV2Api(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Route":
		return d.deployAPIGatewayV2Route(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Integration":
		return d.deployAPIGatewayV2Integration(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Stage":
		return d.deployAPIGatewayV2Stage(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Authorizer":
		return d.deployAPIGatewayV2Authorizer(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.20.0 — Step Functions.
	case "AWS::StepFunctions::StateMachine":
		return d.deployStepFunctionsStateMachine(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::StepFunctions::Activity":
		return d.deployStepFunctionsActivity(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.21.0 — ECS and ECR.
	case "AWS::ECR::Repository":
		return d.deployECRRepository(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECR::LifecyclePolicy":
		return d.deployECRLifecyclePolicy(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::Cluster":
		return d.deployECSCluster(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::TaskDefinition":
		return d.deployECSTaskDefinition(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::Service":
		return d.deployECSService(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::CapacityProvider":
		return d.deployECSCapacityProvider(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.22.0 — Cognito.
	case "AWS::Cognito::UserPool":
		return d.deployCognitoUserPool(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::UserPoolClient":
		return d.deployCognitoUserPoolClient(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::UserPoolGroup":
		return d.deployCognitoUserPoolGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::UserPoolDomain":
		return d.deployCognitoUserPoolDomain(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::IdentityPool":
		return d.deployCognitoIdentityPool(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::IdentityPoolRoleAttachment":
		return d.deployCognitoIdentityPoolRoleAttachment(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.23.0 — Kinesis and CloudFront.
	case "AWS::Kinesis::Stream":
		return d.deployKinesisStream(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudFront::Distribution":
		return d.deployCloudFrontDistribution(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudFront::CloudFrontOriginAccessIdentity":
		return d.deployCloudFrontOAI(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.25.0 — RDS and ElastiCache.
	case "AWS::RDS::DBSubnetGroup":
		return d.deployRDSDBSubnetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::RDS::DBParameterGroup":
		return d.deployRDSDBParameterGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::RDS::DBCluster":
		return d.deployRDSDBCluster(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::RDS::DBInstance":
		return d.deployRDSDBInstance(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::SubnetGroup":
		return d.deployElastiCacheSubnetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::ParameterGroup":
		return d.deployElastiCacheParameterGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::CacheCluster":
		return d.deployElastiCacheCacheCluster(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::ReplicationGroup":
		return d.deployElastiCacheReplicationGroup(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.26.0 — EFS and Glue.
	case "AWS::EFS::FileSystem":
		return d.deployEFSFileSystem(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EFS::AccessPoint":
		return d.deployEFSAccessPoint(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EFS::MountTarget":
		return d.deployEFSMountTarget(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Database":
		return d.deployGlueDatabase(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Connection":
		return d.deployGlueConnection(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Table":
		return d.deployGlueTable(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Crawler":
		return d.deployGlueCrawler(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Job":
		return d.deployGlueJob(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.27.0 — Budgets.
	case "AWS::Budgets::Budget":
		return d.deployBudgetsBudget(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.28.0 — SES v2 and Firehose.
	case "AWS::SES::EmailIdentity":
		return d.deploySESv2EmailIdentity(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KinesisFirehose::DeliveryStream":
		return d.deployFirehoseDeliveryStream(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.41.0 — Elastic IPs and NAT Gateways.
	case "AWS::EC2::EIP":
		return d.deployEC2EIP(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::NatGateway":
		return d.deployEC2NatGateway(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.30.0 — Lambda ESM.
	case "AWS::Lambda::EventSourceMapping":
		return d.deployLambdaEventSourceMapping(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.31.0 — AppSync.
	case "AWS::AppSync::GraphQLApi":
		return d.deployAppSyncGraphQLApi(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::AppSync::DataSource":
		return d.deployAppSyncDataSource(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::AppSync::Resolver":
		return d.deployAppSyncResolver(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::AppSync::FunctionConfiguration":
		return d.deployAppSyncFunction(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.43.0 — FSx.
	case "AWS::FSx::FileSystem":
		return d.deployFSxFileSystem(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.34.0 — RDS Aurora cluster and MSK.
	case "AWS::MSK::Cluster":
		return d.deployMSKCluster(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.32.0 — extended CFN stubs.
	case "AWS::OpenSearchService::Domain":
		return d.deployOpenSearchDomain(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::WAFv2::WebACL":
		return d.deployWAFv2WebACL(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Backup::BackupPlan":
		return d.deployBackupBackupPlan(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CodeBuild::Project":
		return d.deployCodeBuildProject(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CodePipeline::Pipeline":
		return d.deployCodePipelinePipeline(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CodeDeploy::DeploymentGroup":
		return d.deployCodeDeployDeploymentGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudTrail::Trail":
		return d.deployCloudTrailTrail(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Config::ConfigRule":
		return d.deployConfigConfigRule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Config::ConfigurationRecorder":
		return d.deployConfigConfigurationRecorder(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Transfer::Server":
		return d.deployTransferServer(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Athena::WorkGroup":
		return d.deployAthenaWorkGroup(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.77.0 — resource types whose API handlers already existed but which fell
	// through to the generic stub (#388).
	case "AWS::EC2::LaunchTemplate":
		return d.deployEC2LaunchTemplate(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::IAM::InstanceProfile":
		return d.deployIAMInstanceProfile(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::SecurityGroupIngress":
		return d.deployEC2SecurityGroupIngress(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::SecurityGroupEgress":
		return d.deployEC2SecurityGroupEgress(ctx, logicalID, res.Properties, streamID, cctx)
	default:
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
}

// deployS3Bucket creates an S3 bucket for the given CFN resource.
func (d *StackDeployer) deployS3Bucket(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	bucketName := strings.ToLower(resolveStringProp(props, "BucketName", logicalID, cctx))

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

	// Apply VersioningConfiguration if present.
	if vc, ok := props["VersioningConfiguration"].(map[string]interface{}); ok {
		if status, _ := vc["Status"].(string); status == "Enabled" {
			vReq := &AWSRequest{
				Service:   "s3",
				Operation: "PUT",
				Path:      "/" + bucketName,
				Params:    map[string]string{"versioning": "1"},
				Headers:   map[string]string{"Content-Type": "application/xml"},
				Body:      []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
			}
			_, _, _ = d.dispatch(ctx, vReq, streamID)
		}
	}

	return dr, cost, nil
}

// deployIAMRole creates an IAM role for the given CFN resource.
func (d *StackDeployer) deployIAMRole(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	roleName := resolveStringProp(props, "RoleName", logicalID, cctx)

	body := map[string]string{
		"RoleName":                 roleName,
		"Path":                     resolveStringProp(props, "Path", "/", cctx),
		"AssumeRolePolicyDocument": marshalToJSON(props["AssumeRolePolicyDocument"]),
		"Description":              resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal role body: %w", err)
	}

	req := &AWSRequest{
		Service:   "iam",
		Operation: "CreateRole",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Role",
		PhysicalID: roleName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ARN string `xml:"CreateRoleResult>Role>Arn"`
		}
		if xmlErr := xmlUnmarshalIAM(resp.Body, &result); xmlErr == nil {
			dr.ARN = result.ARN
		}
	}

	return dr, cost, nil
}

// deployIAMPolicy creates an IAM managed policy for the given CFN resource.
func (d *StackDeployer) deployIAMPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	policyName := resolveStringProp(props, "PolicyName", logicalID, cctx)

	body := map[string]string{
		"PolicyName":     policyName,
		"Path":           resolveStringProp(props, "Path", "/", cctx),
		"PolicyDocument": marshalToJSON(props["PolicyDocument"]),
		"Description":    resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal policy body: %w", err)
	}

	req := &AWSRequest{
		Service:   "iam",
		Operation: "CreatePolicy",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Policy",
		PhysicalID: policyName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ARN string `xml:"CreatePolicyResult>Policy>Arn"`
		}
		if xmlErr := xmlUnmarshalIAM(resp.Body, &result); xmlErr == nil {
			dr.ARN = result.ARN
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
	fnName := resolveStringProp(props, "FunctionName", logicalID, cctx)

	body := map[string]interface{}{
		"FunctionName": fnName,
		"Runtime":      resolveStringProp(props, "Runtime", "python3.12", cctx),
		"Role":         resolveStringProp(props, "Role", "", cctx),
		"Handler":      resolveStringProp(props, "Handler", "index.handler", cctx),
		"Description":  resolveStringProp(props, "Description", "", cctx),
	}
	// Timeout and MemorySize are numeric in the Lambda API; convert the resolved
	// string so the JSON body unmarshals into the int fields (CreateFunction
	// rejects string values otherwise).
	if timeout := resolveStringProp(props, "Timeout", "", cctx); timeout != "" {
		if n, convErr := strconv.Atoi(timeout); convErr == nil {
			body["Timeout"] = n
		}
	}
	if memory := resolveStringProp(props, "MemorySize", "", cctx); memory != "" {
		if n, convErr := strconv.Atoi(memory); convErr == nil {
			body["MemorySize"] = n
		}
	}
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
// does not run an inline package unless Docker is present.
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
	queueName := resolveStringProp(props, "QueueName", logicalID, cctx)

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
	tableName := resolveStringProp(props, "TableName", logicalID, cctx)

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
				TableARN string `json:"TableARN"`
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
	groupName := resolveStringProp(props, "GroupName", logicalID, cctx)
	description := resolveStringProp(props, "GroupDescription", groupName, cctx)
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateSecurityGroup",
		Params: map[string]string{
			"Action":      "CreateSecurityGroup",
			"GroupName":   groupName,
			"Description": description,
			"VpcId":       vpcID,
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
	imageID := resolveStringProp(props, "ImageId", "ami-00000000", cctx)
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
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::LoadBalancer", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "LoadBalancerArn")
	}
	return dr, cost, nil
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
		dr.ARN = extractXMLField(resp.Body, "Id")
		dr.PhysicalID = dr.ARN
	}
	return dr, cost, nil
}

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
			_, cost, err := d.deployRoute53RecordSet(ctx, childID, rsProps, streamID, cctx)
			if err != nil {
				return DeployedResource{}, totalCost, fmt.Errorf("deployRoute53RecordSetGroup item %d: %w", i, err)
			}
			totalCost += cost
		}
	}
	return DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::Route53::RecordSetGroup",
	}, totalCost, nil
}

// deployKMSKey creates a KMS key for the given CFN resource.
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
	if enableKeyRotation, ok := props["EnableKeyRotation"]; ok {
		body["EnableKeyRotation"] = enableKeyRotation
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
	return d.deployKMSKey(ctx, logicalID, props, streamID, cctx)
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

// deploySecretRotationSchedule enables rotation on a Secrets Manager secret.
func (d *StackDeployer) deploySecretRotationSchedule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	secretID := resolveStringProp(props, "SecretId", "", cctx)
	body := map[string]string{"SecretId": secretID}
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
	}
	return dr, cost, nil
}

// deploySSMAssociation is a stub for SSM::Association resources.
func (d *StackDeployer) deploySSMAssociation(
	_ context.Context,
	logicalID string,
	_ map[string]interface{},
	_ string,
	_ *cfnContext,
) (DeployedResource, float64, error) {
	assocID := randomHex(16)
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
	topicName := resolveStringProp(props, "TopicName", logicalID, cctx)
	params := map[string]string{
		"Action": "CreateTopic",
		"Name":   topicName,
	}
	// Forward DisplayName when declared so it round-trips and is drift-checkable.
	if _, declared := props["DisplayName"]; declared {
		if dn := resolveValue(props["DisplayName"], cctx); dn != "" {
			params["DisplayName"] = dn
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
	policy := marshalToJSON(props["PolicyDocument"])
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
	lgName := resolveStringProp(props, "LogGroupName", logicalID, cctx)
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
		body["EventPattern"] = marshalToJSON(ep)
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

// dispatch performs in-process request routing, records the event, and returns
// the response, the estimated cost, and any routing error.
func (d *StackDeployer) dispatch(
	ctx context.Context,
	req *AWSRequest,
	streamID string,
) (*AWSResponse, float64, error) {
	reqCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: d.identity.accountID,
		Region:    d.identity.region,
		Timestamp: d.tc.Now(),
		Metadata:  map[string]interface{}{"stream_id": streamID},
	}

	start := d.tc.Now()
	resp, routeErr := d.registry.RouteRequest(reqCtx, req)
	duration := time.Since(start)
	cost := d.costs.CostForRequest(req)

	_ = d.store.RecordRequest(ctx, reqCtx, req, resp, duration, cost, routeErr)

	return resp, cost, cfnDispatchError(resp, routeErr)
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
	// Fallback: JSON-encode.
	b, _ := json.Marshal(v)
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
	// Deployed resource Ref (physical ID).
	if dr, ok := cctx.resources[ref]; ok {
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
		return ""
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
		switch attr {
		case "Arn", "KeyArn":
			if dr.ARN != "" {
				return dr.ARN
			}
			return dr.PhysicalID
		case "TopicArn":
			// AWS::SNS::Topic GetAtt TopicArn returns the ARN.
			if dr.ARN != "" {
				return dr.ARN
			}
			return dr.PhysicalID
		case "Value":
			// AWS::SSM::Parameter GetAtt Value — physical ID is the parameter name.
			return dr.PhysicalID
		case "RootResourceId":
			// AWS::ApiGateway::RestApi GetAtt RootResourceId — stored as extra in PhysicalID with prefix.
			if strings.HasPrefix(dr.PhysicalID, "root:") {
				return strings.TrimPrefix(dr.PhysicalID, "root:")
			}
			// Fallback: the metadata map stores it separately.
			if v, ok := dr.Metadata["RootResourceId"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "InvokeURL":
			// AWS::ApiGateway::Stage GetAtt InvokeURL.
			if v, ok := dr.Metadata["InvokeURL"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "Name":
			// AWS::StepFunctions::StateMachine GetAtt Name.
			if v, ok := dr.Metadata["Name"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "RepositoryUri":
			// AWS::ECR::Repository GetAtt RepositoryUri.
			if v, ok := dr.Metadata["RepositoryUri"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.ARN
		case "ProviderName":
			// AWS::Cognito::UserPool GetAtt ProviderName.
			if v, ok := dr.Metadata["ProviderName"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "ProviderURL":
			// AWS::Cognito::UserPool GetAtt ProviderURL.
			if v, ok := dr.Metadata["ProviderURL"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "DomainName":
			// AWS::CloudFront::Distribution GetAtt DomainName.
			if v, ok := dr.Metadata["DomainName"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "StreamArn":
			// AWS::Kinesis::Stream GetAtt StreamArn.
			if dr.ARN != "" {
				return dr.ARN
			}
			return dr.PhysicalID
		case "Endpoint.Address", "Endpoint.Port",
			"ConfigurationEndpoint.Address", "ConfigurationEndpoint.Port",
			"RedisEndPoint.Address", "RedisEndPoint.Port",
			"PrimaryEndPoint.Address", "PrimaryEndPoint.Port":
			// AWS::RDS::DBInstance and AWS::ElastiCache::* endpoint GetAtts.
			if v, ok := dr.Metadata[attr]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		default:
			return dr.PhysicalID
		}
	}
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

	if d.state != nil && props != nil {
		data, err := json.Marshal(props)
		if err == nil {
			key := fmt.Sprintf("%s/%s/%s", cctx.accountID, cctx.region, logicalID)
			_ = d.state.Put(ctx, cfnStubNamespace, key, data)
		}
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

// xmlUnmarshalIAM strips the IAM namespace from body and unmarshals into dst.
// Used to parse XML responses from IAMPlugin (which uses the iam.amazonaws.com namespace).
func xmlUnmarshalIAM(body []byte, dst interface{}) error {
	body = bytes.ReplaceAll(body, []byte(` xmlns="`+iamXMLNS+`"`), nil)
	return xml.Unmarshal(body, dst)
}

// marshalToJSON marshals v to a JSON string. Returns "" on nil or error.
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
