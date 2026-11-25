package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// appAutoScalingNamespace is the Application Auto Scaling plugin's registry name and its state
// namespace. It is the IAM action prefix and the ARN service segment of a scalable target, so the
// tagging API's owner lookup ([taggingARNOwner]) reaches the plugin by the ARN alone.
const appAutoScalingNamespace = "application-autoscaling"

// aasTargetKeyPrefix begins every scalable target's state key.
const aasTargetKeyPrefix = "target:"

// aasTarget is one registered scalable target, the unit Application Auto Scaling identifies by
// service namespace, scalable dimension and resource ID. Its scaling policies and scheduled actions
// are held on the record, because "Deregistering a scalable target deletes the scaling policies and
// the scheduled actions that are associated with it" (API_DeregisterScalableTarget): they have no
// existence apart from it.
//
// The record is persisted state. Responses are rendered from the wire types in
// appautoscaling_wire.go, so EverTagged never reaches a caller.
type aasTarget struct {
	ServiceNamespace  string             `json:"ServiceNamespace"`
	ResourceID        string             `json:"ResourceId"`
	ScalableDimension string             `json:"ScalableDimension"`
	MinCapacity       int32              `json:"MinCapacity"`
	MaxCapacity       int32              `json:"MaxCapacity"`
	RoleARN           string             `json:"RoleARN"`
	SuspendedState    *aasSuspendedState `json:"SuspendedState,omitempty"`
	ScalableTargetARN string             `json:"ScalableTargetARN"`
	CreationTime      time.Time          `json:"CreationTime"`
	Tags              map[string]string  `json:"Tags,omitempty"`
	// EverTagged is the previously-tagged flag the tagging API's GetResources reads (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`

	Policies         []aasPolicy          `json:"Policies,omitempty"`
	ScheduledActions []aasScheduledAction `json:"ScheduledActions,omitempty"`
}

// aasSuspendedState is the SuspendedState structure, three flags that are false by default.
type aasSuspendedState struct {
	DynamicScalingInSuspended  bool `json:"DynamicScalingInSuspended"`
	DynamicScalingOutSuspended bool `json:"DynamicScalingOutSuspended"`
	ScheduledScalingSuspended  bool `json:"ScheduledScalingSuspended"`
}

// aasPolicy is one scaling policy on a target. The three configurations are kept as the caller
// sent them, compacted, and answered back by DescribeScalingPolicies: the policy is recorded intent,
// and substrate evaluates none of it.
type aasPolicy struct {
	PolicyARN    string          `json:"PolicyARN"`
	PolicyName   string          `json:"PolicyName"`
	PolicyType   string          `json:"PolicyType"`
	StepConfig   json.RawMessage `json:"StepScalingPolicyConfiguration,omitempty"`
	TargetConfig json.RawMessage `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	Predictive   json.RawMessage `json:"PredictiveScalingPolicyConfiguration,omitempty"`
	Alarms       []aasAlarm      `json:"Alarms,omitempty"`
	CreationTime time.Time       `json:"CreationTime"`
}

// aasAlarm is the Alarm structure: a CloudWatch alarm a target tracking policy created.
type aasAlarm struct {
	AlarmName string `json:"AlarmName"`
	AlarmARN  string `json:"AlarmARN"`
}

// aasScheduledAction is one scheduled action on a target. It is recorded intent: substrate runs no
// schedule, so an action never changes the target's capacity (the scope rule).
type aasScheduledAction struct {
	ScheduledActionName  string                   `json:"ScheduledActionName"`
	ScheduledActionARN   string                   `json:"ScheduledActionARN"`
	Schedule             string                   `json:"Schedule"`
	Timezone             string                   `json:"Timezone,omitempty"`
	StartTime            *time.Time               `json:"StartTime,omitempty"`
	EndTime              *time.Time               `json:"EndTime,omitempty"`
	ScalableTargetAction *aasScalableTargetAction `json:"ScalableTargetAction,omitempty"`
	CreationTime         time.Time                `json:"CreationTime"`
}

// aasScalableTargetAction is the ScalableTargetAction structure.
type aasScalableTargetAction struct {
	MinCapacity *int32 `json:"MinCapacity,omitempty"`
	MaxCapacity *int32 `json:"MaxCapacity,omitempty"`
}

// aasTargetKey is the state key of the target namespace/dimension/resourceID names in an account
// and Region. The resource ID is last, because it is the only component that may itself hold the
// separator; the namespace and the dimension are enum values that hold none.
func aasTargetKey(accountID, region, namespace, dimension, resourceID string) string {
	return aasTargetKeyPrefix + accountID + "/" + region + "/" + namespace + "|" + dimension + "|" + resourceID
}

// aasNamespacePrefix is the key prefix of every target in one service namespace.
func aasNamespacePrefix(accountID, region, namespace string) string {
	return aasTargetKeyPrefix + accountID + "/" + region + "/" + namespace + "|"
}

// aasServiceLinkedRoles maps each service namespace to the role name Application Auto Scaling
// gives its service-linked role, from the user guide's "Service-linked role ARN reference" table.
// EMR has no service-linked role; RegisterScalableTarget "is required for services that do not
// support service-linked roles (such as Amazon EMR)".
var aasServiceLinkedRoles = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	"appstream":       "AWSServiceRoleForApplicationAutoScaling_AppStreamFleet",
	"rds":             "AWSServiceRoleForApplicationAutoScaling_RDSCluster",
	"comprehend":      "AWSServiceRoleForApplicationAutoScaling_ComprehendEndpoint",
	"dynamodb":        "AWSServiceRoleForApplicationAutoScaling_DynamoDBTable",
	"ecs":             "AWSServiceRoleForApplicationAutoScaling_ECSService",
	"elasticache":     "AWSServiceRoleForApplicationAutoScaling_ElastiCacheRG",
	"cassandra":       "AWSServiceRoleForApplicationAutoScaling_CassandraTable",
	"lambda":          "AWSServiceRoleForApplicationAutoScaling_LambdaConcurrency",
	"kafka":           "AWSServiceRoleForApplicationAutoScaling_KafkaCluster",
	"neptune":         "AWSServiceRoleForApplicationAutoScaling_NeptuneCluster",
	"sagemaker":       "AWSServiceRoleForApplicationAutoScaling_SageMakerEndpoint",
	"ec2":             "AWSServiceRoleForApplicationAutoScaling_EC2SpotFleetRequest",
	"workspaces":      "AWSServiceRoleForApplicationAutoScaling_WorkSpacesPool",
	"custom-resource": "AWSServiceRoleForApplicationAutoScaling_CustomResource",
}

// aasServiceLinkedRoleARN returns the service-linked role a target in namespace uses when the
// caller names no RoleARN, or "" for a namespace that has none.
func aasServiceLinkedRoleARN(accountID, namespace string) string {
	role, ok := aasServiceLinkedRoles[namespace]
	if !ok {
		return ""
	}
	return "arn:aws:iam::" + accountID + ":role/aws-service-role/" + namespace +
		".application-autoscaling.amazonaws.com/" + role
}

// aasDimensionResource names, for each scalable dimension, the published form of the resource ID
// it scales: a fixed prefix the ID must begin with, and for two dimensions a segment it must or
// must not contain. The forms are the ResourceId member's list in API_RegisterScalableTarget. A
// custom resource's ID is "defined by the service provider", so it is not checked.
type aasResourceForm struct {
	prefix     string
	contains   string
	notContain string
}

// aasDimensions maps every scalable dimension in the model's enum to its resource ID form.
var aasDimensions = map[string]aasResourceForm{ //nolint:gochecknoglobals // immutable lookup table
	"ecs:service:DesiredCount":                                      {prefix: "service/"},
	"ec2:spot-fleet-request:TargetCapacity":                         {prefix: "spot-fleet-request/"},
	"elasticmapreduce:instancegroup:InstanceCount":                  {prefix: "instancegroup/"},
	"appstream:fleet:DesiredCapacity":                               {prefix: "fleet/"},
	"dynamodb:table:ReadCapacityUnits":                              {prefix: "table/", notContain: "/index/"},
	"dynamodb:table:WriteCapacityUnits":                             {prefix: "table/", notContain: "/index/"},
	"dynamodb:index:ReadCapacityUnits":                              {prefix: "table/", contains: "/index/"},
	"dynamodb:index:WriteCapacityUnits":                             {prefix: "table/", contains: "/index/"},
	"rds:cluster:ReadReplicaCount":                                  {prefix: "cluster:"},
	"sagemaker:variant:DesiredInstanceCount":                        {prefix: "endpoint/", contains: "/variant/"},
	"custom-resource:ResourceType:Property":                         {},
	"comprehend:document-classifier-endpoint:DesiredInferenceUnits": {prefix: "arn:"},
	"comprehend:entity-recognizer-endpoint:DesiredInferenceUnits":   {prefix: "arn:"},
	"lambda:function:ProvisionedConcurrency":                        {prefix: "function:"},
	"cassandra:table:ReadCapacityUnits":                             {prefix: "keyspace/", contains: "/table/"},
	"cassandra:table:WriteCapacityUnits":                            {prefix: "keyspace/", contains: "/table/"},
	"kafka:broker-storage:VolumeSize":                               {prefix: "arn:"},
	"elasticache:cache-cluster:Nodes":                               {prefix: "cache-cluster/"},
	"elasticache:replication-group:NodeGroups":                      {prefix: "replication-group/"},
	"elasticache:replication-group:Replicas":                        {prefix: "replication-group/"},
	"neptune:cluster:ReadReplicaCount":                              {prefix: "cluster:"},
	"sagemaker:variant:DesiredProvisionedConcurrency":               {prefix: "endpoint/", contains: "/variant/"},
	"sagemaker:inference-component:DesiredCopyCount":                {prefix: "inference-component/"},
	"workspaces:workspacespool:DesiredUserSessions":                 {prefix: "workspacespool/"},
}

// aasStepUnsupported lists the namespaces whose targets take no step scaling policy: "StepScaling—Not
// supported for DynamoDB, Amazon Comprehend, Lambda, Amazon Keyspaces, Amazon MSK, Amazon
// ElastiCache, or Neptune" (API_PutScalingPolicy, PolicyType).
var aasStepUnsupported = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"dynamodb": true, "comprehend": true, "lambda": true, "cassandra": true,
	"kafka": true, "elasticache": true, "neptune": true,
}

// aasCheckTarget validates the three members that identify a target: the namespace is one the model
// lists, the dimension is one the model lists and belongs to the namespace, and the resource ID has
// the dimension's published form. Every refusal is a ValidationException; the messages are
// substrate's own.
func aasCheckTarget(namespace, dimension, resourceID string) *AWSError {
	if namespace == "" {
		return aasValidation("ServiceNamespace is required.")
	}
	if _, ok := aasServiceLinkedRoles[namespace]; !ok && namespace != "elasticmapreduce" {
		return aasValidation(fmt.Sprintf("Unsupported service namespace: %s.", namespace))
	}
	if dimension == "" {
		return aasValidation("ScalableDimension is required.")
	}
	form, ok := aasDimensions[dimension]
	if !ok {
		return aasValidation(fmt.Sprintf("Unsupported scalable dimension: %s.", dimension))
	}
	if !strings.HasPrefix(dimension, namespace+":") {
		return aasValidation(fmt.Sprintf(
			"Scalable dimension %s does not belong to service namespace %s.", dimension, namespace))
	}
	if resourceID == "" {
		return aasValidation("ResourceId is required.")
	}
	if len(resourceID) > 1600 {
		return aasValidation("ResourceId must have length less than or equal to 1600.")
	}
	if !strings.HasPrefix(resourceID, form.prefix) ||
		(form.contains != "" && !strings.Contains(resourceID, form.contains)) ||
		(form.notContain != "" && strings.Contains(resourceID, form.notContain)) {
		return aasValidation(fmt.Sprintf(
			"Unsupported resource ID %s for scalable dimension %s.", resourceID, dimension))
	}
	return nil
}

// aasValidation builds a ValidationException, which every Application Auto Scaling operation
// publishes at HTTP 400 ("An exception was thrown for a validation issue").
func aasValidation(msg string) *AWSError {
	return &AWSError{Code: "ValidationException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// aasTargetNotFound builds the ObjectNotFoundException an operation on an unregistered target
// answers. The message is AWS's observed wording for PutScalingPolicy, quoted from an AWS CLI
// refusal ("No scalable target registered for service namespace: dynamodb, resource ID:
// table/tableName, scalable dimension: dynamodb:table:ReadCapacityUnits", Stack Overflow 50139305
// and AWS re:Post QU3KEw3laKSvqYtjlnS8Nxwg); substrate answers it for every operation that needs a
// registered target.
func aasTargetNotFound(namespace, resourceID, dimension string) *AWSError {
	return &AWSError{
		Code: "ObjectNotFoundException",
		Message: fmt.Sprintf("No scalable target registered for service namespace: %s, resource ID: %s, scalable dimension: %s",
			namespace, resourceID, dimension),
		HTTPStatus: http.StatusBadRequest,
	}
}

// aasObjectNotFound builds an ObjectNotFoundException with substrate's own message, for a policy or
// scheduled action that a delete names and the target does not hold.
func aasObjectNotFound(msg string) *AWSError {
	return &AWSError{Code: "ObjectNotFoundException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// aasResourceNotFound builds the ResourceNotFoundException the three tag operations answer for an
// ARN that names no scalable target. The message is substrate's own.
func aasResourceNotFound(arn string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    fmt.Sprintf("Scalable target with ARN %s not found.", arn),
		HTTPStatus: http.StatusBadRequest,
	}
}

// aasInvalidNextToken builds the InvalidNextTokenException the four Describe operations publish:
// "The next token supplied was invalid." That sentence is the shape's documentation, and is used as
// the message.
func aasInvalidNextToken() *AWSError {
	return &AWSError{
		Code: "InvalidNextTokenException", Message: "The next token supplied was invalid.",
		HTTPStatus: http.StatusBadRequest,
	}
}
