package substrate

// eventbridgeNamespace is the state namespace used by EventBridgePlugin.
const eventbridgeNamespace = "eventbridge"

// EBRule represents an emulated Amazon EventBridge rule.
type EBRule struct {
	// Name is the name of the rule.
	Name string `json:"Name"`

	// ARN is the Amazon Resource Name for the rule.
	ARN string `json:"ARN,omitempty"`

	// EventPattern is the JSON event pattern the rule matches.
	EventPattern string `json:"EventPattern,omitempty"`

	// ScheduleExpression is the schedule expression (e.g., "rate(5 minutes)").
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`

	// State is the rule state: "ENABLED" or "DISABLED".
	State string `json:"State"`

	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`

	// EventBusName is the name of the event bus the rule applies to.
	EventBusName string `json:"EventBusName,omitempty"`
}

// EBTarget represents a target for an EventBridge rule.
type EBTarget struct {
	// Id is the unique identifier for the target.
	Id string `json:"Id"` //nolint:revive

	// ARN is the Amazon Resource Name of the target resource.
	ARN string `json:"Arn"`

	// RoleARN is the ARN of an IAM role to use for the target.
	RoleARN string `json:"RoleArn,omitempty"`
}

// EBEvent represents a stored EventBridge event.
type EBEvent struct {
	// Source is the event source (e.g., "com.example.myapp").
	Source string `json:"Source"`

	// DetailType is the detail type of the event.
	DetailType string `json:"DetailType"`

	// Detail is the event detail as a JSON string.
	Detail string `json:"Detail"`

	// EventBusName is the name of the event bus.
	EventBusName string `json:"EventBusName,omitempty"`

	// Time is the event timestamp in milliseconds since epoch.
	Time int64 `json:"Time"`

	// EventID is the unique identifier assigned to the event.
	EventID string `json:"EventId"`
}

// ebRuleARN constructs the ARN for an EventBridge rule.
func ebRuleARN(region, accountID, ruleName string) string {
	return "arn:aws:events:" + region + ":" + accountID + ":rule/" + ruleName
}
