package emulator

// eventbridgeNamespace is the state namespace used by EventBridgePlugin.
const eventbridgeNamespace = "eventbridge"

// EBRule represents an emulated Amazon EventBridge rule.
type EBRule struct {
	// Name is the name of the rule.
	Name string `json:"Name"`

	// ARN is the Amazon Resource Name for the rule. EventBridge publishes it as Arn
	// on the Rule type, which DescribeRule and ListRules render; PutRule wraps the
	// same value under RuleArn.
	ARN string `json:"Arn,omitempty"`

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

	// RoleARN is "the Amazon Resource Name (ARN) of the IAM role associated with the rule", as PutRule
	// was given it. DescribeRule and ListRules report it (#1607).
	RoleARN string `json:"RoleArn,omitempty"`

	// CreatedBy is "the account ID of the user that created the rule", which DescribeRule reports and
	// ListRules does not. It is stamped by the PutRule that creates the rule and kept by one that
	// updates it.
	CreatedBy string `json:"CreatedBy,omitempty"`

	// Tags are the rule's tags, sorted by key (#1607). They are written by PutRule's Tags when it
	// creates the rule — "if you are updating an existing rule, any tags you specify in the PutRule
	// operation are ignored" — and by TagResource and UntagResource, and read by ListTagsForResource.
	// No rule response carries them; see [ebRuleWire].
	Tags []EBTag `json:"Tags,omitempty"`

	// EverTagged records that this rule has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// EBTag is a key-value tag on an EventBridge resource, EventBridge's Tag type.
type EBTag struct {
	// Key is the tag key, 1 to 128 characters.
	Key string `json:"Key"`

	// Value is the tag value, 0 to 256 characters.
	Value string `json:"Value"`
}

// ebRuleWire is the Rule shape ListRules renders: every member of [EBRule] that ListRules' Rule type
// publishes, and none of the record's tags or bookkeeping. ManagedBy is published and not modeled:
// substrate has no rules created by another service.
type ebRuleWire struct {
	Name               string `json:"Name"`
	ARN                string `json:"Arn,omitempty"`
	EventPattern       string `json:"EventPattern,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	State              string `json:"State"`
	Description        string `json:"Description,omitempty"`
	EventBusName       string `json:"EventBusName,omitempty"`
	RoleARN            string `json:"RoleArn,omitempty"`
}

// ebDescribeRuleWire is DescribeRule's response: the Rule members plus CreatedBy, which only
// DescribeRule publishes.
type ebDescribeRuleWire struct {
	ebRuleWire
	CreatedBy string `json:"CreatedBy,omitempty"`
}

// ebRuleToWire projects a stored rule onto ListRules' Rule shape.
func ebRuleToWire(r EBRule) ebRuleWire {
	return ebRuleWire{
		Name:               r.Name,
		ARN:                r.ARN,
		EventPattern:       r.EventPattern,
		ScheduleExpression: r.ScheduleExpression,
		State:              r.State,
		Description:        r.Description,
		EventBusName:       r.EventBusName,
		RoleARN:            r.RoleARN,
	}
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

// ebRuleKeyPrefix is the colon-terminated prefix every rule record is stored under; see [ebRuleKey].
// The colon matters because "ebrule" is also a prefix of "ebrule_names", the rule-name index.
const ebRuleKeyPrefix = "ebrule:"

// ebRuleARN constructs the ARN for an EventBridge rule.
func ebRuleARN(region, accountID, ruleName string) string {
	return "arn:aws:events:" + region + ":" + accountID + ":rule/" + ruleName
}

// ebEventBusARN constructs the ARN for an event bus in the caller's account and
// Region. A nil context yields the parser's defaults, so a plugin driven directly
// by a unit test gets the same ARN an HTTP caller would.
func ebEventBusARN(reqCtx *RequestContext, busName string) string {
	region, accountID := defaultRegion, defaultAccountID
	if reqCtx != nil {
		if reqCtx.Region != "" {
			region = reqCtx.Region
		}
		if reqCtx.AccountID != "" {
			accountID = reqCtx.AccountID
		}
	}
	return "arn:aws:events:" + region + ":" + accountID + ":event-bus/" + busName
}
