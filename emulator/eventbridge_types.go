package emulator

import "time"

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

// ebRuleKeyPrefix is the colon-terminated prefix every rule record is stored under; see [ebRuleRef.key].
// The colon matters because "ebrule" is also a prefix of "ebrule_names", the rule-name index.
const ebRuleKeyPrefix = "ebrule:"

// ebDefaultBus is the name of the event bus every account has in every Region.
const ebDefaultBus = "default"

// ebRuleARN constructs the ARN for an EventBridge rule on bus: rule/{name} on the default bus and
// rule/{bus}/{name} on a custom one, the two forms EventBridge mints.
func ebRuleARN(region, accountID, bus, ruleName string) string {
	if bus == "" || bus == ebDefaultBus {
		return "arn:aws:events:" + region + ":" + accountID + ":rule/" + ruleName
	}
	return "arn:aws:events:" + region + ":" + accountID + ":rule/" + bus + "/" + ruleName
}

// ebBusARN constructs the ARN of the event bus named busName in an account and Region.
func ebBusARN(region, accountID, busName string) string {
	return "arn:aws:events:" + region + ":" + accountID + ":event-bus/" + busName
}

// EBEventBus is an emulated EventBridge event bus: a custom bus CreateEventBus made, or the default
// bus once a write (PutPermission, TagResource) has given it state of its own. An unwritten default
// bus has no record and is synthesized on read ([EventBridgePlugin.loadBus]).
type EBEventBus struct {
	// Name is the bus name.
	Name string `json:"Name"`

	// ARN is the bus ARN, arn:aws:events:{region}:{account}:event-bus/{name}.
	ARN string `json:"Arn"`

	// Description is the bus description, at most 512 characters.
	Description string `json:"Description,omitempty"`

	// KmsKeyIdentifier is the customer managed key the bus encrypts events with, as given.
	KmsKeyIdentifier string `json:"KmsKeyIdentifier,omitempty"`

	// DeadLetterConfig names the SQS queue EventBridge uses as the bus's dead-letter queue.
	DeadLetterConfig *EBDeadLetterConfig `json:"DeadLetterConfig,omitempty"`

	// LogConfig is the bus's logging configuration, as given.
	LogConfig *EBLogConfig `json:"LogConfig,omitempty"`

	// Policy is the bus's resource policy JSON, written by PutPermission and RemovePermission.
	Policy string `json:"Policy,omitempty"`

	// Tags are the bus's tags, sorted by key. No bus response carries them; ListTagsForResource is
	// their read path.
	Tags []EBTag `json:"Tags,omitempty"`

	// CreatedAt is when CreateEventBus made the bus, on the simulated clock. It is zero for the
	// default bus, whose creation predates anything substrate observed.
	CreatedAt time.Time `json:"created_at,omitzero"`

	// UpdatedAt is when the bus was last written, on the simulated clock.
	UpdatedAt time.Time `json:"updated_at,omitzero"`

	// EverTagged records that this bus has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// EBDeadLetterConfig is EventBridge's DeadLetterConfig type.
type EBDeadLetterConfig struct {
	// Arn is the ARN of the SQS queue used as the dead-letter queue.
	Arn string `json:"Arn,omitempty"`
}

// EBLogConfig is EventBridge's LogConfig type.
type EBLogConfig struct {
	// IncludeDetail is FULL or NONE.
	IncludeDetail string `json:"IncludeDetail,omitempty"`

	// Level is OFF, ERROR, INFO or TRACE.
	Level string `json:"Level,omitempty"`
}

// ebEventBusWire is the EventBus shape ListEventBuses renders. CreationTime and LastModifiedTime
// are absent for the default bus until a write gives it a record.
type ebEventBusWire struct {
	Name             string        `json:"Name"`
	ARN              string        `json:"Arn"`
	Description      string        `json:"Description,omitempty"`
	Policy           string        `json:"Policy,omitempty"`
	CreationTime     *EpochSeconds `json:"CreationTime,omitempty"`
	LastModifiedTime *EpochSeconds `json:"LastModifiedTime,omitempty"`
}

// ebDescribeEventBusWire is DescribeEventBus's response: the EventBus members plus the
// configuration ListEventBuses does not publish.
type ebDescribeEventBusWire struct {
	ebEventBusWire
	KmsKeyIdentifier string              `json:"KmsKeyIdentifier,omitempty"`
	DeadLetterConfig *EBDeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	LogConfig        *EBLogConfig        `json:"LogConfig,omitempty"`
}

// ebBusToWire projects a stored bus onto the EventBus shape.
func ebBusToWire(b *EBEventBus) ebEventBusWire {
	w := ebEventBusWire{Name: b.Name, ARN: b.ARN, Description: b.Description, Policy: b.Policy}
	if !b.CreatedAt.IsZero() {
		c := EpochSeconds(b.CreatedAt)
		w.CreationTime = &c
	}
	if !b.UpdatedAt.IsZero() {
		u := EpochSeconds(b.UpdatedAt)
		w.LastModifiedTime = &u
	}
	return w
}
