package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// AWS Config state, keys, ARNs and error helpers (#580).
//
// Everything here is prefixed cfgsvc/configService rather than config, because
// emulator/config.go already owns Config, ServerConfig, DefaultConfig and
// LoadConfig — substrate's own configuration, unrelated to the AWS service. Only
// the namespace *value* and ConfigServicePlugin.Name() are "config", because those
// are what the request parser resolves a Config request to.

// configServiceNamespace is the state namespace for AWS Config.
const configServiceNamespace = "config"

// Recorder status values, the RecorderStatus enum from the config/2014-11-12 API
// model. A recorder's lastStatus is one of these; substrate reports Pending for a
// recorder that has never been started and Success once it has, and a seed can pin
// any of the four.
const (
	// cfgsvcRecorderStatusPending is a recorder that has not yet delivered anything.
	cfgsvcRecorderStatusPending = "Pending"

	// cfgsvcRecorderStatusSuccess is a recorder whose last delivery succeeded.
	cfgsvcRecorderStatusSuccess = "Success"

	// cfgsvcRecorderStatusFailure is a recorder whose last delivery failed.
	cfgsvcRecorderStatusFailure = "Failure"

	// cfgsvcRecorderStatusNotApplicable is a recorder for which delivery does not apply.
	cfgsvcRecorderStatusNotApplicable = "NotApplicable"
)

// cfgsvcRecorderStatuses is the RecorderStatus enum, used to validate a seeded
// status. A value outside it would be reported as a string no SDK enum member
// matches, so a caller's switch would fall through to its default and the test
// would pass while asserting nothing.
var cfgsvcRecorderStatuses = []string{
	cfgsvcRecorderStatusPending,
	cfgsvcRecorderStatusSuccess,
	cfgsvcRecorderStatusFailure,
	cfgsvcRecorderStatusNotApplicable,
}

// Recording strategies, the RecordingStrategyType enum.
const (
	// cfgsvcStrategyAllSupported records every supported resource type.
	cfgsvcStrategyAllSupported = "ALL_SUPPORTED_RESOURCE_TYPES"

	// cfgsvcStrategyInclusion records only the types in resourceTypes.
	cfgsvcStrategyInclusion = "INCLUSION_BY_RESOURCE_TYPES"

	// cfgsvcStrategyExclusion records every supported type except those excluded.
	cfgsvcStrategyExclusion = "EXCLUSION_BY_RESOURCE_TYPES"
)

// cfgsvcRecordingStrategies is the RecordingStrategyType enum, used to refuse a
// useOnly value the service would not accept.
var cfgsvcRecordingStrategies = []string{
	cfgsvcStrategyAllSupported,
	cfgsvcStrategyInclusion,
	cfgsvcStrategyExclusion,
}

// cfgsvcServiceLinkedNamePrefix is the recorder-name prefix AWS reserves for
// service-linked configuration recorders. PutConfigurationRecorder refuses a name
// carrying it with InvalidConfigurationRecorderNameException.
const cfgsvcServiceLinkedNamePrefix = "AWSConfigurationRecorderFor"

// cfgsvcDefaultName is the name both PutConfigurationRecorder and
// PutDeliveryChannel use when the request omits one — the API reference states for
// each that "If you do not specify a name, the default is used".
const cfgsvcDefaultName = "default"

// cfgsvcMaxNameLen is the RecorderName/ChannelName ceiling (both 1-256).
const cfgsvcMaxNameLen = 256

// ConfigRecorder is a stored configuration recorder — the ConfigurationRecorder
// shape, plus the tags a Put set at creation.
//
// The JSON tags are the wire names, which are lowerCamelCase for this shape
// (name, roleARN, recordingGroup) while every other Config shape is UpperCamel.
// That asymmetry is the API model's, not substrate's.
type ConfigRecorder struct {
	// ARN is the recorder's Amazon Resource Name.
	ARN string `json:"arn,omitempty"`

	// Name is the recorder name, "default" when the request omitted one.
	Name string `json:"name"`

	// RoleARN is the IAM role Config assumes. Required in practice — see
	// cfgsvcCheckRoleARN.
	RoleARN string `json:"roleARN,omitempty"`

	// RecordingGroup is the set of resource types being recorded.
	RecordingGroup *ConfigRecordingGroup `json:"recordingGroup,omitempty"`

	// RecordingMode is the recording frequency and its per-type overrides.
	RecordingMode json.RawMessage `json:"recordingMode,omitempty"`

	// RecordingScope is INTERNAL or PAID.
	RecordingScope string `json:"recordingScope,omitempty"`

	// Tags are the tags set when the recorder was created. A later Put does not
	// replace them: the API reference states tags "are added at creation and are not
	// updated with configuration recorder updates".
	Tags map[string]string `json:"-"`
}

// ConfigRecordingGroup is the RecordingGroup shape: which resource types a
// recorder records.
type ConfigRecordingGroup struct {
	// AllSupported records every supported type except the global IAM ones.
	AllSupported bool `json:"allSupported"`

	// IncludeGlobalResourceTypes adds the global IAM types back in.
	IncludeGlobalResourceTypes bool `json:"includeGlobalResourceTypes"`

	// ResourceTypes is the explicit inclusion list.
	ResourceTypes []string `json:"resourceTypes,omitempty"`

	// ExclusionByResourceTypes is the exclusion list.
	ExclusionByResourceTypes *ConfigExclusionByResourceTypes `json:"exclusionByResourceTypes,omitempty"`

	// RecordingStrategy names which of the three strategies is in force.
	RecordingStrategy *ConfigRecordingStrategy `json:"recordingStrategy,omitempty"`
}

// ConfigExclusionByResourceTypes is the ExclusionByResourceTypes shape.
type ConfigExclusionByResourceTypes struct {
	// ResourceTypes are the types excluded from recording.
	ResourceTypes []string `json:"resourceTypes,omitempty"`
}

// ConfigRecordingStrategy is the RecordingStrategy shape.
type ConfigRecordingStrategy struct {
	// UseOnly is a RecordingStrategyType enum member.
	UseOnly string `json:"useOnly,omitempty"`
}

// ConfigRecorderStatus is the ConfigurationRecorderStatus shape: whether a
// recorder is recording, and how its last delivery went.
//
// It is stored separately from ConfigRecorder because the two answer different
// questions and only the second is what "is this account covered" turns on. A
// recorder can exist with Recording false, which is the misconfiguration #580's
// behavior #1 exists to make testable.
type ConfigRecorderStatus struct {
	// ARN is the recorder's ARN, repeated here because the status shape carries it.
	ARN string `json:"arn,omitempty"`

	// Name is the recorder name.
	Name string `json:"name"`

	// LastStartTime is when the recorder was last started, zero if never.
	LastStartTime EpochSeconds `json:"lastStartTime,omitempty"`

	// LastStopTime is when the recorder was last stopped, zero if never.
	LastStopTime EpochSeconds `json:"lastStopTime,omitempty"`

	// Recording reports whether the recorder is recording. This is the field
	// DescribeConfigurationRecorders cannot answer.
	Recording bool `json:"recording"`

	// LastStatus is a RecorderStatus enum member.
	LastStatus string `json:"lastStatus,omitempty"`

	// LastErrorCode is the code of the last delivery failure, if any.
	LastErrorCode string `json:"lastErrorCode,omitempty"`

	// LastErrorMessage is the message of the last delivery failure, if any.
	LastErrorMessage string `json:"lastErrorMessage,omitempty"`

	// LastStatusChangeTime is when LastStatus last changed.
	LastStatusChangeTime EpochSeconds `json:"lastStatusChangeTime,omitempty"`
}

// --- state keys ---
//
// Every key is scoped by account *and* Region, because Config is regional and
// "recording in one Region only" is a common real misconfiguration (#580 behavior
// #7). A key that dropped the Region would make a recorder created in us-east-1
// visible in eu-west-1, which is the opposite of the thing under test.

// cfgsvcRecorderKey holds the single configuration recorder for an account and
// Region. AWS permits one per account per Region, so this is keyed by neither a
// name nor an ID: a second recorder under a different name is refused rather than
// stored.
func cfgsvcRecorderKey(accountID, region string) string {
	return "recorder:" + accountID + "/" + region
}

// cfgsvcRecorderStatusKey holds that recorder's status.
func cfgsvcRecorderStatusKey(accountID, region string) string {
	return "recorder_status:" + accountID + "/" + region
}

// cfgsvcChannelKey holds the single delivery channel for an account and Region,
// keyed for the same reason cfgsvcRecorderKey is.
func cfgsvcChannelKey(accountID, region string) string {
	return "channel:" + accountID + "/" + region
}

// cfgsvcTagsKey holds the tags on a Config resource, keyed by its ARN. Tags are
// keyed by ARN rather than by name so one map serves recorders, rules and packs,
// which is also the shape TagResource's own input has.
func cfgsvcTagsKey(arn string) string { return "tags:" + arn }

// --- ARNs ---
//
// The ARN formats come from the Service Authorization Reference's "Resource types
// defined by AWS Config" table, not from the API reference, which gives none:
//
//	arn:${Partition}:config:${Region}:${Account}:config-recorder/${Name}
//	arn:${Partition}:config:${Region}:${Account}:delivery-channel/${Name}
//	arn:${Partition}:config:${Region}:${Account}:config-rule/${ConfigRuleId}
//	arn:${Partition}:config:${Region}:${Account}:conformance-pack/${Name}/${Id}
//
// The recorder form is "config-recorder", not "recorder". Substrate's own
// CloudFormation stub emitted the latter (#580); it is corrected where that stub
// becomes a real dispatch.

// cfgsvcARN builds a Config ARN for a resource type and identifier.
func cfgsvcARN(ctx *RequestContext, resourceType, id string) string {
	return fmt.Sprintf("arn:aws:config:%s:%s:%s/%s", ctx.Region, ctx.AccountID, resourceType, id)
}

// cfgsvcRecorderARN builds the ARN of a configuration recorder.
func cfgsvcRecorderARN(ctx *RequestContext, name string) string {
	return cfgsvcARN(ctx, "config-recorder", name)
}

// A delivery-channel ARN is deliberately absent here: the DeliveryChannel shape
// carries no arn member, so nothing in this cluster has one to emit. The
// delivery-channel form above is recorded because the tag operations address a
// channel by ARN, and it arrives with them.

// --- errors ---
//
// Every AWS Config exception is HTTP 400. Each exception shape in the API model
// carries "exception": true with no "error" member giving a status, and every
// exception's reference page states "HTTP Status Code: 400" — including the
// not-found ones such as NoSuchConfigurationRecorderException. The SDKs match on
// the error code rather than the status, so a 404 here would be a shape a caller
// cannot reproduce against AWS. Organizations is in the same position, and orgErr
// hard-codes 400 for the same reason.

// cfgsvcErr returns an AWS Config error with the given code at HTTP 400.
func cfgsvcErr(code, message string) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: http.StatusBadRequest}
}

// cfgsvcValidation returns ValidationException, which the model documents for
// "missing required fields or if the input value fails the validation".
func cfgsvcValidation(message string) *AWSError {
	return cfgsvcErr("ValidationException", message)
}

// cfgsvcInvalidAction reports an operation the plugin does not implement.
//
// AWS Config has 97 operations and substrate implements the detective-controls
// subset, so this is the answer for the rest. It names the operation so a consumer
// discovers which call is missing rather than a bare refusal.
func cfgsvcInvalidAction(op string) *AWSError {
	return cfgsvcErr("InvalidAction", "ConfigServicePlugin: unsupported operation "+op)
}

// cfgsvcUnmarshal decodes a request body, answering ValidationException rather
// than a generic malformed-data code: ValidationException is the input exception
// every Config operation declares, so it is the branch a caller's error handling
// is written against.
func cfgsvcUnmarshal(body []byte, out interface{}) error {
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, out); err != nil {
		return cfgsvcValidation("could not parse request body: " + err.Error())
	}
	return nil
}

// --- generic state helpers ---

// cfgsvcGetJSON loads and decodes the value at key, reporting found=false when the
// key is absent so a caller can tell "no such entity" from a read error.
func (p *ConfigServicePlugin) cfgsvcGetJSON(ctx context.Context, key string, out interface{}) (bool, error) {
	data, err := p.state.Get(ctx, configServiceNamespace, key)
	if err != nil {
		return false, fmt.Errorf("config get %s: %w", key, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("config unmarshal %s: %w", key, err)
	}
	return true, nil
}

// cfgsvcPutJSON encodes and stores v at key.
func (p *ConfigServicePlugin) cfgsvcPutJSON(ctx context.Context, key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("config marshal %s: %w", key, err)
	}
	if err := p.state.Put(ctx, configServiceNamespace, key, data); err != nil {
		return fmt.Errorf("config put %s: %w", key, err)
	}
	return nil
}

// cfgsvcDeleteKey removes a state key outright, so a deleted resource leaves no
// shadow in a state dump.
func (p *ConfigServicePlugin) cfgsvcDeleteKey(ctx context.Context, key string) error {
	if err := p.state.Delete(ctx, configServiceNamespace, key); err != nil {
		return fmt.Errorf("config delete %s: %w", key, err)
	}
	return nil
}

// cfgsvcSaveTags stores the tags on a resource ARN, deleting the key when the map
// is empty rather than storing "{}".
func (p *ConfigServicePlugin) cfgsvcSaveTags(ctx context.Context, arn string, tags map[string]string) error {
	if len(tags) == 0 {
		return p.cfgsvcDeleteKey(ctx, cfgsvcTagsKey(arn))
	}
	return p.cfgsvcPutJSON(ctx, cfgsvcTagsKey(arn), tags)
}

// --- tag validation ---
//
// The restrictions come from the developer guide's "Tagging AWS Config resources"
// page rather than from the API model, which bounds only the key and value lengths.
// The tag trio (TagResource/UntagResource/ListTagsForResource) validates through the
// same helpers a Put's creation-time TagsList does, so a tag AWS would refuse cannot
// enter through one door and be refused at the other.

// cfgsvcMaxTags is the number of tags AWS Config permits per resource, from both the
// service-limits page ("Maximum number of tags per resource: 50") and TagsList's own
// bound.
const cfgsvcMaxTags = 50

// cfgsvcReservedTagPrefix is the prefix AWS reserves for its own tags. The guide
// states "you can't edit or delete a tag with this prefix" and that such tags "do not
// count against your tags per resource limit" — so a caller-supplied one is refused
// rather than stored and counted.
const cfgsvcReservedTagPrefix = "aws:"

// cfgsvcTagCharset is the character set a tag key or value may use: "Unicode letters,
// digits, whitespace, or these symbols: _ . : / = + - @".
//
// Unicode letters and digits are matched by class rather than enumerated, so a tag in
// a non-Latin script is accepted as AWS accepts it.
var cfgsvcTagCharset = regexp.MustCompile(`^[\p{L}\p{N}\p{Z}_.:/=+\-@]*$`)

// cfgsvcCheckTag validates one tag key and value.
//
// An empty value is permitted — "you can set the value of a tag to an empty string,
// but you can't set the value of a tag to null" — and an empty *key* is not, because a
// key is what identifies the tag.
func cfgsvcCheckTag(key, value string) error {
	if key == "" {
		return cfgsvcValidation("A tag key cannot be empty.")
	}
	if len([]rune(key)) > 128 {
		return cfgsvcValidation("A tag key may be up to 128 Unicode characters long.")
	}
	if len([]rune(value)) > 256 {
		return cfgsvcValidation("A tag value may be up to 256 Unicode characters long.")
	}
	if strings.HasPrefix(strings.ToLower(key), cfgsvcReservedTagPrefix) {
		return cfgsvcValidation(`The "aws:" prefix is reserved for AWS use and cannot be used in a tag key.`)
	}
	if !cfgsvcTagCharset.MatchString(key) || !cfgsvcTagCharset.MatchString(value) {
		return cfgsvcValidation("A tag key or value may contain Unicode letters, digits, whitespace, " +
			"or these symbols: _ . : / = + - @")
	}
	return nil
}
