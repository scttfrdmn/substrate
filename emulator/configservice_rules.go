package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// The Config-rule cluster, and compliance (#580).
//
// A rule here is a *recorded intent* plus a *seeded verdict*, and the split is
// deliberate. Creating a rule is an API observation and is modeled fully: the name,
// source, scope, evaluation modes, the minted ConfigRuleId and ConfigRuleArn, the
// refusals, the 1000-rule ceiling. Whether a rule finds a resource compliant is
// not: it is the outcome of running rule logic — a Guard policy, a Lambda function,
// or one of several hundred AWS-managed rule implementations — against resource
// state. Per the scope boundary in CLAUDE.md that is workload-internal, so
// substrate does not compute it.
//
// Compliance is therefore **seeded, never computed**, defaulting to
// INSUFFICIENT_DATA. That default is AWS's own answer for a rule that has not
// evaluated — "Config returns the INSUFFICIENT_DATA value when no evaluation
// results are available" — and not a fabricated COMPLIANT. The distinction is the
// whole value of the cluster: a consumer whose gate reads COMPLIANT as "pass" must
// fail closed on a rule that has not run yet, and an emulator that answered
// COMPLIANT by default would let that bug through while looking green.
//
// Computing compliance instead would also make a consumer's assertion silently
// change meaning: a test written against a rule substrate judges COMPLIANT today
// would flip to NON_COMPLIANT as an unrelated plugin gained fidelity, through no
// change of the consumer's. A seed cannot drift like that.
//
// **PutEvaluations is the exception that proves the rule.** A custom rule reporting
// its own verdict *is* an API observation — the caller supplies the answer — so what
// arrives is recorded and reported back in preference to any seed. That ordering is
// the point: a fixture seeds a default for rules it does not care about, and a
// consumer's own Lambda under test overrides it for the rule it does.

// cfgsvcMaxConfigRules is the number of Config rules AWS permits per account per
// Region: "the account already contains the maximum number of 1000 rules". The
// service-limits page marks this one as not increasable.
const cfgsvcMaxConfigRules = 1000

// cfgsvcMaxRuleNamesFilter is the ConfigRuleNames list bound (0-25), shared by
// DescribeConfigRules and DescribeComplianceByConfigRule.
const cfgsvcMaxRuleNamesFilter = 25

// cfgsvcMaxComplianceTypesFilter is the ComplianceTypes list bound (0-3).
const cfgsvcMaxComplianceTypesFilter = 3

// cfgsvcLimitCap is the Limit ceiling for the operations that take one, from the
// model's Limit shape (0-100).
const cfgsvcLimitCap = 100

// cfgsvcDefaultPageSize is the page size substrate serves when a caller supplies no
// Limit, for operations whose input has no Limit member at all.
//
// DescribeConfigRules and DescribeComplianceByConfigRule paginate by token only —
// neither input shape carries a Limit, and the botocore paginator config gives
// neither a limit_key — so the page size is the service's to choose. 100 matches
// the ceiling the operations that *do* take a Limit use, so a caller looping until
// the token is empty behaves the same across all four.
const cfgsvcDefaultPageSize = 100

// Compliance types, the ComplianceType enum.
const (
	// cfgsvcCompliant is a rule all of whose evaluated resources comply.
	cfgsvcCompliant = "COMPLIANT"

	// cfgsvcNonCompliant is a rule at least one of whose resources does not comply.
	cfgsvcNonCompliant = "NON_COMPLIANT"

	// cfgsvcNotApplicableCompliance is a resource the rule does not apply to.
	cfgsvcNotApplicableCompliance = "NOT_APPLICABLE"

	// cfgsvcInsufficientData is a rule with no evaluation results — the default here.
	cfgsvcInsufficientData = "INSUFFICIENT_DATA"
)

// cfgsvcComplianceTypes is the ComplianceType enum in full, used to validate a
// filter or a seed.
var cfgsvcComplianceTypes = []string{
	cfgsvcCompliant,
	cfgsvcNonCompliant,
	cfgsvcNotApplicableCompliance,
	cfgsvcInsufficientData,
}

// cfgsvcRuleComplianceTypes is the subset the Compliance shape supports.
//
// The shape's own documentation narrows the enum: "For the Compliance data type,
// Config supports only COMPLIANT, NON_COMPLIANT, and INSUFFICIENT_DATA values.
// Config does not support the NOT_APPLICABLE value for the Compliance data type."
// A rule-level seed of NOT_APPLICABLE is therefore refused rather than stored — it
// would be a value DescribeComplianceByConfigRule cannot legitimately report.
var cfgsvcRuleComplianceTypes = []string{
	cfgsvcCompliant,
	cfgsvcNonCompliant,
	cfgsvcInsufficientData,
}

// cfgsvcEvaluationComplianceTypes is the subset the Evaluation shape supports.
//
// Narrowed the other way from cfgsvcRuleComplianceTypes: "Config does not support
// the INSUFFICIENT_DATA value for this data type. Similarly, Config does not accept
// INSUFFICIENT_DATA as the value for ComplianceType from a PutEvaluations request.
// For example, an Lambda function for a custom Config rule cannot pass an
// INSUFFICIENT_DATA value to Config." A rule with no data reports
// INSUFFICIENT_DATA; a resource evaluated by a rule cannot.
var cfgsvcEvaluationComplianceTypes = []string{
	cfgsvcCompliant,
	cfgsvcNonCompliant,
	cfgsvcNotApplicableCompliance,
}

// cfgsvcRuleOwners is the Owner enum: who owns and maintains the rule's logic.
var cfgsvcRuleOwners = []string{"AWS", "CUSTOM_LAMBDA", "CUSTOM_POLICY"}

// cfgsvcEvaluationModes is the EvaluationMode enum.
var cfgsvcEvaluationModes = []string{"DETECTIVE", "PROACTIVE"}

// cfgsvcExecutionFrequencies is the MaximumExecutionFrequency enum. Note the
// underscore-and-capital spelling of each member — One_Hour, TwentyFour_Hours —
// which is the model's and not a normalization substrate is free to apply.
var cfgsvcExecutionFrequencies = []string{
	"One_Hour", "Three_Hours", "Six_Hours", "Twelve_Hours", "TwentyFour_Hours",
}

// cfgsvcRuleStateActive is the ConfigRuleState a rule substrate stores is in. The
// other three members (DELETING, DELETING_RESULTS, EVALUATING) are transient states
// of an asynchronous process substrate does not run: a delete here completes within
// the call, so a rule is either ACTIVE or absent.
const cfgsvcRuleStateActive = "ACTIVE"

// cfgsvcRuleNamePattern is ConfigRuleName's pattern (.*\S.*): at least one
// non-whitespace character.
var cfgsvcRuleNamePattern = regexp.MustCompile(`\S`)

// cfgsvcPolicyRuntimePattern is PolicyRuntime's pattern (guard\-2\.x\.x), the only
// runtime the model accepts for a Custom Policy rule.
var cfgsvcPolicyRuntimePattern = regexp.MustCompile(`^guard\-2\.x\.x$`)

// ConfigRuleSource is the Source shape: who owns the rule's logic and where it lives.
type ConfigRuleSource struct {
	// Owner is AWS, CUSTOM_LAMBDA or CUSTOM_POLICY. Required.
	Owner string `json:"Owner"`

	// SourceIdentifier is a managed-rule identifier or a Lambda function ARN. Ignored
	// for a CUSTOM_POLICY rule, per the member's own note.
	SourceIdentifier string `json:"SourceIdentifier,omitempty"`

	// SourceDetails are the events that trigger an evaluation.
	SourceDetails []ConfigRuleSourceDetail `json:"SourceDetails,omitempty"`

	// CustomPolicyDetails is the Guard policy, required when Owner is CUSTOM_POLICY.
	CustomPolicyDetails *ConfigCustomPolicyDetails `json:"CustomPolicyDetails,omitempty"`
}

// ConfigRuleSourceDetail is the SourceDetail shape.
type ConfigRuleSourceDetail struct {
	// EventSource is the service whose changes trigger an evaluation.
	EventSource string `json:"EventSource,omitempty"`

	// MessageType is the notification type that triggers an evaluation.
	MessageType string `json:"MessageType,omitempty"`

	// MaximumExecutionFrequency is the periodic-trigger frequency.
	MaximumExecutionFrequency string `json:"MaximumExecutionFrequency,omitempty"`
}

// ConfigCustomPolicyDetails is the CustomPolicyDetails shape: the Guard runtime and
// policy text of a Custom Policy rule.
//
// The policy text is stored verbatim and never interpreted. Running Guard against
// resource state is exactly the workload-internal evaluation this cluster's comment
// declines to model, so the text is recorded intent — round-tripped by
// DescribeConfigRules — and the verdict comes from a seed.
type ConfigCustomPolicyDetails struct {
	// PolicyRuntime is the Guard runtime, which must be guard-2.x.x.
	PolicyRuntime string `json:"PolicyRuntime"`

	// PolicyText is the Guard policy, stored and not evaluated.
	PolicyText string `json:"PolicyText"`

	// EnableDebugLogDelivery turns on debug logging for the rule.
	EnableDebugLogDelivery bool `json:"EnableDebugLogDelivery,omitempty"`
}

// ConfigRuleScope is the Scope shape: which resources trigger an evaluation.
type ConfigRuleScope struct {
	// ComplianceResourceTypes are the resource types in scope (0-100).
	ComplianceResourceTypes []string `json:"ComplianceResourceTypes,omitempty"`

	// TagKey scopes the rule to resources carrying this tag key.
	TagKey string `json:"TagKey,omitempty"`

	// TagValue scopes it further by value, and requires TagKey.
	TagValue string `json:"TagValue,omitempty"`

	// ComplianceResourceId scopes the rule to one resource, and requires exactly one
	// entry in ComplianceResourceTypes.
	ComplianceResourceId string `json:"ComplianceResourceId,omitempty"` //nolint:revive // wire name.
}

// ConfigEvaluationMode is the EvaluationModeConfiguration shape.
type ConfigEvaluationMode struct {
	// Mode is DETECTIVE or PROACTIVE.
	Mode string `json:"Mode,omitempty"`
}

// ConfigRule is a stored Config rule — the ConfigRule shape, plus the tags a Put
// set at creation.
//
// Unlike ConfigRecorder, whose members are lowerCamel, this shape's are UpperCamel.
// The asymmetry is the API model's.
type ConfigRule struct {
	// ConfigRuleName is the rule name, required when creating.
	ConfigRuleName string `json:"ConfigRuleName,omitempty"`

	// ConfigRuleArn is minted by substrate, never accepted from a create.
	ConfigRuleArn string `json:"ConfigRuleArn,omitempty"`

	// ConfigRuleId is minted by substrate, never accepted from a create.
	ConfigRuleId string `json:"ConfigRuleId,omitempty"` //nolint:revive // wire name.

	// Description is the caller's description of the rule.
	Description string `json:"Description,omitempty"`

	// Scope narrows which resources trigger an evaluation.
	Scope *ConfigRuleScope `json:"Scope,omitempty"`

	// Source is the rule's owner and logic, required.
	Source *ConfigRuleSource `json:"Source,omitempty"`

	// InputParameters is a JSON string handed to the rule's function.
	InputParameters string `json:"InputParameters,omitempty"`

	// MaximumExecutionFrequency is how often a periodic rule evaluates.
	MaximumExecutionFrequency string `json:"MaximumExecutionFrequency,omitempty"`

	// ConfigRuleState is ACTIVE for every rule substrate stores; see
	// cfgsvcRuleStateActive.
	ConfigRuleState string `json:"ConfigRuleState,omitempty"`

	// CreatedBy is the service principal of the service that created a service-linked
	// rule, and is empty for a rule a caller created — "The field is empty if you
	// create your own rule." Substrate models no service-linked rules, so it is always
	// empty and is never accepted from a request.
	CreatedBy string `json:"CreatedBy,omitempty"`

	// EvaluationModes are the modes the rule evaluates in.
	EvaluationModes []ConfigEvaluationMode `json:"EvaluationModes,omitempty"`

	// Tags are the tags set when the rule was created, not updated by a later Put.
	Tags map[string]string `json:"-"`
}

// ConfigEvaluation is the Evaluation shape: one resource's verdict, as submitted
// through PutEvaluations.
type ConfigEvaluation struct {
	// ComplianceResourceType is the evaluated resource's type. Required.
	ComplianceResourceType string `json:"ComplianceResourceType"`

	// ComplianceResourceId is the evaluated resource's ID. Required.
	ComplianceResourceId string `json:"ComplianceResourceId"` //nolint:revive // wire name.

	// ComplianceType is the verdict, which may not be INSUFFICIENT_DATA. Required.
	ComplianceType string `json:"ComplianceType"`

	// Annotation is supplementary information about how the verdict was reached.
	Annotation string `json:"Annotation,omitempty"`

	// OrderingTimestamp is when the triggering event occurred. Required.
	OrderingTimestamp EpochSeconds `json:"OrderingTimestamp"`
}

// cfgsvcStoredEvaluation is a submitted evaluation with the times substrate
// recorded it at, which the Evaluation shape does not carry but EvaluationResult
// does.
type cfgsvcStoredEvaluation struct {
	// Evaluation is what the caller submitted, verbatim.
	Evaluation ConfigEvaluation `json:"evaluation"`

	// RecordedAt is when PutEvaluations accepted it, reported as ResultRecordedTime.
	RecordedAt EpochSeconds `json:"recordedAt"`

	// ResultToken is the token the submission carried, echoed by EvaluationResult.
	ResultToken string `json:"resultToken,omitempty"`
}

// --- request shapes ---

// cfgsvcPutRuleRequest is PutConfigRuleRequest.
type cfgsvcPutRuleRequest struct {
	// ConfigRule is the rule to create or update. Required.
	ConfigRule *ConfigRule `json:"ConfigRule"`

	// Tags are applied at creation only.
	Tags []ConfigTag `json:"Tags"`
}

// cfgsvcDescribeRulesRequest is DescribeConfigRulesRequest.
type cfgsvcDescribeRulesRequest struct {
	// ConfigRuleNames selects rules by name (0-25). Empty means all of them.
	ConfigRuleNames []string `json:"ConfigRuleNames"`

	// NextToken continues a previous page.
	NextToken string `json:"NextToken"`

	// Filters is DescribeConfigRulesFilters, whose only member is EvaluationMode.
	Filters *cfgsvcDescribeRulesFilters `json:"Filters"`
}

// cfgsvcDescribeRulesFilters is DescribeConfigRulesFilters.
type cfgsvcDescribeRulesFilters struct {
	// EvaluationMode selects Detective or Proactive rules.
	EvaluationMode string `json:"EvaluationMode"`
}

// cfgsvcDeleteRuleRequest is DeleteConfigRuleRequest.
type cfgsvcDeleteRuleRequest struct {
	// ConfigRuleName names the rule to delete. Required.
	ConfigRuleName string `json:"ConfigRuleName"`
}

// cfgsvcDescribeComplianceRequest is DescribeComplianceByConfigRuleRequest.
type cfgsvcDescribeComplianceRequest struct {
	// ConfigRuleNames selects rules by name (0-25).
	ConfigRuleNames []string `json:"ConfigRuleNames"`

	// ComplianceTypes filters by verdict (0-3).
	ComplianceTypes []string `json:"ComplianceTypes"`

	// NextToken continues a previous page.
	NextToken string `json:"NextToken"`
}

// cfgsvcComplianceDetailsRequest is GetComplianceDetailsByConfigRuleRequest.
type cfgsvcComplianceDetailsRequest struct {
	// ConfigRuleName names the rule. Required.
	ConfigRuleName string `json:"ConfigRuleName"`

	// ComplianceTypes filters by verdict (0-3).
	ComplianceTypes []string `json:"ComplianceTypes"`

	// Limit caps the page (0-100).
	Limit int `json:"Limit"`

	// NextToken continues a previous page.
	NextToken string `json:"NextToken"`
}

// cfgsvcPutEvaluationsRequest is PutEvaluationsRequest.
//
// Note what is *not* here: no ConfigRuleName. A real caller is a Lambda function
// invoked by a rule, and the encrypted ResultToken it was handed identifies the
// rule — which is why substrate's token encodes the rule name it was minted for.
// See cfgsvcResultToken.
type cfgsvcPutEvaluationsRequest struct {
	// Evaluations are the verdicts to record (0-100, optional).
	Evaluations []ConfigEvaluation `json:"Evaluations"`

	// ResultToken identifies the rule and triggering event. Required.
	ResultToken *string `json:"ResultToken"`

	// TestMode verifies delivery without recording anything.
	TestMode bool `json:"TestMode"`
}

// --- state keys ---

// cfgsvcRuleKey holds one Config rule, by account, Region and name.
func cfgsvcRuleKey(accountID, region, name string) string {
	return "rule:" + accountID + "/" + region + "/" + name
}

// cfgsvcRuleNamesKey holds the index of rule names for an account and Region, which
// the describe operations enumerate.
func cfgsvcRuleNamesKey(accountID, region string) string {
	return "rule_names:" + accountID + "/" + region
}

// cfgsvcEvaluationsKey holds the evaluations submitted for one rule.
func cfgsvcEvaluationsKey(accountID, region, rule string) string {
	return "evaluations:" + accountID + "/" + region + "/" + rule
}

// cfgsvcRuleARN builds a Config rule's ARN.
//
// The identifier component is the ConfigRuleId, per the Service Authorization
// Reference's config-rule/${ConfigRuleId} template — not the name. Substrate's own
// CloudFormation stub built config-rule/config-rule-<logicalID>, which is the same
// shape because a minted ID carries the config-rule- prefix AWS uses.
func cfgsvcRuleARN(ctx *RequestContext, ruleID string) string {
	return cfgsvcARN(ctx, "config-rule", ruleID)
}

// cfgsvcMintRuleID derives a rule's ConfigRuleId deterministically from the account,
// Region and name.
//
// AWS mints an opaque config-rule-xxxxxx; substrate derives it by hash so replaying
// the same event stream produces the same ID — a random or clock-derived one would
// make every recorded run's ARNs differ from the next, which is the property
// deterministic replay exists to provide. The config-rule- prefix and six-character
// suffix match the shape AWS returns, so a consumer's ID-parsing regex behaves the
// same.
func cfgsvcMintRuleID(accountID, region, name string) string {
	sum := sha256.Sum256([]byte("config-rule/" + accountID + "/" + region + "/" + name))
	return "config-rule-" + strings.ToLower(base64.RawURLEncoding.EncodeToString(sum[:4]))[:6]
}

// cfgsvcResultTokenPrefix is the envelope substrate's ResultToken carries.
//
// AWS's token is "an encrypted token that associates an evaluation with an Config
// rule" — opaque, and the *only* thing in a PutEvaluations request that says which
// rule the verdicts belong to, since that request carries no rule name. Substrate's
// is the base64url-encoded rule name behind this prefix:
//
//	substrate-config-rule:<base64url(ConfigRuleName)>
//
// Encoding the rule into the token rather than keeping server-side token state is
// what lets the association survive a replay: there is no minting event to replay,
// so a recorded run's tokens mean the same thing on every run.
//
// It is deliberately readable rather than encrypted, and the format is documented, so
// a consumer testing its own Config rule Lambda can construct one. Against AWS it
// could not — the token arrives in the rule's invocation event, which substrate does
// not produce because running the rule is workload-internal — so a constructible
// token is the only thing that makes a custom-rule fixture writable here at all.
const cfgsvcResultTokenPrefix = "substrate-config-rule:"

// cfgsvcRuleFromResultToken recovers the rule name a ResultToken carries; see
// cfgsvcResultTokenPrefix for the format.
//
// A token substrate did not mint is not an error here: TestMode accepts any non-null
// token, and outside TestMode an unrecognized token is InvalidResultTokenException,
// which the caller decides. So this reports only whether the token is one of
// substrate's and, if so, for which rule.
func cfgsvcRuleFromResultToken(token string) (string, bool) {
	if !strings.HasPrefix(token, cfgsvcResultTokenPrefix) {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, cfgsvcResultTokenPrefix))
	if err != nil || len(decoded) == 0 {
		return "", false
	}
	return string(decoded), true
}

// --- errors ---

// cfgsvcInvalidParameter is InvalidParameterValueException, the input complaint
// every operation in this cluster declares.
//
// The recorder cluster answers ValidationException, because its operations declare
// that one. None of the six rule operations does — their errors are
// InvalidParameterValueException, InvalidNextTokenException, NoSuchConfigRuleException
// and the two ceiling exceptions — so a caller's error handling here is written
// against this code and a ValidationException would be a branch it has not got.
func cfgsvcInvalidParameter(message string) *AWSError {
	return cfgsvcErr("InvalidParameterValueException", message)
}

// cfgsvcNoSuchRule is NoSuchConfigRuleException, with the reference's own message
// (including its "Resouce" typo left as AWS spells it, so a consumer matching on
// message text matches).
func cfgsvcNoSuchRule() *AWSError {
	return cfgsvcErr("NoSuchConfigRuleException",
		"The Config rule in the request is not valid. Verify that the rule is an Config Process "+
			"Check rule, that the rule name is correct, and that valid Amazon Resouce Names (ARNs) "+
			"are used before trying again.")
}

// cfgsvcInvalidNextToken is InvalidNextTokenException.
func cfgsvcInvalidNextToken() *AWSError {
	return cfgsvcErr("InvalidNextTokenException",
		"The specified next token is not valid. Specify the nextToken string that was returned in "+
			"the previous response to get the next page of results.")
}

// cfgsvcNoAvailableRecorder — PutConfigRule reports the same
// NoAvailableConfigurationRecorderException PutDeliveryChannel does, so it is shared
// from configservice_channel.go rather than duplicated: a rule cannot be created in
// an account with no recorder, because there would be nothing for it to evaluate.

// ruleOperation claims the Config-rule and compliance operations.
func (p *ConfigServicePlugin) ruleOperation(op string) (cfgsvcHandler, bool) {
	switch op {
	case "PutConfigRule":
		return p.putConfigRule, true
	case "DescribeConfigRules":
		return p.describeConfigRules, true
	case "DeleteConfigRule":
		return p.deleteConfigRule, true
	case "DescribeComplianceByConfigRule":
		return p.describeComplianceByConfigRule, true
	case "GetComplianceDetailsByConfigRule":
		return p.getComplianceDetailsByConfigRule, true
	case "PutEvaluations":
		return p.putEvaluations, true
	}
	return nil, false
}

// putConfigRule creates or updates a Config rule.
//
// A create must supply ConfigRuleName and must *not* supply ConfigRuleArn or
// ConfigRuleId — "These values are generated by Config for new rules" — so a
// consumer that echoes back a described rule to create a second one is told, rather
// than quietly getting a rule whose ID does not match its ARN.
//
// An update may name the rule by name, ID or ARN, and is idempotent: it does not
// create a duplicate and it does **not** replace tags. "Tags are added at creation
// and cannot be updated with this operation... If a following request has different
// tags values, Config will ignore these differences and treat it as an idempotent
// request of the previous".
func (p *ConfigServicePlugin) putConfigRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPutRuleRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if in.ConfigRule == nil {
		return nil, cfgsvcInvalidParameter("ConfigRule is required.")
	}
	rule := *in.ConfigRule

	goCtx := context.Background()

	// The recorder check comes first, before any field validation: a rule cannot exist
	// in an account with no recorder at all, so complaining about a scope member would
	// send the caller after the wrong problem.
	var recorder ConfigRecorder
	hasRecorder, err := p.cfgsvcGetJSON(goCtx, cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder)
	if err != nil {
		return nil, err
	}
	if !hasRecorder {
		return nil, cfgsvcNoAvailableRecorder()
	}

	existing, found, err := p.cfgsvcResolveRuleForPut(ctx, &rule)
	if err != nil {
		return nil, err
	}
	if !found {
		if rule.ConfigRuleArn != "" || rule.ConfigRuleId != "" {
			return nil, cfgsvcInvalidParameter("For any new Config rule that you add, specify the " +
				"ConfigRuleName in the ConfigRule object. Do not specify the ConfigRuleArn or the " +
				"ConfigRuleId. These values are generated by Config for new rules.")
		}
		if rule.ConfigRuleName == "" {
			return nil, cfgsvcInvalidParameter("The name is required if you are adding a new rule.")
		}
	} else {
		// An update keeps the stored name even when the request named the rule by ID or
		// ARN and left ConfigRuleName empty, so an update cannot silently rename a rule.
		rule.ConfigRuleName = existing.ConfigRuleName
	}

	if err := cfgsvcCheckRuleName(rule.ConfigRuleName); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckRuleSource(rule.Source); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckRuleScope(rule.Scope); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckRuleFields(&rule); err != nil {
		return nil, err
	}

	names, err := p.cfgsvcRuleNames(goCtx, ctx)
	if err != nil {
		return nil, err
	}
	if !found && len(names) >= cfgsvcMaxConfigRules {
		return nil, cfgsvcErr("MaxNumberOfConfigRulesExceededException",
			"Failed to add the Config rule because the account already contains the maximum number "+
				"of 1000 rules. Consider deleting any deactivated rules before you add new rules.")
	}

	// CreatedBy names the service that created a service-linked rule and is empty for
	// a caller's own rule. Substrate models no service-linked rules, so a
	// caller-supplied value is dropped rather than stored: echoing it back would
	// report a rule as service-created when it was not.
	rule.CreatedBy = ""
	rule.ConfigRuleState = cfgsvcRuleStateActive
	rule.ConfigRuleId = cfgsvcMintRuleID(ctx.AccountID, ctx.Region, rule.ConfigRuleName)
	rule.ConfigRuleArn = cfgsvcRuleARN(ctx, rule.ConfigRuleId)

	if err := p.cfgsvcSaveRule(goCtx, ctx, &rule); err != nil {
		return nil, err
	}
	if !found {
		tags, err := cfgsvcTagsToMap(in.Tags)
		if err != nil {
			return nil, err
		}
		if err := p.cfgsvcSaveTags(goCtx, rule.ConfigRuleArn, tags); err != nil {
			return nil, err
		}
	}
	return cfgsvcEmptyResponse(), nil
}

// cfgsvcResolveRuleForPut finds the rule a Put addresses, which may be named by
// ConfigRuleName, ConfigRuleId or ConfigRuleArn — "If you are updating a rule that
// you added previously, you can specify the rule by ConfigRuleName, ConfigRuleId,
// or ConfigRuleArn".
//
// An ID or ARN that matches no stored rule reports found=false rather than an error,
// so the create path's refusal ("do not specify the ConfigRuleArn or the
// ConfigRuleId") is what the caller sees. That is the more useful complaint: a
// request carrying an ID substrate never minted is far more likely to be an echoed
// describe response than an update of a rule that has been deleted.
func (p *ConfigServicePlugin) cfgsvcResolveRuleForPut(ctx *RequestContext, rule *ConfigRule) (
	*ConfigRule, bool, error) {
	goCtx := context.Background()
	if rule.ConfigRuleName != "" {
		var stored ConfigRule
		found, err := p.cfgsvcGetJSON(goCtx,
			cfgsvcRuleKey(ctx.AccountID, ctx.Region, rule.ConfigRuleName), &stored)
		if err != nil {
			return nil, false, err
		}
		if found {
			return &stored, true, nil
		}
		// A name that matches nothing is a create, even when an ID or ARN came with it —
		// which is the case the create-path refusal catches. Resolving by the ID instead
		// would update whichever rule owns it while ignoring the name the caller gave,
		// silently editing a rule the request never named.
		return nil, false, nil
	}
	if rule.ConfigRuleArn == "" && rule.ConfigRuleId == "" {
		return nil, false, nil
	}

	names, err := p.cfgsvcRuleNames(goCtx, ctx)
	if err != nil {
		return nil, false, err
	}
	for _, name := range names {
		var stored ConfigRule
		found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleKey(ctx.AccountID, ctx.Region, name), &stored)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		if (rule.ConfigRuleId != "" && stored.ConfigRuleId == rule.ConfigRuleId) ||
			(rule.ConfigRuleArn != "" && stored.ConfigRuleArn == rule.ConfigRuleArn) {
			return &stored, true, nil
		}
	}
	return nil, false, nil
}

// cfgsvcCheckRuleName validates a rule name against ConfigRuleName's bounds and
// pattern (1-128, .*\S.*).
func cfgsvcCheckRuleName(name string) error {
	if len(name) > 128 {
		return cfgsvcInvalidParameter("The Config rule name must be between 1 and 128 characters long.")
	}
	if !cfgsvcRuleNamePattern.MatchString(name) {
		return cfgsvcInvalidParameter("The Config rule name must contain at least one " +
			"non-whitespace character.")
	}
	return nil
}

// cfgsvcCheckRuleSource validates the Source shape, whose members are conditionally
// required by Owner.
func cfgsvcCheckRuleSource(src *ConfigRuleSource) error {
	if src == nil {
		return cfgsvcInvalidParameter("Source is required.")
	}
	if !slices.Contains(cfgsvcRuleOwners, src.Owner) {
		return cfgsvcInvalidParameter("The Source Owner must be one of AWS, CUSTOM_LAMBDA or " +
			"CUSTOM_POLICY.")
	}
	if len(src.SourceIdentifier) > 256 {
		return cfgsvcInvalidParameter("The SourceIdentifier may be up to 256 characters long.")
	}
	switch src.Owner {
	case "CUSTOM_POLICY":
		// "Required when owner is set to CUSTOM_POLICY." A Custom Policy rule with no
		// policy has nothing to evaluate with, which is a create AWS refuses.
		if src.CustomPolicyDetails == nil {
			return cfgsvcInvalidParameter("CustomPolicyDetails is required when the Source Owner " +
				"is CUSTOM_POLICY.")
		}
		if !cfgsvcPolicyRuntimePattern.MatchString(src.CustomPolicyDetails.PolicyRuntime) {
			return cfgsvcInvalidParameter("The PolicyRuntime must be guard-2.x.x.")
		}
		if len(src.CustomPolicyDetails.PolicyText) > 10000 {
			return cfgsvcInvalidParameter("The PolicyText may be up to 10000 characters long.")
		}
	default:
		// An AWS managed rule needs its managed-rule identifier and a Custom Lambda rule
		// needs its function ARN; both arrive in SourceIdentifier. It is not required for
		// CUSTOM_POLICY, where "this field will be ignored".
		if src.SourceIdentifier == "" {
			return cfgsvcInvalidParameter("The SourceIdentifier is required when the Source Owner " +
				"is AWS or CUSTOM_LAMBDA.")
		}
	}
	for _, detail := range src.SourceDetails {
		if detail.MaximumExecutionFrequency != "" &&
			!slices.Contains(cfgsvcExecutionFrequencies, detail.MaximumExecutionFrequency) {
			return cfgsvcInvalidParameter("The MaximumExecutionFrequency " +
				detail.MaximumExecutionFrequency + " is not valid.")
		}
	}
	return nil
}

// cfgsvcCheckRuleScope validates the Scope shape's documented co-requirements.
func cfgsvcCheckRuleScope(scope *ConfigRuleScope) error {
	if scope == nil {
		return nil
	}
	if len(scope.ComplianceResourceTypes) > 100 {
		return cfgsvcInvalidParameter("ComplianceResourceTypes accepts up to 100 resource types.")
	}
	// "If you specify a value for TagValue, you must also specify a value for TagKey."
	if scope.TagValue != "" && scope.TagKey == "" {
		return cfgsvcInvalidParameter("If you specify a value for TagValue, you must also specify " +
			"a value for TagKey.")
	}
	// "If you specify a resource ID, you must specify one resource type for
	// ComplianceResourceTypes."
	if scope.ComplianceResourceId != "" && len(scope.ComplianceResourceTypes) != 1 {
		return cfgsvcInvalidParameter("If you specify a resource ID, you must specify one resource " +
			"type for ComplianceResourceTypes.")
	}
	if len(scope.TagKey) > 128 {
		return cfgsvcInvalidParameter("The Scope TagKey may be up to 128 characters long.")
	}
	if len(scope.TagValue) > 256 {
		return cfgsvcInvalidParameter("The Scope TagValue may be up to 256 characters long.")
	}
	return nil
}

// cfgsvcCheckRuleFields validates the rule's own scalar members against the model's
// bounds and enums.
func cfgsvcCheckRuleFields(rule *ConfigRule) error {
	if len(rule.Description) > 256 {
		return cfgsvcInvalidParameter("The Description may be up to 256 characters long.")
	}
	if len(rule.InputParameters) > 1024 {
		return cfgsvcInvalidParameter("The InputParameters may be up to 1024 characters long.")
	}
	if rule.MaximumExecutionFrequency != "" &&
		!slices.Contains(cfgsvcExecutionFrequencies, rule.MaximumExecutionFrequency) {
		return cfgsvcInvalidParameter("The MaximumExecutionFrequency " +
			rule.MaximumExecutionFrequency + " is not valid.")
	}
	for _, mode := range rule.EvaluationModes {
		if mode.Mode != "" && !slices.Contains(cfgsvcEvaluationModes, mode.Mode) {
			return cfgsvcInvalidParameter("The evaluation mode " + mode.Mode + " is not valid.")
		}
	}
	return nil
}

// cfgsvcSaveRule stores a rule and adds its name to the index in one place, so the
// two cannot diverge: an index that can exist on one side only, will.
func (p *ConfigServicePlugin) cfgsvcSaveRule(goCtx context.Context, ctx *RequestContext, rule *ConfigRule) error {
	if err := p.cfgsvcPutJSON(goCtx,
		cfgsvcRuleKey(ctx.AccountID, ctx.Region, rule.ConfigRuleName), rule); err != nil {
		return err
	}
	names, err := p.cfgsvcRuleNames(goCtx, ctx)
	if err != nil {
		return err
	}
	if slices.Contains(names, rule.ConfigRuleName) {
		return nil
	}
	names = append(names, rule.ConfigRuleName)
	sort.Strings(names)
	return p.cfgsvcPutJSON(goCtx, cfgsvcRuleNamesKey(ctx.AccountID, ctx.Region), names)
}

// cfgsvcDeleteRuleRecord removes a rule, its index entry, its tags, its compliance
// seed and its recorded evaluations — the counterpart of cfgsvcSaveRule.
func (p *ConfigServicePlugin) cfgsvcDeleteRuleRecord(goCtx context.Context, ctx *RequestContext,
	rule *ConfigRule) error {
	for _, key := range []string{
		cfgsvcRuleKey(ctx.AccountID, ctx.Region, rule.ConfigRuleName),
		cfgsvcTagsKey(rule.ConfigRuleArn),
		cfgsvcEvaluationsKey(ctx.AccountID, ctx.Region, rule.ConfigRuleName),
	} {
		if err := p.cfgsvcDeleteKey(goCtx, key); err != nil {
			return err
		}
	}
	names, err := p.cfgsvcRuleNames(goCtx, ctx)
	if err != nil {
		return err
	}
	remaining := make([]string, 0, len(names))
	for _, name := range names {
		if name != rule.ConfigRuleName {
			remaining = append(remaining, name)
		}
	}
	if len(remaining) == 0 {
		return p.cfgsvcDeleteKey(goCtx, cfgsvcRuleNamesKey(ctx.AccountID, ctx.Region))
	}
	return p.cfgsvcPutJSON(goCtx, cfgsvcRuleNamesKey(ctx.AccountID, ctx.Region), remaining)
}

// cfgsvcRuleNames loads the rule-name index for an account and Region.
func (p *ConfigServicePlugin) cfgsvcRuleNames(goCtx context.Context, ctx *RequestContext) ([]string, error) {
	var names []string
	if _, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleNamesKey(ctx.AccountID, ctx.Region), &names); err != nil {
		return nil, err
	}
	return names, nil
}

// describeConfigRules reports the account's rules, paginated by token.
//
// The input shape carries no Limit — this operation paginates by token alone — so
// the page size is substrate's, and it is cfgsvcDefaultPageSize.
func (p *ConfigServicePlugin) describeConfigRules(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcDescribeRulesRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if len(in.ConfigRuleNames) > cfgsvcMaxRuleNamesFilter {
		return nil, cfgsvcInvalidParameter("ConfigRuleNames accepts up to 25 rule names.")
	}
	mode := ""
	if in.Filters != nil {
		mode = in.Filters.EvaluationMode
		if mode != "" && !slices.Contains(cfgsvcEvaluationModes, mode) {
			return nil, cfgsvcInvalidParameter("The evaluation mode " + mode + " is not valid.")
		}
	}

	goCtx := context.Background()
	selected, err := p.cfgsvcSelectRules(goCtx, ctx, in.ConfigRuleNames)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(selected))
	byName := make(map[string]ConfigRule, len(selected))
	for _, rule := range selected {
		if !cfgsvcRuleHasMode(&rule, mode) {
			continue
		}
		names = append(names, rule.ConfigRuleName)
		byName[rule.ConfigRuleName] = rule
	}

	page, next, err := cfgsvcPaginate(names, in.NextToken, 0, cfgsvcDefaultPageSize)
	if err != nil {
		return nil, err
	}
	rules := make([]ConfigRule, 0, len(page))
	for _, name := range page {
		rules = append(rules, byName[name])
	}
	out := map[string]interface{}{"ConfigRules": rules}
	if next != "" {
		out["NextToken"] = next
	}
	return cfgsvcJSONResponse(out, "describeConfigRules")
}

// cfgsvcRuleHasMode reports whether a rule matches an EvaluationMode filter.
//
// A rule with no EvaluationModes matches DETECTIVE, because that is the service's
// default: "By default, the value is Detective evaluation mode only." Treating an
// empty list as matching nothing would hide every rule created without the member —
// which is most of them, since the CLI does not require it.
func cfgsvcRuleHasMode(rule *ConfigRule, mode string) bool {
	if mode == "" {
		return true
	}
	if len(rule.EvaluationModes) == 0 {
		return mode == "DETECTIVE"
	}
	for _, m := range rule.EvaluationModes {
		if m.Mode == mode {
			return true
		}
	}
	return false
}

// cfgsvcSelectRules loads the rules a name filter selects, or all of them when the
// filter is empty.
//
// A named rule that does not exist is NoSuchConfigRuleException, because the caller
// asserted the name; an empty filter against an account with no rules is an empty
// list, because that is the state a fresh account is in.
func (p *ConfigServicePlugin) cfgsvcSelectRules(goCtx context.Context, ctx *RequestContext,
	filter []string) ([]ConfigRule, error) {
	names := filter
	if len(names) == 0 {
		var err error
		names, err = p.cfgsvcRuleNames(goCtx, ctx)
		if err != nil {
			return nil, err
		}
	}
	rules := make([]ConfigRule, 0, len(names))
	for _, name := range names {
		var rule ConfigRule
		found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleKey(ctx.AccountID, ctx.Region, name), &rule)
		if err != nil {
			return nil, err
		}
		if !found {
			if len(filter) > 0 {
				return nil, cfgsvcNoSuchRule()
			}
			continue
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

// deleteConfigRule deletes a rule "and all of its evaluation results".
//
// The evaluation results go with it, per that sentence, and so does the compliance
// seed: a rebuilt rule of the same name starts at INSUFFICIENT_DATA rather than
// inheriting its predecessor's verdict, which no AWS account does.
//
// AWS sets the rule's state to DELETING and completes asynchronously, answering
// ResourceInUseException to a Put or Delete arriving in the meantime. Substrate's
// delete completes within the call, so that window does not exist and no state
// substrate stores is ever DELETING. A consumer polling DescribeConfigRules until
// the rule disappears converges on the first poll rather than looping.
func (p *ConfigServicePlugin) deleteConfigRule(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcDeleteRuleRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if in.ConfigRuleName == "" {
		return nil, cfgsvcInvalidParameter("ConfigRuleName is required.")
	}
	goCtx := context.Background()
	var rule ConfigRule
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleKey(ctx.AccountID, ctx.Region, in.ConfigRuleName), &rule)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cfgsvcNoSuchRule()
	}
	if err := p.cfgsvcDeleteRuleRecord(goCtx, ctx, &rule); err != nil {
		return nil, err
	}
	if err := p.cfgsvcClearRuleCompliance(goCtx, ctx, rule.ConfigRuleName); err != nil {
		return nil, err
	}
	return cfgsvcEmptyResponse(), nil
}

// describeComplianceByConfigRule reports each rule's verdict.
//
// The verdict is the seed, or INSUFFICIENT_DATA when nothing is seeded — see the
// cluster comment for why it is never computed. Where PutEvaluations has recorded
// submissions for a rule, they win: the rule is NON_COMPLIANT if any submission is,
// COMPLIANT if all are, and the CappedCount reports how many are not. A custom
// rule's own report is an API observation and outranks a fixture default.
func (p *ConfigServicePlugin) describeComplianceByConfigRule(ctx *RequestContext, req *AWSRequest) (
	*AWSResponse, error) {
	var in cfgsvcDescribeComplianceRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if len(in.ConfigRuleNames) > cfgsvcMaxRuleNamesFilter {
		return nil, cfgsvcInvalidParameter("ConfigRuleNames accepts up to 25 rule names.")
	}
	if err := cfgsvcCheckComplianceTypes(in.ComplianceTypes); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	rules, err := p.cfgsvcSelectRules(goCtx, ctx, in.ConfigRuleNames)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(rules))
	entries := make(map[string]map[string]interface{}, len(rules))
	for i := range rules {
		compliance, err := p.cfgsvcRuleCompliance(goCtx, ctx, &rules[i])
		if err != nil {
			return nil, err
		}
		if len(in.ComplianceTypes) > 0 &&
			!slices.Contains(in.ComplianceTypes, compliance["ComplianceType"].(string)) {
			continue
		}
		names = append(names, rules[i].ConfigRuleName)
		entries[rules[i].ConfigRuleName] = map[string]interface{}{
			"ConfigRuleName": rules[i].ConfigRuleName,
			"Compliance":     compliance,
		}
	}

	page, next, err := cfgsvcPaginate(names, in.NextToken, 0, cfgsvcDefaultPageSize)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(page))
	for _, name := range page {
		out = append(out, entries[name])
	}
	body := map[string]interface{}{"ComplianceByConfigRules": out}
	if next != "" {
		body["NextToken"] = next
	}
	return cfgsvcJSONResponse(body, "describeComplianceByConfigRule")
}

// cfgsvcRuleCompliance builds one rule's Compliance shape.
//
// Precedence is recorded evaluations, then the seed, then INSUFFICIENT_DATA. The
// CappedCount is the number of NON_COMPLIANT contributors, which is meaningful only
// when there are evaluations to count: a seeded NON_COMPLIANT reports a count of 1,
// because the shape's own definition is "the number of resources that cause a result
// of NON_COMPLIANT" and a seed asserts that at least one does. CapExceeded is always
// false — substrate has no contributor cap to exceed.
func (p *ConfigServicePlugin) cfgsvcRuleCompliance(goCtx context.Context, ctx *RequestContext,
	rule *ConfigRule) (map[string]interface{}, error) {
	stored, err := p.cfgsvcLoadEvaluations(goCtx, ctx, rule.ConfigRuleName)
	if err != nil {
		return nil, err
	}
	if len(stored) > 0 {
		nonCompliant := 0
		for _, e := range stored {
			if e.Evaluation.ComplianceType == cfgsvcNonCompliant {
				nonCompliant++
			}
		}
		verdict := cfgsvcCompliant
		if nonCompliant > 0 {
			verdict = cfgsvcNonCompliant
		}
		return cfgsvcComplianceShape(verdict, nonCompliant), nil
	}

	seed, seeded, err := p.seededRuleCompliance(goCtx, ctx.AccountID, ctx.Region, rule.ConfigRuleName)
	if err != nil {
		return nil, err
	}
	if !seeded {
		return cfgsvcComplianceShape(cfgsvcInsufficientData, 0), nil
	}
	count := 0
	if seed.ComplianceType == cfgsvcNonCompliant {
		count = 1
	}
	return cfgsvcComplianceShape(seed.ComplianceType, count), nil
}

// cfgsvcComplianceShape builds the Compliance shape for a verdict and contributor
// count.
func cfgsvcComplianceShape(verdict string, nonCompliant int) map[string]interface{} {
	return map[string]interface{}{
		"ComplianceType": verdict,
		"ComplianceContributorCount": map[string]interface{}{
			"CappedCount": nonCompliant,
			"CapExceeded": false,
		},
	}
}

// cfgsvcCheckComplianceTypes validates a ComplianceTypes filter against its bound
// (0-3) and the enum.
func cfgsvcCheckComplianceTypes(types []string) error {
	if len(types) > cfgsvcMaxComplianceTypesFilter {
		return cfgsvcInvalidParameter("ComplianceTypes accepts up to 3 compliance types.")
	}
	for _, t := range types {
		if !slices.Contains(cfgsvcComplianceTypes, t) {
			return cfgsvcInvalidParameter("The compliance type " + t + " is not valid.")
		}
	}
	return nil
}

// getComplianceDetailsByConfigRule reports the per-resource evaluation results for
// one rule.
//
// Recorded evaluations are reported as they were submitted. With none, the rule's
// seeded verdict — if it names resources — is reported as synthesized results, and
// otherwise the list is empty: a rule that has evaluated nothing has no per-resource
// results, and inventing a resource to attach a verdict to would put a resource ID
// in the response that exists nowhere in the account.
func (p *ConfigServicePlugin) getComplianceDetailsByConfigRule(ctx *RequestContext, req *AWSRequest) (
	*AWSResponse, error) {
	var in cfgsvcComplianceDetailsRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if in.ConfigRuleName == "" {
		return nil, cfgsvcInvalidParameter("ConfigRuleName is required.")
	}
	if in.Limit < 0 || in.Limit > cfgsvcLimitCap {
		return nil, cfgsvcInvalidParameter("The Limit must be between 0 and 100.")
	}
	if err := cfgsvcCheckComplianceTypes(in.ComplianceTypes); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	var rule ConfigRule
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleKey(ctx.AccountID, ctx.Region, in.ConfigRuleName), &rule)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cfgsvcNoSuchRule()
	}

	results, err := p.cfgsvcEvaluationResults(goCtx, ctx, &rule)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(results))
	byKey := make(map[string]map[string]interface{}, len(results))
	for _, r := range results {
		if len(in.ComplianceTypes) > 0 && !slices.Contains(in.ComplianceTypes, r.compliance) {
			continue
		}
		keys = append(keys, r.key)
		byKey[r.key] = r.body
	}

	page, next, err := cfgsvcPaginate(keys, in.NextToken, in.Limit, cfgsvcLimitCap)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(page))
	for _, key := range page {
		out = append(out, byKey[key])
	}
	body := map[string]interface{}{"EvaluationResults": out}
	if next != "" {
		body["NextToken"] = next
	}
	return cfgsvcJSONResponse(body, "getComplianceDetailsByConfigRule")
}

// cfgsvcEvaluationResult is one EvaluationResult with the fields pagination and
// filtering need alongside it.
type cfgsvcEvaluationResult struct {
	// key is the pagination key: type/id, unique per evaluated resource.
	key string

	// compliance is the verdict, for the ComplianceTypes filter.
	compliance string

	// body is the EvaluationResult shape as it goes on the wire.
	body map[string]interface{}
}

// cfgsvcEvaluationResults builds a rule's EvaluationResult list from recorded
// evaluations, or from the seed's named resources when there are none.
func (p *ConfigServicePlugin) cfgsvcEvaluationResults(goCtx context.Context, ctx *RequestContext,
	rule *ConfigRule) ([]cfgsvcEvaluationResult, error) {
	stored, err := p.cfgsvcLoadEvaluations(goCtx, ctx, rule.ConfigRuleName)
	if err != nil {
		return nil, err
	}
	if len(stored) > 0 {
		out := make([]cfgsvcEvaluationResult, 0, len(stored))
		for _, e := range stored {
			out = append(out, cfgsvcEvaluationResult{
				key:        e.Evaluation.ComplianceResourceType + "/" + e.Evaluation.ComplianceResourceId,
				compliance: e.Evaluation.ComplianceType,
				body: cfgsvcEvaluationResultShape(rule.ConfigRuleName, e.Evaluation,
					e.RecordedAt, e.ResultToken),
			})
		}
		return out, nil
	}

	seed, seeded, err := p.seededRuleCompliance(goCtx, ctx.AccountID, ctx.Region, rule.ConfigRuleName)
	if err != nil {
		return nil, err
	}
	// A seed with no resources is a rule-level verdict only. It answers
	// DescribeComplianceByConfigRule and leaves this list empty, which is what AWS
	// returns for a rule whose evaluation produced no per-resource results.
	if !seeded || len(seed.Resources) == 0 {
		return nil, nil
	}
	// INSUFFICIENT_DATA cannot appear in an EvaluationResult — "Config does not
	// support the INSUFFICIENT_DATA value for the EvaluationResult data type" — so a
	// seed that pins it has no per-resource results to report either.
	if seed.ComplianceType == cfgsvcInsufficientData {
		return nil, nil
	}

	now := EpochSeconds(p.tc.Now())
	out := make([]cfgsvcEvaluationResult, 0, len(seed.Resources))
	for _, res := range seed.Resources {
		evaluation := ConfigEvaluation{
			ComplianceResourceType: res.ResourceType,
			ComplianceResourceId:   res.ResourceID,
			ComplianceType:         seed.ComplianceType,
			Annotation:             seed.Annotation,
			OrderingTimestamp:      now,
		}
		out = append(out, cfgsvcEvaluationResult{
			key:        res.ResourceType + "/" + res.ResourceID,
			compliance: seed.ComplianceType,
			body:       cfgsvcEvaluationResultShape(rule.ConfigRuleName, evaluation, now, ""),
		})
	}
	return out, nil
}

// cfgsvcEvaluationResultShape builds the EvaluationResult shape for one evaluation.
//
// ResourceEvaluationId is omitted: it identifies a result produced by
// StartResourceEvaluation, an on-demand proactive evaluation substrate does not
// model, so emitting a synthesized one would advertise a resource-evaluation record
// that GetResourceEvaluationSummary cannot then produce.
func cfgsvcEvaluationResultShape(ruleName string, e ConfigEvaluation, recordedAt EpochSeconds,
	resultToken string) map[string]interface{} {
	body := map[string]interface{}{
		"EvaluationResultIdentifier": map[string]interface{}{
			"EvaluationResultQualifier": map[string]interface{}{
				"ConfigRuleName": ruleName,
				"ResourceType":   e.ComplianceResourceType,
				"ResourceId":     e.ComplianceResourceId,
				"EvaluationMode": "DETECTIVE",
			},
			"OrderingTimestamp": e.OrderingTimestamp,
		},
		"ComplianceType":     e.ComplianceType,
		"ResultRecordedTime": recordedAt,
		// The rule ran when the triggering event was ordered, as far as anything
		// observable here goes: substrate runs no rule logic, so there is no separate
		// invocation instant to report.
		"ConfigRuleInvokedTime": e.OrderingTimestamp,
	}
	if e.Annotation != "" {
		body["Annotation"] = e.Annotation
	}
	if resultToken != "" {
		body["ResultToken"] = resultToken
	}
	return body
}

// putEvaluations records the verdicts a custom rule's function submits.
//
// This is the one place compliance is *not* seeded, and the reason is the scope
// boundary rather than convenience: the caller is supplying the answer, so accepting
// it is recording an API observation. A consumer testing its own Config rule Lambda
// can therefore drive the real code path — invoke the handler, have it call
// PutEvaluations, then assert through DescribeComplianceByConfigRule that the rule
// reports what the handler decided.
//
// The rule is identified by the ResultToken alone; this request carries no rule
// name. See cfgsvcResultToken.
func (p *ConfigServicePlugin) putEvaluations(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPutEvaluationsRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	// "When TestMode is true, PutEvaluations doesn't require a valid value for the
	// ResultToken parameter, but the value cannot be null." A null token is refused in
	// both modes; only its *validity* is waived.
	if in.ResultToken == nil {
		return nil, cfgsvcErr("InvalidResultTokenException", "The specified ResultToken is not valid.")
	}
	if len(in.Evaluations) > 100 {
		return nil, cfgsvcInvalidParameter("Evaluations accepts up to 100 evaluations.")
	}
	for i := range in.Evaluations {
		if err := cfgsvcCheckEvaluation(&in.Evaluations[i]); err != nil {
			return nil, err
		}
	}

	// TestMode verifies delivery and stores nothing: "No updates occur to your existing
	// evaluations, and evaluation results are not sent to Config." It answers before
	// the token is resolved, since the token need not be valid in this mode.
	if in.TestMode {
		return cfgsvcJSONResponse(map[string]interface{}{
			"FailedEvaluations": []ConfigEvaluation{},
		}, "putEvaluations")
	}

	ruleName, ok := cfgsvcRuleFromResultToken(*in.ResultToken)
	if !ok {
		return nil, cfgsvcErr("InvalidResultTokenException", "The specified ResultToken is not valid.")
	}
	goCtx := context.Background()
	var rule ConfigRule
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRuleKey(ctx.AccountID, ctx.Region, ruleName), &rule)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cfgsvcNoSuchRule()
	}

	existing, err := p.cfgsvcLoadEvaluations(goCtx, ctx, ruleName)
	if err != nil {
		return nil, err
	}
	now := EpochSeconds(p.tc.Now())
	for _, e := range in.Evaluations {
		// A resubmission for the same resource replaces the previous verdict rather than
		// accumulating beside it: a rule reports one current result per resource, so two
		// contradictory verdicts for one resource is a state AWS never reports.
		replaced := false
		for i := range existing {
			if existing[i].Evaluation.ComplianceResourceType == e.ComplianceResourceType &&
				existing[i].Evaluation.ComplianceResourceId == e.ComplianceResourceId {
				existing[i] = cfgsvcStoredEvaluation{Evaluation: e, RecordedAt: now, ResultToken: *in.ResultToken}
				replaced = true
				break
			}
		}
		if !replaced {
			existing = append(existing, cfgsvcStoredEvaluation{
				Evaluation: e, RecordedAt: now, ResultToken: *in.ResultToken,
			})
		}
	}
	if err := p.cfgsvcPutJSON(goCtx,
		cfgsvcEvaluationsKey(ctx.AccountID, ctx.Region, ruleName), existing); err != nil {
		return nil, err
	}

	// FailedEvaluations is "requests that failed because of a client or server error".
	// Substrate refuses a malformed evaluation outright with
	// InvalidParameterValueException rather than accepting the batch and reporting the
	// bad member here, so the list is empty on success. Partial acceptance would need a
	// rule for which failures are per-member and which are fatal, and the API
	// documents none.
	return cfgsvcJSONResponse(map[string]interface{}{
		"FailedEvaluations": []ConfigEvaluation{},
	}, "putEvaluations")
}

// cfgsvcCheckEvaluation validates one submitted Evaluation.
func cfgsvcCheckEvaluation(e *ConfigEvaluation) error {
	if e.ComplianceResourceType == "" || len(e.ComplianceResourceType) > 256 {
		return cfgsvcInvalidParameter("The ComplianceResourceType is required and may be up to " +
			"256 characters long.")
	}
	if e.ComplianceResourceId == "" || len(e.ComplianceResourceId) > 768 {
		return cfgsvcInvalidParameter("The ComplianceResourceId is required and may be up to " +
			"768 characters long.")
	}
	// INSUFFICIENT_DATA is in the ComplianceType enum but not accepted here: "Config
	// does not accept INSUFFICIENT_DATA as the value for ComplianceType from a
	// PutEvaluations request." Accepting it would let a rule report no-data as a
	// per-resource result, which no AWS response carries.
	if !slices.Contains(cfgsvcEvaluationComplianceTypes, e.ComplianceType) {
		return cfgsvcInvalidParameter("The ComplianceType must be one of COMPLIANT, NON_COMPLIANT " +
			"or NOT_APPLICABLE. Config does not accept INSUFFICIENT_DATA from a PutEvaluations request.")
	}
	if len(e.Annotation) > 256 {
		return cfgsvcInvalidParameter("The Annotation may be up to 256 characters long.")
	}
	if e.OrderingTimestamp.IsZero() {
		return cfgsvcInvalidParameter("The OrderingTimestamp is required.")
	}
	return nil
}

// cfgsvcLoadEvaluations loads the evaluations recorded for one rule.
func (p *ConfigServicePlugin) cfgsvcLoadEvaluations(goCtx context.Context, ctx *RequestContext,
	ruleName string) ([]cfgsvcStoredEvaluation, error) {
	var stored []cfgsvcStoredEvaluation
	if _, err := p.cfgsvcGetJSON(goCtx,
		cfgsvcEvaluationsKey(ctx.AccountID, ctx.Region, ruleName), &stored); err != nil {
		return nil, err
	}
	return stored, nil
}

// --- pagination ---

// cfgsvcPaginate returns one page of ids and the token for the next.
//
// The ceiling is a parameter rather than a constant because AWS Config's page
// ceilings differ per operation: 100 for GetComplianceDetailsByConfigRule and
// ListTagsForResource (the Limit shape), 20 for DescribeConformancePacks and its
// siblings (PageSizeLimit), 1000 for DescribeConformancePackCompliance. A single
// hard-coded ceiling would either refuse a page size AWS accepts or serve one it
// does not.
//
// The token is the opaque encoding of the last ID returned and is empty when the
// listing is exhausted, so a caller looping until the token is empty terminates. An
// unreadable or unknown token is InvalidNextTokenException rather than a silent
// restart: a paginating caller that restarts sees duplicates instead of an error,
// which is the failure mode hardest to notice.
func cfgsvcPaginate(ids []string, nextToken string, limit, ceiling int) (page []string, next string, err error) {
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)

	start := 0
	if nextToken != "" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(nextToken)
		if decodeErr != nil {
			return nil, "", cfgsvcInvalidNextToken()
		}
		found := false
		for i, id := range sorted {
			if id == string(decoded) {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", cfgsvcInvalidNextToken()
		}
	}

	if limit <= 0 || limit > ceiling {
		limit = ceiling
	}
	end := min(start+limit, len(sorted))
	page = sorted[start:end]
	if end < len(sorted) && len(page) > 0 {
		next = base64.StdEncoding.EncodeToString([]byte(page[len(page)-1]))
	}
	return page, next, nil
}

// cfgsvcRuleComplianceDescription describes a seeded rule verdict for a log line.
func cfgsvcRuleComplianceDescription(seed cfgsvcSeededRuleCompliance) string {
	if len(seed.Resources) == 0 {
		return fmt.Sprintf("%s (rule level)", seed.ComplianceType)
	}
	return fmt.Sprintf("%s over %d resource(s)", seed.ComplianceType, len(seed.Resources))
}
