package emulator

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// EventBridge Scheduler's published constraints, as constants so the guard and the message it renders
// cannot drift apart. Every bound here is from the API reference:
//
//   - Name and GroupName: `API_CreateSchedule`'s URI parameter and body member, both length 1–64 with
//     pattern [0-9a-zA-Z-_.]+.
//   - ScheduleExpression: length 1–256, `Required: Yes`.
//   - Description: length 0–512.
//   - Target.Arn and Target.RoleArn: length 1–1600, both `Required: Yes` on `API_Target`.
//   - FlexibleTimeWindow.MaximumWindowInMinutes: `Valid Range` 1–1440 on `API_FlexibleTimeWindow`.
//   - ScheduleExpressionTimezone: length 1–50.
//   - KmsKeyArn: length 1–2048 and the pattern [schedulerKMSKeyARNPattern] holds.
const (
	schedulerTimezoneMaxLength           = 50
	schedulerKMSKeyARNMaxLength          = 2048
	schedulerNameMaxLength               = 64
	schedulerExpressionMaxLength         = 256
	schedulerDescriptionMaxLength        = 512
	schedulerTargetARNMaxLength          = 1600
	schedulerMaximumWindowMinutesMinimum = 1
	schedulerMaximumWindowMinutesMaximum = 1440
)

// schedulerFlexibleTimeWindowModes is `API_FlexibleTimeWindow`'s Mode enum, and schedulerStates is
// `API_CreateSchedule`'s State enum. Both are published as Valid Values, so a value outside them is a
// refusal rather than a passthrough.
var (
	schedulerFlexibleTimeWindowModes = []string{"OFF", "FLEXIBLE"}     //nolint:gochecknoglobals // published enum
	schedulerStates                  = []string{"ENABLED", "DISABLED"} //nolint:gochecknoglobals // published enum
	// schedulerActionsAfterCompletion is the ActionAfterCompletion enum all three schedule pages
	// publish (#1442).
	schedulerActionsAfterCompletion = []string{"NONE", "DELETE"} //nolint:gochecknoglobals // published enum
)

// schedulerKMSKeyARNPattern is KmsKeyArn's published pattern, anchored:
// arn:aws[a-z-]*:kms:[a-z0-9\-]+:\d{12}:(key|alias)\/[0-9a-zA-Z-_]*.
var schedulerKMSKeyARNPattern = regexp.MustCompile( //nolint:gochecknoglobals // immutable compiled pattern
	`^arn:aws[a-z-]*:kms:[a-z0-9\-]+:\d{12}:(key|alias)/[0-9a-zA-Z-_]*$`)

// schedulerValidateKMSKeyARN checks KmsKeyArn against its published length and pattern, treating an
// empty value as absent, since the member is optional.
func schedulerValidateKMSKeyARN(value string) *AWSError {
	if value == "" {
		return nil
	}
	if err := schedulerValidateLength("kmsKeyArn", value, schedulerKMSKeyARNMaxLength); err != nil {
		return err
	}
	if !schedulerKMSKeyARNPattern.MatchString(value) {
		return schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%s' at 'kmsKeyArn' "+
			"failed to satisfy constraint: Member must satisfy regular expression pattern: "+
			"arn:aws[a-z-]*:kms:[a-z0-9\\-]+:\\d{12}:(key|alias)\\/[0-9a-zA-Z-_]*", value))
	}
	return nil
}

// schedulerValidation returns ValidationException/400, the only refusal EventBridge Scheduler publishes
// for an input that fails a constraint.
//
// Every operation page lists ValidationException at HTTP 400 and describes it as "The input fails to
// satisfy the constraints specified by an AWS service", so a missing required member, a length outside
// a published range and a value outside a published enum all land here — the service publishes no
// finer-grained code to distinguish them, which is why the message carries the distinction instead.
func schedulerValidation(message string) *AWSError {
	return &AWSError{Code: "ValidationException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// schedulerInvalidBody returns the refusal for a body that is not JSON at all.
func schedulerInvalidBody() *AWSError {
	return schedulerValidation("invalid request body")
}

// schedulerNotFound returns ResourceNotFoundException/404 for a schedule that does not exist.
func schedulerNotFound(name, groupName string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    fmt.Sprintf("Schedule %s not found in group %s", name, groupName),
		HTTPStatus: http.StatusNotFound,
	}
}

// schedulerConflict returns ConflictException/409 for a create naming a schedule that already exists.
func schedulerConflict(name, groupName string) *AWSError {
	return &AWSError{
		Code:       "ConflictException",
		Message:    fmt.Sprintf("Schedule %s already exists in group %s", name, groupName),
		HTTPStatus: http.StatusConflict,
	}
}

// schedulerRequiredMessage renders the message for an absent required member.
//
// The shape follows [iamEnumMessage]'s: it names the member as the caller spelled it, so a refusal can
// be acted on from the response alone rather than by consulting the reference page.
func schedulerRequiredMessage(member string) string {
	return fmt.Sprintf("1 validation error detected: Value null at '%s' failed to satisfy constraint: "+
		"Member must not be null", member)
}

// schedulerValidateName checks a schedule or group name against the published length and character set.
//
// Both `Name` and `GroupName` publish length 1–64 and pattern [0-9a-zA-Z-_.]+. The pattern is checked
// by hand rather than by a compiled expression because the class is four literal ranges; there is no
// alternation, anchoring or repetition to get wrong.
func schedulerValidateName(member, value string) *AWSError {
	if value == "" {
		return schedulerValidation(schedulerRequiredMessage(member))
	}
	if len(value) > schedulerNameMaxLength {
		return schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed "+
			"to satisfy constraint: Member must have length less than or equal to %d",
			value, member, schedulerNameMaxLength))
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			c == '-', c == '_', c == '.':
		default:
			return schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%s' at '%s' "+
				"failed to satisfy constraint: Member must satisfy regular expression pattern: "+
				"[0-9a-zA-Z-_.]+", value, member))
		}
	}
	return nil
}

// schedulerValidateLength checks a member against a published maximum length.
func schedulerValidateLength(member, value string, maximum int) *AWSError {
	if len(value) <= maximum {
		return nil
	}
	return schedulerValidation(fmt.Sprintf("1 validation error detected: Value at '%s' failed to "+
		"satisfy constraint: Member must have length less than or equal to %d", member, maximum))
}

// schedulerValidateEnum checks a member against a published Valid Values set, treating an empty value
// as absent — every enum member Scheduler publishes is optional, and the plugin substitutes its own
// default for an absent one.
func schedulerValidateEnum(member, value string, allowed []string) *AWSError {
	if value == "" {
		return nil
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return schedulerValidation(iamEnumMessage(member, value, allowed))
}

// schedulerValidateTarget checks the target's two required members and their published lengths.
//
// `API_Target` marks `Arn` and `RoleArn` `Required: Yes`. `RoleArn` also publishes an IAM-role ARN
// pattern, which is *not* checked: substrate does not model the role, so refusing a string that is not
// an ARN would refuse a call this emulator otherwise serves without ever needing the value to resolve.
// That omission is recorded rather than silently taken (#1008).
func schedulerValidateTarget(target *schedulerTargetInput) *AWSError {
	if target == nil {
		return schedulerValidation(schedulerRequiredMessage("target"))
	}
	if target.Arn == "" {
		return schedulerValidation(schedulerRequiredMessage("target.arn"))
	}
	if err := schedulerValidateLength("target.arn", target.Arn, schedulerTargetARNMaxLength); err != nil {
		return err
	}
	if target.RoleArn == "" {
		return schedulerValidation(schedulerRequiredMessage("target.roleArn"))
	}
	if err := schedulerValidateLength("target.roleArn", target.RoleArn, schedulerTargetARNMaxLength); err != nil {
		return err
	}
	return schedulerValidateTargetParameters(target)
}

// schedulerDeadLetterARNPattern is `API_DeadLetterConfig`'s published Arn pattern.
var schedulerDeadLetterARNPattern = regexp.MustCompile( //nolint:gochecknoglobals // immutable compiled pattern
	`^arn:aws[a-z-]*:sqs:[a-z0-9\-]+:\d{12}:[a-zA-Z0-9\-_]+$`)

// schedulerValidateTargetParameters checks the three target objects substrate decodes since #1501
// against their published constraints: DeadLetterConfig.Arn's length (1–1600) and pattern,
// EventBridgeParameters' two Required: Yes members (DetailType 1–128, Source 1–256), and
// SqsParameters.MessageGroupId's length (1–128).
func schedulerValidateTargetParameters(target *schedulerTargetInput) *AWSError {
	if d := target.DeadLetterConfig; d != nil && d.Arn != "" {
		if err := schedulerValidateLength("target.deadLetterConfig.arn", d.Arn, schedulerTargetARNMaxLength); err != nil {
			return err
		}
		if !schedulerDeadLetterARNPattern.MatchString(d.Arn) {
			return schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%s' at "+
				"'target.deadLetterConfig.arn' failed to satisfy constraint: Member must satisfy regular "+
				`expression pattern: arn:aws[a-z-]*:sqs:[a-z0-9\-]+:\d{12}:[a-zA-Z0-9\-_]+`, d.Arn))
		}
	}
	if e := target.EventBridgeParameters; e != nil {
		if e.DetailType == "" {
			return schedulerValidation(schedulerRequiredMessage("target.eventBridgeParameters.detailType"))
		}
		if err := schedulerValidateLength("target.eventBridgeParameters.detailType", e.DetailType, 128); err != nil {
			return err
		}
		if e.Source == "" {
			return schedulerValidation(schedulerRequiredMessage("target.eventBridgeParameters.source"))
		}
		if err := schedulerValidateLength("target.eventBridgeParameters.source", e.Source, 256); err != nil {
			return err
		}
	}
	if s := target.SqsParameters; s != nil {
		if err := schedulerValidateLength("target.sqsParameters.messageGroupId", s.MessageGroupID, 128); err != nil {
			return err
		}
	}
	return schedulerValidateRetryPolicy(target.RetryPolicy)
}

// schedulerValidateRetryPolicy checks RetryPolicy's two members against the Valid Ranges
// `API_RetryPolicy` publishes (#1514): MaximumEventAgeInSeconds 60–86400 and MaximumRetryAttempts
// 0–185. Both are `Required: No`, so an absent member is not checked. The message follows the form
// MaximumWindowInMinutes' range check uses; the wording is substrate's own.
func schedulerValidateRetryPolicy(rp *schedulerRetryPolicyInput) *AWSError {
	if rp == nil {
		return nil
	}
	for _, m := range []struct {
		member string
		value  *int32
		lo, hi int32
	}{
		{"target.retryPolicy.maximumEventAgeInSeconds", rp.MaximumEventAgeInSeconds, 60, 86400},
		{"target.retryPolicy.maximumRetryAttempts", rp.MaximumRetryAttempts, 0, 185},
	} {
		if m.value == nil {
			continue
		}
		if n := *m.value; n < m.lo || n > m.hi {
			return schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%d' at '%s' failed "+
				"to satisfy constraint: Member must be between %d and %d", n, m.member, m.lo, m.hi))
		}
	}
	return nil
}

// schedulerValidateEvaluable refuses a ScheduleExpression substrate cannot evaluate, or a
// ScheduleExpressionTimezone that is not an IANA zone, with ValidationException/400 (#1514).
//
// Until #1514 such a schedule was stored, reported as sent, and never fired. AWS refuses it with
// ValidationException, the code `API_CreateSchedule` and `API_UpdateSchedule` publish for input that
// "fails to satisfy the constraints", but no page publishes which inputs or the message. The parser is
// the one the clock evaluates schedules with (scheduler_expression.go), which accepts the three forms
// and the cron wildcards the API Reference and the User Guide publish, so what it refuses is what the
// clock could never fire. The messages are substrate's own.
func schedulerValidateEvaluable(expr, tz string) *AWSError {
	if _, err := schedLocation(tz); err != nil {
		return schedulerValidation(fmt.Sprintf("Invalid ScheduleExpressionTimezone %q: it is not an IANA time zone.", tz))
	}
	if _, err := parseSchedExpression(expr, tz); err != nil {
		return schedulerValidation(fmt.Sprintf("Invalid ScheduleExpression %q.", expr))
	}
	return nil
}

// schedulerValidateFlexibleTimeWindow checks the window's required mode and its optional bound.
//
// `Mode` is `Required: Yes` with Valid Values OFF | FLEXIBLE, and `MaximumWindowInMinutes` publishes a
// Valid Range of 1–1440 while remaining `Required: No`. The distinction between an absent value and an
// explicit zero is why the request member is a pointer: zero is outside the published range and is
// refused, where absent is accepted. AWS does *not* publish that a FLEXIBLE window requires the bound,
// so substrate does not invent that rule — only what the page states.
func schedulerValidateFlexibleTimeWindow(window *schedulerFlexibleTimeWindowInput) *AWSError {
	if window == nil {
		return schedulerValidation(schedulerRequiredMessage("flexibleTimeWindow"))
	}
	if window.Mode == "" {
		return schedulerValidation(schedulerRequiredMessage("flexibleTimeWindow.mode"))
	}
	if err := schedulerValidateEnum("flexibleTimeWindow.mode", window.Mode,
		schedulerFlexibleTimeWindowModes); err != nil {
		return err
	}
	if window.MaximumWindowInMinutes == nil {
		return nil
	}
	if n := *window.MaximumWindowInMinutes; n < schedulerMaximumWindowMinutesMinimum ||
		n > schedulerMaximumWindowMinutesMaximum {
		return schedulerValidation(fmt.Sprintf("1 validation error detected: Value '%d' at "+
			"'flexibleTimeWindow.maximumWindowInMinutes' failed to satisfy constraint: Member must be "+
			"between %d and %d", n, schedulerMaximumWindowMinutesMinimum,
			schedulerMaximumWindowMinutesMaximum))
	}
	return nil
}

// schedulerValidateScheduleInput checks every published constraint on a CreateSchedule or
// UpdateSchedule body, in the order a caller reads them off the page: the URI parameter first, then the
// three required body members, then the optional ones.
//
// Both operations publish the same three `Required: Yes` body members — `ScheduleExpression`, `Target`
// and `FlexibleTimeWindow` — so one validator serves both, which is also why UpdateSchedule's merge of
// the *optional* members is a separate divergence rather than something this guard hides: after this,
// the only members an update can leave unstated are ones the page marks optional.
//
// StartDate, EndDate, KmsKeyArn and ActionAfterCompletion are published and unmodelled: substrate
// decodes none of them, so it validates none of them, and #1013's rule keeps them out of the response
// as well.
func schedulerValidateScheduleInput(name string, in *schedulerScheduleInput) *AWSError {
	if err := schedulerValidateName("name", name); err != nil {
		return err
	}
	if in.GroupName != "" {
		if err := schedulerValidateName("groupName", in.GroupName); err != nil {
			return err
		}
	}
	if strings.TrimSpace(in.ScheduleExpression) == "" {
		return schedulerValidation(schedulerRequiredMessage("scheduleExpression"))
	}
	if err := schedulerValidateLength("scheduleExpression", in.ScheduleExpression,
		schedulerExpressionMaxLength); err != nil {
		return err
	}
	if err := schedulerValidateTarget(in.Target); err != nil {
		return err
	}
	if err := schedulerValidateFlexibleTimeWindow(in.FlexibleTimeWindow); err != nil {
		return err
	}
	if err := schedulerValidateLength("description", in.Description,
		schedulerDescriptionMaxLength); err != nil {
		return err
	}
	if err := schedulerValidateLength("scheduleExpressionTimezone", in.ScheduleExpressionTimezone,
		schedulerTimezoneMaxLength); err != nil {
		return err
	}
	if err := schedulerValidateEvaluable(in.ScheduleExpression, in.ScheduleExpressionTimezone); err != nil {
		return err
	}
	if err := schedulerValidateEnum("actionAfterCompletion", in.ActionAfterCompletion,
		schedulerActionsAfterCompletion); err != nil {
		return err
	}
	if err := schedulerValidateKMSKeyARN(in.KmsKeyArn); err != nil {
		return err
	}
	return schedulerValidateEnum("state", in.State, schedulerStates)
}
