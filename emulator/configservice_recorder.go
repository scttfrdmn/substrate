package emulator

import (
	"context"
	"regexp"
	"slices"
	"strings"
)

// The configuration-recorder cluster (#580).
//
// This is the point of the release. AWS Config has two operations that look like
// they answer the same question and do not:
//
//	DescribeConfigurationRecorders       — does a recorder exist?
//	DescribeConfigurationRecorderStatus  — is it recording?
//
// A recorder created and never started exists, is returned by the first call in
// full, and records nothing. That is a real and common misconfiguration, and a
// consumer that checks only the first call reports an account as covered when it is
// not. Substrate stores the two answers separately so the difference is observable:
// PutConfigurationRecorder always leaves Recording false, and only
// StartConfigurationRecorder sets it true.
//
// One recorder per account per Region. AWS documents this per operation rather than
// on the service-limits page — there is no recorder maximum listed there — and the
// note is what makes MaxNumberOfConfigurationRecordersExceededException reachable
// at a count of two. State is therefore keyed by account and Region with no name
// component: a second recorder under a different name is refused rather than
// stored.

// cfgsvcMaxRecorders is the number of configuration recorders AWS permits per
// account per Region. See the cluster comment: this comes from
// PutConfigurationRecorder's own "you can have only one" note, not from the
// service-limits page.
const cfgsvcMaxRecorders = 1

// cfgsvcResourceTypePattern is the shape a RecordingGroup resource type must have.
//
// The API model's ResourceType enum has 533 members and grows with every AWS
// service launch. Substrate checks the *shape* rather than membership, on the same
// reasoning as the attach-time policy-ARN check (#499): a vendored enum goes stale,
// and refusing a resource type that AWS has since added would fail a request that
// succeeds against AWS — a wrong refusal breaks working consumer code, which is
// worse than accepting a typo. A value that is not AWS::Service::Type shaped is a
// typo in any vintage of the enum, and that is what this catches.
var cfgsvcResourceTypePattern = regexp.MustCompile(`^AWS::[A-Za-z0-9]+::[A-Za-z0-9]+$`)

// cfgsvcServicePrincipalPattern is the ServicePrincipal shape, from the model
// (1-128 characters of [\w+=,.@-]).
var cfgsvcServicePrincipalPattern = regexp.MustCompile(`^[\w+=,.@-]+$`)

// ConfigTag is the Tag shape AWS Config uses in a Put's TagsList.
type ConfigTag struct {
	// Key is the tag key.
	Key string `json:"Key"`

	// Value is the tag value, which may be empty but not null.
	Value string `json:"Value"`
}

// cfgsvcPutRecorderRequest is PutConfigurationRecorderRequest.
//
// Note the case asymmetry: the request's own members are UpperCamel while the
// nested ConfigurationRecorder's are lowerCamel (name, roleARN, recordingGroup).
// That is the API model's, and a shape that "corrected" it would not decode what an
// SDK sends.
type cfgsvcPutRecorderRequest struct {
	// ConfigurationRecorder is the recorder to create or update, required.
	ConfigurationRecorder *ConfigRecorder `json:"ConfigurationRecorder"`

	// Tags are applied at creation only, per the operation's own note.
	Tags []ConfigTag `json:"Tags"`
}

// cfgsvcDescribeRecordersRequest is the input shared by
// DescribeConfigurationRecorders and DescribeConfigurationRecorderStatus: both take
// the same three filters and neither paginates.
type cfgsvcDescribeRecordersRequest struct {
	// ConfigurationRecorderNames selects recorders by name. More than one is a
	// ValidationException, because an account has at most one recorder.
	ConfigurationRecorderNames []string `json:"ConfigurationRecorderNames"`

	// ServicePrincipal selects the service-linked recorder for a service.
	ServicePrincipal string `json:"ServicePrincipal"`

	// Arn selects a recorder by ARN.
	Arn string `json:"Arn"`
}

// cfgsvcRecorderNameRequest is the input to StartConfigurationRecorder,
// StopConfigurationRecorder and DeleteConfigurationRecorder, all of which take only
// the name.
type cfgsvcRecorderNameRequest struct {
	// ConfigurationRecorderName names the recorder, required.
	ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
}

// recorderOperation claims the configuration-recorder operations.
func (p *ConfigServicePlugin) recorderOperation(op string) (cfgsvcHandler, bool) {
	switch op {
	case "PutConfigurationRecorder":
		return p.putConfigurationRecorder, true
	case "DescribeConfigurationRecorders":
		return p.describeConfigurationRecorders, true
	case "DescribeConfigurationRecorderStatus":
		return p.describeConfigurationRecorderStatus, true
	case "StartConfigurationRecorder":
		return p.startConfigurationRecorder, true
	case "StopConfigurationRecorder":
		return p.stopConfigurationRecorder, true
	case "DeleteConfigurationRecorder":
		return p.deleteConfigurationRecorder, true
	}
	return nil, false
}

// putConfigurationRecorder creates or updates the account's configuration
// recorder.
//
// The operation is idempotent by name: a second call with the same name updates
// the role and recording group and does **not** create a second recorder. A second
// call with a *different* name is refused, because that would be a second recorder.
// Tags are not touched by an update — "the tags for a configuration recorder are
// added at creation and are not updated with configuration recorder updates" — so a
// consumer that expects a Put to retag has a bug substrate reports rather than
// hides.
func (p *ConfigServicePlugin) putConfigurationRecorder(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPutRecorderRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if in.ConfigurationRecorder == nil {
		return nil, cfgsvcValidation("ConfigurationRecorder is required.")
	}

	recorder := *in.ConfigurationRecorder
	if recorder.Name == "" {
		recorder.Name = cfgsvcDefaultName
	}
	if err := cfgsvcCheckRecorderName(recorder.Name); err != nil {
		return nil, err
	}
	// A null or empty roleARN is InvalidRoleException, which is a *presence* check
	// and nothing more: the exception's own text is "You have provided a null or
	// empty Amazon Resource Name (ARN) for the IAM role assumed by Config". Substrate
	// deliberately does not check that the role exists or is assumable — #580
	// described this exception as an assumability check, and modeling it that way
	// would refuse requests AWS accepts. The requirement itself is a documented
	// divergence from the model, which marks roleARN optional: "While the API model
	// does not require this field, the server will reject a request without a defined
	// roleARN."
	if strings.TrimSpace(recorder.RoleARN) == "" {
		return nil, cfgsvcErr("InvalidRoleException",
			"You have provided a null or empty Amazon Resource Name (ARN) for the IAM role "+
				"assumed by Config and used by the customer managed configuration recorder.")
	}
	if err := cfgsvcCheckRecordingGroup(recorder.RecordingGroup); err != nil {
		return nil, err
	}
	if recorder.RecordingGroup == nil {
		recorder.RecordingGroup = cfgsvcDefaultRecordingGroup()
	}

	goCtx := context.Background()
	key := cfgsvcRecorderKey(ctx.AccountID, ctx.Region)
	var existing ConfigRecorder
	found, err := p.cfgsvcGetJSON(goCtx, key, &existing)
	if err != nil {
		return nil, err
	}
	if found && existing.Name != recorder.Name {
		return nil, cfgsvcErr("MaxNumberOfConfigurationRecordersExceededException",
			"You have reached the limit of the number of configuration recorders you can create.")
	}

	recorder.ARN = cfgsvcRecorderARN(ctx, recorder.Name)
	if err := p.cfgsvcPutJSON(goCtx, key, recorder); err != nil {
		return nil, err
	}

	// The status record is created alongside a new recorder with Recording false —
	// the whole point of the cluster — and left alone on an update, so a Put against a
	// running recorder does not silently stop it.
	if !found {
		status := ConfigRecorderStatus{ARN: recorder.ARN, Name: recorder.Name, Recording: false}
		if err := p.cfgsvcPutJSON(goCtx, cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region), status); err != nil {
			return nil, err
		}
		tags, err := cfgsvcTagsToMap(in.Tags)
		if err != nil {
			return nil, err
		}
		if err := p.cfgsvcSaveTags(goCtx, recorder.ARN, tags); err != nil {
			return nil, err
		}
	}

	return cfgsvcEmptyResponse(), nil
}

// cfgsvcCheckRecorderName validates a recorder name against RecorderName's bounds
// and the reserved service-linked prefix.
func cfgsvcCheckRecorderName(name string) error {
	if len(name) > cfgsvcMaxNameLen {
		return cfgsvcValidation("The configuration recorder name must be between 1 and 256 characters long.")
	}
	// AWS reserves this prefix for recorders created through
	// PutServiceLinkedConfigurationRecorder, which substrate does not model. The
	// refusal still matters: a consumer that picks the prefix by accident gets the
	// same error it would from AWS rather than a recorder AWS would not have made.
	if strings.HasPrefix(name, cfgsvcServiceLinkedNamePrefix) {
		return cfgsvcErr("InvalidConfigurationRecorderNameException",
			"The configuration recorder name is not valid. The prefix "+
				`"AWSConfigurationRecorderFor" is reserved for service-linked configuration recorders.`)
	}
	return nil
}

// cfgsvcDefaultRecordingGroup is the group a Put gets when it specifies none: all
// supported resource types, excluding the global IAM types.
//
// The exclusion is expressed the way AWS expresses it — allSupported true with
// includeGlobalResourceTypes false, that field being "a bundle which only applies to
// the global IAM resource types: IAM users, groups, roles, and customer managed
// policies". It is deliberately *not* expressed by synthesizing an
// exclusionByResourceTypes list naming the four types: AWS does not return one for an
// allSupported group, and an exclusion list paired with ALL_SUPPORTED_RESOURCE_TYPES
// is the combination the recordingStrategy documentation treats as contradictory —
// exclusionByResourceTypes is what EXCLUSION_BY_RESOURCE_TYPES reads. Emitting it
// would be substrate inventing a response member, and a consumer asserting against
// its own recorder would be asserting against substrate rather than against AWS.
func cfgsvcDefaultRecordingGroup() *ConfigRecordingGroup {
	return &ConfigRecordingGroup{
		AllSupported:               true,
		IncludeGlobalResourceTypes: false,
		RecordingStrategy:          &ConfigRecordingStrategy{UseOnly: cfgsvcStrategyAllSupported},
	}
}

// cfgsvcCheckRecordingGroup implements InvalidRecordingGroupException.
//
// The reference enumerates the cases, and these are them in order. A nil group is
// valid — it means "use the default" — so only a group that was supplied is checked.
//
// The one enumerated case not implemented is "you have provided too many resource
// types": no ceiling is documented anywhere, and inventing a number would refuse a
// request AWS accepts.
func cfgsvcCheckRecordingGroup(g *ConfigRecordingGroup) error {
	if g == nil {
		return nil
	}
	const invalid = "InvalidRecordingGroupException"

	strategy := ""
	if g.RecordingStrategy != nil {
		strategy = g.RecordingStrategy.UseOnly
	}
	var excluded []string
	if g.ExclusionByResourceTypes != nil {
		excluded = g.ExclusionByResourceTypes.ResourceTypes
	}

	if g.AllSupported && len(g.ResourceTypes) > 0 {
		return cfgsvcErr(invalid, "You have provided a configuration recorder that is not valid: "+
			"allSupported is set to true and resourceTypes is not empty. If allSupported is set to "+
			"true, do not provide a list of resource types.")
	}
	if g.AllSupported && strategy == cfgsvcStrategyExclusion {
		return cfgsvcErr(invalid, "You have provided a configuration recorder that is not valid: "+
			"allSupported is set to true and recordingStrategy is set to "+
			"EXCLUSION_BY_RESOURCE_TYPES. These settings conflict.")
	}
	if !g.AllSupported && !g.IncludeGlobalResourceTypes &&
		len(g.ResourceTypes) == 0 && len(excluded) == 0 && strategy == "" {
		return cfgsvcErr(invalid, "You have provided a configuration recorder that is not valid: "+
			"all of the recording group parameters are either null, false, or empty.")
	}
	if strategy != "" && !slices.Contains(cfgsvcRecordingStrategies, strategy) {
		return cfgsvcErr(invalid, "You have provided a configuration recorder that is not valid: "+
			"the recording strategy "+strategy+" is not a valid recording strategy.")
	}
	for _, list := range [][]string{g.ResourceTypes, excluded} {
		for _, rt := range list {
			if !cfgsvcResourceTypePattern.MatchString(rt) {
				return cfgsvcErr(invalid, "You have provided a configuration recorder that is not "+
					"valid: the resource type "+rt+" is not a valid resource type.")
			}
		}
	}
	return nil
}

// cfgsvcTagsToMap folds a TagsList into the map substrate stores, refusing a list
// longer than the documented 50.
func cfgsvcTagsToMap(tags []ConfigTag) (map[string]string, error) {
	if len(tags) > cfgsvcMaxTags {
		return nil, cfgsvcValidation("You can associate up to 50 tags with a resource.")
	}
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		if err := cfgsvcCheckTag(tag.Key, tag.Value); err != nil {
			return nil, err
		}
		out[tag.Key] = tag.Value
	}
	return out, nil
}

// describeConfigurationRecorders reports the account's recorder — whether or not it
// is recording, which is what makes DescribeConfigurationRecorderStatus a separate
// call and not a redundant one.
func (p *ConfigServicePlugin) describeConfigurationRecorders(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	in, recorder, found, err := p.cfgsvcResolveDescribed(ctx, req)
	if err != nil {
		return nil, err
	}
	recorders := []ConfigRecorder{}
	if found {
		// A service-principal filter selects a service-linked recorder, and substrate
		// models none — so a syntactically valid principal matches nothing rather than
		// matching the customer managed recorder, which is not what the caller asked for.
		if in.ServicePrincipal == "" {
			recorders = append(recorders, *recorder)
		}
	}
	return cfgsvcJSONResponse(map[string]interface{}{
		"ConfigurationRecorders": recorders,
	}, "describeConfigurationRecorders")
}

// describeConfigurationRecorderStatus reports whether the recorder is recording.
//
// This is the only operation that can answer that question, and a consumer reading
// only DescribeConfigurationRecorders has not asked it.
func (p *ConfigServicePlugin) describeConfigurationRecorderStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	in, _, found, err := p.cfgsvcResolveDescribed(ctx, req)
	if err != nil {
		return nil, err
	}
	statuses := []ConfigRecorderStatus{}
	if found && in.ServicePrincipal == "" {
		status, err := p.cfgsvcRecorderStatus(ctx)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, *status)
	}
	return cfgsvcJSONResponse(map[string]interface{}{
		"ConfigurationRecordersStatus": statuses,
	}, "describeConfigurationRecorderStatus")
}

// cfgsvcResolveDescribed decodes and applies the filters both describe operations
// share, so the two cannot disagree about which recorder a request names.
//
// An absent recorder is reported as found=false rather than as an error when the
// request carries no name: "no recorder yet" is the state a fresh account is in, and
// an empty list is what AWS returns for it. A request that *names* a recorder that
// does not exist is NoSuchConfigurationRecorderException, because the caller
// asserted the name.
func (p *ConfigServicePlugin) cfgsvcResolveDescribed(ctx *RequestContext, req *AWSRequest) (
	*cfgsvcDescribeRecordersRequest, *ConfigRecorder, bool, error) {
	var in cfgsvcDescribeRecordersRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, nil, false, err
	}
	if len(in.ConfigurationRecorderNames) > cfgsvcMaxRecorders {
		return nil, nil, false, cfgsvcValidation("You have specified more than one configuration recorder.")
	}
	if in.ServicePrincipal != "" &&
		(len(in.ServicePrincipal) > 128 || !cfgsvcServicePrincipalPattern.MatchString(in.ServicePrincipal)) {
		return nil, nil, false, cfgsvcValidation(
			"You have provided a service principal for service-linked configuration recorder that is not valid.")
	}

	var recorder ConfigRecorder
	found, err := p.cfgsvcGetJSON(context.Background(), cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder)
	if err != nil {
		return nil, nil, false, err
	}

	if len(in.ConfigurationRecorderNames) == 1 {
		if !found || recorder.Name != in.ConfigurationRecorderNames[0] {
			return nil, nil, false, cfgsvcNoSuchRecorder()
		}
	}
	if in.Arn != "" {
		if !found || recorder.ARN != in.Arn {
			return nil, nil, false, cfgsvcNoSuchRecorder()
		}
	}
	return &in, &recorder, found, nil
}

// cfgsvcNoSuchRecorder is NoSuchConfigurationRecorderException, at 400 like every
// other Config exception — including this one, whose reference page says 400 even
// though it reports a missing entity.
func cfgsvcNoSuchRecorder() *AWSError {
	return cfgsvcErr("NoSuchConfigurationRecorderException",
		"You have specified a configuration recorder that does not exist.")
}

// startConfigurationRecorder starts recording.
//
// The delivery-channel guard is the interesting part: "You must have created a
// delivery channel to successfully start the customer managed configuration
// recorder." A consumer that creates a recorder and starts it without a channel has
// an ordering bug, and without this refusal the emulator would report a recording
// recorder that AWS would not have started.
//
// Starting an already-recording recorder is a no-op at 200. No refusal is
// documented for it, and inventing one would fail a retry that AWS accepts.
func (p *ConfigServicePlugin) startConfigurationRecorder(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	status, err := p.cfgsvcNamedRecorderStatus(ctx, req)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	var channel ConfigDeliveryChannel
	hasChannel, err := p.cfgsvcGetJSON(goCtx, cfgsvcChannelKey(ctx.AccountID, ctx.Region), &channel)
	if err != nil {
		return nil, err
	}
	if !hasChannel {
		return nil, cfgsvcErr("NoAvailableDeliveryChannelException",
			"There is no delivery channel available to record configurations.")
	}

	if !status.Recording {
		now := EpochSeconds(p.tc.Now())
		status.Recording = true
		status.LastStartTime = now
		status.LastStatusChangeTime = now
		// Substrate has nothing that can fail a delivery, so a started recorder reports
		// Success. A consumer that needs to exercise a failing recorder seeds it; see
		// configservice_control.go.
		status.LastStatus = cfgsvcRecorderStatusSuccess
		if err := p.cfgsvcPutJSON(goCtx, cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region), *status); err != nil {
			return nil, err
		}
	}
	return cfgsvcEmptyResponse(), nil
}

// stopConfigurationRecorder stops recording. Stopping an already-stopped recorder
// is a no-op at 200, for the same reason starting a running one is.
func (p *ConfigServicePlugin) stopConfigurationRecorder(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	status, err := p.cfgsvcNamedRecorderStatus(ctx, req)
	if err != nil {
		return nil, err
	}
	if status.Recording {
		now := EpochSeconds(p.tc.Now())
		status.Recording = false
		status.LastStopTime = now
		status.LastStatusChangeTime = now
		if err := p.cfgsvcPutJSON(context.Background(),
			cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region), *status); err != nil {
			return nil, err
		}
	}
	return cfgsvcEmptyResponse(), nil
}

// deleteConfigurationRecorder deletes the recorder and its status and tags.
//
// Deleting a recorder while it is recording is permitted — no refusal is documented
// — and it does not delete the delivery channel, so a fixture that tears down and
// rebuilds must delete both.
func (p *ConfigServicePlugin) deleteConfigurationRecorder(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcRecorderNameRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	var recorder ConfigRecorder
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder)
	if err != nil {
		return nil, err
	}
	if !found || recorder.Name != in.ConfigurationRecorderName {
		return nil, cfgsvcNoSuchRecorder()
	}

	// Tags go with the resource: "if you delete a resource, tags for the resource are
	// deleted as well". Leaving them would make a rebuilt recorder inherit the tags of
	// its predecessor, which no AWS account does.
	for _, key := range []string{
		cfgsvcRecorderKey(ctx.AccountID, ctx.Region),
		cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region),
		cfgsvcTagsKey(recorder.ARN),
	} {
		if err := p.cfgsvcDeleteKey(goCtx, key); err != nil {
			return nil, err
		}
	}
	return cfgsvcEmptyResponse(), nil
}

// cfgsvcNamedRecorderStatus resolves the status record for a name-only request,
// refusing a name that is not the account's recorder.
func (p *ConfigServicePlugin) cfgsvcNamedRecorderStatus(ctx *RequestContext, req *AWSRequest) (
	*ConfigRecorderStatus, error) {
	var in cfgsvcRecorderNameRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	var recorder ConfigRecorder
	found, err := p.cfgsvcGetJSON(context.Background(),
		cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder)
	if err != nil {
		return nil, err
	}
	if !found || recorder.Name != in.ConfigurationRecorderName {
		return nil, cfgsvcNoSuchRecorder()
	}
	return p.cfgsvcRecorderStatus(ctx)
}

// cfgsvcRecorderStatus loads the recorder's status with any seeded values applied.
//
// The seed is applied at read time rather than written into state, so clearing it
// restores the real status rather than leaving a seeded value behind — the same
// reasoning as the account Region-opt seed.
func (p *ConfigServicePlugin) cfgsvcRecorderStatus(ctx *RequestContext) (*ConfigRecorderStatus, error) {
	goCtx := context.Background()
	var status ConfigRecorderStatus
	found, err := p.cfgsvcGetJSON(goCtx, cfgsvcRecorderStatusKey(ctx.AccountID, ctx.Region), &status)
	if err != nil {
		return nil, err
	}
	if !found {
		// A recorder with no status record predates the status write or was written
		// directly into state; report it as existing and not recording rather than
		// failing, because "exists but not recording" is a real state and an error is not.
		var recorder ConfigRecorder
		if _, err := p.cfgsvcGetJSON(goCtx, cfgsvcRecorderKey(ctx.AccountID, ctx.Region), &recorder); err != nil {
			return nil, err
		}
		status = ConfigRecorderStatus{ARN: recorder.ARN, Name: recorder.Name}
	}

	seed, seeded, err := p.seededRecorderStatus(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	if seeded {
		status.LastStatus = seed.LastStatus
		status.LastErrorCode = seed.LastErrorCode
		status.LastErrorMessage = seed.LastErrorMessage
		if status.LastStatusChangeTime.IsZero() {
			status.LastStatusChangeTime = EpochSeconds(p.tc.Now())
		}
	}
	return &status, nil
}
