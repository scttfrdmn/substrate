package emulator

// The FIS actions and target resource types substrate knows (#1619): ListActions, GetAction,
// ListTargetResourceTypes and GetTargetResourceType answer from them, and CreateExperimentTemplate
// refuses an action ID or resource type outside them.
//
// The list is deliberately small. Each entry is from the AWS FIS Actions reference
// (fis/latest/userguide/fis-actions-reference.html): its ID, its target key and resource type, and
// its parameters with their published ranges. Four actions change EC2 state (see fis_clock.go);
// the rest are recorded and complete as no-ops, after their `duration` when they take one. The
// descriptions are substrate's own except stop-instances' and its startInstancesAfterDuration
// parameter's, which are the CLI get-action sample's. The resource types' descriptions are the
// FIS user guide's Targets page ("aws:ec2:instance – An Amazon EC2 instance").

import (
	"sort"
	"strings"
	"time"
)

// The action IDs whose effects substrate models.
const (
	fisActionStopInstances      = "aws:ec2:stop-instances"
	fisActionTerminateInstances = "aws:ec2:terminate-instances"
	fisActionRebootInstances    = "aws:ec2:reboot-instances"
	fisActionSpotInterruptions  = "aws:ec2:send-spot-instance-interruptions"
	fisActionWait               = "aws:fis:wait"
)

// The target resource types the known actions take.
const (
	fisResourceEC2Instance  = "aws:ec2:instance"
	fisResourceSpotInstance = "aws:ec2:spot-instance"
	fisResourceIAMRole      = "aws:iam:role"
	fisResourceASG          = "aws:ec2:autoscaling-group"
)

// fisParamSpec is one action parameter: its description, whether it is required, and for a
// duration its published range.
type fisParamSpec struct {
	description string
	required    bool
	minDuration time.Duration
	maxDuration time.Duration
}

// fisActionSpec is one known action.
type fisActionSpec struct {
	id          string
	description string
	// targetKey is the action's target key, e.g. Instances, and resourceType the type it takes;
	// both are empty for an action that takes no target.
	targetKey    string
	resourceType string
	params       map[string]fisParamSpec
}

// fisDurationParam is a duration parameter with the 1-minute to 12-hour range most publish.
func fisDurationParam(description string, required bool) fisParamSpec {
	return fisParamSpec{description: description, required: required, minDuration: time.Minute, maxDuration: 12 * time.Hour}
}

// fisAPIErrorParams are the parameters of the three aws:fis:inject-api-* actions.
func fisAPIErrorParams() map[string]fisParamSpec {
	return map[string]fisParamSpec{
		"duration":   fisDurationParam("The duration, from one minute to 12 hours (ISO 8601 duration).", true),
		"service":    {description: "The target AWS API namespace.", required: true},
		"percentage": {description: "The percentage (1-100) of calls to inject the fault into.", required: true},
		"operations": {description: "The operations to inject the fault into, separated using commas.", required: true},
	}
}

// fisCapacityErrorParams are the parameters of the two insufficient-capacity actions.
func fisCapacityErrorParams(percentageRequired bool) map[string]fisParamSpec {
	return map[string]fisParamSpec{
		"duration":                    {description: "The duration of the action (ISO 8601 duration).", required: true},
		"availabilityZoneIdentifiers": {description: "The comma separated list of Availability Zones.", required: true},
		"percentage":                  {description: "The percentage (1-100) of calls to inject the fault into.", required: percentageRequired},
	}
}

// fisKnownActions returns the known actions by ID. It is a function rather than a package
// variable so no caller can mutate a shared table.
func fisKnownActions() map[string]fisActionSpec {
	specs := []fisActionSpec{
		{id: fisActionRebootInstances, description: "Reboot the specified EC2 instances.",
			targetKey: "Instances", resourceType: fisResourceEC2Instance, params: map[string]fisParamSpec{}},
		{id: fisActionStopInstances, description: "Stop the specified EC2 instances.",
			targetKey: "Instances", resourceType: fisResourceEC2Instance, params: map[string]fisParamSpec{
				"startInstancesAfterDuration": fisDurationParam(
					"The time to wait before restarting the instances (ISO 8601 duration).", false),
				"completeIfInstancesTerminated": {description: "Whether the action completes when a targeted instance " +
					"was terminated outside the experiment and cannot be restarted."},
				"skipOsShutdown": {description: "Whether to bypass the graceful operating system shutdown."},
			}},
		{id: fisActionTerminateInstances, description: "Terminate the specified EC2 instances.",
			targetKey: "Instances", resourceType: fisResourceEC2Instance, params: map[string]fisParamSpec{
				"skipOsShutdown": {description: "Whether to bypass the graceful operating system shutdown."},
			}},
		{id: fisActionSpotInterruptions, description: "Send a Spot Instance interruption to the specified Spot Instances.",
			targetKey: "SpotInstances", resourceType: fisResourceSpotInstance, params: map[string]fisParamSpec{
				"durationBeforeInterruption": {
					description: "The time to wait before interrupting the instance, from 2 to 15 minutes (ISO 8601 duration).",
					required:    true, minDuration: 2 * time.Minute, maxDuration: 15 * time.Minute,
				},
			}},
		{id: fisActionWait, description: "Wait for the specified duration.", params: map[string]fisParamSpec{
			"duration": fisDurationParam("The duration, from one minute to 12 hours (ISO 8601 duration).", true),
		}},
		{id: "aws:fis:inject-api-internal-error", description: "Inject internal errors into requests made by the target IAM role.",
			targetKey: "Roles", resourceType: fisResourceIAMRole, params: fisAPIErrorParams()},
		{id: "aws:fis:inject-api-throttle-error", description: "Inject throttling errors into requests made by the target IAM role.",
			targetKey: "Roles", resourceType: fisResourceIAMRole, params: fisAPIErrorParams()},
		{id: "aws:fis:inject-api-unavailable-error", description: "Inject unavailable errors into requests made by the target IAM role.",
			targetKey: "Roles", resourceType: fisResourceIAMRole, params: fisAPIErrorParams()},
		{id: "aws:ec2:api-insufficient-instance-capacity-error",
			description: "Inject InsufficientInstanceCapacity error responses on requests made by the target IAM roles.",
			targetKey:   "Roles", resourceType: fisResourceIAMRole, params: fisCapacityErrorParams(true)},
		{id: "aws:ec2:asg-insufficient-instance-capacity-error",
			description: "Inject InsufficientInstanceCapacity error responses on requests made by the target Auto Scaling groups.",
			targetKey:   "AutoScalingGroups", resourceType: fisResourceASG, params: fisCapacityErrorParams(false)},
	}
	out := make(map[string]fisActionSpec, len(specs))
	for _, s := range specs {
		out[s.id] = s
	}
	return out
}

// fisResourceTypeDescriptions are the known resource types, described as the Targets page
// describes them.
var fisResourceTypeDescriptions = map[string]string{
	fisResourceASG:          "An Amazon EC2 Auto Scaling group",
	fisResourceEC2Instance:  "An Amazon EC2 instance",
	fisResourceSpotInstance: "An Amazon EC2 Spot Instance",
	fisResourceIAMRole:      "An IAM role",
}

// fisActionARN is an action's ARN. The account segment is empty: an action belongs to the
// service, not to an account. That form is substrate's reading; no published sample shows one.
func fisActionARN(region, id string) string {
	return "arn:aws:fis:" + region + "::action/" + id
}

// fisActionTargetWire is ActionTarget.
type fisActionTargetWire struct {
	ResourceType string `json:"resourceType"`
}

// fisActionParameterWire is ActionParameter.
type fisActionParameterWire struct {
	Description string `json:"description"`
	Required    bool   `json:"required"`
}

// fisActionWire is Action and ActionSummary; the summary omits parameters.
type fisActionWire struct {
	ID          string                            `json:"id"`
	Arn         string                            `json:"arn"`
	Description string                            `json:"description"`
	Parameters  map[string]fisActionParameterWire `json:"parameters,omitempty"`
	Targets     map[string]fisActionTargetWire    `json:"targets"`
	Tags        map[string]string                 `json:"tags"`
}

// wire renders the action, with its parameters when withParams is set.
func (s fisActionSpec) wire(region string, withParams bool) fisActionWire {
	w := fisActionWire{ID: s.id, Arn: fisActionARN(region, s.id), Description: s.description,
		Targets: map[string]fisActionTargetWire{}, Tags: map[string]string{}}
	if s.targetKey != "" {
		w.Targets[s.targetKey] = fisActionTargetWire{ResourceType: s.resourceType}
	}
	if withParams {
		w.Parameters = make(map[string]fisActionParameterWire, len(s.params))
		for name, ps := range s.params {
			w.Parameters[name] = fisActionParameterWire{Description: ps.description, Required: ps.required}
		}
	}
	return w
}

// fisListPage cuts one page of a listing by the shared offset token, refusing a token no
// previous call returned and a maxResults outside 1 to 100.
func fisListPage[T any](req *AWSRequest, items []T) ([]T, string, *AWSError) {
	offset, ok := decodeOffsetPaginationToken(req.Params["nextToken"])
	if !ok {
		return nil, "", fisValidation("The nextToken is not a token returned by a previous request.")
	}
	maxResults, awsErr := fisMaxResults(req)
	if awsErr != nil {
		return nil, "", awsErr
	}
	page, next := pageByOffsetToken(items, offset, maxResults)
	return page, next, nil
}

func (p *FISPlugin) listActions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	known := fisKnownActions()
	ids := make([]string, 0, len(known))
	for id := range known {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	all := make([]fisActionWire, 0, len(ids))
	for _, id := range ids {
		all = append(all, known[id].wire(ctx.Region, false))
	}
	page, next, awsErr := fisListPage(req, all)
	if awsErr != nil {
		return nil, awsErr
	}
	return fisJSONResponse(struct {
		Actions   []fisActionWire `json:"actions"`
		NextToken string          `json:"nextToken,omitempty"`
	}{page, next})
}

func (p *FISPlugin) getAction(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := strings.TrimPrefix(req.Path, "/actions/")
	spec, ok := fisKnownActions()[id]
	if !ok {
		return nil, fisNotFound("Action " + id + " not found.")
	}
	return fisJSONResponse(map[string]any{"action": spec.wire(ctx.Region, true)})
}

// fisTargetResourceTypeWire is TargetResourceType and TargetResourceTypeSummary.
type fisTargetResourceTypeWire struct {
	ResourceType string                            `json:"resourceType"`
	Description  string                            `json:"description"`
	Parameters   map[string]fisActionParameterWire `json:"parameters,omitempty"`
}

func (p *FISPlugin) listTargetResourceTypes(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	types := make([]string, 0, len(fisResourceTypeDescriptions))
	for t := range fisResourceTypeDescriptions {
		types = append(types, t)
	}
	sort.Strings(types)
	all := make([]fisTargetResourceTypeWire, 0, len(types))
	for _, t := range types {
		all = append(all, fisTargetResourceTypeWire{ResourceType: t, Description: fisResourceTypeDescriptions[t]})
	}
	page, next, awsErr := fisListPage(req, all)
	if awsErr != nil {
		return nil, awsErr
	}
	return fisJSONResponse(struct {
		TargetResourceTypes []fisTargetResourceTypeWire `json:"targetResourceTypes"`
		NextToken           string                      `json:"nextToken,omitempty"`
	}{page, next})
}

func (p *FISPlugin) getTargetResourceType(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	t := strings.TrimPrefix(req.Path, "/targetResourceTypes/")
	desc, ok := fisResourceTypeDescriptions[t]
	if !ok {
		return nil, fisNotFound("Target resource type " + t + " not found.")
	}
	// None of the four types takes a resource parameter (Targets page, Resource parameters).
	return fisJSONResponse(map[string]any{"targetResourceType": fisTargetResourceTypeWire{
		ResourceType: t, Description: desc, Parameters: map[string]fisActionParameterWire{},
	}})
}
