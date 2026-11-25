package emulator

// The records the FIS plugin stores and the wire shapes it renders (#1619).
//
// Each record keeps the members the API publishes under their own camelCase names, so a stored
// template renders by copying, and keeps its bookkeeping (owner, idempotency token, the
// experiment engine's progress) in fields the wire structs do not carry. Every response is
// rendered through a wire struct, never by marshaling a record: see [FISTemplate.wire] and
// [FISExperiment.wire].

import (
	"sort"
	"time"
)

// fisNamespace is the state namespace of the FIS plugin.
const fisNamespace = "fis"

// FISTarget is one experiment-template target, as CreateExperimentTemplateTargetInput publishes it.
type FISTarget struct {
	// ResourceType is the target's resource type, e.g. aws:ec2:spot-instance.
	ResourceType string `json:"resourceType"`
	// ResourceArns names specific resources.
	ResourceArns []string `json:"resourceArns,omitempty"`
	// ResourceTags selects resources carrying every tag (AND).
	ResourceTags map[string]string `json:"resourceTags,omitempty"`
	// Filters scopes the identified resources by attribute path.
	Filters []FISTargetFilter `json:"filters,omitempty"`
	// SelectionMode is ALL, COUNT(n) or PERCENT(n).
	SelectionMode string `json:"selectionMode"`
	// Parameters are the resource type's parameters.
	Parameters map[string]string `json:"parameters,omitempty"`
}

// FISTargetFilter is one target filter: an attribute path and the values any of which matches.
type FISTargetFilter struct {
	// Path is the attribute path in the resource's Describe output, e.g. State.Name.
	Path string `json:"path"`
	// Values are the values the attribute may take.
	Values []string `json:"values"`
}

// FISAction is one experiment-template action.
type FISAction struct {
	// ActionID is the action's ID, e.g. aws:ec2:stop-instances.
	ActionID string `json:"actionId"`
	// Description is the action's description.
	Description string `json:"description,omitempty"`
	// Parameters are the action's parameters.
	Parameters map[string]string `json:"parameters,omitempty"`
	// Targets maps the action's target keys (e.g. Instances) to template target names.
	Targets map[string]string `json:"targets,omitempty"`
	// StartAfter names the actions that must complete before this one starts.
	StartAfter []string `json:"startAfter,omitempty"`
}

// FISStopCondition is one stop condition.
type FISStopCondition struct {
	// Source is aws:cloudwatch:alarm or none.
	Source string `json:"source"`
	// Value is the alarm's ARN for an alarm source.
	Value string `json:"value,omitempty"`
}

// FISExperimentOptions are a template's or an experiment's options.
type FISExperimentOptions struct {
	// AccountTargeting is single-account or multi-account.
	AccountTargeting string `json:"accountTargeting,omitempty"`
	// EmptyTargetResolutionMode is fail or skip.
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode,omitempty"`
	// ActionsMode is run-all or skip-all; set on experiments only.
	ActionsMode string `json:"actionsMode,omitempty"`
}

// FISTemplate is one stored experiment template.
type FISTemplate struct {
	// ID is the template's ID.
	ID string `json:"id"`
	// ARN is the template's ARN.
	ARN string `json:"arn"`
	// Description is the template's description.
	Description string `json:"description"`
	// Targets are the template's targets by name.
	Targets map[string]FISTarget `json:"targets,omitempty"`
	// Actions are the template's actions by name.
	Actions map[string]FISAction `json:"actions"`
	// StopConditions are the template's stop conditions.
	StopConditions []FISStopCondition `json:"stopConditions"`
	// RoleArn is the experiment role.
	RoleArn string `json:"roleArn"`
	// Tags are the template's tags.
	Tags map[string]string `json:"tags,omitempty"`
	// LogConfiguration is recorded as sent and reported; no log is written.
	LogConfiguration map[string]any `json:"logConfiguration,omitempty"`
	// ExperimentReportConfiguration is recorded as sent and reported; no report is written.
	ExperimentReportConfiguration map[string]any `json:"experimentReportConfiguration,omitempty"`
	// ExperimentOptions are the template's options, with their defaults applied.
	ExperimentOptions FISExperimentOptions `json:"experimentOptions"`
	// CreationTime and LastUpdateTime are when the template was created and last updated.
	CreationTime   time.Time `json:"creationTime"`
	LastUpdateTime time.Time `json:"lastUpdateTime"`

	// ClientToken is the idempotency token the create named.
	ClientToken string `json:"client_token,omitempty"`
	// AccountID and Region own the template.
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
}

// FISActionState is an experiment action's state.
type FISActionState struct {
	// Status is the action's status.
	Status string `json:"status"`
	// Reason is the reason for the status.
	Reason string `json:"reason"`
}

// FISExperimentAction is one action of a running experiment.
type FISExperimentAction struct {
	FISAction
	// State is the action's state.
	State FISActionState `json:"state"`
	// StartTime and EndTime are when the action started and ended on the simulated clock.
	StartTime *time.Time `json:"startTime,omitempty"`
	EndTime   *time.Time `json:"endTime,omitempty"`

	// DueAt is when a running action's timed part ends: the restart of a stop-instances
	// action, the interruption of a send-spot-instance-interruptions action, the end of a wait.
	DueAt *time.Time `json:"due_at,omitempty"`
}

// FISExperiment is one stored experiment.
type FISExperiment struct {
	// ID is the experiment's ID.
	ID string `json:"id"`
	// ARN is the experiment's ARN.
	ARN string `json:"arn"`
	// TemplateID is the template the experiment was started from.
	TemplateID string `json:"experimentTemplateId"`
	// RoleArn is the experiment role.
	RoleArn string `json:"roleArn"`
	// State is the experiment's state.
	State FISActionState `json:"state"`
	// Targets are the template's targets as the experiment copied them.
	Targets map[string]FISTarget `json:"targets,omitempty"`
	// Actions are the experiment's actions by name.
	Actions map[string]*FISExperimentAction `json:"actions"`
	// StopConditions are the template's stop conditions.
	StopConditions []FISStopCondition `json:"stopConditions"`
	// Tags are the experiment's tags.
	Tags map[string]string `json:"tags,omitempty"`
	// LogConfiguration and ExperimentReportConfiguration are copied from the template.
	LogConfiguration              map[string]any `json:"logConfiguration,omitempty"`
	ExperimentReportConfiguration map[string]any `json:"experimentReportConfiguration,omitempty"`
	// ExperimentOptions are the experiment's options.
	ExperimentOptions FISExperimentOptions `json:"experimentOptions"`
	// CreationTime, StartTime and EndTime are on the simulated clock.
	CreationTime time.Time  `json:"creationTime"`
	StartTime    time.Time  `json:"startTime"`
	EndTime      *time.Time `json:"endTime,omitempty"`

	// Resolved maps each target name to the resources it resolved to, in selection order:
	// instance IDs for an EC2 target, ARNs for any other.
	Resolved map[string][]string `json:"resolved,omitempty"`
	// StopRequested is when StopExperiment, or a stop condition, asked the experiment to stop.
	StopRequested *time.Time `json:"stop_requested,omitempty"`
	// StopReason is the stopped state's reason.
	StopReason string `json:"stop_reason,omitempty"`
	// ClientToken is the idempotency token the start named.
	ClientToken string `json:"client_token,omitempty"`
	// AccountID and Region own the experiment.
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
}

// fisEpoch renders a time as the epoch seconds a REST-JSON timestamp carries, to the millisecond.
func fisEpoch(t time.Time) float64 {
	return float64(t.UnixMilli()) / 1000
}

// fisEpochPtr renders an optional time.
func fisEpochPtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}
	v := fisEpoch(*t)
	return &v
}

// fisTemplateWire is ExperimentTemplate.
type fisTemplateWire struct {
	ID                               string               `json:"id"`
	Arn                              string               `json:"arn"`
	Description                      string               `json:"description"`
	Targets                          map[string]FISTarget `json:"targets"`
	Actions                          map[string]FISAction `json:"actions"`
	StopConditions                   []FISStopCondition   `json:"stopConditions"`
	CreationTime                     float64              `json:"creationTime"`
	LastUpdateTime                   float64              `json:"lastUpdateTime"`
	RoleArn                          string               `json:"roleArn"`
	Tags                             map[string]string    `json:"tags"`
	LogConfiguration                 map[string]any       `json:"logConfiguration,omitempty"`
	ExperimentOptions                FISExperimentOptions `json:"experimentOptions"`
	TargetAccountConfigurationsCount int64                `json:"targetAccountConfigurationsCount"`
	ExperimentReportConfiguration    map[string]any       `json:"experimentReportConfiguration,omitempty"`
}

// wire renders the template as ExperimentTemplate. Empty maps render as {}, as the CLI's
// create-experiment-template sample renders "tags": {}.
func (t *FISTemplate) wire() fisTemplateWire {
	return fisTemplateWire{
		ID: t.ID, Arn: t.ARN, Description: t.Description,
		Targets: fisNonNil(t.Targets), Actions: fisNonNil(t.Actions), StopConditions: t.StopConditions,
		CreationTime: fisEpoch(t.CreationTime), LastUpdateTime: fisEpoch(t.LastUpdateTime),
		RoleArn: t.RoleArn, Tags: fisNonNil(t.Tags), LogConfiguration: t.LogConfiguration,
		ExperimentOptions: t.ExperimentOptions, ExperimentReportConfiguration: t.ExperimentReportConfiguration,
	}
}

// fisTemplateSummaryWire is ExperimentTemplateSummary.
type fisTemplateSummaryWire struct {
	ID             string            `json:"id"`
	Arn            string            `json:"arn"`
	Description    string            `json:"description"`
	CreationTime   float64           `json:"creationTime"`
	LastUpdateTime float64           `json:"lastUpdateTime"`
	Tags           map[string]string `json:"tags"`
}

// summary renders the template as ExperimentTemplateSummary.
func (t *FISTemplate) summary() fisTemplateSummaryWire {
	return fisTemplateSummaryWire{
		ID: t.ID, Arn: t.ARN, Description: t.Description,
		CreationTime: fisEpoch(t.CreationTime), LastUpdateTime: fisEpoch(t.LastUpdateTime), Tags: fisNonNil(t.Tags),
	}
}

// fisExperimentActionWire is ExperimentAction.
type fisExperimentActionWire struct {
	ActionID    string            `json:"actionId"`
	Description string            `json:"description,omitempty"`
	Parameters  map[string]string `json:"parameters"`
	Targets     map[string]string `json:"targets,omitempty"`
	StartAfter  []string          `json:"startAfter,omitempty"`
	State       FISActionState    `json:"state"`
	StartTime   *float64          `json:"startTime,omitempty"`
	EndTime     *float64          `json:"endTime,omitempty"`
}

// fisExperimentWire is Experiment.
type fisExperimentWire struct {
	ID                               string                             `json:"id"`
	Arn                              string                             `json:"arn"`
	ExperimentTemplateID             string                             `json:"experimentTemplateId"`
	RoleArn                          string                             `json:"roleArn"`
	State                            FISActionState                     `json:"state"`
	Targets                          map[string]FISTarget               `json:"targets"`
	Actions                          map[string]fisExperimentActionWire `json:"actions"`
	StopConditions                   []FISStopCondition                 `json:"stopConditions"`
	CreationTime                     float64                            `json:"creationTime"`
	StartTime                        float64                            `json:"startTime"`
	EndTime                          *float64                           `json:"endTime,omitempty"`
	Tags                             map[string]string                  `json:"tags"`
	LogConfiguration                 map[string]any                     `json:"logConfiguration,omitempty"`
	ExperimentOptions                FISExperimentOptions               `json:"experimentOptions"`
	TargetAccountConfigurationsCount int64                              `json:"targetAccountConfigurationsCount"`
	ExperimentReportConfiguration    map[string]any                     `json:"experimentReportConfiguration,omitempty"`
}

// wire renders the experiment as Experiment. An action's parameters render as {} when it has
// none, as the CLI's start-experiment sample renders "parameters": {}.
func (e *FISExperiment) wire() fisExperimentWire {
	actions := make(map[string]fisExperimentActionWire, len(e.Actions))
	for name, a := range e.Actions {
		actions[name] = fisExperimentActionWire{
			ActionID: a.ActionID, Description: a.Description, Parameters: fisNonNil(a.Parameters),
			Targets: a.Targets, StartAfter: a.StartAfter, State: a.State,
			StartTime: fisEpochPtr(a.StartTime), EndTime: fisEpochPtr(a.EndTime),
		}
	}
	return fisExperimentWire{
		ID: e.ID, Arn: e.ARN, ExperimentTemplateID: e.TemplateID, RoleArn: e.RoleArn, State: e.State,
		Targets: fisNonNil(e.Targets), Actions: actions, StopConditions: e.StopConditions,
		CreationTime: fisEpoch(e.CreationTime), StartTime: fisEpoch(e.StartTime), EndTime: fisEpochPtr(e.EndTime),
		Tags: fisNonNil(e.Tags), LogConfiguration: e.LogConfiguration, ExperimentOptions: e.ExperimentOptions,
		ExperimentReportConfiguration: e.ExperimentReportConfiguration,
	}
}

// fisExperimentSummaryWire is ExperimentSummary.
type fisExperimentSummaryWire struct {
	ID                   string               `json:"id"`
	Arn                  string               `json:"arn"`
	ExperimentTemplateID string               `json:"experimentTemplateId"`
	State                FISActionState       `json:"state"`
	CreationTime         float64              `json:"creationTime"`
	Tags                 map[string]string    `json:"tags"`
	ExperimentOptions    FISExperimentOptions `json:"experimentOptions"`
}

// summary renders the experiment as ExperimentSummary.
func (e *FISExperiment) summary() fisExperimentSummaryWire {
	return fisExperimentSummaryWire{
		ID: e.ID, Arn: e.ARN, ExperimentTemplateID: e.TemplateID, State: e.State,
		CreationTime: fisEpoch(e.CreationTime), Tags: fisNonNil(e.Tags), ExperimentOptions: e.ExperimentOptions,
	}
}

// actionNames returns the experiment's action names in order, which is the order the engine
// visits them in, so a tie between two actions due at one instant resolves the same way on
// every run.
func (e *FISExperiment) actionNames() []string {
	names := make([]string, 0, len(e.Actions))
	for name := range e.Actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// fisNonNil returns m, or an empty map when m is nil, so it renders as {} rather than null.
func fisNonNil[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return map[K]V{}
	}
	return m
}
