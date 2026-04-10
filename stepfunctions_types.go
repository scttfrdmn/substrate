package substrate

import "time"

// statesNamespace is the state namespace for Step Functions.
const statesNamespace = "states"

// StateMachineDefinition is the parsed Amazon States Language definition.
type StateMachineDefinition struct {
	// Comment is an optional human-readable description.
	Comment string `json:"Comment"`

	// StartAt is the name of the first state to execute.
	StartAt string `json:"StartAt"`

	// States maps state names to their definitions.
	States map[string]*ASLState `json:"States"`
}

// ASLState represents a single state in an ASL definition.
type ASLState struct {
	// Type is the state type: Task|Wait|Choice|Pass|Succeed|Fail|Parallel|Map.
	Type string `json:"Type"`

	// Next is the name of the next state to transition to.
	Next string `json:"Next"`

	// End marks this as a terminal state when true.
	End bool `json:"End"`

	// InputPath selects the effective input for the state (default "$").
	InputPath string `json:"InputPath"`

	// OutputPath filters the state output (default "$").
	OutputPath string `json:"OutputPath"`

	// ResultPath determines where to place the result in the current input (default "$").
	ResultPath string `json:"ResultPath"`

	// Parameters replaces the effective input with a new payload.
	Parameters map[string]interface{} `json:"Parameters"`

	// ResultSelector filters the raw result before ResultPath is applied.
	ResultSelector map[string]interface{} `json:"ResultSelector"`

	// Resource is the URI of the Task resource (e.g. Lambda ARN).
	Resource string `json:"Resource"`

	// TimeoutSeconds is the Task timeout.
	TimeoutSeconds int `json:"TimeoutSeconds"`

	// Retry is the list of retry policies for Task, Parallel, and Map states.
	Retry []RetryConfig `json:"Retry"`

	// Catch is the list of catch policies for Task, Parallel, and Map states.
	Catch []CatchConfig `json:"Catch"`

	// Seconds is the number of seconds to wait in a Wait state.
	Seconds int `json:"Seconds"`

	// SecondsPath is a reference path to the wait duration in a Wait state.
	SecondsPath string `json:"SecondsPath"`

	// Choices is the list of branching conditions in a Choice state.
	Choices []ChoiceRule `json:"Choices"`

	// Default is the fallback state name for a Choice state.
	Default string `json:"Default"`

	// Result is the literal output of a Pass state (overrides input pass-through).
	Result interface{} `json:"Result"`

	// Error is the error code for a Fail state.
	Error string `json:"Error"`

	// Cause is the error cause for a Fail state.
	Cause string `json:"Cause"`

	// Branches holds the parallel branch definitions for a Parallel state.
	Branches []StateMachineDefinition `json:"Branches"`

	// Iterator is the sub-state machine for a Map state.
	Iterator *StateMachineDefinition `json:"Iterator"`

	// ItemsPath is the reference path to the input array for a Map state.
	ItemsPath string `json:"ItemsPath"`

	// MaxConcurrency is the maximum number of concurrent Map iterations.
	MaxConcurrency int `json:"MaxConcurrency"`
}

// RetryConfig holds the retry policy for Task, Parallel, and Map states.
type RetryConfig struct {
	// ErrorEquals is the list of error codes that trigger this retry.
	ErrorEquals []string `json:"ErrorEquals"`

	// IntervalSeconds is the initial retry delay in seconds.
	IntervalSeconds int `json:"IntervalSeconds"`

	// MaxAttempts is the maximum number of retry attempts (0 = no retry).
	MaxAttempts int `json:"MaxAttempts"`

	// BackoffRate is the multiplier applied to the retry interval on each attempt.
	BackoffRate float64 `json:"BackoffRate"`
}

// CatchConfig holds the catch/fallback policy for Task, Parallel, and Map states.
type CatchConfig struct {
	// ErrorEquals is the list of error codes that trigger this catch.
	ErrorEquals []string `json:"ErrorEquals"`

	// Next is the name of the state to transition to on catch.
	Next string `json:"Next"`

	// ResultPath determines where the error output is placed.
	ResultPath string `json:"ResultPath"`
}

// ChoiceRule is a single condition branch in a Choice state.
type ChoiceRule struct {
	// Variable is the reference path to the value being compared.
	Variable string `json:"Variable"`

	// StringEquals matches when the variable equals the given string.
	StringEquals *string `json:"StringEquals"`

	// NumericEquals matches when the variable equals the given number.
	NumericEquals *float64 `json:"NumericEquals"`

	// BooleanEquals matches when the variable equals the given boolean.
	BooleanEquals *bool `json:"BooleanEquals"`

	// StringGreaterThan matches when the variable is lexicographically greater.
	StringGreaterThan *string `json:"StringGreaterThan"`

	// StringLessThan matches when the variable is lexicographically less.
	StringLessThan *string `json:"StringLessThan"`

	// NumericGreaterThan matches when the variable is numerically greater.
	NumericGreaterThan *float64 `json:"NumericGreaterThan"`

	// NumericLessThan matches when the variable is numerically less.
	NumericLessThan *float64 `json:"NumericLessThan"`

	// StringGreaterThanOrEquals matches when the variable is lexicographically >= the value.
	StringGreaterThanOrEquals *string `json:"StringGreaterThanOrEquals"` //nolint:revive

	// StringLessThanOrEquals matches when the variable is lexicographically <= the value.
	StringLessThanOrEquals *string `json:"StringLessThanOrEquals"` //nolint:revive

	// NumericGreaterThanOrEquals matches when the variable is numerically >= the value.
	NumericGreaterThanOrEquals *float64 `json:"NumericGreaterThanOrEquals"` //nolint:revive

	// NumericLessThanOrEquals matches when the variable is numerically <= the value.
	NumericLessThanOrEquals *float64 `json:"NumericLessThanOrEquals"` //nolint:revive

	// IsNull matches when the variable is null (true) or non-null (false).
	IsNull *bool `json:"IsNull"`

	// IsPresent matches when the variable exists (true) or is absent (false).
	IsPresent *bool `json:"IsPresent"`

	// And requires all nested rules to match.
	And []ChoiceRule `json:"And"`

	// Or requires any nested rule to match.
	Or []ChoiceRule `json:"Or"`

	// Not negates the nested rule.
	Not *ChoiceRule `json:"Not"`

	// Next is the state to transition to when this rule matches.
	Next string `json:"Next"`
}

// HistoryEvent is a single event in an execution's history.
type HistoryEvent struct {
	// ID is the sequential event identifier.
	ID int64 `json:"id"`

	// Type is the event type string (e.g. "ExecutionStarted").
	Type string `json:"type"`

	// Timestamp is when the event occurred.
	Timestamp time.Time `json:"timestamp"`

	// ExecutionStartedEventDetails holds details for ExecutionStarted events.
	ExecutionStartedEventDetails *map[string]interface{} `json:"executionStartedEventDetails,omitempty"`

	// ExecutionSucceededEventDetails holds details for ExecutionSucceeded events.
	ExecutionSucceededEventDetails *map[string]interface{} `json:"executionSucceededEventDetails,omitempty"`

	// ExecutionFailedEventDetails holds details for ExecutionFailed events.
	ExecutionFailedEventDetails *map[string]interface{} `json:"executionFailedEventDetails,omitempty"`

	// StateEnteredEventDetails holds details for StateEntered events.
	StateEnteredEventDetails *map[string]interface{} `json:"stateEnteredEventDetails,omitempty"`

	// StateExitedEventDetails holds details for StateExited events.
	StateExitedEventDetails *map[string]interface{} `json:"stateExitedEventDetails,omitempty"`

	// TaskScheduledEventDetails holds details for TaskScheduled events.
	TaskScheduledEventDetails *map[string]interface{} `json:"taskScheduledEventDetails,omitempty"`

	// TaskSucceededEventDetails holds details for TaskSucceeded events.
	TaskSucceededEventDetails *map[string]interface{} `json:"taskSucceededEventDetails,omitempty"`

	// TaskFailedEventDetails holds details for TaskFailed events.
	TaskFailedEventDetails *map[string]interface{} `json:"taskFailedEventDetails,omitempty"`
}

// StateMachineState holds the persisted state of an AWS Step Functions state machine.
type StateMachineState struct {
	// StateMachineArn is the ARN of the state machine.
	StateMachineArn string `json:"StateMachineArn"`

	// Name is the state machine name.
	Name string `json:"Name"`

	// Status is the state machine status: ACTIVE or DELETING.
	Status string `json:"Status"`

	// Definition is the Amazon States Language definition as raw JSON.
	Definition string `json:"Definition"`

	// RoleArn is the IAM role ARN used by the state machine.
	RoleArn string `json:"RoleArn"`

	// Type is the state machine type: STANDARD or EXPRESS.
	Type string `json:"Type"`

	// Tags holds resource tags.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is when the state machine was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// AccountID is the owning AWS account.
	AccountID string `json:"AccountID"`

	// Region is the AWS region.
	Region string `json:"Region"`
}

// ExecutionState holds the persisted state of a Step Functions execution.
type ExecutionState struct {
	// ExecutionArn is the ARN of the execution.
	ExecutionArn string `json:"ExecutionArn"`

	// StateMachineArn is the ARN of the parent state machine.
	StateMachineArn string `json:"StateMachineArn"`

	// Name is the execution name.
	Name string `json:"Name"`

	// Status is the execution status: RUNNING, SUCCEEDED, FAILED, or ABORTED.
	Status string `json:"Status"`

	// Input is the raw JSON input provided to the execution.
	Input string `json:"Input,omitempty"`

	// Output is the raw JSON output of the execution.
	Output string `json:"Output,omitempty"`

	// StartDate is when the execution started.
	StartDate time.Time `json:"StartDate"`

	// StopDate is when the execution stopped (zero if still running).
	StopDate time.Time `json:"StopDate,omitempty"`

	// AccountID is the owning AWS account.
	AccountID string `json:"AccountID"`

	// Region is the AWS region.
	Region string `json:"Region"`

	// History holds the ordered list of events recorded during execution.
	History []HistoryEvent `json:"History,omitempty"`

	// ErrorDetails holds the error description for FAILED executions.
	ErrorDetails string `json:"ErrorDetails,omitempty"`
}

// ActivityState holds the persisted state of a Step Functions activity.
type ActivityState struct {
	// ActivityArn is the ARN of the activity.
	ActivityArn string `json:"ActivityArn"`

	// Name is the activity name.
	Name string `json:"Name"`

	// Tags holds resource tags.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is when the activity was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// AccountID is the owning AWS account.
	AccountID string `json:"AccountID"`

	// Region is the AWS region.
	Region string `json:"Region"`
}
