package substrate

import "time"

// statesNamespace is the state namespace for Step Functions.
const statesNamespace = "states"

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
