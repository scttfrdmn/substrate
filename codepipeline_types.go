package substrate

import "time"

// codepipelineNamespace is the state namespace for AWS CodePipeline resources.
const codepipelineNamespace = "codepipeline"

// CodePipelineState represents an AWS CodePipeline pipeline definition and metadata.
type CodePipelineState struct {
	// Name is the pipeline name.
	Name string `json:"name"`
	// RoleArn is the IAM service role ARN.
	RoleArn string `json:"roleArn,omitempty"`
	// Stages is the ordered list of pipeline stage definitions.
	Stages []map[string]interface{} `json:"stages,omitempty"`
	// Version is the pipeline version number, incremented on each update.
	Version int `json:"version"`
	// Created is when the pipeline was created.
	Created time.Time `json:"created"`
	// Updated is when the pipeline was last modified.
	Updated time.Time `json:"updated"`
	// AccountID is the AWS account that owns this pipeline.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the pipeline exists.
	Region string `json:"region"`
}

// CodePipelineExecution represents a single pipeline execution run.
type CodePipelineExecution struct {
	// PipelineExecutionID is the unique execution identifier (UUID).
	PipelineExecutionID string `json:"pipelineExecutionId"`
	// PipelineName is the pipeline that was executed.
	PipelineName string `json:"pipelineName"`
	// PipelineVersion is the pipeline version at the time of execution.
	PipelineVersion int `json:"pipelineVersion"`
	// Status is the execution status.
	Status string `json:"status"` // Succeeded
	// StartTime is when the execution began.
	StartTime time.Time `json:"startTime"`
	// AccountID is the AWS account that owns this execution.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the execution ran.
	Region string `json:"region"`
}
