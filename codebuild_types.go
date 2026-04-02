package substrate

import "time"

// codebuildNamespace is the state namespace for AWS CodeBuild resources.
const codebuildNamespace = "codebuild"

// CodeBuildProject represents an AWS CodeBuild build project.
type CodeBuildProject struct {
	// Name is the project name.
	Name string `json:"name"`
	// ARN is the Amazon Resource Name of the project.
	ARN string `json:"arn"`
	// Description is an optional human-readable description.
	Description string `json:"description,omitempty"`
	// Source describes the build input source code settings.
	Source map[string]interface{} `json:"source,omitempty"`
	// Artifacts describes the build output artifacts settings.
	Artifacts map[string]interface{} `json:"artifacts,omitempty"`
	// Environment describes the build environment settings.
	Environment map[string]interface{} `json:"environment,omitempty"`
	// ServiceRole is the IAM role ARN used during a build.
	ServiceRole string `json:"serviceRole,omitempty"`
	// Created is when the project was created.
	Created time.Time `json:"created"`
	// LastModified is when the project was last changed.
	LastModified time.Time `json:"lastModified"`
	// AccountID is the AWS account that owns this project.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the project exists.
	Region string `json:"region"`
}

// CodeBuildBuild represents an AWS CodeBuild build execution.
type CodeBuildBuild struct {
	// ID is the unique identifier for the build in the form projectName:uuid.
	ID string `json:"id"`
	// ARN is the Amazon Resource Name of the build.
	ARN string `json:"arn"`
	// ProjectName is the name of the build project.
	ProjectName string `json:"projectName"`
	// BuildStatus is the current status of the build.
	BuildStatus string `json:"buildStatus"` // SUCCEEDED
	// StartTime is when the build started.
	StartTime time.Time `json:"startTime"`
	// EndTime is when the build completed.
	EndTime time.Time `json:"endTime"`
	// CurrentPhase is the current build phase.
	CurrentPhase string `json:"currentPhase"` // COMPLETED
	// AccountID is the AWS account that owns this build.
	AccountID string `json:"accountID"`
	// Region is the AWS region where the build ran.
	Region string `json:"region"`
}
