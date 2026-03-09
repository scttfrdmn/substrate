package substrate

import "time"

// lambdaNamespace is the state namespace used by LambdaPlugin.
const lambdaNamespace = "lambda"

// LambdaFunction represents an emulated AWS Lambda function.
type LambdaFunction struct {
	// FunctionName is the function name or ARN.
	FunctionName string `json:"FunctionName"`

	// FunctionArn is the function's Amazon Resource Name.
	FunctionArn string `json:"FunctionArn"`

	// Runtime is the identifier of the function's runtime.
	Runtime string `json:"Runtime"`

	// Role is the function's execution role ARN.
	Role string `json:"Role"`

	// Handler is the function entrypoint in the format file.handler.
	Handler string `json:"Handler"`

	// Description is a human-readable description of the function.
	Description string `json:"Description,omitempty"`

	// Timeout is the function execution time limit in seconds (default 3).
	Timeout int `json:"Timeout"`

	// MemorySize is the memory allocated to the function in MB (default 128).
	MemorySize int `json:"MemorySize"`

	// Environment holds the function's environment variables.
	Environment map[string]string `json:"Environment,omitempty"`

	// CodeSize is the size of the deployment package in bytes.
	CodeSize int64 `json:"CodeSize"`

	// CodeSha256 is the SHA256 hash of the deployment package.
	CodeSha256 string `json:"CodeSha256"`

	// RevisionID is a unique identifier for the current function code and configuration.
	RevisionID string `json:"RevisionId"`

	// State indicates the current state of the function (e.g., "Active").
	State string `json:"State"`

	// PackageType is the type of deployment package ("Zip" or "Image").
	PackageType string `json:"PackageType"`

	// Architectures is the instruction set architectures the function supports.
	Architectures []string `json:"Architectures,omitempty"`

	// Tags are key-value pairs applied to the function.
	Tags map[string]string `json:"Tags,omitempty"`

	// LastModified is the date the function was last updated in ISO-8601 format.
	LastModified time.Time `json:"LastModified"`

	// CreateDate is the date the function was created.
	CreateDate time.Time `json:"CreateDate"`
}

// LambdaPermissionStatement is a single statement in a Lambda resource policy.
type LambdaPermissionStatement struct {
	// Sid is the statement ID.
	Sid string `json:"Sid"`

	// Effect is either "Allow" or "Deny".
	Effect string `json:"Effect"`

	// Principal maps principal type to principal value.
	Principal map[string]string `json:"Principal"`

	// Action is the Lambda API action the statement applies to.
	Action string `json:"Action"`

	// Resource is the function ARN the statement applies to.
	Resource string `json:"Resource"`

	// Condition holds optional condition keys and values.
	Condition map[string]map[string]string `json:"Condition,omitempty"`
}

// LambdaResourcePolicy is the resource-based policy document for a Lambda function.
type LambdaResourcePolicy struct {
	// Version is the policy language version (e.g., "2012-10-17").
	Version string `json:"Version"`

	// Statement contains the policy statements.
	Statement []LambdaPermissionStatement `json:"Statement"`
}

// LambdaEventInvokeConfig holds asynchronous invocation configuration for a function.
type LambdaEventInvokeConfig struct {
	// FunctionName is the name of the Lambda function.
	FunctionName string `json:"FunctionName"`

	// MaximumRetryAttempts is the maximum number of retries for async invocations.
	MaximumRetryAttempts int `json:"MaximumRetryAttempts"`

	// MaximumEventAgeInSeconds is the maximum age for queued events in seconds.
	MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds"`
}

// lambdaFunctionARN constructs the ARN for a Lambda function.
func lambdaFunctionARN(region, accountID, name string) string {
	return "arn:aws:lambda:" + region + ":" + accountID + ":function:" + name
}
