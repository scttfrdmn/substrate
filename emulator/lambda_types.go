package emulator

import (
	"strings"
	"time"
)

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

	// ImageURI is the URI of a container image in ECR when PackageType is "Image".
	ImageURI string `json:"ImageUri,omitempty"`

	// ZipStored indicates whether the deployment ZIP bytes are held in state.
	// False when the function was created via S3 reference (not directly uploaded).
	ZipStored bool `json:"ZipStored,omitempty"`
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

// lambdaARNScope reports the account and Region a Lambda function ARN names, or two empty
// strings when the value is not one.
//
// A function ARN is `arn:aws:lambda:{region}:{account}:function:{name}`, so the two are read
// positionally rather than re-derived from the caller's request context: an ARN naming another
// account's function has to resolve that account's function or none, which is the rule #826
// established for the tagging resolver and #943 carried into the plugin's own tag operations.
func lambdaARNScope(arn string) (accountID, region string) {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "lambda" {
		return "", ""
	}
	return parts[4], parts[3]
}

// lambdaFunctionStateKey names the record one function's configuration and tags live in.
//
// A function name identifies a function only within one account and one Region, so the key
// carries both, following the `<type>:<account>/<region>/<id>` shape every other regional key in
// the tree uses. `API_CreateFunction` states no uniqueness scope in prose — the provenance is its
// own `FunctionArn` response pattern,
// `arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}(...)-[a-z]+-\d{1}:\d{12}:function:[a-zA-Z0-9-_\.]+...`,
// which qualifies the name by Region and account and so cannot be minted from the name alone.
// Until #943 the key was the bare name, so a second account creating a function of a name the
// first had used answered `ResourceConflictException`/409, and a `GetFunction` in one Region
// answered another Region's function.
func lambdaFunctionStateKey(accountID, region, name string) string {
	return "function:" + accountID + "/" + region + "/" + name
}

// lambdaFunctionKeyPrefix is the [lambdaFunctionStateKey] prefix selecting one account's
// functions in one Region, for a scan that must not reach another account's or Region's.
func lambdaFunctionKeyPrefix(accountID, region string) string {
	return "function:" + accountID + "/" + region + "/"
}

// lambdaPolicyStateKey names the record one function's resource policy lives in.
//
// It carries the same scope as [lambdaFunctionStateKey], for a reason the function record's own
// scope does not supply: leaving this key on the bare name while qualifying the function's would
// make two accounts' same-named functions *share* one policy — a cross-account leak the fix
// introduced rather than one it inherited. The same holds for [lambdaZipStateKey] and
// [lambdaInvokeConfigStateKey].
func lambdaPolicyStateKey(accountID, region, name string) string {
	return "function_policy:" + accountID + "/" + region + "/" + name
}

// lambdaZipStateKey names the record one function's uploaded deployment package lives in.
func lambdaZipStateKey(accountID, region, name string) string {
	return "function_zip:" + accountID + "/" + region + "/" + name
}

// lambdaInvokeConfigStateKey names the record one function's event-invoke configuration lives in.
func lambdaInvokeConfigStateKey(accountID, region, name string) string {
	return "function_invoke_config:" + accountID + "/" + region + "/" + name
}
