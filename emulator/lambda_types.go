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

	// TracingMode is TracingConfig.Mode, Active or PassThrough; empty is PassThrough.
	TracingMode string `json:"TracingMode,omitempty"`

	// LastModified is the date the function was last updated in ISO-8601 format.
	LastModified time.Time `json:"LastModified"`

	// CreateDate is the date the function was created.
	CreateDate time.Time `json:"CreateDate"`

	// ImageURI is the URI of a container image in ECR when PackageType is "Image".
	ImageURI string `json:"ImageUri,omitempty"`

	// CodeS3Location is the S3 object an S3-sourced package names, as bucket, key and object
	// version joined by "/"; empty for an inline or image package. CodeSha256 can be empty for an
	// S3 package substrate's S3 does not hold, so PublishVersion's fingerprint reads this too, and a
	// package moved to another object publishes a new version.
	CodeS3Location string `json:"CodeS3Location,omitempty"`

	// ZipStored indicates whether the deployment ZIP bytes are held in state.
	// False when the function was created via S3 reference (not directly uploaded).
	ZipStored bool `json:"ZipStored,omitempty"`

	// EverTagged records that this function has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// LambdaPermissionStatement is a single statement in a Lambda resource policy.
type LambdaPermissionStatement struct {
	// Sid is the statement ID.
	Sid string `json:"Sid"`

	// Effect is either "Allow" or "Deny".
	Effect string `json:"Effect"`

	// Principal is the principal the statement grants: {"Service": …} for a service,
	// {"AWS": …} for an account or an IAM ARN, or the string "*" for everyone — the forms
	// AddPermission's output takes (see lambdaPermissionPrincipal).
	Principal interface{} `json:"Principal"`

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

// LambdaFunctionVersion is a published version of a function: an immutable snapshot of the
// function's code and configuration as they were when PublishVersion ran.
//
// The embedded [LambdaFunction] is the snapshot. Its FunctionArn is the version's qualified ARN,
// its RevisionID is the version's own, and it carries no Tags, because tags belong to the
// function rather than to any one version.
type LambdaFunctionVersion struct {
	LambdaFunction

	// Version is the version number, "1" for the first one published.
	Version string `json:"Version"`

	// Fingerprint identifies the $LATEST code and configuration the version was published from.
	// PublishVersion compares it to decide whether anything has changed since the last version.
	Fingerprint string `json:"Fingerprint"`
}

// LambdaAlias is a named pointer to a function version, with an optional second version that
// receives a share of invocations.
type LambdaAlias struct {
	// Name is the alias name.
	Name string `json:"Name"`

	// FunctionName is the name of the function the alias belongs to.
	FunctionName string `json:"FunctionName"`

	// AliasArn is the function ARN qualified by the alias name.
	AliasArn string `json:"AliasArn"`

	// FunctionVersion is the version the alias invokes: "$LATEST" or a published version number.
	FunctionVersion string `json:"FunctionVersion"`

	// Description is a human-readable description of the alias.
	Description string `json:"Description,omitempty"`

	// AdditionalVersionWeights is RoutingConfig.AdditionalVersionWeights: the second version and
	// the share of invocations, from 0.0 to 1.0, routed to it.
	AdditionalVersionWeights map[string]float64 `json:"AdditionalVersionWeights,omitempty"`

	// RevisionID changes on every update to the alias.
	RevisionID string `json:"RevisionId"`
}

// lambdaVersionStateKey names the record one published version lives in.
func lambdaVersionStateKey(accountID, region, name, version string) string {
	return lambdaVersionKeyPrefix(accountID, region, name) + version
}

// lambdaVersionKeyPrefix is the [lambdaVersionStateKey] prefix selecting one function's
// published versions. The trailing slash keeps function "a" from selecting function "ab".
func lambdaVersionKeyPrefix(accountID, region, name string) string {
	return "function_version:" + accountID + "/" + region + "/" + name + "/"
}

// lambdaVersionSeqStateKey names the record holding the last version number a function
// published. Numbers are never reused, so the counter outlives the version it names and is
// removed only with the function.
func lambdaVersionSeqStateKey(accountID, region, name string) string {
	return "function_version_seq:" + accountID + "/" + region + "/" + name
}

// lambdaAliasStateKey names the record one alias lives in.
func lambdaAliasStateKey(accountID, region, name, alias string) string {
	return lambdaAliasKeyPrefix(accountID, region, name) + alias
}

// lambdaAliasKeyPrefix is the [lambdaAliasStateKey] prefix selecting one function's aliases.
func lambdaAliasKeyPrefix(accountID, region, name string) string {
	return "function_alias:" + accountID + "/" + region + "/" + name + "/"
}

// lambdaQualifiedPolicyStateKey names the resource policy of one version or alias.
//
// Each version and alias has a policy of its own, separate from the function's: AddPermission's
// Qualifier is "a version or alias to add permissions to a published version of the function", and
// GetPolicy with the same Qualifier reads it back. An empty qualifier, or $LATEST, is the
// function's own policy, [lambdaPolicyStateKey]. A function name cannot contain a colon, so the
// qualified key cannot collide with another function's.
func lambdaQualifiedPolicyStateKey(accountID, region, name, qualifier string) string {
	if qualifier == "" || qualifier == lambdaLatestVersion {
		return lambdaPolicyStateKey(accountID, region, name)
	}
	return lambdaPolicyStateKey(accountID, region, name) + ":" + qualifier
}
