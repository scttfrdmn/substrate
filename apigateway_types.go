package substrate

import "time"

// apigatewayNamespace is the state namespace for API Gateway v1.
const apigatewayNamespace = "apigateway"

// RestAPIState holds the state of an API Gateway v1 REST API.
type RestAPIState struct {
	// ID is the unique identifier for the REST API.
	ID string `json:"Id"`

	// Name is the name of the REST API.
	Name string `json:"Name"`

	// Description is an optional description of the REST API.
	Description string `json:"Description,omitempty"`

	// RootResourceID is the ID of the automatically created root resource.
	RootResourceID string `json:"RootResourceId"`

	// Tags holds arbitrary key-value metadata.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is the time the REST API was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// AccountID is the AWS account that owns the REST API.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the REST API lives.
	Region string `json:"Region"`
}

// ResourceState holds the state of an API Gateway v1 resource.
type ResourceState struct {
	// ID is the unique identifier for the resource.
	ID string `json:"Id"`

	// ParentID is the ID of the parent resource.
	ParentID string `json:"ParentId,omitempty"`

	// PathPart is the URL path segment for this resource.
	PathPart string `json:"PathPart,omitempty"`

	// Path is the full path from the root to this resource.
	Path string `json:"Path"`

	// ResourceMethods maps HTTP verbs to their method configurations.
	ResourceMethods map[string]MethodState `json:"ResourceMethods,omitempty"`

	// APIId is the REST API that owns this resource.
	APIId string `json:"APIId"`

	// AccountID is the AWS account that owns this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this resource lives.
	Region string `json:"Region"`
}

// MethodState holds the configuration of an API Gateway v1 method.
type MethodState struct {
	// HTTPMethod is the HTTP verb (GET, POST, etc.).
	HTTPMethod string `json:"HttpMethod"`

	// AuthorizationType is the authorisation type (NONE, AWS_IAM, CUSTOM, COGNITO_USER_POOLS).
	AuthorizationType string `json:"AuthorizationType"`

	// AuthorizerID is the ID of the authorizer when AuthorizationType is CUSTOM.
	AuthorizerID string `json:"AuthorizerId,omitempty"`

	// APIKeyRequired indicates whether an API key is required.
	APIKeyRequired bool `json:"ApiKeyRequired"`

	// Integration holds the method integration configuration.
	Integration *IntegrationState `json:"MethodIntegration,omitempty"`

	// MethodResponses maps status codes to method response configurations.
	MethodResponses map[string]interface{} `json:"MethodResponses,omitempty"`
}

// IntegrationState holds the backend integration configuration for a method.
type IntegrationState struct {
	// Type is the integration type (AWS, AWS_PROXY, HTTP, HTTP_PROXY, MOCK).
	Type string `json:"Type"`

	// URI is the integration endpoint URI.
	URI string `json:"Uri,omitempty"`

	// HTTPMethod is the HTTP method used to communicate with the integration.
	HTTPMethod string `json:"HttpMethod,omitempty"`

	// IntegrationResponses maps status codes to integration response configurations.
	IntegrationResponses map[string]interface{} `json:"IntegrationResponses,omitempty"`
}

// StageState holds the state of an API Gateway v1 deployment stage.
type StageState struct {
	// StageName is the name of the stage.
	StageName string `json:"StageName"`

	// DeploymentID is the ID of the deployment associated with this stage.
	DeploymentID string `json:"DeploymentId,omitempty"`

	// Description is an optional description of the stage.
	Description string `json:"Description,omitempty"`

	// Variables holds stage-level variables.
	Variables map[string]string `json:"Variables,omitempty"`

	// Tags holds arbitrary key-value metadata.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is the time the stage was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// APIId is the REST API that owns this stage.
	APIId string `json:"APIId"`

	// AccountID is the AWS account that owns this stage.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this stage lives.
	Region string `json:"Region"`
}

// DeploymentState holds the state of an API Gateway v1 deployment.
type DeploymentState struct {
	// ID is the unique identifier for the deployment.
	ID string `json:"Id"`

	// Description is an optional description.
	Description string `json:"Description,omitempty"`

	// CreatedDate is the time the deployment was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// APIId is the REST API that owns this deployment.
	APIId string `json:"APIId"`

	// AccountID is the AWS account that owns this deployment.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this deployment lives.
	Region string `json:"Region"`
}

// AuthorizerState holds the state of an API Gateway v1 authorizer.
type AuthorizerState struct {
	// ID is the unique identifier for the authorizer.
	ID string `json:"Id"`

	// Name is the name of the authorizer.
	Name string `json:"Name"`

	// Type is the authorizer type (TOKEN, REQUEST, COGNITO_USER_POOLS).
	Type string `json:"Type"`

	// ProviderARNs is a list of Cognito user pool ARNs for COGNITO_USER_POOLS type.
	ProviderARNs []string `json:"ProviderARNs,omitempty"`

	// AuthorizerURI is the Lambda function ARN for custom authorizers.
	AuthorizerURI string `json:"AuthorizerUri,omitempty"`

	// IdentitySource is the request header that carries the token.
	IdentitySource string `json:"IdentitySource,omitempty"`

	// APIId is the REST API that owns this authorizer.
	APIId string `json:"APIId"`

	// AccountID is the AWS account that owns this authorizer.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this authorizer lives.
	Region string `json:"Region"`
}

// APIKeyState holds the state of an API Gateway API key.
type APIKeyState struct {
	// ID is the unique identifier for the API key.
	ID string `json:"Id"`

	// Name is the name of the API key.
	Name string `json:"Name"`

	// Value is the API key value.
	Value string `json:"Value"`

	// Enabled indicates whether the API key is active.
	Enabled bool `json:"Enabled"`

	// Tags holds arbitrary key-value metadata.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is the time the API key was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// AccountID is the AWS account that owns this API key.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this API key lives.
	Region string `json:"Region"`
}

// UsagePlanState holds the state of an API Gateway usage plan.
type UsagePlanState struct {
	// ID is the unique identifier for the usage plan.
	ID string `json:"Id"`

	// Name is the name of the usage plan.
	Name string `json:"Name"`

	// Description is an optional description.
	Description string `json:"Description,omitempty"`

	// Tags holds arbitrary key-value metadata.
	Tags map[string]string `json:"Tags,omitempty"`

	// APIStages is the list of API stages associated with this usage plan.
	APIStages []interface{} `json:"ApiStages,omitempty"`

	// CreatedDate is the time the usage plan was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// AccountID is the AWS account that owns this usage plan.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this usage plan lives.
	Region string `json:"Region"`
}

// DomainNameState holds the state of an API Gateway custom domain name.
type DomainNameState struct {
	// DomainName is the custom domain name.
	DomainName string `json:"DomainName"`

	// CertificateArn is the ARN of the ACM certificate associated with this domain.
	CertificateArn string `json:"CertificateArn,omitempty"`

	// RegionalDomainName is the regional endpoint hostname.
	RegionalDomainName string `json:"RegionalDomainName"`

	// AccountID is the AWS account that owns this domain name.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this domain name lives.
	Region string `json:"Region"`
}

// BasePathMappingState holds the state of an API Gateway base path mapping.
type BasePathMappingState struct {
	// BasePath is the base path that callers must use (e.g., "(none)", "v1").
	BasePath string `json:"BasePath"`

	// RestAPIID is the ID of the REST API.
	RestAPIID string `json:"RestApiId"`

	// Stage is the stage name the mapping points to.
	Stage string `json:"Stage,omitempty"`

	// DomainName is the custom domain name for this mapping.
	DomainName string `json:"DomainName"`
}

// --- State key helpers -------------------------------------------------------

func apigwAPIKey(accountID, region, apiID string) string {
	return "api:" + accountID + "/" + region + "/" + apiID
}

func apigwAPIIDsKey(accountID, region string) string {
	return "api_ids:" + accountID + "/" + region
}

func apigwResourceKey(accountID, region, apiID, resID string) string {
	return "resource:" + accountID + "/" + region + "/" + apiID + "/" + resID
}

func apigwResourceIDsKey(accountID, region, apiID string) string {
	return "resource_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwMethodKey(accountID, region, apiID, resID, verb string) string {
	return "method:" + accountID + "/" + region + "/" + apiID + "/" + resID + "/" + verb
}

func apigwIntegrationKey(accountID, region, apiID, resID, verb string) string {
	return "integration:" + accountID + "/" + region + "/" + apiID + "/" + resID + "/" + verb
}

func apigwStageKey(accountID, region, apiID, stageName string) string {
	return "stage:" + accountID + "/" + region + "/" + apiID + "/" + stageName
}

func apigwStageNamesKey(accountID, region, apiID string) string {
	return "stage_names:" + accountID + "/" + region + "/" + apiID
}

func apigwDeploymentKey(accountID, region, apiID, deployID string) string {
	return "deployment:" + accountID + "/" + region + "/" + apiID + "/" + deployID
}

func apigwDeploymentIDsKey(accountID, region, apiID string) string {
	return "deployment_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwAuthorizerKey(accountID, region, apiID, authID string) string {
	return "authorizer:" + accountID + "/" + region + "/" + apiID + "/" + authID
}

func apigwAuthorizerIDsKey(accountID, region, apiID string) string {
	return "authorizer_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwAPIKeyKey(accountID, region, keyID string) string {
	return "apikey:" + accountID + "/" + region + "/" + keyID
}

func apigwAPIKeyIDsKey(accountID, region string) string {
	return "apikey_ids:" + accountID + "/" + region
}

func apigwUsagePlanKey(accountID, region, planID string) string {
	return "usageplan:" + accountID + "/" + region + "/" + planID
}

func apigwUsagePlanIDsKey(accountID, region string) string {
	return "usageplan_ids:" + accountID + "/" + region
}

func apigwDomainNameKey(accountID, region, name string) string {
	return "domainname:" + accountID + "/" + region + "/" + name
}

func apigwBasePathKey(accountID, region, domain, basePath string) string {
	return "basepath:" + accountID + "/" + region + "/" + domain + "/" + basePath
}
