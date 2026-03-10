package substrate

import "time"

// apigatewayv2Namespace is the state namespace for API Gateway v2.
const apigatewayv2Namespace = "apigatewayv2"

// V2ApiState holds the state of an API Gateway v2 (HTTP/WebSocket) API.
type V2ApiState struct {
	// APIID is the unique identifier for the API.
	APIID string `json:"ApiId"`

	// Name is the name of the API.
	Name string `json:"Name"`

	// ProtocolType is the protocol type (HTTP or WEBSOCKET).
	ProtocolType string `json:"ProtocolType"`

	// Description is an optional description.
	Description string `json:"Description,omitempty"`

	// APIEndpoint is the default endpoint for the API.
	APIEndpoint string `json:"ApiEndpoint"`

	// Tags holds arbitrary key-value metadata.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is the time the API was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// AccountID is the AWS account that owns the API.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the API lives.
	Region string `json:"Region"`
}

// V2RouteState holds the state of an API Gateway v2 route.
type V2RouteState struct {
	// RouteID is the unique identifier for the route.
	RouteID string `json:"RouteId"`

	// RouteKey is the route expression (e.g., "GET /users" or "$default").
	RouteKey string `json:"RouteKey"`

	// Target is the integration target (e.g., "integrations/{id}").
	Target string `json:"Target,omitempty"`

	// AuthorizationType is the authorisation type for the route.
	AuthorizationType string `json:"AuthorizationType"`

	// AuthorizerID is the ID of the authorizer when authorisation is required.
	AuthorizerID string `json:"AuthorizerId,omitempty"`

	// APIID is the API that owns this route.
	APIID string `json:"ApiId"`

	// AccountID is the AWS account that owns this route.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this route lives.
	Region string `json:"Region"`
}

// V2IntegrationState holds the state of an API Gateway v2 integration.
type V2IntegrationState struct {
	// IntegrationID is the unique identifier for the integration.
	IntegrationID string `json:"IntegrationId"`

	// IntegrationType is the integration type (AWS_PROXY, HTTP_PROXY, MOCK).
	IntegrationType string `json:"IntegrationType"`

	// IntegrationURI is the integration endpoint URI.
	IntegrationURI string `json:"IntegrationUri,omitempty"`

	// PayloadFormatVersion specifies the format of the payload (e.g., "2.0").
	PayloadFormatVersion string `json:"PayloadFormatVersion,omitempty"`

	// APIID is the API that owns this integration.
	APIID string `json:"ApiId"`

	// AccountID is the AWS account that owns this integration.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this integration lives.
	Region string `json:"Region"`
}

// V2StageState holds the state of an API Gateway v2 stage.
type V2StageState struct {
	// StageName is the name of the stage.
	StageName string `json:"StageName"`

	// DeploymentID is the ID of the deployment associated with this stage.
	DeploymentID string `json:"DeploymentId,omitempty"`

	// Description is an optional description.
	Description string `json:"Description,omitempty"`

	// StageVariables holds stage-level variables.
	StageVariables map[string]string `json:"StageVariables,omitempty"`

	// Tags holds arbitrary key-value metadata.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedDate is the time the stage was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// APIID is the API that owns this stage.
	APIID string `json:"ApiId"`

	// AccountID is the AWS account that owns this stage.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this stage lives.
	Region string `json:"Region"`
}

// V2AuthorizerState holds the state of an API Gateway v2 authorizer.
type V2AuthorizerState struct {
	// AuthorizerID is the unique identifier for the authorizer.
	AuthorizerID string `json:"AuthorizerId"`

	// Name is the name of the authorizer.
	Name string `json:"Name"`

	// AuthorizerType is the authorizer type (JWT or REQUEST).
	AuthorizerType string `json:"AuthorizerType"`

	// IdentitySource is the list of request parameters that identify the caller.
	IdentitySource []string `json:"IdentitySource,omitempty"`

	// JwtConfiguration holds JWT authorizer configuration.
	JwtConfiguration interface{} `json:"JwtConfiguration,omitempty"`

	// APIID is the API that owns this authorizer.
	APIID string `json:"ApiId"`

	// AccountID is the AWS account that owns this authorizer.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this authorizer lives.
	Region string `json:"Region"`
}

// V2DeploymentState holds the state of an API Gateway v2 deployment.
type V2DeploymentState struct {
	// DeploymentID is the unique identifier for the deployment.
	DeploymentID string `json:"DeploymentId"`

	// DeploymentStatus is the status of the deployment (e.g., DEPLOYED).
	DeploymentStatus string `json:"DeploymentStatus"`

	// Description is an optional description.
	Description string `json:"Description,omitempty"`

	// CreatedDate is the time the deployment was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// APIID is the API that owns this deployment.
	APIID string `json:"ApiId"`

	// AccountID is the AWS account that owns this deployment.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this deployment lives.
	Region string `json:"Region"`
}

// V2DomainNameState holds the state of an API Gateway v2 custom domain name.
type V2DomainNameState struct {
	// DomainName is the custom domain name.
	DomainName string `json:"DomainName"`

	// APIMappingID is the ID of the API mapping for this domain.
	APIMappingID string `json:"ApiMappingId,omitempty"`

	// RegionalDomainName is the regional endpoint hostname.
	RegionalDomainName string `json:"RegionalDomainName"`

	// AccountID is the AWS account that owns this domain name.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this domain name lives.
	Region string `json:"Region"`
}

// --- State key helpers -------------------------------------------------------

func apigwv2APIKey(accountID, region, apiID string) string {
	return "apiv2:" + accountID + "/" + region + "/" + apiID
}

func apigwv2APIIDsKey(accountID, region string) string {
	return "apiv2_ids:" + accountID + "/" + region
}

func apigwv2RouteKey(accountID, region, apiID, routeID string) string {
	return "routev2:" + accountID + "/" + region + "/" + apiID + "/" + routeID
}

func apigwv2RouteIDsKey(accountID, region, apiID string) string {
	return "routev2_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwv2IntegrationKey(accountID, region, apiID, intID string) string {
	return "integrationv2:" + accountID + "/" + region + "/" + apiID + "/" + intID
}

func apigwv2IntegrationIDsKey(accountID, region, apiID string) string {
	return "integrationv2_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwv2StageKey(accountID, region, apiID, stageName string) string {
	return "stagev2:" + accountID + "/" + region + "/" + apiID + "/" + stageName
}

func apigwv2StageNamesKey(accountID, region, apiID string) string {
	return "stagev2_names:" + accountID + "/" + region + "/" + apiID
}

func apigwv2AuthorizerKey(accountID, region, apiID, authID string) string {
	return "authorizerv2:" + accountID + "/" + region + "/" + apiID + "/" + authID
}

func apigwv2AuthorizerIDsKey(accountID, region, apiID string) string {
	return "authorizerv2_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwv2DeploymentKey(accountID, region, apiID, deployID string) string {
	return "deploymentv2:" + accountID + "/" + region + "/" + apiID + "/" + deployID
}

func apigwv2DeploymentIDsKey(accountID, region, apiID string) string {
	return "deploymentv2_ids:" + accountID + "/" + region + "/" + apiID
}

func apigwv2DomainNameKey(accountID, region, name string) string {
	return "domainnamev2:" + accountID + "/" + region + "/" + name
}

func apigwv2APIMappingKey(accountID, region, domain, mappingID string) string {
	return "apimapping:" + accountID + "/" + region + "/" + domain + "/" + mappingID
}
