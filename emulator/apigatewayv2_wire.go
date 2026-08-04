package emulator

import "time"

// The wire is a different thing from the state, and the types below keep them
// apart — the same separation as apigateway_wire.go (#529) and
// cloudwatchlogs_types.go (#528), for the same reason.
//
// The state types in apigatewayv2_types.go are a persisted format:
// MemoryStateManager snapshots them and recorded runs replay from those bytes, so
// their PascalCase tags must not change. API Gateway v2's wire members are
// lowerCamel, and botocore matches a response key against the model's
// locationName case-sensitively — so a PascalCase key matches nothing and parses
// to nothing. `aws apigatewayv2 create-api --query ApiId` printed None, because
// the state struct was marshaled straight onto the wire.
//
// v2 is more regular than v1: every member of every v2 response shape is the
// plain lowerCamel of its member name. The only irregular spellings in the whole
// model — ResourceArn -> "resource-arn" — are on the three tag *request* shapes,
// which substrate does not route. Each tag below is still transcribed from the
// model rather than lowercased programmatically, because a mechanical transform
// is exactly what would silently get an exception wrong.
//
// Two consequences are deliberate: substrate's own AccountID and Region are
// simply absent from these types, so the leak cannot come back through a field
// someone adds later; and `omitempty` follows the model's optionality, because
// real API Gateway omits an unset member rather than sending null.
//
// Do not "fix" a casing bug here by retagging a state type. That conflates the
// two jobs again, and it silently changes the format of every recorded run.

// v2APIOut is the Api element of the v2 API responses.
type v2APIOut struct {
	APIID        string            `json:"apiId"`
	Name         string            `json:"name"`
	ProtocolType string            `json:"protocolType"`
	Description  string            `json:"description,omitempty"`
	APIEndpoint  string            `json:"apiEndpoint"`
	Tags         map[string]string `json:"tags,omitempty"`
	CreatedDate  time.Time         `json:"createdDate"`
}

// v2RouteOut is the Route element. The Route shape declares no apiId — the API is
// a path parameter of the request, not a member of the response — so substrate
// reports none, even though it stores one.
type v2RouteOut struct {
	RouteID           string `json:"routeId"`
	RouteKey          string `json:"routeKey"`
	Target            string `json:"target,omitempty"`
	AuthorizationType string `json:"authorizationType,omitempty"`
	AuthorizerID      string `json:"authorizerId,omitempty"`
}

// v2IntegrationOut is the Integration element. As with a route, the Integration
// shape declares no apiId.
type v2IntegrationOut struct {
	IntegrationID        string `json:"integrationId"`
	IntegrationType      string `json:"integrationType,omitempty"`
	IntegrationURI       string `json:"integrationUri,omitempty"`
	PayloadFormatVersion string `json:"payloadFormatVersion,omitempty"`
}

// v2StageOut is the Stage element. The Stage shape declares no apiId.
type v2StageOut struct {
	StageName      string            `json:"stageName"`
	DeploymentID   string            `json:"deploymentId,omitempty"`
	Description    string            `json:"description,omitempty"`
	StageVariables map[string]string `json:"stageVariables,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	CreatedDate    time.Time         `json:"createdDate"`
}

// v2AuthorizerOut is the Authorizer element. The model spells the JWT member
// jwtConfiguration, not jwtConfig — its shape is named JWTConfiguration but the
// member's locationName is what goes on the wire.
type v2AuthorizerOut struct {
	AuthorizerID     string      `json:"authorizerId"`
	Name             string      `json:"name,omitempty"`
	AuthorizerType   string      `json:"authorizerType,omitempty"`
	IdentitySource   []string    `json:"identitySource,omitempty"`
	JwtConfiguration interface{} `json:"jwtConfiguration,omitempty"`
}

// v2DeploymentOut is the Deployment element.
type v2DeploymentOut struct {
	DeploymentID     string    `json:"deploymentId"`
	DeploymentStatus string    `json:"deploymentStatus"`
	Description      string    `json:"description,omitempty"`
	CreatedDate      time.Time `json:"createdDate"`
}

// v2DomainNameConfigurationOut is one element of a domain name's
// domainNameConfigurations list.
//
// This is where v2 reports the regional hostname: the DomainName shape has no
// top-level regionalDomainName member — that spelling is v1's — and instead
// nests apiGatewayDomainName here. Substrate computes the hostname either way, so
// projecting it into this list is what makes it observable to an SDK rather than
// silently dropped as an undeclared key.
type v2DomainNameConfigurationOut struct {
	APIGatewayDomainName string `json:"apiGatewayDomainName"`
	EndpointType         string `json:"endpointType"`
	DomainNameStatus     string `json:"domainNameStatus"`
}

// v2DomainNameOut is the DomainName element.
type v2DomainNameOut struct {
	DomainName               string                         `json:"domainName"`
	DomainNameConfigurations []v2DomainNameConfigurationOut `json:"domainNameConfigurations"`
}

// v2APIMappingOut is the ApiMapping element. The model declares no domainName on
// a mapping, for the same reason a route declares no apiId.
type v2APIMappingOut struct {
	APIMappingID  string `json:"apiMappingId"`
	APIID         string `json:"apiId"`
	Stage         string `json:"stage"`
	APIMappingKey string `json:"apiMappingKey,omitempty"`
}

// --- Projections -------------------------------------------------------------

// v2APIWire projects a stored API onto the wire.
func v2APIWire(a V2ApiState) v2APIOut {
	return v2APIOut{
		APIID:        a.APIID,
		Name:         a.Name,
		ProtocolType: a.ProtocolType,
		Description:  a.Description,
		APIEndpoint:  a.APIEndpoint,
		Tags:         a.Tags,
		CreatedDate:  a.CreatedDate,
	}
}

// v2RouteWire projects a stored route onto the wire.
func v2RouteWire(r V2RouteState) v2RouteOut {
	return v2RouteOut{
		RouteID:           r.RouteID,
		RouteKey:          r.RouteKey,
		Target:            r.Target,
		AuthorizationType: r.AuthorizationType,
		AuthorizerID:      r.AuthorizerID,
	}
}

// v2IntegrationWire projects a stored integration onto the wire.
func v2IntegrationWire(i V2IntegrationState) v2IntegrationOut {
	return v2IntegrationOut{
		IntegrationID:        i.IntegrationID,
		IntegrationType:      i.IntegrationType,
		IntegrationURI:       i.IntegrationURI,
		PayloadFormatVersion: i.PayloadFormatVersion,
	}
}

// v2StageWire projects a stored stage onto the wire.
func v2StageWire(s V2StageState) v2StageOut {
	return v2StageOut{
		StageName:      s.StageName,
		DeploymentID:   s.DeploymentID,
		Description:    s.Description,
		StageVariables: s.StageVariables,
		Tags:           s.Tags,
		CreatedDate:    s.CreatedDate,
	}
}

// v2AuthorizerWire projects a stored authorizer onto the wire.
func v2AuthorizerWire(a V2AuthorizerState) v2AuthorizerOut {
	return v2AuthorizerOut{
		AuthorizerID:     a.AuthorizerID,
		Name:             a.Name,
		AuthorizerType:   a.AuthorizerType,
		IdentitySource:   a.IdentitySource,
		JwtConfiguration: a.JwtConfiguration,
	}
}

// v2DeploymentWire projects a stored deployment onto the wire.
func v2DeploymentWire(d V2DeploymentState) v2DeploymentOut {
	return v2DeploymentOut{
		DeploymentID:     d.DeploymentID,
		DeploymentStatus: d.DeploymentStatus,
		Description:      d.Description,
		CreatedDate:      d.CreatedDate,
	}
}

// v2DomainNameWire projects a stored domain name onto the wire, nesting the
// regional hostname where the model puts it.
func v2DomainNameWire(d V2DomainNameState) v2DomainNameOut {
	return v2DomainNameOut{
		DomainName: d.DomainName,
		DomainNameConfigurations: []v2DomainNameConfigurationOut{{
			APIGatewayDomainName: d.RegionalDomainName,
			EndpointType:         "REGIONAL",
			DomainNameStatus:     "AVAILABLE",
		}},
	}
}

// v2APIMappingWire projects a stored API mapping onto the wire.
func v2APIMappingWire(m v2APIMappingState) v2APIMappingOut {
	return v2APIMappingOut{
		APIMappingID:  m.APIMappingID,
		APIID:         m.APIID,
		Stage:         m.Stage,
		APIMappingKey: m.APIMappingKey,
	}
}

// --- List envelopes ----------------------------------------------------------

// Every v2 collection response nests its elements under "items" — lowercase,
// which is what the member's locationName spells. Emitting "Items" parses to
// nothing.
//
// nextToken is omitted rather than sent empty, because substrate returns every
// element in one page and honors no token; emitting one would invite a caller to
// page on nothing.
type apigwV2ItemsOut[T any] struct {
	Items     []T    `json:"items"`
	NextToken string `json:"nextToken,omitempty"`
}
