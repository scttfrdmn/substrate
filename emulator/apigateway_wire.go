package emulator

import "time"

// The wire is a different thing from the state, and the types below exist to keep
// them apart.
//
// The state types in apigateway_types.go are a persisted format: MemoryStateManager
// snapshots them and recorded runs replay from those bytes, so their PascalCase tags
// must not change. API Gateway v1's wire members are lowerCamel, and botocore
// matches a response key against the model's locationName case-sensitively — so a
// PascalCase key matches nothing and parses to nothing. `aws apigateway
// get-rest-apis` returned HTTP 200 with an empty result and no error, because the
// state struct was marshaled straight onto the wire (#529).
//
// So each response gets its own element type, tagged from the model, projected from
// the state by a Wire function. Two consequences are deliberate: substrate's own
// AccountID and Region are simply absent from these types, so the leak cannot come
// back through a field someone adds later; and `omitempty` follows the model's
// optionality, because real API Gateway omits an unset member rather than sending
// null.
//
// Do not "fix" a casing bug here by retagging a state type. That conflates the two
// jobs again, and it silently changes the format of every recorded run.
//
// One member is substrate's own and no model declares it: stageOut.InvokeURL. It is
// retained because a consumer reading the raw response uses it, and because botocore
// drops a key the model does not declare, so it cannot break an SDK caller. It is
// spelled lowerCamel for consistency with everything around it, not because the
// model says so.

// restAPIOut is the RestApi element of the v1 REST API responses.
type restAPIOut struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Description    string            `json:"description,omitempty"`
	RootResourceID string            `json:"rootResourceId"`
	Tags           map[string]string `json:"tags,omitempty"`
	CreatedDate    time.Time         `json:"createdDate"`
}

// resourceOut is the Resource element of the v1 resource responses. resourceMethods
// is a map of Method, so a projection that stops here would leave every method
// PascalCase and a caller would parse a resource whose methods are empty.
type resourceOut struct {
	ID              string               `json:"id"`
	ParentID        string               `json:"parentId,omitempty"`
	PathPart        string               `json:"pathPart,omitempty"`
	Path            string               `json:"path"`
	ResourceMethods map[string]methodOut `json:"resourceMethods,omitempty"`
}

// methodOut is the Method element, and the methodIntegration member of a resource's
// method map.
type methodOut struct {
	HTTPMethod        string                 `json:"httpMethod"`
	AuthorizationType string                 `json:"authorizationType"`
	AuthorizerID      string                 `json:"authorizerId,omitempty"`
	APIKeyRequired    bool                   `json:"apiKeyRequired"`
	MethodIntegration *integrationOut        `json:"methodIntegration,omitempty"`
	MethodResponses   map[string]interface{} `json:"methodResponses,omitempty"`
}

// integrationOut is the Integration element. Its integrationResponses hold whatever
// the caller PUT, so they are passed through unchanged — those are the caller's own
// keys, not substrate's to rename.
type integrationOut struct {
	Type                 string                 `json:"type"`
	URI                  string                 `json:"uri,omitempty"`
	HTTPMethod           string                 `json:"httpMethod,omitempty"`
	IntegrationResponses map[string]interface{} `json:"integrationResponses,omitempty"`
}

// stageOut is the Stage element. invokeUrl is substrate's own addition; see the
// comment at the top of this file.
type stageOut struct {
	StageName    string            `json:"stageName"`
	DeploymentID string            `json:"deploymentId,omitempty"`
	Description  string            `json:"description,omitempty"`
	Variables    map[string]string `json:"variables,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	CreatedDate  time.Time         `json:"createdDate"`
	InvokeURL    string            `json:"invokeUrl,omitempty"`
}

// deploymentOut is the Deployment element.
type deploymentOut struct {
	ID          string    `json:"id"`
	Description string    `json:"description,omitempty"`
	CreatedDate time.Time `json:"createdDate"`
}

// authorizerOut is the Authorizer element. providerARNs keeps its capitalised
// acronym, which is what the model spells.
type authorizerOut struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Type           string   `json:"type"`
	ProviderARNs   []string `json:"providerARNs,omitempty"`
	AuthorizerURI  string   `json:"authorizerUri,omitempty"`
	IdentitySource string   `json:"identitySource,omitempty"`
}

// apiKeyOut is the ApiKey element.
type apiKeyOut struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Value       string            `json:"value"`
	Enabled     bool              `json:"enabled"`
	Tags        map[string]string `json:"tags,omitempty"`
	CreatedDate time.Time         `json:"createdDate"`
}

// usagePlanOut is the UsagePlan element. The model declares no createdDate on a
// usage plan, so substrate does not report one even though it stores one.
type usagePlanOut struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	APIStages   []interface{}     `json:"apiStages,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// domainNameOut is the DomainName element.
type domainNameOut struct {
	DomainName         string `json:"domainName"`
	CertificateArn     string `json:"certificateArn,omitempty"`
	RegionalDomainName string `json:"regionalDomainName"`
}

// basePathMappingOut is the BasePathMapping element. The model declares no
// domainName on a mapping — the domain is a path parameter of the request, not a
// member of the response — so substrate no longer reports one.
type basePathMappingOut struct {
	BasePath  string `json:"basePath"`
	RestAPIID string `json:"restApiId"`
	Stage     string `json:"stage,omitempty"`
}

// --- Projections -------------------------------------------------------------

// restAPIWire projects a stored REST API onto the wire.
func restAPIWire(a RestAPIState) restAPIOut {
	return restAPIOut{
		ID:             a.ID,
		Name:           a.Name,
		Description:    a.Description,
		RootResourceID: a.RootResourceID,
		Tags:           a.Tags,
		CreatedDate:    a.CreatedDate,
	}
}

// resourceWire projects a stored resource onto the wire, recursing through its
// method map and each method's integration.
func resourceWire(r ResourceState) resourceOut {
	out := resourceOut{
		ID:       r.ID,
		ParentID: r.ParentID,
		PathPart: r.PathPart,
		Path:     r.Path,
	}
	if len(r.ResourceMethods) > 0 {
		out.ResourceMethods = make(map[string]methodOut, len(r.ResourceMethods))
		for verb, m := range r.ResourceMethods {
			out.ResourceMethods[verb] = methodWire(m)
		}
	}
	return out
}

// methodWire projects a stored method onto the wire, including its integration.
func methodWire(m MethodState) methodOut {
	out := methodOut{
		HTTPMethod:        m.HTTPMethod,
		AuthorizationType: m.AuthorizationType,
		AuthorizerID:      m.AuthorizerID,
		APIKeyRequired:    m.APIKeyRequired,
		MethodResponses:   m.MethodResponses,
	}
	if m.Integration != nil {
		i := integrationWire(*m.Integration)
		out.MethodIntegration = &i
	}
	return out
}

// integrationWire projects a stored integration onto the wire.
//
// The two types happen to have identical field sets today, so a conversion would
// compile — but that is incidental, and writing the members out keeps this
// projection readable the same way as the ten around it, and keeps adding a
// state-only field (an AccountID, say) a local edit here rather than a compile
// error in an unrelated line.
func integrationWire(i IntegrationState) integrationOut {
	return integrationOut{ //nolint:staticcheck // S1016: an explicit projection, not a coincidental conversion.
		Type:                 i.Type,
		URI:                  i.URI,
		HTTPMethod:           i.HTTPMethod,
		IntegrationResponses: i.IntegrationResponses,
	}
}

// stageWire projects a stored stage onto the wire. invokeURL is passed in rather
// than derived here, because it depends on the request's region and API id.
func stageWire(s StageState, invokeURL string) stageOut {
	return stageOut{
		StageName:    s.StageName,
		DeploymentID: s.DeploymentID,
		Description:  s.Description,
		Variables:    s.Variables,
		Tags:         s.Tags,
		CreatedDate:  s.CreatedDate,
		InvokeURL:    invokeURL,
	}
}

// deploymentWire projects a stored deployment onto the wire.
func deploymentWire(d DeploymentState) deploymentOut {
	return deploymentOut{ID: d.ID, Description: d.Description, CreatedDate: d.CreatedDate}
}

// authorizerWire projects a stored authorizer onto the wire.
func authorizerWire(a AuthorizerState) authorizerOut {
	return authorizerOut{
		ID:             a.ID,
		Name:           a.Name,
		Type:           a.Type,
		ProviderARNs:   a.ProviderARNs,
		AuthorizerURI:  a.AuthorizerURI,
		IdentitySource: a.IdentitySource,
	}
}

// apiKeyWire projects a stored API key onto the wire.
func apiKeyWire(k APIKeyState) apiKeyOut {
	return apiKeyOut{
		ID:          k.ID,
		Name:        k.Name,
		Value:       k.Value,
		Enabled:     k.Enabled,
		Tags:        k.Tags,
		CreatedDate: k.CreatedDate,
	}
}

// usagePlanWire projects a stored usage plan onto the wire.
func usagePlanWire(u UsagePlanState) usagePlanOut {
	return usagePlanOut{
		ID:          u.ID,
		Name:        u.Name,
		Description: u.Description,
		APIStages:   u.APIStages,
		Tags:        u.Tags,
	}
}

// domainNameWire projects a stored domain name onto the wire.
func domainNameWire(d DomainNameState) domainNameOut {
	return domainNameOut{
		DomainName:         d.DomainName,
		CertificateArn:     d.CertificateArn,
		RegionalDomainName: d.RegionalDomainName,
	}
}

// basePathMappingWire projects a stored base path mapping onto the wire.
func basePathMappingWire(m BasePathMappingState) basePathMappingOut {
	return basePathMappingOut{BasePath: m.BasePath, RestAPIID: m.RestAPIID, Stage: m.Stage}
}

// --- List envelopes ----------------------------------------------------------

// Every v1 collection response nests its elements under the member named "items"
// — whose locationName is "item", singular. GetStages is the one exception, and only
// in the model's member name: its member is literally called "item", and its
// locationName is "item" too. So on the wire all twenty collections agree, and
// substrate emits "item" for every one of them. Emitting "items" parses to nothing.
//
// No collection carries a "position" token, because substrate returns every element
// in one page and does not honor one; inventing a token would invite a caller to
// page on nothing. GetUsage's member is a third spelling, "values", but substrate
// does not route GetUsage.
type apigwItemsOut[T any] struct {
	Item []T `json:"item"`
}
