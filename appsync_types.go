package substrate

import (
	"fmt"
	"strings"
)

// appSyncNamespace is the canonical service name for AppSync.
const appSyncNamespace = "appsync"

// AppSyncGraphQLApi represents an AWS AppSync GraphQL API.
type AppSyncGraphQLApi struct {
	APIID              string            `json:"apiId"`
	Name               string            `json:"name"`
	AuthenticationType string            `json:"authenticationType"`
	APIARN             string            `json:"apiArn"`
	URIS               map[string]string `json:"uris,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	XrayEnabled        bool              `json:"xrayEnabled,omitempty"`
	Region             string            `json:"region,omitempty"`
	AccountID          string            `json:"accountId,omitempty"`
}

// AppSyncDataSource represents a data source attached to an AppSync GraphQL API.
type AppSyncDataSource struct {
	APIID          string `json:"apiId"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	Description    string `json:"description,omitempty"`
	ServiceRoleARN string `json:"serviceRoleArn,omitempty"`
	DataSourceARN  string `json:"dataSourceArn"`
}

// AppSyncResolver represents a field resolver in an AppSync GraphQL API.
type AppSyncResolver struct {
	APIID                   string `json:"apiId"`
	TypeName                string `json:"typeName"`
	FieldName               string `json:"fieldName"`
	DataSourceName          string `json:"dataSourceName,omitempty"`
	Kind                    string `json:"kind"`
	RequestMappingTemplate  string `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string `json:"responseMappingTemplate,omitempty"`
	ResolverARN             string `json:"resolverArn"`
}

// AppSyncFunction represents an AppSync pipeline function.
type AppSyncFunction struct {
	APIID          string `json:"apiId"`
	FunctionID     string `json:"functionId"`
	Name           string `json:"name"`
	DataSourceName string `json:"dataSourceName"`
	Description    string `json:"description,omitempty"`
	FunctionARN    string `json:"functionArn"`
}

// State key helpers (return the key portion; namespace is always appSyncNamespace).

func appSyncAPIKey(acct, region, apiID string) string {
	return fmt.Sprintf("api:%s/%s/%s", acct, region, apiID)
}

func appSyncAPIIDsKey(acct, region string) string {
	return fmt.Sprintf("api_ids:%s/%s", acct, region)
}

func appSyncDataSourceKey(acct, region, apiID, name string) string {
	return fmt.Sprintf("ds:%s/%s/%s/%s", acct, region, apiID, name)
}

func appSyncDataSourceNamesKey(acct, region, apiID string) string {
	return fmt.Sprintf("ds_names:%s/%s/%s", acct, region, apiID)
}

func appSyncResolverKey(acct, region, apiID, typeName, fieldName string) string {
	return fmt.Sprintf("resolver:%s/%s/%s/%s/%s", acct, region, apiID, typeName, fieldName)
}

func appSyncResolverKeysKey(acct, region, apiID string) string {
	return fmt.Sprintf("resolver_keys:%s/%s/%s", acct, region, apiID)
}

func appSyncFunctionKey(acct, region, apiID, funcID string) string {
	return fmt.Sprintf("func:%s/%s/%s/%s", acct, region, apiID, funcID)
}

func appSyncFunctionIDsKey(acct, region, apiID string) string {
	return fmt.Sprintf("func_ids:%s/%s/%s", acct, region, apiID)
}

func appSyncSchemaKey(acct, region, apiID string) string {
	return fmt.Sprintf("schema:%s/%s/%s", acct, region, apiID)
}

// generateAppSyncAPIID returns a new unique AppSync API ID (13 lowercase hex chars).
func generateAppSyncAPIID() string {
	return randomHex(13)
}

// generateAppSyncFunctionID returns a new unique AppSync function ID.
func generateAppSyncFunctionID() string {
	return randomHex(26)
}

// parseAppSyncOperation derives the AppSync operation name from the HTTP method
// and URL path. It also returns the apiID and any resource segment/ID when
// present.
//
// API path patterns:
//
//	POST   /v1/apis                               → CreateGraphqlApi
//	GET    /v1/apis                               → ListGraphqlApis
//	GET    /v1/apis/{apiId}                       → GetGraphqlApi
//	POST   /v1/apis/{apiId}                       → UpdateGraphqlApi
//	DELETE /v1/apis/{apiId}                       → DeleteGraphqlApi
//	POST   /v1/apis/{apiId}/DataSources           → CreateDataSource
//	GET    /v1/apis/{apiId}/DataSources           → ListDataSources
//	GET    /v1/apis/{apiId}/DataSources/{name}    → GetDataSource
//	POST   /v1/apis/{apiId}/DataSources/{name}    → UpdateDataSource
//	DELETE /v1/apis/{apiId}/DataSources/{name}    → DeleteDataSource
//	POST   /v1/apis/{apiId}/types/{typeName}/resolvers           → CreateResolver
//	GET    /v1/apis/{apiId}/types/{typeName}/resolvers           → ListResolvers
//	GET    /v1/apis/{apiId}/types/{typeName}/resolvers/{field}   → GetResolver
//	POST   /v1/apis/{apiId}/types/{typeName}/resolvers/{field}   → UpdateResolver
//	DELETE /v1/apis/{apiId}/types/{typeName}/resolvers/{field}   → DeleteResolver
//	POST   /v1/apis/{apiId}/functions             → CreateFunction
//	GET    /v1/apis/{apiId}/functions             → ListFunctions
//	GET    /v1/apis/{apiId}/functions/{functionId} → GetFunction
//	DELETE /v1/apis/{apiId}/functions/{functionId} → DeleteFunction
//	POST   /v1/apis/{apiId}/schemacreation        → StartSchemaCreation
//	GET    /v1/apis/{apiId}/schema                → GetIntrospectionSchema
//	POST   /graphql                               → ExecuteGraphQL
func parseAppSyncOperation(method, path string) (op, apiID, segment, resourceID string) {
	// GraphQL execution endpoint.
	if path == "/graphql" || strings.HasPrefix(path, "/graphql?") {
		return "ExecuteGraphQL", "", "", ""
	}

	// Trim leading /v1 prefix.
	trimmed := strings.TrimPrefix(path, "/v1")

	if !strings.HasPrefix(trimmed, "/apis") {
		return "UnknownOperation", "", "", ""
	}
	rest := strings.TrimPrefix(trimmed, "/apis")
	rest = strings.TrimPrefix(rest, "/")

	if rest == "" {
		switch method {
		case "POST":
			return "CreateGraphqlApi", "", "", ""
		case "GET":
			return "ListGraphqlApis", "", "", ""
		}
		return "UnknownOperation", "", "", ""
	}

	parts := strings.SplitN(rest, "/", 3)
	apiID = parts[0]

	if len(parts) == 1 {
		switch method {
		case "GET":
			return "GetGraphqlApi", apiID, "", ""
		case "POST":
			return "UpdateGraphqlApi", apiID, "", ""
		case "DELETE":
			return "DeleteGraphqlApi", apiID, "", ""
		}
		return "UnknownOperation", apiID, "", ""
	}

	segment = parts[1]
	tail := ""
	if len(parts) == 3 {
		tail = parts[2]
	}

	switch segment {
	case "DataSources":
		if tail == "" {
			switch method {
			case "POST":
				return "CreateDataSource", apiID, segment, ""
			case "GET":
				return "ListDataSources", apiID, segment, ""
			}
		} else {
			name := strings.SplitN(tail, "/", 2)[0]
			switch method {
			case "GET":
				return "GetDataSource", apiID, segment, name
			case "POST":
				return "UpdateDataSource", apiID, segment, name
			case "DELETE":
				return "DeleteDataSource", apiID, segment, name
			}
		}
	case "types":
		typeParts := strings.SplitN(tail, "/", 3)
		typeName := ""
		if len(typeParts) > 0 {
			typeName = typeParts[0]
		}
		resource := ""
		if len(typeParts) > 1 {
			resource = typeParts[1]
		}
		fieldName := ""
		if len(typeParts) > 2 {
			fieldName = typeParts[2]
		}
		if resource == "resolvers" {
			if fieldName == "" {
				switch method {
				case "POST":
					return "CreateResolver", apiID, typeName, ""
				case "GET":
					return "ListResolvers", apiID, typeName, ""
				}
			} else {
				switch method {
				case "GET":
					return "GetResolver", apiID, typeName, fieldName
				case "POST":
					return "UpdateResolver", apiID, typeName, fieldName
				case "DELETE":
					return "DeleteResolver", apiID, typeName, fieldName
				}
			}
		}
	case "functions":
		if tail == "" {
			switch method {
			case "POST":
				return "CreateFunction", apiID, segment, ""
			case "GET":
				return "ListFunctions", apiID, segment, ""
			}
		} else {
			funcID := strings.SplitN(tail, "/", 2)[0]
			switch method {
			case "GET":
				return "GetFunction", apiID, segment, funcID
			case "DELETE":
				return "DeleteFunction", apiID, segment, funcID
			}
		}
	case "schemacreation":
		return "StartSchemaCreation", apiID, segment, ""
	case "schema":
		return "GetIntrospectionSchema", apiID, segment, ""
	}

	return "UnknownOperation", apiID, segment, resourceID
}
