package emulator

// AppSync's wire shapes, projected from the persisted records in appsync_types.go (#1121).
//
// Four records were handed straight to the caller by every operation that answers one, and the
// persisted struct is not the published shape:
//
//   - AppSyncGraphQLApi spells the ARN apiArn, where API_GraphqlApi publishes **arn**. So
//     `out.GraphqlApi.Arn` decoded to "" with no error at CreateGraphqlApi, GetGraphqlApi,
//     UpdateGraphqlApi and ListGraphqlApis — the #1017 class, a silent wrong answer rather than a
//     refusal. The value substrate computes is right; only the member name was wrong.
//   - The same record carries region and accountId, which API_GraphqlApi publishes nowhere. That is
//     #756's class, and AppSync is not in the inventory #756 will produce because its worked
//     instance is ECR (#1090).
//   - AppSyncDataSource, AppSyncResolver and AppSyncFunction each carry an apiId. API_DataSource,
//     API_Resolver and API_FunctionConfiguration publish no such member: the API is the path
//     segment the request was addressed to, not data the shape carries. API Gateway v2 records the
//     identical reading for its Route shape (apigatewayv2_wire_test.go).
//
// Why a projection and not a retagged field. The record is what state.Put writes, so renaming
// apiArn to arn in place would make every already-stored API read back with an empty ARN, and
// dropping region/accountId in place would change the bytes MemoryStateManager snapshots and a
// replay reads. That is the state-shape consideration #1071 raised for Step Functions'
// ErrorDetails; the answer here is the one ECR reached for in ecr_wire.go, which #529 established
// for API Gateway v1 and #1013 repeated for DynamoDB — the state record stays internal, and the
// response is rendered from a type tagged from the API model. json:"-" is deliberately not used:
// it fixes one field and leaves the next to be remembered rather than prevented, and on a
// persisted struct it silently changes the recorded format.
//
// Only the members substrate actually models are declared. API_GraphqlApi publishes 22 and
// API_Resolver 14; the rest are absent from the type rather than present and empty, which is
// #1013's rule — a response reports nothing AWS would not.

// appsyncGraphqlAPIOut is the graphqlApi element of the four operations that answer an API:
// CreateGraphqlApi, GetGraphqlApi, UpdateGraphqlApi and — under the name graphqlApis —
// ListGraphqlApis.
//
// Member names follow API_GraphqlApi, whose 22 members are all Required: No. `arn` is the member
// this type exists for.
type appsyncGraphqlAPIOut struct {
	APIID              string            `json:"apiId"`
	Name               string            `json:"name"`
	AuthenticationType string            `json:"authenticationType"`
	ARN                string            `json:"arn"`
	URIS               map[string]string `json:"uris,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	XrayEnabled        bool              `json:"xrayEnabled,omitempty"`
}

// appsyncAPIToWire projects a persisted API onto the published shape.
func appsyncAPIToWire(api AppSyncGraphQLApi) appsyncGraphqlAPIOut {
	return appsyncGraphqlAPIOut{
		APIID:              api.APIID,
		Name:               api.Name,
		AuthenticationType: api.AuthenticationType,
		ARN:                api.APIARN,
		URIS:               api.URIS,
		Tags:               api.Tags,
		XrayEnabled:        api.XrayEnabled,
	}
}

// appsyncAPIsToWire projects a slice of persisted APIs, for ListGraphqlApis.
func appsyncAPIsToWire(apis []AppSyncGraphQLApi) []appsyncGraphqlAPIOut {
	out := make([]appsyncGraphqlAPIOut, 0, len(apis))
	for _, api := range apis {
		out = append(out, appsyncAPIToWire(api))
	}
	return out
}

// appsyncDataSourceOut is the dataSource element of CreateDataSource, GetDataSource,
// UpdateDataSource and — under the name dataSources — ListDataSources.
//
// Member names follow API_DataSource, which publishes no apiId.
type appsyncDataSourceOut struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Description    string `json:"description,omitempty"`
	ServiceRoleARN string `json:"serviceRoleArn,omitempty"`
	DataSourceARN  string `json:"dataSourceArn"`
}

// appsyncDataSourceToWire projects a persisted data source onto the published shape.
func appsyncDataSourceToWire(ds AppSyncDataSource) appsyncDataSourceOut {
	return appsyncDataSourceOut{
		Name:           ds.Name,
		Type:           ds.Type,
		Description:    ds.Description,
		ServiceRoleARN: ds.ServiceRoleARN,
		DataSourceARN:  ds.DataSourceARN,
	}
}

// appsyncDataSourcesToWire projects a slice of persisted data sources, for ListDataSources.
func appsyncDataSourcesToWire(list []AppSyncDataSource) []appsyncDataSourceOut {
	out := make([]appsyncDataSourceOut, 0, len(list))
	for _, ds := range list {
		out = append(out, appsyncDataSourceToWire(ds))
	}
	return out
}

// appsyncResolverOut is the resolver element of CreateResolver, GetResolver, UpdateResolver and —
// under the name resolvers — ListResolvers.
//
// Member names follow API_Resolver, which publishes no apiId.
type appsyncResolverOut struct {
	TypeName                string `json:"typeName"`
	FieldName               string `json:"fieldName"`
	DataSourceName          string `json:"dataSourceName,omitempty"`
	Kind                    string `json:"kind"`
	RequestMappingTemplate  string `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string `json:"responseMappingTemplate,omitempty"`
	ResolverARN             string `json:"resolverArn"`
}

// appsyncResolverToWire projects a persisted resolver onto the published shape.
func appsyncResolverToWire(res AppSyncResolver) appsyncResolverOut {
	return appsyncResolverOut{
		TypeName:                res.TypeName,
		FieldName:               res.FieldName,
		DataSourceName:          res.DataSourceName,
		Kind:                    res.Kind,
		RequestMappingTemplate:  res.RequestMappingTemplate,
		ResponseMappingTemplate: res.ResponseMappingTemplate,
		ResolverARN:             res.ResolverARN,
	}
}

// appsyncResolversToWire projects a slice of persisted resolvers, for ListResolvers.
func appsyncResolversToWire(list []AppSyncResolver) []appsyncResolverOut {
	out := make([]appsyncResolverOut, 0, len(list))
	for _, res := range list {
		out = append(out, appsyncResolverToWire(res))
	}
	return out
}

// appsyncFunctionOut is the functionConfiguration element of CreateFunction and GetFunction, and —
// under the name functions — ListFunctions.
//
// Member names follow API_FunctionConfiguration, which publishes no apiId.
type appsyncFunctionOut struct {
	FunctionID     string `json:"functionId"`
	Name           string `json:"name"`
	DataSourceName string `json:"dataSourceName"`
	Description    string `json:"description,omitempty"`
	FunctionARN    string `json:"functionArn"`
}

// appsyncFunctionToWire projects a persisted function onto the published shape.
func appsyncFunctionToWire(fn AppSyncFunction) appsyncFunctionOut {
	return appsyncFunctionOut{
		FunctionID:     fn.FunctionID,
		Name:           fn.Name,
		DataSourceName: fn.DataSourceName,
		Description:    fn.Description,
		FunctionARN:    fn.FunctionARN,
	}
}

// appsyncFunctionsToWire projects a slice of persisted functions, for ListFunctions.
func appsyncFunctionsToWire(list []AppSyncFunction) []appsyncFunctionOut {
	out := make([]appsyncFunctionOut, 0, len(list))
	for _, fn := range list {
		out = append(out, appsyncFunctionToWire(fn))
	}
	return out
}
