# API Gateway (REST)

**Endpoint:** `apigateway.{region}.amazonaws.com`
**Protocol:** REST/JSON

**Response shape:** every member is lowerCamel as the service model spells it
(`id`, `name`, `rootResourceId`, `resourceMethods`, `methodIntegration`, …), and
collection responses nest their elements under **`item`** — singular, because that
is the `locationName` of the `items` member. `GetUsage` uses a third spelling,
`values`, and is not routed.

**Every collection whose URI publishes `limit` and `position` now reads the pair** —
`GetBasePathMappings`, `GetRestApis`, `GetResources`, `GetDeployments`,
`GetAuthorizers`, `GetApiKeys` and `GetUsagePlans`, seven of the eight — see [Two
more cursors published and
unread](/reference/concepts/listing-order#two-more-cursors-published-and-unread-outside-ec2) for the first and [Six
more v1 collections read the
pair](/reference/concepts/listing-order#six-more-v1-collections-read-the-pair-they-publish) for the other six (#1025).
A collection that fits in one page leaves the member unset, so it is omitted rather
than sent empty: a caller must not be handed a token for a page that does not exist.
The eighth collection, `GetStages`, publishes neither parameter and lists no
`position` response member, so its single page is what AWS describes rather than a
gap. Seven of the eight handlers also could not read a query parameter as written —
their signatures took no request — which is why `GetBasePathMappings` converted
without a signature change and each of the six needed one; `GetStages` has
nothing to read, so its signature stays as it is. Earlier releases sent
PascalCase members under an `items` envelope, which an AWS SDK parsed to an empty
result with no error (#529).

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateRestApi | Auto-creates root `/` resource |
| GetRestApi | |
| UpdateRestApi | `404 NotFoundException` for an unknown API; `patchOperations` is **not** applied — the stored API is reported back unchanged |
| DeleteRestApi | |
| GetRestApis | Pages on `limit`/`position`; ascending API ID (#1025) |
| CreateResource | |
| GetResource | |
| DeleteResource | |
| GetResources | Pages on `limit`/`position`; ascending resource ID. `embed` unread — methods always reported (#1025) |
| PutMethod | |
| GetMethod | |
| DeleteMethod | |
| PutIntegration | `GetIntegration` is **not** modelled — see below |
| PutIntegrationResponse | Echoes the request body back at `201` and stores nothing, so the response is not readable afterwards; a body that will not parse is `BadRequestException`/400 |
| PutMethodResponse | Echoes the request body back at `201` and stores nothing, so `GetMethod` does not report it; a body that will not parse is `BadRequestException`/400 |
| CreateDeployment | |
| GetDeployment | |
| GetDeployments | Pages on `limit`/`position`; ascending deployment ID, which is unrelated to `createdDate` (#1025) |
| DeleteDeployment | `204`, and idempotent: a deployment ID with nothing behind it is not refused |
| CreateStage | |
| GetStage | |
| GetStages | One page, always, under the `item` envelope — the operation publishes neither `limit` nor `position`, as above. Each stage reports its `invokeUrl` |
| UpdateStage | `404 NotFoundException` for an unknown stage; `patchOperations` is **not** applied — the stored stage is reported back unchanged |
| DeleteStage | `204`, and idempotent: a stage name with nothing behind it is not refused |
| CreateAuthorizer | |
| GetAuthorizer | |
| GetAuthorizers | Pages on `limit`/`position`; ascending authorizer ID (#1025) |
| DeleteAuthorizer | `204`, and idempotent: an authorizer ID with nothing behind it is not refused |
| CreateApiKey | |
| GetApiKey | |
| GetApiKeys | Pages on `limit`/`position`; ascending key ID. `customerId`, `includeValues` and `name` unread; the published `warnings` member is unmodelled and omitted (#1025) |
| DeleteApiKey | |
| CreateUsagePlan | |
| GetUsagePlan | |
| GetUsagePlans | Pages on `limit`/`position`; ascending plan ID. `keyId` unread (#1025) |
| DeleteUsagePlan | |
| CreateUsagePlanKey | Mints a key ID and reports `{id, type: "API_KEY"}`; the association is not stored, so neither the plan nor the API key records it, and there is no `GetUsagePlanKeys` to read it back |
| CreateDomainName | Stores `domainName` and `certificateArn`, and derives `regionalDomainName` from the name and the caller's Region. `distributionDomainName`, the endpoint configuration and the mutual-TLS members are not modelled |
| GetDomainName | `404 NotFoundException` for a domain name the account and Region hold no record of |
| CreateBasePathMapping | Keyed by base path within the domain, so a second mapping on the same base path replaces the first; an empty `basePath` is stored as `(none)`, as AWS reports it. The domain name is taken from the URI and is not required to exist |
| GetBasePathMappings | Pages on `limit`/`position`; ascending base path within the domain (#917) |

**Not modelled: `GetIntegration`.** AWS publishes it on this API — `GET
/restapis/{restapi_id}/resources/{resource_id}/methods/{http_method}/integration` — and this
table claimed it until #1015. The path is parsed, but only `PUT` on it routes, so a `GET`
reaches the unknown-action refusal. What made the wrong row hard to see is that API Gateway
v2 *does* route `GetIntegration`, so the name is in the reference twice and only one of the two
is real.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ApiGateway::RestApi | RestApiId | |
| AWS::ApiGateway::Resource | ResourceId | |
| AWS::ApiGateway::Method | method ID | Generated `{stack}-{logical}-{suffix}`, matching AWS's documented example (#843) |
| AWS::ApiGateway::Deployment | DeploymentId | |
| AWS::ApiGateway::Stage | StageName | |

## Cost

API Gateway REST API calls: $3.50 per million calls.
