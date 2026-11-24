# API Gateway v2 (HTTP)

**Endpoint:** `apigateway.{region}.amazonaws.com`
**Protocol:** REST/JSON (`/v2/` prefix)

**Routing:** v1 and v2 are one endpoint and one SigV4 signing name — an
apigatewayv2 client signs as `apigateway`, uses the same hostname, and sends no
`X-Amz-Target` — so Substrate discriminates them by **path**: a request under
`/v2/` that would otherwise resolve to `apigateway` is routed to the v2 plugin.
Every requestUri in the apigatewayv2 API is under `/v2/` and none of v1's is, so
the split is exact. A consumer pointing an `apigatewayv2` client at Substrate needs
no special configuration.

**Response shape:** every member is lowerCamel as the service model spells it
(`apiId`, `routeId`, `integrationId`, `apiEndpoint`, `protocolType`, …), and
collection responses nest their elements under **`items`** — lowercase, unlike v1's
singular `item`. Substrate returns every element in one page and honours no
pagination token, so no response carries a `nextToken`. Two members v1 has are
absent here because the v2 model does not declare them: a route, integration, stage
or API mapping reports no `apiId` (the API is a path parameter of the request), and
a domain name reports no `regionalDomainName` — v2 nests that hostname as
`domainNameConfigurations[].apiGatewayDomainName`. Earlier releases sent PascalCase
members under an `Items` envelope, which an AWS SDK parsed to an empty result with
no error (#529).

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateApi | |
| GetApi | |
| UpdateApi | Applies `Name` and `Description` when non-empty and ignores every other member, including `RouteSelectionExpression` and the CORS configuration; `404 NotFoundException` for an unknown API |
| DeleteApi | |
| GetApis | Pages by `maxResults`/`nextToken`, in ascending API ID — see [The v2 collections page](#the-v2-collections-page) |
| CreateRoute | |
| GetRoute | |
| GetRoutes | Pages by `maxResults`/`nextToken`, in ascending route ID |
| UpdateRoute | Patches `routeKey`, `target`, `authorizationType` and `authorizerId`; a member absent from the body is left as it was (see *An update changes only what it names*). `404 NotFoundException` for an unknown API or route, `400 BadRequestException` for a body that does not decode |
| DeleteRoute | |
| CreateIntegration | |
| GetIntegration | |
| GetIntegrations | Pages by `maxResults`/`nextToken`, in ascending integration ID |
| DeleteIntegration | `204`, and idempotent: an integration ID with nothing behind it is not refused |
| CreateStage | |
| GetStage | |
| GetStages | Pages by `maxResults`/`nextToken`, in ascending stage name |
| UpdateStage | Patches `autoDeploy`, `accessLogSettings`, `defaultRouteSettings`, `description`, `stageVariables` and `deploymentId`; a member absent from the body is left as it was, and `lastUpdatedDate` is set from the simulated clock. `404 NotFoundException` for an unknown API or stage, `400 BadRequestException` for a body that does not decode |
| DeleteStage | `204`, and idempotent: a stage name with nothing behind it is not refused |
| CreateAuthorizer | |
| GetAuthorizer | `404 NotFoundException` for an unknown authorizer |
| GetAuthorizers | Pages by `maxResults`/`nextToken`, in ascending authorizer ID |
| DeleteAuthorizer | `204`, and idempotent: an authorizer ID with nothing behind it is not refused |
| CreateDeployment | Reports `DeploymentStatus` `DEPLOYED` at once and dates the deployment from the simulated clock; the API id is taken from the path and not checked, so a deployment can be recorded against an API that does not exist |
| GetDeployment | `404 NotFoundException` for an unknown deployment |
| CreateDomainName | Reads `DomainName` only, and reports one `domainNameConfigurations` entry — `apiGatewayDomainName` derived from the name and the caller's Region, `endpointType` `REGIONAL`, `domainNameStatus` `AVAILABLE`. `DomainNameConfigurations` and `MutualTlsAuthentication` in the request are not read |
| GetDomainName | `404 NotFoundException` for a domain name the account and Region hold no record of |
| CreateApiMapping | Mints an `apiMappingId`. `apiId` and `stage` are required (`400 BadRequestException`); the domain, the API and the stage must exist (`404 NotFoundException`); and an `apiMappingKey` another mapping of the domain already uses, the empty key included, is `409 ConflictException` (#566, #1423) |
| GetApiMappings | The domain's mappings in ascending mapping ID, in the `items` envelope; pages by `maxResults`/`nextToken`. `404 NotFoundException` for a domain that does not exist |
| GetApiMapping | `404 NotFoundException` for a mapping, or a domain, that does not exist |
| UpdateApiMapping | `PATCH`; changes `apiId`, `stage` and `apiMappingKey`, each `Required: No`, so an absent member is left as it was. Answers the mapping. Same refusals as `GetApiMapping`, and the mapping it leaves is held to `CreateApiMapping`'s checks: an empty `apiId` or `stage` is `400 BadRequestException`, an absent API or stage `404 NotFoundException`, and another mapping's key `409 ConflictException` |
| DeleteApiMapping | `204`, and the mapping leaves `GetApiMappings`. Same refusals as `GetApiMapping`, so deleting twice is refused the second time |
| TagResource | `POST /v2/tags/{resource-arn}` with `{"tags": {…}}`; **201** with no body. Merges onto an API's tags (see *An API's tags*). `404 NotFoundException` for an API that does not exist, `400 BadRequestException` for a body that does not decode or an ARN that names no API |
| GetTags | `GET /v2/tags/{resource-arn}`; answers `{"tags": {…}}`, an empty map for an untagged API. Same refusals as `TagResource` |
| UntagResource | `DELETE /v2/tags/{resource-arn}?tagKeys=…`; **204**. `tagKeys` is Required and may repeat; its absence is `400 BadRequestException`. Same refusals as `TagResource` |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ApiGatewayV2::Api | ApiId | |
| AWS::ApiGatewayV2::Route | RouteId | |
| AWS::ApiGatewayV2::Integration | IntegrationId | |
| AWS::ApiGatewayV2::Stage | StageName | |

## The v2 collections page

`GetApis`, `GetRoutes`, `GetIntegrations`, `GetStages`, `GetAuthorizers` and `GetApiMappings` each
publish `maxResults` and `nextToken`, and until [#1423](https://github.com/scttfrdmn/substrate/issues/1423)
none read either, so each answered its whole collection in one page. They now page. The pages publish
no default, maximum or minimum for `maxResults` and no order, so these readings are substrate's:

- A request with no `maxResults` gets the whole collection in one page with no `nextToken`, which is what
  every list answered before, so an existing fixture is unchanged.
- `maxResults` must be a positive integer, and a `nextToken` substrate could not have issued is refused
  rather than answered with page one. Both are `400 BadRequestException`, which every page publishes for
  *"One of the parameters in the request is invalid"*, and both are checked before any state is read.
- `nextToken` is the base64 offset the v1 collections use, and the last page carries none. Elements are in
  ascending ID (or stage name), so the offset is stable between pages.
- A record that cannot be read or decoded fails the listing with a 500, rather than being left out of a
  page that then reports fewer elements without saying so. Only an index entry whose record is gone is
  skipped. `GetApiMappings` always did this; the other five did from
  [#1538](https://github.com/scttfrdmn/substrate/issues/1538).

## An update changes only what it names

`UpdateRoute` and `UpdateStage` are the convergence operations a second deploy calls: a route is
retargeted at a replaced integration, and the `$default` stage's auto-deploy and access logging
are brought to the desired state. Every member of `UpdateRouteInput` and `UpdateStageInput` is
Required: No, so a member absent from the body leaves the stored value as it was. A member that is
present replaces it, even when it is empty or `false`; that is how a caller detaches an authorizer
(`"authorizerId": ""`) or turns auto-deploy off. Both answer the full updated `Route` or `Stage`,
the shape `GetRoute` and `GetStage` read back, so a caller can converge on what it reads
([#1279](https://github.com/scttfrdmn/substrate/issues/1279)).

`autoDeploy` is recorded intent: substrate creates no deployment when the API changes. Members the
published input declares and the records do not model are not read. For a route these are
`authorizationScopes`, `apiKeyRequired`, `requestParameters`, `requestModels`,
`modelSelectionExpression`, `operationName` and `routeResponseSelectionExpression`. For a stage they
are `clientCertificateId` and `routeSettings`. Neither update answers the published
`ConflictException`: no route-key or stage uniqueness is enforced.

## An API's tags

The tags-resource-arn page publishes `TagResource`, `GetTags` and `UntagResource` on
`/v2/tags/{resource-arn}`, and until [#1378](https://github.com/scttfrdmn/substrate/issues/1378)
none was routed: an HTTP or WebSocket API's tags could be set at `CreateApi` and never read back.
All three now route, and the Resource Groups Tagging API reaches the same tags, so a tag written
either way is visible both ways.

- **The ARN.** An API's ARN is `arn:aws:apigateway:{region}::/apis/{apiId}`. Its account segment is
  empty by format, so the API is the caller's; an ARN that does name an account, or another Region,
  reaches that scope's API or none, never the caller's. The rule is the one REST APIs follow
  ([#1307](https://github.com/scttfrdmn/substrate/issues/1307)).
- **What is taggable.** Only an API. AWS also tags stages, domain names and VPC links through these
  operations; substrate stores no tags on those records, so their ARNs answer
  `400 BadRequestException` rather than an empty set. That code is substrate's reading: the page
  publishes it for an invalid parameter, not a list of taggable types.
- **Tag history.** `GetResources` reports an API that is tagged or has been (#938). That history is a
  side-car key beside the API (`apiv2_tagged:`) rather than an `ever_tagged` member of its record,
  for the reason [Tagging a log group takes the unsuffixed ARN](/reference/logs#tagging-a-log-group-takes-the-unsuffixed-arn)
  gives. Every writer of an API's tags stamps it, and `DeleteApi` removes it with the API.
- **Not modeled.** The page publishes no per-resource tag limit, so none is enforced.

## Cost

API Gateway HTTP API calls: $1.00 per million calls.
