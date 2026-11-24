# execute-api (API Gateway data plane)

**Endpoint:** `{apiId}.execute-api.{region}.amazonaws.com/{stage}/{resourcePath}`
**Protocol:** whatever the deployed API accepts — there is no service model
**Routing:** any host containing `.execute-api.`; the first host label is the API ID, and is data rather than part of the service name

This is not a modelled AWS API. It is the runtime endpoint a browser or an HTTP
client calls, and substrate's job on it is to do what a deployed stage does: find
the `AWS_PROXY` integration for the requested method and path, build the proxy
event AWS would build, invoke the Lambda function through the registry, and turn
the function's proxy response back into an HTTP response. Nothing is stored and
no state is owned — the API, its routes and its integrations are read out of the
API Gateway and API Gateway v2 plugins' state, and the function is whatever the
Lambda plugin holds. An SDK-generated client never calls this endpoint, which is
why it has no operation list.

**The name a caller needs is `execute-api`.** The plugin registry keys every
plugin on its `Name()`, and this one returns `execute-api`; that is the value a
request's resolved service name must equal, and the value to use in a fault
injection rule, a cost key or an audit filter. The string `apigateway-proxy` that
appears beside the plugin in the registration table is a label used only to
interpolate into an initialization error message. It is not an alias, it never
reaches routing, and addressing it gets `ServiceNotAvailable` at 501.

## What a request does

| Step | Result |
|------|--------|
| Host header read for the API ID | Missing host is refused 400; a host without `.execute-api.` is refused 400 |
| First path segment taken as the stage | Never validated against a stage or a deployment |
| Remainder taken as the resource path | Compared to route keys and resource paths by exact string equality |
| v2 route lookup, then v1 resource lookup | The first `AWS_PROXY` integration found wins; v2 falls back to `$default` |
| Proxy event built | 2.0 format for an HTTP API, 1.0 format for a REST API, chosen by which plugin held the API |
| Lambda invoked | Through the registry, as `POST /2015-03-31/functions/{name}/invocations` |
| Response parsed | `statusCode`, `headers`, `body` and `isBase64Encoded` are read; anything else is relayed at 200 |

Nothing else in the data-plane surface is modelled. There are no authorizers,
no usage plans or API keys, no WAF, no request or response mapping templates, no
non-proxy integration types, no binary media-type handling beyond a single
base64 decode, no CORS preflight handling, no throttling and no gateway response
customisation. A request whose API cannot be found, or whose method and path
match no `AWS_PROXY` integration, is refused 502.

## The stage in the path is never checked

The stage is split off the path and used only to populate the event. No stage
record is read, no deployment is read, and no check ties the two together, so
`/prod/users`, `/dev/users` and `/typo/users` are one request. A template that
creates a resource, a method and an integration but never a `AWS::ApiGateway::Deployment`
or `AWS::ApiGateway::Stage` — a real and common CDK mistake — is indistinguishable
from a correct one when driven through this endpoint, which removes exactly the
signal a pre-AWS validation run is supposed to produce. What AWS returns for a
stage that does not exist is not stated in the gateway-response table and is
recorded here as unverified; the table does publish
`MISSING_AUTHENTICATION_TOKEN` at 403 for `the cases when the client attempts to
invoke an unsupported API method or resource`, which is the landing for an unknown
resource path
([#1214](https://github.com/scttfrdmn/substrate/issues/1214)).

## A path parameter never matches, so a proxy resource is unreachable

Resolution compares the request's resource path to the stored path with `!=` for
a REST API and `==` for an HTTP API route key. Neither compare understands a path
template, so a REST API whose only resource is `/{proxy+}` matches nothing, and
one with `/users/{id}` cannot serve `/users/42`. Since `/{proxy+}` forwarding to a
single handler is the shape most CDK and SAM applications emit, the common case is
that a correctly deployed API answers 502 for every request. `pathParameters` is
hard-coded to `nil` in the v1 event and absent from the v2 event for the same
reason: with no template there is nothing to extract. An HTTP API is partially
rescued by its `$default` route, which substrate does fall back to, so a
`$default`-only HTTP API works and a parameterised one does not
([#1214](https://github.com/scttfrdmn/substrate/issues/1214)).

## A failed Lambda is reported to the caller as a success

Only the invoke response's **body** is examined. The invoke status and headers are
discarded, including `X-Amz-Function-Error`, which substrate's own Lambda plugin
sets when a function errors and which a control-plane seed can force. The body is
then decoded leniently: a body that is not JSON is passed through verbatim at HTTP
200, and a body that is JSON but not a proxy response yields a zero `statusCode`
that is promoted to 200 with an empty body. The REST API developer guide states
the opposite — `If the function output is of a different format, API Gateway
returns a 502 Bad Gateway error response.` — so an unhandled exception payload,
which is neither a proxy shape nor an error to this code, reaches the caller as a
200 carrying `{"errorMessage": …}` or as a 200 carrying nothing at all. The path a
test seeds a Lambda failure in order to exercise is therefore the path that cannot
be observed
([#1214](https://github.com/scttfrdmn/substrate/issues/1214)).

## The v2 rawQueryString is sorted and percent-encoded

The HTTP API developer guide's 2.0 payload example shows `rawQueryString` as the
query string the client sent, `parameter1=value1&parameter1=value2&parameter2=value`,
a repeated key's values in the order sent. Substrate's parsed request keeps the
parameters but not the raw string, so the client's own key order cannot be
reproduced. The string is built instead with keys in **sorted order**, each repeated
key's values in the order sent, and every key and value **percent-encoded** (a space
as `+`): `?zeta=1&alpha=a%26b` arrives as `alpha=a%26b&zeta=1`. The same request
therefore always yields the same string, and the string always parses back to the
parameters it came from. Before
[#1201](https://github.com/scttfrdmn/substrate/issues/1201) it was concatenated in Go
map order, so it differed between two runs of one test, and values went in
unencoded, so a value holding `&` or `=` changed the string's shape. A bare key
(`?flag`) reaches the event as `flag=1`, which is how the parser records one.

## PayloadFormatVersion is recorded by the control plane and ignored by the proxy

The v2 integration record carries `PayloadFormatVersion`, and the proxy never
reads it: the event format is decided by which plugin's state held the API, 2.0
for an HTTP API and 1.0 for a REST API. The HTTP API reference states `The
supported values are 1.0 and 2.0` and documents two different event shapes, so an
HTTP API deliberately configured for `1.0` — which is what a consumer does when
migrating a REST API handler unchanged — receives a 2.0 event, and its handler
reads `event["httpMethod"]` and `event["path"]` as absent
([#1215](https://github.com/scttfrdmn/substrate/issues/1215)).

## Both proxy events are thinner than the published ones

The 1.0 event carries a `version` member, which the REST API proxy input
reference does not publish at all — payload-format versioning belongs to HTTP
APIs — so a handler that dispatches on `event.get("version")` sends a REST API
request down its HTTP API branch. It omits `multiValueHeaders` and
`multiValueQueryStringParameters`, sends `stageVariables` as an empty object where
the published example shows `null`, and reduces `requestContext` to `stage`,
`requestId`, `httpMethod`, `resourcePath` and `apiId` — five of the fifteen
members published, with `identity` and `authorizer` among the absent, so
`requestContext.identity.sourceIp` and `requestContext.authorizer.claims` cannot
be read. The 2.0 event omits `cookies`, `queryStringParameters`, `pathParameters`
and `stageVariables`, and its `requestContext` omits `time`, `timeEpoch`,
`domainName`, `domainPrefix`, `accountId` and the `http` members `protocol`,
`sourceIp` and `userAgent`. The omission that bites first is
`queryStringParameters`: substrate does parse the query string into
`rawQueryString`, so the information is present and simply not offered in the
member a 2.0 handler reads
([#1215](https://github.com/scttfrdmn/substrate/issues/1215)).
Whether a named stage contributes a segment to the 1.0 `path` and the 2.0
`rawPath`, as substrate assumes, is unverified: both published examples use the
`$default` stage, where the question does not arise, and the REST example shows
`path` equal to `resource`.

## What a refusal reports

A refusal is a JSON object with a single `message` member carrying substrate's own
diagnostic text, which is not the shape or the status API Gateway publishes for any
of these conditions.

| Condition | Body | Status |
|-----------|------|--------|
| Plugin initialized without a registry | `{"message": "proxy plugin not wired to registry"}` | 500 |
| No `Host` header | `{"message": "missing Host header"}` | 400 |
| Host without an `.execute-api.` label | `{"message": "unexpected host: …"}` | 400 |
| API ID not found in either plugin's state | `{"message": "no Lambda integration found: API … not found"}` | 502 |
| No `AWS_PROXY` integration for the method and path | `{"message": "no Lambda integration found: no AWS_PROXY integration for …"}` | 502 |
| Proxy event will not marshal | `{"message": "build proxy event: …"}` | 500 |
| Lambda invocation refused, or nil | `{"message": "lambda invoke: …"}` or `{"message": "nil lambda response"}` | 502 |
| Lambda answered a body that is not a proxy response | the body verbatim, or an empty body | 200 |

API Gateway publishes a gateway response type and a default status for each of
these situations, and none of them is a 502 carrying prose.
`MISSING_AUTHENTICATION_TOKEN` is 403 and its gloss covers `the cases when the
client attempts to invoke an unsupported API method or resource`, which is the
published landing for an unrecognised path — substrate answers 502.
`RESOURCE_NOT_FOUND` is 404, `INTEGRATION_FAILURE` and `INTEGRATION_TIMEOUT` are
504, `API_CONFIGURATION_ERROR` is 500, `BAD_REQUEST_BODY` and
`BAD_REQUEST_PARAMETERS` are 400, `UNAUTHORIZED` is 401, `ACCESS_DENIED`,
`EXPIRED_TOKEN`, `INVALID_API_KEY`, `INVALID_SIGNATURE` and `WAF_FILTERED` are
403, `REQUEST_TOO_LARGE` is 413, `UNSUPPORTED_MEDIA_TYPE` is 415, and
`THROTTLED` and `QUOTA_EXCEEDED` are 429; `DEFAULT_4XX` and `DEFAULT_5XX`
publish no default status of their own. Every one of those is currently without a
site in substrate. Which type covers a resource that exists but carries no
`AWS_PROXY` integration is not stated in the table and is recorded here as
unverified. The published answer for a malformed function output is not a gateway
response type at all but a plain `502 Bad Gateway`, which is the one status
substrate does emit — for the wrong conditions, and never for that one
([#1214](https://github.com/scttfrdmn/substrate/issues/1214)).

## Cost

A stage request is charged once an API has resolved it, at the first-tier rates the
API Gateway pricing page's examples use: $3.50 per million for a REST API
(`execute-api/RESTAPIRequest`, $0.0000035) and $1.00 per million for an HTTP API
(`execute-api/HTTPAPIRequest`, $0.000001). An HTTP API request is metered in 512 KB
increments, as the page's HTTP API example divides each upload to count requests, so
a 1 MB body is two requests. A request refused before an API resolves — a missing
host, an unknown API, no `AWS_PROXY` integration — is not charged. The two keys name
no operation, because this endpoint routes none; they are reached only by the proxy.

**A proxied Lambda invocation is charged too**, as `lambda/Invoke`, when the
registry served it (a refused invocation ran nothing; a function error is an
invocation that ran). Before
[#1202](https://github.com/scttfrdmn/substrate/issues/1202) neither charge existed:
the invocation goes through the plugin registry, which consults no cost controller,
so a run that drove a hundred thousand requests through a stage reported no API
Gateway spend and no Lambda spend. Both charges are folded into the cost of the
**one** event the stage request records — no child event is written for the
invocation, so the event log's shape is unchanged and a replay reads back the
recorded figure — which means a cost summary's `ByService` attributes the Lambda
share to `execute-api`. Each is still priced from its own key, so an override of
`lambda/Invoke` applies to a proxied invocation as it does to a direct one.
