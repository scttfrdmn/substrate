# Cloud Map

**Endpoint:** `servicediscovery.{region}.amazonaws.com`, and `data-servicediscovery.{region}.amazonaws.com` for `DiscoverInstances`
**Protocol:** JSON 1.1 (`X-Amz-Target: Route53AutoNaming_v20170314.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateHttpNamespace | Asynchronous (see below). A name already in use in the account and Region, by a namespace of any type, is `NamespaceAlreadyExists`/400, except a retry with the same `CreatorRequestId`, which answers the original `OperationId`. The uniqueness is substrate's reading: every namespace's `HttpName` is its name, and `DiscoverInstances` finds a namespace by that name alone |
| CreatePrivateDnsNamespace | As above; `Vpc` is required. Creates a private Route 53 hosted zone of the same name |
| CreatePublicDnsNamespace | As above; creates a public hosted zone |
| GetNamespace | `Id` or ARN. `Properties.DnsProperties.HostedZoneId` for a DNS namespace, `SOA` when one was set, and `HttpProperties.HttpName` for every namespace |
| ListNamespaces | `Filters` on `TYPE`, `NAME`, `HTTP_NAME` (`EQ`, `BEGINS_WITH`, and `IN` for `TYPE`) and `RESOURCE_OWNER`. Name order; pages on `NextToken`/`MaxResults` (1–100, default 100) |
| DeleteNamespace | Asynchronous. A namespace holding services is `ResourceInUse`/409. Deletes the hosted zone |
| UpdateHttpNamespace | Asynchronous; `Description` |
| UpdatePrivateDnsNamespace | Asynchronous; `Description` and the SOA `TTL` |
| UpdatePublicDnsNamespace | Asynchronous; `Description` and the SOA `TTL` |
| CreateService | Synchronous. `NamespaceId` (or the deprecated `DnsConfig.NamespaceId`), `DnsConfig`, `HealthCheckConfig` or `HealthCheckCustomConfig`, `Type`, `Tags`. See below for the checks |
| GetService | `Id` or ARN; `InstanceCount` counts registered instances |
| ListServices | `Filters` on `NAMESPACE_ID` (`EQ`, `IN`) and `RESOURCE_OWNER`. Name order; pages as above |
| UpdateService | Asynchronous. `Description`, `DnsConfig.DnsRecords` and `HealthCheckConfig`; an absent `HealthCheckConfig` is deleted, as the page states |
| DeleteService | A service with registered instances is `ResourceInUse`/409 |
| RegisterInstance | Asynchronous. Creates or updates the instance; see below for the attribute checks |
| DeregisterInstance | Asynchronous |
| GetInstance | |
| ListInstances | Instance ID order; pages as above |
| DiscoverInstances | On the data host. See below |
| GetInstancesHealthStatus | `Instances` (all when absent); an unknown one is `InstanceNotFound`/404 |
| UpdateInstanceCustomHealthStatus | Only for a service with `HealthCheckCustomConfig`, else `CustomHealthNotFound`/404. Applies at once |
| GetOperation | An observation of the operation's status (see below) |
| ListOperations | `Filters` on `NAMESPACE_ID`, `SERVICE_ID`, `STATUS`, `TYPE` and `UPDATE_DATE` (`BETWEEN` two epoch-second values). Creation order; reports the status a `GetOperation` would, without spending an observation |
| TagResource | Namespaces and services. At most 50 tags (`TooManyTagsException`); an `aws:` key is `InvalidInput` |
| UntagResource | |
| ListTagsForResource | `Tags` is always present, in key order |

Every refusal's code and HTTP status is the model's: `InvalidInput` 400; `NamespaceNotFound`,
`ServiceNotFound`, `InstanceNotFound`, `OperationNotFound`, `CustomHealthNotFound` and
`ResourceNotFoundException` 404; `ResourceInUse` 409. The messages are substrate's own.

## Operations are asynchronous, and their outcome is seedable

Creating, updating and deleting a namespace, updating a service, and registering and deregistering
an instance each answer an `OperationId`. `GetOperation` reports `Type`, `Status`, `Targets`
(`NAMESPACE`, `SERVICE`, `INSTANCE`), `CreateDate` and `UpdateDate`, and a FAIL's `ErrorCode` and
`ErrorMessage`. The Terraform AWS provider polls it until `SUCCESS` before reading the resource.

An operation's settled status is decided when it starts, and its effect is applied in that request
or not at all: an operation that will succeed has already changed the namespace, service or
instance when the request answers, and one that will fail changes nothing. A caller that polls
`GetOperation` to `SUCCESS` and then reads always sees the effect; one that polls to `FAIL` never
does. What this does not reproduce is AWS's latency: a read made before the operation reports
`SUCCESS` already sees the effect.

Unseeded, an operation reports its settled status, `SUCCESS`, from its first observation.
`POST /v1/servicediscovery/operation-status` seeds it, keyed by `operationId` or `"*"`:

```json
{"operationId": "*", "submittedObservations": 2, "transientStatus": "PENDING",
 "status": "FAIL", "errorCode": "CANNOT_CREATE_HOSTED_ZONE", "errorMessage": "..."}
```

- `submittedObservations` is how many `GetOperation` calls report `transientStatus` (`SUBMITTED` by
  default, or `PENDING`) before the settled status. It is counted in observations, per operation, as
  every [seeded progression](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded) is.
- `status` (`SUCCESS` by default, or `FAIL`), `errorCode` and `errorMessage` decide the outcome of
  every operation that **starts** while the seed is in force. An operation already started keeps its
  outcome, because its effect has already been applied or withheld; a seed naming it by ID governs
  only its countdown. A `FAIL` seed must name an `errorCode`, so a failed operation always says what
  failed; `errorMessage` may be empty.
- `DELETE /v1/servicediscovery/operation-status` clears every seed, or one with `?operationId=…`.

The seed is a control-plane write, recorded and replayed in position. A DNS namespace whose hosted
zone Route 53 refuses fails its create with `CANNOT_CREATE_HOSTED_ZONE`, the code the Terraform
provider quotes from such a failure; a zone Route 53 refuses to delete fails the namespace's delete
with Route 53's own code.

## A DNS namespace creates its Route 53 hosted zone

`CreatePrivateDnsNamespace` and `CreatePublicDnsNamespace` create a hosted zone of the namespace's
name through the Route 53 plugin, private for a private namespace, with the namespace's `Vpc` in the
request. `ListHostedZones` and `GetHostedZone` report it, and `GetNamespace` reports its ID as
`Properties.DnsProperties.HostedZoneId`. Deleting the namespace deletes the zone. The Route 53 plugin
does not record a private zone's VPC association
([#1734](https://github.com/scttfrdmn/substrate/issues/1734)).

`RegisterInstance` does not create the records a DNS service's instance gets in the zone, and
`DeregisterInstance` deletes none; the instance and its attributes are recorded
([#1732](https://github.com/scttfrdmn/substrate/issues/1732)).

## Services and instances

`CreateService` checks what the API reference publishes: a service in an HTTP namespace, or with
`Type` `HTTP`, takes no `DnsConfig`, and one in a DNS namespace needs it (its `Type` is then
`DNS_HTTP`); the record types are `A`, `AAAA`, `A` and `AAAA`, `SRV` or `CNAME`; a `CNAME` needs the
`WEIGHTED` routing policy and no `HealthCheckConfig`; at most one of the two health check
configurations. `RoutingPolicy` defaults to `MULTIVALUE`. A name the namespace already holds is
`ServiceAlreadyExists`/400, compared without case in a DNS namespace.

`RegisterInstance` checks the attributes against its `Attributes` documentation: at most 30 custom
attributes, keys up to 255 and values up to 1,024 characters and 5,000 in total;
`AWS_INSTANCE_IPV4` for an `A` record, `AWS_INSTANCE_IPV6` for `AAAA`, `AWS_INSTANCE_CNAME` for
`CNAME`, `AWS_INSTANCE_PORT` and an address for `SRV`, `AWS_INSTANCE_PORT` for a `HealthCheckConfig`
service; `AWS_INIT_HEALTH_STATUS` `HEALTHY` or `UNHEALTHY`; `AWS_EC2_INSTANCE_ID` in HTTP namespaces
only. `AWS_INSTANCE_IPV4` is not filled in from an `AWS_EC2_INSTANCE_ID`.

An instance's health is its custom health check's status for a `HealthCheckCustomConfig` service
(initially `AWS_INIT_HEALTH_STATUS`, else `HEALTHY`, as the page states), `HEALTHY` for a
`HealthCheckConfig` service, because substrate runs no Route 53 health check, and `UNKNOWN` for a
service with neither.

## DiscoverInstances

`DiscoverInstances` finds the namespace by its `HttpName` and the service by name, keeps the
instances carrying every `QueryParameters` pair, narrows them to those also matching
`OptionalParameters` when any do, and applies `HealthStatus` only to a service with a health check.
An absent `HealthStatus` is `HEALTHY`: "When you configure a health check, AWS Cloud Map returns
healthy resources by default" (the developer guide's health check page). `HEALTHY_OR_ELSE_ALL`
returns every instance when none is healthy. `MaxResults` (1–1000, default 100) caps the list.
AWS randomizes the order; substrate answers instance ID order, so a test reads the same list on
every run. `InstancesRevision` changes on every register and deregister, and not on a health change.

## A namespace or service is reachable through the tagging API

The Resource Groups Tagging API's `GetResources`, `TagResources` and `UntagResources` reach
namespaces and services, under the lock the plugin's own tag operations hold. A resource whose tags
were all removed is still reported, as a previously tagged resource. An ARN naming another account's or Region's namespace or service is reported failed and changes nothing.

## The CBOR protocol is not served

The Cloud Map model lists `smithy-rpc-v2-cbor` ahead of `json`. botocore (boto3, the AWS CLI) sends
JSON, and so does the Terraform AWS provider through aws-sdk-go-v2 servicediscovery v1.49, but
v1.50 and later prefer CBOR, which this plugin does not answer yet
([#1733](https://github.com/scttfrdmn/substrate/issues/1733)).

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ServiceDiscovery::HttpNamespace | the namespace ID | Waits on the operation; a FAIL fails the resource. `Fn::GetAtt Id`, `Arn`. `Description` updates in place; `Name` replaces it |
| AWS::ServiceDiscovery::PrivateDnsNamespace | the namespace ID | As above, with `Fn::GetAtt HostedZoneId`. `Description` and `Properties` update in place; `Name` and `Vpc` replace it |
| AWS::ServiceDiscovery::PublicDnsNamespace | the namespace ID | As for the private namespace, without `Vpc` |
| AWS::ServiceDiscovery::Service | the service ID | `Fn::GetAtt Id`, `Arn`, `Name`. `Description`, `DnsConfig` and `HealthCheckConfig` update in place through `UpdateService`; `Name`, `NamespaceId`, `Type` and `HealthCheckCustomConfig` replace it. `ServiceAttributes` fails the resource, because substrate does not route `UpdateServiceAttributes` |
| AWS::ServiceDiscovery::Instance | the `InstanceId` (the logical ID when absent) | Registers and waits on the operation; changed `InstanceAttributes` re-register it in place, and a new `ServiceId` or `InstanceId` replaces it |

A namespace's or instance's delete waits on its operation, so a `FAIL` (seeded, or a hosted zone
Route 53 refuses to delete) fails the resource rather than reading as deleted. A namespace create's
`CreatorRequestId` is derived from the stack, the logical ID, the type, the `Name` and the `Vpc`, so
a retry is idempotent and a replacement is never answered with the namespace it replaces. A
replacement that keeps its `Name` — a `PrivateDnsNamespace` moved to another `Vpc` — is therefore
refused as `NamespaceAlreadyExists` and the update rolls back, as CloudFormation's does: "If you
specify a name, you cannot perform updates that require replacement of this resource." A new `Name`
replaces it. An instance's physical ID is its `InstanceId`; a `ServiceId` change replaces it in the
new service and deregisters it from the old one.
