# OpenSearch

**Endpoint:** `search-{domain}-{suffix}.{region}.es.amazonaws.com`, `vpc-{domain}-{suffix}.{region}.es.amazonaws.com`, `{id}.{region}.aoss.amazonaws.com`
**Protocol:** OpenSearch REST (paths and JSON bodies, not an AWS service model)
**Routing:** any host containing `.es.` or `.aoss.`, and the SigV4 signing names `es` and `aoss`, are mapped to this plugin. The domain or collection name in the host scopes the plugin's state (see below); it is not a routing key.

**Substrate routes the OpenSearch data plane only.** Indexing, searching, bulk
writes, scrolling and the handful of index operations below are emulated; the
`es`/`opensearch` **control plane is not**. There is no `CreateDomain`,
`DescribeDomain`, `UpdateDomainConfig`, `ListDomainNames` or `DeleteDomain`, and no
`aoss` collection operation. A control-plane call is refused in the AWS envelope
rather than misread as an index; see
[The control plane is refused, not routed](#the-control-plane-is-refused-not-routed).
A consumer must treat the domain as pre-existing and address the data plane
directly. The control plane itself is tracked in
[#1564](https://github.com/scttfrdmn/substrate/issues/1564).

## Supported operations

| Method and path | Notes |
|-----------------|-------|
| `PUT /{index}` | Stores a supplied `mappings` and `settings`; refuses an existing index, a body that is not JSON, and a setting named twice |
| `GET /{index}` | Reports the stored mappings and settings; see *What an index read reports* |
| `HEAD /{index}` | 200 or 404 and no body |
| `DELETE /{index}` | Deletes the index and every document in it |
| `GET /{index}/_mapping` | `{"{index}": {"mappings": {…}}}` |
| `PUT\|POST /{index}/_mapping` | Merges the body into the stored mapping |
| `POST /{index}/_refresh` | Reports a one-shard `_shards` object |
| `PUT\|POST /{index}/_doc/{id}` | 201 `created` or 200 `updated`; `?op_type=create` refuses an existing document at 409 |
| `PUT\|POST /{index}/_create/{id}` | 201 `created`; an existing document is refused at 409 |
| `POST /{index}/_doc`, `POST /{index}/_doc/` | ID minted from the request's ID mint |
| `GET /{index}/_doc/{id}` | `found: true` with the source, or `found: false` at 404 |
| `HEAD /{index}/_doc/{id}` | 200 or 404 and no body |
| `DELETE /{index}/_doc/{id}` | 200 `deleted`, or 404 `not_found` for an absent document |
| `POST\|PUT /_bulk`, `POST\|PUT /{index}/_bulk` | `index`, `create`, `update` and `delete`; `errors` reports whether any item failed |
| `{any method} /{index}/_search` | `query`, `from`, `size`, `sort`, `_source`, `track_scores`, `aggs`/`aggregations` and `scroll` are read |
| `GET\|POST /_search/scroll` | Body or `scroll_id` parameter; reports the query's total |
| `DELETE /_search/scroll` | Reports `succeeded` and the number of contexts freed |
| `GET /_cluster/health` | A fixed single-node green response |

Anything else is refused `route_not_found` at 404. That set includes the
index-less `GET\|POST /_search`, which is misread as an index named `_search` and
answers `index_not_found_exception`; the published path form
`GET\|POST /_search/scroll/{scroll_id}`; `DELETE /_search/scroll/_all`; `_update`,
`_update_by_query`, `_delete_by_query`, `_reindex`, `_count`, `_msearch`, `_alias`,
`_aliases`, `_settings`, `_cat/*`, `_nodes/*`, `_snapshot/*` and the point-in-time
API. A search addresses one index by name; a comma-separated list or a wildcard
pattern is read as a single index name. These are tracked in
[#1589](https://github.com/scttfrdmn/substrate/issues/1589). Substrate is not an OpenSearch engine and
does not aim to be one: what it models is the set of observations a consumer's
index-then-query code makes.

## What a state key is scoped by

Every key carries the request's account, its Region and a domain segment
([#1200](https://github.com/scttfrdmn/substrate/issues/1200)): an index is stored at
`index:{account}/{region}/{domain}/{name}`, a document at
`doc:{account}/{region}/{domain}/{index}/{id}`, and a scroll context at
`scroll:{account}/{region}/{domain}/{id}`. Two accounts, two Regions or two domains
in one run are therefore separate clusters, which is what a multi-account or
multi-Region topology expects.

The domain segment is read from the `Host` header, because that is the only place a
data-plane request names its domain. The developer guide documents two
domain-endpoint forms, `search-{domain-name}-{identifier}.{region}.es.amazonaws.com`
for a public domain and `vpc-{domain-name}-{identifier}.{region}.es.amazonaws.com`
for a VPC domain, and the domain name is what lies between the prefix and the last
hyphen, so a hyphenated name such as `my-app-logs` is read whole. A Serverless
collection endpoint `{id}.{region}.aoss.amazonaws.com` scopes by `aoss.{id}`. The
`DomainEndpoint` a deployed `AWS::OpenSearchService::Domain` resolves to is in the
first form, so it reaches that domain's documents.

**Every other host shares one domain, `_default`, per account and Region.** That
covers a base endpoint such as `localhost:4566` — which is what a client configured
with an endpoint URL sends, routed here by its `es` or `aoss` signing name — the
control-plane host `es.{region}.amazonaws.com`, and an `.es.` host whose first label
is in neither documented form. So one run is one cluster per domain endpoint, and
one more for everything addressed to a base endpoint.

State written before this change was keyed `index:{name}`, `doc:{index}/{id}`,
`doc_ids:{index}` and `scroll:{id}`, with documents stored as their raw bodies. None
of it is read; a persisted state store needs a reset after upgrading.

## What a write reports

Documents are stored with their metadata
([#1213](https://github.com/scttfrdmn/substrate/issues/1213)). Every write and every
delete answers the members the index-document and delete-document references
publish: `_index`, `_id`, `_version`, `result`, `_shards`, `_seq_no` and
`_primary_term`.

| Write | Status | `result` | `_version` |
|-------|--------|----------|------------|
| Index a new document | 201 | `created` | 1 |
| Index over an existing document | 200 | `updated` | the stored version plus one |
| Create (`_create`, `op_type=create`) a new document | 201 | `created` | 1 |
| Create over an existing document | 409, `version_conflict_engine_exception` | — | — |
| Delete an existing document | 200 | `deleted` | the stored version plus one |
| Delete an absent document | 404 | `not_found` | 1 |

The index-document reference publishes `created` and `updated` and a version
"Incremented each time the document is updated"; the delete-document reference
publishes `result` as "`deleted` if the document was successfully deleted or
`not_found` if the document did not exist". The statuses are the ones the Bulk API
reference's example items carry: 201 for `created`, 200 for `updated`, 404 for
`not_found`.

`_seq_no` numbers every write to an index, starting at 0; a delete of an absent
document consumes one too, as the Bulk API example's `not_found` item does.
`_primary_term` is always 1, because substrate models one node whose primary is
never reassigned. `_shards` is always `{"total": 1, "successful": 1, "failed": 0}`.
A document deleted and then indexed again starts over at `_version: 1`. Optimistic
concurrency (`if_seq_no`/`if_primary_term`) and external versioning are not read (#1589).

A document body must be a JSON object: an empty body is refused
`action_request_validation_exception` and any other non-object
`mapper_parsing_exception`, both at 400.

`GET /{index}/_doc/{id}` answers the same metadata with `found: true` and the
source. An absent document is a normal body at 404,
`{"_index": …, "_id": …, "found": false}` — not the error envelope — because the
get-document reference publishes `found` as "`true` if the document was found,
`false` otherwise" and a client switches on it. An absent *index* is the error
envelope, `index_not_found_exception` at 404. `_source`, `_source_includes` and
`_source_excludes` filter the source as they do on a search.

## What a bulk request reports

The Bulk API reference publishes four actions and substrate runs all four:

| Action | Outcome |
|--------|---------|
| `index` | Creates or replaces the document: item 201 `created` or 200 `updated` |
| `create` | Creates it; an existing document is a 409 `version_conflict_engine_exception` item |
| `update` | Merges the line's `doc` into the stored source: 200 `updated`, or 200 `noop` when the merge changes nothing. A missing document is created from `upsert`, or from `doc` under `doc_as_upsert` (201); otherwise it is the reference's 404 `document_missing_exception` item, reason `[{id}]: document missing` |
| `delete` | 200 `deleted`, or 404 `not_found` for an absent document; on an index that does not exist, a failed 404 `index_not_found_exception` item |

An update is a no-op when merging its `doc` leaves the stored source unchanged. The
update-document reference states `detect_noop` "Default is `true`" and that a
detected no-op "skips the operation and returns `"result": "noop"`", with
`_shards.total` 0. The item reports the stored `_version` and `_seq_no`, and nothing
is written. `"detect_noop": false` on the update line writes regardless.

Every action but `delete` creates a missing index, and a `delete` on a missing index
fails its item rather than creating one. Neither rule is on the Bulk API page, which
says nothing about a missing index; both are the engine's, from `TransportBulkAction`
in the OpenSearch source. It collects the indices to create from every request
except a delete ("delete requests should not attempt to create the index"), and
fails a delete whose index is absent with `IndexNotFoundException`. So an `update`
of a missing document in a missing index creates the index and then fails
`document_missing_exception`.

Each successful item carries the same members as the single-document write plus
`status`. A failed item carries `_index`, `_id`, `status` and the published `error`
object — `type`, `reason`, `index`, `shard` and `index_uuid`. `errors` is true when
any item carries an `error`; a `not_found` delete does not, which is how the
reference's own example reports one beside `errors: false`. An `update` whose line
carries a `script` is refused per item at 400 `illegal_argument_exception`:
running a script is the workload, not an API observation, and is out of scope.

The request is validated whole before any action runs. An empty body, a line that
is not an object naming exactly one action, an action other than the four, an
action with no source line, or an action with no index (and none in the path)
refuses the whole request at 400 — `action_request_validation_exception` or
`illegal_argument_exception`, with substrate's own reasons — as does an `update` or
`delete` with no `_id`.

## What an index read reports

`GET /{index}` answers `aliases`, `mappings` and `settings` keyed by the index name,
the get-index reference's shape. `settings.index` holds the settings the create
supplied — `{"number_of_shards": 2}`, `{"index": {"number_of_shards": 2}}` and
`{"index.number_of_shards": "2"}` all read back as `number_of_shards: "2"` — with
every value a string, as the reference prints them, plus `creation_date` (epoch
milliseconds on the simulated clock), `uuid` and `provided_name`. Because those forms
converge, one body can name a setting twice, as `{"number_of_shards": 1, "index":
{"number_of_shards": 2}}` does. No page says which wins, and choosing by map order
would give a different index on different runs. So it is refused
`illegal_argument_exception` at 400, naming the setting, with substrate's own reason.
`max_result_window` is read back as stored and bounds the index's searches. A create
that supplied no shard or replica count reports `number_of_shards: "1"` and
`number_of_replicas: "0"`: substrate models one node, on which a replica could never
be allocated, and the cluster health it reports is green. The engine's own default
of one replica is therefore not what substrate reports.

`PUT /{index}/_mapping` merges its body into the stored mapping — a field it names
replaces or extends the stored one, and a field it does not name is kept, the
additive behavior the put-mapping reference's follow-up reads show — and
`GET /{index}/_mapping` answers `{"{index}": {"mappings": {…}}}`. The reference
states an existing field's type cannot be changed once it holds data; substrate does
not refuse that, since it indexes nothing by type. Both answer
`index_not_found_exception` for a missing index.

`HEAD /{index}` answers 200 or 404 and no body: the index-exists reference states
the operation "returns only one of two possible response codes".

## What a search reports, and in what order

A search is a filter: substrate evaluates the query against each document and does
not rank. Every hit's `_score` is the constant `1.0`, and `max_score` is `1.0` when
the page holds a hit and `null` when it holds none. So **the order a consumer sees is
the only orderable signal there is**, and it is defined
([#1201](https://github.com/scttfrdmn/substrate/issues/1201)):

- With a `sort` — in the body, as a field name, `{"field": "desc"}`,
  `{"field": {"order": "desc", "missing": "_first"}}` or an array of those, or as the
  `sort=field:desc,…` query parameter — documents are ordered by each key in turn. A
  field name alone sorts ascending, except `_score`, which sorts descending; `_doc` is
  insertion order and `_id` the document ID. A missing value sorts last unless the key
  asks for `_first`, and an array field sorts by its minimum ascending and its maximum
  descending. Each hit then carries a `sort` array of its values, and — as the search
  reference states for a search not sorted by `_score` — `_score` and `max_score` are
  `null` unless `track_scores` is true. A missing value's `sort` entry is `null`.
- Without one, documents are in insertion order: ascending `_seq_no` of each
  document's last write, the engine's own tie-break for equal scores on one shard.

Insertion order, rather than document ID, is the default because a generated
document ID is minted from the request ID, which a live request derives from the
wall clock: ordering by ID gave a different order on every fresh run. Insertion
order also breaks any tie a `sort` leaves. A field addressed as `name.keyword`
reads `name` when the source has no member of that name, the keyword sub-field
dynamic mapping gives a string field. The same request against the same state
answers the same bytes.

The query matcher reads `match_all`, `match_none`, `term`, `terms`, `match`,
`match_phrase` (as a case-insensitive substring), `range`, `exists` and `bool`
(`must`, `filter`, `should`, `must_not` and `minimum_should_match`).
Several top-level clauses in one query object are evaluated in sorted key order and
must all match, so `{"match_all": {}, "term": {…}}` is the term filter, on every
run. The Query DSL documents `bool` as the way to combine clauses and publishes no
error for a multi-clause object, so substrate does not refuse one. `should` is
optional beside a `must` or `filter` and requires one match otherwise, the Query
DSL's default. A term or match against an array field matches when any element
does. A clause type outside this list matches every document.

`minimum_should_match` takes every form the minimum_should_match reference
publishes. A non-negative integer is that many `should` clauses, and a negative one
is the total less that many. A percentage is that share of the total, rounded down,
and a negative percentage is the share that may be missed, rounded down. `n<value`
applies the value when there are more than `n` clauses and requires all of them
otherwise; several space-separated conditions apply the one with the largest `n` the
total exceeds. A count from a percentage below 1 becomes 1 and one above the total
becomes the total, as the reference states, and an integer is held to the same
range. Any other value is refused `illegal_argument_exception` at 400, with
substrate's own reason, rather than ignored.

`match` reads a string, or an object with `query` and `zero_terms_query`. Substrate
does not analyze text, so a match is a case-insensitive substring test, but two
published behaviors still hold. A field the document does not have matches nothing.
A query with no terms — here, an empty or all-whitespace one — matches no document,
because the match query reference gives `zero_terms_query` "Default is `none`" and
says `none` "returns no documents"; `"zero_terms_query": "all"` matches every
document.

`_source` is applied: `true`, `false`, a field pattern, an array of patterns, or
`{"includes": […], "excludes": […]}` in the body, or the `_source`,
`_source_includes` and `_source_excludes` query parameters; a pattern may use `*`,
and an exclude wins over an include. A path reaches through arrays of objects, so
`items.sku` keeps the `sku` of every object in an `items` array, and an object left
with no member is dropped from it. `false` omits `_source` from every hit.

`size` and `from` are read from the body or the query string; `size: 0` returns no
hits, which is how an aggregations-only search is written. `from + size` may not
exceed the index's `index.max_result_window`. The pagination reference states
OpenSearch "rejects requests in which `from` + `size` exceeds" that setting, "Default
is `10000`". Substrate reads the setting from the index's stored settings. A larger
window is refused `illegal_argument_exception` at 400, before any document is read.
The page publishes no error body, so the reasons are the engine's own, from
`DefaultSearchContext` in the OpenSearch source: "Result window is too large, from +
size must be less than or equal to: [10000] but was [10001]. …" for a search, and
"Batch size is too large, …" for a scroll. `hits.total` is the number of documents
the query matched, `{"value": N, "relation": "eq"}`.

## A scroll continuation reports the query's total

A scroll context stores the hits beyond the first page in the order the search
returned them, with their sort values and the search's `_source` filter, and each
continuation returns the next `size` of them. `hits.total` on every page is the
total the initial search matched, which is what the scroll reference's
end-of-results example shows beside an empty page. A document deleted after the
search is skipped rather than returned. `DELETE /_search/scroll` reports
`num_freed` as the number of contexts that existed and were freed.

## The control plane is refused, not routed

Both control-plane APIs put their API version date first in every request URI —
`/2021-01-01/…` for `opensearch` and `/2015-01-01/…` for the legacy `es` API — and
every one of the 155 operations in botocore's two models carries one of those
prefixes. A request whose first path segment is either date is refused before it
reaches the data plane
([#1212](https://github.com/scttfrdmn/substrate/issues/1212)):

```
HTTP/1.1 404
X-Amzn-ErrorType: UnknownOperationException

{"message": "substrate does not emulate the OpenSearch Service control plane: CreateDomain (POST /2021-01-01/opensearch/domain) is not supported; see https://github.com/scttfrdmn/substrate/issues/1564"}
```

The code and status are the ones substrate answers for every unrouted operation of a
REST-JSON service. The message is substrate's own; it names the operation when the
method and path match a published URI template, and the method and path when they
do not. Before #1212 the path was split as `{index}/{rest…}` and answered the
engine's `route_not_found` in the engine's envelope, a body with no AWS code an SDK
could model. The cost is that an index literally named `2021-01-01` or `2015-01-01`
cannot be addressed.

## What a refusal reports

Data-plane refusals are the engine's own JSON envelope, and not an AWS
`Code`/`Message` pair, because the data plane is the domain's REST API rather than
an AWS control-plane operation:

```
{"error": {"root_cause": [{"type": …, "reason": …}], "type": …, "reason": …}, "status": N}
```

`root_cause` is the shape OpenSearch's documented error responses print; substrate
has one cause per error, so it holds the error itself. The `status` member repeats
the HTTP status.

| Condition | `error.type` | Status |
|-----------|--------------|--------|
| Path with no index segment | index_not_specified | 400 |
| `PUT /{index}` where the index exists | resource_already_exists_exception | 400 |
| Any operation on an index that does not exist (`HEAD` answers 404 with no body) | index_not_found_exception | 404 |
| A create-only write of a document that exists | version_conflict_engine_exception | 409 |
| A document body that is empty | action_request_validation_exception | 400 |
| A document body that is not a JSON object | mapper_parsing_exception | 400 |
| Create-index, mapping, search, scroll or clear-scroll body that is not valid JSON | json_parse_exception | 400 |
| A malformed bulk request (see above) | action_request_validation_exception or illegal_argument_exception | 400 |
| A `sort` order other than `asc`/`desc`, a `_source` of the wrong type, a negative or non-integer `size`/`from`, `from + size` over `index.max_result_window`, or an unreadable `minimum_should_match` | illegal_argument_exception | 400 |
| `PUT /{index}` naming one setting twice (`number_of_shards` beside `index.number_of_shards`) | illegal_argument_exception | 400 |
| Scroll with neither a body `scroll_id` nor a `scroll_id` parameter | illegal_argument_exception | 400 |
| Scroll ID with no stored context | search_context_missing_exception | 404 |
| Stored scroll context that will not decode | internal_error | 500 |
| Any data-plane path the dispatcher does not recognise | route_not_found | 404 |

None of these type strings comes from an AWS API model, because there is no AWS
API model for the data plane; they follow the engine's own lower-cased exception
names, and the reasons are substrate's own except `document_missing_exception`'s,
which is the Bulk API reference's, and the two result-window reasons, which are the
engine's source text. Two types are substrate's own coinages with no
counterpart in the engine — `index_not_specified` and `route_not_found` — and are
named as such so that a reader does not mistake them for published values. The
reference publishes no error body for an expired scroll ID, so substrate's answer
there has nothing to be measured against.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::OpenSearchService::Domain | domain name | Deployed as a stub, because no control plane is routed (#1564), so there is no `CreateDomain` to call. No property is read but `DomainName`. `Fn::GetAtt Arn` and `DomainArn` resolve to the domain ARN, and `Id` to `{account}/{name}`. `DomainEndpoint` resolves to `search-{name}-{suffix}.{region}.es.amazonaws.com`, a host the data plane answers and scopes to that domain, so two domains in one template are two clusters. `DomainEndpointV2` and the two nested attributes are refused, failing the resource that reads them |
| AWS::Elasticsearch::Domain | domain name | Deployed the same way, rather than declined, because AWS still documents the legacy type. Its three attributes, `Arn`, `DomainArn` and `DomainEndpoint`, resolve as above |

The endpoint's 12-character suffix is derived from the account, Region, stack and logical
ID, so an `UpdateStack` keeps the endpoint a consumer already wired up. AWS's example
suffix is longer, and the width is substrate's reading
([#1203](https://github.com/scttfrdmn/substrate/issues/1203)). The data plane does not
check that a domain was deployed: a request to any domain endpoint is served, and
indexing into a domain that does not exist succeeds (#1564).

## Cost

OpenSearch is priced per instance-hour and per GB of storage, not per request, so
substrate attributes a nominal $0.0000001 to every `PUT` and every `POST` — a
document write, a bulk request, an index create, a mapping update, and a search sent
as `POST` — and nothing to `GET`, `HEAD` or `DELETE`. The keys are
`opensearch/PUT` and `opensearch/POST` because the data plane dispatches on the HTTP
method and the event records the verb as the operation. OpenSearch is deliberately
absent from the operation-name resolver table — AWS's IAM actions for the data plane
are the per-verb `es:ESHttp*` family — so the verb is the name the audit log, fault
rules and cost table see. The `opensearch/IndexDocument` and `opensearch/Bulk` keys
these replace could never match a request, so before #1202 an OpenSearch workload
reported no cost
([#1202](https://github.com/scttfrdmn/substrate/issues/1202)).
