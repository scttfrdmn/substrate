# Price List Query API

**Endpoints:** `api.pricing.us-east-1.amazonaws.com`,
`api.pricing.ap-south-1.amazonaws.com`, `api.pricing.eu-central-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSPriceListService.{Op}`)

This is the *server* side of pricing — for code that queries AWS rates at
runtime. It is the inverse of Substrate's own cost-tracking pricing provider,
which consumes the public offer index to cost simulated usage (the
`/v1/pricing/refresh`, `/v1/pricing/lookup`, `/v1/pricing/discounts` and
`/v1/pricing/credits` control endpoints, and the `substrate pricing` command).

The offer corpus is 39 SKUs copied verbatim from the live offer files: seven
Amazon S3 SKUs from `AmazonS3/current/us-east-1/index.json` (version
`20260728131000`) and 32 Amazon EC2 SKUs from
`AmazonEC2/current/{us-east-1,us-west-2,eu-west-1}/index.json` (version
`20260910195514`). It is small on purpose: each SKU exists to reproduce a
response shape that callers get wrong, so a consumer's parser is tested against
real awkwardness rather than a tidied-up fixture.

Each document reports **its own service's** offer-file revision in `version` and
`publicationDate`, so an EC2 document and an S3 document from one Substrate build
disagree on both — as the real API's do, because the two services publish on
their own schedules.

## Supported operations

| Operation | Notes |
|-----------|-------|
| GetProducts | `ServiceCode` required; `Filters` 0–50; `MaxResults` 1–100 |
| DescribeServices | All fields optional; `MaxResults` 1–100 |
| GetAttributeValues | `AttributeName` **and** `ServiceCode` required; `MaxResults` 1–**10000** |

`FormatVersion` accepts only `aws_v1`, the sole documented value. Pagination
uses an opaque `NextToken`; a token that does not decode, or that points past the
end of the result set, is an `InvalidNextTokenException`.

`Filter.Type` supports the full documented enum — `TERM_MATCH`, `EQUALS`,
`CONTAINS`, `ANY_OF`, `NONE_OF` — not just `TERM_MATCH`. `ANY_OF` and `NONE_OF`
take a comma-separated `Value`. Filters are conjunctive, and a filter naming a
field a product does not carry never matches it, including `NONE_OF`.

## Response shapes worth knowing about

These are the traps the corpus deliberately preserves. Each is verified against
the live offer file.

- **`PriceList` elements are JSON documents encoded as strings**, not objects.
  Decoding requires a second unmarshal per element.
- **`pricePerUnit` values are strings** with trailing zeros (`"0.0230000000"`),
  never numbers. So are `beginRange` and `endRange`.
- **`productFamily` is absent from most products** — 315 of the 381 in the real
  S3 offer file omit it. A filter on `productFamily` therefore misses the
  majority of SKUs. `usagetype` is the attribute that is reliably present and
  1:1 with a SKU **in the S3 offer file**; it is neither in EC2's, where four of
  the corpus SKUs share `BoxUsage:m5.xlarge`. Keying on `usagetype` relies on an
  S3 accident rather than a Price List rule.
- **`TimedStorage-ByteHrs` carries three `priceDimensions`**, the last with
  `"endRange": "Inf"`. Reading only the first reports the first-50 TB rate as if
  it were the only rate.
- **`Requests-Tier1` is `"0.0000050000"` per request**, and its `unit` is
  `Requests` — that is $0.005 per 1,000. Dividing by 1,000 again is a 1,000×
  error.
- **Filtering `productFamily=Storage` with `volumeType="Glacier Deep Archive"`
  returns only `TimedStorage-GDA-Staging` at $0.021/GB-Mo** — the staging rate,
  21× the $0.00099 archive rate. No `TimedStorage-GDA-ByteHrs` SKU exists in the
  S3 offer file at all, so that filter cannot return the rate a caller expects;
  the nearest $0.00099 SKU is Intelligent-Tiering's
  `TimedStorage-INT-DAA-ByteHrs`.

An unknown `ServiceCode` is a `NotFoundException` rather than an empty
`PriceList`. Substrate's corpus is far smaller than AWS's catalog, and a loud
error is better than an empty result that reads as "AWS has no such price".

## The AmazonEC2 corpus

Nine instance types (`t3.micro`, `m5.xlarge`, `g4dn.xlarge`, `g5.2xlarge`,
`g6.xlarge`, `inf2.xlarge`, `p4d.24xlarge`, `p5.48xlarge`, `trn1.32xlarge`) in
three regions, plus seven `m5.xlarge` variants that differ from the nominal row in
exactly one attribute, plus the free-tier pseudo-product. Every rate, SKU, rate
code, description and attribute value is the offer file's own; none is derived
from another rate, because a plausible-looking rate is worse than no rate — a
consumer computing a cost from it would be wrong with no way to notice.

The traps, each measured in the live files rather than reasoned about:

- **The documented seven-filter recipe does not isolate one rate.** Filtering
  `regionCode`, `instanceType`, `operatingSystem`, `tenancy`, `preInstalledSw`,
  `capacitystatus` and `marketoption` returns **three** SKUs for a Windows
  `m5.xlarge` in `us-east-1`. All three share one `usagetype` and differ only in
  `licenseModel` and `operation`: $0.376 with a license, $0.192 without one and
  $0.192 BYOL. A caller that takes `PriceList[0]` picks one of the three
  arbitrarily. `licenseModel` is the eighth discriminator.
- **`marketoption` matters at a single instance type.** `p5.48xlarge` publishes an
  on-demand rate of $55.04 and a Capacity Block rate of **$0.00**. Omitting
  `marketoption` can report a free p5.
- **`capacitystatus` and `tenancy` change the `usagetype` token, not only the
  price**: `UnusedBox:`, `DedicatedUsage:`. A caller matching on the prefix
  `BoxUsage:` silently misses both.
- **The `usagetype` Region prefix is not derivable from the Region code.**
  `us-east-1` has none, `us-west-2` uses `USW2-`, and `eu-west-1` uses the legacy
  `EU-`, not `EUW1-`.
- **Two Regions can agree on nine rates and disagree on the tenth.** `us-west-2`
  is byte-identical to `us-east-1` for every type here except `p4d.24xlarge`,
  which is `21.9576420000` in one and `21.9576400000` in the other.
- **`eu-west-1` is a per-*family* premium, not one Region multiplier**:
  `p4d` ≈1.080×, `inf2` exactly 1.250×, `m5` ≈1.115×, `t3` ≈1.096×. Deriving one
  Region's price from another's would be wrong by up to 15%.
- **A Region can publish no rate at all.** `eu-west-1` publishes no `g6`, `p5` or
  `trn1` compute product, so an empty `PriceList` there is a real observation
  about the Region.
- **The free-tier pseudo-product falsifies five invariants.** It carries no
  `instanceType`, its `endRange` is `"750"` rather than `"Inf"`, its
  `offerTermCode` is `A429C66SYZ` rather than the global on-demand code, its
  `termAttributes` is non-empty (`"Restriction": "Limited SKU Usage"`) and its
  `appliesTo` lists 170 SKUs where every other on-demand dimension's is empty. Its
  `location` is `"Any"` and its `regionCode` is the **empty string**, so a
  `regionCode` filter never selects it.
- **`Compute Instance` excludes bare metal.** Bare-metal instances are a separate
  `productFamily`, `Compute Instance (bare metal)`, so a `productFamily=Compute
  Instance` filter does not see a `.metal` type.

Two things the corpus is deliberately not:

- **It is not EC2's catalog.** `GetProducts` serves this corpus; EC2's
  `DescribeInstanceTypes` serves its own instance-type data (#896). A type present
  in one is not thereby present in the other, and the two are not generated from a
  shared source — so a rate here is not a claim that the type can be launched.
- **It is not exhaustive, and an absence is not a statement.** `us-east-1` alone
  publishes 107,022 compute-instance products. An instance type absent from the
  corpus returns an empty `PriceList`, which reads the same as a Region that
  genuinely does not publish it. Where the absence *is* the observation — the three
  `eu-west-1` types above — it is because the real file omits them too. `trn2`
  appears nowhere at any rate for the same reason: a real query returns nothing for
  it in all three Regions.

## Endpoint regions — a deliberate divergence

AWS hosts the Price List Query API in exactly three regions: `us-east-1`,
`ap-south-1` and `eu-central-1`. There is no `api.pricing.eu-west-1.amazonaws.com`
to resolve.

Substrate serves every region from one endpoint, so it cannot reproduce a name
that fails to resolve. Instead, a request signed for any other region is rejected
with **`SubstrateInvalidPricingEndpoint`** (HTTP 400). The code is deliberately
not an AWS code — this is Substrate reporting a condition AWS surfaces at the
transport layer, and naming it as such is better than silently pricing a request
against an endpoint that does not exist.

## Seeding failures

Pricing is the kind of dependency whose failure should degrade a caller, not stop
it. That property is only testable if the failure can be produced on demand:

```bash
# Fail one operation.
curl -X POST http://localhost:4566/v1/pricing/query-failures \
  -d '{"operation":"GetProducts","code":"ThrottlingException","message":"Rate exceeded"}'

# Fail every operation (wildcard).
curl -X POST http://localhost:4566/v1/pricing/query-failures \
  -d '{"code":"InternalErrorException"}'

# Clear one, or all.
curl -X DELETE 'http://localhost:4566/v1/pricing/query-failures?operation=GetProducts'
curl -X DELETE http://localhost:4566/v1/pricing/query-failures
```

An operation-specific seed takes precedence over the wildcard. `statusCode`
defaults to the status the Price List API documents for the code — 400 for every
documented code except `InternalErrorException`, which is 500 — and may be
overridden explicitly.

`code` must be one of the seven codes the Price List API documents:
`AccessDeniedException`, `ExpiredNextTokenException`, `InvalidNextTokenException`,
`InvalidParameterException`, `NotFoundException`, `ThrottlingException`,
`InternalErrorException`. Anything else is rejected with a 400, because a typo'd
code would seed an error no SDK catch branch matches — the fallback path would go
untested while the seed itself appeared to work.

## Seeding an offer document

The bundled corpus answers *what does AWS charge*. Every SKU in it is copied from a
real offer file, and it can never be grown to answer the other question a consumer
has — *what does my code do when the rate is X* — because a fixture carrying an
invented rate is worse than no rate: a caller computing a cost from it is wrong with
no way to notice. `us-east-1` alone publishes 107,022 compute-instance products, so
a consumer whose cost path depends on a rate outside the bundled 39 SKUs had no
offline test at all.

An invented rate therefore arrives the way a deterministic emulator is allowed to
produce a different answer: as a seed the caller wrote, in the request that caused
it.

```bash
# Seed one offer document, in the shape GetProducts serves it.
curl -X POST http://localhost:4566/v1/pricing/offers -d @offer.json

# Override a SKU the bundled corpus measures — deliberately.
curl -X POST 'http://localhost:4566/v1/pricing/offers?replace=true' -d @offer.json

# Remove one, or every seed.
curl -X DELETE 'http://localhost:4566/v1/pricing/offers?sku=SEEDEDRDSSKU0001'
curl -X DELETE http://localhost:4566/v1/pricing/offers
```

The body is one `PriceList` element — `product`, `serviceCode`, `terms`, and
optionally `version` and `publicationDate` — so a seed can be pasted straight out of
a real offer file or out of a recorded response. `replace` is a query parameter
rather than a body member for exactly that reason: a body member would make the
document no longer a document.

**A seeded SKU is not a second code path.** It becomes a corpus entry like any
other, so `GetProducts` filtering and paging, `DescribeServices` and
`GetAttributeValues` all see it through the same functions that serve the bundled
corpus. That matters because the discovery path AWS documents runs
`DescribeServices` → `GetAttributeValues` → a `GetProducts` filter, and those two
refuse an unknown `ServiceCode` with a *different* code from `GetProducts`
(`InvalidParameterException` against `NotFoundException`) — an overlay that reached
only the last of the three would let a caller query a SKU the first two deny exists.

Four decisions the endpoint makes, each because the alternative would let a test
pass against something AWS never serves:

| Question | Answer |
|----------|--------|
| Does a seeded service appear in `DescribeServices`? | Yes, and its `AttributeNames` is **computed** — the union of its seeded products' attribute keys, plus `productFamily` when one is carried, unioned with the bundled list for a service that is also bundled. A declared list could not hold the property the bundled lists hold, that every name reported filters to at least one product. |
| May a seed replace a bundled SKU? | Only with `?replace=true`. Overriding a measured rate is useful; overriding it *silently* is the one thing the corpus exists to prevent. The flag does not prevent the override, it puts it in the request that caused it. A seed always replaces an earlier seed, which is what makes the endpoint re-runnable. |
| Which revision does a seeded document report? | Its own `version`/`publicationDate` when it carries them, otherwise `substrate-seeded` / `1970-01-01T00:00:00Z`. Those are deliberately not plausible — a real offer file reports a 14-digit version — and a seeded SKU for a bundled service does **not** inherit that service's real revision, because the measured file does not contain it. |
| Where does a seed appear in `PriceList` order? | After the bundled entries, sorted by SKU among themselves. The bundled order is fixed so that page boundaries are stable; a map-ordered overlay would undo that for every seeded query. |

A seed that would emit a shape the real API does not is refused with a 400 naming
the member, because such a seed would let a consumer's parser pass here and fail
against AWS — the exact class of bug the corpus was assembled to expose. Refused:
an empty `product.sku` or `serviceCode`; a product with no `usagetype` (the one
attribute present on every product in both bundled offer files); a `terms` key other
than `OnDemand`, or more than one term, since a corpus entry carries exactly one and
a dropped `Reserved` term would serve half of what was seeded; an on-demand key that
is not `<sku>.<offerTermCode>`; a price-dimension key that is not
`<sku>.<offerTermCode>.<dimensionCode>` or whose `rateCode` disagrees with it; an
empty `unit`, `effectiveDate` or `pricePerUnit`; and a `pricePerUnit` value that is
not a decimal string — including the JSON *number* a hand-written seed most often
carries, where AWS emits every rate as a string.

Seeded offers live under their own state prefix, so clearing seeded failures
(`DELETE /v1/pricing/query-failures`) leaves them in place and vice versa.

## Cost

Price List API calls are free.
