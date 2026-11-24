# Cost Explorer

**Endpoint:** `ce.us-east-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSInsightsIndexService.{Op}`)

Cost Explorer reads from the Substrate `EventStore` to return real usage data
from your test runs.

## Supported operations

| Operation | Notes |
|-----------|-------|
| GetCostAndUsage | Aggregates event costs by service/operation |
| GetCostForecast | Returns stub forecast based on recent usage |
| GetDimensionValues | The service names the caller's recorded events carry, sorted, each with empty `Attributes`; `ReturnSize` equals `TotalSize` because there is one page. `Dimension` and `TimePeriod` are decoded and **unread** — every dimension answers with service names, so a caller asking for `INSTANCE_TYPE` gets services |

## Cost

Cost Explorer API calls: $0.01 per request.
