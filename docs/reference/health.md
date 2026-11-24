# Health

**Endpoint:** `health.us-east-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSHealth_20160804.{Op}`)

The Health plugin is a stub that returns empty valid responses. It exists to
allow infrastructure code that calls the Health API to run without errors.

## Supported operations

| Operation | Notes |
|-----------|-------|
| DescribeEvents | Returns empty events list |
| DescribeEventDetails | Returns empty details |
| DescribeAffectedEntities | Returns empty entities |
| DescribeEventAggregates | Returns empty aggregates. Nothing in substrate raises a Health event, so an empty answer is the honest one and a consumer's loop over it terminates |

## Cost

Health API calls are free.
