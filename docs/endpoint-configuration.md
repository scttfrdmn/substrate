# Endpoint Configuration Guide

This guide shows how to point each major AWS SDK and tool at a running Substrate
server instead of the real AWS APIs.

Substrate listens on `:4566` by default — the same default port as LocalStack —
so most LocalStack configurations work without changes.

---

## Quick start: substratelocal wrapper

The `substratelocal` binary injects the necessary environment variables into any
child process automatically:

```sh
substratelocal aws s3 ls
substratelocal terraform apply
substratelocal python deploy.py
```

By default it targets `http://localhost:4566`. Override with `--endpoint` or
`SUBSTRATE_ENDPOINT`:

```sh
substratelocal --endpoint http://my-ci-host:4566 aws s3 ls
SUBSTRATE_ENDPOINT=http://my-ci-host:4566 substratelocal aws s3 ls
```

---

## AWS CLI v2

### Environment variable (recommended)

```sh
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls
aws dynamodb list-tables
```

### Per-command flag

```sh
aws --endpoint-url http://localhost:4566 s3 ls
```

### AWS profile (`~/.aws/config`)

```ini
[profile substrate]
endpoint_url = http://localhost:4566
region = us-east-1

[profile substrate]
aws_access_key_id = test
aws_secret_access_key = test
```

Then use `--profile substrate` or `export AWS_PROFILE=substrate`.

---

## AWS SDK for Go v2

```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

cfg, err := config.LoadDefaultConfig(context.Background(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(
        aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
            return aws.Credentials{
                AccessKeyID:     "test",
                SecretAccessKey: "test",
            }, nil
        }),
    ),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:4566"}, nil
        }),
    ),
)

client := s3.NewFromConfig(cfg)
```

Or set `AWS_ENDPOINT_URL=http://localhost:4566` and let the SDK pick it up
automatically (SDK v2 ≥ 1.27.0 honours `AWS_ENDPOINT_URL`).

---

## AWS SDK for Python (boto3)

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)
s3.list_buckets()
```

Or set the environment variables and omit the explicit kwargs:

```sh
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

---

## Terraform (AWS provider)

```hcl
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3       = "http://localhost:4566"
    dynamodb = "http://localhost:4566"
    iam      = "http://localhost:4566"
    lambda   = "http://localhost:4566"
    sqs      = "http://localhost:4566"
    sns      = "http://localhost:4566"
    # add other services as needed
  }
}
```

---

## CDK (AWS Cloud Development Kit)

Set the environment variables before running `cdk deploy`:

```sh
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
cdk deploy
```

Or use `substratelocal`:

```sh
substratelocal cdk deploy
```

---

## Prism integration

Prism polls `/_localstack/health` to verify services are available. Substrate
exposes this endpoint and returns all registered plugins as `"available"`:

```json
{
  "services": {
    "s3": "available",
    "dynamodb": "available",
    "lambda": "available",
    ...
  },
  "version": "v0.24.0"
}
```

Set the following environment variables before starting Prism:

```sh
export LOCALSTACK_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

---

## Docker / Docker Compose

Run Substrate as a sidecar and configure the AWS SDK to use the container's
hostname:

```yaml
services:
  substrate:
    image: ghcr.io/scttfrdmn/substrate:latest
    ports:
      - "4566:4566"

  app:
    image: my-app
    environment:
      AWS_ENDPOINT_URL: http://substrate:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
    depends_on:
      - substrate
```

---

## State reset between tests

To wipe all emulated state between test suites without restarting the server,
send a `POST /v1/state/reset`:

```sh
curl -X POST http://localhost:4566/v1/state/reset
# {"status":"ok"}
```

In Go integration tests, use the [StartTestServer] helper:

```go
func TestMyWorkflow(t *testing.T) {
    ts := substrate.StartTestServer(t)

    t.Run("first case", func(t *testing.T) {
        defer ts.ResetState(t)
        // ... test using ts.URL ...
    })

    t.Run("second case", func(t *testing.T) {
        defer ts.ResetState(t)
        // ... clean state ...
    })
}
```
