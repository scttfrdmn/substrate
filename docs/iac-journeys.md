# Terraform and CDK

Substrate answers the AWS calls the Terraform AWS provider and the AWS CDK CLI make, so an
infrastructure-as-code change can be applied, read back and destroyed before it touches an AWS
account. This page shows how to point each tool at substrate, and describes the two journeys
the repository runs in CI to keep that true.

Substrate models what an API call can observe, not what the resource would do
([Scope & Philosophy](/scope)). A KMS key named on a queue is recorded, not used to encrypt; a
Lambda function's package is stored, not run. What Terraform and the CDK check — that each create
succeeds, that a read returns what was written, that a delete removes it — is all observable, and
that is what substrate answers.

## Terraform

Point the AWS provider at substrate with one endpoint for every service and the three checks
that would otherwise reach real AWS turned off:

```hcl
provider "aws" {
  region = "us-east-1"

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  # Substrate serves every bucket from one host.
  s3_use_path_style = true
}
```

```sh
substrate server --address 127.0.0.1:4566 &
export AWS_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
terraform init
terraform apply -auto-approve
terraform plan -detailed-exitcode   # exit 0: substrate reads back what the apply wrote
terraform destroy -auto-approve
```

`AWS_ENDPOINT_URL` sends every service to substrate. An `endpoints` block works too, one entry per
service; see [Endpoint Configuration](/endpoint-configuration#terraform-aws-provider). Use the
literal `127.0.0.1` rather than `localhost`, so the provider's SDK does not try a virtual-hosted
S3 address.

[`examples/terraform`](https://github.com/scttfrdmn/substrate/tree/main/examples/terraform) is a
complete configuration: a VPC, a subnet, a security group, an IAM role and policy, a versioned S3
bucket, an SQS queue, an SNS topic, a DynamoDB table, a KMS key, a Secrets Manager secret, a
CloudWatch alarm, an EventBridge rule, a log group and a Lambda function from a zip.

## AWS CDK

The CDK CLI reads the same variables. Bootstrap the environment once per server, as you would
once per account, then deploy:

```sh
export AWS_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export CDK_DISABLE_VERSION_CHECK=true
npx cdk bootstrap aws://123456789012/us-east-1 --no-notices
npx cdk deploy --require-approval never --no-notices
npx cdk destroy --force --no-notices
```

`cdk bootstrap` deploys the default `CDKToolkit` stack (qualifier `hnb659fds`) through a
CloudFormation change set, and `cdk deploy` uploads the synthesized template to that stack's
staging bucket and deploys it from there. Substrate's account is `123456789012` unless the server
names others. `--no-notices` and `CDK_DISABLE_VERSION_CHECK` stop the CLI's own calls to the
internet.

[`examples/cdk`](https://github.com/scttfrdmn/substrate/tree/main/examples/cdk) is a TypeScript app
(a bucket, a queue and a table) with a lockfile that pins `aws-cdk-lib` and the CLI. It has no
Docker assets: substrate builds no images.

## The journeys in CI

`test/iac` drives both examples with the real tools against an in-process server
(`emulator.StartTestServer`). The Terraform journey runs `init`, `apply -parallelism=1`, a
second `plan -detailed-exitcode` that must find nothing to change, and `destroy`, and checks the
state is empty. The CDK journey runs `bootstrap`, `deploy`, checks the stack's outputs and
resources, and runs `destroy`. Both read what they created back through the AWS API.

```sh
make iac-e2e-setup   # once, with network: the AWS provider and examples/cdk's node_modules
make iac-e2e         # offline
```

The journeys skip unless `SUBSTRATE_IAC=1` is set and the tool is installed; `make iac-e2e` sets
it. Nothing inside a test uses the network: Terraform installs the provider from
`TF_PLUGIN_CACHE_DIR` as a filesystem mirror and never contacts the registry, and the CDK CLI is
the one `npm ci` installed. Each tool runs with a fresh `HOME`, so nothing in your own shell's
AWS configuration reaches it.

| Variable | Effect |
|----------|--------|
| `SUBSTRATE_IAC=1` | Run the journeys rather than skip them |
| `TF_PLUGIN_CACHE_DIR` | Where the AWS provider is read from; defaults to `~/.terraform.d/plugin-cache` |
| `SUBSTRATE_IAC_TERRAFORM` | The Terraform binary, for example `tofu`; defaults to `terraform` |
| `SUBSTRATE_IAC_TIMEOUT` | The longest one tool invocation may take, as a Go duration; defaults to 10m |
| `SUBSTRATE_IAC_VERBOSE=1` | Stream each tool's output, and log the calls substrate refused |
| `SUBSTRATE_IAC_EVENTS` | A directory to write every recorded event to, as NDJSON, one file per run |
| `SUBSTRATE_IAC_TF_PARALLELISM` | Terraform's `-parallelism`; anything above 1 collects every failure in one run but breaks the double-run comparison |

When a journey fails it logs each call substrate refused, once per operation and error, which
names the API call to look at rather than the resource that failed.

## Every journey runs twice

The CDK journey, and a set of the SDK journeys in `test/e2e`, run under `emulator.RunTwice`. It
starts two fresh servers in [deterministic-run mode](/testing-guide#deterministic-runs), runs the
journey against each, pairs the two servers' recorded events by sequence, and fails unless they
agree: the same count, the same operations, and the same request IDs, statuses, errors, headers
other than `Date`, and response bodies. A body whose order came from iterating a Go map shows up
as a difference between the runs. The CDK CLI names its change sets with fresh random values,
so the journey preloads `test/iac/testdata/deterministic-random.cjs` into Node, which replaces
every random source with one seeded stream. Without it the runs would differ in what the CLI
sent rather than in what substrate answered.

The Terraform journey runs under `emulator.RunThenReplay` instead. Terraform walks independent
resources in whichever order its scheduler picks, even at `-parallelism=1`, and its waiters
poll on the wall clock, so two runs of one configuration issue the same calls in different
orders. Deterministic-run mode promises identical answers only to identical sequences.
`RunThenReplay` runs the journey once, replays the recorded requests in recorded order into a
second fresh server, and fails on any difference the replay reports, so the second server gets
the identical sequence and any difference is substrate's.

```go
func TestMyStack(t *testing.T) {
    emulator.RunTwice(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
        func(t *testing.T, ts *emulator.TestServer) {
            // drive your tool or SDK client at ts.URL, one request at a time
        })
}
```

`emulator.CompareRecordedRuns` is the comparison on its own, for two event logs obtained some
other way. A `RunTwice` journey has to issue its requests one at a time and in the same order on
every run: requests sent concurrently draw their request IDs in whichever order the scheduler
runs them. Use `RunThenReplay` when the caller cannot promise the same order.
