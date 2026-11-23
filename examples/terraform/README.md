# Terraform against substrate

Fourteen kinds of resource most Terraform configurations start from, applied with the AWS
provider pointed at substrate: a VPC, a subnet, a security group, an IAM role and policy, a
versioned S3 bucket, an SQS queue, an SNS topic, a DynamoDB table, a KMS key, a Secrets Manager
secret, a CloudWatch metric alarm, an EventBridge rule, a CloudWatch Logs log group, and a Lambda
function from a zip.

## Run it

```sh
substrate server --address 127.0.0.1:4566 &

export AWS_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1

terraform init
terraform apply -auto-approve
terraform plan -detailed-exitcode    # exit 0 means substrate reads back what the apply wrote
terraform destroy -auto-approve
```

`provider.tf` turns off the three checks that would reach real AWS (credential validation, the
instance-metadata probe and the account lookup) and sets `s3_use_path_style`, because substrate
serves every bucket from one host. `AWS_ENDPOINT_URL` sends every service to substrate.

The provider is pinned to `6.55.0` in `versions.tf`, and `.terraform.lock.hcl` records its
checksums for linux and darwin on amd64 and arm64.

## The Lambda package

`handler.zip` holds `handler/index.mjs`. Substrate stores a function's package and never runs it,
so the code only has to be a valid archive; a checked-in zip is cheaper than adding the `archive`
provider. Rebuild it after editing the handler with
`cd handler && zip -X ../handler.zip index.mjs`.

## In CI

`test/iac` runs this configuration twice against fresh deterministic-run servers, checks that the
second plan is empty and that destroy leaves no state, and diffs every response the two servers
recorded. See [Terraform and CDK](../../docs/iac-journeys.md), or run it with
`make iac-e2e-setup iac-e2e`.
