# AWS CDK against substrate

A TypeScript CDK app, `SubstrateCdkExample`, with a versioned S3 bucket, an SQS queue and a
DynamoDB table. It has no Docker assets and no custom resources, so the only asset `cdk deploy`
publishes is the template itself.

## Run it

Node 24 or later runs `bin/app.ts` directly, stripping its types itself, so the app needs no
`ts-node` and no build step. `npm ci` installs `aws-cdk-lib` and the CDK CLI at the versions
`package-lock.json` pins.

```sh
substrate server --address 127.0.0.1:4566 &

npm ci
export AWS_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export CDK_DISABLE_VERSION_CHECK=true

npx cdk bootstrap aws://123456789012/us-east-1 --no-notices
npx cdk deploy --require-approval never --no-notices
npx cdk destroy --force --no-notices
```

`cdk bootstrap` creates the default `CDKToolkit` stack, with its staging bucket, its ECR
repository and its roles, through a CloudFormation change set. `cdk deploy` uploads the template to
that bucket and deploys the stack from it. `npm run typecheck` runs `tsc` over the app.

`go.mod` is here only to keep the Go toolchain out of this directory: once `npm ci` has run,
`node_modules/aws-cdk` contains Go init templates whose file names Go refuses, and `go test ./...`
at the repository root would otherwise try to build them.

## In CI

`test/iac` bootstraps, deploys and destroys this app twice against fresh deterministic-run
servers, checks the stack's outputs and resources, and diffs every response the two servers
recorded. See [Terraform and CDK](../../docs/iac-journeys.md), or run it with
`make iac-e2e-setup iac-e2e`.
