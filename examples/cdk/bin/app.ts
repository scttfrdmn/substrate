// The CDK app substrate's IaC journey deploys (test/iac). Node runs this file
// directly: it strips the TypeScript types itself, so the app needs no ts-node and
// no build step.
import * as cdk from 'aws-cdk-lib';
import { StorageStack } from '../lib/storage-stack.ts';

const app = new cdk.App();

// The environment is the one the CLI resolves from its credentials, which against
// substrate is the default account 123456789012 and the region in AWS_REGION.
new StorageStack(app, 'SubstrateCdkExample', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});

app.synth();
