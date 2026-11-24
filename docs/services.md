# Service Reference

<script setup>
import { onMounted, onUnmounted } from 'vue'
import { useRouter, withBase } from 'vitepress'
import legacyAnchors from './.vitepress/legacy-anchors.json'

// A link written when every service section was on this page reads /services#<anchor>.
// This sends it to the page under /reference/ that holds the heading now. The map is
// frozen; cmd/gen-service-reference checks each entry still lands (#1610).
//
// The URL is replaced before router.go so the old link leaves no history entry: with one,
// Back would land on the old anchor and redirect forward again.
const router = useRouter()
function redirect() {
  let anchor
  try {
    anchor = decodeURIComponent(location.hash.slice(1))
  } catch {
    return
  }
  if (!Object.hasOwn(legacyAnchors, anchor)) return
  const target = legacyAnchors[anchor]
  history.replaceState({}, '', withBase(target))
  router.go(withBase(target))
}
onMounted(() => { redirect(); window.addEventListener('hashchange', redirect) })
onUnmounted(() => window.removeEventListener('hashchange', redirect))
</script>

## Coverage matrix

<!-- BEGIN GENERATED COVERAGE MATRIX -->
Substrate ships **68 built-in service plugins** routing **1144 operations**. This
section is generated from the plugin registry and the operation catalog
(`make docs-reference`), so the counts and the plugin list cannot drift from the
implementation: the catalog is itself generated from each plugin's dispatch switch
(`make operation-catalog`) and is readable at run time through
`emulator.RoutedServices` and `emulator.RoutedOperations`. The live plugin count is
also available from the `/ready` endpoint (`curl http://localhost:4566/ready`).
Each row links to the plugin's page, which carries its operation, CloudFormation and
cost detail.

Two plugins route **0** operations, and that is their protocol rather than a gap:
`execute-api` serves a deployed API's own routes (AWS's action for it is the single
`execute-api:Invoke`), and `opensearch` dispatches on the HTTP method plus path
shape, as AWS's own per-verb `es:ESHttp*` actions reflect.

| # | Service | Plugin name | Protocol | Operations |
|---|---------|-------------|----------|-----------:|
| 1 | [Account Management](/reference/account) | `account` | REST/JSON | 4 |
| 2 | [ACM](/reference/acm) | `acm` | JSON | 8 |
| 3 | [API Gateway (REST)](/reference/apigateway) | `apigateway` | REST/JSON | 41 |
| 4 | [API Gateway (HTTP)](/reference/apigatewayv2) | `apigatewayv2` | REST/JSON | 35 |
| 5 | [AppSync](/reference/appsync) | `appsync` | REST/JSON | 24 |
| 6 | [Athena](/reference/athena) | `athena` | JSON | 9 |
| 7 | [EC2 Auto Scaling](/reference/autoscaling) | `autoscaling` | Query | 17 |
| 8 | [Backup](/reference/backup) | `backup` | REST/JSON | 13 |
| 9 | [Batch](/reference/batch) | `batch` | REST/JSON | 16 |
| 10 | [Bedrock Runtime](/reference/bedrock-runtime) | `bedrock-runtime` | REST/JSON | 6 |
| 11 | [Budgets](/reference/budgets) | `budgets` | JSON | 5 |
| 12 | [Cost Explorer](/reference/ce) | `ce` | JSON | 3 |
| 13 | [CloudFormation](/reference/cloudformation) | `cloudformation` | Query | 20 |
| 14 | [CloudFront](/reference/cloudfront) | `cloudfront` | REST/XML | 17 |
| 15 | [CloudTrail](/reference/cloudtrail) | `cloudtrail` | JSON | 8 |
| 16 | [CodeBuild](/reference/codebuild) | `codebuild` | JSON | 7 |
| 17 | [CodeDeploy](/reference/codedeploy) | `codedeploy` | JSON | 9 |
| 18 | [CodePipeline](/reference/codepipeline) | `codepipeline` | JSON | 8 |
| 19 | [Cognito Identity](/reference/cognito-identity) | `cognito-identity` | JSON | 8 |
| 20 | [Cognito Identity Provider](/reference/cognito-idp) | `cognito-idp` | JSON | 34 |
| 21 | [Config](/reference/config) | `config` | JSON | 25 |
| 22 | [DynamoDB](/reference/dynamodb) | `dynamodb` | JSON | 28 |
| 23 | [EC2 / VPC](/reference/ec2) | `ec2` | Query | 100 |
| 24 | [ECR](/reference/ecr) | `ecr` | JSON | 26 |
| 25 | [ECS](/reference/ecs) | `ecs` | JSON | 21 |
| 26 | [EFS](/reference/efs) | `efs` | REST/JSON | 13 |
| 27 | [ElastiCache](/reference/elasticache) | `elasticache` | Query | 17 |
| 28 | [ELBv2](/reference/elasticloadbalancing) | `elasticloadbalancing` | Query | 24 |
| 29 | [EMR Serverless](/reference/emrserverless) | `emrserverless` | REST/JSON | 7 |
| 30 | [EventBridge](/reference/eventbridge) | `eventbridge` | JSON | 12 |
| 31 | [API Gateway (execute-api)](/reference/execute-api) | `execute-api` | REST/JSON | 0 |
| 32 | [Kinesis Data Firehose](/reference/firehose) | `firehose` | JSON | 6 |
| 33 | [FSx](/reference/fsx) | `fsx` | JSON | 3 |
| 34 | [Glue](/reference/glue) | `glue` | JSON | 33 |
| 35 | [Health](/reference/health) | `health` | JSON | 4 |
| 36 | [IAM](/reference/iam) | `iam` | Query | 79 |
| 37 | [Kinesis Data Streams](/reference/kinesis) | `kinesis` | JSON | 29 |
| 38 | [KMS](/reference/kms) | `kms` | JSON | 25 |
| 39 | [Lambda](/reference/lambda) | `lambda` | REST/JSON | 31 |
| 40 | [CloudWatch Logs](/reference/logs) | `logs` | JSON | 16 |
| 41 | [CloudWatch](/reference/monitoring) | `monitoring` | CBOR / JSON / Query | 14 |
| 42 | [MSK](/reference/msk) | `msk` | REST/JSON | 9 |
| 43 | [HealthOmics](/reference/omics) | `omics` | REST/JSON | 5 |
| 44 | [OpenSearch](/reference/opensearch) | `opensearch` | REST/JSON | 0 |
| 45 | [Organizations](/reference/organizations) | `organizations` | JSON | 39 |
| 46 | [Price List Query API](/reference/pricing) | `pricing` | JSON | 3 |
| 47 | [QuickSight](/reference/quicksight) | `quicksight` | REST/JSON | 4 |
| 48 | [RAM](/reference/ram) | `ram` | REST/JSON | 8 |
| 49 | [RDS](/reference/rds) | `rds` | Query | 23 |
| 50 | [Redshift](/reference/redshift) | `redshift` | Query | 10 |
| 51 | [Redshift Data API](/reference/redshift-data) | `redshift-data` | JSON | 3 |
| 52 | [Route 53](/reference/route53) | `route53` | REST/XML | 6 |
| 53 | [S3](/reference/s3) | `s3` | REST/XML | 54 |
| 54 | [SageMaker](/reference/sagemaker) | `sagemaker` | JSON | 10 |
| 55 | [EventBridge Scheduler](/reference/scheduler) | `scheduler` | REST/JSON | 12 |
| 56 | [Secrets Manager](/reference/secretsmanager) | `secretsmanager` | JSON | 16 |
| 57 | [Service Quotas](/reference/servicequotas) | `servicequotas` | JSON | 9 |
| 58 | [SES v2](/reference/sesv2) | `sesv2` | REST/JSON | 5 |
| 59 | [SNS](/reference/sns) | `sns` | Query | 18 |
| 60 | [SQS](/reference/sqs) | `sqs` | JSON | 17 |
| 61 | [SSM](/reference/ssm) | `ssm` | JSON | 15 |
| 62 | [SSO / Identity Store](/reference/sso) | `sso` | JSON | 12 |
| 63 | [Step Functions](/reference/states) | `states` | JSON | 18 |
| 64 | [STS](/reference/sts) | `sts` | Query | 3 |
| 65 | [Resource Groups Tagging](/reference/tagging) | `tagging` | JSON | 3 |
| 66 | [Timestream](/reference/timestream) | `timestream` | JSON | 12 |
| 67 | [Transfer Family](/reference/transfer) | `transfer` | JSON | 12 |
| 68 | [WAFv2](/reference/wafv2) | `wafv2` | JSON | 13 |
<!-- END GENERATED COVERAGE MATRIX -->

---

Each plugin in the matrix has its own page, linked from its row. A page is written and
maintained by hand, and carries the operations substrate routes, the divergences from
the published API with the issue that tracks each one, what a refusal reports, the
CloudFormation resource types the service deploys, and its cost notes. A page describes
what substrate does today rather than what AWS does: where the two differ, the
difference is named and linked rather than smoothed over.

## Behaviour every service shares

These pages cover what holds across plugins rather than inside one:

- [Request routing](/reference/concepts/routing)
- [Errors](/reference/concepts/errors)
- [Which account a request is attributed to](/reference/concepts/accounts)
- [The order a listing returns its members in](/reference/concepts/listing-order)
- [Seeds and replay](/reference/concepts/seeds-and-replay)
- [Fault injection](/reference/concepts/fault-injection)
