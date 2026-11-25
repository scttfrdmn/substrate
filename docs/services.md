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
Substrate ships **71 built-in service plugins** routing **1307 operations**. This
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
| 5 | [Application Auto Scaling](/reference/application-autoscaling) | `application-autoscaling` | JSON | 13 |
| 6 | [AppSync](/reference/appsync) | `appsync` | REST/JSON | 24 |
| 7 | [Athena](/reference/athena) | `athena` | JSON | 9 |
| 8 | [EC2 Auto Scaling](/reference/autoscaling) | `autoscaling` | Query | 17 |
| 9 | [Backup](/reference/backup) | `backup` | REST/JSON | 13 |
| 10 | [Batch](/reference/batch) | `batch` | REST/JSON | 16 |
| 11 | [Bedrock Runtime](/reference/bedrock-runtime) | `bedrock-runtime` | REST/JSON | 6 |
| 12 | [Budgets](/reference/budgets) | `budgets` | JSON | 5 |
| 13 | [Cost Explorer](/reference/ce) | `ce` | JSON | 3 |
| 14 | [CloudFormation](/reference/cloudformation) | `cloudformation` | Query | 20 |
| 15 | [CloudFront](/reference/cloudfront) | `cloudfront` | REST/XML | 17 |
| 16 | [CloudTrail](/reference/cloudtrail) | `cloudtrail` | JSON | 8 |
| 17 | [CodeBuild](/reference/codebuild) | `codebuild` | JSON | 7 |
| 18 | [CodeDeploy](/reference/codedeploy) | `codedeploy` | JSON | 9 |
| 19 | [CodePipeline](/reference/codepipeline) | `codepipeline` | JSON | 8 |
| 20 | [Cognito Identity](/reference/cognito-identity) | `cognito-identity` | JSON | 8 |
| 21 | [Cognito Identity Provider](/reference/cognito-idp) | `cognito-idp` | JSON | 34 |
| 22 | [Config](/reference/config) | `config` | JSON | 25 |
| 23 | [DynamoDB](/reference/dynamodb) | `dynamodb` | JSON | 28 |
| 24 | [EC2 / VPC](/reference/ec2) | `ec2` | Query | 120 |
| 25 | [ECR](/reference/ecr) | `ecr` | JSON | 26 |
| 26 | [ECS](/reference/ecs) | `ecs` | JSON | 21 |
| 27 | [EFS](/reference/efs) | `efs` | REST/JSON | 13 |
| 28 | [ElastiCache](/reference/elasticache) | `elasticache` | Query | 17 |
| 29 | [ELBv2](/reference/elasticloadbalancing) | `elasticloadbalancing` | Query | 24 |
| 30 | [EMR Serverless](/reference/emrserverless) | `emrserverless` | REST/JSON | 7 |
| 31 | [EventBridge](/reference/eventbridge) | `eventbridge` | JSON | 21 |
| 32 | [API Gateway (execute-api)](/reference/execute-api) | `execute-api` | REST/JSON | 0 |
| 33 | [Kinesis Data Firehose](/reference/firehose) | `firehose` | JSON | 6 |
| 34 | [FSx](/reference/fsx) | `fsx` | JSON | 3 |
| 35 | [Glue](/reference/glue) | `glue` | JSON | 33 |
| 36 | [Health](/reference/health) | `health` | JSON | 4 |
| 37 | [IAM](/reference/iam) | `iam` | Query | 109 |
| 38 | [Kinesis Data Streams](/reference/kinesis) | `kinesis` | JSON | 29 |
| 39 | [KMS](/reference/kms) | `kms` | JSON | 25 |
| 40 | [Lambda](/reference/lambda) | `lambda` | REST/JSON | 42 |
| 41 | [CloudWatch Logs](/reference/logs) | `logs` | JSON | 16 |
| 42 | [CloudWatch](/reference/monitoring) | `monitoring` | CBOR / JSON / Query | 18 |
| 43 | [MSK](/reference/msk) | `msk` | REST/JSON | 9 |
| 44 | [HealthOmics](/reference/omics) | `omics` | REST/JSON | 5 |
| 45 | [OpenSearch](/reference/opensearch) | `opensearch` | REST/JSON | 0 |
| 46 | [Organizations](/reference/organizations) | `organizations` | JSON | 39 |
| 47 | [Price List Query API](/reference/pricing) | `pricing` | JSON | 3 |
| 48 | [QuickSight](/reference/quicksight) | `quicksight` | REST/JSON | 4 |
| 49 | [RAM](/reference/ram) | `ram` | REST/JSON | 8 |
| 50 | [RDS](/reference/rds) | `rds` | Query | 23 |
| 51 | [Redshift](/reference/redshift) | `redshift` | Query | 10 |
| 52 | [Redshift Data API](/reference/redshift-data) | `redshift-data` | JSON | 3 |
| 53 | [Route 53](/reference/route53) | `route53` | REST/XML | 19 |
| 54 | [S3](/reference/s3) | `s3` | REST/XML | 68 |
| 55 | [SageMaker](/reference/sagemaker) | `sagemaker` | JSON | 10 |
| 56 | [EventBridge Scheduler](/reference/scheduler) | `scheduler` | REST/JSON | 12 |
| 57 | [Secrets Manager](/reference/secretsmanager) | `secretsmanager` | JSON | 16 |
| 58 | [Cloud Map](/reference/servicediscovery) | `servicediscovery` | JSON | 26 |
| 59 | [Service Quotas](/reference/servicequotas) | `servicequotas` | JSON | 9 |
| 60 | [SES](/reference/ses) | `ses` | Query | 21 |
| 61 | [SES v2](/reference/sesv2) | `sesv2` | REST/JSON | 5 |
| 62 | [SNS](/reference/sns) | `sns` | Query | 18 |
| 63 | [SQS](/reference/sqs) | `sqs` | JSON | 17 |
| 64 | [SSM](/reference/ssm) | `ssm` | JSON | 15 |
| 65 | [SSO / Identity Store](/reference/sso) | `sso` | JSON | 12 |
| 66 | [Step Functions](/reference/states) | `states` | JSON | 18 |
| 67 | [STS](/reference/sts) | `sts` | Query | 5 |
| 68 | [Resource Groups Tagging](/reference/tagging) | `tagging` | JSON | 3 |
| 69 | [Timestream](/reference/timestream) | `timestream` | JSON | 12 |
| 70 | [Transfer Family](/reference/transfer) | `transfer` | JSON | 12 |
| 71 | [WAFv2](/reference/wafv2) | `wafv2` | JSON | 13 |
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
