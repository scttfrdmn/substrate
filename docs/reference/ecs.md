# ECS

**Endpoint:** `ecs.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonEC2ContainerServiceV20141113.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateCluster | |
| DescribeClusters | `include` is not read, so a cluster's `tags` are reported whether or not the request asked for them. Unlike the task-definition case below this over-reports a member `API_Cluster` does publish, so it is a page-size-style divergence recorded rather than smoothed over |
| DeleteCluster | |
| ListClusters | |
| RegisterTaskDefinition | `tags` is reported as a **top-level response element**, a sibling of `taskDefinition`, which is where the operation publishes it — `API_TaskDefinition` has no `tags` member at all (#756) |
| DescribeTaskDefinition | Same placement, and gated as the reference states it: tags are reported only for `include: ["TAGS"]`, and a request that omits `include` gets no `tags` member (#756) |
| DeregisterTaskDefinition | Sets `status` to `INACTIVE` and reports the definition; the record stays, so `DescribeTaskDefinition` still resolves it. The reference accepts an ARN, `family:revision` or a bare `family` (latest revision), the same three forms every task-definition member does. `taskDefinition` is its only response element — there is no `tags` here even when the definition has them |
| ListTaskDefinitions | |
| ListTaskDefinitionFamilies | Every family in the caller's account and Region, in one page. `familyPrefix`, `status` and `maxResults` are unread, so a deregistered family is still listed and no `nextToken` is ever returned |
| CreateService | The service is reported without a `clusterName` member: `API_Service` publishes `clusterArn` and has no such member, and the name is the ARN's last segment (#756) |
| DescribeServices | Same service shape as CreateService. `include` is not read here either, so `tags` are always reported |
| ListServices | Service ARNs for one cluster — `cluster` defaults to `default` and accepts a name or a cluster ARN. One page; `launchType`, `schedulingStrategy` and `maxResults` are unread |
| UpdateService | Same service shape as CreateService |
| DeleteService | Same service shape as CreateService |
| RunTask | A task starts `RUNNING`, so it reports `startedAt` and **no** `stoppedAt` — an optional timestamp with no value is omitted rather than reported as `null` (#756) |
| DescribeTasks | Same task shape as RunTask |
| ListTasks | |
| StopTask | `lastStatus` and `desiredStatus` both `STOPPED`, with `stoppedAt` and the request's `reason` as `stoppedReason`. `containers` is absent throughout: substrate does not run the workload, so it has no container to describe |
| TagResource | Addressed **only** by `resourceArn`, with no account or Region taken from the request context, so an ARN naming another account's cluster cannot reach the caller's same-named one. `ResourceNotFoundException` for an ARN that resolves nothing and for a resource that does not exist — previously both answered the 200-with-empty-body AWS documents for a *successful* tag, over a resource nothing had been written to, while `ListTagsForResource` refused the same ARN (#845) |
| UntagResource | Same ARN resolution and same refusals; an unknown key is a no-op |
| ListTagsForResource | The stored `tags` array. The write path edits the record as raw JSON rather than decoding one of the four shapes the ECS namespace holds, so tagging a service or task definition cannot silently truncate it to a cluster's members (#845) |

**Responses are rendered from the published shape.** All four ECS records — cluster, task
definition, service and task — used to be answered to the caller as stored, so each of the
thirteen operations that reports a resource carried substrate's own `AccountID` and `Region`, and
`ever_tagged` once the resource had been tagged. None is a member of `API_Cluster`,
`API_TaskDefinition`, `API_Service` or `API_Task`. Each record is now projected onto its published
shape before it is answered (#756), which is also what settled the three divergences noted in the
table above: a service no longer reports a `clusterName` AWS does not publish, task-definition
`tags` moved to the top-level element the three operations publish them as, and an unset
`startedAt`/`stoppedAt` is omitted rather than rendered `null`. The fields themselves stay on all
four stored records, so the state a recorded run replays from is unchanged and the Resource Groups
Tagging API — which reads those records rather than these responses — still reports an ECS
resource's tags and whether it has ever been tagged.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ECS::Cluster | ClusterName | |
| AWS::ECS::TaskDefinition | TaskDefinitionArn | |
| AWS::ECS::Service | ServiceName | |

## Cost

ECS Fargate vCPU: $0.04048 per vCPU-hour. Memory: $0.004445 per GB-hour.
