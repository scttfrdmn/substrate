package emulator

import (
	"context"
	"fmt"
)

// The stamp beyond EC2 (#765).
//
// [StackDeployer.stampCFNResourceTags] resolved a physical ID to a state key with
// [ec2TaggableResource], which covers every EC2 type the deployer creates and nothing else.
// So most of a stack was unstamped: an S3 bucket, a DynamoDB table, a Lambda function, an SQS
// queue and every ELBv2 resource carried none of the three `aws:cloudformation:*` keys, and a
// policy or a cost-allocation assertion keyed on the stack name saw nothing on them.
//
// #819 then added the nine types in [cfnRegionalStampKinds], on the same test: a stamp counts
// as landed only when it reads back through the *owning service's own* tag call.
//
// The resolvers here are keyed on [DeployedResource.Type] rather than on the physical ID's
// shape, because outside EC2 a physical ID is a bare name with no prefix to switch on — a
// bucket named `orders` and a queue named `orders` are the same string. The CFN resource type
// is already carried beside it, so this needs no new bookkeeping.
//
// **AWS does not publish an exhaustive propagation list**, which is what makes a partial cut
// the right answer rather than a shortfall: "The propagation of stack-level tags to resources,
// including tags with the `aws:` prefix, varies by resource type. For example, tags aren't
// propagated to Amazon EBS volumes that are created from block device mappings." So the rule
// substrate follows is stated instead: a resource is stamped when substrate models tags for
// its service, and skipped silently otherwise. `docs/services.md` names what is skipped.

// cfnStampTarget names the state record holding one resource's tags.
type cfnStampTarget struct {
	// namespace is the state namespace the owning service keeps its records in.
	namespace string

	// stateKey is the key within that namespace.
	stateKey string
}

// cfnResolveStampTarget maps a deployed resource to the record holding its tags, reporting
// false for a type substrate models no tags for.
//
// Each key is the one the *owning plugin* writes, verified against it rather than copied from
// [TaggingPlugin.resolveARN], which computes a key for the same services and got SQS wrong
// until #826, DynamoDB's account wrong until #845, and both Lambda's and DynamoDB's region wrong
// until #943. That is the ordering rule when the two
// disagree: the record the service's own tag-reading call loads is the record the stamp has to
// land in, because #765's criterion is that the tag is readable through that call. Reusing the
// shape rather than the function is forced by the input anyway — `resolveARN` parses an ARN, and
// the deployer holds a CFN type and a physical ID, since [DeployedResource.ARN] is empty for
// most of the types it handles.
//
// ELBv2 is absent here and resolved separately, because its four kinds are keyed by a suffix
// that is not derivable from the name — see [cfnStampELBResource].
func cfnResolveStampTarget(dr DeployedResource, accountID, region string) (cfnStampTarget, bool) {
	if kind, ok := cfnRegionalStampKinds[dr.Type]; ok {
		return cfnStampTarget{
			namespace: kind.namespace,
			stateKey:  kind.prefix + ":" + accountID + "/" + region + "/" + dr.PhysicalID,
		}, true
	}

	switch dr.Type {
	case "AWS::S3::Bucket":
		// A bucket name is globally unique, so the key carries no account or region.
		return cfnStampTarget{namespace: s3Namespace, stateKey: "bucket:" + dr.PhysicalID}, true
	case "AWS::SQS::Queue":
		// `sqsURLKey` (`sqs_plugin.go:98`) keys a queue by the last two components of its URL,
		// so the record the SQS plugin reads is `queue:<account>/<name>` — the form the
		// deployer's own drift and deletion reads already use (`cfn_deployer.go:2110`, `:2352`).
		// [TaggingPlugin.resolveARN] built `queue:<name>` until #826 and so wrote a record the
		// SQS plugin never reads; it now builds this same key, from the ARN's own account.
		return cfnStampTarget{
			namespace: sqsNamespace,
			stateKey:  "queue:" + accountID + "/" + dr.PhysicalID,
		}, true
	default:
		return cfnStampTarget{}, false
	}
}

// cfnRegionalStampKind names the state record one CFN resource type's tags live in, for a
// service that keys its records by account and region.
type cfnRegionalStampKind struct {
	// namespace is the state namespace the owning service keeps its records in.
	namespace string

	// prefix is the key's leading segment, ahead of the account and region.
	prefix string
}

// cfnRegionalStampKinds are the CFN resource types whose tag record is keyed
// `<prefix>:<account>/<region>/<physical-id>` — #819's group 3a.
//
// They are one table rather than eleven `switch` arms because they share one key shape: each
// owning plugin builds its key from `reqCtx.AccountID + "/" + reqCtx.Region` and the same
// identifier the deployer already records as the physical ID. Lambda and DynamoDB joined the
// table with #943, which gave their keys the same shape — before it a function's key carried
// neither account nor region and a table's carried the account but not the region, so each
// needed its own arm. The two services left in [cfnResolveStampTarget]'s switch still cannot
// join: a bucket name is globally unique so its key carries neither, and a queue's carries the
// account but not the region.
//
// Both of #819's conditions were checked against the owning plugin for every entry, not
// inferred. First, [mergeResourceTags] already has an arm for the namespace and key prefix, so
// the stamp merges into the concrete record the service persists and the merge semantics are
// the ones a `TagResources` call gets. Second, the deployer already sets
// [DeployedResource.PhysicalID] to *exactly* the identifier the plugin keys on: a state
// machine, an ECR repository, an ECS cluster, a Kinesis stream and a Glue database are named by
// the template (falling back to the logical ID); an RDS instance and an ElastiCache cluster
// carry their own identifier property; and an EFS file system and access point take the
// `fs-`/`fsap-` ID out of the create response, which is what EFS's `ListTagsForResource` path
// segment names them by. A Lambda function and a DynamoDB table are named by the template's
// `FunctionName`/`TableName` (falling back to the logical ID), which is what
// [lambdaFunctionStateKey] and [DynamoDBPlugin.tableStateKey] key on.
//
// The first condition is what holds the rest of #819's group 3 out rather than a judgement about
// which service matters: KMS, Secrets Manager, SNS, a Step Functions activity, an RDS cluster or
// subnet group, ACM, CloudFront and SSM all keep tag state, but none has a [mergeResourceTags] arm
// that reaches it — so a stamp would have nowhere to land, and `TagResources` cannot reach them
// either. One defect with two symptoms, filed as
// [#835](https://github.com/scttfrdmn/substrate/issues/835). Two of them cannot be tagged through
// their own service at all — an RDS cluster and an RDS subnet group — because its ARN resolver has
// no arm for either kind, so they need that fixing first; and where an arm's namespace *is* in
// [mergeResourceTags] it may unmarshal one sibling unconditionally — `statesNamespace` assumes a
// state machine even for an `activity:` key, `rdsNamespace` a DB instance — which is why adding
// them is that issue's work and not a line here.
//
// **ECS's service and task definition are no longer in that group and are held out for a different
// reason.** [mergeResourceTags]' `ecsNamespace` arm merges through [ecsMergeRecordTags], which
// treats the record as raw JSON rather than as a cluster, so all four ECS kinds are reachable by
// `TagResources` and by a stamp alike. What they lack is an entry in [cfnRegionalStampKinds] — a
// table line, not a writer — because the deployer's physical ID for those two types has not been
// checked against the identifier `ecsTagStateKey` keys on, which is the second of the two
// conditions above and the one that cannot be inferred (#867).
var cfnRegionalStampKinds = map[string]cfnRegionalStampKind{
	"AWS::StepFunctions::StateMachine": {namespace: statesNamespace, prefix: "statemachine"},
	"AWS::ECR::Repository":             {namespace: ecrNamespace, prefix: "ecrrepo"},
	"AWS::ECS::Cluster":                {namespace: ecsNamespace, prefix: "cluster"},
	"AWS::EFS::FileSystem":             {namespace: efsNamespace, prefix: "filesystem"},
	"AWS::EFS::AccessPoint":            {namespace: efsNamespace, prefix: "accesspoint"},
	"AWS::ElastiCache::CacheCluster":   {namespace: elasticacheNamespace, prefix: "cachecluster"},
	"AWS::RDS::DBInstance":             {namespace: rdsNamespace, prefix: "dbinstance"},
	"AWS::Kinesis::Stream":             {namespace: kinesisNamespace, prefix: "stream"},
	"AWS::Glue::Database":              {namespace: glueNamespace, prefix: "database"},
	"AWS::Lambda::Function":            {namespace: lambdaNamespace, prefix: "function"},
	"AWS::DynamoDB::Table":             {namespace: dynamodbNamespace, prefix: "table"},
}

// cfnELBStampableTypes are the ELBv2 CFN types whose records substrate keeps tags on.
//
// All four became stampable with #748, which gave ELBv2 a tag store; before that the service
// kept no tags at all and there was nowhere for the stamp to go.
var cfnELBStampableTypes = map[string]bool{
	"AWS::ElasticLoadBalancingV2::LoadBalancer": true,
	"AWS::ElasticLoadBalancingV2::TargetGroup":  true,
	"AWS::ElasticLoadBalancingV2::Listener":     true,
	"AWS::ElasticLoadBalancingV2::ListenerRule": true,
}

// cfnStampResourceTags writes the given tags onto a resource the deployer created, for a
// service substrate models tags for but EC2's resolver does not reach. It reports whether it
// found somewhere to write.
//
// The two arms differ because the two tag stores differ, not by choice: every service
// [cfnResolveStampTarget] covers keeps tags in a record [mergeResourceTags] already knows how
// to merge, while ELBv2 keeps an ordered `[]ELBTag` on a record found by scanning for its ARN.
// Both upsert, as EC2's writer does, so re-deploying a stack rewrites the three values rather
// than accumulating them.
//
// A resource of a type neither arm knows reports false and is skipped by the caller in
// silence. That is the documented behavior, not an omission — see the header above.
func cfnStampResourceTags(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, tags []EC2Tag,
) (bool, error) {
	if cfnELBStampableTypes[dr.Type] {
		return cfnStampELBResource(state, reqCtx, dr, tags)
	}

	target, ok := cfnResolveStampTarget(dr, reqCtx.AccountID, reqCtx.Region)
	if !ok {
		return false, nil
	}
	add := make(map[string]string, len(tags))
	for _, t := range tags {
		add[t.Key] = t.Value
	}
	if err := mergeResourceTags(
		context.Background(), state, target.namespace, target.stateKey, add, nil,
	); err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.PhysicalID, err)
	}
	return true, nil
}

// cfnStampELBResource writes the stamp onto one ELBv2 resource, found by its ARN.
//
// The ARN rather than the physical ID, because that is what ELBv2's resolver takes and what a
// listener or rule is identified by — [StackDeployer.deployELBListener] sets the physical ID
// *to* the ARN, while a load balancer's and a target group's physical ID is its name and the
// ARN is carried beside it. A resource whose create failed has no ARN and never reaches here.
//
// [elbResolveTaggedResource]'s refusal is treated as "nothing to stamp" rather than as an
// error: it answers a `LoadBalancerNotFound`-shaped refusal for an ARN naming no record, which
// for a resource the deployer just created cannot happen, and if it somehow did there is
// nothing to write. A genuine read failure is still returned.
func cfnStampELBResource(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, tags []EC2Tag,
) (bool, error) {
	if dr.ARN == "" {
		return false, nil
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	res, _, err := elbResolveTaggedResource(state, scope, dr.ARN)
	if err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.ARN, err)
	}
	if res == nil {
		return false, nil
	}

	// A conversion rather than a literal: [EC2Tag] and [ELBTag] happen to have identical
	// fields, which staticcheck notices and which is worth relying on only because both are
	// the same `Key`/`Value` pair AWS publishes for every service's tag.
	incoming := make([]ELBTag, 0, len(tags))
	for _, t := range tags {
		incoming = append(incoming, ELBTag(t))
	}
	updated, err := res.withTags(elbMergeTags(res.tags, incoming))
	if err != nil {
		return true, fmt.Errorf("stamp %s %s: marshal: %w", dr.Type, dr.ARN, err)
	}
	if err := state.Put(context.Background(), elbNamespace, res.stateKey, updated); err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.ARN, err)
	}
	return true, nil
}
