package emulator

import (
	"context"
	"fmt"
	"strings"
)

// The stamp beyond EC2 (#765).
//
// [StackDeployer.stampCFNResourceTags] resolved a physical ID to a state key with
// [ec2TaggableResource], which covers every EC2 type the deployer creates and nothing else.
// So most of a stack was unstamped: an S3 bucket, a DynamoDB table, a Lambda function, an SQS
// queue and every ELBv2 resource carried none of the three `aws:cloudformation:*` keys, and a
// policy or a cost-allocation assertion keyed on the stack name saw nothing on them.
//
// #819 then added the nine types in [cfnRegionalStampKinds], and a further twelve once #835 gave
// their services a [mergeResourceTags] arm — all on the same test: a stamp counts as landed only
// when it reads back through the *owning service's own* tag call.
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
	case "AWS::CloudFront::Distribution":
		// [cfDistKey] carries no Region, because CloudFront is global and the distribution's own
		// ARN has an empty Region field — which is also why the table above cannot hold this one.
		return cfnStampTarget{
			namespace: cloudfrontNamespace,
			stateKey:  cfDistKey(accountID, dr.PhysicalID),
		}, true
	case "AWS::SSM::Parameter":
		// `PutParameter` gives a name its leading "/" before storing it (`ssm_plugin.go:167`), and
		// [ssmParameterStateKey] keeps that slash inside the key — so the record sits at
		// `parameter:<account>/<region>//app/db`, with the doubled separator that function
		// documents. The deployer's physical ID is the template's `Name` unnormalized, so the same
		// two lines the deployer already applies before building the ARN (`cfn_deployer.go:4238`,
		// #827) are what make this key the one `ListTagsForResource` loads.
		name := dr.PhysicalID
		if !strings.HasPrefix(name, "/") {
			name = "/" + name
		}
		return cfnStampTarget{
			namespace: ssmNamespace,
			stateKey:  ssmParameterStateKey(accountID, region, name),
		}, true
	case "AWS::KMS::Key", "AWS::KMS::ReplicaKey":
		// The physical ID is the key *ARN* (`cfn_deployer.go:3973`), while [kmsKeyStateKey] keys on
		// the bare key ID — so the table's shape would build `key:<account>/<region>/arn:aws:kms:…`.
		// A replica key deploys through the same function and only relabels its type (`:4031`).
		return cfnStampARNTarget(dr, kmsResolveARN, cfnStampTarget{
			namespace: kmsNamespace,
			stateKey:  kmsKeyStateKey(accountID, region, dr.PhysicalID),
		})
	case "AWS::SecretsManager::Secret":
		// The physical ID is the secret ARN (`cfn_deployer.go:4075`), and [smSecretStateKey] keys on
		// the name. Recoverable exactly, because substrate mints no random `-xxxxxx` suffix
		// (`secretsmanager_tags.go:43`).
		return cfnStampARNTarget(dr, smResolveARN, cfnStampTarget{
			namespace: secretsManagerNamespace,
			stateKey:  smSecretStateKey(accountID, region, dr.PhysicalID),
		})
	case "AWS::SNS::Topic":
		// The physical ID is the topic ARN (`cfn_deployer.go:4305`), and [snsTopicStateKey] keys on
		// the name.
		return cfnStampARNTarget(dr, snsResolveARN, cfnStampTarget{
			namespace: snsNamespace,
			stateKey:  snsTopicStateKey(accountID, region, dr.PhysicalID),
		})
	case "AWS::ECS::Service", "AWS::ECS::TaskDefinition":
		// Neither can be a table line, which is what #819's own note predicted wrongly:
		// [ecsTagStateKey] builds a *four*-segment key, `service:<account>/<region>/<cluster>/<name>`
		// and `taskdef:<account>/<region>/<family>/<revision>`, and neither the cluster nor the
		// revision appears in the physical ID. Both are in the ARN, which the deployer records
		// (`cfn_resources_v21.go:196`, `:241`), so the ARN is what resolves — through ECS's own
		// function, so the two cannot disagree about which record a task definition names.
		if ns, key, ok := ecsTagStateKey(dr.ARN); ok {
			return cfnStampTarget{namespace: ns, stateKey: key}, true
		}
		// A task definition's physical ID *is* its ARN, so a stored record that carries one
		// without the other still resolves.
		if ns, key, ok := ecsTagStateKey(dr.PhysicalID); ok {
			return cfnStampTarget{namespace: ns, stateKey: key}, true
		}
		return cfnStampTarget{}, false
	default:
		return cfnStampTarget{}, false
	}
}

// cfnStampARNTarget resolves a physical ID that is an ARN through the owning service's own ARN
// resolver, falling back to the given identifier-keyed target when it is not one.
//
// The service's resolver rather than a second parser here, for the reason
// [cfnResolveStampTarget] gives about `resolveARN`: the record the service's own tag call loads is
// the record the stamp has to land in, and the only way the two cannot disagree is to call the
// same function. Each of the three already returns exactly the namespace and key its plugin
// writes — see [kmsResolveARN], [smResolveARN] and [snsResolveARN].
//
// The fallback is not dead code. Each of the three deploy paths leaves the physical ID as the bare
// name or ID when the create response carries no ARN, and
// [StackDeployer.reconcileStackTags] may substitute a `DeployedResource` recorded by an earlier
// run, which for SNS is a shape `cfn_delete.go:77` still normalizes upward today. A fallback key
// that names no record fails loudly through `errTagResourceNotFound` rather than landing a tag
// somewhere unread, which is the failure mode #826 taught this file to avoid.
func cfnStampARNTarget(
	dr DeployedResource,
	resolve func(string) (ns, key string, err error),
	fallback cfnStampTarget,
) (cfnStampTarget, bool) {
	if ns, key, err := resolve(dr.PhysicalID); err == nil {
		return cfnStampTarget{namespace: ns, stateKey: key}, true
	}
	return fallback, true
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
// `<prefix>:<account>/<region>/<physical-id>` — #819's group 3.
//
// They are one table rather than fifteen `switch` arms because they share one key shape: each
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
// machine, an activity, an ECR repository, an ECS cluster, a Kinesis stream, a Glue database, an
// RDS cluster and an RDS subnet group are named by the template (falling back to the logical ID);
// an RDS instance and an ElastiCache cluster carry their own identifier property; and an EFS file
// system and access point take the `fs-`/`fsap-` ID out of the create response, which is what
// EFS's `ListTagsForResource` path segment names them by. A Lambda function and a DynamoDB table
// are named by the template's `FunctionName`/`TableName` (falling back to the logical ID), which is
// what [lambdaFunctionStateKey] and [DynamoDBPlugin.tableStateKey] key on. An ACM certificate is
// the odd one: its physical ID is the certificate ARN, and [acmCertKey]'s last segment is the whole
// ARN by design (`acm_types.go:53`), so the two agree without parsing anything.
//
// The first condition, not a judgement about which service matters, is what held the rest of #819's
// group 3 out until #835 closed: KMS, Secrets Manager, SNS, a Step Functions activity, an RDS
// cluster or subnet group, ACM, CloudFront and SSM all kept tag state that no [mergeResourceTags]
// arm reached, so a stamp had nowhere to land and `TagResources` could not reach them either — one
// defect with two symptoms. Every one of those arms now exists and merges raw JSON behind a
// colon-terminated guard, so the sibling-truncation worry that made `statesNamespace` unsafe for an
// `activity:` key and `rdsNamespace` unsafe for anything but a DB instance is gone too.
//
// **Seven of the twelve are in [cfnResolveStampTarget]'s switch rather than here, and in every case
// because the second condition fails**: the physical ID is not the identifier the plugin keys on.
// A KMS key's, a secret's and a topic's physical ID is an ARN where the key holds a bare ID or name;
// CloudFront's key carries no Region; an SSM parameter's name needs its leading slash; and ECS's
// service and task definition are keyed on four segments, one of which — the cluster, and the
// revision — appears only in the ARN. That last pair corrects a prediction this comment used to
// carry, that they lacked "a table line, not a writer": a table line is exactly what they cannot
// have.
//
// AWS Config is absent here for the same reason ELBv2 is — its tags are not on the record this
// resolves to — and is handled by [cfnStampConfigResource].
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

	// The four #835 unblocked whose physical ID already *is* the identifier the plugin keys on.
	"AWS::StepFunctions::Activity":         {namespace: statesNamespace, prefix: "activity"},
	"AWS::CertificateManager::Certificate": {namespace: acmNamespace, prefix: "cert"},
	"AWS::RDS::DBCluster":                  {namespace: rdsNamespace, prefix: "dbcluster"},
	"AWS::RDS::DBSubnetGroup":              {namespace: rdsNamespace, prefix: "dbsubnetgroup"},

	// SQS joined the table in #1088 rather than keeping its own arm below. Its key was
	// `queue:<account>/<name>`, which is why it needed one; now that [sqsQueueStateKey] carries the
	// Region it is this table's shape exactly, and the arm it replaced was the last place the old
	// two-component form was written by hand. [TaggingPlugin.resolveARN] built `queue:<name>` until
	// #826 and so wrote a record the SQS plugin never reads; both now derive from that one function.
	"AWS::SQS::Queue": {namespace: sqsNamespace, prefix: "queue"},
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

// cfnConfigStampableTypes are the AWS Config CFN types whose tags substrate keeps.
//
// Both are taggable in AWS's own reckoning — `TagResource`'s `ResourceArn` documentation
// enumerates the supported types and both a configuration recorder and a Config rule are among
// them — and both record their ARN, which is what a Config tag is keyed by. The third type the
// deployer creates, `AWS::Config::DeliveryChannel`, is deliberately absent: that same enumeration
// **does not include a delivery channel**, so no `TagResource` call can name one, the
// `DeliveryChannel` shape has no `arn` member to be named by, and substrate's deploy records none.
// The Service Authorization Reference agrees — see `configservice_types.go`.
var cfnConfigStampableTypes = map[string]bool{
	"AWS::Config::ConfigRule":            true,
	"AWS::Config::ConfigurationRecorder": true,
}

// cfnStampResourceTags writes the given tags onto a resource the deployer created, for a
// service substrate models tags for but EC2's resolver does not reach. It reports whether it
// found somewhere to write.
//
// The three arms differ because the three tag stores differ, not by choice: every service
// [cfnResolveStampTarget] covers keeps tags in a record [mergeResourceTags] already knows how
// to merge; ELBv2 keeps an ordered `[]ELBTag` on a record found by scanning for its ARN; and
// AWS Config keeps a side-car record whose whole document *is* the tag map, under a key the
// resource's own record does not contain. All three upsert, as EC2's writer does, so
// re-deploying a stack rewrites the three values rather than accumulating them.
//
// A resource of a type no arm knows reports false and is skipped by the caller in
// silence. That is the documented behavior, not an omission — see the header above.
func cfnStampResourceTags(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, tags []EC2Tag,
) (bool, error) {
	if cfnELBStampableTypes[dr.Type] {
		return cfnStampELBResource(state, reqCtx, dr, tags)
	}
	if cfnConfigStampableTypes[dr.Type] {
		return cfnStampConfigResource(state, reqCtx, dr, tags)
	}

	target, ok := cfnResolveStampTarget(dr, reqCtx.AccountID, reqCtx.Region)
	if !ok {
		return false, nil
	}
	add := make(map[string]string, len(tags))
	for _, t := range tags {
		add[t.Key] = t.Value
	}
	// skipTagQuota: every key here carries the `aws:` prefix, which the services that publish a
	// per-resource tag quota exclude from the count (#1000, and [taggingCheckTagQuota]).
	if err := mergeResourceTags(
		context.Background(), state, target.namespace, target.stateKey, add, nil, skipTagQuota,
	); err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.PhysicalID, err)
	}
	return true, nil
}

// cfnStampELBResource writes the stamp onto one Elastic Load Balancing resource, found by its ARN.
//
// The ARN rather than the physical ID, because that is what ELB's resolver takes and what a
// listener or rule is identified by — [StackDeployer.deployELBListener] sets the physical ID
// *to* the ARN, while a load balancer's and a target group's physical ID is its name and the
// ARN is carried beside it. A resource whose create failed has no ARN and never reaches here.
//
// The resolver is [elbResolveAnyGenerationTaggedResource] rather than ELBv2's own, because a
// template deploys resources and not API generations: `AWS::ElasticLoadBalancing::LoadBalancer`
// names a Classic Load Balancer and `AWS::ElasticLoadBalancingV2::LoadBalancer` an ELBv2 one, and a
// stack's tags reach whichever it declared. ELBv2's `AddTags` refuses a classic ARN because its own
// page says it only tags ELBv2 resources; CloudFormation never made that claim (#844).
//
// **No template reaches the classic half today**, and the resolver is the generation-blind one
// anyway: [cfnELBStampableTypes] lists the four ELBv2 types and the classic type has no deploy
// helper, so a stack declaring one falls through to the generic stub and is not stamped at all. This
// is the rule written where it belongs rather than a path a caller can take, so that the classic
// deploy helper — Tier 2 of #844 — is one entry in that map and not a second tagging decision made
// later under pressure. The live caller of the generation-blind resolver is the Resource Groups
// Tagging API's arm.
//
// The resolver's refusal is treated as "nothing to stamp" rather than as an
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
	res, _, err := elbResolveAnyGenerationTaggedResource(state, scope, dr.ARN)
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
	updated, err := res.encode(elbMergeTags(res.tags, incoming))
	if err != nil {
		return true, fmt.Errorf("stamp %s %s: marshal: %w", dr.Type, dr.ARN, err)
	}
	if err := state.Put(context.Background(), elbNamespace, res.stateKey, updated); err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.ARN, err)
	}
	return true, nil
}

// cfnStampConfigResource writes the stamp onto one AWS Config resource, found by its ARN.
//
// #819's last row, and the one that needed a **writer** rather than a resolver arm. Config keeps
// a resource's tags in a side-car record keyed by ARN whose whole document is the tag map
// (`cfgsvcTagsKey`), which is deliberate — neither AWS's `ConfigurationRecorder` shape nor its
// `ConfigRule` shape has a `Tags` member, so a tag field on either struct would emit a member AWS
// never emits (#836). Three consequences follow, and none of them fits [mergeResourceTags]:
//
//   - There is no tag member to merge *into*. [mergeRecordStringMapTags] and
//     [mergeRecordTagListTags] both look for one inside a record.
//   - The side-car is frequently absent, because a resource created with no tags has none written
//     — so this arm has to **create** the record, where every other arm refuses a missing one
//     through `errTagResourceNotFound`.
//   - The record proving the resource exists is a different key from the one holding its tags, so
//     the absent-record check has to be made against the resource. [cfgsvcResolveStampTags] makes
//     it, through the same lookup `ListTagsForResource` uses rather than a second copy of it.
//
// The ARN rather than the physical ID, because that is what Config keys a tag by: a rule's
// physical ID is its *name* while its ARN names it by a hashed `ConfigRuleId`, and a recorder's
// ARN carries a minted `RecorderId` that no API member holds. Both are read back from the service
// at deploy time (`cfn_resources_v101.go:399`, `:121`) rather than rebuilt here, so an unreadable
// one leaves the ARN empty and this skips rather than guessing.
//
// A resolved-but-absent resource is "nothing to stamp" rather than an error, as ELBv2's arm has
// it: for a resource the deployer just created it cannot happen, and a reconciliation replaying an
// earlier run's resource must not fail a stack over one somebody has since deleted.
func cfnStampConfigResource(
	state StateManager, reqCtx *RequestContext, dr DeployedResource, tags []EC2Tag,
) (bool, error) {
	if dr.ARN == "" {
		return false, nil
	}
	arn, existing, found, err := cfgsvcResolveStampTags(
		state, reqCtx.AccountID, reqCtx.Region, dr.ARN,
	)
	if err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.ARN, err)
	}
	if !found {
		return false, nil
	}

	for _, tag := range tags {
		existing[tag.Key] = tag.Value
	}
	if err := cfgsvcSaveStateTags(context.Background(), state, arn, existing); err != nil {
		return true, fmt.Errorf("stamp %s %s: %w", dr.Type, dr.ARN, err)
	}
	return true, nil
}
