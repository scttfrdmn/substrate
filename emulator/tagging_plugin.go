package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
)

// taggingNamespace is the service name used for state lookups by the tagging plugin.
// The plugin itself does not write to this namespace; it operates on the
// per-service namespaces of the resources it manages.
const taggingNamespace = "tagging"

// TaggingPlugin implements the AWS Resource Groups Tagging API on
// tagging.{region}.amazonaws.com. Operations are identified via
// X-Amz-Target: ResourceGroupsTaggingAPI_20170126.{Operation}.
type TaggingPlugin struct {
	state  StateManager
	logger Logger
}

// Name returns the service name handled by this plugin.
func (p *TaggingPlugin) Name() string { return taggingNamespace }

// Initialize sets up the TaggingPlugin with state and logger.
func (p *TaggingPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	return nil
}

// Shutdown is a no-op for the TaggingPlugin.
func (p *TaggingPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches tagging API operations.
func (p *TaggingPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	target := req.Headers["x-amz-target"]
	const targetPrefix = "ResourceGroupsTaggingAPI_20170126."
	op := strings.TrimPrefix(target, targetPrefix)
	if op == target {
		// x-amz-target header didn't match expected prefix; derive from operation.
		op = req.Operation
	}

	switch op {
	case "GetResources":
		return p.getResources(reqCtx, req)
	case "TagResources":
		return p.tagResources(reqCtx, req)
	case "UntagResources":
		return p.untagResources(reqCtx, req)
	default:
		return nil, unknownActionError(p.Name(), op)
	}
}

// ----- GetResources --------------------------------------------------------

// getResourcesInput and the members it decodes live in `tagging_get_resources.go` (#1004, #1010),
// which is also where the published constraints and the readings behind them are recorded.

type tagFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

type resourceTagMapping struct {
	ResourceARN string       `json:"ResourceARN"`
	Tags        []taggingTag `json:"Tags"`

	// ComplianceDetails is reported only when the caller sets IncludeComplianceDetails. It was
	// `*struct{}` until #1010 — a shape that could render only as absent or as `{}`, never as the
	// published four members. See [taggingComplianceDetails] for which of the four substrate
	// answers and why `ComplianceStatus` is not among them.
	ComplianceDetails *taggingComplianceDetails `json:"ComplianceDetails,omitempty"`

	// everTagged is the scanned record's previously-tagged flag. It is unexported because it is not a
	// response member: AWS reports the history by including the resource with an empty tag set, not by
	// publishing a flag. See [taggingResourceReported] (#938).
	everTagged bool
}

type taggingTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// getResourcesOutput is the GetResources response.
//
// `PaginationToken` carries no `omitempty` since #1010: AWS's own Sample Response for this operation
// emits `"PaginationToken": ""` on a complete result, and the prose tells a caller to repeat the
// query "until you receive a null value" — which a caller cannot do if the member is absent on the
// last page. That is one sample rather than a citation, so it is recorded as substrate's reading in
// `tagging_get_resources.go`.
type getResourcesOutput struct {
	ResourceTagMappingList []resourceTagMapping `json:"ResourceTagMappingList"`
	PaginationToken        string               `json:"PaginationToken"`
}

// getResources answers the tagged and previously-tagged resources in the caller's account and
// Region, honoring all eight of the operation's published request members.
//
// Every refusal, every published bound and every reading substrate makes where the page is silent is
// recorded in `tagging_get_resources.go`, which also states which of the request members' constraints
// this function deliberately does not enforce. The validation runs before the scan, so a request AWS
// would refuse costs no state read.
func (p *TaggingPlugin) getResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in getResourcesInput
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &in); err != nil {
			return nil, taggingInvalidParameter("invalid JSON body")
		}
	}
	perPage, offset, awsErr := taggingValidateGetResources(&in)
	if awsErr != nil {
		return nil, awsErr
	}

	all, err := p.scanAllResources(reqCtx)
	if err != nil {
		return nil, err
	}

	// Sort by ARN for stable, deterministic pagination. This is the ordering the offset in a
	// PaginationToken is an offset into, so it has to happen before any cut.
	sort.Slice(all, func(i, j int) bool {
		return all[i].ResourceARN < all[j].ResourceARN
	})

	// Filter by ResourceARNList. Mutually exclusive with both filters below, so the three arms
	// cannot combine — the refusal for that is in taggingValidateARNListExclusions.
	if len(in.ResourceARNList) > 0 {
		all = taggingFilterByARNList(all, in.ResourceARNList)
	}

	// Filter by ResourceTypeFilters.
	if len(in.ResourceTypeFilters) > 0 {
		filtered := all[:0]
		for _, rm := range all {
			if resourceTypeMatches(rm.ResourceARN, in.ResourceTypeFilters) {
				filtered = append(filtered, rm)
			}
		}
		all = filtered
	}

	// Filter by TagFilters.
	if len(in.TagFilters) > 0 {
		filtered := all[:0]
		for _, rm := range all {
			if tagFiltersMatch(rm.Tags, in.TagFilters) {
				filtered = append(filtered, rm)
			}
		}
		all = filtered
	}

	page, nextToken := pageByOffsetToken(all, offset, perPage)

	// TagsPerPage cuts the page a second time, by tag count rather than resource count, and reissues
	// the token at the shorter boundary. Both members apply when both are sent, the page breaking at
	// whichever limit is reached first; nothing here runs when the member is absent, so a caller that
	// never sends it pages exactly as before.
	if in.TagsPerPage != nil {
		if kept := taggingTagBudget(page, *in.TagsPerPage); kept < len(page) {
			page = page[:kept]
			nextToken = encodeOffsetPaginationToken(offset + kept)
		}
	}

	// ComplianceDetails is attached only when asked for, and ExcludeCompliantResources removes
	// nothing — see [taggingNoEffectiveTagPolicy] for why no resource is evaluated as compliant.
	if in.IncludeComplianceDetails {
		for i := range page {
			page[i].ComplianceDetails = taggingNoEffectiveTagPolicy()
		}
	}

	out := getResourcesOutput{
		ResourceTagMappingList: page,
		PaginationToken:        nextToken,
	}
	return taggingJSONResponse(http.StatusOK, out)
}

// resourceTypeMatches returns true if the resource ARN matches any of the
// requested resource type filter strings (prefix match on the ARN service/type).
func resourceTypeMatches(arn string, filters []string) bool {
	for _, f := range filters {
		if matchesResourceType(arn, f) {
			return true
		}
	}
	return false
}

// matchesResourceType checks whether an ARN's service/type matches a filter
// like "s3", "ec2:instance", "lambda:function", etc.
func matchesResourceType(arn, filter string) bool {
	// ARN format: arn:aws:{service}:{region}:{acct}:{resourceType}/{id}
	// or:        arn:aws:{service}:::{bucket}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 {
		return false
	}
	svc := parts[2]
	resource := parts[5] // e.g. "instance/i-abc123", "table/mytable", "my-bucket"

	filterParts := strings.SplitN(filter, ":", 2)
	if filterParts[0] != svc {
		return false
	}
	if len(filterParts) == 1 {
		// Service-only filter (e.g. "s3") — match all resources of that service.
		return true
	}
	// Service:type filter (e.g. "ec2:instance") — the type must be the ARN's own type segment.
	return arnResourceTypeSegment(resource) == filterParts[1]
}

// arnResourceTypeSegment returns the resource-type segment of an ARN's resource portion, or the
// whole portion when it embeds no type.
//
// AWS states that a ResourceTypeFilters entry of "ec2:instance" "returns only EC2 instances", and
// that "[t]he string for each service name and resource type is the same as that embedded in a
// resource's Amazon Resource Name (ARN)" (GetResources). Matching an unanchored prefix instead — as
// this did before #936 — broke the first sentence in both directions: "ecs:task" selected a
// task-definition/… ARN, and "apigateway:restapis" selected nothing at all, because API Gateway's
// resource portion begins with a slash.
//
// So the type is delimited by the ARN itself, which is #910's anchored-segment rule applied to the
// one comparison whose left-hand side comes from the caller. Four shapes occur across the services
// substrate scans, and one leading slash is stripped before the segment is taken:
//
//	instance/i-abc          -> instance
//	stateMachine:hello      -> stateMachine
//	task-definition/fam:3   -> task-definition
//	/restapis/abc123        -> restapis
//	my-bucket               -> my-bucket    (S3 embeds no type)
//
// The last row is why the whole portion is returned rather than the empty string: a service whose
// ARNs carry no type can still be named by a service-only filter, and returning "" here would make
// an empty type string match it.
func arnResourceTypeSegment(resource string) string {
	resource = strings.TrimPrefix(resource, "/")
	if i := strings.IndexAny(resource, "/:"); i >= 0 {
		return resource[:i]
	}
	return resource
}

// tagFiltersMatch returns true when all tag filters match the resource's tags.
// Within a filter, any value in Values matches (OR); across filters it is AND.
func tagFiltersMatch(tags []taggingTag, filters []tagFilter) bool {
	tagMap := make(map[string]string, len(tags))
	for _, t := range tags {
		tagMap[t.Key] = t.Value
	}
	for _, f := range filters {
		val, ok := tagMap[f.Key]
		if !ok {
			return false
		}
		if len(f.Values) > 0 {
			matched := false
			for _, fv := range f.Values {
				if val == fv {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	return true
}

// ----- Resource scanning ---------------------------------------------------

// resourceDescriptor holds the resource type label and the scan function
// for a class of resource.
type resourceDescriptor struct {
	// typePrefix is the service name used in ARNs (e.g. "s3", "ec2").
	typePrefix string
	scan       func(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error)
}

// taggingScanPrefix returns the state-key prefix that selects one resource type in the caller's
// account and Region.
//
// Every scanner whose owning service builds an account- and Region-qualified key goes through it, so
// no two can disagree about scope and none has to remember to include the Region. Forgetting is the
// documented failure: scanECSClusters prefixed by account alone, so a caller in us-west-2 was
// reported a us-east-1 cluster while the same caller's services — keyed identically — were
// Region-scoped. One service answering a caller two different ways about one namespace is worse than
// either answer, and GetResources' own opening sentence settles which is right: it "[r]eturns all
// the tagged or previously tagged resources that are located in the specified AWS Region for the
// account".
//
// Four scanners deliberately do not use it, and each for a reason the key or the ARN gives:
// DynamoDB's key carries no Region, Lambda's and S3's carry neither, and CloudFront's ARNs are
// global. IAM's are global in the Region axis alone, so it stays account-only. What holds the scope
// for all of them is [taggingResourceInScope], which is the guarantee; this prefix is the narrowing
// (#937).
func taggingScanPrefix(typePrefix string, reqCtx *RequestContext) string {
	return typePrefix + reqCtx.AccountID + "/" + reqCtx.Region + "/"
}

// taggingResourceInScope reports whether a scanned resource's own ARN places it in the caller's
// account and Region.
//
// GetResources "[r]eturns all the tagged or previously tagged resources that are located in the
// specified AWS Region for the account", so the scan is scoped by where a resource *is* — and an ARN
// is where AWS states that. Reading the scope from the reported ARN rather than from each scanner's
// state-key prefix is what makes it one rule instead of twenty-nine: a scanner whose key carries no
// Region cannot express the scope in a prefix at all, and one that can has to remember to. This is
// the read-side form of the rule #826 through #932 established for the write side, where every
// resolver takes account and Region from the ARN and never from the caller.
//
// An empty account or Region segment means the ARN states no such scope, and such a resource is in
// scope everywhere. IAM depends on that: its ARNs carry no Region, so an IAM user is reported to a
// caller in any Region, which is what a global service's resource should do. It is also why this
// filter does not replace CloudFront's own us-east-1 gate — CloudFront ARNs are Region-less too, but
// AWS publishes its tagging through that one Region, which is a narrower rule than this one can
// express.
//
// An ARN that does not parse is left in scope rather than dropped. A scan that silently discarded a
// resource because substrate had built a malformed ARN for it would hide the ARN defect behind a
// missing row, which is the opposite of what #827 settled: report what is there rather than
// swallowing it.
func taggingResourceInScope(arn string, reqCtx *RequestContext) bool {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return true
	}
	region, account := parts[3], parts[4]
	if region != "" && region != reqCtx.Region {
		return false
	}
	return account == "" || account == reqCtx.AccountID
}

func (p *TaggingPlugin) scanAllResources(reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	descriptors := []resourceDescriptor{
		{typePrefix: "s3", scan: p.scanS3Buckets},
		{typePrefix: "lambda", scan: p.scanLambdaFunctions},
		{typePrefix: "sqs", scan: p.scanSQSQueues},
		{typePrefix: "dynamodb", scan: p.scanDynamoDBTables},
		{typePrefix: "ec2", scan: p.scanEC2Instances},
		{typePrefix: "iam", scan: p.scanIAMEntities},
		{typePrefix: "apigateway", scan: p.scanAPIGatewayAPIs},
		{typePrefix: "states", scan: p.scanStepFunctionsStateMachines},
		{typePrefix: "states", scan: p.scanStepFunctionsActivities},
		{typePrefix: "ecr", scan: p.scanECRRepositories},
		{typePrefix: "ecs", scan: p.scanECSClusters},
		{typePrefix: "ecs", scan: p.scanECSServices},
		{typePrefix: "ecs", scan: p.scanECSTasks},
		{typePrefix: "ecs", scan: p.scanECSTaskDefinitions},
		{typePrefix: "cognito-idp", scan: p.scanCognitoUserPools},
		{typePrefix: "kinesis", scan: p.scanKinesisStreams},
		{typePrefix: "rds", scan: p.scanRDSInstances},
		{typePrefix: "rds", scan: p.scanRDSClusters},
		{typePrefix: "rds", scan: p.scanRDSSubnetGroups},
		{typePrefix: "elasticache", scan: p.scanElastiCacheClusters},
		{typePrefix: "elasticfilesystem", scan: p.scanEFSFileSystems},
		{typePrefix: "glue", scan: p.scanGlueDatabases},
		{typePrefix: "acm", scan: p.scanACMCertificates},
		{typePrefix: "cloudfront", scan: p.scanCloudFrontDistributions},
		{typePrefix: "kms", scan: p.scanKMSKeys},
		{typePrefix: "sns", scan: p.scanSNSTopics},
		{typePrefix: "secretsmanager", scan: p.scanSecretsManagerSecrets},
		{typePrefix: "ssm", scan: p.scanSSMParameters},
		{typePrefix: "elasticloadbalancing", scan: p.scanELBKind(elbKindLoadBalancer)},
		{typePrefix: "elasticloadbalancing", scan: p.scanELBKind(elbKindTargetGroup)},
		{typePrefix: "elasticloadbalancing", scan: p.scanELBKind(elbKindListener)},
		{typePrefix: "elasticloadbalancing", scan: p.scanELBKind(elbKindRule)},
		{typePrefix: "elasticloadbalancing", scan: p.scanELBKind(elbKindClassicLB)},
	}

	var all []resourceTagMapping
	for _, d := range descriptors {
		resources, err := d.scan(goCtx, reqCtx)
		if err != nil {
			p.logger.Warn("tagging: scan error", "type", d.typePrefix, "err", err)
			continue
		}
		// Scope is decided here rather than in each scanner, so a scanner that cannot narrow its
		// state-key prefix — or forgets to — still cannot report another account's or Region's
		// resource. See [taggingResourceInScope].
		for _, r := range resources {
			if !taggingResourceInScope(r.ResourceARN, reqCtx) {
				continue
			}
			// Never-tagged resources are dropped here rather than in each scanner, for the reason
			// the scope check is: it is one rule instead of twenty-nine, and it is inside the scan,
			// so a page cut at ResourcesPerPage=1 is not under-filled by a filter applied after the
			// cut. See [taggingResourceReported] (#938).
			if !taggingResourceReported(r) {
				continue
			}
			// A previously-tagged resource is reported with "Tags": [], which is what AWS publishes
			// and what its own sample response shows. The tag converters return nil for an empty
			// set, and resourceTagMapping.Tags carries no omitempty, so without this the member
			// would render as null — a shape no reference publishes, and one an SDK decodes into a
			// nil slice indistinguishable from an absent member.
			if r.Tags == nil {
				r.Tags = []taggingTag{}
			}
			all = append(all, r)
		}
	}
	return all, nil
}

// scanS3Buckets lists the caller's buckets.
//
// S3 is the one service whose scope [taggingResourceInScope] cannot decide, because an S3 bucket ARN
// is arn:aws:s3:::{name} — it carries neither an account nor a Region segment, S3's bucket namespace
// being global. So the scope comes off the record instead: [S3Bucket] stores the Region it was
// created in, and #937 added the account that created it. The key cannot carry either, since a bucket
// name is globally unique and the key is what makes that true here as well.
//
// An empty Region or AccountID is in scope, on the same rule [taggingResourceInScope] applies to an
// absent ARN segment: a record that states no scope is not evidence of a scope to exclude it from,
// and a bucket written by a path that does not set the field would otherwise disappear from
// GetResources rather than be reported with what is known about it.
func (p *TaggingPlugin) scanS3Buckets(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, s3Namespace, "bucket:")
	if err != nil {
		return nil, fmt.Errorf("list s3 buckets: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, s3Namespace, k)
		if err != nil || raw == nil {
			continue
		}
		var b S3Bucket
		if err := json.Unmarshal(raw, &b); err != nil {
			continue
		}
		if b.Region != "" && b.Region != reqCtx.Region {
			continue
		}
		if b.AccountID != "" && b.AccountID != reqCtx.AccountID {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: s3BucketARN(b.Name),
			Tags:        mapToTaggingTags(b.Tags),
			everTagged:  b.EverTagged,
		})
	}
	return out, nil
}

// scanLambdaFunctions lists the caller's functions.
//
// Narrowed by both halves of the scope, because since #943 [lambdaFunctionStateKey] carries both:
// the key was "function:{name}" until then and could not tell two callers' functions of one name
// apart at all, so the scan had to narrow by nothing and rely on [taggingResourceInScope] reading
// the scope back off the function's own ARN. That check still runs — it is what makes a
// ResourceTypeFilter and a malformed record answer alike — but the scan no longer loads another
// account's or Region's record to reject it.
func (p *TaggingPlugin) scanLambdaFunctions(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, lambdaNamespace,
		lambdaFunctionKeyPrefix(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("list lambda functions: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, lambdaNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var fn LambdaFunction
		if err := json.Unmarshal(raw, &fn); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: fn.FunctionArn,
			Tags:        mapToTaggingTags(fn.Tags),
			everTagged:  fn.EverTagged,
		})
	}
	return out, nil
}

// scanSQSQueues lists the caller's queues.
//
// Account- and Region-qualified since #1088, which put the Region in [sqsQueueStateKey]; until then
// the key was account and name alone and the Region half of the scope came from the queue's own ARN
// through [taggingResourceInScope] (#937). That filter still runs and is still what a cross-Region
// `ResourceARNList` is answered by, so the prefix is a narrowing rather than the whole rule — but it
// is the narrowing that stops this scan unmarshalling every other Region's record to discard it.
func (p *TaggingPlugin) scanSQSQueues(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, sqsNamespace, sqsQueueKeyPrefix(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("list sqs queues: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, sqsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var q SQSQueue
		if err := json.Unmarshal(raw, &q); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: q.QueueARN,
			Tags:        mapToTaggingTags(q.Tags),
			everTagged:  q.EverTagged,
		})
	}
	return out, nil
}

// scanDynamoDBTables lists the caller's tables.
//
// Qualified by both account and Region, since #943 gave [DynamoDBPlugin.tableStateKey] the Region
// it had been missing — it was account-qualified only, the one regional key that could not express
// the whole scope, and the Region half came from the table's own ARN through
// [taggingResourceInScope] (#937). That check still runs; the scan simply no longer loads another
// Region's record in order to reject it.
func (p *TaggingPlugin) scanDynamoDBTables(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "table:" + reqCtx.AccountID + "/" + reqCtx.Region + "/"
	keys, err := p.state.List(goCtx, dynamodbNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list dynamodb tables: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, dynamodbNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var t DynamoDBTable
		if err := json.Unmarshal(raw, &t); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: t.TableARN,
			Tags:        mapToTaggingTags(t.Tags),
			everTagged:  t.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanEC2Instances(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("instance:", reqCtx)
	keys, err := p.state.List(goCtx, ec2Namespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list ec2 instances: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ec2Namespace, k)
		if err != nil || raw == nil {
			continue
		}
		var inst EC2Instance
		if err := json.Unmarshal(raw, &inst); err != nil {
			continue
		}
		arn := "arn:aws:ec2:" + inst.Region + ":" + inst.AccountID + ":instance/" + inst.InstanceID
		out = append(out, resourceTagMapping{
			ResourceARN: arn,
			Tags:        ec2TagsToTaggingTags(inst.Tags),
			everTagged:  inst.EverTagged,
		})
	}
	return out, nil
}

// scanIAMEntities returns the users and roles of the caller's own account.
//
// The two scans are per-account since #737: an IAM key carries the account it
// belongs to, so a resource listing made by one account no longer reports another's
// entities. Nothing filters index keys out of either sweep because nothing has to —
// "user_policies:" and its siblings do not begin with "user:", so the prefix has
// always excluded them, and the guard that claimed to do it never fired.
func (p *TaggingPlugin) scanIAMEntities(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	var out []resourceTagMapping

	userKeys, err := p.state.List(goCtx, iamNamespace, iamUserPrefix(reqCtx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("list iam users: %w", err)
	}
	for _, k := range userKeys {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var u IAMUser
		if err := json.Unmarshal(raw, &u); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: u.ARN,
			Tags:        iamTagsToTaggingTags(u.Tags),
			everTagged:  u.EverTagged,
		})
	}

	roleKeys, err := p.state.List(goCtx, iamNamespace, iamRolePrefix(reqCtx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("list iam roles: %w", err)
	}
	for _, k := range roleKeys {
		raw, err := p.state.Get(goCtx, iamNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var r IAMRole
		if err := json.Unmarshal(raw, &r); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: r.ARN,
			Tags:        iamTagsToTaggingTags(r.Tags),
			everTagged:  r.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanAPIGatewayAPIs(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("api:", reqCtx)
	keys, err := p.state.List(goCtx, apigatewayNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list apigateway apis: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, apigatewayNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var api RestAPIState
		if err := json.Unmarshal(raw, &api); err != nil {
			continue
		}
		arn := "arn:aws:apigateway:" + api.Region + "::/restapis/" + api.ID
		out = append(out, resourceTagMapping{
			ResourceARN: arn,
			Tags:        mapToTaggingTags(api.Tags),
			everTagged:  api.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanStepFunctionsStateMachines(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(sfnStateMachineKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, statesNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list stepfunctions state machines: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, statesNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var sm StateMachineState
		if err := json.Unmarshal(raw, &sm); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: sm.StateMachineArn,
			Tags:        mapToTaggingTags(sm.Tags),
			everTagged:  sm.EverTagged,
		})
	}
	return out, nil
}

// scanStepFunctionsActivities reports the activities GetResources can discover.
//
// Without it an activity was taggable by name once the resolver gained its arm, but invisible to
// a caller discovering resources — the two halves of #835's criterion per resource type.
func (p *TaggingPlugin) scanStepFunctionsActivities(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(sfnActivityKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, statesNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list stepfunctions activities: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, statesNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var act ActivityState
		if err := json.Unmarshal(raw, &act); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: act.ActivityArn,
			Tags:        mapToTaggingTags(act.Tags),
			everTagged:  act.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanECRRepositories(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("ecrrepo:", reqCtx)
	keys, err := p.state.List(goCtx, ecrNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list ecr repositories: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ecrNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var repo ECRRepository
		if err := json.Unmarshal(raw, &repo); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: repo.RepositoryArn,
			Tags:        mapToTaggingTags(repo.Tags),
			everTagged:  repo.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanECSClusters(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, ecsNamespace, taggingScanPrefix(ecsClusterKeyPrefix, reqCtx))
	if err != nil {
		return nil, fmt.Errorf("list ecs clusters: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ecsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var cluster ECSCluster
		if err := json.Unmarshal(raw, &cluster); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: cluster.ClusterArn,
			Tags:        ecsTagsToTaggingTags(cluster.Tags),
			everTagged:  cluster.EverTagged,
		})
	}
	return out, nil
}

// scanECSServices reports every ECS service in the caller's account and Region, with its tags.
//
// Without it a service was taggable through TagResources — mergeResourceTags' ecs arm has reached
// one since #765, and ecsTagStateKey resolves its ARN — and invisible to a caller discovering
// resources. That asymmetry is what each row of #835 closes: a tag the tagging API writes and the
// tagging API cannot find is a tag no consumer can audit.
func (p *TaggingPlugin) scanECSServices(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, ecsNamespace, taggingScanPrefix(ecsServiceKeyPrefix, reqCtx))
	if err != nil {
		return nil, fmt.Errorf("list ecs services: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ecsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var svc ECSService
		if err := json.Unmarshal(raw, &svc); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: svc.ServiceArn,
			Tags:        ecsTagsToTaggingTags(svc.Tags),
			everTagged:  svc.EverTagged,
		})
	}
	return out, nil
}

// scanECSTasks reports every ECS task in the caller's account and Region, with its tags.
//
// A task is not one of the two types #835's table names, and it is in scope with them anyway:
// ecsTagStateKey already resolves a task ARN, so the tagging API can write a tag there today, and
// AWS lists tasks first among the taggable ECS resources — "There are multiple ways that Amazon ECS
// tasks, services, task definitions, and clusters are tagged" (Tagging Amazon ECS resources). A row
// that left it out would leave the same asymmetry the row exists to close.
func (p *TaggingPlugin) scanECSTasks(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, ecsNamespace, taggingScanPrefix(ecsTaskKeyPrefix, reqCtx))
	if err != nil {
		return nil, fmt.Errorf("list ecs tasks: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ecsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var task ECSTask
		if err := json.Unmarshal(raw, &task); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: task.TaskArn,
			Tags:        ecsTagsToTaggingTags(task.Tags),
			everTagged:  task.EverTagged,
		})
	}
	return out, nil
}

// scanECSTaskDefinitions reports every ECS task-definition revision in the caller's account and
// Region, with its tags.
//
// Every revision is reported, not just the family's newest, because each is a resource with its own
// ARN and its own tags: RegisterTaskDefinition accepts tags per revision, and ecsTagStateKey splits
// a "{family}:{revision}" ARN to reach exactly one of them. Reporting only the newest would hide a
// tag the tagging API itself had written.
//
// A revision whose status is INACTIVE is reported too. AWS's tagging page says nothing about
// deregistration, and a deregistered revision keeps its ARN and remains describable, so omitting it
// would be substrate inventing a rule; the honest reading is that a resource which still answers
// DescribeTaskDefinition is still a resource.
func (p *TaggingPlugin) scanECSTaskDefinitions(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, ecsNamespace, taggingScanPrefix(ecsTaskDefKeyPrefix, reqCtx))
	if err != nil {
		return nil, fmt.Errorf("list ecs task definitions: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ecsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var td ECSTaskDefinition
		if err := json.Unmarshal(raw, &td); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: td.TaskDefinitionArn,
			Tags:        ecsTagsToTaggingTags(td.Tags),
			everTagged:  td.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanCognitoUserPools(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("userpool:", reqCtx)
	keys, err := p.state.List(goCtx, cognitoIDPNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list cognito user pools: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, cognitoIDPNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var pool CognitoUserPool
		if err := json.Unmarshal(raw, &pool); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: pool.Arn,
			Tags:        mapToTaggingTags(pool.Tags),
			everTagged:  pool.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanKinesisStreams(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("stream:", reqCtx)
	keys, err := p.state.List(goCtx, kinesisNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list kinesis streams: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, kinesisNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var stream KinesisStream
		if err := json.Unmarshal(raw, &stream); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: stream.StreamArn,
			Tags:        mapToTaggingTags(stream.Tags),
			everTagged:  stream.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanRDSInstances(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(rdsDBInstanceKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list rds instances: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, rdsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var inst RDSDBInstance
		if err := json.Unmarshal(raw, &inst); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: inst.DBInstanceArn,
			Tags:        mapToTaggingTags(inst.Tags),
			everTagged:  inst.EverTagged,
		})
	}
	return out, nil
}

// scanRDSClusters reports every Aurora DB cluster in the caller's account.
//
// A sibling of [TaggingPlugin.scanRDSInstances] rather than a branch inside it, because the two
// records are different shapes with the ARN in a different member, and one function decoding both
// is how the merge arm above came to truncate a cluster in the first place.
func (p *TaggingPlugin) scanRDSClusters(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(rdsDBClusterKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list rds clusters: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, rdsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var cluster RDSDBCluster
		if err := json.Unmarshal(raw, &cluster); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: cluster.DBClusterArn,
			Tags:        mapToTaggingTags(cluster.Tags),
			everTagged:  cluster.EverTagged,
		})
	}
	return out, nil
}

// scanRDSSubnetGroups reports every DB subnet group in the caller's account.
func (p *TaggingPlugin) scanRDSSubnetGroups(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(rdsDBSubnetGroupKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list rds subnet groups: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, rdsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var group RDSDBSubnetGroup
		if err := json.Unmarshal(raw, &group); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: group.DBSubnetGroupArn,
			Tags:        mapToTaggingTags(group.Tags),
			everTagged:  group.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanElastiCacheClusters(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("cachecluster:", reqCtx)
	keys, err := p.state.List(goCtx, elasticacheNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list elasticache clusters: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, elasticacheNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var cluster ElastiCacheCacheCluster
		if err := json.Unmarshal(raw, &cluster); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: cluster.CacheClusterARN,
			Tags:        mapToTaggingTags(cluster.Tags),
			everTagged:  cluster.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanEFSFileSystems(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("filesystem:", reqCtx)
	keys, err := p.state.List(goCtx, efsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list efs filesystems: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, efsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var fs EFSFileSystem
		if err := json.Unmarshal(raw, &fs); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: fs.FileSystemArn,
			Tags:        efsTagsToTaggingTags(fs.Tags),
			everTagged:  fs.EverTagged,
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanGlueDatabases(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix("database:", reqCtx)
	keys, err := p.state.List(goCtx, glueNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list glue databases: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, glueNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var db GlueDatabase
		if err := json.Unmarshal(raw, &db); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: db.Arn,
			Tags:        mapToTaggingTags(db.Tags),
			everTagged:  db.EverTagged,
		})
	}
	return out, nil
}

// scanACMCertificates reports every ACM certificate in the caller's account and Region.
//
// The prefix is account- *and* Region-qualified because [acmCertKey] is: a certificate is a
// regional resource, and GetResources is a per-Region operation. The index key ("cert_arns:") is
// not matched by this prefix and so is never decoded as a certificate.
// scanKMSKeys reports every KMS key in the caller's account and Region.
//
// The prefix is built from [kmsKeyKeyPrefix] rather than a literal, so the scan and KMS's own writers
// cannot fall out of step about where a key record lives — the drift #918 found in CloudFront's four
// inline key literals.
//
// A key pending deletion is still reported. AWS's tagging page says you may not *tag* such a key, but
// GetResources is a read and the key exists until the waiting period elapses; suppressing it here
// would hide a resource whose ARN DescribeKey still resolves. Refusing the write is a separate,
// unmodelled behavior (KMSInvalidStateException) noted on #922 rather than guessed at.
func (p *TaggingPlugin) scanKMSKeys(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(kmsKeyKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, kmsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list kms keys: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, kmsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var key KMSKey
		if err := json.Unmarshal(raw, &key); err != nil {
			continue
		}
		tags := make([]taggingTag, 0, len(key.Tags))
		for _, t := range key.Tags {
			tags = append(tags, taggingTag{Key: t.TagKey, Value: t.TagValue})
		}
		sortTagsByKey(tags, func(t taggingTag) string { return t.Key })
		out = append(out, resourceTagMapping{
			ResourceARN: key.ARN,
			Tags:        tags,
			everTagged:  key.EverTagged,
		})
	}
	return out, nil
}

// scanSNSTopics reports every SNS topic in the caller's account and Region, with its tags.
//
// The prefix is built from [snsTopicKeyPrefix] — the same constant [snsTopicStateKey] writes and
// [snsKeyIsTaggable] tests — so the scanner, the resolver and SNS's own operations cannot fall out of
// step about where a topic record lives.
//
// The colon in the prefix is load-bearing here as it is for KMS: this namespace also holds
// "topic_names:{acct}/{region}", whose value is a JSON array of names, and a bare "topic" prefix would
// list it, fail to unmarshal it into an [SNSTopic] and skip it silently — reporting nothing rather
// than reporting wrongly, but for a reason no reader could see.
func (p *TaggingPlugin) scanSNSTopics(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(snsTopicKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, snsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list sns topics: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, snsNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var topic SNSTopic
		if err := json.Unmarshal(raw, &topic); err != nil {
			continue
		}
		tags := make([]taggingTag, 0, len(topic.Tags))
		for _, t := range topic.Tags {
			tags = append(tags, taggingTag(t))
		}
		sortTagsByKey(tags, func(t taggingTag) string { return t.Key })
		out = append(out, resourceTagMapping{
			ResourceARN: topic.ARN,
			Tags:        tags,
			everTagged:  topic.EverTagged,
		})
	}
	return out, nil
}

// scanSecretsManagerSecrets reports every Secrets Manager secret in the caller's account and Region,
// with its tags.
//
// The prefix is built from [smSecretKeyPrefix] — the same constant [smSecretStateKey] writes and
// [smKeyIsTaggable] tests — so the scanner, the resolver and Secrets Manager's own operations cannot
// fall out of step about where a secret record lives.
//
// The colon in the prefix is load-bearing, as it is for KMS and SNS: this namespace also holds
// "secret_names:{acct}/{region}", a JSON array of names, and "secret_version:{...}", whose value is a
// raw secret payload and not JSON. A bare "secret" prefix would list both, fail to unmarshal them
// into a [SecretState] and skip them silently — and listing a version key at all would be worse than
// reporting nothing, because it is the one key in the tree whose value is a caller's secret.
func (p *TaggingPlugin) scanSecretsManagerSecrets(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(smSecretKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, secretsManagerNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list secretsmanager secrets: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, secretsManagerNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var secret SecretState
		if err := json.Unmarshal(raw, &secret); err != nil {
			continue
		}
		tags := make([]taggingTag, 0, len(secret.Tags))
		for _, t := range secret.Tags {
			tags = append(tags, taggingTag(t))
		}
		sortTagsByKey(tags, func(t taggingTag) string { return t.Key })
		out = append(out, resourceTagMapping{
			ResourceARN: secret.ARN,
			Tags:        tags,
			everTagged:  secret.EverTagged,
		})
	}
	return out, nil
}

// scanSSMParameters reports every Systems Manager parameter in the caller's account and Region, with
// its tags.
//
// The prefix is built from [ssmParameterKeyPrefix] — the same constant [ssmParameterStateKey] writes
// and [ssmKeyIsTaggable] tests — so the scanner, the resolver and Systems Manager's own operations
// cannot fall out of step about where a parameter record lives.
//
// The colon in the prefix is load-bearing, as it is for KMS, SNS and Secrets Manager: this namespace
// also holds "parameter_paths:{acct}/{region}", a JSON array of names, which a bare "parameter"
// prefix would list and then skip silently when it failed to unmarshal into an [SSMParameter].
func (p *TaggingPlugin) scanSSMParameters(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(ssmParameterKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, ssmNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list ssm parameters: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, ssmNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var param SSMParameter
		if err := json.Unmarshal(raw, &param); err != nil {
			continue
		}
		tags := make([]taggingTag, 0, len(param.Tags))
		for _, t := range param.Tags {
			tags = append(tags, taggingTag(t))
		}
		sortTagsByKey(tags, func(t taggingTag) string { return t.Key })
		out = append(out, resourceTagMapping{
			ResourceARN: param.ARN,
			Tags:        tags,
			everTagged:  param.EverTagged,
		})
	}
	return out, nil
}

// scanELBKind returns the scanner for one of ELB's five taggable kinds — ELBv2's four and the
// Classic Load Balancer #844 added.
//
// One parameterized scanner rather than five, because ELB is the one namespace whose kinds share a
// decoder: [elbDecodeTaggedResource] already reads a classic load balancer, an ELBv2 one, a target
// group, a listener or a rule and reports the ARN, the tags and the previously-tagged flag from each.
// Reusing it is the read-side half of what the resolver arm does by reusing
// [elbResolveAnyGenerationTaggedResource] — the scan reports a resource under exactly the ARN the
// resolver will accept for it, so #765's cross-readability holds by construction rather than by two
// parsers agreeing.
//
// The prefix goes through [taggingScanPrefix] over [elbKindKeyPrefix], so the scan is narrowed by
// the same account/Region-qualified prefix ELB's own writers build and every other scanner is
// narrowed by (#937). A record whose stored ARN puts it elsewhere is still dropped by
// [taggingResourceInScope].
//
// A record that does not decode is skipped rather than reported. The five kinds live under five
// distinct prefixes, so the only way that happens is a corrupt record, and reporting a resource
// with no ARN would put an empty string in the response (#863).
func (p *TaggingPlugin) scanELBKind(kind string) func(context.Context, *RequestContext) ([]resourceTagMapping, error) {
	return func(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
		goCtx := context.Background()
		prefix := taggingScanPrefix(elbKindKeyPrefix(kind), reqCtx)
		keys, err := p.state.List(goCtx, elbNamespace, prefix)
		if err != nil {
			return nil, fmt.Errorf("list elb %s: %w", kind, err)
		}
		var out []resourceTagMapping
		for _, k := range keys {
			raw, getErr := p.state.Get(goCtx, elbNamespace, k)
			if getErr != nil || raw == nil {
				continue
			}
			res := elbDecodeTaggedResource(kind, k, raw)
			if res == nil {
				continue
			}
			tags := make([]taggingTag, 0, len(res.tags))
			for _, t := range res.tags {
				tags = append(tags, taggingTag(t))
			}
			sortTagsByKey(tags, func(t taggingTag) string { return t.Key })
			out = append(out, resourceTagMapping{
				ResourceARN: res.arn,
				Tags:        tags,
				everTagged:  res.everTagged,
			})
		}
		return out, nil
	}
}

func (p *TaggingPlugin) scanACMCertificates(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := taggingScanPrefix(acmCertKeyPrefix, reqCtx)
	keys, err := p.state.List(goCtx, acmNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list acm certificates: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, acmNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var cert ACMCertificate
		if err := json.Unmarshal(raw, &cert); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: cert.CertificateArn,
			Tags:        mapToTaggingTags(cert.Tags),
			everTagged:  cert.EverTagged,
		})
	}
	return out, nil
}

// scanCloudFrontDistributions reports every CloudFront distribution in the caller's account, and
// only when the request names the Region AWS attributes a global resource to.
//
// The prefix carries no Region because [cfDistKey] carries none — CloudFront is global and its
// ARNs have an empty Region segment. But GetResources is a per-Region operation, so a global
// resource still has to be attributed to one Region or it would be reported by every Region's
// call. AWS attributes it to us-east-1: the Resource Groups user guide's supported-resources table
// marks AWS::CloudFront::Distribution "Yes" with the footnote that a global service's resources
// are found by selecting the us-east-1 Region, and TagResources states that "you can only tag
// resources that are located in the specified AWS Region for the AWS account".
//
// Which Region that is, is AWS's; that the gate exists at all is substrate's reading, because AWS
// publishes the Tag Editor footnote and not a GetResources rule. CloudFront's own
// ListDistributions keeps answering from any Region, which is not an inconsistency — it is what a
// global service does, and the per-Region attribution belongs to the tagging API alone.
func (p *TaggingPlugin) scanCloudFrontDistributions(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	if reqCtx.Region != cfGlobalRegion {
		return nil, nil
	}
	goCtx := context.Background()
	prefix := cfDistKeyPrefix + reqCtx.AccountID + "/"
	keys, err := p.state.List(goCtx, cloudfrontNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("list cloudfront distributions: %w", err)
	}
	var out []resourceTagMapping
	for _, k := range keys {
		raw, err := p.state.Get(goCtx, cloudfrontNamespace, k)
		if err != nil || raw == nil {
			continue
		}
		var dist CloudFrontDistribution
		if err := json.Unmarshal(raw, &dist); err != nil {
			continue
		}
		out = append(out, resourceTagMapping{
			ResourceARN: dist.ARN,
			Tags:        mapToTaggingTags(dist.Tags),
			everTagged:  dist.EverTagged,
		})
	}
	return out, nil
}

// ----- TagResources --------------------------------------------------------

type tagResourcesInput struct {
	ResourceARNList []string          `json:"ResourceARNList"`
	Tags            map[string]string `json:"Tags"`
}

type failedResourcesInfo struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
	StatusCode   int    `json:"StatusCode"`
}

type tagResourcesOutput struct {
	FailedResourcesMap map[string]failedResourcesInfo `json:"FailedResourcesMap,omitempty"`
}

// tagResources implements TagResources. It takes no account or region from the request
// context: every resource is addressed by the ARN the caller named, through
// [TaggingPlugin.resolveARN], which is what keeps a cross-account ARN out of the caller's own
// resources.
func (p *TaggingPlugin) tagResources(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in tagResourcesInput
	if err := json.Unmarshal(req.Body, &in); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	failures := make(map[string]failedResourcesInfo)
	goCtx := context.Background()

	for _, arn := range in.ResourceARNList {
		ns, key, err := p.resolveARN(arn)
		if err != nil {
			failures[arn] = p.tagResolveFailure(arn, err)
			continue
		}
		if err := p.mergeTags(goCtx, ns, key, in.Tags, nil); err != nil {
			failures[arn] = p.tagMergeFailure(arn, err)
		}
	}

	out := tagResourcesOutput{}
	if len(failures) > 0 {
		out.FailedResourcesMap = failures
	}
	return taggingJSONResponse(http.StatusOK, out)
}

// ----- UntagResources ------------------------------------------------------

type untagResourcesInput struct {
	ResourceARNList []string `json:"ResourceARNList"`
	TagKeys         []string `json:"TagKeys"`
}

type untagResourcesOutput struct {
	FailedResourcesMap map[string]failedResourcesInfo `json:"FailedResourcesMap,omitempty"`
}

// untagResources implements UntagResources, addressing resources exactly as
// [TaggingPlugin.tagResources] does so that the two cannot diverge on which resource an ARN
// names — a removal aimed at the wrong resource is the more damaging direction.
func (p *TaggingPlugin) untagResources(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in untagResourcesInput
	if err := json.Unmarshal(req.Body, &in); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	failures := make(map[string]failedResourcesInfo)
	goCtx := context.Background()

	for _, arn := range in.ResourceARNList {
		ns, key, err := p.resolveARN(arn)
		if err != nil {
			failures[arn] = p.tagResolveFailure(arn, err)
			continue
		}
		if err := p.mergeTags(goCtx, ns, key, nil, in.TagKeys); err != nil {
			failures[arn] = p.tagMergeFailure(arn, err)
		}
	}

	out := untagResourcesOutput{}
	if len(failures) > 0 {
		out.FailedResourcesMap = failures
	}
	return taggingJSONResponse(http.StatusOK, out)
}

// ----- ARN resolver --------------------------------------------------------

// unsupportedTagResourceError reports an ARN that is well formed but names a resource type
// the tagging API does not handle. It exists to keep that case distinguishable from a
// malformed ARN, because the two answer different error codes.
//
// [FailureInfo] documents InternalServiceException as covering the case where "the resource
// type in the request is not supported by the Resource Groups Tagging API", and tells the
// caller "it's safe to retry the request and then call GetResources to verify the changes".
// InvalidParameterException is documented for a different set of causes, of which the one
// that fits here is "a provided string parameter is malformed".
//
// The split is substrate's reading, not AWS's: the same InvalidParameterException list also
// says "the target ID is invalid, unsupported, or doesn't exist", so both codes can be read
// to cover an unsupported type. Substrate splits them on whether the ARN parses at all,
// which is the only distinction a caller can act on differently — a malformed ARN is a bug
// in the request, an unsupported type is a gap in the emulator.
type unsupportedTagResourceError struct {
	// detail names the offending resource portion. It reaches the log, not the response.
	detail string
}

// Error implements error.
func (e *unsupportedTagResourceError) Error() string {
	return "unsupported resource type for tagging: " + e.detail
}

// unsupportedTagResource builds an [unsupportedTagResourceError] for a well-formed ARN whose
// resource type has no arm in [TaggingPlugin.resolveARN].
func unsupportedTagResource(format string, args ...any) error {
	return &unsupportedTagResourceError{detail: fmt.Sprintf(format, args...)}
}

// tagResolveFailure maps a [TaggingPlugin.resolveARN] error onto the FailureInfo a caller
// sees, keeping both handlers on one mapping so TagResources and UntagResources cannot drift.
//
// The message is deliberately not err.Error() for the unsupported case and never the merge
// error's text: those carry substrate's internal state-key layout, which is not something an
// API response should publish. The detail goes to the log instead.
func (p *TaggingPlugin) tagResolveFailure(arn string, err error) failedResourcesInfo {
	// An ELB ARN is resolved by finding the record it names rather than by building a key, so
	// "no such resource" can surface from the resolver instead of from the merge (#863). It is the
	// same failure and gets the same answer, which is why the mapping is delegated rather than
	// duplicated: a caller must not be able to tell which stage discovered it.
	if errors.Is(err, errTagResourceNotFound) {
		return p.tagMergeFailure(arn, err)
	}
	var unsupported *unsupportedTagResourceError
	if errors.As(err, &unsupported) {
		p.logger.Warn("tagging API cannot resolve a resource type",
			"arn", arn, "detail", unsupported.detail)
		return failedResourcesInfo{
			ErrorCode:    "InternalServiceException",
			ErrorMessage: "the resource type in the request is not supported by the Resource Groups Tagging API",
			StatusCode:   http.StatusInternalServerError,
		}
	}
	return failedResourcesInfo{
		ErrorCode:    "InvalidParameterException",
		ErrorMessage: err.Error(),
		StatusCode:   http.StatusBadRequest,
	}
}

// tagMergeFailure maps a tag-merge error onto a FailureInfo. The error's own text names the
// state key it failed at, so it is logged rather than returned.
//
// The two codes split on whose failure it is. An ARN that resolved and addressed no record is
// [errTagResourceNotFound], and that is InvalidParameterException at HTTP 400: TagResources and
// UntagResources both list "[t]he target ID is invalid, unsupported, or doesn't exist" among that
// code's causes, and a resource that is not there is exactly the third of the three. Every other
// merge error — a stored record that will not unmarshal, a Get or a Put that failed — is
// InternalServiceException at HTTP 500, whose published gloss is that the request "failed because
// of an internal error" and that it is "safe to retry the request and then call GetResources to
// verify the changes". Answering 500 for both, as substrate did before #939, told a caller to
// retry a request that could only fail again, and left "substrate cannot tag this type" and "this
// resource does not exist" indistinguishable.
//
// AWS publishes a contradiction about this field: FailureInfo's ErrorCode carries "Valid Values:
// InternalServiceException | InvalidParameterException" while the same member's prose says it "can
// also include any valid error code returned by the AWS service that hosts the resource that the ARN
// key represents", offering AccessDeniedException — which is neither of the two — as its example.
// Substrate reads the enumeration for every failure it can express that way, because the enumeration
// is the part a caller can switch on, and the prose for the one it cannot: a per-resource tag quota
// (#1000). Neither enumerated code says "this resource is at its tag limit", and the four services
// that publish a quota publish four different codes at two different statuses, so reporting the
// owning service's own is the only answer that tells a caller what happened and matches what its own
// tagging operation would have said.
func (p *TaggingPlugin) tagMergeFailure(arn string, err error) failedResourcesInfo {
	var quota *taggingQuotaError
	if errors.As(err, &quota) {
		p.logger.Warn("tagging API refused a merge that would exceed a service's tag quota",
			"arn", arn, "code", quota.awsErr.Code, "error", err)
		return failedResourcesInfo{
			ErrorCode:    quota.awsErr.Code,
			ErrorMessage: quota.awsErr.Message,
			StatusCode:   quota.awsErr.HTTPStatus,
		}
	}
	if errors.Is(err, errTagResourceNotFound) {
		p.logger.Warn("tagging API found no resource at the ARN", "arn", arn, "error", err)
		return failedResourcesInfo{
			ErrorCode:    "InvalidParameterException",
			ErrorMessage: "the target ID is invalid, unsupported, or doesn't exist",
			StatusCode:   http.StatusBadRequest,
		}
	}
	p.logger.Error("tagging API failed to merge tags", "arn", arn, "error", err)
	return failedResourcesInfo{
		ErrorCode:    "InternalServiceException",
		ErrorMessage: "the request failed because of an internal error; retry the request and then call GetResources to verify the changes",
		StatusCode:   http.StatusInternalServerError,
	}
}

// resolveARN parses an ARN and returns the (namespace, stateKey) for the resource.
//
// Every arm checks the resource-type prefix it strips before stripping it. strings.TrimPrefix
// returns its input unchanged when the prefix does not match, so an arm that strips without
// checking builds a well-formed key for the *wrong kind of resource* rather than failing —
// which is how a tag landed on a state machine when an activity ARN was passed, and how a
// layer ARN resolved to a function key (#845, and the resolver half of #835).
//
// Most arms build the state key from the ARN, and one — elasticloadbalancing — finds it, because
// ELB's listener and rule keys are minted suffixes an ARN carries in one of its two shapes only.
// That arm reads through [elbResolveTaggedResource] and so through p.state; every arm still takes
// its account and Region from the ARN, which is the property below.
//
// The account and region come from the ARN, and the resolver deliberately takes no
// [RequestContext] so that no arm can reach for the caller's account instead. An ARN naming
// another account must resolve that account's resource or none: resolving it against the
// caller's account tags a same-named resource the caller never named, which is the defect
// #826 fixed for SQS and this pass fixed for DynamoDB. AWS does not publish what a
// foreign-account ARN does — TagResources says only that "you can only tag resources that are
// located in the specified AWS Region for the AWS account", and an explicit refusal is
// documented for a partition mismatch but not for an account mismatch — so refusing is
// substrate's reading, applied uniformly rather than per arm.
//
// Returns an error if the ARN format is unrecognized.
func (p *TaggingPlugin) resolveARN(arn string) (ns, key string, err error) {
	// ARN format: arn:aws:{service}:{region}:{account}:{resource}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return "", "", fmt.Errorf("invalid ARN: %q", arn)
	}
	svc := parts[2]
	resource := parts[5] // e.g. "instance/i-abc", "table/mytable", "my-bucket"

	switch svc {
	case "s3":
		// arn:aws:s3:::bucket-name — the resource portion is the bare bucket name, with no
		// type prefix to strip. An object, access point or job ARN carries a "/" and is not
		// a bucket, which is the only distinction this ARN shape offers.
		if strings.Contains(resource, "/") {
			return "", "", unsupportedTagResource("S3 %q is not a bucket ARN", resource)
		}
		return s3Namespace, "bucket:" + resource, nil

	case "lambda":
		// arn:aws:lambda:{region}:{acct}:function:{name}
		//
		// A qualified ARN is refused rather than resolved to the unqualified function. AWS
		// documents a version as function:{name}:{version} and an alias as
		// function:{name}:{alias}, which are lexically identical — only whether the suffix is
		// numeric tells them apart, and AWS does not resolve the ambiguity. Tags belong to
		// the function, so there is nothing a qualified ARN could correctly address here.
		//
		// The account and Region come from the ARN rather than from the caller's request
		// context, as they do for the SQS, IAM and EC2 arms. Until #943 neither was in the key
		// at all, so an ARN naming another account's or another Region's function resolved to
		// the caller's own same-named one and a TagResources against it wrote there — the
		// #826 defect, in the arm whose key shape hid it.
		name, ok := strings.CutPrefix(resource, "function:")
		if !ok || strings.Contains(name, ":") {
			return "", "", unsupportedTagResource("Lambda %q is not an unqualified function ARN", resource)
		}
		return lambdaNamespace, lambdaFunctionStateKey(parts[4], parts[3], name), nil

	case "sqs":
		// arn:aws:sqs:{region}:{acct}:{name}
		//
		// The key is account-qualified because dropping the account addressed a key no queue is
		// ever stored at, so a TagResources against a real queue wrote a phantom record and
		// answered 200 (#826); it is Region-qualified since #1088, which is the same defect one
		// component over — an ARN naming another Region's queue resolved to the caller's own
		// same-named one. Both components come from the ARN rather than from the caller's request
		// context, as they do for the Lambda, DynamoDB, IAM and EC2 arms: an ARN naming another
		// account or Region must resolve that queue or none.
		return sqsNamespace, sqsQueueStateKey(parts[4], parts[3], resource), nil

	case "dynamodb":
		// arn:aws:dynamodb:{region}:{acct}:table/{name}
		//
		// A remaining "/" means the ARN names something nested under the table rather than the
		// table: AWS documents an index as table/{name}/index/{index} and a stream as
		// table/{name}/stream/{label}, both sharing this prefix.
		//
		// The account and Region both come from the ARN. Taking the account from the caller's
		// request context meant an ARN naming another account's table tagged the caller's own
		// same-named table, and UntagResources stripped tags from it — the defect #826 fixed for
		// the SQS arm above, in the one arm that pass missed. The resolver no longer receives a
		// request context at all, so no arm can reach for the caller's account again. The Region
		// was not in the key until #943, so until then the same substitution happened across
		// Regions within one account and nothing here could prevent it.
		name, ok := strings.CutPrefix(resource, "table/")
		if !ok || strings.Contains(name, "/") {
			return "", "", unsupportedTagResource("DynamoDB %q is not a table ARN", resource)
		}
		return dynamodbNamespace, "table:" + parts[4] + "/" + parts[3] + "/" + name, nil

	case "ec2":
		// arn:aws:ec2:{region}:{acct}:instance/{id}
		if strings.HasPrefix(resource, "instance/") {
			id := strings.TrimPrefix(resource, "instance/")
			region := parts[3]
			acct := parts[4]
			return ec2Namespace, "instance:" + acct + "/" + region + "/" + id, nil
		}
		return "", "", unsupportedTagResource("EC2 %q is not a taggable resource type", resource)

	case "iam":
		// The account comes from the ARN, as it does for every other arm here: an IAM
		// key carries the account it belongs to since #737, and the ARN is what names it.
		acct := parts[4]
		if strings.HasPrefix(resource, "user/") {
			name := strings.TrimPrefix(resource, "user/")
			return iamNamespace, iamUserKey(acct, name), nil
		}
		if strings.HasPrefix(resource, "role/") {
			name := strings.TrimPrefix(resource, "role/")
			return iamNamespace, iamRoleKey(acct, name), nil
		}
		return "", "", unsupportedTagResource("IAM %q is not a taggable resource type", resource)

	case "apigateway":
		// arn:aws:apigateway:{region}::/restapis/{apiId} — a v1 REST API. An HTTP or WebSocket
		// API is /apis/{id}, which is a different resource with its own state key.
		apiID, ok := strings.CutPrefix(resource, "/restapis/")
		if !ok || strings.Contains(apiID, "/") {
			return "", "", unsupportedTagResource("API Gateway %q is not a REST API ARN", resource)
		}
		region := parts[3]
		acct := parts[4]
		return apigatewayNamespace, "api:" + acct + "/" + region + "/" + apiID, nil

	case "states":
		// arn:aws:states:{region}:{acct}:stateMachine:{name} or :activity:{name}
		//
		// Through [sfnResolveARN], which is what Step Functions' own three tagging operations
		// key through, so the two sides cannot disagree about which record an ARN names. It also
		// carries the activity arm this one lacked, which is what made an activity unreachable
		// from here (part of #835), and the case-sensitivity of the "stateMachine" segment, which
		// AWS distinguishes from "activity" by the literal segment alone.
		ns, key, resolveErr := sfnResolveARN(arn)
		if resolveErr != nil {
			return "", "", unsupportedTagResource("Step Functions %q is not a taggable ARN: %v", resource, resolveErr)
		}
		return ns, key, nil

	case "ecr":
		// arn:aws:ecr:{region}:{acct}:repository/{name}
		name, ok := strings.CutPrefix(resource, "repository/")
		if !ok {
			return "", "", unsupportedTagResource("ECR %q is not a repository ARN", resource)
		}
		region := parts[3]
		acct := parts[4]
		return ecrNamespace, "ecrrepo:" + acct + "/" + region + "/" + name, nil

	case "ecs":
		// A cluster, service, task or task definition, keyed by [ecsTagStateKey] — the same
		// function ECS's own TagResource keys through, so the tagging API and the owning service
		// cannot disagree about where one resource's tags live. Building the key here by hand
		// meant this arm reached a cluster only, which is why a service and a task definition
		// appear in #835.
		if ns, key, ok := ecsTagStateKey(arn); ok {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("ECS %q is not a taggable resource type", resource)

	case "cognito-idp":
		// arn:aws:cognito-idp:{region}:{acct}:userpool/{poolId}
		poolID, ok := strings.CutPrefix(resource, "userpool/")
		if !ok || strings.Contains(poolID, "/") {
			return "", "", unsupportedTagResource("Cognito %q is not a user pool ARN", resource)
		}
		region := parts[3]
		acct := parts[4]
		return cognitoIDPNamespace, "userpool:" + acct + "/" + region + "/" + poolID, nil

	case "kinesis":
		// Through [kinesisParseStreamARN] and [kinesisStreamKey] rather than a parse and a key built
		// here, so Kinesis's own fifteen ARN-accepting operations and this one cannot disagree about
		// which stream an ARN names or where its tags live — the arrangement #826 warns about and the
		// one the ecs and rds arms already follow. Before #966 the shared parser did not exist,
		// because Kinesis decoded no StreamARN anywhere.
		target, arnErr := kinesisParseStreamARN(arn)
		if arnErr != nil {
			return "", "", unsupportedTagResource("Kinesis %q is not a stream ARN", resource)
		}
		return kinesisNamespace, kinesisStreamKey(target.AccountID, target.Region, target.Name), nil

	case "rds":
		// Through [rdsResolveARN] rather than a key built here, so RDS's own three tag
		// operations and this one cannot disagree about where a resource's tags live — the
		// arrangement #826 warns about and the one the ecs arm above already follows. It is also
		// what lets this arm reach a DB cluster, a snapshot and a subnet group at all: it
		// recognized `db:` only, while RDS mints `cluster:` and `subgrp:` ARNs of its own
		// (part of #835).
		if ns, key, resolveErr := rdsResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("RDS %q is not a taggable resource type", resource)

	case "elasticache":
		// arn:aws:elasticache:{region}:{acct}:cluster:{id}
		if strings.HasPrefix(resource, "cluster:") {
			id := strings.TrimPrefix(resource, "cluster:")
			region := parts[3]
			acct := parts[4]
			return elasticacheNamespace, "cachecluster:" + acct + "/" + region + "/" + id, nil
		}
		return "", "", unsupportedTagResource("ElastiCache %q is not a taggable resource type", resource)

	case "elasticfilesystem":
		// arn:aws:elasticfilesystem:{region}:{acct}:file-system/{id}
		region := parts[3]
		acct := parts[4]
		if strings.HasPrefix(resource, "file-system/") {
			id := strings.TrimPrefix(resource, "file-system/")
			return efsNamespace, "filesystem:" + acct + "/" + region + "/" + id, nil
		}
		if strings.HasPrefix(resource, "access-point/") {
			id := strings.TrimPrefix(resource, "access-point/")
			return efsNamespace, "accesspoint:" + acct + "/" + region + "/" + id, nil
		}
		return "", "", unsupportedTagResource("EFS %q is not a taggable resource type", resource)

	case "glue":
		// arn:aws:glue:{region}:{acct}:{type}/{name}
		region := parts[3]
		acct := parts[4]
		if strings.HasPrefix(resource, "database/") {
			name := strings.TrimPrefix(resource, "database/")
			return glueNamespace, "database:" + acct + "/" + region + "/" + name, nil
		}
		if strings.HasPrefix(resource, "job/") {
			name := strings.TrimPrefix(resource, "job/")
			return glueNamespace, "job:" + acct + "/" + region + "/" + name, nil
		}
		if strings.HasPrefix(resource, "crawler/") {
			name := strings.TrimPrefix(resource, "crawler/")
			return glueNamespace, "crawler:" + acct + "/" + region + "/" + name, nil
		}
		if strings.HasPrefix(resource, "connection/") {
			name := strings.TrimPrefix(resource, "connection/")
			return glueNamespace, "connection:" + acct + "/" + region + "/" + name, nil
		}
		return "", "", unsupportedTagResource("Glue %q is not a taggable resource type", resource)

	case "acm":
		// arn:aws:acm:{region}:{acct}:certificate/{id}
		//
		// Through [acmResolveARN], which builds the key with [acmCertKey] — the same builder ACM's
		// own three tag operations use, so the two sides cannot disagree about where one
		// certificate's tags live. AWS scopes those three operations to this one resource type in
		// prose ("This action applies only to the certificate resource type"), and substrate models
		// no other ACM resource, so an ACME-endpoint ARN is refused here rather than resolved
		// (part of #835).
		if ns, key, resolveErr := acmResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("ACM %q is not a taggable resource type", resource)

	case "cloudfront":
		// arn:aws:cloudfront::{acct}:distribution/{id} — no Region segment, because CloudFront is
		// global.
		//
		// Through [cfResolveARN], which wraps the parser CloudFront's own three tagging operations
		// key through, so an ARN addresses the same distribution from either arm. That parser is
		// also what refuses every other CloudFront resource type by name: the developer guide's
		// "You can tag distributions, but you can't tag origin access identities or invalidations"
		// is the boundary, and substrate stores invalidations in the same namespace (part of #835,
		// on top of #918).
		if ns, key, resolveErr := cfResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("CloudFront %q is not a taggable resource type", resource)

	case "kms":
		// arn:aws:kms:{region}:{acct}:key/{id}
		//
		// Through [kmsResolveARN], which shares [kmsKeyStateKey] with KMS's own three tag operations,
		// so a key's tags are at one address whichever arm writes them. That parser is also what
		// refuses an alias ARN: the developer guide states "You cannot tag aliases, custom key stores,
		// AWS managed keys, AWS owned keys, or KMS keys in other AWS accounts", and an alias is the
		// one of those five substrate stores in this namespace, keyed by a name whose own "/" made
		// the previous last-component scan resolve alias/aws/s3 to "s3" (part of #835, on top of the
		// resolution fix in kms_tags.go).
		if ns, key, resolveErr := kmsResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("KMS %q is not a taggable resource type", resource)

	case "sns":
		// arn:aws:sns:{region}:{acct}:{topic-name}
		//
		// Through [snsResolveARN], which shares [snsTopicStateKey] with SNS's own three tag
		// operations, so a topic's tags are at one address whichever arm writes them. SNS's ARN
		// carries no type keyword and no separator — the resource portion *is* the name — so the
		// discriminator the shape offers is the colon: a subscription ARN appends ":{sub-id}", and
		// SNS publishes no tagging for subscriptions, so that parser refuses it rather than resolving
		// it to the topic it names or to its own identifier as a topic (part of #835, on top of the
		// resolution fix in sns_tags.go).
		if ns, key, resolveErr := snsResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("SNS %q is not a taggable resource type", resource)

	case "secretsmanager":
		// arn:aws:secretsmanager:{region}:{acct}:secret:{name}
		//
		// Through [smResolveARN], which shares [smSecretStateKey] with Secrets Manager's own
		// TagResource and UntagResource, so a secret's tags are at one address whichever arm writes
		// them. Unlike SNS this shape does have a type keyword to anchor a match against, and
		// [smParseSecretARN] matches "secret" as a whole colon-delimited segment per #910 rather than
		// as a prefix. The ARN-only form is deliberate: Secrets Manager's own operations accept a bare
		// name for SecretId, but the tagging API's parameter is ResourceARNList and a name is not an
		// ARN, so there is nothing here for a caller's own account to supply (part of #835, on top of
		// the resolution fix in secretsmanager_tags.go).
		if ns, key, resolveErr := smResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("Secrets Manager %q is not a taggable resource type", resource)

	case "ssm":
		// arn:aws:ssm:{region}:{acct}:parameter{name} — the name's own leading "/" is the separator,
		// which [ssmParameterARN] relies on and [ssmParseParameterARN] inverts exactly.
		//
		// Through [ssmResolveARN], which shares [ssmParameterStateKey] with Systems Manager's own
		// AddTagsToResource and RemoveTagsFromResource, so a parameter's tags are at one address
		// whichever arm writes them. Systems Manager's own operations do not take an ARN at all — AWS
		// documents a Parameter ResourceId as the parameter *name* — so this is the one arm of #835
		// where the two sides take different identifier forms and still have to agree; sharing the key
		// builder is what makes them (part of #835, on top of the resolution fix in ssm_tags.go).
		//
		// The type segment is matched whole per #910: this namespace also addresses document/,
		// servicesetting/, opsmetadata/, maintenancewindow/, patchbaseline/ and managed-instance/
		// resources, none of which store tags here.
		if ns, key, resolveErr := ssmResolveARN(arn); resolveErr == nil {
			return ns, key, nil
		}
		return "", "", unsupportedTagResource("Systems Manager %q is not a taggable resource type", resource)

	case "elasticloadbalancing":
		// A Classic Load Balancer, or an ELBv2 load balancer, target group, listener or listener
		// rule, found by scanning for the record whose stored ARN equals this one — through
		// [elbResolveAnyGenerationTaggedResource], which shares its body with the resolver ELBv2's
		// own AddTags, RemoveTags and DescribeTags use.
		//
		// This is the one arm that reads state, and the reason is structural rather than a
		// shortcut. Every other arm *builds* a key from the ARN; a listener's key is
		// `listener:{acct}/{region}/{suffix}` and a rule's is `rule:{acct}/{region}/{suffix}`,
		// where the suffix is minted at create time and appears in the ARN only as its last
		// segment in the flat shape #774 mints — not in the nested shape an earlier version
		// recorded and [elbResourceKindFromARN] still resolves. Deriving the key would therefore
		// have to be shape-aware, and would be a second answer to a question ELB already answers
		// once. Sharing the resolver is what the other arms get from sharing a key builder: the two
		// sides cannot disagree about which record an ARN names, and they cannot disagree about the
		// refusal either.
		//
		// The scope comes from the ARN's own account and Region, never from the caller, per #826
		// and its successors ([elbARNScope]).
		//
		// The generation is deliberately *not* part of what this arm refuses, which is the one
		// place it differs from ELBv2's own tag doors. The Resource Groups Tagging API's documented
		// matching rule reads the resource type out of the ARN, and both ELB generations spell that
		// segment `loadbalancer` — so `ResourceTypeFilters=elasticloadbalancing:loadbalancer`
		// names both, and an ARN of either is a resource this API tags. AWS's own Service
		// Authorization Reference agrees from the other side: it lists the classic `loadbalancer`
		// resource under the single `AddTags` action, because one IAM action spans both APIs. Hence
		// [elbAnyGenerationKindFromARN] here and [elbResourceKindFromARN] there (#844).
		//
		// The two refusals are deliberately different failures. An ARN that names no ELB resource
		// *type* at all is unsupported, which is what the twenty-two other arms answer for a type
		// they do not reach. A well-formed ARN of a kind substrate does model, naming nothing, is
		// [errTagResourceNotFound], so it answers the same InvalidParameterException a
		// resolved-but-absent resource answers from [TaggingPlugin.tagMergeFailure] — the ARN
		// resolved as far as this arm can take it, and the resource is simply not there (#863).
		scope, ok := elbARNScope(arn)
		if !ok {
			return "", "", unsupportedTagResource("ELB %q is not an ARN", arn)
		}
		if elbAnyGenerationKindFromARN(arn) == "" {
			return "", "", unsupportedTagResource("ELB %q is not a taggable resource type", resource)
		}
		res, awsErr, readErr := elbResolveAnyGenerationTaggedResource(p.state, scope, arn)
		if readErr != nil {
			return "", "", fmt.Errorf("resolve ELB %q: %w", arn, readErr)
		}
		if awsErr != nil {
			return "", "", fmt.Errorf("%w: %s answers %s", errTagResourceNotFound, arn, awsErr.Code)
		}
		return elbNamespace, res.stateKey, nil

	default:
		return "", "", unsupportedTagResource("service %q has no tagging arm", svc)
	}
}

// mergeTags loads the resource at ns/key, applies addTags (merge) and removes
// removeKeys, then persists the updated resource.
//
// A thin wrapper over [mergeResourceTags], which holds the behavior so the tagging plugin
// and the CloudFormation deployer cannot drift into two merge semantics.
//
// This is the path that enforces the owning service's published per-resource tag quota, where the
// deployer's stamp does not; see the comment on [taggingCheckTagQuota] for why the two differ
// (#1000).
func (p *TaggingPlugin) mergeTags(goCtx context.Context, ns, key string, addTags map[string]string, removeKeys []string) error {
	return mergeResourceTags(goCtx, p.state, ns, key, addTags, removeKeys, enforceTagQuota)
}

// errTagResourceNotFound reports that ns/key addressed no record, so there was nothing for a tag
// to merge into.
//
// It is a sentinel rather than a plain error string because the tagging API answers it with a
// different FailureInfo code from every other merge failure, and the caller that needs to tell
// them apart — [TaggingPlugin.tagMergeFailure] — is not the caller that produces it. The error's
// text still names the state key, which is why that mapping logs it instead of returning it (#939).
var errTagResourceNotFound = errors.New("no resource at the resolved state key")

// mergeResourceTags loads the resource at ns/key, applies addTags (merge) and removes
// removeKeys, then persists the updated resource.
//
// For S3 / Lambda / SQS / DynamoDB, tags are map[string]string.
// For IAM and EC2, tags are []IAMTag / []EC2Tag.
//
// A free function taking the state rather than a method, for the reason given on
// [ec2ApplyTagsToResource]: the CloudFormation deployer writes the `aws:cloudformation:*`
// stamp through here ([#765](https://github.com/scttfrdmn/substrate/issues/765)) and holds no
// [TaggingPlugin]. Sharing the writer is the point — a tag the Resource Groups Tagging API
// would write and one the deployer writes land in the same place, in the same shape, so a
// consumer reading either back through the owning service's own call sees one behavior.
//
// The default arm and the per-namespace key fallbacks below stay even though
// [TaggingPlugin.resolveARN] can no longer reach them (#845). Two of the three callers are
// CloudFormation paths that build ns/key themselves rather than from an ARN
// ([cfnStampResourceTags], [cfnPropagateRecordStackTags]), so for those the arms are the only
// thing between a stamp aimed at an unhandled namespace and a silent success — the defect
// class #845 exists to remove, not to relocate.
// quota says whether the owning service's published per-resource tag quota is enforced. It is a
// parameter rather than always-on because the CloudFormation stamp path writes `aws:`-prefixed keys,
// which the services that publish a quota either exclude from the count or never receive from a
// caller at all; [taggingCheckTagQuota] records the reasoning (#1000).
func mergeResourceTags(
	goCtx context.Context, state StateManager, ns, key string,
	addTags map[string]string, removeKeys []string, quota tagQuotaMode,
) error {
	raw, err := state.Get(goCtx, ns, key)
	if err != nil {
		return fmt.Errorf("get resource: %w", err)
	}
	if raw == nil {
		return fmt.Errorf("%w: %s/%s", errTagResourceNotFound, ns, key)
	}

	// Before the switch, so a refused request writes nothing — the rule #965 established for the
	// service-side quotas this reuses.
	if quota == enforceTagQuota {
		if quotaErr := taggingCheckTagQuota(ns, key, raw, addTags, removeKeys); quotaErr != nil {
			return quotaErr
		}
	}

	switch ns {
	case s3Namespace:
		var b S3Bucket
		if err := json.Unmarshal(raw, &b); err != nil {
			return fmt.Errorf("unmarshal S3Bucket: %w", err)
		}
		b.EverTagged = taggingEverTagged(b.EverTagged, len(b.Tags), len(addTags))
		b.Tags = mergeStringMap(b.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(b)
		return state.Put(goCtx, ns, key, updated)

	case lambdaNamespace:
		var fn LambdaFunction
		if err := json.Unmarshal(raw, &fn); err != nil {
			return fmt.Errorf("unmarshal LambdaFunction: %w", err)
		}
		fn.EverTagged = taggingEverTagged(fn.EverTagged, len(fn.Tags), len(addTags))
		fn.Tags = mergeStringMap(fn.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(fn)
		return state.Put(goCtx, ns, key, updated)

	case sqsNamespace:
		var q SQSQueue
		if err := json.Unmarshal(raw, &q); err != nil {
			return fmt.Errorf("unmarshal SQSQueue: %w", err)
		}
		q.EverTagged = taggingEverTagged(q.EverTagged, len(q.Tags), len(addTags))
		q.Tags = mergeStringMap(q.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(q)
		return state.Put(goCtx, ns, key, updated)

	case dynamodbNamespace:
		var t DynamoDBTable
		if err := json.Unmarshal(raw, &t); err != nil {
			return fmt.Errorf("unmarshal DynamoDBTable: %w", err)
		}
		t.EverTagged = taggingEverTagged(t.EverTagged, len(t.Tags), len(addTags))
		t.Tags = mergeStringMap(t.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(t)
		return state.Put(goCtx, ns, key, updated)

	case ec2Namespace:
		var inst EC2Instance
		if err := json.Unmarshal(raw, &inst); err != nil {
			return fmt.Errorf("unmarshal EC2Instance: %w", err)
		}
		inst.EverTagged = taggingEverTagged(inst.EverTagged, len(inst.Tags), len(addTags))
		inst.Tags = mergeEC2Tags(inst.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(inst)
		return state.Put(goCtx, ns, key, updated)

	case iamNamespace:
		if strings.HasPrefix(key, "user:") {
			var u IAMUser
			if err := json.Unmarshal(raw, &u); err != nil {
				return fmt.Errorf("unmarshal IAMUser: %w", err)
			}
			u.EverTagged = taggingEverTagged(u.EverTagged, len(u.Tags), len(addTags))
			u.Tags = mergeIAMTags(u.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(u)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "role:") {
			var r IAMRole
			if err := json.Unmarshal(raw, &r); err != nil {
				return fmt.Errorf("unmarshal IAMRole: %w", err)
			}
			r.EverTagged = taggingEverTagged(r.EverTagged, len(r.Tags), len(addTags))
			r.Tags = mergeIAMTags(r.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(r)
			return state.Put(goCtx, ns, key, updated)
		}
		return fmt.Errorf("unsupported IAM resource key: %s", key)

	case apigatewayNamespace:
		var api RestAPIState
		if err := json.Unmarshal(raw, &api); err != nil {
			return fmt.Errorf("unmarshal RestAPIState: %w", err)
		}
		api.EverTagged = taggingEverTagged(api.EverTagged, len(api.Tags), len(addTags))
		api.Tags = mergeStringMap(api.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(api)
		return state.Put(goCtx, ns, key, updated)

	case statesNamespace:
		// Through [mergeRecordStringMapTags] rather than a concrete type, and behind a kind
		// guard. This arm decoded [StateMachineState] whatever the key named, which was safe only
		// while the resolver could reach nothing but a state machine: an activity round-tripped
		// through that struct loses every member it does not carry under the same name, and the
		// namespace also holds executions and three index keys that no ARN addresses and that
		// store no tags. The two fix different failures and neither substitutes for the other
		// (part of #835, #910).
		if !sfnKeyIsTaggable(key) {
			return fmt.Errorf("unsupported Step Functions resource key: %s", key)
		}
		updated, err := mergeRecordStringMapTags(raw, sfnTagsJSONMember, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge Step Functions tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case ecrNamespace:
		var repo ECRRepository
		if err := json.Unmarshal(raw, &repo); err != nil {
			return fmt.Errorf("unmarshal ECRRepository: %w", err)
		}
		repo.EverTagged = taggingEverTagged(repo.EverTagged, len(repo.Tags), len(addTags))
		repo.Tags = mergeStringMap(repo.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(repo)
		return state.Put(goCtx, ns, key, updated)

	case ecsNamespace:
		// Through [ecsMergeRecordTags] rather than a concrete type, because this one namespace
		// holds a cluster, a service, a task and a task definition, and this arm previously
		// decoded [ECSCluster] whatever the key named — safe only while the resolver could reach
		// nothing but a cluster (#845).
		updated, err := ecsMergeRecordTags(raw, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge ECS tags %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case cognitoIDPNamespace:
		var pool CognitoUserPool
		if err := json.Unmarshal(raw, &pool); err != nil {
			return fmt.Errorf("unmarshal CognitoUserPool: %w", err)
		}
		pool.EverTagged = taggingEverTagged(pool.EverTagged, len(pool.Tags), len(addTags))
		pool.Tags = mergeStringMap(pool.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(pool)
		return state.Put(goCtx, ns, key, updated)

	case kinesisNamespace:
		var stream KinesisStream
		if err := json.Unmarshal(raw, &stream); err != nil {
			return fmt.Errorf("unmarshal KinesisStream: %w", err)
		}
		stream.EverTagged = taggingEverTagged(stream.EverTagged, len(stream.Tags), len(addTags))
		stream.Tags = mergeStringMap(stream.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(stream)
		return state.Put(goCtx, ns, key, updated)

	case rdsNamespace:
		// Guarded on the prefix and merged through raw JSON, because this one namespace holds a
		// DB instance, a DB cluster, a snapshot, a subnet group, a parameter group, a container
		// handle and several index keys. This arm decoded [RDSDBInstance] whatever the key
		// named — safe only while [TaggingPlugin.resolveARN] could reach nothing but an instance
		// (part of #835, the same shape #845 fixed for ECS).
		//
		// The guard and the raw-JSON merge answer different failures and both are needed: the
		// guard refuses a key naming something substrate stores no tags on, and the merge keeps
		// a member of one taggable shape from being dropped because another shape lacks it.
		if !rdsKeyIsTaggable(key) {
			return fmt.Errorf("unsupported RDS resource key: %s", key)
		}
		updated, err := mergeRecordStringMapTags(raw, rdsTagsJSONMember, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge RDS tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case acmNamespace:
		// Guarded on the prefix and merged through raw JSON for the reason the states and rds arms
		// are: the namespace also holds the per-account/Region index of certificate ARNs, which
		// stores no tags and is a JSON array rather than a record. Decoding [ACMCertificate]
		// unconditionally would turn that index into a certificate with every member zero (part of
		// #835).
		if !acmKeyIsTaggable(key) {
			return fmt.Errorf("unsupported ACM resource key: %s", key)
		}
		updated, err := mergeRecordStringMapTags(raw, acmTagsJSONMember, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge ACM tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case cloudfrontNamespace:
		// Guarded and merged through raw JSON, same shape: this namespace holds a distribution, an
		// invalidation and two index keys, and only the distribution stores tags — which is the
		// reference's boundary, not substrate's convenience. See [cfKeyIsTaggable].
		if !cfKeyIsTaggable(key) {
			return fmt.Errorf("unsupported CloudFront resource key: %s", key)
		}
		updated, err := mergeRecordStringMapTags(raw, cfTagsJSONMember, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge CloudFront tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case kmsNamespace:
		// The first arm to merge an array-shaped tags member, through [mergeRecordTagListTags]. The
		// guard matters more here than in most namespaces: kms holds five kinds of key and only the
		// key record stores tags, two of the other four have prefixes the key's own prefix is a
		// prefix of, and one of those — key_ids — is a JSON array of identifier strings that a tags
		// merge would leave looking like a record. See [kmsKeyIsTaggable].
		if !kmsKeyIsTaggable(key) {
			return fmt.Errorf("unsupported KMS resource key: %s", key)
		}
		updated, err := mergeRecordTagListTags(raw, kmsTagsJSONMember, kmsTagKeyField, kmsTagValueField, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge KMS tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case snsNamespace:
		// Array-shaped like KMS's, but spelled Key/Value: SNS's Tag shape uses the ordinary names, so
		// the same helper serves with different field arguments. The guard is colon-terminated because
		// "topic" is a prefix of "topic_names", whose value is a JSON array of names. See
		// [snsKeyIsTaggable].
		if !snsKeyIsTaggable(key) {
			return fmt.Errorf("unsupported SNS resource key: %s", key)
		}
		updated, err := mergeRecordTagListTags(raw, snsTagsJSONMember, snsTagKeyField, snsTagValueField, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge SNS tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case secretsManagerNamespace:
		// Array-shaped and spelled Key/Value, like SNS's. The guard is colon-terminated because
		// "secret" is a prefix of both "secret_names" and "secret_version" — and the version key's
		// value is a raw secret payload, not JSON at all, so a merge there would corrupt it rather
		// than merely write a member nothing reads. See [smKeyIsTaggable].
		if !smKeyIsTaggable(key) {
			return fmt.Errorf("unsupported Secrets Manager resource key: %s", key)
		}
		updated, err := mergeRecordTagListTags(raw, smTagsJSONMember, smTagKeyField, smTagValueField, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge Secrets Manager tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case ssmNamespace:
		// Array-shaped and spelled Key/Value, like SNS's and Secrets Manager's. The guard is
		// colon-terminated because "parameter" is a prefix of "parameter_paths", whose value is a JSON
		// array of names — the sixth namespace to need that. The namespace also holds Run Command's
		// command and invocation records, which store no tags. See [ssmKeyIsTaggable].
		if !ssmKeyIsTaggable(key) {
			return fmt.Errorf("unsupported Systems Manager resource key: %s", key)
		}
		updated, err := mergeRecordTagListTags(raw, ssmTagsJSONMember, ssmTagKeyField, ssmTagValueField, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge Systems Manager tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	case elasticacheNamespace:
		var cluster ElastiCacheCacheCluster
		if err := json.Unmarshal(raw, &cluster); err != nil {
			return fmt.Errorf("unmarshal ElastiCacheCacheCluster: %w", err)
		}
		cluster.EverTagged = taggingEverTagged(cluster.EverTagged, len(cluster.Tags), len(addTags))
		cluster.Tags = mergeStringMap(cluster.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(cluster)
		return state.Put(goCtx, ns, key, updated)

	case efsNamespace:
		if strings.HasPrefix(key, "filesystem:") {
			var fs EFSFileSystem
			if err := json.Unmarshal(raw, &fs); err != nil {
				return fmt.Errorf("unmarshal EFSFileSystem: %w", err)
			}
			fs.EverTagged = taggingEverTagged(fs.EverTagged, len(fs.Tags), len(addTags))
			fs.Tags = mergeEFSTags(fs.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(fs)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "accesspoint:") {
			var ap EFSAccessPoint
			if err := json.Unmarshal(raw, &ap); err != nil {
				return fmt.Errorf("unmarshal EFSAccessPoint: %w", err)
			}
			ap.EverTagged = taggingEverTagged(ap.EverTagged, len(ap.Tags), len(addTags))
			ap.Tags = mergeEFSTags(ap.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(ap)
			return state.Put(goCtx, ns, key, updated)
		}
		return fmt.Errorf("unsupported EFS resource key: %s", key)

	case glueNamespace:
		if strings.HasPrefix(key, "database:") {
			var db GlueDatabase
			if err := json.Unmarshal(raw, &db); err != nil {
				return fmt.Errorf("unmarshal GlueDatabase: %w", err)
			}
			db.EverTagged = taggingEverTagged(db.EverTagged, len(db.Tags), len(addTags))
			db.Tags = mergeStringMap(db.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(db)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "job:") {
			var job GlueJob
			if err := json.Unmarshal(raw, &job); err != nil {
				return fmt.Errorf("unmarshal GlueJob: %w", err)
			}
			job.EverTagged = taggingEverTagged(job.EverTagged, len(job.Tags), len(addTags))
			job.Tags = mergeStringMap(job.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(job)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "crawler:") {
			var crawler GlueCrawler
			if err := json.Unmarshal(raw, &crawler); err != nil {
				return fmt.Errorf("unmarshal GlueCrawler: %w", err)
			}
			crawler.EverTagged = taggingEverTagged(crawler.EverTagged, len(crawler.Tags), len(addTags))
			crawler.Tags = mergeStringMap(crawler.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(crawler)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "connection:") {
			var conn GlueConnection
			if err := json.Unmarshal(raw, &conn); err != nil {
				return fmt.Errorf("unmarshal GlueConnection: %w", err)
			}
			conn.EverTagged = taggingEverTagged(conn.EverTagged, len(conn.Tags), len(addTags))
			conn.Tags = mergeStringMap(conn.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(conn)
			return state.Put(goCtx, ns, key, updated)
		}
		return fmt.Errorf("unsupported Glue resource key: %s", key)

	case elbNamespace:
		// Guarded on the prefix and merged through raw JSON, like the states, rds, acm, cloudfront,
		// kms, sns, secretsmanager and ssm arms — but for only the first of the two reasons those
		// have. The guard is needed because this namespace also holds four index keys
		// (`lb_names:`, `tg_names:`, `listener_ids:`, `rule_ids:`) whose values are JSON arrays of
		// names, and a merge against one would leave an array looking like a record. The per-kind
		// decode is *not* needed: all four taggable records spell the member `Tags` and hold the
		// same `Key`/`Value` pair, so one call covers a load balancer, a target group, a listener
		// and a rule without any of them losing a member the others lack (#863).
		if !elbKeyIsTaggable(key) {
			return fmt.Errorf("unsupported ELB resource key: %s", key)
		}
		updated, err := mergeRecordTagListTags(raw, elbTagsJSONMember, elbTagKeyField, elbTagValueField, addTags, removeKeys)
		if err != nil {
			return fmt.Errorf("merge ELB tags for %s: %w", key, err)
		}
		return state.Put(goCtx, ns, key, updated)

	default:
		return fmt.Errorf("unsupported namespace for tag merge: %s", ns)
	}
}

// mergeRecordStringMapTags applies addTags and removeKeys to the named tag member of a stored
// record, returning the re-encoded record. The member must hold a JSON object of string values —
// the map[string]string shape most services store tags in.
//
// It edits the record as raw JSON rather than decoding a concrete type, for the reason
// [ecsMergeRecordTags] records for ECS: a namespace can hold several record shapes, and a writer
// that decoded one of them drops every member the others carry and stores the truncated record
// back. The rds namespace is the case in point — it holds a DB instance, a DB cluster, a
// snapshot and a subnet group, and this path decoded [RDSDBInstance] whatever the key named, so
// tagging a cluster would have replaced it with an instance-shaped husk. That is data loss
// rather than a missing feature, which is why the conversion lands with the resolver arms that
// make the other shapes reachable at all (part of #835).
//
// Unlike [ecsMergeRecordTags] the member name is a parameter, because the shapes disagree about
// it — ECS's records spell it "tags" and RDS's spell it "Tags". A caller passing a name the
// record does not use would add a second member and leave the real tags untouched, so a member
// differing from the requested one only by case is refused rather than written alongside: every
// way of not writing a tag has to be an error, per [applyTagsToResource].
func mergeRecordStringMapTags(raw []byte, member string, addTags map[string]string, removeKeys []string) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("unmarshal record: %w", err)
	}
	for name := range record {
		if name != member && strings.EqualFold(name, member) {
			return nil, fmt.Errorf("record stores tags in %q, not %q", name, member)
		}
	}

	var existing map[string]string
	if t, ok := record[member]; ok {
		if err := json.Unmarshal(t, &existing); err != nil {
			return nil, fmt.Errorf("unmarshal %s member: %w", member, err)
		}
	}
	if err := taggingStampRecordEverTagged(record, len(existing), len(addTags)); err != nil {
		return nil, err
	}
	merged, err := json.Marshal(mergeStringMap(existing, addTags, removeKeys))
	if err != nil {
		return nil, fmt.Errorf("marshal merged tags: %w", err)
	}
	record[member] = merged

	updated, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal record: %w", err)
	}
	return updated, nil
}

// mergeRecordTagListTags applies addTags and removeKeys to the named tag member of a stored record,
// returning the re-encoded record. The member must hold a JSON array of two-field objects — the
// shape a service stores tags in when its API models a Tag structure rather than a map.
//
// The array-shaped sibling of [mergeRecordStringMapTags], and raw JSON for the same reason: a
// namespace holds several record shapes and decoding one of them stores the others back truncated.
// It is a separate function rather than a mode of that one because the two disagree about more than
// the member name — an array element's field names are part of the wire shape and vary by service,
// which is why keyField and valueField are parameters. KMS alone among the services #835's remaining
// rows cover spells them "TagKey" and "TagValue" (API_Tag); SNS, Secrets Manager and Systems Manager
// spell them "Key" and "Value".
//
// The output is sorted by key. A merge that ranged the intermediate map and stopped there would
// write a member whose order came from Go's map hash seed, so one recorded run would not replay
// byte-identically — the rule #862 established for the four EC2-shaped helpers, which this one joins.
//
// A member differing from the requested one only by case is refused rather than written alongside,
// exactly as in [mergeRecordStringMapTags]: adding a second tags member reports success and stores
// nothing a reader will find.
func mergeRecordTagListTags(raw []byte, member, keyField, valueField string, addTags map[string]string, removeKeys []string) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("unmarshal record: %w", err)
	}
	for name := range record {
		if name != member && strings.EqualFold(name, member) {
			return nil, fmt.Errorf("record stores tags in %q, not %q", name, member)
		}
	}

	// Decoded as generic maps rather than a typed pair, because the field names are the caller's.
	// An element carrying members beyond the two named is preserved as far as the two go and no
	// further; no service substrate models stores a third, and inventing one here would be writing
	// a shape no reference publishes.
	var existing []map[string]string
	if t, ok := record[member]; ok && len(t) > 0 && string(t) != "null" {
		if err := json.Unmarshal(t, &existing); err != nil {
			return nil, fmt.Errorf("unmarshal %s member: %w", member, err)
		}
	}

	pairs := make(map[string]string, len(existing))
	for _, e := range existing {
		k, ok := e[keyField]
		if !ok {
			return nil, fmt.Errorf("%s element has no %q field", member, keyField)
		}
		pairs[k] = e[valueField]
	}
	if err := taggingStampRecordEverTagged(record, len(pairs), len(addTags)); err != nil {
		return nil, err
	}
	merged := mergeStringMap(pairs, addTags, removeKeys)

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]string{keyField: k, valueField: merged[k]})
	}

	// An emptied member is encoded as an empty array rather than dropped or nulled, because the
	// owning service's read path ranges it: a record whose tags member is absent and one whose
	// member is [] are the same to a decoder, and [] is the shape the reference publishes.
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal merged tags: %w", err)
	}
	record[member] = encoded

	updated, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal record: %w", err)
	}
	return updated, nil
}

// mergeStringMap applies addTags and removeKeys to an existing tag map.
func mergeStringMap(existing, add map[string]string, removeKeys []string) map[string]string {
	m := make(map[string]string)
	for k, v := range existing {
		m[k] = v
	}
	for k, v := range add {
		m[k] = v
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// mergeEC2Tags applies addTags and removeKeys to a []EC2Tag slice.
func mergeEC2Tags(existing []EC2Tag, add map[string]string, removeKeys []string) []EC2Tag {
	m := make(map[string]string, len(existing))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for k, v := range add {
		m[k] = v
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	out := make([]EC2Tag, 0, len(m))
	for k, v := range m {
		out = append(out, EC2Tag{Key: k, Value: v})
	}
	sortTagsByKey(out, func(t EC2Tag) string { return t.Key })
	return out
}

// mergeIAMTags applies addTags and removeKeys to a []IAMTag slice.
func mergeIAMTags(existing []IAMTag, add map[string]string, removeKeys []string) []IAMTag {
	m := make(map[string]string, len(existing))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for k, v := range add {
		m[k] = v
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	out := make([]IAMTag, 0, len(m))
	for k, v := range m {
		out = append(out, IAMTag{Key: k, Value: v})
	}
	sortTagsByKey(out, func(t IAMTag) string { return t.Key })
	return out
}

// sortTagsByKey orders a slice of tag structs by the key that keyOf reads off each one.
//
// The four merge helpers above persist their result, and each built its slice by ranging over a
// Go map — so two identical runs stored one resource's tags in different orders. That made
// ListTagsForResource, DescribeTags and GetResources report a different order each run for the
// same state, and it made a hash over the record differ when nothing about the record had
// changed, which is the one thing an event-sourced emulator cannot afford: a replay has to be
// able to compare state it did not change and find it equal. Sorting by key is the same answer
// DescribeStacks already gives for a stack's own tags, and for the same reason.
func sortTagsByKey[T any](tags []T, keyOf func(T) string) {
	slices.SortFunc(tags, func(a, b T) int { return strings.Compare(keyOf(a), keyOf(b)) })
}

// ----- Helpers -------------------------------------------------------------

// mapToTaggingTags converts a map[string]string to []taggingTag, ordered by key.
//
// The sort is what makes a GetResources body reproducible. Ranging the map without it returned a
// different order on almost every call — twelve identical calls against one eight-tag queue produced
// eight distinct bodies (#1011) — because Go randomizes where a map range begins. This is the single
// most load-bearing of the four converters: it has most of the scanners as call sites, and it is the
// only one that passes through a map at all, so the other three were order-preserving by luck and this
// one could not be.
//
// AWS documents no order for Tags, on this operation or on the ResourceTagMapping shape, so
// lexicographic is substrate's reading rather than a match — see docs/services.md.
func mapToTaggingTags(m map[string]string) []taggingTag {
	if len(m) == 0 {
		return nil
	}
	tags := make([]taggingTag, 0, len(m))
	for k, v := range m {
		tags = append(tags, taggingTag{Key: k, Value: v})
	}
	sortTagsByKey(tags, func(t taggingTag) string { return t.Key })
	return tags
}

// iamTagsToTaggingTags converts []IAMTag to []taggingTag, ordered by key.
//
// The stored slice already arrives sorted from mergeIAMTags, so this sort is not what fixes an observed
// disorder — it is what stops the guarantee from resting on every writer remembering to sort. A raw
// writer that bypasses the merge helper cannot make the response non-deterministic through here.
func iamTagsToTaggingTags(tags []IAMTag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag(t)
	}
	sortTagsByKey(out, func(t taggingTag) string { return t.Key })
	return out
}

// ec2TagsToTaggingTags converts []EC2Tag to []taggingTag, ordered by key, for the reason on
// [iamTagsToTaggingTags].
func ec2TagsToTaggingTags(tags []EC2Tag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag(t)
	}
	sortTagsByKey(out, func(t taggingTag) string { return t.Key })
	return out
}

// ecsTagsToTaggingTags converts []ECSTag to []taggingTag, ordered by key.
//
// The sort is here rather than at each of the four call sites so no ECS scanner can report an order
// that depends on the order tags happened to be written in, per #862.
func ecsTagsToTaggingTags(tags []ECSTag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag{Key: t.Key, Value: t.Value} //nolint:staticcheck
	}
	sortTagsByKey(out, func(t taggingTag) string { return t.Key })
	return out
}

// mergeECSTags applies addTags and removeKeys to a []ECSTag slice.
func mergeECSTags(existing []ECSTag, add map[string]string, removeKeys []string) []ECSTag {
	m := make(map[string]string, len(existing))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for k, v := range add {
		m[k] = v
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	out := make([]ECSTag, 0, len(m))
	for k, v := range m {
		out = append(out, ECSTag{Key: k, Value: v})
	}
	sortTagsByKey(out, func(t ECSTag) string { return t.Key })
	return out
}

// efsTagsToTaggingTags converts []EFSTag to []taggingTag, ordered by key, for the reason on
// [iamTagsToTaggingTags].
func efsTagsToTaggingTags(tags []EFSTag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag(t)
	}
	sortTagsByKey(out, func(t taggingTag) string { return t.Key })
	return out
}

// mergeEFSTags applies addTags and removeKeys to a []EFSTag slice.
func mergeEFSTags(existing []EFSTag, add map[string]string, removeKeys []string) []EFSTag {
	m := make(map[string]string, len(existing))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for k, v := range add {
		m[k] = v
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	out := make([]EFSTag, 0, len(m))
	for k, v := range m {
		out = append(out, EFSTag{Key: k, Value: v})
	}
	sortTagsByKey(out, func(t EFSTag) string { return t.Key })
	return out
}

// taggingJSONResponse builds an AWSResponse with a JSON body and
// Content-Type: application/json.
func taggingJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal tagging response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
