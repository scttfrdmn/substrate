package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
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

type getResourcesInput struct {
	TagFilters          []tagFilter `json:"TagFilters"`
	ResourceTypeFilters []string    `json:"ResourceTypeFilters"`
	ResourcesPerPage    int         `json:"ResourcesPerPage"`
	PaginationToken     string      `json:"PaginationToken"`
}

type tagFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

type resourceTagMapping struct {
	ResourceARN       string       `json:"ResourceARN"`
	Tags              []taggingTag `json:"Tags"`
	ComplianceDetails *struct{}    `json:"ComplianceDetails,omitempty"`
}

type taggingTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type getResourcesOutput struct {
	ResourceTagMappingList []resourceTagMapping `json:"ResourceTagMappingList"`
	PaginationToken        string               `json:"PaginationToken,omitempty"`
}

func (p *TaggingPlugin) getResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in getResourcesInput
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &in); err != nil {
			return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
		}
	}
	if in.ResourcesPerPage <= 0 {
		in.ResourcesPerPage = 100
	}

	all, err := p.scanAllResources(reqCtx)
	if err != nil {
		return nil, err
	}

	// Sort by ARN for stable, deterministic pagination.
	sort.Slice(all, func(i, j int) bool {
		return all[i].ResourceARN < all[j].ResourceARN
	})

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

	// Pagination.
	offset := 0
	if in.PaginationToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(in.PaginationToken); err == nil {
			if n, err := strconv.Atoi(string(decoded)); err == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(all) {
		offset = len(all)
	}
	page := all[offset:]
	var nextToken string
	if len(page) > in.ResourcesPerPage {
		page = page[:in.ResourcesPerPage]
		nextOffset := offset + in.ResourcesPerPage
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
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
	// Service:type filter (e.g. "ec2:instance") — match resource type prefix.
	rtype := filterParts[1]
	return strings.HasPrefix(resource, rtype+"/") || strings.HasPrefix(resource, rtype)
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
		{typePrefix: "ecr", scan: p.scanECRRepositories},
		{typePrefix: "ecs", scan: p.scanECSClusters},
		{typePrefix: "cognito-idp", scan: p.scanCognitoUserPools},
		{typePrefix: "kinesis", scan: p.scanKinesisStreams},
		{typePrefix: "rds", scan: p.scanRDSInstances},
		{typePrefix: "rds", scan: p.scanRDSClusters},
		{typePrefix: "rds", scan: p.scanRDSSubnetGroups},
		{typePrefix: "elasticache", scan: p.scanElastiCacheClusters},
		{typePrefix: "elasticfilesystem", scan: p.scanEFSFileSystems},
		{typePrefix: "glue", scan: p.scanGlueDatabases},
	}

	var all []resourceTagMapping
	for _, d := range descriptors {
		resources, err := d.scan(goCtx, reqCtx)
		if err != nil {
			p.logger.Warn("tagging: scan error", "type", d.typePrefix, "err", err)
			continue
		}
		all = append(all, resources...)
	}
	return all, nil
}

func (p *TaggingPlugin) scanS3Buckets(_ context.Context, _ *RequestContext) ([]resourceTagMapping, error) {
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
		out = append(out, resourceTagMapping{
			ResourceARN: s3BucketARN(b.Name),
			Tags:        mapToTaggingTags(b.Tags),
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanLambdaFunctions(_ context.Context, _ *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, lambdaNamespace, "function:")
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanSQSQueues(_ context.Context, _ *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, sqsNamespace, "queue:")
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanDynamoDBTables(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "table:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanEC2Instances(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "instance:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanAPIGatewayAPIs(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "api:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanStepFunctionsStateMachines(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "statemachine:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanECRRepositories(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "ecrrepo:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanECSClusters(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "cluster:" + reqCtx.AccountID + "/"
	keys, err := p.state.List(goCtx, ecsNamespace, prefix)
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanCognitoUserPools(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "userpool:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanKinesisStreams(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "stream:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanRDSInstances(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "dbinstance:" + reqCtx.AccountID + "/"
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
	prefix := rdsDBClusterKeyPrefix + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

// scanRDSSubnetGroups reports every DB subnet group in the caller's account.
func (p *TaggingPlugin) scanRDSSubnetGroups(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := rdsDBSubnetGroupKeyPrefix + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanElastiCacheClusters(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "cachecluster:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanEFSFileSystems(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "filesystem:" + reqCtx.AccountID + "/"
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
		})
	}
	return out, nil
}

func (p *TaggingPlugin) scanGlueDatabases(_ context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	prefix := "database:" + reqCtx.AccountID + "/"
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
func (p *TaggingPlugin) tagMergeFailure(arn string, err error) failedResourcesInfo {
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
		name, ok := strings.CutPrefix(resource, "function:")
		if !ok || strings.Contains(name, ":") {
			return "", "", unsupportedTagResource("Lambda %q is not an unqualified function ARN", resource)
		}
		return lambdaNamespace, "function:" + name, nil

	case "sqs":
		// arn:aws:sqs:{region}:{acct}:{name}
		//
		// The key is account-qualified because [sqsURLKey] builds it from the last *two*
		// components of a queue URL, and a queue URL's penultimate component is the account.
		// Dropping the account addressed a key no queue is ever stored at, so a TagResources
		// against a real queue wrote a phantom record and answered 200 (#826).
		//
		// The account comes from the ARN rather than from the caller's request context, as it
		// does for the IAM and EC2 arms: an ARN naming another account must resolve that
		// account's queue or none.
		return sqsNamespace, "queue:" + parts[4] + "/" + resource, nil

	case "dynamodb":
		// arn:aws:dynamodb:{region}:{acct}:table/{name}
		//
		// A remaining "/" means the ARN names something nested under the table rather than the
		// table: AWS documents an index as table/{name}/index/{index} and a stream as
		// table/{name}/stream/{label}, both sharing this prefix.
		//
		// The account comes from the ARN. Taking it from the caller's request context meant an
		// ARN naming another account's table tagged the caller's own same-named table, and
		// UntagResources stripped tags from it — the defect #826 fixed for the SQS arm above,
		// in the one arm that pass missed. The resolver no longer receives a request context at
		// all, so no arm can reach for the caller's account again.
		name, ok := strings.CutPrefix(resource, "table/")
		if !ok || strings.Contains(name, "/") {
			return "", "", unsupportedTagResource("DynamoDB %q is not a table ARN", resource)
		}
		return dynamodbNamespace, "table:" + parts[4] + "/" + name, nil

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
		// arn:aws:states:{region}:{acct}:stateMachine:{name}
		//
		// The prefix check is case-sensitive on purpose. AWS distinguishes the two taggable
		// Step Functions resources by the literal segment alone — stateMachine:{name} with a
		// capital M against activity:{name} — so an activity ARN otherwise resolved to a
		// state-machine key and tagged a same-named state machine if one existed, or wrote a
		// phantom record if it did not.
		name, ok := strings.CutPrefix(resource, "stateMachine:")
		if !ok || strings.Contains(name, ":") {
			return "", "", unsupportedTagResource("Step Functions %q is not a state machine ARN", resource)
		}
		region := parts[3]
		acct := parts[4]
		return statesNamespace, "statemachine:" + acct + "/" + region + "/" + name, nil

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
		// arn:aws:kinesis:{region}:{acct}:stream/{name} — a consumer ARN nests under the same
		// prefix as stream/{name}/consumer/{name}:{timestamp}, so a remaining "/" is not a stream.
		name, ok := strings.CutPrefix(resource, "stream/")
		if !ok || strings.Contains(name, "/") {
			return "", "", unsupportedTagResource("Kinesis %q is not a stream ARN", resource)
		}
		region := parts[3]
		acct := parts[4]
		return kinesisNamespace, "stream:" + acct + "/" + region + "/" + name, nil

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

	default:
		return "", "", unsupportedTagResource("service %q has no tagging arm", svc)
	}
}

// mergeTags loads the resource at ns/key, applies addTags (merge) and removes
// removeKeys, then persists the updated resource.
//
// A thin wrapper over [mergeResourceTags], which holds the behavior so the tagging plugin
// and the CloudFormation deployer cannot drift into two merge semantics.
func (p *TaggingPlugin) mergeTags(goCtx context.Context, ns, key string, addTags map[string]string, removeKeys []string) error {
	return mergeResourceTags(goCtx, p.state, ns, key, addTags, removeKeys)
}

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
func mergeResourceTags(
	goCtx context.Context, state StateManager, ns, key string,
	addTags map[string]string, removeKeys []string,
) error {
	raw, err := state.Get(goCtx, ns, key)
	if err != nil {
		return fmt.Errorf("get resource: %w", err)
	}
	if raw == nil {
		return fmt.Errorf("resource not found: %s/%s", ns, key)
	}

	switch ns {
	case s3Namespace:
		var b S3Bucket
		if err := json.Unmarshal(raw, &b); err != nil {
			return fmt.Errorf("unmarshal S3Bucket: %w", err)
		}
		b.Tags = mergeStringMap(b.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(b)
		return state.Put(goCtx, ns, key, updated)

	case lambdaNamespace:
		var fn LambdaFunction
		if err := json.Unmarshal(raw, &fn); err != nil {
			return fmt.Errorf("unmarshal LambdaFunction: %w", err)
		}
		fn.Tags = mergeStringMap(fn.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(fn)
		return state.Put(goCtx, ns, key, updated)

	case sqsNamespace:
		var q SQSQueue
		if err := json.Unmarshal(raw, &q); err != nil {
			return fmt.Errorf("unmarshal SQSQueue: %w", err)
		}
		q.Tags = mergeStringMap(q.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(q)
		return state.Put(goCtx, ns, key, updated)

	case dynamodbNamespace:
		var t DynamoDBTable
		if err := json.Unmarshal(raw, &t); err != nil {
			return fmt.Errorf("unmarshal DynamoDBTable: %w", err)
		}
		t.Tags = mergeStringMap(t.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(t)
		return state.Put(goCtx, ns, key, updated)

	case ec2Namespace:
		var inst EC2Instance
		if err := json.Unmarshal(raw, &inst); err != nil {
			return fmt.Errorf("unmarshal EC2Instance: %w", err)
		}
		inst.Tags = mergeEC2Tags(inst.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(inst)
		return state.Put(goCtx, ns, key, updated)

	case iamNamespace:
		if strings.HasPrefix(key, "user:") {
			var u IAMUser
			if err := json.Unmarshal(raw, &u); err != nil {
				return fmt.Errorf("unmarshal IAMUser: %w", err)
			}
			u.Tags = mergeIAMTags(u.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(u)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "role:") {
			var r IAMRole
			if err := json.Unmarshal(raw, &r); err != nil {
				return fmt.Errorf("unmarshal IAMRole: %w", err)
			}
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
		api.Tags = mergeStringMap(api.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(api)
		return state.Put(goCtx, ns, key, updated)

	case statesNamespace:
		var sm StateMachineState
		if err := json.Unmarshal(raw, &sm); err != nil {
			return fmt.Errorf("unmarshal StateMachineState: %w", err)
		}
		sm.Tags = mergeStringMap(sm.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(sm)
		return state.Put(goCtx, ns, key, updated)

	case ecrNamespace:
		var repo ECRRepository
		if err := json.Unmarshal(raw, &repo); err != nil {
			return fmt.Errorf("unmarshal ECRRepository: %w", err)
		}
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
		pool.Tags = mergeStringMap(pool.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(pool)
		return state.Put(goCtx, ns, key, updated)

	case kinesisNamespace:
		var stream KinesisStream
		if err := json.Unmarshal(raw, &stream); err != nil {
			return fmt.Errorf("unmarshal KinesisStream: %w", err)
		}
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

	case elasticacheNamespace:
		var cluster ElastiCacheCacheCluster
		if err := json.Unmarshal(raw, &cluster); err != nil {
			return fmt.Errorf("unmarshal ElastiCacheCacheCluster: %w", err)
		}
		cluster.Tags = mergeStringMap(cluster.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(cluster)
		return state.Put(goCtx, ns, key, updated)

	case efsNamespace:
		if strings.HasPrefix(key, "filesystem:") {
			var fs EFSFileSystem
			if err := json.Unmarshal(raw, &fs); err != nil {
				return fmt.Errorf("unmarshal EFSFileSystem: %w", err)
			}
			fs.Tags = mergeEFSTags(fs.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(fs)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "accesspoint:") {
			var ap EFSAccessPoint
			if err := json.Unmarshal(raw, &ap); err != nil {
				return fmt.Errorf("unmarshal EFSAccessPoint: %w", err)
			}
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
			db.Tags = mergeStringMap(db.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(db)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "job:") {
			var job GlueJob
			if err := json.Unmarshal(raw, &job); err != nil {
				return fmt.Errorf("unmarshal GlueJob: %w", err)
			}
			job.Tags = mergeStringMap(job.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(job)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "crawler:") {
			var crawler GlueCrawler
			if err := json.Unmarshal(raw, &crawler); err != nil {
				return fmt.Errorf("unmarshal GlueCrawler: %w", err)
			}
			crawler.Tags = mergeStringMap(crawler.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(crawler)
			return state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "connection:") {
			var conn GlueConnection
			if err := json.Unmarshal(raw, &conn); err != nil {
				return fmt.Errorf("unmarshal GlueConnection: %w", err)
			}
			conn.Tags = mergeStringMap(conn.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(conn)
			return state.Put(goCtx, ns, key, updated)
		}
		return fmt.Errorf("unsupported Glue resource key: %s", key)

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

// mapToTaggingTags converts a map[string]string to []taggingTag.
func mapToTaggingTags(m map[string]string) []taggingTag {
	if len(m) == 0 {
		return nil
	}
	tags := make([]taggingTag, 0, len(m))
	for k, v := range m {
		tags = append(tags, taggingTag{Key: k, Value: v})
	}
	return tags
}

// iamTagsToTaggingTags converts []IAMTag to []taggingTag.
func iamTagsToTaggingTags(tags []IAMTag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag(t)
	}
	return out
}

// ec2TagsToTaggingTags converts []EC2Tag to []taggingTag.
func ec2TagsToTaggingTags(tags []EC2Tag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag(t)
	}
	return out
}

// ecsTagsToTaggingTags converts []ECSTag to []taggingTag.
func ecsTagsToTaggingTags(tags []ECSTag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag{Key: t.Key, Value: t.Value} //nolint:staticcheck
	}
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

func efsTagsToTaggingTags(tags []EFSTag) []taggingTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]taggingTag, len(tags))
	for i, t := range tags {
		out[i] = taggingTag(t)
	}
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
