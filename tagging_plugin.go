package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
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
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("The action %q is not valid for this web service", op),
			HTTPStatus: http.StatusBadRequest,
		}
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
			ResourceARN: "arn:aws:s3:::" + b.Name,
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

func (p *TaggingPlugin) scanIAMEntities(_ context.Context, _ *RequestContext) ([]resourceTagMapping, error) {
	goCtx := context.Background()
	var out []resourceTagMapping

	userKeys, err := p.state.List(goCtx, iamNamespace, "user:")
	if err != nil {
		return nil, fmt.Errorf("list iam users: %w", err)
	}
	for _, k := range userKeys {
		// Skip index keys (user_policies:, user_inline:, etc.).
		if !strings.HasPrefix(k, "user:") || strings.ContainsAny(strings.TrimPrefix(k, "user:"), ":") {
			continue
		}
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

	roleKeys, err := p.state.List(goCtx, iamNamespace, "role:")
	if err != nil {
		return nil, fmt.Errorf("list iam roles: %w", err)
	}
	for _, k := range roleKeys {
		if !strings.HasPrefix(k, "role:") || strings.ContainsAny(strings.TrimPrefix(k, "role:"), ":") {
			continue
		}
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

func (p *TaggingPlugin) tagResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in tagResourcesInput
	if err := json.Unmarshal(req.Body, &in); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	failures := make(map[string]failedResourcesInfo)
	goCtx := context.Background()

	for _, arn := range in.ResourceARNList {
		ns, key, err := p.resolveARN(arn, reqCtx)
		if err != nil {
			failures[arn] = failedResourcesInfo{
				ErrorCode:    "InvalidParameterException",
				ErrorMessage: err.Error(),
				StatusCode:   http.StatusBadRequest,
			}
			continue
		}
		if err := p.mergeTags(goCtx, ns, key, in.Tags, nil); err != nil {
			failures[arn] = failedResourcesInfo{
				ErrorCode:    "InternalServiceException",
				ErrorMessage: err.Error(),
				StatusCode:   http.StatusInternalServerError,
			}
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

func (p *TaggingPlugin) untagResources(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in untagResourcesInput
	if err := json.Unmarshal(req.Body, &in); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	failures := make(map[string]failedResourcesInfo)
	goCtx := context.Background()

	for _, arn := range in.ResourceARNList {
		ns, key, err := p.resolveARN(arn, reqCtx)
		if err != nil {
			failures[arn] = failedResourcesInfo{
				ErrorCode:    "InvalidParameterException",
				ErrorMessage: err.Error(),
				StatusCode:   http.StatusBadRequest,
			}
			continue
		}
		if err := p.mergeTags(goCtx, ns, key, nil, in.TagKeys); err != nil {
			failures[arn] = failedResourcesInfo{
				ErrorCode:    "InternalServiceException",
				ErrorMessage: err.Error(),
				StatusCode:   http.StatusInternalServerError,
			}
		}
	}

	out := untagResourcesOutput{}
	if len(failures) > 0 {
		out.FailedResourcesMap = failures
	}
	return taggingJSONResponse(http.StatusOK, out)
}

// ----- ARN resolver --------------------------------------------------------

// resolveARN parses an ARN and returns the (namespace, stateKey) for the resource.
// Returns an error if the ARN format is unrecognised.
func (p *TaggingPlugin) resolveARN(arn string, reqCtx *RequestContext) (ns, key string, err error) {
	// ARN format: arn:aws:{service}:{region}:{account}:{resource}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return "", "", fmt.Errorf("invalid ARN: %q", arn)
	}
	svc := parts[2]
	resource := parts[5] // e.g. "instance/i-abc", "table/mytable", "my-bucket"

	switch svc {
	case "s3":
		// arn:aws:s3:::bucket-name
		bucket := strings.TrimPrefix(resource, "")
		if strings.Contains(bucket, "/") {
			// object ARN — not supported for tagging via this API
			return "", "", fmt.Errorf("S3 object ARNs are not supported; use bucket ARN")
		}
		return s3Namespace, "bucket:" + bucket, nil

	case "lambda":
		// arn:aws:lambda:{region}:{acct}:function:{name}
		name := strings.TrimPrefix(resource, "function:")
		return lambdaNamespace, "function:" + name, nil

	case "sqs":
		// arn:aws:sqs:{region}:{acct}:{name}
		return sqsNamespace, "queue:" + resource, nil

	case "dynamodb":
		// arn:aws:dynamodb:{region}:{acct}:table/{name}
		name := strings.TrimPrefix(resource, "table/")
		return dynamodbNamespace, "table:" + reqCtx.AccountID + "/" + name, nil

	case "ec2":
		// arn:aws:ec2:{region}:{acct}:instance/{id}
		if strings.HasPrefix(resource, "instance/") {
			id := strings.TrimPrefix(resource, "instance/")
			region := parts[3]
			acct := parts[4]
			return ec2Namespace, "instance:" + acct + "/" + region + "/" + id, nil
		}
		return "", "", fmt.Errorf("unsupported EC2 resource type in ARN: %q", resource)

	case "iam":
		if strings.HasPrefix(resource, "user/") {
			name := strings.TrimPrefix(resource, "user/")
			return iamNamespace, "user:" + name, nil
		}
		if strings.HasPrefix(resource, "role/") {
			name := strings.TrimPrefix(resource, "role/")
			return iamNamespace, "role:" + name, nil
		}
		return "", "", fmt.Errorf("unsupported IAM resource type in ARN: %q", resource)

	default:
		return "", "", fmt.Errorf("unsupported service %q for tagging", svc)
	}
}

// mergeTags loads the resource at ns/key, applies addTags (merge) and removes
// removeKeys, then persists the updated resource.
//
// For S3 / Lambda / SQS / DynamoDB, tags are map[string]string.
// For IAM and EC2, tags are []IAMTag / []EC2Tag.
func (p *TaggingPlugin) mergeTags(goCtx context.Context, ns, key string, addTags map[string]string, removeKeys []string) error {
	raw, err := p.state.Get(goCtx, ns, key)
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
		return p.state.Put(goCtx, ns, key, updated)

	case lambdaNamespace:
		var fn LambdaFunction
		if err := json.Unmarshal(raw, &fn); err != nil {
			return fmt.Errorf("unmarshal LambdaFunction: %w", err)
		}
		fn.Tags = mergeStringMap(fn.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(fn)
		return p.state.Put(goCtx, ns, key, updated)

	case sqsNamespace:
		var q SQSQueue
		if err := json.Unmarshal(raw, &q); err != nil {
			return fmt.Errorf("unmarshal SQSQueue: %w", err)
		}
		q.Tags = mergeStringMap(q.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(q)
		return p.state.Put(goCtx, ns, key, updated)

	case dynamodbNamespace:
		var t DynamoDBTable
		if err := json.Unmarshal(raw, &t); err != nil {
			return fmt.Errorf("unmarshal DynamoDBTable: %w", err)
		}
		t.Tags = mergeStringMap(t.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(t)
		return p.state.Put(goCtx, ns, key, updated)

	case ec2Namespace:
		var inst EC2Instance
		if err := json.Unmarshal(raw, &inst); err != nil {
			return fmt.Errorf("unmarshal EC2Instance: %w", err)
		}
		inst.Tags = mergeEC2Tags(inst.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(inst)
		return p.state.Put(goCtx, ns, key, updated)

	case iamNamespace:
		if strings.HasPrefix(key, "user:") {
			var u IAMUser
			if err := json.Unmarshal(raw, &u); err != nil {
				return fmt.Errorf("unmarshal IAMUser: %w", err)
			}
			u.Tags = mergeIAMTags(u.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(u)
			return p.state.Put(goCtx, ns, key, updated)
		}
		if strings.HasPrefix(key, "role:") {
			var r IAMRole
			if err := json.Unmarshal(raw, &r); err != nil {
				return fmt.Errorf("unmarshal IAMRole: %w", err)
			}
			r.Tags = mergeIAMTags(r.Tags, addTags, removeKeys)
			updated, _ := json.Marshal(r)
			return p.state.Put(goCtx, ns, key, updated)
		}
		return fmt.Errorf("unsupported IAM resource key: %s", key)

	default:
		return fmt.Errorf("unsupported namespace for tag merge: %s", ns)
	}
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
	return out
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
