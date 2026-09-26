package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ECSPlugin emulates the Amazon Elastic Container Service JSON-protocol API.
// It handles CreateCluster, DescribeClusters, DeleteCluster, ListClusters,
// RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition,
// ListTaskDefinitions, ListTaskDefinitionFamilies, CreateService, UpdateService,
// DescribeServices, DeleteService, ListServices, RunTask, StopTask,
// DescribeTasks, ListTasks, TagResource, UntagResource, and ListTagsForResource.
type ECSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "ecs".
func (p *ECSPlugin) Name() string { return "ecs" }

// Initialize sets up the ECSPlugin with the provided configuration.
func (p *ECSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ECSPlugin.
func (p *ECSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an ECS JSON-protocol request to the appropriate handler.
// The operation is derived from the X-Amz-Target header suffix after the last dot.
func (p *ECSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if dot := strings.LastIndexByte(target, '.'); dot >= 0 {
			op = target[dot+1:]
		}
	}

	switch op {
	// Cluster operations.
	case "CreateCluster":
		return p.createCluster(ctx, req)
	case "DescribeClusters":
		return p.describeClusters(ctx, req)
	case "DeleteCluster":
		return p.deleteCluster(ctx, req)
	case "ListClusters":
		return p.listClusters(ctx, req)
	// Task definition operations.
	case "RegisterTaskDefinition":
		return p.registerTaskDefinition(ctx, req)
	case "DeregisterTaskDefinition":
		return p.deregisterTaskDefinition(ctx, req)
	case "DescribeTaskDefinition":
		return p.describeTaskDefinition(ctx, req)
	case "ListTaskDefinitions":
		return p.listTaskDefinitions(ctx, req)
	case "ListTaskDefinitionFamilies":
		return p.listTaskDefinitionFamilies(ctx, req)
	// Service operations.
	case "CreateService":
		return p.createService(ctx, req)
	case "UpdateService":
		return p.updateService(ctx, req)
	case "DescribeServices":
		return p.describeServices(ctx, req)
	case "DeleteService":
		return p.deleteService(ctx, req)
	case "ListServices":
		return p.listServices(ctx, req)
	// Task operations.
	case "RunTask":
		return p.runTask(ctx, req)
	case "StopTask":
		return p.stopTask(ctx, req)
	case "DescribeTasks":
		return p.describeTasks(ctx, req)
	case "ListTasks":
		return p.listTasks(ctx, req)
	// Tagging.
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), op)
	}
}

// --- State key helpers -------------------------------------------------------

// The prefix of every ECS record key the Resource Groups Tagging API scans. Each is a named
// constant so the key builder below and the scanner in tagging_plugin.go read the same string and
// cannot fall out of step about where a record lives — the pattern ssmParameterKeyPrefix and
// acmCertKeyPrefix already follow.
//
// The trailing colon is load-bearing in all four cases. This namespace also holds index keys
// spelled "cluster_names:", "taskdef_families:", "taskdef_revisions:", "service_names:" and
// "task_ids:", whose values are JSON arrays of names rather than resource records; a prefix without
// the colon would list them and then skip them silently when the unmarshal into a resource type
// failed. ECS is the sixth namespace to need the guard.
const (
	ecsClusterKeyPrefix = "cluster:"
	ecsTaskDefKeyPrefix = "taskdef:"
	ecsServiceKeyPrefix = "service:"
	ecsTaskKeyPrefix    = "task:"
)

func ecsClusterKey(accountID, region, name string) string {
	return ecsClusterKeyPrefix + accountID + "/" + region + "/" + name
}

func ecsClusterNamesKey(accountID, region string) string {
	return "cluster_names:" + accountID + "/" + region
}

func ecsTaskDefKey(accountID, region, family string, revision int) string {
	return ecsTaskDefKeyPrefix + accountID + "/" + region + "/" + family + "/" + strconv.Itoa(revision)
}

func ecsTaskDefFamiliesKey(accountID, region string) string {
	return "taskdef_families:" + accountID + "/" + region
}

func ecsTaskDefRevisionsKey(accountID, region, family string) string {
	return "taskdef_revisions:" + accountID + "/" + region + "/" + family
}

func ecsServiceKey(accountID, region, clusterName, serviceName string) string {
	return ecsServiceKeyPrefix + accountID + "/" + region + "/" + clusterName + "/" + serviceName
}

func ecsServiceNamesKey(accountID, region, clusterName string) string {
	return "service_names:" + accountID + "/" + region + "/" + clusterName
}

func ecsTaskKey(accountID, region, clusterName, taskID string) string {
	return ecsTaskKeyPrefix + accountID + "/" + region + "/" + clusterName + "/" + taskID
}

func ecsTaskIDsKey(accountID, region, clusterName string) string {
	return "task_ids:" + accountID + "/" + region + "/" + clusterName
}

// --- Cluster resolution ------------------------------------------------------

// resolveClusterName returns the cluster name from a name or ARN.
// ARN format: arn:aws:ecs:{region}:{acct}:cluster/{name}.
func resolveClusterName(nameOrARN string) string {
	if idx := strings.LastIndexByte(nameOrARN, '/'); idx >= 0 {
		return nameOrARN[idx+1:]
	}
	return nameOrARN
}

// --- Cluster operations ------------------------------------------------------

func (p *ECSPlugin) createCluster(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ClusterName string   `json:"clusterName"`
		Tags        []ECSTag `json:"tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
		}
	}
	if body.ClusterName == "" {
		body.ClusterName = "default"
	}

	goCtx := context.Background()
	stateKey := ecsClusterKey(ctx.AccountID, ctx.Region, body.ClusterName)

	cluster := ECSCluster{
		ClusterArn:  fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", ctx.Region, ctx.AccountID, body.ClusterName),
		ClusterName: body.ClusterName,
		Status:      "ACTIVE",
		Tags:        body.Tags,
		AccountID:   ctx.AccountID,
		Region:      ctx.Region,
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("ecs createCluster marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecsNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("ecs createCluster state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, ecsNamespace, ecsClusterNamesKey(ctx.AccountID, ctx.Region), body.ClusterName)

	type response struct {
		Cluster ECSCluster `json:"cluster"`
	}
	return ecsJSONResponse(http.StatusOK, response{Cluster: cluster})
}

func (p *ECSPlugin) describeClusters(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Clusters []string `json:"clusters"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecsInvalidBody()
		}
	}

	goCtx := context.Background()

	var names []string
	if len(body.Clusters) > 0 {
		for _, c := range body.Clusters {
			names = append(names, resolveClusterName(c))
		}
	} else {
		var err error
		names, err = loadStringIndex(goCtx, p.state, ecsNamespace, ecsClusterNamesKey(ctx.AccountID, ctx.Region))
		if err != nil {
			return nil, fmt.Errorf("ecs describeClusters loadIndex: %w", err)
		}
	}

	type failure struct {
		Arn    string `json:"arn"`
		Reason string `json:"reason"`
	}

	var clusters []ECSCluster
	var failures []failure
	for _, name := range names {
		data, err := p.state.Get(goCtx, ecsNamespace, ecsClusterKey(ctx.AccountID, ctx.Region, name))
		if err != nil {
			return nil, fmt.Errorf("ecs describeClusters state.Get: %w", err)
		}
		if data == nil {
			failures = append(failures, failure{
				Arn:    name,
				Reason: "MISSING",
			})
			continue
		}
		var c ECSCluster
		if err := json.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("ecs describeClusters unmarshal: %w", err)
		}
		clusters = append(clusters, c)
	}

	type response struct {
		Clusters []ECSCluster `json:"clusters"`
		Failures []failure    `json:"failures"`
	}
	return ecsJSONResponse(http.StatusOK, response{Clusters: clusters, Failures: failures})
}

func (p *ECSPlugin) deleteCluster(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Cluster string `json:"cluster"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "cluster is required", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveClusterName(body.Cluster)
	goCtx := context.Background()
	stateKey := ecsClusterKey(ctx.AccountID, ctx.Region, name)

	data, err := p.state.Get(goCtx, ecsNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecs deleteCluster state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ClusterNotFoundException", Message: "Cluster not found: " + name, HTTPStatus: http.StatusNotFound}
	}

	var cluster ECSCluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return nil, fmt.Errorf("ecs deleteCluster unmarshal: %w", err)
	}
	cluster.Status = "INACTIVE"

	if err := p.state.Delete(goCtx, ecsNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("ecs deleteCluster state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, ecsNamespace, ecsClusterNamesKey(ctx.AccountID, ctx.Region), name)

	type response struct {
		Cluster ECSCluster `json:"cluster"`
	}
	return ecsJSONResponse(http.StatusOK, response{Cluster: cluster})
}

func (p *ECSPlugin) listClusters(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsClusterNamesKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("ecs listClusters loadIndex: %w", err)
	}

	arns := make([]string, 0, len(names))
	for _, name := range names {
		arns = append(arns, fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", ctx.Region, ctx.AccountID, name))
	}

	type response struct {
		ClusterArns []string `json:"clusterArns"`
	}
	return ecsJSONResponse(http.StatusOK, response{ClusterArns: arns})
}

// --- Task definition operations ----------------------------------------------

func (p *ECSPlugin) registerTaskDefinition(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Family                  string        `json:"family"`
		ContainerDefinitions    []interface{} `json:"containerDefinitions"`
		NetworkMode             string        `json:"networkMode"`
		RequiresCompatibilities []string      `json:"requiresCompatibilities"`
		CPU                     string        `json:"cpu"`
		Memory                  string        `json:"memory"`
		ExecutionRoleArn        string        `json:"executionRoleArn"`
		TaskRoleArn             string        `json:"taskRoleArn"`
		Tags                    []ECSTag      `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Family == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "family is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Determine next revision.
	revKey := ecsTaskDefRevisionsKey(ctx.AccountID, ctx.Region, body.Family)
	revisions, err := loadStringIndex(goCtx, p.state, ecsNamespace, revKey)
	if err != nil {
		return nil, fmt.Errorf("ecs registerTaskDefinition loadRevisions: %w", err)
	}
	revision := len(revisions) + 1

	taskDef := ECSTaskDefinition{
		TaskDefinitionArn:       fmt.Sprintf("arn:aws:ecs:%s:%s:task-definition/%s:%d", ctx.Region, ctx.AccountID, body.Family, revision),
		Family:                  body.Family,
		Revision:                revision,
		Status:                  "ACTIVE",
		ContainerDefinitions:    body.ContainerDefinitions,
		NetworkMode:             body.NetworkMode,
		RequiresCompatibilities: body.RequiresCompatibilities,
		CPU:                     body.CPU,
		Memory:                  body.Memory,
		ExecutionRoleArn:        body.ExecutionRoleArn,
		TaskRoleArn:             body.TaskRoleArn,
		Tags:                    body.Tags,
		RegisteredAt:            EpochSeconds(p.tc.Now()),
		AccountID:               ctx.AccountID,
		Region:                  ctx.Region,
	}

	data, err := json.Marshal(taskDef)
	if err != nil {
		return nil, fmt.Errorf("ecs registerTaskDefinition marshal: %w", err)
	}

	tdKey := ecsTaskDefKey(ctx.AccountID, ctx.Region, body.Family, revision)
	if err := p.state.Put(goCtx, ecsNamespace, tdKey, data); err != nil {
		return nil, fmt.Errorf("ecs registerTaskDefinition state.Put: %w", err)
	}

	// Update indexes.
	updateStringIndex(goCtx, p.state, ecsNamespace, revKey, strconv.Itoa(revision))
	updateStringIndex(goCtx, p.state, ecsNamespace, ecsTaskDefFamiliesKey(ctx.AccountID, ctx.Region), body.Family)

	type response struct {
		TaskDefinition ECSTaskDefinition `json:"taskDefinition"`
	}
	return ecsJSONResponse(http.StatusOK, response{TaskDefinition: taskDef})
}

func (p *ECSPlugin) deregisterTaskDefinition(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TaskDefinition string `json:"taskDefinition"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	td, tdKey, err := p.resolveTaskDefinition(goCtx, ctx, body.TaskDefinition)
	if err != nil {
		return nil, err
	}

	td.Status = "INACTIVE"
	updated, err := json.Marshal(td)
	if err != nil {
		return nil, fmt.Errorf("ecs deregisterTaskDefinition marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecsNamespace, tdKey, updated); err != nil {
		return nil, fmt.Errorf("ecs deregisterTaskDefinition state.Put: %w", err)
	}

	type response struct {
		TaskDefinition ECSTaskDefinition `json:"taskDefinition"`
	}
	return ecsJSONResponse(http.StatusOK, response{TaskDefinition: *td})
}

func (p *ECSPlugin) describeTaskDefinition(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TaskDefinition string `json:"taskDefinition"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	td, _, err := p.resolveTaskDefinition(goCtx, ctx, body.TaskDefinition)
	if err != nil {
		return nil, err
	}

	type response struct {
		TaskDefinition ECSTaskDefinition `json:"taskDefinition"`
	}
	return ecsJSONResponse(http.StatusOK, response{TaskDefinition: *td})
}

func (p *ECSPlugin) listTaskDefinitions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		FamilyPrefix string `json:"familyPrefix"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecsInvalidBody()
		}
	}

	goCtx := context.Background()
	families, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsTaskDefFamiliesKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("ecs listTaskDefinitions loadFamilies: %w", err)
	}

	var arns []string
	for _, family := range families {
		if body.FamilyPrefix != "" && !strings.HasPrefix(family, body.FamilyPrefix) {
			continue
		}
		revisions, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsTaskDefRevisionsKey(ctx.AccountID, ctx.Region, family))
		if err != nil {
			return nil, fmt.Errorf("ecs listTaskDefinitions loadRevisions: %w", err)
		}
		for _, rev := range revisions {
			arns = append(arns, fmt.Sprintf("arn:aws:ecs:%s:%s:task-definition/%s:%s", ctx.Region, ctx.AccountID, family, rev))
		}
	}
	sort.Strings(arns)

	type response struct {
		TaskDefinitionArns []string `json:"taskDefinitionArns"`
	}
	return ecsJSONResponse(http.StatusOK, response{TaskDefinitionArns: arns})
}

func (p *ECSPlugin) listTaskDefinitionFamilies(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	families, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsTaskDefFamiliesKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("ecs listTaskDefinitionFamilies loadIndex: %w", err)
	}

	type response struct {
		Families []string `json:"families"`
	}
	return ecsJSONResponse(http.StatusOK, response{Families: families})
}

// resolveTaskDefinition looks up a task definition by ARN, "family:revision", or "family" (latest).
func (p *ECSPlugin) resolveTaskDefinition(goCtx context.Context, ctx *RequestContext, ref string) (*ECSTaskDefinition, string, error) {
	if ref == "" {
		return nil, "", &AWSError{Code: "InvalidParameterException", Message: "taskDefinition is required", HTTPStatus: http.StatusBadRequest}
	}

	family, revision := "", 0

	// If it looks like an ARN: arn:aws:ecs:{region}:{acct}:task-definition/{family}:{revision}
	if strings.HasPrefix(ref, "arn:") {
		// Extract "family:revision" from end of ARN.
		parts := strings.Split(ref, ":")
		if len(parts) >= 2 {
			revStr := parts[len(parts)-1]
			fam := parts[len(parts)-2]
			// The family may include the "task-definition/" prefix.
			fam = strings.TrimPrefix(fam, "task-definition/")
			if n, err := strconv.Atoi(revStr); err == nil {
				family = fam
				revision = n
			}
		}
	} else if idx := strings.LastIndexByte(ref, ':'); idx >= 0 {
		// "family:revision" format.
		if n, err := strconv.Atoi(ref[idx+1:]); err == nil {
			family = ref[:idx]
			revision = n
		}
	}

	if family == "" {
		// Use ref as family name, pick latest revision.
		family = ref
		revisions, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsTaskDefRevisionsKey(ctx.AccountID, ctx.Region, family))
		if err != nil {
			return nil, "", fmt.Errorf("ecs resolveTaskDefinition loadRevisions: %w", err)
		}
		if len(revisions) == 0 {
			return nil, "", &AWSError{Code: "ClientException", Message: "TaskDefinition not found: " + ref, HTTPStatus: http.StatusNotFound}
		}
		// Revisions are stored as strings ("1", "2", …); pick the highest.
		maxRev := 0
		for _, r := range revisions {
			if n, err := strconv.Atoi(r); err == nil && n > maxRev {
				maxRev = n
			}
		}
		revision = maxRev
	}

	tdKey := ecsTaskDefKey(ctx.AccountID, ctx.Region, family, revision)
	data, err := p.state.Get(goCtx, ecsNamespace, tdKey)
	if err != nil {
		return nil, "", fmt.Errorf("ecs resolveTaskDefinition state.Get: %w", err)
	}
	if data == nil {
		return nil, "", &AWSError{Code: "ClientException", Message: "TaskDefinition not found: " + ref, HTTPStatus: http.StatusNotFound}
	}

	var td ECSTaskDefinition
	if err := json.Unmarshal(data, &td); err != nil {
		return nil, "", fmt.Errorf("ecs resolveTaskDefinition unmarshal: %w", err)
	}
	return &td, tdKey, nil
}

// --- Service operations ------------------------------------------------------

func (p *ECSPlugin) createService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ServiceName    string   `json:"serviceName"`
		Cluster        string   `json:"cluster"`
		TaskDefinition string   `json:"taskDefinition"`
		DesiredCount   int      `json:"desiredCount"`
		LaunchType     string   `json:"launchType"`
		Tags           []ECSTag `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.ServiceName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "serviceName is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)
	clusterArn := fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", ctx.Region, ctx.AccountID, clusterName)

	goCtx := context.Background()

	svc := ECSService{
		ServiceArn:     fmt.Sprintf("arn:aws:ecs:%s:%s:service/%s/%s", ctx.Region, ctx.AccountID, clusterName, body.ServiceName),
		ServiceName:    body.ServiceName,
		ClusterArn:     clusterArn,
		TaskDefinition: body.TaskDefinition,
		DesiredCount:   body.DesiredCount,
		RunningCount:   0,
		Status:         "ACTIVE",
		LaunchType:     body.LaunchType,
		Tags:           body.Tags,
		CreatedAt:      EpochSeconds(p.tc.Now()),
		ClusterName:    clusterName,
		AccountID:      ctx.AccountID,
		Region:         ctx.Region,
	}

	data, err := json.Marshal(svc)
	if err != nil {
		return nil, fmt.Errorf("ecs createService marshal: %w", err)
	}

	svcKey := ecsServiceKey(ctx.AccountID, ctx.Region, clusterName, body.ServiceName)
	if err := p.state.Put(goCtx, ecsNamespace, svcKey, data); err != nil {
		return nil, fmt.Errorf("ecs createService state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, ecsNamespace, ecsServiceNamesKey(ctx.AccountID, ctx.Region, clusterName), body.ServiceName)

	type response struct {
		Service ECSService `json:"service"`
	}
	return ecsJSONResponse(http.StatusOK, response{Service: svc})
}

func (p *ECSPlugin) updateService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Service        string `json:"service"`
		Cluster        string `json:"cluster"`
		DesiredCount   *int   `json:"desiredCount"`
		TaskDefinition string `json:"taskDefinition"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)
	serviceName := resolveClusterName(body.Service) // same logic: extract last segment if ARN

	goCtx := context.Background()
	svcKey := ecsServiceKey(ctx.AccountID, ctx.Region, clusterName, serviceName)
	data, err := p.state.Get(goCtx, ecsNamespace, svcKey)
	if err != nil {
		return nil, fmt.Errorf("ecs updateService state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ServiceNotFoundException", Message: "Service not found: " + serviceName, HTTPStatus: http.StatusNotFound}
	}

	var svc ECSService
	if err := json.Unmarshal(data, &svc); err != nil {
		return nil, fmt.Errorf("ecs updateService unmarshal: %w", err)
	}

	if body.DesiredCount != nil {
		svc.DesiredCount = *body.DesiredCount
	}
	if body.TaskDefinition != "" {
		svc.TaskDefinition = body.TaskDefinition
	}

	updated, err := json.Marshal(svc)
	if err != nil {
		return nil, fmt.Errorf("ecs updateService marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecsNamespace, svcKey, updated); err != nil {
		return nil, fmt.Errorf("ecs updateService state.Put: %w", err)
	}

	type response struct {
		Service ECSService `json:"service"`
	}
	return ecsJSONResponse(http.StatusOK, response{Service: svc})
}

func (p *ECSPlugin) describeServices(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Cluster  string   `json:"cluster"`
		Services []string `json:"services"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)
	goCtx := context.Background()

	type failure struct {
		Arn    string `json:"arn"`
		Reason string `json:"reason"`
	}

	var services []ECSService
	var failures []failure
	for _, svc := range body.Services {
		svcName := resolveClusterName(svc)
		data, err := p.state.Get(goCtx, ecsNamespace, ecsServiceKey(ctx.AccountID, ctx.Region, clusterName, svcName))
		if err != nil {
			return nil, fmt.Errorf("ecs describeServices state.Get: %w", err)
		}
		if data == nil {
			failures = append(failures, failure{Arn: svc, Reason: "MISSING"})
			continue
		}
		var s ECSService
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf("ecs describeServices unmarshal: %w", err)
		}
		services = append(services, s)
	}

	type response struct {
		Services []ECSService `json:"services"`
		Failures []failure    `json:"failures"`
	}
	return ecsJSONResponse(http.StatusOK, response{Services: services, Failures: failures})
}

func (p *ECSPlugin) deleteService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Service string `json:"service"`
		Cluster string `json:"cluster"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)
	serviceName := resolveClusterName(body.Service)

	goCtx := context.Background()
	svcKey := ecsServiceKey(ctx.AccountID, ctx.Region, clusterName, serviceName)
	data, err := p.state.Get(goCtx, ecsNamespace, svcKey)
	if err != nil {
		return nil, fmt.Errorf("ecs deleteService state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ServiceNotFoundException", Message: "Service not found: " + serviceName, HTTPStatus: http.StatusNotFound}
	}

	var svc ECSService
	if err := json.Unmarshal(data, &svc); err != nil {
		return nil, fmt.Errorf("ecs deleteService unmarshal: %w", err)
	}
	svc.Status = "INACTIVE"

	if err := p.state.Delete(goCtx, ecsNamespace, svcKey); err != nil {
		return nil, fmt.Errorf("ecs deleteService state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, ecsNamespace, ecsServiceNamesKey(ctx.AccountID, ctx.Region, clusterName), serviceName)

	type response struct {
		Service ECSService `json:"service"`
	}
	return ecsJSONResponse(http.StatusOK, response{Service: svc})
}

func (p *ECSPlugin) listServices(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Cluster string `json:"cluster"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecsInvalidBody()
		}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsServiceNamesKey(ctx.AccountID, ctx.Region, clusterName))
	if err != nil {
		return nil, fmt.Errorf("ecs listServices loadIndex: %w", err)
	}

	arns := make([]string, 0, len(names))
	for _, name := range names {
		arns = append(arns, fmt.Sprintf("arn:aws:ecs:%s:%s:service/%s/%s", ctx.Region, ctx.AccountID, clusterName, name))
	}

	type response struct {
		ServiceArns []string `json:"serviceArns"`
	}
	return ecsJSONResponse(http.StatusOK, response{ServiceArns: arns})
}

// --- Task operations ---------------------------------------------------------

func (p *ECSPlugin) runTask(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TaskDefinition string   `json:"taskDefinition"`
		Cluster        string   `json:"cluster"`
		Count          int      `json:"count"`
		LaunchType     string   `json:"launchType"`
		Tags           []ECSTag `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.TaskDefinition == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "taskDefinition is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	if body.Count <= 0 {
		body.Count = 1
	}
	clusterName := resolveClusterName(body.Cluster)
	clusterArn := fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", ctx.Region, ctx.AccountID, clusterName)

	goCtx := context.Background()

	// Resolve task definition ARN.
	td, _, err := p.resolveTaskDefinition(goCtx, ctx, body.TaskDefinition)
	if err != nil {
		return nil, err
	}

	type failure struct {
		Arn    string `json:"arn"`
		Reason string `json:"reason"`
	}

	var tasks []ECSTask
	for i := 0; i < body.Count; i++ {
		taskID := ctx.IDs.HexUUID()[:16]
		task := ECSTask{
			TaskArn:           fmt.Sprintf("arn:aws:ecs:%s:%s:task/%s/%s", ctx.Region, ctx.AccountID, clusterName, taskID),
			TaskDefinitionArn: td.TaskDefinitionArn,
			ClusterArn:        clusterArn,
			LastStatus:        "RUNNING",
			DesiredStatus:     "RUNNING",
			LaunchType:        body.LaunchType,
			StartedAt:         EpochSeconds(p.tc.Now()),
			Tags:              body.Tags,
			AccountID:         ctx.AccountID,
			Region:            ctx.Region,
		}

		data, err := json.Marshal(task)
		if err != nil {
			return nil, fmt.Errorf("ecs runTask marshal: %w", err)
		}

		taskKey := ecsTaskKey(ctx.AccountID, ctx.Region, clusterName, taskID)
		if err := p.state.Put(goCtx, ecsNamespace, taskKey, data); err != nil {
			return nil, fmt.Errorf("ecs runTask state.Put: %w", err)
		}
		updateStringIndex(goCtx, p.state, ecsNamespace, ecsTaskIDsKey(ctx.AccountID, ctx.Region, clusterName), taskID)

		tasks = append(tasks, task)
	}

	type response struct {
		Tasks    []ECSTask `json:"tasks"`
		Failures []failure `json:"failures"`
	}
	return ecsJSONResponse(http.StatusOK, response{Tasks: tasks, Failures: nil})
}

func (p *ECSPlugin) stopTask(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Task          string `json:"task"`
		Cluster       string `json:"cluster"`
		StoppedReason string `json:"reason"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Task == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "task is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)

	// Extract task ID from ARN or use directly.
	taskID := body.Task
	if idx := strings.LastIndexByte(taskID, '/'); idx >= 0 {
		taskID = taskID[idx+1:]
	}

	goCtx := context.Background()
	taskKey := ecsTaskKey(ctx.AccountID, ctx.Region, clusterName, taskID)
	data, err := p.state.Get(goCtx, ecsNamespace, taskKey)
	if err != nil {
		return nil, fmt.Errorf("ecs stopTask state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Task not found: " + body.Task, HTTPStatus: http.StatusNotFound}
	}

	var task ECSTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("ecs stopTask unmarshal: %w", err)
	}

	task.LastStatus = "STOPPED"
	task.DesiredStatus = "STOPPED"
	task.StoppedAt = EpochSeconds(p.tc.Now())
	task.StoppedReason = body.StoppedReason

	updated, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("ecs stopTask marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecsNamespace, taskKey, updated); err != nil {
		return nil, fmt.Errorf("ecs stopTask state.Put: %w", err)
	}

	type response struct {
		Task ECSTask `json:"task"`
	}
	return ecsJSONResponse(http.StatusOK, response{Task: task})
}

func (p *ECSPlugin) describeTasks(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Cluster string   `json:"cluster"`
		Tasks   []string `json:"tasks"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)

	goCtx := context.Background()

	type failure struct {
		Arn    string `json:"arn"`
		Reason string `json:"reason"`
	}

	var tasks []ECSTask
	var failures []failure
	for _, taskRef := range body.Tasks {
		taskID := taskRef
		if idx := strings.LastIndexByte(taskID, '/'); idx >= 0 {
			taskID = taskID[idx+1:]
		}
		data, err := p.state.Get(goCtx, ecsNamespace, ecsTaskKey(ctx.AccountID, ctx.Region, clusterName, taskID))
		if err != nil {
			return nil, fmt.Errorf("ecs describeTasks state.Get: %w", err)
		}
		if data == nil {
			failures = append(failures, failure{Arn: taskRef, Reason: "MISSING"})
			continue
		}
		var t ECSTask
		if err := json.Unmarshal(data, &t); err != nil {
			return nil, fmt.Errorf("ecs describeTasks unmarshal: %w", err)
		}
		tasks = append(tasks, t)
	}

	type response struct {
		Tasks    []ECSTask `json:"tasks"`
		Failures []failure `json:"failures"`
	}
	return ecsJSONResponse(http.StatusOK, response{Tasks: tasks, Failures: failures})
}

func (p *ECSPlugin) listTasks(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Cluster string `json:"cluster"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecsInvalidBody()
		}
	}
	if body.Cluster == "" {
		body.Cluster = "default"
	}
	clusterName := resolveClusterName(body.Cluster)

	goCtx := context.Background()
	taskIDs, err := loadStringIndex(goCtx, p.state, ecsNamespace, ecsTaskIDsKey(ctx.AccountID, ctx.Region, clusterName))
	if err != nil {
		return nil, fmt.Errorf("ecs listTasks loadIndex: %w", err)
	}

	arns := make([]string, 0, len(taskIDs))
	for _, id := range taskIDs {
		arns = append(arns, fmt.Sprintf("arn:aws:ecs:%s:%s:task/%s/%s", ctx.Region, ctx.AccountID, clusterName, id))
	}

	type response struct {
		TaskArns []string `json:"taskArns"`
	}
	return ecsJSONResponse(http.StatusOK, response{TaskArns: arns})
}

// --- Tagging -----------------------------------------------------------------

// tagResource implements TagResource. The resource is addressed by the ARN the caller named,
// so no account or region is taken from the request context — see [ECSPlugin.resourceStateKey].
func (p *ECSPlugin) tagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string   `json:"resourceArn"`
		Tags        []ECSTag `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	out, err := p.applyTagsToResource(goCtx, body.ResourceArn, body.Tags, false)
	if err != nil {
		return nil, err
	}
	return ecsJSONResponse(http.StatusOK, out)
}

// untagResource implements UntagResource, addressing the resource exactly as
// [ECSPlugin.tagResource] does so the two cannot disagree about which resource an ARN names.
func (p *ECSPlugin) untagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	tags := make([]ECSTag, len(body.TagKeys))
	for i, k := range body.TagKeys {
		tags[i] = ECSTag{Key: k}
	}
	goCtx := context.Background()
	out, err := p.applyTagsToResource(goCtx, body.ResourceArn, tags, true)
	if err != nil {
		return nil, err
	}
	return ecsJSONResponse(http.StatusOK, out)
}

// listTagsForResource implements ListTagsForResource, addressing the resource exactly as the
// two write operations do.
func (p *ECSPlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string `json:"resourceArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	tags, err := p.getTagsForResource(goCtx, body.ResourceArn)
	if err != nil {
		return nil, err
	}

	type response struct {
		Tags []ECSTag `json:"tags"`
	}
	return ecsJSONResponse(http.StatusOK, response{Tags: tags})
}

// applyTagsToResource merges or removes tags on the resource identified by ARN.
// When remove is true, tag entries are treated as keys to delete.
//
// Every way of not writing a tag is an error, because the alternative is the response AWS
// documents for a *successful* tag — "an HTTP 200 response with an empty HTTP body" — over a
// resource nothing was written to. That is what TagResource and UntagResource answered for an
// ARN [ECSPlugin.resourceStateKey] could not key and for a resource that does not exist, while
// ListTagsForResource refused the same ARN: the two sides of one tag disagreed, and the write
// side was the one that lied (#845).
func (p *ECSPlugin) applyTagsToResource(goCtx context.Context, arn string, tags []ECSTag, remove bool) (struct{}, error) {
	stateKey, ns, err := p.resourceStateKey(arn)
	if err != nil {
		return struct{}{}, err
	}

	data, err := p.state.Get(goCtx, ns, stateKey)
	if err != nil {
		return struct{}{}, fmt.Errorf("ecs applyTagsToResource state.Get: %w", err)
	}
	if data == nil {
		return struct{}{}, ecsResourceNotFound(arn)
	}

	var addTags map[string]string
	var removeKeys []string
	if remove {
		removeKeys = make([]string, len(tags))
		for i, t := range tags {
			removeKeys[i] = t.Key
		}
	} else {
		addTags = make(map[string]string, len(tags))
		for _, t := range tags {
			addTags[t.Key] = t.Value
		}
	}

	updated, err := ecsMergeRecordTags(data, addTags, removeKeys)
	if err != nil {
		return struct{}{}, fmt.Errorf("ecs applyTagsToResource %s: %w", stateKey, err)
	}
	if err := p.state.Put(goCtx, ns, stateKey, updated); err != nil {
		return struct{}{}, fmt.Errorf("ecs applyTagsToResource state.Put: %w", err)
	}
	return struct{}{}, nil
}

// ecsMergeRecordTags applies addTags and removeKeys to the "tags" member of a stored ECS
// record, returning the re-encoded record.
//
// It edits the record as raw JSON rather than decoding a concrete type, because the ECS
// namespace holds four shapes — [ECSCluster], [ECSService], [ECSTask] and [ECSTaskDefinition] —
// and a writer that guessed one of them would silently drop every member the others carry and
// store the truncated record back. That is not hypothetical: [mergeResourceTags]'s ECS arm
// decoded [ECSCluster] unconditionally, which was harmless only for as long as
// [TaggingPlugin.resolveARN] could reach nothing but a cluster (#845).
//
// Shared with [mergeResourceTags] so that a tag written by ECS's own TagResource, by the
// Resource Groups Tagging API and by the CloudFormation stamp land in one place in one shape —
// the reason [mergeResourceTags] is a free function to begin with.
func ecsMergeRecordTags(raw []byte, addTags map[string]string, removeKeys []string) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("unmarshal ECS record: %w", err)
	}

	var existing []ECSTag
	if t, ok := record["tags"]; ok {
		if err := json.Unmarshal(t, &existing); err != nil {
			return nil, fmt.Errorf("unmarshal ECS record tags: %w", err)
		}
	}

	if err := taggingStampRecordEverTagged(record, len(existing), len(addTags)); err != nil {
		return nil, fmt.Errorf("stamp ECS record: %w", err)
	}

	merged, err := json.Marshal(mergeECSTags(existing, addTags, removeKeys))
	if err != nil {
		return nil, fmt.Errorf("marshal ECS record tags: %w", err)
	}
	record["tags"] = merged

	updated, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal ECS record: %w", err)
	}
	return updated, nil
}

// getTagsForResource retrieves tags from the resource identified by ARN.
func (p *ECSPlugin) getTagsForResource(goCtx context.Context, arn string) ([]ECSTag, error) {
	stateKey, ns, err := p.resourceStateKey(arn)
	if err != nil {
		return nil, err
	}

	data, err := p.state.Get(goCtx, ns, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecs getTagsForResource state.Get: %w", err)
	}
	if data == nil {
		return nil, ecsResourceNotFound(arn)
	}

	var raw struct {
		Tags []ECSTag `json:"tags"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("ecs getTagsForResource unmarshal: %w", err)
	}
	return raw.Tags, nil
}

// ecsUnsupportedTagARN reports an ARN the ECS tag operations cannot key onto a resource.
//
// InvalidParameterException, which is what TagResource's own `resourceArn` description names
// for the one bad-ARN case AWS spells out: "If you try to tag a service with a short ARN, you
// receive an InvalidParameterException error." Extending it to every other unkeyable ARN is
// substrate's reading — the page lists no error per resource type — but it is the reading that
// short service ARN forces, and answering one code for the whole class keeps the three tag
// operations from disagreeing about what a bad ARN is.
func ecsUnsupportedTagARN(arn string) error {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "unsupported resource ARN: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecsResourceNotFound reports a well-formed ECS ARN naming no stored resource.
//
// HTTP 400, which is what every ECS exception except ServerException carries: TagResource,
// UntagResource and ListTagsForResource all document "ResourceNotFoundException … HTTP Status
// Code: 400". The 404 substrate answered here is the shape of a REST service, not of a
// JSON-protocol one, and no ECS page publishes it.
func ecsResourceNotFound(arn string) error {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "Resource not found: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// resourceStateKey returns the state key and namespace for an ECS resource ARN, or an error
// naming why the ARN keys nothing. It is [ecsTagStateKey] plus ECS's own error code.
func (p *ECSPlugin) resourceStateKey(arn string) (string, string, error) {
	ns, key, ok := ecsTagStateKey(arn)
	if !ok {
		return "", "", ecsUnsupportedTagARN(arn)
	}
	return key, ns, nil
}

// ecsTagStateKey resolves a taggable ECS resource ARN to its (namespace, state key), reporting
// false for an ARN that names no resource substrate stores tags for.
//
// A free function with two callers on purpose: ECS's own three tag operations reach it through
// [ECSPlugin.resourceStateKey], and the Resource Groups Tagging API reaches it from
// [TaggingPlugin.resolveARN]. Both previously built the cluster key by hand from their own
// string concatenation, which is the arrangement #826 exists to warn about — two spellings of
// one resource's address that agree until one of them is edited. Sharing the builder is what
// makes them unable to drift, and it is what lets the tagging API reach an ECS service, task
// and task definition at all (part of #835).
//
// The two callers keep their own error codes because AWS publishes different ones: ECS names
// InvalidParameterException on TagResource's `resourceArn` description, while the tagging API's
// FailureInfo names InternalServiceException for an unsupported resource type. Only the
// *keying* is shared; what a refusal is called is each API's own business.
//
// The account and region come from the ARN, never from the caller's request context. Taking
// them from the context meant an ARN naming another account's cluster resolved the caller's own
// same-named cluster and tagged it — the defect #826 fixed for SQS. Neither caller passes a
// [RequestContext], so no arm can regress that way again.
//
// AWS lists six taggable ECS resource types — "capacity providers, tasks, services, task
// definitions, clusters, and container instances". Substrate keys four; a capacity provider and
// a container instance are refused rather than silently accepted, because substrate stores
// neither, so there is no record a tag could land on or be read back from.
func ecsTagStateKey(arn string) (ns, key string, ok bool) {
	// ARN patterns:
	//   arn:aws:ecs:{region}:{acct}:cluster/{name}
	//   arn:aws:ecs:{region}:{acct}:service/{cluster}/{name}
	//   arn:aws:ecs:{region}:{acct}:task/{cluster}/{taskID}
	//   arn:aws:ecs:{region}:{acct}:task-definition/{family}:{revision}
	if !strings.HasPrefix(arn, "arn:aws:ecs:") {
		return "", "", false
	}
	// Extract resource type and name: last two colon-separated parts give "{type}/{id...}".
	// ARN format: arn:aws:ecs:{region}:{account}:{type}/{...}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 {
		return "", "", false
	}
	region, acct := parts[3], parts[4]
	resource := parts[5] // e.g. "cluster/my-cluster" or "service/clusterName/serviceName"
	rType, rRest, found := strings.Cut(resource, "/")
	if !found {
		return "", "", false
	}

	switch rType {
	case "cluster":
		if rRest == "" || strings.ContainsAny(rRest, "/:") {
			return "", "", false
		}
		return ecsNamespace, ecsClusterKey(acct, region, rRest), true
	case "service":
		// rRest = "clusterName/serviceName". A service ARN with no cluster segment is AWS's
		// documented *short* ARN, whose refusal ECS's own page states outright: "If you try to
		// tag a service with a short ARN, you receive an InvalidParameterException error."
		cluster, name, found := strings.Cut(rRest, "/")
		if !found || cluster == "" || name == "" || strings.Contains(name, "/") {
			return "", "", false
		}
		return ecsNamespace, ecsServiceKey(acct, region, cluster, name), true
	case "task":
		// rRest = "clusterName/taskID"
		cluster, id, found := strings.Cut(rRest, "/")
		if !found || cluster == "" || id == "" || strings.Contains(id, "/") {
			return "", "", false
		}
		return ecsNamespace, ecsTaskKey(acct, region, cluster, id), true
	case "task-definition":
		// rRest = "{family}:{revision}" — ECS is the one service here that mixes separators,
		// and the revision is part of the identifier: a family alone names no single record,
		// since every RegisterTaskDefinition mints another revision under it.
		family, rev, found := strings.Cut(rRest, ":")
		if !found || family == "" || strings.ContainsAny(family, "/:") {
			return "", "", false
		}
		revision, err := strconv.Atoi(rev)
		if err != nil || revision < 1 {
			return "", "", false
		}
		return ecsNamespace, ecsTaskDefKey(acct, region, family, revision), true
	default:
		return "", "", false
	}
}

// ecsJSONResponse marshals v as JSON and returns an AWSResponse with
// Content-Type: application/x-amz-json-1.1 and the given HTTP status code.
func ecsJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ecsJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
