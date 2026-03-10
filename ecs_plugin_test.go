package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupECSPlugin(t *testing.T) (*substrate.ECSPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.ECSPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("substrate.ECSPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-ecs-1",
	}
}

func ecsRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal ecs request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "ecs",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "AmazonEC2ContainerServiceV20141113." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestECSPlugin_CreateAndDescribeCluster(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	// Create cluster.
	resp, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "my-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createOut struct {
		Cluster struct {
			ClusterName string `json:"clusterName"`
			ClusterArn  string `json:"clusterArn"`
			Status      string `json:"status"`
		} `json:"cluster"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if createOut.Cluster.ClusterName != "my-cluster" {
		t.Errorf("want clusterName my-cluster, got %q", createOut.Cluster.ClusterName)
	}
	if createOut.Cluster.Status != "ACTIVE" {
		t.Errorf("want status ACTIVE, got %q", createOut.Cluster.Status)
	}
	if !strings.HasPrefix(createOut.Cluster.ClusterArn, "arn:aws:ecs:us-east-1:123456789012:cluster/") {
		t.Errorf("unexpected ARN: %q", createOut.Cluster.ClusterArn)
	}

	// Describe by name.
	descResp, err := p.HandleRequest(ctx, ecsRequest(t, "DescribeClusters", map[string]any{
		"clusters": []string{"my-cluster"},
	}))
	if err != nil {
		t.Fatalf("DescribeClusters: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", descResp.StatusCode)
	}

	var descOut struct {
		Clusters []struct {
			ClusterName string `json:"clusterName"`
			Status      string `json:"status"`
		} `json:"clusters"`
		Failures []any `json:"failures"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe response: %v", err)
	}
	if len(descOut.Clusters) != 1 {
		t.Fatalf("want 1 cluster, got %d (failures=%v)", len(descOut.Clusters), descOut.Failures)
	}
	if descOut.Clusters[0].Status != "ACTIVE" {
		t.Errorf("want ACTIVE, got %q", descOut.Clusters[0].Status)
	}
}

func TestECSPlugin_RegisterAndDescribeTaskDefinition(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	// Register first revision.
	resp, err := p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "web",
		"containerDefinitions": []map[string]any{
			{"name": "nginx", "image": "nginx:latest"},
		},
		"cpu":    "256",
		"memory": "512",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var regOut struct {
		TaskDefinition struct {
			TaskDefinitionArn string `json:"taskDefinitionArn"`
			Family            string `json:"family"`
			Revision          int    `json:"revision"`
			Status            string `json:"status"`
		} `json:"taskDefinition"`
	}
	if err := json.Unmarshal(resp.Body, &regOut); err != nil {
		t.Fatalf("unmarshal register response: %v", err)
	}
	if regOut.TaskDefinition.Family != "web" {
		t.Errorf("want family web, got %q", regOut.TaskDefinition.Family)
	}
	if regOut.TaskDefinition.Revision != 1 {
		t.Errorf("want revision 1, got %d", regOut.TaskDefinition.Revision)
	}
	if regOut.TaskDefinition.Status != "ACTIVE" {
		t.Errorf("want status ACTIVE, got %q", regOut.TaskDefinition.Status)
	}
	arn := regOut.TaskDefinition.TaskDefinitionArn
	if !strings.Contains(arn, "task-definition/web:1") {
		t.Errorf("unexpected ARN: %q", arn)
	}

	// Register second revision.
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "web",
		"containerDefinitions": []map[string]any{
			{"name": "nginx", "image": "nginx:1.25"},
		},
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition (rev2): %v", err)
	}

	// Describe by family:revision.
	descResp, err := p.HandleRequest(ctx, ecsRequest(t, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "web:1",
	}))
	if err != nil {
		t.Fatalf("DescribeTaskDefinition: %v", err)
	}
	var descOut struct {
		TaskDefinition struct {
			Revision int    `json:"revision"`
			Status   string `json:"status"`
		} `json:"taskDefinition"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe response: %v", err)
	}
	if descOut.TaskDefinition.Revision != 1 {
		t.Errorf("want revision 1, got %d", descOut.TaskDefinition.Revision)
	}

	// Describe by family only should return latest (revision 2).
	descLatestResp, err := p.HandleRequest(ctx, ecsRequest(t, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "web",
	}))
	if err != nil {
		t.Fatalf("DescribeTaskDefinition latest: %v", err)
	}
	var descLatestOut struct {
		TaskDefinition struct {
			Revision int `json:"revision"`
		} `json:"taskDefinition"`
	}
	if err := json.Unmarshal(descLatestResp.Body, &descLatestOut); err != nil {
		t.Fatalf("unmarshal describe latest response: %v", err)
	}
	if descLatestOut.TaskDefinition.Revision != 2 {
		t.Errorf("want latest revision 2, got %d", descLatestOut.TaskDefinition.Revision)
	}
}

func TestECSPlugin_CreateAndDescribeService(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	// Create cluster.
	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "app-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// Register task definition.
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "app",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	// Create service.
	svcResp, err := p.HandleRequest(ctx, ecsRequest(t, "CreateService", map[string]any{
		"serviceName":    "my-service",
		"cluster":        "app-cluster",
		"taskDefinition": "app:1",
		"desiredCount":   3,
		"launchType":     "FARGATE",
	}))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if svcResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", svcResp.StatusCode, svcResp.Body)
	}

	var svcOut struct {
		Service struct {
			ServiceName  string `json:"serviceName"`
			DesiredCount int    `json:"desiredCount"`
			Status       string `json:"status"`
			LaunchType   string `json:"launchType"`
		} `json:"service"`
	}
	if err := json.Unmarshal(svcResp.Body, &svcOut); err != nil {
		t.Fatalf("unmarshal service response: %v", err)
	}
	if svcOut.Service.ServiceName != "my-service" {
		t.Errorf("want serviceName my-service, got %q", svcOut.Service.ServiceName)
	}
	if svcOut.Service.DesiredCount != 3 {
		t.Errorf("want desiredCount 3, got %d", svcOut.Service.DesiredCount)
	}
	if svcOut.Service.Status != "ACTIVE" {
		t.Errorf("want status ACTIVE, got %q", svcOut.Service.Status)
	}
	if svcOut.Service.LaunchType != "FARGATE" {
		t.Errorf("want launchType FARGATE, got %q", svcOut.Service.LaunchType)
	}

	// Describe service.
	descResp, err := p.HandleRequest(ctx, ecsRequest(t, "DescribeServices", map[string]any{
		"cluster":  "app-cluster",
		"services": []string{"my-service"},
	}))
	if err != nil {
		t.Fatalf("DescribeServices: %v", err)
	}
	var descOut struct {
		Services []struct {
			ServiceName string `json:"serviceName"`
		} `json:"services"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe services response: %v", err)
	}
	if len(descOut.Services) != 1 {
		t.Fatalf("want 1 service, got %d", len(descOut.Services))
	}
	if descOut.Services[0].ServiceName != "my-service" {
		t.Errorf("want serviceName my-service, got %q", descOut.Services[0].ServiceName)
	}
}

func TestECSPlugin_RunAndStopTask(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	// Create cluster.
	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "task-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// Register task definition.
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "worker",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	// Run task (count=1).
	runResp, err := p.HandleRequest(ctx, ecsRequest(t, "RunTask", map[string]any{
		"taskDefinition": "worker",
		"cluster":        "task-cluster",
		"count":          1,
	}))
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", runResp.StatusCode, runResp.Body)
	}

	var runOut struct {
		Tasks []struct {
			TaskArn    string `json:"taskArn"`
			LastStatus string `json:"lastStatus"`
		} `json:"tasks"`
		Failures []any `json:"failures"`
	}
	if err := json.Unmarshal(runResp.Body, &runOut); err != nil {
		t.Fatalf("unmarshal run response: %v", err)
	}
	if len(runOut.Tasks) != 1 {
		t.Fatalf("want 1 task, got %d (failures=%v)", len(runOut.Tasks), runOut.Failures)
	}
	taskArn := runOut.Tasks[0].TaskArn
	if runOut.Tasks[0].LastStatus != "RUNNING" {
		t.Errorf("want lastStatus RUNNING, got %q", runOut.Tasks[0].LastStatus)
	}

	// Stop the task.
	stopResp, err := p.HandleRequest(ctx, ecsRequest(t, "StopTask", map[string]any{
		"task":    taskArn,
		"cluster": "task-cluster",
		"reason":  "test complete",
	}))
	if err != nil {
		t.Fatalf("StopTask: %v", err)
	}
	if stopResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", stopResp.StatusCode, stopResp.Body)
	}

	var stopOut struct {
		Task struct {
			LastStatus    string `json:"lastStatus"`
			StoppedReason string `json:"stoppedReason"`
		} `json:"task"`
	}
	if err := json.Unmarshal(stopResp.Body, &stopOut); err != nil {
		t.Fatalf("unmarshal stop response: %v", err)
	}
	if stopOut.Task.LastStatus != "STOPPED" {
		t.Errorf("want lastStatus STOPPED, got %q", stopOut.Task.LastStatus)
	}
	if stopOut.Task.StoppedReason != "test complete" {
		t.Errorf("want stoppedReason 'test complete', got %q", stopOut.Task.StoppedReason)
	}

	// Describe tasks should reflect STOPPED status.
	descResp, err := p.HandleRequest(ctx, ecsRequest(t, "DescribeTasks", map[string]any{
		"cluster": "task-cluster",
		"tasks":   []string{taskArn},
	}))
	if err != nil {
		t.Fatalf("DescribeTasks: %v", err)
	}
	var descOut struct {
		Tasks []struct {
			LastStatus string `json:"lastStatus"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe tasks response: %v", err)
	}
	if len(descOut.Tasks) != 1 {
		t.Fatalf("want 1 task in describe, got %d", len(descOut.Tasks))
	}
	if descOut.Tasks[0].LastStatus != "STOPPED" {
		t.Errorf("want STOPPED in describe, got %q", descOut.Tasks[0].LastStatus)
	}
}

func TestECSPlugin_ListClusters(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	for _, name := range []string{"cluster-a", "cluster-b"} {
		_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
			"clusterName": name,
		}))
		if err != nil {
			t.Fatalf("CreateCluster %s: %v", name, err)
		}
	}

	listResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListClusters", map[string]any{}))
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", listResp.StatusCode)
	}

	var listOut struct {
		ClusterArns []string `json:"clusterArns"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(listOut.ClusterArns) != 2 {
		t.Fatalf("want 2 cluster ARNs, got %d", len(listOut.ClusterArns))
	}
	for _, arn := range listOut.ClusterArns {
		if !strings.HasPrefix(arn, "arn:aws:ecs:us-east-1:123456789012:cluster/") {
			t.Errorf("unexpected ARN format: %q", arn)
		}
	}
}

func TestECSPlugin_DeleteCluster(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "to-delete",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	delResp, err := p.HandleRequest(ctx, ecsRequest(t, "DeleteCluster", map[string]any{
		"cluster": "to-delete",
	}))
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", delResp.StatusCode, delResp.Body)
	}
}

func TestECSPlugin_DeregisterTaskDefinition(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	regResp, err := p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "deregtest",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}
	var regOut struct {
		TaskDefinition struct {
			TaskDefinitionArn string `json:"taskDefinitionArn"`
		} `json:"taskDefinition"`
	}
	if err := json.Unmarshal(regResp.Body, &regOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	deregResp, err := p.HandleRequest(ctx, ecsRequest(t, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": regOut.TaskDefinition.TaskDefinitionArn,
	}))
	if err != nil {
		t.Fatalf("DeregisterTaskDefinition: %v", err)
	}
	if deregResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", deregResp.StatusCode)
	}
}

func TestECSPlugin_UpdateService(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "svc-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "svc-task",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "CreateService", map[string]any{
		"serviceName":    "update-svc",
		"cluster":        "svc-cluster",
		"taskDefinition": "svc-task:1",
		"desiredCount":   1,
	}))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	updateResp, err := p.HandleRequest(ctx, ecsRequest(t, "UpdateService", map[string]any{
		"service":      "update-svc",
		"cluster":      "svc-cluster",
		"desiredCount": 3,
	}))
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if updateResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", updateResp.StatusCode, updateResp.Body)
	}
	var out struct {
		Service struct {
			DesiredCount int `json:"desiredCount"`
		} `json:"service"`
	}
	if err := json.Unmarshal(updateResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Service.DesiredCount != 3 {
		t.Errorf("want desiredCount 3, got %d", out.Service.DesiredCount)
	}
}

func TestECSPlugin_DeleteService(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "del-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "del-task",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "CreateService", map[string]any{
		"serviceName":    "del-svc",
		"cluster":        "del-cluster",
		"taskDefinition": "del-task:1",
		"desiredCount":   1,
	}))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	delResp, err := p.HandleRequest(ctx, ecsRequest(t, "DeleteService", map[string]any{
		"service": "del-svc",
		"cluster": "del-cluster",
	}))
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", delResp.StatusCode)
	}
}

func TestECSPlugin_ListServices(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "list-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "list-task",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}
	for _, name := range []string{"svc1", "svc2"} {
		_, err = p.HandleRequest(ctx, ecsRequest(t, "CreateService", map[string]any{
			"serviceName":    name,
			"cluster":        "list-cluster",
			"taskDefinition": "list-task:1",
			"desiredCount":   1,
		}))
		if err != nil {
			t.Fatalf("CreateService %s: %v", name, err)
		}
	}

	listResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListServices", map[string]any{
		"cluster": "list-cluster",
	}))
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", listResp.StatusCode)
	}
	var out struct {
		ServiceArns []string `json:"serviceArns"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.ServiceArns) != 2 {
		t.Errorf("want 2 services, got %d", len(out.ServiceArns))
	}
}

func TestECSPlugin_ListTasks(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	_, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "task-list-cluster",
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "task-family",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}
	_, err = p.HandleRequest(ctx, ecsRequest(t, "RunTask", map[string]any{
		"taskDefinition": "task-family",
		"cluster":        "task-list-cluster",
		"count":          2,
	}))
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}

	listResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListTasks", map[string]any{
		"cluster": "task-list-cluster",
	}))
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", listResp.StatusCode)
	}
	var out struct {
		TaskArns []string `json:"taskArns"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.TaskArns) != 2 {
		t.Errorf("want 2 tasks, got %d", len(out.TaskArns))
	}
}

func TestECSPlugin_ListTaskDefinitionFamilies(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	for _, family := range []string{"web", "api", "worker"} {
		_, err := p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
			"family": family,
		}))
		if err != nil {
			t.Fatalf("RegisterTaskDefinition %s: %v", family, err)
		}
	}

	listResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListTaskDefinitionFamilies", map[string]any{}))
	if err != nil {
		t.Fatalf("ListTaskDefinitionFamilies: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", listResp.StatusCode)
	}
	var out struct {
		Families []string `json:"families"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Families) != 3 {
		t.Errorf("want 3 families, got %d", len(out.Families))
	}
}

func TestECSPlugin_TagsAndListing(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	// Create cluster to tag.
	createResp, err := p.HandleRequest(ctx, ecsRequest(t, "CreateCluster", map[string]any{
		"clusterName": "tagged-cluster",
		"tags": []map[string]string{
			{"key": "env", "value": "test"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	var out struct {
		Cluster struct {
			ClusterArn string `json:"clusterArn"`
		} `json:"cluster"`
	}
	if err := json.Unmarshal(createResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	arn := out.Cluster.ClusterArn

	// TagResource.
	tagResp, err := p.HandleRequest(ctx, ecsRequest(t, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]string{
			{"key": "team", "value": "platform"},
		},
	}))
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}
	if tagResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", tagResp.StatusCode)
	}

	// ListTagsForResource.
	listResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	}))
	if err != nil {
		t.Fatalf("ListTagsForResource: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", listResp.StatusCode)
	}
	var tagOut struct {
		Tags []map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(listResp.Body, &tagOut); err != nil {
		t.Fatalf("unmarshal tags: %v", err)
	}
	if len(tagOut.Tags) == 0 {
		t.Error("expected at least one tag")
	}

	// UntagResource.
	untagResp, err := p.HandleRequest(ctx, ecsRequest(t, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"team"},
	}))
	if err != nil {
		t.Fatalf("UntagResource: %v", err)
	}
	if untagResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", untagResp.StatusCode)
	}
}

func TestECSPlugin_ListTaskDefinitions(t *testing.T) {
	p, ctx := setupECSPlugin(t)

	// Register 2 revisions of the same family.
	for range 2 {
		_, err := p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
			"family": "batch",
		}))
		if err != nil {
			t.Fatalf("RegisterTaskDefinition: %v", err)
		}
	}
	// Register a different family.
	_, err := p.HandleRequest(ctx, ecsRequest(t, "RegisterTaskDefinition", map[string]any{
		"family": "other",
	}))
	if err != nil {
		t.Fatalf("RegisterTaskDefinition other: %v", err)
	}

	// List all.
	listResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListTaskDefinitions", map[string]any{}))
	if err != nil {
		t.Fatalf("ListTaskDefinitions: %v", err)
	}
	var listOut struct {
		TaskDefinitionArns []string `json:"taskDefinitionArns"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(listOut.TaskDefinitionArns) != 3 {
		t.Fatalf("want 3 task definition ARNs, got %d", len(listOut.TaskDefinitionArns))
	}

	// List with family prefix filter.
	filtResp, err := p.HandleRequest(ctx, ecsRequest(t, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "batch",
	}))
	if err != nil {
		t.Fatalf("ListTaskDefinitions with prefix: %v", err)
	}
	var filtOut struct {
		TaskDefinitionArns []string `json:"taskDefinitionArns"`
	}
	if err := json.Unmarshal(filtResp.Body, &filtOut); err != nil {
		t.Fatalf("unmarshal filtered list response: %v", err)
	}
	if len(filtOut.TaskDefinitionArns) != 2 {
		t.Fatalf("want 2 batch ARNs, got %d", len(filtOut.TaskDefinitionArns))
	}
	for _, arn := range filtOut.TaskDefinitionArns {
		if !strings.Contains(arn, "task-definition/batch:") {
			t.Errorf("unexpected ARN in filtered results: %q", arn)
		}
	}
}
