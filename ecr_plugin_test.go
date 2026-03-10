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

func setupECRPlugin(t *testing.T) (*substrate.ECRPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.ECRPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("substrate.ECRPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-ecr-1",
	}
}

func ecrRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal ecr request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "ecr",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "AmazonEC2ContainerRegistry_V1_1_0." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestECRPlugin_CreateAndDescribeRepository(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	// Create repository.
	req := ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "my-app",
	})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createOut struct {
		Repository struct {
			RepositoryName string `json:"repositoryName"`
			RepositoryArn  string `json:"repositoryArn"`
			RegistryID     string `json:"registryId"`
			RepositoryURI  string `json:"repositoryUri"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if createOut.Repository.RepositoryName != "my-app" {
		t.Errorf("want repositoryName my-app, got %q", createOut.Repository.RepositoryName)
	}
	if createOut.Repository.RegistryID != "123456789012" {
		t.Errorf("want registryId 123456789012, got %q", createOut.Repository.RegistryID)
	}
	expectedURI := "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-app"
	if createOut.Repository.RepositoryURI != expectedURI {
		t.Errorf("want repositoryUri %q, got %q", expectedURI, createOut.Repository.RepositoryURI)
	}
	if !strings.HasPrefix(createOut.Repository.RepositoryArn, "arn:aws:ecr:us-east-1:123456789012:repository/") {
		t.Errorf("unexpected ARN: %q", createOut.Repository.RepositoryArn)
	}

	// Describe the repository.
	descReq := ecrRequest(t, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"my-app"},
	})
	descResp, err := p.HandleRequest(ctx, descReq)
	if err != nil {
		t.Fatalf("DescribeRepositories: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", descResp.StatusCode)
	}

	var descOut struct {
		Repositories []struct {
			RepositoryName string `json:"repositoryName"`
			RepositoryURI  string `json:"repositoryUri"`
		} `json:"repositories"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe response: %v", err)
	}
	if len(descOut.Repositories) != 1 {
		t.Fatalf("want 1 repository, got %d", len(descOut.Repositories))
	}
	if descOut.Repositories[0].RepositoryURI != expectedURI {
		t.Errorf("want URI %q, got %q", expectedURI, descOut.Repositories[0].RepositoryURI)
	}

	// Duplicate create should return conflict.
	resp2, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "my-app",
	}))
	if err == nil {
		t.Errorf("expected error for duplicate repository, got status %d", resp2.StatusCode)
	}
	var awsErr *substrate.AWSError
	awsErrOk := false
	if e, ok := err.(*substrate.AWSError); ok {
		awsErr = e
		awsErrOk = true
	}
	if !awsErrOk || awsErr.Code != "RepositoryAlreadyExistsException" {
		t.Errorf("want RepositoryAlreadyExistsException, got %v", err)
	}
}

func TestECRPlugin_PutAndGetImage(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	// Create repository.
	_, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "my-images",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	// Put an image.
	putReq := ecrRequest(t, "PutImage", map[string]any{
		"repositoryName": "my-images",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "latest",
	})
	putResp, err := p.HandleRequest(ctx, putReq)
	if err != nil {
		t.Fatalf("PutImage: %v", err)
	}
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d: %s", putResp.StatusCode, putResp.Body)
	}

	var putOut struct {
		Image struct {
			ImageID struct {
				ImageDigest string `json:"imageDigest"`
				ImageTag    string `json:"imageTag"`
			} `json:"imageId"`
		} `json:"image"`
	}
	if err := json.Unmarshal(putResp.Body, &putOut); err != nil {
		t.Fatalf("unmarshal put response: %v", err)
	}
	digest := putOut.Image.ImageID.ImageDigest
	if !strings.HasPrefix(digest, "sha256:") {
		t.Errorf("want sha256: digest, got %q", digest)
	}
	if putOut.Image.ImageID.ImageTag != "latest" {
		t.Errorf("want tag latest, got %q", putOut.Image.ImageID.ImageTag)
	}

	// BatchGetImage by tag.
	getReq := ecrRequest(t, "BatchGetImage", map[string]any{
		"repositoryName": "my-images",
		"imageIds": []map[string]any{
			{"imageTag": "latest"},
		},
	})
	getResp, err := p.HandleRequest(ctx, getReq)
	if err != nil {
		t.Fatalf("BatchGetImage: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", getResp.StatusCode)
	}

	var getOut struct {
		Images []struct {
			ImageID struct {
				ImageDigest string `json:"imageDigest"`
			} `json:"imageId"`
			ImageManifest string `json:"imageManifest"`
		} `json:"images"`
		Failures []any `json:"failures"`
	}
	if err := json.Unmarshal(getResp.Body, &getOut); err != nil {
		t.Fatalf("unmarshal batch get response: %v", err)
	}
	if len(getOut.Images) != 1 {
		t.Fatalf("want 1 image, got %d; failures=%v", len(getOut.Images), getOut.Failures)
	}
	if getOut.Images[0].ImageManifest != `{"schemaVersion":2}` {
		t.Errorf("unexpected manifest: %q", getOut.Images[0].ImageManifest)
	}

	// ListImages.
	listResp, err := p.HandleRequest(ctx, ecrRequest(t, "ListImages", map[string]any{
		"repositoryName": "my-images",
	}))
	if err != nil {
		t.Fatalf("ListImages: %v", err)
	}
	var listOut struct {
		ImageIDs []struct {
			ImageDigest string `json:"imageDigest"`
			ImageTag    string `json:"imageTag"`
		} `json:"imageIds"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(listOut.ImageIDs) != 1 {
		t.Fatalf("want 1 imageId, got %d", len(listOut.ImageIDs))
	}
	if listOut.ImageIDs[0].ImageTag != "latest" {
		t.Errorf("want tag latest, got %q", listOut.ImageIDs[0].ImageTag)
	}
}

func TestECRPlugin_GetAuthorizationToken(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	resp, err := p.HandleRequest(ctx, ecrRequest(t, "GetAuthorizationToken", map[string]any{}))
	if err != nil {
		t.Fatalf("GetAuthorizationToken: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", resp.StatusCode)
	}

	var out struct {
		AuthorizationData []struct {
			AuthorizationToken string `json:"authorizationToken"`
			ProxyEndpoint      string `json:"proxyEndpoint"`
		} `json:"authorizationData"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal auth token response: %v", err)
	}
	if len(out.AuthorizationData) != 1 {
		t.Fatalf("want 1 authorizationData entry, got %d", len(out.AuthorizationData))
	}
	if out.AuthorizationData[0].AuthorizationToken == "" {
		t.Error("authorizationToken is empty")
	}
	expectedEndpoint := "https://123456789012.dkr.ecr.us-east-1.amazonaws.com"
	if out.AuthorizationData[0].ProxyEndpoint != expectedEndpoint {
		t.Errorf("want proxyEndpoint %q, got %q", expectedEndpoint, out.AuthorizationData[0].ProxyEndpoint)
	}
}

func TestECRPlugin_DeleteRepository(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	// Create.
	_, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "to-delete",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	// Delete.
	delResp, err := p.HandleRequest(ctx, ecrRequest(t, "DeleteRepository", map[string]any{
		"repositoryName": "to-delete",
	}))
	if err != nil {
		t.Fatalf("DeleteRepository: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", delResp.StatusCode)
	}

	// Describe should return empty.
	descResp, err := p.HandleRequest(ctx, ecrRequest(t, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"to-delete"},
	}))
	if err != nil {
		t.Fatalf("DescribeRepositories after delete: %v", err)
	}
	var descOut struct {
		Repositories []any `json:"repositories"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe response: %v", err)
	}
	if len(descOut.Repositories) != 0 {
		t.Errorf("want 0 repositories after delete, got %d", len(descOut.Repositories))
	}

	// Second delete should fail.
	_, err = p.HandleRequest(ctx, ecrRequest(t, "DeleteRepository", map[string]any{
		"repositoryName": "to-delete",
	}))
	if err == nil {
		t.Error("expected error for deleting nonexistent repository")
	}
}

func TestECRPlugin_LifecyclePolicy(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	// Create repository.
	_, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "lifecycle-repo",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	policyText := `{"rules":[{"rulePriority":1,"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}}]}`

	// Put lifecycle policy.
	putResp, err := p.HandleRequest(ctx, ecrRequest(t, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lifecycle-repo",
		"lifecyclePolicyText": policyText,
	}))
	if err != nil {
		t.Fatalf("PutLifecyclePolicy: %v", err)
	}
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", putResp.StatusCode)
	}

	// Get lifecycle policy.
	getResp, err := p.HandleRequest(ctx, ecrRequest(t, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "lifecycle-repo",
	}))
	if err != nil {
		t.Fatalf("GetLifecyclePolicy: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", getResp.StatusCode)
	}

	var getOut struct {
		LifecyclePolicyText string `json:"lifecyclePolicyText"`
		RepositoryName      string `json:"repositoryName"`
	}
	if err := json.Unmarshal(getResp.Body, &getOut); err != nil {
		t.Fatalf("unmarshal get lifecycle response: %v", err)
	}
	if getOut.LifecyclePolicyText != policyText {
		t.Errorf("policy text mismatch: got %q", getOut.LifecyclePolicyText)
	}
	if getOut.RepositoryName != "lifecycle-repo" {
		t.Errorf("want repositoryName lifecycle-repo, got %q", getOut.RepositoryName)
	}

	// GetLifecyclePolicy on nonexistent repo should error.
	_, err = p.HandleRequest(ctx, ecrRequest(t, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "nonexistent",
	}))
	if err == nil {
		t.Error("expected error for nonexistent repository")
	}
}

func TestECRPlugin_RepositoryPolicy(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	_, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "policy-repo",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ecr:GetDownloadUrlForLayer"}]}`

	// SetRepositoryPolicy.
	setResp, err := p.HandleRequest(ctx, ecrRequest(t, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo",
		"policyText":     policyDoc,
	}))
	if err != nil {
		t.Fatalf("SetRepositoryPolicy: %v", err)
	}
	if setResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", setResp.StatusCode)
	}

	// GetRepositoryPolicy.
	getResp, err := p.HandleRequest(ctx, ecrRequest(t, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo",
	}))
	if err != nil {
		t.Fatalf("GetRepositoryPolicy: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", getResp.StatusCode)
	}
	var getOut struct {
		PolicyText     string `json:"policyText"`
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(getResp.Body, &getOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if getOut.PolicyText != policyDoc {
		t.Errorf("policy text mismatch: got %q", getOut.PolicyText)
	}

	// DeleteRepositoryPolicy.
	delResp, err := p.HandleRequest(ctx, ecrRequest(t, "DeleteRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo",
	}))
	if err != nil {
		t.Fatalf("DeleteRepositoryPolicy: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", delResp.StatusCode)
	}
}

func TestECRPlugin_BatchDeleteImage(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	_, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "batch-del-repo",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	// Put two images.
	for _, tag := range []string{"v1", "v2"} {
		_, err = p.HandleRequest(ctx, ecrRequest(t, "PutImage", map[string]any{
			"repositoryName": "batch-del-repo",
			"imageManifest":  `{"schemaVersion":2,"tag":"` + tag + `"}`,
			"imageTag":       tag,
		}))
		if err != nil {
			t.Fatalf("PutImage %s: %v", tag, err)
		}
	}

	// BatchDeleteImage.
	delResp, err := p.HandleRequest(ctx, ecrRequest(t, "BatchDeleteImage", map[string]any{
		"repositoryName": "batch-del-repo",
		"imageIds": []map[string]any{
			{"imageTag": "v1"},
		},
	}))
	if err != nil {
		t.Fatalf("BatchDeleteImage: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", delResp.StatusCode)
	}
	var delOut struct {
		ImageIDs []struct {
			ImageTag string `json:"imageTag"`
		} `json:"imageIds"`
		Failures []any `json:"failures"`
	}
	if err := json.Unmarshal(delResp.Body, &delOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(delOut.ImageIDs) != 1 {
		t.Errorf("want 1 deleted image id, got %d", len(delOut.ImageIDs))
	}
}

func TestECRPlugin_DescribeImages(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	_, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "desc-images-repo",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	_, err = p.HandleRequest(ctx, ecrRequest(t, "PutImage", map[string]any{
		"repositoryName": "desc-images-repo",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "latest",
	}))
	if err != nil {
		t.Fatalf("PutImage: %v", err)
	}

	descResp, err := p.HandleRequest(ctx, ecrRequest(t, "DescribeImages", map[string]any{
		"repositoryName": "desc-images-repo",
	}))
	if err != nil {
		t.Fatalf("DescribeImages: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", descResp.StatusCode)
	}
	var out struct {
		ImageDetails []struct {
			ImageTags []string `json:"imageTags"`
		} `json:"imageDetails"`
	}
	if err := json.Unmarshal(descResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.ImageDetails) != 1 {
		t.Errorf("want 1 image detail, got %d", len(out.ImageDetails))
	}
}

func TestECRPlugin_Tags(t *testing.T) {
	p, ctx := setupECRPlugin(t)

	createResp, err := p.HandleRequest(ctx, ecrRequest(t, "CreateRepository", map[string]any{
		"repositoryName": "tagged-ecr-repo",
	}))
	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}
	var repoOut struct {
		Repository struct {
			RepositoryArn string `json:"repositoryArn"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(createResp.Body, &repoOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	arn := repoOut.Repository.RepositoryArn

	// TagResource.
	tagResp, err := p.HandleRequest(ctx, ecrRequest(t, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        map[string]string{"env": "prod"},
	}))
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}
	if tagResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", tagResp.StatusCode)
	}

	// ListTagsForResource.
	listResp, err := p.HandleRequest(ctx, ecrRequest(t, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	}))
	if err != nil {
		t.Fatalf("ListTagsForResource: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", listResp.StatusCode)
	}
	var listOut struct {
		Tags map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(listOut.Tags) == 0 {
		t.Error("expected at least one tag")
	}

	// UntagResource.
	untagResp, err := p.HandleRequest(ctx, ecrRequest(t, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"env"},
	}))
	if err != nil {
		t.Fatalf("UntagResource: %v", err)
	}
	if untagResp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", untagResp.StatusCode)
	}
}
