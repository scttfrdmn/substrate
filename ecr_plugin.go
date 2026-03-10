package substrate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ECRPlugin emulates the Amazon Elastic Container Registry JSON-protocol API.
// It handles CreateRepository, DescribeRepositories, DeleteRepository, PutImage,
// BatchGetImage, DescribeImages, BatchDeleteImage, ListImages,
// GetAuthorizationToken, PutLifecyclePolicy, GetLifecyclePolicy,
// SetRepositoryPolicy, GetRepositoryPolicy, DeleteRepositoryPolicy,
// TagResource, UntagResource, and ListTagsForResource.
type ECRPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "ecr".
func (p *ECRPlugin) Name() string { return "ecr" }

// Initialize sets up the ECRPlugin with the provided configuration.
func (p *ECRPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ECRPlugin.
func (p *ECRPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an ECR JSON-protocol request to the appropriate handler.
// The operation is derived from the X-Amz-Target header suffix after the last dot.
func (p *ECRPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if dot := strings.LastIndexByte(target, '.'); dot >= 0 {
			op = target[dot+1:]
		}
	}

	switch op {
	case "CreateRepository":
		return p.createRepository(ctx, req)
	case "DescribeRepositories":
		return p.describeRepositories(ctx, req)
	case "DeleteRepository":
		return p.deleteRepository(ctx, req)
	case "PutImage":
		return p.putImage(ctx, req)
	case "BatchGetImage":
		return p.batchGetImage(ctx, req)
	case "DescribeImages":
		return p.describeImages(ctx, req)
	case "BatchDeleteImage":
		return p.batchDeleteImage(ctx, req)
	case "ListImages":
		return p.listImages(ctx, req)
	case "GetAuthorizationToken":
		return p.getAuthorizationToken(ctx)
	case "PutLifecyclePolicy":
		return p.putLifecyclePolicy(ctx, req)
	case "GetLifecyclePolicy":
		return p.getLifecyclePolicy(ctx, req)
	case "SetRepositoryPolicy":
		return p.setRepositoryPolicy(ctx, req)
	case "GetRepositoryPolicy":
		return p.getRepositoryPolicy(ctx, req)
	case "DeleteRepositoryPolicy":
		return p.deleteRepositoryPolicy(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("ECRPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State key helpers -------------------------------------------------------

func ecrRepoKey(accountID, region, name string) string {
	return "ecrrepo:" + accountID + "/" + region + "/" + name
}

func ecrRepoNamesKey(accountID, region string) string {
	return "ecrrepo_names:" + accountID + "/" + region
}

func ecrImageKey(accountID, region, repo, digest string) string {
	return "ecrimage:" + accountID + "/" + region + "/" + repo + "/" + digest
}

func ecrImageTagsKey(accountID, region, repo string) string {
	return "ecrimage_tags:" + accountID + "/" + region + "/" + repo
}

// --- Repository operations ---------------------------------------------------

func (p *ECRPlugin) createRepository(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName             string            `json:"repositoryName"`
		Tags                       map[string]string `json:"tags"`
		ImageScanningConfiguration struct {
			ScanOnPush bool `json:"scanOnPush"`
		} `json:"imageScanningConfiguration"`
		EncryptionConfiguration struct {
			EncryptionType string `json:"encryptionType"`
		} `json:"encryptionConfiguration"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	existing, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecr createRepository state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "RepositoryAlreadyExistsException", Message: "Repository already exists: " + body.RepositoryName, HTTPStatus: http.StatusConflict}
	}

	encType := body.EncryptionConfiguration.EncryptionType
	if encType == "" {
		encType = "AES256"
	}

	repo := ECRRepository{
		RepositoryName: body.RepositoryName,
		RepositoryArn:  fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", ctx.Region, ctx.AccountID, body.RepositoryName),
		RegistryID:     ctx.AccountID,
		RepositoryURI:  fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", ctx.AccountID, ctx.Region, body.RepositoryName),
		CreatedAt:      p.tc.Now(),
		Tags:           body.Tags,
		AccountID:      ctx.AccountID,
		Region:         ctx.Region,
	}
	repo.ImageScanningConfiguration.ScanOnPush = body.ImageScanningConfiguration.ScanOnPush
	repo.EncryptionConfiguration.EncryptionType = encType

	data, err := json.Marshal(repo)
	if err != nil {
		return nil, fmt.Errorf("ecr createRepository marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("ecr createRepository state.Put: %w", err)
	}

	idxKey := ecrRepoNamesKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, ecrNamespace, idxKey, body.RepositoryName)

	type response struct {
		Repository ECRRepository `json:"repository"`
	}
	return ecrJSONResponse(http.StatusOK, response{Repository: repo})
}

func (p *ECRPlugin) describeRepositories(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryNames []string `json:"repositoryNames"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	goCtx := context.Background()

	var names []string
	if len(body.RepositoryNames) > 0 {
		names = body.RepositoryNames
	} else {
		idxKey := ecrRepoNamesKey(ctx.AccountID, ctx.Region)
		var err error
		names, err = loadStringIndex(goCtx, p.state, ecrNamespace, idxKey)
		if err != nil {
			return nil, fmt.Errorf("ecr describeRepositories loadIndex: %w", err)
		}
	}

	repos := make([]ECRRepository, 0, len(names))
	for _, name := range names {
		data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, name))
		if err != nil {
			return nil, fmt.Errorf("ecr describeRepositories state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		var repo ECRRepository
		if err := json.Unmarshal(data, &repo); err != nil {
			return nil, fmt.Errorf("ecr describeRepositories unmarshal: %w", err)
		}
		repos = append(repos, repo)
	}

	type response struct {
		Repositories []ECRRepository `json:"repositories"`
	}
	return ecrJSONResponse(http.StatusOK, response{Repositories: repos})
}

func (p *ECRPlugin) deleteRepository(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
		Force          bool   `json:"force"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	data, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecr deleteRepository state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr deleteRepository unmarshal: %w", err)
	}

	if err := p.state.Delete(goCtx, ecrNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("ecr deleteRepository state.Delete: %w", err)
	}

	idxKey := ecrRepoNamesKey(ctx.AccountID, ctx.Region)
	removeFromStringIndex(goCtx, p.state, ecrNamespace, idxKey, body.RepositoryName)

	type response struct {
		Repository ECRRepository `json:"repository"`
	}
	return ecrJSONResponse(http.StatusOK, response{Repository: repo})
}

// --- Image operations --------------------------------------------------------

func (p *ECRPlugin) putImage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
		ImageManifest  string `json:"imageManifest"`
		ImageTag       string `json:"imageTag"`
		ImageDigest    string `json:"imageDigest"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Verify repository exists.
	repoData, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName))
	if err != nil {
		return nil, fmt.Errorf("ecr putImage state.Get repo: %w", err)
	}
	if repoData == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	digest := body.ImageDigest
	if digest == "" {
		digest = generateECRDigest()
	}

	img := ECRImage{
		ImageDigest:      digest,
		ImageTag:         body.ImageTag,
		ImagePushedAt:    p.tc.Now(),
		ImageSizeInBytes: int64(len(body.ImageManifest)),
		ImageManifest:    body.ImageManifest,
		RepoName:         body.RepositoryName,
	}

	imgData, err := json.Marshal(img)
	if err != nil {
		return nil, fmt.Errorf("ecr putImage marshal: %w", err)
	}

	imgKey := ecrImageKey(ctx.AccountID, ctx.Region, body.RepositoryName, digest)
	if err := p.state.Put(goCtx, ecrNamespace, imgKey, imgData); err != nil {
		return nil, fmt.Errorf("ecr putImage state.Put: %w", err)
	}

	// Update tag→digest index if a tag was provided.
	if body.ImageTag != "" {
		tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
		tagsMap := p.loadImageTagsMap(goCtx, tagsKey)
		tagsMap[body.ImageTag] = digest
		p.saveImageTagsMap(goCtx, tagsKey, tagsMap)
	}

	type imageID struct {
		ImageDigest string `json:"imageDigest"`
		ImageTag    string `json:"imageTag,omitempty"`
	}
	type imageResult struct {
		RepositoryName string  `json:"repositoryName"`
		ImageID        imageID `json:"imageId"`
		ImageManifest  string  `json:"imageManifest"`
	}
	type response struct {
		Image imageResult `json:"image"`
	}
	return ecrJSONResponse(http.StatusOK, response{Image: imageResult{
		RepositoryName: body.RepositoryName,
		ImageID:        imageID{ImageDigest: digest, ImageTag: body.ImageTag},
		ImageManifest:  body.ImageManifest,
	}})
}

func (p *ECRPlugin) batchGetImage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
		ImageIDs       []struct {
			ImageDigest string `json:"imageDigest"`
			ImageTag    string `json:"imageTag"`
		} `json:"imageIds"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)

	type imageID struct {
		ImageDigest string `json:"imageDigest"`
		ImageTag    string `json:"imageTag,omitempty"`
	}
	type imageResult struct {
		RepositoryName string  `json:"repositoryName"`
		ImageID        imageID `json:"imageId"`
		ImageManifest  string  `json:"imageManifest"`
	}
	type failure struct {
		ImageID       imageID `json:"imageId"`
		FailureCode   string  `json:"failureCode"`
		FailureReason string  `json:"failureReason"`
	}

	var images []imageResult
	var failures []failure

	for _, id := range body.ImageIDs {
		digest := id.ImageDigest
		if digest == "" && id.ImageTag != "" {
			var ok bool
			digest, ok = tagsMap[id.ImageTag]
			if !ok {
				failures = append(failures, failure{
					ImageID:       imageID{ImageTag: id.ImageTag},
					FailureCode:   "ImageNotFoundException",
					FailureReason: "Image not found",
				})
				continue
			}
		}
		data, err := p.state.Get(goCtx, ecrNamespace, ecrImageKey(ctx.AccountID, ctx.Region, body.RepositoryName, digest))
		if err != nil {
			return nil, fmt.Errorf("ecr batchGetImage state.Get: %w", err)
		}
		if data == nil {
			failures = append(failures, failure{
				ImageID:       imageID{ImageDigest: digest, ImageTag: id.ImageTag},
				FailureCode:   "ImageNotFoundException",
				FailureReason: "Image not found",
			})
			continue
		}
		var img ECRImage
		if err := json.Unmarshal(data, &img); err != nil {
			return nil, fmt.Errorf("ecr batchGetImage unmarshal: %w", err)
		}
		images = append(images, imageResult{
			RepositoryName: body.RepositoryName,
			ImageID:        imageID{ImageDigest: img.ImageDigest, ImageTag: img.ImageTag},
			ImageManifest:  img.ImageManifest,
		})
	}

	type response struct {
		Images   []imageResult `json:"images"`
		Failures []failure     `json:"failures"`
	}
	return ecrJSONResponse(http.StatusOK, response{Images: images, Failures: failures})
}

func (p *ECRPlugin) describeImages(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
		ImageIDs       []struct {
			ImageDigest string `json:"imageDigest"`
			ImageTag    string `json:"imageTag"`
		} `json:"imageIds"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)

	// Collect requested digests.
	var requestedDigests []string
	if len(body.ImageIDs) > 0 {
		for _, id := range body.ImageIDs {
			if id.ImageDigest != "" {
				requestedDigests = append(requestedDigests, id.ImageDigest)
			} else if id.ImageTag != "" {
				if d, ok := tagsMap[id.ImageTag]; ok {
					requestedDigests = append(requestedDigests, d)
				}
			}
		}
	} else {
		// All images: collect all digests from tags map.
		seen := make(map[string]bool)
		for _, d := range tagsMap {
			if !seen[d] {
				seen[d] = true
				requestedDigests = append(requestedDigests, d)
			}
		}
	}

	type imageDetail struct {
		RegistryID       string    `json:"registryId"`
		RepositoryName   string    `json:"repositoryName"`
		ImageDigest      string    `json:"imageDigest"`
		ImageTags        []string  `json:"imageTags,omitempty"`
		ImageSizeInBytes int64     `json:"imageSizeInBytes"`
		ImagePushedAt    time.Time `json:"imagePushedAt"`
	}

	// Build reverse: digest → tags.
	digestToTags := make(map[string][]string)
	for tag, digest := range tagsMap {
		digestToTags[digest] = append(digestToTags[digest], tag)
	}
	for d := range digestToTags {
		sort.Strings(digestToTags[d])
	}

	var details []imageDetail
	for _, digest := range requestedDigests {
		data, err := p.state.Get(goCtx, ecrNamespace, ecrImageKey(ctx.AccountID, ctx.Region, body.RepositoryName, digest))
		if err != nil {
			return nil, fmt.Errorf("ecr describeImages state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		var img ECRImage
		if err := json.Unmarshal(data, &img); err != nil {
			return nil, fmt.Errorf("ecr describeImages unmarshal: %w", err)
		}
		details = append(details, imageDetail{
			RegistryID:       ctx.AccountID,
			RepositoryName:   body.RepositoryName,
			ImageDigest:      img.ImageDigest,
			ImageTags:        digestToTags[img.ImageDigest],
			ImageSizeInBytes: img.ImageSizeInBytes,
			ImagePushedAt:    img.ImagePushedAt,
		})
	}

	type response struct {
		ImageDetails []imageDetail `json:"imageDetails"`
	}
	return ecrJSONResponse(http.StatusOK, response{ImageDetails: details})
}

func (p *ECRPlugin) batchDeleteImage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
		ImageIDs       []struct {
			ImageDigest string `json:"imageDigest"`
			ImageTag    string `json:"imageTag"`
		} `json:"imageIds"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)

	type imageID struct {
		ImageDigest string `json:"imageDigest,omitempty"`
		ImageTag    string `json:"imageTag,omitempty"`
	}
	type failure struct {
		ImageID       imageID `json:"imageId"`
		FailureCode   string  `json:"failureCode"`
		FailureReason string  `json:"failureReason"`
	}

	var deleted []imageID
	var failures []failure

	for _, id := range body.ImageIDs {
		digest := id.ImageDigest
		tag := id.ImageTag
		if digest == "" && tag != "" {
			var ok bool
			digest, ok = tagsMap[tag]
			if !ok {
				failures = append(failures, failure{
					ImageID:       imageID{ImageTag: tag},
					FailureCode:   "ImageNotFoundException",
					FailureReason: "Image not found",
				})
				continue
			}
		}
		imgKey := ecrImageKey(ctx.AccountID, ctx.Region, body.RepositoryName, digest)
		if err := p.state.Delete(goCtx, ecrNamespace, imgKey); err != nil {
			return nil, fmt.Errorf("ecr batchDeleteImage state.Delete: %w", err)
		}
		// Remove from tags map.
		if tag != "" {
			delete(tagsMap, tag)
		} else {
			for t, d := range tagsMap {
				if d == digest {
					delete(tagsMap, t)
				}
			}
		}
		deleted = append(deleted, imageID{ImageDigest: digest, ImageTag: tag})
	}
	p.saveImageTagsMap(goCtx, tagsKey, tagsMap)

	type response struct {
		ImageIDs []imageID `json:"imageIds"`
		Failures []failure `json:"failures"`
	}
	return ecrJSONResponse(http.StatusOK, response{ImageIDs: deleted, Failures: failures})
}

func (p *ECRPlugin) listImages(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)

	type imageID struct {
		ImageDigest string `json:"imageDigest"`
		ImageTag    string `json:"imageTag,omitempty"`
	}

	// Build de-duplicated list: one entry per digest (with or without tag).
	seen := make(map[string]bool)
	var ids []imageID
	for tag, digest := range tagsMap {
		if !seen[digest] {
			seen[digest] = true
			ids = append(ids, imageID{ImageDigest: digest, ImageTag: tag})
		}
	}

	type response struct {
		ImageIDs []imageID `json:"imageIds"`
	}
	return ecrJSONResponse(http.StatusOK, response{ImageIDs: ids})
}

// --- Auth token --------------------------------------------------------------

func (p *ECRPlugin) getAuthorizationToken(ctx *RequestContext) (*AWSResponse, error) {
	token := base64.StdEncoding.EncodeToString([]byte("AWS:password"))
	expiresAt := p.tc.Now().Add(12 * time.Hour)

	type authData struct {
		AuthorizationToken string    `json:"authorizationToken"`
		ExpiresAt          time.Time `json:"expiresAt"`
		ProxyEndpoint      string    `json:"proxyEndpoint"`
	}
	type response struct {
		AuthorizationData []authData `json:"authorizationData"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		AuthorizationData: []authData{{
			AuthorizationToken: token,
			ExpiresAt:          expiresAt,
			ProxyEndpoint:      fmt.Sprintf("https://%s.dkr.ecr.%s.amazonaws.com", ctx.AccountID, ctx.Region),
		}},
	})
}

// --- Lifecycle policy --------------------------------------------------------

func (p *ECRPlugin) putLifecyclePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName      string `json:"repositoryName"`
		LifecyclePolicyText string `json:"lifecyclePolicyText"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	data, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecr putLifecyclePolicy state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr putLifecyclePolicy unmarshal: %w", err)
	}
	repo.LifecyclePolicy = body.LifecyclePolicyText

	updated, err := json.Marshal(repo)
	if err != nil {
		return nil, fmt.Errorf("ecr putLifecyclePolicy marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ecr putLifecyclePolicy state.Put: %w", err)
	}

	type response struct {
		LifecyclePolicyText string `json:"lifecyclePolicyText"`
		RepositoryName      string `json:"repositoryName"`
		RegistryID          string `json:"registryId"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		LifecyclePolicyText: body.LifecyclePolicyText,
		RepositoryName:      body.RepositoryName,
		RegistryID:          ctx.AccountID,
	})
}

func (p *ECRPlugin) getLifecyclePolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName))
	if err != nil {
		return nil, fmt.Errorf("ecr getLifecyclePolicy state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr getLifecyclePolicy unmarshal: %w", err)
	}
	if repo.LifecyclePolicy == "" {
		return nil, &AWSError{Code: "LifecyclePolicyNotFoundException", Message: "No lifecycle policy found for repository: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	type response struct {
		LifecyclePolicyText string `json:"lifecyclePolicyText"`
		RepositoryName      string `json:"repositoryName"`
		RegistryID          string `json:"registryId"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		LifecyclePolicyText: repo.LifecyclePolicy,
		RepositoryName:      body.RepositoryName,
		RegistryID:          ctx.AccountID,
	})
}

// --- Repository policy -------------------------------------------------------

func (p *ECRPlugin) setRepositoryPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
		PolicyText     string `json:"policyText"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	data, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecr setRepositoryPolicy state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr setRepositoryPolicy unmarshal: %w", err)
	}
	repo.RepositoryPolicy = body.PolicyText

	updated, err := json.Marshal(repo)
	if err != nil {
		return nil, fmt.Errorf("ecr setRepositoryPolicy marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ecr setRepositoryPolicy state.Put: %w", err)
	}

	type response struct {
		PolicyText     string `json:"policyText"`
		RepositoryName string `json:"repositoryName"`
		RegistryID     string `json:"registryId"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		PolicyText:     body.PolicyText,
		RepositoryName: body.RepositoryName,
		RegistryID:     ctx.AccountID,
	})
}

func (p *ECRPlugin) getRepositoryPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName))
	if err != nil {
		return nil, fmt.Errorf("ecr getRepositoryPolicy state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr getRepositoryPolicy unmarshal: %w", err)
	}
	if repo.RepositoryPolicy == "" {
		return nil, &AWSError{Code: "RepositoryPolicyNotFoundException", Message: "No policy found for repository: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	type response struct {
		PolicyText     string `json:"policyText"`
		RepositoryName string `json:"repositoryName"`
		RegistryID     string `json:"registryId"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		PolicyText:     repo.RepositoryPolicy,
		RepositoryName: body.RepositoryName,
		RegistryID:     ctx.AccountID,
	})
}

func (p *ECRPlugin) deleteRepositoryPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	data, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecr deleteRepositoryPolicy state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr deleteRepositoryPolicy unmarshal: %w", err)
	}
	if repo.RepositoryPolicy == "" {
		return nil, &AWSError{Code: "RepositoryPolicyNotFoundException", Message: "No policy found for repository: " + body.RepositoryName, HTTPStatus: http.StatusNotFound}
	}

	oldPolicy := repo.RepositoryPolicy
	repo.RepositoryPolicy = ""

	updated, err := json.Marshal(repo)
	if err != nil {
		return nil, fmt.Errorf("ecr deleteRepositoryPolicy marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ecr deleteRepositoryPolicy state.Put: %w", err)
	}

	type response struct {
		PolicyText     string `json:"policyText"`
		RepositoryName string `json:"repositoryName"`
		RegistryID     string `json:"registryId"`
	}
	return ecrJSONResponse(http.StatusOK, response{
		PolicyText:     oldPolicy,
		RepositoryName: body.RepositoryName,
		RegistryID:     ctx.AccountID,
	})
}

// --- Tagging -----------------------------------------------------------------

func (p *ECRPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string            `json:"resourceArn"`
		Tags        map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	repo, stateKey, err := p.loadRepoByARN(goCtx, ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}

	if repo.Tags == nil {
		repo.Tags = make(map[string]string)
	}
	for k, v := range body.Tags {
		repo.Tags[k] = v
	}

	updated, err := json.Marshal(repo)
	if err != nil {
		return nil, fmt.Errorf("ecr tagResource marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ecr tagResource state.Put: %w", err)
	}
	return ecrJSONResponse(http.StatusOK, struct{}{})
}

func (p *ECRPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	repo, stateKey, err := p.loadRepoByARN(goCtx, ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}

	for _, k := range body.TagKeys {
		delete(repo.Tags, k)
	}

	updated, err := json.Marshal(repo)
	if err != nil {
		return nil, fmt.Errorf("ecr untagResource marshal: %w", err)
	}
	if err := p.state.Put(goCtx, ecrNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ecr untagResource state.Put: %w", err)
	}
	return ecrJSONResponse(http.StatusOK, struct{}{})
}

func (p *ECRPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string `json:"resourceArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	repo, _, err := p.loadRepoByARN(goCtx, ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}

	type response struct {
		Tags map[string]string `json:"tags"`
	}
	return ecrJSONResponse(http.StatusOK, response{Tags: repo.Tags})
}

// --- Internal helpers --------------------------------------------------------

// loadRepoByARN loads a repository by ARN, extracting the name from the ARN suffix.
func (p *ECRPlugin) loadRepoByARN(goCtx context.Context, ctx *RequestContext, arn string) (*ECRRepository, string, error) {
	// ARN format: arn:aws:ecr:{region}:{acct}:repository/{name}
	name := arn
	if idx := strings.LastIndex(arn, "/"); idx >= 0 {
		name = arn[idx+1:]
	}
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, name)
	data, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, "", fmt.Errorf("ecr loadRepoByARN state.Get: %w", err)
	}
	if data == nil {
		return nil, "", &AWSError{Code: "RepositoryNotFoundException", Message: "Repository not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, "", fmt.Errorf("ecr loadRepoByARN unmarshal: %w", err)
	}
	return &repo, stateKey, nil
}

// loadImageTagsMap reads the tag→digest map for a repository.
func (p *ECRPlugin) loadImageTagsMap(goCtx context.Context, tagsKey string) map[string]string {
	data, _ := p.state.Get(goCtx, ecrNamespace, tagsKey)
	m := make(map[string]string)
	if data != nil {
		_ = json.Unmarshal(data, &m)
	}
	return m
}

// saveImageTagsMap persists the tag→digest map for a repository.
func (p *ECRPlugin) saveImageTagsMap(goCtx context.Context, tagsKey string, m map[string]string) {
	b, _ := json.Marshal(m)
	_ = p.state.Put(goCtx, ecrNamespace, tagsKey, b)
}

// generateECRDigest creates a random sha256 digest string.
func generateECRDigest() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return fmt.Sprintf("sha256:%x", b)
}

// ecrJSONResponse marshals v as JSON and returns an AWSResponse with
// Content-Type: application/x-amz-json-1.1 and the given HTTP status code.
func ecrJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ecrJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
