package emulator

import (
	"context"
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
		return nil, unknownActionError(p.Name(), op)
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
		RepositoryName             string   `json:"repositoryName"`
		Tags                       []ecrTag `json:"tags"`
		ImageTagMutability         string   `json:"imageTagMutability"`
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

	// The tag set is folded before any state is read, so a malformed entry is refused whether or not
	// the repository name is free — the shape of the request does not depend on what is stored.
	tags, awsErr := ecrTagsToMap(body.Tags)
	if awsErr != nil {
		return nil, awsErr
	}

	// Checked here for the same reason, and refused rather than silently corrected: the member is
	// reported back on all three repository responses, so accepting an unpublished value would put
	// it on the wire under a name whose Valid Values are published (#1090). The code is the page's
	// own InvalidParameterException, whose gloss — "The specified parameter is invalid. Review the
	// available parameters for the API request." — is this case.
	mutability := body.ImageTagMutability
	if mutability == "" {
		mutability = ecrImageTagMutabilityDefault
	}
	if !ecrImageTagMutabilityValues[mutability] {
		return nil, &AWSError{
			Code: "InvalidParameterException",
			Message: "Invalid parameter at 'imageTagMutability' failed to satisfy constraint: " +
				"'Member must satisfy enum value set: " +
				"[MUTABLE, IMMUTABLE, IMMUTABLE_WITH_EXCLUSION, MUTABLE_WITH_EXCLUSION]'",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	stateKey := ecrRepoKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	existing, err := p.state.Get(goCtx, ecrNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ecr createRepository state.Get: %w", err)
	}
	if existing != nil {
		return nil, ecrRepositoryAlreadyExists(body.RepositoryName)
	}

	encType := body.EncryptionConfiguration.EncryptionType
	if encType == "" {
		encType = "AES256"
	}

	repo := ECRRepository{
		RepositoryName:     body.RepositoryName,
		RepositoryArn:      fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", ctx.Region, ctx.AccountID, body.RepositoryName),
		RegistryID:         ctx.AccountID,
		RepositoryURI:      fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", ctx.AccountID, ctx.Region, body.RepositoryName),
		CreatedAt:          p.tc.Now(),
		ImageTagMutability: mutability,
		Tags:               tags,
		AccountID:          ctx.AccountID,
		Region:             ctx.Region,
		// Set where the fact becomes true: this repository has carried a tag. No observation depends
		// on it today — every remover recomputes the flag from the count it saw before deleting, so
		// UntagResource (:945) and the Resource Groups Tagging API's untag arm
		// (tagging_plugin.go:2119) each stamp it themselves — which is why deleting this line breaks
		// no test. It is here so #938's rule (a resource that has been tagged stays reported with an
		// empty tag set) rests on the flag meaning what it says rather than on every future remover
		// remembering to derive it. The line was unreachable before #1017, when a create carrying
		// tags answered 400.
		EverTagged: taggingEverTagged(false, 0, len(tags)),
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
		Repository ecrRepositoryOut `json:"repository"`
	}
	return ecrJSONResponse(http.StatusOK, response{Repository: ecrRepositoryToWire(repo)})
}

func (p *ECRPlugin) describeRepositories(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		RepositoryNames []string `json:"repositoryNames"`
		MaxResults      int      `json:"maxResults"`
		NextToken       string   `json:"nextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ecrInvalidBody()
		}
	}

	goCtx := context.Background()

	// A name the caller asked for is answered for or refused; a name read out of the index is
	// one substrate wrote itself, so a missing record there is an inconsistency to skip rather
	// than a caller's mistake. AWS publishes RepositoryNotFoundException on this operation and
	// it had no site to fire from: every name was looked up and a miss was dropped from the
	// list, so a request naming one real and one imaginary repository answered 200 with one
	// entry (#1090).
	named := len(body.RepositoryNames) > 0

	// API_DescribeRepositories publishes the same sentence on both pagination members: "This
	// option cannot be used when you specify repositories with `repositoryNames`." So a named
	// request is unpaginated and combining the two is refused rather than one silently winning
	// (#1090). The other two ECR listings exclude different members, or none — see
	// ecr_pagination.go.
	if named {
		if body.MaxResults != 0 {
			return nil, ecrPageExcluded("maxResults", "repositories with repositoryNames")
		}
		if body.NextToken != "" {
			return nil, ecrPageExcluded("nextToken", "repositories with repositoryNames")
		}
	}

	pageReq, err := ecrDecodePage(body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}

	var names []string
	if named {
		names = body.RepositoryNames
	} else {
		idxKey := ecrRepoNamesKey(ctx.AccountID, ctx.Region)
		names, err = loadStringIndex(goCtx, p.state, ecrNamespace, idxKey)
		if err != nil {
			return nil, fmt.Errorf("ecr describeRepositories loadIndex: %w", err)
		}
		// The offset cursor below is only meaningful over a stable order, and the names index is
		// in creation order — so a repository created between two pages shifted every later
		// repository by one. Sorting by name is the same basis StateManager.List guarantees
		// (#865), and it is the caller-side obligation pageByOffsetToken states.
		sort.Strings(names)
	}

	repos := make([]ECRRepository, 0, len(names))
	for _, name := range names {
		data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, name))
		if err != nil {
			return nil, fmt.Errorf("ecr describeRepositories state.Get: %w", err)
		}
		if data == nil {
			if named {
				return nil, ecrRepositoryNotFound(name)
			}
			continue
		}
		var repo ECRRepository
		if err := json.Unmarshal(data, &repo); err != nil {
			return nil, fmt.Errorf("ecr describeRepositories unmarshal: %w", err)
		}
		repos = append(repos, repo)
	}

	type response struct {
		Repositories []ecrRepositoryOut `json:"repositories"`
		NextToken    string             `json:"nextToken,omitempty"`
	}

	// The named form answers every repository it was asked for, because both pagination members
	// are excluded from it. Only the registry-wide form pages. The cut is over the built list
	// rather than over the names, so an index entry with no record — an internal inconsistency
	// the loop above skips — shortens the listing rather than the page.
	if named {
		return ecrJSONResponse(http.StatusOK, response{Repositories: ecrRepositoriesToWire(repos)})
	}
	page, next := pageByOffsetToken(repos, pageReq.offset, pageReq.pageSize)
	return ecrJSONResponse(http.StatusOK, response{
		Repositories: ecrRepositoriesToWire(page),
		NextToken:    next,
	})
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr deleteRepository unmarshal: %w", err)
	}

	// force is the one request member here that governs whether the delete happens at all, and
	// it was decoded and never read (#1090). A repository's contents are its tag index: an
	// image pushed without a tag is written under its digest and entered in no index, so
	// nothing in this plugin can enumerate it — every image operation reads the tag map — and
	// emptiness is measured the same way the operations that report contents measure it.
	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)
	if len(tagsMap) > 0 && !body.Force {
		return nil, ecrRepositoryNotEmpty(body.RepositoryName)
	}

	if err := p.state.Delete(goCtx, ecrNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("ecr deleteRepository state.Delete: %w", err)
	}

	// The images go with it. Without this the tag index outlived the repository, so a name
	// re-created after a forced delete reported the previous repository's images.
	for _, digest := range tagsMap {
		imgKey := ecrImageKey(ctx.AccountID, ctx.Region, body.RepositoryName, digest)
		if err := p.state.Delete(goCtx, ecrNamespace, imgKey); err != nil {
			return nil, fmt.Errorf("ecr deleteRepository state.Delete image: %w", err)
		}
	}
	if len(tagsMap) > 0 {
		if err := p.state.Delete(goCtx, ecrNamespace, tagsKey); err != nil {
			return nil, fmt.Errorf("ecr deleteRepository state.Delete tags: %w", err)
		}
	}

	idxKey := ecrRepoNamesKey(ctx.AccountID, ctx.Region)
	removeFromStringIndex(goCtx, p.state, ecrNamespace, idxKey, body.RepositoryName)

	type response struct {
		Repository ecrRepositoryOut `json:"repository"`
	}
	return ecrJSONResponse(http.StatusOK, response{Repository: ecrRepositoryToWire(repo)})
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
	}

	digest := body.ImageDigest
	if digest == "" {
		digest = generateECRDigest(ctx.IDs)
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

	// Verify repository exists. Without this the tag index for a name that addresses no
	// repository reads as empty and the operation answers 200 (#1090).
	if err := p.requireRepository(goCtx, ctx, body.RepositoryName); err != nil {
		return nil, err
	}

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
		MaxResults int    `json:"maxResults"`
		NextToken  string `json:"nextToken"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	// API_DescribeImages publishes, on both pagination members, "This option cannot be used when
	// you specify images with `imageIds`." That is a different excluded member from
	// DescribeRepositories', and ListImages publishes no exclusion at all, which is why the three
	// shapes are declared per operation (#1090, see ecr_pagination.go).
	enumerated := len(body.ImageIDs) > 0
	if enumerated {
		if body.MaxResults != 0 {
			return nil, ecrPageExcluded("maxResults", "images with imageIds")
		}
		if body.NextToken != "" {
			return nil, ecrPageExcluded("nextToken", "images with imageIds")
		}
	}
	pageReq, err := ecrDecodePage(body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()

	// Verify repository exists. Without this the tag index for a name that addresses no
	// repository reads as empty and the operation answers 200 (#1090).
	if err := p.requireRepository(goCtx, ctx, body.RepositoryName); err != nil {
		return nil, err
	}

	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)

	// Collect requested digests.
	//
	// API_DescribeImages publishes ImageNotFoundException/400 — "The image requested does not
	// exist in the specified repository" — and it had no site to fire from: a tag that resolved
	// to nothing was dropped from the request and a digest naming no record was dropped from the
	// answer, so a caller naming one real and one imaginary image got 200 and a short list. That
	// is the same shape as the DescribeRepositories defect one member up (#1090). It applies only
	// to the enumerated form; the registry-wide form names no image and so cannot miss one.
	var requestedDigests []string
	if enumerated {
		for _, id := range body.ImageIDs {
			switch {
			case id.ImageDigest != "":
				requestedDigests = append(requestedDigests, id.ImageDigest)
			case id.ImageTag != "":
				d, ok := tagsMap[id.ImageTag]
				if !ok {
					return nil, ecrImageNotFound(body.RepositoryName)
				}
				requestedDigests = append(requestedDigests, d)
			}
		}
	} else {
		// All images: one entry per digest, however many tags point at it, because ImageDetail
		// publishes imageTags as an array. Ranging over the tag map puts them in Go's randomized
		// map order, so two identical calls could answer the same images in a different order and
		// no offset cursor over them would mean anything; sorting by digest is the stable basis
		// pageByOffsetToken requires of its caller.
		seen := make(map[string]bool)
		for _, d := range tagsMap {
			if !seen[d] {
				seen[d] = true
				requestedDigests = append(requestedDigests, d)
			}
		}
		sort.Strings(requestedDigests)
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
			// A digest the caller named is refused; one derived from the tag map without a record
			// behind it is substrate's own inconsistency, which is skipped for the same reason
			// describeRepositories skips an index entry with no record.
			if enumerated {
				return nil, ecrImageNotFound(body.RepositoryName)
			}
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
		NextToken    string        `json:"nextToken,omitempty"`
	}

	// The enumerated form answers every image it was asked for, because both pagination members
	// are excluded from it.
	if enumerated {
		return ecrJSONResponse(http.StatusOK, response{ImageDetails: details})
	}
	page, next := pageByOffsetToken(details, pageReq.offset, pageReq.pageSize)
	return ecrJSONResponse(http.StatusOK, response{ImageDetails: page, NextToken: next})
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

	// Verify repository exists. Without this the tag index for a name that addresses no
	// repository reads as empty and the operation answers 200 (#1090).
	if err := p.requireRepository(goCtx, ctx, body.RepositoryName); err != nil {
		return nil, err
	}

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
		MaxResults     int    `json:"maxResults"`
		NextToken      string `json:"nextToken"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.RepositoryName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "repositoryName is required", HTTPStatus: http.StatusBadRequest}
	}

	// API_ListImages publishes no exclusion sentence on either pagination member — the third of
	// the three shapes (#1090). It names no images to be excluded against: its only narrowing
	// member is `filter`, which selects rather than enumerates, so a filtered listing still pages.
	pageReq, err := ecrDecodePage(body.MaxResults, body.NextToken)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()

	// Verify repository exists. Without this the tag index for a name that addresses no
	// repository reads as empty and the operation answers 200 (#1090).
	if err := p.requireRepository(goCtx, ctx, body.RepositoryName); err != nil {
		return nil, err
	}

	tagsKey := ecrImageTagsKey(ctx.AccountID, ctx.Region, body.RepositoryName)
	tagsMap := p.loadImageTagsMap(goCtx, tagsKey)

	type imageID struct {
		ImageDigest string `json:"imageDigest"`
		ImageTag    string `json:"imageTag,omitempty"`
	}

	// One entry per image ID — that is, per digest-and-tag pair — not per digest.
	//
	// Substrate de-duplicated by digest and kept whichever tag the map yielded first, so an image
	// carrying two tags was reported under one of them, chosen by Go's randomized map order: the
	// same repository listed twice could answer two different tags. AWS's own published sample for
	// this operation answers two entries with the same digest and different tags, and the page's
	// prose says a TAGGED filter lists "all of the tags in your repository", so the published
	// listing is over image IDs rather than over images. Sorting by digest then tag is also the
	// stable order pageByOffsetToken requires of its caller.
	ids := make([]imageID, 0, len(tagsMap))
	for tag, digest := range tagsMap {
		ids = append(ids, imageID{ImageDigest: digest, ImageTag: tag})
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].ImageDigest != ids[j].ImageDigest {
			return ids[i].ImageDigest < ids[j].ImageDigest
		}
		return ids[i].ImageTag < ids[j].ImageTag
	})

	type response struct {
		ImageIDs  []imageID `json:"imageIds"`
		NextToken string    `json:"nextToken,omitempty"`
	}
	page, next := pageByOffsetToken(ids, pageReq.offset, pageReq.pageSize)
	return ecrJSONResponse(http.StatusOK, response{ImageIDs: page, NextToken: next})
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr getLifecyclePolicy unmarshal: %w", err)
	}
	if repo.LifecyclePolicy == "" {
		return nil, ecrLifecyclePolicyNotFound(body.RepositoryName)
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr getRepositoryPolicy unmarshal: %w", err)
	}
	if repo.RepositoryPolicy == "" {
		return nil, ecrRepositoryPolicyNotFound(body.RepositoryName)
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
		return nil, ecrRepositoryNotFound(body.RepositoryName)
	}

	var repo ECRRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("ecr deleteRepositoryPolicy unmarshal: %w", err)
	}
	if repo.RepositoryPolicy == "" {
		return nil, ecrRepositoryPolicyNotFound(body.RepositoryName)
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
		ResourceArn string   `json:"resourceArn"`
		Tags        []ecrTag `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	// Folded before the repository is loaded, as in createRepository: the request's own shape is
	// decidable without state.
	tags, awsErr := ecrTagsToMap(body.Tags)
	if awsErr != nil {
		return nil, awsErr
	}

	goCtx := context.Background()
	repo, stateKey, err := p.loadRepoByARN(goCtx, ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}

	repo.EverTagged = taggingEverTagged(repo.EverTagged, len(repo.Tags), len(tags))
	if repo.Tags == nil {
		repo.Tags = make(map[string]string)
	}
	for k, v := range tags {
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

	repo.EverTagged = taggingEverTagged(repo.EverTagged, len(repo.Tags), 0)
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
		Tags []ecrTag `json:"tags"`
	}
	return ecrJSONResponse(http.StatusOK, response{Tags: ecrTagList(repo.Tags)})
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
		return nil, "", ecrRepositoryNotFound(name)
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

// generateECRDigest mints a sha256-shaped image digest from m, for a PutImage that supplied no
// `imageDigest` of its own.
//
// A real digest is the SHA-256 of the image manifest, so ECR computes the same one for two pushes
// of identical manifest bytes and treats the second as the same image. Substrate mints an
// unrelated value instead, which is why every image it stores is distinct even when the manifests
// are byte-identical; #1283 tracks deriving it from the manifest, which is a change to what the
// digest *means* rather than to where its bytes come from and so is not #856's to make.
func generateECRDigest(m *IDMint) string {
	return "sha256:" + m.Hex(32)
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
