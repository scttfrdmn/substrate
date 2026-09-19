package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// Amazon ECR refusal constructors (#1090).
//
// Every ECR operation page publishes exactly one non-400 status — ServerException at 500,
// which substrate never answers — so every refusal an ECR handler can reach is a 400. That
// was read off the pages rather than generalised from one of them (#671): API_CreateRepository,
// API_DescribeRepositories, API_DeleteRepository, API_GetLifecyclePolicy,
// API_GetRepositoryPolicy, API_ListImages, API_DescribeImages, API_BatchGetImage and
// API_BatchDeleteImage each publish their errors at 400 and nothing else below 500.
//
// Substrate answered four of its five ECR codes at the wrong status: RepositoryAlreadyExists
// at 409 and the three NotFound codes at 404, twelve sites in all. The status is the part a
// consumer branches on before it has parsed a body — an SDK's retry classifier reads it, and
// a 409 is what CFN's own create-exists probe looks for — so the codes were right and every
// caller keying on the status was told something the service never says.
//
// The messages are substrate's own: no ECR page publishes a message string for any of these,
// and the published `message` member is documented only as "The error message associated with
// the exception".

// ecrRepositoryNotFound reports that no repository of that name exists in the registry.
//
// Published at 400 on every ECR operation that names a repository. Substrate answered 404.
func ecrRepositoryNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "RepositoryNotFoundException",
		Message:    "Repository not found: " + name,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrRepositoryAlreadyExists reports that the name is taken.
//
// API_CreateRepository publishes RepositoryAlreadyExistsException at 400. Substrate answered
// 409, which is the status a consumer would reasonably treat as "already there, carry on" —
// and `cfnCreateExistsCodes` (cfn_rollback.go) keys on the code rather than the status, so the
// change is invisible to it.
func ecrRepositoryAlreadyExists(name string) *AWSError {
	return &AWSError{
		Code:       "RepositoryAlreadyExistsException",
		Message:    "Repository already exists: " + name,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrLifecyclePolicyNotFound reports that the repository has no lifecycle policy.
//
// API_GetLifecyclePolicy and API_DeleteLifecyclePolicy publish
// LifecyclePolicyNotFoundException at 400. Substrate answered 404.
func ecrLifecyclePolicyNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "LifecyclePolicyNotFoundException",
		Message:    "No lifecycle policy found for repository: " + name,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrRepositoryPolicyNotFound reports that the repository has no policy.
//
// API_GetRepositoryPolicy and API_DeleteRepositoryPolicy publish
// RepositoryPolicyNotFoundException at 400. Substrate answered 404.
func ecrRepositoryPolicyNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "RepositoryPolicyNotFoundException",
		Message:    "No policy found for repository: " + name,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrRepositoryNotEmpty reports that the repository holds images and force was not set.
//
// API_DeleteRepository publishes RepositoryNotEmptyException at 400, and its `force` entry is
// the condition: deletion proceeds on a repository that contains images only when force is
// true. Substrate decoded `force` and never read it, so the refusal could not fire and a
// repository full of images was deleted silently.
func ecrRepositoryNotEmpty(name string) *AWSError {
	return &AWSError{
		Code:       "RepositoryNotEmptyException",
		Message:    "The repository with name '" + name + "' in registry cannot be deleted because it still contains images",
		HTTPStatus: http.StatusBadRequest,
	}
}

// requireRepository reports the published refusal when name addresses no repository in the
// caller's registry, and nil when it does.
//
// The four image operations — ListImages, DescribeImages, BatchGetImage and BatchDeleteImage —
// each publish RepositoryNotFoundException and each read the repository's tag index without
// ever looking at the repository record, so all four answered 200 with an empty result for a
// name that exists nowhere. A caller distinguishing "no images yet" from "no such repository"
// could not, and the published refusal had no site to fire from.
//
// The returned error is an *AWSError for a missing repository and a wrapped state error
// otherwise, so a caller can return it unexamined.
func (p *ECRPlugin) requireRepository(goCtx context.Context, ctx *RequestContext, name string) error {
	data, err := p.state.Get(goCtx, ecrNamespace, ecrRepoKey(ctx.AccountID, ctx.Region, name))
	if err != nil {
		return fmt.Errorf("ecr requireRepository state.Get: %w", err)
	}
	if data == nil {
		return ecrRepositoryNotFound(name)
	}
	return nil
}
