package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every ECR refusal is a 400, and three of them can now be reached at all (#1090).
//
// Substrate answered four of its five ECR codes at a status no ECR page publishes —
// RepositoryAlreadyExistsException at 409 and the three NotFound codes at 404, twelve sites —
// and two published refusals had no site to fire from: RepositoryNotFoundException on the four
// image operations and on a named DescribeRepositories, and RepositoryNotEmptyException on
// DeleteRepository, whose `force` member was decoded and never read.
//
// The statuses are asserted over the wire rather than through HandleRequest, because the status
// is what an SDK's retry classifier and CloudFormation's create-exists probe read before any
// body is parsed — and because ecr_plugin_test.go asserts `err != nil` and a code and never a
// status, which is why 409 and 404 survived. Every case pairs the status with the code, so a
// status corrected by answering a different code would not pass.

// ecrErrorStatus makes one call that must be refused and returns its status and code.
func ecrErrorStatus(t *testing.T, ts *httptest.Server, op string, body map[string]any) (int, string) {
	t.Helper()
	status, raw := ecrCall(t, ts, op, body)
	require.NotEqual(t, http.StatusOK, status, "%s was not refused: %s", op, raw)
	return status, ecrErrorCode(t, raw)
}

// TestECR_EveryRefusalIsFourHundred walks the five codes substrate answers, one case per code,
// and requires the published status.
//
// The five pages were each read for this: API_CreateRepository, API_DescribeRepositories,
// API_DeleteRepository, API_GetLifecyclePolicy and API_GetRepositoryPolicy publish their errors
// at 400, and the only non-400 status any ECR page publishes is ServerException at 500, which
// substrate never answers.
func TestECR_EveryRefusalIsFourHundred(t *testing.T) {
	ts := newECRTestServer(t)

	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "statuses"})

	for _, tc := range []struct {
		name      string
		operation string
		body      map[string]any
		code      string
	}{
		{
			// 409 before this change, which is the status a consumer reads as "already there,
			// carry on" — and the one CFN's own create-exists probe looks for.
			name:      "a name that is taken",
			operation: "CreateRepository",
			body:      map[string]any{"repositoryName": "statuses"},
			code:      "RepositoryAlreadyExistsException",
		},
		{
			name:      "a repository that does not exist",
			operation: "PutLifecyclePolicy",
			body:      map[string]any{"repositoryName": "absent", "lifecyclePolicyText": "{}"},
			code:      "RepositoryNotFoundException",
		},
		{
			name:      "a repository with no lifecycle policy",
			operation: "GetLifecyclePolicy",
			body:      map[string]any{"repositoryName": "statuses"},
			code:      "LifecyclePolicyNotFoundException",
		},
		{
			name:      "a repository with no policy",
			operation: "GetRepositoryPolicy",
			body:      map[string]any{"repositoryName": "statuses"},
			code:      "RepositoryPolicyNotFoundException",
		},
		{
			name:      "a missing required member, which was already right",
			operation: "DeleteRepository",
			body:      map[string]any{},
			code:      "InvalidParameterException",
		},
		// The remaining RepositoryNotFoundException sites, one per handler, because the status was
		// wrong at every one of them independently and a table that walked only one would have
		// left the other seven to be corrected by inspection.
		{
			name:      "a push to a repository that does not exist",
			operation: "PutImage",
			body: map[string]any{
				"repositoryName": "absent",
				"imageManifest":  `{"schemaVersion":2}`,
			},
			code: "RepositoryNotFoundException",
		},
		{
			name:      "a policy set on a repository that does not exist",
			operation: "SetRepositoryPolicy",
			body:      map[string]any{"repositoryName": "absent", "policyText": "{}"},
			code:      "RepositoryNotFoundException",
		},
		{
			name:      "a policy read from a repository that does not exist",
			operation: "GetRepositoryPolicy",
			body:      map[string]any{"repositoryName": "absent"},
			code:      "RepositoryNotFoundException",
		},
		{
			name:      "a policy deleted from a repository that does not exist",
			operation: "DeleteRepositoryPolicy",
			body:      map[string]any{"repositoryName": "absent"},
			code:      "RepositoryNotFoundException",
		},
		{
			name:      "a policy deleted from a repository that has none",
			operation: "DeleteRepositoryPolicy",
			body:      map[string]any{"repositoryName": "statuses"},
			code:      "RepositoryPolicyNotFoundException",
		},
		{
			// The tagging doors reach the repository through an ARN rather than a name, which is a
			// separate lookup (loadRepoByARN) and so was a separate 404.
			name:      "a tag listing for an ARN naming no repository",
			operation: "ListTagsForResource",
			body: map[string]any{
				"resourceArn": "arn:aws:ecr:us-east-1:000000000000:repository/absent",
			},
			code: "RepositoryNotFoundException",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ecrErrorStatus(t, ts, tc.operation, tc.body)
			assert.Equal(t, http.StatusBadRequest, status,
				"%s answers %s, which every ECR page publishes at 400", tc.operation, tc.code)
			assert.Equal(t, tc.code, code)
		})
	}
}

// TestECR_AnUnknownRepositoryIsRefusedByEveryImageOperation is the first of the two refusals
// that could not fire.
//
// All four operations read the repository's tag index and never the repository record, so the
// index for a name that addresses nothing read as empty and each answered 200 with an empty
// result — indistinguishable from a repository that exists and holds no images. All four
// publish RepositoryNotFoundException.
func TestECR_AnUnknownRepositoryIsRefusedByEveryImageOperation(t *testing.T) {
	ts := newECRTestServer(t)

	// The anchor: the same four operations answer 200 for a repository that does exist and is
	// empty, so the refusal below is about the repository and not about the emptiness.
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "real"})

	// rest is whatever the operation needs besides the repository name; imageIds is
	// Required: Yes on the two batch operations.
	for _, tc := range []struct {
		operation string
		rest      map[string]any
	}{
		{operation: "ListImages"},
		{operation: "DescribeImages"},
		{operation: "BatchGetImage", rest: map[string]any{
			"imageIds": []map[string]string{{"imageTag": "latest"}},
		}},
		{operation: "BatchDeleteImage", rest: map[string]any{
			"imageIds": []map[string]string{{"imageTag": "latest"}},
		}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			call := func(name string) map[string]any {
				body := map[string]any{"repositoryName": name}
				for k, v := range tc.rest {
					body[k] = v
				}
				return body
			}

			ecrCallOK(t, ts, tc.operation, call("real"))

			status, code := ecrErrorStatus(t, ts, tc.operation, call("imaginary"))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "RepositoryNotFoundException", code,
				"%s answered 200 with an empty result for a repository that exists nowhere",
				tc.operation)
		})
	}
}

// TestECR_DescribeRepositoriesRefusesANameItCannotAnswerFor pins the fourth defect: a name the
// caller supplied was looked up and a miss was dropped from the list.
//
// The mixed case is the one that shows what a consumer saw — one real name and one imaginary
// one answered 200 with a single entry, so a caller checking "did I get what I asked for?" had
// to compare lengths. A name read out of substrate's own index is still skipped, which is what
// the registry-wide case asserts.
func TestECR_DescribeRepositoriesRefusesANameItCannotAnswerFor(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "present"})

	status, code := ecrErrorStatus(t, ts, "DescribeRepositories",
		map[string]any{"repositoryNames": []string{"absent"}})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "RepositoryNotFoundException", code)

	status, code = ecrErrorStatus(t, ts, "DescribeRepositories",
		map[string]any{"repositoryNames": []string{"present", "absent"}})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "RepositoryNotFoundException", code,
		"a short list is not an answer to a request naming two repositories")

	body := ecrCallOK(t, ts, "DescribeRepositories", map[string]any{})
	assert.Contains(t, body, `"repositoryName":"present"`,
		"the registry-wide form still answers, and names nothing the caller supplied")
}

// TestECR_ANonEmptyRepositoryNeedsForce is the second refusal that could not fire.
//
// API_DeleteRepository publishes RepositoryNotEmptyException, and its `force` entry is the
// condition. Substrate decoded `force` into a field nothing read, so a repository holding
// images was deleted silently — the worst shape for this defect, since the caller's mistake was
// the one the refusal exists to prevent.
func TestECR_ANonEmptyRepositoryNeedsForce(t *testing.T) {
	ts := newECRTestServer(t)

	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "full"})
	ecrCallOK(t, ts, "PutImage", map[string]any{
		"repositoryName": "full",
		"imageTag":       "latest",
		"imageManifest":  `{"schemaVersion":2}`,
	})

	status, code := ecrErrorStatus(t, ts, "DeleteRepository", map[string]any{"repositoryName": "full"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "RepositoryNotEmptyException", code)

	// The repository survived the refusal, which is the half a caller retrying with force
	// depends on.
	assert.Contains(t, ecrCallOK(t, ts, "DescribeRepositories",
		map[string]any{"repositoryNames": []string{"full"}}), `"repositoryName":"full"`)

	ecrCallOK(t, ts, "DeleteRepository", map[string]any{"repositoryName": "full", "force": true})

	// And the images went with it. The tag index used to outlive the repository, so a name
	// re-created after a delete reported the previous repository's images.
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "full"})
	assert.NotContains(t, ecrCallOK(t, ts, "ListImages", map[string]any{"repositoryName": "full"}),
		"latest", "a re-created repository starts empty")
}

// TestECR_AnEmptyRepositoryDeletesWithoutForce keeps the nominal path pinned, since the guard
// above is the first thing in this plugin that can refuse a delete.
func TestECR_AnEmptyRepositoryDeletesWithoutForce(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "empty"})
	assert.Contains(t, ecrCallOK(t, ts, "DeleteRepository", map[string]any{"repositoryName": "empty"}),
		`"repositoryName":"empty"`)
}
