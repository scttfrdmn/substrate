package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ecrCallIn is [ecrCall] against a named Region's endpoint, which is how a request reaches another
// registry of the same account.
func ecrCallIn(t *testing.T, ts *httptest.Server, region, op string, body map[string]any) (int, string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = "api.ecr." + region + ".amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// ecrRequireRefusal asserts a 400 carrying code.
func ecrRequireRefusal(t *testing.T, status int, body, code string) {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, status, "body was %s", body)
	require.Equal(t, code, ecrErrorCode(t, body), "body was %s", body)
}

// TestECR_TaggingResolvesNamespacedRepositoryARN is #1465: everything after `repository/` is the
// name, so a repository whose name contains `/` is reachable by its own ARN.
func TestECR_TaggingResolvesNamespacedRepositoryARN(t *testing.T) {
	ts := newECRTestServer(t)

	created := ecrCallOK(t, ts, "CreateRepository", map[string]any{
		"repositoryName": "ns/sub/repo",
		"tags":           []map[string]string{{"Key": "k", "Value": "v"}},
	})
	var out struct {
		Repository struct {
			RepositoryArn string `json:"repositoryArn"`
		} `json:"repository"`
	}
	require.NoError(t, json.Unmarshal([]byte(created), &out))
	arn := out.Repository.RepositoryArn
	require.True(t, strings.HasSuffix(arn, ":repository/ns/sub/repo"), arn)

	// A repository named by the last segment alone must not be what the ARN resolves to.
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "repo"})

	assert.Contains(t, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{"resourceArn": arn}), `{"Key":"k","Value":"v"}`)
	ecrCallOK(t, ts, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]string{{"Key": "owner", "Value": "aegc"}},
	})
	listed := ecrCallOK(t, ts, "ListTagsForResource", map[string]any{"resourceArn": arn})
	assert.Contains(t, listed, `"owner"`)
	ecrCallOK(t, ts, "UntagResource", map[string]any{"resourceArn": arn, "tagKeys": []string{"k"}})
	assert.Equal(t, `{"tags":[{"Key":"owner","Value":"aegc"}]}`, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{"resourceArn": arn}))

	// And the last-segment repository was never touched.
	assert.Equal(t, `{"tags":[]}`, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/repo",
	}))
}

// TestECR_RepositoryARNIsResolvedInTheRegistryItNames is #1113: the ARN's own account and Region
// decide which registry is read, and a string that is not an ECR repository ARN is refused.
func TestECR_RepositoryARNIsResolvedInTheRegistryItNames(t *testing.T) {
	ts := newECRTestServer(t)

	// "prod" exists in the caller's us-east-1 registry and in its us-west-2 registry, with
	// different tags, so a resolution in the wrong registry is visible in the answer.
	ecrCallOK(t, ts, "CreateRepository", map[string]any{
		"repositoryName": "prod",
		"tags":           []map[string]string{{"Key": "where", "Value": "east"}},
	})
	status, body := ecrCallIn(t, ts, "us-west-2", "CreateRepository", map[string]any{
		"repositoryName": "prod",
		"tags":           []map[string]string{{"Key": "where", "Value": "west"}},
	})
	require.Equal(t, http.StatusOK, status, body)

	t.Run("cross-Region ARN reads the Region it names", func(t *testing.T) {
		got := ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
			"resourceArn": "arn:aws:ecr:us-west-2:123456789012:repository/prod",
		})
		assert.Contains(t, got, `"west"`, "presented in us-east-1, answered from us-west-2")
	})

	t.Run("same-registry ARN is served as before", func(t *testing.T) {
		got := ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
			"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/prod",
		})
		assert.Contains(t, got, `"east"`)
	})

	t.Run("cross-account ARN is not served from the caller's registry", func(t *testing.T) {
		for _, op := range []string{"ListTagsForResource", "TagResource", "UntagResource"} {
			status, body := ecrCall(t, ts, op, map[string]any{
				"resourceArn": "arn:aws:ecr:us-east-1:999999999999:repository/prod",
				"tags":        []map[string]string{{"Key": "x", "Value": "y"}},
				"tagKeys":     []string{"where"},
			})
			ecrRequireRefusal(t, status, body, "RepositoryNotFoundException")
		}
		// The caller's own "prod" kept its tag through all three refusals.
		assert.Contains(t, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
			"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/prod",
		}), `"east"`)
	})

	for _, tc := range []struct {
		name string
		arn  string
	}{
		{"a Lambda ARN", "arn:aws:lambda:us-east-1:123456789012:function:prod"},
		{"a bare name", "prod"},
		{"repository with no name", "arn:aws:ecr:us-east-1:123456789012:repository/"},
		{"another ECR resource type", "arn:aws:ecr:us-east-1:123456789012:registry/prod"},
		{"a short account", "arn:aws:ecr:us-east-1:1234:repository/prod"},
		{"no Region", "arn:aws:ecr::123456789012:repository/prod"},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			status, body := ecrCall(t, ts, "ListTagsForResource", map[string]any{"resourceArn": tc.arn})
			ecrRequireRefusal(t, status, body, "InvalidParameterException")
		})
	}
}
