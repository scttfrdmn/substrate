package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestECR_PullThroughCacheRuleLifecycle drives the five rule operations over the wire, asserting
// each response's raw members against its own page's Response Syntax.
func TestECR_PullThroughCacheRuleLifecycle(t *testing.T) {
	ts := newECRTestServer(t)

	created := ecrCallOK(t, ts, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "aegc-quay",
		"upstreamRegistryUrl": "quay.io",
	})
	assert.Contains(t, created, `"upstreamRegistry":"quay"`, "the registry name is derived from the URL")
	assert.Contains(t, created, `"registryId":"123456789012"`)
	assert.Regexp(t, `"createdAt":[0-9]`, created, "awsJson1_1 timestamps are epoch seconds")
	assert.NotContains(t, created, "updatedAt", "API_CreatePullThroughCacheRule publishes no updatedAt")

	// The same prefix, with or without the assumed trailing "/", is the same rule.
	for _, prefix := range []string{"aegc-quay", "aegc-quay/"} {
		status, body := ecrCall(t, ts, "CreatePullThroughCacheRule", map[string]any{
			"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io",
		})
		ecrRequireRefusal(t, status, body, "PullThroughCacheRuleAlreadyExistsException")
	}

	ecrCallOK(t, ts, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"upstreamRegistryUrl": "registry-1.docker.io",
		"credentialArn":       "arn:aws:secretsmanager:us-east-1:123456789012:secret:ecr-pullthroughcache/dockerhub",
	})

	described := ecrCallOK(t, ts, "DescribePullThroughCacheRules", map[string]any{})
	assert.Less(t, strings.Index(described, `"aegc-quay"`), strings.Index(described, `"docker-hub"`), "listed in prefix order")
	assert.Contains(t, described, `"updatedAt":`, "API_PullThroughCacheRule publishes updatedAt")

	filtered := ecrCallOK(t, ts, "DescribePullThroughCacheRules", map[string]any{"ecrRepositoryPrefixes": []string{"docker-hub"}})
	assert.NotContains(t, filtered, "aegc-quay")
	assert.Contains(t, filtered, `"upstreamRegistry":"docker-hub"`)

	status, body := ecrCall(t, ts, "DescribePullThroughCacheRules", map[string]any{"ecrRepositoryPrefixes": []string{"nope"}})
	ecrRequireRefusal(t, status, body, "PullThroughCacheRuleNotFoundException")

	page := ecrCallOK(t, ts, "DescribePullThroughCacheRules", map[string]any{"maxResults": 1})
	var paged struct {
		Rules     []map[string]any `json:"pullThroughCacheRules"`
		NextToken string           `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal([]byte(page), &paged))
	require.Len(t, paged.Rules, 1)
	require.NotEmpty(t, paged.NextToken)
	second := ecrCallOK(t, ts, "DescribePullThroughCacheRules", map[string]any{"maxResults": 1, "nextToken": paged.NextToken})
	assert.Contains(t, second, "docker-hub")
	assert.NotContains(t, second, "nextToken")

	updated := ecrCallOK(t, ts, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"credentialArn":       "arn:aws:secretsmanager:us-east-1:123456789012:secret:ecr-pullthroughcache/rotated",
	})
	assert.Contains(t, updated, "ecr-pullthroughcache/rotated")
	assert.Contains(t, updated, `"updatedAt":`)
	for _, member := range []string{"createdAt", "upstreamRegistry"} {
		assert.NotContains(t, updated, `"`+member+`"`, "API_UpdatePullThroughCacheRule publishes no %s", member)
	}

	validated := ecrCallOK(t, ts, "ValidatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "docker-hub"})
	assert.Contains(t, validated, `"isValid":true`, "the nominal outcome")
	assert.NotContains(t, validated, "failure")

	deleted := ecrCallOK(t, ts, "DeletePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "docker-hub"})
	assert.Contains(t, deleted, `"upstreamRegistryUrl":"registry-1.docker.io"`)
	assert.NotContains(t, deleted, `"upstreamRegistry"`, "API_DeletePullThroughCacheRule publishes no upstreamRegistry")

	for _, op := range []string{"DeletePullThroughCacheRule", "UpdatePullThroughCacheRule", "ValidatePullThroughCacheRule"} {
		status, body := ecrCall(t, ts, op, map[string]any{"ecrRepositoryPrefix": "docker-hub"})
		ecrRequireRefusal(t, status, body, "PullThroughCacheRuleNotFoundException")
	}

	for _, member := range []string{"AccountID", "Region"} {
		for _, b := range []string{created, described, updated, validated, deleted} {
			assert.NotContains(t, b, `"`+member+`"`, "substrate's bookkeeping must not reach the wire")
		}
	}
}

// TestECR_PullThroughCacheRuleRefusals pins each refusal CreatePullThroughCacheRule can reach.
func TestECR_PullThroughCacheRuleRefusals(t *testing.T) {
	ts := newECRTestServer(t)
	const secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:ecr-pullthroughcache/creds"

	for _, tc := range []struct {
		name string
		body map[string]any
		code string
	}{
		{"missing prefix", map[string]any{"upstreamRegistryUrl": "quay.io"}, "InvalidParameterException"},
		{"prefix too short", map[string]any{"ecrRepositoryPrefix": "q", "upstreamRegistryUrl": "quay.io"}, "InvalidParameterException"},
		{"prefix with upper case", map[string]any{"ecrRepositoryPrefix": "Quay", "upstreamRegistryUrl": "quay.io"}, "InvalidParameterException"},
		{"missing URL", map[string]any{"ecrRepositoryPrefix": "quay"}, "InvalidParameterException"},
		{"unknown upstreamRegistry", map[string]any{"ecrRepositoryPrefix": "quay", "upstreamRegistryUrl": "quay.io", "upstreamRegistry": "harbor"}, "InvalidParameterException"},
		{"credentialArn outside the pattern", map[string]any{"ecrRepositoryPrefix": "hub", "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:dockerhub-creds-0123456789"}, "InvalidParameterException"},
		{"registryId outside the pattern", map[string]any{"ecrRepositoryPrefix": "quay", "upstreamRegistryUrl": "quay.io", "registryId": "abc"}, "InvalidParameterException"},
		{"unsupported upstream", map[string]any{"ecrRepositoryPrefix": "harbor", "upstreamRegistryUrl": "harbor.example.com"}, "UnsupportedUpstreamRegistryException"},
		{"a URL with a scheme", map[string]any{"ecrRepositoryPrefix": "quay", "upstreamRegistryUrl": "https://quay.io"}, "UnsupportedUpstreamRegistryException"},
		{"authenticated upstream without a secret", map[string]any{"ecrRepositoryPrefix": "hub", "upstreamRegistryUrl": "registry-1.docker.io"}, "ValidationException"},
		{"cross-account ECR upstream without a role", map[string]any{"ecrRepositoryPrefix": "ecr", "upstreamRegistryUrl": "999999999999.dkr.ecr.us-west-2.amazonaws.com"}, "ValidationException"},
		{"upstreamRegistry disagreeing with the URL", map[string]any{"ecrRepositoryPrefix": "quay", "upstreamRegistryUrl": "quay.io", "upstreamRegistry": "k8s"}, "ValidationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := ecrCall(t, ts, "CreatePullThroughCacheRule", tc.body)
			ecrRequireRefusal(t, status, body, tc.code)
		})
	}

	// The accepted forms the user guide documents, one per upstream kind.
	for _, body := range []map[string]any{
		{"ecrRepositoryPrefix": "ecr-public", "upstreamRegistryUrl": "public.ecr.aws"},
		{"ecrRepositoryPrefix": "kubernetes", "upstreamRegistryUrl": "registry.k8s.io"},
		{"ecrRepositoryPrefix": "github", "upstreamRegistryUrl": "ghcr.io", "credentialArn": secret},
		{"ecrRepositoryPrefix": "azure", "upstreamRegistryUrl": "myregistry.azurecr.io", "credentialArn": secret},
		{"ecrRepositoryPrefix": "gitlab", "upstreamRegistryUrl": "registry.gitlab.com", "credentialArn": secret},
		{"ecrRepositoryPrefix": "chainguard", "upstreamRegistryUrl": "cgr.dev", "credentialArn": secret},
		{"ecrRepositoryPrefix": "ecr", "upstreamRegistryUrl": "123456789012.dkr.ecr.us-west-2.amazonaws.com"},
		{"ecrRepositoryPrefix": "ecr-x", "upstreamRegistryUrl": "999999999999.dkr.ecr.us-west-2.amazonaws.com", "customRoleArn": "arn:aws:iam::123456789012:role/ptc"},
	} {
		got := ecrCallOK(t, ts, "CreatePullThroughCacheRule", body)
		if strings.Contains(body["upstreamRegistryUrl"].(string), "dkr.ecr") {
			assert.Contains(t, got, `"upstreamRepositoryPrefix":"ROOT"`, "an omitted upstream prefix is ROOT for an ECR upstream")
		}
	}
}

// TestECR_ValidatePullThroughCacheRuleIsSeedable is the seed: a test chooses the outcome an
// upstream would have decided, per rule or for every rule.
func TestECR_ValidatePullThroughCacheRuleIsSeedable(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "quay", "upstreamRegistryUrl": "quay.io"})
	ecrCallOK(t, ts, "CreatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "k8s", "upstreamRegistryUrl": "registry.k8s.io"})

	seed := func(method string, body map[string]any, query string) int {
		t.Helper()
		data, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(context.Background(), method, ts.URL+"/v1/ecr/pull-through-cache-validation"+query, bytes.NewReader(data))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}

	require.Equal(t, http.StatusBadRequest, seed(http.MethodPost, map[string]any{"isValid": true, "failure": "x"}, ""),
		"a valid rule has no failure to report")
	require.Equal(t, http.StatusOK, seed(http.MethodPost, map[string]any{"ecrRepositoryPrefix": "quay", "isValid": false, "failure": "upstream unreachable"}, ""))

	got := ecrCallOK(t, ts, "ValidatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "quay"})
	assert.Contains(t, got, `"isValid":false`)
	assert.Contains(t, got, `"failure":"upstream unreachable"`)
	assert.Contains(t, ecrCallOK(t, ts, "ValidatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "k8s"}), `"isValid":true`,
		"a per-prefix seed governs that rule only")

	require.Equal(t, http.StatusOK, seed(http.MethodPost, map[string]any{"ecrRepositoryPrefix": "*", "isValid": false}, ""))
	assert.Contains(t, ecrCallOK(t, ts, "ValidatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "k8s"}), `"isValid":false`)

	require.Equal(t, http.StatusOK, seed(http.MethodDelete, nil, ""))
	assert.Contains(t, ecrCallOK(t, ts, "ValidatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "quay"}), `"isValid":true`,
		"clearing every seed restores the nominal outcome")
}

// TestECR_RepositoryCreationTemplateLifecycle drives the four template operations over the wire.
func TestECR_RepositoryCreationTemplateLifecycle(t *testing.T) {
	ts := newECRTestServer(t)

	created := ecrCallOK(t, ts, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":          "aegc-quay",
		"appliedFor":      []string{"PULL_THROUGH_CACHE"},
		"resourceTags":    []map[string]string{{"Key": "owner", "Value": "aegc"}},
		"lifecyclePolicy": `{"rules":[]}`,
		"description":     "cached science images",
	})
	for _, want := range []string{
		`"registryId":"123456789012"`,
		`"prefix":"aegc-quay"`,
		`"appliedFor":["PULL_THROUGH_CACHE"]`,
		`"imageTagMutability":"MUTABLE"`,
		`"encryptionConfiguration":{"encryptionType":"AES256"}`,
		`"resourceTags":[{"Key":"owner","Value":"aegc"}]`,
	} {
		assert.Contains(t, created, want)
	}
	assert.Regexp(t, `"createdAt":[0-9]`, created)

	status, body := ecrCall(t, ts, "CreateRepositoryCreationTemplate", map[string]any{"prefix": "aegc-quay/", "appliedFor": []string{"REPLICATION"}})
	ecrRequireRefusal(t, status, body, "TemplateAlreadyExistsException")

	ecrCallOK(t, ts, "CreateRepositoryCreationTemplate", map[string]any{"prefix": "ROOT", "appliedFor": []string{"CREATE_ON_PUSH"}})

	all := ecrCallOK(t, ts, "DescribeRepositoryCreationTemplates", map[string]any{})
	assert.Contains(t, all, `"prefix":"ROOT"`)
	assert.Contains(t, all, `"registryId":"123456789012"`)
	filtered := ecrCallOK(t, ts, "DescribeRepositoryCreationTemplates", map[string]any{"prefixes": []string{"ROOT", "missing"}})
	assert.Contains(t, filtered, `"prefix":"ROOT"`)
	assert.NotContains(t, filtered, "aegc-quay", "a named prefix with no template is omitted, not refused")

	updated := ecrCallOK(t, ts, "UpdateRepositoryCreationTemplate", map[string]any{
		"prefix":     "aegc-quay",
		"appliedFor": []string{"PULL_THROUGH_CACHE", "REPLICATION"},
	})
	assert.Contains(t, updated, `"appliedFor":["PULL_THROUGH_CACHE","REPLICATION"]`)
	assert.Contains(t, updated, `"description":"cached science images"`, "an omitted member keeps its value")
	assert.Contains(t, updated, `"lifecyclePolicy":"{\"rules\":[]}"`)

	deleted := ecrCallOK(t, ts, "DeleteRepositoryCreationTemplate", map[string]any{"prefix": "aegc-quay"})
	assert.Contains(t, deleted, `"prefix":"aegc-quay"`)
	for _, op := range []string{"DeleteRepositoryCreationTemplate", "UpdateRepositoryCreationTemplate"} {
		status, body := ecrCall(t, ts, op, map[string]any{"prefix": "aegc-quay"})
		ecrRequireRefusal(t, status, body, "TemplateNotFoundException")
	}

	for _, b := range []string{created, all, updated, deleted} {
		for _, member := range []string{"AccountID", "Region"} {
			assert.NotContains(t, b, `"`+member+`"`, "substrate's bookkeeping must not reach the wire")
		}
	}

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"missing appliedFor", map[string]any{"prefix": "x1"}},
		{"empty appliedFor", map[string]any{"prefix": "x1", "appliedFor": []string{}}},
		{"unknown scenario", map[string]any{"prefix": "x1", "appliedFor": []string{"ON_PULL"}}},
		{"prefix outside the pattern", map[string]any{"prefix": "Bad Prefix", "appliedFor": []string{"REPLICATION"}}},
		{"unknown mutability", map[string]any{"prefix": "x1", "appliedFor": []string{"REPLICATION"}, "imageTagMutability": "SOMETIMES"}},
		{"unknown encryption", map[string]any{"prefix": "x1", "appliedFor": []string{"REPLICATION"}, "encryptionConfiguration": map[string]string{"encryptionType": "ROT13"}}},
		{"description too long", map[string]any{"prefix": "x1", "appliedFor": []string{"REPLICATION"}, "description": strings.Repeat("d", 257)}},
		{"too many exclusion filters", map[string]any{"prefix": "x1", "appliedFor": []string{"REPLICATION"}, "imageTagMutabilityExclusionFilters": []map[string]string{
			{"filter": "a", "filterType": "WILDCARD"}, {"filter": "b", "filterType": "WILDCARD"}, {"filter": "c", "filterType": "WILDCARD"},
			{"filter": "d", "filterType": "WILDCARD"}, {"filter": "e", "filterType": "WILDCARD"}, {"filter": "f", "filterType": "WILDCARD"},
		}}},
		{"a tag with no key", map[string]any{"prefix": "x1", "appliedFor": []string{"REPLICATION"}, "resourceTags": []map[string]string{{"Value": "v"}}}},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			status, body := ecrCall(t, ts, "CreateRepositoryCreationTemplate", tc.body)
			ecrRequireRefusal(t, status, body, "InvalidParameterException")
		})
	}
}

// TestECR_PullThroughCacheCreatesTheRepositoryFromItsTemplate is #1461's "nice to have", at the one
// API call that is observably a pull: BatchGetImage against an absent repository under a rule's
// prefix creates it, carrying the matching PULL_THROUGH_CACHE template's settings.
func TestECR_PullThroughCacheCreatesTheRepositoryFromItsTemplate(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "aegc-quay", "upstreamRegistryUrl": "quay.io"})
	ecrCallOK(t, ts, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":             "aegc-quay",
		"appliedFor":         []string{"PULL_THROUGH_CACHE"},
		"resourceTags":       []map[string]string{{"Key": "owner", "Value": "aegc"}},
		"imageTagMutability": "IMMUTABLE",
		"lifecyclePolicy":    `{"rules":[]}`,
	})
	// A template that applies to another scenario is not consulted for a pull.
	ecrCallOK(t, ts, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix": "ROOT", "appliedFor": []string{"REPLICATION"},
		"resourceTags": []map[string]string{{"Key": "wrong", "Value": "template"}},
	})

	const name = "aegc-quay/aarchsci/dev"
	pulled := ecrCallOK(t, ts, "BatchGetImage", map[string]any{
		"repositoryName": name,
		"imageIds":       []map[string]string{{"imageTag": "latest"}},
	})
	assert.Contains(t, pulled, `"failures":[`, "substrate reaches no upstream, so the image itself is not there to answer")

	described := ecrCallOK(t, ts, "DescribeRepositories", map[string]any{"repositoryNames": []string{name}})
	assert.Contains(t, described, `"imageTagMutability":"IMMUTABLE"`)
	tags := ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/" + name,
	})
	assert.Equal(t, `{"tags":[{"Key":"owner","Value":"aegc"}]}`, tags)
	assert.Contains(t, ecrCallOK(t, ts, "GetLifecyclePolicy", map[string]any{"repositoryName": name}), `{\"rules\":[]}`)

	t.Run("no template means the published defaults", func(t *testing.T) {
		ecrCallOK(t, ts, "CreatePullThroughCacheRule", map[string]any{"ecrRepositoryPrefix": "k8s", "upstreamRegistryUrl": "registry.k8s.io"})
		ecrCallOK(t, ts, "BatchGetImage", map[string]any{"repositoryName": "k8s/pause", "imageIds": []map[string]string{{"imageTag": "3.9"}}})
		got := ecrCallOK(t, ts, "DescribeRepositories", map[string]any{"repositoryNames": []string{"k8s/pause"}})
		assert.Contains(t, got, `"imageTagMutability":"MUTABLE"`)
		assert.Contains(t, got, `"encryptionType":"AES256"`)
		assert.Equal(t, `{"tags":[]}`, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
			"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/k8s/pause",
		}))
	})

	t.Run("a name under no rule is still refused", func(t *testing.T) {
		for _, n := range []string{"elsewhere/repo", "aegc-quay"} {
			status, body := ecrCall(t, ts, "BatchGetImage", map[string]any{"repositoryName": n, "imageIds": []map[string]string{{"imageTag": "latest"}}})
			ecrRequireRefusal(t, status, body, "RepositoryNotFoundException")
		}
	})

	t.Run("an existing repository is not re-templated", func(t *testing.T) {
		ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "aegc-quay/by-hand"})
		ecrCallOK(t, ts, "BatchGetImage", map[string]any{"repositoryName": "aegc-quay/by-hand", "imageIds": []map[string]string{{"imageTag": "latest"}}})
		assert.Equal(t, `{"tags":[]}`, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
			"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/aegc-quay/by-hand",
		}))
	})
}

// TestECR_CreateOnPushNeedsATemplate pins the push half: PutImage creates an absent repository only
// when a CREATE_ON_PUSH template governs its name, longest prefix first.
func TestECR_CreateOnPushNeedsATemplate(t *testing.T) {
	ts := newECRTestServer(t)
	const manifest = `{"schemaVersion":2}`

	status, body := ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "prod/team/app", "imageManifest": manifest, "imageTag": "v1"})
	ecrRequireRefusal(t, status, body, "RepositoryNotFoundException")

	ecrCallOK(t, ts, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix": "prod", "appliedFor": []string{"CREATE_ON_PUSH"},
		"resourceTags": []map[string]string{{"Key": "tier", "Value": "prod"}},
	})
	ecrCallOK(t, ts, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix": "prod/team", "appliedFor": []string{"CREATE_ON_PUSH"},
		"resourceTags": []map[string]string{{"Key": "tier", "Value": "team"}},
	})

	// A push this handler refuses creates nothing.
	status, body = ecrCall(t, ts, "PutImage", map[string]any{"repositoryName": "prod/team/app"})
	ecrRequireRefusal(t, status, body, "RepositoryNotFoundException")

	ecrCallOK(t, ts, "PutImage", map[string]any{"repositoryName": "prod/team/app", "imageManifest": manifest, "imageTag": "v1"})
	assert.Equal(t, `{"tags":[{"Key":"tier","Value":"team"}]}`, ecrCallOK(t, ts, "ListTagsForResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:123456789012:repository/prod/team/app",
	}), "the longest matching prefix wins")
	assert.Contains(t, ecrCallOK(t, ts, "ListImages", map[string]any{"repositoryName": "prod/team/app"}), `"imageTag":"v1"`)
}
