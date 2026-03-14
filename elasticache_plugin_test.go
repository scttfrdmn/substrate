package substrate_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newElastiCacheTestServer builds a minimal Server with the ElastiCache plugin.
func newElastiCacheTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.ElastiCachePlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize elasticache plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// ecRequest sends an ElastiCache query-protocol request.
func ecRequest(t *testing.T, ts *httptest.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build elasticache request: %v", err)
	}
	req.Host = "elasticache.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do elasticache request: %v", err)
	}
	return resp
}

// ecBody reads and closes the response body.
func ecBody(t *testing.T, r *http.Response) string {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read elasticache response body: %v", err)
	}
	return string(b)
}

func TestElastiCachePlugin_CacheClusterCRUD(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// CreateCacheCluster
	resp := ecRequest(t, ts, map[string]string{
		"Action":         "CreateCacheCluster",
		"CacheClusterId": "my-redis",
		"CacheNodeType":  "cache.t3.micro",
		"Engine":         "redis",
		"NumCacheNodes":  "1",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheCluster status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "my-redis") {
		t.Error("CreateCacheCluster response missing cluster ID")
	}
	if !strings.Contains(body, "available") {
		t.Error("CreateCacheCluster response missing available status")
	}
	// Redis default port should be present.
	if !strings.Contains(body, "6379") {
		t.Error("CreateCacheCluster redis response missing port 6379")
	}

	// DescribeCacheClusters — all
	resp = ecRequest(t, ts, map[string]string{"Action": "DescribeCacheClusters"})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheClusters status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-redis") {
		t.Error("DescribeCacheClusters response missing cluster")
	}

	// DescribeCacheClusters — filter
	resp = ecRequest(t, ts, map[string]string{
		"Action":         "DescribeCacheClusters",
		"CacheClusterId": "my-redis",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheClusters filter status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-redis") {
		t.Error("DescribeCacheClusters filter response missing cluster")
	}

	// DescribeCacheClusters — not found
	resp = ecRequest(t, ts, map[string]string{
		"Action":         "DescribeCacheClusters",
		"CacheClusterId": "nonexistent",
	})
	ecBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("DescribeCacheClusters nonexistent should return non-200")
	}

	// ModifyCacheCluster
	resp = ecRequest(t, ts, map[string]string{
		"Action":         "ModifyCacheCluster",
		"CacheClusterId": "my-redis",
		"CacheNodeType":  "cache.t3.small",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyCacheCluster status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "cache.t3.small") {
		t.Error("ModifyCacheCluster response missing updated node type")
	}

	// DeleteCacheCluster
	resp = ecRequest(t, ts, map[string]string{
		"Action":         "DeleteCacheCluster",
		"CacheClusterId": "my-redis",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteCacheCluster status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "deleting") {
		t.Error("DeleteCacheCluster response missing deleting status")
	}

	// Verify gone
	resp = ecRequest(t, ts, map[string]string{
		"Action":         "DescribeCacheClusters",
		"CacheClusterId": "my-redis",
	})
	ecBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("DescribeCacheClusters after delete should return non-200")
	}
}

func TestElastiCachePlugin_MemcachedCluster(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":         "CreateCacheCluster",
		"CacheClusterId": "my-memcached",
		"CacheNodeType":  "cache.t3.micro",
		"Engine":         "memcached",
		"NumCacheNodes":  "2",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheCluster memcached status %d, body: %s", resp.StatusCode, body)
	}
	// Memcached default port is 11211.
	if !strings.Contains(body, "11211") {
		t.Error("CreateCacheCluster memcached response missing port 11211")
	}
}

func TestElastiCachePlugin_ReplicationGroupCRUD(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// CreateReplicationGroup
	resp := ecRequest(t, ts, map[string]string{
		"Action":                      "CreateReplicationGroup",
		"ReplicationGroupId":          "my-rg",
		"ReplicationGroupDescription": "test replication group",
		"AutomaticFailoverEnabled":    "true",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateReplicationGroup status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "my-rg") {
		t.Error("CreateReplicationGroup response missing group ID")
	}
	if !strings.Contains(body, "available") {
		t.Error("CreateReplicationGroup response missing available status")
	}
	if !strings.Contains(body, "enabled") {
		t.Error("CreateReplicationGroup response missing AutomaticFailover enabled")
	}

	// DescribeReplicationGroups
	resp = ecRequest(t, ts, map[string]string{
		"Action":             "DescribeReplicationGroups",
		"ReplicationGroupId": "my-rg",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeReplicationGroups status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-rg") {
		t.Error("DescribeReplicationGroups response missing group")
	}

	// ModifyReplicationGroup
	resp = ecRequest(t, ts, map[string]string{
		"Action":                      "ModifyReplicationGroup",
		"ReplicationGroupId":          "my-rg",
		"ReplicationGroupDescription": "updated description",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyReplicationGroup status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "updated description") {
		t.Error("ModifyReplicationGroup response missing updated description")
	}

	// DeleteReplicationGroup
	resp = ecRequest(t, ts, map[string]string{
		"Action":             "DeleteReplicationGroup",
		"ReplicationGroupId": "my-rg",
	})
	ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteReplicationGroup status %d", resp.StatusCode)
	}

	// Verify gone
	resp = ecRequest(t, ts, map[string]string{
		"Action":             "DescribeReplicationGroups",
		"ReplicationGroupId": "my-rg",
	})
	ecBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("DescribeReplicationGroups after delete should return non-200")
	}
}

func TestElastiCachePlugin_SubnetAndParamGroups(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// CreateCacheSubnetGroup
	resp := ecRequest(t, ts, map[string]string{
		"Action":                      "CreateCacheSubnetGroup",
		"CacheSubnetGroupName":        "my-cache-sg",
		"CacheSubnetGroupDescription": "test subnet group",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheSubnetGroup status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "my-cache-sg") {
		t.Error("CreateCacheSubnetGroup response missing name")
	}

	// DescribeCacheSubnetGroups
	resp = ecRequest(t, ts, map[string]string{"Action": "DescribeCacheSubnetGroups"})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheSubnetGroups status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-cache-sg") {
		t.Error("DescribeCacheSubnetGroups response missing subnet group")
	}

	// DeleteCacheSubnetGroup
	resp = ecRequest(t, ts, map[string]string{
		"Action":               "DeleteCacheSubnetGroup",
		"CacheSubnetGroupName": "my-cache-sg",
	})
	ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteCacheSubnetGroup status %d", resp.StatusCode)
	}

	// CreateCacheParameterGroup
	resp = ecRequest(t, ts, map[string]string{
		"Action":                    "CreateCacheParameterGroup",
		"CacheParameterGroupName":   "my-cache-pg",
		"CacheParameterGroupFamily": "redis7",
		"Description":               "test param group",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheParameterGroup status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "my-cache-pg") {
		t.Error("CreateCacheParameterGroup response missing name")
	}

	// DescribeCacheParameterGroups
	resp = ecRequest(t, ts, map[string]string{
		"Action":                  "DescribeCacheParameterGroups",
		"CacheParameterGroupName": "my-cache-pg",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheParameterGroups status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "my-cache-pg") {
		t.Error("DescribeCacheParameterGroups response missing param group")
	}

	// DeleteCacheParameterGroup
	resp = ecRequest(t, ts, map[string]string{
		"Action":                  "DeleteCacheParameterGroup",
		"CacheParameterGroupName": "my-cache-pg",
	})
	ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteCacheParameterGroup status %d", resp.StatusCode)
	}
}

func TestElastiCachePlugin_Tagging(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// Create a cluster first.
	resp := ecRequest(t, ts, map[string]string{
		"Action":         "CreateCacheCluster",
		"CacheClusterId": "tag-cluster",
		"CacheNodeType":  "cache.t3.micro",
		"Engine":         "redis",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheCluster status %d, body: %s", resp.StatusCode, body)
	}

	arn := "arn:aws:elasticache:us-east-1:000000000000:cluster:tag-cluster"

	// AddTagsToResource
	resp = ecRequest(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        arn,
		"Tags.member.1.Key":   "Env",
		"Tags.member.1.Value": "test",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("AddTagsToResource status %d, body: %s", resp.StatusCode, body)
	}

	// ListTagsForResource
	resp = ecRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": arn,
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "Env") {
		t.Error("ListTagsForResource response missing tag key")
	}

	// RemoveTagsFromResource
	resp = ecRequest(t, ts, map[string]string{
		"Action":           "RemoveTagsFromResource",
		"ResourceName":     arn,
		"TagKeys.member.1": "Env",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RemoveTagsFromResource status %d, body: %s", resp.StatusCode, body)
	}
}

func TestElastiCachePlugin_UnknownAction(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{"Action": "NoSuchElastiCacheAction"})
	ecBody(t, resp) //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Error("unknown action should return non-200")
	}
}

func TestElastiCachePlugin_ErrorPaths(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// ModifyCacheCluster — not found.
	resp := ecRequest(t, ts, map[string]string{
		"Action":         "ModifyCacheCluster",
		"CacheClusterId": "no-such-cluster",
		"CacheNodeType":  "cache.t3.small",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("ModifyCacheCluster on missing cluster should fail")
	}

	// DeleteCacheCluster — not found.
	resp = ecRequest(t, ts, map[string]string{
		"Action":         "DeleteCacheCluster",
		"CacheClusterId": "no-such-cluster",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("DeleteCacheCluster on missing cluster should fail")
	}

	// ModifyReplicationGroup — not found.
	resp = ecRequest(t, ts, map[string]string{
		"Action":             "ModifyReplicationGroup",
		"ReplicationGroupId": "no-such-rg",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("ModifyReplicationGroup on missing group should fail")
	}

	// DeleteReplicationGroup — not found.
	resp = ecRequest(t, ts, map[string]string{
		"Action":             "DeleteReplicationGroup",
		"ReplicationGroupId": "no-such-rg",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("DeleteReplicationGroup on missing group should fail")
	}

	// CreateCacheCluster — missing identifier.
	resp = ecRequest(t, ts, map[string]string{
		"Action":        "CreateCacheCluster",
		"CacheNodeType": "cache.t3.micro",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("CreateCacheCluster without identifier should fail")
	}

	// CreateReplicationGroup — missing identifier.
	resp = ecRequest(t, ts, map[string]string{
		"Action":                      "CreateReplicationGroup",
		"ReplicationGroupDescription": "desc",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("CreateReplicationGroup without identifier should fail")
	}
}

func TestElastiCachePlugin_MultiAZReplicationGroup(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":                      "CreateReplicationGroup",
		"ReplicationGroupId":          "multi-az-rg",
		"ReplicationGroupDescription": "multi-az test",
		"AutomaticFailoverEnabled":    "false",
		"MultiAZEnabled":              "true",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateReplicationGroup MultiAZ status %d, body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "disabled") {
		t.Error("AutomaticFailover should be disabled")
	}

	// ModifyReplicationGroup to enable failover.
	resp = ecRequest(t, ts, map[string]string{
		"Action":                   "ModifyReplicationGroup",
		"ReplicationGroupId":       "multi-az-rg",
		"AutomaticFailoverEnabled": "true",
		"MultiAZEnabled":           "false",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyReplicationGroup status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "enabled") {
		t.Error("AutomaticFailover should be enabled after modify")
	}
}

func TestElastiCachePlugin_ReplicationGroupTagging(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// Create a replication group.
	resp := ecRequest(t, ts, map[string]string{
		"Action":                      "CreateReplicationGroup",
		"ReplicationGroupId":          "tag-rg",
		"ReplicationGroupDescription": "tagging test",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateReplicationGroup status %d, body: %s", resp.StatusCode, body)
	}

	// Tagging via replicationgroup ARN type.
	arn := "arn:aws:elasticache:us-east-1:000000000000:replicationgroup:tag-rg"

	resp = ecRequest(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        arn,
		"Tags.member.1.Key":   "Team",
		"Tags.member.1.Value": "infra",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("AddTagsToResource on RG status %d, body: %s", resp.StatusCode, body)
	}
}

func TestElastiCachePlugin_DescribeAllGroups(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// Create two replication groups.
	for _, id := range []string{"rg-a", "rg-b"} {
		resp := ecRequest(t, ts, map[string]string{
			"Action":                      "CreateReplicationGroup",
			"ReplicationGroupId":          id,
			"ReplicationGroupDescription": id + " description",
		})
		ecBody(t, resp)
	}

	// DescribeReplicationGroups — no filter returns all.
	resp := ecRequest(t, ts, map[string]string{"Action": "DescribeReplicationGroups"})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeReplicationGroups status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "rg-a") || !strings.Contains(body, "rg-b") {
		t.Error("DescribeReplicationGroups should return both groups")
	}
}

// TestElastiCachePlugin_ModifyAllParams covers EngineVersion and NumCacheNodes branches in modifyCacheCluster.
func TestElastiCachePlugin_ModifyAllParams(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":         "CreateCacheCluster",
		"CacheClusterId": "modify-all",
		"CacheNodeType":  "cache.t3.micro",
		"Engine":         "redis",
		"NumCacheNodes":  "1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheCluster status %d", resp.StatusCode)
	}

	resp = ecRequest(t, ts, map[string]string{
		"Action":         "ModifyCacheCluster",
		"CacheClusterId": "modify-all",
		"CacheNodeType":  "cache.t3.medium",
		"EngineVersion":  "7.0.7",
		"NumCacheNodes":  "2",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyCacheCluster status %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "cache.t3.medium") {
		t.Error("modified node type should appear in response")
	}
}

// TestElastiCachePlugin_DescribeClusterNotFound covers CacheClusterNotFound error.
func TestElastiCachePlugin_DescribeClusterNotFound(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":         "DescribeCacheClusters",
		"CacheClusterId": "no-such-cluster",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("DescribeCacheClusters for missing cluster should not return 200")
	}
}

// TestElastiCachePlugin_DeleteSubnetAndParamGroups covers delete success paths.
func TestElastiCachePlugin_DeleteSubnetAndParamGroups(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	// SubnetGroup.
	resp := ecRequest(t, ts, map[string]string{
		"Action":                      "CreateCacheSubnetGroup",
		"CacheSubnetGroupName":        "del-sg",
		"CacheSubnetGroupDescription": "to delete",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheSubnetGroup status %d", resp.StatusCode)
	}
	resp = ecRequest(t, ts, map[string]string{
		"Action":               "DeleteCacheSubnetGroup",
		"CacheSubnetGroupName": "del-sg",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteCacheSubnetGroup status %d", resp.StatusCode)
	}
	// Confirm gone (returns empty 200).
	resp = ecRequest(t, ts, map[string]string{
		"Action":               "DescribeCacheSubnetGroups",
		"CacheSubnetGroupName": "del-sg",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheSubnetGroups after delete status %d", resp.StatusCode)
	}
	if strings.Contains(body, "del-sg") {
		t.Error("DescribeCacheSubnetGroups should not return deleted group")
	}

	// ParameterGroup.
	resp = ecRequest(t, ts, map[string]string{
		"Action":                    "CreateCacheParameterGroup",
		"CacheParameterGroupName":   "del-pg",
		"CacheParameterGroupFamily": "redis7",
		"Description":               "to delete",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateCacheParameterGroup status %d", resp.StatusCode)
	}
	resp = ecRequest(t, ts, map[string]string{
		"Action":                  "DeleteCacheParameterGroup",
		"CacheParameterGroupName": "del-pg",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteCacheParameterGroup status %d", resp.StatusCode)
	}
}

// TestElastiCachePlugin_ReplicationGroupTaggingARN exercises the elasticacheResolveARN
// "replicationgroup" branch via tagging operations on a replication group ARN.
func TestElastiCachePlugin_ReplicationGroupTaggingARN(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":                      "CreateReplicationGroup",
		"ReplicationGroupId":          "rg-tag-test",
		"ReplicationGroupDescription": "for tagging",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateReplicationGroup status %d", resp.StatusCode)
	}

	rgARN := "arn:aws:elasticache:us-east-1:000000000000:replicationgroup:rg-tag-test"

	resp = ecRequest(t, ts, map[string]string{
		"Action":              "AddTagsToResource",
		"ResourceName":        rgARN,
		"Tags.member.1.Key":   "tier",
		"Tags.member.1.Value": "cache",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("AddTagsToResource on rg status %d", resp.StatusCode)
	}

	resp = ecRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": rgARN,
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource on rg status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "tier") {
		t.Error("replication group tags should include tier key")
	}

	resp = ecRequest(t, ts, map[string]string{
		"Action":           "RemoveTagsFromResource",
		"ResourceName":     rgARN,
		"TagKeys.member.1": "tier",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RemoveTagsFromResource on rg status %d", resp.StatusCode)
	}
}

// TestElastiCachePlugin_InvalidARN covers the invalid-ARN path in loadTagsByARN.
func TestElastiCachePlugin_InvalidARN(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": "not-an-arn",
	})
	ecBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		t.Error("ListTagsForResource with invalid ARN should return error")
	}
}

// TestElastiCachePlugin_MissingParams covers missing-parameter error branches.
func TestElastiCachePlugin_MissingParams(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	cases := []map[string]string{
		{"Action": "CreateCacheCluster"},
		{"Action": "ModifyCacheCluster"},
		{"Action": "DeleteCacheCluster"},
		{"Action": "CreateReplicationGroup"},
		{"Action": "ModifyReplicationGroup"},
		{"Action": "DeleteReplicationGroup"},
		{"Action": "CreateCacheSubnetGroup"},
		{"Action": "DeleteCacheSubnetGroup"},
		// DescribeCacheSubnetGroups returns empty 200 for unknown names — skip.
		{"Action": "CreateCacheParameterGroup"},
		{"Action": "DeleteCacheParameterGroup"},
		// DescribeCacheParameterGroups returns empty 200 for unknown names — skip.
		{"Action": "ListTagsForResource"},
		{"Action": "AddTagsToResource"},
		{"Action": "RemoveTagsFromResource"},
	}
	for _, params := range cases {
		resp := ecRequest(t, ts, params)
		ecBody(t, resp)
		if resp.StatusCode == http.StatusOK {
			t.Errorf("action %s with missing/invalid params should not return 200", params["Action"])
		}
	}
}

// TestElastiCachePlugin_CacheClusterPagination covers Marker-based pagination.
func TestElastiCachePlugin_CacheClusterPagination(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	for i := range 5 {
		resp := ecRequest(t, ts, map[string]string{
			"Action":         "CreateCacheCluster",
			"CacheClusterId": fmt.Sprintf("pg-cluster-%d", i),
			"CacheNodeType":  "cache.t3.micro",
			"Engine":         "redis",
			"NumCacheNodes":  "1",
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateCacheCluster %d status %d", i, resp.StatusCode)
		}
	}

	// Page 1: max 2.
	resp := ecRequest(t, ts, map[string]string{
		"Action":     "DescribeCacheClusters",
		"MaxRecords": "2",
	})
	body := ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheClusters page1 status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "<Marker>") {
		t.Error("page 1 should include a Marker")
	}

	// Page 2: use marker.
	resp = ecRequest(t, ts, map[string]string{
		"Action":     "DescribeCacheClusters",
		"MaxRecords": "2",
		"Marker":     "2",
	})
	body = ecBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeCacheClusters page2 status %d", resp.StatusCode)
	}
	_ = body
}
