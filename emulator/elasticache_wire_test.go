package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ElastiCache answers every resource through an item struct in elasticache_wire.go rather than by
// encoding the persisted record, and nothing asserted that (#756). These tests are that assertion:
// for each of the four records, every operation that answers it is driven and the body's element
// paths are compared against the published shape's — so AccountID, Region, CreatedAt and
// `ever_tagged` cannot reach a body, and neither can anything else substrate has not deliberately
// published.
//
// Element *paths*, not a decode into a Go struct, for the reason rds_wire_test.go gives: decoding
// into a type that has no field for an extra member is exactly the step that hides it. A path rather
// than a bare name also catches a published member rendered at the wrong depth, which is how the RDS
// cluster's DBSubnetGroup came to be nested.

// elasticacheWireClock is the instant setupElastiCacheWirePlugin starts the simulated clock at, so
// every projected timestamp has a known value. Non-zero on purpose: elasticacheTimeOrNil omits the
// zero time, so a test whose clock was the zero time would see CacheClusterCreateTime and
// ReplicationGroupCreateTime absent from every body and call that a pass.
//
// The controller advances a small deterministic step per observation rather than standing still, so
// the assertions below bound the value rather than equate it — still nothing read off the wall clock.
var elasticacheWireClock = time.Unix(1700000000, 0).UTC()

// elasticacheWireAccount and elasticacheWireRegion scope every state key the plugin writes, and are
// what the ARNs in the responses are built from.
const (
	elasticacheWireAccount = "123456789012"
	elasticacheWireRegion  = "us-east-1"
)

// The published member paths of each shape, one entry per element the body may carry, relative to
// the record's own element. A nested element appears as "parent/child" so a member rendered at the
// wrong depth fails rather than passing on its leaf name.
//
// Each list is the published shape's membership minus what substrate does not model;
// elasticache_wire.go records which members those are and why each is absent rather than present
// and zero.
var (
	// API_CacheCluster. ConfigurationEndpoint is an API_Endpoint object, so its two members are one
	// element deeper.
	elasticacheWireCacheClusterMembers = []string{
		"ARN",
		"CacheClusterCreateTime",
		"CacheClusterId",
		"CacheClusterStatus",
		"CacheNodeType",
		"ConfigurationEndpoint",
		"ConfigurationEndpoint/Address",
		"ConfigurationEndpoint/Port",
		"Engine",
		"EngineVersion",
		"NumCacheNodes",
		"ReplicationGroupId",
	}

	// API_ReplicationGroup. AutomaticFailover and MultiAZ are Type: String there, not Boolean.
	elasticacheWireReplicationGroupMembers = []string{
		"ARN",
		"AutomaticFailover",
		"Description",
		"MultiAZ",
		"ReplicationGroupCreateTime",
		"ReplicationGroupId",
		"Status",
	}

	// API_CacheSubnetGroup, which publishes no creation-time member — so unlike the two shapes above,
	// nothing here reports a timestamp.
	elasticacheWireCacheSubnetGroupMembers = []string{
		"ARN",
		"CacheSubnetGroupDescription",
		"CacheSubnetGroupName",
		"VpcId",
	}

	// API_CacheParameterGroup, four of whose five members substrate models; IsGlobal is the fifth.
	elasticacheWireCacheParameterGroupMembers = []string{
		"ARN",
		"CacheParameterGroupFamily",
		"CacheParameterGroupName",
		"Description",
	}
)

// setupElastiCacheWirePlugin returns the ElastiCache plugin, a request context and the state manager
// behind it.
//
// The state manager is handed back because half of what is under test is that the record keeps the
// members the response drops — and because it is the only place the anchors below can read
// AccountID, Region and `ever_tagged` from, none of which has a published home to read it back
// through.
//
// Its own harness rather than newElastiCacheTestServer, which seeds the clock from time.Now(): a
// timestamp assertion needs a known instant. The plugin is called directly rather than through an
// httptest server because what is under test is the bytes of a response body, and the server adds
// nothing to those.
func setupElastiCacheWirePlugin(t *testing.T) (*emulator.ElastiCachePlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.ElastiCachePlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(elasticacheWireClock)},
	}), "emulator.ElastiCachePlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: elasticacheWireAccount,
		Region:    elasticacheWireRegion,
		RequestID: "req-elasticache-wire",
	}, state
}

// elasticacheWire issues one query-protocol action and returns the raw response body, failing the
// test on anything but 200.
func elasticacheWire(t *testing.T, p *emulator.ElastiCachePlugin, ctx *emulator.RequestContext, action string, params map[string]string) []byte {
	t.Helper()
	full := map[string]string{"Action": action}
	maps.Copy(full, params)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "elasticache",
		Operation: action,
		Params:    full,
	})
	require.NoError(t, err, "%s", action)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return resp.Body
}

// elasticacheWireMembers walks an ElastiCache response body and returns every element inside
// wrapper, keyed by its path relative to wrapper and valued with the text it carries.
//
// A list response carries wrapper more than once
// (DescribeCacheClustersResult>CacheClusters>CacheCluster), and the paths are unioned across every
// occurrence — so an extra member on any item in the page fails, not just on the first.
func elasticacheWireMembers(t *testing.T, action string, body []byte, wrapper string) map[string]string {
	t.Helper()
	out := map[string]string{}
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	inside := false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "%s: decode body: %s", action, body)
		switch el := tok.(type) {
		case xml.StartElement:
			if !inside {
				// Everything above the record — the response envelope and the result element — is
				// skipped, so one expectation serves every operation that answers the record.
				inside = el.Name.Local == wrapper
				continue
			}
			path = append(path, el.Name.Local)
			key := strings.Join(path, "/")
			if _, seen := out[key]; !seen {
				out[key] = ""
			}
		case xml.EndElement:
			switch {
			case !inside:
			case len(path) == 0:
				inside = false // wrapper's own end tag
			default:
				path = path[:len(path)-1]
			}
		case xml.CharData:
			if !inside || len(path) == 0 {
				continue
			}
			if text := strings.TrimSpace(string(el)); text != "" {
				out[strings.Join(path, "/")] += text
			}
		}
	}
	require.NotEmpty(t, out, "%s: body carries no %s element: %s", action, wrapper, body)
	return out
}

// elasticacheWireAssertMembers requires that the record's elements are exactly the published ones.
//
// Equality rather than a set of NotContains assertions: it subsumes every absence check at once, and
// it cannot go vacuous the way an absence check on a member the body could not have carried does.
func elasticacheWireAssertMembers(t *testing.T, action string, body []byte, wrapper string, want []string) map[string]string {
	t.Helper()
	members := elasticacheWireMembers(t, action, body, wrapper)
	assert.Equal(t, want, slices.Sorted(maps.Keys(members)),
		"%s: %s must carry exactly the published members: %s", action, wrapper, body)
	return members
}

// elasticacheWireTime parses a projected timestamp and requires it to be the simulated clock's,
// within a tolerance.
//
// Bounded rather than equated because TimeController advances a deterministic step per observation; a
// test demanding the seeded instant exactly fails on the second request of a run.
func elasticacheWireTime(t *testing.T, action string, members map[string]string, member string) {
	t.Helper()
	raw, ok := members[member]
	require.True(t, ok, "%s must report %s", action, member)
	ts, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err, "%s: %s is not an ISO8601 instant: %q", action, member, raw)
	assert.WithinDuration(t, elasticacheWireClock, ts, time.Second, "%s: %s", action, member)
}

// elasticacheWireRecord returns the record at key as raw JSON, so a member with no published home
// can be read without a Go type deciding which members exist.
func elasticacheWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "elasticache", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// elasticacheWireKey builds the scoped state key the plugin stores a record under.
func elasticacheWireKey(prefix, id string) string {
	return prefix + ":" + elasticacheWireAccount + "/" + elasticacheWireRegion + "/" + id
}

// elasticacheWireAnchorScope requires that the record still carries the two members every one of the
// four records declares and no shape publishes.
//
// This is the presence anchor for the two records that cannot be tagged: an assertion that a
// response omits a member passes without testing anything if the record never held it, so the
// absence assertions only mean something once the record is known to hold both values at the moment
// the response was rendered.
func elasticacheWireAnchorScope(t *testing.T, state emulator.StateManager, action, key string) map[string]json.RawMessage {
	t.Helper()
	record := elasticacheWireRecord(t, state, key)
	require.Equal(t, `"`+elasticacheWireAccount+`"`, string(record["AccountID"]), "%s must persist AccountID", action)
	require.Equal(t, `"`+elasticacheWireRegion+`"`, string(record["Region"]), "%s must persist Region", action)
	return record
}

// elasticacheWireTag tags a resource through ElastiCache's own AddTagsToResource and proves through
// ListTagsForResource that the tag landed.
//
// Tagging is what makes the `ever_tagged` absence assertions non-vacuous: the member is `,omitempty`,
// so it is absent from a record that has never been tagged, and an assertion that a response omits a
// member the record could not have supplied tests nothing. Tagging first makes the member exist at
// the moment every later response is rendered.
func elasticacheWireTag(t *testing.T, p *emulator.ElastiCachePlugin, ctx *emulator.RequestContext, arn string) {
	t.Helper()
	elasticacheWire(t, p, ctx, "AddTagsToResource", map[string]string{
		"ResourceName":        arn,
		"Tags.member.1.Key":   "wire",
		"Tags.member.1.Value": "anchor",
	})

	// Read back through the tag operation, never out of the state store — #765's rule.
	listed := elasticacheWire(t, p, ctx, "ListTagsForResource", map[string]string{"ResourceName": arn})
	assert.Contains(t, string(listed), "<Key>wire</Key>", "AddTagsToResource stored no tag for %s", arn)
}

func TestElastiCacheWire_CacheClusterResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupElastiCacheWirePlugin(t)

	create := map[string]string{
		"CacheClusterId":      "wire-cluster",
		"CacheNodeType":       "cache.t3.micro",
		"Engine":              "memcached",
		"EngineVersion":       "1.6.17",
		"NumCacheNodes":       "2",
		"ReplicationGroupId":  "wire-repl",
		"Tags.member.1.Key":   "seeded",
		"Tags.member.1.Value": "at-create",
	}
	created := elasticacheWireAssertMembers(t, "CreateCacheCluster",
		elasticacheWire(t, p, ctx, "CreateCacheCluster", create), "CacheCluster",
		elasticacheWireCacheClusterMembers)
	elasticacheWireTime(t, "CreateCacheCluster", created, "CacheClusterCreateTime")

	arn := created["ARN"]
	require.NotEmpty(t, arn, "CreateCacheCluster must report ARN")
	clusterKey := elasticacheWireKey("cachecluster", "wire-cluster")

	// Create's own body is rendered before any tag exists, so it anchors on the members that are
	// always set. CacheClusterCreateTime above proves CreatedAt was non-zero; these two have no
	// published home at all, which is the whole point.
	elasticacheWireAnchorScope(t, state, "CreateCacheCluster", clusterKey)

	// Every response after this one is rendered from a record carrying ever_tagged. Create-with-tags
	// does not set the flag — only the tag path does, see [taggingEverTagged] (#938) — which is why
	// the seeded tag above is not enough and this call is.
	elasticacheWireTag(t, p, ctx, arn)
	require.Equal(t, "true", string(elasticacheWireRecord(t, state, clusterKey)["ever_tagged"]),
		"%s must carry ever_tagged before an absence assertion on it means anything", clusterKey)

	// The remaining three operations that answer a CacheCluster.
	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"DescribeCacheClusters", map[string]string{"CacheClusterId": "wire-cluster"}},
		{"ModifyCacheCluster", map[string]string{"CacheClusterId": "wire-cluster", "NumCacheNodes": "3"}},
		// Last: it deletes the record the two above read.
		{"DeleteCacheCluster", map[string]string{"CacheClusterId": "wire-cluster"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			members := elasticacheWireAssertMembers(t, tc.action,
				elasticacheWire(t, p, ctx, tc.action, tc.params), "CacheCluster",
				elasticacheWireCacheClusterMembers)
			elasticacheWireTime(t, tc.action, members, "CacheClusterCreateTime")
		})
	}
}

func TestElastiCacheWire_ReplicationGroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupElastiCacheWirePlugin(t)

	create := map[string]string{
		"ReplicationGroupId":          "wire-repl",
		"ReplicationGroupDescription": "pinned by the wire test",
		"AutomaticFailoverEnabled":    "true",
		"MultiAZEnabled":              "true",
	}
	created := elasticacheWireAssertMembers(t, "CreateReplicationGroup",
		elasticacheWire(t, p, ctx, "CreateReplicationGroup", create), "ReplicationGroup",
		elasticacheWireReplicationGroupMembers)
	elasticacheWireTime(t, "CreateReplicationGroup", created, "ReplicationGroupCreateTime")
	assert.Equal(t, "enabled", created["AutomaticFailover"],
		"API_ReplicationGroup.AutomaticFailover is Type: String, not a Boolean")
	assert.Equal(t, "enabled", created["MultiAZ"],
		"API_ReplicationGroup.MultiAZ is Type: String, not a Boolean")

	arn := created["ARN"]
	require.NotEmpty(t, arn, "CreateReplicationGroup must report ARN")
	groupKey := elasticacheWireKey("replgroup", "wire-repl")
	elasticacheWireAnchorScope(t, state, "CreateReplicationGroup", groupKey)

	// ElastiCacheReplicationGroup declares no EverTagged field, but updateTagsByARN stamps
	// `ever_tagged` into the raw JSON regardless, so tagging here proves that member stays inert —
	// which is the only reason it is not a leak. The absence assertions below run against a record
	// that holds it.
	elasticacheWireTag(t, p, ctx, arn)
	require.Equal(t, "true", string(elasticacheWireRecord(t, state, groupKey)["ever_tagged"]),
		"updateTagsByARN stamps ever_tagged even where the Go type declares no field for it")

	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"DescribeReplicationGroups", map[string]string{"ReplicationGroupId": "wire-repl"}},
		{"ModifyReplicationGroup", map[string]string{"ReplicationGroupId": "wire-repl", "MultiAZEnabled": "false"}},
		// Last: it deletes the record the two above read.
		{"DeleteReplicationGroup", map[string]string{"ReplicationGroupId": "wire-repl"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			members := elasticacheWireAssertMembers(t, tc.action,
				elasticacheWire(t, p, ctx, tc.action, tc.params), "ReplicationGroup",
				elasticacheWireReplicationGroupMembers)
			elasticacheWireTime(t, tc.action, members, "ReplicationGroupCreateTime")
		})
	}
}

func TestElastiCacheWire_CacheSubnetGroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupElastiCacheWirePlugin(t)

	created := elasticacheWireAssertMembers(t, "CreateCacheSubnetGroup",
		elasticacheWire(t, p, ctx, "CreateCacheSubnetGroup", map[string]string{
			"CacheSubnetGroupName":        "wire-subnets",
			"CacheSubnetGroupDescription": "pinned by the wire test",
			"VpcId":                       "vpc-0wire",
			"SubnetIds.member.1":          "subnet-0wire",
		}), "CacheSubnetGroup", elasticacheWireCacheSubnetGroupMembers)

	// createCacheSubnetGroup never reads SubnetIds, so Subnets is absent for want of a value rather
	// than by choice (#1013). Asserted explicitly because the member-set equality above would also
	// pass if it were reported empty.
	assert.NotContains(t, created, "Subnets",
		"API_CacheSubnetGroup.Subnets has no value to report, so it is absent rather than empty")

	// This record cannot be tagged — elasticacheResolveARN reaches only cluster and
	// replicationgroup — so its anchor is the two members it declares and no shape publishes.
	elasticacheWireAnchorScope(t, state, "CreateCacheSubnetGroup",
		elasticacheWireKey("cachesubnetgroup", "wire-subnets"))

	elasticacheWireAssertMembers(t, "DescribeCacheSubnetGroups",
		elasticacheWire(t, p, ctx, "DescribeCacheSubnetGroups", map[string]string{
			"CacheSubnetGroupName": "wire-subnets",
		}), "CacheSubnetGroup", elasticacheWireCacheSubnetGroupMembers)
}

func TestElastiCacheWire_CacheParameterGroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupElastiCacheWirePlugin(t)

	created := elasticacheWireAssertMembers(t, "CreateCacheParameterGroup",
		elasticacheWire(t, p, ctx, "CreateCacheParameterGroup", map[string]string{
			"CacheParameterGroupName":   "wire-params",
			"CacheParameterGroupFamily": "memcached1.6",
			"Description":               "pinned by the wire test",
		}), "CacheParameterGroup", elasticacheWireCacheParameterGroupMembers)

	// IsGlobal flags a Global datastore, which substrate does not model, so it is absent rather than
	// reported false (#1013).
	assert.NotContains(t, created, "IsGlobal",
		"API_CacheParameterGroup.IsGlobal has no value to report, so it is absent rather than false")

	// Untaggable for the same reason as the subnet group, so the same anchor.
	elasticacheWireAnchorScope(t, state, "CreateCacheParameterGroup",
		elasticacheWireKey("cacheparamgroup", "wire-params"))

	elasticacheWireAssertMembers(t, "DescribeCacheParameterGroups",
		elasticacheWire(t, p, ctx, "DescribeCacheParameterGroups", map[string]string{
			"CacheParameterGroupName": "wire-params",
		}), "CacheParameterGroup", elasticacheWireCacheParameterGroupMembers)
}

// TestElastiCacheWire_ProjectionLeavesTheRecordIntact is the other half of the projection: the
// members the responses drop are still persisted, which is what keeps scanElastiCacheClusters and
// the Resource Groups Tagging API's GetResources working — and the reason the eleven baseline lines
// stay in scripts/wire-bookkeeping-baseline.txt rather than being deleted.
func TestElastiCacheWire_ProjectionLeavesTheRecordIntact(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupElastiCacheWirePlugin(t)

	clusterARN := elasticacheWireMembers(t, "CreateCacheCluster",
		elasticacheWire(t, p, ctx, "CreateCacheCluster", map[string]string{
			"CacheClusterId": "intact-cluster",
			"Engine":         "redis",
		}), "CacheCluster")["ARN"]
	elasticacheWire(t, p, ctx, "CreateReplicationGroup", map[string]string{
		"ReplicationGroupId":          "intact-repl",
		"ReplicationGroupDescription": "intact",
	})
	elasticacheWire(t, p, ctx, "CreateCacheSubnetGroup", map[string]string{
		"CacheSubnetGroupName": "intact-subnets",
	})
	elasticacheWire(t, p, ctx, "CreateCacheParameterGroup", map[string]string{
		"CacheParameterGroupName": "intact-params",
	})
	elasticacheWireTag(t, p, ctx, clusterARN)

	for _, tc := range []struct {
		name       string
		key        string
		wantTime   bool
		wantTagged bool
	}{
		{"CacheCluster", elasticacheWireKey("cachecluster", "intact-cluster"), true, true},
		{"ReplicationGroup", elasticacheWireKey("replgroup", "intact-repl"), true, false},
		{"CacheSubnetGroup", elasticacheWireKey("cachesubnetgroup", "intact-subnets"), false, false},
		{"CacheParameterGroup", elasticacheWireKey("cacheparamgroup", "intact-params"), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := elasticacheWireAnchorScope(t, state, tc.name, tc.key)
			assert.Contains(t, record, "Tags", "%s must persist Tags for GetResources", tc.name)

			if tc.wantTime {
				var created time.Time
				require.NoError(t, json.Unmarshal(record["CreatedAt"], &created), "%s CreatedAt", tc.name)
				assert.WithinDuration(t, elasticacheWireClock, created, time.Second, "%s CreatedAt", tc.name)
			} else {
				// Neither API_CacheSubnetGroup nor API_CacheParameterGroup publishes a creation time,
				// so neither record stores one — the declaration surface already agrees here.
				assert.NotContains(t, record, "CreatedAt",
					"%s stores no creation time, matching its published shape", tc.name)
			}

			if tc.wantTagged {
				assert.Equal(t, "true", string(record["ever_tagged"]), "%s must persist ever_tagged", tc.name)
			}
		})
	}
}

// TestElastiCacheWire_TimeOrNilOmitsTheZeroInstant covers elasticacheTimeOrNil's nil arm, which no
// request reaches: every handler that stores a creation time reads it from the simulated clock. The
// arm is what keeps a Required: No timestamp from being reported as the year-one instant, so it is
// asserted directly rather than left uncovered.
func TestElastiCacheWire_TimeOrNilOmitsTheZeroInstant(t *testing.T) {
	t.Parallel()
	assert.Nil(t, emulator.ElastiCacheTimeOrNilForTest(time.Time{}), "the zero time must be omitted")

	seeded := emulator.ElastiCacheTimeOrNilForTest(elasticacheWireClock)
	require.NotNil(t, seeded, "a set time must be reported")
	assert.Equal(t, elasticacheWireClock, *seeded)
}
