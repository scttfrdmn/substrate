package emulator_test

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// RDS and ElastiCache answered no `ResponseMetadata` (#1242) and spelled their document namespace
// `https://` where both services' pages publish `http://` (#1238).
//
// The two defects share a document header, which is why they are pinned in one file. The sweep below
// is exhaustive over every routed operation of both plugins, for the reason
// `elb_response_metadata_test.go` gives: the member was absent from all 37 handlers because it was a
// per-handler convention, so a representative sample would pass again the first time a new handler
// built its own document.

const (
	// rdsDocNamespace is the RDS 2014-10-31 document namespace, as `API_CreateDBInstance`'s sample
	// response spells it.
	rdsDocNamespace = "http://rds.amazonaws.com/doc/2014-10-31/"

	// elasticacheDocNamespace is the ElastiCache 2015-02-02 document namespace, as
	// `API_CreateCacheCluster`'s sample response spells it.
	elasticacheDocNamespace = "http://elasticache.amazonaws.com/doc/2015-02-02/"

	// rdsRoutedOperationCount and elasticacheRoutedOperationCount are how many actions each plugin
	// routes, so a handler added without a row fails the sweep rather than narrowing it.
	rdsRoutedOperationCount         = 23
	elasticacheRoutedOperationCount = 17
)

// queryEnvelope is an awsQuery response's root and its top-level children in document order, so the
// presence of the result element and the position of the metadata are both read off one decode.
type queryEnvelope struct {
	XMLName  xml.Name
	Children []struct {
		XMLName xml.Name
		// RequestID is set on the `ResponseMetadata` child only.
		RequestID string `xml:"RequestId"`
	} `xml:",any"`
}

// requestID is the `ResponseMetadata/RequestId` value, or "" when the document carries none.
func (e queryEnvelope) requestID() string {
	for _, c := range e.Children {
		if c.XMLName.Local == "ResponseMetadata" {
			return c.RequestID
		}
	}
	return ""
}

// hasChild reports whether the document carries a top-level element named name.
func (e queryEnvelope) hasChild(name string) bool {
	for _, c := range e.Children {
		if c.XMLName.Local == name {
			return true
		}
	}
	return false
}

// queryPost sends one Query-protocol request under host and returns the status and raw body.
func queryPost(t *testing.T, baseURL, host string, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

const (
	rdsHost         = "rds.us-east-1.amazonaws.com"
	elasticacheHost = "elasticache.us-east-1.amazonaws.com"
)

// envelopeRow is one routed operation: the request, and whether its page publishes a result element.
type envelopeRow struct {
	operation string
	params    map[string]string
	// noResult marks an operation whose page publishes the root and the metadata alone.
	noResult bool
}

// assertEnvelope sends row and asserts its document's root, namespace, result element and request ID.
func assertEnvelope(t *testing.T, baseURL, host, namespace string, row envelopeRow) {
	t.Helper()
	params := map[string]string{"Action": row.operation}
	for k, v := range row.params {
		params[k] = v
	}
	status, body := queryPost(t, baseURL, host, params)
	require.Equal(t, http.StatusOK, status, "%s: %s", row.operation, body)

	assert.Contains(t, body, `<`+row.operation+`Response xmlns="`+namespace+`">`,
		"%s answers the published root and namespace", row.operation)
	assert.NotContains(t, body, `xmlns="https://`, "%s must not spell the namespace https:// (#1238)", row.operation)

	var env queryEnvelope
	require.NoError(t, xml.Unmarshal([]byte(body), &env), body)
	assert.NotEmpty(t, env.requestID(), "%s carries ResponseMetadata/RequestId (#1242)", row.operation)
	assert.Equal(t, !row.noResult, env.hasChild(row.operation+"Result"),
		"%s: result element presence must match the page", row.operation)
	require.NotEmpty(t, env.Children, body)
	last := env.Children[len(env.Children)-1].XMLName.Local
	assert.Equal(t, "ResponseMetadata", last, "%s: the metadata closes the document", row.operation)
}

// TestRDSEnvelope_EveryRoutedOperationAnswersTheEnvelope walks all 23 routed RDS actions.
//
// The four rows marked noResult are the operations whose pages — `API_DeleteDBSubnetGroup`,
// `API_DeleteDBParameterGroup`, `API_AddTagsToResource`, `API_RemoveTagsFromResource` — publish a
// sample response of the root and the metadata only. They run in order against one server, and each
// delete addresses a resource created for it.
func TestRDSEnvelope_EveryRoutedOperationAnswersTheEnvelope(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	const arn = "arn:aws:rds:us-east-1:123456789012:db:env-db"

	rows := []envelopeRow{
		{operation: "CreateDBInstance", params: map[string]string{"DBInstanceIdentifier": "env-db", "Engine": "postgres"}},
		{operation: "DescribeDBInstances"},
		{operation: "ModifyDBInstance", params: map[string]string{"DBInstanceIdentifier": "env-db", "AllocatedStorage": "40"}},
		{operation: "StopDBInstance", params: map[string]string{"DBInstanceIdentifier": "env-db"}},
		{operation: "StartDBInstance", params: map[string]string{"DBInstanceIdentifier": "env-db"}},
		{operation: "RebootDBInstance", params: map[string]string{"DBInstanceIdentifier": "env-db"}},
		{operation: "CreateDBSnapshot", params: map[string]string{"DBSnapshotIdentifier": "env-snap", "DBInstanceIdentifier": "env-db"}},
		{operation: "DescribeDBSnapshots"},
		{operation: "RestoreDBInstanceFromDBSnapshot", params: map[string]string{"DBSnapshotIdentifier": "env-snap", "DBInstanceIdentifier": "env-restored"}},
		{operation: "DeleteDBSnapshot", params: map[string]string{"DBSnapshotIdentifier": "env-snap"}},
		{operation: "CreateDBSubnetGroup", params: map[string]string{"DBSubnetGroupName": "env-sng", "DBSubnetGroupDescription": "d"}},
		{operation: "DescribeDBSubnetGroups"},
		{operation: "DeleteDBSubnetGroup", params: map[string]string{"DBSubnetGroupName": "env-sng"}, noResult: true},
		{operation: "CreateDBParameterGroup", params: map[string]string{"DBParameterGroupName": "env-pg", "DBParameterGroupFamily": "postgres16", "Description": "d"}},
		{operation: "DescribeDBParameterGroups"},
		{operation: "DeleteDBParameterGroup", params: map[string]string{"DBParameterGroupName": "env-pg"}, noResult: true},
		{operation: "CreateDBCluster", params: map[string]string{"DBClusterIdentifier": "env-cluster", "Engine": "aurora-postgresql"}},
		{operation: "DescribeDBClusters"},
		{operation: "DeleteDBCluster", params: map[string]string{"DBClusterIdentifier": "env-cluster", "SkipFinalSnapshot": "true"}},
		{operation: "AddTagsToResource", params: map[string]string{"ResourceName": arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"}, noResult: true},
		{operation: "ListTagsForResource", params: map[string]string{"ResourceName": arn}},
		{operation: "RemoveTagsFromResource", params: map[string]string{"ResourceName": arn, "TagKeys.member.1": "env"}, noResult: true},
		{operation: "DeleteDBInstance", params: map[string]string{"DBInstanceIdentifier": "env-db", "SkipFinalSnapshot": "true"}},
	}
	require.Len(t, rows, rdsRoutedOperationCount, "every routed RDS action gets a row")
	for _, row := range rows {
		t.Run(row.operation, func(t *testing.T) {
			assertEnvelope(t, ts.URL, rdsHost, rdsDocNamespace, row)
		})
	}
}

// TestElastiCacheEnvelope_EveryRoutedOperationAnswersTheEnvelope walks all 17 routed ElastiCache
// actions.
//
// Only `DeleteCacheSubnetGroup` and `DeleteCacheParameterGroup` publish no result element. Unlike
// RDS, ElastiCache's `AddTagsToResource` and `RemoveTagsFromResource` publish one — their output
// shape is `TagListMessage`, whose one response element is `TagList.Tag.N` — so they are asserted to
// carry it, and the tag test below asserts its contents.
func TestElastiCacheEnvelope_EveryRoutedOperationAnswersTheEnvelope(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	const arn = "arn:aws:elasticache:us-east-1:123456789012:cluster:env-cc"

	rows := []envelopeRow{
		{operation: "CreateCacheCluster", params: map[string]string{"CacheClusterId": "env-cc", "Engine": "redis", "CacheNodeType": "cache.t3.micro"}},
		{operation: "DescribeCacheClusters"},
		{operation: "ModifyCacheCluster", params: map[string]string{"CacheClusterId": "env-cc", "NumCacheNodes": "1"}},
		{operation: "CreateReplicationGroup", params: map[string]string{"ReplicationGroupId": "env-rg", "ReplicationGroupDescription": "d"}},
		{operation: "DescribeReplicationGroups"},
		{operation: "ModifyReplicationGroup", params: map[string]string{"ReplicationGroupId": "env-rg", "ReplicationGroupDescription": "e"}},
		{operation: "DeleteReplicationGroup", params: map[string]string{"ReplicationGroupId": "env-rg"}},
		{operation: "CreateCacheSubnetGroup", params: map[string]string{"CacheSubnetGroupName": "env-sng", "CacheSubnetGroupDescription": "d"}},
		{operation: "DescribeCacheSubnetGroups"},
		{operation: "DeleteCacheSubnetGroup", params: map[string]string{"CacheSubnetGroupName": "env-sng"}, noResult: true},
		{operation: "CreateCacheParameterGroup", params: map[string]string{"CacheParameterGroupName": "env-pg", "CacheParameterGroupFamily": "redis7", "Description": "d"}},
		{operation: "DescribeCacheParameterGroups"},
		{operation: "DeleteCacheParameterGroup", params: map[string]string{"CacheParameterGroupName": "env-pg"}, noResult: true},
		{operation: "AddTagsToResource", params: map[string]string{"ResourceName": arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"}},
		{operation: "ListTagsForResource", params: map[string]string{"ResourceName": arn}},
		{operation: "RemoveTagsFromResource", params: map[string]string{"ResourceName": arn, "TagKeys.member.1": "env"}},
		{operation: "DeleteCacheCluster", params: map[string]string{"CacheClusterId": "env-cc"}},
	}
	require.Len(t, rows, elasticacheRoutedOperationCount, "every routed ElastiCache action gets a row")
	for _, row := range rows {
		t.Run(row.operation, func(t *testing.T) {
			assertEnvelope(t, ts.URL, elasticacheHost, elasticacheDocNamespace, row)
		})
	}
}

// TestElastiCache_TagMutationsAnswerTheResultingTagList pins the `TagListMessage` that
// API_AddTagsToResource and API_RemoveTagsFromResource publish: each answers the resource's tags
// as they stand after the change, which until #1242 neither did.
func TestElastiCache_TagMutationsAnswerTheResultingTagList(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	const arn = "arn:aws:elasticache:us-east-1:123456789012:cluster:tags-cc"
	status, body := queryPost(t, ts.URL, elasticacheHost, map[string]string{
		"Action": "CreateCacheCluster", "CacheClusterId": "tags-cc", "Engine": "redis",
	})
	require.Equal(t, http.StatusOK, status, body)

	// The children are decoded generically because the result element is named for the operation;
	// only `{Operation}Result` carries a TagList, so the metadata child contributes nothing.
	type tagList struct {
		Children []struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"TagList>Tag"`
		} `xml:",any"`
	}
	keys := func(body string) []string {
		var out tagList
		require.NoError(t, xml.Unmarshal([]byte(body), &out), body)
		ks := []string{}
		for _, child := range out.Children {
			for _, tag := range child.Tags {
				ks = append(ks, tag.Key+"="+tag.Value)
			}
		}
		return ks
	}

	for _, tc := range []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{"add two", map[string]string{
			"Action": "AddTagsToResource", "ResourceName": arn,
			"Tags.member.1.Key": "team", "Tags.member.1.Value": "data",
			"Tags.member.2.Key": "env", "Tags.member.2.Value": "prod",
		}, []string{"env=prod", "team=data"}},
		{"add one more", map[string]string{
			"Action": "AddTagsToResource", "ResourceName": arn,
			"Tags.member.1.Key": "cost", "Tags.member.1.Value": "7",
		}, []string{"cost=7", "env=prod", "team=data"}},
		{"remove one", map[string]string{
			"Action": "RemoveTagsFromResource", "ResourceName": arn, "TagKeys.member.1": "env",
		}, []string{"cost=7", "team=data"}},
	} {
		status, body := queryPost(t, ts.URL, elasticacheHost, tc.params)
		require.Equal(t, http.StatusOK, status, "%s: %s", tc.name, body)
		assert.Equal(t, tc.want, keys(body), "%s answers the resulting TagList", tc.name)
	}
}

// TestRDSElastiCacheEnvelope_TheIDIsTheOneTheEventRecorded ties the rendered value to the request:
// the ID [emulator.Event.RequestID] records, in the recorded body as well as the live one.
func TestRDSElastiCacheEnvelope_TheIDIsTheOneTheEventRecorded(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	_, rdsBody := queryPost(t, ts.URL, rdsHost, map[string]string{"Action": "CreateDBInstance", "DBInstanceIdentifier": "rec-db"})
	_, ecBody := queryPost(t, ts.URL, elasticacheHost, map[string]string{"Action": "CreateCacheCluster", "CacheClusterId": "rec-cc"})

	events, err := ts.Store().GetStream(t.Context(), "default")
	require.NoError(t, err)
	require.Len(t, events, 2)
	for i, live := range []string{rdsBody, ecBody} {
		event := events[i]
		require.NotEmpty(t, event.RequestID, "%s recorded no request id", event.Operation)
		want := "<RequestId>" + event.RequestID + "</RequestId>"
		assert.Contains(t, live, want, "%s answered an id other than the one the event carries", event.Operation)
		require.NotNil(t, event.Response)
		assert.Contains(t, string(event.Response.Body), want, "%s recorded an id other than its own", event.Operation)
	}
}

// TestRDSElastiCacheEnvelope_AReplayReproducesTheRecordedRequestID is the determinism half: the
// replay engine diffs each replayed body against its recording, so an ID minted per call would be
// reported at the `ResponseMetadata/RequestId` path of every event.
func TestRDSElastiCacheEnvelope_AReplayReproducesTheRecordedRequestID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	for _, call := range []struct {
		host   string
		params map[string]string
	}{
		{rdsHost, map[string]string{"Action": "CreateDBInstance", "DBInstanceIdentifier": "replay-db"}},
		{rdsHost, map[string]string{"Action": "DescribeDBInstances"}},
		{rdsHost, map[string]string{"Action": "AddTagsToResource", "ResourceName": "arn:aws:rds:us-east-1:123456789012:db:replay-db", "Tags.member.1.Key": "k", "Tags.member.1.Value": "v"}},
		{elasticacheHost, map[string]string{"Action": "CreateCacheCluster", "CacheClusterId": "replay-cc"}},
		{elasticacheHost, map[string]string{"Action": "DescribeCacheClusters"}},
	} {
		status, body := queryPost(t, ts.URL, call.host, call.params)
		require.Equal(t, http.StatusOK, status, body)
	}

	engine := emulator.NewReplayEngine(
		ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
	)
	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Equal(t, 5, results.SuccessEvents, "every recorded event must be re-executed")
	for _, diff := range results.Differences {
		assert.False(t, strings.HasSuffix(diff.Field, "/ResponseMetadata/RequestId"),
			"%s replayed a different request id: recorded %v, replayed %v",
			diff.Operation, diff.Expected, diff.Actual)
	}
}
