package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A state record was handed straight to a caller, so two of substrate's own fields
// were TableDescription members (#1013).
//
// DynamoDBTable is persisted state. Twelve of its fourteen fields are genuine
// TableDescription members, so the two that are not went unnoticed: CreateTable with a
// tag answered "Tags":{"env":"test"} inside its TableDescription, and DescribeTable
// after UpdateTimeToLive answered "TTLAttribute":"expiresAt". API_TableDescription
// publishes neither — its member list is ArchivalSummary, AttributeDefinitions,
// BillingModeSummary, CreationDateTime, DeletionProtectionEnabled,
// GlobalSecondaryIndexes, GlobalTableSettingsReplicationMode, GlobalTableVersion,
// GlobalTableWitnesses, ItemCount, KeySchema, LatestStreamArn, LatestStreamLabel,
// LocalSecondaryIndexes, MultiRegionConsistency, OnDemandThroughput,
// ProvisionedThroughput, Replicas, RestoreSummary, SSEDescription,
// StreamSpecification, TableArn, TableClassSummary, TableId, TableName,
// TableSizeBytes, TableStatus, VectorIndexes and WarmThroughput. Tags on a table are
// published on ListTagsOfResource and the TTL attribute on DescribeTimeToLive.
//
// Both fields are ,omitempty, which is why nothing caught this: an untagged table with
// no TTL answers a clean body, and the tests that do tag a table read the tags back
// through ListTagsOfResource (dynamodb_plugin_test.go), which was always correct, and
// never look at what DescribeTable says.
//
// The assertions below are on decoded top-level keys rather than on a struct, because
// a struct cannot distinguish an absent member from an empty one — the same reason
// iam_shape_members_test.go works that way. What is asserted is membership of the key
// set, so a member substrate adds later is not failed by this test unless AWS does not
// publish it.

// ddbTableDescriptionMembers is API_TableDescription's member set, verbatim from the
// page. A key outside this set is a member AWS does not publish, whatever its value.
var ddbTableDescriptionMembers = map[string]bool{
	"ArchivalSummary": true, "AttributeDefinitions": true, "BillingModeSummary": true,
	"CreationDateTime": true, "DeletionProtectionEnabled": true,
	"GlobalSecondaryIndexes": true, "GlobalTableSettingsReplicationMode": true,
	"GlobalTableVersion": true, "GlobalTableWitnesses": true, "ItemCount": true,
	"KeySchema": true, "LatestStreamArn": true, "LatestStreamLabel": true,
	"LocalSecondaryIndexes": true, "MultiRegionConsistency": true,
	"OnDemandThroughput": true, "ProvisionedThroughput": true, "Replicas": true,
	"RestoreSummary": true, "SSEDescription": true, "StreamSpecification": true,
	"TableArn": true, "TableClassSummary": true, "TableId": true, "TableName": true,
	"TableSizeBytes": true, "TableStatus": true, "VectorIndexes": true,
	"WarmThroughput": true,
}

// ddbDescriptionKeys decodes one table-description body and returns the description's
// own top-level keys, alongside the raw bytes so a failure names what was reported.
func ddbDescriptionKeys(t *testing.T, resp *http.Response, wrapper string) ([]string, string) {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body")
	require.NoErrorf(t, resp.Body.Close(), "close body")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: %s", wrapper, raw)

	var out map[string]json.RawMessage
	require.NoErrorf(t, json.Unmarshal(raw, &out), "decode %s: %s", wrapper, raw)
	inner, ok := out[wrapper]
	require.Truef(t, ok, "%s present: %s", wrapper, raw)

	var desc map[string]json.RawMessage
	require.NoErrorf(t, json.Unmarshal(inner, &desc), "decode %s body: %s", wrapper, raw)

	keys := make([]string, 0, len(desc))
	for k := range desc {
		keys = append(keys, k)
	}
	return keys, string(raw)
}

// assertOnlyTableDescriptionMembers fails naming every key AWS does not publish.
func assertOnlyTableDescriptionMembers(t *testing.T, keys []string, raw, where string) {
	t.Helper()
	for _, k := range keys {
		assert.Truef(t, ddbTableDescriptionMembers[k],
			"%s reports %q, which is not a member of API_TableDescription: %s", where, k, raw)
	}
}

// TestDynamoDB_TableDescriptionReportsOnlyItsOwnMembers is #1013 at all four sites
// that render a table description, with the two leaking fields set.
//
// The table is created with a tag so Tags is non-empty, and TTL is enabled before the
// reads so TTLAttribute is non-empty; with both unset every body passes trivially,
// which is how this survived.
func TestDynamoDB_TableDescriptionReportsOnlyItsOwnMembers(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	create := map[string]any{
		"TableName":            "members",
		"AttributeDefinitions": []map[string]string{{"AttributeName": "id", "AttributeType": "S"}},
		"KeySchema":            []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
		"BillingMode":          "PAY_PER_REQUEST",
		"Tags":                 []map[string]string{{"Key": "env", "Value": "test"}},
	}

	t.Run("CreateTable", func(t *testing.T) {
		keys, raw := ddbDescriptionKeys(t, dynamodbRequest(t, srv, "CreateTable", create), "TableDescription")
		assertOnlyTableDescriptionMembers(t, keys, raw, "CreateTable")
	})

	resp := dynamodbRequest(t, srv, "UpdateTimeToLive", map[string]any{
		"TableName": "members",
		"TimeToLiveSpecification": map[string]any{
			"Enabled":       true,
			"AttributeName": "expiresAt",
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, "UpdateTimeToLive")
	require.NoError(t, resp.Body.Close(), "close UpdateTimeToLive body")

	t.Run("DescribeTable", func(t *testing.T) {
		keys, raw := ddbDescriptionKeys(t, dynamodbRequest(t, srv,
			"DescribeTable", map[string]any{"TableName": "members"}), "Table")
		assertOnlyTableDescriptionMembers(t, keys, raw, "DescribeTable")
	})

	t.Run("UpdateTable", func(t *testing.T) {
		keys, raw := ddbDescriptionKeys(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
			"TableName":   "members",
			"BillingMode": "PROVISIONED",
			"ProvisionedThroughput": map[string]any{
				"ReadCapacityUnits":  5,
				"WriteCapacityUnits": 5,
			},
		}), "TableDescription")
		assertOnlyTableDescriptionMembers(t, keys, raw, "UpdateTable")
	})

	t.Run("DeleteTable", func(t *testing.T) {
		keys, raw := ddbDescriptionKeys(t, dynamodbRequest(t, srv,
			"DeleteTable", map[string]any{"TableName": "members"}), "TableDescription")
		assertOnlyTableDescriptionMembers(t, keys, raw, "DeleteTable")
	})
}

// TestDynamoDB_TagsAndTTLStayReadableThroughTheirOwnOperations is the other half of
// #1013: the projection must remove the members, not the state behind them.
//
// A fix that dropped the fields from the record would pass the test above and lose
// what a caller is entitled to read. Both values go in through the operation that
// writes them and come back out through the operation AWS publishes them on.
func TestDynamoDB_TagsAndTTLStayReadableThroughTheirOwnOperations(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName":            "readable",
		"AttributeDefinitions": []map[string]string{{"AttributeName": "id", "AttributeType": "S"}},
		"KeySchema":            []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
		"BillingMode":          "PAY_PER_REQUEST",
		"Tags":                 []map[string]string{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateTable")
	require.NoError(t, resp.Body.Close(), "close CreateTable body")

	resp = dynamodbRequest(t, srv, "UpdateTimeToLive", map[string]any{
		"TableName": "readable",
		"TimeToLiveSpecification": map[string]any{
			"Enabled":       true,
			"AttributeName": "expiresAt",
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, "UpdateTimeToLive")
	require.NoError(t, resp.Body.Close(), "close UpdateTimeToLive body")

	arn := "arn:aws:dynamodb:us-east-1:123456789012:table/readable"

	t.Run("the tag is readable through ListTagsOfResource", func(t *testing.T) {
		var out struct {
			Tags []emulator.DynamoDBTag `json:"Tags"`
		}
		decodeDynamoJSON(t, dynamodbRequest(t, srv,
			"ListTagsOfResource", map[string]any{"ResourceArn": arn}), &out)
		assert.Equal(t, []emulator.DynamoDBTag{{Key: "env", Value: "test"}}, out.Tags)
	})

	t.Run("the TTL attribute is readable through DescribeTimeToLive", func(t *testing.T) {
		var out struct {
			TimeToLiveDescription struct {
				AttributeName    string `json:"AttributeName"`
				TimeToLiveStatus string `json:"TimeToLiveStatus"`
			} `json:"TimeToLiveDescription"`
		}
		decodeDynamoJSON(t, dynamodbRequest(t, srv,
			"DescribeTimeToLive", map[string]any{"TableName": "readable"}), &out)
		assert.Equal(t, "expiresAt", out.TimeToLiveDescription.AttributeName)
		assert.Equal(t, "ENABLED", out.TimeToLiveDescription.TimeToLiveStatus)
	})
}
