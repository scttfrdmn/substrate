package emulator_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ddbWarm is a WarmThroughput description.
type ddbWarm struct {
	ReadUnitsPerSecond  int64
	WriteUnitsPerSecond int64
	Status              string
}

// ddbSettingsDescription is the part of a table description these tests read.
type ddbSettingsDescription struct {
	WarmThroughput         *ddbWarm
	GlobalSecondaryIndexes []struct {
		IndexName      string
		WarmThroughput *ddbWarm
	}
	SSEDescription *struct {
		Status          string
		SSEType         string
		KMSMasterKeyArn string
	}
}

func ddbSettingsCreateBody(name string, extra map[string]any) map[string]any {
	body := map[string]any{
		"TableName": name,
		"KeySchema": []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]string{
			{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "g", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	}
	for k, v := range extra {
		body[k] = v
	}
	return body
}

// Terraform's waitTableWarmThroughputActive polled for WarmThroughput.Status ACTIVE on the table and on
// every GSI, and spun on every table because the member was absent (#1608).
func TestDynamoDB_WarmThroughputIsReported(t *testing.T) {
	t.Parallel()
	srv := newDynamoDBTestServer(t)

	var created struct{ TableDescription ddbSettingsDescription }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "CreateTable", ddbSettingsCreateBody("warm", map[string]any{
		"GlobalSecondaryIndexes": []map[string]any{
			{"IndexName": "by-g", "KeySchema": []map[string]string{{"AttributeName": "g", "KeyType": "HASH"}},
				"Projection": map[string]string{"ProjectionType": "ALL"}},
			{"IndexName": "by-g-warm", "KeySchema": []map[string]string{{"AttributeName": "g", "KeyType": "HASH"}},
				"Projection":     map[string]string{"ProjectionType": "ALL"},
				"WarmThroughput": map[string]int64{"ReadUnitsPerSecond": 20000}},
		},
	})), &created)
	// The developer guide: "A new on-demand table starts with a warm throughput of 12,000 read units per
	// second and 4,000 write units per second."
	assert.Equal(t, &ddbWarm{12000, 4000, "ACTIVE"}, created.TableDescription.WarmThroughput)
	require.Len(t, created.TableDescription.GlobalSecondaryIndexes, 2)
	assert.Equal(t, &ddbWarm{12000, 4000, "ACTIVE"}, created.TableDescription.GlobalSecondaryIndexes[0].WarmThroughput)
	assert.Equal(t, &ddbWarm{20000, 4000, "ACTIVE"}, created.TableDescription.GlobalSecondaryIndexes[1].WarmThroughput,
		"a requested value above the default is reported")

	// UpdateTable raises warm throughput, and a lower request leaves it where it was: "After warm
	// throughput is increased, the values can't be decreased."
	var updated struct{ TableDescription ddbSettingsDescription }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "warm", "WarmThroughput": map[string]int64{"ReadUnitsPerSecond": 30000, "WriteUnitsPerSecond": 10000},
	}), &updated)
	assert.Equal(t, &ddbWarm{30000, 10000, "ACTIVE"}, updated.TableDescription.WarmThroughput)
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "warm", "WarmThroughput": map[string]int64{"ReadUnitsPerSecond": 15000},
	}), &updated)
	assert.Equal(t, &ddbWarm{30000, 10000, "ACTIVE"}, updated.TableDescription.WarmThroughput)

	// Provisioned capacity above the warm value raises it: "if you set the provisioned throughput to
	// 50,000 RCU, the warm throughput will increase to 50,000 read units per second."
	var described struct{ Table ddbSettingsDescription }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "warm", "BillingMode": "PROVISIONED",
		"ProvisionedThroughput": map[string]int64{"ReadCapacityUnits": 50000, "WriteCapacityUnits": 5},
	}), &updated)
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "DescribeTable", map[string]any{"TableName": "warm"}), &described)
	assert.Equal(t, &ddbWarm{50000, 10000, "ACTIVE"}, described.Table.WarmThroughput)
	assert.Equal(t, &ddbWarm{12000, 4000, "ACTIVE"}, described.Table.GlobalSecondaryIndexes[0].WarmThroughput,
		"the stored index is not changed by rendering it")
}

// With server_side_encryption { kms_key_arn }, Terraform's SSE waiter polled for SSEDescription and hung
// because CreateTable discarded the SSESpecification (#1608).
func TestDynamoDB_SSEDescriptionFollowsTheSpecification(t *testing.T) {
	t.Parallel()
	srv := newDynamoDBTestServer(t)
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"

	cases := []struct {
		name string
		spec map[string]any
		want string // KMSMasterKeyArn; "-" for no SSEDescription
	}{
		{"no specification is an AWS owned key", nil, "-"},
		{"disabled is an AWS owned key", map[string]any{"Enabled": false}, "-"},
		{"enabled with a key ARN", map[string]any{"Enabled": true, "SSEType": "KMS", "KMSMasterKeyId": keyARN}, keyARN},
		{"enabled with a key ID", map[string]any{"Enabled": true, "KMSMasterKeyId": "1234abcd-12ab-34cd-56ef-1234567890ab"}, keyARN},
		{"enabled with an alias", map[string]any{"Enabled": true, "KMSMasterKeyId": "alias/tables"},
			"arn:aws:kms:us-east-1:123456789012:alias/tables"},
		{"enabled with the AWS managed key", map[string]any{"Enabled": true}, ""},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			extra := map[string]any{}
			if tc.spec != nil {
				extra["SSESpecification"] = tc.spec
			}
			name := "sse-" + string(rune('a'+i))
			resp := dynamodbRequest(t, srv, "CreateTable", ddbSettingsCreateBody(name, extra))
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			var described struct{ Table ddbSettingsDescription }
			decodeDynamoJSON(t, dynamodbRequest(t, srv, "DescribeTable", map[string]any{"TableName": name}), &described)
			if tc.want == "-" {
				assert.Nil(t, described.Table.SSEDescription)
				return
			}
			require.NotNil(t, described.Table.SSEDescription)
			assert.Equal(t, "ENABLED", described.Table.SSEDescription.Status)
			assert.Equal(t, "KMS", described.Table.SSEDescription.SSEType)
			assert.Equal(t, tc.want, described.Table.SSEDescription.KMSMasterKeyArn)
		})
	}
}

func TestDynamoDB_UpdateTableChangesEncryption(t *testing.T) {
	t.Parallel()
	srv := newDynamoDBTestServer(t)
	resp := dynamodbRequest(t, srv, "CreateTable", ddbSettingsCreateBody("sse-update", nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	var updated struct{ TableDescription ddbSettingsDescription }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "sse-update", "SSESpecification": map[string]any{"Enabled": true, "KMSMasterKeyId": "k1"},
	}), &updated)
	require.NotNil(t, updated.TableDescription.SSEDescription)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/k1", updated.TableDescription.SSEDescription.KMSMasterKeyArn)

	decodeDynamoJSON(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "sse-update", "BillingMode": "PAY_PER_REQUEST",
	}), &updated)
	require.NotNil(t, updated.TableDescription.SSEDescription, "an update without a specification leaves encryption alone")

	var disabled struct{ TableDescription ddbSettingsDescription }
	decodeDynamoJSON(t, dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "sse-update", "SSESpecification": map[string]any{"Enabled": false},
	}), &disabled)
	assert.Nil(t, disabled.TableDescription.SSEDescription, "Enabled false returns the table to an AWS owned key")
}

func TestDynamoDB_TableSettingsRefusals(t *testing.T) {
	t.Parallel()
	srv := newDynamoDBTestServer(t)
	resp := dynamodbRequest(t, srv, "CreateTable", ddbSettingsCreateBody("settings-refusals", nil))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	cases := []struct {
		name string
		op   string
		body map[string]any
	}{
		{"create unknown SSEType", "CreateTable", ddbSettingsCreateBody("bad-sse",
			map[string]any{"SSESpecification": map[string]any{"Enabled": true, "SSEType": "ROT13"}})},
		{"create negative warm throughput", "CreateTable", ddbSettingsCreateBody("bad-warm",
			map[string]any{"WarmThroughput": map[string]int64{"ReadUnitsPerSecond": -1}})},
		{"create negative index warm throughput", "CreateTable", ddbSettingsCreateBody("bad-gsi-warm",
			map[string]any{"GlobalSecondaryIndexes": []map[string]any{{
				"IndexName": "i", "KeySchema": []map[string]string{{"AttributeName": "g", "KeyType": "HASH"}},
				"Projection":     map[string]string{"ProjectionType": "ALL"},
				"WarmThroughput": map[string]int64{"WriteUnitsPerSecond": -5},
			}}})},
		{"update unknown SSEType", "UpdateTable", map[string]any{
			"TableName": "settings-refusals", "SSESpecification": map[string]any{"SSEType": "ROT13"}}},
		{"update negative warm throughput", "UpdateTable", map[string]any{
			"TableName": "settings-refusals", "WarmThroughput": map[string]int64{"WriteUnitsPerSecond": -1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := dynamodbRequest(t, srv, tc.op, tc.body)
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", raw)
			assert.Contains(t, string(raw), "ValidationException")
		})
	}
}
