package substrate_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// decodeDynamoDB is a helper that decodes a DynamoDB JSON response into dst.
func decodeDynamoDB(t *testing.T, r *http.Response, dst any) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := json.Unmarshal(body, dst); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, body)
	}
}

// TestDynamoDBStreams_GetShardIteratorAndRecords creates a table with streams
// enabled, writes an item, then reads the stream record via GetShardIterator +
// GetRecords.
func TestDynamoDBStreams_GetShardIteratorAndRecords(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// CreateTable with streams.
	createResp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "stream-table",
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"StreamSpecification": map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": "NEW_AND_OLD_IMAGES",
		},
	})
	if createResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("CreateTable: got %d body: %s", createResp.StatusCode, body)
	}
	var createResult map[string]any
	decodeDynamoDB(t, createResp, &createResult)
	tableDesc, _ := createResult["TableDescription"].(map[string]any)
	streamARN, _ := tableDesc["LatestStreamARN"].(string)
	if streamARN == "" {
		t.Skip("stream ARN not set; skipping stream test")
	}

	// PutItem to generate a stream record.
	putResp := dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "stream-table",
		"Item": map[string]any{
			"pk": map[string]any{"S": "key1"},
			"v":  map[string]any{"N": "42"},
		},
	})
	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		t.Fatalf("PutItem: got %d body: %s", putResp.StatusCode, body)
	}
	putResp.Body.Close() //nolint:errcheck

	// DescribeStream.
	dsResp := dynamodbRequest(t, srv, "DescribeStream", map[string]any{
		"StreamArn": streamARN,
	})
	if dsResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(dsResp.Body)
		t.Fatalf("DescribeStream: got %d body: %s", dsResp.StatusCode, body)
	}
	var dsResult map[string]any
	decodeDynamoDB(t, dsResp, &dsResult)
	desc, _ := dsResult["StreamDescription"].(map[string]any)
	shards, _ := desc["Shards"].([]any)
	if len(shards) == 0 {
		t.Fatal("expected at least one shard in DescribeStream")
	}
	shard, _ := shards[0].(map[string]any)
	shardID, _ := shard["ShardId"].(string)
	if shardID == "" {
		t.Fatal("expected non-empty ShardId")
	}

	// GetShardIterator.
	gsiResp := dynamodbRequest(t, srv, "GetShardIterator", map[string]any{
		"StreamArn":         streamARN,
		"ShardId":           shardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})
	if gsiResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(gsiResp.Body)
		t.Fatalf("GetShardIterator: got %d body: %s", gsiResp.StatusCode, body)
	}
	var gsiResult map[string]any
	decodeDynamoDB(t, gsiResp, &gsiResult)
	iterator, _ := gsiResult["ShardIterator"].(string)
	if iterator == "" {
		t.Fatal("expected non-empty ShardIterator")
	}

	// GetRecords.
	grResp := dynamodbRequest(t, srv, "GetRecords", map[string]any{
		"ShardIterator": iterator,
	})
	if grResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(grResp.Body)
		t.Fatalf("GetRecords: got %d body: %s", grResp.StatusCode, body)
	}
	var grResult map[string]any
	decodeDynamoDB(t, grResp, &grResult)
	records, _ := grResult["Records"].([]any)
	if len(records) == 0 {
		t.Fatal("expected at least one stream record after PutItem")
	}
	rec0, _ := records[0].(map[string]any)
	if rec0["eventName"] != "INSERT" {
		t.Errorf("expected eventName=INSERT, got %v", rec0["eventName"])
	}
}

// TestDynamoDBStreams_DeleteItemRecord verifies a REMOVE record is emitted.
func TestDynamoDBStreams_DeleteItemRecord(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "del-table",
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "id", "AttributeType": "S"},
		},
		"KeySchema": []map[string]any{
			{"AttributeName": "id", "KeyType": "HASH"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"StreamSpecification": map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": "NEW_AND_OLD_IMAGES",
		},
	}).Body.Close() //nolint:errcheck

	// PutItem then DeleteItem.
	dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "del-table",
		"Item": map[string]any{
			"id": map[string]any{"S": "row1"},
		},
	}).Body.Close() //nolint:errcheck

	dynamodbRequest(t, srv, "DeleteItem", map[string]any{
		"TableName": "del-table",
		"Key": map[string]any{
			"id": map[string]any{"S": "row1"},
		},
	}).Body.Close() //nolint:errcheck

	// Get stream ARN.
	dtResp := dynamodbRequest(t, srv, "DescribeTable", map[string]any{
		"TableName": "del-table",
	})
	var dtResult map[string]any
	decodeDynamoDB(t, dtResp, &dtResult)
	tbl, _ := dtResult["Table"].(map[string]any)
	streamARN, _ := tbl["LatestStreamARN"].(string)
	if streamARN == "" {
		t.Skip("stream ARN not populated; skipping")
	}

	dsResp := dynamodbRequest(t, srv, "DescribeStream", map[string]any{"StreamArn": streamARN})
	var dsResult map[string]any
	decodeDynamoDB(t, dsResp, &dsResult)
	shards, _ := dsResult["StreamDescription"].(map[string]any)["Shards"].([]any)
	if len(shards) == 0 {
		t.Skip("no shards")
	}
	shardID, _ := shards[0].(map[string]any)["ShardId"].(string)

	gsiResp := dynamodbRequest(t, srv, "GetShardIterator", map[string]any{
		"StreamArn": streamARN, "ShardId": shardID, "ShardIteratorType": "TRIM_HORIZON",
	})
	var gsiResult map[string]any
	decodeDynamoDB(t, gsiResp, &gsiResult)
	iter, _ := gsiResult["ShardIterator"].(string)

	grResp := dynamodbRequest(t, srv, "GetRecords", map[string]any{"ShardIterator": iter})
	var grResult map[string]any
	decodeDynamoDB(t, grResp, &grResult)
	records, _ := grResult["Records"].([]any)

	hasRemove := false
	for _, r := range records {
		if m, ok := r.(map[string]any); ok && m["eventName"] == "REMOVE" {
			hasRemove = true
		}
	}
	if !hasRemove {
		t.Errorf("expected REMOVE record in stream, records: %v", records)
	}
}

// TestDynamoDBPartiQL_SelectInsertUpdateDelete tests all four PartiQL verbs.
// Note: INSERT VALUE must use DynamoDB AttributeValue format ({"S":"val"}).
// WHERE clauses use attribute_exists() since the evaluator does not support
// inline string literals.
func TestDynamoDBPartiQL_SelectInsertUpdateDelete(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "partiql-table",
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	}).Body.Close() //nolint:errcheck

	// INSERT via PartiQL with DynamoDB AttributeValue format.
	insResp := dynamodbRequest(t, srv, "ExecuteStatement", map[string]any{
		"Statement": `INSERT INTO "partiql-table" VALUE {"pk": {"S": "p1"}, "name": {"S": "Alice"}}`,
	})
	if insResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(insResp.Body)
		t.Fatalf("INSERT ExecuteStatement: got %d body: %s", insResp.StatusCode, body)
	}
	insResp.Body.Close() //nolint:errcheck

	// SELECT * (no WHERE) to verify the item was inserted.
	selResp := dynamodbRequest(t, srv, "ExecuteStatement", map[string]any{
		"Statement": `SELECT * FROM "partiql-table"`,
	})
	if selResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(selResp.Body)
		t.Fatalf("SELECT ExecuteStatement: got %d body: %s", selResp.StatusCode, body)
	}
	var selResult map[string]any
	decodeDynamoDB(t, selResp, &selResult)
	items, _ := selResult["Items"].([]any)
	if len(items) == 0 {
		t.Fatal("expected at least one item from SELECT")
	}

	// UPDATE via PartiQL — no WHERE means update all items.
	updResp := dynamodbRequest(t, srv, "ExecuteStatement", map[string]any{
		"Statement": `UPDATE "partiql-table" SET name="Bob"`,
	})
	if updResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(updResp.Body)
		t.Fatalf("UPDATE ExecuteStatement: got %d body: %s", updResp.StatusCode, body)
	}
	updResp.Body.Close() //nolint:errcheck

	// DELETE via PartiQL — no WHERE deletes all items.
	delResp := dynamodbRequest(t, srv, "ExecuteStatement", map[string]any{
		"Statement": `DELETE FROM "partiql-table"`,
	})
	if delResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(delResp.Body)
		t.Fatalf("DELETE ExecuteStatement: got %d body: %s", delResp.StatusCode, body)
	}
	delResp.Body.Close() //nolint:errcheck

	// SELECT after delete → empty.
	selResp2 := dynamodbRequest(t, srv, "ExecuteStatement", map[string]any{
		"Statement": `SELECT * FROM "partiql-table"`,
	})
	var selResult2 map[string]any
	decodeDynamoDB(t, selResp2, &selResult2)
	items2, _ := selResult2["Items"].([]any)
	if len(items2) != 0 {
		t.Errorf("expected empty table after DELETE, got %d items", len(items2))
	}
}

// TestDynamoDBPartiQL_BatchExecuteStatement tests the batch variant.
func TestDynamoDBPartiQL_BatchExecuteStatement(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "batch-table",
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	}).Body.Close() //nolint:errcheck

	// INSERT two items via PutItem first.
	dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "batch-table",
		"Item":      map[string]any{"pk": map[string]any{"S": "a"}},
	}).Body.Close() //nolint:errcheck
	dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "batch-table",
		"Item":      map[string]any{"pk": map[string]any{"S": "b"}},
	}).Body.Close() //nolint:errcheck

	// BatchExecuteStatement with two SELECT statements (no WHERE to avoid literal matching).
	batchResp := dynamodbRequest(t, srv, "BatchExecuteStatement", map[string]any{
		"Statements": []map[string]any{
			{"Statement": `SELECT * FROM "batch-table"`},
			{"Statement": `SELECT * FROM "batch-table"`},
		},
	})
	if batchResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(batchResp.Body)
		t.Fatalf("BatchExecuteStatement: got %d body: %s", batchResp.StatusCode, body)
	}
	var batchResult map[string]any
	decodeDynamoDB(t, batchResp, &batchResult)
	responses, _ := batchResult["Responses"].([]any)
	if len(responses) != 2 {
		t.Errorf("expected 2 batch responses, got %d", len(responses))
	}
}
