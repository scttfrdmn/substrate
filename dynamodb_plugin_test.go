package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newDynamoDBTestServer creates a test server with DynamoDB plugin registered.
func newDynamoDBTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	dynamoPlugin := &substrate.DynamoDBPlugin{}
	require.NoError(t, dynamoPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(dynamoPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// dynamodbRequest posts a DynamoDB JSON request to the test server.
// A fake SigV4 Authorization header is included so the server maps the
// request to account 123456789012 (testAccountID), matching the account
// used by the CFN deployer.
func dynamodbRequest(t *testing.T, srv *substrate.Server, op string, body any) *http.Response {
	t.Helper()
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "dynamodb.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
	// Fake auth header: maps AKIA key prefix to account 123456789012.
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=fake")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// decodeDynamoJSON decodes the JSON response body into dst.
func decodeDynamoJSON(t *testing.T, r *http.Response, dst any) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, dst))
}

// av returns a string AttributeValue.
func av(s string) map[string]any {
	return map[string]any{"S": s}
}

// avn returns a number AttributeValue.
func avn(n string) map[string]any {
	return map[string]any{"N": n}
}

func TestDynamoDB_TableLifecycle(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Users",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var createResult map[string]any
	decodeDynamoJSON(t, resp, &createResult)
	desc := createResult["TableDescription"].(map[string]any)
	assert.Equal(t, "Users", desc["TableName"])
	assert.Equal(t, "ACTIVE", desc["TableStatus"])
	assert.Contains(t, desc["TableARN"].(string), "arn:aws:dynamodb")

	// Describe table.
	resp = dynamodbRequest(t, srv, "DescribeTable", map[string]any{
		"TableName": "Users",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var descResult map[string]any
	decodeDynamoJSON(t, resp, &descResult)
	tbl := descResult["Table"].(map[string]any)
	assert.Equal(t, "Users", tbl["TableName"])
	assert.Equal(t, "ACTIVE", tbl["TableStatus"])

	// List tables.
	resp = dynamodbRequest(t, srv, "ListTables", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var listResult map[string]any
	decodeDynamoJSON(t, resp, &listResult)
	tableNames := listResult["TableNames"].([]any)
	assert.Equal(t, 1, len(tableNames))
	assert.Equal(t, "Users", tableNames[0])

	// Duplicate create → ResourceInUseException.
	resp = dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Users",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	var errResult map[string]any
	decodeDynamoJSON(t, resp, &errResult)
	assert.Contains(t, errResult["Code"].(string), "ResourceInUseException")

	// Delete table.
	resp = dynamodbRequest(t, srv, "DeleteTable", map[string]any{
		"TableName": "Users",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var deleteResult map[string]any
	decodeDynamoJSON(t, resp, &deleteResult)
	deletedDesc := deleteResult["TableDescription"].(map[string]any)
	assert.Equal(t, "DELETING", deletedDesc["TableStatus"])

	// Describe after delete → ResourceNotFoundException.
	resp = dynamodbRequest(t, srv, "DescribeTable", map[string]any{
		"TableName": "Users",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDynamoDB_ItemCRUD(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with composite key.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Orders",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
			map[string]any{"AttributeName": "OrderID", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
			map[string]any{"AttributeName": "OrderID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PutItem.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "Orders",
		"Item": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
			"Amount":  avn("99.99"),
			"Status":  av("pending"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetItem.
	resp = dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName": "Orders",
		"Key": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var getResult map[string]any
	decodeDynamoJSON(t, resp, &getResult)
	item := getResult["Item"].(map[string]any)
	assert.Equal(t, "user1", item["UserID"].(map[string]any)["S"])
	assert.Equal(t, "99.99", item["Amount"].(map[string]any)["N"])

	// GetItem — non-existent returns empty.
	resp = dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName": "Orders",
		"Key": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order999"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var getMissingResult map[string]any
	decodeDynamoJSON(t, resp, &getMissingResult)
	_, hasItem := getMissingResult["Item"]
	assert.False(t, hasItem)

	// UpdateItem — SET.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName": "Orders",
		"Key": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
		},
		"UpdateExpression":          "SET #s = :newStatus",
		"ExpressionAttributeNames":  map[string]any{"#s": "Status"},
		"ExpressionAttributeValues": map[string]any{":newStatus": av("shipped")},
		"ReturnValues":              "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var updateResult map[string]any
	decodeDynamoJSON(t, resp, &updateResult)
	updatedAttrs := updateResult["Attributes"].(map[string]any)
	assert.Equal(t, "shipped", updatedAttrs["Status"].(map[string]any)["S"])

	// UpdateItem — REMOVE.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName": "Orders",
		"Key": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
		},
		"UpdateExpression":         "REMOVE #s",
		"ExpressionAttributeNames": map[string]any{"#s": "Status"},
		"ReturnValues":             "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var removeResult map[string]any
	decodeDynamoJSON(t, resp, &removeResult)
	afterRemove := removeResult["Attributes"].(map[string]any)
	_, hasStatus := afterRemove["Status"]
	assert.False(t, hasStatus)

	// Conditional PutItem — attribute_exists check (item exists, condition passes).
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "Orders",
		"Item": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
			"Status":  av("delivered"),
		},
		"ConditionExpression": "attribute_exists(UserID)",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Conditional PutItem — attribute_not_exists check (item exists, condition fails).
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "Orders",
		"Item": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
			"Status":  av("canceled"),
		},
		"ConditionExpression": "attribute_not_exists(UserID)",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var condErr map[string]any
	decodeDynamoJSON(t, resp, &condErr)
	assert.Equal(t, "ConditionalCheckFailedException", condErr["Code"])

	// DeleteItem.
	resp = dynamodbRequest(t, srv, "DeleteItem", map[string]any{
		"TableName": "Orders",
		"Key": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
		},
		"ReturnValues": "ALL_OLD",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var delResult map[string]any
	decodeDynamoJSON(t, resp, &delResult)
	_, hasOldAttrs := delResult["Attributes"]
	assert.True(t, hasOldAttrs)

	// GetItem after delete — not found.
	resp = dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName": "Orders",
		"Key": map[string]any{
			"UserID":  av("user1"),
			"OrderID": av("order1"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var afterDelResult map[string]any
	decodeDynamoJSON(t, resp, &afterDelResult)
	_, hasItemAfterDel := afterDelResult["Item"]
	assert.False(t, hasItemAfterDel)
}

func TestDynamoDB_BatchOperations(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Products",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ProductID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ProductID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// BatchWriteItem — put multiple items.
	resp = dynamodbRequest(t, srv, "BatchWriteItem", map[string]any{
		"RequestItems": map[string]any{
			"Products": []any{
				map[string]any{"PutRequest": map[string]any{
					"Item": map[string]any{
						"ProductID": av("p1"),
						"Name":      av("Widget A"),
						"Price":     avn("9.99"),
					},
				}},
				map[string]any{"PutRequest": map[string]any{
					"Item": map[string]any{
						"ProductID": av("p2"),
						"Name":      av("Widget B"),
						"Price":     avn("19.99"),
					},
				}},
				map[string]any{"PutRequest": map[string]any{
					"Item": map[string]any{
						"ProductID": av("p3"),
						"Name":      av("Widget C"),
						"Price":     avn("29.99"),
					},
				}},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var batchWriteResult map[string]any
	decodeDynamoJSON(t, resp, &batchWriteResult)
	unprocessed := batchWriteResult["UnprocessedItems"].(map[string]any)
	assert.Empty(t, unprocessed)

	// BatchGetItem — retrieve two of the items.
	resp = dynamodbRequest(t, srv, "BatchGetItem", map[string]any{
		"RequestItems": map[string]any{
			"Products": map[string]any{
				"Keys": []any{
					map[string]any{"ProductID": av("p1")},
					map[string]any{"ProductID": av("p3")},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var batchGetResult map[string]any
	decodeDynamoJSON(t, resp, &batchGetResult)
	responses := batchGetResult["Responses"].(map[string]any)
	products := responses["Products"].([]any)
	assert.Equal(t, 2, len(products))

	// BatchWriteItem — delete one item.
	resp = dynamodbRequest(t, srv, "BatchWriteItem", map[string]any{
		"RequestItems": map[string]any{
			"Products": []any{
				map[string]any{"DeleteRequest": map[string]any{
					"Key": map[string]any{"ProductID": av("p2")},
				}},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify p2 is gone.
	resp = dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName": "Products",
		"Key":       map[string]any{"ProductID": av("p2")},
	})
	var getAfterBatch map[string]any
	decodeDynamoJSON(t, resp, &getAfterBatch)
	_, hasP2 := getAfterBatch["Item"]
	assert.False(t, hasP2)
}

func TestDynamoDB_Scan(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table and insert items.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Items",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Insert 5 items.
	for i := 1; i <= 5; i++ {
		category := "A"
		if i > 3 {
			category = "B"
		}
		resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "Items",
			"Item": map[string]any{
				"ID":       av("item" + string(rune('0'+i))),
				"Category": av(category),
				"Value":    avn(string(rune('0' + i))),
			},
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Full scan.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName": "Items",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var scanResult map[string]any
	decodeDynamoJSON(t, resp, &scanResult)
	assert.Equal(t, float64(5), scanResult["Count"])

	// Filtered scan — only Category = "B".
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "Items",
		"FilterExpression":          "#cat = :catVal",
		"ExpressionAttributeNames":  map[string]any{"#cat": "Category"},
		"ExpressionAttributeValues": map[string]any{":catVal": av("B")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var filteredResult map[string]any
	decodeDynamoJSON(t, resp, &filteredResult)
	assert.Equal(t, float64(2), filteredResult["Count"])

	// Paginated scan — Limit=2.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName": "Items",
		"Limit":     2,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var paginatedResult map[string]any
	decodeDynamoJSON(t, resp, &paginatedResult)
	assert.Equal(t, float64(2), paginatedResult["Count"])
	_, hasLastKey := paginatedResult["LastEvaluatedKey"]
	assert.True(t, hasLastKey)
}

func TestDynamoDB_Query(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with composite key.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Events",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
			map[string]any{"AttributeName": "Timestamp", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
			map[string]any{"AttributeName": "Timestamp", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Insert items.
	items := []map[string]any{
		{"UserID": av("alice"), "Timestamp": av("2024-01-01"), "Type": av("login")},
		{"UserID": av("alice"), "Timestamp": av("2024-01-05"), "Type": av("purchase")},
		{"UserID": av("alice"), "Timestamp": av("2024-01-10"), "Type": av("login")},
		{"UserID": av("bob"), "Timestamp": av("2024-01-03"), "Type": av("login")},
	}
	for _, item := range items {
		resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "Events",
			"Item":      item,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Query by PK only.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":              "Events",
		"KeyConditionExpression": "UserID = :uid",
		"ExpressionAttributeValues": map[string]any{
			":uid": av("alice"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var queryResult map[string]any
	decodeDynamoJSON(t, resp, &queryResult)
	assert.Equal(t, float64(3), queryResult["Count"])

	// Query with SK BETWEEN.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":              "Events",
		"KeyConditionExpression": "UserID = :uid AND Timestamp BETWEEN :lo AND :hi",
		"ExpressionAttributeValues": map[string]any{
			":uid": av("alice"),
			":lo":  av("2024-01-02"),
			":hi":  av("2024-01-08"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var betweenResult map[string]any
	decodeDynamoJSON(t, resp, &betweenResult)
	assert.Equal(t, float64(1), betweenResult["Count"])

	// Query with FilterExpression.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":                "Events",
		"KeyConditionExpression":   "UserID = :uid",
		"FilterExpression":         "#t = :type",
		"ExpressionAttributeNames": map[string]any{"#t": "Type"},
		"ExpressionAttributeValues": map[string]any{
			":uid":  av("alice"),
			":type": av("login"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var filterResult map[string]any
	decodeDynamoJSON(t, resp, &filterResult)
	assert.Equal(t, float64(2), filterResult["Count"])

	// Query with ScanIndexForward=false (descending).
	forwardFalse := false
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":              "Events",
		"KeyConditionExpression": "UserID = :uid",
		"ExpressionAttributeValues": map[string]any{
			":uid": av("alice"),
		},
		"ScanIndexForward": forwardFalse,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var descResult map[string]any
	decodeDynamoJSON(t, resp, &descResult)
	descItems := descResult["Items"].([]any)
	require.Equal(t, 3, len(descItems))
	// First item in descending order should have latest timestamp.
	firstItem := descItems[0].(map[string]any)
	assert.Equal(t, "2024-01-10", firstItem["Timestamp"].(map[string]any)["S"])
}

func TestDynamoDB_GSI(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with GSI.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Posts",
		"KeySchema": []any{
			map[string]any{"AttributeName": "PostID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "PostID", "AttributeType": "S"},
			map[string]any{"AttributeName": "AuthorID", "AttributeType": "S"},
			map[string]any{"AttributeName": "CreatedAt", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"GlobalSecondaryIndexes": []any{
			map[string]any{
				"IndexName": "AuthorIndex",
				"KeySchema": []any{
					map[string]any{"AttributeName": "AuthorID", "KeyType": "HASH"},
					map[string]any{"AttributeName": "CreatedAt", "KeyType": "RANGE"},
				},
				"Projection": map[string]any{"ProjectionType": "ALL"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var createResult map[string]any
	decodeDynamoJSON(t, resp, &createResult)
	desc := createResult["TableDescription"].(map[string]any)
	gsis := desc["GlobalSecondaryIndexes"].([]any)
	assert.Equal(t, 1, len(gsis))
	gsi := gsis[0].(map[string]any)
	assert.Equal(t, "AuthorIndex", gsi["IndexName"])
	assert.Equal(t, "ACTIVE", gsi["IndexStatus"])

	// Insert items.
	posts := []map[string]any{
		{"PostID": av("p1"), "AuthorID": av("author1"), "CreatedAt": av("2024-01-01"), "Title": av("Post A")},
		{"PostID": av("p2"), "AuthorID": av("author1"), "CreatedAt": av("2024-01-05"), "Title": av("Post B")},
		{"PostID": av("p3"), "AuthorID": av("author2"), "CreatedAt": av("2024-01-03"), "Title": av("Post C")},
	}
	for _, post := range posts {
		resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "Posts",
			"Item":      post,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Query via GSI.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":              "Posts",
		"IndexName":              "AuthorIndex",
		"KeyConditionExpression": "AuthorID = :aid",
		"ExpressionAttributeValues": map[string]any{
			":aid": av("author1"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var queryResult map[string]any
	decodeDynamoJSON(t, resp, &queryResult)
	assert.Equal(t, float64(2), queryResult["Count"])
}

func TestDynamoDB_TTL(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Sessions",
		"KeySchema": []any{
			map[string]any{"AttributeName": "SessionID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "SessionID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// DescribeTimeToLive — initially disabled.
	resp = dynamodbRequest(t, srv, "DescribeTimeToLive", map[string]any{
		"TableName": "Sessions",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var ttlDesc map[string]any
	decodeDynamoJSON(t, resp, &ttlDesc)
	ttlInfo := ttlDesc["TimeToLiveDescription"].(map[string]any)
	assert.Equal(t, "DISABLED", ttlInfo["TimeToLiveStatus"])

	// UpdateTimeToLive — enable.
	resp = dynamodbRequest(t, srv, "UpdateTimeToLive", map[string]any{
		"TableName": "Sessions",
		"TimeToLiveSpecification": map[string]any{
			"Enabled":       true,
			"AttributeName": "ExpiresAt",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var ttlUpdateResult map[string]any
	decodeDynamoJSON(t, resp, &ttlUpdateResult)
	ttlSpec := ttlUpdateResult["TimeToLiveSpecification"].(map[string]any)
	assert.Equal(t, true, ttlSpec["Enabled"])
	assert.Equal(t, "ExpiresAt", ttlSpec["AttributeName"])

	// DescribeTimeToLive — now enabled.
	resp = dynamodbRequest(t, srv, "DescribeTimeToLive", map[string]any{
		"TableName": "Sessions",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var ttlDesc2 map[string]any
	decodeDynamoJSON(t, resp, &ttlDesc2)
	ttlInfo2 := ttlDesc2["TimeToLiveDescription"].(map[string]any)
	assert.Equal(t, "ENABLED", ttlInfo2["TimeToLiveStatus"])
	assert.Equal(t, "ExpiresAt", ttlInfo2["AttributeName"])

	// UpdateTimeToLive — disable.
	resp = dynamodbRequest(t, srv, "UpdateTimeToLive", map[string]any{
		"TableName": "Sessions",
		"TimeToLiveSpecification": map[string]any{
			"Enabled":       false,
			"AttributeName": "ExpiresAt",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify disabled.
	resp = dynamodbRequest(t, srv, "DescribeTimeToLive", map[string]any{
		"TableName": "Sessions",
	})
	var ttlDesc3 map[string]any
	decodeDynamoJSON(t, resp, &ttlDesc3)
	ttlInfo3 := ttlDesc3["TimeToLiveDescription"].(map[string]any)
	assert.Equal(t, "DISABLED", ttlInfo3["TimeToLiveStatus"])
}

func TestDynamoDB_Streams(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with streams enabled.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "StreamTable",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"StreamSpecification": map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": "NEW_AND_OLD_IMAGES",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var createResult map[string]any
	decodeDynamoJSON(t, resp, &createResult)
	desc := createResult["TableDescription"].(map[string]any)
	assert.NotEmpty(t, desc["LatestStreamARN"])
	streamARN := desc["LatestStreamARN"].(string)

	// ListStreams — should include our table's stream.
	resp = dynamodbRequest(t, srv, "ListStreams", map[string]any{
		"TableName": "StreamTable",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var listResult map[string]any
	decodeDynamoJSON(t, resp, &listResult)
	streams := listResult["Streams"].([]any)
	assert.Equal(t, 1, len(streams))
	stream := streams[0].(map[string]any)
	assert.Equal(t, streamARN, stream["StreamArn"])

	// DescribeStream.
	resp = dynamodbRequest(t, srv, "DescribeStream", map[string]any{
		"StreamArn": streamARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var descResult map[string]any
	decodeDynamoJSON(t, resp, &descResult)
	streamDesc := descResult["StreamDescription"].(map[string]any)
	assert.Equal(t, "ENABLED", streamDesc["StreamStatus"])

	// GetShardIterator.
	resp = dynamodbRequest(t, srv, "GetShardIterator", map[string]any{
		"StreamArn":         streamARN,
		"ShardId":           "shardId-000000000000",
		"ShardIteratorType": "TRIM_HORIZON",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var iterResult map[string]any
	decodeDynamoJSON(t, resp, &iterResult)
	assert.NotEmpty(t, iterResult["ShardIterator"])

	// GetRecords.
	resp = dynamodbRequest(t, srv, "GetRecords", map[string]any{
		"ShardIterator": iterResult["ShardIterator"],
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var recordsResult map[string]any
	decodeDynamoJSON(t, resp, &recordsResult)
	records := recordsResult["Records"].([]any)
	assert.Empty(t, records)
}

func TestCFN_DynamoDB(t *testing.T) {
	cfg := substrate.DefaultConfig()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()

	dynamoPlugin := &substrate.DynamoDBPlugin{}
	require.NoError(t, dynamoPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(dynamoPlugin)

	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})
	deployer := substrate.NewStackDeployer(registry, store, state, tc, logger, costs)

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyTable": {
				"Type": "AWS::DynamoDB::Table",
				"Properties": {
					"TableName": "cfn-test-table",
					"KeySchema": [
						{"AttributeName": "PK", "KeyType": "HASH"},
						{"AttributeName": "SK", "KeyType": "RANGE"}
					],
					"AttributeDefinitions": [
						{"AttributeName": "PK", "AttributeType": "S"},
						{"AttributeName": "SK", "AttributeType": "S"}
					],
					"BillingMode": "PAY_PER_REQUEST"
				}
			}
		}
	}`

	result, err := deployer.Deploy(context.Background(), template, "test-stack", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, len(result.Resources))
	assert.Equal(t, "cfn-test-table", result.Resources[0].PhysicalID)
	assert.Contains(t, result.Resources[0].ARN, "arn:aws:dynamodb")

	// Verify table was actually created.
	dynamoSrv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	resp := dynamodbRequest(t, dynamoSrv, "DescribeTable", map[string]any{
		"TableName": "cfn-test-table",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var descResult map[string]any
	decodeDynamoJSON(t, resp, &descResult)
	tbl := descResult["Table"].(map[string]any)
	assert.Equal(t, "cfn-test-table", tbl["TableName"])
	assert.Equal(t, "ACTIVE", tbl["TableStatus"])
}

// TestCFN_DynamoDB_WithGSIAndTTL exercises deployDynamoDBTable with ProvisionedThroughput,
// GSIs, StreamSpecification, and TimeToLiveSpecification.
func TestCFN_DynamoDB_WithGSIAndTTL(t *testing.T) {
	cfg := substrate.DefaultConfig()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()

	dynamoPlugin := &substrate.DynamoDBPlugin{}
	require.NoError(t, dynamoPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(dynamoPlugin)

	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})
	deployer := substrate.NewStackDeployer(registry, store, state, tc, logger, costs)

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"UsersTable": {
				"Type": "AWS::DynamoDB::Table",
				"Properties": {
					"TableName": "users-table",
					"KeySchema": [
						{"AttributeName": "UserID", "KeyType": "HASH"}
					],
					"AttributeDefinitions": [
						{"AttributeName": "UserID", "AttributeType": "S"},
						{"AttributeName": "Email", "AttributeType": "S"}
					],
					"ProvisionedThroughput": {
						"ReadCapacityUnits": 5,
						"WriteCapacityUnits": 5
					},
					"GlobalSecondaryIndexes": [
						{
							"IndexName": "EmailIndex",
							"KeySchema": [
								{"AttributeName": "Email", "KeyType": "HASH"}
							],
							"Projection": {"ProjectionType": "ALL"},
							"ProvisionedThroughput": {
								"ReadCapacityUnits": 1,
								"WriteCapacityUnits": 1
							}
						}
					],
					"StreamSpecification": {
						"StreamEnabled": true,
						"StreamViewType": "NEW_AND_OLD_IMAGES"
					},
					"TimeToLiveSpecification": {
						"Enabled": true,
						"AttributeName": "ExpiresAt"
					}
				}
			}
		}
	}`

	result, err := deployer.Deploy(context.Background(), template, "users-stack", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, len(result.Resources))
	assert.Equal(t, "users-table", result.Resources[0].PhysicalID)
	assert.Contains(t, result.Resources[0].ARN, "arn:aws:dynamodb")
}

func TestDynamoDB_UpdateItem_AddAndDelete(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Tags",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PutItem with a number and SS attribute.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "Tags",
		"Item": map[string]any{
			"ID":    av("item1"),
			"Count": avn("10"),
			"Tags":  map[string]any{"SS": []any{"alpha", "beta"}},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// ADD numeric value.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "Tags",
		"Key":                       map[string]any{"ID": av("item1")},
		"UpdateExpression":          "ADD #cnt :delta",
		"ExpressionAttributeNames":  map[string]any{"#cnt": "Count"},
		"ExpressionAttributeValues": map[string]any{":delta": avn("5")},
		"ReturnValues":              "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var addResult map[string]any
	decodeDynamoJSON(t, resp, &addResult)
	attrs := addResult["Attributes"].(map[string]any)
	assert.Equal(t, "15", attrs["Count"].(map[string]any)["N"])

	// DELETE from string set.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":        "Tags",
		"Key":              map[string]any{"ID": av("item1")},
		"UpdateExpression": "DELETE Tags :remove",
		"ExpressionAttributeValues": map[string]any{
			":remove": map[string]any{"SS": []any{"beta"}},
		},
		"ReturnValues": "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var delSetResult map[string]any
	decodeDynamoJSON(t, resp, &delSetResult)
	delAttrs := delSetResult["Attributes"].(map[string]any)
	remainingTags := delAttrs["Tags"].(map[string]any)["SS"].([]any)
	assert.Equal(t, 1, len(remainingTags))
	assert.Equal(t, "alpha", remainingTags[0])
}

func TestDynamoDB_UnknownOperation(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "FakeOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var errResult map[string]any
	decodeDynamoJSON(t, resp, &errResult)
	assert.Equal(t, "UnknownOperationException", errResult["Code"])
}

func TestDynamoDB_UpdateTable(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with provisioned throughput.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "Throughput",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PROVISIONED",
		"ProvisionedThroughput": map[string]any{
			"ReadCapacityUnits":  5,
			"WriteCapacityUnits": 5,
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// UpdateTable — change throughput.
	resp = dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "Throughput",
		"ProvisionedThroughput": map[string]any{
			"ReadCapacityUnits":  10,
			"WriteCapacityUnits": 10,
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var updateResult map[string]any
	decodeDynamoJSON(t, resp, &updateResult)
	desc := updateResult["TableDescription"].(map[string]any)
	pt := desc["ProvisionedThroughput"].(map[string]any)
	assert.Equal(t, float64(10), pt["ReadCapacityUnits"])

	// UpdateTable — resource not found.
	resp = dynamodbRequest(t, srv, "UpdateTable", map[string]any{
		"TableName": "NonExistent",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDynamoDB_ExpressionFunctions(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "ExprTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Put an item with various attribute types.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "ExprTest",
		"Item": map[string]any{
			"ID":    av("item1"),
			"Name":  av("Widget"),
			"Count": avn("42"),
			"Tags":  map[string]any{"SS": []any{"a", "b", "c"}},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Scan with begins_with filter.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "ExprTest",
		"FilterExpression":          "begins_with(#n, :prefix)",
		"ExpressionAttributeNames":  map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{":prefix": av("Wid")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var beResult map[string]any
	decodeDynamoJSON(t, resp, &beResult)
	assert.Equal(t, float64(1), beResult["Count"])

	// Scan with contains filter.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "ExprTest",
		"FilterExpression":          "contains(#n, :sub)",
		"ExpressionAttributeNames":  map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{":sub": av("idg")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var containsResult map[string]any
	decodeDynamoJSON(t, resp, &containsResult)
	assert.Equal(t, float64(1), containsResult["Count"])

	// Scan with NOT condition.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "ExprTest",
		"FilterExpression":          "NOT #n = :wrongName",
		"ExpressionAttributeNames":  map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{":wrongName": av("Other")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var notResult map[string]any
	decodeDynamoJSON(t, resp, &notResult)
	assert.Equal(t, float64(1), notResult["Count"])

	// Scan with OR condition.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                "ExprTest",
		"FilterExpression":         "#n = :name OR #n = :other",
		"ExpressionAttributeNames": map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{
			":name":  av("Widget"),
			":other": av("None"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var orResult map[string]any
	decodeDynamoJSON(t, resp, &orResult)
	assert.Equal(t, float64(1), orResult["Count"])

	// Scan with BETWEEN condition on number.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":        "ExprTest",
		"FilterExpression": "Count BETWEEN :lo AND :hi",
		"ExpressionAttributeValues": map[string]any{
			":lo": avn("40"),
			":hi": avn("50"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var betweenResult map[string]any
	decodeDynamoJSON(t, resp, &betweenResult)
	assert.Equal(t, float64(1), betweenResult["Count"])

	// Scan with IN condition.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                "ExprTest",
		"FilterExpression":         "#n IN (:v1, :v2)",
		"ExpressionAttributeNames": map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{
			":v1": av("Widget"),
			":v2": av("Gadget"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var inResult map[string]any
	decodeDynamoJSON(t, resp, &inResult)
	assert.Equal(t, float64(1), inResult["Count"])

	// UpdateItem — SET with arithmetic (Count + delta).
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "ExprTest",
		"Key":                       map[string]any{"ID": av("item1")},
		"UpdateExpression":          "SET Count = Count - :dec",
		"ExpressionAttributeValues": map[string]any{":dec": avn("2")},
		"ReturnValues":              "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var arithResult map[string]any
	decodeDynamoJSON(t, resp, &arithResult)
	arithAttrs := arithResult["Attributes"].(map[string]any)
	assert.Equal(t, "40", arithAttrs["Count"].(map[string]any)["N"])
}

func TestDynamoDB_ListTablesWithPagination(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create multiple tables.
	for _, name := range []string{"TableA", "TableB", "TableC"} {
		resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
			"TableName": name,
			"KeySchema": []any{
				map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
			},
			"AttributeDefinitions": []any{
				map[string]any{"AttributeName": "ID", "AttributeType": "S"},
			},
			"BillingMode": "PAY_PER_REQUEST",
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// List with Limit=2.
	resp := dynamodbRequest(t, srv, "ListTables", map[string]any{
		"Limit": 2,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var page1 map[string]any
	decodeDynamoJSON(t, resp, &page1)
	assert.Equal(t, 2, len(page1["TableNames"].([]any)))
	lastEval, hasLast := page1["LastEvaluatedTableName"]
	assert.True(t, hasLast)

	// List next page.
	resp = dynamodbRequest(t, srv, "ListTables", map[string]any{
		"ExclusiveStartTableName": lastEval,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var page2 map[string]any
	decodeDynamoJSON(t, resp, &page2)
	assert.Equal(t, 1, len(page2["TableNames"].([]any)))
}

func TestDynamoDB_ErrorCases(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// GetItem on non-existent table.
	resp := dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName": "NonExistent",
		"Key":       map[string]any{"ID": av("x")},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// DeleteItem on non-existent table.
	resp = dynamodbRequest(t, srv, "DeleteItem", map[string]any{
		"TableName": "NonExistent",
		"Key":       map[string]any{"ID": av("x")},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// UpdateItem on non-existent table.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "NonExistent",
		"Key":                       map[string]any{"ID": av("x")},
		"UpdateExpression":          "SET Name = :n",
		"ExpressionAttributeValues": map[string]any{":n": av("test")},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Scan on non-existent table.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName": "NonExistent",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Query on non-existent table.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":                 "NonExistent",
		"KeyConditionExpression":    "ID = :id",
		"ExpressionAttributeValues": map[string]any{":id": av("x")},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// DescribeTimeToLive on non-existent table.
	resp = dynamodbRequest(t, srv, "DescribeTimeToLive", map[string]any{
		"TableName": "NonExistent",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// UpdateTimeToLive on non-existent table.
	resp = dynamodbRequest(t, srv, "UpdateTimeToLive", map[string]any{
		"TableName": "NonExistent",
		"TimeToLiveSpecification": map[string]any{
			"Enabled":       true,
			"AttributeName": "Exp",
		},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// DeleteTable — not found.
	resp = dynamodbRequest(t, srv, "DeleteTable", map[string]any{
		"TableName": "NonExistent",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// DescribeTable — not found.
	resp = dynamodbRequest(t, srv, "DescribeTable", map[string]any{
		"TableName": "NonExistent",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDynamoDB_ConditionalDelete(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "CondTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PutItem.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "CondTest",
		"Item": map[string]any{
			"ID":     av("x1"),
			"Status": av("active"),
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteItem with condition (fails).
	resp = dynamodbRequest(t, srv, "DeleteItem", map[string]any{
		"TableName":                 "CondTest",
		"Key":                       map[string]any{"ID": av("x1")},
		"ConditionExpression":       "#s = :inactive",
		"ExpressionAttributeNames":  map[string]any{"#s": "Status"},
		"ExpressionAttributeValues": map[string]any{":inactive": av("inactive")},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var condErr map[string]any
	decodeDynamoJSON(t, resp, &condErr)
	assert.Equal(t, "ConditionalCheckFailedException", condErr["Code"])

	// DeleteItem with condition (succeeds).
	resp = dynamodbRequest(t, srv, "DeleteItem", map[string]any{
		"TableName":                 "CondTest",
		"Key":                       map[string]any{"ID": av("x1")},
		"ConditionExpression":       "#s = :active",
		"ExpressionAttributeNames":  map[string]any{"#s": "Status"},
		"ExpressionAttributeValues": map[string]any{":active": av("active")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDynamoDB_ProjectionExpression(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "ProjTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "ProjTest",
		"Item": map[string]any{
			"ID":     av("i1"),
			"Name":   av("Test"),
			"Secret": av("hidden"),
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// GetItem with ProjectionExpression.
	resp = dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName":            "ProjTest",
		"Key":                  map[string]any{"ID": av("i1")},
		"ProjectionExpression": "ID, Name",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var projResult map[string]any
	decodeDynamoJSON(t, resp, &projResult)
	item := projResult["Item"].(map[string]any)
	_, hasName := item["Name"]
	_, hasSecret := item["Secret"]
	assert.True(t, hasName)
	assert.False(t, hasSecret)
}

// TestDynamoDB_AttributeTypeSizeExpressions covers attribute_type() and size()
// expression functions, which exercise avTypeName and avSize.
func TestDynamoDB_AttributeTypeSizeExpressions(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "TypeSizeTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Put items with string, number, boolean, list, and map attributes.
	for _, item := range []map[string]any{
		{
			"ID":    av("s1"),
			"Name":  av("hello"),
			"Count": avn("5"),
		},
		{
			"ID":    av("s2"),
			"Name":  av("world-long-name"),
			"Count": avn("99"),
		},
		{
			"ID":   av("s3"),
			"Name": av("short"),
		},
	} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "TypeSizeTest",
			"Item":      item,
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// Scan with attribute_type filter — only items where Name is type S.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "TypeSizeTest",
		"FilterExpression":          "attribute_type(#n, :t)",
		"ExpressionAttributeNames":  map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{":t": av("S")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var typeResult map[string]any
	decodeDynamoJSON(t, resp, &typeResult)
	assert.Equal(t, float64(3), typeResult["Count"])

	// Scan with size() filter — items where size(Name) > 5.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "TypeSizeTest",
		"FilterExpression":          "size(#n) > :minLen",
		"ExpressionAttributeNames":  map[string]any{"#n": "Name"},
		"ExpressionAttributeValues": map[string]any{":minLen": avn("5")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var sizeResult map[string]any
	decodeDynamoJSON(t, resp, &sizeResult)
	// "world-long-name" (15) > 5; "short" (5) is not > 5; "hello" (5) is not > 5.
	assert.Equal(t, float64(1), sizeResult["Count"])

	// Scan with AND condition — Count attribute exists AND Count > 50.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "TypeSizeTest",
		"FilterExpression":          "attribute_exists(#c) AND #c > :fifty",
		"ExpressionAttributeNames":  map[string]any{"#c": "Count"},
		"ExpressionAttributeValues": map[string]any{":fifty": avn("50")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var andResult map[string]any
	decodeDynamoJSON(t, resp, &andResult)
	assert.Equal(t, float64(1), andResult["Count"])

	// Scan with attribute_not_exists.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                "TypeSizeTest",
		"FilterExpression":         "attribute_not_exists(#c)",
		"ExpressionAttributeNames": map[string]any{"#c": "Count"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var notExistsResult map[string]any
	decodeDynamoJSON(t, resp, &notExistsResult)
	assert.Equal(t, float64(1), notExistsResult["Count"])
}

// TestDynamoDB_QuerySKConditions exercises all sort key condition operators
// in KeyConditionExpression, covering evalSKCondition paths.
func TestDynamoDB_QuerySKConditions(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with hash+range key.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "SKTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
			map[string]any{"AttributeName": "Score", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
			map[string]any{"AttributeName": "Score", "AttributeType": "N"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Insert items with scores 10..50.
	for _, score := range []string{"10", "20", "30", "40", "50"} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "SKTest",
			"Item": map[string]any{
				"UserID": av("u1"),
				"Score":  avn(score),
			},
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	tests := []struct {
		name   string
		kcExpr string
		vals   map[string]any
		want   int
	}{
		{
			name:   "SK equals",
			kcExpr: "UserID = :uid AND Score = :s",
			vals:   map[string]any{":uid": av("u1"), ":s": avn("30")},
			want:   1,
		},
		{
			name:   "SK less than",
			kcExpr: "UserID = :uid AND Score < :s",
			vals:   map[string]any{":uid": av("u1"), ":s": avn("25")},
			want:   2,
		},
		{
			name:   "SK less than or equal",
			kcExpr: "UserID = :uid AND Score <= :s",
			vals:   map[string]any{":uid": av("u1"), ":s": avn("20")},
			want:   2,
		},
		{
			name:   "SK greater than",
			kcExpr: "UserID = :uid AND Score > :s",
			vals:   map[string]any{":uid": av("u1"), ":s": avn("35")},
			want:   2,
		},
		{
			name:   "SK greater than or equal",
			kcExpr: "UserID = :uid AND Score >= :s",
			vals:   map[string]any{":uid": av("u1"), ":s": avn("40")},
			want:   2,
		},
		{
			name:   "SK BETWEEN",
			kcExpr: "UserID = :uid AND Score BETWEEN :lo AND :hi",
			vals:   map[string]any{":uid": av("u1"), ":lo": avn("20"), ":hi": avn("40")},
			want:   3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := dynamodbRequest(t, srv, "Query", map[string]any{
				"TableName":                 "SKTest",
				"KeyConditionExpression":    tc.kcExpr,
				"ExpressionAttributeValues": tc.vals,
			})
			assert.Equal(t, http.StatusOK, r.StatusCode)
			var result map[string]any
			decodeDynamoJSON(t, r, &result)
			assert.Equal(t, float64(tc.want), result["Count"], tc.name)
		})
	}
}

// TestDynamoDB_NestedAttributeAccess exercises getAttrByPath via FilterExpression
// on nested map attributes and attribute_type with numeric/boolean types.
func TestDynamoDB_NestedAttributeAccess(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "NestedTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PutItem with nested map, boolean, and null attributes.
	trueVal := true
	nullVal := true
	_ = trueVal
	_ = nullVal

	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "NestedTest",
		"Item": map[string]any{
			"ID":     av("n1"),
			"Count":  avn("7"),
			"Active": map[string]any{"BOOL": true},
			"Meta": map[string]any{
				"M": map[string]any{
					"Region": map[string]any{"S": "us-east-1"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "NestedTest",
		"Item": map[string]any{
			"ID":     av("n2"),
			"Count":  avn("3"),
			"Active": map[string]any{"BOOL": false},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Scan with attribute_type N — only numeric Count items.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "NestedTest",
		"FilterExpression":          "attribute_type(#c, :t)",
		"ExpressionAttributeNames":  map[string]any{"#c": "Count"},
		"ExpressionAttributeValues": map[string]any{":t": av("N")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var typeResult map[string]any
	decodeDynamoJSON(t, resp, &typeResult)
	assert.Equal(t, float64(2), typeResult["Count"])

	// Scan with attribute_type BOOL — only boolean Active items.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "NestedTest",
		"FilterExpression":          "attribute_type(#a, :t)",
		"ExpressionAttributeNames":  map[string]any{"#a": "Active"},
		"ExpressionAttributeValues": map[string]any{":t": av("BOOL")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var boolTypeResult map[string]any
	decodeDynamoJSON(t, resp, &boolTypeResult)
	assert.Equal(t, float64(2), boolTypeResult["Count"])

	// Scan with attribute_type M — only map Meta items.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "NestedTest",
		"FilterExpression":          "attribute_type(Meta, :t)",
		"ExpressionAttributeValues": map[string]any{":t": av("M")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var mapTypeResult map[string]any
	decodeDynamoJSON(t, resp, &mapTypeResult)
	assert.Equal(t, float64(1), mapTypeResult["Count"])

	// Scan with size(Count) > 0 — exercises avSize on N type.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "NestedTest",
		"FilterExpression":          "size(#c) > :z",
		"ExpressionAttributeNames":  map[string]any{"#c": "Count"},
		"ExpressionAttributeValues": map[string]any{":z": avn("0")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var sizeNResult map[string]any
	decodeDynamoJSON(t, resp, &sizeNResult)
	// "7" has size 1, "3" has size 1 — both > 0.
	assert.Equal(t, float64(2), sizeNResult["Count"])

	// Scan with parenthesized expression.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                "NestedTest",
		"FilterExpression":         "(#c > :lo) AND (#c < :hi)",
		"ExpressionAttributeNames": map[string]any{"#c": "Count"},
		"ExpressionAttributeValues": map[string]any{
			":lo": avn("5"),
			":hi": avn("10"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var parenResult map[string]any
	decodeDynamoJSON(t, resp, &parenResult)
	assert.Equal(t, float64(1), parenResult["Count"])
}

// TestDynamoDB_StreamsWithEnabled tests listStreams/describeStream/getShardIterator
// when streams are enabled during CreateTable.
func TestDynamoDB_StreamsWithEnabled(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "StreamEnabledTable",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"StreamSpecification": map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": "NEW_AND_OLD_IMAGES",
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// ListStreams should return the stream ARN.
	resp = dynamodbRequest(t, srv, "ListStreams", map[string]any{
		"TableName": "StreamEnabledTable",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listStreamsResult map[string]any
	decodeDynamoJSON(t, resp, &listStreamsResult)
	streams, ok := listStreamsResult["Streams"].([]any)
	require.True(t, ok)
	assert.Equal(t, 1, len(streams))

	// DescribeStream with the ARN.
	streamEntry := streams[0].(map[string]any)
	streamARN := streamEntry["StreamArn"].(string)

	resp = dynamodbRequest(t, srv, "DescribeStream", map[string]any{
		"StreamArn": streamARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descResult map[string]any
	decodeDynamoJSON(t, resp, &descResult)
	streamDesc := descResult["StreamDescription"].(map[string]any)
	assert.Equal(t, "ENABLED", streamDesc["StreamStatus"])

	// GetShardIterator.
	resp = dynamodbRequest(t, srv, "GetShardIterator", map[string]any{
		"StreamArn":         streamARN,
		"ShardId":           "shard-0001",
		"ShardIteratorType": "TRIM_HORIZON",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var shardResult map[string]any
	decodeDynamoJSON(t, resp, &shardResult)
	assert.NotEmpty(t, shardResult["ShardIterator"])
}

// TestDynamoDB_UpdateItemReturnValues exercises all ReturnValues options in UpdateItem
// and also tests creating a new item via UpdateItem (isNew path).
func TestDynamoDB_UpdateItemReturnValues(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "RVTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Put initial item.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "RVTest",
		"Item": map[string]any{
			"ID":    av("rv1"),
			"Score": avn("100"),
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// UpdateItem with ALL_OLD.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "RVTest",
		"Key":                       map[string]any{"ID": av("rv1")},
		"UpdateExpression":          "SET Score = :s",
		"ExpressionAttributeValues": map[string]any{":s": avn("200")},
		"ReturnValues":              "ALL_OLD",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var oldResult map[string]any
	decodeDynamoJSON(t, resp, &oldResult)
	attrs := oldResult["Attributes"].(map[string]any)
	assert.Equal(t, "100", attrs["Score"].(map[string]any)["N"])

	// UpdateItem with UPDATED_NEW.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "RVTest",
		"Key":                       map[string]any{"ID": av("rv1")},
		"UpdateExpression":          "SET Score = :s",
		"ExpressionAttributeValues": map[string]any{":s": avn("300")},
		"ReturnValues":              "UPDATED_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var updNewResult map[string]any
	decodeDynamoJSON(t, resp, &updNewResult)
	updAttrs := updNewResult["Attributes"].(map[string]any)
	assert.Equal(t, "300", updAttrs["Score"].(map[string]any)["N"])

	// UpdateItem with UPDATED_OLD.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "RVTest",
		"Key":                       map[string]any{"ID": av("rv1")},
		"UpdateExpression":          "SET Score = :s",
		"ExpressionAttributeValues": map[string]any{":s": avn("400")},
		"ReturnValues":              "UPDATED_OLD",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var updOldResult map[string]any
	decodeDynamoJSON(t, resp, &updOldResult)
	updOldAttrs := updOldResult["Attributes"].(map[string]any)
	assert.Equal(t, "300", updOldAttrs["Score"].(map[string]any)["N"])

	// UpdateItem creating a new item (isNew path).
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":                 "RVTest",
		"Key":                       map[string]any{"ID": av("newviaupdate")},
		"UpdateExpression":          "SET Score = :s",
		"ExpressionAttributeValues": map[string]any{":s": avn("50")},
		"ReturnValues":              "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var newItemResult map[string]any
	decodeDynamoJSON(t, resp, &newItemResult)
	newAttrs := newItemResult["Attributes"].(map[string]any)
	assert.Equal(t, "50", newAttrs["Score"].(map[string]any)["N"])
}

// TestDynamoDB_UpdateItemSetUnion exercises ADD with SS set union (addAV SS path).
func TestDynamoDB_UpdateItemSetUnion(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "SetUnionTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Put item with SS attribute.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "SetUnionTest",
		"Item": map[string]any{
			"ID":   av("su1"),
			"Tags": map[string]any{"SS": []any{"alpha", "beta"}},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// ADD new elements to the SS set.
	resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":        "SetUnionTest",
		"Key":              map[string]any{"ID": av("su1")},
		"UpdateExpression": "ADD Tags :newTags",
		"ExpressionAttributeValues": map[string]any{
			":newTags": map[string]any{"SS": []any{"beta", "gamma"}},
		},
		"ReturnValues": "ALL_NEW",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var setResult map[string]any
	decodeDynamoJSON(t, resp, &setResult)
	attrs := setResult["Attributes"].(map[string]any)
	tags := attrs["Tags"].(map[string]any)["SS"].([]any)
	// alpha, beta, gamma (beta deduplicated).
	assert.Equal(t, 3, len(tags))
}

// TestDynamoDB_NEOperatorAndListContains exercises the <> operator and contains() on lists.
func TestDynamoDB_NEOperatorAndListContains(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "NETest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, item := range []map[string]any{
		{
			"ID":     av("ne1"),
			"Status": av("active"),
			"Tags":   map[string]any{"L": []any{map[string]any{"S": "web"}, map[string]any{"S": "api"}}},
		},
		{
			"ID":     av("ne2"),
			"Status": av("inactive"),
			"Tags":   map[string]any{"L": []any{map[string]any{"S": "db"}}},
		},
	} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "NETest",
			"Item":      item,
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// Scan with <> (not equal) operator.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "NETest",
		"FilterExpression":          "#s <> :inactive",
		"ExpressionAttributeNames":  map[string]any{"#s": "Status"},
		"ExpressionAttributeValues": map[string]any{":inactive": av("inactive")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var neResult map[string]any
	decodeDynamoJSON(t, resp, &neResult)
	assert.Equal(t, float64(1), neResult["Count"])

	// Scan with contains() on list.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "NETest",
		"FilterExpression":          "contains(Tags, :tag)",
		"ExpressionAttributeValues": map[string]any{":tag": av("web")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listContainsResult map[string]any
	decodeDynamoJSON(t, resp, &listContainsResult)
	assert.Equal(t, float64(1), listContainsResult["Count"])
}

// TestDynamoDB_PutItemReturnValues exercises PutItem with ReturnValues=ALL_OLD.
func TestDynamoDB_PutItemReturnValues(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "PutRVTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Initial put.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "PutRVTest",
		"Item": map[string]any{
			"ID":    av("r1"),
			"Value": av("original"),
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Replace with ALL_OLD return — should return previous item.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "PutRVTest",
		"Item": map[string]any{
			"ID":    av("r1"),
			"Value": av("updated"),
		},
		"ReturnValues": "ALL_OLD",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var putRVResult map[string]any
	decodeDynamoJSON(t, resp, &putRVResult)
	attrs, ok := putRVResult["Attributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "original", attrs["Value"].(map[string]any)["S"])
}

// TestDynamoDB_AttributeTypeAllTypes tests attribute_type with SS, NS, L attribute types
// to improve avTypeName and avSize coverage.
func TestDynamoDB_AttributeTypeAllTypes(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "AllTypesTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Item with SS set.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "AllTypesTest",
		"Item": map[string]any{
			"ID":   av("t1"),
			"Tags": map[string]any{"SS": []any{"x", "y"}},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Item with NS set.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "AllTypesTest",
		"Item": map[string]any{
			"ID":     av("t2"),
			"Scores": map[string]any{"NS": []any{"1", "2", "3"}},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Item with L list.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "AllTypesTest",
		"Item": map[string]any{
			"ID":    av("t3"),
			"Items": map[string]any{"L": []any{map[string]any{"S": "a"}, map[string]any{"S": "b"}}},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Scan for SS type.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "AllTypesTest",
		"FilterExpression":          "attribute_type(Tags, :t)",
		"ExpressionAttributeValues": map[string]any{":t": av("SS")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var ssResult map[string]any
	decodeDynamoJSON(t, resp, &ssResult)
	assert.Equal(t, float64(1), ssResult["Count"])

	// Scan for NS type.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "AllTypesTest",
		"FilterExpression":          "attribute_type(Scores, :t)",
		"ExpressionAttributeValues": map[string]any{":t": av("NS")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var nsResult map[string]any
	decodeDynamoJSON(t, resp, &nsResult)
	assert.Equal(t, float64(1), nsResult["Count"])

	// Scan for L type.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "AllTypesTest",
		"FilterExpression":          "attribute_type(#items, :t)",
		"ExpressionAttributeNames":  map[string]any{"#items": "Items"},
		"ExpressionAttributeValues": map[string]any{":t": av("L")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var lResult map[string]any
	decodeDynamoJSON(t, resp, &lResult)
	assert.Equal(t, float64(1), lResult["Count"])

	// size() on SS — should return number of elements.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "AllTypesTest",
		"FilterExpression":          "size(Tags) = :n",
		"ExpressionAttributeValues": map[string]any{":n": avn("2")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var ssSizeResult map[string]any
	decodeDynamoJSON(t, resp, &ssSizeResult)
	assert.Equal(t, float64(1), ssSizeResult["Count"])

	// size() on L — should return number of elements.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "AllTypesTest",
		"FilterExpression":          "size(#items) = :n",
		"ExpressionAttributeNames":  map[string]any{"#items": "Items"},
		"ExpressionAttributeValues": map[string]any{":n": avn("2")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var lSizeResult map[string]any
	decodeDynamoJSON(t, resp, &lSizeResult)
	assert.Equal(t, float64(1), lSizeResult["Count"])
}

// TestDynamoDB_LSI tests a table with LocalSecondaryIndex and exercises findIndexKeySchema LSI path.
func TestDynamoDB_LSI(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "LSITable",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
			map[string]any{"AttributeName": "Timestamp", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
			map[string]any{"AttributeName": "Timestamp", "AttributeType": "S"},
			map[string]any{"AttributeName": "Priority", "AttributeType": "N"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"LocalSecondaryIndexes": []any{
			map[string]any{
				"IndexName": "PriorityIndex",
				"KeySchema": []any{
					map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
					map[string]any{"AttributeName": "Priority", "KeyType": "RANGE"},
				},
				"Projection": map[string]any{"ProjectionType": "ALL"},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, item := range []map[string]any{
		{"UserID": av("u1"), "Timestamp": av("2024-01"), "Priority": avn("1")},
		{"UserID": av("u1"), "Timestamp": av("2024-02"), "Priority": avn("3")},
		{"UserID": av("u1"), "Timestamp": av("2024-03"), "Priority": avn("2")},
	} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "LSITable",
			"Item":      item,
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// Query via LSI.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":              "LSITable",
		"IndexName":              "PriorityIndex",
		"KeyConditionExpression": "UserID = :uid AND Priority >= :p",
		"ExpressionAttributeValues": map[string]any{
			":uid": av("u1"),
			":p":   avn("2"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeDynamoJSON(t, resp, &result)
	assert.Equal(t, float64(2), result["Count"])
}

// TestDynamoDB_InvalidExpression exercises the evalDynamoCondition error path.
func TestDynamoDB_InvalidExpression(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "ErrExprTable",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "ErrExprTable",
		"Item":      map[string]any{"ID": av("x")},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Scan with an invalid expression character to trigger tokenizeDynamo error.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":        "ErrExprTable",
		"FilterExpression": "ID = @invalid",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestDynamoDB_ScanPagination exercises Scan with ExclusiveStartKey (follow-up page).
func TestDynamoDB_ScanPagination(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "ScanPageTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, id := range []string{"s1", "s2", "s3", "s4", "s5"} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "ScanPageTest",
			"Item":      map[string]any{"ID": av(id)},
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// First page.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName": "ScanPageTest",
		"Limit":     3,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var page1 map[string]any
	decodeDynamoJSON(t, resp, &page1)
	assert.Equal(t, float64(3), page1["Count"])
	lastKey, hasLast := page1["LastEvaluatedKey"]
	require.True(t, hasLast)

	// Second page using ExclusiveStartKey.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":         "ScanPageTest",
		"ExclusiveStartKey": lastKey,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var page2 map[string]any
	decodeDynamoJSON(t, resp, &page2)
	assert.Equal(t, float64(2), page2["Count"])
}

// TestDynamoDB_ShutdownAndName tests Name() and Shutdown() methods.
func TestDynamoDB_ShutdownAndName(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	// Confirm server is working.
	resp := dynamodbRequest(t, srv, "ListTables", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify Name() via the server's ready endpoint.
	r, err := http.NewRequest(http.MethodGet, "/ready", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	assert.Equal(t, http.StatusOK, rr.Code)

	var readyResp map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&readyResp))
	plugins := readyResp["plugins"].([]any)
	found := false
	for _, p := range plugins {
		if p.(string) == "dynamodb" {
			found = true
			break
		}
	}
	assert.True(t, found, "dynamodb plugin should be registered")

	// Call Shutdown directly on the plugin.
	plugin := &substrate.DynamoDBPlugin{}
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(0, false)
	require.NoError(t, plugin.Initialize(t.Context(), substrate.PluginConfig{State: state, Logger: logger}))
	assert.NoError(t, plugin.Shutdown(t.Context()))
}

// TestDynamoDB_QueryPagination exercises Query with Limit and ExclusiveStartKey.
func TestDynamoDB_QueryPagination(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "QueryPageTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "PK", "KeyType": "HASH"},
			map[string]any{"AttributeName": "SK", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "PK", "AttributeType": "S"},
			map[string]any{"AttributeName": "SK", "AttributeType": "N"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Insert 5 items.
	for _, sk := range []string{"1", "2", "3", "4", "5"} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "QueryPageTest",
			"Item": map[string]any{
				"PK": av("user1"),
				"SK": avn(sk),
			},
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// Query first page.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":                 "QueryPageTest",
		"KeyConditionExpression":    "PK = :pk",
		"ExpressionAttributeValues": map[string]any{":pk": av("user1")},
		"Limit":                     3,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var page1 map[string]any
	decodeDynamoJSON(t, resp, &page1)
	assert.Equal(t, float64(3), page1["Count"])
	lastKey, hasLast := page1["LastEvaluatedKey"]
	assert.True(t, hasLast)

	// Query second page.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":                 "QueryPageTest",
		"KeyConditionExpression":    "PK = :pk",
		"ExpressionAttributeValues": map[string]any{":pk": av("user1")},
		"ExclusiveStartKey":         lastKey,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var page2 map[string]any
	decodeDynamoJSON(t, resp, &page2)
	assert.Equal(t, float64(2), page2["Count"])
}

// TestDynamoDB_ListStreamsGlobal exercises ListStreams without TableName (global scan).
func TestDynamoDB_ListStreamsGlobal(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create one table with streams, one without.
	for _, tc := range []struct {
		name   string
		stream bool
	}{
		{"StreamTable1", true},
		{"NoStreamTable", false},
	} {
		body := map[string]any{
			"TableName": tc.name,
			"KeySchema": []any{
				map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
			},
			"AttributeDefinitions": []any{
				map[string]any{"AttributeName": "ID", "AttributeType": "S"},
			},
			"BillingMode": "PAY_PER_REQUEST",
		}
		if tc.stream {
			body["StreamSpecification"] = map[string]any{
				"StreamEnabled":  true,
				"StreamViewType": "KEYS_ONLY",
			}
		}
		r := dynamodbRequest(t, srv, "CreateTable", body)
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// ListStreams with no TableName — should find the one stream.
	resp := dynamodbRequest(t, srv, "ListStreams", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeDynamoJSON(t, resp, &result)
	streams := result["Streams"].([]any)
	assert.Equal(t, 1, len(streams))

	// ListStreams for the non-stream table — should return empty.
	resp = dynamodbRequest(t, srv, "ListStreams", map[string]any{
		"TableName": "NoStreamTable",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var noStreamResult map[string]any
	decodeDynamoJSON(t, resp, &noStreamResult)
	noStreams := noStreamResult["Streams"].([]any)
	assert.Equal(t, 0, len(noStreams))
}

// TestDynamoDB_GSIScan tests Scan with IndexName, which exercises the GSI scan path.
func TestDynamoDB_GSIScan(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table with a GSI on Status.
	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "GSIScanTable",
		"KeySchema": []any{
			map[string]any{"AttributeName": "UserID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "UserID", "AttributeType": "S"},
			map[string]any{"AttributeName": "Status", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
		"GlobalSecondaryIndexes": []any{
			map[string]any{
				"IndexName": "StatusIndex",
				"KeySchema": []any{
					map[string]any{"AttributeName": "Status", "KeyType": "HASH"},
				},
				"Projection": map[string]any{"ProjectionType": "ALL"},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, item := range []map[string]any{
		{"UserID": av("u1"), "Status": av("active")},
		{"UserID": av("u2"), "Status": av("inactive")},
		{"UserID": av("u3"), "Status": av("active")},
	} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "GSIScanTable",
			"Item":      item,
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// Scan via GSI IndexName.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":                 "GSIScanTable",
		"IndexName":                 "StatusIndex",
		"FilterExpression":          "#s = :active",
		"ExpressionAttributeNames":  map[string]any{"#s": "Status"},
		"ExpressionAttributeValues": map[string]any{":active": av("active")},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var gsiScanResult map[string]any
	decodeDynamoJSON(t, resp, &gsiScanResult)
	assert.Equal(t, float64(2), gsiScanResult["Count"])
}

// TestDynamoDB_NestedPathFilter exercises dotted path access in evalPath
// (e.g., attribute_exists(Meta.Region)).
func TestDynamoDB_NestedPathFilter(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "NestedPath",
		"KeySchema": []any{
			map[string]any{"AttributeName": "ID", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "ID", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Put item with nested map attribute.
	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "NestedPath",
		"Item": map[string]any{
			"ID": av("np1"),
			"Meta": map[string]any{
				"M": map[string]any{
					"Region": map[string]any{"S": "us-east-1"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "NestedPath",
		"Item": map[string]any{
			"ID": av("np2"),
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Scan with dotted path in attribute_exists.
	resp = dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName":        "NestedPath",
		"FilterExpression": "attribute_exists(Meta.Region)",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var nestedResult map[string]any
	decodeDynamoJSON(t, resp, &nestedResult)
	assert.Equal(t, float64(1), nestedResult["Count"])
}

// TestDynamoDB_QueryBeginsWith exercises begins_with SK condition.
func TestDynamoDB_QueryBeginsWith(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	resp := dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "BeginsWithTest",
		"KeySchema": []any{
			map[string]any{"AttributeName": "PK", "KeyType": "HASH"},
			map[string]any{"AttributeName": "SK", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "PK", "AttributeType": "S"},
			map[string]any{"AttributeName": "SK", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, sk := range []string{"2024-01-01", "2024-02-01", "2023-12-01"} {
		r := dynamodbRequest(t, srv, "PutItem", map[string]any{
			"TableName": "BeginsWithTest",
			"Item": map[string]any{
				"PK": av("user1"),
				"SK": av(sk),
			},
		})
		require.Equal(t, http.StatusOK, r.StatusCode)
	}

	// Query with begins_with SK condition.
	resp = dynamodbRequest(t, srv, "Query", map[string]any{
		"TableName":              "BeginsWithTest",
		"KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
		"ExpressionAttributeValues": map[string]any{
			":pk":     av("user1"),
			":prefix": av("2024"),
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeDynamoJSON(t, resp, &result)
	assert.Equal(t, float64(2), result["Count"])
}

// TestDynamoDBPlugin_EmptyStringAttribute verifies that nested map attributes
// with empty string values round-trip correctly through PutItem / GetItem (#252).
func TestDynamoDBPlugin_EmptyStringAttribute(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "empstr-table",
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "pk", "AttributeType": "S"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})

	// PutItem with nested map containing empty string values.
	putResp := dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "empstr-table",
		"Item": map[string]any{
			"pk": map[string]any{"S": "row1"},
			"schema": map[string]any{
				"M": map[string]any{
					"columns": map[string]any{
						"L": []any{
							map[string]any{
								"M": map[string]any{
									"name":    map[string]any{"S": "gene"},
									"type":    map[string]any{"S": "string"},
									"comment": map[string]any{"S": ""},
								},
							},
						},
					},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, putResp.StatusCode)

	// GetItem and verify the empty string is preserved in the response.
	getResp := dynamodbRequest(t, srv, "GetItem", map[string]any{
		"TableName": "empstr-table",
		"Key": map[string]any{
			"pk": map[string]any{"S": "row1"},
		},
	})
	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	var raw map[string]any
	decodeDynamoJSON(t, getResp, &raw)
	itemRaw, _ := raw["Item"].(map[string]any)
	schemaRaw, _ := itemRaw["schema"].(map[string]any)
	schemaMRaw, _ := schemaRaw["M"].(map[string]any)
	columnsRaw, _ := schemaMRaw["columns"].(map[string]any)
	colsL, _ := columnsRaw["L"].([]any)
	if len(colsL) == 0 {
		t.Fatal("columns L list is empty — empty string attribute was likely lost")
	}
	col0, _ := colsL[0].(map[string]any)
	col0M, _ := col0["M"].(map[string]any)
	commentAV, _ := col0M["comment"].(map[string]any)
	// Before fix: commentAV would be {} because {"S":""} was omitted by omitempty.
	// After fix: commentAV should be {"S": ""}.
	if _, ok := commentAV["S"]; !ok {
		t.Errorf("expected 'S' key in comment attribute value, got: %v", commentAV)
	}
}

// TestDynamoDBPlugin_EmptyStringAttribute_Scan verifies that flat top-level
// empty string attributes round-trip correctly through PutItem / Scan (#254).
// Before fix: {"S":""} was serialised as {} by json omitempty, so a Scan
// would return {} for those attributes and boto3's TypeDeserializer would
// raise "Value must be a nonempty dictionary whose key is a valid dynamodb type".
func TestDynamoDBPlugin_EmptyStringAttribute_Scan(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	// Create table.
	dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "empstr-scan-table",
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "slug", "AttributeType": "S"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "slug", "KeyType": "HASH"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})

	// PutItem with flat top-level empty string attributes (mirrors issue #254 repro).
	putResp := dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "empstr-scan-table",
		"Item": map[string]any{
			"slug":          map[string]any{"S": "item1"},
			"registryUrl":   map[string]any{"S": ""},
			"documentation": map[string]any{"S": ""},
		},
	})
	assert.Equal(t, http.StatusOK, putResp.StatusCode)

	// Scan the table.
	scanResp := dynamodbRequest(t, srv, "Scan", map[string]any{
		"TableName": "empstr-scan-table",
	})
	assert.Equal(t, http.StatusOK, scanResp.StatusCode)

	var raw map[string]any
	decodeDynamoJSON(t, scanResp, &raw)
	itemsRaw, _ := raw["Items"].([]any)
	if len(itemsRaw) == 0 {
		t.Fatal("Scan returned no items")
	}
	item0, _ := itemsRaw[0].(map[string]any)

	// Verify registryUrl has {"S": ""} — not {} — so boto3 TypeDeserializer succeeds.
	registryAV, _ := item0["registryUrl"].(map[string]any)
	if _, ok := registryAV["S"]; !ok {
		t.Errorf("registryUrl: expected {\"S\":\"\"} but got %v — empty string was lost in scan response", registryAV)
	}
	if v, _ := registryAV["S"].(string); v != "" {
		t.Errorf("registryUrl.S: expected empty string, got %q", v)
	}

	docAV, _ := item0["documentation"].(map[string]any)
	if _, ok := docAV["S"]; !ok {
		t.Errorf("documentation: expected {\"S\":\"\"} but got %v — empty string was lost in scan response", docAV)
	}
}
