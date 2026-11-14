package emulator_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1448/#1451 (conditions against the stored item), #1450 (if_not_exists,
// list_append, document paths), #1454 (typed comparison and binary keys) and #1449
// (per-item serialization).

// newDynamoDBStateServer is newDynamoDBTestServer with the state manager exposed, so a
// test can assert the exact state keys items are stored under.
func newDynamoDBStateServer(t *testing.T) (*emulator.Server, emulator.StateManager) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	p := &emulator.DynamoDBPlugin{}
	require.NoError(t, p.Initialize(context.TODO(), emulator.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	return emulator.NewServer(*cfg, registry, store, state, tc, logger), state
}

// ddbCall sends one DynamoDB request and returns its status and decoded body.
func ddbCall(t *testing.T, srv *emulator.Server, op string, body map[string]any) (int, map[string]any) {
	t.Helper()
	resp := dynamodbRequest(t, srv, op, body)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := map[string]any{}
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out), "body: %s", raw)
	}
	return resp.StatusCode, out
}

// ddbErrCode returns the error code of a DynamoDB error body.
func ddbErrCode(body map[string]any) string {
	if c, ok := body["Code"].(string); ok {
		return c
	}
	if c, ok := body["__type"].(string); ok {
		return c
	}
	return ""
}

// ddbMustOK fails the test unless the call answered 200.
func ddbMustOK(t *testing.T, srv *emulator.Server, op string, body map[string]any) map[string]any {
	t.Helper()
	status, out := ddbCall(t, srv, op, body)
	require.Equal(t, http.StatusOK, status, "%s: %v", op, out)
	return out
}

// ddbCreateTable creates a table keyed by the given (name, type, keyType) triples.
func ddbCreateTable(t *testing.T, srv *emulator.Server, name string, keys ...[3]string) {
	t.Helper()
	var schema, defs []map[string]any
	for _, k := range keys {
		schema = append(schema, map[string]any{"AttributeName": k[0], "KeyType": k[2]})
		defs = append(defs, map[string]any{"AttributeName": k[0], "AttributeType": k[1]})
	}
	ddbMustOK(t, srv, "CreateTable", map[string]any{
		"TableName": name, "KeySchema": schema, "AttributeDefinitions": defs,
		"BillingMode": "PAY_PER_REQUEST",
		"StreamSpecification": map[string]any{
			"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES",
		},
	})
}

// ddbGet returns the item stored under key, or nil.
func ddbGet(t *testing.T, srv *emulator.Server, table string, key map[string]any) map[string]any {
	t.Helper()
	out := ddbMustOK(t, srv, "GetItem", map[string]any{"TableName": table, "Key": key, "ConsistentRead": true})
	item, _ := out["Item"].(map[string]any)
	return item
}

// ddbBin returns a binary AttributeValue holding b.
func ddbBin(b ...byte) map[string]any {
	return map[string]any{"B": base64.StdEncoding.EncodeToString(b)}
}

func TestDynamoDB_UpdateItem_ConditionSeesNoItemWhenAbsent(t *testing.T) {
	cases := []struct {
		name      string
		cond      string
		names     map[string]any
		wantOK    bool
		wantStore bool
	}{
		// #1448: "update only if it exists" must refuse a missing item.
		{name: "attribute_exists(pk) on missing item fails", cond: "attribute_exists(watch_id)", wantOK: false},
		{name: "attribute_exists(non-key) on missing item fails", cond: "attribute_exists(#st)", names: map[string]any{"#st": "status"}, wantOK: false},
		// #1451: "create only if absent" must accept a missing item.
		{name: "attribute_not_exists(pk) on missing item creates", cond: "attribute_not_exists(watch_id)", wantOK: true, wantStore: true},
		{name: "attribute_not_exists(pk) OR owner = :o", cond: "attribute_not_exists(watch_id) OR group_owner = :o", wantOK: true, wantStore: true},
		{name: "attribute_not_exists(pk) OR (owner AND state)", cond: "attribute_not_exists(watch_id) OR (group_owner = :o AND group_state = :h)", wantOK: true, wantStore: true},
		{name: "pk = :pk on missing item fails", cond: "watch_id = :pk", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newDynamoDBTestServer(t)
			ddbCreateTable(t, srv, "watches", [3]string{"watch_id", "S", "HASH"})
			req := map[string]any{
				"TableName":           "watches",
				"Key":                 map[string]any{"watch_id": av("w-gone")},
				"ConditionExpression": tc.cond,
				"UpdateExpression":    "SET last_poll_error = :err",
				"ExpressionAttributeValues": map[string]any{
					":err": av("boom"), ":o": av("me"), ":h": av("held"), ":pk": av("w-gone"),
				},
			}
			if tc.names != nil {
				req["ExpressionAttributeNames"] = tc.names
			}
			status, body := ddbCall(t, srv, "UpdateItem", req)
			if tc.wantOK {
				require.Equal(t, http.StatusOK, status, "%v", body)
			} else {
				require.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "ConditionalCheckFailedException", ddbErrCode(body))
			}
			item := ddbGet(t, srv, "watches", map[string]any{"watch_id": av("w-gone")})
			if tc.wantStore {
				require.NotNil(t, item)
				assert.Equal(t, av("boom"), item["last_poll_error"])
			} else {
				assert.Nil(t, item, "a refused update must not create a stub item")
			}
		})
	}
}

func TestDynamoDB_UpdateItem_ExistingItemConditions(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "watches", [3]string{"watch_id", "S", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "watches", "Item": map[string]any{"watch_id": av("w1"), "n": avn("1")}})

	// attribute_exists(pk) on an existing item passes.
	ddbMustOK(t, srv, "UpdateItem", map[string]any{
		"TableName": "watches", "Key": map[string]any{"watch_id": av("w1")},
		"ConditionExpression": "attribute_exists(watch_id)", "UpdateExpression": "SET n = :two",
		"ExpressionAttributeValues": map[string]any{":two": avn("2")},
	})
	// attribute_not_exists(pk) on an existing item fails.
	status, body := ddbCall(t, srv, "UpdateItem", map[string]any{
		"TableName": "watches", "Key": map[string]any{"watch_id": av("w1")},
		"ConditionExpression": "attribute_not_exists(watch_id)", "UpdateExpression": "SET n = :two",
		"ExpressionAttributeValues": map[string]any{":two": avn("3")},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ConditionalCheckFailedException", ddbErrCode(body))
	assert.Equal(t, avn("2"), ddbGet(t, srv, "watches", map[string]any{"watch_id": av("w1")})["n"])
}

func TestDynamoDB_UpdateItem_ReturnValuesForNewAndExistingItems(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "rv", [3]string{"pk", "S", "HASH"})

	// A creating update has no old attributes.
	for _, rv := range []string{"ALL_OLD", "UPDATED_OLD"} {
		out := ddbMustOK(t, srv, "UpdateItem", map[string]any{
			"TableName": "rv", "Key": map[string]any{"pk": av("new-" + rv)},
			"UpdateExpression": "SET a = :v", "ExpressionAttributeValues": map[string]any{":v": av("x")},
			"ReturnValues": rv,
		})
		assert.NotContains(t, out, "Attributes", rv)
	}

	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "rv", "Item": map[string]any{
		"pk": av("k"), "a": av("old-a"), "b": av("old-b"), "c": av("keep"),
	}})
	cases := []struct {
		rv   string
		want map[string]any
	}{
		{"UPDATED_OLD", map[string]any{"a": av("old-a"), "b": av("old-b")}},
		{"UPDATED_NEW", map[string]any{"a": av("new-a"), "d": av("new-d")}},
		{"ALL_OLD", map[string]any{"pk": av("k"), "a": av("old-a"), "b": av("old-b"), "c": av("keep")}},
		{"ALL_NEW", map[string]any{"pk": av("k"), "a": av("new-a"), "c": av("keep"), "d": av("new-d")}},
	}
	for _, tc := range cases {
		t.Run(tc.rv, func(t *testing.T) {
			// Reset the item, then SET a, SET d (new), REMOVE b.
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "rv", "Item": map[string]any{
				"pk": av("k"), "a": av("old-a"), "b": av("old-b"), "c": av("keep"),
			}})
			out := ddbMustOK(t, srv, "UpdateItem", map[string]any{
				"TableName": "rv", "Key": map[string]any{"pk": av("k")},
				"UpdateExpression":          "SET a = :a, d = :d REMOVE b",
				"ExpressionAttributeValues": map[string]any{":a": av("new-a"), ":d": av("new-d")},
				"ReturnValues":              tc.rv,
			})
			assert.Equal(t, tc.want, out["Attributes"])
		})
	}
}

func TestDynamoDB_UpdateItem_CreateEmitsInsertWithoutOldImage(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "st", [3]string{"pk", "S", "HASH"})
	ddbMustOK(t, srv, "UpdateItem", map[string]any{
		"TableName": "st", "Key": map[string]any{"pk": av("a")},
		"UpdateExpression": "SET v = :v", "ExpressionAttributeValues": map[string]any{":v": avn("1")},
	})
	ddbMustOK(t, srv, "TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"Update": map[string]any{
			"TableName": "st", "Key": map[string]any{"pk": av("b")},
			"UpdateExpression": "SET v = :v", "ConditionExpression": "attribute_not_exists(pk)",
			"ExpressionAttributeValues": map[string]any{":v": avn("1")},
		}},
	}})

	records := ddbStreamRecords(t, srv, "st")
	require.Len(t, records, 2)
	for _, r := range records {
		assert.Equal(t, "INSERT", r["eventName"])
		ddb, _ := r["dynamodb"].(map[string]any)
		assert.NotContains(t, ddb, "OldImage", "an INSERT has no old image")
	}
}

// ddbStreamRecords reads every record of a table's stream.
func ddbStreamRecords(t *testing.T, srv *emulator.Server, table string) []map[string]any {
	t.Helper()
	desc := ddbMustOK(t, srv, "DescribeTable", map[string]any{"TableName": table})
	streamARN, _ := desc["Table"].(map[string]any)["LatestStreamArn"].(string)
	require.NotEmpty(t, streamARN)
	sd := ddbMustOK(t, srv, "DescribeStream", map[string]any{"StreamArn": streamARN})
	shards, _ := sd["StreamDescription"].(map[string]any)["Shards"].([]any)
	require.NotEmpty(t, shards)
	shardID, _ := shards[0].(map[string]any)["ShardId"].(string)
	it := ddbMustOK(t, srv, "GetShardIterator", map[string]any{
		"StreamArn": streamARN, "ShardId": shardID, "ShardIteratorType": "TRIM_HORIZON",
	})
	gr := ddbMustOK(t, srv, "GetRecords", map[string]any{"ShardIterator": it["ShardIterator"], "Limit": 1000})
	raw, _ := gr["Records"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		out = append(out, r.(map[string]any))
	}
	return out
}

func TestDynamoDB_TransactWriteItems_ConditionSeesNoItemWhenAbsent(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "tx", [3]string{"pk", "S", "HASH"})

	status, body := ddbCall(t, srv, "TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"Update": map[string]any{
			"TableName": "tx", "Key": map[string]any{"pk": av("missing")},
			"UpdateExpression": "SET v = :v", "ConditionExpression": "attribute_exists(pk)",
			"ExpressionAttributeValues": map[string]any{":v": avn("1")},
		}},
	}})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, fmt.Sprint(body), "ConditionalCheckFailed")
	assert.Nil(t, ddbGet(t, srv, "tx", map[string]any{"pk": av("missing")}))

	ddbMustOK(t, srv, "TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"Update": map[string]any{
			"TableName": "tx", "Key": map[string]any{"pk": av("fresh")},
			"UpdateExpression": "SET v = if_not_exists(v, :zero) + :one", "ConditionExpression": "attribute_not_exists(pk)",
			"ExpressionAttributeValues": map[string]any{":zero": avn("0"), ":one": avn("1")},
		}},
	}})
	assert.Equal(t, avn("1"), ddbGet(t, srv, "tx", map[string]any{"pk": av("fresh")})["v"])

	// An update expression that cannot be applied fails the whole transaction with
	// nothing written.
	status, body = ddbCall(t, srv, "TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"Put": map[string]any{"TableName": "tx", "Item": map[string]any{"pk": av("put-first")}}},
		map[string]any{"Update": map[string]any{
			"TableName": "tx", "Key": map[string]any{"pk": av("fresh")},
			"UpdateExpression": "SET v = nope + :one", "ExpressionAttributeValues": map[string]any{":one": avn("1")},
		}},
	}})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationException", ddbErrCode(body))
	assert.Nil(t, ddbGet(t, srv, "tx", map[string]any{"pk": av("put-first")}))
}

func TestDynamoDB_UpdateExpression_Functions(t *testing.T) {
	emptyList := map[string]any{"L": []any{}}
	cases := []struct {
		name    string
		initial map[string]any // nil: no item
		expr    string
		names   map[string]any
		values  map[string]any
		want    map[string]any // expected attributes (besides pk), nil-valued entries must be absent
		absent  []string
		wantErr string // substring of the ValidationException message
	}{
		{
			name:   "if_not_exists absent writes operand (#1450 repro)",
			expr:   "SET a = if_not_exists(a, :v), b = :v",
			values: map[string]any{":v": av("val")},
			want:   map[string]any{"a": av("val"), "b": av("val")},
		},
		{
			name:    "if_not_exists present keeps value",
			initial: map[string]any{"created_at": avn("100")},
			expr:    "SET created_at = if_not_exists(created_at, :now)",
			values:  map[string]any{":now": avn("200")},
			want:    map[string]any{"created_at": avn("100")},
		},
		{
			name:   "if_not_exists in arithmetic on absent attribute",
			expr:   "SET n = if_not_exists(n, :zero) + :one",
			values: map[string]any{":zero": avn("0"), ":one": avn("1")},
			want:   map[string]any{"n": avn("1")},
		},
		{
			name:    "if_not_exists in arithmetic on present attribute",
			initial: map[string]any{"n": avn("41")},
			expr:    "SET n = if_not_exists(n, :zero) + :one",
			values:  map[string]any{":zero": avn("0"), ":one": avn("1")},
			want:    map[string]any{"n": avn("42")},
		},
		{
			name:    "subtraction",
			initial: map[string]any{"n": avn("10.5")},
			expr:    "SET n = n - :d",
			values:  map[string]any{":d": avn("0.25")},
			want:    map[string]any{"n": avn("10.25")},
		},
		{
			name:    "arithmetic beyond float64 precision",
			initial: map[string]any{"n": avn("12345678901234567890123456789012345678")},
			expr:    "SET n = n + :one",
			values:  map[string]any{":one": avn("1")},
			want:    map[string]any{"n": avn("12345678901234567890123456789012345679")},
		},
		{
			name:    "list_append appends",
			initial: map[string]any{"l": map[string]any{"L": []any{av("a")}}},
			expr:    "SET l = list_append(l, :v)",
			values:  map[string]any{":v": map[string]any{"L": []any{av("b"), av("c")}}},
			want:    map[string]any{"l": map[string]any{"L": []any{av("a"), av("b"), av("c")}}},
		},
		{
			name:    "list_append prepends with swapped operands",
			initial: map[string]any{"l": map[string]any{"L": []any{av("a")}}},
			expr:    "SET #l = list_append(:v, #l)",
			names:   map[string]any{"#l": "l"},
			values:  map[string]any{":v": map[string]any{"L": []any{av("z")}}},
			want:    map[string]any{"l": map[string]any{"L": []any{av("z"), av("a")}}},
		},
		{
			name:   "list_append over if_not_exists on absent list",
			expr:   "SET l = list_append(if_not_exists(l, :empty), :v)",
			values: map[string]any{":empty": emptyList, ":v": map[string]any{"L": []any{avn("1")}}},
			want:   map[string]any{"l": map[string]any{"L": []any{avn("1")}}},
		},
		{
			name:    "document path into map",
			initial: map[string]any{"m": map[string]any{"M": map[string]any{}}},
			expr:    "SET m.a = :v, m.#b = :w",
			names:   map[string]any{"#b": "b"},
			values:  map[string]any{":v": av("x"), ":w": avn("2")},
			want:    map[string]any{"m": map[string]any{"M": map[string]any{"a": av("x"), "b": avn("2")}}},
		},
		{
			name:    "document path list index replace and append",
			initial: map[string]any{"l": map[string]any{"L": []any{av("a"), av("b")}}},
			expr:    "SET l[0] = :x, l[9] = :y",
			values:  map[string]any{":x": av("A"), ":y": av("C")},
			want:    map[string]any{"l": map[string]any{"L": []any{av("A"), av("b"), av("C")}}},
		},
		{
			name:    "REMOVE list elements indexes the original list",
			initial: map[string]any{"l": map[string]any{"L": []any{av("a"), av("b"), av("c"), av("d")}}},
			expr:    "REMOVE l[1], l[2]",
			want:    map[string]any{"l": map[string]any{"L": []any{av("a"), av("d")}}},
		},
		{
			name:    "if_not_exists on nested path",
			initial: map[string]any{"m": map[string]any{"M": map[string]any{"x": av("keep")}}},
			expr:    "SET m.x = if_not_exists(m.x, :v), m.y = if_not_exists(m.y, :v)",
			values:  map[string]any{":v": av("new")},
			want:    map[string]any{"m": map[string]any{"M": map[string]any{"x": av("keep"), "y": av("new")}}},
		},
		{
			// Review of #1479: a comparator that ordered only index REMOVEs among
			// themselves was not transitive across the non-index REMOVE between them.
			name:    "REMOVE list indexes around a non-index REMOVE",
			initial: map[string]any{"a": map[string]any{"L": []any{avn("0"), avn("1"), avn("2"), avn("3")}}, "b": av("x")},
			expr:    "REMOVE a[0], b, a[2]",
			want:    map[string]any{"a": map[string]any{"L": []any{avn("1"), avn("3")}}},
			absent:  []string{"b"},
		},
		{
			name:    "REMOVE indexes in two lists and a repeated index",
			initial: map[string]any{"a": map[string]any{"L": []any{avn("0"), avn("1"), avn("2")}}, "c": map[string]any{"L": []any{av("x"), av("y")}}},
			expr:    "REMOVE c[0], a[2], a[0], a[2]",
			want:    map[string]any{"a": map[string]any{"L": []any{avn("1")}}, "c": map[string]any{"L": []any{av("y")}}},
		},
		{
			name:    "SET a list element and REMOVE another by pre-update index",
			initial: map[string]any{"a": map[string]any{"L": []any{avn("0"), avn("1"), avn("2"), avn("3")}}},
			expr:    "SET a[0] = :x REMOVE a[1]",
			values:  map[string]any{":x": av("x")},
			want:    map[string]any{"a": map[string]any{"L": []any{av("x"), avn("2"), avn("3")}}},
		},
		{
			name:    "arithmetic past the Number range is a ValidationException",
			initial: map[string]any{"n": avn("9.9999999999999999999999999999999999999E+125")},
			expr:    "SET n = n + n",
			wantErr: "Number overflow",
		},
		{
			name:    "an invalid Number expression value is a ValidationException",
			expr:    "SET n = :v",
			values:  map[string]any{":v": avn("0x10")},
			wantErr: "A value provided cannot be converted into a number",
		},
		{
			name:    "actions read the pre-update item",
			initial: map[string]any{"a": avn("1"), "b": avn("2"), "c": avn("3")},
			expr:    "REMOVE a SET b = a, c = b",
			want:    map[string]any{"b": avn("1"), "c": avn("2")},
			absent:  []string{"a"},
		},
		{
			name:    "ADD to number and number set",
			initial: map[string]any{"n": avn("1"), "ns": map[string]any{"NS": []any{"1", "2"}}},
			expr:    "ADD n :d, ns :s",
			values:  map[string]any{":d": avn("-3"), ":s": map[string]any{"NS": []any{"2.0", "3"}}},
			want:    map[string]any{"n": avn("-2"), "ns": map[string]any{"NS": []any{"1", "2", "3"}}},
		},
		{
			name:    "DELETE the last members removes the set",
			initial: map[string]any{"ss": map[string]any{"SS": []any{"a"}}},
			expr:    "DELETE ss :s",
			values:  map[string]any{":s": map[string]any{"SS": []any{"a"}}},
			absent:  []string{"ss"},
		},
		{
			name:    "list_append on a non-list is a ValidationException",
			initial: map[string]any{"l": av("not-a-list")},
			expr:    "SET l = list_append(l, :v)",
			values:  map[string]any{":v": map[string]any{"L": []any{av("b")}}},
			wantErr: "Incorrect operand type for operator or function; operator or function: list_append, operand type: S",
		},
		{
			name:    "arithmetic on a string is a ValidationException",
			initial: map[string]any{"n": av("5")},
			expr:    "SET n = n + :one",
			values:  map[string]any{":one": avn("1")},
			wantErr: "An operand in the update expression has an incorrect data type",
		},
		{
			name:    "arithmetic on an absent attribute is a ValidationException",
			expr:    "SET n = n + :one",
			values:  map[string]any{":one": avn("1")},
			wantErr: "The provided expression refers to an attribute that does not exist in the item",
		},
		{
			name:    "nested path under an absent parent is a ValidationException",
			expr:    "SET m.a = :v",
			values:  map[string]any{":v": av("x")},
			wantErr: "The document path provided in the update expression is invalid for update",
		},
		{
			name:    "ADD with a string is a ValidationException",
			expr:    "ADD s :v",
			values:  map[string]any{":v": av("x")},
			wantErr: "operator: ADD, operand type: STRING",
		},
		{
			name:    "undefined expression attribute value",
			expr:    "SET a = :missing",
			values:  map[string]any{":v": av("x")},
			wantErr: "An expression attribute value used in expression is not defined; attribute value: :missing",
		},
		{
			name:    "a clause may appear only once",
			expr:    "SET a = :v SET b = :v",
			values:  map[string]any{":v": av("x")},
			wantErr: `The "SET" section can only be used once`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newDynamoDBTestServer(t)
			ddbCreateTable(t, srv, "fx", [3]string{"pk", "S", "HASH"})
			key := map[string]any{"pk": av("k")}
			if tc.initial != nil {
				item := map[string]any{"pk": av("k")}
				for k, v := range tc.initial {
					item[k] = v
				}
				ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "fx", "Item": item})
			}
			before := ddbGet(t, srv, "fx", key)
			req := map[string]any{"TableName": "fx", "Key": key, "UpdateExpression": tc.expr}
			if tc.names != nil {
				req["ExpressionAttributeNames"] = tc.names
			}
			if tc.values != nil {
				req["ExpressionAttributeValues"] = tc.values
			}
			status, body := ddbCall(t, srv, "UpdateItem", req)
			if tc.wantErr != "" {
				require.Equal(t, http.StatusBadRequest, status, "%v", body)
				assert.Equal(t, "ValidationException", ddbErrCode(body))
				assert.Contains(t, fmt.Sprint(body), tc.wantErr)
				assert.Equal(t, before, ddbGet(t, srv, "fx", key), "a failed update writes nothing")
				return
			}
			require.Equal(t, http.StatusOK, status, "%v", body)
			got := ddbGet(t, srv, "fx", key)
			require.NotNil(t, got)
			for k, v := range tc.want {
				assert.Equal(t, v, got[k], k)
			}
			for _, k := range tc.absent {
				assert.NotContains(t, got, k)
			}
		})
	}
}

func TestDynamoDB_EmptyMapAndListSurviveStorage(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "empty", [3]string{"pk", "S", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "empty", "Item": map[string]any{
		"pk": av("k"), "m": map[string]any{"M": map[string]any{}}, "l": map[string]any{"L": []any{}},
	}})
	got := ddbGet(t, srv, "empty", map[string]any{"pk": av("k")})
	assert.Equal(t, map[string]any{"M": map[string]any{}}, got["m"])
	assert.Equal(t, map[string]any{"L": []any{}}, got["l"])
}

func TestDynamoDB_ConditionTypedComparison(t *testing.T) {
	item := map[string]any{
		"pk":   av("1"),
		"b":    ddbBin(0x01),
		"hi":   ddbBin(0x80),
		"s":    av("1"),
		"n":    avn("1"),
		"big":  avn("12345678901234567890123456789012345678"),
		"t":    map[string]any{"BOOL": true},
		"nul":  map[string]any{"NULL": true},
		"ss":   map[string]any{"SS": []any{"a", "b"}},
		"ns":   map[string]any{"NS": []any{"1", "2"}},
		"bs":   map[string]any{"BS": []any{base64.StdEncoding.EncodeToString([]byte{1}), base64.StdEncoding.EncodeToString([]byte{2})}},
		"m":    map[string]any{"M": map[string]any{"x": avn("1"), "y": av("z")}},
		"l":    map[string]any{"L": []any{av("a"), avn("1")}},
		"blob": ddbBin(0xde, 0xad, 0xbe, 0xef),
	}
	values := map[string]any{
		":b1": ddbBin(0x01), ":b2": ddbBin(0x02), ":b7f": ddbBin(0x7f), ":bff": ddbBin(0xff),
		":s1": av("1"), ":n1": avn("1"), ":n1x": avn("1.00"),
		":bigp1": avn("12345678901234567890123456789012345679"),
		":true":  map[string]any{"BOOL": true}, ":false": map[string]any{"BOOL": false},
		":null": map[string]any{"NULL": true},
		":ss":   map[string]any{"SS": []any{"b", "a"}}, ":ss2": map[string]any{"SS": []any{"a"}},
		":ns": map[string]any{"NS": []any{"2.0", "1"}},
		":bs": map[string]any{"BS": []any{base64.StdEncoding.EncodeToString([]byte{2}), base64.StdEncoding.EncodeToString([]byte{1})}},
		":m":  map[string]any{"M": map[string]any{"y": av("z"), "x": avn("1.0")}},
		":m2": map[string]any{"M": map[string]any{"x": avn("1")}},
		":l":  map[string]any{"L": []any{av("a"), avn("1")}}, ":l2": map[string]any{"L": []any{avn("1"), av("a")}},
		":a": av("a"), ":dead": ddbBin(0xde, 0xad), ":adbe": ddbBin(0xad, 0xbe),
	}
	cases := []struct {
		cond string
		want bool
	}{
		// #1454 repro: different Binary values are not equal.
		{"b = :b2", false},
		{"b = :b1", true},
		{"b <> :b2", true},
		// Binary ordering is unsigned: 0x80 > 0x7f.
		{"hi > :b7f", true},
		{"hi < :bff", true},
		{"b < :b2", true},
		{"hi BETWEEN :b7f AND :bff", true},
		{"b BETWEEN :b2 AND :bff", false},
		{"b IN (:b2, :b1)", true},
		{"b IN (:b2, :bff)", false},
		{"begins_with(blob, :dead)", true},
		{"begins_with(blob, :adbe)", false},
		{"contains(blob, :adbe)", true},
		// Different types are never equal and never ordered.
		{"s = :n1", false},
		{"n = :s1", false},
		{"s <> :n1", true},
		{"n < :s1", false},
		{"n >= :s1", false},
		{"b = :s1", false},
		// Numbers compare numerically and exactly.
		{"n = :n1x", true},
		{"big < :bigp1", true},
		{"big = :bigp1", false},
		// BOOL, NULL.
		{"t = :true", true},
		{"t = :false", false},
		{"nul = :null", true},
		// Sets are unordered; maps and lists compare deeply.
		{"ss = :ss", true},
		{"ss = :ss2", false},
		{"ns = :ns", true},
		{"bs = :bs", true},
		{"m = :m", true},
		{"m = :m2", false},
		{"l = :l", true},
		{"l = :l2", false},
		{"contains(ss, :a)", true},
		{"contains(ns, :n1x)", true},
		{"contains(l, :n1)", true},
		{"contains(bs, :b2)", true},
		// A missing attribute matches no ordering comparison.
		{"missing < :n1", false},
		{"missing = :n1", false},
		{"missing <> :n1", true},
	}
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "cmp", [3]string{"pk", "S", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "cmp", "Item": item})
	for _, tc := range cases {
		t.Run(tc.cond, func(t *testing.T) {
			used := map[string]any{}
			for k, v := range values {
				if containsToken(tc.cond, k) {
					used[k] = v
				}
			}
			status, body := ddbCall(t, srv, "UpdateItem", map[string]any{
				"TableName": "cmp", "Key": map[string]any{"pk": av("1")},
				"ConditionExpression": tc.cond, "UpdateExpression": "SET touched = :touched",
				"ExpressionAttributeValues": mergeMaps(used, map[string]any{":touched": av("y")}),
			})
			if tc.want {
				assert.Equal(t, http.StatusOK, status, "%v", body)
			} else {
				assert.Equal(t, http.StatusBadRequest, status, "%v", body)
				assert.Equal(t, "ConditionalCheckFailedException", ddbErrCode(body))
			}
		})
	}
}

// containsToken reports whether the placeholder tok appears in expr as a whole token.
func containsToken(expr, tok string) bool {
	for i := 0; i+len(tok) <= len(expr); i++ {
		if expr[i:i+len(tok)] != tok {
			continue
		}
		end := i + len(tok)
		if end == len(expr) || !isPlaceholderChar(expr[end]) {
			return true
		}
	}
	return false
}

func isPlaceholderChar(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func mergeMaps(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func TestDynamoDB_BinaryKeysAreDistinct(t *testing.T) {
	srv, state := newDynamoDBStateServer(t)
	ddbCreateTable(t, srv, "bk", [3]string{"id", "B", "HASH"})
	for _, b := range []byte{1, 2, 3} {
		ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "bk", "Item": map[string]any{
			"id": ddbBin(b), "v": avn(fmt.Sprint(b)),
		}})
	}
	for _, b := range []byte{1, 2, 3} {
		got := ddbGet(t, srv, "bk", map[string]any{"id": ddbBin(b)})
		require.NotNil(t, got)
		assert.Equal(t, avn(fmt.Sprint(b)), got["v"])
	}
	scan := ddbMustOK(t, srv, "Scan", map[string]any{"TableName": "bk"})
	assert.EqualValues(t, 3, scan["Count"])

	// The state key of a Binary key is "B:" + base64.
	raw, err := state.Get(context.Background(), "dynamodb", "item:123456789012/us-east-1/bk/B:AQ==")
	require.NoError(t, err)
	assert.NotNil(t, raw)
}

func TestDynamoDB_StringAndNumberStateKeysUnchanged(t *testing.T) {
	srv, state := newDynamoDBStateServer(t)
	ddbCreateTable(t, srv, "sk", [3]string{"pk", "S", "HASH"}, [3]string{"n", "N", "RANGE"})
	ddbCreateTable(t, srv, "nk", [3]string{"id", "N", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "sk", "Item": map[string]any{"pk": av("user#1"), "n": avn("42")}})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "nk", "Item": map[string]any{"id": avn("7")}})
	for _, key := range []string{
		"item:123456789012/us-east-1/sk/user#1#42",
		"item:123456789012/us-east-1/nk/7",
	} {
		raw, err := state.Get(context.Background(), "dynamodb", key)
		require.NoError(t, err)
		assert.NotNil(t, raw, key)
	}
}

func TestDynamoDB_KeyTypeMismatchIsRejected(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "typed", [3]string{"pk", "S", "HASH"})
	status, body := ddbCall(t, srv, "PutItem", map[string]any{"TableName": "typed", "Item": map[string]any{"pk": avn("1")}})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationException", ddbErrCode(body))
	assert.Contains(t, fmt.Sprint(body), "Type mismatch for key pk expected: S actual: N")
}

func TestDynamoDB_QueryTypedSortKeyOrder(t *testing.T) {
	t.Run("binary sort key orders by unsigned bytes", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "qb", [3]string{"pk", "S", "HASH"}, [3]string{"sk", "B", "RANGE"})
		for _, b := range []byte{0x80, 0x01, 0x7f, 0xff} {
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "qb", "Item": map[string]any{"pk": av("p"), "sk": ddbBin(b)}})
		}
		out := ddbMustOK(t, srv, "Query", map[string]any{
			"TableName": "qb", "KeyConditionExpression": "pk = :p",
			"ExpressionAttributeValues": map[string]any{":p": av("p")},
		})
		assert.Equal(t, []any{ddbBin(0x01), ddbBin(0x7f), ddbBin(0x80), ddbBin(0xff)}, ddbColumn(out, "sk"))

		out = ddbMustOK(t, srv, "Query", map[string]any{
			"TableName": "qb", "KeyConditionExpression": "pk = :p AND sk > :x",
			"ExpressionAttributeValues": map[string]any{":p": av("p"), ":x": ddbBin(0x7f)},
			"ScanIndexForward":          false,
		})
		assert.Equal(t, []any{ddbBin(0xff), ddbBin(0x80)}, ddbColumn(out, "sk"))

		out = ddbMustOK(t, srv, "Query", map[string]any{
			"TableName": "qb", "KeyConditionExpression": "pk = :p AND sk = :x",
			"ExpressionAttributeValues": map[string]any{":p": av("p"), ":x": ddbBin(0x80)},
		})
		assert.Equal(t, []any{ddbBin(0x80)}, ddbColumn(out, "sk"))
	})
	t.Run("number sort key orders numerically", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "qn", [3]string{"pk", "S", "HASH"}, [3]string{"sk", "N", "RANGE"})
		for _, n := range []string{"100", "9", "10", "-1"} {
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "qn", "Item": map[string]any{"pk": av("p"), "sk": avn(n)}})
		}
		out := ddbMustOK(t, srv, "Query", map[string]any{
			"TableName": "qn", "KeyConditionExpression": "pk = :p AND sk BETWEEN :lo AND :hi",
			"ExpressionAttributeValues": map[string]any{":p": av("p"), ":lo": avn("0"), ":hi": avn("50")},
		})
		assert.Equal(t, []any{avn("9"), avn("10")}, ddbColumn(out, "sk"))
	})
}

// ddbColumn returns one attribute of every item of a Query or Scan result, in order.
func ddbColumn(out map[string]any, attr string) []any {
	items, _ := out["Items"].([]any)
	col := make([]any, 0, len(items))
	for _, it := range items {
		col = append(col, it.(map[string]any)[attr])
	}
	return col
}

func TestDynamoDB_ConcurrentConditionalWritesSerialize(t *testing.T) {
	const writers = 24

	t.Run("exactly one attribute_not_exists create wins", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "race", [3]string{"pk", "S", "HASH"})
		var wg sync.WaitGroup
		statuses := make([]int, writers)
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp := dynamodbRequest(t, srv, "UpdateItem", map[string]any{
					"TableName": "race", "Key": map[string]any{"pk": av("claim")},
					"ConditionExpression": "attribute_not_exists(pk)", "UpdateExpression": "SET owner = :me",
					"ExpressionAttributeValues": map[string]any{":me": av(fmt.Sprint(i))},
				})
				statuses[i] = resp.StatusCode
				resp.Body.Close() //nolint:errcheck
			}()
		}
		wg.Wait()
		assert.Equal(t, 1, countStatus(statuses, http.StatusOK))
		assert.Equal(t, writers-1, countStatus(statuses, http.StatusBadRequest))
	})

	t.Run("exactly one lease claim wins", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "lease", [3]string{"pk", "S", "HASH"})
		ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "lease", "Item": map[string]any{"pk": av("job"), "st": av("active")}})
		var wg sync.WaitGroup
		statuses := make([]int, writers)
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp := dynamodbRequest(t, srv, "UpdateItem", map[string]any{
					"TableName": "lease", "Key": map[string]any{"pk": av("job")},
					"ConditionExpression":       "st = :active AND (attribute_not_exists(lease_owner) OR lease_owner = :me)",
					"UpdateExpression":          "SET lease_owner = :me",
					"ExpressionAttributeValues": map[string]any{":active": av("active"), ":me": av(fmt.Sprint(i))},
				})
				statuses[i] = resp.StatusCode
				resp.Body.Close() //nolint:errcheck
			}()
		}
		wg.Wait()
		assert.Equal(t, 1, countStatus(statuses, http.StatusOK))
	})

	t.Run("version-checked increments lose no update", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "ver", [3]string{"pk", "S", "HASH"})
		ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "ver", "Item": map[string]any{"pk": av("c"), "v": avn("0")}})
		const perWriter = 5
		var wg sync.WaitGroup
		for range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for done := 0; done < perWriter; {
					resp := dynamodbRequest(t, srv, "GetItem", map[string]any{"TableName": "ver", "Key": map[string]any{"pk": av("c")}})
					var got struct {
						Item map[string]map[string]string `json:"Item"`
					}
					raw, _ := io.ReadAll(resp.Body)
					resp.Body.Close() //nolint:errcheck
					if json.Unmarshal(raw, &got) != nil {
						continue
					}
					cur := got.Item["v"]["N"]
					resp = dynamodbRequest(t, srv, "UpdateItem", map[string]any{
						"TableName": "ver", "Key": map[string]any{"pk": av("c")},
						"ConditionExpression": "v = :cur", "UpdateExpression": "SET v = v + :one",
						"ExpressionAttributeValues": map[string]any{":cur": avn(cur), ":one": avn("1")},
					})
					if resp.StatusCode == http.StatusOK {
						done++
					}
					resp.Body.Close() //nolint:errcheck
				}
			}()
		}
		wg.Wait()
		got := ddbGet(t, srv, "ver", map[string]any{"pk": av("c")})
		assert.Equal(t, avn(fmt.Sprint(writers*perWriter)), got["v"])
	})

	t.Run("unconditional ADD loses no update", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "add", [3]string{"pk", "S", "HASH"})
		var wg sync.WaitGroup
		for range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp := dynamodbRequest(t, srv, "UpdateItem", map[string]any{
					"TableName": "add", "Key": map[string]any{"pk": av("n")},
					"UpdateExpression": "ADD hits :one", "ExpressionAttributeValues": map[string]any{":one": avn("1")},
				})
				resp.Body.Close() //nolint:errcheck
			}()
		}
		wg.Wait()
		assert.Equal(t, avn(fmt.Sprint(writers)), ddbGet(t, srv, "add", map[string]any{"pk": av("n")})["hits"])
	})

	t.Run("concurrent distinct creates are all listed by Scan", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "many", [3]string{"pk", "S", "HASH"})
		ops := []string{"PutItem", "UpdateItem", "BatchWriteItem", "TransactWriteItems"}
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := fmt.Sprintf("item-%02d", i)
				var body map[string]any
				switch op := ops[i%len(ops)]; op {
				case "PutItem":
					body = map[string]any{"TableName": "many", "Item": map[string]any{"pk": av(key)}}
				case "UpdateItem":
					body = map[string]any{"TableName": "many", "Key": map[string]any{"pk": av(key)},
						"UpdateExpression": "SET x = :x", "ExpressionAttributeValues": map[string]any{":x": avn("1")}}
				case "BatchWriteItem":
					body = map[string]any{"RequestItems": map[string]any{"many": []any{
						map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"pk": av(key)}}},
					}}}
				default:
					body = map[string]any{"TransactItems": []any{
						map[string]any{"Put": map[string]any{"TableName": "many", "Item": map[string]any{"pk": av(key)}}},
					}}
				}
				resp := dynamodbRequest(t, srv, ops[i%len(ops)], body)
				resp.Body.Close() //nolint:errcheck
			}()
		}
		wg.Wait()
		scan := ddbMustOK(t, srv, "Scan", map[string]any{"TableName": "many"})
		assert.EqualValues(t, writers, scan["Count"])
		assert.Len(t, ddbStreamRecords(t, srv, "many"), writers, "every create keeps its stream record")
	})

	t.Run("overlapping transactions do not deadlock or lose updates", func(t *testing.T) {
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "txr", [3]string{"pk", "S", "HASH"})
		for _, k := range []string{"a", "b"} {
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "txr", "Item": map[string]any{"pk": av(k), "n": avn("0")}})
		}
		inc := func(k string) map[string]any {
			return map[string]any{"Update": map[string]any{
				"TableName": "txr", "Key": map[string]any{"pk": av(k)},
				"UpdateExpression": "SET n = n + :one", "ExpressionAttributeValues": map[string]any{":one": avn("1")},
			}}
		}
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				items := []any{inc("a"), inc("b")}
				if i%2 == 1 {
					items = []any{inc("b"), inc("a")}
				}
				resp := dynamodbRequest(t, srv, "TransactWriteItems", map[string]any{"TransactItems": items})
				resp.Body.Close() //nolint:errcheck
			}()
		}
		wg.Wait()
		for _, k := range []string{"a", "b"} {
			assert.Equal(t, avn(fmt.Sprint(writers)), ddbGet(t, srv, "txr", map[string]any{"pk": av(k)})["n"], k)
		}
	})
}

func countStatus(statuses []int, want int) int {
	n := 0
	for _, s := range statuses {
		if s == want {
			n++
		}
	}
	return n
}

func TestDynamoDB_PartiQLUpdateSetsLiteralValues(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "pq", [3]string{"pk", "S", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "pq", "Item": map[string]any{"pk": av("k")}})
	ddbMustOK(t, srv, "ExecuteStatement", map[string]any{
		"Statement": `UPDATE "pq" SET name = 'Bob''s', age = 42, ok = true`,
	})
	got := ddbGet(t, srv, "pq", map[string]any{"pk": av("k")})
	assert.Equal(t, av("Bob's"), got["name"])
	assert.Equal(t, avn("42"), got["age"])
	assert.Equal(t, map[string]any{"BOOL": true}, got["ok"])
}

func TestDynamoDB_PartiQLWhereClause(t *testing.T) {
	setup := func(t *testing.T) *emulator.Server {
		t.Helper()
		srv := newDynamoDBTestServer(t)
		ddbCreateTable(t, srv, "pw", [3]string{"pk", "S", "HASH"})
		for _, k := range []string{"k1", "k2", "k3"} {
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "pw", "Item": map[string]any{"pk": av(k), "n": avn("1")}})
		}
		return srv
	}
	get := func(t *testing.T, srv *emulator.Server, k string) map[string]any {
		t.Helper()
		return ddbGet(t, srv, "pw", map[string]any{"pk": av(k)})
	}

	t.Run("UPDATE with a quoted-literal WHERE updates only the match", func(t *testing.T) {
		srv := setup(t)
		ddbMustOK(t, srv, "ExecuteStatement", map[string]any{"Statement": `UPDATE "pw" SET a = 1 WHERE pk = 'k1'`})
		assert.Equal(t, avn("1"), get(t, srv, "k1")["a"])
		assert.NotContains(t, get(t, srv, "k2"), "a")
	})
	t.Run("a SET literal containing WHERE is kept whole", func(t *testing.T) {
		srv := setup(t)
		ddbMustOK(t, srv, "ExecuteStatement", map[string]any{"Statement": `UPDATE "pw" SET note = 'x WHERE y' WHERE pk = 'k2'`})
		assert.Equal(t, av("x WHERE y"), get(t, srv, "k2")["note"])
		assert.NotContains(t, get(t, srv, "k1"), "note")
	})
	t.Run("DELETE with a WHERE deletes only the match", func(t *testing.T) {
		srv := setup(t)
		ddbMustOK(t, srv, "ExecuteStatement", map[string]any{"Statement": `DELETE FROM "pw" WHERE pk = 'k2'`})
		assert.Nil(t, get(t, srv, "k2"))
		assert.NotNil(t, get(t, srv, "k1"))
		assert.NotNil(t, get(t, srv, "k3"))
	})
	t.Run("SELECT with WHERE on a string and a number", func(t *testing.T) {
		srv := setup(t)
		out := ddbMustOK(t, srv, "ExecuteStatement", map[string]any{"Statement": `SELECT * FROM "pw" WHERE pk = 'k3' AND n = 1`})
		assert.Len(t, out["Items"], 1)
		out = ddbMustOK(t, srv, "ExecuteStatement", map[string]any{"Statement": `SELECT * FROM "pw" WHERE pk != 'k3'`})
		assert.Len(t, out["Items"], 2)
	})
	t.Run("? parameters are substituted in statement order", func(t *testing.T) {
		srv := setup(t)
		ddbMustOK(t, srv, "ExecuteStatement", map[string]any{
			"Statement":  `UPDATE "pw" SET a = ? WHERE pk = ?`,
			"Parameters": []any{avn("7"), av("k3")},
		})
		assert.Equal(t, avn("7"), get(t, srv, "k3")["a"])
		assert.NotContains(t, get(t, srv, "k1"), "a")
	})
	for _, tc := range []struct {
		name   string
		stmt   string
		params []any
	}{
		{"an unterminated literal", `DELETE FROM "pw" WHERE pk = 'k1`, nil},
		{"a clause that does not parse", `UPDATE "pw" SET a = 1 WHERE pk = = 'k1'`, nil},
		{"too few parameters", `DELETE FROM "pw" WHERE pk = ?`, nil},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			srv := setup(t)
			req := map[string]any{"Statement": tc.stmt}
			if tc.params != nil {
				req["Parameters"] = tc.params
			}
			status, body := ddbCall(t, srv, "ExecuteStatement", req)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationException", ddbErrCode(body))
			for _, k := range []string{"k1", "k2", "k3"} {
				assert.NotNil(t, get(t, srv, k), "a refused statement changes nothing")
			}
		})
	}
}

func TestDynamoDB_NumberKeysAreCanonical(t *testing.T) {
	srv, state := newDynamoDBStateServer(t)
	ddbCreateTable(t, srv, "nc", [3]string{"id", "N", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "nc", "Item": map[string]any{"id": avn("1.0"), "v": av("first")}})

	got := ddbGet(t, srv, "nc", map[string]any{"id": avn("1")})
	require.NotNil(t, got, "1.0 and 1 are one key")
	assert.Equal(t, av("first"), got["v"])

	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "nc", "Item": map[string]any{"id": avn("1E0"), "v": av("second")}})
	scan := ddbMustOK(t, srv, "Scan", map[string]any{"TableName": "nc"})
	assert.EqualValues(t, 1, scan["Count"], "a second spelling of the same number overwrites")

	raw, err := state.Get(context.Background(), "dynamodb", "item:123456789012/us-east-1/nc/1")
	require.NoError(t, err)
	assert.NotNil(t, raw, "stored under the canonical form")
}

func TestDynamoDB_NumberFormatIsValidated(t *testing.T) {
	cases := []struct {
		n       string
		wantErr string // "" when valid
	}{
		{"42", ""},
		{"-1.5e3", ""},
		{".5", ""},
		{"1E-130", ""},
		{"9.9999999999999999999999999999999999999E+125", ""},
		{"12345678901234567890123456789012345678", ""},
		{"0x10", "A value provided cannot be converted into a number"},
		{"1/2", "A value provided cannot be converted into a number"},
		{"0b101", "A value provided cannot be converted into a number"},
		{"1e", "A value provided cannot be converted into a number"},
		{"abc", "A value provided cannot be converted into a number"},
		{"123456789012345678901234567890123456789", "Attempting to store more than 38 significant digits in a Number"},
		{"1E126", "Number overflow"},
		{"1E-131", "Number underflow"},
	}
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "nf", [3]string{"pk", "S", "HASH"})
	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			status, body := ddbCall(t, srv, "PutItem", map[string]any{"TableName": "nf", "Item": map[string]any{"pk": av(tc.n), "n": avn(tc.n)}})
			if tc.wantErr == "" {
				assert.Equal(t, http.StatusOK, status, "%v", body)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationException", ddbErrCode(body))
			assert.Contains(t, fmt.Sprint(body), tc.wantErr)
		})
	}
	t.Run("in a number set inside a map", func(t *testing.T) {
		status, body := ddbCall(t, srv, "PutItem", map[string]any{"TableName": "nf", "Item": map[string]any{
			"pk": av("deep"), "m": map[string]any{"M": map[string]any{"ns": map[string]any{"NS": []any{"1", "0x2"}}}},
		}})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "ValidationException", ddbErrCode(body))
	})
}
