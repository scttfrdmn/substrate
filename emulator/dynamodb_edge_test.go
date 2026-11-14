package emulator_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Edge cases of the DynamoDB expression, comparison and PartiQL code added for #1450,
// #1454 and the review of #1479: every malformed expression is refused with a
// ValidationException rather than misread, and every type pairing compares as the
// Condition reference states.

func TestDynamoDB_MalformedExpressionsAreValidationExceptions(t *testing.T) {
	values := map[string]any{":v": av("x"), ":n": avn("1"), ":ss": map[string]any{"SS": []any{"a"}}}
	cases := []struct {
		name string
		op   string
		body map[string]any
	}{
		// Condition expressions.
		{"unclosed attribute_exists", "UpdateItem", map[string]any{"ConditionExpression": "attribute_exists(a"}},
		{"begins_with without comma", "UpdateItem", map[string]any{"ConditionExpression": "begins_with(a :v)"}},
		{"begins_with unclosed", "UpdateItem", map[string]any{"ConditionExpression": "begins_with(a, :v"}},
		{"BETWEEN without AND", "UpdateItem", map[string]any{"ConditionExpression": "a BETWEEN :v :v"}},
		{"IN without list", "UpdateItem", map[string]any{"ConditionExpression": "a IN :v"}},
		{"IN unclosed", "UpdateItem", map[string]any{"ConditionExpression": "a IN (:v, :v"}},
		{"undefined name", "UpdateItem", map[string]any{"ConditionExpression": "#nope = :v"}},
		{"undefined nested name", "UpdateItem", map[string]any{"ConditionExpression": "a.#nope = :v"}},
		{"undefined value", "UpdateItem", map[string]any{"ConditionExpression": "a = :missing"}},
		{"bad list index", "UpdateItem", map[string]any{"ConditionExpression": "a[x] = :v"}},
		{"unclosed parenthesis", "UpdateItem", map[string]any{"ConditionExpression": "(a = :v"}},
		// Update expressions.
		{"empty SET", "UpdateItem", map[string]any{"UpdateExpression": "SET"}},
		{"SET without =", "UpdateItem", map[string]any{"UpdateExpression": "SET a"}},
		{"SET without value", "UpdateItem", map[string]any{"UpdateExpression": "SET a ="}},
		{"SET trailing comma", "UpdateItem", map[string]any{"UpdateExpression": "SET a = :v,"}},
		{"SET dangling +", "UpdateItem", map[string]any{"UpdateExpression": "SET a = :v +"}},
		{"if_not_exists on a value", "UpdateItem", map[string]any{"UpdateExpression": "SET a = if_not_exists(:v, :v)"}},
		{"if_not_exists without comma", "UpdateItem", map[string]any{"UpdateExpression": "SET a = if_not_exists(a :v)"}},
		{"if_not_exists with bad fallback", "UpdateItem", map[string]any{"UpdateExpression": "SET a = if_not_exists(a, )"}},
		{"list_append without comma", "UpdateItem", map[string]any{"UpdateExpression": "SET a = list_append(:v :v)"}},
		{"list_append unclosed", "UpdateItem", map[string]any{"UpdateExpression": "SET a = list_append(:v, :v"}},
		{"list_append with bad operand", "UpdateItem", map[string]any{"UpdateExpression": "SET a = list_append(, :v)"}},
		{"list_append second bad operand", "UpdateItem", map[string]any{"UpdateExpression": "SET a = list_append(:v, )"}},
		{"ADD with a path value", "UpdateItem", map[string]any{"UpdateExpression": "ADD a b"}},
		{"DELETE without value", "UpdateItem", map[string]any{"UpdateExpression": "DELETE s"}},
		{"not a clause", "UpdateItem", map[string]any{"UpdateExpression": "FOO a"}},
		{"undefined name in SET", "UpdateItem", map[string]any{"UpdateExpression": "SET #nope = :v"}},
		{"bad list index in SET", "UpdateItem", map[string]any{"UpdateExpression": "SET a[x] = :v"}},
		{"unexpected character", "UpdateItem", map[string]any{"UpdateExpression": "SET a = :v $"}},
		{"path then junk", "UpdateItem", map[string]any{"UpdateExpression": "SET a = = :v"}},
		// Key conditions.
		{"no key condition", "Query", map[string]any{"KeyConditionExpression": ""}},
		{"key condition without =", "Query", map[string]any{"KeyConditionExpression": "pk < :v"}},
		{"key condition with a path value", "Query", map[string]any{"KeyConditionExpression": "pk = pk"}},
		{"key condition OR", "Query", map[string]any{"KeyConditionExpression": "pk = :v OR sk = :v"}},
		{"key condition BETWEEN path", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND sk BETWEEN pk AND :v"}},
		{"key condition BETWEEN without AND", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND sk BETWEEN :v OR :v"}},
		{"key condition BETWEEN bad high", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND sk BETWEEN :v AND sk"}},
		{"key condition unsupported operator", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND sk <> :v"}},
		{"key condition bad sort value", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND sk = sk"}},
		{"key condition begins_with unopened", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND begins_with sk"}},
		{"key condition begins_with without comma", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND begins_with(sk :v)"}},
		{"key condition begins_with bad prefix", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND begins_with(sk, sk)"}},
		{"key condition begins_with unclosed", "Query", map[string]any{"KeyConditionExpression": "pk = :v AND begins_with(sk, :v"}},
		{"key condition bad character", "Query", map[string]any{"KeyConditionExpression": "pk = :v $"}},
		{"key condition undefined value", "Query", map[string]any{"KeyConditionExpression": "pk = :missing"}},
	}
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "edge", [3]string{"pk", "S", "HASH"}, [3]string{"sk", "S", "RANGE"})
	key := map[string]any{"pk": av("k"), "sk": av("s")}
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "edge", "Item": map[string]any{"pk": av("k"), "sk": av("s"), "a": av("x")}})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"TableName": "edge", "ExpressionAttributeValues": values}
			if tc.op == "UpdateItem" {
				body["Key"] = key
				body["UpdateExpression"] = "SET touched = :v"
			}
			for k, v := range tc.body {
				body[k] = v
			}
			status, out := ddbCall(t, srv, tc.op, body)
			assert.Equal(t, http.StatusBadRequest, status, "%v", out)
			assert.Equal(t, "ValidationException", ddbErrCode(out))
		})
	}
}

func TestDynamoDB_UpdateExpression_TypeRules(t *testing.T) {
	b64 := func(b ...byte) string { return base64.StdEncoding.EncodeToString(b) }
	cases := []struct {
		name    string
		initial map[string]any
		expr    string
		values  map[string]any
		want    map[string]any
		absent  []string
		wantErr string
	}{
		{name: "ADD a number to a set", initial: map[string]any{"ss": map[string]any{"SS": []any{"a"}}}, expr: "ADD ss :n",
			values: map[string]any{":n": avn("1")}, wantErr: "incorrect data type"},
		{name: "ADD a string set to a number set", initial: map[string]any{"ns": map[string]any{"NS": []any{"1"}}}, expr: "ADD ns :s",
			values: map[string]any{":s": map[string]any{"SS": []any{"a"}}}, wantErr: "incorrect data type"},
		{name: "ADD binary sets", initial: map[string]any{"bs": map[string]any{"BS": []any{b64(1)}}}, expr: "ADD bs :b",
			values: map[string]any{":b": map[string]any{"BS": []any{b64(1), b64(2)}}},
			want:   map[string]any{"bs": map[string]any{"BS": []any{b64(1), b64(2)}}}},
		{name: "ADD a string set to an absent attribute", expr: "ADD ss :s",
			values: map[string]any{":s": map[string]any{"SS": []any{"a"}}}, want: map[string]any{"ss": map[string]any{"SS": []any{"a"}}}},
		{name: "ADD a binary", expr: "ADD x :v", values: map[string]any{":v": ddbBin(1)}, wantErr: "operand type: BINARY"},
		{name: "ADD a boolean", expr: "ADD x :v", values: map[string]any{":v": map[string]any{"BOOL": true}}, wantErr: "operand type: BOOLEAN"},
		{name: "ADD a list", expr: "ADD x :v", values: map[string]any{":v": map[string]any{"L": []any{}}}, wantErr: "operand type: LIST"},
		{name: "ADD a map", expr: "ADD x :v", values: map[string]any{":v": map[string]any{"M": map[string]any{}}}, wantErr: "operand type: MAP"},
		{name: "ADD a null", expr: "ADD x :v", values: map[string]any{":v": map[string]any{"NULL": true}}, wantErr: "operand type: NULL"},
		{name: "DELETE a number", expr: "DELETE x :v", values: map[string]any{":v": avn("1")}, wantErr: "operator: DELETE, operand type: NUMBER"},
		{name: "DELETE from an absent set", expr: "DELETE x :v", values: map[string]any{":v": map[string]any{"SS": []any{"a"}}}, absent: []string{"x"}},
		{name: "DELETE mismatched set types", initial: map[string]any{"ns": map[string]any{"NS": []any{"1"}}}, expr: "DELETE ns :s",
			values: map[string]any{":s": map[string]any{"SS": []any{"a"}}}, wantErr: "incorrect data type"},
		{name: "DELETE from a number set", initial: map[string]any{"ns": map[string]any{"NS": []any{"1", "2"}}}, expr: "DELETE ns :s",
			values: map[string]any{":s": map[string]any{"NS": []any{"1.0"}}}, want: map[string]any{"ns": map[string]any{"NS": []any{"2"}}}},
		{name: "DELETE the last number", initial: map[string]any{"ns": map[string]any{"NS": []any{"1"}}}, expr: "DELETE ns :s",
			values: map[string]any{":s": map[string]any{"NS": []any{"1"}}}, absent: []string{"ns"}},
		{name: "DELETE from a binary set", initial: map[string]any{"bs": map[string]any{"BS": []any{b64(1), b64(2)}}}, expr: "DELETE bs :s",
			values: map[string]any{":s": map[string]any{"BS": []any{b64(2)}}}, want: map[string]any{"bs": map[string]any{"BS": []any{b64(1)}}}},
		{name: "DELETE the last binary", initial: map[string]any{"bs": map[string]any{"BS": []any{b64(1)}}}, expr: "DELETE bs :s",
			values: map[string]any{":s": map[string]any{"BS": []any{b64(1)}}}, absent: []string{"bs"}},
		{name: "DELETE from a string set", initial: map[string]any{"ss": map[string]any{"SS": []any{"a", "b"}}}, expr: "DELETE ss :s",
			values: map[string]any{":s": map[string]any{"SS": []any{"a"}}}, want: map[string]any{"ss": map[string]any{"SS": []any{"b"}}}},
		{name: "SET under a string parent", initial: map[string]any{"a": av("x")}, expr: "SET a.b = :v",
			values: map[string]any{":v": av("y")}, wantErr: "document path provided in the update expression is invalid"},
		{name: "SET a list index of a map", initial: map[string]any{"m": map[string]any{"M": map[string]any{}}}, expr: "SET m[0] = :v",
			values: map[string]any{":v": av("y")}, wantErr: "document path provided in the update expression is invalid"},
		{name: "SET into a nested list element", initial: map[string]any{"m": map[string]any{"M": map[string]any{"l": map[string]any{"L": []any{av("a")}}}}},
			expr: "SET m.l[0] = :v", values: map[string]any{":v": av("b")},
			want: map[string]any{"m": map[string]any{"M": map[string]any{"l": map[string]any{"L": []any{av("b")}}}}}},
		{name: "REMOVE a nested map key", initial: map[string]any{"m": map[string]any{"M": map[string]any{"x": av("1"), "y": av("2")}}},
			expr: "REMOVE m.x, m.missing.deeper, gone[3]", want: map[string]any{"m": map[string]any{"M": map[string]any{"y": av("2")}}}},
		{name: "REMOVE an index past the end", initial: map[string]any{"l": map[string]any{"L": []any{av("a")}}},
			expr: "REMOVE l[5]", want: map[string]any{"l": map[string]any{"L": []any{av("a")}}}},
		{name: "subtraction on a string", initial: map[string]any{"s": av("x")}, expr: "SET s = s - :n",
			values: map[string]any{":n": avn("1")}, wantErr: "incorrect data type"},
		{name: "list_append with an absent path", expr: "SET l = list_append(l, :l)",
			values: map[string]any{":l": map[string]any{"L": []any{}}}, wantErr: "refers to an attribute that does not exist"},
		{name: "list_append with an absent second path", expr: "SET l = list_append(:l, l)",
			values: map[string]any{":l": map[string]any{"L": []any{}}}, wantErr: "refers to an attribute that does not exist"},
		{name: "subtraction with an absent right operand", initial: map[string]any{"n": avn("1")}, expr: "SET n = n - m",
			wantErr: "refers to an attribute that does not exist"},
		{name: "ADD past the Number range", initial: map[string]any{"n": avn("9E+125")}, expr: "ADD n :v",
			values: map[string]any{":v": avn("9E+125")}, wantErr: "Number overflow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newDynamoDBTestServer(t)
			ddbCreateTable(t, srv, "tr", [3]string{"pk", "S", "HASH"})
			item := map[string]any{"pk": av("k")}
			for k, v := range tc.initial {
				item[k] = v
			}
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "tr", "Item": item})
			req := map[string]any{"TableName": "tr", "Key": map[string]any{"pk": av("k")}, "UpdateExpression": tc.expr}
			if tc.values != nil {
				req["ExpressionAttributeValues"] = tc.values
			}
			status, body := ddbCall(t, srv, "UpdateItem", req)
			if tc.wantErr != "" {
				require.Equal(t, http.StatusBadRequest, status, "%v", body)
				assert.Contains(t, fmt.Sprint(body), tc.wantErr)
				return
			}
			require.Equal(t, http.StatusOK, status, "%v", body)
			got := ddbGet(t, srv, "tr", map[string]any{"pk": av("k")})
			for k, v := range tc.want {
				assert.Equal(t, v, got[k], k)
			}
			for _, k := range tc.absent {
				assert.NotContains(t, got, k)
			}
		})
	}
}

func TestDynamoDB_ConditionTypedComparison_MorePairings(t *testing.T) {
	b64 := func(b ...byte) string { return base64.StdEncoding.EncodeToString(b) }
	item := map[string]any{
		"pk": av("1"), "f": map[string]any{"BOOL": false}, "nul": map[string]any{"NULL": true},
		"ss": map[string]any{"SS": []any{"a", "b"}}, "ns": map[string]any{"NS": []any{"1", "2"}},
		"bs": map[string]any{"BS": []any{b64(1)}}, "s": av("abc"), "b": ddbBin(1, 2),
		"l": map[string]any{"L": []any{av("a")}}, "m": map[string]any{"M": map[string]any{"x": av("1")}},
	}
	values := map[string]any{
		":false": map[string]any{"BOOL": false}, ":null": map[string]any{"NULL": true},
		":ss3": map[string]any{"SS": []any{"a", "b", "c"}}, ":ssx": map[string]any{"SS": []any{"a", "x"}},
		":ns3": map[string]any{"NS": []any{"1", "3"}}, ":n1": avn("1"), ":s": av("a"), ":b1": ddbBin(1),
		":b9": ddbBin(9), ":l2": map[string]any{"L": []any{av("a"), av("b")}},
		":my": map[string]any{"M": map[string]any{"y": av("1")}}, ":typeS": av("S"),
	}
	cases := []struct {
		cond string
		want bool
	}{
		{"f = :false", true},
		{"nul = :null", true},
		{"ss = :ss3", false},
		{"ss = :ssx", false},
		{"ns = :ns3", false},
		{"l = :l2", false},
		{"m = :my", false},
		{"contains(ss, :n1)", false},
		{"contains(ns, :s)", false},
		{"contains(ns, :n1)", true},
		{"contains(bs, :b1)", true},
		{"contains(bs, :s)", false},
		{"contains(bs, :b9)", false},
		{"contains(s, :n1)", false},
		{"contains(b, :b1)", true},
		{"contains(missing, :s)", false},
		{"contains(m, :s)", false},
		{"begins_with(s, :b1)", false},
		{"begins_with(b, :b1)", true},
		{"begins_with(missing, :s)", false},
		{"attribute_type(s, :typeS)", true},
		{"attribute_type(missing, :typeS)", false},
		{"f", false},
		{"s", true},
		{"ss < :ss3", false},
		{"s BETWEEN :n1 AND :n1", false},
	}
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "more", [3]string{"pk", "S", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "more", "Item": item})
	for _, tc := range cases {
		t.Run(tc.cond, func(t *testing.T) {
			used := map[string]any{}
			for k, v := range values {
				if containsToken(tc.cond, k) {
					used[k] = v
				}
			}
			out := ddbMustOK(t, srv, "Scan", map[string]any{"TableName": "more", "FilterExpression": tc.cond, "ExpressionAttributeValues": nonEmpty(used)})
			if tc.want {
				assert.EqualValues(t, 1, out["Count"], tc.cond)
			} else {
				assert.EqualValues(t, 0, out["Count"], tc.cond)
			}
		})
	}
}

// nonEmpty returns m, or nil when it is empty, so a request omits an empty map.
func nonEmpty(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	return m
}

func TestDynamoDB_PartiQLStatementShapes(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "ps", [3]string{"pk", "S", "HASH"})
	ok := func(stmt string) map[string]any {
		t.Helper()
		return ddbMustOK(t, srv, "ExecuteStatement", map[string]any{"Statement": stmt})
	}
	ok(`INSERT INTO ps VALUE {"pk": {"S": "a"}, "n": {"N": "1"}, "on": {"BOOL": true}}`)
	ok(`INSERT INTO "ps" VALUE {"pk": {"S": "b"}, "n": {"N": "2"}, "on": {"BOOL": false}}`)

	assert.Len(t, ok(`SELECT * FROM ps WHERE "n" = 2`)["Items"], 1, "a double-quoted name in WHERE")
	assert.Len(t, ok(`SELECT * FROM "ps" WHERE on = true`)["Items"], 1, "a boolean literal")
	assert.Len(t, ok(`SELECT * FROM "ps" WHERE n >= -1.5e0`)["Items"], 2, "a signed exponent literal")
	assert.Len(t, ok(`SELECT * FROM "ps"`)["Items"], 2)
	ok(`UPDATE "ps" SET "label" = 'L', flag = false WHERE pk = 'a'`)
	got := ddbGet(t, srv, "ps", map[string]any{"pk": av("a")})
	assert.Equal(t, av("L"), got["label"])
	assert.Equal(t, map[string]any{"BOOL": false}, got["flag"])

	for _, stmt := range []string{
		`SELECT *`,
		`INSERT "ps"`,
		`DELETE "ps"`,
		`MERGE INTO "ps"`,
		`INSERT INTO "ps" VALUE {not json}`,
		`INSERT INTO "ps" VALUE {"pk": {"S": "c"}, "n": {"N": "0x1"}}`,
		`INSERT INTO "ps" VALUE {"n": {"N": "1"}}`,
		`SELECT * FROM "ps" WHERE "unterminated = 1`,
		`SELECT * FROM "ps" WHERE n = 1x`,
		`SELECT * FROM "ps" WHERE n = ?`,
		`UPDATE "ps" SET a WHERE pk = 'a'`,
		`UPDATE "ps" SET = 1 WHERE pk = 'a'`,
		`UPDATE "ps" SET a = 'unterminated WHERE pk = 'a'`,
		`UPDATE "ps" SET a = b WHERE pk = 'a'`,
		`UPDATE "ps" SET a = 1 WHERE pk = = 'a'`,
		`DELETE FROM "ps" WHERE pk = = 'a'`,
	} {
		t.Run(stmt, func(t *testing.T) {
			status, body := ddbCall(t, srv, "ExecuteStatement", map[string]any{"Statement": stmt})
			assert.Equal(t, http.StatusBadRequest, status, "%v", body)
			assert.Equal(t, "ValidationException", ddbErrCode(body))
		})
	}

	t.Run("unknown table", func(t *testing.T) {
		status, body := ddbCall(t, srv, "ExecuteStatement", map[string]any{"Statement": `SELECT * FROM "nope"`})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "ResourceNotFoundException", ddbErrCode(body))
	})
}

func TestDynamoDB_ReturnValuesOnDeleteAndBatchDelete(t *testing.T) {
	srv := newDynamoDBTestServer(t)
	ddbCreateTable(t, srv, "rd", [3]string{"pk", "S", "HASH"})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "rd", "Item": map[string]any{"pk": av("a")}})
	ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "rd", "Item": map[string]any{"pk": av("b")}})
	ddbMustOK(t, srv, "BatchWriteItem", map[string]any{"RequestItems": map[string]any{"rd": []any{
		map[string]any{"DeleteRequest": map[string]any{"Key": map[string]any{"pk": av("a")}}},
		map[string]any{"DeleteRequest": map[string]any{"Key": map[string]any{"pk": av("never")}}},
	}}})
	assert.Nil(t, ddbGet(t, srv, "rd", map[string]any{"pk": av("a")}))
	ddbMustOK(t, srv, "TransactWriteItems", map[string]any{"TransactItems": []any{
		map[string]any{"Delete": map[string]any{"TableName": "rd", "Key": map[string]any{"pk": av("b")}}},
		map[string]any{"ConditionCheck": map[string]any{"TableName": "rd", "Key": map[string]any{"pk": av("x")}, "ConditionExpression": "attribute_not_exists(pk)"}},
		map[string]any{},
	}})
	assert.Nil(t, ddbGet(t, srv, "rd", map[string]any{"pk": av("b")}))

	for _, tc := range []struct {
		name string
		op   string
		body map[string]any
		code string
	}{
		{"transact on a missing table", "TransactWriteItems", map[string]any{"TransactItems": []any{
			map[string]any{"Put": map[string]any{"TableName": "nope", "Item": map[string]any{"pk": av("a")}}}}}, "ResourceNotFoundException"},
		{"transact with a bad key", "TransactWriteItems", map[string]any{"TransactItems": []any{
			map[string]any{"Put": map[string]any{"TableName": "rd", "Item": map[string]any{"other": av("a")}}}}}, "ValidationException"},
		{"transact with a bad number", "TransactWriteItems", map[string]any{"TransactItems": []any{
			map[string]any{"Put": map[string]any{"TableName": "rd", "Item": map[string]any{"pk": av("a"), "n": avn("x")}}}}}, "ValidationException"},
		{"transact with a malformed condition", "TransactWriteItems", map[string]any{"TransactItems": []any{
			map[string]any{"ConditionCheck": map[string]any{"TableName": "rd", "Key": map[string]any{"pk": av("a")}, "ConditionExpression": "a ="}}}}, "ValidationException"},
		{"batch with a bad key", "BatchWriteItem", map[string]any{"RequestItems": map[string]any{"rd": []any{
			map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"other": av("a")}}}}}}, "ValidationException"},
		{"batch with a bad number", "BatchWriteItem", map[string]any{"RequestItems": map[string]any{"rd": []any{
			map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"pk": av("a"), "n": avn("x")}}}}}}, "ValidationException"},
		{"get with a wrong key type", "GetItem", map[string]any{"TableName": "rd", "Key": map[string]any{"pk": avn("1")}}, "ValidationException"},
		{"delete with a wrong key type", "DeleteItem", map[string]any{"TableName": "rd", "Key": map[string]any{"pk": avn("1")}}, "ValidationException"},
		{"update with a wrong key type", "UpdateItem", map[string]any{"TableName": "rd", "Key": map[string]any{"pk": avn("1")}}, "ValidationException"},
		{"query with a bad number value", "Query", map[string]any{"TableName": "rd", "KeyConditionExpression": "pk = :v",
			"ExpressionAttributeValues": map[string]any{":v": avn("x")}}, "ValidationException"},
		{"scan with a bad number value", "Scan", map[string]any{"TableName": "rd", "FilterExpression": "n = :v",
			"ExpressionAttributeValues": map[string]any{":v": avn("x")}}, "ValidationException"},
		{"update with a malformed condition", "UpdateItem", map[string]any{"TableName": "rd", "Key": map[string]any{"pk": av("z")},
			"ConditionExpression": "a ="}, "ValidationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := ddbCall(t, srv, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%v", body)
			assert.Equal(t, tc.code, ddbErrCode(body))
		})
	}
}
