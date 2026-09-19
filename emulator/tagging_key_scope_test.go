package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A Lambda function's state key carried neither the account nor the Region, and a DynamoDB table's
// carried the account but not the Region (#943).
//
// Both are wrong against the identifier each service publishes. CreateFunction's FunctionArn is
// arn:{partition}:lambda:{region}:{account}:function:{name}, and CreateTable's TableArn is
// arn:{partition}:dynamodb:{region}:{account}:table/{name} — a name is qualified by both halves, so
// two callers naming one function are naming two functions. Substrate keyed both on the name alone,
// or on the name and the account, so the second create found the first's record and answered
// ResourceConflictException/409 or ResourceInUseException/400: a collision reported to a caller who
// had done nothing wrong, and a record shared between two resources that are not the same resource.
//
// The tests here are the observable half of that fix, and they are the reason the key shape is worth
// changing rather than merely tidying. Each creates its resources through the owning service's own
// operation against the endpoint of the Region it is meant to be in, as the account it is meant to
// belong to, and reads them back the same way — a helper writing state directly would choose the very
// fields the key is built from, so it could only prove the test agrees with itself (#765).
//
// The scope-scan tests in tagging_scan_scope_test.go are the complement: they assert that a resource
// is reported only to its own account in its own Region, which is a listing property. These assert
// that the resource exists at all, which is the property a listing cannot see — a resource that
// collided on create is not mis-reported, it is refused.

// keyScopeFunction creates a Lambda function in one account and Region and returns its reported ARN.
//
// Description is what distinguishes two functions of one name: it is a member the caller chose and
// the service echoes, so reading it back proves which of the two records answered rather than merely
// that some record did.
func keyScopeFunction(t *testing.T, ts *emulator.TestServer, account, region, name,
	description string,
) string {
	t.Helper()
	var out struct {
		FunctionArn string `json:"FunctionArn"`
	}
	scanScopeREST(t, ts, account, "lambda", region, http.MethodPost, "/2015-03-31/functions",
		map[string]any{
			"FunctionName": name,
			"Runtime":      "python3.12",
			"Role":         "arn:aws:iam::" + account + ":role/lambda-exec",
			"Handler":      "index.handler",
			"Description":  description,
			"Code":         map[string]any{"ZipFile": "ZHVtbXk="},
		}, &out)
	require.NotEmptyf(t, out.FunctionArn, "CreateFunction %s in %s/%s reports an ARN",
		name, account, region)
	return out.FunctionArn
}

// keyScopeGetFunction reads a function back through GetFunction and returns its ARN and description.
func keyScopeGetFunction(t *testing.T, ts *emulator.TestServer, account, region,
	name string,
) (arn, description string) {
	t.Helper()
	var out struct {
		Configuration struct {
			FunctionArn string `json:"FunctionArn"`
			Description string `json:"Description"`
		} `json:"Configuration"`
	}
	scanScopeREST(t, ts, account, "lambda", region, http.MethodGet,
		"/2015-03-31/functions/"+name, nil, &out)
	return out.Configuration.FunctionArn, out.Configuration.Description
}

// keyScopeFunctionTags reads one function's tags through Lambda's own ListTags.
//
// Lambda's, not GetResources': the question is what is on the record, and a tagging-API listing
// answers a second question — whether the scanner's scope reaches it — which would make a missing
// tag ambiguous between the two.
func keyScopeFunctionTags(t *testing.T, ts *emulator.TestServer, account, region,
	arn string,
) map[string]string {
	t.Helper()
	var out struct {
		Tags map[string]string `json:"Tags"`
	}
	scanScopeREST(t, ts, account, "lambda", region, http.MethodGet,
		"/2017-03-31/tags/"+arn, nil, &out)
	return out.Tags
}

// keyScopeTable creates a DynamoDB table in one account and Region and returns its reported ARN.
func keyScopeTable(t *testing.T, ts *emulator.TestServer, account, region, name string) string {
	t.Helper()
	var out struct {
		TableDescription struct {
			TableArn string `json:"TableArn"`
		} `json:"TableDescription"`
	}
	scanScopeJSON(t, ts, account, "dynamodb", region, "DynamoDB_20120810", "CreateTable",
		map[string]any{
			"TableName":            name,
			"BillingMode":          "PAY_PER_REQUEST",
			"KeySchema":            []map[string]string{{"AttributeName": "pk", "KeyType": "HASH"}},
			"AttributeDefinitions": []map[string]string{{"AttributeName": "pk", "AttributeType": "S"}},
		}, &out)
	require.NotEmptyf(t, out.TableDescription.TableArn, "CreateTable %s in %s/%s reports an ARN",
		name, account, region)
	return out.TableDescription.TableArn
}

// keyScopeDescribeTable reads a table back through DescribeTable and returns its reported ARN.
func keyScopeDescribeTable(t *testing.T, ts *emulator.TestServer, account, region,
	name string,
) string {
	t.Helper()
	var out struct {
		Table struct {
			TableArn string `json:"TableArn"`
		} `json:"Table"`
	}
	scanScopeJSON(t, ts, account, "dynamodb", region, "DynamoDB_20120810", "DescribeTable",
		map[string]any{"TableName": name}, &out)
	return out.Table.TableArn
}

// TestTaggingKeyScope_TwoAccountsHoldOneFunctionName is the account half of #943.
//
// Before the fix the second CreateFunction answered ResourceConflictException/409, because
// lambdaFunctionStateKey was "function:{name}" and the first account's record was already there. The
// assertion is deliberately the plain one — both creates succeed and both read back — because that is
// the whole of what a caller wants and none of it was true.
//
// Each function carries a distinct Description, so the read proves which record answered. Without it
// a shared record would still satisfy both GetFunctions, since the collision this test exists to
// catch is precisely two names resolving to one thing.
func TestTaggingKeyScope_TwoAccountsHoldOneFunctionName(t *testing.T) {
	ts := scanScopeServer(t)
	const name = "key-scope-shared-fn"

	ownARN := keyScopeFunction(t, ts, taggingTestAccount, scanScopeEast, name, "owned by the caller")
	foreignARN := keyScopeFunction(t, ts, taggingForeignAccount, scanScopeEast, name,
		"owned by the other account")

	assert.Equal(t, "arn:aws:lambda:"+scanScopeEast+":"+taggingTestAccount+":function:"+name, ownARN)
	assert.Equal(t, "arn:aws:lambda:"+scanScopeEast+":"+taggingForeignAccount+":function:"+name,
		foreignARN)

	gotARN, gotDesc := keyScopeGetFunction(t, ts, taggingTestAccount, scanScopeEast, name)
	assert.Equal(t, ownARN, gotARN, "the caller's GetFunction reports the caller's function")
	assert.Equal(t, "owned by the caller", gotDesc,
		"the caller reads its own record, not the other account's")

	gotARN, gotDesc = keyScopeGetFunction(t, ts, taggingForeignAccount, scanScopeEast, name)
	assert.Equal(t, foreignARN, gotARN, "the other account's GetFunction reports its own function")
	assert.Equal(t, "owned by the other account", gotDesc,
		"the other account reads its own record, not the caller's")
}

// TestTaggingKeyScope_OneAccountHoldsOneFunctionNameInTwoRegions is the Region half for Lambda.
//
// It is a separate case from the account half because the two halves failed for different reasons:
// the key carried neither, so fixing only the account would leave one account's us-east-1 and
// us-west-2 functions of one name still sharing a record.
func TestTaggingKeyScope_OneAccountHoldsOneFunctionNameInTwoRegions(t *testing.T) {
	ts := scanScopeServer(t)
	const name = "key-scope-two-region-fn"

	eastARN := keyScopeFunction(t, ts, taggingTestAccount, scanScopeEast, name, "the east function")
	westARN := keyScopeFunction(t, ts, taggingTestAccount, scanScopeWest, name, "the west function")
	require.NotEqual(t, eastARN, westARN, "the two functions report different ARNs")

	gotARN, gotDesc := keyScopeGetFunction(t, ts, taggingTestAccount, scanScopeEast, name)
	assert.Equal(t, eastARN, gotARN)
	assert.Equal(t, "the east function", gotDesc, "us-east-1 reads the us-east-1 record")

	gotARN, gotDesc = keyScopeGetFunction(t, ts, taggingTestAccount, scanScopeWest, name)
	assert.Equal(t, westARN, gotARN)
	assert.Equal(t, "the west function", gotDesc, "us-west-2 reads the us-west-2 record")
}

// TestTaggingKeyScope_OneAccountHoldsOneTableNameInTwoRegions is the Region half for DynamoDB.
//
// Before the fix the second CreateTable answered ResourceInUseException/400: the key was
// "table:{account}/{name}", so one account could hold one table name once across every Region, and a
// consumer deploying the same stack to two Regions — which is the ordinary case, not an exotic one —
// was told its table already existed.
//
// There is no account half for DynamoDB, because #845 gave the key the account already; the ARN
// assertions below pin both halves regardless, so a regression in either direction fails here.
func TestTaggingKeyScope_OneAccountHoldsOneTableNameInTwoRegions(t *testing.T) {
	ts := scanScopeServer(t)
	const name = "key-scope-two-region-table"

	eastARN := keyScopeTable(t, ts, taggingTestAccount, scanScopeEast, name)
	westARN := keyScopeTable(t, ts, taggingTestAccount, scanScopeWest, name)

	assert.Equal(t, "arn:aws:dynamodb:"+scanScopeEast+":"+taggingTestAccount+":table/"+name, eastARN)
	assert.Equal(t, "arn:aws:dynamodb:"+scanScopeWest+":"+taggingTestAccount+":table/"+name, westARN)

	assert.Equal(t, eastARN, keyScopeDescribeTable(t, ts, taggingTestAccount, scanScopeEast, name),
		"us-east-1 describes the us-east-1 table")
	assert.Equal(t, westARN, keyScopeDescribeTable(t, ts, taggingTestAccount, scanScopeWest, name),
		"us-west-2 describes the us-west-2 table")
}

// TestTaggingKeyScope_TwoAccountsHoldOneTableName is the account half for DynamoDB, asserted here
// rather than assumed from #845.
//
// #845 gave the key the account and this states the consequence, so the pair of tests covers both
// segments of the DynamoDB key and a later change that drops either one fails a test that says which.
func TestTaggingKeyScope_TwoAccountsHoldOneTableName(t *testing.T) {
	ts := scanScopeServer(t)
	const name = "key-scope-shared-table"

	ownARN := keyScopeTable(t, ts, taggingTestAccount, scanScopeEast, name)
	foreignARN := keyScopeTable(t, ts, taggingForeignAccount, scanScopeEast, name)
	require.NotEqual(t, ownARN, foreignARN, "the two tables report different ARNs")

	assert.Equal(t, ownARN, keyScopeDescribeTable(t, ts, taggingTestAccount, scanScopeEast, name),
		"the caller describes its own table")
	assert.Equal(t, foreignARN,
		keyScopeDescribeTable(t, ts, taggingForeignAccount, scanScopeEast, name),
		"the other account describes its own table")
}

// TestTaggingKeyScope_TagResourcesDoesNotReachAnotherAccountsFunction is the tagging resolver's half.
//
// TaggingPlugin.resolveARN builds the state key from the ARN it was handed, and the Lambda arm used
// only the name — so a TagResources naming another account's function wrote to whichever single record
// that name resolved to, which was the caller's own. The tag landed on a resource the caller did own
// but had not named: a write aimed at one resource applied to another.
//
// The assertion is on the caller's record staying untagged, and on the named record being the one that
// changed. Both directions matter: the first is the defect, and the second is what distinguishes the
// fix from a resolver that simply refused the ARN.
func TestTaggingKeyScope_TagResourcesDoesNotReachAnotherAccountsFunction(t *testing.T) {
	ts := scanScopeServer(t)
	const name = "key-scope-tag-target"

	ownARN := keyScopeFunction(t, ts, taggingTestAccount, scanScopeEast, name, "the caller's")
	foreignARN := keyScopeFunction(t, ts, taggingForeignAccount, scanScopeEast, name, "the other's")

	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts, signedRequestTarget{
		host:        "tagging." + scanScopeEast + ".amazonaws.com",
		target:      "ResourceGroupsTaggingAPI_20170126",
		signingName: "tagging",
	}, taggingTestAccount, "TagResources", map[string]any{
		"ResourceARNList": []string{foreignARN},
		"Tags":            map[string]string{"Owner": "the-other-account"},
	}), &out)
	require.Emptyf(t, errCode, "TagResources on %s", foreignARN)
	require.Equal(t, http.StatusOK, status, "TagResources on %s", foreignARN)
	require.Empty(t, out.FailedResourcesMap, "TagResources on %s reports no failure", foreignARN)

	assert.Empty(t, keyScopeFunctionTags(t, ts, taggingTestAccount, scanScopeEast, ownARN),
		"the caller's own same-named function is not tagged by an ARN naming another account's")
	assert.Equal(t, map[string]string{"Owner": "the-other-account"},
		keyScopeFunctionTags(t, ts, taggingForeignAccount, scanScopeEast, foreignARN),
		"the function the ARN names is the one that was tagged")
}
