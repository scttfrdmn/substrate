package emulator_test

import (
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// The tagging API's ARN resolver used to derive a state key with strings.TrimPrefix without
// first checking the prefix was there, and TrimPrefix returns its input unchanged when the
// prefix does not match. So an ARN naming a *different resource type* under the same service
// produced a well-formed key for the wrong kind of resource rather than an error: a Step
// Functions activity ARN resolved to a state-machine key and tagged a same-named state
// machine, a Lambda layer ARN resolved to a function key, and a DynamoDB stream ARN resolved
// under its table. Eight of sixteen arms did this (#845, and the resolver half of #835).
//
// Every case here goes over the wire against a real server, and every success is read back
// through the *owning service's own* tag call rather than out of the state store. That is
// #765's standing rule, and it is the only thing that can tell a tag that landed from a tag
// that was reported as landed — which is the whole of the defect class this file covers. It
// is also why none of these tests reach for the putTest* helpers in tagging_plugin_test.go:
// a helper that writes state directly cannot prove cross-readability, because it writes the
// key the assertion would have to trust.

// taggingTestAccount is the account every request here is made as, and the account every
// ARN below names except the deliberately foreign one.
const taggingTestAccount = "123456789012"

// taggingForeignAccount is an account the caller is not. It exists so a cross-account ARN can
// be built for a resource the caller does own the *name* of — the shape of the defect, which a
// differently-named resource would not reproduce.
const taggingForeignAccount = "999999999999"

// arnGuardServer starts a server callable as [taggingTestAccount].
//
// WithAccounts rather than a bare StartTestServer because [signedRequest] needs a registered
// credential to sign with, and rather than StartTestServerWithAccounts because none of these
// tests is about signatures — verification stays off, which is the default (#630).
func arnGuardServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// Wire details for the four services these tests reach. Each is the Host, X-Amz-Target prefix
// and SigV4 signing name a real SDK would send; the parser routes on the target first and
// takes the region off the Host (parser.go:198-228).
var (
	taggingTarget  = signedRequestTarget{host: "tagging.us-east-1.amazonaws.com", target: "ResourceGroupsTaggingAPI_20170126", signingName: "tagging"}
	dynamodbTarget = signedRequestTarget{host: "dynamodb.us-east-1.amazonaws.com", target: "DynamoDB_20120810", signingName: "dynamodb"}
	statesTarget   = signedRequestTarget{host: "states.us-east-1.amazonaws.com", target: "AWSStepFunctions", signingName: "states"}
	ecsTarget      = signedRequestTarget{host: "ecs.us-east-1.amazonaws.com", target: "AmazonEC2ContainerServiceV20141113", signingName: "ecs"}
)

// taggingFailure is one entry of a TagResources or UntagResources FailedResourcesMap.
type taggingFailure struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
	StatusCode   int    `json:"StatusCode"`
}

// tagResourcesFailures posts op ("TagResources" or "UntagResources") for a single ARN and
// returns the FailedResourcesMap.
//
// Both operations are driven through one helper on purpose: they resolve ARNs through the same
// function and map failures through the same pair of functions, and asserting only one of them
// is how the two came to disagree in the first place. A removal aimed at the wrong resource is
// the more damaging direction, so it is never the one left untested.
func tagResourcesFailures(t *testing.T, ts *emulator.TestServer, op, arn string) map[string]taggingFailure {
	t.Helper()

	body := map[string]any{"ResourceARNList": []string{arn}}
	if op == "TagResources" {
		body["Tags"] = map[string]string{"env": "test"}
	} else {
		body["TagKeys"] = []string{"env"}
	}

	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts, taggingTarget, taggingTestAccount, op, body), &out)
	if errCode != "" {
		t.Fatalf("%s %s: unexpected top-level refusal %s (status %d)", op, arn, errCode, status)
	}
	if status != 200 {
		t.Fatalf("%s %s: status %d, want 200", op, arn, status)
	}
	return out.FailedResourcesMap
}

// awsErrorCode reduces a "__type" member to its bare shape name, since a JSON-RPC error may
// carry an absolute shape ID ("com.amazonaws.ecs#ResourceNotFoundException") rather than the
// code alone, and which of the two a service sends is not what any test here is about.
func awsErrorCode(t string) string {
	if _, shape, found := strings.Cut(t, "#"); found {
		return shape
	}
	return t
}

// TestTaggingResolveARN_AWrongTypeARNIsRefusedRatherThanMisKeyed is the core assertion of
// #845: an ARN whose resource type an arm does not handle must fail, not resolve to a
// same-service resource of a different type.
//
// No resource is created for any case, and that is deliberate rather than a shortcut — a
// refusal happens in the resolver, before any state lookup, so a case that needed a resource
// would be asserting the merge instead. The two arms that resolve correctly are asserted from
// the positive side in TestTaggingResolveARN_TheRightTypeStillResolves, which is what makes
// the pair meaningful: a resolver that refused everything would pass this test alone.
func TestTaggingResolveARN_AWrongTypeARNIsRefusedRatherThanMisKeyed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		arn  string
		why  string
	}{{
		name: "lambda layer",
		arn:  "arn:aws:lambda:us-east-1:123456789012:layer:my-layer",
		why:  `TrimPrefix("function:") left the whole resource, keying function:layer:my-layer`,
	}, {
		name: "lambda layer version",
		arn:  "arn:aws:lambda:us-east-1:123456789012:layer:my-layer:3",
		why:  "a layer version is not a function either",
	}, {
		name: "lambda function version",
		arn:  "arn:aws:lambda:us-east-1:123456789012:function:my-fn:3",
		why:  "a version and an alias ARN are lexically identical, so a qualified ARN is refused rather than guessed at",
	}, {
		name: "lambda function alias",
		arn:  "arn:aws:lambda:us-east-1:123456789012:function:my-fn:live",
		why:  "same shape as a version; AWS does not resolve which it is",
	}, {
		name: "dynamodb stream",
		arn:  "arn:aws:dynamodb:us-east-1:123456789012:table/orders/stream/2026-01-01T00:00:00.000",
		why:  "a stream nests under the table prefix, so HasPrefix alone is not sufficient",
	}, {
		name: "dynamodb index",
		arn:  "arn:aws:dynamodb:us-east-1:123456789012:table/orders/index/by-date",
		why:  "an index nests under the table prefix too",
	}, {
		name: "apigateway http api",
		arn:  "arn:aws:apigateway:us-east-1::/apis/abc123",
		why:  `TrimPrefix("/restapis/") left "/apis/abc123", keying api:…//apis/abc123`,
	}, {
		name: "step functions activity",
		arn:  "arn:aws:states:us-east-1:123456789012:activity:my-activity",
		why:  "the two types differ only by the literal segment, so an activity tagged a same-named state machine",
	}, {
		name: "kinesis consumer",
		arn:  "arn:aws:kinesis:us-east-1:123456789012:stream/orders/consumer/reader:1700000000",
		why:  "a consumer nests under the stream prefix",
	}, {
		name: "s3 object",
		arn:  "arn:aws:s3:::my-bucket/key.txt",
		why:  "the arm stripped an empty prefix, which is a no-op, so an object keyed as a bucket",
	}, {
		name: "ecr image",
		arn:  "arn:aws:ecr:us-east-1:123456789012:image/my-repo",
		why:  "not a repository",
	}, {
		name: "cognito identity pool",
		arn:  "arn:aws:cognito-idp:us-east-1:123456789012:identitypool/us-east-1:abc",
		why:  "not a user pool",
	}, {
		name: "ecs capacity provider",
		arn:  "arn:aws:ecs:us-east-1:123456789012:capacity-provider/my-cp",
		why:  "AWS lists it as taggable but substrate stores no such record, so there is nothing to read a tag back from",
	}, {
		name: "ecs short service arn",
		arn:  "arn:aws:ecs:us-east-1:123456789012:service/my-service",
		why:  "AWS's documented short ARN, whose refusal ECS's own page states outright",
	}, {
		name: "service with no arm at all",
		arn:  "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
		why:  "#835's six armless services take the same answer as a wrong type within a service",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			for _, op := range []string{"TagResources", "UntagResources"} {
				failures := tagResourcesFailures(t, ts, op, tc.arn)
				got, ok := failures[tc.arn]
				if !ok {
					t.Fatalf("%s: %s reported no failure for %s — %s", op, tc.arn, tc.arn, tc.why)
				}
				if got.ErrorCode != "InternalServiceException" {
					t.Errorf("%s: ErrorCode = %q, want InternalServiceException", op, got.ErrorCode)
				}
				if got.StatusCode != 500 {
					t.Errorf("%s: StatusCode = %d, want 500", op, got.StatusCode)
				}
			}
		})
	}
}

// TestTaggingResolveARN_AMalformedARNIsInvalidParameter asserts the other half of the split
// #845 asks for: a well-formed ARN naming a type substrate cannot key answers
// InternalServiceException, and a string that is not an ARN answers
// InvalidParameterException/400.
//
// AWS can be read either way here — FailureInfo documents InternalServiceException for "the
// resource type in the request is not supported", while the same page's InvalidParameterException
// bullets say "the target ID is invalid, unsupported, or doesn't exist" — so substrate splits
// them on whether the ARN parses, because that is the only distinction a caller can act on
// differently. The test pins the split rather than claiming AWS specifies it.
func TestTaggingResolveARN_AMalformedARNIsInvalidParameter(t *testing.T) {
	t.Parallel()

	for _, arn := range []string{
		"not-an-arn",
		"",
		"arn:aws:dynamodb:us-east-1:123456789012",
		"arn:aws:s3",
		"AWS:aws:s3:::my-bucket",
	} {
		t.Run(arn, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			failures := tagResourcesFailures(t, ts, "TagResources", arn)
			got, ok := failures[arn]
			if !ok {
				t.Fatalf("%q reported no failure", arn)
			}
			if got.ErrorCode != "InvalidParameterException" {
				t.Errorf("ErrorCode = %q, want InvalidParameterException", got.ErrorCode)
			}
			if got.StatusCode != 400 {
				t.Errorf("StatusCode = %d, want 400", got.StatusCode)
			}
		})
	}
}

// TestTaggingResolveARN_ARefusalNeverPublishesAStateKey asserts the message a caller sees for
// an unsupported type is AWS's own sentence and not err.Error().
//
// The mapping used to pass the resolver's error text straight through, so a FailedResourcesMap
// entry disclosed substrate's internal state-key layout in an API response. A message is prose
// and normally not worth asserting on; this one is asserted because *what it must not contain*
// is the defect.
func TestTaggingResolveARN_ARefusalNeverPublishesAStateKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	// A well-formed table ARN for a table that does not exist: it resolves, so the failure
	// comes from the merge, which is the arm whose error text names the state key.
	const missing = "arn:aws:dynamodb:us-east-1:123456789012:table/no-such-table"
	failures := tagResourcesFailures(t, ts, "TagResources", missing)
	got, ok := failures[missing]
	if !ok {
		t.Fatalf("tagging a nonexistent table reported no failure")
	}
	for _, leak := range []string{"table:", "statemachine:", "dynamodb/", "no-such-table"} {
		if strings.Contains(got.ErrorMessage, leak) {
			t.Errorf("ErrorMessage %q contains internal detail %q", got.ErrorMessage, leak)
		}
	}

	// And the unsupported-type arm, whose message is AWS's published sentence verbatim.
	const activity = "arn:aws:states:us-east-1:123456789012:activity:my-activity"
	unsupported := tagResourcesFailures(t, ts, "TagResources", activity)[activity]
	if strings.Contains(unsupported.ErrorMessage, "statemachine:") {
		t.Errorf("ErrorMessage %q contains a state key", unsupported.ErrorMessage)
	}
}

// TestTaggingResolveARN_TheRightTypeStillResolves is the positive side of the guards. Without
// it, a resolver that refused every ARN would pass every test above.
//
// Each tag is read back through the owning service's own tag call, never out of the state
// store (#765) — which for the Step Functions case is also what asserts the case-sensitive
// stateMachine: guard from the direction that matters: it still resolves.
func TestTaggingResolveARN_TheRightTypeStillResolves(t *testing.T) {
	t.Parallel()

	t.Run("dynamodb table", func(t *testing.T) {
		t.Parallel()
		ts := arnGuardServer(t)
		createDynamoDBTable(t, ts, "orders")

		arn := "arn:aws:dynamodb:us-east-1:" + taggingTestAccount + ":table/orders"
		if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
			t.Fatalf("TagResources %s: %+v", arn, failures)
		}
		if got := dynamoDBTags(t, ts, arn)["env"]; got != "test" {
			t.Errorf("ListTagsOfResource env = %q, want test", got)
		}
	})

	t.Run("step functions state machine", func(t *testing.T) {
		t.Parallel()
		ts := arnGuardServer(t)
		arn := createStateMachine(t, ts, "MyStateMachine")

		if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
			t.Fatalf("TagResources %s: %+v", arn, failures)
		}
		var out struct {
			Tags map[string]string `json:"tags"`
		}
		resp := signedRequest(t, ts, statesTarget, taggingTestAccount, "ListTagsForResource",
			map[string]any{"resourceArn": arn})
		if status, errCode := decodeAWSResponse(t, resp, &out); errCode != "" || status != 200 {
			t.Fatalf("ListTagsForResource: status %d, %s", status, errCode)
		}
		if out.Tags["env"] != "test" {
			t.Errorf("ListTagsForResource tags = %v, want env=test", out.Tags)
		}
	})
}

// TestTaggingResolveARN_AForeignAccountARNDoesNotTagTheCallersTable is the #826 recurrence
// #845 found: the DynamoDB arm built its key from the *caller's* account rather than from the
// ARN's, so an ARN naming another account's table tagged the caller's own same-named table and
// UntagResources stripped tags from it.
//
// The resolver no longer takes a *RequestContext at all, so the invariant is structural rather
// than advisory. This test asserts the consequence a consumer can observe: the caller's table
// is untouched, read back through DynamoDB's own ListTagsOfResource.
//
// AWS publishes no cross-account behavior to compare against — TagResources says only that
// "you can only tag resources that are located in the specified AWS Region for the AWS
// account", and an explicit refusal is documented for a partition mismatch but not for an
// account mismatch. So the refusal is substrate's reading; what is not a reading is that the
// caller's own resource must not be the one that changes.
func TestTaggingResolveARN_AForeignAccountARNDoesNotTagTheCallersTable(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	createDynamoDBTable(t, ts, "orders")
	own := "arn:aws:dynamodb:us-east-1:" + taggingTestAccount + ":table/orders"
	foreign := "arn:aws:dynamodb:us-east-1:" + taggingForeignAccount + ":table/orders"

	// Seed a tag through the caller's own ARN, so a stripped tag is observable as well as an
	// added one — UntagResources aimed at the wrong resource is the more damaging direction.
	if failures := tagResourcesFailures(t, ts, "TagResources", own); len(failures) != 0 {
		t.Fatalf("seeding TagResources: %+v", failures)
	}

	if failures := tagResourcesFailures(t, ts, "TagResources", foreign); len(failures[foreign].ErrorCode) == 0 {
		t.Errorf("TagResources against %s reported no failure; a foreign-account ARN must resolve that account's table or none", foreign)
	}
	if got := dynamoDBTags(t, ts, own)["env"]; got != "test" {
		t.Errorf("after a foreign-account TagResources, own table env = %q, want it untouched at test", got)
	}

	if failures := tagResourcesFailures(t, ts, "UntagResources", foreign); len(failures[foreign].ErrorCode) == 0 {
		t.Errorf("UntagResources against %s reported no failure", foreign)
	}
	if got := dynamoDBTags(t, ts, own)["env"]; got != "test" {
		t.Errorf("after a foreign-account UntagResources, own table env = %q, want it still test", got)
	}
}

// TestTaggingECS_AWriteThatKeysNothingIsRefused covers the silent success #845 folded in:
// ECSPlugin.resourceStateKey returned ("", "") for an ARN it could not key and
// applyTagsToResource then reported success, so TagResource against a task definition answered
// the exact response AWS documents for a *successful* tag — "an HTTP 200 response with an empty
// HTTP body" — having written nothing. UntagResource did the same, while ListTagsForResource
// refused the same ARN: the two sides of one tag disagreed and the write side was the one that
// lied.
//
// All three operations are asserted together, because "the read side already refused correctly"
// is exactly the state that let the write side go unnoticed.
func TestTaggingECS_AWriteThatKeysNothingIsRefused(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		arn      string
		wantCode string
	}{{
		name:     "capacity provider",
		arn:      "arn:aws:ecs:us-east-1:123456789012:capacity-provider/my-cp",
		wantCode: "InvalidParameterException",
	}, {
		name:     "short service arn",
		arn:      "arn:aws:ecs:us-east-1:123456789012:service/my-service",
		wantCode: "InvalidParameterException",
	}, {
		name:     "task definition with a non-numeric revision",
		arn:      "arn:aws:ecs:us-east-1:123456789012:task-definition/web:latest",
		wantCode: "InvalidParameterException",
	}, {
		name:     "cluster that does not exist",
		arn:      "arn:aws:ecs:us-east-1:123456789012:cluster/no-such-cluster",
		wantCode: "ResourceNotFoundException",
	}, {
		name:     "task definition that does not exist",
		arn:      "arn:aws:ecs:us-east-1:123456789012:task-definition/no-such-family:1",
		wantCode: "ResourceNotFoundException",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			ops := []struct {
				op   string
				body map[string]any
			}{
				{"TagResource", map[string]any{"resourceArn": tc.arn, "tags": []map[string]string{{"key": "env", "value": "test"}}}},
				{"UntagResource", map[string]any{"resourceArn": tc.arn, "tagKeys": []string{"env"}}},
				{"ListTagsForResource", map[string]any{"resourceArn": tc.arn}},
			}
			for _, o := range ops {
				resp := signedRequest(t, ts, ecsTarget, taggingTestAccount, o.op, o.body)
				status, errCode := decodeAWSResponse(t, resp, nil)
				if awsErrorCode(errCode) != tc.wantCode {
					t.Errorf("%s: error code %q, want %s", o.op, errCode, tc.wantCode)
				}
				// Every ECS exception except ServerException is documented at 400. The read
				// side answered 404 for a missing resource, which is the shape of a REST
				// service rather than a JSON-protocol one and is on no ECS page.
				if status != 400 {
					t.Errorf("%s: status %d, want 400", o.op, status)
				}
			}
		})
	}
}

// TestTaggingECS_ATaskDefinitionTaggedThroughTheTaggingAPIReadsBackThroughECS is the
// cross-readability assertion for the types the shared key builder newly reaches.
//
// The tagging API's ECS arm built the cluster key by hand, so it reached a cluster only; it now
// delegates to ecsTagStateKey, the same function ECS's own TagResource keys through. That is
// what makes the two unable to drift, and it is what closes the ECS service and task-definition
// rows of #835 on the write side. AWS's own API_TagResource page settles that a task definition
// is taggable, listing "capacity providers, tasks, services, task definitions, clusters, and
// container instances".
func TestTaggingECS_ATaskDefinitionTaggedThroughTheTaggingAPIReadsBackThroughECS(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	arn := registerECSTaskDefinition(t, ts, "web")
	if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
		t.Fatalf("TagResources %s: %+v", arn, failures)
	}
	if got := ecsTags(t, ts, arn)["env"]; got != "test" {
		t.Errorf("ECS ListTagsForResource env = %q, want test", got)
	}

	// And the removal side, since the two used to disagree about which resource an ARN names.
	if failures := tagResourcesFailures(t, ts, "UntagResources", arn); len(failures) != 0 {
		t.Fatalf("UntagResources %s: %+v", arn, failures)
	}
	if _, still := ecsTags(t, ts, arn)["env"]; still {
		t.Errorf("UntagResources left env in place")
	}
}

// TestTaggingECS_TagsAreStoredInKeyOrder covers a determinism defect no issue covered: all four
// slice-returning merge helpers built their result by ranging over a Go map, so two identical
// runs stored one resource's tags in different orders.
//
// That is not a cosmetic problem in an event-sourced emulator. It made ListTagsForResource,
// DescribeTags and GetResources report a different order each run for the same state, and it
// made a hash over the record differ when nothing about the record had changed — so a replay
// could not compare state it did not change and find it equal. ECS is the case asserted because
// its tags travel as an ordered list on the wire; a service storing map[string]string cannot
// show the difference.
func TestTaggingECS_TagsAreStoredInKeyOrder(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	arn := createECSCluster(t, ts, "app-cluster")
	body := map[string]any{
		"ResourceARNList": []string{arn},
		"Tags":            map[string]string{"zebra": "3", "apple": "1", "mango": "2"},
	}
	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	if status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, taggingTarget, taggingTestAccount, "TagResources", body), &out); errCode != "" || status != 200 {
		t.Fatalf("TagResources: status %d, %s", status, errCode)
	}
	if len(out.FailedResourcesMap) != 0 {
		t.Fatalf("TagResources: %+v", out.FailedResourcesMap)
	}

	got := ecsTagKeys(t, ts, arn)
	want := []string{"apple", "mango", "zebra"}
	if len(got) != len(want) {
		t.Fatalf("ListTagsForResource keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ListTagsForResource keys = %v, want %v", got, want)
		}
	}
}

// ----- resource creation, all over the wire ---------------------------------

// createDynamoDBTable creates a table with one tag and returns nothing: the ARN is built by the
// caller, because that is what a consumer holding only an ARN does and it is the input the
// resolver is under test for.
func createDynamoDBTable(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	body := map[string]any{
		"TableName":            name,
		"AttributeDefinitions": []map[string]string{{"AttributeName": "id", "AttributeType": "S"}},
		"KeySchema":            []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
		"BillingMode":          "PAY_PER_REQUEST",
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, dynamodbTarget, taggingTestAccount, "CreateTable", body), nil)
	if errCode != "" || status != 200 {
		t.Fatalf("CreateTable %s: status %d, %s", name, status, errCode)
	}
}

// dynamoDBTags reads a table's tags through DynamoDB's own ListTagsOfResource.
func dynamoDBTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, dynamodbTarget, taggingTestAccount, "ListTagsOfResource",
			map[string]any{"ResourceArn": arn}), &out)
	if errCode != "" || status != 200 {
		t.Fatalf("ListTagsOfResource %s: status %d, %s", arn, status, errCode)
	}
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// createStateMachine creates a state machine and returns the ARN the service itself minted,
// rather than one built here — so the test cannot pass by agreeing with a key substrate
// happens to build.
func createStateMachine(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	var out struct {
		StateMachineARN string `json:"stateMachineArn"`
	}
	body := map[string]any{
		"name":       name,
		"definition": `{"Comment":"test","StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`,
		"roleArn":    "arn:aws:iam::" + taggingTestAccount + ":role/sfn-role",
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, statesTarget, taggingTestAccount, "CreateStateMachine", body), &out)
	if errCode != "" || status != 200 {
		t.Fatalf("CreateStateMachine %s: status %d, %s", name, status, errCode)
	}
	if out.StateMachineARN == "" {
		t.Fatalf("CreateStateMachine %s returned no stateMachineArn", name)
	}
	return out.StateMachineARN
}

// createECSCluster creates a cluster and returns the ARN ECS minted for it.
func createECSCluster(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	var out struct {
		Cluster struct {
			ClusterARN string `json:"clusterArn"`
		} `json:"cluster"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, ecsTarget, taggingTestAccount, "CreateCluster",
			map[string]any{"clusterName": name}), &out)
	if errCode != "" || status != 200 {
		t.Fatalf("CreateCluster %s: status %d, %s", name, status, errCode)
	}
	if out.Cluster.ClusterARN == "" {
		t.Fatalf("CreateCluster %s returned no clusterArn", name)
	}
	return out.Cluster.ClusterARN
}

// registerECSTaskDefinition registers a revision and returns the ARN ECS minted, revision
// included — the revision is part of the identifier, since a family alone names no single
// record once a second RegisterTaskDefinition has run.
func registerECSTaskDefinition(t *testing.T, ts *emulator.TestServer, family string) string {
	t.Helper()
	var out struct {
		TaskDefinition struct {
			TaskDefinitionARN string `json:"taskDefinitionArn"`
		} `json:"taskDefinition"`
	}
	body := map[string]any{
		"family":               family,
		"containerDefinitions": []map[string]any{{"name": "nginx", "image": "nginx:latest"}},
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, ecsTarget, taggingTestAccount, "RegisterTaskDefinition", body), &out)
	if errCode != "" || status != 200 {
		t.Fatalf("RegisterTaskDefinition %s: status %d, %s", family, status, errCode)
	}
	if out.TaskDefinition.TaskDefinitionARN == "" {
		t.Fatalf("RegisterTaskDefinition %s returned no taskDefinitionArn", family)
	}
	return out.TaskDefinition.TaskDefinitionARN
}

// ecsTagList reads a resource's tags through ECS's own ListTagsForResource, in the order the
// service reports them.
func ecsTagList(t *testing.T, ts *emulator.TestServer, arn string) []struct {
	Key   string `json:"key"`
	Value string `json:"value"`
} {
	t.Helper()
	var out struct {
		Tags []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"tags"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, ecsTarget, taggingTestAccount, "ListTagsForResource",
			map[string]any{"resourceArn": arn}), &out)
	if errCode != "" || status != 200 {
		t.Fatalf("ECS ListTagsForResource %s: status %d, %s", arn, status, errCode)
	}
	return out.Tags
}

// ecsTags is [ecsTagList] reduced to a map, for an assertion that does not care about order.
func ecsTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	tags := make(map[string]string)
	for _, tag := range ecsTagList(t, ts, arn) {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// ecsTagKeys is [ecsTagList] reduced to the keys in the order reported, which is the whole
// point of the determinism assertion.
func ecsTagKeys(t *testing.T, ts *emulator.TestServer, arn string) []string {
	t.Helper()
	list := ecsTagList(t, ts, arn)
	keys := make([]string, len(list))
	for i, tag := range list {
		keys[i] = tag.Key
	}
	return keys
}
