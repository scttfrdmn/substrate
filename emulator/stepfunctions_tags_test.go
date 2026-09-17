package emulator_test

// Step Functions tagging, over the wire (#910, part of #835).
//
// Every resource is created through its own operation and every ARN is taken out of the create
// response rather than composed in the test. A composed ARN would let this file agree with a
// resolver that disagreed with what CreateStateMachine reports, which is the divergence being
// tested. No putTest* helper is used, per #765's rule that a helper writing state directly
// cannot prove a value is readable through the owning service's own call.

import (
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

const sfnTagDefinition = `{"Comment":"test","StartAt":"Start","States":{"Start":{"Type":"Pass","End":true}}}`

func sfnTagServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// createActivity creates one activity and returns the ARN CreateActivity reports for it.
func createActivity(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	var out struct {
		ActivityARN string `json:"activityArn"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, statesTarget, taggingTestAccount, "CreateActivity",
			map[string]any{"name": name}), &out)
	if errCode != "" || status != http.StatusOK {
		t.Fatalf("CreateActivity %s: status %d, %s", name, status, errCode)
	}
	if out.ActivityARN == "" {
		t.Fatalf("CreateActivity %s returned no activityArn", name)
	}
	return out.ActivityARN
}

// sfnTagCall posts one Step Functions tagging operation and returns (status, errorCode).
func sfnTagCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, string) {
	t.Helper()
	return decodeAWSResponse(t,
		signedRequest(t, ts, statesTarget, taggingTestAccount, op, body), nil)
}

// sfnTagOK posts one Step Functions tagging operation and fails the test unless it succeeds.
func sfnTagOK(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) {
	t.Helper()
	if status, errCode := sfnTagCall(t, ts, op, body); errCode != "" || status != http.StatusOK {
		t.Fatalf("%s %v: status %d, %s", op, body["resourceArn"], status, errCode)
	}
}

// sfnTagPairs reads one resource's tags through Step Functions' own ListTagsForResource and
// returns them in the order reported, so a test can assert the order as well as the contents.
func sfnTagPairs(t *testing.T, ts *emulator.TestServer, arn string) [][2]string {
	t.Helper()
	var out struct {
		Tags []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"tags"`
	}
	resp := signedRequest(t, ts, statesTarget, taggingTestAccount, "ListTagsForResource",
		map[string]any{"resourceArn": arn})
	status, errCode := decodeAWSResponse(t, resp, &out)
	if errCode != "" || status != http.StatusOK {
		t.Fatalf("ListTagsForResource %s: status %d, %s", arn, status, errCode)
	}
	pairs := make([][2]string, 0, len(out.Tags))
	for _, tag := range out.Tags {
		pairs = append(pairs, [2]string{tag.Key, tag.Value})
	}
	return pairs
}

// sfnTags reads one resource's tags as a map.
func sfnTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	tags := map[string]string{}
	for _, pair := range sfnTagPairs(t, ts, arn) {
		tags[pair[0]] = pair[1]
	}
	return tags
}

// sfnTagListBody renders AWS's array-of-Tag-objects request member.
func sfnTagListBody(tags map[string]string) []map[string]string {
	out := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		out = append(out, map[string]string{"key": k, "value": v})
	}
	return out
}

// sfnStateMachineView holds the DescribeStateMachine members that are not tags — the ones a merge
// that round-tripped the record through the wrong struct would drop.
type sfnStateMachineView struct {
	Definition string `json:"definition"`
	RoleArn    string `json:"roleArn"`
	Type       string `json:"type"`
	Status     string `json:"status"`
}

func describeSFNStateMachine(t *testing.T, ts *emulator.TestServer, arn string) sfnStateMachineView {
	t.Helper()
	var out sfnStateMachineView
	resp := signedRequest(t, ts, statesTarget, taggingTestAccount, "DescribeStateMachine",
		map[string]any{"stateMachineArn": arn})
	if status, errCode := decodeAWSResponse(t, resp, &out); errCode != "" || status != http.StatusOK {
		t.Fatalf("DescribeStateMachine %s: status %d, %s", arn, status, errCode)
	}
	return out
}

// sfnActivityView holds the DescribeActivity members that are not tags.
type sfnActivityView struct {
	ActivityArn  string  `json:"activityArn"`
	Name         string  `json:"name"`
	CreationDate float64 `json:"creationDate"`
}

func describeSFNActivity(t *testing.T, ts *emulator.TestServer, arn string) sfnActivityView {
	t.Helper()
	var out sfnActivityView
	resp := signedRequest(t, ts, statesTarget, taggingTestAccount, "DescribeActivity",
		map[string]any{"activityArn": arn})
	if status, errCode := decodeAWSResponse(t, resp, &out); errCode != "" || status != http.StatusOK {
		t.Fatalf("DescribeActivity %s: status %d, %s", arn, status, errCode)
	}
	return out
}

// TestSFNTags_EveryARNStepFunctionsMintsIsReadableThroughItsOwnTagCalls is #765's rule applied to
// the two ARNs Step Functions reports: a value substrate hands a caller has to be usable against
// the API that reported it. Both ARNs come out of their own create response.
func TestSFNTags_EveryARNStepFunctionsMintsIsReadableThroughItsOwnTagCalls(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)

	for _, tc := range []struct {
		kind string
		arn  string
	}{
		{"state machine", createStateMachine(t, ts, "orders")},
		{"activity", createActivity(t, ts, "fulfill")},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			sfnTagOK(t, ts, "TagResource", map[string]any{
				"resourceArn": tc.arn,
				"tags":        sfnTagListBody(map[string]string{"env": "test", "owner": "team"}),
			})
			if got := sfnTags(t, ts, tc.arn); got["env"] != "test" || got["owner"] != "team" {
				t.Errorf("%s: tags = %v, want env=test owner=team", tc.arn, got)
			}

			// Removal is the more damaging direction — an UntagResource aimed at a resource the
			// resolver could not key stripped tags from whatever it reached instead — so it is
			// never the untested half.
			sfnTagOK(t, ts, "UntagResource", map[string]any{
				"resourceArn": tc.arn,
				"tagKeys":     []string{"owner"},
			})
			got := sfnTags(t, ts, tc.arn)
			if got["env"] != "test" {
				t.Errorf("%s: env tag lost by UntagResource: %v", tc.arn, got)
			}
			if _, ok := got["owner"]; ok {
				t.Errorf("%s: owner tag survived UntagResource: %v", tc.arn, got)
			}
		})
	}
}

// TestSFNTags_ATaggedActivityKeepsItsOwnMembers is the truncation defect the states merge arm
// carried: it decoded StateMachineState whatever the key named, so an activity round-tripped
// through a state machine's struct. Asserted on the members that are *not* tags, because the tag
// itself was written correctly — which is exactly why a test asserting on the tag cannot see it.
func TestSFNTags_ATaggedActivityKeepsItsOwnMembers(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	arn := createActivity(t, ts, "fulfill")

	before := describeSFNActivity(t, ts, arn)
	if before.ActivityArn == "" || before.Name != "fulfill" || before.CreationDate == 0 {
		t.Fatalf("CreateActivity stored an incomplete record, so this test would pass vacuously: %+v", before)
	}

	if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
		t.Fatalf("TagResources %s: %+v", arn, failures)
	}
	if got := sfnTags(t, ts, arn)["env"]; got != "test" {
		t.Fatalf("TagResources wrote no readable tag: %v", sfnTags(t, ts, arn))
	}

	after := describeSFNActivity(t, ts, arn)
	if after != before {
		t.Errorf("tagging truncated the activity record:\n before %+v\n after  %+v", before, after)
	}
}

// TestSFNTags_ATaggedStateMachineKeepsItsOwnMembers is the same assertion for a state machine,
// which the old arm decoded correctly — so this is the non-regression direction.
func TestSFNTags_ATaggedStateMachineKeepsItsOwnMembers(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	arn := createStateMachine(t, ts, "orders")

	before := describeSFNStateMachine(t, ts, arn)
	if before.Definition == "" || before.RoleArn == "" || before.Type == "" || before.Status == "" {
		t.Fatalf("CreateStateMachine stored an incomplete record: %+v", before)
	}

	if failures := tagResourcesFailures(t, ts, "TagResources", arn); len(failures) != 0 {
		t.Fatalf("TagResources %s: %+v", arn, failures)
	}
	if after := describeSFNStateMachine(t, ts, arn); after != before {
		t.Errorf("tagging truncated the state machine record:\n before %+v\n after  %+v", before, after)
	}
}

// TestSFNTags_BothTypesAreReachableFromTheTaggingAPI is #835's criterion per row: a tag written
// through the Resource Groups Tagging API is readable through the owning service's own call, and
// back again. The activity direction is the row this change adds.
func TestSFNTags_BothTypesAreReachableFromTheTaggingAPI(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)

	for _, tc := range []struct {
		kind string
		arn  string
	}{
		{"state machine", createStateMachine(t, ts, "orders")},
		{"activity", createActivity(t, ts, "fulfill")},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			// Tagging API writes, Step Functions reads.
			if failures := tagResourcesFailures(t, ts, "TagResources", tc.arn); len(failures) != 0 {
				t.Fatalf("TagResources %s: %+v", tc.arn, failures)
			}
			if got := sfnTags(t, ts, tc.arn)["env"]; got != "test" {
				t.Errorf("ListTagsForResource does not report the tag TagResources wrote: %v", sfnTags(t, ts, tc.arn))
			}

			// Step Functions writes, tagging API reads.
			sfnTagOK(t, ts, "TagResource", map[string]any{
				"resourceArn": tc.arn,
				"tags":        sfnTagListBody(map[string]string{"owner": "team"}),
			})
			if got := getResourcesTags(t, ts, tc.arn)["owner"]; got != "team" {
				t.Errorf("GetResources does not report the tag TagResource wrote: %v", getResourcesTags(t, ts, tc.arn))
			}

			// Tagging API removes, Step Functions confirms.
			if failures := tagResourcesFailures(t, ts, "UntagResources", tc.arn); len(failures) != 0 {
				t.Fatalf("UntagResources %s: %+v", tc.arn, failures)
			}
			if _, ok := sfnTags(t, ts, tc.arn)["env"]; ok {
				t.Errorf("UntagResources left the env tag in place: %v", sfnTags(t, ts, tc.arn))
			}
		})
	}
}

// TestSFNTags_GetResourcesReportsBothStepFunctionsTypes covers the other half of each #835 row: a
// resolver arm alone makes a resource taggable by name while leaving it invisible to a caller
// discovering resources.
func TestSFNTags_GetResourcesReportsBothStepFunctionsTypes(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	smARN := createStateMachine(t, ts, "orders")
	actARN := createActivity(t, ts, "fulfill")

	// Tagged through Step Functions' own TagResource, because GetResources reports what has been
	// tagged and a resource that never was is absent by rule (#938). Tagging here rather than at
	// creation also keeps this test's subject the scanner rather than CreateStateMachine's tags
	// member.
	for _, arn := range []string{smARN, actARN} {
		sfnTagOK(t, ts, "TagResource", map[string]any{
			"resourceArn": arn,
			"tags":        sfnTagListBody(map[string]string{"env": "test"}),
		})
	}

	arns := getResourcesARNs(t, ts, "states")
	for _, want := range []string{smARN, actARN} {
		found := false
		for _, got := range arns {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetResources(states) did not report %s; reported %v", want, arns)
		}
	}
}

// TestSFNTags_AMissingResourceAnswersResourceNotFoundAt400 pins the status as well as the code.
// All three operations publish ResourceNotFound at "HTTP Status Code: 400", which is unusual
// enough to be worth asserting: this path answered 404, which no Step Functions endpoint returns.
func TestSFNTags_AMissingResourceAnswersResourceNotFoundAt400(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	base := "arn:aws:states:us-east-1:" + taggingTestAccount + ":"

	for _, arn := range []string{base + "stateMachine:absent", base + "activity:absent"} {
		for _, call := range []struct {
			op   string
			body map[string]any
		}{
			{"TagResource", map[string]any{"resourceArn": arn, "tags": sfnTagListBody(map[string]string{"env": "test"})}},
			{"UntagResource", map[string]any{"resourceArn": arn, "tagKeys": []string{"env"}}},
			{"ListTagsForResource", map[string]any{"resourceArn": arn}},
		} {
			status, errCode := sfnTagCall(t, ts, call.op, call.body)
			if errCode != "ResourceNotFound" || status != http.StatusBadRequest {
				t.Errorf("%s %s: status %d code %q, want 400 ResourceNotFound", call.op, arn, status, errCode)
			}
		}
	}
}

// TestSFNTags_AnUnsupportedOrMalformedARNIsRefused covers the resource-type check the previous
// strings.Contains test could not make. An execution ARN is well-formed and names a resource
// these operations do not accept, so it is InvalidArn rather than ResourceNotFound.
func TestSFNTags_AnUnsupportedOrMalformedARNIsRefused(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	// Created so that the refusals below cannot be passing merely because nothing exists.
	createStateMachine(t, ts, "orders")
	base := "arn:aws:states:us-east-1:" + taggingTestAccount + ":"

	for _, tc := range []struct {
		name string
		arn  string
	}{
		{"an execution ARN names a resource these operations do not accept", base + "execution:orders:run-1"},
		{"a lowercase statemachine segment is not the type AWS publishes", base + "statemachine:orders"},
		{"a capitalized Activity segment is not the type AWS publishes", base + "Activity:fulfill"},
		{"a name carrying the type segment as a substring is not a type", base + "activity:x:stateMachine:orders"},
		{"a name carrying a slash addresses nothing", base + "stateMachine:orders/v2"},
		{"an empty name names no resource", base + "stateMachine:"},
		{"another service's ARN is not a states ARN", "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":orders"},
		{"a truncated ARN has no resource segment", "arn:aws:states:us-east-1:" + taggingTestAccount},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, errCode := sfnTagCall(t, ts, "ListTagsForResource", map[string]any{"resourceArn": tc.arn})
			if errCode != "InvalidArn" || status != http.StatusBadRequest {
				t.Errorf("%s: status %d code %q, want 400 InvalidArn", tc.arn, status, errCode)
			}
		})
	}
}

// TestSFNTags_AForeignAccountARNDoesNotTagTheCallersResource is the #826 recurrence: all three
// operations took the account from the caller's request context and the name from the ARN's last
// segment, so an ARN naming another account's same-named resource reached the caller's own.
// UntagResource is the damaging direction, since stripping a tag can turn an aws:ResourceTag Deny
// into an allow — so the assertion is on the caller's tags, not only on the response code.
func TestSFNTags_AForeignAccountARNDoesNotTagTheCallersResource(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)

	for _, tc := range []struct {
		kind    string
		arn     string
		foreign string
	}{
		{
			"state machine",
			createStateMachine(t, ts, "orders"),
			"arn:aws:states:us-east-1:" + taggingForeignAccount + ":stateMachine:orders",
		},
		{
			"activity",
			createActivity(t, ts, "fulfill"),
			"arn:aws:states:us-east-1:" + taggingForeignAccount + ":activity:fulfill",
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			sfnTagOK(t, ts, "TagResource", map[string]any{
				"resourceArn": tc.arn,
				"tags":        sfnTagListBody(map[string]string{"env": "test"}),
			})

			status, errCode := sfnTagCall(t, ts, "TagResource", map[string]any{
				"resourceArn": tc.foreign,
				"tags":        sfnTagListBody(map[string]string{"env": "stolen"}),
			})
			if errCode != "ResourceNotFound" || status != http.StatusBadRequest {
				t.Errorf("TagResource %s: status %d code %q, want 400 ResourceNotFound", tc.foreign, status, errCode)
			}

			status, errCode = sfnTagCall(t, ts, "UntagResource", map[string]any{
				"resourceArn": tc.foreign,
				"tagKeys":     []string{"env"},
			})
			if errCode != "ResourceNotFound" || status != http.StatusBadRequest {
				t.Errorf("UntagResource %s: status %d code %q, want 400 ResourceNotFound", tc.foreign, status, errCode)
			}

			if got := sfnTags(t, ts, tc.arn); got["env"] != "test" {
				t.Errorf("a foreign-account ARN reached the caller's own %s: tags = %v, want env=test",
					tc.kind, got)
			}
		})
	}
}

// TestSFNTags_ACrossRegionARNDoesNotTagTheCallersResource is the same defect in the Region
// segment, which the previous code ignored just as completely as the account one.
func TestSFNTags_ACrossRegionARNDoesNotTagTheCallersResource(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	arn := createStateMachine(t, ts, "orders")
	foreign := "arn:aws:states:eu-west-1:" + taggingTestAccount + ":stateMachine:orders"

	sfnTagOK(t, ts, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        sfnTagListBody(map[string]string{"env": "test"}),
	})

	status, errCode := sfnTagCall(t, ts, "UntagResource", map[string]any{
		"resourceArn": foreign,
		"tagKeys":     []string{"env"},
	})
	if errCode != "ResourceNotFound" || status != http.StatusBadRequest {
		t.Errorf("UntagResource %s: status %d code %q, want 400 ResourceNotFound", foreign, status, errCode)
	}
	if got := sfnTags(t, ts, arn); got["env"] != "test" {
		t.Errorf("a cross-Region ARN reached the caller's own state machine: tags = %v", got)
	}
}

// TestSFNTags_TagsUseTheArrayShapeAWSPublishes pins the wire shape. AWS's tags member is an array
// of Tag objects on TagResource's request and ListTagsForResource's response; substrate rendered
// and accepted an *object* at these two operations while CreateStateMachine and CreateActivity in
// the same plugin already took the array — so a tag set at create time could not be read back in
// a shape any SDK decodes, and TagResource could not be called by one at all.
func TestSFNTags_TagsUseTheArrayShapeAWSPublishes(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)

	// Set at create time, in AWS's array shape...
	var created struct {
		StateMachineARN string `json:"stateMachineArn"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, statesTarget, taggingTestAccount, "CreateStateMachine", map[string]any{
			"name":       "orders",
			"definition": sfnTagDefinition,
			"roleArn":    "arn:aws:iam::" + taggingTestAccount + ":role/sfn-role",
			"tags":       sfnTagListBody(map[string]string{"env": "test"}),
		}), &created)
	if errCode != "" || status != http.StatusOK {
		t.Fatalf("CreateStateMachine: status %d, %s", status, errCode)
	}

	// ...and read back through the tagging operation in the same shape.
	if got := sfnTagPairs(t, ts, created.StateMachineARN); len(got) != 1 || got[0] != [2]string{"env", "test"} {
		t.Errorf("ListTagsForResource = %v, want [[env test]]", got)
	}

	// An object where AWS publishes an array is refused rather than half-accepted.
	status, errCode = sfnTagCall(t, ts, "TagResource", map[string]any{
		"resourceArn": created.StateMachineARN,
		"tags":        map[string]string{"owner": "team"},
	})
	if status != http.StatusBadRequest || errCode == "" {
		t.Errorf("TagResource with an object tags member: status %d code %q, want a 400 refusal", status, errCode)
	}
	if _, ok := sfnTags(t, ts, created.StateMachineARN)["owner"]; ok {
		t.Error("a refused TagResource wrote a tag anyway")
	}
}

// TestSFNTags_TagsAreReportedInKeyOrder asserts the order twice, because one call cannot
// distinguish a sorted order from a Go map order that happened to come out sorted. AWS documents
// no order for the tags array; lexicographic is substrate's reading, justified by the replay
// promise (#862) — a member order that followed the map hash seed could not replay.
func TestSFNTags_TagsAreReportedInKeyOrder(t *testing.T) {
	t.Parallel()
	ts := sfnTagServer(t)
	arn := createActivity(t, ts, "fulfill")

	sfnTagOK(t, ts, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": sfnTagListBody(map[string]string{
			"zebra": "z", "alpha": "a", "middle": "m", "beta": "b", "yak": "y",
		}),
	})

	want := [][2]string{{"alpha", "a"}, {"beta", "b"}, {"middle", "m"}, {"yak", "y"}, {"zebra", "z"}}
	for i := range 2 {
		got := sfnTagPairs(t, ts, arn)
		if len(got) != len(want) {
			t.Fatalf("call %d: %d tags, want %d", i+1, len(got), len(want))
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("call %d: tags = %v, want %v", i+1, got, want)
			}
		}
	}
}
