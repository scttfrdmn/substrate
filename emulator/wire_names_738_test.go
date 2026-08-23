package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// requireWireNames asserts that body publishes each wanted member and none of the rejected
// spellings. Decoding into a raw map is the point: a typed struct would read whatever tag
// substrate happens to carry, which is exactly how #738's nine wrong names stayed green.
func requireWireNames(t *testing.T, what string, body map[string]any, want, reject []string) {
	t.Helper()
	for _, name := range want {
		if _, ok := body[name]; !ok {
			t.Errorf("%s: does not publish %q", what, name)
		}
	}
	for _, name := range reject {
		if _, ok := body[name]; ok {
			t.Errorf("%s: publishes %q, which AWS does not", what, name)
		}
	}
}

// TestCloudTrail_WireNames pins the two members CloudTrail spells differently from substrate's
// Go fields (#738). EnableLogFileValidation is CreateTrail's and UpdateTrail's *input* name —
// every response publishes the setting as LogFileValidationEnabled — and the key is KmsKeyId,
// not KMSKeyId. A real SDK read both as absent, so a caller could not tell validation was on.
func TestCloudTrail_WireNames(t *testing.T) {
	p, ctx := setupCloudTrailPlugin(t)
	const kmsKey = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"

	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
		"Name":                    "wire-trail",
		"S3BucketName":            "wire-bucket",
		"EnableLogFileValidation": true,
		"KmsKeyId":                kmsKey,
	}))
	if err != nil {
		t.Fatalf("CreateTrail: %v", err)
	}
	var created map[string]any
	if err := json.Unmarshal(resp.Body, &created); err != nil {
		t.Fatalf("unmarshal CreateTrail: %v", err)
	}
	requireWireNames(t, "CreateTrail",
		created,
		[]string{"LogFileValidationEnabled", "KmsKeyId", "TrailARN"},
		[]string{"EnableLogFileValidation", "KMSKeyId"},
	)
	if created["LogFileValidationEnabled"] != true {
		t.Errorf("CreateTrail: LogFileValidationEnabled = %v, want true — the input was not read",
			created["LogFileValidationEnabled"])
	}
	if created["KmsKeyId"] != kmsKey {
		t.Errorf("CreateTrail: KmsKeyId = %v, want %q", created["KmsKeyId"], kmsKey)
	}

	// GetTrail nests the same shape, so the tag has to be right in both places.
	resp, err = p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrail", map[string]any{
		"Name": "wire-trail",
	}))
	if err != nil {
		t.Fatalf("GetTrail: %v", err)
	}
	var got struct {
		Trail map[string]any `json:"Trail"`
	}
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		t.Fatalf("unmarshal GetTrail: %v", err)
	}
	requireWireNames(t, "GetTrail",
		got.Trail,
		[]string{"LogFileValidationEnabled", "KmsKeyId"},
		[]string{"EnableLogFileValidation", "KMSKeyId"},
	)
	if got.Trail["LogFileValidationEnabled"] != true {
		t.Errorf("GetTrail: LogFileValidationEnabled = %v, want true", got.Trail["LogFileValidationEnabled"])
	}

	// UpdateTrail still reads the input name, and turning validation off must be observable.
	resp, err = p.HandleRequest(ctx, cloudtrailRequest(t, "UpdateTrail", map[string]any{
		"Name":                    "wire-trail",
		"EnableLogFileValidation": false,
	}))
	if err != nil {
		t.Fatalf("UpdateTrail: %v", err)
	}
	var updated map[string]any
	if err := json.Unmarshal(resp.Body, &updated); err != nil {
		t.Fatalf("unmarshal UpdateTrail: %v", err)
	}
	if updated["LogFileValidationEnabled"] != false {
		t.Errorf("UpdateTrail: LogFileValidationEnabled = %v, want false",
			updated["LogFileValidationEnabled"])
	}
}

// TestEB_RuleWireNames pins a rule's ARN member (#738). EventBridge publishes it as Arn on the
// Rule type; PutRule wraps the same value under RuleArn, which was already correct.
func TestEB_RuleWireNames(t *testing.T) {
	srv := newEBTestServer(t)

	putResp := ebRequest(t, srv, "PutRule", map[string]string{
		"Name":               "wire-rule",
		"ScheduleExpression": "rate(5 minutes)",
		"State":              "ENABLED",
	})
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PutRule: got %d", putResp.StatusCode)
	}
	var put map[string]any
	if err := json.Unmarshal(ebReadBody(t, putResp), &put); err != nil {
		t.Fatalf("unmarshal PutRule: %v", err)
	}
	requireWireNames(t, "PutRule", put, []string{"RuleArn"}, []string{"RuleARN"})

	describeResp := ebRequest(t, srv, "DescribeRule", map[string]string{"Name": "wire-rule"})
	var described map[string]any
	if err := json.Unmarshal(ebReadBody(t, describeResp), &described); err != nil {
		t.Fatalf("unmarshal DescribeRule: %v", err)
	}
	requireWireNames(t, "DescribeRule", described, []string{"Arn"}, []string{"ARN"})
	arn, _ := described["Arn"].(string)
	if !strings.HasPrefix(arn, "arn:aws:events:") {
		t.Errorf("DescribeRule: Arn = %q, want an arn:aws:events: prefix", arn)
	}

	listResp := ebRequest(t, srv, "ListRules", map[string]string{})
	var listed struct {
		Rules []map[string]any `json:"Rules"`
	}
	if err := json.Unmarshal(ebReadBody(t, listResp), &listed); err != nil {
		t.Fatalf("unmarshal ListRules: %v", err)
	}
	if len(listed.Rules) != 1 {
		t.Fatalf("ListRules: want 1 rule, got %d", len(listed.Rules))
	}
	requireWireNames(t, "ListRules", listed.Rules[0], []string{"Arn"}, []string{"ARN"})
}

// TestLambdaESM_WireNames pins the two ARN members on an EventSourceMappingConfiguration
// (#738). Lambda publishes FunctionArn and EventSourceArn; the lowercase eventSourceARN in a
// stream record's event payload is a different, and correct, spelling and is untouched.
func TestLambdaESM_WireNames(t *testing.T) {
	srv := newLambdaTestServer(t)

	createResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "wire-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/lambda-role",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": "Zm9v"},
	})
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateFunction: got %d body: %s", createResp.StatusCode, readBody(t, createResp))
	}
	createResp.Body.Close() //nolint:errcheck

	const sourceARN = "arn:aws:kinesis:us-east-1:123456789012:stream/wire-stream"
	esmResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"FunctionName":     "wire-fn",
		"EventSourceArn":   sourceARN,
		"StartingPosition": "TRIM_HORIZON",
	})
	if esmResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateEventSourceMapping: got %d body: %s", esmResp.StatusCode, readBody(t, esmResp))
	}
	var created map[string]any
	decodeLambdaJSON(t, esmResp, &created)
	requireWireNames(t, "CreateEventSourceMapping",
		created,
		[]string{"UUID", "FunctionArn", "EventSourceArn"},
		[]string{"FunctionARN", "EventSourceARN"},
	)
	if created["EventSourceArn"] != sourceARN {
		t.Errorf("CreateEventSourceMapping: EventSourceArn = %v, want %q",
			created["EventSourceArn"], sourceARN)
	}
	fnARN, _ := created["FunctionArn"].(string)
	if !strings.HasPrefix(fnARN, "arn:aws:lambda:") {
		t.Errorf("CreateEventSourceMapping: FunctionArn = %q, want an arn:aws:lambda: prefix", fnARN)
	}

	uuid, _ := created["UUID"].(string)
	getResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GetEventSourceMapping: got %d body: %s", getResp.StatusCode, readBody(t, getResp))
	}
	var got map[string]any
	decodeLambdaJSON(t, getResp, &got)
	requireWireNames(t, "GetEventSourceMapping",
		got,
		[]string{"FunctionArn", "EventSourceArn"},
		[]string{"FunctionARN", "EventSourceARN"},
	)
}
