package substrate_test

import (
	"net/http"
	"testing"
)

// TestLambdaESM_CreateAndList creates an event source mapping and lists it.
func TestLambdaESM_CreateAndList(t *testing.T) {
	srv := newLambdaTestServer(t)

	// Create a Lambda function first so the ARN resolves.
	createResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "processor",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/lambda-role",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": "Zm9v"},
	})
	if createResp.StatusCode != http.StatusCreated {
		body := readBody(t, createResp)
		t.Fatalf("CreateFunction: got %d body: %s", createResp.StatusCode, body)
	}
	createResp.Body.Close() //nolint:errcheck

	// CreateEventSourceMapping.
	esmResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"FunctionName":     "processor",
		"EventSourceArn":   "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
		"StartingPosition": "TRIM_HORIZON",
		"BatchSize":        10,
	})
	if esmResp.StatusCode != http.StatusCreated {
		body := readBody(t, esmResp)
		t.Fatalf("CreateEventSourceMapping: got %d body: %s", esmResp.StatusCode, body)
	}
	var esmResult map[string]any
	decodeLambdaJSON(t, esmResp, &esmResult)
	uuid, _ := esmResult["UUID"].(string)
	if uuid == "" {
		t.Fatal("expected non-empty UUID in CreateEventSourceMapping response")
	}
	if esmResult["State"] != "Enabled" {
		t.Errorf("expected State=Enabled, got %v", esmResult["State"])
	}

	// GetEventSourceMapping.
	getResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
	if getResp.StatusCode != http.StatusOK {
		body := readBody(t, getResp)
		t.Fatalf("GetEventSourceMapping: got %d body: %s", getResp.StatusCode, body)
	}
	var getResult map[string]any
	decodeLambdaJSON(t, getResp, &getResult)
	if getResult["UUID"] != uuid {
		t.Errorf("expected UUID=%s, got %v", uuid, getResult["UUID"])
	}

	// ListEventSourceMappings.
	listResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/", nil)
	if listResp.StatusCode != http.StatusOK {
		body := readBody(t, listResp)
		t.Fatalf("ListEventSourceMappings: got %d body: %s", listResp.StatusCode, body)
	}
	var listResult map[string]any
	decodeLambdaJSON(t, listResp, &listResult)
	mappings, _ := listResult["EventSourceMappings"].([]any)
	if len(mappings) == 0 {
		t.Fatal("expected at least one ESM in ListEventSourceMappings")
	}
}

// TestLambdaESM_UpdateAndDelete updates an ESM's BatchSize then deletes it.
func TestLambdaESM_UpdateAndDelete(t *testing.T) {
	srv := newLambdaTestServer(t)

	// Create function.
	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "fn2",
		"Runtime":      "nodejs20.x",
		"Role":         "arn:aws:iam::123456789012:role/role",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": "Zm9v"},
	}).Body.Close() //nolint:errcheck

	// Create ESM.
	esmResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"FunctionName":     "fn2",
		"EventSourceArn":   "arn:aws:dynamodb:us-east-1:123456789012:table/t/stream/label",
		"StartingPosition": "LATEST",
		"BatchSize":        5,
	})
	if esmResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateESM: got %d", esmResp.StatusCode)
	}
	var created map[string]any
	decodeLambdaJSON(t, esmResp, &created)
	uuid, _ := created["UUID"].(string)

	// UpdateEventSourceMapping — change batch size.
	updResp := lambdaRequest(t, srv, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid, map[string]any{
		"BatchSize": 100,
	})
	if updResp.StatusCode != http.StatusOK {
		body := readBody(t, updResp)
		t.Fatalf("UpdateEventSourceMapping: got %d body: %s", updResp.StatusCode, body)
	}
	var updated map[string]any
	decodeLambdaJSON(t, updResp, &updated)
	batchSize, _ := updated["BatchSize"].(float64)
	if int(batchSize) != 100 {
		t.Errorf("expected BatchSize=100 after update, got %v", batchSize)
	}

	// DeleteEventSourceMapping.
	delResp := lambdaRequest(t, srv, http.MethodDelete, "/2015-03-31/event-source-mappings/"+uuid, nil)
	if delResp.StatusCode != http.StatusOK && delResp.StatusCode != http.StatusNoContent {
		body := readBody(t, delResp)
		t.Fatalf("DeleteEventSourceMapping: got %d body: %s", delResp.StatusCode, body)
	}
	delResp.Body.Close() //nolint:errcheck

	// GetEventSourceMapping after delete → 404.
	getResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
	if getResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getResp.StatusCode)
	}
	getResp.Body.Close() //nolint:errcheck
}

// TestLambdaESM_GetNotFound returns 404 for an unknown UUID.
func TestLambdaESM_GetNotFound(t *testing.T) {
	srv := newLambdaTestServer(t)
	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/unknown-uuid", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown ESM, got %d", resp.StatusCode)
	}
	resp.Body.Close() //nolint:errcheck
}
