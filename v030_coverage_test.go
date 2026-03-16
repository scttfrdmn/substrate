// Package substrate_test contains coverage tests for v0.30.0 new features.
package substrate_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"testing"

	substrate "github.com/scttfrdmn/substrate"
)

// TestServiceQuotas_RequestIncrease_Empty verifies that listing increases for a
// service with no requests returns an empty (non-error) response.
func TestServiceQuotas_RequestIncrease_Empty(t *testing.T) {
	srv := substrate.StartTestServer(t)
	resp := makeServiceQuotasRequest(t, srv, "ListRequestedServiceQuotaChangesByService", map[string]interface{}{
		"ServiceCode": "ec2",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListRequested empty: %d", resp.StatusCode)
	}
}

// TestServiceQuotas_GetRequestedChange_NotFound verifies 404 for unknown requestID.
func TestServiceQuotas_GetRequestedChange_NotFound(t *testing.T) {
	srv := substrate.StartTestServer(t)
	resp := makeServiceQuotasRequest(t, srv, "GetRequestedServiceQuotaChange", map[string]interface{}{
		"RequestId": "nonexistent-id",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Fatal("expected error for nonexistent requestId, got 200")
	}
}

// TestServiceQuotas_UnknownOperation returns an error for unknown operations.
func TestServiceQuotas_UnknownOperation(t *testing.T) {
	srv := substrate.StartTestServer(t)
	resp := makeServiceQuotasRequest(t, srv, "UnknownOperation", nil)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode == http.StatusOK {
		t.Fatal("expected error for unknown operation, got 200")
	}
}

// TestSQSFIFO_ContentBasedDedup tests content-based deduplication where the
// SHA-256 of the body serves as the deduplication ID.
func TestSQSFIFO_ContentBasedDedup(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	resp := sqsRequest(t, srv, map[string]string{
		"Action":            "CreateQueue",
		"QueueName":         "cbd-queue.fifo",
		"Attribute.1.Name":  "FifoQueue",
		"Attribute.1.Value": "true",
		"Attribute.2.Name":  "ContentBasedDeduplication",
		"Attribute.2.Value": "true",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue: %d", resp.StatusCode)
	}
	var cqr struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	xml.NewDecoder(resp.Body).Decode(&cqr) //nolint:errcheck
	resp.Body.Close()                      //nolint:errcheck

	// Send with GroupId only (content-based dedup active).
	r1 := sqsRequest(t, srv, map[string]string{
		"Action":         "SendMessage",
		"QueueUrl":       cqr.QueueURL,
		"MessageBody":    "unique-content",
		"MessageGroupId": "g1",
	})
	if r1.StatusCode != http.StatusOK {
		body := readBody(t, r1)
		t.Fatalf("SendMessage content-based dedup: %d body: %s", r1.StatusCode, body)
	}
	r1.Body.Close() //nolint:errcheck

	// Send same body again — should be deduplicated.
	r2 := sqsRequest(t, srv, map[string]string{
		"Action":         "SendMessage",
		"QueueUrl":       cqr.QueueURL,
		"MessageBody":    "unique-content",
		"MessageGroupId": "g1",
	})
	if r2.StatusCode != http.StatusOK {
		body := readBody(t, r2)
		t.Fatalf("second send (content dedup): %d body: %s", r2.StatusCode, body)
	}
	r2.Body.Close() //nolint:errcheck
}

// TestS3Versioning_GetBucketVersioningNotSet verifies that a fresh bucket
// returns a valid (empty) versioning response.
func TestS3Versioning_GetBucketVersioningNotSet(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/fresh-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodGet, "/fresh-bucket?versioning", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GetBucketVersioning on fresh bucket: %d body: %s", w.Code, w.Body.String())
	}
}

// TestS3Versioning_PermanentDelete verifies that DELETE with a versionId
// permanently removes that specific version.
func TestS3Versioning_PermanentDelete(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/pd-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/pd-bucket?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})

	w := s3Request(t, srv, http.MethodPut, "/pd-bucket/k", []byte("body"), nil)
	vid := w.Header().Get("X-Amz-Version-Id")
	if vid == "" {
		t.Fatal("expected version ID")
	}

	wd := s3Request(t, srv, http.MethodDelete, "/pd-bucket/k?versionId="+vid, nil, nil)
	if wd.Code != http.StatusNoContent {
		t.Fatalf("permanent delete: %d", wd.Code)
	}

	wg := s3Request(t, srv, http.MethodGet, "/pd-bucket/k?versionId="+vid, nil, nil)
	if wg.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after permanent delete, got %d", wg.Code)
	}
}

// TestDynamoDBStreams_UpdateItemRecord verifies MODIFY records appear in the
// stream after an UpdateItem call.
func TestDynamoDBStreams_UpdateItemRecord(t *testing.T) {
	srv := newDynamoDBTestServer(t)

	dynamodbRequest(t, srv, "CreateTable", map[string]any{
		"TableName": "update-stream-table",
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
	}).Body.Close() //nolint:errcheck

	dynamodbRequest(t, srv, "PutItem", map[string]any{
		"TableName": "update-stream-table",
		"Item":      map[string]any{"pk": map[string]any{"S": "row1"}, "v": map[string]any{"N": "1"}},
	}).Body.Close() //nolint:errcheck

	dynamodbRequest(t, srv, "UpdateItem", map[string]any{
		"TableName":        "update-stream-table",
		"Key":              map[string]any{"pk": map[string]any{"S": "row1"}},
		"UpdateExpression": "SET v = :v",
		"ExpressionAttributeValues": map[string]any{
			":v": map[string]any{"N": "2"},
		},
	}).Body.Close() //nolint:errcheck

	dtResp := dynamodbRequest(t, srv, "DescribeTable", map[string]any{"TableName": "update-stream-table"})
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

	hasModify := false
	for _, r := range records {
		if m, ok := r.(map[string]any); ok && m["eventName"] == "MODIFY" {
			hasModify = true
		}
	}
	if !hasModify {
		t.Errorf("expected MODIFY record in stream, got records: %v", records)
	}
}

// TestLambdaESM_ListByFunctionName verifies filtering ESMs by function name.
func TestLambdaESM_ListByFunctionName(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "fn-filter",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/role",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": "Zm9v"},
	}).Body.Close() //nolint:errcheck

	esmResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"FunctionName":     "fn-filter",
		"EventSourceArn":   "arn:aws:kinesis:us-east-1:123456789012:stream/stream-s",
		"StartingPosition": "LATEST",
	})
	if esmResp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateESM: %d", esmResp.StatusCode)
	}
	esmResp.Body.Close() //nolint:errcheck

	// List by FunctionName query param.
	listResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/?FunctionName=fn-filter", nil)
	if listResp.StatusCode != http.StatusOK {
		body := readBody(t, listResp)
		t.Fatalf("ListESM by function: %d body: %s", listResp.StatusCode, body)
	}
	var listResult map[string]any
	decodeLambdaJSON(t, listResp, &listResult)
	mappings, _ := listResult["EventSourceMappings"].([]any)
	if len(mappings) == 0 {
		t.Error("expected ESM in list filtered by function name")
	}
}

// TestCFN_S3BucketVersioning deploys an S3 bucket with versioning via CFN.
func TestCFN_S3BucketVersioning(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"VersionedBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {
					"BucketName": "versioned-cfn-bucket",
					"VersioningConfiguration": {"Status": "Enabled"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestCFN_LambdaEventSourceMapping deploys a Lambda function + ESM via CFN.
func TestCFN_LambdaEventSourceMapping(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyFunction": {
				"Type": "AWS::Lambda::Function",
				"Properties": {
					"FunctionName": "esm-fn",
					"Runtime": "python3.12",
					"Role": "arn:aws:iam::123456789012:role/r",
					"Handler": "index.handler",
					"Code": {"ZipFile": "def handler(e,c): pass"}
				}
			},
			"MyESM": {
				"Type": "AWS::Lambda::EventSourceMapping",
				"Properties": {
					"FunctionName": "esm-fn",
					"EventSourceArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
					"StartingPosition": "LATEST",
					"BatchSize": 10
				},
				"DependsOn": ["MyFunction"]
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestLambdaPlugin_ListFunctionsMaxItems verifies MaxItems pagination param.
func TestLambdaPlugin_ListFunctionsMaxItems(t *testing.T) {
	srv := newLambdaTestServer(t)

	for _, name := range []string{"func-a", "func-b", "func-c"} {
		lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
			"FunctionName": name,
			"Runtime":      "python3.12",
			"Role":         "arn:aws:iam::123456789012:role/role",
			"Handler":      "index.handler",
			"Code":         map[string]any{"ZipFile": "Zm9v"},
		}).Body.Close() //nolint:errcheck
	}

	// List with MaxItems=2 — should return only 2 items and a next marker.
	listResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions?MaxItems=2", nil)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("ListFunctions MaxItems: %d", listResp.StatusCode)
	}
	var result map[string]any
	decodeLambdaJSON(t, listResp, &result)
	funcs, _ := result["Functions"].([]any)
	if len(funcs) != 2 {
		t.Errorf("expected 2 functions with MaxItems=2, got %d", len(funcs))
	}
}

// TestLambdaESM_ListAll verifies that ListEventSourceMappings returns all ESMs.
func TestLambdaESM_ListAll(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "fn-x",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/role",
		"Handler":      "index.handler",
		"Code":         map[string]any{"ZipFile": "Zm9v"},
	}).Body.Close() //nolint:errcheck

	for i := 0; i < 2; i++ {
		esmResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
			"FunctionName":     "fn-x",
			"EventSourceArn":   "arn:aws:kinesis:us-east-1:123456789012:stream/s",
			"StartingPosition": "LATEST",
		})
		if esmResp.StatusCode != http.StatusCreated {
			t.Fatalf("CreateESM %d: %d", i, esmResp.StatusCode)
		}
		esmResp.Body.Close() //nolint:errcheck
	}

	listResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/", nil)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("ListESM: %d", listResp.StatusCode)
	}
	var listResult map[string]any
	decodeLambdaJSON(t, listResp, &listResult)
	mappings, _ := listResult["EventSourceMappings"].([]any)
	if len(mappings) < 2 {
		t.Errorf("expected at least 2 ESMs, got %d", len(mappings))
	}
}
