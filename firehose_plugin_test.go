package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupFirehosePlugin(t *testing.T) (*substrate.FirehosePlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.FirehosePlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("FirehosePlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-firehose-1",
	}
}

func firehoseRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal firehose request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "firehose",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "Firehose_20150804." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestFirehosePlugin(t *testing.T) {
	p, ctx := setupFirehosePlugin(t)

	t.Run("CreateDeliveryStream", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, firehoseRequest(t, "CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": "my-stream",
			"DeliveryStreamType": "DirectPut",
		}))
		if err != nil {
			t.Fatalf("CreateDeliveryStream: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			DeliveryStreamARN string `json:"DeliveryStreamARN"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal create response: %v", err)
		}
		if result.DeliveryStreamARN == "" {
			t.Error("want non-empty DeliveryStreamARN")
		}
	})

	t.Run("CreateDeliveryStream_Duplicate", func(t *testing.T) {
		_, err := p.HandleRequest(ctx, firehoseRequest(t, "CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": "my-stream",
		}))
		if err == nil {
			t.Fatal("want error for duplicate stream, got nil")
		}
		awsErr, ok := err.(*substrate.AWSError)
		if !ok {
			t.Fatalf("want *AWSError, got %T", err)
		}
		if awsErr.Code != "ResourceInUseException" {
			t.Errorf("want ResourceInUseException, got %q", awsErr.Code)
		}
	})

	t.Run("DescribeDeliveryStream", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, firehoseRequest(t, "DescribeDeliveryStream", map[string]any{
			"DeliveryStreamName": "my-stream",
		}))
		if err != nil {
			t.Fatalf("DescribeDeliveryStream: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			DeliveryStreamDescription substrate.FirehoseDeliveryStream `json:"DeliveryStreamDescription"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal describe response: %v", err)
		}
		if result.DeliveryStreamDescription.DeliveryStreamName != "my-stream" {
			t.Errorf("want DeliveryStreamName=my-stream, got %q", result.DeliveryStreamDescription.DeliveryStreamName)
		}
		if result.DeliveryStreamDescription.DeliveryStreamStatus != "ACTIVE" {
			t.Errorf("want DeliveryStreamStatus=ACTIVE, got %q", result.DeliveryStreamDescription.DeliveryStreamStatus)
		}
	})

	t.Run("PutRecord", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, firehoseRequest(t, "PutRecord", map[string]any{
			"DeliveryStreamName": "my-stream",
			"Record":             map[string]string{"Data": "aGVsbG8="},
		}))
		if err != nil {
			t.Fatalf("PutRecord: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			RecordID  string `json:"RecordId"`
			Encrypted bool   `json:"Encrypted"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal putrecord response: %v", err)
		}
		if result.RecordID == "" {
			t.Error("want non-empty RecordId")
		}
	})

	t.Run("PutRecordBatch", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, firehoseRequest(t, "PutRecordBatch", map[string]any{
			"DeliveryStreamName": "my-stream",
			"Records": []map[string]string{
				{"Data": "cmVjb3JkMQ=="},
				{"Data": "cmVjb3JkMg=="},
				{"Data": "cmVjb3JkMw=="},
			},
		}))
		if err != nil {
			t.Fatalf("PutRecordBatch: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			FailedPutCount   int `json:"FailedPutCount"`
			RequestResponses []struct {
				RecordID string `json:"RecordId"`
			} `json:"RequestResponses"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal putrecordbatch response: %v", err)
		}
		if result.FailedPutCount != 0 {
			t.Errorf("want FailedPutCount=0, got %d", result.FailedPutCount)
		}
		if len(result.RequestResponses) != 3 {
			t.Errorf("want 3 responses, got %d", len(result.RequestResponses))
		}
	})

	t.Run("ListDeliveryStreams", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, firehoseRequest(t, "ListDeliveryStreams", map[string]any{}))
		if err != nil {
			t.Fatalf("ListDeliveryStreams: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var result struct {
			DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
			HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal list response: %v", err)
		}
		if len(result.DeliveryStreamNames) != 1 {
			t.Errorf("want 1 stream, got %d", len(result.DeliveryStreamNames))
		}
		if result.DeliveryStreamNames[0] != "my-stream" {
			t.Errorf("want my-stream, got %q", result.DeliveryStreamNames[0])
		}
		if result.HasMoreDeliveryStreams {
			t.Error("want HasMoreDeliveryStreams=false")
		}
	})

	t.Run("DeleteDeliveryStream", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, firehoseRequest(t, "DeleteDeliveryStream", map[string]any{
			"DeliveryStreamName": "my-stream",
		}))
		if err != nil {
			t.Fatalf("DeleteDeliveryStream: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
	})

	t.Run("DescribeDeliveryStream_NotFound", func(t *testing.T) {
		_, err := p.HandleRequest(ctx, firehoseRequest(t, "DescribeDeliveryStream", map[string]any{
			"DeliveryStreamName": "my-stream",
		}))
		if err == nil {
			t.Fatal("want error for missing stream, got nil")
		}
		awsErr, ok := err.(*substrate.AWSError)
		if !ok {
			t.Fatalf("want *AWSError, got %T", err)
		}
		if awsErr.Code != "ResourceNotFoundException" {
			t.Errorf("want ResourceNotFoundException, got %q", awsErr.Code)
		}
	})

	t.Run("ListDeliveryStreams_Pagination", func(t *testing.T) {
		p2, ctx2 := setupFirehosePlugin(t)
		// Create 3 streams.
		for _, name := range []string{"stream-a", "stream-b", "stream-c"} {
			_, err := p2.HandleRequest(ctx2, firehoseRequest(t, "CreateDeliveryStream", map[string]any{
				"DeliveryStreamName": name,
				"DeliveryStreamType": "DirectPut",
			}))
			if err != nil {
				t.Fatalf("CreateDeliveryStream %s: %v", name, err)
			}
		}
		// List with Limit=2.
		resp, err := p2.HandleRequest(ctx2, firehoseRequest(t, "ListDeliveryStreams", map[string]any{
			"Limit": 2,
		}))
		if err != nil {
			t.Fatalf("ListDeliveryStreams: %v", err)
		}
		var result struct {
			DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
			HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
		}
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(result.DeliveryStreamNames) != 2 {
			t.Errorf("want 2 streams, got %d", len(result.DeliveryStreamNames))
		}
		if !result.HasMoreDeliveryStreams {
			t.Error("want HasMoreDeliveryStreams=true")
		}
		// Second page using ExclusiveStartDeliveryStreamName cursor.
		resp2, err := p2.HandleRequest(ctx2, firehoseRequest(t, "ListDeliveryStreams", map[string]any{
			"ExclusiveStartDeliveryStreamName": result.DeliveryStreamNames[1],
		}))
		if err != nil {
			t.Fatalf("ListDeliveryStreams page2: %v", err)
		}
		var result2 struct {
			DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
			HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
		}
		if err := json.Unmarshal(resp2.Body, &result2); err != nil {
			t.Fatalf("unmarshal page2: %v", err)
		}
		if len(result2.DeliveryStreamNames) == 0 {
			t.Error("want at least 1 stream on second page")
		}
		if result2.HasMoreDeliveryStreams {
			t.Error("want HasMoreDeliveryStreams=false on last page")
		}
	})

	t.Run("DeleteDeliveryStream_NotFound", func(t *testing.T) {
		_, err := p.HandleRequest(ctx, firehoseRequest(t, "DeleteDeliveryStream", map[string]any{
			"DeliveryStreamName": "nonexistent-stream",
		}))
		if err == nil {
			t.Fatal("want error for missing stream, got nil")
		}
		awsErr, ok := err.(*substrate.AWSError)
		if !ok {
			t.Fatalf("want *AWSError, got %T", err)
		}
		if awsErr.Code != "ResourceNotFoundException" {
			t.Errorf("want ResourceNotFoundException, got %q", awsErr.Code)
		}
	})
}
