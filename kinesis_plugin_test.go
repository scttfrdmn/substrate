package substrate_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupKinesisPlugin(t *testing.T) (*substrate.KinesisPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.KinesisPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("KinesisPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-1",
	}
}

func kinesisRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal kinesis request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "kinesis",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "Kinesis_20131202." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestKinesisPlugin_CreateAndDescribeStream(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	// Create stream.
	resp, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "my-stream",
		"ShardCount": 2,
	}))
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateStream: want 200, got %d", resp.StatusCode)
	}

	// Describe stream.
	resp, err = p.HandleRequest(ctx, kinesisRequest(t, "DescribeStream", map[string]any{
		"StreamName": "my-stream",
	}))
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeStream: want 200, got %d", resp.StatusCode)
	}

	var result struct {
		StreamDescription struct {
			StreamName   string `json:"StreamName"`
			StreamStatus string `json:"StreamStatus"`
			ShardCount   int    `json:"OpenShardCount"`
			Shards       []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal DescribeStream response: %v", err)
	}
	if result.StreamDescription.StreamName != "my-stream" {
		t.Errorf("want StreamName=my-stream, got %q", result.StreamDescription.StreamName)
	}
	if result.StreamDescription.StreamStatus != "ACTIVE" {
		t.Errorf("want StreamStatus=ACTIVE, got %q", result.StreamDescription.StreamStatus)
	}
	if result.StreamDescription.ShardCount != 2 {
		t.Errorf("want OpenShardCount=2, got %d", result.StreamDescription.ShardCount)
	}
	if len(result.StreamDescription.Shards) != 2 {
		t.Errorf("want 2 shards, got %d", len(result.StreamDescription.Shards))
	}
}

func TestKinesisPlugin_PutAndGetRecord(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	// Create stream.
	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "test-stream",
		"ShardCount": 1,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// Put a record.
	payload := base64.StdEncoding.EncodeToString([]byte("hello world"))
	putResp, err := p.HandleRequest(ctx, kinesisRequest(t, "PutRecord", map[string]any{
		"StreamName":   "test-stream",
		"Data":         payload,
		"PartitionKey": "key-1",
	}))
	if err != nil {
		t.Fatalf("PutRecord: %v", err)
	}
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PutRecord: want 200, got %d", putResp.StatusCode)
	}

	var putResult struct {
		ShardID        string `json:"ShardId"`
		SequenceNumber string `json:"SequenceNumber"`
		EncryptionType string `json:"EncryptionType"`
	}
	if err := json.Unmarshal(putResp.Body, &putResult); err != nil {
		t.Fatalf("unmarshal PutRecord response: %v", err)
	}
	if putResult.ShardID == "" {
		t.Error("PutRecord: ShardId is empty")
	}
	if putResult.SequenceNumber == "" {
		t.Error("PutRecord: SequenceNumber is empty")
	}
	if putResult.EncryptionType != "NONE" {
		t.Errorf("PutRecord: want EncryptionType=NONE, got %q", putResult.EncryptionType)
	}

	// Get shard iterator at TRIM_HORIZON.
	iterResp, err := p.HandleRequest(ctx, kinesisRequest(t, "GetShardIterator", map[string]any{
		"StreamName":        "test-stream",
		"ShardId":           putResult.ShardID,
		"ShardIteratorType": "TRIM_HORIZON",
	}))
	if err != nil {
		t.Fatalf("GetShardIterator: %v", err)
	}
	if iterResp.StatusCode != http.StatusOK {
		t.Fatalf("GetShardIterator: want 200, got %d", iterResp.StatusCode)
	}

	var iterResult struct {
		ShardIterator string `json:"ShardIterator"`
	}
	if err := json.Unmarshal(iterResp.Body, &iterResult); err != nil {
		t.Fatalf("unmarshal GetShardIterator response: %v", err)
	}
	if iterResult.ShardIterator == "" {
		t.Fatal("GetShardIterator: ShardIterator is empty")
	}

	// Get records.
	getResp, err := p.HandleRequest(ctx, kinesisRequest(t, "GetRecords", map[string]any{
		"ShardIterator": iterResult.ShardIterator,
		"Limit":         10,
	}))
	if err != nil {
		t.Fatalf("GetRecords: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GetRecords: want 200, got %d", getResp.StatusCode)
	}

	var getResult struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
			Data           string `json:"Data"`
			PartitionKey   string `json:"PartitionKey"`
		} `json:"Records"`
		NextShardIterator  string `json:"NextShardIterator"`
		MillisBehindLatest int    `json:"MillisBehindLatest"`
	}
	if err := json.Unmarshal(getResp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal GetRecords response: %v", err)
	}
	if len(getResult.Records) != 1 {
		t.Fatalf("want 1 record, got %d", len(getResult.Records))
	}
	if getResult.Records[0].Data != payload {
		t.Errorf("want Data=%q, got %q", payload, getResult.Records[0].Data)
	}
	if getResult.Records[0].PartitionKey != "key-1" {
		t.Errorf("want PartitionKey=key-1, got %q", getResult.Records[0].PartitionKey)
	}
	if getResult.NextShardIterator == "" {
		t.Error("GetRecords: NextShardIterator is empty")
	}
}

func TestKinesisPlugin_PutRecords(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	// Create stream with 3 shards.
	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "batch-stream",
		"ShardCount": 3,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	records := []map[string]any{
		{"Data": base64.StdEncoding.EncodeToString([]byte("msg1")), "PartitionKey": "pk1"},
		{"Data": base64.StdEncoding.EncodeToString([]byte("msg2")), "PartitionKey": "pk2"},
		{"Data": base64.StdEncoding.EncodeToString([]byte("msg3")), "PartitionKey": "pk3"},
	}

	resp, err := p.HandleRequest(ctx, kinesisRequest(t, "PutRecords", map[string]any{
		"StreamName": "batch-stream",
		"Records":    records,
	}))
	if err != nil {
		t.Fatalf("PutRecords: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PutRecords: want 200, got %d", resp.StatusCode)
	}

	var result struct {
		FailedRecordCount int `json:"FailedRecordCount"`
		Records           []struct {
			ShardID        string `json:"ShardId"`
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal PutRecords response: %v", err)
	}
	if result.FailedRecordCount != 0 {
		t.Errorf("want FailedRecordCount=0, got %d", result.FailedRecordCount)
	}
	if len(result.Records) != 3 {
		t.Errorf("want 3 records in response, got %d", len(result.Records))
	}
	for i, r := range result.Records {
		if r.ShardID == "" {
			t.Errorf("record[%d]: ShardId is empty", i)
		}
		if r.SequenceNumber == "" {
			t.Errorf("record[%d]: SequenceNumber is empty", i)
		}
	}
}

func TestKinesisPlugin_ListStreams(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	for _, name := range []string{"stream-a", "stream-b"} {
		if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
			"StreamName": name,
			"ShardCount": 1,
		})); err != nil {
			t.Fatalf("CreateStream %q: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, kinesisRequest(t, "ListStreams", map[string]any{}))
	if err != nil {
		t.Fatalf("ListStreams: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListStreams: want 200, got %d", resp.StatusCode)
	}

	var result struct {
		StreamNames    []string `json:"StreamNames"`
		HasMoreStreams bool     `json:"HasMoreStreams"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal ListStreams response: %v", err)
	}
	if len(result.StreamNames) != 2 {
		t.Errorf("want 2 streams, got %d", len(result.StreamNames))
	}
	if result.HasMoreStreams {
		t.Error("want HasMoreStreams=false")
	}
}

func TestKinesisPlugin_Tags(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "tagged-stream",
		"ShardCount": 1,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// Add tags.
	resp, err := p.HandleRequest(ctx, kinesisRequest(t, "AddTagsToStream", map[string]any{
		"StreamName": "tagged-stream",
		"Tags":       map[string]string{"env": "test", "team": "platform"},
	}))
	if err != nil {
		t.Fatalf("AddTagsToStream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("AddTagsToStream: want 200, got %d", resp.StatusCode)
	}

	// List tags.
	resp, err = p.HandleRequest(ctx, kinesisRequest(t, "ListTagsForStream", map[string]any{
		"StreamName": "tagged-stream",
	}))
	if err != nil {
		t.Fatalf("ListTagsForStream: %v", err)
	}

	var listResult struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
		HasMoreTags bool `json:"HasMoreTags"`
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal ListTagsForStream response: %v", err)
	}
	if len(listResult.Tags) != 2 {
		t.Errorf("want 2 tags, got %d", len(listResult.Tags))
	}
	if listResult.HasMoreTags {
		t.Error("want HasMoreTags=false")
	}

	// Remove one tag.
	resp, err = p.HandleRequest(ctx, kinesisRequest(t, "RemoveTagsFromStream", map[string]any{
		"StreamName": "tagged-stream",
		"TagKeys":    []string{"team"},
	}))
	if err != nil {
		t.Fatalf("RemoveTagsFromStream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RemoveTagsFromStream: want 200, got %d", resp.StatusCode)
	}

	// List tags again — should be 1.
	resp, err = p.HandleRequest(ctx, kinesisRequest(t, "ListTagsForStream", map[string]any{
		"StreamName": "tagged-stream",
	}))
	if err != nil {
		t.Fatalf("ListTagsForStream (after remove): %v", err)
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal ListTagsForStream response: %v", err)
	}
	if len(listResult.Tags) != 1 {
		t.Errorf("want 1 tag after removal, got %d", len(listResult.Tags))
	}
	if listResult.Tags[0].Key != "env" {
		t.Errorf("want remaining tag key=env, got %q", listResult.Tags[0].Key)
	}
}

func TestKinesisPlugin_DeleteStream(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "delete-me",
		"ShardCount": 1,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	delResp, err := p.HandleRequest(ctx, kinesisRequest(t, "DeleteStream", map[string]any{
		"StreamName": "delete-me",
	}))
	if err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", delResp.StatusCode)
	}

	// DescribeStream should fail after delete.
	_, err = p.HandleRequest(ctx, kinesisRequest(t, "DescribeStream", map[string]any{
		"StreamName": "delete-me",
	}))
	if err == nil {
		t.Error("expected error for deleted stream, got nil")
	}
}

func TestKinesisPlugin_DescribeStreamSummary(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "summary-stream",
		"ShardCount": 2,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	resp, err := p.HandleRequest(ctx, kinesisRequest(t, "DescribeStreamSummary", map[string]any{
		"StreamName": "summary-stream",
	}))
	if err != nil {
		t.Fatalf("DescribeStreamSummary: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var out struct {
		StreamDescriptionSummary struct {
			StreamName     string `json:"StreamName"`
			StreamStatus   string `json:"StreamStatus"`
			OpenShardCount int    `json:"OpenShardCount"`
		} `json:"StreamDescriptionSummary"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.StreamDescriptionSummary.StreamName != "summary-stream" {
		t.Errorf("want StreamName=summary-stream, got %q", out.StreamDescriptionSummary.StreamName)
	}
	if out.StreamDescriptionSummary.OpenShardCount != 2 {
		t.Errorf("want OpenShardCount=2, got %d", out.StreamDescriptionSummary.OpenShardCount)
	}
}

func TestKinesisPlugin_UpdateShardCount(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "reshardable",
		"ShardCount": 2,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	resp, err := p.HandleRequest(ctx, kinesisRequest(t, "UpdateShardCount", map[string]any{
		"StreamName":       "reshardable",
		"TargetShardCount": 4,
		"ScalingType":      "UNIFORM_SCALING",
	}))
	if err != nil {
		t.Fatalf("UpdateShardCount: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var out struct {
		StreamName        string `json:"StreamName"`
		CurrentShardCount int    `json:"CurrentShardCount"`
		TargetShardCount  int    `json:"TargetShardCount"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TargetShardCount != 4 {
		t.Errorf("want TargetShardCount=4, got %d", out.TargetShardCount)
	}
}

func TestKinesisPlugin_MergeAndSplitShards(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "split-merge-stream",
		"ShardCount": 2,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// MergeShards — stub that returns OK.
	mergeResp, err := p.HandleRequest(ctx, kinesisRequest(t, "MergeShards", map[string]any{
		"StreamName":           "split-merge-stream",
		"ShardToMerge":         "shardId-000000000000",
		"AdjacentShardToMerge": "shardId-000000000001",
	}))
	if err != nil {
		t.Fatalf("MergeShards: %v", err)
	}
	if mergeResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", mergeResp.StatusCode)
	}

	// SplitShard — stub that returns OK.
	splitResp, err := p.HandleRequest(ctx, kinesisRequest(t, "SplitShard", map[string]any{
		"StreamName":         "split-merge-stream",
		"ShardToSplit":       "shardId-000000000000",
		"NewStartingHashKey": "170141183460469231731687303715884105728",
	}))
	if err != nil {
		t.Fatalf("SplitShard: %v", err)
	}
	if splitResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", splitResp.StatusCode)
	}
}

func TestKinesisPlugin_EnhancedMonitoring(t *testing.T) {
	p, ctx := setupKinesisPlugin(t)

	if _, err := p.HandleRequest(ctx, kinesisRequest(t, "CreateStream", map[string]any{
		"StreamName": "monitor-stream",
		"ShardCount": 1,
	})); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// EnableEnhancedMonitoring.
	enableResp, err := p.HandleRequest(ctx, kinesisRequest(t, "EnableEnhancedMonitoring", map[string]any{
		"StreamName":        "monitor-stream",
		"ShardLevelMetrics": []string{"IncomingBytes", "OutgoingRecords"},
	}))
	if err != nil {
		t.Fatalf("EnableEnhancedMonitoring: %v", err)
	}
	if enableResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", enableResp.StatusCode)
	}

	// DisableEnhancedMonitoring.
	disableResp, err := p.HandleRequest(ctx, kinesisRequest(t, "DisableEnhancedMonitoring", map[string]any{
		"StreamName":        "monitor-stream",
		"ShardLevelMetrics": []string{"IncomingBytes"},
	}))
	if err != nil {
		t.Fatalf("DisableEnhancedMonitoring: %v", err)
	}
	if disableResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", disableResp.StatusCode)
	}
}
