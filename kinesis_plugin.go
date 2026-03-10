package substrate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"
)

// kinesisMaxRecordsPerShard is the maximum number of records retained per shard
// in the ring buffer.
const kinesisMaxRecordsPerShard = 10000

// KinesisPlugin emulates the Amazon Kinesis Data Streams JSON-protocol API.
// It handles stream lifecycle, shard management, record production, and
// record consumption operations.
type KinesisPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "kinesis".
func (p *KinesisPlugin) Name() string { return "kinesis" }

// Initialize sets up the KinesisPlugin with the provided configuration.
func (p *KinesisPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for KinesisPlugin.
func (p *KinesisPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Kinesis JSON-protocol request to the appropriate handler.
// The operation is derived from the X-Amz-Target suffix after "Kinesis_20131202.".
func (p *KinesisPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		const prefix = "Kinesis_20131202."
		if after, ok := strings.CutPrefix(target, prefix); ok {
			op = after
		}
	}

	switch op {
	case "CreateStream":
		return p.createStream(ctx, req)
	case "DeleteStream":
		return p.deleteStream(ctx, req)
	case "DescribeStream":
		return p.describeStream(ctx, req)
	case "DescribeStreamSummary":
		return p.describeStreamSummary(ctx, req)
	case "ListStreams":
		return p.listStreams(ctx, req)
	case "UpdateShardCount":
		return p.updateShardCount(ctx, req)
	case "PutRecord":
		return p.putRecord(ctx, req)
	case "PutRecords":
		return p.putRecords(ctx, req)
	case "GetShardIterator":
		return p.getShardIterator(ctx, req)
	case "GetRecords":
		return p.getRecords(ctx, req)
	case "MergeShards":
		return p.mergeShards(ctx, req)
	case "SplitShard":
		return p.splitShard(ctx, req)
	case "AddTagsToStream":
		return p.addTagsToStream(ctx, req)
	case "RemoveTagsFromStream":
		return p.removeTagsFromStream(ctx, req)
	case "ListTagsForStream":
		return p.listTagsForStream(ctx, req)
	case "EnableEnhancedMonitoring":
		return p.enableEnhancedMonitoring(ctx, req)
	case "DisableEnhancedMonitoring":
		return p.disableEnhancedMonitoring(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("KinesisPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Stream lifecycle -------------------------------------------------------

func (p *KinesisPlugin) createStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
		ShardCount int    `json:"ShardCount"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.ShardCount <= 0 {
		body.ShardCount = 1
	}

	goCtx := context.Background()
	stateKey := kinesisStreamKey(ctx.AccountID, ctx.Region, body.StreamName)
	existing, err := p.state.Get(goCtx, kinesisNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("kinesis createStream state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceInUseException", Message: "Stream already exists: " + body.StreamName, HTTPStatus: http.StatusConflict}
	}

	streamARN := fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", ctx.Region, ctx.AccountID, body.StreamName)
	stream := KinesisStream{
		StreamName:           body.StreamName,
		StreamArn:            streamARN,
		StreamStatus:         "ACTIVE",
		ShardCount:           body.ShardCount,
		Shards:               generateKinesisShards(body.ShardCount),
		RetentionPeriodHours: 24,
		Tags:                 map[string]string{},
		EnhancedMonitoring:   []string{},
		CreatedAt:            p.tc.Now(),
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
	}

	data, err := json.Marshal(stream)
	if err != nil {
		return nil, fmt.Errorf("kinesis createStream marshal: %w", err)
	}
	if err := p.state.Put(goCtx, kinesisNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("kinesis createStream state.Put: %w", err)
	}

	idxKey := kinesisStreamNamesKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, kinesisNamespace, idxKey, body.StreamName)

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) deleteStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := kinesisStreamKey(ctx.AccountID, ctx.Region, body.StreamName)
	existing, err := p.state.Get(goCtx, kinesisNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("kinesis deleteStream state.Get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Stream not found: " + body.StreamName, HTTPStatus: http.StatusNotFound}
	}

	var stream KinesisStream
	if err := json.Unmarshal(existing, &stream); err != nil {
		return nil, fmt.Errorf("kinesis deleteStream unmarshal: %w", err)
	}

	// Delete shard record buffers.
	for _, shard := range stream.Shards {
		rk := kinesisRecordKey(ctx.AccountID, ctx.Region, body.StreamName, shard.ShardID)
		_ = p.state.Delete(goCtx, kinesisNamespace, rk)
	}

	if err := p.state.Delete(goCtx, kinesisNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("kinesis deleteStream state.Delete: %w", err)
	}

	idxKey := kinesisStreamNamesKey(ctx.AccountID, ctx.Region)
	removeFromStringIndex(goCtx, p.state, kinesisNamespace, idxKey, body.StreamName)

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) describeStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	desc := buildStreamDescription(stream)
	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamDescription": desc,
	})
}

func (p *KinesisPlugin) describeStreamSummary(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	desc := buildStreamDescription(stream)
	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamDescriptionSummary": desc,
	})
}

func (p *KinesisPlugin) listStreams(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ExclusiveStartStreamName string `json:"ExclusiveStartStreamName"`
		Limit                    int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	goCtx := context.Background()
	idxKey := kinesisStreamNamesKey(ctx.AccountID, ctx.Region)
	names, err := loadStringIndex(goCtx, p.state, kinesisNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("kinesis listStreams loadIndex: %w", err)
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamNames":    names,
		"HasMoreStreams": false,
	})
}

func (p *KinesisPlugin) updateShardCount(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName       string `json:"StreamName"`
		TargetShardCount int    `json:"TargetShardCount"`
		ScalingType      string `json:"ScalingType"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	current := stream.ShardCount
	stream.ShardCount = body.TargetShardCount
	stream.Shards = generateKinesisShards(body.TargetShardCount)
	stream.StreamStatus = "ACTIVE"

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamName":        stream.StreamName,
		"CurrentShardCount": current,
		"TargetShardCount":  body.TargetShardCount,
	})
}

// --- Record operations ------------------------------------------------------

func (p *KinesisPlugin) putRecord(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName   string `json:"StreamName"`
		Data         string `json:"Data"`
		PartitionKey string `json:"PartitionKey"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	shardID := "shardId-000000000000"
	if len(stream.Shards) > 0 {
		shardID = stream.Shards[0].ShardID
	}

	seqNo, err := generateKinesisSeqNo()
	if err != nil {
		return nil, fmt.Errorf("kinesis putRecord generateSeqNo: %w", err)
	}

	record := KinesisRecord{
		SequenceNumber:              seqNo,
		ApproximateArrivalTimestamp: p.tc.Now(),
		Data:                        body.Data,
		PartitionKey:                body.PartitionKey,
		ShardID:                     shardID,
	}

	if err := p.appendRecord(ctx, body.StreamName, shardID, record); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"ShardId":        shardID,
		"SequenceNumber": seqNo,
		"EncryptionType": "NONE",
	})
}

func (p *KinesisPlugin) putRecords(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
		Records    []struct {
			Data         string `json:"Data"`
			PartitionKey string `json:"PartitionKey"`
		} `json:"Records"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	type recordResult struct {
		ShardID        string `json:"ShardId"`
		SequenceNumber string `json:"SequenceNumber"`
	}

	results := make([]recordResult, 0, len(body.Records))
	for i, rec := range body.Records {
		shardIdx := 0
		if len(stream.Shards) > 1 {
			shardIdx = i % len(stream.Shards)
		}
		shardID := "shardId-000000000000"
		if len(stream.Shards) > 0 {
			shardID = stream.Shards[shardIdx].ShardID
		}

		seqNo, seqErr := generateKinesisSeqNo()
		if seqErr != nil {
			return nil, fmt.Errorf("kinesis putRecords generateSeqNo: %w", seqErr)
		}

		record := KinesisRecord{
			SequenceNumber:              seqNo,
			ApproximateArrivalTimestamp: p.tc.Now(),
			Data:                        rec.Data,
			PartitionKey:                rec.PartitionKey,
			ShardID:                     shardID,
		}

		if appendErr := p.appendRecord(ctx, body.StreamName, shardID, record); appendErr != nil {
			return nil, appendErr
		}

		results = append(results, recordResult{ShardID: shardID, SequenceNumber: seqNo})
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"FailedRecordCount": 0,
		"Records":           results,
	})
}

func (p *KinesisPlugin) getShardIterator(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName             string `json:"StreamName"`
		ShardID                string `json:"ShardId"`
		ShardIteratorType      string `json:"ShardIteratorType"`
		StartingSequenceNumber string `json:"StartingSequenceNumber"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.ShardID == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "ShardId is required", HTTPStatus: http.StatusBadRequest}
	}

	// Validate stream exists.
	if _, err := p.loadStream(ctx, body.StreamName); err != nil {
		return nil, err
	}

	startSeq := body.StartingSequenceNumber
	switch body.ShardIteratorType {
	case "TRIM_HORIZON":
		startSeq = ""
	case "LATEST":
		startSeq = "LATEST"
	}

	iter := kinesisIterator{
		StreamName: body.StreamName,
		ShardID:    body.ShardID,
		SeqNo:      startSeq,
		Type:       body.ShardIteratorType,
		Region:     ctx.Region,
		AccountID:  ctx.AccountID,
	}

	iterJSON, err := json.Marshal(iter)
	if err != nil {
		return nil, fmt.Errorf("kinesis getShardIterator marshal: %w", err)
	}

	iterB64 := base64.StdEncoding.EncodeToString(iterJSON)
	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"ShardIterator": iterB64,
	})
}

func (p *KinesisPlugin) getRecords(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ShardIterator string `json:"ShardIterator"`
		Limit         int    `json:"Limit"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.ShardIterator == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "ShardIterator is required", HTTPStatus: http.StatusBadRequest}
	}
	limit := body.Limit
	if limit <= 0 {
		limit = 100
	}

	iterJSON, err := base64.StdEncoding.DecodeString(body.ShardIterator)
	if err != nil {
		return nil, &AWSError{Code: "InvalidArgumentException", Message: "invalid shard iterator", HTTPStatus: http.StatusBadRequest}
	}

	var iter kinesisIterator
	if err := json.Unmarshal(iterJSON, &iter); err != nil {
		return nil, &AWSError{Code: "InvalidArgumentException", Message: "invalid shard iterator", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	rk := kinesisRecordKey(iter.AccountID, iter.Region, iter.StreamName, iter.ShardID)
	data, err := p.state.Get(goCtx, kinesisNamespace, rk)
	if err != nil {
		return nil, fmt.Errorf("kinesis getRecords state.Get: %w", err)
	}

	var allRecords []KinesisRecord
	if data != nil {
		if err := json.Unmarshal(data, &allRecords); err != nil {
			return nil, fmt.Errorf("kinesis getRecords unmarshal: %w", err)
		}
	}

	// Filter records after the iterator sequence number.
	var filtered []KinesisRecord
	switch iter.SeqNo {
	case "", "TRIM_HORIZON":
		filtered = allRecords
	case "LATEST":
		filtered = []KinesisRecord{}
	default:
		for _, r := range allRecords {
			if r.SequenceNumber > iter.SeqNo {
				filtered = append(filtered, r)
			}
		}
	}

	// Apply limit.
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}

	// Compute next iterator seqNo.
	nextSeqNo := iter.SeqNo
	if len(filtered) > 0 {
		nextSeqNo = filtered[len(filtered)-1].SequenceNumber
	}

	nextIter := kinesisIterator{
		StreamName: iter.StreamName,
		ShardID:    iter.ShardID,
		SeqNo:      nextSeqNo,
		Type:       "AFTER_SEQUENCE_NUMBER",
		Region:     iter.Region,
		AccountID:  iter.AccountID,
	}
	nextIterJSON, err := json.Marshal(nextIter)
	if err != nil {
		return nil, fmt.Errorf("kinesis getRecords nextIter marshal: %w", err)
	}
	nextIterB64 := base64.StdEncoding.EncodeToString(nextIterJSON)

	// Build response records (without ShardId field per API spec).
	type respRecord struct {
		SequenceNumber              string    `json:"SequenceNumber"`
		ApproximateArrivalTimestamp time.Time `json:"ApproximateArrivalTimestamp"`
		Data                        string    `json:"Data"`
		PartitionKey                string    `json:"PartitionKey"`
		EncryptionType              string    `json:"EncryptionType"`
	}
	respRecords := make([]respRecord, 0, len(filtered))
	for _, r := range filtered {
		respRecords = append(respRecords, respRecord{
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			EncryptionType:              "NONE",
		})
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"Records":            respRecords,
		"NextShardIterator":  nextIterB64,
		"MillisBehindLatest": 0,
	})
}

// --- Shard operations -------------------------------------------------------

func (p *KinesisPlugin) mergeShards(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName           string `json:"StreamName"`
		ShardToMerge         string `json:"ShardToMerge"`
		AdjacentShardToMerge string `json:"AdjacentShardToMerge"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	if stream.ShardCount > 1 {
		stream.ShardCount--
		stream.Shards = generateKinesisShards(stream.ShardCount)
	}

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) splitShard(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName         string `json:"StreamName"`
		ShardToSplit       string `json:"ShardToSplit"`
		NewStartingHashKey string `json:"NewStartingHashKey"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	stream.ShardCount++
	stream.Shards = generateKinesisShards(stream.ShardCount)

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

// --- Tag operations ---------------------------------------------------------

func (p *KinesisPlugin) addTagsToStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string            `json:"StreamName"`
		Tags       map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	if stream.Tags == nil {
		stream.Tags = make(map[string]string)
	}
	for k, v := range body.Tags {
		stream.Tags[k] = v
	}

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) removeTagsFromStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string   `json:"StreamName"`
		TagKeys    []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	for _, k := range body.TagKeys {
		delete(stream.Tags, k)
	}

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) listTagsForStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	type tagItem struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	}
	tags := make([]tagItem, 0, len(stream.Tags))
	for k, v := range stream.Tags {
		tags = append(tags, tagItem{Key: k, Value: v})
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"Tags":        tags,
		"HasMoreTags": false,
	})
}

// --- Enhanced monitoring ----------------------------------------------------

func (p *KinesisPlugin) enableEnhancedMonitoring(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName        string   `json:"StreamName"`
		ShardLevelMetrics []string `json:"ShardLevelMetrics"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	existing := map[string]struct{}{}
	for _, m := range stream.EnhancedMonitoring {
		existing[m] = struct{}{}
	}
	for _, m := range body.ShardLevelMetrics {
		if _, ok := existing[m]; !ok {
			stream.EnhancedMonitoring = append(stream.EnhancedMonitoring, m)
		}
	}

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamName":               stream.StreamName,
		"CurrentShardLevelMetrics": stream.EnhancedMonitoring,
		"DesiredShardLevelMetrics": stream.EnhancedMonitoring,
	})
}

func (p *KinesisPlugin) disableEnhancedMonitoring(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName        string   `json:"StreamName"`
		ShardLevelMetrics []string `json:"ShardLevelMetrics"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "StreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	stream, err := p.loadStream(ctx, body.StreamName)
	if err != nil {
		return nil, err
	}

	remove := map[string]struct{}{}
	for _, m := range body.ShardLevelMetrics {
		remove[m] = struct{}{}
	}
	kept := stream.EnhancedMonitoring[:0]
	for _, m := range stream.EnhancedMonitoring {
		if _, ok := remove[m]; !ok {
			kept = append(kept, m)
		}
	}
	stream.EnhancedMonitoring = kept

	if err := p.saveStream(ctx, stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamName":               stream.StreamName,
		"CurrentShardLevelMetrics": stream.EnhancedMonitoring,
		"DesiredShardLevelMetrics": stream.EnhancedMonitoring,
	})
}

// --- Helpers ----------------------------------------------------------------

// loadStream loads a KinesisStream from state, returning a ResourceNotFoundException if absent.
func (p *KinesisPlugin) loadStream(ctx *RequestContext, name string) (KinesisStream, error) {
	goCtx := context.Background()
	stateKey := kinesisStreamKey(ctx.AccountID, ctx.Region, name)
	data, err := p.state.Get(goCtx, kinesisNamespace, stateKey)
	if err != nil {
		return KinesisStream{}, fmt.Errorf("kinesis loadStream state.Get: %w", err)
	}
	if data == nil {
		return KinesisStream{}, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "Stream not found: " + name,
			HTTPStatus: http.StatusNotFound,
		}
	}
	var stream KinesisStream
	if err := json.Unmarshal(data, &stream); err != nil {
		return KinesisStream{}, fmt.Errorf("kinesis loadStream unmarshal: %w", err)
	}
	return stream, nil
}

// saveStream persists a KinesisStream to state.
func (p *KinesisPlugin) saveStream(ctx *RequestContext, stream KinesisStream) error {
	data, err := json.Marshal(stream)
	if err != nil {
		return fmt.Errorf("kinesis saveStream marshal: %w", err)
	}
	stateKey := kinesisStreamKey(ctx.AccountID, ctx.Region, stream.StreamName)
	if err := p.state.Put(context.Background(), kinesisNamespace, stateKey, data); err != nil {
		return fmt.Errorf("kinesis saveStream state.Put: %w", err)
	}
	return nil
}

// appendRecord appends a record to a shard's ring buffer, trimming to the last 10,000 entries.
func (p *KinesisPlugin) appendRecord(ctx *RequestContext, streamName, shardID string, record KinesisRecord) error {
	goCtx := context.Background()
	rk := kinesisRecordKey(ctx.AccountID, ctx.Region, streamName, shardID)

	data, err := p.state.Get(goCtx, kinesisNamespace, rk)
	if err != nil {
		return fmt.Errorf("kinesis appendRecord state.Get: %w", err)
	}

	var records []KinesisRecord
	if data != nil {
		if err := json.Unmarshal(data, &records); err != nil {
			return fmt.Errorf("kinesis appendRecord unmarshal: %w", err)
		}
	}

	records = append(records, record)
	if len(records) > kinesisMaxRecordsPerShard {
		records = records[len(records)-kinesisMaxRecordsPerShard:]
	}

	newData, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("kinesis appendRecord marshal: %w", err)
	}
	if err := p.state.Put(goCtx, kinesisNamespace, rk, newData); err != nil {
		return fmt.Errorf("kinesis appendRecord state.Put: %w", err)
	}
	return nil
}

// buildStreamDescription builds the common stream description map used by
// DescribeStream and DescribeStreamSummary.
func buildStreamDescription(stream KinesisStream) map[string]interface{} {
	return map[string]interface{}{
		"StreamName":              stream.StreamName,
		"StreamARN":               stream.StreamArn,
		"StreamStatus":            stream.StreamStatus,
		"Shards":                  stream.Shards,
		"HasMoreShards":           false,
		"RetentionPeriodHours":    stream.RetentionPeriodHours,
		"StreamCreationTimestamp": stream.CreatedAt.Unix(),
		"EnhancedMonitoring":      stream.EnhancedMonitoring,
		"OpenShardCount":          stream.ShardCount,
	}
}

// generateKinesisShards creates n evenly-partitioned KinesisShard descriptors.
func generateKinesisShards(n int) []KinesisShard {
	shards := make([]KinesisShard, n)
	for i := range shards {
		shards[i] = KinesisShard{
			ShardID: fmt.Sprintf("shardId-%012d", i),
		}
		shards[i].HashKeyRange.StartingHashKey = fmt.Sprintf("%d", i*1000)
		shards[i].HashKeyRange.EndingHashKey = fmt.Sprintf("%d", (i+1)*1000-1)
		shards[i].SequenceNumberRange.StartingSequenceNumber = "0"
	}
	return shards
}

// generateKinesisSeqNo generates a unique Kinesis sequence number.
func generateKinesisSeqNo() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(99999999))
	if err != nil {
		return "", fmt.Errorf("generateKinesisSeqNo rand: %w", err)
	}
	return fmt.Sprintf("%d-%08d", time.Now().UnixNano(), n.Int64()), nil
}

// kinesisIterator is the internal structure encoded into a shard iterator token.
type kinesisIterator struct {
	// StreamName is the name of the stream.
	StreamName string `json:"s"`
	// ShardID is the shard identifier.
	ShardID string `json:"i"`
	// SeqNo is the starting sequence number.
	SeqNo string `json:"n"`
	// Type is the iterator type.
	Type string `json:"t"`
	// Region is the AWS region.
	Region string `json:"r"`
	// AccountID is the AWS account ID.
	AccountID string `json:"a"`
}

// kinesisJSONResponse marshals v as JSON and returns an AWSResponse with
// Content-Type: application/x-amz-json-1.1 and the given HTTP status code.
func kinesisJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("kinesisJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
