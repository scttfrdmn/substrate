package emulator

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
		return nil, unknownActionError(p.Name(), op)
	}
}

// --- Stream lifecycle -------------------------------------------------------

func (p *KinesisPlugin) createStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamName string `json:"StreamName"`
		ShardCount int    `json:"ShardCount"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	if body.StreamName == "" {
		return nil, kinesisInvalidArgument("StreamName is required")
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
		// 400, not the 409 substrate answered: API_CreateStream publishes ResourceInUseException at
		// 400 like every other error in the service. See [kinesisStreamNotFound].
		return nil, &AWSError{
			Code:       "ResourceInUseException",
			Message:    "Stream already exists: " + body.StreamName,
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// CreateStream is the one operation that mints an ARN rather than resolving one, so the caller's
	// own account and Region are the right source here and the only place in the file they are.
	target := kinesisStreamTarget{AccountID: ctx.AccountID, Region: ctx.Region, Name: body.StreamName}
	streamARN := kinesisStreamARN(target)
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
		kinesisStreamRef
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}

	// Every key removed is derived from the resolved target, so a delete through a cross-account ARN
	// removes that account's records and that account's index entry rather than the caller's.
	goCtx := context.Background()
	for _, shard := range stream.Shards {
		rk := kinesisRecordKey(target.AccountID, target.Region, target.Name, shard.ShardID)
		_ = p.state.Delete(goCtx, kinesisNamespace, rk)
	}

	stateKey := kinesisStreamKey(target.AccountID, target.Region, target.Name)
	if err := p.state.Delete(goCtx, kinesisNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("kinesis deleteStream state.Delete: %w", err)
	}

	idxKey := kinesisStreamNamesKey(target.AccountID, target.Region)
	removeFromStringIndex(goCtx, p.state, kinesisNamespace, idxKey, target.Name)

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) describeStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
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
		kinesisStreamRef
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
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
		kinesisStreamRef
		TargetShardCount int    `json:"TargetShardCount"`
		ScalingType      string `json:"ScalingType"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}

	current := stream.ShardCount
	stream.ShardCount = body.TargetShardCount
	stream.Shards = generateKinesisShards(body.TargetShardCount)
	stream.StreamStatus = "ACTIVE"

	if err := p.saveStream(stream); err != nil {
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
		kinesisStreamRef
		Data         string `json:"Data"`
		PartitionKey string `json:"PartitionKey"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
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

	if err := p.appendRecord(target, shardID, record); err != nil {
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
		kinesisStreamRef
		Records []struct {
			Data         string `json:"Data"`
			PartitionKey string `json:"PartitionKey"`
		} `json:"Records"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
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

		if appendErr := p.appendRecord(target, shardID, record); appendErr != nil {
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
		kinesisStreamRef
		ShardID                string `json:"ShardId"`
		ShardIteratorType      string `json:"ShardIteratorType"`
		StartingSequenceNumber string `json:"StartingSequenceNumber"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}
	if body.ShardID == "" {
		return nil, kinesisInvalidArgument("ShardId is required")
	}

	// Validate stream exists.
	if _, err := p.loadStream(target); err != nil {
		return nil, err
	}

	startSeq := body.StartingSequenceNumber
	switch body.ShardIteratorType {
	case "TRIM_HORIZON":
		startSeq = ""
	case "LATEST":
		startSeq = "LATEST"
	}

	// The token carries the resolved target, which is why GetRecords needs no request context of its
	// own: an iterator minted against a cross-account ARN keeps reading that account's shard however
	// the caller is signed. That arrangement predates #966 — it is what the iterator's Region and
	// AccountID members were always for — and it is the in-file precedent the rest of the plugin has
	// now been brought into line with.
	iter := kinesisIterator{
		StreamName: target.Name,
		ShardID:    body.ShardID,
		SeqNo:      startSeq,
		Type:       body.ShardIteratorType,
		Region:     target.Region,
		AccountID:  target.AccountID,
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
		// API_GetRecords is the one page that publishes StreamARN and **no StreamName** — its stream
		// is implied by ShardIterator, so the ARN is Required: No and redundant. It is decoded
		// anyway, because the SDKs send it whenever their client was constructed from an ARN, and
		// because ignoring it would be the very failure #966 is about: a request naming stream B
		// while its iterator reads stream A would silently be served A's records.
		StreamARN string `json:"StreamARN"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	if body.ShardIterator == "" {
		return nil, kinesisInvalidArgument("ShardIterator is required")
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

	// The iterator is authoritative — it is the required member and it carries the account and Region
	// the stream was resolved in. A supplied StreamARN is checked against it rather than used, and a
	// disagreement is refused on the same reading [kinesisResolveStream] records: all three segments
	// are compared here, because both sides of this comparison are fully qualified.
	iterTarget := kinesisStreamTarget{AccountID: iter.AccountID, Region: iter.Region, Name: iter.StreamName}
	if body.StreamARN != "" {
		argTarget, refErr := kinesisParseStreamARN(body.StreamARN)
		if refErr != nil {
			return nil, refErr
		}
		if argTarget != iterTarget {
			return nil, &AWSError{
				Code: "InvalidArgumentException",
				Message: fmt.Sprintf("StreamARN %q names a different stream than ShardIterator, which reads %q",
					body.StreamARN, kinesisStreamARN(iterTarget)),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}

	goCtx := context.Background()
	rk := kinesisRecordKey(iterTarget.AccountID, iterTarget.Region, iterTarget.Name, iter.ShardID)
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
		kinesisStreamRef
		ShardToMerge         string `json:"ShardToMerge"`
		AdjacentShardToMerge string `json:"AdjacentShardToMerge"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}

	if stream.ShardCount > 1 {
		stream.ShardCount--
		stream.Shards = generateKinesisShards(stream.ShardCount)
	}

	if err := p.saveStream(stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) splitShard(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		ShardToSplit       string `json:"ShardToSplit"`
		NewStartingHashKey string `json:"NewStartingHashKey"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}

	stream.ShardCount++
	stream.Shards = generateKinesisShards(stream.ShardCount)

	if err := p.saveStream(stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

// --- Tag operations ---------------------------------------------------------

func (p *KinesisPlugin) addTagsToStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	// The request's own shape is checked before the stream is loaded, matching listTagsForStream's
	// ordering: a parameter refusal does not depend on the stream existing.
	if tagErr := kinesisValidateTagMap(body.Tags); tagErr != nil {
		return nil, tagErr
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}

	// The quota is a property of the stream, so it needs the stored set and is checked here rather than
	// above — and before the merge, so a refused request writes nothing (#965).
	if quotaErr := kinesisCheckTagQuota(target, stream.Tags, body.Tags); quotaErr != nil {
		return nil, quotaErr
	}

	if stream.Tags == nil {
		stream.Tags = make(map[string]string)
	}
	for k, v := range body.Tags {
		stream.Tags[k] = v
	}

	if err := p.saveStream(stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) removeTagsFromStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		TagKeys []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}

	for _, k := range body.TagKeys {
		delete(stream.Tags, k)
	}

	if err := p.saveStream(stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

// kinesisMinListTagsLimit and kinesisMaxListTagsLimit bound ListTagsForStream's Limit, whose published
// Valid Range is "Minimum value of 1. Maximum value of 50".
//
// AWS's own numbers do not agree about how large the set being paged can get: the response's Tags array
// publishes "Maximum number of 200 items" and AddTagsToStream's Tags map publishes the same 200, while
// both operations' prose caps a stream at 50 tags ("you can assign up to 50 tags to a data stream",
// "you can add up to 50 tags per resource") — exactly Limit's maximum. Both figures are enforced, at
// different codes and against different things, which is what makes them consistent rather than
// contradictory; see [kinesisMaxTagsPerStream] and its file's preamble. A stream's tag set therefore
// cannot exceed 50 through this service's own operations, so one maximum-Limit page holds all of it and
// the cursor matters only for a smaller Limit.
const (
	kinesisMinListTagsLimit = 1
	kinesisMaxListTagsLimit = 50
)

// kinesisMaxTagKeyLength is the published maximum length of ExclusiveStartTagKey, which is also the
// maximum length of a tag key itself.
const kinesisMaxTagKeyLength = 128

func (p *KinesisPlugin) listTagsForStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		ExclusiveStartTagKey string `json:"ExclusiveStartTagKey"`
		// A pointer, because Limit's absence and a Limit of 0 mean different things: absent returns
		// every tag, and 0 is outside the published 1–50 range and is refused. Decoding into an int
		// would collapse the two and make the refusal below unreachable.
		Limit *int `json:"Limit"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}
	// InvalidArgumentException/400 is one of this operation's four published errors and its description
	// is exactly this case: "A specified parameter exceeds its restrictions, is not supported, or can't
	// be used." The two restrictions the request model states are Limit's 1–50 range and
	// ExclusiveStartTagKey's 1–128 length, so both are refused under it. It is also what
	// [kinesisResolveStream] answers, so this operation's two parameter refusals and its stream
	// resolution agree on one code — and, since #950 corrected the nineteen sites that answered a code
	// Kinesis publishes nowhere at all, so does every other refusal in the plugin. See
	// [kinesisInvalidBody].
	if body.Limit != nil && (*body.Limit < kinesisMinListTagsLimit || *body.Limit > kinesisMaxListTagsLimit) {
		return nil, &AWSError{
			Code: "InvalidArgumentException",
			Message: fmt.Sprintf("Limit must be between %d and %d, inclusive; got %d",
				kinesisMinListTagsLimit, kinesisMaxListTagsLimit, *body.Limit),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if len(body.ExclusiveStartTagKey) > kinesisMaxTagKeyLength {
		return nil, &AWSError{
			Code: "InvalidArgumentException",
			Message: fmt.Sprintf("ExclusiveStartTagKey must be at most %d characters; got %d",
				kinesisMaxTagKeyLength, len(body.ExclusiveStartTagKey)),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	stream, err := p.loadStream(target)
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
	// Ranging the map put Go's map order into the Tags list, so two identical calls could report one
	// stream's tags in a different order (#946).
	//
	// Kinesis is the one of #946's five whose page does not leave the order entirely open: it
	// publishes a cursor over the tag key itself. ExclusiveStartTagKey is "the key to use as the
	// starting point for the list of tags. If this parameter is set, ListTagsForStream gets all tags
	// that occur after ExclusiveStartTagKey", and Tags is "a list of tags associated with StreamName,
	// starting with the first tag after ExclusiveStartTagKey and up to the specified Limit". A tag
	// cannot "occur after" a key unless the tags are walked in some order over keys, so an order is
	// implied even though no sentence names one — the same reasoning that made ListObjectVersions'
	// order cursor-implied in #865. Which order is still substrate's reading, and lexicographic is
	// taken for the reason [sortTagsByKey] records; AWS's own sample response is not sorted.
	//
	// The sort is the cursor's prerequisite rather than a separate nicety, which is why it landed
	// first: a cursor over an unstable order can skip a tag or report it twice (#954).
	sortTagsByKey(tags, func(t tagItem) string { return t.Key })

	// "Gets all tags that occur after ExclusiveStartTagKey" — strictly after, so the named key is
	// excluded and a caller that passes back the last key it received advances rather than repeating.
	// An empty string is read as absent, not as a min-length-1 violation: it is what an omitted member
	// decodes to and what a caller starting the walk sends, and no tag key can sort before it anyway.
	if body.ExclusiveStartTagKey != "" {
		cut := len(tags)
		for i, t := range tags {
			if t.Key > body.ExclusiveStartTagKey {
				cut = i
				break
			}
		}
		tags = tags[cut:]
	}

	// HasMoreTags is "true exactly when tags were withheld", which is substrate's reading of two AWS
	// sentences that do not agree. Limit's says HasMoreTags is set "if this number is less than the
	// total number of tags associated with the stream" — read literally, a walk at Limit 2 over six
	// tags would report true on every page including the last, and the loop AWS itself describes
	// ("to list additional tags, set ExclusiveStartTagKey to the last key in the response") would never
	// terminate. HasMoreTags' own description is the coherent one: "if set to true, more tags are
	// available", i.e. more remain after this page. That is what is implemented.
	hasMore := false
	if body.Limit != nil && len(tags) > *body.Limit {
		tags = tags[:*body.Limit]
		hasMore = true
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"Tags":        tags,
		"HasMoreTags": hasMore,
	})
}

// --- Enhanced monitoring ----------------------------------------------------

func (p *KinesisPlugin) enableEnhancedMonitoring(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		ShardLevelMetrics []string `json:"ShardLevelMetrics"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
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

	if err := p.saveStream(stream); err != nil {
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
		kinesisStreamRef
		ShardLevelMetrics []string `json:"ShardLevelMetrics"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}

	stream, err := p.loadStream(target)
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

	if err := p.saveStream(stream); err != nil {
		return nil, err
	}

	return kinesisJSONResponse(http.StatusOK, map[string]interface{}{
		"StreamName":               stream.StreamName,
		"CurrentShardLevelMetrics": stream.EnhancedMonitoring,
		"DesiredShardLevelMetrics": stream.EnhancedMonitoring,
	})
}

// --- Helpers ----------------------------------------------------------------

// loadStream loads the stream a resolved target names, returning ResourceNotFoundException/400 if
// absent.
//
// It takes a [kinesisStreamTarget] rather than a *RequestContext and a name, which is the structural
// half of #966: there is no request context in scope here to locate a stream with, so a handler
// cannot reach the caller's own account and Region by accident. The compiler enforces what a
// convention would only ask for.
func (p *KinesisPlugin) loadStream(target kinesisStreamTarget) (KinesisStream, error) {
	goCtx := context.Background()
	stateKey := kinesisStreamKey(target.AccountID, target.Region, target.Name)
	data, err := p.state.Get(goCtx, kinesisNamespace, stateKey)
	if err != nil {
		return KinesisStream{}, fmt.Errorf("kinesis loadStream state.Get: %w", err)
	}
	if data == nil {
		return KinesisStream{}, kinesisStreamNotFound(target)
	}
	var stream KinesisStream
	if err := json.Unmarshal(data, &stream); err != nil {
		return KinesisStream{}, fmt.Errorf("kinesis loadStream unmarshal: %w", err)
	}
	return stream, nil
}

// saveStream persists a KinesisStream to state.
//
// The key comes from the record's own AccountID and Region, which [createStream] is the only writer
// of, so a stream is written back where it was read from however the caller that modified it was
// signed. Reading them off a *RequestContext is what let a cross-account UpdateShardCount, MergeShards
// or AddTagsToStream copy another account's stream into the caller's own namespace (#966).
func (p *KinesisPlugin) saveStream(stream KinesisStream) error {
	data, err := json.Marshal(stream)
	if err != nil {
		return fmt.Errorf("kinesis saveStream marshal: %w", err)
	}
	stateKey := kinesisStreamKey(stream.AccountID, stream.Region, stream.StreamName)
	if err := p.state.Put(context.Background(), kinesisNamespace, stateKey, data); err != nil {
		return fmt.Errorf("kinesis saveStream state.Put: %w", err)
	}
	return nil
}

// appendRecord appends a record to a shard's ring buffer, trimming to the last 10,000 entries.
func (p *KinesisPlugin) appendRecord(target kinesisStreamTarget, shardID string, record KinesisRecord) error {
	goCtx := context.Background()
	rk := kinesisRecordKey(target.AccountID, target.Region, target.Name, shardID)

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
