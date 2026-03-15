package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// FirehosePlugin emulates the Amazon Kinesis Data Firehose service.
// It handles delivery stream CRUD operations and stub record ingestion
// using the Firehose JSON-target protocol (X-Amz-Target: Firehose_20150804.{Op}).
type FirehosePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "firehose".
func (p *FirehosePlugin) Name() string { return firehoseNamespace }

// Initialize sets up the FirehosePlugin with the provided configuration.
func (p *FirehosePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for FirehosePlugin.
func (p *FirehosePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Firehose JSON-target request to the appropriate handler.
func (p *FirehosePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateDeliveryStream":
		return p.createDeliveryStream(ctx, req)
	case "DescribeDeliveryStream":
		return p.describeDeliveryStream(ctx, req)
	case "ListDeliveryStreams":
		return p.listDeliveryStreams(ctx, req)
	case "PutRecord":
		return p.putRecord(ctx, req)
	case "PutRecordBatch":
		return p.putRecordBatch(ctx, req)
	case "DeleteDeliveryStream":
		return p.deleteDeliveryStream(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "FirehosePlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *FirehosePlugin) createDeliveryStream(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DeliveryStreamName string            `json:"DeliveryStreamName"`
		DeliveryStreamType string            `json:"DeliveryStreamType"`
		Tags               map[string]string `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.DeliveryStreamName == "" {
		return nil, &AWSError{Code: "InvalidArgumentException", Message: "DeliveryStreamName is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.DeliveryStreamType == "" {
		input.DeliveryStreamType = "DirectPut"
	}

	goCtx := context.Background()
	key := "stream:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.DeliveryStreamName
	existing, err := p.state.Get(goCtx, firehoseNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("firehose createDeliveryStream get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceInUseException", Message: "Delivery stream " + input.DeliveryStreamName + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/%s", reqCtx.Region, reqCtx.AccountID, input.DeliveryStreamName)

	stream := FirehoseDeliveryStream{
		DeliveryStreamName:   input.DeliveryStreamName,
		DeliveryStreamARN:    arn,
		DeliveryStreamStatus: "ACTIVE",
		DeliveryStreamType:   input.DeliveryStreamType,
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
		Tags:                 input.Tags,
		CreatedAt:            p.tc.Now(),
	}

	data, err := json.Marshal(stream)
	if err != nil {
		return nil, fmt.Errorf("firehose createDeliveryStream marshal: %w", err)
	}
	if err := p.state.Put(goCtx, firehoseNamespace, key, data); err != nil {
		return nil, fmt.Errorf("firehose createDeliveryStream put: %w", err)
	}
	updateStringIndex(goCtx, p.state, firehoseNamespace, "stream_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.DeliveryStreamName)

	return firehoseJSONResponse(http.StatusOK, map[string]string{
		"DeliveryStreamARN": arn,
	})
}

func (p *FirehosePlugin) describeDeliveryStream(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DeliveryStreamName string `json:"DeliveryStreamName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.DeliveryStreamName == "" {
		return nil, &AWSError{Code: "InvalidArgumentException", Message: "DeliveryStreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := "stream:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.DeliveryStreamName
	data, err := p.state.Get(goCtx, firehoseNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("firehose describeDeliveryStream get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Delivery stream " + input.DeliveryStreamName + " does not exist.", HTTPStatus: http.StatusBadRequest}
	}
	var stream FirehoseDeliveryStream
	if err := json.Unmarshal(data, &stream); err != nil {
		return nil, fmt.Errorf("firehose describeDeliveryStream unmarshal: %w", err)
	}
	return firehoseJSONResponse(http.StatusOK, map[string]interface{}{
		"DeliveryStreamDescription": stream,
	})
}

func (p *FirehosePlugin) listDeliveryStreams(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Limit                            int    `json:"Limit"`
		ExclusiveStartDeliveryStreamName string `json:"ExclusiveStartDeliveryStreamName"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, firehoseNamespace, "stream_names:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("firehose listDeliveryStreams load index: %w", err)
	}

	// Apply cursor pagination.
	startIdx := 0
	if input.ExclusiveStartDeliveryStreamName != "" {
		for i, n := range names {
			if n == input.ExclusiveStartDeliveryStreamName {
				startIdx = i + 1
				break
			}
		}
	}
	page := names[startIdx:]
	hasMore := false
	if len(page) > limit {
		hasMore = true
		page = page[:limit]
	}
	if page == nil {
		page = []string{}
	}

	return firehoseJSONResponse(http.StatusOK, map[string]interface{}{
		"DeliveryStreamNames":    page,
		"HasMoreDeliveryStreams": hasMore,
	})
}

func (p *FirehosePlugin) putRecord(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	recordID := fmt.Sprintf("rec-%d", p.tc.Now().UnixNano())
	return firehoseJSONResponse(http.StatusOK, map[string]interface{}{
		"RecordId":  recordID,
		"Encrypted": false,
	})
}

func (p *FirehosePlugin) putRecordBatch(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Records []interface{} `json:"Records"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	nano := p.tc.Now().UnixNano()
	responses := make([]map[string]string, len(input.Records))
	for i := range input.Records {
		responses[i] = map[string]string{
			"RecordId": fmt.Sprintf("rec-%d-%d", i, nano),
		}
	}
	if len(responses) == 0 {
		responses = []map[string]string{}
	}

	return firehoseJSONResponse(http.StatusOK, map[string]interface{}{
		"FailedPutCount":   0,
		"RequestResponses": responses,
	})
}

func (p *FirehosePlugin) deleteDeliveryStream(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DeliveryStreamName string `json:"DeliveryStreamName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.DeliveryStreamName == "" {
		return nil, &AWSError{Code: "InvalidArgumentException", Message: "DeliveryStreamName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := "stream:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + input.DeliveryStreamName

	existing, err := p.state.Get(goCtx, firehoseNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("firehose deleteDeliveryStream get: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Delivery stream " + input.DeliveryStreamName + " does not exist.", HTTPStatus: http.StatusBadRequest}
	}

	if err := p.state.Delete(goCtx, firehoseNamespace, key); err != nil {
		return nil, fmt.Errorf("firehose deleteDeliveryStream delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, firehoseNamespace, "stream_names:"+reqCtx.AccountID+"/"+reqCtx.Region, input.DeliveryStreamName)

	return firehoseJSONResponse(http.StatusOK, map[string]interface{}{})
}

// firehoseJSONResponse serialises v to JSON and returns an AWSResponse.
func firehoseJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("firehose json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
