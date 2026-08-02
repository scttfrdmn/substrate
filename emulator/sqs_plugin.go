package emulator

import (
	"context"
	"crypto/md5" //nolint:gosec // SQS MD5OfBody is defined by the protocol; not used for security.
	"crypto/rand"
	"crypto/sha256" //nolint:gosec // SHA-256 used for content-based deduplication; not for security.
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SQSPlugin emulates the Amazon Simple Queue Service (SQS) API.
// It supports both the query protocol (application/x-www-form-urlencoded) and
// the JSON protocol (application/x-amz-json-1.0 with X-Amz-Target header).
// Handled operations: CreateQueue, DeleteQueue, GetQueueUrl, GetQueueAttributes,
// SetQueueAttributes, ListQueues, TagQueue, UntagQueue, ListQueueTags,
// SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage,
// DeleteMessageBatch, ChangeMessageVisibility, and PurgeQueue.
type SQSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController

	// queueMu serializes consumption of a seeded consistency window per queue,
	// since decrementing the miss counter is a read-modify-write and StateManager
	// offers no compare-and-swap. See [sqsQueueMutex].
	queueMu sqsQueueMutex
}

// Name returns the service name "sqs".
func (p *SQSPlugin) Name() string { return "sqs" }

// Initialize sets up the SQSPlugin with the provided configuration.
func (p *SQSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SQSPlugin.
func (p *SQSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an SQS query-protocol request to the appropriate handler.
func (p *SQSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateQueue":
		return p.createQueue(ctx, req)
	case "GetQueueUrl":
		return p.getQueueURL(ctx, req)
	case "GetQueueAttributes":
		return p.getQueueAttributes(ctx, req)
	case "SetQueueAttributes":
		return p.setQueueAttributes(ctx, req)
	case "DeleteQueue":
		return p.deleteQueue(ctx, req)
	case "ListQueues":
		return p.listQueues(ctx, req)
	case "TagQueue":
		return p.tagQueue(ctx, req)
	case "UntagQueue":
		return p.untagQueue(ctx, req)
	case "ListQueueTags":
		return p.listQueueTags(ctx, req)
	case "SendMessage":
		return p.sendMessage(ctx, req)
	case "SendMessageBatch":
		return p.sendMessageBatch(ctx, req)
	case "ReceiveMessage":
		return p.receiveMessage(ctx, req)
	case "DeleteMessage":
		return p.deleteMessage(ctx, req)
	case "DeleteMessageBatch":
		return p.deleteMessageBatch(ctx, req)
	case "ChangeMessageVisibility":
		return p.changeMessageVisibility(ctx, req)
	case "PurgeQueue":
		return p.purgeQueue(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("The action %s is not valid for this endpoint.", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Queue URL helpers -------------------------------------------------------

// sqsURLKey returns a stable state key component for a queue URL.
func sqsURLKey(queueURL string) string {
	// Use the last two path components (accountID/queueName) as the key.
	parts := strings.Split(strings.TrimRight(queueURL, "/"), "/")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "/" + parts[len(parts)-1]
	}
	return queueURL
}

// sqsIsJSONProtocol reports whether req was sent using the SQS JSON protocol
// (Content-Type: application/x-amz-json-1.0 with X-Amz-Target header).
func sqsIsJSONProtocol(req *AWSRequest) bool {
	return strings.HasPrefix(req.Headers["Content-Type"], "application/x-amz-json")
}

// sqsJSONResponse marshals v as JSON and returns an AWSResponse with
// Content-Type: application/x-amz-json-1.0 and the given HTTP status code.
func sqsJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sqsJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
		Body:       body,
	}, nil
}

// sqsQueueURLFromRequest extracts the QueueUrl from the request, supporting
// both query protocol (Params["QueueUrl"]) and JSON protocol (body field "QueueUrl").
func sqsQueueURLFromRequest(req *AWSRequest) string {
	if sqsIsJSONProtocol(req) {
		var input struct {
			QueueURL string `json:"QueueUrl"`
		}
		_ = json.Unmarshal(req.Body, &input)
		return input.QueueURL
	}
	return req.Params["QueueUrl"]
}

// --- State helpers -----------------------------------------------------------

func (p *SQSPlugin) loadQueue(ctx context.Context, queueURL string) (*SQSQueue, error) {
	key := "queue:" + sqsURLKey(queueURL)
	data, err := p.state.Get(ctx, sqsNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("sqs loadQueue state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var q SQSQueue
	if err := json.Unmarshal(data, &q); err != nil {
		return nil, fmt.Errorf("sqs loadQueue unmarshal: %w", err)
	}
	return &q, nil
}

func (p *SQSPlugin) saveQueue(ctx context.Context, q *SQSQueue) error {
	key := "queue:" + sqsURLKey(q.QueueURL)
	data, err := json.Marshal(q)
	if err != nil {
		return fmt.Errorf("sqs saveQueue marshal: %w", err)
	}
	return p.state.Put(ctx, sqsNamespace, key, data)
}

func (p *SQSPlugin) loadQueueNames(ctx context.Context) ([]string, error) {
	data, err := p.state.Get(ctx, sqsNamespace, "queue_names")
	if err != nil {
		return nil, fmt.Errorf("sqs loadQueueNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("sqs loadQueueNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *SQSPlugin) saveQueueNames(ctx context.Context, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("sqs saveQueueNames marshal: %w", err)
	}
	return p.state.Put(ctx, sqsNamespace, "queue_names", data)
}

func (p *SQSPlugin) loadMsgIDs(ctx context.Context, urlKey string) ([]string, error) {
	data, err := p.state.Get(ctx, sqsNamespace, "msg_ids:"+urlKey)
	if err != nil {
		return nil, fmt.Errorf("sqs loadMsgIDs: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("sqs loadMsgIDs unmarshal: %w", err)
	}
	return ids, nil
}

func (p *SQSPlugin) saveMsgIDs(ctx context.Context, urlKey string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("sqs saveMsgIDs marshal: %w", err)
	}
	return p.state.Put(ctx, sqsNamespace, "msg_ids:"+urlKey, data)
}

func (p *SQSPlugin) loadMsg(ctx context.Context, urlKey, msgID string) (*SQSMessage, error) {
	data, err := p.state.Get(ctx, sqsNamespace, "msg:"+urlKey+":"+msgID)
	if err != nil {
		return nil, fmt.Errorf("sqs loadMsg: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var msg SQSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("sqs loadMsg unmarshal: %w", err)
	}
	return &msg, nil
}

func (p *SQSPlugin) saveMsg(ctx context.Context, urlKey string, msg *SQSMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("sqs saveMsg marshal: %w", err)
	}
	return p.state.Put(ctx, sqsNamespace, "msg:"+urlKey+":"+msg.MessageID, data)
}

func (p *SQSPlugin) deleteMsg(ctx context.Context, urlKey, msgID string) error {
	return p.state.Delete(ctx, sqsNamespace, "msg:"+urlKey+":"+msgID)
}

// --- Queue operations --------------------------------------------------------

func (p *SQSPlugin) createQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var name string
	var attrs map[string]string
	if sqsIsJSONProtocol(req) {
		var input struct {
			QueueName  string            `json:"QueueName"`
			Attributes map[string]string `json:"Attributes"`
		}
		_ = json.Unmarshal(req.Body, &input)
		name, attrs = input.QueueName, input.Attributes
		if attrs == nil {
			attrs = make(map[string]string)
		}
	} else {
		name = req.Params["QueueName"]
		attrs = parseSQSAttributes(req.Params)
	}
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "QueueName is required", HTTPStatus: http.StatusBadRequest}
	}
	isFifo := strings.HasSuffix(name, ".fifo")

	queueURL := sqsQueueURL(ctx.Region, ctx.AccountID, name)
	existing, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}

	// A seeded QueueDeletedRecently is checked before the existence branch, unlike
	// the lookup windows, which require the queue to exist. AWS raises this only
	// when the name is free but too recently freed, so the absent case is the only
	// one where it can apply — the seed is consulted here, and only here, for that
	// reason. It is unseeded by default, so nothing changes without a seed.
	if existing == nil {
		if miss, missErr := p.consumeQueueMiss(name, sqsConsistencyDeletedRecently); missErr != nil {
			return nil, missErr
		} else if miss {
			return nil, sqsQueueDeletedRecently()
		}
	}
	if existing != nil {
		// Same name with differing attribute values is an error, not an idempotent
		// hit (#429). AWS scopes this to "attributes whose values differ from those
		// of the existing queue", so only what the request names is compared and the
		// same-name-same-attributes case stays idempotent below.
		if conflict := sqsConflictingAttribute(existing, attrs); conflict != "" {
			return nil, sqsQueueNameExists(conflict)
		}

		// Idempotent — return existing URL.
		if sqsIsJSONProtocol(req) {
			return sqsJSONResponse(http.StatusOK, map[string]string{"QueueUrl": existing.QueueURL})
		}
		type result struct {
			QueueURL string `xml:"QueueUrl"`
		}
		type response struct {
			XMLName           xml.Name         `xml:"CreateQueueResponse"`
			Xmlns             string           `xml:"xmlns,attr"`
			CreateQueueResult result           `xml:"CreateQueueResult"`
			ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
		}
		return sqsXMLResponse(http.StatusOK, response{
			Xmlns:             "http://queue.amazonaws.com/doc/2012-11-05/",
			CreateQueueResult: result{QueueURL: existing.QueueURL},
			ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
		})
	}

	now := p.tc.Now().Unix()

	q := &SQSQueue{
		QueueName:             name,
		QueueURL:              queueURL,
		QueueARN:              sqsQueueARN(ctx.Region, ctx.AccountID, name),
		Attributes:            attrs,
		Tags:                  make(map[string]string),
		CreatedTimestamp:      now,
		LastModifiedTimestamp: now,
		FifoQueue:             isFifo,
	}

	if err := p.saveQueue(context.Background(), q); err != nil {
		return nil, fmt.Errorf("sqs createQueue saveQueue: %w", err)
	}

	// Update queue names list.
	names, err := p.loadQueueNames(context.Background())
	if err != nil {
		return nil, err
	}
	names = append(names, queueURL)
	if err := p.saveQueueNames(context.Background(), names); err != nil {
		return nil, fmt.Errorf("sqs createQueue saveQueueNames: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, map[string]string{"QueueUrl": queueURL})
	}
	type result struct {
		QueueURL string `xml:"QueueUrl"`
	}
	type response struct {
		XMLName           xml.Name         `xml:"CreateQueueResponse"`
		Xmlns             string           `xml:"xmlns,attr"`
		CreateQueueResult result           `xml:"CreateQueueResult"`
		ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:             "http://queue.amazonaws.com/doc/2012-11-05/",
		CreateQueueResult: result{QueueURL: queueURL},
		ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) getQueueURL(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var name string
	if sqsIsJSONProtocol(req) {
		var input struct {
			QueueName string `json:"QueueName"`
		}
		_ = json.Unmarshal(req.Body, &input)
		name = input.QueueName
	} else {
		name = req.Params["QueueName"]
	}
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "QueueName is required", HTTPStatus: http.StatusBadRequest}
	}
	queueURL := sqsQueueURL(ctx.Region, ctx.AccountID, name)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}
	// The queue exists, so a seeded create→lookup window may still hide it (#413).
	// Checked only after the existence test: consuming a miss for a genuinely
	// absent queue would burn budget the test meant to spend on the window.
	if miss, err := p.consumeQueueMiss(name, sqsConsistencyGetURL); err != nil {
		return nil, err
	} else if miss {
		return nil, sqsQueueDoesNotExist()
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, map[string]string{"QueueUrl": q.QueueURL})
	}
	type result struct {
		QueueURL string `xml:"QueueUrl"`
	}
	type response struct {
		XMLName           xml.Name         `xml:"GetQueueUrlResponse"`
		Xmlns             string           `xml:"xmlns,attr"`
		GetQueueURLResult result           `xml:"GetQueueUrlResult"`
		ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:             "http://queue.amazonaws.com/doc/2012-11-05/",
		GetQueueURLResult: result{QueueURL: q.QueueURL},
		ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) getQueueAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}
	// As in getQueueURL: only after the queue is known to exist. Seeds are keyed by
	// name, so the name is recovered from the URL this operation identifies it by.
	if miss, err := p.consumeQueueMiss(sqsQueueNameFromURL(queueURL), sqsConsistencyGetAttributes); err != nil {
		return nil, err
	} else if miss {
		return nil, sqsQueueDoesNotExist()
	}

	// Build standard attributes.
	attrs := map[string]string{
		"QueueArn":                      q.QueueARN,
		"CreatedTimestamp":              strconv.FormatInt(q.CreatedTimestamp, 10),
		"LastModifiedTimestamp":         strconv.FormatInt(q.LastModifiedTimestamp, 10),
		"VisibilityTimeout":             getAttrOrDefault(q.Attributes, "VisibilityTimeout", "30"),
		"MaximumMessageSize":            getAttrOrDefault(q.Attributes, "MaximumMessageSize", sqsDefaultMaximumMessageSize),
		"MessageRetentionPeriod":        getAttrOrDefault(q.Attributes, "MessageRetentionPeriod", "345600"),
		"DelaySeconds":                  getAttrOrDefault(q.Attributes, "DelaySeconds", "0"),
		"ReceiveMessageWaitTimeSeconds": getAttrOrDefault(q.Attributes, "ReceiveMessageWaitTimeSeconds", "0"),
	}
	for k, v := range q.Attributes {
		attrs[k] = v
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{"Attributes": attrs})
	}

	type attrEntry struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type result struct {
		Attribute []attrEntry `xml:"Attribute"`
	}
	type response struct {
		XMLName                  xml.Name         `xml:"GetQueueAttributesResponse"`
		Xmlns                    string           `xml:"xmlns,attr"`
		GetQueueAttributesResult result           `xml:"GetQueueAttributesResult"`
		ResponseMetadata         responseMetadata `xml:"ResponseMetadata"`
	}

	attrList := make([]attrEntry, 0, len(attrs))
	for k, v := range attrs {
		attrList = append(attrList, attrEntry{Name: k, Value: v})
	}
	sort.Slice(attrList, func(i, j int) bool { return attrList[i].Name < attrList[j].Name })

	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                    "http://queue.amazonaws.com/doc/2012-11-05/",
		GetQueueAttributesResult: result{Attribute: attrList},
		ResponseMetadata:         responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) setQueueAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	var attrs map[string]string
	if sqsIsJSONProtocol(req) {
		var input struct {
			Attributes map[string]string `json:"Attributes"`
		}
		_ = json.Unmarshal(req.Body, &input)
		attrs = input.Attributes
	} else {
		attrs = parseSQSAttributes(req.Params)
	}
	if q.Attributes == nil {
		q.Attributes = make(map[string]string)
	}
	for k, v := range attrs {
		q.Attributes[k] = v
	}
	q.LastModifiedTimestamp = p.tc.Now().Unix()

	if err := p.saveQueue(context.Background(), q); err != nil {
		return nil, fmt.Errorf("sqs setQueueAttributes saveQueue: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"SetQueueAttributesResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) deleteQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	urlKey := sqsURLKey(queueURL)

	// Delete all messages.
	msgIDs, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}
	for _, id := range msgIDs {
		_ = p.deleteMsg(context.Background(), urlKey, id)
	}
	_ = p.state.Delete(context.Background(), sqsNamespace, "msg_ids:"+urlKey)

	// Delete queue.
	_ = p.state.Delete(context.Background(), sqsNamespace, "queue:"+urlKey)

	// Remove from names list.
	names, err := p.loadQueueNames(context.Background())
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != queueURL {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveQueueNames(context.Background(), newNames); err != nil {
		return nil, fmt.Errorf("sqs deleteQueue saveQueueNames: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"DeleteQueueResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) listQueues(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var prefix string
	if sqsIsJSONProtocol(req) {
		var input struct {
			QueueNamePrefix string `json:"QueueNamePrefix"`
		}
		_ = json.Unmarshal(req.Body, &input)
		prefix = input.QueueNamePrefix
	} else {
		prefix = req.Params["QueueNamePrefix"]
	}
	names, err := p.loadQueueNames(context.Background())
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(names))
	for _, u := range names {
		if prefix == "" {
			filtered = append(filtered, u)
		} else {
			// Check if the queue name (last path segment) starts with prefix.
			parts := strings.Split(u, "/")
			qName := parts[len(parts)-1]
			if strings.HasPrefix(qName, prefix) {
				filtered = append(filtered, u)
			}
		}
	}
	sort.Strings(filtered)

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{"QueueUrls": filtered})
	}
	type result struct {
		QueueURL []string `xml:"QueueUrl"`
	}
	type response struct {
		XMLName          xml.Name         `xml:"ListQueuesResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ListQueuesResult result           `xml:"ListQueuesResult"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ListQueuesResult: result{QueueURL: filtered},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) tagQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	if q.Tags == nil {
		q.Tags = make(map[string]string)
	}
	if sqsIsJSONProtocol(req) {
		var input struct {
			Tags map[string]string `json:"Tags"`
		}
		_ = json.Unmarshal(req.Body, &input)
		for k, v := range input.Tags {
			q.Tags[k] = v
		}
	} else {
		// Parse Tag.N.Key / Tag.N.Value pairs.
		for i := 1; ; i++ {
			k := req.Params[fmt.Sprintf("Tag.%d.Key", i)]
			v := req.Params[fmt.Sprintf("Tag.%d.Value", i)]
			if k == "" {
				break
			}
			q.Tags[k] = v
		}
	}

	if err := p.saveQueue(context.Background(), q); err != nil {
		return nil, fmt.Errorf("sqs tagQueue saveQueue: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"TagQueueResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) untagQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	if sqsIsJSONProtocol(req) {
		var input struct {
			TagKeys []string `json:"TagKeys"`
		}
		_ = json.Unmarshal(req.Body, &input)
		for _, k := range input.TagKeys {
			delete(q.Tags, k)
		}
	} else {
		for i := 1; ; i++ {
			k := req.Params[fmt.Sprintf("TagKey.%d", i)]
			if k == "" {
				break
			}
			delete(q.Tags, k)
		}
	}

	if err := p.saveQueue(context.Background(), q); err != nil {
		return nil, fmt.Errorf("sqs untagQueue saveQueue: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"UntagQueueResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) listQueueTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	type tagEntry struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type result struct {
		Tag []tagEntry `xml:"Tag"`
	}
	type response struct {
		XMLName             xml.Name         `xml:"ListQueueTagsResponse"`
		Xmlns               string           `xml:"xmlns,attr"`
		ListQueueTagsResult result           `xml:"ListQueueTagsResult"`
		ResponseMetadata    responseMetadata `xml:"ResponseMetadata"`
	}

	if sqsIsJSONProtocol(req) {
		tags := make(map[string]string, len(q.Tags))
		for k, v := range q.Tags {
			tags[k] = v
		}
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{"Tags": tags})
	}

	tags := make([]tagEntry, 0, len(q.Tags))
	for k, v := range q.Tags {
		tags = append(tags, tagEntry{Key: k, Value: v})
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })

	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:               "http://queue.amazonaws.com/doc/2012-11-05/",
		ListQueueTagsResult: result{Tag: tags},
		ResponseMetadata:    responseMetadata{RequestID: ctx.RequestID},
	})
}

// --- Message operations ------------------------------------------------------

func (p *SQSPlugin) sendMessage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	// Extract parameters supporting both protocols.
	var msgBody, delayStr, msgGroupID, dedupIDParam string
	var msgAttrs map[string]SQSMessageAttribute
	if sqsIsJSONProtocol(req) {
		var input struct {
			MessageBody            string                         `json:"MessageBody"`
			DelaySeconds           int                            `json:"DelaySeconds"`
			MessageGroupID         string                         `json:"MessageGroupId"`
			MessageDeduplicationID string                         `json:"MessageDeduplicationId"`
			MessageAttributes      map[string]SQSMessageAttribute `json:"MessageAttributes"`
		}
		_ = json.Unmarshal(req.Body, &input)
		msgBody = input.MessageBody
		if input.DelaySeconds > 0 {
			delayStr = strconv.Itoa(input.DelaySeconds)
		}
		msgGroupID = input.MessageGroupID
		dedupIDParam = input.MessageDeduplicationID
		msgAttrs = input.MessageAttributes
	} else {
		msgBody = req.Params["MessageBody"]
		delayStr = req.Params["DelaySeconds"]
		msgGroupID = req.Params["MessageGroupId"]
		dedupIDParam = req.Params["MessageDeduplicationId"]
		msgAttrs = sqsQueryMessageAttributes(req.Params, "")
	}

	// Validate the attributes before measuring them (#472). A message that is both
	// malformed and oversized reports the malformation, which is the more specific
	// fault: the size is a property of what was sent, while an illegal attribute name
	// or a non-numeric Number is a defect in the request itself, and shrinking the
	// message would not fix it.
	if awsErr := sqsCheckMessageAttributes(msgAttrs); awsErr != nil {
		return nil, awsErr
	}

	// Enforce MaximumMessageSize before anything else observable happens (#454).
	// Ahead of the FIFO branch deliberately: both queue types enforce it, and a FIFO
	// send that recorded a deduplication ID before failing would poison the dedup
	// window for the corrected retry that follows. The attribute check above inherits
	// that ordering for the same reason.
	if limit := sqsEffectiveMaximumMessageSize(q); sqsMessageSize(msgBody, msgAttrs) > limit {
		return nil, sqsMessageTooLong(limit)
	}

	if delayStr == "" {
		delayStr = getAttrOrDefault(q.Attributes, "DelaySeconds", "0")
	}
	delay, _ := strconv.Atoi(delayStr)

	// FIFO queue enforcement.
	if q.FifoQueue {
		if msgGroupID == "" {
			return nil, &AWSError{
				Code:       "MissingParameter",
				Message:    "The request must contain the parameter MessageGroupId.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		dedupID := dedupIDParam
		if dedupID == "" {
			if getAttrOrDefault(q.Attributes, "ContentBasedDeduplication", "false") == "true" {
				// SHA-256 of body as hex string.
				dedupID = sqsContentHash(msgBody)
			} else {
				return nil, &AWSError{
					Code:       "InvalidParameterValue",
					Message:    "The queue requires MessageDeduplicationId or ContentBasedDeduplication.",
					HTTPStatus: http.StatusBadRequest,
				}
			}
		}
		// Check deduplication window.
		urlKey := sqsURLKey(queueURL)
		if existing, dupMsgID := p.checkFIFODedup(context.Background(), urlKey, dedupID, p.tc.Now()); existing {
			// Return success with original message ID (idempotent).
			//
			// The attribute digest is computed over *this* request's attributes rather
			// than the deduplicated original's, because the digest is a checksum of
			// what the caller sent — its purpose is letting a caller verify the
			// service received the attributes intact. Reporting the earlier message's
			// digest would fail that check for a caller whose retry is byte-identical
			// only in its deduplication ID.
			md5Body := computeMD5(msgBody)
			md5Attrs := sqsMD5OfMessageAttributes(msgAttrs)
			if sqsIsJSONProtocol(req) {
				out := map[string]string{"MessageId": dupMsgID, "MD5OfMessageBody": md5Body}
				if md5Attrs != "" {
					out["MD5OfMessageAttributes"] = md5Attrs
				}
				return sqsJSONResponse(http.StatusOK, out)
			}
			type result struct {
				MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
				MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
				MessageID              string `xml:"MessageId"`
			}
			type response struct {
				XMLName           xml.Name         `xml:"SendMessageResponse"`
				Xmlns             string           `xml:"xmlns,attr"`
				SendMessageResult result           `xml:"SendMessageResult"`
				ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
			}
			return sqsXMLResponse(http.StatusOK, response{
				Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
				SendMessageResult: result{
					MD5OfMessageBody:       md5Body,
					MD5OfMessageAttributes: md5Attrs,
					MessageID:              dupMsgID,
				},
				ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
			})
		}
		// Record this deduplication ID.
		msgID := generateSQSMessageID()
		p.recordFIFODedup(context.Background(), urlKey, dedupID, msgID, p.tc.Now())

		md5Body := computeMD5(msgBody)
		md5Attrs := sqsMD5OfMessageAttributes(msgAttrs)
		now := p.tc.Now()
		msg := &SQSMessage{
			MessageID:     msgID,
			ReceiptHandle: generateSQSReceiptHandle(),
			Body:          msgBody,
			MD5OfBody:     md5Body,
			Attributes: map[string]string{
				"SenderId":      ctx.AccountID,
				"SentTimestamp": strconv.FormatInt(now.UnixMilli(), 10),
			},
			MessageAttributes: msgAttrs,
			SentTimestamp:     now.UnixMilli(),
			DelayUntil:        now.Add(time.Duration(delay) * time.Second),
			VisibleAfter:      time.Time{},
			ReceiveCount:      0,
			MessageGroupID:    msgGroupID,
		}
		if saveErr := p.saveMsg(context.Background(), urlKey, msg); saveErr != nil {
			return nil, fmt.Errorf("sqs sendMessage saveMsg: %w", saveErr)
		}
		ids, loadErr := p.loadMsgIDs(context.Background(), urlKey)
		if loadErr != nil {
			return nil, loadErr
		}
		ids = append(ids, msgID)
		if saveErr := p.saveMsgIDs(context.Background(), urlKey, ids); saveErr != nil {
			return nil, fmt.Errorf("sqs sendMessage saveMsgIDs: %w", saveErr)
		}
		if sqsIsJSONProtocol(req) {
			out := map[string]string{"MessageId": msgID, "MD5OfMessageBody": md5Body}
			if md5Attrs != "" {
				out["MD5OfMessageAttributes"] = md5Attrs
			}
			return sqsJSONResponse(http.StatusOK, out)
		}
		type result struct {
			MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
			MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
			MessageID              string `xml:"MessageId"`
		}
		type response struct {
			XMLName           xml.Name         `xml:"SendMessageResponse"`
			Xmlns             string           `xml:"xmlns,attr"`
			SendMessageResult result           `xml:"SendMessageResult"`
			ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
		}
		return sqsXMLResponse(http.StatusOK, response{
			Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
			SendMessageResult: result{
				MD5OfMessageBody:       md5Body,
				MD5OfMessageAttributes: md5Attrs,
				MessageID:              msgID,
			},
			ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
		})
	}

	msgID := generateSQSMessageID()
	md5Body := computeMD5(msgBody)
	md5Attrs := sqsMD5OfMessageAttributes(msgAttrs)
	now := p.tc.Now()

	msg := &SQSMessage{
		MessageID:     msgID,
		ReceiptHandle: generateSQSReceiptHandle(),
		Body:          msgBody,
		MD5OfBody:     md5Body,
		Attributes: map[string]string{
			"SenderId":      ctx.AccountID,
			"SentTimestamp": strconv.FormatInt(now.UnixMilli(), 10),
		},
		MessageAttributes: msgAttrs,
		SentTimestamp:     now.UnixMilli(),
		DelayUntil:        now.Add(time.Duration(delay) * time.Second),
		VisibleAfter:      time.Time{},
		ReceiveCount:      0,
	}

	urlKey := sqsURLKey(queueURL)
	if err := p.saveMsg(context.Background(), urlKey, msg); err != nil {
		return nil, fmt.Errorf("sqs sendMessage saveMsg: %w", err)
	}

	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}
	ids = append(ids, msgID)
	if err := p.saveMsgIDs(context.Background(), urlKey, ids); err != nil {
		return nil, fmt.Errorf("sqs sendMessage saveMsgIDs: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		out := map[string]string{"MessageId": msgID, "MD5OfMessageBody": md5Body}
		if md5Attrs != "" {
			out["MD5OfMessageAttributes"] = md5Attrs
		}
		return sqsJSONResponse(http.StatusOK, out)
	}
	type result struct {
		MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
		MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
		MessageID              string `xml:"MessageId"`
	}
	type response struct {
		XMLName           xml.Name         `xml:"SendMessageResponse"`
		Xmlns             string           `xml:"xmlns,attr"`
		SendMessageResult result           `xml:"SendMessageResult"`
		ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
		SendMessageResult: result{
			MD5OfMessageBody:       md5Body,
			MD5OfMessageAttributes: md5Attrs,
			MessageID:              msgID,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) sendMessageBatch(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	urlKey := sqsURLKey(queueURL)
	now := p.tc.Now()

	type successEntryXML struct {
		ID                     string `xml:"Id"`
		MessageID              string `xml:"MessageId"`
		MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
		MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
	}
	type successEntryJSON struct {
		ID                     string `json:"Id"`
		MessageID              string `json:"MessageId"`
		MD5OfMessageBody       string `json:"MD5OfMessageBody"`
		MD5OfMessageAttributes string `json:"MD5OfMessageAttributes,omitempty"`
	}

	var successesXML []successEntryXML
	var successesJSON []successEntryJSON

	// Failed entries, shared by both protocol branches. An attribute-rule violation
	// fails only its own entry (#472) — see [sqsBatchFailure] for why that differs from
	// the size checks below, which fail the whole request.
	var failures []sqsBatchResultErrorEntry

	// The per-message limit is the queue's effective MaximumMessageSize; the batch
	// total has its own cap. Both are checked before any entry is stored, so a
	// rejected batch enqueues nothing — a partially-applied batch would leave a
	// consumer's retry to re-send the entries that already landed (#454).
	perMessageLimit := sqsEffectiveMaximumMessageSize(q)

	if sqsIsJSONProtocol(req) {
		var input struct {
			Entries []struct {
				ID                string                         `json:"Id"`
				MessageBody       string                         `json:"MessageBody"`
				DelaySeconds      int                            `json:"DelaySeconds"`
				MessageAttributes map[string]SQSMessageAttribute `json:"MessageAttributes"`
			} `json:"Entries"`
		}
		_ = json.Unmarshal(req.Body, &input)
		sizes := make([]int, 0, len(input.Entries))
		for _, entry := range input.Entries {
			sizes = append(sizes, sqsMessageSize(entry.MessageBody, entry.MessageAttributes))
		}
		if sizeErr := sqsCheckBatchSizes(perMessageLimit, sizes); sizeErr != nil {
			return nil, sizeErr
		}
		for _, entry := range input.Entries {
			// A malformed entry fails alone and is not stored, while its siblings
			// proceed. The sizes above were measured over every entry as sent,
			// including this one: the payload cap is about what the caller
			// transmitted, so a whole-request size failure still supersedes a
			// per-entry one.
			if awsErr := sqsCheckMessageAttributes(entry.MessageAttributes); awsErr != nil {
				failures = append(failures, sqsBatchFailure(entry.ID, awsErr))
				continue
			}
			msgID := generateSQSMessageID()
			md5Body := computeMD5(entry.MessageBody)
			msg := &SQSMessage{
				MessageID:     msgID,
				ReceiptHandle: generateSQSReceiptHandle(),
				Body:          entry.MessageBody,
				MD5OfBody:     md5Body,
				Attributes: map[string]string{
					"SenderId":      ctx.AccountID,
					"SentTimestamp": strconv.FormatInt(now.UnixMilli(), 10),
				},
				MessageAttributes: entry.MessageAttributes,
				SentTimestamp:     now.UnixMilli(),
				DelayUntil:        now.Add(time.Duration(entry.DelaySeconds) * time.Second),
				VisibleAfter:      time.Time{},
				ReceiveCount:      0,
			}
			if saveErr := p.saveMsg(context.Background(), urlKey, msg); saveErr != nil {
				return nil, fmt.Errorf("sqs sendMessageBatch saveMsg: %w", saveErr)
			}
			ids, loadErr := p.loadMsgIDs(context.Background(), urlKey)
			if loadErr != nil {
				return nil, loadErr
			}
			ids = append(ids, msgID)
			if saveErr := p.saveMsgIDs(context.Background(), urlKey, ids); saveErr != nil {
				return nil, fmt.Errorf("sqs sendMessageBatch saveMsgIDs: %w", saveErr)
			}
			successesJSON = append(successesJSON, successEntryJSON{
				ID:                     entry.ID,
				MessageID:              msgID,
				MD5OfMessageBody:       md5Body,
				MD5OfMessageAttributes: sqsMD5OfMessageAttributes(entry.MessageAttributes),
			})
		}
		if successesJSON == nil {
			successesJSON = make([]successEntryJSON, 0)
		}
		if failures == nil {
			failures = make([]sqsBatchResultErrorEntry, 0)
		}
		// HTTP 200 even when entries failed, per the reference: a caller "should check
		// for batch errors even when the call returns an HTTP status code of 200".
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{
			"Successful": successesJSON,
			"Failed":     failures,
		})
	}

	// Size every entry before storing any, for the reason above. This walks the
	// entries a second time rather than checking inside the storing loop, because a
	// mid-loop rejection is exactly the partial application the check exists to
	// prevent.
	var querySizes []int
	for i := 1; ; i++ {
		entryPrefix := fmt.Sprintf("SendMessageBatchRequestEntry.%d.", i)
		if req.Params[entryPrefix+"Id"] == "" {
			break
		}
		querySizes = append(querySizes, sqsMessageSize(
			req.Params[entryPrefix+"MessageBody"],
			sqsQueryMessageAttributes(req.Params, entryPrefix),
		))
	}
	if sizeErr := sqsCheckBatchSizes(perMessageLimit, querySizes); sizeErr != nil {
		return nil, sizeErr
	}

	for i := 1; ; i++ {
		entryPrefix := fmt.Sprintf("SendMessageBatchRequestEntry.%d.", i)
		entryID := req.Params[entryPrefix+"Id"]
		if entryID == "" {
			break
		}
		body := req.Params[entryPrefix+"MessageBody"]
		delayStr := req.Params[entryPrefix+"DelaySeconds"]
		delay, _ := strconv.Atoi(delayStr)
		entryAttrs := sqsQueryMessageAttributes(req.Params, entryPrefix)

		// Per-entry, as in the JSON branch above.
		if awsErr := sqsCheckMessageAttributes(entryAttrs); awsErr != nil {
			failures = append(failures, sqsBatchFailure(entryID, awsErr))
			continue
		}

		msgID := generateSQSMessageID()
		md5Body := computeMD5(body)

		msg := &SQSMessage{
			MessageID:     msgID,
			ReceiptHandle: generateSQSReceiptHandle(),
			Body:          body,
			MD5OfBody:     md5Body,
			Attributes: map[string]string{
				"SenderId":      ctx.AccountID,
				"SentTimestamp": strconv.FormatInt(now.UnixMilli(), 10),
			},
			MessageAttributes: entryAttrs,
			SentTimestamp:     now.UnixMilli(),
			DelayUntil:        now.Add(time.Duration(delay) * time.Second),
			VisibleAfter:      time.Time{},
			ReceiveCount:      0,
		}

		if saveErr := p.saveMsg(context.Background(), urlKey, msg); saveErr != nil {
			return nil, fmt.Errorf("sqs sendMessageBatch saveMsg: %w", saveErr)
		}

		ids, loadErr := p.loadMsgIDs(context.Background(), urlKey)
		if loadErr != nil {
			return nil, loadErr
		}
		ids = append(ids, msgID)
		if saveErr := p.saveMsgIDs(context.Background(), urlKey, ids); saveErr != nil {
			return nil, fmt.Errorf("sqs sendMessageBatch saveMsgIDs: %w", saveErr)
		}

		successesXML = append(successesXML, successEntryXML{
			ID:                     entryID,
			MessageID:              msgID,
			MD5OfMessageBody:       md5Body,
			MD5OfMessageAttributes: sqsMD5OfMessageAttributes(entryAttrs),
		})
	}

	// Both lists sit directly under the result wrapper as repeated elements, since the
	// query-protocol model declares each flattened with its own locationName rather
	// than nested under a Successful or Failed wrapper.
	type result struct {
		SendMessageBatchResultEntry []successEntryXML          `xml:"SendMessageBatchResultEntry"`
		BatchResultErrorEntry       []sqsBatchResultErrorEntry `xml:"BatchResultErrorEntry"`
	}
	type response struct {
		XMLName                xml.Name         `xml:"SendMessageBatchResponse"`
		Xmlns                  string           `xml:"xmlns,attr"`
		SendMessageBatchResult result           `xml:"SendMessageBatchResult"`
		ResponseMetadata       responseMetadata `xml:"ResponseMetadata"`
	}
	// HTTP 200 with failures present, as in the JSON branch.
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
		SendMessageBatchResult: result{
			SendMessageBatchResultEntry: successesXML,
			BatchResultErrorEntry:       failures,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) receiveMessage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	var maxNum int
	var visTimeout int
	if sqsIsJSONProtocol(req) {
		var input struct {
			MaxNumberOfMessages int `json:"MaxNumberOfMessages"`
			VisibilityTimeout   int `json:"VisibilityTimeout"`
		}
		_ = json.Unmarshal(req.Body, &input)
		maxNum = input.MaxNumberOfMessages
		if maxNum <= 0 {
			maxNum = 1
		}
		if maxNum > 10 {
			maxNum = 10
		}
		visTimeout = input.VisibilityTimeout
		if visTimeout == 0 {
			if v := getAttrOrDefault(q.Attributes, "VisibilityTimeout", "30"); v != "" {
				visTimeout, _ = strconv.Atoi(v)
			}
			if visTimeout == 0 {
				visTimeout = 30
			}
		}
	} else {
		maxStr := req.Params["MaxNumberOfMessages"]
		maxNum = 1
		if maxStr != "" {
			if n, parseErr := strconv.Atoi(maxStr); parseErr == nil && n > 0 {
				maxNum = n
				if maxNum > 10 {
					maxNum = 10
				}
			}
		}
		visStr := req.Params["VisibilityTimeout"]
		visTimeout = 30
		if visStr != "" {
			if n, parseErr := strconv.Atoi(visStr); parseErr == nil {
				visTimeout = n
			}
		} else {
			if v := getAttrOrDefault(q.Attributes, "VisibilityTimeout", "30"); v != "" {
				if n, parseErr := strconv.Atoi(v); parseErr == nil {
					visTimeout = n
				}
			}
		}
	}

	urlKey := sqsURLKey(queueURL)
	now := p.tc.Now()

	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	// Attributes are returned only for the names the request asked for (#461).
	// Volunteering them unconditionally would be its own infidelity: a consumer whose
	// production caller never sets MessageAttributeNames would see substrate hand back
	// attributes real SQS withholds, so the routing branch their test exercises is not
	// the branch that runs against AWS.
	requestedAttrNames := sqsRequestedAttributeNames(req)

	type msgResultXML struct {
		MessageID              string                   `xml:"MessageId"`
		ReceiptHandle          string                   `xml:"ReceiptHandle"`
		MD5OfBody              string                   `xml:"MD5OfBody"`
		Body                   string                   `xml:"Body"`
		MD5OfMessageAttributes string                   `xml:"MD5OfMessageAttributes,omitempty"`
		MessageAttribute       []sqsMessageAttributeXML `xml:"MessageAttribute,omitempty"`
		MessageGroupID         string                   `xml:"MessageGroupId,omitempty"`
	}
	type msgResultJSON struct {
		MessageID              string                         `json:"MessageId"`
		ReceiptHandle          string                         `json:"ReceiptHandle"`
		MD5OfBody              string                         `json:"MD5OfBody"`
		Body                   string                         `json:"Body"`
		MD5OfMessageAttributes string                         `json:"MD5OfMessageAttributes,omitempty"`
		MessageAttributes      map[string]SQSMessageAttribute `json:"MessageAttributes,omitempty"`
		MessageGroupID         string                         `json:"MessageGroupId,omitempty"`
	}

	messagesXML := make([]msgResultXML, 0)
	messagesJSON := make([]msgResultJSON, 0)

	// fifoGroup locks all messages in this ReceiveMessage call to a single
	// message group (AWS FIFO spec: at most one group returned per call).
	fifoGroup := ""

	for _, id := range ids {
		if len(messagesXML)+len(messagesJSON) >= maxNum {
			break
		}
		msg, loadErr := p.loadMsg(context.Background(), urlKey, id)
		if loadErr != nil || msg == nil {
			continue
		}
		// Check visibility.
		if !msg.DelayUntil.IsZero() && now.Before(msg.DelayUntil) {
			continue
		}
		if !msg.VisibleAfter.IsZero() && now.Before(msg.VisibleAfter) {
			continue
		}

		// FIFO: enforce single-group-per-call.
		if q.FifoQueue {
			if fifoGroup == "" {
				fifoGroup = msg.MessageGroupID
			} else if msg.MessageGroupID != fifoGroup {
				continue
			}
		}

		// Update receipt handle and visibility timeout.
		newHandle := generateSQSReceiptHandle()
		msg.ReceiptHandle = newHandle
		msg.VisibleAfter = now.Add(time.Duration(visTimeout) * time.Second)
		msg.ReceiveCount++

		if saveErr := p.saveMsg(context.Background(), urlKey, msg); saveErr != nil {
			p.logger.Warn("sqs receiveMessage: failed to update msg", "err", saveErr)
			continue
		}

		// Warn, but return the message unchanged, when its stored attributes violate a
		// rule enforced on send (#491). The observation is deliberately untouched: the
		// message was accepted by the substrate that recorded it, so withholding it here
		// would make an older event log unreplayable.
		sqsWarnStoredAttributes(p.logger, queueURL, msg)

		// The digest covers what is being returned, not what was sent: a request
		// naming a subset gets the digest of that subset, which is what lets a caller
		// checksum the attributes actually in hand. Reusing the send-time digest would
		// hand them a value their own recomputation could never match.
		selectedAttrs := sqsSelectMessageAttributes(msg.MessageAttributes, requestedAttrNames)

		if sqsIsJSONProtocol(req) {
			messagesJSON = append(messagesJSON, msgResultJSON{
				MessageID:              msg.MessageID,
				ReceiptHandle:          newHandle,
				MD5OfBody:              msg.MD5OfBody,
				Body:                   msg.Body,
				MD5OfMessageAttributes: sqsMD5OfMessageAttributes(selectedAttrs),
				MessageAttributes:      selectedAttrs,
				MessageGroupID:         msg.MessageGroupID,
			})
		} else {
			messagesXML = append(messagesXML, msgResultXML{
				MessageID:              msg.MessageID,
				ReceiptHandle:          newHandle,
				MD5OfBody:              msg.MD5OfBody,
				Body:                   msg.Body,
				MD5OfMessageAttributes: sqsMD5OfMessageAttributes(selectedAttrs),
				MessageAttribute:       sqsMessageAttributesXML(selectedAttrs),
				MessageGroupID:         msg.MessageGroupID,
			})
		}
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{"Messages": messagesJSON})
	}
	type result struct {
		Message []msgResultXML `xml:"Message"`
	}
	type response struct {
		XMLName              xml.Name         `xml:"ReceiveMessageResponse"`
		Xmlns                string           `xml:"xmlns,attr"`
		ReceiveMessageResult result           `xml:"ReceiveMessageResult"`
		ResponseMetadata     responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                "http://queue.amazonaws.com/doc/2012-11-05/",
		ReceiveMessageResult: result{Message: messagesXML},
		ResponseMetadata:     responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) deleteMessage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	var receiptHandle string
	if sqsIsJSONProtocol(req) {
		var input struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		}
		_ = json.Unmarshal(req.Body, &input)
		receiptHandle = input.ReceiptHandle
	} else {
		receiptHandle = req.Params["ReceiptHandle"]
	}
	urlKey := sqsURLKey(queueURL)

	// Find message by receipt handle.
	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	var deletedID string
	for _, id := range ids {
		msg, loadErr := p.loadMsg(context.Background(), urlKey, id)
		if loadErr != nil || msg == nil {
			continue
		}
		if msg.ReceiptHandle == receiptHandle {
			deletedID = id
			break
		}
	}

	if deletedID != "" {
		_ = p.deleteMsg(context.Background(), urlKey, deletedID)
		newIDs := make([]string, 0, len(ids)-1)
		for _, id := range ids {
			if id != deletedID {
				newIDs = append(newIDs, id)
			}
		}
		if err := p.saveMsgIDs(context.Background(), urlKey, newIDs); err != nil {
			return nil, fmt.Errorf("sqs deleteMessage saveMsgIDs: %w", err)
		}
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"DeleteMessageResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) deleteMessageBatch(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	urlKey := sqsURLKey(queueURL)
	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	type successEntryXML struct {
		ID string `xml:"Id"`
	}
	type successEntryJSON struct {
		ID string `json:"Id"`
	}
	var successesXML []successEntryXML
	var successesJSON []successEntryJSON

	if sqsIsJSONProtocol(req) {
		var input struct {
			Entries []struct {
				ID            string `json:"Id"`
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Entries"`
		}
		_ = json.Unmarshal(req.Body, &input)
		for _, entry := range input.Entries {
			for _, msgID := range ids {
				msg, loadErr := p.loadMsg(context.Background(), urlKey, msgID)
				if loadErr != nil || msg == nil {
					continue
				}
				if msg.ReceiptHandle == entry.ReceiptHandle {
					_ = p.deleteMsg(context.Background(), urlKey, msgID)
					newIDs := make([]string, 0, len(ids))
					for _, id := range ids {
						if id != msgID {
							newIDs = append(newIDs, id)
						}
					}
					ids = newIDs
					break
				}
			}
			successesJSON = append(successesJSON, successEntryJSON{ID: entry.ID})
		}
		if err := p.saveMsgIDs(context.Background(), urlKey, ids); err != nil {
			return nil, fmt.Errorf("sqs deleteMessageBatch saveMsgIDs: %w", err)
		}
		if successesJSON == nil {
			successesJSON = make([]successEntryJSON, 0)
		}
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{
			"Successful": successesJSON,
			"Failed":     []struct{}{},
		})
	}

	for i := 1; ; i++ {
		entryID := req.Params[fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", i)]
		handle := req.Params[fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i)]
		if entryID == "" {
			break
		}

		for _, msgID := range ids {
			msg, loadErr := p.loadMsg(context.Background(), urlKey, msgID)
			if loadErr != nil || msg == nil {
				continue
			}
			if msg.ReceiptHandle == handle {
				_ = p.deleteMsg(context.Background(), urlKey, msgID)
				newIDs := make([]string, 0, len(ids))
				for _, id := range ids {
					if id != msgID {
						newIDs = append(newIDs, id)
					}
				}
				ids = newIDs
				break
			}
		}
		successesXML = append(successesXML, successEntryXML{ID: entryID})
	}

	if err := p.saveMsgIDs(context.Background(), urlKey, ids); err != nil {
		return nil, fmt.Errorf("sqs deleteMessageBatch saveMsgIDs: %w", err)
	}

	type result struct {
		DeleteMessageBatchResultEntry []successEntryXML `xml:"DeleteMessageBatchResultEntry"`
	}
	type response struct {
		XMLName                  xml.Name         `xml:"DeleteMessageBatchResponse"`
		Xmlns                    string           `xml:"xmlns,attr"`
		DeleteMessageBatchResult result           `xml:"DeleteMessageBatchResult"`
		ResponseMetadata         responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                    "http://queue.amazonaws.com/doc/2012-11-05/",
		DeleteMessageBatchResult: result{DeleteMessageBatchResultEntry: successesXML},
		ResponseMetadata:         responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) changeMessageVisibility(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	var receiptHandle string
	var vis int
	if sqsIsJSONProtocol(req) {
		var input struct {
			ReceiptHandle     string `json:"ReceiptHandle"`
			VisibilityTimeout int    `json:"VisibilityTimeout"`
		}
		_ = json.Unmarshal(req.Body, &input)
		receiptHandle = input.ReceiptHandle
		vis = input.VisibilityTimeout
	} else {
		receiptHandle = req.Params["ReceiptHandle"]
		visStr := req.Params["VisibilityTimeout"]
		vis, _ = strconv.Atoi(visStr)
	}

	urlKey := sqsURLKey(queueURL)
	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	for _, id := range ids {
		msg, loadErr := p.loadMsg(context.Background(), urlKey, id)
		if loadErr != nil || msg == nil {
			continue
		}
		if msg.ReceiptHandle == receiptHandle {
			msg.VisibleAfter = p.tc.Now().Add(time.Duration(vis) * time.Second)
			if saveErr := p.saveMsg(context.Background(), urlKey, msg); saveErr != nil {
				return nil, fmt.Errorf("sqs changeMessageVisibility saveMsg: %w", saveErr)
			}
			break
		}
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"ChangeMessageVisibilityResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) purgeQueue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := sqsQueueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	urlKey := sqsURLKey(queueURL)
	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		_ = p.deleteMsg(context.Background(), urlKey, id)
	}
	if err := p.saveMsgIDs(context.Background(), urlKey, nil); err != nil {
		return nil, fmt.Errorf("sqs purgeQueue saveMsgIDs: %w", err)
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, struct{}{})
	}
	type response struct {
		XMLName          xml.Name         `xml:"PurgeQueueResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:            "http://queue.amazonaws.com/doc/2012-11-05/",
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

// --- Response helpers --------------------------------------------------------

// sqsXMLResponse marshals v as XML and returns an AWSResponse with text/xml Content-Type.
func sqsXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sqsXMLResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// --- Utility -----------------------------------------------------------------

// parseSQSAttributes extracts Attribute.N.Name / Attribute.N.Value pairs from params.
func parseSQSAttributes(params map[string]string) map[string]string {
	attrs := make(map[string]string)
	for i := 1; ; i++ {
		k := params[fmt.Sprintf("Attribute.%d.Name", i)]
		v := params[fmt.Sprintf("Attribute.%d.Value", i)]
		if k == "" {
			break
		}
		attrs[k] = v
	}
	return attrs
}

// getAttrOrDefault returns attrs[key] or fallback when key is absent.
func getAttrOrDefault(attrs map[string]string, key, fallback string) string {
	if attrs == nil {
		return fallback
	}
	if v, ok := attrs[key]; ok {
		return v
	}
	return fallback
}

// generateSQSMessageID generates a unique SQS message ID.
func generateSQSMessageID() string {
	return generateLambdaRevisionID() // Reuse UUID-style generator.
}

// generateSQSReceiptHandle generates a unique receipt handle.
func generateSQSReceiptHandle() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) //nolint:gosec // Receipt handle just needs to be unique, not cryptographically secure.
	return fmt.Sprintf("%x", b)
}

// computeMD5 computes the hex MD5 of s.
func computeMD5(s string) string {
	h := md5.Sum([]byte(s)) //nolint:gosec // nosemgrep
	return fmt.Sprintf("%x", h)
}

// --- FIFO deduplication helpers ----------------------------------------------

// sqsFIFODedupKey returns the state key for FIFO deduplication tracking.
func sqsFIFODedupKey(urlKey string) string {
	return "fifo_dedup:" + urlKey
}

// sqsFIFODedupEntry holds a single deduplication entry.
type sqsFIFODedupEntry struct {
	// MessageID is the ID of the original message.
	MessageID string `json:"MessageID"`
	// ExpiresNano is the Unix nanosecond timestamp after which this entry is
	// considered expired (deduplication window = 5 minutes).
	ExpiresNano int64 `json:"ExpiresNano"`
}

// checkFIFODedup returns (true, originalMsgID) if dedupID is within the 5-minute
// deduplication window, (false, "") otherwise.
func (p *SQSPlugin) checkFIFODedup(ctx context.Context, urlKey, dedupID string, now time.Time) (bool, string) {
	data, err := p.state.Get(ctx, sqsNamespace, sqsFIFODedupKey(urlKey))
	if err != nil || data == nil {
		return false, ""
	}
	var window map[string]sqsFIFODedupEntry
	if err := json.Unmarshal(data, &window); err != nil {
		return false, ""
	}
	entry, ok := window[dedupID]
	if !ok {
		return false, ""
	}
	if now.UnixNano() > entry.ExpiresNano {
		return false, ""
	}
	return true, entry.MessageID
}

// recordFIFODedup adds dedupID → msgID to the deduplication window and prunes
// expired entries.
func (p *SQSPlugin) recordFIFODedup(ctx context.Context, urlKey, dedupID, msgID string, now time.Time) {
	data, _ := p.state.Get(ctx, sqsNamespace, sqsFIFODedupKey(urlKey))
	var window map[string]sqsFIFODedupEntry
	if data != nil {
		_ = json.Unmarshal(data, &window)
	}
	if window == nil {
		window = make(map[string]sqsFIFODedupEntry)
	}
	// Prune expired entries.
	nowNano := now.UnixNano()
	for k, e := range window {
		if nowNano > e.ExpiresNano {
			delete(window, k)
		}
	}
	window[dedupID] = sqsFIFODedupEntry{
		MessageID:   msgID,
		ExpiresNano: now.Add(5 * time.Minute).UnixNano(),
	}
	if b, err := json.Marshal(window); err == nil {
		_ = p.state.Put(ctx, sqsNamespace, sqsFIFODedupKey(urlKey), b)
	}
}

// sqsContentHash returns the hex SHA-256 digest of body for content-based
// deduplication. Uses SHA-256 per the AWS SQS specification.
func sqsContentHash(body string) string {
	h := sha256.Sum256([]byte(body)) //nolint:gosec
	return fmt.Sprintf("%x", h)
}
