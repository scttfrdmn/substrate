package substrate

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

// SQSPlugin emulates the Amazon Simple Queue Service (SQS) query-protocol API.
// It handles CreateQueue, DeleteQueue, GetQueueUrl, GetQueueAttributes,
// SetQueueAttributes, ListQueues, TagQueue, UntagQueue, ListQueueTags,
// SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage,
// DeleteMessageBatch, ChangeMessageVisibility, and PurgeQueue.
type SQSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
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

// queueURLFromRequest extracts the queue URL from the QueueUrl param.
func queueURLFromRequest(req *AWSRequest) string {
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
	name := req.Params["QueueName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "QueueName is required", HTTPStatus: http.StatusBadRequest}
	}
	isFifo := strings.HasSuffix(name, ".fifo")

	queueURL := sqsQueueURL(ctx.Region, ctx.AccountID, name)
	existing, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// Idempotent — return existing URL.
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

	// Collect Attribute.N.Name / Attribute.N.Value pairs.
	attrs := parseSQSAttributes(req.Params)
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
	name := req.Params["QueueName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "QueueName is required", HTTPStatus: http.StatusBadRequest}
	}
	queueURL := sqsQueueURL(ctx.Region, ctx.AccountID, name)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	// Build standard attributes.
	attrs := map[string]string{
		"QueueArn":                      q.QueueARN,
		"CreatedTimestamp":              strconv.FormatInt(q.CreatedTimestamp, 10),
		"LastModifiedTimestamp":         strconv.FormatInt(q.LastModifiedTimestamp, 10),
		"VisibilityTimeout":             getAttrOrDefault(q.Attributes, "VisibilityTimeout", "30"),
		"MaximumMessageSize":            getAttrOrDefault(q.Attributes, "MaximumMessageSize", "262144"),
		"MessageRetentionPeriod":        getAttrOrDefault(q.Attributes, "MessageRetentionPeriod", "345600"),
		"DelaySeconds":                  getAttrOrDefault(q.Attributes, "DelaySeconds", "0"),
		"ReceiveMessageWaitTimeSeconds": getAttrOrDefault(q.Attributes, "ReceiveMessageWaitTimeSeconds", "0"),
	}
	for k, v := range q.Attributes {
		attrs[k] = v
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	attrs := parseSQSAttributes(req.Params)
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
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
	prefix := req.Params["QueueNamePrefix"]
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	// Parse Tag.N.Key / Tag.N.Value pairs.
	if q.Tags == nil {
		q.Tags = make(map[string]string)
	}
	for i := 1; ; i++ {
		k := req.Params[fmt.Sprintf("Tag.%d.Key", i)]
		v := req.Params[fmt.Sprintf("Tag.%d.Value", i)]
		if k == "" {
			break
		}
		q.Tags[k] = v
	}

	if err := p.saveQueue(context.Background(), q); err != nil {
		return nil, fmt.Errorf("sqs tagQueue saveQueue: %w", err)
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	for i := 1; ; i++ {
		k := req.Params[fmt.Sprintf("TagKey.%d", i)]
		if k == "" {
			break
		}
		delete(q.Tags, k)
	}

	if err := p.saveQueue(context.Background(), q); err != nil {
		return nil, fmt.Errorf("sqs untagQueue saveQueue: %w", err)
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	body := req.Params["MessageBody"]
	delayStr := req.Params["DelaySeconds"]
	if delayStr == "" {
		delayStr = getAttrOrDefault(q.Attributes, "DelaySeconds", "0")
	}
	delay, _ := strconv.Atoi(delayStr)

	// FIFO queue enforcement.
	if q.FifoQueue {
		if req.Params["MessageGroupId"] == "" {
			return nil, &AWSError{
				Code:       "MissingParameter",
				Message:    "The request must contain the parameter MessageGroupId.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		dedupID := req.Params["MessageDeduplicationId"]
		if dedupID == "" {
			if getAttrOrDefault(q.Attributes, "ContentBasedDeduplication", "false") == "true" {
				// SHA-256 of body as hex string.
				dedupID = sqsContentHash(body)
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
			md5Body := computeMD5(body)
			type result struct {
				MD5OfMessageBody string `xml:"MD5OfMessageBody"`
				MessageID        string `xml:"MessageId"`
			}
			type response struct {
				XMLName           xml.Name         `xml:"SendMessageResponse"`
				Xmlns             string           `xml:"xmlns,attr"`
				SendMessageResult result           `xml:"SendMessageResult"`
				ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
			}
			return sqsXMLResponse(http.StatusOK, response{
				Xmlns:             "http://queue.amazonaws.com/doc/2012-11-05/",
				SendMessageResult: result{MD5OfMessageBody: md5Body, MessageID: dupMsgID},
				ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
			})
		}
		// Record this deduplication ID.
		msgID := generateSQSMessageID()
		p.recordFIFODedup(context.Background(), urlKey, dedupID, msgID, p.tc.Now())

		md5Body := computeMD5(body)
		now := p.tc.Now()
		msg := &SQSMessage{
			MessageID:     msgID,
			ReceiptHandle: generateSQSReceiptHandle(),
			Body:          body,
			MD5OfBody:     md5Body,
			Attributes: map[string]string{
				"SenderId":      ctx.AccountID,
				"SentTimestamp": strconv.FormatInt(now.UnixMilli(), 10),
			},
			SentTimestamp: now.UnixMilli(),
			DelayUntil:    now.Add(time.Duration(delay) * time.Second),
			VisibleAfter:  time.Time{},
			ReceiveCount:  0,
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
		type result struct {
			MD5OfMessageBody string `xml:"MD5OfMessageBody"`
			MessageID        string `xml:"MessageId"`
		}
		type response struct {
			XMLName           xml.Name         `xml:"SendMessageResponse"`
			Xmlns             string           `xml:"xmlns,attr"`
			SendMessageResult result           `xml:"SendMessageResult"`
			ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
		}
		return sqsXMLResponse(http.StatusOK, response{
			Xmlns:             "http://queue.amazonaws.com/doc/2012-11-05/",
			SendMessageResult: result{MD5OfMessageBody: md5Body, MessageID: msgID},
			ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
		})
	}

	msgID := generateSQSMessageID()
	md5Body := computeMD5(body)
	now := p.tc.Now()

	msg := &SQSMessage{
		MessageID:     msgID,
		ReceiptHandle: generateSQSReceiptHandle(),
		Body:          body,
		MD5OfBody:     md5Body,
		Attributes: map[string]string{
			"SenderId":      ctx.AccountID,
			"SentTimestamp": strconv.FormatInt(now.UnixMilli(), 10),
		},
		SentTimestamp: now.UnixMilli(),
		DelayUntil:    now.Add(time.Duration(delay) * time.Second),
		VisibleAfter:  time.Time{},
		ReceiveCount:  0,
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

	type result struct {
		MD5OfMessageBody string `xml:"MD5OfMessageBody"`
		MessageID        string `xml:"MessageId"`
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
			MD5OfMessageBody: md5Body,
			MessageID:        msgID,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) sendMessageBatch(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	urlKey := sqsURLKey(queueURL)
	now := p.tc.Now()

	type successEntry struct {
		ID               string `xml:"Id"`
		MessageID        string `xml:"MessageId"`
		MD5OfMessageBody string `xml:"MD5OfMessageBody"`
	}

	var successes []successEntry
	for i := 1; ; i++ {
		entryID := req.Params[fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", i)]
		if entryID == "" {
			break
		}
		body := req.Params[fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", i)]
		delayStr := req.Params[fmt.Sprintf("SendMessageBatchRequestEntry.%d.DelaySeconds", i)]
		delay, _ := strconv.Atoi(delayStr)

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
			SentTimestamp: now.UnixMilli(),
			DelayUntil:    now.Add(time.Duration(delay) * time.Second),
			VisibleAfter:  time.Time{},
			ReceiveCount:  0,
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

		successes = append(successes, successEntry{ID: entryID, MessageID: msgID, MD5OfMessageBody: md5Body})
	}

	type result struct {
		SendMessageBatchResultEntry []successEntry `xml:"SendMessageBatchResultEntry"`
	}
	type response struct {
		XMLName                xml.Name         `xml:"SendMessageBatchResponse"`
		Xmlns                  string           `xml:"xmlns,attr"`
		SendMessageBatchResult result           `xml:"SendMessageBatchResult"`
		ResponseMetadata       responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                  "http://queue.amazonaws.com/doc/2012-11-05/",
		SendMessageBatchResult: result{SendMessageBatchResultEntry: successes},
		ResponseMetadata:       responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) receiveMessage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	maxStr := req.Params["MaxNumberOfMessages"]
	maxNum := 1
	if maxStr != "" {
		if n, parseErr := strconv.Atoi(maxStr); parseErr == nil && n > 0 {
			maxNum = n
			if maxNum > 10 {
				maxNum = 10
			}
		}
	}

	visStr := req.Params["VisibilityTimeout"]
	visTimeout := 30
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

	urlKey := sqsURLKey(queueURL)
	now := p.tc.Now()

	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	type msgResult struct {
		MessageID     string `xml:"MessageId"`
		ReceiptHandle string `xml:"ReceiptHandle"`
		MD5OfBody     string `xml:"MD5OfBody"`
		Body          string `xml:"Body"`
	}
	type result struct {
		Message []msgResult `xml:"Message"`
	}
	type response struct {
		XMLName              xml.Name         `xml:"ReceiveMessageResponse"`
		Xmlns                string           `xml:"xmlns,attr"`
		ReceiveMessageResult result           `xml:"ReceiveMessageResult"`
		ResponseMetadata     responseMetadata `xml:"ResponseMetadata"`
	}

	var messages []msgResult
	for _, id := range ids {
		if len(messages) >= maxNum {
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

		// Update receipt handle and visibility timeout.
		newHandle := generateSQSReceiptHandle()
		msg.ReceiptHandle = newHandle
		msg.VisibleAfter = now.Add(time.Duration(visTimeout) * time.Second)
		msg.ReceiveCount++

		if saveErr := p.saveMsg(context.Background(), urlKey, msg); saveErr != nil {
			p.logger.Warn("sqs receiveMessage: failed to update msg", "err", saveErr)
			continue
		}

		messages = append(messages, msgResult{
			MessageID:     msg.MessageID,
			ReceiptHandle: newHandle,
			MD5OfBody:     msg.MD5OfBody,
			Body:          msg.Body,
		})
	}

	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                "http://queue.amazonaws.com/doc/2012-11-05/",
		ReceiveMessageResult: result{Message: messages},
		ResponseMetadata:     responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) deleteMessage(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	receiptHandle := req.Params["ReceiptHandle"]
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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	urlKey := sqsURLKey(queueURL)
	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	type successEntry struct {
		ID string `xml:"Id"`
	}
	var successes []successEntry

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
		successes = append(successes, successEntry{ID: entryID})
	}

	if err := p.saveMsgIDs(context.Background(), urlKey, ids); err != nil {
		return nil, fmt.Errorf("sqs deleteMessageBatch saveMsgIDs: %w", err)
	}

	type result struct {
		DeleteMessageBatchResultEntry []successEntry `xml:"DeleteMessageBatchResultEntry"`
	}
	type response struct {
		XMLName                  xml.Name         `xml:"DeleteMessageBatchResponse"`
		Xmlns                    string           `xml:"xmlns,attr"`
		DeleteMessageBatchResult result           `xml:"DeleteMessageBatchResult"`
		ResponseMetadata         responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                    "http://queue.amazonaws.com/doc/2012-11-05/",
		DeleteMessageBatchResult: result{DeleteMessageBatchResultEntry: successes},
		ResponseMetadata:         responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SQSPlugin) changeMessageVisibility(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
	}

	receiptHandle := req.Params["ReceiptHandle"]
	visStr := req.Params["VisibilityTimeout"]
	vis, _ := strconv.Atoi(visStr)

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
	queueURL := queueURLFromRequest(req)
	q, err := p.loadQueue(context.Background(), queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, &AWSError{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist", HTTPStatus: http.StatusBadRequest}
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
	h := md5.Sum([]byte(s)) //nolint:gosec // MD5 for SQS protocol; not used for security.
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
