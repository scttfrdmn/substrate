package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// sqsMaxVisibilityTimeout is the largest VisibilityTimeout SQS accepts, in seconds:
// "Values range: 0 to 43200. Maximum: 12 hours".
const sqsMaxVisibilityTimeout = 43200

// sqsMaxBatchEntries is the most entries one SQS batch request may carry.
const sqsMaxBatchEntries = 10

// changeVisibility sets the visibility timeout of the message receiptHandle names to
// vis seconds from now. It returns the AWS refusal — as the first result, so a batch
// can report it per entry — when the handle names no message of the queue
// (ReceiptHandleIsInvalid), when the message is not in flight (MessageNotInflight), or
// when vis is outside 0..43200. Before #1462 an unknown handle answered 200 and changed
// nothing, so a consumer extending a lease it had already lost was never told.
func (p *SQSPlugin) changeVisibility(ctx context.Context, urlKey string, ids []string, receiptHandle string, vis int) (*AWSError, error) {
	if vis < 0 || vis > sqsMaxVisibilityTimeout {
		return sqsVisibilityTimeoutOutOfRange(vis), nil
	}
	if receiptHandle == "" {
		return sqsReceiptHandleIsInvalid(receiptHandle), nil
	}
	now := p.tc.Now()
	for _, id := range ids {
		msg, err := p.loadMsg(ctx, urlKey, id)
		if err != nil {
			return nil, err
		}
		if msg == nil || msg.ReceiptHandle != receiptHandle {
			continue
		}
		// In flight: received, and still inside its visibility timeout.
		if msg.ReceiveCount == 0 || msg.VisibleAfter.IsZero() || !now.Before(msg.VisibleAfter) {
			return sqsMessageNotInflight(), nil
		}
		msg.VisibleAfter = now.Add(time.Duration(vis) * time.Second)
		if err := p.saveMsg(ctx, urlKey, msg); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return sqsReceiptHandleIsInvalid(receiptHandle), nil
}

// sqsValidBatchEntryID reports whether id is a valid batch entry Id: 1 to 80
// alphanumeric characters, hyphens and underscores.
func sqsValidBatchEntryID(id string) bool {
	if id == "" || len(id) > 80 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c != '-' && c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

// sqsCheckBatchEntryIDs applies the request-level batch rules every SQS batch
// operation publishes, in the order SQS reports them: no entries
// (EmptyBatchRequest), more than ten (TooManyEntriesInBatchRequest), a repeated Id
// (BatchEntryIdsNotDistinct) and an Id outside the specification (InvalidBatchEntryId).
func sqsCheckBatchEntryIDs(entryName string, ids []string) *AWSError {
	if len(ids) == 0 {
		return sqsEmptyBatchRequest(entryName)
	}
	if len(ids) > sqsMaxBatchEntries {
		return sqsTooManyEntriesInBatchRequest(len(ids))
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return sqsBatchEntryIDsNotDistinct(id)
		}
		seen[id] = true
	}
	for _, id := range ids {
		if !sqsValidBatchEntryID(id) {
			return sqsInvalidBatchEntryID()
		}
	}
	return nil
}

// sqsVisibilityBatchEntry is one entry of a ChangeMessageVisibilityBatch request.
type sqsVisibilityBatchEntry struct {
	ID                string `json:"Id"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

// changeMessageVisibilityBatch implements ChangeMessageVisibilityBatch (#1462): up to
// ten ChangeMessageVisibility requests whose results are reported per entry — an
// invalid receipt handle or a message not in flight fails its entry in Failed at
// HTTP 200 while its siblings proceed.
func (p *SQSPlugin) changeMessageVisibilityBatch(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	queueURL, refusal := sqsQueueURLFromRequest(req)
	if refusal != nil {
		return nil, refusal
	}
	q, err := p.loadQueue(context.Background(), ctx.Region, queueURL)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, sqsQueueDoesNotExist()
	}

	var entries []sqsVisibilityBatchEntry
	if sqsIsJSONProtocol(req) {
		var input struct {
			Entries []sqsVisibilityBatchEntry `json:"Entries"`
		}
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, sqsInvalidBody()
		}
		entries = input.Entries
	} else {
		for i := 1; ; i++ {
			prefix := fmt.Sprintf("ChangeMessageVisibilityBatchRequestEntry.%d.", i)
			id, ok := req.Params[prefix+"Id"]
			if !ok {
				break
			}
			vis, _ := strconv.Atoi(req.Params[prefix+"VisibilityTimeout"])
			entries = append(entries, sqsVisibilityBatchEntry{
				ID: id, ReceiptHandle: req.Params[prefix+"ReceiptHandle"], VisibilityTimeout: vis,
			})
		}
	}

	entryIDs := make([]string, len(entries))
	for i, e := range entries {
		entryIDs[i] = e.ID
	}
	if awsErr := sqsCheckBatchEntryIDs("ChangeMessageVisibilityBatchRequestEntry", entryIDs); awsErr != nil {
		return nil, awsErr
	}

	urlKey := sqsURLKey(ctx.Region, queueURL)
	ids, err := p.loadMsgIDs(context.Background(), urlKey)
	if err != nil {
		return nil, err
	}

	type successEntry struct {
		ID string `xml:"Id" json:"Id"`
	}
	successes := make([]successEntry, 0, len(entries))
	failures := make([]sqsBatchResultErrorEntry, 0)
	for _, e := range entries {
		awsErr, err := p.changeVisibility(context.Background(), urlKey, ids, e.ReceiptHandle, e.VisibilityTimeout)
		if err != nil {
			return nil, fmt.Errorf("sqs changeMessageVisibilityBatch: %w", err)
		}
		if awsErr != nil {
			failures = append(failures, sqsBatchFailure(e.ID, awsErr))
			continue
		}
		successes = append(successes, successEntry{ID: e.ID})
	}

	if sqsIsJSONProtocol(req) {
		return sqsJSONResponse(http.StatusOK, map[string]interface{}{
			"Successful": successes,
			"Failed":     failures,
		})
	}
	type result struct {
		Entries []successEntry             `xml:"ChangeMessageVisibilityBatchResultEntry"`
		Failed  []sqsBatchResultErrorEntry `xml:"BatchResultErrorEntry"`
	}
	type response struct {
		XMLName                            xml.Name         `xml:"ChangeMessageVisibilityBatchResponse"`
		Xmlns                              string           `xml:"xmlns,attr"`
		ChangeMessageVisibilityBatchResult result           `xml:"ChangeMessageVisibilityBatchResult"`
		ResponseMetadata                   responseMetadata `xml:"ResponseMetadata"`
	}
	return sqsXMLResponse(http.StatusOK, response{
		Xmlns:                              "http://queue.amazonaws.com/doc/2012-11-05/",
		ChangeMessageVisibilityBatchResult: result{Entries: successes, Failed: failures},
		ResponseMetadata:                   responseMetadata{RequestID: ctx.RequestID},
	})
}
