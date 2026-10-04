package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// An SQS event-source mapping polls on the simulated clock (#1292). See clock_driven.go for
// the model; this file is the Lambda half of it.
//
// # The cadence
//
// A mapping polls once per [esmPollInterval] of simulated time, starting one interval
// after it is created or enabled. AWS publishes no polling cadence for an SQS mapping;
// one second is the interval substrate's goroutine poller used, kept so a test written
// against it sees the same rhythm. It is measured in simulated time, so a frozen clock
// polls never and a scaled clock polls proportionally faster or slower.
//
// # When several polls are due at once
//
// A request arriving after a gap finds every poll in the gap due. They run in order, at
// the one clock reading the request carries, until one receives nothing: every later poll
// in the gap would see the same queue at the same instant, so it would receive nothing too,
// and the cursor jumps to the first poll after now. [esmMaxPollsPerRun] bounds the work one
// request can trigger; polls beyond it stay due and run on the next request, which keeps a
// queue whose invocations keep failing from spinning a request indefinitely.
//
// # The cursor
//
// When a mapping's next poll is due is state, stored at esm_poll:{uuid} beside the mapping
// rather than on [ESMConfig], which every event-source-mapping operation answers whole. A
// state key is what a snapshot restores and what a replay rebuilds identically.

// esmPollInterval is how much simulated time separates two polls of one mapping.
const esmPollInterval = time.Second

// esmMaxPollsPerRun bounds the polls of one mapping a single request can trigger.
const esmMaxPollsPerRun = 100

// esmPollCursor records when a mapping's next poll is due.
type esmPollCursor struct {
	// Next is the simulated time at which the next poll is due.
	Next time.Time `json:"next"`
}

func esmPollCursorKey(uuid string) string { return "esm_poll:" + uuid }

func (p *LambdaPlugin) loadPollCursor(ctx context.Context, uuid string) (*esmPollCursor, error) {
	data, err := p.state.Get(ctx, lambdaNamespace, esmPollCursorKey(uuid))
	if err != nil {
		return nil, fmt.Errorf("lambda load ESM poll cursor: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var cur esmPollCursor
	if err := json.Unmarshal(data, &cur); err != nil {
		return nil, fmt.Errorf("lambda decode ESM poll cursor: %w", err)
	}
	return &cur, nil
}

func (p *LambdaPlugin) savePollCursor(ctx context.Context, uuid string, cur esmPollCursor) error {
	data, err := json.Marshal(cur)
	if err != nil {
		return fmt.Errorf("lambda marshal ESM poll cursor: %w", err)
	}
	if err := p.state.Put(ctx, lambdaNamespace, esmPollCursorKey(uuid), data); err != nil {
		return fmt.Errorf("lambda save ESM poll cursor: %w", err)
	}
	return nil
}

// activatePolling starts polling a mapping: its first poll is due one interval from now.
func (p *LambdaPlugin) activatePolling(ctx context.Context, uuid string) error {
	if err := p.savePollCursor(ctx, uuid, esmPollCursor{Next: p.tc.Now().Add(esmPollInterval)}); err != nil {
		return err
	}
	p.esmMu.Lock()
	p.esmActive[uuid] = struct{}{}
	p.esmMu.Unlock()
	return nil
}

// deactivatePolling stops polling a mapping and removes its cursor.
func (p *LambdaPlugin) deactivatePolling(ctx context.Context, uuid string) error {
	p.esmMu.Lock()
	delete(p.esmActive, uuid)
	p.esmMu.Unlock()
	if err := p.state.Delete(ctx, lambdaNamespace, esmPollCursorKey(uuid)); err != nil {
		return fmt.Errorf("lambda delete ESM poll cursor: %w", err)
	}
	return nil
}

// RunDue runs every poll due at the simulated clock's current reading, for every enabled SQS
// mapping, in UUID order. It implements [ClockDrivenPlugin].
func (p *LambdaPlugin) RunDue(trigger *RequestContext, dispatch InternalDispatch) error {
	p.esmMu.Lock()
	uuids := make([]string, 0, len(p.esmActive))
	for uuid := range p.esmActive {
		uuids = append(uuids, uuid)
	}
	p.esmMu.Unlock()
	if len(uuids) == 0 {
		return nil
	}
	sort.Strings(uuids)

	p.pollMu.Lock()
	defer p.pollMu.Unlock()

	ctx := context.Background()
	now := p.tc.Now()
	for _, uuid := range uuids {
		if err := p.runDueFor(ctx, trigger, dispatch, uuid, now); err != nil {
			return err
		}
	}
	return nil
}

// runDueFor runs one mapping's due polls and advances its cursor.
func (p *LambdaPlugin) runDueFor(ctx context.Context, trigger *RequestContext, dispatch InternalDispatch, uuid string, now time.Time) error {
	esm, err := p.loadESM(ctx, uuid)
	if err != nil {
		return err
	}
	if esm == nil || esm.State != "Enabled" {
		return nil
	}
	cur, err := p.loadPollCursor(ctx, uuid)
	if err != nil {
		return err
	}
	if cur == nil {
		return p.savePollCursor(ctx, uuid, esmPollCursor{Next: now.Add(esmPollInterval)})
	}
	if cur.Next.After(now) {
		return nil
	}

	for n := 0; !cur.Next.After(now) && n < esmMaxPollsPerRun; n++ {
		received, err := p.pollAndInvoke(trigger, dispatch, *esm, n)
		if err != nil {
			return err
		}
		if received == 0 {
			// Every later poll in the gap would see this same queue at this same instant.
			gap := now.Sub(cur.Next)
			cur.Next = cur.Next.Add((gap/esmPollInterval + 1) * esmPollInterval)
			break
		}
		cur.Next = cur.Next.Add(esmPollInterval)
	}
	return p.savePollCursor(ctx, uuid, *cur)
}

// pollAndInvoke runs one poll: it receives a batch from the queue, invokes the function with
// it, and deletes the batch if the invocation succeeded. It returns how many messages it
// received. Every request goes through dispatch, under a request ID derived from the
// triggering request, the mapping and the poll's index, so a replay derives the same ones.
func (p *LambdaPlugin) pollAndInvoke(trigger *RequestContext, dispatch InternalDispatch, esm ESMConfig, poll int) (int, error) {
	// The queue is named by its ARN: arn:aws:sqs:{region}:{account}:{name}.
	arnParts := strings.Split(esm.EventSourceARN, ":")
	if len(arnParts) < 6 {
		return 0, nil
	}
	region, acct, queueName := arnParts[3], arnParts[4], arnParts[5]
	queueURL := "http://sqs." + region + ".localhost/" + acct + "/" + queueName

	batchSize := esm.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}

	label := esm.UUID + "/" + strconv.Itoa(poll)
	ctxFor := func(step, account, reg string) *RequestContext {
		id := clockDrivenRequestID(trigger.RequestID, label+"/"+step)
		return &RequestContext{
			RequestID: id,
			AccountID: account,
			Region:    reg,
			Timestamp: p.tc.Now(),
			IDs:       NewIDMint(id),
			Metadata:  map[string]interface{}{"stream_id": streamIDFromContext(trigger)},
		}
	}

	rxResp, err := dispatch(ctxFor("receive", acct, region), &AWSRequest{
		Service:   "sqs",
		Operation: "ReceiveMessage",
		Params: map[string]string{
			"Action":              "ReceiveMessage",
			"QueueUrl":            queueURL,
			"MaxNumberOfMessages": strconv.Itoa(batchSize),
			"WaitTimeSeconds":     "0",
		},
		Headers: map[string]string{},
		Path:    "/",
	})
	// A refused receive (a queue the mapping names but nobody has created yet, say) is an
	// empty poll, as a real mapping's poll of it is. Anything else failed.
	var refused *AWSError
	if errors.As(err, &refused) || (err == nil && rxResp == nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("lambda ESM receive: %w", err)
	}

	type sqsMessage struct {
		MessageID     string `xml:"MessageId"`
		ReceiptHandle string `xml:"ReceiptHandle"`
		Body          string `xml:"Body"`
	}
	type rxResult struct {
		Messages []sqsMessage `xml:"ReceiveMessageResult>Message"`
	}
	var parsed rxResult
	if err := xml.Unmarshal(rxResp.Body, &parsed); err != nil {
		return 0, fmt.Errorf("lambda ESM decode ReceiveMessage: %w", err)
	}
	if len(parsed.Messages) == 0 {
		return 0, nil
	}

	type sqsRecord struct {
		MessageID      string            `json:"messageId"`
		ReceiptHandle  string            `json:"receiptHandle"`
		Body           string            `json:"body"`
		Attributes     map[string]string `json:"attributes"`
		EventSource    string            `json:"eventSource"`
		EventSourceARN string            `json:"eventSourceARN"`
		AWSRegion      string            `json:"awsRegion"`
	}
	records := make([]sqsRecord, len(parsed.Messages))
	for i, m := range parsed.Messages {
		records[i] = sqsRecord{
			MessageID:      m.MessageID,
			ReceiptHandle:  m.ReceiptHandle,
			Body:           m.Body,
			Attributes:     map[string]string{},
			EventSource:    "aws:sqs",
			EventSourceARN: esm.EventSourceARN,
			AWSRegion:      region,
		}
	}
	eventJSON, err := json.Marshal(map[string]interface{}{"Records": records})
	if err != nil {
		return 0, fmt.Errorf("lambda ESM marshal event: %w", err)
	}

	// The invoke carries the function's own account and Region, from its ARN
	// (arn:aws:lambda:{region}:{account}:function:{name}): since #943 the function's state key
	// is built from them, and a mapping can name a queue in one account and a function in
	// another.
	fnParts := strings.Split(esm.FunctionARN, ":")
	if len(fnParts) < 7 {
		return len(parsed.Messages), nil
	}
	fnRegion, fnAcct, fnName := fnParts[3], fnParts[4], fnParts[6]
	invokeResp, invokeErr := dispatch(ctxFor("invoke", fnAcct, fnRegion), &AWSRequest{
		Service:    "lambda",
		Operation:  "Invoke",
		HTTPMethod: http.MethodPost,
		Path:       "/2015-03-31/functions/" + fnName + "/invocations",
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       eventJSON,
	})
	if invokeErr != nil {
		// A failed invocation leaves the batch on the queue, invisible until its visibility
		// timeout passes, which is how a real mapping retries it.
		p.logger.Warn("lambda ESM: invoke failed", "function", fnName, "err", invokeErr)
		return len(parsed.Messages), nil
	}
	if invokeResp != nil && invokeResp.StatusCode >= http.StatusMultipleChoices {
		p.logger.Warn("lambda ESM: invoke non-2xx", "function", fnName, "status", invokeResp.StatusCode)
		return len(parsed.Messages), nil
	}

	for i, m := range parsed.Messages {
		if _, err := dispatch(ctxFor("delete/"+strconv.Itoa(i), acct, region), &AWSRequest{
			Service:   "sqs",
			Operation: "DeleteMessage",
			Params: map[string]string{
				"Action":        "DeleteMessage",
				"QueueUrl":      queueURL,
				"ReceiptHandle": m.ReceiptHandle,
			},
			Headers: map[string]string{},
			Path:    "/",
		}); err != nil {
			return len(parsed.Messages), fmt.Errorf("lambda ESM delete message: %w", err)
		}
	}
	return len(parsed.Messages), nil
}
