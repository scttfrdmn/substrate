package emulator

import "time"

// sqsNamespace is the state namespace used by SQSPlugin.
const sqsNamespace = "sqs"

// SQSQueue represents an emulated Amazon SQS queue.
type SQSQueue struct {
	// QueueName is the name of the queue.
	QueueName string `json:"QueueName"`

	// QueueURL is the URL of the queue.
	QueueURL string `json:"QueueURL"`

	// QueueARN is the Amazon Resource Name of the queue.
	QueueARN string `json:"QueueARN"`

	// Attributes holds queue attributes as string key-value pairs.
	Attributes map[string]string `json:"Attributes,omitempty"`

	// Tags holds user-defined key-value tags applied to the queue.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedTimestamp is the Unix epoch time at which the queue was created.
	CreatedTimestamp int64 `json:"CreatedTimestamp"`

	// LastModifiedTimestamp is the Unix epoch time of the last attribute change.
	LastModifiedTimestamp int64 `json:"LastModifiedTimestamp"`

	// FifoQueue indicates whether this is a FIFO queue.
	FifoQueue bool `json:"FifoQueue"`

	// EverTagged records that this queue has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// SQSMessage represents a single message in an SQS queue.
type SQSMessage struct {
	// MessageID is the message identifier.
	MessageID string `json:"MessageId"`

	// ReceiptHandle is the identifier used to delete the message after processing.
	ReceiptHandle string `json:"ReceiptHandle"`

	// Body is the message body.
	Body string `json:"Body"`

	// MD5OfBody is the MD5 digest of the message body.
	MD5OfBody string `json:"MD5OfBody"`

	// Attributes holds system message attributes (SenderId, SentTimestamp, etc.).
	Attributes map[string]string `json:"Attributes,omitempty"`

	// MessageAttributes holds user-defined message attributes.
	MessageAttributes map[string]SQSMessageAttribute `json:"MessageAttributes,omitempty"`

	// SentTimestamp is the Unix epoch milliseconds at which the message was sent.
	SentTimestamp int64 `json:"SentTimestamp"`

	// VisibleAfter is the earliest time after which the message is visible.
	// Zero means immediately visible.
	VisibleAfter time.Time `json:"VisibleAfter"`

	// DelayUntil is the time before which the message is not delivered.
	DelayUntil time.Time `json:"DelayUntil"`

	// ReceiveCount is how many times the message has been received.
	ReceiveCount int `json:"ReceiveCount"`

	// MessageGroupID is the FIFO message group identifier. Empty for standard queues.
	MessageGroupID string `json:"MessageGroupId,omitempty"`
}

// SQSMessageAttribute holds a single user-defined message attribute.
type SQSMessageAttribute struct {
	// DataType is the attribute data type (e.g., "String", "Number", "Binary").
	DataType string `json:"DataType"`

	// StringValue is the attribute value when DataType is "String" or "Number".
	StringValue string `json:"StringValue,omitempty"`

	// BinaryValue is the attribute value when DataType is "Binary".
	BinaryValue []byte `json:"BinaryValue,omitempty"`
}

// sqsQueueURL constructs the local queue URL for testing.
func sqsQueueURL(region, accountID, name string) string {
	return "http://sqs." + region + ".localhost/" + accountID + "/" + name
}

// sqsQueueARN constructs the ARN for an SQS queue.
func sqsQueueARN(region, accountID, name string) string {
	return "arn:aws:sqs:" + region + ":" + accountID + ":" + name
}

// sqsQueueStateKey names the record one queue lives in, qualified by account, Region and name.
//
// Until #1088 the key was account and name alone, built by taking the last two components of a queue
// URL — which skips the Region, because a queue URL carries it in the **host**. So one name was one
// record across every Region, and the consequence was not merely colliding state: `CreateQueue` for a
// name another Region already held found that record, took its idempotent branch and answered the
// **other Region's URL**, so every later call the caller made addressed the wrong endpoint and
// nothing refused it.
//
// AWS publishes no prose scoping a queue name to a Region, so the citation is structural: the sample
// queue URL carries the Region in the host on all four published protocol variants, and
// `API_GetQueueUrl` publishes **no Region parameter** — the Region comes from the endpoint alone.
// Two endpoints are therefore two namespaces, or the URL in the response is wrong.
//
// The precedent is [lambdaFunctionStateKey], whose own doc comment records the same defect in the
// same words for #943; this is that fix in the one service the sweep left, since Budgets,
// Organizations and IAM are global and ELB's prefix was already account and Region scoped.
func sqsQueueStateKey(accountID, region, name string) string {
	return "queue:" + sqsQueueKeyComponent(accountID, region, name)
}

// sqsQueueKeyComponent is the account/Region/name triple the queue key and every key derived from it
// share, so a message key and its queue's key cannot disagree about the scope.
//
// The `msg:`, `msg_ids:` and `fifo_dedup:` keys are all built from this rather than from the queue
// key, which is why they moved with it rather than needing their own decision.
func sqsQueueKeyComponent(accountID, region, name string) string {
	return accountID + "/" + region + "/" + name
}

// sqsQueueKeyPrefix is the [sqsQueueStateKey] prefix selecting one account's queues in one Region,
// for a scan that must not reach another account's or another Region's.
//
// Copied from [lambdaFunctionKeyPrefix] along with the key itself, because a Region-qualified key
// without its prefix leaves every scan to rebuild the shape by hand — which is how
// `TaggingPlugin.scanSQSQueues` came to hold a literal `"queue:" + accountID + "/"` that stopped
// being the whole scope the moment the Region went in.
func sqsQueueKeyPrefix(accountID, region string) string {
	return "queue:" + accountID + "/" + region + "/"
}
