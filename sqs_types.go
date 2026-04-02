package substrate

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

	// MessageGroupId is the FIFO message group identifier. Empty for standard queues.
	MessageGroupId string `json:"MessageGroupId,omitempty"`
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
