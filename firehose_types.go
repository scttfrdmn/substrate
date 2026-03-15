package substrate

import "time"

// firehoseNamespace is the state namespace for Kinesis Data Firehose resources.
const firehoseNamespace = "firehose"

// FirehoseDeliveryStream represents a Kinesis Data Firehose delivery stream.
type FirehoseDeliveryStream struct {
	// DeliveryStreamName is the name of the delivery stream.
	DeliveryStreamName string `json:"DeliveryStreamName"`
	// DeliveryStreamARN is the ARN of the delivery stream.
	DeliveryStreamARN string `json:"DeliveryStreamARN"`
	// DeliveryStreamStatus is the stream status (e.g., "ACTIVE").
	DeliveryStreamStatus string `json:"DeliveryStreamStatus"`
	// DeliveryStreamType is the stream type (e.g., "DirectPut").
	DeliveryStreamType string `json:"DeliveryStreamType"`
	// AccountID is the AWS account ID that owns this stream.
	AccountID string `json:"AccountId"`
	// Region is the AWS region where the stream exists.
	Region string `json:"Region"`
	// Tags holds optional resource tags.
	Tags map[string]string `json:"Tags,omitempty"`
	// CreatedAt is the time the stream was created.
	CreatedAt time.Time `json:"CreateTimestamp"`
}
