package substrate

import "time"

// kinesisNamespace is the state namespace for Kinesis.
const kinesisNamespace = "kinesis"

// KinesisStream holds the persisted state of a Kinesis data stream.
type KinesisStream struct {
	// StreamName is the name of the Kinesis stream.
	StreamName string `json:"StreamName"`

	// StreamArn is the Amazon Resource Name for the stream.
	StreamArn string `json:"StreamArn"`

	// StreamStatus is the current status: CREATING, ACTIVE, UPDATING, or DELETING.
	StreamStatus string `json:"StreamStatus"`

	// ShardCount is the current number of open shards in the stream.
	ShardCount int `json:"ShardCount"`

	// Shards contains the individual shard descriptors.
	Shards []KinesisShard `json:"Shards,omitempty"`

	// RetentionPeriodHours is the number of hours data is retained (default 24).
	RetentionPeriodHours int `json:"RetentionPeriodHours"`

	// Tags holds arbitrary key-value metadata attached to the stream.
	Tags map[string]string `json:"Tags,omitempty"`

	// EnhancedMonitoring lists the shard-level metrics enabled on the stream.
	EnhancedMonitoring []string `json:"EnhancedMonitoring,omitempty"`

	// CreatedAt is the time the stream was created.
	CreatedAt time.Time `json:"CreatedAt"`

	// AccountID is the AWS account that owns the stream.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the stream resides.
	Region string `json:"Region"`
}

// KinesisShard describes a shard within a Kinesis stream.
type KinesisShard struct {
	// ShardID is the unique identifier of the shard.
	ShardID string `json:"ShardId"`

	// ParentShardID is the shard ID of the parent shard, if any.
	ParentShardID string `json:"ParentShardId,omitempty"`

	// HashKeyRange defines the range of partition keys that map to this shard.
	HashKeyRange struct {
		// StartingHashKey is the first hash key in the range.
		StartingHashKey string `json:"StartingHashKey"`
		// EndingHashKey is the last hash key in the range.
		EndingHashKey string `json:"EndingHashKey"`
	} `json:"HashKeyRange"`

	// SequenceNumberRange defines the range of sequence numbers for records in this shard.
	SequenceNumberRange struct {
		// StartingSequenceNumber is the first sequence number in the range.
		StartingSequenceNumber string `json:"StartingSequenceNumber"`
		// EndingSequenceNumber is the last sequence number; empty for open shards.
		EndingSequenceNumber string `json:"EndingSequenceNumber,omitempty"`
	} `json:"SequenceNumberRange"`
}

// KinesisRecord is a single data record stored in a Kinesis shard.
type KinesisRecord struct {
	// SequenceNumber uniquely identifies the record within its shard.
	SequenceNumber string `json:"SequenceNumber"`

	// ApproximateArrivalTimestamp is the time the record was added to the stream.
	ApproximateArrivalTimestamp time.Time `json:"ApproximateArrivalTimestamp"`

	// Data is the base64-encoded payload of the record.
	Data string `json:"Data"`

	// PartitionKey is the partition key used to determine the shard assignment.
	PartitionKey string `json:"PartitionKey"`

	// ShardID is the shard that holds this record.
	ShardID string `json:"ShardId"`
}

// kinesisStreamKey returns the state key for a Kinesis stream.
func kinesisStreamKey(accountID, region, name string) string {
	return "stream:" + accountID + "/" + region + "/" + name
}

// kinesisStreamNamesKey returns the state index key for all stream names in an account/region.
func kinesisStreamNamesKey(accountID, region string) string {
	return "stream_names:" + accountID + "/" + region
}

// kinesisRecordKey returns the state key for the records of a specific shard.
func kinesisRecordKey(accountID, region, name, shardID string) string {
	return "record:" + accountID + "/" + region + "/" + name + "/" + shardID
}
