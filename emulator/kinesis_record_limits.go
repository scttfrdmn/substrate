package emulator

import (
	"encoding/base64"
	"fmt"
	"strings"
	"unicode/utf8"
)

// PutRecord and PutRecords' published limits (#1418).
//
// The limits, and where each is published:
//
//   - **PartitionKey is 1 to 256 characters.** API_PutRecord and API_PutRecordsRequestEntry publish
//     "Length Constraints: Minimum length of 1. Maximum length of 256." The member is Required: No only
//     because a stream using the AUTO record distribution strategy ignores it; the same pages say "If
//     the stream uses the USER_PARTITION_KEY record distribution strategy (the default), a partition
//     key is required". Substrate models no other strategy, so every stream is USER_PARTITION_KEY and
//     an absent key is the too-short case. The length is counted in characters, as the pages state it.
//   - **Records is 1 to 500 entries.** API_PutRecords: "Array Members: Minimum number of 1 item.
//     Maximum number of 500 items."
//   - **A record is at most the stream's maximum record size**, which is 1 MiB unless the stream was
//     created with a larger MaxRecordSizeInKiB (1024 to 10240, API_CreateStream). The large-records
//     guide: "The default maximum record size for existing, and newly created streams is set to 1 MiB."
//     API_PutRecords: "The record size limit applies to the total size of the partition key and data
//     blob", and API_PutRecord says the same of the blob "added to the partition key size". The blob is
//     counted before base64 encoding.
//   - **A PutRecords request is at most 10 MiB**, partition keys included. API_PutRecords and the quotas
//     page both now publish "up to a limit of 10 MiB for the entire request, including partition keys";
//     the 5 MiB figure the pre-large-records pages carried is no longer published, so substrate follows
//     the current pages.
//
// The code is InvalidArgumentException/400 for every one, the only validation code either page's
// Errors section publishes ("A specified parameter exceeds its restrictions, is not supported, or can't
// be used"). ValidationException is not on either page; see kinesis_errors.go. The messages are
// substrate's own.
//
// The order follows the rule every Kinesis handler keeps: what the request alone decides is refused
// before the stream is read, so the partition-key, entry-count and 10 MiB checks run before
// [KinesisPlugin.loadStream]. Only the per-record size against a stream's own maximum needs the
// record, so it runs after, which is the order updateShardCount uses for its bounds stated against the
// current shard count.

const (
	// kinesisPartitionKeyMaxLength is PartitionKey's published maximum length, in characters.
	kinesisPartitionKeyMaxLength = 256

	// kinesisPutRecordsMaxEntries is the Records member's published maximum.
	kinesisPutRecordsMaxEntries = 500

	// kinesisDefaultMaxRecordSizeKiB is a stream's maximum record size when it was created with none.
	kinesisDefaultMaxRecordSizeKiB = 1024

	// kinesisMaxRecordSizeKiBCeiling is MaxRecordSizeInKiB's published maximum.
	kinesisMaxRecordSizeKiBCeiling = 10240

	// kinesisPutRecordsMaxRequestBytes is the published limit on a whole PutRecords request.
	kinesisPutRecordsMaxRequestBytes = 10 << 20
)

// kinesisCheckPartitionKey refuses a partition key outside its published 1-256 character length.
// field names the member in the message, so a PutRecords refusal says which entry it was.
func kinesisCheckPartitionKey(field, key string) *AWSError {
	n := utf8.RuneCountInString(key)
	if n < 1 || n > kinesisPartitionKeyMaxLength {
		return kinesisInvalidArgument(fmt.Sprintf(
			"%s must be between 1 and %d characters long; it is %d.", field, kinesisPartitionKeyMaxLength, n))
	}
	return nil
}

// kinesisRecordSize is the size a record counts against its limits: the data blob before base64
// encoding plus the partition key's bytes.
//
// The blob's decoded length is computed from the encoded length rather than by decoding, so a blob
// that is not valid base64 is sized as what it would decode to. Substrate stores the blob as sent and
// does not refuse a malformed one; that is a separate question from its size.
func kinesisRecordSize(data, partitionKey string) int {
	padding := len(data) - len(strings.TrimRight(data, "="))
	decoded := base64.StdEncoding.DecodedLen(len(data)) - padding
	if len(data)%4 != 0 {
		decoded = base64.RawStdEncoding.DecodedLen(len(data))
	}
	return decoded + len(partitionKey)
}

// kinesisMaxRecordSizeBytes returns a stream's maximum record size in bytes.
func kinesisMaxRecordSizeBytes(stream KinesisStream) int {
	kib := stream.MaxRecordSizeInKiB
	if kib == 0 {
		kib = kinesisDefaultMaxRecordSizeKiB
	}
	return kib << 10
}

// kinesisCheckRecordSize refuses a record larger than its stream's maximum record size.
func kinesisCheckRecordSize(field string, size int, stream KinesisStream) *AWSError {
	if limit := kinesisMaxRecordSizeBytes(stream); size > limit {
		return kinesisInvalidArgument(fmt.Sprintf(
			"%s is %d bytes, data and partition key together, which exceeds the stream's maximum record size of %d bytes.",
			field, size, limit))
	}
	return nil
}

// kinesisValidateMaxRecordSize checks a CreateStream MaxRecordSizeInKiB against its published range,
// 1024 to 10240. A nil value is the member omitted, which leaves the stream at the 1 MiB default.
func kinesisValidateMaxRecordSize(kib *int) *AWSError {
	if kib == nil {
		return nil
	}
	if *kib < kinesisDefaultMaxRecordSizeKiB || *kib > kinesisMaxRecordSizeKiBCeiling {
		return kinesisInvalidArgument(fmt.Sprintf(
			"MaxRecordSizeInKiB %d is not between %d and %d.", *kib, kinesisDefaultMaxRecordSizeKiB, kinesisMaxRecordSizeKiBCeiling))
	}
	return nil
}

// kinesisRecordEntry is the part of a PutRecords entry its limits are checked on.
type kinesisRecordEntry struct {
	Data         string
	PartitionKey string
}

// kinesisCheckPutRecordsRequest checks what a PutRecords request alone decides: the entry count, each
// entry's partition key, and the request's total size. It returns each entry's size so the per-record
// check against the stream's maximum, which needs the record, does not recompute them.
func kinesisCheckPutRecordsRequest(entries []kinesisRecordEntry) ([]int, *AWSError) {
	if n := len(entries); n < 1 || n > kinesisPutRecordsMaxEntries {
		return nil, kinesisInvalidArgument(fmt.Sprintf(
			"Records must hold between 1 and %d entries; it holds %d.", kinesisPutRecordsMaxEntries, n))
	}
	sizes := make([]int, len(entries))
	total := 0
	for i, entry := range entries {
		if refusal := kinesisCheckPartitionKey(fmt.Sprintf("Records[%d].PartitionKey", i), entry.PartitionKey); refusal != nil {
			return nil, refusal
		}
		sizes[i] = kinesisRecordSize(entry.Data, entry.PartitionKey)
		total += sizes[i]
	}
	if total > kinesisPutRecordsMaxRequestBytes {
		return nil, kinesisInvalidArgument(fmt.Sprintf(
			"The request's records total %d bytes, data and partition keys together, which exceeds the limit of %d bytes.",
			total, kinesisPutRecordsMaxRequestBytes))
	}
	return sizes, nil
}
