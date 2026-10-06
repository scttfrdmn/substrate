package emulator

import (
	"crypto/md5" //nolint:gosec // Kinesis publishes MD5 as its partition-key hash; it is not used for security.
	"fmt"
	"math/big"
)

// A record is routed by its hash key (#1409).
//
// API_PutRecord: "An MD5 hash function is used to map partition keys to 128-bit integer values and to
// map associated data records to shards using the hash key ranges of the shards. You can override
// hashing the partition key to determine the shard by explicitly specifying a hash value using the
// ExplicitHashKey parameter." ExplicitHashKey's published pattern is ^(0|([1-9]\d{0,38}))$, which
// admits 39-digit values above the largest hash key, 2^128-1; such a value names no shard's range, so
// substrate refuses it with InvalidArgumentException, the code the page publishes for "a specified
// parameter exceeds its restrictions".

// kinesisRecordHashKey returns the hash key a record is routed by: explicitHashKey when sent,
// otherwise MD5(partitionKey) read as an unsigned big-endian 128-bit integer.
func kinesisRecordHashKey(partitionKey, explicitHashKey string) (*big.Int, *AWSError) {
	if explicitHashKey == "" {
		sum := md5.Sum([]byte(partitionKey)) //nolint:gosec // see the file comment.
		return new(big.Int).SetBytes(sum[:]), nil
	}
	if !kinesisHashKeyPattern.MatchString(explicitHashKey) {
		return nil, kinesisInvalidArgument(fmt.Sprintf(
			"ExplicitHashKey %q does not match the pattern ^(0|([1-9]\\d{0,38}))$.", explicitHashKey))
	}
	key, _ := new(big.Int).SetString(explicitHashKey, 10) // the pattern guarantees a decimal integer
	if key.Cmp(kinesisHashKeySpace) >= 0 {
		return nil, kinesisInvalidArgument(fmt.Sprintf(
			"ExplicitHashKey %s is outside the hash key range 0 to 2^128-1.", explicitHashKey))
	}
	return key, nil
}

// kinesisShardForHashKey returns the ID of the open shard whose hash key range contains key. A closed
// parent takes no more data (#1399), and the open shards of a stream cover the whole hash key space,
// so exactly one open shard matches; finding none means the stored ranges are corrupt.
func kinesisShardForHashKey(stream KinesisStream, key *big.Int) (string, error) {
	for _, shard := range kinesisOpenShards(stream) {
		start, end, err := kinesisHashRange(shard)
		if err != nil {
			return "", fmt.Errorf("route kinesis record: %w", err)
		}
		if key.Cmp(start) >= 0 && key.Cmp(end) <= 0 {
			return shard.ShardID, nil
		}
	}
	return "", fmt.Errorf("route kinesis record: no open shard of stream %s covers hash key %s", stream.StreamName, key)
}
