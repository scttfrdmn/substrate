package emulator

import (
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Resharding acts on the shards a request names (#1399).
//
// MergeShards and SplitShard used to decrement or increment the shard count and regenerate the whole
// shard list, so the shards a caller named were never the ones that changed, parents were never
// closed, and no child ever reported its parent. Each now follows its page.
//
//   - API_MergeShards: two shards "are considered adjacent if the union of the hash key ranges for
//     the two shards form a contiguous set with no gaps"; the single child "receives data for all hash
//     key values covered by the two parent shards". The child reports ShardToMerge as its
//     ParentShardId and AdjacentShardToMerge as its AdjacentParentShardId, the two members
//     API_Shard publishes for a merge.
//   - API_SplitShard: NewStartingHashKey "must be in the range of hash keys being mapped into the
//     shard"; it "and all higher hash key values in hash key range are distributed to one of the child
//     shards. All the lower hash key values in the range are distributed to the other child shard."
//     Both children report the split shard as their ParentShardId.
//
// A parent is closed rather than removed: its SequenceNumberRange gains an EndingSequenceNumber, and
// it stays in DescribeStream's Shards, as a closed shard does in AWS until it ages out of retention
// (which is not modeled). New shard IDs continue the stream's own sequence, shardId-000000000000
// upward, so an ID is never reused.
//
// Hash key ranges are the real ones: a stream's hash key space is 0 to 2^128-1, divided evenly
// between the shards a create or an UpdateShardCount makes, which is what a split key and an
// adjacency check are measured against. Substrate used to give each shard a toy range of a thousand
// keys, against which the published split keys (such as the page's own 2^127) fell outside every
// shard.
//
// Refusals:
//
//   - A shard the stream does not hold is ResourceNotFoundException, as both pages publish for "the
//     requested resource".
//   - Two shards that are not adjacent, the same shard named twice, a closed shard, a split key outside
//     the shard's range or not matching the published pattern, and a missing Required member are each
//     InvalidArgumentException, the code both pages publish for "a specified parameter [that] exceeds
//     its restrictions". The messages are substrate's own wording.
//   - A split key equal to the shard's own starting key is refused too. It is "in the range", but it
//     would leave the lower child with no hash keys at all, which is substrate's reading of why AWS
//     requires it to split the shard in two.

// kinesisHashKeySpace is 2^128, one past the largest hash key, so a shard's range is [start, end]
// with end < kinesisHashKeySpace.
var kinesisHashKeySpace = new(big.Int).Lsh(big.NewInt(1), 128)

// kinesisHashKeyPattern is API_SplitShard's published pattern for NewStartingHashKey.
var kinesisHashKeyPattern = regexp.MustCompile(`^(0|([1-9]\d{0,38}))$`)

// kinesisUniformShards makes n open shards dividing the whole hash key space evenly, numbered from
// firstIndex. The last shard absorbs the remainder, so the ranges cover 0 to 2^128-1 with no gap.
func kinesisUniformShards(firstIndex, n int, startingSequence string) []KinesisShard {
	shards := make([]KinesisShard, n)
	width := new(big.Int).Div(kinesisHashKeySpace, big.NewInt(int64(n)))
	for i := range shards {
		start := new(big.Int).Mul(width, big.NewInt(int64(i)))
		end := new(big.Int).Sub(new(big.Int).Mul(width, big.NewInt(int64(i+1))), big.NewInt(1))
		if i == n-1 {
			end = new(big.Int).Sub(kinesisHashKeySpace, big.NewInt(1))
		}
		shards[i] = KinesisShard{ShardID: kinesisShardID(firstIndex + i)}
		shards[i].HashKeyRange.StartingHashKey = start.String()
		shards[i].HashKeyRange.EndingHashKey = end.String()
		shards[i].SequenceNumberRange.StartingSequenceNumber = startingSequence
	}
	return shards
}

// kinesisShardID renders a shard index in the published form, shardId-000000000000.
func kinesisShardID(index int) string { return fmt.Sprintf("shardId-%012d", index) }

// kinesisNextShardIndex returns one past the highest shard index the stream has ever used, so a new
// shard never reuses an ID, closed shards included.
func kinesisNextShardIndex(stream KinesisStream) int {
	next := 0
	for _, shard := range stream.Shards {
		n, err := strconv.Atoi(strings.TrimPrefix(shard.ShardID, "shardId-"))
		if err == nil && n+1 > next {
			next = n + 1
		}
	}
	return next
}

// kinesisShardOpen reports whether a shard still accepts records: it has no EndingSequenceNumber.
func kinesisShardOpen(shard KinesisShard) bool {
	return shard.SequenceNumberRange.EndingSequenceNumber == ""
}

// kinesisOpenShards returns the stream's open shards, in order.
func kinesisOpenShards(stream KinesisStream) []KinesisShard {
	open := make([]KinesisShard, 0, len(stream.Shards))
	for _, shard := range stream.Shards {
		if kinesisShardOpen(shard) {
			open = append(open, shard)
		}
	}
	return open
}

// kinesisFindShard returns the index of the named shard in the stream, refusing a shard the stream
// does not hold with ResourceNotFoundException and a closed one with InvalidArgumentException.
func kinesisFindShard(stream KinesisStream, target kinesisStreamTarget, shardID string) (int, *AWSError) {
	for i, shard := range stream.Shards {
		if shard.ShardID != shardID {
			continue
		}
		if !kinesisShardOpen(shard) {
			return -1, kinesisInvalidArgument(fmt.Sprintf("Shard %s in stream %s under account %s is closed.",
				shardID, target.Name, target.AccountID))
		}
		return i, nil
	}
	return -1, &AWSError{
		Code: "ResourceNotFoundException",
		Message: fmt.Sprintf("Could not find shard %s in stream %s under account %s.",
			shardID, target.Name, target.AccountID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kinesisHashRange parses a shard's hash key range.
func kinesisHashRange(shard KinesisShard) (start, end *big.Int, err error) {
	start, ok := new(big.Int).SetString(shard.HashKeyRange.StartingHashKey, 10)
	if !ok {
		return nil, nil, fmt.Errorf("kinesis shard %s: unparsable StartingHashKey %q", shard.ShardID, shard.HashKeyRange.StartingHashKey)
	}
	end, ok = new(big.Int).SetString(shard.HashKeyRange.EndingHashKey, 10)
	if !ok {
		return nil, nil, fmt.Errorf("kinesis shard %s: unparsable EndingHashKey %q", shard.ShardID, shard.HashKeyRange.EndingHashKey)
	}
	return start, end, nil
}

// kinesisCloseShard closes a shard at the given sequence number.
func kinesisCloseShard(shard *KinesisShard, endingSequence string) {
	shard.SequenceNumberRange.EndingSequenceNumber = endingSequence
}

// kinesisMergeShards merges the two named shards of stream in place, returning the refusal a request
// that cannot be honored answers. The two sequence numbers close the parents and open the child.
func kinesisMergeShards(stream *KinesisStream, target kinesisStreamTarget, shardToMerge, adjacent string, closeSeq, openSeq string) error {
	if shardToMerge == "" || adjacent == "" {
		return kinesisInvalidArgument("ShardToMerge and AdjacentShardToMerge are required.")
	}
	if shardToMerge == adjacent {
		return kinesisInvalidArgument(fmt.Sprintf("ShardToMerge and AdjacentShardToMerge both name %s.", shardToMerge))
	}
	i, refusal := kinesisFindShard(*stream, target, shardToMerge)
	if refusal != nil {
		return refusal
	}
	j, refusal := kinesisFindShard(*stream, target, adjacent)
	if refusal != nil {
		return refusal
	}
	aStart, aEnd, err := kinesisHashRange(stream.Shards[i])
	if err != nil {
		return err
	}
	bStart, bEnd, err := kinesisHashRange(stream.Shards[j])
	if err != nil {
		return err
	}
	one := big.NewInt(1)
	contiguous := new(big.Int).Add(aEnd, one).Cmp(bStart) == 0 || new(big.Int).Add(bEnd, one).Cmp(aStart) == 0
	if !contiguous {
		return kinesisInvalidArgument(fmt.Sprintf("Shards %s and %s in stream %s under account %s are not adjacent.",
			shardToMerge, adjacent, target.Name, target.AccountID))
	}
	start, end := aStart, aEnd
	if bStart.Cmp(start) < 0 {
		start = bStart
	}
	if bEnd.Cmp(end) > 0 {
		end = bEnd
	}

	child := KinesisShard{
		ShardID:               kinesisShardID(kinesisNextShardIndex(*stream)),
		ParentShardID:         shardToMerge,
		AdjacentParentShardID: adjacent,
	}
	child.HashKeyRange.StartingHashKey = start.String()
	child.HashKeyRange.EndingHashKey = end.String()
	child.SequenceNumberRange.StartingSequenceNumber = openSeq

	kinesisCloseShard(&stream.Shards[i], closeSeq)
	kinesisCloseShard(&stream.Shards[j], closeSeq)
	stream.Shards = append(stream.Shards, child)
	stream.ShardCount = len(kinesisOpenShards(*stream))
	return nil
}

// kinesisSplitShard splits the named shard of stream at newStartingHashKey in place.
func kinesisSplitShard(stream *KinesisStream, target kinesisStreamTarget, shardToSplit, newStartingHashKey string, closeSeq, openSeq string) error {
	if shardToSplit == "" || newStartingHashKey == "" {
		return kinesisInvalidArgument("ShardToSplit and NewStartingHashKey are required.")
	}
	if !kinesisHashKeyPattern.MatchString(newStartingHashKey) {
		return kinesisInvalidArgument(fmt.Sprintf("NewStartingHashKey %q does not match the pattern ^(0|([1-9]\\d{0,38}))$.", newStartingHashKey))
	}
	i, refusal := kinesisFindShard(*stream, target, shardToSplit)
	if refusal != nil {
		return refusal
	}
	start, end, err := kinesisHashRange(stream.Shards[i])
	if err != nil {
		return err
	}
	key, _ := new(big.Int).SetString(newStartingHashKey, 10) // the pattern guarantees a decimal integer
	if key.Cmp(start) <= 0 || key.Cmp(end) > 0 {
		return kinesisInvalidArgument(fmt.Sprintf(
			"NewStartingHashKey %s is not inside the hash key range of shard %s (%s to %s), above its starting key.",
			newStartingHashKey, shardToSplit, start, end))
	}

	next := kinesisNextShardIndex(*stream)
	lower := KinesisShard{ShardID: kinesisShardID(next), ParentShardID: shardToSplit}
	lower.HashKeyRange.StartingHashKey = start.String()
	lower.HashKeyRange.EndingHashKey = new(big.Int).Sub(key, big.NewInt(1)).String()
	lower.SequenceNumberRange.StartingSequenceNumber = openSeq
	upper := KinesisShard{ShardID: kinesisShardID(next + 1), ParentShardID: shardToSplit}
	upper.HashKeyRange.StartingHashKey = key.String()
	upper.HashKeyRange.EndingHashKey = end.String()
	upper.SequenceNumberRange.StartingSequenceNumber = openSeq

	kinesisCloseShard(&stream.Shards[i], closeSeq)
	stream.Shards = append(stream.Shards, lower, upper)
	stream.ShardCount = len(kinesisOpenShards(*stream))
	return nil
}

// kinesisReshardUniform closes every open shard and opens target new ones dividing the hash key
// space evenly, the shape UpdateShardCount's UNIFORM_SCALING produces. The new shards continue the
// stream's shard IDs, so none collides with a shard a merge or split made.
func kinesisReshardUniform(stream *KinesisStream, target int, closeSeq, openSeq string) {
	for i := range stream.Shards {
		if kinesisShardOpen(stream.Shards[i]) {
			kinesisCloseShard(&stream.Shards[i], closeSeq)
		}
	}
	stream.Shards = append(stream.Shards, kinesisUniformShards(kinesisNextShardIndex(*stream), target, openSeq)...)
	stream.ShardCount = target
}

// kinesisReshardSequences mints the two sequence numbers a reshard needs: the one that closes the
// parents, then the later one the children start at.
func kinesisReshardSequences(now time.Time, ids *IDMint) (closeSeq, openSeq string) {
	closeSeq = generateKinesisSeqNo(now, ids)
	openSeq = generateKinesisSeqNo(now.Add(time.Nanosecond), ids)
	return closeSeq, openSeq
}
