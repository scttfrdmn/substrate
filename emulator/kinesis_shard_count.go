package emulator

import "fmt"

// kinesisMaxShardsPerStream is the shard ceiling API_UpdateShardCount publishes for a stream.
//
// "Set this value to more than 10000 shards in a stream (the default limit for shard count per
// stream is 10000 per account per region), unless you request a limit increase." Substrate models
// no quota increase, so the default is the bound.
const kinesisMaxShardsPerStream = 10000

// kinesisValidateShardCountUpdate checks an UpdateShardCount request against its published shape.
//
// Until #1076 the handler decoded ScalingType and TargetShardCount and read neither, so an absent
// scaling type, a misspelled one, a zero target and a tenfold scale-up were all accepted and
// written to the stream. The page publishes:
//
//   - ScalingType — Required: Yes, Valid Values: UNIFORM_SCALING.
//   - TargetShardCount — Required: Yes, Valid Range: Minimum value of 1.
//   - seven "default limits", of which the four checked here are the minimum's companions: no more
//     than double the current count, no less than half it, and no more than 10 000 shards.
//
// The code is InvalidArgumentException/400 for all of them, and the choice matters twice over.
//
// The issue asked for ValidationException, which the page does publish — but its gloss is
// capacity-mode-specific ("Specifies that you tried to invoke this API for a data stream with the
// on-demand capacity mode"), and [kinesisInvalidArgument]'s file comment already records from #950
// that the code must not be read as a generic validation code despite the name. Substrate cannot
// answer it at all until capacity mode exists (#1118).
//
// LimitExceededException is the other candidate and is rejected: its gloss is about a resource
// exceeding a maximum or a concurrent-request count, and the page attributes it explicitly to one
// rule only, the 10 TPS call rate. The double/half/ceiling rules are stated inside the
// TargetShardCount parameter entry, and "a specified parameter exceeds its restrictions" is
// InvalidArgumentException's own sentence. One code for the whole shape is also the "one helper,
// one status" shape #950 left the plugin in.
//
// Three published restrictions are deliberately unenforced, because asserting them would mean
// modeling state substrate does not hold: the ten-scalings-per-rolling-24-hours limit needs a
// request history, the account shard limit needs an account quota, and the ">10 000 shards may
// only scale down below 10 000" rule is unreachable while the ceiling above is enforced. The 10
// TPS call rate is a rate limit, not a request property.
//
// target is a pointer so that an absent member is reported as absent rather than as a zero that
// fails the minimum — the distinction [kinesisValidateShardLevelMetrics] draws with a nil slice.
func kinesisValidateShardCountUpdate(scalingType string, target *int, current int) *AWSError {
	if scalingType == "" {
		return kinesisInvalidArgument("ScalingType is required")
	}
	if scalingType != kinesisUniformScaling {
		return kinesisInvalidArgument(fmt.Sprintf(
			"%q is not a valid ScalingType; the only valid value is %s",
			scalingType, kinesisUniformScaling))
	}
	if target == nil {
		return kinesisInvalidArgument("TargetShardCount is required")
	}
	if *target < 1 {
		return kinesisInvalidArgument(fmt.Sprintf(
			"TargetShardCount must be at least 1; got %d", *target))
	}
	if *target > kinesisMaxShardsPerStream {
		return kinesisInvalidArgument(fmt.Sprintf(
			"TargetShardCount may not exceed %d shards in a stream; got %d",
			kinesisMaxShardsPerStream, *target))
	}
	if *target > current*2 {
		return kinesisInvalidArgument(fmt.Sprintf(
			"TargetShardCount may not be more than double the stream's current %d shards; got %d",
			current, *target))
	}
	// Stated as "Scale down below half your current shard count", so the bound is half itself and
	// the comparison is doubled rather than halved to keep an odd current count exact: against 3
	// shards a target of 2 is permitted and 1 is not.
	if *target*2 < current {
		return kinesisInvalidArgument(fmt.Sprintf(
			"TargetShardCount may not be less than half the stream's current %d shards; got %d",
			current, *target))
	}
	return nil
}

// kinesisUniformScaling is the only value API_UpdateShardCount publishes for ScalingType.
const kinesisUniformScaling = "UNIFORM_SCALING"
