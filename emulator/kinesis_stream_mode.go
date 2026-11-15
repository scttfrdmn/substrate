package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// A stream's capacity mode (#1118).
//
// API_StreamModeDetails has one member, StreamMode, Required: Yes, Valid Values PROVISIONED |
// ON_DEMAND. CreateStream takes it as StreamModeDetails (Required: No) and UpdateStreamMode as the
// same member (Required: Yes, alongside StreamARN, Required: Yes). Both describe shapes carry it as
// Required: No, and substrate always renders it, from the record.
//
// What the mode changes in substrate is the four refusals it is the precondition of: API_UpdateShardCount,
// API_MergeShards and API_SplitShard each say "This API is only supported for the data streams with
// the provisioned capacity mode", and the SDK model glosses the ValidationException those three pages
// publish as "Specifies that you tried to invoke this API for a data stream with the on-demand
// capacity mode. This API is only supported for data streams with the provisioned capacity mode."
// (botocore kinesis/2013-12-02 service-2.json). So a reshard of an on-demand stream answers
// ValidationException/400. The API Reference pages now gloss the same code more generally ("The
// request failed data validation for this data stream …"), which fits the refusal equally.
//
// Readings substrate makes where no page states the behaviour:
//
//   - **A stream with no recorded mode is PROVISIONED.** Every stream substrate created before #1118
//     was implicitly provisioned, so its record carries no StreamMode and reads back as PROVISIONED;
//     the field is omitempty so such a record is byte-identical.
//   - **An on-demand stream starts with four shards.** The quotas page publishes that "new data
//     streams created with the on-demand capacity mode have 4 MB/s of write … throughput", and a shard
//     takes 1 MB/s of writes, so four shards is the shard count that throughput implies. On-demand
//     auto-scaling is resource-internal and not modelled: the count changes only when the stream is
//     switched back to PROVISIONED and resharded.
//   - **ShardCount alongside ON_DEMAND is refused** with InvalidArgumentException, whose gloss is
//     "A specified parameter exceeds its restrictions, is not supported, or can't be used". The page
//     #1118 quoted said the parameter "is no longer supported" for on-demand streams; the current page
//     says nothing either way, so the refusal and its message are substrate's.
//   - **UpdateStreamMode to the mode the stream already has succeeds and changes nothing.**
//   - **A switch keeps the stream's shards**, and starts the same UPDATING transition a reshard does
//     (#1119), since the operation is asynchronous like the reshards. The quota of two switches per
//     24 hours and WarmThroughputMiBps are not modelled.

// Stream capacity modes, the Valid Values of API_StreamModeDetails.
const (
	kinesisStreamModeProvisioned = "PROVISIONED"
	kinesisStreamModeOnDemand    = "ON_DEMAND"
)

// kinesisOnDemandInitialShards is the shard count an on-demand stream starts with. See the file
// comment for the reading.
const kinesisOnDemandInitialShards = 4

// kinesisStreamModeDetails is the API_StreamModeDetails object.
type kinesisStreamModeDetails struct {
	StreamMode string `json:"StreamMode"`
}

// kinesisValidateStreamMode checks a StreamModeDetails a request carried. StreamMode is Required:
// Yes inside the object and enumerated, so an object with no mode or an unknown one is refused with
// InvalidArgumentException, the code #950 settled on for an unusable Kinesis parameter. The messages
// are substrate's own.
func kinesisValidateStreamMode(details kinesisStreamModeDetails) *AWSError {
	switch details.StreamMode {
	case kinesisStreamModeProvisioned, kinesisStreamModeOnDemand:
		return nil
	case "":
		return kinesisInvalidArgument("StreamModeDetails.StreamMode is required")
	default:
		return kinesisInvalidArgument(fmt.Sprintf(
			"StreamModeDetails.StreamMode %q is not one of PROVISIONED, ON_DEMAND.", details.StreamMode))
	}
}

// kinesisStreamMode returns a stream's capacity mode, reading an unrecorded one as PROVISIONED.
func kinesisStreamMode(stream KinesisStream) string {
	if stream.StreamMode == "" {
		return kinesisStreamModeProvisioned
	}
	return stream.StreamMode
}

// kinesisRenderStreamModeDetails renders a stream's mode as the published StreamModeDetails object.
func kinesisRenderStreamModeDetails(stream KinesisStream) kinesisStreamModeDetails {
	return kinesisStreamModeDetails{StreamMode: kinesisStreamMode(stream)}
}

// kinesisCreateStreamCapacity resolves CreateStream's capacity mode and shard count. The mode it
// returns is empty when StreamModeDetails was omitted, so such a stream's record is byte-identical to
// one written before the mode was modelled. A provisioned stream with no positive ShardCount gets one
// shard, as every provisioned stream always has.
func kinesisCreateStreamCapacity(details *kinesisStreamModeDetails, shardCount *int) (string, int, *AWSError) {
	mode := ""
	if details != nil {
		if refusal := kinesisValidateStreamMode(*details); refusal != nil {
			return "", 0, refusal
		}
		mode = details.StreamMode
	}
	switch {
	case mode == kinesisStreamModeOnDemand && shardCount != nil:
		// Substrate's reading and message; see the file comment.
		return "", 0, kinesisInvalidArgument("ShardCount is not supported for a stream whose capacity mode is ON_DEMAND.")
	case mode == kinesisStreamModeOnDemand:
		return mode, kinesisOnDemandInitialShards, nil
	case shardCount != nil && *shardCount > 0:
		return mode, *shardCount, nil
	default:
		return mode, 1, nil
	}
}

// kinesisRequireProvisioned refuses a reshard of an on-demand stream with the ValidationException/400
// API_UpdateShardCount, API_MergeShards and API_SplitShard publish. The message is substrate's own;
// the pages publish the code and its gloss, not a message.
func kinesisRequireProvisioned(stream KinesisStream, target kinesisStreamTarget, op string) *AWSError {
	if kinesisStreamMode(stream) != kinesisStreamModeOnDemand {
		return nil
	}
	return &AWSError{
		Code: "ValidationException",
		Message: fmt.Sprintf("%s is not supported for stream %s under account %s because its capacity mode is ON_DEMAND.",
			op, target.Name, target.AccountID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// updateStreamMode handles UpdateStreamMode.
//
// The request is checked in full before the stream is read: StreamARN and StreamModeDetails are both
// Required: Yes and the mode is enumerated, so none of those refusals depends on state. Then the
// stream must exist (ResourceNotFoundException) and be ACTIVE (ResourceInUseException), the two
// state-dependent codes API_UpdateStreamMode publishes. The response body is empty.
func (p *KinesisPlugin) updateStreamMode(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamARN         string                    `json:"StreamARN"`
		StreamModeDetails *kinesisStreamModeDetails `json:"StreamModeDetails"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	// StreamARN is the only stream reference this operation publishes; it takes no StreamName.
	if body.StreamARN == "" {
		return nil, kinesisInvalidArgument("StreamARN is required")
	}
	target, refErr := kinesisParseStreamARN(body.StreamARN)
	if refErr != nil {
		return nil, refErr
	}
	if body.StreamModeDetails == nil {
		return nil, kinesisInvalidArgument("StreamModeDetails is required")
	}
	if refusal := kinesisValidateStreamMode(*body.StreamModeDetails); refusal != nil {
		return nil, refusal
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}
	goCtx := context.Background()
	if err := p.requireStreamActive(goCtx, stream, target); err != nil {
		return nil, err
	}

	mode := body.StreamModeDetails.StreamMode
	if kinesisStreamMode(stream) == mode {
		return kinesisJSONResponse(http.StatusOK, struct{}{})
	}
	stream.StreamMode = mode
	stream.StreamStatus = kinesisStatusActive
	if err := p.startStreamTransition(goCtx, &stream); err != nil {
		return nil, err
	}
	if err := p.saveStream(stream); err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, struct{}{})
}
