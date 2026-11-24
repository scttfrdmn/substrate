package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// A stream's retention period and maximum record size (#1613, #1539).
//
// IncreaseStreamRetentionPeriod and DecreaseStreamRetentionPeriod set RetentionPeriodHours, which
// API_IncreaseStreamRetentionPeriod bounds at "8760 hours (365 days)" and
// API_DecreaseStreamRetentionPeriod at "24 hours". Each page's RetentionPeriodHours must be "more
// than" or "less than" the current period. A value outside 24–8760, or one that does not move the
// period in the operation's direction, is InvalidArgumentException, the code both pages publish for
// a parameter that "exceeds its restrictions". Neither starts a status transition, but both refuse
// a stream that is not ACTIVE with the ResourceInUseException both publish.
//
// UpdateMaxRecordSize sets MaxRecordSizeInKiB, which PutRecord and PutRecords then enforce
// (kinesis_record_limits.go). Its page names the stream by StreamARN only, and says of a value
// outside 1024–10240 that "UpdateMaxRecordSize sends back an ValidationException message", so that
// is the code here. CreateStream keeps InvalidArgumentException for the same range: its page's
// MaxRecordSizeInKiB publishes no code of its own.
//
// The messages are substrate's own.

const (
	// kinesisMinRetentionHours and kinesisMaxRetentionHours are the published retention bounds.
	kinesisMinRetentionHours = 24
	kinesisMaxRetentionHours = 8760
)

func (p *KinesisPlugin) increaseStreamRetentionPeriod(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.changeRetention(ctx, req, true)
}

func (p *KinesisPlugin) decreaseStreamRetentionPeriod(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.changeRetention(ctx, req, false)
}

// changeRetention is both retention operations; increase picks the direction.
func (p *KinesisPlugin) changeRetention(ctx *RequestContext, req *AWSRequest, increase bool) (*AWSResponse, error) {
	var body struct {
		kinesisStreamRef
		RetentionPeriodHours *int `json:"RetentionPeriodHours"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	target, refErr := kinesisResolveStream(ctx, body.kinesisStreamRef)
	if refErr != nil {
		return nil, refErr
	}
	if body.RetentionPeriodHours == nil {
		return nil, kinesisInvalidArgument("RetentionPeriodHours is required")
	}
	hours := *body.RetentionPeriodHours
	if hours < kinesisMinRetentionHours || hours > kinesisMaxRetentionHours {
		return nil, kinesisInvalidArgument(fmt.Sprintf("RetentionPeriodHours %d is not between %d and %d.",
			hours, kinesisMinRetentionHours, kinesisMaxRetentionHours))
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}
	current := stream.RetentionPeriodHours
	switch {
	case increase && hours <= current:
		return nil, kinesisInvalidArgument(fmt.Sprintf(
			"RetentionPeriodHours %d for stream %s is not more than the current retention period of %d hours; use DecreaseStreamRetentionPeriod to shorten it.",
			hours, kinesisStreamARN(target), current))
	case !increase && hours >= current:
		return nil, kinesisInvalidArgument(fmt.Sprintf(
			"RetentionPeriodHours %d for stream %s is not less than the current retention period of %d hours; use IncreaseStreamRetentionPeriod to lengthen it.",
			hours, kinesisStreamARN(target), current))
	}
	if err := p.requireStreamActive(context.Background(), stream, target); err != nil {
		return nil, err
	}
	stream.RetentionPeriodHours = hours
	if err := p.saveStream(stream); err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) updateMaxRecordSize(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamARN          string `json:"StreamARN"`
		MaxRecordSizeInKiB *int   `json:"MaxRecordSizeInKiB"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	if body.StreamARN == "" {
		return nil, kinesisInvalidArgument("StreamARN is required")
	}
	target, refErr := kinesisParseStreamARN(body.StreamARN)
	if refErr != nil {
		return nil, refErr
	}
	if body.MaxRecordSizeInKiB == nil {
		return nil, kinesisInvalidArgument("MaxRecordSizeInKiB is required")
	}
	if refusal := kinesisValidateMaxRecordSize(body.MaxRecordSizeInKiB); refusal != nil {
		refusal.Code = "ValidationException"
		return nil, refusal
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}
	if err := p.requireStreamActive(context.Background(), stream, target); err != nil {
		return nil, err
	}
	stream.MaxRecordSizeInKiB = *body.MaxRecordSizeInKiB
	if err := p.saveStream(stream); err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, struct{}{})
}
