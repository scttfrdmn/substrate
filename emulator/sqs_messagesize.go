package emulator

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
)

// sqsEffectiveMaximumMessageSize returns the byte limit a queue observably enforces,
// resolved through [sqsDefaultMaximumMessageSize] when the attribute is unset.
//
// It reads through the same default the read path does rather than holding a second
// copy, so the number GetQueueAttributes reports is by construction the number
// SendMessage enforces. A divergence here would be its own confident wrong answer: a
// consumer that sizes its payloads from the reported attribute would have them
// rejected anyway.
//
// A malformed attribute value falls back to the default rather than to zero. Zero
// would reject every message including an empty one, turning an unparseable
// attribute into a queue that accepts nothing — CreateQueue is where an invalid
// value belongs, not every subsequent send.
func sqsEffectiveMaximumMessageSize(q *SQSQueue) int {
	v := getAttrOrDefault(q.Attributes, "MaximumMessageSize", sqsDefaultMaximumMessageSize)
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		n, _ = strconv.Atoi(sqsDefaultMaximumMessageSize)
	}
	return n
}

// sqsMessageSize returns the byte size AWS measures a message at: the body plus
// every component of every message attribute.
//
// The developer guide is explicit that attributes count — "All components of a
// message attribute are included in the 1 MiB message size restriction" — and the
// per-component breakdown is AWS's own, from the size calculation the Amazon SQS
// Extended Client Library uses to decide whether a payload needs offloading to S3
// (`getMsgAttributesSize`): the name, the data type, and the value, each as UTF-8
// bytes, with a binary value counted as its raw byte length.
//
// Measuring the body alone would be the wrong boundary in the direction that
// matters: a caller who packs 900 KiB of metadata beside a 200 KiB body is over the
// limit in AWS and under it here, so the very payload shape that motivates a
// chunking branch is the one substrate would accept.
//
// Message *system* attributes are deliberately excluded — the SendMessage reference
// states "The size of a message system attribute doesn't count towards the total
// size of a message." Substrate does not model them today; the exclusion is recorded
// here so that adding them does not silently add them to the total.
func sqsMessageSize(body string, attrs map[string]SQSMessageAttribute) int {
	size := len(body)
	for name, attr := range attrs {
		size += len(name) + len(attr.DataType) + len(attr.StringValue) + len(attr.BinaryValue)
	}
	return size
}

// sqsQueryMessageAttributes collects the message attributes carried under a
// query-protocol parameter prefix, for measurement by [sqsMessageSize].
//
// The wire form is the SendMessage reference's own example:
// MessageAttribute.N.Name, MessageAttribute.N.Value.DataType and
// MessageAttribute.N.Value.StringValue, numbered from 1. The prefix argument is what
// lets SendMessageBatch reuse this for its per-entry
// SendMessageBatchRequestEntry.N.MessageAttribute.M.* nesting. Note this is not SNS's
// MessageAttributes.entry.N.* shape; the two services differ and
// [parseSNSMessageAttributes] is deliberately separate.
//
// A BinaryValue arrives base64-encoded, and AWS counts a binary attribute as its raw
// byte length, so it is decoded before measurement. Undecodable input is measured as
// the bytes actually sent rather than dropped to zero — a request substrate cannot
// decode is still a request whose size a caller can observe being rejected.
//
// Attributes are collected for measurement only; substrate does not yet store or
// return them on ReceiveMessage (#461). Returning nil for a request carrying none
// keeps that the common, allocation-free path.
func sqsQueryMessageAttributes(params map[string]string, prefix string) map[string]SQSMessageAttribute {
	var attrs map[string]SQSMessageAttribute
	for i := 1; ; i++ {
		base := fmt.Sprintf("%sMessageAttribute.%d.", prefix, i)
		name, ok := params[base+"Name"]
		if !ok {
			break
		}
		attr := SQSMessageAttribute{
			DataType:    params[base+"Value.DataType"],
			StringValue: params[base+"Value.StringValue"],
		}
		if raw := params[base+"Value.BinaryValue"]; raw != "" {
			decoded, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				decoded = []byte(raw)
			}
			attr.BinaryValue = decoded
		}
		if attrs == nil {
			attrs = make(map[string]SQSMessageAttribute)
		}
		attrs[name] = attr
	}
	return attrs
}

// sqsMessageTooLong returns the error SendMessage raises for a message exceeding the
// queue's effective MaximumMessageSize.
//
// The code is InvalidParameterValue, HTTP 400. It is not derivable from the API
// model: the SendMessage reference lists no oversized-message error at all, its
// InvalidMessageContents is documented as a character-set error ("The message
// contains characters outside the allowed set"), and BatchRequestTooLong is declared
// only on SendMessageBatch. The provenance is therefore an observed real-AWS
// response rather than a doc citation, and is recorded as such: a captured SDK error
// from real SQS carries `code: 'InvalidParameterValue'`, statusCode 400, and the
// message reproduced below (artilleryio/artillery#3463, whose 262144 reflects the
// queue's limit at the time). The same wording is what moto emits, which corroborates
// it as transcribed AWS text rather than one project's invention.
//
// The limit is interpolated rather than fixed because the message is the only place a
// caller learns which limit applied — a queue with an explicit 1 KiB attribute
// rejects at 1 KiB, and reporting 1 MiB there would send them looking for a bug in
// their own sizing.
func sqsMessageTooLong(limit int) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: fmt.Sprintf("One or more parameters are invalid. Reason: Message must be shorter than %d bytes.",
			limit),
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsMaxBatchPayloadSize is the combined payload limit for one SendMessageBatch
// request: 1 MiB, per the reference's "the maximum total payload size (the sum of the
// individual lengths of all of the batched messages)".
//
// It is derived from [sqsDefaultMaximumMessageSize] rather than written as a second
// literal, because AWS states the two limits are the same number ("are both 1 MiB")
// and a divergence would be silent. They are nonetheless distinct *limits*: a queue's
// MaximumMessageSize attribute is documented as a per-message cap, so lowering it does
// not lower the request payload cap. That is what keeps both checks reachable — a
// queue with a 1 KiB attribute rejects a 2 KiB entry as too long while a batch of ten
// legal 1 KiB entries stays under the 1 MiB request cap, which is the behavior a
// per-message reading of the attribute requires.
var sqsMaxBatchPayloadSize = func() int {
	n, err := strconv.Atoi(sqsDefaultMaximumMessageSize)
	if err != nil {
		// Unreachable: the constant is a literal decimal. Checked rather than
		// discarded so that editing it to something unparseable fails loudly at the
		// first batch send instead of silently capping batches at zero bytes.
		panic("sqsDefaultMaximumMessageSize is not an integer: " + err.Error())
	}
	return n
}()

// sqsCheckBatchSizes validates a SendMessageBatch's entry sizes, returning the error
// AWS raises or nil.
//
// The combined total is checked first, which is not an arbitrary ordering: because the
// per-message and total limits are equal on a default queue, a batch carrying one
// oversized entry breaches the total too, and real AWS reports BatchRequestTooLong for
// that case rather than the per-message error (aws/aws-sdk-go-v2#3026). Checking
// per-entry first would report a different code than AWS does for the single most
// common batch mistake.
//
// An oversized entry fails the whole request rather than landing in the response's
// Failed list. The batch reference warns that a caller "should check for batch errors
// even when the call returns an HTTP status code of 200" precisely because per-entry
// failures are easy to miss — so for a condition AWS is observed to report as a
// request-level 400, reporting it the same way is both faithful and the version a
// consumer cannot overlook.
func sqsCheckBatchSizes(perMessageLimit int, sizes []int) *AWSError {
	total := 0
	for _, n := range sizes {
		total += n
	}
	if total > sqsMaxBatchPayloadSize {
		return sqsBatchRequestTooLong(sqsMaxBatchPayloadSize, total)
	}
	for _, n := range sizes {
		if n > perMessageLimit {
			return sqsMessageTooLong(perMessageLimit)
		}
	}
	return nil
}

// sqsBatchRequestTooLong returns the error SendMessageBatch raises when the batch's
// combined payload exceeds the limit.
//
// SendMessageBatch declares this one in the API model — "The length of all the
// messages put together is more than the limit" — and the reference states that "The
// maximum allowed individual message size and the maximum total payload size (the sum
// of the individual lengths of all of the batched messages) are both 1 MiB". So the
// two limits are equal, which has a consequence worth stating: a batch containing one
// oversized entry breaches the total as well, and real AWS reports
// BatchRequestTooLong for it rather than the per-message error. A captured real-AWS
// response shows exactly that shape (aws/aws-sdk-go-v2#3026), including the
// "You have sent N bytes" clause reproduced here.
//
// The message wording is not published in the reference; it comes from that observed
// response, corroborated by moto emitting the same text.
func sqsBatchRequestTooLong(limit, sent int) *AWSError {
	return &AWSError{
		Code: "BatchRequestTooLong",
		Message: fmt.Sprintf("Batch requests cannot be longer than %d bytes. You have sent %d bytes.",
			limit, sent),
		HTTPStatus: http.StatusBadRequest,
	}
}
