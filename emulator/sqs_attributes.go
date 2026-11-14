package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// sqsIsQueueAttributeName reports whether name is one GetQueueAttributes'
// AttributeNames accepts, "All" included — the reference's Valid Values list, verbatim.
// A name outside it is refused with InvalidAttributeName (#1441).
func sqsIsQueueAttributeName(name string) bool {
	switch name {
	case "All", "Policy", "VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod",
		"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "CreatedTimestamp",
		"LastModifiedTimestamp", "QueueArn", "ApproximateNumberOfMessagesDelayed", "DelaySeconds",
		"ReceiveMessageWaitTimeSeconds", "RedrivePolicy", "FifoQueue", "ContentBasedDeduplication",
		"KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds", "DeduplicationScope", "FifoThroughputLimit",
		"RedriveAllowPolicy", "SqsManagedSseEnabled":
		return true
	}
	return false
}

// sqsQueryList reads a flattened query-protocol list: the indexed members name.1,
// name.2, … and, because the reference's own examples send it that way
// (`&AttributeName=All`), a bare unindexed name as well.
func sqsQueryList(params map[string]string, name string) []string {
	var out []string
	if v, ok := params[name]; ok && v != "" {
		out = append(out, v)
	}
	for i := 1; ; i++ {
		v, ok := params[fmt.Sprintf("%s.%d", name, i)]
		if !ok {
			break
		}
		out = append(out, v)
	}
	return out
}

// sqsRequestedQueueAttributeNames returns the AttributeNames a GetQueueAttributes
// request carried. The JSON protocol sends an AttributeNames array; the query protocol
// sends AttributeName.N.
func sqsRequestedQueueAttributeNames(req *AWSRequest) ([]string, *AWSError) {
	if sqsIsJSONProtocol(req) {
		var input struct {
			AttributeNames []string `json:"AttributeNames"`
		}
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, sqsInvalidBody()
		}
		return input.AttributeNames, nil
	}
	return sqsQueryList(req.Params, "AttributeName"), nil
}

// sqsWantsMessageCounts reports whether a GetQueueAttributes request names "All" or
// one of the three ApproximateNumberOfMessages* attributes.
func sqsWantsMessageCounts(requested []string) bool {
	for _, n := range requested {
		switch n {
		case "All", "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed":
			return true
		}
	}
	return false
}

// sqsMessageCounts counts a queue's messages by state at now, using the rules
// ReceiveMessage applies: a message whose DelayUntil is in the future is delayed; one
// whose VisibleAfter is in the future is in flight (received, inside its visibility
// timeout); every other message is available.
func (p *SQSPlugin) sqsMessageCounts(ctx context.Context, urlKey string, now time.Time) (visible, notVisible, delayed int, err error) {
	ids, err := p.loadMsgIDs(ctx, urlKey)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, id := range ids {
		msg, loadErr := p.loadMsg(ctx, urlKey, id)
		if loadErr != nil {
			return 0, 0, 0, loadErr
		}
		if msg == nil {
			continue
		}
		switch {
		case !msg.DelayUntil.IsZero() && now.Before(msg.DelayUntil):
			delayed++
		case !msg.VisibleAfter.IsZero() && now.Before(msg.VisibleAfter):
			notVisible++
		default:
			visible++
		}
	}
	return visible, notVisible, delayed, nil
}

// sqsSelectQueueAttributes returns the members of all that requested names. "All"
// selects everything; a defined name the queue has no value for is absent rather than
// an error; an undefined name is InvalidAttributeName. No names selects nothing — the
// reference: "if you don't specify values for this parameter, the request returns
// empty results".
func sqsSelectQueueAttributes(all map[string]string, requested []string) (map[string]string, *AWSError) {
	out := map[string]string{}
	for _, name := range requested {
		if !sqsIsQueueAttributeName(name) {
			return nil, sqsInvalidAttributeName(name)
		}
	}
	for _, name := range requested {
		if name == "All" {
			for k, v := range all {
				out[k] = v
			}
			continue
		}
		if v, ok := all[name]; ok {
			out[name] = v
		}
	}
	return out, nil
}

// sqsQuerySystemAttributeNames returns the system attribute names a query-protocol
// ReceiveMessage asked for: the union of the deprecated AttributeName.N and the newer
// MessageSystemAttributeName.N, which the reference documents with the same members
// (#1462). The JSON protocol's AttributeNames and MessageSystemAttributeNames arrays
// are read by ReceiveMessage's own decode.
func sqsQuerySystemAttributeNames(params map[string]string) []string {
	return append(sqsQueryList(params, "AttributeName"), sqsQueryList(params, "MessageSystemAttributeName")...)
}

// sqsMessageSystemAttributes returns every system attribute substrate records for a
// message, as the decimal strings SQS returns them in. SequenceNumber and
// AWSTraceHeader are not recorded, so they are never returned.
func sqsMessageSystemAttributes(msg *SQSMessage) map[string]string {
	out := map[string]string{
		"ApproximateReceiveCount": strconv.Itoa(msg.ReceiveCount),
	}
	if v := msg.Attributes["SenderId"]; v != "" {
		out["SenderId"] = v
	}
	if v := msg.Attributes["SentTimestamp"]; v != "" {
		out["SentTimestamp"] = v
	} else if msg.SentTimestamp != 0 {
		out["SentTimestamp"] = strconv.FormatInt(msg.SentTimestamp, 10)
	}
	if msg.FirstReceiveTimestamp != 0 {
		out["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(msg.FirstReceiveTimestamp, 10)
	}
	if msg.MessageGroupID != "" {
		out["MessageGroupId"] = msg.MessageGroupID
	}
	if msg.MessageDeduplicationID != "" {
		out["MessageDeduplicationId"] = msg.MessageDeduplicationID
	}
	return out
}

// sqsSelectSystemAttributes returns the system attributes requested names select:
// "All" (or ".*", the spelling the message-attribute selector also accepts) selects
// every one, and a name selects that attribute when the message has it. Nothing is
// returned unless asked for, as with message attributes (#461).
func sqsSelectSystemAttributes(available map[string]string, requested []string) map[string]string {
	if len(requested) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, want := range requested {
		if want == "All" || want == ".*" {
			for k, v := range available {
				out[k] = v
			}
			continue
		}
		if v, ok := available[want]; ok {
			out[want] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// sqsAttributeXML is one Name/Value attribute in an SQS XML response — the shape both
// GetQueueAttributes' result and a received Message's system attributes use.
type sqsAttributeXML struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// sqsAttributesXML renders a string map as Name/Value entries ordered by name, so one
// response is byte-identical to the next for identical state.
func sqsAttributesXML(attrs map[string]string) []sqsAttributeXML {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]sqsAttributeXML, 0, len(attrs))
	for k, v := range attrs {
		out = append(out, sqsAttributeXML{Name: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
