package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Enhanced fan-out consumers: RegisterStreamConsumer, DescribeStreamConsumer, ListStreamConsumers
// and DeregisterStreamConsumer (#1613).
//
// A consumer is a record under its stream, keyed consumer:{account}/{region}/{stream}/{name}. Its
// ARN is the stream's ARN followed by /consumer/{name}:{epoch seconds}, the seconds taken from the
// simulated clock at registration, so a consumer registered again under the same name later gets a
// different ARN, as API_Consumer says of AWS's. A stale ARN resolves to ResourceNotFoundException.
//
// The record holds the settled status, ACTIVE. CREATING is reported through the seeded progression
// in kinesis_consumer_status_control.go. Deregistration deletes the record in the request, so a
// DELETING window is not observable, the same reading the stream status takes for DeleteStream.
//
// Limits (API_RegisterStreamConsumer): 20 consumers per stream, and no more than five CREATING at
// once, both LimitExceededException. A duplicate name is ResourceInUseException. The 50-consumer
// figure for On-demand Advantage streams is not modeled, since substrate models no such stream.
// SubscribeToShard, which reads through a consumer, is refused; see kinesisSubscribeToShardRefusal.
//
// The messages are substrate's own; the pages publish codes and glosses, not messages.

const (
	// kinesisMaxConsumersPerStream is the per-stream consumer limit for provisioned and On-demand
	// Standard streams.
	kinesisMaxConsumersPerStream = 20

	// kinesisMaxCreatingConsumers is how many consumers of one stream may be CREATING at once.
	kinesisMaxCreatingConsumers = 5

	// kinesisConsumerNameMaxLength is ConsumerName's published maximum length.
	kinesisConsumerNameMaxLength = 128

	// kinesisConsumerARNInfix separates a consumer ARN's stream part from its consumer part.
	kinesisConsumerARNInfix = "/consumer/"
)

// KinesisConsumer holds the persisted state of an enhanced fan-out consumer.
type KinesisConsumer struct {
	// ConsumerName is the name the consumer was registered under.
	ConsumerName string `json:"ConsumerName"`

	// ConsumerARN is the consumer's ARN, carrying its registration time in epoch seconds.
	ConsumerARN string `json:"ConsumerARN"`

	// StreamARN is the ARN of the stream the consumer is registered with.
	StreamARN string `json:"StreamARN"`

	// ConsumerStatus is the settled status, ACTIVE; see the file comment.
	ConsumerStatus string `json:"ConsumerStatus"`

	// CreationTimestamp is when the consumer was registered, on the simulated clock.
	CreationTimestamp time.Time `json:"CreationTimestamp"`

	// Tags holds the consumer's tags.
	Tags map[string]string `json:"Tags,omitempty"`
}

// kinesisConsumerKey returns the state key for one consumer of a stream.
func kinesisConsumerKey(target kinesisStreamTarget, name string) string {
	return kinesisConsumerPrefix(target) + name
}

// kinesisConsumerPrefix returns the key prefix every consumer of one stream shares. The trailing
// slash keeps stream "a" from listing the consumers of stream "ab".
func kinesisConsumerPrefix(target kinesisStreamTarget) string {
	return "consumer:" + target.AccountID + "/" + target.Region + "/" + target.Name + "/"
}

// kinesisConsumerARN renders a consumer's ARN from its stream, name and registration time.
func kinesisConsumerARN(target kinesisStreamTarget, name string, registered time.Time) string {
	return kinesisStreamARN(target) + kinesisConsumerARNInfix + name + ":" + strconv.FormatInt(registered.Unix(), 10)
}

// kinesisParseConsumerARN parses a consumer ARN, published as
// arn:aws.*:kinesis:.*:\d{12}:.*stream/[a-zA-Z0-9_.-]+/consumer/[a-zA-Z0-9_.-]+:[0-9]+, into the
// stream it belongs to and the consumer's name.
func kinesisParseConsumerARN(arn string) (kinesisStreamTarget, string, *AWSError) {
	refuse := kinesisInvalidArgument("ConsumerARN is not a Kinesis consumer ARN: " + arn)
	streamPart, consumerPart, ok := strings.Cut(arn, kinesisConsumerARNInfix)
	if !ok {
		return kinesisStreamTarget{}, "", refuse
	}
	target, err := kinesisParseStreamARN(streamPart)
	if err != nil {
		return kinesisStreamTarget{}, "", refuse
	}
	i := strings.LastIndex(consumerPart, ":")
	if i < 0 {
		return kinesisStreamTarget{}, "", refuse
	}
	name, stamp := consumerPart[:i], consumerPart[i+1:]
	if kinesisCheckConsumerName(name) != nil || stamp == "" || strings.Trim(stamp, "0123456789") != "" {
		return kinesisStreamTarget{}, "", refuse
	}
	return target, name, nil
}

// kinesisCheckConsumerName refuses a ConsumerName outside its published 1–128 length and
// [a-zA-Z0-9_.-]+ pattern.
func kinesisCheckConsumerName(name string) *AWSError {
	if name == "" {
		return kinesisInvalidArgument("ConsumerName is required")
	}
	if len(name) > kinesisConsumerNameMaxLength {
		return kinesisInvalidArgument(fmt.Sprintf("ConsumerName must be at most %d characters; got %d",
			kinesisConsumerNameMaxLength, len(name)))
	}
	for _, r := range name {
		if !kinesisNameRune(r) {
			return kinesisInvalidArgument(fmt.Sprintf("ConsumerName %q must match [a-zA-Z0-9_.-]+", name))
		}
	}
	return nil
}

// kinesisNameRune reports whether r is in the [a-zA-Z0-9_.-] class Kinesis names use.
func kinesisNameRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '.' || r == '-'
}

// kinesisConsumerNotFound reports that no consumer answers to the request's reference.
func kinesisConsumerNotFound(what string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "Consumer not found: " + what,
		HTTPStatus: http.StatusBadRequest,
	}
}

// kinesisConsumerRef is the three members Describe- and DeregisterStreamConsumer name a consumer
// by: its ARN, or its stream's ARN and its name, or all three when they agree.
type kinesisConsumerRef struct {
	StreamARN    string `json:"StreamARN"`
	ConsumerName string `json:"ConsumerName"`
	ConsumerARN  string `json:"ConsumerARN"`
}

// resolve returns the stream and consumer name a reference names. Members that disagree are
// refused with InvalidArgumentException, per the pages' "as long as they don't conflict".
func (ref kinesisConsumerRef) resolve() (kinesisStreamTarget, string, *AWSError) {
	if ref.ConsumerARN != "" {
		target, name, err := kinesisParseConsumerARN(ref.ConsumerARN)
		if err != nil {
			return kinesisStreamTarget{}, "", err
		}
		if ref.StreamARN != "" && ref.StreamARN != kinesisStreamARN(target) {
			return kinesisStreamTarget{}, "", kinesisInvalidArgument(fmt.Sprintf(
				"StreamARN %q and ConsumerARN %q name different streams", ref.StreamARN, ref.ConsumerARN))
		}
		if ref.ConsumerName != "" && ref.ConsumerName != name {
			return kinesisStreamTarget{}, "", kinesisInvalidArgument(fmt.Sprintf(
				"ConsumerName %q and ConsumerARN %q name different consumers", ref.ConsumerName, ref.ConsumerARN))
		}
		return target, name, nil
	}
	if ref.StreamARN == "" || ref.ConsumerName == "" {
		return kinesisStreamTarget{}, "", kinesisInvalidArgument("either ConsumerARN, or StreamARN and ConsumerName, is required")
	}
	target, err := kinesisParseStreamARN(ref.StreamARN)
	if err != nil {
		return kinesisStreamTarget{}, "", err
	}
	if nameErr := kinesisCheckConsumerName(ref.ConsumerName); nameErr != nil {
		return kinesisStreamTarget{}, "", nameErr
	}
	return target, ref.ConsumerName, nil
}

// loadConsumer returns the consumer a reference names, or ResourceNotFoundException. A ConsumerARN
// whose timestamp is not the registered consumer's names a consumer that no longer exists.
func (p *KinesisPlugin) loadConsumer(ref kinesisConsumerRef) (KinesisConsumer, error) {
	target, name, refErr := ref.resolve()
	if refErr != nil {
		return KinesisConsumer{}, refErr
	}
	var consumer KinesisConsumer
	found, err := p.getJSON(kinesisConsumerKey(target, name), &consumer)
	if err != nil {
		return KinesisConsumer{}, err
	}
	what := kinesisStreamARN(target) + kinesisConsumerARNInfix + name
	if ref.ConsumerARN != "" {
		what = ref.ConsumerARN
	}
	if !found || (ref.ConsumerARN != "" && ref.ConsumerARN != consumer.ConsumerARN) {
		return KinesisConsumer{}, kinesisConsumerNotFound(what)
	}
	return consumer, nil
}

// listConsumers returns every consumer registered with a stream.
func (p *KinesisPlugin) listConsumers(target kinesisStreamTarget) ([]KinesisConsumer, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, kinesisNamespace, kinesisConsumerPrefix(target))
	if err != nil {
		return nil, fmt.Errorf("kinesis listConsumers: %w", err)
	}
	out := make([]KinesisConsumer, 0, len(keys))
	for _, key := range keys {
		var consumer KinesisConsumer
		found, err := p.getJSON(key, &consumer)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, consumer)
		}
	}
	return out, nil
}

// saveConsumer persists a consumer under the stream its ARN names.
func (p *KinesisPlugin) saveConsumer(consumer KinesisConsumer) error {
	target, name, refErr := kinesisParseConsumerARN(consumer.ConsumerARN)
	if refErr != nil {
		return fmt.Errorf("kinesis saveConsumer: %s", refErr.Message)
	}
	return p.putJSON(kinesisConsumerKey(target, name), consumer)
}

// deleteConsumer removes a consumer's record and restarts its status countdown.
func (p *KinesisPlugin) deleteConsumer(consumer KinesisConsumer) error {
	target, name, refErr := kinesisParseConsumerARN(consumer.ConsumerARN)
	if refErr != nil {
		return fmt.Errorf("kinesis deleteConsumer: %s", refErr.Message)
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, kinesisNamespace, kinesisConsumerKey(target, name)); err != nil {
		return fmt.Errorf("kinesis deleteConsumer: %w", err)
	}
	return kinesisConsumerProgressions.reset(goCtx, p.state, consumer.ConsumerARN)
}

// getJSON reads one Kinesis record, reporting whether it exists.
func (p *KinesisPlugin) getJSON(key string, out any) (bool, error) {
	data, err := p.state.Get(context.Background(), kinesisNamespace, key)
	if err != nil {
		return false, fmt.Errorf("kinesis get %s: %w", key, err)
	}
	if data == nil {
		return false, nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("kinesis unmarshal %s: %w", key, err)
	}
	return true, nil
}

// putJSON writes one Kinesis record.
func (p *KinesisPlugin) putJSON(key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kinesis marshal %s: %w", key, err)
	}
	if err := p.state.Put(context.Background(), kinesisNamespace, key, data); err != nil {
		return fmt.Errorf("kinesis put %s: %w", key, err)
	}
	return nil
}

// kinesisConsumerView is the API_Consumer shape; ConsumerDescription adds StreamARN.
type kinesisConsumerView struct {
	ConsumerName              string       `json:"ConsumerName"`
	ConsumerARN               string       `json:"ConsumerARN"`
	ConsumerStatus            string       `json:"ConsumerStatus"`
	ConsumerCreationTimestamp EpochSeconds `json:"ConsumerCreationTimestamp"`
	StreamARN                 string       `json:"StreamARN,omitempty"`
}

// view renders a consumer as API_Consumer, with status as one read reports it.
func (c KinesisConsumer) view(status string) kinesisConsumerView {
	return kinesisConsumerView{
		ConsumerName:              c.ConsumerName,
		ConsumerARN:               c.ConsumerARN,
		ConsumerStatus:            status,
		ConsumerCreationTimestamp: EpochSeconds(c.CreationTimestamp),
	}
}

func (p *KinesisPlugin) registerStreamConsumer(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamARN    string            `json:"StreamARN"`
		ConsumerName string            `json:"ConsumerName"`
		Tags         map[string]string `json:"Tags"`
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
	if nameErr := kinesisCheckConsumerName(body.ConsumerName); nameErr != nil {
		return nil, nameErr
	}
	if body.Tags != nil {
		if tagErr := kinesisValidateTagMap(body.Tags); tagErr != nil {
			return nil, tagErr
		}
		if quotaErr := kinesisCheckResourceTagQuota(body.StreamARN+kinesisConsumerARNInfix+body.ConsumerName, nil, body.Tags); quotaErr != nil {
			return nil, quotaErr
		}
	}

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}
	goCtx := context.Background()
	if err := p.requireStreamActive(goCtx, stream, target); err != nil {
		return nil, err
	}

	// The count, the duplicate check and the write are one read-modify-write over the stream's
	// consumer set.
	p.consumerMu.Lock()
	defer p.consumerMu.Unlock()
	existing, err := p.listConsumers(target)
	if err != nil {
		return nil, err
	}
	creating := 0
	for _, c := range existing {
		if c.ConsumerName == body.ConsumerName {
			return nil, &AWSError{
				Code:       "ResourceInUseException",
				Message:    fmt.Sprintf("Consumer %s already exists for stream %s.", body.ConsumerName, kinesisStreamARN(target)),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		status, statusErr := p.consumerStatus(goCtx, c, false)
		if statusErr != nil {
			return nil, statusErr
		}
		if status == kinesisStatusCreating {
			creating++
		}
	}
	if len(existing) >= kinesisMaxConsumersPerStream {
		return nil, kinesisLimitExceeded(fmt.Sprintf("Stream %s already has %d registered consumers, the limit.",
			kinesisStreamARN(target), kinesisMaxConsumersPerStream))
	}
	if creating >= kinesisMaxCreatingConsumers {
		return nil, kinesisLimitExceeded(fmt.Sprintf("Stream %s already has %d consumers in CREATING status, the limit.",
			kinesisStreamARN(target), kinesisMaxCreatingConsumers))
	}

	now := p.tc.Now()
	consumer := KinesisConsumer{
		ConsumerName:      body.ConsumerName,
		ConsumerARN:       kinesisConsumerARN(target, body.ConsumerName, now),
		StreamARN:         kinesisStreamARN(target),
		ConsumerStatus:    kinesisStatusActive,
		CreationTimestamp: now,
		Tags:              body.Tags,
	}
	// A consumer registered again under a deregistered one's name in the same second shares its ARN,
	// so its countdown restarts here rather than continuing the predecessor's.
	if err := kinesisConsumerProgressions.reset(goCtx, p.state, consumer.ConsumerARN); err != nil {
		return nil, err
	}
	if err := p.saveConsumer(consumer); err != nil {
		return nil, err
	}
	status, err := p.consumerStatus(goCtx, consumer, false)
	if err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, map[string]any{"Consumer": consumer.view(status)})
}

func (p *KinesisPlugin) describeStreamConsumer(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var ref kinesisConsumerRef
	if err := json.Unmarshal(req.Body, &ref); err != nil {
		return nil, kinesisInvalidBody()
	}
	consumer, err := p.loadConsumer(ref)
	if err != nil {
		return nil, err
	}
	status, err := p.consumerStatus(context.Background(), consumer, true)
	if err != nil {
		return nil, err
	}
	view := consumer.view(status)
	view.StreamARN = consumer.StreamARN
	return kinesisJSONResponse(http.StatusOK, map[string]any{"ConsumerDescription": view})
}

func (p *KinesisPlugin) deregisterStreamConsumer(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var ref kinesisConsumerRef
	if err := json.Unmarshal(req.Body, &ref); err != nil {
		return nil, kinesisInvalidBody()
	}
	p.consumerMu.Lock()
	defer p.consumerMu.Unlock()
	consumer, err := p.loadConsumer(ref)
	if err != nil {
		return nil, err
	}
	if err := p.deleteConsumer(consumer); err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

// kinesisListStreamConsumersPaging is ListStreamConsumers' paging contract: MaxResults 1–10000,
// "the default value is 100. If you specify a value greater than 100, at most 100 results are
// returned" (API_ListStreamConsumers).
func kinesisListStreamConsumersPaging() jsonPaging {
	return jsonPaging{
		op:           "ListStreamConsumers",
		size:         pageSizeRange{member: "MaxResults", min: 1, max: 10000, def: 100},
		invalidSize:  kinesisInvalidArgument,
		invalidToken: kinesisInvalidNextToken,
	}
}

// kinesisInvalidNextToken refuses a NextToken this operation did not issue for this request.
// ExpiredNextTokenException is the published code for an expired token; substrate's tokens do not
// expire, so an unissued one is InvalidArgumentException, the code for a parameter that "can't be
// used".
func kinesisInvalidNextToken() *AWSError {
	return kinesisInvalidArgument("NextToken is not a token this operation issued for this request")
}

func (p *KinesisPlugin) listStreamConsumers(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		StreamARN               string        `json:"StreamARN"`
		NextToken               string        `json:"NextToken"`
		MaxResults              *int          `json:"MaxResults"`
		StreamCreationTimestamp *EpochSeconds `json:"StreamCreationTimestamp"`
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
	if body.NextToken != "" && body.StreamCreationTimestamp != nil {
		return nil, kinesisInvalidArgument("StreamCreationTimestamp cannot be specified with NextToken")
	}
	pr, pageErr := jsonNextToken(kinesisListStreamConsumersPaging(),
		map[string][]string{"StreamARN": {kinesisStreamARN(target)}}, body.NextToken, body.MaxResults)
	if pageErr != nil {
		return nil, pageErr
	}
	pr.Size = min(pr.Size, 100)

	stream, err := p.loadStream(target)
	if err != nil {
		return nil, err
	}
	if refusal := kinesisCheckCreationTimestamp(stream, target, body.StreamCreationTimestamp); refusal != nil {
		return nil, refusal
	}
	consumers, err := p.listConsumers(target)
	if err != nil {
		return nil, err
	}
	res, err := page(consumers, func(c KinesisConsumer) cursorKey { return cursorKey{Sort: c.ConsumerName} }, pr)
	if err != nil {
		return nil, kinesisInvalidNextToken()
	}
	goCtx := context.Background()
	views := make([]kinesisConsumerView, 0, len(res.Items))
	for _, c := range res.Items {
		status, statusErr := p.consumerStatus(goCtx, c, true)
		if statusErr != nil {
			return nil, statusErr
		}
		views = append(views, c.view(status))
	}
	out := map[string]any{"Consumers": views}
	if res.Next != "" {
		out["NextToken"] = res.Next
	}
	return kinesisJSONResponse(http.StatusOK, out)
}

// kinesisCheckCreationTimestamp refuses a StreamCreationTimestamp that is not the stream's own.
// The member exists to tell a stream from an earlier one of the same name, and substrate holds only
// the current one, so a mismatch names a stream that no longer exists. Compared to the second,
// which is the precision DescribeStream reports the timestamp at.
func kinesisCheckCreationTimestamp(stream KinesisStream, target kinesisStreamTarget, ts *EpochSeconds) *AWSError {
	if ts == nil || time.Time(*ts).Unix() == stream.CreatedAt.Unix() {
		return nil
	}
	return kinesisStreamNotFound(target)
}

// kinesisLimitExceeded is LimitExceededException/400.
func kinesisLimitExceeded(message string) *AWSError {
	return &AWSError{Code: "LimitExceededException", Message: message, HTTPStatus: http.StatusBadRequest}
}
