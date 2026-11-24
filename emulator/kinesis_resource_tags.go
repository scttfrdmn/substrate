package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// TagResource, UntagResource and ListTagsForResource (#1613): Kinesis's ARN-addressed tag
// operations, which "can be a data stream, a queue, or an enhanced fan-out consumer". A stream ARN
// reaches the same tag set AddTagsToStream writes; a consumer ARN reaches the consumer's, which
// RegisterStreamConsumer's Tags seeds. Substrate models no queues, so a queue or channel ARN is
// refused with InvalidArgumentException, like any ARN that is neither.
//
// The bounds are AddTagsToStream's: a request map of at most 200 entries and a resource of at most
// 50 tags ("you can assign up to 50 tags to a Kinesis resource"), the second LimitExceededException.
// UntagResource's TagKeys is 1–50 keys. ListTagsForResource answers every tag, sorted by key.

// kinesisMaxUntagKeys is UntagResource's published TagKeys maximum.
const kinesisMaxUntagKeys = 50

// kinesisTaggable is one resource the ARN-addressed tag operations reach.
type kinesisTaggable struct {
	tags func() map[string]string
	save func(tags map[string]string, added int) error
}

// resolveTaggable loads the stream or consumer an ARN names.
func (p *KinesisPlugin) resolveTaggable(arn string) (kinesisTaggable, error) {
	if arn == "" {
		return kinesisTaggable{}, kinesisInvalidArgument("ResourceARN is required")
	}
	if strings.Contains(arn, kinesisConsumerARNInfix) {
		consumer, err := p.loadConsumer(kinesisConsumerRef{ConsumerARN: arn})
		if err != nil {
			return kinesisTaggable{}, err
		}
		return kinesisTaggable{
			tags: func() map[string]string { return consumer.Tags },
			save: func(tags map[string]string, _ int) error {
				consumer.Tags = tags
				return p.saveConsumer(consumer)
			},
		}, nil
	}
	target, refErr := kinesisParseStreamARN(arn)
	if refErr != nil {
		return kinesisTaggable{}, kinesisInvalidArgument("ResourceARN is not a Kinesis stream or consumer ARN: " + arn)
	}
	stream, err := p.loadStream(target)
	if err != nil {
		return kinesisTaggable{}, err
	}
	return kinesisTaggable{
		tags: func() map[string]string { return stream.Tags },
		save: func(tags map[string]string, added int) error {
			stream.EverTagged = taggingEverTagged(stream.EverTagged, len(stream.Tags), added)
			stream.Tags = tags
			return p.saveStream(stream)
		},
	}, nil
}

// kinesisCheckResourceTagQuota refuses a tag set that would exceed 50 tags on one resource.
func kinesisCheckResourceTagQuota(resource string, existing, adding map[string]string) *AWSError {
	fresh := 0
	for key := range adding {
		if _, ok := existing[key]; !ok {
			fresh++
		}
	}
	if len(existing)+fresh <= kinesisMaxTagsPerStream {
		return nil
	}
	return kinesisLimitExceeded(fmt.Sprintf(
		"Resource %s has %d tags and this request adds %d more, which exceeds the limit of %d tags per resource",
		resource, len(existing), fresh, kinesisMaxTagsPerStream))
}

func (p *KinesisPlugin) tagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string            `json:"ResourceARN"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	if tagErr := kinesisValidateTagMap(body.Tags); tagErr != nil {
		return nil, tagErr
	}
	res, err := p.resolveTaggable(body.ResourceARN)
	if err != nil {
		return nil, err
	}
	existing := res.tags()
	if quotaErr := kinesisCheckResourceTagQuota(body.ResourceARN, existing, body.Tags); quotaErr != nil {
		return nil, quotaErr
	}
	merged := make(map[string]string, len(existing)+len(body.Tags))
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range body.Tags {
		merged[k] = v
	}
	if err := res.save(merged, len(body.Tags)); err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) untagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	if n := len(body.TagKeys); n < 1 || n > kinesisMaxUntagKeys {
		return nil, kinesisInvalidArgument(fmt.Sprintf("TagKeys must hold between 1 and %d keys; it holds %d.", kinesisMaxUntagKeys, n))
	}
	res, err := p.resolveTaggable(body.ResourceARN)
	if err != nil {
		return nil, err
	}
	kept := make(map[string]string)
	for k, v := range res.tags() {
		kept[k] = v
	}
	for _, k := range body.TagKeys {
		delete(kept, k)
	}
	if err := res.save(kept, 0); err != nil {
		return nil, err
	}
	return kinesisJSONResponse(http.StatusOK, struct{}{})
}

func (p *KinesisPlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string `json:"ResourceARN"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, kinesisInvalidBody()
	}
	res, err := p.resolveTaggable(body.ResourceARN)
	if err != nil {
		return nil, err
	}
	type tagItem struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	}
	tags := make([]tagItem, 0, len(res.tags()))
	for k, v := range res.tags() {
		tags = append(tags, tagItem{Key: k, Value: v})
	}
	sortTagsByKey(tags, func(t tagItem) string { return t.Key })
	return kinesisJSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}
