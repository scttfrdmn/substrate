package emulator

import (
	"fmt"
	"net/http"
)

// ecrTag is one entry of the `tags` array ECR publishes.
//
// The member names are capitalized `Key` and `Value` inside a service whose every other member —
// `repositoryName`, `resourceArn`, `tagKeys` — is lowerCamelCase. That is AWS's shape and not a
// transcription slip: `API_Tag` publishes `Key` and `Value` with a capital, both `Required: Yes`, and
// the Request Syntax of `API_CreateRepository` and `API_TagResource` and the Response Syntax of
// `API_ListTagsForResource` all spell them that way. It is the kind of shape that cannot be guessed
// from the surrounding service, which is why substrate had it wrong: all three sites decoded and
// rendered a JSON *object* (#1017), so the array a real SDK sends failed to unmarshal into a
// map[string]string and every tagged CreateRepository or TagResource answered
// InvalidParameterException/400.
//
// Only the wire changes. Storage stays a map[string]string on [ECRRepository], which is what every
// other service's tag store holds, what `mergeResourceTags` and the Resource Groups Tagging API
// operate on, and what #1013's rule asks for: a record projects through a wire struct rather than
// going onto the wire as it is stored.
type ecrTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ecrTagsToMap folds a published `tags` array into the storage shape, refusing an entry with no key.
//
// `Key` is `Required: Yes` on `API_Tag`, and a required member that is absent is
// InvalidParameterException/400 — the code all three operations that carry `tags` publish, and the
// one this service already answers for an absent `repositoryName`. An entry naming a key twice keeps
// the last value, which is what a map assignment does and what merging into the stored map would do
// anyway; AWS publishes nothing about a duplicate key.
//
// An empty *value* is accepted. `API_Tag` marks `Value` `Required: Yes` as well, but
// `API_CreateRepository`'s own description of `tags` says each tag "consists of a key and an
// **optional** value". The page contradicts itself, and refusing the empty string would refuse a tag
// AWS's prose invites — so the narrower reading is taken, and only a missing key is refused.
func ecrTagsToMap(tags []ecrTag) (map[string]string, *AWSError) {
	if len(tags) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(tags))
	for i, t := range tags {
		if t.Key == "" {
			return nil, &AWSError{
				Code:       "InvalidParameterException",
				Message:    fmt.Sprintf("tags[%d].Key is required", i),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		out[t.Key] = t.Value
	}
	return out, nil
}

// ecrTagList renders a stored tag map as the published array, ordered by key.
//
// The order is substrate's reading: `API_ListTagsForResource` publishes no order for `tags`, and its
// sample response carries one entry. Lexicographic is taken for the reason [sortTagsByKey] records —
// a recorded run has to replay byte-identically (#862), and ranging a Go map put map order on the
// wire.
//
// An empty set renders as `[]` rather than `null` or an absent member, which is why this returns a
// zero-length slice where [mapToTaggingTags] returns nil: #938's rule is that an empty tag set is
// reported rather than hidden, and an SDK decoding `null` into a list cannot tell "no tags" from "the
// service did not answer".
func ecrTagList(stored map[string]string) []ecrTag {
	tags := make([]ecrTag, 0, len(stored))
	for k, v := range stored {
		tags = append(tags, ecrTag{Key: k, Value: v})
	}
	sortTagsByKey(tags, func(t ecrTag) string { return t.Key })
	return tags
}
