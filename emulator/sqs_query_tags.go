package emulator

import "fmt"

// How a query-protocol SQS request spells a tag set, and why substrate accepts two spellings when
// AWS publishes only one (#1087).
//
// AWS's query samples for both tagging doors are **unindexed**. API_CreateQueue's is
// `&Tag.Key=QueueType&Tag.Value=Production`, and API_TagQueue's is the same string. Neither page
// publishes a `Tag.N.Key` form anywhere — not in a sample, not in a parameter description — even
// though the very same CreateQueue sample indexes its *attributes*
// (`&Attribute.1.Name=&Attribute.1.Value=`). So the published form can carry exactly one pair, and
// substrate parsed only the form AWS never published: [SQSPlugin.tagQueue] read `Tag.%d.Key`
// counting from 1 and nothing else, so the only spelling the reference shows was dropped.
//
// The indexed form still has to be accepted, because it is what a query-protocol SDK serialiser
// emits for a map member and therefore the form every real call arrives in. Both are read here:
// indexed first, and the unindexed pair only when the indexed scan matched nothing, so a request
// carrying both (which no publisher and no serialiser produces) resolves the same way every time
// rather than depending on map iteration.
//
// One function serves CreateQueue and TagQueue so the loop is not written twice. Sharing it widens
// TagQueue to the published unindexed form, which is a strict widening: every request TagQueue
// accepted before is still accepted, with the same result.
//
// The JSON protocol needs nothing here. CreateQueue publishes the member as lowercase `tags` and
// TagQueue as capitalised `Tags` — AWS's own inconsistency rather than a transcription slip — but
// encoding/json falls back to a case-insensitive member match when no exact one is found, so a
// single `json:"Tags"` field decodes both published spellings.

// sqsParseQueryTags extracts a tag set from a query-protocol request's parameters, reading the
// indexed `Tag.N.Key`/`Tag.N.Value` form an SDK emits and the unindexed `Tag.Key`/`Tag.Value` form
// AWS publishes. See the file preamble for why both.
//
// The returned map is never nil, so a caller can merge into it without a presence check. A
// `Tag.Value` with no matching key is not a tag and is ignored, which is how the indexed scan
// already treated an empty key.
func sqsParseQueryTags(params map[string]string) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("Tag.%d.Key", i)]
		if key == "" {
			break
		}
		tags[key] = params[fmt.Sprintf("Tag.%d.Value", i)]
	}
	if len(tags) > 0 {
		return tags
	}
	if key := params["Tag.Key"]; key != "" {
		tags[key] = params["Tag.Value"]
	}
	return tags
}
