package emulator

// How many tags a Kinesis stream may carry, and which of AWS's two published answers substrate
// enforces.
//
// `API_AddTagsToStream` states both numbers **inside a single parameter entry**, which is what makes
// this a judgement rather than a lookup. The `Tags` member's description reads *"A set of up to 50
// key-value pairs to use to create the tags. A tag consists of a required key and an optional value.
// You can add up to 50 tags per resource."* — and the constraint lines directly beneath it read *"Map
// Entries: Maximum number of 200 items."* The operation's own lede says 50 again (*"You can assign up
// to 50 tags to a data stream"*), and `API_ListTagsForStream`'s response `Tags` array publishes 0–200
// while its `Limit` maxes out at exactly 50. Until #965 substrate enforced neither, so a stream could
// hold 300 tags and `ListTagsForStream` would answer an array longer than its own published maximum:
// substrate emitting a response its own reference says cannot exist.
//
// **Both limits are real, and they are different limits, so both are enforced under different codes.**
// 200 is the request shape's own bound on one call; 50 is the resource's quota. The distinction is
// observable and a consumer's retry path may care which it hit:
//
//   - **more than 200 entries in one request** → `InvalidArgumentException`, whose description is this
//     case exactly ("A specified parameter exceeds its restrictions"). It is checked **first**, against
//     the request alone: a request that does not satisfy its own shape cannot be evaluated against
//     account state at all.
//   - **more than 50 tags on the stream once the request is applied** → `LimitExceededException`,
//     whose description is *"The requested resource exceeds the maximum number allowed"*. Both codes
//     are published on `AddTagsToStream` at HTTP 400, as every Kinesis error is.
//
// The quota is a property of the **stream**, not of the request, so it is counted against the
// **merged** result rather than against the incoming map: two accepted requests of thirty tags each
// must be refused on the second. A key already present does not count twice, since AWS states that
// *"`AddTagsToStream` overwrites any existing tags that correspond to the specified tag keys"* — so
// re-tagging a stream that is already at the quota with a key it already has is a rewrite, not a
// thirty-first tag, and succeeds.
//
// Substrate models no tag quota for any other service, so this is the first of its kind; SSM's absence
// is recorded the same way in `docs/services.md`. **The Resource Groups Tagging API does not enforce
// it**, because that path merges tags for sixteen services through one helper and refusing there needs
// the per-resource `FailedResourcesMap` semantics `TagResources` publishes. A caller can therefore
// still push a stream past fifty tags through `TagResources`, which is filed separately rather than
// half-done here; `AddTagsToStream` then refuses every further add, which is the correct answer for a
// stream over quota however it got there.

import (
	"fmt"
	"net/http"
)

const (
	// kinesisMaxTagsPerStream is the service quota on a stream's tag set, published in
	// AddTagsToStream's lede and twice in its Tags parameter's own description.
	kinesisMaxTagsPerStream = 50

	// kinesisMaxTagMapEntries is the request shape's bound on one AddTagsToStream call, published as
	// "Map Entries: Maximum number of 200 items". See the file preamble for why both numbers are real.
	kinesisMaxTagMapEntries = 200

	// kinesisMaxTagValueLength is the published maximum length of a tag value. Its minimum is 0,
	// because "a tag consists of a required key and an optional value", so an empty value is valid
	// where an empty key is not.
	kinesisMaxTagValueLength = 256
)

// kinesisValidateTagMap checks an AddTagsToStream request's Tags member against the constraints its
// own shape publishes: present, at most 200 entries, each key 1–128 characters and each value at most
// 256.
//
// An **absent** Tags member is refused, since it is the operation's one Required: Yes member other
// than the stream reference. An **empty** one is accepted as a no-op, which is substrate's reading:
// the map publishes a maximum entry count and no minimum, where the sibling RemoveTagsFromStream's
// TagKeys array publishes "Minimum number of 1 item" — so AWS states a minimum for that operation and
// declines to for this one, and reading one in anyway would refuse a request its shape admits.
//
// The message names the offending key, because a caller whose one bad tag is among fifty cannot act on
// a refusal that will not say which.
func kinesisValidateTagMap(tags map[string]string) *AWSError {
	if tags == nil {
		return kinesisInvalidTagArgument("Tags is required")
	}
	if len(tags) > kinesisMaxTagMapEntries {
		return kinesisInvalidTagArgument(fmt.Sprintf("Tags may contain at most %d entries; got %d",
			kinesisMaxTagMapEntries, len(tags)))
	}
	for key, value := range tags {
		if len(key) < 1 || len(key) > kinesisMaxTagKeyLength {
			return kinesisInvalidTagArgument(fmt.Sprintf(
				"tag key %q must be between 1 and %d characters; got %d",
				key, kinesisMaxTagKeyLength, len(key)))
		}
		if len(value) > kinesisMaxTagValueLength {
			return kinesisInvalidTagArgument(fmt.Sprintf(
				"tag value for key %q must be at most %d characters; got %d",
				key, kinesisMaxTagValueLength, len(value)))
		}
	}
	return nil
}

// kinesisCheckTagQuota reports whether applying adding to existing would leave the stream over the
// published fifty-tag quota.
//
// The count is of the **merged** set, so a key already on the stream is a rewrite rather than an
// addition — per *"AddTagsToStream overwrites any existing tags that correspond to the specified tag
// keys"* — and a stream at the quota can still have its existing tags' values changed.
//
// The message names the current count, the number of new keys and the limit, so a caller can tell a
// request that was too large from a stream that was already full.
func kinesisCheckTagQuota(target kinesisStreamTarget, existing, adding map[string]string) *AWSError {
	fresh := 0
	for key := range adding {
		if _, ok := existing[key]; !ok {
			fresh++
		}
	}
	if len(existing)+fresh <= kinesisMaxTagsPerStream {
		return nil
	}
	return &AWSError{
		Code: "LimitExceededException",
		Message: fmt.Sprintf(
			"Stream %s has %d tags and this request adds %d more, which exceeds the limit of %d tags per stream",
			kinesisStreamARN(target), len(existing), fresh, kinesisMaxTagsPerStream),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kinesisInvalidTagArgument reports that a tag parameter violates a constraint its own shape
// publishes. See [kinesisValidateTagMap] for why the code is InvalidArgumentException.
func kinesisInvalidTagArgument(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidArgumentException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
