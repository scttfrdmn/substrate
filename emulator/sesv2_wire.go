package emulator

import (
	"maps"
	"slices"
)

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// SESv2Identity (sesv2_types.go) is a persisted state record, and getEmailIdentity used to hand it
// to the caller as the whole response body. The record carries AccountId and Region, which no SESv2
// shape publishes (#756), CreatedAt, which API_GetEmailIdentity does not publish either, and
// IdentityName, which the response does not carry because the caller named the identity in the
// request path. It also stores Tags as a map, where the response publishes an array of Tag objects,
// so a tagged identity's response could not be decoded by a typed SDK (#1335).
//
// Do not "fix" that by adding `json:"-"` to a state field or by retyping Tags in the record. Either
// changes the format of every recorded run, because MemoryStateManager snapshots those bytes and a
// replay reads them back. Projecting leaves the stored bytes unchanged, so a run recorded before
// this replays identically.

// sesv2TagOut is one element of a published Tags array.
type sesv2TagOut struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// sesv2IdentityOut is GetEmailIdentity's response body.
//
// Two of the members API_GetEmailIdentity publishes. DkimAttributes, VerificationStatus,
// VerifiedForSendingStatus and the rest are not modeled and are absent rather than present and
// empty (#1013's rule).
type sesv2IdentityOut struct {
	IdentityType string        `json:"IdentityType"`
	Tags         []sesv2TagOut `json:"Tags,omitempty"`
}

// sesv2IdentityToWire projects a persisted identity onto the published shape.
//
// The record holds its tags as a map, which has no order, so the array is sorted by key: the same
// identity answers the same bytes on every read, which is what lets a replay match its recording.
func sesv2IdentityToWire(identity SESv2Identity) sesv2IdentityOut {
	out := sesv2IdentityOut{IdentityType: identity.IdentityType}
	for _, key := range slices.Sorted(maps.Keys(identity.Tags)) {
		out.Tags = append(out.Tags, sesv2TagOut{Key: key, Value: identity.Tags[key]})
	}
	return out
}

// sesv2TagMap folds a published Tags array into the map the record stores, returning nil for no tags
// so an untagged identity's record is byte-identical to the one it has always written. A key given
// twice keeps its last value.
func sesv2TagMap(tags []sesv2TagOut) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[tag.Key] = tag.Value
	}
	return out
}
