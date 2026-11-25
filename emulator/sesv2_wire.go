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
// The members API_GetEmailIdentity publishes that an identity's record can answer. Verification
// is seeded and shared with SES v1 (ses_verification.go), so VerificationStatus is the seeded
// status in SES v2's spelling and VerifiedForSendingStatus is true exactly when it is SUCCESS.
// DkimAttributes and MailFromAttributes are present only once SES v1's VerifyDomainDkim or
// SetIdentityMailFromDomain set them up; Policies, ConfigurationSetName and VerificationInfo
// are not modeled and are absent rather than present and empty (#1013's rule).
type sesv2IdentityOut struct {
	IdentityType             string            `json:"IdentityType"`
	FeedbackForwardingStatus bool              `json:"FeedbackForwardingStatus"`
	VerifiedForSendingStatus bool              `json:"VerifiedForSendingStatus"`
	VerificationStatus       string            `json:"VerificationStatus"`
	DkimAttributes           *sesv2DkimOut     `json:"DkimAttributes,omitempty"`
	MailFromAttributes       *sesv2MailFromOut `json:"MailFromAttributes,omitempty"`
	Tags                     []sesv2TagOut     `json:"Tags,omitempty"`
}

// sesv2DkimOut is the DkimAttributes members an Easy DKIM setup answers.
type sesv2DkimOut struct {
	SigningEnabled bool     `json:"SigningEnabled"`
	Status         string   `json:"Status"`
	Tokens         []string `json:"Tokens"`
}

// sesv2MailFromOut is MailFromAttributes, in SES v2's enum spellings.
type sesv2MailFromOut struct {
	MailFromDomain       string `json:"MailFromDomain"`
	MailFromDomainStatus string `json:"MailFromDomainStatus"`
	BehaviorOnMxFailure  string `json:"BehaviorOnMxFailure"`
}

// sesv2IdentityToWire projects a persisted identity onto the published shape. status is the
// identity's seeded verification status in SES v1's spelling ([sesVerificationStatus]), and
// dkim its Easy DKIM state ([sesDkimFor]), which for an address is its domain's.
//
// The record holds its tags as a map, which has no order, so the array is sorted by key: the same
// identity answers the same bytes on every read, which is what lets a replay match its recording.
func sesv2IdentityToWire(identity SESv2Identity, status string, dkim sesDkimView) sesv2IdentityOut {
	v2Status := sesVerificationStatuses[status]
	out := sesv2IdentityOut{
		IdentityType:             identity.IdentityType,
		FeedbackForwardingStatus: !identity.ForwardingDisabled,
		VerifiedForSendingStatus: status == "Success",
		VerificationStatus:       v2Status,
	}
	if dkim.SetUp {
		out.DkimAttributes = &sesv2DkimOut{SigningEnabled: dkim.Enabled, Status: sesVerificationStatuses[dkim.Status], Tokens: dkim.Tokens}
	}
	if identity.MailFromDomain != "" {
		behavior := "USE_DEFAULT_VALUE"
		if identity.BehaviorOnMXFailure == "RejectMessage" {
			behavior = "REJECT_MESSAGE"
		}
		out.MailFromAttributes = &sesv2MailFromOut{
			MailFromDomain:       identity.MailFromDomain,
			MailFromDomainStatus: sesVerificationStatuses[sesMailFromStatus(status)],
			BehaviorOnMxFailure:  behavior,
		}
	}
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
