package emulator

import "time"

// sesv2Namespace is the state namespace for Amazon SES resources. SES v1 ([SESPlugin]) and
// SES v2 ([SESv2Plugin]) share it: an identity verified through either API is one record,
// visible through both, as it is in AWS.
const sesv2Namespace = "sesv2"

// SESv2CapturedEmail holds a captured outbound email for test assertions. SES v1's SendEmail
// and SendRawEmail capture into the same record type as SES v2's SendEmail, so
// GET /v1/emails lists both.
type SESv2CapturedEmail struct {
	// MessageID is the unique identifier for the sent email.
	MessageID string `json:"MessageId"`
	// To holds the recipient email addresses.
	To []string `json:"To"`
	// Cc holds the carbon-copy recipients, when the sender named any.
	Cc []string `json:"Cc,omitempty"`
	// Bcc holds the blind-carbon-copy recipients, when the sender named any.
	Bcc []string `json:"Bcc,omitempty"`
	// From is the sender email address.
	From string `json:"From"`
	// Subject is the email subject line.
	Subject string `json:"Subject"`
	// Body is the plain-text or HTML body of the email.
	Body string `json:"Body"`
	// RawMessage is SendRawEmail's RawMessage.Data as the caller sent it, base64-encoded.
	RawMessage string `json:"RawMessage,omitempty"`
	// SentAt is the time the email was sent.
	SentAt time.Time `json:"SentAt"`
	// AccountID is the AWS account that sent the email.
	AccountID string `json:"AccountId"`
	// Region is the AWS region from which the email was sent.
	Region string `json:"Region"`
}

// SESv2Identity represents an SES email or domain identity, for both SES APIs.
//
// The members after CreatedAt are set only through SES v1's Set* and VerifyDomainDkim
// operations, and each is omitted while unset, so an identity no v1 call has touched stores
// the bytes it always has.
type SESv2Identity struct {
	// IdentityName is the email address or domain name.
	IdentityName string `json:"IdentityName"`
	// IdentityType is "EMAIL_ADDRESS" or "DOMAIN".
	IdentityType string `json:"IdentityType"`
	// AccountID is the AWS account ID that owns this identity.
	AccountID string `json:"AccountId"`
	// Region is the AWS region where the identity is registered.
	Region string `json:"Region"`
	// Tags holds optional resource tags.
	Tags map[string]string `json:"Tags,omitempty"`
	// CreatedAt is the time the identity was created.
	CreatedAt time.Time `json:"CreatedAt"`

	// DkimTokens are the three Easy DKIM tokens VerifyDomainDkim issued.
	DkimTokens []string `json:"DkimTokens,omitempty"`
	// DkimDisabled records SetIdentityDkimEnabled false. DKIM signing is enabled from
	// VerifyDomainDkim on.
	DkimDisabled bool `json:"DkimDisabled,omitempty"`
	// MailFromDomain is the custom MAIL FROM domain SetIdentityMailFromDomain set.
	MailFromDomain string `json:"MailFromDomain,omitempty"`
	// BehaviorOnMXFailure is SetIdentityMailFromDomain's BehaviorOnMXFailure, in SES v1's
	// spelling ("UseDefaultValue" or "RejectMessage").
	BehaviorOnMXFailure string `json:"BehaviorOnMXFailure,omitempty"`
	// BounceTopic is the SNS topic ARN SetIdentityNotificationTopic set for bounces.
	BounceTopic string `json:"BounceTopic,omitempty"`
	// ComplaintTopic is the SNS topic ARN SetIdentityNotificationTopic set for complaints.
	ComplaintTopic string `json:"ComplaintTopic,omitempty"`
	// DeliveryTopic is the SNS topic ARN SetIdentityNotificationTopic set for deliveries.
	DeliveryTopic string `json:"DeliveryTopic,omitempty"`
	// ForwardingDisabled records SetIdentityFeedbackForwardingEnabled false. Feedback
	// forwarding is enabled by default.
	ForwardingDisabled bool `json:"ForwardingDisabled,omitempty"`
	// HeadersInBounce records SetIdentityHeadersInNotificationsEnabled for bounces.
	HeadersInBounce bool `json:"HeadersInBounce,omitempty"`
	// HeadersInComplaint records SetIdentityHeadersInNotificationsEnabled for complaints.
	HeadersInComplaint bool `json:"HeadersInComplaint,omitempty"`
	// HeadersInDelivery records SetIdentityHeadersInNotificationsEnabled for deliveries.
	HeadersInDelivery bool `json:"HeadersInDelivery,omitempty"`
}
