package substrate

import "time"

// sesv2Namespace is the state namespace for Amazon SES v2 resources.
const sesv2Namespace = "sesv2"

// SESv2CapturedEmail holds a captured outbound email for test assertions.
type SESv2CapturedEmail struct {
	// MessageID is the unique identifier for the sent email.
	MessageID string `json:"MessageId"`
	// To holds the recipient email addresses.
	To []string `json:"To"`
	// From is the sender email address.
	From string `json:"From"`
	// Subject is the email subject line.
	Subject string `json:"Subject"`
	// Body is the plain-text or HTML body of the email.
	Body string `json:"Body"`
	// SentAt is the time the email was sent.
	SentAt time.Time `json:"SentAt"`
	// AccountID is the AWS account that sent the email.
	AccountID string `json:"AccountId"`
	// Region is the AWS region from which the email was sent.
	Region string `json:"Region"`
}

// SESv2Identity represents an SES v2 email or domain identity.
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
}
