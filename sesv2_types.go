package substrate

import "time"

// sesv2Namespace is the state namespace for Amazon SES v2 resources.
const sesv2Namespace = "sesv2"

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
