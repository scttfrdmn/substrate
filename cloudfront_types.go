package substrate

import "time"

// cloudfrontNamespace is the state namespace for CloudFront.
const cloudfrontNamespace = "cloudfront"

// CloudFrontDistribution holds persisted state for a CloudFront distribution.
type CloudFrontDistribution struct {
	// ID is the unique CloudFront distribution identifier (e.g., E1ABCD2345678).
	ID string `json:"Id"`

	// ARN is the Amazon Resource Name for the distribution.
	ARN string `json:"ARN"`

	// Status is the current deployment status: Deployed or InProgress.
	Status string `json:"Status"`

	// DomainName is the CloudFront domain assigned to the distribution (e.g., {id}.cloudfront.net).
	DomainName string `json:"DomainName"`

	// Origins holds the origin configuration, stored as raw JSON to avoid tight coupling.
	Origins interface{} `json:"Origins,omitempty"`

	// DefaultCacheBehavior holds the default cache behavior, stored as raw JSON.
	DefaultCacheBehavior interface{} `json:"DefaultCacheBehavior,omitempty"`

	// Comment is an optional human-readable description of the distribution.
	Comment string `json:"Comment,omitempty"`

	// Enabled indicates whether the distribution is enabled to accept end-user requests.
	Enabled bool `json:"Enabled"`

	// Tags holds arbitrary key-value metadata attached to the distribution.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedTime is the time the distribution was created.
	CreatedTime time.Time `json:"CreatedTime"`

	// LastModifiedTime is the time the distribution configuration was last changed.
	LastModifiedTime time.Time `json:"LastModifiedTime"`

	// AccountID is the AWS account that owns the distribution.
	// CloudFront is a global service; no Region field is stored.
	AccountID string `json:"AccountID"`
}

// CloudFrontInvalidation holds state for a CloudFront invalidation request.
type CloudFrontInvalidation struct {
	// ID is the unique invalidation identifier.
	ID string `json:"Id"`

	// Status is the invalidation status (Completed).
	Status string `json:"Status"`

	// CreateTime is the time the invalidation was created.
	CreateTime time.Time `json:"CreateTime"`

	// Paths is the list of paths included in the invalidation.
	Paths []string `json:"Paths,omitempty"`
}

// cfDistKey returns the state key for a CloudFront distribution.
func cfDistKey(accountID, distID string) string {
	return "cfdist:" + accountID + "/" + distID
}

// cfDistIDsKey returns the state index key for all distribution IDs in an account.
func cfDistIDsKey(accountID string) string {
	return "cfdist_ids:" + accountID
}
