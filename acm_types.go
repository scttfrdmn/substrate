package substrate

import "time"

// acmNamespace is the state namespace for ACM.
const acmNamespace = "acm"

// ACMCertificate holds the state of an ACM certificate.
type ACMCertificate struct {
	// CertificateArn is the Amazon Resource Name for the certificate.
	CertificateArn string `json:"CertificateArn"`

	// DomainName is the primary domain name for the certificate.
	DomainName string `json:"DomainName"`

	// SubjectAlternativeNames is the list of additional domain names.
	SubjectAlternativeNames []string `json:"SubjectAlternativeNames"`

	// Status is the certificate status (e.g., ISSUED, PENDING_VALIDATION).
	Status string `json:"Status"`

	// Type is the certificate type (AMAZON_ISSUED or IMPORTED).
	Type string `json:"Type"`

	// KeyAlgorithm is the algorithm used to generate the key pair (e.g., RSA_2048).
	KeyAlgorithm string `json:"KeyAlgorithm"`

	// Tags holds arbitrary key-value metadata attached to the certificate.
	Tags map[string]string `json:"Tags,omitempty"`

	// CreatedAt is the time the certificate was created.
	CreatedAt time.Time `json:"CreatedAt"`

	// IssuedAt is the time the certificate was issued.
	IssuedAt time.Time `json:"IssuedAt"`

	// NotAfter is the expiration time of the certificate.
	NotAfter time.Time `json:"NotAfter"`

	// NotBefore is the start of the certificate validity period.
	NotBefore time.Time `json:"NotBefore"`

	// AccountID is the AWS account that owns the certificate.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the certificate is stored.
	Region string `json:"Region"`
}

// acmCertKey returns the state key for an ACM certificate.
func acmCertKey(accountID, region, certArn string) string {
	return "cert:" + accountID + "/" + region + "/" + certArn
}

// acmCertARNsKey returns the state index key for all certificate ARNs in an account/region.
func acmCertARNsKey(accountID, region string) string {
	return "cert_arns:" + accountID + "/" + region
}
