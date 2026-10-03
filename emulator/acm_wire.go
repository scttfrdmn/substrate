package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// ACMCertificate (acm_types.go) is a persisted state record, and describeCertificate handed it
// straight to the caller under `Certificate`. That answered AccountID, Region and ever_tagged, which
// no ACM shape publishes (#756), and Tags, which API_CertificateDetail does not publish either: a
// certificate's tags are read through ListTagsForCertificate. It also rendered the four dates as
// RFC3339 strings (#1305).
//
// Do not "fix" that by adding `json:"-"` to a state field or by retyping the dates in the record.
// Either changes the format of every recorded run, because MemoryStateManager snapshots those bytes
// and a replay reads them back. Projecting leaves the stored bytes unchanged.
//
// # Why ACM's dates are EpochSeconds
//
// ACM speaks awsJson1_1, where a Timestamp is published as epoch seconds with fractional precision.
// API_CertificateDetail types CreatedAt, IssuedAt, NotAfter and NotBefore as Timestamp, and an
// awsJson1_1 timestamp deserializer expects a number, so the RFC3339 strings the record marshaled to
// made DescribeCertificate undecodable in a typed SDK (#1305). The record sets all four on every
// certificate it writes, so none renders as null.

// acmCertificateOut is the Certificate element of DescribeCertificate's response.
//
// Ten of API_CertificateDetail's members — the ones the record models. The rest are absent rather
// than present and empty (#1013's rule, #1199's gap).
type acmCertificateOut struct {
	CertificateArn          string       `json:"CertificateArn"`
	CreatedAt               EpochSeconds `json:"CreatedAt"`
	DomainName              string       `json:"DomainName"`
	IssuedAt                EpochSeconds `json:"IssuedAt"`
	KeyAlgorithm            string       `json:"KeyAlgorithm"`
	NotAfter                EpochSeconds `json:"NotAfter"`
	NotBefore               EpochSeconds `json:"NotBefore"`
	Status                  string       `json:"Status"`
	SubjectAlternativeNames []string     `json:"SubjectAlternativeNames"`
	Type                    string       `json:"Type"`
}

// acmCertificateToWire projects a persisted certificate onto the published shape.
func acmCertificateToWire(cert ACMCertificate) acmCertificateOut {
	return acmCertificateOut{
		CertificateArn:          cert.CertificateArn,
		CreatedAt:               EpochSeconds(cert.CreatedAt),
		DomainName:              cert.DomainName,
		IssuedAt:                EpochSeconds(cert.IssuedAt),
		KeyAlgorithm:            cert.KeyAlgorithm,
		NotAfter:                EpochSeconds(cert.NotAfter),
		NotBefore:               EpochSeconds(cert.NotBefore),
		Status:                  cert.Status,
		SubjectAlternativeNames: cert.SubjectAlternativeNames,
		Type:                    cert.Type,
	}
}
