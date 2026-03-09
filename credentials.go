package substrate

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// CredentialEntry represents a simulated AWS credential pair bound to an account.
type CredentialEntry struct {
	// AccessKeyID is the AWS access key identifier.
	AccessKeyID string

	// SecretAccessKey is the secret used to sign requests.
	SecretAccessKey string

	// AccountID is the AWS account ID this credential belongs to.
	AccountID string

	// SessionToken is non-empty for STS temporary credentials.
	SessionToken string
}

// CredentialRegistry maps access key IDs to CredentialEntry values.
// It is safe for concurrent use.
type CredentialRegistry struct {
	mu    sync.RWMutex
	store map[string]CredentialEntry
}

// defaultTestAccessKeyID is the access key for the built-in test credential.
const defaultTestAccessKeyID = "AKIATEST12345678901"

// defaultTestSecretKey is the secret for the built-in test credential.
const defaultTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

// NewCredentialRegistry creates a CredentialRegistry pre-loaded with the
// built-in test credential (AKIATEST12345678901 → account 123456789012).
func NewCredentialRegistry() *CredentialRegistry {
	r := &CredentialRegistry{store: make(map[string]CredentialEntry)}
	r.store[defaultTestAccessKeyID] = CredentialEntry{
		AccessKeyID:     defaultTestAccessKeyID,
		SecretAccessKey: defaultTestSecretKey,
		AccountID:       testAccountID,
	}
	return r
}

// Register adds or replaces a credential entry keyed by AccessKeyID.
func (r *CredentialRegistry) Register(e CredentialEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[e.AccessKeyID] = e
}

// Lookup returns the CredentialEntry for the given access key ID and whether it
// was found.
func (r *CredentialRegistry) Lookup(accessKeyID string) (CredentialEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.store[accessKeyID]
	return e, ok
}

// extractAccessKeyFromAuth returns the access key ID from a SigV4 Authorization
// header, or "" if the header is absent or malformed.
func extractAccessKeyFromAuth(authHeader string) string {
	const credPrefix = "Credential="
	idx := strings.Index(authHeader, credPrefix)
	if idx < 0 {
		return ""
	}
	cred := authHeader[idx+len(credPrefix):]
	if end := strings.IndexAny(cred, "/, "); end > 0 {
		return cred[:end]
	}
	return ""
}

// buildCallerARN derives a principal ARN from an account ID and access key ID.
// AKIA-prefixed keys are treated as long-term IAM user credentials.
func buildCallerARN(accountID, accessKeyID string) string {
	return fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, accessKeyID)
}

// VerifySigV4 validates the SigV4 signature on r using secret keys from reg.
// Returns nil when reg is nil, the Authorization header is absent or not SigV4,
// or the signature matches. Returns an *AWSError with HTTP 403 otherwise.
func VerifySigV4(r *http.Request, body []byte, reg *CredentialRegistry) error {
	if reg == nil {
		return nil
	}
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return nil
	}

	accessKey, date, region, service, signedHeaders, signature, err := parseSigV4Auth(authHeader)
	if err != nil {
		return &AWSError{
			Code:       "InvalidClientTokenId",
			Message:    "The authorization header is malformed.",
			HTTPStatus: http.StatusForbidden,
		}
	}

	entry, ok := reg.Lookup(accessKey)
	if !ok {
		return &AWSError{
			Code:       "InvalidClientTokenId",
			Message:    "The security token included in the request is invalid.",
			HTTPStatus: http.StatusForbidden,
		}
	}

	// Build the canonical request.
	canonicalReq := buildCanonicalRequest(r, body, signedHeaders)

	// Build the string to sign.
	dateTime := r.Header.Get("X-Amz-Date")
	if dateTime == "" {
		dateTime = date + "T000000Z"
	}
	credentialScope := date + "/" + region + "/" + service + "/aws4_request"
	hashCanonical := sigV4SHA256Hex([]byte(canonicalReq))
	stringToSign := "AWS4-HMAC-SHA256\n" + dateTime + "\n" + credentialScope + "\n" + hashCanonical

	// Derive signing key and compute expected signature.
	signingKey := deriveSigningKey(entry.SecretAccessKey, date, region, service)
	expectedSig := hex.EncodeToString(sigV4HMAC(signingKey, []byte(stringToSign)))

	if !hmac.Equal([]byte(expectedSig), []byte(signature)) {
		return &AWSError{
			Code:       "SignatureDoesNotMatch",
			Message:    "The request signature we calculated does not match the signature you provided.",
			HTTPStatus: http.StatusForbidden,
		}
	}
	return nil
}

// parseSigV4Auth parses an AWS4-HMAC-SHA256 Authorization header into its
// component fields.
func parseSigV4Auth(authHeader string) (accessKey, date, region, service, signedHeaders, signature string, err error) {
	// Format: AWS4-HMAC-SHA256 Credential=<k>/<date>/<region>/<svc>/aws4_request, SignedHeaders=<h>, Signature=<s>
	body := strings.TrimPrefix(authHeader, "AWS4-HMAC-SHA256 ")
	parts := strings.SplitN(body, ", ", 3)
	if len(parts) < 3 {
		return "", "", "", "", "", "", fmt.Errorf("malformed Authorization header: expected 3 comma-separated parts")
	}

	for _, part := range parts {
		switch {
		case strings.HasPrefix(part, "Credential="):
			cred := strings.TrimPrefix(part, "Credential=")
			credParts := strings.Split(cred, "/")
			if len(credParts) < 5 {
				return "", "", "", "", "", "", fmt.Errorf("malformed Credential scope")
			}
			accessKey = credParts[0]
			date = credParts[1]
			region = credParts[2]
			service = credParts[3]
		case strings.HasPrefix(part, "SignedHeaders="):
			signedHeaders = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			signature = strings.TrimPrefix(part, "Signature=")
		}
	}

	if accessKey == "" || date == "" || signature == "" {
		return "", "", "", "", "", "", fmt.Errorf("missing required Authorization fields")
	}
	return accessKey, date, region, service, signedHeaders, signature, nil
}

// buildCanonicalRequest constructs the SigV4 canonical request string.
func buildCanonicalRequest(r *http.Request, body []byte, signedHeaderNames string) string {
	// URI: URL-encoded path.
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}

	// Canonical query string: sorted key=value pairs.
	canonicalQuery := buildCanonicalQueryString(r.URL.RawQuery)

	// Canonical headers: only the signed headers, lowercase name, trimmed value.
	headerNames := strings.Split(signedHeaderNames, ";")
	var canonicalHeaders strings.Builder
	for _, name := range headerNames {
		var val string
		if name == "host" {
			val = r.Host
			if val == "" {
				val = r.URL.Host
			}
		} else {
			val = r.Header.Get(name)
		}
		canonicalHeaders.WriteString(strings.ToLower(name))
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.TrimSpace(val))
		canonicalHeaders.WriteByte('\n')
	}

	bodyHash := sigV4SHA256Hex(body)

	return strings.Join([]string{
		r.Method,
		uri,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaderNames,
		bodyHash,
	}, "\n")
}

// buildCanonicalQueryString builds a canonical (sorted, URL-encoded) query string.
func buildCanonicalQueryString(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	parsed, err := url.ParseQuery(rawQuery)
	if err != nil {
		return rawQuery
	}
	keys := make([]string, 0, len(parsed))
	for k := range parsed {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		vals := parsed[k]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// deriveSigningKey derives the SigV4 signing key from the secret key and
// credential scope components.
func deriveSigningKey(secretKey, date, region, service string) []byte {
	kDate := sigV4HMAC([]byte("AWS4"+secretKey), []byte(date))
	kRegion := sigV4HMAC(kDate, []byte(region))
	kService := sigV4HMAC(kRegion, []byte(service))
	return sigV4HMAC(kService, []byte("aws4_request"))
}

// sigV4HMAC computes HMAC-SHA256 of data using key.
func sigV4HMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// sigV4SHA256Hex returns the lowercase hex-encoded SHA-256 hash of data.
func sigV4SHA256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
