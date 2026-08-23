package emulator

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// defaultTestSecretKey is the secret for the built-in test credential. It is
// AWS's own documented example secret, which is also what substrate's docs pair
// with documentedExampleAccessKeyID.
const defaultTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

// documentedShortAccessKeyID is the access key README.md and
// docs/endpoint-configuration.md tell a caller to configure, paired with an
// identical secret.
const documentedShortAccessKeyID = "test"

// documentedShortSecretKey is the secret paired with documentedShortAccessKeyID.
const documentedShortSecretKey = "test"

// documentedExampleAccessKeyID is AWS's canonical example access key, which
// substrate's testing guide and every e2e check sign with.
const documentedExampleAccessKeyID = "AKIAIOSFODNN7EXAMPLE"

// NewCredentialRegistry creates a CredentialRegistry pre-loaded with the
// credentials substrate's own documentation tells a caller to use, each bound to
// account 123456789012: AKIATEST12345678901, "test" (README.md,
// docs/endpoint-configuration.md) and AKIAIOSFODNN7EXAMPLE (docs/testing-guide.md,
// test/e2e).
//
// All three are seeded because a registry is what
// [ServerOptions.VerifySignatures] checks a signature against, and a key it does
// not hold is InvalidClientTokenId 403 (#736). A registry that omitted them would
// make substrate's own quickstart wrong the moment a consumer set
// credentials.enabled in substrate.yaml — the caller would have followed the
// documentation exactly and been refused. Their secrets are documented too, so
// signing with them still proves the caller holds the secret; seeding them
// widens who can sign, not what a signature has to satisfy.
func NewCredentialRegistry() *CredentialRegistry {
	r := &CredentialRegistry{store: make(map[string]CredentialEntry)}
	for _, e := range []CredentialEntry{
		{AccessKeyID: defaultTestAccessKeyID, SecretAccessKey: defaultTestSecretKey},
		{AccessKeyID: documentedShortAccessKeyID, SecretAccessKey: documentedShortSecretKey},
		{AccessKeyID: documentedExampleAccessKeyID, SecretAccessKey: defaultTestSecretKey},
	} {
		e.AccountID = defaultAccountID
		r.store[e.AccessKeyID] = e
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
//
// This names the *access key*, not the IAM user holding it, so the ARN it
// produces resolves to no policies: nothing is stored under
// user_policies:AKIA…. That makes it the right answer only for a credential the
// registry knows and IAM does not — a caller who has an account but no IAM
// identity, who is therefore not authorized against anything. Use
// [resolvePrincipal] for a credential that may have an IAM entity behind it;
// see #411 for the enforcement that could not work while this was the only
// answer.
func buildCallerARN(accountID, accessKeyID string) string {
	return fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, accessKeyID)
}

// resolvePrincipal returns the principal an access key identifies, or nil when
// the key belongs to no IAM entity or STS session.
//
// Resolution is from *state*, not from a [CredentialRegistry], and that
// separation is the point. A registry entry answers "which account, and is this
// signature valid"; only IAM's own records answer "which principal", because the
// access key ID and the user's name are different strings. Deriving the ARN from
// the key (see [buildCallerARN]) yielded arn:aws:iam::123456789012:user/AKIA…,
// which matches no user_policies key, so a user with a perfectly good inline
// policy evaluated as though it had none.
//
// Keeping this independent of ServerOptions.Credentials is deliberate. It was
// once necessary as well: the registry also gated SigV4 verification, so wiring
// one in order to identify callers would have 403'd every credential substrate
// documents (AKIAIOSFODNN7EXAMPLE, test/test). That coupling is gone — see
// [ServerOptions.VerifySignatures] and #630 — but the separation stands on its
// own, because a registry entry answers "which account" and only IAM's records
// answer "which principal". Reading state costs one Get on a request that
// carries an Authorization header and refuses nothing.
//
// A nil principal means "no IAM identity", which [AuthController.CheckAccess]
// treats as unenforced rather than denied — enforcement is opt-in by creating
// the principal, so a caller who never touched IAM is unaffected.
//
// The second return is an account ID to adopt, or "" to keep the one already
// resolved. Both arms supply one, for the same reason: a credential's account is
// recorded when the credential is minted and appears nowhere on the wire, so a
// cross-account AssumeRole — or a long-term key belonging to another account's
// user — would otherwise leave the caller in the server's own account rather
// than the credential's (#737).
func resolvePrincipal(ctx context.Context, state StateManager, accountID, accessKeyID string) (*Principal, string) {
	if state == nil || accessKeyID == "" {
		return nil, ""
	}

	// A long-term key: IAMPlugin.CreateAccessKey stores the owning user's name
	// beside it, which is the link from a signed request to a set of policies.
	if raw, err := state.Get(ctx, iamNamespace, iamAccessKeyKey(accessKeyID)); err == nil && raw != nil {
		var key IAMAccessKey
		if unmarshalErr := json.Unmarshal(raw, &key); unmarshalErr == nil && key.UserName != "" {
			// The key's own account, when it has one. A record written before #737 does
			// not, and falls back to the account the request resolved to — which is what
			// it always used, so nothing that worked stops working.
			account, adopt := key.AccountID, key.AccountID
			if account == "" {
				account, adopt = accountID, ""
			}
			return &Principal{
				ARN:  fmt.Sprintf("arn:aws:iam::%s:user/%s", account, key.UserName),
				Type: "IAMUser",
				// The name AWS publishes as aws:username, taken from the record rather
				// than re-derived from the ARN above (#745). See [Principal.UserName].
				UserName: key.UserName,
			}, adopt
		}
	}

	// A temporary key: STSPlugin.assumeRole records the session under its own
	// access key ID, already carrying the assumed-role ARN it minted. Substrate
	// wrote that record from the beginning and nothing read it until #411, which
	// is why STS credentials authorized as nothing in particular.
	if raw, err := state.Get(ctx, stsNamespace, "session:"+accessKeyID); err == nil && raw != nil {
		var sess STSSessionCredentials
		if unmarshalErr := json.Unmarshal(raw, &sess); unmarshalErr == nil && sess.PrincipalARN != "" {
			// UserName is set only for a GetSessionToken session, whose principal *is*
			// an IAM user and for which AWS does publish aws:username; assumeRole
			// leaves it empty, because an assumed role has no user name. Type is
			// "AssumedRole" for both, which is why presence is keyed off the recorded
			// name rather than off Type (#745).
			return &Principal{
				ARN:      sess.PrincipalARN,
				Type:     "AssumedRole",
				UserName: sess.UserName,
			}, sess.AccountID
		}
	}

	return nil, ""
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
