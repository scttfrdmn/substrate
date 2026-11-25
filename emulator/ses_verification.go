package emulator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// SES identity verification, shared by SES v1 ([SESPlugin]) and SES v2 ([SESv2Plugin]).
//
// AWS verifies an identity outside the API: a domain when it finds the TXT record (or the DKIM
// CNAMEs) in the domain's DNS, an address when its owner clicks the link in the email SES sent.
// Neither is observable to substrate, so the status is a seeded value read at request time
// (POST /v1/ses/identity-verification), and it defaults to Success, the nominal path: an
// identity is verified as soon as it is created, and every consumer's wait loop
// (aws_ses_domain_identity_verification polls GetIdentityVerificationAttributes) finishes on
// its first poll. A seed of Pending, Failed, TemporaryFailure or NotStarted is how a test
// exercises the loop's other arms.
//
// One seed governs both APIs, so an identity reports the same status whichever asks: SES v1
// in its own spelling ("Pending"), SES v2 in its enum's ("PENDING").

// sesVerificationSeedPrefix is the state-key prefix of a verification seed. The rest of the
// key is "{account}/{region}/{identity}", each part "*" for a seed that applies to every value
// of it ([sesVerificationSeedKey]).
const sesVerificationSeedPrefix = "verification-seed:"

// sesVerificationSeedKey is the state key of the seed for one account, Region and identity. An
// empty part is the wildcard "*".
func sesVerificationSeedKey(accountID, region, identity string) string {
	wild := func(v string) string {
		if v == "" {
			return "*"
		}
		return v
	}
	return sesVerificationSeedPrefix + wild(accountID) + "/" + wild(region) + "/" + wild(identity)
}

// sesVerificationStatuses are SES v1's VerificationStatus enum, from the botocore ses model,
// mapped to SES v2's spelling of the same value.
var sesVerificationStatuses = map[string]string{
	"Pending":          "PENDING",
	"Success":          "SUCCESS",
	"Failed":           "FAILED",
	"TemporaryFailure": "TEMPORARY_FAILURE",
	"NotStarted":       "NOT_STARTED",
}

// sesVerificationStatus returns identity's seeded verification status in SES v1's spelling, for
// the account and Region that hold it: the most specific seed that matches, else Success. The
// identity is the most significant part, so a seed naming the identity in any account outranks
// a "*" identity seed scoped to this account; within either, an exact account outranks "*", and
// then an exact Region does.
func sesVerificationStatus(ctx context.Context, state StateManager, accountID, region, identity string) (string, error) {
	for _, id := range []string{identity, "*"} {
		for _, acct := range []string{accountID, "*"} {
			for _, rg := range []string{region, "*"} {
				key := sesVerificationSeedKey(acct, rg, id)
				data, err := state.Get(ctx, sesv2Namespace, key)
				if err != nil {
					return "", fmt.Errorf("ses verification seed %s: %w", key, err)
				}
				if data != nil {
					return string(data), nil
				}
			}
		}
	}
	return "Success", nil
}

// sesMailFromStatus is the MAIL FROM domain's status for an identity whose verification status
// is status. CustomMailFromStatus has no NotStarted, and a MAIL FROM domain that is set is
// being checked, so NotStarted reads as Pending.
func sesMailFromStatus(status string) string {
	if status == "NotStarted" {
		return "Pending"
	}
	return status
}

// sesVerificationToken is a domain's VerificationToken: 32 bytes in standard base64, the shape
// AWS issues (44 characters, the TXT value of `_amazonses.{domain}`). It is derived from the
// account, Region and domain rather than minted, so VerifyDomainIdentity answers the same token
// on every call, as AWS does for a domain it already holds, and GetIdentityVerificationAttributes
// can report it for a domain SES v2 created.
func sesVerificationToken(accountID, region, domain string) string {
	sum := sha256.Sum256([]byte("ses-verification|" + accountID + "|" + region + "|" + domain))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// sesDkimTokens are a domain's three Easy DKIM tokens, derived as [sesVerificationToken] is.
// AWS's tokens are 32 lowercase letters and digits, each naming a CNAME
// `{token}._domainkey.{domain}`; base32 of a digest, lowercased, has that alphabet.
func sesDkimTokens(accountID, region, domain string) []string {
	tokens := make([]string, 3)
	for i := range tokens {
		sum := sha256.Sum256([]byte("ses-dkim|" + accountID + "|" + region + "|" + domain + "|" + strconv.Itoa(i)))
		tokens[i] = strings.ToLower(base32.StdEncoding.EncodeToString(sum[:]))[:32]
	}
	return tokens
}

// sesVerificationSeedBody is the scoped form of POST /v1/ses/identity-verification's body. An
// omitted AccountID or Region applies the seeds to every account or Region.
type sesVerificationSeedBody struct {
	AccountID  string            `json:"accountId"`
	Region     string            `json:"region"`
	Identities map[string]string `json:"identities"`
}

// handleSESSeedIdentityVerification handles POST /v1/ses/identity-verification. The body maps an
// identity, or "*" for every identity without a seed of its own, to the verification status it
// reports, in one of two forms:
//
//	{"example.com": "Pending", "*": "Failed"}
//	{"accountId": "123456789012", "region": "us-east-1", "identities": {"example.com": "Pending"}}
//
// The bare form applies to every account and Region; the scoped form to the account and Region
// it names, an omitted one being the wildcard. A body carrying an "identities" member is the
// scoped form. The status is SES v1's spelling, one of Pending, Success, Failed,
// TemporaryFailure and NotStarted; SES v2 reports the same value in its own.
//
// Every entry is validated, in identity order, before any is written, so a refusal names the
// same entry on every run and a refused body writes nothing.
func (s *Server) handleSESSeedIdentityVerification(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	var body sesVerificationSeedBody
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	_, hasAccount := probe["accountId"]
	_, hasRegion := probe["region"]
	if _, scoped := probe["identities"]; scoped {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		err = dec.Decode(&body)
	} else if hasAccount || hasRegion {
		http.Error(w, `{"error":"accountId and region belong to the scoped form, which needs an identities member"}`, http.StatusBadRequest)
		return
	} else {
		err = json.Unmarshal(raw, &body.Identities)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	// A seed key is "{account}/{region}/{identity}" with "*" for a wildcard, so a scope part
	// carrying either character would name another seed's key.
	for _, part := range []string{body.AccountID, body.Region} {
		if strings.ContainsAny(part, "/*") {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, "accountId and region must not contain / or *: "+strconv.Quote(part)),
				http.StatusBadRequest)
			return
		}
	}
	identities := slices.Sorted(maps.Keys(body.Identities))
	for _, identity := range identities {
		status := body.Identities[identity]
		if identity == "" {
			http.Error(w, `{"error":"an identity must not be empty; use \"*\" for every identity"}`, http.StatusBadRequest)
			return
		}
		if _, ok := sesVerificationStatuses[status]; !ok {
			http.Error(w, fmt.Sprintf(`{"error":%q}`,
				"identity "+strconv.Quote(identity)+": status "+strconv.Quote(status)+
					" is not one of Pending, Success, Failed, TemporaryFailure, NotStarted"),
				http.StatusBadRequest)
			return
		}
	}
	for _, identity := range identities {
		key := sesVerificationSeedKey(body.AccountID, body.Region, identity)
		if err := s.state.Put(r.Context(), sesv2Namespace, key, []byte(body.Identities[identity])); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "seeded": len(identities)})
}

// handleSESClearIdentityVerification handles DELETE /v1/ses/identity-verification. With any of
// ?identity=, ?accountId= or ?region= it removes the one seed those name, an omitted part being
// the wildcard, exactly as the seed was posted; with none, every seed, so every identity reports
// Success again.
func (s *Server) handleSESClearIdentityVerification(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	identity, acct, region := q.Get("identity"), q.Get("accountId"), q.Get("region")
	var keys []string
	if identity != "" || acct != "" || region != "" {
		keys = []string{sesVerificationSeedKey(acct, region, identity)}
	} else {
		listed, err := s.state.List(r.Context(), sesv2Namespace, sesVerificationSeedPrefix)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		keys = listed
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), sesv2Namespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
