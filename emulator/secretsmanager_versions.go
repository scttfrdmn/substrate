package emulator

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
)

// A secret version's identity (#1285).
//
// API_PutSecretValue, API_CreateSecret and API_RotateSecret publish ClientRequestToken with the same
// meaning and the same "Length Constraints: Minimum length of 32. Maximum length of 64." — and say of
// it: "This value becomes the VersionId of the new version." So a caller-supplied token *is* the
// version ID, and the idempotency contract PutSecretValue and CreateSecret both publish is built on it:
//
//   - a token not yet associated with a version of the secret creates a new version;
//   - a token naming an existing version whose SecretString or SecretBinary equals the request's is
//     ignored: "the operation succeeds but does nothing";
//   - a token naming an existing version that holds a different value fails, "because you can't
//     modify an existing version". ResourceExistsException, 400, is the code each page publishes that
//     fits; InvalidRequestException is published too, but none of its listed causes is this one.
//
// A token outside 32–64 characters is refused with InvalidParameterException ("The parameter name or
// value is invalid", 400), the code each of these pages publishes for a bad member value and the one
// the plugin already answers for an out-of-range member (RotationRules' AutomaticallyAfterDays). With
// no token, substrate mints one, [generateVersionID].

const (
	// smMinClientRequestToken is the shortest ClientRequestToken, and VersionId, AWS publishes.
	smMinClientRequestToken = 32

	// smMaxClientRequestToken is the longest, from the same constraint.
	smMaxClientRequestToken = 64
)

// smValidClientRequestToken refuses a supplied ClientRequestToken outside the published width. An
// empty token is valid here; whether one is required is the operation's decision.
func smValidClientRequestToken(token string) *AWSError {
	if token == "" {
		return nil
	}
	if n := len(token); n < smMinClientRequestToken || n > smMaxClientRequestToken {
		return &AWSError{
			Code: "InvalidParameterException",
			Message: fmt.Sprintf("ClientRequestToken must be from %d to %d characters, not %d",
				smMinClientRequestToken, smMaxClientRequestToken, n),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// smVersionID resolves the VersionId a write creates: the caller's ClientRequestToken when one was
// supplied, which [smValidClientRequestToken] has already checked, or a minted one when none was.
func smVersionID(token string, m *IDMint) string {
	if token != "" {
		return token
	}
	return generateVersionID(m)
}

// smExistingVersion reports whether the secret already holds a version under versionID and, if so,
// whether that version's value equals value. It is how a resubmitted token is told from a new one.
func (p *SecretsManagerPlugin) smExistingVersion(ctx context.Context, acct, region, name, versionID, value string) (exists, same bool, err error) {
	data, err := p.state.Get(ctx, secretsManagerNamespace, smSecretVersionStateKey(acct, region, name, versionID))
	if err != nil {
		return false, false, fmt.Errorf("sm load version %s: %w", versionID, err)
	}
	if data == nil {
		return false, false, nil
	}
	return true, bytes.Equal(data, []byte(value)), nil
}

// smVersionAlreadyExists is the published refusal for a resubmitted token carrying a different value.
func smVersionAlreadyExists(versionID string) *AWSError {
	return &AWSError{
		Code: "ResourceExistsException",
		Message: fmt.Sprintf("a version with the ClientRequestToken %q already exists and holds a different "+
			"secret value; an existing version cannot be modified", versionID),
		HTTPStatus: http.StatusBadRequest,
	}
}
