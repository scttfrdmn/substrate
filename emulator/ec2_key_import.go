package emulator

import (
	"bytes"
	"crypto/md5" //nolint:gosec // The EC2 User Guide publishes an imported RSA key's fingerprint as an MD5 digest; it identifies, it does not protect.
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/big"
	"net/http"
	"strings"
)

// ec2KeyNameMaxLen is KeyName's published bound: "Constraints: Up to 255 ASCII characters"
// (API_CreateKeyPair). The EC2 User Guide's import steps state the same bound for an imported
// key pair's name.
const ec2KeyNameMaxLen = 255

// ec2CheckKeyName refuses a key-pair name longer than 255 characters or carrying a non-ASCII
// byte (#1190). The code is InvalidParameterValue, the client-error table's code for a value that
// "is not valid"; no page publishes one for this constraint, and the message is substrate's own.
func ec2CheckKeyName(name string) *AWSError {
	ascii := true
	for i := 0; i < len(name); i++ {
		if name[i] > 0x7f {
			ascii = false
			break
		}
	}
	if len(name) <= ec2KeyNameMaxLen && ascii {
		return nil
	}
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Value for parameter KeyName is invalid: a key pair name is up to 255 ASCII characters",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CheckKeyFormat reads CreateKeyPair's KeyFormat, "Valid Values: pem | ppk", "Default: pem"
// (#1190).
//
// Through v0.128.0 it was not read, so KeyFormat=ppk answered PEM material with a 200. ppk is now
// refused rather than answered with a format the caller did not ask for: PuTTY's key format
// carries a MAC over the key, and substrate has no reference output to check a generated one
// against, so the PPK would be a claim nothing backs. The refusal is InvalidParameterValue, the
// client-error table's code for a value that "is unsupported"; its message is substrate's own and
// says the format is substrate's gap rather than AWS's. A value outside the two Valid Values is
// refused the way KeyType's is.
func ec2CheckKeyFormat(format string) *AWSError {
	switch format {
	case "", "pem":
		return nil
	case "ppk":
		return &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "KeyFormat ppk is not supported by substrate; request pem",
			HTTPStatus: http.StatusBadRequest,
		}
	default:
		return &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "Value (" + format + ") for parameter KeyFormat is invalid. Valid values are pem and ppk.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// ec2InvalidKeyFormatError is InvalidKeyPair.Format, with the client-error table's gloss as its
// message, verbatim.
func ec2InvalidKeyFormatError() *AWSError {
	return &AWSError{
		Code:       "InvalidKeyPair.Format",
		Message:    "The format of the public key you are attempting to import is not valid.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2ImportedKey reads an ImportKeyPair public key and returns its type and the fingerprint the
// published rule gives it (#1190).
//
// The formats are the EC2 User Guide's "Supported formats": the OpenSSH public key format
// (`ssh-rsa AAAA… comment`, `ssh-ed25519 AAAA… comment`), and for RSA only the base64-encoded DER
// format and RFC 4716's SSH public key file format. The supported types are RSA and ED25519;
// "Amazon EC2 does not accept DSA keys", and ECDSA is not listed. Anything else is
// InvalidKeyPair.Format, the client-error table's code for "The format of the public key you are
// attempting to import is not valid." Through v0.128.0 any bytes were accepted, and an ECDSA key
// was stored as `rsa`.
//
// The fingerprints:
//
//   - ED25519 is API_ImportKeyPair's "base64-encoded SHA-256 digest, which is the default for
//     OpenSSH" — the digest of the key's SSH wire encoding, as `ssh-keygen -l` computes it, padded
//     with "=", which the User Guide notes EC2 may append. It is the rule CreateKeyPair uses, so a
//     created ED25519 key re-imported keeps its fingerprint, as the User Guide says it does.
//   - RSA is the MD5 digest of the DER-encoded public key, colon-separated: the EC2 User Guide's
//     verification command for an imported key, `openssl rsa -in … -pubout -outform DER | openssl
//     md5 -c`. API_ImportKeyPair glosses it as "the MD5 public key fingerprint as specified in
//     section 4 of RFC 4716", which would digest the SSH wire encoding instead; the two disagree,
//     and substrate follows the User Guide's command because it is the one a caller runs to check
//     the value. Through v0.128.0 every import answered a SHA-256 of the raw bytes, which matched
//     neither.
//
// material is the decoded PublicKeyMaterial. A caller whose material was base64-encoded twice —
// the SDK encodes the blob it is handed, and a key file may already be base64 DER — is read
// through the extra layer.
func ec2ImportedKey(material []byte) (keyType, fingerprint string, awsErr *AWSError) {
	text := strings.TrimSpace(string(material))
	var blob []byte
	switch {
	case strings.HasPrefix(text, "---- BEGIN SSH2 PUBLIC KEY ----"):
		blob = ec2RFC4716Blob(text)
	case strings.HasPrefix(text, "ssh-rsa ") || strings.HasPrefix(text, "ssh-ed25519 "):
		fields := strings.Fields(text)
		decoded, err := base64.StdEncoding.DecodeString(fields[1])
		if err != nil {
			return "", "", ec2InvalidKeyFormatError()
		}
		blob = decoded
		if name, _, ok := sshReadString(blob); !ok || string(name) != fields[0] {
			return "", "", ec2InvalidKeyFormatError()
		}
	default:
		// The base64-encoded DER form, RSA only.
		der, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			der = material
		}
		pub, ok := ec2ParseDERPublicKey(der)
		if !ok {
			return "", "", ec2InvalidKeyFormatError()
		}
		fp, fpErr := ec2ImportedRSAFingerprint(pub)
		if fpErr != nil {
			return "", "", ec2InvalidKeyFormatError()
		}
		return "rsa", fp, nil
	}
	if blob == nil {
		return "", "", ec2InvalidKeyFormatError()
	}
	keyType, fingerprint, awsErr = ec2KeyFromSSHBlob(blob)
	if awsErr == nil && keyType != "rsa" && strings.HasPrefix(text, "---- BEGIN SSH2") {
		// The User Guide lists RFC 4716 as "(RSA only)".
		return "", "", ec2InvalidKeyFormatError()
	}
	return keyType, fingerprint, awsErr
}

// ec2KeyFromSSHBlob reads an SSH wire-format public key: an ssh-rsa key's exponent and modulus,
// or an ssh-ed25519 key's 32 bytes.
func ec2KeyFromSSHBlob(blob []byte) (keyType, fingerprint string, awsErr *AWSError) {
	name, rest, ok := sshReadString(blob)
	if !ok {
		return "", "", ec2InvalidKeyFormatError()
	}
	switch string(name) {
	case "ssh-rsa":
		e, rest, okE := sshReadString(rest)
		n, rest, okN := sshReadString(rest)
		if !okE || !okN || len(rest) != 0 {
			return "", "", ec2InvalidKeyFormatError()
		}
		exp := new(big.Int).SetBytes(e)
		if !exp.IsInt64() || exp.Int64() < 3 || exp.Int64() > 1<<31-1 {
			return "", "", ec2InvalidKeyFormatError()
		}
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(exp.Int64())}
		fp, err := ec2ImportedRSAFingerprint(pub)
		if err != nil {
			return "", "", ec2InvalidKeyFormatError()
		}
		return "rsa", fp, nil
	case "ssh-ed25519":
		key, rest, okKey := sshReadString(rest)
		if !okKey || len(key) != 32 || len(rest) != 0 {
			return "", "", ec2InvalidKeyFormatError()
		}
		digest := sha256.Sum256(blob)
		return "ed25519", base64.StdEncoding.EncodeToString(digest[:]), nil
	default:
		return "", "", ec2InvalidKeyFormatError()
	}
}

// ec2ImportedRSAFingerprint is an imported RSA key's fingerprint: the colon-separated MD5 of its
// DER-encoded SubjectPublicKeyInfo, which is what `openssl rsa -pubout -outform DER` writes.
func ec2ImportedRSAFingerprint(pub *rsa.PublicKey) (string, error) {
	if pub.N.Sign() <= 0 {
		return "", fmt.Errorf("ec2 import key: empty RSA modulus")
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("ec2 import key: marshal RSA public key: %w", err)
	}
	sum := md5.Sum(der) //nolint:gosec // The published fingerprint algorithm; see the import.
	return ec2ColonHex(sum[:]), nil
}

// ec2ParseDERPublicKey reads a DER RSA public key, as SubjectPublicKeyInfo or as PKCS#1.
func ec2ParseDERPublicKey(der []byte) (*rsa.PublicKey, bool) {
	if pub, err := x509.ParsePKIXPublicKey(der); err == nil {
		rsaPub, ok := pub.(*rsa.PublicKey)
		return rsaPub, ok
	}
	if pub, err := x509.ParsePKCS1PublicKey(der); err == nil {
		return pub, true
	}
	return nil, false
}

// ec2RFC4716Blob returns the key an RFC 4716 SSH public key file carries, or nil. Header lines
// ("Tag: value", continued by a trailing backslash) are skipped; the body is the base64 between
// the BEGIN and END markers.
func ec2RFC4716Blob(text string) []byte {
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	var body bytes.Buffer
	continued := false
	ended := false
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "---- END SSH2 PUBLIC KEY ----" {
			ended = true
			break
		}
		if continued || strings.Contains(line, ":") {
			continued = strings.HasSuffix(line, "\\")
			continue
		}
		body.WriteString(line)
	}
	if !ended {
		return nil
	}
	blob, err := base64.StdEncoding.DecodeString(body.String())
	if err != nil {
		return nil
	}
	return blob
}

// sshReadString reads one SSH wire-format string from b, returning it and what follows.
func sshReadString(b []byte) (s, rest []byte, ok bool) {
	if len(b) < 4 {
		return nil, nil, false
	}
	n := binary.BigEndian.Uint32(b)
	if uint64(n) > uint64(len(b)-4) {
		return nil, nil, false
	}
	return b[4 : 4+n], b[4+n:], true
}
