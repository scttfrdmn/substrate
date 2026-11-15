package emulator_test

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/md5" //nolint:gosec // The fingerprint algorithm the EC2 User Guide publishes for an imported RSA key.
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sshWireString appends b to dst as an SSH wire-format string.
func sshWireString(dst, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b))) //nolint:gosec // Test keys are tiny.
	return append(dst, b...)
}

// ec2TestEd25519Blob is the SSH wire encoding of a fixed ed25519 public key.
func ec2TestEd25519Blob() []byte {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey) //nolint:forcetypeassert // ed25519's Public is always a PublicKey.
	return sshWireString(sshWireString(nil, []byte("ssh-ed25519")), pub)
}

// ec2TestEd25519AuthorizedKey is a real ed25519 public key in the OpenSSH public key format, the
// format ImportKeyPair accepts and validates since #1190.
func ec2TestEd25519AuthorizedKey() string {
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(ec2TestEd25519Blob()) + " test@substrate"
}

// ec2RSABlob is the SSH wire encoding of an RSA public key.
func ec2RSABlob(pub *rsa.PublicKey) []byte {
	mpint := func(n *big.Int) []byte {
		b := n.Bytes()
		if len(b) > 0 && b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	blob := sshWireString(nil, []byte("ssh-rsa"))
	blob = sshWireString(blob, mpint(big.NewInt(int64(pub.E))))
	return sshWireString(blob, mpint(pub.N))
}

// ec2ImportFingerprint imports material under name and returns the answered fingerprint, after
// requiring that the response carries no keyType.
func ec2ImportFingerprint(t *testing.T, ts *httptest.Server, name, material string) string {
	t.Helper()
	var doc struct {
		Fingerprint string  `xml:"keyFingerprint"`
		KeyType     *string `xml:"keyType"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "ImportKeyPair", "KeyName": name, "PublicKeyMaterial": material}, &doc)
	assert.Nil(t, doc.KeyType, "keyType is not among API_ImportKeyPair's Response Elements")
	return doc.Fingerprint
}

// ec2DescribedKey returns DescribeKeyPairs' keyType and fingerprint for name.
func ec2DescribedKey(t *testing.T, ts *httptest.Server, name string) (keyType, fingerprint string) {
	t.Helper()
	var doc struct {
		Type        string `xml:"keySet>item>keyType"`
		Fingerprint string `xml:"keySet>item>keyFingerprint"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeKeyPairs", "KeyName.1": name}, &doc)
	return doc.Type, doc.Fingerprint
}

// TestEC2_ImportKeyPair_FingerprintFollowsThePublishedRule is #1190's import half: an ED25519
// key's fingerprint is "the base64-encoded SHA-256 digest, which is the default for OpenSSH", and
// an RSA key's is the MD5 of its DER public key, as the EC2 User Guide's `openssl rsa -pubout
// -outform DER | openssl md5 -c` computes it — in each supported format.
func TestEC2_ImportKeyPair_FingerprintFollowsThePublishedRule(t *testing.T) {
	t.Parallel()
	edDigest := sha256.Sum256(ec2TestEd25519Blob())
	edWant := base64.StdEncoding.EncodeToString(edDigest[:])
	require.Len(t, edWant, 44, "a padded base64 SHA-256 digest")

	rsaKey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	require.NoError(t, err)
	sum := md5.Sum(der) //nolint:gosec // The published fingerprint algorithm.
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02x", b)
	}
	rsaWant := strings.Join(parts, ":")
	require.Len(t, rsaWant, 47, "a colon-separated MD5 digest")
	rsaLine := "ssh-rsa " + base64.StdEncoding.EncodeToString(ec2RSABlob(&rsaKey.PublicKey)) + " me@host"
	rfc4716 := "---- BEGIN SSH2 PUBLIC KEY ----\nComment: \"1024-bit RSA, converted by me@host\\\n from OpenSSH\"\n" +
		base64.StdEncoding.EncodeToString(ec2RSABlob(&rsaKey.PublicKey)) + "\n---- END SSH2 PUBLIC KEY ----\n"
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	tests := []struct {
		name, material, keyType, want string
	}{
		{"ed25519, OpenSSH format, as an SDK encodes it", b64(ec2TestEd25519AuthorizedKey()), "ed25519", edWant},
		{"ed25519, OpenSSH format, unencoded", ec2TestEd25519AuthorizedKey(), "ed25519", edWant},
		{"rsa, OpenSSH format", b64(rsaLine), "rsa", rsaWant},
		{"rsa, base64 DER", b64(base64.StdEncoding.EncodeToString(der)), "rsa", rsaWant},
		{"rsa, RFC 4716", b64(rfc4716), "rsa", rsaWant},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			assert.Equal(t, tc.want, ec2ImportFingerprint(t, ts, "k", tc.material))
			gotType, gotFP := ec2DescribedKey(t, ts, "k")
			assert.Equal(t, tc.keyType, gotType)
			assert.Equal(t, tc.want, gotFP)
		})
	}
}

// TestEC2_ImportKeyPair_CreatedEd25519KeyKeepsItsFingerprint pins the EC2 User Guide's "For
// ED25519 key pairs, the fingerprints will be same regardless of whether they're created by
// Amazon EC2 or imported to Amazon EC2".
func TestEC2_ImportKeyPair_CreatedEd25519KeyKeepsItsFingerprint(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var created struct {
		Fingerprint string `xml:"keyFingerprint"`
		Material    string `xml:"keyMaterial"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateKeyPair", "KeyName": "made", "KeyType": "ed25519"}, &created)
	block, _ := pem.Decode([]byte(created.Material))
	require.NotNil(t, block)
	pubBlob, _ := parseOpenSSHEd25519(t, block.Bytes)
	line := "ssh-ed25519 " + base64.StdEncoding.EncodeToString(pubBlob)
	assert.Equal(t, created.Fingerprint, ec2ImportFingerprint(t, ts, "again", line))
}

// TestEC2_ImportKeyPair_UnsupportedKeysAreRefused pins the User Guide's supported types and
// formats — RSA and ED25519, "Amazon EC2 does not accept DSA keys", base64 DER and RFC 4716 for
// RSA only — refused with InvalidKeyPair.Format, writing nothing.
func TestEC2_ImportKeyPair_UnsupportedKeysAreRefused(t *testing.T) {
	t.Parallel()
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecDER, err := x509.MarshalPKIXPublicKey(&ecKey.PublicKey)
	require.NoError(t, err)
	edRFC4716 := "---- BEGIN SSH2 PUBLIC KEY ----\n" + base64.StdEncoding.EncodeToString(ec2TestEd25519Blob()) +
		"\n---- END SSH2 PUBLIC KEY ----"
	for _, tc := range []struct{ name, material string }{
		{"not a key", "c3NoLWVkMjU1MTkgQUFBQUM="},
		{"a truncated ed25519 key", "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIexample"},
		{"an ECDSA key", "ecdsa-sha2-nistp256 " + base64.StdEncoding.EncodeToString(sshWireString(nil, []byte("ecdsa-sha2-nistp256")))},
		{"a DSA key", "ssh-dss " + base64.StdEncoding.EncodeToString(sshWireString(nil, []byte("ssh-dss")))},
		{"an ECDSA key as DER", base64.StdEncoding.EncodeToString(ecDER)},
		{"an ed25519 key as RFC 4716", edRFC4716},
		{"a type that disagrees with its blob", "ssh-rsa " + base64.StdEncoding.EncodeToString(ec2TestEd25519Blob())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			ec2RequireRefusalMessage(t, ts, map[string]string{
				"Action": "ImportKeyPair", "KeyName": "k", "PublicKeyMaterial": tc.material,
			}, "InvalidKeyPair.Format", "The format of the public key you are attempting to import is not valid.")
			var doc struct {
				Names []string `xml:"keySet>item>keyName"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeKeyPairs"}, &doc)
			assert.Empty(t, doc.Names, "a refused import writes nothing")
		})
	}
}

// TestEC2_KeyPair_NameAndFormatConstraints pins KeyName's "Constraints: Up to 255 ASCII
// characters" on both create paths, and CreateKeyPair's KeyFormat, "Valid Values: pem | ppk",
// whose ppk substrate refuses rather than answering PEM (#1190).
func TestEC2_KeyPair_NameAndFormatConstraints(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"a 255-character name", map[string]string{"KeyName": strings.Repeat("k", 255)}, ""},
		{"a 256-character name", map[string]string{"KeyName": strings.Repeat("k", 256)}, "InvalidParameterValue"},
		{"a non-ASCII name", map[string]string{"KeyName": "clé"}, "InvalidParameterValue"},
		{"KeyFormat=pem", map[string]string{"KeyName": "k", "KeyFormat": "pem"}, ""},
		{"KeyFormat=ppk", map[string]string{"KeyName": "k", "KeyFormat": "ppk"}, "InvalidParameterValue"},
		{"an unpublished KeyFormat", map[string]string{"KeyName": "k", "KeyFormat": "der"}, "InvalidParameterValue"},
	} {
		for _, action := range []string{"CreateKeyPair", "ImportKeyPair"} {
			if action == "ImportKeyPair" && tc.params["KeyFormat"] != "" {
				continue // KeyFormat is CreateKeyPair's alone.
			}
			t.Run(action+": "+tc.name, func(t *testing.T) {
				t.Parallel()
				ts := newEC2TestServer(t)
				params := map[string]string{"Action": action}
				for k, v := range tc.params {
					params[k] = v
				}
				if action == "ImportKeyPair" {
					params["PublicKeyMaterial"] = ec2TestEd25519AuthorizedKey()
				}
				if tc.code == "" {
					ec2OK(t, ts, params)
					return
				}
				ec2RequireRefusal(t, ts, params, tc.code)
			})
		}
	}
}
