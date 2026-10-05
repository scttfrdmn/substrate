package emulator_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha1" //nolint:gosec // The fingerprint algorithm API_CreateKeyPair publishes for RSA.
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CreateKeyPair's material is derived from the request's IDMint (#1296), and is the key type the
// request names, in the format and with the fingerprint API_CreateKeyPair publishes.

// keyPairOut is the part of a CreateKeyPair response these tests read.
type keyPairOut struct {
	KeyPairID   string `xml:"keyPairId"`
	Fingerprint string `xml:"keyFingerprint"`
	KeyType     string `xml:"keyType"`
	Material    string `xml:"keyMaterial"`
}

// keyPairCreate issues CreateKeyPair through ctx and decodes the response.
func keyPairCreate(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext, params map[string]string) keyPairOut {
	t.Helper()
	full := map[string]string{"Action": "CreateKeyPair"}
	for k, v := range params {
		full[k] = v
	}
	var out keyPairOut
	natDecode(t, ec2Wire(t, p, ctx, "CreateKeyPair", full), &out)
	require.NotEmpty(t, out.Material)
	return out
}

func TestEC2KeyMaterial_ARecordedCreateKeyPairReplaysWithZeroDifferences(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	ts.FreezeTime()

	ec2ReplayCall(t, ts, "CreateKeyPair", url.Values{"KeyName": {"replay-rsa"}})
	ec2ReplayCall(t, ts, "CreateKeyPair", url.Values{"KeyName": {"replay-ed"}, "KeyType": {"ed25519"}})

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)
	assert.Empty(t, results.Differences,
		"a replayed CreateKeyPair answers the key material and fingerprint it recorded: %s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid, "and state_hash_after matches, fingerprint included: %v", results.StateErrors)
}

func TestEC2KeyMaterial_OneRequestIdAlwaysDerivesTheSameKey(t *testing.T) {
	t.Parallel()
	for _, keyType := range []string{"rsa", "ed25519"} {
		t.Run(keyType, func(t *testing.T) {
			t.Parallel()
			p1, ctx1, _ := setupEC2WirePlugin(t)
			p2, ctx2, _ := setupEC2WirePlugin(t)
			a := keyPairCreate(t, p1, ctx1, map[string]string{"KeyName": "k", "KeyType": keyType})
			b := keyPairCreate(t, p2, ctx2, map[string]string{"KeyName": "k", "KeyType": keyType})
			assert.Equal(t, a, b, "two servers given one request id mint one key")
		})
	}
}

func TestEC2KeyMaterial_TwoCreatesInOneRunAreDistinct(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupEC2WirePlugin(t)
	a := keyPairCreate(t, p, ctx, map[string]string{"KeyName": "first"})
	b := keyPairCreate(t, p, ctx, map[string]string{"KeyName": "second"})
	assert.NotEqual(t, a.Fingerprint, b.Fingerprint, "the ordinal advances, as everywhere in #856")
	assert.NotEqual(t, a.Material, b.Material)
}

func TestEC2KeyMaterial_ASeedlessMintStillDrawsARandomKey(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i := range 3 {
		p, ctx, _ := setupEC2WirePlugin(t)
		ctx.IDs = emulator.NewIDMint("") // What a hand-built RequestContext carries.
		got := keyPairCreate(t, p, ctx, map[string]string{"KeyName": fmt.Sprintf("seedless-%d", i), "KeyType": "ed25519"})
		assert.False(t, seen[got.Fingerprint], "a seedless mint must not yield a constant key")
		seen[got.Fingerprint] = true
	}
}

func TestEC2KeyMaterial_AnRSAKeyIsWhatThePagePublishes(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupEC2WirePlugin(t)
	got := keyPairCreate(t, p, ctx, map[string]string{"KeyName": "rsa-default"})
	assert.Equal(t, "rsa", got.KeyType, "rsa is the published default")

	block, _ := pem.Decode([]byte(got.Material))
	require.NotNil(t, block, "keyMaterial is PEM")
	assert.Equal(t, "RSA PRIVATE KEY", block.Type, "an unencrypted PEM encoded PKCS#1 private key, as the page states")
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	require.NoError(t, err)
	assert.Equal(t, 2048, key.N.BitLen(), "a 2048-bit RSA key pair, as the page states")
	require.NoError(t, key.Validate())

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	sum := sha1.Sum(der) //nolint:gosec // The published fingerprint algorithm.
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02x", b)
	}
	assert.Equal(t, strings.Join(parts, ":"), got.Fingerprint, "the SHA-1 digest of the DER encoded private key")
}

func TestEC2KeyMaterial_AnEd25519KeyIsWhatThePagePublishes(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupEC2WirePlugin(t)
	got := keyPairCreate(t, p, ctx, map[string]string{"KeyName": "ed", "KeyType": "ed25519"})
	assert.Equal(t, "ed25519", got.KeyType)

	block, _ := pem.Decode([]byte(got.Material))
	require.NotNil(t, block)
	assert.Equal(t, "OPENSSH PRIVATE KEY", block.Type)
	pubBlob, priv := parseOpenSSHEd25519(t, block.Bytes)

	digest := sha256.Sum256(pubBlob)
	assert.Equal(t, base64.StdEncoding.EncodeToString(digest[:]), got.Fingerprint,
		"the base64-encoded SHA-256 digest of the public key, the OpenSSH default")

	// The private key in the document is the one whose public key the fingerprint names.
	sig := ed25519.Sign(priv, []byte("substrate"))
	pub := pubBlob[len(pubBlob)-ed25519.PublicKeySize:]
	assert.True(t, ed25519.Verify(pub, []byte("substrate"), sig))
}

func TestEC2KeyMaterial_AnUnpublishedKeyTypeIsRefused(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupEC2WirePlugin(t)
	_, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "ec2", Operation: "CreateKeyPair", Path: "/",
		Params: map[string]string{"Action": "CreateKeyPair", "Version": "2016-11-15", "KeyName": "k", "KeyType": "ecdsa"},
	})
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "InvalidParameterValue", awsErr.Code)
}

// parseOpenSSHEd25519 parses an unencrypted openssh-key-v1 document holding one ed25519 key,
// returning the public key blob and the private key, and checking the document's own invariants.
func parseOpenSSHEd25519(t *testing.T, doc []byte) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	const magic = "openssh-key-v1\x00"
	require.True(t, bytes.HasPrefix(doc, []byte(magic)))
	rest := doc[len(magic):]
	next := func() []byte {
		require.GreaterOrEqual(t, len(rest), 4)
		n := binary.BigEndian.Uint32(rest)
		require.GreaterOrEqual(t, uint64(len(rest)-4), uint64(n))
		s := rest[4 : 4+n]
		rest = rest[4+n:]
		return s
	}
	assert.Equal(t, "none", string(next()), "ciphername")
	assert.Equal(t, "none", string(next()), "kdfname")
	assert.Empty(t, next(), "kdfoptions")
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(rest), "one key")
	rest = rest[4:]
	pubBlob := next()
	section := next()

	rest = section
	require.GreaterOrEqual(t, len(rest), 8)
	assert.Equal(t, rest[0:4], rest[4:8], "the two check integers match")
	rest = rest[8:]
	assert.Equal(t, "ssh-ed25519", string(next()))
	pub := next()
	priv := next()
	require.Len(t, priv, ed25519.PrivateKeySize)
	assert.Equal(t, pub, priv[32:], "the private key carries its public half")
	assert.Equal(t, 0, len(section)%8, "the private section is padded to the block size")
	return pubBlob, ed25519.PrivateKey(priv)
}
