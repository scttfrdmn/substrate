package emulator

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // API_CreateKeyPair publishes an RSA fingerprint as a SHA-1 digest; it identifies, it does not protect.
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
)

// The private key a CreateKeyPair answers, derived from the request's [IDMint] so a replayed
// CreateKeyPair answers the key material and fingerprint its recording did (#1296).
//
// # Why the key is constructed, not generated
//
// #1296 proposed handing the generator a deterministic io.Reader over the mint. That no longer
// works: the Go toolchain's ecdsa.GenerateKey and rsa.GenerateKey ignore a caller-supplied reader
// and always draw from the system's CSPRNG, so the same reader yields a different key every call.
// Substrate therefore builds each key from bytes the mint derives:
//
//   - ed25519: [ed25519.NewKeyFromSeed] is deterministic by definition, over 32 mint bytes.
//   - rsa: two 1024-bit primes are found by a deterministic search over an HMAC-SHA256 stream
//     keyed by 32 mint bytes, and the key assembled from them.
//
// A seedless mint — a hand-built RequestContext, which every plugin unit test carries — still
// yields a random key, because [IDMint.bytes] falls back to crypto/rand: only the 32 seed bytes
// come from the mint, and everything after them is a function of those.
//
// # Why the key type now matters
//
// CreateKeyPair answered keyType `rsa` (the published default) beside an EC P-256 key, a response
// that disagreed with itself (#1296). The key is now the type the request names, in the format the
// page publishes: "an unencrypted PEM encoded PKCS#1 private key" for rsa, which is the published
// sample's `RSA PRIVATE KEY`, and the OpenSSH private-key format for ed25519. The fingerprint is
// computed as the page states for each type.

// ec2KeyTypes is CreateKeyPair's KeyType Valid Values.
var ec2KeyTypes = []string{"rsa", "ed25519"}

// ec2KeyPairMaterial returns the PEM private key and the published fingerprint for a new key pair
// of keyType, drawing its seed from mint.
func ec2KeyPairMaterial(keyType string, mint *IDMint) (material, fingerprint string, err error) {
	seed := mint.bytes(32)
	switch keyType {
	case "ed25519":
		return ec2Ed25519Material(seed)
	case "rsa":
		return ec2RSAMaterial(seed)
	default:
		return "", "", fmt.Errorf("ec2 key material: unsupported key type %q", keyType)
	}
}

// ec2RSAMaterial builds a 2048-bit RSA key deterministically from seed. Its material is PKCS#1
// PEM, and its fingerprint is what API_CreateKeyPair publishes for an RSA key pair: "the SHA-1
// digest of the DER encoded private key", rendered colon-separated as the page's sample shows. The
// DER is PKCS#8, as the EC2 User Guide's verification command (`openssl pkcs8 … -topk8 | openssl
// sha1 -c`) computes it.
func ec2RSAMaterial(seed []byte) (material, fingerprint string, err error) {
	key, err := ec2DeterministicRSAKey(seed)
	if err != nil {
		return "", "", err
	}
	material = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", fmt.Errorf("ec2 key material: marshal RSA PKCS#8: %w", err)
	}
	sum := sha1.Sum(der) //nolint:gosec // The published fingerprint algorithm; see the import.
	return material, ec2ColonHex(sum[:]), nil
}

// ec2Ed25519Material builds an ed25519 key from seed. Its material is the OpenSSH private-key
// format, and its fingerprint is what API_CreateKeyPair publishes for ED25519: "the
// base64-encoded SHA-256 digest, which is the default for OpenSSH" — the digest of the public key
// in its SSH wire encoding.
func ec2Ed25519Material(seed []byte) (material, fingerprint string, err error) {
	priv := ed25519.NewKeyFromSeed(seed)
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return "", "", fmt.Errorf("ec2 key material: ed25519 public key has type %T", priv.Public())
	}
	pubBlob := sshString(nil, []byte("ssh-ed25519"))
	pubBlob = sshString(pubBlob, pub)

	// The two check integers must match each other; OpenSSH draws them at random. Deriving them
	// from the seed keeps the whole document a function of the seed.
	check := binary.BigEndian.Uint32(sha256Of(seed)[:4])
	var section []byte
	section = binary.BigEndian.AppendUint32(section, check)
	section = binary.BigEndian.AppendUint32(section, check)
	section = sshString(section, []byte("ssh-ed25519"))
	section = sshString(section, pub)
	section = sshString(section, priv) // ed25519's 64-byte private key: seed || public key.
	section = sshString(section, nil)  // An empty comment.
	for i := byte(1); len(section)%8 != 0; i++ {
		section = append(section, i)
	}

	doc := []byte("openssh-key-v1\x00")
	doc = sshString(doc, []byte("none")) // ciphername
	doc = sshString(doc, []byte("none")) // kdfname
	doc = sshString(doc, nil)            // kdfoptions
	doc = binary.BigEndian.AppendUint32(doc, 1)
	doc = sshString(doc, pubBlob)
	doc = sshString(doc, section)

	material = string(pem.EncodeToMemory(&pem.Block{Type: "OPENSSH PRIVATE KEY", Bytes: doc}))
	digest := sha256.Sum256(pubBlob)
	return material, base64.StdEncoding.EncodeToString(digest[:]), nil
}

// ec2DeterministicRSAKey assembles a 2048-bit RSA key whose primes are found by searching an
// HMAC-SHA256 stream keyed by seed: each candidate has its top two bits and its low bit set, and
// is stepped by two until it is a probable prime coprime to the public exponent. Setting the top
// two bits of both primes makes the modulus exactly 2048 bits.
func ec2DeterministicRSAKey(seed []byte) (*rsa.PrivateKey, error) {
	const e = 65537
	stream := &hmacStream{key: seed}
	exponent := big.NewInt(e)
	one := big.NewInt(1)
	two := big.NewInt(2)
	prime := func() *big.Int {
		b := stream.read(128)
		b[0] |= 0xC0
		b[len(b)-1] |= 1
		p := new(big.Int).SetBytes(b)
		pm1 := new(big.Int)
		gcd := new(big.Int)
		for {
			pm1.Sub(p, one)
			if gcd.GCD(nil, nil, exponent, pm1).Cmp(one) == 0 && p.ProbablyPrime(20) {
				return p
			}
			p.Add(p, two)
		}
	}
	p := prime()
	q := prime()
	for q.Cmp(p) == 0 {
		q = prime()
	}
	n := new(big.Int).Mul(p, q)
	if n.BitLen() != 2048 {
		return nil, fmt.Errorf("ec2 key material: derived RSA modulus is %d bits, not 2048", n.BitLen())
	}
	phi := new(big.Int).Mul(new(big.Int).Sub(p, one), new(big.Int).Sub(q, one))
	d := new(big.Int).ModInverse(exponent, phi)
	if d == nil {
		return nil, fmt.Errorf("ec2 key material: public exponent is not invertible")
	}
	key := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{N: n, E: e},
		D:         d,
		Primes:    []*big.Int{p, q},
	}
	key.Precompute()
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("ec2 key material: derived RSA key is invalid: %w", err)
	}
	return key, nil
}

// hmacStream is HMAC-SHA256 keyed by key in counter mode: a deterministic byte stream, the
// derivation [IDMint.bytes] uses, without its truncation.
type hmacStream struct {
	key     []byte
	counter uint64
	buf     []byte
}

// read returns the next n bytes of the stream.
func (s *hmacStream) read(n int) []byte {
	for len(s.buf) < n {
		mac := hmac.New(sha256.New, s.key)
		var block [8]byte
		binary.BigEndian.PutUint64(block[:], s.counter)
		_, _ = mac.Write(block[:]) // hash.Hash.Write never returns an error.
		s.buf = mac.Sum(s.buf)
		s.counter++
	}
	out := make([]byte, n)
	copy(out, s.buf[:n])
	s.buf = s.buf[n:]
	return out
}

// sshString appends b to dst as an SSH wire-format string: a uint32 length, then the bytes.
func sshString(dst, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b))) //nolint:gosec // Key fields are far below 4 GiB.
	return append(dst, b...)
}

// sha256Of returns the SHA-256 digest of b.
func sha256Of(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// ec2ColonHex renders b as lowercase hex pairs joined by colons, the form AWS prints an RSA key
// pair's fingerprint in.
func ec2ColonHex(b []byte) string {
	parts := make([]string, len(b))
	for i, c := range b {
		parts[i] = fmt.Sprintf("%02x", c)
	}
	return strings.Join(parts, ":")
}
