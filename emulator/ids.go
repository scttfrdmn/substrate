package emulator

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync/atomic"
)

// Derived identifiers, so a replay mints what its recording minted (#856).
//
// Substrate drew most of its identifiers straight from crypto/rand, so a recorded run and
// its replay produced *different* ones. That is not a cosmetic difference: a replayed
// CreateVolume returns a new `vol-…`, the recorded CreateSnapshot that names the old one
// answers `InvalidVolume.NotFound`, and every describe after it reports an absent item. It
// is why #1140's regression tests had to be built on an S3 seed — no stream containing a
// create could be asserted to replay with zero differences — and it is why a `ValidateState`
// replay reports a `state_hash_after` mismatch for every create, which is the right answer
// and makes state validation useless. #833 meets the same wall from the other side: a
// replayed Authorization header names an access key absent from replayed state, so
// resolvePrincipal returns nil and CheckAccess *fails open*.
//
// # What an identifier is derived from
//
// HMAC-SHA256 keyed by the request's own id, over an ordinal that advances with each draw.
// Three properties, and each is the reason one of the alternatives was rejected.
//
// Reproducible across a replay, and across processes: [RequestContext.RequestID] during a
// replay is the *recorded* one, because replayEvent dispatches with replayRequestID (#866),
// so the same recorded request mints the same identifiers however many times and wherever it
// is replayed. A per-plugin seeded PRNG — [OmicsPlugin]'s pattern, the nearest thing
// substrate had — cannot reach that: its seed is per process, which its own doc comment says.
//
// Two identical creates still differ. Two CreateAccessKey calls for one user must produce
// two distinct keys, which is what rules out hashing the request: the requests are byte
// identical. The request *id* is not, because a live one is minted from the wall clock
// deliberately (see generateRequestID, and #866 for why it is not derived).
//
// Concurrency-safe by construction, because a mint belongs to one request. A shared source
// would hand two requests in flight whichever values they won a mutex for, so a replay of
// a run that had two calls in flight would not reproduce it. A monotonic counter in the
// [StateManager] would be reproducible — it is state, and a replay resets state — at the
// price of a state write per identifier and something to rewind in ResetForRun.
//
// The cost is that an identifier is *guessable* from a request id. That is acceptable and
// already true of the request id itself: substrate is a test emulator, its identifiers name
// nothing outside it, and none of them is a secret. A caller who needs unpredictability from
// a test double is asking the wrong tool.
//
// # What is deliberately still random
//
// The request id itself, which is the seed (#866 settled that it is wall-clock and recorded
// rather than derived). Substrate's own bookkeeping ids — an event id, a snapshot id, a
// replay id — which no AWS call observes. EC2 key-pair *material*, which needs a
// deterministic reader into the key generator rather than a string; a replayed CreateKeyPair
// still diverges on the key and its fingerprint (see the #856 checklist). And everything
// already derived from its inputs: a public IP from its instance id, a secret's ARN from its
// name, CloudFormation's stack UUIDs from account and region.
//
// TODO(#856): 11 draw sites remain on crypto/rand, tiered by service family on the issue.

// IDMint mints the identifiers one request publishes, derived from that request's own id so
// that replaying the request mints the same ones.
//
// A mint belongs to exactly one request. [RequestContext.IDs] carries it, so every plugin
// handler already holds one; helpers that mint without a request context in scope take the
// mint as a parameter rather than reaching for a package-level source.
type IDMint struct {
	// seed keys the HMAC. Empty means "no request id to derive from", and every draw then
	// falls back to crypto/rand — see [IDMint.bytes].
	seed string

	// ordinal is the counter each draw advances, so the second identifier a request mints
	// differs from the first. Atomic because a handler may mint from more than one
	// goroutine; the *values* are then assigned in whichever order the draws land, which is
	// why a handler that needs reproducible identifiers must mint them sequentially.
	ordinal atomic.Uint64
}

// NewIDMint returns a mint deriving identifiers from seed, which is the request id the
// identifiers belong to.
//
// An empty seed is not an error: it yields a mint that draws from crypto/rand, which is what
// every mint site did before #856. That is the behavior a mint site reached outside a request
// keeps — the CloudFormation deployer's internal request contexts, for instance — rather than
// minting a constant, which would collide two resources onto one id.
func NewIDMint(seed string) *IDMint {
	return &IDMint{seed: seed}
}

// Derived reports whether this mint derives its identifiers, rather than drawing them from
// crypto/rand. It is false for a nil mint and for one built from an empty seed.
func (m *IDMint) Derived() bool {
	return m != nil && m.seed != ""
}

// bytes returns n bytes for one identifier, and advances the ordinal.
//
// The derived path is HMAC-SHA256 keyed by the seed over the ordinal, extended in counter
// mode for an identifier wider than one digest — 96-byte STS session tokens are the only
// current caller past 32. A nil or seedless mint reads from crypto/rand instead, and panics
// on failure exactly as the sites it replaces did: a machine whose CSPRNG is unavailable
// cannot serve an identifier at all, and returning a zero one would put a colliding id into
// state.
func (m *IDMint) bytes(n int) []byte {
	if n <= 0 {
		return nil
	}
	if !m.Derived() {
		b := make([]byte, n)
		if _, err := rand.Read(b); err != nil {
			panic(fmt.Sprintf("emulator: crypto/rand read for %d bytes failed: %v", n, err))
		}
		return b
	}

	ordinal := m.ordinal.Add(1)
	out := make([]byte, 0, n)
	for block := uint64(0); len(out) < n; block++ {
		mac := hmac.New(sha256.New, []byte(m.seed))
		var msg [16]byte
		binary.BigEndian.PutUint64(msg[0:8], ordinal)
		binary.BigEndian.PutUint64(msg[8:16], block)
		_, _ = mac.Write(msg[:]) // hash.Hash.Write never returns an error.
		out = mac.Sum(out)
	}
	return out[:n]
}

// Hex returns n bytes as a lowercase hex string, so the identifier is 2n characters.
func (m *IDMint) Hex(n int) string {
	return hex.EncodeToString(m.bytes(n))
}

// Chars returns an n-character identifier drawn from alphabet.
//
// For the identifiers AWS publishes in a restricted alphabet rather than in hex — an IAM
// entity id is 21 characters of uppercase letters and digits. The mapping is a modulo of
// one byte per character, which is very slightly biased toward the start of an alphabet
// whose length does not divide 256; that is the bias generateIAMID always had, and an
// identifier substrate mints has no uniformity requirement to lose.
func (m *IDMint) Chars(n int, alphabet string) string {
	if n <= 0 || alphabet == "" {
		return ""
	}
	raw := m.bytes(n)
	out := make([]byte, n)
	for i, b := range raw {
		out[i] = alphabet[int(b)%len(alphabet)]
	}
	return string(out)
}

// Digits returns an n-digit decimal identifier, which may have a leading zero.
func (m *IDMint) Digits(n int) string {
	return m.Chars(n, "0123456789")
}

// UUID returns an identifier in the RFC 4122 version-4 shape — 8-4-4-4-12 lowercase hex
// with the version and variant bits set — which is the form AWS publishes for the ids it
// documents as UUIDs.
//
// Version 4 means "random", and a derived value is not random; the shape is what a consumer
// parses, and a caller that requires unpredictability is in the position the file comment
// describes.
func (m *IDMint) UUID() string {
	b := m.bytes(16)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Base64 returns n bytes in standard base64, the encoding STS publishes its session tokens
// and secret access keys in.
func (m *IDMint) Base64(n int) string {
	return base64.StdEncoding.EncodeToString(m.bytes(n))
}

// Base64URL returns n bytes in unpadded URL-safe base64, the encoding OpenSearch generates a
// document id in.
//
// Separate from [IDMint.Base64] because the two differ in the two characters that matter here: a
// document id reaches an OpenSearch caller inside a URL path, so `-` and `_` are the alphabet and
// `+` and `/` are not. Unpadded because a generated id carries no `=`, and at OpenSearch's twelve
// bytes there would be none to carry — 12 divides by 3 — so the distinction only shows if a later
// caller asks for a width that does not.
func (m *IDMint) Base64URL(n int) string {
	return base64.RawURLEncoding.EncodeToString(m.bytes(n))
}

// HexUUID returns sixteen bytes in UUID *shape* — 8-4-4-4-12 lowercase hex — without the RFC
// 4122 version and variant bits [IDMint.UUID] sets.
//
// It exists because a dozen call sites across nine services published exactly this rendering
// from crypto/rand, and #856 is about making an identifier reproducible across a replay rather
// than about changing which bytes a caller sees: setting the two nibbles UUID sets would change
// every one of them. A new mint site that wants a real version-4 shape should use UUID; this is
// for the identifiers substrate already publishes in the looser form.
//
// The callers are Lambda revision and code ids, ECS task ids, Step Functions execution names,
// SQS message ids, EventBridge event ids, CloudWatch Logs upload sequence tokens, Service Quotas
// request ids, Batch job ids and EMR Serverless job-run ids. Until this method existed the first
// seven of those reached a helper declared in lambda_plugin.go, which is how a Batch job id came
// to be minted by a function named for a Lambda revision.
func (m *IDMint) HexUUID() string {
	h := m.Hex(16)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
