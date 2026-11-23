package emulator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"sync"
	"time"
)

// RequestIDSource assigns the request ID of every request a [Server] serves on its own
// behalf: each top-level AWS request, each recorded control-plane write, and each
// deployment a [Client] runs. Every identifier a request mints is derived from its
// request ID ([IDMint]), so this source is what decides whether two runs of the same
// calls answer the same identifiers (#1598).
//
// A source built with a non-zero seed is **seeded**. It hands out
// `req-<prefix>-<n>`, where prefix is eight hex characters derived from the seed and n
// counts the IDs drawn since the source was built or last rewound, starting at 1. Two
// servers given the same seed and the same sequence of requests therefore assign the
// same request IDs, mint the same resource identifiers from them, and — with the clock
// frozen at one epoch — answer the same bodies.
//
// A source with seed 0, and a nil source, are **unseeded**: [RequestIDSource.Next]
// returns the wall-clock-and-random ID substrate has always assigned, so a server
// configured with no seed behaves exactly as it did before the source existed.
//
// The prefix cannot collide with the request IDs substrate derives for internal
// dispatches (`req-clock-`, `req-cfn-`, `req-apigw-`), because each of those prefixes
// contains a letter outside the hex alphabet, and a replay recognizes clock-driven
// dispatches by theirs.
//
// The counter is assigned in the order requests reach [RequestIDSource.Next], which is
// arrival order for requests that do not overlap. Requests a client sends concurrently
// draw in whichever order the scheduler runs them, so their IDs — and the identifiers
// minted from them — can swap between runs; that is a documented limit of deterministic
// runs, not a defect.
//
// A replay never draws from this source: it dispatches every event under the request
// ID the event recorded ([ReplayEngine]), so replaying a stream on a live server leaves
// the live counter where it was.
//
// [RequestIDSource.Rewind] makes the IDs after a reset repeat the ones before it, so one
// event log can hold a request ID twice. A seeded stream therefore replays faithfully only
// between resets; recording the reset itself is #1631.
type RequestIDSource struct {
	mu     sync.Mutex
	seed   int64
	prefix string
	n      uint64
}

// NewRequestIDSource returns a source seeded with seed; 0 returns an unseeded one.
func NewRequestIDSource(seed int64) *RequestIDSource {
	s := &RequestIDSource{}
	s.SetSeed(seed)
	return s
}

// requestIDSeedPrefix derives the eight hex characters a seeded source's IDs carry.
//
// Hashed rather than rendered, so the ID does not advertise the seed in decimal and two
// nearby seeds do not produce nearby IDs a reader could confuse at a glance.
func requestIDSeedPrefix(seed int64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seed))
	sum := sha256.Sum256(append([]byte("substrate-run-seed\x00"), b[:]...))
	return hex.EncodeToString(sum[:4])
}

// Next returns the request ID for the next request. It is safe for concurrent use.
func (s *RequestIDSource) Next() string {
	if s == nil {
		return generateRequestID()
	}
	s.mu.Lock()
	if s.seed == 0 {
		s.mu.Unlock()
		return generateRequestID()
	}
	s.n++
	id := "req-" + s.prefix + "-" + strconv.FormatUint(s.n, 10)
	s.mu.Unlock()
	return id
}

// Seed returns the seed the source derives from; 0 means unseeded.
func (s *RequestIDSource) Seed() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seed
}

// Seeded reports whether the source derives its IDs from a seed.
func (s *RequestIDSource) Seeded() bool {
	return s.Seed() != 0
}

// Drawn returns how many IDs a seeded source has handed out since it was built or last
// rewound. It is always 0 for an unseeded source, which counts nothing.
func (s *RequestIDSource) Drawn() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// SetSeed reseeds the source and rewinds its counter, so the next ID is the first one the
// new seed yields. A seed of 0 makes the source unseeded.
func (s *RequestIDSource) SetSeed(seed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seed = seed
	s.prefix = ""
	if seed != 0 {
		s.prefix = requestIDSeedPrefix(seed)
	}
	s.n = 0
}

// Rewind restarts the counter, so the next ID is the first the seed yields.
// POST /v1/state/reset calls it, which is what makes each test case that shares a server
// see the IDs a fresh server would give it.
func (s *RequestIDSource) Rewind() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n = 0
}

// NewTimeController returns the clock a server configured with this run should start
// with: frozen at [RunCfg.Epoch] when one is set, and otherwise running from the wall
// clock, as every server's clock always has.
//
// Building the clock pinned, rather than pinning it once the server exists, matters
// because plugins are registered against the clock first, and anything a plugin stamps at
// registration would otherwise read the wall clock. [NewServer] pins the clock as well,
// which is a no-op on one built here.
func (c RunCfg) NewTimeController() (*TimeController, error) {
	epoch, ok, err := c.ParseEpoch()
	if err != nil {
		return nil, err
	}
	if !ok {
		return NewTimeController(time.Now()), nil
	}
	tc := NewTimeController(epoch)
	tc.Freeze()
	tc.SetTime(epoch)
	return tc, nil
}
