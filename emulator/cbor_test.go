package emulator_test

import (
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The CBOR codec's tests assert wire bytes, not round trips (#785).
//
// A round-trip test is nearly worthless for a codec: one that is self-consistently
// wrong — a timestamp written as tag 0, a map written with an off-by-one length —
// passes every round trip and fails against every real client. So the encoder's
// output is compared against hexadecimal literals taken from RFC 8949 Appendix A and
// from Smithy's normative rpcv2Cbor protocol tests, and the decoder is fed the byte
// sequences those tests say a peer may send. The round-trip test at the end is a
// cheap extra, not the argument.

// pair is a shorthand for building an ordered map in the tables below.
type pair = emulator.CBORPairForTest

// cborMapOf is a shorthand for the ordered-map constructor.
func cborMapOf(pairs ...pair) any { return emulator.CBORMapForTest(pairs...) }

// mustHex decodes a hexadecimal literal, failing the test if it is malformed.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// TestCBOREncode_WireBytes pins the encoder's output byte for byte.
//
// The integer, string, array and float vectors are RFC 8949 Appendix A's, so a
// disagreement is substrate's and not a matter of interpretation. The choices that are
// substrate's own — definite length with a minimal argument, doubles always 0xfb,
// timestamps always tag 1 over a double — are pinned here because they are what makes
// a recorded response replayable, and nothing else would notice if they changed.
func TestCBOREncode_WireBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   any
		want string
	}{
		// RFC 8949 Appendix A, major type 0 and 1, exercising all five length forms.
		{name: "zero", in: 0, want: "00"},
		{name: "twenty-three is inline", in: 23, want: "17"},
		{name: "twenty-four takes one following byte", in: 24, want: "1818"},
		{name: "one hundred", in: 100, want: "1864"},
		{name: "one thousand takes two", in: 1000, want: "1903e8"},
		{name: "one million takes four", in: 1000000, want: "1a000f4240"},
		{name: "one trillion takes eight", in: int64(1000000000000), want: "1b000000e8d4a51000"},
		{name: "max int64", in: int64(math.MaxInt64), want: "1b7fffffffffffffff"},
		{name: "negative one", in: -1, want: "20"},
		{name: "negative twenty-four", in: -24, want: "37"},
		{name: "negative twenty-five", in: -25, want: "3818"},
		{name: "negative one thousand", in: -1000, want: "3903e7"},
		{
			// The -1-n encoding is computed as -(n+1) so the most negative int64 does
			// not overflow on the way to its unsigned argument.
			name: "min int64 does not overflow",
			in:   int64(math.MinInt64),
			want: "3b7fffffffffffffff",
		},
		{name: "int32 widens", in: int32(1000), want: "1903e8"},

		// Doubles are always 0xfb, never a narrower form, so the width never depends on
		// the value.
		{name: "one point zero is a double", in: 1.0, want: "fb3ff0000000000000"},
		{name: "one point five", in: 1.5, want: "fb3ff8000000000000"},
		{name: "negative zero keeps its sign", in: math.Copysign(0, -1), want: "fb8000000000000000"},
		{
			name: "a float32 is widened rather than written as 0xfa",
			in:   float32(1.5),
			want: "fb3ff8000000000000",
		},

		{name: "false", in: false, want: "f4"},
		{name: "true", in: true, want: "f5"},
		{
			// nil is an explicit null, which is only meaningful in a @sparse collection.
			// An absent optional member is omitted by the caller instead.
			name: "nil is null",
			in:   nil,
			want: "f6",
		},

		{name: "empty string", in: "", want: "60"},
		{name: "one-character string", in: "a", want: "6161"},
		{name: "IETF", in: "IETF", want: "6449455446"},
		{
			// The length is in bytes, not runes: "ü" is two bytes of UTF-8.
			name: "a multi-byte rune counts its bytes",
			in:   "ü",
			want: "62c3bc",
		},

		{name: "empty blob", in: []byte{}, want: "40"},
		{name: "blob", in: []byte{1, 2, 3, 4}, want: "4401020304"},

		{
			// Definite, not the protocol tests' indefinite `9f ff`.
			name: "empty list",
			in:   []any{},
			want: "80",
		},
		{name: "list of three integers", in: []any{1, 2, 3}, want: "83010203"},
		{name: "list of strings", in: []string{"a", "b"}, want: "8261616162"},
		{
			name: "list of doubles",
			in:   []float64{1.0, 1.5},
			want: "82fb3ff0000000000000fb3ff8000000000000",
		},

		{
			// Definite `a0`, not the protocol tests' canonical empty structure `bf ff`.
			// Both decode to the same thing, the decode table below pins that this
			// reader accepts theirs, and `a0` is what smithy-go's own encoder emits.
			name: "empty structure",
			in:   cborMapOf(),
			want: "a0",
		},
		{
			name: "a nil map is an empty map, not null",
			in:   cborMapOf(),
			want: "a0",
		},
		{
			name: "one member",
			in:   cborMapOf(pair{Key: "a", Value: 1}),
			want: "a1616101",
		},
		{
			// Declaration order, not sorted and not Go's map iteration order. "b"
			// deliberately precedes "a" to prove nothing reorders it.
			name: "member order is the caller's",
			in: cborMapOf(
				pair{Key: "b", Value: 1},
				pair{Key: "a", Value: 2},
			),
			want: "a2616201616102",
		},
		{
			name: "an explicit null member",
			in:   cborMapOf(pair{Key: "a", Value: nil}),
			want: "a16161f6",
		},
		{
			name: "nested structures and lists",
			in: cborMapOf(
				pair{Key: "a", Value: 1},
				pair{Key: "b", Value: []any{2, 3}},
			),
			want: "a26161016162820203",
		},

		{
			// The Smithy protocol tests' timestamp vector: tag 1 over a double holding
			// epoch seconds at millisecond resolution.
			name: "timestamp",
			in:   time.UnixMilli(946845296123).UTC(),
			want: "c1fb41cc37db380fbe77",
		},
		{
			name: "the epoch itself",
			in:   time.Unix(0, 0).UTC(),
			want: "c1fb0000000000000000",
		},
		{
			// Sub-millisecond precision is dropped rather than carried: the wire format
			// promises milliseconds, so encoding nanoseconds would produce digits that
			// do not survive a peer's own decoder.
			name: "sub-millisecond precision is truncated",
			in:   time.Unix(1, 123456789).UTC(),
			want: "c1fb3ff1f7ced916872b",
		},
		{
			name: "a timestamp before the epoch",
			in:   time.UnixMilli(-500).UTC(),
			want: "c1fbbfe0000000000000",
		},
		{
			name: "list of timestamps",
			in:   []time.Time{time.Unix(0, 0).UTC(), time.Unix(1, 0).UTC()},
			want: "82c1fb0000000000000000c1fb3ff0000000000000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.CBOREncodeForTest(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, hex.EncodeToString(got))
		})
	}
}

// TestCBOREncode_Refusals covers what the encoder will not encode.
//
// Each refusal is a guard against silently producing something wrong rather than a
// limitation: an unordered map would produce different bytes on the next run, and a
// type the protocol has no encoding for has no correct answer.
func TestCBOREncode_Refusals(t *testing.T) {
	t.Parallel()

	deep := any(0)
	for range 70 {
		deep = []any{deep}
	}

	tests := []struct {
		name    string
		in      any
		wantErr string
	}{
		{
			// The whole point of cborMap. Accepting this and sorting the keys would
			// give stable bytes in an order no model declares, and accepting it at all
			// makes the ordered type optional.
			name:    "an unordered map is refused",
			in:      map[string]any{"a": 1},
			wantErr: "cbor: cannot encode map[string]any; use cborMap",
		},
		{
			name:    "an unsupported type names itself",
			in:      struct{ A int }{A: 1},
			wantErr: "cbor: cannot encode struct { A int }",
		},
		{
			name:    "an unsupported type inside a member names the member",
			in:      cborMapOf(pair{Key: "Threshold", Value: uint8(1)}),
			wantErr: `member "Threshold": cbor: cannot encode uint8`,
		},
		{
			name:    "an unsupported type inside a list names the index",
			in:      []any{1, uint8(2)},
			wantErr: "element 1: cbor: cannot encode uint8",
		},
		{
			name:    "nesting deeper than the limit is refused",
			in:      deep,
			wantErr: "cbor: cannot encode a value nested deeper than 64 levels",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := emulator.CBOREncodeForTest(tc.in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestCBORDecode_WireBytes covers the decoder, and in particular the forms it is
// *required* to accept but never writes.
//
// The liberality is not politeness. Smithy's specification leaves the length form to
// the writer and its normative protocol tests use indefinite length almost everywhere;
// the AWS query-compatible vectors encode a two-entry map with a non-minimal length
// argument; and a modeled double may arrive as float16, float32, float64 or an
// integer. A reader that only accepted this encoder's own output would reject the
// reference client.
func TestCBORDecode_WireBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want any
	}{
		{name: "zero", in: "00", want: int64(0)},
		{name: "twenty-four", in: "1818", want: int64(24)},
		{name: "one trillion", in: "1b000000e8d4a51000", want: int64(1000000000000)},
		{name: "negative one", in: "20", want: int64(-1)},
		{name: "min int64", in: "3b7fffffffffffffff", want: int64(math.MinInt64)},
		{
			// A writer SHOULD use the smallest representation but a reader MUST NOT
			// require it. Smithy's query-compatible vectors are non-minimal.
			name: "a non-minimal integer argument decodes",
			in:   "190001",
			want: int64(1),
		},

		{name: "false", in: "f4", want: false},
		{name: "true", in: "f5", want: true},
		{name: "null", in: "f6", want: nil},
		{
			// A server MUST NOT serialize undefined and MUST decode it as null.
			name: "undefined decodes as null",
			in:   "f7",
			want: nil,
		},

		{name: "double", in: "fb3ff8000000000000", want: 1.5},
		{
			// The Smithy vectors send a modeled double as float32 in one case and
			// float64 in another, so both must widen to the same value.
			name: "a float32 widens to the same double",
			in:   "fa3f800000",
			want: 1.0,
		},
		{
			// Half-precision is never written here and MUST still be read.
			name: "half-precision one",
			in:   "f93c00",
			want: 1.0,
		},
		{
			name: "half-precision subnormal",
			in:   "f90001",
			want: 5.960464477539063e-08,
		},
		{
			name: "half-precision negative zero keeps its sign",
			in:   "f98000",
			want: math.Copysign(0, -1),
		},
		{
			name: "half-precision one third",
			in:   "f93555",
			want: 0.333251953125,
		},
		{name: "half-precision infinity", in: "f97c00", want: math.Inf(1)},
		{name: "half-precision negative infinity", in: "f9fc00", want: math.Inf(-1)},

		{name: "empty string", in: "60", want: ""},
		{name: "IETF", in: "6449455446", want: "IETF"},
		{
			// RFC 8949 §3.2.3: a sequence of definite-length chunks ended by a break.
			name: "an indefinite-length string is joined",
			in:   "7f657374726561646d696e67ff",
			want: "streaming",
		},
		{name: "an empty indefinite-length string", in: "7fff", want: ""},

		{name: "blob", in: "4401020304", want: []byte{1, 2, 3, 4}},
		{name: "an indefinite-length blob is joined", in: "5f42010243030405ff", want: []byte{1, 2, 3, 4, 5}},
		{name: "empty blob", in: "40", want: []byte{}},

		{name: "empty list", in: "80", want: []any{}},
		{
			name: "an indefinite-length empty list",
			in:   "9fff",
			want: []any{},
		},
		{name: "list of integers", in: "83010203", want: []any{int64(1), int64(2), int64(3)}},
		{
			// RFC 8949 Appendix A: definite and indefinite nested in one another.
			name: "mixed definite and indefinite nesting",
			in:   "9f018202039f0405ffff",
			want: []any{int64(1), []any{int64(2), int64(3)}, []any{int64(4), int64(5)}},
		},
		{
			// A @sparse list's null is a value, not an absent member.
			name: "a null element in a sparse list",
			in:   "8201f6",
			want: []any{int64(1), nil},
		},

		{name: "empty map", in: "a0", want: map[string]any{}},
		{
			// The canonical empty structure in Smithy's own vectors, which this encoder
			// writes as a0. Both must decode alike.
			name: "an indefinite-length empty structure",
			in:   "bfff",
			want: map[string]any{},
		},
		{
			// From the AWS query-compatible rpcv2Cbor vectors: a two-entry map whose
			// length is written in two bytes rather than inline.
			name: "a non-minimal map length decodes",
			in:   "b90002616101616202",
			want: map[string]any{"a": int64(1), "b": int64(2)},
		},
		{
			name: "an indefinite-length map",
			in:   "bf616101616202ff",
			want: map[string]any{"a": int64(1), "b": int64(2)},
		},

		{
			name: "timestamp as a double",
			in:   "c1fb41cc37db380fbe77",
			want: time.UnixMilli(946845296123).UTC(),
		},
		{
			// The protocol allows either form for tag 1, and the vectors use both.
			name: "timestamp as an integer",
			in:   "c100",
			want: time.Unix(0, 0).UTC(),
		},
		{
			name: "a negative integer timestamp",
			in:   "c120",
			want: time.Unix(-1, 0).UTC(),
		},
		{
			// Rounded to the resolution the protocol promises, so a double a fraction of
			// a nanosecond below a whole millisecond does not lose one.
			name: "a timestamp double is rounded to milliseconds",
			in:   "c1fb3fbf7ced916872b0",
			want: time.UnixMilli(123).UTC(),
		},
		{
			// A tag substrate does not model decodes to its payload, so an unmodeled
			// member is consumed structurally rather than failing the whole request.
			name: "an unrecognized tag yields its payload",
			in:   "c24101",
			want: []byte{1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.CBORDecodeForTest(mustHex(t, tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCBORDecode_Malformed covers input the decoder must refuse.
//
// These matter more than they look: the body is attacker-controlled, and two of them —
// a declared element count larger than the input, and nesting deeper than the limit —
// are the difference between a 400 and a server that allocates until it dies or
// exhausts its stack.
func TestCBORDecode_Malformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{
			name:    "empty input",
			in:      "",
			wantErr: "unexpected end of input reading an item head",
		},
		{
			// Additional information 28, 29 and 30 are reserved; RFC 8949 §3 makes them
			// not well-formed, so there is no length to guess at.
			name:    "reserved additional information",
			in:      "1c",
			wantErr: "cbor: reserved additional information 28 is not well-formed",
		},
		{
			name:    "a truncated length argument",
			in:      "19ff",
			wantErr: "declares a 2-byte argument with only 1 byte(s) left",
		},
		{
			name:    "a string longer than the input",
			in:      "64494554",
			wantErr: "cbor: string of 4 byte(s) runs past the end of a 4-byte input",
		},
		{
			// Nine bytes asking for 2⁶⁴ elements. Sizing an allocation from an unchecked
			// declared count is the classic CBOR denial of service.
			name:    "a list count larger than the input",
			in:      "9bffffffffffffffff",
			wantErr: "cbor: array declares 18446744073709551615 element(s) with only 0 byte(s) left",
		},
		{
			name:    "a map count larger than the input",
			in:      "bbffffffffffffffff",
			wantErr: "cbor: map declares 18446744073709551615 element(s) with only 0 byte(s) left",
		},
		{
			name:    "an unterminated indefinite list",
			in:      "9f01",
			wantErr: "unexpected end of input looking for a break",
		},
		{
			name:    "a break with nothing open",
			in:      "ff",
			wantErr: "cbor: unexpected break",
		},
		{
			// Every shape that encodes as a CBOR map has string keys, and the one thing
			// that could carry another key type — a document — is not supported by this
			// protocol.
			name:    "a non-text map key",
			in:      "a10101",
			wantErr: "cbor: map key decoded as int64, not a text string",
		},
		{
			name:    "an integer cannot be indefinite",
			in:      "1f",
			wantErr: "cbor: an integer cannot have an indefinite length",
		},
		{
			name:    "a tag cannot be indefinite",
			in:      "df",
			wantErr: "cbor: a tag cannot have an indefinite length",
		},
		{
			name:    "an unsigned integer too large for an int64",
			in:      "1bffffffffffffffff",
			wantErr: "does not fit in an int64",
		},
		{
			name:    "an unassigned simple value",
			in:      "f0",
			wantErr: "cbor: unassigned simple value 16",
		},
		{
			name:    "an indefinite string chunk of the wrong type",
			in:      "7f4101ff",
			wantErr: "may only contain definite-length chunks of its own type",
		},
		{
			name:    "a timestamp payload that is not a number",
			in:      "c16161",
			wantErr: "cbor: timestamp payload decoded as string, not a number",
		},
		{
			name:    "a NaN timestamp",
			in:      "c1fb7ff8000000000000",
			wantErr: "cbor: timestamp payload is NaN",
		},
		{
			// One item per body. Trailing bytes mean the peer and this decoder disagree
			// about the message, which is worth saying rather than ignoring.
			name:    "trailing bytes after the top-level item",
			in:      "0101",
			wantErr: "cbor: 1 trailing byte(s) after the top-level item",
		},
		{
			name:    "a member's error names the member",
			in:      "a161611c",
			wantErr: `member "a": cbor: reserved additional information 28`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := emulator.CBORDecodeForTest(mustHex(t, tc.in))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestCBOR_AgainstSmithyGo pins substrate's codec against the reference
// implementation's actual bytes.
//
// The hexadecimal below is what github.com/aws/smithy-go v1.28.1's
// encoding/cbor.Encode produced for each document — the encoder aws-sdk-go-v2 uses,
// which is the client that could not talk to CloudWatch at all before #785. Recording
// it here is the cross-validation the byte tables cannot do on their own: those assert
// that substrate agrees with a specification as read, and this asserts that it agrees
// with the peer it has to interoperate with. Every one of substrate's own encodings in
// the table above was fed back through cbor.Decode and accepted.
//
// Two of these are forms substrate never writes and must read: a modeled double as a
// 0xfa float32, and 0xf7 for undefined. The timestamp is the interesting one — it is
// byte-identical, so the two implementations agree on tag 1 over a float64 of epoch
// seconds without either having been written against the other.
func TestCBOR_AgainstSmithyGo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want any
	}{
		{name: "cbor.Map{}", in: "a0", want: map[string]any{}},
		{
			name: "cbor.Map with two members",
			in:   "a2616101616202",
			want: map[string]any{"a": int64(1), "b": int64(2)},
		},
		{
			name: "cbor.Tag{ID: 1, Value: Float64(946845296.123)}",
			in:   "c1fb41cc37db380fbe77",
			want: time.UnixMilli(946845296123).UTC(),
		},
		{name: "cbor.Float32(1.0)", in: "fa3f800000", want: 1.0},
		{
			name: "nested cbor.List",
			in:   "8201820203",
			want: []any{int64(1), []any{int64(2), int64(3)}},
		},
		{name: "cbor.NegInt(1)", in: "20", want: int64(-1)},
		{name: "cbor.Nil{}", in: "f6", want: nil},
		{name: "cbor.Undefined{}", in: "f7", want: nil},
		{name: "cbor.Slice{1, 2, 3, 4}", in: "4401020304", want: []byte{1, 2, 3, 4}},
		{name: `cbor.String("IETF")`, in: "6449455446", want: "IETF"},
		{name: "cbor.Bool(true)", in: "f5", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.CBORDecodeForTest(mustHex(t, tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCBORDecode_NestingLimit pins that deep nesting is refused rather than recursed
// into, which is what keeps a few bytes of `9f` from exhausting the goroutine stack.
func TestCBORDecode_NestingLimit(t *testing.T) {
	t.Parallel()

	deep := mustHex(t, strings.Repeat("9f", 70)+"00"+strings.Repeat("ff", 70))
	_, err := emulator.CBORDecodeForTest(deep)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cbor: input nests deeper than 64 levels")

	// The limit is generous relative to any AWS shape, so a document nested well past
	// CloudWatch's deepest still decodes.
	shallow := mustHex(t, strings.Repeat("9f", 8)+"00"+strings.Repeat("ff", 8))
	_, err = emulator.CBORDecodeForTest(shallow)
	require.NoError(t, err)
}

// TestCBOR_RoundTrip is the cheap extra on top of the byte assertions: a document
// carrying every type substrate encodes survives a trip through both halves.
//
// The convenience slices deliberately do not survive as themselves — []string encodes
// as a CBOR array and so decodes as []any — because CBOR carries no Go type. That is
// asserted rather than worked around, since the input layer reading these values needs
// to know it.
func TestCBOR_RoundTrip(t *testing.T) {
	t.Parallel()

	stamp := time.UnixMilli(1735689600123).UTC()
	doc := cborMapOf(
		pair{Key: "Text", Value: "value"},
		pair{Key: "Blob", Value: []byte{0xde, 0xad}},
		pair{Key: "Count", Value: 42},
		pair{Key: "Negative", Value: -7},
		pair{Key: "Threshold", Value: 99.5},
		pair{Key: "Flag", Value: true},
		pair{Key: "Timestamp", Value: stamp},
		pair{Key: "Values", Value: []float64{1.5, 2.5}},
		pair{Key: "Names", Value: []string{"a", "b"}},
		pair{Key: "Timestamps", Value: []time.Time{stamp}},
		pair{Key: "Absent", Value: nil},
		pair{Key: "Nested", Value: cborMapOf(pair{Key: "Inner", Value: 1})},
	)

	encoded, err := emulator.CBOREncodeForTest(doc)
	require.NoError(t, err)
	decoded, err := emulator.CBORDecodeForTest(encoded)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"Text":       "value",
		"Blob":       []byte{0xde, 0xad},
		"Count":      int64(42),
		"Negative":   int64(-7),
		"Threshold":  99.5,
		"Flag":       true,
		"Timestamp":  stamp,
		"Values":     []any{1.5, 2.5},
		"Names":      []any{"a", "b"},
		"Timestamps": []any{stamp},
		"Absent":     nil,
		"Nested":     map[string]any{"Inner": int64(1)},
	}, decoded)

	// Encoding the same document twice must produce the same bytes — the property the
	// whole design exists for, and the one a round trip cannot see.
	again, err := emulator.CBOREncodeForTest(doc)
	require.NoError(t, err)
	assert.Equal(t, encoded, again)
}
