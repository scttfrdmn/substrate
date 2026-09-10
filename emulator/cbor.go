package emulator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// A minimal CBOR (RFC 8949) codec, scoped to what Smithy's RPC v2 CBOR protocol
// actually puts on the wire (#785).
//
// It exists because substrate had no CBOR at all. The only CBOR in the repo was a
// hardcoded `[]byte{0xa0}` in CloudWatch's GetMetricData; every other operation
// answered XML to a client that had asked for CBOR, so aws-sdk-go-v2 reported
// "deserialization failed, expected map for struct, got major type 1" — the '<' of
// "<GetMetricDataResponse" being read as a CBOR head.
//
// It is written here rather than taken from github.com/aws/smithy-go or a
// general-purpose CBOR library for two reasons. The root module has no AWS SDK
// dependency and adding one to emit a response format would be a large surface for a
// small need. More importantly, substrate's premise is that the same inputs produce
// the same bytes: a library free to choose indefinite-length encoding, map ordering
// or float width per call would make a recorded response non-reproducible, and there
// is no configuration that promises otherwise. The encoder below has exactly one
// output for any input.
//
// # What the writer chooses, and why
//
// The protocol specification is silent on definite versus indefinite length, and the
// normative protocol tests use indefinite almost everywhere (`bf … ff` for a
// structure, `9f … ff` for a list), so both forms are conformant and a reader may
// assume neither. This writer emits definite-length with the *minimal* length
// argument, which is the only choice that is byte-stable.
//
// That is also what aws-sdk-go-v2 puts on the wire: smithy-go v1.28.1's
// encoding/cbor.Encode has no indefinite-length path, and encoding an equivalent
// document with it produces these bytes exactly — including `c1fb41cc37db380fbe77` for
// the protocol tests' timestamp vector. The indefinite forms come from the test
// vectors and from other SDKs' streaming writers, which is why the reader below must
// take them and the writer need not produce them.
//
// The same reasoning fixes the rest of the writer: integers use the smallest
// representation, doubles are always 0xfb (never float16, which the spec discourages
// writing), timestamps are always tag 1 over a float64, and member order comes from
// [cborMap], which is a slice rather than a Go map so that ordering is the caller's
// explicit choice and never Go's randomized iteration.
//
// # What the reader must accept, and why
//
// Liberality here is not politeness, it is required by the specification and pinned
// by the protocol tests, whose vectors use forms this writer never emits. The reader
// accepts indefinite-length maps, arrays and strings; non-minimal length arguments
// (the AWS query-compatible vectors encode a two-entry map as `b9 00 02`, and
// smithy-go writes a modeled double as `fa 3f800000` where this writer would write
// `fb`); a double arriving as float16, float32, float64 or an
// integer; a timestamp's tag 1 payload as either an integer or a float; and 0xf7
// (undefined), which a server MUST decode as null. An unrecognized tag decodes to its
// payload, so a member substrate does not model is still skipped structurally rather
// than failing the whole request.

// CBOR major types, the high three bits of an initial byte (RFC 8949 §3).
const (
	cborMajorUint   = 0
	cborMajorNegInt = 1
	cborMajorBytes  = 2
	cborMajorText   = 3
	cborMajorArray  = 4
	cborMajorMap    = 5
	cborMajorTag    = 6
	cborMajorSimple = 7
)

// CBOR additional-information values that carry a length argument in following bytes,
// or that mark an indefinite-length item. Values 0–23 are the argument itself;
// 28, 29 and 30 are reserved and are not well-formed.
const (
	cborInfoUint8      = 24
	cborInfoUint16     = 25
	cborInfoUint32     = 26
	cborInfoUint64     = 27
	cborInfoIndefinite = 31
)

// Complete initial bytes for the major-7 items this codec uses.
const (
	cborFalse     = 0xf4
	cborTrue      = 0xf5
	cborNullByte  = 0xf6
	cborUndefined = 0xf7
	cborBreak     = 0xff
)

// cborTagEpoch is tag 1, "epoch-based date/time" (RFC 8949 §3.4.2), which is how
// Smithy RPC v2 CBOR encodes every timestamp: seconds since the Unix epoch, at
// millisecond resolution. A @timestampFormat trait MUST NOT be respected.
const cborTagEpoch = 1

// cborMaxDepth bounds nesting on both encode and decode.
//
// It is a guard against a hostile body rather than a modeling limit: recursive
// decoding of a few bytes of deeply nested heads (`9f 9f 9f …`) would otherwise
// exhaust the goroutine stack and take the server down. No AWS shape comes close —
// CloudWatch's deepest is four levels — so a request this refuses was not going to be
// served anyway.
const cborMaxDepth = 64

// cborMap is a CBOR map in a fixed member order: a Smithy structure, union or map
// shape, as an ordered slice rather than a Go map.
//
// The type exists so that encoded output is byte-stable. Ranging a map[string]any
// would order members by Go's randomized map iteration, so the same response would
// encode differently between two runs and no recorded event could be replayed
// byte-for-byte. Ordering is therefore the caller's explicit choice, which for a
// structure means the model's member order.
type cborMap []cborEntry

// cborEntry is one member of a [cborMap].
type cborEntry struct {
	// Key is the member name, encoded as a CBOR text string.
	Key string

	// Value is the member's value, encoded per [cborEncode]'s type table. An absent
	// optional member is omitted from the map entirely rather than present as nil;
	// nil is reserved for a null that carries meaning, which is a @sparse
	// collection's element.
	Value any
}

// cborEncode encodes v as CBOR.
//
// The accepted types are exactly the ones Smithy's simple shapes map onto: nil (null),
// bool, string (major 3), []byte (major 2), int/int32/int64 (major 0 or 1),
// float32/float64 (0xfb), [time.Time] (tag 1), [cborMap] (major 5), and []any,
// []string, []float64 or []time.Time (major 4). A document shape has no encoding —
// RPC v2 CBOR does not support documents — and any other Go type is refused rather
// than guessed at.
func cborEncode(v any) ([]byte, error) {
	return cborAppend(make([]byte, 0, 64), v, 0)
}

// cborAppend appends v's encoding to dst, at the given nesting depth.
func cborAppend(dst []byte, v any, depth int) ([]byte, error) {
	if depth > cborMaxDepth {
		return nil, fmt.Errorf("cbor: cannot encode a value nested deeper than %d levels", cborMaxDepth)
	}
	switch t := v.(type) {
	case nil:
		return append(dst, cborNullByte), nil
	case bool:
		if t {
			return append(dst, cborTrue), nil
		}
		return append(dst, cborFalse), nil
	case string:
		dst = cborAppendHead(dst, cborMajorText, uint64(len(t)))
		return append(dst, t...), nil
	case []byte:
		dst = cborAppendHead(dst, cborMajorBytes, uint64(len(t)))
		return append(dst, t...), nil
	case int:
		return cborAppendInt(dst, int64(t)), nil
	case int32:
		return cborAppendInt(dst, int64(t)), nil
	case int64:
		return cborAppendInt(dst, t), nil
	case float32:
		return cborAppendFloat(dst, float64(t)), nil
	case float64:
		return cborAppendFloat(dst, t), nil
	case time.Time:
		return cborAppendTime(dst, t), nil
	case cborMap:
		dst = cborAppendHead(dst, cborMajorMap, uint64(len(t)))
		for _, entry := range t {
			dst = cborAppendHead(dst, cborMajorText, uint64(len(entry.Key)))
			dst = append(dst, entry.Key...)
			var err error
			if dst, err = cborAppend(dst, entry.Value, depth+1); err != nil {
				return nil, fmt.Errorf("member %q: %w", entry.Key, err)
			}
		}
		return dst, nil
	case []any:
		dst = cborAppendHead(dst, cborMajorArray, uint64(len(t)))
		for i, elem := range t {
			var err error
			if dst, err = cborAppend(dst, elem, depth+1); err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
		}
		return dst, nil
	case []string:
		dst = cborAppendHead(dst, cborMajorArray, uint64(len(t)))
		for _, elem := range t {
			dst = cborAppendHead(dst, cborMajorText, uint64(len(elem)))
			dst = append(dst, elem...)
		}
		return dst, nil
	case []float64:
		dst = cborAppendHead(dst, cborMajorArray, uint64(len(t)))
		for _, elem := range t {
			dst = cborAppendFloat(dst, elem)
		}
		return dst, nil
	case []time.Time:
		dst = cborAppendHead(dst, cborMajorArray, uint64(len(t)))
		for _, elem := range t {
			dst = cborAppendTime(dst, elem)
		}
		return dst, nil
	case map[string]any:
		// Refused rather than sorted. Sorting would produce stable bytes but in an
		// order no model declares, and accepting it at all would make the ordered type
		// optional — at which point a caller reaching for the convenient one
		// reintroduces exactly the nondeterminism cborMap exists to prevent.
		return nil, errors.New("cbor: cannot encode map[string]any; use cborMap, " +
			"whose member order is explicit and therefore byte-stable")
	default:
		return nil, fmt.Errorf("cbor: cannot encode %T", v)
	}
}

// cborAppendHead appends an initial byte for major with arg as its argument, using the
// smallest of the five representations that holds arg.
func cborAppendHead(dst []byte, major byte, arg uint64) []byte {
	switch {
	case arg < cborInfoUint8:
		return append(dst, major<<5|byte(arg))
	case arg <= math.MaxUint8:
		return append(dst, major<<5|cborInfoUint8, byte(arg))
	case arg <= math.MaxUint16:
		dst = append(dst, major<<5|cborInfoUint16)
		return binary.BigEndian.AppendUint16(dst, uint16(arg))
	case arg <= math.MaxUint32:
		dst = append(dst, major<<5|cborInfoUint32)
		return binary.BigEndian.AppendUint32(dst, uint32(arg))
	default:
		dst = append(dst, major<<5|cborInfoUint64)
		return binary.BigEndian.AppendUint64(dst, arg)
	}
}

// cborAppendInt appends n as major type 0 or 1.
//
// A negative value is encoded as major 1 with the argument -1-n, computed as
// -(n+1) so that math.MinInt64 does not overflow on the way.
func cborAppendInt(dst []byte, n int64) []byte {
	if n < 0 {
		return cborAppendHead(dst, cborMajorNegInt, uint64(-(n + 1)))
	}
	return cborAppendHead(dst, cborMajorUint, uint64(n))
}

// cborAppendFloat appends f as a 0xfb double.
//
// Always double, never a narrower form: the specification says a writer SHOULD NOT
// emit half-precision, and shortening to float32 only when the value happens to be
// exactly representable would make the width depend on the data, so the same modeled
// double would encode two different ways.
func cborAppendFloat(dst []byte, f float64) []byte {
	dst = append(dst, cborMajorSimple<<5|cborInfoUint64)
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(f))
}

// cborAppendTime appends t as tag 1 over a double holding epoch seconds.
//
// Resolution is milliseconds, which is what the protocol specifies, so the value is
// built from [time.Time.UnixMilli] rather than from the nanosecond clock: a
// nanosecond-precision float would carry digits the wire format does not promise, and
// would differ between a value that made a round trip and one that did not.
func cborAppendTime(dst []byte, t time.Time) []byte {
	dst = cborAppendHead(dst, cborMajorTag, cborTagEpoch)
	return cborAppendFloat(dst, float64(t.UnixMilli())/1e3)
}

// cborDecode decodes one CBOR item from data, which must hold exactly one.
//
// The Go types returned are int64 (majors 0 and 1), []byte (major 2), string
// (major 3), []any (major 4), map[string]any (major 5), [time.Time] (tag 1), float64
// (0xf9, 0xfa and 0xfb alike), bool, and nil for null or undefined.
//
// A modeled double may therefore arrive as an int64, since the protocol lets a peer
// send one whenever the value is lossless. Widening is left to the caller rather than
// done here, because this function does not know the model: the same int64 is the
// correct answer for a modeled integer and needs converting for a modeled double, and
// only the shape says which.
func cborDecode(data []byte) (any, error) {
	r := &cborReader{data: data}
	v, err := r.value(0)
	if err != nil {
		return nil, err
	}
	if r.pos != len(r.data) {
		return nil, fmt.Errorf("cbor: %d trailing byte(s) after the top-level item",
			len(r.data)-r.pos)
	}
	return v, nil
}

// cborReader is a position in a CBOR byte slice.
type cborReader struct {
	// data is the whole input, which is never modified.
	data []byte

	// pos is the offset of the next byte to read.
	pos int
}

// head reads an item's initial byte and its argument.
//
// info is the initial byte's low five bits, returned alongside arg because the two are
// not interchangeable: for major 7 the info selects between a simple value and a
// float width, and for every major the value cborInfoIndefinite means the item has no
// length. For a float, arg holds the raw IEEE bits.
func (r *cborReader) head() (major, info byte, arg uint64, err error) {
	if r.pos >= len(r.data) {
		return 0, 0, 0, errors.New("cbor: unexpected end of input reading an item head")
	}
	b := r.data[r.pos]
	r.pos++
	major, info = b>>5, b&0x1f

	switch {
	case info < cborInfoUint8:
		return major, info, uint64(info), nil
	case info == cborInfoIndefinite:
		return major, info, 0, nil
	case info > cborInfoUint64:
		// 28, 29 and 30 are reserved. RFC 8949 §3 makes them not well-formed, so
		// guessing at a length here would be inventing a message.
		return 0, 0, 0, fmt.Errorf("cbor: reserved additional information %d is not well-formed", info)
	}

	n := 1 << (info - cborInfoUint8)
	if len(r.data)-r.pos < n {
		return 0, 0, 0, fmt.Errorf("cbor: item head declares a %d-byte argument with only %d byte(s) left",
			n, len(r.data)-r.pos)
	}
	switch n {
	case 1:
		arg = uint64(r.data[r.pos])
	case 2:
		arg = uint64(binary.BigEndian.Uint16(r.data[r.pos:]))
	case 4:
		arg = uint64(binary.BigEndian.Uint32(r.data[r.pos:]))
	default:
		arg = binary.BigEndian.Uint64(r.data[r.pos:])
	}
	r.pos += n
	return major, info, arg, nil
}

// value decodes one item at the given nesting depth.
func (r *cborReader) value(depth int) (any, error) {
	if depth > cborMaxDepth {
		return nil, fmt.Errorf("cbor: input nests deeper than %d levels", cborMaxDepth)
	}
	major, info, arg, err := r.head()
	if err != nil {
		return nil, err
	}
	indefinite := info == cborInfoIndefinite

	switch major {
	case cborMajorUint:
		if indefinite {
			return nil, errors.New("cbor: an integer cannot have an indefinite length")
		}
		if arg > math.MaxInt64 {
			return nil, fmt.Errorf("cbor: unsigned integer %d does not fit in an int64", arg)
		}
		return int64(arg), nil

	case cborMajorNegInt:
		if indefinite {
			return nil, errors.New("cbor: an integer cannot have an indefinite length")
		}
		if arg > math.MaxInt64 {
			return nil, fmt.Errorf("cbor: negative integer -%d-1 does not fit in an int64", arg)
		}
		return -1 - int64(arg), nil

	case cborMajorBytes:
		b, err := r.stringBytes(info, arg, cborMajorBytes)
		if err != nil {
			return nil, err
		}
		// Copied rather than aliased: the caller owns the request buffer and may reuse
		// it, and a decoded blob outliving the request would then change under a
		// plugin that stored it.
		out := make([]byte, len(b))
		copy(out, b)
		return out, nil

	case cborMajorText:
		b, err := r.stringBytes(info, arg, cborMajorText)
		if err != nil {
			return nil, err
		}
		return string(b), nil

	case cborMajorArray:
		return r.array(indefinite, arg, depth)

	case cborMajorMap:
		return r.mapping(indefinite, arg, depth)

	case cborMajorTag:
		if indefinite {
			return nil, errors.New("cbor: a tag cannot have an indefinite length")
		}
		return r.tagged(arg, depth)

	default:
		return r.simple(info, arg)
	}
}

// stringBytes reads a byte or text string's content, joining the chunks of an
// indefinite-length one. The returned slice may alias the input.
func (r *cborReader) stringBytes(info byte, arg uint64, major byte) ([]byte, error) {
	if info != cborInfoIndefinite {
		return r.take(arg)
	}
	// An indefinite-length string is a sequence of definite-length strings of the same
	// major type, terminated by a break (RFC 8949 §3.2.3).
	var out []byte
	for {
		atBreak, err := r.atBreak()
		if err != nil {
			return nil, err
		}
		if atBreak {
			return out, nil
		}
		chunkMajor, chunkInfo, chunkLen, err := r.head()
		if err != nil {
			return nil, err
		}
		if chunkMajor != major || chunkInfo == cborInfoIndefinite {
			return nil, errors.New("cbor: an indefinite-length string may only contain " +
				"definite-length chunks of its own type")
		}
		chunk, err := r.take(chunkLen)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
	}
}

// array decodes a major-4 item, of either length form.
func (r *cborReader) array(indefinite bool, arg uint64, depth int) ([]any, error) {
	if indefinite {
		out := []any{}
		for {
			atBreak, err := r.atBreak()
			if err != nil {
				return nil, err
			}
			if atBreak {
				return out, nil
			}
			elem, err := r.value(depth + 1)
			if err != nil {
				return nil, err
			}
			out = append(out, elem)
		}
	}
	// Every element occupies at least one byte, so a count larger than the bytes that
	// remain is malformed. The check is what stops a five-byte `9a ffffffff` from
	// asking for a four-billion-element allocation.
	if err := r.checkCount(arg, 1, "array"); err != nil {
		return nil, err
	}
	out := make([]any, 0, arg)
	for range arg {
		elem, err := r.value(depth + 1)
		if err != nil {
			return nil, err
		}
		out = append(out, elem)
	}
	return out, nil
}

// mapping decodes a major-5 item, of either length form.
//
// Keys must be text strings. Every Smithy shape that encodes as a map — structure,
// union and map — has string keys, and a document shape, the only thing that could
// carry another key type, is not supported by this protocol at all.
func (r *cborReader) mapping(indefinite bool, arg uint64, depth int) (map[string]any, error) {
	out := map[string]any{}
	readPair := func() error {
		rawKey, err := r.value(depth + 1)
		if err != nil {
			return err
		}
		key, ok := rawKey.(string)
		if !ok {
			return fmt.Errorf("cbor: map key decoded as %T, not a text string", rawKey)
		}
		value, err := r.value(depth + 1)
		if err != nil {
			return fmt.Errorf("member %q: %w", key, err)
		}
		out[key] = value
		return nil
	}

	if indefinite {
		for {
			atBreak, err := r.atBreak()
			if err != nil {
				return nil, err
			}
			if atBreak {
				return out, nil
			}
			if err := readPair(); err != nil {
				return nil, err
			}
		}
	}
	// Two bytes minimum per pair: a one-byte key head and a one-byte value.
	if err := r.checkCount(arg, 2, "map"); err != nil {
		return nil, err
	}
	for range arg {
		if err := readPair(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// tagged decodes a major-6 item's payload and applies the tag.
//
// Tag 1 becomes a [time.Time]. Any other tag decodes to its payload with the tag
// discarded, which is deliberate: no CloudWatch shape uses one, and returning the
// payload means an unmodeled member is still consumed structurally rather than
// failing the request that carried it.
func (r *cborReader) tagged(tag uint64, depth int) (any, error) {
	payload, err := r.value(depth + 1)
	if err != nil {
		return nil, err
	}
	if tag != cborTagEpoch {
		return payload, nil
	}
	switch t := payload.(type) {
	case int64:
		return time.Unix(t, 0).UTC(), nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil, fmt.Errorf("cbor: timestamp payload is %v", t)
		}
		// Rounded to milliseconds, the resolution the protocol specifies, so a value
		// whose float representation lands a fraction of a nanosecond off a whole
		// millisecond does not decode to 1970-01-01T00:00:00.122999999Z.
		return time.UnixMilli(int64(math.Round(t * 1e3))).UTC(), nil
	default:
		return nil, fmt.Errorf("cbor: timestamp payload decoded as %T, not a number", payload)
	}
}

// simple decodes a major-7 item: a boolean, null, undefined, or a float.
func (r *cborReader) simple(info byte, arg uint64) (any, error) {
	switch info {
	case cborInfoUint16:
		return cborFromFloat16(uint16(arg)), nil
	case cborInfoUint32:
		return float64(math.Float32frombits(uint32(arg))), nil
	case cborInfoUint64:
		return math.Float64frombits(arg), nil
	case cborInfoIndefinite:
		// 0xff is a break, which only a collection reader may consume. Reaching it here
		// means a break with no indefinite-length item open.
		return nil, errors.New("cbor: unexpected break")
	}
	switch cborMajorSimple<<5 | info {
	case cborFalse:
		return false, nil
	case cborTrue:
		return true, nil
	case cborNullByte:
		return nil, nil
	case cborUndefined:
		// A server MUST NOT serialize undefined and MUST decode it as null.
		return nil, nil
	}
	return nil, fmt.Errorf("cbor: unassigned simple value %d", arg)
}

// cborFromFloat16 converts IEEE 754 binary16 bits to a float64.
//
// The conversion is written out because Go has no float16: math.Float32frombits is the
// narrowest the standard library offers. Half-precision is decoded even though this
// codec never writes it, because the specification requires a reader to handle it,
// including the infinities and NaN.
func cborFromFloat16(bits uint16) float64 {
	sign := uint32(bits>>15) << 31
	exponent := (bits >> 10) & 0x1f
	mantissa := uint32(bits & 0x03ff)

	switch exponent {
	case 0:
		if mantissa == 0 {
			// Zero, whose sign is bit 63 of the double.
			return math.Float64frombits(uint64(sign) << 32)
		}
		// Subnormal: the value is mantissa × 2⁻²⁴, exact in a float64.
		f := float64(mantissa) / (1 << 24)
		if sign != 0 {
			return -f
		}
		return f
	case 0x1f:
		if mantissa != 0 {
			return math.NaN()
		}
		if sign != 0 {
			return math.Inf(-1)
		}
		return math.Inf(1)
	}
	// Normal: rebias the exponent from 15 to 127 and shift the mantissa into a
	// float32, which represents every binary16 value exactly.
	return float64(math.Float32frombits(sign | (uint32(exponent)-15+127)<<23 | mantissa<<13))
}

// atBreak reports whether the next byte is a break, consuming it when it is.
func (r *cborReader) atBreak() (bool, error) {
	if r.pos >= len(r.data) {
		return false, errors.New("cbor: unexpected end of input looking for a break")
	}
	if r.data[r.pos] != cborBreak {
		return false, nil
	}
	r.pos++
	return true, nil
}

// take consumes n bytes, returning a slice that aliases the input.
func (r *cborReader) take(n uint64) ([]byte, error) {
	if n > uint64(len(r.data)-r.pos) {
		return nil, fmt.Errorf("cbor: string of %d byte(s) runs past the end of a %d-byte input",
			n, len(r.data))
	}
	b := r.data[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return b, nil
}

// checkCount refuses a declared element count that cannot fit in the bytes that
// remain, where each element needs at least perElement of them.
//
// A declared count is attacker-controlled and is used to size an allocation, so it is
// bounded by the input rather than trusted: `9b ffffffffffffffff` is nine bytes long
// and would otherwise ask for a slice of 2⁶⁴ elements.
func (r *cborReader) checkCount(count uint64, perElement int, kind string) error {
	remaining := uint64(len(r.data) - r.pos)
	if count > remaining/uint64(perElement) {
		return fmt.Errorf("cbor: %s declares %d element(s) with only %d byte(s) left",
			kind, count, remaining)
	}
	return nil
}
