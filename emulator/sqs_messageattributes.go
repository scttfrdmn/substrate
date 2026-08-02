package emulator

import (
	"crypto/md5" // #nosec G501 -- AWS specifies MD5 for this digest; it is not a security control.
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// SQS transport types for the message-attribute MD5 digest, from the developer
// guide's "Calculating the MD5 digest" procedure: a single byte distinguishing a
// string-shaped value from a binary one.
const (
	sqsAttrTransportString = 1
	sqsAttrTransportBinary = 2
)

// sqsMD5OfMessageAttributes returns the MD5OfMessageAttributes digest AWS computes
// for a message's user-defined attributes, or "" when there are none.
//
// The algorithm is the one published in the SQS developer guide under "Calculating
// the MD5 message digest for message attributes": sort the attributes by name, then
// for each one append the name's UTF-8 length as a 4-byte big-endian integer followed
// by the name, the data type's length and the data type, a single transport-type byte
// (1 for String and Number, 2 for Binary), and the value's length followed by the
// value bytes. MD5 the result.
//
// Two details are load-bearing, and a reimplementation gets both wrong by default:
//
//   - A binary value is hashed **raw**, not base64-encoded. This is the same
//     raw-versus-encoded distinction [sqsMessageSize] already makes for measurement,
//     and here there is a hash to prove which one AWS means: vector 3 in
//     sqs_messageattributes_test.go digests to 049075255ebc53fb95f7f9f3cedf3c50 from
//     raw bytes and 5ff413c9dc7bd18abea88ca05643f902 from base64.
//   - The full data type is hashed, custom suffix included, so
//     "Number.java.lang.Long" is 21 bytes rather than the 6 of its "Number" base.
//     The guide states that a custom type is appended to the base type with a period,
//     and vector 2 does not reproduce without it.
//
// Sorting is by byte order rather than by any locale collation — the guide's wording
// is "sorted by name", and Go's [sort.Strings] is byte order, which is what makes
// the captured digests reproduce.
//
// The empty case returns "" rather than the MD5 of zero bytes, because AWS omits
// MD5OfMessageAttributes from a response for a message with no attributes rather than
// reporting a digest of nothing. Callers use "" as the signal to leave the field out.
//
// MD5 is not a security control here: it is a wire-format value AWS specifies, and
// substrate must produce the same bytes real SQS does in order for a consumer's
// integrity check to pass.
func sqsMD5OfMessageAttributes(attrs map[string]SQSMessageAttribute) string {
	if len(attrs) == 0 {
		return ""
	}

	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	h := md5.New() // #nosec G401 -- see the doc comment: AWS specifies MD5 for this digest.
	for _, name := range names {
		attr := attrs[name]
		sqsAttrWriteField(h, []byte(name))
		sqsAttrWriteField(h, []byte(attr.DataType))
		// A Binary attribute is identified by its declared data type, not by which
		// value field happens to be populated. Keying off BinaryValue instead would
		// hash a Binary attribute with an empty value as a string and silently
		// produce a digest real SQS never emits.
		if sqsAttrIsBinary(attr.DataType) {
			_, _ = h.Write([]byte{sqsAttrTransportBinary})
			sqsAttrWriteField(h, attr.BinaryValue)
		} else {
			_, _ = h.Write([]byte{sqsAttrTransportString})
			sqsAttrWriteField(h, []byte(attr.StringValue))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sqsAttrIsBinary reports whether a data type is the Binary family.
//
// The match is on the base type before any custom suffix, since "Binary.gzip" is a
// documented custom binary type and transports as binary.
func sqsAttrIsBinary(dataType string) bool {
	base, _, _ := strings.Cut(dataType, ".")
	return base == "Binary"
}

// sqsAttrWriteField appends one length-prefixed field to the digest input: a 4-byte
// big-endian length followed by the bytes themselves.
//
// [hash.Hash] never returns an error from Write, which is why the results are
// discarded here rather than plumbed out: its contract states that Write, via the
// embedded io.Writer interface, adds more data to the running hash and never returns
// an error.
func sqsAttrWriteField(h interface{ Write([]byte) (int, error) }, b []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(b)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(b)
}

// sqsRequestedAttributeNames returns the MessageAttributeName selectors a
// ReceiveMessage request carried.
//
// The wire forms differ by protocol and both are flattened lists: the query protocol
// sends MessageAttributeName.N numbered from 1, and the JSON protocol sends a
// MessageAttributeNames array. The model member is MessageAttributeNames
// (AttributeNameList), which is why the query name is singular and the JSON one
// plural.
func sqsRequestedAttributeNames(req *AWSRequest) []string {
	if sqsIsJSONProtocol(req) {
		var input struct {
			MessageAttributeNames []string `json:"MessageAttributeNames"`
		}
		// A body that does not parse yields no selectors, which is the same outcome
		// as a request that named none: attributes are withheld rather than
		// returned by accident.
		_ = json.Unmarshal(req.Body, &input)
		return input.MessageAttributeNames
	}
	var names []string
	for i := 1; ; i++ {
		name, ok := req.Params[fmt.Sprintf("MessageAttributeName.%d", i)]
		if !ok {
			break
		}
		names = append(names, name)
	}
	return names
}

// sqsSelectMessageAttributes returns the subset of a message's attributes a
// ReceiveMessage request asked for, or nil when it asked for none.
//
// Attributes are returned **only when requested**, which is the part of this that is
// easy to get wrong in the permissive direction. The ReceiveMessage reference is
// explicit: "MessageAttributeNames — The name of the message attribute" and
// attributes are not returned unless named. A consumer whose test passes because
// substrate volunteered an attribute their production caller never requests has a
// test that will not survive contact with real SQS.
//
// The documented selectors:
//
//   - "All" or ".*" — every attribute. The reference gives both spellings.
//   - "Name" — that attribute, if the message has it.
//   - "Prefix.*" — every attribute whose name starts with "Prefix.". The reference
//     documents this form: "You can also use all message attributes starting with a
//     prefix, for example bar.*".
//
// A named attribute the message does not carry is simply absent from the result
// rather than an error, matching AWS: the selector describes what to return, not an
// assertion that it exists.
func sqsSelectMessageAttributes(attrs map[string]SQSMessageAttribute, requested []string) map[string]SQSMessageAttribute {
	if len(attrs) == 0 || len(requested) == 0 {
		return nil
	}

	selected := make(map[string]SQSMessageAttribute)
	for _, want := range requested {
		if want == "All" || want == ".*" {
			for name, attr := range attrs {
				selected[name] = attr
			}
			continue
		}
		if prefix, ok := strings.CutSuffix(want, ".*"); ok {
			for name, attr := range attrs {
				if strings.HasPrefix(name, prefix+".") {
					selected[name] = attr
				}
			}
			continue
		}
		if attr, ok := attrs[want]; ok {
			selected[want] = attr
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

// sqsMessageAttributeXML is one attribute in a ReceiveMessage XML response.
//
// The element name is MessageAttribute rather than the model's MessageAttributes
// member name, because the shape carries locationName "MessageAttribute" and is
// flattened — so real SDKs parse a repeated MessageAttribute element, not a wrapper.
type sqsMessageAttributeXML struct {
	Name  string                      `xml:"Name"`
	Value sqsMessageAttributeValueXML `xml:"Value"`
}

// sqsMessageAttributeValueXML is an attribute's value in a ReceiveMessage XML
// response.
//
// BinaryValue is a string rather than []byte because the wire form is base64 and
// encoding/xml would otherwise emit the raw bytes; encoding/json base64s a []byte for
// free, which is why only the XML path needs the explicit conversion.
type sqsMessageAttributeValueXML struct {
	DataType    string `xml:"DataType"`
	StringValue string `xml:"StringValue,omitempty"`
	BinaryValue string `xml:"BinaryValue,omitempty"`
}

// sqsMessageAttributesXML renders a message's attributes for an XML response,
// ordered by name.
//
// The ordering is deliberate rather than incidental. Real SQS does not promise an
// order, but a Go map iterates randomly, so emitting in map order would make one
// substrate response differ from the next for identical input — which is the one
// property this emulator exists to provide. Sorting by name matches the order the
// digest is computed in.
func sqsMessageAttributesXML(attrs map[string]SQSMessageAttribute) []sqsMessageAttributeXML {
	if len(attrs) == 0 {
		return nil
	}
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]sqsMessageAttributeXML, 0, len(names))
	for _, name := range names {
		attr := attrs[name]
		value := sqsMessageAttributeValueXML{
			DataType:    attr.DataType,
			StringValue: attr.StringValue,
		}
		if len(attr.BinaryValue) > 0 {
			value.BinaryValue = base64.StdEncoding.EncodeToString(attr.BinaryValue)
		}
		out = append(out, sqsMessageAttributeXML{Name: name, Value: value})
	}
	return out
}
