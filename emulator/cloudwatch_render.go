package emulator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"
)

// Response rendering for CloudWatch's three wire protocols (#785).
//
// CloudWatch's service shape carries aws.protocols#awsQuery, aws.protocols#awsJson1_0
// and smithy.protocols#rpcv2Cbor at the same time, so the same operation must be able
// to answer in three serializations. Before this file every handler built an
// encoding/xml struct and answered XML no matter what the caller had asked for, which
// is why aws-sdk-go-v2 reported "deserialization failed" and the AWS CLI printed
// nothing at all.
//
// The shape of the fix is one *ordered neutral document* per operation, built once by
// the handler, plus three renderers over it. A handler therefore has no idea which
// protocol it is answering: nothing in this file is reachable from a handler except
// [cwRespond] and [cwUnitResponse]. That is deliberate — the alternative, a
// per-protocol branch inside ten handlers, is thirty places for the three
// serializations to drift apart.
//
// # Why the document is ordered
//
// [cwDoc] is a slice, not a map, for the same reason [cborMap] is: substrate's premise
// is that the same inputs produce the same bytes, and ranging a Go map would order
// members by randomized iteration, so one response would encode two ways and no
// recorded event could be replayed byte-for-byte.
//
// The order each handler uses is the order substrate's XML structs already used, not
// the model's member order. Member order is semantically meaningless in all three
// protocols — XML and JSON name their members, and CBOR maps are unordered — so
// keeping the existing order costs nothing and buys a byte-for-byte guarantee that the
// Query path a consumer already depends on did not move.
//
// # Absent versus present-and-empty
//
// A member the handler does not append is absent in all three renderings: omitted from
// the XML, from the JSON object and from the CBOR map. A member appended with an empty
// [cwList] is present but empty, and renders as <Name></Name>, [] and a zero-length
// array respectively. The distinction is the handler's to make and matters to a typed
// client, which reads an absent list as nil and an empty one as a zero-length slice.
//
// # Unit outputs
//
// Six of CloudWatch's ten operations are modeled with a smithy.api#Unit output, and the
// RPC v2 CBOR protocol tests are explicit that such a response carries no body and
// MUST NOT set Content-Type (`no_output` in
// smithy-protocol-tests/model/rpcv2Cbor/empty-input-output.smithy lists Content-Type in
// its forbidHeaders). [cwUnitResponse] is how a handler says so; the empty CBOR map
// substrate used to send from GetMetricData is merely *tolerated* by the
// NoOutputClientAllowsEmptyCbor test, not conformant.

// cwJSONContentType is the media type for CloudWatch's awsJson1_0 protocol. The
// version is in the media type, not the payload, so it is stated here rather than
// derived.
const cwJSONContentType = "application/x-amz-json-1.0"

// cwXMLContentType is the media type for CloudWatch's awsQuery responses.
const cwXMLContentType = "text/xml; charset=UTF-8"

// cwTimestampLayout is how a timestamp is written in a Query response: ISO 8601 at
// millisecond resolution, always three fractional digits, always UTC. This is the
// query protocol's own spelling; the JSON and CBOR renderers use epoch seconds
// instead, because that is what their protocols specify.
const cwTimestampLayout = "2006-01-02T15:04:05.000Z"

// cwMaxDepth bounds nesting on both rendering and input flattening.
//
// Like [cborMaxDepth] it is a guard against a hostile body rather than a modeling
// limit: CloudWatch's deepest input shape, a MetricDataQuery's MetricStat's Metric's
// Dimensions, is five levels, so a request this refuses was not going to be served.
const cwMaxDepth = 32

// cwDoc is a CloudWatch structure as an ordered list of members: the neutral document
// all three renderers consume. Absent optional members are simply not present.
type cwDoc []cwMember

// cwMember is one member of a [cwDoc].
type cwMember struct {
	// Name is the member name as the model spells it. Every CloudWatch shape uses the
	// same name in all three protocols — the model declares no smithy.api#xmlName
	// trait anywhere — so one name serves the XML, JSON and CBOR renderings alike.
	Name string

	// Value is a scalar (string, int, int64, float64, bool, [time.Time] or []byte), a
	// nested [cwDoc], or a [cwList].
	Value any
}

// cwList is a CloudWatch list shape. Elements are scalars or [cwDoc] values.
//
// No CloudWatch list is smithy.api#xmlFlattened — verified across the whole model, not
// assumed — so the Query rendering always wraps elements in <member> elements.
type cwList []any

// with returns doc with a member appended. It is a value method returning a new slice
// so that a handler can build a document in one expression.
func (doc cwDoc) with(name string, value any) cwDoc {
	return append(doc, cwMember{Name: name, Value: value})
}

// withNonEmpty returns doc with a string member appended only when it is non-empty,
// which is how an unset optional string stays absent from all three renderings.
func (doc cwDoc) withNonEmpty(name, value string) cwDoc {
	if value == "" {
		return doc
	}
	return doc.with(name, value)
}

// withStrings returns doc with a list-of-strings member appended only when the list has
// elements. An unset list must be absent rather than empty: a typed client reads the
// two differently, and CloudWatch omits an alarm's action lists when none are set.
func (doc cwDoc) withStrings(name string, values []string) cwDoc {
	if len(values) == 0 {
		return doc
	}
	list := make(cwList, 0, len(values))
	for _, v := range values {
		list = append(list, v)
	}
	return doc.with(name, list)
}

// cwRespond renders result in whichever of CloudWatch's three wire protocols the caller
// used, as recorded in req.Protocol during parsing.
//
// A nil result means the operation's modeled output is smithy.api#Unit; use
// [cwUnitResponse] rather than passing nil directly, so the intent is visible at the
// call site. operation names the operation for the Query rendering's element names, and
// requestID fills its ResponseMetadata, neither of which the other two protocols have.
func cwRespond(req *AWSRequest, operation, requestID string, result cwDoc) (*AWSResponse, error) {
	switch req.Protocol {
	case WireRPCV2CBOR:
		headers := map[string]string{headerSmithyProtocol: smithyProtocolRPCV2CBOR}
		if result == nil {
			// No body and no Content-Type: see the Unit outputs note above.
			return &AWSResponse{StatusCode: http.StatusOK, Headers: headers}, nil
		}
		body, err := cwRenderCBOR(result)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch %s: render CBOR: %w", operation, err)
		}
		headers["Content-Type"] = contentTypeCBOR
		return &AWSResponse{StatusCode: http.StatusOK, Headers: headers, Body: body}, nil

	case WireJSONRPC:
		// An operation with a Unit output answers "{}" here rather than an empty body:
		// awsJson1_0 has no equivalent of the CBOR protocol's forbidden Content-Type,
		// and botocore's JSON parser reads a zero-length body as a parse failure.
		body, err := cwRenderJSON(result)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch %s: render JSON: %w", operation, err)
		}
		return &AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": cwJSONContentType},
			Body:       body,
		}, nil

	default:
		body, err := cwRenderQueryXML(operation, requestID, result)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch %s: render XML: %w", operation, err)
		}
		return &AWSResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": cwXMLContentType},
			Body:       body,
		}, nil
	}
}

// cwUnitResponse answers an operation whose modeled output is smithy.api#Unit:
// PutMetricAlarm, DeleteAlarms, SetAlarmState, EnableAlarmActions, DisableAlarmActions
// and PutMetricData.
func cwUnitResponse(req *AWSRequest, operation, requestID string) (*AWSResponse, error) {
	return cwRespond(req, operation, requestID, nil)
}

// --- Query (XML) ------------------------------------------------------------

// cwRenderQueryXML renders result as an awsQuery response document.
//
// The wrapper is the query protocol's: <{Operation}Response> holding an
// <{Operation}Result> and a <ResponseMetadata>. A Unit output has no result element at
// all, which is why result being nil is not the same as it being empty.
func cwRenderQueryXML(operation, requestID string, result cwDoc) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString("<" + operation + `Response xmlns="` + cloudwatchXMLNS + `">`)
	if result != nil {
		b.WriteString("<" + operation + "Result>")
		if err := cwWriteXMLMembers(&b, result, 0); err != nil {
			return nil, err
		}
		b.WriteString("</" + operation + "Result>")
	}
	b.WriteString("<ResponseMetadata><RequestId>")
	if err := xml.EscapeText(&b, []byte(requestID)); err != nil {
		return nil, fmt.Errorf("escape RequestId: %w", err)
	}
	b.WriteString("</RequestId></ResponseMetadata>")
	b.WriteString("</" + operation + "Response>")
	return b.Bytes(), nil
}

// cwWriteXMLMembers writes each of doc's members as an element.
func cwWriteXMLMembers(b *bytes.Buffer, doc cwDoc, depth int) error {
	for _, m := range doc {
		if err := cwWriteXMLValue(b, m.Name, m.Value, depth); err != nil {
			return fmt.Errorf("member %q: %w", m.Name, err)
		}
	}
	return nil
}

// cwWriteXMLValue writes one value as an element named name.
//
// Member names come from the model and are never caller-supplied, so they are written
// without escaping; every text value goes through [xml.EscapeText], which is the same
// escaper encoding/xml itself uses.
func cwWriteXMLValue(b *bytes.Buffer, name string, v any, depth int) error {
	if depth > cwMaxDepth {
		return fmt.Errorf("cloudwatch: response nests deeper than %d levels", cwMaxDepth)
	}
	b.WriteString("<" + name + ">")
	switch t := v.(type) {
	case cwDoc:
		if err := cwWriteXMLMembers(b, t, depth+1); err != nil {
			return err
		}
	case cwList:
		for i, elem := range t {
			if err := cwWriteXMLValue(b, "member", elem, depth+1); err != nil {
				return fmt.Errorf("element %d: %w", i+1, err)
			}
		}
	default:
		text, err := cwXMLText(v)
		if err != nil {
			return err
		}
		if err := xml.EscapeText(b, []byte(text)); err != nil {
			return fmt.Errorf("escape text: %w", err)
		}
	}
	b.WriteString("</" + name + ">")
	return nil
}

// cwXMLText renders a scalar as query-protocol character data.
//
// Numbers use the formats encoding/xml itself would have produced — strconv.Itoa for an
// integer and 'g' with the shortest round-tripping precision for a double — so that the
// Query responses substrate already served are unchanged byte-for-byte.
func cwXMLText(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case int:
		return strconv.Itoa(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return "", fmt.Errorf("cloudwatch: cannot render %v in a response", t)
		}
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case bool:
		return strconv.FormatBool(t), nil
	case time.Time:
		return t.UTC().Format(cwTimestampLayout), nil
	case []byte:
		return base64.StdEncoding.EncodeToString(t), nil
	default:
		return "", fmt.Errorf("cloudwatch: cannot render %T in a response", v)
	}
}

// --- awsJson1_0 -------------------------------------------------------------

// cwRenderJSON renders result as an awsJson1_0 response document.
//
// Written by hand rather than via json.Marshal because member order must come from the
// document: marshaling a map[string]any would order members by Go's randomized map
// iteration, so the same response would produce different bytes on different runs.
func cwRenderJSON(result cwDoc) ([]byte, error) {
	var b bytes.Buffer
	if err := cwWriteJSONDoc(&b, result, 0); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// cwWriteJSONDoc writes doc as a JSON object. A nil doc writes "{}".
func cwWriteJSONDoc(b *bytes.Buffer, doc cwDoc, depth int) error {
	if depth > cwMaxDepth {
		return fmt.Errorf("cloudwatch: response nests deeper than %d levels", cwMaxDepth)
	}
	b.WriteByte('{')
	for i, m := range doc {
		if i > 0 {
			b.WriteByte(',')
		}
		key, err := json.Marshal(m.Name)
		if err != nil {
			return fmt.Errorf("member name %q: %w", m.Name, err)
		}
		b.Write(key)
		b.WriteByte(':')
		if err := cwWriteJSONValue(b, m.Value, depth+1); err != nil {
			return fmt.Errorf("member %q: %w", m.Name, err)
		}
	}
	b.WriteByte('}')
	return nil
}

// cwWriteJSONValue writes one value as JSON.
func cwWriteJSONValue(b *bytes.Buffer, v any, depth int) error {
	switch t := v.(type) {
	case cwDoc:
		return cwWriteJSONDoc(b, t, depth)
	case cwList:
		if depth > cwMaxDepth {
			return fmt.Errorf("cloudwatch: response nests deeper than %d levels", cwMaxDepth)
		}
		b.WriteByte('[')
		for i, elem := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := cwWriteJSONValue(b, elem, depth+1); err != nil {
				return fmt.Errorf("element %d: %w", i+1, err)
			}
		}
		b.WriteByte(']')
		return nil
	case time.Time:
		// awsJson1_0's default timestamp format is epoch-seconds: a JSON number, at
		// millisecond resolution, and not the ISO 8601 string the Query protocol uses.
		b.WriteString(strconv.FormatFloat(float64(t.UnixMilli())/1e3, 'g', -1, 64))
		return nil
	case string, int, int64, float64, bool, []byte:
		// json.Marshal is correct for each of these: a blob becomes a base64 string,
		// which is awsJson1_0's blob encoding, and a float is refused if it is NaN or
		// an infinity rather than emitted as invalid JSON.
		encoded, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("cloudwatch: cannot render %T in a response: %w", v, err)
		}
		b.Write(encoded)
		return nil
	default:
		return fmt.Errorf("cloudwatch: cannot render %T in a response", v)
	}
}

// --- Smithy RPC v2 CBOR -----------------------------------------------------

// cwRenderCBOR renders result as an RPC v2 CBOR response document.
func cwRenderCBOR(result cwDoc) ([]byte, error) {
	return cborEncode(cwToCBOR(result))
}

// cwToCBOR converts a neutral document into the codec's own ordered types.
//
// Scalars pass through untouched: [cborEncode] already accepts exactly the set
// [cwMember] permits, including [time.Time], which it writes as tag 1 over epoch
// seconds.
func cwToCBOR(v any) any {
	switch t := v.(type) {
	case cwDoc:
		out := make(cborMap, 0, len(t))
		for _, m := range t {
			out = append(out, cborEntry{Key: m.Name, Value: cwToCBOR(m.Value)})
		}
		return out
	case cwList:
		out := make([]any, 0, len(t))
		for _, elem := range t {
			out = append(out, cwToCBOR(elem))
		}
		return out
	default:
		return v
	}
}
