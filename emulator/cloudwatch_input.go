package emulator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"
)

// Request normalization for CloudWatch's three wire protocols (#785).
//
// The counterpart to cloudwatch_render.go. Where that file gives one document three
// renderings, this one gives three request serializations one representation: a JSON or
// RPC v2 CBOR body is flattened into req.Params using the query protocol's spelling, so
// that every handler reads req.Params and none of them knows which protocol brought the
// request in.
//
// This is what keeps a three-protocol CloudWatch from being three implementations. The
// alternative — teaching ten handlers to read a decoded document as well as a parameter
// map — would have meant twenty parsing paths and a second place for every default,
// every required-member check and every enum validation to disagree.
//
// # Why the query spelling is the common one
//
// It works because CloudWatch's model declares no smithy.api#xmlName trait anywhere and
// no smithy.api#xmlFlattened list anywhere. Both were checked across the whole model
// rather than assumed. The first means a member's query key is its member name, so
// `{"AlarmName": "x"}` is unambiguously `AlarmName=x`; the second means every list is
// wrapped, so `{"AlarmNames": ["a"]}` is `AlarmNames.member.1=a` and
// [parseMemberList] reads it unchanged.
//
// Going the other way — normalizing to a decoded document and teaching the handlers to
// read that — would have been the cleaner direction on a blank page, but it would have
// rewritten the parameter reads in all ten handlers, and those reads are the part of
// CloudWatch that already works.
//
// # What is lost, and why it does not matter here
//
// Flattening to strings discards static types: a CBOR double arrives as a float64 and
// leaves as the text "0.1". Nothing is lost in practice because the handlers parse
// their numbers out of the query form already, and a value that round-trips through
// [strconv.FormatFloat] with -1 precision returns the same float64. What the flattening
// does not preserve is the difference between an absent member and one explicitly sent
// as null, and CloudWatch has no operation where that distinction is observable.

// cwNormalizeInput flattens a JSON-RPC or RPC v2 CBOR request body into req.Params
// using the query protocol's spelling, leaving a Query request untouched.
//
// It returns an *AWSError for a body that is not well-formed, so that a caller sending
// a truncated or mistyped document is told so rather than seeing an operation quietly
// act on no parameters at all.
func cwNormalizeInput(req *AWSRequest) error {
	if req.Protocol == WireQuery || len(req.Body) == 0 {
		return nil
	}

	var doc map[string]any
	switch req.Protocol {
	case WireRPCV2CBOR:
		value, err := cborDecode(req.Body)
		if err != nil {
			return cwSerializationError("the request body is not well-formed CBOR: " + err.Error())
		}
		// A Smithy structure always encodes as a map, so anything else is a client that
		// serialized the wrong shape.
		decoded, ok := value.(map[string]any)
		if !ok {
			return cwSerializationError(fmt.Sprintf(
				"the request body decoded as %T, but an operation input must be a CBOR map", value))
		}
		doc = decoded

	case WireJSONRPC:
		if err := json.Unmarshal(req.Body, &doc); err != nil {
			return cwSerializationError("the request body is not well-formed JSON: " + err.Error())
		}

	case WireQuery:
		return nil
	}

	if req.Params == nil {
		req.Params = make(map[string]string, len(doc))
	}
	if err := cwFlattenDoc(req.Params, "", doc, 0); err != nil {
		return cwSerializationError(err.Error())
	}
	return nil
}

// cwSerializationError builds the error substrate answers for a malformed body.
//
// SerializationException is the code AWS services use for a body that cannot be
// deserialized, and 400 is the Smithy default for a client error. Neither the rpcv2Cbor
// specification nor CloudWatch's model names a shape for this case, so the choice is
// substrate's; it is documented rather than presented as modeled.
func cwSerializationError(message string) *AWSError {
	return &AWSError{
		Code:       "SerializationException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwFlattenDoc writes each of doc's members into params under prefix.
//
// Iteration order is Go's randomized map order, which is safe precisely because the
// destination is a map: every member lands under a distinct key, so the result does not
// depend on the order they were visited in.
func cwFlattenDoc(params map[string]string, prefix string, doc map[string]any, depth int) error {
	if depth > cwMaxDepth {
		return fmt.Errorf("the request body nests deeper than %d levels", cwMaxDepth)
	}
	for name, value := range doc {
		if err := cwFlattenValue(params, prefix+name, value, depth); err != nil {
			return fmt.Errorf("member %q: %w", name, err)
		}
	}
	return nil
}

// cwFlattenValue writes one value into params under key.
func cwFlattenValue(params map[string]string, key string, v any, depth int) error {
	switch t := v.(type) {
	case nil:
		// The query form has no spelling for null, and an absent member and a null one
		// mean the same thing to every CloudWatch operation, so it is dropped.
		return nil
	case map[string]any:
		return cwFlattenDoc(params, key+".", t, depth+1)
	case []any:
		// One-based, and wrapped in "member" because no CloudWatch list is flattened.
		for i, elem := range t {
			indexed := key + ".member." + strconv.Itoa(i+1)
			if err := cwFlattenValue(params, indexed, elem, depth+1); err != nil {
				return fmt.Errorf("element %d: %w", i+1, err)
			}
		}
		return nil
	default:
		text, err := cwParamText(v)
		if err != nil {
			return err
		}
		params[key] = text
		return nil
	}
}

// cwParamText renders a decoded scalar in the query form's spelling.
func cwParamText(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		return cwFormatNumber(t)
	case time.Time:
		return t.UTC().Format(cwTimestampLayout), nil
	case []byte:
		return base64.StdEncoding.EncodeToString(t), nil
	default:
		return "", fmt.Errorf("decoded as %T, which no CloudWatch member is modeled as", v)
	}
}

// cwFormatNumber renders a decoded number as text a handler can parse back.
//
// A whole number is written without an exponent even when it is large, because the
// handlers read integers with [strconv.Atoi] and JSON decodes every number — including
// a Period of 86400 — as a float64. 'g' formatting would turn 1e21 into "1e+21", which
// Atoi refuses; writing the integer form means a value that is integral in the document
// is integral in the parameter map too. Anything not integral keeps 'g' with the
// shortest round-tripping precision, which is what botocore puts on the query wire.
func cwFormatNumber(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("is %v, which no CloudWatch member may be", f)
	}
	if f == math.Trunc(f) && math.Abs(f) < math.MaxInt64 {
		return strconv.FormatInt(int64(f), 10), nil
	}
	return strconv.FormatFloat(f, 'g', -1, 64), nil
}
