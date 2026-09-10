package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CloudWatch's error shapes, and error shaping that follows the request (#757, #758).
//
// Every expectation here is read off the AWS-published Smithy model for CloudWatch
// (codegen/sdk-codegen/aws-models/cloudwatch.json in aws-sdk-go-v2), not off the API
// reference, which documents the Query codes and says nothing about shape names, member
// casing or fault type. The table below restates the four model facts per shape so that
// a change to cloudWatchErrorShapes has to be justified against the model rather than
// against itself.

func TestCloudWatchErrorShapes(t *testing.T) {
	tests := []struct {
		queryCode     string
		wantShapeID   string
		wantMember    string
		wantServer    bool
		wantHTTPError int
	}{
		// The four codes substrate's own CloudWatch plugin and pipeline can raise with a
		// modeled shape behind them come first; the rest of the model follows.
		{"InvalidParameterValue", "com.amazonaws.cloudwatch#InvalidParameterValueException", "message", false, 400},
		{"ResourceNotFoundException", "com.amazonaws.cloudwatch#ResourceNotFoundException", "Message", false, 404},
		{"MissingParameter", "com.amazonaws.cloudwatch#MissingRequiredParameterException", "message", false, 400},
		{"InternalServiceError", "com.amazonaws.cloudwatch#InternalServiceFault", "Message", true, 500},

		{"ConcurrentModificationException", "com.amazonaws.cloudwatch#ConcurrentModificationException", "Message", false, 429},
		{"ConflictException", "com.amazonaws.cloudwatch#ConflictException", "Message", false, 409},
		{"InvalidFormat", "com.amazonaws.cloudwatch#InvalidFormatFault", "message", false, 400},
		{"InvalidNextToken", "com.amazonaws.cloudwatch#InvalidNextToken", "message", false, 400},
		{"InvalidParameterCombination", "com.amazonaws.cloudwatch#InvalidParameterCombinationException", "message", false, 400},
		{"InvalidParameterInput", "com.amazonaws.cloudwatch#DashboardInvalidInputError", "message", false, 400},
		{"LimitExceeded", "com.amazonaws.cloudwatch#LimitExceededFault", "message", false, 400},
		{"LimitExceededException", "com.amazonaws.cloudwatch#LimitExceededException", "Message", false, 400},
		{"ResourceConflict", "com.amazonaws.cloudwatch#ResourceConflict", "message", false, 409},
		{"ResourceNotFound", "com.amazonaws.cloudwatch#ResourceNotFound", "message", false, 404},
		{"KmsAccessDeniedException", "com.amazonaws.cloudwatch#KmsAccessDeniedException", "Message", false, 400},
		{"KmsKeyDisabledException", "com.amazonaws.cloudwatch#KmsKeyDisabledException", "Message", false, 400},
		{"KmsKeyNotFoundException", "com.amazonaws.cloudwatch#KmsKeyNotFoundException", "Message", false, 400},
	}

	for _, tc := range tests {
		t.Run(tc.queryCode, func(t *testing.T) {
			shapeID, member, serverFault, status := emulator.ErrorShapeForTest("monitoring", tc.queryCode, 0)
			assert.Equal(t, tc.wantShapeID, shapeID)
			assert.Equal(t, tc.wantMember, member)
			assert.Equal(t, tc.wantServer, serverFault)
			assert.Equal(t, tc.wantHTTPError, status)
		})
	}

	// The table holds exactly the model's error shapes and nothing invented alongside
	// them, so a row added without a model behind it fails here.
	t.Run("table is complete and closed", func(t *testing.T) {
		want := make([]string, 0, len(tests))
		for _, tc := range tests {
			want = append(want, tc.queryCode)
		}
		assert.ElementsMatch(t, want, emulator.CloudWatchErrorCodesForTest())
	})

	// The casing really does vary per shape, which is the whole reason the member name
	// is stored rather than assumed. A CBOR client deserializes by exact member name.
	t.Run("message member casing is not uniform", func(t *testing.T) {
		_, lower, _, _ := emulator.ErrorShapeForTest("monitoring", "InvalidParameterValue", 0)
		_, upper, _, _ := emulator.ErrorShapeForTest("monitoring", "InternalServiceError", 0)
		require.Equal(t, "message", lower)
		require.Equal(t, "Message", upper)
	})
}

// TestErrorShapeFallback covers the codes with no shape behind them: the pipeline's own,
// raised before any service model is consulted. They pass through unchanged, which is
// the honest answer — a reference client sanitizes __type by taking the text after "#",
// so an unqualified value yields the same discriminant a qualified one would.
func TestErrorShapeFallback(t *testing.T) {
	tests := []struct {
		code       string
		status     int
		wantServer bool
	}{
		{"AccessDenied", http.StatusForbidden, false},
		{"InvalidAction", http.StatusBadRequest, false},
		{"InvalidClientTokenId", http.StatusForbidden, false},
		{"ThrottlingException", http.StatusBadRequest, false},
		{"UnknownOperationException", http.StatusBadRequest, false},
		{"InternalFailure", http.StatusInternalServerError, true},
		{"SerializationException", http.StatusBadRequest, false},
	}
	for _, tc := range tests {
		t.Run(tc.code, func(t *testing.T) {
			shapeID, member, serverFault, status := emulator.ErrorShapeForTest("monitoring", tc.code, tc.status)
			assert.Equal(t, tc.code, shapeID, "an unmodeled code passes through unqualified")
			assert.Equal(t, "message", member)
			assert.Equal(t, tc.wantServer, serverFault)
			assert.Equal(t, tc.status, status)
		})
	}

	// A service with no table at all takes the same path, so nothing about the 66 other
	// services' error bodies changes: their shape is already their code.
	t.Run("service with no model", func(t *testing.T) {
		shapeID, _, _, _ := emulator.ErrorShapeForTest("dynamodb", "ResourceNotFoundException", 400)
		assert.Equal(t, "ResourceNotFoundException", shapeID)
	})
}

// TestErrorProtocolForRequest is the classification the emulator actually uses. The
// property that matters is asymmetric: a multi-protocol service follows its caller, and
// a single-protocol one never does.
func TestErrorProtocolForRequest(t *testing.T) {
	cbor := map[string]string{"Smithy-Protocol": "rpc-v2-cbor", "Content-Type": "application/cbor"}
	jsonRPC := map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801.ListMetrics", "Content-Type": "application/x-amz-json-1.0"}
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}

	tests := []struct {
		name    string
		service string
		headers map[string]string
		want    string
	}{
		{"CloudWatch over CBOR", "monitoring", cbor, emulator.ErrProtoRPCV2CBORForTest},
		{"CloudWatch over JSON RPC", "monitoring", jsonRPC, emulator.ErrProtoJSONRPCForTest},
		{"CloudWatch over Query", "monitoring", form, emulator.ErrProtoQueryXMLForTest},

		// The regression guard for #392. A REST-JSON service must keep its modeled
		// protocol whatever headers arrive, because a request cannot tell REST-JSON from
		// a plain JSON body — and if the request could override it, a Lambda call would
		// be reclassified and the SDK would lose the error code again.
		{"Lambda is not reclassified by a JSON target", "lambda", jsonRPC, emulator.ErrProtoRESTJSONForTest},
		{"Lambda is not reclassified by a CBOR header", "lambda", cbor, emulator.ErrProtoRESTJSONForTest},
		{"S3 is not reclassified", "s3", jsonRPC, emulator.ErrProtoS3XMLForTest},
		{"EC2 is not reclassified", "ec2", cbor, emulator.ErrProtoEC2XMLForTest},
		{"IAM is not reclassified", "iam", cbor, emulator.ErrProtoQueryXMLForTest},

		// #758: sso is JSON RPC now, and stays JSON RPC whatever arrives.
		{"sso is JSON RPC", "sso", form, emulator.ErrProtoJSONRPCForTest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := requestWith(t, "/", tc.headers)
			assert.Equal(t, tc.want, emulator.ErrorProtocolForRequestForTest(tc.service, r))
		})
	}

	// An in-process caller has no request, and must get the per-service answer.
	t.Run("nil request is the per-service answer", func(t *testing.T) {
		assert.Equal(t, emulator.ErrProtoQueryXMLForTest, emulator.ErrorProtocolForRequestForTest("monitoring", nil))
		assert.Equal(t, emulator.ErrProtoJSONRPCForTest, emulator.ErrorProtocolForRequestForTest("dynamodb", nil))
	})

	// Keeping the multi-protocol set small is the property, not an implementation
	// detail: every name in it is a service whose single-protocol guarantee is given up.
	t.Run("only CloudWatch is multi-protocol", func(t *testing.T) {
		assert.Equal(t, []string{"monitoring"}, emulator.MultiProtocolServicesForTest())
	})
}

// TestMarshalAWSError_RPCV2CBOR pins the error document's bytes. Decoding rather than
// comparing a hex literal is deliberate here — the codec's own wire bytes are pinned in
// cbor_test.go, and what this test is about is which members the document carries.
func TestMarshalAWSError_RPCV2CBOR(t *testing.T) {
	t.Run("modeled CloudWatch error", func(t *testing.T) {
		body, ct, headers := emulator.MarshalAWSErrorWireForTest(
			"InvalidParameterValue", "The parameter Namespace is invalid.",
			emulator.ErrProtoRPCV2CBORForTest, "application/cbor", "monitoring", false, 400)

		assert.Equal(t, "application/cbor", ct)
		// The response header is required and must match the request's.
		assert.Equal(t, "rpc-v2-cbor", headers["Smithy-Protocol"])
		// No query-error header: the caller did not ask for one.
		assert.NotContains(t, headers, "x-amzn-query-error")
		// The specification says X-Amzn-ErrorType SHOULD NOT be sent on this protocol,
		// and that a client MUST ignore it. Not sending it is the cleaner answer.
		assert.NotContains(t, headers, "x-amzn-ErrorType")

		decoded, err := emulator.CBORDecodeForTest(body)
		require.NoError(t, err)
		doc, ok := decoded.(map[string]any)
		require.True(t, ok, "error body decodes as a CBOR map")

		// __type names the shape, not the Query code. This is the translation the whole
		// table exists for: an SDK matches this against its generated shape names.
		assert.Equal(t, "com.amazonaws.cloudwatch#InvalidParameterValueException", doc["__type"])
		assert.Equal(t, "The parameter Namespace is invalid.", doc["message"])
		// The Query code appears nowhere in the body — the specification is explicit
		// that a "Code" member MUST NOT be used to distinguish the error.
		assert.NotContains(t, doc, "Code")
		assert.NotContains(t, doc, "Message")
		assert.Len(t, doc, 2)
	})

	t.Run("uppercase message member is honored", func(t *testing.T) {
		body, _, _ := emulator.MarshalAWSErrorWireForTest(
			"InternalServiceError", "boom", emulator.ErrProtoRPCV2CBORForTest,
			"application/cbor", "monitoring", false, 500)
		decoded, err := emulator.CBORDecodeForTest(body)
		require.NoError(t, err)
		doc := decoded.(map[string]any)
		assert.Equal(t, "boom", doc["Message"], "InternalServiceFault spells it Message")
		assert.NotContains(t, doc, "message")
	})

	t.Run("query mode adds the query code in a header", func(t *testing.T) {
		_, _, headers := emulator.MarshalAWSErrorWireForTest(
			"InvalidParameterValue", "bad", emulator.ErrProtoRPCV2CBORForTest,
			"application/cbor", "monitoring", true, 400)
		// The Query code, not the shape name — that is the point of the bridge: the SDK
		// matches the shape in the body while its caller matches the legacy code here.
		assert.Equal(t, "InvalidParameterValue;Sender", headers["x-amzn-query-error"])
	})

	t.Run("a server fault is a Receiver", func(t *testing.T) {
		_, _, headers := emulator.MarshalAWSErrorWireForTest(
			"InternalServiceError", "boom", emulator.ErrProtoRPCV2CBORForTest,
			"application/cbor", "monitoring", true, 500)
		assert.Equal(t, "InternalServiceError;Receiver", headers["x-amzn-query-error"])
	})

	t.Run("an unmodeled code passes through", func(t *testing.T) {
		body, _, headers := emulator.MarshalAWSErrorWireForTest(
			"AccessDenied", "not authorized", emulator.ErrProtoRPCV2CBORForTest,
			"application/cbor", "monitoring", true, 403)
		decoded, err := emulator.CBORDecodeForTest(body)
		require.NoError(t, err)
		doc := decoded.(map[string]any)
		assert.Equal(t, "AccessDenied", doc["__type"])
		assert.Equal(t, "not authorized", doc["message"])
		assert.Equal(t, "AccessDenied;Sender", headers["x-amzn-query-error"])
	})
}

// TestMarshalAWSError_JSONRPCShapeID covers the same translation on the protocol the AWS
// CLI uses. aws-sdk-go-v2's JSON deserializers prefer X-Amzn-ErrorType over the body
// when it is present, so the header has to agree with __type or the two disagree about
// which error this is.
func TestMarshalAWSError_JSONRPCShapeID(t *testing.T) {
	t.Run("CloudWatch names the shape", func(t *testing.T) {
		body, ct, headers := emulator.MarshalAWSErrorWireForTest(
			"InvalidParameterValue", "bad", emulator.ErrProtoJSONRPCForTest,
			"application/x-amz-json-1.0", "monitoring", true, 400)

		assert.Equal(t, "application/x-amz-json-1.0", ct, "the caller's JSON version is echoed")
		assert.Equal(t, "com.amazonaws.cloudwatch#InvalidParameterValueException", headers["x-amzn-ErrorType"])
		assert.Equal(t, "InvalidParameterValue;Sender", headers["x-amzn-query-error"])

		var doc map[string]string
		require.NoError(t, json.Unmarshal(body, &doc))
		assert.Equal(t, "com.amazonaws.cloudwatch#InvalidParameterValueException", doc["__type"])
		// Code and Message stay the Query code and the text, which is what they have
		// always been; they are a convenience for callers reading the body directly and
		// are not what an SDK dispatches on.
		assert.Equal(t, "InvalidParameterValue", doc["Code"])
		assert.Equal(t, "bad", doc["message"])
		assert.Equal(t, "bad", doc["Message"])
	})

	// Nothing changes for a service with no error model, which is every other JSON-RPC
	// service: the shape and the code are the same string, so the bytes are unchanged.
	t.Run("other JSON services are byte-identical", func(t *testing.T) {
		withService, _, hdrsWith := emulator.MarshalAWSErrorWireForTest(
			"ResourceNotFoundException", "no table", emulator.ErrProtoJSONRPCForTest,
			"application/x-amz-json-1.0", "dynamodb", false, 400)
		noService, _, hdrsWithout := emulator.MarshalAWSErrorForTest(
			"ResourceNotFoundException", "no table", emulator.ErrProtoJSONRPCForTest,
			"application/x-amz-json-1.0", 400)
		assert.Equal(t, noService, withService)
		assert.Equal(t, hdrsWithout, hdrsWith)
		assert.NotContains(t, hdrsWith, "x-amzn-query-error", "query mode is off")
	})
}

// TestMarshalAWSError_QueryModeIgnoredByXML pins that the bridge is confined to the two
// protocols that need it. A Query client already has the code in the document, and an S3
// or EC2 client never sends the header at all.
func TestMarshalAWSError_QueryModeIgnoredByXML(t *testing.T) {
	for _, proto := range []string{
		emulator.ErrProtoQueryXMLForTest,
		emulator.ErrProtoS3XMLForTest,
		emulator.ErrProtoEC2XMLForTest,
		emulator.ErrProtoRESTJSONForTest,
	} {
		t.Run(proto, func(t *testing.T) {
			_, _, headers := emulator.MarshalAWSErrorWireForTest(
				"InvalidParameterValue", "bad", proto, "", "monitoring", true, 400)
			assert.NotContains(t, headers, "x-amzn-query-error")
		})
	}
}
