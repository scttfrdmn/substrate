package emulator

// CloudWatch's modeled error shapes, and why substrate needs them (#757).
//
// The Query protocol puts a short code in the response document —
// <Error><Code>InvalidParameterValue</Code> — and that is the only spelling the
// CloudWatch API reference documents. The JSON and CBOR protocols put the *shape ID*
// in a "__type" member — com.amazonaws.cloudwatch#InvalidParameterValueException —
// and that is what an SDK matches against to pick the typed error it hands its caller.
// The two spellings differ for most of CloudWatch's error shapes, so a service that
// answers all three protocols cannot use one string for all of them: substrate's
// plugins raise the Query code (it is what the documentation names), and this table
// translates it on the way out.
//
// Provenance: the mapping is not guessed. Every entry below is read off the
// AWS-published Smithy model for CloudWatch, shipped as
// codegen/sdk-codegen/aws-models/cloudwatch.json in aws-sdk-go-v2, where each error
// shape carries the traits that decide all four columns:
//
//   - aws.protocols#awsQueryError.code — the Query spelling, which is this table's key.
//     A shape without the trait is keyed by its own name, because then the two
//     protocols agree and there is nothing to translate.
//   - the member name carrying the text. It is *not* uniform: the model spells it
//     "Message" on InternalServiceFault and ConcurrentModificationException and
//     "message" on InvalidParameterValueException and its siblings. A CBOR client
//     deserializes by exact member name, so getting the case wrong loses the message.
//   - smithy.api#error — "client" or "server", which becomes Sender or Receiver in the
//     x-amzn-query-error header.
//   - smithy.api#httpError — the status. Recorded for assertion rather than applied:
//     the status substrate sends stays the one the plugin chose, so adding this table
//     cannot change any existing response's status code. In every CloudWatch case the
//     two agree with aws.protocols#awsQueryError.httpResponseCode anyway, so there is
//     no divergence to reconcile — a divergence the protocol test suite does construct,
//     but only in a synthetic service.
//
// The API reference is not a substitute for the model here: it documents the Query
// codes and nothing about shape names, member casing, or fault type.

// cwErrorNamespace is the Smithy namespace CloudWatch's shapes live in, which prefixes
// every shape ID in a "__type" member.
const cwErrorNamespace = "com.amazonaws.cloudwatch"

// modeledError is one service error as its Smithy model declares it, reduced to the
// four facts a wire serializer needs. See the file comment for where each comes from.
type modeledError struct {
	// Namespace is the Smithy namespace the shape lives in, e.g. "com.amazonaws.cloudwatch".
	Namespace string

	// Shape is the error shape's name, e.g. "InvalidParameterValueException".
	Shape string

	// MessageMember is the member name carrying the human-readable text — "Message" or
	// "message", which varies per shape.
	MessageMember string

	// ServerFault reports whether the model declares @error("server") rather than
	// @error("client").
	ServerFault bool

	// HTTPStatus is the shape's @httpError. It is recorded for assertion; the status
	// substrate actually sends is the one the raising plugin chose.
	HTTPStatus int
}

// ShapeID returns the absolute shape ID a "__type" member carries.
func (m modeledError) ShapeID() string {
	if m.Namespace == "" {
		return m.Shape
	}
	return m.Namespace + "#" + m.Shape
}

// cloudWatchErrorShapes maps each CloudWatch error's Query code to the shape behind it.
//
// Two shapes share the Query code "ResourceNotFound" — DashboardNotFoundError and
// ResourceNotFound — so that key can only name one of them, and it names the shape of
// the same name. The collision is harmless in practice: both are @error("client") with
// status 404 and a lowercase "message" member, so a client distinguishing them by
// __type is the only caller that could tell, and substrate raises neither. The code
// substrate does raise for a missing metric or alarm is
// "ResourceNotFoundException" (cloudwatch_plugin.go), whose own row is unambiguous.
var cloudWatchErrorShapes = map[string]modeledError{
	"ConcurrentModificationException": {Shape: "ConcurrentModificationException", MessageMember: "Message", HTTPStatus: 429},
	"ConflictException":               {Shape: "ConflictException", MessageMember: "Message", HTTPStatus: 409},
	"InternalServiceError":            {Shape: "InternalServiceFault", MessageMember: "Message", ServerFault: true, HTTPStatus: 500},
	"InvalidFormat":                   {Shape: "InvalidFormatFault", MessageMember: "message", HTTPStatus: 400},
	"InvalidNextToken":                {Shape: "InvalidNextToken", MessageMember: "message", HTTPStatus: 400},
	"InvalidParameterCombination":     {Shape: "InvalidParameterCombinationException", MessageMember: "message", HTTPStatus: 400},
	"InvalidParameterInput":           {Shape: "DashboardInvalidInputError", MessageMember: "message", HTTPStatus: 400},
	"InvalidParameterValue":           {Shape: "InvalidParameterValueException", MessageMember: "message", HTTPStatus: 400},
	"LimitExceeded":                   {Shape: "LimitExceededFault", MessageMember: "message", HTTPStatus: 400},
	"LimitExceededException":          {Shape: "LimitExceededException", MessageMember: "Message", HTTPStatus: 400},
	"MissingParameter":                {Shape: "MissingRequiredParameterException", MessageMember: "message", HTTPStatus: 400},
	"ResourceConflict":                {Shape: "ResourceConflict", MessageMember: "message", HTTPStatus: 409},
	"ResourceNotFound":                {Shape: "ResourceNotFound", MessageMember: "message", HTTPStatus: 404},
	"ResourceNotFoundException":       {Shape: "ResourceNotFoundException", MessageMember: "Message", HTTPStatus: 404},

	// The three KMS shapes declare @error("client") and a Message member but no
	// @httpError and no awsQueryError, so 400 below is the Smithy default for a client
	// error rather than a modeled value. Substrate raises none of them today; they are
	// here so that when a plugin does, the casing is already right.
	"KmsAccessDeniedException": {Shape: "KmsAccessDeniedException", MessageMember: "Message", HTTPStatus: 400},
	"KmsKeyDisabledException":  {Shape: "KmsKeyDisabledException", MessageMember: "Message", HTTPStatus: 400},
	"KmsKeyNotFoundException":  {Shape: "KmsKeyNotFoundException", MessageMember: "Message", HTTPStatus: 400},
}

// serviceErrorModel is one service's error shapes and the namespace they live in. The
// namespace is held once here rather than repeated on every row, since a Smithy model
// declares all of a service's shapes in one namespace.
type serviceErrorModel struct {
	// Namespace prefixes every shape ID this model produces.
	Namespace string

	// Shapes maps the Query code substrate's plugins raise to the shape behind it.
	Shapes map[string]modeledError
}

// serviceErrorShapes maps a service name to its modeled error shapes.
//
// Only CloudWatch is listed, because only CloudWatch answers more than one protocol
// today (see multiProtocolServices). A single-protocol service needs no translation:
// its plugin already raises the one spelling its one protocol uses.
var serviceErrorShapes = map[string]serviceErrorModel{
	"monitoring": {Namespace: cwErrorNamespace, Shapes: cloudWatchErrorShapes},
}

// errorShapeFor resolves the shape a "__type" member should name for an error raised
// by service with the given code, and the fault and message-member spelling that go
// with it. status is the HTTP status the error carries, used only by the fallback.
//
// The fallback, for a code no table names, is to pass the code through as the shape and
// to derive the fault from the status. That is deliberately lossy in one way and safe in
// another. It is lossy because an unqualified "__type" names no namespace — but the
// reference clients sanitize the member by taking the text after "#" and truncating at
// ":", so an unqualified value yields exactly the same discriminant a qualified one
// would, and a client matching on the shape name still matches. It is safe because the
// codes that reach it are the pipeline's own, raised before any service model is
// consulted — AccessDenied, InternalFailure, InvalidAction, InvalidClientTokenId,
// ThrottlingException, UnknownOperationException — and none of those is a shape in any
// service's model, so there is nothing to translate them to. A client sees the code it
// would have seen from the Query protocol, which is the honest answer.
func errorShapeFor(service, code string, status int) modeledError {
	if model, ok := serviceErrorShapes[service]; ok {
		if shape, ok := model.Shapes[code]; ok {
			shape.Namespace = model.Namespace
			return shape
		}
	}
	return modeledError{
		Shape:         code,
		MessageMember: "message",
		ServerFault:   status >= 500,
		HTTPStatus:    status,
	}
}
