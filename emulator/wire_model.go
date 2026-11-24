package emulator

// The types of the generated constraint tables in wire_model_gen.go (#1612).
//
// cmd/gen-wire-model writes the tables from the botocore models vendored under
// emulator/wiremodel; [validateRequest] in wire_validate.go walks them. A table holds only
// what carries a constraint or leads to one, so a member absent from a structure's Members
// is one the validator has nothing to check on.

// wireKind is a shape's botocore type, with the integer and floating-point widths folded
// together because the validator checks a range the same way for each.
type wireKind uint8

const (
	// wireStructure is a structure: named members.
	wireStructure wireKind = iota + 1
	// wireList is a list: one member shape, repeated.
	wireList
	// wireMap is a map: a key shape and a value shape.
	wireMap
	// wireString is a string.
	wireString
	// wireBlob is binary data, base64 on a JSON wire.
	wireBlob
	// wireInteger is an integer, long, short or byte.
	wireInteger
	// wireFloat is a float or double.
	wireFloat
	// wireBoolean is a boolean.
	wireBoolean
	// wireTimestamp is a timestamp.
	wireTimestamp
)

// wireLocation is where a REST protocol binds a member. The zero value is the body, which is
// where every member of a non-REST protocol is.
type wireLocation uint8

const (
	// wireInBody is the request body.
	wireInBody wireLocation = iota
	// wireInURI is a label of the request URI.
	wireInURI
	// wireInQuery is a query-string parameter.
	wireInQuery
	// wireInHeader is one HTTP header.
	wireInHeader
	// wireInHeaders is a map of headers sharing a prefix.
	wireInHeaders
)

// wireShape is one shape's constraints.
type wireShape struct {
	// Kind is the shape's type.
	Kind wireKind
	// Min and Max are the shape's bounds as the model writes them, and empty when absent: a
	// length for a string, blob, list or map, a range for a number.
	Min, Max string
	// Enum indexes wireEnums, and is zero when the shape has no enum.
	Enum int
	// Pattern indexes wirePatterns, and is zero when the shape has no pattern.
	Pattern int
	// Members are a structure's constrained members, in the model's order.
	Members []wireMember
	// Member is a list's element, or a map's value.
	Member wireRef
	// Key is a map's key.
	Key wireRef
	// Flat is a list or map the Query protocol serializes without its wrapper element.
	Flat bool
	// Payload names the member a REST input binds to the whole body.
	Payload string
}

// wireRef is a list element, map key or map value reference.
type wireRef struct {
	// Shape indexes wireShapes; zero is the shape with no constraint.
	Shape int
	// LocationName is the element's serialized name, when the model names one.
	LocationName string
}

// wireMember is one structure member.
type wireMember struct {
	// Name is the member's name in the model, which is what AWS's validation messages
	// report, with the first letter lowered.
	Name string
	// Shape indexes wireShapes.
	Shape int
	// Required is the structure's required list naming the member.
	Required bool
	// Location is where a REST protocol binds the member.
	Location wireLocation
	// LocationName is the member's serialized name, when it differs from Name: the JSON or
	// XML key, the Query parameter, the URI label, query-string key or header.
	LocationName string
	// QueryName is the member's name on the EC2 protocol, when the model gives one.
	QueryName string
	// Flat is the reference's own flattened trait.
	Flat bool
}

// wireOperation is one routed operation.
type wireOperation struct {
	// Model indexes wireModels.
	Model int
	// Input indexes wireShapes, and is zero for an operation with nothing to check.
	Input int
	// Method and URI are a REST operation's HTTP binding, with the URI's labels in braces.
	Method, URI string
}

// wireModelInfo names one vendored model.
type wireModelInfo struct {
	// Name is botocore's data directory name.
	Name string
	// APIVersion is the model's API version.
	APIVersion string
	// Protocol is the model's metadata.protocol.
	Protocol string
}
