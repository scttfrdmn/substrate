package emulator

// WireFailurePathsForTest walks req against its operation's input shape, whether or not
// [validatedServices] lists the service, with enum and pattern checks on, and returns each
// failure's path in order. It is nil when the tables do not cover the operation or the
// request will not parse, and empty when nothing fails.
func WireFailurePathsForTest(req *AWSRequest) []string {
	failures := wireFailuresForTest(req)
	if failures == nil {
		return nil
	}
	out := make([]string, len(failures))
	for i, f := range failures {
		out[i] = f.path
	}
	return out
}

// WireFailureMessageForTest is [WireFailurePathsForTest] rendered in the quoted message style,
// or "" when nothing fails.
func WireFailureMessageForTest(req *AWSRequest) string {
	failures := wireFailuresForTest(req)
	if len(failures) == 0 {
		return ""
	}
	rules := wireServiceRules{style: wireStyleQuoted, missing: wireErrorCode{"X", 400}, invalid: wireErrorCode{"X", 400}}
	return rules.refusal(failures).Message
}

func wireFailuresForTest(req *AWSRequest) []wireFailure {
	op, ok := wireOperations[req.Service][req.Operation]
	if !ok || op.Input == 0 {
		return nil
	}
	view, ok := requestView(req, op)
	if !ok {
		return nil
	}
	w := wireWalker{rules: wireServiceRules{enum: true, pattern: true}, ec2: wireModels[op.Model].Protocol == "ec2"}
	w.structure(view, op.Input, "")
	if w.unreadable {
		return nil
	}
	if w.failures == nil {
		return []wireFailure{}
	}
	return w.failures
}

// WireViewReadsForTest reads a map's entry count and a blob's length through each adapter, for
// the reads no routed operation's constraints reach today: a Query map and blob under prefix
// "M"/"B", an XML body's <M> map and <B> blob, and a CBOR blob.
func WireViewReadsForTest(params map[string]string, xmlBody []byte, cborBlob []byte) (queryEntries, queryBlob, xmlEntries, xmlBlob, cborLen int) {
	mapShape := &wireShape{Kind: wireMap}
	q := queryView{params: params}
	if v, ok := q.field(&wireMember{Name: "M"}, mapShape); ok {
		queryEntries = v.entries(mapShape)
	}
	if v, ok := q.field(&wireMember{Name: "B"}, &wireShape{Kind: wireBlob}); ok {
		queryBlob, _ = v.blobLen()
	}
	if root, err := parseXMLTree(xmlBody); err == nil {
		x := xmlView{nodes: []*xmlNode{root}}
		if v, ok := x.field(&wireMember{Name: "M"}, mapShape); ok {
			xmlEntries = v.entries(mapShape)
		}
		if v, ok := x.field(&wireMember{Name: "B"}, &wireShape{Kind: wireBlob}); ok {
			xmlBlob, _ = v.blobLen()
		}
	}
	cborLen, _ = jsonView{v: cborBlob}.blobLen()
	return queryEntries, queryBlob, xmlEntries, xmlBlob, cborLen
}

// WireRESTReadsForTest reads, from req through the REST adapter, a list bound to the query-string
// key "q", the header "H" as a scalar and as a blob, and the number of headers prefixed
// "x-meta-". A missing binding reads as -1, or "" for the header.
func WireRESTReadsForTest(req *AWSRequest) (list int, header string, headerBlob, prefixed int) {
	r := restView{req: req}
	listShape := &wireShape{Kind: wireList}
	list, headerBlob, prefixed = -1, -1, -1
	if v, ok := r.field(&wireMember{Name: "Q", LocationName: "q", Location: wireInQuery}, listShape); ok {
		list = len(v.elems(listShape))
	}
	if v, ok := r.field(&wireMember{Name: "H", Location: wireInHeader}, &wireShape{Kind: wireString}); ok {
		header, _ = v.scalar()
		if _, isStruct := v.field(&wireMember{Name: "X"}, nil); isStruct {
			header = "a header read as a structure"
		}
		if n, ok := v.blobLen(); ok {
			headerBlob = n
		}
	}
	mapShape := &wireShape{Kind: wireMap}
	if v, ok := r.field(&wireMember{Name: "M", LocationName: "x-meta-", Location: wireInHeaders}, mapShape); ok {
		prefixed = v.entries(mapShape)
	}
	return list, header, headerBlob, prefixed
}

// WireEC2InvalidForTest renders EC2's refusal for a value of parameter wire that breaks a
// constraint, a failure no operation of the EC2 slice can produce today.
func WireEC2InvalidForTest(value, wire string) (string, string) {
	err := validatedServices["ec2"].refusal([]wireFailure{{kind: wireNotInEnum, value: value, hasValue: true, wire: wire}})
	return err.Code, err.Message
}

// WireModelVersionForTest returns the vendored snapshot's version.
func WireModelVersionForTest() string { return wireModelVersion }

// WireValidatedServicesForTest returns the services request validation covers.
func WireValidatedServicesForTest() []string {
	out := make([]string, 0, len(validatedServices))
	for svc := range validatedServices {
		out = append(out, svc)
	}
	return out
}

// WireOperationCoveredForTest reports whether the generated tables hold plugin's operation.
func WireOperationCoveredForTest(plugin, op string) bool {
	_, ok := wireOperations[plugin][op]
	return ok
}

// WireRequireMemberFallbackForTest calls wireRequireMember for a service the tables do not
// validate, which must answer the plugin's own fallback.
func WireRequireMemberFallbackForTest() *AWSError {
	fallback := &AWSError{Code: "Fallback", Message: "the plugin's own", HTTPStatus: 400}
	return wireRequireMember("no-such-service", "Op", &AWSRequest{}, "Name", fallback)
}

// WireMissingAfterInvalidForTest renders ElastiCache's refusal for a length failure followed by
// a missing member.
func WireMissingAfterInvalidForTest() string {
	return validatedServices["elasticache"].refusal([]wireFailure{
		{kind: wireTooLong, path: "a", wire: "A", value: "x", hasValue: true, constraint: "Member must have length less than or equal to 0"},
		{kind: wireMissing, path: "b", wire: "B", constraint: "Member must not be null"},
	}).Message
}
