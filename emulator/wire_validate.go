package emulator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Request validation from the vendored service models (#1612).
//
// [validateRequest] checks a client's request against the constraints its operation's input
// shape publishes, before any plugin sees it: required members, string, blob, list and map
// lengths, and numeric ranges, plus enum values and patterns for a service that opts into
// them. The constraints are the tables cmd/gen-wire-model generates from botocore into
// wire_model_gen.go, and one interpreter walks them for every protocol through an [inputView]:
// a JSON or CBOR body, Query or EC2 form parameters, or a REST request's URI labels, query
// string, headers and body.
//
// Which services are validated, and the code, status and message each answers, is the table
// in wire_validate_errors.go. A request whose operation the tables do not cover, or whose body
// will not parse, passes untouched: the plugin's own refusal for an unreadable body is the
// service's published answer, and validation has nothing to add to it.

// wireFailureKind is the constraint a member failed.
type wireFailureKind uint8

const (
	// wireMissing is an absent required member.
	wireMissing wireFailureKind = iota + 1
	// wireTooShort is a length under the minimum.
	wireTooShort
	// wireTooLong is a length over the maximum.
	wireTooLong
	// wireTooSmall is a number under the range minimum.
	wireTooSmall
	// wireTooLarge is a number over the range maximum.
	wireTooLarge
	// wireNotInEnum is a value outside the enum.
	wireNotInEnum
	// wirePatternMismatch is a value the pattern does not match.
	wirePatternMismatch
	// wireElementInvalid is a list whose scalar elements fail their own constraints.
	wireElementInvalid
)

// wireFailure is one failed constraint.
type wireFailure struct {
	kind wireFailureKind
	// path is the member's path as AWS reports it: member names with the first letter
	// lowered, joined by dots, and a list element as <list>.<n>.member, counting from 1.
	path string
	// member is the failing member's name as the model spells it.
	member string
	// wire is the failing member's name as the request spelled it.
	wire string
	// value is the value as the message renders it, and empty when it renders none.
	value string
	// hasValue tells a rendered empty string from no rendering.
	hasValue bool
	// constraint is the message's constraint clause, "Member must ...".
	constraint string
}

// inputView reads one value of a parsed request: a structure, a list, a map or a scalar.
type inputView interface {
	// field returns member m of the structure this view reads, whose shape is sh, and
	// whether the request carries it. An explicit JSON null is not carried.
	field(m *wireMember, sh *wireShape) (inputView, bool)
	// elems returns the elements of the list this view reads, in order.
	elems(sh *wireShape) []inputView
	// entries returns the number of entries in the map this view reads.
	entries(sh *wireShape) int
	// scalar returns a scalar's text, and false when the value is not a scalar.
	scalar() (string, bool)
	// blobLen returns a blob's length in bytes, and false when it cannot be read as one.
	blobLen() (int, bool)
	// fits reports whether the value can be a value of kind. A view that cannot tell, because
	// its wire format does not type its values, answers true.
	fits(kind wireKind) bool
}

// validateRequest checks req against the input constraints of its operation, for a service
// [validatedServices] lists, and returns the service's refusal or nil.
func validateRequest(req *AWSRequest) *AWSError {
	rules, ok := validatedServices[req.Service]
	if !ok || !rules.covers(req.Operation) {
		return nil
	}
	op, ok := wireOperations[req.Service][req.Operation]
	if !ok || op.Input == 0 {
		return nil
	}
	view, ok := requestView(req, op)
	if !ok {
		return nil
	}
	w := wireWalker{rules: rules}
	w.structure(view, op.Input, "")
	if w.unreadable || len(w.failures) == 0 {
		return nil
	}
	return rules.refusal(w.failures)
}

// wireValidates reports whether [validateRequest] checks the request's operation, which is
// what decides whether a recorded event carries [wireModelVersion].
func wireValidates(req *AWSRequest) bool {
	rules, ok := validatedServices[req.Service]
	if !ok || !rules.covers(req.Operation) {
		return false
	}
	_, ok = wireOperations[req.Service][req.Operation]
	return ok
}

// requestView picks the adapter for the request's wire format — a Smithy-Protocol header for CBOR,
// a target or a JSON content type for JSON, form parameters otherwise — among the protocols the
// model serves (metadata.protocols), so CloudWatch, which serves CBOR, JSON and Query, is read in
// whichever its caller sent. A request in a format the model does not list is not validated: SQS's
// model lists JSON alone, and its Query form, whose parameter names the JSON model does not
// describe, is left to the plugin.
func requestView(req *AWSRequest, op wireOperation) (inputView, bool) {
	model := wireModels[op.Model]
	speaks := func(protocol string) bool { return slices.Contains(model.Protocols, protocol) }
	switch {
	case strings.EqualFold(headerValue(req.Headers, "Smithy-Protocol"), "rpc-v2-cbor"):
		if !speaks("smithy-rpc-v2-cbor") {
			return nil, false
		}
		v, err := cborDecode(req.Body)
		if err != nil {
			return nil, false
		}
		return jsonView{v: v}, true
	case speaks("rest-json"):
		return newRESTView(req, op, false)
	case speaks("rest-xml"):
		return newRESTView(req, op, true)
	case headerValue(req.Headers, "X-Amz-Target") != "" ||
		strings.Contains(headerValue(req.Headers, "Content-Type"), "x-amz-json"):
		if !speaks("json") {
			return nil, false
		}
		return newJSONView(req.Body)
	case !wireFormEncoded(req):
	case speaks("ec2"):
		return queryView{params: req.Params, ec2: true}, true
	case speaks("query"):
		return queryView{params: req.Params}, true
	}
	return nil, false
}

// wireFormEncoded reports whether a request is in the Query form: it names its Action, or its
// body is a form. Anything else is not read as Query, because an absent parameter would then read
// as a missing member.
func wireFormEncoded(req *AWSRequest) bool {
	return req.Params["Action"] != "" ||
		strings.Contains(headerValue(req.Headers, "Content-Type"), "application/x-www-form-urlencoded")
}

// headerValue reads a header case-insensitively, since an in-process caller may not
// canonicalize the key.
func headerValue(headers map[string]string, name string) string {
	if v, ok := headers[http.CanonicalHeaderKey(name)]; ok {
		return v
	}
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

// wireWalker collects failures while walking a request against its input shape.
type wireWalker struct {
	// unreadable is set when a value is of the wrong kind for its shape, a string where a
	// structure belongs; the walk stops and the request is left to the plugin, whose answer for
	// a body it cannot decode is the service's.
	unreadable bool
	rules      wireServiceRules
	failures   []wireFailure
}

// structure checks each constrained member of the structure at view.
func (w *wireWalker) structure(view inputView, shape int, path string) {
	sh := &wireShapes[shape]
	for i := range sh.Members {
		if w.unreadable {
			return
		}
		w.member(view, &sh.Members[i], path)
	}
}

// member checks one member of the structure at view, which is at path.
func (w *wireWalker) member(view inputView, m *wireMember, path string) {
	msh := &wireShapes[m.Shape]
	child := lowerFirst(m.Name)
	if path != "" {
		child = path + "." + child
	}
	v, present := view.field(m, msh)
	if !present {
		if m.Required {
			w.failures = append(w.failures, wireFailure{kind: wireMissing, path: child, member: m.Name,
				wire: wireName(m), constraint: "Member must not be null"})
		}
		return
	}
	w.value(v, m, m.Shape, child)
}

// wireCheckMember checks one top-level member of req's operation input exactly as the gate
// does — the same view, the same walk, the same refusal — and reports only failures of the given
// kinds, or of any kind when none is given. A plugin calls it for a check it keeps because a
// request the gate never sees reaches it (a stack's or a schedule's dispatch, or any request with
// the gate off), so the answer is the gate's byte for byte. A request the tables do not cover, or
// that cannot be read, returns nil, as the gate would.
func wireCheckMember(req *AWSRequest, name string, kinds ...wireFailureKind) *AWSError {
	rules, ok := validatedServices[req.Service]
	if !ok || !rules.covers(req.Operation) {
		return nil
	}
	op, ok := wireOperations[req.Service][req.Operation]
	if !ok || op.Input == 0 {
		return nil
	}
	view, ok := pluginView(req, op)
	if !ok {
		return nil
	}
	sh := &wireShapes[op.Input]
	for i := range sh.Members {
		if sh.Members[i].Name != name {
			continue
		}
		w := wireWalker{rules: rules}
		w.member(view, &sh.Members[i], "")
		if w.unreadable {
			return nil
		}
		var kept []wireFailure
		for _, f := range w.failures {
			if len(kinds) == 0 || slices.Contains(kinds, f.kind) {
				kept = append(kept, f)
			}
		}
		if len(kept) == 0 {
			return nil
		}
		return rules.refusal(kept)
	}
	return nil
}

// pluginView is [requestView] for a request a plugin holds, which may have been built in process
// without the headers a client sends: a JSON-protocol request with no target is read as JSON, and
// a Query one as its parameters.
func pluginView(req *AWSRequest, op wireOperation) (inputView, bool) {
	if view, ok := requestView(req, op); ok {
		return view, true
	}
	model := wireModels[op.Model]
	switch {
	case slices.Contains(model.Protocols, "json"):
		return newJSONView(req.Body)
	case slices.Contains(model.Protocols, "ec2"):
		return queryView{params: req.Params, ec2: true}, true
	case slices.Contains(model.Protocols, "query"):
		return queryView{params: req.Params}, true
	}
	return nil, false
}

// value checks one present value against its shape.
func (w *wireWalker) value(v inputView, m *wireMember, shape int, path string) {
	sh := &wireShapes[shape]
	if sh.Kind != 0 && !v.fits(sh.Kind) {
		w.unreadable = true
		return
	}
	fail := func(kind wireFailureKind, constraint string) {
		f := wireFailure{kind: kind, path: path, constraint: constraint}
		if m != nil {
			f.member, f.wire = m.Name, wireName(m)
		}
		f.value, f.hasValue = renderValue(v, sh)
		w.failures = append(w.failures, f)
	}
	switch sh.Kind {
	case wireStructure:
		w.structure(v, shape, path)
	case wireList:
		elems := v.elems(sh)
		w.length(len(elems), sh, fail)
		el := &wireShapes[sh.Member.Shape]
		if sh.Member.Shape == 0 {
			return
		}
		if el.Kind == wireStructure || el.Kind == wireList || el.Kind == wireMap {
			for i, e := range elems {
				if w.unreadable {
					return
				}
				w.value(e, nil, sh.Member.Shape, fmt.Sprintf("%s.%d.member", path, i+1))
			}
			return
		}
		// A list of scalars is reported once, at the list, naming every constraint its
		// element carries; see [wireElementConstraints].
		for _, e := range elems {
			sub := wireWalker{rules: w.rules}
			sub.value(e, nil, sh.Member.Shape, path)
			if sub.unreadable {
				w.unreadable = true
				return
			}
			if len(sub.failures) > 0 {
				fail(wireElementInvalid, "Member must satisfy constraint: ["+
					strings.Join(wireElementConstraints(el), ", ")+"]")
				return
			}
		}
	case wireMap:
		w.length(v.entries(sh), sh, fail)
	case wireString:
		text, ok := v.scalar()
		if !ok {
			return
		}
		w.length(utf8.RuneCountInString(text), sh, fail)
		if w.rules.enum && sh.Enum != 0 && !wireInEnum(text, wireEnums[sh.Enum]) {
			fail(wireNotInEnum, "Member must satisfy enum value set: ["+strings.Join(wireEnums[sh.Enum], ", ")+"]")
		}
		if w.rules.pattern && sh.Pattern != 0 {
			if re := wirePattern(sh.Pattern); re != nil && !re.MatchString(text) {
				fail(wirePatternMismatch, "Member must satisfy regular expression pattern: "+wirePatterns[sh.Pattern])
			}
		}
	case wireBlob:
		if n, ok := v.blobLen(); ok {
			w.length(n, sh, fail)
		}
	case wireInteger, wireFloat:
		text, ok := v.scalar()
		if !ok {
			return
		}
		// The range clauses are AWS's: "Value '3600000000000' at
		// 'changeBatch.changes.1.member.resourceRecordSet.tTL' failed to satisfy constraint: Member
		// must have value less than or equal to 2147483647" (Route 53, caddy-dns/route53#17) and
		// "Member must have value greater than or equal to 300" (SESv2,
		// hashicorp/terraform-provider-aws#40591).
		if sh.Min != "" && wireCompare(text, sh.Min) < 0 {
			fail(wireTooSmall, "Member must have value greater than or equal to "+sh.Min)
		}
		if sh.Max != "" && wireCompare(text, sh.Max) > 0 {
			fail(wireTooLarge, "Member must have value less than or equal to "+sh.Max)
		}
	case wireBoolean, wireTimestamp:
	}
}

// length checks a length against a string, blob, list or map shape's bounds.
func (w *wireWalker) length(n int, sh *wireShape, fail func(wireFailureKind, string)) {
	if sh.Min != "" {
		if min, err := strconv.Atoi(sh.Min); err == nil && n < min {
			fail(wireTooShort, "Member must have length greater than or equal to "+sh.Min)
		}
	}
	if sh.Max != "" {
		if max, err := strconv.Atoi(sh.Max); err == nil && n > max {
			fail(wireTooLong, "Member must have length less than or equal to "+sh.Max)
		}
	}
}

// wireElementConstraints lists the constraints a list's scalar element carries, in the order
// AWS's messages list them: maximum length, minimum length, then pattern. That order is the
// one every published instance of the "Member must satisfy constraint: [...]" form shows, for
// example Lambda's "Value '[]' at 'layers' failed to satisfy constraint: Member must satisfy
// constraint: [Member must have length less than or equal to 2048, Member must have length
// greater than or equal to 1, Member must satisfy regular expression pattern: …]"
// (appleboy/lambda-action#83) and CloudFormation's at 'typeNameList' (AWS re:Post,
// "typeNameList failed to satisfy constraint"). The pattern is listed whether or not the
// service opts into pattern checks, because the clause describes the element's constraints
// rather than the one that failed. An enum, which no published instance shows, follows the
// pattern.
func wireElementConstraints(el *wireShape) []string {
	var out []string
	switch el.Kind {
	case wireInteger, wireFloat:
		if el.Max != "" {
			out = append(out, "Member must have value less than or equal to "+el.Max)
		}
		if el.Min != "" {
			out = append(out, "Member must have value greater than or equal to "+el.Min)
		}
	default:
		if el.Max != "" {
			out = append(out, "Member must have length less than or equal to "+el.Max)
		}
		if el.Min != "" {
			out = append(out, "Member must have length greater than or equal to "+el.Min)
		}
	}
	if el.Pattern != 0 {
		out = append(out, "Member must satisfy regular expression pattern: "+wirePatterns[el.Pattern])
	}
	if el.Enum != 0 {
		out = append(out, "Member must satisfy enum value set: ["+strings.Join(wireEnums[el.Enum], ", ")+"]")
	}
	return out
}

// renderValue renders a failing value for the message: a scalar's text, or a list of scalars
// as [a, b]. Anything else renders nothing, and the message omits the value.
func renderValue(v inputView, sh *wireShape) (string, bool) {
	switch sh.Kind {
	case wireStructure, wireMap, wireBlob:
		return "", false
	case wireList:
		el := &wireShapes[sh.Member.Shape]
		if sh.Member.Shape != 0 && (el.Kind == wireStructure || el.Kind == wireList || el.Kind == wireMap) {
			return "", false
		}
		var parts []string
		for _, e := range v.elems(sh) {
			text, ok := e.scalar()
			if !ok {
				return "", false
			}
			parts = append(parts, text)
		}
		return "[" + strings.Join(parts, ", ") + "]", true
	}
	return v.scalar()
}

func wireInEnum(value string, values []string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// wireCompare compares two decimal numbers exactly, so a long's bound such as
// 9223372036854775807 is not rounded through a float64. A value that does not parse compares
// equal, which raises no failure: a malformed number is the plugin's to refuse.
func wireCompare(value, bound string) int {
	a, ok1 := new(big.Rat).SetString(value)
	b, ok2 := new(big.Rat).SetString(bound)
	if !ok1 || !ok2 {
		return 0
	}
	return a.Cmp(b)
}

// wirePattern returns a model pattern compiled, or nil for one Go's RE2 cannot compile —
// botocore's are Java expressions, and lookaround such as (?!-) has no RE2 form — which is not
// checked.
func wirePattern(idx int) *regexp.Regexp {
	return wireCompiledPatterns()[idx]
}

// wireCompiledPatterns compiles every model pattern on first use, anchored at both ends because
// a model's pattern constrains the whole value. The slice is built once and never written again.
var wireCompiledPatterns = sync.OnceValue(func() []*regexp.Regexp { //nolint:gochecknoglobals // immutable once built
	out := make([]*regexp.Regexp, len(wirePatterns))
	for i, p := range wirePatterns {
		if i == 0 {
			continue
		}
		if re, err := regexp.Compile("^(?:" + p + ")$"); err == nil {
			out[i] = re
		}
	}
	return out
})

// wireName is a member's serialized name outside the EC2 protocol.
func wireName(m *wireMember) string {
	if m.LocationName != "" {
		return m.LocationName
	}
	return m.Name
}

// ----- JSON and CBOR -------------------------------------------------------

// jsonView reads a decoded JSON or CBOR value. A JSON document is decoded with UseNumber, so
// a number is a [json.Number]; a CBOR one carries int64, float64 and []byte as well.
type jsonView struct {
	v any
}

// newJSONView decodes a JSON body. An empty body is an empty object, which is what a JSON
// client sends for an operation with no input members set.
func newJSONView(body []byte) (inputView, bool) {
	if len(bytes.TrimSpace(body)) == 0 {
		return jsonView{v: map[string]any{}}, true
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, false
	}
	return jsonView{v: v}, true
}

func (j jsonView) field(m *wireMember, _ *wireShape) (inputView, bool) {
	obj, ok := j.v.(map[string]any)
	if !ok {
		return nil, false
	}
	v, ok := obj[wireName(m)]
	if !ok || v == nil {
		return nil, false
	}
	return jsonView{v: v}, true
}

func (j jsonView) elems(*wireShape) []inputView {
	arr, ok := j.v.([]any)
	if !ok {
		return nil
	}
	out := make([]inputView, len(arr))
	for i, e := range arr {
		out[i] = jsonView{v: e}
	}
	return out
}

func (j jsonView) entries(*wireShape) int {
	obj, ok := j.v.(map[string]any)
	if !ok {
		return 0
	}
	return len(obj)
}

func (j jsonView) fits(kind wireKind) bool {
	switch t := j.v.(type) {
	case map[string]any:
		return kind == wireStructure || kind == wireMap
	case []any:
		return kind == wireList
	case string:
		if kind == wireFloat {
			// A JSON float that is not a number travels as one of these strings (Smithy's JSON
			// protocols); it carries no range to compare.
			return t == "NaN" || t == "Infinity" || t == "-Infinity"
		}
		return kind == wireString || kind == wireBlob || kind == wireTimestamp
	case []byte:
		return kind == wireBlob
	case json.Number, int64, float64:
		return kind == wireInteger || kind == wireFloat || kind == wireTimestamp
	case bool:
		return kind == wireBoolean
	}
	return true
}

func (j jsonView) scalar() (string, bool) {
	switch t := j.v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case int64:
		return strconv.FormatInt(t, 10), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	}
	return "", false
}

func (j jsonView) blobLen() (int, bool) {
	switch t := j.v.(type) {
	case []byte:
		return len(t), true
	case string:
		b, err := base64.StdEncoding.DecodeString(t)
		if err != nil {
			return 0, false
		}
		return len(b), true
	}
	return 0, false
}

// ----- Query and EC2 -------------------------------------------------------

// queryView reads a value of a Query or EC2 request's form parameters: the value whose
// parameters begin with key. The rules are botocore's serializers': a structure member is
// key.Member, a list element key.member.N (key.N when flattened, and always on EC2), and a
// map entry key.entry.N.key and .value.
type queryView struct {
	params map[string]string
	key    string
	ec2    bool
	// flat is the reference's own flattened trait, which a list or map shape's may add to.
	flat bool
}

// queryWireName is a member's parameter name: the model's locationName, or on EC2 its
// queryName or its locationName with the first letter raised.
func queryWireName(m *wireMember, ec2 bool) string {
	if !ec2 {
		return wireName(m)
	}
	if m.QueryName != "" {
		return m.QueryName
	}
	if m.LocationName != "" {
		return strings.ToUpper(m.LocationName[:1]) + m.LocationName[1:]
	}
	return m.Name
}

func (q queryView) child(name string) string {
	if q.key == "" {
		return name
	}
	return q.key + "." + name
}

// present reports whether any parameter is key itself or lies under it.
func (q queryView) present(key string) bool {
	if _, ok := q.params[key]; ok {
		return true
	}
	prefix := key + "."
	for k := range q.params {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func (q queryView) field(m *wireMember, _ *wireShape) (inputView, bool) {
	key := q.child(queryWireName(m, q.ec2))
	if !q.present(key) {
		return nil, false
	}
	return queryView{params: q.params, key: key, ec2: q.ec2, flat: m.Flat}, true
}

// indexes returns the distinct element indexes under base, ascending.
func (q queryView) indexes(base string) []int {
	prefix := base + "."
	seen := map[int]bool{}
	for k := range q.params {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		if dot := strings.IndexByte(rest, '.'); dot >= 0 {
			rest = rest[:dot]
		}
		if n, err := strconv.Atoi(rest); err == nil && n > 0 {
			seen[n] = true
		}
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

func (q queryView) elems(sh *wireShape) []inputView {
	base := q.key
	switch {
	case q.ec2:
	case q.flat || sh.Flat:
		if name := sh.Member.LocationName; name != "" {
			if dot := strings.LastIndexByte(base, '.'); dot >= 0 {
				base = base[:dot+1] + name
			} else {
				base = name
			}
		}
	default:
		name := sh.Member.LocationName
		if name == "" {
			name = "member"
		}
		base += "." + name
	}
	idx := q.indexes(base)
	out := make([]inputView, len(idx))
	for i, n := range idx {
		out[i] = queryView{params: q.params, key: base + "." + strconv.Itoa(n), ec2: q.ec2}
	}
	return out
}

func (q queryView) entries(sh *wireShape) int {
	base := q.key
	if !q.ec2 && !q.flat && !sh.Flat {
		base += ".entry"
	}
	return len(q.indexes(base))
}

func (q queryView) fits(wireKind) bool { return true }

func (q queryView) scalar() (string, bool) {
	v, ok := q.params[q.key]
	return v, ok
}

func (q queryView) blobLen() (int, bool) {
	v, ok := q.params[q.key]
	if !ok {
		return 0, false
	}
	b, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return 0, false
	}
	return len(b), true
}

// ----- REST ----------------------------------------------------------------

// restView reads a REST request's input structure: each member from where its location
// binds it — a URI label, a query-string parameter, a header, a prefixed set of headers —
// and the rest from the body.
type restView struct {
	// leafView answers the list, map and scalar reads, which a structure has none of.
	leafView
	req    *AWSRequest
	labels map[string]string
	body   inputView // the body's structure, or nil when the body is empty or is a payload
	// payloadName is the member an input's payload trait binds to the whole body, and payload
	// that member's value, nil when the body is empty.
	payloadName string
	payload     inputView
}

// newRESTView matches the request's path against the operation's URI template, for the URI
// labels, and parses the body as JSON or XML. A path the template does not match, or a body
// that does not parse, is not validated.
func newRESTView(req *AWSRequest, op wireOperation, isXML bool) (inputView, bool) {
	labels, ok := matchURITemplate(op.URI, req.Path)
	if !ok {
		return nil, false
	}
	in := &wireShapes[op.Input]
	rv := restView{req: req, labels: labels, payloadName: in.Payload}
	if len(bytes.TrimSpace(req.Body)) == 0 {
		return rv, true
	}
	if in.Payload != "" {
		// The body is the payload member. A blob or string payload is the bytes themselves,
		// and a payload member the tables do not hold carries nothing to check, so neither
		// body is parsed.
		var payload *wireMember
		for i := range in.Members {
			if in.Members[i].Name == in.Payload {
				payload = &in.Members[i]
			}
		}
		if payload == nil {
			return rv, true
		}
		if k := wireShapes[payload.Shape].Kind; k == wireBlob || k == wireString {
			rv.payload = leafView{raw: req.Body, isRaw: true}
			return rv, true
		}
	}
	var body inputView
	if isXML {
		node, err := parseXMLTree(req.Body)
		if err != nil {
			return nil, false
		}
		body = xmlView{nodes: []*xmlNode{node}}
	} else {
		v, ok := newJSONView(req.Body)
		if !ok {
			return nil, false
		}
		body = v
	}
	if in.Payload != "" {
		rv.payload = body
	} else {
		rv.body = body
	}
	return rv, true
}

// matchURITemplate matches path against a model requestUri, returning the labels. A label
// {Name} matches one path segment and {Name+} the rest of the path; the template's own query
// string, which names a static parameter rather than a label, is ignored.
func matchURITemplate(template, path string) (map[string]string, bool) {
	if i := strings.IndexByte(template, '?'); i >= 0 {
		template = template[:i]
	}
	tsegs := strings.Split(strings.Trim(template, "/"), "/")
	psegs := strings.Split(strings.Trim(path, "/"), "/")
	labels := map[string]string{}
	for i, t := range tsegs {
		if strings.HasPrefix(t, "{") && strings.HasSuffix(t, "+}") {
			if i >= len(psegs) {
				return nil, false
			}
			labels[t[1:len(t)-2]] = strings.Join(psegs[i:], "/")
			return labels, true
		}
		if i >= len(psegs) {
			return nil, false
		}
		if strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}") {
			labels[t[1:len(t)-1]] = psegs[i]
			continue
		}
		if t != psegs[i] {
			return nil, false
		}
	}
	if len(psegs) != len(tsegs) {
		return nil, false
	}
	return labels, true
}

func (r restView) field(m *wireMember, sh *wireShape) (inputView, bool) {
	name := wireName(m)
	switch m.Location {
	case wireInBody:
		// Read from the body, below.
	case wireInURI:
		v, ok := r.labels[name]
		if !ok {
			return nil, false
		}
		return leafView{text: v, isText: true}, true
	case wireInQuery:
		if vs, ok := r.req.MultiValueParams[name]; ok {
			return leafView{list: vs}, true
		}
		v, ok := r.req.Params[name]
		if !ok {
			return nil, false
		}
		if sh.Kind == wireList {
			return leafView{list: []string{v}}, true
		}
		return leafView{text: v, isText: true}, true
	case wireInHeader:
		for k, v := range r.req.Headers {
			if strings.EqualFold(k, name) {
				return leafView{text: v, isText: true}, true
			}
		}
		return nil, false
	case wireInHeaders:
		n := 0
		for k := range r.req.Headers {
			if len(k) >= len(name) && strings.EqualFold(k[:len(name)], name) {
				n++
			}
		}
		if n == 0 {
			return nil, false
		}
		return leafView{count: n}, true
	}
	if r.payloadName != "" && m.Name == r.payloadName {
		if r.payload == nil {
			return nil, false
		}
		return r.payload, true
	}
	if r.body == nil {
		return nil, false
	}
	return r.body.field(m, sh)
}

// leafView is a value a REST request binds outside the body: a URI label, query-string value
// or header (text), a list bound to a repeated query-string key (list), the count of headers
// sharing a map's prefix (count), or a blob or string payload, which is the body's bytes (raw).
// The structure reads answer absent, because nothing bound there is a structure.
type leafView struct {
	text   string
	list   []string
	count  int
	raw    []byte
	isText bool
	isRaw  bool
}

func (l leafView) field(*wireMember, *wireShape) (inputView, bool) { return nil, false }

func (l leafView) elems(*wireShape) []inputView {
	out := make([]inputView, len(l.list))
	for i, v := range l.list {
		out[i] = leafView{text: v, isText: true}
	}
	return out
}

func (l leafView) entries(*wireShape) int { return l.count }

func (l leafView) fits(wireKind) bool { return true }

func (l leafView) scalar() (string, bool) {
	if l.isRaw {
		return string(l.raw), true
	}
	return l.text, l.isText
}

func (l leafView) blobLen() (int, bool) {
	if l.isRaw {
		return len(l.raw), true
	}
	b, err := base64.StdEncoding.DecodeString(l.text)
	if err != nil || !l.isText {
		return 0, false
	}
	return len(b), true
}

// ----- XML -----------------------------------------------------------------

// xmlNode is one element of an XML body.
type xmlNode struct {
	name     string
	text     string
	children []*xmlNode
}

// parseXMLTree parses an XML body into its root element.
func parseXMLTree(body []byte) (*xmlNode, error) {
	dec := xml.NewDecoder(bytes.NewReader(body))
	var stack []*xmlNode
	var root *xmlNode
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse xml body: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			n := &xmlNode{name: t.Name.Local}
			if len(stack) > 0 {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, n)
			} else if root == nil {
				root = n
			}
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			if len(stack) > 0 {
				stack[len(stack)-1].text += string(t)
			}
		}
	}
	if root == nil {
		return nil, errors.New("parse xml body: no root element")
	}
	return root, nil
}

// xmlView reads a value of an XML body. A structure or scalar is one element; a flattened list
// is the run of sibling elements that carry its name.
type xmlView struct {
	nodes []*xmlNode
	// flat is the reference's own flattened trait.
	flat bool
}

func (x xmlView) one() *xmlNode {
	if len(x.nodes) == 0 {
		return nil
	}
	return x.nodes[0]
}

func (x xmlView) field(m *wireMember, sh *wireShape) (inputView, bool) {
	parent := x.one()
	if parent == nil {
		return nil, false
	}
	// A flattened list's elements carry the member's own serialized name, as botocore's REST-XML
	// serializer writes them; the list member's locationName names a wrapped list's elements.
	name := wireName(m)
	if sh.Kind == wireList && (m.Flat || sh.Flat) {
		var run []*xmlNode
		for _, c := range parent.children {
			if c.name == name {
				run = append(run, c)
			}
		}
		if len(run) == 0 {
			return nil, false
		}
		return xmlView{nodes: run, flat: true}, true
	}
	for _, c := range parent.children {
		if c.name == name {
			return xmlView{nodes: []*xmlNode{c}, flat: m.Flat}, true
		}
	}
	return nil, false
}

func (x xmlView) elems(sh *wireShape) []inputView {
	var nodes []*xmlNode
	if x.flat || sh.Flat {
		nodes = x.nodes
	} else if wrapper := x.one(); wrapper != nil {
		name := sh.Member.LocationName
		if name == "" {
			name = "member"
		}
		for _, c := range wrapper.children {
			if c.name == name {
				nodes = append(nodes, c)
			}
		}
	}
	out := make([]inputView, len(nodes))
	for i, n := range nodes {
		out[i] = xmlView{nodes: []*xmlNode{n}}
	}
	return out
}

func (x xmlView) entries(sh *wireShape) int {
	if x.flat || sh.Flat {
		return len(x.nodes)
	}
	wrapper := x.one()
	if wrapper == nil {
		return 0
	}
	n := 0
	for _, c := range wrapper.children {
		if c.name == "entry" {
			n++
		}
	}
	return n
}

func (x xmlView) fits(wireKind) bool { return true }

func (x xmlView) scalar() (string, bool) {
	n := x.one()
	if n == nil {
		return "", false
	}
	return n.text, true
}

func (x xmlView) blobLen() (int, bool) {
	n := x.one()
	if n == nil {
		return 0, false
	}
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(n.text))
	if err != nil {
		return 0, false
	}
	return len(b), true
}
