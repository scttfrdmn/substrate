package emulator

// Response-body comparison for replay verification (#817).
//
// Until this file existed, [ReplayEngine.replayEvent] compared a replayed
// response against its recording at the granularity of the status code and the
// error string, and carried a TODO where the body comparison belonged. Two runs
// that agreed on 200 and differed on every value inside the document were
// reported as matching — which is the weakest place for the verification to be
// coarse, because a regression inside a body is exactly what a replay is meant to
// catch and is invisible to a state hash whenever the operation is a read.
//
// # Why this is a tree walk and not bytes.Equal
//
// The deliverable #817 asks for is "a comparison with a stated normalisation
// policy and a diff a human can read". bytes.Equal gives neither: it cannot say
// *where* two documents diverge, and it reports a difference for a body that a
// caller's SDK would decode identically. So each body is parsed into a tree and
// walked, and every divergence names its own path.
//
// # The normalisation policy, stated in full
//
// Three normalisations are applied, and nothing else:
//
//  1. **JSON member order is ignored.** A JSON object is a map by definition, so
//     two orderings of one object are the same document to every SDK that decodes
//     it. This is not a concession — it is what the format means.
//  2. **XML whitespace between elements is ignored.** Character data is compared
//     after trimming, and whitespace-only text on an element that has child
//     elements is dropped entirely, because it is indentation rather than content.
//     Non-whitespace text is always compared.
//  3. **A body that parses as neither JSON nor XML is compared as bytes.** That
//     covers rpc-v2-cbor, an S3 object payload, and an empty body. The path
//     reported is the whole body, because there is no tree to name a position in.
//
// Everything else is compared as it stands, and in particular:
//
//   - **Collection order is compared, not normalised.** An array element and an
//     XML sibling are matched by position. Sorting either side before comparing
//     would hide the class of defect #864 fixed (a listing rendered in Go map
//     order), which is precisely a body that differs between two runs of one
//     input. A reordered collection is a divergence and is reported as one.
//   - **No identifier is normalised.** A resource id or ARN minted during the
//     recording appears in the body, and a replay that mints a different one has
//     not reproduced the run. Masking it would make the verification report
//     success for the single largest known source of replay divergence; see
//     [replayPrincipal] and #856. This is the reason a recording that creates a
//     resource reports body differences today — an honest report of an open
//     defect, not a defect in this comparison.
//   - **No timestamp is normalised.** [ReplayEngine.replayEvent] sets the
//     simulated clock to the recorded event's timestamp before re-executing, so a
//     body rendered from that clock is expected to match. One that does not is a
//     finding about the clock — [TimeController.Now] advances by wall-clock
//     elapsed since the last SetTime — and not something to paper over here.
//
// # Path syntax
//
// Each format's own standard is used, rather than one invented syntax that
// matches neither: **JSON Pointer** (RFC 6901) for JSON, so members are
// slash-separated and array indices are zero-based, and **XPath**-style for XML,
// so a repeated sibling carries a one-based `[n]` and an attribute carries `@`.
// The indices therefore differ in base between the two formats, which is the
// price of each path being one a reader can paste into the tool that format
// already has. Both are rooted with a leading slash, matching
// [CFNPropertyDiff.PropertyPath]'s existing convention in this package.

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"
)

// replayBodyDiffLimit caps the number of differences reported for a single
// response body.
//
// A bound is needed because a divergence near the root of a large document — a
// listing whose members all differ, say — produces one difference per leaf, and a
// stream of such events would fill [ReplayResults.Differences] with thousands of
// entries reporting one fact. The bound is not silent: when it is reached the
// walk stops and appends [replayBodyDiffTruncated], so a reader is told the
// comparison was cut short rather than shown a partial list that looks complete.
const replayBodyDiffLimit = 20

// replayBodyDiffTruncated is the Actual value of the difference appended when a
// body comparison stops at [replayBodyDiffLimit].
const replayBodyDiffTruncated = "comparison stopped: more differences remain in this body"

// bodyDifference is one divergence between a recorded response body and the body
// a replay produced, located by path.
//
// It is not an [EventDifference] because it knows nothing about the event: the
// walk produces paths and values, and [ReplayEngine.replayEvent] attaches the
// event id, sequence and operation. Keeping the two apart is what lets the walk
// be tested on a pair of bodies alone.
type bodyDifference struct {
	// path locates the divergence within the body, empty for the body as a whole.
	path string

	// expected is the value the recording carries at path, nil when it has none.
	expected interface{}

	// actual is the value the replay produced at path, nil when it produced none.
	actual interface{}
}

// bodyMemberAbsent is the value reported for a side of the comparison that has no
// member, element or attribute at a path.
//
// A marker rather than nil, because a JSON null is a value a body can legitimately
// carry: reporting an absent member and a null one as the same nil would collapse
// two different divergences into one indistinguishable difference, and one of them
// would read as a difference between nil and nil.
const bodyMemberAbsent = "<absent>"

// bodyDifferenceField renders a [bodyDifference] path as an
// [EventDifference.Field] value.
//
// The "response_body" prefix distinguishes a body difference from the fields the
// other comparisons report ("status_code", "error", "state_hash_after"), so a
// caller filtering [ReplayResults.Differences] by field can separate them without
// parsing. A path already starts with a slash, so the two concatenate.
func bodyDifferenceField(path string) string {
	return "response_body" + path
}

// responseBodyDifferences compares the recorded response body against the body a
// replay produced.
//
// Both responses must be non-nil; the caller checks that, because a nil recorded
// response means the recording was refused before any plugin ran and the error
// comparison has already reported it — comparing a body against nothing would
// report the same fact a second time under a different field.
func responseBodyDifferences(recorded, replayed *AWSResponse) []bodyDifference {
	if bytes.Equal(recorded.Body, replayed.Body) {
		return nil
	}

	switch {
	case bodySniffsJSON(recorded.Body) && bodySniffsJSON(replayed.Body):
		return jsonBodyDifferences(recorded.Body, replayed.Body)
	case bodySniffsXML(recorded.Body) && bodySniffsXML(replayed.Body):
		return xmlBodyDifferences(recorded.Body, replayed.Body)
	default:
		return []bodyDifference{opaqueBodyDifference(recorded.Body, replayed.Body)}
	}
}

// bodySniffsJSON reports whether body's first non-space byte opens a JSON object
// or array.
//
// Sniffing the body beats reading the Content-Type header: a header can be absent
// or wrong, the two sides can disagree about it, and the question here is only
// which parser to try. A body that sniffs as JSON but does not parse falls back to
// a byte comparison, so a wrong guess costs precision, never correctness.
func bodySniffsJSON(body []byte) bool {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	return len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[')
}

// bodySniffsXML reports whether body's first non-space byte opens an XML tag.
//
// A UTF-8 byte-order mark is trimmed along with the whitespace, because an XML
// document is allowed to open with one and [xml.Decoder] accepts it.
func bodySniffsXML(body []byte) bool {
	trimmed := bytes.TrimLeft(bytes.TrimPrefix(body, []byte("\uFEFF")), " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '<'
}

// opaqueBodyDifference reports two bodies that share no comparable structure as a
// single whole-body divergence.
//
// The rendering is a length and a bounded preview rather than the bodies
// themselves, because this path is reached for a CBOR document and for an S3
// object payload, either of which can be megabytes and neither of which is
// readable inline.
func opaqueBodyDifference(recorded, replayed []byte) bodyDifference {
	return bodyDifference{
		expected: opaqueBodyPreview(recorded),
		actual:   opaqueBodyPreview(replayed),
	}
}

// opaqueBodyPreviewLen is how many bytes of an unparseable body are shown.
const opaqueBodyPreviewLen = 64

// opaqueBodyPreview renders a body as its length and a bounded prefix.
func opaqueBodyPreview(body []byte) string {
	if len(body) == 0 {
		return "empty body"
	}
	preview := body
	suffix := ""
	if len(preview) > opaqueBodyPreviewLen {
		preview = preview[:opaqueBodyPreviewLen]
		suffix = "…"
	}
	return fmt.Sprintf("%d bytes: %q%s", len(body), preview, suffix)
}

// jsonBodyDifferences parses both bodies as JSON and walks them.
//
// A body that fails to parse falls back to a byte comparison rather than being
// reported as a parse error: the caller asked whether the two bodies agree, and a
// malformed document is still an answerable comparison.
func jsonBodyDifferences(recorded, replayed []byte) []bodyDifference {
	expected, expErr := decodeJSONBody(recorded)
	actual, actErr := decodeJSONBody(replayed)
	if expErr != nil || actErr != nil {
		return []bodyDifference{opaqueBodyDifference(recorded, replayed)}
	}

	w := &bodyWalker{}
	w.walkJSON("", expected, actual)
	return w.diffs
}

// decodeJSONBody decodes body into a generic tree.
//
// Numbers are decoded with [json.Decoder.UseNumber] so that a literal is compared
// as written. Decoding into float64 would report 1 and 1.0 as equal, which is a
// normalisation this file's policy does not grant: the two are different bytes on
// the wire, an SDK can distinguish them, and neither side has any reason to render
// one where it rendered the other.
func decodeJSONBody(body []byte) (interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var out interface{}
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode json body: %w", err)
	}
	// A trailing token means the body is not one document, so the comparison
	// would be over a prefix of it.
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("json body has trailing content")
	}
	return out, nil
}

// bodyWalker accumulates differences and enforces [replayBodyDiffLimit].
type bodyWalker struct {
	diffs     []bodyDifference
	truncated bool
}

// record appends one difference, or the truncation marker once the limit is hit.
func (w *bodyWalker) record(path string, expected, actual interface{}) {
	if w.truncated {
		return
	}
	if len(w.diffs) >= replayBodyDiffLimit {
		w.truncated = true
		w.diffs = append(w.diffs, bodyDifference{
			path:   path,
			actual: replayBodyDiffTruncated,
		})
		return
	}
	w.diffs = append(w.diffs, bodyDifference{path: path, expected: expected, actual: actual})
}

// done reports whether the walk should stop.
func (w *bodyWalker) done() bool { return w.truncated }

// walkJSON compares two decoded JSON values at path.
func (w *bodyWalker) walkJSON(path string, expected, actual interface{}) {
	if w.done() {
		return
	}

	switch exp := expected.(type) {
	case map[string]interface{}:
		act, ok := actual.(map[string]interface{})
		if !ok {
			w.record(path, renderJSONValue(expected), renderJSONValue(actual))
			return
		}
		for _, key := range jsonMemberUnion(exp, act) {
			expVal, expOK := exp[key]
			actVal, actOK := act[key]
			switch {
			case !actOK:
				w.record(path+"/"+jsonPointerToken(key), renderJSONValue(expVal), bodyMemberAbsent)
			case !expOK:
				w.record(path+"/"+jsonPointerToken(key), bodyMemberAbsent, renderJSONValue(actVal))
			default:
				w.walkJSON(path+"/"+jsonPointerToken(key), expVal, actVal)
			}
			if w.done() {
				return
			}
		}
	case []interface{}:
		act, ok := actual.([]interface{})
		if !ok {
			w.record(path, renderJSONValue(expected), renderJSONValue(actual))
			return
		}
		for i := 0; i < max(len(exp), len(act)); i++ {
			elemPath := fmt.Sprintf("%s/%d", path, i)
			switch {
			case i >= len(act):
				w.record(elemPath, renderJSONValue(exp[i]), bodyMemberAbsent)
			case i >= len(exp):
				w.record(elemPath, bodyMemberAbsent, renderJSONValue(act[i]))
			default:
				w.walkJSON(elemPath, exp[i], act[i])
			}
			if w.done() {
				return
			}
		}
	default:
		if expected != actual {
			w.record(path, renderJSONValue(expected), renderJSONValue(actual))
		}
	}
}

// jsonMemberUnion returns every member name in either object, sorted.
//
// Sorted so that two runs of one comparison report the same differences in the
// same order: ranging a map here would make the difference list itself depend on
// Go's map iteration order, which is the defect class this comparison exists to
// detect (#864).
func jsonMemberUnion(expected, actual map[string]interface{}) []string {
	seen := make(map[string]struct{}, len(expected)+len(actual))
	for k := range expected {
		seen[k] = struct{}{}
	}
	for k := range actual {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// jsonPointerToken escapes a member name for a JSON Pointer path, per RFC 6901:
// "~" becomes "~0" and "/" becomes "~1", in that order.
func jsonPointerToken(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")
}

// renderJSONValue renders a decoded value for an [EventDifference] field.
//
// A scalar is passed through, so a caller reading Expected and Actual gets the
// value rather than a quoted rendering of it. An object or array is rendered as
// compact JSON, because a difference reported at the root of a document has to be
// printable on one line and a nested map is not.
func renderJSONValue(v interface{}) interface{} {
	switch typed := v.(type) {
	case map[string]interface{}, []interface{}:
		encoded, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(encoded)
	case json.Number:
		return typed.String()
	default:
		return v
	}
}

// xmlBodyNode is one element of a parsed XML body.
type xmlBodyNode struct {
	name     xml.Name
	attrs    []xml.Attr
	text     string
	children []*xmlBodyNode
}

// xmlBodyDifferences parses both bodies as XML and walks them.
//
// As with JSON, a parse failure on either side falls back to a byte comparison
// rather than being reported as a parse error of its own.
func xmlBodyDifferences(recorded, replayed []byte) []bodyDifference {
	expected, expErr := parseXMLBody(recorded)
	actual, actErr := parseXMLBody(replayed)
	if expErr != nil || actErr != nil {
		return []bodyDifference{opaqueBodyDifference(recorded, replayed)}
	}

	w := &bodyWalker{}
	// The root's own step is built here rather than inside walkXML, so that
	// walkXML is handed the path of the element it is comparing and every child
	// step is built in exactly one place — compareXMLChildren, which is the only
	// caller that knows an element's ordinal among its siblings.
	if expected.name.Local != actual.name.Local {
		w.record("/"+expected.name.Local, expected.name.Local, actual.name.Local)
		return w.diffs
	}
	w.walkXML("/"+expected.name.Local, expected, actual)
	return w.diffs
}

// parseXMLBody decodes body into a tree of [xmlBodyNode].
//
// Processing instructions, comments and directives are dropped: none carries data
// an SDK decodes, and a difference in one is not a divergence in the response. A
// body with no root element is an error rather than an empty tree, so the caller
// falls back to a byte comparison instead of reporting every element of the other
// side as missing.
func parseXMLBody(body []byte) (*xmlBodyNode, error) {
	dec := xml.NewDecoder(bytes.NewReader(body))
	var root *xmlBodyNode
	var stack []*xmlBodyNode

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode xml body: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			node := &xmlBodyNode{name: t.Name, attrs: sortedXMLAttrs(t.Attr)}
			if len(stack) == 0 {
				if root != nil {
					return nil, fmt.Errorf("xml body has more than one root element")
				}
				root = node
			} else {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, node)
			}
			stack = append(stack, node)
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, fmt.Errorf("xml body closes an element that was not open")
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				node := stack[len(stack)-1]
				node.text += string(t)
			}
		}
	}

	if root == nil {
		return nil, fmt.Errorf("xml body has no root element")
	}
	return root, nil
}

// sortedXMLAttrs copies attrs into name order.
//
// Attribute order carries no meaning in XML, so it is normalised for the same
// reason JSON member order is: two orderings of one attribute set are the same
// document. Sorting also makes the difference list deterministic.
func sortedXMLAttrs(attrs []xml.Attr) []xml.Attr {
	out := make([]xml.Attr, len(attrs))
	copy(out, attrs)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name.Space != out[j].Name.Space {
			return out[i].Name.Space < out[j].Name.Space
		}
		return out[i].Name.Local < out[j].Name.Local
	})
	return out
}

// walkXML compares two parsed elements of the same local name. path is the path
// of the elements themselves, already carrying their step.
func (w *bodyWalker) walkXML(path string, expected, actual *xmlBodyNode) {
	if w.done() {
		return
	}

	// The resolved namespace is deliberately not compared element by element. Go's
	// decoder retains an xmlns declaration in Attr *and* resolves it onto every
	// descendant's Name.Space, so comparing both would report one changed default
	// namespace three times: once for the declaring element's Space, once for its
	// xmlns attribute, and once for every element beneath it. The attribute is the
	// one report that names where the change was made, and it also catches a change
	// of prefix that leaves the URI alone — which is a wire difference the resolved
	// Space cannot see.
	w.compareXMLAttrs(path, expected, actual)
	if w.done() {
		return
	}
	w.compareXMLText(path, expected, actual)
	if w.done() {
		return
	}
	w.compareXMLChildren(path, expected, actual)
}

// compareXMLAttrs reports attributes present on one element and not the other, or
// present on both with different values.
func (w *bodyWalker) compareXMLAttrs(path string, expected, actual *xmlBodyNode) {
	expAttrs := xmlAttrMap(expected.attrs)
	actAttrs := xmlAttrMap(actual.attrs)
	for _, name := range xmlAttrUnion(expAttrs, actAttrs) {
		expVal, expOK := expAttrs[name]
		actVal, actOK := actAttrs[name]
		switch {
		case !actOK:
			w.record(path+"/@"+name, expVal, bodyMemberAbsent)
		case !expOK:
			w.record(path+"/@"+name, bodyMemberAbsent, actVal)
		case expVal != actVal:
			w.record(path+"/@"+name, expVal, actVal)
		}
		if w.done() {
			return
		}
	}
}

// xmlAttrMap keys attributes by local name.
func xmlAttrMap(attrs []xml.Attr) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, a := range attrs {
		out[a.Name.Local] = a.Value
	}
	return out
}

// xmlAttrUnion returns every attribute name on either element, sorted, for the
// same determinism reason as [jsonMemberUnion].
func xmlAttrUnion(expected, actual map[string]string) []string {
	seen := make(map[string]struct{}, len(expected)+len(actual))
	for k := range expected {
		seen[k] = struct{}{}
	}
	for k := range actual {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// compareXMLText compares an element's own character data.
//
// Text is trimmed on both sides, and whitespace-only text is ignored on an
// element that has children: that text is the indentation between those children,
// and reporting it would make every difference in an indented document arrive
// twice. Whitespace-only text on a leaf is compared, because there the difference
// between an empty element and one holding a space is the value itself.
func (w *bodyWalker) compareXMLText(path string, expected, actual *xmlBodyNode) {
	expText := strings.TrimSpace(expected.text)
	actText := strings.TrimSpace(actual.text)
	if expText == actText {
		return
	}
	if len(expected.children) > 0 && len(actual.children) > 0 && expText == "" && actText == "" {
		return
	}
	w.record(path, expText, actText)
}

// compareXMLChildren compares child elements by position.
//
// Position, not name: an XML sibling list is ordered, AWS's own shapes rely on
// that order (a listing's members arrive in the order the operation documents),
// and matching by name would silently accept a reordered response. A repeated
// name carries a one-based XPath index so that a difference in the third of five
// members names the third.
func (w *bodyWalker) compareXMLChildren(path string, expected, actual *xmlBodyNode) {
	expOrdinals := xmlChildOrdinals(expected.children)
	actOrdinals := xmlChildOrdinals(actual.children)

	for i := 0; i < max(len(expected.children), len(actual.children)); i++ {
		switch {
		case i >= len(actual.children):
			child := expected.children[i]
			w.record(path+"/"+expOrdinals[i], renderXMLNode(child), bodyMemberAbsent)
		case i >= len(expected.children):
			child := actual.children[i]
			w.record(path+"/"+actOrdinals[i], bodyMemberAbsent, renderXMLNode(child))
		default:
			expChild, actChild := expected.children[i], actual.children[i]
			// A child that changed name is one difference at that position, not a
			// walk of two unrelated subtrees: comparing <Name> against <Value>
			// member by member would report every leaf beneath them.
			if expChild.name.Local != actChild.name.Local {
				w.record(path+"/"+expOrdinals[i], expChild.name.Local, actChild.name.Local)
			} else {
				w.walkXML(path+"/"+expOrdinals[i], expChild, actChild)
			}
		}
		if w.done() {
			return
		}
	}
}

// xmlChildOrdinals returns one path step per child: the local name alone when it
// is the only child of that name, and name[n] with a one-based index when it is
// not.
func xmlChildOrdinals(children []*xmlBodyNode) []string {
	counts := make(map[string]int, len(children))
	for _, c := range children {
		counts[c.name.Local]++
	}
	seen := make(map[string]int, len(children))
	out := make([]string, len(children))
	for i, c := range children {
		local := c.name.Local
		if counts[local] == 1 {
			out[i] = local
			continue
		}
		seen[local]++
		out[i] = fmt.Sprintf("%s[%d]", local, seen[local])
	}
	return out
}

// renderXMLNode renders an element that exists on only one side, for the Expected
// or Actual value of the difference reporting it.
//
// The rendering is the element's own text when it has no children, and its name
// with a child count when it does — enough to identify what is missing without
// rendering a whole subtree into a difference field.
func renderXMLNode(n *xmlBodyNode) string {
	if len(n.children) == 0 {
		return fmt.Sprintf("<%s>%s</%s>", n.name.Local, strings.TrimSpace(n.text), n.name.Local)
	}
	return fmt.Sprintf("<%s> with %d child element(s)", n.name.Local, len(n.children))
}
