package emulator

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// This file parses and applies DynamoDB update expressions (#1450). It replaces a
// string-splitting implementation that read `if_not_exists(a, :v)` as an attribute
// named "if_not_exists(a, :v)" (writing NULL), split `if_not_exists(x, :a) + :b` at the
// first '+', had no list_append, and wrote `SET a.b = :v` to a top-level attribute
// literally named "a.b". The grammar is the one the update-expression guide states:
//
//	update-expression ::= [SET action, ...] [REMOVE action, ...] [ADD action, ...] [DELETE action, ...]
//	set-action        ::= path = value
//	value             ::= operand | operand '+' operand | operand '-' operand
//	operand           ::= path | :value | function
//	function          ::= if_not_exists(path, value) | list_append(operand, operand)
//
// Every action reads the item "as it was before the update": operands are evaluated
// against a snapshot, so `REMOVE a SET b = a, c = b` moves values rather than chaining.

// ddbValidationError is an expression error whose text is DynamoDB's own message, which
// is capitalized and punctuated the way the service writes it; callers return it as a
// ValidationException.
type ddbValidationError struct{ msg string }

// Error returns DynamoDB's message.
func (e *ddbValidationError) Error() string { return e.msg }

// ddbInvalidf formats a [ddbValidationError].
func ddbInvalidf(format string, args ...any) error {
	return &ddbValidationError{msg: fmt.Sprintf(format, args...)}
}

// ddbPathSeg is one step of a DynamoDB document path: a map key or a list index.
type ddbPathSeg struct {
	name  string
	index int
	isIdx bool
}

// parseDDBPath reads a document path — `a`, `#n`, `a.b`, `a[0].b` — starting at
// tokens[pos], resolving expression attribute names. It returns the path and the
// position after it.
func parseDDBPath(tokens []dynamoToken, pos int, names map[string]string) ([]ddbPathSeg, int, error) {
	name := func(tok dynamoToken) (string, error) {
		switch tok.typ {
		case dynTokIdent:
			return tok.value, nil
		case dynTokExprName:
			if resolved, ok := names[tok.value]; ok {
				return resolved, nil
			}
			return "", ddbInvalidf("An expression attribute name used in the document path is not defined; attribute name: %s", tok.value)
		}
		return "", ddbInvalidf("Syntax error; expected an attribute path, got %q", tok.value)
	}
	at := func(i int) dynamoToken {
		if i < len(tokens) {
			return tokens[i]
		}
		return dynamoToken{dynTokEOF, ""}
	}

	first, err := name(at(pos))
	if err != nil {
		return nil, pos, err
	}
	path := []ddbPathSeg{{name: first}}
	pos++
	for {
		switch at(pos).typ {
		case dynTokDot:
			n, err := name(at(pos + 1))
			if err != nil {
				return nil, pos, err
			}
			path = append(path, ddbPathSeg{name: n})
			pos += 2
		case dynTokLBracket:
			idxTok := at(pos + 1)
			if idxTok.typ != dynTokNumber || at(pos+2).typ != dynTokRBracket {
				return nil, pos, ddbInvalidf("Syntax error; a list index must be a number in brackets")
			}
			idx, convErr := strconv.Atoi(idxTok.value)
			if convErr != nil {
				return nil, pos, ddbInvalidf("Syntax error; invalid list index %q", idxTok.value)
			}
			path = append(path, ddbPathSeg{index: idx, isIdx: true})
			pos += 3
		default:
			return path, pos, nil
		}
	}
}

// getAttrByPath returns the value at a document path, or nil when any step of the path
// is absent or of the wrong container type.
func getAttrByPath(item map[string]*AttributeValue, path []ddbPathSeg) *AttributeValue {
	if item == nil || len(path) == 0 || path[0].isIdx {
		return nil
	}
	cur := item[path[0].name]
	for _, seg := range path[1:] {
		if cur == nil {
			return nil
		}
		if seg.isIdx {
			if cur.L == nil || seg.index < 0 || seg.index >= len(cur.L) {
				return nil
			}
			cur = cur.L[seg.index]
			continue
		}
		if cur.M == nil {
			return nil
		}
		cur = cur.M[seg.name]
	}
	return cur
}

// ddbUpdateNodeKind classifies a node of a parsed SET value.
type ddbUpdateNodeKind int

const (
	ddbNodePath ddbUpdateNodeKind = iota
	ddbNodeValue
	ddbNodeIfNotExists
	ddbNodeListAppend
	ddbNodePlus
	ddbNodeMinus
)

// ddbUpdateNode is one node of a parsed SET value: an operand, a function call or an
// arithmetic operation.
type ddbUpdateNode struct {
	kind  ddbUpdateNodeKind
	path  []ddbPathSeg
	value *AttributeValue
	args  []*ddbUpdateNode
}

// ddbUpdateAction is one parsed action of an update expression.
type ddbUpdateAction struct {
	clause string // SET, REMOVE, ADD or DELETE
	path   []ddbPathSeg
	value  *ddbUpdateNode // SET's right-hand side; ADD and DELETE's value operand
}

// ddbUpdateParser is a recursive-descent parser over an update expression's tokens.
type ddbUpdateParser struct {
	tokens []dynamoToken
	pos    int
	names  map[string]string
	values map[string]*AttributeValue
}

func (u *ddbUpdateParser) peek() dynamoToken {
	if u.pos < len(u.tokens) {
		return u.tokens[u.pos]
	}
	return dynamoToken{dynTokEOF, ""}
}

// ddbClauseKeyword reports the clause a token opens, or "" if it opens none. SET, REMOVE,
// ADD and DELETE are DynamoDB reserved words, so a bare identifier spelled as one is
// always the keyword; an attribute with such a name has to be written as #name.
func ddbClauseKeyword(tok dynamoToken) string {
	if tok.typ != dynTokIdent {
		return ""
	}
	switch kw := strings.ToUpper(tok.value); kw {
	case "SET", "REMOVE", "ADD", "DELETE":
		return kw
	}
	return ""
}

// parseDDBUpdateExpression parses a whole update expression into its actions.
func parseDDBUpdateExpression(expr string, names map[string]string, values map[string]*AttributeValue) ([]ddbUpdateAction, error) {
	tokens, err := tokenizeDynamo(expr)
	if err != nil {
		return nil, ddbInvalidf("Invalid UpdateExpression: %v", err)
	}
	u := &ddbUpdateParser{tokens: tokens, names: names, values: values}
	seen := map[string]bool{}
	var actions []ddbUpdateAction
	for u.peek().typ != dynTokEOF {
		kw := ddbClauseKeyword(u.peek())
		if kw == "" {
			return nil, ddbInvalidf("Invalid UpdateExpression: Syntax error; token: %q", u.peek().value)
		}
		if seen[kw] {
			return nil, ddbInvalidf("Invalid UpdateExpression: The %q section can only be used once in an update expression;", kw)
		}
		seen[kw] = true
		u.pos++
		for {
			act, err := u.parseAction(kw)
			if err != nil {
				return nil, ddbInvalidf("Invalid UpdateExpression: %v", err)
			}
			actions = append(actions, act)
			if u.peek().typ != dynTokComma {
				break
			}
			u.pos++
		}
	}
	if len(actions) == 0 {
		return nil, ddbInvalidf("Invalid UpdateExpression: The expression can not be empty;")
	}
	return actions, nil
}

// parseAction parses one action of the given clause.
func (u *ddbUpdateParser) parseAction(clause string) (ddbUpdateAction, error) {
	path, pos, err := parseDDBPath(u.tokens, u.pos, u.names)
	if err != nil {
		return ddbUpdateAction{}, err
	}
	u.pos = pos
	act := ddbUpdateAction{clause: clause, path: path}
	switch clause {
	case "SET":
		if u.peek().typ != dynTokEQ {
			return act, ddbInvalidf("Syntax error; token: %q, expected \"=\"", u.peek().value)
		}
		u.pos++
		act.value, err = u.parseValue()
	case "ADD", "DELETE":
		act.value, err = u.parseExprValue()
	}
	return act, err
}

// parseValue parses `operand [(+|-) operand]`.
func (u *ddbUpdateParser) parseValue() (*ddbUpdateNode, error) {
	left, err := u.parseOperand()
	if err != nil {
		return nil, err
	}
	var kind ddbUpdateNodeKind
	switch u.peek().typ {
	case dynTokPlus:
		kind = ddbNodePlus
	case dynTokMinus:
		kind = ddbNodeMinus
	default:
		return left, nil
	}
	u.pos++
	right, err := u.parseOperand()
	if err != nil {
		return nil, err
	}
	return &ddbUpdateNode{kind: kind, args: []*ddbUpdateNode{left, right}}, nil
}

// parseExprValue parses a `:value` placeholder.
func (u *ddbUpdateParser) parseExprValue() (*ddbUpdateNode, error) {
	tok := u.peek()
	if tok.typ != dynTokExprValue {
		return nil, ddbInvalidf("Syntax error; token: %q, expected an expression attribute value", tok.value)
	}
	u.pos++
	av, ok := u.values[tok.value]
	if !ok {
		return nil, ddbInvalidf("An expression attribute value used in expression is not defined; attribute value: %s", tok.value)
	}
	return &ddbUpdateNode{kind: ddbNodeValue, value: av}, nil
}

// parseOperand parses a path, a `:value` or a function call.
func (u *ddbUpdateParser) parseOperand() (*ddbUpdateNode, error) {
	tok := u.peek()
	if tok.typ == dynTokExprValue {
		return u.parseExprValue()
	}
	// Function names are case-sensitive, as the guide states.
	if tok.typ == dynTokIdent && (tok.value == "if_not_exists" || tok.value == "list_append") &&
		u.pos+1 < len(u.tokens) && u.tokens[u.pos+1].typ == dynTokLParen {
		u.pos += 2
		var node *ddbUpdateNode
		if tok.value == "if_not_exists" {
			path, pos, err := parseDDBPath(u.tokens, u.pos, u.names)
			if err != nil {
				return nil, err
			}
			u.pos = pos
			if err := u.expect(dynTokComma, ","); err != nil {
				return nil, err
			}
			fallback, err := u.parseValue()
			if err != nil {
				return nil, err
			}
			node = &ddbUpdateNode{kind: ddbNodeIfNotExists, path: path, args: []*ddbUpdateNode{fallback}}
		} else {
			first, err := u.parseOperand()
			if err != nil {
				return nil, err
			}
			if err := u.expect(dynTokComma, ","); err != nil {
				return nil, err
			}
			second, err := u.parseOperand()
			if err != nil {
				return nil, err
			}
			node = &ddbUpdateNode{kind: ddbNodeListAppend, args: []*ddbUpdateNode{first, second}}
		}
		if err := u.expect(dynTokRParen, ")"); err != nil {
			return nil, err
		}
		return node, nil
	}
	path, pos, err := parseDDBPath(u.tokens, u.pos, u.names)
	if err != nil {
		return nil, err
	}
	u.pos = pos
	return &ddbUpdateNode{kind: ddbNodePath, path: path}, nil
}

// expect consumes one token of the given type or reports a syntax error.
func (u *ddbUpdateParser) expect(typ dynamoTokenType, want string) error {
	if u.peek().typ != typ {
		return ddbInvalidf("Syntax error; token: %q, expected %q", u.peek().value, want)
	}
	u.pos++
	return nil
}

// Messages DynamoDB returns for update expressions that are well-formed but cannot be
// applied to the item at hand.
const (
	ddbMsgAttrNotExist     = "The provided expression refers to an attribute that does not exist in the item"
	ddbMsgIncorrectType    = "An operand in the update expression has an incorrect data type"
	ddbMsgInvalidDocPath   = "The document path provided in the update expression is invalid for update"
	ddbMsgIncorrectOperand = "Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: %s, operand type: %s"
	ddbMsgIncorrectAddDel  = "Invalid UpdateExpression: Incorrect operand type for operator or function; operator: %s, operand type: %s"
)

// ddbLongTypeName spells a type the way DynamoDB's ADD/DELETE operand errors do.
func ddbLongTypeName(av *AttributeValue) string {
	switch t := avTypeName(av); t {
	case "S":
		return "STRING"
	case "N":
		return "NUMBER"
	case "B":
		return "BINARY"
	case "BOOL":
		return "BOOLEAN"
	case "L":
		return "LIST"
	case "M":
		return "MAP"
	default:
		return t
	}
}

// evalDDBUpdateNode evaluates a SET value against the pre-update snapshot of the item.
func evalDDBUpdateNode(n *ddbUpdateNode, snapshot map[string]*AttributeValue) (*AttributeValue, error) {
	switch n.kind {
	case ddbNodeValue:
		return n.value, nil
	case ddbNodePath:
		v := getAttrByPath(snapshot, n.path)
		if v == nil {
			return nil, ddbInvalidf(ddbMsgAttrNotExist)
		}
		return v, nil
	case ddbNodeIfNotExists:
		if v := getAttrByPath(snapshot, n.path); v != nil {
			return v, nil
		}
		return evalDDBUpdateNode(n.args[0], snapshot)
	case ddbNodeListAppend:
		a, err := evalDDBUpdateNode(n.args[0], snapshot)
		if err != nil {
			return nil, err
		}
		b, err := evalDDBUpdateNode(n.args[1], snapshot)
		if err != nil {
			return nil, err
		}
		for _, v := range []*AttributeValue{a, b} {
			if avTypeName(v) != "L" {
				return nil, ddbInvalidf(ddbMsgIncorrectOperand, "list_append", avTypeName(v))
			}
		}
		out := make([]*AttributeValue, 0, len(a.L)+len(b.L))
		out = append(out, a.L...)
		out = append(out, b.L...)
		return &AttributeValue{L: out}, nil
	case ddbNodePlus, ddbNodeMinus:
		a, err := evalDDBUpdateNode(n.args[0], snapshot)
		if err != nil {
			return nil, err
		}
		b, err := evalDDBUpdateNode(n.args[1], snapshot)
		if err != nil {
			return nil, err
		}
		ar, aok := ddbParseNumber(a.N)
		br, bok := ddbParseNumber(b.N)
		if avTypeName(a) != "N" || avTypeName(b) != "N" || !aok || !bok {
			return nil, ddbInvalidf(ddbMsgIncorrectType)
		}
		if n.kind == ddbNodePlus {
			return ddbNumberResult(ar.Add(ar, br))
		}
		return ddbNumberResult(ar.Sub(ar, br))
	}
	return nil, ddbInvalidf("Invalid UpdateExpression: unsupported operand")
}

// ddbNumberResult is the Number an arithmetic result r stores as, or DynamoDB's
// ValidationException when r exceeds a Number's precision or range.
func ddbNumberResult(r *big.Rat) (*AttributeValue, error) {
	n := ddbFormatNumber(r)
	if msg := ddbValidateNumber(n); msg != "" {
		return nil, ddbInvalidf("%s", msg)
	}
	return &AttributeValue{N: n}, nil
}

// setDDBPath writes v at path in item. A nested path needs its parent to exist and be
// of the right container type; a list index past the end appends.
func setDDBPath(item map[string]*AttributeValue, path []ddbPathSeg, v *AttributeValue) error {
	if len(path) == 1 {
		item[path[0].name] = v
		return nil
	}
	parent := getAttrByPath(item, path[:len(path)-1])
	last := path[len(path)-1]
	if parent == nil {
		return ddbInvalidf(ddbMsgInvalidDocPath)
	}
	if last.isIdx {
		if parent.L == nil {
			return ddbInvalidf(ddbMsgInvalidDocPath)
		}
		if last.index < len(parent.L) {
			parent.L[last.index] = v
		} else {
			parent.L = append(parent.L, v)
		}
		return nil
	}
	if parent.M == nil {
		return ddbInvalidf(ddbMsgInvalidDocPath)
	}
	parent.M[last.name] = v
	return nil
}

// applyDDBRemovals removes every path in removals from item. A list element's index
// refers to the list as it was before the update, so the indexed removals of each
// list are grouped by the list's path and applied from the highest index down, each
// index once: `REMOVE l[0], b, l[2]` on [0,1,2,3] leaves [1,3]. Every other removal
// is order-independent.
func applyDDBRemovals(item map[string]*AttributeValue, removals [][]ddbPathSeg) {
	type listRemovals struct {
		parent  []ddbPathSeg
		indexes []int
	}
	var lists []*listRemovals
	byParent := map[string]*listRemovals{}
	for _, path := range removals {
		last := path[len(path)-1]
		if !last.isIdx {
			removeDDBPath(item, path)
			continue
		}
		key := ddbPathKey(path[:len(path)-1])
		lr, ok := byParent[key]
		if !ok {
			lr = &listRemovals{parent: path[:len(path)-1]}
			byParent[key] = lr
			lists = append(lists, lr)
		}
		lr.indexes = append(lr.indexes, last.index)
	}
	for _, lr := range lists {
		sort.Sort(sort.Reverse(sort.IntSlice(lr.indexes)))
		for i, idx := range lr.indexes {
			if i > 0 && idx == lr.indexes[i-1] {
				continue
			}
			path := append(append([]ddbPathSeg(nil), lr.parent...), ddbPathSeg{index: idx, isIdx: true})
			removeDDBPath(item, path)
		}
	}
}

// ddbPathKey renders a document path as a string that identifies it, for grouping.
// A map key and a list index cannot render alike, because a key is written after a
// NUL and an index in brackets.
func ddbPathKey(path []ddbPathSeg) string {
	var b strings.Builder
	for _, seg := range path {
		if seg.isIdx {
			b.WriteString("[" + strconv.Itoa(seg.index) + "]")
			continue
		}
		b.WriteString("\x00" + seg.name)
	}
	return b.String()
}

// removeDDBPath deletes the value at path. Removing an absent attribute does nothing.
func removeDDBPath(item map[string]*AttributeValue, path []ddbPathSeg) {
	if len(path) == 1 {
		delete(item, path[0].name)
		return
	}
	parent := getAttrByPath(item, path[:len(path)-1])
	if parent == nil {
		return
	}
	last := path[len(path)-1]
	if last.isIdx {
		if parent.L != nil && last.index >= 0 && last.index < len(parent.L) {
			parent.L = append(parent.L[:last.index], parent.L[last.index+1:]...)
		}
		return
	}
	if parent.M != nil {
		delete(parent.M, last.name)
	}
}

// ddbAddValues applies ADD's semantics: a Number is added to a Number, a set is
// unioned with a set of the same type, and an absent attribute takes the value.
func ddbAddValues(existing, v *AttributeValue) (*AttributeValue, error) {
	vt := avTypeName(v)
	switch vt {
	case "N", "SS", "NS", "BS":
	default:
		return nil, ddbInvalidf(ddbMsgIncorrectAddDel, "ADD", ddbLongTypeName(v))
	}
	if existing == nil {
		return deepCopyAV(v), nil
	}
	if avTypeName(existing) != vt {
		return nil, ddbInvalidf(ddbMsgIncorrectType)
	}
	switch vt {
	case "N":
		er, eok := ddbParseNumber(existing.N)
		vr, vok := ddbParseNumber(v.N)
		if !eok || !vok {
			return nil, ddbInvalidf(ddbMsgIncorrectType)
		}
		return ddbNumberResult(er.Add(er, vr))
	case "SS":
		return &AttributeValue{SS: ddbUnion(existing.SS, v.SS, func(s string) string { return s })}, nil
	case "NS":
		return &AttributeValue{NS: ddbUnion(existing.NS, v.NS, ddbCanonicalNumber)}, nil
	default: // BS
		merged := ddbUnion(ddbBinaryStrings(existing.BS), ddbBinaryStrings(v.BS), func(s string) string { return s })
		out := make([][]byte, len(merged))
		for i, s := range merged {
			out[i] = []byte(s)
		}
		return &AttributeValue{BS: out}, nil
	}
}

// ddbDeleteValues applies DELETE's semantics: the members of v are removed from the
// set existing. The result is nil — the attribute is removed — when no member remains,
// because DynamoDB does not store an empty set.
func ddbDeleteValues(existing, v *AttributeValue) (*AttributeValue, error) {
	vt := avTypeName(v)
	switch vt {
	case "SS", "NS", "BS":
	default:
		return nil, ddbInvalidf(ddbMsgIncorrectAddDel, "DELETE", ddbLongTypeName(v))
	}
	if existing == nil {
		return nil, nil
	}
	if avTypeName(existing) != vt {
		return nil, ddbInvalidf(ddbMsgIncorrectType)
	}
	var out *AttributeValue
	switch vt {
	case "SS":
		out = &AttributeValue{SS: ddbMinus(existing.SS, v.SS, func(s string) string { return s })}
		if len(out.SS) == 0 {
			return nil, nil
		}
	case "NS":
		out = &AttributeValue{NS: ddbMinus(existing.NS, v.NS, ddbCanonicalNumber)}
		if len(out.NS) == 0 {
			return nil, nil
		}
	default:
		rest := ddbMinus(ddbBinaryStrings(existing.BS), ddbBinaryStrings(v.BS), func(s string) string { return s })
		if len(rest) == 0 {
			return nil, nil
		}
		out = &AttributeValue{BS: make([][]byte, len(rest))}
		for i, s := range rest {
			out.BS[i] = []byte(s)
		}
	}
	return out, nil
}

// ddbCanonicalNumber returns a number's canonical decimal form, or the input when it
// does not parse.
func ddbCanonicalNumber(n string) string {
	if r, ok := ddbParseNumber(n); ok {
		return ddbFormatNumber(r)
	}
	return n
}

// ddbUnion returns a's members followed by b's members not already present, comparing
// members by key.
func ddbUnion(a, b []string, key func(string) string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range append(append([]string(nil), a...), b...) {
		k := key(s)
		if !seen[k] {
			seen[k] = true
			out = append(out, s)
		}
	}
	return out
}

// ddbMinus returns a's members that are not in b, comparing members by key.
func ddbMinus(a, b []string, key func(string) string) []string {
	drop := make(map[string]bool, len(b))
	for _, s := range b {
		drop[key(s)] = true
	}
	out := make([]string, 0, len(a))
	for _, s := range a {
		if !drop[key(s)] {
			out = append(out, s)
		}
	}
	return out
}

// applyUpdateExpression parses expr and applies it to item in place, returning the
// top-level attribute names the expression touched (what UPDATED_OLD and UPDATED_NEW
// report). item must not share nested storage with any value the caller still needs
// unchanged — pass a deep copy.
//
// Parsing and evaluation finish before the first write, so an expression that fails —
// a missing operand, a wrong type, an invalid document path — leaves item unchanged.
func applyUpdateExpression(item map[string]*AttributeValue, expr string, names map[string]string, values map[string]*AttributeValue) ([]string, error) {
	actions, err := parseDDBUpdateExpression(expr, names, values)
	if err != nil {
		return nil, err
	}
	snapshot := copyItem(item)
	work := copyItem(item)

	// Evaluate every right-hand side against the snapshot first.
	results := make([]*AttributeValue, len(actions))
	for i, act := range actions {
		switch act.clause {
		case "SET":
			v, err := evalDDBUpdateNode(act.value, snapshot)
			if err != nil {
				return nil, err
			}
			results[i] = deepCopyAV(v)
		case "ADD":
			v, err := ddbAddValues(getAttrByPath(snapshot, act.path), act.value.value)
			if err != nil {
				return nil, err
			}
			results[i] = v
		case "DELETE":
			v, err := ddbDeleteValues(getAttrByPath(snapshot, act.path), act.value.value)
			if err != nil {
				return nil, err
			}
			results[i] = v
		}
	}

	// Writes first, removals second. A SET to an existing list element replaces it in
	// place and one past the end appends, so neither moves an element the pre-update
	// list indexed; removals then resolve against those pre-update indexes (#1450).
	touched := make([]string, 0, len(actions))
	seen := map[string]bool{}
	var removals [][]ddbPathSeg
	for i, act := range actions {
		if top := act.path[0].name; !seen[top] {
			seen[top] = true
			touched = append(touched, top)
		}
		// A REMOVE, or a DELETE that emptied its set, removes the path.
		if act.clause == "REMOVE" || results[i] == nil {
			removals = append(removals, act.path)
			continue
		}
		if err := setDDBPath(work, act.path, results[i]); err != nil {
			return nil, err
		}
	}
	applyDDBRemovals(work, removals)

	for k := range item {
		delete(item, k)
	}
	for k, v := range work {
		item[k] = v
	}
	return touched, nil
}
