package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// This file holds substrate's small PartiQL subset for ExecuteStatement and
// BatchExecuteStatement. A WHERE clause and a SET clause are translated into the
// condition- and update-expression forms UpdateItem uses — every literal and `?`
// parameter becomes an expression attribute value, every double-quoted name an
// expression attribute name — so PartiQL is evaluated by exactly the code the
// expression APIs are.

// tokenizePartiQL extracts the verb, table name, WHERE clause, and SET clause
// from a simple PartiQL statement. Only basic patterns are supported.
func tokenizePartiQL(stmt string) (verb, tableName, whereClause, setClause string) {
	stmt = strings.TrimSpace(stmt)
	upper := strings.ToUpper(stmt)

	if strings.HasPrefix(upper, "SELECT") {
		// SELECT * FROM "table" [WHERE ...]
		fromIdx := partiQLIndexKeyword(stmt, "FROM")
		if fromIdx < 0 {
			return "SELECT", "", "", ""
		}
		rest := strings.TrimSpace(stmt[fromIdx+len("FROM"):])
		tableName = extractPartiQLTableName(rest)
		whereClause = extractPartiQLClause(rest, "WHERE")
		return "SELECT", tableName, whereClause, ""
	}

	if strings.HasPrefix(upper, "INSERT") {
		// INSERT INTO "table" VALUE {...}
		intoIdx := partiQLIndexKeyword(stmt, "INTO")
		if intoIdx < 0 {
			return "INSERT", "", "", ""
		}
		rest := strings.TrimSpace(stmt[intoIdx+len("INTO"):])
		tableName = extractPartiQLTableName(rest)
		valueClause := extractPartiQLClause(rest, "VALUE")
		return "INSERT", tableName, "", valueClause
	}

	if strings.HasPrefix(upper, "UPDATE") {
		// UPDATE "table" SET ... WHERE ...
		rest := strings.TrimSpace(stmt[len("UPDATE"):])
		tableName = extractPartiQLTableName(rest)
		setClause = extractPartiQLClause(rest, "SET")
		// The SET clause ends where the WHERE clause begins — a WHERE outside quotes,
		// so `SET a = 'x WHERE y'` keeps its literal whole.
		if w := partiQLIndexKeyword(setClause, "WHERE"); w >= 0 {
			setClause = strings.TrimSpace(setClause[:w])
		}
		whereClause = extractPartiQLClause(rest, "WHERE")
		return "UPDATE", tableName, whereClause, setClause
	}

	if strings.HasPrefix(upper, "DELETE") {
		// DELETE FROM "table" WHERE ...
		fromIdx := partiQLIndexKeyword(stmt, "FROM")
		if fromIdx < 0 {
			return "DELETE", "", "", ""
		}
		rest := strings.TrimSpace(stmt[fromIdx+len("FROM"):])
		tableName = extractPartiQLTableName(rest)
		whereClause = extractPartiQLClause(rest, "WHERE")
		return "DELETE", tableName, whereClause, ""
	}

	return "", "", "", ""
}

// extractPartiQLTableName returns the table name from the leading part of a
// PartiQL statement fragment. Table names may be quoted or unquoted.
func extractPartiQLTableName(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return ""
	}
	if s[0] == '"' {
		end := strings.Index(s[1:], `"`)
		if end < 0 {
			return s[1:]
		}
		return s[1 : end+1]
	}
	// Unquoted: read until space.
	if idx := strings.IndexAny(s, " \t"); idx >= 0 {
		return s[:idx]
	}
	return s
}

// extractPartiQLClause returns the text following keyword in a statement, or "" when
// the keyword does not appear outside quotes.
func extractPartiQLClause(stmt, keyword string) string {
	idx := partiQLIndexKeyword(stmt, keyword)
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(stmt[idx+len(keyword):])
}

// partiQLIndexKeyword returns the index of keyword in s as a whole word — at the start
// or after whitespace, and followed by whitespace or the end — outside single- and
// double-quoted text, matched case-insensitively; -1 when there is none.
func partiQLIndexKeyword(s, keyword string) int {
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if i > 0 && s[i-1] != ' ' && s[i-1] != '\t' && s[i-1] != '\n' {
			continue
		}
		end := i + len(keyword)
		if end > len(s) || !strings.EqualFold(s[i:end], keyword) {
			continue
		}
		if end == len(s) || s[end] == ' ' || s[end] == '\t' || s[end] == '\n' {
			return i
		}
	}
	return -1
}

// partiQLParams hands out a statement's `?` parameters in order.
type partiQLParams struct {
	values []*AttributeValue
	next   int
}

// take returns the next parameter, or an error when the statement has more `?` than
// the request has Parameters.
func (pp *partiQLParams) take() (*AttributeValue, error) {
	if pp.next >= len(pp.values) {
		return nil, ddbInvalidf("Number of parameters in request and statement don't match.")
	}
	v := pp.values[pp.next]
	pp.next++
	return v, nil
}

// partiQLExpr accumulates the expression attribute names and values a translation
// introduces.
type partiQLExpr struct {
	names  map[string]string
	values map[string]*AttributeValue
	params *partiQLParams
}

// newPartiQLExpr returns an empty translation drawing `?` values from params.
func newPartiQLExpr(params *partiQLParams) *partiQLExpr {
	return &partiQLExpr{names: map[string]string{}, values: map[string]*AttributeValue{}, params: params}
}

// value records av under a fresh placeholder and returns the placeholder.
func (x *partiQLExpr) value(av *AttributeValue) string {
	ph := ":pq" + strconv.Itoa(len(x.values))
	x.values[ph] = av
	return ph
}

// name records an attribute name under a fresh placeholder and returns it.
func (x *partiQLExpr) name(n string) string {
	ph := "#pq" + strconv.Itoa(len(x.names))
	x.names[ph] = n
	return ph
}

// isPartiQLWordChar reports whether c can be part of an identifier or a number.
func isPartiQLWordChar(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// translate rewrites a PartiQL expression fragment — a WHERE clause, or a SET
// action's right-hand side — into expression syntax: a single-quoted string (” is an
// escaped quote), a number, true/false and `?` become placeholders for their values; a
// double-quoted name becomes a name placeholder; `!=` becomes `<>`. Everything else is
// passed through to the expression parser, which reports what it cannot read.
func (x *partiQLExpr) translate(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); {
		c := s[i]
		prevWord := i > 0 && (isPartiQLWordChar(s[i-1]) || s[i-1] == '#' || s[i-1] == ':' || s[i-1] == '[')
		switch {
		case c == '\'':
			var lit strings.Builder
			j := i + 1
			for {
				if j >= len(s) {
					return "", fmt.Errorf("unterminated string literal in %q", s)
				}
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						lit.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				lit.WriteByte(s[j])
				j++
			}
			v := lit.String()
			b.WriteString(x.value(&AttributeValue{S: &v}))
			i = j + 1
		case c == '"':
			end := strings.IndexByte(s[i+1:], '"')
			if end < 0 {
				return "", fmt.Errorf("unterminated quoted name in %q", s)
			}
			b.WriteString(x.name(s[i+1 : i+1+end]))
			i += end + 2
		case c == '?':
			av, err := x.params.take()
			if err != nil {
				return "", err
			}
			b.WriteString(x.value(av))
			i++
		case c == '!' && i+1 < len(s) && s[i+1] == '=':
			b.WriteString("<>")
			i += 2
		case !prevWord && (c >= '0' && c <= '9' || ((c == '-' || c == '+' || c == '.') && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9')):
			j := i + 1
			for j < len(s) && (isPartiQLWordChar(s[j]) || s[j] == '.' ||
				((s[j] == '+' || s[j] == '-') && (s[j-1] == 'e' || s[j-1] == 'E'))) {
				j++
			}
			n := s[i:j]
			if _, ok := ddbParseNumber(n); !ok {
				return "", fmt.Errorf("invalid number literal %q", n)
			}
			b.WriteString(x.value(&AttributeValue{N: n}))
			i = j
		case !prevWord && isPartiQLWordChar(c):
			j := i
			for j < len(s) && isPartiQLWordChar(s[j]) {
				j++
			}
			word := s[i:j]
			switch {
			case strings.EqualFold(word, "true"), strings.EqualFold(word, "false"):
				v := strings.EqualFold(word, "true")
				b.WriteString(x.value(&AttributeValue{BOOL: &v}))
			default:
				b.WriteString(word)
			}
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), nil
}

// partiQLWhereCondition translates a WHERE clause into a condition expression and
// checks that it parses, so a malformed clause is refused even against an empty table
// rather than matching nothing.
func partiQLWhereCondition(where string, params *partiQLParams) (string, map[string]string, map[string]*AttributeValue, error) {
	if where == "" {
		return "", nil, nil, nil
	}
	x := newPartiQLExpr(params)
	expr, err := x.translate(where)
	if err != nil {
		return "", nil, nil, err
	}
	if _, err := evalDynamoCondition(expr, x.names, x.values, nil); err != nil {
		return "", nil, nil, err
	}
	return expr, x.names, x.values, nil
}

// partiQLValidation wraps a translation or evaluation failure as the
// ValidationException PartiQL answers.
func partiQLValidation(prefix string, err error) error {
	return &AWSError{Code: "ValidationException", Message: prefix + err.Error(), HTTPStatus: http.StatusBadRequest}
}

// partiQLMatch evaluates a translated WHERE condition against an item. A condition
// that fails to evaluate is an error, not a non-match.
func partiQLMatch(cond string, names map[string]string, values map[string]*AttributeValue, item map[string]*AttributeValue) (bool, error) {
	if cond == "" {
		return true, nil
	}
	ok, err := evalDynamoCondition(cond, names, values, item)
	if err != nil {
		return false, partiQLValidation("Invalid WHERE clause: ", err)
	}
	return ok, nil
}

func (p *DynamoDBPlugin) partiQLSelect(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause string,
	params []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	cond, names, values, err := partiQLWhereCondition(whereClause, &partiQLParams{values: params})
	if err != nil {
		return nil, partiQLValidation("Invalid WHERE clause: ", err)
	}
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName)
	if err != nil {
		return nil, err
	}

	var results []map[string]*AttributeValue
	for _, k := range keys {
		item, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName, k)
		if err != nil {
			return nil, err
		}
		if item == nil {
			continue
		}
		ok, err := partiQLMatch(cond, names, values, item)
		if err != nil {
			return nil, err
		}
		if ok {
			results = append(results, item)
		}
	}
	return results, nil
}

func (p *DynamoDBPlugin) partiQLInsert(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	valueClause string,
	_ []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	// valueClause should be a JSON object like {"pk": {"S": "val"}, ...}
	var item map[string]*AttributeValue
	if err := json.Unmarshal([]byte(valueClause), &item); err != nil {
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "Failed to parse VALUE clause as item: " + err.Error(),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err := dynamodbValidateNumbers(item); err != nil {
		return nil, err
	}

	ik, err := dynamodbItemKeyFor(tbl, item)
	if err != nil {
		return nil, err
	}

	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tbl.TableName, ik)
	defer unlock()
	old, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName, ik)
	if err != nil {
		return nil, err
	}
	if err := p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName, ik, old, item); err != nil {
		return nil, fmt.Errorf("dynamodb partiQLInsert: %w", err)
	}
	return nil, nil
}

// partiQLStatement is a translated UPDATE or DELETE: its WHERE condition and, for an
// UPDATE, its update expression, sharing one set of placeholders.
type partiQLStatement struct {
	cond       string
	updateExpr string
	names      map[string]string
	values     map[string]*AttributeValue
}

// partiQLTranslateWrite translates an UPDATE's SET clause and a WHERE clause.
// Parameters are consumed in statement order: SET's, then WHERE's.
func partiQLTranslateWrite(setClause, whereClause string, params []*AttributeValue) (*partiQLStatement, error) {
	pp := &partiQLParams{values: params}
	x := newPartiQLExpr(pp)
	st := &partiQLStatement{}
	if setClause != "" {
		expr, err := partiQLSetToUpdateExpression(setClause, x)
		if err != nil {
			return nil, partiQLValidation("Failed to apply SET clause: ", err)
		}
		st.updateExpr = expr
	}
	if whereClause != "" {
		cond, err := x.translate(whereClause)
		if err != nil {
			return nil, partiQLValidation("Invalid WHERE clause: ", err)
		}
		if _, err := evalDynamoCondition(cond, x.names, x.values, nil); err != nil {
			return nil, partiQLValidation("Invalid WHERE clause: ", err)
		}
		st.cond = cond
	}
	st.names, st.values = x.names, x.values
	return st, nil
}

func (p *DynamoDBPlugin) partiQLUpdate(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause, setClause string,
	params []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	st, err := partiQLTranslateWrite(setClause, whereClause, params)
	if err != nil {
		return nil, err
	}

	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		if err := p.partiQLUpdateOne(ctx, tbl.TableName, k, st); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// partiQLUpdateOne applies a PartiQL UPDATE to one item under the item's stripe. An
// item that does not exist or does not match the WHERE clause is left alone.
func (p *DynamoDBPlugin) partiQLUpdateOne(ctx *RequestContext, tableName, ik string, st *partiQLStatement) error {
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tableName, ik)
	defer unlock()
	current, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	ok, err := partiQLMatch(st.cond, st.names, st.values, current)
	if err != nil || !ok {
		return err
	}
	item := copyItem(current)
	if st.updateExpr != "" {
		if _, applyErr := applyUpdateExpression(item, st.updateExpr, st.names, st.values); applyErr != nil {
			return partiQLValidation("Failed to apply SET clause: ", applyErr)
		}
	}
	if err := p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik, current, item); err != nil {
		return fmt.Errorf("dynamodb partiQLUpdate: %w", err)
	}
	return nil
}

// partiQLSetToUpdateExpression rewrites a PartiQL SET clause — `a = 'x', "b" = 5` —
// as an update expression over x's placeholders, so it is applied by the same code as
// UpdateItem. Each value is translated by [partiQLExpr.translate]: a single-quoted
// string, a number, true/false or `?`. A double-quoted right-hand side is read as a
// string, as substrate has always accepted it, though PartiQL reads it as a name.
func partiQLSetToUpdateExpression(setClause string, x *partiQLExpr) (string, error) {
	var actions []string
	for _, part := range partiQLSplitTopLevel(setClause, ',') {
		eq := partiQLIndexTopLevel(part, '=')
		if eq < 0 {
			return "", fmt.Errorf("SET action missing '=': %q", strings.TrimSpace(part))
		}
		lhs := strings.Trim(strings.TrimSpace(part[:eq]), `"`)
		rhs := strings.TrimSpace(part[eq+1:])
		if lhs == "" {
			return "", fmt.Errorf("SET action missing attribute name: %q", strings.TrimSpace(part))
		}
		if len(rhs) >= 2 && rhs[0] == '"' && rhs[len(rhs)-1] == '"' {
			v := rhs[1 : len(rhs)-1]
			rhs = x.value(&AttributeValue{S: &v})
		} else {
			translated, err := x.translate(rhs)
			if err != nil {
				return "", err
			}
			rhs = translated
		}
		actions = append(actions, x.name(lhs)+" = "+rhs)
	}
	return "SET " + strings.Join(actions, ", "), nil
}

// partiQLSplitTopLevel splits s at sep where sep is outside quotes.
func partiQLSplitTopLevel(s string, sep byte) []string {
	var parts []string
	start := 0
	for {
		i := partiQLIndexTopLevel(s[start:], sep)
		if i < 0 {
			return append(parts, s[start:])
		}
		parts = append(parts, s[start:start+i])
		start += i + 1
	}
}

// partiQLIndexTopLevel returns the index of the first sep in s outside quotes, or -1.
func partiQLIndexTopLevel(s string, sep byte) int {
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == sep:
			return i
		}
	}
	return -1
}

func (p *DynamoDBPlugin) partiQLDelete(
	ctx *RequestContext,
	tbl *DynamoDBTable,
	whereClause string,
	params []*AttributeValue,
) ([]map[string]*AttributeValue, error) {
	st, err := partiQLTranslateWrite("", whereClause, params)
	if err != nil {
		return nil, err
	}
	keys, err := p.loadItemKeys(context.Background(), ctx.AccountID, ctx.Region, tbl.TableName)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		if err := p.partiQLDeleteOne(ctx, tbl.TableName, k, st); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// partiQLDeleteOne applies a PartiQL DELETE to one item under the item's stripe.
func (p *DynamoDBPlugin) partiQLDeleteOne(ctx *RequestContext, tableName, ik string, st *partiQLStatement) error {
	unlock := p.locks.lockItem(ctx.AccountID, ctx.Region, tableName, ik)
	defer unlock()
	current, err := p.loadItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	ok, err := partiQLMatch(st.cond, st.names, st.values, current)
	if err != nil || !ok {
		return err
	}
	if err := p.commitItem(context.Background(), ctx.AccountID, ctx.Region, tableName, ik, current, nil); err != nil {
		return fmt.Errorf("dynamodb partiQLDelete: %w", err)
	}
	return nil
}
