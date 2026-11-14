package emulator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"sort"
	"strings"
)

// This file holds DynamoDB's typed value semantics: how two AttributeValues compare,
// how a Number is read and written exactly, and how a key attribute becomes a
// state-key component. The rules are the ones the Condition API reference states for
// every comparator (#1454):
//
//   - Equality (=, <>, IN) is defined for every type, including lists and maps. Values
//     of different types are never equal: {"S":"6"} does not equal {"N":"6"}.
//   - Ordering (<, <=, >, >=, BETWEEN) is defined only between two Strings, two
//     Numbers or two Binaries. Any other pairing does not match.
//   - Numbers compare numerically; Strings by their UTF-8 bytes; Binaries "treat each
//     byte of the binary data as unsigned".

// MarshalJSON encodes an AttributeValue with exactly the members that are set.
//
// It differs from the struct tags' omitempty in one respect, which is the reason it
// exists: an empty list ({"L": []}), an empty map ({"M": {}}) and an empty binary
// ({"B": ""}) are values in their own right, and omitempty dropped them, so a stored
// `{"M": {}}` came back as a typeless `{}`. The update-expression guide's own advice for
// nested updates is to "initialize empty maps for the parent attributes", which only
// works if the empty map survives being stored (#1450).
func (av AttributeValue) MarshalJSON() ([]byte, error) {
	type wire struct {
		S    *string                     `json:"S,omitempty"`
		N    string                      `json:"N,omitempty"`
		B    *[]byte                     `json:"B,omitempty"`
		SS   []string                    `json:"SS,omitempty"`
		NS   []string                    `json:"NS,omitempty"`
		BS   [][]byte                    `json:"BS,omitempty"`
		M    *map[string]*AttributeValue `json:"M,omitempty"`
		L    *[]*AttributeValue          `json:"L,omitempty"`
		NULL *bool                       `json:"NULL,omitempty"`
		BOOL *bool                       `json:"BOOL,omitempty"`
	}
	w := wire{S: av.S, N: av.N, SS: av.SS, NS: av.NS, BS: av.BS, NULL: av.NULL, BOOL: av.BOOL}
	if av.B != nil {
		w.B = &av.B
	}
	if av.M != nil {
		w.M = &av.M
	}
	if av.L != nil {
		w.L = &av.L
	}
	return json.Marshal(w)
}

// ddbParseNumber reads a DynamoDB Number exactly. DynamoDB numbers carry up to 38
// significant digits, more than a float64 holds, so they are compared and added as
// rationals rather than floats.
func ddbParseNumber(s string) (*big.Rat, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	return new(big.Rat).SetString(s)
}

// ddbFormatNumber writes r in the plain decimal form DynamoDB returns: no exponent, no
// trailing fractional zeros. Every value reaching it is a sum or difference of decimal
// literals, so its denominator divides a power of ten and the expansion terminates.
func ddbFormatNumber(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	d := r.Denom()
	ten := big.NewInt(10)
	pow := big.NewInt(10)
	mod := new(big.Int)
	digits := 1
	for ; digits < 400; digits++ {
		if mod.Mod(pow, d).Sign() == 0 {
			break
		}
		pow.Mul(pow, ten)
	}
	out := r.FloatString(digits)
	if strings.Contains(out, ".") {
		out = strings.TrimRight(out, "0")
		out = strings.TrimSuffix(out, ".")
	}
	return out
}

// ddbNumbersEqual reports whether two Number strings denote the same value. A string
// that does not parse is compared literally.
func ddbNumbersEqual(a, b string) bool {
	ar, aok := ddbParseNumber(a)
	br, bok := ddbParseNumber(b)
	if !aok || !bok {
		return a == b
	}
	return ar.Cmp(br) == 0
}

// avEqual reports whether a and b are equal under DynamoDB's typed equality: the same
// type and the same value, with sets compared as unordered collections and lists and
// maps compared deeply. A missing value (nil) equals nothing.
func avEqual(a, b *AttributeValue) bool {
	if a == nil || b == nil {
		return false
	}
	ta, tb := avTypeName(a), avTypeName(b)
	if ta != tb {
		return false
	}
	switch ta {
	case "S":
		return *a.S == *b.S
	case "N":
		return ddbNumbersEqual(a.N, b.N)
	case "B":
		return bytes.Equal(a.B, b.B)
	case "BOOL":
		return *a.BOOL == *b.BOOL
	case "NULL":
		return true
	case "SS":
		return ddbStringSetEqual(a.SS, b.SS)
	case "NS":
		return ddbStringSetEqual(ddbCanonicalNumbers(a.NS), ddbCanonicalNumbers(b.NS))
	case "BS":
		return ddbStringSetEqual(ddbBinaryStrings(a.BS), ddbBinaryStrings(b.BS))
	case "L":
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !avEqual(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	case "M":
		if len(a.M) != len(b.M) {
			return false
		}
		for k, av := range a.M {
			bv, ok := b.M[k]
			if !ok || !avEqual(av, bv) {
				return false
			}
		}
		return true
	}
	// Two typeless values: nothing to tell them apart.
	return true
}

// avOrder orders a against b. ok is false unless both are Strings, both Numbers or
// both Binaries, the only pairings DynamoDB's <, <=, >, >= and BETWEEN accept; for any
// other pairing the comparison does not match.
func avOrder(a, b *AttributeValue) (cmp int, ok bool) {
	if a == nil || b == nil {
		return 0, false
	}
	ta, tb := avTypeName(a), avTypeName(b)
	if ta != tb {
		return 0, false
	}
	switch ta {
	case "S":
		return strings.Compare(*a.S, *b.S), true
	case "N":
		ar, aok := ddbParseNumber(a.N)
		br, bok := ddbParseNumber(b.N)
		if !aok || !bok {
			return 0, false
		}
		return ar.Cmp(br), true
	case "B":
		return bytes.Compare(a.B, b.B), true
	}
	return 0, false
}

// ddbStringSetEqual reports whether two sets hold the same members, in any order.
func ddbStringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// ddbCanonicalNumbers rewrites each member of a number set in canonical decimal form,
// so "1.0" and "1" are recognized as the same member.
func ddbCanonicalNumbers(ns []string) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		if r, ok := ddbParseNumber(n); ok {
			out[i] = ddbFormatNumber(r)
		} else {
			out[i] = n
		}
	}
	return out
}

// ddbBinaryStrings renders each member of a binary set as a string for set comparison.
func ddbBinaryStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}

// dynamodbBinaryKeyPrefix marks a Binary key attribute in a state-key component.
//
// String and Number keys are encoded as their bare value, exactly as before #1454, so
// every existing table's state keys are unchanged. Binary keys used to encode as the
// empty string — every Binary key of a table mapped to one item. They now encode as
// this prefix followed by the standard base64 of the bytes, which is distinct per value.
// A table's key attribute has one declared type, which substrate now enforces
// (dynamodbValidateKeyTypes), so a String key and a Binary key never meet in one table.
const dynamodbBinaryKeyPrefix = "B:"

// dynamodbKeyComponent encodes one key attribute value as a state-key component.
func dynamodbKeyComponent(av *AttributeValue) string {
	switch {
	case av == nil:
		return ""
	case av.S != nil:
		return *av.S
	case av.N != "":
		return av.N
	case av.B != nil:
		return dynamodbBinaryKeyPrefix + base64.StdEncoding.EncodeToString(av.B)
	}
	return ""
}

// dynamodbValidateKeyTypes checks that each key attribute in key has the scalar type
// the table declares for it in AttributeDefinitions. DynamoDB rejects a mismatch with a
// ValidationException; a key attribute the table does not declare is not checked.
func dynamodbValidateKeyTypes(tbl *DynamoDBTable, key map[string]*AttributeValue) error {
	declared := make(map[string]string, len(tbl.AttributeDefinitions))
	for _, d := range tbl.AttributeDefinitions {
		declared[d.AttributeName] = d.AttributeType
	}
	for _, ks := range tbl.KeySchema {
		want, ok := declared[ks.AttributeName]
		if !ok {
			continue
		}
		av := key[ks.AttributeName]
		if av == nil {
			continue // a missing key is reported by extractPrimaryKey
		}
		if got := avTypeName(av); got != want {
			return &AWSError{
				Code:       "ValidationException",
				Message:    "One or more parameter values were invalid: Type mismatch for key " + ks.AttributeName + " expected: " + want + " actual: " + got,
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	return nil
}

// deepCopyAV returns a copy of av that shares no mutable storage with it.
func deepCopyAV(av *AttributeValue) *AttributeValue {
	if av == nil {
		return nil
	}
	out := &AttributeValue{N: av.N}
	if av.S != nil {
		s := *av.S
		out.S = &s
	}
	if av.B != nil {
		out.B = append([]byte{}, av.B...)
	}
	if av.SS != nil {
		out.SS = append([]string{}, av.SS...)
	}
	if av.NS != nil {
		out.NS = append([]string{}, av.NS...)
	}
	if av.BS != nil {
		out.BS = make([][]byte, len(av.BS))
		for i, b := range av.BS {
			out.BS[i] = append([]byte{}, b...)
		}
	}
	if av.M != nil {
		out.M = make(map[string]*AttributeValue, len(av.M))
		for k, v := range av.M {
			out.M[k] = deepCopyAV(v)
		}
	}
	if av.L != nil {
		out.L = make([]*AttributeValue, len(av.L))
		for i, v := range av.L {
			out.L[i] = deepCopyAV(v)
		}
	}
	if av.NULL != nil {
		b := *av.NULL
		out.NULL = &b
	}
	if av.BOOL != nil {
		b := *av.BOOL
		out.BOOL = &b
	}
	return out
}

// avCompareOp applies one of the comparators =, <>, <, <=, >, >= with DynamoDB's typed
// semantics. <> is the negation of =, so it holds for values of different types and
// for a missing attribute; the ordering comparators hold only between two Strings, two
// Numbers or two Binaries.
func avCompareOp(op dynamoTokenType, a, b *AttributeValue) bool {
	switch op {
	case dynTokEQ:
		return avEqual(a, b)
	case dynTokNE:
		return !avEqual(a, b)
	}
	c, ok := avOrder(a, b)
	if !ok {
		return false
	}
	switch op {
	case dynTokLT:
		return c < 0
	case dynTokLE:
		return c <= 0
	case dynTokGT:
		return c > 0
	case dynTokGE:
		return c >= 0
	}
	return false
}

// avBeginsWith implements begins_with: a String prefix of a String, or a Binary prefix
// of a Binary. Any other pairing is false.
func avBeginsWith(a, prefix *AttributeValue) bool {
	if a == nil || prefix == nil {
		return false
	}
	switch {
	case a.S != nil && prefix.S != nil:
		return strings.HasPrefix(*a.S, *prefix.S)
	case avTypeName(a) == "B" && avTypeName(prefix) == "B":
		return bytes.HasPrefix(a.B, prefix.B)
	}
	return false
}

// avContains implements contains: a substring of a String, a subsequence of a Binary,
// a member of a set of the operand's type, or an element of a list.
func avContains(a, v *AttributeValue) bool {
	if a == nil || v == nil {
		return false
	}
	vt := avTypeName(v)
	switch avTypeName(a) {
	case "S":
		return vt == "S" && strings.Contains(*a.S, *v.S)
	case "B":
		return vt == "B" && bytes.Contains(a.B, v.B)
	case "SS":
		if vt != "S" {
			return false
		}
		for _, m := range a.SS {
			if m == *v.S {
				return true
			}
		}
	case "NS":
		if vt != "N" {
			return false
		}
		for _, m := range a.NS {
			if ddbNumbersEqual(m, v.N) {
				return true
			}
		}
	case "BS":
		if vt != "B" {
			return false
		}
		for _, m := range a.BS {
			if bytes.Equal(m, v.B) {
				return true
			}
		}
	case "L":
		for _, el := range a.L {
			if avEqual(el, v) {
				return true
			}
		}
	}
	return false
}
