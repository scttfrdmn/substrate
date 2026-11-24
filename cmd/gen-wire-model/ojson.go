package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// value is a JSON value that keeps the order of an object's keys.
//
// Go's encoding/json decodes an object into a map, and a map forgets the order its keys
// arrived in. A botocore model lists a structure's members in declaration order, and that
// order is the order the generated validator reports failures in, so the vendored snapshot
// has to keep it: decoding the upstream file into a map and re-encoding it would sort every
// structure's members alphabetically.
type value struct {
	// obj holds an object's members in document order, and is nil for any other value.
	obj []member
	// arr holds an array's elements, and is nil for any other value.
	arr []*value
	// raw is a scalar's encoding (a string, number, true, false or null), and is nil for
	// an object or an array.
	raw json.RawMessage
	// isObj and isArr tell an empty object or array from a scalar.
	isObj, isArr bool
}

// member is one key of an object.
type member struct {
	key string
	val *value
}

// decodeValue parses one JSON document.
func decodeValue(data []byte) (*value, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := readValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after the JSON document")
	}
	return v, nil
}

func readValue(dec *json.Decoder) (*value, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			v := &value{isObj: true}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("read key: %w", err)
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, fmt.Errorf("object key is %T, not a string", keyTok)
				}
				child, err := readValue(dec)
				if err != nil {
					return nil, err
				}
				v.obj = append(v.obj, member{key: key, val: child})
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("read object end: %w", err)
			}
			return v, nil
		case '[':
			v := &value{isArr: true}
			for dec.More() {
				child, err := readValue(dec)
				if err != nil {
					return nil, err
				}
				v.arr = append(v.arr, child)
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("read array end: %w", err)
			}
			return v, nil
		}
		return nil, fmt.Errorf("unexpected delimiter %v", t)
	case string:
		var b bytes.Buffer
		appendJSONString(&b, t)
		return &value{raw: b.Bytes()}, nil
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return nil, fmt.Errorf("encode scalar: %w", err)
		}
		return &value{raw: raw}, nil
	}
}

// encode writes v compactly, keeping each object's key order.
func (v *value) encode(b *bytes.Buffer) {
	switch {
	case v.isObj:
		b.WriteByte('{')
		for i, m := range v.obj {
			if i > 0 {
				b.WriteByte(',')
			}
			appendJSONString(b, m.key)
			b.WriteByte(':')
			m.val.encode(b)
		}
		b.WriteByte('}')
	case v.isArr:
		b.WriteByte('[')
		for i, e := range v.arr {
			if i > 0 {
				b.WriteByte(',')
			}
			e.encode(b)
		}
		b.WriteByte(']')
	default:
		b.Write(v.raw)
	}
}

// appendJSONString writes s as a JSON string. Only the quote, the backslash and the control
// characters are escaped, so a string's encoding cannot fail and does not depend on an
// encoder's HTML-escaping setting.
func appendJSONString(b *bytes.Buffer, s string) {
	const hex = "0123456789abcdef"
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
}

// get returns the member named key, or nil.
func (v *value) get(key string) *value {
	if v == nil {
		return nil
	}
	for _, m := range v.obj {
		if m.key == key {
			return m.val
		}
	}
	return nil
}

// str returns a string scalar's value, or "" for anything else.
func (v *value) str() string {
	if v == nil || v.raw == nil {
		return ""
	}
	var s string
	if err := json.Unmarshal(v.raw, &s); err != nil {
		return ""
	}
	return s
}

// boolean returns a boolean scalar's value, false for anything else.
func (v *value) boolean() bool {
	return v != nil && string(v.raw) == "true"
}

// number returns a number scalar's text, or "" for anything else.
func (v *value) number() string {
	if v == nil || len(v.raw) == 0 {
		return ""
	}
	c := v.raw[0]
	if c == '-' || (c >= '0' && c <= '9') {
		return string(v.raw)
	}
	return ""
}

// keys returns an object's keys in document order.
func (v *value) keys() []string {
	if v == nil {
		return nil
	}
	out := make([]string, 0, len(v.obj))
	for _, m := range v.obj {
		out = append(out, m.key)
	}
	return out
}

// strings returns an array of strings, skipping any element that is not one.
func (v *value) strings() []string {
	if v == nil {
		return nil
	}
	out := make([]string, 0, len(v.arr))
	for _, e := range v.arr {
		if s := e.str(); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// without returns a copy of the object v with the named keys removed from it. Only v's own
// keys are removed: the caller names each level a key may appear at, because a key that is
// documentation on an operation or a shape is a member name inside a structure's members.
func (v *value) without(drop ...string) *value {
	if v == nil || !v.isObj {
		return v
	}
	out := &value{isObj: true}
	for _, m := range v.obj {
		skip := false
		for _, d := range drop {
			if m.key == d {
				skip = true
				break
			}
		}
		if !skip {
			out.obj = append(out.obj, m)
		}
	}
	return out
}

// members returns an object's members, and nil for a nil or non-object value.
func (v *value) members() []member {
	if v == nil {
		return nil
	}
	return v.obj
}

// elems returns an array's elements, and nil for a nil or non-array value.
func (v *value) elems() []*value {
	if v == nil {
		return nil
	}
	return v.arr
}

// set replaces the member named key, or appends it when v has none.
func (v *value) set(key string, val *value) {
	for i := range v.obj {
		if v.obj[i].key == key {
			v.obj[i].val = val
			return
		}
	}
	v.obj = append(v.obj, member{key: key, val: val})
}
