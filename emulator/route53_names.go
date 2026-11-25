package emulator

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Route 53's DNS name format, and the canonical form a record set is keyed by.
//
// The Developer Guide's "DNS domain name format" gives the rules this file applies:
//
//   - "Amazon Route 53 stores alphabetic characters as lowercase letters (a-z), regardless of how
//     you specify them: as uppercase letters, lowercase letters, or the corresponding letters in
//     escape codes";
//   - characters are specified "by using escape codes in the format \three-digit octal code", and
//     "If the domain name includes any characters other than a to z, 0 to 9, - (hyphen), or _
//     (underscore), Route 53 API actions return the characters as escape codes. This is true
//     whether you specify the characters as characters or as escape codes";
//   - a period inside a label is \056, and a period between labels is written as itself.
//
// So "*.example.com" and "\052.example.com" are one name, and both are reported as
// "\052.example.com." — the form Terraform's AWS provider normalizes a record name to before it
// compares it with a listing (internal/dns.Normalize). API_ResourceRecordSet: Route 53 "treats
// www.example.com (without a trailing dot) and www.example.com. (with a trailing dot) as
// identical", and every name it reports carries the dot.

// r53NormalizeName returns name in the form Route 53 stores and reports it, its trailing dot as
// given: letters lower-cased, every character outside a–z, 0–9, "-", "_" and the label separator
// written as a three-digit octal escape, and an escape that names a letter, digit, "-" or "_" written
// as the character. A backslash that does not begin a three-digit octal escape is itself escaped.
func r53NormalizeName(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if v, ok := r53OctalEscape(name, i); ok {
			i += 3
			if ch := r53Lower(v); r53PlainNameByte(ch) {
				b.WriteByte(ch)
				continue
			}
			fmt.Fprintf(&b, `\%03o`, v)
			continue
		}
		c = r53Lower(c)
		if c == '.' || r53PlainNameByte(c) {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, `\%03o`, c)
	}
	return b.String()
}

// r53NameOctets returns how many octets s stands for: an octal escape (\ooo) is one, as is every
// other byte.
func r53NameOctets(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if _, ok := r53OctalEscape(s, i); ok {
			i += 3
		}
		n++
	}
	return n
}

// r53PlainNameByte reports whether c is a character Route 53 reports as itself.
func r53PlainNameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}

// r53OctalEscape reads the escape \ooo at name[i], reporting false when name[i] does not begin one
// naming a byte (000 to 377).
func r53OctalEscape(name string, i int) (byte, bool) {
	if name[i] != '\\' || i+4 > len(name) {
		return 0, false
	}
	v := 0
	for _, c := range []byte(name[i+1 : i+4]) {
		if c < '0' || c > '7' {
			return 0, false
		}
		v = v<<3 | int(c-'0')
	}
	if v > 0xff {
		return 0, false
	}
	return byte(v), true
}

func r53Lower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

// r53CanonicalName returns a DNS name's normalized form ([r53NormalizeName]) without its trailing
// dot, so "Example.com.", "example.com" and "\145xample.com" compare equal, as do "*.example.com"
// and "\052.example.com".
func r53CanonicalName(name string) string {
	return strings.TrimSuffix(r53NormalizeName(name), ".")
}

// r53WireName returns name as a response reports it: normalized, with its trailing dot.
func r53WireName(name string) string {
	return r53FQDN(r53NormalizeName(name))
}

// r53EntryIdentity returns the identity a record-set index entry names: its Type upper-cased and its
// name canonical, as [r53RRSetIdentityKey] builds a key, so an entry written under a non-canonical
// spelling and one written under its canonical key are one identity. A key [r53RRSetIdentityKey]
// wrote for a SetIdentifier ends in "|" and the identifier in hex; a canonical name never holds a
// raw "|" (it is reported as \174), so the last "|" followed by hex is that suffix, and is kept as
// it is.
func r53EntryIdentity(entry string) string {
	rrType, name, ok := strings.Cut(entry, ":")
	if !ok {
		return entry
	}
	suffix := ""
	if at := strings.LastIndexByte(name, '|'); at >= 0 {
		if set := name[at+1:]; set != "" && set == strings.ToLower(set) {
			if _, err := hex.DecodeString(set); err == nil {
				name, suffix = name[:at], name[at:]
			}
		}
	}
	return r53RRSetKey(rrType, name) + suffix
}
