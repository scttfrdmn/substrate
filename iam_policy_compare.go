package substrate

import (
	"encoding/json"
	"sort"
)

// policyDocumentsEqual reports whether two IAM policy documents are semantically
// equivalent, independent of statement order. The Version must match and the set
// of statements must be equal as multisets. Each statement is canonicalized by
// marshaling to JSON (StringOrSlice and PolicyPrincipal already normalize
// single-value vs. array forms on unmarshal, so equivalent statements produce
// identical JSON), then the per-statement JSON strings are sorted before
// comparison so statement ordering does not produce false drift.
func policyDocumentsEqual(a, b PolicyDocument) bool {
	if a.Version != b.Version {
		return false
	}
	sa := canonicalStatements(a.Statement)
	sb := canonicalStatements(b.Statement)
	if len(sa) != len(sb) {
		return false
	}
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

// canonicalStatements marshals each statement to JSON and returns the sorted
// slice of JSON strings, giving an order-independent canonical form.
func canonicalStatements(stmts []PolicyStatement) []string {
	out := make([]string, 0, len(stmts))
	for i := range stmts {
		b, err := json.Marshal(stmts[i])
		if err != nil {
			// Fall back to a best-effort string so unequal inputs stay unequal.
			out = append(out, "")
			continue
		}
		out = append(out, string(b))
	}
	sort.Strings(out)
	return out
}
