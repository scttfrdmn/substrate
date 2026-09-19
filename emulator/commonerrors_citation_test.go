package emulator_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #1064 swept every common-errors citation in emulator/ against the page as it reads
// now, and found the reference has three live generations rather than the two #950
// assumed: the fifteen-entry JSON list (byte-identical on sixteen services), the
// eighteen-entry Query list (byte-identical on five), and SQS's legacy eighteen-entry
// list, which alone carries different statuses and a different ValidationError gloss.
//
// The sweep produced no code corrections, which is the useful result — but it produced
// them by reading twenty-two pages, and that cannot be repeated on every change. What
// can be pinned is the one rule the three generations agree on, and the one a doc
// comment has drifted across before:
//
//	ValidationError and ValidationException are different codes with different sources.
//
// ValidationError appears *only* on a common-errors page, in all three generations, and
// never in an operation's own Errors section. ValidationException appears *only* in
// operation Errors sections, and on no common-errors page in any generation. ACM
// publishes both, with different glosses, which is why the distinction is worth a
// tripwire rather than a sentence: acmValidationError cites ACM's operation pages for
// ValidationException, and acmInvalidBody cites the common page for ValidationError.
//
// So a constructor answering ValidationException whose own doc comment sources it to a
// common-errors page is citing something no such page says. The tripwire below refuses
// exactly that, structurally.

// commonErrorsCitation matches the ways the tree refers to a common-errors page.
var commonErrorsCitation = []string{"CommonErrors", "common-errors", "Common Errors", "Common Error Types"}

// TestCommonErrors_ValidationExceptionIsNeverSourcedToTheCommonPage is the tripwire.
//
// It parses every non-test Go file in emulator/ and reports any function that both
// answers ValidationException and cites a common-errors page in its *own* doc comment.
// The doc comment is the unit deliberately: invalid_body_refusals.go's file comment
// discusses the common list at length while its individual constructors cite operation
// pages, so attributing the file comment to each function would flag nine correct sites.
//
// A negated mention ("absent from the Common Errors page") is a correct citation of what
// the page does not say, so the check looks for it and allows it. That is the shape of
// several honest comments in the tree — ramInvalidBody's is the clearest — and a rule
// that failed them would push the next author toward deleting the provenance rather than
// writing it.
func TestCommonErrors_ValidationExceptionIsNeverSourcedToTheCommonPage(t *testing.T) {
	root, err := filepath.Abs(".")
	require.NoError(t, err)

	type finding struct {
		file string
		line int
		fn   string
	}
	var found []finding

	// The control. ValidationError is the code that *does* belong to the common page, so
	// the same two predicates must find it — otherwise a green run above proves only that
	// the detector is broken. Counted rather than asserted by name so that fixing any one
	// site does not break the control.
	controls := 0

	fset := token.NewFileSet()
	entries, err := filepath.Glob(filepath.Join(root, "*.go"))
	require.NoError(t, err)

	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}

		f, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		require.NoError(t, parseErr, "parsing %s", path)

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Doc == nil || fn.Body == nil {
				continue
			}
			if !citesCommonErrorsPositively(fn.Doc.Text()) {
				continue
			}
			if answersCode(fn.Body, "ValidationError") {
				controls++
			}
			if !answersCode(fn.Body, "ValidationException") {
				continue
			}
			found = append(found, finding{
				file: rel,
				line: fset.Position(fn.Pos()).Line,
				fn:   fn.Name.Name,
			})
		}
	}

	assert.Positive(t, controls,
		"no function was found answering ValidationError and citing a common-errors page, so the "+
			"detector below cannot fire and its green result means nothing — fix the predicates, "+
			"not this assertion")

	for _, v := range found {
		t.Errorf("%s:%d: %s answers ValidationException and sources it to a common-errors page in its doc comment; "+
			"no common-errors page publishes ValidationException in any of the three generations — "+
			"cite the operation's own Errors section, or answer ValidationError",
			v.file, v.line, v.fn)
	}
	assert.Empty(t, found)
}

// citesCommonErrorsPositively reports whether doc cites a common-errors page as a source,
// as opposed to naming it to say what it does not publish.
//
// The negated form is the one #950 and #1007 wrote most often — "absent from its Common
// Errors page", "CommonErrors, which does not list InvalidInput" — because ruling a page
// out is how those issues chose between codes. Treating it as a citation would flag the
// most carefully sourced comments in the tree, so it is detected per sentence: a sentence
// naming the page and a negation is not sourcing a code to it.
func citesCommonErrorsPositively(doc string) bool {
	for _, sentence := range strings.Split(doc, ".") {
		named := false
		for _, needle := range commonErrorsCitation {
			if strings.Contains(sentence, needle) {
				named = true
				break
			}
		}
		if !named {
			continue
		}
		lower := strings.ToLower(sentence)
		negated := strings.Contains(lower, "not ") ||
			strings.Contains(lower, "no ") ||
			strings.Contains(lower, "absent") ||
			strings.Contains(lower, "nowhere") ||
			strings.Contains(lower, "neither") ||
			strings.Contains(lower, "declined")
		if !negated {
			return true
		}
	}
	return false
}

// answersCode reports whether body constructs a composite literal whose Code field is the
// given string constant.
//
// It matches the shape every refusal in the tree uses — a Code: "…" key in an AWSError
// literal — rather than the type name, because the literal is sometimes built through a
// helper's return type that the AST at this point does not resolve.
func answersCode(body *ast.BlockStmt, code string) bool {
	answers := false
	ast.Inspect(body, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "Code" {
			return true
		}
		lit, ok := kv.Value.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if strings.Trim(lit.Value, `"`) == code {
			answers = true
			return false
		}
		return true
	})
	return answers
}
