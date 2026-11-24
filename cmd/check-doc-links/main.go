// Command check-doc-links fails when a Go comment links to an identifier that does not
// exist.
//
// A doc link is `[Name]`, `[Type.Member]` or `[pkg.Name]` in a comment, the syntax
// go/doc renders as a link. A link whose target is renamed or deleted keeps compiling
// and keeps reading as authoritative, so it sends a reader to code that is not there:
// `[operationNameResolvers]` named a map called operationResolvers, and
// `[WithDeployerPrincipal]` an option that does not exist (#1609).
//
// go/doc/comment recognizes only exported names as links, and the tree links unexported
// ones throughout, so this matches the bracket syntax itself, with go/doc's rule for what
// may surround it: the rune before `[` and after `]` must be a space or punctuation. A
// Markdown link `[text](url)` is not a doc link, nor is a bracket on a tab-indented line
// (a code block) or inside a quotation. `[Text]` is a URL link, and resolves, when the
// file defines it with `[Text]: URL`.
//
// A link resolves when:
//   - `[Name]` names a package-level declaration of the comment's package, a method or
//     field of any of its types, a predeclared identifier, or a package;
//   - `[Type.Member]` names a method or field of a package-level type;
//   - `[pkg.Name...]` starts with an imported or standard-library package (not
//     type-checked), or with another package of this repository, against which the rest
//     is resolved.
//
// A package and its external `_test` package share one scope, because each cites the
// other unqualified: a test names the function it pins, and a function the test that
// pins it.
//
// Usage:
//
//	go run ./cmd/check-doc-links [dir ...]
//
// With no arguments it walks the repository from the current directory. It exits 1 if
// any link dangles and 2 on an error.
package main

import (
	"fmt"
	"go/ast"
	"go/doc/comment"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

func main() {
	n, err := run(os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "check-doc-links:", err)
		os.Exit(2)
	}
	if n > 0 {
		fmt.Printf("\n%d doc link(s) name no identifier. Point each at the name it means, or drop the brackets.\n", n)
		os.Exit(1)
	}
}

// run checks every package under roots (default ".") and writes one line per dangling
// link to w. It returns how many it found.
func run(roots []string, w io.Writer) (int, error) {
	if len(roots) == 0 {
		roots = []string{"."}
	}
	dirs, err := goDirs(roots)
	if err != nil {
		return 0, err
	}
	fset := token.NewFileSet()
	var pkgs []*pkg
	repo := map[string]*scope{}
	for _, dir := range dirs {
		got, err := loadDir(fset, dir)
		if err != nil {
			return 0, err
		}
		for _, p := range got {
			if p.name != "main" && !strings.HasSuffix(p.name, "_test") {
				repo[p.name] = p.scope
			}
		}
		pkgs = append(pkgs, got...)
	}

	type finding struct {
		pos    token.Position
		target string
	}
	var found []finding
	for _, p := range pkgs {
		p.scope.repo = repo
		for _, f := range p.files {
			defs := linkDefs(f)
			for _, cg := range f.Comments {
				for _, l := range danglingLinks(cg, p.scope, defs) {
					found = append(found, finding{fset.Position(l.pos), l.target})
				}
			}
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].pos.Filename != found[j].pos.Filename {
			return found[i].pos.Filename < found[j].pos.Filename
		}
		return found[i].pos.Offset < found[j].pos.Offset
	})
	for _, f := range found {
		if _, err := fmt.Fprintf(w, "%s:%d: [%s]\n", f.pos.Filename, f.pos.Line, f.target); err != nil {
			return 0, fmt.Errorf("writing report: %w", err)
		}
	}
	return len(found), nil
}

// goDirs returns, sorted, every directory under roots holding a .go file, skipping
// hidden directories, node_modules, testdata and vendor.
func goDirs(roots []string) ([]string, error) {
	dirs := map[string]bool{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				name := d.Name()
				if path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "testdata" || name == "vendor") {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") {
				dirs[filepath.Dir(path)] = true
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walking %s: %w", root, err)
		}
	}
	out := make([]string, 0, len(dirs))
	for d := range dirs {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

// pkg is one package of one directory, with the scope its comments resolve against.
type pkg struct {
	name  string
	files []*ast.File
	scope *scope
}

// loadDir parses the .go files of dir into one pkg per package clause. A package and
// its external _test package get the same scope.
func loadDir(fset *token.FileSet, dir string) ([]*pkg, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	byName := map[string]*pkg{}
	var order []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}
		p := byName[f.Name.Name]
		if p == nil {
			p = &pkg{name: f.Name.Name}
			byName[f.Name.Name] = p
			order = append(order, f.Name.Name)
		}
		p.files = append(p.files, f)
	}
	shared := map[string]*scope{}
	out := make([]*pkg, 0, len(order))
	for _, name := range order {
		p := byName[name]
		base := strings.TrimSuffix(name, "_test")
		if shared[base] == nil {
			shared[base] = newScope()
		}
		p.scope = shared[base]
		for _, f := range p.files {
			p.scope.add(f)
		}
		out = append(out, p)
	}
	return out, nil
}

// scope is what a link inside one package may resolve to.
type scope struct {
	decls     map[string]bool            // package-level names
	members   map[string]map[string]bool // type name -> method and field names
	anyMember map[string]bool            // every method and field name, of any type
	pkgs      map[string]bool            // package names imported by any file
	repo      map[string]*scope          // the repository's packages, by name
}

func newScope() *scope {
	return &scope{
		decls:     map[string]bool{},
		members:   map[string]map[string]bool{},
		anyMember: map[string]bool{},
		pkgs:      map[string]bool{},
	}
}

func (s *scope) member(typ, name string) {
	if s.members[typ] == nil {
		s.members[typ] = map[string]bool{}
	}
	s.members[typ][name] = true
	s.anyMember[name] = true
}

// majorVersion matches an import path's major-version suffix: the package of
// github.com/go-chi/chi/v5 is chi.
var majorVersion = regexp.MustCompile(`^v[0-9]+$`)

// add records file's package-level declarations, methods, fields and imports.
func (s *scope) add(file *ast.File) {
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		name := path[strings.LastIndex(path, "/")+1:]
		if majorVersion.MatchString(name) {
			trimmed := strings.TrimSuffix(path, "/"+name)
			name = trimmed[strings.LastIndex(trimmed, "/")+1:]
		}
		if imp.Name != nil {
			name = imp.Name.Name
		}
		s.pkgs[name] = true
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil || len(d.Recv.List) == 0 {
				s.decls[d.Name.Name] = true
				continue
			}
			if typ := typeName(d.Recv.List[0].Type); typ != "" {
				s.member(typ, d.Name.Name)
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch sp := spec.(type) {
				case *ast.ValueSpec:
					for _, n := range sp.Names {
						s.decls[n.Name] = true
					}
				case *ast.TypeSpec:
					s.decls[sp.Name.Name] = true
					s.typeMembers(sp.Name.Name, sp.Type)
				}
			}
		}
	}
}

// typeMembers records the fields of a struct type and the methods of an interface type.
func (s *scope) typeMembers(typ string, expr ast.Expr) {
	var fields *ast.FieldList
	switch t := expr.(type) {
	case *ast.StructType:
		fields = t.Fields
	case *ast.InterfaceType:
		fields = t.Methods
	default:
		return
	}
	for _, f := range fields.List {
		for _, n := range f.Names {
			s.member(typ, n.Name)
		}
		if len(f.Names) == 0 { // embedded: named by the embedded type
			if name := typeName(f.Type); name != "" {
				s.member(typ, name)
			}
		}
	}
}

// typeName is the bare type name of a receiver or embedded-field expression.
func typeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return typeName(t.X)
	case *ast.IndexExpr:
		return typeName(t.X)
	case *ast.IndexListExpr:
		return typeName(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}

var predeclared = map[string]bool{
	"any": true, "bool": true, "byte": true, "comparable": true, "complex64": true,
	"complex128": true, "error": true, "float32": true, "float64": true, "int": true,
	"int8": true, "int16": true, "int32": true, "int64": true, "rune": true, "string": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"uintptr": true, "true": true, "false": true, "iota": true, "nil": true, "append": true,
	"cap": true, "clear": true, "close": true, "complex": true, "copy": true, "delete": true,
	"imag": true, "len": true, "make": true, "max": true, "min": true, "new": true,
	"panic": true, "print": true, "println": true, "real": true, "recover": true,
}

// isPkg reports whether name is a package a link may qualify: one the package imports,
// or a standard-library package go/doc recognizes by its bare name.
func (s *scope) isPkg(name string) bool {
	if s.pkgs[name] {
		return true
	}
	_, ok := comment.DefaultLookupPackage(name)
	return ok
}

// resolves reports whether the dotted link target names something.
func (s *scope) resolves(target string) bool {
	parts := strings.Split(target, ".")
	if len(parts) > 1 && !s.decls[parts[0]] {
		if other, ok := s.repo[parts[0]]; ok && other != s {
			return other.resolves(strings.Join(parts[1:], "."))
		}
		return s.isPkg(parts[0])
	}
	switch len(parts) {
	case 1:
		return s.decls[parts[0]] || s.anyMember[parts[0]] || predeclared[parts[0]] || s.isPkg(parts[0])
	case 2:
		return s.members[parts[0]][parts[1]]
	}
	return false
}

// link is one dangling doc link.
type link struct {
	pos    token.Pos
	target string
}

// linkRE matches a bracketed identifier path with go/doc's optional leading `*`.
var linkRE = regexp.MustCompile(`\[\*?([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\]`)

// linkDefRE matches a go/doc link definition, `[Text]: URL`, on a line of its own.
var linkDefRE = regexp.MustCompile(`^\s*\[([^\]]+)\]:\s*\S`)

// linkDefs returns the link-definition texts in a file. A definition makes `[Text]` a URL
// link anywhere in the file, which is how an AWS page is cited.
func linkDefs(f *ast.File) map[string]bool {
	defs := map[string]bool{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if m := linkDefRE.FindStringSubmatch(strings.TrimPrefix(c.Text, "//")); m != nil {
				defs[m[1]] = true
			}
		}
	}
	return defs
}

// danglingLinks returns the links in one comment group that resolve to nothing. defs
// holds the file's link definitions.
//
// A bracket inside a quotation is an editor's insertion or the quoted text's own
// bracket ("a parameter [that] exceeds", "Unresolved resource dependencies [X]"), not a
// link, so quote state is tracked across the group's lines and reset at a blank line.
func danglingLinks(cg *ast.CommentGroup, sc *scope, defs map[string]bool) []link {
	var out []link
	quoted := false
	for _, c := range cg.List {
		if !strings.HasPrefix(c.Text, "//") {
			continue // a /* */ block holds no doc links in this tree
		}
		text := c.Text[2:]
		if strings.TrimSpace(text) == "" {
			quoted = false
			continue
		}
		// gofmt indents every doc-comment code block with a tab; it holds code, not links.
		if strings.HasPrefix(text, "\t") {
			continue
		}
		if linkDefRE.MatchString(text) {
			continue
		}
		last := 0
		for _, m := range linkRE.FindAllStringSubmatchIndex(text, -1) {
			quoted = quoted != (quotes(text[last:m[0]])%2 == 1)
			last = m[0]
			before, after := text[:m[0]], text[m[1]:]
			if quoted || !boundary(before, true) || !boundary(after, false) {
				continue
			}
			if strings.HasPrefix(after, "(") {
				continue // a Markdown link
			}
			target := text[m[2]:m[3]]
			if defs[target] || sc.resolves(target) {
				continue
			}
			out = append(out, link{c.Slash + token.Pos(2+m[0]), target})
		}
		quoted = quoted != (quotes(text[last:])%2 == 1)
	}
	return out
}

// quotes counts the double quotation marks in s, straight or curly.
func quotes(s string) int {
	return strings.Count(s, `"`) + strings.Count(s, "\u201c") + strings.Count(s, "\u201d")
}

// boundary applies go/doc's rule: the rune beside a link must be a space or punctuation.
func boundary(s string, last bool) bool {
	if s == "" {
		return true
	}
	var r rune
	if last {
		r, _ = utf8.DecodeLastRuneInString(s)
	} else {
		r, _ = utf8.DecodeRuneInString(s)
	}
	return unicode.IsPunct(r) || unicode.IsSpace(r)
}
