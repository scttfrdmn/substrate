// Command gen-operation-catalog turns each plugin's dispatch switch into the exported
// per-plugin operation catalog substrate publishes, so that the set of operations
// substrate routes is readable without parsing Go source (issue #1095).
//
// # Why generated, and not declared
//
// Before this, the operation set existed only as `case` labels inside each plugin's
// HandleRequest switch. Nothing in the package could enumerate it, which is why #1015's
// drift check — "fail when a routed operation has no documentation row" — could not be
// written, and why two careful hand counts of the same tree disagreed by five.
//
// The obvious alternatives were both rejected on #1095, and the reasoning is worth having
// here rather than only in the issue:
//
//   - An `Operations() []string` method on [emulator.Plugin] holds the list *alongside*
//     the switch, so the list can drift from the router. That is the same class of defect
//     one level up, and it would break every out-of-tree Plugin implementation.
//   - A table-driven dispatch (`map[string]handlerFunc`) makes the catalog *be* the
//     router and so cannot disagree with it at all. That is the better end state, but it
//     rewrites the dispatch of 67 plugins in service of a documentation check, against
//     #1095's "no behavior change to any operation".
//
// Generating the catalog from the router's own AST gets the drift-proofness of the second
// for the cost of the first: the catalog is a projection of the switch rather than a
// second copy of it, and `make operation-catalog-check` fails when the projection is
// stale. The residual gap — a projection that is current but wrong because the extractor
// stopped recognizing a dispatch shape — is closed by refusing to emit an empty catalog:
// an unrecognized HandleRequest is a hard error here, not a silently short list.
//
// # What it reads
//
// Two things, both in emulator/:
//
//   - The registration table in plugins.go, for the list of plugin types, and each type's
//     Name method for the key the registry files it under. That key — "ec2",
//     "elasticloadbalancing", "execute-api" — is the vocabulary PluginRegistry.Names,
//     routing.go and cmd/gen-service-reference share, so it is what the catalog is keyed
//     by rather than the Go type name.
//   - Each registered type's HandleRequest, for switches whose tag names an operation
//     (`op`, `action`, `operation`, `req.Operation`). A REST-path service needs no special
//     case: its parser hands the name it derived straight to that switch, as in
//     `bucket, key, op := parseS3Operation(req); req.Operation = op; switch op {…}`.
//
// Two plugins route no operation names at all and are declared as exceptions below, so
// that an empty catalog is always a stated decision rather than an extraction failure.
//
// # Usage
//
//	go run ./cmd/gen-operation-catalog         # regenerate emulator/operation_catalog_gen.go
//	go run ./cmd/gen-operation-catalog -check  # exit non-zero if it is out of date
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// emptyByDesign names the plugins whose HandleRequest routes no operation names, with the
// reason each one does not. A plugin is allowed an empty catalog only if it appears here,
// and a plugin listed here that turns out to route operations is an error too, so neither
// direction of this exception can go stale unnoticed.
var emptyByDesign = map[string]string{
	"execute-api": "APIGatewayProxyPlugin serves a deployed API's own routes: it parses the API id\n" +
		"// from the Host header and the stage and resource from the path, then invokes the\n" +
		"// integration. There is no operation name in that protocol — AWS's action for it is\n" +
		"// the single execute-api:Invoke.",
	"opensearch": "OpenSearchPlugin dispatches on the HTTP method plus path shape, so its\n" +
		"// req.Operation stays an HTTP verb end to end; it is also the one service absent from\n" +
		"// operationResolvers in operation_names.go. AWS publishes per-verb actions there\n" +
		"// (es:ESHttpGet and siblings), so the verb is very nearly the operation name.",
}

// operationTags are the identifiers a dispatch switch tags on. A switch on anything else —
// a filter name, an attribute name, a resource type — is not a dispatch and is skipped,
// which is the distinction a `case "…"` grep cannot make and the reason it miscounts.
var operationTags = map[string]bool{"op": true, "action": true, "operation": true}

// maxClaimDepth bounds how far a claim chain is followed. ConfigService and Organizations
// route through one level of `func(string) (handler, bool)` claim helpers; nothing in the
// tree nests them deeper, and a bound keeps a future cycle from hanging generation.
const maxClaimDepth = 2

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "gen-operation-catalog:", err)
		os.Exit(1)
	}
}

// run parses args, builds the catalog and either writes the generated file or, under -check,
// reports that it is stale.
//
// main is a wrapper around it so that the -check flag's own behavior is reachable from a
// test. That flag is the whole reason the catalog can be relied on as a projection of the
// router rather than as a snapshot of it, and an untested drift check is one nobody knows
// still fires (#739).
func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("gen-operation-catalog", flag.ContinueOnError)
	fs.SetOutput(stdout)
	check := fs.Bool("check", false, "exit non-zero if the generated file is out of date instead of writing it")
	dir := fs.String("dir", "emulator", "directory holding the plugin sources")
	out := fs.String("out", filepath.Join("emulator", "operation_catalog_gen.go"), "path to the generated Go file")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	cat, err := build(*dir)
	if err != nil {
		return err
	}

	generated, err := render(cat)
	if err != nil {
		return err
	}

	existing, err := os.ReadFile(*out)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", *out, err)
	}

	if *check {
		if !bytes.Equal(existing, generated) {
			return fmt.Errorf("%s is out of date with %s; run `make operation-catalog` and commit the result", *out, *dir)
		}
		return nil
	}

	if bytes.Equal(existing, generated) {
		_, _ = fmt.Fprintf(stdout, "%s already up to date (%d plugins, %d operations)\n", *out, len(cat), cat.total())
		return nil
	}
	if err := os.WriteFile(*out, generated, 0o644); err != nil { //nolint:gosec // generated source, world-readable is fine.
		return fmt.Errorf("write %s: %w", *out, err)
	}
	_, _ = fmt.Fprintf(stdout, "updated %s (%d plugins, %d operations)\n", *out, len(cat), cat.total())
	return nil
}

// catalog maps a registered plugin name to the operations that plugin routes, sorted.
type catalog map[string][]string

// total returns the number of operations across every plugin in the catalog.
func (c catalog) total() int {
	n := 0
	for _, ops := range c {
		n += len(ops)
	}
	return n
}

// services returns the catalog's plugin names, sorted.
func (c catalog) services() []string {
	names := make([]string, 0, len(c))
	for name := range c {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// build parses the plugin sources in dir and extracts the catalog from them.
func build(dir string) (catalog, error) {
	fset := token.NewFileSet()
	files, err := parseDir(fset, dir)
	if err != nil {
		return nil, err
	}
	return buildFrom(files)
}

// buildFrom extracts one catalog entry per registered plugin. It fails rather than
// returning a short catalog: an entry missing, empty without an [emptyByDesign] reason, or
// non-empty despite one, is a generation error.
//
// It takes parsed files rather than a directory so that every one of those refusals is
// reachable from a test on a few lines of synthetic source. A generator whose failure modes
// are only reachable by breaking the real tree is one whose refusals nobody has seen fire.
func buildFrom(files []*ast.File) (catalog, error) {
	methods := indexMethods(files)
	consts := indexStringConsts(files)
	types, err := registeredTypes(files)
	if err != nil {
		return nil, err
	}

	cat := make(catalog, len(types))
	for _, typeName := range types {
		service, err := serviceName(typeName, methods, consts)
		if err != nil {
			return nil, err
		}
		if prior, dup := cat[service]; dup {
			return nil, fmt.Errorf("two plugins report Name() == %q (%d operations already cataloged); "+
				"the catalog is keyed by the registry key, which must be unique", service, len(prior))
		}
		handler, ok := methods[typeName+".HandleRequest"]
		if !ok {
			return nil, fmt.Errorf("plugin %s (%q) has no HandleRequest method", typeName, service)
		}
		ops := operations(handler, methods, 0)
		_, exempt := emptyByDesign[service]
		switch {
		case len(ops) == 0 && !exempt:
			return nil, fmt.Errorf("no operations extracted from %s.HandleRequest: either its dispatch is a shape this "+
				"generator does not recognize — add the shape rather than leaving the catalog short — or it genuinely "+
				"routes no operation names, in which case add %q to emptyByDesign with the reason", typeName, service)
		case len(ops) > 0 && exempt:
			return nil, fmt.Errorf("%q is listed in emptyByDesign but %s.HandleRequest routes %d operations; "+
				"remove the exception", service, typeName, len(ops))
		}
		cat[service] = ops
	}
	return cat, nil
}

// parseDir parses every non-test Go file in dir.
func parseDir(fset *token.FileSet, dir string) ([]*ast.File, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", dir, err)
	}
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no Go sources found in %s", dir)
	}
	return files, nil
}

// indexMethods keys every method declaration by "ReceiverType.Name". Plain functions are
// skipped: a dispatch switch and its claim helpers are always methods on the plugin.
func indexMethods(files []*ast.File) map[string]*ast.FuncDecl {
	methods := make(map[string]*ast.FuncDecl)
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 || fn.Body == nil {
				continue
			}
			if recv := receiverType(fn.Recv.List[0].Type); recv != "" {
				methods[recv+"."+fn.Name.Name] = fn
			}
		}
	}
	return methods
}

// registeredTypes reads the registration table in RegisterDefaultPlugins and returns the
// plugin type name of each entry. Reading the table rather than holding a second copy of it
// is what makes a newly registered plugin a generation error instead of a silent omission
// from the catalog.
//
// Only the type is taken from the table. The string beside it there is a label for the
// error a failed initialization is wrapped with, not the registry key — PluginRegistry
// keys by Plugin.Name(), and for ELB the two differ ("elb" against
// "elasticloadbalancing"). Keying the catalog by the label instead is a mistake that
// surfaces immediately, because cmd/gen-service-reference compares the catalog against
// reg.Names(); see [serviceName] for where the key actually comes from.
func registeredTypes(files []*ast.File) ([]string, error) {
	var register *ast.FuncDecl
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Name.Name == "RegisterDefaultPlugins" {
				register = fn
			}
		}
	}
	if register == nil {
		return nil, fmt.Errorf("RegisterDefaultPlugins not found; it is the source of the plugin list")
	}

	var types []string
	seen := make(map[string]bool)
	ast.Inspect(register.Body, func(n ast.Node) bool {
		entry, ok := n.(*ast.CompositeLit)
		if !ok || entry.Type != nil || len(entry.Elts) == 0 {
			return true
		}
		typeName := pluginTypeOf(entry.Elts[0])
		if typeName == "" || seen[typeName] {
			return true
		}
		seen[typeName] = true
		types = append(types, typeName)
		return true
	})
	if len(types) == 0 {
		return nil, fmt.Errorf("no plugin registrations found in RegisterDefaultPlugins; its table shape changed")
	}
	sort.Strings(types)
	return types, nil
}

// indexStringConsts records every package-level string constant, so that a Name() which
// returns a named constant can be resolved. ELB is why this exists: its Name() returns
// elbServiceName, declared in elb_authz.go.
func indexStringConsts(files []*ast.File) map[string]string {
	consts := make(map[string]string)
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != len(value.Values) {
					continue
				}
				for i, name := range value.Names {
					if s, err := stringLit(value.Values[i]); err == nil {
						consts[name.Name] = s
					}
				}
			}
		}
	}
	return consts
}

// serviceName returns the key the registry files a plugin under: whatever its Name() method
// returns, either as a literal or as a package-level string constant.
//
// It is an error rather than a fallback if Name() is a shape this cannot read. A guessed key
// — the type name lowercased, say — would produce a catalog that looks complete and that
// nothing can look a plugin up in, which is the failure mode this whole generator is built
// to avoid.
func serviceName(typeName string, methods map[string]*ast.FuncDecl, consts map[string]string) (string, error) {
	name, ok := methods[typeName+".Name"]
	if !ok {
		return "", fmt.Errorf("plugin %s has no Name method; it is the registry key the catalog is keyed by", typeName)
	}
	if len(name.Body.List) != 1 {
		return "", fmt.Errorf("%s.Name has %d statements; this generator reads a single return", typeName, len(name.Body.List))
	}
	ret, ok := name.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return "", fmt.Errorf("%s.Name does not return a single value", typeName)
	}
	if s, err := stringLit(ret.Results[0]); err == nil {
		return s, nil
	}
	ident, ok := ret.Results[0].(*ast.Ident)
	if !ok {
		return "", fmt.Errorf("%s.Name returns neither a string literal nor a named constant", typeName)
	}
	s, ok := consts[ident.Name]
	if !ok {
		return "", fmt.Errorf("%s.Name returns %s, which is not a package-level string constant", typeName, ident.Name)
	}
	return s, nil
}

// pluginTypeOf returns the type name of a `&XPlugin{}` registration element, or "".
func pluginTypeOf(e ast.Expr) string {
	unary, ok := e.(*ast.UnaryExpr)
	if !ok || unary.Op != token.AND {
		return ""
	}
	lit, ok := unary.X.(*ast.CompositeLit)
	if !ok {
		return ""
	}
	ident, ok := lit.Type.(*ast.Ident)
	if !ok || !strings.HasSuffix(ident.Name, "Plugin") {
		return ""
	}
	return ident.Name
}

// operations gathers the case-clause strings of every dispatch switch reachable from fn,
// following claim helpers when fn dispatches through a claim chain rather than a switch.
func operations(fn *ast.FuncDecl, methods map[string]*ast.FuncDecl, depth int) []string {
	if fn == nil || fn.Body == nil || depth > maxClaimDepth {
		return nil
	}
	seen := make(map[string]bool)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok || !isDispatchTag(sw.Tag) {
			return true
		}
		for _, stmt := range sw.Body.List {
			clause, ok := stmt.(*ast.CaseClause)
			if !ok {
				continue
			}
			for _, expr := range clause.List {
				if op, err := stringLit(expr); err == nil {
					seen[op] = true
				}
			}
		}
		return true
	})

	if len(seen) == 0 {
		for _, op := range claimChainOperations(fn, methods, depth) {
			seen[op] = true
		}
	}

	ops := make([]string, 0, len(seen))
	for op := range seen {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return ops
}

// claimChainOperations follows the claim helpers ConfigService and Organizations dispatch
// through: methods on the *same* receiver with the signature func(string) (T, bool), each
// of which claims the operations it handles and switches on them itself.
//
// The scoping is load-bearing in both halves. Restricting to the same receiver and to that
// signature is what keeps the walk inside the plugin: a version that followed any selector
// expression swept 854 operations into apigateway-proxy by wandering into other plugins'
// switches. Following them at all is what finds the 59 operations these two plugins route
// that appear in no switch inside HandleRequest, and which every hand count therefore
// missed.
func claimChainOperations(fn *ast.FuncDecl, methods map[string]*ast.FuncDecl, depth int) []string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return nil
	}
	recv := receiverType(fn.Recv.List[0].Type)
	if recv == "" {
		return nil
	}
	var ops []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		claim, ok := methods[recv+"."+sel.Sel.Name]
		if !ok || claim == fn || !isClaimHelper(claim) {
			return true
		}
		ops = append(ops, operations(claim, methods, depth+1)...)
		return true
	})
	return ops
}

// isDispatchTag reports whether a switch tag names the operation being dispatched.
func isDispatchTag(e ast.Expr) bool {
	switch tag := e.(type) {
	case *ast.Ident:
		return operationTags[tag.Name]
	case *ast.SelectorExpr:
		ident, ok := tag.X.(*ast.Ident)
		return ok && ident.Name == "req" && tag.Sel.Name == "Operation"
	}
	return false
}

// isClaimHelper reports whether fn has a claim helper's signature, func(string) (T, bool).
func isClaimHelper(fn *ast.FuncDecl) bool {
	params, results := fn.Type.Params, fn.Type.Results
	if params == nil || results == nil || len(params.List) != 1 || len(results.List) != 2 {
		return false
	}
	if ident, ok := params.List[0].Type.(*ast.Ident); !ok || ident.Name != "string" {
		return false
	}
	ident, ok := results.List[1].Type.(*ast.Ident)
	return ok && ident.Name == "bool"
}

// receiverType returns the bare type name of a method receiver, pointer or value.
func receiverType(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverType(t.X)
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// stringLit unquotes an untyped string literal, and errors for any other expression.
func stringLit(e ast.Expr) (string, error) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", fmt.Errorf("not a string literal")
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", fmt.Errorf("unquote %s: %w", lit.Value, err)
	}
	return s, nil
}

// render writes the generated source for cat.
func render(cat catalog) ([]byte, error) {
	var buf bytes.Buffer

	fmt.Fprintln(&buf, "// Code generated by cmd/gen-operation-catalog; DO NOT EDIT.")
	fmt.Fprintln(&buf, "//")
	fmt.Fprintln(&buf, "// Source: the dispatch switch in each plugin's HandleRequest, keyed by the name that")
	fmt.Fprintln(&buf, "// plugin is registered under in RegisterDefaultPlugins. The catalog is a projection of")
	fmt.Fprintln(&buf, "// the router rather than a second copy of it, so it cannot describe an operation the")
	fmt.Fprintln(&buf, "// router does not route; `make operation-catalog-check` fails when the projection is")
	fmt.Fprintln(&buf, "// stale. See cmd/gen-operation-catalog for why this is generated and not declared (#1095).")
	fmt.Fprintln(&buf, "//")
	fmt.Fprintf(&buf, "// %d plugins, %d routed operations.\n", len(cat), cat.total())
	fmt.Fprintln(&buf, "//")
	fmt.Fprintln(&buf, "// Regenerate with `make operation-catalog`.")
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "package emulator")
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "// routedOperations maps a registered plugin name to the operations that plugin routes,")
	fmt.Fprintln(&buf, "// sorted. Read it through [RoutedServices] and [RoutedOperations]; a caller outside the")
	fmt.Fprintln(&buf, "// package has to, and a caller inside it should, because those return copies.")
	fmt.Fprintln(&buf, "var routedOperations = map[string][]string{")

	for _, service := range cat.services() {
		ops := cat[service]
		if reason, ok := emptyByDesign[service]; ok {
			fmt.Fprintf(&buf, "\t// %s\n", reason)
			fmt.Fprintf(&buf, "\t%q: {},\n", service)
			continue
		}
		fmt.Fprintf(&buf, "\t%q: {\n", service)
		for _, op := range ops {
			fmt.Fprintf(&buf, "\t\t%q,\n", op)
		}
		fmt.Fprintln(&buf, "\t},")
	}
	fmt.Fprintln(&buf, "}")

	src, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated source: %w", err)
	}
	return src, nil
}
