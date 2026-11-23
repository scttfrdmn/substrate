// Command gen-cfn-schema turns AWS's published CloudFormation resource specification into
// the Go table substrate consults when it validates a template (#1605): which resource
// types CloudFormation recognizes, which properties each type declares, and which
// Fn::GetAtt attributes it publishes.
//
// # Why this data, and why generated
//
// CloudFormation refuses a template naming a type it does not recognize ("Template format
// error: Unrecognized resource types: [X]") and fails a resource whose Properties carry a
// key its type does not declare ("#: extraneous key [Y] is not permitted"). Substrate can
// only make either check against the list AWS itself publishes, and AWS publishes it per
// Region as the resource specification:
//
//	https://d1uauaxba7bl26.cloudfront.net/latest/gzip/CloudFormationResourceSpecification.json
//
// which is the us-east-1 file in the "CloudFormation resource specification" table of the
// CloudFormation User Guide. us-east-1 is chosen because it is substrate's default Region
// and the Region where new types arrive first, so it is the broadest single list.
//
// The specification is about 20 MB, almost all of it documentation URLs and property-type
// definitions substrate does not read. -fetch strips it to the resource types' own property
// names (with their required flag and value kind) and attribute names (with their kind),
// and vendors that, gzipped, under emulator/cfnschema. Generation, the drift check and the
// test suite are offline: no CI job and no test may depend on network access. Refreshing
// the snapshot is a deliberate, human-run step, and the diff to the generated Go file it
// produces is the review.
//
// # Usage
//
//	go run ./cmd/gen-cfn-schema           # regenerate emulator/cfn_schema_gen.go
//	go run ./cmd/gen-cfn-schema -check    # exit non-zero if it is out of date (offline)
//	go run ./cmd/gen-cfn-schema -fetch    # refresh emulator/cfnschema (network)
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// specURL is the us-east-1 CloudFormation resource specification, as the User Guide's
// "CloudFormation resource specification" table lists it. The server answers it gzipped.
const specURL = "https://d1uauaxba7bl26.cloudfront.net/latest/gzip/CloudFormationResourceSpecification.json"

// snapshotName is the vendored, stripped specification under the data directory.
const snapshotName = "us-east-1.json.gz"

// snapshot is the stripped specification: the version AWS stamps on it, and each resource
// type's properties and attributes. Everything else the specification carries — the
// documentation links, update behaviors, the PropertyTypes section — is dropped.
type snapshot struct {
	Version       string                  `json:"ResourceSpecificationVersion"`
	ResourceTypes map[string]resourceType `json:"ResourceTypes"`
}

// resourceType is one resource type's top-level property names and attribute names.
type resourceType struct {
	Properties map[string]property `json:"Properties"`
	Attributes map[string]string   `json:"Attributes"`
}

// property is one top-level property: whether the type requires it, and the kind of
// value it takes (a primitive type name, or List, Map or Object).
type property struct {
	Required bool   `json:"Required"`
	Kind     string `json:"Kind"`
}

// rawSpec is the shape AWS serves, of which this tool keeps a subset.
type rawSpec struct {
	Version       string                     `json:"ResourceSpecificationVersion"`
	ResourceTypes map[string]rawResourceType `json:"ResourceTypes"`
}

// rawResourceType is one resource type as AWS serves it.
type rawResourceType struct {
	Properties map[string]rawValue `json:"Properties"`
	Attributes map[string]rawValue `json:"Attributes"`
}

// rawValue is one property or attribute as AWS serves it. Exactly one of PrimitiveType
// and Type is set: a primitive, or List, Map or the name of a property type.
type rawValue struct {
	Required      bool   `json:"Required"`
	PrimitiveType string `json:"PrimitiveType"`
	Type          string `json:"Type"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the command, returning its exit status, so the write and check paths can be tested
// without a subprocess.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("gen-cfn-schema", flag.ContinueOnError)
	flags.SetOutput(stderr)
	check := flags.Bool("check", false, "exit non-zero if the generated file is out of date instead of writing it")
	fetch := flags.Bool("fetch", false, "refresh the vendored snapshot from AWS (requires network access)")
	dir := flags.String("data", filepath.Join("emulator", "cfnschema"), "directory holding the vendored snapshot")
	out := flags.String("out", filepath.Join("emulator", "cfn_schema_gen.go"), "path to the generated Go file")
	url := flags.String("url", specURL, "the specification to fetch with -fetch")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	fail := func(err error) int {
		_, _ = fmt.Fprintln(stderr, "gen-cfn-schema:", err)
		return 1
	}

	if *fetch {
		if err := refresh(*dir, *url); err != nil {
			return fail(err)
		}
		return 0
	}

	snap, err := load(filepath.Join(*dir, snapshotName))
	if err != nil {
		return fail(err)
	}
	generated, err := render(snap)
	if err != nil {
		return fail(err)
	}
	existing, err := os.ReadFile(*out)
	if err != nil && !os.IsNotExist(err) {
		return fail(fmt.Errorf("read %s: %w", *out, err))
	}
	if *check {
		if !bytes.Equal(existing, generated) {
			_, _ = fmt.Fprintf(stderr, "gen-cfn-schema: %s is out of date with %s; run `make cfn-schema` and commit the result\n", *out, *dir)
			return 1
		}
		return 0
	}
	if bytes.Equal(existing, generated) {
		_, _ = fmt.Fprintf(stdout, "%s already up to date (%d resource types)\n", *out, len(snap.ResourceTypes))
		return 0
	}
	if err := os.WriteFile(*out, generated, 0o644); err != nil { //nolint:gosec // generated source, world-readable is fine.
		return fail(err)
	}
	_, _ = fmt.Fprintf(stdout, "updated %s (%d resource types)\n", *out, len(snap.ResourceTypes))
	return 0
}

// load reads and validates the vendored snapshot.
func load(path string) (snapshot, error) {
	f, err := os.Open(path) //nolint:gosec // path is the fixed snapshot location.
	if err != nil {
		return snapshot{}, fmt.Errorf("open %s: %w (run `make cfn-schema-fetch` to create it)", path, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return snapshot{}, fmt.Errorf("gunzip %s: %w", path, err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return snapshot{}, fmt.Errorf("read %s: %w", path, err)
	}
	var snap snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return snapshot{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if snap.Version == "" || len(snap.ResourceTypes) == 0 {
		return snapshot{}, fmt.Errorf("%s: snapshot has no version or no resource types", path)
	}
	return snap, nil
}

// refresh downloads the specification from url, strips it and writes the gzipped snapshot. It is
// the only part of this tool that touches the network, and it is never run by CI or a test.
func refresh(dir, url string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Get(url) //nolint:noctx // one-shot generator fetch with a client timeout.
	if err != nil {
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: HTTP %d", url, resp.StatusCode)
	}
	// net/http undoes a Content-Encoding: gzip transparently; a body that is still gzip
	// (served as an octet stream) is undone here.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s: %w", url, err)
	}
	if len(body) > 2 && body[0] == 0x1f && body[1] == 0x8b {
		zr, zerr := gzip.NewReader(bytes.NewReader(body))
		if zerr != nil {
			return fmt.Errorf("gunzip %s: %w", url, zerr)
		}
		if body, err = io.ReadAll(zr); err != nil {
			return fmt.Errorf("gunzip %s: %w", url, err)
		}
	}
	var raw rawSpec
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("parse %s: %w", url, err)
	}
	snap := strip(raw)
	encoded, err := json.Marshal(snap) // map keys are sorted, so the encoding is byte-stable.
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return fmt.Errorf("gzip snapshot: %w", err)
	}
	// No name and a zero ModTime in the header, so a refresh that changes nothing
	// produces a byte-identical file.
	if _, err := zw.Write(encoded); err != nil {
		return fmt.Errorf("gzip snapshot: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("gzip snapshot: %w", err)
	}
	path := filepath.Join(dir, snapshotName)
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil { //nolint:gosec // vendored data, world-readable is fine.
		return fmt.Errorf("write %s: %w", path, err)
	}
	fmt.Printf("fetched %s (specification %s, %d resource types, %d bytes)\n",
		path, snap.Version, len(snap.ResourceTypes), buf.Len())
	return nil
}

// strip reduces the specification to what the generated table needs.
func strip(raw rawSpec) snapshot {
	snap := snapshot{Version: raw.Version, ResourceTypes: make(map[string]resourceType, len(raw.ResourceTypes))}
	for name, rt := range raw.ResourceTypes {
		out := resourceType{
			Properties: make(map[string]property, len(rt.Properties)),
			Attributes: make(map[string]string, len(rt.Attributes)),
		}
		for p, v := range rt.Properties {
			out.Properties[p] = property{Required: v.Required, Kind: kindOf(v)}
		}
		for a, v := range rt.Attributes {
			out.Attributes[a] = kindOf(v)
		}
		snap.ResourceTypes[name] = out
	}
	return snap
}

// kindOf names the kind of value a property or attribute takes: its primitive type, or
// List or Map, or Object for a property type.
func kindOf(v rawValue) string {
	switch {
	case v.PrimitiveType != "":
		return v.PrimitiveType
	case v.Type == "List", v.Type == "Map":
		return v.Type
	default:
		return "Object"
	}
}

// kindConst maps a kind name to the Go constant the generated file uses.
var kindConst = map[string]string{
	"String":    "cfnKindString",
	"Integer":   "cfnKindInteger",
	"Long":      "cfnKindLong",
	"Double":    "cfnKindDouble",
	"Boolean":   "cfnKindBoolean",
	"Timestamp": "cfnKindTimestamp",
	"Json":      "cfnKindJSON",
	"List":      "cfnKindList",
	"Map":       "cfnKindMap",
	"Object":    "cfnKindObject",
}

// render produces the generated Go source for the snapshot.
func render(snap snapshot) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString("// Code generated by cmd/gen-cfn-schema; DO NOT EDIT.\n//\n")
	b.WriteString("// Source: the AWS CloudFormation resource specification for us-east-1,\n")
	fmt.Fprintf(&b, "// %s,\n", specURL)
	fmt.Fprintf(&b, "// version %s, stripped and vendored under emulator/cfnschema.\n//\n", snap.Version)
	b.WriteString("// Regenerate with `make cfn-schema`; refresh the snapshot with `make cfn-schema-fetch`\n")
	b.WriteString("// (network).\n\npackage emulator\n\n")
	fmt.Fprintf(&b, "// cfnSchemaVersion is the ResourceSpecificationVersion the table was generated from.\nconst cfnSchemaVersion = %q\n\n", snap.Version)
	b.WriteString(`// cfnSchemaTypes lists every resource type the specification publishes, sorted by name,
// each with its top-level properties and its Fn::GetAtt attributes, both sorted by name.
// Read it through [cfnSchemaLookup], which binary-searches it.
var cfnSchemaTypes = []cfnSchemaType{
`)
	names := make([]string, 0, len(snap.ResourceTypes))
	for n := range snap.ResourceTypes {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		rt := snap.ResourceTypes[n]
		fmt.Fprintf(&b, "\t{Name: %q,\n", n)
		props := sortedKeys(rt.Properties)
		if len(props) > 0 {
			b.WriteString("\t\tProperties: []cfnSchemaProperty{\n")
			for _, p := range props {
				k, err := constFor(rt.Properties[p].Kind, n, p)
				if err != nil {
					return nil, err
				}
				fmt.Fprintf(&b, "\t\t\t{%q, %t, %s},\n", p, rt.Properties[p].Required, k)
			}
			b.WriteString("\t\t},\n")
		}
		attrs := sortedKeys(rt.Attributes)
		if len(attrs) > 0 {
			b.WriteString("\t\tAttributes: []cfnSchemaAttribute{\n")
			for _, a := range attrs {
				k, err := constFor(rt.Attributes[a], n, a)
				if err != nil {
					return nil, err
				}
				fmt.Fprintf(&b, "\t\t\t{%q, %s},\n", a, k)
			}
			b.WriteString("\t\t},\n")
		}
		b.WriteString("\t},\n")
	}
	b.WriteString("}\n")
	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated source: %w", err)
	}
	return formatted, nil
}

func constFor(kind, typ, member string) (string, error) {
	k, ok := kindConst[kind]
	if !ok {
		return "", fmt.Errorf("%s.%s: unknown kind %q; add it to kindConst and to emulator/cfn_schema.go", typ, member, kind)
	}
	return k, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
