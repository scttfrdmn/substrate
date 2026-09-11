// Command gen-authz-reference turns AWS's published Service Reference Information into
// the Go table substrate consults when it decides which resource a request is authorized
// against (issues #762 and #770).
//
// # Why this data, and why generated
//
// Both issues require a per-operation answer to "does this action support resource-level
// permissions, and for which resource types" — a table AWS publishes in the Service
// Authorization Reference. Those HTML pages render their tables in JavaScript, so they
// cannot be read by a fetch; docs/services.md records that dead end for both ELB pages,
// which is why some ELB parameters are still authorized against "*". AWS publishes the
// same data as JSON at
//
//	https://servicereference.us-east-1.amazonaws.com/v1/{service}/{service}.json
//
// where an action's Resources list names the resource types it supports and an action
// with no Resources list supports none. That is machine-readable, so every row of the
// generated table is cited by construction rather than by a comment, and a test can
// assert substrate's own per-operation tables agree with it.
//
// The snapshot is vendored under emulator/authzref rather than fetched at generate time
// so that generation, the drift check and the test suite are all offline: no CI job and
// no test may depend on network access. Refreshing the snapshot is a deliberate,
// human-run step (-fetch), and the diff it produces is the review.
//
// # Usage
//
//	go run ./cmd/gen-authz-reference           # regenerate emulator/authz_reference_gen.go
//	go run ./cmd/gen-authz-reference -check    # exit non-zero if it is out of date (offline)
//	go run ./cmd/gen-authz-reference -fetch    # refresh emulator/authzref/*.json (network)
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// services are the services whose request resource substrate derives per operation, and
// so the only ones whose reference data it needs. Adding a service here is what makes its
// actions available to a per-operation resource table; nothing else consults the file.
var services = []string{"ec2", "iam"}

// referenceEndpoint is AWS's Service Reference Information endpoint. The index at the
// root lists every service; the per-service document is the one this tool prunes.
const referenceEndpoint = "https://servicereference.us-east-1.amazonaws.com/v1/%s/%s.json"

// service is the pruned form of a Service Reference Information document: the action
// names, the resource types each action supports, and the ARN format of each type.
// Everything else AWS publishes there — condition keys, per-action annotations, the
// Operations list — is dropped, because substrate reads its condition keys from the API
// reference and would have no use for a second, differently-shaped copy.
type service struct {
	Name      string     `json:"Name"`
	Version   string     `json:"Version"`
	Actions   []action   `json:"Actions"`
	Resources []resource `json:"Resources"`
}

// action is one API action and the resource types it supports. An empty Resources means
// AWS publishes no resource type for it, which is the published way of saying the action
// does not support resource-level permissions and its request resource is "*".
type action struct {
	Name      string   `json:"Name"`
	Resources []string `json:"Resources"`
}

// resource is one resource type and the ARN formats AWS documents for it.
type resource struct {
	Name       string   `json:"Name"`
	ARNFormats []string `json:"ARNFormats"`
}

// rawService is the shape AWS serves, of which this tool keeps a subset. AWS nests an
// action's resource types as objects with a Name — and further keys this tool drops —
// where the pruned form keeps the names alone.
type rawService struct {
	Name      string        `json:"Name"`
	Version   string        `json:"Version"`
	Actions   []rawAction   `json:"Actions"`
	Resources []rawResource `json:"Resources"`
}

// rawAction is one action as AWS serves it.
type rawAction struct {
	Name      string            `json:"Name"`
	Resources []rawActionTarget `json:"Resources"`
}

// rawActionTarget names one resource type an action supports.
type rawActionTarget struct {
	Name string `json:"Name"`
}

// rawResource is one resource type as AWS serves it.
type rawResource struct {
	Name       string   `json:"Name"`
	ARNFormats []string `json:"ARNFormats"`
}

func main() {
	check := flag.Bool("check", false, "exit non-zero if the generated file is out of date instead of writing it")
	fetch := flag.Bool("fetch", false, "refresh the vendored snapshots from AWS (requires network access)")
	dir := flag.String("data", filepath.Join("emulator", "authzref"), "directory holding the vendored snapshots")
	out := flag.String("out", filepath.Join("emulator", "authz_reference_gen.go"), "path to the generated Go file")
	flag.Parse()

	if *fetch {
		if err := refresh(*dir); err != nil {
			fmt.Fprintln(os.Stderr, "gen-authz-reference:", err)
			os.Exit(1)
		}
		return
	}

	svcs, err := load(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-authz-reference:", err)
		os.Exit(1)
	}

	generated, err := render(svcs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-authz-reference:", err)
		os.Exit(1)
	}

	existing, err := os.ReadFile(*out)
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "gen-authz-reference: read %s: %v\n", *out, err)
		os.Exit(1)
	}

	if *check {
		if !bytes.Equal(existing, generated) {
			fmt.Fprintf(os.Stderr, "gen-authz-reference: %s is out of date with %s; run `make authz-reference` and commit the result\n", *out, *dir)
			os.Exit(1)
		}
		return
	}

	if bytes.Equal(existing, generated) {
		fmt.Printf("%s already up to date (%d services)\n", *out, len(svcs))
		return
	}
	if err := os.WriteFile(*out, generated, 0o644); err != nil { //nolint:gosec // generated source, world-readable is fine.
		fmt.Fprintln(os.Stderr, "gen-authz-reference:", err)
		os.Exit(1)
	}
	fmt.Printf("updated %s (%d services)\n", *out, len(svcs))
}

// load reads and validates the vendored snapshot for every service in [services].
func load(dir string) ([]service, error) {
	svcs := make([]service, 0, len(services))
	for _, name := range services {
		path := filepath.Join(dir, name+".json")
		raw, err := os.ReadFile(path) //nolint:gosec // path is built from the fixed service list.
		if err != nil {
			return nil, fmt.Errorf("read %s: %w (run `make authz-reference-fetch` to create it)", path, err)
		}
		var svc service
		if err := json.Unmarshal(raw, &svc); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if svc.Name != name {
			return nil, fmt.Errorf("%s: snapshot names service %q", path, svc.Name)
		}
		if len(svc.Actions) == 0 {
			return nil, fmt.Errorf("%s: snapshot lists no actions", path)
		}
		svcs = append(svcs, svc)
	}
	return svcs, nil
}

// refresh downloads each service's document, prunes it and writes it to dir. It is the
// only part of this tool that touches the network, and it is never run by CI or a test.
func refresh(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	client := &http.Client{Timeout: 60 * time.Second}
	for _, name := range services {
		url := fmt.Sprintf(referenceEndpoint, name, name)
		pruned, err := download(client, url, name)
		if err != nil {
			return err
		}
		encoded, err := json.MarshalIndent(pruned, "", "  ")
		if err != nil {
			return fmt.Errorf("encode %s: %w", name, err)
		}
		encoded = append(encoded, '\n')
		path := filepath.Join(dir, name+".json")
		if err := os.WriteFile(path, encoded, 0o644); err != nil { //nolint:gosec // vendored data, world-readable is fine.
			return fmt.Errorf("write %s: %w", path, err)
		}
		fmt.Printf("fetched %s (%s, %d actions, %d resource types)\n",
			path, pruned.Version, len(pruned.Actions), len(pruned.Resources))
	}
	return nil
}

// download fetches one service document and returns its pruned form, sorted so that a
// refresh that changes nothing produces no diff.
func download(client *http.Client, url, name string) (service, error) {
	resp, err := client.Get(url) //nolint:noctx // one-shot generator fetch with a client timeout.
	if err != nil {
		return service{}, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return service{}, fmt.Errorf("fetch %s: HTTP %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return service{}, fmt.Errorf("read %s: %w", url, err)
	}
	var raw rawService
	if err := json.Unmarshal(body, &raw); err != nil {
		return service{}, fmt.Errorf("parse %s: %w", url, err)
	}
	if raw.Name != name {
		return service{}, fmt.Errorf("%s names service %q, want %q", url, raw.Name, name)
	}
	return prune(raw), nil
}

// prune reduces a fetched document to the fields the generated table needs, sorting
// every list so the snapshot is byte-stable across refreshes.
func prune(raw rawService) service {
	svc := service{Name: raw.Name, Version: raw.Version}
	for _, a := range raw.Actions {
		types := make([]string, 0, len(a.Resources))
		for _, r := range a.Resources {
			types = append(types, r.Name)
		}
		sort.Strings(types)
		svc.Actions = append(svc.Actions, action{Name: a.Name, Resources: types})
	}
	sort.Slice(svc.Actions, func(i, j int) bool { return svc.Actions[i].Name < svc.Actions[j].Name })
	for _, r := range raw.Resources {
		formats := make([]string, len(r.ARNFormats))
		copy(formats, r.ARNFormats)
		sort.Strings(formats)
		svc.Resources = append(svc.Resources, resource{Name: r.Name, ARNFormats: formats})
	}
	sort.Slice(svc.Resources, func(i, j int) bool { return svc.Resources[i].Name < svc.Resources[j].Name })
	return svc
}

// wrapAt is the column past which a map entry is written one element per line. Keeping
// long entries readable matters for exactly two rows — ec2's CreateTags and DeleteTags
// each name over a hundred resource types — and costs nothing for the rest.
const wrapAt = 96

// render produces the generated Go source for the loaded snapshots.
func render(svcs []service) ([]byte, error) {
	var b bytes.Buffer

	b.WriteString("// Code generated by cmd/gen-authz-reference; DO NOT EDIT.\n//\n")
	b.WriteString("// Source: AWS Service Reference Information,\n")
	b.WriteString("// https://servicereference.us-east-1.amazonaws.com/, vendored under emulator/authzref:\n//\n")
	for _, svc := range svcs {
		fmt.Fprintf(&b, "//   %-4s %-4s %4d actions, %3d resource types\n",
			svc.Name, svc.Version, len(svc.Actions), len(svc.Resources))
	}
	b.WriteString("//\n// Regenerate with `make authz-reference`; refresh the snapshots with\n")
	b.WriteString("// `make authz-reference-fetch` (network).\n\n")
	b.WriteString("package emulator\n\n")

	b.WriteString(`// authzActionResources maps "<service>:<Action>" to the resource types AWS publishes
// for that action, sorted.
//
// A present key with an empty value is AWS saying the action supports no resource-level
// permissions, so a real request's resource is "*" — that is the distinction #762 turns
// on, and it is why callers must use the two-value map form rather than testing the
// length of a single return. An absent key is an action AWS does not publish at all.
// Read it through [authzActionSupportsResourceType] rather than directly.
var authzActionResources = map[string][]string{
`)
	for _, svc := range svcs {
		fmt.Fprintf(&b, "\t// %s (%s)\n", svc.Name, svc.Version)
		for _, a := range svc.Actions {
			writeEntry(&b, svc.Name+":"+a.Name, a.Resources)
		}
	}
	b.WriteString("}\n\n")

	b.WriteString(`// authzResourceARNFormatsByType maps "<service>:<resource-type>" to the ARN formats AWS
// documents for that type, in AWS's own ${Placeholder} notation.
//
// It is the citation for every ARN substrate mints in a resource position: a test asserts
// that what a per-operation resolver builds has the shape recorded here, so the shapes
// cannot drift from AWS's without a failure.
var authzResourceARNFormatsByType = map[string][]string{
`)
	for _, svc := range svcs {
		fmt.Fprintf(&b, "\t// %s (%s)\n", svc.Name, svc.Version)
		for _, r := range svc.Resources {
			writeEntry(&b, svc.Name+":"+r.Name, r.ARNFormats)
		}
	}
	b.WriteString("}\n")

	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated source: %w", err)
	}
	return formatted, nil
}

// writeEntry writes one map entry, on one line when it fits within [wrapAt] columns and
// one element per line when it does not.
func writeEntry(b *bytes.Buffer, key string, values []string) {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = `"` + v + `"`
	}
	oneLine := fmt.Sprintf("\t%q: {%s},\n", key, strings.Join(quoted, ", "))
	if len(oneLine) <= wrapAt {
		b.WriteString(oneLine)
		return
	}
	fmt.Fprintf(b, "\t%q: {\n", key)
	for _, q := range quoted {
		fmt.Fprintf(b, "\t\t%s,\n", q)
	}
	b.WriteString("\t},\n")
}
