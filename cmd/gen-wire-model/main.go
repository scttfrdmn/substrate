// Command gen-wire-model vendors the botocore service models behind substrate's plugins and
// turns them into the constraint tables substrate's request validator reads (#1612).
//
// # Why botocore, and why vendored
//
// Each plugin used to check its own input by hand, and those checks were thin and
// inconsistent: about half of the routed operations refused a missing required member, few
// checked a length or a range, and "missing member" was answered with at least eight different
// codes. AWS publishes each service's constraints in its service model, and botocore
// (https://github.com/boto/botocore, Apache-2.0) ships them as JSON: for every operation the
// input shape, and for every shape its required members, length and range bounds, enum values
// and pattern.
//
// -fetch downloads one pinned botocore release tag as a tarball, picks each mapped service's
// newest API version (the directory botocore itself loads), prunes it to the shapes reachable
// from the operations substrate routes (inputs, outputs and errors), strips the documentation,
// and writes the result gzipped under emulator/wiremodel, with a manifest naming the tag.
// Generation, the drift check and the test suite are offline: no CI job and no test may depend
// on network access, so refreshing the snapshot is a deliberate, human-run step, and the diff
// to the generated Go file is the review.
//
// # What is generated
//
// emulator/wire_model_gen.go holds tables, not code: one slice of shapes, each with its kind,
// its bounds, its enum and pattern as indexes into shared slices, and its members; and a map
// from plugin and operation to the operation's input shape and, for a REST protocol, its HTTP
// method and URI template. A member or shape is kept only when it carries a constraint or leads
// to one, because the interpreter in emulator/wire_validate.go walks the tables and has nothing
// to check anywhere else. Per-field code would be thousands of functions for EC2 alone.
//
// # Usage
//
//	go run ./cmd/gen-wire-model           # regenerate emulator/wire_model_gen.go
//	go run ./cmd/gen-wire-model -check    # exit non-zero if it is out of date (offline)
//	go run ./cmd/gen-wire-model -fetch    # refresh emulator/wiremodel from GitHub (network)
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// botocoreTag is the botocore release the vendored snapshot is cut from. -fetch reads it, and
// the manifest records it, so a refresh that changes nothing upstream changes nothing here.
const botocoreTag = "1.44.10"

// tarballURL is GitHub's archive of a botocore tag.
const tarballURL = "https://codeload.github.com/boto/botocore/tar.gz/refs/tags/%s"

// manifestName is the snapshot's index under the data directory.
const manifestName = "manifest.json"

// manifest records what the snapshot was cut from and what it covers.
type manifest struct {
	// Botocore is the release tag.
	Botocore string `json:"botocore"`
	// Models maps each vendored model to its API version directory and the number of
	// operations it was pruned to.
	Models map[string]modelEntry `json:"models"`
	// Unmodelled maps a plugin to the routed operations none of its models defines.
	Unmodelled map[string][]string `json:"unmodelled,omitempty"`
}

// modelEntry is one vendored model in the manifest.
type modelEntry struct {
	APIVersion string `json:"apiVersion"`
	Operations int    `json:"operations"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the command, returning its exit status, so the write and check paths can be tested
// without a subprocess.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("gen-wire-model", flag.ContinueOnError)
	flags.SetOutput(stderr)
	check := flags.Bool("check", false, "exit non-zero if the generated file is out of date instead of writing it")
	fetch := flags.Bool("fetch", false, "refresh the vendored snapshot from GitHub (requires network access)")
	dir := flags.String("data", filepath.Join("emulator", "wiremodel"), "directory holding the vendored snapshot")
	out := flags.String("out", filepath.Join("emulator", "wire_model_gen.go"), "path to the generated Go file")
	tag := flags.String("tag", botocoreTag, "the botocore release tag -fetch reads")
	url := flags.String("url", "", "the tarball -fetch reads (default: GitHub's archive of -tag)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	fail := func(err error) int {
		_, _ = fmt.Fprintln(stderr, "gen-wire-model:", err)
		return 1
	}

	if *fetch {
		src := *url
		if src == "" {
			src = fmt.Sprintf(tarballURL, *tag)
		}
		if err := refresh(*dir, *tag, src, stdout); err != nil {
			return fail(err)
		}
		return 0
	}

	snap, err := loadSnapshot(*dir)
	if err != nil {
		return fail(err)
	}
	generated, stats, err := render(snap)
	if err != nil {
		return fail(err)
	}
	existing, err := os.ReadFile(*out)
	if err != nil && !os.IsNotExist(err) {
		return fail(fmt.Errorf("read %s: %w", *out, err))
	}
	if *check {
		if !bytes.Equal(existing, generated) {
			_, _ = fmt.Fprintf(stderr, "gen-wire-model: %s is out of date with %s; run `make wire-model` and commit the result\n", *out, *dir)
			return 1
		}
		return 0
	}
	if bytes.Equal(existing, generated) {
		_, _ = fmt.Fprintf(stdout, "%s already up to date (%s)\n", *out, stats)
		return 0
	}
	if err := os.WriteFile(*out, generated, 0o644); err != nil { //nolint:gosec // generated source, world-readable is fine.
		return fail(err)
	}
	_, _ = fmt.Fprintf(stdout, "updated %s (%s)\n", *out, stats)
	return 0
}

// refresh downloads the botocore tarball at src, prunes each mapped model and writes the
// snapshot. It is the only part of this tool that touches the network, and it is never run by
// CI or a test against the network: the tests hand it a local server.
func refresh(dir, tag, src string, stdout io.Writer) error {
	if !strings.Contains(src, "://") {
		// A local tarball, for a refresh from a download already on disk.
		f, err := os.Open(src) //nolint:gosec // the operator names the file.
		if err != nil {
			return fmt.Errorf("open %s: %w", src, err)
		}
		defer func() { _ = f.Close() }()
		files, err := readTarball(f, modelNames())
		if err != nil {
			return fmt.Errorf("read %s: %w", src, err)
		}
		return writeSnapshot(dir, tag, files, stdout)
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Get(src) //nolint:noctx // one-shot generator fetch with a client timeout.
	if err != nil {
		return fmt.Errorf("fetch %s: %w", src, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: HTTP %d", src, resp.StatusCode)
	}
	files, err := readTarball(resp.Body, modelNames())
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	return writeSnapshot(dir, tag, files, stdout)
}

// upstreamModel is one service's newest API version as the tarball holds it.
type upstreamModel struct {
	version    string
	service    []byte
	paginators []byte
}

// readTarball collects service-2.json and paginators-1.json for each wanted model, keeping the
// newest API version directory of each, which is the one botocore loads.
func readTarball(r io.Reader, wanted []string) (map[string]*upstreamModel, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}
	want := map[string]bool{}
	for _, w := range wanted {
		want[w] = true
	}
	found := map[string]*upstreamModel{}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("untar: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		// <root>/botocore/data/<service>/<version>/<file>
		parts := strings.Split(path.Clean(hdr.Name), "/")
		if len(parts) != 6 || parts[1] != "botocore" || parts[2] != "data" || !want[parts[3]] {
			continue
		}
		svc, version, file := parts[3], parts[4], parts[5]
		if file != "service-2.json" && file != "paginators-1.json" {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", hdr.Name, err)
		}
		m := found[svc]
		if m == nil || version > m.version {
			m = &upstreamModel{version: version}
			found[svc] = m
		} else if version < m.version {
			continue
		}
		if file == "service-2.json" {
			m.service = data
		} else {
			m.paginators = data
		}
	}
	for _, w := range wanted {
		if found[w] == nil || found[w].service == nil {
			return nil, fmt.Errorf("the tarball has no service-2.json for %s", w)
		}
	}
	return found, nil
}

// writeSnapshot prunes each model to its plugins' routed operations and writes it with the
// manifest.
func writeSnapshot(dir, tag string, files map[string]*upstreamModel, stdout io.Writer) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	routed := routedByModel()
	man := manifest{Botocore: tag, Models: map[string]modelEntry{}, Unmodelled: map[string][]string{}}
	defined := map[string]map[string]bool{} // model -> operations it defines upstream
	total := 0
	for _, name := range modelNames() {
		up := files[name]
		svc, err := decodeValue(up.service)
		if err != nil {
			return fmt.Errorf("parse %s service-2.json: %w", name, err)
		}
		ops := map[string]bool{}
		for _, op := range svc.get("operations").keys() {
			ops[op] = true
		}
		defined[name] = ops
		keep := map[string]bool{}
		for _, op := range routed[name] {
			if ops[op] {
				keep[op] = true
			}
		}
		if len(keep) == 0 {
			continue
		}
		pruned := prune(svc, keep)
		var pag *value
		if up.paginators != nil {
			raw, err := decodeValue(up.paginators)
			if err != nil {
				return fmt.Errorf("parse %s paginators-1.json: %w", name, err)
			}
			pag = prunePaginators(raw, keep)
		}
		doc := &value{isObj: true}
		doc.set("apiVersion", jsonString(up.version))
		doc.set("service", pruned)
		if pag != nil {
			doc.set("paginators", pag)
		}
		var raw bytes.Buffer
		doc.encode(&raw)
		gz, err := gzipBytes(raw.Bytes())
		if err != nil {
			return err
		}
		p := filepath.Join(dir, name+".json.gz")
		if err := os.WriteFile(p, gz, 0o644); err != nil { //nolint:gosec // vendored data, world-readable is fine.
			return fmt.Errorf("write %s: %w", p, err)
		}
		total += len(gz)
		man.Models[name] = modelEntry{APIVersion: up.version, Operations: len(keep)}
	}
	for _, pm := range pluginModels {
		for _, op := range emulator.RoutedOperations(pm.plugin) {
			inAny := false
			for _, m := range pm.models {
				if defined[m][op] {
					inAny = true
				}
			}
			if !inAny {
				man.Unmodelled[pm.plugin] = append(man.Unmodelled[pm.plugin], op)
			}
		}
	}
	if err := removeStale(dir, man); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	p := filepath.Join(dir, manifestName)
	if err := os.WriteFile(p, append(encoded, '\n'), 0o644); err != nil { //nolint:gosec // vendored data.
		return fmt.Errorf("write %s: %w", p, err)
	}
	_, _ = fmt.Fprintf(stdout, "fetched botocore %s into %s: %d models, %d bytes gzipped\n", tag, dir, len(man.Models), total)
	return nil
}

// removeStale deletes each vendored model the new manifest no longer lists, so a model a
// mapping change or a re-prune drops does not linger beside the snapshot that replaced it.
func removeStale(dir string, man manifest) error {
	existing, err := filepath.Glob(filepath.Join(dir, "*.json.gz"))
	if err != nil {
		return fmt.Errorf("list %s: %w", dir, err)
	}
	for _, p := range existing {
		name := strings.TrimSuffix(filepath.Base(p), ".json.gz")
		if _, ok := man.Models[name]; ok {
			continue
		}
		if err := os.Remove(p); err != nil {
			return fmt.Errorf("remove stale %s: %w", p, err)
		}
	}
	return nil
}

// routedByModel maps each model to the routed operations of every plugin that names it.
func routedByModel() map[string][]string {
	out := map[string][]string{}
	for _, pm := range pluginModels {
		for _, m := range pm.models {
			out[m] = append(out[m], emulator.RoutedOperations(pm.plugin)...)
		}
	}
	return out
}

// docKeys are the keys stripped from operations, shapes and member references: prose, which
// substrate never renders and which would carry botocore's HTML into the repository.
var docKeys = []string{"documentation", "documentationUrl", "deprecatedMessage"}

// prune keeps the operations in keep and the shapes their inputs, outputs and errors reach.
func prune(svc *value, keep map[string]bool) *value {
	shapes := svc.get("shapes")
	reach := map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		if name == "" || reach[name] {
			return
		}
		reach[name] = true
		sh := shapes.get(name)
		for _, m := range sh.get("members").members() {
			visit(m.val.get("shape").str())
		}
		for _, k := range []string{"member", "key", "value"} {
			visit(sh.get(k).get("shape").str())
		}
	}
	ops := &value{isObj: true}
	for _, m := range svc.get("operations").members() {
		if !keep[m.key] {
			continue
		}
		visit(m.val.get("input").get("shape").str())
		visit(m.val.get("output").get("shape").str())
		for _, e := range m.val.get("errors").elems() {
			visit(e.get("shape").str())
		}
		ops.obj = append(ops.obj, member{key: m.key, val: m.val.without(docKeys...)})
	}
	kept := &value{isObj: true}
	for _, m := range shapes.obj {
		if !reach[m.key] {
			continue
		}
		kept.obj = append(kept.obj, member{key: m.key, val: stripShape(m.val)})
	}
	out := &value{isObj: true}
	for _, m := range svc.obj {
		switch m.key {
		case "documentation":
		case "metadata":
			out.obj = append(out.obj, member{key: m.key, val: m.val.without(docKeys...)})
		case "operations":
			out.obj = append(out.obj, member{key: m.key, val: ops})
		case "shapes":
			out.obj = append(out.obj, member{key: m.key, val: kept})
		default:
			out.obj = append(out.obj, m)
		}
	}
	return out
}

// stripShape removes the documentation from a shape and from each reference it holds.
func stripShape(sh *value) *value {
	out := sh.without(docKeys...)
	if members := out.get("members"); members != nil {
		stripped := &value{isObj: true}
		for _, m := range members.obj {
			stripped.obj = append(stripped.obj, member{key: m.key, val: m.val.without(docKeys...)})
		}
		out.set("members", stripped)
	}
	for _, k := range []string{"member", "key", "value"} {
		if ref := out.get(k); ref != nil {
			out.set(k, ref.without(docKeys...))
		}
	}
	return out
}

// prunePaginators keeps the paginator configurations of the operations in keep.
func prunePaginators(pag *value, keep map[string]bool) *value {
	src := pag.get("pagination")
	if src == nil {
		return nil
	}
	kept := &value{isObj: true}
	for _, m := range src.obj {
		if keep[m.key] {
			kept.obj = append(kept.obj, m)
		}
	}
	if len(kept.obj) == 0 {
		return nil
	}
	out := &value{isObj: true}
	out.set("pagination", kept)
	return out
}

func jsonString(s string) *value {
	var raw bytes.Buffer
	appendJSONString(&raw, s)
	return &value{raw: raw.Bytes()}
}

// gzipBytes compresses data with no name and a zero ModTime in the header, so a refresh that
// changes nothing produces a byte-identical file.
func gzipBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if _, err := zw.Write(data); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	return buf.Bytes(), nil
}

// snapshot is the vendored data, loaded.
type snapshot struct {
	manifest manifest
	models   map[string]*model
}

// model is one vendored service model.
type model struct {
	name       string
	apiVersion string
	service    *value
}

// loadSnapshot reads the manifest and every model it lists.
func loadSnapshot(dir string) (*snapshot, error) {
	raw, err := os.ReadFile(filepath.Join(dir, manifestName)) //nolint:gosec // fixed snapshot location.
	if err != nil {
		return nil, fmt.Errorf("read %s: %w (run `make wire-model-fetch` to create it)", filepath.Join(dir, manifestName), err)
	}
	snap := &snapshot{models: map[string]*model{}}
	if err := json.Unmarshal(raw, &snap.manifest); err != nil {
		return nil, fmt.Errorf("parse %s: %w", manifestName, err)
	}
	if snap.manifest.Botocore == "" || len(snap.manifest.Models) == 0 {
		return nil, fmt.Errorf("%s names no botocore tag or no models", manifestName)
	}
	for name := range snap.manifest.Models {
		m, err := loadModel(dir, name)
		if err != nil {
			return nil, err
		}
		snap.models[name] = m
	}
	return snap, nil
}

// loadModel reads one gzipped model from the snapshot.
func loadModel(dir, name string) (*model, error) {
	p := filepath.Join(dir, name+".json.gz")
	f, err := os.Open(p) //nolint:gosec // path is under the fixed snapshot directory.
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", p, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gunzip %s: %w", p, err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", p, err)
	}
	doc, err := decodeValue(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", p, err)
	}
	return &model{name: name, apiVersion: doc.get("apiVersion").str(), service: doc.get("service")}, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
