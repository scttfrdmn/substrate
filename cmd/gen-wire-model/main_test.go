package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestPluginModels_CoverEveryRoutedPlugin pins the mapping: every plugin the operation catalog
// routes has an entry naming its models or saying why it has none, and no entry names a plugin
// that does not exist.
func TestPluginModels_CoverEveryRoutedPlugin(t *testing.T) {
	mapped := map[string]bool{}
	for _, pm := range pluginModels {
		if mapped[pm.plugin] {
			t.Errorf("%s is listed twice", pm.plugin)
		}
		mapped[pm.plugin] = true
		if (len(pm.models) == 0) == (pm.unmodelled == "") {
			t.Errorf("%s must name its models or say why it has none, not both or neither", pm.plugin)
		}
	}
	routed := emulator.RoutedServices()
	for _, p := range routed {
		if !mapped[p] {
			t.Errorf("routed plugin %s has no entry in pluginModels", p)
		}
		delete(mapped, p)
	}
	for p := range mapped {
		t.Errorf("pluginModels lists %s, which is not a routed plugin", p)
	}
	if !sort.SliceIsSorted(pluginModels, func(i, j int) bool { return pluginModels[i].plugin < pluginModels[j].plugin }) {
		t.Error("pluginModels is not in registry-name order")
	}
}

// TestRun_TheVendoredSnapshotIsCurrent is make wire-model-check, run by the test suite too.
func TestRun_TheVendoredSnapshotIsCurrent(t *testing.T) {
	root := filepath.Join("..", "..")
	var stderr bytes.Buffer
	code := run([]string{"-check", "-data", filepath.Join(root, "emulator", "wiremodel"),
		"-out", filepath.Join(root, "emulator", "wire_model_gen.go")}, &bytes.Buffer{}, &stderr)
	if code != 0 {
		t.Fatalf("wire_model_gen.go is out of date with the snapshot: %s", stderr.String())
	}
	raw, err := os.ReadFile(filepath.Join(root, "emulator", "wiremodel", manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var man manifest
	if err := json.Unmarshal(raw, &man); err != nil {
		t.Fatal(err)
	}
	if man.Botocore != botocoreTag {
		t.Errorf("the snapshot is cut from botocore %s, and -fetch reads %s; run `make wire-model-fetch`", man.Botocore, botocoreTag)
	}
}

// fakeService is a model with one routed operation whose shapes carry documentation, a member
// order that is not alphabetical, a recursive shape, and a shape no routed operation reaches.
const fakeService = `{
  "version": "2.0",
  "metadata": {"apiVersion": "2015-12-08", "protocol": "json", "documentation": "<p>x</p>"},
  "documentation": "<p>service</p>",
  "operations": {
    "RequestCertificate": {"name": "RequestCertificate", "http": {"method": "POST", "requestUri": "/"},
      "input": {"shape": "Req"}, "output": {"shape": "Out"}, "errors": [{"shape": "Err"}],
      "documentation": "<p>op</p>", "documentationUrl": "https://example.com"},
    "NotRouted": {"name": "NotRouted", "input": {"shape": "Orphan"}}
  },
  "shapes": {
    "Req": {"type": "structure", "required": ["Zeta"], "members": {
      "Zeta": {"shape": "Name", "documentation": "<p>z</p>"},
      "Alpha": {"shape": "Loop"},
      "Plain": {"shape": "Free"}
    }, "documentation": "<p>req</p>"},
    "Name": {"type": "string", "min": 1, "max": 5, "pattern": "[a-z]+", "enum": ["a", "b"]},
    "Free": {"type": "string", "min": 0},
    "Loop": {"type": "structure", "members": {"Next": {"shape": "Loop"}, "Count": {"shape": "Num"}}},
    "Num": {"type": "integer", "min": 0, "max": 10},
    "Out": {"type": "structure", "members": {}},
    "Err": {"type": "structure", "members": {"message": {"shape": "Free"}}, "exception": true},
    "Orphan": {"type": "structure", "members": {}}
  }
}`

// fakeTarball builds a botocore release tarball holding fakeService as acm's newest version, an
// older acm version that must be ignored, and an empty model for every other mapped service.
func fakeTarball(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	add := func(name, body string) {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range modelNames() {
		if m == "acm" {
			continue
		}
		add("botocore-9.9.9/botocore/data/"+m+"/2000-01-01/service-2.json",
			`{"metadata": {"protocol": "json"}, "operations": {}, "shapes": {}}`)
	}
	add("botocore-9.9.9/botocore/data/acm/2010-01-01/service-2.json", `{"operations": {}, "shapes": {}}`)
	add("botocore-9.9.9/botocore/data/acm/2015-12-08/service-2.json", fakeService)
	add("botocore-9.9.9/botocore/data/acm/2015-12-08/paginators-1.json",
		`{"pagination": {"RequestCertificate": {"input_token": "NextToken"}, "NotRouted": {}}}`)
	add("botocore-9.9.9/botocore/data/acm/2015-12-08/waiters-2.json", `{}`)
	add("botocore-9.9.9/README.rst", "readme")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestFetch_PrunesStripsAndRenders runs -fetch against a local server and then generation, and
// checks what each keeps.
func TestFetch_PrunesStripsAndRenders(t *testing.T) {
	tarball := fakeTarball(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-fetch", "-data", dir, "-tag", "9.9.9", "-url", srv.URL}, &stdout, &stderr); code != 0 {
		t.Fatalf("fetch: %s", stderr.String())
	}
	m, err := loadModel(dir, "acm")
	if err != nil {
		t.Fatal(err)
	}
	if m.apiVersion != "2015-12-08" {
		t.Errorf("apiVersion %q: the newest version directory is the one botocore loads", m.apiVersion)
	}
	var enc bytes.Buffer
	m.service.encode(&enc)
	got := enc.String()
	for _, absent := range []string{"documentation", "NotRouted", "Orphan", "<p>"} {
		if strings.Contains(got, absent) {
			t.Errorf("the pruned model holds %q: %s", absent, got)
		}
	}
	for _, kept := range []string{`"Req"`, `"Name"`, `"Loop"`, `"Out"`, `"Err"`, `"exception":true`} {
		if !strings.Contains(got, kept) {
			t.Errorf("the pruned model lost %s: %s", kept, got)
		}
	}
	if strings.Index(got, `"Zeta"`) > strings.Index(got, `"Alpha"`) {
		t.Error("the members' declaration order was not kept")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "acm.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paginators struct {
			Pagination map[string]json.RawMessage `json:"pagination"`
		} `json:"paginators"`
	}
	if err := json.NewDecoder(zr).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Paginators.Pagination["NotRouted"]; ok || len(doc.Paginators.Pagination) != 1 {
		t.Errorf("paginators not pruned to the routed operations: %v", doc.Paginators.Pagination)
	}

	// A second fetch of the same tarball changes nothing.
	if code := run([]string{"-fetch", "-data", dir, "-tag", "9.9.9", "-url", srv.URL}, &stdout, &stderr); code != 0 {
		t.Fatalf("second fetch: %s", stderr.String())
	}
	again, err := os.ReadFile(filepath.Join(dir, "acm.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) {
		t.Error("a refresh that changes nothing upstream rewrote the snapshot")
	}

	out := filepath.Join(t.TempDir(), "gen.go")
	if code := run([]string{"-data", dir, "-out", out, "-check"}, &stdout, &stderr); code == 0 {
		t.Fatal("-check of a missing file must fail")
	}
	if code := run([]string{"-data", dir, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("generate: %s", stderr.String())
	}
	src, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	gen := strings.Join(strings.Fields(string(src)), " ")
	for _, want := range []string{
		`const wireModelVersion = "botocore/9.9.9"`,
		`{Name: "acm", APIVersion: "2015-12-08", Protocol: "json", Protocols: []string{"json"}}`,
		`"RequestCertificate": {Model: 0, Input: 1}`,
		`{Name: "Zeta", Shape: 2, Required: true}`,
		`{Kind: wireString, Min: "1", Max: "5", Enum: 1, Pattern: 1}`,
		`{Name: "Next", Shape: 3}`, // the recursive shape refers to itself
		`{Kind: wireInteger, Min: "0", Max: "10"}`,
		`{"a", "b"}`,
		`"[a-z]+"`,
	} {
		if !strings.Contains(gen, want) {
			t.Errorf("generated source lacks %s:\n%s", want, src)
		}
	}
	if strings.Contains(gen, `"Plain"`) {
		t.Error("a member with no constraint was kept")
	}
	if code := run([]string{"-data", dir, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("regenerate: %s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "already up to date") {
		t.Errorf("an unchanged regeneration says %q", stdout.String())
	}
	if code := run([]string{"-data", dir, "-out", out, "-check"}, &stdout, &stderr); code != 0 {
		t.Fatalf("-check right after writing: %s", stderr.String())
	}
	if err := os.WriteFile(out, []byte("package emulator\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-data", dir, "-out", out, "-check"}, &stdout, &stderr); code == 0 {
		t.Fatal("-check of a stale file must fail")
	}

	// A refresh removes a model the previous manifest vendored and the new one does not list,
	// and leaves alone a file the snapshot never vendored.
	stale := filepath.Join(dir, "gone.json.gz")
	foreign := filepath.Join(dir, "foreign.json.gz")
	for _, p := range []string{stale, foreign} {
		if err := os.WriteFile(p, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	prev, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	prev.Models["gone"] = modelEntry{APIVersion: "1"}
	encoded, err := json.Marshal(prev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-fetch", "-data", dir, "-tag", "9.9.9", "-url", srv.URL}, &stdout, &stderr); code != 0 {
		t.Fatalf("third fetch: %s", stderr.String())
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("a model the manifest no longer lists was left behind: %v", err)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Errorf("a file the snapshot never vendored was removed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-fetch", "-data", dir, "-tag", "9.9.9", "-url", srv.URL}, &stdout, &stderr); code != 1 {
		t.Error("a refresh over a manifest that does not parse must fail")
	}

	// A local tarball path refreshes the same way.
	local := filepath.Join(t.TempDir(), "botocore.tar.gz")
	if err := os.WriteFile(local, tarball, 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-fetch", "-data", t.TempDir(), "-tag", "9.9.9", "-url", local}, &stdout, &stderr); code != 0 {
		t.Fatalf("fetch from a file: %s", stderr.String())
	}
}

func TestRun_Refusals(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-no-such-flag"}, &stdout, &stderr); code != 2 {
		t.Errorf("an unknown flag exits %d, want 2", code)
	}
	if code := run([]string{"-data", t.TempDir()}, &stdout, &stderr); code != 1 {
		t.Error("an empty snapshot directory must fail")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer srv.Close()
	if code := run([]string{"-fetch", "-data", t.TempDir(), "-url", srv.URL}, &stdout, &stderr); code != 1 {
		t.Error("a failed download must fail")
	}
	notGzip := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a tarball"))
	}))
	defer notGzip.Close()
	if code := run([]string{"-fetch", "-data", t.TempDir(), "-url", notGzip.URL}, &stdout, &stderr); code != 1 {
		t.Error("a body that is not a tarball must fail")
	}
	if code := run([]string{"-fetch", "-data", t.TempDir(), "-url", filepath.Join(t.TempDir(), "missing.tar.gz")}, &stdout, &stderr); code != 1 {
		t.Error("a missing local tarball must fail")
	}

	// A tarball missing a mapped model.
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	_ = tw.Close()
	_ = zw.Close()
	if _, err := readTarball(bytes.NewReader(buf.Bytes()), []string{"acm"}); err == nil {
		t.Error("a tarball without a wanted model must fail")
	}

	// A manifest naming a model whose file is absent.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte(`{"botocore":"1","models":{"acm":{}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-data", dir}, &stdout, &stderr); code != 1 {
		t.Error("a manifest naming a missing model must fail")
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte(`{`), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-data", dir}, &stdout, &stderr); code != 1 {
		t.Error("a manifest that does not parse must fail")
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte(`{"botocore":"1","models":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-data", dir}, &stdout, &stderr); code != 1 {
		t.Error("a manifest naming no models must fail")
	}
}

// TestValue_RoundTripsInOrder pins the ordered JSON codec the snapshot relies on.
func TestValue_RoundTripsInOrder(t *testing.T) {
	in := `{"z":1,"a":[true,null,"s",{"y":2,"b":3}],"m":{},"q":"a\"b\\c\u0001<\u0000"}`
	v, err := decodeValue([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	v.encode(&out)
	if out.String() != in {
		t.Errorf("round trip: %s", out.String())
	}
	if got := v.keys(); strings.Join(got, ",") != "z,a,m,q" {
		t.Errorf("keys: %v", got)
	}
	if v.get("z").number() != "1" || v.get("a").elems()[0].boolean() != true || v.get("a").elems()[2].str() != "s" {
		t.Error("scalar accessors")
	}
	if v.get("missing").members() != nil || v.get("missing").elems() != nil || v.get("missing").str() != "" {
		t.Error("a missing member must read as empty")
	}
	for _, bad := range []string{`{`, `{"a":1} x`, `[1,`, `{1:2}`} {
		if _, err := decodeValue([]byte(bad)); err == nil {
			t.Errorf("%q decoded", bad)
		}
	}
}
