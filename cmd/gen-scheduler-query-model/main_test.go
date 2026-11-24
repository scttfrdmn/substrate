package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The emitter runs against the fixture models in testdata/models, which fixtureSnapshot packs into
// the vendored snapshot's layout. The fixtures hold one awsQuery service (sns) and one ec2-protocol
// service, with a shape for every serialization fact the generator records, and
// testdata/golden.go.txt is what they must produce.

var update = flag.Bool("update", false, "rewrite testdata/golden.go.txt from the fixtures")

var fixtureServices = []service{
	{"sns", "sns", "sns", false},
	{"ec2", "ec2", "ec2", true},
}

// fixtureSnapshot writes each fixture model as emulator/wiremodel holds one, <name>.json.gz
// wrapping service-2.json, and returns the directory.
func fixtureSnapshot(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"sns", "ec2"} {
		matches, err := filepath.Glob(filepath.Join("testdata", "models", name, "*", "service-2.json"))
		if err != nil || len(matches) != 1 {
			t.Fatalf("fixture %s: %v %v", name, matches, err)
		}
		raw, err := os.ReadFile(matches[0])
		if err != nil {
			t.Fatal(err)
		}
		doc, err := json.Marshal(map[string]json.RawMessage{
			"apiVersion": json.RawMessage(`"` + filepath.Base(filepath.Dir(matches[0])) + `"`),
			"service":    raw,
		})
		if err != nil {
			t.Fatal(err)
		}
		writeGzip(t, filepath.Join(dir, name+".json.gz"), doc)
	}
	return dir
}

func writeGzip(t *testing.T, path string, data []byte) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestGenerate_Refusals(t *testing.T) {
	if _, err := generate(fixtureSnapshot(t), []service{{"sts", "sts", "sts", false}}); err == nil {
		t.Error("a service with no model must fail")
	}
	for name, data := range map[string][]byte{
		"not gzip": []byte("{not gzip"),
		"not json": nil, // gzipped below
	} {
		dir := t.TempDir()
		path := filepath.Join(dir, "sns.json.gz")
		if data == nil {
			writeGzip(t, path, []byte("{not json"))
		} else if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := generate(dir, fixtureServices[:1]); err == nil {
			t.Errorf("a model that is %s must fail", name)
		}
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sns.json.gz"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	trunc := filepath.Join(dir, "sns.json.gz")
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write([]byte(`{"service": {}}`))
	_ = zw.Close()
	if err := os.WriteFile(trunc, buf.Bytes()[:buf.Len()-6], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := generate(dir, fixtureServices[:1]); err == nil {
		t.Error("a truncated model must fail")
	}
}

func TestGenerate_MatchesTheGolden(t *testing.T) {
	src, err := generate(fixtureSnapshot(t), fixtureServices)
	if err != nil {
		t.Fatal(err)
	}
	golden := filepath.Join("testdata", "golden.go.txt")
	if *update {
		if err := os.WriteFile(golden, src, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(src) != string(want) {
		t.Fatalf("generated output differs from %s (rerun with -update after checking it):\n%s", golden, src)
	}
}

// The facts the golden encodes, stated one by one so a reviewer need not read the golden to know
// what each fixture shape must produce.
func TestGenerate_RecordsEachSerializationFact(t *testing.T) {
	src, err := generate(fixtureSnapshot(t), fixtureServices)
	if err != nil {
		t.Fatal(err)
	}
	out := strings.Join(strings.Fields(string(src)), " ") // alignment-insensitive
	for _, want := range []string{
		// awsQuery
		`"Attributes":`, `{Map: true, Key: "key", Value: "value"}`, // a map, default key/value names
		`"MessageAttributes": {Map: true, Key: "Name", Value: "Value"}`,  // model-named key and value
		`"MessageAttributes{}.Values": {List: "item"}`,                   // a list inside a map's value
		`"FlatMap": {Flat: true, Map: true, Key: "key", Value: "value"}`, // a flattened map
		`"Renamed":`, `{Name: "WireName"}`, // a renamed member
		`"Flat":`, `{Flat: true}`, // a flattened list
		`"FlatNamed":`, `{List: "item", Flat: true}`, // a flattened list with a named member
		`"Named":`, `{List: "item"}`, // a list with a named member
		`"Loop.Again":`, `{Name: "Deeper"}`, // a recursive shape, bounded
		`"Nested[]":`, // a list of lists carries the inner list's facts
		// ec2
		`"InstanceIds": {Name: "InstanceId"}`, // capitalized locationName
		`"Filters[].Values": {Name: "Value"}`, // inside a list element
		`"Resources": {Name: "ResourceId"}`,   // queryName wins
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %s", want)
		}
	}
	for _, absent := range []string{
		`"SetTopicAttributes"`,   // an operation with no departure from the generic rule
		`"ListTopics"`,           // an operation with no input
		`"NotRoutedBySubstrate"`, // an operation substrate does not route
		`"Tags[]"`,               // a default list of default structures
		`"MaxResults"`,           // an ec2 member with no rename
		`"DryRun"`,               // an ec2 lowerCamel locationName that capitalizes to the member name
	} {
		if strings.Contains(out, absent) {
			t.Errorf("output holds %s, which needs no hint", absent)
		}
	}
	if !strings.Contains(out, "// - sns 2010-03-31 (query)") || !strings.Contains(out, "// - ec2 2016-11-15 (ec2)") {
		t.Error("the header does not record the models' versions")
	}
}

func TestRun_WritesAndChecks(t *testing.T) {
	snap := fixtureSnapshot(t)
	out := filepath.Join(t.TempDir(), "gen.go")
	if err := run(snap, out, true, fixtureServices); err == nil {
		t.Fatal("-check of a missing file must fail")
	}
	if err := run(snap, out, false, fixtureServices); err != nil {
		t.Fatal(err)
	}
	if err := run(snap, out, true, fixtureServices); err != nil {
		t.Fatalf("-check right after writing: %v", err)
	}
	if err := os.WriteFile(out, []byte("package emulator\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(snap, out, true, fixtureServices); !errors.Is(err, errStale) {
		t.Fatalf("-check of a stale file: got %v, want errStale", err)
	}
	if err := run(snap, filepath.Join(t.TempDir(), "no", "such", "dir", "gen.go"), false, fixtureServices); err == nil {
		t.Fatal("an unwritable output must fail")
	}
}
