package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The emitter runs against the fixture models in testdata/models, so it needs no AWS CLI. The
// fixtures hold one awsQuery service (sns) and one ec2-protocol service, with a shape for every
// serialization fact the generator records, and testdata/golden.go.txt is what they must produce.

var update = flag.Bool("update", false, "rewrite testdata/golden.go.txt from the fixtures")

var fixtureServices = []service{
	{"sns", "sns", "sns", false},
	{"ec2", "ec2", "ec2", true},
}

const fixtureModels = "testdata/models"

func TestGenerate_MatchesTheGolden(t *testing.T) {
	src, err := generate(fixtureModels, fixtureServices)
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
	src, err := generate(fixtureModels, fixtureServices)
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
	out := filepath.Join(t.TempDir(), "gen.go")
	if err := run(fixtureModels, out, true, fixtureServices); err == nil {
		t.Fatal("-check of a missing file must fail")
	}
	if err := run(fixtureModels, out, false, fixtureServices); err != nil {
		t.Fatal(err)
	}
	if err := run(fixtureModels, out, true, fixtureServices); err != nil {
		t.Fatalf("-check right after writing: %v", err)
	}
	if err := os.WriteFile(out, []byte("package emulator\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(fixtureModels, out, true, fixtureServices); !errors.Is(err, errStale) {
		t.Fatalf("-check of a stale file: got %v, want errStale", err)
	}
	if err := run(fixtureModels, filepath.Join(t.TempDir(), "no", "such", "dir", "gen.go"), false, fixtureServices); err == nil {
		t.Fatal("an unwritable output must fail")
	}
}

func TestGenerate_Refusals(t *testing.T) {
	if _, err := generate(fixtureModels, []service{{"sts", "sts", "sts", false}}); err == nil {
		t.Error("a service with no model must fail")
	}
	dir := t.TempDir()
	bad := filepath.Join(dir, "sns", "2010-03-31")
	if err := os.MkdirAll(bad, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "service-2.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := generate(dir, fixtureServices[:1]); err == nil {
		t.Error("a model that will not decode must fail")
	}
}

// findModels resolves `aws` on PATH through its symlink to the Homebrew/pip layout.
func TestFindModels(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinked executables")
	}
	root := t.TempDir()
	cellar := filepath.Join(root, "Cellar", "awscli", "2.0.0")
	data := filepath.Join(cellar, "libexec", "lib", "python3.99", "site-packages", "awscli", "botocore", "data")
	if err := os.MkdirAll(data, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cellar, "bin"), 0o750); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(cellar, "bin", "aws")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o700); err != nil { //nolint:gosec // a fake executable
		t.Fatal(err)
	}
	pathDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(pathDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bin, filepath.Join(pathDir, "aws")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)
	got, err := findModels()
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := filepath.EvalSymlinks(data); got != want {
		gotReal, _ := filepath.EvalSymlinks(got)
		if gotReal != want {
			t.Fatalf("findModels = %s, want %s", got, want)
		}
	}

	t.Setenv("PATH", t.TempDir())
	if _, err := findModels(); err == nil {
		t.Error("no aws on PATH must fail")
	}
	if err := run("", filepath.Join(t.TempDir(), "gen.go"), false, fixtureServices); err == nil {
		t.Error("run with no -models and no aws on PATH must fail")
	}

	// An aws whose install has no botocore data directory.
	empty := filepath.Join(t.TempDir(), "x", "bin")
	if err := os.MkdirAll(empty, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(empty, "aws"), []byte("#!/bin/sh\n"), 0o700); err != nil { //nolint:gosec // a fake executable
		t.Fatal(err)
	}
	t.Setenv("PATH", empty)
	if _, err := findModels(); err == nil {
		t.Error("an install with no data directory must fail")
	}
}
