package main

import (
	"strings"
	"testing"

	emu "github.com/scttfrdmn/substrate/emulator"
)

// TestRegisteredPlugins_AgreesWithTheRoutingTable is the drift check the -check
// flag runs in CI, exercised directly: the live registry and
// emulator/routing.go must name the same plugins.
func TestRegisteredPlugins_AgreesWithTheRoutingTable(t *testing.T) {
	names, err := registeredPlugins()
	if err != nil {
		t.Fatalf("registeredPlugins: %v", err)
	}
	if len(names) != len(routing) {
		t.Errorf("registry has %d plugins, routing table has %d", len(names), len(routing))
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] > names[i] {
			t.Fatalf("names are not sorted: %q before %q", names[i-1], names[i])
		}
	}
}

// TestCheckRouting_RefusesDriftInBothDirections asserts both halves of the drift
// check fire. A registered plugin with no routing entry is the failure that let
// EventBridge ship unreachable; an entry naming no plugin is the invented
// identifier that hid AWS Health's real target prefix (#739).
func TestCheckRouting_RefusesDriftInBothDirections(t *testing.T) {
	row := emu.PluginRouting{Display: "Thing", Protocol: "JSON"}

	tests := []struct {
		name  string
		names []string
		table map[string]emu.PluginRouting
		want  string
	}{
		{
			name:  "registered plugin with no routing entry",
			names: []string{"sqs", "brandnew"},
			table: map[string]emu.PluginRouting{"sqs": row},
			want:  `plugin "brandnew" is registered but has no entry`,
		},
		{
			name:  "routing entry naming no plugin",
			names: []string{"sqs"},
			table: map[string]emu.PluginRouting{"sqs": row, "ghost": row},
			want:  `routing entry "ghost" does not correspond to a registered plugin`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkRouting(tt.names, tt.table)
			if err == nil {
				t.Fatalf("want a refusal, got names %v", got)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("want error containing %q, got %q", tt.want, err)
			}
		})
	}
}

// TestCheckRouting_SortsWithoutMutatingTheInput guards the copy: reg.Names()
// returns the registry's own slice in registration order, and sorting it in
// place would reorder a caller's view of the registry as a side effect.
func TestCheckRouting_SortsWithoutMutatingTheInput(t *testing.T) {
	row := emu.PluginRouting{Display: "Thing", Protocol: "JSON"}
	names := []string{"sqs", "ec2", "iam"}
	table := map[string]emu.PluginRouting{"sqs": row, "ec2": row, "iam": row}

	got, err := checkRouting(names, table)
	if err != nil {
		t.Fatalf("checkRouting: %v", err)
	}
	if want := []string{"ec2", "iam", "sqs"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("want %v, got %v", want, got)
	}
	if want := []string{"sqs", "ec2", "iam"}; strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("input was mutated: want %v, got %v", want, names)
	}
}

// TestReplaceMatrix_RendersFromTheRoutingTable asserts the matrix row carries the
// Display and Protocol the routing table holds, and that content outside the
// markers survives untouched.
func TestReplaceMatrix_RendersFromTheRoutingTable(t *testing.T) {
	doc := []byte("before\n" + beginMarker + "\nstale\n" + endMarker + "\nafter\n")

	got, err := replaceMatrix(doc, []string{"sqs"})
	if err != nil {
		t.Fatalf("replaceMatrix: %v", err)
	}
	out := string(got)
	if strings.Contains(out, "stale") {
		t.Error("the previous matrix survived the replacement")
	}
	if !strings.HasPrefix(out, "before\n") || !strings.HasSuffix(out, "\nafter\n") {
		t.Errorf("content outside the markers changed: %q", out)
	}
	want := "| 1 | " + routing["sqs"].Display + " | `sqs` | " + routing["sqs"].Protocol + " |"
	if !strings.Contains(out, want) {
		t.Errorf("want a row %q in:\n%s", want, out)
	}
	if !strings.Contains(out, "**1 built-in service plugins**") {
		t.Error("want the count to come from the name list")
	}
}

// TestReplaceMatrix_RefusesADocumentWithoutMarkers keeps the tool from silently
// rewriting a file whose shape it does not know.
func TestReplaceMatrix_RefusesADocumentWithoutMarkers(t *testing.T) {
	tests := []struct {
		name string
		doc  string
	}{
		{name: "no markers at all", doc: "nothing here\n"},
		{name: "begin only", doc: beginMarker + "\n"},
		{name: "end only", doc: endMarker + "\n"},
		{name: "end before begin", doc: endMarker + "\n" + beginMarker + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := replaceMatrix([]byte(tt.doc), []string{"sqs"}); err == nil {
				t.Fatal("want a refusal, got nil")
			}
		})
	}
}
