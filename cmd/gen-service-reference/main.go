// Command gen-service-reference keeps the coverage matrix in docs/services.md in
// sync with the live plugin registry so the documented service count and plugin
// list can never drift from the implementation (see issue #364).
//
// It boots the default plugin registry, enumerates the registered plugins, and
// renders a Markdown coverage table between the marker comments
//
//	<!-- BEGIN GENERATED COVERAGE MATRIX -->
//	<!-- END GENERATED COVERAGE MATRIX -->
//
// in docs/services.md. Everything outside the markers — including the
// hand-written per-service operation, CloudFormation, and cost detail — is left
// untouched. Per-plugin display names and protocols come from the emulator's own
// routing table ([emu.PluginRoutingCatalog], emulator/routing.go); the tool fails
// if a registered plugin has no entry there (or an entry names a plugin that is
// not registered), which is what a CI drift check enforces.
//
// Usage:
//
//	go run ./cmd/gen-service-reference -out docs/services.md          # rewrite the matrix
//	go run ./cmd/gen-service-reference -out docs/services.md -check    # exit non-zero if out of date
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	emu "github.com/scttfrdmn/substrate/emulator"
)

const (
	beginMarker = "<!-- BEGIN GENERATED COVERAGE MATRIX -->"
	endMarker   = "<!-- END GENERATED COVERAGE MATRIX -->"
)

// routing is the per-plugin routing and documentation metadata, read from the
// emulator's own table (emulator/routing.go) rather than kept in a second copy
// here. One table then serves three consumers: this matrix, the drift check
// below, and the reachability sweep test that asserts every identifier in it
// resolves to a registered plugin (#739). A duplicate map in cmd/ could agree
// with the docs while disagreeing with what an SDK actually sends, which is the
// class of failure #739 exists to close.
var routing = emu.PluginRoutingCatalog()

func main() {
	check := flag.Bool("check", false, "exit non-zero if the file is out of date instead of writing it")
	out := flag.String("out", "docs/services.md", "path to the services reference file")
	flag.Parse()

	names, err := registeredPlugins()
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-service-reference:", err)
		os.Exit(1)
	}

	existing, err := os.ReadFile(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gen-service-reference: read %s: %v\n", *out, err)
		os.Exit(1)
	}

	updated, err := replaceMatrix(existing, names)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-service-reference:", err)
		os.Exit(1)
	}

	if *check {
		if !bytes.Equal(existing, updated) {
			fmt.Fprintf(os.Stderr, "gen-service-reference: %s coverage matrix is out of date; run `make docs-reference` and commit the result\n", *out)
			os.Exit(1)
		}
		return
	}

	if bytes.Equal(existing, updated) {
		fmt.Printf("%s already up to date (%d plugins)\n", *out, len(names))
		return
	}
	if err := os.WriteFile(*out, updated, 0o644); err != nil { //nolint:gosec // docs file, world-readable is fine.
		fmt.Fprintln(os.Stderr, "gen-service-reference:", err)
		os.Exit(1)
	}
	fmt.Printf("updated %s (%d plugins)\n", *out, len(names))
}

// registeredPlugins boots the default registry and returns the sorted plugin
// names, verifying that metadata is complete in both directions.
func registeredPlugins() ([]string, error) {
	reg := emu.NewPluginRegistry()
	state := emu.NewMemoryStateManager()
	tc := emu.NewTimeController(time.Unix(0, 0).UTC())
	logger := emu.NewDefaultLogger(slog.LevelError, false)
	store := emu.NewEventStore(emu.EventStoreConfig{Enabled: false})

	if err := emu.RegisterDefaultPlugins(context.Background(), reg, state, tc, logger, store, nil); err != nil {
		return nil, fmt.Errorf("register default plugins: %w", err)
	}
	return checkRouting(reg.Names(), routing)
}

// checkRouting verifies that the routing table and the registered plugin names
// agree in both directions and returns the names sorted. It takes the table as a
// parameter rather than reading the package-level [routing] so that both refusals
// are reachable from a test — the drift check is the whole point of the -check
// flag, and an untested drift check is one nobody knows still fires (#739).
func checkRouting(names []string, table map[string]emu.PluginRouting) ([]string, error) {
	registered := make(map[string]bool, len(names))
	for _, n := range names {
		registered[n] = true
		if _, ok := table[n]; !ok {
			return nil, fmt.Errorf("plugin %q is registered but has no entry in emulator/routing.go; add one, citing the source of its target prefix, hosts and signing names", n)
		}
	}
	for n := range table {
		if !registered[n] {
			return nil, fmt.Errorf("routing entry %q does not correspond to a registered plugin; remove it from emulator/routing.go", n)
		}
	}
	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)
	return sorted, nil
}

// replaceMatrix swaps the content between the begin/end markers in doc for a
// freshly rendered coverage matrix, leaving everything else untouched.
func replaceMatrix(doc []byte, names []string) ([]byte, error) {
	begin := bytes.Index(doc, []byte(beginMarker))
	end := bytes.Index(doc, []byte(endMarker))
	if begin < 0 || end < 0 || end < begin {
		return nil, fmt.Errorf("could not find %q ... %q markers in the document", beginMarker, endMarker)
	}

	var matrix bytes.Buffer
	fmt.Fprintln(&matrix, beginMarker)
	fmt.Fprintf(&matrix, "Substrate ships **%d built-in service plugins**. This section is generated\n", len(names))
	fmt.Fprintln(&matrix, "from the plugin registry (`make docs-reference`), so the count and plugin list")
	fmt.Fprintln(&matrix, "cannot drift from the implementation. The live count is also available from the")
	fmt.Fprintln(&matrix, "`/ready` endpoint (`curl http://localhost:4566/ready`). Per-service operation,")
	fmt.Fprintln(&matrix, "CloudFormation, and cost detail follows below the matrix.")
	fmt.Fprintln(&matrix)
	fmt.Fprintln(&matrix, "| # | Service | Plugin name | Protocol |")
	fmt.Fprintln(&matrix, "|---|---------|-------------|----------|")
	for i, n := range names {
		m := routing[n]
		fmt.Fprintf(&matrix, "| %d | %s | `%s` | %s |\n", i+1, m.Display, n, m.Protocol)
	}
	fmt.Fprint(&matrix, endMarker)

	var result bytes.Buffer
	result.Write(doc[:begin])
	result.Write(matrix.Bytes())
	result.Write(doc[end+len(endMarker):])
	return result.Bytes(), nil
}
