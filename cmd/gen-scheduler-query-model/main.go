// Command gen-scheduler-query-model extracts, from the botocore service models vendored under
// emulator/wiremodel (see cmd/gen-wire-model), the serialization facts an EventBridge Scheduler
// universal target needs to turn a JSON Input into a Query (awsQuery or ec2) request (#1501).
//
// # Why
//
// A universal target's Input is the operation's request in the SDK's JSON shape, and a Query
// request is flat form parameters. Most of the mapping is a generic rule — a structure member is
// `Parent.Member`, a list element `Parent.member.N` — but some of it is a property of the shape,
// not of the JSON: a map serializes as `Parent.entry.N.key`/`.value` (with model-named key and
// value), a list can be flattened or name its members, and a structure member can be renamed on
// the wire (`locationName`; on the ec2 protocol, `queryName` or the capitalized `locationName`).
// A JSON object does not say whether it is a structure or a map, so those facts have to come from
// the model, and substrate carries no shapes of its own.
//
// The generator walks each routed operation's input shape and records only the paths whose
// serialization departs from the generic rule. That keeps the output to the facts that change a
// request, and the generic rule stays the default for everything else.
//
// # Usage
//
//	go run ./cmd/gen-scheduler-query-model                   # read emulator/wiremodel
//	go run ./cmd/gen-scheduler-query-model -models <dir>     # read another snapshot directory
//	go run ./cmd/gen-scheduler-query-model -check            # exit non-zero if the table is stale
//
// It writes emulator/scheduler_query_model_gen.go. The snapshot is vendored, so -check is offline and
// CI runs it (`make scheduler-query-model-check`); the emitter itself is tested against fixture
// models in testdata/.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/scttfrdmn/substrate/emulator"
)

// service is one SDK service identifier whose plugin speaks Query.
type service struct {
	sdkID  string // the universal target's service, and the generated table's key
	model  string // botocore data directory name
	plugin string // substrate plugin whose routed operations are walked
	ec2    bool   // the ec2 protocol's rules rather than awsQuery's
}

// services are the Query services schedulerSDKServices maps (emulator/scheduler_targets.go).
var services = []service{
	{"autoscaling", "autoscaling", "autoscaling", false},
	{"cloudformation", "cloudformation", "cloudformation", false},
	{"cloudwatch", "cloudwatch", "monitoring", false},
	{"ec2", "ec2", "ec2", true},
	{"elasticache", "elasticache", "elasticache", false},
	{"elasticloadbalancing", "elb", "elasticloadbalancing", false},
	{"elasticloadbalancingv2", "elbv2", "elasticloadbalancing", false},
	{"iam", "iam", "iam", false},
	{"rds", "rds", "rds", false},
	{"redshift", "redshift", "redshift", false},
	{"sns", "sns", "sns", false},
	{"sts", "sts", "sts", false},
}

type ref struct {
	Shape        string `json:"shape"`
	LocationName string `json:"locationName"`
	QueryName    string `json:"queryName"`
	Flattened    bool   `json:"flattened"`
}

type shape struct {
	Type      string         `json:"type"`
	Members   map[string]ref `json:"members"`
	Member    *ref           `json:"member"`
	Key       *ref           `json:"key"`
	Value     *ref           `json:"value"`
	Flattened bool           `json:"flattened"`
}

type model struct {
	Metadata struct {
		APIVersion string `json:"apiVersion"`
		Protocol   string `json:"protocol"`
	} `json:"metadata"`
	Operations map[string]struct {
		Input *ref `json:"input"`
	} `json:"operations"`
	Shapes map[string]shape `json:"shapes"`
}

// hint is one path's departure from the generic rule; it mirrors emulator.schedQueryHint.
type hint struct {
	Name  string // the member's wire name
	List  string // a list's member name
	Flat  bool   // a flattened list or map
	Map   bool   // the value is a map
	Key   string // a map's key name
	Value string // a map's value name
}

func main() {
	modelsDir := flag.String("models", filepath.Join("emulator", "wiremodel"), "the vendored botocore snapshot")
	out := flag.String("out", "emulator/scheduler_query_model_gen.go", "output file")
	check := flag.Bool("check", false, "exit non-zero if the output file is out of date, without writing it")
	flag.Parse()
	if err := run(*modelsDir, *out, *check, services); err != nil {
		fmt.Fprintln(os.Stderr, "gen-scheduler-query-model:", err)
		os.Exit(1)
	}
}

// errStale reports a -check run whose output file differs from what the models generate.
var errStale = errors.New("out of date; run `make scheduler-query-model`")

// run generates the table for svcs from modelsDir and writes it to out, or with check compares it.
func run(modelsDir, out string, check bool, svcs []service) error {
	src, err := generate(modelsDir, svcs)
	if err != nil {
		return err
	}
	if check {
		current, err := os.ReadFile(out)
		if err != nil {
			return fmt.Errorf("read %s: %w", out, err)
		}
		if !bytes.Equal(current, src) {
			return fmt.Errorf("%s: %w", out, errStale)
		}
		return nil
	}
	if err := os.WriteFile(out, src, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}
	fmt.Printf("updated %s\n", out)
	return nil
}

// generate renders the table for svcs from the models under modelsDir.
func generate(modelsDir string, svcs []service) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("// Code generated by cmd/gen-scheduler-query-model; DO NOT EDIT.\n//\n")
	buf.WriteString("// Source: the botocore service models vendored under emulator/wiremodel, at these API versions:\n")
	tables := map[string]map[string]map[string]hint{}
	for _, svc := range svcs {
		m, err := loadModel(modelsDir, svc.model)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&buf, "//   - %s %s (%s)\n", svc.model, m.Metadata.APIVersion, m.Metadata.Protocol)
		ops := map[string]map[string]hint{}
		for _, op := range emulator.RoutedOperations(svc.plugin) {
			o, ok := m.Operations[op]
			if !ok || o.Input == nil {
				continue
			}
			hints := map[string]hint{}
			walk(m, svc.ec2, o.Input.Shape, "", hints, 0)
			if len(hints) > 0 {
				ops[op] = hints
			}
		}
		tables[svc.sdkID] = ops
	}
	buf.WriteString("//\n// Regenerate with `make scheduler-query-model`.\n\n")
	buf.WriteString("package emulator\n\n")
	buf.WriteString("// schedQueryModel maps an SDK service identifier and an operation to the input paths whose Query\n")
	buf.WriteString("// serialization departs from the generic rule; see cmd/gen-scheduler-query-model.\n")
	buf.WriteString("var schedQueryModel = map[string]map[string]map[string]schedQueryHint{ //nolint:gochecknoglobals // generated immutable table\n")
	for _, sdk := range sortedKeys(tables) {
		fmt.Fprintf(&buf, "%q: {\n", sdk)
		for _, op := range sortedKeys(tables[sdk]) {
			fmt.Fprintf(&buf, "%q: {\n", op)
			for _, p := range sortedKeys(tables[sdk][op]) {
				fmt.Fprintf(&buf, "%q: %s,\n", p, literal(tables[sdk][op][p]))
			}
			buf.WriteString("},\n")
		}
		buf.WriteString("},\n")
	}
	buf.WriteString("}\n")
	src, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format: %w", err)
	}
	return src, nil
}

// walk records the hints under the shape named name, reached at path.
func walk(m *model, ec2 bool, name, path string, hints map[string]hint, depth int) {
	if depth > 12 {
		return
	}
	sh, ok := m.Shapes[name]
	if !ok {
		return
	}
	switch sh.Type {
	case "structure":
		for _, member := range sortedKeys(sh.Members) {
			r := sh.Members[member]
			child := member
			if path != "" {
				child = path + "." + member
			}
			if wire := wireName(ec2, member, r); wire != member {
				h := hints[child]
				h.Name = wire
				hints[child] = h
			}
			describe(m, ec2, r, child, hints)
			walk(m, ec2, r.Shape, child, hints, depth+1)
		}
	case "list":
		// An element is described as well as walked, so a list of lists or of maps carries the
		// inner shape's facts at path[].
		if sh.Member != nil {
			describe(m, ec2, *sh.Member, path+"[]", hints)
			walk(m, ec2, sh.Member.Shape, path+"[]", hints, depth+1)
		}
	case "map":
		if sh.Value != nil {
			describe(m, ec2, *sh.Value, path+"{}", hints)
			walk(m, ec2, sh.Value.Shape, path+"{}", hints, depth+1)
		}
	}
}

// describe records a list or map member's own serialization facts at path.
func describe(m *model, ec2 bool, r ref, path string, hints map[string]hint) {
	sh := m.Shapes[r.Shape]
	h := hints[path]
	switch sh.Type {
	case "list":
		if ec2 || sh.Member == nil {
			return // the ec2 protocol always numbers a list's elements directly
		}
		h.Flat = sh.Flattened || r.Flattened
		if sh.Member.LocationName != "" && sh.Member.LocationName != "member" {
			h.List = sh.Member.LocationName
		}
		if !h.Flat && h.List == "" {
			return
		}
	case "map":
		h.Map = true
		h.Flat = sh.Flattened || r.Flattened
		h.Key, h.Value = "key", "value"
		if sh.Key != nil && sh.Key.LocationName != "" {
			h.Key = sh.Key.LocationName
		}
		if sh.Value != nil && sh.Value.LocationName != "" {
			h.Value = sh.Value.LocationName
		}
	default:
		return
	}
	hints[path] = h
}

// wireName is a structure member's name on the wire, as botocore's serializers compute it.
func wireName(ec2 bool, member string, r ref) string {
	if ec2 {
		if r.QueryName != "" {
			return r.QueryName
		}
		if r.LocationName != "" {
			return strings.ToUpper(r.LocationName[:1]) + r.LocationName[1:]
		}
		return member
	}
	if r.LocationName != "" {
		return r.LocationName
	}
	return member
}

func literal(h hint) string {
	var parts []string
	if h.Name != "" {
		parts = append(parts, "Name: "+strconv.Quote(h.Name))
	}
	if h.List != "" {
		parts = append(parts, "List: "+strconv.Quote(h.List))
	}
	if h.Flat {
		parts = append(parts, "Flat: true")
	}
	if h.Map {
		parts = append(parts, "Map: true", "Key: "+strconv.Quote(h.Key), "Value: "+strconv.Quote(h.Value))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// loadModel reads one model from the snapshot: name.json.gz, whose "service" member is botocore's
// service-2.json pruned to the routed operations.
func loadModel(dir, name string) (*model, error) {
	path := filepath.Join(dir, name+".json.gz")
	f, err := os.Open(path) //nolint:gosec // path is under the snapshot directory.
	if err != nil {
		return nil, fmt.Errorf("no model for %s under %s: %w", name, dir, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gunzip %s model: %w", name, err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("read %s model: %w", name, err)
	}
	var doc struct {
		Service model `json:"service"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("decode %s model: %w", name, err)
	}
	return &doc.Service, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
