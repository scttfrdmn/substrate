package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// parse turns one synthetic source file into the shape buildFrom reads. Every refusal in
// this generator is a refusal about source, so the tests are about source too: a few lines
// of a plugin that dispatches in an unexpected way is a far more direct test than breaking
// the real tree and reading the fallout.
func parse(t *testing.T, src string) []*ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic source: %v", err)
	}
	return []*ast.File{file}
}

// plugin renders a synthetic plugin: a registration table naming it, a Name method, and the
// HandleRequest body given.
func plugin(typeName, name, handler string) string {
	return `package emulator

func RegisterDefaultPlugins() error {
	registrations := []struct {
		plugin Plugin
		name   string
	}{
		{&` + typeName + `{}, "label"},
	}
	_ = registrations
	return nil
}

type ` + typeName + ` struct{}

func (p *` + typeName + `) Name() string { return ` + name + ` }

func (p *` + typeName + `) HandleRequest(req *AWSRequest) error {
` + handler + `
}
`
}

// TestBuildFrom_ReadsTheDispatchSwitch is the nominal path, across every switch tag shape
// the tree uses. All four name the operation being dispatched; a switch on anything else is
// not a dispatch, which is the distinction that makes this an extractor and not a grep.
func TestBuildFrom_ReadsTheDispatchSwitch(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "req.Operation",
			body: `	switch req.Operation {
	case "CreateThing":
		return nil
	case "DeleteThing":
		return nil
	}
	return nil`,
		},
		{
			name: "op",
			body: `	op := req.Operation
	switch op {
	case "CreateThing":
		return nil
	case "DeleteThing":
		return nil
	}
	return nil`,
		},
		{
			name: "action",
			body: `	action := req.Operation
	switch action {
	case "CreateThing", "DeleteThing":
		return nil
	}
	return nil`,
		},
		{
			name: "operation",
			body: `	operation := req.Operation
	switch operation {
	case "DeleteThing":
		return nil
	case "CreateThing":
		return nil
	}
	return nil`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat, err := buildFrom(parse(t, plugin("ThingPlugin", `"thing"`, tt.body)))
			if err != nil {
				t.Fatalf("buildFrom: %v", err)
			}
			got := strings.Join(cat["thing"], ",")
			if want := "CreateThing,DeleteThing"; got != want {
				t.Errorf("want %q, got %q (sorted, deduplicated)", want, got)
			}
		})
	}
}

// TestBuildFrom_SkipsASwitchThatIsNotADispatch pins the property a `case "…"` grep cannot
// have. EC2 is the case that matters: a naive grep over ec2_plugin.go returns 106 strings
// where the dispatch switch has 92, because filter names and attribute names are `case`
// labels too.
func TestBuildFrom_SkipsASwitchThatIsNotADispatch(t *testing.T) {
	body := `	switch req.Operation {
	case "DescribeThings":
		switch filter {
		case "thing-id", "thing-state":
			return nil
		}
		return nil
	}
	return nil`
	cat, err := buildFrom(parse(t, plugin("ThingPlugin", `"thing"`, body)))
	if err != nil {
		t.Fatalf("buildFrom: %v", err)
	}
	if got, want := strings.Join(cat["thing"], ","), "DescribeThings"; got != want {
		t.Errorf("want %q, got %q — the inner switch is over filter names, not operations", want, got)
	}
}

// TestBuildFrom_FollowsAClaimChain covers ConfigService and Organizations, whose operations
// appear in no switch inside HandleRequest at all. Those 59 operations are most of what the
// hand counts on #1095 missed.
func TestBuildFrom_FollowsAClaimChain(t *testing.T) {
	src := plugin("ThingPlugin", `"thing"`, `	for _, claim := range []func(string) (handler, bool){p.recorderOperation, p.channelOperation} {
		if h, ok := claim(req.Operation); ok {
			return h(req)
		}
	}
	return nil`) + `
type handler func(req *AWSRequest) error

func (p *ThingPlugin) recorderOperation(op string) (handler, bool) {
	switch op {
	case "PutRecorder":
		return p.putRecorder, true
	}
	return nil, false
}

func (p *ThingPlugin) channelOperation(op string) (handler, bool) {
	switch op {
	case "PutChannel":
		return p.putChannel, true
	}
	return nil, false
}
`
	cat, err := buildFrom(parse(t, src))
	if err != nil {
		t.Fatalf("buildFrom: %v", err)
	}
	if got, want := strings.Join(cat["thing"], ","), "PutChannel,PutRecorder"; got != want {
		t.Errorf("want %q, got %q", want, got)
	}
}

// TestBuildFrom_DoesNotFollowAMethodThatIsNotAClaimHelper pins the scoping that keeps the
// claim-chain walk from wandering. The unscoped version of this walk followed any selector
// expression and swept 854 operations into execute-api by reaching other plugins' switches;
// here the helper is on the same receiver but has the wrong signature, so it is not one.
func TestBuildFrom_DoesNotFollowAMethodThatIsNotAClaimHelper(t *testing.T) {
	src := plugin("ThingPlugin", `"thing"`, `	return p.render(req)`) + `
func (p *ThingPlugin) render(req *AWSRequest) error {
	switch req.Operation {
	case "NotADispatch":
		return nil
	}
	return nil
}
`
	if _, err := buildFrom(parse(t, src)); err == nil {
		t.Fatal("want a refusal: nothing in HandleRequest dispatches, so the catalog would be empty")
	} else if !strings.Contains(err.Error(), "no operations extracted") {
		t.Errorf("want the empty-catalog refusal, got %q", err)
	}
}

// TestBuildFrom_ResolvesANameConstant covers ELB, whose Name returns elbServiceName rather
// than a literal — and whose registry key ("elasticloadbalancing") differs from the label
// beside it in the registration table ("elb"). Keying the catalog by the label would produce
// a catalog nothing can look ELB up in.
func TestBuildFrom_ResolvesANameConstant(t *testing.T) {
	src := plugin("ELBPlugin", "elbServiceName", `	switch req.Operation {
	case "CreateLoadBalancer":
		return nil
	}
	return nil`) + `
const elbServiceName = "elasticloadbalancing"
`
	cat, err := buildFrom(parse(t, src))
	if err != nil {
		t.Fatalf("buildFrom: %v", err)
	}
	if _, ok := cat["elasticloadbalancing"]; !ok {
		t.Errorf("want the catalog keyed by the registry key, got keys %v", cat.services())
	}
	if _, ok := cat["label"]; ok {
		t.Error("the catalog is keyed by the registration table's label, which is not the registry key")
	}
}

// TestBuildFrom_RefusesWhatItCannotRead collects the refusals that keep a shape this
// generator does not understand from becoming a quietly short catalog. Each of these would
// otherwise produce a file that looks complete.
func TestBuildFrom_RefusesWhatItCannotRead(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a dispatch shape it does not recognize",
			src:  plugin("ThingPlugin", `"thing"`, "\treturn nil"),
			want: "no operations extracted",
		},
		{
			name: "an exception that is no longer true",
			src: plugin("OpenSearchPlugin", `"opensearch"`, `	switch req.Operation {
	case "CreateDomain":
		return nil
	}
	return nil`),
			want: `"opensearch" is listed in emptyByDesign`,
		},
		{
			name: "a Name method it cannot read",
			src: plugin("ThingPlugin", "thingName()", `	switch req.Operation {
	case "CreateThing":
		return nil
	}
	return nil`),
			want: "returns neither a string literal nor a named constant",
		},
		{
			name: "a Name method returning an unknown constant",
			src: plugin("ThingPlugin", "thingName", `	switch req.Operation {
	case "CreateThing":
		return nil
	}
	return nil`),
			want: "which is not a package-level string constant",
		},
		{
			name: "no registration table",
			src:  "package emulator\n\nfunc RegisterDefaultPlugins() error { return nil }\n",
			want: "no plugin registrations found",
		},
		{
			name: "no RegisterDefaultPlugins at all",
			src:  "package emulator\n",
			want: "RegisterDefaultPlugins not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat, err := buildFrom(parse(t, tt.src))
			if err == nil {
				t.Fatalf("want a refusal, got catalog %v", cat)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("want error containing %q, got %q", tt.want, err)
			}
		})
	}
}

// TestBuildFrom_RefusesANameMethodItCannotRead collects the Name shapes this generator does
// not read. The registry key comes from Name and from nothing else, so guessing one — the
// type name lowercased, say — would produce a catalog that looks complete and that nothing
// can look a plugin up in.
func TestBuildFrom_RefusesANameMethodItCannotRead(t *testing.T) {
	const table = `package emulator

func RegisterDefaultPlugins() error {
	registrations := []struct {
		plugin Plugin
		name   string
	}{
		{&ThingPlugin{}, "label"},
	}
	_ = registrations
	return nil
}

type ThingPlugin struct{}

func (p *ThingPlugin) HandleRequest(req *AWSRequest) error {
	switch req.Operation {
	case "CreateThing":
		return nil
	}
	return nil
}
`
	tests := []struct {
		name string
		decl string
		want string
	}{
		{
			name: "absent",
			decl: "",
			want: "has no Name method",
		},
		{
			name: "more than one statement",
			decl: "func (p *ThingPlugin) Name() string {\n\tn := \"thing\"\n\treturn n\n}\n",
			want: "has 2 statements; this generator reads a single return",
		},
		{
			name: "a single statement that is not a return",
			decl: "func (p *ThingPlugin) Name() string {\n\tpanic(\"thing\")\n}\n",
			want: "does not return a single value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat, err := buildFrom(parse(t, table+"\n"+tt.decl))
			if err == nil {
				t.Fatalf("want a refusal, got catalog %v", cat)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("want error containing %q, got %q", tt.want, err)
			}
		})
	}
}

// TestRegisteredTypes_SkipsWhatIsNotAPluginRegistration pins how narrowly the registration
// table is read. Only `&XPlugin{}` is an entry; anything else in there — and the same type
// registered twice — must not become a catalog key, because a key with no plugin behind it
// fails the registry cross-check in cmd/gen-service-reference for a reason nobody can act on.
func TestRegisteredTypes_SkipsWhatIsNotAPluginRegistration(t *testing.T) {
	src := `package emulator

var thingSingleton = ThingPlugin{}

func RegisterDefaultPlugins() error {
	registrations := []struct {
		plugin Plugin
		name   string
	}{
		{nil, "not an address"},
		{&thingSingleton, "not a composite literal"},
		{&thingRouter{}, "type name does not end in Plugin"},
		{&ThingPlugin{}, "label"},
		{&ThingPlugin{}, "the same type twice"},
	}
	_ = registrations
	return nil
}
`
	types, err := registeredTypes(parse(t, src))
	if err != nil {
		t.Fatalf("registeredTypes: %v", err)
	}
	if got, want := strings.Join(types, ","), "ThingPlugin"; got != want {
		t.Errorf("want %q, got %q", want, got)
	}
}

// TestBuildFrom_DoesNotFollowAHelperWithTheWrongParameterType is the other half of the
// claim-chain scoping: the helper is a method on the same receiver returning (T, bool), but it
// takes the request rather than the operation name, so it is a request handler and not a claim.
func TestBuildFrom_DoesNotFollowAHelperWithTheWrongParameterType(t *testing.T) {
	src := plugin("ThingPlugin", `"thing"`, `	if h, ok := p.claim(req); ok {
		return h(req)
	}
	return nil`) + `
type handler func(req *AWSRequest) error

func (p *ThingPlugin) claim(req *AWSRequest) (handler, bool) {
	switch req.Operation {
	case "NotADispatch":
		return nil, true
	}
	return nil, false
}
`
	if _, err := buildFrom(parse(t, src)); err == nil {
		t.Fatal("want a refusal: nothing in HandleRequest dispatches, so the catalog would be empty")
	} else if !strings.Contains(err.Error(), "no operations extracted") {
		t.Errorf("want the empty-catalog refusal, got %q", err)
	}
}

// TestBuildFrom_RefusesAPluginWithNoHandleRequest guards the case a registration table can
// present but a compiling tree cannot: a plugin type that does not satisfy Plugin. It is
// worth a refusal rather than a skip because a skip is how a plugin leaves the catalog
// without anyone noticing.
func TestBuildFrom_RefusesAPluginWithNoHandleRequest(t *testing.T) {
	src := `package emulator

func RegisterDefaultPlugins() error {
	registrations := []struct {
		plugin Plugin
		name   string
	}{
		{&ThingPlugin{}, "thing"},
	}
	_ = registrations
	return nil
}

type ThingPlugin struct{}

func (p *ThingPlugin) Name() string { return "thing" }
`
	_, err := buildFrom(parse(t, src))
	if err == nil {
		t.Fatal("want a refusal, got nil")
	}
	if want := "has no HandleRequest method"; !strings.Contains(err.Error(), want) {
		t.Errorf("want error containing %q, got %q", want, err)
	}
}

// TestBuildFrom_RefusesTwoPluginsWithOneRegistryKey covers the collision the catalog's
// shape makes possible: PluginRegistry keys by Name, so two plugins reporting one Name
// would silently leave one of them out of the catalog.
func TestBuildFrom_RefusesTwoPluginsWithOneRegistryKey(t *testing.T) {
	dispatch := `	switch req.Operation {
	case "CreateThing":
		return nil
	}
	return nil`
	src := `package emulator

func RegisterDefaultPlugins() error {
	registrations := []struct {
		plugin Plugin
		name   string
	}{
		{&FirstPlugin{}, "first"},
		{&SecondPlugin{}, "second"},
	}
	_ = registrations
	return nil
}

type FirstPlugin struct{}
type SecondPlugin struct{}

func (p *FirstPlugin) Name() string { return "thing" }
func (p *SecondPlugin) Name() string { return "thing" }

func (p *FirstPlugin) HandleRequest(req *AWSRequest) error {
` + dispatch + `
}

func (p *SecondPlugin) HandleRequest(req *AWSRequest) error {
` + dispatch + `
}
`
	_, err := buildFrom(parse(t, src))
	if err == nil {
		t.Fatal("want a refusal, got nil")
	}
	if want := `two plugins report Name() == "thing"`; !strings.Contains(err.Error(), want) {
		t.Errorf("want error containing %q, got %q", want, err)
	}
}

// TestBuild_AgreesWithTheCommittedFile is the -check flag's own assertion, run as a test so
// that the drift is visible from `make test` and not only from CI. It also exercises
// parseDir and render, which the synthetic tests above do not reach.
func TestBuild_AgreesWithTheCommittedFile(t *testing.T) {
	cat, err := build("../../emulator")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := render(cat); err != nil {
		t.Fatalf("render: %v", err)
	}
	if cat.total() == 0 || len(cat) == 0 {
		t.Fatalf("extracted %d plugins and %d operations from the live tree", len(cat), cat.total())
	}
	for _, service := range cat.services() {
		ops := cat[service]
		if _, exempt := emptyByDesign[service]; len(ops) == 0 && !exempt {
			t.Errorf("%q has an empty catalog and no recorded reason", service)
		}
		for i := 1; i < len(ops); i++ {
			if ops[i-1] >= ops[i] {
				t.Errorf("%q operations are not sorted and unique: %q then %q", service, ops[i-1], ops[i])
			}
		}
	}
}

// TestParseDir_RefusesADirectoryWithNoSources keeps a mistyped -dir from generating an empty
// catalog that then fails the -check in every later run for the wrong reason.
func TestParseDir_RefusesADirectoryWithNoSources(t *testing.T) {
	if _, err := parseDir(token.NewFileSet(), t.TempDir()); err == nil {
		t.Fatal("want a refusal, got nil")
	}
}

// TestParseDir_RefusesUnparseableSource keeps a half-written plugin file from producing a
// catalog short by exactly the plugins that file declares.
func TestParseDir_RefusesUnparseableSource(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.go"), []byte("package emulator\n\nfunc {\n"), 0o600); err != nil {
		t.Fatalf("write synthetic source: %v", err)
	}
	if _, err := parseDir(token.NewFileSet(), dir); err == nil {
		t.Fatal("want a refusal, got nil")
	}
}

// TestRun_WritesTheCatalogAndThenAgreesWithIt walks the two paths `make operation-catalog` and
// `make operation-catalog-check` take, against the live tree but not the committed file: -out
// is a temporary path, so the first call writes it and the second finds it current.
func TestRun_WritesTheCatalogAndThenAgreesWithIt(t *testing.T) {
	out := filepath.Join(t.TempDir(), "operation_catalog_gen.go")
	args := []string{"-dir", "../../emulator", "-out", out}

	var written bytes.Buffer
	if err := run(args, &written); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := written.String(); !strings.HasPrefix(got, "updated ") {
		t.Errorf("want the write path to report what it wrote, got %q", got)
	}

	var again bytes.Buffer
	if err := run(args, &again); err != nil {
		t.Fatalf("run a second time: %v", err)
	}
	if got := again.String(); !strings.Contains(got, "already up to date") {
		t.Errorf("want the no-op path to say so, got %q", got)
	}

	if err := run(append(args, "-check"), io.Discard); err != nil {
		t.Errorf("-check against the file run just wrote: %v", err)
	}
}

// TestRun_CheckRefusesAStaleFile is the failure `make operation-catalog-check` exists to
// produce, and the reason the committed catalog cannot silently fall behind the router. The
// message names both the file and the directory, because regenerating is the only useful reply.
func TestRun_CheckRefusesAStaleFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "operation_catalog_gen.go")
	if err := os.WriteFile(out, []byte("package emulator\n"), 0o600); err != nil {
		t.Fatalf("write a stale catalog: %v", err)
	}
	err := run([]string{"-check", "-dir", "../../emulator", "-out", out}, io.Discard)
	if err == nil {
		t.Fatal("want a refusal, got nil")
	}
	if want := "is out of date with"; !strings.Contains(err.Error(), want) {
		t.Errorf("want error containing %q, got %q", want, err)
	}
}

// TestRun_RefusesWhatItCannotRead covers the ways run gives up before it has a catalog, each
// of which has to be an exit code rather than an empty file that then passes its own -check.
func TestRun_RefusesWhatItCannotRead(t *testing.T) {
	empty := t.TempDir()
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "an unknown flag",
			args: []string{"-nope"},
			want: "parse flags",
		},
		{
			name: "a directory holding no plugin sources",
			args: []string{"-dir", empty},
			want: "no Go sources found",
		},
		{
			name: "an -out that is not a file",
			args: []string{"-dir", "../../emulator", "-out", empty},
			want: "read " + empty,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(tt.args, io.Discard)
			if err == nil {
				t.Fatal("want a refusal, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("want error containing %q, got %q", tt.want, err)
			}
		})
	}
}
