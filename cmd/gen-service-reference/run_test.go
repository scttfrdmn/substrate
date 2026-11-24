package main

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var theDocs = filepath.Join("..", "..", "docs")

// TestRun_TheTreeIsCurrent is `make docs-reference-check` run directly: the matrix and the
// sidebar match the registry, the legacy map lands, and every link between pages lands.
func TestRun_TheTreeIsCurrent(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"-out", filepath.Join(theDocs, "services.md"), "-check"}, &out); err != nil {
		t.Fatalf("the reference is out of date: %v\n%s", err, out.String())
	}
}

// copyDocs copies the parts of the real docs tree this tool reads into a temporary
// directory, so a test can damage one and assert the tool notices.
func copyDocs(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	pages, err := filepath.Glob(filepath.Join(theDocs, "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	rels := []string{".vitepress/legacy-anchors.json", ".vitepress/services-sidebar.json", "reference"}
	for _, p := range pages {
		rels = append(rels, filepath.Base(p))
	}
	for _, rel := range rels {
		src := filepath.Join(theDocs, filepath.FromSlash(rel))
		err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			r, err := filepath.Rel(theDocs, p)
			if err != nil {
				return err
			}
			if d.IsDir() {
				return os.MkdirAll(filepath.Join(dst, r), 0o750)
			}
			b, err := os.ReadFile(p) //nolint:gosec // a test fixture path.
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(filepath.Join(dst, r)), 0o750); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dst, r), b, 0o600)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func runIn(t *testing.T, docs string, extra ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := run(append([]string{"-out", filepath.Join(docs, "services.md")}, extra...), &out)
	return out.String(), err
}

func TestRun_CheckRefusesAStaleSidebarAndWriteRestoresIt(t *testing.T) {
	docs := copyDocs(t)
	sidebar := filepath.Join(docs, ".vitepress", "services-sidebar.json")
	if err := os.Remove(sidebar); err != nil {
		t.Fatal(err)
	}
	if _, err := runIn(t, docs, "-check"); err == nil || !strings.Contains(err.Error(), "services-sidebar.json is out of date") {
		t.Fatalf("want the missing sidebar refused, got %v", err)
	}
	out, err := runIn(t, docs)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if !strings.Contains(out, "updated "+sidebar) || !strings.Contains(out, "services.md already up to date") {
		t.Errorf("want one file updated and one current, got %q", out)
	}
	if _, err := runIn(t, docs, "-check"); err != nil {
		t.Fatalf("the check rejects what the write produced: %v", err)
	}
}

func TestRun_RefusesAnIndexWithoutMarkers(t *testing.T) {
	docs := copyDocs(t)
	if err := os.WriteFile(filepath.Join(docs, "services.md"), []byte("# Service Reference\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runIn(t, docs); err == nil || !strings.Contains(err.Error(), "markers") {
		t.Fatalf("want a refusal naming the markers, got %v", err)
	}
	if _, err := runIn(t, t.TempDir()); err == nil {
		t.Fatal("want a refusal for a docs directory with no index")
	}
	var out bytes.Buffer
	if err := run([]string{"-nope"}, &out); err == nil || !strings.Contains(err.Error(), "parse flags") {
		t.Fatalf("want a flag parse failure, got %v", err)
	}
}

func TestRenderSidebar_LabelsEachEntryWithItsPageTitle(t *testing.T) {
	docs := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(docs, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("reference/sqs.md", "# SQS\n")
	write("reference/ec2.md", "```\n# not the title\n```\n\n# EC2\n")
	write("reference/apigatewayv2.md", "# API Gateway v2 (HTTP)\n")
	write("reference/lambda.md", "# `Lambda` {#lambda}\n")
	write("reference/concepts/routing.md", "# Request routing\n")
	write("reference/concepts/errors.md", "# Errors\n")

	raw, err := renderSidebar(docs, []string{"sqs", "ec2", "apigatewayv2", "lambda"})
	if err != nil {
		t.Fatal(err)
	}
	var sb sidebar
	if err := json.Unmarshal(raw, &sb); err != nil {
		t.Fatal(err)
	}
	label := func(items []sidebarItem) string {
		var out []string
		for _, it := range items {
			out = append(out, it.Text+"="+it.Link)
		}
		return strings.Join(out, ", ")
	}
	// The page title, not PluginRouting.Display ("API Gateway (HTTP)", "EC2 / VPC").
	want := "API Gateway v2 (HTTP)=/reference/apigatewayv2, EC2=/reference/ec2, Lambda=/reference/lambda, SQS=/reference/sqs"
	if got := label(sb.Services); got != want {
		t.Errorf("services = %s\nwant       %s", got, want)
	}
	if got, want := label(sb.Concepts), "Errors=/reference/concepts/errors, Request routing=/reference/concepts/routing"; got != want {
		t.Errorf("concepts = %s, want %s", got, want)
	}
	if !bytes.HasSuffix(raw, []byte("}\n")) {
		t.Error("want a trailing newline")
	}

	if _, err := renderSidebar(docs, []string{"sqs", "kms"}); err == nil || !strings.Contains(err.Error(), "-scaffold kms") {
		t.Errorf("want a plugin with no page refused with the scaffold hint, got %v", err)
	}
	write("reference/concepts/untitled.md", "no title\n")
	if _, err := renderSidebar(docs, []string{"sqs"}); err == nil || !strings.Contains(err.Error(), "has no `# ` title") {
		t.Errorf("want an untitled concept page refused, got %v", err)
	}
}

func TestRun_CheckRefusesAConceptPageTheSidebarLacks(t *testing.T) {
	docs := copyDocs(t)
	if err := os.WriteFile(filepath.Join(docs, "reference", "concepts", "pagination.md"), []byte("# Pagination\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runIn(t, docs, "-check"); err == nil || !strings.Contains(err.Error(), "services-sidebar.json is out of date") {
		t.Fatalf("want the stale sidebar refused, got %v", err)
	}
}

// editLegacy rewrites the copied map through fn.
func editLegacy(t *testing.T, docs string, fn func(map[string]string)) {
	t.Helper()
	p := filepath.Join(docs, legacyAnchorsPath)
	raw, err := os.ReadFile(p) //nolint:gosec // a path this test made.
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	fn(m)
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckLegacyAnchors(t *testing.T) {
	tests := []struct {
		name   string
		damage func(t *testing.T, docs string)
		want   string
	}{
		{
			name:   "a key removed",
			damage: func(t *testing.T, docs string) { editLegacy(t, docs, func(m map[string]string) { delete(m, "ec2") }) },
			want:   "are frozen",
		},
		{
			name: "a key added",
			damage: func(t *testing.T, docs string) {
				editLegacy(t, docs, func(m map[string]string) { m["brand-new"] = "/reference/ec2" })
			},
			want: "are frozen",
		},
		{
			name: "a target outside the reference",
			damage: func(t *testing.T, docs string) {
				editLegacy(t, docs, func(m map[string]string) { m["ec2"] = "/testing-guide" })
			},
			want: "#ec2 -> /testing-guide: a target is a /reference/ page",
		},
		{
			name: "a target page that does not exist",
			damage: func(t *testing.T, docs string) {
				editLegacy(t, docs, func(m map[string]string) { m["ec2"] = "/reference/ec3" })
			},
			want: "#ec2 -> /reference/ec3: no such page",
		},
		{
			name: "a heading renamed out from under its entry",
			damage: func(t *testing.T, docs string) {
				p := filepath.Join(docs, "reference", "cloudformation.md")
				b, err := os.ReadFile(p) //nolint:gosec // a path this test made.
				if err != nil {
					t.Fatal(err)
				}
				b = bytes.Replace(b, []byte("## Strict mode and template validation"), []byte("## Strict mode"), 1)
				if err := os.WriteFile(p, b, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "-> /reference/cloudformation#strict-mode-and-template-validation: the page has no heading with that anchor",
		},
		{
			name: "a key the index answers itself",
			damage: func(t *testing.T, docs string) {
				p := filepath.Join(docs, "services.md")
				b, err := os.ReadFile(p) //nolint:gosec // a path this test made.
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, append(b, []byte("\n## EC2\n")...), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "#ec2 is a heading on services.md itself",
		},
		{
			name: "a map that is not JSON",
			damage: func(t *testing.T, docs string) {
				if err := os.WriteFile(filepath.Join(docs, legacyAnchorsPath), []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "parse ",
		},
		{
			name: "no map",
			damage: func(t *testing.T, docs string) {
				if err := os.Remove(filepath.Join(docs, legacyAnchorsPath)); err != nil {
					t.Fatal(err)
				}
			},
			want: "read ",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			docs := copyDocs(t)
			if err := checkLegacyAnchors(docs); err != nil {
				t.Fatalf("the undamaged copy is refused: %v", err)
			}
			tc.damage(t, docs)
			err := checkLegacyAnchors(docs)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestCheckLinks(t *testing.T) {
	docs := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(docs, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("index.md", "# Home\n\n[root](/) [self](#home) [ext](https://x.invalid#nope) [img](/og.png#x)\n")
	write("guide.md", "# Guide\n\n## Part one\n\n[page](/reference/ec2#supported-operations) [rel](reference/ec2.md)\n"+
		"[html](/reference/ec2.html#ec2) `[code](#nope)`\n\n```\n[fenced](#nope)\n```\n")
	write("reference/ec2.md", "# EC2\n\n## Supported operations\n\n[up](../guide#part-one) [same](#ec2)\n"+
		"[angle](</guide#part-one>) [def]\n\n[def]: /guide#part-one\n\n<a href=\"/reference/ec2#supported-operations\">x</a>\n"+
		"[encoded](/reference/ec2#supported%2Doperations)\n")
	write("node_modules/x/readme.md", "[dead](#nope)\n")
	write(".vitepress/cache/x.md", "[dead](#nope)\n")
	if err := checkLinks(docs); err != nil {
		t.Fatalf("want every link accepted, got %v", err)
	}

	write("reference/s3.md", "# S3\n\n[a](#no-such-heading) [b](/reference/nope) [c](/guide#part-two) [d](/guide#part-one)\n"+
		"\n[e]: /guide#part-three\n\n<a href='#gone'>x</a>\n")
	err := checkLinks(docs)
	if err == nil {
		t.Fatal("want dead links refused")
	}
	for _, want := range []string{
		"5 links between docs pages do not land",
		"(/guide#part-three) names an anchor",
		"(#gone) names an anchor",
		"(#no-such-heading) names an anchor",
		"(/reference/nope) names a page that does not exist",
		"(/guide#part-two) names an anchor",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("want %q in:\n%v", want, err)
		}
	}
	if err := checkLinks(filepath.Join(docs, "absent")); err == nil {
		t.Error("want a missing docs directory refused")
	}
}
