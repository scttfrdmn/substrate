package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The key set of docs/.vitepress/legacy-anchors.json, pinned. At v0.131.0 the whole
// reference was one page, so a link written against it reads /services#<anchor>. The map
// sends each of the 908 anchors that page served below its coverage matrix to the page that
// holds the heading now, and services.md redirects through it in the browser (#1610).
//
// Its keys are a record of what was published, so they never change: an anchor dropped from
// the map is a link somewhere that silently stops landing. Its values may change, to follow
// a heading that is renamed or moved, and [checkLegacyAnchors] requires that each still
// lands on an anchor its page has.
const (
	legacyAnchorCount  = 908
	legacyAnchorDigest = "e125ae1d44792451b56f92ad3e9a28b05177c9296f4cfdffa4541c1dd85f52e6"
)

// legacyAnchorsPath is where the map lives, relative to the docs directory.
var legacyAnchorsPath = filepath.Join(".vitepress", "legacy-anchors.json")

// checkLegacyAnchors refuses a legacy map whose key set is not the pinned one, a target
// that is not a reference page or names an anchor that page lacks, and a key the index
// still answers itself (a redirect would take the reader away from the heading they
// asked for).
func checkLegacyAnchors(docsDir string) error {
	mapPath := filepath.Join(docsDir, legacyAnchorsPath)
	raw, err := os.ReadFile(mapPath) //nolint:gosec // a checked-in docs file.
	if err != nil {
		return fmt.Errorf("read %s: %w", mapPath, err)
	}
	var legacy map[string]string
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return fmt.Errorf("parse %s: %w", mapPath, err)
	}

	keys := make([]string, 0, len(legacy))
	for k := range legacy {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	if len(keys) != legacyAnchorCount || hex.EncodeToString(sum[:]) != legacyAnchorDigest {
		return fmt.Errorf("%s has %d keys (digest %x); its keys are the %d anchors docs/services.md served "+
			"at v0.131.0 and are frozen. Restore the file from git: a target may change, a key may not",
			mapPath, len(keys), sum, legacyAnchorCount)
	}

	pages := newPageCache(docsDir)
	index, err := pages.anchors(filepath.Join(docsDir, "services.md"))
	if err != nil {
		return err
	}
	var problems []string
	for _, k := range keys {
		target := legacy[k]
		if index[k] {
			problems = append(problems, fmt.Sprintf("#%s is a heading on services.md itself, so redirecting it "+
				"to %s takes the reader away from it; rename one of the two", k, target))
		}
		route, anchor, _ := strings.Cut(target, "#")
		if !strings.HasPrefix(route, "/reference/") {
			problems = append(problems, fmt.Sprintf("#%s -> %s: a target is a /reference/ page", k, target))
			continue
		}
		anchors, err := pages.anchors(filepath.Join(docsDir, filepath.FromSlash(route)+".md"))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			problems = append(problems, fmt.Sprintf("#%s -> %s: no such page", k, target))
		case err != nil:
			return err
		case anchor != "" && !anchors[anchor]:
			problems = append(problems, fmt.Sprintf("#%s -> %s: the page has no heading with that anchor; "+
				"point the entry at the heading's current anchor", k, target))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s:\n  %s", mapPath, strings.Join(problems, "\n  "))
	}
	return nil
}

// checkLinks refuses a link between docs pages whose target page does not exist, or whose
// fragment names no anchor on it. VitePress's build refuses a dead page link but not a dead
// fragment, which lands at the top of the page. The usual cause is a GitHub-style anchor
// ("a-role's" written as a-roles), which VitePress spells a-role-s.
//
// It reads docs/*.md and docs/reference/**, not README.md, which GitHub renders with its own
// anchor rules.
func checkLinks(docsDir string) error {
	pages := newPageCache(docsDir)
	var files []string
	err := filepath.WalkDir(docsDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); p != docsDir && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "public") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".md") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk %s: %w", docsDir, err)
	}
	sort.Strings(files)

	var problems []string
	for _, file := range files {
		raw, err := os.ReadFile(file) //nolint:gosec // a path under the docs directory.
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		for _, link := range pageLinks(string(raw)) {
			target, anchor, ok := resolveLink(docsDir, file, link)
			if !ok {
				continue
			}
			anchors, err := pages.anchors(target)
			switch {
			case errors.Is(err, fs.ErrNotExist):
				problems = append(problems, fmt.Sprintf("%s: (%s) names a page that does not exist", file, link))
			case err != nil:
				return err
			case anchor != "" && !anchors[anchor]:
				problems = append(problems, fmt.Sprintf("%s: (%s) names an anchor %s does not have", file, link, target))
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%d links between docs pages do not land:\n  %s", len(problems), strings.Join(problems, "\n  "))
	}
	return nil
}

var (
	codeSpan = regexp.MustCompile("`+[^`]*`+")
	scheme   = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*:`)
	// linkForms capture a destination in group 1, or in group 2 where a form has two.
	linkForms = []*regexp.Regexp{
		regexp.MustCompile(`\]\(<([^<>\n]*)>|\]\(([^()\s]+)(?:\s+(?:"[^"]*"|'[^']*'))?\)`), // [text](dest "title")
		regexp.MustCompile(`^ {0,3}\[(?:[^\[\]\\]|\\.)+\]:\s*(?:<([^<>\n]*)>|(\S+))`),      // [label]: dest
		regexp.MustCompile(`\shref\s*=\s*(?:"([^"]*)"|'([^']*)')`),                         // <a href="dest">
	}
)

// pageLinks returns the destination of every link in src outside code: inline links,
// reference definitions, and href attributes in the page's HTML.
func pageLinks(src string) []string {
	var out []string
	for _, line := range proseLines(src) {
		line = codeSpan.ReplaceAllString(line, "")
		for _, re := range linkForms {
			for _, m := range re.FindAllStringSubmatch(line, -1) {
				out = append(out, m[1]+m[2])
			}
		}
	}
	return out
}

// resolveLink maps a link written in file to the Markdown page it routes to and the
// fragment it names. ok is false for a link that leaves the docs (a URL, an asset).
func resolveLink(docsDir, file, link string) (target, anchor string, ok bool) {
	if scheme.MatchString(link) || strings.HasPrefix(link, "//") {
		return "", "", false
	}
	route, anchor, _ := strings.Cut(link, "#")
	if decoded, err := url.PathUnescape(anchor); err == nil {
		anchor = decoded
	}
	switch {
	case route == "":
		return file, anchor, true
	case strings.HasPrefix(route, "/"):
		route = path.Clean(route)
		if route == "/" {
			route = "/index"
		}
		target = filepath.Join(docsDir, filepath.FromSlash(route))
	default:
		target = filepath.Join(filepath.Dir(file), filepath.FromSlash(route))
	}
	target = strings.TrimSuffix(target, ".html")
	switch ext := filepath.Ext(target); ext {
	case ".md":
	case "":
		target += ".md"
	default:
		return "", "", false
	}
	return target, anchor, true
}

// pageCache reads each page's anchors once.
type pageCache struct {
	docsDir string
	byPath  map[string]map[string]bool
}

func newPageCache(docsDir string) *pageCache {
	return &pageCache{docsDir: docsDir, byPath: make(map[string]map[string]bool)}
}

func (c *pageCache) anchors(page string) (map[string]bool, error) {
	if a, ok := c.byPath[page]; ok {
		return a, nil
	}
	raw, err := os.ReadFile(page) //nolint:gosec // a path under the docs directory.
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", page, err)
	}
	a := pageAnchors(string(raw))
	c.byPath[page] = a
	return a, nil
}
