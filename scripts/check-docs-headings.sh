#!/usr/bin/env bash
# check-docs-headings.sh — fail if a service reference page is titled wrongly or twice.
#
# The reference is docs/services.md (the index) plus one page per plugin and per
# cross-cutting concept under docs/reference/. Each page is hand-edited, so nothing else
# notices:
#   - a page with no `# ` title, or with two (a section pasted in at the wrong level);
#   - two pages with one title, which the sidebar and search then show as one;
#   - a `## ` heading repeated within one page. #1384 shipped 41 duplicated sections when
#     an edit reinserted a 6,680-line copy, and docs-reference-check, which compares the
#     generated matrix and not the prose, passed. A reader then finds two sections that
#     disagree, and a later edit lands in whichever copy it hits.
# Headings inside fenced code blocks are examples, not headings, and are skipped.
#
# `check-docs-headings.sh --self-test` runs the rules over known-bad and known-good pages.
set -euo pipefail

cd "$(dirname "$0")/.."

# headings <level-prefix> <file>: the headings of that level outside fenced code blocks.
# A fence opens on three or more backticks or tildes (after up to three spaces) and closes
# only on a run of the same character at least as long, as CommonMark has it, so a ````
# fence can hold a ``` example.
headings() {
  awk -v p="$1" '
    {
      line = $0
      sub(/^ ? ? ?/, "", line)
      ch = substr(line, 1, 1)
      n = 0
      if (ch == "`" || ch == "~") { while (substr(line, n + 1, 1) == ch) n++ }
      if (fence != "") {
        if (ch == substr(fence, 1, 1) && n >= length(fence) && substr(line, n + 1) ~ /^[ \t]*$/) fence = ""
        next
      }
      if (n >= 3) { fence = substr(line, 1, n); next }
      if (index($0, p) == 1) print substr($0, length(p) + 1)
    }
  ' "$2"
}

# check <file>...: print every problem, and exit non-zero if there is one.
check() {
  local status=0 f titles n dupes
  local all_titles=""
  for f in "$@"; do
    titles="$(headings '# ' "$f")"
    n="$(printf '%s' "$titles" | grep -c '' || true)"
    if [[ "$n" -ne 1 ]]; then
      echo "check-docs-headings: $f has $n \`# \` titles; a page has exactly one"
      status=1
    fi
    dupes="$(headings '## ' "$f" | sort | uniq -d)"
    if [[ -n "$dupes" ]]; then
      echo "check-docs-headings: $f names these sections more than once:"
      printf '%s\n' "$dupes" | sed 's/^/    /'
      echo "  Merge the copies, keeping the one with the newer content."
      status=1
    fi
    all_titles+="$(printf '%s' "$titles" | head -1)"$'\t'"$f"$'\n'
  done
  dupes="$(printf '%s' "$all_titles" | cut -f1 | sort | uniq -d)"
  if [[ -n "$dupes" ]]; then
    echo "check-docs-headings: more than one page is titled:"
    while IFS= read -r t; do
      printf '%s' "$all_titles" | awk -F'\t' -v t="$t" '$1 == t { print "    " t ": " $2 }'
    done <<<"$dupes"
    status=1
  fi
  return "$status"
}

if [[ "${1:-}" == "--self-test" ]]; then
  dir="$(mktemp -d)"
  trap 'rm -rf "$dir"' EXIT
  status=0
  expect() { # expect <want: ok|fail> <description> <file>...
    local want="$1" what="$2" got
    shift 2
    if check "$@" >/dev/null; then got=ok; else got=fail; fi
    if [[ "$got" != "$want" ]]; then
      echo "self-test: want $want, got $got: $what"
      status=1
    fi
  }
  printf '# EC2\n\n## Supported operations\n\n## Cost\n' >"$dir/good.md"
  printf '# S3\n\n```sh\n# a shell comment\n## and another\n```\n\n## Cost\n' >"$dir/fenced.md"
  printf '# SNS\n\n````md\n```\n# inside\n````\n\n~~~\n## Cost\n~~~\n\n## Cost\n' >"$dir/nested-fence.md"
  printf '# SES\n\n```\n~~~\n# still inside\n```\n' >"$dir/mixed-fence.md"
  printf 'no title\n\n## Cost\n' >"$dir/untitled.md"
  printf '# EC2\n\n# Lambda\n' >"$dir/two-titles.md"
  printf '# SQS\n\n## Cost\n\n## Cost\n' >"$dir/dup-section.md"
  printf '# EC2\n\n## Other\n' >"$dir/same-title.md"
  expect ok "one title, distinct sections" "$dir/good.md"
  expect ok "headings in a fence are not headings" "$dir/fenced.md" "$dir/good.md"
  expect ok "a longer fence holds a shorter one" "$dir/nested-fence.md"
  expect ok "a tilde line does not close a backtick fence" "$dir/mixed-fence.md"
  expect fail "a page with no title" "$dir/untitled.md"
  expect fail "a page with two titles" "$dir/two-titles.md"
  expect fail "a section repeated within a page" "$dir/dup-section.md"
  expect fail "two pages with one title" "$dir/good.md" "$dir/same-title.md"
  [[ "$status" -eq 0 ]] && echo "check-docs-headings self-test: ok"
  exit "$status"
fi

pages=(docs/services.md)
while IFS= read -r f; do
  pages+=("$f")
done < <(find docs/reference -name '*.md' | sort)

if ! check "${pages[@]}"; then
  exit 1
fi
echo "check-docs-headings: ok — ${#pages[@]} pages, each titled once and uniquely, no section repeated within a page"
