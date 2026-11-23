#!/usr/bin/env bash
# check-doc-counts.sh — fail if the README or docs prose pins a plugin, service or
# operation count ("63 plugins", "1,105 operations"). Pinned counts go stale
# silently: the README and the docs home page said 63 plugins while the registry
# had 68 (#1604). The one place a count may appear is the coverage matrix in
# docs/services.md, between its BEGIN/END GENERATED COVERAGE MATRIX markers, which
# `make docs-reference` renders from the registry and `make docs-reference-check`
# keeps in sync. Link to that matrix, or to the server's /ready endpoint, instead.
#
# docs/services.md outside the matrix is held only to the plugin total: its
# per-service sections legitimately say how many operations one service routes.
set -euo pipefail

cd "$(dirname "$0")/.."

# A number (digits, optionally comma-grouped, optionally closing a **bold** run),
# then at most three words, then the counted noun.
pattern='\b\d[\d,]*\**(?:\s+[A-Za-z-]+){0,3}\s+(?:plugins|services|operations)\b'
plugin_pattern='\b\d[\d,]*\**(?:\s+[A-Za-z-]+){0,3}\s+plugins\b'

files=()
while IFS= read -r f; do
  files+=("$f")
done < <(
  {
    echo "README.md"
    echo "python/README.md"
    find docs -name '*.md' \
      -not -path '*/node_modules/*' \
      -not -path '*/.vitepress/dist/*' \
      -not -path '*/.vitepress/cache/*' \
      -not -path 'docs/services.md'
  } | sort -u
)

status=0
for f in "${files[@]}"; do
  [[ -f "$f" ]] || continue
  if PATTERN="$pattern" perl -ne '
      if (/$ENV{PATTERN}/i) { print "$ARGV:$.: $_"; $hit = 1 }
      END { exit($hit ? 1 : 0) }' "$f"; then
    :
  else
    status=1
  fi
done

if [[ -f docs/services.md ]]; then
  if PATTERN="$plugin_pattern" perl -ne '
      $in = 1 if /BEGIN GENERATED COVERAGE MATRIX/;
      if (!$in && /$ENV{PATTERN}/i) { print "docs/services.md:$.: $_"; $hit = 1 }
      $in = 0 if /END GENERATED COVERAGE MATRIX/;
      END { exit($hit ? 1 : 0) }' docs/services.md; then
    :
  else
    status=1
  fi
fi

if [[ "$status" -ne 0 ]]; then
  echo ""
  echo "Pinned plugin/service/operation counts go stale. Link to the coverage matrix"
  echo "in docs/services.md, or to the server's /ready endpoint, instead."
  echo "See scripts/check-doc-counts.sh for the rule."
fi
exit "$status"
