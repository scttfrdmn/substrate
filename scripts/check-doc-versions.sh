#!/usr/bin/env bash
# check-doc-versions.sh — fail if docs or the README pin a specific vX.Y.Z release
# in prose. Pinned versions go stale silently (this is what let the docs advertise
# v0.68.0 while the repo was at v0.75.0; see #363/#365). Use a neutral placeholder
# such as `v0.x.y` or read the version from the release tag at build time instead.
#
# Legitimate version references are allowed: comparison/diff links, release-tag
# URLs, blob/tree URLs, the CHANGELOG, and module-path references.
set -euo pipefail

# Files to scan: README plus all Markdown under docs/, excluding the changelog
# and build artifacts (node_modules, VitePress output).
mapfile -t files < <(
  {
    echo "README.md"
    find docs -name '*.md' \
      -not -path '*/node_modules/*' \
      -not -path '*/.vitepress/dist/*' \
      -not -path '*/.vitepress/cache/*'
  } | sort -u
)

status=0
for f in "${files[@]}"; do
  [[ "$f" == "CHANGELOG.md" ]] && continue
  [[ -f "$f" ]] || continue
  # Match vMAJOR.MINOR.PATCH, then drop lines where the match is part of a URL or
  # an allowed context.
  while IFS= read -r line; do
    hits=$(grep -nE 'v[0-9]+\.[0-9]+\.[0-9]+' <<<"$line" || true)
    [[ -z "$hits" ]] && continue
    # Allowed contexts: URLs (github.com, compare, releases/tag, blob, tree),
    # go module paths, and the explicit x.y placeholder.
    if grep -qE 'github\.com|/compare/|releases/tag|/blob/|/tree/|go [0-9]|toolchain|x\.y' <<<"$line"; then
      continue
    fi
    echo "$f: pinned version in prose — use a neutral placeholder (e.g. v0.x.y):"
    echo "    $line"
    status=1
  done < <(grep -E 'v[0-9]+\.[0-9]+\.[0-9]+' "$f" || true)
done

if [[ "$status" -ne 0 ]]; then
  echo ""
  echo "Pinned versions go stale. See scripts/check-doc-versions.sh for the rule."
fi
exit "$status"
