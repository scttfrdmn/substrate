#!/usr/bin/env bash
# check-tag-releases.sh — fail if a published vX.Y.Z tag has no GitHub Release
# behind it.
#
# Pushing a tag does not create a Release. Nothing asserted otherwise, so v0.84.0
# and v0.85.0 were both cut correctly — signed tags at the right commits, CHANGELOG
# sections in place — and neither appeared on the releases page: `gh release list`
# showed v0.83.0 as Latest with two newer versions tagged and invisible. Anything
# watching releases rather than tags saw nothing ship. Both were published
# retroactively and the step is now documented in CLAUDE.md, but a documented step
# is one a human has to remember, which is what this script replaces.
#
# The inverse direction is not checked. A Release without a tag cannot exist —
# `gh release create` creates the tag when one is absent, which is exactly why
# CLAUDE.md requires `--verify-tag`.
#
# Requires `gh` authenticated against the repo. Skips cleanly when unavailable, so
# a contributor without a token is not blocked; CI always has one.
set -euo pipefail

# CLAUDE.md's procedure pushes the tag (step 3) and publishes the Release (step 4),
# so a just-pushed tag legitimately has no Release for as long as writing the notes
# takes. Without a grace window this check would fail whenever it ran inside that
# gap — and a guard that cries wolf is one people learn to ignore, which is worse
# than not having it. Two hours is far longer than publishing takes and far shorter
# than a forgotten release goes unnoticed.
GRACE_SECONDS=$((2 * 60 * 60))

# Tags below this predate the release convention: 77 of them have no Release and
# never will, so auditing the whole history would report a wall of unactionable
# failures and train everyone to ignore this check. v0.68.0 is the floor because
# every tag from it onward already has a Release — verified when this was written,
# and asserted below so the claim cannot rot silently.
#
# v0.67.0 sits below the floor for an unrelated reason worth knowing: it is the
# void out-of-order tag documented in SECURITY.md. It must never acquire a
# Release, and the floor keeps this check from ever asking for one.
FLOOR_MINOR=68

if ! command -v gh >/dev/null 2>&1; then
  echo "check-tag-releases: gh not installed — skipping."
  exit 0
fi
if ! gh auth status >/dev/null 2>&1; then
  echo "check-tag-releases: gh not authenticated — skipping."
  exit 0
fi

# Tags are read from the remote rather than the local clone: a local checkout may
# be missing tags (a shallow CI clone especially), and a tag that was never pushed
# is not a published release and is none of this check's business.
mapfile -t tags < <(
  git ls-remote --tags origin 'refs/tags/v*' 2>/dev/null |
    sed 's|.*refs/tags/||' |
    grep -vE '\^\{\}$' |
    grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' |
    sort -u
)

if [[ "${#tags[@]}" -eq 0 ]]; then
  echo "check-tag-releases: no version tags found on origin — skipping."
  exit 0
fi

mapfile -t releases < <(gh release list --limit 500 --json tagName --jq '.[].tagName' | sort -u)

has_release() {
  local needle="$1" r
  for r in "${releases[@]}"; do
    [[ "$r" == "$needle" ]] && return 0
  done
  return 1
}

# above_floor reports whether a tag is at or above the floor, comparing major then
# minor numerically. A string comparison would place v0.9.0 above v0.68.0.
above_floor() {
  local v="${1#v}" major minor rest
  major="${v%%.*}"
  rest="${v#*.}"
  minor="${rest%%.*}"
  [[ "$major" -gt 0 ]] && return 0
  [[ "$minor" -ge "$FLOOR_MINOR" ]]
}

# within_grace reports whether a tag's commit is recent enough that its Release may
# still be in progress. The commit date is used rather than the tag's own creation
# time because an annotated tag's date is not exposed by ls-remote, and a release
# tag points at a merge commit made minutes earlier — close enough for a window
# measured in hours, and it fails safe: an old commit tagged today is reported
# immediately rather than being granted a window it does not need.
now=$(date +%s)
within_grace() {
  local tag="$1" ts
  ts=$(git log -1 --format=%ct "refs/tags/$tag" 2>/dev/null) || return 1
  [[ -n "$ts" ]] || return 1
  (( now - ts < GRACE_SECONDS ))
}

missing=()
pending=()
legacy_missing=0
checked=0
for t in "${tags[@]}"; do
  if above_floor "$t"; then
    checked=$((checked + 1))
    has_release "$t" && continue
    if within_grace "$t"; then
      pending+=("$t")
    else
      missing+=("$t")
    fi
  elif ! has_release "$t"; then
    legacy_missing=$((legacy_missing + 1))
  fi
done

for t in "${pending[@]}"; do
  echo "check-tag-releases: ${t} has no Release yet, but was tagged within the grace window — not failing."
done

if [[ "${#missing[@]}" -gt 0 ]]; then
  echo "Tags with no GitHub Release:"
  for t in "${missing[@]}"; do
    echo "    $t"
  done
  cat <<'EOF'

A pushed tag does not create a Release, and consumers watching releases rather
than tags see nothing ship. Publish against the existing tag:

    gh release create vX.Y.Z --verify-tag --latest --title vX.Y.Z --notes-file <file>

Always pass --verify-tag: without it, gh release create *creates* the tag at the
current branch tip, unsigned, when the named tag is absent. See CLAUDE.md's
release procedure for the notes house style.
EOF
  exit 1
fi

# The floor's own premise is asserted rather than trusted: the tag immediately below
# it must still be one of the legacy tags with no Release. If v0.67.0 ever acquires
# one, the floor was drawn in the wrong place — or, worse, someone published a
# Release for the void tag SECURITY.md says never to use.
#
# An earlier version of this check compared legacy_missing against a hardcoded 77.
# That was wrong in a way worth recording: the count depends on the full tag history
# being present, so a shallow clone or a partial fetch failed the check for a reason
# that had nothing to do with releases. This assertion tests the one fact the floor
# actually rests on.
if has_release "v0.$((FLOOR_MINOR - 1)).0"; then
  echo "check-tag-releases: v0.$((FLOOR_MINOR - 1)).0 has a Release, so the v0.${FLOOR_MINOR}.0 floor is misplaced."
  echo "If that tag is the void v0.67.0 (see SECURITY.md), the Release must be deleted;"
  echo "otherwise lower FLOOR_MINOR in scripts/check-tag-releases.sh."
  exit 1
fi

echo "check-tag-releases: all ${checked} version tags at or above v0.${FLOOR_MINOR}.0 have a Release (${legacy_missing} older tags exempt)."
