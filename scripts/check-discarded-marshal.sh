#!/usr/bin/env bash
# check-discarded-marshal.sh — fail if non-test code encodes a value and throws the
# error away.
#
# #1365 found 142 sites spelled `x, _ := json.Marshal(v)` (and one `xml.Marshal`).
# Most of them fed state.Put, so a failed encode wrote an empty record and the
# operation answered 200 over state it never recorded. CLAUDE.md's rule is "errors:
# never discard", and this is the encode-side sibling of #1007's discarded-unmarshal
# gate (scripts/check-discarded-unmarshal.sh), written the same way and for the same
# reason: to errcheck, `x, _ :=` is an explicit assignment and therefore "handled",
# so the rule can only be stated against the source text.
#
# If a site genuinely must discard the error, add it to ALLOWED below with the
# reason, in the same shape as the existing entries. A bare `, _ :=` on a marshal with
# no entry here is what this script is for.
set -euo pipefail

cd "$(dirname "$0")/.."

# Sites that discard the error deliberately. Each needs a reason, because the reason is
# the thing being reviewed — an allowlist without one is just a suppression. Keyed
# `file:function`, the function enclosing the marshal, so an entry exempts one reviewed
# site rather than every marshal in its file.
#
# The eight string-index helpers #1365 listed here for #1175 now return their marshal
# errors with the rest of their failures, so they need no entry.
#
# openSearchError marshals a literal map of strings and an int; s3ErrorResponseWith
# marshals an s3ErrorXML of strings with element names taken from non-empty detail
# names, which encoding/xml cannot fail to encode. Both return a bare *AWSResponse to
# error paths that have no error of their own to return.
ALLOWED=(
  "emulator/opensearch_plugin.go:openSearchError"
  "emulator/s3_plugin.go:s3ErrorResponseWith"
)

# enclosing_func FILE LINE prints the name of the top-level function whose body holds
# LINE: the last `func` declaration at column 0 before it. gofmt puts every top-level
# declaration at column 0, so this is exact for a function, and a closure inside one
# reports its enclosing declaration, which is the unit an entry reviews.
enclosing_func() {
  awk -v target="$2" '
    NR > target { exit }
    /^func / {
      line = $0
      sub(/^func (\([^)]*\) )?/, "", line)
      sub(/[(\[].*/, "", line)
      name = line
    }
    END { print name }
  ' "$1"
}

status=0
found=0

while IFS= read -r hit; do
  file="${hit%%:*}"
  rest="${hit#*:}"
  line="${rest%%:*}"
  key="$file:$(enclosing_func "$file" "$line")"
  allowed=0
  for a in "${ALLOWED[@]}"; do
    if [[ "$key" == "$a" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" -eq 1 ]]; then
    continue
  fi
  if [[ "$found" -eq 0 ]]; then
    echo "A value is encoded and the error thrown away (#1365):"
    found=1
  fi
  echo "    $hit   [$key]"
  status=1
# Anchored to the start of the line, as the unmarshal gate is, so the match is a
# statement rather than a mention of one in a doc comment. The three forms are an
# assignment discarding the error (`x, _ :=` or `x, _ =`), a blank assignment of both
# results (`_, _ =`), and a bare call whose results are dropped entirely.
done < <(grep -rnE '^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*, _ :?=|_, _ =|_ =)?[[:space:]]*(json|xml)\.(Marshal|MarshalIndent)\(' \
  --include='*.go' --exclude='*_test.go' emulator/ cmd/ || true)

if [[ "$status" -ne 0 ]]; then
  cat <<'EOF'

A discarded encode error writes or answers whatever a nil slice implies: an empty
record stored over the real one, or an empty body under a 200. Check the error and
return it wrapped with context, `fmt.Errorf("<plugin> <op> marshal: %w", err)`. If
the enclosing function cannot return an error, change its signature rather than
dropping the error, as #1365 did for openSearchStatusOK and the EFS mount-target
counters.

If a site must discard because the value provably cannot fail to encode, add it to
ALLOWED in this script with the reason.
EOF
fi
exit "$status"
