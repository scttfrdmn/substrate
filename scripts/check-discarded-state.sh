#!/usr/bin/env bash
# check-discarded-state.sh — fail if non-test code writes to state and throws the
# error away.
#
# #1192 found 26 sites spelled `_ = p.state.Put(...)`, and #1175 found 165 calls to the
# string-index helpers (updateStringIndex, removeFromStringIndex and their per-plugin
# siblings) whose failures were swallowed inside the helper. MemoryStateManager.Put never
# fails, so no test saw it: against a store that does fail, the operation answered 200
# over a write that never happened, and the resource vanished from its own list or
# outlived its delete. This is the state-write sibling of #1365's discarded-marshal gate
# (scripts/check-discarded-marshal.sh), written the same way and for the same reason: to
# errcheck, `_ =` is an explicit assignment and therefore "handled", so the rule can only
# be stated against the source text.
#
# Discarded state.Get and state.Delete are deliberately not covered: #1192 scoped them
# out, and a gate that fails on today's tree is a gate nobody runs.
#
# If a site genuinely must discard the error, add it to ALLOWED below with the reason, in
# the same shape as check-discarded-marshal.sh's entries.
set -euo pipefail

cd "$(dirname "$0")/.."

# Sites that discard the error deliberately. Each needs a reason, because the reason is
# the thing being reviewed — an allowlist without one is just a suppression. Keyed
# `file:function`, the function enclosing the call, so an entry exempts one reviewed site
# rather than every write in its file.
#
# There are none: every site #1175 and #1192 found returns its error.
ALLOWED=()

# The helpers that write state on a caller's behalf. A bare call to one drops the write's
# error exactly as `_ = p.state.Put(...)` does, so it is held to the same rule.
HELPERS='updateStringIndex|removeFromStringIndex|athenaAppendStringIndex|athenaRemoveStringIndex|removeFromIndex|removeFromList|appendToList|saveImageTagsMap|saveVersionIDs|recordFIFODedup|appendStreamRecord|saveReplay|autoCreateLambdaLogGroup|stubStore'

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
  for a in "${ALLOWED[@]+"${ALLOWED[@]}"}"; do
    if [[ "$key" == "$a" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" -eq 1 ]]; then
    continue
  fi
  if [[ "$found" -eq 0 ]]; then
    echo "A write to state has its error thrown away (#1175, #1192):"
    found=1
  fi
  echo "    $hit   [$key]"
  status=1
# Anchored to the start of the line, as the marshal gate is, so the match is a statement
# rather than a mention of one in a doc comment. The forms are a blank assignment of the
# error (`_ =`, or `_, _ =` for a helper with two results) and a bare call whose result is
# dropped entirely; the call is a Put on any receiver, or one of HELPERS called directly
# or as a method. A call that is checked starts the line with `if`, `return` or an
# assignment to a named variable, none of which match.
done < <(grep -rnE "^[[:space:]]*(_ =|_, _ =)?[[:space:]]*(([A-Za-z_][A-Za-z0-9_]*\.)+Put|([A-Za-z_][A-Za-z0-9_]*\.)?(${HELPERS}))\(" \
  --include='*.go' --exclude='*_test.go' emulator/ cmd/ || true)

if [[ "$status" -ne 0 ]]; then
  cat <<'EOF'

A discarded write answers success over state that was never recorded: a create the
resource's list does not show, a delete that leaves it listed, an update that reads back
as the old value. Check the error and return it wrapped with context,
`fmt.Errorf("<service> <handler>: %w", err)`; the server answers it as InternalFailure
at 500 (see updateStringIndex's doc comment). If the enclosing function cannot return
an error, change its signature rather than dropping the error, as #1192 did for
ELBPlugin.removeFromList. A test seed helper fails its test instead (see
TestServer.SeedSSMParameter).

If a site must discard, add it to ALLOWED in this script with the reason.
EOF
fi
exit "$status"
