#!/usr/bin/env bash
# check-wire-bookkeeping.sh — fail if substrate's own bookkeeping fields gain new
# wire-visible ground.
#
# Substrate persists five fields of its own on the records behind many services:
# AccountID, Region, CreatedAt, UpdatedAt and EverTagged. None of them is a member
# of any AWS shape. They exist so the emulator can scope a record to a caller, age
# it, and know whether it has ever been tagged — they are the emulator's
# bookkeeping, not the service's data.
#
# The defect #756 names is that they carry ordinary json tags, so any handler that
# marshals a persisted record straight into an AWSResponse.Body ships them to the
# caller. A consumer decoding into the real SDK's shape ignores them; a consumer
# asserting on the raw JSON, diffing two responses, or round-tripping a body
# through a schema sees members AWS does not publish. #1090 fixed ECR as the one
# worked instance, and the sharpest case there is instructive: EFS declares
# `CreatedAt` where AWS publishes `CreationTime`, so the leak is not merely an
# extra member but a near-miss of a real one.
#
# ## Why this is a ratchet over declarations and not a reachability check
#
# The rule one actually wants is "no bookkeeping field reaches an
# AWSResponse.Body", and no grep can state that. Deciding it needs to distinguish
# a marshal into a response from a marshal into state.Set, through 500-plus
# bare-identifier json.Marshal(x) calls and the per-plugin response helpers that
# wrap them (ecrJSONResponse and its 60-odd siblings). That is a type-and-dataflow
# question, and substrate depends on neither golang.org/x/tools nor any vendored
# analysis, so a precise answer would mean adding a dependency to serve one check.
#
# So the reachability question was answered once, by observation rather than by
# analysis: every plugin's HandleRequest was temporarily instrumented with a
# deferred hook that walked the response body it was about to return, and the
# suite was run. That pass found 213 wire-visible observations across 31 services
# and 97 operations, of which 194 — across 25 services and 86 operations — are
# members AWS publishes nowhere. It is a floor and not a ceiling, because it sees
# only what the suite exercises, and its findings are recorded on #756 rather than
# here, because a temporary instrumentation is not a thing CI can re-run.
#
# What CI can assert, exactly and cheaply, is the declaration surface: the set of
# bookkeeping fields that are wire-visible at all. That set is the upper bound on
# the leak, it is a pure lexical fact, and it can only shrink. Hence this script.
#
# ## What the baseline is, and why it is not a suppression
#
# scripts/wire-bookkeeping-baseline.txt is not an allowlist of approved sites. It
# is an inventory of known defects. The reason is therefore the same for all of
# them and is stated once, in the file's own header, rather than per line.
#
# That is the opposite of check-discarded-unmarshal.sh's ALLOWED_FILES, where each
# entry is a site that should stay and so has to argue for itself. Here an entry
# is a site that should go, so the check fails in both directions: a field absent
# from the baseline is new leak surface, and a baseline entry that no longer
# matches the tree is a stale record.
#
# ## Why the count of declarations is not the count of leaks
#
# The baseline was written expecting each line to be deleted as its service was
# fixed. It does not work that way, and ECR is the proof: #1090 fixed ECR by the
# route this script recommends, and all four of its lines are still here. They
# have to be. emulator/ecr_wire.go states the reason — `json:"-"` and retyping a
# field in place both change the format of every recorded run, because
# MemoryStateManager snapshots those bytes and a replay reads them back. A fix
# removes the field from the *response*, not from the record, so it leaves the
# declaration standing.
#
# So a fixed service records itself in scripts/wire-bookkeeping-projected.txt
# instead: one line per persisted record that is rendered through a wire struct,
# citing the test that asserts on the raw response bytes that no unpublished
# member appears. That file is where an entry argues for itself, and the number
# #756's AC3 drives to zero is the difference between the two files —
# reported below as "still reachable".
#
# Note that json:"-" is used on zero of the 330 fields. The exclusion is available
# and has never been reached for, and for a field already written to state it is
# not the fix: see above.
set -euo pipefail

cd "$(dirname "$0")/.."

BASELINE="scripts/wire-bookkeeping-baseline.txt"
PROJECTED="scripts/wire-bookkeeping-projected.txt"

# The five field names, matched on the Go identifier rather than on the rendered
# json tag. Keying on the identifier is what catches the abbreviated tags: one
# AccountID renders as `json:"a"` and one Region as `json:"r"`, and a tag-name
# scan would miss both while a field-name scan cannot.
#
# CreatedTime is deliberately absent. Three structs declare it, and both ELB
# generations publish CreatedTime as a real member, so a field of that name is the
# service's data rather than substrate's. (CloudFrontDistribution.CreatedTime is a
# divergence — CloudFront publishes LastModifiedTime — but that is a wrong member,
# not a bookkeeping leak, and belongs to its own issue.)
#
# The same reasoning applies one level finer, per site rather than per name: a
# field is excluded when the operation's own reference publishes a member of that
# name on the shape the struct renders. The BEGIN block below holds those, keyed by
# file, type and Go field so the exclusion cannot spread to another struct by
# accident. It currently holds one:
#
#   - batch_list_jobs.go batchJobSummary.CreatedAt — Batch's JobSummary publishes
#     `"createdAt": number` in ListJobs' own Response Syntax, and that struct is
#     the wire struct this check recommends building (#1090's pattern), carrying
#     only published members. Tagging it `json:"-"` would drop a published member;
#     recording it in the baseline would file a published member as a defect owed a
#     deletion. (emulator/batch_plugin.go's BatchJob.CreatedAt is a separate line,
#     still in the baseline and unchanged here: that struct is the persisted record
#     and its AccountID and Region leak alongside, which is what #756 is about.)
#
# Test files are skipped. A bookkeeping-named field in a _test.go file is a decode
# target — a test reading a member off a response — not wire surface, and one
# exists: organizations_account_test.go's orgCreateStatus.AccountID reads
# CreateAccountStatus.AccountId, which AWS does publish.
#
# One further reason the 330 is an upper bound rather than a leak count: a request
# decode struct is matched too, and cannot leak. Exactly one entry is of that kind
# — accountRegionRequest.AccountID (account_plugin.go:568), which decodes the
# AccountId that Account's own operations publish as an input. There is no lexical
# way to tell a request struct from a response struct, and excluding by a name
# suffix would be a heuristic that silently drops real surface, so it stays in and
# is named here instead.
#
# The awk is written for the POSIX subset because CI's awk is mawk, not gawk:
# matching the struct header on field position rather than on an escaped brace
# avoids the one construct the two disagree about (`\{` is a literal brace to mawk
# and the start of an interval expression to a POSIX-mode gawk). gofmt guarantees
# a top-level declaration begins at column 0 and its closing brace stands alone,
# so `$1 == "type"` and `$0 == "}"` are exact. A nested anonymous struct's brace is
# indented and so does not end the enclosing type, which is what we want: its
# fields belong to the type that contains them.
extract() {
  awk '
    BEGIN {
      # Published members whose Go identifier collides with the five names; see the
      # header. Keyed file, type, field — tab-separated, as the output is.
      published["emulator/batch_list_jobs.go\tbatchJobSummary\tCreatedAt"] = 1
    }
    FILENAME ~ /_test\.go$/ { next }
    $1 == "type" && $3 == "struct" { t = $2; next }
    $0 == "}" { t = ""; next }
    t != "" && $1 ~ /^(AccountID|Region|CreatedAt|UpdatedAt|EverTagged)$/ {
      if ((FILENAME "\t" t "\t" $1) in published) { next }
      if (match($0, /`json:"[^"]*"/)) {
        tag = substr($0, RSTART + 7, RLENGTH - 8)
        if (tag != "-") { print FILENAME "\t" t "\t" $1 "\t" tag }
      }
    }
  ' emulator/*.go | LC_ALL=C sort
}

if [[ ! -f "$BASELINE" ]]; then
  echo "check-wire-bookkeeping: $BASELINE is missing" >&2
  exit 1
fi

if [[ ! -f "$PROJECTED" ]]; then
  echo "check-wire-bookkeeping: $PROJECTED is missing" >&2
  exit 1
fi

current="$(mktemp)"
recorded="$(mktemp)"
trap 'rm -f "$current" "$recorded"' EXIT

extract > "$current"

# --write rewrites the baseline from the tree, keeping the header. It is how a
# deliberate change — a fix that removes fields, or a reviewed addition — records
# itself; it is not something CI ever runs.
if [[ "${1:-}" == "--write" ]]; then
  header="$(grep '^#' "$BASELINE" || true)"
  { [[ -n "$header" ]] && printf '%s\n' "$header"; cat "$current"; } > "$BASELINE.tmp"
  mv "$BASELINE.tmp" "$BASELINE"
  echo "check-wire-bookkeeping: rewrote $BASELINE ($(wc -l < "$current" | tr -d ' ') fields)"
  exit 0
fi

grep -v '^#' "$BASELINE" | grep -v '^[[:space:]]*$' | LC_ALL=C sort > "$recorded"

added="$(comm -23 "$current" "$recorded" || true)"
removed="$(comm -13 "$current" "$recorded" || true)"

status=0

if [[ -n "$added" ]]; then
  status=1
  echo "New wire-visible bookkeeping fields, not in $BASELINE (#756):"
  echo "    ${added//$'\n'/$'\n'    }"
  echo
fi

if [[ -n "$removed" ]]; then
  status=1
  echo "Stale entries in $BASELINE — the tree no longer has these (#756):"
  echo "    ${removed//$'\n'/$'\n'    }"
  echo
fi

# Every projected record must still have bookkeeping fields declared, and must still
# have the test it cites. Both directions matter: the first catches a claim left behind
# by a record that lost its fields, the second stops the third column from decaying
# into a comment when a test is renamed.
declared=0
projected_records=0
while IFS=$'\t' read -r file type test; do
  [[ -z "$file" || "$file" == \#* ]] && continue
  projected_records=$((projected_records + 1))
  fields="$(awk -F'\t' -v f="$file" -v t="$type" '$1 == f && $2 == t' "$recorded" | wc -l | tr -d ' ')"
  if [[ "$fields" -eq 0 ]]; then
    status=1
    echo "$PROJECTED claims $file $type is projected, but $BASELINE records no bookkeeping field"
    echo "    on that type. A projection for a record with no such field is a stale claim (#756)."
    echo
    continue
  fi
  declared=$((declared + fields))
  if ! grep -lq "^func ${test}(" emulator/*_test.go 2>/dev/null; then
    status=1
    echo "$PROJECTED cites ${test} for $file $type, and emulator/ defines no such test."
    echo "    The citation is the only thing holding the projection; rename the entry with the test."
    echo
  fi
done < "$PROJECTED"

# The advice below is about the baseline, so it prints only when the baseline is what
# disagreed. A projected-file failure has already said what to do about itself.
if [[ -n "$added" || -n "$removed" ]]; then
  cat <<'EOF'
A field named AccountID, Region, CreatedAt, UpdatedAt or EverTagged is
substrate's own bookkeeping. No AWS shape publishes any of them, so a handler
that marshals the persisted record straight into an AWSResponse.Body ships a
member the service does not have — and EFS shows the sharp case, where substrate
spells CreatedAt over AWS's published CreationTime.

If you ADDED a field: keep the persisted struct internal and render the response
from a separate wire struct carrying only published members. That is what #1090
did for ECR, and emulator/ecr_wire.go is the pattern. Raw-JSON assertions are the
part that makes such a fix stick; emulator/iam_shape_members_test.go:115 is the
template, and the record then earns a line in
scripts/wire-bookkeeping-projected.txt citing that test.

`json:"-"` is available and is not the fix for a field already written to state:
it changes the format of every recorded run, because MemoryStateManager snapshots
those bytes and a replay reads them back. On a field that has never been
persisted it is fine.

If you REMOVED a field: delete its line from the baseline in the same commit. A
stale line is as much a failure as a new one.

Regenerate with:  ./scripts/check-wire-bookkeeping.sh --write
EOF
fi

if [[ "$status" -eq 0 ]]; then
  total="$(wc -l < "$current" | tr -d ' ')"
  noun="records"
  [[ "$projected_records" -eq 1 ]] && noun="record"
  echo "check-wire-bookkeeping: ok — $total declared, $declared projected across" \
    "$projected_records $noun, $((total - declared)) still reachable (#756)"
fi
exit "$status"
