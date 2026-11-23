#!/usr/bin/env bash
# check-nondeterminism.sh — fail if emulator code reads the wall clock or a random
# source outside a reviewed allowlist.
#
# Substrate's claim is that the same calls, the same seeds and the same clock give the
# same answers, live and on replay. Every identifier a request mints derives from its
# request ID (ids.go), and every instant a handler renders comes from the simulated clock
# (TimeController). A `time.Now()` or a `rand` draw in a handler breaks both halves at
# once and no test notices, because each run is internally consistent: #1600 found the S3
# multipart UploadId built from the wall clock (so a replayed UploadPart answered
# NoSuchUpload), every Organizations ID drawn from the auto-seeded math/rand, Firehose
# RecordIds that collided on a frozen clock, and an API Gateway proxy event requestId
# that differed between a run and its replay.
#
# The rule can only be stated against the source text: a linter cannot tell a handler's
# clock read from a server's latency measurement. So the gate is lexical, like
# check-discarded-state.sh, and every legitimate site is named below with its reason.
#
# What it refuses, in emulator/ non-test code:
#   - a call to time.Now(;
#   - an import of math/rand, math/rand/v2 or crypto/rand.
# A mention inside a // comment is not a call and is ignored.
#
# If a site is legitimate, add it to ALLOWED with the reason. Prefer fixing it: read the
# clock through the plugin's TimeController (p.tc.Now()) and mint through the request's
# IDMint (reqCtx.IDs).
set -euo pipefail

cd "$(dirname "$0")/.."

# Keyed `file:function`, the top-level function enclosing the call, so an entry exempts
# one reviewed site rather than a whole file. A package-level import is keyed
# `file:import`. Each group states why its sites cannot reach an AWS response.
ALLOWED=(
  # The simulated clock's own wall base: Now() is simulated_baseline + wall elapsed, so
  # the controller is the one place that must read the real clock.
  "emulator/types.go:NewTimeController"
  "emulator/types.go:SetTime"
  "emulator/types.go:Freeze"
  "emulator/types.go:Unfreeze"
  "emulator/types.go:SetScale"
  # A deterministic run starts its clock from the wall clock when no epoch is pinned,
  # which is what every server's clock has always done.
  "emulator/request_ids.go:NewTimeController"

  # The unseeded request ID: a server configured with no run.seed keeps the
  # wall-clock-and-random ID it always assigned (#1598), and the in-process dispatch
  # fallbacks use it for a seedless mint. ParseAWSRequest's placeholders are replaced
  # by the server before any handler runs.
  "emulator/parser.go:generateRequestID"
  "emulator/parser.go:ParseAWSRequest"

  # IDMint's seedless fallback: a hand-built RequestContext — every plugin unit test's,
  # substrate's and a consumer's — has no request ID to derive from (ids.go).
  "emulator/ids.go:import"

  # Fault injection's PRNGs are seeded from fault.seed (default 0), so a probabilistic
  # rule fires on the same requests every run.
  "emulator/fault.go:import"

  # The replay engine's random source is seeded from replay.random_seed and reaches no
  # response; #1611 removes it.
  "emulator/replay.go:import"

  # Bookkeeping no AWS call observes: event and snapshot IDs, replay and recording
  # session IDs and their start times (#1598's documented exceptions). An event ID is
  # unique by its sequence already; the nanoseconds keep two stores' IDs apart.
  "emulator/eventstore.go:generateEventID"
  "emulator/eventstore.go:generateSnapshotID"
  "emulator/replay.go:generateReplayID"
  "emulator/replay.go:StartRecording"
  "emulator/replay.go:Replay"

  # Fallback for a plugin or store built without a time controller — a hand-built
  # PluginConfig in a unit test. The registry always injects one, so on a server these
  # read the simulated clock.
  "emulator/*:Initialize"
  "emulator/*:now"
  "emulator/ce_plugin.go:clampedQueryEnd"

  # Durations, deadlines and caches measured in real time: request latency, the
  # test server's startup probe, the pricing cache's age. None is rendered into an AWS
  # response.
  "emulator/server.go:handleAWSRequest"
  "emulator/control_plane_events.go:recordControlPlaneWrites"
  "emulator/pricing.go:Refresh"
  "emulator/testing.go:awaitTestServer"

  # The Docker executors run a real container and wait on it in real time; #1611 removes
  # them.
  "emulator/lambda_exec.go:*"
  "emulator/rds_exec.go:*"
)

# enclosing_func FILE LINE prints the name of the top-level function whose body holds
# LINE: the last `func` declaration at column 0 before it, or `import` when there is none
# yet. Copied from check-discarded-state.sh.
enclosing_func() {
  awk -v target="$2" '
    BEGIN { name = "import" }
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

# allowed KEY reports whether KEY matches an ALLOWED entry. An entry's file or function
# may be `*`, for the two shapes that are reviewed as a whole: every Initialize fallback,
# and a file whose every site is real-time by nature.
#
# Each entry that matches is noted in USED, so an entry no site matches any more — the
# site was fixed or moved — is reported too: a stale entry would exempt whatever lands in
# that function next, unreviewed.
USED="|"
allowed() {
  local file="${1%%:*}" fn="${1#*:}" a afile afn
  for a in "${ALLOWED[@]}"; do
    afile="${a%%:*}"
    afn="${a#*:}"
    if [[ "$afile" != "emulator/*" && "$afile" != "$file" ]]; then
      continue
    fi
    if [[ "$afn" == "*" || "$afn" == "$fn" ]]; then
      USED="$USED$a|"
      return 0
    fi
  done
  return 1
}

status=0
found=0

while IFS= read -r hit; do
  file="${hit%%:*}"
  rest="${hit#*:}"
  line="${rest%%:*}"
  content="${rest#*:}"
  # The code before any // comment: a doc comment that names time.Now() is not a call.
  code="${content%%//*}"
  if ! grep -qE 'time\.Now\(|"(math/rand|math/rand/v2|crypto/rand)"' <<<"$code"; then
    continue
  fi
  key="$file:$(enclosing_func "$file" "$line")"
  if allowed "$key"; then
    continue
  fi
  if [[ "$found" -eq 0 ]]; then
    echo "The wall clock or a random source is read outside the allowlist (#1600):"
    found=1
  fi
  echo "    $hit   [$key]"
  status=1
done < <(grep -rnE 'time\.Now\(|"(math/rand|math/rand/v2|crypto/rand)"' \
  --include='*.go' --exclude='*_test.go' emulator/ || true)

for a in "${ALLOWED[@]}"; do
  if [[ "$USED" != *"|$a|"* ]]; then
    echo "ALLOWED entry matches no site; remove it: $a"
    status=1
  fi
done

if [[ "$status" -ne 0 ]]; then
  cat <<'EOF'

A wall-clock or random value in a handler makes two runs of the same calls answer
differently, and makes a replay mint a different value from the one the recording
published. Read the clock through the plugin's TimeController (`p.tc.Now()`), and mint
identifiers through the request's IDMint (`reqCtx.IDs.Hex(n)`, `.UUID()`, `.Chars(...)`),
which derives them from the request ID a replay restores.

If a site cannot reach any response — a latency measurement, a cache TTL — add it to
ALLOWED in this script with the reason.
EOF
fi
exit "$status"
