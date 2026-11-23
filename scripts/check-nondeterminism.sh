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
#   - the wall clock: time.Now, called or passed as a value, and the functions that read
#     it — time.Since, Until, After, Tick, NewTimer, NewTicker and AfterFunc;
#   - a random source: an import of math/rand, math/rand/v2 or crypto/rand;
#   - an unseeded identifier: a call to generateRequestID or NewIDMint(""), each of which
#     draws a wall-clock or crypto/rand value a seeded run cannot reproduce.
# A mention inside a // comment or a string literal is not a use and is ignored.
#
# If a site is legitimate, add it to ALLOWED with the reason. Prefer fixing it: read the
# clock through the plugin's TimeController (p.tc.Now()) and mint through the request's
# IDMint (reqCtx.IDs).
#
# `--self-test` runs the line classifier against fixed cases and exits.
set -euo pipefail

cd "$(dirname "$0")/.."

# Keyed `file:function`, the top-level function enclosing the use, so an entry exempts one
# reviewed function rather than a whole file. A package-level import is keyed
# `file:import`. An entry may end `=TEXT`, and then it exempts only a use whose code
# contains TEXT — which is how the two entries reviewed as a pattern rather than a site
# (the Initialize fallbacks, the latency measurements) stay narrow. A file of `emulator/*`
# is allowed only with `=TEXT`.
ALLOWED=(
  # The simulated clock's own wall base: Now() is simulated_baseline + wall elapsed, so
  # the controller is the one place that must read the real clock.
  "emulator/types.go:NewTimeController"
  "emulator/types.go:running"
  "emulator/types.go:SetTime"
  "emulator/types.go:Freeze"
  "emulator/types.go:Unfreeze"
  "emulator/types.go:SetScale"
  # A deterministic run starts its clock from the wall clock when no epoch is pinned,
  # which is what every server's clock has always done.
  "emulator/request_ids.go:NewTimeController"

  # The unseeded request ID: a server configured with no run.seed keeps the
  # wall-clock-and-random ID it always assigned (#1598), and RequestIDSource.Next returns
  # it for an unseeded source. ParseAWSRequest's placeholder is replaced by the server
  # before any handler runs. The two in-process dispatch fallbacks use it only for a
  # seedless mint, where a derived ID would collide two dispatches onto one.
  "emulator/parser.go:generateRequestID"
  "emulator/parser.go:ParseAWSRequest"
  "emulator/request_ids.go:Next"
  "emulator/apigateway_plugin.go:apigwInternalRequestID"
  "emulator/cfn_deployer.go:internalRequestID"

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
  # session IDs and their start times and durations (#1598's documented exceptions). An
  # event ID is unique by its sequence already; the nanoseconds keep two stores' IDs apart.
  "emulator/eventstore.go:generateEventID"
  "emulator/eventstore.go:generateSnapshotID"
  "emulator/replay.go:generateReplayID"
  "emulator/replay.go:StartRecording"
  "emulator/replay.go:StopRecording"
  "emulator/replay.go:Replay"

  # Fallback for a plugin or store built without a time controller — a hand-built
  # PluginConfig in a unit test. The registry always injects one, so on a server these
  # read the simulated clock.
  "emulator/*:Initialize=NewTimeController(time.Now())"
  "emulator/iam_plugin.go:now=return time.Now()"
  "emulator/route53_plugin.go:now=return time.Now()"
  "emulator/sts_plugin.go:now=return time.Now().UTC()"
  "emulator/eventstore.go:now=return time.Now()"
  "emulator/costs.go:now=return time.Now()"
  "emulator/ce_plugin.go:clampedQueryEnd=now := time.Now()"

  # Latency, measured in wall time because it is how long the work took to run: the
  # request's Event.Duration, a recorded control-plane write's, and a CloudFormation
  # dispatch's. None is rendered into an AWS response.
  "emulator/server.go:handleAWSRequest=start := time.Now()"
  "emulator/server.go:handleAWSRequest=time.Since(start)"
  "emulator/control_plane_events.go:recordControlPlaneWrites=start := time.Now()"
  "emulator/control_plane_events.go:recordControlPlaneWrites=time.Since(start)"
  "emulator/cfn_deployer.go:dispatch=wallStart := time.Now()"
  "emulator/cfn_deployer.go:dispatch=time.Since(wallStart)"

  # Real-time waits and caches: the graceful-shutdown wait, the test server's startup
  # probe, the pricing cache's age and TTL.
  "emulator/server.go:awaitStop=time.After(timeout)"
  "emulator/testing.go:awaitTestServer"
  "emulator/pricing.go:Refresh"
  "emulator/pricing.go:CacheAge"
  "emulator/pricing.go:isCacheStale"

  # The Docker executors run a real container and wait on it in real time; #1611 removes
  # them.
  "emulator/lambda_exec.go:*"
  "emulator/rds_exec.go:*"
)

# CANDIDATE is the broad text a use can contain; offending decides.
CANDIDATE='time\.(Now|Since|Until|After|Tick|NewTimer|NewTicker|AfterFunc)|generateRequestID\(|NewIDMint\(""\)|"(math/rand|math/rand/v2|crypto/rand)"'

# strip_code LINE prints LINE with each string literal emptied (to "") and any // comment
# removed. The literals go first, so a // inside one — a URL — does not end the code, and
# a call spelled inside one is not a call.
strip_code() {
  local code
  code="$(sed -E -e 's/"([^"\\]|\\.)*"/""/g' -e 's/`[^`]*`/""/g' <<<"$1")"
  printf '%s\n' "${code%%//*}"
}

# offending LINE succeeds when LINE uses the wall clock, a random source or an unseeded ID.
offending() {
  local line="$1" code
  # An import line is judged on its text, before the literal it names is emptied.
  if grep -qE '^[[:space:]]*(import[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*[[:space:]]+)?"(math/rand|math/rand/v2|crypto/rand)"' <<<"$line"; then
    return 0
  fi
  code="$(strip_code "$line")"
  grep -qE '(^|[^A-Za-z0-9_.])time\.(Now([^A-Za-z0-9_]|$)|(Since|Until|After|Tick|NewTimer|NewTicker|AfterFunc)\()|(^|[^A-Za-z0-9_.])generateRequestID\(|(^|[^A-Za-z0-9_.])NewIDMint\(""\)' <<<"$code"
}

if [[ "${1:-}" == "--self-test" ]]; then
  fail=0
  check() {
    local want="$1" line="$2" got=no
    if offending "$line"; then got=yes; fi
    if [[ "$got" != "$want" ]]; then
      echo "self-test: want offending=$want, got $got: $line"
      fail=1
    fi
  }
  check yes 'x := f("https://example.com", time.Now())'
  check yes 'u := fmt.Sprintf("%s//%s", a, time.Now())'
  check yes '	clock := time.Now'
  check yes '	opts.Now = time.Now'
  check yes '	d := time.Since(start)'
  check yes '	<-time.After(time.Second)'
  check yes '	t := time.NewTicker(d)'
  check yes '	id := generateRequestID()'
  check yes '	m := NewIDMint("")'
  check yes '	"math/rand" // nosemgrep'
  check yes '	mrand "math/rand/v2"'
  check yes 'import "crypto/rand"'
  check no  '	// time.Now() in a comment'
  check no  '	s := "time.Now()"'
  check no  '	raw := `time.Since(x)`'
  check no  '	t := p.tc.Now()'
  check no  '	d := time.Duration(5) * time.Second'
  check no  '	m := NewIDMint(requestID)'
  check no  '	x := runtime.NumGoroutine() // time.Now()'
  check no  '	label := "crypto/rand is not imported here"'
  if [[ "$fail" -eq 0 ]]; then
    echo "check-nondeterminism: self-test ok"
  fi
  exit "$fail"
fi

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

# allowed KEY CODE reports whether an ALLOWED entry exempts the use at KEY whose code is
# CODE. Each entry that does is noted in USED, so an entry no site matches any more — the
# site was fixed or moved — is reported too: a stale entry would exempt whatever lands in
# that function next, unreviewed.
USED="|"
allowed() {
  local file="${1%%:*}" fn="${1#*:}" code="$2" a spec afile afn text
  for a in "${ALLOWED[@]}"; do
    spec="${a%%=*}"
    text=""
    if [[ "$a" == *=* ]]; then
      text="${a#*=}"
    fi
    afile="${spec%%:*}"
    afn="${spec#*:}"
    if [[ "$afile" == "emulator/*" && -z "$text" ]]; then
      continue
    fi
    if [[ "$afile" != "emulator/*" && "$afile" != "$file" ]]; then
      continue
    fi
    if [[ "$afn" != "*" && "$afn" != "$fn" ]]; then
      continue
    fi
    if [[ -n "$text" && "$code" != *"$text"* ]]; then
      continue
    fi
    USED="$USED$a|"
    return 0
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
  if ! offending "$content"; then
    continue
  fi
  key="$file:$(enclosing_func "$file" "$line")"
  if allowed "$key" "$content"; then
    continue
  fi
  if [[ "$found" -eq 0 ]]; then
    echo "The wall clock, a random source or an unseeded ID is used outside the allowlist (#1600):"
    found=1
  fi
  echo "    $hit   [$key]"
  status=1
done < <(grep -rnE "$CANDIDATE" --include='*.go' --exclude='*_test.go' emulator/ || true)

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
