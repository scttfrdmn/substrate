#!/usr/bin/env bash
# check-version-stamping.sh — build the substrate binary with the exact -ldflags the
# Dockerfile uses and assert the running server reports that version.
#
# `go build -X` against a symbol that does not exist — a wrong package path, a
# renamed variable — is silently ignored by the linker: no warning, no non-zero
# exit. The stamped variable simply keeps its "dev" default, and nothing notices
# until someone curls /health on a released image. That is exactly what happened
# in #402: the Dockerfile stamped `github.com/scttfrdmn/substrate.Version`, a
# module path with no Go files, so every published image reported "dev" while the
# Makefile-built binary reported the right version.
#
# So this asserts the observable outcome rather than the spelling of the flag: a
# real build, a real server, a real HTTP response. A typo in any -X path fails
# here instead of at the next release.
set -euo pipefail

cd "$(dirname "$0")/.."

SENTINEL="v9.99.99-stamptest"
status=0

fail() {
  echo "check-version-stamping: $*" >&2
  status=1
}

# ---------------------------------------------------------------------------
# 1. The Dockerfile and the Makefile must stamp the same symbols.
#
# They are separate recipes for the same binary, so they drift silently. The
# Makefile is the reference: it is exercised by every local `make build`.
# ---------------------------------------------------------------------------
extract_x_paths() {
  # Print each -X target (the part before '=') from stdin, one per line, sorted.
  grep -oE -- '-X [^= ]+=' | sed -e 's/^-X //' -e 's/=$//' | sort -u
}

docker_ldflags=$(grep -oE -- '-ldflags "[^"]*"' Dockerfile || true)
make_ldflags=$(grep -oE -- '-ldflags "[^"]*"' Makefile || true)

if [[ -z "$docker_ldflags" ]]; then
  fail "no -ldflags found in Dockerfile"
fi
if [[ -z "$make_ldflags" ]]; then
  fail "no -ldflags found in Makefile"
fi

docker_paths=$(extract_x_paths <<<"$docker_ldflags")
make_paths=$(extract_x_paths <<<"$make_ldflags")

if [[ "$docker_paths" != "$make_paths" ]]; then
  fail "Dockerfile and Makefile stamp different symbols:"
  diff <(echo "$make_paths") <(echo "$docker_paths") \
    --label Makefile --label Dockerfile -u >&2 || true
fi

# ---------------------------------------------------------------------------
# 2. Every stamped symbol must actually exist.
#
# Catches the #402 typo directly, and reports which flag is wrong — the runtime
# check below proves something is broken, this says what.
# ---------------------------------------------------------------------------
while IFS= read -r target; do
  [[ -z "$target" ]] && continue
  pkg="${target%.*}"
  sym="${target##*.}"
  if [[ "$pkg" == "main" ]]; then
    # `-X main.Sym` targets the main package of the build, which for every recipe
    # here is ./cmd/substrate. -u because these are conventionally unexported.
    if ! go doc -u ./cmd/substrate "$sym" >/dev/null 2>&1; then
      fail "-X $target: no symbol $sym in ./cmd/substrate"
    fi
  elif ! go doc "$pkg" "$sym" >/dev/null 2>&1; then
    fail "-X $target: no symbol $sym in package $pkg (a -X against a missing symbol is silently ignored)"
  fi
done <<<"$docker_paths"

# ---------------------------------------------------------------------------
# 3. Build with the Dockerfile's flags and assert the server reports the version.
#
# The spelling checks above can only catch mistakes we thought of; this catches
# any reason the version fails to reach a caller.
# ---------------------------------------------------------------------------
tmpdir=$(mktemp -d)
bin="$tmpdir/substrate"
srv_pid=""
# shellcheck disable=SC2329  # invoked by the trap below, not by name.
cleanup() {
  [[ -n "$srv_pid" ]] && kill "$srv_pid" 2>/dev/null
  rm -rf "$tmpdir"
}
trap cleanup EXIT

# Reuse the Dockerfile's flags verbatim, with the sentinel substituted for the
# build arg. Rewriting them here would defeat the purpose.
ldflags=${docker_ldflags#-ldflags \"}
ldflags=${ldflags%\"}
ldflags=${ldflags//\$\{VERSION\}/$SENTINEL}

echo "check-version-stamping: building with -ldflags \"$ldflags\""
go build -ldflags "$ldflags" -o "$bin" ./cmd/substrate

# Bind an ephemeral port by trial: the server takes an --address and does not
# report the port it chose, so :0 gives us nothing to connect to.
port=""
for candidate in $(seq 14566 14600); do
  "$bin" server --address ":$candidate" >"$tmpdir/server.log" 2>&1 &
  srv_pid=$!
  for _ in $(seq 1 40); do
    if curl -fsS "http://127.0.0.1:$candidate/health" >/dev/null 2>&1; then
      port=$candidate
      break
    fi
    kill -0 "$srv_pid" 2>/dev/null || break # died — port in use, try the next
    sleep 0.25
  done
  [[ -n "$port" ]] && break
  kill "$srv_pid" 2>/dev/null
  wait "$srv_pid" 2>/dev/null
  srv_pid=""
done

if [[ -z "$port" ]]; then
  echo "check-version-stamping: server never became healthy; log follows" >&2
  cat "$tmpdir/server.log" >&2
  exit 1
fi

# `substrate --version` reads main.version; the endpoints read emulator.Version.
# Assert both — #402 was invisible for five releases precisely because the CLI
# was right while the endpoints were wrong.
cli_version=$("$bin" --version)
if [[ "$cli_version" != *"$SENTINEL"* ]]; then
  fail "substrate --version reported \"$cli_version\", want it to contain $SENTINEL"
fi

for path in /health /_localstack/health /_localstack/info; do
  body=$(curl -fsS "http://127.0.0.1:$port$path")
  # Parse without jq: this runs on a bare `make` with no assumed tooling. The
  # sentinel is distinctive, so a loose match is safe here.
  got=$(grep -oE '"version"[[:space:]]*:[[:space:]]*"[^"]*"' <<<"$body" | grep -oE '[^"]*"$' | tr -d '"')
  if [[ "$got" != "$SENTINEL" ]]; then
    fail "GET $path reported version \"$got\", want \"$SENTINEL\" — the -X flag is not reaching the variable this endpoint serves"
  fi
done

if [[ "$status" -ne 0 ]]; then
  echo "" >&2
  echo "A -X flag naming a symbol that does not exist links cleanly and leaves the" >&2
  echo "variable at its default. See scripts/check-version-stamping.sh and #402." >&2
else
  echo "check-version-stamping: ok — version $SENTINEL reported by the CLI and all health endpoints"
fi
exit "$status"
