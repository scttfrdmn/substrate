#!/usr/bin/env bash
# check-discarded-unmarshal.sh — fail if a plugin decodes a request body and throws
# the error away.
#
# #1007 found 95 sites spelled `_ = json.Unmarshal(req.Body, &x)`. A discarded
# decode error is worse than a wrong error code, because a wrong code is at least
# an error: a discarded one produces a plausible success from a body the caller
# meant to be read, or a refusal about something else entirely — four SQS
# operations answered QueueDoesNotExist for a JSON syntax error, because the
# zero-valued queue URL went straight into a lookup.
#
# This check exists because `errcheck` cannot express the rule. To errcheck, a
# bare `_ =` and a `//nolint:errcheck` with a written reason are the same
# construct: the assignment is explicit, so the error is "handled". The whole
# point of #1007's third acceptance criterion is to tell those two apart, which
# means matching on the source text rather than on the type system. The same
# reasoning as scripts/check-doc-versions.sh: a rule a linter cannot state is
# still a rule, so state it here and wire it to a make target.
#
# If you are adding a site that genuinely must discard the error, add it to
# ALLOWED below with the reason, in the same shape as the existing entry. A bare
# `_ =` with no entry here is what this script is for.
set -euo pipefail

# Sites that discard the error deliberately. Each needs a reason, because the
# reason is the thing being reviewed — an allowlist without one is just a
# suppression. Keyed `file:line-anchor`, matched as a substring of `file:line`.
#
# sqs_messageattributes.go: the caller has already decoded this body through
# sqsInvalidBody() and refused it if it would not parse, so reaching here means
# the body parses. The second decode narrows to the message-attribute subtree and
# cannot fail for a reason the first decode would not have caught. Refusing twice
# would answer the same code for the same request at two depths.
ALLOWED_FILES=(
  "emulator/sqs_messageattributes.go"
)

status=0
found=0

while IFS= read -r hit; do
  file="${hit%%:*}"
  allowed=0
  for a in "${ALLOWED_FILES[@]}"; do
    if [[ "$file" == "$a" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" -eq 1 ]]; then
    continue
  fi
  if [[ "$found" -eq 0 ]]; then
    echo "A request body is decoded and the error thrown away (#1007):"
    found=1
  fi
  echo "    $hit"
  status=1
# Anchored to the start of the line so the match is a statement rather than any
# mention of one: gofmt puts a Go statement on its own line, and the pattern is
# quoted in prose by the doc comments that explain this rule — including in
# emulator/invalid_body_refusals.go, which this script's own failure message
# points the reader at. A trailing `//nolint` comment still matches, which is the
# point: to errcheck that form is handled, and to this script it is not.
done < <(grep -rnE '^[[:space:]]*_ = json\.Unmarshal\(req\.Body' --include='*.go' emulator/ || true)

if [[ "$status" -ne 0 ]]; then
  cat <<'EOF'

A discarded decode error means a malformed body is not refused at all: the
handler proceeds on a zero-valued struct and answers whatever that implies.
Check the error and return the service's own published refusal — every plugin
that needs one now has a *InvalidBody() constructor carrying its provenance
(see emulator/invalid_body_refusals.go). If the body is optional, keep the
`len(req.Body) > 0` check and refuse only a body that is present: the length
check and the error check answer two different questions.

If a site must discard, add it to ALLOWED_FILES in this script with the reason.
EOF
fi
exit "$status"
