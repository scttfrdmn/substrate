#!/usr/bin/env bash
# check-docs-headings.sh — fail if docs/services.md names a service section twice.
#
# Each `## ` heading in docs/services.md is one service, and the file is hand-edited
# between generated regions, so nothing else notices a section that appears twice.
# #1384 shipped 41 of them: an edit to the Timestream section reinserted a 6,680-line
# copy of every section from Step Functions onward, and docs-reference-check, which
# compares the generated matrix and not the prose, passed. A reader then finds two
# Timestream sections that disagree, and a later edit lands in whichever copy it hits.
set -euo pipefail

cd "$(dirname "$0")/.."

file="docs/services.md"
dupes="$(grep -E '^## ' "$file" | sort | uniq -d)"
if [[ -n "$dupes" ]]; then
  echo "check-docs-headings: $file names these sections more than once:"
  echo "$dupes" | sed 's/^/    /'
  echo "Each service has one section. Merge the copies, keeping the one with the newer content."
  exit 1
fi
echo "check-docs-headings: ok — $(grep -cE '^## ' "$file") sections, each named once"
