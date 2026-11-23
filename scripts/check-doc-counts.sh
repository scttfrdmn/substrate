#!/usr/bin/env bash
# check-doc-counts.sh — fail if the README or docs prose pins a plugin, service or
# operation count ("63 plugins", "1,105-operation catalog", "sixty-eight built-in
# service plugins"). Pinned counts go stale silently: the README and the docs home
# page said 63 plugins while the registry had 68 (#1604). The one place a count may
# appear is the coverage matrix in docs/services.md, between its BEGIN/END GENERATED
# COVERAGE MATRIX markers, which `make docs-reference` renders from the registry and
# `make docs-reference-check` keeps in sync. Link to that matrix, or to the server's
# /ready endpoint, instead.
#
# Scope:
#   - README.md, python/README.md and docs/**/*.md other than docs/services.md: any
#     plugin, service or operation count.
#   - docs/services.md outside the matrix: any plugin or service count, and an
#     operation count of 1,000 or more, which can only be a total. A per-service
#     section legitimately says how many operations that one service routes, and no
#     AWS service has a thousand.
#
# What counts as a count: a number (digits, comma-grouped or not, or a spelled-out
# number of twenty or more, which is how a total is written out), then up to five
# modifier words, then the noun, with spaces or hyphens between ("1,105-operation").
# A word such as "the", "of" or "in" between them breaks the phrase, so "returns
# 400 for the other services" is not a count. Spelled-out numbers below twenty are
# allowed: "two services" in prose is almost always a specific case, not a total.
#
# `check-doc-counts.sh --self-test` runs the matcher over known-bad and known-good
# lines and fails if it misjudges any.
set -euo pipefail

cd "$(dirname "$0")/.."

# The matcher, shared by the scan and the self-test. Reads lines on stdin with
# $ENV{FILE} as the label and $ENV{MODE} as "all" or "services-md". In
# services-md mode, lines between the matrix markers are skipped. Exits 1 if any
# line matches.
read -r -d '' matcher <<'PERL' || true
use strict;
use warnings;
my $mode  = $ENV{MODE} // 'all';
my $tens  = qr/(?:twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety)(?:-(?:one|two|three|four|five|six|seven|eight|nine))?/i;
my $big   = qr/(?:hundred|thousand)/i;
my $spelt = qr/(?:(?:(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|$tens)[\s-]+)?$big(?:[\s-]+(?:and[\s-]+)?$tens)?|$tens)/i;
my $digits = qr/(?<![\w.])(?<!Route )\d[\d,]*/;   # "Route 53" is a name, not a count
# Spelled-out numbers only in "all" mode: services.md's hand-written sections use
# them for counts scoped to one feature ("across twenty-one services").
my $num   = $mode eq 'all' ? qr/(?:$digits|(?<![\w-])$spelt)/ : $digits;
my $stop  = qr/(?:the|a|an|of|in|on|for|to|and|or|with|from|by|at|as|that|which|is|are|was|were|be|its|their|these|those|this|other|each|every|per|than|across|into|over|under|between|most|many|some|all|both|any|more|fewer|returns?|answers?)(?![\w-])/i;
my $word  = qr/(?!$stop)[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*/;   # "built-in" is one word
my $mods  = qr/(?:[\s-]+\**$word\**){0,5}?/;
# A plural noun after whitespace or a hyphen, or a singular one only when hyphenated
# onto the count ("1,105-operation catalog"), since "Route 53 plugin" or "the 1
# service" is not a total.
my $plural   = $mode eq 'all' ? qr/(?:plugins|services|operations)/i : qr/(?:plugins|services)/i;
my $singular = $mode eq 'all' ? qr/(?:plugin|service|operation)/i  : qr/(?:plugin|service)/i;
my $count = qr/$num\**$mods(?:[\s-]+\**$plural|-$singular)(?![\w-])/;
my $total_ops = qr/(?<![\w.])(\d[\d,]*)\**$mods(?:[\s-]+\**operations|-operation)(?![\w-])/i;

my @lines = <STDIN>;
my @skip  = (0) x @lines;
if ($mode eq 'services-md') {
  my $in = 0;
  for my $i (0 .. $#lines) {
    $in = 1 if $lines[$i] =~ /BEGIN GENERATED COVERAGE MATRIX/;
    $skip[$i] = $in;
    $in = 0 if $lines[$i] =~ /END GENERATED COVERAGE MATRIX/;
  }
}
my $hit = 0;
for my $i (0 .. $#lines) {
  next if $skip[$i];
  # Join the next line so a count wrapped across two lines ("all 63 service" /
  # "plugins") is seen whole; only a match that starts on this line is reported.
  (my $cur = $lines[$i]) =~ s/\s+\z//;
  my $next = ($i < $#lines && !$skip[$i + 1]) ? $lines[$i + 1] : '';
  my $text = "$cur $next";
  my $bad = 0;
  while ($text =~ /$count/g) {
    if ($-[0] < length $cur) { $bad = 1; last }
  }
  if (!$bad && $mode eq 'services-md') {
    while ($text =~ /$total_ops/g) {
      (my $n = $1) =~ s/,//g;
      if ($-[0] < length $cur && $n >= 1000) { $bad = 1; last }
    }
  }
  if ($bad) { print "$ENV{FILE}:", $i + 1, ": $lines[$i]"; $hit = 1 }
}
exit($hit ? 1 : 0);
PERL

if [[ "${1:-}" == "--self-test" ]]; then
  status=0
  check() { # check <mode> <want: bad|good> <line>
    local got
    if printf '%s\n' "$3" | FILE=self-test MODE="$1" perl -e "$matcher" >/dev/null; then got=good; else got=bad; fi
    if [[ "$got" != "$2" ]]; then
      echo "self-test: [$1] want $2, got $got: $3"
      status=1
    fi
  }
  # Known-bad: every form a total count has taken or could take.
  check all bad 'Substrate ships **63 built-in service plugins** spanning compute'
  check all bad '- [Service Reference](services.md) — all 63 plugins with operation lists'
  check all bad 'Substrate ships 68 plugins routing 1,105 operations.'
  check all bad 'the 1,105-operation catalog'
  check all bad 'a 68-plugin registry'
  check all bad '63 built-in AWS service plugins'
  check all bad 'sixty-eight plugins'
  check all bad 'Sixty-eight built-in service plugins'
  check all bad 'over one hundred services'
  check all bad 'It covers 164 services.'
  check all bad 'about 1105 routed operations'
  check all bad $'registers all 63 service\nplugins, and schedules t.Cleanup'
  check services-md bad 'Substrate now has 70 plugins.'
  check services-md bad 'guarded operations in 42 services'
  check services-md bad 'the catalog routes 1,105 operations in total'
  check services-md bad 'a 1,105-operation catalog'
  # Known-good: what the prose legitimately says.
  check all good 'returns 400 for the other services'
  check all good 'the 400 the JSON-RPC services in this document use'
  check all good 'Two plugins route no operations'
  check all good 'both services agree'
  check all good 'see the [coverage matrix](docs/services.md#coverage-matrix)'
  check all good 'version 0.1.0 of the plugin'
  check all good 'The Route 53 plugin is deliberately unchanged'
  check all good 'not the 403 most services use'
  check all good 'an in-process server registers every plugin'
  check services-md good 'Forty CFN resource types are stamped, across twenty-one services'
  check services-md good '**All 97 operations the EC2 plugin routes have a row below.**'
  check services-md good '| Any of the other 131 operations | `InvalidParameterValue` | 400 |'
  check services-md good 'Ten of the API'"'"'s 141 operations are routed.'
  check services-md good '<!-- BEGIN GENERATED COVERAGE MATRIX -->'
  [[ "$status" -eq 0 ]] && echo "check-doc-counts self-test: ok"
  exit "$status"
fi

files=()
while IFS= read -r f; do
  files+=("$f")
done < <(
  {
    echo "README.md"
    echo "python/README.md"
    find docs -name '*.md' \
      -not -path '*/node_modules/*' \
      -not -path '*/.vitepress/dist/*' \
      -not -path '*/.vitepress/cache/*' \
      -not -path 'docs/services.md'
  } | sort -u
)

status=0
for f in "${files[@]}"; do
  [[ -f "$f" ]] || continue
  FILE="$f" MODE=all perl -e "$matcher" <"$f" || status=1
done
if [[ -f docs/services.md ]]; then
  FILE=docs/services.md MODE=services-md perl -e "$matcher" <docs/services.md || status=1
fi

if [[ "$status" -ne 0 ]]; then
  echo ""
  echo "Pinned plugin/service/operation counts go stale. Link to the coverage matrix"
  echo "in docs/services.md, or to the server's /ready endpoint, instead."
  echo "See scripts/check-doc-counts.sh for the rule."
fi
exit "$status"
