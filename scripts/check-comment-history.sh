#!/usr/bin/env bash
# check-comment-history.sh — fail if a change adds a Go comment that narrates issue
# history ("before #123", "until #456", "it used to return …", "before this fix").
#
# A comment describes the code as it is. What the code did before a change belongs in
# git, the PR and the CHANGELOG: a narration goes stale the moment the next change lands,
# and the tree had about 1,100 such lines when the rule was written (#1609). A trailing
# `(#N)` pointing at the reasoning, an AWS source citation and `TODO(#N)` are all fine,
# because none of them claims what the code used to do.
#
# Scope: only lines the change *adds* to a `*.go` file, read from
#   git diff -U0 "${BASE:-origin/main}"...HEAD -- '*.go'
# so the existing narrations do not fail every PR; they are trimmed in batches instead.
# Of those lines only the comment text is checked: a whole-line `//` comment, or the
# part of a line after a `//` that is outside a string literal. Lines inside a `/* */`
# block are not seen, which is acceptable because the emulator's comments are `//`.
#
# The pattern is deliberately narrow, so the gate refuses narration and little else:
#   - "before|until|since|after #N" and "pre-#N" (an issue number as a point in time);
#   - "used to be|treat|report|answer|return|refuse|accept|drop|mint|store", unless it
#     is the passive "is/are/was/were/be/been/being used to" or the participle
#     ", used to" — "the key is used to return" is ordinary English;
#   - "before this fix" and "before the fix".
# Words such as "previously-tagged" (a resource state), "no longer" (often about AWS:
# "AWS no longer publishes …") and "previously" are not matched at all.
#
# A record persisted by an older build is a fact about the data the code must still
# read, not narration, so "recorded|written|persisted|stored|minted|registered|exported|
# saved before #N" and "a record|recording|stream from before #N" are allowed. Only
# "before" is: "written since #N" or "stored until #N" says what the code did, and a
# generic verb ("made", "created") says nothing about persisted data. For any other
# legitimate case, put `history-ok:` in the same comment with the reason; use it rarely.
#
# A `…` raw string can span lines, so whether a line starts inside one is carried from
# line to line within a diff hunk: a `//` inside a raw string is not a comment.
#
# `--self-test` runs the matcher over known-bad and known-good lines and exits.
set -euo pipefail

cd "$(dirname "$0")/.."

# The matcher, shared by the scan and the self-test. Reads a unified diff on stdin
# (MODE=diff) or bare source lines (MODE=lines), and prints each offending added line
# as file:line: text. Exits 1 if any line offends.
read -r -d '' matcher <<'PERL' || true
use strict;
use warnings;
my $mode = $ENV{MODE} // 'diff';

# The comment text of one line of Go, or undef: the part after the first `//` that is
# outside a "…", `…` or '…' literal. $raw is a reference to whether the line starts inside
# a `…` raw string; it is updated to whether the line ends inside one.
sub comment_of {
  my ($s, $raw) = @_;
  my $q = $$raw ? '`' : '';
  $$raw = 0;
  for (my $i = 0; $i < length $s; $i++) {
    my $c = substr($s, $i, 1);
    if ($q ne '') {
      if ($c eq '\\' && $q ne '`') { $i++; next }
      $q = '' if $c eq $q;
      next;
    }
    if ($c eq '"' || $c eq '`' || $c eq "'") { $q = $c; next }
    return substr($s, $i + 2) if substr($s, $i, 2) eq '//';
  }
  $$raw = 1 if $q eq '`';
  return undef;
}

my $verbs = qr/(?:be|treat|report|answer|return|refuse|accept|drop|mint|store)/i;
my $data  = qr/(?:recorded|written|persisted|stored|minted|registered|exported|saved|(?:record|recording|stream)s? from)/i;
sub offends {
  my ($c) = @_;
  return 0 if $c =~ /history-ok:/;
  while ($c =~ /(?<![\w-])(before|until|since|after) ?#\d+/gi) {
    next if lc($1) eq 'before' && substr($c, 0, $-[0]) =~ /\b$data\s+$/;
    return 1;
  }
  return 1 if $c =~ /(?<![\w-])pre- ?#\d+/i;
  return 1 if $c =~ /\bbefore (?:this|the) fix\b/i;
  while ($c =~ /\bused to $verbs\b/gi) {
    my $pre = substr($c, 0, $-[0]);
    next if $pre =~ /(?:\b(?:is|are|was|were|be|been|being|get|gets|got)|'s|,)\s*$/i;
    return 1;
  }
  return 0;
}

my ($file, $line, $hit, $raw) = ('', 0, 0, 0);
while (my $l = <STDIN>) {
  chomp $l;
  if ($mode eq 'lines') {
    $line++;
    my $c = comment_of($l, \$raw);
    if (defined $c && offends($c)) { print "self-test:$line: $l\n"; $hit = 1 }
    next;
  }
  if ($l =~ m{^\+\+\+ (?:b/)?(.*)$}) { ($file, $raw) = ($1, 0); next }
  if ($l =~ /^@@ -\S+ \+(\d+)/) { ($line, $raw) = ($1, 0); next }
  next unless $l =~ /^\+(.*)$/s;
  my $src = $1;
  my $c = comment_of($src, \$raw);
  if (defined $c && offends($c)) { print "$file:$line: $src\n"; $hit = 1 }
  $line++;
}
exit($hit ? 1 : 0);
PERL

if [[ "${1:-}" == "--self-test" ]]; then
  status=0
  check() { # check <want: bad|good> <line>
    local got
    if printf '%s\n' "$2" | MODE=lines perl -e "$matcher" >/dev/null; then got=good; else got=bad; fi
    if [[ "$got" != "$1" ]]; then
      echo "self-test: want $1, got $got: $2"
      status=1
    fi
  }
  # Known-bad: narration in each form the tree has used.
  check bad '	// Before #913 the Query family answered a bare 400.'
  check bad '	// The key was the bare name until #943, so two accounts collided.'
  check bad '	// ?encryption is routed since #493.'
  check bad '	// This comment claimed all five asserted existence until #731.'
  check bad '	// The pre-#771 fallback reads the registry.'
  check bad '	// after #1199 the two dates are present'
  check bad '	// It used to answer 200 whatever the name.'
  check bad '	// Substrate used to mint an ID from the clock.'
  check bad '	// a delete used to answer 200 for any name (#1162).'
  check bad '	// Page size 1 is the sharpest case, since before the fix every page was empty.'
  check bad '	// before this fix the list was unsorted'
  check bad '	x := 1 // until #12 this was 2'
  check bad '	s := "a//b" // used to be "a/b"'
  check bad '	// The range is canonical as written since #1553.'
  check bad '	// The bare name was stored until #943.'
  check bad '	// A topic created after #1200 carries a policy.'
  check bad '	// by a recording made before #1210'
  check bad $'\tx := `a\nb` // until #3 it was c'
  # Known-good: present tense, citations, and the English the pattern must not hit.
  check good '	// A previously-tagged resource keeps its tag set (#708).'
  check good '	// The key is used to return the cached page.'
  check good '	// Both names are used to store the record.'
  check good '	// cfgsvcRecordingStrategies is the RecordingStrategyType enum, used to refuse a bad value.'
  check good '	// AWS no longer publishes this field.'
  check good '	// Answers 404 for an absent zone (#1123).'
  check good '	// TODO(#1611): drop the fallback.'
  check good '	// API_CreateTopic publishes Attributes.entry.N (see #993 for the reasoning).'
  check good '	// A stream recorded before #1601 carries no requestId.'
  check good '	// A group written before #1553 holds each range as sent.'
  check good '	// Absent from a record persisted before #1071.'
  check good '	// Empty on a record from before #1196, which accepts any version.'
  check good '	// any AMI registered before #711 has a root device'
  check good '	// The 403 reading stands until #1700 decides it. history-ok: pending decision.'
  check good '	msg := "used to be // before #12"'
  check good '	re := `until #4` // the literal is test data'
  check good '	// The used-to-be-default path is gone.'
  check good '	// Use it before #N style references are added.'
  check good $'\tre := `\n// before #12 is test data inside a raw string\n`'
  [[ "$status" -eq 0 ]] && echo "check-comment-history self-test: ok"
  exit "$status"
fi

base="${BASE:-origin/main}"
if ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
  echo "check-comment-history: base ref '$base' not found; fetch it or set BASE" >&2
  exit 2
fi

if ! diff=$(git diff -U0 -M --no-color --no-ext-diff "$base"...HEAD -- '*.go'); then
  echo "check-comment-history: git diff $base...HEAD failed; is the base fetched with full history?" >&2
  exit 2
fi

status=0
printf '%s\n' "$diff" | MODE=diff perl -e "$matcher" || status=1

if [[ "$status" -ne 0 ]]; then
  echo ""
  echo "These added comments narrate history. Say what the code does now and why;"
  echo "history goes in git, the PR and the CHANGELOG. A trailing (#N), an AWS"
  echo "citation and TODO(#N) are fine; mark a rare legitimate case with history-ok:."
  echo "See scripts/check-comment-history.sh for the rule."
fi
exit "$status"
