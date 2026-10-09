#!/usr/bin/env bash
# Self-test for scripts/check-redaction.sh.
#
# Copies scripts/ into a temporary directory, plants a file containing the first term of the
# public denylist, and asserts the copied scan reports it (exit 1, output mentions
# "REDACTION HIT" and names planted.txt). Then runs the scan on an empty directory and asserts
# it passes (exit 0). Finally runs it with a non-existent root and asserts a usage error (exit 2).
# Cleans up with plain rm -f and rmdir; nothing recursive.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
SCRIPTS_COPY="$WORK/scripts"
FIXTURE="$WORK/planted.txt"
EMPTY="$WORK/empty"
MISSING="$WORK/does-not-exist"

cleanup() {
  rm -f "$FIXTURE" "$SCRIPTS_COPY"/*
  rmdir "$EMPTY" "$SCRIPTS_COPY" "$WORK" 2>/dev/null || true
}
trap cleanup EXIT

mkdir "$SCRIPTS_COPY" "$EMPTY"
cp "$SCRIPT_DIR"/* "$SCRIPTS_COPY"/
SCAN="$SCRIPTS_COPY/check-redaction.sh"

term="$(grep -v -e '^#' -e '^[[:space:]]*$' "$SCRIPT_DIR/redaction-denylist.txt" | head -n 1)"
if [[ -z "$term" ]]; then
  echo "self-test: public denylist has no terms" >&2
  exit 1
fi
printf 'planted line containing %s for the self-test\n' "$term" > "$FIXTURE"

set +e
output="$(REDACTION_EXTRA_DENYLIST= "$SCAN" "$WORK" 2>&1)"
rc=$?
set -e
if [[ $rc -ne 1 ]]; then
  echo "self-test: expected exit 1 for the planted term, got $rc" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ "$output" != *"REDACTION HIT"* ]]; then
  echo "self-test: expected output to contain 'REDACTION HIT'" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
if [[ "$output" != *"planted.txt"* ]]; then
  echo "self-test: expected the hit to be reported from planted.txt" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

set +e
output="$(REDACTION_EXTRA_DENYLIST= "$SCAN" "$EMPTY" 2>&1)"
rc=$?
set -e
if [[ $rc -ne 0 ]]; then
  echo "self-test: expected exit 0 on an empty directory, got $rc" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

set +e
output="$(REDACTION_EXTRA_DENYLIST= "$SCAN" "$MISSING" 2>&1)"
rc=$?
set -e
if [[ $rc -ne 2 ]]; then
  echo "self-test: expected exit 2 for a non-existent root, got $rc" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

echo "Self-test passed."
