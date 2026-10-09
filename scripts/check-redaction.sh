#!/usr/bin/env bash
# Fails (exit 1) if any denylisted term appears anywhere in the repository.
#
# Usage:
#   scripts/check-redaction.sh [ROOT]
#   REDACTION_EXTRA_DENYLIST=/path/to/private-list.txt scripts/check-redaction.sh
#   REDACTION_IGNORE_PATTERNS=/path/to/private-ignore.txt scripts/check-redaction.sh
#
# The public list lives in scripts/redaction-denylist.txt and holds only generic credential
# markers. Organisation-specific vocabulary belongs in a second, private list supplied via
# REDACTION_EXTRA_DENYLIST so that the public list never discloses what it protects.
# REDACTION_IGNORE_PATTERNS optionally names a file of extended regular expressions (one per
# line) that are blanked out of each matching line before the term is re-checked; use it for
# known false positives such as a library identifier that happens to contain a private term.
# An optional first argument overrides the directory to scan (default: the repository root);
# the denylist is always read from this script's own directory.
# Exit codes: 0 clean, 1 at least one hit, 2 usage or grep error.
# Binary files are skipped (grep -I). Short terms may match inside identifiers; when that
# happens, prefer renaming the identifier over weakening the list.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -ge 1 && ! -d "$1" ]]; then
  echo "not a directory: $1" >&2
  exit 2
fi
ROOT="$(cd "${1:-$SCRIPT_DIR/..}" && pwd)"
DENYLIST="$SCRIPT_DIR/redaction-denylist.txt"
EXTRA="${REDACTION_EXTRA_DENYLIST:-}"
IGNORE="${REDACTION_IGNORE_PATTERNS:-}"
status=0

# Blanks every ignore pattern out of stdin. A no-op when no ignore file is configured.
apply_ignores() {
  if [[ -z "$IGNORE" ]]; then
    cat
    return
  fi
  local expr=""
  local pattern
  while IFS= read -r pattern || [[ -n "$pattern" ]]; do
    pattern="${pattern%%$'\r'}"
    [[ -z "$pattern" || "$pattern" == \#* ]] && continue
    expr+="s/${pattern//\//\\/}/ /g;"
  done < "$IGNORE"
  if [[ -z "$expr" ]]; then
    cat
  else
    sed -E "$expr"
  fi
}

check_list() {
  local list="$1"
  local term raw hits rc
  while IFS= read -r term || [[ -n "$term" ]]; do
    term="${term%%$'\r'}"
    [[ -z "$term" || "$term" == \#* ]] && continue
    # grep runs from inside the root so reported paths are relative (./path:line:text) and
    # the checkout location can never cause a hit. The scan excludes its own inputs.
    set +e
    raw=$(cd "$ROOT" && grep -rIin --fixed-strings -e "$term" . \
        --exclude-dir=.git --exclude-dir=target --exclude-dir=node_modules \
        --exclude=redaction-denylist.txt --exclude=check-redaction.sh --exclude='*.min.js')
    rc=$?
    set -e
    if (( rc >= 2 )); then
      echo "grep failed (rc=$rc) for '$term'" >&2
      exit 2
    fi
    if (( rc == 1 )); then
      continue
    fi
    # Re-check after blanking known false positives; a line is reported only if the term
    # survives without them.
    hits=$(printf '%s\n' "$raw" | apply_ignores | grep -iF -e "$term" || true)
    if [[ -n "$hits" ]]; then
      echo "REDACTION HIT for '$term':"
      head -n 20 <<<"$hits"
      echo "  ($(printf '%s\n' "$hits" | wc -l | tr -d ' ') line(s) total)"
      status=1
    fi
  done < "$list"
}

if [[ -n "$IGNORE" && ! -f "$IGNORE" ]]; then
  echo "REDACTION_IGNORE_PATTERNS points to a missing file: $IGNORE" >&2
  exit 2
fi

check_list "$DENYLIST"
if [[ -n "$EXTRA" ]]; then
  if [[ ! -f "$EXTRA" ]]; then
    echo "REDACTION_EXTRA_DENYLIST points to a missing file: $EXTRA" >&2
    exit 2
  fi
  check_list "$EXTRA"
fi

if [[ $status -eq 0 ]]; then
  echo "Redaction check passed."
fi
exit $status
