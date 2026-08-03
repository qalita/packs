#!/usr/bin/env bash
# Emit the JSON matrix of packs whose test suite CI should run.
#
# Reads changed file paths on stdin (one per line), writes a JSON array of pack
# directory names on stdout. With --all, ignores stdin and selects everything.
#
# A change outside any *_pack/ directory — scripts/, .github/, root config —
# can affect every pack, so it selects all of them. Erring the other way would
# let a change to this very script, or to a shared helper, ship untested.
#
# Packs without a pyproject.toml, or without a tests/test_*.py, are never
# selected: pytest exits 5 on an empty collection, which would fail the job for
# a pack that simply has no suite yet. Removing a pack's last test therefore
# makes it silently unwatched rather than red — scripts/lint_packs.sh is the
# thing that still sees it.
#
# Usable outside CI: `git diff --name-only main | scripts/select_test_matrix.sh`
set -euo pipefail
shopt -s nullglob

cd "$(dirname "$0")/.."

testable() {
  local pack="$1"
  [ -f "$pack/pyproject.toml" ] || return 1
  local matches=("$pack"/tests/test_*.py)
  [ ${#matches[@]} -gt 0 ]
}

all_testable() {
  local dir pack
  for dir in */; do
    pack="${dir%/}"
    if testable "$pack"; then
      echo "$pack"
    fi
  done
}

emit() {
  # jq is present on GitHub runners and in the devcontainer; sort -u keeps the
  # matrix stable so a rerun does not reshuffle job names.
  sort -u | jq -R -s -c 'split("\n") | map(select(length > 0))'
}

if [ "${1:-}" = "--all" ]; then
  all_testable | emit
  exit 0
fi

shared=0
selected=()
while IFS= read -r path; do
  [ -n "$path" ] || continue
  case "$path" in
    *_pack/*) selected+=("${path%%/*}") ;;
    *) shared=1 ;;
  esac
done

if [ "$shared" -eq 1 ]; then
  all_testable | emit
  exit 0
fi

{
  for pack in ${selected[@]+"${selected[@]}"}; do
    if testable "$pack"; then
      echo "$pack"
    fi
  done
} | emit
