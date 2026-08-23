#!/usr/bin/env bash
# Self-test for the runner-parity guard and for the manual publish path.
#
# scripts/check_runner_parity.sh only ever runs on diffs that are meant to be
# clean, so a guard that never refuses anything looks exactly like a guard that
# works. push_all_packs.sh has the same problem: nobody runs it in CI, which is
# precisely how it went on shipping packs with no Windows runner.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
failures=0

fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

# expect_guard <description> <expected exit status> <changed paths...>
expect_guard() {
  local desc="$1" expected="$2"
  shift 2
  local status=0
  printf '%s\n' "$@" | "$ROOT_DIR/scripts/check_runner_parity.sh" > /dev/null 2>&1 || status=$?
  if [ "$status" -eq "$expected" ]; then
    echo "ok: $desc"
  else
    fail "$desc (expected exit $expected, got $status)"
  fi
}

expect_guard "run.sh alone is refused" 1 scripts/run.sh
expect_guard "run.sh together with run.ps1 is accepted" 0 scripts/run.sh scripts/run.ps1
expect_guard "run.ps1 alone is accepted" 0 scripts/run.ps1
expect_guard "a change touching neither runner is accepted" 0 profiling_pack/main.py

# push_all_packs.sh syncs the runners of the repository it lives in, so the
# fixture is a throwaway copy of the script next to a throwaway pack — running
# the real one here would rewrite the working tree and push to the platform.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/scripts" "$tmp/demo_pack" "$tmp/bin"
cp "$ROOT_DIR/push_all_packs.sh" "$tmp/push_all_packs.sh"
for runner in run.sh run.bat run.ps1; do
  printf 'marker for %s\n' "$runner" > "$tmp/scripts/$runner"
done
printf 'name: demo\nversion: 1.0.0\n' > "$tmp/demo_pack/properties.yaml"
# The script ends by pushing to the platform; stub the CLI so the test
# exercises the sync and stops there.
printf '#!/bin/sh\nexit 0\n' > "$tmp/bin/qalita"
chmod +x "$tmp/bin/qalita"

if PATH="$tmp/bin:$PATH" "$tmp/push_all_packs.sh" > /dev/null 2>&1; then
  for runner in run.sh run.bat run.ps1; do
    if [ -f "$tmp/demo_pack/$runner" ]; then
      echo "ok: push_all_packs.sh syncs $runner"
    else
      fail "push_all_packs.sh did not sync $runner into the pack"
    fi
  done
else
  fail "push_all_packs.sh exited non-zero on the fixture"
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "all checks passed"
