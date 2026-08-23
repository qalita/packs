#!/usr/bin/env bash
# Refuse a change to scripts/run.sh that leaves scripts/run.ps1 behind.
#
# Reads changed file paths on stdin (one per line), same contract as
# scripts/select_test_matrix.sh. Exits 1 when the two have drifted apart.
#
# The two runners are one program written twice — the platform picks one per
# worker OS — and nothing kept them in step. run.ps1 spent seven months on
# January's install logic (no lock export, and a fallback that installed the
# pack instead of its dependencies) because three fixes to run.sh were never
# reported, and the drift was invisible until it reached a Windows worker.
#
# Only the run.sh -> run.ps1 direction is guarded: a Windows-only fix stands on
# its own. A run.sh change with no Windows counterpart — the apt install of
# python3-venv, say — is acknowledged by saying so in a comment in run.ps1 in
# the same commit, which is both the escape hatch and the trace.
#
# Usable outside CI: `git diff --name-only main | scripts/check_runner_parity.sh`
set -euo pipefail

sh_changed=0
ps1_changed=0
while IFS= read -r path; do
  case "$path" in
    scripts/run.sh) sh_changed=1 ;;
    scripts/run.ps1) ps1_changed=1 ;;
  esac
done

if [ "$sh_changed" -eq 1 ] && [ "$ps1_changed" -eq 0 ]; then
  echo "scripts/run.sh changed but scripts/run.ps1 did not." >&2
  echo "Port the change to the PowerShell runner, or add a comment there saying why it does not apply." >&2
  exit 1
fi

echo "runner parity ok (run.sh changed: $sh_changed, run.ps1 changed: $ps1_changed)"
