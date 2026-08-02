#!/usr/bin/env bash
# Run pylint once per pack directory.
#
# Every pack's entrypoint is named main.py — that name is fixed by the platform's
# pack contract. Linting several packs in ONE pylint process therefore binds the
# module `main` to whichever pack was parsed first, and every `main.xxx` in the
# other packs' tests is reported as a bogus no-member error. Running pylint per
# directory keeps the check at full strength instead of disabling no-member
# repo-wide to work around a layout we do not control.
#
# Usage: scripts/lint_packs.sh [changed files...]
# With no arguments, lints every pack.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

# --rcfile is explicit because we run pylint with the pack directory as cwd:
# pylint only walks up from cwd while the directories are packages, and a pack
# has no __init__.py, so the repository .pylintrc would otherwise be ignored and
# every suppression in it silently lost.
PYLINT_ARGS=(
  --rcfile="$PWD/.pylintrc"
  --fail-under=8
  --disable=E0401,C0301,C0114,C0411,R0801
)

# Invoke pylint through the interpreter rather than the console script: a venv
# that has been moved or renamed keeps a stale shebang in bin/pylint, which then
# fails with "bad interpreter" and, because pre-commit only checks the exit code
# of the last command, would look like a clean lint.
PYTHON="${PYTHON:-python3}"
if ! "$PYTHON" -c "import pylint" >/dev/null 2>&1; then
  echo "pylint is not importable with $PYTHON; set PYTHON to an interpreter that has it" >&2
  exit 1
fi

# Map the files pre-commit passes us back to the pack that owns them, so a
# one-file commit does not lint the whole repository.
declare -A packs=()
if [ "$#" -gt 0 ]; then
  for file in "$@"; do
    case "$file" in
      */*) packs["${file%%/*}"]=1 ;;
    esac
  done
else
  for dir in *_pack; do
    [ -d "$dir" ] && packs["$dir"]=1
  done
fi

status=0
for pack in "${!packs[@]}"; do
  [ -d "$pack" ] || continue
  files=$(find "$pack" -name '*.py' -not -path '*/.venv/*' \
    -not -path '*/__pycache__/*' 2>/dev/null)
  [ -n "$files" ] || continue
  # PYTHONPATH mirrors how a pack actually runs: cwd is the pack directory, so
  # `import main` resolves to that pack's entrypoint and nothing else.
  if ! (cd "$pack" && PYTHONPATH=. "$PYTHON" -m pylint "${PYLINT_ARGS[@]}" \
      $(find . -name '*.py' -not -path './.venv/*' \
        -not -path '*/__pycache__/*')); then
    status=1
  fi
done

exit "$status"
