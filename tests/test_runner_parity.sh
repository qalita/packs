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

# The workflow itself chooses the commit range before it invokes the guard.
# Execute that exact run block with no base ref: a green job here would leave
# run.sh changes on a force-push outside the parity check.
expect_missing_base_is_refused() {
  local output status=0
  output=$(awk '
    $0 == "      - name: Fail if run.sh moved without run.ps1" { in_step = 1; next }
    in_step && $0 == "        run: |" { in_run = 1; next }
    in_run && /^  [^ ]/ { exit }
    in_run { sub(/^          /, ""); print }
  ' "$ROOT_DIR/.github/workflows/tests.yml" | BASE= bash -s 2>&1) || status=$?

  if [ "$status" -ne 1 ]; then
    fail "runner-parity workflow refuses a missing base ref (expected exit 1, got $status)"
  elif [[ "$output" != *"::error"* ]]; then
    fail "runner-parity workflow annotates a missing base ref"
  else
    echo "ok: runner-parity workflow refuses and annotates a missing base ref"
  fi
}

expect_missing_base_is_refused

# push_all_packs.sh stages packs from the repository it lives in, so the fixture
# is a throwaway repository with one throwaway pack. Running the real script
# here would push to the platform; only that external CLI boundary is replaced.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/scripts" "$tmp/demo_pack" "$tmp/bin" "$tmp/capture"
cp "$ROOT_DIR/push_all_packs.sh" "$tmp/push_all_packs.sh"
cp "$ROOT_DIR/.gitignore" "$tmp/.gitignore"
for runner in run.sh run.bat run.ps1; do
  printf 'marker for %s\n' "$runner" > "$tmp/scripts/$runner"
done
printf 'name: demo\nversion: 1.0.0\n' > "$tmp/demo_pack/properties.yaml"
printf 'print("committed source")\n' > "$tmp/demo_pack/main.py"

(
  cd "$tmp"
  git init -q
  git config user.email test@example.invalid
  git config user.name "Qalita Packs Tests"
  git add .gitignore push_all_packs.sh scripts demo_pack/properties.yaml demo_pack/main.py
  git commit -qm fixture
)

# A tracked modification must be staged from the working tree, not from HEAD.
printf 'print("modified tracked source")\n' > "$tmp/demo_pack/main.py"

# This file is intentionally untracked but not ignored: manual publishing must
# retain the current working tree, not silently fall back to HEAD.
printf 'release note\n' > "$tmp/demo_pack/local-note.txt"

# These local-only files reproduce what made the real archive reach 198 MB.
mkdir -p \
  "$tmp/demo_pack/.venv/bin" \
  "$tmp/demo_pack/venv/bin" \
  "$tmp/demo_pack/.pytest_cache" \
  "$tmp/demo_pack/__pycache__" \
  "$tmp/demo_pack/.mypy_cache" \
  "$tmp/demo_pack/.ruff_cache" \
  "$tmp/demo_pack/build" \
  "$tmp/demo_pack/dist" \
  "$tmp/demo_pack/demo.egg-info"
printf 'python binary\n' > "$tmp/demo_pack/.venv/bin/python"
printf 'python binary\n' > "$tmp/demo_pack/venv/bin/python"
printf 'pytest state\n' > "$tmp/demo_pack/.pytest_cache/state"
printf 'bytecode\n' > "$tmp/demo_pack/__pycache__/main.pyc"
printf 'mypy state\n' > "$tmp/demo_pack/.mypy_cache/state"
printf 'ruff state\n' > "$tmp/demo_pack/.ruff_cache/state"
printf 'build output\n' > "$tmp/demo_pack/build/output"
printf 'wheel\n' > "$tmp/demo_pack/dist/demo.whl"
printf 'metadata\n' > "$tmp/demo_pack/demo.egg-info/PKG-INFO"
printf '{"generated": true}\n' > "$tmp/demo_pack/metrics.json"

local_artifacts=(
  .venv/bin/python
  venv/bin/python
  .pytest_cache/state
  __pycache__/main.pyc
  .mypy_cache/state
  .ruff_cache/state
  build/output
  dist/demo.whl
  demo.egg-info/PKG-INFO
  metrics.json
)
artifact_checksums="$tmp/capture/source-artifacts.sha256"
(
  cd "$tmp/demo_pack"
  sha256sum "${local_artifacts[@]}"
) > "$artifact_checksums"

# Capture the exact pack tree visible to the CLI. This keeps staging and file
# selection real while replacing only the external upload.
cat > "$tmp/bin/qalita" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
: "${QALITA_TEST_CAPTURE:?}"
find demo_pack -type f -printf '%P\n' | LC_ALL=C sort > "$QALITA_TEST_CAPTURE"
cp demo_pack/main.py "${QALITA_TEST_MAIN:?}"
EOF
chmod +x "$tmp/bin/qalita"

manifest="$tmp/capture/manifest"
if (
  cd "$tmp"
  QALITA_TEST_CAPTURE="$manifest" \
    QALITA_TEST_MAIN="$tmp/capture/main.py" \
    PATH="$tmp/bin:$PATH" \
    ./push_all_packs.sh
) > /dev/null 2>&1; then
  for included in properties.yaml main.py local-note.txt run.sh run.bat run.ps1; do
    if grep -Fqx "$included" "$manifest"; then
      echo "ok: staged pack includes $included"
    else
      fail "staged pack omitted $included"
    fi
  done

  for excluded in "${local_artifacts[@]}"; do
    if grep -Fqx "$excluded" "$manifest"; then
      fail "staged pack included ignored artifact $excluded"
    else
      echo "ok: staged pack excludes $excluded"
    fi
  done

  if grep -Fqx 'print("modified tracked source")' "$tmp/capture/main.py"; then
    echo "ok: staged pack uses modified tracked content"
  else
    fail "staged pack did not use modified tracked content"
  fi

  if (
    cd "$tmp/demo_pack"
    sha256sum --check "$artifact_checksums" > /dev/null
  ); then
    echo "ok: source pack preserves local artifact contents"
  else
    fail "source pack changed or removed a local artifact"
  fi

  for runner in run.sh run.bat run.ps1; do
    if [ -e "$tmp/demo_pack/$runner" ]; then
      fail "push_all_packs.sh copied $runner into the source pack"
    else
      echo "ok: source pack is not mutated with $runner"
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
