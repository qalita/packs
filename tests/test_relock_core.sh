#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEST_ROOT"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local needle="$1"
  local file="$2"
  grep -Fqx "$needle" "$file" || fail "expected $file to contain: $needle"
}

make_repo() {
  local repo="$1"
  mkdir -p "$repo/scripts" "$repo/bin"
  cp "$ROOT_DIR/scripts/relock_core.sh" "$repo/scripts/relock_core.sh"
  chmod +x "$repo/scripts/relock_core.sh"
  (
    cd "$repo"
    git init -q
    git config user.email test@example.invalid
    git config user.name "Qalita Packs Tests"
  )
}

write_pack() {
  local repo="$1"
  local name="$2"
  mkdir -p "$repo/$name"
  printf '%s\n' 'version: 1.0.0' > "$repo/$name/properties.yaml"
  printf '%s\n' '[project]' 'name = "fixture-pack"' > "$repo/$name/pyproject.toml"
  printf '%s\n' '[[package]]' 'name = "qalita-core"' 'version = "2.0.0"' > "$repo/$name/uv.lock"
}

write_fake_uv() {
  local repo="$1"
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'if [ "${UV_FAIL_BETA:-0}" = "1" ] && [ "$(basename "$PWD")" = "beta_pack" ]; then' \
    '  exit 1' \
    'fi' \
    'if [ "$(basename "$PWD")" = "alpha_pack" ]; then' \
    '  printf "%s\\n" "# resolved metadata refresh" >> uv.lock' \
    'else' \
    '  sed -i "s/version = \"2.0.0\"/version = \"2.1.0\"/" uv.lock' \
    'fi' \
    > "$repo/bin/uv"
  chmod +x "$repo/bin/uv"
}

test_retry_reports_every_lock_changed_from_head() {
  local repo="$TEST_ROOT/retry"
  local first_output="$TEST_ROOT/first-output"
  local retry_output="$TEST_ROOT/retry-output"
  local summary="$TEST_ROOT/retry-summary"

  make_repo "$repo"
  write_pack "$repo" alpha_pack
  write_pack "$repo" beta_pack
  write_fake_uv "$repo"
  (
    cd "$repo"
    git add .
    git commit -qm fixture
  )

  if (
    cd "$repo"
    PATH="$repo/bin:$PATH" UV_FAIL_BETA=1 ./scripts/relock_core.sh
  ) > "$first_output" 2>&1; then
    fail "the partial relock unexpectedly succeeded"
  fi

  (
    cd "$repo"
    PATH="$repo/bin:$PATH" ./scripts/relock_core.sh
  ) > "$retry_output" 2>&1 || fail "the retry unexpectedly failed"

  awk '/Locks differing from HEAD/{collect = 1; next} collect {print}' \
    "$retry_output" > "$summary"
  assert_contains '  alpha_pack' "$summary"
  assert_contains '  beta_pack' "$summary"
}

test_metadata_without_pyproject_is_a_failure() {
  local repo="$TEST_ROOT/missing-pyproject"
  local output="$TEST_ROOT/missing-pyproject-output"

  make_repo "$repo"
  write_pack "$repo" valid_pack
  mkdir -p "$repo/broken_pack" "$repo/stub_pack"
  printf '%s\n' 'version: 1.0.0' > "$repo/broken_pack/properties.yaml"
  write_fake_uv "$repo"
  (
    cd "$repo"
    git add .
    git commit -qm fixture
  )

  if (
    cd "$repo"
    PATH="$repo/bin:$PATH" ./scripts/relock_core.sh
  ) > "$output" 2>&1; then
    fail "metadata-bearing pack without pyproject.toml unexpectedly succeeded"
  fi

  grep -F '[broken_pack] no pyproject.toml' "$output" > /dev/null || \
    fail "the invalid pack was not reported"
}

test_retry_reports_every_lock_changed_from_head
test_metadata_without_pyproject_is_a_failure

echo "relock_core tests passed"
