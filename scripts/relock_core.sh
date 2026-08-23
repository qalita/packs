#!/usr/bin/env bash
# Re-lock every pack onto the newest published qalita-core.
#
# Packs declare a floor (`qalita-core>=2.0.0`) but ship a uv.lock, and
# scripts/run.sh replays `uv lock` on the worker at every job. A plain
# `uv lock` keeps a dependency that is already locked as long as it still
# satisfies the floor, so a published pack stays on the qalita-core frozen in
# its lock forever: a core release never reaches it on its own, on the worker
# or anywhere else. `--upgrade-package` is the only thing that moves the pin,
# and neither run.sh nor bump_pack_versions.sh passes it.
#
# Run this after a core release and before bumping/pushing: the list of locks
# differing from HEAD printed at the end is exactly the set of packs whose
# behaviour changed and that therefore need a version bump. Comparing with
# HEAD preserves that release list across a failed run and its later retry.
# See AGENTS.md, "Propager un correctif qalita-core".
#
# Usage: scripts/relock_core.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PACKAGE="qalita-core"

if ! command -v uv > /dev/null 2>&1; then
  echo "uv is not on PATH; install it before re-locking" >&2
  exit 1
fi

# uv.lock lists dependencies as [[package]] blocks, so the pinned version is
# the first `version =` line following the package's name.
locked_version() {
  local lock="$1"
  [ -f "$lock" ] || return 0
  awk -v pkg="name = \"$PACKAGE\"" '
    $0 == pkg { found = 1; next }
    found && /^version = / { gsub(/["]/, "", $3); print $3; exit }
  ' "$lock"
}

# A pack is any directory carrying pack metadata. A directory with metadata
# but no pyproject.toml is malformed: silently skipping it can publish an
# incomplete release. Directories without metadata remain unrelated stubs.
packs=()
failed=()
for dir in */; do
  pack="${dir%/}"
  [ -f "$pack/properties.yaml" ] || [ -f "$pack/pack_conf.json" ] || continue
  if [ ! -f "$pack/pyproject.toml" ]; then
    echo "[$pack] no pyproject.toml" >&2
    failed+=("$pack")
    continue
  fi
  packs+=("$pack")
done

if [ ${#packs[@]} -eq 0 ]; then
  if [ ${#failed[@]} -gt 0 ]; then
    echo "No valid pack found under $PWD" >&2
    echo "Failed to re-lock:" >&2
    printf '  %s\n' "${failed[@]}" >&2
  else
    echo "No pack found under $PWD" >&2
  fi
  exit 1
fi

for pack in "${packs[@]}"; do
  before="$(locked_version "$pack/uv.lock")"

  if ! (cd "$pack" && uv lock --upgrade-package "$PACKAGE"); then
    echo "[$pack] uv lock --upgrade-package $PACKAGE failed" >&2
    failed+=("$pack")
    continue
  fi

  after="$(locked_version "$pack/uv.lock")"
  if [ -z "$after" ]; then
    # Every pack depends on core; an absent pin means the manifest stopped
    # declaring it, which silently un-pins the pack rather than upgrading it.
    echo "[$pack] $PACKAGE absent from uv.lock after re-lock" >&2
    failed+=("$pack")
  elif [ "$before" = "$after" ]; then
    echo "[$pack] $PACKAGE $after (unchanged)"
  else
    echo "[$pack] $PACKAGE ${before:-none} -> $after"
  fi
done

changed_packs=()
for lock in *_pack/uv.lock; do
  [ -f "$lock" ] || continue
  # git diff HEAD includes both staged and unstaged changes. An untracked
  # newly-created lock has no HEAD counterpart and also needs a pack bump.
  if ! git ls-files --error-unmatch -- "$lock" > /dev/null 2>&1 || \
    ! git diff --quiet HEAD -- "$lock"; then
    changed_packs+=("${lock%/uv.lock}")
  fi
done

echo
if [ ${#changed_packs[@]} -gt 0 ]; then
  echo "Locks differing from HEAD — bump the version of these packs, then push:"
  printf '  %s\n' "${changed_packs[@]}"
else
  echo "No locks differ from HEAD: no pack version bump is required."
fi

if [ ${#failed[@]} -gt 0 ]; then
  echo
  echo "Failed to re-lock:" >&2
  printf '  %s\n' ${failed[@]+"${failed[@]}"} >&2
  exit 1
fi
