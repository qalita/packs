#!/usr/bin/env bash
set -euo pipefail

# This script finds every pack folder containing properties.yaml, stages its
# tracked and non-ignored working-tree files, adds the shared runners, and runs
# `qalita pack push -n <pack_name>` from the temporary staging root.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKS_DIR="$ROOT_DIR"
# The platform picks a runner per worker OS, so a pack ships all three or it is
# not runnable everywhere: on Windows the CLI looks for run.bat, then run.ps1,
# then a bash on PATH, and gives up when it finds none. This list must stay the
# same as the one in .github/workflows/publish.yml — syncing only run.sh here is
# how manually published packs ended up with no Windows runner at all.
RUNNERS=(run.sh run.bat run.ps1)

if [[ ! -d "$PACKS_DIR" ]]; then
  echo "Packs directory not found at $PACKS_DIR" >&2
  exit 1
fi

if ! GIT_ROOT="$(git -C "$PACKS_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
  echo "Packs directory is not inside a Git repository: $PACKS_DIR" >&2
  exit 1
fi

if [[ "$GIT_ROOT" != "$PACKS_DIR" ]]; then
  echo "push_all_packs.sh must live at the Git repository root: $GIT_ROOT" >&2
  exit 1
fi

for runner in "${RUNNERS[@]}"; do
  if [[ ! -f "$PACKS_DIR/scripts/$runner" ]]; then
    echo "Source $runner not found at $PACKS_DIR/scripts/$runner" >&2
    exit 1
  fi
done

mapfile -t PROP_FILES < <(find "$PACKS_DIR" -mindepth 2 -maxdepth 2 -type f -name properties.yaml | sort)

if [[ ${#PROP_FILES[@]} -eq 0 ]]; then
  echo "No properties.yaml files found under $PACKS_DIR" >&2
  exit 1
fi

STAGING_DIR="$(mktemp -d)"
FILE_LIST="$STAGING_DIR/files"
cleanup() {
  rm -rf -- "$STAGING_DIR"
}
trap cleanup EXIT

for prop in "${PROP_FILES[@]}"; do
  pack_dir="$(dirname "$prop")"
  pack_rel="${pack_dir#"$PACKS_DIR"/}"
  # Extract name from properties.yaml; expect a line like: name: something
  if ! name_line=$(grep -E '^[[:space:]]*name:[[:space:]]*' "$prop" | head -n1); then
    echo "[$pack_dir] No name found in $prop, skipping" >&2
    continue
  fi
  pack_name=$(echo "$name_line" | sed -E 's/^[[:space:]]*name:[[:space:]]*//; s/[[:space:]]*$//; s/^"//; s/"$//; s/^'"'"'//; s/'"'"'$//')
  # Remove a single trailing slash if present (e.g., "accuracy/")
  if [[ -z "$pack_name" ]]; then
    echo "[$pack_dir] Empty pack name, skipping" >&2
    continue
  fi

  staged_pack_dir="$STAGING_DIR/$pack_rel"
  mkdir -p "$staged_pack_dir"

  # Preserve tracked changes and non-ignored untracked files while keeping
  # local environments, caches, and generated outputs out of the upload.
  git -C "$PACKS_DIR" ls-files -z --cached --others --exclude-standard \
    -- "$pack_rel" > "$FILE_LIST"
  while IFS= read -r -d '' rel_path; do
    source_path="$PACKS_DIR/$rel_path"
    if [[ ! -e "$source_path" && ! -L "$source_path" ]]; then
      continue
    fi
    destination_path="$STAGING_DIR/$rel_path"
    mkdir -p "$(dirname "$destination_path")"
    cp -a -- "$source_path" "$destination_path"
  done < "$FILE_LIST"

  echo "Adding runners to staged pack $pack_rel..."
  for runner in "${RUNNERS[@]}"; do
    cp -f "$PACKS_DIR/scripts/$runner" "$staged_pack_dir/$runner"
  done
  chmod +x "$staged_pack_dir/run.sh"

  echo "Pushing pack '$pack_name' from staged directory..."
  (
    cd "$STAGING_DIR"
    qalita pack push -n "$pack_name"
  )
done

echo "All packs processed."
