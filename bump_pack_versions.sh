#!/usr/bin/env bash
set -euo pipefail

# This script increments the patch version in every properties.yaml under packs/*
# and runs `uv lock` in each corresponding pack directory that has a pyproject.toml.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mapfile -t PROP_FILES < <(find "$ROOT_DIR" -mindepth 2 -maxdepth 2 -type f -name properties.yaml | sort)

if [[ ${#PROP_FILES[@]} -eq 0 ]]; then
  echo "No properties.yaml files found under $ROOT_DIR"
  exit 1
fi

for prop in "${PROP_FILES[@]}"; do
  pack_dir="$(dirname "$prop")"
  pack_name="$(basename "$pack_dir")"

  # Extract current version from the first matching line
  if ! current_line=$(grep -E '^[[:space:]]*version:[[:space:]]*' "$prop" | head -n1); then
    echo "[$pack_name] No version line found in $prop, skipping" >&2
    continue
  fi

  # Remove key, trim, and strip surrounding quotes if present
  current_version=$(echo "$current_line" | sed -E 's/^[[:space:]]*version:[[:space:]]*//; s/[[:space:]]*$//; s/^"//; s/"$//; s/^'"'"'//; s/'"'"'$//')

  if [[ ! "$current_version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "[$pack_name] Version '$current_version' not in x.y.z format, skipping" >&2
    continue
  fi

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  new_patch=$((patch + 1))
  new_version="${major}.${minor}.${new_patch}"

  # Replace the first occurrence of the version line
  tmp_file="${prop}.tmp"
  awk -v newv="$new_version" '
    BEGIN{done=0}
    /^[[:space:]]*version:[[:space:]]*/ && done==0 {
      printf "version: %s\n", newv
      done=1
      next
    }
    {print}
  ' "$prop" > "$tmp_file" && mv "$tmp_file" "$prop"

  echo "[$pack_name] Bumped version: $current_version -> $new_version"

  # Run uv lock if pyproject.toml exists
  if [[ -f "$pack_dir/pyproject.toml" ]]; then
    echo "[$pack_name] Running uv lock..."
    (cd "$pack_dir" && uv lock)
  else
    echo "[$pack_name] No pyproject.toml; skipping uv lock"
  fi
done

echo "Done."
