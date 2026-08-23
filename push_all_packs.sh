#!/usr/bin/env bash
set -euo pipefail

# Run from the repository root. This script finds all pack folders under packs/*
# that contain a properties.yaml, extracts the pack name, and runs
# `qalita pack push -n <pack_name>` from the pack's directory.

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

for prop in "${PROP_FILES[@]}"; do
  pack_dir="$(dirname "$prop")"
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

  echo "Syncing runners into $pack_dir..."
  for runner in "${RUNNERS[@]}"; do
    cp -f "$PACKS_DIR/scripts/$runner" "$pack_dir/$runner"
  done
  chmod +x "$pack_dir/run.sh"

  echo "Pushing pack '$pack_name' from $pack_dir..."
  qalita pack push -n $pack_name
done

echo "All packs processed."
