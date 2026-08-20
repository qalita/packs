#!/bin/sh
echo "Running as user: $(whoami)"

# Extract pack name from properties.yaml using Python
echo "Extracting pack name..."
PACK_NAME=$(grep 'name:' properties.yaml | awk '{print $2}')
if [ $? -ne 0 ]; then
    echo "Failed to extract pack name."
    exit 1
fi
echo "Pack name: $PACK_NAME"

# Select a Python interpreter that satisfies the project's pyproject.toml requirement
echo "Resolving Python version from pyproject.toml..."
if [ ! -f pyproject.toml ]; then
    echo "pyproject.toml not found."
    exit 1
fi

REQUIRED_SPEC=$(grep -E '^\s*requires-python\s*=' pyproject.toml | head -n1 | cut -d '=' -f2- | tr -d ' "')
if [ -z "$REQUIRED_SPEC" ]; then
    echo "Could not read python requirement from pyproject.toml."
    exit 1
fi
echo "Python requirement: $REQUIRED_SPEC"

# Extract lower (>=) and upper (<) bounds if present (major.minor only)
MIN_VER=$(echo "$REQUIRED_SPEC" | sed -n 's/.*>=\([0-9]\+\.[0-9]\+\).*/\1/p')
MAX_VER=$(echo "$REQUIRED_SPEC" | sed -n 's/.*<\([0-9]\+\.[0-9]\+\).*/\1/p')

# Version comparison helpers using sort -V
version_ge() {
    # $1 >= $2 ?
    [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$1" ]
}

version_lt() {
    # $1 < $2 ?
    [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" = "$1" ] && [ "$1" != "$2" ]
}

# Build candidate list of python executables to try (prefer specific versions)
CANDIDATE_CMDS=""
for MINOR in $(seq 6 20); do
    if command -v "python3.$MINOR" > /dev/null 2>&1; then
        CANDIDATE_CMDS="$CANDIDATE_CMDS python3.$MINOR"
    fi
done
if command -v python3 > /dev/null 2>&1; then
    CANDIDATE_CMDS="$CANDIDATE_CMDS python3"
fi
if command -v python > /dev/null 2>&1; then
    CANDIDATE_CMDS="$CANDIDATE_CMDS python"
fi

BEST_CMD=""
BEST_VER=""
for CMD in $CANDIDATE_CMDS; do
    CMD_PATH=$(command -v "$CMD" 2>/dev/null) || continue
    # Filter out Windows shims if any
    echo "$CMD_PATH" | grep -qE '\\.exe$' && continue
    VER=$("$CMD_PATH" -V 2>&1 | awk '{print $2}' | cut -d'.' -f1,2)
    case "$VER" in
        3.*) : ;;
        *) continue ;;
    esac
    if [ -n "$MIN_VER" ] && ! version_ge "$VER" "$MIN_VER"; then
        continue
    fi
    if [ -n "$MAX_VER" ] && ! version_lt "$VER" "$MAX_VER"; then
        continue
    fi
    if [ -z "$BEST_VER" ] || version_lt "$BEST_VER" "$VER"; then
        BEST_VER="$VER"
        BEST_CMD="$CMD_PATH"
    fi
done

if [ -z "$BEST_CMD" ]; then
    echo "No available Python interpreter satisfies requirement: $REQUIRED_SPEC"
    echo "Available candidates attempted: $CANDIDATE_CMDS"
    exit 1
fi

PYTHON_CMD="$BEST_CMD"
PYTHON_VERSION=$($PYTHON_CMD -V | awk '{print $2}' | cut -d'.' -f1,2)
echo "Selected Python: $PYTHON_CMD (version $PYTHON_VERSION)"

# Make a user-level uv discoverable under minimal PATHs (cron, containers).
# Appended, not prepended: a uv provided by the environment (worker image)
# must keep precedence over an older one left in the user's home.
PATH="$PATH:$HOME/.local/bin"
export PATH

echo "Detected Python version: $PYTHON_VERSION"

# Attempt to install python3-venv if not present
if ! $PYTHON_CMD -m venv --help > /dev/null 2>&1; then
    echo "python3-venv is not installed. Attempting to install now..."
    sudo apt update
    sudo apt install "python${PYTHON_VERSION}-venv" -y
    if [ $? -ne 0 ]; then
        echo "Failed to install python3-venv for Python $PYTHON_VERSION."
        exit 1
    fi
fi

# Check if virtual environment specific to the pack exists in the parent directory
# Use the selected Python minor version in the venv name to avoid mixing versions
VENV_PATH="$HOME/.qalita/jobs/${PACK_NAME}_py${PYTHON_VERSION}_venv"
echo "Virtual Environment Path: $VENV_PATH"

if [ ! -d "$VENV_PATH" ]; then
    echo "Creating virtual environment..."
    "$PYTHON_CMD" -m venv "$VENV_PATH"
    if [ $? -ne 0 ]; then
        echo "Failed to create virtual environment for $PACK_NAME."
        exit 1
    fi
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Check if activate script is executable
if [ ! -x "$VENV_PATH/bin/activate" ]; then
    echo "Setting execute permissions on activate script..."
    chmod +x "$VENV_PATH/bin/activate"
    if [ $? -ne 0 ]; then
        echo "Failed to set execute permissions on activate script."
        exit 1
    fi
fi

# Activate the virtual environment specific to the pack from the parent directory
echo "Activating virtual environment..."
. "$VENV_PATH/bin/activate"
if [ $? -ne 0 ]; then
    echo "Failed to activate virtual environment."
    exit 1
fi
echo "Virtual environment activated."
echo "Venv python: $(which python)"
echo "Venv python version: $(python -V 2>&1)"

# Install the requirements using uv
echo "Installing requirements using uv..."
export PIP_DISABLE_PIP_VERSION_CHECK=1
# uv may complain when a project contains .venv; force target interpreter and clear inherited active-venv hint.
unset VIRTUAL_ENV

# Upgrade pip toolchain
python -m pip install --upgrade --quiet pip setuptools wheel

# Ensure uv is available. A uv provided by the environment (worker image) is
# used as-is. Otherwise install it user-wide so every pack shares one copy;
# pip refuses --user when the interpreter is itself a virtualenv or is
# externally managed (PEP 668), so fall back to the pack venv, which stays
# writable even when running unprivileged. Always address the venv
# interpreter by path: a dangling bin/python would silently fall through to
# another interpreter on PATH.
if ! command -v uv > /dev/null 2>&1; then
    echo "uv could not be found, installing now..."
    if "$PYTHON_CMD" -m pip install --user uv > /dev/null 2>&1 && command -v uv > /dev/null 2>&1; then
        echo "Installed uv user-wide."
    elif "$VENV_PATH/bin/python" -m pip install uv; then
        echo "No user-wide install possible; installed uv into the pack venv."
    else
        echo "Failed to install uv."
        exit 1
    fi
fi
echo "Using uv: $(command -v uv)"

# Generate lock file and install dependencies with uv
uv lock
if [ $? -ne 0 ]; then
    echo "Failed to generate uv lock file."
    exit 1
fi

# Proactively remove Dask-related packages from previous runs to avoid import side effects
python -m pip uninstall -y dask dask-sql distributed soda-core-pandas-dask 2>/dev/null || true

# Export lock to requirements format and install. A failed export truncates
# the file to zero bytes, and `uv pip install -r` on an empty file exits 0 —
# which would install nothing and let the pack run against a stale venv, so
# the export is only trusted when it both succeeded and produced content.
installed_from_lock=0
if uv export --no-hashes --no-emit-project > requirements.lock.txt 2>/dev/null &&
   [ -s requirements.lock.txt ]; then
    if uv pip install --python "$VENV_PATH/bin/python" -r requirements.lock.txt; then
        installed_from_lock=1
        echo "Requirements installed from exported lock."
    else
        echo "Failed to install from exported lock, trying direct install..."
    fi
else
    echo "Failed to export the lock file, trying direct install..."
fi

if [ "$installed_from_lock" -eq 0 ]; then
    if ! uv pip install --python "$VENV_PATH/bin/python" -e .; then
        echo "Failed to install requirements with uv."
        exit 1
    fi
    echo "Requirements installed from the project definition."
fi

# Run your script
echo "Running script..."
python main.py
if [ $? -ne 0 ]; then
    echo "Script execution failed."
    exit 1
fi
echo "Script executed successfully."

# Deactivate virtual environment
echo "Deactivating virtual environment..."
deactivate
echo "Virtual environment deactivated."
