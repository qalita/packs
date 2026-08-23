# PowerShell runner for QALITA pack (Windows, no WSL required)
#
# Functional counterpart of scripts/run.sh: same steps, same fallbacks, same
# failure messages. Keep the two in step - scripts/check_runner_parity.sh fails
# a change to run.sh that leaves this file untouched, because a drift here only
# ever surfaces on a client's Windows worker.
Param()
$ErrorActionPreference = 'Stop'

# PowerShell 7.3+ turns a non-zero exit code from a native command into a
# terminating error while $ErrorActionPreference is 'Stop'. Every uv and pip
# call below reads $LASTEXITCODE itself and has a fallback to try, so opt out
# rather than let the first recoverable failure kill the job.
$PSNativeCommandUseErrorActionPreference = $false

function Fail([string]$message) {
  # Write-Error raises a terminating error under 'Stop' and never returns, so
  # the caller's `exit 1` would be dead code and the worker would read
  # PowerShell's generic status instead of ours. Report on stderr and leave.
  [Console]::Error.WriteLine($message)
  exit 1
}

Write-Host ("Running as user: {0}" -f $env:USERNAME)

# Extract pack name from properties.yaml
Write-Host "Extracting pack name..."
$PACK_NAME = ($null)
try {
  $PACK_NAME = (Select-String -Path "properties.yaml" -Pattern '^\s*name:\s*(.+)$' | ForEach-Object { $_.Matches[0].Groups[1].Value.Trim() } | Select-Object -First 1)
} catch {}
if (-not $PACK_NAME) { Fail "Failed to extract pack name." }
Write-Host ("Pack name: {0}" -f $PACK_NAME)

# Resolve Python requirement from pyproject.toml
Write-Host "Resolving Python version from pyproject.toml..."
if (-not (Test-Path "pyproject.toml")) { Fail "pyproject.toml not found." }
$REQUIRED_SPEC = ($null)
try {
  $REQUIRED_SPEC = (Select-String -Path "pyproject.toml" -Pattern '^\s*requires-python\s*=\s*\"?(.+?)\"?\s*$' | Select-Object -First 1).Matches.Groups[1].Value.Trim()
} catch {}
if (-not $REQUIRED_SPEC) { Fail "Could not read python requirement from pyproject.toml." }
Write-Host ("Python requirement: {0}" -f $REQUIRED_SPEC)

$MIN_VER = ([regex]::Match($REQUIRED_SPEC, '>=\s*([0-9]+\.[0-9]+)')).Groups[1].Value
$MAX_VER = ([regex]::Match($REQUIRED_SPEC, '<\s*([0-9]+\.[0-9]+)')).Groups[1].Value

function VersionGE([string]$a,[string]$b) { return ([version]($a + '.0')) -ge ([version]($b + '.0')) }
function VersionLT([string]$a,[string]$b) { return ([version]($a + '.0')) -lt ([version]($b + '.0')) }

# Build candidate list via py launcher and fallbacks. The launcher has printed
# two layouts over the years - ` -3.9-64   C:\...` and, since 3.11,
# ` -V:3.12 *   C:\...` - and matching only one of them silently reduces this
# to whatever `python` happens to be on PATH, which on Windows is often the
# Microsoft Store alias rather than a usable interpreter.
$candidates = @()
if (Get-Command py -ErrorAction SilentlyContinue) {
  try {
    $out = & py -0p 2>$null
    foreach ($l in $out) {
      if ($l -match '^\s*-(?:V:)?([0-9]+\.[0-9]+)\S*\s*\*?\s+(.+?)\s*$') {
        $v = $Matches[1]; $p = $Matches[2]
        if (($v -like '3.*') -and (Test-Path $p)) { $candidates += @{ ver=$v; path=$p } }
      }
    }
  } catch {}
}

foreach ($cmd in @('python3','python')) {
  $p = (Get-Command $cmd -ErrorAction SilentlyContinue).Path
  if ($p) {
    # stderr is discarded rather than merged with 2>&1: a merged native error
    # stream is turned into a NativeCommandError under 'Stop', so the Store
    # alias - which answers on stderr and exits 9009 - would abort the run
    # instead of simply being skipped as a candidate.
    $vout = (& $p -V 2>$null | Select-Object -First 1)
    $v = ([string]$vout -replace 'Python\s+','').Split()[0]
    $v = ($v -split '\.')[0..1] -join '.'
    if ($v -like '3.*') { $candidates += @{ ver=$v; path=$p } }
  }
}

$best = $null
foreach ($c in ($candidates | Sort-Object { [version]($_.ver + '.0') } -Descending)) {
  if ($MIN_VER -and -not (VersionGE $c.ver $MIN_VER)) { continue }
  if ($MAX_VER -and -not (VersionLT $c.ver $MAX_VER)) { continue }
  $best = $c; break
}
if (-not $best) { Fail ("No available Python interpreter satisfies requirement: {0}" -f $REQUIRED_SPEC) }
$PYTHON_CMD = $best.path
$PYTHON_VERSION = $best.ver
Write-Host ("Selected Python: {0} (version {1})" -f $PYTHON_CMD, $PYTHON_VERSION)

# Make a user-level uv discoverable under a minimal PATH (a service account's
# session does not always carry the per-user script directories). Appended, not
# prepended: a uv provided by the environment must keep precedence over an
# older one left in the user profile.
$env:PATH = "$env:PATH;" + (Join-Path $env:USERPROFILE ".local\bin")

Write-Host ("Detected Python version: {0}" -f $PYTHON_VERSION)

# run.sh installs the missing python3-venv package here; Windows has no
# equivalent, because venv ships with the python.org installer. An interpreter
# without it is a Store alias or the embeddable package, and no amount of
# retrying repairs that - so say which interpreter is at fault straight away
# rather than fail later on an unreadable venv error.
$null = & $PYTHON_CMD -m venv --help 2>$null
if ($LASTEXITCODE -ne 0) {
  Fail ("The selected Python ({0}) has no usable venv module. Install Python {1} from python.org: the Microsoft Store alias and the embeddable package do not ship venv." -f $PYTHON_CMD, $PYTHON_VERSION)
}

# Build venv path
$QALITA_HOME = if ($env:QALITA_HOME) { $env:QALITA_HOME } else { Join-Path $env:USERPROFILE ".qalita" }
Write-Host ("Virtual Environment Root: {0}" -f $QALITA_HOME)
$VENV_PATH = Join-Path $QALITA_HOME ("jobs\{0}_py{1}_venv" -f $PACK_NAME, $PYTHON_VERSION)
Write-Host ("Virtual Environment Path: {0}" -f $VENV_PATH)
$VENV_SCRIPTS = Join-Path $VENV_PATH "Scripts"
$VENV_PY = Join-Path $VENV_SCRIPTS "python.exe"

# The venv lives in the worker's profile and outlives the job that created it,
# so a half-created one - a creation interrupted by a full disk, or one whose
# base interpreter has been uninstalled since - is reused by every later run
# until someone deletes the directory by hand. Prove the interpreter runs
# before trusting the directory.
if (Test-Path $VENV_PATH) {
  $venvUsable = $false
  if (Test-Path $VENV_PY) {
    # `-c "pass"` rather than an empty program: PowerShell drops an empty
    # string argument to a native command, which would make python fail on
    # every run and rebuild a healthy venv each time.
    $null = & $VENV_PY -c "pass" 2>$null
    $venvUsable = ($LASTEXITCODE -eq 0)
  }
  if (-not $venvUsable) {
    Write-Host "Existing virtual environment is unusable, recreating it..."
    Remove-Item -Recurse -Force $VENV_PATH
  }
}

if (-not (Test-Path $VENV_PATH)) {
  Write-Host "Creating virtual environment..."
  & $PYTHON_CMD -m venv "$VENV_PATH"
  if ($LASTEXITCODE -ne 0) {
    # Clean up after ourselves: a partial tree is exactly the state the check
    # above exists to recover from.
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $VENV_PATH
    Fail ("Failed to create virtual environment for {0}." -f $PACK_NAME)
  }
  Write-Host "Virtual environment created."
} else {
  Write-Host "Virtual environment already exists."
}

# Activate venv
Write-Host "Activating virtual environment..."
$activatePs1 = Join-Path $VENV_SCRIPTS "Activate.ps1"
if (Test-Path $activatePs1) {
  . $activatePs1
} else {
  # No `cmd /c activate.bat` fallback: a child cmd.exe activates its own
  # environment and takes it to the grave, which used to look like a successful
  # activation. Do what the script does, in this process.
  $env:VIRTUAL_ENV = $VENV_PATH
  $env:PATH = "$VENV_SCRIPTS;$env:PATH"
}

# `python` resolving to something is not proof of activation: a machine-wide
# interpreter answers Get-Command just as well, and the pack would then install
# into, and run against, the wrong site-packages.
$activePython = (Get-Command python -ErrorAction SilentlyContinue).Path
if (-not $activePython -or -not $activePython.StartsWith($VENV_SCRIPTS, [System.StringComparison]::OrdinalIgnoreCase)) {
  Fail "Failed to activate virtual environment."
}
Write-Host ("Venv python: {0}" -f $activePython)
Write-Host ("Venv python version: {0}" -f (& python -V 2>$null))

# Install the requirements using uv
Write-Host "Installing requirements using uv..."
$env:PIP_DISABLE_PIP_VERSION_CHECK = "1"
# uv may complain when a project contains .venv; every uv call below names the
# target interpreter explicitly, so clear the inherited active-venv hint.
Remove-Item Env:VIRTUAL_ENV -ErrorAction SilentlyContinue

# Upgrade pip toolchain. Through `-m pip`, never the pip.exe shim: Windows
# cannot replace a running executable, so upgrading pip by its console script
# fails on a locked file.
& $VENV_PY -m pip install --upgrade --quiet pip setuptools wheel

# Ensure uv is available. A uv provided by the environment (worker image) is
# used as-is. Otherwise install it user-wide so every pack shares one copy;
# pip refuses --user when the interpreter is itself a virtualenv, so fall back
# to the pack venv, which stays writable even for an unprivileged service
# account. Always address an interpreter by path: a dangling Scripts\python.exe
# would silently fall through to another interpreter on PATH.
$UV_BIN = (Get-Command uv -ErrorAction SilentlyContinue).Path
if (-not $UV_BIN) {
  Write-Host "uv could not be found, installing now..."
  $null = & $PYTHON_CMD -m pip install --user uv 2>$null
  if ($LASTEXITCODE -eq 0) {
    # pip --user drops uv.exe in the per-user Scripts directory, whose path
    # depends on the Python version, so ask the interpreter instead of guessing.
    $userScripts = (& $PYTHON_CMD -c "import sysconfig; print(sysconfig.get_path('scripts', 'nt_user'))" 2>$null | Select-Object -First 1)
    if ($userScripts) { $userScripts = $userScripts.Trim() }
    if ($userScripts -and (Test-Path (Join-Path $userScripts "uv.exe"))) {
      $env:PATH = "$env:PATH;$userScripts"
      $UV_BIN = Join-Path $userScripts "uv.exe"
      Write-Host "Installed uv user-wide."
    }
  }
  if (-not $UV_BIN) {
    & $VENV_PY -m pip install uv
    if (($LASTEXITCODE -eq 0) -and (Test-Path (Join-Path $VENV_SCRIPTS "uv.exe"))) {
      $UV_BIN = Join-Path $VENV_SCRIPTS "uv.exe"
      Write-Host "No user-wide install possible; installed uv into the pack venv."
    }
  }
  if (-not $UV_BIN) { Fail "Failed to install uv." }
}
Write-Host ("Using uv: {0}" -f $UV_BIN)

# Generate lock file and install dependencies with uv
& $UV_BIN lock
if ($LASTEXITCODE -ne 0) { Fail "Failed to generate uv lock file." }

# Proactively remove Dask-related packages from previous runs to avoid import side effects
$null = & $VENV_PY -m pip uninstall -y dask dask-sql distributed soda-core-pandas-dask 2>$null

# Export lock to requirements format and install. A failed export truncates the
# file to zero bytes, and `uv pip install -r` on an empty file exits 0 - which
# would install nothing and let the pack run against a stale venv, so the export
# is only trusted when it both succeeded and produced content. The file is
# written through .NET rather than with `>`: Windows PowerShell redirects to
# UTF-16, which uv reads back as garbage.
$LOCK_FILE = Join-Path (Get-Location).Path "requirements.lock.txt"
$installedFromLock = $false
$lockLines = @(& $UV_BIN export --no-hashes --no-emit-project 2>$null)
$exportStatus = $LASTEXITCODE
$lockHasContent = (@($lockLines | Where-Object { $_ -and $_.Trim() }).Count -gt 0)
if (($exportStatus -eq 0) -and $lockHasContent) {
  [System.IO.File]::WriteAllLines($LOCK_FILE, [string[]]$lockLines, (New-Object -TypeName System.Text.UTF8Encoding -ArgumentList $false))
  & $UV_BIN pip install --python "$VENV_PY" -r "$LOCK_FILE"
  if ($LASTEXITCODE -eq 0) {
    $installedFromLock = $true
    Write-Host "Requirements installed from exported lock."
  } else {
    Write-Host "Failed to install from exported lock, trying direct install..."
  }
} else {
  Write-Host "Failed to export the lock file, trying direct install..."
}

if (-not $installedFromLock) {
  # Install the dependencies, not the pack itself: main.py is run from the pack
  # directory and is never imported as an installed package, and no pack
  # declares a hatchling file-selection target - so `-e .` fails the build
  # ("Unable to determine which files to ship inside the wheel") and turns a
  # recoverable lock failure into a dead job.
  & $UV_BIN pip install --python "$VENV_PY" -r pyproject.toml
  if ($LASTEXITCODE -ne 0) { Fail "Failed to install requirements with uv." }
  Write-Host "Requirements installed from pyproject.toml."
}

# Run your script
Write-Host "Running script..."
python main.py
if ($LASTEXITCODE -ne 0) { Fail "Script execution failed." }
Write-Host "Script executed successfully."

# Deactivate virtual environment
Write-Host "Deactivating virtual environment..."
if (Get-Command deactivate -ErrorAction SilentlyContinue) { deactivate }
Write-Host "Virtual environment deactivated."
