"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import sys
from pathlib import Path

# main.py sits at the pack root and is executed as a script by run.sh, so it is
# not importable as a package; put the pack root on the path instead.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
