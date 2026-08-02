"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import sys
from pathlib import Path

# The pack is a script, not an installed package: make main.py importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
