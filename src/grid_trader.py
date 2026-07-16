"""Compatibility import for the archived, non-product ETF grid prototype."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from archive.legacy_grid_trader import *  # noqa: F401,F403
