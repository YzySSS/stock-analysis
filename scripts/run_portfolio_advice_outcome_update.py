#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.portfolio.service import PortfolioService


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate portfolio AI advice outcomes.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum succeeded advice runs to scan.")
    parser.add_argument("--force", action="store_true", help="Recompute already-created horizon outcomes.")
    args = parser.parse_args()

    result = PortfolioService().evaluate_advice_outcomes(limit=args.limit, force=args.force)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if not result.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
