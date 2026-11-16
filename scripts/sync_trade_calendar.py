"""Validate and add missing independent calendar history; dry-run by default."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.data_ingestion.trade_calendar_sync import sync_calendar

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--apply', action='store_true')
    args=parser.parse_args()
    print(json.dumps(sync_calendar(args.start_date,args.end_date,apply=args.apply),ensure_ascii=False))
