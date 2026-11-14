from __future__ import annotations

import re
import sqlite3
import unittest

from app.api.routes.stocks import _overview_rankings


class SqliteCursor:
    """Run the real aggregate SQL against small, isolated populations."""

    def __init__(self, db):
        self.db = db
        self.params = []

    def execute(self, sql, params):
        self.params.append(params)
        self.result = self.db.execute(sql.replace("%s", "?"), params)

    def fetchone(self):
        row = self.result.fetchone()
        return dict(row) if row is not None else None


class OverviewRankingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("REGEXP", 2, lambda pattern, value: bool(re.search(pattern, value)))
        self.db.execute("CREATE TABLE stock_realtime_snapshot(code TEXT, amount NUMERIC, pct_chg NUMERIC)")
        self.db.execute("CREATE TABLE factor_input_daily(code TEXT, trade_date TEXT, total_mv NUMERIC, turnover_rate NUMERIC, volume_ratio NUMERIC)")
        self.db.executemany("INSERT INTO stock_realtime_snapshot VALUES(?,?,?)", [
            ("sh.1", 100, -2), ("sz.2", 100, 0), ("bj.3", 200, 0),
            ("sh.4", None, None), ("etf.5", 1000, 20),
        ])
        self.db.executemany("INSERT INTO factor_input_daily VALUES(?,?,?,?,?)", [
            ("sh.1", "2026-11-13", 200, 3, None),
            ("sz.2", "2026-11-13", 200, 4, 2),
            ("bj.3", "2026-11-13", 300, 4, 2),
            ("sh.4", "2026-11-13", None, None, None),
            ("etf.5", "2026-11-13", 400, 0, 1),
            ("sh.1", "2026-11-12", 9000, 100, 100),
        ])

    def row(self, table, code, date=None):
        sql = f"SELECT * FROM {table} WHERE code = ?"
        args = [code]
        if date:
            sql += " AND trade_date = ?"
            args.append(date)
        row = self.db.execute(sql, args).fetchone()
        return dict(row) if row else None

    def reference(self, table, field, code, date=None):
        scope = "trade_date = ?" if date else r"code REGEXP '^(sh|sz|bj)\.'"
        args = [date] if date else []
        row = self.db.execute(
            f"SELECT rank_no FROM (SELECT code, RANK() OVER (ORDER BY {field} DESC) AS rank_no "
            f"FROM {table} WHERE {scope} AND {field} IS NOT NULL) r WHERE code = ?", args + [code],
        ).fetchone()
        return row[0] if row else None

    def test_real_sql_preserves_ties_nulls_population_and_date_scope(self):
        for code in ["sh.1", "sz.2", "bj.3", "sh.4", "etf.5", "missing"]:
            with self.subTest(code=code):
                factor = self.row("factor_input_daily", code, "2026-11-13") or {}
                ranks, total = _overview_rankings(SqliteCursor(self.db), code, factor)
                self.assertEqual(ranks["sample_size"], 4)
                self.assertEqual(total, 5 if factor else 0)
                for key, field in {"amount_rank": "amount", "pct_chg_rank": "pct_chg", "total_mv_rank": "total_mv", "turnover_rank": "turnover_rate", "volume_ratio_rank": "volume_ratio"}.items():
                    if field in {"amount", "pct_chg"}:
                        expected = self.reference("stock_realtime_snapshot", field, code)
                        denominator = 4
                    elif factor:
                        expected = self.reference("factor_input_daily", field, code, "2026-11-13")
                        denominator = 5
                    else:
                        self.assertNotIn(key, ranks)
                        continue
                    self.assertEqual(ranks[key], expected)
                    self.assertEqual(ranks[key.replace("_rank", "_percentile")], round((denominator - expected + 1) / denominator * 100, 2) if expected else None)

    def test_missing_target_and_empty_population_do_not_invent_rank_one(self):
        self.db.execute("DELETE FROM stock_realtime_snapshot")
        ranks, total = _overview_rankings(SqliteCursor(self.db), "sh.1", {})
        self.assertEqual(total, 0)
        self.assertEqual(ranks, {"sample_size": 0, "amount_rank": None, "amount_percentile": None, "pct_chg_rank": None, "pct_chg_percentile": None})

    def test_singleton_keeps_undefined_percentile(self):
        self.db.execute("DELETE FROM stock_realtime_snapshot WHERE code != 'sh.1'")
        ranks, _ = _overview_rankings(SqliteCursor(self.db), "sh.1", {})
        self.assertEqual(ranks["amount_rank"], 1)
        self.assertIsNone(ranks["amount_percentile"])

    def test_quote_update_uses_target_and_peers_from_same_statement(self):
        self.db.execute("UPDATE stock_realtime_snapshot SET amount = 500 WHERE code = 'sh.1'")
        cursor = SqliteCursor(self.db)
        ranks, _ = _overview_rankings(cursor, "sh.1", {})
        self.assertEqual(ranks["amount_rank"], 1)
        # No earlier Python quote/float value is used as a rank threshold.
        # The only bound value is the target code; DB decimal comparison stays
        # entirely within one statement even at READ COMMITTED isolation.
        self.assertEqual(cursor.params, [("sh.1",)])


if __name__ == "__main__":
    unittest.main()
