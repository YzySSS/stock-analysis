from __future__ import annotations

import ast
import inspect
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app.data_ingestion.news_credibility import NewsCredibilityChecker
from app.data_ingestion.news_filter import NewsFilter
from app.data_ingestion.news_provider import extract_date_from_text, source_from_url
from app.data_ingestion.portfolio_etf_quote_sync import save_snapshot
from app.data_ingestion.sentiment_sync import LocalSentimentScorer
from app.stock_selection import run_selection


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DependencyBoundaryTests(unittest.TestCase):
    def test_app_does_not_import_scripts_or_src(self):
        violations = []
        for path in sorted((PROJECT_ROOT / "app").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if module == "scripts" or module.startswith("scripts.") or module == "src" or module.startswith("src."):
                        violations.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}:{module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "scripts" or alias.name.startswith("scripts.") or alias.name == "src" or alias.name.startswith("src."):
                            violations.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}:{alias.name}")
            source = path.read_text(encoding="utf-8")
            if "scripts." in source or "SRC_ROOT" in source:
                violations.append(f"{path.relative_to(PROJECT_ROOT)}:textual reverse dependency")
        self.assertEqual(violations, [])

    def test_migrated_cli_files_are_thin_launchers(self):
        expected_imports = {
            "run_portfolio_etf_quote_update.py": "app.data_ingestion.portfolio_etf_quote_sync",
            "run_sentiment_daily_update.py": "app.data_ingestion.stock_sentiment_daily_job",
            "run_strategy_sentiment_refresh.py": "app.data_ingestion.strategy_sentiment_refresh_job",
        }
        for filename, module in expected_imports.items():
            path = PROJECT_ROOT / "scripts" / filename
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            functions = [node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
            self.assertEqual(functions, [], filename)
            self.assertIn(f"from {module} import main", source)

    def test_migrated_sentiment_helpers_match_frozen_behavior(self):
        checker = NewsCredibilityChecker()
        news_filter = NewsFilter(min_credibility=0.35, max_age_days=7)
        scorer = LocalSentimentScorer()

        self.assertEqual(source_from_url("https://www.eastmoney.com/a/1"), "eastmoney.com")
        self.assertEqual(source_from_url("bad", "fallback"), "fallback")
        self.assertEqual(extract_date_from_text("公告于2026年7月15日发布"), "2026-07-15")
        self.assertEqual(checker.check_credibility("https://www.sse.com.cn/a", "公告").score, 95)
        self.assertEqual(news_filter.get_source_credibility("财联社"), 0.9)
        self.assertEqual(scorer.score("业绩大增 资金流入"), 2 / 3)
        self.assertEqual(scorer.score("立案调查 亏损扩大"), -1.0)
        self.assertEqual(scorer.score("普通公告"), 0.0)

    def test_etf_snapshot_generates_batch_id_for_service_call(self):
        class FakeCursor:
            def __init__(self):
                self.calls = []

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, sql, params=None):
                self.calls.append((sql, params))
                return 1

            def fetchone(self):
                return {"name": "沪深300ETF"}

            @property
            def rowcount(self):
                return 1

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

        cursor = FakeCursor()

        @contextmanager
        def fake_mysql_conn(*_args, **_kwargs):
            yield FakeConnection(cursor)

        row = {
            "code": "sh.510300",
            "source_code": "510300",
            "trade_date": "2026-07-15",
            "quote_time": "2026-07-15 15:00:00",
            "close": 4.15,
            "change_amount": 0.05,
            "pct_chg": 1.2,
            "pre_close": 4.10,
            "open": 4.1,
            "high": 4.2,
            "low": 4.0,
            "volume": 123,
            "amount": 456.0,
            "source": "portfolio_etf_quote",
        }
        with patch("app.data_ingestion.portfolio_etf_quote_sync.mysql_conn", fake_mysql_conn):
            result = save_snapshot("sh.510300", [row])

        self.assertEqual(result, 1)
        insert_params = cursor.calls[-1][1]
        self.assertRegex(insert_params[14], r"^portfolio_etf_quote_\d{8}_\d{6}$")
        self.assertNotIn("scripts", inspect.getsource(save_snapshot))

    def test_selection_cli_submits_worker_task_instead_of_running_selector(self):
        submitted = {
            "run_id": "selection_task_test",
            "status": "queued",
        }
        with (
            patch.object(run_selection, "SelectionRunService") as service_class,
            patch.object(
                sys,
                "argv",
                [
                    "run_selection",
                    "--strategy",
                    "lowvol_reversal",
                    "--limit",
                    "4",
                    "--score-threshold",
                    "60",
                ],
            ),
            patch("builtins.print"),
        ):
            service_class.return_value.submit.return_value = submitted
            run_selection.main()

        service_class.return_value.submit.assert_called_once_with(
            {
                "strategy_id": "lowvol_reversal",
                "limit": 4,
                "max_picks": 4,
                "score_threshold": 60.0,
                "instrument_type": "stock",
                "market_board": None,
                "save": False,
            }
        )
        selector_source = (PROJECT_ROOT / "app" / "stock_selection" / "selector.py").read_text(encoding="utf-8")
        main_block = selector_source.rsplit('if __name__ == "__main__":', 1)[-1]
        self.assertIn("Direct synchronous selection is disabled", main_block)
        self.assertNotIn("run_from_mysql", main_block)

    def test_unrouted_web_page_and_grid_prototype_are_archived(self):
        self.assertFalse((PROJECT_ROOT / "app" / "api" / "web" / "index.html").exists())
        self.assertTrue((PROJECT_ROOT / "archive" / "legacy_web_index.html").exists())
        self.assertTrue((PROJECT_ROOT / "archive" / "legacy_grid_trader.py").exists())
        grid_wrapper = (PROJECT_ROOT / "src" / "grid_trader.py").read_text(encoding="utf-8")
        self.assertIn("archive.legacy_grid_trader", grid_wrapper)
        self.assertNotIn("class ETFGridTrader", grid_wrapper)


if __name__ == "__main__":
    unittest.main()
