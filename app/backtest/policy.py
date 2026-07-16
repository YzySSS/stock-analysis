from __future__ import annotations


LEGACY_BACKTEST_METHODOLOGY_VERSION = "legacy_pre_point_in_time_v1"
BACKTEST_METHODOLOGY_VERSION = "close_signal_next_open_v2"
LEGACY_BACKTEST_RISK_NOTICE = (
    "当前回测仍存在信号时点、非 point-in-time 基本面和幸存者偏差等待修复项；"
    "结果仅用于研究与排错，不可作为交易证据。"
)
BACKTEST_RISK_NOTICE = (
    "当前回测已改为 T 日收盘形成信号、T+1 开盘成交，并排除非 point-in-time 基本面字段；"
    "历史 ST/退市证券主数据仍不完整，结果继续仅用于研究与排错，不可作为交易证据。"
)


def research_disclosure(methodology_version: str | None = None) -> dict:
    version = methodology_version or LEGACY_BACKTEST_METHODOLOGY_VERSION
    return {
        "research_only": True,
        "validation_status": "validation_pending",
        "methodology_version": version,
        "risk_notice": BACKTEST_RISK_NOTICE if version == BACKTEST_METHODOLOGY_VERSION else LEGACY_BACKTEST_RISK_NOTICE,
    }
