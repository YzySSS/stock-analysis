from __future__ import annotations


LEGACY_BACKTEST_METHODOLOGY_VERSION = "legacy_pre_point_in_time_v1"
PREVIOUS_BACKTEST_METHODOLOGY_VERSION = "close_signal_next_open_v2"
PREVIOUS_PIT_UNIVERSE_METHODOLOGY_VERSION = "close_signal_next_open_pit_universe_v3"
PREVIOUS_PIT_FUNDAMENTAL_METHODOLOGY_VERSION = "close_signal_next_open_pit_fundamentals_v4"
BACKTEST_METHODOLOGY_VERSION = "close_signal_next_open_pit_index_universe_v5"
LEGACY_BACKTEST_RISK_NOTICE = (
    "当前回测仍存在信号时点、非 point-in-time 基本面和幸存者偏差等待修复项；"
    "结果仅用于研究与排错，不可作为交易证据。"
)
PREVIOUS_BACKTEST_RISK_NOTICE = (
    "当前回测已改为 T 日收盘形成信号、T+1 开盘成交，并排除非 point-in-time 基本面字段；"
    "历史 ST/退市证券主数据仍不完整，结果继续仅用于研究与排错，不可作为交易证据。"
)
PREVIOUS_PIT_UNIVERSE_RISK_NOTICE = (
    "当前回测采用 T 日收盘信号、T+1 开盘成交，排除非 point-in-time 基本面字段，"
    "并按历史上市/退市生命周期及名称/ST 区间构造股票池；"
    "状态源完整性仍以 DQ3 审计为准，且尚未完成样本外验证，结果仅用于研究与排错。"
)
PREVIOUS_PIT_FUNDAMENTAL_RISK_NOTICE = (
    "当前回测采用 T 日收盘信号、T+1 开盘成交，按历史上市/退市生命周期及名称/ST 区间构造股票池，"
    "估值使用 T 日 daily_basic，财务指标只使用公告日不晚于信号日的 point-in-time 版本；"
    "完整性仍以 DQ4 审计为准，且尚未完成样本外验证，结果仅用于研究与排错。"
)
BACKTEST_RISK_NOTICE = (
    "当前回测采用 T 日收盘信号、T+1 开盘成交，并使用历史生命周期、公告日基本面；"
    "选择指数股票池时只使用信号日之前最近一次 Tushare 月度成分权重快照，默认历史全A口径不变；"
    "完整性仍以 DQ5 审计为准，且尚未完成样本外验证，结果仅用于研究与排错。"
)


def research_disclosure(methodology_version: str | None = None) -> dict:
    version = methodology_version or LEGACY_BACKTEST_METHODOLOGY_VERSION
    return {
        "research_only": True,
        "validation_status": "validation_pending",
        "methodology_version": version,
        "risk_notice": (
            BACKTEST_RISK_NOTICE
            if version == BACKTEST_METHODOLOGY_VERSION
            else PREVIOUS_PIT_FUNDAMENTAL_RISK_NOTICE
            if version == PREVIOUS_PIT_FUNDAMENTAL_METHODOLOGY_VERSION
            else PREVIOUS_PIT_UNIVERSE_RISK_NOTICE
            if version == PREVIOUS_PIT_UNIVERSE_METHODOLOGY_VERSION
            else PREVIOUS_BACKTEST_RISK_NOTICE
            if version == PREVIOUS_BACKTEST_METHODOLOGY_VERSION
            else LEGACY_BACKTEST_RISK_NOTICE
        ),
    }
