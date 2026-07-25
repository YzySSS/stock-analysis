from __future__ import annotations

from typing import Any, Mapping


MARKET_OPINION_TASK_SUMMARY_VERSION = "market_opinion_task_v1"
MARKET_OPINION_TASK_SECTOR_FIELDS = (
    "sector_name",
    "sector_type",
    "sector_score",
    "weighted_impact_score",
    "timeliness_score",
    "timeliness_level",
    "news_count",
    "positive_news_count",
    "negative_news_count",
    "source_count",
    "stock_count",
    "trade_date",
    "as_of_datetime",
)


def compact_market_opinion_task_metadata(
    metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Keep operational evidence while dropping duplicated normalized details."""

    compacted = dict(metadata or {})
    top_sectors = compacted.get("top_sectors")
    if isinstance(top_sectors, list):
        compacted["top_sectors"] = [
            {
                field: item.get(field)
                for field in MARKET_OPINION_TASK_SECTOR_FIELDS
                if field in item
            }
            for item in top_sectors[:8]
            if isinstance(item, Mapping)
        ]
    compacted["metadata_summary_version"] = MARKET_OPINION_TASK_SUMMARY_VERSION
    compacted["detail_storage"] = "normalized_market_opinion_tables"
    return compacted
