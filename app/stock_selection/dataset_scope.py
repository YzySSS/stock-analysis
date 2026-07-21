from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any


# Tushare's daily ``moneyflow`` dataset does not publish Beijing Stock
# Exchange rows.  A strategy that hard-requires this dataset must therefore
# audit and execute on the supported Shanghai/Shenzhen universe.  Unsupported
# markets remain fail-closed; no synthetic zero-flow rows are created.
DATASET_SUPPORTED_CODE_PREFIXES: Mapping[str, tuple[str, ...]] = {
    "stock_moneyflow_daily": ("sh.", "sz."),
}


def required_dataset_code_prefixes(
    required_datasets: Iterable[str],
) -> tuple[str, ...]:
    scopes = [
        set(DATASET_SUPPORTED_CODE_PREFIXES[dataset_name])
        for dataset_name in required_datasets
        if dataset_name in DATASET_SUPPORTED_CODE_PREFIXES
    ]
    if not scopes:
        return ()
    supported = set.intersection(*scopes)
    if not supported:
        raise ValueError("required datasets have no common supported stock universe")
    return tuple(sorted(supported))


def sql_code_prefix_filter(
    code_expression: str,
    prefixes: Sequence[str],
) -> str:
    if not prefixes:
        return ""
    if code_expression != "sb.code":
        raise ValueError("unsupported stock-code SQL expression")
    normalized = tuple(str(value).strip().lower() for value in prefixes)
    allowed = {"sh.", "sz.", "bj."}
    if not normalized or any(value not in allowed for value in normalized):
        raise ValueError("unsupported stock-code prefix")
    quoted = ", ".join(f"'{value}'" for value in normalized)
    return f" AND LEFT({code_expression}, 3) IN ({quoted})"


def filter_rows_to_code_prefixes(
    rows: Iterable[Mapping[str, Any]],
    prefixes: Sequence[str],
) -> list[dict[str, Any]]:
    normalized = tuple(str(value).strip().lower() for value in prefixes if str(value).strip())
    values = [dict(row) for row in rows]
    if not normalized:
        return values
    return [
        row
        for row in values
        if str(row.get("code") or "").strip().lower().startswith(normalized)
    ]
