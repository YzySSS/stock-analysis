from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from app.shared.db import mysql_maintenance_conn


@dataclass(frozen=True)
class RedundantIndex:
    table_name: str
    redundant_index: str
    replacement_index: str


EXACT_DUPLICATE_INDEXES = (
    RedundantIndex(
        table_name="factor_input_daily",
        redundant_index="idx_factor_input_code_trade_date",
        replacement_index="uniq_factor_input_daily",
    ),
    RedundantIndex(
        table_name="index_constituent_pit",
        redundant_index="idx_index_constituent_pit_asof",
        replacement_index="uniq_index_constituent_pit",
    ),
    RedundantIndex(
        table_name="stock_fundamental_pit",
        redundant_index="idx_fundamental_pit_asof",
        replacement_index="uniq_fundamental_pit_version",
    ),
    RedundantIndex(
        table_name="strategy_forward_observation",
        redundant_index="idx_strategy_forward_observation_protocol",
        replacement_index="uniq_strategy_forward_protocol_date",
    ),
)


@dataclass(frozen=True)
class IndexDefinition:
    non_unique: bool
    index_type: str
    visible: bool
    columns: tuple[tuple[str, int | None, str | None], ...]


def _group_index_rows(rows: Iterable[dict[str, Any]]) -> dict[str, IndexDefinition]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["index_name"]), []).append(row)

    definitions: dict[str, IndexDefinition] = {}
    for index_name, index_rows in grouped.items():
        ordered_rows = sorted(index_rows, key=lambda row: int(row["seq_in_index"]))
        non_unique_values = {bool(int(row["non_unique"])) for row in ordered_rows}
        index_types = {str(row["index_type"]).upper() for row in ordered_rows}
        visible_values = {
            str(row.get("is_visible") or "YES").upper() == "YES"
            for row in ordered_rows
        }
        if (
            len(non_unique_values) != 1
            or len(index_types) != 1
            or len(visible_values) != 1
        ):
            raise RuntimeError(f"inconsistent index metadata for {index_name}")
        definitions[index_name] = IndexDefinition(
            non_unique=non_unique_values.pop(),
            index_type=index_types.pop(),
            visible=visible_values.pop(),
            columns=tuple(
                (
                    str(row["column_name"]),
                    int(row["sub_part"]) if row.get("sub_part") is not None else None,
                    str(row["collation"]) if row.get("collation") is not None else None,
                )
                for row in ordered_rows
            ),
        )
    return definitions


def _validate_candidate(
    candidate: RedundantIndex,
    definitions: dict[str, IndexDefinition],
) -> str:
    replacement = definitions.get(candidate.replacement_index)
    if replacement is None:
        raise RuntimeError(
            f"{candidate.table_name}: replacement index "
            f"{candidate.replacement_index} is missing"
        )
    if replacement.non_unique:
        raise RuntimeError(
            f"{candidate.table_name}: replacement index "
            f"{candidate.replacement_index} is not unique"
        )
    if not replacement.visible:
        raise RuntimeError(
            f"{candidate.table_name}: replacement index "
            f"{candidate.replacement_index} is not visible"
        )

    redundant = definitions.get(candidate.redundant_index)
    if redundant is None:
        return "already_absent"
    if not redundant.non_unique:
        raise RuntimeError(
            f"{candidate.table_name}: candidate index "
            f"{candidate.redundant_index} is unique"
        )
    if redundant.index_type != replacement.index_type:
        raise RuntimeError(
            f"{candidate.table_name}: index types differ between "
            f"{candidate.redundant_index} and {candidate.replacement_index}"
        )
    if redundant.columns != replacement.columns:
        raise RuntimeError(
            f"{candidate.table_name}: ordered index columns differ between "
            f"{candidate.redundant_index} and {candidate.replacement_index}"
        )
    return "drop"


def drop_exact_duplicate_indexes() -> dict[str, Any]:
    dropped: list[dict[str, str]] = []
    already_absent: list[dict[str, str]] = []

    with mysql_maintenance_conn(dict_cursor=True, timeout_seconds=600) as conn:
        with conn.cursor() as cursor:
            for candidate in EXACT_DUPLICATE_INDEXES:
                cursor.execute(
                    """
                    SELECT
                        INDEX_NAME AS index_name,
                        NON_UNIQUE AS non_unique,
                        SEQ_IN_INDEX AS seq_in_index,
                        COLUMN_NAME AS column_name,
                        SUB_PART AS sub_part,
                        COLLATION AS collation,
                        INDEX_TYPE AS index_type,
                        IS_VISIBLE AS is_visible
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA=DATABASE()
                      AND TABLE_NAME=%s
                      AND INDEX_NAME IN (%s, %s)
                    ORDER BY INDEX_NAME, SEQ_IN_INDEX
                    """,
                    (
                        candidate.table_name,
                        candidate.redundant_index,
                        candidate.replacement_index,
                    ),
                )
                definitions = _group_index_rows(cursor.fetchall() or [])
                action = _validate_candidate(candidate, definitions)
                item = {
                    "table": candidate.table_name,
                    "redundant_index": candidate.redundant_index,
                    "replacement_index": candidate.replacement_index,
                }
                if action == "already_absent":
                    already_absent.append(item)
                    continue

                cursor.execute(
                    f"ALTER TABLE `{candidate.table_name}` "
                    f"DROP INDEX `{candidate.redundant_index}`, "
                    "ALGORITHM=INPLACE, LOCK=NONE"
                )
                dropped.append(item)

    return {
        "status": "ok",
        "dropped": dropped,
        "already_absent": already_absent,
        "candidate_count": len(EXACT_DUPLICATE_INDEXES),
    }
