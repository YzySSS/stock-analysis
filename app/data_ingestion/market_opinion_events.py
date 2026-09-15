from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

from app.data_ingestion.market_opinion_semantics import (
    classify_opinion_direction,
    normalize_opinion_text,
)
from app.shared.sentiment_scoring import score_source
from app.shared.market_clock import to_shanghai_wall_clock


EVENT_IDENTITY_VERSION = "sentiment-v06-event-id-v1"
EVIDENCE_RULE_VERSION = "sentiment-v06-evidence-v1"
CREDIBILITY_RULE_VERSION = "sentiment-v06-source-v1"
RELATION_RULE_VERSION = "sentiment-v06-relation-v1"

CONTENT_ROLES = frozenset(
    {
        "original_catalyst",
        "catalyst_update",
        "market_reaction",
        "opinion_repost",
        "risk_event",
    }
)
ACTIONABLE_CONTENT_ROLES = frozenset({"original_catalyst", "catalyst_update"})
CONFIRMED_EVENT_STATES = frozenset({"confirmed_primary", "confirmed_multi_source"})
CONFIRMED_RELATION_STATES = frozenset({"confirmed", "confirmed_primary"})

PRIMARY_SOURCE_KEYS = frozenset(
    {
        "announcement",
        "company_announcement",
        "company",
        "exchange",
        "regulator",
        "government",
        "official",
    }
)
RISK_PATTERN = re.compile(
    r"处罚|立案|造假|重大减持|业绩暴雷|退市|问询|事故|召回|停产|制裁|禁令|亏损|下修",
    re.I,
)
MARKET_REACTION_PATTERN = re.compile(
    r"涨停|跌停|拉升|跳水|异动|大涨|大跌|走强|走弱|冲高回落|资金流入|净流入|主力",
    re.I,
)
OPINION_PATTERN = re.compile(
    r"研报|看好|建议关注|观点|点评|认为|预计|有望|或将|转载|转引|据媒体|报道称|传闻|网传",
    re.I,
)
UPDATE_PATTERN = re.compile(
    r"进展|新增|后续|完成|落地|正式|上调|下调|修订|二期|三期|再次|续签",
    re.I,
)
FACT_PATTERN = re.compile(
    r"公告|签署|签订|中标|订单|合同|采购|业绩|利润|营收|预增|扭亏|政策|印发|实施|"
    r"获批|量产|投产|专利|临床|产品|技术|重组|收购|并购|回购|增持|减持",
    re.I,
)
BUSINESS_RELATION_PATTERN = re.compile(
    r"公司|子公司|业务|产品|客户|供应商|产业链|合同|订单|中标|项目|研发|技术|"
    r"量产|投产|营收|利润|公告|合作|持股|参股",
    re.I,
)
MILESTONE_PATTERN = re.compile(
    r"(?:交付|投产|量产|实施|验收|获批|临床|开工|投标|解禁|到期|续签|业绩发布)[^，。；;]{0,20}",
    re.I,
)
NUMBER_PATTERN = re.compile(
    r"\d+(?:\.\d+)?(?:万|亿|%|％|万元|亿元|万股|亿股|吨|台|套|份|年|月|日)?",
    re.I,
)
REACTION_NOISE_PATTERN = re.compile(
    r"(?:A股|港股|盘中|午后|早盘|尾盘|今日|昨日)?(?:股价|股票|个股|板块)?"
    r"(?:涨停|跌停|拉升|跳水|异动|大涨|大跌|走强|走弱|冲高回落|资金流入|净流入)[^，。；;]{0,16}",
    re.I,
)
REPORTING_NOISE_PATTERN = re.compile(
    r"^(?:快讯|消息|据悉|据媒体报道|报道称|财联社|格隆汇|证券时报|上证报)[:：丨｜\-—]*",
    re.I,
)
REPORTING_TOKEN_PATTERN = re.compile(
    r"(?:快讯|消息|据悉|据媒体报道|报道称|财联社|格隆汇|证券时报|上证报)[:：丨｜\-—]*",
    re.I,
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return round(max(low, min(float(value), high)), 4)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return to_shanghai_wall_clock(parsed)


def _datetime_text(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value is not None else None


def _first_present(row: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None and value != "":
            return value
    return None


def _normalized_source_key(row: Mapping[str, Any]) -> str:
    value = _first_present(
        row,
        ("original_publisher", "source_id", "publisher", "source", "source_name"),
    )
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").strip().lower())


def is_primary_source(row: Mapping[str, Any]) -> bool:
    if bool(row.get("is_primary_source")):
        return True
    values = {
        str(row.get("source_id") or "").strip().lower(),
        str(row.get("source_type") or "").strip().lower(),
        str(row.get("publisher_type") or "").strip().lower(),
    }
    return bool(values & PRIMARY_SOURCE_KEYS)


def classify_content_role(
    *,
    title: str | None,
    summary: str | None = None,
    event_type: str | None = None,
    direction: str | None = None,
    explicit_role: str | None = None,
) -> str:
    """Classify what an article contributes to an event.

    Market reaction and opinion items remain auditable evidence, but they never
    manufacture a business catalyst. Explicit roles are accepted only from the
    closed, versioned vocabulary.
    """

    normalized_explicit = str(explicit_role or "").strip().lower()
    if normalized_explicit in CONTENT_ROLES:
        return normalized_explicit
    text = normalize_opinion_text(f"{title or ''}。{summary or ''}")
    normalized_direction = str(direction or "").strip().lower() or classify_opinion_direction(
        title, summary
    )
    normalized_type = str(event_type or "general").strip().lower()
    if normalized_direction == "negative" and (
        RISK_PATTERN.search(text) or normalized_type in {"negative_risk", "short_lived_risk"}
    ):
        return "risk_event"
    has_fact = bool(FACT_PATTERN.search(text))
    has_reaction = bool(MARKET_REACTION_PATTERN.search(text))
    if has_reaction and not has_fact:
        return "market_reaction"
    if UPDATE_PATTERN.search(text) and has_fact:
        return "catalyst_update"
    if has_fact and normalized_type not in {"market_attention", "hot_theme", "general"}:
        return "original_catalyst"
    if has_fact and re.search(r"公告|签署|签订|中标|获批|印发|实施", text, re.I):
        return "original_catalyst"
    if OPINION_PATTERN.search(text) or not has_fact:
        return "opinion_repost"
    return "opinion_repost"


def infer_event_type(
    title: str | None,
    summary: str | None = None,
    explicit: str | None = None,
) -> str:
    normalized = str(explicit or "").strip().lower()
    if normalized and normalized != "general":
        return normalized
    text = normalize_opinion_text(f"{title or ''}。{summary or ''}")
    patterns = (
        ("negative_risk", RISK_PATTERN),
        ("major_order", re.compile(r"订单|中标|签约|签署|合同|采购", re.I)),
        ("earnings", re.compile(r"业绩|利润|营收|预增|扭亏|分红", re.I)),
        ("policy", re.compile(r"政策|印发|实施方案|国务院|发改委|工信部|证监会", re.I)),
        ("tech_breakthrough", re.compile(r"突破|量产|投产|获批|临床|专利|研发", re.I)),
        ("ma_restructure", re.compile(r"并购|重组|收购|资产注入|定增", re.I)),
        ("price_hike", re.compile(r"涨价|提价|供不应求|库存下降", re.I)),
        ("market_attention", MARKET_REACTION_PATTERN),
    )
    for name, pattern in patterns:
        if pattern.search(text):
            return name
    return "general"


def _event_anchor_tokens(text: str, event_type: str) -> list[str]:
    anchors: dict[str, tuple[str, ...]] = {
        "major_order": ("订单", "中标", "签约", "合同", "采购"),
        "earnings": ("业绩", "利润", "营收", "预增", "扭亏", "分红"),
        "policy": ("政策", "印发", "实施", "试点", "补贴"),
        "tech_breakthrough": ("突破", "量产", "投产", "获批", "临床", "专利"),
        "ma_restructure": ("并购", "重组", "收购", "注入", "定增"),
        "negative_risk": ("处罚", "立案", "造假", "减持", "退市", "亏损"),
        "short_lived_risk": ("事故", "召回", "停产", "跌停"),
        "market_attention": ("涨停", "跌停", "拉升", "异动", "冲高回落"),
    }
    return sorted({token for token in anchors.get(event_type, ()) if token in text})


def _milestone_categories(text: str) -> list[str]:
    categories = (
        ("signed", ("签署", "签订", "签约")),
        ("awarded", ("中标",)),
        ("delivery", ("交付",)),
        ("production_started", ("投产", "量产")),
        ("implemented", ("实施",)),
        ("accepted", ("验收",)),
        ("approved", ("获批",)),
        ("clinical", ("临床",)),
        ("construction_started", ("开工",)),
        ("unlock", ("解禁",)),
        ("renewed", ("续签",)),
        ("terminated", ("终止", "取消")),
    )
    return sorted(
        {
            category
            for category, aliases in categories
            if any(alias in text for alias in aliases)
        }
    )


def _fact_core(title: str | None, summary: str | None) -> str:
    text = normalize_opinion_text(f"{title or ''}。{summary or ''}")
    text = REPORTING_NOISE_PATTERN.sub("", text)
    text = REACTION_NOISE_PATTERN.sub("", text)
    return text[:600]


def _identity_semantic_core(title: str | None, summary: str | None) -> str:
    """Keep distinct facts apart while tolerating common wire retitles.

    Numeric facts alone are not a safe identity key: two unrelated policies
    can have no numbers, and one company can disclose similarly sized events
    on the same day. Stage verbs stay in the revision contract, while this
    identity form normalizes only narrow retitle synonyms.
    """

    text = REPORTING_TOKEN_PATTERN.sub("", _fact_core(title, summary))
    substitutions = (
        (r"签署|签订|签约|中标", "事项阶段"),
        (r"订单|合同|采购", "业务事项"),
        (r"印发|发布|出台", "正式发布"),
    )
    for pattern, replacement in substitutions:
        text = re.sub(pattern, replacement, text, flags=re.I)
    text = text.replace("公告", "")
    return re.sub(r"[\s，,。；;:：丨｜\-—]+", "", text)[:500]


def event_identity_material(
    evidence: Mapping[str, Any],
    *,
    stock_codes: Sequence[str] = (),
    sectors: Sequence[str] = (),
    event_type: str | None = None,
    content_role: str | None = None,
) -> dict[str, Any]:
    source_time = _parse_datetime(
        _first_present(evidence, ("source_time", "published_at", "event_time"))
    )
    received_at = _parse_datetime(
        _first_present(evidence, ("received_at", "first_seen_at", "crawl_time"))
    )
    event_day = (source_time or received_at or datetime(1970, 1, 1)).date().isoformat()
    title = str(evidence.get("title") or "")
    summary = str(evidence.get("summary") or "")
    normalized_type = event_type or infer_event_type(
        title, summary, str(evidence.get("event_type") or "")
    )
    normalized_role = content_role or classify_content_role(
        title=title,
        summary=summary,
        event_type=normalized_type,
        direction=str(evidence.get("direction") or ""),
        explicit_role=str(evidence.get("content_role") or ""),
    )
    core = _fact_core(title, summary)
    return {
        "identity_version": EVENT_IDENTITY_VERSION,
        "event_date": event_day,
        "event_type": normalized_type,
        "role_family": (
            "risk"
            if normalized_role == "risk_event"
            else "reaction"
            if normalized_role == "market_reaction"
            else "catalyst"
            if normalized_role in ACTIONABLE_CONTENT_ROLES
            else "opinion"
        ),
        "stock_codes": sorted({str(value).strip() for value in stock_codes if str(value).strip()}),
        "sectors": sorted({str(value).strip() for value in sectors if str(value).strip()}),
        "anchors": _event_anchor_tokens(core, normalized_type),
        "numbers": sorted(set(NUMBER_PATTERN.findall(core))),
        "milestones": sorted(set(MILESTONE_PATTERN.findall(core))),
        "semantic_core": _identity_semantic_core(title, summary),
    }


def canonical_event_id(
    evidence: Mapping[str, Any],
    *,
    stock_codes: Sequence[str] = (),
    sectors: Sequence[str] = (),
) -> str:
    explicit = str(evidence.get("canonical_event_id") or evidence.get("event_identity_hint") or "").strip()
    if explicit:
        return explicit if re.fullmatch(r"[0-9a-f]{64}", explicit, re.I) else _digest({"explicit": explicit})
    material = event_identity_material(
        evidence,
        stock_codes=stock_codes,
        sectors=sectors,
    )
    # Synonymous verbs (for example “中标/签订合同”) frequently change when a
    # wire item is retitled. Keep those anchors in audit metadata, but base the
    # deterministic identity on subject, day, event family and disclosed
    # numeric facts. Ambiguous no-number events remain pending for review.
    return _digest(
        {
            key: material[key]
            for key in (
                "identity_version",
                "event_date",
                "event_type",
                "role_family",
                "stock_codes",
                "sectors",
                "numbers",
                "semantic_core",
            )
        }
    )


def _driver_contract(
    *,
    text: str,
    event_type: str,
    content_role: str,
) -> dict[str, Any]:
    milestone = MILESTONE_PATTERN.search(text)
    if content_role in {"market_reaction", "opinion_repost"}:
        horizon = "none"
        persistence = 0.0
        one_off = True
    elif content_role == "risk_event":
        horizon = "until_resolved"
        persistence = 72.0 if re.search(r"立案|处罚|退市|停产|禁令", text) else 55.0
        one_off = False
    elif event_type in {"policy", "tech_breakthrough", "ma_restructure"}:
        horizon = "medium_term"
        persistence = 78.0 if milestone else 66.0
        one_off = False
    elif event_type == "major_order":
        horizon = "staged" if re.search(r"交付|履行|合同期|项目期|分期", text) else "one_off"
        persistence = 76.0 if horizon == "staged" else 48.0
        one_off = horizon == "one_off"
    elif event_type == "earnings":
        horizon = "one_off"
        persistence = 42.0
        one_off = True
    else:
        horizon = "unknown"
        persistence = 35.0
        one_off = False
    return {
        "driver_horizon": horizon,
        "next_milestone": milestone.group(0)[:255] if milestone else None,
        "is_one_off": one_off,
        "is_terminal": bool(re.search(r"终止|取消|失败|否决|到期不续", text)),
        "persistence_score": _clamp(persistence),
    }


def normalize_event_evidence(
    evidence: Mapping[str, Any],
    *,
    stock_codes: Sequence[str] = (),
    sectors: Sequence[str] = (),
    decision_as_of: Any = None,
    snapshot_as_of: Any = None,
) -> dict[str, Any]:
    """Normalize one article into a deterministic, point-in-time evidence row."""

    title = str(evidence.get("title") or evidence.get("summary") or "").strip()
    summary = str(evidence.get("summary") or "").strip() or None
    event_type = infer_event_type(title, summary, str(evidence.get("event_type") or ""))
    direction = str(evidence.get("direction") or "").strip().lower()
    if direction not in {"positive", "negative", "neutral"}:
        direction = classify_opinion_direction(title, summary)
    if direction == "neutral" and event_type in {
        "major_order",
        "policy",
        "tech_breakthrough",
        "ma_restructure",
        "price_hike",
    }:
        direction = "positive"
    role = classify_content_role(
        title=title,
        summary=summary,
        event_type=event_type,
        direction=direction,
        explicit_role=str(evidence.get("content_role") or ""),
    )
    source_time = _parse_datetime(
        _first_present(evidence, ("source_time", "published_at", "event_time"))
    )
    received_at = _parse_datetime(
        _first_present(evidence, ("received_at", "first_seen_at", "crawl_time"))
    )
    if received_at is None:
        # A normalized sector snapshot proves only that evidence was available
        # no later than the snapshot time. It must never be backdated to the
        # article's claimed publication timestamp.
        received_at = _parse_datetime(snapshot_as_of)
        received_source = "snapshot_upper_bound" if received_at else "missing"
    else:
        received_source = "source_received_at"
    available_at = max(
        [value for value in (source_time, received_at) if value is not None],
        default=None,
    )
    decision_time = _parse_datetime(decision_as_of)
    after_decision_fields: list[str] = []
    if decision_time is not None:
        if source_time is not None and source_time > decision_time:
            after_decision_fields.append("source_time")
        if received_at is not None and received_at > decision_time:
            after_decision_fields.append("received_at")
    if received_at is None:
        availability_status = "missing_received_at"
    elif after_decision_fields:
        availability_status = "after_decision"
    else:
        availability_status = "available"

    source_key = _normalized_source_key(evidence)
    explicit_credibility = _to_float(
        _first_present(evidence, ("credibility_score", "source_credibility_score"))
    )
    if explicit_credibility is not None and explicit_credibility > 1:
        explicit_credibility /= 100.0
    source_rating = score_source(
        evidence.get("source_id") or evidence.get("source") or evidence.get("source_name")
    )
    credibility = (
        _clamp(explicit_credibility, 0.0, 1.0)
        if explicit_credibility is not None
        else float(source_rating["credibility_score"])
    )
    impact = _to_float(evidence.get("impact_score"))
    if impact is not None:
        impact = _clamp(impact)
    expectation_value = _to_float(
        _first_present(evidence, ("expectation_value", "consensus_value"))
    )
    actual_value = _to_float(
        _first_present(evidence, ("actual_value", "reported_value"))
    )
    expectation_unit = str(evidence.get("expectation_unit") or "").strip()
    actual_unit = str(evidence.get("actual_unit") or "").strip()
    units_comparable = bool(
        expectation_unit and actual_unit and expectation_unit == actual_unit
    )
    if expectation_value is None:
        expectation_status = "unknown"
        surprise_pct = None
    elif actual_value is None:
        expectation_status = "actual_missing"
        surprise_pct = None
    elif not units_comparable:
        expectation_status = "unit_not_comparable"
        surprise_pct = None
    else:
        expectation_status = "comparable"
        surprise_pct = (
            (actual_value - expectation_value) / abs(expectation_value) * 100.0
            if expectation_value != 0
            else None
        )
    primary = is_primary_source(evidence)
    event_id = canonical_event_id(
        {**dict(evidence), "event_type": event_type, "content_role": role},
        stock_codes=stock_codes,
        sectors=sectors,
    )
    identity = event_identity_material(
        {**dict(evidence), "event_type": event_type, "content_role": role},
        stock_codes=stock_codes,
        sectors=sectors,
        event_type=event_type,
        content_role=role,
    )
    fact_core = _fact_core(title, summary)
    revision_material = {
        "event_type": event_type,
        "direction": direction,
        "stock_codes": identity["stock_codes"],
        "sectors": identity["sectors"],
        "numbers": sorted(set(NUMBER_PATTERN.findall(fact_core))),
        "milestone_categories": _milestone_categories(fact_core),
        "terminal": bool(re.search(r"终止|取消|失败|否决|到期不续", fact_core)),
    }
    revision_hash = _digest(revision_material)
    raw_identity = {
        "source": source_key,
        "raw_id": evidence.get("raw_id"),
        "original_news_id": evidence.get("original_news_id") or evidence.get("item_id"),
        "url": evidence.get("url") or evidence.get("source_url"),
        "title": title,
        "source_time": _datetime_text(source_time),
    }
    evidence_id = str(evidence.get("evidence_id") or "").strip() or _digest(raw_identity)
    effective_until = _parse_datetime(evidence.get("effective_until"))
    driver = _driver_contract(text=fact_core, event_type=event_type, content_role=role)
    return {
        **dict(evidence),
        "canonical_event_id": event_id,
        "event_revision": int(evidence.get("event_revision") or 1),
        "revision_hash": revision_hash,
        "evidence_id": evidence_id,
        "identity_version": EVENT_IDENTITY_VERSION,
        "evidence_rule_version": EVIDENCE_RULE_VERSION,
        "credibility_rule_version": CREDIBILITY_RULE_VERSION,
        "event_type": event_type,
        "content_role": role,
        "direction": direction,
        "confirmation_status": "confirmed_primary" if primary else "pending",
        "source_id": evidence.get("source_id") or evidence.get("source"),
        "original_publisher": evidence.get("original_publisher") or evidence.get("source_name") or evidence.get("source"),
        "publisher_key": source_key or None,
        "collection_channel": evidence.get("collection_channel") or evidence.get("source_id"),
        "source_type": evidence.get("source_type"),
        "credibility_score": round(credibility, 4),
        "impact_score": impact,
        "expectation_status": expectation_status,
        "expectation_value": expectation_value,
        "actual_value": actual_value,
        "expectation_unit": expectation_unit or None,
        "actual_unit": actual_unit or None,
        "surprise_pct": round(surprise_pct, 4)
        if surprise_pct is not None
        else None,
        "is_primary_source": primary,
        "source_time": _datetime_text(source_time),
        "published_at": _datetime_text(_parse_datetime(evidence.get("published_at")) or source_time),
        "received_at": _datetime_text(received_at),
        "received_at_source": received_source,
        "available_at": _datetime_text(available_at),
        "availability_status": availability_status,
        "after_decision_fields": after_decision_fields,
        "eligible_at_decision": availability_status == "available",
        "effective_until": _datetime_text(effective_until),
        "identity_material": identity,
        "revision_material": revision_material,
        "raw_payload_hash": _digest(dict(evidence)),
        **driver,
    }


def aggregate_event_evidence(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse repeated articles into bounded canonical events.

    Independent confirmations improve confidence only up to a fixed cap. They
    do not increase the catalyst count or persistence score.
    """

    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        event_id = str(row.get("canonical_event_id") or "").strip()
        if not event_id:
            continue
        grouped.setdefault(event_id, []).append(row)

    events: list[dict[str, Any]] = []
    for event_id, evidence_rows in sorted(grouped.items()):
        evidence_rows.sort(
            key=lambda row: (
                str(row.get("available_at") or ""),
                str(row.get("evidence_id") or ""),
            )
        )
        available_rows = [row for row in evidence_rows if row.get("eligible_at_decision")]
        scoring_rows = available_rows or []
        publishers = {
            str(row.get("publisher_key") or "")
            for row in scoring_rows
            if str(row.get("publisher_key") or "")
        }
        independent_rows = [
            row
            for row in scoring_rows
            if not row.get("repost_of_evidence_id")
            and row.get("content_role")
            in {*ACTIONABLE_CONTENT_ROLES, "risk_event"}
        ]
        independent_publishers = {
            str(row.get("publisher_key") or "")
            for row in independent_rows
            if str(row.get("publisher_key") or "")
        }
        credible_publishers = {
            str(row.get("publisher_key") or "")
            for row in independent_rows
            if str(row.get("publisher_key") or "")
            and (_to_float(row.get("credibility_score")) or 0.0) >= 0.62
        }
        has_primary = any(bool(row.get("is_primary_source")) for row in independent_rows)
        if has_primary:
            confirmation = "confirmed_primary"
        elif len(credible_publishers) >= 2:
            confirmation = "confirmed_multi_source"
        else:
            confirmation = "pending"
        unique_revisions = {
            str(row.get("revision_hash") or "")
            for row in scoring_rows
            if str(row.get("revision_hash") or "")
        }
        best = (
            max(
                scoring_rows,
                key=lambda row: (
                    bool(row.get("is_primary_source")),
                    _to_float(row.get("credibility_score")) or -1.0,
                    _to_float(row.get("impact_score")) or -1.0,
                    str(row.get("available_at") or ""),
                ),
            )
            if scoring_rows
            else {}
        )
        roles = {str(row.get("content_role") or "") for row in scoring_rows}
        if "risk_event" in roles:
            event_role = "risk_event"
        elif "catalyst_update" in roles:
            event_role = "catalyst_update"
        elif "original_catalyst" in roles:
            event_role = "original_catalyst"
        elif "market_reaction" in roles:
            event_role = "market_reaction"
        else:
            event_role = "opinion_repost"
        credibility_values = [
            value
            for row in scoring_rows
            if (value := _to_float(row.get("credibility_score"))) is not None
        ]
        confidence = max(credibility_values, default=0.0)
        if confirmation == "confirmed_multi_source":
            confidence = min(0.92, confidence + min(len(credible_publishers) - 1, 2) * 0.06)
        elif confirmation == "confirmed_primary":
            confidence = max(confidence, 0.90)
        events.append(
            {
                "canonical_event_id": event_id,
                "event_revision": max(
                    max(
                        (int(row.get("event_revision") or 1) for row in scoring_rows),
                        default=1,
                    ),
                    len(unique_revisions),
                ),
                "event_type": best.get("event_type") or "general",
                "content_role": event_role,
                "direction": best.get("direction") or "neutral",
                "confirmation_status": confirmation,
                "independent_source_count": len(independent_publishers),
                "reported_source_count": len(publishers),
                "credible_source_count": len(credible_publishers),
                "event_confidence": round(confidence, 4),
                "persistence_score": max(
                    (_to_float(row.get("persistence_score")) or 0.0 for row in scoring_rows),
                    default=0.0,
                ),
                "driver_horizon": best.get("driver_horizon") or "unknown",
                "next_milestone": best.get("next_milestone"),
                "is_one_off": bool(best.get("is_one_off")),
                "is_terminal": any(bool(row.get("is_terminal")) for row in scoring_rows),
                "effective_until": max(
                    (str(row.get("effective_until")) for row in scoring_rows if row.get("effective_until")),
                    default=None,
                ),
                "available_at": min(
                    (str(row.get("available_at")) for row in scoring_rows if row.get("available_at")),
                    default=None,
                ),
                "evidence_ids": sorted(
                    {
                        str(row.get("evidence_id"))
                        for row in scoring_rows
                        if row.get("evidence_id")
                    }
                ),
                # Evidence received after the decision remains a count-only
                # diagnostic. Exposing its text here would let downstream
                # relationship inference leak future facts into a replay.
                "evidence": scoring_rows,
                "available_evidence_count": len(scoring_rows),
                "excluded_evidence_count": len(evidence_rows) - len(scoring_rows),
                "duplicate_evidence_count": max(len(scoring_rows) - max(len(unique_revisions), 1), 0),
                "delta_from_previous": "material_update" if len(unique_revisions) > 1 else "initial",
            }
        )
    return events


def build_stock_event_relation(
    event: Mapping[str, Any],
    *,
    code: str,
    stock_name: str | None,
    relation_context: str | None = None,
    match_reason: str | None = None,
    match_score: Any = None,
    theme_only: bool = False,
) -> dict[str, Any]:
    evidence_rows = [dict(row) for row in event.get("evidence") or [] if isinstance(row, Mapping)]
    text = normalize_opinion_text(
        relation_context
        or "。".join(
            str(row.get("relation_context") or row.get("title") or "")
            for row in evidence_rows
        )
    )
    name = normalize_opinion_text(stock_name)
    digits = str(code or "").split(".")[-1]
    entity_present = bool((name and name in text) or (digits and digits in text))
    has_business_fact = bool(BUSINESS_RELATION_PATTERN.search(text))
    primary = any(bool(row.get("is_primary_source")) for row in evidence_rows)
    adverse = event.get("content_role") == "risk_event" and entity_present
    disclosed_scale = next(
        (
            (
                _to_float(row.get("benefit_scale")),
                str(row.get("benefit_scale_unit") or "").strip() or None,
            )
            for row in evidence_rows
            if _to_float(row.get("benefit_scale")) is not None
            and str(row.get("benefit_scale_unit") or "").strip()
        ),
        (None, None),
    )

    if adverse:
        relation_type = "adverse_direct"
        relation_status = "confirmed_primary" if primary else "confirmed"
    elif theme_only:
        relation_type = "theme_mapping"
        relation_status = "background_only"
    elif entity_present and has_business_fact and event.get("content_role") in ACTIONABLE_CONTENT_ROLES:
        relation_type = "direct_business"
        relation_status = "confirmed_primary" if primary else "confirmed"
    else:
        relation_type = "mention_only"
        relation_status = "unconfirmed"

    raw_score = _to_float(match_score)
    if relation_status in CONFIRMED_RELATION_STATES:
        relation_score = _clamp(raw_score if raw_score is not None else (96.0 if primary else 78.0))
    elif relation_status == "background_only":
        relation_score = _clamp(raw_score if raw_score is not None else 50.0)
    else:
        relation_score = _clamp(min(raw_score if raw_score is not None else 35.0, 49.0))
    relation_id = _digest(
        {
            "canonical_event_id": event.get("canonical_event_id"),
            "event_revision": event.get("event_revision"),
            "code": code,
            "relation_type": relation_type,
            "rule_version": RELATION_RULE_VERSION,
        }
    )
    return {
        "relation_id": relation_id,
        "canonical_event_id": event.get("canonical_event_id"),
        "event_revision": event.get("event_revision") or 1,
        "code": code,
        "relation_type": relation_type,
        "relation_status": relation_status,
        "relation_score": relation_score,
        "benefit_scale": disclosed_scale[0]
        if relation_status in CONFIRMED_RELATION_STATES
        else None,
        "benefit_scale_unit": disclosed_scale[1]
        if relation_status in CONFIRMED_RELATION_STATES
        else None,
        "evidence_id": next(iter(event.get("evidence_ids") or []), None),
        "evidence_excerpt": text[:1000] or None,
        "relation_reason": match_reason
        or (
            "股票与事件事实处于同一业务语义片段"
            if relation_status in CONFIRMED_RELATION_STATES
            else "只有主题映射，未形成公司直接受益证据"
            if theme_only
            else "只有提及或关系证据不完整"
        ),
        "is_adverse_veto": adverse,
        "valid_from": event.get("available_at"),
        "valid_until": event.get("effective_until"),
        "relation_rule_version": RELATION_RULE_VERSION,
    }


def build_event_relation_bundle(
    news_rows: Sequence[Mapping[str, Any]],
    *,
    code: str,
    stock_name: str | None,
    sector_name: str | None,
    sector_type: str | None,
    match_type: str | None,
    match_reason: str | None,
    decision_as_of: Any,
    snapshot_as_of: Any,
) -> dict[str, Any]:
    sectors = [str(sector_name)] if sector_name else []
    normalized = [
        normalize_event_evidence(
            row,
            stock_codes=[code],
            sectors=sectors,
            decision_as_of=decision_as_of,
            snapshot_as_of=snapshot_as_of,
        )
        for row in news_rows
        if isinstance(row, Mapping)
    ]
    events = aggregate_event_evidence(normalized)
    relations = [
        build_stock_event_relation(
            event,
            code=code,
            stock_name=stock_name,
            relation_context=next(
                (
                    str(row.get("relation_context"))
                    for row in event.get("evidence") or []
                    if row.get("relation_context")
                ),
                None,
            ),
            match_reason=match_reason,
            match_score=max(
                (_to_float(row.get("relation_score")) or 0.0 for row in event.get("evidence") or []),
                default=None,
            ),
            theme_only=str(match_type or "") != "direct_news_match",
        )
        for event in events
    ]
    relation_by_event = {
        str(row.get("canonical_event_id")): row for row in relations
    }
    direct_watch_events = [
        event
        for event in events
        if event.get("available_evidence_count", 0) > 0
        and event.get("content_role") in ACTIONABLE_CONTENT_ROLES
        and event.get("direction") == "positive"
        and relation_by_event.get(str(event.get("canonical_event_id")), {}).get("relation_status")
        in CONFIRMED_RELATION_STATES
        and not event.get("is_terminal")
    ]
    direct_events = [
        event
        for event in direct_watch_events
        if event.get("confirmation_status") in CONFIRMED_EVENT_STATES
    ]
    adverse_events = [
        event
        for event in events
        if event.get("available_evidence_count", 0) > 0
        and event.get("content_role") == "risk_event"
        and event.get("confirmation_status") in CONFIRMED_EVENT_STATES
        and relation_by_event.get(str(event.get("canonical_event_id")), {}).get("is_adverse_veto")
    ]
    lanes: list[str] = []
    if direct_watch_events:
        lanes.append("direct_catalyst")
    if sector_name:
        lanes.append("theme_leader")
    missing_fields: list[str] = []
    if not normalized:
        missing_fields.append("event_evidence")
    if normalized and not any(row.get("received_at") for row in normalized):
        missing_fields.append("received_at")
    if direct_events and not any(
        relation_by_event[str(event.get("canonical_event_id"))].get("evidence_excerpt")
        for event in direct_events
    ):
        missing_fields.append("business_relation_excerpt")
    return {
        "events": events,
        "relations": relations,
        "candidate_lanes": lanes,
        "direct_event_ids": [str(row["canonical_event_id"]) for row in direct_events],
        "direct_watch_event_ids": [
            str(row["canonical_event_id"]) for row in direct_watch_events
        ],
        "adverse_event_ids": [str(row["canonical_event_id"]) for row in adverse_events],
        "evidence_ids": sorted(
            {
                str(value)
                for event in events
                for value in event.get("evidence_ids") or []
                if value
            }
        ),
        "evidence_quality": {
            "status": (
                "invalid"
                if adverse_events
                else "complete"
                if direct_events or (sector_name and normalized)
                else "partial"
            ),
            "available_count": sum(int(event.get("available_evidence_count") or 0) for event in events),
            "excluded_after_decision_count": sum(
                sum(
                    1
                    for row in event.get("evidence") or []
                    if row.get("availability_status") == "after_decision"
                )
                for event in events
            ),
            "missing_fields": sorted(set(missing_fields)),
            "evidence_rule_version": EVIDENCE_RULE_VERSION,
            "relation_rule_version": RELATION_RULE_VERSION,
        },
    }
