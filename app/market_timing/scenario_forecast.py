from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, time
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from app.market_timing.v20 import INDEX_CODE, MODEL_ID as TIMING_MODEL_ID
from app.shared.db import mysql_conn, mysql_read_conn


SPEC_PATH = Path(__file__).resolve().parent / "specs" / "market_scenario_forecast_v1.json"
MODEL_ID = "market_scenario_forecast_v1"
MODEL_VERSION = "v1"
CLASS_NAMES = ("down", "range", "up")
FEATURE_KEYS = (
    "timing_score",
    "trend",
    "breadth",
    "capital",
    "tail_risk",
    "leadership",
)
LEADERSHIP_LABELS = {
    "seed": "萌芽",
    "confirmed": "确认",
    "crowded": "拥挤",
    "decay": "退潮",
}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _json_value(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list, tuple)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _quantile(values: Sequence[float], probability: float) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * probability
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1 - weight) + clean[upper] * weight


def load_market_scenario_spec() -> dict[str, Any]:
    with SPEC_PATH.open("r", encoding="utf-8") as handle:
        spec = json.load(handle)
    if spec.get("model_id") != MODEL_ID:
        raise RuntimeError("market scenario spec model_id mismatch")
    return spec


def market_scenario_spec_hash() -> str:
    return _sha256(load_market_scenario_spec())


def classify_scenario(forward_return_pct: float, expected_sigma_pct: float) -> str:
    boundary = abs(float(expected_sigma_pct)) * 0.5
    if forward_return_pct > boundary:
        return "up"
    if forward_return_pct < -boundary:
        return "down"
    return "range"


def _softmax(values: Sequence[float]) -> list[float]:
    maximum = max(values)
    exps = [math.exp(value - maximum) for value in values]
    total = sum(exps) or 1.0
    return [value / total for value in exps]


def _fit_standardizer(rows: Sequence[Sequence[float]]) -> tuple[list[float], list[float]]:
    if not rows:
        return [], []
    width = len(rows[0])
    means = [sum(row[index] for row in rows) / len(rows) for index in range(width)]
    stds = []
    for index, mean_value in enumerate(means):
        variance = sum((row[index] - mean_value) ** 2 for row in rows) / len(rows)
        stds.append(math.sqrt(variance) if variance > 1e-12 else 1.0)
    return means, stds


def _standardize(
    rows: Sequence[Sequence[float]],
    means: Sequence[float],
    stds: Sequence[float],
) -> list[list[float]]:
    return [
        [
            (float(value) - means[index]) / stds[index]
            for index, value in enumerate(row)
        ]
        for row in rows
    ]


def fit_multinomial_logistic(
    features: Sequence[Sequence[float]],
    labels: Sequence[int],
    *,
    l2: float = 0.5,
    learning_rate: float = 0.05,
    iterations: int = 350,
) -> dict[str, Any]:
    if not features or len(features) != len(labels):
        raise ValueError("features and labels must be non-empty and aligned")
    means, stds = _fit_standardizer(features)
    standardized = _standardize(features, means, stds)
    width = len(standardized[0]) + 1
    weights = [[0.0] * width for _ in CLASS_NAMES]
    sample_count = len(standardized)
    for iteration in range(iterations):
        gradients = [[0.0] * width for _ in CLASS_NAMES]
        for row, label in zip(standardized, labels):
            vector = [1.0, *row]
            probabilities = _softmax(
                [
                    sum(weight * value for weight, value in zip(class_weights, vector))
                    for class_weights in weights
                ]
            )
            for class_index in range(len(CLASS_NAMES)):
                error = probabilities[class_index] - (1.0 if label == class_index else 0.0)
                for feature_index, value in enumerate(vector):
                    gradients[class_index][feature_index] += error * value
        step = learning_rate / (1.0 + iteration / max(iterations, 1))
        for class_index, class_weights in enumerate(weights):
            for feature_index in range(width):
                regularization = (
                    l2 * class_weights[feature_index]
                    if feature_index > 0
                    else 0.0
                )
                class_weights[feature_index] -= step * (
                    gradients[class_index][feature_index] / sample_count
                    + regularization / sample_count
                )
    return {"weights": weights, "means": means, "stds": stds}


def predict_multinomial_logistic(
    model: Mapping[str, Any],
    features: Sequence[float],
) -> list[float]:
    means = list(model["means"])
    stds = list(model["stds"])
    standardized = _standardize([features], means, stds)[0]
    vector = [1.0, *standardized]
    return _softmax(
        [
            sum(weight * value for weight, value in zip(class_weights, vector))
            for class_weights in model["weights"]
        ]
    )


def multiclass_brier(probabilities: Sequence[Sequence[float]], labels: Sequence[int]) -> float:
    if not probabilities:
        return float("nan")
    return sum(
        sum(
            (probability - (1.0 if class_index == label else 0.0)) ** 2
            for class_index, probability in enumerate(row)
        )
        for row, label in zip(probabilities, labels)
    ) / len(probabilities)


def multiclass_log_loss(
    probabilities: Sequence[Sequence[float]],
    labels: Sequence[int],
) -> float:
    if not probabilities:
        return float("nan")
    return -sum(
        math.log(max(1e-12, min(1 - 1e-12, row[label])))
        for row, label in zip(probabilities, labels)
    ) / len(probabilities)


def expected_calibration_error(
    probabilities: Sequence[Sequence[float]],
    labels: Sequence[int],
    *,
    bins: int = 5,
) -> float:
    if not probabilities:
        return float("nan")
    buckets: list[list[tuple[float, bool]]] = [[] for _ in range(bins)]
    for row, label in zip(probabilities, labels):
        predicted = max(range(len(row)), key=lambda index: row[index])
        confidence = float(row[predicted])
        bucket = min(bins - 1, int(confidence * bins))
        buckets[bucket].append((confidence, predicted == label))
    total = len(probabilities)
    return sum(
        len(bucket) / total
        * abs(
            sum(confidence for confidence, _ in bucket) / len(bucket)
            - sum(1.0 if correct else 0.0 for _, correct in bucket) / len(bucket)
        )
        for bucket in buckets
        if bucket
    )


def _class_prior(labels: Sequence[int]) -> list[float]:
    counts = Counter(labels)
    total = len(labels) + len(CLASS_NAMES)
    return [(counts.get(index, 0) + 1) / total for index in range(len(CLASS_NAMES))]


def _state_persistence_probabilities(timing_score: float) -> list[float]:
    if timing_score >= 60:
        return [0.15, 0.30, 0.55]
    if timing_score < 35:
        return [0.55, 0.30, 0.15]
    return [0.25, 0.50, 0.25]


def validate_probability_model(
    samples: Sequence[Mapping[str, Any]],
    *,
    minimum_training_rows: int = 120,
    minimum_validation_rows: int = 30,
) -> dict[str, Any]:
    if len(samples) < minimum_training_rows + minimum_validation_rows:
        return {
            "status": "insufficient_evidence",
            "sample_size": len(samples),
            "minimum_required": minimum_training_rows + minimum_validation_rows,
            "candidate": None,
            "unconditional_baseline": None,
            "state_persistence_baseline": None,
            "beats_both_baselines": False,
        }
    validation_size = max(minimum_validation_rows, int(len(samples) * 0.20))
    split = len(samples) - validation_size
    if split < minimum_training_rows:
        split = minimum_training_rows
    candidate_rows: list[list[float]] = []
    unconditional_rows: list[list[float]] = []
    persistence_rows: list[list[float]] = []
    labels: list[int] = []
    for index in range(split, len(samples)):
        history = samples[:index]
        model = fit_multinomial_logistic(
            [row["features"] for row in history],
            [int(row["label_index"]) for row in history],
        )
        candidate_rows.append(
            predict_multinomial_logistic(model, samples[index]["features"])
        )
        unconditional_rows.append(
            _class_prior([int(row["label_index"]) for row in history])
        )
        persistence_rows.append(
            _state_persistence_probabilities(
                float(samples[index]["features"][0])
            )
        )
        labels.append(int(samples[index]["label_index"]))

    def metrics(rows: Sequence[Sequence[float]]) -> dict[str, float]:
        return {
            "brier": round(multiclass_brier(rows, labels), 8),
            "log_loss": round(multiclass_log_loss(rows, labels), 8),
            "ece": round(expected_calibration_error(rows, labels), 8),
        }

    candidate = metrics(candidate_rows)
    unconditional = metrics(unconditional_rows)
    persistence = metrics(persistence_rows)
    beats_both = all(
        candidate[metric] < unconditional[metric]
        and candidate[metric] < persistence[metric]
        for metric in ("brier", "log_loss")
    ) and candidate["ece"] <= max(unconditional["ece"], persistence["ece"])
    return {
        "status": "baseline_pass" if beats_both else "not_better_than_baseline",
        "sample_size": len(samples),
        "training_size": split,
        "validation_size": len(labels),
        "candidate": candidate,
        "unconditional_baseline": unconditional,
        "state_persistence_baseline": persistence,
        "beats_both_baselines": beats_both,
        "validation_method": "expanding_walk_forward",
    }


def _action_plan(timing_state: str, dominant_scenario: str) -> dict[str, str]:
    if dominant_scenario == "down":
        return {
            "portfolio": "停止加仓，优先降至当前择时区间下沿并保留现金",
            "entry": "不追突破；仅观察，不因概率单独开仓",
            "risk": "若宽度与尾部风险同步恶化，按择时紧急降级执行",
        }
    if dominant_scenario == "range":
        return {
            "portfolio": "维持择时目标附近，保留机动仓位",
            "entry": "降低追突破频率，优先回踩触发和更高盈亏比",
            "risk": "到达压力位分批保护利润，止损仍按 N 执行",
        }
    if timing_state in {"cash", "defensive"}:
        return {
            "portfolio": "只做观察或极小试探，不得突破择时仓位上限",
            "entry": "等待宽度与资金共同确认后再增加一个风险单元",
            "risk": "看多情景不能覆盖账户回撤熔断",
        }
    return {
        "portfolio": "按当前择时目标分批参与，不一次打满",
        "entry": "优先确认主线内的突破或回踩触发",
        "risk": "继续使用 N20、2N 止损和只盈利加仓",
    }


class MarketScenarioForecastRepository:
    def __init__(
        self,
        *,
        connection_factory=None,
        read_connection_factory=None,
    ) -> None:
        self._connection_factory = connection_factory or mysql_conn
        self._read_connection_factory = read_connection_factory or mysql_read_conn
        self.spec = load_market_scenario_spec()
        self.spec_hash = market_scenario_spec_hash()

    def _timing_rows(self, through_date: date | str) -> list[dict[str, Any]]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, timing_score, state, coverage_json
                    FROM market_timing_signal_daily
                    WHERE model_id=%s
                      AND index_code=%s
                      AND trade_date<=%s
                    ORDER BY trade_date
                    """,
                    (TIMING_MODEL_ID, INDEX_CODE, through_date),
                )
                rows = cursor.fetchall() or []
        result = []
        for row in rows:
            coverage = _json_value(row.get("coverage_json"), {})
            dimensions = coverage.get("dimensions") or {}
            features = [
                _to_float(row.get("timing_score")) or 50.0,
                *[
                    _to_float((dimensions.get(key) or {}).get("score")) or 50.0
                    for key in FEATURE_KEYS[1:]
                ],
            ]
            result.append(
                {
                    "trade_date": row["trade_date"],
                    "state": str(row.get("state") or "neutral"),
                    "features": features,
                    "feature_map": dict(zip(FEATURE_KEYS, features)),
                }
            )
        return result

    def _index_rows(self) -> list[dict[str, Any]]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, close
                    FROM market_index_daily
                    WHERE index_code=%s AND close IS NOT NULL
                    ORDER BY trade_date
                    """,
                    (INDEX_CODE,),
                )
                return [dict(row) for row in (cursor.fetchall() or [])]

    @staticmethod
    def _expected_sigma(
        index_rows: Sequence[Mapping[str, Any]],
        signal_index: int,
        horizon_days: int,
    ) -> float:
        start = max(1, signal_index - 60)
        daily_returns = []
        for index in range(start, signal_index + 1):
            previous = _to_float(index_rows[index - 1].get("close"))
            current = _to_float(index_rows[index].get("close"))
            if previous and current:
                daily_returns.append((current / previous - 1) * 100)
        if len(daily_returns) < 10:
            return 1.0 * math.sqrt(horizon_days)
        mean_value = sum(daily_returns) / len(daily_returns)
        variance = sum((value - mean_value) ** 2 for value in daily_returns) / len(daily_returns)
        return max(0.25, math.sqrt(variance) * math.sqrt(horizon_days))

    def _training_samples(
        self,
        timing_rows: Sequence[Mapping[str, Any]],
        index_rows: Sequence[Mapping[str, Any]],
        horizon_days: int,
        through_date: date | str,
    ) -> list[dict[str, Any]]:
        index_by_date = {
            row["trade_date"]: index
            for index, row in enumerate(index_rows)
        }
        through = date.fromisoformat(str(through_date))
        samples = []
        for timing in timing_rows:
            signal_index = index_by_date.get(timing["trade_date"])
            if signal_index is None or signal_index + horizon_days >= len(index_rows):
                continue
            exit_row = index_rows[signal_index + horizon_days]
            if exit_row["trade_date"] > through:
                continue
            entry_close = _to_float(index_rows[signal_index].get("close"))
            exit_close = _to_float(exit_row.get("close"))
            if not entry_close or not exit_close:
                continue
            forward_return = (exit_close / entry_close - 1) * 100
            expected_sigma = self._expected_sigma(
                index_rows,
                signal_index,
                horizon_days,
            )
            scenario = classify_scenario(forward_return, expected_sigma)
            path_returns = [
                (_to_float(index_rows[index].get("close")) / entry_close - 1) * 100
                for index in range(signal_index + 1, signal_index + horizon_days + 1)
                if _to_float(index_rows[index].get("close")) is not None
            ]
            samples.append(
                {
                    "trade_date": timing["trade_date"],
                    "features": list(timing["features"]),
                    "state": timing["state"],
                    "forward_return_pct": forward_return,
                    "expected_sigma_pct": expected_sigma,
                    "scenario": scenario,
                    "label_index": CLASS_NAMES.index(scenario),
                    "max_drawdown_pct": min(path_returns) if path_returns else None,
                }
            )
        return samples

    @staticmethod
    def _analog_rows(
        samples: Sequence[Mapping[str, Any]],
        current_features: Sequence[float],
        *,
        limit: int = 40,
    ) -> list[Mapping[str, Any]]:
        if not samples:
            return []
        means, stds = _fit_standardizer([row["features"] for row in samples])
        standardized = _standardize(
            [row["features"] for row in samples],
            means,
            stds,
        )
        current = _standardize([current_features], means, stds)[0]
        ranked = sorted(
            zip(samples, standardized),
            key=lambda pair: sum(
                (value - current[index]) ** 2
                for index, value in enumerate(pair[1])
            ),
        )
        return [row for row, _ in ranked[:limit]]

    @staticmethod
    def _triggers(feature_map: Mapping[str, float]) -> tuple[list[str], list[str]]:
        bullish = []
        bearish = []
        if feature_map["trend"] < 60:
            bullish.append("趋势维度升至60分以上并连续确认")
        if feature_map["breadth"] < 58:
            bullish.append("全A宽度升至58分以上")
        if feature_map["capital"] < 58:
            bullish.append("资金量维度升至58分以上且不背离")
        if feature_map["leadership"] < 60:
            bullish.append("至少一个主线从萌芽进入确认")
        bearish.extend(
            [
                "宽度跌至35分以下时停止加仓",
                "尾部风险跌至20分以下时触发紧急防守",
                "趋势与资金同时低于35分时降至区间下沿",
            ]
        )
        return bullish, bearish

    @staticmethod
    def _transition_probabilities(
        timing_rows: Sequence[Mapping[str, Any]],
        current_state: str,
        horizon_days: int,
    ) -> dict[str, float]:
        counts: Counter[str] = Counter()
        total = 0
        for index, row in enumerate(timing_rows):
            target_index = index + horizon_days
            if row["state"] != current_state or target_index >= len(timing_rows):
                continue
            counts[str(timing_rows[target_index]["state"])] += 1
            total += 1
        if not total:
            return {current_state: 1.0}
        return {
            state: round(count / total, 4)
            for state, count in counts.most_common()
        }

    def build_forecast(
        self,
        trade_date: date | str,
        horizon_days: int,
    ) -> dict[str, Any]:
        timing_rows = self._timing_rows(trade_date)
        if not timing_rows:
            raise RuntimeError("market timing V2.0 has no feature row")
        current = timing_rows[-1]
        index_rows = self._index_rows()
        samples = self._training_samples(
            timing_rows,
            index_rows,
            horizon_days,
            trade_date,
        )
        validation = validate_probability_model(samples)
        labels = [int(row["label_index"]) for row in samples]
        prior = _class_prior(labels) if labels else [1 / 3, 1 / 3, 1 / 3]
        if validation.get("beats_both_baselines"):
            model = fit_multinomial_logistic(
                [row["features"] for row in samples],
                labels,
            )
            probability_values = predict_multinomial_logistic(
                model,
                current["features"],
            )
            evidence_status = "provisional_baseline_pass"
            confidence = min(0.85, 0.50 + len(samples) / 1000)
        else:
            probability_values = prior
            evidence_status = str(validation.get("status") or "insufficient_evidence")
            confidence = min(0.35, len(samples) / 500)
        probabilities = {
            name: round(probability_values[index], 4)
            for index, name in enumerate(CLASS_NAMES)
        }
        analogs = self._analog_rows(samples, current["features"])
        analog_returns = [
            float(row["forward_return_pct"])
            for row in analogs
        ]
        drawdowns = [
            float(row["max_drawdown_pct"])
            for row in analogs
            if row.get("max_drawdown_pct") is not None
        ]
        return_quantiles = {
            "p10": _quantile(analog_returns, 0.10),
            "p50": _quantile(analog_returns, 0.50),
            "p90": _quantile(analog_returns, 0.90),
        }
        drawdown_probabilities = {
            f"dd_{threshold}pct": (
                sum(value <= -threshold for value in drawdowns) / len(drawdowns)
                if drawdowns
                else None
            )
            for threshold in (3, 5, 8)
        }
        dominant = max(probabilities, key=probabilities.get)
        bullish, bearish = self._triggers(current["feature_map"])
        as_of_date = date.fromisoformat(str(trade_date))
        as_of_datetime = datetime.combine(as_of_date, time(15, 30))
        forecast_id = (
            f"msfv1_{as_of_date.strftime('%Y%m%d')}_"
            f"{INDEX_CODE.replace('.', '')}_h{horizon_days}"
        )
        feature_payload = {
            "keys": FEATURE_KEYS,
            "values": current["feature_map"],
            "timing_state": current["state"],
        }
        result = {
            "forecast_id": forecast_id,
            "model_id": MODEL_ID,
            "model_name": "市场概率情景 V1",
            "version": MODEL_VERSION,
            "spec_hash": self.spec_hash,
            "trade_date": str(as_of_date),
            "as_of": as_of_datetime.isoformat(sep=" "),
            "data_cutoff": as_of_datetime.isoformat(sep=" "),
            "earliest_execution_at": None,
            "index_code": INDEX_CODE,
            "horizon_days": horizon_days,
            "probabilities": probabilities,
            "return_quantiles_pct": {
                key: round(value, 2) if value is not None else None
                for key, value in return_quantiles.items()
            },
            "drawdown_probabilities": {
                key: round(value, 4) if value is not None else None
                for key, value in drawdown_probabilities.items()
            },
            "state_transition_probabilities": self._transition_probabilities(
                timing_rows,
                current["state"],
                horizon_days,
            ),
            "confidence": round(confidence, 4),
            "similar_history_count": len(analogs),
            "evidence_status": evidence_status,
            "validation_status": str(validation.get("status")),
            "validation": validation,
            "bullish_triggers": bullish,
            "bearish_triggers": bearish,
            "action_plan": _action_plan(current["state"], dominant),
            "dominant_scenario": dominant,
            "feature": feature_payload,
            "feature_hash": _sha256(feature_payload),
            "source_lineage": {
                "timing_model_id": TIMING_MODEL_ID,
                "index_table": "market_index_daily",
                "leadership_table": "market_leadership_state_daily",
                "known_month_feature": False,
            },
            "research_only": True,
            "probability_display_allowed": bool(
                validation.get("beats_both_baselines")
            ),
        }
        result["payload_hash"] = _sha256(
            {key: value for key, value in result.items() if key != "payload_hash"}
        )
        return result

    def _leadership_rows(self, trade_date: date | str) -> list[dict[str, Any]]:
        through = date.fromisoformat(str(trade_date))
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT trade_date, as_of_datetime, sector_type, sector_name,
                           sector_score, weighted_impact_score, news_count,
                           source_count, stock_count, positive_news_count,
                           negative_news_count
                    FROM sector_opinion_daily
                    WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 14 DAY) AND %s
                    ORDER BY trade_date, as_of_datetime
                    """,
                    (through, through),
                )
                opinion_rows = cursor.fetchall() or []
                cursor.execute(
                    """
                    SELECT sector_type, sector_name, net_amount, pct_chg,
                           company_count, quote_time
                    FROM market_sector_fund_flow_intraday
                    WHERE trade_date=%s
                    ORDER BY quote_time
                    """,
                    (through,),
                )
                flow_rows = cursor.fetchall() or []
        latest_opinion: dict[tuple[Any, str, str], Mapping[str, Any]] = {}
        for row in opinion_rows:
            key = (row["trade_date"], str(row["sector_type"]), str(row["sector_name"]))
            latest_opinion[key] = row
        current_rows = [
            row
            for (row_date, _, _), row in latest_opinion.items()
            if row_date == through
        ]
        flow_latest: dict[tuple[str, str], Mapping[str, Any]] = {}
        for row in flow_rows:
            flow_latest[(str(row["sector_type"]), str(row["sector_name"]))] = row
        history_by_sector: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
        for (_, sector_type, sector_name), row in latest_opinion.items():
            history_by_sector[(sector_type, sector_name)].append(row)
        result = []
        for row in sorted(
            current_rows,
            key=lambda item: _to_float(item.get("sector_score")) or 0,
            reverse=True,
        )[:30]:
            sector_key = (str(row["sector_type"]), str(row["sector_name"]))
            history = sorted(
                history_by_sector[sector_key],
                key=lambda item: item["trade_date"],
            )
            current_score = _to_float(row.get("sector_score")) or 0.0
            prior_score = (
                _to_float(history[-2].get("sector_score"))
                if len(history) >= 2
                else None
            )
            score_change = current_score - prior_score if prior_score is not None else 0.0
            source_count = int(row.get("source_count") or 0)
            stock_count = int(row.get("stock_count") or 0)
            positive = int(row.get("positive_news_count") or 0)
            negative = int(row.get("negative_news_count") or 0)
            flow = flow_latest.get(sector_key)
            net_amount = _to_float(flow.get("net_amount")) if flow else None
            heat_score = _clamp(current_score / 100, 0, 1) * 100
            capital_score = (
                50 + max(-45, min(45, net_amount / 5))
                if net_amount is not None
                else 50.0
            )
            breadth_score = min(100.0, 35 + stock_count * 3)
            persistence_score = min(100.0, 30 + len(history) * 12 + max(0, score_change) * 2)
            crowding_score = min(
                100.0,
                max(0.0, current_score - 65) * 2.2
                + max(0, positive - max(stock_count, 1)) * 2,
            )
            leadership_score = (
                heat_score * 0.35
                + capital_score * 0.25
                + breadth_score * 0.20
                + persistence_score * 0.20
            )
            contradictions = []
            if negative > positive:
                contradictions.append("负面新闻多于正面新闻")
            if net_amount is not None and net_amount < 0:
                contradictions.append("行业资金为净流出")
            if score_change <= -8:
                contradictions.append("行业评分较前次明显下降")
            if score_change <= -8 or (negative > positive and current_score < 70):
                state = "decay"
            elif current_score >= 82 and crowding_score >= 55:
                state = "crowded"
            elif (
                current_score >= 68
                and source_count >= 2
                and positive >= negative
                and (net_amount is None or net_amount >= 0)
            ):
                state = "confirmed"
            else:
                state = "seed"
            evidence = [
                f"行业评分 {current_score:.1f}",
                f"来源 {source_count} 个、覆盖股票 {stock_count} 只",
                f"评分变化 {score_change:+.1f}",
            ]
            if net_amount is not None:
                evidence.append(f"当日资金净额 {net_amount:+.2f}")
            as_of = row.get("as_of_datetime") or datetime.combine(through, time(15, 30))
            payload = {
                "model_id": MODEL_ID,
                "version": MODEL_VERSION,
                "spec_hash": self.spec_hash,
                "trade_date": str(through),
                "as_of": str(as_of),
                "data_cutoff": str(as_of),
                "sector_type": sector_key[0],
                "sector_name": sector_key[1],
                "leadership_state": state,
                "state_label": LEADERSHIP_LABELS[state],
                "leadership_score": round(leadership_score, 2),
                "confidence": round(
                    min(1.0, 0.30 + source_count * 0.10 + min(len(history), 5) * 0.08),
                    4,
                ),
                "heat_score": round(heat_score, 2),
                "capital_score": round(capital_score, 2),
                "breadth_score": round(breadth_score, 2),
                "persistence_score": round(persistence_score, 2),
                "crowding_score": round(crowding_score, 2),
                "evidence": evidence,
                "contradictions": contradictions,
                "upgrade_triggers": [
                    "资金连续流入且行业宽度继续扩散",
                    "热度、资金、趋势至少三项共振",
                ],
                "downgrade_triggers": [
                    "资金转负并连续减弱",
                    "行业宽度收缩或评分连续下降",
                ],
                "source_lineage": {
                    "opinion": "sector_opinion_daily",
                    "capital": (
                        "market_sector_fund_flow_intraday"
                        if flow
                        else "missing_neutral"
                    ),
                },
                "research_only": True,
            }
            payload["payload_hash"] = _sha256(payload)
            result.append(payload)
        return result

    def materialize(
        self,
        trade_date: date | str,
        *,
        horizons: Iterable[int] = (1, 5, 20),
    ) -> dict[str, Any]:
        leadership = self._leadership_rows(trade_date)
        forecasts = [
            self.build_forecast(trade_date, int(horizon))
            for horizon in sorted(set(horizons))
        ]
        with self._connection_factory(dict_cursor=False) as conn:
            with conn.cursor() as cursor:
                for row in leadership:
                    cursor.execute(
                        """
                        INSERT INTO market_leadership_state_daily (
                            model_id, version, spec_hash, trade_date,
                            as_of_datetime, data_cutoff_datetime,
                            sector_type, sector_name, leadership_state,
                            state_label, leadership_score, confidence,
                            heat_score, capital_score, breadth_score,
                            persistence_score, crowding_score, evidence_json,
                            contradiction_json, upgrade_triggers_json,
                            downgrade_triggers_json, source_lineage_json,
                            payload_hash
                        ) VALUES (
                            %s,%s,%s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,
                            %s
                        )
                        ON DUPLICATE KEY UPDATE
                            payload_hash=payload_hash
                        """,
                        (
                            row["model_id"],
                            row["version"],
                            row["spec_hash"],
                            row["trade_date"],
                            row["as_of"],
                            row["data_cutoff"],
                            row["sector_type"],
                            row["sector_name"],
                            row["leadership_state"],
                            row["state_label"],
                            row["leadership_score"],
                            row["confidence"],
                            row["heat_score"],
                            row["capital_score"],
                            row["breadth_score"],
                            row["persistence_score"],
                            row["crowding_score"],
                            _canonical_json(row["evidence"]),
                            _canonical_json(row["contradictions"]),
                            _canonical_json(row["upgrade_triggers"]),
                            _canonical_json(row["downgrade_triggers"]),
                            _canonical_json(row["source_lineage"]),
                            row["payload_hash"],
                        ),
                    )
                for row in forecasts:
                    cursor.execute(
                        """
                        SELECT payload_hash
                        FROM market_scenario_forecast_daily
                        WHERE forecast_id=%s
                        """,
                        (row["forecast_id"],),
                    )
                    existing = cursor.fetchone()
                    if existing:
                        existing_hash = existing[0] if not isinstance(existing, dict) else existing["payload_hash"]
                        if str(existing_hash) != row["payload_hash"]:
                            raise RuntimeError(
                                f"immutable forecast payload mismatch: {row['forecast_id']}"
                            )
                        continue
                    cursor.execute(
                        """
                        INSERT INTO market_scenario_forecast_daily (
                            forecast_id, model_id, version, spec_hash,
                            trade_date, as_of_datetime, data_cutoff_datetime,
                            earliest_execution_at, index_code, horizon_days,
                            probabilities_json, return_quantiles_json,
                            drawdown_probabilities_json, state_transition_json,
                            confidence, similar_history_count, evidence_status,
                            validation_status, validation_json,
                            bullish_triggers_json, bearish_triggers_json,
                            action_plan_json, feature_json, feature_hash,
                            source_lineage_json, payload_hash
                        ) VALUES (
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s
                        )
                        """,
                        (
                            row["forecast_id"],
                            row["model_id"],
                            row["version"],
                            row["spec_hash"],
                            row["trade_date"],
                            row["as_of"],
                            row["data_cutoff"],
                            row["earliest_execution_at"],
                            row["index_code"],
                            row["horizon_days"],
                            _canonical_json(row["probabilities"]),
                            _canonical_json(row["return_quantiles_pct"]),
                            _canonical_json(row["drawdown_probabilities"]),
                            _canonical_json(row["state_transition_probabilities"]),
                            row["confidence"],
                            row["similar_history_count"],
                            row["evidence_status"],
                            row["validation_status"],
                            _canonical_json(row["validation"]),
                            _canonical_json(row["bullish_triggers"]),
                            _canonical_json(row["bearish_triggers"]),
                            _canonical_json(row["action_plan"]),
                            _canonical_json(row["feature"]),
                            row["feature_hash"],
                            _canonical_json(row["source_lineage"]),
                            row["payload_hash"],
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT IGNORE INTO market_scenario_outcome (
                            forecast_id, trade_date, horizon_days,
                            outcome_status
                        ) VALUES (%s,%s,%s,'pending')
                        """,
                        (
                            row["forecast_id"],
                            row["trade_date"],
                            row["horizon_days"],
                        ),
                    )
        return {
            "status": "success",
            "trade_date": str(trade_date),
            "forecast_count": len(forecasts),
            "leadership_count": len(leadership),
            "forecasts": forecasts,
            "leadership": leadership,
        }

    def refresh_outcomes(self, *, limit: int = 200) -> dict[str, Any]:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT o.forecast_id, o.trade_date, o.horizon_days
                    FROM market_scenario_outcome o
                    WHERE o.outcome_status='pending'
                    ORDER BY o.trade_date
                    LIMIT %s
                    """,
                    (limit,),
                )
                pending = cursor.fetchall() or []
        index_rows = self._index_rows()
        index_by_date = {
            row["trade_date"]: index
            for index, row in enumerate(index_rows)
        }
        updates = []
        for row in pending:
            signal_index = index_by_date.get(row["trade_date"])
            horizon = int(row["horizon_days"])
            if signal_index is None or signal_index + horizon >= len(index_rows):
                continue
            entry = index_rows[signal_index]
            exit_row = index_rows[signal_index + horizon]
            entry_close = _to_float(entry.get("close"))
            exit_close = _to_float(exit_row.get("close"))
            if not entry_close or not exit_close:
                continue
            expected_sigma = self._expected_sigma(index_rows, signal_index, horizon)
            realized_return = (exit_close / entry_close - 1) * 100
            path = [
                (_to_float(index_rows[index].get("close")) / entry_close - 1) * 100
                for index in range(signal_index + 1, signal_index + horizon + 1)
                if _to_float(index_rows[index].get("close")) is not None
            ]
            payload = {
                "entry_trade_date": str(entry["trade_date"]),
                "exit_trade_date": str(exit_row["trade_date"]),
                "expected_sigma_pct": expected_sigma,
                "realized_return_pct": realized_return,
                "realized_scenario": classify_scenario(realized_return, expected_sigma),
                "realized_max_drawdown_pct": min(path) if path else None,
            }
            updates.append((row["forecast_id"], payload))
        if updates:
            with self._connection_factory(dict_cursor=False) as conn:
                with conn.cursor() as cursor:
                    for forecast_id, payload in updates:
                        cursor.execute(
                            """
                            UPDATE market_scenario_outcome
                            SET entry_trade_date=%s,
                                exit_trade_date=%s,
                                expected_sigma_pct=%s,
                                realized_return_pct=%s,
                                realized_scenario=%s,
                                realized_max_drawdown_pct=%s,
                                outcome_status='ready',
                                outcome_hash=%s,
                                metadata_json=%s,
                                computed_at=NOW(6)
                            WHERE forecast_id=%s
                              AND outcome_status='pending'
                            """,
                            (
                                payload["entry_trade_date"],
                                payload["exit_trade_date"],
                                payload["expected_sigma_pct"],
                                payload["realized_return_pct"],
                                payload["realized_scenario"],
                                payload["realized_max_drawdown_pct"],
                                _sha256(payload),
                                _canonical_json(
                                    {"return_basis": "signal_close_to_horizon_close"}
                                ),
                                forecast_id,
                            ),
                        )
        return {
            "status": "success",
            "pending_checked": len(pending),
            "outcomes_ready": len(updates),
        }

    def latest(self) -> dict[str, Any] | None:
        with self._read_connection_factory() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM market_scenario_forecast_daily
                    WHERE model_id=%s
                      AND trade_date=(
                          SELECT MAX(trade_date)
                          FROM market_scenario_forecast_daily
                          WHERE model_id=%s
                      )
                    ORDER BY horizon_days
                    """,
                    (MODEL_ID, MODEL_ID),
                )
                forecast_rows = cursor.fetchall() or []
                if not forecast_rows:
                    return None
                trade_date = forecast_rows[0]["trade_date"]
                cursor.execute(
                    """
                    SELECT *
                    FROM market_leadership_state_daily
                    WHERE model_id=%s AND trade_date=%s
                    ORDER BY FIELD(
                        leadership_state,
                        'confirmed','seed','crowded','decay'
                    ), leadership_score DESC
                    LIMIT 24
                    """,
                    (MODEL_ID, trade_date),
                )
                leadership_rows = cursor.fetchall() or []
        forecasts = []
        for row in forecast_rows:
            forecasts.append(
                {
                    "forecast_id": row.get("forecast_id"),
                    "horizon_days": int(row.get("horizon_days") or 0),
                    "probabilities": _json_value(row.get("probabilities_json"), {}),
                    "return_quantiles_pct": _json_value(row.get("return_quantiles_json"), {}),
                    "drawdown_probabilities": _json_value(
                        row.get("drawdown_probabilities_json"),
                        {},
                    ),
                    "state_transition_probabilities": _json_value(
                        row.get("state_transition_json"),
                        {},
                    ),
                    "confidence": _to_float(row.get("confidence")),
                    "similar_history_count": int(row.get("similar_history_count") or 0),
                    "evidence_status": row.get("evidence_status"),
                    "validation_status": row.get("validation_status"),
                    "validation": _json_value(row.get("validation_json"), {}),
                    "bullish_triggers": _json_value(row.get("bullish_triggers_json"), []),
                    "bearish_triggers": _json_value(row.get("bearish_triggers_json"), []),
                    "action_plan": _json_value(row.get("action_plan_json"), {}),
                    "feature": _json_value(row.get("feature_json"), {}),
                    "probability_display_allowed": (
                        row.get("validation_status") == "baseline_pass"
                    ),
                }
            )
        leadership = [
            {
                "sector_type": row.get("sector_type"),
                "sector_name": row.get("sector_name"),
                "leadership_state": row.get("leadership_state"),
                "state_label": row.get("state_label"),
                "leadership_score": _to_float(row.get("leadership_score")),
                "confidence": _to_float(row.get("confidence")),
                "evidence": _json_value(row.get("evidence_json"), []),
                "contradictions": _json_value(row.get("contradiction_json"), []),
                "upgrade_triggers": _json_value(row.get("upgrade_triggers_json"), []),
                "downgrade_triggers": _json_value(row.get("downgrade_triggers_json"), []),
            }
            for row in leadership_rows
        ]
        return {
            "model_id": MODEL_ID,
            "model_name": "市场概率情景 V1",
            "version": MODEL_VERSION,
            "spec_hash": forecast_rows[0].get("spec_hash"),
            "trade_date": str(trade_date),
            "as_of": str(forecast_rows[0].get("as_of_datetime")),
            "forecasts": forecasts,
            "leadership": leadership,
            "research_only": True,
            "probability_not_direction_command": True,
        }
