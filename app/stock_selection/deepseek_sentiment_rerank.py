from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import requests

from app.shared.db import mysql_conn


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _score_0_100(value: Any, default: float = 50.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return round(max(0.0, min(number, 100.0)), 2)


def _extract_json_object(text: str) -> dict:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if fenced:
        return json.loads(fenced.group(1))
    match = re.search(r"\{.*\}", text, re.S)
    if match:
        return json.loads(match.group(0))
    raise ValueError("DeepSeek response did not contain JSON")


class DeepSeekSentimentReranker:
    """TopN A-share sentiment reranker.

    This is intentionally batch-based: one DeepSeek request ranks a small TopN
    pool using locally cached news. It avoids full-market LLM calls and never
    blocks basic selection when the API is unavailable.
    """

    def __init__(self, config: Dict[str, Any] | None = None):
        _load_env_file()
        self.config = config or {}
        self.api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.base_url = os.getenv("DEEPSEEK_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.deepseek.com/v1"
        # Strategy config takes precedence so the rerank path can use the faster
        # chat model even if other offline analysis scripts prefer reasoner/pro.
        self.model = self.config.get("model") or os.getenv("DEEPSEEK_MODEL") or "deepseek-chat"
        self.timeout_seconds = float(self.config.get("timeout_seconds") or 25)

    def is_available(self) -> bool:
        return bool(self.api_key)

    def load_recent_news(self, codes: List[str], max_news_per_stock: int = 3) -> Dict[str, List[Dict[str, Any]]]:
        if not codes:
            return {}
        placeholders = ",".join(["%s"] * len(codes))
        sql = f"""
        SELECT code, title, summary, source, published_at, sentiment_score, credibility_score, quality_score
        FROM stock_news
        WHERE code IN ({placeholders})
          AND published_at IS NOT NULL
          AND published_at >= DATE_SUB(NOW(), INTERVAL 14 DAY)
        ORDER BY code, published_at DESC, id DESC
        """
        result: Dict[str, List[Dict[str, Any]]] = {code: [] for code in codes}
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, codes)
                for row in cursor.fetchall() or []:
                    code = row.get("code")
                    if code not in result or len(result[code]) >= max_news_per_stock:
                        continue
                    result[code].append({
                        "title": row.get("title"),
                        "summary": row.get("summary"),
                        "source": row.get("source"),
                        "published_at": str(row.get("published_at")) if row.get("published_at") else None,
                        "sentiment_score": row.get("sentiment_score"),
                        "credibility_score": row.get("credibility_score"),
                        "quality_score": row.get("quality_score"),
                    })
        return result

    def rerank_sectors(self, sectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not sectors:
            return {"enabled": True, "available": self.is_available(), "items": [], "error": None}
        if not self.is_available():
            return {"enabled": True, "available": False, "items": [], "error": "DEEPSEEK_API_KEY is not configured"}

        payload_items = []
        for sector in sectors:
            payload_items.append({
                "sector_type": sector.get("sector_type"),
                "sector_name": sector.get("sector_name"),
                "local_sector_score": sector.get("sector_score"),
                "weighted_impact_score": sector.get("weighted_impact_score"),
                "news_count": sector.get("news_count"),
                "source_count": sector.get("source_count"),
                "positive_news_count": sector.get("positive_news_count"),
                "negative_news_count": sector.get("negative_news_count"),
                "top_news": (sector.get("top_news") or [])[:5],
                "tavily_news": (sector.get("tavily_news") or [])[:5],
            })

        prompt = """
你是A股短线题材和市场舆情分析助手。请基于本地热点聚合和 Tavily 搜索新闻，对候选热点板块/主题做精排。
不要编造新闻；Tavily 新闻不足、主题过宽、利好不直接、或已经明显过热时要降低置信度和分数。
优先考虑：政策/订单/产业催化是否真实、是否多源验证、是否能映射到A股可交易板块、是否有延续性。

请输出严格JSON，不要Markdown：
{
  "items": [
    {
      "sector_type": "industry/theme",
      "sector_name": "板块或主题名",
      "ai_sector_score": 0到100,
      "confidence": 0到1,
      "label": "positive/neutral/negative",
      "summary": "不超过36字",
      "opportunities": ["机会1", "机会2"],
      "risks": ["风险1", "风险2"]
    }
  ]
}

候选板块：
""".strip() + "\n" + json.dumps(payload_items, ensure_ascii=False, default=str)

        try:
            response = requests.post(
                f"{self.base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            parsed = _extract_json_object(content)
            items = parsed.get("items") if isinstance(parsed, dict) else []
            normalized = []
            for item in items or []:
                if not item.get("sector_name"):
                    continue
                normalized.append({
                    "sector_type": item.get("sector_type") or "theme",
                    "sector_name": str(item.get("sector_name")),
                    "ai_sector_score": _score_0_100(item.get("ai_sector_score")),
                    "confidence": max(0.0, min(float(item.get("confidence") or 0), 1.0)),
                    "label": item.get("label") or "neutral",
                    "summary": item.get("summary") or "",
                    "opportunities": item.get("opportunities") or [],
                    "risks": item.get("risks") or [],
                })
            return {"enabled": True, "available": True, "items": normalized, "error": None, "model": self.model}
        except Exception as exc:
            return {"enabled": True, "available": True, "items": [], "error": str(exc)[:500], "model": self.model}

    def rerank(self, candidates: List[Dict[str, Any]], max_news_per_stock: int = 3) -> Dict[str, Any]:
        if not candidates:
            return {"enabled": True, "available": self.is_available(), "items": [], "error": None}
        if not self.is_available():
            return {"enabled": True, "available": False, "items": [], "error": "DEEPSEEK_API_KEY is not configured"}

        codes = [str(item.get("code")) for item in candidates if item.get("code")]
        news_by_code = self.load_recent_news(codes, max_news_per_stock=max_news_per_stock)
        payload_items = []
        for item in candidates:
            code = str(item.get("code"))
            payload_items.append({
                "code": code,
                "name": item.get("name"),
                "base_score": item.get("score"),
                "factors": item.get("factors", {}),
                "raw_metrics": {
                    "sentiment_score": item.get("sentiment_score"),
                    "news_count": item.get("news_count"),
                    "net_mf_amount": item.get("net_mf_amount"),
                    "pct_chg_1d": item.get("pct_chg_1d"),
                    "volume_ratio": item.get("volume_ratio"),
                    "market_strength": item.get("market_strength"),
                    "opinion_sector_name": item.get("opinion_sector_name"),
                    "opinion_sector_type": item.get("opinion_sector_type"),
                    "opinion_match_type": item.get("opinion_match_type"),
                    "opinion_match_reason": item.get("opinion_match_reason"),
                    "opinion_news_count": item.get("opinion_news_count"),
                    "opinion_source_count": item.get("opinion_source_count"),
                    "opinion_sector_score": item.get("opinion_sector_score"),
                    "opinion_stock_score": item.get("opinion_stock_score"),
                },
                "news": news_by_code.get(code, []),
            })

        prompt = """
你是A股短线舆情和题材分析助手。请只基于我提供的本地缓存新闻和量价摘要，对候选股做舆情精排。
不要编造不存在的新闻；新闻不足时降低置信度。A股语境下要特别关注：政策/订单/业绩/监管/减持/问询/涨价/行业景气/资金确认/是否过热。

请输出严格JSON，不要Markdown：
{
  "items": [
    {
      "code": "股票代码",
      "ai_sentiment_score": 0到100,
      "confidence": 0到1,
      "label": "positive/neutral/negative",
      "summary": "不超过30字",
      "opportunities": ["机会1", "机会2"],
      "risks": ["风险1", "风险2"]
    }
  ]
}

候选数据：
""".strip() + "\n" + json.dumps(payload_items, ensure_ascii=False, default=str)

        try:
            response = requests.post(
                f"{self.base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            parsed = _extract_json_object(content)
            items = parsed.get("items") if isinstance(parsed, dict) else []
            normalized = []
            for item in items or []:
                if not item.get("code"):
                    continue
                normalized.append({
                    "code": str(item.get("code")),
                    "ai_sentiment_score": _score_0_100(item.get("ai_sentiment_score")),
                    "confidence": max(0.0, min(float(item.get("confidence") or 0), 1.0)),
                    "label": item.get("label") or "neutral",
                    "summary": item.get("summary") or "",
                    "opportunities": item.get("opportunities") or [],
                    "risks": item.get("risks") or [],
                })
            return {"enabled": True, "available": True, "items": normalized, "error": None, "model": self.model}
        except Exception as exc:
            return {"enabled": True, "available": True, "items": [], "error": str(exc)[:500], "model": self.model}
