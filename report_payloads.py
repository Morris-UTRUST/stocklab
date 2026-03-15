import os
from datetime import date
from typing import Any

import numpy as np
import requests

from market_intel_provider import get_market_intel


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    if isinstance(value, dict):
        return {k: sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(v) for v in value]
    return value


def _legacy_event_adjustments(market_intel: dict[str, Any], limit: int = 6) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for disclosure in market_intel.get("exchange_disclosures", []):
        items.append(
            {
                "type": "exchange_disclosure",
                "date": disclosure.get("date"),
                "title": disclosure.get("title"),
                "summary": disclosure.get("summary"),
                "source": disclosure.get("source"),
                "source_url": disclosure.get("source_url"),
            }
        )
    for news in market_intel.get("industry_news", []):
        items.append(
            {
                "type": "industry_news",
                "date": news.get("published_at"),
                "title": news.get("title"),
                "summary": news.get("summary"),
                "source": news.get("source"),
                "source_url": news.get("source_url"),
            }
        )
    return items[:limit]


def _fallback_market_intel(stock_id: str, stock_name: str) -> dict[str, Any]:
    return {
        "exchange_disclosures": [],
        "industry_news": [],
        "meta": {
            "stock_id": stock_id,
            "stock_name": stock_name,
            "provider_version": "market-intel-fallback",
            "fetched_at": str(date.today()),
            "degraded": True,
            "errors": [],
            "exchange_disclosures_source": "empty",
            "industry_news_source": "empty",
        },
    }


def build_report_payload(stock_id: str, stock_name: str, analysis: dict[str, Any]) -> dict[str, Any]:
    try:
        market_intel = get_market_intel(stock_id, stock_name)
    except Exception as exc:
        market_intel = _fallback_market_intel(stock_id, stock_name)
        market_intel["meta"]["errors"] = [f"provider:{exc}"]

    payload = {
        "stock_id": stock_id,
        "stock_name": stock_name,
        "as_of_date": str(date.today()),
        "integrated_score": analysis["score"],
        "action": analysis["action"],
        "trend": analysis["trend"],
        "momentum": analysis["momentum"],
        "risk": analysis["risk"],
        "notes": analysis["notes"],
        "alerts": analysis["alerts"],
        "features": analysis["features"],
        "exchange_disclosures": market_intel.get("exchange_disclosures", []),
        "industry_news": market_intel.get("industry_news", []),
        "market_intel_meta": market_intel.get("meta", {}),
        # Backward compatibility for any downstream consumer still reading this field.
        "event_adjustments": _legacy_event_adjustments(market_intel),
    }
    return sanitize_json_value(payload)


def generate_report_bundle(stock_id: str, stock_name: str, analysis: dict[str, Any]) -> tuple[dict[str, Any], str]:
    payload = build_report_payload(stock_id, stock_name, analysis)
    report_markdown = request_ai_hub_report(payload)
    return payload, report_markdown


def request_ai_hub_report(payload: dict[str, Any]) -> str:
    hub_url = os.getenv("AI_HUB_URL", "http://127.0.0.1:8787/report/investment")
    try:
        resp = requests.post(hub_url, json=payload, timeout=240)
        resp.raise_for_status()
        data = resp.json()
        return data.get("report_markdown", "### AI 報告回傳格式錯誤")
    except Exception as exc:
        return (
            "### AI 投資報告產生失敗（中樞未連線）\n"
            f"錯誤：{exc}\n\n"
            "請先啟動本機 AI Hub：`python ai_hub_server.py`，\n"
            "讓所有系統都透過小芳中樞統一生成報告。"
        )
