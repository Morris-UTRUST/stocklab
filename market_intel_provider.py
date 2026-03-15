import html
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import quote_plus

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROVIDER_VERSION = "market-intel-v1"
DEFAULT_TIMEOUT = float(os.getenv("MARKET_INTEL_TIMEOUT_SECONDS", "8"))
DEFAULT_RETRIES = int(os.getenv("MARKET_INTEL_RETRIES", "2"))
DEFAULT_DISCLOSURE_LIMIT = int(os.getenv("MARKET_INTEL_DISCLOSURE_LIMIT", "5"))
DEFAULT_NEWS_LIMIT = int(os.getenv("MARKET_INTEL_NEWS_LIMIT", "6"))
USER_AGENT = os.getenv(
    "MARKET_INTEL_USER_AGENT",
    "stocklab-market-intel/1.0 (+https://localhost)",
)
MOPS_ALLOW_INSECURE_TLS = os.getenv("MARKET_INTEL_MOPS_ALLOW_INSECURE_TLS", "1") == "1"


class _TableTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        if tag == "tr":
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []
        elif tag == "br" and self._current_cell is not None:
            self._current_cell.append("\n")

    def handle_endtag(self, tag: str):
        if tag in {"td", "th"} and self._current_row is not None and self._current_cell is not None:
            cell = html.unescape("".join(self._current_cell)).strip()
            self._current_row.append(_normalize_whitespace(cell))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if any(self._current_row):
                self.rows.append(self._current_row)
            self._current_row = None

    def handle_data(self, data: str):
        if self._current_cell is not None:
            self._current_cell.append(data)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _build_session() -> requests.Session:
    retry = Retry(
        total=DEFAULT_RETRIES,
        connect=DEFAULT_RETRIES,
        read=DEFAULT_RETRIES,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _request_text(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: float,
    **kwargs: Any,
) -> str:
    if kwargs.get("verify") is False:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = session.request(method, url, timeout=timeout, **kwargs)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def _parse_pub_date(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(raw, fmt).isoformat(timespec="seconds")
        except ValueError:
            continue
    return raw


def _parse_mops_rows(html_text: str, stock_id: str, limit: int) -> list[dict[str, Any]]:
    parser = _TableTextExtractor()
    parser.feed(html_text)
    items: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str]] = set()
    date_pattern = re.compile(r"(20\d{2}[/-]\d{1,2}[/-]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)")

    for row in parser.rows:
        joined = " | ".join(row)
        if stock_id not in joined:
            continue

        matches = [cell for cell in row if stock_id in cell]
        if not matches:
            continue

        title = ""
        for cell in row:
            if len(cell) >= 8 and stock_id not in cell and not date_pattern.search(cell):
                title = cell
                break
        if not title:
            title = matches[0]

        date_value = None
        for cell in row:
            m = date_pattern.search(cell)
            if m:
                date_value = m.group(1).replace("/", "-")
                break

        summary = _normalize_whitespace("；".join(cell for cell in row if cell and cell != title))[:280]
        key = (date_value, title)
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "date": date_value,
                "title": title,
                "summary": summary,
                "source": "MOPS",
                "source_url": "https://mops.twse.com.tw/mops/web/t05sr01_1",
            }
        )
        if len(items) >= limit:
            break

    return items


def _fetch_exchange_disclosures_from_mops(session: requests.Session, stock_id: str, limit: int) -> list[dict[str, Any]]:
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "TYPEK": "all",
        "co_id": stock_id,
    }
    html_text = _request_text(
        session,
        "POST",
        "https://mops.twse.com.tw/mops/web/ajax_t05sr01_1",
        timeout=DEFAULT_TIMEOUT,
        data=payload,
        verify=not MOPS_ALLOW_INSECURE_TLS,
    )
    return _parse_mops_rows(html_text, stock_id, limit)


def _fetch_exchange_disclosures_from_rss(session: requests.Session, stock_id: str, limit: int) -> list[dict[str, Any]]:
    rss_candidates = [
        "https://mops.twse.com.tw/nas/rss/mopsrss.xml",
        "https://mopsov.twse.com.tw/nas/rss/mopsrss.xml",
    ]
    for url in rss_candidates:
        xml_text = _request_text(
            session,
            "GET",
            url,
            timeout=DEFAULT_TIMEOUT,
            verify=not MOPS_ALLOW_INSECURE_TLS,
        )
        root = ET.fromstring(xml_text)
        items: list[dict[str, Any]] = []
        seen: set[tuple[str | None, str]] = set()
        for node in root.findall(".//item"):
            title = _normalize_whitespace(node.findtext("title", default=""))
            desc = _normalize_whitespace(node.findtext("description", default=""))
            if stock_id not in f"{title} {desc}":
                continue
            link = _normalize_whitespace(node.findtext("link", default="")) or url
            pub_date = _parse_pub_date(node.findtext("pubDate"))
            summary = desc[:280] if desc else title
            key = (pub_date, title)
            if key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "date": pub_date,
                    "title": title or f"{stock_id} 重大訊息",
                    "summary": summary,
                    "source": "MOPS RSS",
                    "source_url": link,
                }
            )
            if len(items) >= limit:
                return items
        if items:
            return items
    return []


def _feed_items_from_google(xml_text: str, limit: int) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    items: list[dict[str, Any]] = []
    for node in root.findall(".//item"):
        title = _normalize_whitespace(node.findtext("title", default=""))
        link = _normalize_whitespace(node.findtext("link", default=""))
        pub_date = _parse_pub_date(node.findtext("pubDate"))
        description = _normalize_whitespace(node.findtext("description", default=""))
        source = _normalize_whitespace(node.findtext("source", default="Google News"))
        if not title:
            continue
        items.append(
            {
                "title": title,
                "summary": description[:280] if description else title,
                "published_at": pub_date,
                "source": source or "Google News",
                "source_url": link,
            }
        )
        if len(items) >= limit:
            break
    return items


def _feed_items_from_bing(xml_text: str, limit: int) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    items: list[dict[str, Any]] = []
    for node in root.findall(".//item"):
        title = _normalize_whitespace(node.findtext("title", default=""))
        link = _normalize_whitespace(node.findtext("link", default=""))
        pub_date = _parse_pub_date(node.findtext("pubDate"))
        description = _normalize_whitespace(node.findtext("description", default=""))
        if not title:
            continue
        items.append(
            {
                "title": title,
                "summary": description[:280] if description else title,
                "published_at": pub_date,
                "source": "Bing News",
                "source_url": link,
            }
        )
        if len(items) >= limit:
            break
    return items


def _fetch_news_from_google(session: requests.Session, query: str, limit: int) -> list[dict[str, Any]]:
    url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    xml_text = _request_text(session, "GET", url, timeout=DEFAULT_TIMEOUT)
    return _feed_items_from_google(xml_text, limit)


def _fetch_exchange_disclosures_from_google_news(session: requests.Session, stock_id: str, stock_name: str | None, limit: int) -> list[dict[str, Any]]:
    query_bits = [stock_id]
    if stock_name:
        query_bits.append(stock_name)
    query = " ".join(query_bits + ["重大訊息", "site:mops.twse.com.tw"])
    items = _fetch_news_from_google(session, query, limit)
    mapped: list[dict[str, Any]] = []
    for item in items:
        mapped.append(
            {
                "date": item.get("published_at"),
                "title": item.get("title") or f"{stock_id} 重大訊息",
                "summary": item.get("summary") or item.get("title") or "",
                "source": "Google News (MOPS keyword)",
                "source_url": item.get("source_url") or "https://news.google.com",
            }
        )
    return mapped


def _fetch_news_from_bing(session: requests.Session, query: str, limit: int) -> list[dict[str, Any]]:
    url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"
    xml_text = _request_text(session, "GET", url, timeout=DEFAULT_TIMEOUT)
    return _feed_items_from_bing(xml_text, limit)


def _dedupe_news(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        key = (item.get("title") or "", item.get("source_url") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def _empty_result(stock_id: str, stock_name: str | None = None) -> dict[str, Any]:
    return {
        "exchange_disclosures": [],
        "industry_news": [],
        "meta": {
            "stock_id": stock_id,
            "stock_name": stock_name or "",
            "provider_version": PROVIDER_VERSION,
            "fetched_at": _utc_now_iso(),
            "degraded": True,
            "errors": [],
            "exchange_disclosures_source": "empty",
            "industry_news_source": "empty",
        },
    }


def get_market_intel(
    stock_id: str,
    stock_name: str | None = None,
    *,
    session: requests.Session | None = None,
    disclosure_limit: int = DEFAULT_DISCLOSURE_LIMIT,
    news_limit: int = DEFAULT_NEWS_LIMIT,
) -> dict[str, Any]:
    started = time.perf_counter()
    result = _empty_result(stock_id, stock_name)
    local_session = session or _build_session()
    owns_session = session is None
    errors: list[str] = []

    try:
        disclosures: list[dict[str, Any]] = []
        disclosure_source = "empty"
        for source_name, fn in [
            ("mops_ajax", lambda s, sid, lim: _fetch_exchange_disclosures_from_mops(s, sid, lim)),
            ("mops_rss", lambda s, sid, lim: _fetch_exchange_disclosures_from_rss(s, sid, lim)),
            (
                "google_news_mops_keyword",
                lambda s, sid, lim: _fetch_exchange_disclosures_from_google_news(s, sid, stock_name, lim),
            ),
        ]:
            try:
                disclosures = fn(local_session, stock_id, disclosure_limit)
                if disclosures:
                    disclosure_source = source_name
                    break
            except Exception as exc:
                errors.append(f"{source_name}:{exc}")

        query_bits = [stock_id]
        if stock_name:
            query_bits.append(stock_name)
        news_query = " ".join(query_bits + ["台股"])
        news_items: list[dict[str, Any]] = []
        news_source = "empty"
        for source_name, fn in [
            ("google_news_rss", _fetch_news_from_google),
            ("bing_news_rss", _fetch_news_from_bing),
        ]:
            try:
                batch = fn(local_session, news_query, news_limit)
                if batch:
                    news_items = _dedupe_news(batch, news_limit)
                    news_source = source_name
                    break
            except Exception as exc:
                errors.append(f"{source_name}:{exc}")

        result["exchange_disclosures"] = disclosures
        result["industry_news"] = news_items
        result["meta"] = {
            "stock_id": stock_id,
            "stock_name": stock_name or "",
            "provider_version": PROVIDER_VERSION,
            "fetched_at": _utc_now_iso(),
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
            "degraded": not (disclosures or news_items),
            "errors": errors,
            "exchange_disclosures_source": disclosure_source,
            "industry_news_source": news_source,
            "exchange_disclosures_count": len(disclosures),
            "industry_news_count": len(news_items),
        }
        return result
    except Exception as exc:
        result["meta"]["errors"] = errors + [f"provider:{exc}"]
        result["meta"]["latency_ms"] = round((time.perf_counter() - started) * 1000, 2)
        return result
    finally:
        if owns_session:
            local_session.close()
