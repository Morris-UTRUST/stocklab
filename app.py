import json
import os
import re
import sqlite3
import subprocess
import threading
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from fetcher import fetch_taiwan_stock_price, upsert_to_sqlite
from report_jobs import (
    enqueue_report_job,
    ensure_report_jobs_table,
    get_latest_finished_report_job,
    get_latest_report_job,
    launch_report_worker,
)
from report_payloads import generate_report_bundle

DB_PATH = "stocklab.db"
DEFAULT_WATCHLIST = ["3105", "2330", "2317"]
BENCHMARK_ID = "0050"
STOCK_NAME_MAP = {
    "3105": "穩懋",
    "2330": "台積電",
    "2317": "鴻海",
    "0050": "元大台灣50",
}

st.set_page_config(page_title="台股量化分析智能蝦", page_icon="🦐", layout="centered")

# 手機閱讀優化：整體字體略縮小、卡片更緊湊
st.markdown(
    """
    <style>
      html, body, [class*="css"]  { font-size: 14px; }
      .stMetric label, .stMetric div { font-size: 0.9rem !important; }
      .report-box {
        background: linear-gradient(135deg, #f3f8ff 0%, #eefaf3 100%);
        border: 1px solid #cfe2ff;
        border-left: 6px solid #3b82f6;
        border-radius: 12px;
        padding: 14px 14px 6px 14px;
        margin: 8px 0 14px 0;
      }
      .report-title { font-weight: 700; color: #1e3a8a; margin-bottom: 6px; font-size: 1rem; }
      .report-box h1, .report-box h2, .report-box h3 {
        font-size: 1rem !important;
        line-height: 1.35 !important;
        margin: 0.35rem 0 !important;
      }
      /* AI 報告 markdown 標題統一縮小，避免過大 */
      .stMarkdown h2 { font-size: 1.05rem !important; line-height: 1.35 !important; }
      .stMarkdown h3 { font-size: 0.98rem !important; line-height: 1.35 !important; }
      .report-box p, .report-box li { font-size: 0.92rem !important; }
      .stPlotlyChart { padding-left: 0; padding-right: 0; }
      h1 { white-space: nowrap; font-size: 1.8rem !important; }
      .step-title {
        color: #60a5fa;
        font-weight: 900;
        font-size: 1.12rem;
        margin: 4px 0 4px 0;
      }
      .step-lines {
        margin: 0 0 6px 0;
        line-height: 1.32;
        font-size: 0.93rem;
      }
      .step-lines p { margin: 0.08rem 0 !important; }
      .key-metric-label { font-size: 1.05rem !important; font-weight: 800 !important; color: #60a5fa !important; }
      .key-metric-value { font-size: 1.2rem !important; font-weight: 800 !important; }
      .stock-name-pill {
        display: inline-block;
        background: #eef2ff;
        color: #312e81;
        border: 1px solid #c7d2fe;
        border-radius: 999px;
        padding: 2px 10px;
        font-size: 12px;
        margin-left: 6px;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🦐 台股量化分析智能蝦")
st.caption("手機友善版：追蹤個股、量化儀表板、買賣看門狗（參考訊號，不自動下單）")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                stock_id TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_meta (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_name_cache (
                stock_id TEXT PRIMARY KEY,
                stock_name TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                emails TEXT NOT NULL,
                stock_ids_json TEXT NOT NULL,
                schedule_type TEXT NOT NULL,
                schedule_time TEXT NOT NULL,
                timezone TEXT NOT NULL DEFAULT 'Asia/Taipei',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL,
                run_time TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                email_to TEXT NOT NULL,
                subject TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT
            )
            """
        )
        conn.commit()
        ensure_report_jobs_table()

        cur = conn.execute("SELECT COUNT(*) FROM watchlist")
        if cur.fetchone()[0] == 0:
            now = datetime.now().isoformat(timespec="seconds")
            conn.executemany(
                "INSERT OR IGNORE INTO watchlist(stock_id, enabled, created_at) VALUES (?, 1, ?)",
                [(s, now) for s in DEFAULT_WATCHLIST],
            )
            conn.commit()
    finally:
        conn.close()


def get_stock_name_from_cache(stock_id: str) -> str | None:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT stock_name FROM stock_name_cache WHERE stock_id=?", (stock_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def fetch_stock_name_from_api(stock_id: str) -> str | None:
    # 優先使用 FinMind TaiwanStockInfo
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockInfo", "data_id": stock_id},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", [])
        if data:
            name = data[0].get("stock_name")
            if name:
                return str(name)
    except Exception:
        pass
    return None


def resolve_stock_name(stock_id: str) -> str:
    if stock_id in STOCK_NAME_MAP:
        return STOCK_NAME_MAP[stock_id]

    cached = get_stock_name_from_cache(stock_id)
    if cached:
        return cached

    fetched = fetch_stock_name_from_api(stock_id)
    if fetched:
        conn = get_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO stock_name_cache(stock_id, stock_name, updated_at) VALUES (?, ?, ?)",
                (stock_id, fetched, datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()
        finally:
            conn.close()
        STOCK_NAME_MAP[stock_id] = fetched
        return fetched

    return "未知名稱"


def get_watchlist() -> list[str]:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT stock_id FROM watchlist WHERE enabled=1 ORDER BY stock_id", conn
        )
        return df["stock_id"].astype(str).tolist()
    finally:
        conn.close()


def add_watchlist(stock_id: str):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO watchlist(stock_id, enabled, created_at) VALUES (?, 1, COALESCE((SELECT created_at FROM watchlist WHERE stock_id=?), ?))",
            (stock_id, stock_id, datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()
    finally:
        conn.close()


def remove_watchlist(stock_id: str):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM watchlist WHERE stock_id=?", (stock_id,))
        conn.commit()
    finally:
        conn.close()


def set_meta(key: str, value: str):
    conn = get_conn()
    try:
        conn.execute("INSERT OR REPLACE INTO app_meta(k, v) VALUES (?, ?)", (key, value))
        conn.commit()
    finally:
        conn.close()


def get_meta(key: str) -> str | None:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT v FROM app_meta WHERE k=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def save_subscription(emails: str, stock_ids: list[str], schedule_type: str, schedule_time: str):
    now = datetime.now().isoformat(timespec="seconds")
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO report_subscriptions(emails, stock_ids_json, schedule_type, schedule_time, timezone, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'Asia/Taipei', 1, ?, ?)
            """,
            (emails, json.dumps(stock_ids, ensure_ascii=False), schedule_type, schedule_time, now, now),
        )
        conn.commit()
    finally:
        conn.close()


def list_subscriptions() -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT id, emails, stock_ids_json, schedule_type, schedule_time, enabled, updated_at FROM report_subscriptions ORDER BY id DESC",
            conn,
        )
        return df
    finally:
        conn.close()


def toggle_subscription(sub_id: int, enabled: int):
    conn = get_conn()
    try:
        conn.execute("UPDATE report_subscriptions SET enabled=?, updated_at=? WHERE id=?", (enabled, datetime.now().isoformat(timespec="seconds"), sub_id))
        conn.commit()
    finally:
        conn.close()


def delete_subscription(sub_id: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM report_subscriptions WHERE id=?", (sub_id,))
        conn.commit()
    finally:
        conn.close()


def markdown_to_plain_text(md: str) -> str:
    txt = md or ""
    txt = txt.replace("**", "")
    txt = txt.replace("__", "")
    txt = txt.replace("`", "")
    txt = re.sub(r"^\s{0,3}#{1,6}\s?", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"^\s*[-*]\s+", "- ", txt, flags=re.MULTILINE)
    txt = re.sub(r"^\s*---+\s*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


def send_report_email(to_emails: str, subject: str, body: str) -> tuple[bool, str]:
    cmd = [
        "gog",
        "gmail",
        "send",
        "--account",
        "morris@utrust.com.tw",
        "--to",
        to_emails,
        "--subject",
        subject,
        "--body",
        body,
        "--no-input",
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if p.returncode == 0:
            return True, "ok"
        return False, (p.stderr or p.stdout or "send failed").strip()[:300]
    except Exception as e:
        return False, str(e)


def log_report_run(subscription_id: int, stock_id: str, email_to: str, subject: str, status: str, error_message: str = ""):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO report_runs(subscription_id, run_time, stock_id, email_to, subject, status, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                subscription_id,
                datetime.now().isoformat(timespec="seconds"),
                stock_id,
                email_to,
                subject,
                status,
                error_message,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def queue_subscription_run(subscription_id: int) -> tuple[bool, str]:
    try:
        t = threading.Thread(target=run_subscription_once, args=(subscription_id,), daemon=True)
        t.start()
        return True, "已加入即時寄送佇列，系統會在背景完成寄送。"
    except Exception as e:
        return False, str(e)


def run_subscription_once(subscription_id: int):
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, emails, stock_ids_json FROM report_subscriptions WHERE id=? AND enabled=1",
            (subscription_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return "找不到可執行的訂閱"

    _, emails, stock_ids_json = row
    stock_ids = json.loads(stock_ids_json)
    out = []
    for sid in stock_ids:
        name = resolve_stock_name(sid)
        try:
            update_stock(sid, "2024-01-01", None)
            d = load_price(sid)
            if d.empty:
                out.append(f"{sid}: 無資料")
                continue
            analysis = build_integrated_analysis(d)
            report = generate_llm_report(sid, name, analysis)
            report_plain = markdown_to_plain_text(report)
            subject = f"[智能蝦] {sid} {name} 量化報告 {date.today().isoformat()}"
            body = f"股票：{sid} {name}\n\n" + report_plain
            ok, msg = send_report_email(emails, subject, body)
            if ok:
                log_report_run(subscription_id, sid, emails, subject, "success", "")
                out.append(f"{sid}: 寄送成功")
            else:
                log_report_run(subscription_id, sid, emails, subject, "failed", msg)
                out.append(f"{sid}: 寄送失敗 - {msg}")
        except Exception as e:
            log_report_run(subscription_id, sid, emails, f"[智能蝦] {sid} 報告失敗", "failed", str(e))
            out.append(f"{sid}: 執行錯誤 - {e}")
    return "\n".join(out)


def load_price(stock_id: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT date, stock_id, open, max, min, close, Trading_Volume
            FROM stock_price_daily
            WHERE stock_id = ?
            ORDER BY date
            """,
            conn,
            params=(stock_id,),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "max", "min", "close", "Trading_Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["ma120"] = df["close"].rolling(120).mean()
    df["vol_ma20"] = df["Trading_Volume"].rolling(20).mean()

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = df["macd_dif"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd_dif"] - df["macd_dea"]

    # RSI(14)
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))

    # KD(9,3,3)
    low9 = df["min"].rolling(9).min()
    high9 = df["max"].rolling(9).max()
    rsv = ((df["close"] - low9) / (high9 - low9).replace(0, np.nan)) * 100
    df["k"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["d"] = df["k"].ewm(alpha=1 / 3, adjust=False).mean()

    # Bollinger Bands(20,2)
    bb_mid = df["close"].rolling(20).mean()
    bb_std = df["close"].rolling(20).std()
    df["bb_mid"] = bb_mid
    df["bb_up"] = bb_mid + 2 * bb_std
    df["bb_low"] = bb_mid - 2 * bb_std

    # TR / ATR(14)
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["max"] - df["min"]).abs(),
        (df["max"] - prev_close).abs(),
        (df["min"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1 / 14, adjust=False).mean()

    # ADX / DMI(14)
    up_move = df["max"].diff()
    down_move = df["min"].shift(1) - df["min"]
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    atr = df["atr14"].replace(0, np.nan)
    plus_di = 100 * plus_dm.ewm(alpha=1 / 14, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / 14, adjust=False).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df["plus_di"] = plus_di
    df["minus_di"] = minus_di
    df["adx14"] = dx.ewm(alpha=1 / 14, adjust=False).mean()

    # OBV
    direction = np.sign(df["close"].diff().fillna(0))
    df["obv"] = (direction * df["Trading_Volume"]).fillna(0).cumsum()

    # MFI(14)
    typical = (df["max"] + df["min"] + df["close"]) / 3
    money_flow = typical * df["Trading_Volume"]
    tp_diff = typical.diff()
    pos_mf = money_flow.where(tp_diff > 0, 0.0)
    neg_mf = money_flow.where(tp_diff < 0, 0.0).abs()
    pos_sum = pos_mf.rolling(14).sum()
    neg_sum = neg_mf.rolling(14).sum().replace(0, np.nan)
    mfr = pos_sum / neg_sum
    df["mfi14"] = 100 - (100 / (1 + mfr))

    # VWAP（日線近似）
    tp = (df["max"] + df["min"] + df["close"]) / 3
    cum_v = df["Trading_Volume"].replace(0, np.nan).cumsum()
    df["vwap"] = (tp * df["Trading_Volume"]).cumsum() / cum_v

    # return/volatility/drawdown
    df["ret"] = df["close"].pct_change().fillna(0)
    df["rolling_vol_20"] = df["ret"].rolling(20).std() * np.sqrt(252)
    nav = (1 + df["ret"]).cumprod()
    running_max = nav.cummax()
    df["drawdown"] = (nav / running_max) - 1

    prev_close = df["close"].shift(1)
    prev_ma20 = df["ma20"].shift(1)
    buy_cross_ma20 = (prev_close <= prev_ma20) & (df["close"] > df["ma20"])
    buy_macd_turn = (df["macd_hist"].shift(1) <= 0) & (df["macd_hist"] > 0)
    buy = (buy_cross_ma20 & (df["Trading_Volume"] > df["vol_ma20"])) | buy_macd_turn

    sell_break_ma20 = (prev_close >= prev_ma20) & (df["close"] < df["ma20"])
    sell_macd_weak = (df["macd_hist"] < 0) & (df["close"] < df["ma60"])
    sell = sell_break_ma20 | sell_macd_weak

    signal = pd.Series("HOLD", index=df.index)
    signal = signal.mask(buy, "BUY")
    signal = signal.mask(sell, "SELL")
    df["signal"] = signal
    return df


def load_benchmark_df(stock_id: str = BENCHMARK_ID) -> pd.DataFrame:
    conn = get_conn()
    try:
        b = pd.read_sql_query(
            "SELECT date, close FROM stock_price_daily WHERE stock_id=? ORDER BY date",
            conn,
            params=(stock_id,),
        )
    finally:
        conn.close()
    if b.empty:
        return b
    b["date"] = pd.to_datetime(b["date"])
    b["close"] = pd.to_numeric(b["close"], errors="coerce")
    b["bench_ret"] = b["close"].pct_change().fillna(0)
    b["bench_nav"] = (1 + b["bench_ret"]).cumprod()
    return b


def build_integrated_analysis(df: pd.DataFrame) -> dict:
    latest = df.iloc[-1]

    score = 50
    notes = []
    alerts = []

    close = float(latest["close"])
    ma20 = float(latest["ma20"]) if pd.notna(latest["ma20"]) else close
    ma60 = float(latest["ma60"]) if pd.notna(latest["ma60"]) else close
    ma120 = float(latest["ma120"]) if pd.notna(latest["ma120"]) else close
    hist = float(latest["macd_hist"]) if pd.notna(latest["macd_hist"]) else 0
    dif = float(latest["macd_dif"]) if pd.notna(latest["macd_dif"]) else 0
    dea = float(latest["macd_dea"]) if pd.notna(latest["macd_dea"]) else 0
    vol = float(latest["Trading_Volume"]) if pd.notna(latest["Trading_Volume"]) else 0
    vol_ma20 = float(latest["vol_ma20"]) if pd.notna(latest["vol_ma20"]) else max(vol, 1)
    drawdown = float(latest["drawdown"]) if pd.notna(latest["drawdown"]) else 0

    if close > ma20 > ma60:
        score += 12
        notes.append("趨勢偏多（價在 MA20/MA60 之上）")
    elif close < ma20 < ma60:
        score -= 12
        notes.append("趨勢轉弱（價在 MA20/MA60 之下）")
    else:
        notes.append("趨勢整理中（均線結構未完全表態）")

    if close > ma120:
        score += 6
    else:
        score -= 6

    if hist > 0 and dif > dea:
        score += 10
        notes.append("動能偏強（MACD 正向）")
    elif hist < 0 and dif < dea:
        score -= 10
        notes.append("動能偏弱（MACD 負向）")

    if vol > vol_ma20 * 1.2 and close > ma20:
        score += 8
        notes.append("量價配合良好（放量上行）")
    elif vol > vol_ma20 * 1.2 and close < ma20:
        score -= 8
        alerts.append("放量但跌破 MA20，疑似分歧訊號")

    dd_pct = drawdown * 100
    if dd_pct <= -20:
        score -= 15
        alerts.append("回撤已超過 20%，高風險區")
    elif dd_pct <= -10:
        score -= 8
        alerts.append("回撤超過 10%，留意風險擴大")

    if len(df) >= 2:
        prev_close = float(df.iloc[-2]["close"])
        day_ret = (close / prev_close - 1) * 100 if prev_close else 0
        if day_ret <= -5 and vol > vol_ma20 * 1.2:
            alerts.append("放量長黑（單日跌幅較大）")
        elif day_ret >= 5 and vol > vol_ma20 * 1.2:
            alerts.append("放量長紅（短線波動較大）")

    score = max(0, min(100, int(round(score))))
    if score >= 65:
        action = "偏多（參考 BUY）"
    elif score >= 45:
        action = "觀望（HOLD）"
    else:
        action = "偏空（參考 SELL）"

    trend_label = "偏多" if close > ma20 > ma60 else ("偏空" if close < ma20 < ma60 else "盤整")
    momentum_label = "增強" if hist > 0 else "鈍化"
    risk_label = "高" if dd_pct <= -20 else ("中" if dd_pct <= -10 else "低")

    recent20_high = float(df["close"].tail(20).max()) if len(df) >= 20 else close
    recent20_low = float(df["close"].tail(20).min()) if len(df) >= 20 else close
    recent60_high = float(df["close"].tail(60).max()) if len(df) >= 60 else recent20_high

    short_buy_low, short_buy_high = ma20 * 0.99, ma20 * 1.01
    short_sell_low, short_sell_high = recent20_high * 0.98, recent20_high
    long_buy_low, long_buy_high = ma60 * 0.98, ma60 * 1.02
    long_sell_low, long_sell_high = recent60_high * 0.97, recent60_high

    return {
        "score": score,
        "action": action,
        "trend": trend_label,
        "momentum": momentum_label,
        "risk": risk_label,
        "notes": notes[:3],
        "alerts": alerts[:3],
        "features": {
            "close": round(close, 3),
            "ma20": round(ma20, 3),
            "ma60": round(ma60, 3),
            "ma120": round(ma120, 3),
            "macd_dif": round(dif, 4),
            "macd_dea": round(dea, 4),
            "macd_hist": round(hist, 4),
            "volume": int(vol),
            "vol_ma20": int(vol_ma20) if vol_ma20 else 0,
            "drawdown_pct": round(dd_pct, 2),
            "recent20_high": round(recent20_high, 3),
            "recent20_low": round(recent20_low, 3),
            "recent60_high": round(recent60_high, 3),
            "short_buy_zone": [round(short_buy_low, 2), round(short_buy_high, 2)],
            "short_sell_zone": [round(short_sell_low, 2), round(short_sell_high, 2)],
            "long_buy_zone": [round(long_buy_low, 2), round(long_buy_high, 2)],
            "long_sell_zone": [round(long_sell_low, 2), round(long_sell_high, 2)],
        },
    }


def build_four_step_insights(df: pd.DataFrame, analysis: dict) -> list[dict]:
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest

    close = float(latest["close"])
    ma20 = float(latest["ma20"]) if pd.notna(latest["ma20"]) else close
    ma60 = float(latest["ma60"]) if pd.notna(latest["ma60"]) else close
    ma120 = float(latest["ma120"]) if pd.notna(latest["ma120"]) else close
    adx = float(latest["adx14"]) if pd.notna(latest.get("adx14")) else 0
    pdi = float(latest["plus_di"]) if pd.notna(latest.get("plus_di")) else 0
    mdi = float(latest["minus_di"]) if pd.notna(latest.get("minus_di")) else 0

    macd_hist = float(latest["macd_hist"]) if pd.notna(latest["macd_hist"]) else 0
    rsi = float(latest["rsi14"]) if pd.notna(latest.get("rsi14")) else 50
    k = float(latest["k"]) if pd.notna(latest.get("k")) else 50
    d = float(latest["d"]) if pd.notna(latest.get("d")) else 50

    vol = float(latest["Trading_Volume"]) if pd.notna(latest["Trading_Volume"]) else 0
    volma = float(latest["vol_ma20"]) if pd.notna(latest["vol_ma20"]) else 1
    obv_now = float(latest["obv"]) if pd.notna(latest.get("obv")) else 0
    obv_prev = float(prev["obv"]) if pd.notna(prev.get("obv")) else 0
    mfi = float(latest["mfi14"]) if pd.notna(latest.get("mfi14")) else 50

    atr = float(latest["atr14"]) if pd.notna(latest.get("atr14")) else 0
    vol20 = float(latest["rolling_vol_20"]) if pd.notna(latest.get("rolling_vol_20")) else 0
    dd = float(latest["drawdown"]) * 100 if pd.notna(latest["drawdown"]) else 0

    # Step 1 Trend
    trend_state = "多頭" if close > ma20 > ma60 else ("空頭" if close < ma20 < ma60 else "盤整")
    adx_state = "趨勢明確" if adx >= 25 else ("弱趨勢" if adx >= 20 else "震盪")
    step1 = {
        "title": "① 趨勢分析（MA + ADX）",
        "smart": f"目前屬於{trend_state}結構，{adx_state}。",
        "insight": f"價格{close:.2f} 與 MA20/60/120（{ma20:.2f}/{ma60:.2f}/{ma120:.2f}）的相對位置，顯示方向為 {trend_state}。",
        "alert": "若跌破 MA20 且 ADX 轉弱，短線趨勢可能降級。",
        "detail": f"【資料引用】close={close:.2f}，MA20={ma20:.2f}，MA60={ma60:.2f}，MA120={ma120:.2f}，ADX={adx:.2f}，+DI={pdi:.2f}，-DI={mdi:.2f}。\n【判讀步驟】(1) 先檢查 close 與 MA20/60 的相對位置，判斷短中期方向。 (2) 再檢查 MA20/60/120 是否形成多頭或空頭排列。 (3) 最後用 ADX 強度過濾，避免在震盪區把均線交叉誤當趨勢。\n【關鍵比較】若 ADX>=25 代表趨勢可交易性較高；若 ADX<20 多為震盪盤，應降低追價頻率。+DI 與 -DI 用來確認方向優勢是否與 MA 結構一致。\n【結論邏輯】只有方向（MA）與強度（ADX）同時成立，才視為高信度趨勢判讀。" ,
    }

    # Step 2 Momentum
    momentum = "偏強" if macd_hist > 0 else "偏弱"
    step2 = {
        "title": "② 動能分析（MACD + RSI/KD）",
        "smart": f"動能目前 {momentum}，短線節奏偏 {'上行' if macd_hist > 0 else '保守'}。",
        "insight": f"MACD HIST={macd_hist:.4f}、RSI={rsi:.2f}、KD={k:.2f}/{d:.2f}，顯示動能 {'延續' if macd_hist > 0 else '降溫'}。",
        "alert": "若 RSI 過熱且 KD 高檔鈍化，追價風險會提升。",
        "detail": f"【資料引用】MACD_hist={macd_hist:.4f}，RSI14={rsi:.2f}，K={k:.2f}，D={d:.2f}。\n【判讀步驟】(1) 先看 MACD_hist 正負，確認動能是擴張或收斂。 (2) 再比對 DIF/DEA 的相對關係（已反映在 hist）判斷動能方向。 (3) 以 RSI 判斷是否過熱/過冷，最後以 KD 看短線轉折是否提早出現。\n【關鍵比較】RSI>70 通常代表短線過熱，RSI<30 可能超跌；K 與 D 若在高檔鈍化，常見追價風險。\n【結論邏輯】MACD 提供中期節奏，RSI/KD 提供短線風險校正，兩者一致時訊號品質更高。", 
    }

    # Step 3 Volume
    vol_ratio = vol / volma if volma else 0
    vol_state = "量能充足" if vol_ratio >= 1.2 else ("量能中性" if vol_ratio >= 0.9 else "量能偏弱")
    obv_state = "資金流入" if obv_now >= obv_prev else "資金轉弱"
    step3 = {
        "title": "③ 量能分析（Volume + OBV/MFI）",
        "smart": f"目前{vol_state}，{obv_state}。",
        "insight": f"成交量/均量比={vol_ratio:.2f}，MFI={mfi:.2f}，顯示資金 {'支持趨勢' if vol_ratio>=1 and mfi>=50 else '尚待確認'}。",
        "alert": "若價漲量縮或 OBV 不跟，需防假突破。",
        "detail": f"【資料引用】Volume={int(vol)}，VolMA20={int(volma)}，量比={vol_ratio:.2f}；OBV_now={obv_now:.0f}、OBV_prev={obv_prev:.0f}；MFI14={mfi:.2f}。\n【判讀步驟】(1) 先看量比（當量/均量）判斷市場參與度是否放大。 (2) 再看 OBV 是延續上升或轉折，確認資金是累積還是派發。 (3) 以 MFI 補強量價同向性，避免只看價格造成誤判。\n【關鍵比較】量比>=1.2 常視為量能有效放大；若價漲量縮或 OBV 不跟價，通常代表上攻力道不足；MFI 高檔鈍化需提防追高。\n【結論邏輯】量能是趨勢可信度的驗證層，沒有資金跟進的價格突破，延續性通常較差。", 
    }

    # Step 4 Risk
    risk_lv = "高" if dd <= -20 else ("中" if dd <= -10 else "低")
    step4 = {
        "title": "④ 風險分析（ATR + Volatility + Drawdown）",
        "smart": f"整體風險層級：{risk_lv}。",
        "insight": f"ATR={atr:.2f}、年化波動率={vol20*100:.2f}%、回撤={dd:.2f}%，建議倉位與停損需配合風險調整。",
        "alert": "當 ATR 與波動率同步上升時，請降低單筆風險曝險。",
        "detail": f"【資料引用】ATR14={atr:.2f}，RollingVol20={vol20*100:.2f}%（年化），Drawdown={dd:.2f}%。\n【判讀步驟】(1) 先看 ATR，評估單日波動區間與停損距離應否放寬。 (2) 再看年化波動率是否抬升，判斷是否進入高波動 regime。 (3) 最後看 Drawdown 深度，決定目前策略應採保守或積極風控。\n【關鍵比較】Drawdown<-10% 視為中度壓力區，<-20% 視為高壓力區；當 ATR 與波動率同步抬升時，建議下調倉位。\n【結論邏輯】風險層不是判斷方向，而是決定『可以承受多大錯誤』，直接影響倉位與停損策略。", 
    }

    short_buy = analysis["features"]["short_buy_zone"]
    short_sell = analysis["features"]["short_sell_zone"]
    long_buy = analysis["features"]["long_buy_zone"]
    long_sell = analysis["features"]["long_sell_zone"]

    op_msg = (
        f"短線參考：回測 {short_buy[0]}~{short_buy[1]} 可分批觀察，接近 {short_sell[0]}~{short_sell[1]} 留意減碼。"
        f" 中線參考：靠近 {long_buy[0]}~{long_buy[1]} 觀察布局，接近 {long_sell[0]}~{long_sell[1]} 以風險控制為先。"
    )
    op_reason = (
        f"此建議綜合趨勢({trend_state})、動能({momentum})、量能({vol_state})與風險({risk_lv})，"
        "將可操作區間轉成短中期參考價位，避免只看單一訊號。"
    )
    op_alert = "若價格跌破 MA20 且 MACD 轉負，同時量比低於 1，短線建議應降級為保守。"
    step5 = {
        "title": "⑤ 操作建議（短/中/長參考）",
        "smart": op_msg,
        "insight": op_reason,
        "alert": op_alert,
        "detail": (
            f"【短線買點區】{short_buy[0]}~{short_buy[1]}（以 MA20 附近作為回測區）\n"
            f"【短線賣點區】{short_sell[0]}~{short_sell[1]}（以近20日高點附近作為壓力區）\n"
            f"【中線買點區】{long_buy[0]}~{long_buy[1]}（以 MA60 附近作為中線成本區）\n"
            f"【中線賣點區】{long_sell[0]}~{long_sell[1]}（以近60日高點附近作為中線壓力）\n"
            "【判讀流程】先確認前四步是否同向支持，再把趨勢中樞（MA20/MA60）與近期壓力（20/60日高點）轉為可執行區間。"
        ),
    }

    return [step1, step2, step3, step4, step5]


def _right_side_time_padding(df: pd.DataFrame, min_days: int = 3) -> pd.Timedelta:
    """給時間軸右側預留空間，避免最後一根K棒貼邊被視覺截斷。"""
    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    if len(dates) < 2:
        return pd.Timedelta(days=min_days)
    step = dates.iloc[-1] - dates.iloc[-2]
    if step <= pd.Timedelta(0):
        step = pd.Timedelta(days=1)
    return max(step * 2, pd.Timedelta(days=min_days))


def _apply_common_time_axis(fig: go.Figure, df: pd.DataFrame, *, right_pad_days: int = 3):
    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    if dates.empty:
        return
    right_pad = _right_side_time_padding(df, min_days=right_pad_days)
    fig.update_xaxes(range=[dates.iloc[0], dates.iloc[-1] + right_pad], automargin=True)

def generate_llm_report(stock_id: str, stock_name: str, analysis: dict) -> str:
    _, report_markdown = generate_report_bundle(stock_id, stock_name, analysis)
    return report_markdown


def fig_candle_ma(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["max"],
            low=df["min"],
            close=df["close"],
            name="K線",
        )
    )
    for name, color in [("ma20", "#2E86DE"), ("ma60", "#27AE60"), ("ma120", "#8E44AD")]:
        fig.add_trace(go.Scatter(x=df["date"], y=df[name], mode="lines", name=name.upper(), line=dict(width=1.8, color=color)))
    fig.update_layout(
        title=f"{stock_id}｜K線 + MA20/60/120",
        xaxis_rangeslider_visible=False,
        margin=dict(l=8, r=24, t=45, b=8),
        height=380,
        legend=dict(orientation="h", y=1.02, x=0),
    )
    _apply_common_time_axis(fig, df, right_pad_days=5)
    return fig


def fig_volume(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["Trading_Volume"], name="成交量"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["vol_ma20"], mode="lines", name="均量20", line=dict(color="#E67E22", width=2)))
    fig.update_layout(
        title=f"{stock_id}｜成交量 + 均量20",
        margin=dict(l=8, r=8, t=45, b=8),
        height=280,
        legend=dict(orientation="h", y=1.02, x=0),
    )
    return fig


def fig_macd(df: pd.DataFrame, stock_id: str):
    colors = ["#2ECC71" if v >= 0 else "#E74C3C" for v in df["macd_hist"].fillna(0)]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["macd_hist"], name="HIST", marker_color=colors))
    fig.add_trace(go.Scatter(x=df["date"], y=df["macd_dif"], mode="lines", name="DIF", line=dict(color="#3498DB")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["macd_dea"], mode="lines", name="DEA", line=dict(color="#F39C12")))
    fig.update_layout(
        title=f"{stock_id}｜MACD",
        margin=dict(l=8, r=8, t=45, b=8),
        height=280,
        legend=dict(orientation="h", y=1.02, x=0),
    )
    return fig


def fig_drawdown(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["drawdown"] * 100, fill="tozeroy", name="Drawdown %", line=dict(color="#C0392B")))
    fig.update_layout(
        title=f"{stock_id}｜回撤圖 Drawdown",
        yaxis_title="%",
        margin=dict(l=8, r=8, t=45, b=8),
        height=240,
    )
    return fig


def fig_signal_timeline(df: pd.DataFrame, stock_id: str):
    map_y = {"SELL": -1, "HOLD": 0, "BUY": 1}
    y = df["signal"].map(map_y)
    color_map = {"BUY": "#27AE60", "HOLD": "#F1C40F", "SELL": "#E74C3C"}
    colors = [color_map[s] for s in df["signal"]]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=y, mode="markers+lines", marker=dict(color=colors, size=7), line=dict(color="#95A5A6"), name="訊號"))
    end_dt = df["date"].max()
    start_dt = end_dt - pd.Timedelta(days=183)
    fig.update_layout(
        title=f"{stock_id}｜訊號燈時間軸（近半年）",
        yaxis=dict(tickmode="array", tickvals=[-1, 0, 1], ticktext=["SELL", "HOLD", "BUY"]),
        xaxis=dict(range=[start_dt, end_dt]),
        margin=dict(l=8, r=8, t=45, b=8),
        height=220,
    )
    return fig


def fig_rsi(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["rsi14"], mode="lines", name="RSI14", line=dict(color="#16A085")))
    fig.add_hline(y=70, line_dash="dot", line_color="#E74C3C")
    fig.add_hline(y=30, line_dash="dot", line_color="#3498DB")
    fig.update_layout(title=f"{stock_id}｜RSI(14)", margin=dict(l=8, r=8, t=45, b=8), height=230)
    return fig


def fig_kd(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["k"], mode="lines", name="K", line=dict(color="#8E44AD")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["d"], mode="lines", name="D", line=dict(color="#E67E22")))
    fig.add_hline(y=80, line_dash="dot", line_color="#E74C3C")
    fig.add_hline(y=20, line_dash="dot", line_color="#3498DB")
    fig.update_layout(title=f"{stock_id}｜KD", margin=dict(l=8, r=8, t=45, b=8), height=230)
    return fig


def fig_bollinger(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode="lines", name="Close", line=dict(color="#1F2937")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bb_up"], mode="lines", name="BB Up", line=dict(color="#E11D48", width=1)))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bb_mid"], mode="lines", name="BB Mid", line=dict(color="#6366F1", width=1)))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bb_low"], mode="lines", name="BB Low", line=dict(color="#0891B2", width=1)))
    fig.update_layout(title=f"{stock_id}｜布林通道", margin=dict(l=8, r=8, t=45, b=8), height=260, legend=dict(orientation="h", y=1.02, x=0))
    return fig


def fig_atr(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["atr14"], mode="lines", name="ATR14", line=dict(color="#DC2626")))
    fig.update_layout(title=f"{stock_id}｜ATR(14)", margin=dict(l=8, r=8, t=45, b=8), height=220)
    return fig


def fig_adx_dmi(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["adx14"], mode="lines", name="ADX", line=dict(color="#111827")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["plus_di"], mode="lines", name="+DI", line=dict(color="#16A34A")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["minus_di"], mode="lines", name="-DI", line=dict(color="#DC2626")))
    fig.add_hline(y=25, line_dash="dot", line_color="#6B7280")
    fig.update_layout(title=f"{stock_id}｜ADX / DMI", margin=dict(l=8, r=8, t=45, b=8), height=230, legend=dict(orientation="h", y=1.02, x=0))
    return fig


def fig_obv(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["obv"], mode="lines", name="OBV", line=dict(color="#7C3AED")))
    fig.update_layout(title=f"{stock_id}｜OBV", margin=dict(l=8, r=8, t=45, b=8), height=220)
    return fig


def fig_mfi(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["mfi14"], mode="lines", name="MFI14", line=dict(color="#0EA5E9")))
    fig.add_hline(y=80, line_dash="dot", line_color="#E74C3C")
    fig.add_hline(y=20, line_dash="dot", line_color="#16A34A")
    fig.update_layout(title=f"{stock_id}｜MFI(14)", margin=dict(l=8, r=8, t=45, b=8), height=230)
    return fig


def fig_vwap(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode="lines", name="Close", line=dict(color="#1F2937")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["vwap"], mode="lines", name="VWAP", line=dict(color="#F59E0B")))
    fig.update_layout(title=f"{stock_id}｜VWAP（日線近似）", margin=dict(l=8, r=8, t=45, b=8), height=240, legend=dict(orientation="h", y=1.02, x=0))
    return fig


def fig_relative_return(df: pd.DataFrame, benchmark: pd.DataFrame, stock_id: str):
    s = df[["date", "ret"]].copy()
    s["nav"] = (1 + s["ret"]).cumprod()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=s["date"], y=s["nav"], mode="lines", name=f"{stock_id} 累積報酬", line=dict(color="#2563EB")))

    if benchmark is not None and not benchmark.empty:
        merged = s.merge(benchmark[["date", "bench_nav"]], on="date", how="left")
        fig.add_trace(go.Scatter(x=merged["date"], y=merged["bench_nav"], mode="lines", name=f"{BENCHMARK_ID} 基準", line=dict(color="#6B7280")))
    fig.update_layout(title=f"{stock_id}｜報酬率曲線（相對基準）", margin=dict(l=8, r=8, t=45, b=8), height=240, legend=dict(orientation="h", y=1.02, x=0))
    return fig


def fig_rolling_vol(df: pd.DataFrame, stock_id: str):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["rolling_vol_20"] * 100, mode="lines", name="Rolling Vol 20d", line=dict(color="#B91C1C")))
    fig.update_layout(title=f"{stock_id}｜滾動波動率（20日，年化%）", yaxis_title="%", margin=dict(l=8, r=8, t=45, b=8), height=230)
    return fig


def render_chart(fig, help_text: str):
    _, mid, _ = st.columns([0.03, 0.94, 0.03])
    with mid:
        with st.expander("❓ 圖表說明", expanded=False):
            st.caption(help_text)
        st.plotly_chart(
            fig,
            width="stretch",
            config={"scrollZoom": False, "displaylogo": False},
        )


def update_stock(stock_id: str, start_date: str, token: str | None):
    df = fetch_taiwan_stock_price(stock_id=stock_id, start_date=start_date, end_date=date.today().isoformat(), token=token)
    wrote = upsert_to_sqlite(df, DB_PATH)
    return len(df), wrote


init_db()

with st.expander("⚙️ 追蹤個股設定", expanded=False):
    wl = get_watchlist()
    st.write("目前追蹤：", "、".join(wl) if wl else "（空）")

    c1, c2 = st.columns([2, 1])
    with c1:
        new_id = st.text_input("新增股票代碼", placeholder="例如 2330")
    with c2:
        if st.button("新增", use_container_width=True):
            sid = (new_id or "").strip()
            if sid.isdigit() and len(sid) == 4:
                add_watchlist(sid)
                st.success(f"已新增 {sid}")
                st.rerun()
            else:
                st.error("請輸入 4 位數股票代碼")

    if wl:
        remove_id = st.selectbox("移除股票代碼", wl)
        if st.button("移除", use_container_width=True):
            remove_watchlist(remove_id)
            st.warning(f"已移除 {remove_id}")
            st.rerun()

    st.markdown("---")
    st.markdown("### 📬 研究報告通知")
    sub_email = st.text_input("通知 Email", value="morris@utrust.com.tw", key="sub_email")
    sub_stocks = st.multiselect("通知個股（限追蹤清單）", options=wl, default=wl[:1] if wl else [])
    csub1, csub2 = st.columns(2)
    with csub1:
        sub_type = st.selectbox("通知時段", ["盤前", "盤後"], index=0)
    with csub2:
        sub_time = st.text_input("通知時間(HH:MM)", value="07:30" if sub_type == "盤前" else "14:03")

    if st.button("新增訂閱", use_container_width=True):
        if not sub_email.strip() or not sub_stocks:
            st.error("請填 Email 並至少選一檔股票")
        else:
            sched = "pre_open" if sub_type == "盤前" else "post_close"
            save_subscription(sub_email.strip(), sub_stocks, sched, sub_time.strip())
            st.success("已新增訂閱")
            st.rerun()

    sdf = list_subscriptions()
    if not sdf.empty:
        st.caption("目前訂閱")
        for _, r in sdf.iterrows():
            sid = int(r["id"])
            stocks = ",".join(json.loads(r["stock_ids_json"]))
            enabled = int(r["enabled"]) == 1
            with st.container(border=True):
                st.write(f"#{sid}｜{r['emails']}｜{r['schedule_type']} {r['schedule_time']}｜股票: {stocks}｜狀態: {'啟用' if enabled else '停用'}")
                cbtn1, cbtn2, cbtn3 = st.columns(3)
                if cbtn1.button("即時寄送", key=f"run_{sid}"):
                    ok, msg = queue_subscription_run(sid)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
                if cbtn2.button("啟用/停用", key=f"toggle_{sid}"):
                    toggle_subscription(sid, 0 if enabled else 1)
                    st.rerun()
                if cbtn3.button("刪除", key=f"del_{sid}"):
                    delete_subscription(sid)
                    st.rerun()

st.divider()

watchlist = get_watchlist()
try:
    token = st.secrets.get("FINMIND_TOKEN", None)
except Exception:
    token = None

c1, c2 = st.columns([3, 2])
with c1:
    st.caption(f"追蹤標的：{', '.join(watchlist) if watchlist else '無'}")
with c2:
    if st.button("🔄 更新全部資料", use_container_width=True):
        if not watchlist:
            st.error("請先新增至少一檔股票")
        else:
            prog = st.progress(0, text="更新中...")
            logs = []
            ids_to_update = sorted(set(watchlist + [BENCHMARK_ID]))
            for idx, sid in enumerate(ids_to_update, start=1):
                try:
                    cnt, wrote = update_stock(sid, "2024-01-01", token)
                    logs.append(f"{sid}: 抓取 {cnt} 筆 / 寫入 {wrote} 筆")
                except Exception as e:
                    logs.append(f"{sid}: 更新失敗 ({e})")
                prog.progress(int(idx / len(ids_to_update) * 100), text=f"更新中 {idx}/{len(ids_to_update)}")
            set_meta("last_update", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            st.success("更新完成")
            st.code("\n".join(logs))
            st.rerun()

last_update = get_meta("last_update")
if last_update:
    st.caption(f"最後更新時間：{last_update}")

if not watchlist:
    st.warning("請先在上方設定追蹤股票")
    st.stop()

stock_id = st.selectbox(
    "選擇股票",
    watchlist,
    format_func=lambda x: f"{x}｜{resolve_stock_name(x)}",
)
df = load_price(stock_id)
benchmark_df = load_benchmark_df(BENCHMARK_ID)
if df.empty:
    st.info("尚無資料，請先按「更新全部資料」。")
    st.stop()

latest = df.iloc[-1]
stock_name = resolve_stock_name(stock_id)
latest_data_date = df["date"].iloc[-1].strftime("%Y-%m-%d")
st.markdown(
    f"**目前標的：{stock_id}** <span class='stock-name-pill'>{stock_name}</span>"
    f"<span style='margin-left:8px;color:#64748b;font-size:0.9rem;'>資料最新日：{latest_data_date}</span>",
    unsafe_allow_html=True,
)

signal_map = {
    "BUY": ("🟢 BUY", "偏多訊號"),
    "HOLD": ("🟡 HOLD", "觀察中"),
    "SELL": ("🔴 SELL", "偏空訊號"),
}
sig_label, sig_desc = signal_map.get(latest["signal"], ("🟡 HOLD", "觀察中"))

m1, m2 = st.columns(2)
with m1:
    st.markdown("<div class='key-metric-label'>最新收盤價</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='key-metric-value'>{latest['close']:.2f}</div>", unsafe_allow_html=True)
with m2:
    st.markdown("<div class='key-metric-label'>看門狗燈號</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='key-metric-value'>{sig_label}</div>", unsafe_allow_html=True)
st.caption(f"訊號說明：{sig_desc}（積極策略，僅供參考）")

ma20_v = float(latest["ma20"]) if pd.notna(latest["ma20"]) else float("nan")
ma60_v = float(latest["ma60"]) if pd.notna(latest["ma60"]) else float("nan")
macd_hist_v = float(latest["macd_hist"]) if pd.notna(latest["macd_hist"]) else 0.0
vol_v = int(latest["Trading_Volume"]) if pd.notna(latest["Trading_Volume"]) else 0
volma_v = int(latest["vol_ma20"]) if pd.notna(latest["vol_ma20"]) else 0

with st.expander("＋ 看門狗燈號推理說明", expanded=False):
    st.caption(
        f"目前燈號為 {sig_label}。\n"
        f"判讀依據：close={latest['close']:.2f}、MA20={ma20_v:.2f}、MA60={ma60_v:.2f}、"
        f"MACD_hist={macd_hist_v:.4f}、Volume={vol_v}、VolMA20={volma_v}。\n"
        "流程：先判斷價格是否突破/跌破 MA20，再確認 MACD 動能是否同向，最後用量能是否高於均量做有效性過濾。"
    )

analysis = build_integrated_analysis(df)
st.subheader("🧠 智能判讀")

steps = build_four_step_insights(df, analysis)
for s in steps:
    st.markdown(f"<div class='step-title'>{s['title']}</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='step-lines'>"
        f"<p>{s['smart']}</p>"
        f"<p>{s['insight']}</p>"
        f"<p>{s['alert']}</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    with st.expander("＋", expanded=False):
        st.caption(s["detail"])

st.markdown("<div class='report-box'><div class='report-title'>📄 AI 投資報告建議（論述型）</div>", unsafe_allow_html=True)
latest_job = get_latest_report_job(stock_id)
latest_finished_job = get_latest_finished_report_job(stock_id)

if st.button("產生/更新 AI 報告", use_container_width=True):
    try:
        job_id, created = enqueue_report_job(stock_id, stock_name, analysis)
        if created:
            launch_report_worker(job_id)
        # 不呼叫 st.rerun()，讓使用者在背景執行期間可以關閉手機螢幕
        # 工作會在背景繼續執行，使用者回到本頁時會自動顯示最新狀態
    except Exception as exc:
        st.error(f"建立背景工作失敗：{exc}")

latest_job = get_latest_report_job(stock_id)
latest_finished_job = get_latest_finished_report_job(stock_id)

if latest_job and latest_job["status"] in {"queued", "running"}:
    st.info("報告產生中，請稍待。")
    status_label = {"queued": "排隊中", "running": "產生中"}.get(latest_job["status"], latest_job["status"])
    c_job_1, c_job_2 = st.columns([1, 1])
    if c_job_1.button("重新整理報告狀態", key=f"refresh_report_{stock_id}", use_container_width=True):
        st.rerun()
    c_job_2.caption(f"目前狀態：{status_label}")
elif latest_job and latest_job["status"] == "failed":
    st.error(f"報告產生失敗：{latest_job['error'] or '未知錯誤'}")

if latest_finished_job and latest_finished_job.get("report_markdown"):
    st.markdown(latest_finished_job["report_markdown"])
elif not latest_job:
    st.info("按下「產生/更新 AI 報告」取得論述型建議（由小芳 AI Hub 中樞生成）。")

st.markdown("</div>", unsafe_allow_html=True)

st.subheader("📊 量化指標圖表（15項）")
render_chart(fig_candle_ma(df, stock_id), "理論意義：均線是價格趨勢的平滑化表示，可過濾雜訊並觀察多空結構。\n實務重點：先看 MA20/60/120 是否多頭或空頭排列，再看股價是站上還是跌破 MA20；若趨勢與位置同向，判讀信度更高。")
render_chart(fig_volume(df, stock_id), "理論意義：量是趨勢強度的確認器，價格變動若缺乏成交量支持，延續性通常不足。\n實務重點：看當量是否高於均量、價漲量增是否成立；價漲量縮或價跌量增常代表結構轉弱。")
render_chart(fig_macd(df, stock_id), "理論意義：MACD 透過快慢 EMA 差值衡量動能與轉折。\n實務重點：看 DIF/DEA 交叉與 HIST 正負切換；若與趨勢方向一致，常作為加減碼節奏參考。")
render_chart(fig_rsi(df, stock_id), "理論意義：RSI 反映一段期間漲跌力道比值。\n實務重點：RSI>70 代表偏熱、<30 偏冷；在強趨勢中可長時間高檔鈍化，需搭配趨勢判讀避免反向操作。")
render_chart(fig_kd(df, stock_id), "理論意義：KD 對短期轉折敏感，常用來觀察短線節奏。\n實務重點：看 K/D 交叉位置與高低檔鈍化；高檔死叉不一定立刻反轉，需配合量價與趨勢。")
render_chart(fig_bollinger(df, stock_id), "理論意義：布林通道以標準差描述波動區間，反映價格偏離均值程度。\n實務重點：沿上軌推進常見於強勢趨勢，跌破下軌可能是恐慌或破位，需看是否快速回到中軌。")
render_chart(fig_atr(df, stock_id), "理論意義：ATR 衡量真實波動幅度，不判方向但可量化風險。\n實務重點：ATR 升高時應降低倉位或放寬停損距離，避免被正常波動洗出場。")
render_chart(fig_adx_dmi(df, stock_id), "理論意義：ADX 衡量趨勢強度，+DI/-DI 提供方向。\n實務重點：ADX>25 通常代表趨勢有可交易性；若 +DI 持續大於 -DI，偏多結構較穩。")
render_chart(fig_obv(df, stock_id), "理論意義：OBV 透過量價累積觀察資金流向。\n實務重點：若價格創高但 OBV 未創高，可能是量價背離，需提防後續動能衰退。")
render_chart(fig_mfi(df, stock_id), "理論意義：MFI 結合價格與成交量，屬資金強弱指標。\n實務重點：高檔鈍化或與價格背離常是警訊；低檔回升搭配放量常有反彈機會。")
render_chart(fig_vwap(df, stock_id), "理論意義：VWAP 代表成交量加權平均成本，可視為多空成本中樞。\n實務重點：股價在 VWAP 上方通常偏強，下方偏弱；實務常用於判斷當前價格相對成本是否有優勢。")
render_chart(fig_relative_return(df, benchmark_df, stock_id), "理論意義：相對報酬衡量標的是否持續跑贏基準（alpha）。\n實務重點：若長期落後基準，代表資金效率不佳；持續跑贏才有策略配置價值。")
render_chart(fig_rolling_vol(df, stock_id), "理論意義：滾動波動率用來辨識風險 regime 變化。\n實務重點：波動率急升通常伴隨事件風險，建議縮小倉位與拉大停損緩衝。")
render_chart(fig_drawdown(df, stock_id), "理論意義：回撤反映資金曲線從高點回落的壓力。\n實務重點：除看最大回撤，也要看修復時間；回撤深且修復慢，代表策略韌性不足。")
render_chart(fig_signal_timeline(df, stock_id), "理論意義：把多指標結果映射為可執行訊號序列。\n實務重點：觀察近半年訊號密度、一致性與翻多翻空頻率，評估策略穩定度。")

st.caption("⚠️ 免責聲明：本工具為研究與決策輔助，不構成投資建議。")
