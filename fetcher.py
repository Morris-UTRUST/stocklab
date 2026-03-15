import argparse
import os
import sqlite3
from datetime import date

import pandas as pd
import requests

API_URL = "https://api.finmindtrade.com/api/v4/data"


def fetch_taiwan_stock_price(stock_id: str, start_date: str, end_date: str | None = None, token: str | None = None) -> pd.DataFrame:
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": start_date,
    }
    if end_date:
        params["end_date"] = end_date
    if token:
        params["token"] = token

    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("msg") != "success":
        raise RuntimeError(f"FinMind API error: {payload}")

    df = pd.DataFrame(payload.get("data", []))
    if df.empty:
        return df

    # Keep only needed fields and normalize types
    cols = [
        "date",
        "stock_id",
        "Trading_Volume",
        "Trading_money",
        "open",
        "max",
        "min",
        "close",
        "spread",
        "Trading_turnover",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df


def upsert_to_sqlite(df: pd.DataFrame, db_path: str = "stocklab.db") -> int:
    if df.empty:
        return 0

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_price_daily (
                date TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                Trading_Volume REAL,
                Trading_money REAL,
                open REAL,
                max REAL,
                min REAL,
                close REAL,
                spread REAL,
                Trading_turnover REAL,
                PRIMARY KEY (date, stock_id)
            )
            """
        )

        rows = [
            (
                r.get("date"),
                r.get("stock_id"),
                r.get("Trading_Volume"),
                r.get("Trading_money"),
                r.get("open"),
                r.get("max"),
                r.get("min"),
                r.get("close"),
                r.get("spread"),
                r.get("Trading_turnover"),
            )
            for _, r in df.iterrows()
        ]

        conn.executemany(
            """
            INSERT INTO stock_price_daily (
                date, stock_id, Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, stock_id) DO UPDATE SET
                Trading_Volume=excluded.Trading_Volume,
                Trading_money=excluded.Trading_money,
                open=excluded.open,
                max=excluded.max,
                min=excluded.min,
                close=excluded.close,
                spread=excluded.spread,
                Trading_turnover=excluded.Trading_turnover
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch FinMind TaiwanStockPrice into SQLite")
    parser.add_argument("--stock-id", default="2330", help="Stock code, e.g., 2330")
    parser.add_argument("--start-date", default="2018-01-01", help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--db", default="stocklab.db", help="SQLite path")
    parser.add_argument("--token", default=os.getenv("FINMIND_TOKEN"), help="FinMind token (or FINMIND_TOKEN env)")
    args = parser.parse_args()

    if args.end_date is None:
        args.end_date = date.today().isoformat()

    df = fetch_taiwan_stock_price(args.stock_id, args.start_date, args.end_date, args.token)
    count = upsert_to_sqlite(df, args.db)
    print(f"Fetched {len(df)} rows, wrote {count} rows to {args.db} for {args.stock_id}")


if __name__ == "__main__":
    main()
