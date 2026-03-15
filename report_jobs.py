import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from typing import Any

from report_payloads import generate_report_bundle

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "stocklab.db")
WORKER_PATH = os.path.join(BASE_DIR, "report_job_worker.py")


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)


def ensure_report_jobs_table() -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                error TEXT,
                report_markdown TEXT,
                payload_json TEXT,
                analysis_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_report_jobs_stock_created ON report_jobs(stock_id, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_report_jobs_status_created ON report_jobs(status, created_at ASC)"
        )
        conn.commit()
    finally:
        conn.close()


def _job_row_to_dict(row: sqlite3.Row | tuple[Any, ...] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    columns = [
        "job_id",
        "stock_id",
        "stock_name",
        "status",
        "created_at",
        "started_at",
        "finished_at",
        "error",
        "report_markdown",
        "payload_json",
        "analysis_json",
    ]
    return dict(zip(columns, row))


def get_latest_report_job(stock_id: str) -> dict[str, Any] | None:
    ensure_report_jobs_table()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT job_id, stock_id, stock_name, status, created_at, started_at, finished_at,
                   error, report_markdown, payload_json, analysis_json
            FROM report_jobs
            WHERE stock_id=?
            ORDER BY created_at DESC, job_id DESC
            LIMIT 1
            """,
            (stock_id,),
        ).fetchone()
        return _job_row_to_dict(row)
    finally:
        conn.close()


def get_latest_finished_report_job(stock_id: str) -> dict[str, Any] | None:
    ensure_report_jobs_table()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT job_id, stock_id, stock_name, status, created_at, started_at, finished_at,
                   error, report_markdown, payload_json, analysis_json
            FROM report_jobs
            WHERE stock_id=? AND status='finished'
            ORDER BY finished_at DESC, job_id DESC
            LIMIT 1
            """,
            (stock_id,),
        ).fetchone()
        return _job_row_to_dict(row)
    finally:
        conn.close()


def enqueue_report_job(stock_id: str, stock_name: str, analysis: dict[str, Any]) -> tuple[int, bool]:
    ensure_report_jobs_table()
    now = datetime.now().isoformat(timespec="seconds")
    analysis_json = json.dumps(analysis, ensure_ascii=False)
    conn = get_conn()
    try:
        existing = conn.execute(
            """
            SELECT job_id
            FROM report_jobs
            WHERE stock_id=? AND status IN ('queued', 'running')
            ORDER BY created_at DESC, job_id DESC
            LIMIT 1
            """,
            (stock_id,),
        ).fetchone()
        if existing:
            return int(existing[0]), False

        cur = conn.execute(
            """
            INSERT INTO report_jobs(stock_id, stock_name, status, created_at, analysis_json)
            VALUES (?, ?, 'queued', ?, ?)
            """,
            (stock_id, stock_name, now, analysis_json),
        )
        conn.commit()
        return int(cur.lastrowid), True
    finally:
        conn.close()


def launch_report_worker(job_id: int) -> None:
    subprocess.Popen(
        [sys.executable, WORKER_PATH, "--job-id", str(job_id)],
        cwd=BASE_DIR,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )


def claim_report_job(job_id: int) -> dict[str, Any] | None:
    ensure_report_jobs_table()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT job_id, stock_id, stock_name, status, created_at, started_at, finished_at,
                   error, report_markdown, payload_json, analysis_json
            FROM report_jobs
            WHERE job_id=?
            """,
            (job_id,),
        ).fetchone()
        job = _job_row_to_dict(row)
        if not job or job["status"] != "queued":
            conn.commit()
            return None

        started_at = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            "UPDATE report_jobs SET status='running', started_at=?, error=NULL WHERE job_id=?",
            (started_at, job_id),
        )
        conn.commit()
        job["status"] = "running"
        job["started_at"] = started_at
        job["error"] = None
        return job
    finally:
        conn.close()


def finish_report_job(job_id: int, payload: dict[str, Any], report_markdown: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE report_jobs
            SET status='finished',
                finished_at=?,
                error=NULL,
                payload_json=?,
                report_markdown=?
            WHERE job_id=?
            """,
            (
                datetime.now().isoformat(timespec="seconds"),
                json.dumps(payload, ensure_ascii=False),
                report_markdown,
                job_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fail_report_job(job_id: int, error: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE report_jobs
            SET status='failed',
                finished_at=?,
                error=?
            WHERE job_id=?
            """,
            (datetime.now().isoformat(timespec="seconds"), error[:1000], job_id),
        )
        conn.commit()
    finally:
        conn.close()


def process_report_job(job_id: int) -> bool:
    job = claim_report_job(job_id)
    if not job:
        return False

    analysis = json.loads(job["analysis_json"])
    payload, report_markdown = generate_report_bundle(job["stock_id"], job["stock_name"], analysis)
    if report_markdown.startswith("### AI 投資報告產生失敗"):
        fail_report_job(job_id, report_markdown)
        return False
    finish_report_job(job_id, payload, report_markdown)
    return True
