import sqlite3
import json
import pickle
import base64
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional
from .job import Job


class Queue:
    def __init__(self, db_path: str = "dispatch.db"):
        self.db_path = db_path
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    func_blob TEXT NOT NULL,
                    args_blob TEXT NOT NULL,
                    kwargs_blob TEXT NOT NULL,
                    func_name TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    retries INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    status TEXT DEFAULT 'pending',
                    result_blob TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    scheduled_at TEXT
                )
            """)
            # Migration: add scheduled_at column if it doesn't exist
            columns = [row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()]
            if "scheduled_at" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN scheduled_at TEXT")

    def _encode(self, obj) -> str:
        return base64.b64encode(pickle.dumps(obj)).decode()

    def _decode(self, blob: str):
        return pickle.loads(base64.b64decode(blob))

    def enqueue(
        self,
        func: Callable,
        *args,
        priority: int = 0,
        max_retries: int = 3,
        delay: Optional[float] = None,
        scheduled_at: Optional[datetime] = None,
        **kwargs,
    ) -> Job:
        job = Job(func=func, args=args, kwargs=kwargs, priority=priority, max_retries=max_retries)

        if scheduled_at is not None:
            job.scheduled_at = scheduled_at
        elif delay is not None:
            job.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
        else:
            job.scheduled_at = None

        with self._conn() as conn:
            conn.execute("""
                INSERT INTO jobs (id, func_blob, args_blob, kwargs_blob, func_name,
                    priority, retries, max_retries, status, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.id,
                self._encode(job.func),
                self._encode(job.args),
                self._encode(job.kwargs),
                job.func_name,
                job.priority,
                job.retries,
                job.max_retries,
                job.status,
                job.created_at.isoformat(),
                job.scheduled_at.isoformat() if job.scheduled_at else None,
            ))
        return job

    def dequeue(self) -> Optional[Job]:
        now = datetime.utcnow().isoformat()
        with self._conn() as conn:
            row = conn.execute("""
                SELECT * FROM jobs WHERE status = 'pending'
                    AND (scheduled_at IS NULL OR scheduled_at <= ?)
                ORDER BY priority DESC, created_at ASC LIMIT 1
            """, (now,)).fetchone()
            if not row:
                return None
            scheduled_at = None
            if row["scheduled_at"]:
                scheduled_at = datetime.fromisoformat(row["scheduled_at"])
            job = Job(
                func=self._decode(row["func_blob"]),
                args=self._decode(row["args_blob"]),
                kwargs=self._decode(row["kwargs_blob"]),
                id=row["id"],
                priority=row["priority"],
                retries=row["retries"],
                max_retries=row["max_retries"],
                status=row["status"],
                created_at=datetime.fromisoformat(row["created_at"]),
                scheduled_at=scheduled_at,
            )
            conn.execute("UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?",
                         (datetime.utcnow().isoformat(), job.id))
            return job

    def update(self, job: Job):
        with self._conn() as conn:
            conn.execute("""
                UPDATE jobs SET status=?, retries=?, result_blob=?, error=?,
                    started_at=?, finished_at=?, scheduled_at=?
                WHERE id=?
            """, (
                job.status,
                job.retries,
                self._encode(job.result) if job.result is not None else None,
                job.error,
                job.started_at.isoformat() if job.started_at else None,
                job.finished_at.isoformat() if job.finished_at else None,
                job.scheduled_at.isoformat() if job.scheduled_at else None,
                job.id,
            ))

    def recover_stale_jobs(self, timeout_seconds: int = 300) -> int:
        """Reset jobs stuck in 'running' state for longer than timeout_seconds back to 'pending'."""
        cutoff = (datetime.utcnow() - timedelta(seconds=timeout_seconds)).isoformat()
        with self._conn() as conn:
            cursor = conn.execute("""
                UPDATE jobs SET status = 'pending', started_at = NULL
                WHERE status = 'running' AND started_at < ?
            """, (cutoff,))
            return cursor.rowcount

    def stats(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as count FROM jobs GROUP BY status
            """).fetchall()
            stats = {row["status"]: row["count"] for row in rows}

            # Count scheduled jobs that aren't yet ready
            now = datetime.utcnow().isoformat()
            scheduled_row = conn.execute("""
                SELECT COUNT(*) as count FROM jobs
                WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at > ?
            """, (now,)).fetchone()
            stats["scheduled"] = scheduled_row["count"] if scheduled_row else 0

            return stats

    def all_jobs(self, status: Optional[str] = None, limit: int = 50) -> list:
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status=? ORDER BY created_at DESC LIMIT ?",
                    (status, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
