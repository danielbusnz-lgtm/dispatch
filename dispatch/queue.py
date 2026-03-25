import sqlite3
import json
import pickle
import hashlib
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
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
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
                    scheduled_at TEXT,
                    dedup_key TEXT
                )
            """)
            # Migration: add columns if they don't exist
            columns = [row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()]
            if "scheduled_at" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN scheduled_at TEXT")
            if "dedup_key" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN dedup_key TEXT")

            # Indexes for common query patterns
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status_scheduled
                ON jobs (status, scheduled_at, priority DESC, created_at ASC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_dedup_key
                ON jobs (dedup_key) WHERE dedup_key IS NOT NULL
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status_started
                ON jobs (status, started_at) WHERE status = 'running'
            """)

            # Create dead_letter_jobs table for permanently failed jobs that exceeded retries
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dead_letter_jobs (
                    id TEXT PRIMARY KEY,
                    original_job_id TEXT NOT NULL,
                    func_blob TEXT NOT NULL,
                    args_blob TEXT NOT NULL,
                    kwargs_blob TEXT NOT NULL,
                    func_name TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    retries INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    moved_at TEXT NOT NULL
                )
            """)

    def _encode(self, obj) -> str:
        return base64.b64encode(pickle.dumps(obj)).decode()

    def _decode(self, blob: str):
        return pickle.loads(base64.b64decode(blob))

    def _compute_dedup_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
        """Compute a deterministic dedup key from function name and arguments."""
        func_name = f"{func.__module__}.{func.__qualname__}"
        key_data = pickle.dumps((func_name, args, kwargs))
        return hashlib.sha256(key_data).hexdigest()

    def enqueue(
        self,
        func: Callable,
        *args,
        priority: int = 0,
        max_retries: int = 3,
        delay: Optional[float] = None,
        scheduled_at: Optional[datetime] = None,
        deduplicate: bool = False,
        **kwargs,
    ) -> Optional[Job]:
        job = Job(func=func, args=args, kwargs=kwargs, priority=priority, max_retries=max_retries)

        if scheduled_at is not None:
            job.scheduled_at = scheduled_at
        elif delay is not None:
            job.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
        else:
            job.scheduled_at = None

        dedup_key = None
        if deduplicate:
            dedup_key = self._compute_dedup_key(func, args, kwargs)

        # Use a single connection with a transaction for the dedup check + insert
        # to prevent race conditions between concurrent workers.
        with self._conn() as conn:
            if dedup_key is not None:
                existing = conn.execute("""
                    SELECT id FROM jobs
                    WHERE dedup_key = ? AND status IN ('pending', 'running')
                    LIMIT 1
                """, (dedup_key,)).fetchone()
                if existing:
                    return None

            conn.execute("""
                INSERT INTO jobs (id, func_blob, args_blob, kwargs_blob, func_name,
                    priority, retries, max_retries, status, created_at, scheduled_at, dedup_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                dedup_key,
            ))
        return job

    def dequeue(self) -> Optional[Job]:
        """Atomically claim a pending job using UPDATE ... RETURNING to avoid races."""
        now = datetime.utcnow().isoformat()
        started_at = datetime.utcnow().isoformat()

        with self._conn() as conn:
            # Find the best candidate first, then atomically claim it.
            # Using a subquery with LIMIT 1 + UPDATE ensures only one worker
            # picks up each job even under concurrent access.
            row = conn.execute("""
                UPDATE jobs
                SET status = 'running', started_at = ?
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE status = 'pending'
                      AND (scheduled_at IS NULL OR scheduled_at <= ?)
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                )
                RETURNING *
            """, (started_at, now)).fetchone()

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
                status="running",
                created_at=datetime.fromisoformat(row["created_at"]),
                started_at=datetime.fromisoformat(started_at),
                scheduled_at=scheduled_at,
            )
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

        # Move permanently failed jobs to the dead letter queue
        if job.status == "failed":
            self._move_to_dead_letter(job)

    def _move_to_dead_letter(self, job: Job):
        """Move a permanently failed job to the dead letter queue for later inspection or replay."""
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job.id,)).fetchone()
            if not row:
                return
            conn.execute("""
                INSERT OR IGNORE INTO dead_letter_jobs
                    (id, original_job_id, func_blob, args_blob, kwargs_blob, func_name,
                     priority, retries, max_retries, error, created_at, started_at, finished_at, moved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["id"],
                row["id"],
                row["func_blob"],
                row["args_blob"],
                row["kwargs_blob"],
                row["func_name"],
                row["priority"],
                row["retries"],
                row["max_retries"],
                row["error"],
                row["created_at"],
                row["started_at"],
                row["finished_at"],
                datetime.utcnow().isoformat(),
            ))

    def dead_letter_jobs(self, limit: int = 50) -> list:
        """Return jobs in the dead letter queue."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM dead_letter_jobs ORDER BY moved_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def replay_dead_letter(self, job_id: str) -> Optional[Job]:
        """Re-enqueue a dead letter job back into the main queue for another attempt."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM dead_letter_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if not row:
                return None

            func = self._decode(row["func_blob"])
            args = self._decode(row["args_blob"])
            kwargs = self._decode(row["kwargs_blob"])

            job = Job(
                func=func,
                args=args,
                kwargs=kwargs,
                priority=row["priority"],
                max_retries=row["max_retries"],
            )

            conn.execute("""
                INSERT INTO jobs (id, func_blob, args_blob, kwargs_blob, func_name,
                    priority, retries, max_retries, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.id,
                row["func_blob"],
                row["args_blob"],
                row["kwargs_blob"],
                job.func_name,
                job.priority,
                0,
                job.max_retries,
                "pending",
                job.created_at.isoformat(),
            ))

            conn.execute("DELETE FROM dead_letter_jobs WHERE id = ?", (job_id,))

            return job

    def purge_dead_letters(self) -> int:
        """Remove all dead letter jobs. Returns the number of jobs purged."""
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM dead_letter_jobs")
            return cursor.rowcount

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

            # Count dead letter jobs
            dlq_row = conn.execute(
                "SELECT COUNT(*) as count FROM dead_letter_jobs"
            ).fetchone()
            stats["dead_letter"] = dlq_row["count"] if dlq_row else 0

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
