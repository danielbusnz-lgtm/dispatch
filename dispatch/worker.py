import time
import signal
import logging
import threading
from datetime import datetime
from typing import Optional
from .queue import Queue

logger = logging.getLogger("dispatch.worker")


class Worker:
    def __init__(
        self,
        queue: Queue,
        poll_interval: float = 1.0,
        concurrency: int = 1,
        job_timeout: Optional[float] = None,
        stale_job_timeout: int = 300,
        recovery_interval: int = 60,
    ):
        self.queue = queue
        self.poll_interval = poll_interval
        self.concurrency = concurrency
        self.job_timeout = job_timeout
        self.stale_job_timeout = stale_job_timeout
        self.recovery_interval = recovery_interval
        self._running = False
        self._jobs_processed = 0
        self._jobs_failed = 0
        self._last_recovery_check = 0.0
        self._semaphore = threading.Semaphore(concurrency)
        self._active_threads: list[threading.Thread] = []
        self._lock = threading.Lock()

    def start(self):
        self._running = True
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)
        logger.info(
            "Worker started. poll_interval=%.1fs, concurrency=%d, job_timeout=%s, stale_job_timeout=%ds",
            self.poll_interval,
            self.concurrency,
            f"{self.job_timeout}s" if self.job_timeout else "none",
            self.stale_job_timeout,
        )
        while self._running:
            self._maybe_recover_stale_jobs()
            self._reap_finished_threads()
            if self.concurrency > 1:
                self._tick_concurrent()
            else:
                self._tick()
            time.sleep(self.poll_interval)

        # Wait for all in-flight jobs to finish before exiting
        self._drain()

        logger.info(
            "Worker stopped. Processed %d jobs (%d failed).",
            self._jobs_processed,
            self._jobs_failed,
        )

    def _drain(self):
        """Wait for all active worker threads to complete."""
        with self._lock:
            threads = list(self._active_threads)
        for t in threads:
            t.join()

    def _reap_finished_threads(self):
        """Remove completed threads from the tracking list."""
        with self._lock:
            self._active_threads = [t for t in self._active_threads if t.is_alive()]

    def _tick_concurrent(self):
        """Dequeue and dispatch jobs up to the concurrency limit."""
        # Fill available slots
        while self._semaphore.acquire(blocking=False):
            job = self.queue.dequeue()
            if not job:
                self._semaphore.release()
                break

            logger.info(
                "Dispatching job %s (%s) [priority=%d, attempt=%d/%d]",
                job.id[:8], job.func_name, job.priority,
                job.retries + 1, job.max_retries + 1,
            )

            t = threading.Thread(target=self._run_job, args=(job,), daemon=True)
            with self._lock:
                self._active_threads.append(t)
            t.start()

    def _run_job(self, job):
        """Execute a single job; used as the thread target for concurrent mode."""
        try:
            if self.job_timeout is not None:
                self._run_with_timeout(job, self.job_timeout)
            else:
                job.run()

            self.queue.update(job)

            with self._lock:
                self._jobs_processed += 1
                if job.status == "failed":
                    self._jobs_failed += 1

            if job.status == "done":
                duration = (job.finished_at - job.started_at).total_seconds()
                logger.info("Job %s completed in %.2fs", job.id[:8], duration)
            elif job.status == "failed":
                logger.error("Job %s failed permanently: %s", job.id[:8], job.error)
            elif job.status == "pending":
                logger.warning(
                    "Job %s will retry (%d/%d): %s",
                    job.id[:8], job.retries, job.max_retries, job.error,
                )
        finally:
            self._semaphore.release()

    def _tick(self):
        job = self.queue.dequeue()
        if not job:
            return

        logger.info("Running job %s (%s) [priority=%d, attempt=%d/%d]",
                    job.id[:8], job.func_name, job.priority,
                    job.retries + 1, job.max_retries + 1)

        if self.job_timeout is not None:
            self._run_with_timeout(job, self.job_timeout)
        else:
            job.run()

        self.queue.update(job)
        self._jobs_processed += 1

        if job.status == "done":
            duration = (job.finished_at - job.started_at).total_seconds()
            logger.info("Job %s completed in %.2fs", job.id[:8], duration)
        elif job.status == "failed":
            self._jobs_failed += 1
            logger.error("Job %s failed permanently: %s", job.id[:8], job.error)
        elif job.status == "pending":
            logger.warning("Job %s will retry (%d/%d): %s",
                          job.id[:8], job.retries, job.max_retries, job.error)

    def _run_with_timeout(self, job, timeout: float):
        """Run a job with a timeout using a thread."""
        result_holder = {}

        def target():
            try:
                job.run()
            except Exception as e:
                result_holder["error"] = e

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            # Thread is still running — job timed out
            job.error = f"Job timed out after {timeout}s"
            job.finished_at = datetime.utcnow()
            if job.retries < job.max_retries:
                job.retries += 1
                job.status = "pending"
            else:
                job.status = "failed"
            logger.warning("Job %s timed out after %.1fs", job.id[:8], timeout)

    def _maybe_recover_stale_jobs(self):
        """Periodically check for and recover stale jobs stuck in 'running' state."""
        now = time.monotonic()
        if now - self._last_recovery_check < self.recovery_interval:
            return
        self._last_recovery_check = now
        recovered = self.queue.recover_stale_jobs(timeout_seconds=self.stale_job_timeout)
        if recovered > 0:
            logger.warning("Recovered %d stale job(s) back to pending.", recovered)

    def _stop(self, *_):
        logger.info("Shutdown signal received, finishing current work...")
        self._running = False
