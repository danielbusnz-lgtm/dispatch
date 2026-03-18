import time
import signal
from typing import Optional
from .queue import Queue


class Worker:
    def __init__(self, queue: Queue, poll_interval: float = 1.0, concurrency: int = 1):
        self.queue = queue
        self.poll_interval = poll_interval
        self.concurrency = concurrency
        self._running = False

    def start(self):
        self._running = True
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)
        print(f"Worker started. Polling every {self.poll_interval}s...")
        while self._running:
            self._tick()
            time.sleep(self.poll_interval)

    def _tick(self):
        job = self.queue.dequeue()
        if not job:
            return
        print(f"Running job {job.id[:8]} ({job.func_name})")
        job.run()
        self.queue.update(job)
        if job.status == "done":
            print(f"  done in {(job.finished_at - job.started_at).total_seconds():.2f}s")
        elif job.status == "failed":
            print(f"  failed: {job.error}")
        elif job.status == "pending":
            print(f"  retrying ({job.retries}/{job.max_retries})")

    def _stop(self, *_):
        print("\nShutting down worker...")
        self._running = False
