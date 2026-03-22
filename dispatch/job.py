import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional


@dataclass
class Job:
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: int = 0
    retries: int = 0
    max_retries: int = 3
    status: str = "pending"
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    scheduled_at: Optional[datetime] = None

    @property
    def func_name(self) -> str:
        return f"{self.func.__module__}.{self.func.__qualname__}"

    @property
    def duration_seconds(self) -> Optional[float]:
        """Return wall-clock duration of the job run, or None if not yet finished."""
        if self.started_at is not None and self.finished_at is not None:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def wait_seconds(self) -> Optional[float]:
        """Return time the job spent waiting in the queue before being started."""
        if self.started_at is not None:
            baseline = self.scheduled_at if self.scheduled_at else self.created_at
            return (self.started_at - baseline).total_seconds()
        return None

    def run(self) -> Any:
        self.status = "running"
        self.started_at = datetime.utcnow()
        try:
            self.result = self.func(*self.args, **self.kwargs)
            self.status = "done"
        except Exception as e:
            self.error = str(e)
            if self.retries < self.max_retries:
                self.retries += 1
                self.status = "pending"
            else:
                self.status = "failed"
        finally:
            self.finished_at = datetime.utcnow()
        return self.result

    def to_dict(self) -> dict:
        """Return a plain-dict summary of the job suitable for logging or display."""
        return {
            "id": self.id,
            "func_name": self.func_name,
            "status": self.status,
            "priority": self.priority,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
            "wait_seconds": self.wait_seconds,
            "error": self.error,
        }
