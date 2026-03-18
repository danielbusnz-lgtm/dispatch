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

    @property
    def func_name(self) -> str:
        return f"{self.func.__module__}.{self.func.__qualname__}"

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
