# dispatch

A lightweight Python task queue backed by SQLite. No Redis, no RabbitMQ, no infrastructure.

```python
from dispatch import Queue, Worker

q = Queue()

def send_email(to, subject):
    print(f"Sending email to {to}: {subject}")

q.enqueue(send_email, "user@example.com", "Welcome!")

worker = Worker(q)
worker.start()
```

## Install

```bash
pip install dispatch-queue
```

## CLI

```bash
# Queue stats
dispatch status

# List recent jobs
dispatch jobs
dispatch jobs --status failed

# Clear completed jobs
dispatch clear done
```

## Features

- SQLite backend -- no external services needed
- Priority queues
- Automatic retries with configurable max
- CLI dashboard for monitoring
- Zero dependencies beyond the stdlib

## Roadmap

- Scheduled/cron jobs
- Job timeouts
- Web dashboard
- Multi-process workers
- Dead letter queue
