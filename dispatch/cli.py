import argparse
import json
import sys
from datetime import datetime
from .queue import Queue


DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
WHITE = "\033[37m"
BOLD = "\033[1m"
RESET = "\033[0m"

STATUS_COLORS = {
    "pending": YELLOW,
    "running": CYAN,
    "done": GREEN,
    "failed": RED,
}


def _supports_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _color(text: str, code: str) -> str:
    if _supports_color():
        return f"{code}{text}{RESET}"
    return text


def _fmt_status(status: str) -> str:
    color = STATUS_COLORS.get(status, WHITE)
    return _color(f"{status:8}", color)


def _fmt_dt(iso: str | None) -> str:
    if not iso:
        return _color("—", DIM)
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return iso


def _fmt_duration(started: str | None, finished: str | None) -> str:
    if not started or not finished:
        return _color("—", DIM)
    try:
        s = datetime.fromisoformat(started)
        f = datetime.fromisoformat(finished)
        secs = (f - s).total_seconds()
        if secs < 60:
            return f"{secs:.2f}s"
        mins, secs = divmod(secs, 60)
        return f"{int(mins)}m{secs:.0f}s"
    except ValueError:
        return _color("—", DIM)


def cmd_status(args):
    q = Queue(args.db)
    stats = q.stats()

    total_active = sum(stats.get(s, 0) for s in ["pending", "running", "done", "failed"])

    print(_color(f"\n  Queue: {args.db}", BOLD))
    print(_color("  " + "─" * 36, DIM))

    rows = [
        ("pending",     stats.get("pending", 0)    - stats.get("scheduled", 0)),
        ("scheduled",   stats.get("scheduled", 0)),
        ("running",     stats.get("running", 0)),
        ("done",        stats.get("done", 0)),
        ("failed",      stats.get("failed", 0)),
        ("dead letter", stats.get("dead_letter", 0)),
    ]

    for label, count in rows:
        bar_len = min(count, 30)
        bar = "█" * bar_len if count > 0 else ""
        color = STATUS_COLORS.get(label, DIM)
        count_str = _color(str(count).rjust(5), color if count > 0 else DIM)
        bar_str = _color(bar, color) if count > 0 else ""
        print(f"  {label:12} {count_str}  {bar_str}")

    print(_color("  " + "─" * 36, DIM))
    print(f"  {'total':12} {_color(str(total_active).rjust(5), BOLD)}")
    print()


def cmd_jobs(args):
    q = Queue(args.db)
    jobs = q.all_jobs(status=args.status, limit=args.limit)

    if args.json:
        print(json.dumps(jobs, indent=2, default=str))
        return

    if not jobs:
        print(_color("  No jobs found.", DIM))
        return

    col_id       = 8
    col_status   = 8
    col_name     = 42
    col_retries  = 7
    col_dur      = 8
    col_created  = 19

    header = (
        _color(f"{'ID':<{col_id}}", BOLD) + "  " +
        _color(f"{'STATUS':<{col_status}}", BOLD) + "  " +
        _color(f"{'FUNCTION':<{col_name}}", BOLD) + "  " +
        _color(f"{'RETRY':<{col_retries}}", BOLD) + "  " +
        _color(f"{'DUR':<{col_dur}}", BOLD) + "  " +
        _color(f"{'CREATED':<{col_created}}", BOLD)
    )
    separator = _color("─" * (col_id + col_status + col_name + col_retries + col_dur + col_created + 12), DIM)

    print()
    print("  " + header)
    print("  " + separator)

    for job in jobs:
        status = job.get("status", "")
        retries = job.get("retries", 0)
        max_retries = job.get("max_retries", 3)
        func_name = job.get("func_name", "") or ""
        if len(func_name) > col_name:
            func_name = "…" + func_name[-(col_name - 1):]

        retry_str = f"{retries}/{max_retries}"
        dur_str = _fmt_duration(job.get("started_at"), job.get("finished_at"))
        created_str = _fmt_dt(job.get("created_at"))
        id_str = _color((job.get("id") or "")[:col_id], DIM)

        line = (
            f"{id_str}  " +
            f"{_fmt_status(status)}  " +
            f"{func_name:<{col_name}}  " +
            f"{_color(retry_str, DIM):<{col_retries}}  " +
            f"{dur_str:<{col_dur}}  " +
            f"{_color(created_str, DIM)}"
        )
        print("  " + line)

        if job.get("error"):
            err_preview = job["error"].replace("\n", " ")[:80]
            print("  " + " " * (col_id + 2) + _color(f"↳ {err_preview}", RED))

    print()


def cmd_clear(args):
    import sqlite3
    conn = sqlite3.connect(args.db)
    if args.status == "all":
        cursor = conn.execute("DELETE FROM jobs")
    else:
        cursor = conn.execute("DELETE FROM jobs WHERE status=?", (args.status,))
    count = cursor.rowcount
    conn.commit()
    conn.close()
    label = args.status
    print(_color(f"Cleared {count} {label} job(s).", GREEN if count > 0 else DIM))


def cmd_dead_letter(args):
    q = Queue(args.db)

    if args.dl_cmd == "list":
        jobs = q.dead_letter_jobs(limit=args.limit)
        if args.json:
            print(json.dumps(jobs, indent=2, default=str))
            return
        if not jobs:
            print(_color("  No dead letter jobs.", DIM))
            return
        print()
        for job in jobs:
            moved = _fmt_dt(job.get("moved_at"))
            err = (job.get("error") or "").replace("\n", " ")[:70]
            id_str = _color((job.get("id") or "")[:8], DIM)
            func_name = job.get("func_name", "")
            print(f"  {id_str}  {func_name}")
            print(f"  {' ' * 8}  moved={_color(moved, DIM)}  retries={job.get('retries')}/{job.get('max_retries')}")
            if err:
                print(f"  {' ' * 8}  {_color('↳ ' + err, RED)}")
            print()

    elif args.dl_cmd == "replay":
        q2 = Queue(args.db)
        job = q2.replay_dead_letter(args.job_id)
        if job:
            print(_color(f"Re-enqueued job {job.id[:8]} ({job.func_name}) as new pending job.", GREEN))
        else:
            print(_color(f"Dead letter job '{args.job_id}' not found.", RED))

    elif args.dl_cmd == "purge":
        count = q.purge_dead_letters()
        print(_color(f"Purged {count} dead letter job(s).", GREEN if count > 0 else DIM))


def main():
    parser = argparse.ArgumentParser(prog="dispatch", description="Lightweight Python task queue")
    sub = parser.add_subparsers(dest="cmd")

    # status
    p = sub.add_parser("status", help="Show queue stats")
    p.add_argument("--db", default="dispatch.db")

    # jobs
    p = sub.add_parser("jobs", help="List jobs")
    p.add_argument("--db", default="dispatch.db")
    p.add_argument("--status", choices=["pending", "running", "done", "failed"])
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--json", action="store_true", help="Output raw JSON")

    # clear
    p = sub.add_parser("clear", help="Clear jobs by status")
    p.add_argument("--db", default="dispatch.db")
    p.add_argument("status", choices=["done", "failed", "all"])

    # dead-letter
    p = sub.add_parser("dead-letter", help="Manage the dead letter queue")
    p.add_argument("--db", default="dispatch.db")
    dl_sub = p.add_subparsers(dest="dl_cmd")

    dl_list = dl_sub.add_parser("list", help="List dead letter jobs")
    dl_list.add_argument("--limit", type=int, default=20)
    dl_list.add_argument("--json", action="store_true")

    dl_replay = dl_sub.add_parser("replay", help="Re-enqueue a dead letter job")
    dl_replay.add_argument("job_id", help="Full or partial job ID")

    dl_purge = dl_sub.add_parser("purge", help="Remove all dead letter jobs")

    args = parser.parse_args()

    if args.cmd == "status":
        cmd_status(args)
    elif args.cmd == "jobs":
        cmd_jobs(args)
    elif args.cmd == "clear":
        cmd_clear(args)
    elif args.cmd == "dead-letter":
        if not args.dl_cmd:
            parser.parse_args(["dead-letter", "--help"])
        else:
            cmd_dead_letter(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
