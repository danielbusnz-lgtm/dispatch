import argparse
import json
from .queue import Queue


def main():
    parser = argparse.ArgumentParser(prog="dispatch", description="Lightweight Python task queue")
    sub = parser.add_subparsers(dest="cmd")

    p = sub.add_parser("status", help="Show queue stats")
    p.add_argument("--db", default="dispatch.db")

    p = sub.add_parser("jobs", help="List jobs")
    p.add_argument("--db", default="dispatch.db")
    p.add_argument("--status", choices=["pending", "running", "done", "failed"])
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("clear", help="Clear jobs by status")
    p.add_argument("--db", default="dispatch.db")
    p.add_argument("status", choices=["done", "failed", "all"])

    args = parser.parse_args()

    if args.cmd == "status":
        q = Queue(args.db)
        stats = q.stats()
        total = sum(stats.values())
        print(f"Queue: {args.db}")
        print(f"Total jobs: {total}")
        for status in ["pending", "running", "done", "failed"]:
            print(f"  {status}: {stats.get(status, 0)}")

    elif args.cmd == "jobs":
        q = Queue(args.db)
        jobs = q.all_jobs(status=args.status, limit=args.limit)
        if not jobs:
            print("No jobs found.")
            return
        for job in jobs:
            err = f" | error: {job['error'][:60]}" if job.get("error") else ""
            print(f"[{job['status']:8}] {job['id'][:8]} {job['func_name']}{err}")

    elif args.cmd == "clear":
        import sqlite3
        conn = sqlite3.connect(args.db)
        if args.status == "all":
            conn.execute("DELETE FROM jobs")
        else:
            conn.execute("DELETE FROM jobs WHERE status=?", (args.status,))
        conn.commit()
        conn.close()
        print(f"Cleared {args.status} jobs.")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
