"""
Headless control for the tracker — same engine, no browser.

    ../tracker-venv/bin/python cli.py status
    ../tracker-venv/bin/python cli.py check
    ../tracker-venv/bin/python cli.py scan --source hc --from 2024 --to 2024 --court 11_24
    ../tracker-venv/bin/python cli.py run                 # work until the plan is done
    ../tracker-venv/bin/python cli.py run --once           # one pass, then exit
    ../tracker-venv/bin/python cli.py retry
    ../tracker-venv/bin/python cli.py items --status failed

`run` obeys the same 1 GiB cap and the same pause flag as the dashboard, and
Ctrl-C is safe at any point: every finished download and push is already
committed, and anything mid-flight rewinds on the next start.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time

import state
from config import DISK_CAP_BYTES
from pipeline import client
from worker import cleanup_orphans, plan_from_request, worker


def human(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} TiB"


def cmd_status(_args) -> int:
    st = state.stats()
    w = worker.status()
    counts = st["counts"]
    print(f"phase       : {w['phase']}{' (paused)' if w['paused'] else ''}")
    print(f"auto-push   : {'on' if w['auto_push'] else 'off'}")
    print(f"disk        : {human(w['disk']['used_bytes'])} / {human(DISK_CAP_BYTES)}"
          f"  ({w['disk']['pct']}%)")
    print(f"plan        : {w['plan']['position']}/{w['plan']['total']} partitions")
    print(f"items       : {st['total_items']} total")
    for key in ("discovered", "downloaded", "uploading", "pushed", "failed", "skipped"):
        if counts.get(key):
            print(f"  {key:<11}: {counts[key]}")
    print(f"ingested    : {human(st['pushed_bytes'])}")
    for row in st["by_source"]:
        print(f"  {row['source']}: pushed={row['pushed']} queued={row['queued']} "
              f"failed={row['failed']} skipped={row['skipped']}")
    return 0


def cmd_check(_args) -> int:
    result = client.check()
    print(("OK   — " if result["ok"] else "FAIL — ") + result["reason"])
    return 0 if result["ok"] else 1


def cmd_scan(args) -> int:
    courts = [args.court] if args.court else None
    print(f"Listing S3 partitions for {args.source.upper()} "
          f"{args.year_from}-{args.year_to}…")
    targets = plan_from_request(
        args.source, args.year_from, args.year_to, courts, args.regional
    )
    queued = worker.set_plan(targets, replace=args.replace)
    print(f"{len(targets)} partition(s) found, {queued} not yet scanned.")
    if queued == 0 and targets:
        print("Everything in that range is already scanned — nothing to add.")
    return 0


def cmd_run(args) -> int:
    if args.no_auto_push:
        worker.set_auto_push(False)
    worker.resume()
    worker.start()

    stopping = {"flag": False}

    def handle_signal(_sig, _frame):
        if stopping["flag"]:
            print("\nForced exit.")
            sys.exit(130)
        stopping["flag"] = True
        print("\nStopping after in-flight work finishes… (Ctrl-C again to force)")
        worker.pause("interrupted")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    idle_ticks = 0
    while not stopping["flag"]:
        time.sleep(2)
        st = state.stats()
        w = worker.status()
        counts = st["counts"]
        print(
            f"\r{w['phase']:<12} disk {human(w['disk']['used_bytes']):>9}/"
            f"{human(DISK_CAP_BYTES)}  queued {counts.get('discovered', 0):<6} "
            f"staged {counts.get('downloaded', 0) + counts.get('uploading', 0):<5} "
            f"pushed {counts.get('pushed', 0):<6} failed {counts.get('failed', 0):<4}",
            end="",
            flush=True,
        )
        work_left = (
            counts.get("discovered", 0)
            or counts.get("downloaded", 0)
            or counts.get("uploading", 0)
            or w["plan"]["remaining"]
        )
        if not work_left:
            idle_ticks += 1
            # Three idle passes in a row means the plan really is exhausted.
            if idle_ticks >= 3:
                print("\nNothing left to do.")
                break
        else:
            idle_ticks = 0
        if args.once and w["phase"] in ("idle", "cap-reached"):
            print(f"\nStopping after one pass ({w['phase']}).")
            break

    worker.stop()
    return 0


def cmd_retry(_args) -> int:
    n = state.retry_failed()
    print(f"Requeued {n} failed item(s).")
    np = state.retry_failed_partitions()
    if np:
        print(f"Requeued {np} failed partition(s).")
    return 0


def cmd_cleanup(_args) -> int:
    print(f"Removed {cleanup_orphans()} orphaned staged file(s).")
    return 0


def cmd_items(args) -> int:
    rows = state.list_items(args.status, args.limit, 0)
    if not rows:
        print("No items.")
        return 0
    for row in rows:
        print(
            f"{row['id']:>7} {row['source']:<3} {row['status']:<11} "
            f"{human(row['bytes']):>9}  {str(row['citation'])[:48]:<48} "
            f"{str(row['error'] or '')[:60]}"
        )
    return 0


def cmd_pause(_args) -> int:
    worker.pause("cli")
    print("Paused. A running `run` process will stop taking new work.")
    return 0


def cmd_resume(_args) -> int:
    worker.resume()
    print("Resumed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="tracker", description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)

    subs.add_parser("status", help="show counts, disk usage and plan progress").set_defaults(fn=cmd_status)
    subs.add_parser("check", help="verify the backend is reachable and authorised").set_defaults(fn=cmd_check)

    scan = subs.add_parser("scan", help="queue S3 partitions for discovery")
    scan.add_argument("--source", choices=("hc", "sc"), default="hc")
    scan.add_argument("--from", dest="year_from", type=int, required=True)
    scan.add_argument("--to", dest="year_to", type=int, required=True)
    scan.add_argument("--court", help="S3-form court code, e.g. 11_24 (HC only)")
    scan.add_argument("--regional", action="store_true", help="include SC regional PDFs")
    scan.add_argument("--replace", action="store_true", help="replace the existing plan")
    scan.set_defaults(fn=cmd_scan)

    run = subs.add_parser("run", help="download and push until the plan is exhausted")
    run.add_argument("--once", action="store_true", help="stop at the first idle/cap point")
    run.add_argument("--no-auto-push", action="store_true",
                     help="fill to the cap and stop instead of uploading")
    run.set_defaults(fn=cmd_run)

    items = subs.add_parser("items", help="list tracked items")
    items.add_argument("--status")
    items.add_argument("--limit", type=int, default=40)
    items.set_defaults(fn=cmd_items)

    subs.add_parser("retry", help="requeue failed items").set_defaults(fn=cmd_retry)
    subs.add_parser("cleanup", help="delete staged files no item claims").set_defaults(fn=cmd_cleanup)
    subs.add_parser("pause", help="set the pause flag").set_defaults(fn=cmd_pause)
    subs.add_parser("resume", help="clear the pause flag").set_defaults(fn=cmd_resume)

    args = parser.parse_args()
    state.init()
    return int(args.fn(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
