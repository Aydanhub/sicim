"""Tiny inspection & maintenance CLI for sicim databases.

Usage:
    python -m sicim --db sicim.db list [--status running] [--workflow NAME] [--tag k=v ...] [--limit N]
    python -m sicim --db sicim.db show RUN_ID
    python -m sicim --db sicim.db tag RUN_ID k=v [k2=v2 ...] [--remove k3 ...]
    python -m sicim --db sicim.db reset RUN_ID [--to-op N] [--force]
    python -m sicim --db sicim.db prune --older-than-days 30 [--dry-run]
    python -m sicim --db sicim.db schedule list
    python -m sicim --db sicim.db schedule pause|resume|delete SCHEDULE_ID
    python -m sicim --db sicim.db ui [--host 127.0.0.1] [--port 8787]

Every command also works against PostgreSQL via ``--pg DSN`` instead of ``--db``.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
import time

from .errors import SicimError
from .journal import Kind
from .runtime import Runtime
from .store import RunStatus, SQLiteStore, Store

#: Statuses prune may delete. RUNNING is live and COMPENSATION_FAILED needs a
#: human decision, so both are always kept.
_PRUNABLE = (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.CONTINUED)


def _fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "-"
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_tags(tags: dict[str, str]) -> str:
    return ",".join(f"{key}={value}" for key, value in sorted(tags.items()))


def _parse_tag(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"tag must look like key=value, got {text!r}")
    return key, value


async def _list(store: Store, args: argparse.Namespace) -> None:
    runs = await store.list_runs(
        RunStatus(args.status) if args.status else None,
        workflow=args.workflow,
        tags=dict(args.tag) if args.tag else None,
        limit=args.limit,
        newest_first=args.limit is not None,
    )
    if not runs:
        print("(no runs)")
        return
    print(f"{'RUN ID':<36} {'WORKFLOW':<24} {'STATUS':<20} {'CREATED':<19} {'UPDATED':<19} TAGS")
    for run in runs:
        print(
            f"{run.run_id:<36} {run.workflow:<24} {run.status.value:<20} "
            f"{_fmt_ts(run.created_at):<19} {_fmt_ts(run.updated_at):<19} {_fmt_tags(run.tags)}"
        )


async def _show(store: Store, run_id: str) -> None:
    record = await store.load_run(run_id)
    if record is None:
        print(f"run '{run_id}' not found", file=sys.stderr)
        raise SystemExit(1)
    print(f"run:      {record.run_id}")
    print(f"workflow: {record.workflow} (v{record.version})")
    print(f"status:   {record.status.value}")
    if record.parent_run_id:
        print(f"parent:   {record.parent_run_id}")
    if record.continued_to:
        print(f"continued_to: {record.continued_to}")
    if record.tags:
        print(f"tags:     {_fmt_tags(record.tags)}")
    print(f"args:     {record.args!r}")
    if record.kwargs:
        print(f"kwargs:   {record.kwargs!r}")
    if record.status is RunStatus.COMPLETED:
        print(f"result:   {record.result!r}")
    if record.error:
        print(f"error:    {record.error!r}")

    events = await store.load_events(run_id)
    signals = await store.load_signals(run_id)
    if events:
        start = events[0].ts
        print(f"\njournal ({len(events)} events):")
        for event in events:
            payload = repr(event.payload)
            if len(payload) > 100:
                payload = payload[:99] + "…"
            op = f"op={event.op_id}" if event.op_id >= 0 else "run  "
            print(f"  [{event.seq:>3}] +{event.ts - start:7.3f}s  {event.kind:<20} {op:<8} {payload}")
    if signals:
        print(f"\nsignals ({len(signals)}):")
        for signal in signals:
            state = "consumed" if signal.consumed else "pending"
            print(f"  [{signal.seq:>3}] {signal.name:<20} {state:<9} {signal.payload!r}")


async def _tag(store: Store, args: argparse.Namespace) -> None:
    update: dict[str, str | None] = dict(args.tags)
    for key in args.remove:
        update[key] = None
    if not update:
        print("nothing to do: give KEY=VALUE pairs and/or --remove KEY", file=sys.stderr)
        raise SystemExit(2)
    rt = Runtime(store, scheduler=False)
    try:
        tags = await rt.tag(args.run_id, update)
    except (SicimError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None
    print(f"tags:     {_fmt_tags(tags) or '(none)'}")


async def _reset(store: Store, args: argparse.Namespace) -> None:
    rt = Runtime(store, scheduler=False)
    try:
        # resume=False: this process has no workflow code, so the rewound run
        # is left RUNNING for the worker that recovers it.
        await rt.reset(args.run_id, to_op=args.to_op, resume=False, force=args.force)
    except (SicimError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None
    record = await store.load_run(args.run_id)
    events = await store.load_events(args.run_id)
    marker = next((e for e in reversed(events) if e.kind == Kind.RUN_RESET), None)
    to_op = marker.payload["to_op"] if marker else args.to_op
    print(
        f"run '{args.run_id}' rewound to op {to_op} "
        f"({marker.payload['dropped'] if marker else 0} event(s) dropped); "
        f"status is now {record.status.value}"
    )
    print("a worker with the workflow code picks it up on its next recover()")


async def _ui(store: Store, args: argparse.Namespace) -> None:
    from .ui import start_ui

    server = await start_ui(store, host=args.host, port=args.port)
    print(f"sicim UI at {server.url}  (Ctrl-C to stop)", flush=True)
    try:
        await asyncio.Event().wait()  # until Ctrl-C cancels us
    finally:
        await server.aclose()


async def _prune(store: Store, older_than_days: float, dry_run: bool) -> None:
    cutoff = time.time() - older_than_days * 86400
    victims = [
        run for run in await store.list_runs() if run.status in _PRUNABLE and run.updated_at < cutoff
    ]
    verb = "would delete" if dry_run else "deleting"
    for run in victims:
        print(f"  {verb} {run.run_id}  ({run.status.value}, updated {_fmt_ts(run.updated_at)})")
        if not dry_run:
            await store.delete_run(run.run_id)
    print(
        f"{len(victims)} run(s) {'matched' if dry_run else 'pruned'} "
        "(running and compensation_failed runs are always kept)"
    )


async def _schedule(store: Store, args: argparse.Namespace) -> None:
    rt = Runtime(store, scheduler=False)  # pure store operations; fires nothing
    if args.action == "list":
        schedules = await rt.list_schedules()
        if not schedules:
            print("(no schedules)")
            return
        print(f"{'SCHEDULE ID':<24} {'WORKFLOW':<24} {'SPEC':<28} {'STATE':<7} {'NEXT FIRE':<19} LAST RUN")
        for sched in schedules:
            state = "paused" if sched.paused else ("done" if sched.next_fire_at is None else "active")
            spec = sched.spec + (f" ({sched.tz})" if sched.tz else "")
            print(
                f"{sched.schedule_id:<24} {sched.workflow:<24} {spec:<28} {state:<7} "
                f"{_fmt_ts(sched.next_fire_at):<19} {sched.last_run_id or '-'}"
            )
        return
    if args.action == "pause":
        await rt.pause_schedule(args.schedule_id)
    elif args.action == "resume":
        await rt.resume_schedule(args.schedule_id)
    else:
        await rt.unschedule(args.schedule_id)
    print(f"schedule '{args.schedule_id}' {args.action}d")


async def _amain(args: argparse.Namespace) -> None:
    if args.pg:
        from .pg import PostgresStore

        store: Store = await PostgresStore.connect(args.pg)
    else:
        store = SQLiteStore(args.db)
    try:
        if args.command == "list":
            await _list(store, args)
        elif args.command == "show":
            await _show(store, args.run_id)
        elif args.command == "schedule":
            await _schedule(store, args)
        elif args.command == "tag":
            await _tag(store, args)
        elif args.command == "reset":
            await _reset(store, args)
        elif args.command == "ui":
            await _ui(store, args)
        else:
            await _prune(store, args.older_than_days, args.dry_run)
    finally:
        await store.aclose()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m sicim", description="Inspect and maintain a sicim database."
    )
    parser.add_argument("--db", help="path to a sicim SQLite database")
    parser.add_argument("--pg", metavar="DSN", help="PostgreSQL DSN (alternative to --db)")
    sub = parser.add_subparsers(dest="command", required=True)
    p_list = sub.add_parser("list", help="list runs (chronological; with --limit, the newest N)")
    p_list.add_argument("--status", choices=[s.value for s in RunStatus], default=None)
    p_list.add_argument("--workflow", default=None, help="only runs of this workflow")
    p_list.add_argument(
        "--tag", action="append", type=_parse_tag, metavar="KEY=VALUE",
        help="only runs carrying this tag (repeatable; all must match)",
    )
    p_list.add_argument("--limit", type=int, default=None, help="show only the newest N runs")
    p_show = sub.add_parser("show", help="show a run's record, journal and signals")
    p_show.add_argument("run_id")
    p_tag = sub.add_parser("tag", help="add, replace or remove tags on a run")
    p_tag.add_argument("run_id")
    p_tag.add_argument("tags", nargs="*", type=_parse_tag, metavar="KEY=VALUE", help="tags to set")
    p_tag.add_argument("--remove", action="append", default=[], metavar="KEY", help="tag key to remove (repeatable)")
    p_reset = sub.add_parser(
        "reset", help="rewind a run's journal to an operation and leave it for a worker to replay"
    )
    p_reset.add_argument("run_id")
    p_reset.add_argument(
        "--to-op", type=int, default=None, metavar="N",
        help="operation to rewind to (default: the first failed one, else 0 = full re-run)",
    )
    p_reset.add_argument(
        "--force", action="store_true",
        help="reset even though compensations already ran (their effects are not re-applied)",
    )
    p_ui = sub.add_parser("ui", help="serve the web monitoring UI until Ctrl-C")
    p_ui.add_argument("--host", default="127.0.0.1", help="bind address (default: localhost only)")
    p_ui.add_argument("--port", type=int, default=8787, help="port (0 picks a free one)")
    p_prune = sub.add_parser(
        "prune", help="delete terminal runs (and their journals) older than a cutoff"
    )
    p_prune.add_argument("--older-than-days", type=float, default=30.0)
    p_prune.add_argument("--dry-run", action="store_true")
    p_sched = sub.add_parser("schedule", help="list, pause, resume or delete schedules")
    sched_sub = p_sched.add_subparsers(dest="action", required=True)
    sched_sub.add_parser("list", help="list schedules")
    for action in ("pause", "resume", "delete"):
        p_action = sched_sub.add_parser(action, help=f"{action} a schedule")
        p_action.add_argument("schedule_id")
    args = parser.parse_args(argv)

    if bool(args.db) == bool(args.pg):
        parser.error("exactly one of --db or --pg is required")
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
