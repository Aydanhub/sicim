"""Tiny inspection & maintenance CLI for sicim databases.

Usage:
    python -m sicim --db sicim.db list [--status running]
    python -m sicim --db sicim.db show RUN_ID
    python -m sicim --db sicim.db prune --older-than-days 30 [--dry-run]

Every command also works against PostgreSQL via ``--pg DSN`` instead of ``--db``.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
import time

from .store import RunStatus, SQLiteStore, Store

#: Statuses prune may delete. RUNNING is live and COMPENSATION_FAILED needs a
#: human decision, so both are always kept.
_PRUNABLE = (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.CONTINUED)


def _fmt_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


async def _list(store: Store, status: str | None) -> None:
    runs = await store.list_runs(RunStatus(status) if status else None)
    if not runs:
        print("(no runs)")
        return
    print(f"{'RUN ID':<36} {'WORKFLOW':<24} {'STATUS':<20} {'CREATED':<19} UPDATED")
    for run in runs:
        print(
            f"{run.run_id:<36} {run.workflow:<24} {run.status.value:<20} "
            f"{_fmt_ts(run.created_at):<19} {_fmt_ts(run.updated_at)}"
        )


async def _show(store: Store, run_id: str) -> None:
    record = await store.load_run(run_id)
    if record is None:
        print(f"run '{run_id}' not found", file=sys.stderr)
        raise SystemExit(1)
    print(f"run:      {record.run_id}")
    print(f"workflow: {record.workflow} (v{record.version})")
    print(f"status:   {record.status.value}")
    if record.continued_to:
        print(f"continued_to: {record.continued_to}")
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


async def _amain(args: argparse.Namespace) -> None:
    if args.pg:
        from .pg import PostgresStore

        store: Store = await PostgresStore.connect(args.pg)
    else:
        store = SQLiteStore(args.db)
    try:
        if args.command == "list":
            await _list(store, args.status)
        elif args.command == "show":
            await _show(store, args.run_id)
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
    p_list = sub.add_parser("list", help="list runs")
    p_list.add_argument("--status", choices=[s.value for s in RunStatus], default=None)
    p_show = sub.add_parser("show", help="show a run's record, journal and signals")
    p_show.add_argument("run_id")
    p_prune = sub.add_parser(
        "prune", help="delete terminal runs (and their journals) older than a cutoff"
    )
    p_prune.add_argument("--older-than-days", type=float, default=30.0)
    p_prune.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if bool(args.db) == bool(args.pg):
        parser.error("exactly one of --db or --pg is required")
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
