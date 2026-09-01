"""Tiny inspection CLI for sicim databases.

Usage:
    python -m sicim --db sicim.db list [--status running]
    python -m sicim --db sicim.db show RUN_ID
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys

from .store import RunStatus, SQLiteStore


def _fmt_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


async def _list(store: SQLiteStore, status: str | None) -> None:
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


async def _show(store: SQLiteStore, run_id: str) -> None:
    record = await store.load_run(run_id)
    if record is None:
        print(f"run '{run_id}' not found", file=sys.stderr)
        raise SystemExit(1)
    print(f"run:      {record.run_id}")
    print(f"workflow: {record.workflow}")
    print(f"status:   {record.status.value}")
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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m sicim", description="Inspect a sicim SQLite database.")
    parser.add_argument("--db", required=True, help="path to the sicim SQLite database")
    sub = parser.add_subparsers(dest="command", required=True)
    p_list = sub.add_parser("list", help="list runs")
    p_list.add_argument("--status", choices=[s.value for s in RunStatus], default=None)
    p_show = sub.add_parser("show", help="show a run's record, journal and signals")
    p_show.add_argument("run_id")
    args = parser.parse_args(argv)

    store = SQLiteStore(args.db)
    try:
        if args.command == "list":
            asyncio.run(_list(store, args.status))
        else:
            asyncio.run(_show(store, args.run_id))
    finally:
        store.close()


if __name__ == "__main__":
    main()
