"""Storage backends.

A Store persists three things per run: the run record (inputs, status,
outcome), the journal events, and the signal inbox. ``InMemoryStore`` is for
tests and ephemeral use; ``SQLiteStore`` (WAL mode) is the durable default.

v0.1 assumes a single Runtime drives a given run at a time (no leasing).
"""

from __future__ import annotations

import abc
import asyncio
import dataclasses
import enum
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from . import serde
from .journal import Event

_UNSET = object()


class RunStatus(str, enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPENSATION_FAILED = "compensation_failed"

    @property
    def terminal(self) -> bool:
        return self is not RunStatus.RUNNING


@dataclass
class RunRecord:
    run_id: str
    workflow: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: RunStatus = RunStatus.RUNNING
    result: Any = None
    error: dict[str, Any] | None = None
    cancel_requested: bool = False
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass(frozen=True)
class SignalRecord:
    seq: int
    name: str
    payload: Any
    consumed: bool
    ts: float


class Store(abc.ABC):
    """Async persistence interface."""

    @abc.abstractmethod
    async def create_run(self, record: RunRecord) -> None: ...

    @abc.abstractmethod
    async def load_run(self, run_id: str) -> RunRecord | None: ...

    @abc.abstractmethod
    async def update_run(
        self,
        run_id: str,
        *,
        status: RunStatus | Any = _UNSET,
        result: Any = _UNSET,
        error: dict[str, Any] | None | Any = _UNSET,
        cancel_requested: bool | Any = _UNSET,
    ) -> None: ...

    @abc.abstractmethod
    async def list_runs(self, status: RunStatus | None = None) -> list[RunRecord]: ...

    @abc.abstractmethod
    async def append_event(self, run_id: str, event: Event) -> None: ...

    @abc.abstractmethod
    async def load_events(self, run_id: str) -> list[Event]: ...

    @abc.abstractmethod
    async def append_signal(self, run_id: str, name: str, payload: Any) -> int: ...

    @abc.abstractmethod
    async def load_signals(self, run_id: str) -> list[SignalRecord]: ...

    @abc.abstractmethod
    async def mark_signal_consumed(self, run_id: str, seq: int) -> None: ...

    def close(self) -> None:  # noqa: B027 - optional hook
        pass


class InMemoryStore(Store):
    """Non-durable store for tests and throwaway runs."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        self._events: dict[str, list[Event]] = {}
        self._signals: dict[str, list[SignalRecord]] = {}

    async def create_run(self, record: RunRecord) -> None:
        self._runs[record.run_id] = dataclasses.replace(record)
        self._events.setdefault(record.run_id, [])
        self._signals.setdefault(record.run_id, [])

    async def load_run(self, run_id: str) -> RunRecord | None:
        record = self._runs.get(run_id)
        return dataclasses.replace(record) if record is not None else None

    async def update_run(self, run_id, *, status=_UNSET, result=_UNSET, error=_UNSET, cancel_requested=_UNSET):
        record = self._runs[run_id]
        if status is not _UNSET:
            record.status = status
        if result is not _UNSET:
            record.result = result
        if error is not _UNSET:
            record.error = error
        if cancel_requested is not _UNSET:
            record.cancel_requested = cancel_requested
        record.updated_at = time.time()

    async def list_runs(self, status: RunStatus | None = None) -> list[RunRecord]:
        records = sorted(self._runs.values(), key=lambda r: r.created_at)
        if status is not None:
            records = [r for r in records if r.status == status]
        return [dataclasses.replace(r) for r in records]

    async def append_event(self, run_id: str, event: Event) -> None:
        self._events.setdefault(run_id, []).append(event)

    async def load_events(self, run_id: str) -> list[Event]:
        return list(self._events.get(run_id, []))

    async def append_signal(self, run_id: str, name: str, payload: Any) -> int:
        inbox = self._signals.setdefault(run_id, [])
        seq = len(inbox)
        inbox.append(SignalRecord(seq=seq, name=name, payload=payload, consumed=False, ts=time.time()))
        return seq

    async def load_signals(self, run_id: str) -> list[SignalRecord]:
        return list(self._signals.get(run_id, []))

    async def mark_signal_consumed(self, run_id: str, seq: int) -> None:
        inbox = self._signals.get(run_id, [])
        for i, sig in enumerate(inbox):
            if sig.seq == seq:
                inbox[i] = dataclasses.replace(sig, consumed=True)
                return


_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    args TEXT NOT NULL,
    kwargs TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    error TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    op_id INTEGER NOT NULL,
    payload TEXT NOT NULL,
    ts REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS signals (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    name TEXT NOT NULL,
    payload TEXT,
    consumed INTEGER NOT NULL DEFAULT 0,
    ts REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
"""


class SQLiteStore(Store):
    """Durable store backed by a single SQLite file (WAL mode).

    Blocking sqlite3 calls run in worker threads via ``asyncio.to_thread``;
    a process-level lock serializes access to the shared connection.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    async def _run(self, fn, *args):
        def call():
            with self._lock:
                return fn(*args)

        return await asyncio.to_thread(call)

    # -- runs ----------------------------------------------------------------

    async def create_run(self, record: RunRecord) -> None:
        def op():
            self._conn.execute(
                "INSERT INTO runs (run_id, workflow, args, kwargs, status, result, error,"
                " cancel_requested, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    record.run_id,
                    record.workflow,
                    serde.encode(record.args),
                    serde.encode(record.kwargs),
                    record.status.value,
                    serde.encode(record.result),
                    serde.encode(record.error),
                    int(record.cancel_requested),
                    record.created_at,
                    record.updated_at,
                ),
            )
            self._conn.commit()

        await self._run(op)

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            workflow=row["workflow"],
            args=serde.decode(row["args"]),
            kwargs=serde.decode(row["kwargs"]),
            status=RunStatus(row["status"]),
            result=serde.decode(row["result"]) if row["result"] is not None else None,
            error=serde.decode(row["error"]) if row["error"] is not None else None,
            cancel_requested=bool(row["cancel_requested"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def load_run(self, run_id: str) -> RunRecord | None:
        def op():
            row = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return self._row_to_record(row) if row else None

        return await self._run(op)

    async def update_run(self, run_id, *, status=_UNSET, result=_UNSET, error=_UNSET, cancel_requested=_UNSET):
        sets, params = ["updated_at = ?"], [time.time()]
        if status is not _UNSET:
            sets.append("status = ?")
            params.append(status.value)
        if result is not _UNSET:
            sets.append("result = ?")
            params.append(serde.encode(result))
        if error is not _UNSET:
            sets.append("error = ?")
            params.append(serde.encode(error))
        if cancel_requested is not _UNSET:
            sets.append("cancel_requested = ?")
            params.append(int(cancel_requested))
        params.append(run_id)

        def op():
            self._conn.execute(f"UPDATE runs SET {', '.join(sets)} WHERE run_id = ?", params)
            self._conn.commit()

        await self._run(op)

    async def list_runs(self, status: RunStatus | None = None) -> list[RunRecord]:
        def op():
            if status is None:
                rows = self._conn.execute("SELECT * FROM runs ORDER BY created_at").fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM runs WHERE status = ? ORDER BY created_at", (status.value,)
                ).fetchall()
            return [self._row_to_record(r) for r in rows]

        return await self._run(op)

    # -- events --------------------------------------------------------------

    async def append_event(self, run_id: str, event: Event) -> None:
        def op():
            self._conn.execute(
                "INSERT INTO events (run_id, seq, kind, op_id, payload, ts) VALUES (?,?,?,?,?,?)",
                (run_id, event.seq, event.kind, event.op_id, serde.encode(event.payload), event.ts),
            )
            self._conn.commit()

        await self._run(op)

    async def load_events(self, run_id: str) -> list[Event]:
        def op():
            rows = self._conn.execute(
                "SELECT * FROM events WHERE run_id = ? ORDER BY seq", (run_id,)
            ).fetchall()
            return [
                Event(seq=r["seq"], kind=r["kind"], op_id=r["op_id"], payload=serde.decode(r["payload"]), ts=r["ts"])
                for r in rows
            ]

        return await self._run(op)

    # -- signals -------------------------------------------------------------

    async def append_signal(self, run_id: str, name: str, payload: Any) -> int:
        def op():
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), -1) + 1 AS next FROM signals WHERE run_id = ?", (run_id,)
            ).fetchone()
            seq = row["next"]
            self._conn.execute(
                "INSERT INTO signals (run_id, seq, name, payload, consumed, ts) VALUES (?,?,?,?,0,?)",
                (run_id, seq, name, serde.encode(payload), time.time()),
            )
            self._conn.commit()
            return seq

        return await self._run(op)

    async def load_signals(self, run_id: str) -> list[SignalRecord]:
        def op():
            rows = self._conn.execute(
                "SELECT * FROM signals WHERE run_id = ? ORDER BY seq", (run_id,)
            ).fetchall()
            return [
                SignalRecord(
                    seq=r["seq"],
                    name=r["name"],
                    payload=serde.decode(r["payload"]) if r["payload"] is not None else None,
                    consumed=bool(r["consumed"]),
                    ts=r["ts"],
                )
                for r in rows
            ]

        return await self._run(op)

    async def mark_signal_consumed(self, run_id: str, seq: int) -> None:
        def op():
            self._conn.execute(
                "UPDATE signals SET consumed = 1 WHERE run_id = ? AND seq = ?", (run_id, seq)
            )
            self._conn.commit()

        await self._run(op)
