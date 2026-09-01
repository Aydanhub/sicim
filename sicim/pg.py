"""PostgreSQL store (optional backend).

Requires the ``postgres`` extra:  ``pip install 'sicim[postgres]'``

    store = await sicim.pg.PostgresStore.connect("postgresql://user@host/db")
    rt = sicim.Runtime(store, worker_id="api-1")

One connection per store instance (per worker process); statements run in
autocommit mode, mirroring the SQLite backend's commit-per-operation
semantics. Leases make it safe for many worker processes to share one
database — that is the point of this backend.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from . import serde
from .journal import Event
from .store import RunRecord, RunStatus, SignalRecord, Store, _UNSET

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    args TEXT NOT NULL,
    kwargs TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    error TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    continued_to TEXT,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    op_id INTEGER NOT NULL,
    payload TEXT NOT NULL,
    ts DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS signals (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    name TEXT NOT NULL,
    payload TEXT,
    consumed INTEGER NOT NULL DEFAULT 0,
    ts DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS leases (
    run_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
"""


class PostgresStore(Store):
    """Durable store backed by PostgreSQL (psycopg 3, async)."""

    def __init__(self, conn) -> None:
        self._conn = conn
        self._lock = asyncio.Lock()

    @classmethod
    async def connect(cls, dsn: str) -> "PostgresStore":
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "PostgresStore requires psycopg; install with: pip install 'sicim[postgres]'"
            ) from exc
        conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row)
        store = cls(conn)
        async with store._lock:
            await conn.execute(_SCHEMA)
        return store

    async def aclose(self) -> None:
        await self._conn.close()

    # -- runs ----------------------------------------------------------------

    async def create_run(self, record: RunRecord) -> None:
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO runs (run_id, workflow, version, args, kwargs, status, result, error,"
                " cancel_requested, continued_to, created_at, updated_at)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    record.run_id,
                    record.workflow,
                    record.version,
                    serde.encode(record.args),
                    serde.encode(record.kwargs),
                    record.status.value,
                    serde.encode(record.result),
                    serde.encode(record.error),
                    int(record.cancel_requested),
                    record.continued_to,
                    record.created_at,
                    record.updated_at,
                ),
            )

    @staticmethod
    def _row_to_record(row: dict[str, Any]) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            workflow=row["workflow"],
            version=row["version"],
            args=serde.decode(row["args"]),
            kwargs=serde.decode(row["kwargs"]),
            status=RunStatus(row["status"]),
            result=serde.decode(row["result"]) if row["result"] is not None else None,
            error=serde.decode(row["error"]) if row["error"] is not None else None,
            cancel_requested=bool(row["cancel_requested"]),
            continued_to=row["continued_to"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def load_run(self, run_id: str) -> RunRecord | None:
        async with self._lock:
            cursor = await self._conn.execute("SELECT * FROM runs WHERE run_id = %s", (run_id,))
            row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def update_run(
        self, run_id, *, status=_UNSET, result=_UNSET, error=_UNSET, cancel_requested=_UNSET, continued_to=_UNSET
    ):
        sets, params = ["updated_at = %s"], [time.time()]
        if status is not _UNSET:
            sets.append("status = %s")
            params.append(status.value)
        if result is not _UNSET:
            sets.append("result = %s")
            params.append(serde.encode(result))
        if error is not _UNSET:
            sets.append("error = %s")
            params.append(serde.encode(error))
        if cancel_requested is not _UNSET:
            sets.append("cancel_requested = %s")
            params.append(int(cancel_requested))
        if continued_to is not _UNSET:
            sets.append("continued_to = %s")
            params.append(continued_to)
        params.append(run_id)
        async with self._lock:
            await self._conn.execute(f"UPDATE runs SET {', '.join(sets)} WHERE run_id = %s", params)

    async def list_runs(self, status: RunStatus | None = None) -> list[RunRecord]:
        async with self._lock:
            if status is None:
                cursor = await self._conn.execute("SELECT * FROM runs ORDER BY created_at")
            else:
                cursor = await self._conn.execute(
                    "SELECT * FROM runs WHERE status = %s ORDER BY created_at", (status.value,)
                )
            rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def delete_run(self, run_id: str) -> None:
        async with self._lock:
            for table in ("events", "signals", "leases", "runs"):
                await self._conn.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))

    # -- events --------------------------------------------------------------

    async def append_event(self, run_id: str, event: Event) -> None:
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO events (run_id, seq, kind, op_id, payload, ts) VALUES (%s,%s,%s,%s,%s,%s)",
                (run_id, event.seq, event.kind, event.op_id, serde.encode(event.payload), event.ts),
            )

    async def load_events(self, run_id: str) -> list[Event]:
        async with self._lock:
            cursor = await self._conn.execute(
                "SELECT * FROM events WHERE run_id = %s ORDER BY seq", (run_id,)
            )
            rows = await cursor.fetchall()
        return [
            Event(seq=r["seq"], kind=r["kind"], op_id=r["op_id"], payload=serde.decode(r["payload"]), ts=r["ts"])
            for r in rows
        ]

    # -- signals -------------------------------------------------------------

    async def append_signal(self, run_id: str, name: str, payload: Any) -> int:
        import psycopg.errors

        # Sequence assignment can race with another worker process signalling
        # the same run; a primary-key conflict just means "pick the next seq".
        for _ in range(20):
            try:
                async with self._lock:
                    cursor = await self._conn.execute(
                        "INSERT INTO signals (run_id, seq, name, payload, consumed, ts)"
                        " SELECT %s, COALESCE(MAX(seq), -1) + 1, %s, %s, 0, %s"
                        " FROM signals WHERE run_id = %s RETURNING seq",
                        (run_id, name, serde.encode(payload), time.time(), run_id),
                    )
                    row = await cursor.fetchone()
                return row["seq"]
            except psycopg.errors.UniqueViolation:
                await asyncio.sleep(0.01)
        raise RuntimeError(f"could not assign a signal sequence for run '{run_id}'")

    async def load_signals(self, run_id: str) -> list[SignalRecord]:
        async with self._lock:
            cursor = await self._conn.execute(
                "SELECT * FROM signals WHERE run_id = %s ORDER BY seq", (run_id,)
            )
            rows = await cursor.fetchall()
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

    async def mark_signal_consumed(self, run_id: str, seq: int) -> None:
        async with self._lock:
            await self._conn.execute(
                "UPDATE signals SET consumed = 1 WHERE run_id = %s AND seq = %s", (run_id, seq)
            )

    # -- leases --------------------------------------------------------------

    async def try_acquire_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        now = time.time()
        async with self._lock:
            cursor = await self._conn.execute(
                "INSERT INTO leases (run_id, owner, expires_at) VALUES (%s,%s,%s)"
                " ON CONFLICT (run_id) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at"
                " WHERE leases.owner = EXCLUDED.owner OR leases.expires_at < %s",
                (run_id, owner, now + ttl, now),
            )
        return cursor.rowcount > 0

    async def renew_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        async with self._lock:
            cursor = await self._conn.execute(
                "UPDATE leases SET expires_at = %s WHERE run_id = %s AND owner = %s",
                (time.time() + ttl, run_id, owner),
            )
        return cursor.rowcount > 0

    async def release_lease(self, run_id: str, owner: str) -> None:
        async with self._lock:
            await self._conn.execute(
                "DELETE FROM leases WHERE run_id = %s AND owner = %s", (run_id, owner)
            )

    async def load_lease(self, run_id: str) -> tuple[str, float] | None:
        async with self._lock:
            cursor = await self._conn.execute(
                "SELECT owner, expires_at FROM leases WHERE run_id = %s", (run_id,)
            )
            row = await cursor.fetchone()
        return (row["owner"], row["expires_at"]) if row else None
