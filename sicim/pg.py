"""PostgreSQL store (optional backend).

Requires the ``postgres`` extra:  ``pip install 'sicim[postgres]'``

    store = await sicim.pg.PostgresStore.connect("postgresql://user@host/db")
    rt = sicim.Runtime(store, worker_id="api-1")

One statement connection per store instance (per worker process); statements
run in autocommit mode, mirroring the SQLite backend's commit-per-operation
semantics. A dropped connection is re-established transparently on the next
operation (bounded retry with backoff); an outage that outlasts the retries
surfaces as an error, which the runtime treats crash-equivalently — the run
stays RUNNING and a later ``recover()`` continues it.

Leases make it safe for many worker processes to share one database, and the
``subscribe()`` push channel (LISTEN/NOTIFY) carries cross-worker signals and
cancellations without waiting out a poll interval. Multi-worker setups are the
point of this backend.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any, Awaitable, Callable

from . import serde
from .journal import Event
from .store import (
    _SCHEDULE_COLUMNS,
    _UNSET,
    RunRecord,
    RunStatus,
    ScheduleRecord,
    SignalRecord,
    Store,
    _run_query,
    _schedule_from_row,
    _schedule_params,
    _schedule_update,
    merge_tags,
)

logger = logging.getLogger("sicim")

#: LISTEN/NOTIFY channel shared by every sicim worker on one database.
NOTIFY_CHANNEL = "sicim_wake"

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
    parent_run_id TEXT,
    tags TEXT NOT NULL DEFAULT '{}',
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS run_tags (
    run_id TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (run_id, tag_key)
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
CREATE TABLE IF NOT EXISTS schedules (
    schedule_id TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    spec TEXT NOT NULL,
    args TEXT NOT NULL,
    kwargs TEXT NOT NULL,
    tz TEXT,
    tags TEXT NOT NULL DEFAULT '{}',
    overlap TEXT NOT NULL DEFAULT 'skip',
    paused INTEGER NOT NULL DEFAULT 0,
    next_fire_at DOUBLE PRECISION,
    last_run_id TEXT,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_workflow ON runs(workflow);
CREATE INDEX IF NOT EXISTS idx_run_tags_kv ON run_tags(tag_key, tag_value);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(next_fire_at);
"""

#: Statements bringing databases created by older sicim versions up to date.
_MIGRATIONS = (
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS parent_run_id TEXT",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS tags TEXT NOT NULL DEFAULT '{}'",
)


class PostgresStore(Store):
    """Durable store backed by PostgreSQL (psycopg 3, async)."""

    def __init__(self, conn, dsn: str | None = None) -> None:
        self._conn = conn
        # The DSN enables reconnects and LISTEN connections; a store built on
        # a raw connection (no DSN) works but cannot reconnect or subscribe.
        self._dsn = dsn
        self._lock = asyncio.Lock()

    @classmethod
    async def connect(cls, dsn: str) -> "PostgresStore":
        try:
            import psycopg  # noqa: F401
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "PostgresStore requires psycopg; install with: pip install 'sicim[postgres]'"
            ) from exc
        store = cls(await cls._open(dsn), dsn)
        async with store._lock:
            await store._conn.execute(_SCHEMA)
            for statement in _MIGRATIONS:
                await store._conn.execute(statement)
        return store

    @staticmethod
    async def _open(dsn: str):
        import psycopg
        from psycopg.rows import dict_row

        return await psycopg.AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row)

    async def aclose(self) -> None:
        await self._conn.close()

    # -- connection management -----------------------------------------------

    async def _with_conn(self, op: Callable[[Any], Awaitable[Any]]) -> Any:
        """Run ``op(conn)`` under the store lock; reconnect and retry once if
        the connection turns out to be dead (server restart, network blip)."""
        import psycopg

        async with self._lock:
            try:
                return await op(self._conn)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                if self._dsn is None:
                    raise
                logger.warning("PostgreSQL connection lost; reconnecting")
                await self._reconnect()
                return await op(self._conn)

    async def _reconnect(self) -> None:
        with contextlib.suppress(Exception):
            await self._conn.close()
        delay = 0.05
        for attempt in range(5):
            try:
                self._conn = await self._open(self._dsn)
                return
            except Exception:
                if attempt == 4:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 3, 1.0)

    # -- push notifications ----------------------------------------------------

    async def subscribe(
        self, on_notify: Callable[[str, str], None]
    ) -> Callable[[], Awaitable[None]] | None:
        """LISTEN on a dedicated connection and push store-level wakeups.

        Every ``append_signal`` and cancel-flag update on this database — from
        *any* worker process — NOTIFYs the shared channel; ``on_notify(kind,
        run_id)`` is invoked for each. The listener reconnects with backoff if
        its connection drops; polling remains the correctness backstop while
        it is deaf. Returns an async unsubscribe callable.
        """
        if self._dsn is None:
            return None

        ready = asyncio.Event()

        async def listen_loop() -> None:
            while True:
                try:
                    conn = await self._open(self._dsn)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("sicim LISTEN connect failed; retrying in 1s")
                    await asyncio.sleep(1.0)
                    continue
                try:
                    await conn.execute(f"LISTEN {NOTIFY_CHANNEL}")
                    ready.set()
                    async for note in conn.notifies():
                        kind, _, run_id = note.payload.partition(":")
                        if run_id:
                            try:
                                on_notify(kind, run_id)
                            except Exception:
                                logger.exception("subscribe callback raised; notification dropped")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("sicim LISTEN connection dropped; reconnecting")
                    await asyncio.sleep(0.5)
                finally:
                    with contextlib.suppress(Exception):
                        await conn.close()

        task = asyncio.create_task(listen_loop(), name="sicim-pg-listen")
        # Give the channel a moment to come up so a signal sent right after
        # subscribing is pushed; on timeout we return anyway — polling covers.
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(ready.wait(), 2.0)

        async def unsubscribe() -> None:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task

        return unsubscribe

    @staticmethod
    async def _notify(conn, kind: str, run_id: str) -> None:
        await conn.execute("SELECT pg_notify(%s, %s)", (NOTIFY_CHANNEL, f"{kind}:{run_id}"))

    # -- runs ----------------------------------------------------------------

    async def create_run(self, record: RunRecord) -> None:
        params = (
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
            record.parent_run_id,
            serde.encode(record.tags),
            record.created_at,
            record.updated_at,
        )
        tag_rows = [(record.run_id, key, value) for key, value in record.tags.items()]

        async def op(conn):
            # Record and tag index land together or not at all.
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO runs (run_id, workflow, version, args, kwargs, status, result, error,"
                    " cancel_requested, continued_to, parent_run_id, tags, created_at, updated_at)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    params,
                )
                if tag_rows:
                    async with conn.cursor() as cursor:
                        await cursor.executemany(
                            "INSERT INTO run_tags (run_id, tag_key, tag_value) VALUES (%s,%s,%s)", tag_rows
                        )

        await self._with_conn(op)

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
            parent_run_id=row["parent_run_id"],
            tags=serde.decode(row["tags"]) if row["tags"] else {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def load_run(self, run_id: str) -> RunRecord | None:
        async def op(conn):
            cursor = await conn.execute("SELECT * FROM runs WHERE run_id = %s", (run_id,))
            return await cursor.fetchone()

        row = await self._with_conn(op)
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

        async def op(conn):
            await conn.execute(f"UPDATE runs SET {', '.join(sets)} WHERE run_id = %s", params)
            if cancel_requested is not _UNSET and cancel_requested:
                # Wake the worker driving this run without waiting for its heartbeat.
                await self._notify(conn, "cancel", run_id)

        await self._with_conn(op)

    async def list_runs(
        self, status=None, *, workflow=None, tags=None, parent_run_id=None, limit=None, newest_first=False
    ) -> list[RunRecord]:
        sql, params = _run_query("%s", status, workflow, tags, parent_run_id, limit, newest_first)

        async def op(conn):
            cursor = await conn.execute(sql, params)
            return await cursor.fetchall()

        rows = await self._with_conn(op)
        return [self._row_to_record(row) for row in rows]

    async def delete_run(self, run_id: str) -> None:
        async def op(conn):
            for table in ("events", "signals", "run_tags", "leases", "runs"):
                await conn.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))

        await self._with_conn(op)

    async def delete_run_history(self, run_id: str) -> None:
        async def op(conn):
            for table in ("events", "signals"):
                await conn.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))

        await self._with_conn(op)

    async def update_tags(self, run_id: str, tags) -> dict[str, str]:
        update = dict(tags)
        rows_sql = "INSERT INTO run_tags (run_id, tag_key, tag_value) VALUES (%s,%s,%s)"

        async def op(conn):
            # Read-merge-write under a row lock so concurrent taggers (other
            # workers, the UI) never drop each other's keys.
            async with conn.transaction():
                cursor = await conn.execute(
                    "SELECT tags FROM runs WHERE run_id = %s FOR UPDATE", (run_id,)
                )
                row = await cursor.fetchone()
                if row is None:
                    return {}
                merged = merge_tags(serde.decode(row["tags"]) if row["tags"] else {}, update)
                await conn.execute(
                    "UPDATE runs SET tags = %s WHERE run_id = %s", (serde.encode(merged), run_id)
                )
                await conn.execute("DELETE FROM run_tags WHERE run_id = %s", (run_id,))
                if merged:
                    async with conn.cursor() as cur:
                        await cur.executemany(rows_sql, [(run_id, k, v) for k, v in merged.items()])
                return merged

        return await self._with_conn(op)

    async def count_runs(self) -> dict[str, int]:
        async def op(conn):
            cursor = await conn.execute("SELECT status, COUNT(*) AS n FROM runs GROUP BY status")
            return await cursor.fetchall()

        return {row["status"]: row["n"] for row in await self._with_conn(op)}

    async def list_workflows(self) -> list[str]:
        async def op(conn):
            cursor = await conn.execute("SELECT DISTINCT workflow FROM runs ORDER BY workflow")
            return await cursor.fetchall()

        return [row["workflow"] for row in await self._with_conn(op)]

    # -- events --------------------------------------------------------------

    async def append_event(self, run_id: str, event: Event) -> None:
        payload = serde.encode(event.payload)

        async def op(conn):
            await conn.execute(
                "INSERT INTO events (run_id, seq, kind, op_id, payload, ts) VALUES (%s,%s,%s,%s,%s,%s)",
                (run_id, event.seq, event.kind, event.op_id, payload, event.ts),
            )

        await self._with_conn(op)

    async def load_events(self, run_id: str) -> list[Event]:
        async def op(conn):
            cursor = await conn.execute(
                "SELECT * FROM events WHERE run_id = %s ORDER BY seq", (run_id,)
            )
            return await cursor.fetchall()

        rows = await self._with_conn(op)
        return [
            Event(seq=r["seq"], kind=r["kind"], op_id=r["op_id"], payload=serde.decode(r["payload"]), ts=r["ts"])
            for r in rows
        ]

    async def replace_events(self, run_id: str, events: list[Event]) -> None:
        rows = [
            (run_id, e.seq, e.kind, e.op_id, serde.encode(e.payload), e.ts) for e in events
        ]

        async def op(conn):
            # The old journal disappears and the rewound one lands together:
            # a crash mid-reset must never leave a half-truncated run.
            async with conn.transaction():
                await conn.execute("DELETE FROM events WHERE run_id = %s", (run_id,))
                if rows:
                    async with conn.cursor() as cursor:
                        await cursor.executemany(
                            "INSERT INTO events (run_id, seq, kind, op_id, payload, ts)"
                            " VALUES (%s,%s,%s,%s,%s,%s)",
                            rows,
                        )

        await self._with_conn(op)

    async def latest_event_seq(self, run_id: str) -> int:
        async def op(conn):
            cursor = await conn.execute(
                "SELECT COALESCE(MAX(seq), -1) AS seq FROM events WHERE run_id = %s", (run_id,)
            )
            return (await cursor.fetchone())["seq"]

        return await self._with_conn(op)

    # -- signals -------------------------------------------------------------

    async def append_signal(self, run_id: str, name: str, payload: Any) -> int:
        import psycopg.errors

        encoded = serde.encode(payload)

        async def op(conn):
            cursor = await conn.execute(
                "INSERT INTO signals (run_id, seq, name, payload, consumed, ts)"
                " SELECT %s, COALESCE(MAX(seq), -1) + 1, %s, %s, 0, %s"
                " FROM signals WHERE run_id = %s RETURNING seq",
                (run_id, name, encoded, time.time(), run_id),
            )
            row = await cursor.fetchone()
            # Wake any worker waiting on this run without a poll delay.
            await self._notify(conn, "signal", run_id)
            return row["seq"]

        # Sequence assignment can race with another worker process signalling
        # the same run; a primary-key conflict just means "pick the next seq".
        for _ in range(20):
            try:
                return await self._with_conn(op)
            except psycopg.errors.UniqueViolation:
                await asyncio.sleep(0.01)
        raise RuntimeError(f"could not assign a signal sequence for run '{run_id}'")

    async def load_signals(self, run_id: str) -> list[SignalRecord]:
        async def op(conn):
            cursor = await conn.execute(
                "SELECT * FROM signals WHERE run_id = %s ORDER BY seq", (run_id,)
            )
            return await cursor.fetchall()

        rows = await self._with_conn(op)
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

    async def mark_signal_consumed(self, run_id: str, seq: int, consumed: bool = True) -> None:
        async def op(conn):
            await conn.execute(
                "UPDATE signals SET consumed = %s WHERE run_id = %s AND seq = %s",
                (int(consumed), run_id, seq),
            )

        await self._with_conn(op)

    # -- leases --------------------------------------------------------------

    async def try_acquire_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        async def op(conn):
            now = time.time()
            cursor = await conn.execute(
                "INSERT INTO leases (run_id, owner, expires_at) VALUES (%s,%s,%s)"
                " ON CONFLICT (run_id) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at"
                " WHERE leases.owner = EXCLUDED.owner OR leases.expires_at < %s",
                (run_id, owner, now + ttl, now),
            )
            return cursor.rowcount > 0

        return await self._with_conn(op)

    async def renew_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        async def op(conn):
            cursor = await conn.execute(
                "UPDATE leases SET expires_at = %s WHERE run_id = %s AND owner = %s",
                (time.time() + ttl, run_id, owner),
            )
            return cursor.rowcount > 0

        return await self._with_conn(op)

    async def release_lease(self, run_id: str, owner: str) -> None:
        async def op(conn):
            await conn.execute(
                "DELETE FROM leases WHERE run_id = %s AND owner = %s", (run_id, owner)
            )

        await self._with_conn(op)

    async def load_lease(self, run_id: str) -> tuple[str, float] | None:
        async def op(conn):
            cursor = await conn.execute(
                "SELECT owner, expires_at FROM leases WHERE run_id = %s", (run_id,)
            )
            return await cursor.fetchone()

        row = await self._with_conn(op)
        return (row["owner"], row["expires_at"]) if row else None

    # -- schedules -----------------------------------------------------------

    async def create_schedule(self, record: ScheduleRecord) -> None:
        params = _schedule_params(record)

        async def op(conn):
            await conn.execute(
                f"INSERT INTO schedules ({_SCHEDULE_COLUMNS})"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                params,
            )

        await self._with_conn(op)

    async def load_schedule(self, schedule_id: str) -> ScheduleRecord | None:
        async def op(conn):
            cursor = await conn.execute(
                "SELECT * FROM schedules WHERE schedule_id = %s", (schedule_id,)
            )
            return await cursor.fetchone()

        row = await self._with_conn(op)
        return _schedule_from_row(row) if row else None

    async def list_schedules(self) -> list[ScheduleRecord]:
        async def op(conn):
            cursor = await conn.execute("SELECT * FROM schedules ORDER BY created_at, schedule_id")
            return await cursor.fetchall()

        return [_schedule_from_row(row) for row in await self._with_conn(op)]

    async def list_due_schedules(self, now: float) -> list[ScheduleRecord]:
        async def op(conn):
            cursor = await conn.execute(
                "SELECT * FROM schedules WHERE paused = 0 AND next_fire_at IS NOT NULL"
                " AND next_fire_at <= %s ORDER BY next_fire_at, schedule_id",
                (now,),
            )
            return await cursor.fetchall()

        return [_schedule_from_row(row) for row in await self._with_conn(op)]

    async def update_schedule(
        self, schedule_id, *, expected_next_fire_at=_UNSET, paused=_UNSET, next_fire_at=_UNSET, last_run_id=_UNSET
    ) -> bool:
        sql, params = _schedule_update(
            "%s", schedule_id,
            expected_next_fire_at=expected_next_fire_at, paused=paused,
            next_fire_at=next_fire_at, last_run_id=last_run_id,
        )

        async def op(conn):
            cursor = await conn.execute(sql, params)
            return cursor.rowcount > 0

        return await self._with_conn(op)

    async def delete_schedule(self, schedule_id: str) -> None:
        async def op(conn):
            await conn.execute("DELETE FROM schedules WHERE schedule_id = %s", (schedule_id,))

        await self._with_conn(op)
