"""Storage backends.

A Store persists, per run: the run record (inputs, tags, status, outcome), the
journal events and the signal inbox — plus worker leases and the schedule
table. ``InMemoryStore`` is for tests and ephemeral use; ``SQLiteStore`` (WAL
mode) is the durable default.
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
from typing import Any, Awaitable, Callable, Mapping

from . import serde
from .journal import Event

_UNSET = object()


class RunStatus(str, enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPENSATION_FAILED = "compensation_failed"
    #: The run ended by chaining into a fresh run (continue-as-new);
    #: ``RunRecord.continued_to`` names the successor.
    CONTINUED = "continued"

    @property
    def terminal(self) -> bool:
        return self is not RunStatus.RUNNING


#: Tag keys under this prefix belong to the runtime (``sicim.schedule`` marks
#: runs started by a schedule); user-facing APIs reject them.
RESERVED_TAG_PREFIX = "sicim."


def validate_tags(
    tags: Mapping[str, Any] | None, *, allow_none: bool = False, allow_reserved: bool = True
) -> dict[str, Any]:
    """Normalize run/schedule tags: a flat ``str -> str`` mapping.

    Tags are search keys (``list_runs(tags={...})``) set when a run is created
    and editable later (``Runtime.tag``, ``ctx.tag``). With ``allow_none`` a
    value of None is accepted (in an update it means "remove this key"); with
    ``allow_reserved=False`` keys under :data:`RESERVED_TAG_PREFIX` raise
    ``ValueError``.
    """
    if tags is None:
        return {}
    if not isinstance(tags, Mapping):
        raise TypeError("tags must be a mapping of str -> str")
    normalized: dict[str, Any] = {}
    for key, value in tags.items():
        if not isinstance(key, str) or not key:
            raise TypeError("tag keys must be non-empty strings")
        if not allow_reserved and key.startswith(RESERVED_TAG_PREFIX):
            raise ValueError(f"tag key {key!r} is reserved (prefix {RESERVED_TAG_PREFIX!r})")
        if value is None and allow_none:
            normalized[key] = None
            continue
        if not isinstance(value, str):
            raise TypeError(f"tag {key!r} must map to a string, got {type(value).__name__}")
        normalized[key] = value
    return normalized


def merge_tags(current: Mapping[str, str], update: Mapping[str, str | None]) -> dict[str, str]:
    """Apply a tag update: set the given pairs, drop keys whose value is None."""
    merged = dict(current)
    for key, value in update.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value
    return merged


@dataclass
class RunRecord:
    run_id: str
    workflow: str
    version: int = 1
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: RunStatus = RunStatus.RUNNING
    result: Any = None
    error: dict[str, Any] | None = None
    cancel_requested: bool = False
    continued_to: str | None = None
    #: Run id of the parent workflow for child runs (``ctx.child``); carried
    #: through continue-as-new successors. None for top-level runs.
    parent_run_id: str | None = None
    #: Search tags pinned at start (``Runtime.start(tags=...)``). Child runs
    #: inherit them by default; continue-as-new successors carry them.
    tags: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass(frozen=True)
class SignalRecord:
    seq: int
    name: str
    payload: Any
    consumed: bool
    ts: float


@dataclass
class ScheduleRecord:
    """A standing instruction to start runs of a workflow on a schedule.

    ``spec`` is the serialized schedule (``"every 3600"``, ``"at <ts>"`` or
    ``"cron <expr>"``, see :mod:`sicim.schedule`); ``next_fire_at`` the next
    planned start (None once a one-shot schedule has fired); ``last_run_id``
    the most recently started run. ``overlap`` is ``"skip"`` (a tick is skipped
    while the previous run is still running) or ``"allow"``.
    """

    schedule_id: str
    workflow: str
    spec: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    tz: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    overlap: str = "skip"
    paused: bool = False
    next_fire_at: float | None = None
    last_run_id: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class Store(abc.ABC):
    """Async persistence interface."""

    @abc.abstractmethod
    async def create_run(self, record: RunRecord) -> None:
        """Insert a new run; raises if ``record.run_id`` already exists."""

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
        continued_to: str | None | Any = _UNSET,
    ) -> None: ...

    @abc.abstractmethod
    async def delete_run(self, run_id: str) -> None:
        """Remove a run and everything attached to it (events, signals, tags, lease)."""

    @abc.abstractmethod
    async def delete_run_history(self, run_id: str) -> None:
        """Delete a run's journal and signal inbox but keep its run record.

        Used by automatic chain pruning: an old continue-as-new link keeps its
        (tiny) record so signal/cancel/result routing by old run ids still
        works, while the storage-heavy history is dropped.
        """

    @abc.abstractmethod
    async def list_runs(
        self,
        status: RunStatus | None = None,
        *,
        workflow: str | None = None,
        tags: Mapping[str, str] | None = None,
        parent_run_id: str | None = None,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[RunRecord]:
        """Search runs. Every given filter must match; ``tags`` means *all* of
        the given pairs. Ordered by creation time, oldest first unless
        ``newest_first``; ``limit`` caps the result."""

    @abc.abstractmethod
    async def update_tags(self, run_id: str, tags: Mapping[str, str | None]) -> dict[str, str]:
        """Merge ``tags`` into a run's tags (a value of None removes the key),
        keeping the search index in step, and return the run's resulting
        tags — empty when the run does not exist."""

    async def count_runs(self) -> dict[str, int]:
        """Number of runs per status value (statuses without runs are absent)."""
        counts: dict[str, int] = {}
        for record in await self.list_runs():
            counts[record.status.value] = counts.get(record.status.value, 0) + 1
        return counts

    async def list_workflows(self) -> list[str]:
        """Sorted distinct workflow names that have runs."""
        return sorted({record.workflow for record in await self.list_runs()})

    @abc.abstractmethod
    async def append_event(self, run_id: str, event: Event) -> None: ...

    @abc.abstractmethod
    async def load_events(self, run_id: str) -> list[Event]: ...

    @abc.abstractmethod
    async def replace_events(self, run_id: str, events: list[Event]) -> None:
        """Atomically replace a run's journal with ``events`` (already
        renumbered from seq 0).

        Only :meth:`Runtime.reset` uses this — rewinding a run means dropping
        the events after a chosen operation. Normal execution only ever
        appends.
        """

    @abc.abstractmethod
    async def append_signal(self, run_id: str, name: str, payload: Any) -> int: ...

    @abc.abstractmethod
    async def load_signals(self, run_id: str) -> list[SignalRecord]: ...

    @abc.abstractmethod
    async def mark_signal_consumed(self, run_id: str, seq: int, consumed: bool = True) -> None:
        """Mark a buffered signal consumed — or, with ``consumed=False``,
        available again (a reset that dropped its consumption)."""

    # -- worker leases -------------------------------------------------------
    # A lease grants one worker the exclusive right to drive a run. Acquiring
    # succeeds when the lease is free, expired, or already held by ``owner``.

    @abc.abstractmethod
    async def try_acquire_lease(self, run_id: str, owner: str, ttl: float) -> bool: ...

    @abc.abstractmethod
    async def renew_lease(self, run_id: str, owner: str, ttl: float) -> bool: ...

    @abc.abstractmethod
    async def release_lease(self, run_id: str, owner: str) -> None: ...

    @abc.abstractmethod
    async def load_lease(self, run_id: str) -> tuple[str, float] | None:
        """Current ``(owner, expires_at)`` for the run, or None."""

    # -- schedules -----------------------------------------------------------

    @abc.abstractmethod
    async def create_schedule(self, record: ScheduleRecord) -> None:
        """Insert a schedule; raises if ``record.schedule_id`` already exists."""

    @abc.abstractmethod
    async def load_schedule(self, schedule_id: str) -> ScheduleRecord | None: ...

    @abc.abstractmethod
    async def list_schedules(self) -> list[ScheduleRecord]: ...

    @abc.abstractmethod
    async def list_due_schedules(self, now: float) -> list[ScheduleRecord]:
        """Unpaused schedules whose ``next_fire_at`` is at or before ``now``."""

    @abc.abstractmethod
    async def update_schedule(
        self,
        schedule_id: str,
        *,
        expected_next_fire_at: float | None | Any = _UNSET,
        paused: bool | Any = _UNSET,
        next_fire_at: float | None | Any = _UNSET,
        last_run_id: str | None | Any = _UNSET,
    ) -> bool:
        """Update a schedule; returns whether a row changed.

        With ``expected_next_fire_at`` the update is a compare-and-set on the
        current ``next_fire_at`` — the scheduler uses it so that, of several
        workers seeing the same due tick, exactly one advances the schedule.
        """

    @abc.abstractmethod
    async def delete_schedule(self, schedule_id: str) -> None: ...

    # -- push notifications --------------------------------------------------

    async def subscribe(
        self, on_notify: Callable[[str, str], None]
    ) -> Callable[[], Awaitable[None]] | None:
        """Open a push channel for cross-worker wakeups, if the backend has one.

        Backends with a broadcast mechanism (PostgreSQL LISTEN/NOTIFY) invoke
        ``on_notify(kind, run_id)`` — kind ``"signal"`` or ``"cancel"`` — for
        every relevant write by *any* worker sharing the database, and return
        an async unsubscribe callable. The default returns None: no push
        channel, callers rely on polling alone. Push is an optimization, never
        a correctness requirement — polling stays as the backstop either way.
        """
        return None

    def close(self) -> None:  # noqa: B027 - optional hook
        pass

    async def aclose(self) -> None:
        """Async close; defaults to the sync ``close()``."""
        self.close()


# -- SQL helpers shared by the SQLite and PostgreSQL backends -----------------


def _run_query(
    placeholder: str,
    status: RunStatus | None,
    workflow: str | None,
    tags: Mapping[str, str] | None,
    parent_run_id: str | None,
    limit: int | None,
    newest_first: bool,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if status is not None:
        clauses.append(f"status = {placeholder}")
        params.append(status.value)
    if workflow is not None:
        clauses.append(f"workflow = {placeholder}")
        params.append(workflow)
    if parent_run_id is not None:
        clauses.append(f"parent_run_id = {placeholder}")
        params.append(parent_run_id)
    for key, value in (tags or {}).items():
        clauses.append(
            "EXISTS (SELECT 1 FROM run_tags WHERE run_tags.run_id = runs.run_id"
            f" AND run_tags.tag_key = {placeholder} AND run_tags.tag_value = {placeholder})"
        )
        params.extend((key, value))
    sql = "SELECT * FROM runs"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    direction = "DESC" if newest_first else "ASC"
    sql += f" ORDER BY created_at {direction}, run_id {direction}"
    if limit is not None:
        sql += f" LIMIT {placeholder}"
        params.append(int(limit))
    return sql, params


_SCHEDULE_COLUMNS = (
    "schedule_id, workflow, spec, args, kwargs, tz, tags, overlap, paused,"
    " next_fire_at, last_run_id, created_at, updated_at"
)


def _schedule_params(record: ScheduleRecord) -> tuple[Any, ...]:
    return (
        record.schedule_id,
        record.workflow,
        record.spec,
        serde.encode(record.args),
        serde.encode(record.kwargs),
        record.tz,
        serde.encode(record.tags),
        record.overlap,
        int(record.paused),
        record.next_fire_at,
        record.last_run_id,
        record.created_at,
        record.updated_at,
    )


def _schedule_from_row(row: Any) -> ScheduleRecord:
    return ScheduleRecord(
        schedule_id=row["schedule_id"],
        workflow=row["workflow"],
        spec=row["spec"],
        args=serde.decode(row["args"]),
        kwargs=serde.decode(row["kwargs"]),
        tz=row["tz"],
        tags=serde.decode(row["tags"]) if row["tags"] else {},
        overlap=row["overlap"],
        paused=bool(row["paused"]),
        next_fire_at=row["next_fire_at"],
        last_run_id=row["last_run_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _schedule_update(
    placeholder: str,
    schedule_id: str,
    *,
    expected_next_fire_at: Any,
    paused: Any,
    next_fire_at: Any,
    last_run_id: Any,
) -> tuple[str, list[Any]]:
    sets, params = [f"updated_at = {placeholder}"], [time.time()]
    if paused is not _UNSET:
        sets.append(f"paused = {placeholder}")
        params.append(int(paused))
    if next_fire_at is not _UNSET:
        sets.append(f"next_fire_at = {placeholder}")
        params.append(next_fire_at)
    if last_run_id is not _UNSET:
        sets.append(f"last_run_id = {placeholder}")
        params.append(last_run_id)
    where = f"schedule_id = {placeholder}"
    params.append(schedule_id)
    if expected_next_fire_at is not _UNSET:
        if expected_next_fire_at is None:
            where += " AND next_fire_at IS NULL"
        else:
            where += f" AND next_fire_at = {placeholder}"
            params.append(expected_next_fire_at)
    return f"UPDATE schedules SET {', '.join(sets)} WHERE {where}", params


class InMemoryStore(Store):
    """Non-durable store for tests and throwaway runs."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        self._events: dict[str, list[Event]] = {}
        self._signals: dict[str, list[SignalRecord]] = {}
        self._leases: dict[str, tuple[str, float]] = {}
        self._schedules: dict[str, ScheduleRecord] = {}

    @staticmethod
    def _copy(record: RunRecord) -> RunRecord:
        return dataclasses.replace(record, tags=dict(record.tags))

    async def create_run(self, record: RunRecord) -> None:
        if record.run_id in self._runs:
            raise ValueError(f"run '{record.run_id}' already exists")
        self._runs[record.run_id] = self._copy(record)
        self._events.setdefault(record.run_id, [])
        self._signals.setdefault(record.run_id, [])

    async def load_run(self, run_id: str) -> RunRecord | None:
        record = self._runs.get(run_id)
        return self._copy(record) if record is not None else None

    async def update_run(
        self, run_id, *, status=_UNSET, result=_UNSET, error=_UNSET, cancel_requested=_UNSET, continued_to=_UNSET
    ):
        record = self._runs[run_id]
        if status is not _UNSET:
            record.status = status
        if result is not _UNSET:
            record.result = result
        if error is not _UNSET:
            record.error = error
        if cancel_requested is not _UNSET:
            record.cancel_requested = cancel_requested
        if continued_to is not _UNSET:
            record.continued_to = continued_to
        record.updated_at = time.time()

    async def delete_run(self, run_id: str) -> None:
        self._runs.pop(run_id, None)
        self._events.pop(run_id, None)
        self._signals.pop(run_id, None)
        self._leases.pop(run_id, None)

    async def delete_run_history(self, run_id: str) -> None:
        self._events.pop(run_id, None)
        self._signals.pop(run_id, None)

    async def update_tags(self, run_id: str, tags: Mapping[str, str | None]) -> dict[str, str]:
        record = self._runs.get(run_id)
        if record is None:
            return {}
        record.tags = merge_tags(record.tags, tags)
        return dict(record.tags)

    async def list_runs(
        self, status=None, *, workflow=None, tags=None, parent_run_id=None, limit=None, newest_first=False
    ) -> list[RunRecord]:
        records = sorted(self._runs.values(), key=lambda r: (r.created_at, r.run_id), reverse=newest_first)
        selected: list[RunRecord] = []
        for record in records:
            if status is not None and record.status != status:
                continue
            if workflow is not None and record.workflow != workflow:
                continue
            if parent_run_id is not None and record.parent_run_id != parent_run_id:
                continue
            if tags and any(record.tags.get(key) != value for key, value in tags.items()):
                continue
            selected.append(self._copy(record))
            if limit is not None and len(selected) >= limit:
                break
        return selected

    async def append_event(self, run_id: str, event: Event) -> None:
        self._events.setdefault(run_id, []).append(event)

    async def load_events(self, run_id: str) -> list[Event]:
        return list(self._events.get(run_id, []))

    async def replace_events(self, run_id: str, events: list[Event]) -> None:
        self._events[run_id] = list(events)

    async def append_signal(self, run_id: str, name: str, payload: Any) -> int:
        inbox = self._signals.setdefault(run_id, [])
        seq = len(inbox)
        inbox.append(SignalRecord(seq=seq, name=name, payload=payload, consumed=False, ts=time.time()))
        return seq

    async def load_signals(self, run_id: str) -> list[SignalRecord]:
        return list(self._signals.get(run_id, []))

    async def mark_signal_consumed(self, run_id: str, seq: int, consumed: bool = True) -> None:
        inbox = self._signals.get(run_id, [])
        for i, sig in enumerate(inbox):
            if sig.seq == seq:
                inbox[i] = dataclasses.replace(sig, consumed=consumed)
                return

    async def try_acquire_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        now = time.time()
        current = self._leases.get(run_id)
        if current is None or current[0] == owner or current[1] < now:
            self._leases[run_id] = (owner, now + ttl)
            return True
        return False

    async def renew_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        current = self._leases.get(run_id)
        if current is not None and current[0] == owner:
            self._leases[run_id] = (owner, time.time() + ttl)
            return True
        return False

    async def release_lease(self, run_id: str, owner: str) -> None:
        current = self._leases.get(run_id)
        if current is not None and current[0] == owner:
            del self._leases[run_id]

    async def load_lease(self, run_id: str) -> tuple[str, float] | None:
        return self._leases.get(run_id)

    # -- schedules -----------------------------------------------------------

    @staticmethod
    def _copy_schedule(record: ScheduleRecord) -> ScheduleRecord:
        return dataclasses.replace(record, tags=dict(record.tags))

    async def create_schedule(self, record: ScheduleRecord) -> None:
        if record.schedule_id in self._schedules:
            raise ValueError(f"schedule '{record.schedule_id}' already exists")
        self._schedules[record.schedule_id] = self._copy_schedule(record)

    async def load_schedule(self, schedule_id: str) -> ScheduleRecord | None:
        record = self._schedules.get(schedule_id)
        return self._copy_schedule(record) if record is not None else None

    async def list_schedules(self) -> list[ScheduleRecord]:
        records = sorted(self._schedules.values(), key=lambda s: (s.created_at, s.schedule_id))
        return [self._copy_schedule(r) for r in records]

    async def list_due_schedules(self, now: float) -> list[ScheduleRecord]:
        due = [
            s for s in self._schedules.values()
            if not s.paused and s.next_fire_at is not None and s.next_fire_at <= now
        ]
        due.sort(key=lambda s: (s.next_fire_at, s.schedule_id))
        return [self._copy_schedule(s) for s in due]

    async def update_schedule(
        self, schedule_id, *, expected_next_fire_at=_UNSET, paused=_UNSET, next_fire_at=_UNSET, last_run_id=_UNSET
    ) -> bool:
        record = self._schedules.get(schedule_id)
        if record is None:
            return False
        if expected_next_fire_at is not _UNSET and record.next_fire_at != expected_next_fire_at:
            return False
        if paused is not _UNSET:
            record.paused = paused
        if next_fire_at is not _UNSET:
            record.next_fire_at = next_fire_at
        if last_run_id is not _UNSET:
            record.last_run_id = last_run_id
        record.updated_at = time.time()
        return True

    async def delete_schedule(self, schedule_id: str) -> None:
        self._schedules.pop(schedule_id, None)


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
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS run_tags (
    run_id TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (run_id, tag_key)
);
CREATE TABLE IF NOT EXISTS leases (
    run_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at REAL NOT NULL
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
    next_fire_at REAL,
    last_run_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_workflow ON runs(workflow);
CREATE INDEX IF NOT EXISTS idx_run_tags_kv ON run_tags(tag_key, tag_value);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(next_fire_at);
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
            # Migrations for databases created by older sicim versions.
            columns = {row[1] for row in self._conn.execute("PRAGMA table_info(runs)")}
            if "version" not in columns:
                self._conn.execute("ALTER TABLE runs ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
            if "continued_to" not in columns:
                self._conn.execute("ALTER TABLE runs ADD COLUMN continued_to TEXT")
            if "parent_run_id" not in columns:
                self._conn.execute("ALTER TABLE runs ADD COLUMN parent_run_id TEXT")
            if "tags" not in columns:
                self._conn.execute("ALTER TABLE runs ADD COLUMN tags TEXT NOT NULL DEFAULT '{}'")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    async def _run(self, fn, *args):
        def call():
            with self._lock:
                try:
                    return fn(*args)
                except Exception:
                    # Leave no half-open transaction behind on the shared
                    # connection (e.g. after a primary-key conflict).
                    self._conn.rollback()
                    raise

        return await asyncio.to_thread(call)

    # -- runs ----------------------------------------------------------------

    async def create_run(self, record: RunRecord) -> None:
        def op():
            self._conn.execute(
                "INSERT INTO runs (run_id, workflow, version, args, kwargs, status, result, error,"
                " cancel_requested, continued_to, parent_run_id, tags, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                    record.parent_run_id,
                    serde.encode(record.tags),
                    record.created_at,
                    record.updated_at,
                ),
            )
            if record.tags:
                self._conn.executemany(
                    "INSERT INTO run_tags (run_id, tag_key, tag_value) VALUES (?,?,?)",
                    [(record.run_id, key, value) for key, value in record.tags.items()],
                )
            self._conn.commit()

        await self._run(op)

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> RunRecord:
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
        def op():
            row = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return self._row_to_record(row) if row else None

        return await self._run(op)

    async def update_run(
        self, run_id, *, status=_UNSET, result=_UNSET, error=_UNSET, cancel_requested=_UNSET, continued_to=_UNSET
    ):
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
        if continued_to is not _UNSET:
            sets.append("continued_to = ?")
            params.append(continued_to)
        params.append(run_id)

        def op():
            self._conn.execute(f"UPDATE runs SET {', '.join(sets)} WHERE run_id = ?", params)
            self._conn.commit()

        await self._run(op)

    async def delete_run(self, run_id: str) -> None:
        def op():
            for table in ("events", "signals", "run_tags", "leases", "runs"):
                self._conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
            self._conn.commit()

        await self._run(op)

    async def delete_run_history(self, run_id: str) -> None:
        def op():
            for table in ("events", "signals"):
                self._conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
            self._conn.commit()

        await self._run(op)

    async def update_tags(self, run_id: str, tags: Mapping[str, str | None]) -> dict[str, str]:
        update = dict(tags)

        def op():
            row = self._conn.execute("SELECT tags FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return {}
            merged = merge_tags(serde.decode(row["tags"]) if row["tags"] else {}, update)
            self._conn.execute("UPDATE runs SET tags = ? WHERE run_id = ?", (serde.encode(merged), run_id))
            self._conn.execute("DELETE FROM run_tags WHERE run_id = ?", (run_id,))
            if merged:
                self._conn.executemany(
                    "INSERT INTO run_tags (run_id, tag_key, tag_value) VALUES (?,?,?)",
                    [(run_id, key, value) for key, value in merged.items()],
                )
            self._conn.commit()
            return merged

        return await self._run(op)

    async def list_runs(
        self, status=None, *, workflow=None, tags=None, parent_run_id=None, limit=None, newest_first=False
    ) -> list[RunRecord]:
        sql, params = _run_query("?", status, workflow, tags, parent_run_id, limit, newest_first)

        def op():
            return [self._row_to_record(r) for r in self._conn.execute(sql, params).fetchall()]

        return await self._run(op)

    async def count_runs(self) -> dict[str, int]:
        def op():
            rows = self._conn.execute("SELECT status, COUNT(*) AS n FROM runs GROUP BY status").fetchall()
            return {row["status"]: row["n"] for row in rows}

        return await self._run(op)

    async def list_workflows(self) -> list[str]:
        def op():
            rows = self._conn.execute("SELECT DISTINCT workflow FROM runs ORDER BY workflow").fetchall()
            return [row["workflow"] for row in rows]

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

    async def replace_events(self, run_id: str, events: list[Event]) -> None:
        rows = [
            (run_id, e.seq, e.kind, e.op_id, serde.encode(e.payload), e.ts) for e in events
        ]

        def op():
            self._conn.execute("DELETE FROM events WHERE run_id = ?", (run_id,))
            self._conn.executemany(
                "INSERT INTO events (run_id, seq, kind, op_id, payload, ts) VALUES (?,?,?,?,?,?)", rows
            )
            self._conn.commit()

        await self._run(op)

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

    async def mark_signal_consumed(self, run_id: str, seq: int, consumed: bool = True) -> None:
        def op():
            self._conn.execute(
                "UPDATE signals SET consumed = ? WHERE run_id = ? AND seq = ?",
                (int(consumed), run_id, seq),
            )
            self._conn.commit()

        await self._run(op)

    # -- leases --------------------------------------------------------------

    async def try_acquire_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        def op():
            now = time.time()
            cursor = self._conn.execute(
                "INSERT INTO leases (run_id, owner, expires_at) VALUES (?,?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET owner = excluded.owner, expires_at = excluded.expires_at "
                "WHERE leases.owner = excluded.owner OR leases.expires_at < ?",
                (run_id, owner, now + ttl, now),
            )
            self._conn.commit()
            return cursor.rowcount > 0

        return await self._run(op)

    async def renew_lease(self, run_id: str, owner: str, ttl: float) -> bool:
        def op():
            cursor = self._conn.execute(
                "UPDATE leases SET expires_at = ? WHERE run_id = ? AND owner = ?",
                (time.time() + ttl, run_id, owner),
            )
            self._conn.commit()
            return cursor.rowcount > 0

        return await self._run(op)

    async def release_lease(self, run_id: str, owner: str) -> None:
        def op():
            self._conn.execute("DELETE FROM leases WHERE run_id = ? AND owner = ?", (run_id, owner))
            self._conn.commit()

        await self._run(op)

    async def load_lease(self, run_id: str) -> tuple[str, float] | None:
        def op():
            row = self._conn.execute(
                "SELECT owner, expires_at FROM leases WHERE run_id = ?", (run_id,)
            ).fetchone()
            return (row["owner"], row["expires_at"]) if row else None

        return await self._run(op)

    # -- schedules -----------------------------------------------------------

    async def create_schedule(self, record: ScheduleRecord) -> None:
        params = _schedule_params(record)

        def op():
            self._conn.execute(
                f"INSERT INTO schedules ({_SCHEDULE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", params
            )
            self._conn.commit()

        await self._run(op)

    async def load_schedule(self, schedule_id: str) -> ScheduleRecord | None:
        def op():
            row = self._conn.execute(
                "SELECT * FROM schedules WHERE schedule_id = ?", (schedule_id,)
            ).fetchone()
            return _schedule_from_row(row) if row else None

        return await self._run(op)

    async def list_schedules(self) -> list[ScheduleRecord]:
        def op():
            rows = self._conn.execute(
                "SELECT * FROM schedules ORDER BY created_at, schedule_id"
            ).fetchall()
            return [_schedule_from_row(r) for r in rows]

        return await self._run(op)

    async def list_due_schedules(self, now: float) -> list[ScheduleRecord]:
        def op():
            rows = self._conn.execute(
                "SELECT * FROM schedules WHERE paused = 0 AND next_fire_at IS NOT NULL"
                " AND next_fire_at <= ? ORDER BY next_fire_at, schedule_id",
                (now,),
            ).fetchall()
            return [_schedule_from_row(r) for r in rows]

        return await self._run(op)

    async def update_schedule(
        self, schedule_id, *, expected_next_fire_at=_UNSET, paused=_UNSET, next_fire_at=_UNSET, last_run_id=_UNSET
    ) -> bool:
        sql, params = _schedule_update(
            "?", schedule_id,
            expected_next_fire_at=expected_next_fire_at, paused=paused,
            next_fire_at=next_fire_at, last_run_id=last_run_id,
        )

        def op():
            cursor = self._conn.execute(sql, params)
            self._conn.commit()
            return cursor.rowcount > 0

        return await self._run(op)

    async def delete_schedule(self, schedule_id: str) -> None:
        def op():
            self._conn.execute("DELETE FROM schedules WHERE schedule_id = ?", (schedule_id,))
            self._conn.commit()

        await self._run(op)
