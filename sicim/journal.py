"""The journal: an append-only event log, one per run.

The journal is both the checkpoint and the replay source. Code is the source
of *structure* (which operations run, in which order); the journal is the
source of *outcomes* (what each operation produced). On resume, the workflow
function re-executes from the top and every operation whose outcome is already
journaled returns instantly from the record instead of executing again.

Operations are keyed by ``op_id``, a per-run counter assigned synchronously in
code order (so it is deterministic even for parallel ``ctx.gather`` branches).
Each operation has exactly one *defining* event (STEP_SCHEDULED, TIMER_CREATED,
WAIT_CREATED, VALUE_RECORDED or COMP_REGISTERED); a mismatch between what the
code requests at an op_id and what the journal recorded there is how
non-determinism is detected.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable

if TYPE_CHECKING:
    from .store import Store

logger = logging.getLogger("sicim")


class Kind:
    """Event kind constants (stored as plain strings)."""

    # Run-level events (op_id == -1)
    RUN_STARTED = "run_started"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    RUN_CANCELLED = "run_cancelled"

    # Step lifecycle
    STEP_SCHEDULED = "step_scheduled"
    STEP_ATTEMPT_FAILED = "step_attempt_failed"
    STEP_COMPLETED = "step_completed"
    STEP_FAILED = "step_failed"

    # Saga compensations (keyed by the op_id of the step / registration)
    COMP_REGISTERED = "comp_registered"
    COMP_ATTEMPT_FAILED = "comp_attempt_failed"
    COMP_COMPLETED = "comp_completed"
    COMP_FAILED = "comp_failed"

    # Child workflows
    CHILD_SCHEDULED = "child_scheduled"
    CHILD_COMPLETED = "child_completed"
    CHILD_FAILED = "child_failed"

    # Durable timers
    TIMER_CREATED = "timer_created"
    TIMER_FIRED = "timer_fired"

    # External events / signals
    WAIT_CREATED = "wait_created"
    EVENT_CONSUMED = "event_consumed"
    WAIT_TIMED_OUT = "wait_timed_out"

    # Deterministic values (now / random / uuid)
    VALUE_RECORDED = "value_recorded"


#: Kinds that define an operation slot; used for non-determinism detection.
DEFINING_KINDS = frozenset(
    {
        Kind.STEP_SCHEDULED,
        Kind.TIMER_CREATED,
        Kind.WAIT_CREATED,
        Kind.VALUE_RECORDED,
        Kind.COMP_REGISTERED,
        Kind.CHILD_SCHEDULED,
    }
)


@dataclass(frozen=True)
class Event:
    seq: int
    kind: str
    op_id: int
    payload: dict[str, Any]
    ts: float


class Journal:
    """In-memory view of a run's event log, backed by a Store.

    Appends are serialized with a lock so concurrent branches of a workflow
    (``ctx.gather``) get consistent sequence numbers and durable ordering.
    """

    def __init__(
        self,
        run_id: str,
        store: "Store",
        events: Iterable[Event],
        on_append: Callable[[str, Event], None] | None = None,
    ):
        self.run_id = run_id
        self._store = store
        self._on_append = on_append
        self._events: list[Event] = list(events)
        self._first: dict[tuple[str, int], Event] = {}
        self._counts: dict[tuple[str, int], int] = {}
        self._defining: dict[int, Event] = {}
        self.consumed_signal_seqs: set[int] = set()
        self.max_op_id: int = -1
        for event in self._events:
            self._absorb(event)
        self._lock = asyncio.Lock()

    def _absorb(self, event: Event) -> None:
        key = (event.kind, event.op_id)
        self._first.setdefault(key, event)
        self._counts[key] = self._counts.get(key, 0) + 1
        if event.kind in DEFINING_KINDS:
            self._defining.setdefault(event.op_id, event)
        if event.op_id > self.max_op_id:
            self.max_op_id = event.op_id
        if event.kind == Kind.EVENT_CONSUMED:
            seq = event.payload.get("signal_seq")
            if seq is not None:
                self.consumed_signal_seqs.add(int(seq))

    # -- reads ---------------------------------------------------------------

    @property
    def events(self) -> list[Event]:
        return list(self._events)

    def find(self, kind: str, op_id: int) -> Event | None:
        """First event of ``kind`` at ``op_id``, if recorded."""
        return self._first.get((kind, op_id))

    def count(self, kind: str, op_id: int) -> int:
        return self._counts.get((kind, op_id), 0)

    def defining(self, op_id: int) -> Event | None:
        return self._defining.get(op_id)

    # -- writes --------------------------------------------------------------

    async def append(self, kind: str, op_id: int, payload: dict[str, Any]) -> Event:
        async with self._lock:
            event = Event(seq=len(self._events), kind=kind, op_id=op_id, payload=payload, ts=time.time())
            await self._store.append_event(self.run_id, event)
            self._events.append(event)
            self._absorb(event)
        if self._on_append is not None:
            try:
                self._on_append(self.run_id, event)
            except Exception:  # noqa: BLE001 - observers must never break a run
                logger.exception("on_event observer raised for run '%s'", self.run_id)
        return event

    async def append_once(self, kind: str, op_id: int, payload: dict[str, Any]) -> Event:
        """Append unless an event of this (kind, op_id) already exists."""
        existing = self.find(kind, op_id)
        if existing is not None:
            return existing
        return await self.append(kind, op_id, payload)
