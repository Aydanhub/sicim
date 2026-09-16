"""The Runtime: starts, resumes, recovers, signals and cancels runs.

Lifecycle model:

* ``start()`` creates the run record and drives it in an asyncio task.
* A crash (or ``shutdown()``, which is deliberately crash-equivalent) leaves
  incomplete runs in status RUNNING; ``recover()`` replays and continues them.
* ``signal()`` buffers an event in the store and auto-wakes the run if it is
  not currently in memory.
* ``cancel()`` requests cancellation; the run stops at its next live operation,
  runs its compensations, and ends as CANCELLED.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import re
import datetime as dt
import time
import uuid
from typing import Any, Callable, Mapping

from . import serde
from .context import WorkflowContext, _ContinueAsNew
from .errors import (
    CompensationFailed,
    LeaseUnavailable,
    NonDeterminismError,
    RunNotFound,
    ScheduleNotFound,
    SicimError,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowNotFound,
)
from .journal import Event, Journal, Kind
from .retry import RetryPolicy
from .schedule import build_spec, parse_spec, scheduled_run_id
from .store import (
    InMemoryStore,
    RunRecord,
    RunStatus,
    ScheduleRecord,
    SignalRecord,
    Store,
    validate_tags,
)
from .workflow import WorkflowFn, get_workflow, workflow_name, workflow_version

logger = logging.getLogger("sicim")

_CHAIN_RE = re.compile(r"^(?P<base>.*)#(?P<n>\d+)$")

#: Run-level markers a reset always drops: the run is being re-opened.
_TERMINAL_RUN_KINDS = frozenset(
    {Kind.RUN_COMPLETED, Kind.RUN_FAILED, Kind.RUN_CANCELLED, Kind.RUN_CONTINUED}
)
#: Compensation outcomes a reset always drops, whatever op they belong to, so
#: the rewound run rebuilds its compensation stack and can compensate again.
_COMP_OUTCOME_KINDS = frozenset({Kind.COMP_ATTEMPT_FAILED, Kind.COMP_COMPLETED, Kind.COMP_FAILED})
#: Where ``reset()`` rewinds to when no op is given: the first failed operation.
_FAILURE_KINDS = frozenset({Kind.STEP_FAILED, Kind.CHILD_FAILED, Kind.WAIT_TIMED_OUT})


def _compensated_reset_error(run_id: str) -> SicimError:
    return SicimError(
        f"run '{run_id}' already ran compensations: replaying the steps they undid would use "
        "journaled results for effects that no longer exist. Pass force=True to reset anyway "
        "(and re-apply those effects yourself)."
    )


def _next_chain_id(run_id: str) -> str:
    match = _CHAIN_RE.match(run_id)
    if match:
        return f"{match.group('base')}#{int(match.group('n')) + 1}"
    return f"{run_id}#2"


class _Continued:
    """Internal driver outcome: the run chained into ``next_run_id``."""

    def __init__(self, next_run_id: str):
        self.next_run_id = next_run_id


class RunHandle:
    """Handle to a run: await it (or call ``result()``) for the outcome."""

    def __init__(
        self,
        run_id: str,
        *,
        task: asyncio.Task | None = None,
        record: RunRecord | None = None,
        runtime: "Runtime | None" = None,
    ):
        self.run_id = run_id
        self._task = task
        self._record = record
        self._runtime = runtime

    def done(self) -> bool:
        if self._task is not None:
            return self._task.done()
        return self._record is not None and self._record.status.terminal

    async def result(self) -> Any:
        """Return the workflow result, following continue-as-new chains to the
        final run, or raise the terminal error (:class:`WorkflowFailed`,
        :class:`WorkflowCancelled`, :class:`CompensationFailed`,
        :class:`NonDeterminismError`)."""
        current: RunHandle = self
        while True:
            outcome = await current._outcome()
            if isinstance(outcome, _Continued):
                if current._runtime is None:
                    raise SicimError(
                        f"run '{current.run_id}' continued as '{outcome.next_run_id}' "
                        "but this handle has no runtime to follow the chain with"
                    )
                try:
                    current = await current._runtime.resume(outcome.next_run_id)
                except LeaseUnavailable:
                    # The successor is driven by another worker: follow it via the store.
                    return await current._runtime._await_remote(outcome.next_run_id)
                continue
            return outcome

    async def _outcome(self) -> Any:
        if self._task is not None:
            return await self._task
        record = self._record
        assert record is not None
        if record.status is RunStatus.COMPLETED:
            return record.result
        if record.status is RunStatus.CONTINUED:
            return _Continued(record.continued_to)
        error = record.error or {}
        if record.status is RunStatus.FAILED:
            raise WorkflowFailed(
                self.run_id,
                error.get("type", "Exception"),
                error.get("message", ""),
                compensated=bool(error.get("compensated")),
            )
        if record.status is RunStatus.CANCELLED:
            raise WorkflowCancelled(self.run_id)
        if record.status is RunStatus.COMPENSATION_FAILED:
            raise CompensationFailed(
                self.run_id,
                error.get("compensation_failures", []),
                error.get("type", "Exception"),
                error.get("message", ""),
            )
        raise SicimError(f"run '{self.run_id}' is still {record.status.value}; resume it first")

    def __await__(self):
        return self.result().__await__()


class Runtime:
    """Drives workflow runs against a Store.

    Each Runtime is a *worker*: before driving a run it acquires that run's
    lease (renewed by a heartbeat at ``lease_ttl / 3``), so several workers can
    safely share one store — a run is only ever driven by one of them, and a
    crashed worker's runs become claimable once its leases expire.

    ``signal_poll_interval`` bounds how long a waiting run goes without
    re-scanning its signal inbox; it only matters for signals sent by another
    worker (in-process signals wake the run immediately). When the store
    offers a push channel (``Store.subscribe``; PostgreSQL LISTEN/NOTIFY), a
    driving Runtime subscribes to it and cross-worker signals/cancellations
    arrive without waiting out the poll or the heartbeat — polling then remains
    only as the backstop.

    ``chain_keep`` turns on automatic continue-as-new chain pruning: after
    each continue, only the newest ``chain_keep`` finished links keep their
    journal and signal history; older links are reduced to their (tiny) run
    records, which stay behind so signal/cancel/result routing by old run ids
    keeps working. None (the default) prunes nothing.

    ``scheduler`` (default True) lets this worker fire due schedules
    (``Runtime.schedule``): once it drives anything, a background loop polls
    the schedule table every ``schedule_poll_interval`` seconds. Several
    workers may do this at once — deterministic run ids plus a
    compare-and-set on the schedule make each tick start at most one run.
    Signal-only clients can pass ``scheduler=False``.

    ``recover_interval`` (seconds; default None = off) makes the worker re-run
    :meth:`recover` periodically in the background, so runs orphaned by a
    crashed worker are taken over automatically once their leases expire —
    without anyone calling ``recover()`` by hand. Like the scheduler loop it
    starts on the first drive/recover/schedule call.

    ``on_event`` is an observability hook called as ``on_event(run_id, event)``
    after every journal append; exceptions it raises are logged and ignored.
    """

    def __init__(
        self,
        store: Store | None = None,
        *,
        default_retry: RetryPolicy | None = None,
        worker_id: str | None = None,
        lease_ttl: float = 30.0,
        signal_poll_interval: float = 1.0,
        chain_keep: int | None = None,
        scheduler: bool = True,
        schedule_poll_interval: float = 1.0,
        recover_interval: float | None = None,
        on_event: Callable[[str, Event], None] | None = None,
    ):
        if chain_keep is not None and chain_keep < 0:
            raise ValueError("chain_keep must be None or an int >= 0")
        if schedule_poll_interval <= 0:
            raise ValueError("schedule_poll_interval must be > 0")
        if recover_interval is not None and recover_interval <= 0:
            raise ValueError("recover_interval must be None or > 0")
        self.store = store or InMemoryStore()
        self.default_retry = default_retry or RetryPolicy()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_ttl = lease_ttl
        self.signal_poll_interval = signal_poll_interval
        self.chain_keep = chain_keep
        self.scheduler = scheduler
        self.schedule_poll_interval = schedule_poll_interval
        self.recover_interval = recover_interval
        self.on_event = on_event
        self._handles: dict[str, RunHandle] = {}
        self._waiters: dict[str, set[asyncio.Future]] = {}
        self._cancel_flags: dict[str, bool] = {}
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._spawn_lock = asyncio.Lock()
        self._subscription: Callable[[], Any] | None = None
        self._subscribe_attempted = False
        self._scheduler_task: asyncio.Task | None = None
        self._recover_task: asyncio.Task | None = None
        self._unregistered_warned: set[str] = set()
        self._unregistered_workflows: set[str] = set()
        self._closing = False

    # -- public API ----------------------------------------------------------

    async def start(
        self,
        wf: WorkflowFn,
        /,
        *args: Any,
        run_id: str | None = None,
        tags: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> RunHandle:
        """Start a run. Idempotent on ``run_id``: if the run already exists (for
        the same workflow), it is resumed with its *stored* inputs and the new
        arguments are ignored. ``tags`` (``str -> str``) are pinned to the run
        for :meth:`list_runs` searches; ``run_id`` and ``tags`` are reserved
        keywords, every other keyword argument goes to the workflow."""
        validate_tags(tags, allow_reserved=False)
        return await self._start(wf, args, kwargs, run_id=run_id, parent_run_id=None, tags=tags)

    async def _start(
        self,
        wf: WorkflowFn,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        run_id: str | None,
        parent_run_id: str | None,
        tags: Mapping[str, str] | None = None,
    ) -> RunHandle:
        name = workflow_name(wf)
        tags = validate_tags(tags)
        run_id = run_id or uuid.uuid4().hex
        existing = await self.store.load_run(run_id)
        if existing is not None:
            if existing.workflow != name:
                raise SicimError(
                    f"run '{run_id}' already exists for workflow '{existing.workflow}', "
                    f"cannot start it as '{name}'"
                )
            return await self._ensure(existing)
        record = RunRecord(
            run_id=run_id,
            workflow=name,
            version=workflow_version(wf),
            args=serde.roundtrip(list(args)) or [],
            kwargs=serde.roundtrip(kwargs) or {},
            parent_run_id=parent_run_id,
            tags=tags,
        )
        try:
            await self.store.create_run(record)
        except Exception:
            # Deterministic ids (child runs, schedule ticks) let two workers
            # race to create the same run; the loser attaches to the winner's.
            existing = await self.store.load_run(run_id)
            if existing is None or existing.workflow != name:
                raise
            return await self._ensure(existing)
        logger.info("[%s] started workflow '%s'", run_id, name)
        return await self._ensure(record)

    async def resume(self, run_id: str) -> RunHandle:
        """Resume a persisted run (no-op handle if it already reached a terminal state)."""
        record = await self._load(run_id)
        return await self._ensure(record)

    async def recover(self) -> list[RunHandle]:
        """Resume every claimable incomplete run in the store.

        Runs leased by another live worker are skipped; they become claimable
        here once that worker releases them or its lease expires. Runs of
        workflows not registered in this process are left alone (warned once
        per workflow) for a worker that has the code.
        """
        self._ensure_background()
        records = await self.store.list_runs(status=RunStatus.RUNNING)
        handles, claimed = [], 0
        for record in records:
            live = self._handles.get(record.run_id)
            if live is not None and not live.done():
                handles.append(live)  # already driven here
                continue
            try:
                handles.append(await self._ensure(record))
                claimed += 1
            except LeaseUnavailable:
                logger.debug("[%s] leased elsewhere; skipping recover", record.run_id)
            except WorkflowNotFound:
                if record.workflow not in self._unregistered_workflows:
                    self._unregistered_workflows.add(record.workflow)
                    logger.warning(
                        "workflow '%s' is not registered in this worker; leaving its runs alone (e.g. '%s')",
                        record.workflow, record.run_id,
                    )
        if claimed:
            logger.info("recovered %d incomplete run(s)", claimed)
        return handles

    async def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        """Deliver an external event to a run's inbox (waking it if needed).

        A run that continued-as-new is followed to the live run in its chain.
        """
        payload = serde.roundtrip(payload)
        record = await self._follow_chain(await self._load(run_id))
        if record.status.terminal:
            raise SicimError(f"cannot signal run '{record.run_id}': status is {record.status.value}")
        await self.store.append_signal(record.run_id, name, payload)
        handle = self._handles.get(record.run_id)
        if handle is None or handle.done():
            try:
                await self._ensure(record)
            except LeaseUnavailable:
                pass  # another worker drives it; its inbox poll picks the signal up
            except WorkflowNotFound:
                # No code for it here: the signal is persisted and consumed by
                # the worker driving (or next recovering) the run.
                logger.info(
                    "[%s] signal '%s' queued; workflow '%s' is not registered in this process",
                    record.run_id, name, record.workflow,
                )
        self._wake(record.run_id)

    async def cancel(self, run_id: str) -> None:
        """Request cancellation (cooperative): the run stops at its next *live*
        operation boundary — never mid-replay, never mid-step — runs its
        compensations, and ends as CANCELLED. A step already in flight is
        allowed to finish first (use step ``timeout=`` to bound that).
        A run that continued-as-new is followed to the live run in its chain."""
        record = await self._follow_chain(await self._load(run_id))
        if record.status.terminal:
            return
        run_id = record.run_id
        await self.store.update_run(run_id, cancel_requested=True)
        self._cancel_flags[run_id] = True
        self._cancel_event(run_id).set()
        handle = self._handles.get(run_id)
        if handle is None or handle.done():
            record.cancel_requested = True
            try:
                await self._ensure(record)
            except LeaseUnavailable:
                pass  # the driving worker sees the persisted flag via its heartbeat
            except WorkflowNotFound:
                # No code for it here: the persisted flag takes effect when a
                # worker with the code drives the run.
                logger.info(
                    "[%s] cancel requested; workflow '%s' is not registered in this process",
                    run_id, record.workflow,
                )
                self._cancel_flags.pop(run_id, None)
                self._cancel_events.pop(run_id, None)
        self._wake(run_id)

    async def reset(
        self,
        run_id: str,
        *,
        to_op: int | None = None,
        resume: bool = True,
        force: bool = False,
    ) -> RunHandle:
        """Rewind a run to just before operation ``to_op`` and drive it again.

        Everything the journal recorded at ``to_op`` and later is dropped, so
        replay re-executes those operations *live* — this is how a run that
        failed on a bad LLM answer (or against a bug you have since fixed) is
        retried from the failing step instead of from the beginning. Operations
        before ``to_op`` keep their recorded outcomes and are not re-executed.
        ``to_op=None`` (the default) rewinds to the first failed operation, or
        to 0 — a full re-run — if nothing failed.

        Resettable at any status: a terminal run is re-opened (status back to
        RUNNING, result/error cleared) and, with ``resume=True``, driven here
        right away. The run must not be driven by another worker (its lease is
        claimed here, otherwise :class:`LeaseUnavailable`); a driver *this*
        worker owns is stopped crash-equivalently first.

        Rewinding the journal does not rewind the world — completed steps
        before ``to_op`` stay done and their side effects stay applied. Two
        consequences:

        * Child runs scheduled at dropped ops are deleted (with their own
          descendants) so replay starts them fresh; their leases are claimed
          the same way the parent's is.
        * Compensation outcomes are dropped too, so the rewound run rebuilds
          its compensation stack. If compensations had already run, their
          side effects are *not* re-applied and the steps they undid are
          replayed from the journal as if still done — that ambiguity is why
          the reset then requires ``force=True``.

        Signals consumed by dropped ``wait_event`` ops return to the inbox, so
        the replayed wait consumes them again. The rewind itself is journaled
        (``run_reset``) and survives further resets as an audit trail.

        Stopping the old driver retires the :class:`RunHandle` that started it
        (awaiting it raises ``CancelledError``); use the handle this method
        returns, or :meth:`resume`, to follow the run from here.
        """
        if to_op is not None and to_op < 0:
            raise ValueError("to_op must be >= 0")
        record = await self._load(run_id)
        if record.status is RunStatus.CONTINUED:
            raise SicimError(
                f"run '{run_id}' ended by continuing as '{record.continued_to}'; reset the live "
                "end of the chain (or that successor) instead — rewinding a link would orphan it"
            )
        stopped = await self._stop_local_driver(run_id)
        if not await self.store.try_acquire_lease(run_id, self.worker_id, self.lease_ttl):
            lease = await self.store.load_lease(run_id)
            raise LeaseUnavailable(run_id, lease[0] if lease else None)

        claimed: list[str] = []
        try:
            events = await self.store.load_events(run_id)
            if to_op is None:
                to_op = next(
                    (e.op_id for e in events if e.kind in _FAILURE_KINDS and e.op_id >= 0), 0
                )
            kept, dropped = [], []
            for event in events:
                stale = (
                    event.op_id >= to_op
                    or event.kind in _TERMINAL_RUN_KINDS
                    or event.kind in _COMP_OUTCOME_KINDS
                )
                (dropped if stale else kept).append(event)
            if not force and any(e.kind == Kind.COMP_COMPLETED for e in dropped):
                raise _compensated_reset_error(run_id)

            # Claim everything that will be deleted *before* touching the
            # journal, so a lease we cannot get leaves the run untouched.
            for event in dropped:
                if event.kind == Kind.CHILD_SCHEDULED:
                    child_run_id = event.payload["child_run_id"]
                    if child_run_id != run_id:  # defensive: never delete ourselves
                        claimed.extend(await self._claim_run_tree(child_run_id))

            renumbered = [dataclasses.replace(e, seq=i) for i, e in enumerate(kept)]
            await self.store.replace_events(run_id, renumbered)
            marker = Event(
                seq=len(renumbered),
                kind=Kind.RUN_RESET,
                op_id=-1,
                payload={
                    "to_op": to_op,
                    "dropped": len(dropped),
                    "from_status": record.status.value,
                    "worker": self.worker_id,
                },
                ts=time.time(),
            )
            await self.store.append_event(run_id, marker)
            for event in dropped:
                if event.kind == Kind.EVENT_CONSUMED:
                    seq = event.payload.get("signal_seq")
                    if seq is not None:
                        await self.store.mark_signal_consumed(run_id, int(seq), False)
            await self.store.update_run(
                run_id,
                status=RunStatus.RUNNING,
                result=None,
                error=None,
                cancel_requested=False,
                continued_to=None,
            )
            for victim in claimed:
                await self.store.delete_run(victim)
            self._cancel_flags.pop(run_id, None)
            self._cancel_events.pop(run_id, None)
            logger.info(
                "[%s] reset to op %d (%d event(s) dropped, %d child run(s) deleted)",
                run_id, to_op, len(dropped), len(claimed),
            )
            if self.on_event is not None:
                try:
                    self.on_event(run_id, marker)
                except Exception:  # noqa: BLE001 - observers must never break a reset
                    logger.exception("on_event observer raised for run '%s'", run_id)
        except BaseException:
            for victim in claimed:
                with contextlib.suppress(Exception):
                    await self.store.release_lease(victim, self.worker_id)
            # A refused reset must leave the run as it found it: if we stopped
            # our own driver for it, put it back to work.
            if stopped:
                with contextlib.suppress(Exception):
                    await self._ensure(await self._load(run_id))
            else:
                with contextlib.suppress(Exception):
                    await self.store.release_lease(run_id, self.worker_id)
            raise

        record = await self._load(run_id)
        if resume:
            try:
                return await self._ensure(record)
            except WorkflowNotFound:
                # No code for it here (a UI or CLI process): the rewound run is
                # left RUNNING for a worker that has the workflow to recover.
                logger.info(
                    "[%s] reset; workflow '%s' is not registered in this process, leaving it "
                    "for a worker that has the code",
                    run_id, record.workflow,
                )
        await self.store.release_lease(run_id, self.worker_id)
        return RunHandle(run_id, record=record, runtime=self)

    async def status(self, run_id: str) -> RunRecord:
        return await self._load(run_id)

    async def events(self, run_id: str) -> list[Event]:
        return await self.store.load_events(run_id)

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
        """Search runs. Every given filter must match; ``tags`` means all of
        the given pairs. Oldest first unless ``newest_first``."""
        return await self.store.list_runs(
            status,
            workflow=workflow,
            tags=tags,
            parent_run_id=parent_run_id,
            limit=limit,
            newest_first=newest_first,
        )

    async def tag(self, run_id: str, tags: Mapping[str, str | None]) -> dict[str, str]:
        """Add, replace or remove (value ``None``) search tags on an existing
        run — at any status — and return its resulting tags.

        A run that continued-as-new is followed to the live end of its chain;
        successors created afterwards carry the tags on. The workflow body
        does not see tags added this way (``ctx.tags`` stays deterministic);
        use ``ctx.tag()`` inside the workflow for that. Keys starting with
        ``sicim.`` are reserved.
        """
        update = validate_tags(tags, allow_none=True, allow_reserved=False)
        record = await self._follow_chain(await self._load(run_id))
        return await self.store.update_tags(record.run_id, update)

    async def list_workflows(self) -> list[str]:
        """Distinct workflow names that have runs in the store."""
        return await self.store.list_workflows()

    async def count_runs(self) -> dict[str, int]:
        """Number of runs per status value."""
        return await self.store.count_runs()

    # -- schedules -----------------------------------------------------------

    async def schedule(
        self,
        wf: WorkflowFn,
        /,
        *args: Any,
        schedule_id: str | None = None,
        every: float | dt.timedelta | None = None,
        cron: str | None = None,
        at: float | dt.datetime | None = None,
        tz: str | None = None,
        tags: Mapping[str, str] | None = None,
        overlap: str = "skip",
        **kwargs: Any,
    ) -> ScheduleRecord:
        """Start runs of ``wf(*args, **kwargs)`` on a schedule.

        Exactly one of ``every`` (seconds or ``timedelta``), ``cron`` (five
        fields or ``@daily``-style alias; UTC unless ``tz`` names an IANA zone)
        or ``at`` (UNIX timestamp or ``datetime``, one-shot) is required. Each
        tick starts a run with the deterministic id ``<schedule_id>@<time>``,
        tagged ``sicim.schedule=<schedule_id>`` plus ``tags``. With
        ``overlap="skip"`` (default) a tick is skipped while the previous run
        is still running; ``"allow"`` starts runs regardless. Ticks missed
        while no worker was up collapse into a single catch-up run.

        Idempotent on ``schedule_id``: an existing schedule is returned as is.
        """
        spec = build_spec(every=every, cron=cron, at=at, tz=tz)
        if overlap not in ("skip", "allow"):
            raise ValueError("overlap must be 'skip' or 'allow'")
        name = workflow_name(wf)
        schedule_id = schedule_id or uuid.uuid4().hex
        existing = await self.store.load_schedule(schedule_id)
        if existing is None:
            record = ScheduleRecord(
                schedule_id=schedule_id,
                workflow=name,
                spec=spec.text,
                args=serde.roundtrip(list(args)) or [],
                kwargs=serde.roundtrip(kwargs) or {},
                tz=tz,
                tags=validate_tags(tags, allow_reserved=False),
                overlap=overlap,
                next_fire_at=spec.first_fire_at(time.time()),
            )
            try:
                await self.store.create_schedule(record)
            except Exception:
                existing = await self.store.load_schedule(schedule_id)
                if existing is None:
                    raise
            else:
                logger.info("[schedule %s] created for '%s' (%s)", schedule_id, name, spec.text)
                existing = record
        if existing.workflow != name:
            raise SicimError(
                f"schedule '{schedule_id}' already exists for workflow '{existing.workflow}', "
                f"cannot schedule it as '{name}'"
            )
        self._ensure_background()
        return existing

    async def get_schedule(self, schedule_id: str) -> ScheduleRecord:
        record = await self.store.load_schedule(schedule_id)
        if record is None:
            raise ScheduleNotFound(f"no schedule with id '{schedule_id}'")
        return record

    async def list_schedules(self) -> list[ScheduleRecord]:
        return await self.store.list_schedules()

    async def pause_schedule(self, schedule_id: str) -> None:
        """Stop firing until :meth:`resume_schedule`; runs already started continue."""
        await self.get_schedule(schedule_id)
        await self.store.update_schedule(schedule_id, paused=True)

    async def resume_schedule(self, schedule_id: str) -> None:
        """Resume a paused schedule from *now*: ticks missed while paused are skipped."""
        record = await self.get_schedule(schedule_id)
        spec = parse_spec(record.spec, record.tz)
        await self.store.update_schedule(
            schedule_id, paused=False, next_fire_at=spec.next_after(time.time())
        )
        self._ensure_background()

    async def unschedule(self, schedule_id: str) -> None:
        """Delete a schedule; runs it already started are unaffected."""
        await self.get_schedule(schedule_id)
        await self.store.delete_schedule(schedule_id)
        logger.info("[schedule %s] deleted", schedule_id)

    async def shutdown(self) -> None:
        """Stop driving runs without touching their state (crash-equivalent).

        Incomplete runs stay RUNNING in the store; a later ``recover()``
        replays and continues them.
        """
        self._closing = True
        tasks = [h._task for h in self._handles.values() if h._task is not None and not h._task.done()]
        for loop_task in (self._scheduler_task, self._recover_task):
            if loop_task is not None and not loop_task.done():
                tasks.append(loop_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._subscription is not None:
            subscription, self._subscription = self._subscription, None
            with contextlib.suppress(BaseException):
                await subscription()
        for waiters in self._waiters.values():
            for fut in waiters:
                if not fut.done():
                    fut.cancel()
        self._waiters.clear()

    async def __aenter__(self) -> "Runtime":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.shutdown()

    # -- internals -----------------------------------------------------------

    async def _load(self, run_id: str) -> RunRecord:
        record = await self.store.load_run(run_id)
        if record is None:
            raise RunNotFound(f"no run with id '{run_id}'")
        return record

    async def _follow_chain(self, record: RunRecord) -> RunRecord:
        """Follow continue-as-new links to the newest run in the chain."""
        seen = {record.run_id}
        while record.status is RunStatus.CONTINUED and record.continued_to:
            if record.continued_to in seen:
                break  # defensive: malformed chains must not loop forever
            record = await self._load(record.continued_to)
            seen.add(record.run_id)
        return record

    async def _prune_chain(self, tail_id: str, workflow: str) -> None:
        """Automatic chain pruning (``chain_keep=N``): keep full history for
        the newest N finished links behind the live run ``tail_id``; older
        links lose their journal and signal inbox but keep their run records,
        so routing by old run ids stays intact. Only records that provably
        belong to the chain (CONTINUED, forward-linked, same workflow) are
        touched; failures are logged, never raised — pruning is maintenance,
        not part of the run's outcome.
        """
        try:
            child_id, kept = tail_id, 0
            while True:
                match = _CHAIN_RE.match(child_id)
                if not match:
                    return  # reached the chain root
                n = int(match.group("n"))
                pred_id = f"{match.group('base')}#{n - 1}" if n > 2 else match.group("base")
                pred = await self.store.load_run(pred_id)
                if (
                    pred is None
                    or pred.status is not RunStatus.CONTINUED
                    or pred.continued_to != child_id
                    or pred.workflow != workflow
                ):
                    return
                kept += 1
                if kept > self.chain_keep:
                    if not await self.store.load_events(pred_id):
                        return  # already pruned earlier -> everything older is too
                    await self.store.delete_run_history(pred_id)
                    logger.info("[%s] pruned chain history (record kept for routing)", pred_id)
                child_id = pred_id
        except Exception:
            logger.exception("chain pruning behind '%s' failed; continuing", tail_id)

    async def _stop_local_driver(self, run_id: str) -> bool:
        """Stop a driver task this worker owns, crash-equivalently; returns
        whether there was one.

        Used by :meth:`reset`: the run must stand still while its journal is
        rewritten. Cancelling clears the cancel flag first so the driver does
        *not* take the cooperative-cancellation path (compensations); it exits
        the way it would in a crash, writing nothing.
        """
        handle = self._handles.get(run_id)
        if handle is None or handle._task is None or handle._task.done():
            return False
        self._cancel_flags[run_id] = False
        handle._task.cancel()
        with contextlib.suppress(BaseException):
            await handle._task
        self._handles.pop(run_id, None)
        return True

    async def _claim_run_tree(self, run_id: str) -> list[str]:
        """Claim the leases of a run and its descendants, returning their ids.

        Raises :class:`LeaseUnavailable` if any of them is driven elsewhere;
        the caller releases whatever was claimed before that.
        """
        record = await self.store.load_run(run_id)
        if record is None:
            return []
        await self._stop_local_driver(run_id)
        if not await self.store.try_acquire_lease(run_id, self.worker_id, self.lease_ttl):
            lease = await self.store.load_lease(run_id)
            raise LeaseUnavailable(run_id, lease[0] if lease else None)
        claimed = [run_id]
        for child in await self.store.list_runs(parent_run_id=run_id):
            if child.run_id != run_id:
                claimed.extend(await self._claim_run_tree(child.run_id))
        return claimed

    async def _await_remote(self, run_id: str) -> Any:
        """Outcome of a run driven by *another* worker: poll the store until the
        run (or the live end of its chain) is terminal, then report exactly
        what a local :class:`RunHandle` would."""
        while True:
            record = await self._follow_chain(await self._load(run_id))
            if record.status.terminal:
                return await RunHandle(record.run_id, record=record, runtime=self)._outcome()
            run_id = record.run_id
            await asyncio.sleep(self.signal_poll_interval)

    async def _ensure(self, record: RunRecord) -> RunHandle:
        """Get the live handle for a run, spawning its driver task if needed."""
        async with self._spawn_lock:
            handle = self._handles.get(record.run_id)
            if handle is not None and not handle.done():
                return handle
            if record.status.terminal or self._closing:
                # A shutting-down runtime spawns nothing (crash-equivalence);
                # recover() on the next runtime picks the run up.
                return RunHandle(record.run_id, record=record, runtime=self)
            # Resolve the code before taking the lease: a process without this
            # workflow must never end up holding (and blocking) the run's lease.
            wf = get_workflow(record.workflow)
            if not await self.store.try_acquire_lease(record.run_id, self.worker_id, self.lease_ttl):
                lease = await self.store.load_lease(record.run_id)
                raise LeaseUnavailable(record.run_id, lease[0] if lease else None)
            await self._ensure_subscribed()
            self._ensure_background()
            events = await self.store.load_events(record.run_id)
            journal = Journal(record.run_id, self.store, events, on_append=self.on_event)
            ctx = WorkflowContext(
                run_id=record.run_id,
                workflow_name=record.workflow,
                runtime=self,
                journal=journal,
                version=record.version,
                tags=record.tags,
            )
            self._cancel_flags[record.run_id] = record.cancel_requested
            if record.cancel_requested:
                self._cancel_event(record.run_id).set()
            task = asyncio.create_task(
                self._drive(record, wf, ctx, journal), name=f"sicim:{record.run_id}"
            )
            task.add_done_callback(self._retrieve_exception)
            handle = RunHandle(record.run_id, task=task, runtime=self)
            self._handles[record.run_id] = handle
            return handle

    @staticmethod
    def _retrieve_exception(task: asyncio.Task) -> None:
        # Keep fire-and-forget runs from tripping asyncio's "exception was
        # never retrieved" warning; outcomes are persisted in the store anyway.
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
            if exc is not None:
                logger.debug("task %s finished with %r", task.get_name(), exc)

    async def _drive(self, record: RunRecord, wf: WorkflowFn, ctx: WorkflowContext, journal: Journal) -> Any:
        run_id = record.run_id
        replaying = journal.max_op_id >= 0
        if replaying:
            logger.info("[%s] resuming '%s' (replaying %d events)", run_id, record.workflow, len(journal.events))
        heartbeat = asyncio.create_task(
            self._heartbeat(run_id, asyncio.current_task()), name=f"sicim-lease:{run_id}"
        )
        try:
            started = {"workflow": record.workflow, "args": serde.preview(record.args)}
            if record.parent_run_id:
                started["parent"] = record.parent_run_id
            if record.tags:
                started["tags"] = record.tags
            started_event = await journal.append_once(Kind.RUN_STARTED, -1, started)
            # The body sees the tags pinned in the journal (plus its own
            # ctx.tag() updates as they replay), never later external edits.
            ctx._tags = dict(started_event.payload.get("tags") or {})
            try:
                result = await wf(ctx, *record.args, **record.kwargs)
                result = serde.roundtrip(result)
            except asyncio.CancelledError:
                if not self._cancel_flags.get(run_id, False):
                    raise  # crash-equivalent shutdown: leave the run RUNNING
                logger.info("[%s] cancelled; running %d compensation(s)", run_id, len(ctx._comp_stack))
                failures = await ctx._run_compensations()
                if failures:
                    error = {
                        "type": "WorkflowCancelled",
                        "message": "run was cancelled",
                        "compensation_failures": failures,
                    }
                    await journal.append_once(Kind.RUN_CANCELLED, -1, {"compensation_failures": failures})
                    await self.store.update_run(run_id, status=RunStatus.COMPENSATION_FAILED, error=error)
                    raise CompensationFailed(run_id, failures, "WorkflowCancelled", "run was cancelled") from None
                await journal.append_once(Kind.RUN_CANCELLED, -1, {})
                await self.store.update_run(
                    run_id,
                    status=RunStatus.CANCELLED,
                    error={"type": "WorkflowCancelled", "message": "run was cancelled"},
                )
                raise WorkflowCancelled(run_id) from None
            except _ContinueAsNew as cont:
                # The run ends here and chains into a successor with a fresh
                # journal. Every step below is idempotent, so a crash anywhere
                # in this block resumes cleanly (replay reaches the same point).
                next_id = _next_chain_id(run_id)
                await journal.append_once(
                    Kind.RUN_CONTINUED, -1,
                    {"next_run_id": next_id, "args": serde.preview(cont.next_args)},
                )
                if await self.store.load_run(next_id) is None:
                    current = await self.store.load_run(run_id)  # tags may have changed since start
                    await self.store.create_run(
                        RunRecord(
                            run_id=next_id,
                            workflow=record.workflow,
                            version=workflow_version(wf),  # upgrade point: current code's version
                            args=cont.next_args,
                            kwargs=cont.next_kwargs,
                            parent_run_id=record.parent_run_id,
                            tags=current.tags if current is not None else record.tags,
                        )
                    )
                await self.store.update_run(run_id, status=RunStatus.CONTINUED, continued_to=next_id)
                logger.info("[%s] continued as '%s'", run_id, next_id)
                if self.chain_keep is not None:
                    await self._prune_chain(next_id, record.workflow)
                next_record = await self.store.load_run(next_id)
                if next_record is not None and not next_record.status.terminal:
                    with contextlib.suppress(LeaseUnavailable):
                        await self._ensure(next_record)
                return _Continued(next_id)
            except NonDeterminismError:
                # Deliberately leave the run RUNNING and untouched: fixing the
                # code and resuming again is the recovery path.
                logger.exception("[%s] non-determinism detected during replay", run_id)
                raise
            except BaseException as exc:
                info = serde.error_info(exc)
                logger.info(
                    "[%s] failed (%s: %s); running %d compensation(s)",
                    run_id, info["type"], info["message"], len(ctx._comp_stack),
                )
                failures = await ctx._run_compensations()
                if failures:
                    error = {**info, "compensated": False, "compensation_failures": failures}
                    await journal.append_once(
                        Kind.RUN_FAILED, -1,
                        {"error": info, "compensated": False, "compensation_failures": failures},
                    )
                    await self.store.update_run(run_id, status=RunStatus.COMPENSATION_FAILED, error=error)
                    raise CompensationFailed(run_id, failures, info["type"], info["message"]) from exc
                compensated = bool(ctx._comp_stack)
                await journal.append_once(Kind.RUN_FAILED, -1, {"error": info, "compensated": compensated})
                await self.store.update_run(
                    run_id, status=RunStatus.FAILED, error={**info, "compensated": compensated}
                )
                raise WorkflowFailed(run_id, info["type"], info["message"], compensated=compensated) from exc
            else:
                await journal.append_once(Kind.RUN_COMPLETED, -1, {"result": result})
                await self.store.update_run(run_id, status=RunStatus.COMPLETED, result=result)
                logger.info("[%s] completed", run_id)
                return result
        finally:
            self._cancel_flags.pop(run_id, None)
            self._cancel_events.pop(run_id, None)
            heartbeat.cancel()
            with contextlib.suppress(BaseException):
                await heartbeat
            # Graceful handover; after a hard crash the TTL expiry covers this.
            with contextlib.suppress(BaseException):
                await self.store.release_lease(run_id, self.worker_id)

    async def _heartbeat(self, run_id: str, driver: asyncio.Task) -> None:
        """Renew the run's lease and mirror cross-worker state while driving it.

        Losing the lease (another worker took over after an expiry) stops the
        driver crash-equivalently: no state is written, the run stays with the
        new owner. A ``cancel_requested`` flag persisted by another worker is
        picked up here and turned into local cooperative cancellation.
        """
        interval = max(self.lease_ttl / 3.0, 0.05)
        while True:
            await asyncio.sleep(interval)
            if not await self.store.renew_lease(run_id, self.worker_id, self.lease_ttl):
                logger.error("[%s] lease lost; stopping driver (run stays RUNNING)", run_id)
                driver.cancel()
                return
            if not self._cancel_flags.get(run_id, False):
                record = await self.store.load_run(run_id)
                if record is not None and record.cancel_requested:
                    self._cancel_flags[run_id] = True
                    self._cancel_event(run_id).set()
                    self._wake(run_id)

    async def _ensure_subscribed(self) -> None:
        """Open the store's push channel (if it has one), once per Runtime.

        Push delivers cross-worker signals and cancellations instantly;
        subscribing lazily — on the first driven run — keeps signal-only
        Runtimes from holding listener connections. Failure falls back to
        polling.
        """
        if self._subscribe_attempted:
            return
        self._subscribe_attempted = True
        try:
            self._subscription = await self.store.subscribe(self._on_store_notify)
        except Exception:
            logger.exception("store subscribe failed; relying on polling")

    def _on_store_notify(self, kind: str, run_id: str) -> None:
        """Dispatch a store push notification (see ``Store.subscribe``)."""
        if kind == "cancel" and run_id in self._cancel_flags:  # we drive this run
            self._cancel_flags[run_id] = True
            self._cancel_event(run_id).set()
        self._wake(run_id)

    # -- scheduler -----------------------------------------------------------

    def _ensure_background(self) -> None:
        """Start this worker's background loops, once each: the schedule-firing
        loop (opt-out via ``scheduler=False``) and the orphan-recovery loop
        (opt-in via ``recover_interval``)."""
        if self._closing:
            return
        if self.scheduler and self._scheduler_task is None:
            self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="sicim-scheduler")
            self._scheduler_task.add_done_callback(self._retrieve_exception)
        if self.recover_interval is not None and self._recover_task is None:
            self._recover_task = asyncio.create_task(self._recover_loop(), name="sicim-recover")
            self._recover_task.add_done_callback(self._retrieve_exception)

    async def _recover_loop(self) -> None:
        """Periodically take over claimable orphaned runs (``recover_interval``)."""
        while True:
            await asyncio.sleep(self.recover_interval)
            try:
                await self.recover()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("periodic recover failed; retrying in %.1fs", self.recover_interval)

    async def _scheduler_loop(self) -> None:
        while True:
            try:
                await self._fire_due_schedules()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduler tick failed; retrying in %.1fs", self.schedule_poll_interval)
            await asyncio.sleep(self.schedule_poll_interval)

    async def _fire_due_schedules(self) -> None:
        now = time.time()
        for record in await self.store.list_due_schedules(now):
            await self._fire_schedule(record, now)

    async def _fire_schedule(self, sched: ScheduleRecord, now: float) -> None:
        """Start the run for one due tick and advance the schedule.

        Safe with any number of workers: the tick's run id is deterministic
        (the store's primary key rejects a second create, and the loser just
        attaches), and the schedule is advanced with a compare-and-set on the
        fire time, so exactly one worker records the tick. Missed ticks are
        collapsed: the next fire time is computed from ``max(fire_at, now)``.
        """
        fire_at = sched.next_fire_at
        assert fire_at is not None
        spec = parse_spec(sched.spec, sched.tz)
        next_fire_at = spec.next_after(max(fire_at, now))
        run_id = scheduled_run_id(sched.schedule_id, fire_at)
        if sched.overlap == "skip" and sched.last_run_id and await self._run_is_live(sched.last_run_id):
            if await self.store.update_schedule(
                sched.schedule_id, expected_next_fire_at=fire_at, next_fire_at=next_fire_at
            ):
                logger.info(
                    "[schedule %s] skipped tick %s: previous run '%s' still running",
                    sched.schedule_id, run_id, sched.last_run_id,
                )
            return
        try:
            wf = get_workflow(sched.workflow)
        except WorkflowNotFound:
            # Leave the tick due: a worker that has the code will fire it.
            if sched.schedule_id not in self._unregistered_warned:
                self._unregistered_warned.add(sched.schedule_id)
                logger.warning(
                    "[schedule %s] workflow '%s' is not registered in this worker; not firing",
                    sched.schedule_id, sched.workflow,
                )
            return
        tags = {**sched.tags, "sicim.schedule": sched.schedule_id}
        try:
            await self._start(
                wf, tuple(sched.args), dict(sched.kwargs), run_id=run_id, parent_run_id=None, tags=tags
            )
        except LeaseUnavailable:
            pass  # another worker fired this tick first and is driving the run
        if await self.store.update_schedule(
            sched.schedule_id, expected_next_fire_at=fire_at, next_fire_at=next_fire_at, last_run_id=run_id
        ):
            logger.info("[schedule %s] started run '%s'", sched.schedule_id, run_id)

    async def _run_is_live(self, run_id: str) -> bool:
        record = await self.store.load_run(run_id)
        if record is None:
            return False
        return not (await self._follow_chain(record)).status.terminal

    # -- hooks used by WorkflowContext ---------------------------------------

    def _is_cancel_requested(self, run_id: str) -> bool:
        return self._cancel_flags.get(run_id, False)

    def _cancel_event(self, run_id: str) -> asyncio.Event:
        event = self._cancel_events.get(run_id)
        if event is None:
            event = self._cancel_events[run_id] = asyncio.Event()
        return event

    async def _next_unconsumed_signal(
        self, run_id: str, name: str, exclude: set[int]
    ) -> SignalRecord | None:
        signals = await self.store.load_signals(run_id)
        for signal in signals:
            if signal.name == name and not signal.consumed and signal.seq not in exclude:
                return signal
        return None

    async def _mark_signal_consumed(self, run_id: str, seq: int) -> None:
        await self.store.mark_signal_consumed(run_id, seq)

    def _register_waiter(self, run_id: str) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(run_id, set()).add(fut)
        return fut

    def _unregister_waiter(self, run_id: str, fut: asyncio.Future) -> None:
        waiters = self._waiters.get(run_id)
        if waiters is not None:
            waiters.discard(fut)
            if not waiters:
                self._waiters.pop(run_id, None)

    def _wake(self, run_id: str) -> None:
        for fut in list(self._waiters.get(run_id, ())):
            if not fut.done():
                fut.set_result(None)
