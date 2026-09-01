"""The Runtime: starts, resumes, recovers, signals and cancels runs.

Lifecycle model:

* ``start()`` creates the run record and drives it in an asyncio task.
* A crash (or ``shutdown()``, which is deliberately crash-equivalent) leaves
  incomplete runs in status RUNNING; ``recover()`` replays and continues them.
* ``signal()`` buffers an event in the store and auto-wakes the run if it is
  not currently in memory.
* ``cancel()`` requests cancellation; the run stops at its next live operation,
  runs its compensations, and ends as CANCELLED.

v0.1 assumes one Runtime drives a given run at a time (no cross-process
leasing yet).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import Any

from . import serde
from .context import WorkflowContext
from .errors import (
    CompensationFailed,
    NonDeterminismError,
    RunNotFound,
    SicimError,
    WorkflowCancelled,
    WorkflowFailed,
)
from .journal import Event, Journal, Kind
from .retry import RetryPolicy
from .store import InMemoryStore, RunRecord, RunStatus, SignalRecord, Store
from .workflow import WorkflowFn, get_workflow, workflow_name

logger = logging.getLogger("sicim")


class RunHandle:
    """Handle to a run: await it (or call ``result()``) for the outcome."""

    def __init__(self, run_id: str, *, task: asyncio.Task | None = None, record: RunRecord | None = None):
        self.run_id = run_id
        self._task = task
        self._record = record

    def done(self) -> bool:
        if self._task is not None:
            return self._task.done()
        return self._record is not None and self._record.status.terminal

    async def result(self) -> Any:
        """Return the workflow result, or raise the run's terminal error
        (:class:`WorkflowFailed`, :class:`WorkflowCancelled`,
        :class:`CompensationFailed`, :class:`NonDeterminismError`)."""
        if self._task is not None:
            return await self._task
        record = self._record
        assert record is not None
        if record.status is RunStatus.COMPLETED:
            return record.result
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
    """Drives workflow runs against a Store."""

    def __init__(self, store: Store | None = None, *, default_retry: RetryPolicy | None = None):
        self.store = store or InMemoryStore()
        self.default_retry = default_retry or RetryPolicy()
        self._handles: dict[str, RunHandle] = {}
        self._waiters: dict[str, set[asyncio.Future]] = {}
        self._cancel_flags: dict[str, bool] = {}
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._spawn_lock = asyncio.Lock()

    # -- public API ----------------------------------------------------------

    async def start(self, wf: WorkflowFn, /, *args: Any, run_id: str | None = None, **kwargs: Any) -> RunHandle:
        """Start a run. Idempotent on ``run_id``: if the run already exists (for
        the same workflow), it is resumed with its *stored* inputs and the new
        arguments are ignored."""
        name = workflow_name(wf)
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
            args=serde.roundtrip(list(args)) or [],
            kwargs=serde.roundtrip(kwargs) or {},
        )
        await self.store.create_run(record)
        logger.info("[%s] started workflow '%s'", run_id, name)
        return await self._ensure(record)

    async def resume(self, run_id: str) -> RunHandle:
        """Resume a persisted run (no-op handle if it already reached a terminal state)."""
        record = await self._load(run_id)
        return await self._ensure(record)

    async def recover(self) -> list[RunHandle]:
        """Resume every incomplete run in the store. Call this on process start."""
        records = await self.store.list_runs(status=RunStatus.RUNNING)
        handles = [await self._ensure(record) for record in records]
        if handles:
            logger.info("recovered %d incomplete run(s)", len(handles))
        return handles

    async def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        """Deliver an external event to a run's inbox (waking it if needed)."""
        payload = serde.roundtrip(payload)
        record = await self._load(run_id)
        if record.status.terminal:
            raise SicimError(f"cannot signal run '{run_id}': status is {record.status.value}")
        await self.store.append_signal(run_id, name, payload)
        handle = self._handles.get(run_id)
        if handle is None or handle.done():
            await self._ensure(record)
        self._wake(run_id)

    async def cancel(self, run_id: str) -> None:
        """Request cancellation (cooperative): the run stops at its next *live*
        operation boundary — never mid-replay, never mid-step — runs its
        compensations, and ends as CANCELLED. A step already in flight is
        allowed to finish first (use step ``timeout=`` to bound that)."""
        record = await self._load(run_id)
        if record.status.terminal:
            return
        await self.store.update_run(run_id, cancel_requested=True)
        self._cancel_flags[run_id] = True
        self._cancel_event(run_id).set()
        handle = self._handles.get(run_id)
        if handle is None or handle.done():
            record.cancel_requested = True
            await self._ensure(record)
        self._wake(run_id)

    async def status(self, run_id: str) -> RunRecord:
        return await self._load(run_id)

    async def events(self, run_id: str) -> list[Event]:
        return await self.store.load_events(run_id)

    async def shutdown(self) -> None:
        """Stop driving runs without touching their state (crash-equivalent).

        Incomplete runs stay RUNNING in the store; a later ``recover()``
        replays and continues them.
        """
        tasks = [h._task for h in self._handles.values() if h._task is not None and not h._task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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

    async def _ensure(self, record: RunRecord) -> RunHandle:
        """Get the live handle for a run, spawning its driver task if needed."""
        async with self._spawn_lock:
            handle = self._handles.get(record.run_id)
            if handle is not None and not handle.done():
                return handle
            if record.status.terminal:
                return RunHandle(record.run_id, record=record)
            wf = get_workflow(record.workflow)
            events = await self.store.load_events(record.run_id)
            journal = Journal(record.run_id, self.store, events)
            ctx = WorkflowContext(
                run_id=record.run_id, workflow_name=record.workflow, runtime=self, journal=journal
            )
            self._cancel_flags[record.run_id] = record.cancel_requested
            if record.cancel_requested:
                self._cancel_event(record.run_id).set()
            task = asyncio.create_task(
                self._drive(record, wf, ctx, journal), name=f"sicim:{record.run_id}"
            )
            task.add_done_callback(self._retrieve_exception)
            handle = RunHandle(record.run_id, task=task)
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
        try:
            await journal.append_once(
                Kind.RUN_STARTED, -1, {"workflow": record.workflow, "args": serde.preview(record.args)}
            )
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
