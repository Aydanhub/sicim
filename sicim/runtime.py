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
import logging
import re
import uuid
from typing import Any

from typing import Callable

from . import serde
from .context import WorkflowContext, _ContinueAsNew
from .errors import (
    CompensationFailed,
    LeaseUnavailable,
    NonDeterminismError,
    RunNotFound,
    SicimError,
    WorkflowCancelled,
    WorkflowFailed,
)
from .journal import Event, Journal, Kind
from .retry import RetryPolicy
from .store import InMemoryStore, RunRecord, RunStatus, SignalRecord, Store
from .workflow import WorkflowFn, get_workflow, workflow_name, workflow_version

logger = logging.getLogger("sicim")

_CHAIN_RE = re.compile(r"^(?P<base>.*)#(?P<n>\d+)$")


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
                current = await current._runtime.resume(outcome.next_run_id)
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
        on_event: Callable[[str, Event], None] | None = None,
    ):
        if chain_keep is not None and chain_keep < 0:
            raise ValueError("chain_keep must be None or an int >= 0")
        self.store = store or InMemoryStore()
        self.default_retry = default_retry or RetryPolicy()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_ttl = lease_ttl
        self.signal_poll_interval = signal_poll_interval
        self.chain_keep = chain_keep
        self.on_event = on_event
        self._handles: dict[str, RunHandle] = {}
        self._waiters: dict[str, set[asyncio.Future]] = {}
        self._cancel_flags: dict[str, bool] = {}
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._spawn_lock = asyncio.Lock()
        self._subscription: Callable[[], Any] | None = None
        self._subscribe_attempted = False
        self._closing = False

    # -- public API ----------------------------------------------------------

    async def start(self, wf: WorkflowFn, /, *args: Any, run_id: str | None = None, **kwargs: Any) -> RunHandle:
        """Start a run. Idempotent on ``run_id``: if the run already exists (for
        the same workflow), it is resumed with its *stored* inputs and the new
        arguments are ignored."""
        return await self._start(wf, args, kwargs, run_id=run_id, parent_run_id=None)

    async def _start(
        self,
        wf: WorkflowFn,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        run_id: str | None,
        parent_run_id: str | None,
    ) -> RunHandle:
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
            version=workflow_version(wf),
            args=serde.roundtrip(list(args)) or [],
            kwargs=serde.roundtrip(kwargs) or {},
            parent_run_id=parent_run_id,
        )
        await self.store.create_run(record)
        logger.info("[%s] started workflow '%s'", run_id, name)
        return await self._ensure(record)

    async def resume(self, run_id: str) -> RunHandle:
        """Resume a persisted run (no-op handle if it already reached a terminal state)."""
        record = await self._load(run_id)
        return await self._ensure(record)

    async def recover(self) -> list[RunHandle]:
        """Resume every claimable incomplete run in the store.

        Runs leased by another live worker are skipped; they become claimable
        here once that worker releases them or its lease expires.
        """
        records = await self.store.list_runs(status=RunStatus.RUNNING)
        handles = []
        for record in records:
            try:
                handles.append(await self._ensure(record))
            except LeaseUnavailable:
                logger.debug("[%s] leased elsewhere; skipping recover", record.run_id)
        if handles:
            logger.info("recovered %d incomplete run(s)", len(handles))
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
        self._closing = True
        tasks = [h._task for h in self._handles.values() if h._task is not None and not h._task.done()]
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
            if not await self.store.try_acquire_lease(record.run_id, self.worker_id, self.lease_ttl):
                lease = await self.store.load_lease(record.run_id)
                raise LeaseUnavailable(record.run_id, lease[0] if lease else None)
            await self._ensure_subscribed()
            wf = get_workflow(record.workflow)
            events = await self.store.load_events(record.run_id)
            journal = Journal(record.run_id, self.store, events, on_append=self.on_event)
            ctx = WorkflowContext(
                run_id=record.run_id,
                workflow_name=record.workflow,
                runtime=self,
                journal=journal,
                version=record.version,
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
            await journal.append_once(Kind.RUN_STARTED, -1, started)
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
                    await self.store.create_run(
                        RunRecord(
                            run_id=next_id,
                            workflow=record.workflow,
                            version=workflow_version(wf),  # upgrade point: current code's version
                            args=cont.next_args,
                            kwargs=cont.next_kwargs,
                            parent_run_id=record.parent_run_id,
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
