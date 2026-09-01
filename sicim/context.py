"""WorkflowContext: the deterministic-replay engine seen by workflow code.

Every effectful operation goes through the context. Each call takes the next
``op_id`` *synchronously* (so parallel ``ctx.gather`` branches get stable ids
in code order), then consults the journal:

* outcome already recorded  -> return it without executing (replay)
* no outcome recorded       -> execute live, journal the outcome, return it

Determinism contract for workflow bodies: no direct I/O, no ``time``/
``random``/``uuid`` (use ``ctx.now()/ctx.random()/ctx.uuid4()``), no iteration
over unordered collections that affects operation order. All real work belongs
in steps.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import logging
import random as _random
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from . import serde
from .errors import (
    ChildFailed,
    CompensationFailed,
    NonDeterminismError,
    StepFailed,
    WaitTimeout,
    WorkflowCancelled,
    WorkflowFailed,
)
from .journal import Journal, Kind
from .retry import RetryPolicy
from .workflow import WorkflowFn, workflow_name

if TYPE_CHECKING:
    from .runtime import Runtime

logger = logging.getLogger("sicim")


def _callable_name(fn: Callable[..., Any]) -> str:
    inner = fn
    while isinstance(inner, functools.partial):
        inner = inner.func
    return getattr(inner, "__name__", type(inner).__name__)


def _is_async_callable(fn: Callable[..., Any]) -> bool:
    inner = fn
    while isinstance(inner, functools.partial):
        inner = inner.func
    if inspect.iscoroutinefunction(inner):
        return True
    call = getattr(inner, "__call__", None)  # objects with an async __call__
    return call is not None and inspect.iscoroutinefunction(call)


class _ContinueAsNew(BaseException):
    """Internal control flow for ctx.continue_as_new (not an error).

    BaseException so workflow-level ``except Exception`` blocks cannot
    accidentally swallow it.
    """

    def __init__(self, next_args: list, next_kwargs: dict):
        self.next_args = next_args
        self.next_kwargs = next_kwargs
        super().__init__("workflow requested continue-as-new")


@dataclass
class _Compensation:
    op_id: int
    name: str
    fn: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    retry: RetryPolicy | None = None


class WorkflowContext:
    """Handle passed as the first argument to every workflow function."""

    def __init__(
        self, *, run_id: str, workflow_name: str, runtime: "Runtime", journal: Journal, version: int = 1
    ):
        self.run_id = run_id
        self.workflow_name = workflow_name
        #: Workflow version pinned at run start. New code branches on it
        #: (``if ctx.version >= 2: ...``) to evolve without breaking old runs.
        self.version = version
        self._runtime = runtime
        self._journal = journal
        self._op_counter = 0
        # Ops at or below this id existed before this (re)execution -> replay.
        self._replay_boundary = journal.max_op_id
        self._comp_stack: list[_Compensation] = []
        self._inflight_signal_seqs: set[int] = set()

    # -- bookkeeping ---------------------------------------------------------

    def _next_op(self) -> int:
        op_id = self._op_counter
        self._op_counter += 1
        return op_id

    @property
    def is_replaying(self) -> bool:
        """True while re-executing operations that were journaled before this resume."""
        return self._op_counter <= self._replay_boundary

    def log(self, message: str, *args: Any) -> None:
        """Log only on live execution — silent during replay, so no duplicate lines."""
        if not self.is_replaying:
            logger.info("[%s] " + message, self.run_id, *args)

    def _check_cancel(self) -> None:
        if self._runtime._is_cancel_requested(self.run_id):
            raise asyncio.CancelledError()

    async def _wait_or_cancel(self, seconds: float) -> None:
        """In-process wait that wakes early — raising CancelledError — when
        ``Runtime.cancel()`` is requested for this run.

        Run cancellation is cooperative (it takes effect at operation
        boundaries, never mid-replay), so long timer/backoff waits must watch
        for it themselves instead of relying on ``Task.cancel()``.
        """
        cancel_ev = self._runtime._cancel_event(self.run_id)
        if cancel_ev.is_set():
            raise asyncio.CancelledError()
        ev_task = asyncio.ensure_future(cancel_ev.wait())
        sleep_task = asyncio.ensure_future(asyncio.sleep(seconds))
        try:
            await asyncio.wait({ev_task, sleep_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            ev_task.cancel()
            sleep_task.cancel()
            await asyncio.gather(ev_task, sleep_task, return_exceptions=True)
        if cancel_ev.is_set():
            raise asyncio.CancelledError()

    def _expect(self, kind: str, op_id: int, name: str | None = None):
        """Return the defining event at op_id, verifying it matches the request."""
        recorded = self._journal.defining(op_id)
        if recorded is None:
            return None
        recorded_name = recorded.payload.get("name")
        if recorded.kind != kind or (name is not None and recorded_name != name):
            raise NonDeterminismError(
                f"replay diverged at op {op_id} of run '{self.run_id}': journal recorded "
                f"{recorded.kind}(name={recorded_name!r}) but the code requested "
                f"{kind}(name={name!r}). The workflow code likely changed between the "
                "original execution and this resume. Fix/revert the code and resume again, "
                "or start a fresh run for the new logic."
            )
        return recorded

    # -- steps ---------------------------------------------------------------

    def step(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        name: str | None = None,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
        compensate: Callable[..., Any] | None = None,
        compensate_args: tuple[Any, ...] = (),
        compensate_kwargs: dict[str, Any] | None = None,
        compensate_retry: RetryPolicy | None = None,
        **kwargs: Any,
    ) -> Awaitable[Any]:
        """Run ``fn(*args, **kwargs)`` as a durable step.

        The step executes at-least-once; its (JSON-serializable) return value
        is journaled and never recomputed on replay. Sync callables run in a
        worker thread. ``compensate`` registers a saga compensation that runs
        (LIFO) if the workflow later fails or is cancelled.

        The keyword parameters listed above are reserved for sicim; any other
        ``**kwargs`` are passed through to ``fn``.
        """
        op_id = self._next_op()
        return self._step(
            op_id,
            fn,
            args,
            kwargs,
            name=name or _callable_name(fn),
            retry=retry,
            timeout=timeout,
            compensate=compensate,
            compensate_args=compensate_args,
            compensate_kwargs=compensate_kwargs or {},
            compensate_retry=compensate_retry,
        )

    async def _step(
        self,
        op_id: int,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        name: str,
        retry: RetryPolicy | None,
        timeout: float | None,
        compensate: Callable[..., Any] | None,
        compensate_args: tuple[Any, ...],
        compensate_kwargs: dict[str, Any],
        compensate_retry: RetryPolicy | None,
    ) -> Any:
        journal = self._journal
        recorded = self._expect(Kind.STEP_SCHEDULED, op_id, name=name)
        if recorded is None:
            self._check_cancel()
            await journal.append(
                Kind.STEP_SCHEDULED,
                op_id,
                {
                    "name": name,
                    "args": serde.preview(args),
                    "kwargs": serde.preview(kwargs),
                    "compensation": _callable_name(compensate) if compensate else None,
                },
            )

        def register_compensation() -> None:
            if compensate is not None:
                self._comp_stack.append(
                    _Compensation(
                        op_id=op_id,
                        name=_callable_name(compensate),
                        fn=compensate,
                        args=compensate_args,
                        kwargs=compensate_kwargs,
                        retry=compensate_retry,
                    )
                )

        completed = journal.find(Kind.STEP_COMPLETED, op_id)
        if completed is not None:
            register_compensation()
            return completed.payload["result"]

        failed = journal.find(Kind.STEP_FAILED, op_id)
        if failed is not None:
            error = failed.payload["error"]
            raise StepFailed(name, op_id, failed.payload["attempts"], error["type"], error["message"])

        # Live execution with durable attempt counting.
        policy = retry or self._runtime.default_retry
        attempt = journal.count(Kind.STEP_ATTEMPT_FAILED, op_id)
        while True:
            attempt += 1
            self._check_cancel()
            try:
                result = await self._invoke(fn, args, kwargs, timeout)
                result = serde.roundtrip(result)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 - classified by the retry policy
                if policy.should_retry(exc, attempt):
                    await journal.append(
                        Kind.STEP_ATTEMPT_FAILED,
                        op_id,
                        {"name": name, "attempt": attempt, "error": serde.error_info(exc)},
                    )
                    await self._wait_or_cancel(policy.delay(attempt))
                    continue
                info = serde.error_info(exc)
                await journal.append(
                    Kind.STEP_FAILED,
                    op_id,
                    {"name": name, "attempts": attempt, "error": info},
                )
                raise StepFailed(name, op_id, attempt, info["type"], info["message"]) from exc
            await journal.append(
                Kind.STEP_COMPLETED,
                op_id,
                {"name": name, "result": result, "attempts": attempt},
            )
            register_compensation()
            return result

    async def _invoke(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any], timeout: float | None
    ) -> Any:
        async def call() -> Any:
            if _is_async_callable(fn):
                return await fn(*args, **kwargs)
            return await asyncio.to_thread(fn, *args, **kwargs)

        if timeout is None:
            return await call()
        async with asyncio.timeout(timeout):
            return await call()

    # -- saga compensations --------------------------------------------------

    def add_compensation(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        name: str | None = None,
        retry: RetryPolicy | None = None,
        **kwargs: Any,
    ) -> Awaitable[None]:
        """Register a compensation not tied to a step (e.g. for an external effect)."""
        op_id = self._next_op()
        return self._add_compensation(op_id, fn, args, kwargs, name=name or _callable_name(fn), retry=retry)

    async def _add_compensation(self, op_id, fn, args, kwargs, *, name, retry) -> None:
        if self._expect(Kind.COMP_REGISTERED, op_id, name=name) is None:
            await self._journal.append(Kind.COMP_REGISTERED, op_id, {"name": name})
        self._comp_stack.append(
            _Compensation(op_id=op_id, name=name, fn=fn, args=args, kwargs=kwargs, retry=retry)
        )

    async def _run_compensations(self) -> list[dict[str, Any]]:
        """Run registered compensations LIFO. Durable and resumable.

        Best-effort: a failing compensation is journaled and the remaining ones
        still run; failures are returned for the runtime to surface.
        """
        journal = self._journal
        failures: list[dict[str, Any]] = []
        for entry in reversed(self._comp_stack):
            if journal.find(Kind.COMP_COMPLETED, entry.op_id) is not None:
                continue
            prior = journal.find(Kind.COMP_FAILED, entry.op_id)
            if prior is not None:
                failures.append(prior.payload)
                continue
            policy = entry.retry or self._runtime.default_retry
            attempt = journal.count(Kind.COMP_ATTEMPT_FAILED, entry.op_id)
            while True:
                attempt += 1
                try:
                    await self._invoke(entry.fn, entry.args, entry.kwargs, None)
                except asyncio.CancelledError:
                    raise  # process shutdown: resume finishes compensations later
                except BaseException as exc:  # noqa: BLE001
                    if policy.should_retry(exc, attempt):
                        await journal.append(
                            Kind.COMP_ATTEMPT_FAILED,
                            entry.op_id,
                            {"name": entry.name, "attempt": attempt, "error": serde.error_info(exc)},
                        )
                        await asyncio.sleep(policy.delay(attempt))
                        continue
                    info = {"name": entry.name, "attempts": attempt, "error": serde.error_info(exc)}
                    await journal.append(Kind.COMP_FAILED, entry.op_id, info)
                    failures.append(info)
                    break
                await journal.append(
                    Kind.COMP_COMPLETED, entry.op_id, {"name": entry.name, "attempts": attempt}
                )
                break
        return failures

    # -- child workflows -----------------------------------------------------

    def child(
        self,
        wf: WorkflowFn,
        /,
        *args: Any,
        run_id: str | None = None,
        compensate: Callable[..., Any] | None = None,
        compensate_args: tuple[Any, ...] = (),
        compensate_kwargs: dict[str, Any] | None = None,
        compensate_retry: RetryPolicy | None = None,
        **kwargs: Any,
    ) -> Awaitable[Any]:
        """Run another registered workflow as a durable child and await its result.

        The child is a full run of its own (own journal, own compensations, own
        version pin), so a crash resumes parent and child independently. The
        child's run id is deterministic (``<parent>.c<op>`` unless ``run_id`` is
        given), which makes starting it idempotent across replays. A terminal
        child error raises :class:`ChildFailed` in the parent; cancelling the
        parent while it awaits a child cancels the child too.
        """
        op_id = self._next_op()
        return self._child(
            op_id,
            wf,
            args,
            kwargs,
            run_id=run_id,
            compensate=compensate,
            compensate_args=compensate_args,
            compensate_kwargs=compensate_kwargs or {},
            compensate_retry=compensate_retry,
        )

    async def _child(
        self,
        op_id: int,
        wf: WorkflowFn,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        run_id: str | None,
        compensate: Callable[..., Any] | None,
        compensate_args: tuple[Any, ...],
        compensate_kwargs: dict[str, Any],
        compensate_retry: RetryPolicy | None,
    ) -> Any:
        journal = self._journal
        child_name = workflow_name(wf)
        recorded = self._expect(Kind.CHILD_SCHEDULED, op_id, name=child_name)
        if recorded is None:
            self._check_cancel()
            child_run_id = run_id or f"{self.run_id}.c{op_id}"
            recorded = await journal.append(
                Kind.CHILD_SCHEDULED,
                op_id,
                {"name": child_name, "child_run_id": child_run_id, "args": serde.preview(args)},
            )
        child_run_id = recorded.payload["child_run_id"]

        def register_compensation() -> None:
            if compensate is not None:
                self._comp_stack.append(
                    _Compensation(
                        op_id=op_id,
                        name=_callable_name(compensate),
                        fn=compensate,
                        args=compensate_args,
                        kwargs=compensate_kwargs,
                        retry=compensate_retry,
                    )
                )

        completed = journal.find(Kind.CHILD_COMPLETED, op_id)
        if completed is not None:
            register_compensation()
            return completed.payload["result"]
        failed = journal.find(Kind.CHILD_FAILED, op_id)
        if failed is not None:
            error = failed.payload["error"]
            raise ChildFailed(child_name, child_run_id, error["type"], error["message"])

        # Live: start (or re-attach to) the child run and await it, staying
        # responsive to cooperative cancellation of the parent.
        handle = await self._runtime.start(wf, *args, run_id=child_run_id, **kwargs)
        result_task = asyncio.ensure_future(handle.result())
        cancel_task = asyncio.ensure_future(self._runtime._cancel_event(self.run_id).wait())
        try:
            await asyncio.wait({result_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            cancel_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancel_task
        if not result_task.done():
            result_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await result_task
            await self._runtime.cancel(child_run_id)  # propagate; child compensates itself
            raise asyncio.CancelledError()
        try:
            result = result_task.result()
        except (WorkflowFailed, WorkflowCancelled, CompensationFailed) as exc:
            await journal.append(
                Kind.CHILD_FAILED,
                op_id,
                {"name": child_name, "child_run_id": child_run_id, "error": serde.error_info(exc)},
            )
            raise ChildFailed(child_name, child_run_id, type(exc).__name__, str(exc)) from exc
        # NonDeterminismError (and the like) propagates raw: both runs stay resumable.
        await journal.append(
            Kind.CHILD_COMPLETED,
            op_id,
            {"name": child_name, "child_run_id": child_run_id, "result": result},
        )
        register_compensation()
        return result

    # -- durable timers ------------------------------------------------------

    def sleep(self, seconds: float) -> Awaitable[None]:
        """Durable sleep: the deadline is journaled, so a crash mid-sleep resumes
        with only the *remaining* time, and an already-elapsed timer fires instantly."""
        op_id = self._next_op()
        return self._sleep(op_id, float(seconds))

    async def _sleep(self, op_id: int, seconds: float) -> None:
        journal = self._journal
        recorded = self._expect(Kind.TIMER_CREATED, op_id)
        if recorded is None:
            self._check_cancel()
            fire_at = time.time() + max(seconds, 0.0)
            recorded = await journal.append(
                Kind.TIMER_CREATED, op_id, {"seconds": seconds, "fire_at": fire_at}
            )
        if journal.find(Kind.TIMER_FIRED, op_id) is not None:
            return
        remaining = recorded.payload["fire_at"] - time.time()
        if remaining > 0:
            await self._wait_or_cancel(remaining)
        await journal.append(Kind.TIMER_FIRED, op_id, {})

    # -- external events (signals) -------------------------------------------

    def wait_event(self, name: str, *, timeout: float | None = None) -> Awaitable[Any]:
        """Suspend until ``Runtime.signal(run_id, name, payload)`` delivers an event.

        Signals are buffered in the store, so one sent while the run is down is
        consumed on resume. Consumption is journaled: replay returns the same
        payload without waiting. With ``timeout``, raises :class:`WaitTimeout`
        at a journaled deadline.
        """
        op_id = self._next_op()
        return self._wait_event(op_id, name, timeout)

    async def _wait_event(self, op_id: int, name: str, timeout: float | None) -> Any:
        journal = self._journal
        recorded = self._expect(Kind.WAIT_CREATED, op_id, name=name)
        if recorded is None:
            self._check_cancel()
            deadline = time.time() + timeout if timeout is not None else None
            recorded = await journal.append(
                Kind.WAIT_CREATED, op_id, {"name": name, "deadline": deadline}
            )
        deadline = recorded.payload["deadline"]

        consumed = journal.find(Kind.EVENT_CONSUMED, op_id)
        if consumed is not None:
            return consumed.payload["payload"]
        if journal.find(Kind.WAIT_TIMED_OUT, op_id) is not None:
            raise WaitTimeout(name, op_id)

        while True:
            exclude = journal.consumed_signal_seqs | self._inflight_signal_seqs
            signal = await self._runtime._next_unconsumed_signal(self.run_id, name, exclude)
            if signal is not None:
                self._inflight_signal_seqs.add(signal.seq)
                try:
                    await journal.append(
                        Kind.EVENT_CONSUMED,
                        op_id,
                        {"name": name, "signal_seq": signal.seq, "payload": signal.payload},
                    )
                    await self._runtime._mark_signal_consumed(self.run_id, signal.seq)
                finally:
                    self._inflight_signal_seqs.discard(signal.seq)
                return signal.payload

            self._check_cancel()
            # In-process signals resolve the waiter instantly; the poll cap
            # exists so signals written to the store by *another* worker (which
            # cannot wake our future) are still picked up.
            wait_cap = self._runtime.signal_poll_interval
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    await journal.append(Kind.WAIT_TIMED_OUT, op_id, {"name": name})
                    raise WaitTimeout(name, op_id)
                wait_cap = min(wait_cap, remaining)
            waiter = self._runtime._register_waiter(self.run_id)
            try:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(waiter, wait_cap)
            finally:
                self._runtime._unregister_waiter(self.run_id, waiter)
            if deadline is not None and time.time() >= deadline:
                # Re-scan once before declaring the timeout, so a signal that
                # raced the deadline is not lost.
                exclude = journal.consumed_signal_seqs | self._inflight_signal_seqs
                if await self._runtime._next_unconsumed_signal(self.run_id, name, exclude) is None:
                    await journal.append(Kind.WAIT_TIMED_OUT, op_id, {"name": name})
                    raise WaitTimeout(name, op_id)

    # -- continue-as-new -----------------------------------------------------

    async def continue_as_new(self, *args: Any, **kwargs: Any) -> Any:
        """End this run and chain into a fresh run of the same workflow.

        The successor starts with a brand-new (empty) journal and the given
        arguments — this is how an agent that loops forever keeps its journal
        bounded: carry the state you need in ``args`` and continue every N
        iterations. The successor pins the *currently registered* workflow
        version, so a continue is also the natural upgrade point.

        ``handle.result()`` transparently follows the chain to the final
        outcome; ``signal()`` and ``cancel()`` on any run id in the chain are
        routed to the live run. Registered compensations do NOT carry over —
        a continued run counts as finished, like a successful return.

        This call never returns.
        """
        raise _ContinueAsNew(
            serde.roundtrip(list(args)) or [],
            serde.roundtrip(kwargs) or {},
        )

    # -- deterministic values ------------------------------------------------

    def now(self) -> Awaitable[float]:
        """Wall-clock time, journaled: replay sees the originally observed value."""
        op_id = self._next_op()
        return self._value(op_id, "now", lambda: time.time())

    def random(self) -> Awaitable[float]:
        """Random float in [0, 1), journaled for deterministic replay."""
        op_id = self._next_op()
        return self._value(op_id, "random", _random.random)

    def uuid4(self) -> Awaitable[str]:
        """Journaled UUID — ideal as an idempotency key for at-least-once steps."""
        op_id = self._next_op()
        return self._value(op_id, "uuid4", lambda: str(uuid.uuid4()))

    async def _value(self, op_id: int, subkind: str, produce: Callable[[], Any]) -> Any:
        recorded = self._expect(Kind.VALUE_RECORDED, op_id, name=subkind)
        if recorded is None:
            recorded = await self._journal.append(
                Kind.VALUE_RECORDED, op_id, {"name": subkind, "value": produce()}
            )
        return recorded.payload["value"]

    # -- concurrency ---------------------------------------------------------

    def gather(self, *awaitables: Awaitable[Any], return_exceptions: bool = False) -> Awaitable[list[Any]]:
        """Run context operations concurrently.

        Safe for replay because op_ids are assigned when ``ctx.step(...)`` etc.
        are *called* (synchronously, in code order), not when they are awaited.
        """
        return asyncio.gather(*awaitables, return_exceptions=return_exceptions)
