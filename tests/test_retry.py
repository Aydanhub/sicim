"""Retry policies, including attempt counts surviving a crash."""

import asyncio

import pytest
from helpers import wait_for_events

from sicim import Kind, NonRetryable, RetryPolicy, Runtime, StepFailed, workflow

FAST = RetryPolicy(max_attempts=5, initial_interval=0.01, jitter=0.0)


async def test_retry_until_success(rt):
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError(f"fail {calls['n']}")
        return "finally"

    @workflow(name="wf_retry_ok")
    async def wf(ctx):
        return await ctx.step(flaky, retry=FAST)

    handle = await rt.start(wf, run_id="retry1")
    assert await handle.result() == "finally"
    assert calls["n"] == 3
    events = await rt.events("retry1")
    assert sum(e.kind == Kind.STEP_ATTEMPT_FAILED for e in events) == 2


async def test_retry_exhausted_raises_step_failed(rt):
    calls = {"n": 0}

    async def hopeless():
        calls["n"] += 1
        raise ValueError("always")

    @workflow(name="wf_retry_exhausted")
    async def wf(ctx):
        try:
            await ctx.step(hopeless, retry=RetryPolicy(max_attempts=2, initial_interval=0.01))
        except StepFailed as exc:
            return {"attempts": exc.attempts, "type": exc.error_type}

    handle = await rt.start(wf, run_id="retry2")
    assert await handle.result() == {"attempts": 2, "type": "ValueError"}
    assert calls["n"] == 2


async def test_non_retryable_exception_fails_immediately(rt):
    calls = {"n": 0}

    async def fatal():
        calls["n"] += 1
        raise NonRetryable("bad request")

    @workflow(name="wf_retry_fatal")
    async def wf(ctx):
        try:
            await ctx.step(fatal, retry=FAST)
        except StepFailed as exc:
            return exc.attempts

    handle = await rt.start(wf, run_id="retry3")
    assert await handle.result() == 1
    assert calls["n"] == 1


async def test_non_retryable_exception_types_from_policy(rt):
    calls = {"n": 0}

    async def key_error():
        calls["n"] += 1
        raise KeyError("missing")

    @workflow(name="wf_retry_types")
    async def wf(ctx):
        try:
            await ctx.step(key_error, retry=RetryPolicy(max_attempts=5, non_retryable=(KeyError,)))
        except StepFailed as exc:
            return exc.attempts

    handle = await rt.start(wf, run_id="retry4")
    assert await handle.result() == 1
    assert calls["n"] == 1


async def test_attempt_count_survives_crash(store):
    class Flaky:
        def __init__(self):
            self.calls = 0
            self.reached = asyncio.Event()

        async def __call__(self):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("first attempt fails")
            if self.calls == 2:
                self.reached.set()
                await asyncio.Event().wait()  # crash point, mid attempt 2
            return "ok"

    flaky = Flaky()

    @workflow(name="wf_retry_crash")
    async def wf(ctx):
        return await ctx.step(flaky, name="flaky", retry=RetryPolicy(max_attempts=2, initial_interval=0.01))

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="retry5")
    await asyncio.wait_for(flaky.reached.wait(), timeout=5)
    await rt1.shutdown()

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    assert await handle.result() == "ok"
    # The failed first attempt was journaled, so the resumed execution was
    # attempt 2 of 2 — and its success recorded exactly that.
    events = await wait_for_events(store, "retry5", Kind.STEP_COMPLETED, 1)
    assert events[0].payload["attempts"] == 2
    assert flaky.calls == 3
    await rt2.shutdown()
