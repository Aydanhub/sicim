"""Retry policies, including attempt counts surviving a crash."""

import asyncio
import time

import pytest
from helpers import wait_for_events

from sicim import Kind, NonRetryable, RetryPolicy, Runtime, StepFailed, WorkflowFailed, workflow

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


async def test_backoff_wait_survives_crash(store):
    """A crash in the middle of a retry backoff resumes with only the remaining wait."""
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("first attempt fails")
        return "ok"

    @workflow(name="wf_backoff_crash")
    async def wf(ctx):
        return await ctx.step(
            flaky, name="flaky", retry=RetryPolicy(max_attempts=3, initial_interval=1.0, jitter=0.0)
        )

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="bo1")
    [failed] = await wait_for_events(store, "bo1", Kind.STEP_ATTEMPT_FAILED, 1)
    retry_at = failed.payload["retry_at"]
    assert retry_at >= failed.ts + 0.9
    await asyncio.sleep(0.4)
    await rt1.shutdown()  # crash mid-backoff
    assert calls["n"] == 1

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    assert await handle.result() == "ok"
    finished = time.time()
    # The retry happened at (about) the journaled retry time: the resume waited
    # only the remaining ~0.6s instead of retrying at once or waiting 1.0s again.
    assert finished >= retry_at
    assert finished <= retry_at + 0.4
    assert calls["n"] == 2
    await rt2.shutdown()


async def test_elapsed_backoff_retries_immediately_on_resume(store):
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("first attempt fails")
        return "ok"

    @workflow(name="wf_backoff_elapsed")
    async def wf(ctx):
        return await ctx.step(
            flaky, name="flaky", retry=RetryPolicy(max_attempts=3, initial_interval=0.3, jitter=0.0)
        )

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="bo2")
    await wait_for_events(store, "bo2", Kind.STEP_ATTEMPT_FAILED, 1)
    await rt1.shutdown()
    await asyncio.sleep(0.4)  # the retry time passes while "down"

    rt2 = Runtime(store)
    started = time.time()
    [handle] = await rt2.recover()
    assert await handle.result() == "ok"
    assert time.time() - started < 0.2  # no re-wait
    assert calls["n"] == 2
    await rt2.shutdown()


async def test_compensation_backoff_is_journaled_and_honoured(rt):
    calls = {"n": 0}

    async def ok():
        return "ok"

    async def comp_flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("compensation hiccup")

    async def boom():
        raise NonRetryable("fail the run")

    @workflow(name="wf_comp_backoff")
    async def wf(ctx):
        await ctx.step(
            ok, name="s1", compensate=comp_flaky,
            compensate_retry=RetryPolicy(max_attempts=2, initial_interval=0.05, jitter=0.0),
        )
        await ctx.step(boom)

    handle = await rt.start(wf, run_id="bo3")
    with pytest.raises(WorkflowFailed):
        await handle.result()
    events = await rt.events("bo3")
    [attempt] = [e for e in events if e.kind == Kind.COMP_ATTEMPT_FAILED]
    [done] = [e for e in events if e.kind == Kind.COMP_COMPLETED]
    assert attempt.payload["retry_at"] >= attempt.ts + 0.04
    assert done.ts >= attempt.payload["retry_at"] - 0.01  # the wait was honoured
    assert calls["n"] == 2
