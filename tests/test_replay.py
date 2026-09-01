"""Deterministic replay and crash-resume."""

import pytest
from helpers import Gate, counting_step, wait_for_events

from sicim import Kind, RetryPolicy, Runtime, RunStatus, SerializationError, StepFailed, workflow


async def test_crash_resume_replays_completed_steps(store):
    counters = {}
    gate = Gate()

    @workflow(name="wf_replay_crash")
    async def wf(ctx):
        a = await ctx.step(counting_step(counters, "a"))
        g = await ctx.step(gate, name="gate")
        b = await ctx.step(counting_step(counters, "b"))
        return [a, g, b]

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="r1")
    await gate.wait_reached()
    await rt1.shutdown()  # crash-equivalent: run must stay RUNNING

    assert counters["a"] == 1
    assert (await rt1.status("r1")).status is RunStatus.RUNNING

    rt2 = Runtime(store)
    handles = await rt2.recover()
    assert [h.run_id for h in handles] == ["r1"]
    result = await handles[0].result()

    assert result == ["a", "gate-2", "b"]
    assert counters["a"] == 1  # replayed from journal, not re-executed
    assert counters["b"] == 1
    assert gate.calls == 2  # the in-flight step re-runs (at-least-once)

    record = await rt2.status("r1")
    assert record.status is RunStatus.COMPLETED
    assert record.result == ["a", "gate-2", "b"]
    await rt2.shutdown()


async def test_start_is_idempotent_on_run_id(rt):
    counters = {}

    @workflow(name="wf_idem")
    async def wf(ctx, x):
        counters["body"] = counters.get("body", 0) + 1
        doubled = await ctx.step(counting_step(counters, "double", result=x * 2))
        return doubled

    h1 = await rt.start(wf, 5, run_id="same")
    r1 = await h1.result()
    h2 = await rt.start(wf, 99, run_id="same")  # new args ignored: stored inputs win
    r2 = await h2.result()

    assert r1 == r2 == 10
    assert counters["body"] == 1
    assert counters["double"] == 1


async def test_gather_assigns_stable_op_ids(store):
    counters = {}
    gate = Gate()

    @workflow(name="wf_gather")
    async def wf(ctx):
        results = await ctx.gather(
            ctx.step(counting_step(counters, "left")),
            ctx.step(gate, name="middle"),
            ctx.step(counting_step(counters, "right")),
        )
        return results

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="g1")
    await gate.wait_reached()
    # Let the two fast branches journal their completions before "crashing".
    await wait_for_events(store, "g1", Kind.STEP_COMPLETED, 2)
    await rt1.shutdown()

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    assert await handle.result() == ["left", "gate-2", "right"]
    assert counters["left"] == 1
    assert counters["right"] == 1
    assert gate.calls == 2
    await rt2.shutdown()


async def test_step_failure_is_replayed_not_retried(store):
    """A journaled permanent step failure must replay as the same StepFailed."""
    counters = {}
    gate = Gate()

    @workflow(name="wf_failed_replay")
    async def wf(ctx):
        try:
            await ctx.step(always_boom, retry=None)
        except StepFailed as exc:
            marker = [exc.error_type, exc.attempts]
        await ctx.step(gate, name="gate")
        return marker

    async def always_boom():
        counters["boom"] = counters.get("boom", 0) + 1
        raise ValueError("nope")

    rt1 = Runtime(store, default_retry=RetryPolicy(max_attempts=2, initial_interval=0.01))
    await rt1.start(wf, run_id="f1")
    await gate.wait_reached()
    await rt1.shutdown()
    boom_calls = counters["boom"]
    assert boom_calls == 2  # both attempts burned before the crash

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    assert await handle.result() == ["ValueError", 2]
    assert counters["boom"] == boom_calls  # failure replayed from journal
    await rt2.shutdown()


async def test_unserializable_step_result_fails_without_retry(rt):
    counters = {}

    @workflow(name="wf_serde")
    async def wf(ctx):
        await ctx.step(bad_step)

    async def bad_step():
        counters["n"] = counters.get("n", 0) + 1
        return object()  # not JSON-serializable

    handle = await rt.start(wf, run_id="s1")
    with pytest.raises(Exception) as excinfo:
        await handle.result()
    assert "SerializationError" in str(excinfo.value)
    assert counters["n"] == 1  # serialization bugs must not burn retries


async def test_unserializable_workflow_input_rejected_upfront(rt):
    @workflow(name="wf_badinput")
    async def wf(ctx, x):
        return x

    with pytest.raises(SerializationError):
        await rt.start(wf, object(), run_id="bad-input")
