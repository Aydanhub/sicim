"""Schedules: interval, one-shot and cron starts; exactly-once ticks across workers."""

import asyncio
import datetime as dt
import time

import pytest

from sicim import Runtime, RunStatus, ScheduleNotFound, ScheduleRecord, SicimError, workflow
from sicim.schedule import scheduled_run_id


async def work(label):
    return label


@workflow(name="wf_sched_quick")
async def quick(ctx, label="tick"):
    return await ctx.step(work, label, name="work")


async def scheduled_runs(rt, schedule_id):
    return await rt.list_runs(tags={"sicim.schedule": schedule_id})


async def wait_for_runs(rt, schedule_id, n, timeout=5.0):
    deadline = time.time() + timeout
    while True:
        runs = await scheduled_runs(rt, schedule_id)
        if len(runs) >= n:
            return runs
        if time.time() > deadline:
            raise AssertionError(f"timed out waiting for {n} scheduled run(s) (have {len(runs)})")
        await asyncio.sleep(0.01)


async def wait_until_terminal(rt, runs, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        records = [await rt.status(run.run_id) for run in runs]
        if all(record.status.terminal for record in records):
            return records
        await asyncio.sleep(0.01)
    raise AssertionError("scheduled runs did not finish in time")


async def test_interval_schedule_starts_tagged_runs(store):
    rt = Runtime(store, schedule_poll_interval=0.02)
    before = time.time()
    sched = await rt.schedule(quick, "hello", schedule_id="ticker", every=0.1, tags={"team": "ops"})
    assert sched.spec == "every 0.1"
    assert before + 0.1 <= sched.next_fire_at <= time.time() + 0.1  # first tick one interval out

    runs = await wait_for_runs(rt, "ticker", 2)
    for run in runs:
        assert run.run_id.startswith("ticker@")
        assert run.args == ["hello"]
        assert run.tags == {"team": "ops", "sicim.schedule": "ticker"}
    results = [await (await rt.resume(run.run_id)).result() for run in runs]
    assert results == ["hello", "hello"]
    latest = await rt.get_schedule("ticker")
    assert latest.last_run_id in {run.run_id for run in await scheduled_runs(rt, "ticker")}

    await rt.unschedule("ticker")
    with pytest.raises(ScheduleNotFound):
        await rt.get_schedule("ticker")
    await asyncio.sleep(0.1)  # let a tick already in flight land
    frozen = len(await scheduled_runs(rt, "ticker"))
    await asyncio.sleep(0.3)
    assert len(await scheduled_runs(rt, "ticker")) == frozen  # nothing new after unschedule
    await rt.shutdown()


async def test_overlap_skip_waits_for_the_previous_run(store):
    release = asyncio.Event()
    started = {"n": 0}

    async def blocking():
        started["n"] += 1
        await release.wait()
        return "done"

    @workflow(name="wf_sched_block")
    async def wf(ctx):
        return await ctx.step(blocking, name="block")

    rt = Runtime(store, schedule_poll_interval=0.02)
    await rt.schedule(wf, schedule_id="one-at-a-time", every=0.05)
    [first] = await wait_for_runs(rt, "one-at-a-time", 1)
    await asyncio.sleep(0.3)  # several ticks pass while the first run is still running
    assert len(await scheduled_runs(rt, "one-at-a-time")) == 1
    assert started["n"] == 1
    sched = await rt.get_schedule("one-at-a-time")
    assert sched.last_run_id == first.run_id
    assert sched.next_fire_at >= time.time() - 0.2  # skipped ticks were advanced, not left due

    release.set()  # the run finishes; the next tick starts a fresh run
    await wait_for_runs(rt, "one-at-a-time", 2)
    await rt.shutdown()


async def test_overlap_allow_stacks_runs(store):
    release = asyncio.Event()

    async def blocking():
        await release.wait()
        return "done"

    @workflow(name="wf_sched_stack")
    async def wf(ctx):
        return await ctx.step(blocking, name="block")

    rt = Runtime(store, schedule_poll_interval=0.02)
    await rt.schedule(wf, schedule_id="stack", every=0.05, overlap="allow")
    runs = await wait_for_runs(rt, "stack", 3)
    assert all(run.status is RunStatus.RUNNING for run in runs)
    await rt.unschedule("stack")
    release.set()
    for run in runs:
        assert await (await rt.resume(run.run_id)).result() == "done"
    await rt.shutdown()


async def test_one_shot_schedule_fires_exactly_once(store):
    rt = Runtime(store, schedule_poll_interval=0.02)
    fire_at = time.time() + 0.1
    sched = await rt.schedule(quick, schedule_id="once", at=fire_at)
    assert sched.next_fire_at == fire_at

    [run] = await wait_for_runs(rt, "once", 1)
    assert run.run_id == scheduled_run_id("once", fire_at)
    await asyncio.sleep(0.2)
    assert len(await scheduled_runs(rt, "once")) == 1
    done = await rt.get_schedule("once")
    assert done.next_fire_at is None
    assert done.last_run_id == run.run_id
    await rt.shutdown()


async def test_past_one_shot_fires_on_the_first_tick(store):
    rt = Runtime(store, schedule_poll_interval=0.02)
    await rt.schedule(quick, schedule_id="late", at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1))
    [run] = await wait_for_runs(rt, "late", 1)
    assert run.run_id.startswith("late@")
    assert (await rt.get_schedule("late")).next_fire_at is None
    await rt.shutdown()


async def test_pause_and_resume(store):
    rt = Runtime(store, schedule_poll_interval=0.02)
    await rt.schedule(quick, schedule_id="pausable", every=0.05)
    await wait_for_runs(rt, "pausable", 1)

    await rt.pause_schedule("pausable")
    assert (await rt.get_schedule("pausable")).paused is True
    await asyncio.sleep(0.1)  # let a tick already in flight land
    frozen = len(await scheduled_runs(rt, "pausable"))
    await asyncio.sleep(0.3)
    assert len(await scheduled_runs(rt, "pausable")) == frozen

    await rt.resume_schedule("pausable")
    sched = await rt.get_schedule("pausable")
    assert sched.paused is False
    assert sched.next_fire_at > sched.updated_at  # resumes from now, not from the missed ticks
    await wait_for_runs(rt, "pausable", frozen + 1)
    await rt.shutdown()


async def test_two_workers_share_ticks_without_duplicates(store):
    executions = {"n": 0}

    async def counted():
        executions["n"] += 1
        return "ok"

    @workflow(name="wf_sched_dual")
    async def wf(ctx):
        return await ctx.step(counted, name="work")

    rt1 = Runtime(store, worker_id="w1", schedule_poll_interval=0.01)
    rt2 = Runtime(store, worker_id="w2", schedule_poll_interval=0.01)
    started_at = time.time()
    await rt1.schedule(wf, schedule_id="dual", every=0.05, overlap="allow")
    await rt2.recover()  # w2 starts its scheduler too: both workers race for every tick
    await wait_for_runs(rt1, "dual", 4)
    await rt1.unschedule("dual")
    await asyncio.sleep(0.1)
    elapsed = time.time() - started_at

    runs = await scheduled_runs(rt1, "dual")
    # One run per tick, never two: duplicates would show up as ~2x the tick count.
    assert 4 <= len(runs) <= elapsed / 0.05 + 1
    assert len({run.run_id for run in runs}) == len(runs)
    records = await wait_until_terminal(rt1, runs)
    assert all(record.status is RunStatus.COMPLETED for record in records)
    assert executions["n"] == len(runs)  # each run's body ran exactly once
    await rt1.shutdown()
    await rt2.shutdown()


async def test_missed_ticks_collapse_into_one_catch_up_run(store):
    rt = Runtime(store, schedule_poll_interval=0.02)
    await rt.schedule(quick, schedule_id="catchup", every=100.0)
    # Simulate a long outage: the next tick is far in the past (many intervals missed).
    assert await store.update_schedule("catchup", next_fire_at=time.time() - 1000.0)
    await wait_for_runs(rt, "catchup", 1)
    await asyncio.sleep(0.2)
    assert len(await scheduled_runs(rt, "catchup")) == 1
    sched = await rt.get_schedule("catchup")
    assert sched.next_fire_at > time.time() + 90  # rescheduled from now, not from the missed slot
    await rt.shutdown()


async def test_schedule_is_idempotent_and_validates(rt):
    first = await rt.schedule(quick, schedule_id="idem", every=1000)
    again = await rt.schedule(quick, "other-args", schedule_id="idem", every=5)  # existing wins
    assert again.spec == first.spec == "every 1000.0"
    assert again.args == []

    @workflow(name="wf_sched_other")
    async def other(ctx):
        return 1

    with pytest.raises(SicimError):
        await rt.schedule(other, schedule_id="idem", every=1)
    with pytest.raises(ValueError):
        await rt.schedule(quick, schedule_id="bad", every=1, cron="* * * * *")
    with pytest.raises(ValueError):
        await rt.schedule(quick, schedule_id="bad2", every=1, overlap="maybe")
    with pytest.raises(TypeError):
        await rt.schedule(quick, schedule_id="bad3", every=1, tags={"n": 1})
    with pytest.raises(ScheduleNotFound):
        await rt.unschedule("nope")
    with pytest.raises(ScheduleNotFound):
        await rt.pause_schedule("nope")
    assert [s.schedule_id for s in await rt.list_schedules()] == ["idem"]


async def test_cron_schedule_targets_the_next_matching_minute(rt):
    sched = await rt.schedule(quick, schedule_id="cron5", cron="*/5 * * * *", tz="Europe/Istanbul")
    assert sched.spec == "cron */5 * * * *"
    assert sched.tz == "Europe/Istanbul"
    fire = dt.datetime.fromtimestamp(sched.next_fire_at, dt.timezone.utc)
    assert fire.second == 0 and fire.minute % 5 == 0
    assert 0 < sched.next_fire_at - time.time() <= 300
    assert (await rt.get_schedule("cron5")).next_fire_at == sched.next_fire_at


async def test_scheduler_can_be_disabled(store):
    rt = Runtime(store, scheduler=False, schedule_poll_interval=0.02)
    await rt.schedule(quick, schedule_id="noop", every=0.05)
    await asyncio.sleep(0.3)
    assert await scheduled_runs(rt, "noop") == []
    assert rt._scheduler_task is None
    await rt.shutdown()


async def test_unregistered_workflow_leaves_the_tick_due(store, caplog):
    rt = Runtime(store, schedule_poll_interval=0.02)
    due = time.time() - 1.0
    await store.create_schedule(
        ScheduleRecord(schedule_id="ghost", workflow="wf_not_loaded_here", spec="every 60.0", next_fire_at=due)
    )
    await rt.recover()  # a worker without the code: starts its scheduler, sees the tick
    await asyncio.sleep(0.15)
    sched = await rt.get_schedule("ghost")
    assert sched.next_fire_at == due  # left for a worker that has the code
    assert sched.last_run_id is None
    assert await scheduled_runs(rt, "ghost") == []
    assert sum("not registered" in record.message for record in caplog.records) == 1  # warned once
    await rt.shutdown()


async def test_schedule_store_roundtrip_and_compare_and_set(store):
    record = ScheduleRecord(
        schedule_id="s1", workflow="w", spec="every 5.0", args=[1], kwargs={"a": "b"},
        tags={"k": "v"}, overlap="allow", next_fire_at=100.0,
    )
    await store.create_schedule(record)
    with pytest.raises(Exception):
        await store.create_schedule(record)
    loaded = await store.load_schedule("s1")
    assert (loaded.workflow, loaded.spec, loaded.args, loaded.kwargs) == ("w", "every 5.0", [1], {"a": "b"})
    assert (loaded.tz, loaded.tags, loaded.overlap, loaded.paused) == (None, {"k": "v"}, "allow", False)
    assert (loaded.next_fire_at, loaded.last_run_id) == (100.0, None)
    assert [s.schedule_id for s in await store.list_schedules()] == ["s1"]
    assert await store.list_due_schedules(99.0) == []
    assert [s.schedule_id for s in await store.list_due_schedules(100.0)] == ["s1"]

    # Compare-and-set: only the expected fire time advances the schedule.
    assert not await store.update_schedule("s1", expected_next_fire_at=99.0, next_fire_at=200.0)
    assert await store.update_schedule("s1", expected_next_fire_at=100.0, next_fire_at=200.0, last_run_id="s1@x")
    loaded = await store.load_schedule("s1")
    assert (loaded.next_fire_at, loaded.last_run_id) == (200.0, "s1@x")
    assert await store.update_schedule("s1", paused=True)
    assert await store.list_due_schedules(1e12) == []  # paused schedules are never due
    assert await store.update_schedule("s1", expected_next_fire_at=200.0, next_fire_at=None)
    assert (await store.load_schedule("s1")).next_fire_at is None
    assert await store.update_schedule("s1", expected_next_fire_at=None, next_fire_at=300.0)
    assert not await store.update_schedule("missing", paused=True)
    await store.delete_schedule("s1")
    assert await store.load_schedule("s1") is None
