"""SQLite durability across store instances (real process-restart semantics)."""

from helpers import Gate, counting_step

from sicim import Runtime, RunStatus, SQLiteStore, workflow


async def test_sqlite_survives_store_reopen(tmp_path):
    path = str(tmp_path / "restart.db")
    counters = {}
    gate = Gate()

    @workflow(name="wf_sqlite_restart")
    async def wf(ctx):
        a = await ctx.step(counting_step(counters, "a"))
        g = await ctx.step(gate, name="gate")
        return [a, g]

    store1 = SQLiteStore(path)
    rt1 = Runtime(store1)
    await rt1.start(wf, run_id="p1")
    await gate.wait_reached()
    await rt1.shutdown()
    store1.close()  # everything below sees only what hit the disk

    store2 = SQLiteStore(path)
    rt2 = Runtime(store2)
    [handle] = await rt2.recover()
    assert await handle.result() == ["a", "gate-2"]
    assert counters["a"] == 1

    record = await store2.load_run("p1")
    assert record.status is RunStatus.COMPLETED
    assert record.result == ["a", "gate-2"]
    events = await store2.load_events("p1")
    assert events[0].kind == "run_started"
    assert events[-1].kind == "run_completed"
    await rt2.shutdown()
    store2.close()


async def test_delete_run_removes_everything(store):
    gate = Gate(hang_on=set())  # never hangs; just a friendly step

    @workflow(name="wf_delete_run")
    async def wf(ctx):
        await ctx.step(gate, name="gate")
        return await ctx.wait_event("go")

    rt = Runtime(store)
    await rt.start(wf, run_id="del1")
    await rt.signal("del1", "go", {"x": 1})
    handle = await rt.resume("del1")
    assert await handle.result() == {"x": 1}
    await rt.shutdown()

    await store.delete_run("del1")
    assert await store.load_run("del1") is None
    assert await store.load_events("del1") == []
    assert await store.load_signals("del1") == []
    assert await store.load_lease("del1") is None


async def test_sqlite_signal_roundtrip(tmp_path):
    path = str(tmp_path / "signals.db")
    store = SQLiteStore(path)
    seq0 = await store.append_signal("r", "approval", {"by": "x"})
    seq1 = await store.append_signal("r", "approval", None)
    assert (seq0, seq1) == (0, 1)
    await store.mark_signal_consumed("r", 0)
    signals = await store.load_signals("r")
    assert [s.consumed for s in signals] == [True, False]
    assert signals[0].payload == {"by": "x"}
    assert signals[1].payload is None
    store.close()
