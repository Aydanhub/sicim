"""Store behaviour: durability, deletion, migrations, PostgreSQL reconnect."""

import dataclasses
import sqlite3

import pytest
from helpers import Gate, counting_step

from sicim import Event, Runtime, RunRecord, RunStatus, SQLiteStore, workflow


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


async def test_delete_run_history_keeps_the_record(store):
    await store.create_run(RunRecord(run_id="H1", workflow="w"))
    await store.append_event("H1", Event(seq=0, kind="run_started", op_id=-1, payload={}, ts=1.0))
    await store.append_signal("H1", "go", {"x": 1})

    await store.delete_run_history("H1")

    assert (await store.load_run("H1")) is not None
    assert await store.load_events("H1") == []
    assert await store.load_signals("H1") == []


async def test_replace_events_rewrites_the_journal(store):
    await store.create_run(RunRecord(run_id="ev", workflow="wf"))
    for seq in range(4):
        await store.append_event(
            "ev", Event(seq=seq, kind="step_completed", op_id=seq, payload={"i": seq}, ts=100.0 + seq)
        )
    assert [e.seq for e in await store.load_events("ev")] == [0, 1, 2, 3]

    kept = [e for e in await store.load_events("ev") if e.op_id < 2]
    await store.replace_events(
        "ev", [dataclasses.replace(e, seq=i) for i, e in enumerate(kept)]
    )
    events = await store.load_events("ev")
    assert [(e.seq, e.op_id) for e in events] == [(0, 0), (1, 1)]
    assert events[0].payload == {"i": 0}

    await store.replace_events("ev", [])
    assert await store.load_events("ev") == []


async def test_signals_can_be_marked_unconsumed_again(store):
    await store.create_run(RunRecord(run_id="sig", workflow="wf"))
    seq = await store.append_signal("sig", "go", {"n": 1})
    await store.mark_signal_consumed("sig", seq)
    assert [s.consumed for s in await store.load_signals("sig")] == [True]
    await store.mark_signal_consumed("sig", seq, False)
    [signal] = await store.load_signals("sig")
    assert signal.consumed is False and signal.payload == {"n": 1}


async def test_parent_run_id_roundtrips(store):
    await store.create_run(RunRecord(run_id="child", workflow="w", parent_run_id="papa"))
    loaded = await store.load_run("child")
    assert loaded.parent_run_id == "papa"
    assert (await store.list_runs())[0].parent_run_id == "papa"


async def test_sqlite_migrates_pre_04_schema(tmp_path):
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, workflow TEXT NOT NULL,"
        " version INTEGER NOT NULL DEFAULT 1, args TEXT NOT NULL, kwargs TEXT NOT NULL,"
        " status TEXT NOT NULL, result TEXT, error TEXT,"
        " cancel_requested INTEGER NOT NULL DEFAULT 0, continued_to TEXT,"
        " created_at REAL NOT NULL, updated_at REAL NOT NULL)"
    )
    conn.commit()
    conn.close()

    store = SQLiteStore(path)  # adds the parent_run_id column on open
    await store.create_run(RunRecord(run_id="m1", workflow="w", parent_run_id="papa"))
    assert (await store.load_run("m1")).parent_run_id == "papa"
    store.close()


async def test_postgres_reconnects_after_connection_loss(store):
    from sicim.pg import PostgresStore

    if not isinstance(store, PostgresStore):
        pytest.skip("postgres-only behaviour")

    await store._conn.close()  # simulate a dropped server connection
    assert await store.load_run("missing") is None  # read path reconnects

    await store._conn.close()
    seq = await store.append_signal("R", "go", {"x": 1})  # write path reconnects
    assert seq == 0
    assert [s.payload for s in await store.load_signals("R")] == [{"x": 1}]


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
