"""Run tags: pinned at start, inherited by children, searchable via list_runs."""

import sqlite3

import pytest
from helpers import Gate

from sicim import Kind, Runtime, RunRecord, RunStatus, SQLiteStore, WorkflowFailed, workflow


@workflow(name="wf_tags_echo")
async def echo_tags(ctx):
    return dict(ctx.tags)


def ids(runs):
    return [run.run_id for run in runs]


async def test_tags_are_pinned_and_searchable(rt):
    h1 = await rt.start(echo_tags, run_id="tg1", tags={"customer": "42", "env": "prod"})
    h2 = await rt.start(echo_tags, run_id="tg2", tags={"customer": "42", "env": "dev"})
    h3 = await rt.start(echo_tags, run_id="tg3")
    assert await h1.result() == {"customer": "42", "env": "prod"}  # visible in the body
    assert await h2.result() == {"customer": "42", "env": "dev"}
    assert await h3.result() == {}
    assert (await rt.status("tg1")).tags == {"customer": "42", "env": "prod"}

    assert ids(await rt.list_runs(tags={"customer": "42"})) == ["tg1", "tg2"]
    assert ids(await rt.list_runs(tags={"customer": "42", "env": "dev"})) == ["tg2"]  # all pairs
    assert ids(await rt.list_runs(tags={"customer": "99"})) == []
    assert ids(await rt.list_runs(workflow="wf_tags_echo")) == ["tg1", "tg2", "tg3"]
    assert ids(await rt.list_runs(workflow="nope")) == []
    assert ids(await rt.list_runs(RunStatus.COMPLETED, tags={"env": "prod"})) == ["tg1"]
    assert ids(await rt.list_runs(RunStatus.RUNNING)) == []
    assert ids(await rt.list_runs(limit=2, newest_first=True)) == ["tg3", "tg2"]
    assert ids(await rt.list_runs(limit=1)) == ["tg1"]


async def test_children_inherit_tags_unless_overridden(rt):
    @workflow(name="wf_tags_child")
    async def child(ctx):
        return dict(ctx.tags)

    @workflow(name="wf_tags_parent")
    async def parent(ctx):
        inherited = await ctx.child(child)
        overridden = await ctx.child(child, tags={"scope": "sub"})
        return [inherited, overridden]

    handle = await rt.start(parent, run_id="tp", tags={"customer": "7"})
    assert await handle.result() == [{"customer": "7"}, {"scope": "sub"}]
    assert (await rt.status("tp.c0")).tags == {"customer": "7"}
    assert (await rt.status("tp.c1")).tags == {"scope": "sub"}
    # The whole agent tree by tag; the children by parent.
    assert ids(await rt.list_runs(tags={"customer": "7"})) == ["tp", "tp.c0"]
    assert ids(await rt.list_runs(parent_run_id="tp")) == ["tp.c0", "tp.c1"]


async def test_continue_as_new_carries_tags(rt):
    @workflow(name="wf_tags_chain")
    async def wf(ctx, hops):
        if hops > 0:
            await ctx.continue_as_new(hops - 1)
        return dict(ctx.tags)

    handle = await rt.start(wf, 1, run_id="tc", tags={"agent": "loop"})
    assert await handle.result() == {"agent": "loop"}
    assert (await rt.status("tc#2")).tags == {"agent": "loop"}
    assert ids(await rt.list_runs(tags={"agent": "loop"})) == ["tc", "tc#2"]


async def test_invalid_tags_are_rejected_before_anything_is_created(rt):
    with pytest.raises(TypeError):
        await rt.start(echo_tags, run_id="bad1", tags={"n": 1})
    with pytest.raises(TypeError):
        await rt.start(echo_tags, run_id="bad2", tags="customer=42")
    with pytest.raises(TypeError):
        await rt.start(echo_tags, run_id="bad3", tags={"": "x"})
    assert await rt.list_runs() == []


async def test_run_started_event_carries_tags(rt):
    handle = await rt.start(echo_tags, run_id="te", tags={"k": "v"})
    await handle.result()
    [started] = [e for e in await rt.events("te") if e.kind == Kind.RUN_STARTED]
    assert started.payload["tags"] == {"k": "v"}


async def test_delete_run_removes_tags(store):
    await store.create_run(RunRecord(run_id="dt", workflow="w", tags={"k": "v"}))
    assert ids(await store.list_runs(tags={"k": "v"})) == ["dt"]
    await store.delete_run("dt")
    assert await store.list_runs(tags={"k": "v"}) == []
    # Re-creating the id must not trip over stale tag rows.
    await store.create_run(RunRecord(run_id="dt", workflow="w", tags={"k": "v2"}))
    assert (await store.load_run("dt")).tags == {"k": "v2"}


async def test_create_run_rejects_duplicate_ids(store):
    await store.create_run(RunRecord(run_id="dup", workflow="w"))
    with pytest.raises(Exception):
        await store.create_run(RunRecord(run_id="dup", workflow="w"))
    # The store stays usable afterwards (no half-open transaction).
    await store.create_run(RunRecord(run_id="dup2", workflow="w"))
    assert ids(await store.list_runs()) == ["dup", "dup2"]


async def test_sqlite_migrates_pre_05_schema(tmp_path):
    path = str(tmp_path / "legacy05.db")
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, workflow TEXT NOT NULL,"
        " version INTEGER NOT NULL DEFAULT 1, args TEXT NOT NULL, kwargs TEXT NOT NULL,"
        " status TEXT NOT NULL, result TEXT, error TEXT,"
        " cancel_requested INTEGER NOT NULL DEFAULT 0, continued_to TEXT, parent_run_id TEXT,"
        " created_at REAL NOT NULL, updated_at REAL NOT NULL)"
    )
    conn.execute(
        "INSERT INTO runs VALUES ('old', 'w', 1, '[]', '{}', 'completed', 'null', NULL, 0, NULL, NULL, 1.0, 2.0)"
    )
    conn.commit()
    conn.close()

    store = SQLiteStore(path)  # adds the tags column; creates run_tags and schedules
    assert (await store.load_run("old")).tags == {}
    await store.create_run(RunRecord(run_id="new", workflow="w", tags={"k": "v"}))
    assert ids(await store.list_runs(tags={"k": "v"})) == ["new"]
    assert ids(await store.list_runs()) == ["old", "new"]
    assert await store.list_schedules() == []
    store.close()


# -- tags after start ---------------------------------------------------------


async def test_tags_can_be_added_and_removed_after_start(rt):
    handle = await rt.start(echo_tags, run_id="ta", tags={"customer": "42", "env": "prod"})
    assert await handle.result() == {"customer": "42", "env": "prod"}

    assert await rt.tag("ta", {"stage": "review", "env": None}) == {"customer": "42", "stage": "review"}
    assert (await rt.status("ta")).tags == {"customer": "42", "stage": "review"}
    assert ids(await rt.list_runs(tags={"stage": "review"})) == ["ta"]
    assert ids(await rt.list_runs(tags={"env": "prod"})) == []  # index follows the removal
    assert await rt.tag("ta", {"stage": "done"}) == {"customer": "42", "stage": "done"}

    with pytest.raises(ValueError):
        await rt.tag("ta", {"sicim.schedule": "x"})  # reserved
    with pytest.raises(TypeError):
        await rt.tag("ta", {"n": 1})
    from sicim import RunNotFound

    with pytest.raises(RunNotFound):
        await rt.tag("missing", {"k": "v"})


async def test_reserved_tag_keys_are_rejected_everywhere(rt):
    @workflow(name="wf_tags_reserved_parent")
    async def parent(ctx):
        return await ctx.child(echo_tags, tags={"sicim.x": "1"})

    with pytest.raises(ValueError):
        await rt.start(echo_tags, run_id="rs1", tags={"sicim.schedule": "fake"})
    with pytest.raises(ValueError):
        await rt.schedule(echo_tags, schedule_id="rs2", every=60, tags={"sicim.owner": "me"})
    with pytest.raises(WorkflowFailed) as excinfo:
        await (await rt.start(parent, run_id="rs3")).result()
    assert excinfo.value.error_type == "ValueError"
    assert await rt.list_runs(parent_run_id="rs3") == []  # the child was never created


async def test_ctx_tag_is_journaled_and_replayed_not_re_executed(store):
    gate = Gate(hang_on={1})

    @workflow(name="wf_tags_ctx")
    async def wf(ctx):
        await ctx.tag({"stage": "plan"})
        await ctx.step(gate, name="gate")
        await ctx.tag({"stage": "done", "extra": "yes"})
        await ctx.tag({"extra": None})
        return dict(ctx.tags)

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="tc1", tags={"customer": "7"})
    await gate.wait_reached()
    assert (await rt1.status("tc1")).tags == {"customer": "7", "stage": "plan"}
    await rt1.shutdown()  # crash while parked in the step

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    assert await handle.result() == {"customer": "7", "stage": "done"}
    assert (await rt2.status("tc1")).tags == {"customer": "7", "stage": "done"}
    updates = [e for e in await rt2.events("tc1") if e.kind == Kind.TAGS_UPDATED]
    assert [e.payload["tags"] for e in updates] == [
        {"stage": "plan"}, {"stage": "done", "extra": "yes"}, {"extra": None}
    ]  # the first update was replayed, not journaled twice
    assert ids(await rt2.list_runs(tags={"stage": "done"})) == ["tc1"]
    await rt2.shutdown()


async def test_external_tags_stay_invisible_to_the_body_but_carry_to_successors(store):
    gate = Gate(hang_on={1})

    @workflow(name="wf_tags_external")
    async def wf(ctx, hops):
        await ctx.step(gate, name="gate")
        if hops > 0:
            await ctx.continue_as_new(hops - 1)
        return dict(ctx.tags)

    rt1 = Runtime(store)
    await rt1.start(wf, 1, run_id="te1", tags={"a": "1"})
    await gate.wait_reached()
    assert await rt1.tag("te1", {"ext": "x"}) == {"a": "1", "ext": "x"}
    await rt1.shutdown()

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    # The successor pins the tags as they are *now* (external edit included);
    # the first run's body itself only ever saw {"a": "1"}.
    assert await handle.result() == {"a": "1", "ext": "x"}
    assert (await rt2.status("te1#2")).tags == {"a": "1", "ext": "x"}
    [started] = [e for e in await rt2.events("te1") if e.kind == Kind.RUN_STARTED]
    assert started.payload["tags"] == {"a": "1"}
    # Tagging by the old id follows the chain to the live end.
    assert await rt2.tag("te1", {"late": "yes"}) == {"a": "1", "ext": "x", "late": "yes"}
    assert (await rt2.status("te1#2")).tags["late"] == "yes"
    assert "late" not in (await rt2.status("te1")).tags
    await rt2.shutdown()


async def test_store_update_tags_counts_and_workflows(store):
    await store.create_run(RunRecord(run_id="u1", workflow="wa", tags={"k": "v", "old": "1"}))
    await store.create_run(RunRecord(run_id="u2", workflow="wb", status=RunStatus.FAILED))
    await store.create_run(RunRecord(run_id="u3", workflow="wa", status=RunStatus.COMPLETED))

    assert await store.update_tags("u1", {"k": "v2", "old": None, "new": "n"}) == {"k": "v2", "new": "n"}
    assert (await store.load_run("u1")).tags == {"k": "v2", "new": "n"}
    assert ids(await store.list_runs(tags={"k": "v2", "new": "n"})) == ["u1"]
    assert await store.list_runs(tags={"old": "1"}) == []
    assert await store.update_tags("u1", {"k": None, "new": None}) == {}
    assert await store.list_runs(tags={"k": "v2"}) == []
    assert await store.update_tags("missing", {"k": "v"}) == {}

    assert await store.count_runs() == {"running": 1, "failed": 1, "completed": 1}
    assert await store.list_workflows() == ["wa", "wb"]
    await store.delete_run("u1")
    await store.delete_run("u2")
    assert await store.count_runs() == {"completed": 1}
    assert await store.list_workflows() == ["wa"]
