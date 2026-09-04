"""The web monitoring UI: its JSON API on every backend, actions through the runtime."""

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request

import pytest
from helpers import Gate, wait_for_events

from sicim import Kind, Runtime, RunRecord, RunStatus, WorkflowCancelled, workflow
from sicim.ui import start_ui


async def call(url, method="GET", body=None):
    """One HTTP request (urllib blocks, so it runs in a thread) -> (status, decoded body)."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )

    def go():
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.headers.get("Content-Type", ""), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.headers.get("Content-Type", ""), exc.read()

    status, content_type, raw = await asyncio.to_thread(go)
    return status, (json.loads(raw) if content_type.startswith("application/json") else raw)


def quote(run_id):
    return urllib.parse.quote(run_id, safe="")


async def produce(value):
    return value


@workflow(name="wf_ui_child")
async def child(ctx, value):
    return await ctx.step(produce, value, name="produce")


@workflow(name="wf_ui_parent")
async def parent(ctx, value):
    await ctx.tag({"stage": "fan-out"})
    inner = await ctx.child(child, value)
    return await ctx.step(produce, [inner, "done"], name="finish")


@workflow(name="wf_ui_wait")
async def waiter(ctx):
    return await ctx.wait_event("go")


async def test_page_summary_and_error_shapes(store):
    async with await start_ui(store, port=0) as server:
        assert server.url.startswith("http://127.0.0.1:")
        status, page = await call(server.url + "/")
        assert status == 200 and b"<title>sicim</title>" in page and b"/api/runs" in page

        status, summary = await call(server.url + "/api/summary")
        assert status == 200
        assert summary["counts"] == {} and summary["workflows"] == [] and summary["schedules"] == 0
        assert summary["store"] == type(store).__name__ and "version" in summary

        assert (await call(server.url + "/nope"))[0] == 404
        assert (await call(server.url + "/api/runs/missing"))[0] == 404
        status, body = await call(server.url + "/api/summary", "POST")
        assert status == 405 and "GET" in body["error"]
        assert (await call(server.url + "/api/runs?status=bogus"))[0] == 400
        assert (await call(server.url + "/api/runs?tag=novalue"))[0] == 400
        assert (await call(server.url + "/api/runs?limit=x"))[0] == 400


async def test_run_list_filters_and_detail(rt):
    handle = await rt.start(parent, "v", run_id="ui/odd#1", tags={"customer": "42", "env": "prod"})
    assert await handle.result() == ["v", "done"]
    await (await rt.start(child, "w", run_id="ui-plain", tags={"customer": "43"})).result()

    async with await start_ui(rt, port=0) as server:
        status, summary = await call(server.url + "/api/summary")
        assert summary["counts"] == {"completed": 3}
        assert summary["workflows"] == ["wf_ui_child", "wf_ui_parent"]

        status, data = await call(server.url + "/api/runs")
        assert status == 200
        assert [run["run_id"] for run in data["runs"]] == ["ui-plain", "ui/odd#1.c1", "ui/odd#1"]  # newest first
        _, data = await call(server.url + "/api/runs?order=oldest&limit=2")
        assert [run["run_id"] for run in data["runs"]] == ["ui/odd#1", "ui/odd#1.c1"]
        _, data = await call(server.url + "/api/runs?tag=customer%3D42&tag=stage%3Dfan-out")
        # the child inherited the parent's ctx.tag() update along with its start tags
        assert [run["run_id"] for run in data["runs"]] == ["ui/odd#1.c1", "ui/odd#1"]
        _, data = await call(server.url + "/api/runs?tag=customer%3D42&workflow=wf_ui_parent")
        assert [run["run_id"] for run in data["runs"]] == ["ui/odd#1"]
        _, data = await call(server.url + "/api/runs?workflow=wf_ui_child&status=completed")
        assert {run["run_id"] for run in data["runs"]} == {"ui-plain", "ui/odd#1.c1"}
        _, data = await call(server.url + f"/api/runs?parent={quote('ui/odd#1')}")
        assert [run["run_id"] for run in data["runs"]] == ["ui/odd#1.c1"]
        _, data = await call(server.url + "/api/runs?status=running")
        assert data["runs"] == []

        status, detail = await call(server.url + "/api/runs/" + quote("ui/odd#1"))
        assert status == 200
        run = detail["run"]
        assert run["status"] == "completed" and run["result"] == ["v", "done"]
        assert run["tags"] == {"customer": "42", "env": "prod", "stage": "fan-out"}
        kinds = [event["kind"] for event in detail["events"]]
        assert kinds[0] == Kind.RUN_STARTED and kinds[-1] == Kind.RUN_COMPLETED
        assert Kind.TAGS_UPDATED in kinds and Kind.CHILD_COMPLETED in kinds
        assert [c["run_id"] for c in detail["children"]] == ["ui/odd#1.c1"]
        assert detail["children"][0]["parent_run_id"] == "ui/odd#1"
        assert detail["lease"] is None and detail["signals"] == []
        assert isinstance(detail["now"], float)


async def test_signal_cancel_and_tags_through_the_api(store):
    rt = Runtime(store, signal_poll_interval=0.05)
    first = await rt.start(waiter, run_id="ui-sig")
    second = await rt.start(waiter, run_id="ui-cancel")
    await wait_for_events(store, "ui-sig", Kind.WAIT_CREATED, 1)
    await wait_for_events(store, "ui-cancel", Kind.WAIT_CREATED, 1)

    async with await start_ui(rt, port=0) as server:
        status, detail = await call(server.url + "/api/runs/ui-sig")
        assert detail["lease"]["owner"] == rt.worker_id  # driven by this worker

        status, body = await call(server.url + "/api/runs/ui-sig/signal", "POST", {"name": "", "payload": 1})
        assert status == 400
        status, body = await call(server.url + "/api/runs/ui-sig/signal", "POST", {"name": "go", "payload": {"ok": True}})
        assert status == 200 and body == {"ok": True}
        assert await asyncio.wait_for(first.result(), 3) == {"ok": True}
        status, body = await call(server.url + "/api/runs/ui-sig/signal", "POST", {"name": "go"})
        assert status == 409  # terminal run

        status, body = await call(server.url + "/api/runs/ui-cancel/cancel", "POST")
        assert status == 200
        with pytest.raises(WorkflowCancelled):
            await asyncio.wait_for(second.result(), 3)
        assert (await rt.status("ui-cancel")).status is RunStatus.CANCELLED

        status, body = await call(server.url + "/api/runs/ui-cancel/tags", "POST", {"tags": {"reviewed": "yes"}})
        assert status == 200 and body["tags"] == {"reviewed": "yes"}
        status, body = await call(server.url + "/api/runs/ui-cancel/tags", "POST", {"tags": {"reviewed": None, "k": "v"}})
        assert body["tags"] == {"k": "v"}
        assert (await rt.status("ui-cancel")).tags == {"k": "v"}
        assert (await call(server.url + "/api/runs/ui-cancel/tags", "POST", {"tags": {"sicim.x": "1"}}))[0] == 400
        assert (await call(server.url + "/api/runs/ui-cancel/tags", "POST", {"tags": "nope"}))[0] == 400
        assert (await call(server.url + "/api/runs/ui-cancel/tags", "POST", {"tags": {"k": 1}}))[0] == 400
        assert (await call(server.url + "/api/runs/missing/tags", "POST", {"tags": {"k": "v"}}))[0] == 404
    await rt.shutdown()


async def test_schedules_through_the_api(rt):
    await rt.schedule(child, "tick", schedule_id="ui-sched", cron="0 3 * * *", tz="Europe/Istanbul", tags={"team": "ops"})
    async with await start_ui(rt, port=0) as server:
        status, data = await call(server.url + "/api/schedules")
        [sched] = data["schedules"]
        assert (sched["schedule_id"], sched["workflow"], sched["spec"], sched["tz"]) == ("ui-sched", "wf_ui_child", "cron 0 3 * * *", "Europe/Istanbul")
        assert sched["paused"] is False and sched["tags"] == {"team": "ops"} and sched["args"] == ["tick"]
        assert (await call(server.url + "/api/summary"))[1]["schedules"] == 1

        status, body = await call(server.url + "/api/schedules/ui-sched/pause", "POST")
        assert status == 200 and body["schedule"]["paused"] is True
        status, body = await call(server.url + "/api/schedules/ui-sched/resume", "POST")
        assert status == 200 and body["schedule"]["paused"] is False
        assert (await call(server.url + "/api/schedules/nope/pause", "POST"))[0] == 404
        status, body = await call(server.url + "/api/schedules/ui-sched", "DELETE")
        assert status == 200 and body == {"ok": True}
        assert (await call(server.url + "/api/schedules/ui-sched", "DELETE"))[0] == 404
        assert (await call(server.url + "/api/schedules"))[1]["schedules"] == []


async def test_bare_store_ui_persists_actions_without_touching_leases(store):
    """A UI process without the workflow code must queue signals/cancels for
    a worker instead of failing — and must never hold the run's lease."""
    await store.create_run(RunRecord(run_id="ghost", workflow="wf_not_loaded_anywhere"))
    async with await start_ui(store, port=0) as server:
        status, body = await call(server.url + "/api/runs/ghost/signal", "POST", {"name": "go", "payload": 1})
        assert status == 200
        [signal] = await store.load_signals("ghost")
        assert (signal.name, signal.payload, signal.consumed) == ("go", 1, False)
        status, body = await call(server.url + "/api/runs/ghost/cancel", "POST")
        assert status == 200 and body["run"]["cancel_requested"] is True
        assert (await store.load_run("ghost")).cancel_requested is True
        assert await store.load_lease("ghost") is None
        status, detail = await call(server.url + "/api/runs/ghost")
        assert detail["run"]["status"] == "running" and len(detail["signals"]) == 1


async def test_server_rejects_garbage_and_oversized_bodies(store):
    async with await start_ui(store, port=0) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"NOT HTTP AT ALL\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 5)
        assert response.startswith(b"HTTP/1.1 400") and b"malformed request line" in response
        writer.close()

        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.close()  # a client that connects and leaves must not disturb the server

        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"POST /api/runs/x/tags HTTP/1.1\r\nContent-Length: 99999999\r\n\r\n")
        await writer.drain()
        head = await asyncio.wait_for(reader.readline(), 5)
        assert head.startswith(b"HTTP/1.1 413")
        writer.close()

        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"GET /api/summary?x=1 HTTP/1.0\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 5)
        assert response.startswith(b"HTTP/1.1 200") and b"Connection: close" in response
        writer.close()


async def test_start_ui_validates_its_target():
    with pytest.raises(TypeError):
        await start_ui("not a store")
