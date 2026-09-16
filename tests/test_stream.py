"""The UI's live change stream (SSE): in-process pushes and cross-process polling."""

import asyncio
import contextlib
import json
import urllib.parse

import pytest
from helpers import wait_for_events

from sicim import Kind, Runtime, RunStatus, workflow
from sicim.ui import UIServer, start_ui


async def produce(value):
    return value


@workflow(name="wf_stream")
async def streamed(ctx, value):
    return await ctx.step(produce, value, name="produce")


@workflow(name="wf_stream_wait")
async def waiter(ctx):
    return await ctx.wait_event("go")


class Stream:
    """Minimal SSE client: the UI server speaks HTTP/1.1 over a plain socket."""

    def __init__(self, reader, writer, status, headers):
        self._reader = reader
        self._writer = writer
        self.status = status
        self.headers = headers

    @classmethod
    async def open(cls, url, *, run=None, path="/api/stream"):
        host, _, port = url.removeprefix("http://").partition(":")
        reader, writer = await asyncio.open_connection(host, int(port))
        target = path + (f"?run={urllib.parse.quote(run, safe='')}" if run else "")
        writer.write(
            f"GET {target} HTTP/1.1\r\nHost: {host}\r\nAccept: text/event-stream\r\n\r\n".encode()
        )
        await writer.drain()
        status = int((await reader.readline()).decode().split()[1])
        headers = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        return cls(reader, writer, status, headers)

    async def line(self, timeout=5.0):
        async with asyncio.timeout(timeout):
            return (await self._reader.readline()).decode("utf-8").rstrip("\r\n")

    async def batch(self, timeout=5.0):
        """Next data message, decoded (comments and blank lines are skipped)."""
        async with asyncio.timeout(timeout):
            while True:
                line = await self.line(timeout)
                if line.startswith("data: "):
                    return json.loads(line[len("data: "):])

    async def until(self, predicate, timeout=5.0):
        """Every notification seen up to (and including) the matching one."""
        seen = []
        async with asyncio.timeout(timeout):
            while True:
                for note in await self.batch(timeout):
                    seen.append(note)
                    if predicate(note):
                        return seen

    async def body(self, timeout=5.0):
        async with asyncio.timeout(timeout):
            return (await self._reader.read()).decode("utf-8")

    async def aclose(self):
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()


async def test_stream_headers_and_greeting(store):
    async with await start_ui(store, port=0) as server:
        stream = await Stream.open(server.url)
        assert stream.status == 200
        assert stream.headers["content-type"].startswith("text/event-stream")
        assert stream.headers["cache-control"] == "no-store"
        [hello] = await stream.batch()
        assert hello["kind"] == "hello" and hello["run"] is None and "version" in hello
        await stream.aclose()


async def test_stream_pushes_journal_events_of_local_runs(store):
    rt = Runtime(store)
    async with await start_ui(rt, port=0) as server:
        stream = await Stream.open(server.url)
        assert (await stream.batch())[0]["kind"] == "hello"

        assert await (await rt.start(streamed, "v", run_id="s1")).result() == "v"
        seen = await stream.until(lambda n: n["kind"] == Kind.RUN_COMPLETED)
        kinds = [n["kind"] for n in seen]
        assert Kind.RUN_STARTED in kinds and Kind.STEP_COMPLETED in kinds
        assert {n["run_id"] for n in seen} == {"s1"}
        assert all(isinstance(n["seq"], int) for n in seen)
        await stream.aclose()
    await rt.shutdown()


async def test_stream_for_one_run_skips_other_runs_operations(store):
    rt = Runtime(store, signal_poll_interval=0.05)
    async with await start_ui(rt, port=0) as server:
        await rt.start(waiter, run_id="watched")
        await wait_for_events(store, "watched", Kind.WAIT_CREATED, 1)

        stream = await Stream.open(server.url, run="watched")
        await stream.batch()  # hello

        await (await rt.start(streamed, "other", run_id="noise")).result()
        await rt.signal("watched", "go", {"ok": True})

        seen = await stream.until(lambda n: n["kind"] == Kind.EVENT_CONSUMED)
        noise = [n for n in seen if n["run_id"] == "noise"]
        assert noise, "run-level events of other runs still move the counters"
        assert all(n["kind"] in (Kind.RUN_STARTED, Kind.RUN_COMPLETED) for n in noise)
        assert not [n for n in noise if n["kind"].startswith("step_")]
        await stream.aclose()
    await rt.shutdown()


async def test_stream_notices_another_process_by_polling(store):
    """The UI process drives nothing, so only the store poll can see this."""
    worker = Runtime(store, worker_id="elsewhere", signal_poll_interval=0.05)
    async with await start_ui(store, port=0, stream_poll_interval=0.05) as server:
        handle = await worker.start(waiter, run_id="p1")
        await wait_for_events(store, "p1", Kind.WAIT_CREATED, 1)

        stream = await Stream.open(server.url, run="p1")
        await stream.batch()  # hello
        await asyncio.sleep(0.2)  # let a poll tick snapshot the run as it stands

        await worker.signal("p1", "go", {"ok": True})
        assert await handle.result() == {"ok": True}

        seen = await stream.until(lambda n: n["kind"] == "journal" and n["run_id"] == "p1")
        assert any(n["kind"] == "run_changed" and n["run_id"] == "p1" for n in seen)
        assert [n for n in seen if n["kind"] == "journal"][-1]["seq"] >= 3
        await stream.aclose()
    await worker.shutdown()


async def test_stream_notices_schedule_changes(rt):
    async with await start_ui(rt, port=0, stream_poll_interval=0.05) as server:
        stream = await Stream.open(server.url)
        await stream.batch()  # hello
        await asyncio.sleep(0.15)  # let the first poll snapshot the empty store

        await rt.schedule(streamed, "v", schedule_id="sched", cron="0 4 * * *")
        await stream.until(lambda n: n["kind"] == "schedules")
        await stream.aclose()


async def test_idle_stream_sends_keepalive_comments(store):
    server = UIServer(Runtime(store, scheduler=False), owns_runtime=True, heartbeat=0.05)
    await server._listen("127.0.0.1", 0)
    async with server:
        stream = await Stream.open(server.url)
        await stream.batch()  # hello
        assert await stream.line() == ""        # blank line closing the message
        assert await stream.line() == ": keepalive"
        await stream.aclose()


async def test_stream_limit_and_cleanup(store):
    server = UIServer(Runtime(store, scheduler=False), owns_runtime=True, max_streams=1)
    await server._listen("127.0.0.1", 0)
    async with server:
        first = await Stream.open(server.url)
        await first.batch()
        assert len(server._subscribers) == 1

        refused = await Stream.open(server.url)
        assert refused.status == 503
        assert "too many live streams" in json.loads(await refused.body())["error"]
        await refused.aclose()

        await first.aclose()
        deadline = asyncio.get_running_loop().time() + 5
        while server._subscribers:
            assert asyncio.get_running_loop().time() < deadline, "subscriber was not released"
            await asyncio.sleep(0.02)


async def test_closing_the_ui_detaches_its_observer(store):
    rt = Runtime(store)
    server = await start_ui(rt, port=0)
    assert len(rt._observers) == 1
    await server.aclose()
    assert rt._observers == []
    assert await (await rt.start(streamed, "v", run_id="after")).result() == "v"
    assert (await rt.status("after")).status is RunStatus.COMPLETED
    await rt.shutdown()
