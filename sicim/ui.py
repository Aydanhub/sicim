"""Web-based monitoring UI — standard library only, no extra dependencies.

    python -m sicim --db sicim.db ui                 # http://127.0.0.1:8787
    python -m sicim --pg postgresql://… ui --port 9000

or embedded in a worker, where actions go through that worker in-process:

    from sicim.ui import start_ui
    server = await start_ui(rt, port=8787)
    ...
    await server.aclose()

The page lists runs (filterable by status, workflow, tags and parent), shows a
run's record, tags, journal, signals, lease and children, lets you cancel a
run, send it a signal, edit its tags and rewind it to an operation (reset), and
manages schedules. It refreshes itself every two seconds.

The JSON API behind it (every response is ``application/json``; errors carry
``{"error": message}``):

    GET    /api/summary                    run counts per status, workflow names
    GET    /api/runs?status=&workflow=&tag=k=v&parent=&limit=&order=oldest
    GET    /api/runs/{run_id}              record, events, signals, lease, children
    POST   /api/runs/{run_id}/cancel
    POST   /api/runs/{run_id}/reset        {"to_op": 3, "force": false}
    POST   /api/runs/{run_id}/signal       {"name": "approval", "payload": …}
    POST   /api/runs/{run_id}/tags         {"tags": {"k": "v", "old": null}}
    GET    /api/schedules
    POST   /api/schedules/{id}/pause  |  /resume
    DELETE /api/schedules/{id}

Path segments are percent-encoded (run ids may contain ``#``, ``/``, ``@``).
There is no authentication: bind to localhost (the default) or put the server
behind a proxy that adds it. A signal or cancel sent from a process that lacks
the workflow's code is persisted and picked up by the worker driving the run
(or by the next one that recovers it).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import re
import time
from importlib import resources
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__
from .errors import RunNotFound, ScheduleNotFound, SicimError
from .runtime import Runtime
from .store import RunRecord, RunStatus, ScheduleRecord, Store

logger = logging.getLogger("sicim")

_MAX_BODY = 1 << 20  # 1 MiB is plenty for a signal payload or a tag update
_READ_TIMEOUT = 30.0
_REASONS = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    409: "Conflict",
    413: "Payload Too Large",
    500: "Internal Server Error",
}


class HTTPError(Exception):
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(f"{status}: {message}")


@dataclasses.dataclass
class Request:
    method: str
    path: str
    query: dict[str, list[str]]
    body: bytes

    def param(self, name: str, default: str | None = None) -> str | None:
        values = self.query.get(name)
        return values[-1] if values else default

    def json_object(self) -> dict[str, Any]:
        if not self.body:
            return {}
        try:
            data = json.loads(self.body)
        except ValueError as exc:
            raise HTTPError(400, f"invalid JSON body: {exc}") from None
        if not isinstance(data, dict):
            raise HTTPError(400, "JSON body must be an object")
        return data


@dataclasses.dataclass
class Response:
    body: bytes
    content_type: str = "application/json; charset=utf-8"
    status: int = 200


def _json(data: Any, status: int = 200) -> Response:
    return Response(json.dumps(data, ensure_ascii=False).encode("utf-8"), status=status)


def _run_json(record: RunRecord) -> dict[str, Any]:
    data = dataclasses.asdict(record)
    data["status"] = record.status.value
    return data


def _schedule_json(record: ScheduleRecord) -> dict[str, Any]:
    return dataclasses.asdict(record)


async def _read_request(reader: asyncio.StreamReader) -> Request:
    line = await reader.readline()
    if not line:
        raise ConnectionError("client closed the connection")
    parts = line.decode("latin-1").rstrip("\r\n").split()
    if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
        raise HTTPError(400, "malformed request line")
    method, target = parts[0].upper(), parts[1]
    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, _, value = line.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()
    try:
        length = int(headers.get("content-length") or 0)
    except ValueError:
        raise HTTPError(400, "bad Content-Length") from None
    if length < 0:
        raise HTTPError(400, "bad Content-Length")
    if length > _MAX_BODY:
        raise HTTPError(413, f"request body exceeds {_MAX_BODY} bytes")
    body = await reader.readexactly(length) if length else b""
    split = urlsplit(target)
    return Request(method, split.path or "/", parse_qs(split.query, keep_blank_values=True), body)


class UIServer:
    """The HTTP server behind :func:`start_ui`; ``url`` is where it listens."""

    def __init__(self, runtime: Runtime, *, owns_runtime: bool = False):
        self.runtime = runtime
        self._owns_runtime = owns_runtime
        self._server: asyncio.AbstractServer | None = None
        self._connections: set[asyncio.Task] = set()
        self._page = resources.files(__package__).joinpath("ui.html").read_bytes()
        self.host = "127.0.0.1"
        self.port = 0
        Handler = Callable[..., Awaitable[Response]]
        self._routes: list[tuple[str, re.Pattern[str], Handler]] = [
            ("GET", re.compile(r"/"), self._page_handler),
            ("GET", re.compile(r"/api/summary"), self._summary),
            ("GET", re.compile(r"/api/runs"), self._runs),
            ("GET", re.compile(r"/api/runs/(?P<run_id>[^/]+)"), self._run),
            ("POST", re.compile(r"/api/runs/(?P<run_id>[^/]+)/cancel"), self._cancel),
            ("POST", re.compile(r"/api/runs/(?P<run_id>[^/]+)/reset"), self._reset),
            ("POST", re.compile(r"/api/runs/(?P<run_id>[^/]+)/signal"), self._signal),
            ("POST", re.compile(r"/api/runs/(?P<run_id>[^/]+)/tags"), self._tags),
            ("GET", re.compile(r"/api/schedules"), self._schedules),
            ("POST", re.compile(r"/api/schedules/(?P<schedule_id>[^/]+)/pause"), self._pause),
            ("POST", re.compile(r"/api/schedules/(?P<schedule_id>[^/]+)/resume"), self._resume),
            ("DELETE", re.compile(r"/api/schedules/(?P<schedule_id>[^/]+)"), self._unschedule),
        ]

    @property
    def url(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"http://{host}:{self.port}"

    async def _listen(self, host: str, port: int) -> None:
        self._server = await asyncio.start_server(self._connection, host, port)
        name = self._server.sockets[0].getsockname()
        self.host, self.port = name[0], name[1]

    async def aclose(self) -> None:
        """Stop listening, drop open connections and — for a server started on
        a bare store — shut the internal runtime down."""
        server, self._server = self._server, None
        if server is not None:
            server.close()
            for task in list(self._connections):
                task.cancel()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(server.wait_closed(), 2.0)
        if self._owns_runtime:
            await self.runtime.shutdown()

    async def __aenter__(self) -> "UIServer":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    # -- HTTP plumbing -------------------------------------------------------

    async def _connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            try:
                async with asyncio.timeout(_READ_TIMEOUT):
                    request = await _read_request(reader)
            except HTTPError as exc:
                response = _json({"error": exc.message}, exc.status)
            except (TimeoutError, asyncio.IncompleteReadError, ConnectionError, ValueError):
                return  # idle, aborted or garbage connection: nothing to answer
            else:
                response = await self._dispatch(request)
            head = (
                f"HTTP/1.1 {response.status} {_REASONS.get(response.status, 'OK')}\r\n"
                f"Content-Type: {response.content_type}\r\n"
                f"Content-Length: {len(response.body)}\r\n"
                "Cache-Control: no-store\r\n"
                "Connection: close\r\n\r\n"
            ).encode("latin-1")
            writer.write(head + response.body)
            await writer.drain()
        except (ConnectionError, TimeoutError):
            pass
        finally:
            if task is not None:
                self._connections.discard(task)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _dispatch(self, request: Request) -> Response:
        allowed: list[str] = []
        for method, pattern, handler in self._routes:
            match = pattern.fullmatch(request.path)
            if match is None:
                continue
            if request.method != method:
                allowed.append(method)
                continue
            params = {key: unquote(value) for key, value in match.groupdict().items()}
            try:
                return await handler(request, **params)
            except HTTPError as exc:
                return _json({"error": exc.message}, exc.status)
            except (RunNotFound, ScheduleNotFound) as exc:
                return _json({"error": str(exc)}, 404)
            except (ValueError, TypeError) as exc:
                return _json({"error": str(exc)}, 400)
            except SicimError as exc:
                return _json({"error": str(exc)}, 409)
            except Exception:
                logger.exception("UI request %s %s failed", request.method, request.path)
                return _json({"error": "internal error (see the server log)"}, 500)
        if allowed:
            return _json({"error": f"method not allowed; use {', '.join(allowed)}"}, 405)
        return _json({"error": "not found"}, 404)

    # -- handlers ------------------------------------------------------------

    async def _page_handler(self, request: Request) -> Response:
        return Response(self._page, "text/html; charset=utf-8")

    async def _summary(self, request: Request) -> Response:
        store = self.runtime.store
        return _json(
            {
                "version": __version__,
                "store": type(store).__name__,
                "worker_id": self.runtime.worker_id,
                "counts": await store.count_runs(),
                "workflows": await store.list_workflows(),
                "schedules": len(await store.list_schedules()),
                "now": time.time(),
            }
        )

    async def _runs(self, request: Request) -> Response:
        status: RunStatus | None = None
        status_text = request.param("status")
        if status_text:
            try:
                status = RunStatus(status_text)
            except ValueError:
                raise HTTPError(400, f"unknown status {status_text!r}") from None
        tags: dict[str, str] = {}
        for item in request.query.get("tag", []):
            key, sep, value = item.partition("=")
            if not sep or not key:
                raise HTTPError(400, f"tag filter must look like key=value, got {item!r}")
            tags[key] = value
        try:
            limit = int(request.param("limit", "100") or 0)
        except ValueError:
            raise HTTPError(400, "limit must be an integer (0 = no limit)") from None
        if limit < 0:
            raise HTTPError(400, "limit must be >= 0 (0 = no limit)")
        runs = await self.runtime.list_runs(
            status,
            workflow=request.param("workflow") or None,
            tags=tags or None,
            parent_run_id=request.param("parent") or None,
            limit=limit or None,
            newest_first=request.param("order", "newest") != "oldest",
        )
        return _json({"runs": [_run_json(run) for run in runs], "now": time.time()})

    async def _run(self, request: Request, run_id: str) -> Response:
        store = self.runtime.store
        record = await store.load_run(run_id)
        if record is None:
            raise RunNotFound(f"no run with id '{run_id}'")
        events = await store.load_events(run_id)
        signals = await store.load_signals(run_id)
        lease = await store.load_lease(run_id)
        children = await store.list_runs(parent_run_id=run_id)
        return _json(
            {
                "run": _run_json(record),
                "events": [dataclasses.asdict(event) for event in events],
                "signals": [dataclasses.asdict(signal) for signal in signals],
                "lease": {"owner": lease[0], "expires_at": lease[1]} if lease else None,
                "children": [_run_json(child) for child in children],
                "now": time.time(),
            }
        )

    async def _cancel(self, request: Request, run_id: str) -> Response:
        await self.runtime.cancel(run_id)
        return _json({"run": _run_json(await self.runtime.status(run_id))})

    async def _reset(self, request: Request, run_id: str) -> Response:
        body = request.json_object()
        to_op = body.get("to_op")
        if to_op is not None and (not isinstance(to_op, int) or isinstance(to_op, bool) or to_op < 0):
            raise HTTPError(400, "'to_op' must be an integer >= 0 (or null for the failed op)")
        await self.runtime.reset(run_id, to_op=to_op, force=bool(body.get("force")))
        return _json({"run": _run_json(await self.runtime.status(run_id))})

    async def _signal(self, request: Request, run_id: str) -> Response:
        body = request.json_object()
        name = body.get("name")
        if not isinstance(name, str) or not name:
            raise HTTPError(400, "'name' must be a non-empty string")
        await self.runtime.signal(run_id, name, body.get("payload"))
        return _json({"ok": True})

    async def _tags(self, request: Request, run_id: str) -> Response:
        tags = request.json_object().get("tags")
        if not isinstance(tags, dict):
            raise HTTPError(400, "'tags' must be an object of key -> value (null removes a key)")
        return _json({"tags": await self.runtime.tag(run_id, tags)})

    async def _schedules(self, request: Request) -> Response:
        schedules = await self.runtime.list_schedules()
        return _json({"schedules": [_schedule_json(s) for s in schedules], "now": time.time()})

    async def _pause(self, request: Request, schedule_id: str) -> Response:
        await self.runtime.pause_schedule(schedule_id)
        return _json({"schedule": _schedule_json(await self.runtime.get_schedule(schedule_id))})

    async def _resume(self, request: Request, schedule_id: str) -> Response:
        await self.runtime.resume_schedule(schedule_id)
        return _json({"schedule": _schedule_json(await self.runtime.get_schedule(schedule_id))})

    async def _unschedule(self, request: Request, schedule_id: str) -> Response:
        await self.runtime.unschedule(schedule_id)
        return _json({"ok": True})


async def start_ui(target: Runtime | Store, *, host: str = "127.0.0.1", port: int = 8787) -> UIServer:
    """Serve the monitoring UI for a :class:`Runtime` (cancel/signal/tag go
    through it, in process) or a bare :class:`Store` (a signal-only Runtime
    is created internally). ``port=0`` picks a free port; the server's
    ``url`` says where it listens. Close it with ``await server.aclose()``."""
    if isinstance(target, Runtime):
        server = UIServer(target)
    elif isinstance(target, Store):
        server = UIServer(Runtime(target, scheduler=False), owns_runtime=True)
    else:
        raise TypeError(f"start_ui() expects a Runtime or a Store, got {type(target).__name__}")
    await server._listen(host, port)
    logger.info("sicim UI listening on %s", server.url)
    return server
