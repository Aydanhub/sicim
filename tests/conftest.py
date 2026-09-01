import os
import shutil
import subprocess
import tempfile

import pytest

from sicim import InMemoryStore, Runtime, SQLiteStore

_PG_BIN_DIRS = (
    "",  # PATH
    "/opt/homebrew/opt/postgresql@17/bin",
    "/opt/homebrew/opt/postgresql@16/bin",
    "/usr/lib/postgresql/17/bin",
    "/usr/lib/postgresql/16/bin",
)


def _pg_binary(name: str) -> str | None:
    for base in _PG_BIN_DIRS:
        path = shutil.which(name) if not base else os.path.join(base, name)
        if path and os.path.exists(path):
            return path
    return None


@pytest.fixture(scope="session")
def pg_dsn(tmp_path_factory):
    """DSN of a throwaway PostgreSQL cluster (or None when unavailable).

    Set SICIM_PG_DSN to use an existing server (e.g. in CI) instead of
    spawning one. Without that, a local cluster is initdb'd into a temp dir
    and torn down at session end; if binaries or psycopg are missing, the
    postgres store param is skipped.
    """
    dsn = os.environ.get("SICIM_PG_DSN")
    if dsn:
        yield dsn
        return
    try:
        import psycopg  # noqa: F401
    except ImportError:
        yield None
        return
    initdb, pg_ctl, createdb = _pg_binary("initdb"), _pg_binary("pg_ctl"), _pg_binary("createdb")
    if not (initdb and pg_ctl and createdb):
        yield None
        return
    data = str(tmp_path_factory.mktemp("pgdata"))
    # NOT tmp_path_factory: pytest's deeply nested paths exceed macOS's
    # 104-char AF_UNIX socket path limit.
    sock = tempfile.mkdtemp(prefix="sicim-pg-")
    # LC_ALL must be a plain locale: macOS CoreFoundation otherwise spawns a
    # thread during locale lookup and PostgreSQL 17 aborts with
    # "postmaster became multithreaded during startup".
    env = {**os.environ, "LC_ALL": "C"}
    try:
        subprocess.run(
            [initdb, "-D", data, "-U", "sicim", "--auth=trust", "-E", "UTF8"],
            check=True, capture_output=True, env=env,
        )
        # -l detaches the postmaster's output; without it the daemon inherits
        # our pipes and subprocess.run never sees EOF.
        subprocess.run(
            [
                pg_ctl, "-D", data, "-w", "-l", os.path.join(data, "server.log"),
                "-o", f"-p 54329 -k {sock} -c listen_addresses= -c fsync=off", "start",
            ],
            check=True, capture_output=True, env=env,
        )
        subprocess.run(
            [createdb, "-h", sock, "-p", "54329", "-U", "sicim", "sicim_test"],
            check=True, capture_output=True, env=env,
        )
    except (subprocess.CalledProcessError, OSError):
        yield None
        return
    yield f"postgresql://sicim@/sicim_test?host={sock}&port=54329"
    subprocess.run([pg_ctl, "-D", data, "-m", "immediate", "stop"], capture_output=True, env=env)
    shutil.rmtree(sock, ignore_errors=True)


@pytest.fixture(params=["memory", "sqlite", "postgres"])
async def store(request, tmp_path, pg_dsn):
    """Every core test runs against all three backends."""
    if request.param == "memory":
        backend = InMemoryStore()
    elif request.param == "sqlite":
        backend = SQLiteStore(str(tmp_path / "sicim-test.db"))
    else:
        if pg_dsn is None:
            pytest.skip("PostgreSQL not available")
        from sicim.pg import PostgresStore

        backend = await PostgresStore.connect(pg_dsn)
        # The cluster is session-scoped; isolate each test by clearing tables.
        for table in ("events", "signals", "leases", "runs"):
            await backend._conn.execute(f"DELETE FROM {table}")
    yield backend
    await backend.aclose()


@pytest.fixture
async def rt(store):
    runtime = Runtime(store)
    yield runtime
    await runtime.shutdown()
