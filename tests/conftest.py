import pytest

from sicim import InMemoryStore, Runtime, SQLiteStore


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    """Every core test runs against both backends."""
    if request.param == "memory":
        backend = InMemoryStore()
    else:
        backend = SQLiteStore(str(tmp_path / "sicim-test.db"))
    yield backend
    backend.close()


@pytest.fixture
async def rt(store):
    runtime = Runtime(store)
    yield runtime
    await runtime.shutdown()
