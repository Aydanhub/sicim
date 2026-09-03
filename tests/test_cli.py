"""The ``python -m sicim`` CLI: run filters and schedule management."""

import asyncio

from sicim import Runtime, SQLiteStore, workflow
from sicim.__main__ import main


@workflow(name="wf_cli")
async def wf(ctx):
    return "ok"


def seed(path):
    async def go():
        store = SQLiteStore(path)
        rt = Runtime(store, scheduler=False)
        await (await rt.start(wf, run_id="cli-a", tags={"customer": "42", "env": "prod"})).result()
        await (await rt.start(wf, run_id="cli-b", tags={"customer": "43"})).result()
        await rt.schedule(wf, schedule_id="nightly", cron="0 2 * * *", tz="Europe/Istanbul")
        await rt.shutdown()
        store.close()

    asyncio.run(go())


def test_list_filters_by_tag_workflow_and_limit(tmp_path, capsys):
    db = str(tmp_path / "cli.db")
    seed(db)

    main(["--db", db, "list", "--tag", "customer=42"])
    out = capsys.readouterr().out
    assert "cli-a" in out and "cli-b" not in out
    assert "customer=42,env=prod" in out

    main(["--db", db, "list", "--workflow", "wf_cli", "--limit", "1"])
    out = capsys.readouterr().out
    assert "cli-b" in out and "cli-a" not in out  # --limit shows the newest

    main(["--db", db, "list", "--tag", "customer=42", "--tag", "env=dev"])
    assert "(no runs)" in capsys.readouterr().out

    main(["--db", db, "list", "--workflow", "unknown"])
    assert "(no runs)" in capsys.readouterr().out


def test_show_prints_tags(tmp_path, capsys):
    db = str(tmp_path / "cli.db")
    seed(db)
    main(["--db", db, "show", "cli-a"])
    out = capsys.readouterr().out
    assert "tags:     customer=42,env=prod" in out
    assert "run_started" in out and "run_completed" in out


def test_schedule_commands(tmp_path, capsys):
    db = str(tmp_path / "cli.db")
    seed(db)

    main(["--db", db, "schedule", "list"])
    out = capsys.readouterr().out
    assert "nightly" in out and "cron 0 2 * * * (Europe/Istanbul)" in out and "active" in out

    main(["--db", db, "schedule", "pause", "nightly"])
    assert "schedule 'nightly' paused" in capsys.readouterr().out
    main(["--db", db, "schedule", "list"])
    assert "paused" in capsys.readouterr().out

    main(["--db", db, "schedule", "resume", "nightly"])
    capsys.readouterr()
    main(["--db", db, "schedule", "list"])
    assert "active" in capsys.readouterr().out

    main(["--db", db, "schedule", "delete", "nightly"])
    assert "schedule 'nightly' deleted" in capsys.readouterr().out
    main(["--db", db, "schedule", "list"])
    assert "(no schedules)" in capsys.readouterr().out
