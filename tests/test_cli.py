"""The ``python -m sicim`` CLI: run filters and schedule management."""

import asyncio
import json
import re
import signal
import subprocess
import sys
import urllib.request

import pytest

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


def test_reset_rewinds_a_run_for_a_worker(tmp_path, capsys):
    db = str(tmp_path / "cli.db")
    seed(db)

    main(["--db", db, "reset", "cli-a", "--to-op", "0"])
    out = capsys.readouterr().out
    assert "rewound to op 0" in out
    assert "status is now running" in out

    main(["--db", db, "list", "--status", "running"])
    assert "cli-a" in capsys.readouterr().out

    with pytest.raises(SystemExit):
        main(["--db", db, "reset", "nope"])


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


def test_tag_command(tmp_path, capsys):
    db = str(tmp_path / "cli.db")
    seed(db)

    main(["--db", db, "tag", "cli-a", "stage=review", "--remove", "env"])
    assert capsys.readouterr().out.strip() == "tags:     customer=42,stage=review"
    main(["--db", db, "list", "--tag", "stage=review"])
    assert "cli-a" in capsys.readouterr().out

    with pytest.raises(SystemExit) as excinfo:
        main(["--db", db, "tag", "cli-a"])
    assert excinfo.value.code == 2
    with pytest.raises(SystemExit) as excinfo:
        main(["--db", db, "tag", "missing", "k=v"])
    assert excinfo.value.code == 1 and "no run with id" in capsys.readouterr().err
    with pytest.raises(SystemExit) as excinfo:
        main(["--db", db, "tag", "cli-a", "sicim.schedule=x"])
    assert excinfo.value.code == 1 and "reserved" in capsys.readouterr().err


def test_ui_command_serves_until_interrupted(tmp_path):
    db = str(tmp_path / "cli.db")
    seed(db)
    proc = subprocess.Popen(
        [sys.executable, "-m", "sicim", "--db", db, "ui", "--port", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        line = proc.stdout.readline()
        match = re.search(r"http://[^\s]+", line)
        assert match, f"no URL in {line!r}"
        with urllib.request.urlopen(match.group(0) + "/api/summary", timeout=5) as response:
            summary = json.loads(response.read())
        assert summary["counts"] == {"completed": 2} and summary["schedules"] == 1
        with urllib.request.urlopen(match.group(0) + "/", timeout=5) as response:
            assert b"<title>sicim</title>" in response.read()
        proc.send_signal(signal.SIGINT)
        assert proc.wait(timeout=10) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
