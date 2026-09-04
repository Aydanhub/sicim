"""Cron expression parsing, next-fire computation and schedule specs."""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from sicim.schedule import CronExpr, build_spec, parse_duration, parse_spec, scheduled_run_id

UTC = dt.timezone.utc


def at(*args):
    return dt.datetime(*args, tzinfo=UTC).timestamp()


def nxt(expr, ts, tz=UTC):
    return dt.datetime.fromtimestamp(CronExpr.parse(expr).next_after(ts, tz), tz)


def test_every_minute_is_strictly_after():
    assert nxt("* * * * *", at(2026, 9, 3, 10, 15, 30)) == dt.datetime(2026, 9, 3, 10, 16, tzinfo=UTC)
    assert nxt("* * * * *", at(2026, 9, 3, 10, 15, 0)) == dt.datetime(2026, 9, 3, 10, 16, tzinfo=UTC)


def test_steps_ranges_and_weekday_names():
    # 2026-09-05 is a Saturday: the next weekday slot is Monday 09:00.
    assert nxt("*/15 9-17 * * mon-fri", at(2026, 9, 5, 12, 0)) == dt.datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
    assert nxt("*/15 9-17 * * 1-5", at(2026, 9, 7, 9, 0)) == dt.datetime(2026, 9, 7, 9, 15, tzinfo=UTC)
    assert nxt("*/15 9-17 * * 1-5", at(2026, 9, 7, 17, 45)) == dt.datetime(2026, 9, 8, 9, 0, tzinfo=UTC)


def test_leap_day_is_found_years_ahead():
    assert nxt("0 0 29 2 *", at(2026, 1, 1)) == dt.datetime(2028, 2, 29, 0, 0, tzinfo=UTC)


def test_day_of_month_and_weekday_are_or_ed_when_both_restricted():
    # 2026-09-03 is a Thursday. "13th OR Friday" -> Friday the 4th first...
    assert nxt("0 0 13 * fri", at(2026, 9, 3, 12, 0)) == dt.datetime(2026, 9, 4, 0, 0, tzinfo=UTC)
    # ...then Friday the 11th, before Sunday the 13th.
    assert nxt("0 0 13 * fri", at(2026, 9, 4, 12, 0)) == dt.datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
    # Only the weekday restricted: day-of-month wildcard is ignored.
    assert nxt("0 0 * * sun", at(2026, 9, 3, 12, 0)) == dt.datetime(2026, 9, 6, 0, 0, tzinfo=UTC)
    # Only the day restricted: weekday wildcard is ignored.
    assert nxt("0 0 13 * *", at(2026, 9, 3, 12, 0)) == dt.datetime(2026, 9, 13, 0, 0, tzinfo=UTC)


def test_sunday_aliases_month_names_and_offset_steps():
    assert CronExpr.parse("0 0 * * 7").weekdays == CronExpr.parse("0 0 * * sun").weekdays == frozenset({0})
    assert CronExpr.parse("0 0 1 jan,jul *").months == frozenset({1, 7})
    assert CronExpr.parse("5/20 * * * *").minutes == frozenset({5, 25, 45})
    assert CronExpr.parse("0 0 1-7/3 * *").days == frozenset({1, 4, 7})


def test_aliases():
    assert nxt("@hourly", at(2026, 9, 3, 12, 30)) == dt.datetime(2026, 9, 3, 13, 0, tzinfo=UTC)
    assert nxt("@daily", at(2026, 9, 3, 12, 0)) == dt.datetime(2026, 9, 4, 0, 0, tzinfo=UTC)
    assert nxt("@weekly", at(2026, 9, 3, 12, 0)) == dt.datetime(2026, 9, 6, 0, 0, tzinfo=UTC)  # Sunday
    assert nxt("@monthly", at(2026, 9, 3, 12, 0)) == dt.datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    assert nxt("@yearly", at(2026, 9, 3, 12, 0)) == dt.datetime(2027, 1, 1, 0, 0, tzinfo=UTC)


def test_time_zone_is_honoured():
    istanbul = ZoneInfo("Europe/Istanbul")  # UTC+3 all year
    fire = CronExpr.parse("0 9 * * *").next_after(at(2026, 9, 3, 0, 0), istanbul)
    assert dt.datetime.fromtimestamp(fire, UTC) == dt.datetime(2026, 9, 3, 6, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "expr",
    [
        "0 0 31 2 *",  # never fires
        "60 * * * *",  # minute out of range
        "0 25 * * *",  # hour out of range
        "0 0 0 * *",  # day out of range
        "0 0 * 13 *",  # month out of range
        "0 0 * * 8",  # weekday out of range
        "* * * *",  # too few fields
        "*/0 * * * *",  # zero step
        "x * * * *",  # garbage
        "5-3 * * * *",  # inverted range
    ],
)
def test_invalid_expressions_are_rejected(expr):
    with pytest.raises(ValueError):
        CronExpr.parse(expr)


def test_build_and_parse_spec_roundtrip():
    every = build_spec(every=dt.timedelta(minutes=5))
    assert every.text == "every 300.0"
    assert parse_spec(every.text) == every
    assert every.first_fire_at(1000.0) == 1300.0
    assert every.next_after(1300.0) == 1600.0

    once = build_spec(at=dt.datetime(2026, 9, 3, 10, 0, tzinfo=UTC))
    assert parse_spec(once.text) == once
    assert once.first_fire_at(0.0) == once.at
    assert once.next_after(once.at - 1) == once.at
    assert once.next_after(once.at) is None  # one-shot: nothing after it fired

    cron = build_spec(cron="0 9 * * *", tz="Europe/Istanbul")
    assert cron.text == "cron 0 9 * * *"
    assert parse_spec(cron.text, "Europe/Istanbul") == cron
    assert cron.first_fire_at(at(2026, 9, 3, 0, 0)) == at(2026, 9, 3, 6, 0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"every": 60, "cron": "* * * * *"},
        {"every": 0},
        {"every": -1},
        {"every": 5, "tz": "UTC"},
        {"cron": "0 9 * * *", "tz": "Mars/Olympus"},
        {"cron": "0 0 31 2 *"},
    ],
)
def test_build_spec_rejects_bad_arguments(kwargs):
    with pytest.raises(ValueError):
        build_spec(**kwargs)


def test_scheduled_run_id_is_deterministic_and_readable():
    ts = at(2026, 9, 3, 2, 0) + 0.25
    assert scheduled_run_id("nightly", ts) == "nightly@2026-09-03T02:00:00.250Z"
    assert scheduled_run_id("nightly", ts) == scheduled_run_id("nightly", ts)
    assert scheduled_run_id("nightly", ts) != scheduled_run_id("nightly", ts + 0.001)


# -- seconds field, @every and duration strings --------------------------------


def test_leading_seconds_field():
    assert nxt("*/10 * * * * *", at(2026, 9, 3, 10, 15, 33)) == dt.datetime(2026, 9, 3, 10, 15, 40, tzinfo=UTC)
    assert nxt("*/10 * * * * *", at(2026, 9, 3, 10, 15, 40)) == dt.datetime(2026, 9, 3, 10, 15, 50, tzinfo=UTC)
    assert nxt("*/10 * * * * *", at(2026, 9, 3, 10, 15, 59)) == dt.datetime(2026, 9, 3, 10, 16, 0, tzinfo=UTC)
    assert nxt("30 5 * * * *", at(2026, 9, 3, 10, 5, 31)) == dt.datetime(2026, 9, 3, 11, 5, 30, tzinfo=UTC)
    assert nxt("15 0 12 * * mon-fri", at(2026, 9, 5, 0, 0)) == dt.datetime(2026, 9, 7, 12, 0, 15, tzinfo=UTC)
    assert CronExpr.parse("*/10 * * * * *").has_seconds is True
    assert CronExpr.parse("* * * * *").has_seconds is False
    assert CronExpr.parse("0 * * * * *").seconds == frozenset({0})
    # Five-field expressions still fire on the minute.
    assert nxt("* * * * *", at(2026, 9, 3, 10, 15, 33)).second == 0
    with pytest.raises(ValueError):
        CronExpr.parse("60 * * * * *")
    with pytest.raises(ValueError):
        CronExpr.parse("0 0 0 * * * *")  # seven fields


@pytest.mark.parametrize(
    "text, seconds",
    [("90", 90.0), ("90s", 90.0), ("5m", 300.0), ("1h30m", 5400.0), ("1h 30m", 5400.0), ("2d12h", 216000.0),
     ("1.5h", 5400.0), ("250ms", 0.25), ("1w", 604800.0), (" 10S ", 10.0)],
)
def test_parse_duration(text, seconds):
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["", "abc", "1h30", "5x", "-5s", "h", "1..5s"])
def test_parse_duration_rejects_garbage(text):
    with pytest.raises(ValueError):
        parse_duration(text)


def test_every_accepts_duration_strings_and_at_every_alias():
    assert build_spec(every="5m") == build_spec(every=300) == build_spec(cron="@every 5m")
    assert build_spec(cron="@every 90s").text == "every 90.0"
    assert build_spec(cron="  @EVERY 1h30m ").seconds == 5400.0
    assert parse_spec(build_spec(cron="@every 2d").text) == build_spec(every="2d")
    for bad in ({"every": "abc"}, {"every": "0s"}, {"cron": "@every"}, {"cron": "@every 0m"},
                {"cron": "@every 5m", "tz": "UTC"}, {"every": "5m", "tz": "UTC"}, {"at": 1.0, "tz": "UTC"}):
        with pytest.raises(ValueError):
            build_spec(**bad)
